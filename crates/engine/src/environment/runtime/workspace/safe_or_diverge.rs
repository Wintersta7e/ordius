//! `SafeOrDiverge` write-back — the conflict-aware remote→host reconciliation.
//!
//! Split out of [`super::manager`]: the `SafeOrDiverge` policy writes a node's
//! output back to the host **only where the host still matches what
//! `reconcile_in` uploaded** (`host@in`), and preserves the node's version under
//! `.ordius/diverged/<run>/<env>/<rel>` (plus a JSON report) wherever the host
//! changed concurrently.
//!
//! The entry point is [`write_back_safe_or_diverge`], dispatched by
//! `manager::reconcile_write_back`. The host-shape classification spine
//! ([`classify_host_state`] / [`matches_host_at_in`] / [`HostState`]) and the
//! divergence-artifact writers ([`write_diverged_artifact`] /
//! [`write_diverge_report`]) are shared with the manager's unit tests.

use std::path::Path;
use std::sync::Arc;

use serde::Serialize;

use crate::environment::runtime::error::DispatchError;

use super::manager::{
    RemoteFile, RemoteListing, host_io_err, is_shadowed_by_symlink, list_remote_files, tmp_sibling,
    write_host_file_atomic,
};
use super::safety;
use super::transport::WorkspaceTransportFactory;

/// Run-invariant context shared by the `SafeOrDiverge` phases.
struct SodCtx<'a> {
    host_ws: &'a Path,
    run_id: &'a str,
    env_id: &'a str,
    baseline: &'a safety::Manifest,
}

/// The host file hash when the host is a regular file, else `None`
/// (for `DivergeEntry.host_sha256`).
fn host_sha_opt(host: &HostState) -> Option<String> {
    match host {
        HostState::File { sha256_hex } => Some(sha256_hex.clone()),
        _ => None,
    }
}

/// Classify a host that has *diverged* from what `reconcile_in` uploaded into a
/// [`DivergeReason`]. Only meaningful once `matches_host_at_in` has returned
/// `false` for this rel.
fn conflict_reason(host: &HostState, baseline: &safety::Manifest, rel: &str) -> DivergeReason {
    match host {
        HostState::UnsafeSymlink | HostState::Unreadable => DivergeReason::HostUnsafe,
        HostState::File { .. } => {
            if baseline.dirs.contains(rel) {
                DivergeReason::HostTypeChanged
            } else if baseline.files.contains_key(rel) {
                DivergeReason::HostModified
            } else {
                DivergeReason::HostCreated
            }
        },
        HostState::Dir => {
            if baseline.files.contains_key(rel) {
                DivergeReason::HostTypeChanged
            } else {
                DivergeReason::HostCreated
            }
        },
        HostState::Absent => DivergeReason::HostDeleted,
    }
}

/// A report entry for a conflict where no remote bytes are preserved (the host
/// is kept in place).
fn report_only(rel: &str, reason: DivergeReason, host: &HostState) -> DivergeEntry {
    DivergeEntry {
        rel: rel.to_string(),
        reason,
        host_sha256: host_sha_opt(host),
        remote_sha256: None,
        diverged_path: None,
    }
}

/// Write a remote file's bytes into the divergence dir and record a report
/// entry preserving both hashes + the artifact path. Fails closed (propagates)
/// when the artifact path is unsafe.
fn diverge_file(
    report: &mut DivergeReport,
    ctx: &SodCtx,
    rel: &str,
    bytes: &[u8],
    reason: DivergeReason,
    host: &HostState,
    remote_sha256: &str,
) -> Result<(), DispatchError> {
    let diverged_path = write_diverged_artifact(ctx.host_ws, ctx.run_id, ctx.env_id, rel, bytes)?;
    report.diverged.push(DivergeEntry {
        rel: rel.to_string(),
        reason,
        host_sha256: host_sha_opt(host),
        remote_sha256: Some(remote_sha256.to_string()),
        diverged_path: Some(diverged_path),
    });
    Ok(())
}

/// Phase 1 — apply plain deletions (deepest-first). Each `(rel, is_dir)` whose
/// host still matches `host@in` is removed (`remove_file`/`remove_dir`, never
/// recursive); a non-empty host dir is kept (`dir_delete_nonempty`); a host that
/// diverged is kept (`delete_vs_host_modified` / `host_unsafe`).
fn sod_deletions(
    ctx: &SodCtx,
    report: &mut DivergeReport,
    deletions: &[(String, bool)],
) -> Result<(), DispatchError> {
    for (rel, is_dir) in deletions {
        let host = classify_host_state(ctx.host_ws, rel);
        // No-op: the node deleted this rel and the host is already absent (the
        // user agreed) — nothing to remove and no false conflict to report.
        if host == HostState::Absent {
            continue;
        }
        if !matches_host_at_in(&host, ctx.baseline, rel) {
            let reason = match host {
                HostState::UnsafeSymlink | HostState::Unreadable => DivergeReason::HostUnsafe,
                _ => DivergeReason::DeleteVsHostModified,
            };
            report.diverged.push(report_only(rel, reason, &host));
            continue;
        }
        let target = ctx.host_ws.join(rel);
        let res = if *is_dir {
            std::fs::remove_dir(&target)
        } else {
            std::fs::remove_file(&target)
        };
        match res {
            Ok(()) => {},
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {},
            Err(e) if *is_dir && e.kind() == std::io::ErrorKind::DirectoryNotEmpty => {
                report
                    .diverged
                    .push(report_only(rel, DivergeReason::DirDeleteNonempty, &host));
            },
            // An unexpected host fs error means a node mutation failed; abort the
            // phase so write-back returns `Err`, the baseline is not advanced,
            // and teardown preserves the remote root.
            Err(e) => return Err(host_io_err(&target, "delete", &e)),
        }
    }
    Ok(())
}

/// Phase 2 — file↔dir type-change replacements (the dir→file children were
/// already removed in phase 1, so the host dir is empty here). A host that no
/// longer matches `host@in` is preserved (diverged / reported).
fn sod_type_changes(
    ctx: &SodCtx,
    report: &mut DivergeReport,
    file_to_dir: &[String],
    dir_to_file: &[&RemoteFile],
) -> Result<(), DispatchError> {
    for rel in file_to_dir {
        let host = classify_host_state(ctx.host_ws, rel);
        if host == HostState::Dir {
            continue; // already a dir
        }
        if matches_host_at_in(&host, ctx.baseline, rel) {
            // Host is the uploaded file — remove it, then create the dir in its
            // place. A NotFound here is benign (already gone); any other fs
            // error means the node's dir output would be lost — propagate.
            let target = ctx.host_ws.join(rel);
            if let Err(e) = std::fs::remove_file(&target)
                && e.kind() != std::io::ErrorKind::NotFound
            {
                return Err(host_io_err(&target, "type-change file remove", &e));
            }
            if safety::host_target_is_symlink_safe(ctx.host_ws, rel)
                && let Err(e) = std::fs::create_dir_all(&target)
            {
                return Err(host_io_err(&target, "type-change dir create", &e));
            }
        } else {
            report.diverged.push(report_only(
                rel,
                conflict_reason(&host, ctx.baseline, rel),
                &host,
            ));
        }
    }
    for f in dir_to_file {
        let rel = f.rel.as_str();
        let host = classify_host_state(ctx.host_ws, rel);
        if let HostState::File { sha256_hex } = &host
            && *sha256_hex == f.entry.sha256_hex
        {
            continue; // host already holds the node's bytes
        }
        if matches_host_at_in(&host, ctx.baseline, rel) {
            match std::fs::remove_dir(ctx.host_ws.join(rel)) {
                Ok(()) => {
                    if safety::host_target_is_symlink_safe(ctx.host_ws, rel) {
                        write_host_file_atomic(ctx.host_ws, rel, &f.bytes)?;
                    }
                },
                // Untracked host children remain — cannot replace; preserve bytes.
                Err(_) => diverge_file(
                    report,
                    ctx,
                    rel,
                    &f.bytes,
                    DivergeReason::HostTypeChanged,
                    &host,
                    &f.entry.sha256_hex,
                )?,
            }
        } else {
            diverge_file(
                report,
                ctx,
                rel,
                &f.bytes,
                conflict_reason(&host, ctx.baseline, rel),
                &host,
                &f.entry.sha256_hex,
            )?;
        }
    }
    Ok(())
}

/// Phase 3 — create plain new directories (shallow-first). A host that diverged
/// from `host@in` (now holds a file/symlink) is reported, not overwritten.
fn sod_dir_creates(
    ctx: &SodCtx,
    report: &mut DivergeReport,
    creates: &[String],
) -> Result<(), DispatchError> {
    for rel in creates {
        let host = classify_host_state(ctx.host_ws, rel);
        if host == HostState::Dir {
            continue; // already a dir
        }
        if matches_host_at_in(&host, ctx.baseline, rel) {
            if safety::host_target_is_symlink_safe(ctx.host_ws, rel) {
                let target = ctx.host_ws.join(rel);
                if let Err(e) = std::fs::create_dir_all(&target) {
                    return Err(host_io_err(&target, "dir create", &e));
                }
            }
        } else {
            report.diverged.push(report_only(
                rel,
                conflict_reason(&host, ctx.baseline, rel),
                &host,
            ));
        }
    }
    Ok(())
}

/// Phase 4 — write plain changed/new files. The host is overwritten only where
/// it still matches `host@in`; otherwise the node's bytes are diverged and the
/// host is kept.
fn sod_file_writes(
    ctx: &SodCtx,
    report: &mut DivergeReport,
    writes: &[&RemoteFile],
) -> Result<(), DispatchError> {
    for f in writes {
        let rel = f.rel.as_str();
        let host = classify_host_state(ctx.host_ws, rel);
        if let HostState::File { sha256_hex } = &host
            && *sha256_hex == f.entry.sha256_hex
        {
            continue; // host already holds the node's bytes
        }
        if matches_host_at_in(&host, ctx.baseline, rel) {
            if safety::host_target_is_symlink_safe(ctx.host_ws, rel) {
                write_host_file_atomic(ctx.host_ws, rel, &f.bytes)?;
            }
        } else {
            diverge_file(
                report,
                ctx,
                rel,
                &f.bytes,
                conflict_reason(&host, ctx.baseline, rel),
                &host,
                &f.entry.sha256_hex,
            )?;
        }
    }
    Ok(())
}

/// The classified work for a `SafeOrDiverge` write-back: which rels are plain
/// deletions / dir-creates / file-writes, which are file↔dir type changes, and
/// which baseline rels a remote symlink shadows. Computed up front so the
/// `max_files` cap can fail-fast before any host mutation.
struct SodPlan<'a> {
    /// Baseline files the node turned into a dir (file→dir), replaced atomically.
    file_to_dir: Vec<String>,
    /// Baseline dirs the node turned into a file (dir→file), replaced atomically.
    dir_to_file: Vec<&'a RemoteFile>,
    /// Plain deletions `(rel, is_dir)`, deepest-first.
    deletions: Vec<(String, bool)>,
    /// Plain new directories, shallow-first.
    dir_creates: Vec<String>,
    /// Plain changed/new files to write.
    file_writes: Vec<&'a RemoteFile>,
    /// Remote symlinks that shadow a baseline rel (reported, host untouched).
    shadowing_symlinks: Vec<String>,
    /// Total entries this write-back would mutate (drives the `max_files` cap).
    considered: usize,
}

/// Partition the remote delta into the `SafeOrDiverge` phase work-sets (pure —
/// no host mutation). Type-change rels are excluded from the plain
/// delete/create/write sets so phase 2 can replace them atomically.
fn sod_partition<'a>(
    baseline: &safety::Manifest,
    new_remote: &safety::Manifest,
    listing: &'a RemoteListing,
    ignore: &[String],
) -> SodPlan<'a> {
    // A rel survives the plain passes only if it is safe, not ignored, and not
    // hidden behind a remote symlink.
    let keep = |rel: &str| {
        safety::is_safe_relative(rel)
            && !safety::should_ignore(rel, ignore)
            && is_shadowed_by_symlink(rel, &listing.symlinks).is_none()
    };

    let file_to_dir: Vec<String> = baseline
        .files
        .keys()
        .filter(|rel| new_remote.dirs.contains(*rel) && keep(rel))
        .cloned()
        .collect();
    let dir_to_file: Vec<&RemoteFile> = listing
        .files
        .iter()
        .filter(|f| baseline.dirs.contains(&f.rel) && keep(&f.rel))
        .collect();

    // Plain deletions (baseline rels truly gone — not a type change), deepest-first.
    let mut deletions: Vec<(String, bool)> = Vec::new();
    for rel in baseline.files.keys() {
        if !new_remote.files.contains_key(rel) && !new_remote.dirs.contains(rel) && keep(rel) {
            deletions.push((rel.clone(), false));
        }
    }
    for rel in &baseline.dirs {
        if !new_remote.dirs.contains(rel) && !new_remote.files.contains_key(rel) && keep(rel) {
            deletions.push((rel.clone(), true));
        }
    }
    deletions.sort_by_key(|(r, _)| std::cmp::Reverse(r.len()));

    // Plain new dirs (not a file→dir type change), shallow-first.
    let mut dir_creates: Vec<String> = new_remote
        .dirs
        .difference(&baseline.dirs)
        .filter(|rel| !baseline.files.contains_key(*rel) && keep(rel))
        .cloned()
        .collect();
    dir_creates.sort_unstable();

    // Plain changed/new files (not a dir→file type change). `keep` excludes
    // unsafe/ignored/symlink-shadowed rels — without it a malformed listing
    // (symlink `d` + a listed child `d/x`) could write `d/x` over a host file.
    let file_writes: Vec<&RemoteFile> = listing
        .files
        .iter()
        .filter(|f| {
            keep(&f.rel)
                && !baseline.dirs.contains(&f.rel)
                && baseline
                    .files
                    .get(&f.rel)
                    .is_none_or(|m| m.sha256_hex != f.entry.sha256_hex)
        })
        .collect();

    // Remote symlinks that shadow a baseline rel (or subtree) — unsupported content.
    let shadowing_symlinks: Vec<String> = listing
        .symlinks
        .iter()
        .filter(|s| {
            let prefix = format!("{s}/");
            baseline
                .files
                .keys()
                .any(|r| r == *s || r.starts_with(&prefix))
                || baseline
                    .dirs
                    .iter()
                    .any(|r| r == *s || r.starts_with(&prefix))
        })
        .cloned()
        .collect();

    let considered = deletions.len()
        + file_to_dir.len()
        + dir_to_file.len()
        + dir_creates.len()
        + file_writes.len();

    SodPlan {
        file_to_dir,
        dir_to_file,
        deletions,
        dir_creates,
        file_writes,
        shadowing_symlinks,
        considered,
    }
}

/// `SafeOrDiverge` write-back: write a node's output back to the host **only where
/// the host still matches what `reconcile_in` uploaded** (`host@in`); where the
/// host changed concurrently, preserve the node's version under
/// `.ordius/diverged/<run>/<env>/<rel>` + a JSON report and leave the host alone.
///
/// Remote symlinks are unsupported content: never synced, never counted as a
/// deletion (a baseline rel they shadow is reported `remote_unsupported_symlink`,
/// host untouched). The four phases (deletions → type changes → dir creates →
/// file writes) keep parents/children from colliding; each mutation re-classifies
/// the host immediately before acting (the best-effort TOCTOU recheck). Returns
/// the advanced remote manifest (the new baseline) and the report.
#[allow(clippy::too_many_arguments)]
pub(super) async fn write_back_safe_or_diverge(
    factory: &Arc<dyn WorkspaceTransportFactory>,
    root: &str,
    host_ws: &Path,
    baseline: &safety::Manifest,
    ignore: &[String],
    max_files: usize,
    run_id: &str,
    env_id: &str,
) -> Result<(safety::Manifest, DivergeReport), DispatchError> {
    let t = factory.open().await?;
    let listing = list_remote_files(t, root).await?;

    // Advanced baseline (returned regardless of conflicts).
    let mut new_remote = safety::Manifest::new();
    new_remote.dirs.clone_from(&listing.dirs);
    for f in &listing.files {
        new_remote.files.insert(f.rel.clone(), f.entry.clone());
    }

    // Classify the work up front so the cap fails fast before any host mutation.
    let plan = sod_partition(baseline, &new_remote, &listing, ignore);
    if plan.considered > max_files {
        return Err(DispatchError::WorkspaceUnavailable {
            env_id: "<host>".into(),
            reason: format!(
                "SafeOrDiverge write-back would touch {} entries, exceeding max_files={max_files}",
                plan.considered
            ),
        });
    }

    let mut report = DivergeReport {
        run_id: run_id.to_string(),
        env_id: env_id.to_string(),
        diverged: Vec::new(),
    };
    for rel in &plan.shadowing_symlinks {
        report.diverged.push(DivergeEntry {
            rel: rel.clone(),
            reason: DivergeReason::RemoteUnsupportedSymlink,
            host_sha256: None,
            remote_sha256: None,
            diverged_path: None,
        });
    }

    // Four-phase application (each mutation re-classifies the host immediately
    // before acting — the best-effort TOCTOU recheck).
    let ctx = SodCtx {
        host_ws,
        run_id,
        env_id,
        baseline,
    };
    sod_deletions(&ctx, &mut report, &plan.deletions)?;
    sod_type_changes(&ctx, &mut report, &plan.file_to_dir, &plan.dir_to_file)?;
    sod_dir_creates(&ctx, &mut report, &plan.dir_creates)?;
    sod_file_writes(&ctx, &mut report, &plan.file_writes)?;

    if !report.diverged.is_empty() {
        write_diverge_report(host_ws, run_id, env_id, &report)?;
    }
    Ok((new_remote, report))
}

// ── SafeOrDiverge divergence artifacts ─────────────────────────────────────────

/// Why a file written back from the remote diverged from the host baseline and
/// could not be applied in place.
///
/// Serializes as `snake_case` for the on-disk `diverge-report.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum DivergeReason {
    /// The host file changed since the baseline (and so did the remote).
    HostModified,
    /// The host file was deleted since the baseline.
    HostDeleted,
    /// A new host file appeared where the remote wants to write.
    HostCreated,
    /// The host entry changed file-type (e.g. file → dir) since the baseline.
    HostTypeChanged,
    /// The host path is unsafe to write through (symlink/unreadable component).
    HostUnsafe,
    /// The remote deleted a file the host had concurrently modified.
    DeleteVsHostModified,
    /// A directory the remote deleted is non-empty on the host.
    DirDeleteNonempty,
    /// The remote returned a symlink, which write-back does not support.
    RemoteUnsupportedSymlink,
}

/// One diverged path in a [`DivergeReport`].
#[derive(Debug, Clone, Serialize)]
pub(super) struct DivergeEntry {
    /// Forward-slash relative path (from the workspace root) that diverged.
    pub(super) rel: String,
    /// Why it diverged.
    pub(super) reason: DivergeReason,
    /// Host baseline content hash, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) host_sha256: Option<String>,
    /// Remote content hash, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) remote_sha256: Option<String>,
    /// Relative path of the side-written divergence artifact, when one exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) diverged_path: Option<String>,
}

/// A run's diverged write-backs, serialized to
/// `.ordius/diverged/<run>/<env>/diverge-report.json`.
#[derive(Debug, Clone, Serialize)]
pub(super) struct DivergeReport {
    /// Run that produced the divergence.
    pub(super) run_id: String,
    /// Env whose write-back diverged.
    pub(super) env_id: String,
    /// One entry per diverged path.
    pub(super) diverged: Vec<DivergeEntry>,
}

/// Fail-closed reject for a divergence path that traverses a symlinked or
/// unreadable host component, mirroring [`safety::classify_artifact_path`].
fn reject_unsafe_artifact_path(artifact_rel: &str) -> DispatchError {
    DispatchError::WorkspaceUnavailable {
        env_id: "<host>".into(),
        reason: format!(
            "refusing to write divergence artifact through a symlinked/unreadable path: \
             {artifact_rel}"
        ),
    }
}

/// Reject an empty `run_id`/`env_id`: `encode_segment("")` is `""`, which would
/// collapse the `.ordius/diverged/<run>/<env>/` path layout.
pub(super) fn reject_empty_id(kind: &str) -> DispatchError {
    DispatchError::WorkspaceUnavailable {
        env_id: "<host>".into(),
        reason: format!("refusing to write divergence artifact: empty {kind}"),
    }
}

/// Atomically write `bytes` to `host_ws/<artifact_rel>`, creating each missing
/// `.ordius/diverged/...` parent component one at a time and re-checking — with
/// `symlink_metadata` immediately before each `create_dir` — that the component
/// is a real directory, not a symlink. This narrows (but cannot fully close,
/// absent an `openat`-style primitive) the TOCTOU window where a concurrent
/// actor swaps a freshly-created dir for a symlink that would redirect the write
/// outside the workspace. The leaf is written via temp+rename for atomicity.
///
/// Fail-closed: any component that is, or becomes, a symlink/non-directory →
/// `Err`; an unreadable component (non-`NotFound` `symlink_metadata` error) →
/// `Err`.
fn write_artifact_atomic(
    host_ws: &Path,
    artifact_rel: &str,
    bytes: &[u8],
) -> Result<(), DispatchError> {
    // Preflight the whole path (the existing-components symlink/unreadable gate).
    match safety::classify_artifact_path(host_ws, artifact_rel) {
        safety::ArtifactPathState::Ok => {},
        safety::ArtifactPathState::Symlink | safety::ArtifactPathState::Unreadable => {
            return Err(reject_unsafe_artifact_path(artifact_rel));
        },
    }

    let target = host_ws.join(artifact_rel);

    // Create each missing parent component individually, re-checking right before
    // each `create_dir` that the existing/just-created component is a real dir.
    let mut cur = host_ws.to_path_buf();
    for comp in Path::new(artifact_rel).components() {
        match comp {
            std::path::Component::Normal(c) => cur.push(c),
            std::path::Component::CurDir => continue,
            // is_safe_relative / the preflight reject these; fail closed anyway.
            _ => return Err(reject_unsafe_artifact_path(artifact_rel)),
        }
        // Only walk parent components here; the leaf is written via temp+rename.
        if cur == target {
            break;
        }
        match std::fs::symlink_metadata(&cur) {
            Ok(md) if md.file_type().is_symlink() || !md.file_type().is_dir() => {
                return Err(reject_unsafe_artifact_path(artifact_rel));
            },
            Ok(_) => {},
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir(&cur)
                    .map_err(|e| host_io_err(&cur, "create diverged dir", &e))?;
                // Re-check the component we just created is a real dir (not a
                // symlink a racing actor planted between the check and mkdir).
                match std::fs::symlink_metadata(&cur) {
                    Ok(md) if md.file_type().is_dir() => {},
                    _ => return Err(reject_unsafe_artifact_path(artifact_rel)),
                }
            },
            Err(_) => return Err(reject_unsafe_artifact_path(artifact_rel)),
        }
    }

    let tmp = tmp_sibling(&target);
    std::fs::write(&tmp, bytes).map_err(|e| host_io_err(&tmp, "write diverged temp file", &e))?;
    std::fs::rename(&tmp, &target)
        .map_err(|e| host_io_err(&target, "rename diverged into place", &e))?;
    // Residual race: between each `symlink_metadata` recheck and the subsequent
    // `create_dir`/write there is a sub-syscall gap an attacker could still win.
    // Closing it fully needs an `openat`/`O_NOFOLLOW`-style primitive we don't
    // have on the Windows host; this keeps the window as small as practical.
    Ok(())
}

/// Write `bytes` to a side artifact under
/// `.ordius/diverged/<enc(run_id)>/<enc(env_id)>/<rel>` instead of clobbering
/// `host_ws/<rel>` in place, returning the forward-slash artifact rel path.
///
/// Fail-closed: rejects an empty `run_id`/`env_id`, and (via
/// [`write_artifact_atomic`]) rejects when any existing component of the artifact
/// path is a symlink or unreadable.
pub(super) fn write_diverged_artifact(
    host_ws: &Path,
    run_id: &str,
    env_id: &str,
    rel: &str,
    bytes: &[u8],
) -> Result<String, DispatchError> {
    if run_id.is_empty() {
        return Err(reject_empty_id("run_id"));
    }
    if env_id.is_empty() {
        return Err(reject_empty_id("env_id"));
    }
    let artifact_rel = format!(
        ".ordius/diverged/{}/{}/{}",
        safety::encode_segment(run_id),
        safety::encode_segment(env_id),
        rel
    );

    write_artifact_atomic(host_ws, &artifact_rel, bytes)?;
    Ok(artifact_rel)
}

/// Write `report` as pretty JSON to
/// `.ordius/diverged/<enc(run_id)>/<enc(env_id)>/diverge-report.json`.
///
/// The caller guards `!report.diverged.is_empty()`. Same fail-closed empty-id +
/// per-component symlink gate and atomic temp+rename write as
/// [`write_diverged_artifact`] (both go through [`write_artifact_atomic`]).
pub(super) fn write_diverge_report(
    host_ws: &Path,
    run_id: &str,
    env_id: &str,
    report: &DivergeReport,
) -> Result<(), DispatchError> {
    if run_id.is_empty() {
        return Err(reject_empty_id("run_id"));
    }
    if env_id.is_empty() {
        return Err(reject_empty_id("env_id"));
    }
    let report_rel = format!(
        ".ordius/diverged/{}/{}/diverge-report.json",
        safety::encode_segment(run_id),
        safety::encode_segment(env_id),
    );

    let json =
        serde_json::to_vec_pretty(report).map_err(|e| DispatchError::WorkspaceUnavailable {
            env_id: "<host>".into(),
            reason: format!("serialize diverge report: {e}"),
        })?;

    write_artifact_atomic(host_ws, &report_rel, &json)
}

// ── host@in conflict spine ──────────────────────────────────────────────────────

/// The live shape of a host workspace path, captured for conflict detection.
///
/// Produced by [`classify_host_state`] and compared against the `host@in`
/// baseline manifest via [`matches_host_at_in`] to answer: "is the host path
/// still byte-and-type identical to what we uploaded at the start of the run?".
/// `UnsafeSymlink` and `Unreadable` are distinct *fail-closed* states — a path
/// we cannot safely classify must never be treated as "unchanged".
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum HostState {
    /// The path does not exist on the host.
    Absent,
    /// A regular file with the given lowercase-hex SHA-256 of its bytes
    /// (computed via [`safety::sha256_hex`], so it is comparable to a manifest
    /// [`safety::FileEntry::sha256_hex`]).
    File {
        /// Lowercase-hex SHA-256 of the file contents.
        sha256_hex: String,
    },
    /// A directory.
    Dir,
    /// A component of the path (including the terminal entry) is a symlink, so
    /// the path cannot be classified or mutated safely. Fail closed.
    UnsafeSymlink,
    /// The path's metadata could not be read for a reason other than "does not
    /// exist" (e.g. a permission error), or it is a non-file/non-dir entry
    /// (fifo, socket, …) we cannot safely mutate. Fail closed.
    Unreadable,
}

/// Classify the live shape of `host_ws/rel` for conflict detection.
///
/// First defers to [`safety::classify_artifact_path`] for the symlink-traversal
/// and unreadable-component checks (so the same fail-closed rules used for
/// write-back govern conflict detection). Only when every component is clean
/// does it stat the terminal path: missing → [`HostState::Absent`], a dir →
/// [`HostState::Dir`], a regular file → its content hash, and anything else
/// (other stat error, fifo/socket/…, or a hash read error) →
/// [`HostState::Unreadable`].
pub(super) fn classify_host_state(host_ws: &std::path::Path, rel: &str) -> HostState {
    use std::io::ErrorKind;

    match safety::classify_artifact_path(host_ws, rel) {
        safety::ArtifactPathState::Symlink => return HostState::UnsafeSymlink,
        safety::ArtifactPathState::Unreadable => return HostState::Unreadable,
        safety::ArtifactPathState::Ok => {},
    }

    let target = host_ws.join(rel);
    let md = match std::fs::symlink_metadata(&target) {
        Ok(md) => md,
        Err(e) if e.kind() == ErrorKind::NotFound => return HostState::Absent,
        Err(_) => return HostState::Unreadable,
    };

    if md.is_dir() {
        HostState::Dir
    } else if md.is_file() {
        safety::hash_file(&target).map_or(HostState::Unreadable, |sha256_hex| HostState::File {
            sha256_hex,
        })
    } else {
        // fifo / socket / device / etc. — cannot safely mutate. Fail closed.
        HostState::Unreadable
    }
}

/// Whether `state` is byte-and-type identical to the `host@in` `baseline` at
/// `rel` — i.e. the host path is unchanged since upload.
///
/// `UnsafeSymlink` and `Unreadable` always return `false` (fail closed): a path
/// we cannot trust to be unchanged is treated as a conflict.
pub(super) fn matches_host_at_in(
    state: &HostState,
    baseline: &safety::Manifest,
    rel: &str,
) -> bool {
    match state {
        HostState::Absent => !baseline.files.contains_key(rel) && !baseline.dirs.contains(rel),
        HostState::File { sha256_hex } => baseline
            .files
            .get(rel)
            .is_some_and(|e| &e.sha256_hex == sha256_hex),
        HostState::Dir => baseline.dirs.contains(rel),
        HostState::UnsafeSymlink | HostState::Unreadable => false,
    }
}
