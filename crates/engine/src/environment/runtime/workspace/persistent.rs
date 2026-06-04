//! Persistent-workspace host→remote sync — additive upload that preserves
//! foreign remote state.
//!
//! Split out of [`super::manager`]: a PERSISTENT root (a template without
//! `{{run.id}}`, H5) is reused across runs, so `reconcile_in` may not delete
//! remote-only entries. [`sync_remote_additive`] uploads/overwrites the host's
//! files and creates its dirs, but never removes a foreign file or non-empty
//! dir; its obstruction pass ([`clear_additive_obstructions`]) clears only
//! content-less blockers (escape symlinks, empty dirs at a host-file rel) and
//! fails closed on a genuine conflict.
//!
//! [`advance_host_at_in_force`] advances the `host_at_in` baseline by exactly
//! the remote delta after a `Force` write-back (keeping preserved foreign files
//! out of the baseline). [`LockOwner`] is the JSON record persisted under the
//! held remote `.ordius.lock` so a contending run can name who holds it.
//!
//! `sync_remote_additive` is called by `manager::reconcile_in_persistent`,
//! `advance_host_at_in_force` by `manager::reconcile_write_back`, and `LockOwner`
//! is constructed by `manager::acquire_persistent_lock`.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use crate::environment::runtime::error::DispatchError;

use super::manager::{RemoteListing, list_remote_files};
use super::safety;
use super::transport::{WorkspaceTransport, WorkspaceTransportFactory};

/// Persisted in `<root>/.ordius.lock/owner.json` so a contending run can name who
/// holds the lock. (H5 persistent-reuse lock.)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(super) struct LockOwner {
    /// Top-level run id (child workflows inherit the parent snapshot).
    pub(super) top_run_id: String,
    /// The specific (possibly child) run that acquired the lock.
    pub(super) current_run_id: String,
    /// Host that ran the acquiring process.
    pub(super) host: String,
    /// ISO-8601 acquisition time (the run's `started_at`).
    pub(super) started_at: String,
}

/// Additive host→remote sync for a PERSISTENT root: upload/overwrite the host's
/// files, create the host's dirs, but NEVER delete a remote-only file or
/// non-empty dir (preserve foreign state — design D1).
///
/// Returns `(host_at_in, last_remote)`:
/// - `host_at_in` = the manifest of bytes WE uploaded (host files + the host's
///   dirs). Drives `SafeOrDiverge` host-conflict checks only.
/// - `last_remote` = `host_at_in` PLUS the preserved remote-only entries from the
///   listing (foreign files/dirs not present in the host walk). Drives
///   remote-delta detection in `reconcile_out` so a foreign file is never seen as
///   a new node output.
///
/// Mirrors `manager::reset_remote_to_host`'s structure but is additive — no
/// remote-only deletion. The obstruction pass (design §7 step 4) removes
/// only entries that would block a host write and have no content of their own
/// (escape symlinks, empty dirs at a host-file rel); a foreign file at a host-dir
/// rel or a non-empty dir at a host-file rel is a genuine conflict and fails
/// closed.
///
/// Uploads use [`WorkspaceTransport::upload_file_atomic_via`] with a temp under
/// the held lock dir (`<root>/.ordius.lock/tmp`), so a pre-existing foreign
/// `<target>.ordius.tmp` is never clobbered (§7.1).
pub(super) async fn sync_remote_additive(
    factory: &Arc<dyn WorkspaceTransportFactory>,
    root: &str,
    host_ws: &Path,
) -> Result<(safety::Manifest, safety::Manifest), DispatchError> {
    let t = factory.open().await?;
    t.mkdir(root).await?;
    // Lock-dir temp scratch for collision-free uploads. Idempotent: in the real
    // flow the lock dir already exists from acquisition.
    let temp_dir_rel = format!("{root}/.ordius.lock/tmp");
    t.mkdir(&temp_dir_rel).await?;

    // Host targets (forward-slash rels, ignores applied — same as reset).
    let entries = safety::walk_workspace(host_ws)?;
    let target_files: HashSet<&str> = entries
        .iter()
        .filter(|e| e.kind == safety::EntryKind::File)
        .map(|e| e.rel_path.as_str())
        .collect();
    let target_dirs: HashSet<&str> = entries
        .iter()
        .filter(|e| e.kind == safety::EntryKind::Dir)
        .map(|e| e.rel_path.as_str())
        .collect();

    // Remote snapshot via a second transport: `list_remote_files` consumes the
    // Box, downloads + hashes every foreign file (so `last_remote` carries
    // correct hashes), and already drops reserved rels (`.ordius.lock`, default
    // ignores). Defensively re-filter below in case a future fake omits the prune.
    let listing = {
        let t2 = factory.open().await?;
        list_remote_files(t2, root).await?
    };

    // Clear only the obstructions that block a host write and own no content
    // (escape symlinks, empty dirs at a host-file rel); fail closed on a genuine
    // foreign conflict (design §7 step 4).
    let t = clear_additive_obstructions(t, root, &target_files, &target_dirs, &listing).await?;

    // ── Upload/create the host tree (no deletion of remote-only entries). ──
    //
    // The walk is sorted, so dirs precede their nested entries. Cap-check the
    // bytes actually read and hash the SENT bytes into `host_at_in` (mirrors
    // reset). Uploads write a temp under the held lock dir, never a foreign-
    // clobbering sibling temp.
    let mut tracker = safety::CapTracker::new(safety::UploadCaps::default());
    let mut host_at_in = safety::Manifest::new();
    for entry in &entries {
        match entry.kind {
            safety::EntryKind::Dir => {
                t.mkdir(&format!("{root}/{}", entry.rel_path)).await?;
                host_at_in.dirs.insert(entry.rel_path.clone());
            },
            safety::EntryKind::File => {
                let bytes = safety::read_within_caps(&entry.abs, &mut tracker)?;
                let remote_path = format!("{root}/{}", entry.rel_path);
                t.upload_file_atomic_via(&remote_path, &temp_dir_rel, &bytes)
                    .await?;
                host_at_in.files.insert(
                    entry.rel_path.clone(),
                    safety::FileEntry {
                        sha256_hex: safety::sha256_hex(&bytes),
                        size: bytes.len() as u64,
                        mode: entry.mode,
                    },
                );
            },
        }
    }

    // ── Build last_remote = host_at_in ∪ preserved remote-only entries. ──
    //
    // A listing entry is preserved iff it is NOT a host target and NOT reserved
    // (the reserved filter is defensive — `list_remote_files` already drops them).
    let mut last_remote = host_at_in.clone();
    for f in &listing.files {
        if host_at_in.files.contains_key(&f.rel)
            || host_at_in.dirs.contains(&f.rel)
            || safety::is_reserved_remote_rel(&f.rel)
        {
            continue;
        }
        last_remote.files.insert(f.rel.clone(), f.entry.clone());
    }
    for d in &listing.dirs {
        // Also skip dirs that are host FILE targets: the obstruction pass may have
        // removed an empty remote dir at a host-file rel, but the pre-upload
        // `listing.dirs` still lists it — re-adding it would leave `last_remote`
        // holding BOTH file `d` and dir `d`.
        if host_at_in.dirs.contains(d)
            || host_at_in.files.contains_key(d)
            || safety::is_reserved_remote_rel(d)
        {
            continue;
        }
        last_remote.dirs.insert(d.clone());
    }

    Ok((host_at_in, last_remote))
}

/// Obstruction pass for [`sync_remote_additive`] (design §7 step 4) — remove only
/// remote entries that block a host write and own no content, fail closed on a
/// genuine foreign conflict. Never deletes a remote-only file or a non-empty dir.
///
/// 1. A remote symlink AT a host-file rel OR on an ancestor of it → remove it
///    (escape risk; the link has no content of its own, its target is untouched).
/// 2. A remote dir exactly at a host-FILE rel → non-recursive `remove_dir`. `Ok`
///    ⇒ it was empty, proceed; `Err` ⇒ foreign content under it (do NOT infer
///    emptiness from the listing) → `WorkspaceUnavailable` conflict.
/// 3. A remote file exactly at a host-DIR rel → `WorkspaceUnavailable` conflict
///    (we will not delete a foreign file to make room for a dir).
///
/// `listing` is the reserved-filtered [`RemoteListing`]; the reserved re-checks
/// are defensive in case a future transport omits the prune. Takes the transport
/// by value and returns it so the caller reuses the same session for uploads (a
/// borrowed `&dyn` would make the future `!Send`, since the trait is `Send` but
/// not `Sync`).
async fn clear_additive_obstructions(
    t: Box<dyn WorkspaceTransport>,
    root: &str,
    target_files: &HashSet<&str>,
    target_dirs: &HashSet<&str>,
    listing: &RemoteListing,
) -> Result<Box<dyn WorkspaceTransport>, DispatchError> {
    let remote_dirs: HashSet<&str> = listing.dirs.iter().map(String::as_str).collect();
    let remote_files: HashSet<&str> = listing.files.iter().map(|f| f.rel.as_str()).collect();

    // 1. Escape symlinks (at the rel or on an ancestor). Collect, dedup, remove.
    //    Cover BOTH file targets and dir targets: a remote symlink at a host-dir
    //    rel (even when that dir is empty) must be cleared before `mkdir` runs,
    //    otherwise `mkdir` follows the symlink and host content escapes the root.
    let mut symlinks_to_remove: HashSet<&String> = HashSet::new();
    for target_rel in target_files.iter().chain(target_dirs.iter()) {
        for sym in &listing.symlinks {
            if safety::is_reserved_remote_rel(sym) {
                continue;
            }
            if *target_rel == sym.as_str() || target_rel.starts_with(&format!("{sym}/")) {
                symlinks_to_remove.insert(sym);
            }
        }
    }
    for sym in symlinks_to_remove {
        t.remove_file(&format!("{root}/{sym}")).await?;
    }

    // 2. Remote dir at a host-file rel → try remove_dir; any failure is a conflict.
    for file_rel in target_files {
        if safety::is_reserved_remote_rel(file_rel) {
            continue;
        }
        if remote_dirs.contains(*file_rel)
            && let Err(e) = t.remove_dir(&format!("{root}/{file_rel}")).await
        {
            return Err(DispatchError::WorkspaceUnavailable {
                env_id: "<remote>".into(),
                reason: format!(
                    "remote directory blocks host file `{file_rel}` (non-empty); resolve manually: {e}"
                ),
            });
        }
    }

    // 3. Remote file at a host-dir rel → conflict.
    for dir_rel in target_dirs {
        if safety::is_reserved_remote_rel(dir_rel) {
            continue;
        }
        if remote_files.contains(*dir_rel) {
            return Err(DispatchError::WorkspaceUnavailable {
                env_id: "<remote>".into(),
                reason: format!("remote file blocks host directory `{dir_rel}`; resolve manually"),
            });
        }
    }

    Ok(t)
}

/// Advance the `host_at_in` baseline after a `Force` write-back.
///
/// `Force` mirrors the remote delta (`old_remote` → `new_remote`) onto the host,
/// so `host_at_in` advances by exactly that delta: changed/added file rels take
/// the new entry, removed file rels are dropped, and dir adds/removes are mirrored
/// in `dirs`. Rels equal in `old_remote` and `new_remote` are NOT touched — this
/// is what keeps preserved foreign files (persistent reuse, a later task) out of
/// `host_at_in` even though they appear in both remote snapshots. For the
/// ephemeral path `host_at_in == old_remote`, so the result equals `new_remote`
/// (both baselines advance together — no behaviour change).
pub(super) fn advance_host_at_in_force(
    host_at_in: &safety::Manifest,
    old_remote: &safety::Manifest,
    new_remote: &safety::Manifest,
) -> safety::Manifest {
    let mut advanced = host_at_in.clone();

    // File deltas: a rel whose entry changed/appeared takes the new entry; a rel
    // that disappeared from the remote is dropped.
    for (rel, entry) in &new_remote.files {
        if old_remote.files.get(rel) != Some(entry) {
            advanced.files.insert(rel.clone(), entry.clone());
            advanced.dirs.remove(rel);
        }
    }
    for rel in old_remote.files.keys() {
        if !new_remote.files.contains_key(rel) {
            advanced.files.remove(rel);
        }
    }

    // Dir deltas: gained dirs are added, lost dirs removed.
    for rel in new_remote.dirs.difference(&old_remote.dirs) {
        advanced.dirs.insert(rel.clone());
        advanced.files.remove(rel);
    }
    for rel in old_remote.dirs.difference(&new_remote.dirs) {
        advanced.dirs.remove(rel);
    }

    advanced
}
