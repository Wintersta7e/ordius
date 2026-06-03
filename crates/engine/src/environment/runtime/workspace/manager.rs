/// Workspace manager — H2-T2 scope.
///
/// H1: `resolve_cwd` delegated to `dispatcher.translate_path` for all
/// bindings (behaviour unchanged for Local/WSL/BindMount/Shared/Translated).
///
/// H2-T2: `WorkspaceBinding::Sync` with `SyncStrategy::Sftp` and a
/// `{{run.id}}`-containing template is handled — ephemeral upload via SFTP,
/// singleflight-serialised per `(EnvId, host_ws)` key so parallel nodes on
/// the same env share one upload.
///
/// H2-T3: `teardown_all` writes changed/new files back to the host
/// (`None`/`Force`, skipped on user cancel) and deletes the ephemeral root.
///
/// Not yet implemented (deferred):
/// - Persistent workspace reuse (template without `{{run.id}}`).
/// - `SafeOrDiverge` write-back (rejected upstream in `resolve_cwd`).
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::{Mutex, OnceCell};

use crate::environment::runtime::dispatcher::Dispatcher;
use crate::environment::runtime::env::{EnvId, WorkspaceBinding, WriteBackPolicy};
use crate::environment::runtime::error::DispatchError;
use crate::environment::runtime::transport::EnvPath;

use super::safety;
use super::transport::{FileKind, WorkspaceTransport, WorkspaceTransportFactory};

// ── Type aliases ──────────────────────────────────────────────────────────────

/// Inner map type for the singleflight prepared-workspace registry.
type PreparedMap = HashMap<(EnvId, PathBuf), Arc<OnceCell<Arc<PreparedWorkspace>>>>;

// ── Types ─────────────────────────────────────────────────────────────────────

/// Terminal classification handed to [`WorkspaceManager::teardown_all`]
/// so write-back/cleanup policy can branch on how the run ended.
///
/// Derived from the run's terminal status (or a panic/cancel signal
/// when the run loop unwinds before producing a `RunSummary`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    /// Clean completion (`status == "done"`).
    Completed,
    /// Node failure or stall (`status == "error"`), or a panic.
    Failed,
    /// User cancellation (`status == "stopped"`).
    CancelledByUser,
}

/// Per-node lifecycle of the uploaded workspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lifecycle {
    /// Workspace root contains `{{run.id}}` — unique per run, deleted on teardown.
    Ephemeral,
    /// Workspace root is stable across runs — reuse / incremental sync.
    Persistent,
}

/// State captured once a workspace has been uploaded for one `(EnvId, host_ws)` pair.
///
/// Stored in `WorkspaceManager::prepared` so teardown (H2-T3) can locate it.
struct PreparedWorkspace {
    /// Absolute env-side root that was created and populated.
    env_side_root: String,
    lifecycle: Lifecycle,
    /// Policy the caller specified; teardown branches on it for write-back.
    write_back: WriteBackPolicy,
    /// Manifest of every file that was uploaded. Teardown diffs the env-side
    /// tree against it to find files the run changed or created.
    upload_manifest: safety::Manifest,
    /// Host workspace path; write-back targets are resolved relative to it.
    host_ws: PathBuf,
    /// Factory used to reopen a transport for write-back + ephemeral delete.
    /// Captured at upload time so teardown needs no `Dispatcher` handle.
    transport_factory: Arc<dyn WorkspaceTransportFactory>,
}

// ── Run scope ─────────────────────────────────────────────────────────────────

/// Lightweight view of the current run's identity; passed to
/// `resolve_cwd` so it can expand `env_path_template`.
pub struct RunScope<'a> {
    /// Stable run identifier.
    pub run_id: &'a str,
    /// Workflow id.
    pub workflow_id: &'a str,
    /// Human-readable workflow name.
    pub workflow_name: &'a str,
    /// ISO-8601 run start time.
    pub started_at_iso: &'a str,
}

// ── Manager ───────────────────────────────────────────────────────────────────

/// Run-tree-scoped owner of workspace sync policy.
///
/// Invariant: `prepared` grows monotonically during a run; it is read (but
/// not mutated) by `teardown_all`. The map lock is only held while inserting
/// a new `OnceCell`; actual upload I/O runs outside the lock.
#[derive(Debug)]
pub struct WorkspaceManager {
    /// Per-`(EnvId, host_ws)` upload cell.  Inserting the cell is atomic;
    /// the cell itself serialises the upload so parallel node executions on
    /// the same env share one upload without holding the map lock across I/O.
    prepared: Mutex<PreparedMap>,

    /// Test-only seam: records the last [`RunOutcome`] passed to
    /// [`Self::teardown_all`]. Lets run-loop tests observe that
    /// teardown fired with the correct outcome on every exit path.
    #[cfg(any(test, feature = "testing"))]
    pub last_outcome: std::sync::Mutex<Option<RunOutcome>>,
}

impl std::fmt::Debug for PreparedWorkspace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedWorkspace")
            .field("env_side_root", &self.env_side_root)
            .field("lifecycle", &self.lifecycle)
            .finish_non_exhaustive()
    }
}

impl Default for WorkspaceManager {
    fn default() -> Self {
        Self {
            prepared: Mutex::new(HashMap::new()),
            #[cfg(any(test, feature = "testing"))]
            last_outcome: std::sync::Mutex::new(None),
        }
    }
}

impl WorkspaceManager {
    /// Construct a new `WorkspaceManager`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Resolve the working directory inside the target env.
    ///
    /// - `Shared`, `Translated`, `BindMount` — delegates to
    ///   `dispatcher.translate_path` (H1 behaviour, unchanged).
    /// - `Sync { strategy: Sftp, write_back: None|Force, env_path_template }`
    ///   where the template contains `{{run.id}}` — uploads the workspace over
    ///   SFTP, singleflight-serialised per `(EnvId, host_ws)` pair, returns
    ///   the env-side root.
    /// - Everything else returns `Err(DispatchError::Unsupported)`.
    pub async fn resolve_cwd(
        &self,
        dispatcher: &dyn Dispatcher,
        binding: &WorkspaceBinding,
        host_ws: &Path,
        run: &RunScope<'_>,
    ) -> Result<EnvPath, DispatchError> {
        use crate::environment::runtime::env::SyncStrategy;

        match binding {
            // H1 paths — delegate to translate_path unchanged.
            WorkspaceBinding::Shared
            | WorkspaceBinding::Translated
            | WorkspaceBinding::BindMount { .. }
            | WorkspaceBinding::Unsupported => dispatcher.translate_path(host_ws),

            WorkspaceBinding::Sync {
                env_path_template,
                strategy,
                write_back,
            } => {
                // Guard: only SFTP sync is implemented.
                if *strategy != SyncStrategy::Sftp {
                    return Err(DispatchError::Unsupported(
                        "only SFTP workspace sync is implemented".into(),
                    ));
                }
                // Guard: SafeOrDiverge write-back is deferred.
                if matches!(write_back, WriteBackPolicy::SafeOrDiverge { .. }) {
                    return Err(DispatchError::Unsupported(
                        "SafeOrDiverge write-back is deferred to a later phase".into(),
                    ));
                }

                // Guard: catch the common typo `{{run_id}}` (underscore) before
                // expand_env_root turns it into an opaque "unknown namespace" error.
                if env_path_template.contains("{{run_id}}")
                    && !env_path_template.contains("{{run.id}}")
                {
                    return Err(DispatchError::Unsupported(
                        "the per-run placeholder is {{run.id}}, not {{run_id}}".into(),
                    ));
                }

                let env_side_root = expand_env_root(env_path_template, run, host_ws)?;

                // Ephemeral iff the template contains `{{run.id}}` — the only
                // supported per-run discriminant marker.
                let lifecycle = if env_path_template.contains("{{run.id}}") {
                    Lifecycle::Ephemeral
                } else {
                    Lifecycle::Persistent
                };

                if lifecycle == Lifecycle::Persistent {
                    return Err(DispatchError::Unsupported(
                        "persistent workspace reuse is deferred to a later phase".into(),
                    ));
                }

                // Singleflight: get-or-insert a OnceCell for this key,
                // clone the Arc, DROP the map lock, THEN drive the upload.
                let key = (dispatcher.info().id.clone(), host_ws.to_path_buf());
                let cell = {
                    let mut map = self.prepared.lock().await;
                    Arc::clone(map.entry(key).or_insert_with(|| Arc::new(OnceCell::new())))
                };

                let pw = cell
                    .get_or_try_init(|| async {
                        self.upload(
                            dispatcher,
                            &env_side_root,
                            host_ws,
                            write_back.clone(),
                            lifecycle,
                        )
                        .await
                    })
                    .await?;

                Ok(EnvPath::new(pw.env_side_root.clone()))
            },
        }
    }

    /// Perform the actual SFTP upload for one `(env, host_ws)` pair.
    ///
    /// Called at most once per key thanks to the `OnceCell` guard in
    /// `resolve_cwd`.
    async fn upload(
        &self,
        dispatcher: &dyn Dispatcher,
        env_side_root: &str,
        host_ws: &Path,
        write_back: WriteBackPolicy,
        lifecycle: Lifecycle,
    ) -> Result<Arc<PreparedWorkspace>, DispatchError> {
        let factory = dispatcher.workspace_transport().ok_or_else(|| {
            DispatchError::Unsupported("environment has no workspace transport".into())
        })?;
        let t = factory.open().await?;

        // Create the root, then upload. A failure after mkdir leaves a partial
        // remote root; the never-completed singleflight cell is dropped, so
        // teardown never sees the root. Clean it up best-effort here before
        // propagating, otherwise the ephemeral dir leaks on the remote.
        t.mkdir(env_side_root).await?;
        match upload_files(t, env_side_root, host_ws).await {
            Ok(upload_manifest) => Ok(Arc::new(PreparedWorkspace {
                env_side_root: env_side_root.to_string(),
                lifecycle,
                write_back,
                upload_manifest,
                host_ws: host_ws.to_path_buf(),
                transport_factory: factory,
            })),
            Err(e) => {
                if let Err(cleanup_err) = remove_tree(factory.as_ref(), env_side_root).await {
                    tracing::warn!(
                        env_root = env_side_root,
                        error = %cleanup_err,
                        "failed to remove partial upload root after upload error"
                    );
                }
                Err(e)
            },
        }
    }

    /// Tear down every workspace prepared during the run.
    ///
    /// Fires on every run-loop exit path (success, error, or panic), before the
    /// engine's sender/token/lock cleanup. For each prepared workspace it writes
    /// changed files back to the host (per [`WriteBackPolicy`], skipped entirely
    /// on user cancel) and deletes the ephemeral env-side root.
    ///
    /// Best-effort and panic-free: per-env errors are logged and swallowed so a
    /// failure on one workspace never aborts cleanup of the others nor unwinds
    /// into the run-loop teardown path.
    pub async fn teardown_all(&self, outcome: RunOutcome) {
        #[cfg(any(test, feature = "testing"))]
        {
            *self.last_outcome.lock().unwrap() = Some(outcome);
        }

        // Drain the map — teardown owns these entries; nothing reads `prepared`
        // after the run loop exits. Skip cells whose upload never completed.
        let prepared: Vec<Arc<PreparedWorkspace>> = {
            let mut map = self.prepared.lock().await;
            std::mem::take(&mut *map)
                .into_values()
                .filter_map(|cell| cell.get().cloned())
                .collect()
        };

        for pw in prepared {
            if let Err(e) = teardown_one(&pw, outcome).await {
                tracing::warn!(
                    env_root = %pw.env_side_root,
                    error = %e,
                    "workspace teardown failed; continuing (best-effort)"
                );
            }
        }
    }
}

// ── Teardown helpers ──────────────────────────────────────────────────────────

/// Write back (unless user-cancelled) then delete the ephemeral root for one
/// prepared workspace. Cleanup is attempted even if write-back fails; the first
/// error encountered is returned for the caller to log.
/// Walk + cap-check + upload every file under `host_ws`, building the manifest
/// from the EXACT bytes uploaded.
///
/// Hashing the sent bytes (rather than re-reading the file) means a concurrent
/// host edit during the run can't make write-back mistake unchanged remote
/// content for "changed" and clobber the newer host file. Consumes the
/// transport; the caller `mkdir`s the root first and cleans up on error.
async fn upload_files(
    t: Box<dyn WorkspaceTransport>,
    env_side_root: &str,
    host_ws: &Path,
) -> Result<safety::Manifest, DispatchError> {
    let entries = safety::walk_workspace(host_ws)?;

    // Account caps against the bytes ACTUALLY read (bounded), not the walk's
    // stale stat — a file that grew since the walk can't bypass the cap.
    let mut tracker = safety::CapTracker::new(safety::UploadCaps::default());
    let mut manifest = safety::Manifest::new();
    for entry in &entries {
        let bytes = safety::read_within_caps(&entry.abs, &mut tracker)?;
        let remote_path = format!("{env_side_root}/{}", entry.rel_path);
        t.upload_file(&remote_path, &bytes).await?;
        manifest.insert(
            entry.rel_path.clone(),
            safety::FileEntry {
                sha256_hex: safety::sha256_hex(&bytes),
                size: bytes.len() as u64,
                mode: entry.mode,
            },
        );
    }
    Ok(manifest)
}

async fn teardown_one(pw: &PreparedWorkspace, outcome: RunOutcome) -> Result<(), DispatchError> {
    // Write-back is skipped entirely on user cancellation.
    let write_res = if outcome == RunOutcome::CancelledByUser {
        Ok(())
    } else {
        write_back(pw).await
    };

    // Ephemeral cleanup always runs — even on cancel or after a write-back error.
    let cleanup_res = if pw.lifecycle == Lifecycle::Ephemeral {
        remove_tree(pw.transport_factory.as_ref(), &pw.env_side_root).await
    } else {
        Ok(())
    };

    // Surface the first error (write-back takes precedence), but only after
    // cleanup has been attempted.
    write_res.and(cleanup_res)
}

/// Copy env-side files the run changed or created back into the host workspace.
///
/// `Force` writes back any file absent from the upload manifest or whose content
/// hash differs from it, honouring the policy's ignore globs. `None` is a no-op.
/// `SafeOrDiverge` cannot reach teardown (`resolve_cwd` rejects it before upload);
/// it is treated defensively as "no write-back".
///
/// Ignore globs use single-segment `*` semantics (see `safety::should_ignore`):
/// `*.log` matches `debug.log` but not `logs/debug.log` — use `**/*.log` for
/// nested matches.
async fn write_back(pw: &PreparedWorkspace) -> Result<(), DispatchError> {
    let ignore = match &pw.write_back {
        WriteBackPolicy::Force { ignore } => ignore.as_slice(),
        WriteBackPolicy::None | WriteBackPolicy::SafeOrDiverge { .. } => return Ok(()),
    };

    // A fresh transport per sync phase (the factory's documented contract).
    let t = pw.transport_factory.open().await?;
    let prefix = format!("{}/", pw.env_side_root);

    for entry in t.list_tree(&pw.env_side_root).await? {
        if entry.kind != FileKind::File {
            continue;
        }
        // Map the absolute env-side path back to a host-relative path.
        let Some(rel) = entry.rel_path.strip_prefix(&prefix) else {
            continue; // defensive: listing returned a path outside the root
        };
        // Defense-in-depth: a malicious / compromised server could return a
        // listing entry whose path escapes the synced root (e.g. `../..`); never
        // write outside the host workspace.
        if !safety::is_safe_relative(rel) {
            tracing::warn!(
                rel,
                env_root = %pw.env_side_root,
                "skipping write-back of unsafe (escaping) env-side path"
            );
            continue;
        }
        // A traversal-free `rel` can still escape via a host-side symlinked
        // directory; never write through one.
        if !safety::host_target_is_symlink_safe(&pw.host_ws, rel) {
            tracing::warn!(
                rel,
                env_root = %pw.env_side_root,
                "skipping write-back: host path traverses a symlink"
            );
            continue;
        }
        if safety::should_ignore(rel, ignore) {
            continue;
        }

        let bytes = t.download_file(&entry.rel_path).await?;

        // Changed = absent from the upload manifest, or content hash differs.
        let changed = pw
            .upload_manifest
            .get(rel)
            .is_none_or(|meta| meta.sha256_hex != safety::sha256_hex(&bytes));
        if changed {
            write_host_file_atomic(&pw.host_ws, rel, &bytes)?;
        }
    }
    Ok(())
}

/// Recursively remove `root` and everything under it via the transport: files
/// first, then directories deepest-first, then the root itself.
async fn remove_tree(
    factory: &dyn WorkspaceTransportFactory,
    root: &str,
) -> Result<(), DispatchError> {
    let t = factory.open().await?;
    let entries = t.list_tree(root).await?;

    // Remove every non-directory entry (regular files, symlinks) first.
    for entry in &entries {
        if entry.kind != FileKind::Dir {
            t.remove_file(&entry.rel_path).await?;
        }
    }

    // Collect directories from the listing plus the root itself, dedup, and
    // remove deepest-first so each is empty when removed. Including `root`
    // explicitly covers transports whose `list_tree` lists only the contents of
    // `root` (the real SFTP transport) rather than `root` itself.
    let mut dirs: Vec<String> = entries
        .into_iter()
        .filter(|e| e.kind == FileKind::Dir)
        .map(|e| e.rel_path)
        .collect();
    dirs.push(root.to_string());
    dirs.sort_unstable();
    dirs.dedup();
    dirs.sort_by_key(|d| std::cmp::Reverse(d.len())); // deepest (longest) path first
    for dir in dirs {
        t.remove_dir(&dir).await?;
    }
    Ok(())
}

/// Write `bytes` to `host_ws/rel` via a temp sibling + atomic rename, creating
/// parent directories as needed.
fn write_host_file_atomic(host_ws: &Path, rel: &str, bytes: &[u8]) -> Result<(), DispatchError> {
    let target = host_ws.join(rel);
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| host_io_err(parent, "create parent dir", &e))?;
    }
    let tmp = tmp_sibling(&target);
    std::fs::write(&tmp, bytes).map_err(|e| host_io_err(&tmp, "write temp file", &e))?;
    std::fs::rename(&tmp, &target).map_err(|e| host_io_err(&target, "rename into place", &e))?;
    Ok(())
}

/// A sibling temp path next to `target` (same directory → atomic rename).
fn tmp_sibling(target: &Path) -> PathBuf {
    let mut name = target
        .file_name()
        .map_or_else(|| std::ffi::OsString::from("ordius-wb"), ToOwned::to_owned);
    name.push(".ordius-wb.tmp");
    target
        .parent()
        .map_or_else(|| PathBuf::from(&name), |p| p.join(&name))
}

/// Map a host-side I/O error during write-back to a `DispatchError`.
fn host_io_err(path: &Path, op: &str, e: &std::io::Error) -> DispatchError {
    DispatchError::WorkspaceUnavailable {
        env_id: "<host>".into(),
        reason: format!("write-back {op} `{}`: {e}", path.display()),
    }
}

// ── Template expansion ────────────────────────────────────────────────────────

/// Expand `template` against `run` + `host_ws`, then validate the result.
///
/// Pure function — no I/O, no transport.  Unit-testable in isolation.
pub(crate) fn expand_env_root(
    template: &str,
    run: &RunScope<'_>,
    host_ws: &Path,
) -> Result<String, DispatchError> {
    use crate::template::{SubstitutionContext, default_env_allowlist, substitute};

    let empty_vars: HashMap<String, String> = HashMap::new();
    let empty_outputs: HashMap<(String, String), crate::types::PortValue> = HashMap::new();
    let empty_inputs: HashMap<String, crate::types::PortValue> = HashMap::new();
    let empty_config: HashMap<String, serde_json::Value> = HashMap::new();
    let env_allowlist: HashSet<String> = default_env_allowlist();

    let ctx = SubstitutionContext {
        vars: &empty_vars,
        secrets: &|_| None,
        upstream_outputs: &empty_outputs,
        current_inputs: &empty_inputs,
        current_config: &empty_config,
        kv: &|_| None,
        env: &|_| None,
        env_allowlist: &env_allowlist,
        resources: &|_, _| None,
        run_id: run.run_id,
        workspace: host_ws,
        started_at_iso: run.started_at_iso,
        workflow_id: run.workflow_id,
        workflow_name: run.workflow_name,
    };

    let root = substitute(template, &ctx)
        .map_err(|e| DispatchError::Unsupported(format!("invalid env_path_template: {e}")))?;

    safety::validate_env_root(&root)?;

    Ok(root)
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::environment::runtime::env::{
        ConflictDetect, EnvId, EnvInfo, EnvSpec, EnvState, SyncStrategy, WorkspaceBinding,
        WriteBackPolicy,
    };
    use crate::environment::runtime::local::LocalDispatcher;
    use std::collections::HashMap;
    use std::path::Path;

    use super::super::safety::{FileEntry, Manifest, hash_file};
    use super::super::transport::{
        FakeWorkspaceTransport, FakeWorkspaceTransportFactory, WorkspaceTransport,
    };
    use std::sync::Arc;
    use tokio::sync::OnceCell;

    fn local_info() -> EnvInfo {
        EnvInfo {
            id: EnvId::local(),
            label: "Local (host)".into(),
            spec: EnvSpec::Local {
                resources: vec![],
                host_direct_verifications: HashMap::default(),
            },
            state: EnvState::Reachable,
            enabled: true,
        }
    }

    fn sample_run<'a>() -> RunScope<'a> {
        RunScope {
            run_id: "r1",
            workflow_id: "wf1",
            workflow_name: "Test Workflow",
            started_at_iso: "2026-01-01T00:00:00Z",
        }
    }

    // ── H1 regression ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn resolve_cwd_shared_delegates_to_translate_path() {
        let d = LocalDispatcher::new(local_info());
        let mgr = WorkspaceManager::new();
        let cwd = mgr
            .resolve_cwd(
                &d,
                &WorkspaceBinding::Shared,
                Path::new("/workspaces/wf"),
                &sample_run(),
            )
            .await
            .expect("ok");
        assert_eq!(cwd.as_str(), "/workspaces/wf");
    }

    // ── expand_env_root ───────────────────────────────────────────────────────

    #[test]
    fn expand_env_root_substitutes_run_dot_id() {
        let run = sample_run();
        // Canonical template syntax is `{{run.id}}` (what users write in env_path_template).
        let result =
            expand_env_root("/tmp/ordius-{{run.id}}", &run, Path::new("/host/ws")).unwrap();
        assert_eq!(result, "/tmp/ordius-r1");
    }

    #[test]
    fn expand_env_root_rejects_dotdot() {
        let run = sample_run();
        let err = expand_env_root("/tmp/{{run.id}}/../x", &run, Path::new("/host/ws")).unwrap_err();
        assert!(
            err.to_string().contains(".."),
            "expected dotdot error; got: {err}"
        );
    }

    // ── resolve_cwd Sync guards ───────────────────────────────────────────────

    #[tokio::test]
    async fn resolve_cwd_sync_rsync_unsupported() {
        let d = LocalDispatcher::new(local_info());
        let mgr = WorkspaceManager::new();
        let binding = WorkspaceBinding::Sync {
            env_path_template: "/tmp/ordius-{{run.id}}".into(),
            strategy: SyncStrategy::Rsync,
            write_back: WriteBackPolicy::None,
        };
        let err = mgr
            .resolve_cwd(&d, &binding, Path::new("/ws"), &sample_run())
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("SFTP"),
            "expected SFTP error; got: {err}"
        );
    }

    #[tokio::test]
    async fn resolve_cwd_sync_safe_or_diverge_unsupported() {
        let d = LocalDispatcher::new(local_info());
        let mgr = WorkspaceManager::new();
        let binding = WorkspaceBinding::Sync {
            env_path_template: "/tmp/ordius-{{run.id}}".into(),
            strategy: SyncStrategy::Sftp,
            write_back: WriteBackPolicy::SafeOrDiverge {
                mode: ConflictDetect::Manifest,
                ignore: vec![],
                max_files: 5_000,
            },
        };
        let err = mgr
            .resolve_cwd(&d, &binding, Path::new("/ws"), &sample_run())
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("SafeOrDiverge"),
            "expected SafeOrDiverge error; got: {err}"
        );
    }

    #[tokio::test]
    async fn resolve_cwd_sync_persistent_unsupported() {
        let d = LocalDispatcher::new(local_info());
        let mgr = WorkspaceManager::new();
        // Template without `{{run.id}}` → persistent (deferred).
        let binding = WorkspaceBinding::Sync {
            env_path_template: "/stable/path".into(),
            strategy: SyncStrategy::Sftp,
            write_back: WriteBackPolicy::None,
        };
        let err = mgr
            .resolve_cwd(&d, &binding, Path::new("/ws"), &sample_run())
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("persistent"),
            "expected persistent error; got: {err}"
        );
    }

    /// `{{run_id}}` (underscore) is NOT a valid placeholder — the engine uses
    /// dotted namespaces and would emit an opaque "unknown namespace" error.
    /// We intercept it early with a clear hint message.
    #[tokio::test]
    async fn resolve_cwd_sync_run_id_underscore_gives_hint_error() {
        let d = LocalDispatcher::new(local_info());
        let mgr = WorkspaceManager::new();
        let binding = WorkspaceBinding::Sync {
            env_path_template: "/tmp/ordius-{{run_id}}".into(),
            strategy: SyncStrategy::Sftp,
            write_back: WriteBackPolicy::None,
        };
        let err = mgr
            .resolve_cwd(&d, &binding, Path::new("/ws"), &sample_run())
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("{{run.id}}") && msg.contains("{{run_id}}"),
            "expected hint naming both forms; got: {msg}"
        );
    }

    /// `{{run.id}}` (dotted, canonical) expands correctly — the Ephemeral
    /// branch is taken and `expand_env_root` returns the substituted path.
    #[test]
    fn expand_env_root_run_dot_id_classified_ephemeral() {
        let run = sample_run();
        let template = "/tmp/ordius-ws-{{run.id}}";
        // expand_env_root succeeds and substitutes the run id.
        let root = expand_env_root(template, &run, Path::new("/host/ws")).unwrap();
        assert_eq!(root, "/tmp/ordius-ws-r1");
        // Verify the discriminant: template contains `{{run.id}}` → Ephemeral.
        // (The persistent guard would have returned Err("persistent …"); the
        // fact that expand_env_root returned Ok confirms the Ephemeral branch.)
        assert!(
            template.contains("{{run.id}}"),
            "sanity: template must contain the canonical marker"
        );
        assert!(
            !template.contains("{{run_id}}"),
            "sanity: template must not contain the underscore form"
        );
    }

    // ── teardown_all ──────────────────────────────────────────────────────────

    /// Build a manager whose `prepared` map holds one ephemeral entry rooted at
    /// `root` with the given `write_back` policy and `manifest`, backed by a
    /// fresh fake transport. Returns the manager plus a state-sharing handle to
    /// the fake so the test can seed "remote" changes and assert post-teardown.
    async fn manager_with_prepared(
        root: &str,
        host_ws: &Path,
        write_back: WriteBackPolicy,
        manifest: Manifest,
    ) -> (WorkspaceManager, FakeWorkspaceTransport) {
        let fake = FakeWorkspaceTransport::default();
        let pw = PreparedWorkspace {
            env_side_root: root.to_string(),
            lifecycle: Lifecycle::Ephemeral,
            write_back,
            upload_manifest: manifest,
            host_ws: host_ws.to_path_buf(),
            transport_factory: Arc::new(FakeWorkspaceTransportFactory::new(fake.clone())),
        };
        let cell = OnceCell::new();
        cell.set(Arc::new(pw)).expect("seed prepared cell");
        let mgr = WorkspaceManager::new();
        mgr.prepared.lock().await.insert(
            (EnvId::ssh("h2-teardown"), host_ws.to_path_buf()),
            Arc::new(cell),
        );
        (mgr, fake)
    }

    /// A one-file manifest at `rel` recording the current on-disk hash of
    /// `host_ws/rel` — i.e. "this is what we uploaded".
    fn manifest_of(host_ws: &Path, rel: &str) -> Manifest {
        let abs = host_ws.join(rel);
        let size = std::fs::metadata(&abs).unwrap().len();
        let mut m = Manifest::new();
        m.insert(
            rel.to_string(),
            FileEntry {
                sha256_hex: hash_file(&abs).unwrap(),
                size,
                mode: 0o644,
            },
        );
        m
    }

    /// Force write-back on a clean completion copies changed + new env-side
    /// files into the host workspace, then deletes the ephemeral root.
    #[tokio::test]
    async fn teardown_force_completed_writes_back_and_deletes_root() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("a.txt"), b"original").unwrap();

        let root = "/tmp/ordius-wb-done";
        let (mgr, fake) = manager_with_prepared(
            root,
            host_ws,
            WriteBackPolicy::Force { ignore: vec![] },
            manifest_of(host_ws, "a.txt"),
        )
        .await;

        // Simulate the run: modify a.txt and create new.txt on the remote.
        fake.upload_file(&format!("{root}/a.txt"), b"modified")
            .await
            .unwrap();
        fake.upload_file(&format!("{root}/sub/new.txt"), b"created")
            .await
            .unwrap();

        mgr.teardown_all(RunOutcome::Completed).await;

        // Changed + new files are written back to the host.
        assert_eq!(
            std::fs::read(host_ws.join("a.txt")).unwrap(),
            b"modified",
            "changed file must be written back"
        );
        assert_eq!(
            std::fs::read(host_ws.join("sub").join("new.txt")).unwrap(),
            b"created",
            "new file (with new parent dir) must be written back"
        );

        // Ephemeral root is gone.
        assert!(
            fake.stat(&format!("{root}/a.txt")).await.unwrap().is_none(),
            "remote file must be deleted"
        );
        assert!(
            fake.stat(root).await.unwrap().is_none(),
            "ephemeral root dir must be deleted"
        );
    }

    /// User cancellation skips write-back entirely but STILL deletes the
    /// ephemeral root (cleanup is unconditional).
    #[tokio::test]
    async fn teardown_force_cancelled_skips_writeback_but_deletes_root() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("a.txt"), b"original").unwrap();

        let root = "/tmp/ordius-wb-cancel";
        let (mgr, fake) = manager_with_prepared(
            root,
            host_ws,
            WriteBackPolicy::Force { ignore: vec![] },
            manifest_of(host_ws, "a.txt"),
        )
        .await;

        fake.upload_file(&format!("{root}/a.txt"), b"modified")
            .await
            .unwrap();

        mgr.teardown_all(RunOutcome::CancelledByUser).await;

        // Write-back skipped: host file is untouched.
        assert_eq!(
            std::fs::read(host_ws.join("a.txt")).unwrap(),
            b"original",
            "user cancel must skip write-back"
        );
        // Cleanup still happens.
        assert!(
            fake.stat(root).await.unwrap().is_none(),
            "ephemeral root must be deleted even on cancel"
        );
    }

    /// `WriteBackPolicy::None` performs no write-back even on clean completion,
    /// but the ephemeral root is still deleted.
    #[tokio::test]
    async fn teardown_none_completed_skips_writeback_but_deletes_root() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("a.txt"), b"original").unwrap();

        let root = "/tmp/ordius-wb-none";
        let (mgr, fake) = manager_with_prepared(
            root,
            host_ws,
            WriteBackPolicy::None,
            manifest_of(host_ws, "a.txt"),
        )
        .await;

        fake.upload_file(&format!("{root}/a.txt"), b"modified")
            .await
            .unwrap();

        mgr.teardown_all(RunOutcome::Completed).await;

        assert_eq!(
            std::fs::read(host_ws.join("a.txt")).unwrap(),
            b"original",
            "None policy must not write back"
        );
        assert!(
            fake.stat(root).await.unwrap().is_none(),
            "ephemeral root must still be deleted under None policy"
        );
    }

    /// Force write-back honours the policy's ignore globs: an ignored env-side
    /// file is not copied back, but a non-ignored sibling is.
    #[tokio::test]
    async fn teardown_force_respects_ignore_globs() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();

        let root = "/tmp/ordius-wb-ignore";
        let (mgr, fake) = manager_with_prepared(
            root,
            host_ws,
            WriteBackPolicy::Force {
                ignore: vec!["*.log".into()],
            },
            Manifest::new(), // empty manifest: every env-side file counts as new
        )
        .await;

        fake.upload_file(&format!("{root}/data.txt"), b"keep")
            .await
            .unwrap();
        fake.upload_file(&format!("{root}/debug.log"), b"noise")
            .await
            .unwrap();

        mgr.teardown_all(RunOutcome::Completed).await;

        assert_eq!(
            std::fs::read(host_ws.join("data.txt")).unwrap(),
            b"keep",
            "non-ignored new file must be written back"
        );
        assert!(
            !host_ws.join("debug.log").exists(),
            "ignored *.log file must not be written back"
        );
    }

    /// A malicious / compromised server returning a path that escapes the synced
    /// root must NOT be written outside the host workspace during write-back.
    #[tokio::test]
    async fn teardown_force_skips_traversal_paths() {
        let host = tempfile::TempDir::new().unwrap();
        // Nest the workspace so a `..` escape would land inside the (auto-cleaned)
        // temp dir rather than polluting the real filesystem.
        let host_ws = host.path().join("ws");
        std::fs::create_dir(&host_ws).unwrap();

        let root = "/tmp/ordius-wb-evil";
        let (mgr, fake) = manager_with_prepared(
            root,
            &host_ws,
            WriteBackPolicy::Force { ignore: vec![] },
            Manifest::new(),
        )
        .await;

        // The "server" presents a traversal path plus a legitimate sibling.
        fake.upload_file(&format!("{root}/../escape.txt"), b"pwned")
            .await
            .unwrap();
        fake.upload_file(&format!("{root}/ok.txt"), b"fine")
            .await
            .unwrap();

        mgr.teardown_all(RunOutcome::Completed).await;

        assert!(
            !host.path().join("escape.txt").exists(),
            "traversal write-back must be skipped (no escape outside the workspace)"
        );
        assert_eq!(
            std::fs::read(host_ws.join("ok.txt")).unwrap(),
            b"fine",
            "the legitimate sibling must still be written back"
        );
    }

    /// A host-side symlinked directory inside the workspace must not let
    /// write-back escape — writing through it would redirect outside the tree.
    #[cfg(unix)]
    #[tokio::test]
    async fn teardown_force_does_not_follow_host_symlink_dirs() {
        use std::os::unix::fs::symlink;

        let outside = tempfile::TempDir::new().unwrap();
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path().join("ws");
        std::fs::create_dir(&host_ws).unwrap();
        // host_ws/link -> <outside>  (a symlinked directory inside the workspace)
        symlink(outside.path(), host_ws.join("link")).unwrap();

        let root = "/tmp/ordius-wb-symlink";
        let (mgr, fake) = manager_with_prepared(
            root,
            &host_ws,
            WriteBackPolicy::Force { ignore: vec![] },
            Manifest::new(),
        )
        .await;

        // The "server" creates a file under the host-symlinked `link/` dir.
        fake.upload_file(&format!("{root}/link/pwned.txt"), b"pwned")
            .await
            .unwrap();

        mgr.teardown_all(RunOutcome::Completed).await;

        assert!(
            !outside.path().join("pwned.txt").exists(),
            "write-back must not follow a host-side symlinked directory"
        );
    }
}
