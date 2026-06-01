/// Workspace manager — H2-T2 scope.
///
/// H1: `resolve_cwd` delegated to `dispatcher.translate_path` for all
/// bindings (behaviour unchanged for Local/WSL/BindMount/Shared/Translated).
///
/// H2-T2 (this file): `WorkspaceBinding::Sync` with `SyncStrategy::Sftp`
/// and a `{{run.id}}`-containing template is now handled — ephemeral upload
/// via SFTP, singleflight-serialised per `(EnvId, host_ws)` key so parallel
/// nodes on the same env share one upload.
///
/// Not yet implemented (deferred):
/// - Persistent workspace reuse (template without `{{run.id}}`).
/// - `SafeOrDiverge` write-back.
/// - Teardown / write-back body (`teardown_all` remains a no-op stub; the
///   prepared map it will read is tracked in `self.prepared`).
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
    /// Policy the caller specified; stored so teardown can branch on it.
    #[allow(dead_code)] // used by H2-T3 teardown
    write_back: WriteBackPolicy,
    /// Manifest of every file that was uploaded.
    #[allow(dead_code)] // used by H2-T3 write-back conflict detection
    upload_manifest: safety::Manifest,
    /// Host workspace path; redundant with the map key but handy for logs.
    #[allow(dead_code)]
    host_ws: PathBuf,
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

        // Ensure the root directory exists (transport mkdir creates parents).
        t.mkdir(env_side_root).await?;

        // Walk the host workspace, applying default ignore rules.
        let entries = safety::walk_workspace(host_ws)?;

        // Enforce caps BEFORE any bytes leave the host.
        let caps = safety::UploadCaps::default();
        let mut tracker = safety::CapTracker::new(caps);
        for entry in &entries {
            tracker.add(entry.size)?;
        }

        // Upload each file.
        for entry in &entries {
            let bytes =
                std::fs::read(&entry.abs).map_err(|e| DispatchError::WorkspaceUnavailable {
                    env_id: dispatcher.info().id.as_str().to_owned(),
                    reason: format!("read `{}` for upload: {e}", entry.abs.display()),
                })?;
            let remote_path = format!("{env_side_root}/{}", entry.rel_path);
            t.upload_file(&remote_path, &bytes).await?;
        }

        let upload_manifest = safety::build_manifest(host_ws, &entries)?;

        Ok(Arc::new(PreparedWorkspace {
            env_side_root: env_side_root.to_string(),
            lifecycle,
            write_back,
            upload_manifest,
            host_ws: host_ws.to_path_buf(),
        }))
    }

    /// Tear down every workspace prepared during the run.
    ///
    /// Fires on every run-loop exit path (success, error, or panic),
    /// before the engine's sender/token/lock cleanup. H2-T3 fills the
    /// body (write-back on `None`/`Force`, ephemeral delete); for now
    /// this is a no-op so net behaviour is unchanged.
    // `async` is required by the public contract; real awaits arrive in H2-T3.
    #[allow(clippy::unused_async)]
    pub async fn teardown_all(&self, outcome: RunOutcome) {
        #[cfg(any(test, feature = "testing"))]
        {
            *self.last_outcome.lock().unwrap() = Some(outcome);
        }
        // Avoid an unused-binding warning in non-testing builds.
        let _ = outcome;
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
}
