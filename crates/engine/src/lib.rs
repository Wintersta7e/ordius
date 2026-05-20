//! Ordius workflow engine. See `docs/` at the repo root for the spec.
//!
//! Public surface (filled in by later tasks):
//! - types: `Workflow`, `Node`, `Edge`, `NodeType`, `Run`, `RunEvent`
//! - scheduler: `Scheduler`
//! - executor: `NodeExecutor` + in-process / subprocess / container impls
//! - storage: `Db`, `RunRecorder`
//! - templates: substitute, redact
//! - secrets: keyring read/write

pub mod checkpoints;
pub mod db;
pub mod emitter;
pub mod error;
pub mod events;
pub mod events_registry;
pub mod executor;
pub mod loader;
pub mod manifests;
pub mod recorder;
pub mod registry;
pub mod run;
pub mod scheduler;
pub mod secrets;
pub mod seeds;
pub mod settings;
pub mod system_status;
pub mod template;
pub mod triggers;
pub mod types;
pub mod validation;
pub mod workflows;
pub mod workspaces;

pub use checkpoints::{CheckpointRegistry, Resume};
pub use emitter::Emitter;
pub use error::{EngineError, Result};
pub use events::{EventType, RunEvent};
pub use executor::{
    EnvResolver, InProcessExecutor, NodeError, NodeExecutor, NodeOutputs, RunContext,
    wrap_process_env,
};
pub use loader::{LoadError, load_workflow};
pub use recorder::{NodeRunRow, RunRecorder, sweep_stale_locks};
pub use run::{RunHandle, RunSummary};
pub use scheduler::{LoopFire, NodeState, Scheduler};
pub use secrets::{SecretError, Store, redact_secrets};
pub use template::{SubstitutionContext, TemplateError, default_env_allowlist, substitute};
pub use types::{
    BackoffStrategy, Category, ConfigFieldDef, ConfigFieldType, Edge, EdgeType, ExecutionBackend,
    ExecutionSpec, Node, NodeType, OutputParse, PortDef, PortType, PortValue, Pos, RetryOn,
    RetryPolicy, Trigger, Workflow,
};
pub use validation::{ValidationError, validate};

use crate::db::DbPool;
use crate::registry::Registry;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

/// Top-level engine handle.
///
/// Owns the `SQLite` pool, the node-type registry, the live
/// `CheckpointRegistry`, the run-home workspace directory, and the
/// per-active-run event senders / cancel tokens that let external
/// callers stream events from (or cancel) any run started in this
/// process.
///
/// Cheaply shareable via `Arc<Engine>` — every method that
/// mutates internal state takes `&self` and uses interior
/// mutability (`Mutex`).
pub struct Engine {
    pool: DbPool,
    registry: Arc<Registry>,
    checkpoints: Arc<CheckpointRegistry>,
    events: Arc<events_registry::EventRegistry>,
    home: PathBuf,
    secrets_store: Arc<Store>,
    /// Active-run broadcast senders so subscribers (CLI
    /// `--json-events`, GUI Tauri commands) can stream events for
    /// any run that this process started.
    pub(crate) run_senders: Arc<Mutex<HashMap<String, broadcast::Sender<RunEvent>>>>,
    /// Active-run cancel tokens (cleaned up on completion).
    pub(crate) run_tokens: Arc<Mutex<HashMap<String, CancellationToken>>>,
}

impl Engine {
    /// Construct the engine: open `runs.db` in `home`, apply
    /// migrations, sweep stale `workflow_locks` from prior
    /// crashes, pre-load the v1.0 built-ins, and merge any custom
    /// manifests from `<home>/node-types/`.
    ///
    /// The signature is async because future manifest loading may
    /// want to await disk IO; the body is sync today.
    #[allow(clippy::unused_async)]
    pub async fn new(home: PathBuf) -> Result<Self> {
        let db_path = home.join("runs.db");
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| EngineError::Io {
                context: format!("create engine home {}", parent.display()),
                source: e,
            })?;
        }
        let pool = db::open(&db_path)?;
        let swept = recorder::sweep_stale_locks(&pool, 24 * 3600 * 1000)?;
        if swept > 0 {
            tracing::warn!(swept, "swept stale workflow locks from prior crash");
        }
        let secrets_store = Arc::new(Store::with_index_path(home.join("secrets-index.json")));
        // Built-ins land first; manifests register on top of them.
        // Duplicate-id checks inside load_into ensure a manifest
        // cannot overwrite a built-in spec.
        let mut registry = Registry::with_v1_1_builtins();
        let manifest_errs = manifests::load_into(&mut registry, home.join("node-types"));
        for err in &manifest_errs {
            tracing::warn!(error = %err, "manifest load issue");
        }
        let seeded = seeds::install_if_empty(&home);
        if seeded > 0 {
            tracing::info!(count = seeded, "installed starter workflows");
        }
        Ok(Self {
            pool,
            registry: Arc::new(registry),
            checkpoints: Arc::new(CheckpointRegistry::new()),
            events: Arc::new(events_registry::EventRegistry::new()),
            home,
            secrets_store,
            run_senders: Arc::new(Mutex::new(HashMap::new())),
            run_tokens: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Shared secrets store. Backed by `<home>/secrets-index.json`
    /// plus whatever credential builder the host has installed for
    /// `keyring_core` (libsecret / Credential Manager / Keychain /
    /// sample). Cloned into every `RunContext` so executors can
    /// resolve `{{secrets.X}}` template references.
    #[must_use]
    pub fn secrets_store(&self) -> Arc<Store> {
        self.secrets_store.clone()
    }

    /// `SQLite` pool. Cloning a pool is cheap (it's an `Arc`).
    #[must_use]
    pub fn pool(&self) -> DbPool {
        self.pool.clone()
    }

    /// Shared node-type registry.
    #[must_use]
    pub fn registry(&self) -> Arc<Registry> {
        self.registry.clone()
    }

    /// Shared checkpoint registry — pass into `RunContext`.
    #[must_use]
    pub fn checkpoints(&self) -> Arc<CheckpointRegistry> {
        self.checkpoints.clone()
    }

    /// Shared event registry — pass into `RunContext`. Used by
    /// `wait_event` to park until an external caller delivers an
    /// event via [`Self::deliver_event`].
    #[must_use]
    pub fn events(&self) -> Arc<events_registry::EventRegistry> {
        self.events.clone()
    }

    /// Deliver an event payload to a parked `wait_event` waiter.
    /// Returns `true` if a waiter was present and accepted the
    /// payload, `false` if no waiter exists or has already dropped.
    pub fn deliver_event(
        &self,
        run_id: &str,
        event_name: &str,
        payload: serde_json::Value,
    ) -> bool {
        self.events.deliver(run_id, event_name, payload)
    }

    /// Engine home directory (run workspaces land in
    /// `<home>/workspaces/<run_id>/`).
    #[must_use]
    pub fn home(&self) -> &Path {
        &self.home
    }

    /// Subscribe to a run's event stream. Returns `None` if the
    /// run isn't active in this process (already completed, or
    /// never started here).
    #[must_use]
    pub fn subscribe_run(&self, run_id: &str) -> Option<broadcast::Receiver<RunEvent>> {
        self.run_senders
            .lock()
            .expect("engine run_senders mutex poisoned")
            .get(run_id)
            .map(broadcast::Sender::subscribe)
    }

    /// Cancel a running run by id. Returns `true` if the run was
    /// active and the cancel token fired.
    pub fn cancel_run(&self, run_id: &str) -> bool {
        self.run_tokens
            .lock()
            .expect("engine run_tokens mutex poisoned")
            .get(run_id)
            .is_some_and(|token| {
                token.cancel();
                true
            })
    }

    /// Graceful shutdown. Snapshots every active run's cancel
    /// token, fires them all, polls `run_tokens` until empty (or
    /// `drain_timeout` elapses), then returns. The run loops
    /// themselves remove their own entries from `run_senders` /
    /// `run_tokens` on exit, so emptiness means everything has
    /// finalized.
    pub async fn shutdown(&self, drain_timeout: Duration) -> Result<()> {
        let active: Vec<CancellationToken> = self
            .run_tokens
            .lock()
            .expect("engine run_tokens mutex poisoned")
            .values()
            .cloned()
            .collect();
        for token in &active {
            token.cancel();
        }

        let deadline = tokio::time::Instant::now() + drain_timeout;
        loop {
            if self
                .run_tokens
                .lock()
                .expect("engine run_tokens mutex poisoned")
                .is_empty()
            {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // Last-ditch grace window for subprocess executors to
        // finish their cancellation arm (SIGKILL after SIGTERM,
        // TerminateJobObject, etc.) before we return.
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok(())
    }
}

#[cfg(test)]
mod engine_tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test(flavor = "multi_thread")]
    async fn new_opens_db_and_seeds_registry() {
        let dir = TempDir::new().unwrap();
        let eng = Engine::new(dir.path().to_path_buf()).await.unwrap();
        // v1.0 (8) + v1.1 in-progress (kv) = 9 built-ins.
        let ids = eng.registry().ids();
        for required in [
            "delay",
            "transform",
            "condition",
            "shell",
            "http",
            "llm",
            "file",
            "checkpoint",
            "kv",
        ] {
            assert!(
                ids.iter().any(|id| id == required),
                "missing built-in {required:?} in {ids:?}",
            );
        }
        assert!(eng.subscribe_run("nope").is_none());
        assert!(!eng.cancel_run("nope"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_with_no_active_runs_is_quick() {
        let dir = TempDir::new().unwrap();
        let eng = Engine::new(dir.path().to_path_buf()).await.unwrap();
        let start = std::time::Instant::now();
        eng.shutdown(Duration::from_secs(2)).await.unwrap();
        assert!(
            start.elapsed() < Duration::from_millis(500),
            "no active runs should return immediately, took {:?}",
            start.elapsed(),
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_cancels_running_workflows() {
        use crate::types::{Node, Pos, Workflow};
        use std::collections::HashMap;

        let dir = TempDir::new().unwrap();
        let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());

        let wf = Arc::new(Workflow {
            id: "wf_shutdown".into(),
            name: "long".into(),
            schema_version: 1,
            created_at: None,
            updated_at: None,
            variables: HashMap::new(),
            triggers: vec![],
            nodes: vec![Node {
                id: "slow".into(),
                ty: "delay".into(),
                name: String::new(),
                config: HashMap::from([("ms".into(), serde_json::json!(60_000))]),
                pos: Pos::default(),
                timeout_ms: None,
                retry: None,
                continue_on_error: false,
            }],
            edges: vec![],
        });

        let handle = engine
            .start_run(wf, HashMap::new(), "test", false, None)
            .expect("start_run");
        // Let the run loop start dispatching before we ask to shut down.
        tokio::time::sleep(Duration::from_millis(100)).await;
        engine
            .shutdown(Duration::from_secs(2))
            .await
            .expect("shutdown drains");

        let summary = handle.join.await.expect("join").expect("run ok");
        assert_eq!(summary.status, "stopped");
    }
}
