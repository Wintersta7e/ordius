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
pub mod executor;
pub mod loader;
pub mod recorder;
pub mod registry;
pub mod scheduler;
pub mod secrets;
pub mod template;
pub mod types;
pub mod validation;

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
    home: PathBuf,
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
    /// crashes, and pre-load the v1.0 built-ins.
    ///
    /// Custom-manifest loading (later phase) will extend the
    /// registry from `~/.ordius/node-types/`; that's why the
    /// constructor is async even though the body is sync today.
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
        Ok(Self {
            pool,
            registry: Arc::new(Registry::with_v1_0_builtins()),
            checkpoints: Arc::new(CheckpointRegistry::new()),
            home,
            run_senders: Arc::new(Mutex::new(HashMap::new())),
            run_tokens: Arc::new(Mutex::new(HashMap::new())),
        })
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

    /// Graceful shutdown — drain active runs within `drain_timeout`
    /// before forcing cancellation. Wired in a later phase; the
    /// signature is async today because the drain loop will await
    /// the per-run join handles.
    #[allow(clippy::unused_async)]
    pub async fn shutdown(&self, _drain_timeout: Duration) -> Result<()> {
        Err(EngineError::NotImplemented(
            "Engine::shutdown — graceful drain lands in Phase 8",
        ))
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
        // 8 v1.0 built-ins should be registered.
        assert_eq!(
            eng.registry().ids().len(),
            8,
            "got {:?}",
            eng.registry().ids()
        );
        assert!(eng.subscribe_run("nope").is_none());
        assert!(!eng.cancel_run("nope"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_is_not_implemented_yet() {
        let dir = TempDir::new().unwrap();
        let eng = Engine::new(dir.path().to_path_buf()).await.unwrap();
        let err = eng.shutdown(Duration::from_secs(1)).await.unwrap_err();
        assert!(matches!(err, EngineError::NotImplemented(_)));
    }
}
