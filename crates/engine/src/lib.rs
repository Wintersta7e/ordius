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
pub mod environment;
pub mod error;
pub mod events;
pub mod events_registry;
pub mod executor;
pub mod loader;
pub mod manifests;
pub mod namespaces;
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
pub use loader::{LoadError, load_workflow, load_workflow_unchecked, reject_reserved_node_types};
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
use crate::environment::runtime::{
    ResourceRegistry, install_builtin_resources, load_user_resources,
};
use crate::registry::Registry;
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
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
    /// Scoped resource registry: built-in + user-global at construction,
    /// per-workflow definitions installed at workflow-load time.
    resource_registry: Arc<ResourceRegistry>,
    checkpoints: Arc<CheckpointRegistry>,
    events: Arc<events_registry::EventRegistry>,
    home: PathBuf,
    secrets_store: Arc<Store>,
    /// Engine-owned per-env state: dispatcher + last-probed info per env id.
    /// Populated by the boot probe (Task 4); empty at construction.
    env_registry: Arc<environment::runtime::EnvRegistry>,
    /// Per-env last-probed catalog. Cloned into `RunSnapshot` at run start;
    /// `refresh_environment` swaps this atomically.
    env_catalogs:
        ArcSwap<HashMap<environment::runtime::EnvId, Arc<environment::runtime::ResourceCatalog>>>,
    /// Envs the user added to `env_specs` but flagged disabled. Kept here
    /// (NOT inside `env_registry`) so disabled envs surface in IPC listings
    /// with an Enable affordance while workflow validation refuses them via
    /// `validate_nodes` checking that the `target_env` is in the active
    /// `env_registry`. Populated by the boot probe; Task 5's
    /// `refresh_environment` swaps it atomically.
    env_disabled_specs:
        ArcSwap<HashMap<environment::runtime::EnvId, environment::runtime::EnvSpec>>,
    /// Serialises concurrent `refresh_environment` / inline-`EnvSpec`-write
    /// callers so the spec read + remove paths stay atomic. The lock is NOT
    /// held across the background probe — it covers only the synchronous
    /// section in [`Self::refresh_environment_locked`].
    env_refresh_lock: tokio::sync::Mutex<()>,
    /// Engine-wide refresh epoch, bumped on every successful swap of the env
    /// registry or per-env catalog. A background refresh captures the epoch
    /// before probing and `compare_exchange`s it before storing — concurrent
    /// writes invalidate each other's results, and the loser silently aborts
    /// instead of clobbering newer state.
    env_refresh_epoch: AtomicU64,
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
        let resource_registry = Arc::new(ResourceRegistry::new());
        // Built-ins land at the bottom of the precedence chain; the upsert
        // can never trip OverrideRequired here, so we surface the panic
        // as a programmer error instead of an EngineError variant.
        let builtin_count = install_builtin_resources(&resource_registry)
            .expect("install_builtin_resources cannot collide at Builtin scope");
        let user_count = load_user_resources(&home, &resource_registry)?;
        tracing::info!(
            builtins = builtin_count,
            user_resources = user_count,
            "seeded resource registry",
        );
        // EnvSpec-driven boot probe. Loads `env_specs` rows, builds a
        // Dispatcher per spec (Local + WSL today; SSH/Container land in
        // later phases), probes each, and returns the entries/catalogs/
        // disabled maps. An empty `env_specs` table synthesizes a default
        // Local env so first-run installs always have one usable target.
        let outcome = environment::runtime::boot_probe::run(&pool, &resource_registry).await;
        tracing::info!(
            envs = outcome.entries.len(),
            disabled = outcome.disabled_specs.len(),
            "env-runtime boot probe",
        );
        let env_registry = Arc::new(environment::runtime::EnvRegistry::new());
        env_registry.store(outcome.entries);
        let env_catalogs = ArcSwap::new(Arc::new(outcome.catalogs));
        let env_disabled_specs = ArcSwap::new(Arc::new(outcome.disabled_specs));
        Ok(Self {
            pool,
            registry: Arc::new(registry),
            resource_registry,
            checkpoints: Arc::new(CheckpointRegistry::new()),
            events: Arc::new(events_registry::EventRegistry::new()),
            home,
            secrets_store,
            env_registry,
            env_catalogs,
            env_disabled_specs,
            env_refresh_lock: tokio::sync::Mutex::new(()),
            env_refresh_epoch: AtomicU64::new(0),
            run_senders: Arc::new(Mutex::new(HashMap::new())),
            run_tokens: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Engine-owned env registry. Lock-free reads via the inner `ArcSwap`.
    /// Empty until the boot probe (Task 4) populates it.
    #[must_use]
    pub fn env_registry(&self) -> Arc<environment::runtime::EnvRegistry> {
        Arc::clone(&self.env_registry)
    }

    /// Cloned snapshot of every env's last-probed catalog. Returns an empty
    /// map if no probe has completed (boot is async; the catalog map starts
    /// empty in [`Self::new`]).
    #[must_use]
    pub fn env_catalogs(
        &self,
    ) -> Arc<HashMap<environment::runtime::EnvId, Arc<environment::runtime::ResourceCatalog>>> {
        self.env_catalogs.load_full()
    }

    /// Cloned snapshot of every env spec the user has flagged disabled.
    /// Disabled envs are not paired with a dispatcher and do not probe;
    /// IPC listings render them with an Enable affordance.
    #[must_use]
    pub fn env_disabled_specs(
        &self,
    ) -> Arc<HashMap<environment::runtime::EnvId, environment::runtime::EnvSpec>> {
        self.env_disabled_specs.load_full()
    }

    /// Re-probe the host environment.
    ///
    /// `refresh_environment(None)` schedules a full re-probe of every enabled
    /// env. `refresh_environment(Some(env_id))` schedules a single-env
    /// re-probe; when the row is missing or disabled, the env entry and its
    /// catalog are removed synchronously (no probe needed).
    ///
    /// **The probe itself runs in a background task** — this method returns
    /// as soon as the spec has been read and the probe spawned. Active
    /// `RunSnapshot`s are NOT mutated by the refresh; only the engine's
    /// `env_registry` + `env_catalogs` `ArcSwap` caches swap on probe
    /// completion, and the next run after the swap sees the new catalog.
    ///
    /// Concurrent refresh calls serialise on `env_refresh_lock` for the
    /// synchronous spec read, then race against `env_refresh_epoch` via CAS
    /// before committing. The loser of a race silently drops its result —
    /// the engine never holds inconsistent maps but may transiently hold
    /// the older of two completed probes until the next refresh.
    pub async fn refresh_environment(
        self: &Arc<Self>,
        env_id: Option<&environment::runtime::EnvId>,
    ) -> Result<()> {
        let _guard = self.env_refresh_lock.lock().await;
        self.refresh_environment_locked(env_id)
    }

    /// Internal helper: caller MUST already hold `env_refresh_lock`.
    /// Future inline-`EnvSpec` writers (`add_env`, `remove_env`,
    /// `set_env_enabled` — Task 15) call this directly to avoid re-locking.
    /// Synchronous because the background probe is spawned, not awaited.
    fn refresh_environment_locked(
        self: &Arc<Self>,
        env_id: Option<&environment::runtime::EnvId>,
    ) -> Result<()> {
        use std::sync::atomic::Ordering;

        // SECTION A — synchronous (under the caller's lock): read the spec
        // and capture the epoch before scheduling any background work.
        let pending = match env_id {
            None => RefreshPending::Full,
            Some(env) => {
                match environment::runtime::boot_probe::load_spec_single(&self.pool, env) {
                    Ok(Some(spec)) => RefreshPending::Single {
                        env_id: env.clone(),
                        spec: Box::new(spec),
                    },
                    Ok(None) => RefreshPending::Remove {
                        env_id: env.clone(),
                    },
                    Err(e) => {
                        return Err(EngineError::Db(format!("refresh load_spec: {e}")));
                    },
                }
            },
        };
        let epoch_before = self.env_refresh_epoch.load(Ordering::Acquire);

        // SECTION B — handle Remove inline. No probe required; CoW the maps
        // under the lock, bump the epoch, swap. Concurrent background probes
        // for this env then fail their CAS commit and drop stale results.
        if let RefreshPending::Remove { env_id } = pending {
            self.apply_remove(&env_id);
            return Ok(());
        }

        // SECTION C — spawn the background probe. Returns immediately; the
        // spawned task probes + CAS-commits + swaps.
        let engine = Arc::clone(self);
        tokio::spawn(async move {
            match pending {
                RefreshPending::Full => run_full_refresh(&engine, epoch_before).await,
                RefreshPending::Single { env_id, spec } => {
                    run_single_refresh(&engine, epoch_before, env_id, *spec).await;
                },
                RefreshPending::Remove { .. } => {
                    // SECTION B handles Remove inline; reaching here is a
                    // programming error in this module.
                    unreachable!("RefreshPending::Remove handled before spawn");
                },
            }
        });
        Ok(())
    }

    /// Synchronous Remove: drops the env from `env_registry`, `env_catalogs`,
    /// and `env_disabled_specs`. Bumps the epoch BEFORE the swap so an
    /// in-flight background probe reads the bumped value on its CAS.
    fn apply_remove(&self, env_id: &environment::runtime::EnvId) {
        use std::sync::atomic::Ordering;

        let current_entries = self.env_registry.entries();
        let mut next_entries = (*current_entries).clone();
        next_entries.remove(env_id);
        let current_catalogs = self.env_catalogs.load();
        let mut next_catalogs = (**current_catalogs).clone();
        next_catalogs.remove(env_id);
        let current_disabled = self.env_disabled_specs.load();
        let mut next_disabled = (**current_disabled).clone();
        next_disabled.remove(env_id);
        self.env_refresh_epoch.fetch_add(1, Ordering::AcqRel);
        self.env_registry.store(next_entries);
        self.env_catalogs.store(Arc::new(next_catalogs));
        self.env_disabled_specs.store(Arc::new(next_disabled));
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

    /// Shared resource registry — pass into resource-aware dispatchers and
    /// into workflow-scope install / remove calls.
    #[must_use]
    pub fn resource_registry(&self) -> Arc<ResourceRegistry> {
        self.resource_registry.clone()
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

    /// Load a workflow from disk and install its `resources:` block into
    /// the engine's registry under `ScopeKey::Workflow { id }`.
    ///
    /// Centralised entry point for every user-facing run path (CLI `run`,
    /// Tauri `run_workflow`). Wraps [`crate::workflows::load_in_registry`]
    /// with the engine's `Arc<ResourceRegistry>` and returns the loaded
    /// workflow alongside any non-fatal validation warnings.
    ///
    /// Callers that construct `Workflow` values programmatically (tests,
    /// future entry points) can rely on [`Self::start_run`]'s safety-net
    /// install instead.
    pub fn load_workflow_for_run(
        &self,
        home: &Path,
        id: &str,
    ) -> std::result::Result<(Arc<types::Workflow>, Vec<workflows::WorkflowWarning>), EngineError>
    {
        let (wf, warnings) = workflows::load_in_registry(home, id, &self.resource_registry)?;
        Ok((Arc::new(wf), warnings))
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

/// Refresh-environment branches built under `env_refresh_lock` and consumed
/// by [`Engine::refresh_environment_locked`].
enum RefreshPending {
    /// Full re-probe across every enabled `env_specs` row.
    Full,
    /// Re-probe a single env whose row was present and enabled at lock-read
    /// time. The `spec` is captured under the lock so the background probe
    /// works against a frozen snapshot of the user's configuration.
    Single {
        env_id: environment::runtime::EnvId,
        /// `EnvSpec` is ~272 bytes; boxing keeps the enum variant size in
        /// line with the other cheap variants (clippy `large_enum_variant`).
        spec: Box<environment::runtime::EnvSpec>,
    },
    /// Row absent or disabled at lock-read time. Handled inline (no probe);
    /// the env's entry + catalog are dropped under the lock.
    Remove { env_id: environment::runtime::EnvId },
}

/// Background refresh body for `RefreshPending::Full`. Re-probes every env
/// in the DB; CAS-commits the result against `epoch_before` so a concurrent
/// `refresh_environment` write that ran during the probe drops this result
/// silently. Tracing logs the loss at debug level.
async fn run_full_refresh(engine: &Arc<Engine>, epoch_before: u64) {
    use std::sync::atomic::Ordering;
    let outcome =
        environment::runtime::boot_probe::run(&engine.pool, &engine.resource_registry).await;
    match engine.env_refresh_epoch.compare_exchange(
        epoch_before,
        epoch_before + 1,
        Ordering::AcqRel,
        Ordering::Acquire,
    ) {
        Ok(_) => {
            engine.env_registry.store(outcome.entries);
            engine.env_catalogs.store(Arc::new(outcome.catalogs));
            engine
                .env_disabled_specs
                .store(Arc::new(outcome.disabled_specs));
        },
        Err(current) => {
            tracing::debug!(
                epoch_before,
                current,
                "full refresh stale; another write happened during probe",
            );
        },
    }
}

/// Background refresh body for `RefreshPending::Single`. Re-probes one env
/// against the snapshot of its `EnvSpec` captured under the lock. On Ok the
/// epoch CAS commits the merged update; on probe error / CAS loss the
/// result is dropped.
async fn run_single_refresh(
    engine: &Arc<Engine>,
    epoch_before: u64,
    env_id: environment::runtime::EnvId,
    spec: environment::runtime::EnvSpec,
) {
    use std::sync::atomic::Ordering;
    match environment::runtime::boot_probe::run_single(&engine.resource_registry, &env_id, &spec)
        .await
    {
        Ok(single) => {
            match engine.env_refresh_epoch.compare_exchange(
                epoch_before,
                epoch_before + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let mut next_entries = (*engine.env_registry.entries()).clone();
                    next_entries.insert(env_id.clone(), single.entry);
                    engine.env_registry.store(next_entries);
                    let mut next_catalogs = (**engine.env_catalogs.load()).clone();
                    next_catalogs.insert(env_id.clone(), single.catalog);
                    engine.env_catalogs.store(Arc::new(next_catalogs));
                    // If the env was previously disabled (now enabled), drop
                    // its entry from env_disabled_specs so the IPC stops
                    // listing it under disabled.
                    let mut next_disabled = (**engine.env_disabled_specs.load()).clone();
                    if next_disabled.remove(&env_id).is_some() {
                        engine.env_disabled_specs.store(Arc::new(next_disabled));
                    }
                },
                Err(current) => {
                    tracing::debug!(
                        env_id = %env_id,
                        epoch_before,
                        current,
                        "per-env refresh stale; dropping result",
                    );
                },
            }
        },
        Err(e) => {
            tracing::warn!(env_id = %env_id, error = %e, "background refresh failed");
        },
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
    async fn new_seeds_builtin_resources() {
        use crate::environment::runtime::{ResourceId, ScopeKey};

        let dir = TempDir::new().unwrap();
        let engine = Engine::new(dir.path().to_path_buf()).await.expect("new");
        let snap = engine.resource_registry().snapshot();
        let builtin_layer = snap
            .layers
            .get(&ScopeKey::Builtin)
            .expect("builtin layer present");
        assert!(
            builtin_layer.contains_key(&ResourceId("ollama".into())),
            "ollama in builtins",
        );
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
                target_env: None,
            }],
            edges: vec![],
            resources: vec![],
            default_env: None,
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
