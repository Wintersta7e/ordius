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

/// User-visible environment refresh state change.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EnvRefreshEvent {
    /// `None` means a full refresh committed; `Some` identifies a single env
    /// whose active or disabled state changed.
    pub env_id: Option<environment::runtime::EnvId>,
    /// Engine refresh epoch after the commit.
    pub epoch: u64,
}

/// Outcome of [`Engine::test_host_direct`].
///
/// Returned both for hits and for transport / parse failures so the
/// wizard can render a single uniform result screen. Hard errors
/// (env / resource / probe spec missing) propagate as `EngineError`
/// instead of populating this struct.
#[derive(Debug, Clone)]
pub struct HostDirectTestOutcome {
    /// Base URL the probe was sent to.
    pub host_url: String,
    /// Probe route path appended to `host_url`.
    pub probe_route_path: String,
    /// HTTP status code; `None` when the request never returned a
    /// response (transport error, timeout).
    pub status_code: Option<u16>,
    /// Stable fingerprint derived from the response body. `None` when
    /// the request failed, the body did not parse as JSON, or any
    /// `fingerprint_jsonpaths` entry had no match.
    pub stable_fingerprint: Option<String>,
    /// Up to ~2 KB of the response body as a UTF-8 string. `None` for
    /// failures and for bodies whose first 2 KB are not valid UTF-8.
    pub response_excerpt: Option<String>,
    /// Set when `success()` is `false`.
    pub error: Option<String>,
}

impl HostDirectTestOutcome {
    /// `true` when the request returned a 2xx response AND a stable
    /// fingerprint was extracted from the body.
    #[must_use]
    pub fn success(&self) -> bool {
        self.error.is_none()
            && self.stable_fingerprint.is_some()
            && matches!(self.status_code, Some(s) if (200..300).contains(&s))
    }
}

/// Maximum bytes of response body returned on `response_excerpt`. The
/// wizard renders this verbatim, so a small ceiling keeps the IPC
/// payload bounded.
const HOST_DIRECT_EXCERPT_LIMIT: usize = 2048;

/// 5-second ceiling for the host-direct probe request. Surfaces a
/// "request timed out" error on the outcome rather than blocking the
/// caller indefinitely.
const HOST_DIRECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Fire a single HTTP probe at `base_url + route_path` via the shared
/// `reqwest::Client` and turn the response into a
/// [`HostDirectTestOutcome`]. Pure of any engine state — split from
/// [`Engine::test_host_direct`] so the per-call body stays under the
/// crate's clippy line ceiling.
async fn probe_host_direct(
    base_url: String,
    probe_route_path: String,
    method: environment::runtime::HttpProbeMethod,
    fingerprint_jsonpaths: Vec<String>,
) -> HostDirectTestOutcome {
    let url = format!("{base_url}{probe_route_path}");
    let client = crate::executor::http_client::shared();
    let request = match method {
        environment::runtime::HttpProbeMethod::Get => client.get(&url),
        environment::runtime::HttpProbeMethod::Head => client.head(&url),
    };

    let response = match tokio::time::timeout(HOST_DIRECT_TIMEOUT, request.send()).await {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => {
            return HostDirectTestOutcome {
                host_url: base_url,
                probe_route_path,
                status_code: None,
                stable_fingerprint: None,
                response_excerpt: None,
                error: Some(format!("request failed: {e}")),
            };
        },
        Err(_) => {
            return HostDirectTestOutcome {
                host_url: base_url,
                probe_route_path,
                status_code: None,
                stable_fingerprint: None,
                response_excerpt: None,
                error: Some("request timed out after 5s".into()),
            };
        },
    };
    let status_code = response.status().as_u16();
    let bytes = match response.bytes().await {
        Ok(b) => b,
        Err(e) => {
            return HostDirectTestOutcome {
                host_url: base_url,
                probe_route_path,
                status_code: Some(status_code),
                stable_fingerprint: None,
                response_excerpt: None,
                error: Some(format!("read body failed: {e}")),
            };
        },
    };

    let excerpt_len = bytes.len().min(HOST_DIRECT_EXCERPT_LIMIT);
    let response_excerpt = std::str::from_utf8(&bytes[..excerpt_len])
        .ok()
        .map(std::string::ToString::to_string);
    let stable_fingerprint =
        environment::runtime::wsl::host_direct::compute_fingerprint(&bytes, &fingerprint_jsonpaths);

    HostDirectTestOutcome {
        host_url: base_url,
        probe_route_path,
        status_code: Some(status_code),
        stable_fingerprint,
        response_excerpt,
        error: None,
    }
}

/// Resolve the [`environment::runtime::ProbeSpec`] for
/// `(env_id, resource_id)` consulting both the engine's resource
/// registry and the env's persisted inline `EnvSpec.resources`.
///
/// The engine-level registry only carries `Builtin` + `UserGlobal`
/// scopes — env-local resources live inline on the `EnvSpec` row and
/// are layered in by `with_env_local_overlay` only at run-snapshot
/// construction. Host-direct verification runs outside a run, so the
/// registry-only lookup misses inline env-local defs; this helper
/// falls back to a row read of `EnvSpec::resources()` so the wizard
/// can target the same definitions probed at refresh time.
fn resolve_probe_for_test(
    engine: &Arc<Engine>,
    env_id: &environment::runtime::EnvId,
    resource_id: &environment::runtime::ResourceId,
) -> Result<environment::runtime::ProbeSpec> {
    // Inline env-local definitions take precedence over the engine-level
    // registry, mirroring the runtime `Workflow > EnvLocal > UserGlobal >
    // Builtin` precedence chain. The engine's registry only carries
    // `EnvLocal` rows during a live run snapshot, so checking it first
    // would silently route a user's `override_lower_scope: true` shadow
    // through the Builtin's probe spec.
    //
    // NOTE: this resolves the probe spec correctly for the override
    // case, but the env catalog itself (used by `test_host_direct` for
    // the `base_url`) is built from the registry and inherits the same
    // gap — a Builtin id shadowed by an env-local override is still
    // probed against the Builtin's ports at refresh time. Closing that
    // gap requires installing `EnvLocal` layers into the engine-level
    // registry at refresh time, not just at run-snapshot construction.
    let row = environment::runtime::boot_probe::load_spec_single(&engine.pool, env_id)
        .map_err(|e| EngineError::Db(format!("load env spec: {e}")))?
        .ok_or_else(|| EngineError::EnvUnknown(env_id.clone()))?;
    if let Some(def) = row
        .spec
        .as_ref()
        .and_then(|s| s.resources().iter().find(|d| &d.id == resource_id))
    {
        return Ok(def.probe.clone());
    }
    let registry = engine.resource_registry();
    let snap = registry.snapshot();
    if let Some((def, _scope)) = snap.resolve(resource_id, env_id, None) {
        return Ok(def.probe.clone());
    }
    Err(EngineError::Db(format!(
        "resource '{}' not declared in registry or env '{}' inline spec",
        resource_id.0,
        env_id.as_str(),
    )))
}

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
    env_disabled_specs: ArcSwap<
        HashMap<environment::runtime::EnvId, environment::runtime::boot_probe::DisabledEnv>,
    >,
    /// Serialises concurrent `refresh_environment` / inline-`EnvSpec`-write
    /// callers so the spec read + remove paths stay atomic. The lock is NOT
    /// held across the background probe, but spawned refresh tasks re-enter
    /// it for their compare-and-swap commit section.
    env_refresh_lock: tokio::sync::Mutex<()>,
    /// Engine-wide refresh epoch, bumped on every successful swap of the env
    /// registry or per-env catalog. A background refresh captures the epoch
    /// before probing and `compare_exchange`s it before storing — concurrent
    /// writes invalidate each other's results, and the loser silently aborts
    /// instead of clobbering newer state.
    env_refresh_epoch: AtomicU64,
    /// Broadcasts after an env refresh/remove/disable commit so hosts can
    /// re-fetch the lock-free snapshots returned by `environment_list`.
    env_refresh_tx: broadcast::Sender<EnvRefreshEvent>,
    /// Active-run broadcast senders so subscribers (CLI
    /// `--json-events`, GUI Tauri commands) can stream events for
    /// any run that this process started.
    pub(crate) run_senders: Arc<Mutex<HashMap<String, broadcast::Sender<RunEvent>>>>,
    /// Active-run cancel tokens (cleaned up on completion).
    pub(crate) run_tokens: Arc<Mutex<HashMap<String, CancellationToken>>>,
    /// Active-run snapshot map. Populated synchronously by `start_run`
    /// before `tokio::spawn`, and removed by an RAII guard inside the
    /// spawned task — so any drop path (normal exit, `?`-propagation,
    /// or panic unwind) cleans the entry. Used by the test-only
    /// `Engine::run_snapshot` accessor to verify that an in-flight
    /// run's view does not change under `refresh_environment`.
    pub(crate) run_snapshots: Arc<Mutex<HashMap<String, Arc<environment::runtime::RunSnapshot>>>>,
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
        let outcome = environment::runtime::boot_probe::run(
            &pool,
            &resource_registry,
            Arc::clone(&secrets_store),
        )
        .await;
        tracing::info!(
            envs = outcome.entries.len(),
            disabled = outcome.disabled_specs.len(),
            "env-runtime boot probe",
        );
        let env_registry = Arc::new(environment::runtime::EnvRegistry::new());
        env_registry.store(outcome.entries);
        let env_catalogs = ArcSwap::new(Arc::new(outcome.catalogs));
        let env_disabled_specs = ArcSwap::new(Arc::new(outcome.disabled_specs));
        let (env_refresh_tx, _env_refresh_rx) = broadcast::channel(32);
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
            env_refresh_tx,
            run_senders: Arc::new(Mutex::new(HashMap::new())),
            run_tokens: Arc::new(Mutex::new(HashMap::new())),
            run_snapshots: Arc::new(Mutex::new(HashMap::new())),
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
    ) -> Arc<HashMap<environment::runtime::EnvId, environment::runtime::boot_probe::DisabledEnv>>
    {
        self.env_disabled_specs.load_full()
    }

    /// Subscribe to environment state changes committed after construction.
    #[must_use]
    pub fn subscribe_env_refresh(&self) -> broadcast::Receiver<EnvRefreshEvent> {
        self.env_refresh_tx.subscribe()
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
    /// Inline-`EnvSpec` writers (`add_env`, `remove_env`,
    /// `set_env_enabled`, `add_env_local_resource`,
    /// `remove_env_local_resource`) call this directly to avoid
    /// re-locking. Synchronous because the background probe is spawned,
    /// not awaited.
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
                    Ok(Some(row)) => {
                        if row.enabled {
                            // `spec` is `Option<EnvSpec>`; a None-spec row is
                            // forced to `enabled = false` by `load_spec_single`,
                            // so unwrap is safe here. Use unwrap_or_else as a
                            // belt-and-suspenders guard: treat a None-spec
                            // enabled row as Disabled.
                            match row.spec {
                                Some(spec) => RefreshPending::Single {
                                    env_id: env.clone(),
                                    label: row.label,
                                    spec: Box::new(spec),
                                },
                                None => RefreshPending::Disabled {
                                    env_id: env.clone(),
                                    label: row.label,
                                    spec: None,
                                    reason: row.disabled_reason,
                                },
                            }
                        } else {
                            RefreshPending::Disabled {
                                env_id: env.clone(),
                                label: row.label,
                                spec: row.spec.map(Box::new),
                                reason: row.disabled_reason,
                            }
                        }
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

        // SECTION B — handle Remove + Disabled inline. No probe required;
        // CoW the maps under the lock, bump the epoch, swap. Concurrent
        // background probes for this env then fail their CAS commit and
        // drop stale results.
        match pending {
            RefreshPending::Remove { env_id } => {
                self.apply_remove(&env_id);
                return Ok(());
            },
            RefreshPending::Disabled {
                env_id,
                label,
                spec,
                reason,
            } => {
                self.apply_disable(&env_id, label, spec.map(|b| *b), reason);
                return Ok(());
            },
            _ => {},
        }

        // SECTION C — spawn the background probe. Returns immediately; the
        // spawned task probes + CAS-commits + swaps.
        let engine = Arc::clone(self);
        tokio::spawn(async move {
            match pending {
                RefreshPending::Full => run_full_refresh(&engine, epoch_before).await,
                RefreshPending::Single {
                    env_id,
                    label,
                    spec,
                } => {
                    run_single_refresh(&engine, epoch_before, env_id, label, *spec).await;
                },
                RefreshPending::Remove { .. } | RefreshPending::Disabled { .. } => {
                    // SECTION B handles both inline; reaching here is a
                    // programming error in this module.
                    unreachable!("Remove / Disabled handled before spawn");
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
        let epoch = self.env_refresh_epoch.fetch_add(1, Ordering::AcqRel) + 1;
        self.env_registry.store(next_entries);
        self.env_catalogs.store(Arc::new(next_catalogs));
        self.env_disabled_specs.store(Arc::new(next_disabled));
        self.publish_env_refresh(Some(env_id.clone()), epoch);
    }

    /// Synchronous Disable: drops the env from `env_registry` +
    /// `env_catalogs` (a disabled env can't dispatch) and inserts the spec
    /// into `env_disabled_specs` so the IPC layer can list it under
    /// "Disabled" with an Enable action. Bumps the epoch BEFORE the swap
    /// so an in-flight background probe reads the bumped value on its CAS.
    fn apply_disable(
        &self,
        env_id: &environment::runtime::EnvId,
        label: String,
        spec: Option<environment::runtime::EnvSpec>,
        reason: Option<String>,
    ) {
        use std::sync::atomic::Ordering;

        let current_entries = self.env_registry.entries();
        let mut next_entries = (*current_entries).clone();
        next_entries.remove(env_id);
        let current_catalogs = self.env_catalogs.load();
        let mut next_catalogs = (**current_catalogs).clone();
        next_catalogs.remove(env_id);
        let current_disabled = self.env_disabled_specs.load();
        let mut next_disabled = (**current_disabled).clone();
        next_disabled.insert(
            env_id.clone(),
            environment::runtime::boot_probe::DisabledEnv {
                label,
                spec,
                reason,
            },
        );
        let epoch = self.env_refresh_epoch.fetch_add(1, Ordering::AcqRel) + 1;
        self.env_registry.store(next_entries);
        self.env_catalogs.store(Arc::new(next_catalogs));
        self.env_disabled_specs.store(Arc::new(next_disabled));
        self.publish_env_refresh(Some(env_id.clone()), epoch);
    }

    fn publish_env_refresh(&self, env_id: Option<environment::runtime::EnvId>, epoch: u64) {
        let send_result = self.env_refresh_tx.send(EnvRefreshEvent { env_id, epoch });
        drop(send_result);
    }

    /// Insert a new `EnvSpec` row and schedule a probe for it.
    ///
    /// Holds `env_refresh_lock` across the SQL write + the call to
    /// [`Self::refresh_environment_locked`] so concurrent refreshers see
    /// either the pre-insert or the post-insert spec table — never a
    /// partially-applied state.
    ///
    /// Errors when the id collides with an existing row or the spec fails
    /// to serialize.
    pub async fn add_env(
        self: &Arc<Self>,
        id: environment::runtime::EnvId,
        label: String,
        enabled: bool,
        spec: environment::runtime::EnvSpec,
    ) -> Result<()> {
        validate_id_spec_kind(&id, &spec)?;
        validate_workspace_binding(&id, &spec)?;
        let _guard = self.env_refresh_lock.lock().await;
        let spec_json =
            serde_json::to_string(&spec).map_err(|e| EngineError::Db(format!("EnvSpec: {e}")))?;
        let now = chrono::Utc::now().timestamp_millis();
        let conn = self.pool.get()?;
        conn.execute(
            "INSERT INTO env_specs (id, label, enabled, spec_json, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?5)",
            rusqlite::params![id.as_str(), label, i64::from(enabled), spec_json, now],
        )?;
        drop(conn);
        self.refresh_environment_locked(Some(&id))
    }

    /// Delete an `EnvSpec` row and tear down its registry + catalog entry.
    ///
    /// Holds `env_refresh_lock` across the SQL delete + the call to
    /// [`Self::refresh_environment_locked`]. With the row gone,
    /// [`environment::runtime::boot_probe::load_spec_single`] returns
    /// `None` and the refresh helper takes the synchronous Remove path
    /// (no background probe).
    pub async fn remove_env(self: &Arc<Self>, id: &environment::runtime::EnvId) -> Result<()> {
        let _guard = self.env_refresh_lock.lock().await;
        let conn = self.pool.get()?;
        conn.execute(
            "DELETE FROM env_specs WHERE id = ?1",
            rusqlite::params![id.as_str()],
        )?;
        drop(conn);
        self.refresh_environment_locked(Some(id))
    }

    /// Toggle the `enabled` flag on an existing `EnvSpec` row.
    ///
    /// Holds `env_refresh_lock` across the SQL update + the call to
    /// [`Self::refresh_environment_locked`]. Disabling a row causes the
    /// next refresh to drop it via the same Remove path as a delete; the
    /// `env_disabled_specs` map is populated by the boot probe path on
    /// subsequent reloads. A no-op (matching `enabled` flag) still
    /// triggers the refresh so callers can use this as a forced re-probe.
    pub async fn set_env_enabled(
        self: &Arc<Self>,
        id: &environment::runtime::EnvId,
        enabled: bool,
    ) -> Result<()> {
        // Local cannot be disabled. Snapshot construction defaults to Local
        // when a workflow omits `default_env`, and `validate_nodes` treats
        // Local as always-valid — a disabled Local would corrupt both
        // invariants. Reject at the writer so the IPC can render the
        // constraint explicitly.
        if !enabled && *id == environment::runtime::EnvId::local() {
            return Err(EngineError::EnvCannotBeDisabled(id.clone()));
        }
        let _guard = self.env_refresh_lock.lock().await;
        let now = chrono::Utc::now().timestamp_millis();
        let conn = self.pool.get()?;
        conn.execute(
            "UPDATE env_specs SET enabled = ?1, updated_at = ?2 WHERE id = ?3",
            rusqlite::params![i64::from(enabled), now, id.as_str()],
        )?;
        drop(conn);
        self.refresh_environment_locked(Some(id))
    }

    /// Append an env-local resource to `<env>.spec.resources` atomically.
    ///
    /// Holds `env_refresh_lock` across:
    ///   1. UPDATE `env_specs.spec_json` with the mutated resources vec
    ///   2. `refresh_environment_locked(Some(env))` so the per-env catalog
    ///      and registry layer pick up the new resource on the next probe.
    ///
    /// Errors:
    ///   - [`EngineError::EnvUnknown`] if no row matches `env_id`.
    ///   - [`EngineError::Db`] when a resource with the same id is already
    ///     declared inline on this env (collisions across scopes must use
    ///     `override_lower_scope` at higher layers; the env-local layer
    ///     never duplicates ids within itself).
    pub async fn add_env_local_resource(
        self: &Arc<Self>,
        env_id: &environment::runtime::EnvId,
        def: environment::runtime::ResourceDefinition,
    ) -> Result<()> {
        let _guard = self.env_refresh_lock.lock().await;
        let conn = self.pool.get()?;
        let mut row = environment::runtime::boot_probe::load_spec_single(&self.pool, env_id)
            .map_err(|e| EngineError::Db(format!("load env spec: {e}")))?
            .ok_or_else(|| EngineError::EnvUnknown(env_id.clone()))?;
        let spec = row.spec.as_mut().ok_or_else(|| {
            EngineError::Db(format!(
                "env '{}' requires reconfiguration and cannot accept new resources",
                env_id.as_str(),
            ))
        })?;
        if spec.resources().iter().any(|r| r.id == def.id) {
            return Err(EngineError::Db(format!(
                "resource id '{}' already declared on env '{}'",
                def.id.0,
                env_id.as_str(),
            )));
        }
        spec.resources_mut().push(def);
        let spec_json =
            serde_json::to_string(&*spec).map_err(|e| EngineError::Db(format!("EnvSpec: {e}")))?;
        let now = chrono::Utc::now().timestamp_millis();
        conn.execute(
            "UPDATE env_specs SET spec_json = ?1, updated_at = ?2 WHERE id = ?3",
            rusqlite::params![spec_json, now, env_id.as_str()],
        )?;
        drop(conn);
        self.refresh_environment_locked(Some(env_id))
    }

    /// Remove an env-local resource by id from `<env>.spec.resources`.
    ///
    /// Holds `env_refresh_lock` across the SQL UPDATE + the call to
    /// [`Self::refresh_environment_locked`] so callers see the new
    /// catalog state immediately after the await resolves.
    ///
    /// Errors:
    ///   - [`EngineError::EnvUnknown`] if no row matches `env_id`.
    ///   - [`EngineError::Db`] when `resource_id` is not present on the
    ///     env's inline list — surfaces as a clear error rather than a
    ///     silent no-op so the IPC can refuse stale UI clicks.
    pub async fn remove_env_local_resource(
        self: &Arc<Self>,
        env_id: &environment::runtime::EnvId,
        resource_id: &environment::runtime::ResourceId,
    ) -> Result<()> {
        let _guard = self.env_refresh_lock.lock().await;
        let conn = self.pool.get()?;
        let mut row = environment::runtime::boot_probe::load_spec_single(&self.pool, env_id)
            .map_err(|e| EngineError::Db(format!("load env spec: {e}")))?
            .ok_or_else(|| EngineError::EnvUnknown(env_id.clone()))?;
        let spec = row.spec.as_mut().ok_or_else(|| {
            EngineError::Db(format!(
                "env '{}' requires reconfiguration and cannot accept resource changes",
                env_id.as_str(),
            ))
        })?;
        let before = spec.resources().len();
        spec.resources_mut().retain(|r| &r.id != resource_id);
        if spec.resources().len() == before {
            return Err(EngineError::Db(format!(
                "resource id '{}' not declared on env '{}'",
                resource_id.0,
                env_id.as_str(),
            )));
        }
        let spec_json =
            serde_json::to_string(&*spec).map_err(|e| EngineError::Db(format!("EnvSpec: {e}")))?;
        let now = chrono::Utc::now().timestamp_millis();
        conn.execute(
            "UPDATE env_specs SET spec_json = ?1, updated_at = ?2 WHERE id = ?3",
            rusqlite::params![spec_json, now, env_id.as_str()],
        )?;
        drop(conn);
        self.refresh_environment_locked(Some(env_id))
    }

    /// Probe a Found HTTP resource directly from the engine process and
    /// derive a stable fingerprint from the response body.
    ///
    /// Read-only: this never mutates the env registry, the resource
    /// registry, or `env_specs`. The `HostDirect` wizard in the GUI calls
    /// this to populate the "Test direct access" preview before
    /// committing the result via [`Self::enable_host_direct`].
    ///
    /// The catalog supplies the `base_url` (so the probe targets whatever
    /// loopback the env's last probe resolved). The registry supplies the
    /// resource's probe routes; the first route advertising at least one
    /// `fingerprint_jsonpaths` entry is selected. Reqwest is issued via
    /// the shared client (same connection pool as built-in HTTP nodes)
    /// with a 5-second deadline applied around `send()`.
    ///
    /// Returns a [`HostDirectTestOutcome`] in every non-error path: an
    /// unreachable host or a parse failure still produces a well-formed
    /// outcome carrying the error message so the wizard can display it.
    /// Hard errors (env / resource / probe-spec missing) propagate as
    /// [`EngineError::EnvUnknown`] or [`EngineError::Db`].
    pub async fn test_host_direct(
        self: &Arc<Self>,
        env_id: &environment::runtime::EnvId,
        resource_id: &environment::runtime::ResourceId,
    ) -> Result<HostDirectTestOutcome> {
        use environment::runtime::{ProbeSpec, ResourceDetail, ResourceProbeOutcome};

        let catalogs = self.env_catalogs();
        let catalog = catalogs
            .get(env_id)
            .ok_or_else(|| EngineError::EnvUnknown(env_id.clone()))?;
        let outcome = catalog.resources.get(resource_id).ok_or_else(|| {
            EngineError::Db(format!(
                "resource '{}' has no catalog entry in env '{}'",
                resource_id.0,
                env_id.as_str(),
            ))
        })?;
        let base_url = match outcome {
            ResourceProbeOutcome::Found(ResourceDetail::HttpEndpoint { base_url, .. }) => {
                base_url.clone()
            },
            _ => {
                return Err(EngineError::Db(format!(
                    "resource '{}' is not a Found HTTP endpoint in env '{}'",
                    resource_id.0,
                    env_id.as_str(),
                )));
            },
        };

        let probe = resolve_probe_for_test(self, env_id, resource_id)?;
        let probe_route = match &probe {
            ProbeSpec::Http { routes, .. } => routes
                .iter()
                .find(|r| !r.fingerprint_jsonpaths.is_empty())
                .ok_or_else(|| {
                    EngineError::Db(format!(
                        "resource '{}' has no probe route with fingerprint_jsonpaths",
                        resource_id.0,
                    ))
                })?,
            _ => {
                return Err(EngineError::Db(format!(
                    "resource '{}' is not an HTTP probe",
                    resource_id.0,
                )));
            },
        };

        Ok(probe_host_direct(
            base_url,
            probe_route.path.clone(),
            probe_route.method,
            probe_route.fingerprint_jsonpaths.clone(),
        )
        .await)
    }

    /// Persist a [`environment::runtime::HostDirectVerification`] inline
    /// on the env's `EnvSpec.host_direct_verifications` map.
    ///
    /// Holds `env_refresh_lock` across the SQL `UPDATE` + the call to
    /// [`Self::refresh_environment_locked`] so concurrent dispatchers see
    /// either the pre- or the post-mutation spec — never a partially
    /// applied state. Existing entries for the same resource id are
    /// replaced (the wizard re-runs verification when a fingerprint
    /// drifts).
    ///
    /// Errors:
    ///   - [`EngineError::EnvUnknown`] if no row matches `env_id`.
    ///   - [`EngineError::Db`] when the env kind does not carry
    ///     `host_direct_verifications` (today: `Ssh`).
    pub async fn enable_host_direct(
        self: &Arc<Self>,
        env_id: &environment::runtime::EnvId,
        resource_id: &environment::runtime::ResourceId,
        verification: environment::runtime::HostDirectVerification,
    ) -> Result<()> {
        let _guard = self.env_refresh_lock.lock().await;
        let mut row = environment::runtime::boot_probe::load_spec_single(&self.pool, env_id)
            .map_err(|e| EngineError::Db(format!("load env spec: {e}")))?
            .ok_or_else(|| EngineError::EnvUnknown(env_id.clone()))?;
        let spec = row.spec.as_mut().ok_or_else(|| {
            EngineError::Db(format!(
                "env '{}' requires reconfiguration and cannot accept host-direct verifications",
                env_id.as_str(),
            ))
        })?;
        let kind = spec.kind_str();
        let map = spec.host_direct_verifications_mut().ok_or_else(|| {
            EngineError::Db(format!(
                "env '{}' kind '{kind}' does not support host-direct verifications",
                env_id.as_str(),
            ))
        })?;
        map.insert(resource_id.clone(), verification);
        let spec_json =
            serde_json::to_string(&*spec).map_err(|e| EngineError::Db(format!("EnvSpec: {e}")))?;
        let now = chrono::Utc::now().timestamp_millis();
        let conn = self.pool.get()?;
        conn.execute(
            "UPDATE env_specs SET spec_json = ?1, updated_at = ?2 WHERE id = ?3",
            rusqlite::params![spec_json, now, env_id.as_str()],
        )?;
        drop(conn);
        self.refresh_environment_locked(Some(env_id))
    }

    /// Perform a TOFU SSH enrollment: connect to the remote host, capture the
    /// server's public key during the transport handshake (before any auth),
    /// and return it as an [`SshHostKeyPin`] ready for inline persistence.
    ///
    /// The caller is responsible for persisting the pin (e.g. via a follow-up
    /// call to `add_env` or by patching the existing `EnvSpec`). This method
    /// is purely read-only — it opens a connection, reads the host key, and
    /// disconnects without touching the database.
    ///
    /// `spec` must be `EnvSpec::Ssh`; any other variant returns an error.
    /// The `(host, port)` tuple is passed directly to `russh::client::connect`
    /// which accepts `tokio::net::ToSocketAddrs` — async DNS is handled
    /// internally, so no blocking `getaddrinfo` call occurs on the tokio thread.
    /// A 10-second TCP connect timeout guards against unreachable hosts.
    /// Connection failures are reported as [`EngineError::Dispatch`] with
    /// [`DispatchError::EnvUnreachable`].
    pub async fn test_ssh_enrollment(
        self: &Arc<Self>,
        _env_id: &environment::runtime::EnvId,
        _label: String,
        spec: environment::runtime::EnvSpec,
    ) -> Result<environment::runtime::SshHostKeyPin> {
        use std::sync::Arc as StdArc;
        use std::time::Duration;

        use environment::runtime::error::DispatchError;
        use environment::runtime::ssh::config::extract_target;
        use environment::runtime::ssh::host_key::HostKeyHandler;

        let (host, port) = extract_target(&spec).map_err(|e| DispatchError::EnvUnreachable {
            env_id: "ssh:enrollment".to_string(),
            reason: e.to_string(),
        })?;

        let config = russh::client::Config {
            inactivity_timeout: Some(Duration::from_secs(30)),
            ..Default::default()
        };
        let handler = HostKeyHandler::enroll();
        let captured = handler.captured_key();

        // Pass the unresolved (host, port) tuple directly — russh/tokio performs
        // async DNS internally (no blocking getaddrinfo on a worker thread).
        // Wrap in a 10-second timeout so an unreachable host (TCP SYN to a
        // dropped port) fails fast instead of hanging for the OS retransmit window.
        let session = tokio::time::timeout(
            Duration::from_secs(10),
            russh::client::connect(StdArc::new(config), (host.as_str(), port), handler),
        )
        .await
        .map_err(|_| DispatchError::EnvUnreachable {
            env_id: format!("{host}:{port}"),
            reason: "SSH enrollment: connection timed out".to_string(),
        })?
        .map_err(|e| DispatchError::EnvUnreachable {
            env_id: format!("{host}:{port}"),
            reason: format!("SSH enrollment connect: {e}"),
        })?;

        let presented =
            captured
                .lock()
                .await
                .take()
                .ok_or_else(|| DispatchError::EnvUnreachable {
                    env_id: format!("{host}:{port}"),
                    reason: "SSH enrollment: no host key captured".to_string(),
                })?;

        // Disconnect gracefully; ignore errors (connection may already be torn down).
        drop(
            session
                .disconnect(russh::Disconnect::ByApplication, "enrollment", "en")
                .await,
        );

        Ok(presented.to_pin(chrono::Utc::now()))
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
        let env_disabled = self.env_disabled_specs.load_full();
        let (wf, warnings) = workflows::load_in_registry(
            home,
            id,
            &self.resource_registry,
            &self.env_registry,
            &env_disabled,
        )?;
        Ok((Arc::new(wf), warnings))
    }

    /// Freeze the per-env substrate a run needs into an [`Arc<RunSnapshot>`].
    ///
    /// Resolves the effective scope from `default_env ∪ envs_in_scope`,
    /// validates every entry against the engine's env registry, and clones
    /// the matching dispatcher, last-probed catalog, and `EnvSpec` for each.
    /// The result is immutable for the duration of the run — concurrent
    /// `refresh_environment` calls cannot invalidate it.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::EnvUnknown`] when any env in scope is missing
    /// from [`Self::env_registry`]. Callers that surfaced the workflow
    /// through [`Self::load_workflow_for_run`] should have failed validation
    /// already; this is the defence-in-depth check at run start.
    pub fn build_run_snapshot(
        &self,
        run_id: &str,
        workflow_id: environment::runtime::WorkflowId,
        default_env: environment::runtime::EnvId,
        envs_in_scope: &[environment::runtime::EnvId],
    ) -> Result<Arc<environment::runtime::run_snapshot::RunSnapshot>> {
        use std::collections::HashMap as StdHashMap;

        // Effective scope = default_env ∪ envs_in_scope (deduplicated).
        let mut scope: Vec<environment::runtime::EnvId> =
            Vec::with_capacity(envs_in_scope.len() + 1);
        scope.push(default_env.clone());
        for env in envs_in_scope {
            if !scope.contains(env) {
                scope.push(env.clone());
            }
        }

        let entries = self.env_registry.entries();
        let catalogs_map = self.env_catalogs.load_full();

        let mut dispatchers: StdHashMap<
            environment::runtime::EnvId,
            Arc<dyn environment::runtime::dispatcher::Dispatcher>,
        > = StdHashMap::with_capacity(scope.len());
        let mut catalogs: StdHashMap<
            environment::runtime::EnvId,
            Arc<environment::runtime::run_catalog::RunCatalog>,
        > = StdHashMap::with_capacity(scope.len());
        let mut specs: StdHashMap<environment::runtime::EnvId, environment::runtime::EnvSpec> =
            StdHashMap::with_capacity(scope.len());

        // Per-env env-local resource definitions, harvested from each
        // in-scope EnvSpec. These land at `ScopeKey::EnvLocal { id: env }`
        // in the per-run registry snapshot below — NOT the engine-level
        // registry. Workflows referencing a resource from another env's
        // scope chain see `None` at resolve time.
        let mut env_locals: Vec<(
            environment::runtime::EnvId,
            Vec<environment::runtime::resource::ResourceDefinition>,
        )> = Vec::with_capacity(scope.len());

        for env in &scope {
            let entry = entries
                .get(env)
                .ok_or_else(|| EngineError::EnvUnknown(env.clone()))?;
            dispatchers.insert(env.clone(), Arc::clone(&entry.dispatcher));
            specs.insert(env.clone(), entry.info.spec.clone());

            let env_local = match &entry.info.spec {
                environment::runtime::EnvSpec::Local { resources, .. }
                | environment::runtime::EnvSpec::WslDistro { resources, .. }
                | environment::runtime::EnvSpec::Ssh { resources, .. }
                | environment::runtime::EnvSpec::Container { resources, .. } => resources.clone(),
            };
            env_locals.push((env.clone(), env_local));

            // Last-probed catalog Arc, cloned out of the engine's
            // ArcSwap. Envs without a probed catalog yet (newly added /
            // mid-refresh) get an empty `ResourceCatalog` keyed to the env so
            // `RunCatalog::new` still receives an `env_id`-consistent frozen
            // view.
            let catalog_arc = catalogs_map.get(env).cloned().unwrap_or_else(|| {
                Arc::new(environment::runtime::catalog::ResourceCatalog {
                    env_id: env.clone(),
                    registry_revision: 0,
                    probed_at: chrono::Utc::now(),
                    resources: std::collections::HashMap::new(),
                })
            });
            catalogs.insert(
                env.clone(),
                Arc::new(environment::runtime::run_catalog::RunCatalog::new(
                    env.clone(),
                    catalog_arc,
                )),
            );
        }

        let engine_registry_snap = self.resource_registry.snapshot();
        let run_registry = engine_registry_snap
            .with_env_local_overlay(&env_locals)
            .map_err(|e| {
                EngineError::Workflows(crate::workflows::WorkflowsError::Other(e.to_string()))
            })?;

        Ok(Arc::new(environment::runtime::run_snapshot::RunSnapshot {
            run_id: run_id.to_string(),
            workflow_id,
            default_env,
            registry: run_registry,
            dispatchers: Arc::new(dispatchers),
            catalogs: Arc::new(catalogs),
            specs: Arc::new(specs),
        }))
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

    /// Look up the per-run `RunSnapshot` for a currently active run.
    ///
    /// Returns `None` once the run has completed (the RAII guard
    /// inside the spawned run task removes the entry on drop). Used
    /// by isolation tests to assert that `refresh_environment` does
    /// not perturb an in-flight run's frozen view of the env
    /// substrate. Gated behind `cfg(test)` + the `testing` feature so
    /// production code cannot rely on this introspection.
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    pub fn run_snapshot(&self, run_id: &str) -> Option<Arc<environment::runtime::RunSnapshot>> {
        self.run_snapshots.lock().ok()?.get(run_id).map(Arc::clone)
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
        /// Persisted display label, propagated into the rebuilt `EnvInfo`.
        label: String,
        /// `EnvSpec` is ~272 bytes; boxing keeps the enum variant size in
        /// line with the other cheap variants (clippy `large_enum_variant`).
        spec: Box<environment::runtime::EnvSpec>,
    },
    /// Row present but `enabled = 0` at lock-read time. Handled inline (no
    /// probe); the env's entry + catalog are dropped from the active maps
    /// and the spec is landed in `env_disabled_specs` so the IPC layer can
    /// surface it under "Disabled" with an Enable action.
    Disabled {
        env_id: environment::runtime::EnvId,
        /// Persisted display label, threaded through so the IPC entry shows
        /// the user-customised name.
        label: String,
        /// Decoded spec, absent for legacy rows that cannot be parsed.
        spec: Option<Box<environment::runtime::EnvSpec>>,
        /// Engine-assigned reason, forwarded into `DisabledEnv`.
        reason: Option<String>,
    },
    /// Row absent at lock-read time. Handled inline (no probe); the env's
    /// entry, catalog, and any prior disabled-spec are all dropped.
    Remove { env_id: environment::runtime::EnvId },
}

/// Reject (id, spec) combinations whose kinds don't match — e.g. `wsl:Ubuntu`
/// paired with `EnvSpec::Local`, or a `wsl:` id whose suffix differs from the
/// spec's `name`. Without this, the boot probe would silently construct a
/// `LocalDispatcher` under a WSL id (and never reach the actual distro).
fn validate_id_spec_kind(
    id: &environment::runtime::EnvId,
    spec: &environment::runtime::EnvSpec,
) -> Result<()> {
    use environment::runtime::{EnvId, EnvSpec};

    let id_str = id.as_str();
    let mismatch = || EngineError::EnvIdSpecMismatch {
        id: id_str.to_string(),
        spec_kind: spec.kind_str(),
    };

    if id_str == EnvId::LOCAL {
        return if matches!(spec, EnvSpec::Local { .. }) {
            Ok(())
        } else {
            Err(mismatch())
        };
    }
    if let Some(suffix) = id_str.strip_prefix("wsl:") {
        return match spec {
            EnvSpec::WslDistro { name, .. } if name == suffix => Ok(()),
            _ => Err(mismatch()),
        };
    }
    if id_str.starts_with("ssh:") {
        return if matches!(spec, EnvSpec::Ssh { .. }) {
            Ok(())
        } else {
            Err(mismatch())
        };
    }
    if id_str.starts_with("container:") {
        return if matches!(spec, EnvSpec::Container { .. }) {
            Ok(())
        } else {
            Err(mismatch())
        };
    }
    Err(mismatch())
}

/// Reject workspace bindings an env variant cannot honour.
///
/// SSH reaches the workspace over SFTP, so only `Sync` (upload) or `Unsupported`
/// (no workspace) apply. `Shared`/`Translated`/`BindMount` all assume the host
/// filesystem is reachable in-place inside the env, which SSH cannot provide —
/// reject them at [`Engine::add_env`] so the boot probe never builds an SSH
/// dispatcher with an impossible binding.
fn validate_workspace_binding(
    id: &environment::runtime::EnvId,
    spec: &environment::runtime::EnvSpec,
) -> Result<()> {
    use environment::runtime::{EnvSpec, WorkspaceBinding};

    if let EnvSpec::Ssh {
        workspace_binding, ..
    } = spec
    {
        let rejected = match workspace_binding {
            WorkspaceBinding::Shared => Some("shared"),
            WorkspaceBinding::Translated => Some("translated"),
            WorkspaceBinding::BindMount { .. } => Some("bind_mount"),
            WorkspaceBinding::Sync { .. } | WorkspaceBinding::Unsupported => None,
        };
        if let Some(binding) = rejected {
            return Err(EngineError::EnvWorkspaceBindingUnsupported {
                id: id.as_str().to_string(),
                binding,
            });
        }
    }
    Ok(())
}

/// Background refresh body for `RefreshPending::Full`. Re-probes every env
/// in the DB; CAS-commits the result against `epoch_before` so a concurrent
/// `refresh_environment` write that ran during the probe drops this result
/// silently. Tracing logs the loss at debug level.
async fn run_full_refresh(engine: &Arc<Engine>, epoch_before: u64) {
    use std::sync::atomic::Ordering;
    let outcome = environment::runtime::boot_probe::run(
        &engine.pool,
        &engine.resource_registry,
        Arc::clone(&engine.secrets_store),
    )
    .await;
    let _guard = engine.env_refresh_lock.lock().await;
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
            engine.publish_env_refresh(None, epoch_before + 1);
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
    label: String,
    spec: environment::runtime::EnvSpec,
) {
    use std::sync::atomic::Ordering;
    match environment::runtime::boot_probe::run_single(
        &engine.resource_registry,
        &env_id,
        &label,
        &spec,
        Arc::clone(&engine.secrets_store),
    )
    .await
    {
        Ok(single) => {
            let _guard = engine.env_refresh_lock.lock().await;
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
                    next_disabled.remove(&env_id);
                    engine.env_disabled_specs.store(Arc::new(next_disabled));
                    engine.publish_env_refresh(Some(env_id.clone()), epoch_before + 1);
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

    // ── H2-T4: SSH workspace-binding validation ──────────────────────────────

    fn sync_binding() -> environment::runtime::WorkspaceBinding {
        environment::runtime::WorkspaceBinding::Sync {
            env_path_template: "/tmp/ordius-{{run.id}}".into(),
            strategy: environment::runtime::SyncStrategy::Sftp,
            write_back: environment::runtime::WriteBackPolicy::None,
        }
    }

    fn ssh_spec(binding: environment::runtime::WorkspaceBinding) -> environment::runtime::EnvSpec {
        environment::runtime::EnvSpec::Ssh {
            host: "example.test".into(),
            port: 22,
            user: "u".into(),
            auth: environment::runtime::SshAuth::KeyFile {
                path: "/nonexistent/key".into(),
                passphrase_ref: None,
            },
            host_key_pins: vec![],
            workspace_binding: binding,
            resources: vec![],
        }
    }

    #[test]
    fn validate_workspace_binding_restricts_ssh_only() {
        use environment::runtime::{EnvId, EnvSpec, WorkspaceBinding};

        let ssh = EnvId::ssh("box");
        for binding in [
            WorkspaceBinding::Shared,
            WorkspaceBinding::Translated,
            WorkspaceBinding::BindMount {
                env_path: "/x".into(),
            },
        ] {
            let err = validate_workspace_binding(&ssh, &ssh_spec(binding)).unwrap_err();
            assert!(
                matches!(err, EngineError::EnvWorkspaceBindingUnsupported { .. }),
                "SSH binding must be rejected, got {err:?}"
            );
        }
        validate_workspace_binding(&ssh, &ssh_spec(sync_binding())).expect("SSH Sync ok");
        validate_workspace_binding(&ssh, &ssh_spec(WorkspaceBinding::Unsupported))
            .expect("SSH Unsupported ok");

        // Non-SSH specs are not restricted by this check.
        let local = EnvSpec::Local {
            resources: vec![],
            host_direct_verifications: std::collections::HashMap::new(),
        };
        validate_workspace_binding(&EnvId::local(), &local).expect("Local ok");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn add_env_rejects_ssh_shared_binding() {
        use environment::runtime::{EnvId, WorkspaceBinding};

        let dir = TempDir::new().unwrap();
        let engine = std::sync::Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());
        let err = engine
            .add_env(
                EnvId::ssh("box"),
                "SSH Box".into(),
                true,
                ssh_spec(WorkspaceBinding::Shared),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, EngineError::EnvWorkspaceBindingUnsupported { .. }),
            "add_env must reject SSH+Shared, got {err:?}"
        );
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
