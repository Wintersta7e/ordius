//! Typed environment runtime.
//!
//! Implementation of `docs/plans/environment-runtime.md`. The Engine seeds
//! a [`ResourceRegistry`] from [`BUILTIN_RESOURCES`] plus `<home>/resources.toml`
//! at boot; workflows install their own `resources:` block under
//! `ScopeKey::Workflow { id }` at load time. Dispatchers (`LocalDispatcher`,
//! `WslDispatcher`) read the resolved view of the registry when probing.
//!
//! Coexists with the legacy `environment::{types, detect, local, wsl, custom}`
//! modules until later phases wire this into IPC.

pub mod builtin;
pub mod catalog;
pub mod dispatcher;
pub mod env;
pub mod env_registry;
pub mod error;
pub mod helper;
pub mod local;
pub mod plan;
pub mod registry;
pub mod resource;
pub mod run_catalog;
pub mod run_snapshot;
pub mod search_paths;
pub mod transport;
pub mod user_file;
pub mod workflow_scope;
pub mod wsl;

/// Test-support fixtures (requires `features = ["testing"]` or `#[cfg(test)]`).
#[cfg(any(test, feature = "testing"))]
pub mod fake;

// ── Identity, spec, state ────────────────────────────────────────────────────

/// Environment identifier, kind, spec variants, state, and runtime info.
pub use env::{
    BindMount, ConflictDetect, EnvId, EnvInfo, EnvKind, EnvSpec, EnvState, HostDirectMethod,
    HostDirectVerification, RunId, SecretRef, SyncStrategy, WorkflowId, WorkspaceBinding,
    WriteBackPolicy, default_max_files,
};

// ── Resource definitions + probe specs ──────────────────────────────────────

/// Resource identity, kinds, capabilities, probe specs, and node config refs.
pub use resource::{
    ApiFlavor, Capability, HttpProbeMethod, HttpProbeRoute, ProbeSpec, ResourceDefinition,
    ResourceId, ResourceKind, ResourceRef,
};

// ── Catalog + probe outcomes ─────────────────────────────────────────────────

/// Catalog holding post-probe outcomes per resource; detail and route types.
pub use catalog::{
    ProvenRoute, ResourceCatalog, ResourceDetail, ResourceProbeOutcome, RouteAddress, RouteOrigin,
};

// ── Probe plan + summary ─────────────────────────────────────────────────────

/// Input plan (resolved definitions, timeouts, concurrency) and output summary.
pub use plan::{ProbePlan, ProbeSummary};

// ── Transport primitives ─────────────────────────────────────────────────────

/// HTTP request/response/error, argv-only `ProcessCmd`, `EnvPath` newtype,
/// and `WorkspaceHandle` (drop-fires teardown).
pub use transport::{
    EnvPath, HttpError, HttpMethod, HttpRequest, HttpResponse, ProcessCmd, WorkspaceHandle,
};

// ── Errors ───────────────────────────────────────────────────────────────────

/// `DispatchError` (wires into `EngineError` via `#[from]`) and `RegistryError`.
pub use error::{DispatchError, RegistryError};

// ── Registry ─────────────────────────────────────────────────────────────────

/// Scoped resource registry: built-in → user-global → workflow → env layers.
pub use registry::{RegistryInner, ResourceRegistry, ScopeKey};

// ── Built-ins ────────────────────────────────────────────────────────────────

/// Static `BUILTIN_RESOURCES` slice, `builtin_by_id` accessor, and the
/// `install_builtin_resources` seeder consumed by `Engine::new`.
pub use builtin::{BUILTIN_RESOURCES, builtin_by_id, install_builtin_resources};

// ── User-global resources file ───────────────────────────────────────────────

/// User-global `resources.toml` loader.
pub use user_file::{ResourcesFileError, UserResourcesFile, load_user_resources};

// ── Workflow-scoped resources ────────────────────────────────────────────────

/// Install / remove / snapshot workflow-scoped resources from the registry.
pub use workflow_scope::{
    WorkflowScopeError, install_workflow_resources, remove_workflow_scope, snapshot_workflow_scope,
};

// ── Traits ───────────────────────────────────────────────────────────────────

/// `Dispatcher` (env abstraction) and `HttpTransport` (pluggable HTTP layer)
/// traits, plus `ResponseStream` type alias.
pub use dispatcher::{Dispatcher, HttpTransport, ResponseStream};

// ── Local dispatcher ─────────────────────────────────────────────────────────

/// `LocalDispatcher` and `LocalHttpTransport` — production impl for the host env.
pub use local::{LocalDispatcher, LocalHttpTransport};

// ── Test-support fixture ─────────────────────────────────────────────────────

/// In-process fake dispatcher with per-resource seeded outcomes. Used by unit
/// and integration tests; emits `EnvLoopback` route origin.
#[cfg(any(test, feature = "testing"))]
pub use fake::{FakeHttpTransport, FakeRemoteDispatcher, FakeResource};

// ── Run-time snapshot ────────────────────────────────────────────────────────

/// Per-run frozen view (registry + dispatchers + catalogs).
pub use run_snapshot::RunSnapshot;

/// Per-env probe catalog with run-local monotonic overlay + singleflight re-probe.
pub use run_catalog::RunCatalog;

// ── Engine env registry ──────────────────────────────────────────────────────

/// Engine-owned per-env state: `EnvEntry` pairs `Arc<EnvInfo>` with
/// `Arc<dyn Dispatcher>`; `EnvRegistry` is the ArcSwap-backed map that the
/// boot probe and refresh swap atomically.
pub use env_registry::{EnvEntry, EnvRegistry};
