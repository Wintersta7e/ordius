//! Typed environment runtime.
//!
//! Phase A of the env-runtime rewrite (`docs/plans/environment-runtime.md`).
//! Coexists with the legacy `environment::{types, detect, local, wsl, custom}`
//! modules until later phases wire this into `Engine` and IPC.

pub mod builtin;
pub mod catalog;
pub mod dispatcher;
pub mod env;
pub mod error;
pub mod helper;
pub mod local;
pub mod plan;
pub mod registry;
pub mod resource;
pub mod transport;
pub mod user_file;
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
    ProvenRoute, ResourceCatalog, ResourceDetail, ResourceProbeOutcome, RouteOrigin,
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
