//! Typed environment runtime — Phase A of the env-runtime rewrite.
//!
//! New code lives here alongside the legacy `environment::{types, detect, local,
//! wsl, custom}` modules. See `docs/plans/environment-runtime.md`.

pub mod builtin;
pub mod catalog;
pub mod dispatcher;
pub mod env;
pub mod error;
pub mod local;
pub mod plan;
pub mod registry;
pub mod resource;
pub mod transport;

#[cfg(any(test, feature = "testing"))]
pub mod fake;

pub use env::{
    BindMount, ConflictDetect, EnvId, EnvInfo, EnvKind, EnvSpec, EnvState, HostDirectMethod,
    HostDirectVerification, RunId, SecretRef, SyncStrategy, WorkflowId, WorkspaceBinding,
    WriteBackPolicy, default_max_files,
};

pub use resource::{
    ApiFlavor, Capability, HttpProbeMethod, HttpProbeRoute, ProbeSpec, ResourceDefinition,
    ResourceId, ResourceKind, ResourceRef,
};

pub use catalog::{
    ProvenRoute, ResourceCatalog, ResourceDetail, ResourceProbeOutcome, RouteOrigin,
};

pub use plan::{ProbePlan, ProbeSummary};

pub use transport::{
    EnvPath, HttpError, HttpMethod, HttpRequest, HttpResponse, ProcessCmd, WorkspaceHandle,
};

pub use error::{DispatchError, RegistryError};

pub use builtin::{BUILTIN_RESOURCES, builtin_by_id};

pub use registry::{RegistryInner, ScopeKey};
