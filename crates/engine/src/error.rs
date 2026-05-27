//! Unified top-level error type for the engine. Per-module errors
//! convert into the corresponding `EngineError` variant via `#[from]`.

use thiserror::Error;

/// Top-level engine error.
///
/// Aggregates load + validation + IO failures, plus engine-internal
/// failure modes (storage, template substitution, secret resolution,
/// per-node failures, workflow-locking, shutdown, and an explicit
/// `NotImplemented` for the v1.0 `Container` backend stub).
#[derive(Debug, Error)]
pub enum EngineError {
    /// Failure loading a workflow file.
    #[error("load: {0}")]
    Load(#[from] crate::loader::LoadError),
    /// Workflow failed structural validation.
    #[error("validation: {0}")]
    Validation(#[from] crate::validation::ValidationError),
    /// IO failure with caller-provided context (subprocess pipes,
    /// filesystem reads outside the loader, etc.). The `context`
    /// string is required at every construction site — there is no
    /// `#[from]` conversion, so `?` cannot silently route an
    /// `io::Error` into a context-less variant.
    #[error("io: {context}: {source}")]
    Io {
        /// Caller-supplied description of what was being attempted
        /// (e.g. `"opening run database"`, `"spawning shell node"`).
        context: String,
        /// Underlying `io::Error`.
        #[source]
        source: std::io::Error,
    },
    /// Storage layer failure (`SQLite` operations, connection pool, etc.).
    #[error("db: {0}")]
    Db(String),
    /// Template substitution failure (missing variable, type coercion failure, etc.).
    #[error("template: {0}")]
    Template(String),
    /// Secret resolution failure (keyring lookup, missing entry, etc.).
    #[error("secret: {0}")]
    Secret(String),
    /// A node failed with a typed reason (subprocess error, HTTP error, executor refusal, etc.).
    #[error("node {node_id}: {message}")]
    Node {
        /// Offending node id.
        node_id: String,
        /// Failure description.
        message: String,
    },
    /// Cross-process or cross-binary lock conflict: a run is already
    /// active for this workflow id. Maps to CLI exit code 3.
    #[error("workflow {id} already running (run {run_id})")]
    AlreadyRunning {
        /// Workflow id being requested.
        id: String,
        /// Existing run id holding the lock.
        run_id: String,
    },
    /// Engine has been asked to shut down and is no longer accepting new runs.
    #[error("engine shutting down")]
    ShuttingDown,
    /// Dispatcher / runtime-layer error (environment unreachable, missing
    /// resource or capability, workspace setup failure, path translation,
    /// or spawn failure).
    #[error("dispatch: {0}")]
    Dispatch(#[from] crate::environment::runtime::error::DispatchError),
    /// User-global / built-in / workflow resource registry seeding failed.
    /// Wraps both the TOML loader error and registry-side override-required
    /// rejections so callers don't need to know which layer produced the failure.
    #[error("resources: {0}")]
    Resources(#[from] crate::environment::runtime::user_file::ResourcesFileError),
    /// Workflow filesystem helper (load / save / scope install / scope
    /// removal) failed. Covers retired-id rejection, unknown-resource
    /// references, unadvertised capabilities, and workflow-scope install
    /// errors raised by the centralised `load_workflow_for_run` path.
    #[error("workflow: {0}")]
    Workflows(#[from] crate::workflows::WorkflowsError),
    /// Workflow-scope installation rejected by the registry. Surfaced by
    /// the safety-net install at the top of [`crate::Engine::start_run`]
    /// for programmatic `Workflow` construction paths that bypass the
    /// centralised loader.
    #[error("workflow scope: {0}")]
    Scope(#[from] crate::environment::runtime::WorkflowScopeError),
    /// A node's `target_env` (or the workflow's `default_env`) refers to an
    /// env id that is not present in the engine's env registry. Raised by
    /// [`crate::Engine::build_run_snapshot`] when freezing the per-env
    /// dispatchers + catalogs at run start.
    #[error("env '{0}' not in the engine's env registry")]
    EnvUnknown(crate::environment::runtime::EnvId),
    /// A code path that intentionally returns `NotImplemented` in this
    /// release. The static string identifies the missing capability
    /// (e.g. `"container backend"` for the v1.0 stub).
    #[error("not implemented: {0}")]
    NotImplemented(&'static str),
}

impl From<rusqlite::Error> for EngineError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Db(e.to_string())
    }
}

impl From<r2d2::Error> for EngineError {
    fn from(e: r2d2::Error) -> Self {
        Self::Db(e.to_string())
    }
}

/// Engine-wide `Result` alias — every public engine fn returns this.
pub type Result<T> = std::result::Result<T, EngineError>;
