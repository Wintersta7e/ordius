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
    /// Generic IO failure (subprocess pipes, filesystem reads outside the loader, etc.).
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
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
    /// A code path that intentionally returns `NotImplemented` in this
    /// release. The static string identifies the missing capability
    /// (e.g. `"container backend"` for the v1.0 stub).
    #[error("not implemented: {0}")]
    NotImplemented(&'static str),
}

/// Engine-wide `Result` alias — every public engine fn returns this.
pub type Result<T> = std::result::Result<T, EngineError>;
