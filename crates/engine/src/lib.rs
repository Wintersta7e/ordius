//! Ordius workflow engine. See `docs/` at the repo root for the spec.
//!
//! Public surface (filled in by later tasks):
//! - types: `Workflow`, `Node`, `Edge`, `NodeType`, `Run`, `RunEvent`
//! - scheduler: `Scheduler`
//! - executor: `NodeExecutor` + in-process / subprocess / container impls
//! - storage: `Db`, `RunRecorder`
//! - templates: substitute, redact
//! - secrets: keyring read/write

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

pub use emitter::Emitter;
pub use error::{EngineError, Result};
pub use events::{EventType, RunEvent};
pub use executor::{InProcessExecutor, NodeError, NodeExecutor, NodeOutputs, RunContext};
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
