//! `NodeExecutor` trait + backend implementations.
//! Spec: `docs/02-engine-model.md` "The executor".

pub mod builtins;
pub mod context;
pub mod in_process;

pub use context::RunContext;
pub use in_process::InProcessExecutor;

use crate::types::{Node, NodeType, PortValue};
use async_trait::async_trait;
use std::collections::HashMap;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

/// Per-node execution failure modes.
///
/// `Config` is programmer-facing (bad workflow definition); the
/// remaining variants are runtime failures observed by an
/// executor. `Io` carries explicit caller-supplied context — no
/// `#[from]` conversion, so a stray `?` cannot route an
/// `io::Error` into a context-less variant.
#[derive(Debug, Error)]
pub enum NodeError {
    /// Template substitution failed.
    #[error("template: {0}")]
    Template(String),
    /// Required config field missing or invalid.
    #[error("config: {0}")]
    Config(String),
    /// IO failure with caller-provided context.
    #[error("io: {context}: {source}")]
    Io {
        /// Caller-supplied description of what was being attempted
        /// (e.g. `"opening run workspace"`).
        context: String,
        /// Underlying `io::Error`.
        #[source]
        source: std::io::Error,
    },
    /// HTTP request failure (used by the `http` node in Phase 7).
    #[error("http: {0}")]
    Http(String),
    /// Subprocess invocation failure (used by the `shell` node in Phase 6).
    #[error("subprocess: {0}")]
    Subprocess(String),
    /// Node exceeded its `timeout_ms` budget.
    #[error("timeout after {0}ms")]
    Timeout(u64),
    /// The node's `CancellationToken` was triggered before completion.
    #[error("cancelled")]
    Cancelled,
    /// No executor in the registry knows how to run this node type.
    #[error("not implemented")]
    NotImplemented,
    /// Catch-all for everything that doesn't fit above.
    #[error("{0}")]
    Other(String),
}

/// A node's emitted outputs after execution: port name → value.
pub type NodeOutputs = HashMap<String, PortValue>;

/// Trait implemented by concrete executors (in-process built-ins,
/// subprocess executor, future container executor).
///
/// Each call receives a fresh [`CancellationToken`] that the
/// executor must honor — the engine triggers it on timeout, on
/// graceful shutdown, or when the run is explicitly stopped.
#[async_trait]
pub trait NodeExecutor: Send + Sync {
    /// Whether this executor can run nodes of the given type.
    fn supports(&self, node_type: &NodeType) -> bool;

    /// Execute the node, returning its emitted outputs on success.
    async fn run(
        &self,
        node: &Node,
        node_type: &NodeType,
        ctx: &RunContext,
        cancel: CancellationToken,
    ) -> Result<NodeOutputs, NodeError>;
}
