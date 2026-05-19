//! In-process dispatcher: routes nodes to the right built-in executor.

use crate::executor::builtins::{ConditionExecutor, DelayExecutor, TransformExecutor};
use crate::executor::{NodeError, NodeExecutor, NodeOutputs, RunContext};
use crate::types::{ExecutionBackend, Node, NodeType};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

/// Top-level in-process executor.
///
/// Owns one instance of each `InProcess`-backed built-in
/// (`DelayExecutor`, `TransformExecutor`, `ConditionExecutor`)
/// and forwards each `run` call to whichever one supports the
/// node. When the engine's top-level dispatcher sees
/// `ExecutionBackend::Subprocess`, it falls through to the
/// subprocess executor instead.
pub struct InProcessExecutor {
    executors: Vec<Box<dyn NodeExecutor>>,
}

impl InProcessExecutor {
    /// Construct a dispatcher with the v1.0 built-ins registered.
    #[must_use]
    pub fn new() -> Self {
        Self {
            executors: vec![
                Box::new(DelayExecutor),
                Box::new(TransformExecutor),
                Box::new(ConditionExecutor),
            ],
        }
    }
}

impl Default for InProcessExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl NodeExecutor for InProcessExecutor {
    fn supports(&self, nt: &NodeType) -> bool {
        nt.execution.backend == ExecutionBackend::InProcess
            && self.executors.iter().any(|e| e.supports(nt))
    }

    async fn run(
        &self,
        node: &Node,
        nt: &NodeType,
        ctx: &RunContext,
        cancel: CancellationToken,
    ) -> Result<NodeOutputs, NodeError> {
        for ex in &self.executors {
            if ex.supports(nt) {
                return ex.run(node, nt, ctx, cancel).await;
            }
        }
        Err(NodeError::Config(format!(
            "no in-process executor for '{}'",
            nt.id
        )))
    }
}
