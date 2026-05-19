//! `delay` built-in: sleep N milliseconds. Cancellable.

use crate::executor::{NodeError, NodeExecutor, NodeOutputs, RunContext};
use crate::types::{Node, NodeType};
use async_trait::async_trait;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;

/// Sleeps for `config.ms` milliseconds, returning empty outputs.
/// Cancellation aborts the sleep immediately with [`NodeError::Cancelled`].
pub struct DelayExecutor;

#[async_trait]
impl NodeExecutor for DelayExecutor {
    fn supports(&self, nt: &NodeType) -> bool {
        nt.id == "delay"
    }

    async fn run(
        &self,
        node: &Node,
        _nt: &NodeType,
        _ctx: &RunContext,
        cancel: CancellationToken,
    ) -> Result<NodeOutputs, NodeError> {
        let ms = node
            .config
            .get("ms")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| NodeError::Config("delay: 'ms' (number) required".into()))?;
        tokio::select! {
            () = sleep(Duration::from_millis(ms)) => Ok(NodeOutputs::new()),
            () = cancel.cancelled() => Err(NodeError::Cancelled),
        }
    }
}

#[cfg(test)]
mod tests;
