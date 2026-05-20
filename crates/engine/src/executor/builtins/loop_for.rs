//! `loop_for` built-in: bounded counter that drives a loop edge.
//!
//! Emits `branch = "loop"` while the current iteration is below
//! `config.count`, then `branch = "exit"`. Paired with a workflow
//! loop edge from this node back to the loop body's entry, with the
//! edge's `max_iterations` matching `count`. The current iteration
//! is read from [`RunContext::iteration`] (set by the run loop's
//! loop tracker), so this executor is itself stateless.

use super::util::config_u64_or;
use crate::executor::{NodeError, NodeExecutor, NodeOutputs, RunContext};
use crate::types::{Node, NodeType, PortValue};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

#[allow(unreachable_pub)]
pub const NODE_TYPE_ID: &str = "loop_for";

const LOOP_BRANCH: &str = "loop";
const EXIT_BRANCH: &str = "exit";

/// Bounded loop counter — see module docs.
pub struct LoopForExecutor;

#[async_trait]
impl NodeExecutor for LoopForExecutor {
    fn supports(&self, nt: &NodeType) -> bool {
        nt.id == NODE_TYPE_ID
    }

    async fn run(
        &self,
        node: &Node,
        _nt: &NodeType,
        ctx: &RunContext,
        _cancel: CancellationToken,
    ) -> Result<NodeOutputs, NodeError> {
        let count = config_u64_or(&node.config, "count", 1);
        // `ctx.iteration` is 1-based: the first execution is iteration 1.
        let iter = u64::from(ctx.iteration);
        let branch = if iter < count {
            LOOP_BRANCH
        } else {
            EXIT_BRANCH
        };
        let mut out = NodeOutputs::new();
        out.insert("branch".into(), PortValue::String(branch.into()));
        // u64 → f64 narrows above 2^53; iteration counters never get
        // anywhere near that range so the precision loss is academic.
        #[allow(clippy::cast_precision_loss)]
        let iter_f = iter as f64;
        out.insert("iteration".into(), PortValue::Number(iter_f));
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::test_support::make_ctx;
    use crate::types::{Category, ExecutionBackend, ExecutionSpec, OutputParse, Pos};
    use serde_json::json;
    use std::collections::HashMap;

    fn nt() -> NodeType {
        NodeType {
            id: NODE_TYPE_ID.into(),
            name: "Loop For".into(),
            category: Category::Control,
            tags: vec![],
            icon: "repeat".into(),
            description: String::new(),
            inputs: vec![],
            outputs: vec![],
            config: vec![],
            execution: ExecutionSpec {
                backend: ExecutionBackend::InProcess,
                command: vec![],
                stdin_template: None,
                env: HashMap::new(),
                timeout_ms: None,
                output_parse: OutputParse::Text,
                output_map: HashMap::new(),
            },
        }
    }

    fn node(count: u64) -> Node {
        let mut config = HashMap::new();
        config.insert("count".into(), json!(count));
        Node {
            id: "n".into(),
            ty: NODE_TYPE_ID.into(),
            name: "loop_for".into(),
            config,
            pos: Pos { x: 0.0, y: 0.0 },
            timeout_ms: None,
            retry: None,
            continue_on_error: false,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn first_iteration_emits_loop_when_count_above_one() {
        let (ctx, _rx, _td) = make_ctx();
        // make_ctx seeds iteration = 1.
        let out = LoopForExecutor
            .run(&node(3), &nt(), &ctx, CancellationToken::new())
            .await
            .expect("loop_for");
        assert_eq!(out.get("branch"), Some(&PortValue::String("loop".into())));
        assert_eq!(out.get("iteration"), Some(&PortValue::Number(1.0)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn count_of_one_exits_immediately() {
        let (ctx, _rx, _td) = make_ctx();
        let out = LoopForExecutor
            .run(&node(1), &nt(), &ctx, CancellationToken::new())
            .await
            .expect("loop_for");
        assert_eq!(out.get("branch"), Some(&PortValue::String("exit".into())));
    }
}
