//! `checkpoint` built-in: pauses a run until an external caller
//! signals via [`crate::checkpoints::CheckpointRegistry`].
//!
//! Emits `node:paused` on entry, awaits a `oneshot` from the
//! registry (or cancellation), emits `node:resumed` on exit. When
//! `config.auto_resume` is `true` the node is a no-op — useful
//! for unit tests that exercise the rest of the graph without
//! actually pausing.

use super::util::{config_bool_or, config_str_or};
use crate::checkpoints::Resume;
use crate::events::EventType;
use crate::executor::{NodeError, NodeExecutor, NodeOutputs, RunContext};
use crate::types::{Node, NodeType};
use async_trait::async_trait;
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;

#[allow(unreachable_pub)]
pub const NODE_TYPE_ID: &str = "checkpoint";
const DEFAULT_MESSAGE: &str = "Waiting for user to continue...";

/// Built-in `checkpoint` executor — see module docs.
pub struct CheckpointExecutor;

#[async_trait]
impl NodeExecutor for CheckpointExecutor {
    fn supports(&self, nt: &NodeType) -> bool {
        nt.id == NODE_TYPE_ID
    }

    async fn run(
        &self,
        node: &Node,
        _nt: &NodeType,
        ctx: &RunContext,
        cancel: CancellationToken,
    ) -> Result<NodeOutputs, NodeError> {
        let auto_resume = config_bool_or(&node.config, "auto_resume", false);
        if auto_resume {
            return Ok(NodeOutputs::new());
        }

        let message = config_str_or(&node.config, "message", DEFAULT_MESSAGE).to_string();

        let mut paused_payload: HashMap<String, serde_json::Value> = HashMap::with_capacity(1);
        paused_payload.insert("message".into(), serde_json::Value::String(message));
        let attempt = ctx.attempt.load(std::sync::atomic::Ordering::Relaxed);
        ctx.emitter.emit_node(
            EventType::NodePaused,
            node.id.clone(),
            ctx.iteration,
            attempt,
            paused_payload,
        );

        let rx = ctx.checkpoints.register(&ctx.run_id, &node.id);

        let verdict = tokio::select! {
            res = rx => res.unwrap_or(Resume::Cancel),
            () = cancel.cancelled() => Resume::Cancel,
        };

        ctx.emitter.emit_node(
            EventType::NodeResumed,
            node.id.clone(),
            ctx.iteration,
            attempt,
            HashMap::new(),
        );

        match verdict {
            Resume::Continue => Ok(NodeOutputs::new()),
            Resume::Cancel => Err(NodeError::Cancelled),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::test_support::make_ctx;
    use crate::types::{Category, ExecutionBackend, ExecutionSpec, OutputParse, Pos};
    use std::sync::Arc;
    use std::time::Duration;

    fn checkpoint_nt() -> NodeType {
        NodeType {
            id: NODE_TYPE_ID.into(),
            name: String::new(),
            category: Category::Control,
            tags: vec![],
            icon: String::new(),
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

    fn checkpoint_node(extra: serde_json::Value) -> Node {
        let config: HashMap<String, serde_json::Value> =
            serde_json::from_value(extra).unwrap_or_default();
        Node {
            id: "ck".into(),
            ty: NODE_TYPE_ID.into(),
            name: String::new(),
            config,
            pos: Pos::default(),
            timeout_ms: None,
            retry: None,
            continue_on_error: false,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn auto_resume_short_circuits_without_pausing() {
        let (ctx, _rx, _dir) = make_ctx();
        let out = CheckpointExecutor
            .run(
                &checkpoint_node(serde_json::json!({"auto_resume": true})),
                &checkpoint_nt(),
                &ctx,
                CancellationToken::new(),
            )
            .await
            .expect("auto_resume should pass through");
        assert!(out.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn external_resume_continues() {
        let (ctx, _rx, _dir) = make_ctx();
        let reg = ctx.checkpoints.clone();
        let run_id = ctx.run_id.clone();
        let node = checkpoint_node(serde_json::json!({"message": "approve"}));
        let nt = checkpoint_nt();
        let ctx = Arc::new(ctx);
        let ctx_for_task = ctx.clone();
        let node_for_task = node.clone();
        let nt_for_task = nt.clone();

        let h = tokio::spawn(async move {
            CheckpointExecutor
                .run(
                    &node_for_task,
                    &nt_for_task,
                    &ctx_for_task,
                    CancellationToken::new(),
                )
                .await
        });

        // Give the executor time to call `register` before we resume.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(reg.resume(&run_id, "ck", Resume::Continue));

        let res = h.await.expect("task join");
        assert!(res.is_ok(), "got {res:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cancellation_returns_cancelled() {
        let (ctx, _rx, _dir) = make_ctx();
        let node = checkpoint_node(serde_json::json!({}));
        let nt = checkpoint_nt();
        let ctx = Arc::new(ctx);
        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();

        let h = tokio::spawn(async move {
            CheckpointExecutor
                .run(&node, &nt, &ctx, cancel_for_task)
                .await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();

        let res = h.await.expect("task join");
        assert!(matches!(res, Err(NodeError::Cancelled)), "got {res:?}");
    }
}
