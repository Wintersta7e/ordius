//! `wait_event` built-in: park until an external caller delivers an
//! event with the configured name via `Engine::deliver_event`.

use super::util::config_str;
use crate::events::EventType;
use crate::executor::{NodeError, NodeExecutor, NodeOutputs, RunContext};
use crate::types::{Node, NodeType, PortValue};
use async_trait::async_trait;
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;

#[allow(unreachable_pub)]
pub const NODE_TYPE_ID: &str = "wait_event";

/// Waits for an external event, returning its JSON payload.
pub struct WaitEventExecutor;

#[async_trait]
impl NodeExecutor for WaitEventExecutor {
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
        let event_name = config_str(&node.config, "event", "wait_event")?.to_string();
        let rx = ctx.events.register(&ctx.run_id, &event_name);

        // Emit a paused-style event so the GUI can show the waiter.
        let mut payload: HashMap<String, serde_json::Value> = HashMap::with_capacity(2);
        payload.insert("event".into(), serde_json::json!(event_name));
        payload.insert("kind".into(), serde_json::json!("wait_event"));
        ctx.emitter.emit(
            EventType::NodePaused,
            Some(node.id.clone()),
            Some(ctx.iteration),
            Some(ctx.attempt.load(std::sync::atomic::Ordering::SeqCst)),
            payload,
        );

        let payload = tokio::select! {
            r = rx => r.map_err(|_| NodeError::Cancelled)?,
            () = cancel.cancelled() => return Err(NodeError::Cancelled),
        };

        let mut out = NodeOutputs::new();
        out.insert("payload".into(), PortValue::Json(payload));
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::test_support::make_ctx;
    use crate::types::{Category, ExecutionBackend, ExecutionSpec, OutputParse, Pos};
    use serde_json::json;

    fn nt() -> NodeType {
        NodeType {
            id: NODE_TYPE_ID.into(),
            name: "Wait Event".into(),
            category: Category::Control,
            tags: vec![],
            icon: "bell".into(),
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
            skip_config_templates: false,
        }
    }

    fn node(event: &str) -> Node {
        let mut config = HashMap::new();
        config.insert("event".into(), json!(event));
        Node {
            id: "n".into(),
            ty: NODE_TYPE_ID.into(),
            name: "wait_event".into(),
            config,
            pos: Pos { x: 0.0, y: 0.0 },
            timeout_ms: None,
            retry: None,
            continue_on_error: false,
            target_env: None,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delivers_payload_when_event_arrives() {
        let (ctx, _rx, _td) = make_ctx();
        let events = ctx.events.clone();
        let run_id = ctx.run_id.clone();
        let task = tokio::spawn({
            let n = node("approved");
            let ntype = nt();
            async move {
                WaitEventExecutor
                    .run(&n, &ntype, &ctx, CancellationToken::new())
                    .await
            }
        });
        // Give the executor a moment to register.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(events.deliver(&run_id, "approved", json!({"by": "alice"})));
        let out = task.await.unwrap().expect("executor ok");
        assert_eq!(
            out.get("payload"),
            Some(&PortValue::Json(json!({"by": "alice"}))),
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cancellation_aborts_the_wait() {
        let (ctx, _rx, _td) = make_ctx();
        let cancel = CancellationToken::new();
        let cancel2 = cancel.clone();
        let task = tokio::spawn({
            let n = node("never");
            let ntype = nt();
            async move { WaitEventExecutor.run(&n, &ntype, &ctx, cancel2).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        cancel.cancel();
        let err = task.await.unwrap().unwrap_err();
        assert!(matches!(err, NodeError::Cancelled));
    }
}
