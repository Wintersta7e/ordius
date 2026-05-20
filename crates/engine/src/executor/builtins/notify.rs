//! `notify` built-in: POST a `{title, message}` JSON body to a
//! configured webhook URL. Slack/Discord/Mattermost compatible when
//! the URL points at the right ingest. Failure policy mirrors `http`:
//! any response (incl. 4xx/5xx) returns `Ok` with the status on the
//! `status` port; only network-level failures raise `NodeError::Http`.

use super::util::{config_str, config_str_or, config_u64_or};
use crate::executor::{NodeError, NodeExecutor, NodeOutputs, RunContext};
use crate::types::{Node, NodeType, PortValue};
use async_trait::async_trait;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const DEFAULT_TIMEOUT_MS: u64 = 10_000;
#[allow(unreachable_pub)]
pub const NODE_TYPE_ID: &str = "notify";

/// Notify executor — POST `{title, message}` to a webhook.
pub struct NotifyExecutor;

#[async_trait]
impl NodeExecutor for NotifyExecutor {
    fn supports(&self, nt: &NodeType) -> bool {
        nt.id == NODE_TYPE_ID
    }

    async fn run(
        &self,
        node: &Node,
        _nt: &NodeType,
        _ctx: &RunContext,
        cancel: CancellationToken,
    ) -> Result<NodeOutputs, NodeError> {
        let url = config_str(&node.config, "url", "notify")?;
        let message = config_str(&node.config, "message", "notify")?;
        let title = config_str_or(&node.config, "title", "");
        let timeout_ms = config_u64_or(&node.config, "timeout_ms", DEFAULT_TIMEOUT_MS);

        let body = serde_json::json!({
            "title": title,
            "message": message,
        });

        let req = super::super::http_client::shared()
            .post(url)
            .timeout(Duration::from_millis(timeout_ms))
            .json(&body);

        let resp = tokio::select! {
            r = req.send() => r.map_err(|e| NodeError::Http(format!("notify: send: {e}")))?,
            () = cancel.cancelled() => return Err(NodeError::Cancelled),
        };

        let status = resp.status().as_u16();
        let mut out = NodeOutputs::new();
        out.insert("status".into(), PortValue::Number(f64::from(status)));
        out.insert("ok".into(), PortValue::Boolean(status < 400));
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
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn notify_nt() -> NodeType {
        NodeType {
            id: NODE_TYPE_ID.into(),
            name: "Notify".into(),
            category: Category::Integration,
            tags: vec![],
            icon: "bell".into(),
            description: "notify".into(),
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

    fn notify_node(config: serde_json::Value) -> Node {
        let mut map = HashMap::new();
        if let serde_json::Value::Object(o) = config {
            for (k, v) in o {
                map.insert(k, v);
            }
        }
        Node {
            id: "n".into(),
            ty: NODE_TYPE_ID.into(),
            name: "notify".into(),
            config: map,
            pos: Pos { x: 0.0, y: 0.0 },
            timeout_ms: None,
            retry: None,
            continue_on_error: false,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn posts_json_body_and_reports_ok_status() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/hook"))
            .and(body_json(json!({"title": "alert", "message": "ship it"})))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;
        let (ctx, _rx, _td) = make_ctx();
        let url = format!("{}/hook", server.uri());
        let out = NotifyExecutor
            .run(
                &notify_node(json!({
                    "url": url,
                    "title": "alert",
                    "message": "ship it",
                })),
                &notify_nt(),
                &ctx,
                CancellationToken::new(),
            )
            .await
            .expect("notify");
        assert_eq!(out.get("status"), Some(&PortValue::Number(204.0)));
        assert_eq!(out.get("ok"), Some(&PortValue::Boolean(true)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn non_2xx_status_is_not_an_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/hook"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let (ctx, _rx, _td) = make_ctx();
        let url = format!("{}/hook", server.uri());
        let out = NotifyExecutor
            .run(
                &notify_node(json!({
                    "url": url,
                    "message": "boom",
                })),
                &notify_nt(),
                &ctx,
                CancellationToken::new(),
            )
            .await
            .expect("notify");
        assert_eq!(out.get("status"), Some(&PortValue::Number(503.0)));
        assert_eq!(out.get("ok"), Some(&PortValue::Boolean(false)));
    }
}
