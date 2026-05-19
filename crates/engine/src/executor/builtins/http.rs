//! `http` built-in: in-process HTTP via a shared `reqwest::Client`.
//!
//! Failure policy is the one the spec locks in: any HTTP response
//! (including 4xx / 5xx) returns `Ok(NodeOutputs)` with the status
//! code on the `status` output port. Only network-level failures
//! (DNS, connection refused, timeout) return [`NodeError::Http`].
//! Retry-on-status is a workflow-graph concern (downstream
//! `condition` node), not an executor concern.

use crate::executor::{NodeError, NodeExecutor, NodeOutputs, RunContext};
use crate::types::{Node, NodeType, PortValue};
use async_trait::async_trait;
use reqwest::{Method, header::HeaderMap, header::HeaderName, header::HeaderValue};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const DEFAULT_TIMEOUT_MS: u64 = 30_000;
#[allow(unreachable_pub)]
pub const NODE_TYPE_ID: &str = "http";

/// HTTP executor — see module docs for failure policy.
pub struct HttpExecutor;

#[async_trait]
impl NodeExecutor for HttpExecutor {
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
        let url = node
            .config
            .get("url")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| NodeError::Config("http: 'url' (string) required".into()))?;
        let method = node
            .config
            .get("method")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("GET");
        let method = Method::from_bytes(method.as_bytes())
            .map_err(|e| NodeError::Config(format!("http: invalid method '{method}': {e}")))?;
        let timeout_ms = node
            .config
            .get("timeout_ms")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(DEFAULT_TIMEOUT_MS);

        let mut req = super::super::http_client::shared()
            .request(method, url)
            .timeout(Duration::from_millis(timeout_ms));

        if let Some(headers_val) = node.config.get("headers") {
            req = req.headers(parse_headers(headers_val)?);
        }
        if let Some(query_val) = node.config.get("query") {
            req = req.query(query_val);
        }
        if let Some(body_val) = node.config.get("body") {
            req = match body_val {
                serde_json::Value::String(s) => req.body(s.clone()),
                other => req.json(other),
            };
        }

        let resp = tokio::select! {
            r = req.send() => r.map_err(|e| NodeError::Http(format!("send: {e}")))?,
            () = cancel.cancelled() => return Err(NodeError::Cancelled),
        };

        let status = resp.status().as_u16();
        let resp_headers = headers_to_json(resp.headers());
        let is_json = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|ct| ct.starts_with("application/json"));

        let body_bytes = tokio::select! {
            r = resp.bytes() => r.map_err(|e| NodeError::Http(format!("read body: {e}")))?,
            () = cancel.cancelled() => return Err(NodeError::Cancelled),
        };
        let body_str = String::from_utf8_lossy(&body_bytes).into_owned();
        // Fall back to String if the server lied about content-type.
        let body_port = if is_json {
            serde_json::from_str::<serde_json::Value>(&body_str)
                .ok()
                .map_or_else(move || PortValue::String(body_str), PortValue::Json)
        } else {
            PortValue::String(body_str)
        };

        let mut out = NodeOutputs::new();
        out.insert("status".into(), PortValue::Number(f64::from(status)));
        out.insert("body".into(), body_port);
        out.insert("headers".into(), PortValue::Json(resp_headers));
        Ok(out)
    }
}

fn parse_headers(val: &serde_json::Value) -> Result<HeaderMap, NodeError> {
    let map = val
        .as_object()
        .ok_or_else(|| NodeError::Config("http: 'headers' must be an object".into()))?;
    let mut hm = HeaderMap::with_capacity(map.len());
    for (k, v) in map {
        let name = HeaderName::from_bytes(k.as_bytes())
            .map_err(|e| NodeError::Config(format!("http: invalid header name '{k}': {e}")))?;
        let s = v.as_str().ok_or_else(|| {
            NodeError::Config(format!("http: header '{k}' value must be a string"))
        })?;
        let hv = HeaderValue::from_str(s)
            .map_err(|e| NodeError::Config(format!("http: invalid header value for '{k}': {e}")))?;
        hm.insert(name, hv);
    }
    Ok(hm)
}

fn headers_to_json(h: &HeaderMap) -> serde_json::Value {
    let mut map = serde_json::Map::with_capacity(h.len());
    for (name, value) in h {
        if let Ok(s) = value.to_str() {
            map.insert(
                name.as_str().to_string(),
                serde_json::Value::String(s.into()),
            );
        }
    }
    serde_json::Value::Object(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::test_support::make_ctx;
    use crate::types::{Category, ExecutionBackend, ExecutionSpec, OutputParse, Pos};
    use std::collections::HashMap;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn http_nt() -> NodeType {
        NodeType {
            id: NODE_TYPE_ID.into(),
            name: String::new(),
            category: Category::Integration,
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

    fn http_node(url: &str) -> Node {
        Node {
            id: "n".into(),
            ty: NODE_TYPE_ID.into(),
            name: String::new(),
            config: HashMap::from([("url".into(), serde_json::json!(url))]),
            pos: Pos::default(),
            timeout_ms: None,
            retry: None,
            continue_on_error: false,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_200_json_body_returns_json_port() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/data"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"k": "v", "n": 42})),
            )
            .mount(&server)
            .await;

        let (ctx, _rx, _dir) = make_ctx();
        let out = HttpExecutor
            .run(
                &http_node(&format!("{}/data", server.uri())),
                &http_nt(),
                &ctx,
                CancellationToken::new(),
            )
            .await
            .expect("200 should be Ok");

        assert_eq!(out.get("status"), Some(&PortValue::Number(200.0)));
        match out.get("body").expect("body port") {
            PortValue::Json(v) => assert_eq!(v["k"], "v"),
            other => panic!("expected Json body, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_200_text_body_returns_string_port() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/txt"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/plain")
                    .set_body_string("hello there"),
            )
            .mount(&server)
            .await;

        let (ctx, _rx, _dir) = make_ctx();
        let out = HttpExecutor
            .run(
                &http_node(&format!("{}/txt", server.uri())),
                &http_nt(),
                &ctx,
                CancellationToken::new(),
            )
            .await
            .expect("200 should be Ok");

        assert_eq!(out.get("status"), Some(&PortValue::Number(200.0)));
        assert_eq!(
            out.get("body"),
            Some(&PortValue::String("hello there".into()))
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn http_404_returns_ok_with_status_404() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/missing"))
            .respond_with(ResponseTemplate::new(404).set_body_string("nope"))
            .mount(&server)
            .await;

        let (ctx, _rx, _dir) = make_ctx();
        let out = HttpExecutor
            .run(
                &http_node(&format!("{}/missing", server.uri())),
                &http_nt(),
                &ctx,
                CancellationToken::new(),
            )
            .await
            .expect("4xx must NOT raise NodeError");

        assert_eq!(out.get("status"), Some(&PortValue::Number(404.0)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unresolvable_host_returns_http_error() {
        // `.invalid` is reserved by RFC 6761 for guaranteed-
        // non-resolvable names. DNS failure here is portable —
        // dropping a wiremock server doesn't always refuse the
        // port on WSL / containerised hosts.
        let (ctx, _rx, _dir) = make_ctx();
        let mut node = http_node("http://does-not-resolve.invalid/x");
        // Cap the wait so a slow resolver doesn't drag the test out.
        node.config
            .insert("timeout_ms".into(), serde_json::json!(2_000));

        let err = HttpExecutor
            .run(&node, &http_nt(), &ctx, CancellationToken::new())
            .await
            .expect_err("network failure must raise NodeError");

        assert!(matches!(err, NodeError::Http(_)), "got {err:?}");
    }
}
