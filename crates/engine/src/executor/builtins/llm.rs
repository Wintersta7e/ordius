//! `llm` built-in: OpenAI-compatible chat-completions client with
//! optional Server-Sent-Events streaming.
//!
//! Failure policy mirrors [`super::http`]: non-2xx responses
//! resolve to `Ok` with `finish_reason: "http_<code>"` and an
//! empty `text`. Only network-level failures (DNS, connection
//! refused, timeout) return [`NodeError::Http`].
//!
//! When `config.stream == true` (the default), each assistant-
//! content delta in the SSE stream is emitted as one `node:output`
//! event tagged `channel: "llm"`; the full text is also
//! accumulated into the `text` output port.

use super::util::{
    config_bool_or, config_f64_or, config_str, config_str_opt, config_str_or, config_u64_or,
};
use crate::events::EventType;
use crate::executor::{NodeError, NodeExecutor, NodeOutputs, RunContext};
use crate::types::{Node, NodeType, PortValue};
use async_trait::async_trait;
use futures::StreamExt;
use std::collections::HashMap;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const DEFAULT_URL: &str = "http://localhost:11434/v1";
const DEFAULT_MODEL_TEMP: f64 = 0.7;
const DEFAULT_TIMEOUT_MS: u64 = 120_000;
#[allow(unreachable_pub)]
pub const NODE_TYPE_ID: &str = "llm";
const SSE_DONE: &str = "[DONE]";
const SSE_DATA_PREFIX: &str = "data:";
const CHANNEL_LLM: &str = "llm";

/// In-process LLM executor — see module docs for failure policy
/// and streaming semantics.
pub struct LlmExecutor;

#[async_trait]
impl NodeExecutor for LlmExecutor {
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
        let base_url = config_str_or(&node.config, "url", DEFAULT_URL).trim_end_matches('/');
        let model = config_str(&node.config, "model", "llm")?;
        let messages = node
            .config
            .get("messages")
            .cloned()
            .ok_or_else(|| NodeError::Config("llm: 'messages' (array) required".into()))?;
        let temperature = config_f64_or(&node.config, "temperature", DEFAULT_MODEL_TEMP);
        let max_tokens = node.config.get("max_tokens").cloned();
        let stream = config_bool_or(&node.config, "stream", true);
        let api_key = config_str_opt(&node.config, "api_key").map(str::to_string);
        let timeout_ms = config_u64_or(&node.config, "timeout_ms", DEFAULT_TIMEOUT_MS);

        let mut body = serde_json::Map::new();
        body.insert("model".into(), serde_json::Value::String(model.into()));
        body.insert("messages".into(), messages);
        body.insert("temperature".into(), serde_json::json!(temperature));
        if let Some(mt) = max_tokens {
            body.insert("max_tokens".into(), mt);
        }
        body.insert("stream".into(), serde_json::Value::Bool(stream));

        let url = format!("{base_url}/chat/completions");
        let mut req = super::super::http_client::shared()
            .post(&url)
            .timeout(Duration::from_millis(timeout_ms))
            .json(&serde_json::Value::Object(body));
        if let Some(key) = api_key {
            req = req.bearer_auth(key);
        }

        let resp = tokio::select! {
            r = req.send() => r.map_err(|e| NodeError::Http(format!("llm send: {e}")))?,
            () = cancel.cancelled() => return Err(NodeError::Cancelled),
        };

        let status = resp.status();
        if !status.is_success() {
            return Ok(non_success_outputs(status.as_u16()));
        }

        if stream {
            read_sse_stream(resp, ctx, &node.id, cancel).await
        } else {
            let bytes = tokio::select! {
                r = resp.bytes() => r.map_err(|e| NodeError::Http(format!("llm body: {e}")))?,
                () = cancel.cancelled() => return Err(NodeError::Cancelled),
            };
            Ok(parse_complete_response(&bytes))
        }
    }
}

fn non_success_outputs(code: u16) -> NodeOutputs {
    let mut out = NodeOutputs::new();
    out.insert("text".into(), PortValue::String(String::new()));
    out.insert("tokens_used".into(), PortValue::Number(0.0));
    out.insert(
        "finish_reason".into(),
        PortValue::String(format!("http_{code}")),
    );
    out
}

fn parse_complete_response(bytes: &[u8]) -> NodeOutputs {
    let mut out = NodeOutputs::new();
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        // Server promised JSON but didn't deliver — surface empty
        // text + parse_error finish_reason rather than raise; the
        // caller can still see the empty fields and react.
        out.insert("text".into(), PortValue::String(String::new()));
        out.insert("tokens_used".into(), PortValue::Number(0.0));
        out.insert(
            "finish_reason".into(),
            PortValue::String("parse_error".into()),
        );
        return out;
    };
    let text = v
        .pointer("/choices/0/message/content")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string();
    let tokens = v
        .pointer("/usage/total_tokens")
        .and_then(serde_json::Value::as_f64)
        .unwrap_or(0.0);
    let finish = v
        .pointer("/choices/0/finish_reason")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string();
    out.insert("text".into(), PortValue::String(text));
    out.insert("tokens_used".into(), PortValue::Number(tokens));
    out.insert("finish_reason".into(), PortValue::String(finish));
    out
}

/// Read an SSE body chunk-by-chunk, emit one `node:output` event
/// per assistant-content delta, and return the assembled text +
/// finish reason as output ports.
async fn read_sse_stream(
    resp: reqwest::Response,
    ctx: &RunContext,
    node_id: &str,
    cancel: CancellationToken,
) -> Result<NodeOutputs, NodeError> {
    let mut stream = resp.bytes_stream();
    let mut buf = String::new();
    let mut accumulated = String::new();
    let mut finish_reason = String::new();
    let mut tokens_used: f64 = 0.0;

    loop {
        let next = tokio::select! {
            n = stream.next() => n,
            () = cancel.cancelled() => return Err(NodeError::Cancelled),
        };
        let Some(chunk_res) = next else { break };
        let chunk = chunk_res.map_err(|e| NodeError::Http(format!("llm stream: {e}")))?;
        buf.push_str(&String::from_utf8_lossy(&chunk));

        while let Some(idx) = buf.find("\n\n") {
            let event = buf[..idx].to_string();
            buf.drain(..idx + 2);
            if process_sse_event(
                &event,
                ctx,
                node_id,
                &mut accumulated,
                &mut finish_reason,
                &mut tokens_used,
            ) {
                // [DONE] marker — drain remaining buffer + return.
                return Ok(finalize_stream(accumulated, tokens_used, finish_reason));
            }
        }
    }

    Ok(finalize_stream(accumulated, tokens_used, finish_reason))
}

/// Returns `true` if this event was the SSE `[DONE]` terminator.
fn process_sse_event(
    event: &str,
    ctx: &RunContext,
    node_id: &str,
    accumulated: &mut String,
    finish_reason: &mut String,
    tokens_used: &mut f64,
) -> bool {
    // Concatenate all `data: ` lines per the SSE spec.
    let mut data = String::new();
    for line in event.lines() {
        let line = line.trim_start_matches('\u{feff}'); // strip BOM if present
        let Some(rest) = line.strip_prefix(SSE_DATA_PREFIX) else {
            continue;
        };
        let rest = rest.trim_start();
        if rest == SSE_DONE {
            return true;
        }
        if !data.is_empty() {
            data.push('\n');
        }
        data.push_str(rest);
    }
    if data.is_empty() {
        return false;
    }
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&data) else {
        return false; // skip malformed lines
    };
    if let Some(content) = value
        .pointer("/choices/0/delta/content")
        .and_then(serde_json::Value::as_str)
    {
        accumulated.push_str(content);
        let mut payload: HashMap<String, serde_json::Value> = HashMap::with_capacity(2);
        payload.insert(
            "channel".into(),
            serde_json::Value::String(CHANNEL_LLM.into()),
        );
        payload.insert("text".into(), serde_json::Value::String(content.into()));
        ctx.emitter.emit_node(
            EventType::NodeOutput,
            node_id.to_string(),
            ctx.iteration,
            ctx.attempt.load(std::sync::atomic::Ordering::Relaxed),
            payload,
        );
    }
    if let Some(fr) = value
        .pointer("/choices/0/finish_reason")
        .and_then(serde_json::Value::as_str)
    {
        *finish_reason = fr.to_string();
    }
    if let Some(usage) = value
        .pointer("/usage/total_tokens")
        .and_then(serde_json::Value::as_f64)
    {
        *tokens_used = usage;
    }
    false
}

fn finalize_stream(text: String, tokens_used: f64, finish_reason: String) -> NodeOutputs {
    let mut out = NodeOutputs::new();
    out.insert("text".into(), PortValue::String(text));
    out.insert("tokens_used".into(), PortValue::Number(tokens_used));
    out.insert("finish_reason".into(), PortValue::String(finish_reason));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::test_support::make_ctx;
    use crate::types::{Category, ExecutionBackend, ExecutionSpec, OutputParse, Pos};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn llm_nt() -> NodeType {
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

    fn llm_node(url: &str, stream: bool) -> Node {
        Node {
            id: "n".into(),
            ty: NODE_TYPE_ID.into(),
            name: String::new(),
            config: HashMap::from([
                ("url".into(), serde_json::json!(url)),
                ("model".into(), serde_json::json!("test-model")),
                (
                    "messages".into(),
                    serde_json::json!([{"role": "user", "content": "hi"}]),
                ),
                ("stream".into(), serde_json::json!(stream)),
            ]),
            pos: Pos::default(),
            timeout_ms: None,
            retry: None,
            continue_on_error: false,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn streaming_emits_one_event_per_delta_and_accumulates() {
        let server = MockServer::start().await;
        let sse_body = "\
            data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n\
            data: {\"choices\":[{\"delta\":{\"content\":\" \"}}]}\n\n\
            data: {\"choices\":[{\"delta\":{\"content\":\"world\"},\"finish_reason\":\"stop\"}],\"usage\":{\"total_tokens\":3}}\n\n\
            data: [DONE]\n\n";
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(sse_body),
            )
            .mount(&server)
            .await;

        let (ctx, mut rx, _dir) = make_ctx();
        let out = LlmExecutor
            .run(
                &llm_node(&server.uri(), true),
                &llm_nt(),
                &ctx,
                CancellationToken::new(),
            )
            .await
            .expect("200 stream is Ok");

        assert_eq!(
            out.get("text"),
            Some(&PortValue::String("hello world".into()))
        );
        assert_eq!(
            out.get("finish_reason"),
            Some(&PortValue::String("stop".into()))
        );
        assert_eq!(out.get("tokens_used"), Some(&PortValue::Number(3.0)));

        let mut deltas = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            if ev.ty == EventType::NodeOutput
                && let Some(t) = ev.payload.get("text").and_then(|v| v.as_str())
            {
                deltas.push(t.to_string());
            }
        }
        assert_eq!(deltas, vec!["hello", " ", "world"], "got {deltas:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn non_stream_extracts_message_content() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{
                    "message": {"role": "assistant", "content": "complete response"},
                    "finish_reason": "stop"
                }],
                "usage": {"total_tokens": 7}
            })))
            .mount(&server)
            .await;

        let (ctx, _rx, _dir) = make_ctx();
        let out = LlmExecutor
            .run(
                &llm_node(&server.uri(), false),
                &llm_nt(),
                &ctx,
                CancellationToken::new(),
            )
            .await
            .expect("200 non-stream is Ok");

        assert_eq!(
            out.get("text"),
            Some(&PortValue::String("complete response".into()))
        );
        assert_eq!(out.get("tokens_used"), Some(&PortValue::Number(7.0)));
        assert_eq!(
            out.get("finish_reason"),
            Some(&PortValue::String("stop".into()))
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn non_2xx_returns_ok_with_finish_reason_http_code() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(503).set_body_string("upstream error"))
            .mount(&server)
            .await;

        let (ctx, _rx, _dir) = make_ctx();
        let out = LlmExecutor
            .run(
                &llm_node(&server.uri(), true),
                &llm_nt(),
                &ctx,
                CancellationToken::new(),
            )
            .await
            .expect("non-2xx must NOT raise NodeError");

        assert_eq!(out.get("text"), Some(&PortValue::String(String::new())));
        assert_eq!(
            out.get("finish_reason"),
            Some(&PortValue::String("http_503".into()))
        );
    }
}
