//! `agent` built-in: runs an OpenAI-compatible chat-completions loop
//! with inline tool definitions. Each tool call is dispatched to an
//! HTTP endpoint declared inline on the node; the JSON response (or
//! a JSONPath-selected sub-value) is appended as a tool message and
//! the loop continues. Terminates when the assistant turn has no
//! tool calls, or when `max_turns` is reached.
//!
//! v1.1 ships non-streaming (whole-response per turn). Streaming
//! tool-call assembly across SSE deltas is deferred to v1.2.

use super::util::{config_bool_or, config_f64_or, config_str, config_str_or, config_u64_or};
use crate::executor::{NodeError, NodeExecutor, NodeOutputs, RunContext};
use crate::types::{Node, NodeType, PortValue};
use async_trait::async_trait;
use jsonpath_rust::JsonPath;
use std::collections::HashMap;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const DEFAULT_URL: &str = "https://api.openai.com/v1";
const DEFAULT_MODEL_TEMP: f64 = 0.7;
const DEFAULT_TIMEOUT_MS: u64 = 60_000;
const DEFAULT_MAX_TURNS: u64 = 8;
const CHANNEL_LLM: &str = "llm";
const CHANNEL_TOOL: &str = "agent_tool";

#[allow(unreachable_pub)]
pub const NODE_TYPE_ID: &str = "agent";

/// Agent executor — see module docs.
pub struct AgentExecutor;

#[async_trait]
impl NodeExecutor for AgentExecutor {
    fn supports(&self, nt: &NodeType) -> bool {
        nt.id == NODE_TYPE_ID
    }

    #[allow(clippy::too_many_lines)]
    async fn run(
        &self,
        node: &Node,
        _nt: &NodeType,
        ctx: &RunContext,
        cancel: CancellationToken,
    ) -> Result<NodeOutputs, NodeError> {
        let base_url = config_str_or(&node.config, "url", DEFAULT_URL).trim_end_matches('/');
        let model = config_str(&node.config, "model", "agent")?.to_string();
        let temperature = config_f64_or(&node.config, "temperature", DEFAULT_MODEL_TEMP);
        let max_tokens = node.config.get("max_tokens").cloned();
        let max_turns = config_u64_or(&node.config, "max_turns", DEFAULT_MAX_TURNS).max(1);
        let api_key = node
            .config
            .get("api_key")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        let timeout_ms = config_u64_or(&node.config, "timeout_ms", DEFAULT_TIMEOUT_MS);
        // Reserved for streaming work in v1.2 — accept the flag now so
        // existing workflows don't break when streaming lands.
        let _stream_unused = config_bool_or(&node.config, "stream", false);

        let initial_messages = node
            .config
            .get("messages")
            .cloned()
            .ok_or_else(|| NodeError::Config("agent: 'messages' (array) required".into()))?;
        let tools_cfg = node
            .config
            .get("tools")
            .cloned()
            .unwrap_or_else(|| serde_json::json!([]));
        let tools = parse_tools(&tools_cfg)?;
        let openai_tools = tools_to_openai_format(&tools);

        let url = format!("{base_url}/chat/completions");
        let client = super::super::http_client::shared();

        let serde_json::Value::Array(mut messages) = initial_messages else {
            return Err(NodeError::Config(
                "agent: 'messages' must be an array".into(),
            ));
        };

        let mut total_tokens: u64 = 0;
        let mut tool_invocations: Vec<serde_json::Value> = Vec::new();
        let mut final_text = String::new();
        let mut final_finish_reason = String::from("max_turns");

        for turn in 0..max_turns {
            if cancel.is_cancelled() {
                return Err(NodeError::Cancelled);
            }

            let mut body = serde_json::Map::new();
            body.insert("model".into(), serde_json::Value::String(model.clone()));
            body.insert(
                "messages".into(),
                serde_json::Value::Array(messages.clone()),
            );
            body.insert("temperature".into(), serde_json::json!(temperature));
            if let Some(mt) = max_tokens.clone() {
                body.insert("max_tokens".into(), mt);
            }
            body.insert("stream".into(), serde_json::Value::Bool(false));
            if !openai_tools.is_empty() {
                body.insert(
                    "tools".into(),
                    serde_json::Value::Array(openai_tools.clone()),
                );
            }

            let mut req = client
                .post(&url)
                .timeout(Duration::from_millis(timeout_ms))
                .json(&serde_json::Value::Object(body));
            if let Some(key) = &api_key {
                req = req.bearer_auth(key);
            }

            let resp = tokio::select! {
                r = req.send() => r.map_err(|e| NodeError::Http(format!("agent send: {e}")))?,
                () = cancel.cancelled() => return Err(NodeError::Cancelled),
            };
            let status = resp.status();
            if !status.is_success() {
                final_finish_reason = format!("http_{}", status.as_u16());
                break;
            }
            let body_text = tokio::select! {
                r = resp.text() => r.map_err(|e| NodeError::Http(format!("agent body: {e}")))?,
                () = cancel.cancelled() => return Err(NodeError::Cancelled),
            };
            let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&body_text) else {
                final_finish_reason = "parse_error".into();
                break;
            };

            if let Some(usage_tokens) = parsed
                .pointer("/usage/total_tokens")
                .and_then(serde_json::Value::as_u64)
            {
                total_tokens = usage_tokens;
            }

            let Some(choice) = parsed.pointer("/choices/0") else {
                final_finish_reason = "parse_error".into();
                break;
            };
            let message = choice.pointer("/message").cloned().unwrap_or_default();
            let assistant_content = message
                .pointer("/content")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("")
                .to_string();
            let finish_reason = choice
                .pointer("/finish_reason")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("")
                .to_string();
            final_finish_reason = finish_reason.clone();

            // Emit per-turn assistant text on the llm channel.
            if !assistant_content.is_empty() {
                emit_chunk(ctx, &node.id, CHANNEL_LLM, &assistant_content);
                final_text = assistant_content.clone();
            }

            messages.push(message.clone());

            let tool_calls = message
                .pointer("/tool_calls")
                .and_then(serde_json::Value::as_array)
                .cloned()
                .unwrap_or_default();
            if tool_calls.is_empty() {
                // No tool calls = terminate (Codex: stop when there are
                // no tool calls regardless of finish_reason value).
                break;
            }

            for tc in tool_calls {
                if cancel.is_cancelled() {
                    return Err(NodeError::Cancelled);
                }
                let tool_msg = invoke_tool(&tc, &tools, client, timeout_ms, &cancel).await?;
                tool_invocations.push(tool_msg.clone());
                let tool_text = tool_msg
                    .get("result_text")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let tool_call_id = tc
                    .get("id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .to_string();
                emit_chunk(ctx, &node.id, CHANNEL_TOOL, &tool_text);
                messages.push(serde_json::json!({
                    "role": "tool",
                    "tool_call_id": tool_call_id,
                    "content": tool_text,
                }));
            }

            if turn + 1 == max_turns {
                final_finish_reason = "max_turns".into();
            }
        }

        let mut out = NodeOutputs::new();
        out.insert("text".into(), PortValue::String(final_text));
        out.insert(
            "transcript".into(),
            PortValue::Json(serde_json::Value::Array(messages)),
        );
        out.insert(
            "finish_reason".into(),
            PortValue::String(final_finish_reason),
        );
        // tokens_used is informational; precision loss above 2^53 is academic.
        #[allow(clippy::cast_precision_loss)]
        let tokens_f = total_tokens as f64;
        out.insert("tokens_used".into(), PortValue::Number(tokens_f));
        out.insert(
            "tool_calls".into(),
            PortValue::Json(serde_json::Value::Array(tool_invocations)),
        );
        Ok(out)
    }
}

/// Internal inline tool descriptor.
struct InlineTool {
    name: String,
    description: String,
    parameters_schema: serde_json::Value,
    method: String,
    url: String,
    headers: HashMap<String, String>,
    body_template: Option<String>,
    result_path: Option<String>,
}

fn parse_tools(value: &serde_json::Value) -> Result<Vec<InlineTool>, NodeError> {
    let arr = value.as_array().ok_or_else(|| {
        NodeError::Config("agent: 'tools' must be an array of tool descriptors".into())
    })?;
    let mut tools = Vec::with_capacity(arr.len());
    for (i, raw) in arr.iter().enumerate() {
        let obj = raw.as_object().ok_or_else(|| {
            NodeError::Config(format!("agent.tools[{i}]: each tool must be an object"))
        })?;
        let name = obj
            .get("name")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| NodeError::Config(format!("agent.tools[{i}]: 'name' required")))?
            .to_string();
        let description = obj
            .get("description")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_string();
        let parameters_schema = obj
            .get("parameters_schema")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({"type": "object", "properties": {}}));
        let kind = obj
            .get("kind")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("http")
            .to_string();
        if kind != "http" {
            return Err(NodeError::Config(format!(
                "agent.tools[{i}]: only kind='http' is supported in v1.1"
            )));
        }
        let method = obj
            .get("method")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("POST")
            .to_string();
        let url = obj
            .get("url")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                NodeError::Config(format!("agent.tools[{i}]: 'url' required for http tool"))
            })?
            .to_string();
        let headers: HashMap<String, String> = obj
            .get("headers")
            .and_then(serde_json::Value::as_object)
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        let body_template = obj
            .get("body_template")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        let result_path = obj
            .get("result_path")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        drop(kind); // validated above; only stored discriminator we care about is `http`.
        tools.push(InlineTool {
            name,
            description,
            parameters_schema,
            method,
            url,
            headers,
            body_template,
            result_path,
        });
    }
    Ok(tools)
}

fn tools_to_openai_format(tools: &[InlineTool]) -> Vec<serde_json::Value> {
    tools
        .iter()
        .map(|t| {
            serde_json::json!({
                "type": "function",
                "function": {
                    "name": t.name,
                    "description": t.description,
                    "parameters": t.parameters_schema,
                },
            })
        })
        .collect()
}

async fn invoke_tool(
    tool_call: &serde_json::Value,
    tools: &[InlineTool],
    client: &reqwest::Client,
    timeout_ms: u64,
    cancel: &CancellationToken,
) -> Result<serde_json::Value, NodeError> {
    let name = tool_call
        .pointer("/function/name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string();
    let raw_args = tool_call
        .pointer("/function/arguments")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("{}");
    let args_json: serde_json::Value =
        serde_json::from_str(raw_args).unwrap_or_else(|_| serde_json::json!({}));

    let Some(tool) = tools.iter().find(|t| t.name == name) else {
        return Ok(serde_json::json!({
            "tool": name,
            "args": args_json,
            "error": "unknown tool",
            "result_text": format!("error: unknown tool '{name}'"),
        }));
    };

    // Body template: `{{args | json}}` is replaced verbatim with the
    // serialised arguments JSON. Anything else passes through.
    let serialized_args = serde_json::to_string(&args_json).unwrap_or_else(|_| "{}".into());
    let body_text = tool.body_template.as_deref().map_or_else(
        || serialized_args.clone(),
        |tmpl| tmpl.replace("{{args | json}}", &serialized_args),
    );

    let method = reqwest::Method::from_bytes(tool.method.as_bytes()).map_err(|e| {
        NodeError::Config(format!("agent.tool[{}]: invalid method: {e}", tool.name))
    })?;
    let mut req = client
        .request(method, &tool.url)
        .timeout(Duration::from_millis(timeout_ms))
        .body(body_text.clone())
        .header(reqwest::header::CONTENT_TYPE, "application/json");
    for (k, v) in &tool.headers {
        req = req.header(k.as_str(), v.as_str());
    }
    let resp = tokio::select! {
        r = req.send() => r.map_err(|e| NodeError::Http(format!("agent.tool[{}] send: {e}", tool.name)))?,
        () = cancel.cancelled() => return Err(NodeError::Cancelled),
    };
    let status = resp.status();
    let body_bytes = tokio::select! {
        r = resp.bytes() => r.map_err(|e| NodeError::Http(format!("agent.tool[{}] body: {e}", tool.name)))?,
        () = cancel.cancelled() => return Err(NodeError::Cancelled),
    };
    let body_str = String::from_utf8_lossy(&body_bytes).into_owned();

    let result_text = select_result_text(tool.result_path.as_deref(), &body_str);

    Ok(serde_json::json!({
        "tool": tool.name,
        "args": args_json,
        "status": status.as_u16(),
        "result_text": result_text,
    }))
}

/// Pull the `result_path` selection out of `body_str` if the path is set
/// and the body parses as JSON; otherwise return the body verbatim.
fn select_result_text(result_path: Option<&str>, body_str: &str) -> String {
    let Some(path) = result_path else {
        return body_str.to_string();
    };
    let Ok(json_body) = serde_json::from_str::<serde_json::Value>(body_str) else {
        return body_str.to_string();
    };
    let Ok(matched) = json_body.query(path) else {
        return body_str.to_string();
    };
    if matched.is_empty() {
        return body_str.to_string();
    }
    serde_json::to_string(matched[0]).unwrap_or_else(|_| body_str.to_string())
}

fn emit_chunk(ctx: &RunContext, node_id: &str, channel: &str, text: &str) {
    let mut payload: HashMap<String, serde_json::Value> = HashMap::with_capacity(2);
    payload.insert("channel".into(), serde_json::json!(channel));
    payload.insert("text".into(), serde_json::json!(text));
    ctx.emitter.emit(
        crate::events::EventType::NodeOutput,
        Some(node_id.to_string()),
        Some(ctx.iteration),
        Some(ctx.attempt.load(std::sync::atomic::Ordering::SeqCst)),
        payload,
    );
}
