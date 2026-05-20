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
        let stream_mode = config_bool_or(&node.config, "stream", true);

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
            body.insert("stream".into(), serde_json::Value::Bool(stream_mode));
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

            let TurnResult {
                assistant_content,
                finish_reason,
                tokens_this_turn,
                message_value,
            } = if stream_mode {
                read_streaming_turn(resp, ctx, &node.id, &cancel).await?
            } else {
                read_non_streaming_turn(resp, &cancel).await?
            };
            if tokens_this_turn > 0 {
                total_tokens = tokens_this_turn;
            }
            final_finish_reason = finish_reason.clone();

            // For non-streaming mode the assistant text wasn't emitted
            // incrementally — emit once now so the GUI run viewer
            // shows it. Streaming has already emitted deltas inline.
            if !stream_mode && !assistant_content.is_empty() {
                emit_chunk(ctx, &node.id, CHANNEL_LLM, &assistant_content);
            }
            if !assistant_content.is_empty() {
                final_text = assistant_content.clone();
            }
            // Use the assembled message for the transcript so later
            // turns see the full role/content/tool_calls structure.
            let message = message_value;

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

/// Per-turn assembled result from either the streaming or
/// non-streaming reader. `message_value` mirrors `OpenAI`'s
/// `{role, content, tool_calls?}` shape so the agent loop can push
/// it onto the messages list verbatim.
struct TurnResult {
    assistant_content: String,
    finish_reason: String,
    tokens_this_turn: u64,
    message_value: serde_json::Value,
}

const SSE_DATA_PREFIX: &str = "data:";
const SSE_DONE: &str = "[DONE]";

/// Read one non-streaming `chat/completions` response and assemble a
/// `TurnResult`. Wraps the existing blocking body-read + JSON parse.
async fn read_non_streaming_turn(
    resp: reqwest::Response,
    cancel: &CancellationToken,
) -> Result<TurnResult, NodeError> {
    let body_text = tokio::select! {
        r = resp.text() => r.map_err(|e| NodeError::Http(format!("agent body: {e}")))?,
        () = cancel.cancelled() => return Err(NodeError::Cancelled),
    };
    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&body_text) else {
        return Ok(TurnResult {
            assistant_content: String::new(),
            finish_reason: "parse_error".into(),
            tokens_this_turn: 0,
            message_value: serde_json::Value::Null,
        });
    };
    let tokens = parsed
        .pointer("/usage/total_tokens")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let Some(choice) = parsed.pointer("/choices/0") else {
        return Ok(TurnResult {
            assistant_content: String::new(),
            finish_reason: "parse_error".into(),
            tokens_this_turn: tokens,
            message_value: serde_json::Value::Null,
        });
    };
    let message_value = choice.pointer("/message").cloned().unwrap_or_default();
    let assistant_content = message_value
        .pointer("/content")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string();
    let finish_reason = choice
        .pointer("/finish_reason")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string();
    Ok(TurnResult {
        assistant_content,
        finish_reason,
        tokens_this_turn: tokens,
        message_value,
    })
}

/// Read one streaming `chat/completions` response: parse each
/// `data: {...}` event, accumulate content + tool-call argument
/// deltas, emit content chunks as `node:output` events on the `llm`
/// channel. Returns the assembled `TurnResult` whose `message_value`
/// mirrors the non-streaming shape so the agent loop is agnostic.
async fn read_streaming_turn(
    resp: reqwest::Response,
    ctx: &RunContext,
    node_id: &str,
    cancel: &CancellationToken,
) -> Result<TurnResult, NodeError> {
    use futures::StreamExt;

    let mut stream = resp.bytes_stream();
    let mut buf = String::new();
    let mut assistant_content = String::new();
    let mut finish_reason = String::new();
    let mut tokens: u64 = 0;
    // Accumulators keyed by the `tool_calls[i].index` value from the
    // delta stream. Each entry's `arguments` string concatenates
    // across chunks until the stream terminates.
    let mut tool_call_accum: std::collections::BTreeMap<u64, AccumTool> =
        std::collections::BTreeMap::new();

    loop {
        let next = tokio::select! {
            n = stream.next() => n,
            () = cancel.cancelled() => return Err(NodeError::Cancelled),
        };
        let Some(chunk) = next else { break };
        let chunk = chunk.map_err(|e| NodeError::Http(format!("agent stream: {e}")))?;
        buf.push_str(&String::from_utf8_lossy(&chunk));

        while let Some(end) = buf.find("\n\n") {
            let event = buf[..end].to_string();
            buf.drain(..end + 2);
            let done = process_agent_sse_event(
                &event,
                ctx,
                node_id,
                &mut assistant_content,
                &mut finish_reason,
                &mut tokens,
                &mut tool_call_accum,
            );
            if done {
                let message_value = assembled_message(&assistant_content, &tool_call_accum);
                return Ok(TurnResult {
                    assistant_content,
                    finish_reason,
                    tokens_this_turn: tokens,
                    message_value,
                });
            }
        }
    }

    let message_value = assembled_message(&assistant_content, &tool_call_accum);
    Ok(TurnResult {
        assistant_content,
        finish_reason,
        tokens_this_turn: tokens,
        message_value,
    })
}

/// In-flight tool call assembled from successive `delta.tool_calls[]`
/// fragments. Only `arguments` is multi-chunk in `OpenAI`'s protocol;
/// `id` and `name` arrive on the first chunk for an index.
struct AccumTool {
    id: String,
    name: String,
    arguments: String,
}

fn process_agent_sse_event(
    event: &str,
    ctx: &RunContext,
    node_id: &str,
    assistant_content: &mut String,
    finish_reason: &mut String,
    tokens: &mut u64,
    tool_call_accum: &mut std::collections::BTreeMap<u64, AccumTool>,
) -> bool {
    let mut data = String::new();
    for line in event.lines() {
        let line = line.trim_start_matches('\u{feff}');
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
        return false;
    };
    if let Some(usage) = value
        .pointer("/usage/total_tokens")
        .and_then(serde_json::Value::as_u64)
    {
        *tokens = usage;
    }
    if let Some(content) = value
        .pointer("/choices/0/delta/content")
        .and_then(serde_json::Value::as_str)
    {
        assistant_content.push_str(content);
        emit_chunk(ctx, node_id, CHANNEL_LLM, content);
    }
    if let Some(tc_array) = value
        .pointer("/choices/0/delta/tool_calls")
        .and_then(serde_json::Value::as_array)
    {
        for tc in tc_array {
            let Some(index) = tc.get("index").and_then(serde_json::Value::as_u64) else {
                continue;
            };
            let entry = tool_call_accum.entry(index).or_insert_with(|| AccumTool {
                id: String::new(),
                name: String::new(),
                arguments: String::new(),
            });
            if let Some(id) = tc.get("id").and_then(serde_json::Value::as_str)
                && entry.id.is_empty()
            {
                entry.id = id.to_string();
            }
            if let Some(name) = tc
                .pointer("/function/name")
                .and_then(serde_json::Value::as_str)
                && entry.name.is_empty()
            {
                entry.name = name.to_string();
            }
            if let Some(args) = tc
                .pointer("/function/arguments")
                .and_then(serde_json::Value::as_str)
            {
                entry.arguments.push_str(args);
            }
        }
    }
    if let Some(fr) = value
        .pointer("/choices/0/finish_reason")
        .and_then(serde_json::Value::as_str)
    {
        *finish_reason = fr.to_string();
    }
    false
}

/// Build the message value that goes into the agent's running
/// messages list. Mirrors `OpenAI`'s chat-completion message shape so
/// later turns see a consistent representation.
fn assembled_message(
    assistant_content: &str,
    tool_call_accum: &std::collections::BTreeMap<u64, AccumTool>,
) -> serde_json::Value {
    let mut msg = serde_json::Map::new();
    msg.insert("role".into(), serde_json::json!("assistant"));
    msg.insert("content".into(), serde_json::json!(assistant_content));
    if !tool_call_accum.is_empty() {
        let tcs: Vec<serde_json::Value> = tool_call_accum
            .values()
            .map(|t| {
                serde_json::json!({
                    "id": t.id,
                    "type": "function",
                    "function": {
                        "name": t.name,
                        "arguments": t.arguments,
                    },
                })
            })
            .collect();
        msg.insert("tool_calls".into(), serde_json::Value::Array(tcs));
    }
    serde_json::Value::Object(msg)
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
