//! `llm` built-in: OpenAI-compatible chat-completions client. Two
//! shapes depending on whether `tools` is set in node config:
//! - tools empty/absent → single-turn completion with optional SSE
//!   streaming (per `stream: StreamMode`)
//! - tools non-empty → multi-turn tool-call loop, terminating when
//!   the assistant produces no tool calls or `max_turns` is reached
//!
//! Failure policy mirrors [`super::http`]: non-2xx responses resolve
//! to `Ok` with `finish_reason: "http_<code>"` and an empty `text`.
//! Only network-level failures (DNS, connection refused, timeout)
//! return [`NodeError::Http`].
//!
//! When streaming is active each assistant-content delta is emitted
//! as one `node:output` event tagged `channel: "llm"`; the full text
//! is also accumulated into the `text` output port.

use super::util::{config_f64_or, config_str, config_str_opt, config_str_or, config_u64_or};
use crate::environment::runtime::env::{EnvId, WorkflowId};
use crate::environment::runtime::resource::{ApiFlavor, Capability, ProbeSpec, ResourceRef};
use crate::events::EventType;
use crate::executor::{NodeError, NodeExecutor, NodeOutputs, RunContext};
use crate::template::{
    BoxedResourceResolver, SubstitutionContext, build_resources_resolver, default_env_allowlist,
    substitute_in_config,
};
use crate::types::{Node, NodeType, PortValue, StreamMode};
use async_trait::async_trait;
use futures::StreamExt;
use jsonpath_rust::JsonPath;
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const DEFAULT_URL: &str = "http://localhost:11434/v1";
const DEFAULT_MODEL_TEMP: f64 = 0.7;
const DEFAULT_TIMEOUT_MS: u64 = 120_000;
const DEFAULT_MAX_TURNS: u64 = 8;
#[allow(unreachable_pub)]
pub const NODE_TYPE_ID: &str = "llm";
const SSE_DONE: &str = "[DONE]";
const SSE_DATA_PREFIX: &str = "data:";
const CHANNEL_LLM: &str = "llm";
const CHANNEL_TOOL: &str = "agent_tool";

/// In-process LLM executor — see module docs for the two shapes and
/// failure / streaming semantics.
pub struct LlmExecutor;

#[async_trait]
impl NodeExecutor for LlmExecutor {
    fn supports(&self, nt: &NodeType) -> bool {
        nt.id == NODE_TYPE_ID
    }

    async fn run(
        &self,
        node: &Node,
        nt: &NodeType,
        ctx: &RunContext,
        cancel: CancellationToken,
    ) -> Result<NodeOutputs, NodeError> {
        // The merged executor opts out of the run-loop's universal config
        // substitution (NodeType.skip_config_templates=true) because the
        // tool-loop fills `tools[].body_template` with `{{args | json}}`
        // per turn — pre-substituting it would fail with Undefined. We
        // own the substitution timing here: every config field EXCEPT
        // `tools` is templated up-front via the standard substitution
        // context (vars / secrets / inputs / nodes.*.outputs / resource),
        // and the raw `tools` value is reattached untouched so the loop
        // body can still do its per-call `{{args | json}}` swap.
        let substituted_config = substitute_llm_config(node, ctx)?;
        let resolved_node = Node {
            config: substituted_config,
            ..node.clone()
        };

        let tools_cfg = resolved_node
            .config
            .get("tools")
            .cloned()
            .unwrap_or_else(|| serde_json::json!([]));
        let tools = parse_tools(&tools_cfg)?;

        if tools.is_empty() {
            run_single_turn(&resolved_node, nt, ctx, cancel).await
        } else {
            run_tool_loop(&resolved_node, nt, ctx, cancel, &tools).await
        }
    }
}

/// Apply the standard substitution context to every config field except
/// `tools`. The tools array stays raw — its `body_template` strings are
/// evaluated per tool call by [`invoke_tool`] (the `{{args | json}}`
/// expansion needs the runtime tool-call arguments, not the workflow
/// substitution context).
///
/// Mirrors the closure-construction pattern in
/// [`crate::executor::subprocess::resolve_templated_inputs`] so secrets
/// register with the emitter for redaction, env reads use the run-loop
/// allowlist, and resource refs resolve through the shared registry.
fn substitute_llm_config(
    node: &Node,
    ctx: &RunContext,
) -> Result<HashMap<String, serde_json::Value>, NodeError> {
    let secrets_resolver = crate::executor::context::make_secrets_resolver(ctx);
    let kv_resolver = |_: &str| -> Option<String> { None };
    let env_allowlist = default_env_allowlist();
    let resources_resolver: BoxedResourceResolver = if let Some(engine) = ctx.engine.upgrade() {
        Box::new(build_resources_resolver(
            engine.resource_registry(),
            ctx.workflow_id.clone(),
        ))
    } else {
        Box::new(|_, _| None)
    };

    // Detach `tools` so the substitution pass never touches it; reattach
    // afterwards so downstream code sees the same map shape it would
    // have gotten without the carve-out.
    let mut working = node.config.clone();
    let raw_tools = working.remove("tools");

    let sub_ctx = SubstitutionContext {
        vars: &ctx.variables,
        secrets: &secrets_resolver,
        upstream_outputs: &ctx.upstream_outputs,
        current_inputs: &ctx.current_inputs,
        current_config: &working,
        kv: &kv_resolver,
        env: &*ctx.env,
        env_allowlist: &env_allowlist,
        resources: &resources_resolver,
        run_id: &ctx.run_id,
        workspace: &ctx.workspace,
        started_at_iso: &ctx.started_at_iso,
        workflow_id: &ctx.workflow_id,
        workflow_name: &ctx.workflow_name,
    };

    let mut substituted =
        substitute_in_config(&working, &sub_ctx).map_err(|e| NodeError::Template(e.to_string()))?;
    if let Some(tools) = raw_tools {
        substituted.insert("tools".into(), tools);
    }
    Ok(substituted)
}

/// Return the parent-directory stem of a probe path. The result is
/// concatenated onto `http://127.0.0.1:<port>` to form the base URL
/// for `/chat/completions`. Examples (and the edge cases we care about):
///
/// - `"/v1/models"` → `"/v1"` (the normal OpenAI-compat shape)
/// - `"/api/version"` → `"/api"` (Ollama-native, but kept generic)
/// - `"/"` or `""` → `""` (nothing to derive a stem from)
/// - `"/chat/completions"` → `"/chat"` (would be unusual but consistent)
/// - `"models"` (no leading slash) → `""` (no parent segment exists)
/// - `"/v1/"` (trailing slash, empty leaf) → `"/v1"` (drop the trailing
///   slash before taking the parent)
///
/// The stem is meant to be appended verbatim to `http://host:port`, so
/// the empty string degrades gracefully to the bare host:port form.
fn path_parent_stem(path: &str) -> String {
    // Drop a trailing slash so paths like "/v1/" don't collapse to "/v1"
    // with an empty leaf segment that then yields "/v1" anyway — the
    // trim happens up-front so the rfind below sees the real leaf.
    let trimmed = path.trim_end_matches('/');
    let Some(idx) = trimmed.rfind('/') else {
        // No slash anywhere → there's no parent segment to lift.
        return String::new();
    };
    // `trimmed[..idx]` keeps everything before the last `/`. For
    // `"/v1/models"` that's `"/v1"`; for `"/models"` it's `""`.
    trimmed[..idx].to_string()
}

/// Resolve the optional `resource` config field on an `llm` node into
/// a base URL via the shared resource registry. Returns:
///
/// - `Ok(Some(base_url))` when the node has a resource ref AND the
///   registry resolved it. `base_url` is `scheme://host:port[/stem]`
///   with no trailing slash — the caller appends `/chat/completions`.
/// - `Ok(None)` when the node has no `resource` field (caller falls
///   back to the legacy literal `url` config).
///
/// Errors with [`NodeError::Config`] on a malformed ref, an unknown
/// resource id, a non-HTTP resource (Binary / Toolchain), an HTTP
/// resource without any ports declared, or an advertised-capability
/// mismatch when a capability constraint is in effect.
///
/// `tools_present` is the same `tools.is_empty()` bool the caller
/// already computed and is used to infer the required capability when
/// the ref is the bare-id form: `false` → `OpenaiChatCompletions`,
/// `true` → `OpenaiToolCalling`. The detailed form's
/// `required_capability` overrides the inference.
///
/// Phase D derives the base path by looking for an OpenAI-flavored
/// probe route on the resource whose `proves` list contains the
/// effective capability (user-stated `required_capability` if present,
/// otherwise the inferred one). The parent directory of that route's
/// `path` becomes the stem appended to `http://127.0.0.1:<port>` so
/// the final URL is `http://127.0.0.1:<port>/v1/chat/completions` for
/// the typical `routes: [{ path: "/v1/models", flavor: openai_chat, ...}]`
/// shape. Resources without a matching route fall back to the bare
/// `http://127.0.0.1:<port>` form — that's a Phase-D bridge that keeps
/// existing user configs working. Phase E replaces this with
/// `ResourceCatalog::route_for_capability` once env-scoped catalogs are
/// kept on `Engine`.
fn resolve_resource_url(
    node: &Node,
    ctx: &RunContext,
    tools_present: bool,
) -> Result<Option<String>, NodeError> {
    let Some(rref_val) = node.config.get("resource") else {
        return Ok(None);
    };
    let rref: ResourceRef = serde_json::from_value(rref_val.clone())
        .map_err(|e| NodeError::Config(format!("llm: invalid resource ref: {e}")))?;

    let engine = ctx.engine.upgrade().ok_or_else(|| {
        NodeError::Config("internal: engine handle gone before resource lookup".into())
    })?;
    let registry = engine.resource_registry();
    let snap = registry.snapshot();

    // The workflow scope is the highest precedence; pair it with the
    // local env so the chain walks `Workflow → EnvLocal(local) →
    // UserGlobal → Builtin`. Env-aware resolution lands in Phase E.
    let workflow_id = WorkflowId(ctx.workflow_id.clone());
    let (def, _scope) = snap
        .resolve(rref.id(), &EnvId::local(), Some(&workflow_id))
        .ok_or_else(|| NodeError::Config(format!("llm: unknown resource id '{}'", rref.id().0)))?;

    // Capability gating mirrors `workflows::validate_nodes`: only
    // enforce when the user explicitly stated `required_capability`.
    // Untyped resources (empty advertised list) resolve unconditionally
    // for bare ResourceRefs whose capability is inferred. The inferred
    // capability (tools_present → OpenaiToolCalling, else
    // OpenaiChatCompletions) feeds the route lookup below so the stem
    // we derive matches the capability the caller actually wants.
    if let Some(required_cap) = rref.required_capability()
        && !def.advertised_capabilities.contains(&required_cap)
    {
        return Err(NodeError::Config(format!(
            "llm: resource '{}' does not advertise {required_cap:?}",
            rref.id().0
        )));
    }
    let inferred_cap = if tools_present {
        Capability::OpenaiToolCalling
    } else {
        Capability::OpenaiChatCompletions
    };
    let effective_cap = rref.required_capability().unwrap_or(inferred_cap);

    let ProbeSpec::Http { ports, routes, .. } = &def.probe else {
        return Err(NodeError::Config(format!(
            "llm: resource '{}' is not an HTTP endpoint",
            rref.id().0
        )));
    };
    let port = ports.first().copied().ok_or_else(|| {
        NodeError::Config(format!(
            "llm: resource '{}' declares no probe ports",
            rref.id().0
        ))
    })?;

    // Find an OpenAI-flavored proving route for the capability we need
    // and lift its parent path segment. Resources without such a route
    // fall back to the bare host:port form so legacy configs that put
    // `/v1` into a non-standard place still work.
    let stem = routes
        .iter()
        .find(|r| matches!(r.flavor, ApiFlavor::OpenaiChat) && r.proves.contains(&effective_cap))
        .map(|r| path_parent_stem(&r.path))
        .unwrap_or_default();

    Ok(Some(format!("http://127.0.0.1:{port}{stem}")))
}

/// Single-turn chat completion. Honors `stream: StreamMode` (Auto +
/// Force both attempt SSE in Phase D; Phase E's dispatcher layer will
/// enforce Force against the route's advertised capabilities).
async fn run_single_turn(
    node: &Node,
    _nt: &NodeType,
    ctx: &RunContext,
    cancel: CancellationToken,
) -> Result<NodeOutputs, NodeError> {
    let resolved = resolve_resource_url(node, ctx, false)?;
    let base_url: String = resolved.unwrap_or_else(|| {
        config_str_or(&node.config, "url", DEFAULT_URL)
            .trim_end_matches('/')
            .to_string()
    });
    let model = config_str(&node.config, "model", "llm")?;
    let messages = node
        .config
        .get("messages")
        .cloned()
        .ok_or_else(|| NodeError::Config("llm: 'messages' (array) required".into()))?;
    let temperature = config_f64_or(&node.config, "temperature", DEFAULT_MODEL_TEMP);
    let max_tokens = node.config.get("max_tokens").cloned();
    let stream_mode = read_stream_mode(node)?;
    warn_if_force_stream(node, stream_mode);
    let stream = !matches!(stream_mode, StreamMode::Off);
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

/// Multi-turn tool-call loop. Each turn calls the chat-completions
/// endpoint; if the assistant returns tool calls, each is dispatched
/// to its inline HTTP endpoint and the response appended as a tool
/// message. Terminates when an assistant turn has no tool calls or
/// `max_turns` is reached.
///
/// Streaming follows `stream: StreamMode` exactly as `run_single_turn`:
/// in Phase D, Auto and Force both attempt SSE. The loop body itself
/// is non-streaming-friendly — it assembles the per-turn message
/// either way before deciding whether to continue.
#[allow(clippy::too_many_lines)]
async fn run_tool_loop(
    node: &Node,
    _nt: &NodeType,
    ctx: &RunContext,
    cancel: CancellationToken,
    tools: &[InlineTool],
) -> Result<NodeOutputs, NodeError> {
    let resolved = resolve_resource_url(node, ctx, true)?;
    let base_url: String = resolved.unwrap_or_else(|| {
        config_str_or(&node.config, "url", DEFAULT_URL)
            .trim_end_matches('/')
            .to_string()
    });
    let model = config_str(&node.config, "model", "llm")?.to_string();
    let temperature = config_f64_or(&node.config, "temperature", DEFAULT_MODEL_TEMP);
    let max_tokens = node.config.get("max_tokens").cloned();
    let max_turns = config_u64_or(&node.config, "max_turns", DEFAULT_MAX_TURNS).max(1);
    let api_key = node
        .config
        .get("api_key")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let timeout_ms = config_u64_or(&node.config, "timeout_ms", DEFAULT_TIMEOUT_MS);
    let stream_mode = read_stream_mode(node)?;
    warn_if_force_stream(node, stream_mode);
    let stream = !matches!(stream_mode, StreamMode::Off);

    let initial_messages = node
        .config
        .get("messages")
        .cloned()
        .ok_or_else(|| NodeError::Config("llm: 'messages' (array) required".into()))?;
    let openai_tools = tools_to_openai_format(tools);

    let url = format!("{base_url}/chat/completions");
    let client = super::super::http_client::shared();

    let serde_json::Value::Array(mut messages) = initial_messages else {
        return Err(NodeError::Config("llm: 'messages' must be an array".into()));
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
        body.insert("stream".into(), serde_json::Value::Bool(stream));
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
            r = req.send() => r.map_err(|e| NodeError::Http(format!("llm send: {e}")))?,
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
        } = if stream {
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
        if !stream && !assistant_content.is_empty() {
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
            // No tool calls = terminate (stop when there are no tool
            // calls regardless of finish_reason value).
            break;
        }

        for tc in tool_calls {
            if cancel.is_cancelled() {
                return Err(NodeError::Cancelled);
            }
            let tool_msg = invoke_tool(&tc, tools, client, timeout_ms, &cancel).await?;
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

/// Decode the `stream` field. Default is `StreamMode::Auto`. Phase D
/// treats Auto and Force identically (both attempt streaming); Phase
/// E's dispatcher layer adds the route-capability check that makes
/// Force errorable when the route can't stream.
fn read_stream_mode(node: &Node) -> Result<StreamMode, NodeError> {
    node.config.get("stream").cloned().map_or_else(
        || Ok(StreamMode::default()),
        |v| {
            serde_json::from_value(v)
                .map_err(|e| NodeError::Config(format!("llm: invalid stream value: {e}")))
        },
    )
}

/// Phase D treats `StreamMode::Force` identically to `Auto` (both attempt
/// SSE without a capability check). The docstring on `StreamMode::Force`
/// promises an error when streaming is unavailable — that promise is
/// kept only after Phase E's dispatcher layer adds the route-capability
/// gate. Until then, emit a warning when Force is selected so the
/// surprising no-op is visible under `RUST_LOG=warn`.
fn warn_if_force_stream(node: &Node, stream_mode: StreamMode) {
    if matches!(stream_mode, StreamMode::Force) {
        tracing::warn!(
            node_id = %node.id,
            "llm: StreamMode::Force is selected but Phase D treats it as Auto; \
             Phase E adds the route-capability check that makes Force errorable"
        );
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

// ===== tool-loop helpers (formerly in agent.rs) ============================

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
        NodeError::Config("llm: 'tools' must be an array of tool descriptors".into())
    })?;
    let mut tools = Vec::with_capacity(arr.len());
    for (i, raw) in arr.iter().enumerate() {
        let obj = raw.as_object().ok_or_else(|| {
            NodeError::Config(format!("llm.tools[{i}]: each tool must be an object"))
        })?;
        let name = obj
            .get("name")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| NodeError::Config(format!("llm.tools[{i}]: 'name' required")))?
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
                "llm.tools[{i}]: only kind='http' is supported in v1.1"
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
                NodeError::Config(format!("llm.tools[{i}]: 'url' required for http tool"))
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

    let method = reqwest::Method::from_bytes(tool.method.as_bytes())
        .map_err(|e| NodeError::Config(format!("llm.tool[{}]: invalid method: {e}", tool.name)))?;
    let mut req = client
        .request(method, &tool.url)
        .timeout(Duration::from_millis(timeout_ms))
        .body(body_text.clone())
        .header(reqwest::header::CONTENT_TYPE, "application/json");
    for (k, v) in &tool.headers {
        req = req.header(k.as_str(), v.as_str());
    }
    let resp = tokio::select! {
        r = req.send() => r.map_err(|e| NodeError::Http(format!("llm.tool[{}] send: {e}", tool.name)))?,
        () = cancel.cancelled() => return Err(NodeError::Cancelled),
    };
    let status = resp.status();
    let body_bytes = tokio::select! {
        r = resp.bytes() => r.map_err(|e| NodeError::Http(format!("llm.tool[{}] body: {e}", tool.name)))?,
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
/// `{role, content, tool_calls?}` shape so the tool loop can push
/// it onto the messages list verbatim.
struct TurnResult {
    assistant_content: String,
    finish_reason: String,
    tokens_this_turn: u64,
    message_value: serde_json::Value,
}

/// Read one non-streaming `chat/completions` response and assemble a
/// `TurnResult`. Wraps the existing blocking body-read + JSON parse.
async fn read_non_streaming_turn(
    resp: reqwest::Response,
    cancel: &CancellationToken,
) -> Result<TurnResult, NodeError> {
    let body_text = tokio::select! {
        r = resp.text() => r.map_err(|e| NodeError::Http(format!("llm body: {e}")))?,
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
/// mirrors the non-streaming shape so the tool loop is agnostic.
async fn read_streaming_turn(
    resp: reqwest::Response,
    ctx: &RunContext,
    node_id: &str,
    cancel: &CancellationToken,
) -> Result<TurnResult, NodeError> {
    let mut stream = resp.bytes_stream();
    let mut buf = String::new();
    let mut assistant_content = String::new();
    let mut finish_reason = String::new();
    let mut tokens: u64 = 0;
    // Accumulators keyed by the `tool_calls[i].index` value from the
    // delta stream. Each entry's `arguments` string concatenates
    // across chunks until the stream terminates.
    let mut tool_call_accum: BTreeMap<u64, AccumTool> = BTreeMap::new();

    loop {
        let next = tokio::select! {
            n = stream.next() => n,
            () = cancel.cancelled() => return Err(NodeError::Cancelled),
        };
        let Some(chunk) = next else { break };
        let chunk = chunk.map_err(|e| NodeError::Http(format!("llm stream: {e}")))?;
        buf.push_str(&String::from_utf8_lossy(&chunk));

        while let Some(end) = buf.find("\n\n") {
            let event = buf[..end].to_string();
            buf.drain(..end + 2);
            let done = process_tool_sse_event(
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

fn process_tool_sse_event(
    event: &str,
    ctx: &RunContext,
    node_id: &str,
    assistant_content: &mut String,
    finish_reason: &mut String,
    tokens: &mut u64,
    tool_call_accum: &mut BTreeMap<u64, AccumTool>,
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

/// Build the message value that goes into the running messages list.
/// Mirrors `OpenAI`'s chat-completion message shape so later turns
/// see a consistent representation.
fn assembled_message(
    assistant_content: &str,
    tool_call_accum: &BTreeMap<u64, AccumTool>,
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
        EventType::NodeOutput,
        Some(node_id.to_string()),
        Some(ctx.iteration),
        Some(ctx.attempt.load(std::sync::atomic::Ordering::SeqCst)),
        payload,
    );
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
            skip_config_templates: false,
        }
    }

    fn llm_node(url: &str, stream: bool) -> Node {
        let stream_val = if stream {
            serde_json::json!("auto")
        } else {
            serde_json::json!("off")
        };
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
                ("stream".into(), stream_val),
            ]),
            pos: Pos::default(),
            timeout_ms: None,
            retry: None,
            continue_on_error: false,
            target_env: None,
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

    #[tokio::test(flavor = "multi_thread")]
    async fn single_turn_substitutes_vars_in_messages_and_url() {
        // {{vars.X}} on `messages[].content`, `model`, and `url` should
        // resolve before the request hits the wire — the executor owns
        // the substitution because skip_config_templates=true bypasses
        // the run-loop's universal pass.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{
                    "message": {"role": "assistant", "content": "ack"},
                    "finish_reason": "stop"
                }]
            })))
            .mount(&server)
            .await;

        let (mut ctx, _rx, _dir) = make_ctx();
        ctx.variables.insert("base".into(), server.uri());
        ctx.variables
            .insert("greeting".into(), "hello there".into());
        ctx.variables.insert("model_id".into(), "test-model".into());

        let node = Node {
            id: "n".into(),
            ty: NODE_TYPE_ID.into(),
            name: String::new(),
            config: HashMap::from([
                ("url".into(), serde_json::json!("{{vars.base}}")),
                ("model".into(), serde_json::json!("{{vars.model_id}}")),
                (
                    "messages".into(),
                    serde_json::json!([
                        {"role": "user", "content": "{{vars.greeting}}"}
                    ]),
                ),
                ("stream".into(), serde_json::json!("off")),
            ]),
            pos: Pos::default(),
            timeout_ms: None,
            retry: None,
            continue_on_error: false,
            target_env: None,
        };

        let out = LlmExecutor
            .run(&node, &llm_nt(), &ctx, CancellationToken::new())
            .await
            .expect("substituted call succeeds");
        assert_eq!(out.get("text"), Some(&PortValue::String("ack".into())));

        let reqs = server.received_requests().await.expect("requests captured");
        assert_eq!(reqs.len(), 1, "exactly one /chat/completions hit");
        let body: serde_json::Value = reqs[0].body_json().expect("json body");
        assert_eq!(body["model"], serde_json::json!("test-model"));
        assert_eq!(
            body["messages"][0]["content"],
            serde_json::json!("hello there"),
            "vars substituted in messages.content; got {body}",
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn tool_loop_does_not_pre_substitute_body_template() {
        // The tool loop's `body_template` carries `{{args | json}}` which
        // only resolves per tool call inside `invoke_tool`; the executor's
        // up-front substitution must leave the `tools` array untouched.
        let server = MockServer::start().await;
        // Turn 1: assistant emits a tool call.
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{
                    "message": {
                        "role": "assistant",
                        "content": "",
                        "tool_calls": [{
                            "id": "call_1",
                            "type": "function",
                            "function": {
                                "name": "echo",
                                "arguments": "{\"k\":\"v\"}"
                            }
                        }]
                    },
                    "finish_reason": "tool_calls"
                }]
            })))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        // Tool endpoint: capture body so we can assert on it.
        Mock::given(method("POST"))
            .and(path("/tool"))
            .respond_with(ResponseTemplate::new(200).set_body_string("tool-ok"))
            .mount(&server)
            .await;
        // Turn 2: assistant terminates the loop.
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{
                    "message": {"role": "assistant", "content": "done"},
                    "finish_reason": "stop"
                }]
            })))
            .mount(&server)
            .await;

        let (ctx, _rx, _dir) = make_ctx();
        let tool_url = format!("{}/tool", server.uri());
        let node = Node {
            id: "n".into(),
            ty: NODE_TYPE_ID.into(),
            name: String::new(),
            config: HashMap::from([
                ("url".into(), serde_json::json!(server.uri())),
                ("model".into(), serde_json::json!("test-model")),
                (
                    "messages".into(),
                    serde_json::json!([{"role": "user", "content": "go"}]),
                ),
                ("stream".into(), serde_json::json!("off")),
                ("max_turns".into(), serde_json::json!(2)),
                (
                    "tools".into(),
                    serde_json::json!([{
                        "name": "echo",
                        "description": "echoes args",
                        "kind": "http",
                        "method": "POST",
                        "url": tool_url,
                        "body_template": "{{args | json}}",
                    }]),
                ),
            ]),
            pos: Pos::default(),
            timeout_ms: None,
            retry: None,
            continue_on_error: false,
            target_env: None,
        };

        let out = LlmExecutor
            .run(&node, &llm_nt(), &ctx, CancellationToken::new())
            .await
            .expect("tool loop completes");
        assert_eq!(out.get("text"), Some(&PortValue::String("done".into())));

        let reqs = server.received_requests().await.expect("requests captured");
        let tool_calls: Vec<&wiremock::Request> =
            reqs.iter().filter(|r| r.url.path() == "/tool").collect();
        assert_eq!(tool_calls.len(), 1, "tool endpoint hit exactly once");
        let body = String::from_utf8_lossy(&tool_calls[0].body);
        assert_eq!(
            body.as_ref(),
            "{\"k\":\"v\"}",
            "body_template kept its `{{{{args | json}}}}` placeholder until the \
             loop swapped it for the JSON-encoded tool args; got {body}",
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial]
    async fn secret_substitutes_in_api_key_and_is_redacted() {
        // `{{secrets.X}}` on `api_key` resolves through the standard
        // resolver (which registers the value for redaction on the
        // emitter) and reaches the wire as a `Bearer <value>` header.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{
                    "message": {"role": "assistant", "content": "ok"},
                    "finish_reason": "stop"
                }]
            })))
            .mount(&server)
            .await;

        // Seed an in-process secrets store with a single entry. The
        // `keyring::use_sample_store` shim keeps the real OS keyring out
        // of the test path; the `secrets-index.json` lives in a tempdir
        // that gets cleaned up at end-of-test.
        keyring::use_sample_store(&HashMap::from([("persist", "false")])).unwrap();
        let store_dir = tempfile::TempDir::new().unwrap();
        let store = Arc::new(crate::secrets::Store::with_index_path(
            store_dir.path().join("secrets-index.json"),
        ));
        store.set("api_token", "sk-secret-abc").expect("set");

        let (mut ctx, _rx, _dir) = make_ctx();
        ctx.secrets_store = Some(store);

        let node = Node {
            id: "n".into(),
            ty: NODE_TYPE_ID.into(),
            name: String::new(),
            config: HashMap::from([
                ("url".into(), serde_json::json!(server.uri())),
                ("model".into(), serde_json::json!("test-model")),
                (
                    "messages".into(),
                    serde_json::json!([{"role": "user", "content": "hi"}]),
                ),
                ("stream".into(), serde_json::json!("off")),
                ("api_key".into(), serde_json::json!("{{secrets.api_token}}")),
            ]),
            pos: Pos::default(),
            timeout_ms: None,
            retry: None,
            continue_on_error: false,
            target_env: None,
        };

        LlmExecutor
            .run(&node, &llm_nt(), &ctx, CancellationToken::new())
            .await
            .expect("substituted call succeeds");

        let reqs = server.received_requests().await.expect("requests captured");
        assert_eq!(reqs.len(), 1);
        let auth = reqs[0]
            .headers
            .get(reqwest::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert_eq!(
            auth, "Bearer sk-secret-abc",
            "api_key resolved from secrets; got {auth}",
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn invalid_stream_value_is_a_config_error() {
        let mut node = llm_node("http://example.invalid", false);
        node.config
            .insert("stream".into(), serde_json::json!("sometimes"));
        let (ctx, _rx, _dir) = make_ctx();
        let err = LlmExecutor
            .run(&node, &llm_nt(), &ctx, CancellationToken::new())
            .await
            .expect_err("unknown stream variant must reject");
        assert!(
            matches!(err, NodeError::Config(_)),
            "expected Config error, got {err:?}"
        );
    }

    // ── resolve_resource_url tests ───────────────────────────────────────────
    //
    // These cover the alt config path that names a resource by id instead of
    // a literal `url`. Resolution flows through the shared registry seeded
    // in Phase C; Phase E swaps the synthesizer for the probed catalog's
    // `route_for_capability`.

    use crate::Engine;
    use crate::checkpoints::CheckpointRegistry;
    use crate::db::open;
    use crate::emitter::Emitter;
    use crate::environment::runtime::install_workflow_resources;
    use crate::environment::runtime::resource::{
        ApiFlavor, HttpProbeMethod, HttpProbeRoute, ProbeSpec, ResourceDefinition, ResourceId,
        ResourceKind,
    };
    use crate::executor::wrap_process_env;
    use crate::recorder::RunRecorder;
    use crate::types::Workflow;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    /// Build a `RunContext` wired to a real Engine so `resolve_resource_url`
    /// can upgrade the `Weak<Engine>` and read the live registry.
    async fn ctx_with_engine(workflow_id: &str) -> (Arc<Engine>, RunContext, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
        let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());
        let pool = open(dir.path().join("ctx.db")).unwrap();
        let wf = Workflow {
            id: workflow_id.into(),
            name: String::new(),
            schema_version: 1,
            created_at: None,
            updated_at: None,
            variables: HashMap::new(),
            triggers: vec![],
            nodes: vec![],
            edges: vec![],
            resources: vec![],
            default_env: None,
        };
        let rec = Arc::new(RunRecorder::start(pool, &wf, "{}", &HashMap::new(), "test").unwrap());
        let (em, _rx) = Emitter::new(rec.clone());
        let ctx = RunContext {
            run_id: rec.run_id.clone(),
            workflow_id: workflow_id.into(),
            workflow_name: String::new(),
            started_at_iso: String::new(),
            workspace: dir.path().to_path_buf(),
            variables: HashMap::new(),
            recorder: rec,
            emitter: Arc::new(em),
            secrets_store: None,
            env: wrap_process_env(),
            current_inputs: HashMap::new(),
            upstream_outputs: HashMap::new(),
            checkpoints: Arc::new(CheckpointRegistry::new()),
            events: Arc::new(crate::events_registry::EventRegistry::new()),
            engine: Arc::downgrade(&engine),
            compose_depth: 0,
            iteration: 1,
            attempt: AtomicU32::new(1),
            auto_resume: false,
        };
        (engine, ctx, dir)
    }

    fn llm_node_with_resource(resource_cfg: serde_json::Value) -> Node {
        let mut config: HashMap<String, serde_json::Value> = HashMap::new();
        config.insert("resource".into(), resource_cfg);
        config.insert("model".into(), serde_json::json!("test-model"));
        config.insert(
            "messages".into(),
            serde_json::json!([{"role": "user", "content": "hi"}]),
        );
        Node {
            id: "n".into(),
            ty: NODE_TYPE_ID.into(),
            name: String::new(),
            config,
            pos: Pos::default(),
            timeout_ms: None,
            retry: None,
            continue_on_error: false,
            target_env: None,
        }
    }

    /// Build an HTTP resource fixture whose single `/v1/models` probe route
    /// proves every capability the caller advertises. This keeps the stem-
    /// derivation lookup in `resolve_resource_url` happy for whichever
    /// capability the test ends up inferring (chat / tool-calling / etc.).
    fn http_resource(id: &str, port: u16, caps: Vec<Capability>) -> ResourceDefinition {
        ResourceDefinition {
            id: ResourceId(id.into()),
            kind: ResourceKind::HttpEndpoint,
            advertised_capabilities: caps.clone(),
            probe: ProbeSpec::Http {
                ports: vec![port],
                routes: vec![HttpProbeRoute {
                    path: "/v1/models".into(),
                    method: HttpProbeMethod::Get,
                    flavor: ApiFlavor::OpenaiChat,
                    proves: caps,
                    models_jsonpath: None,
                    fingerprint_jsonpaths: vec![],
                }],
                timeout_ms: None,
            },
            override_lower_scope: false,
        }
    }

    /// Build an HTTP resource whose probe route has no openai-flavored
    /// proving entry — used to exercise the Phase-D fallback that yields
    /// a bare `http://host:port` URL when no matching route exists.
    fn http_resource_without_openai_route(
        id: &str,
        port: u16,
        caps: Vec<Capability>,
    ) -> ResourceDefinition {
        ResourceDefinition {
            id: ResourceId(id.into()),
            kind: ResourceKind::HttpEndpoint,
            advertised_capabilities: caps,
            probe: ProbeSpec::Http {
                ports: vec![port],
                routes: vec![],
                timeout_ms: None,
            },
            override_lower_scope: false,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resource_missing_returns_ok_none() {
        // Legacy literal-url path: no `resource` key → caller falls back.
        let (_engine, ctx, _dir) = ctx_with_engine("wf-no-res").await;
        let node = llm_node("http://example.invalid", false);
        let out = resolve_resource_url(&node, &ctx, false).expect("ok");
        assert!(out.is_none(), "no resource field → None, got {out:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resource_short_form_resolves_to_synthesized_base_url() {
        // The fixture's `/v1/models` proving route lifts its parent path
        // segment (`/v1`) onto the base URL so callers can append
        // `/chat/completions` and reach the standard OpenAI-compat shape.
        let (engine, ctx, _dir) = ctx_with_engine("wf-short").await;
        let resource = http_resource("wf-llm", 39_111, vec![Capability::OpenaiChatCompletions]);
        install_workflow_resources(
            &WorkflowId("wf-short".into()),
            &[resource],
            &engine.resource_registry(),
        )
        .expect("install");

        let node = llm_node_with_resource(serde_json::json!("wf-llm"));
        let url = resolve_resource_url(&node, &ctx, false)
            .expect("ok")
            .expect("some");
        assert_eq!(url, "http://127.0.0.1:39111/v1");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resource_without_matching_route_falls_back_to_host_port_only() {
        // No openai-flavored proving route → the Phase-D bridge degrades
        // gracefully to the bare `http://host:port` form. This preserves
        // existing user configs that put `/v1` into the resource's URL by
        // some other convention; Phase E's `route_for_capability` removes
        // the fallback entirely.
        let (engine, ctx, _dir) = ctx_with_engine("wf-fallback").await;
        let resource = http_resource_without_openai_route(
            "wf-no-route",
            39_114,
            vec![Capability::OpenaiChatCompletions],
        );
        install_workflow_resources(
            &WorkflowId("wf-fallback".into()),
            &[resource],
            &engine.resource_registry(),
        )
        .expect("install");

        let node = llm_node_with_resource(serde_json::json!("wf-no-route"));
        let url = resolve_resource_url(&node, &ctx, false)
            .expect("ok")
            .expect("some");
        assert_eq!(url, "http://127.0.0.1:39114");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resource_long_form_with_required_capability_resolves() {
        let (engine, ctx, _dir) = ctx_with_engine("wf-long").await;
        let resource = http_resource(
            "wf-tool",
            39_112,
            vec![
                Capability::OpenaiChatCompletions,
                Capability::OpenaiToolCalling,
            ],
        );
        install_workflow_resources(
            &WorkflowId("wf-long".into()),
            &[resource],
            &engine.resource_registry(),
        )
        .expect("install");

        let node = llm_node_with_resource(serde_json::json!({
            "id": "wf-tool",
            "required_capability": "openai_tool_calling",
        }));
        // tools_present=false would otherwise default to ChatCompletions, but
        // the long-form ref's explicit `required_capability` overrides it,
        // so the stem-derivation picks up the route that proves ToolCalling.
        let url = resolve_resource_url(&node, &ctx, false)
            .expect("ok")
            .expect("some");
        assert_eq!(url, "http://127.0.0.1:39112/v1");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resource_inferred_capability_does_not_gate_bare_ref() {
        // Bare ResourceRef ("wf-tools-only") never asks the loader/executor
        // to enforce a specific capability — the inference rule only feeds
        // Phase E's route selection. So a resource that advertises ONLY
        // ToolCalling still resolves for a tools_present=false (single-turn)
        // call. User-stated `required_capability` is the only thing that
        // gates Phase D.
        let (engine, ctx, _dir) = ctx_with_engine("wf-infer").await;
        let resource = http_resource("wf-tools-only", 39_113, vec![Capability::OpenaiToolCalling]);
        install_workflow_resources(
            &WorkflowId("wf-infer".into()),
            &[resource],
            &engine.resource_registry(),
        )
        .expect("install");

        let node = llm_node_with_resource(serde_json::json!("wf-tools-only"));
        // tools_present=true → inferred ToolCalling → matches the proving
        // route → stem `/v1` is appended.
        let url_with_tools = resolve_resource_url(&node, &ctx, true)
            .expect("tools_present=true bare-ref resolves")
            .expect("some");
        assert_eq!(url_with_tools, "http://127.0.0.1:39113/v1");
        // tools_present=false → inferred ChatCompletions → the resource's
        // route only proves ToolCalling, so the fallback kicks in and we
        // get the bare host:port form. Resolution still succeeds because
        // there is no user-stated capability constraint to enforce.
        let url_without_tools = resolve_resource_url(&node, &ctx, false)
            .expect("tools_present=false bare-ref also resolves (no user-stated cap)")
            .expect("some");
        assert_eq!(url_without_tools, "http://127.0.0.1:39113");

        // But the long-form ref with an explicit required_capability that
        // the resource doesn't advertise → Config error regardless of tools.
        let strict_node = llm_node_with_resource(serde_json::json!({
            "id": "wf-tools-only",
            "required_capability": "openai_chat_completions",
        }));
        let err =
            resolve_resource_url(&strict_node, &ctx, false).expect_err("user-stated cap rejects");
        assert!(matches!(err, NodeError::Config(_)), "got {err:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resource_unknown_id_is_a_config_error() {
        let (_engine, ctx, _dir) = ctx_with_engine("wf-missing").await;
        let node = llm_node_with_resource(serde_json::json!("nobody-here"));
        let err = resolve_resource_url(&node, &ctx, false).expect_err("unknown id rejects");
        assert!(matches!(err, NodeError::Config(_)), "got {err:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resource_non_http_kind_is_a_config_error() {
        // Install a Binary-kind resource that *does* advertise the capability
        // the caller asks for, so the cap check passes and we exercise the
        // "not HttpEndpoint" branch below it.
        let (engine, ctx, _dir) = ctx_with_engine("wf-binary").await;
        let resource = ResourceDefinition {
            id: ResourceId("misdeclared-bin".into()),
            kind: ResourceKind::Binary,
            advertised_capabilities: vec![Capability::OpenaiChatCompletions],
            probe: ProbeSpec::Binary {
                bin: "true".into(),
                version_args: vec!["--version".into()],
                version_regex: r"(\d+)".into(),
                extra_search_paths: vec![],
                timeout_ms: None,
            },
            override_lower_scope: false,
        };
        install_workflow_resources(
            &WorkflowId("wf-binary".into()),
            &[resource],
            &engine.resource_registry(),
        )
        .expect("install");

        let node = llm_node_with_resource(serde_json::json!("misdeclared-bin"));
        let err = resolve_resource_url(&node, &ctx, false).expect_err("non-http resource rejects");
        let msg = format!("{err:?}");
        assert!(matches!(err, NodeError::Config(_)), "got {msg}");
        assert!(
            msg.contains("not an HTTP endpoint"),
            "expected non-http hint, got {msg}",
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resource_malformed_ref_is_a_config_error() {
        let (_engine, ctx, _dir) = ctx_with_engine("wf-bad").await;
        // Numeric scalar can't deserialize into ResourceRef.
        let node = llm_node_with_resource(serde_json::json!(42));
        let err = resolve_resource_url(&node, &ctx, false).expect_err("bad shape rejects");
        assert!(matches!(err, NodeError::Config(_)), "got {err:?}");
    }
}
