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
use crate::environment::runtime::ResourceProbeOutcome;
use crate::environment::runtime::catalog::{
    ResourceDetail, RouteOrigin, route_for_capability_from_detail,
};
use crate::environment::runtime::dispatcher::HttpTransport;
use crate::environment::runtime::env::EnvId;
use crate::environment::runtime::resource::{Capability, ResourceRef};
use crate::environment::runtime::transport::{HttpMethod, HttpRequest, HttpResponse};
use crate::events::EventType;
use crate::executor::{NodeError, NodeExecutor, NodeOutputs, RunContext};
use crate::template::{
    BoxedResourceResolver, SubstitutionContext, build_resources_resolver, default_env_allowlist,
    substitute_in_config,
};
use crate::types::{Node, NodeType, PortValue, StreamMode};
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use jsonpath_rust::JsonPath;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
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

/// Capabilities the `llm` executor can build a request for. Restricting
/// the effective capability to this set prevents a misdeclared
/// `required_capability: OpenaiEmbeddings` from dispatching a chat-style
/// body to `/embeddings`.
const LLM_VALID_CAPS: &[Capability] = &[
    Capability::OpenaiChatCompletions,
    Capability::OpenaiToolCalling,
    Capability::OpenaiStreamingChat,
    Capability::OllamaNative,
    Capability::LmStudioNative,
];

/// Path suffixes the legacy literal-URL fallback recognises as already
/// fully-qualified, so it doesn't append `/chat/completions` a second
/// time.
const KNOWN_LLM_DISPATCH_PATHS: &[&str] = &[
    "/chat/completions",
    "/embeddings",
    "/api/chat",
    "/api/generate",
];

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

/// Resolver return shape for the `llm` executor.
///
/// Fields:
/// - `url`: dispatch URL (e.g. `http://127.0.0.1:11434/v1/chat/completions`),
///   derived from the proven probe route's `base_url` + path-parent stem
///   + capability-specific suffix.
/// - `detail`: resolved `ResourceDetail::HttpEndpoint` — carries
///   `auth_secret_ref`, `streaming_supported_natively`, and `route_origin`
///   for downstream consumers.
/// - `transport`: per-env `HttpTransport` the executor sends through, so
///   `target_env` actually routes to that env.
/// - `effective_env`: env id used for the lookup (kept for future
///   `Origin::Host` gating).
struct LlmRoute {
    url: url::Url,
    detail: ResourceDetail,
    transport: Arc<dyn HttpTransport>,
    #[allow(dead_code)]
    effective_env: EnvId,
}

/// Resolve the optional `resource` config field on an `llm` node into
/// a dispatch-ready route. Returns `Ok(None)` when the node has no
/// `resource` field (caller falls back to the legacy literal `url`).
///
/// `cancel` is the executor's `CancellationToken`; the resolver creates
/// a child token for the opportunistic re-probe so cancelling the run
/// also cancels the re-probe.
async fn resolve_resource_url(
    node: &Node,
    ctx: &RunContext,
    cancel: &CancellationToken,
    tools_present: bool,
) -> Result<Option<LlmRoute>, NodeError> {
    let Some(rref_val) = node.config.get("resource") else {
        return Ok(None);
    };
    let rref: ResourceRef = serde_json::from_value(rref_val.clone())
        .map_err(|e| NodeError::Config(format!("llm: invalid resource ref: {e}")))?;

    let effective_env = node
        .target_env
        .clone()
        .unwrap_or_else(|| ctx.run_snapshot.default_env.clone());

    let catalog = ctx.run_snapshot.catalog(&effective_env).ok_or_else(|| {
        NodeError::Config(format!(
            "llm: env '{}' not in run snapshot scope",
            effective_env.as_str(),
        ))
    })?;
    let dispatcher = ctx.run_snapshot.dispatcher(&effective_env).ok_or_else(|| {
        NodeError::Config(format!(
            "llm: env '{}' has no dispatcher",
            effective_env.as_str(),
        ))
    })?;
    let transport = dispatcher.http_transport();

    let inferred_cap = if tools_present {
        Capability::OpenaiToolCalling
    } else {
        Capability::OpenaiChatCompletions
    };
    let effective_cap = rref.required_capability().unwrap_or(inferred_cap);

    if !LLM_VALID_CAPS.contains(&effective_cap) {
        return Err(NodeError::Config(format!(
            "llm: capability {effective_cap:?} cannot be used on an llm node \
             (supported: chat-completions, tool-calling, streaming-chat, \
             ollama-native, lm-studio-native). Use an http node for \
             non-chat capabilities like embeddings.",
        )));
    }

    // Lookup; on NotFound / Skipped / missing entry, fall through to an
    // opportunistic singleflight re-probe via the snapshot registry +
    // dispatcher.
    let outcome = if let Some(o @ ResourceProbeOutcome::Found(_)) = catalog.lookup(rref.id()) {
        o
    } else {
        let workflow_id = ctx.run_snapshot.workflow_id.clone();
        let (def, _scope) = ctx
            .run_snapshot
            .registry
            .resolve(rref.id(), &effective_env, Some(&workflow_id))
            .ok_or_else(|| {
                NodeError::Config(format!(
                    "llm: resource '{}' not in registry (env '{}')",
                    rref.id().0,
                    effective_env.as_str(),
                ))
            })?;
        let def = def.clone();
        Arc::clone(catalog)
            .opportunistic_reprobe(&def, Arc::clone(dispatcher), cancel.child_token())
            .await
    };

    let detail = match outcome {
        ResourceProbeOutcome::Found(d) => d,
        non_found => {
            return Err(NodeError::Config(format!(
                "llm: resource '{}' not reachable in env '{}': {non_found:?}",
                rref.id().0,
                effective_env.as_str(),
            )));
        },
    };
    if !matches!(&detail, ResourceDetail::HttpEndpoint { .. }) {
        return Err(NodeError::Config(format!(
            "llm: resource '{}' is not an HTTP endpoint",
            rref.id().0,
        )));
    }

    // The proven probe route's base + path-parent stem + capability
    // suffix together yield the dispatch URL (e.g. `/v1/models` →
    // stem `/v1` + suffix `/chat/completions`).
    let probe_route =
        route_for_capability_from_detail(&detail, effective_cap).ok_or_else(|| {
            NodeError::Config(format!(
                "llm: resource '{}' does not prove {effective_cap:?}",
                rref.id().0,
            ))
        })?;
    let stem = super::util::path_parent_stem(&probe_route.path);
    let suffix = super::util::dispatch_suffix_for_capability(effective_cap).ok_or_else(|| {
        NodeError::Config(format!(
            "llm: no dispatch suffix mapped for {effective_cap:?}",
        ))
    })?;
    let url_str = format!("{}{}{}", probe_route.base_url, stem, suffix);
    let url = url::Url::parse(&url_str)
        .map_err(|e| NodeError::Config(format!("llm: malformed dispatch URL '{url_str}': {e}")))?;

    Ok(Some(LlmRoute {
        url,
        detail,
        transport,
        effective_env,
    }))
}

/// Dispatch route the executor sends through. Combines the URL string,
/// per-env transport, and (when resolved from a `resource` ref) the
/// detail carrying auth-secret + route-origin metadata.
struct DispatchRoute {
    url: String,
    transport: Arc<dyn HttpTransport>,
    detail: Option<ResourceDetail>,
}

/// Convert a resolver result into a `DispatchRoute`, falling back to
/// the legacy literal-`url` + per-env transport when the resolver
/// returned `None`.
fn route_or_fallback(
    resolved: Option<LlmRoute>,
    node: &Node,
    ctx: &RunContext,
) -> Result<DispatchRoute, NodeError> {
    if let Some(r) = resolved {
        return Ok(DispatchRoute {
            url: r.url.to_string(),
            transport: r.transport,
            detail: Some(r.detail),
        });
    }
    let raw = config_str_or(&node.config, "url", DEFAULT_URL).to_string();
    let url = compose_legacy_chat_url(&raw);
    let transport = legacy_fallback_transport(node, ctx)?;
    Ok(DispatchRoute {
        url,
        transport,
        detail: None,
    })
}

/// Pick an `HttpTransport` for the legacy literal-URL fallback path.
/// `target_env` still routes — workflows that haven't migrated to
/// resource refs but set `target_env: wsl:Ubuntu` still go through the
/// WSL transport. Errors when the env is out of run-snapshot scope.
fn legacy_fallback_transport(
    node: &Node,
    ctx: &RunContext,
) -> Result<Arc<dyn HttpTransport>, NodeError> {
    let effective_env = node
        .target_env
        .clone()
        .unwrap_or_else(|| ctx.run_snapshot.default_env.clone());
    let dispatcher = ctx.run_snapshot.dispatcher(&effective_env).ok_or_else(|| {
        NodeError::Config(format!(
            "llm: env '{}' not in run snapshot scope",
            effective_env.as_str(),
        ))
    })?;
    Ok(dispatcher.http_transport())
}

/// Compose the legacy literal-URL into a chat-completions dispatch URL.
/// Phase D unconditionally appended `/chat/completions` to whatever
/// `url` the user supplied. Preserve that behaviour, with a guard so
/// users who already wrote out a fully-pathed dispatch URL don't get
/// `/chat/completions/chat/completions`:
///
/// - If the URL already ends in a known dispatch path
///   (`/chat/completions`, `/embeddings`, `/api/chat`, `/api/generate`),
///   keep it verbatim.
/// - Otherwise, append `/chat/completions` to the (trimmed) base.
fn compose_legacy_chat_url(raw: &str) -> String {
    let trimmed = raw.trim_end_matches('/');
    if KNOWN_LLM_DISPATCH_PATHS
        .iter()
        .any(|p| trimmed.ends_with(p))
    {
        return trimmed.to_string();
    }
    format!("{trimmed}/chat/completions")
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
    let resolved = resolve_resource_url(node, ctx, &cancel, false).await?;
    let DispatchRoute {
        url: url_string,
        transport,
        detail,
    } = route_or_fallback(resolved, node, ctx)?;
    let model = config_str(&node.config, "model", "llm")?;
    let messages = node
        .config
        .get("messages")
        .cloned()
        .ok_or_else(|| NodeError::Config("llm: 'messages' (array) required".into()))?;
    let temperature = config_f64_or(&node.config, "temperature", DEFAULT_MODEL_TEMP);
    let max_tokens = node.config.get("max_tokens").cloned();
    let stream_mode = read_stream_mode(node)?;
    let stream = resolve_effective_stream(
        node,
        ctx,
        stream_mode,
        detail.as_ref(),
        transport.as_ref(),
        &url_string,
    )?;
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

    let headers = build_request_headers(api_key.as_deref(), detail.as_ref(), ctx)?;
    let req = HttpRequest {
        method: HttpMethod::Post,
        url: url_string,
        headers,
        body: Some(serialize_body(&serde_json::Value::Object(body))?),
        timeout: Duration::from_millis(timeout_ms),
    };

    if stream {
        let stream_resp = tokio::select! {
            r = transport.execute_stream(req) => r.map_err(|e| NodeError::Http(format!("llm send: {e}")))?,
            () = cancel.cancelled() => return Err(NodeError::Cancelled),
        };
        read_sse_stream(stream_resp, ctx, &node.id, cancel).await
    } else {
        let resp = tokio::select! {
            r = transport.execute(req) => r.map_err(|e| NodeError::Http(format!("llm send: {e}")))?,
            () = cancel.cancelled() => return Err(NodeError::Cancelled),
        };
        if !is_success(resp.status) {
            return Ok(non_success_outputs(resp.status));
        }
        Ok(parse_complete_response(&resp.body))
    }
}

/// Build the request header map. Prefers an explicit `api_key` from
/// node config (Phase D's literal-auth behaviour); falls back to the
/// resource detail's `auth_secret_ref` resolved via the secrets store.
fn build_request_headers(
    api_key: Option<&str>,
    detail: Option<&ResourceDetail>,
    ctx: &RunContext,
) -> Result<HashMap<String, String>, NodeError> {
    let mut headers = HashMap::new();
    headers.insert("content-type".into(), "application/json".into());
    if let Some(key) = api_key {
        headers.insert("authorization".into(), format!("Bearer {key}"));
        return Ok(headers);
    }
    if let Some(ResourceDetail::HttpEndpoint {
        auth_secret_ref: Some(secret_ref),
        ..
    }) = detail
    {
        let store = ctx.secrets_store.as_ref().ok_or_else(|| {
            NodeError::Config(format!(
                "llm: secret '{}' required but no secrets store configured",
                secret_ref.0,
            ))
        })?;
        let token = store.get(secret_ref.0.as_str()).map_err(|e| {
            NodeError::Config(format!("llm: secret '{}' not found: {e}", secret_ref.0))
        })?;
        headers.insert("authorization".into(), format!("Bearer {token}"));
    }
    Ok(headers)
}

fn serialize_body(value: &serde_json::Value) -> Result<Bytes, NodeError> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|e| NodeError::Config(format!("llm: body serialize: {e}")))
}

const fn is_success(status: u16) -> bool {
    status >= 200 && status < 300
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
    let resolved = resolve_resource_url(node, ctx, &cancel, true).await?;
    let DispatchRoute {
        url: url_string,
        transport,
        detail,
    } = route_or_fallback(resolved, node, ctx)?;
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
    let stream = resolve_effective_stream(
        node,
        ctx,
        stream_mode,
        detail.as_ref(),
        transport.as_ref(),
        &url_string,
    )?;

    let initial_messages = node
        .config
        .get("messages")
        .cloned()
        .ok_or_else(|| NodeError::Config("llm: 'messages' (array) required".into()))?;
    let openai_tools = tools_to_openai_format(tools);
    let request_headers = build_request_headers(api_key.as_deref(), detail.as_ref(), ctx)?;

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

        let req = HttpRequest {
            method: HttpMethod::Post,
            url: url_string.clone(),
            headers: request_headers.clone(),
            body: Some(serialize_body(&serde_json::Value::Object(body))?),
            timeout: Duration::from_millis(timeout_ms),
        };

        let TurnResult {
            assistant_content,
            finish_reason,
            tokens_this_turn,
            message_value,
            http_status,
        } = if stream {
            let stream_resp = tokio::select! {
                r = transport.execute_stream(req) => r.map_err(|e| NodeError::Http(format!("llm send: {e}")))?,
                () = cancel.cancelled() => return Err(NodeError::Cancelled),
            };
            read_streaming_turn(stream_resp, ctx, &node.id, &cancel).await?
        } else {
            let resp = tokio::select! {
                r = transport.execute(req) => r.map_err(|e| NodeError::Http(format!("llm send: {e}")))?,
                () = cancel.cancelled() => return Err(NodeError::Cancelled),
            };
            read_non_streaming_turn(&resp)
        };
        if let Some(code) = http_status
            && !is_success(code)
        {
            final_finish_reason = format!("http_{code}");
            break;
        }
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
            let tool_msg = invoke_tool(&tc, tools, &transport, timeout_ms, &cancel).await?;
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

/// True iff the resolved route is allowed to stream under the Phase E
/// rule:
///
/// `route_origin != RouteOrigin::EnvLoopback AND transport.can_stream(url)`
///
/// `streaming_supported_natively` on the detail stays advisory in Phase
/// E — no probe yet populates it from real upstream introspection.
/// When that changes, fold it in as an additional AND clause.
///
/// `detail` is `Some` when the executor reached the route through the
/// resource catalog and `None` when it fell back to a literal `url`
/// node-config. In the literal-URL case the rule degrades to
/// `transport.can_stream(url)` only — the user explicitly typed a URL,
/// so we defer to the transport's own gate.
fn streaming_supported_for(
    detail: Option<&ResourceDetail>,
    transport: &dyn HttpTransport,
    url: &url::Url,
) -> bool {
    match detail {
        None => transport.can_stream(url),
        Some(ResourceDetail::HttpEndpoint { route_origin, .. }) => {
            *route_origin != RouteOrigin::EnvLoopback && transport.can_stream(url)
        },
        Some(_) => false,
    }
}

/// Apply the `StreamMode` rule against the resolved route. Returns
/// `Ok(true)` when the executor should stream, `Ok(false)` when it
/// should send a one-shot request (emitting a `StreamFallback` event
/// when `Auto` downgrades), and `Err(NodeError::Config)` when `Force`
/// is selected against a route that cannot stream.
fn resolve_effective_stream(
    node: &Node,
    ctx: &RunContext,
    stream_mode: StreamMode,
    detail: Option<&ResourceDetail>,
    transport: &dyn HttpTransport,
    url_str: &str,
) -> Result<bool, NodeError> {
    let parsed = url::Url::parse(url_str).map_err(|e| {
        NodeError::Config(format!("llm: cannot parse dispatch URL '{url_str}': {e}"))
    })?;
    let can_stream = streaming_supported_for(detail, transport, &parsed);
    match (stream_mode, can_stream) {
        (StreamMode::Off, _) => Ok(false),
        (StreamMode::Force | StreamMode::Auto, true) => Ok(true),
        (StreamMode::Force, false) => Err(NodeError::Config(format!(
            "llm: stream=force but route '{url_str}' does not support streaming"
        ))),
        (StreamMode::Auto, false) => {
            ctx.emitter.emit_stream_fallback(
                node.id.clone(),
                ctx.iteration,
                ctx.attempt.load(std::sync::atomic::Ordering::Relaxed),
                url_str.to_string(),
                "route does not support streaming".to_string(),
            );
            Ok(false)
        },
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
    mut stream: crate::environment::runtime::dispatcher::ResponseStream,
    ctx: &RunContext,
    node_id: &str,
    cancel: CancellationToken,
) -> Result<NodeOutputs, NodeError> {
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
    transport: &Arc<dyn HttpTransport>,
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

    let method = parse_http_method(&tool.method)
        .map_err(|e| NodeError::Config(format!("llm.tool[{}]: invalid method: {e}", tool.name)))?;
    let mut headers = HashMap::new();
    headers.insert("content-type".into(), "application/json".into());
    for (k, v) in &tool.headers {
        headers.insert(k.clone(), v.clone());
    }
    let req = HttpRequest {
        method,
        url: tool.url.clone(),
        headers,
        body: Some(Bytes::from(body_text.into_bytes())),
        timeout: Duration::from_millis(timeout_ms),
    };
    let resp = tokio::select! {
        r = transport.execute(req) => r.map_err(|e| NodeError::Http(format!("llm.tool[{}] send: {e}", tool.name)))?,
        () = cancel.cancelled() => return Err(NodeError::Cancelled),
    };
    let body_str = String::from_utf8_lossy(&resp.body).into_owned();

    let result_text = select_result_text(tool.result_path.as_deref(), &body_str);

    Ok(serde_json::json!({
        "tool": tool.name,
        "args": args_json,
        "status": resp.status,
        "result_text": result_text,
    }))
}

fn parse_http_method(s: &str) -> Result<HttpMethod, String> {
    match s.to_ascii_uppercase().as_str() {
        "GET" => Ok(HttpMethod::Get),
        "HEAD" => Ok(HttpMethod::Head),
        "POST" => Ok(HttpMethod::Post),
        "PUT" => Ok(HttpMethod::Put),
        "PATCH" => Ok(HttpMethod::Patch),
        "DELETE" => Ok(HttpMethod::Delete),
        other => Err(format!("unsupported HTTP method '{other}'")),
    }
}

/// Per-turn assembled result from either the streaming or
/// non-streaming reader. `message_value` mirrors `OpenAI`'s
/// `{role, content, tool_calls?}` shape so the tool loop can push
/// it onto the messages list verbatim. `http_status` carries the
/// response status so the tool loop can short-circuit on non-2xx
/// without losing the streaming/non-streaming distinction; `Some`
/// for non-streaming, `None` for streaming (the SSE parser already
/// surfaces failures via early returns).
struct TurnResult {
    assistant_content: String,
    finish_reason: String,
    tokens_this_turn: u64,
    message_value: serde_json::Value,
    http_status: Option<u16>,
}

/// Parse one non-streaming `chat/completions` response into a
/// `TurnResult`. Network/transport errors are already surfaced by the
/// transport call; this just decodes the body.
fn read_non_streaming_turn(resp: &HttpResponse) -> TurnResult {
    let status = resp.status;
    if !is_success(status) {
        return TurnResult {
            assistant_content: String::new(),
            finish_reason: format!("http_{status}"),
            tokens_this_turn: 0,
            message_value: serde_json::Value::Null,
            http_status: Some(status),
        };
    }
    let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&resp.body) else {
        return TurnResult {
            assistant_content: String::new(),
            finish_reason: "parse_error".into(),
            tokens_this_turn: 0,
            message_value: serde_json::Value::Null,
            http_status: Some(status),
        };
    };
    let tokens = parsed
        .pointer("/usage/total_tokens")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let Some(choice) = parsed.pointer("/choices/0") else {
        return TurnResult {
            assistant_content: String::new(),
            finish_reason: "parse_error".into(),
            tokens_this_turn: tokens,
            message_value: serde_json::Value::Null,
            http_status: Some(status),
        };
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
    TurnResult {
        assistant_content,
        finish_reason,
        tokens_this_turn: tokens,
        message_value,
        http_status: Some(status),
    }
}

/// Read one streaming `chat/completions` response: parse each
/// `data: {...}` event, accumulate content + tool-call argument
/// deltas, emit content chunks as `node:output` events on the `llm`
/// channel. Returns the assembled `TurnResult` whose `message_value`
/// mirrors the non-streaming shape so the tool loop is agnostic.
async fn read_streaming_turn(
    mut stream: crate::environment::runtime::dispatcher::ResponseStream,
    ctx: &RunContext,
    node_id: &str,
    cancel: &CancellationToken,
) -> Result<TurnResult, NodeError> {
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
                    http_status: None,
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
        http_status: None,
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
        // Phase E surfaces status via the non-streaming response shape;
        // the streaming-error path is covered by integration tests once
        // `StreamMode::Force` errors land in Task 12.
        let out = LlmExecutor
            .run(
                &llm_node(&server.uri(), false),
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

    // ── Task 12: StreamMode enforcement ──────────────────────────────

    use crate::environment::runtime::FakeHttpTransport;
    use crate::environment::runtime::local::LocalHttpTransport;

    fn http_endpoint(route_origin: RouteOrigin) -> ResourceDetail {
        ResourceDetail::HttpEndpoint {
            base_url: "http://127.0.0.1:11434".into(),
            routes_by_capability: HashMap::new(),
            version: None,
            models_list: None,
            auth_secret_ref: None,
            streaming_supported_natively: false,
            route_origin,
        }
    }

    #[test]
    fn streaming_supported_envloopback_blocks_even_when_transport_can_stream() {
        // EnvLoopback + LocalHttpTransport (can_stream=true) → false.
        let transport = LocalHttpTransport::new();
        let url = url::Url::parse("http://127.0.0.1:11434/v1/chat/completions").unwrap();
        let detail = http_endpoint(RouteOrigin::EnvLoopback);
        assert!(!streaming_supported_for(Some(&detail), &transport, &url));
    }

    #[test]
    fn streaming_supported_hostdirect_with_local_transport_streams() {
        let transport = LocalHttpTransport::new();
        let url = url::Url::parse("http://127.0.0.1:11434/v1/chat/completions").unwrap();
        let detail = http_endpoint(RouteOrigin::HostDirect);
        assert!(streaming_supported_for(Some(&detail), &transport, &url));
    }

    #[test]
    fn streaming_supported_hostdirect_but_transport_cannot_stream_blocks() {
        // HostDirect route but FakeHttpTransport reports can_stream=false → false.
        let transport = FakeHttpTransport;
        let url = url::Url::parse("http://127.0.0.1:11434/v1/chat/completions").unwrap();
        let detail = http_endpoint(RouteOrigin::HostDirect);
        assert!(!streaming_supported_for(Some(&detail), &transport, &url));
    }

    #[test]
    fn streaming_supported_literal_url_defers_to_transport() {
        // No detail (literal-URL fallback) → transport.can_stream alone.
        let url = url::Url::parse("http://127.0.0.1:11434/v1/chat/completions").unwrap();
        let local = LocalHttpTransport::new();
        assert!(streaming_supported_for(None, &local, &url));
        let fake = FakeHttpTransport;
        assert!(!streaming_supported_for(None, &fake, &url));
    }

    #[test]
    fn streaming_supported_binary_detail_is_never_streamable() {
        // Non-HttpEndpoint detail (Binary, Toolchain) → false regardless.
        let transport = LocalHttpTransport::new();
        let url = url::Url::parse("http://127.0.0.1:11434/v1/chat/completions").unwrap();
        let detail = ResourceDetail::Binary {
            path: "/usr/bin/echo".into(),
            version: None,
            capabilities: vec![],
        };
        assert!(!streaming_supported_for(Some(&detail), &transport, &url));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resolve_effective_stream_force_errors_on_envloopback() {
        let (ctx, _rx, _dir) = make_ctx();
        let node = llm_node("http://127.0.0.1:11434/v1/chat/completions", true);
        let detail = http_endpoint(RouteOrigin::EnvLoopback);
        let transport = LocalHttpTransport::new();
        let err = resolve_effective_stream(
            &node,
            &ctx,
            StreamMode::Force,
            Some(&detail),
            &transport,
            "http://127.0.0.1:11434/v1/chat/completions",
        )
        .expect_err("Force on EnvLoopback must error");
        match err {
            NodeError::Config(msg) => {
                assert!(
                    msg.contains("stream=force") && msg.contains("does not support streaming"),
                    "expected force-stream config error, got: {msg}",
                );
            },
            other => panic!("expected NodeError::Config, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resolve_effective_stream_auto_falls_back_and_emits_event() {
        let (ctx, mut rx, _dir) = make_ctx();
        let node = llm_node("http://127.0.0.1:11434/v1/chat/completions", true);
        let detail = http_endpoint(RouteOrigin::EnvLoopback);
        let transport = LocalHttpTransport::new();
        let stream = resolve_effective_stream(
            &node,
            &ctx,
            StreamMode::Auto,
            Some(&detail),
            &transport,
            "http://127.0.0.1:11434/v1/chat/completions",
        )
        .expect("Auto fallback is Ok");
        assert!(
            !stream,
            "Auto on EnvLoopback must downgrade to non-streaming"
        );

        let mut saw_fallback = false;
        while let Ok(ev) = rx.try_recv() {
            if ev.ty == EventType::StreamFallback {
                let url = ev
                    .payload
                    .get("url")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                let reason = ev
                    .payload
                    .get("reason")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                assert_eq!(url, "http://127.0.0.1:11434/v1/chat/completions");
                assert!(reason.contains("does not support streaming"));
                saw_fallback = true;
                break;
            }
        }
        assert!(saw_fallback, "expected a stream:fallback event");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resolve_effective_stream_off_never_streams() {
        let (ctx, _rx, _dir) = make_ctx();
        let node = llm_node("http://127.0.0.1:11434/v1/chat/completions", false);
        let detail = http_endpoint(RouteOrigin::HostDirect);
        let transport = LocalHttpTransport::new();
        let stream = resolve_effective_stream(
            &node,
            &ctx,
            StreamMode::Off,
            Some(&detail),
            &transport,
            "http://127.0.0.1:11434/v1/chat/completions",
        )
        .expect("Off is Ok");
        assert!(!stream, "Off must never stream even when route supports it");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resolve_effective_stream_force_passes_on_hostdirect() {
        let (ctx, _rx, _dir) = make_ctx();
        let node = llm_node("http://127.0.0.1:11434/v1/chat/completions", true);
        let detail = http_endpoint(RouteOrigin::HostDirect);
        let transport = LocalHttpTransport::new();
        let stream = resolve_effective_stream(
            &node,
            &ctx,
            StreamMode::Force,
            Some(&detail),
            &transport,
            "http://127.0.0.1:11434/v1/chat/completions",
        )
        .expect("Force on streamable route is Ok");
        assert!(stream, "Force on HostDirect must request streaming");
    }
}
