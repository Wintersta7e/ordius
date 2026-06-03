//! `http` built-in: HTTP via the per-env `HttpTransport`.
//!
//! Failure policy is the one the spec locks in: any HTTP response
//! (including 4xx / 5xx) returns `Ok(NodeOutputs)` with the status
//! code on the `status` output port. Only network-level failures
//! (DNS, connection refused, timeout) return [`NodeError::Http`].
//! Retry-on-status is a workflow-graph concern (downstream
//! `condition` node), not an executor concern.

use super::util::{config_str, config_str_or, config_u64_or};
use crate::environment::runtime::ResourceProbeOutcome;
use crate::environment::runtime::catalog::{ResourceDetail, RouteOrigin};
use crate::environment::runtime::dispatcher::HttpTransport;
use crate::environment::runtime::env::{EnvId, EnvSpec};
use crate::environment::runtime::resource::{ResourceId, ResourceRef};
use crate::environment::runtime::transport::{HttpMethod, HttpRequest, HttpResponse};
use crate::executor::{NodeError, NodeExecutor, NodeOutputs, RunContext};
use crate::types::{Node, NodeType, PortValue};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use url::Url;

const DEFAULT_TIMEOUT_MS: u64 = 30_000;
#[allow(unreachable_pub)]
pub const NODE_TYPE_ID: &str = "http";

/// Where an HTTP request originates from when the node has a
/// `target_env` set. `Env` (default) means the request runs from
/// inside the target env (e.g. via the WSL transport for WSL).
/// `Host` forces the request to originate from the Ordius host
/// process even when `target_env != local` — useful for "talk to
/// the public Internet from my host even though this node otherwise
/// runs in WSL".
///
/// Phase E routes `Origin::Host` through the local dispatcher's
/// transport (regardless of `target_env`) and gates loopback URLs
/// against `host_direct_verifications` so a host-side request that
/// would otherwise hit the wrong loopback service is rejected.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
#[allow(unreachable_pub)]
pub enum Origin {
    #[default]
    Env,
    Host,
}

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
        ctx: &RunContext,
        cancel: CancellationToken,
    ) -> Result<NodeOutputs, NodeError> {
        let route = resolve_url(node, ctx, &cancel).await?;

        let method_str = config_str_or(&node.config, "method", "GET");
        let method = parse_http_method(method_str)?;
        let timeout_ms = config_u64_or(&node.config, "timeout_ms", DEFAULT_TIMEOUT_MS);
        let mut headers: HashMap<String, String> = node
            .config
            .get("headers")
            .map(parse_headers_map)
            .transpose()?
            .unwrap_or_default();

        // Pre-join query params into the URL string. `HttpRequest` has no
        // query field. If the user's URL already has a query, append (so
        // both the literal and the resource branches preserve any query
        // present on the resolved URL).
        let mut final_url = route.url.clone();
        if let Some(query_val) = node.config.get("query") {
            let query_str = encode_query_value(query_val)
                .map_err(|e| NodeError::Config(format!("http: invalid query: {e}")))?;
            if !query_str.is_empty() {
                let existing = final_url.query().map(str::to_string);
                let combined = match existing {
                    Some(ref e) if !e.is_empty() => format!("{e}&{query_str}"),
                    _ => query_str,
                };
                final_url.set_query(Some(&combined));
            }
        }

        let body = match node.config.get("body") {
            Some(serde_json::Value::String(s)) => Some(Bytes::from(s.clone().into_bytes())),
            Some(other) => {
                headers
                    .entry("content-type".to_string())
                    .or_insert_with(|| "application/json".to_string());
                Some(Bytes::from(serde_json::to_vec(other).map_err(|e| {
                    NodeError::Config(format!("http: body serialize: {e}"))
                })?))
            },
            None => None,
        };

        let req = HttpRequest {
            method,
            url: final_url.to_string(),
            headers,
            body,
            timeout: Duration::from_millis(timeout_ms),
        };

        let resp = tokio::select! {
            r = route.transport.execute(req) => r.map_err(|e| NodeError::Http(e.to_string()))?,
            () = cancel.cancelled() => return Err(NodeError::Cancelled),
        };

        Ok(build_outputs_from_response(resp))
    }
}

/// Resolved HTTP route returned from [`resolve_url`].
///
/// `detail` is `Some` only for the resource form; literal `url` form
/// returns `None`. `effective_env` is the env the resource lookup
/// resolved against; `transport` is the dispatcher we send through —
/// the two diverge under `Origin::Host` (lookup in `target_env`, send
/// via the local dispatcher).
struct HttpRoute {
    url: Url,
    transport: Arc<dyn HttpTransport>,
}

impl std::fmt::Debug for HttpRoute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpRoute")
            .field("url", &self.url.as_str())
            .field("transport", &"<HttpTransport>")
            .finish()
    }
}

/// Resolve the node's effective HTTP route from either:
///
/// - `url`: literal string → parsed verbatim.
/// - `resource` + `path`: registry-driven. The resource id is looked up
///   in the run snapshot's catalog for `effective_env`; on a `Found`
///   `HttpEndpoint`, `path` is joined onto the catalog's `base_url`
///   via [`url::Url::join`]. Stale (`NotFound` / `Skipped` / missing)
///   entries trigger an opportunistic singleflight re-probe.
///
/// Mutually exclusive: setting both is rejected. `resource` without
/// `path` is rejected.
///
/// `Origin::Host` gate: when `effective_env != EnvId::local()`,
/// `origin == Origin::Host`, and the resolved URL is loopback, the
/// request is refused unless the resource is proved via
/// `RouteOrigin::HostDirect` or a matching `host_direct_verifications`
/// entry exists on the env's spec. Literal-URL form has no resource
/// id to key the verification map by and is rejected outright.
async fn resolve_url(
    node: &Node,
    ctx: &RunContext,
    cancel: &CancellationToken,
) -> Result<HttpRoute, NodeError> {
    let has_resource = node.config.contains_key("resource");
    let has_url = node.config.contains_key("url");
    if has_resource && has_url {
        return Err(NodeError::Config(
            "http: 'url' and 'resource' are mutually exclusive".into(),
        ));
    }

    let origin: Origin = node
        .config
        .get("origin")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|e| NodeError::Config(format!("http: invalid origin: {e}")))?
        .unwrap_or_default();

    let effective_env = node
        .target_env
        .clone()
        .unwrap_or_else(|| ctx.run_snapshot.default_env.clone());

    // Transport selection: `Origin::Env` routes via the target env's
    // dispatcher; `Origin::Host` always routes via the local dispatcher.
    // Resource lookup still happens against `effective_env`.
    let transport_env = match origin {
        Origin::Env => effective_env.clone(),
        Origin::Host => EnvId::local(),
    };
    let transport = ctx
        .run_snapshot
        .dispatcher(&transport_env)
        .ok_or_else(|| {
            NodeError::Config(format!(
                "http: transport env '{}' not in snapshot scope",
                transport_env.as_str(),
            ))
        })?
        .http_transport();

    let mut resource_id_for_gate: Option<ResourceId> = None;
    let (url, detail) = if has_resource {
        let (url, detail, rid) = resolve_resource_branch(node, ctx, &effective_env, cancel).await?;
        resource_id_for_gate = Some(rid);
        (url, Some(detail))
    } else {
        let url_str = config_str(&node.config, "url", "http")?;
        let url = Url::parse(url_str)
            .map_err(|e| NodeError::Config(format!("http: invalid url '{url_str}': {e}")))?;
        (url, None)
    };

    enforce_origin_host_gate(
        &url,
        origin,
        &effective_env,
        detail.as_ref(),
        resource_id_for_gate.as_ref(),
        ctx,
    )?;
    drop(detail); // detail consumed by the gate; drop after use.

    Ok(HttpRoute { url, transport })
}

/// Resolve the resource-form branch into a `(url, detail, resource_id)` triple.
/// Looks up the resource in the run snapshot's catalog, falling through to a
/// singleflight opportunistic re-probe on stale/missing entries.
async fn resolve_resource_branch(
    node: &Node,
    ctx: &RunContext,
    effective_env: &EnvId,
    cancel: &CancellationToken,
) -> Result<(Url, ResourceDetail, ResourceId), NodeError> {
    let rref_val = node
        .config
        .get("resource")
        .expect("caller checked has_resource");
    let rref: ResourceRef = serde_json::from_value(rref_val.clone())
        .map_err(|e| NodeError::Config(format!("http: invalid resource ref: {e}")))?;
    let path = node
        .config
        .get("path")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| NodeError::Config("http: 'path' required when 'resource' is set".into()))?;

    let catalog = ctx.run_snapshot.catalog(effective_env).ok_or_else(|| {
        NodeError::Config(format!(
            "http: env '{}' not in snapshot scope",
            effective_env.as_str(),
        ))
    })?;
    let res_dispatcher = ctx.run_snapshot.dispatcher(effective_env).ok_or_else(|| {
        NodeError::Config(format!(
            "http: env '{}' has no dispatcher",
            effective_env.as_str(),
        ))
    })?;

    let outcome = if let Some(o @ ResourceProbeOutcome::Found(_)) = catalog.lookup(rref.id()) {
        o
    } else {
        let workflow_id = ctx.run_snapshot.workflow_id.clone();
        let (def, _scope) = ctx
            .run_snapshot
            .registry
            .resolve(rref.id(), effective_env, Some(&workflow_id))
            .ok_or_else(|| {
                NodeError::Config(format!(
                    "http: resource '{}' not in registry (env '{}')",
                    rref.id().0,
                    effective_env.as_str(),
                ))
            })?;
        let def = def.clone();
        Arc::clone(catalog)
            .opportunistic_reprobe(&def, Arc::clone(res_dispatcher), cancel.child_token())
            .await
    };

    let detail = match outcome {
        ResourceProbeOutcome::Found(d) => d,
        non_found => {
            return Err(NodeError::Config(format!(
                "http: resource '{}' not reachable in env '{}': {non_found:?}",
                rref.id().0,
                effective_env.as_str(),
            )));
        },
    };
    let ResourceDetail::HttpEndpoint { base_url, .. } = &detail else {
        return Err(NodeError::Config(format!(
            "http: resource '{}' is not an HTTP endpoint",
            rref.id().0,
        )));
    };
    // URL join via `url::Url` — raw string concat is wrong: a base
    // `http://x:11434` joined with a leading-`/` path needs the slash
    // boundary handled by the URL crate, not by `format!`.
    let base = Url::parse(base_url)
        .map_err(|e| NodeError::Config(format!("http: malformed base_url '{base_url}': {e}")))?;
    let url = base.join(path).map_err(|e| {
        NodeError::Config(format!(
            "http: malformed path '{path}' for base '{base_url}': {e}",
        ))
    })?;
    Ok((url, detail, rref.id().clone()))
}

/// Enforce the `Origin::Host` gate (spec Delta 6). Refuses loopback in a
/// non-local env unless the resource is proved `HostDirect` or has a matching
/// `host_direct_verifications` entry on the env spec.
fn enforce_origin_host_gate(
    url: &Url,
    origin: Origin,
    effective_env: &EnvId,
    detail: Option<&ResourceDetail>,
    resource_id_for_gate: Option<&ResourceId>,
    ctx: &RunContext,
) -> Result<(), NodeError> {
    if !matches!(origin, Origin::Host) {
        return Ok(());
    }
    if effective_env == &EnvId::local() {
        return Ok(());
    }
    if !is_loopback_url(url) {
        return Ok(());
    }
    let ok_via_route_origin = detail.is_some_and(|d| {
        matches!(
            d,
            ResourceDetail::HttpEndpoint {
                route_origin: RouteOrigin::HostDirect,
                ..
            }
        )
    });
    let ok_via_verifications = resource_id_for_gate.is_some_and(|rid| {
        ctx.run_snapshot
            .spec_for(effective_env)
            .and_then(|spec| host_direct_map_for(spec))
            .is_some_and(|map| {
                map.get(rid).is_some_and(|v| {
                    Url::parse(&v.host_url).is_ok_and(|verified| verified.origin() == url.origin())
                })
            })
    });
    if ok_via_route_origin || ok_via_verifications {
        return Ok(());
    }
    let id_label = resource_id_for_gate.map_or("<literal-url>", |id| id.0.as_str());
    Err(NodeError::Config(format!(
        "http: origin=host on loopback URL '{url}' requires a HostDirect \
         verification on resource '{id_label}' (env '{}'). Run 'Test direct \
         access' in Settings on this resource, or drop origin=host.",
        effective_env.as_str(),
    )))
}

/// True if the URL points at host loopback. Same surface as Phase B's
/// `wsl/transport.rs::is_loopback_url`; kept private here because the
/// other copy is private too.
fn is_loopback_url(url: &Url) -> bool {
    match url.host() {
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        Some(url::Host::Domain(d)) => d.eq_ignore_ascii_case("localhost"),
        None => false,
    }
}

/// Pluck the `host_direct_verifications` map out of any `EnvSpec` variant
/// that carries one (Local / `WslDistro` / Container). Ssh has its own
/// reach model and lacks this field.
const fn host_direct_map_for(
    spec: &EnvSpec,
) -> Option<&HashMap<ResourceId, crate::environment::runtime::env::HostDirectVerification>> {
    match spec {
        EnvSpec::Local {
            host_direct_verifications,
            ..
        }
        | EnvSpec::WslDistro {
            host_direct_verifications,
            ..
        }
        | EnvSpec::Container {
            host_direct_verifications,
            ..
        } => Some(host_direct_verifications),
        EnvSpec::Ssh { .. } => None,
    }
}

/// Parse `HttpMethod` from a case-insensitive method string.
fn parse_http_method(s: &str) -> Result<HttpMethod, NodeError> {
    match s.to_ascii_uppercase().as_str() {
        "GET" => Ok(HttpMethod::Get),
        "HEAD" => Ok(HttpMethod::Head),
        "POST" => Ok(HttpMethod::Post),
        "PUT" => Ok(HttpMethod::Put),
        "PATCH" => Ok(HttpMethod::Patch),
        "DELETE" => Ok(HttpMethod::Delete),
        other => Err(NodeError::Config(format!(
            "http: unsupported method '{other}'",
        ))),
    }
}

/// Parse a `serde_json::Value` headers map into the transport's plain
/// `HashMap<String, String>` shape (lower-casing happens at the transport
/// boundary).
fn parse_headers_map(val: &serde_json::Value) -> Result<HashMap<String, String>, NodeError> {
    let map = val
        .as_object()
        .ok_or_else(|| NodeError::Config("http: 'headers' must be an object".into()))?;
    let mut out = HashMap::with_capacity(map.len());
    for (k, v) in map {
        let s = v.as_str().ok_or_else(|| {
            NodeError::Config(format!("http: header '{k}' value must be a string"))
        })?;
        out.insert(k.clone(), s.to_string());
    }
    Ok(out)
}

/// Encode a `serde_json::Value` as a URL-encoded query string. Keys + string
/// values are percent-encoded; numbers/bools coerce to their text form.
fn encode_query_value(val: &serde_json::Value) -> Result<String, &'static str> {
    let obj = val.as_object().ok_or("query must be an object")?;
    let mut parts = Vec::with_capacity(obj.len());
    for (k, v) in obj {
        let key = urlencoding::encode(k);
        match v {
            serde_json::Value::String(s) => {
                parts.push(format!("{key}={}", urlencoding::encode(s)));
            },
            serde_json::Value::Number(n) => {
                parts.push(format!("{key}={n}"));
            },
            serde_json::Value::Bool(b) => {
                parts.push(format!("{key}={b}"));
            },
            _ => return Err("query value must be string/number/bool"),
        }
    }
    Ok(parts.join("&"))
}

/// Build the node outputs from an `HttpResponse`. Preserves Phase D
/// semantics: status code on `status` port; headers on `headers` port;
/// body on `body` port as JSON when content-type matches, otherwise as
/// a String (lossy UTF-8 fallback if the body isn't valid UTF-8).
fn build_outputs_from_response(resp: HttpResponse) -> NodeOutputs {
    let status = resp.status;
    let is_json = resp
        .headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-type"))
        .is_some_and(|(_, v)| v.starts_with("application/json"));
    let body_str = String::from_utf8_lossy(&resp.body).into_owned();
    let body_port = if is_json {
        serde_json::from_str::<serde_json::Value>(&body_str)
            .ok()
            .map_or_else(move || PortValue::String(body_str), PortValue::Json)
    } else {
        PortValue::String(body_str)
    };

    let headers_json = serde_json::Value::Object(
        resp.headers
            .into_iter()
            .map(|(k, v)| (k, serde_json::Value::String(v)))
            .collect(),
    );

    let mut out = NodeOutputs::new();
    out.insert("status".into(), PortValue::Number(f64::from(status)));
    out.insert("body".into(), body_port);
    out.insert("headers".into(), PortValue::Json(headers_json));
    out
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
            skip_config_templates: false,
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
            target_env: None,
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

    // ── resolve_url + Origin::Host gate ───────────────────────────────

    use crate::Engine;
    use crate::checkpoints::CheckpointRegistry;
    use crate::db::open;
    use crate::emitter::Emitter;
    use crate::environment::runtime::dispatcher::Dispatcher;
    use crate::environment::runtime::env::{
        EnvInfo, EnvState, HostDirectMethod, HostDirectVerification,
    };
    use crate::environment::runtime::local::LocalDispatcher;
    use crate::environment::runtime::resource::{
        ApiFlavor, HttpProbeMethod, HttpProbeRoute, ProbeSpec, ResourceDefinition, ResourceKind,
    };
    use crate::environment::runtime::run_catalog::RunCatalog;
    use crate::environment::runtime::{EnvId, RunSnapshot, WorkflowId, install_workflow_resources};
    use crate::executor::wrap_process_env;
    use crate::recorder::RunRecorder;
    use crate::types::Workflow;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

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
        let run_snapshot =
            crate::executor::test_support::test_run_snapshot(&rec.run_id, workflow_id);
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
            run_snapshot,
            engine: Arc::downgrade(&engine),
            compose_depth: 0,
            iteration: 1,
            attempt: AtomicU32::new(1),
            auto_resume: false,
            workspace_manager: std::sync::Arc::new(
                crate::environment::runtime::workspace::WorkspaceManager::new(),
            ),
            env_cwd: parking_lot::Mutex::new(None),
            run_cancel: tokio_util::sync::CancellationToken::new(),
        };
        (engine, ctx, dir)
    }

    fn http_node_with(config: HashMap<String, serde_json::Value>) -> Node {
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

    /// Build a `RunSnapshot` with `local` + `wsl:test` envs. The wsl env
    /// has an overlay-installed `HttpEndpoint` resource at loopback, the
    /// caller-supplied `host_direct_verifications`, and the registry
    /// holds an `ollama` resource definition so opportunistic re-probe
    /// (if it fires) can resolve. Used by the `Origin::Host` gate tests.
    fn build_two_env_snapshot(
        workflow_id: &str,
        verifications: HashMap<ResourceId, HostDirectVerification>,
        route_origin: RouteOrigin,
        run_id: &str,
    ) -> Arc<RunSnapshot> {
        let local_id = EnvId::local();
        let wsl_id = EnvId::wsl("test");

        let local_info = EnvInfo {
            id: local_id.clone(),
            label: "local".into(),
            spec: EnvSpec::Local {
                resources: vec![],
                host_direct_verifications: HashMap::new(),
            },
            state: EnvState::Reachable,
            enabled: true,
        };
        let wsl_spec = EnvSpec::WslDistro {
            name: "test".into(),
            resources: vec![],
            host_direct_verifications: verifications,
        };
        let wsl_info = EnvInfo {
            id: wsl_id.clone(),
            label: "wsl:test".into(),
            spec: wsl_spec.clone(),
            state: EnvState::Reachable,
            enabled: true,
        };

        let local_dispatcher: Arc<dyn Dispatcher> = Arc::new(LocalDispatcher::new(local_info));
        let wsl_dispatcher: Arc<dyn Dispatcher> = Arc::new(LocalDispatcher::new(wsl_info));
        let mut dispatchers: HashMap<EnvId, Arc<dyn Dispatcher>> = HashMap::new();
        dispatchers.insert(local_id.clone(), local_dispatcher);
        dispatchers.insert(wsl_id.clone(), wsl_dispatcher);

        let wsl_catalog = empty_catalog(&wsl_id);
        let wsl_run_catalog = Arc::new(RunCatalog::new(wsl_id.clone(), wsl_catalog));
        wsl_run_catalog.install_overlay_for_test(
            ResourceId("ollama".into()),
            ResourceDetail::HttpEndpoint {
                base_url: "http://127.0.0.1:11434".into(),
                routes_by_capability: HashMap::new(),
                version: None,
                models_list: None,
                auth_secret_ref: None,
                streaming_supported_natively: true,
                route_origin,
            },
        );
        let local_run_catalog =
            Arc::new(RunCatalog::new(local_id.clone(), empty_catalog(&local_id)));

        let mut catalogs: HashMap<EnvId, Arc<RunCatalog>> = HashMap::new();
        catalogs.insert(wsl_id.clone(), wsl_run_catalog);
        catalogs.insert(local_id.clone(), local_run_catalog);

        let mut specs: HashMap<EnvId, EnvSpec> = HashMap::new();
        specs.insert(
            local_id.clone(),
            EnvSpec::Local {
                resources: vec![],
                host_direct_verifications: HashMap::new(),
            },
        );
        specs.insert(wsl_id, wsl_spec);

        let registry = crate::environment::runtime::ResourceRegistry::new();
        install_workflow_resources(
            &WorkflowId(workflow_id.into()),
            &[ResourceDefinition {
                id: ResourceId("ollama".into()),
                kind: ResourceKind::HttpEndpoint,
                advertised_capabilities: vec![],
                probe: ProbeSpec::Http {
                    ports: vec![11434],
                    routes: vec![HttpProbeRoute {
                        path: "/api/version".into(),
                        method: HttpProbeMethod::Get,
                        flavor: ApiFlavor::OllamaNative,
                        proves: vec![],
                        models_jsonpath: None,
                        fingerprint_jsonpaths: vec![],
                    }],
                    timeout_ms: None,
                },
                override_lower_scope: false,
            }],
            &registry,
        )
        .expect("install resource");

        Arc::new(RunSnapshot {
            run_id: run_id.to_string(),
            workflow_id: WorkflowId(workflow_id.into()),
            default_env: local_id,
            registry: registry.snapshot(),
            dispatchers: Arc::new(dispatchers),
            catalogs: Arc::new(catalogs),
            specs: Arc::new(specs),
        })
    }

    fn empty_catalog(env_id: &EnvId) -> Arc<crate::environment::runtime::catalog::ResourceCatalog> {
        Arc::new(crate::environment::runtime::catalog::ResourceCatalog {
            env_id: env_id.clone(),
            registry_revision: 1,
            probed_at: chrono::Utc::now(),
            resources: HashMap::new(),
        })
    }

    /// Build a `RunContext` whose snapshot has a non-local env (`wsl:test`)
    /// alongside the default local env. The fake env carries a single
    /// overlay-installed `HttpEndpoint` resource at loopback so the
    /// `Origin::Host` gate test exercises the real loopback branch.
    fn ctx_with_two_envs(
        workflow_id: &str,
        verifications: HashMap<ResourceId, HostDirectVerification>,
        route_origin: RouteOrigin,
    ) -> (RunContext, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
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

        let snapshot =
            build_two_env_snapshot(workflow_id, verifications, route_origin, &rec.run_id);

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
            run_snapshot: snapshot,
            engine: std::sync::Weak::new(),
            compose_depth: 0,
            iteration: 1,
            attempt: AtomicU32::new(1),
            auto_resume: false,
            workspace_manager: std::sync::Arc::new(
                crate::environment::runtime::workspace::WorkspaceManager::new(),
            ),
            env_cwd: parking_lot::Mutex::new(None),
            run_cancel: tokio_util::sync::CancellationToken::new(),
        };
        (ctx, dir)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn url_and_resource_mutually_exclusive() {
        let (_engine, ctx, _dir) = ctx_with_engine("wf-excl").await;
        let mut cfg: HashMap<String, serde_json::Value> = HashMap::new();
        cfg.insert("url".into(), serde_json::json!("http://example.invalid/x"));
        cfg.insert("resource".into(), serde_json::json!("anything"));
        cfg.insert("path".into(), serde_json::json!("/y"));
        let node = http_node_with(cfg);

        let err = resolve_url(&node, &ctx, &CancellationToken::new())
            .await
            .expect_err("both fields set must reject");
        let msg = format!("{err:?}");
        assert!(matches!(err, NodeError::Config(_)), "got {msg}");
        assert!(
            msg.contains("mutually exclusive"),
            "expected mutually-exclusive hint, got {msg}",
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resource_without_path_errors() {
        let (_engine, ctx, _dir) = ctx_with_engine("wf-nopath").await;
        let mut cfg: HashMap<String, serde_json::Value> = HashMap::new();
        cfg.insert("resource".into(), serde_json::json!("anything"));
        let node = http_node_with(cfg);

        let err = resolve_url(&node, &ctx, &CancellationToken::new())
            .await
            .expect_err("missing path must reject");
        let msg = format!("{err:?}");
        assert!(matches!(err, NodeError::Config(_)), "got {msg}");
        assert!(
            msg.contains("'path' required"),
            "expected path-required hint, got {msg}",
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resource_form_url_joins_path() {
        // Resource catalog seeded with base_url "http://127.0.0.1:11434"
        // + path "/api/version" → joined URL preserves the slash boundary.
        let (ctx, _dir) = ctx_with_two_envs("wf-join", HashMap::new(), RouteOrigin::EnvLoopback);
        let mut cfg: HashMap<String, serde_json::Value> = HashMap::new();
        cfg.insert("resource".into(), serde_json::json!("ollama"));
        cfg.insert("path".into(), serde_json::json!("/api/version"));
        let mut node = http_node_with(cfg);
        node.target_env = Some(EnvId::wsl("test"));

        let route = resolve_url(&node, &ctx, &CancellationToken::new())
            .await
            .expect("resource + path resolves");
        assert_eq!(route.url.as_str(), "http://127.0.0.1:11434/api/version");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn origin_host_loopback_without_verification_errors() {
        let (ctx, _dir) = ctx_with_two_envs("wf-gate", HashMap::new(), RouteOrigin::EnvLoopback);
        let mut cfg: HashMap<String, serde_json::Value> = HashMap::new();
        cfg.insert("resource".into(), serde_json::json!("ollama"));
        cfg.insert("path".into(), serde_json::json!("/api/version"));
        cfg.insert("origin".into(), serde_json::json!("host"));
        let mut node = http_node_with(cfg);
        node.target_env = Some(EnvId::wsl("test"));

        let err = resolve_url(&node, &ctx, &CancellationToken::new())
            .await
            .expect_err("origin=host on loopback must reject without verification");
        let msg = format!("{err:?}");
        assert!(matches!(err, NodeError::Config(_)), "got {msg}");
        assert!(
            msg.contains("HostDirect"),
            "expected gate message, got {msg}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn origin_host_loopback_with_host_direct_route_origin_passes() {
        let (ctx, _dir) = ctx_with_two_envs("wf-ok", HashMap::new(), RouteOrigin::HostDirect);
        let mut cfg: HashMap<String, serde_json::Value> = HashMap::new();
        cfg.insert("resource".into(), serde_json::json!("ollama"));
        cfg.insert("path".into(), serde_json::json!("/api/version"));
        cfg.insert("origin".into(), serde_json::json!("host"));
        let mut node = http_node_with(cfg);
        node.target_env = Some(EnvId::wsl("test"));

        let route = resolve_url(&node, &ctx, &CancellationToken::new())
            .await
            .expect("HostDirect route origin satisfies the gate");
        assert_eq!(route.url.as_str(), "http://127.0.0.1:11434/api/version");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn origin_host_loopback_with_verification_entry_passes() {
        let mut verifications: HashMap<ResourceId, HostDirectVerification> = HashMap::new();
        verifications.insert(
            ResourceId("ollama".into()),
            HostDirectVerification {
                verified_at: chrono::Utc::now(),
                method: HostDirectMethod::UserAssertedNoVerification,
                host_url: "http://127.0.0.1:11434".into(),
                probe_route_path: "/api/version".into(),
                stable_fingerprint: "abc".into(),
                recompute_jsonpaths: vec![],
            },
        );
        let (ctx, _dir) = ctx_with_two_envs("wf-vfy", verifications, RouteOrigin::EnvLoopback);
        let mut cfg: HashMap<String, serde_json::Value> = HashMap::new();
        cfg.insert("resource".into(), serde_json::json!("ollama"));
        cfg.insert("path".into(), serde_json::json!("/api/version"));
        cfg.insert("origin".into(), serde_json::json!("host"));
        let mut node = http_node_with(cfg);
        node.target_env = Some(EnvId::wsl("test"));

        let route = resolve_url(&node, &ctx, &CancellationToken::new())
            .await
            .expect("matching verification entry satisfies the gate");
        assert_eq!(route.url.as_str(), "http://127.0.0.1:11434/api/version");
    }
}
