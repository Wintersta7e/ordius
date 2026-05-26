//! `http` built-in: in-process HTTP via a shared `reqwest::Client`.
//!
//! Failure policy is the one the spec locks in: any HTTP response
//! (including 4xx / 5xx) returns `Ok(NodeOutputs)` with the status
//! code on the `status` output port. Only network-level failures
//! (DNS, connection refused, timeout) return [`NodeError::Http`].
//! Retry-on-status is a workflow-graph concern (downstream
//! `condition` node), not an executor concern.

use super::util::{config_str, config_str_or, config_u64_or};
use crate::environment::runtime::env::{EnvId, WorkflowId};
use crate::environment::runtime::resource::{ProbeSpec, ResourceRef};
use crate::executor::{NodeError, NodeExecutor, NodeOutputs, RunContext};
use crate::types::{Node, NodeType, PortValue};
use async_trait::async_trait;
use reqwest::{Method, header::HeaderMap, header::HeaderName, header::HeaderValue};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const DEFAULT_TIMEOUT_MS: u64 = 30_000;
#[allow(unreachable_pub)]
pub const NODE_TYPE_ID: &str = "http";

/// Where an HTTP request originates from when the node has a
/// `target_env` set. `Env` (default) means the request runs from
/// inside the target env (e.g. `wsl.exe --exec curl …` for WSL).
/// `Host` forces the request to originate from the Ordius host
/// process even when `target_env != local` — useful for "talk to
/// the public Internet from my host even though this node otherwise
/// runs in WSL".
///
/// Phase D parses the field; Phase E's dispatcher selection honours
/// it. Until then, every http call goes through the host's reqwest
/// client regardless of this value.
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
        // origin: structural-only in Phase D; Phase E threads through dispatcher.
        let _origin: Origin = node
            .config
            .get("origin")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|e| NodeError::Config(format!("http: invalid origin: {e}")))?
            .unwrap_or_default();

        let url = resolve_url(node, ctx)?;
        let method_str = config_str_or(&node.config, "method", "GET");
        let method = Method::from_bytes(method_str.as_bytes())
            .map_err(|e| NodeError::Config(format!("http: invalid method '{method_str}': {e}")))?;
        let timeout_ms = config_u64_or(&node.config, "timeout_ms", DEFAULT_TIMEOUT_MS);

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

/// Resolve the node's effective URL from either:
///
/// - `url`: literal string → returned as-is.
/// - `resource` + `path`: registry-driven. The resource id is looked up via
///   the engine's [`ResourceRegistry`] in the chain
///   `Workflow → EnvLocal(local) → UserGlobal → Builtin`, the resource's
///   HTTP probe ports are used to synthesize `http://127.0.0.1:<port>`, and
///   the `path` field is concatenated on.
///
/// Mutually exclusive: setting both is rejected. `resource` without `path`
/// is rejected. Neither field surfaces the original missing-`url` error
/// from [`config_str`].
///
/// Phase D synthesizes the base URL from `ProbeSpec::Http.ports[0]` since
/// the engine has no probe-driven catalog wired into dispatch yet. Phase E
/// swaps this out for `ResourceCatalog::route_for_capability` once
/// env-scoped catalogs are kept on `Engine`.
fn resolve_url(node: &Node, ctx: &RunContext) -> Result<String, NodeError> {
    let has_resource = node.config.contains_key("resource");
    let has_url = node.config.contains_key("url");

    if has_resource && has_url {
        return Err(NodeError::Config(
            "http: 'url' and 'resource' are mutually exclusive".into(),
        ));
    }

    if has_resource {
        let rref_val = node
            .config
            .get("resource")
            .expect("has_resource just checked");
        let rref: ResourceRef = serde_json::from_value(rref_val.clone())
            .map_err(|e| NodeError::Config(format!("http: invalid resource ref: {e}")))?;
        let path = node
            .config
            .get("path")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                NodeError::Config("http: 'path' required when 'resource' is set".into())
            })?;

        let engine = ctx.engine.upgrade().ok_or_else(|| {
            NodeError::Config("internal: engine handle gone before resource lookup".into())
        })?;
        let registry = engine.resource_registry();
        let snap = registry.snapshot();
        let workflow_id = WorkflowId(ctx.workflow_id.clone());
        let (def, _scope) = snap
            .resolve(rref.id(), &EnvId::local(), Some(&workflow_id))
            .ok_or_else(|| {
                NodeError::Config(format!(
                    "http: resource '{}' not found in registry",
                    rref.id().0
                ))
            })?;

        // Phase D port-synthesis (Phase E swaps to route_for_capability).
        let ProbeSpec::Http { ports, .. } = &def.probe else {
            return Err(NodeError::Config(format!(
                "http: resource '{}' is not an HTTP endpoint",
                rref.id().0
            )));
        };
        let port = ports.first().copied().ok_or_else(|| {
            NodeError::Config(format!(
                "http: resource '{}' declares no probe ports",
                rref.id().0
            ))
        })?;
        let base_url = format!("http://127.0.0.1:{port}");
        return Ok(format!("{base_url}{path}"));
    }

    // Legacy literal form. Preserves the original missing-field error.
    Ok(config_str(&node.config, "url", "http")?.to_string())
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

    // ── resolve_url tests ───────────────────────────────────────────────────
    //
    // These cover the alt config path that names a resource by id + path
    // instead of a literal `url`. Resolution flows through the shared
    // registry seeded in Phase C; Phase E swaps the synthesizer for the
    // probed catalog's `route_for_capability`.

    use crate::Engine;
    use crate::checkpoints::CheckpointRegistry;
    use crate::db::open;
    use crate::emitter::Emitter;
    use crate::environment::runtime::install_workflow_resources;
    use crate::environment::runtime::resource::{
        ProbeSpec, ResourceDefinition, ResourceId, ResourceKind,
    };
    use crate::executor::wrap_process_env;
    use crate::recorder::RunRecorder;
    use crate::types::Workflow;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    /// Build a `RunContext` wired to a real Engine so `resolve_url` can
    /// upgrade the `Weak<Engine>` and read the live registry.
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

    #[tokio::test(flavor = "multi_thread")]
    async fn url_and_resource_mutually_exclusive() {
        let (_engine, ctx, _dir) = ctx_with_engine("wf-excl").await;
        let mut cfg: HashMap<String, serde_json::Value> = HashMap::new();
        cfg.insert("url".into(), serde_json::json!("http://example.invalid/x"));
        cfg.insert("resource".into(), serde_json::json!("anything"));
        cfg.insert("path".into(), serde_json::json!("/y"));
        let node = http_node_with(cfg);

        let err = resolve_url(&node, &ctx).expect_err("both fields set must reject");
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

        let err = resolve_url(&node, &ctx).expect_err("missing path must reject");
        let msg = format!("{err:?}");
        assert!(matches!(err, NodeError::Config(_)), "got {msg}");
        assert!(
            msg.contains("'path' required"),
            "expected path-required hint, got {msg}",
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resource_plus_path_concatenates_url() {
        let (engine, ctx, _dir) = ctx_with_engine("wf-ok").await;
        let resource = ResourceDefinition {
            id: ResourceId("wf-api".into()),
            kind: ResourceKind::HttpEndpoint,
            // Empty advertised caps = untyped; Phase D allows.
            advertised_capabilities: vec![],
            probe: ProbeSpec::Http {
                ports: vec![7777],
                routes: vec![],
                timeout_ms: None,
            },
            override_lower_scope: false,
        };
        install_workflow_resources(
            &WorkflowId("wf-ok".into()),
            &[resource],
            &engine.resource_registry(),
        )
        .expect("install");

        let mut cfg: HashMap<String, serde_json::Value> = HashMap::new();
        cfg.insert("resource".into(), serde_json::json!("wf-api"));
        cfg.insert("path".into(), serde_json::json!("/v1/ping"));
        let node = http_node_with(cfg);

        let url = resolve_url(&node, &ctx).expect("resource + path resolves");
        assert_eq!(url, "http://127.0.0.1:7777/v1/ping");
    }
}
