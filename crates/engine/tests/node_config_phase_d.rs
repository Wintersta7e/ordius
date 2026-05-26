//! End-to-end smoke for Phase D's node-config refactor.
//!
//! Covers the three new alt config paths that name a resource by id
//! instead of a literal URL — `llm.resource`, `http.resource + path`,
//! and the shell template `{{resource.<id>.base_url}}` — plus the
//! reserved-id rejection hints for retired node types (`agent` → `llm`,
//! `container` → `docker-run`) and the loopback-URL-in-remote-env lint.
//!
//! Tests 1-3 build the workflow as in-Rust `Workflow` values and install
//! the workflow-scope resources via `install_workflow_resources` before
//! `run_workflow`. Tests 4-6 round-trip through the JSON-file load path
//! (`workflows::load_in_registry`) so the file-level validation surface
//! is exercised end-to-end alongside the unit-level coverage in
//! `workflows::tests`.

use ordius_engine::Engine;
use ordius_engine::environment::runtime::{
    ApiFlavor, Capability, HttpProbeMethod, HttpProbeRoute, ProbeSpec, ResourceDefinition,
    ResourceId, ResourceKind, WorkflowId, install_workflow_resources,
};
use ordius_engine::types::{Node, Pos, Trigger, Workflow};
use ordius_engine::workflows::{WorkflowWarningKind, WorkflowsError};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ── helpers ────────────────────────────────────────────────────────────────

fn workflow_with_resource(
    id: &str,
    nodes: Vec<Node>,
    resources: Vec<ResourceDefinition>,
) -> Workflow {
    Workflow {
        id: id.into(),
        name: id.into(),
        schema_version: 1,
        created_at: None,
        updated_at: None,
        variables: HashMap::new(),
        triggers: vec![Trigger::Manual],
        nodes,
        edges: vec![],
        resources,
        default_env: None,
    }
}

fn http_resource(id: &str, port: u16, caps: Vec<Capability>) -> ResourceDefinition {
    ResourceDefinition {
        id: ResourceId(id.into()),
        kind: ResourceKind::HttpEndpoint,
        advertised_capabilities: caps,
        probe: ProbeSpec::Http {
            ports: vec![port],
            routes: vec![HttpProbeRoute {
                path: "/".into(),
                method: HttpProbeMethod::Get,
                flavor: ApiFlavor::OpenaiChat,
                proves: vec![],
                models_jsonpath: None,
                fingerprint_jsonpaths: vec![],
            }],
            timeout_ms: None,
        },
        override_lower_scope: false,
    }
}

fn mock_port(server: &MockServer) -> u16 {
    server
        .uri()
        .rsplit(':')
        .next()
        .and_then(|s| s.parse::<u16>().ok())
        .expect("MockServer URI must end in a port")
}

// ── test 1: llm.resource short form drives dispatch ────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn llm_resource_form_dispatches_via_registry() {
    let llm = MockServer::start().await;
    // The resource below declares an OpenAI-flavored probe route at
    // `/v1/models`, which lifts `/v1` onto the base URL — so the final
    // request lands on `/v1/chat/completions`, matching the layout of
    // every real OpenAI-compat server (Ollama-compat, llama.cpp, vLLM,
    // LM Studio).
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "choices": [{
                "message": {"role": "assistant", "content": "ok"},
                "finish_reason": "stop",
            }],
            "usage": {"total_tokens": 4},
        })))
        .expect(1)
        .mount(&llm)
        .await;

    let dir = TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());

    let port = mock_port(&llm);
    let resource = ResourceDefinition {
        id: ResourceId("wf-llm".into()),
        kind: ResourceKind::HttpEndpoint,
        advertised_capabilities: vec![Capability::OpenaiChatCompletions],
        probe: ProbeSpec::Http {
            ports: vec![port],
            routes: vec![HttpProbeRoute {
                path: "/v1/models".into(),
                method: HttpProbeMethod::Get,
                flavor: ApiFlavor::OpenaiChat,
                proves: vec![Capability::OpenaiChatCompletions],
                models_jsonpath: None,
                fingerprint_jsonpaths: vec![],
            }],
            timeout_ms: None,
        },
        override_lower_scope: false,
    };

    let mut cfg: HashMap<String, serde_json::Value> = HashMap::new();
    cfg.insert("resource".into(), serde_json::json!("wf-llm"));
    cfg.insert("model".into(), serde_json::json!("test-model"));
    cfg.insert(
        "messages".into(),
        serde_json::json!([{"role": "user", "content": "hi"}]),
    );
    // Pin off so we exercise the JSON-body (non-streaming) path.
    cfg.insert("stream".into(), serde_json::json!("off"));
    let node = Node {
        id: "n".into(),
        ty: "llm".into(),
        name: String::new(),
        config: cfg,
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
        target_env: None,
    };

    let wf = workflow_with_resource("wf-llm-res", vec![node], vec![resource]);
    install_workflow_resources(
        &WorkflowId(wf.id.clone()),
        &wf.resources,
        &engine.resource_registry(),
    )
    .expect("install workflow scope");

    let summary = engine
        .run_workflow(Arc::new(wf), HashMap::new(), "test", false, None)
        .await
        .expect("run");
    assert_eq!(summary.status, "done");
    // `Mock::expect(1)` causes the MockServer to verify on drop that
    // exactly one request hit /chat/completions.
}

// ── test 2: http.resource + path concatenates url ──────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_resource_path_form_concatenates_url() {
    let api = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/ping"))
        .respond_with(ResponseTemplate::new(200).set_body_string("pong"))
        .expect(1)
        .mount(&api)
        .await;

    let dir = TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());

    let port = mock_port(&api);
    // No caps → "untyped"; the http executor allows it.
    let resource = http_resource("wf-api", port, vec![]);

    let mut cfg: HashMap<String, serde_json::Value> = HashMap::new();
    cfg.insert("resource".into(), serde_json::json!({"id": "wf-api"}));
    cfg.insert("path".into(), serde_json::json!("/v1/ping"));
    let node = Node {
        id: "n".into(),
        ty: "http".into(),
        name: String::new(),
        config: cfg,
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
        target_env: None,
    };

    let wf = workflow_with_resource("wf-http-res", vec![node], vec![resource]);
    install_workflow_resources(
        &WorkflowId(wf.id.clone()),
        &wf.resources,
        &engine.resource_registry(),
    )
    .expect("install workflow scope");

    let summary = engine
        .run_workflow(Arc::new(wf), HashMap::new(), "test", false, None)
        .await
        .expect("run");
    assert_eq!(summary.status, "done");
}

// ── test 3: shell `{{resource.<id>.base_url}}` template substitutes ────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shell_template_substitutes_resource_base_url() {
    let api = MockServer::start().await;
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());

    let port = mock_port(&api);
    let expected_base = format!("http://127.0.0.1:{port}");
    let resource = http_resource("wf-api", port, vec![]);

    // `shell` keeps its own template pass; the universal dispatch pass
    // is skipped. The per-spawn substitution wires the same resources
    // resolver, so `{{resource.<id>.base_url}}` resolves to the same
    // `http://127.0.0.1:<port>` the http/llm builtins synthesise.
    let mut cfg: HashMap<String, serde_json::Value> = HashMap::new();
    cfg.insert(
        "command".into(),
        serde_json::json!("echo {{resource.wf-api.base_url}}"),
    );
    let node = Node {
        id: "echo".into(),
        ty: "shell".into(),
        name: String::new(),
        config: cfg,
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
        target_env: None,
    };

    let wf = workflow_with_resource("wf-shell-res", vec![node], vec![resource]);
    install_workflow_resources(
        &WorkflowId(wf.id.clone()),
        &wf.resources,
        &engine.resource_registry(),
    )
    .expect("install workflow scope");

    let summary = engine
        .run_workflow(Arc::new(wf), HashMap::new(), "test", false, None)
        .await
        .expect("run");
    assert_eq!(summary.status, "done");

    // Verify the recorded stdout actually carries the synthesized base
    // URL — proves the template was substituted by the per-spawn pass
    // (not silently passed through as a literal).
    let conn = engine.pool().get().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT payload_json FROM run_events \
             WHERE run_id = ? AND type = 'node:output' AND channel = 'stdout' \
             ORDER BY seq",
        )
        .unwrap();
    let payloads: Vec<String> = stmt
        .query_map([&summary.run_id], |r| r.get::<_, String>(0))
        .unwrap()
        .filter_map(Result::ok)
        .collect();
    assert!(
        !payloads.is_empty(),
        "shell node should have emitted at least one stdout line, got none",
    );
    let joined = payloads.join("\n");
    assert!(
        joined.contains(&expected_base),
        "expected stdout to contain {expected_base}, got {joined}",
    );
}

// ── test 4: reserved `agent` → llm rename hint ─────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn workflow_with_agent_type_is_rejected_with_hint() {
    let dir = TempDir::new().unwrap();
    let wf_dir = dir.path().join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(
        wf_dir.join("retired-agent.json"),
        r#"{
            "id": "retired-agent",
            "name": "uses retired agent",
            "nodes": [
                {"id": "n1", "type": "agent", "name": "ask", "config": {}}
            ],
            "edges": []
        }"#,
    )
    .unwrap();

    let engine = Engine::new(dir.path().to_path_buf()).await.expect("new");
    let registry = engine.resource_registry();

    let err = ordius_engine::workflows::load_in_registry(dir.path(), "retired-agent", &registry)
        .expect_err("agent type must be rejected");
    match err {
        WorkflowsError::ReservedNodeType {
            id,
            replacement,
            node_id,
        } => {
            assert_eq!(id, "agent");
            assert_eq!(replacement, "llm");
            assert_eq!(node_id, "n1");
        },
        other => panic!("expected ReservedNodeType, got {other:?}"),
    }
}

// ── test 5: reserved `container` → docker-run rename hint ──────────────────

#[tokio::test(flavor = "multi_thread")]
async fn workflow_with_container_type_is_rejected_with_hint() {
    let dir = TempDir::new().unwrap();
    let wf_dir = dir.path().join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(
        wf_dir.join("retired-container.json"),
        r#"{
            "id": "retired-container",
            "name": "uses retired container",
            "nodes": [
                {"id": "c1", "type": "container", "name": "boxed", "config": {}}
            ],
            "edges": []
        }"#,
    )
    .unwrap();

    let engine = Engine::new(dir.path().to_path_buf()).await.expect("new");
    let registry = engine.resource_registry();

    let err =
        ordius_engine::workflows::load_in_registry(dir.path(), "retired-container", &registry)
            .expect_err("container type must be rejected");
    match err {
        WorkflowsError::ReservedNodeType {
            id,
            replacement,
            node_id,
        } => {
            assert_eq!(id, "container");
            assert_eq!(replacement, "docker-run");
            assert_eq!(node_id, "c1");
        },
        other => panic!("expected ReservedNodeType, got {other:?}"),
    }
}

// ── test 6: loopback url + remote env warning ──────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn loopback_url_with_remote_target_env_warns() {
    let dir = TempDir::new().unwrap();
    let wf_dir = dir.path().join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(
        wf_dir.join("looped.json"),
        r#"{
            "id": "looped",
            "name": "loopback in remote env",
            "nodes": [
                {
                    "id": "fetch",
                    "type": "http",
                    "name": "fetch",
                    "config": {
                        "url": "http://127.0.0.1:11434/api/version",
                        "method": "GET"
                    },
                    "target_env": "wsl:Ubuntu"
                }
            ],
            "edges": []
        }"#,
    )
    .unwrap();

    let engine = Engine::new(dir.path().to_path_buf()).await.expect("new");
    let registry = engine.resource_registry();

    let (_wf, warnings) =
        ordius_engine::workflows::load_in_registry(dir.path(), "looped", &registry)
            .expect("load with loopback warning, not error");
    assert_eq!(
        warnings.len(),
        1,
        "expected exactly one warning, got {warnings:?}",
    );
    let w = &warnings[0];
    assert_eq!(w.node_id, "fetch");
    assert_eq!(w.kind, WorkflowWarningKind::LoopbackUrlInRemoteEnv);
}
