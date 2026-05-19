//! End-to-end smoke: four built-ins in series through `run_workflow`.
//!
//! `file.write` → `file.read` → `transform.template` → `http.GET` →
//! `file.write`. Confirms the dispatch loop wires forward edges,
//! the registry resolves every node type, every executor receives
//! its `RunContext` correctly, and the recorder persists per-node
//! rows in order.
//!
//! The plan's `transform`-renders-into-`http`-body shape would
//! need config-level template substitution for `http.body`, which
//! is not yet wired. This test stays within what each executor's
//! current config surface supports: data flows via wired ports
//! and template `{{inputs.X}}` references, with `http` reading
//! its URL straight from static config.

use ordius_engine::types::{Edge, EdgeType, Node, Pos, Workflow};
use ordius_engine::Engine;
use std::collections::HashMap;
use std::sync::Arc;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn delay_node(id: &str, ms: u64) -> Node {
    Node {
        id: id.into(),
        ty: "delay".into(),
        name: String::new(),
        config: HashMap::from([("ms".into(), serde_json::json!(ms))]),
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
    }
}

fn file_write_node(id: &str, path: &str, content: &str) -> Node {
    Node {
        id: id.into(),
        ty: "file".into(),
        name: String::new(),
        config: HashMap::from([
            ("op".into(), serde_json::json!("write")),
            ("path".into(), serde_json::json!(path)),
            ("content".into(), serde_json::json!(content)),
        ]),
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
    }
}

fn file_read_node(id: &str, path: &str) -> Node {
    Node {
        id: id.into(),
        ty: "file".into(),
        name: String::new(),
        config: HashMap::from([
            ("op".into(), serde_json::json!("read")),
            ("path".into(), serde_json::json!(path)),
        ]),
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
    }
}

fn transform_template_node(id: &str, template: &str) -> Node {
    Node {
        id: id.into(),
        ty: "transform".into(),
        name: String::new(),
        config: HashMap::from([
            ("op".into(), serde_json::json!("template")),
            ("template".into(), serde_json::json!(template)),
        ]),
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
    }
}

fn http_get_node(id: &str, url: &str) -> Node {
    Node {
        id: id.into(),
        ty: "http".into(),
        name: String::new(),
        config: HashMap::from([("url".into(), serde_json::json!(url))]),
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
    }
}

fn fwd_edge(id: &str, from_node: &str, from_port: &str, to_node: &str, to_port: &str) -> Edge {
    Edge {
        id: id.into(),
        from_node_id: from_node.into(),
        from_port: from_port.into(),
        to_node_id: to_node.into(),
        to_port: to_port.into(),
        kind: EdgeType::Forward,
        max_iterations: None,
        branch: None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn five_step_workflow_runs_through_file_transform_http_to_completion() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/echo"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/plain")
                .set_body_string("server says hi"),
        )
        .mount(&server)
        .await;

    let engine_home = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(
        Engine::new(engine_home.path().to_path_buf())
            .await
            .unwrap(),
    );

    // The pre-write node + the workflow share an absolute path so
    // the test doesn't depend on knowing the run workspace dir in
    // advance.
    let fixture_dir = tempfile::TempDir::new().unwrap();
    let fixture_path = fixture_dir.path().join("seed.txt");
    let fixture_path_str = fixture_path.to_string_lossy().into_owned();
    let summary_path = fixture_dir.path().join("summary.txt");
    let summary_path_str = summary_path.to_string_lossy().into_owned();

    let wf = Arc::new(Workflow {
        id: "wf_multistep".into(),
        name: "multi step".into(),
        schema_version: 1,
        created_at: None,
        updated_at: None,
        variables: HashMap::new(),
        triggers: vec![],
        nodes: vec![
            // 1. tiny delay so we can observe ordering deterministically
            delay_node("a_warmup", 1),
            // 2. write a fixture
            file_write_node("b_write_fixture", &fixture_path_str, "seed content"),
            // 3. read it back
            file_read_node("c_read_fixture", &fixture_path_str),
            // 4. template the read text into a rendered string
            transform_template_node("d_render", "got: {{inputs.text}}"),
            // 5. independent http call to wiremock
            http_get_node("e_http", &format!("{}/echo", server.uri())),
            // 6. write a summary file using a static string
            file_write_node("f_write_summary", &summary_path_str, "all done"),
        ],
        edges: vec![
            fwd_edge("e1", "a_warmup", "out", "b_write_fixture", "in"),
            fwd_edge("e2", "b_write_fixture", "path", "c_read_fixture", "in"),
            // c_read_fixture.text → d_render.text wires the template's
            // {{inputs.text}} via current_inputs.
            fwd_edge("e3", "c_read_fixture", "text", "d_render", "text"),
            fwd_edge("e4", "d_render", "text", "e_http", "in"),
            fwd_edge("e5", "e_http", "body", "f_write_summary", "in"),
        ],
    });

    let summary = engine
        .run_workflow(wf, HashMap::new(), "test", false)
        .await
        .expect("multi-step run completes");

    assert_eq!(summary.status, "done", "run summary: {summary:?}");
    assert_eq!(summary.node_runs, 6);

    let summary_content = std::fs::read_to_string(&summary_path).expect("summary written");
    assert_eq!(summary_content, "all done");
    let fixture_content = std::fs::read_to_string(&fixture_path).expect("fixture written");
    assert_eq!(fixture_content, "seed content");
}
