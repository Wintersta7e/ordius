//! End-to-end smoke for the universal config-level template
//! substitution wired in the dispatch loop. The classic case that
//! motivated this is `http.url` referencing an upstream node's output
//! (or a workflow variable) — the v1.0 dispatcher dropped this and the
//! Phase 7.12 multistep smoke had to work around it.

use ordius_engine::Engine;
use ordius_engine::types::{Edge, EdgeType, Node, Pos, Trigger, Workflow};
use std::collections::HashMap;
use std::sync::Arc;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn transform_template_node(id: &str, template: &str) -> Node {
    let mut config = HashMap::new();
    config.insert("op".into(), serde_json::json!("template"));
    config.insert("template".into(), serde_json::json!(template));
    Node {
        id: id.into(),
        ty: "transform".into(),
        name: String::new(),
        config,
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
    }
}

fn http_node_with_templated_url(id: &str, url_template: &str) -> Node {
    let mut config = HashMap::new();
    config.insert("url".into(), serde_json::json!(url_template));
    config.insert("method".into(), serde_json::json!("GET"));
    Node {
        id: id.into(),
        ty: "http".into(),
        name: String::new(),
        config,
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
    }
}

fn forward(id: &str, from: &str, fp: &str, to: &str, tp: &str) -> Edge {
    Edge {
        id: id.into(),
        from_node_id: from.into(),
        from_port: fp.into(),
        to_node_id: to.into(),
        to_port: tp.into(),
        kind: EdgeType::Forward,
        max_iterations: None,
        branch: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_url_template_resolves_from_vars() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/hello"))
        .respond_with(ResponseTemplate::new(200).set_body_string("hi"))
        .mount(&server)
        .await;
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());

    // http.url is a workflow-variable-templated string. The dispatch
    // loop's universal substitution resolves {{vars.endpoint}}.
    let url_template = format!("{}/{{{{vars.path}}}}", server.uri());
    let wf = Workflow {
        id: "p".into(),
        name: "p".into(),
        schema_version: 1,
        created_at: None,
        updated_at: None,
        variables: HashMap::from([("path".to_string(), "hello".to_string())]),
        triggers: vec![Trigger::Manual],
        nodes: vec![http_node_with_templated_url("fetch", &url_template)],
        edges: vec![],
        resources: vec![],
    };
    let variables = HashMap::from([("path".to_string(), "hello".to_string())]);
    let summary = engine
        .run_workflow(Arc::new(wf), variables, "test", false, None)
        .await
        .expect("run");
    assert_eq!(summary.status, "done");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_url_template_resolves_from_upstream_node_output() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/from-upstream"))
        .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
        .mount(&server)
        .await;
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());

    // transform.template emits the path on its `text` port; http.url
    // references {{inputs.url}} which is the wired upstream value.
    // The dispatch loop substitutes that reference before http runs.
    let upstream_path_template = format!("{}/from-upstream", server.uri());
    let wf = Workflow {
        id: "p".into(),
        name: "p".into(),
        schema_version: 1,
        created_at: None,
        updated_at: None,
        variables: HashMap::new(),
        triggers: vec![Trigger::Manual],
        nodes: vec![
            transform_template_node("emit_url", &upstream_path_template),
            http_node_with_templated_url("fetch", "{{inputs.url}}"),
        ],
        edges: vec![forward("e1", "emit_url", "text", "fetch", "url")],
        resources: vec![],
    };
    let summary = engine
        .run_workflow(Arc::new(wf), HashMap::new(), "test", false, None)
        .await
        .expect("run");
    assert_eq!(summary.status, "done");
}
