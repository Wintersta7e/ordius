//! End-to-end smoke for the >8 KB output file-back fallback.
//!
//! A `transform.template` node renders a string larger than the spill
//! threshold. After the run finishes, the `node_outputs` row for the
//! `text` port should have NULL `value_inline` and a non-NULL
//! `value_path` pointing at a file under `<home>/output-cache/<run>/`.

use ordius_engine::Engine;
use ordius_engine::types::{Node, Pos, Trigger, Workflow};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_string_output_spills_to_disk() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());

    // Render a string above the 8 KB spill threshold.
    let big = "x".repeat(16 * 1024);
    let wf = Workflow {
        id: "p".into(),
        name: "p".into(),
        schema_version: 1,
        created_at: None,
        updated_at: None,
        variables: HashMap::new(),
        triggers: vec![Trigger::Manual],
        nodes: vec![transform_template_node("emit_big", &big)],
        edges: vec![],
    };
    let summary = engine
        .run_workflow(Arc::new(wf), HashMap::new(), "test", false, None)
        .await
        .expect("run");
    assert_eq!(summary.status, "done");

    let conn = engine.pool().get().unwrap();
    let (inline, path): (Option<String>, Option<String>) = conn
        .prepare(
            "SELECT value_inline, value_path FROM node_outputs \
             WHERE node_id = 'emit_big' AND port_name = 'text'",
        )
        .unwrap()
        .query_row([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap();
    assert!(inline.is_none(), "large output should NOT be inlined");
    let path = path.expect("large output should have value_path set");
    assert!(
        path.contains("output-cache"),
        "path should be under output-cache: {path}",
    );
    let spilled = std::fs::read_to_string(&path).expect("spill file readable");
    // The serialized value is a JSON string, so the rendered template
    // appears wrapped in quotes — `"xxx..."`.
    assert!(
        spilled.len() > 16 * 1024,
        "spill content carries the rendered string"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_output_stays_inline() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());

    let small = "hello";
    let wf = Workflow {
        id: "p".into(),
        name: "p".into(),
        schema_version: 1,
        created_at: None,
        updated_at: None,
        variables: HashMap::new(),
        triggers: vec![Trigger::Manual],
        nodes: vec![transform_template_node("emit_small", small)],
        edges: vec![],
    };
    let summary = engine
        .run_workflow(Arc::new(wf), HashMap::new(), "test", false, None)
        .await
        .expect("run");
    assert_eq!(summary.status, "done");

    let conn = engine.pool().get().unwrap();
    let (inline, path): (Option<String>, Option<String>) = conn
        .prepare(
            "SELECT value_inline, value_path FROM node_outputs \
             WHERE node_id = 'emit_small' AND port_name = 'text'",
        )
        .unwrap()
        .query_row([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap();
    assert!(path.is_none(), "small output should NOT spill");
    let inline = inline.expect("small output should be inline");
    assert!(inline.contains("hello"));
}
