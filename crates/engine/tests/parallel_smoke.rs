//! End-to-end smoke: `parallel` fans out a child workflow over an
//! `items` array and joins the per-child outputs.

use ordius_engine::Engine;
use ordius_engine::types::{Node, Pos, Trigger, Workflow};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

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

fn parallel_node(id: &str, child_workflow_id: &str, items: serde_json::Value) -> Node {
    let mut config = HashMap::new();
    config.insert("workflow_id".into(), serde_json::json!(child_workflow_id));
    config.insert("items".into(), items);
    config.insert("max_concurrent".into(), serde_json::json!(2));
    Node {
        id: id.into(),
        ty: "parallel".into(),
        name: String::new(),
        config,
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
    }
}

fn workflow(id: &str, nodes: Vec<Node>) -> Workflow {
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
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_fans_out_over_items_and_records_child_runs() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());

    // Child does a tiny delay; one run per parallel item.
    let child = workflow("worker", vec![delay_node("step", 5)]);
    ordius_engine::workflows::save(engine.home(), &child).unwrap();

    let items = serde_json::json!(["a", "b", "c", "d", "e"]);
    let parent = workflow(
        "parent",
        vec![parallel_node("fan", "worker", items.clone())],
    );
    let summary = engine
        .run_workflow(Arc::new(parent), HashMap::new(), "test", false, None)
        .await
        .expect("run");
    assert_eq!(summary.status, "done");

    let conn = engine.pool().get().unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM runs WHERE trigger_kind = 'compose'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 5, "one child run per parallel item");
}

#[tokio::test(flavor = "multi_thread")]
async fn parallel_empty_items_is_a_noop() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());

    let child = workflow("worker", vec![delay_node("step", 1)]);
    ordius_engine::workflows::save(engine.home(), &child).unwrap();

    let parent = workflow(
        "parent",
        vec![parallel_node("fan", "worker", serde_json::json!([]))],
    );
    let summary = engine
        .run_workflow(Arc::new(parent), HashMap::new(), "test", false, None)
        .await
        .expect("run");
    assert_eq!(summary.status, "done");
}
