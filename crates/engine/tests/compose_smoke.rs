//! End-to-end smoke: `compose` invokes a saved child workflow and
//! surfaces its outputs.
//!
//! Sets up two workflow files on disk:
//! - `child`: a single `delay` node named `return` that emits no
//!   ports but completes successfully.
//! - `parent`: a `compose` node referencing `child` by id.
//!
//! Confirms the parent run completes status='done' and the child's
//! own `runs` row was created.

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

fn compose_node(id: &str, child_workflow_id: &str) -> Node {
    Node {
        id: id.into(),
        ty: "compose".into(),
        name: String::new(),
        config: HashMap::from([("workflow_id".into(), serde_json::json!(child_workflow_id))]),
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

#[tokio::test(flavor = "multi_thread")]
async fn compose_runs_child_workflow_and_records_separate_run() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());

    // Persist the child workflow first — compose loads it by id at run time.
    let child = workflow("child", vec![delay_node("return", 5)]);
    ordius_engine::workflows::save(engine.home(), &child).unwrap();

    // Parent invokes the child via the compose node.
    let parent = workflow("parent", vec![compose_node("invoke", "child")]);
    let summary = engine
        .run_workflow(Arc::new(parent), HashMap::new(), "test", false, None)
        .await
        .expect("parent run completes");
    assert_eq!(summary.status, "done", "parent should finish ok");

    // Both runs should land in the `runs` table — parent (trigger=test)
    // and child (trigger=compose).
    let conn = engine.pool().get().unwrap();
    let trigger_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM runs WHERE trigger_kind = 'compose'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(trigger_count, 1, "exactly one compose-trigger child run");
}

#[tokio::test(flavor = "multi_thread")]
async fn compose_rejects_missing_workflow() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());

    let parent = workflow("parent", vec![compose_node("invoke", "does-not-exist")]);
    let summary = engine
        .run_workflow(Arc::new(parent), HashMap::new(), "test", false, None)
        .await
        .expect("run returns summary even on child miss");
    // Compose node fails → unhandled failure → parent status is "error".
    assert_eq!(summary.status, "error");
}
