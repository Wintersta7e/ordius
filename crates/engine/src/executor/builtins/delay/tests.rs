use super::*;
use crate::db::open;
use crate::emitter::Emitter;
use crate::recorder::RunRecorder;
use crate::types::{Category, ExecutionBackend, ExecutionSpec, OutputParse, Pos, Workflow};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;

fn make_ctx() -> (RunContext, TempDir) {
    let dir = TempDir::new().unwrap();
    let pool = open(dir.path().join("t.db")).unwrap();
    let wf = Workflow {
        id: "w".into(),
        name: String::new(),
        schema_version: 1,
        created_at: None,
        updated_at: None,
        variables: HashMap::new(),
        triggers: vec![],
        nodes: vec![],
        edges: vec![],
    };
    let rec = Arc::new(RunRecorder::start(pool, &wf, "{}", &HashMap::new(), "test").unwrap());
    let (em, _rx) = Emitter::new(rec.clone());
    let ctx = RunContext {
        run_id: rec.run_id.clone(),
        workflow_id: "w".into(),
        workspace: dir.path().to_path_buf(),
        variables: HashMap::new(),
        recorder: rec,
        emitter: Arc::new(em),
        current_inputs: HashMap::new(),
        upstream_outputs: HashMap::new(),
    };
    (ctx, dir)
}

fn delay_node_type() -> NodeType {
    NodeType {
        id: "delay".into(),
        name: String::new(),
        category: Category::Control,
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
    }
}

fn delay_node(ms: u64) -> Node {
    Node {
        id: "n1".into(),
        ty: "delay".into(),
        name: String::new(),
        config: HashMap::from([("ms".into(), serde_json::json!(ms))]),
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
    }
}

#[tokio::test]
async fn delay_sleeps_at_least_n_ms() {
    let (ctx, _dir) = make_ctx();
    let node = delay_node(80);
    let start = Instant::now();
    DelayExecutor
        .run(&node, &delay_node_type(), &ctx, CancellationToken::new())
        .await
        .unwrap();
    assert!(start.elapsed() >= Duration::from_millis(75));
}

#[tokio::test]
async fn delay_cancels_promptly() {
    let (ctx, _dir) = make_ctx();
    let node = delay_node(60_000);
    let cancel = CancellationToken::new();
    let cancel_child = cancel.clone();
    let nt = delay_node_type();
    let handle =
        tokio::spawn(async move { DelayExecutor.run(&node, &nt, &ctx, cancel_child).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    cancel.cancel();
    let start_wait = Instant::now();
    let res = handle.await.unwrap();
    assert!(start_wait.elapsed() < Duration::from_millis(500));
    assert!(matches!(res, Err(NodeError::Cancelled)));
}

#[tokio::test]
async fn delay_rejects_missing_ms_config() {
    let (ctx, _dir) = make_ctx();
    let node = Node {
        id: "n".into(),
        ty: "delay".into(),
        name: String::new(),
        config: HashMap::new(),
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
    };
    let res = DelayExecutor
        .run(&node, &delay_node_type(), &ctx, CancellationToken::new())
        .await;
    assert!(matches!(res, Err(NodeError::Config(_))));
}
