use super::*;
use crate::db::open;
use crate::emitter::Emitter;
use crate::recorder::RunRecorder;
use crate::types::{Category, ExecutionBackend, ExecutionSpec, OutputParse, Pos, Workflow};
use std::collections::HashMap;
use std::sync::Arc;
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

fn transform_node_type() -> NodeType {
    NodeType {
        id: "transform".into(),
        name: String::new(),
        category: Category::Data,
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

fn transform_node(cfg: &serde_json::Value) -> Node {
    let config = cfg
        .as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    Node {
        id: "n".into(),
        ty: "transform".into(),
        name: String::new(),
        config,
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
    }
}

async fn run_transform(cfg: &serde_json::Value) -> Result<String, NodeError> {
    let (ctx, _dir) = make_ctx();
    let n = transform_node(cfg);
    let outs = TransformExecutor
        .run(&n, &transform_node_type(), &ctx, CancellationToken::new())
        .await?;
    match outs.get("text").unwrap() {
        PortValue::String(s) => Ok(s.clone()),
        other => panic!("expected String, got {other:?}"),
    }
}

#[tokio::test]
async fn jsonpath_extracts_matched_array() {
    let s = run_transform(&serde_json::json!({
        "op": "jsonpath",
        "input": r#"{"a":[1,2,3]}"#,
        "expr": "$.a[1]",
    }))
    .await
    .unwrap();
    assert_eq!(s, "[2]");
}

#[tokio::test]
async fn regex_extract_finds_first_match() {
    let s = run_transform(&serde_json::json!({
        "op": "regex_extract",
        "input": "err42 ok7",
        "pattern": r"\d+",
    }))
    .await
    .unwrap();
    assert_eq!(s, "42");
}

#[tokio::test]
async fn regex_replace_substitutes_all() {
    let s = run_transform(&serde_json::json!({
        "op": "regex_replace",
        "input": "foo bar foo",
        "pattern": "foo",
        "replacement": "baz",
    }))
    .await
    .unwrap();
    assert_eq!(s, "baz bar baz");
}

#[tokio::test]
async fn unknown_op_is_config_error() {
    let res = run_transform(&serde_json::json!({"op": "wat"})).await;
    assert!(matches!(res, Err(NodeError::Config(_))));
}

#[tokio::test]
async fn missing_op_is_config_error() {
    let res = run_transform(&serde_json::json!({})).await;
    assert!(matches!(res, Err(NodeError::Config(_))));
}
