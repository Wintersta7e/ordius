use super::*;
use crate::executor::test_support::{dummy_node_type, make_ctx};
use crate::types::{Category, Pos};

fn condition_node_type() -> NodeType {
    dummy_node_type("condition", Category::Control)
}

fn condition_node(cfg: &serde_json::Value) -> Node {
    Node {
        id: "n".into(),
        ty: "condition".into(),
        name: String::new(),
        config: cfg
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
    }
}

async fn run_condition(cfg: &serde_json::Value) -> Result<String, NodeError> {
    let (ctx, _rx, _dir) = make_ctx();
    let n = condition_node(cfg);
    let outs = ConditionExecutor
        .run(&n, &condition_node_type(), &ctx, CancellationToken::new())
        .await?;
    match outs.get("branch").unwrap() {
        PortValue::String(s) => Ok(s.clone()),
        other => panic!("expected String, got {other:?}"),
    }
}

#[tokio::test]
async fn boolean_true_emits_true_branch() {
    let s = run_condition(&serde_json::json!({"mode":"boolean","value":true}))
        .await
        .unwrap();
    assert_eq!(s, "true");
}

#[tokio::test]
async fn boolean_false_emits_false_branch() {
    let s = run_condition(&serde_json::json!({"mode":"boolean","value":false}))
        .await
        .unwrap();
    assert_eq!(s, "false");
}

#[tokio::test]
async fn exit_code_zero_is_true() {
    let s = run_condition(&serde_json::json!({"mode":"exit_code","exit_code":0}))
        .await
        .unwrap();
    assert_eq!(s, "true");
}

#[tokio::test]
async fn exit_code_nonzero_is_false() {
    let s = run_condition(&serde_json::json!({"mode":"exit_code","exit_code":1}))
        .await
        .unwrap();
    assert_eq!(s, "false");
}

#[tokio::test]
async fn regex_match_is_true() {
    let s = run_condition(&serde_json::json!({
        "mode":"regex","input":"oranges","pattern":"o.a"
    }))
    .await
    .unwrap();
    assert_eq!(s, "true");
}

#[tokio::test]
async fn regex_no_match_is_false() {
    let s = run_condition(&serde_json::json!({
        "mode":"regex","input":"hello","pattern":"^xyz"
    }))
    .await
    .unwrap();
    assert_eq!(s, "false");
}

#[tokio::test]
async fn jsonpath_present_truthy_value_is_true() {
    let s = run_condition(&serde_json::json!({
        "mode":"jsonpath","input": r#"{"flag":true}"#,"expr":"$.flag"
    }))
    .await
    .unwrap();
    assert_eq!(s, "true");
}

#[tokio::test]
async fn jsonpath_all_false_values_is_false() {
    let s = run_condition(&serde_json::json!({
        "mode":"jsonpath","input": r#"{"flag":false}"#,"expr":"$.flag"
    }))
    .await
    .unwrap();
    assert_eq!(s, "false");
}

#[tokio::test]
async fn jsonpath_missing_path_is_false() {
    let s = run_condition(&serde_json::json!({
        "mode":"jsonpath","input": r#"{"a":1}"#,"expr":"$.missing"
    }))
    .await
    .unwrap();
    assert_eq!(s, "false");
}

#[tokio::test]
async fn unknown_mode_is_config_error() {
    let res = run_condition(&serde_json::json!({"mode":"nope"})).await;
    assert!(matches!(res, Err(NodeError::Config(_))));
}

#[tokio::test]
async fn compare_eq_numbers() {
    let s = run_condition(&serde_json::json!({
        "mode":"compare","op":"eq","left":1,"right":1
    }))
    .await
    .unwrap();
    assert_eq!(s, "true");
}

#[tokio::test]
async fn compare_neq_strings() {
    let s = run_condition(&serde_json::json!({
        "mode":"compare","op":"neq","left":"hello","right":"world"
    }))
    .await
    .unwrap();
    assert_eq!(s, "true");
}

#[tokio::test]
async fn compare_lt_numbers() {
    let s = run_condition(&serde_json::json!({
        "mode":"compare","op":"lt","left":2,"right":5
    }))
    .await
    .unwrap();
    assert_eq!(s, "true");
}

#[tokio::test]
async fn compare_ge_strings() {
    let s = run_condition(&serde_json::json!({
        "mode":"compare","op":"ge","left":"b","right":"a"
    }))
    .await
    .unwrap();
    assert_eq!(s, "true");
}

#[tokio::test]
async fn compare_contains() {
    let s = run_condition(&serde_json::json!({
        "mode":"compare","op":"contains","left":"hello world","right":"orl"
    }))
    .await
    .unwrap();
    assert_eq!(s, "true");
}

#[tokio::test]
async fn compare_starts_with() {
    let s = run_condition(&serde_json::json!({
        "mode":"compare","op":"starts_with","left":"hello","right":"hel"
    }))
    .await
    .unwrap();
    assert_eq!(s, "true");
}

#[tokio::test]
async fn compare_ends_with_false() {
    let s = run_condition(&serde_json::json!({
        "mode":"compare","op":"ends_with","left":"hello","right":"foo"
    }))
    .await
    .unwrap();
    assert_eq!(s, "false");
}

#[tokio::test]
async fn compare_mixed_types_lt_is_config_error() {
    let res = run_condition(&serde_json::json!({
        "mode":"compare","op":"lt","left":"a","right":1
    }))
    .await;
    assert!(matches!(res, Err(NodeError::Config(_))));
}

#[tokio::test]
async fn compare_unknown_op_is_config_error() {
    let res = run_condition(&serde_json::json!({
        "mode":"compare","op":"weird","left":1,"right":1
    }))
    .await;
    assert!(matches!(res, Err(NodeError::Config(_))));
}
