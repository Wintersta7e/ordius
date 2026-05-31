use super::*;
use crate::executor::test_support::{dummy_node_type, make_ctx};
use crate::types::{Category, Pos};
use std::collections::HashMap;
use std::time::Instant;

fn delay_node_type() -> NodeType {
    dummy_node_type("delay", Category::Control)
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
        target_env: None,
    }
}

#[tokio::test]
async fn delay_sleeps_at_least_n_ms() {
    let (ctx, _rx, _dir) = make_ctx();
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
    let (ctx, _rx, _dir) = make_ctx();
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
    // Generous bound: the point is it returns long before the 60s delay,
    // not a tight latency SLA — a 500ms bound flakes on loaded CI runners.
    assert!(start_wait.elapsed() < Duration::from_secs(5));
    assert!(matches!(res, Err(NodeError::Cancelled)));
}

#[tokio::test]
async fn delay_rejects_missing_ms_config() {
    let (ctx, _rx, _dir) = make_ctx();
    let node = Node {
        id: "n".into(),
        ty: "delay".into(),
        name: String::new(),
        config: HashMap::new(),
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
        target_env: None,
    };
    let res = DelayExecutor
        .run(&node, &delay_node_type(), &ctx, CancellationToken::new())
        .await;
    assert!(matches!(res, Err(NodeError::Config(_))));
}
