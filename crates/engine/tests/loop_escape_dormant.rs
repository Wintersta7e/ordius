//! Regression for the B0 "dormant escape" fix: a looping condition's forward
//! escape edge must stay dormant until the loop's `max_iterations` budget is
//! exhausted, and the green ("true") exit must survive earlier "false"
//! iterations. Both runs drive the real run loop via `Engine::run_workflow`.

use ordius_engine::Engine;
use ordius_engine::types::{Edge, EdgeType, Node, Pos, Workflow};
use std::collections::HashMap;
use std::sync::Arc;

fn delay_node(id: &str) -> Node {
    Node {
        id: id.into(),
        ty: "delay".into(),
        name: String::new(),
        config: HashMap::from([("ms".into(), serde_json::json!(1))]),
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
        target_env: None,
    }
}

fn condition_bool_node(id: &str, value: bool) -> Node {
    Node {
        id: id.into(),
        ty: "condition".into(),
        name: String::new(),
        config: HashMap::from([
            ("mode".into(), serde_json::json!("boolean")),
            ("value".into(), serde_json::json!(value)),
        ]),
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
        target_env: None,
    }
}

fn fwd(id: &str, from: &str, to: &str) -> Edge {
    Edge {
        id: id.into(),
        from_node_id: from.into(),
        from_port: "out".into(),
        to_node_id: to.into(),
        to_port: "in".into(),
        kind: EdgeType::Forward,
        max_iterations: None,
        branch: None,
    }
}

fn fwd_branch(id: &str, from: &str, to: &str, branch: &str) -> Edge {
    let mut e = fwd(id, from, to);
    e.branch = Some(branch.into());
    e
}

fn loop_edge(id: &str, from: &str, to: &str, branch: &str, max: u32) -> Edge {
    let mut e = fwd(id, from, to);
    e.kind = EdgeType::Loop;
    e.branch = Some(branch.into());
    e.max_iterations = Some(max);
    e
}

fn count_runs(engine: &Engine, run_id: &str, node_id: &str) -> i64 {
    let conn = engine.pool().get().unwrap();
    conn.query_row(
        "SELECT COUNT(*) FROM node_runs WHERE run_id = ? AND node_id = ?",
        rusqlite::params![run_id, node_id],
        |r| r.get(0),
    )
    .unwrap()
}

/// Lowest `seq` of a node-start event for `node_id`, or `None` if it never
/// started. The wire tag is `node:started` (confirmed against
/// `events.rs` / `recorder_smoke.rs`).
fn first_start_seq(engine: &Engine, run_id: &str, node_id: &str) -> Option<i64> {
    let conn = engine.pool().get().unwrap();
    conn.query_row(
        "SELECT MIN(seq) FROM run_events \
         WHERE run_id = ? AND node_id = ? AND type = 'node:started'",
        rusqlite::params![run_id, node_id],
        |r| r.get::<_, Option<i64>>(0),
    )
    .unwrap()
}

/// Highest `seq` of a node-done event for `node_id`. The wire tag is
/// `node:done` (confirmed against `events.rs` / `recorder_smoke.rs`).
fn last_done_seq(engine: &Engine, run_id: &str, node_id: &str) -> i64 {
    let conn = engine.pool().get().unwrap();
    conn.query_row(
        "SELECT MAX(seq) FROM run_events \
         WHERE run_id = ? AND node_id = ? AND type = 'node:done'",
        rusqlite::params![run_id, node_id],
        |r| r.get(0),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn escape_runs_only_after_loop_budget_exhausted() {
    let home = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(home.path().to_path_buf()).await.unwrap());

    // a -> cond ; loop(cond -> a, "false", max 2) ; escape(cond -> escape, "false").
    // cond is always false: it loops twice, then the escape fires once.
    let wf = Arc::new(Workflow {
        id: "wf_loop_escape".into(),
        name: "loop escape".into(),
        schema_version: 1,
        created_at: None,
        updated_at: None,
        variables: HashMap::new(),
        triggers: vec![],
        nodes: vec![
            delay_node("a"),
            condition_bool_node("cond", false),
            delay_node("escape"),
        ],
        edges: vec![
            fwd("e1", "a", "cond"),
            loop_edge("eloop", "cond", "a", "false", 2),
            fwd_branch("e2", "cond", "escape", "false"),
        ],
        resources: vec![],
        default_env: None,
    });

    let summary = engine
        .run_workflow(wf, HashMap::new(), "test", false, None)
        .await
        .expect("loop-escape run completes");
    assert_eq!(summary.status, "done", "run summary: {summary:?}");

    // `a` runs initial + 2 loop resets = 3; escape runs exactly once.
    assert_eq!(count_runs(&engine, &summary.run_id, "a"), 3);
    assert_eq!(count_runs(&engine, &summary.run_id, "escape"), 1);

    // The load-bearing assertion: escape must START only AFTER the condition's
    // FINAL evaluation. This is causally deterministic — with the fix, escape
    // is promoted only at the terminal `try_loop`-None resolution, so its start
    // seq strictly follows the last `cond` done. With the bug, `complete_node`
    // promotes escape after the FIRST cond evaluation, so it starts before the
    // later cond evaluations and this fails. (Comparing against `a`'s last
    // start would be racy — escape and the reset `a` become Ready together.)
    let escape_start = first_start_seq(&engine, &summary.run_id, "escape").expect("escape started");
    let last_cond_done = last_done_seq(&engine, &summary.run_id, "cond");
    assert!(
        escape_start > last_cond_done,
        "escape (start seq {escape_start}) must start after the condition's \
         final evaluation (last cond done seq {last_cond_done})"
    );
}
