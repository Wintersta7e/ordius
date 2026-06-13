//! `loop_for` must drive a real loop: with count = N the body runs N times and
//! the "exit" edge fires exactly once, after the final iteration. Before B2,
//! `loop_for` emitted its branch into the void and never looped.

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

fn loop_for_node(id: &str, count: u64) -> Node {
    Node {
        id: id.into(),
        ty: "loop_for".into(),
        name: String::new(),
        config: HashMap::from([("count".into(), serde_json::json!(count))]),
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
async fn loop_for_runs_body_n_times_then_exits() {
    let home = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(home.path().to_path_buf()).await.unwrap());

    // body -> lf ; lf -(Loop "loop", max 5)-> body ; lf -(Forward "exit")-> done.
    // loop_for emits "loop" for iterations 1..count-1 and "exit" at iteration
    // count, so with count = 3 the body runs 3 times and "done" runs once.
    let wf = Arc::new(Workflow {
        id: "wf_loop_for".into(),
        name: "loop for".into(),
        schema_version: 1,
        created_at: None,
        updated_at: None,
        variables: HashMap::new(),
        triggers: vec![],
        nodes: vec![
            delay_node("body"),
            loop_for_node("lf", 3),
            delay_node("done"),
        ],
        edges: vec![
            fwd("e1", "body", "lf"),
            loop_edge("eloop", "lf", "body", "loop", 5),
            fwd_branch("e2", "lf", "done", "exit"),
        ],
        resources: vec![],
        default_env: None,
    });

    let summary = engine
        .run_workflow(wf, HashMap::new(), "test", false, None)
        .await
        .expect("loop_for run completes");
    assert_eq!(summary.status, "done", "run summary: {summary:?}");

    assert_eq!(
        count_runs(&engine, &summary.run_id, "body"),
        3,
        "loop_for count = 3 should run the body 3 times"
    );
    assert_eq!(
        count_runs(&engine, &summary.run_id, "done"),
        1,
        "the exit edge should run exactly once"
    );

    // Dormancy: the exit node must START only after loop_for's final
    // evaluation — not on the first iteration. (Count-only assertions miss
    // an early exit because the final node_run counts are unchanged.)
    let done_start = first_start_seq(&engine, &summary.run_id, "done").expect("done started");
    let last_lf_done = last_done_seq(&engine, &summary.run_id, "lf");
    assert!(
        done_start > last_lf_done,
        "exit node must start after loop_for's final evaluation (done seq {done_start}, last lf done {last_lf_done})"
    );
}
