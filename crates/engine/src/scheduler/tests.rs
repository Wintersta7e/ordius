use super::{NodeState, Scheduler};
use crate::types::{Edge, EdgeType, Node, Pos, Workflow};
use std::collections::HashMap;

fn node(id: &str) -> Node {
    Node {
        id: id.into(),
        ty: "delay".into(),
        name: id.into(),
        config: HashMap::new(),
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
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

fn wf(nodes: Vec<Node>, edges: Vec<Edge>) -> Workflow {
    Workflow {
        id: "w".into(),
        name: "w".into(),
        schema_version: 1,
        created_at: None,
        updated_at: None,
        variables: HashMap::new(),
        triggers: vec![],
        nodes,
        edges,
    }
}

#[test]
fn source_nodes_start_ready() {
    let w = wf(vec![node("a"), node("b")], vec![fwd("e", "a", "b")]);
    let s = Scheduler::new(&w);
    assert_eq!(s.state_of("a"), NodeState::Ready);
    assert_eq!(s.state_of("b"), NodeState::Pending);
    assert_eq!(s.ready().len(), 1);
}

#[test]
fn completion_promotes_downstream() {
    let w = wf(vec![node("a"), node("b")], vec![fwd("e", "a", "b")]);
    let mut s = Scheduler::new(&w);
    s.complete_node("a");
    assert_eq!(s.state_of("a"), NodeState::Done);
    assert_eq!(s.state_of("b"), NodeState::Ready);
}

#[test]
fn start_node_marks_running() {
    let w = wf(vec![node("a")], vec![]);
    let mut s = Scheduler::new(&w);
    s.start_node("a");
    assert_eq!(s.state_of("a"), NodeState::Running);
}

#[test]
fn failure_cascades_to_skipped() {
    let w = wf(
        vec![node("a"), node("b"), node("c")],
        vec![fwd("e1", "a", "b"), fwd("e2", "b", "c")],
    );
    let mut s = Scheduler::new(&w);
    s.fail_node("a");
    assert_eq!(s.state_of("a"), NodeState::Error);
    assert_eq!(s.state_of("b"), NodeState::Skipped);
    assert_eq!(s.state_of("c"), NodeState::Skipped);
}

#[test]
fn convergent_nodes_wait_for_all_upstream() {
    let w = wf(
        vec![node("a"), node("b"), node("c")],
        vec![fwd("e1", "a", "c"), fwd("e2", "b", "c")],
    );
    let mut s = Scheduler::new(&w);
    s.complete_node("a");
    assert_eq!(s.state_of("c"), NodeState::Pending);
    s.complete_node("b");
    assert_eq!(s.state_of("c"), NodeState::Ready);
}

#[test]
fn condition_skips_unused_branch() {
    let w = wf(
        vec![node("cond"), node("b"), node("c")],
        vec![
            fwd_branch("e1", "cond", "b", "true"),
            fwd_branch("e2", "cond", "c", "false"),
        ],
    );
    let mut s = Scheduler::new(&w);
    s.resolve_condition("cond", "true");
    assert_eq!(s.state_of("cond"), NodeState::Done);
    assert_eq!(s.state_of("b"), NodeState::Ready);
    assert_eq!(s.state_of("c"), NodeState::Skipped);
}

#[test]
fn condition_promotes_unbranched_edges() {
    let w = wf(
        vec![node("cond"), node("b"), node("c")],
        vec![
            fwd_branch("e1", "cond", "b", "true"),
            fwd("e2", "cond", "c"),
        ],
    );
    let mut s = Scheduler::new(&w);
    s.resolve_condition("cond", "false");
    assert_eq!(s.state_of("b"), NodeState::Skipped);
    assert_eq!(s.state_of("c"), NodeState::Ready);
}

#[test]
fn loop_resets_subgraph_within_cap() {
    let w = wf(
        vec![node("a"), node("cond"), node("b")],
        vec![
            fwd("e1", "a", "cond"),
            fwd_branch("e2", "cond", "b", "done"),
            loop_edge("eloop", "cond", "a", "loop", 2),
        ],
    );
    let mut s = Scheduler::new(&w);
    s.complete_node("a");
    assert_eq!(s.state_of("cond"), NodeState::Ready);
    let fired = s.try_loop("cond", "loop").expect("first fire");
    assert_eq!(fired.iteration, 1);
    assert!(fired.reset_nodes.iter().any(|id| id == "a"));
    assert!(fired.reset_nodes.iter().any(|id| id == "cond"));
    assert_eq!(s.state_of("a"), NodeState::Ready);
    assert_eq!(s.state_of("cond"), NodeState::Pending);
    s.complete_node("a");
    let fired2 = s.try_loop("cond", "loop").expect("second fire");
    assert_eq!(fired2.iteration, 2);
    s.complete_node("a");
    assert!(s.try_loop("cond", "loop").is_none());
}

#[test]
fn try_loop_returns_none_when_branch_unknown() {
    let w = wf(
        vec![node("a"), node("cond")],
        vec![
            fwd("e1", "a", "cond"),
            loop_edge("eloop", "cond", "a", "loop", 3),
        ],
    );
    let mut s = Scheduler::new(&w);
    s.complete_node("a");
    assert!(s.try_loop("cond", "no-such-branch").is_none());
}

#[test]
fn try_loop_returns_none_for_node_with_no_loop_edges() {
    let w = wf(vec![node("a")], vec![]);
    let mut s = Scheduler::new(&w);
    assert!(s.try_loop("a", "loop").is_none());
}

#[test]
fn drain_newly_skipped_is_idempotent() {
    let w = wf(
        vec![node("a"), node("b"), node("c")],
        vec![fwd("e1", "a", "b"), fwd("e2", "b", "c")],
    );
    let mut s = Scheduler::new(&w);
    s.fail_node("a");
    let first = s.drain_newly_skipped();
    assert!(first.iter().any(|id| id == "b"));
    assert!(first.iter().any(|id| id == "c"));
    assert!(s.drain_newly_skipped().is_empty());
}

#[test]
fn drain_newly_skipped_reports_each_id_once_across_calls() {
    // Two disconnected chains so each fail affects only one of
    // them — confirms the second call doesn't re-report ids that
    // were already drained in the first.
    let w = wf(
        vec![node("a"), node("b"), node("c"), node("d")],
        vec![fwd("e1", "a", "b"), fwd("e2", "c", "d")],
    );
    let mut s = Scheduler::new(&w);
    s.fail_node("a");
    let first = s.drain_newly_skipped();
    assert!(first.iter().any(|id| id == "b"));
    assert!(first.iter().all(|id| id != "d"));
    s.fail_node("c");
    let second = s.drain_newly_skipped();
    assert!(second.iter().any(|id| id == "d"));
    assert!(second.iter().all(|id| id != "b"));
}

#[test]
fn is_done_when_all_terminal() {
    let w = wf(vec![node("a"), node("b")], vec![fwd("e", "a", "b")]);
    let mut s = Scheduler::new(&w);
    assert!(!s.is_done());
    s.complete_node("a");
    s.complete_node("b");
    assert!(s.is_done());
}

#[test]
fn stall_detected_when_no_progress_possible() {
    let w = wf(vec![node("a"), node("b")], vec![fwd("e", "a", "b")]);
    let mut s = Scheduler::new(&w);
    // Force the source node Pending so nothing is Ready/Running
    // but the workflow isn't done.
    s.state.insert("a".into(), NodeState::Pending);
    assert!(s.is_stalled());
}

#[test]
fn fresh_scheduler_with_source_is_not_stalled() {
    let w = wf(vec![node("a"), node("b")], vec![fwd("e", "a", "b")]);
    let s = Scheduler::new(&w);
    assert!(!s.is_stalled());
}

#[test]
fn running_node_is_not_stalled() {
    let w = wf(vec![node("a"), node("b")], vec![fwd("e", "a", "b")]);
    let mut s = Scheduler::new(&w);
    s.start_node("a");
    assert!(!s.is_stalled());
}

#[test]
fn done_workflow_is_not_stalled() {
    let w = wf(vec![node("a")], vec![]);
    let mut s = Scheduler::new(&w);
    s.complete_node("a");
    assert!(s.is_done());
    assert!(!s.is_stalled());
}
