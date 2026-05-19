//! Edge-activation DAG scheduler. Pure state machine — no executors.
//! Spec: `docs/02-engine-model.md` "The scheduler".

mod state;

pub use state::NodeState;

use crate::types::{Edge, EdgeType, Node, Workflow};
use std::collections::{HashMap, HashSet};

/// Edge-activation scheduler over a workflow's DAG.
///
/// Walks the graph without executing anything: callers report node
/// completion, failure, and loop fires; the scheduler maintains
/// per-node state and an indexed view of incoming, outgoing, and
/// loop edges. The run-loop polls [`Scheduler::ready`] for work and
/// [`Scheduler::is_done`] (and, later, `is_stalled`) for termination.
pub struct Scheduler<'a> {
    pub(crate) nodes: &'a [Node],
    pub(crate) incoming: HashMap<&'a str, Vec<&'a Edge>>,
    pub(crate) outgoing: HashMap<&'a str, Vec<&'a Edge>>,
    pub(crate) loops_by_condition: HashMap<&'a str, Vec<&'a Edge>>,
    pub(crate) state: HashMap<String, NodeState>,
    pub(crate) loop_counters: HashMap<String, u32>,
    #[expect(dead_code, reason = "consumed by skip-event drainer")]
    pub(crate) emitted_skipped: HashSet<String>,
}

/// A successful loop fire returned by [`Scheduler::try_loop`].
///
/// The caller is responsible for emitting the corresponding
/// `node:loop` event using these fields, and for re-dispatching
/// the freshly-reset nodes.
#[derive(Debug, PartialEq, Eq)]
pub struct LoopFire<'a> {
    /// The loop edge that fired.
    pub edge: &'a Edge,
    /// 1-based iteration count (the first fire reports 1).
    pub iteration: u32,
    /// Nodes whose state was reset for the new iteration —
    /// includes the condition node itself plus everything in the
    /// loop subgraph between the loop target and the condition.
    pub reset_nodes: Vec<String>,
}

impl<'a> Scheduler<'a> {
    /// Build a scheduler over `wf`. Indexes edges by direction and
    /// seeds initial state: nodes with no incoming forward edges
    /// start `Ready`, all others start `Pending`.
    #[must_use]
    pub fn new(wf: &'a Workflow) -> Self {
        let mut incoming: HashMap<&'a str, Vec<&'a Edge>> = HashMap::new();
        let mut outgoing: HashMap<&'a str, Vec<&'a Edge>> = HashMap::new();
        let mut loops_by_condition: HashMap<&'a str, Vec<&'a Edge>> = HashMap::new();
        for e in &wf.edges {
            if e.kind == EdgeType::Forward {
                incoming.entry(&e.to_node_id).or_default().push(e);
                outgoing.entry(&e.from_node_id).or_default().push(e);
            } else {
                loops_by_condition
                    .entry(&e.from_node_id)
                    .or_default()
                    .push(e);
            }
        }
        let state: HashMap<String, NodeState> = wf
            .nodes
            .iter()
            .map(|n| {
                let s = if incoming.get(n.id.as_str()).is_none_or(Vec::is_empty) {
                    NodeState::Ready
                } else {
                    NodeState::Pending
                };
                (n.id.clone(), s)
            })
            .collect();
        Self {
            nodes: &wf.nodes,
            incoming,
            outgoing,
            loops_by_condition,
            state,
            loop_counters: HashMap::new(),
            emitted_skipped: HashSet::new(),
        }
    }

    /// Current state of `node_id`. Unknown ids return `Pending`.
    #[must_use]
    pub fn state_of(&self, node_id: &str) -> NodeState {
        self.state
            .get(node_id)
            .copied()
            .unwrap_or(NodeState::Pending)
    }

    /// Nodes currently in `Ready`, in workflow declaration order.
    #[must_use]
    pub fn ready(&self) -> Vec<&'a Node> {
        self.nodes
            .iter()
            .filter(|n| self.state_of(&n.id) == NodeState::Ready)
            .collect()
    }

    /// True when every node has reached a terminal state
    /// (`Done`, `Error`, or `Skipped`).
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.nodes.iter().all(|n| {
            matches!(
                self.state_of(&n.id),
                NodeState::Done | NodeState::Error | NodeState::Skipped
            )
        })
    }

    /// Mark `node_id` as `Running`. Idempotent.
    pub fn start_node(&mut self, node_id: &str) {
        self.state.insert(node_id.into(), NodeState::Running);
    }

    /// Mark `node_id` as `Done` and promote any downstream nodes
    /// whose forward-edge predecessors are now all `Done`.
    pub fn complete_node(&mut self, node_id: &str) {
        self.state.insert(node_id.into(), NodeState::Done);
        self.promote_downstream(node_id);
    }

    /// Mark `node_id` as `Error` and cascade `Skipped` through the
    /// transitive forward-edge descendants that are still
    /// `Pending` or `Ready`.
    pub fn fail_node(&mut self, node_id: &str) {
        self.state.insert(node_id.into(), NodeState::Error);
        let children: Vec<String> = self
            .outgoing
            .get(node_id)
            .map(|outs| outs.iter().map(|e| e.to_node_id.clone()).collect())
            .unwrap_or_default();
        for child in &children {
            self.skip_subtree(child);
        }
    }

    /// Resolve a condition node's chosen branch. Outgoing edges
    /// whose `branch` label differs from `branch` have their
    /// downstream subtree (target + descendants) cascaded to
    /// `Skipped`; the matching branch and any unbranched edges
    /// then activate normally.
    pub fn resolve_condition(&mut self, node_id: &str, branch: &str) {
        let to_skip: Vec<String> = self
            .outgoing
            .get(node_id)
            .map(|outs| {
                outs.iter()
                    .filter(|e| e.branch.as_deref().is_some_and(|b| b != branch))
                    .map(|e| e.to_node_id.clone())
                    .collect()
            })
            .unwrap_or_default();
        for target in &to_skip {
            self.skip_subtree(target);
        }
        self.state.insert(node_id.into(), NodeState::Done);
        self.promote_downstream(node_id);
    }

    fn promote_downstream(&mut self, node_id: &str) {
        let downstream: Vec<String> = self
            .outgoing
            .get(node_id)
            .map(|v| v.iter().map(|e| e.to_node_id.clone()).collect())
            .unwrap_or_default();
        for d in downstream {
            if self.state_of(&d) != NodeState::Pending {
                continue;
            }
            let all_satisfied = self.incoming.get(d.as_str()).is_none_or(|incs| {
                incs.iter()
                    .all(|e| self.state_of(&e.from_node_id) == NodeState::Done)
            });
            if all_satisfied {
                self.state.insert(d, NodeState::Ready);
            }
        }
    }

    /// Attempt to fire a loop edge from `condition_node_id` with
    /// the chosen `branch` label. Returns `None` if there is no
    /// matching loop edge or the edge's `max_iterations` cap has
    /// already been reached.
    ///
    /// On success, the loop subgraph (forward-reachable nodes
    /// from the loop target up to but not including the
    /// condition node, plus the condition node itself) is reset
    /// to its initial state (`Ready` if it has no incoming
    /// forward edges, otherwise `Pending`) and a [`LoopFire`] is
    /// returned describing what was reset.
    pub fn try_loop(&mut self, condition_node_id: &str, branch: &str) -> Option<LoopFire<'a>> {
        let loops = self.loops_by_condition.get(condition_node_id)?.clone();
        let edge = loops
            .into_iter()
            .find(|e| e.branch.as_deref() == Some(branch))?;
        let cap = edge.max_iterations.unwrap_or(1);
        let count = self.loop_counters.entry(edge.id.clone()).or_insert(0);
        if *count >= cap {
            return None;
        }
        *count += 1;
        let iteration = *count;
        let mut reset_nodes = self.collect_loop_subgraph(&edge.to_node_id, condition_node_id);
        // Include the condition node itself in the reset — otherwise
        // it stays Done and the next iteration cannot re-run it.
        reset_nodes.push(condition_node_id.to_string());
        for id in &reset_nodes {
            let s = if self.incoming.get(id.as_str()).is_none_or(Vec::is_empty) {
                NodeState::Ready
            } else {
                NodeState::Pending
            };
            self.state.insert(id.clone(), s);
        }
        Some(LoopFire {
            edge,
            iteration,
            reset_nodes,
        })
    }

    /// BFS from `start` through forward edges, stopping at `end`
    /// (exclusive). Returns the nodes between `start` and `end`
    /// on the looping subgraph; the caller adds `end` to the
    /// reset set itself.
    fn collect_loop_subgraph(&self, start: &str, end: &str) -> Vec<String> {
        let mut seen: HashSet<String> = HashSet::new();
        let mut out: Vec<String> = Vec::new();
        let mut q: Vec<String> = vec![start.to_string()];
        while let Some(cur) = q.pop() {
            if cur == end {
                continue;
            }
            if !seen.insert(cur.clone()) {
                continue;
            }
            if let Some(outs) = self.outgoing.get(cur.as_str()) {
                for e in outs {
                    q.push(e.to_node_id.clone());
                }
            }
            out.push(cur);
        }
        out
    }

    /// Mark `root` and its transitive forward-edge descendants as
    /// `Skipped`, stopping at nodes already in a terminal or
    /// `Running` state. Used by [`Self::fail_node`] (iterated
    /// over children, not the failed node itself) and
    /// [`Self::resolve_condition`] (over unselected branch
    /// targets).
    fn skip_subtree(&mut self, root: &str) {
        let mut stack: Vec<String> = vec![root.to_string()];
        while let Some(id) = stack.pop() {
            if !matches!(self.state_of(&id), NodeState::Pending | NodeState::Ready) {
                continue;
            }
            self.state.insert(id.clone(), NodeState::Skipped);
            if let Some(outs) = self.outgoing.get(id.as_str()) {
                for e in outs {
                    stack.push(e.to_node_id.clone());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
