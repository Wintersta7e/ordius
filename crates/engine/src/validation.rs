//! Structural workflow validation.
//!
//! Checks unique node ids, edges reference real nodes, and no forward-only
//! cycles. Loop edges from condition nodes are allowed even though they create
//! graph-level cycles — the scheduler handles those as deliberate back-edges.

use std::collections::{HashMap, HashSet};

use thiserror::Error;

use crate::scheduler::collect_loop_subgraph;
use crate::types::{Edge, EdgeType, Node, Workflow};

/// Failure modes for `validate`.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ValidationError {
    /// Workflow has no nodes — nothing to run.
    #[error("workflow has no nodes")]
    Empty,
    /// Two or more nodes share an id.
    #[error("duplicate node id: {0}")]
    DuplicateNodeId(String),
    /// An edge references a node that doesn't exist in the workflow.
    #[error("edge {edge_id} references unknown {side} node {node_id}")]
    UnknownNode {
        /// Offending edge id.
        edge_id: String,
        /// Which end of the edge — `"from"` or `"to"`.
        side: &'static str,
        /// Node id the edge points at.
        node_id: String,
    },
    /// Forward-only edges form a cycle. The reported node id is one
    /// participant in the cycle (DFS doesn't necessarily report the
    /// smallest cycle, just the first one it finds).
    #[error("forward-only cycle through node: {0}")]
    ForwardCycle(String),
    /// A loop's closing condition reads an upstream output produced
    /// outside the loop's reset subgraph. That source never re-runs
    /// across iterations, so the green-check sees a stale value and
    /// the loop can never make progress.
    #[error(
        "loop condition {condition} reads node {outside} from outside the loop subgraph; \
         its value never changes across iterations"
    )]
    LoopInputFromOutside {
        /// The condition node closing the loop.
        condition: String,
        /// The out-of-subgraph node the condition references. Named
        /// `outside` rather than `source` because thiserror reserves a
        /// `source` field as the error-cause chain (requires `dyn Error`).
        outside: String,
    },
}

/// Validate a workflow's structural invariants. Returns the first failure
/// encountered; no batched error reporting in v1.0.
pub fn validate(wf: &Workflow) -> Result<(), ValidationError> {
    if wf.nodes.is_empty() {
        return Err(ValidationError::Empty);
    }

    // Unique-id check, accumulated id set for edge lookups.
    let mut ids = HashSet::with_capacity(wf.nodes.len());
    for n in &wf.nodes {
        if !ids.insert(n.id.as_str()) {
            return Err(ValidationError::DuplicateNodeId(n.id.clone()));
        }
    }

    // Edge endpoint existence.
    for e in &wf.edges {
        if !ids.contains(e.from_node_id.as_str()) {
            return Err(ValidationError::UnknownNode {
                edge_id: e.id.clone(),
                side: "from",
                node_id: e.from_node_id.clone(),
            });
        }
        if !ids.contains(e.to_node_id.as_str()) {
            return Err(ValidationError::UnknownNode {
                edge_id: e.id.clone(),
                side: "to",
                node_id: e.to_node_id.clone(),
            });
        }
    }

    // Forward-only adjacency list. Loop edges are deliberate back-edges;
    // the scheduler handles them, not the validator.
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
    for e in &wf.edges {
        if e.kind == EdgeType::Forward {
            adj.entry(e.from_node_id.as_str())
                .or_default()
                .push(e.to_node_id.as_str());
        }
    }

    // Classical 3-colour DFS: 0 = unvisited, 1 = on current stack, 2 = done.
    let mut color: HashMap<&str, u8> = HashMap::new();
    for n in &wf.nodes {
        if color.get(n.id.as_str()).copied().unwrap_or(0) == 0
            && let Err(cycle_node) = dfs(n.id.as_str(), &adj, &mut color)
        {
            return Err(ValidationError::ForwardCycle(cycle_node.to_owned()));
        }
    }

    check_loop_green_inputs(wf)?;

    Ok(())
}

/// GR-5: reject a loop whose closing condition reads node outputs but none
/// from inside the loop's reset subgraph.
///
/// For each `Loop` edge `cond -> loop_target`, the scheduler resets
/// `collect_loop_subgraph(loop_target, cond)` plus `cond` itself every
/// iteration. The green-check must read at least one CHANGING signal from
/// inside that set. A baseline read from OUTSIDE is fine as long as something
/// inside is also read (design GR-3). Reject only when every referenced node
/// is outside — that result can never change, so the loop is stuck.
///
/// Condition nodes read upstream values only through `{{nodes.*}}` config
/// templates, never via wired input ports (every mode in
/// `executor/builtins/condition.rs` reads from `node.config`), so scanning the
/// condition's config strings is the complete model of what its green-check
/// sees. If conditions ever grow port-based inputs, this rule must scan those.
fn check_loop_green_inputs(wf: &Workflow) -> Result<(), ValidationError> {
    // Forward-edge adjacency, shaped exactly like the scheduler's
    // `outgoing` index so the shared BFS walks the same graph.
    let mut outgoing: HashMap<&str, Vec<&Edge>> = HashMap::new();
    for e in &wf.edges {
        if e.kind == EdgeType::Forward {
            outgoing.entry(e.from_node_id.as_str()).or_default().push(e);
        }
    }
    let nodes_by_id: HashMap<&str, &Node> = wf.nodes.iter().map(|n| (n.id.as_str(), n)).collect();

    for e in &wf.edges {
        if e.kind != EdgeType::Loop {
            continue;
        }
        let cond_id = e.from_node_id.as_str();
        let loop_target = e.to_node_id.as_str();
        let Some(cond_node) = nodes_by_id.get(cond_id) else {
            // Dangling endpoints were already rejected above.
            continue;
        };
        // Reset set = subgraph (loop_target .. cond, exclusive) + cond.
        let mut reset: HashSet<&str> = collect_loop_subgraph(&outgoing, loop_target, cond_id)
            .iter()
            .map(|s| {
                // Re-borrow against the workflow's owned ids so the set
                // holds `&str` with the workflow's lifetime. The `""` fallback
                // is unreachable — the BFS only visits validated edge targets
                // and endpoint existence is checked above — but keeps the
                // re-borrow total against future reorderings.
                nodes_by_id
                    .get_key_value(s.as_str())
                    .map_or_else(|| "", |(k, _)| *k)
            })
            .filter(|s| !s.is_empty())
            .collect();
        reset.insert(cond_id);

        // Split the condition's real-node references by whether they sit inside
        // the reset set. Reject only when the check reads node outputs but NONE
        // are inside (no changing signal) — a baseline read from outside is fine
        // as long as an inside signal is also read (design GR-3).
        let mut reads_inside = false;
        let mut first_outside: Option<String> = None;
        for value in cond_node.config.values() {
            if let Some(text) = value.as_str() {
                for source in node_refs(text) {
                    if !nodes_by_id.contains_key(source.as_str()) {
                        continue; // unknown id (typo) — reported at run time
                    }
                    if reset.contains(source.as_str()) {
                        reads_inside = true;
                    } else if first_outside.is_none() {
                        first_outside = Some(source);
                    }
                }
            }
        }
        if !reads_inside && let Some(outside) = first_outside {
            return Err(ValidationError::LoopInputFromOutside {
                condition: cond_id.to_string(),
                outside,
            });
        }
    }
    Ok(())
}

/// Extract the `ID` from every plain `{{nodes.ID.outputs.PORT}}` reference in
/// `text`: the inner reference is split on `.`, the namespace must be `nodes`,
/// and the second segment is the node id. Whitespace inside `{{ ... }}` is
/// trimmed as the substituter does. Malformed or non-`nodes` references are
/// ignored here — `substitute` reports those at run time.
///
/// The optional `{{json nodes.*}}` helper-prefixed form is intentionally NOT
/// matched (its first segment is `json nodes`). Missing it only yields a false
/// negative for GR-5, and the loop still terminates via `max_iterations`; that
/// form also never appears on a condition's compare operands (it JSON-quotes
/// the value, breaking the compare).
fn node_refs(text: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find("{{") {
        let after_open = &rest[start + 2..];
        let Some(end) = after_open.find("}}") else {
            break;
        };
        let inner = after_open[..end].trim();
        let mut parts = inner.split('.');
        if parts.next() == Some("nodes")
            && let Some(nid) = parts.next()
            && !nid.is_empty()
        {
            out.push(nid.to_string());
        }
        rest = &after_open[end + 2..];
    }
    out
}

fn dfs<'a>(
    node: &'a str,
    adj: &'a HashMap<&'a str, Vec<&'a str>>,
    color: &mut HashMap<&'a str, u8>,
) -> Result<(), &'a str> {
    color.insert(node, 1);
    if let Some(outs) = adj.get(node) {
        for &m in outs {
            match color.get(m).copied().unwrap_or(0) {
                0 => dfs(m, adj, color)?,
                1 => return Err(m),
                _ => {},
            }
        }
    }
    color.insert(node, 2);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::types::{Edge, EdgeType, Node, Workflow};

    fn n(id: &str) -> Node {
        Node {
            id: id.into(),
            ty: "delay".into(),
            name: id.into(),
            config: HashMap::new(),
            pos: crate::types::Pos::default(),
            timeout_ms: None,
            retry: None,
            continue_on_error: false,
            target_env: None,
        }
    }

    fn e(id: &str, from: &str, to: &str, kind: EdgeType) -> Edge {
        Edge {
            id: id.into(),
            from_node_id: from.into(),
            from_port: "x".into(),
            to_node_id: to.into(),
            to_port: "y".into(),
            kind,
            max_iterations: None,
            branch: None,
        }
    }

    fn wf(nodes: Vec<Node>, edges: Vec<Edge>) -> Workflow {
        Workflow {
            id: String::new(),
            name: String::new(),
            schema_version: 1,
            created_at: None,
            updated_at: None,
            variables: HashMap::new(),
            triggers: Vec::new(),
            nodes,
            edges,
            resources: vec![],
            default_env: None,
        }
    }

    #[test]
    fn empty_rejected() {
        assert_eq!(validate(&wf(vec![], vec![])), Err(ValidationError::Empty));
    }

    #[test]
    fn single_node_ok() {
        assert!(validate(&wf(vec![n("a")], vec![])).is_ok());
    }

    #[test]
    fn duplicate_node_id_caught() {
        assert_eq!(
            validate(&wf(vec![n("a"), n("a")], vec![])),
            Err(ValidationError::DuplicateNodeId("a".into())),
        );
    }

    #[test]
    fn dangling_from_edge_caught() {
        let w = wf(vec![n("a")], vec![e("e1", "ghost", "a", EdgeType::Forward)]);
        assert!(matches!(
            validate(&w),
            Err(ValidationError::UnknownNode { side: "from", .. })
        ));
    }

    #[test]
    fn dangling_to_edge_caught() {
        let w = wf(vec![n("a")], vec![e("e1", "a", "ghost", EdgeType::Forward)]);
        assert!(matches!(
            validate(&w),
            Err(ValidationError::UnknownNode { side: "to", .. })
        ));
    }

    #[test]
    fn forward_cycle_rejected() {
        let w = wf(
            vec![n("a"), n("b")],
            vec![
                e("e1", "a", "b", EdgeType::Forward),
                e("e2", "b", "a", EdgeType::Forward),
            ],
        );
        assert!(matches!(
            validate(&w),
            Err(ValidationError::ForwardCycle(_))
        ));
    }

    #[test]
    fn loop_edge_doesnt_count_as_cycle() {
        let w = wf(
            vec![n("a"), n("b")],
            vec![
                e("e1", "a", "b", EdgeType::Forward),
                e("e2", "b", "a", EdgeType::Loop),
            ],
        );
        assert!(validate(&w).is_ok());
    }

    /// A `condition` node whose config carries `fields` (key -> raw
    /// string value, which may embed `{{nodes.X.outputs.Y}}`).
    fn cond(id: &str, fields: &[(&str, &str)]) -> Node {
        let mut config = HashMap::new();
        for (k, v) in fields {
            config.insert(
                (*k).to_string(),
                serde_json::Value::String((*v).to_string()),
            );
        }
        Node {
            id: id.into(),
            ty: "condition".into(),
            name: id.into(),
            config,
            pos: crate::types::Pos::default(),
            timeout_ms: None,
            retry: None,
            continue_on_error: false,
            target_env: None,
        }
    }

    fn loop_e(id: &str, from: &str, to: &str, branch: &str) -> Edge {
        let mut e = e(id, from, to, EdgeType::Loop);
        e.branch = Some(branch.into());
        e
    }

    #[test]
    fn loop_condition_reading_outside_subgraph_rejected() {
        // outside -> body -> cond ; cond loops back to body on "false".
        // The condition reads outside's output, which the loop never resets.
        let w = wf(
            vec![
                n("outside"),
                n("body"),
                cond("cond", &[("left", "{{nodes.outside.outputs.text}}")]),
            ],
            vec![
                e("e1", "outside", "body", EdgeType::Forward),
                e("e2", "body", "cond", EdgeType::Forward),
                loop_e("L", "cond", "body", "false"),
            ],
        );
        assert_eq!(
            validate(&w),
            Err(ValidationError::LoopInputFromOutside {
                condition: "cond".into(),
                outside: "outside".into(),
            }),
        );
    }

    #[test]
    fn loop_condition_reading_inside_subgraph_ok() {
        // body is inside the reset subgraph; reading its output is fine.
        let w = wf(
            vec![
                n("seed"),
                n("body"),
                cond("cond", &[("left", "{{nodes.body.outputs.text}}")]),
            ],
            vec![
                e("e1", "seed", "body", EdgeType::Forward),
                e("e2", "body", "cond", EdgeType::Forward),
                loop_e("L", "cond", "body", "false"),
            ],
        );
        assert!(validate(&w).is_ok());
    }

    #[test]
    fn loop_condition_reading_inside_and_outside_baseline_ok() {
        // GR-3 baseline: the condition reads a CHANGING signal from inside the
        // subgraph (body) AND a once-captured baseline from outside (baseline).
        // Allowed, because an inside signal is read.
        let w = wf(
            vec![
                n("seed"),
                n("baseline"),
                n("body"),
                cond(
                    "cond",
                    &[
                        ("left", "{{nodes.body.outputs.text}}"),
                        ("right", "{{nodes.baseline.outputs.text}}"),
                    ],
                ),
            ],
            vec![
                e("e1", "seed", "body", EdgeType::Forward),
                e("e2", "seed", "baseline", EdgeType::Forward),
                e("e3", "body", "cond", EdgeType::Forward),
                e("e4", "baseline", "cond", EdgeType::Forward),
                loop_e("L", "cond", "body", "false"),
            ],
        );
        assert!(
            validate(&w).is_ok(),
            "a baseline read from outside is fine when an inside signal is also read"
        );
    }
}
