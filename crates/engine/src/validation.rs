//! Structural workflow validation.
//!
//! Checks unique node ids, edges reference real nodes, and no forward-only
//! cycles. Loop edges from condition nodes are allowed even though they create
//! graph-level cycles — the scheduler handles those as deliberate back-edges.

use std::collections::{HashMap, HashSet};

use thiserror::Error;

use crate::types::{EdgeType, Workflow};

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

    Ok(())
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
}
