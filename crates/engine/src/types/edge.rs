//! Edges connect a node's output port to another node's input port.
//! Spec: docs/06-data-flow.md "Edges + branches".

use serde::{Deserialize, Serialize};

/// A typed connection between two nodes' ports. Forward edges drive
/// normal DAG execution; loop edges create back-edges from condition
/// nodes for the scheduler's iteration logic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Edge {
    /// Stable identifier within the workflow.
    pub id: String,
    /// Source node id.
    #[serde(rename = "fromNodeId")]
    pub from_node_id: String,
    /// Source node port name.
    #[serde(rename = "fromPort")]
    pub from_port: String,
    /// Destination node id.
    #[serde(rename = "toNodeId")]
    pub to_node_id: String,
    /// Destination node port name.
    #[serde(rename = "toPort")]
    pub to_port: String,
    /// Forward vs. loop. Default forward.
    #[serde(default, rename = "edgeType")]
    pub kind: EdgeType,
    /// Maximum iterations for loop edges. Ignored for forward edges.
    #[serde(default, rename = "maxIterations")]
    pub max_iterations: Option<u32>,
    /// Optional branch tag — used by `condition` nodes to route output
    /// to specific edges (e.g. `"true"` / `"false"`).
    #[serde(default)]
    pub branch: Option<String>,
}

/// Edge kind — forward (normal DAG) or loop (back-edge from condition).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EdgeType {
    /// Normal forward edge — activates the downstream node when upstream completes.
    #[default]
    Forward,
    /// Back-edge from a condition node. Activates the downstream node
    /// for the next iteration of a loop. Excluded from cycle detection.
    Loop,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forward_edge_defaults_are_implicit() {
        let e: Edge = serde_json::from_str(
            r#"{"id":"e1","fromNodeId":"a","fromPort":"out","toNodeId":"b","toPort":"in"}"#,
        )
        .unwrap();
        assert_eq!(e.kind, EdgeType::Forward);
        assert_eq!(e.max_iterations, None);
        assert!(e.branch.is_none());
    }

    #[test]
    fn loop_edge_parses() {
        let e: Edge = serde_json::from_str(
            r#"{"id":"e1","fromNodeId":"cond","fromPort":"out","toNodeId":"a","toPort":"in","edgeType":"loop","maxIterations":5,"branch":"true"}"#,
        )
        .unwrap();
        assert_eq!(e.kind, EdgeType::Loop);
        assert_eq!(e.max_iterations, Some(5));
        assert_eq!(e.branch.as_deref(), Some("true"));
    }
}
