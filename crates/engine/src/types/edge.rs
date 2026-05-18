//! Edges connect a node's output port to another node's input port.
//! Spec: docs/06-data-flow.md "Edges + branches".

use serde::{Deserialize, Serialize};

/// A typed connection between two nodes' ports. Forward edges drive
/// normal DAG execution; loop edges create back-edges from condition
/// nodes for the scheduler's iteration logic.
///
/// **Wire format.** Fields serialise in `snake_case` on disk (per project
/// convention). The `kind` Rust field renames to `edge_type` in JSON so
/// the storage key matches the rest of the schema. The Tauri command
/// boundary converts to `camelCase` for the GUI; engine code never sees
/// the `camelCase` form.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Edge {
    /// Stable identifier within the workflow.
    pub id: String,
    /// Source node id.
    pub from_node_id: String,
    /// Source node port name.
    pub from_port: String,
    /// Destination node id.
    pub to_node_id: String,
    /// Destination node port name.
    pub to_port: String,
    /// Forward vs. loop. Default forward.
    #[serde(default, rename = "edge_type")]
    pub kind: EdgeType,
    /// Maximum iterations for loop edges. Ignored for forward edges.
    #[serde(default)]
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
            r#"{"id":"e1","from_node_id":"a","from_port":"out","to_node_id":"b","to_port":"in"}"#,
        )
        .unwrap();
        assert_eq!(e.kind, EdgeType::Forward);
        assert_eq!(e.max_iterations, None);
        assert!(e.branch.is_none());
    }

    #[test]
    fn loop_edge_parses() {
        let e: Edge = serde_json::from_str(
            r#"{"id":"e1","from_node_id":"cond","from_port":"out","to_node_id":"a","to_port":"in","edge_type":"loop","max_iterations":5,"branch":"true"}"#,
        )
        .unwrap();
        assert_eq!(e.kind, EdgeType::Loop);
        assert_eq!(e.max_iterations, Some(5));
        assert_eq!(e.branch.as_deref(), Some("true"));
    }

    #[test]
    fn edge_serialises_as_snake_case() {
        let e = Edge {
            id: "e1".into(),
            from_node_id: "a".into(),
            from_port: "out".into(),
            to_node_id: "b".into(),
            to_port: "in".into(),
            kind: EdgeType::Loop,
            max_iterations: Some(3),
            branch: None,
        };
        let json = serde_json::to_string(&e).unwrap();
        assert!(
            json.contains(r#""from_node_id":"a""#),
            "snake_case from_node_id: {json}"
        );
        assert!(
            json.contains(r#""edge_type":"loop""#),
            "snake_case edge_type: {json}"
        );
        assert!(
            json.contains(r#""max_iterations":3"#),
            "snake_case max_iterations: {json}"
        );
    }
}
