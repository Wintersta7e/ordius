//! Workflow file format — the root structure of `~/.ordius/workflows/<id>.json`.
//! Spec: docs/04-storage-and-format.md.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::types::{Edge, Node};

/// A complete workflow: identity, schema metadata, variables, triggers,
/// nodes, and edges. The root structure persisted to disk and exchanged
/// over the Tauri command boundary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Workflow {
    /// Stable workflow identifier (filename stem).
    pub id: String,
    /// Display name shown in the GUI.
    pub name: String,
    /// Workflow file schema version. v1.0 is `1`.
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    /// ISO-8601 timestamp the workflow was first written.
    #[serde(default)]
    pub created_at: Option<String>,
    /// ISO-8601 timestamp of the last save.
    #[serde(default)]
    pub updated_at: Option<String>,
    /// User-declared variables. Default values overridable per-run.
    #[serde(default)]
    pub variables: HashMap<String, String>,
    /// Trigger declarations. Empty implies manual-only.
    #[serde(default)]
    pub triggers: Vec<Trigger>,
    /// All node instances on the graph.
    #[serde(default)]
    pub nodes: Vec<Node>,
    /// All edges between nodes.
    #[serde(default)]
    pub edges: Vec<Edge>,
}

const fn default_schema_version() -> u32 {
    1
}

/// Trigger declaration. Tagged union over the four trigger kinds.
///
/// `#[serde(tag = "type", rename_all = "kebab-case")]` renames only the
/// variant tag value (e.g. `FileWatch` → `"file-watch"`); fields within
/// the struct variants stay `snake_case`.
///
/// v1.0 ignores all triggers except `Manual` at runtime, but the schema
/// accepts all four for forward-compat with v1.1.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum Trigger {
    /// CLI / GUI button — always implicitly available even if absent.
    Manual,
    /// Cron-scheduled. Requires `ordius daemon` or open GUI to fire.
    Schedule {
        /// Cron expression (5- or 6-field).
        cron: String,
        /// Variables injected into the run.
        #[serde(default)]
        vars: HashMap<String, String>,
    },
    /// Filesystem-watch trigger. Requires `ordius daemon` or open GUI.
    FileWatch {
        /// Glob paths to watch.
        paths: Vec<String>,
        /// Debounce window in milliseconds.
        #[serde(default = "default_debounce")]
        debounce_ms: u64,
        /// Variables injected into the run.
        #[serde(default)]
        vars: HashMap<String, String>,
    },
    /// HTTP webhook (v1.1+). Schema-accepted in v1.0, never fires.
    Webhook {
        /// Template expression resolving to the shared-secret token at run
        /// time — e.g. `"{{secrets.WHK_TOKEN}}"`. The substitution layer
        /// (Phase 5) reads the actual value from the OS keyring; the raw
        /// secret must never appear here on disk.
        #[serde(default)]
        secret_token: Option<String>,
    },
}

const fn default_debounce() -> u64 {
    2000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_minimal_json_loads() {
        let w: Workflow =
            serde_json::from_str(r#"{"id":"w1","name":"hi","nodes":[],"edges":[]}"#).unwrap();
        assert_eq!(w.id, "w1");
        assert_eq!(w.schema_version, 1);
        assert!(w.triggers.is_empty());
        assert!(w.variables.is_empty());
    }

    #[test]
    fn triggers_tagged_union_parses() {
        let json = r#"{
            "id":"w1","name":"x","triggers":[
              {"type":"manual"},
              {"type":"schedule","cron":"0 9 * * *"},
              {"type":"file-watch","paths":["./a"],"debounce_ms":1500}
            ]}"#;
        let w: Workflow = serde_json::from_str(json).unwrap();
        assert_eq!(w.triggers.len(), 3);
        match &w.triggers[1] {
            Trigger::Schedule { cron, .. } => assert_eq!(cron, "0 9 * * *"),
            other => panic!("expected schedule, got {other:?}"),
        }
        match &w.triggers[2] {
            Trigger::FileWatch {
                paths, debounce_ms, ..
            } => {
                assert_eq!(paths, &vec!["./a".to_string()]);
                assert_eq!(*debounce_ms, 1500);
            },
            other => panic!("expected file-watch, got {other:?}"),
        }
    }

    #[test]
    fn trigger_tag_serialises_kebab_case() {
        // FileWatch variant must serialise as "file-watch" (kebab), and
        // its fields must stay snake_case (debounce_ms not debounce-ms).
        let t = Trigger::FileWatch {
            paths: vec!["./x".into()],
            debounce_ms: 500,
            vars: HashMap::new(),
        };
        let json = serde_json::to_string(&t).unwrap();
        assert!(json.contains(r#""type":"file-watch""#), "kebab tag: {json}");
        assert!(json.contains(r#""debounce_ms":500"#), "snake field: {json}");
    }
}
