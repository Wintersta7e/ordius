//! Workflow file format — the root structure of `~/.ordius/workflows/<id>.json`.
//! Spec: docs/04-storage-and-format.md.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::environment::runtime::resource::ResourceDefinition;
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
    /// Workflow-scoped resource definitions. Installed under
    /// `ScopeKey::Workflow { id }` when the workflow is loaded; removed
    /// when the workflow is deleted. Defaults to empty.
    #[serde(default)]
    pub resources: Vec<ResourceDefinition>,
    /// Default env applied to every node that does not set its own
    /// `target_env`. Defaults to `None`, which is functionally equivalent
    /// to the engine's `local` env in Phase D. Phase E adds env-registry
    /// validation.
    #[serde(default)]
    pub default_env: Option<crate::types::EnvId>,
}

impl Workflow {
    /// Compute the deduplicated, sorted list of envs the engine must wire
    /// up before this workflow can run.
    ///
    /// The list always covers:
    /// - every node's explicit `target_env`,
    /// - the workflow-level `default_env` (if set),
    /// - `local` whenever any `http` or `llm` node carries
    ///   `config.origin == "host"` (`HostDirect` routes always reach via the
    ///   host loopback dispatcher, regardless of where the node's
    ///   `target_env` resolves).
    ///
    /// Used by [`crate::Engine::build_run_snapshot`] to freeze dispatchers,
    /// catalogs, and `EnvSpec`s at run start.
    #[must_use]
    pub fn envs_in_scope(&self) -> Vec<crate::types::EnvId> {
        use std::collections::HashSet;

        let mut scope: HashSet<crate::types::EnvId> = HashSet::new();

        if let Some(ref default_env) = self.default_env {
            scope.insert(default_env.clone());
        }

        let mut host_origin_seen = false;
        for node in &self.nodes {
            if let Some(ref target) = node.target_env {
                scope.insert(target.clone());
            }
            if !host_origin_seen
                && (node.ty == "http" || node.ty == "llm")
                && node
                    .config
                    .get("origin")
                    .and_then(serde_json::Value::as_str)
                    == Some("host")
            {
                host_origin_seen = true;
            }
        }

        if host_origin_seen {
            scope.insert(crate::types::EnvId::local());
        }

        let mut out: Vec<crate::types::EnvId> = scope.into_iter().collect();
        out.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        out
    }
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
        /// reads the actual value from the OS keyring; the raw secret
        /// must never appear here on disk.
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

    #[test]
    fn workflow_resources_block_defaults_empty() {
        let w: Workflow =
            serde_json::from_str(r#"{"id":"w1","name":"hi","nodes":[],"edges":[]}"#).unwrap();
        assert!(w.resources.is_empty());
    }

    #[test]
    fn workflow_default_env_defaults_to_none() {
        let w: Workflow =
            serde_json::from_str(r#"{"id":"w1","name":"x","nodes":[],"edges":[]}"#).unwrap();
        assert!(w.default_env.is_none());
    }

    #[test]
    fn workflow_default_env_loads_when_present() {
        let json = r#"{"id":"w1","name":"x","nodes":[],"edges":[],"default_env":"wsl:Ubuntu"}"#;
        let w: Workflow = serde_json::from_str(json).unwrap();
        assert_eq!(w.default_env.as_ref().unwrap().as_str(), "wsl:Ubuntu");
    }

    #[test]
    fn workflow_resources_block_loads_when_present() {
        let json = r#"{
            "id": "w1",
            "name": "x",
            "nodes": [],
            "edges": [],
            "resources": [
                {
                    "id": "wf-local-llm",
                    "kind": "http_endpoint",
                    "advertised_capabilities": ["openai_chat_completions"],
                    "probe": {
                        "kind": "http",
                        "ports": [7777],
                        "routes": [
                            {
                                "path": "/v1/models",
                                "method": "get",
                                "flavor": "openai_chat",
                                "proves": ["openai_chat_completions"],
                                "models_jsonpath": null,
                                "fingerprint_jsonpaths": []
                            }
                        ]
                    },
                    "override_lower_scope": false
                },
                {
                    "id": "wf-second",
                    "kind": "binary",
                    "advertised_capabilities": [],
                    "probe": {
                        "kind": "binary",
                        "bin": "my-tool",
                        "version_args": ["--version"],
                        "version_regex": "(\\d+\\.\\d+)"
                    },
                    "override_lower_scope": false
                }
            ]
        }"#;
        let w: Workflow = serde_json::from_str(json).unwrap();
        assert_eq!(w.resources.len(), 2);
        assert_eq!(w.resources[0].id.0, "wf-local-llm");
        assert_eq!(w.resources[1].id.0, "wf-second");
    }
}
