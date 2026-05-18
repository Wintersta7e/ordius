//! `NodeType` spec — built-in or manifest-loaded. Same shape covers both.
//! Spec: `docs/03-node-types.md` "The node-type contract".

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::types::PortDef;

/// Specification of a node type. Drives schema validation, GUI palette
/// rendering, and executor dispatch. Built-in node types and JSON/YAML
/// manifest custom nodes share this shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeType {
    /// Stable type identifier — referenced by `Node.ty`.
    pub id: String,
    /// Display name for the GUI palette.
    pub name: String,
    /// One of five primary categories — drives palette grouping.
    pub category: Category,
    /// Orthogonal capability tags (e.g. `gpu`, `streaming`).
    #[serde(default)]
    pub tags: Vec<String>,
    /// Lucide icon id or relative path. Optional.
    #[serde(default)]
    pub icon: String,
    /// Human-readable description shown in the palette tooltip.
    #[serde(default)]
    pub description: String,
    /// Input ports — drives edge-target validation.
    #[serde(default)]
    pub inputs: Vec<PortDef>,
    /// Output ports — drives edge-source validation.
    #[serde(default)]
    pub outputs: Vec<PortDef>,
    /// Configurable fields rendered in the GUI properties panel.
    #[serde(default)]
    pub config: Vec<ConfigFieldDef>,
    /// How the engine runs this type.
    pub execution: ExecutionSpec,
}

/// Primary palette category. Every node type belongs to exactly one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Category {
    /// `shell`, `subprocess`-style runners.
    Execution,
    /// LLM-shaped nodes (chat, completion, embedding, etc.).
    Llm,
    /// Data manipulation — `transform`, `parse`, `file`.
    Data,
    /// Flow control — `condition`, `checkpoint`, `delay`.
    Control,
    /// External services — `http`, `notification`, etc.
    Integration,
}

/// Definition of a single configurable field on a `NodeType`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigFieldDef {
    /// Stable field identifier within the type.
    pub name: String,
    /// Label shown in the GUI.
    pub label: String,
    /// Field renderer hint.
    #[serde(rename = "type")]
    pub ty: ConfigFieldType,
    /// Optional default value (`serde_json::Value` to keep typing flexible).
    #[serde(default)]
    pub default: Option<serde_json::Value>,
    /// If true, the GUI / loader requires a value.
    #[serde(default)]
    pub required: bool,
}

/// GUI renderer hint for a `ConfigFieldDef`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConfigFieldType {
    /// Single-line text input.
    String,
    /// Numeric input.
    Number,
    /// Checkbox / toggle.
    Boolean,
    /// Multi-line text input.
    Textarea,
    /// Dropdown — values supplied via `default`.
    Select,
    /// File picker.
    File,
    /// OS-keyring secret picker (lists secret names only).
    Secret,
}

/// How the engine executes a `NodeType` — backend, command, env, output handling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionSpec {
    /// Backend selector.
    pub backend: ExecutionBackend,
    /// Argv when `backend = subprocess` or `container`.
    #[serde(default)]
    pub command: Vec<String>,
    /// Optional stdin template (subject to template substitution).
    #[serde(default)]
    pub stdin_template: Option<String>,
    /// Environment variables passed to the child process.
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Per-type timeout (milliseconds). `None` means no explicit timeout.
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    /// How stdout is parsed for the `output_map` step.
    #[serde(default)]
    pub output_parse: OutputParse,
    /// Output-port name → `JSONPath` expression evaluated against parsed stdout.
    /// `JSONPath` only in v1.0 — full DSL is v1.1.
    #[serde(default)]
    pub output_map: HashMap<String, String>,
}

/// How stdout should be parsed before `output_map` evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputParse {
    /// stdout treated as a UTF-8 string. `output_map` operates on the
    /// raw string (only `$.` root references it).
    #[default]
    Text,
    /// stdout parsed as JSON; `output_map` evaluates `JSONPath` expressions.
    Json,
}

/// Executor backend — drives `NodeExecutor` selection in the dispatcher.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionBackend {
    /// Runs entirely in-process (delay, transform, condition, etc.).
    InProcess,
    /// Spawned subprocess with process-group / Job-Object cancellation.
    Subprocess,
    /// Containerised execution (v1.x — not yet implemented in v1.0).
    Container,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_yaml_parses() {
        let y = r#"
id: ffmpeg-trim
name: Trim video
category: data
tags: [video, file]
inputs:
  - { name: input, type: file, required: true }
outputs:
  - { name: output, type: file }
config:
  - { name: start, label: Start, type: string, default: "00:00:00" }
execution:
  backend: subprocess
  command: [sh, -c, 'echo $1', '--', '{{inputs.input}}']
  output_parse: json
  output_map:
    output: $.output
"#;
        let n: NodeType = serde_yaml::from_str(y).unwrap();
        assert_eq!(n.id, "ffmpeg-trim");
        assert_eq!(n.category, Category::Data);
        assert_eq!(n.tags, vec!["video".to_string(), "file".to_string()]);
        assert_eq!(n.execution.backend, ExecutionBackend::Subprocess);
        assert_eq!(n.execution.output_parse, OutputParse::Json);
    }

    #[test]
    fn output_parse_default_is_text() {
        // `OutputParse::Text` is the default via `#[derive(Default)]` + `#[default]`.
        let parsed: OutputParse = serde_json::from_str(r#""text""#).unwrap();
        assert_eq!(parsed, OutputParse::default());
        assert_eq!(parsed, OutputParse::Text);
    }

    #[test]
    fn execution_backend_serialises_snake_case() {
        // `in_process` — not `InProcess` or `in-process`.
        assert_eq!(
            serde_json::to_string(&ExecutionBackend::InProcess).unwrap(),
            r#""in_process""#
        );
        assert_eq!(
            serde_json::to_string(&ExecutionBackend::Subprocess).unwrap(),
            r#""subprocess""#
        );
        assert_eq!(
            serde_json::to_string(&ExecutionBackend::Container).unwrap(),
            r#""container""#
        );
    }
}
