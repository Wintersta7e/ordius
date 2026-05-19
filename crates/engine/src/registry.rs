//! `NodeType` registry.
//!
//! Built-ins are inserted at startup via [`Registry::with_v1_0_builtins`];
//! manifest-loaded types land here once the manifest loader ships.

use crate::executor::subprocess::{PORT_EXIT_CODE, PORT_TEXT, SHELL_NODE_TYPE_ID};
use crate::types::{
    Category, ConfigFieldDef, ConfigFieldType, ExecutionBackend, ExecutionSpec, NodeType,
    OutputParse, PortDef, PortType,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Lookup table from node type id to its specification.
///
/// Specs are wrapped in `Arc` so the dispatcher and executors can
/// hold the same definition without cloning the underlying data.
#[derive(Default)]
pub struct Registry {
    types: HashMap<String, Arc<NodeType>>,
}

impl Registry {
    /// Empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            types: HashMap::new(),
        }
    }

    /// Insert (or replace) a type spec keyed by its `id`.
    pub fn register(&mut self, nt: NodeType) {
        self.types.insert(nt.id.clone(), Arc::new(nt));
    }

    /// Look up a type spec by id. Returns a cheap `Arc` clone.
    #[must_use]
    pub fn get(&self, id: &str) -> Option<Arc<NodeType>> {
        self.types.get(id).cloned()
    }

    /// All registered type ids.
    #[must_use]
    pub fn ids(&self) -> Vec<String> {
        self.types.keys().cloned().collect()
    }

    /// Registry pre-populated with the v1.0 built-ins.
    #[must_use]
    pub fn with_v1_0_builtins() -> Self {
        let mut r = Self::new();
        r.register(delay_spec());
        r.register(transform_spec());
        r.register(condition_spec());
        r.register(shell_spec());
        r.register(http_spec());
        r.register(llm_spec());
        r.register(file_spec());
        r.register(checkpoint_spec());
        r
    }
}

fn in_process_execution_spec() -> ExecutionSpec {
    ExecutionSpec {
        backend: ExecutionBackend::InProcess,
        command: vec![],
        stdin_template: None,
        env: HashMap::new(),
        timeout_ms: None,
        output_parse: OutputParse::Text,
        output_map: HashMap::new(),
    }
}

fn delay_spec() -> NodeType {
    NodeType {
        id: "delay".into(),
        name: "Delay".into(),
        category: Category::Control,
        tags: vec![],
        icon: "clock".into(),
        description: "Sleep N milliseconds".into(),
        inputs: vec![],
        outputs: vec![],
        config: vec![],
        execution: in_process_execution_spec(),
    }
}

fn transform_spec() -> NodeType {
    NodeType {
        id: "transform".into(),
        name: "Transform".into(),
        category: Category::Data,
        tags: vec![],
        icon: "shuffle".into(),
        description: "JSONPath / regex extraction and replacement".into(),
        inputs: vec![],
        outputs: vec![],
        config: vec![],
        execution: in_process_execution_spec(),
    }
}

fn http_spec() -> NodeType {
    NodeType {
        id: "http".into(),
        name: "HTTP".into(),
        category: Category::Integration,
        tags: vec![],
        icon: "globe".into(),
        description: "Make an HTTP request. 4xx/5xx are NOT errors — they surface on the \
                      `status` port. Only network failures raise NodeError."
            .into(),
        inputs: vec![],
        outputs: vec![
            PortDef {
                name: "status".into(),
                ty: PortType::Number,
                required: false,
            },
            PortDef {
                name: "body".into(),
                ty: PortType::Json,
                required: false,
            },
            PortDef {
                name: "headers".into(),
                ty: PortType::Json,
                required: false,
            },
        ],
        config: vec![
            ConfigFieldDef {
                name: "url".into(),
                label: "URL".into(),
                ty: ConfigFieldType::String,
                default: None,
                required: true,
            },
            ConfigFieldDef {
                name: "method".into(),
                label: "Method".into(),
                ty: ConfigFieldType::String,
                default: Some(serde_json::json!("GET")),
                required: false,
            },
            ConfigFieldDef {
                name: "headers".into(),
                label: "Headers".into(),
                ty: ConfigFieldType::Textarea,
                default: None,
                required: false,
            },
            ConfigFieldDef {
                name: "body".into(),
                label: "Body".into(),
                ty: ConfigFieldType::Textarea,
                default: None,
                required: false,
            },
            ConfigFieldDef {
                name: "query".into(),
                label: "Query".into(),
                ty: ConfigFieldType::Textarea,
                default: None,
                required: false,
            },
            ConfigFieldDef {
                name: "timeout_ms".into(),
                label: "Timeout (ms)".into(),
                ty: ConfigFieldType::Number,
                default: Some(serde_json::json!(30_000)),
                required: false,
            },
        ],
        execution: in_process_execution_spec(),
    }
}

fn checkpoint_spec() -> NodeType {
    NodeType {
        id: "checkpoint".into(),
        name: "Checkpoint".into(),
        category: Category::Control,
        tags: vec![],
        icon: "pause-circle".into(),
        description: "Pause the run until an external caller resumes via the \
                      CheckpointRegistry. auto_resume=true skips the pause."
            .into(),
        inputs: vec![],
        outputs: vec![],
        config: vec![
            ConfigFieldDef {
                name: "message".into(),
                label: "Message".into(),
                ty: ConfigFieldType::String,
                default: Some(serde_json::json!("Waiting for user to continue...")),
                required: false,
            },
            ConfigFieldDef {
                name: "auto_resume".into(),
                label: "Auto-resume (testing)".into(),
                ty: ConfigFieldType::Boolean,
                default: Some(serde_json::json!(false)),
                required: false,
            },
        ],
        execution: in_process_execution_spec(),
    }
}

fn file_spec() -> NodeType {
    NodeType {
        id: "file".into(),
        name: "File".into(),
        category: Category::Data,
        tags: vec![],
        icon: "file".into(),
        description: "Read / write / append / list / glob / stat. Paths relative to \
                      the run workspace unless absolute."
            .into(),
        inputs: vec![],
        outputs: vec![],
        config: vec![
            ConfigFieldDef {
                name: "op".into(),
                label: "Operation".into(),
                ty: ConfigFieldType::Select,
                default: Some(serde_json::json!([
                    "read", "write", "append", "list", "glob", "stat"
                ])),
                required: true,
            },
            ConfigFieldDef {
                name: "path".into(),
                label: "Path".into(),
                ty: ConfigFieldType::String,
                default: None,
                required: false,
            },
            ConfigFieldDef {
                name: "content".into(),
                label: "Content (write / append)".into(),
                ty: ConfigFieldType::Textarea,
                default: None,
                required: false,
            },
            ConfigFieldDef {
                name: "pattern".into(),
                label: "Glob pattern".into(),
                ty: ConfigFieldType::String,
                default: None,
                required: false,
            },
        ],
        execution: in_process_execution_spec(),
    }
}

fn llm_spec() -> NodeType {
    NodeType {
        id: "llm".into(),
        name: "LLM".into(),
        category: Category::Integration,
        tags: vec![],
        icon: "sparkles".into(),
        description: "OpenAI-compatible chat completion. Streams assistant deltas as \
                      node:output when stream=true. Non-2xx surfaces on finish_reason."
            .into(),
        inputs: vec![],
        outputs: vec![
            PortDef {
                name: "text".into(),
                ty: PortType::String,
                required: false,
            },
            PortDef {
                name: "tokens_used".into(),
                ty: PortType::Number,
                required: false,
            },
            PortDef {
                name: "finish_reason".into(),
                ty: PortType::String,
                required: false,
            },
        ],
        config: vec![
            ConfigFieldDef {
                name: "url".into(),
                label: "Base URL".into(),
                ty: ConfigFieldType::String,
                default: Some(serde_json::json!("http://localhost:11434/v1")),
                required: false,
            },
            ConfigFieldDef {
                name: "model".into(),
                label: "Model".into(),
                ty: ConfigFieldType::String,
                default: None,
                required: true,
            },
            ConfigFieldDef {
                name: "messages".into(),
                label: "Messages".into(),
                ty: ConfigFieldType::Textarea,
                default: None,
                required: true,
            },
            ConfigFieldDef {
                name: "temperature".into(),
                label: "Temperature".into(),
                ty: ConfigFieldType::Number,
                default: Some(serde_json::json!(0.7)),
                required: false,
            },
            ConfigFieldDef {
                name: "max_tokens".into(),
                label: "Max tokens".into(),
                ty: ConfigFieldType::Number,
                default: None,
                required: false,
            },
            ConfigFieldDef {
                name: "stream".into(),
                label: "Stream".into(),
                ty: ConfigFieldType::Boolean,
                default: Some(serde_json::json!(true)),
                required: false,
            },
            ConfigFieldDef {
                name: "api_key".into(),
                label: "API key".into(),
                ty: ConfigFieldType::Secret,
                default: None,
                required: false,
            },
        ],
        execution: in_process_execution_spec(),
    }
}

fn condition_spec() -> NodeType {
    NodeType {
        id: "condition".into(),
        name: "Condition".into(),
        category: Category::Control,
        tags: vec![],
        icon: "git-branch".into(),
        description: "Branch evaluator (boolean / exit_code / regex / jsonpath)".into(),
        inputs: vec![],
        outputs: vec![],
        config: vec![],
        execution: in_process_execution_spec(),
    }
}

/// `shell` built-in: run a free-form shell command via `bash -c`
/// on Unix and `cmd /C` on Windows. The user's `config.command` is
/// passed as the shell's `-c` / `/C` argument (so compound forms
/// like `for`/`if`/pipes work), after going through the unified
/// template engine first.
fn shell_spec() -> NodeType {
    NodeType {
        id: SHELL_NODE_TYPE_ID.into(),
        name: "Shell".into(),
        category: Category::Execution,
        tags: vec![],
        icon: "terminal".into(),
        description: "Run a shell command (bash on Unix, cmd on Windows). \
                      Captures stdout/stderr + exit code."
            .into(),
        inputs: vec![],
        outputs: vec![
            PortDef {
                name: PORT_TEXT.into(),
                ty: PortType::String,
                required: false,
            },
            PortDef {
                name: PORT_EXIT_CODE.into(),
                ty: PortType::Number,
                required: false,
            },
        ],
        config: vec![ConfigFieldDef {
            name: "command".into(),
            label: "Command".into(),
            ty: ConfigFieldType::Textarea,
            default: None,
            required: true,
        }],
        execution: ExecutionSpec {
            backend: ExecutionBackend::Subprocess,
            // SubprocessExecutor keys off SHELL_NODE_TYPE_ID and
            // wraps config.command in the per-platform shell argv.
            command: vec![],
            stdin_template: None,
            env: HashMap::new(),
            timeout_ms: None,
            output_parse: OutputParse::Text,
            output_map: HashMap::new(),
        },
    }
}
