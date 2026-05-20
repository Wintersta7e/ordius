//! `NodeType` registry.
//!
//! Built-ins are inserted at startup via [`Registry::with_v1_0_builtins`];
//! manifest-loaded types land here once the manifest loader ships.

use crate::executor::builtins::checkpoint::{
    NODE_TYPE_ID as CHECKPOINT_NODE_TYPE_ID, PAUSE_NODE_TYPE_ID,
};
use crate::executor::builtins::condition::NODE_TYPE_ID as CONDITION_NODE_TYPE_ID;
use crate::executor::builtins::delay::NODE_TYPE_ID as DELAY_NODE_TYPE_ID;
use crate::executor::builtins::file::NODE_TYPE_ID as FILE_NODE_TYPE_ID;
use crate::executor::builtins::http::NODE_TYPE_ID as HTTP_NODE_TYPE_ID;
use crate::executor::builtins::kv::NODE_TYPE_ID as KV_NODE_TYPE_ID;
use crate::executor::builtins::llm::NODE_TYPE_ID as LLM_NODE_TYPE_ID;
use crate::executor::builtins::loop_for::NODE_TYPE_ID as LOOP_FOR_NODE_TYPE_ID;
use crate::executor::builtins::notify::NODE_TYPE_ID as NOTIFY_NODE_TYPE_ID;
use crate::executor::builtins::transform::NODE_TYPE_ID as TRANSFORM_NODE_TYPE_ID;
use crate::executor::builtins::wait_event::NODE_TYPE_ID as WAIT_EVENT_NODE_TYPE_ID;
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

    /// Registry pre-populated with the v1.0 + v1.1 built-ins. Engine
    /// constructors use this; `with_v1_0_builtins` stays for tests that
    /// pin the v1.0 surface.
    #[must_use]
    pub fn with_v1_1_builtins() -> Self {
        let mut r = Self::with_v1_0_builtins();
        r.register(kv_spec());
        r.register(notify_spec());
        r.register(pause_spec());
        r.register(loop_for_spec());
        r.register(wait_event_spec());
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
        id: DELAY_NODE_TYPE_ID.into(),
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
        id: TRANSFORM_NODE_TYPE_ID.into(),
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
        id: HTTP_NODE_TYPE_ID.into(),
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
        id: CHECKPOINT_NODE_TYPE_ID.into(),
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

fn wait_event_spec() -> NodeType {
    NodeType {
        id: WAIT_EVENT_NODE_TYPE_ID.into(),
        name: "Wait Event".into(),
        category: Category::Control,
        tags: vec![],
        icon: "bell".into(),
        description: "Park until an external caller delivers an event with the \
                      matching name via Engine::deliver_event."
            .into(),
        inputs: vec![],
        outputs: vec![PortDef {
            name: "payload".into(),
            ty: PortType::Json,
            required: false,
        }],
        config: vec![ConfigFieldDef {
            name: "event".into(),
            label: "Event name".into(),
            ty: ConfigFieldType::String,
            default: None,
            required: true,
        }],
        execution: in_process_execution_spec(),
    }
}

fn loop_for_spec() -> NodeType {
    NodeType {
        id: LOOP_FOR_NODE_TYPE_ID.into(),
        name: "Loop For".into(),
        category: Category::Control,
        tags: vec![],
        icon: "repeat".into(),
        description: "Bounded iteration counter. Emits branch='loop' while \
                      iteration < count, then 'exit'. Pair with a loop edge \
                      whose max_iterations matches count."
            .into(),
        inputs: vec![],
        outputs: vec![
            PortDef {
                name: "branch".into(),
                ty: PortType::String,
                required: false,
            },
            PortDef {
                name: "iteration".into(),
                ty: PortType::Number,
                required: false,
            },
        ],
        config: vec![ConfigFieldDef {
            name: "count".into(),
            label: "Iterations".into(),
            ty: ConfigFieldType::Number,
            default: Some(serde_json::json!(1)),
            required: true,
        }],
        execution: in_process_execution_spec(),
    }
}

fn pause_spec() -> NodeType {
    NodeType {
        id: PAUSE_NODE_TYPE_ID.into(),
        name: "Pause".into(),
        category: Category::Control,
        tags: vec![],
        icon: "pause".into(),
        description: "Human-approval gate. Run halts until an external caller resumes \
                      via the engine's checkpoint registry."
            .into(),
        inputs: vec![],
        outputs: vec![],
        config: vec![
            ConfigFieldDef {
                name: "message".into(),
                label: "Prompt".into(),
                ty: ConfigFieldType::Textarea,
                default: None,
                required: false,
            },
            ConfigFieldDef {
                name: "auto_resume".into(),
                label: "Auto-resume (test-only)".into(),
                ty: ConfigFieldType::Boolean,
                default: Some(serde_json::json!(false)),
                required: false,
            },
        ],
        execution: in_process_execution_spec(),
    }
}

fn notify_spec() -> NodeType {
    NodeType {
        id: NOTIFY_NODE_TYPE_ID.into(),
        name: "Notify".into(),
        category: Category::Integration,
        tags: vec![],
        icon: "bell".into(),
        description: "POST a {title, message} body to a webhook URL — Slack / Discord / \
                      Mattermost compatible."
            .into(),
        inputs: vec![],
        outputs: vec![
            PortDef {
                name: "status".into(),
                ty: PortType::Number,
                required: false,
            },
            PortDef {
                name: "ok".into(),
                ty: PortType::Boolean,
                required: false,
            },
        ],
        config: vec![
            ConfigFieldDef {
                name: "url".into(),
                label: "Webhook URL".into(),
                ty: ConfigFieldType::String,
                default: None,
                required: true,
            },
            ConfigFieldDef {
                name: "title".into(),
                label: "Title".into(),
                ty: ConfigFieldType::String,
                default: None,
                required: false,
            },
            ConfigFieldDef {
                name: "message".into(),
                label: "Message".into(),
                ty: ConfigFieldType::Textarea,
                default: None,
                required: true,
            },
            ConfigFieldDef {
                name: "timeout_ms".into(),
                label: "Timeout (ms)".into(),
                ty: ConfigFieldType::Number,
                default: Some(serde_json::json!(10000)),
                required: false,
            },
        ],
        execution: in_process_execution_spec(),
    }
}

fn kv_spec() -> NodeType {
    NodeType {
        id: KV_NODE_TYPE_ID.into(),
        name: "KV".into(),
        category: Category::Data,
        tags: vec![],
        icon: "database".into(),
        description: "Persistent per-workflow key-value store backed by SQLite. \
                      Survives across runs of the same workflow."
            .into(),
        inputs: vec![],
        outputs: vec![
            PortDef {
                name: "value".into(),
                ty: PortType::String,
                required: false,
            },
            PortDef {
                name: "exists".into(),
                ty: PortType::Boolean,
                required: false,
            },
        ],
        config: vec![
            ConfigFieldDef {
                name: "op".into(),
                label: "Operation".into(),
                ty: ConfigFieldType::Select,
                default: Some(serde_json::json!(["get", "set", "delete"])),
                required: true,
            },
            ConfigFieldDef {
                name: "key".into(),
                label: "Key".into(),
                ty: ConfigFieldType::String,
                default: None,
                required: true,
            },
            ConfigFieldDef {
                name: "value".into(),
                label: "Value (set only)".into(),
                ty: ConfigFieldType::String,
                default: None,
                required: false,
            },
        ],
        execution: in_process_execution_spec(),
    }
}

fn file_spec() -> NodeType {
    NodeType {
        id: FILE_NODE_TYPE_ID.into(),
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
        id: LLM_NODE_TYPE_ID.into(),
        name: "LLM".into(),
        category: Category::Llm,
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
        id: CONDITION_NODE_TYPE_ID.into(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_v1_0_builtins_registers_all_eight() {
        let r = Registry::with_v1_0_builtins();
        let mut ids = r.ids();
        ids.sort();
        assert_eq!(
            ids,
            vec![
                "checkpoint",
                "condition",
                "delay",
                "file",
                "http",
                "llm",
                "shell",
                "transform",
            ],
        );
    }
}
