//! `coding-agent` built-in: run a detected CLI coding agent (claude/codex/…)
//! in non-interactive ("print") mode via the env dispatcher. Prompt over
//! stdin, result on the `text` port. Pure helpers here; the executor branch
//! lives in `executor/subprocess.rs`.

use crate::environment::runtime::ScopeKey;

/// A coding-agent may only run a resource defined at a TRUSTED scope: a
/// shipped built-in or a user-global definition. Workflow- and env-local-scoped
/// definitions are rejected, so an imported workflow cannot override a known
/// agent id (e.g. `aider`) to point at an arbitrary binary.
#[allow(unreachable_pub, dead_code)]
pub const fn agent_scope_is_trusted(scope: &ScopeKey) -> bool {
    matches!(scope, ScopeKey::Builtin | ScopeKey::UserGlobal)
}

/// Stable node-type id. NOT `"agent"` — that id is reserved → `llm`
/// (`loader.rs` [`RESERVED_NODE_TYPE_IDS`]), so we use `coding-agent`.
#[allow(unreachable_pub, dead_code)]
pub const NODE_TYPE_ID: &str = "coding-agent";

/// Output port carrying the agent's final result text.
#[allow(unreachable_pub, dead_code)]
pub const PORT_TEXT: &str = "text";
/// Output port carrying the process exit code.
#[allow(unreachable_pub, dead_code)]
pub const PORT_EXIT_CODE: &str = "exit_code";
/// Output port carrying the agent session/thread id (for loop resume, Phase B5).
#[allow(unreachable_pub, dead_code)]
pub const PORT_SESSION_ID: &str = "session_id";

/// Permission level requested on the node.
#[allow(unreachable_pub, dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Permission {
    Read,
    Edit,
    Full,
}

impl Permission {
    #[allow(unreachable_pub, dead_code)]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "read" => Some(Self::Read),
            "edit" => Some(Self::Edit),
            "full" => Some(Self::Full),
            _ => None,
        }
    }
}

/// Non-interactive ("print") subcommand/flags per agent id, including the
/// structured-output request where the CLI supports it (claude/codex).
#[allow(unreachable_pub, dead_code)]
pub fn print_flags(agent_id: &str) -> Vec<&'static str> {
    match agent_id {
        "claude-code" => vec!["--print", "--output-format", "json"],
        "codex" => vec!["exec", "--json"],
        "aider" => vec!["--message"],
        "gemini-cli" => vec!["-p"],
        "goose" => vec!["run", "-t"],
        "amazon-q" => vec!["chat", "--no-interactive", "--trust-all-tools"],
        "opencode" => vec!["run"],
        "cursor-cli" => vec!["--print"],
        _ => Vec::new(),
    }
}

/// Sandbox/permission flags per (agent id, level). Empty for agents with no
/// sandbox model.
#[allow(unreachable_pub, dead_code)]
pub fn permission_flags(agent_id: &str, level: Permission) -> Vec<&'static str> {
    match (agent_id, level) {
        ("claude-code", Permission::Read) => vec!["--permission-mode", "plan"],
        ("claude-code", Permission::Edit) => vec!["--permission-mode", "acceptEdits"],
        ("claude-code", Permission::Full) => vec!["--dangerously-skip-permissions"],
        ("codex", Permission::Read) => vec!["--sandbox", "read-only"],
        ("codex", Permission::Edit) => vec!["--sandbox", "workspace-write"],
        ("codex", Permission::Full) => vec!["--dangerously-bypass-approvals-and-sandbox"],
        _ => Vec::new(),
    }
}

/// Permission levels a given agent meaningfully supports (drives the UI).
#[allow(unreachable_pub, dead_code)]
pub fn supported_permission_levels(agent_id: &str) -> Vec<&'static str> {
    match agent_id {
        "claude-code" | "codex" => vec!["read", "edit", "full"],
        _ => Vec::new(),
    }
}

/// Join the non-empty prompt sections with blank lines, in the fixed order
/// persona → prompt → output-format → context.
#[allow(unreachable_pub, dead_code)]
pub fn assemble_prompt(
    persona: Option<&str>,
    prompt: &str,
    output_format: Option<&str>,
    context: Option<&str>,
) -> String {
    [persona, Some(prompt), output_format, context]
        .into_iter()
        .flatten()
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// Whether we have a non-interactive invocation recipe for this agent id.
/// Resources advertising `CliAgentPrint` are self-asserted, so the executor
/// also requires the id to be a known agent before running the binary.
#[allow(unreachable_pub, dead_code)]
pub fn is_known_agent(agent_id: &str) -> bool {
    !print_flags(agent_id).is_empty()
}

/// Assemble the agent argv tail (everything after the binary path). Order:
/// print/structured flags → model → permission flags LAST, so the model never
/// shadows the node's permission posture.
#[allow(unreachable_pub, dead_code)]
pub fn build_agent_argv(
    agent_id: &str,
    permission: Option<Permission>,
    model: Option<&str>,
) -> Vec<String> {
    let mut args: Vec<String> = print_flags(agent_id)
        .into_iter()
        .map(str::to_string)
        .collect();
    if let Some(m) = model.filter(|s| !s.is_empty()) {
        args.push("--model".into());
        args.push(m.to_string());
    }
    if let Some(level) = permission {
        args.extend(
            permission_flags(agent_id, level)
                .into_iter()
                .map(str::to_string),
        );
    }
    args
}

/// Normalized agent result, dialect-agnostic.
#[allow(unreachable_pub, dead_code)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AgentOutput {
    pub text: String,
    pub session_id: Option<String>,
    pub is_error: bool,
}

/// Parse the agent's stdout into a normalized result. claude emits a single
/// JSON object (`--output-format json`); codex emits JSONL events (`--json`);
/// everyone else (and any parse failure) falls back to raw stdout.
#[allow(unreachable_pub, dead_code)]
pub fn normalize_output(agent_id: &str, stdout: &str) -> AgentOutput {
    match agent_id {
        "claude-code" => parse_claude_json(stdout).unwrap_or_else(|| raw(stdout)),
        "codex" => parse_codex_jsonl(stdout).unwrap_or_else(|| raw(stdout)),
        _ => raw(stdout),
    }
}

fn raw(stdout: &str) -> AgentOutput {
    AgentOutput {
        text: stdout.trim_end().to_string(),
        session_id: None,
        is_error: false,
    }
}

fn parse_claude_json(stdout: &str) -> Option<AgentOutput> {
    let v: serde_json::Value = serde_json::from_str(stdout.trim()).ok()?;
    // Require a string `result`; valid-but-unrecognized JSON falls back to raw.
    let result = v.get("result").and_then(|r| r.as_str())?;
    Some(AgentOutput {
        text: result.to_string(),
        session_id: v
            .get("session_id")
            .and_then(|s| s.as_str())
            .map(str::to_string),
        is_error: v
            .get("is_error")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
    })
}

fn parse_codex_jsonl(stdout: &str) -> Option<AgentOutput> {
    let mut text = None;
    let mut session_id = None;
    for line in stdout.lines().filter(|l| !l.trim().is_empty()) {
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if let Some(tid) = v.get("thread_id").and_then(|s| s.as_str()) {
            session_id = Some(tid.to_string());
        }
        if v.get("item")
            .and_then(|i| i.get("type"))
            .and_then(|t| t.as_str())
            == Some("agent_message")
            && let Some(t) = v
                .get("item")
                .and_then(|i| i.get("text"))
                .and_then(|t| t.as_str())
        {
            text = Some(t.to_string());
        }
    }
    // Only treat the run as parsed when an agent_message text was found; a
    // thread_id without any message falls back to raw stdout.
    text.map(|t| AgentOutput {
        text: t,
        session_id,
        is_error: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_scope_trust_gate() {
        use crate::environment::runtime::ScopeKey;
        use crate::environment::runtime::{EnvId, WorkflowId};
        assert!(agent_scope_is_trusted(&ScopeKey::Builtin));
        assert!(agent_scope_is_trusted(&ScopeKey::UserGlobal));
        assert!(!agent_scope_is_trusted(&ScopeKey::EnvLocal {
            id: EnvId::new("e")
        }));
        assert!(!agent_scope_is_trusted(&ScopeKey::Workflow {
            id: WorkflowId("w".into())
        }));
    }

    #[test]
    fn claude_print_flags_and_permission() {
        assert_eq!(
            print_flags("claude-code"),
            vec!["--print", "--output-format", "json"]
        );
        assert_eq!(
            permission_flags("claude-code", Permission::Read),
            vec!["--permission-mode", "plan"]
        );
        assert_eq!(
            permission_flags("claude-code", Permission::Edit),
            vec!["--permission-mode", "acceptEdits"]
        );
        assert_eq!(
            permission_flags("claude-code", Permission::Full),
            vec!["--dangerously-skip-permissions"]
        );
    }

    #[test]
    fn codex_print_flags_and_permission() {
        assert_eq!(print_flags("codex"), vec!["exec", "--json"]);
        assert_eq!(
            permission_flags("codex", Permission::Read),
            vec!["--sandbox", "read-only"]
        );
        assert_eq!(
            permission_flags("codex", Permission::Edit),
            vec!["--sandbox", "workspace-write"]
        );
        assert_eq!(
            permission_flags("codex", Permission::Full),
            vec!["--dangerously-bypass-approvals-and-sandbox"]
        );
    }

    #[test]
    fn unknown_agent_has_no_permission_flags() {
        assert!(permission_flags("aider", Permission::Full).is_empty());
        assert_eq!(print_flags("aider"), vec!["--message"]);
    }

    #[test]
    fn supported_permission_levels_only_for_sandboxed_agents() {
        assert_eq!(
            supported_permission_levels("claude-code"),
            vec!["read", "edit", "full"]
        );
        assert_eq!(
            supported_permission_levels("codex"),
            vec!["read", "edit", "full"]
        );
        assert!(supported_permission_levels("aider").is_empty());
    }

    #[test]
    fn assemble_prompt_orders_sections() {
        let p = assemble_prompt(
            Some("You are a Reviewer."),
            "Find the bug.",
            Some("Output format:\nREVIEW_PASS or REVIEW_FAIL"),
            Some("Context from previous steps:\n<diff>"),
        );
        assert_eq!(
            p,
            "You are a Reviewer.\n\nFind the bug.\n\nOutput format:\nREVIEW_PASS or REVIEW_FAIL\n\nContext from previous steps:\n<diff>"
        );
    }

    #[test]
    fn assemble_prompt_omits_empty_sections() {
        assert_eq!(assemble_prompt(None, "Do it.", None, None), "Do it.");
    }

    #[test]
    fn is_known_agent_only_for_recipe_agents() {
        assert!(is_known_agent("claude-code"));
        assert!(is_known_agent("codex"));
        assert!(is_known_agent("aider"));
        assert!(!is_known_agent("bash"));
        assert!(!is_known_agent(""));
    }

    #[test]
    fn build_argv_puts_permission_last() {
        let argv = build_agent_argv("claude-code", Some(Permission::Read), Some("opus"));
        assert_eq!(
            argv,
            vec![
                "--print",
                "--output-format",
                "json",
                "--model",
                "opus",
                "--permission-mode",
                "plan"
            ]
        );
        // permission flags appear AFTER the --model value
        let perm_idx = argv.iter().position(|a| a == "--permission-mode").unwrap();
        let model_idx = argv.iter().position(|a| a == "opus").unwrap();
        assert!(perm_idx > model_idx);
    }

    #[test]
    fn build_argv_omits_model_and_permission_when_absent() {
        let argv = build_agent_argv("codex", None, None);
        assert_eq!(argv, vec!["exec", "--json"]);
    }

    #[test]
    fn normalize_claude_json_extracts_result_and_session() {
        let stdout = r#"{"type":"result","result":"All fixed.","is_error":false,"session_id":"abc123","total_cost_usd":0.012}"#;
        let n = normalize_output("claude-code", stdout);
        assert_eq!(n.text, "All fixed.");
        assert_eq!(n.session_id.as_deref(), Some("abc123"));
        assert!(!n.is_error);
    }

    #[test]
    fn normalize_codex_jsonl_takes_last_agent_message() {
        let stdout = "{\"type\":\"thread.started\",\"thread_id\":\"t1\"}\n{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"Done.\"}}\n";
        let n = normalize_output("codex", stdout);
        assert_eq!(n.text, "Done.");
        assert_eq!(n.session_id.as_deref(), Some("t1"));
    }

    #[test]
    fn normalize_unknown_agent_falls_back_to_raw() {
        let n = normalize_output("aider", "raw model output\nmore");
        assert_eq!(n.text, "raw model output\nmore");
        assert_eq!(n.session_id, None);
        assert!(!n.is_error);
    }

    #[test]
    fn normalize_claude_unparseable_falls_back_to_raw() {
        let n = normalize_output("claude-code", "not json at all");
        assert_eq!(n.text, "not json at all");
        assert!(!n.is_error);
    }

    #[test]
    fn normalize_claude_valid_json_without_result_falls_back_to_raw() {
        // Valid JSON, but no string `result` field → raw fallback (not empty text).
        let stdout = r#"{"type":"system","session_id":"abc123"}"#;
        let n = normalize_output("claude-code", stdout);
        assert_eq!(n.text, stdout);
        assert_eq!(n.session_id, None);
        assert!(!n.is_error);
    }

    #[test]
    fn normalize_codex_thread_started_only_falls_back_to_raw() {
        // thread_id but no agent_message → raw fallback (not empty text).
        let stdout = r#"{"type":"thread.started","thread_id":"t1"}"#;
        let n = normalize_output("codex", stdout);
        assert_eq!(n.text, stdout);
        assert_eq!(n.session_id, None);
        assert!(!n.is_error);
    }
}
