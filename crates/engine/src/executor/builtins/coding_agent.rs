//! `coding-agent` built-in: run a detected CLI coding agent (claude/codex/…)
//! in non-interactive ("print") mode via the env dispatcher. Prompt over
//! stdin, result on the `text` port. Pure helpers here; the executor branch
//! lives in `executor/subprocess.rs`.

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

/// Flags a user may NOT smuggle through `extra_flags`: anything that changes
/// the agent's permission/sandbox posture or loads config. The node's
/// `permission` field is the single source of truth for sandboxing, so a
/// shared/imported workflow can't display `permission: read` while silently
/// escalating via `extra_flags`. Matched case-sensitively (Unix short flags
/// are case-sensitive — `-C` is codex's cwd flag, distinct from `-c` config)
/// against each token's name portion (before any `=`). Entries use each CLI's
/// real spelling (e.g. claude's camelCase `--allowedTools`).
const DENIED_EXTRA_FLAGS: &[&str] = &[
    "--dangerously-skip-permissions",
    "--dangerously-bypass-approvals-and-sandbox",
    "--permission-mode",
    "--permission-prompt-tool",
    "--allowedTools",
    "--disallowedTools",
    "--sandbox",
    "--ask-for-approval",
    "--full-auto",
    "--trust-all-tools",
    "-a",
    "-c",
    "--config",
];

/// Gate on user-supplied extra flags: (1) a character allowlist blocking shell
/// metacharacters, and (2) a denylist rejecting permission/sandbox/config
/// overrides (see [`DENIED_EXTRA_FLAGS`]). Returns the split tokens.
#[allow(unreachable_pub, dead_code)]
pub fn sanitize_extra_flags(raw: &str) -> Result<Vec<String>, String> {
    let ok = raw
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || " -_=./:@,".contains(c));
    if !ok {
        return Err(format!(
            "coding-agent: extra_flags contains disallowed characters: {raw:?}"
        ));
    }
    let tokens: Vec<String> = raw.split_whitespace().map(str::to_string).collect();
    for tok in &tokens {
        let name = tok.split('=').next().unwrap_or(tok);
        if DENIED_EXTRA_FLAGS.contains(&name) {
            return Err(format!(
                "coding-agent: extra_flags may not override permission/sandbox flags ({tok:?}); \
                 use the node's permission field instead"
            ));
        }
    }
    Ok(tokens)
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
    Some(AgentOutput {
        text: v
            .get("result")
            .and_then(|r| r.as_str())
            .unwrap_or_default()
            .to_string(),
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
    match (text, session_id) {
        (Some(t), sid) => Some(AgentOutput {
            text: t,
            session_id: sid,
            is_error: false,
        }),
        (None, sid @ Some(_)) => Some(AgentOutput {
            text: String::new(),
            session_id: sid,
            is_error: false,
        }),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn sanitize_extra_flags_accepts_safe_rejects_metachars() {
        assert!(sanitize_extra_flags("--model gpt-5.5 -C ./x").is_ok());
        assert!(sanitize_extra_flags("--foo; rm -rf /").is_err());
        assert!(sanitize_extra_flags("$(whoami)").is_err());
    }

    #[test]
    fn sanitize_extra_flags_rejects_permission_escalation() {
        // A workflow must not silently override the node's permission via extra_flags.
        assert!(sanitize_extra_flags("--dangerously-skip-permissions").is_err());
        assert!(sanitize_extra_flags("--dangerously-bypass-approvals-and-sandbox").is_err());
        assert!(sanitize_extra_flags("--permission-mode bypassPermissions").is_err());
        assert!(sanitize_extra_flags("--permission-mode=acceptEdits").is_err());
        assert!(sanitize_extra_flags("--sandbox workspace-write").is_err());
        assert!(sanitize_extra_flags("--allowedTools Bash").is_err());
        assert!(sanitize_extra_flags("--config foo").is_err());
        assert!(sanitize_extra_flags("-c key=val").is_err());
        // Benign flags still pass.
        assert!(sanitize_extra_flags("--model gpt-5.5 -C ./repo --verbose").is_ok());
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
}
