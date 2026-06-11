//! End-to-end smoke: the `coding-agent` built-in resolves a detected CLI
//! agent resource to its binary, builds the print-mode argv, feeds the
//! assembled prompt over stdin, runs it via the Local dispatcher, and
//! normalizes the agent's structured output onto the `text` / `session_id`
//! ports.
//!
//! The "agent" here is a stub shell script: on `--version` it prints a
//! version line (so the binary probe resolves it + proves `CliAgentPrint`
//! via the resource's advertised capabilities); on any other invocation it
//! reads + discards stdin and prints one canned result line.
//!
//! The resource uses the `aider` id: a *known* agent (so the executor's
//! known-agent gate admits it) that has no structured-output dialect, so
//! `normalize_output` takes the raw-passthrough path and `text` is the stub's
//! stdout verbatim. `aider`'s recipe (`--message`) and absence of a sandbox
//! model also mean no permission flags are appended.
//!
//! The happy-path stub is registered at the **user-global** scope (a trusted
//! scope), because a coding-agent only runs resources defined at a trusted
//! scope (built-in or user-global). The user-global definition shadows the
//! built-in `aider` (so the probe points at the stub rather than any real
//! on-PATH `aider`), and `opportunistic_reprobe` is what resolves it.
//! Claude/codex dialect parsing is covered by the `coding_agent` unit tests.
//!
//! A second test asserts the security gate: a *workflow-scoped* override of a
//! known agent id is rejected at runtime, so an imported workflow can't repoint
//! a known agent id at an arbitrary binary.

#![cfg(unix)]

use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use ordius_engine::Engine;
use ordius_engine::environment::runtime::resource::ResourceDefinition;
use ordius_engine::types::{Node, Pos, Trigger, Workflow};
use tempfile::TempDir;

/// Write an executable stub that doubles as the version probe target and the
/// agent itself. Returns its absolute path.
fn write_stub_agent(dir: &std::path::Path) -> std::path::PathBuf {
    let script = dir.join("stub-claude.sh");
    std::fs::write(
        &script,
        r#"#!/usr/bin/env bash
if [ "$1" = "--version" ]; then
  echo "1.2.3 (stub)"
  exit 0
fi
# Real invocation: drain + discard stdin, emit one canned result line.
cat >/dev/null
echo 'hello from stub'
"#,
    )
    .unwrap();
    let mut perms = std::fs::metadata(&script).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script, perms).unwrap();
    script
}

/// Write a `resources.toml` in the engine home that registers the stub under
/// the `aider` id at **user-global** scope. `override_lower_scope = true`
/// lets it shadow the built-in `aider` so the probe resolves the stub instead
/// of any real on-PATH `aider`. User-global is a trusted scope, so the
/// coding-agent gate admits it.
fn write_user_global_aider(home: &std::path::Path, stub_path: &str) {
    let body = format!(
        r#"
[[resource]]
id = "aider"
override_lower_scope = true
kind = "binary"
advertised_capabilities = ["cli_agent_print"]

[resource.probe]
kind = "binary"
bin = "{stub_path}"
version_args = ["--version"]
version_regex = '^(\S+)'
"#
    );
    std::fs::write(home.join("resources.toml"), body).expect("write resources.toml");
}

/// A workflow-scoped resource definition whose binary probe resolves the stub
/// and advertises `cli_agent_print`, overriding the known `aider` id. Used by
/// the negative test: at runtime this is rejected because workflow scope is not
/// a trusted scope for coding agents.
fn stub_resource_def(stub_path: &str) -> ResourceDefinition {
    serde_json::from_value(serde_json::json!({
        "id": "aider",
        "kind": "binary",
        "advertised_capabilities": ["cli_agent_print"],
        "probe": {
            "kind": "binary",
            "bin": stub_path,
            "version_args": ["--version"],
            "version_regex": r"^(\S+)",
        },
        "override_lower_scope": true,
    }))
    .expect("stub resource def deserializes")
}

fn coding_agent_node() -> Node {
    let mut config = HashMap::new();
    config.insert("agent".into(), serde_json::json!("aider"));
    config.insert("prompt".into(), serde_json::json!("do the thing"));
    Node {
        id: "a".into(),
        ty: "coding-agent".into(),
        name: String::new(),
        config,
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
        target_env: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coding_agent_runs_stub_and_normalizes_output() {
    let dir = TempDir::new().unwrap();
    let stub = write_stub_agent(dir.path());
    // Register the stub at the trusted user-global scope BEFORE Engine::new so
    // the boot loader seeds it into the UserGlobal registry layer.
    write_user_global_aider(dir.path(), &stub.to_string_lossy());

    let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());

    let wf = Workflow {
        id: "p-agent".into(),
        name: "p-agent".into(),
        schema_version: 1,
        created_at: None,
        updated_at: None,
        variables: HashMap::new(),
        triggers: vec![Trigger::Manual],
        nodes: vec![coding_agent_node()],
        edges: vec![],
        resources: vec![],
        default_env: None,
    };

    let summary = engine
        .run_workflow(Arc::new(wf), HashMap::new(), "test", false, None)
        .await
        .expect("coding-agent run");
    assert_eq!(summary.status, "done", "run should finish ok");

    // value_inline holds the JSON-serialised PortValue. A String port is a
    // JSON string literal with surrounding quotes. The unknown-agent raw
    // passthrough trims trailing whitespace and emits stdout verbatim.
    let conn = engine.pool().get().unwrap();
    let text: String = conn
        .query_row(
            "SELECT value_inline FROM node_outputs \
             WHERE run_id=? AND node_id='a' AND port_name='text'",
            rusqlite::params![&summary.run_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(text, "\"hello from stub\"", "agent stdout on the text port");

    // exit_code is always populated (the stub exits 0).
    let exit: String = conn
        .query_row(
            "SELECT value_inline FROM node_outputs \
             WHERE run_id=? AND node_id='a' AND port_name='exit_code'",
            rusqlite::params![&summary.run_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(exit, "0.0", "stub exits 0");

    // Raw passthrough (unknown agent dialect) yields no session_id port.
    let session_rows: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM node_outputs \
             WHERE run_id=? AND node_id='a' AND port_name='session_id'",
            rusqlite::params![&summary.run_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(session_rows, 0, "no session_id for raw-passthrough dialect");
}

/// Security gate: a workflow-scoped resource that overrides a known agent id
/// (`aider`) to point at an arbitrary binary must be REJECTED at runtime.
/// Workflow scope is not a trusted scope for coding agents, so an imported
/// workflow can't repoint a known agent id at an attacker-chosen binary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coding_agent_rejects_workflow_scoped_override() {
    let dir = TempDir::new().unwrap();
    let stub = write_stub_agent(dir.path());

    let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());

    let wf = Workflow {
        id: "p-agent-untrusted".into(),
        name: "p-agent-untrusted".into(),
        schema_version: 1,
        created_at: None,
        updated_at: None,
        variables: HashMap::new(),
        triggers: vec![Trigger::Manual],
        nodes: vec![coding_agent_node()],
        edges: vec![],
        // Workflow-scoped override of `aider` → the untrusted-scope gate fires.
        resources: vec![stub_resource_def(&stub.to_string_lossy())],
        default_env: None,
    };

    let summary = engine
        .run_workflow(Arc::new(wf), HashMap::new(), "test", false, None)
        .await
        .expect("run completes (the node fails, not the engine)");
    assert_ne!(
        summary.status, "done",
        "run must not succeed with a workflow-scoped agent override"
    );

    // The node should have failed with the untrusted-scope error.
    let conn = engine.pool().get().unwrap();
    let err: Option<String> = conn
        .query_row(
            "SELECT error FROM node_runs \
             WHERE run_id=? AND node_id='a' \
             ORDER BY iteration DESC, attempt DESC LIMIT 1",
            rusqlite::params![&summary.run_id],
            |r| r.get(0),
        )
        .unwrap();
    let err = err.expect("node recorded an error");
    assert!(
        err.contains("untrusted scope"),
        "node error should cite the untrusted scope, got: {err}"
    );
}
