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
//! model also mean no permission flags are appended. The boot probe never
//! caches a real `aider` because the workflow-scoped resource points the probe
//! at the stub path, and `opportunistic_reprobe` is what resolves it.
//! Claude/codex dialect parsing is covered by the `coding_agent` unit tests.

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

/// A workflow-scoped resource definition whose binary probe resolves the stub
/// and advertises `cli_agent_print`. Uses the `aider` id: a known agent (so the
/// executor's known-agent gate admits it) with no structured-output dialect, so
/// the result takes `normalize_output`'s raw-passthrough path. The probe points
/// at the stub path, so the boot probe resolves the stub rather than any real
/// on-PATH `aider`.
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
        // `aider` is a builtin resource at a lower scope; overriding it points
        // the probe at the stub instead of any real on-PATH aider.
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
        resources: vec![stub_resource_def(&stub.to_string_lossy())],
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
