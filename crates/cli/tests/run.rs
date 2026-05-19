//! Tests for the `run` subcommand.

use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use tempfile::TempDir;

const DELAY_WORKFLOW: &str = r#"{
  "id": "demo",
  "name": "Demo",
  "schema_version": 1,
  "nodes": [
    {"id": "n", "type": "delay", "name": "wait", "config": {"ms": 20}}
  ],
  "edges": []
}"#;

fn cli(home: &TempDir) -> Command {
    let mut cmd = Command::cargo_bin("ordius-cli").unwrap();
    cmd.env("ORDIUS_TEST_KEYRING", "1")
        .args(["--home", home.path().to_str().unwrap()]);
    cmd
}

fn seed(home: &TempDir, id: &str, body: &str) {
    let dir = home.path().join("workflows");
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join(format!("{id}.json")), body).unwrap();
}

#[test]
fn run_succeeds_with_status_line() {
    let home = TempDir::new().unwrap();
    seed(&home, "demo", DELAY_WORKFLOW);
    cli(&home)
        .args(["run", "demo"])
        .assert()
        .success()
        .stdout(predicate::str::contains("done:"))
        .stdout(predicate::str::contains("1 node runs"));
}

#[test]
fn run_missing_workflow_errors_nonzero() {
    let home = TempDir::new().unwrap();
    cli(&home).args(["run", "ghost"]).assert().failure();
}

#[test]
fn run_invalid_workflow_exits_two() {
    let home = TempDir::new().unwrap();
    let cyclic = r#"{
      "id": "cyc",
      "name": "Cyclic",
      "schema_version": 1,
      "nodes": [
        {"id": "a", "type": "delay", "name": "a", "config": {"ms": 1}},
        {"id": "b", "type": "delay", "name": "b", "config": {"ms": 1}}
      ],
      "edges": [
        {"id": "e1", "from_node_id": "a", "from_port": "x", "to_node_id": "b", "to_port": "y", "edge_type": "forward"},
        {"id": "e2", "from_node_id": "b", "from_port": "x", "to_node_id": "a", "to_port": "y", "edge_type": "forward"}
      ]
    }"#;
    seed(&home, "cyc", cyclic);
    cli(&home)
        .args(["run", "cyc"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("validation failed"));
}

#[test]
fn run_json_events_emits_ndjson() {
    let home = TempDir::new().unwrap();
    seed(&home, "demo", DELAY_WORKFLOW);
    let out = cli(&home)
        .args(["run", "demo", "--json-events"])
        .output()
        .unwrap();
    assert!(out.status.success(), "exit: {}", out.status);
    let stdout = String::from_utf8(out.stdout).unwrap();
    let mut saw_workflow_started = false;
    let mut saw_workflow_done = false;
    for line in stdout.lines() {
        let v: serde_json::Value =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("line {line:?} not JSON: {e}"));
        let ty = v.get("type").and_then(|t| t.as_str()).unwrap();
        // Common envelope fields per spec.
        assert!(v.get("seq").is_some());
        assert!(v.get("emitted_at").is_some());
        assert!(v.get("run_id").is_some());
        if ty == "workflow:started" {
            saw_workflow_started = true;
            assert_eq!(v.get("workflow_id").and_then(|x| x.as_str()), Some("demo"));
            assert_eq!(v.get("trigger_kind").and_then(|x| x.as_str()), Some("cli"));
        }
        if ty == "workflow:done" {
            saw_workflow_done = true;
        }
    }
    assert!(saw_workflow_started, "missing workflow:started event");
    assert!(saw_workflow_done, "missing workflow:done event");
}

// Workflow used by the --var / --vars-file tests below. The
// transform.template node interpolates `{{vars.GREETING}}`; the
// run succeeds only when GREETING is provided.
const TEMPLATE_WORKFLOW: &str = r#"{
  "id": "tmpl",
  "name": "Template",
  "schema_version": 1,
  "nodes": [
    {
      "id": "tx",
      "type": "transform",
      "name": "tx",
      "config": {"op": "template", "template": "hi {{vars.GREETING}}"}
    }
  ],
  "edges": []
}"#;

#[test]
fn run_with_var_kv_resolves_template() {
    let home = TempDir::new().unwrap();
    seed(&home, "tmpl", TEMPLATE_WORKFLOW);
    cli(&home)
        .args(["run", "tmpl", "--var", "GREETING=world"])
        .assert()
        .success()
        .stdout(predicate::str::contains("done:"));
}

#[test]
fn run_without_required_var_fails() {
    // No --var, so the {{vars.GREETING}} substitution errors —
    // transform returns NodeError, run finalises as "error", exit
    // 1. Failing this assertion means vars are being silently
    // resolved as empty strings rather than loud-failing.
    let home = TempDir::new().unwrap();
    seed(&home, "tmpl", TEMPLATE_WORKFLOW);
    cli(&home)
        .args(["run", "tmpl"])
        .assert()
        .code(1)
        .stdout(predicate::str::contains("error:"));
}

#[test]
fn run_vars_file_loads_json() {
    let home = TempDir::new().unwrap();
    seed(&home, "tmpl", TEMPLATE_WORKFLOW);
    let vars_file = home.path().join("vars.json");
    fs::write(&vars_file, r#"{"GREETING": "fromfile"}"#).unwrap();
    cli(&home)
        .args(["run", "tmpl", "--vars-file", vars_file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("done:"));
}

#[test]
fn run_yes_short_circuits_checkpoint() {
    let home = TempDir::new().unwrap();
    let cp = r#"{
      "id": "cp",
      "name": "CP",
      "schema_version": 1,
      "nodes": [
        {"id": "wait", "type": "checkpoint", "name": "wait", "config": {}}
      ],
      "edges": []
    }"#;
    seed(&home, "cp", cp);
    // Without --yes this would park forever; assert_cmd will hang.
    cli(&home)
        .args(["run", "cp", "--yes"])
        .timeout(std::time::Duration::from_secs(5))
        .assert()
        .success()
        .stdout(predicate::str::contains("done:"));
}
