//! Tests for the `workflows` subcommand surface (ls/show/validate/rm).
//!
//! Each test seeds a fresh `TempDir` as `--home`, optionally drops a
//! workflow file into `<home>/workflows/`, and asserts the CLI's
//! observable surface (exit code, stdout, stderr).

use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use tempfile::TempDir;

const DEMO_JSON: &str = r#"{
  "id": "demo",
  "name": "Demo Workflow",
  "schema_version": 1,
  "triggers": [{"type": "manual"}],
  "nodes": [
    {"id": "n", "type": "delay", "name": "wait", "config": {"ms": 10}}
  ],
  "edges": []
}"#;

fn seed(home: &TempDir, id: &str, body: &str) {
    let wf_dir = home.path().join("workflows");
    fs::create_dir_all(&wf_dir).unwrap();
    fs::write(wf_dir.join(format!("{id}.json")), body).unwrap();
}

fn cli(home: &TempDir) -> Command {
    let mut cmd = Command::cargo_bin("ordius-cli").unwrap();
    cmd.args(["--home", home.path().to_str().unwrap()]);
    cmd
}

#[test]
fn workflows_ls_on_empty_home_returns_zero() {
    let home = TempDir::new().unwrap();
    cli(&home)
        .args(["workflows", "ls"])
        .assert()
        .success()
        .stdout(predicate::str::contains("no workflows"));
}

#[test]
fn workflows_ls_shows_imported_workflow() {
    let home = TempDir::new().unwrap();
    seed(&home, "demo", DEMO_JSON);
    cli(&home)
        .args(["workflows", "ls"])
        .assert()
        .success()
        .stdout(predicate::str::contains("demo"))
        .stdout(predicate::str::contains("Demo Workflow"));
}

#[test]
fn workflows_ls_skips_non_json_files() {
    let home = TempDir::new().unwrap();
    seed(&home, "demo", DEMO_JSON);
    fs::write(home.path().join("workflows/scratch.txt"), "not a workflow").unwrap();
    cli(&home)
        .args(["workflows", "ls"])
        .assert()
        .success()
        .stdout(predicate::str::contains("demo"))
        .stdout(predicate::str::contains("scratch").not());
}

#[test]
fn workflows_show_prints_json() {
    let home = TempDir::new().unwrap();
    seed(&home, "demo", DEMO_JSON);
    cli(&home)
        .args(["workflows", "show", "demo"])
        .assert()
        .success()
        .stdout(predicate::str::contains(r#""id": "demo""#))
        .stdout(predicate::str::contains(r#""type": "delay""#));
}

#[test]
fn workflows_show_missing_errors() {
    let home = TempDir::new().unwrap();
    cli(&home)
        .args(["workflows", "show", "ghost"])
        .assert()
        .failure();
}

#[test]
fn workflows_validate_by_id_succeeds() {
    let home = TempDir::new().unwrap();
    seed(&home, "demo", DEMO_JSON);
    cli(&home)
        .args(["workflows", "validate", "demo"])
        .assert()
        .success()
        .stdout(predicate::str::contains("ok"));
}

#[test]
fn workflows_validate_by_path_succeeds() {
    let home = TempDir::new().unwrap();
    seed(&home, "demo", DEMO_JSON);
    let path = home.path().join("workflows/demo.json");
    cli(&home)
        .args(["workflows", "validate", path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("ok"));
}

#[test]
fn workflows_validate_forward_cycle_exits_two() {
    let home = TempDir::new().unwrap();
    let bad = r#"{
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
    seed(&home, "cyc", bad);
    cli(&home)
        .args(["workflows", "validate", "cyc"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("validation error"));
}

#[test]
fn workflows_rm_with_force_removes_file() {
    let home = TempDir::new().unwrap();
    seed(&home, "demo", DEMO_JSON);
    let path = home.path().join("workflows/demo.json");
    assert!(path.exists());
    cli(&home)
        .args(["workflows", "rm", "demo", "--force"])
        .assert()
        .success()
        .stdout(predicate::str::contains("removed demo"));
    assert!(!path.exists());
}

#[test]
fn workflows_rm_yes_prompt_removes_file() {
    let home = TempDir::new().unwrap();
    seed(&home, "demo", DEMO_JSON);
    cli(&home)
        .args(["workflows", "rm", "demo"])
        .write_stdin("y\n")
        .assert()
        .success();
    assert!(!home.path().join("workflows/demo.json").exists());
}

#[test]
fn workflows_rm_empty_response_aborts() {
    let home = TempDir::new().unwrap();
    seed(&home, "demo", DEMO_JSON);
    cli(&home)
        .args(["workflows", "rm", "demo"])
        .write_stdin("\n")
        .assert()
        .code(1)
        .stderr(predicate::str::contains("aborted"));
    assert!(home.path().join("workflows/demo.json").exists());
}

#[test]
fn workflows_rm_missing_errors() {
    let home = TempDir::new().unwrap();
    cli(&home)
        .args(["workflows", "rm", "ghost", "--force"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("not found"));
}
