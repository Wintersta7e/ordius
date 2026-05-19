//! Tests for the `import` / `export` subcommands.

use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use tempfile::TempDir;

const DEMO_JSON: &str = r#"{
  "id": "demo",
  "name": "Demo",
  "schema_version": 1,
  "nodes": [
    {"id": "n", "type": "delay", "name": "wait", "config": {"ms": 10}}
  ],
  "edges": []
}"#;

const DEMO_YAML: &str = r"
id: yaml-demo
name: YAML Demo
schema_version: 1
nodes:
  - id: n
    type: delay
    name: wait
    config:
      ms: 5
edges: []
";

fn cli(home: &TempDir) -> Command {
    let mut cmd = Command::cargo_bin("ordius-cli").unwrap();
    cmd.args(["--home", home.path().to_str().unwrap()]);
    cmd
}

fn seed(home: &TempDir, id: &str, body: &str) {
    let dir = home.path().join("workflows");
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join(format!("{id}.json")), body).unwrap();
}

#[test]
fn export_writes_workflow_json_to_stdout() {
    let home = TempDir::new().unwrap();
    seed(&home, "demo", DEMO_JSON);
    let out = cli(&home).args(["export", "demo"]).output().unwrap();
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("valid JSON");
    assert_eq!(parsed["id"], "demo");
    assert_eq!(parsed["nodes"][0]["id"], "n");
}

#[test]
fn export_missing_workflow_errors() {
    let home = TempDir::new().unwrap();
    cli(&home).args(["export", "ghost"]).assert().failure();
}

#[test]
fn import_json_via_stdin_writes_workflow() {
    let home = TempDir::new().unwrap();
    cli(&home)
        .args(["import"])
        .write_stdin(DEMO_JSON)
        .assert()
        .success()
        .stdout(predicate::str::contains("imported demo"));
    let target = home.path().join("workflows/demo.json");
    assert!(target.exists());
    let body = fs::read_to_string(&target).unwrap();
    assert!(body.contains(r#""id": "demo""#));
}

#[test]
fn import_yaml_via_stdin_writes_workflow() {
    let home = TempDir::new().unwrap();
    cli(&home)
        .args(["import"])
        .write_stdin(DEMO_YAML)
        .assert()
        .success()
        .stdout(predicate::str::contains("imported yaml-demo"));
    let target = home.path().join("workflows/yaml-demo.json");
    assert!(target.exists());
    let body = fs::read_to_string(&target).unwrap();
    assert!(body.contains(r#""id": "yaml-demo""#));
}

#[test]
fn import_with_as_renames_id_on_disk() {
    let home = TempDir::new().unwrap();
    cli(&home)
        .args(["import", "--as", "renamed"])
        .write_stdin(DEMO_JSON)
        .assert()
        .success()
        .stdout(predicate::str::contains("imported renamed"));
    assert!(home.path().join("workflows/renamed.json").exists());
    assert!(!home.path().join("workflows/demo.json").exists());
}

#[test]
fn import_invalid_json_errors() {
    let home = TempDir::new().unwrap();
    cli(&home)
        .args(["import"])
        .write_stdin("{not valid")
        .assert()
        .failure()
        .stderr(predicate::str::contains("parse"));
}

#[test]
fn import_structurally_invalid_workflow_errors() {
    // Empty nodes list → validation rejects with "workflow has no nodes".
    let body = r#"{"id": "empty", "name": "Empty", "schema_version": 1, "nodes": [], "edges": []}"#;
    let home = TempDir::new().unwrap();
    cli(&home)
        .args(["import"])
        .write_stdin(body)
        .assert()
        .failure()
        .stderr(predicate::str::contains("validate"));
}

#[test]
fn export_then_import_round_trips() {
    let home = TempDir::new().unwrap();
    seed(&home, "demo", DEMO_JSON);
    let exported = cli(&home).args(["export", "demo"]).output().unwrap().stdout;
    // Import into a fresh home so we don't overwrite the original.
    let home2 = TempDir::new().unwrap();
    cli(&home2)
        .args(["import"])
        .write_stdin(exported.clone())
        .assert()
        .success();
    // Imported file equals the exported JSON modulo pretty-print whitespace.
    let imported = fs::read_to_string(home2.path().join("workflows/demo.json")).unwrap();
    let a: serde_json::Value = serde_json::from_slice(&exported).unwrap();
    let b: serde_json::Value = serde_json::from_str(&imported).unwrap();
    assert_eq!(a, b);
}
