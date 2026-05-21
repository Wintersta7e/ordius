//! Tests for the `runs` subcommand surface (ls/show/logs/rm).
//!
//! Each test seeds a real workflow + executes it via `run`, then
//! exercises the runs surface against the resulting `SQLite` state.
//! This trades a bit of test wall time for end-to-end coverage —
//! seeding the DB by hand would skip the recorder + emitter flow.

use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use tempfile::TempDir;

const DEMO_WORKFLOW: &str = r#"{
  "id": "demo",
  "name": "Demo",
  "schema_version": 1,
  "nodes": [
    {"id": "n", "type": "delay", "name": "wait", "config": {"ms": 10}}
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

fn run_once(home: &TempDir, id: &str) {
    cli(home).args(["run", id]).assert().success();
}

#[test]
fn runs_ls_empty_db_prints_placeholder() {
    let home = TempDir::new().unwrap();
    cli(&home)
        .args(["runs", "ls"])
        .assert()
        .success()
        .stdout(predicate::str::contains("no runs yet"));
}

#[test]
fn runs_ls_shows_recent_run() {
    let home = TempDir::new().unwrap();
    seed(&home, "demo", DEMO_WORKFLOW);
    run_once(&home, "demo");
    cli(&home)
        .args(["runs", "ls"])
        .assert()
        .success()
        .stdout(predicate::str::contains("demo"))
        .stdout(predicate::str::contains("done"));
}

#[test]
fn runs_ls_filters_by_status() {
    let home = TempDir::new().unwrap();
    seed(&home, "demo", DEMO_WORKFLOW);
    run_once(&home, "demo");
    cli(&home)
        .args(["runs", "ls", "--status", "done"])
        .assert()
        .success()
        .stdout(predicate::str::contains("demo"));
    cli(&home)
        .args(["runs", "ls", "--status", "error"])
        .assert()
        .success()
        .stdout(predicate::str::contains("no runs yet"));
}

#[test]
fn runs_ls_json_emits_array() {
    let home = TempDir::new().unwrap();
    seed(&home, "demo", DEMO_WORKFLOW);
    run_once(&home, "demo");
    let out = cli(&home).args(["--json", "runs", "ls"]).output().unwrap();
    assert!(out.status.success());
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("valid JSON");
    let arr = v.as_array().expect("top-level array");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["workflow_id"], "demo");
    assert_eq!(arr[0]["status"], "done");
}

#[test]
fn runs_show_prints_run_and_node_rows() {
    let home = TempDir::new().unwrap();
    seed(&home, "demo", DEMO_WORKFLOW);
    run_once(&home, "demo");

    let out = cli(&home).args(["--json", "runs", "ls"]).output().unwrap();
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let run_id = v[0]["run_id"].as_str().unwrap().to_string();

    cli(&home)
        .args(["runs", "show", &run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("workflow_id demo"))
        .stdout(predicate::str::contains("status      done"))
        .stdout(predicate::str::contains("done"));
}

#[test]
fn runs_show_missing_errors() {
    let home = TempDir::new().unwrap();
    cli(&home)
        .args(["runs", "show", "00000000-0000-0000-0000-000000000000"])
        .assert()
        .failure();
}

#[test]
fn runs_logs_emits_ndjson_for_completed_run() {
    let home = TempDir::new().unwrap();
    seed(&home, "demo", DEMO_WORKFLOW);
    run_once(&home, "demo");

    let out = cli(&home).args(["--json", "runs", "ls"]).output().unwrap();
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let run_id = v[0]["run_id"].as_str().unwrap().to_string();

    let logs = cli(&home).args(["runs", "logs", &run_id]).output().unwrap();
    assert!(logs.status.success());
    let stdout = String::from_utf8(logs.stdout).unwrap();
    let mut saw_done = false;
    for line in stdout.lines() {
        let ev: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("logs line {line:?} not JSON: {e}"));
        assert!(ev.get("seq").is_some());
        assert!(ev.get("run_id").is_some());
        assert!(ev.get("type").is_some());
        if ev["type"] == "workflow:done" {
            saw_done = true;
        }
    }
    assert!(saw_done, "expected workflow:done in run_events");
}

#[test]
fn runs_rm_with_force_deletes_row() {
    let home = TempDir::new().unwrap();
    seed(&home, "demo", DEMO_WORKFLOW);
    run_once(&home, "demo");

    let out = cli(&home).args(["--json", "runs", "ls"]).output().unwrap();
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let run_id = v[0]["run_id"].as_str().unwrap().to_string();

    cli(&home)
        .args(["runs", "rm", &run_id, "--force"])
        .assert()
        .success()
        .stdout(predicate::str::contains("removed"));

    // Verify gone from ls
    cli(&home)
        .args(["runs", "ls"])
        .assert()
        .success()
        .stdout(predicate::str::contains("no runs yet"));
}

#[test]
fn runs_rm_missing_errors() {
    let home = TempDir::new().unwrap();
    cli(&home)
        .args([
            "runs",
            "rm",
            "00000000-0000-0000-0000-000000000000",
            "--force",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("not found"));
}

#[test]
fn runs_rm_removes_output_cache_spill_dir() {
    let home = TempDir::new().unwrap();
    seed(&home, "demo", DEMO_WORKFLOW);
    run_once(&home, "demo");

    let out = cli(&home).args(["--json", "runs", "ls"]).output().unwrap();
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let run_id = v[0]["run_id"].as_str().unwrap().to_string();

    // Simulate a large-output spill the engine would write itself for
    // any node whose serialised output exceeds the in-DB threshold.
    let spill_dir = home.path().join("output-cache").join(&run_id);
    fs::create_dir_all(&spill_dir).unwrap();
    fs::write(spill_dir.join("n-out-0-0.json"), b"\"hello\"").unwrap();
    assert!(spill_dir.exists());

    cli(&home)
        .args(["runs", "rm", &run_id, "--force"])
        .assert()
        .success()
        .stdout(predicate::str::contains("+ spill dir"));

    assert!(
        !spill_dir.exists(),
        "spill dir {} should have been removed",
        spill_dir.display(),
    );
}
