//! Tests for the `nodes` subcommand surface (ls/show with filters).

use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn nodes_ls_lists_all_eight_builtins() {
    Command::cargo_bin("ordius-cli")
        .unwrap()
        .args(["nodes", "ls"])
        .assert()
        .success()
        .stdout(predicate::str::contains("checkpoint"))
        .stdout(predicate::str::contains("condition"))
        .stdout(predicate::str::contains("delay"))
        .stdout(predicate::str::contains("file"))
        .stdout(predicate::str::contains("http"))
        .stdout(predicate::str::contains("llm"))
        .stdout(predicate::str::contains("shell"))
        .stdout(predicate::str::contains("transform"));
}

#[test]
fn nodes_ls_filters_by_category_control() {
    Command::cargo_bin("ordius-cli")
        .unwrap()
        .args(["nodes", "ls", "--category", "control"])
        .assert()
        .success()
        .stdout(predicate::str::contains("delay"))
        .stdout(predicate::str::contains("condition"))
        .stdout(predicate::str::contains("checkpoint"))
        .stdout(predicate::str::contains("shell").not())
        .stdout(predicate::str::contains("http").not());
}

#[test]
fn nodes_ls_filters_by_category_llm() {
    Command::cargo_bin("ordius-cli")
        .unwrap()
        .args(["nodes", "ls", "--category", "llm"])
        .assert()
        .success()
        .stdout(predicate::str::contains("llm"))
        // shell is execution, not llm — should be filtered out.
        .stdout(predicate::str::contains("shell").not());
}

#[test]
fn nodes_ls_filters_by_category_execution() {
    Command::cargo_bin("ordius-cli")
        .unwrap()
        .args(["nodes", "ls", "--category", "execution"])
        .assert()
        .success()
        .stdout(predicate::str::contains("shell"))
        .stdout(predicate::str::contains("delay").not());
}

#[test]
fn nodes_ls_category_match_is_case_insensitive() {
    Command::cargo_bin("ordius-cli")
        .unwrap()
        .args(["nodes", "ls", "--category", "Control"])
        .assert()
        .success()
        .stdout(predicate::str::contains("delay"));
}

#[test]
fn nodes_ls_unknown_category_returns_empty() {
    Command::cargo_bin("ordius-cli")
        .unwrap()
        .args(["nodes", "ls", "--category", "ghost"])
        .assert()
        .success()
        .stdout(predicate::str::contains("shell").not())
        .stdout(predicate::str::contains("delay").not());
}

#[test]
fn nodes_ls_json_emits_array() {
    let out = Command::cargo_bin("ordius-cli")
        .unwrap()
        .args(["--json", "nodes", "ls"])
        .output()
        .unwrap();
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).unwrap();
    let v: serde_json::Value = serde_json::from_str(&stdout).expect("valid JSON array");
    let arr = v.as_array().expect("top-level array");
    assert_eq!(arr.len(), 8, "all 8 builtins should be present");
    // Confirm each entry has the documented NodeType shape.
    for nt in arr {
        assert!(nt.get("id").is_some());
        assert!(nt.get("name").is_some());
        assert!(nt.get("category").is_some());
        assert!(nt.get("execution").is_some());
    }
}

#[test]
fn nodes_show_prints_full_spec_as_json() {
    Command::cargo_bin("ordius-cli")
        .unwrap()
        .args(["nodes", "show", "shell"])
        .assert()
        .success()
        .stdout(predicate::str::contains(r#""id": "shell""#))
        .stdout(predicate::str::contains(r#""category": "execution""#))
        .stdout(predicate::str::contains(r#""backend": "subprocess""#));
}

#[test]
fn nodes_show_unknown_errors() {
    Command::cargo_bin("ordius-cli")
        .unwrap()
        .args(["nodes", "show", "ghost"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unknown node type"));
}

#[test]
fn nodes_ls_tag_filter_with_no_match_returns_empty() {
    // None of the v1.0 builtins declare tags, so any --tag filter
    // should hide every entry from the table.
    Command::cargo_bin("ordius-cli")
        .unwrap()
        .args(["nodes", "ls", "--tag", "gpu"])
        .assert()
        .success()
        .stdout(predicate::str::contains("delay").not())
        .stdout(predicate::str::contains("shell").not());
}
