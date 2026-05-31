//! End-to-end smoke tests for the `ordius-helper` binary on the test host.

use std::io::Write;
use std::process::{Command, Stdio};

fn helper_path() -> std::path::PathBuf {
    // CARGO_BIN_EXE_<name> resolves to the path of the built binary when
    // running `cargo test`.
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_ordius-helper"))
}

#[test]
fn probe_subcommand_handles_empty_plan() {
    let mut child = Command::new(helper_path())
        .arg("probe")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn helper");
    {
        let mut stdin = child.stdin.take().unwrap();
        stdin
            .write_all(
                br#"{"version":1,"per_resource_timeout_ms":1000,"max_concurrency":1,"overall_budget_ms":5000,"resources":[]}"#,
            )
            .unwrap();
    }
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success(), "helper exited non-zero: {out:?}");
    assert!(out.stdout.is_empty(), "empty plan should emit no lines");
}

#[test]
fn probe_subcommand_reports_missing_binary() {
    let mut child = Command::new(helper_path())
        .arg("probe")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn helper");
    {
        let mut stdin = child.stdin.take().unwrap();
        let plan = r#"{
            "version":1,
            "per_resource_timeout_ms":500,
            "max_concurrency":1,
            "overall_budget_ms":5000,
            "resources":[
                {"id":"absent","kind":{"type":"binary","bin":"definitely_not_a_real_bin_qzqzqz","extra_search_paths":[]}}
            ]
        }"#;
        stdin.write_all(plan.as_bytes()).unwrap();
    }
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success());
    let line = String::from_utf8(out.stdout).unwrap();
    assert!(
        line.contains("\"kind\":\"not_found\""),
        "unexpected: {line}"
    );
}

#[cfg(unix)]
#[test]
fn exec_subcommand_runs_true() {
    // Protocol: write the request as one line, then keep stdin OPEN until the
    // helper exits. Closing stdin early would trip the helper's cancel monitor
    // (stdin-EOF = cancel) and kill the child before it completes.
    let req = r#"{"version":1,"program":"true","args":[]}"#;
    let mut child = Command::new(helper_path())
        .args(["exec", "--argv-json"])
        .stdin(Stdio::piped())
        .spawn()
        .expect("spawn helper");
    let mut stdin = child.stdin.take().unwrap();
    writeln!(stdin, "{req}").unwrap();
    let status = child.wait().expect("wait");
    drop(stdin);
    assert!(status.success());
}

#[cfg(unix)]
#[test]
fn exec_subcommand_propagates_failure() {
    let req = r#"{"version":1,"program":"false","args":[]}"#;
    let mut child = Command::new(helper_path())
        .args(["exec", "--argv-json"])
        .stdin(Stdio::piped())
        .spawn()
        .expect("spawn helper");
    let mut stdin = child.stdin.take().unwrap();
    writeln!(stdin, "{req}").unwrap();
    let status = child.wait().expect("wait");
    drop(stdin);
    assert!(!status.success(), "false should propagate non-zero exit");
}

#[cfg(unix)]
#[test]
fn exec_subcommand_runs_with_explicit_env() {
    // Single-line JSON: the helper reads the request via `read_line`, so the
    // request must not contain embedded newlines.
    let req = r#"{"version":1,"program":"sh","args":["-c","test \"$ORDIUS_HELPER_TEST_VAR\" = \"set\""],"env":{"ORDIUS_HELPER_TEST_VAR":"set"}}"#;
    let mut child = Command::new(helper_path())
        .args(["exec", "--argv-json"])
        .stdin(Stdio::piped())
        .spawn()
        .expect("spawn helper");
    let mut stdin = child.stdin.take().unwrap();
    writeln!(stdin, "{req}").unwrap();
    let status = child.wait().expect("wait");
    drop(stdin);
    assert!(status.success(), "explicit env not propagated");
}

#[cfg(unix)]
#[test]
fn exec_subcommand_finds_sh_with_empty_env() {
    let req = r#"{"version":1,"program":"sh","args":["-c","printf ok"]}"#;
    let mut child = Command::new(helper_path())
        .args(["exec", "--argv-json"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn helper");
    let mut stdin = child.stdin.take().unwrap();
    writeln!(stdin, "{req}").unwrap();
    // Hold stdin open until the helper has produced its output and exited;
    // closing it early would trip the cancel monitor (stdin-EOF = cancel).
    let out = child.wait_with_output().expect("wait");
    drop(stdin);
    assert!(out.status.success(), "helper failed: {out:?}");
    assert_eq!(String::from_utf8(out.stdout).unwrap(), "ok");
}

#[cfg(unix)]
#[test]
fn exec_subcommand_explicit_path_overrides_default_path() {
    let req = r#"{"version":1,"program":"sh","args":["-c","test \"$PATH\" = \"/custom/bin\""],"env":{"PATH":"/custom/bin"}}"#;
    let mut child = Command::new(helper_path())
        .args(["exec", "--argv-json"])
        .stdin(Stdio::piped())
        .spawn()
        .expect("spawn helper");
    let mut stdin = child.stdin.take().unwrap();
    writeln!(stdin, "{req}").unwrap();
    let status = child.wait().expect("wait");
    drop(stdin);
    assert!(status.success(), "explicit PATH was not preserved");
}
