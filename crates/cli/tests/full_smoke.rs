//! End-to-end smoke test exercising the entire v1.0 CLI surface
//! against a single temporary `ORDIUS_HOME`. Touches every
//! subcommand that doesn't require the GUI binary.

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;

fn cli(home: &TempDir) -> Command {
    let mut cmd = Command::cargo_bin("ordius-cli").unwrap();
    cmd.env("ORDIUS_TEST_KEYRING", "1")
        .args(["--home", home.path().to_str().unwrap()]);
    cmd
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "single end-to-end test that drives the whole CLI surface — splitting it would lose the cross-subcommand state coupling"
)]
fn full_v1_0_surface_smoke() {
    let home = TempDir::new().unwrap();

    // 1. nodes ls --category=control — registry available without
    //    any workflow / db state on disk.
    cli(&home)
        .args(["nodes", "ls", "--category", "control"])
        .assert()
        .success()
        .stdout(predicate::str::contains("delay"))
        .stdout(predicate::str::contains("condition"))
        .stdout(predicate::str::contains("checkpoint"));

    // 2. secrets set TEST_KEY — sidecar must persist the name even
    //    though the sample keyring is per-process. ls reads only the
    //    sidecar, so a separate invocation still sees the name.
    cli(&home)
        .args(["secrets", "set", "TEST_KEY"])
        .write_stdin("hunter2\n")
        .assert()
        .success()
        .stdout(predicate::str::contains("set TEST_KEY"));
    cli(&home)
        .args(["secrets", "ls"])
        .assert()
        .success()
        .stdout(predicate::str::contains("TEST_KEY"));

    // 3. import a YAML workflow from stdin. delay then transform —
    //    transform consumes a workflow variable so the run also
    //    exercises the var path.
    let yaml = r"
id: smoke
name: Smoke
schema_version: 1
nodes:
  - id: wait
    type: delay
    name: wait
    config:
      ms: 5
  - id: tx
    type: transform
    name: tx
    config:
      op: template
      template: 'hello {{vars.SUBJECT}}'
edges:
  - id: e1
    from_node_id: wait
    from_port: out
    to_node_id: tx
    to_port: in
    edge_type: forward
";
    cli(&home)
        .args(["import"])
        .write_stdin(yaml)
        .assert()
        .success()
        .stdout(predicate::str::contains("imported smoke"));

    // 4. workflows ls shows the imported workflow.
    cli(&home)
        .args(["workflows", "ls"])
        .assert()
        .success()
        .stdout(predicate::str::contains("smoke"))
        .stdout(predicate::str::contains("Smoke"));

    // 5. workflows validate (by id) — exit 0, "ok" on stdout.
    cli(&home)
        .args(["workflows", "validate", "smoke"])
        .assert()
        .success()
        .stdout(predicate::str::contains("ok"));

    // 6. run smoke --var SUBJECT=world → exit 0 done.
    cli(&home)
        .args(["run", "smoke", "--var", "SUBJECT=world"])
        .assert()
        .success()
        .stdout(predicate::str::contains("done:"));

    // 7. runs ls shows it with status=done. Pull the run_id out of
    //    --json for the next two assertions.
    let ls = cli(&home).args(["--json", "runs", "ls"]).output().unwrap();
    assert!(ls.status.success());
    let parsed: serde_json::Value = serde_json::from_slice(&ls.stdout).unwrap();
    let arr = parsed.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["status"], "done");
    let run_id = arr[0]["run_id"].as_str().unwrap().to_string();

    // 8. runs show — node_runs has both nodes.
    cli(&home)
        .args(["runs", "show", &run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("wait"))
        .stdout(predicate::str::contains("tx"))
        .stdout(predicate::str::contains("done"));

    // 9. runs logs — at least the started + done envelopes are
    //    present and parse as JSON.
    let logs = cli(&home).args(["runs", "logs", &run_id]).output().unwrap();
    assert!(logs.status.success());
    let stdout = String::from_utf8(logs.stdout).unwrap();
    let mut started_seen = false;
    let mut done_seen = false;
    for line in stdout.lines() {
        let ev: serde_json::Value =
            serde_json::from_str(line).expect("each logs line must be JSON");
        match ev["type"].as_str() {
            Some("workflow:started") => started_seen = true,
            Some("workflow:done") => done_seen = true,
            _ => {},
        }
    }
    assert!(started_seen, "no workflow:started in run_events");
    assert!(done_seen, "no workflow:done in run_events");

    // 10. workflows show round-trips the imported JSON.
    cli(&home)
        .args(["workflows", "show", "smoke"])
        .assert()
        .success()
        .stdout(predicate::str::contains(r#""id": "smoke""#));

    // 11. export → import into a new home should round-trip.
    let exported = cli(&home).args(["export", "smoke"]).output().unwrap();
    assert!(exported.status.success());
    let home2 = TempDir::new().unwrap();
    cli(&home2)
        .args(["import"])
        .write_stdin(exported.stdout)
        .assert()
        .success();
    assert!(home2.path().join("workflows/smoke.json").exists());

    // 12. secrets rm is intentionally skipped here: the sample
    //     keyring backend is per-process so the value set earlier
    //     isn't visible to the rm subprocess (the sidecar names
    //     persist but the keyring itself doesn't). The dedicated
    //     secrets tests cover rm via direct sidecar seeding.

    // 13. runs rm with --force.
    cli(&home)
        .args(["runs", "rm", &run_id, "--force"])
        .assert()
        .success()
        .stdout(predicate::str::contains("removed"));

    // 14. gui stub still exits 2 with the v1.0 explanation.
    cli(&home)
        .args(["gui"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("GUI binary not installed"));
}
