//! Tests for the `secrets` subcommand surface (ls/set/rm).
//!
//! Every test runs the CLI with `ORDIUS_TEST_KEYRING=1` so the
//! sample (in-memory) keyring is installed instead of the host's
//! credential store. The sample store is per-process, so cross-
//! invocation flows are limited to what the sidecar file can model
//! on its own.

use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use tempfile::TempDir;

fn cli(home: &TempDir) -> Command {
    let mut cmd = Command::cargo_bin("ordius-cli").unwrap();
    cmd.env("ORDIUS_TEST_KEYRING", "1")
        .args(["--home", home.path().to_str().unwrap()]);
    cmd
}

fn seed_sidecar(home: &TempDir, names: &[&str]) {
    let json = serde_json::to_string_pretty(names).unwrap();
    fs::write(home.path().join("secrets-index.json"), json).unwrap();
}

#[test]
fn secrets_ls_empty_home_prints_placeholder() {
    let home = TempDir::new().unwrap();
    cli(&home)
        .args(["secrets", "ls"])
        .assert()
        .success()
        .stdout(predicate::str::contains("no secrets known"));
}

#[test]
fn secrets_ls_with_seeded_sidecar_lists_names() {
    let home = TempDir::new().unwrap();
    seed_sidecar(&home, &["API_KEY", "DB_URL"]);
    cli(&home)
        .args(["secrets", "ls"])
        .assert()
        .success()
        .stdout(predicate::str::contains("API_KEY"))
        .stdout(predicate::str::contains("DB_URL"));
}

#[test]
fn secrets_set_records_name_in_sidecar() {
    let home = TempDir::new().unwrap();
    cli(&home)
        .args(["secrets", "set", "MY_KEY"])
        .write_stdin("hunter2\n")
        .assert()
        .success()
        .stdout(predicate::str::contains("set MY_KEY"));
    let sidecar = home.path().join("secrets-index.json");
    assert!(sidecar.exists());
    let body = fs::read_to_string(&sidecar).unwrap();
    assert!(
        body.contains("MY_KEY"),
        "sidecar should record the set secret: {body}"
    );
}

#[test]
fn secrets_set_empty_value_rejected() {
    let home = TempDir::new().unwrap();
    cli(&home)
        .args(["secrets", "set", "EMPTY"])
        .write_stdin("\n")
        .assert()
        .failure()
        .stderr(predicate::str::contains("refusing to store empty value"));
}

#[test]
fn secrets_rm_unknown_name_errors() {
    let home = TempDir::new().unwrap();
    seed_sidecar(&home, &["KEEP"]);
    cli(&home)
        .args(["secrets", "rm", "GHOST", "--force"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("not known to the sidecar"));
}

#[test]
fn secrets_rm_prompt_abort_keeps_sidecar() {
    let home = TempDir::new().unwrap();
    seed_sidecar(&home, &["KEEP"]);
    cli(&home)
        .args(["secrets", "rm", "KEEP"])
        .write_stdin("\n")
        .assert()
        .code(1)
        .stderr(predicate::str::contains("aborted"));
    let body = fs::read_to_string(home.path().join("secrets-index.json")).unwrap();
    assert!(body.contains("KEEP"));
}

#[test]
fn secrets_set_then_rm_in_one_invocation_via_shell_isnt_supported() {
    // The sample keyring is per-process and `secrets rm` in a fresh
    // process can't see the value put there by an earlier `secrets
    // set`. This test documents that limitation rather than testing
    // it — it's intentionally minimal and only confirms the rm
    // sidecar-presence check is what guards the prompt.
    let home = TempDir::new().unwrap();
    seed_sidecar(&home, &["TARGET"]);
    cli(&home)
        .args(["secrets", "rm", "TARGET"])
        .write_stdin("y\n")
        .assert()
        // delete returns NotFound from the in-memory keyring
        // because the value was never seeded in this process; we
        // expect the engine layer to surface that as a failure.
        .failure()
        .stderr(predicate::str::contains("delete secret"));
}
