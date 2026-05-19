//! Surface-level CLI smoke. Confirms clap parses the full subcommand
//! tree and that bare invocation is an error (not an auto-launched
//! GUI).

use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn help_lists_subcommands() {
    Command::cargo_bin("ordius-cli")
        .unwrap()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("run"))
        .stdout(predicate::str::contains("workflows"))
        .stdout(predicate::str::contains("runs"))
        .stdout(predicate::str::contains("nodes"))
        .stdout(predicate::str::contains("secrets"))
        .stdout(predicate::str::contains("export"))
        .stdout(predicate::str::contains("import"))
        .stdout(predicate::str::contains("gui"));
}

#[test]
fn help_exposes_global_flags() {
    Command::cargo_bin("ordius-cli")
        .unwrap()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("--json"))
        .stdout(predicate::str::contains("--no-color"))
        .stdout(predicate::str::contains("--verbose"))
        .stdout(predicate::str::contains("--home"));
}

#[test]
fn bare_invocation_fails_with_usage() {
    Command::cargo_bin("ordius-cli")
        .unwrap()
        .assert()
        .failure()
        .stderr(predicate::str::contains("Usage:"));
}

#[test]
fn run_help_lists_flags() {
    Command::cargo_bin("ordius-cli")
        .unwrap()
        .args(["run", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--json-events"))
        .stdout(predicate::str::contains("--var"))
        .stdout(predicate::str::contains("--vars-file"));
}

#[test]
fn workflows_help_lists_subcommands() {
    Command::cargo_bin("ordius-cli")
        .unwrap()
        .args(["workflows", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("ls"))
        .stdout(predicate::str::contains("show"))
        .stdout(predicate::str::contains("validate"))
        .stdout(predicate::str::contains("rm"));
}
