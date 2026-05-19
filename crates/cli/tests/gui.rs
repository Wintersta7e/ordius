//! Tests for the `gui` subcommand stub.

use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn gui_subcommand_explains_v1_0_state() {
    Command::cargo_bin("ordius-cli")
        .unwrap()
        .arg("gui")
        .assert()
        .failure()
        .code(2)
        .stderr(predicate::str::contains("GUI binary not installed"))
        .stderr(predicate::str::contains("ordius-cli run"));
}
