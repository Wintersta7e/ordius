//! Exec subcommand: reads an `ExecRequestV1` from stdin, runs the requested
//! argv-only command, forwards stdout/stderr in real time, exits with the
//! child's status code.

use std::io::BufRead;

/// Placeholder until T4 lands the real runner.
pub fn run<R: BufRead>(_input: R) -> anyhow::Result<()> {
    anyhow::bail!("subcommand not implemented yet")
}
