//! Probe subcommand: reads a `ProbePlanV1` from stdin, emits one JSONL
//! `ProbeOutcomeV1` per resource on stdout.

use std::io::{BufRead, Write};

/// Placeholder until T3 lands the real probe orchestrator.
pub fn run<R: BufRead, W: Write>(_input: R, _output: W) -> anyhow::Result<()> {
    anyhow::bail!("probe subcommand not implemented yet")
}
