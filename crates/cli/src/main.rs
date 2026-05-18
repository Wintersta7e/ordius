//! Ordius CLI entry point. See `docs/` at the repo root for the spec.

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "ordius-cli", version, about = "Ordius workflow runner CLI")]
struct Cli {}

// `anyhow::Result` return is intentional: subcommands propagate errors via `?`
// once they land. The stub body has no fallible calls yet, hence the allow.
#[allow(clippy::unnecessary_wraps)]
fn main() -> anyhow::Result<()> {
    Cli::parse();
    println!("ordius-cli (stub — full surface lands in Phase 9)");
    Ok(())
}
