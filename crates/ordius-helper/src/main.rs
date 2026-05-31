//! Entry point for the `ordius-helper` binary.

use clap::{Parser, Subcommand};
use ordius_helper::{exec, probe};

#[derive(Parser, Debug)]
#[command(
    name = "ordius-helper",
    version,
    about = "Ordius in-environment runner"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Read a probe plan from stdin, emit JSONL outcomes on stdout.
    Probe,
    /// Read an argv-only exec request from stdin, run it, forward streams.
    Exec {
        /// Required marker: signals argv-only JSON input.  Other transports
        /// are reserved for future protocol versions and are rejected today.
        #[arg(long = "argv-json")]
        argv_json: bool,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Probe => probe::run(std::io::stdin().lock(), std::io::stdout().lock()),
        Cmd::Exec { argv_json } => {
            anyhow::ensure!(argv_json, "only --argv-json is supported in this version");
            // Owned `Stdin` (not `stdin().lock()`, which is `!Send`): the exec
            // monitor thread moves the reader to wait on stdin EOF for cancel.
            exec::run(std::io::BufReader::new(std::io::stdin()))
        },
    }
}
