//! Ordius CLI entry point. See `docs/` at the repo root for the spec.

use clap::Parser;
use ordius_engine::Engine;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(name = "ordius-cli", version, about = "Ordius workflow runner CLI")]
struct Cli {
    /// Engine home directory (defaults to `~/.ordius`). The
    /// `runs.db` `SQLite` database and per-run workspaces live
    /// here.
    #[arg(long, env = "ORDIUS_HOME")]
    home: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let home = cli.home.unwrap_or_else(default_home);

    let engine = Arc::new(Engine::new(home).await?);

    // SIGINT (ctrl-c) drains active runs then exits 130. Placeholder
    // until subcommands land — once they do they'll wait on the
    // engine instead of returning immediately, and the engine clone
    // here will become load-bearing.
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            eprintln!("\nordius: shutting down...");
            let res = engine.shutdown(Duration::from_secs(5)).await;
            drop(res);
            std::process::exit(130);
        }
    });

    println!("ordius-cli (stub — subcommand surface lands in a later phase)");
    Ok(())
}

fn default_home() -> PathBuf {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map_or_else(
            || PathBuf::from(".ordius"),
            |h| PathBuf::from(h).join(".ordius"),
        )
}
