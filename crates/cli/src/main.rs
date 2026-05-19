//! Ordius CLI entry point. See `docs/` at the repo root for the spec.
//!
//! Subcommand surface is declared here; each subcommand body is
//! wired across this and the following commits in the implementation
//! plan. `main` parses, initialises tracing per `--verbose`, and
//! dispatches by subcommand. Each subcommand owns its own engine
//! initialisation (so `--help` doesn't open `runs.db`).

use anyhow::Context;
use clap::{Parser, Subcommand};
use ordius_engine::{load_workflow, validate};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

/// Top-level CLI: global flags + a required subcommand.
#[derive(Parser, Debug)]
#[command(name = "ordius-cli", version, about = "Ordius workflow runner CLI")]
#[command(propagate_version = true)]
struct Cli {
    /// Emit JSON output where applicable.
    #[arg(long, global = true)]
    json: bool,
    /// Disable ANSI colours in human output.
    #[arg(long, global = true)]
    no_color: bool,
    /// Verbose tracing: `-v` info, `-vv` debug, `-vvv` trace.
    #[arg(short, long, global = true, action = clap::ArgAction::Count)]
    verbose: u8,
    /// Override the `$HOME/.ordius` engine home (also via `$ORDIUS_HOME`).
    #[arg(long, global = true, env = "ORDIUS_HOME")]
    home: Option<PathBuf>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Run a workflow.
    Run(RunArgs),
    /// Manage workflows on disk.
    Workflows {
        #[command(subcommand)]
        sub: WorkflowsSub,
    },
    /// Inspect run history.
    Runs {
        #[command(subcommand)]
        sub: RunsSub,
    },
    /// Inspect registered node types.
    Nodes {
        #[command(subcommand)]
        sub: NodesSub,
    },
    /// Manage OS-keyring secrets.
    Secrets {
        #[command(subcommand)]
        sub: SecretsSub,
    },
    /// Export a workflow as JSON to stdout.
    Export { id: String },
    /// Import a workflow definition from stdin.
    Import {
        /// Rename to this id before saving.
        #[arg(long = "as")]
        as_id: Option<String>,
    },
    /// Launch the GUI binary (lands in v1.1).
    Gui,
}

#[derive(clap::Args, Debug)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "CLI flag args — bools are the natural representation; each flag is independent."
)]
struct RunArgs {
    /// Workflow id (filename without `.json` in `<home>/workflows/`).
    id: String,
    /// `KEY=VALUE` pair, repeatable.
    #[arg(long, value_parser = parse_var)]
    var: Vec<(String, String)>,
    /// Path to a JSON/YAML file of `{ "VAR": "value" }` pairs.
    #[arg(long)]
    vars_file: Option<PathBuf>,
    /// Auto-resume any `checkpoint` nodes encountered.
    #[arg(long)]
    yes: bool,
    /// Stream `RunEvent`s as NDJSON on stdout.
    #[arg(long)]
    json_events: bool,
    /// Run in a private workspace removed when the run ends.
    #[arg(long)]
    isolate: bool,
    /// Spawn the run and exit immediately.
    #[arg(long)]
    detach: bool,
    /// Keep the run's workspace dir on disk after the run ends.
    #[arg(long)]
    keep_workspace: bool,
}

fn parse_var(s: &str) -> Result<(String, String), String> {
    s.split_once('=')
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .ok_or_else(|| format!("expected KEY=VALUE, got '{s}'"))
}

#[derive(Subcommand, Debug)]
enum WorkflowsSub {
    /// List all workflows in `<home>/workflows/`.
    Ls,
    /// Show a workflow as JSON.
    Show { id: String },
    /// Validate a workflow's structure.
    Validate { id_or_path: String },
    /// Delete a workflow (prompts unless `--force`).
    Rm {
        id: String,
        #[arg(long)]
        force: bool,
    },
}

#[derive(Subcommand, Debug)]
enum RunsSub {
    /// List recent runs.
    Ls {
        /// Restrict to a single workflow id.
        #[arg(long)]
        workflow: Option<String>,
        /// Filter by run status (`done`/`error`/`stopped`/`running`).
        #[arg(long)]
        status: Option<String>,
        /// Human-readable cutoff, e.g. `7d`, `12h`.
        #[arg(long)]
        since: Option<String>,
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Show a single run plus its node-runs.
    Show { run_id: String },
    /// Stream a run's events.
    Logs {
        run_id: String,
        /// Restrict to a single node id.
        #[arg(long)]
        node: Option<String>,
        /// Follow live runs by polling `run_events`.
        #[arg(short = 'f', long)]
        follow: bool,
    },
    /// Delete a run record (FK cascade clears its rows).
    Rm {
        run_id: String,
        #[arg(long)]
        force: bool,
    },
}

#[derive(Subcommand, Debug)]
enum NodesSub {
    /// List registered node types.
    Ls {
        /// Exact-match category filter.
        #[arg(long)]
        category: Option<String>,
        /// Tag filter, repeatable, AND semantics.
        #[arg(long)]
        tag: Vec<String>,
    },
    /// Print the full `NodeType` spec for one node.
    Show { ty: String },
}

#[derive(Subcommand, Debug)]
enum SecretsSub {
    /// List known secret names (values never displayed).
    Ls,
    /// Set a secret (value prompted with no echo).
    Set { name: String },
    /// Delete a secret (prompts unless `--force`).
    Rm {
        name: String,
        #[arg(long)]
        force: bool,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    init_tracing(cli.verbose);

    match dispatch(cli).await {
        Ok(code) => ExitCode::from(code),
        Err(e) => {
            eprintln!("ordius: {e:#}");
            ExitCode::from(1)
        },
    }
}

#[allow(
    clippy::unused_async,
    reason = "subcommand bodies in follow-up commits will await (engine.start_run, ndjson streaming)."
)]
async fn dispatch(cli: Cli) -> anyhow::Result<u8> {
    let home = resolve_home(cli.home);
    match cli.cmd {
        Cmd::Workflows { sub } => cmd_workflows(sub, &home),
        Cmd::Run(_)
        | Cmd::Runs { .. }
        | Cmd::Nodes { .. }
        | Cmd::Secrets { .. }
        | Cmd::Export { .. }
        | Cmd::Import { .. }
        | Cmd::Gui => {
            anyhow::bail!("subcommand not yet wired in this build");
        },
    }
}

/// Resolve the engine home directory: explicit `--home` wins,
/// otherwise `$HOME/.ordius` (Unix) or `$USERPROFILE/.ordius`
/// (Windows). Falls back to a relative `./.ordius` if neither env
/// var is set — keeps the binary runnable in stripped environments.
fn resolve_home(override_path: Option<PathBuf>) -> PathBuf {
    override_path.unwrap_or_else(|| {
        std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .map_or_else(
                || PathBuf::from(".ordius"),
                |h| PathBuf::from(h).join(".ordius"),
            )
    })
}

fn workflows_dir(home: &Path) -> PathBuf {
    home.join("workflows")
}

fn workflow_path(home: &Path, id: &str) -> PathBuf {
    workflows_dir(home).join(format!("{id}.json"))
}

fn cmd_workflows(sub: WorkflowsSub, home: &Path) -> anyhow::Result<u8> {
    match sub {
        WorkflowsSub::Ls => workflows_ls(home),
        WorkflowsSub::Show { id } => workflows_show(home, &id),
        WorkflowsSub::Validate { id_or_path } => workflows_validate(home, &id_or_path),
        WorkflowsSub::Rm { id, force } => workflows_rm(home, &id, force),
    }
}

fn workflows_ls(home: &Path) -> anyhow::Result<u8> {
    let dir = workflows_dir(home);
    if !dir.exists() {
        // Empty home is not an error — just nothing to list yet.
        println!("(no workflows; nothing in {})", dir.display());
        return Ok(0);
    }
    let mut entries: Vec<(String, String, usize)> = Vec::new();
    for raw in fs::read_dir(&dir).with_context(|| format!("read_dir {}", dir.display()))? {
        let entry = raw.with_context(|| format!("read_dir entry under {}", dir.display()))?;
        let path = entry.path();
        if path.extension().and_then(std::ffi::OsStr::to_str) != Some("json") {
            continue;
        }
        match load_workflow(&path) {
            Ok(wf) => entries.push((wf.id.clone(), wf.name.clone(), wf.triggers.len())),
            Err(e) => eprintln!("warn: {} failed to parse: {e}", path.display()),
        }
    }
    entries.sort();
    println!("{:<24} {:<32} TRIGGERS", "ID", "NAME");
    for (id, name, triggers) in entries {
        println!("{id:<24} {name:<32} {triggers}");
    }
    Ok(0)
}

fn workflows_show(home: &Path, id: &str) -> anyhow::Result<u8> {
    let path = workflow_path(home, id);
    let wf = load_workflow(&path)
        .with_context(|| format!("load workflow {id} from {}", path.display()))?;
    let json =
        serde_json::to_string_pretty(&wf).with_context(|| format!("serialise workflow {id}"))?;
    println!("{json}");
    Ok(0)
}

fn workflows_validate(home: &Path, id_or_path: &str) -> anyhow::Result<u8> {
    let path = resolve_id_or_path(home, id_or_path);
    let wf =
        load_workflow(&path).with_context(|| format!("load workflow from {}", path.display()))?;
    match validate(&wf) {
        Ok(()) => {
            println!("ok");
            Ok(0)
        },
        Err(e) => {
            eprintln!("validation error: {e}");
            Ok(2)
        },
    }
}

fn workflows_rm(home: &Path, id: &str, force: bool) -> anyhow::Result<u8> {
    let path = workflow_path(home, id);
    if !path.exists() {
        anyhow::bail!("workflow {id} not found at {}", path.display());
    }
    if !force && !confirm(&format!("Delete workflow {id}?"))? {
        eprintln!("aborted");
        return Ok(1);
    }
    fs::remove_file(&path).with_context(|| format!("remove {}", path.display()))?;
    println!("removed {id}");
    Ok(0)
}

fn resolve_id_or_path(home: &Path, id_or_path: &str) -> PathBuf {
    let p = Path::new(id_or_path);
    let has_known_ext = p.extension().is_some_and(|ext| {
        ["json", "yaml", "yml"]
            .iter()
            .any(|e| ext.eq_ignore_ascii_case(e))
    });
    let has_separator = id_or_path.contains('/') || id_or_path.contains('\\');
    if has_known_ext || has_separator {
        PathBuf::from(id_or_path)
    } else {
        workflow_path(home, id_or_path)
    }
}

fn confirm(prompt: &str) -> anyhow::Result<bool> {
    use std::io::BufRead;
    eprint!("{prompt} [y/N] ");
    io::stderr().flush()?;
    let stdin = io::stdin();
    let mut line = String::new();
    stdin.lock().read_line(&mut line)?;
    let ans = line.trim().to_lowercase();
    Ok(matches!(ans.as_str(), "y" | "yes"))
}

fn init_tracing(verbose: u8) {
    let default = match verbose {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default));
    let init = tracing_subscriber::fmt().with_env_filter(filter).try_init();
    drop(init);
}
