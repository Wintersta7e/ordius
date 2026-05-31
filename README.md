<div align="center">

# Ordius

**A local-first workflow runner for personal automation.**

Orchestrate coding agents, LLMs, containers, shell commands, and HTTP calls
as a directed acyclic graph. Run from the CLI or a Tauri 2 desktop GUI.

[![Rust](https://img.shields.io/badge/rust-1.95%2B-orange?logo=rust&logoColor=white)](https://www.rust-lang.org)
[![Edition](https://img.shields.io/badge/edition-2024-purple)](https://doc.rust-lang.org/edition-guide/rust-2024/)
[![Tauri](https://img.shields.io/badge/Tauri-2-24C8DB?logo=tauri&logoColor=white)](https://tauri.app)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![Status](https://img.shields.io/badge/status-personal%20%C2%B7%20actively%20developed-brightgreen)](#status)

</div>

---

## Why

I built Ordius for myself — a workflow substrate to chain agent loops, local
LLMs, image generation, containers, and shell scripts without hand-writing
one-off bash glue every time. It's **local-first**, **offline-capable**, and
sends **no telemetry**.

It's a personal tool, not a product — but it's open source under Apache-2.0,
and if it looks useful to you, you're welcome to clone it and give it a try.
There's no adoption goal and no support guarantees, but issues and PRs are read.

The same workflows run from a terminal (`ordius-cli run my-workflow`) or a
desktop GUI — two binaries, one shared engine crate.

## Status

Actively developed personal tool. The engine and CLI are implemented and
covered by a large unit + integration test suite under strict workspace lints.
It's still rough in places, and the workflow schema / internal APIs can shift
between commits — treat it as a working engine you can build and run, not a
finished, packaged app.

**Implemented:**
- Edge-activation DAG engine — branching, looping, parallel fan-out, retries,
  timeouts, graceful cancellation
- 16 built-in node types (see [Features](#features))
- `ordius-cli` — `run`, `workflows`, `runs`, `nodes`, `secrets`,
  `export` / `import`, and `daemon`
- Tauri 2 desktop GUI — visual DAG editor, live run view, run-history browser
- Triggers — cron schedules, file-watch, and inbound webhooks, hosted by
  `ordius daemon`
- Container execution via [`bollard`][bollard] (the `docker-run` node)
- Execution environments — local, WSL, and remote SSH
- SQLite run history, OS-keyring secrets, template substitution, NDJSON events

**Not done yet:** end-to-end polish, packaged installers, and the
planned items below.

## Features

### Engine + CLI
- Edge-activation DAG scheduler with branching, looping, parallel execution
- Per-node owned [`CancellationToken`s][cancel] (tokio-util)
- Process-group (Unix) / Job Object (Windows) subprocess supervision
- **16 built-in node types:**
  - *core:* `shell`, `llm`, `http`, `file`, `transform`, `condition`,
    `checkpoint`, `delay`
  - *control + integration:* `kv`, `notify`, `pause`, `loop_for`,
    `wait_event`, `compose`, `parallel`, `docker-run`
- JSON-manifest custom node types (declarative, no plugin SDK)
- SQLite run history keyed by `(run_id, node_id, iteration, attempt)` —
  loops and retries don't overwrite each other
- Workflows are plain JSON / YAML files, git-trackable
- Template substitution with type coercion + secret redaction
- OS-keyring secrets (no env-var leaks)
- NDJSON event stream over stdout for piping to other tools

### Desktop GUI
- Tauri 2 webview shell (React + TypeScript frontend, no Electron)
- Visual DAG editor: drag nodes from a categorised palette, wire typed ports
- Live run view with per-node streaming output
- Run-history browser with status / date / trigger filters

### Triggers & environments
- `ordius daemon` — long-lived host for schedule + file-watch + webhook triggers
- Cron schedules, filesystem watches, and inbound webhooks
- Execution environments: local, WSL, and remote SSH (key/password auth,
  host-key pinning, helper bootstrap over SFTP)

### Planned / architected (not built)
- WASM plugin SDK, vector-store node type, additional LLM provider adapters
- Richer write-back policies for remote-environment runs

### Explicitly declined (not on the roadmap)
- Multi-user / team mode • Cloud sync • Mobile companion • Telemetry • Marketing surface

## Stack

| Layer | Choice | Notes |
|---|---|---|
| Core | [Rust][rust] 1.95+, edition 2024 | Strict workspace lints from commit one |
| Async | [tokio][tokio] + [tokio-util][tokio-util] | `tokio::process` for subprocesses, `CancellationToken` per node |
| Storage | [rusqlite][rusqlite] (bundled) + [r2d2][r2d2] pool | WAL mode for cross-process concurrency |
| Errors | [thiserror][thiserror] | Per-module enums + top-level `EngineError` aggregator |
| HTTP / LLM | [reqwest][reqwest] + rustls | No native-tls dep, cross-compile friendly |
| Secrets | [keyring][keyring] | OS-native: macOS Keychain, Win Credential Vault, Secret Service |
| CLI | [clap][clap] (derive) | Subcommands + global flags |
| GUI | [Tauri 2][tauri] webview + React + TypeScript | Inline styles over CSS custom properties |
| Containers | [bollard][bollard] | Docker / Podman backend for the `docker-run` node |

## Quick start

```bash
# Clone and build
git clone <repo-url> && cd Ordius
cargo build --workspace

# Run the checks
cargo test   --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt    --all -- --check

# CLI
cargo run -p ordius-cli -- --help
cargo run -p ordius-cli -- nodes ls     # list the built-in node types
```

Rust 1.95+ is required; the [`rust-toolchain.toml`](rust-toolchain.toml)
pin handles install via `rustup`.

## Layout

```
Ordius/
├── crates/
│   ├── engine/         # workflow engine (library, no UI deps)
│   ├── cli/            # ordius-cli binary (clap-based)
│   ├── desktop/        # Tauri 2 GUI shell
│   └── ordius-helper/  # remote-exec helper (cross-compiled, embedded)
├── Cargo.toml          # workspace deps + strict workspace.lints
├── rust-toolchain.toml
├── rustfmt.toml        # 100-col, edition 2024
├── clippy.toml         # MSRV 1.95, complexity thresholds, test allowances
└── deny.toml           # cargo-deny: licenses, advisories, sources
```

## Design principles

1. **Easy to start.** Run `ordius-cli` from a terminal or launch the desktop
   GUI. No daemon to configure, no port to pick.
2. **Small core, declarative custom nodes.** 16 built-in types; everything
   else is a JSON-manifest file dropped in the engine's node-types directory.
3. **Correctness over cleverness.** Process groups + Job Objects for
   subprocess kill, not `taskkill /F /T` workarounds.
4. **Inspectable.** Every run in SQLite, every node output captured, every
   variable substitution logged. Workflows are plain text.
5. **CLI is first-class.** Anything the GUI does, the CLI does. Built for
   piping (NDJSON events, exit-code contract, `--json` flags).

## License

[Apache-2.0](LICENSE).

---

<sub>Ordius is a personal tool — built for my own daily use, with no telemetry,
analytics, or growth metrics. Not chasing adoption, but if you're interested
you're welcome to try it.</sub>

[rust]:        https://www.rust-lang.org
[tauri]:       https://tauri.app
[tokio]:       https://tokio.rs
[tokio-util]:  https://docs.rs/tokio-util
[rusqlite]:    https://docs.rs/rusqlite
[r2d2]:        https://docs.rs/r2d2
[thiserror]:   https://docs.rs/thiserror
[reqwest]:     https://docs.rs/reqwest
[keyring]:     https://docs.rs/keyring
[clap]:        https://docs.rs/clap
[bollard]:     https://docs.rs/bollard
[cancel]:      https://docs.rs/tokio-util/latest/tokio_util/sync/struct.CancellationToken.html
