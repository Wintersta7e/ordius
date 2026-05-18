<div align="center">

# Ordius

**A local-first workflow runner for personal automation.**

Orchestrate coding agents, LLMs, containers, shell commands, and HTTP calls
as a directed acyclic graph. Run from the CLI or a Tauri 2 desktop GUI.

[![Rust](https://img.shields.io/badge/rust-1.95%2B-orange?logo=rust&logoColor=white)](https://www.rust-lang.org)
[![Edition](https://img.shields.io/badge/edition-2024-purple)](https://doc.rust-lang.org/edition-guide/rust-2024/)
[![Tauri](https://img.shields.io/badge/Tauri-2-24C8DB?logo=tauri&logoColor=white)](https://tauri.app)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](#license)
[![Status](https://img.shields.io/badge/status-WIP%20%E2%80%94%20v1.0%20in%20progress-yellow)](#roadmap)
[![Lints](https://img.shields.io/badge/lints-clippy%20pedantic%20%2B%20nursery%20%2B%20cargo-success)](clippy.toml)

</div>

---

## Why

Ordius is the workflow substrate behind the author's daily coding workflows
— composable enough to chain agent loops + local LLMs + image generation +
containers + shell scripts without writing one-off bash glue every time.
Personal-scale, **local-first**, **offline-capable**, **no telemetry**.

The same workflows run from a terminal (`ordius-cli run my-workflow`) or
a desktop GUI — two binaries, one shared engine crate.

## Status

> **In active development.** Not yet usable end-to-end.

- **v1.0** (engine + CLI) — Phase 0 (workspace + strict tooling) and
  Phase 1 (engine types + JSON schema + workflow loader + structural
  validation + unified error) are complete. 8 phases remain (DAG
  scheduler, SQLite recorder, executors, templates, retries, CLI
  surface, manifest loading).
- **v1.1** (GUI) — design prototypes done; implementation gated on v1.0.
- **v1.x** (container backend, daemon mode, additional triggers) —
  architected, not yet built.

See [`docs/plans/`](#layout) (local-only) for the canonical per-phase plan.

## Features

### v1.0 — engine + CLI (current milestone)
- Edge-activation DAG scheduler with branching, looping, parallel execution
- Per-node owned [`CancellationToken`s][cancel] (tokio-util)
- Process-group (Unix) / Job Object (Windows) subprocess supervision
- 8 built-in node types: `shell`, `llm`, `http`, `file`, `transform`,
  `condition`, `checkpoint`, `delay`
- JSON-manifest custom node types (declarative, no plugin SDK)
- SQLite run history with `(run_id, node_id, iteration, attempt)` keying —
  loops and retries don't overwrite each other
- Workflows are plain JSON / YAML files, git-trackable
- Template substitution with type coercion + secret redaction
- OS-keyring secrets (no env-var leaks)
- NDJSON event stream over stdout for piping to other tools

### v1.1 — GUI + remaining built-ins (next milestone)
- Tauri 2 webview shell (HTML/CSS/JS frontend, no Electron)
- Visual DAG editor: drag nodes from a categorised palette, wire typed ports
- Live run view with per-node streaming output
- Run history browser with status / date / trigger filters
- 10 additional built-in types: `agent`, `python`, `node`, `embedding`,
  `vision`, `image-gen`, `parse`, `variable`, `kv-store`, `notification`
- `ordius daemon` for headless schedule + file-watch triggers

### v1.x and later — architected, not built
- Container execution backend (Docker / Podman via [`bollard`][bollard])
- Webhook triggers, WASM plugin SDK, vector-store node type
- Workflow chaining (workflow-as-a-node)

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
| CLI | [clap][clap] (derive) | Subcommands land in Phase 9 |
| GUI (v1.1) | [Tauri 2][tauri] webview + React | Frontend prototypes in `docs/UI/` |

## Quick start

```bash
# Clone and build
git clone <repo-url> && cd Ordius
cargo build --workspace

# Run the gated checks
cargo test  -p ordius-engine --lib
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt    --all -- --check

# CLI stub (subcommands land in Phase 9)
cargo run -p ordius-cli -- --help
```

Rust 1.95+ is required; the [`rust-toolchain.toml`](rust-toolchain.toml)
pin handles install via `rustup`.

## Layout

```
Ordius/
├── crates/
│   ├── engine/        # workflow engine (library, no UI deps)
│   ├── cli/           # ordius-cli binary (clap-based)
│   └── desktop/       # Tauri GUI shell (lands in v1.1)
├── Cargo.toml         # workspace deps + strict workspace.lints
├── rust-toolchain.toml
├── rustfmt.toml       # 100-col, edition 2024, nightly intent documented
├── clippy.toml        # MSRV 1.95, complexity thresholds, test allowances
├── deny.toml          # cargo-deny: licenses, advisories, sources
└── docs/              # spec, plans, UI prototypes (local-only, gitignored)
```

## Roadmap

| Milestone | What | Status |
|---|---|---|
| **v1.0** | Rust `engine` crate + `ordius-cli` binary | 2/10 phases complete |
| **v1.1** | Tauri 2 GUI shell, daemon mode, 10 more built-ins | design done, awaiting v1.0 |
| **v1.x** | Container backend, webhook triggers, vector store | architected only |

The implementation plan lives in [`docs/plans/v1.0-implementation.md`][plan]
(local-only). 72 tasks across 10 phases, TDD-shaped, with real code in
every step.

## Design principles

1. **Easy to start.** Double-click `ordius.exe` (v1.1) or run `ordius-cli`
   from a terminal. No daemon to configure, no port to pick.
2. **Small core, declarative custom nodes.** 18 built-in types;
   everything else is a JSON-manifest file dropped in `~/.ordius/node-types/`.
3. **Correctness over cleverness.** Process groups + Job Objects for
   subprocess kill, not `taskkill /F /T` workarounds.
4. **Inspectable.** Every run in SQLite, every node output captured,
   every variable substitution logged. Workflows are plain text.
5. **CLI is first-class.** Anything the GUI does, the CLI does. Built
   for piping (NDJSON events, exit-code contract, `--json` flags).

## License

Apache-2.0. The `LICENSE` file is not yet committed — see deferred-items
in `docs/plans/SESSION-HANDOFF.md`.

---

<sub>Ordius is a personal tool. No telemetry, no analytics, no growth metrics.
Built for the author's daily use; PRs welcome but adoption is not a goal.</sub>

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
[plan]:        docs/plans/v1.0-implementation.md
