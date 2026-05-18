# Ordius

A local-first workflow runner for developers — orchestrate agents,
LLMs, containers, and shell commands as a visual DAG, or trigger
the same workflows from the CLI.

**Status:** Design / pre-implementation. Concept and section specs in
[`docs/`](./docs/). No code yet — see [`docs/00-concept.md`](./docs/00-concept.md)
for the overview and [`docs/07-triggers-and-v1-scope.md`](./docs/07-triggers-and-v1-scope.md)
for the v1 scope cut.

## Reading order

> If you're an AI agent picking this up, read [`docs/AGENT-ONBOARDING.md`](./docs/AGENT-ONBOARDING.md) first — it covers the boot sequence, decision rationale, and sharp edges in one place.

1. [`docs/00-concept.md`](./docs/00-concept.md) — what Ordius is, why, who for
2. [`docs/01-system-shape.md`](./docs/01-system-shape.md) — two binaries (GUI + CLI), engine as shared library
3. [`docs/02-engine-model.md`](./docs/02-engine-model.md) — DAG, scheduler, executor
4. [`docs/03-node-types.md`](./docs/03-node-types.md) — built-in types + JSON manifest format
5. [`docs/04-storage-and-format.md`](./docs/04-storage-and-format.md) — workflow files, run DB, secrets
6. [`docs/05-cli-surface.md`](./docs/05-cli-surface.md) — `ordius` command-line
7. [`docs/06-data-flow.md`](./docs/06-data-flow.md) — ports, variables, substitution, secrets
8. [`docs/07-triggers-and-v1-scope.md`](./docs/07-triggers-and-v1-scope.md) — triggers + v1 scope cut
9. [`docs/gui-brief.md`](./docs/gui-brief.md) — input for the GUI design pass

## Stack

Rust core (engine as a library) + Tauri 2 (GUI shell). Two binaries —
`ordius.exe` (GUI) and `ordius-cli.exe` (headless / scripting) — shipped
together, linking the same engine crate.
