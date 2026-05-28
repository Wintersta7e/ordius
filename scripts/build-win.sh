#!/usr/bin/env bash
# Build the Windows release binaries (GUI + CLI) and stage them in dist-win/.
#
# WHY THIS SCRIPT EXISTS
# ----------------------
# The prod binary is produced with `cargo build` (NOT `cargo tauri build`) so
# the `custom-protocol` default feature yields a real standalone exe. But
# `cargo build` does not run tauri's `beforeBuildCommand` (`npm run build`),
# so on its own it silently embeds whatever assets already sit in
# `crates/desktop/ui/dist/`. If that bundle is stale, the shipped exe serves
# an old webview build — e.g. one that still invokes renamed IPC commands and
# greets the user with "command renamed: use environment_list" on launch.
#
# Building the UI first is therefore mandatory. This script enforces that
# ordering so the staged binary always matches the current frontend source.
#
# Usage: scripts/build-win.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

TARGET="x86_64-pc-windows-gnu"
UI_DIR="crates/desktop/ui"
OUT="target/${TARGET}/release"
DIST="dist-win"

echo "==> [1/3] Building UI bundle (mandatory before cargo build)…"
npm --prefix "$UI_DIR" run build

echo "==> [2/3] Cross-compiling Windows release binaries (${TARGET})…"
cargo build --release --target "$TARGET" -p ordius-desktop -p ordius-cli

echo "==> [3/3] Staging binaries into ${DIST}/…"
mkdir -p "$DIST"
cp -f "$OUT/ordius.exe" "$DIST/ordius.exe"
cp -f "$OUT/ordius-cli.exe" "$DIST/ordius-cli.exe"

echo "==> Done. Staged:"
ls -1 --time-style=long-iso -la "$DIST/ordius.exe" "$DIST/ordius-cli.exe"
