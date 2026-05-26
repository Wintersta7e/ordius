#!/usr/bin/env bash
# Build cross-compiled ordius-helper binaries for each embedded target and
# drop them under crates/engine/embedded/helper/<triple>/ordius-helper.
#
# Requires `rustup target add <triple>` for each, and a compatible linker
# (musl-cross / clang / mingw-w64 depending on triple).
#
# Usage: scripts/build-helpers.sh [triple ...]
#   With no args, builds the two default targets (linux-x86_64-musl,
#   linux-aarch64-musl) per spec §3.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEFAULT_TARGETS=(
    x86_64-unknown-linux-musl
    aarch64-unknown-linux-musl
)
TARGETS=("${@:-${DEFAULT_TARGETS[@]}}")

cd "$ROOT"
for triple in "${TARGETS[@]}"; do
    echo ">> building ordius-helper for $triple"
    cargo build --release --target "$triple" -p ordius-helper
    src="target/$triple/release/ordius-helper"
    dst_dir="crates/engine/embedded/helper/$triple"
    dst="$dst_dir/ordius-helper"
    mkdir -p "$dst_dir"
    cp "$src" "$dst"
    if command -v strip >/dev/null 2>&1; then
        strip "$dst" || true
    fi
    size=$(wc -c <"$dst")
    echo ">> $dst  $size bytes"
done
echo "Done — run \`cargo build -p ordius-engine\` to pick up the embedded binaries."
