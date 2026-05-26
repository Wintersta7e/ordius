# scripts/build-helpers.ps1
# Build cross-compiled ordius-helper binaries for each embedded target.
# Run from project root: .\scripts\build-helpers.ps1
#
# Requires:
#   rustup target add x86_64-unknown-linux-musl
#   rustup target add aarch64-unknown-linux-musl
# and a working musl linker (cargo-zigbuild + zig, or cross + Docker).

$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent (Split-Path -Parent $PSCommandPath))

$Targets = @(
    "x86_64-unknown-linux-musl",
    "aarch64-unknown-linux-musl"
)
if ($args.Count -gt 0) { $Targets = $args }

foreach ($triple in $Targets) {
    Write-Host ">> building ordius-helper for $triple"
    # Prefer cargo-zigbuild when available (works without Docker on Windows).
    if (Get-Command cargo-zigbuild -ErrorAction SilentlyContinue) {
        cargo zigbuild --release --target $triple -p ordius-helper
    } else {
        cargo build --release --target $triple -p ordius-helper
    }
    $src = "target\$triple\release\ordius-helper"
    $dst_dir = "crates\engine\embedded\helper\$triple"
    $dst = "$dst_dir\ordius-helper"
    New-Item -ItemType Directory -Force -Path $dst_dir | Out-Null
    Copy-Item -Force $src $dst
    $size = (Get-Item $dst).Length
    Write-Host ">> $dst  $size bytes"
}
Write-Host "Done — run ``cargo build -p ordius-engine`` to pick up the embedded binaries."
