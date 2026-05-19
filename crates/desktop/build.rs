//! Tauri 2 build script. Reads `tauri.conf.json` + the
//! `capabilities/` dir and generates the Rust context the runtime
//! macro needs.

fn main() {
    tauri_build::build();
}
