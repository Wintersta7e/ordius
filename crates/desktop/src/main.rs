//! Ordius desktop binary entry. Delegates to `ordius_desktop::run`
//! so the same Tauri setup also drives the mobile entry point macro
//! and any test harness that wants to spin up the host without
//! linking through `main`.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    ordius_desktop::run();
}
