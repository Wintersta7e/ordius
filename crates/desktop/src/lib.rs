//! Ordius Tauri 2 host. Owns the long-lived `Engine` for the
//! desktop process and exposes IPC commands the React UI calls.
//!
//! Phase 1 wires the bare shell — commands land in the next phase.

/// Boot the Tauri runtime. Called from `main.rs` and (when
/// `mobile` cfg is set) from Tauri's mobile entry-point macro.
#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .setup(|_app| Ok(()))
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
