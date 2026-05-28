//! Ordius Tauri 2 host. Owns the long-lived `Engine` for the
//! desktop process and exposes IPC commands the React UI calls.

pub mod commands;
pub mod dto;
pub mod state;

use ordius_engine::Engine;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tauri::{Emitter, Manager};

/// Boot the Tauri runtime. Called from `main.rs` and (when
/// `mobile` cfg is set) from Tauri's mobile entry-point macro.
#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    init_tracing();

    tauri::Builder::default()
        .setup(|app| {
            install_keyring_backend();
            let home = resolve_engine_home();
            let engine = tauri::async_runtime::block_on(async { Engine::new(home).await })
                .map_err(|e| -> Box<dyn std::error::Error> {
                    Box::new(EngineInitError(e.to_string()))
                })?;
            let engine = Arc::new(engine);
            spawn_env_refresh_bridge(app, &engine);
            app.manage(state::AppState::new(engine));
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::list_workflows,
            commands::load_workflow,
            commands::save_workflow,
            commands::validate_workflow,
            commands::delete_workflow,
            commands::duplicate_workflow,
            commands::run_workflow,
            commands::stop_run,
            commands::deliver_event,
            commands::list_runs,
            commands::get_run,
            commands::list_node_types,
            commands::list_workspaces,
            commands::add_workspace,
            commands::remove_workspace,
            commands::rename_workspace,
            commands::list_secrets,
            commands::add_secret,
            commands::remove_secret,
            commands::get_settings,
            commands::set_settings,
            commands::system_status,
            commands::environment_list,
            commands::environment_refresh,
            commands::environment_add,
            commands::environment_remove,
            commands::environment_set_enabled,
            commands::environment_add_resource,
            commands::environment_remove_resource,
            commands::environment_definitions,
            commands::environment_test_host_direct,
            commands::environment_enable_host_direct,
            // Loud-failure shims for the session-C names. Phase F deletes
            // these once the frontend has migrated to the `environment_*`
            // family.
            commands::system_environment,
            commands::refresh_environment,
            commands::add_custom_namespace,
            commands::remove_custom_namespace,
            commands::set_namespace_enabled,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

fn spawn_env_refresh_bridge(app: &tauri::App, engine: &Engine) {
    let app_handle = app.handle().clone();
    let mut rx = engine.subscribe_env_refresh();
    tauri::async_runtime::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(event) => {
                    if let Err(e) = app_handle.emit("env_refresh_completed", event) {
                        tracing::warn!(error = ?e, "failed to emit env refresh event");
                    }
                },
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    tracing::warn!(skipped, "env refresh event listener lagged");
                },
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });
}

/// Wrap engine init failure in a `std::error::Error`-compatible type
/// so the setup hook's `Box<dyn Error>` return is happy. Tauri's
/// setup signature doesn't accept arbitrary error strings directly.
#[derive(Debug)]
struct EngineInitError(String);
impl std::fmt::Display for EngineInitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "engine init failed: {}", self.0)
    }
}
impl std::error::Error for EngineInitError {}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let init = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
    drop(init);
}

/// `ORDIUS_TEST_KEYRING=1` installs the in-memory sample backend
/// (matches the CLI's test path) so dev runs on WSL2 / CI hosts
/// without a real keyring service don't crash. Production gets the
/// platform-native store. Failures are logged but non-fatal —
/// secrets-using workflows surface the error later, the rest of
/// the app keeps working.
fn install_keyring_backend() {
    let result = if std::env::var_os("ORDIUS_TEST_KEYRING").is_some() {
        let cfg: HashMap<&str, &str> = HashMap::from([("persist", "false")]);
        keyring::use_sample_store(&cfg)
    } else {
        // `not_keyutils = true` picks DBus secret-service on Linux
        // over the kernel keyutils backend — keyutils flushes on
        // logout, secret-service persists across sessions.
        keyring::use_native_store(true)
    };
    if let Err(e) = result {
        tracing::warn!(error = ?e, "keyring backend install failed; secrets disabled until fixed");
    }
}

/// Pick the engine home directory.
///
/// `$ORDIUS_HOME` wins so dev sessions can point at a scratch dir;
/// otherwise `$HOME/.ordius` (Unix) / `$USERPROFILE/.ordius` (Windows).
/// Falls back to `./.ordius` when neither env var is set — the same
/// fallback the CLI uses.
fn resolve_engine_home() -> PathBuf {
    if let Some(h) = std::env::var_os("ORDIUS_HOME") {
        return PathBuf::from(h);
    }
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map_or_else(
            || PathBuf::from(".ordius"),
            |h| PathBuf::from(h).join(".ordius"),
        )
}
