//! Long-lived state Tauri commands lean on.

use ordius_engine::Engine;
use std::sync::Arc;

/// Tauri-managed state. One per process. Cloning the inner
/// `Arc<Engine>` is cheap and lets commands spawn tokio work
/// without holding a `tauri::State` guard across awaits.
pub struct AppState {
    /// Shared engine handle — `Arc` so per-command clones are cheap.
    pub engine: Arc<Engine>,
}

impl AppState {
    /// Wrap an existing engine.
    #[must_use]
    pub const fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }
}
