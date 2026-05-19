//! Application-wide preferences.
//!
//! The GUI's `Settings` screen owns retention, concurrency, model
//! endpoints, appearance — all of which need to survive process
//! restarts. The engine itself doesn't yet enforce these (max
//! concurrent runs is the only one with semantic meaning today,
//! and it's still always 1 in v1.0); the catalog lives here so
//! the Tauri layer has somewhere to round-trip them.
//!
//! Persistence: a single JSON file at `<home>/settings.json`,
//! with sane defaults when the file is missing.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Failure modes for the settings catalog.
#[derive(Debug, Error)]
pub enum SettingsError {
    /// Filesystem read / write error.
    #[error("io {context}: {source}")]
    Io {
        /// What was being attempted.
        context: String,
        /// Underlying `io::Error`.
        #[source]
        source: std::io::Error,
    },
    /// `<home>/settings.json` failed to parse.
    #[error("parse settings.json: {0}")]
    Parse(String),
}

/// Top-level settings object. Every field has a sane default so a
/// fresh `<home>/settings.json` (or none at all) yields a usable
/// configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Settings {
    /// `dark` | `light` — drives `document.documentElement.dataset.theme`.
    #[serde(default = "default_theme")]
    pub theme: String,
    /// `left` | `right` — palette pane position in the editor.
    #[serde(default = "default_palette_side")]
    pub palette_side: String,
    /// `bezier` | `orthogonal` | `straight` — canvas edge style.
    #[serde(default = "default_edge_style")]
    pub edge_style: String,
    /// `comfortable` | `rich` — content density.
    #[serde(default = "default_density")]
    pub density: String,
    /// `dots` | `lines` | `off` — canvas grid render.
    #[serde(default = "default_grid")]
    pub grid: String,
    /// `jewel` | `citrus` | `glacier` — category hue palette.
    #[serde(default = "default_color_scheme")]
    pub color_scheme: String,
    /// Max number of concurrent runs the dispatcher allows.
    #[serde(default = "default_concurrency")]
    pub max_concurrent_runs: u32,
    /// How long to keep `runs.db` rows before sweeping.
    #[serde(default = "default_retention_days")]
    pub retention_days: u32,
    /// OpenAI-compatible endpoints registered by the user.
    #[serde(default)]
    pub model_endpoints: Vec<ModelEndpoint>,
}

/// One model-provider endpoint (`Ollama`, `OpenAI`, `llama.cpp`, …).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelEndpoint {
    /// Stable id (UUID v4 string).
    pub id: String,
    /// Display name in the GUI picker.
    pub name: String,
    /// Base URL — e.g. `http://localhost:11434/v1`.
    pub base_url: String,
    /// Optional `{{secrets.X}}` reference for the API key.
    #[serde(default)]
    pub api_key_secret: Option<String>,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            theme: default_theme(),
            palette_side: default_palette_side(),
            edge_style: default_edge_style(),
            density: default_density(),
            grid: default_grid(),
            color_scheme: default_color_scheme(),
            max_concurrent_runs: default_concurrency(),
            retention_days: default_retention_days(),
            model_endpoints: Vec::new(),
        }
    }
}

fn default_theme() -> String {
    "dark".into()
}
fn default_palette_side() -> String {
    "left".into()
}
fn default_edge_style() -> String {
    "orthogonal".into()
}
fn default_density() -> String {
    "rich".into()
}
fn default_grid() -> String {
    "dots".into()
}
fn default_color_scheme() -> String {
    "jewel".into()
}
const fn default_concurrency() -> u32 {
    4
}
const fn default_retention_days() -> u32 {
    30
}

fn settings_path(home: &Path) -> PathBuf {
    home.join("settings.json")
}

/// Load settings from `<home>/settings.json`. Returns
/// [`Settings::default`] when the file doesn't exist.
pub fn load(home: &Path) -> Result<Settings, SettingsError> {
    let p = settings_path(home);
    if !p.exists() {
        return Ok(Settings::default());
    }
    let body = std::fs::read_to_string(&p).map_err(|e| SettingsError::Io {
        context: format!("read {}", p.display()),
        source: e,
    })?;
    serde_json::from_str(&body).map_err(|e| SettingsError::Parse(e.to_string()))
}

/// Persist settings to `<home>/settings.json`. Replaces in place;
/// callers wanting patch semantics should `load → mutate → save`.
pub fn save(home: &Path, settings: &Settings) -> Result<(), SettingsError> {
    std::fs::create_dir_all(home).map_err(|e| SettingsError::Io {
        context: format!("create {}", home.display()),
        source: e,
    })?;
    let body =
        serde_json::to_string_pretty(settings).map_err(|e| SettingsError::Parse(e.to_string()))?;
    let p = settings_path(home);
    std::fs::write(&p, body).map_err(|e| SettingsError::Io {
        context: format!("write {}", p.display()),
        source: e,
    })
}

#[cfg(test)]
mod tests;
