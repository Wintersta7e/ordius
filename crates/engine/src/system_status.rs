//! Engine-side data sources for the GUI's Home left-rail "System" card.
//!
//! Reports disk usage of the engine home + per-endpoint
//! reachability hints. Service pings (`Ollama` / `OpenAI` etc.) are
//! out of scope for v1.0 — the report carries placeholder entries
//! so the Home card has a shape to render against.

use serde::{Deserialize, Serialize};
use std::path::Path;

/// Snapshot of engine-side state the GUI surfaces on the Home page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SystemStatus {
    /// Size of `<home>/runs.db` in bytes, or 0 if the file is missing.
    pub runs_db_bytes: u64,
    /// Size of `<home>/workspaces/` in bytes (sum of immediate children).
    pub workspaces_bytes: u64,
    /// Engine package version string at build time.
    pub engine_version: &'static str,
    /// Reachability hints for registered endpoints. Empty in v1.0 —
    /// the GUI renders placeholder rows from this list.
    pub endpoints: Vec<EndpointStatus>,
}

/// Per-endpoint reachability hint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointStatus {
    /// Endpoint id (matches `settings.model_endpoints[].id`).
    pub id: String,
    /// Endpoint display name (mirrors settings).
    pub name: String,
    /// `ok` | `down` | `unknown` — `unknown` until a real ping lands.
    pub state: String,
}

/// Read disk usage figures for the Home left-rail.
#[must_use]
pub fn snapshot(home: &Path) -> SystemStatus {
    let runs_db = home.join("runs.db");
    let runs_db_bytes = std::fs::metadata(&runs_db).map_or(0, |m| m.len());
    let workspaces_dir = home.join("workspaces");
    let workspaces_bytes = dir_size(&workspaces_dir);
    SystemStatus {
        runs_db_bytes,
        workspaces_bytes,
        engine_version: env!("CARGO_PKG_VERSION"),
        endpoints: Vec::new(),
    }
}

/// Best-effort sum of file sizes under `dir`. Walks one level deep
/// only — the GUI just needs a rough byte counter, not a precise
/// recursive tally. Missing dir reports 0.
fn dir_size(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut total = 0u64;
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else { continue };
        if meta.is_file() {
            total = total.saturating_add(meta.len());
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn snapshot_handles_empty_home() {
        let home = TempDir::new().unwrap();
        let s = snapshot(home.path());
        assert_eq!(s.runs_db_bytes, 0);
        assert_eq!(s.workspaces_bytes, 0);
        assert!(!s.engine_version.is_empty());
        assert!(s.endpoints.is_empty());
    }

    #[test]
    fn snapshot_reports_runs_db_size() {
        let home = TempDir::new().unwrap();
        std::fs::write(home.path().join("runs.db"), b"x".repeat(2048)).unwrap();
        let s = snapshot(home.path());
        assert_eq!(s.runs_db_bytes, 2048);
    }
}
