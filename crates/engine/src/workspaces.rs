//! User-registered project directories (workspaces).
//!
//! A workspace is a directory the user has added through the GUI's
//! `Settings → Workspaces` table. It carries a stable id, a display
//! name, and an absolute path that becomes the CWD of runs spawned
//! against that workspace. The engine itself doesn't dispatch
//! workspace-aware work yet — `RunContext::workspace` is filled by
//! the engine home today — but the catalog lives here so the Tauri
//! layer can persist + surface it to the GUI ahead of the wiring.
//!
//! Persistence: a single JSON file at `<home>/workspaces.json`.
//! Missing file is treated as an empty list (fresh install).

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;
use uuid::Uuid;

/// A registered project directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Workspace {
    /// Stable identifier (UUID v4 string).
    pub id: String,
    /// Display name shown in the GUI workspace picker.
    pub name: String,
    /// Absolute path on disk.
    pub path: PathBuf,
}

/// Failure modes for the workspace catalog.
#[derive(Debug, Error)]
pub enum WorkspacesError {
    /// Filesystem read / write error.
    #[error("io {context}: {source}")]
    Io {
        /// What was being attempted.
        context: String,
        /// Underlying `io::Error`.
        #[source]
        source: std::io::Error,
    },
    /// `<home>/workspaces.json` failed to parse.
    #[error("parse workspaces.json: {0}")]
    Parse(String),
    /// The supplied path doesn't exist (or isn't a directory).
    #[error("workspace path not a directory: {0}")]
    NotADirectory(String),
    /// Tried to register a path that's already in the catalog.
    #[error("workspace already registered for path: {0}")]
    DuplicatePath(String),
    /// Tried to remove an id that isn't in the catalog.
    #[error("unknown workspace id: {0}")]
    Unknown(String),
}

fn catalog_path(home: &Path) -> PathBuf {
    home.join("workspaces.json")
}

/// Read every registered workspace. Returns an empty vector when
/// `<home>/workspaces.json` doesn't exist — that's a fresh install,
/// not an error.
pub fn list(home: &Path) -> Result<Vec<Workspace>, WorkspacesError> {
    let p = catalog_path(home);
    if !p.exists() {
        return Ok(Vec::new());
    }
    let body = std::fs::read_to_string(&p).map_err(|e| WorkspacesError::Io {
        context: format!("read {}", p.display()),
        source: e,
    })?;
    serde_json::from_str(&body).map_err(|e| WorkspacesError::Parse(e.to_string()))
}

/// Register a new workspace. Generates a fresh UUID id, refuses
/// non-directory paths and refuses duplicate paths.
pub fn add(home: &Path, name: &str, path: &Path) -> Result<Workspace, WorkspacesError> {
    if !path.is_dir() {
        return Err(WorkspacesError::NotADirectory(path.display().to_string()));
    }
    let canonical = path.canonicalize().map_err(|e| WorkspacesError::Io {
        context: format!("canonicalize {}", path.display()),
        source: e,
    })?;
    let mut catalog = list(home)?;
    if catalog.iter().any(|w| w.path == canonical) {
        return Err(WorkspacesError::DuplicatePath(
            canonical.display().to_string(),
        ));
    }
    let ws = Workspace {
        id: Uuid::new_v4().to_string(),
        name: name.to_string(),
        path: canonical,
    };
    catalog.push(ws.clone());
    write_catalog(home, &catalog)?;
    Ok(ws)
}

/// Remove a workspace by id. Returns `Ok(())` only when an entry
/// was actually removed.
pub fn remove(home: &Path, id: &str) -> Result<(), WorkspacesError> {
    let mut catalog = list(home)?;
    let len_before = catalog.len();
    catalog.retain(|w| w.id != id);
    if catalog.len() == len_before {
        return Err(WorkspacesError::Unknown(id.to_string()));
    }
    write_catalog(home, &catalog)
}

fn write_catalog(home: &Path, catalog: &[Workspace]) -> Result<(), WorkspacesError> {
    std::fs::create_dir_all(home).map_err(|e| WorkspacesError::Io {
        context: format!("create {}", home.display()),
        source: e,
    })?;
    let body =
        serde_json::to_string_pretty(catalog).map_err(|e| WorkspacesError::Parse(e.to_string()))?;
    let p = catalog_path(home);
    std::fs::write(&p, body).map_err(|e| WorkspacesError::Io {
        context: format!("write {}", p.display()),
        source: e,
    })
}

#[cfg(test)]
mod tests;
