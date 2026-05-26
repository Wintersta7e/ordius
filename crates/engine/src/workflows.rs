//! On-disk workflow storage.
//!
//! Workflows live as JSON (or YAML) under `<home>/workflows/<id>.json`.
//! The engine itself only ever loads workflows from arbitrary paths
//! (via [`crate::loader::load_workflow`]); both the CLI and the Tauri
//! GUI need the additional listing / saving / id-keyed lookup
//! helpers exposed here so the surface doesn't drift between them.

use crate::environment::runtime::{
    ResourceRegistry, WorkflowId, WorkflowScopeError, install_workflow_resources,
    remove_workflow_scope,
};
use crate::loader::{LoadError, load_workflow};
use crate::types::Workflow;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Failure modes for the workflow filesystem helpers.
#[derive(Debug, Error)]
pub enum WorkflowsError {
    /// Filesystem read / write / mkdir error.
    #[error("io {context}: {source}")]
    Io {
        /// What was being attempted (e.g. `"read_dir <home>/workflows"`).
        context: String,
        /// Underlying `io::Error`.
        #[source]
        source: std::io::Error,
    },
    /// One of the loader's failure modes for a specific path.
    #[error("load {path}: {source}")]
    Load {
        /// Display-formatted offending path.
        path: String,
        /// Loader error.
        #[source]
        source: LoadError,
    },
    /// JSON serialisation failure when saving.
    #[error("serialise workflow {id}: {source}")]
    Serialise {
        /// Workflow id that failed to serialise.
        id: String,
        /// `serde_json::Error`.
        #[source]
        source: serde_json::Error,
    },
    /// Workflow-scope installation rejected by the registry (e.g.
    /// `override_lower_scope` was not set and an id collides with a built-in).
    #[error("install workflow scope: {0}")]
    Scope(#[from] WorkflowScopeError),
    /// Catch-all for engine-level invariants that don't fit the other
    /// variants — e.g. clone-id collision space exhausted.
    #[error("{0}")]
    Other(String),
}

/// `<home>/workflows/`.
#[must_use]
pub fn dir(home: &Path) -> PathBuf {
    home.join("workflows")
}

/// `<home>/workflows/<id>.json`.
#[must_use]
pub fn path(home: &Path, id: &str) -> PathBuf {
    dir(home).join(format!("{id}.json"))
}

/// One per-file load failure: the offending path + the loader's error.
pub type LoadFailure = (PathBuf, LoadError);

/// Read every `*.json` file under `<home>/workflows/`.
///
/// Returns the parsed [`Workflow`]s sorted by id. Files that fail
/// to parse are returned in the second tuple element so callers
/// can surface them (CLI prints a warning; GUI shows an error
/// badge in the palette). `<home>/workflows/` not existing is not
/// an error — fresh installs have no workflows until import.
pub fn list(home: &Path) -> Result<(Vec<Workflow>, Vec<LoadFailure>), WorkflowsError> {
    let dir = dir(home);
    if !dir.exists() {
        return Ok((Vec::new(), Vec::new()));
    }
    let entries = std::fs::read_dir(&dir).map_err(|e| WorkflowsError::Io {
        context: format!("read_dir {}", dir.display()),
        source: e,
    })?;
    let mut paths: Vec<PathBuf> = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| WorkflowsError::Io {
            context: format!("read_dir entry under {}", dir.display()),
            source: e,
        })?;
        let p = entry.path();
        if p.extension().and_then(std::ffi::OsStr::to_str) == Some("json") {
            paths.push(p);
        }
    }
    paths.sort();
    let mut workflows = Vec::with_capacity(paths.len());
    let mut errors: Vec<LoadFailure> = Vec::new();
    for p in paths {
        match load_workflow(&p) {
            Ok(wf) => workflows.push(wf),
            Err(err) => errors.push((p, err)),
        }
    }
    workflows.sort_by(|a, b| a.id.cmp(&b.id));
    Ok((workflows, errors))
}

/// Load a single workflow by id. Returns the loader's error type
/// directly so callers can distinguish missing-file from parse
/// errors.
pub fn load(home: &Path, id: &str) -> Result<Workflow, WorkflowsError> {
    let p = path(home, id);
    load_workflow(&p).map_err(|e| WorkflowsError::Load {
        path: p.display().to_string(),
        source: e,
    })
}

/// Persist a workflow to `<home>/workflows/<wf.id>.json`. Creates
/// the directory if missing. Overwrites in place — callers wanting
/// rename semantics should mutate `wf.id` first.
pub fn save(home: &Path, wf: &Workflow) -> Result<(), WorkflowsError> {
    let dir = dir(home);
    std::fs::create_dir_all(&dir).map_err(|e| WorkflowsError::Io {
        context: format!("create {}", dir.display()),
        source: e,
    })?;
    let target = path(home, &wf.id);
    let json = serde_json::to_string_pretty(wf).map_err(|e| WorkflowsError::Serialise {
        id: wf.id.clone(),
        source: e,
    })?;
    std::fs::write(&target, json).map_err(|e| WorkflowsError::Io {
        context: format!("write {}", target.display()),
        source: e,
    })
}

/// Duplicate an existing workflow. The clone gets a fresh id
/// (`<source>-copy`, then `-copy-2`, `-copy-3`, ... to avoid
/// collisions) and a `(copy)` suffix on the display name. Returns
/// the saved clone.
pub fn duplicate(home: &Path, source_id: &str) -> Result<Workflow, WorkflowsError> {
    let original = load(home, source_id)?;
    let new_id = unique_clone_id(home, source_id)?;
    let mut clone = original;
    clone.id = new_id;
    clone.name = format!("{} (copy)", clone.name);
    save(home, &clone)?;
    Ok(clone)
}

const MAX_CLONE_ATTEMPTS: u32 = 999;

fn unique_clone_id(home: &Path, base: &str) -> Result<String, WorkflowsError> {
    // Strip a single trailing `-copy` or `-copy-<n>` so duplicating
    // a clone yields `foo-copy-2`, not `foo-copy-copy`.
    let root = if let Some((head, tail)) = base.rsplit_once("-copy-")
        && !tail.is_empty()
        && tail.chars().all(|c| c.is_ascii_digit())
    {
        head
    } else {
        base.strip_suffix("-copy").unwrap_or(base)
    };

    let mut candidate = format!("{root}-copy");
    let mut counter: u32 = 2;
    while path(home, &candidate).exists() {
        if counter > MAX_CLONE_ATTEMPTS {
            return Err(WorkflowsError::Other(format!(
                "could not find a free clone id under {root}-copy-* after {MAX_CLONE_ATTEMPTS} attempts",
            )));
        }
        candidate = format!("{root}-copy-{counter}");
        counter += 1;
    }
    Ok(candidate)
}

/// Delete a workflow by id. Returns `Ok(true)` if the file existed
/// and was removed, `Ok(false)` if the file was missing.
pub fn delete(home: &Path, id: &str) -> Result<bool, WorkflowsError> {
    let p = path(home, id);
    if !p.exists() {
        return Ok(false);
    }
    std::fs::remove_file(&p).map_err(|e| WorkflowsError::Io {
        context: format!("remove {}", p.display()),
        source: e,
    })?;
    Ok(true)
}

/// Install the workflow's `resources:` block under
/// `ScopeKey::Workflow { id: wf.id }`. Used as a helper by
/// [`load_in_registry`] and [`duplicate_in_registry`].
pub fn install_resources_into_registry(
    wf: &Workflow,
    registry: &ResourceRegistry,
) -> Result<usize, WorkflowsError> {
    let wf_id = WorkflowId(wf.id.clone());
    let count = install_workflow_resources(&wf_id, &wf.resources, registry)?;
    Ok(count)
}

/// Load a workflow and install its `resources:` block into the registry.
///
/// Combines [`load`] with [`install_resources_into_registry`]; rolls back
/// the scope on any load error so a partially-installed scope never lingers.
pub fn load_in_registry(
    home: &Path,
    id: &str,
    registry: &ResourceRegistry,
) -> Result<Workflow, WorkflowsError> {
    let wf = load(home, id)?;
    install_resources_into_registry(&wf, registry)?;
    Ok(wf)
}

/// Delete a workflow by id and drop its scope from the registry.
///
/// Returns `Ok(true)` if the file existed; `Ok(false)` if it was already
/// gone. The scope is dropped in both cases — orphaned scopes (file removed
/// out-of-band, then we are asked to clean up) get the same treatment as
/// the normal path.
pub fn delete_in_registry(
    home: &Path,
    id: &str,
    registry: &ResourceRegistry,
) -> Result<bool, WorkflowsError> {
    let wf_id = WorkflowId(id.to_string());
    let _removed = remove_workflow_scope(&wf_id, registry);
    delete(home, id)
}

/// Duplicate a workflow and install the clone's scope into the registry.
///
/// Returns the saved clone. Failures during scope installation roll back
/// the on-disk file so the registry and disk stay in sync.
pub fn duplicate_in_registry(
    home: &Path,
    source_id: &str,
    registry: &ResourceRegistry,
) -> Result<Workflow, WorkflowsError> {
    let clone = duplicate(home, source_id)?;
    if let Err(err) = install_resources_into_registry(&clone, registry) {
        // Best-effort: drop the file we just wrote so disk doesn't keep
        // a workflow whose scope is missing from the registry.
        let _drop_orphan = delete(home, &clone.id);
        return Err(err);
    }
    Ok(clone)
}

#[cfg(test)]
mod tests;
