//! On-disk workflow storage.
//!
//! Workflows live as JSON (or YAML) under `<home>/workflows/<id>.json`.
//! The engine itself only ever loads workflows from arbitrary paths
//! (via [`crate::loader::load_workflow`]); both the CLI and the Tauri
//! GUI need the additional listing / saving / id-keyed lookup
//! helpers exposed here so the surface doesn't drift between them.

use crate::environment::runtime::boot_probe::DisabledEnv;
use crate::environment::runtime::{
    EnvId, EnvRegistry, ResourceRef, ResourceRegistry, WorkflowId, WorkflowScopeError,
    install_workflow_resources, remove_workflow_scope, snapshot_workflow_scope,
};
use crate::loader::{LoadError, load_workflow};
use crate::types::Workflow;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Non-fatal lint emitted during workflow load. Surfaced to the
/// caller alongside the loaded `Workflow`. The engine does not act
/// on warnings; the UI surfaces them in the editor (Phase F).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowWarning {
    /// Id of the node the warning applies to.
    pub node_id: String,
    /// Discriminant for matching on the warning kind.
    pub kind: WorkflowWarningKind,
    /// Human-readable explanation suitable for UI surfacing.
    pub message: String,
}

/// Discriminant for [`WorkflowWarning`]. Marked `#[non_exhaustive]` so
/// later phases can add new lint kinds (Phase E will add
/// `ResourceUnreachableInEnv`, etc.) without breaking downstream
/// matches.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum WorkflowWarningKind {
    /// `http.url` is a loopback literal but the node targets a non-local env.
    LoopbackUrlInRemoteEnv,
}

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
    /// Workflow JSON references a node-type id that has been retired in
    /// favour of a new name. The loader surfaces the rename target so the
    /// user can fix the file without guessing.
    #[error("workflow node '{node_id}' uses reserved type id '{id}'; rename to '{replacement}'")]
    ReservedNodeType {
        /// Retired node-type id that appeared in the workflow.
        id: String,
        /// Current node-type id the user should switch to.
        replacement: String,
        /// Id of the offending node inside the workflow.
        node_id: String,
    },
    /// A node's `config.resource` field names a resource id that is not
    /// declared at any scope visible to the workflow.
    #[error("workflow node '{node_id}' references unknown resource id '{resource_id}'")]
    ResourceNotInRegistry {
        /// Id of the offending node inside the workflow.
        node_id: String,
        /// Resource id that failed to resolve.
        resource_id: String,
    },
    /// A node's long-form resource ref requires a capability that the
    /// resolved resource does not advertise.
    #[error(
        "workflow node '{node_id}' resource '{resource_id}' does not advertise required capability '{capability}'"
    )]
    CapabilityNotAdvertised {
        /// Id of the offending node inside the workflow.
        node_id: String,
        /// Resource id whose advertisement was insufficient.
        resource_id: String,
        /// Debug form of the required capability.
        capability: String,
    },
    /// A node's `config` block could not be parsed into a Phase D
    /// runtime type (e.g. malformed `ResourceRef`).
    #[error("workflow node '{node_id}' has invalid config: {reason}")]
    InvalidNodeConfig {
        /// Id of the offending node inside the workflow.
        node_id: String,
        /// Underlying parse failure (already display-formatted).
        reason: String,
    },
    /// A node names a `target_env` that is not registered with the engine.
    /// Emitted at load so users get the error before run start.
    #[error("node '{node_id}': target_env '{env_id}' is not in the engine's env registry")]
    TargetEnvUnknown {
        /// Env id named by the offending node.
        env_id: String,
        /// Id of the offending node inside the workflow.
        node_id: String,
    },
    /// A node names a `target_env` that is registered but currently
    /// disabled. Distinct from `TargetEnvUnknown` so the UI can render
    /// "Re-enable in Settings" guidance.
    #[error(
        "node '{node_id}': target_env '{env_id}' is registered but disabled. \
         Re-enable it in Settings"
    )]
    TargetEnvDisabled {
        /// Disabled env id named by the offending node.
        env_id: String,
        /// Id of the offending node inside the workflow.
        node_id: String,
    },
    /// The workflow's top-level `default_env` is not registered with
    /// the engine.
    #[error("workflow default_env '{env_id}' is not in the engine's env registry")]
    DefaultEnvUnknown {
        /// Env id named by `workflow.default_env`.
        env_id: String,
    },
    /// The workflow's top-level `default_env` is registered but disabled.
    #[error(
        "workflow default_env '{env_id}' is registered but disabled. \
         Re-enable it in Settings"
    )]
    DefaultEnvDisabled {
        /// Disabled env id named by `workflow.default_env`.
        env_id: String,
    },
}

/// Convert the loader's [`LoadError`] into a [`WorkflowsError`], preserving
/// the distinguished `ReservedNodeType` variant so tests + UI surfaces can
/// still pattern-match on it without inspecting the wrapped loader error.
fn loader_err(path: &Path, err: LoadError) -> WorkflowsError {
    match err {
        LoadError::ReservedNodeType {
            id,
            replacement,
            node_id,
        } => WorkflowsError::ReservedNodeType {
            id,
            replacement,
            node_id,
        },
        other => WorkflowsError::Load {
            path: path.display().to_string(),
            source: other,
        },
    }
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
///
/// Delegates to [`crate::loader::load_workflow`] which already runs the
/// reserved-id check, so callers automatically surface
/// [`WorkflowsError::ReservedNodeType`] for retired node-type ids
/// without a separate post-load pass.
pub fn load(home: &Path, id: &str) -> Result<Workflow, WorkflowsError> {
    let p = path(home, id);
    load_workflow(&p).map_err(|e| loader_err(&p, e))
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

/// Validate workflow nodes against the resource registry. Returns
/// non-fatal warnings; hard errors are returned via the `Err` arm.
///
/// Phase D validates:
/// - `resource.id` resolution against the registry's scope chain
/// - `required_capability` (if set) is in the resource's
///   `advertised_capabilities` (empty list is treated as untyped and
///   silently allowed, matching `Tasks 9/10` behavior)
/// - `http` nodes with literal `localhost` / `127.0.0.1` / `0.0.0.0`
///   URLs and a non-local `target_env` (`LoopbackUrlInRemoteEnv`
///   warning)
///
/// Phase E adds `target_env` and `default_env` existence checks against
/// the engine's `EnvRegistry`. `EnvId::local()` is always treated as
/// valid (the boot probe always synthesises it). Envs that exist but
/// are flagged disabled in `env_disabled` surface as
/// [`WorkflowsError::TargetEnvDisabled`] / [`WorkflowsError::DefaultEnvDisabled`]
/// so the IPC layer can render "Re-enable in Settings" guidance.
///
/// `resource known but probe NotFound in resolved env` is still deferred
/// to later Phase E tasks (no env-scoped catalog probe at load time).
fn validate_nodes<S: std::hash::BuildHasher>(
    workflow: &Workflow,
    registry: &ResourceRegistry,
    env_registry: &EnvRegistry,
    env_disabled: &HashMap<EnvId, DisabledEnv, S>,
) -> Result<Vec<WorkflowWarning>, WorkflowsError> {
    let snap = registry.snapshot();
    let env = EnvId::local();
    let wf = WorkflowId(workflow.id.clone());
    let mut warnings: Vec<WorkflowWarning> = Vec::new();

    // Workflow-level default_env check.
    if let Some(default_env) = &workflow.default_env
        && *default_env != EnvId::local()
        && env_registry.get(default_env).is_none()
    {
        if env_disabled.contains_key(default_env) {
            return Err(WorkflowsError::DefaultEnvDisabled {
                env_id: default_env.as_str().to_string(),
            });
        }
        return Err(WorkflowsError::DefaultEnvUnknown {
            env_id: default_env.as_str().to_string(),
        });
    }

    for node in &workflow.nodes {
        // Per-node target_env check.
        if let Some(target_env) = &node.target_env
            && *target_env != EnvId::local()
            && env_registry.get(target_env).is_none()
        {
            if env_disabled.contains_key(target_env) {
                return Err(WorkflowsError::TargetEnvDisabled {
                    env_id: target_env.as_str().to_string(),
                    node_id: node.id.clone(),
                });
            }
            return Err(WorkflowsError::TargetEnvUnknown {
                env_id: target_env.as_str().to_string(),
                node_id: node.id.clone(),
            });
        }
        // 1. `resource` ref resolution + capability assertion.
        if let Some(rref_val) = node.config.get("resource") {
            let rref: ResourceRef = serde_json::from_value(rref_val.clone()).map_err(|e| {
                WorkflowsError::InvalidNodeConfig {
                    node_id: node.id.clone(),
                    reason: format!("invalid resource ref: {e}"),
                }
            })?;

            let Some((def, _scope)) = snap.resolve(rref.id(), &env, Some(&wf)) else {
                return Err(WorkflowsError::ResourceNotInRegistry {
                    node_id: node.id.clone(),
                    resource_id: rref.id().0.clone(),
                });
            };

            if let Some(cap) = rref.required_capability()
                && !def.advertised_capabilities.contains(&cap)
            {
                // Strict: an explicit required_capability must be in the
                // resource's advertised list. Untyped resources (empty
                // advertised list) only resolve through bare ResourceRefs
                // that don't request a specific capability.
                return Err(WorkflowsError::CapabilityNotAdvertised {
                    node_id: node.id.clone(),
                    resource_id: rref.id().0.clone(),
                    capability: format!("{cap:?}"),
                });
            }
        }

        // 2. `http` node validations.
        if node.ty == "http" {
            // 2a. Validate `origin` wire form at load so users get the
            //     error at edit/load rather than mid-run.
            if let Some(origin_val) = node.config.get("origin") {
                let _origin: crate::executor::builtins::http::Origin =
                    serde_json::from_value(origin_val.clone()).map_err(|e| {
                        WorkflowsError::InvalidNodeConfig {
                            node_id: node.id.clone(),
                            reason: format!("invalid origin: {e}"),
                        }
                    })?;
            }

            // 2b. Loopback-in-remote-env lint.
            if let Some(url) = node.config.get("url").and_then(serde_json::Value::as_str)
                && let Some(target) = &node.target_env
            {
                let target_str = target.as_str();
                let is_local = target_str == EnvId::LOCAL;
                let is_loopback_literal = url.contains("127.0.0.1")
                    || url.contains("localhost")
                    || url.contains("0.0.0.0");
                if !is_local && is_loopback_literal {
                    warnings.push(WorkflowWarning {
                        node_id: node.id.clone(),
                        kind: WorkflowWarningKind::LoopbackUrlInRemoteEnv,
                        message: format!(
                            "node {} targets env {target_str} but its http.url is a \
                             loopback literal; the request will not reach the env \
                             (likely a bug)",
                            node.id
                        ),
                    });
                }
            }
        }
    }

    Ok(warnings)
}

/// Load a workflow, install its `resources:` block into the registry,
/// and validate its nodes against the registry.
///
/// Combines [`load`] with [`install_resources_into_registry`] and the
/// validation pass. Validation runs *after* the workflow scope is
/// installed so workflow-scope resources are visible to it. Returns the
/// loaded workflow together with any non-fatal warnings emitted by the
/// validator.
///
/// `install_workflow_resources` is replace-then-install: it drops the
/// existing scope before installing the new one. If a workflow id was
/// already loaded with a valid scope and the *reload* fails validation,
/// naively calling `remove_workflow_scope` as cleanup would wipe the
/// previously-valid scope entirely. Instead, snapshot the prior scope
/// before mutating and restore it on validation failure: callers that
/// fail to reload keep the last-known-good resources visible.
pub fn load_in_registry<S: std::hash::BuildHasher>(
    home: &Path,
    id: &str,
    registry: &ResourceRegistry,
    env_registry: &EnvRegistry,
    env_disabled: &HashMap<EnvId, DisabledEnv, S>,
) -> Result<(Workflow, Vec<WorkflowWarning>), WorkflowsError> {
    let wf = load(home, id)?;
    let wf_id = WorkflowId(wf.id.clone());

    // Snapshot before any mutation so we can restore on validation failure.
    let prior_scope = snapshot_workflow_scope(&wf_id, registry);

    install_resources_into_registry(&wf, registry)?;

    let warnings = match validate_nodes(&wf, registry, env_registry, env_disabled) {
        Ok(w) => w,
        Err(e) => {
            // Restore the prior scope. If prior was empty this collapses
            // to `remove_workflow_scope`; if prior had resources, re-install
            // them to recover the pre-install state.
            let _removed = remove_workflow_scope(&wf_id, registry);
            if !prior_scope.is_empty()
                && let Err(install_err) = install_workflow_resources(&wf_id, &prior_scope, registry)
            {
                tracing::error!(
                    workflow_id = %wf_id.0,
                    ?install_err,
                    "load_in_registry: failed to restore prior workflow scope after validation failure"
                );
            }
            return Err(e);
        },
    };
    Ok((wf, warnings))
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
    // Delete the file first. If that fails (permission, locked, etc.),
    // the workflow remains on disk and its in-memory scope MUST stay
    // installed so a future probe can still resolve its resources. Only
    // after a successful Ok(true)/Ok(false) do we clear the scope. The
    // orphaned-scope cleanup (file removed out-of-band) still works
    // because `delete` returns Ok(false) for the missing-file case.
    let removed = delete(home, id)?;
    let wf_id = WorkflowId(id.to_string());
    let _scope_count = remove_workflow_scope(&wf_id, registry);
    Ok(removed)
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
