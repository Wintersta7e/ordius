//! `RunSnapshot`: per-run frozen view of registry + dispatchers + catalogs.
//!
//! Built once by `Engine::build_run_snapshot` at the top of `Engine::start_run`
//! and threaded through every `RunContext` for the duration of the run. The
//! engine's live registry, dispatchers, and catalogs can refresh underneath an
//! active run without affecting it — the snapshot holds its own Arcs.

use std::collections::HashMap;
use std::sync::Arc;

use super::dispatcher::Dispatcher;
use super::env::{EnvId, EnvSpec, WorkflowId, WorkspaceBinding};
use super::registry::RegistryInner;
use super::run_catalog::RunCatalog;

/// Per-run frozen view of the environment substrate.
///
/// Constructed once by `Engine::build_run_snapshot` at the top of
/// `Engine::start_run`. Cloned cheaply (every field is an `Arc`); the run
/// loop and `RunContext` both hold `Arc<RunSnapshot>` clones.
pub struct RunSnapshot {
    /// Run id this snapshot belongs to.
    pub run_id: String,
    /// Workflow id this snapshot belongs to.
    pub workflow_id: WorkflowId,
    /// Effective default env for nodes without a `target_env`.
    pub default_env: EnvId,
    /// Immutable registry snapshot — used by re-probe so single-resource
    /// re-probes always agree with what the run started with. Includes
    /// `ScopeKey::EnvLocal { id }` overlays for every env in scope (installed
    /// by a later task).
    pub registry: Arc<RegistryInner>,
    /// One dispatcher Arc per env in scope.
    ///
    /// The map covers `default_env` plus every `target_env` referenced by a
    /// node in the workflow; envs not referenced are not pre-cloned.
    pub dispatchers: Arc<HashMap<EnvId, Arc<dyn Dispatcher>>>,
    /// Frozen `Found` entries + run-local overlay, one per env in scope.
    pub catalogs: Arc<HashMap<EnvId, Arc<RunCatalog>>>,
    /// `EnvSpec` per env in scope. Frozen at snapshot construction; used by
    /// executor gates that consult `host_direct_verifications` (HTTP
    /// `Origin::Host` loopback check) and any code that needs to read
    /// env-static configuration (workspace bindings, sync strategies)
    /// without re-reading the engine's env registry.
    pub specs: Arc<HashMap<EnvId, EnvSpec>>,
}

impl RunSnapshot {
    /// Return the dispatcher for an env, or `None` if not in scope.
    #[must_use]
    pub fn dispatcher(&self, env: &EnvId) -> Option<&Arc<dyn Dispatcher>> {
        self.dispatchers.get(env)
    }

    /// Return the catalog for an env, or `None` if not in scope.
    #[must_use]
    pub fn catalog(&self, env: &EnvId) -> Option<&Arc<RunCatalog>> {
        self.catalogs.get(env)
    }

    /// Return the frozen `EnvSpec` for an env, or `None` if not in scope.
    /// Used by the `Origin::Host` HTTP gate to consult
    /// `host_direct_verifications` without holding an engine handle.
    #[must_use]
    pub fn spec_for(&self, env: &EnvId) -> Option<&EnvSpec> {
        self.specs.get(env)
    }

    /// Return the workspace binding for an env in scope.
    ///
    /// - `Local`     → [`WorkspaceBinding::Shared`] (same FS, no translation needed)
    /// - `WslDistro` → [`WorkspaceBinding::Translated`] (`translate_path` maps `/mnt/c/…`)
    /// - `Ssh`       → the `workspace_binding` field from the spec
    /// - `Container` → the `workspace_binding` field from the spec
    /// - env not found → [`WorkspaceBinding::Unsupported`]
    #[must_use]
    pub fn workspace_binding(&self, env: &EnvId) -> WorkspaceBinding {
        match self.specs.get(env) {
            Some(EnvSpec::Local { .. }) => WorkspaceBinding::Shared,
            Some(EnvSpec::WslDistro { .. }) => WorkspaceBinding::Translated,
            Some(
                EnvSpec::Ssh {
                    workspace_binding, ..
                }
                | EnvSpec::Container {
                    workspace_binding, ..
                },
            ) => workspace_binding.clone(),
            None => WorkspaceBinding::Unsupported,
        }
    }
}
