//! Workflow-scoped resource installation.
//!
//! Workflows may carry their own `resources:` block; this module installs
//! that block into the registry under `ScopeKey::Workflow { id }` when the
//! workflow is loaded, and removes the scope when the workflow is deleted.

use thiserror::Error;

use super::env::WorkflowId;
use super::error::RegistryError;
use super::registry::{ResourceRegistry, ScopeKey};
use super::resource::ResourceDefinition;

/// Failure modes for [`install_workflow_resources`].
#[derive(Debug, Error)]
pub enum WorkflowScopeError {
    /// The registry rejected the upsert (e.g. shadow collision without
    /// `override_lower_scope`).
    #[error("workflow {workflow_id} resource {id} rejected: {source}")]
    Registry {
        /// Workflow id that owned the rejected definition.
        workflow_id: String,
        /// Resource id that failed.
        id: String,
        /// Underlying registry error.
        #[source]
        source: RegistryError,
    },
}

/// Replace the workflow's scope with `resources`.
///
/// Drops any existing entries under `ScopeKey::Workflow { id }` first, then
/// upserts each definition in `resources`. On any upsert failure the workflow
/// scope is wiped before returning `Err`, so the registry never carries a
/// half-installed scope. Returns the count of installed definitions on
/// success.
///
/// This is "replace", not "upsert into existing": reloading a workflow whose
/// `resources:` block has shrunk or had ids renamed drops the gone entries.
pub fn install_workflow_resources(
    workflow_id: &WorkflowId,
    resources: &[ResourceDefinition],
    registry: &ResourceRegistry,
) -> Result<usize, WorkflowScopeError> {
    let scope = ScopeKey::Workflow {
        id: workflow_id.clone(),
    };
    // Start from an empty scope so a previous load's entries don't linger.
    registry.remove_scope(&scope);
    let mut written = 0_usize;
    for def in resources {
        if let Err(e) = registry.upsert(&scope, def) {
            // Roll back the partially-installed entries so the failed load
            // doesn't leak resources 1..written into the registry.
            registry.remove_scope(&scope);
            return Err(WorkflowScopeError::Registry {
                workflow_id: workflow_id.0.clone(),
                id: def.id.0.clone(),
                source: e,
            });
        }
        written += 1;
    }
    Ok(written)
}

/// Remove every definition the workflow installed. Returns the count of
/// removed definitions; `0` if the workflow had no scope (no-op).
pub fn remove_workflow_scope(workflow_id: &WorkflowId, registry: &ResourceRegistry) -> usize {
    registry.remove_scope(&ScopeKey::Workflow {
        id: workflow_id.clone(),
    })
}

/// Take a structured snapshot of the resources currently held at
/// `ScopeKey::Workflow { id }`. Returns owned [`ResourceDefinition`]s
/// — empty if no scope exists.
///
/// Used by the workflow loader to roll back to the pre-install state when
/// subsequent validation fails: a reload that fails validation must not
/// wipe a previously-valid scope, so the loader snapshots before mutating
/// and re-installs the snapshot on failure.
///
/// Clones the definitions out of the snapshot so the returned `Vec` is
/// self-contained and not lifetime-bound to the registry's internal state.
pub fn snapshot_workflow_scope(
    workflow_id: &WorkflowId,
    registry: &ResourceRegistry,
) -> Vec<ResourceDefinition> {
    let scope = ScopeKey::Workflow {
        id: workflow_id.clone(),
    };
    registry
        .snapshot()
        .layers
        .get(&scope)
        .map(|layer| layer.values().cloned().collect())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::environment::runtime::env::{EnvId, WorkflowId};
    use crate::environment::runtime::resource::{ProbeSpec, ResourceId, ResourceKind};

    fn wf_def(id: &str) -> ResourceDefinition {
        ResourceDefinition {
            id: ResourceId(id.into()),
            kind: ResourceKind::HttpEndpoint,
            advertised_capabilities: vec![],
            probe: ProbeSpec::Http {
                ports: vec![],
                routes: vec![],
                timeout_ms: None,
            },
            override_lower_scope: false,
        }
    }

    #[test]
    fn install_two_then_remove() {
        let reg = ResourceRegistry::new();
        let wf = WorkflowId("wf1".into());
        let written =
            install_workflow_resources(&wf, &[wf_def("a"), wf_def("b")], &reg).expect("ok");
        assert_eq!(written, 2);

        let snap = reg.snapshot();
        let (_, scope) = snap
            .resolve(&ResourceId("a".into()), &EnvId::local(), Some(&wf))
            .expect("a visible to wf");
        assert!(matches!(scope, ScopeKey::Workflow { .. }));

        let removed = remove_workflow_scope(&wf, &reg);
        assert_eq!(removed, 2);
        assert!(
            reg.snapshot()
                .resolve(&ResourceId("a".into()), &EnvId::local(), Some(&wf))
                .is_none()
        );
    }

    #[test]
    fn other_workflow_does_not_see_my_resources() {
        let reg = ResourceRegistry::new();
        let mine = WorkflowId("mine".into());
        let theirs = WorkflowId("theirs".into());
        install_workflow_resources(&mine, &[wf_def("only-mine")], &reg).expect("ok");

        let snap = reg.snapshot();
        assert!(
            snap.resolve(
                &ResourceId("only-mine".into()),
                &EnvId::local(),
                Some(&mine)
            )
            .is_some(),
            "mine sees it"
        );
        assert!(
            snap.resolve(
                &ResourceId("only-mine".into()),
                &EnvId::local(),
                Some(&theirs)
            )
            .is_none(),
            "theirs does not"
        );
    }

    #[test]
    fn shadow_collision_without_override_errors() {
        let reg = ResourceRegistry::new();
        reg.upsert(&ScopeKey::Builtin, &wf_def("ollama")).unwrap();
        let wf = WorkflowId("wf1".into());
        let mut clone = wf_def("ollama");
        clone.override_lower_scope = false;
        let err = install_workflow_resources(&wf, &[clone], &reg).expect_err("collision");
        assert!(matches!(err, WorkflowScopeError::Registry { .. }));
    }

    #[test]
    fn reload_replaces_previous_scope() {
        // First load gives the workflow scope {old}. Edit + reload gives
        // {new}. The first install's `old` must NOT linger after the second
        // install — old upsert-only semantics would have left it visible.
        let reg = ResourceRegistry::new();
        let wf = WorkflowId("editing".into());
        install_workflow_resources(&wf, &[wf_def("old")], &reg).expect("first load");
        let layer_first = reg
            .snapshot()
            .layers
            .get(&ScopeKey::Workflow { id: wf.clone() })
            .cloned()
            .unwrap();
        assert!(layer_first.contains_key(&ResourceId("old".into())));

        // Reload with a different id; the previous `old` must be gone.
        install_workflow_resources(&wf, &[wf_def("new")], &reg).expect("reload");
        let snap = reg.snapshot();
        let scope = ScopeKey::Workflow { id: wf };
        let layer = snap.layers.get(&scope).unwrap();
        assert!(layer.contains_key(&ResourceId("new".into())));
        assert!(
            !layer.contains_key(&ResourceId("old".into())),
            "old must not linger after reload"
        );
    }

    #[test]
    fn reload_with_empty_resources_drops_previous_scope() {
        let reg = ResourceRegistry::new();
        let wf = WorkflowId("emptying".into());
        install_workflow_resources(&wf, &[wf_def("a"), wf_def("b")], &reg).expect("first");
        // Reload with no resources should leave the workflow scope absent (or
        // empty) so the entries don't outlive the workflow's `resources:`
        // block being deleted.
        install_workflow_resources(&wf, &[], &reg).expect("empty reload");
        let snap = reg.snapshot();
        let scope = ScopeKey::Workflow { id: wf };
        let layer = snap.layers.get(&scope);
        assert!(
            layer.is_none() || layer.unwrap().is_empty(),
            "scope must be empty after reload-with-empty-resources"
        );
    }

    #[test]
    fn partial_install_failure_rolls_back_prior_entries() {
        // Seed builtin `ollama`. The workflow asks for [`safe`, `ollama` (no
        // override)] — `safe` succeeds, `ollama` collides. The Err return must
        // not leave `safe` installed under the workflow scope.
        let reg = ResourceRegistry::new();
        reg.upsert(&ScopeKey::Builtin, &wf_def("ollama")).unwrap();
        let wf = WorkflowId("partial".into());
        let mut colliding = wf_def("ollama");
        colliding.override_lower_scope = false;
        let err = install_workflow_resources(&wf, &[wf_def("safe"), colliding], &reg)
            .expect_err("collision");
        assert!(matches!(err, WorkflowScopeError::Registry { .. }));

        let snap = reg.snapshot();
        let layer = snap.layers.get(&ScopeKey::Workflow { id: wf });
        assert!(
            layer.is_none() || !layer.unwrap().contains_key(&ResourceId("safe".into())),
            "safe must not linger after rollback"
        );
    }
}
