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

/// Install each definition in `resources` under
/// `ScopeKey::Workflow { id: workflow_id.clone() }`.
///
/// Returns the count of installed definitions on success. On failure the
/// caller is responsible for rolling back (typically by calling
/// [`remove_workflow_scope`]) — leaving a half-installed scope behind would
/// hide later definitions of the same id.
pub fn install_workflow_resources(
    workflow_id: &WorkflowId,
    resources: &[ResourceDefinition],
    registry: &ResourceRegistry,
) -> Result<usize, WorkflowScopeError> {
    let scope = ScopeKey::Workflow {
        id: workflow_id.clone(),
    };
    let mut written = 0_usize;
    for def in resources {
        registry
            .upsert(&scope, def)
            .map_err(|e| WorkflowScopeError::Registry {
                workflow_id: workflow_id.0.clone(),
                id: def.id.0.clone(),
                source: e,
            })?;
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
}
