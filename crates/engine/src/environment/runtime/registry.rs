//! Scoped resource registry: scope keys, registry inner state, and scope chain helpers.
//!
//! [`ScopeKey`] is the `HashMap` key for the layered registry. [`RegistryInner`] holds the
//! versioned layer map. [`RegistryInner::scope_chain`] returns the precedence order used
//! by resolvers in Task 10+.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::env::{EnvId, WorkflowId};
use super::resource::{ResourceDefinition, ResourceId};

/// Identifies which layer of the registry a definition belongs to.
///
/// Precedence (highest first): `Workflow` > `EnvLocal` > `UserGlobal` > `Builtin`.
///
/// # Serde note
/// Uses `#[serde(tag = "scope")]` (internally-tagged). Serde does not support
/// internally-tagged tuple variants, so `EnvLocal` and `Workflow` are expressed
/// as struct variants with a single named field `id` rather than bare tuple
/// variants — the wire shape is `{"scope":"env_local","id":"..."}`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "scope", rename_all = "snake_case")]
pub enum ScopeKey {
    /// Built-in definitions shipped with Ordius (`BUILTIN_RESOURCES`). Lowest precedence.
    Builtin,
    /// User-global overrides/additions. Apply across all envs and workflows.
    UserGlobal,
    /// Definitions scoped to a specific environment. Override `UserGlobal` and `Builtin`.
    EnvLocal {
        /// The environment this scope is bound to.
        id: EnvId,
    },
    /// Definitions scoped to a specific workflow run. Highest precedence.
    Workflow {
        /// The workflow this scope is bound to.
        id: WorkflowId,
    },
}

/// Versioned, layered map of resource definitions keyed by [`ScopeKey`].
///
/// Each layer is a `HashMap<ResourceId, ResourceDefinition>`. `revision` is
/// bumped on every write so callers can detect staleness without deep equality.
#[derive(Debug)]
pub struct RegistryInner {
    /// Monotonically increasing write counter. Starts at `0` for an empty registry.
    pub revision: u64,
    /// One layer per scope. Layers not yet populated are simply absent.
    pub layers: HashMap<ScopeKey, HashMap<ResourceId, ResourceDefinition>>,
}

impl RegistryInner {
    /// Build the precedence chain for a given `(env, workflow?)` context.
    ///
    /// Returns scopes from highest to lowest precedence:
    /// - `[Workflow(wf), EnvLocal(env), UserGlobal, Builtin]` when `workflow` is `Some`
    /// - `[EnvLocal(env), UserGlobal, Builtin]` when `workflow` is `None`
    ///
    /// Consumers walk this slice front-to-back and stop at the first hit.
    pub fn scope_chain(env: &EnvId, workflow: Option<&WorkflowId>) -> Vec<ScopeKey> {
        let mut chain = Vec::with_capacity(4);
        if let Some(wf) = workflow {
            chain.push(ScopeKey::Workflow { id: wf.clone() });
        }
        chain.push(ScopeKey::EnvLocal { id: env.clone() });
        chain.push(ScopeKey::UserGlobal);
        chain.push(ScopeKey::Builtin);
        chain
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::environment::runtime::env::{EnvId, WorkflowId};

    #[test]
    fn scope_chain_full_walk() {
        let env = EnvId::wsl("Ubuntu");
        let wf = WorkflowId("pipeline".into());
        let chain = RegistryInner::scope_chain(&env, Some(&wf));
        assert_eq!(chain.len(), 4);
        assert!(matches!(chain[0], ScopeKey::Workflow { .. }));
        assert!(matches!(chain[1], ScopeKey::EnvLocal { .. }));
        assert_eq!(chain[2], ScopeKey::UserGlobal);
        assert_eq!(chain[3], ScopeKey::Builtin);
    }

    #[test]
    fn scope_chain_without_workflow_is_three() {
        let env = EnvId::local();
        let chain = RegistryInner::scope_chain(&env, None);
        assert_eq!(chain.len(), 3);
        assert!(matches!(chain[0], ScopeKey::EnvLocal { .. }));
        assert_eq!(chain[1], ScopeKey::UserGlobal);
        assert_eq!(chain[2], ScopeKey::Builtin);
    }

    #[test]
    fn scope_chain_env_specificity() {
        let env_a = EnvId::wsl("A");
        let env_b = EnvId::wsl("B");
        let chain_a = RegistryInner::scope_chain(&env_a, None);
        let chain_b = RegistryInner::scope_chain(&env_b, None);
        // EnvLocal slot differs by env
        assert_ne!(chain_a[0], chain_b[0]);
    }
}
