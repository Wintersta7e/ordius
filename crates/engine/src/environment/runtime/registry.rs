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

    /// Walk the scope chain in precedence order and return the first
    /// `ResourceDefinition` whose id matches, together with the scope it
    /// lives in. Returns `None` if the id is not declared at any scope
    /// visible to `(env, workflow?)`.
    pub fn resolve(
        &self,
        id: &ResourceId,
        env: &EnvId,
        workflow: Option<&WorkflowId>,
    ) -> Option<(&ResourceDefinition, ScopeKey)> {
        for sk in Self::scope_chain(env, workflow) {
            if let Some(def) = self.layers.get(&sk).and_then(|m| m.get(id)) {
                return Some((def, sk));
            }
        }
        None
    }

    /// All resource definitions visible to `(env, workflow?)`, deduplicated
    /// by precedence: the first scope in the chain that declares an id wins;
    /// lower-precedence declarations of the same id are silently dropped.
    ///
    /// Order within the returned `Vec` is precedence-descending (highest
    /// scope first within each scope, then next scope, etc.).
    pub fn visible_to(
        &self,
        env: &EnvId,
        workflow: Option<&WorkflowId>,
    ) -> Vec<(&ResourceDefinition, ScopeKey)> {
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::new();
        for sk in Self::scope_chain(env, workflow) {
            if let Some(layer) = self.layers.get(&sk) {
                for (id, def) in layer {
                    if seen.insert(id.clone()) {
                        out.push((def, sk.clone()));
                    }
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::environment::runtime::env::{EnvId, WorkflowId};
    use crate::environment::runtime::resource::{ProbeSpec, ResourceKind};

    /// Build a minimal `ResourceDefinition` for test scaffolding.
    fn def(id: &str, override_flag: bool) -> ResourceDefinition {
        ResourceDefinition {
            id: ResourceId(id.into()),
            kind: ResourceKind::HttpEndpoint,
            advertised_capabilities: vec![],
            probe: ProbeSpec::Http {
                ports: vec![],
                routes: vec![],
                timeout_ms: None,
            },
            override_lower_scope: override_flag,
        }
    }

    fn empty_inner() -> RegistryInner {
        RegistryInner {
            revision: 1,
            layers: HashMap::new(),
        }
    }

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

    // ── Task 10 tests ────────────────────────────────────────────────────────

    #[test]
    fn resolve_returns_highest_scope() {
        let mut inner = empty_inner();
        inner
            .layers
            .entry(ScopeKey::Builtin)
            .or_default()
            .insert(ResourceId("ollama".into()), def("ollama", false));
        inner
            .layers
            .entry(ScopeKey::EnvLocal {
                id: EnvId::wsl("Ubuntu"),
            })
            .or_default()
            .insert(ResourceId("ollama".into()), def("ollama", true));

        let (found, scope) = inner
            .resolve(&ResourceId("ollama".into()), &EnvId::wsl("Ubuntu"), None)
            .expect("found");
        assert!(found.override_lower_scope);
        assert!(matches!(scope, ScopeKey::EnvLocal { .. }));
    }

    #[test]
    fn resolve_falls_back_to_builtin() {
        let mut inner = empty_inner();
        inner
            .layers
            .entry(ScopeKey::Builtin)
            .or_default()
            .insert(ResourceId("ollama".into()), def("ollama", false));

        let (_, scope) = inner
            .resolve(&ResourceId("ollama".into()), &EnvId::wsl("Ubuntu"), None)
            .expect("found");
        assert_eq!(scope, ScopeKey::Builtin);
    }

    #[test]
    fn resolve_misses_returns_none() {
        let inner = empty_inner();
        assert!(
            inner
                .resolve(&ResourceId("nope".into()), &EnvId::local(), None)
                .is_none()
        );
    }

    #[test]
    fn visible_to_dedupes_by_scope_precedence() {
        let mut inner = empty_inner();
        inner
            .layers
            .entry(ScopeKey::Builtin)
            .or_default()
            .insert(ResourceId("ollama".into()), def("ollama", false));
        inner
            .layers
            .entry(ScopeKey::UserGlobal)
            .or_default()
            .insert(ResourceId("custom".into()), def("custom", false));

        let visible = inner.visible_to(&EnvId::local(), None);
        let ids: Vec<&str> = visible.iter().map(|(d, _)| d.id.0.as_str()).collect();
        assert!(ids.contains(&"ollama"));
        assert!(ids.contains(&"custom"));
        assert_eq!(ids.len(), 2);
    }
}
