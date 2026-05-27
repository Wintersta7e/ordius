//! Scoped resource registry: scope keys, registry inner state, and scope chain helpers.
//!
//! [`ScopeKey`] is the `HashMap` key for the layered registry. [`RegistryInner`] holds the
//! versioned layer map. [`RegistryInner::scope_chain`] returns the precedence order used
//! by resolvers in Task 10+.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};

use super::env::{EnvId, WorkflowId};
use super::error::RegistryError;
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

    /// Clone `self` with `ScopeKey::EnvLocal { id }` layers populated from
    /// `envs`. Used by [`crate::Engine::build_run_snapshot`] to materialize
    /// per-run env-scoped resources without mutating the engine-level
    /// registry.
    ///
    /// Existing `EnvLocal` layers for the same env id are replaced (not
    /// merged) — the caller passes the canonical per-env resource list and
    /// every layer is rebuilt from scratch.
    ///
    /// Honors [`ResourceDefinition::override_lower_scope`]: an env-local def
    /// whose id already lives at `Builtin` or `UserGlobal` AND has
    /// `override_lower_scope == false` is rejected with
    /// [`RegistryError::OverrideRequired`]. The precedence chain is
    /// `Workflow > EnvLocal > UserGlobal > Builtin`, so colliding with a
    /// `Workflow` scope entry is allowed (workflow wins at resolve time);
    /// EnvLocal-vs-EnvLocal collisions across different envs are also fine
    /// because each env has its own scope key.
    ///
    /// On success returns a new `Arc<RegistryInner>` with `revision`
    /// incremented by one.
    pub fn with_env_local_overlay(
        self: &Arc<Self>,
        envs: &[(EnvId, Vec<ResourceDefinition>)],
    ) -> Result<Arc<Self>, RegistryError> {
        let mut next_layers = self.layers.clone();
        for (env_id, defs) in envs {
            let scope = ScopeKey::EnvLocal { id: env_id.clone() };
            let mut layer = HashMap::with_capacity(defs.len());
            for def in defs {
                if !def.override_lower_scope
                    && let Some(existing_scope) =
                        find_lower_scope_with_id_for_env_local(&self.layers, &def.id)
                {
                    return Err(RegistryError::OverrideRequired {
                        id: def.id.0.clone(),
                        existing_scope: format!("{existing_scope:?}"),
                    });
                }
                layer.insert(def.id.clone(), def.clone());
            }
            next_layers.insert(scope, layer);
        }
        Ok(Arc::new(Self {
            revision: self.revision + 1,
            layers: next_layers,
        }))
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

/// Live, mutable resource registry.
///
/// The active [`RegistryInner`] is held behind [`ArcSwap`] so reads can take
/// immutable snapshots while writes publish a copied replacement atomically.
#[derive(Debug)]
pub struct ResourceRegistry {
    inner: ArcSwap<RegistryInner>,
}

impl ResourceRegistry {
    /// Create an empty registry at revision `0`.
    pub fn new() -> Self {
        Self {
            inner: ArcSwap::from_pointee(RegistryInner {
                revision: 0,
                layers: HashMap::new(),
            }),
        }
    }

    /// Insert or replace a resource definition in `scope`.
    ///
    /// When `def.override_lower_scope` is `false`, this rejects writes that
    /// would shadow an existing definition with the same id at a lower
    /// precedence scope. On success, returns the newly published revision.
    pub fn upsert(&self, scope: &ScopeKey, def: &ResourceDefinition) -> Result<u64, RegistryError> {
        loop {
            let current = self.inner.load_full();

            if !def.override_lower_scope
                && let Some(existing_scope) = find_lower_scope_with_id(&current, scope, &def.id)
            {
                return Err(RegistryError::OverrideRequired {
                    id: def.id.0.clone(),
                    existing_scope: format!("{existing_scope:?}"),
                });
            }

            let mut new_layers = current.layers.clone();
            new_layers
                .entry(scope.clone())
                .or_default()
                .insert(def.id.clone(), def.clone());
            let new = Arc::new(RegistryInner {
                revision: current.revision + 1,
                layers: new_layers,
            });
            let revision = new.revision;
            let previous = self.inner.compare_and_swap(&current, new);
            if Arc::ptr_eq(&current, &previous) {
                return Ok(revision);
            }
        }
    }

    /// Return an immutable snapshot of the current registry state.
    ///
    /// The returned [`Arc`] remains valid even after later writes publish newer
    /// registry revisions.
    pub fn snapshot(&self) -> Arc<RegistryInner> {
        self.inner.load_full()
    }

    /// Drop a whole scope from the registry. Returns the number of
    /// definitions that were removed; `0` if the scope was absent.
    ///
    /// Bumps the registry revision exactly when at least one definition was
    /// removed, so callers using `revision` to invalidate caches don't see a
    /// spurious refresh signal when this is a no-op.
    pub fn remove_scope(&self, scope: &ScopeKey) -> usize {
        loop {
            let current = self.inner.load_full();
            let Some(layer) = current.layers.get(scope) else {
                return 0;
            };
            let removed = layer.len();
            if removed == 0 {
                return 0;
            }
            let mut new_layers = current.layers.clone();
            new_layers.remove(scope);
            let new = Arc::new(RegistryInner {
                revision: current.revision + 1,
                layers: new_layers,
            });
            let previous = self.inner.compare_and_swap(&current, new);
            if Arc::ptr_eq(&current, &previous) {
                return removed;
            }
        }
    }
}

impl Default for ResourceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Find an existing declaration for `id` at a lower-precedence scope.
fn find_lower_scope_with_id(
    inner: &RegistryInner,
    incoming_scope: &ScopeKey,
    id: &ResourceId,
) -> Option<ScopeKey> {
    let highest_lower_rank = match incoming_scope {
        ScopeKey::Workflow { .. } => 2,
        ScopeKey::EnvLocal { .. } => 1,
        ScopeKey::UserGlobal => 0,
        ScopeKey::Builtin => return None,
    };
    for (existing_scope, layer) in &inner.layers {
        if layer.contains_key(id) && scope_rank(existing_scope) <= highest_lower_rank {
            return Some(existing_scope.clone());
        }
    }
    None
}

/// Find an existing declaration for `id` at a scope strictly lower than
/// `EnvLocal` (i.e. `Builtin` or `UserGlobal`). Used by
/// [`RegistryInner::with_env_local_overlay`] to enforce
/// `override_lower_scope` against lower-precedence layers without consulting
/// `Workflow` (which is HIGHER than `EnvLocal` and therefore not a shadow).
fn find_lower_scope_with_id_for_env_local(
    layers: &HashMap<ScopeKey, HashMap<ResourceId, ResourceDefinition>>,
    id: &ResourceId,
) -> Option<ScopeKey> {
    for (scope, layer) in layers {
        if matches!(scope, ScopeKey::Builtin | ScopeKey::UserGlobal) && layer.contains_key(id) {
            return Some(scope.clone());
        }
    }
    None
}

/// Return the precedence rank for a scope; higher ranks are more specific.
const fn scope_rank(scope: &ScopeKey) -> u8 {
    match scope {
        ScopeKey::Workflow { .. } => 3,
        ScopeKey::EnvLocal { .. } => 2,
        ScopeKey::UserGlobal => 1,
        ScopeKey::Builtin => 0,
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

    #[test]
    fn registry_starts_empty_revision_zero() {
        let reg = ResourceRegistry::new();
        assert_eq!(reg.snapshot().revision, 0);
    }

    #[test]
    fn upsert_bumps_revision() {
        let reg = ResourceRegistry::new();
        let new_rev = reg
            .upsert(&ScopeKey::Builtin, &def("ollama", false))
            .unwrap();
        assert_eq!(new_rev, 1);
        assert_eq!(reg.snapshot().revision, 1);
    }

    #[test]
    fn upsert_collision_without_override_errors() {
        let reg = ResourceRegistry::new();
        let _ = reg
            .upsert(&ScopeKey::Builtin, &def("ollama", false))
            .unwrap();
        let err = reg
            .upsert(&ScopeKey::UserGlobal, &def("ollama", false))
            .unwrap_err();
        assert!(matches!(err, RegistryError::OverrideRequired { .. }));
    }

    #[test]
    fn upsert_with_override_succeeds() {
        let reg = ResourceRegistry::new();
        let _ = reg
            .upsert(&ScopeKey::Builtin, &def("ollama", false))
            .unwrap();
        let new_rev = reg
            .upsert(&ScopeKey::UserGlobal, &def("ollama", true))
            .unwrap();
        assert_eq!(new_rev, 2);
    }

    #[test]
    fn snapshot_does_not_observe_later_upserts() {
        let reg = ResourceRegistry::new();
        let _ = reg
            .upsert(&ScopeKey::Builtin, &def("ollama", false))
            .unwrap();
        let snap = reg.snapshot();
        let _ = reg.upsert(&ScopeKey::Builtin, &def("vllm", false)).unwrap();
        assert!(
            snap.layers
                .get(&ScopeKey::Builtin)
                .unwrap()
                .contains_key(&ResourceId("ollama".into()))
        );
        assert!(
            !snap
                .layers
                .get(&ScopeKey::Builtin)
                .unwrap()
                .contains_key(&ResourceId("vllm".into()))
        );
    }

    #[test]
    fn remove_scope_drops_layer_and_bumps_revision() {
        let reg = ResourceRegistry::new();
        let scope = ScopeKey::Workflow {
            id: crate::environment::runtime::env::WorkflowId("wf1".into()),
        };
        let _ = reg.upsert(&scope, &def("custom", false)).unwrap();
        let rev_before_remove = reg.snapshot().revision;
        let removed = reg.remove_scope(&scope);
        assert_eq!(removed, 1);
        let snap = reg.snapshot();
        assert!(snap.revision > rev_before_remove);
        assert!(!snap.layers.contains_key(&scope));
    }

    #[test]
    fn remove_scope_missing_returns_zero() {
        let reg = ResourceRegistry::new();
        let scope = ScopeKey::Workflow {
            id: crate::environment::runtime::env::WorkflowId("ghost".into()),
        };
        assert_eq!(reg.remove_scope(&scope), 0);
        // No revision bump when nothing changed.
        assert_eq!(reg.snapshot().revision, 0);
    }
}
