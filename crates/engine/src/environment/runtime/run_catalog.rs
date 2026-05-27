//! `RunCatalog`: per-env probe results frozen at run start plus a run-local
//! monotonic overlay.
//!
//! At the start of a workflow run, the engine clones each in-scope env's
//! `Arc<ResourceCatalog>` into a `RunCatalog`. `frozen` is authoritative for
//! `ResourceProbeOutcome::Found(...)` entries — once a resource was reachable
//! when the run started, the run's view of it never changes. Non-`Found`
//! frozen entries (`NotFound` / `Skipped` / `TimedOut` / `ProbeFailed`) can
//! be opportunistically re-probed via `RunCatalog::opportunistic_reprobe`
//! (added in the next task). Successful re-probes write to `overlay`;
//! subsequent lookups within the same run see the updated value via
//! overlay-first read order.
//!
//! Singleflight discipline: only one re-probe per `(env_id, resource_id)` is
//! ever in flight at a time, regardless of how many nodes triggered it. See
//! `inflight` field docs in the next task.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;

use super::catalog::{ResourceCatalog, ResourceDetail, ResourceProbeOutcome};
use super::env::EnvId;
use super::resource::ResourceId;

/// Per-env run-local catalog (spec §7).
///
/// Lookup order: `overlay` first, then `frozen`. The overlay never holds a
/// non-`Found` outcome; that contract simplifies callers (a present overlay
/// entry is always a successful re-probe).
pub struct RunCatalog {
    /// Environment this catalog describes.
    pub env_id: EnvId,
    /// Frozen probe results at run start. Cloned once from the engine's
    /// per-env `ResourceCatalog` at `build_run_snapshot` time; never mutated
    /// for the lifetime of this run.
    pub frozen: Arc<ResourceCatalog>,
    /// Run-local monotonic overlay of successful re-probes.
    ///
    /// Write rules (enforced by `try_set_overlay`, added in the next task):
    ///   - WRITE allowed only when the resulting outcome is `Found(detail)`.
    ///     Non-`Found` re-probe results are returned to the caller but NOT
    ///     written; this keeps overlay strictly monotonic.
    ///   - WRITE forbidden when `frozen[id]` is already `Found(...)` — the
    ///     frozen `Found` takes precedence; re-probe was unnecessary.
    ///
    /// `ArcSwap<HashMap<...>>` so reads are lock-free copy-out and writes are
    /// CAS-retry copy-on-write. The map clones at write time but reads
    /// (which dominate) never copy.
    overlay: ArcSwap<HashMap<ResourceId, ResourceDetail>>,
}

impl RunCatalog {
    /// Build from a frozen catalog with an empty overlay.
    #[must_use]
    pub fn new(env_id: EnvId, frozen: Arc<ResourceCatalog>) -> Self {
        Self {
            env_id,
            frozen,
            overlay: ArcSwap::new(Arc::new(HashMap::new())),
        }
    }

    /// Return the effective outcome for `id`, honouring overlay-first read order:
    /// - If overlay has an entry, return `Found(detail)`.
    /// - Else, if frozen has any entry, return it verbatim (`Found` or non-`Found`).
    /// - Else `None` (catalog has no entry at all — e.g., a workflow resource
    ///   that never made it into the run's probe). Callers treat `None` and
    ///   `Some(NotFound | Skipped | TimedOut | ProbeFailed)` symmetrically:
    ///   trigger `opportunistic_reprobe` if the node marks the resource required.
    #[must_use]
    pub fn lookup(&self, id: &ResourceId) -> Option<ResourceProbeOutcome> {
        if let Some(detail) = self.overlay.load().get(id) {
            return Some(ResourceProbeOutcome::Found(detail.clone()));
        }
        self.frozen.resources.get(id).cloned()
    }

    /// Test-only: install a `Found` detail into the overlay directly (skips
    /// the re-probe path). Used by the per-run isolation tests and by the
    /// inline tests below.
    #[cfg(any(test, feature = "testing"))]
    pub fn install_overlay_for_test(&self, id: ResourceId, detail: ResourceDetail) {
        let mut next = (**self.overlay.load()).clone();
        next.insert(id, detail);
        self.overlay.store(Arc::new(next));
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use chrono::Utc;

    use super::super::catalog::{ResourceCatalog, ResourceDetail, ResourceProbeOutcome};
    use super::super::env::EnvId;
    use super::super::resource::ResourceId;
    use super::RunCatalog;

    fn binary_detail(path: &str) -> ResourceDetail {
        ResourceDetail::Binary {
            path: path.into(),
            version: None,
            capabilities: vec![],
        }
    }

    fn frozen_with(entries: Vec<(ResourceId, ResourceProbeOutcome)>) -> Arc<ResourceCatalog> {
        let mut resources = HashMap::new();
        for (id, outcome) in entries {
            resources.insert(id, outcome);
        }
        Arc::new(ResourceCatalog {
            env_id: EnvId::new("local"),
            registry_revision: 1,
            probed_at: Utc::now(),
            resources,
        })
    }

    #[test]
    fn lookup_returns_frozen_found() {
        let id = ResourceId("ollama".into());
        let detail = binary_detail("/usr/local/bin/ollama");
        let frozen = frozen_with(vec![(
            id.clone(),
            ResourceProbeOutcome::Found(detail.clone()),
        )]);
        let cat = RunCatalog::new(EnvId::new("local"), frozen);
        assert_eq!(cat.lookup(&id), Some(ResourceProbeOutcome::Found(detail)),);
    }

    #[test]
    fn lookup_returns_frozen_not_found() {
        let id = ResourceId("missing".into());
        let frozen = frozen_with(vec![(id.clone(), ResourceProbeOutcome::NotFound)]);
        let cat = RunCatalog::new(EnvId::new("local"), frozen);
        assert_eq!(cat.lookup(&id), Some(ResourceProbeOutcome::NotFound));
    }

    #[test]
    fn overlay_promotes_not_found_to_found() {
        let id = ResourceId("ollama".into());
        let frozen = frozen_with(vec![(id.clone(), ResourceProbeOutcome::NotFound)]);
        let cat = RunCatalog::new(EnvId::new("local"), frozen);
        let detail = binary_detail("/usr/local/bin/ollama");
        cat.install_overlay_for_test(id.clone(), detail.clone());
        assert_eq!(cat.lookup(&id), Some(ResourceProbeOutcome::Found(detail)),);
    }

    #[test]
    fn overlay_wins_over_frozen_found_at_read_time() {
        // Read order is overlay-first; the next task's `try_set_overlay`
        // forbids this case at write time, but the read path itself stays
        // pure overlay-first to keep `lookup` branch-free of frozen-check.
        let id = ResourceId("ollama".into());
        let frozen_detail = binary_detail("/old/path");
        let frozen = frozen_with(vec![(
            id.clone(),
            ResourceProbeOutcome::Found(frozen_detail),
        )]);
        let cat = RunCatalog::new(EnvId::new("local"), frozen);
        let overlay_detail = binary_detail("/new/path");
        cat.install_overlay_for_test(id.clone(), overlay_detail.clone());
        assert_eq!(
            cat.lookup(&id),
            Some(ResourceProbeOutcome::Found(overlay_detail)),
        );
    }

    #[test]
    fn lookup_unknown_resource_is_none() {
        let frozen = frozen_with(vec![]);
        let cat = RunCatalog::new(EnvId::new("local"), frozen);
        assert!(cat.lookup(&ResourceId("nope".into())).is_none());
    }
}
