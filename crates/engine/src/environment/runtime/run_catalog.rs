//! `RunCatalog`: per-env probe results frozen at run start plus a run-local
//! monotonic overlay.
//!
//! At the start of a workflow run, the engine clones each in-scope env's
//! `Arc<ResourceCatalog>` into a `RunCatalog`. `frozen` is authoritative for
//! `ResourceProbeOutcome::Found(...)` entries — once a resource was reachable
//! when the run started, the run's view of it never changes. Non-`Found`
//! frozen entries (`NotFound` / `Skipped` / `TimedOut` / `ProbeFailed`) can
//! be opportunistically re-probed via [`RunCatalog::opportunistic_reprobe`].
//! Successful re-probes write to `overlay`; subsequent lookups within the
//! same run see the updated value via overlay-first read order.
//!
//! Singleflight discipline: only one re-probe per `(env_id, resource_id)` is
//! ever in flight at a time, regardless of how many nodes triggered it. See
//! `inflight` field docs below.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use super::catalog::{ResourceCatalog, ResourceDetail, ResourceProbeOutcome};
use super::dispatcher::Dispatcher;
use super::env::EnvId;
use super::resource::{ResourceDefinition, ResourceId};

/// In-flight singleflight entry. The `generation` field is monotonic per
/// `(env_id, resource_id)` and lets the post-await removal step distinguish
/// "this is still the future I started" from "a later caller already started
/// a new one after mine completed". Without it, a slow waiter for an old
/// future could `remove()` a newer in-flight entry and force a third caller
/// to start a redundant re-probe.
struct Inflight {
    generation: u64,
    fut: Shared<BoxFuture<'static, ResourceProbeOutcome>>,
}

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
    /// Write rules (enforced by `try_set_overlay`):
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
    /// In-flight re-probe registry. Inflight entries carry a monotonic `gen`
    /// so concurrent callers for the same resource id share one execution
    /// via `Shared::clone`, and the post-await cleanup step is identity-safe
    /// (it only removes the entry whose `gen` matches what this caller
    /// started or joined). `parking_lot::Mutex` is synchronous; the lock is
    /// never held across `.await`.
    ///
    /// Drop note: awaiters hold their own `Shared` clones, which keep the
    /// underlying probe future alive past `RunCatalog::drop`. Cancellation
    /// must come from the dispatcher's `CancellationToken`, not from
    /// `RunCatalog`'s lifetime — dropping the `inflight` map does NOT cancel
    /// outstanding futures.
    inflight: Mutex<HashMap<ResourceId, Arc<Inflight>>>,
    /// Per-resource monotonic generation counter. Incremented under the
    /// `inflight` lock each time a new probe is registered. Lives next to
    /// `inflight` so the monotonic increment is visible to the post-await
    /// identity check.
    inflight_gen: Mutex<HashMap<ResourceId, u64>>,
}

impl RunCatalog {
    /// Build from a frozen catalog with an empty overlay.
    #[must_use]
    pub fn new(env_id: EnvId, frozen: Arc<ResourceCatalog>) -> Self {
        Self {
            env_id,
            frozen,
            overlay: ArcSwap::new(Arc::new(HashMap::new())),
            inflight: Mutex::new(HashMap::new()),
            inflight_gen: Mutex::new(HashMap::new()),
        }
    }

    /// Opportunistically re-probe a single resource. Returns the live outcome
    /// to this caller.
    ///
    /// Singleflight: if another caller already has a re-probe in flight for
    /// `def.id`, this call shares its future via `Shared::clone` and awaits
    /// the same result. Only one `Dispatcher::probe_resource` call ever fires
    /// per `(env_id, resource_id)` at a time.
    ///
    /// Fast path: if `frozen[id]` is already `Found(...)`, returns the frozen
    /// value verbatim without touching the dispatcher.
    ///
    /// Overlay write rules:
    /// - On `Found(detail)` AND `frozen[id]` is NOT already `Found(...)`:
    ///   writes `detail` to the overlay (visible to subsequent lookups
    ///   within this run via overlay-first read order).
    /// - On non-`Found` outcomes or when `frozen[id]` was already `Found(...)`:
    ///   returns the outcome to the caller but does NOT write the overlay.
    ///   Subsequent calls re-trigger the singleflight via a new `gen`.
    pub async fn opportunistic_reprobe(
        self: &Arc<Self>,
        def: &ResourceDefinition,
        dispatcher: Arc<dyn Dispatcher>,
        cancel: CancellationToken,
    ) -> ResourceProbeOutcome {
        // Fast path: already Found in frozen. Re-probe is pointless; return
        // the frozen value directly without invoking the dispatcher.
        if let Some(outcome @ ResourceProbeOutcome::Found(_)) = self.frozen.resources.get(&def.id) {
            return outcome.clone();
        }

        // Slow path: register-or-join the singleflight future. Capture the
        // generation we observe so we can do an identity-safe removal
        // after the await. The synchronous mutex is never held across
        // `.await` — `register_or_join` returns before the await point.
        let (shared, my_generation) = self.register_or_join(def, &dispatcher, &cancel);

        let outcome = shared.await;

        self.remove_inflight_if_matches(&def.id, my_generation);

        if let ResourceProbeOutcome::Found(detail) = &outcome {
            self.try_set_overlay(&def.id, detail);
        }
        outcome
    }

    /// Synchronous helper: under the inflight mutex, either join an existing
    /// in-flight future or register a new one and bump the generation. Drops
    /// the guards before returning so the caller's await never sees a held
    /// lock. Factored out so `parking_lot::MutexGuard`s do not span the
    /// `.await` point (and so clippy's `significant_drop_tightening` lint
    /// stays clean).
    fn register_or_join(
        &self,
        def: &ResourceDefinition,
        dispatcher: &Arc<dyn Dispatcher>,
        cancel: &CancellationToken,
    ) -> (Shared<BoxFuture<'static, ResourceProbeOutcome>>, u64) {
        let mut inflight_guard = self.inflight.lock();
        if let Some(existing) = inflight_guard.get(&def.id) {
            let joined = (existing.fut.clone(), existing.generation);
            drop(inflight_guard);
            return joined;
        }
        let generation = {
            let mut gen_guard = self.inflight_gen.lock();
            let slot = gen_guard.entry(def.id.clone()).or_insert(0);
            *slot += 1;
            let next = *slot;
            drop(gen_guard);
            next
        };

        let def_clone = def.clone();
        let dispatcher_clone = Arc::clone(dispatcher);
        let cancel_clone = cancel.clone();
        let fut: BoxFuture<'static, ResourceProbeOutcome> = async move {
            dispatcher_clone
                .probe_resource(&def_clone, cancel_clone)
                .await
        }
        .boxed();
        let shared = fut.shared();
        inflight_guard.insert(
            def.id.clone(),
            Arc::new(Inflight {
                generation,
                fut: shared.clone(),
            }),
        );
        drop(inflight_guard);
        (shared, generation)
    }

    /// Identity-safe removal: only clear the slot if it still holds the
    /// generation we started (or joined). A later caller may have already
    /// started a newer generation after observing our completion; we must
    /// not clobber their entry.
    fn remove_inflight_if_matches(&self, id: &ResourceId, my_generation: u64) {
        let mut inflight_guard = self.inflight.lock();
        if inflight_guard
            .get(id)
            .is_some_and(|cur| cur.generation == my_generation)
        {
            inflight_guard.remove(id);
        }
    }

    /// Internal: copy-on-write overlay write, gated by the monotonic-write
    /// contract. Only called from `opportunistic_reprobe`'s post-await branch
    /// (and from `install_overlay_for_test` for unit tests).
    fn try_set_overlay(&self, id: &ResourceId, detail: &ResourceDetail) {
        // Reject the write if `frozen[id]` was already Found — the frozen
        // value is authoritative when present.
        if matches!(
            self.frozen.resources.get(id),
            Some(ResourceProbeOutcome::Found(_)),
        ) {
            return;
        }
        loop {
            let cur = self.overlay.load_full();
            let mut next = (*cur).clone();
            next.insert(id.clone(), detail.clone());
            let next_arc = Arc::new(next);
            let previous = self.overlay.compare_and_swap(&cur, next_arc);
            if Arc::ptr_eq(&cur, &previous) {
                break;
            }
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

#[cfg(test)]
mod singleflight_tests {
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use chrono::Utc;
    use tokio::sync::Barrier;
    use tokio_util::sync::CancellationToken;

    use super::super::catalog::{
        ResourceCatalog, ResourceDetail, ResourceProbeOutcome, RouteOrigin,
    };
    use super::super::dispatcher::{Dispatcher, HttpTransport};
    use super::super::env::{EnvId, EnvInfo, EnvSpec, EnvState, RunId, WorkspaceBinding};
    use super::super::error::DispatchError;
    use super::super::fake::{FakeHttpTransport, FakeRemoteDispatcher, FakeResource};
    use super::super::plan::{ProbePlan, ProbeSummary};
    use super::super::resource::{
        ApiFlavor, Capability, HttpProbeMethod, HttpProbeRoute, ProbeSpec, ResourceDefinition,
        ResourceId, ResourceKind,
    };
    use super::super::transport::{EnvPath, ProcessCmd, WorkspaceHandle};
    use super::RunCatalog;

    fn local_info() -> EnvInfo {
        EnvInfo {
            id: EnvId::local(),
            label: "local".into(),
            spec: EnvSpec::Local {
                resources: vec![],
                host_direct_verifications: std::collections::HashMap::default(),
            },
            state: EnvState::Reachable,
            enabled: true,
        }
    }

    fn ollama_def() -> ResourceDefinition {
        ResourceDefinition {
            id: ResourceId("ollama".into()),
            kind: ResourceKind::HttpEndpoint,
            advertised_capabilities: vec![Capability::OllamaNative],
            probe: ProbeSpec::Http {
                ports: vec![11434],
                routes: vec![HttpProbeRoute {
                    path: "/api/version".into(),
                    method: HttpProbeMethod::Get,
                    flavor: ApiFlavor::OllamaNative,
                    proves: vec![Capability::OllamaNative],
                    models_jsonpath: None,
                    fingerprint_jsonpaths: vec![],
                }],
                timeout_ms: None,
            },
            override_lower_scope: false,
        }
    }

    fn frozen_with_outcome(id: &str, outcome: ResourceProbeOutcome) -> Arc<ResourceCatalog> {
        let mut resources = std::collections::HashMap::new();
        resources.insert(ResourceId(id.into()), outcome);
        Arc::new(ResourceCatalog {
            env_id: EnvId::local(),
            registry_revision: 1,
            probed_at: Utc::now(),
            resources,
        })
    }

    fn http_detail_at(base_url: &str) -> ResourceDetail {
        ResourceDetail::HttpEndpoint {
            base_url: base_url.into(),
            routes_by_capability: std::collections::HashMap::default(),
            version: None,
            models_list: None,
            auth_secret_ref: None,
            streaming_supported_natively: false,
            route_origin: RouteOrigin::EnvLoopback,
        }
    }

    /// Dispatcher wrapper that gates `probe_resource` on a barrier, counts
    /// invocations, and returns a configurable outcome. All non-probe trait
    /// methods are unimplemented because the singleflight tests never call
    /// them.
    ///
    /// The barrier MUST be `Barrier::new(2)` so the probe side and the test
    /// driver side rendezvous. A 1-party barrier completes immediately and
    /// defeats the gating.
    struct GatedDispatcher {
        info: EnvInfo,
        barrier: Arc<Barrier>,
        counter: Arc<AtomicUsize>,
        outcome: ResourceProbeOutcome,
    }

    #[async_trait]
    impl Dispatcher for GatedDispatcher {
        fn info(&self) -> &EnvInfo {
            &self.info
        }

        async fn probe(
            &self,
            _plan: ProbePlan,
            _cancel: CancellationToken,
        ) -> Result<ProbeSummary, DispatchError> {
            unimplemented!("GatedDispatcher::probe is not used by singleflight tests")
        }

        async fn probe_resource(
            &self,
            _def: &ResourceDefinition,
            _cancel: CancellationToken,
        ) -> ResourceProbeOutcome {
            self.counter.fetch_add(1, Ordering::SeqCst);
            // Block until every concurrent caller has reached the barrier so
            // we can verify the singleflight collapses them into one probe.
            self.barrier.wait().await;
            self.outcome.clone()
        }

        async fn spawn(
            &self,
            _cmd: ProcessCmd,
        ) -> Result<Box<dyn crate::environment::runtime::transport::EnvProcess>, DispatchError>
        {
            Err(DispatchError::Unsupported(
                "singleflight test dispatcher does not spawn processes".into(),
            ))
        }

        fn http_transport(&self) -> Arc<dyn HttpTransport> {
            Arc::new(FakeHttpTransport::default())
        }

        fn translate_path(&self, _host_path: &Path) -> Result<EnvPath, DispatchError> {
            unimplemented!("GatedDispatcher::translate_path is not used by singleflight tests")
        }

        async fn prepare_workspace(
            &self,
            _workspace_host: &Path,
            _binding: &WorkspaceBinding,
            _run_id: &RunId,
        ) -> Result<WorkspaceHandle, DispatchError> {
            unimplemented!("GatedDispatcher::prepare_workspace is not used by singleflight tests")
        }
    }

    #[tokio::test]
    async fn reprobe_writes_overlay_on_found_when_frozen_was_notfound() {
        let frozen = frozen_with_outcome("ollama", ResourceProbeOutcome::NotFound);
        let cat = Arc::new(RunCatalog::new(EnvId::local(), frozen));

        let dispatcher: Arc<dyn Dispatcher> =
            Arc::new(FakeRemoteDispatcher::new(local_info()).with_seeded(
                "ollama",
                FakeResource::http("http://fake/11434", &[Capability::OllamaNative]),
            ));

        let def = ollama_def();
        let outcome = cat
            .opportunistic_reprobe(&def, Arc::clone(&dispatcher), CancellationToken::new())
            .await;
        assert!(matches!(outcome, ResourceProbeOutcome::Found(_)));

        // Overlay-first lookup now hits the new Found.
        let later = cat.lookup(&def.id).expect("lookup after reprobe");
        assert!(matches!(later, ResourceProbeOutcome::Found(_)));
    }

    #[tokio::test]
    async fn singleflight_collapses_concurrent_callers() {
        const CALLERS: usize = 8;

        let frozen = frozen_with_outcome("ollama", ResourceProbeOutcome::NotFound);
        let cat = Arc::new(RunCatalog::new(EnvId::local(), frozen));

        let counter = Arc::new(AtomicUsize::new(0));
        // 2 parties: the test driver and the single probe execution.
        let barrier = Arc::new(Barrier::new(2));
        let detail = http_detail_at("http://gated/11434");
        let dispatcher: Arc<dyn Dispatcher> = Arc::new(GatedDispatcher {
            info: local_info(),
            barrier: Arc::clone(&barrier),
            counter: Arc::clone(&counter),
            outcome: ResourceProbeOutcome::Found(detail.clone()),
        });

        let def = ollama_def();
        let mut handles = Vec::with_capacity(CALLERS);
        for _ in 0..CALLERS {
            let cat = Arc::clone(&cat);
            let def = def.clone();
            let dispatcher = Arc::clone(&dispatcher);
            handles.push(tokio::spawn(async move {
                cat.opportunistic_reprobe(&def, dispatcher, CancellationToken::new())
                    .await
            }));
        }

        // Give the spawned tasks time to register/join the inflight entry
        // before we release the barrier. Without this delay the test would
        // race: the first task could enter `probe_resource` and hit the
        // barrier before the others have a chance to join the singleflight.
        tokio::time::sleep(Duration::from_millis(50)).await;
        barrier.wait().await;

        let mut found_count = 0;
        for h in handles {
            let outcome = h.await.expect("task panicked");
            if matches!(outcome, ResourceProbeOutcome::Found(ref d) if d == &detail) {
                found_count += 1;
            }
        }
        assert_eq!(found_count, CALLERS, "every caller saw the same Found");
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "singleflight collapsed concurrent callers into one probe",
        );
    }

    #[tokio::test]
    async fn reprobe_does_not_write_when_frozen_was_found() {
        let frozen_detail = http_detail_at("http://frozen/11434");
        let frozen =
            frozen_with_outcome("ollama", ResourceProbeOutcome::Found(frozen_detail.clone()));
        let cat = Arc::new(RunCatalog::new(EnvId::local(), frozen));

        // Dispatcher would return a *different* detail, but the fast path
        // means the dispatcher is never invoked. Use a counter to confirm.
        let counter = Arc::new(AtomicUsize::new(0));
        let dispatcher: Arc<dyn Dispatcher> = Arc::new(GatedDispatcher {
            info: local_info(),
            barrier: Arc::new(Barrier::new(1)),
            counter: Arc::clone(&counter),
            outcome: ResourceProbeOutcome::Found(http_detail_at("http://reprobed/11434")),
        });

        let def = ollama_def();
        let outcome = cat
            .opportunistic_reprobe(&def, dispatcher, CancellationToken::new())
            .await;
        match outcome {
            ResourceProbeOutcome::Found(d) => assert_eq!(d, frozen_detail),
            other => panic!("expected frozen Found, got {other:?}"),
        }
        assert_eq!(
            counter.load(Ordering::SeqCst),
            0,
            "fast path must skip the dispatcher when frozen is Found",
        );

        // Lookup still returns the frozen detail; overlay was not written.
        let later = cat.lookup(&def.id).expect("lookup after reprobe");
        match later {
            ResourceProbeOutcome::Found(d) => assert_eq!(d, frozen_detail),
            other => panic!("expected frozen Found from lookup, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn reprobe_returns_notfound_without_writing_overlay() {
        let frozen = frozen_with_outcome("ollama", ResourceProbeOutcome::NotFound);
        let cat = Arc::new(RunCatalog::new(EnvId::local(), frozen));

        // FakeRemoteDispatcher with no seeded entry returns NotFound.
        let dispatcher: Arc<dyn Dispatcher> = Arc::new(FakeRemoteDispatcher::new(local_info()));

        let def = ollama_def();
        let outcome = cat
            .opportunistic_reprobe(&def, dispatcher, CancellationToken::new())
            .await;
        assert!(matches!(outcome, ResourceProbeOutcome::NotFound));

        // Lookup still returns the frozen NotFound; overlay stayed empty.
        let later = cat.lookup(&def.id).expect("lookup after reprobe");
        assert!(matches!(later, ResourceProbeOutcome::NotFound));
    }
}
