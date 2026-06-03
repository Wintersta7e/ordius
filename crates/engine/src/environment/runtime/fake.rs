//! `FakeRemoteDispatcher` — in-process `Dispatcher` impl for tests.
//!
//! Configurable per-resource seeded outcomes. Emulates env-local routing
//! (`EnvLoopback` origin, non-streaming transport) so route-identity tests
//! can run in CI without real WSL/SSH.
//!
//! Gated on `#[cfg(any(test, feature = "testing"))]` in `runtime/mod.rs`;
//! this inner attribute mirrors that gate so the module body compiles only
//! under the same conditions.

#![cfg(any(test, feature = "testing"))]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;
use url::Url;

use super::catalog::{
    ProvenRoute, ResourceCatalog, ResourceDetail, ResourceProbeOutcome, RouteOrigin,
};
use super::dispatcher::{Dispatcher, HttpTransport, ResponseStream};
use super::env::EnvInfo;
use super::error::DispatchError;
use super::plan::{ProbePlan, ProbeSummary};
use super::resource::{Capability, ProbeSpec, ResourceDefinition, ResourceId};
use super::transport::{EnvPath, HttpError, HttpRequest, HttpResponse, ProcessCmd};
use super::workspace::WorkspaceTransportFactory;

// ── FakeResource ─────────────────────────────────────────────────────────────

/// Seeded probe outcome for a single resource in a `FakeRemoteDispatcher`.
///
/// `Http` variant emulates an HTTP endpoint reachable only through the fake
/// env's loopback. `Binary` emulates a CLI tool present at a synthetic path.
#[derive(Debug, Clone)]
pub enum FakeResource {
    /// Emulated HTTP endpoint.
    Http {
        /// Base URL returned in the `ResourceDetail`. Does not need to be
        /// reachable — the fake never makes real HTTP calls during probing.
        base_url: String,
        /// Capabilities the seeded endpoint advertises (all treated as proven).
        capabilities: Vec<Capability>,
    },
    /// Emulated binary / toolchain.
    Binary {
        /// Synthetic path reported in the `ResourceDetail`.
        path: String,
        /// Optional version string.
        version: Option<String>,
    },
}

impl FakeResource {
    /// Convenience constructor for the `Http` variant.
    pub fn http(base_url: &str, capabilities: &[Capability]) -> Self {
        Self::Http {
            base_url: base_url.into(),
            capabilities: capabilities.to_vec(),
        }
    }
}

// ── FakeRemoteDispatcher ──────────────────────────────────────────────────────

/// In-process `Dispatcher` that returns seeded outcomes without network I/O.
///
/// Build one with `new`, seed resources with `with_seeded`, then pass to any
/// code that accepts `&dyn Dispatcher`.
pub struct FakeRemoteDispatcher {
    info: EnvInfo,
    /// Seeded outcomes keyed by resource id. Protected by `parking_lot::Mutex`
    /// so the builder pattern (taking `self` by value) can mutate after
    /// construction without requiring `async` or `tokio` context.
    seeded: Mutex<HashMap<ResourceId, FakeResource>>,
    transport: Arc<FakeHttpTransport>,
    /// Optional workspace transport factory; `None` returns the default (no
    /// transport). Set via [`Self::with_workspace_transport`].
    workspace_transport_factory: Option<Arc<dyn WorkspaceTransportFactory>>,
}

impl std::fmt::Debug for FakeRemoteDispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeRemoteDispatcher")
            .field("info", &self.info)
            .field("transport", &self.transport)
            .field(
                "workspace_transport_factory",
                &self
                    .workspace_transport_factory
                    .as_ref()
                    .map(|_| "<factory>"),
            )
            .finish_non_exhaustive()
    }
}

impl FakeRemoteDispatcher {
    /// Create a new fake dispatcher for the given env.
    pub fn new(info: EnvInfo) -> Self {
        Self {
            info,
            seeded: Mutex::new(HashMap::new()),
            transport: Arc::new(FakeHttpTransport::default()),
            workspace_transport_factory: None,
        }
    }

    /// Borrow the transport handle so tests can observe call counters
    /// after running a workflow through this dispatcher.
    #[must_use]
    pub fn transport_handle(&self) -> Arc<FakeHttpTransport> {
        Arc::clone(&self.transport)
    }

    /// Seed a resource outcome. Returns `self` for chaining.
    ///
    /// When seeding a `FakeResource::Http` resource, the matching
    /// `ResourceDefinition` must use `ProbeSpec::Http`; if it does not,
    /// `probe_resource` will return an `HttpEndpoint` detail with an empty
    /// `routes_by_capability` map. That is a test-authoring error, not a
    /// runtime contract violation.
    #[must_use]
    pub fn with_seeded(self, id: &str, res: FakeResource) -> Self {
        self.seeded.lock().insert(ResourceId(id.into()), res);
        self
    }

    /// Attach a workspace transport factory, enabling `workspace_transport()`
    /// to return `Some(factory)`. Returns `self` for chaining.
    ///
    /// Use this to drive `WorkspaceManager::resolve_cwd` and reconcile tests
    /// without a real SSH connection.
    #[must_use]
    pub fn with_workspace_transport(mut self, factory: Arc<dyn WorkspaceTransportFactory>) -> Self {
        self.workspace_transport_factory = Some(factory);
        self
    }
}

#[async_trait]
impl Dispatcher for FakeRemoteDispatcher {
    fn info(&self) -> &EnvInfo {
        &self.info
    }

    /// Probe every def in the plan synchronously against the seeded map.
    ///
    /// Returns immediately — no concurrency or budgets needed for a fake.
    async fn probe(
        &self,
        plan: ProbePlan,
        cancel: CancellationToken,
    ) -> Result<ProbeSummary, DispatchError> {
        let mut resources = HashMap::new();
        for def in &plan.defs {
            let outcome = self.probe_resource(def, cancel.clone()).await;
            resources.insert(def.id.clone(), outcome);
        }
        let total = resources.len();
        Ok(ProbeSummary {
            catalog: ResourceCatalog {
                env_id: self.info.id.clone(),
                registry_revision: plan.registry_revision,
                probed_at: chrono::Utc::now(),
                resources,
            },
            total_probed: total,
            elapsed: Duration::ZERO,
        })
    }

    /// Look up the seeded entry for `def.id`; return `NotFound` if absent.
    ///
    /// For `Http` seeds the routes are taken from the first route in
    /// `def.probe` (if any) so the returned `ProvenRoute` is structurally
    /// consistent with what a real dispatcher would return.
    async fn probe_resource(
        &self,
        def: &ResourceDefinition,
        cancel: CancellationToken,
    ) -> ResourceProbeOutcome {
        // Clone the seed while the lock is held for the minimum possible time.
        // Using map+cloned avoids naming the guard, so clippy does not see a
        // significant-drop temporary living into the match body.
        let seed = tokio::select! {
            biased;
            () = cancel.cancelled() => return cancelled_probe_outcome(),
            seed = async { self.seeded.lock().get(&def.id).cloned() } => seed,
        };
        let Some(seed) = seed else {
            return ResourceProbeOutcome::NotFound;
        };

        match seed {
            FakeResource::Http {
                base_url,
                capabilities,
            } => {
                let mut routes_by_capability = HashMap::new();
                if let ProbeSpec::Http { routes, .. } = &def.probe {
                    for route in routes {
                        for cap in &route.proves {
                            if capabilities.contains(cap) {
                                routes_by_capability
                                    .entry(*cap)
                                    .or_insert_with(|| ProvenRoute {
                                        path: route.path.clone(),
                                        method: route.method,
                                        flavor: route.flavor,
                                    });
                            }
                        }
                    }
                }
                ResourceProbeOutcome::Found(ResourceDetail::HttpEndpoint {
                    base_url,
                    routes_by_capability,
                    version: None,
                    models_list: None,
                    auth_secret_ref: None,
                    // Non-streaming: emulates an env-wrapped transport that
                    // must tunnel through the namespace boundary.
                    streaming_supported_natively: false,
                    route_origin: RouteOrigin::EnvLoopback,
                })
            },
            FakeResource::Binary { path, version } => {
                ResourceProbeOutcome::Found(ResourceDetail::Binary {
                    path,
                    version,
                    capabilities: def.advertised_capabilities.clone(),
                })
            },
        }
    }

    /// Not implemented — fake dispatcher is for probe/catalog tests only.
    async fn spawn(
        &self,
        _cmd: ProcessCmd,
    ) -> Result<Box<dyn super::transport::EnvProcess>, DispatchError> {
        Err(DispatchError::Unsupported(
            "test dispatcher does not spawn processes".into(),
        ))
    }

    fn http_transport(&self) -> Arc<dyn HttpTransport> {
        self.transport.clone()
    }

    /// Prepend `/fake/` to the host path — emulates a simple env-side mount.
    fn translate_path(&self, host_path: &Path) -> Result<EnvPath, DispatchError> {
        Ok(EnvPath::new(format!(
            "/fake/{}",
            host_path.to_string_lossy().trim_start_matches('/'),
        )))
    }

    /// Return the seeded factory if one was set via
    /// [`FakeRemoteDispatcher::with_workspace_transport`]; otherwise `None`.
    fn workspace_transport(&self) -> Option<Arc<dyn super::workspace::WorkspaceTransportFactory>> {
        self.workspace_transport_factory.clone()
    }
}

fn cancelled_probe_outcome() -> ResourceProbeOutcome {
    ResourceProbeOutcome::Skipped {
        reason: "cancelled".into(),
    }
}

// ── FakeHttpTransport ─────────────────────────────────────────────────────────

/// Non-streaming `HttpTransport` stub. Always returns `200 {}`.
///
/// Streaming is explicitly unsupported — callers testing streaming behaviour
/// must use `LocalHttpTransport` against a wiremock server.
///
/// Tracks `execute` / `execute_stream` invocations on an internal atomic so
/// integration tests can prove that the right env's transport was used by the
/// run loop (rather than the host-local one).
#[derive(Debug, Default)]
pub struct FakeHttpTransport {
    execute_calls: AtomicUsize,
    stream_calls: AtomicUsize,
}

impl FakeHttpTransport {
    /// Number of `execute` invocations observed so far.
    #[must_use]
    pub fn execute_calls(&self) -> usize {
        self.execute_calls.load(Ordering::Acquire)
    }

    /// Number of `execute_stream` invocations observed so far.
    #[must_use]
    pub fn stream_calls(&self) -> usize {
        self.stream_calls.load(Ordering::Acquire)
    }
}

#[async_trait]
impl HttpTransport for FakeHttpTransport {
    /// Return a minimal `200 {}` response without touching the network.
    async fn execute(&self, _req: HttpRequest) -> Result<HttpResponse, HttpError> {
        self.execute_calls.fetch_add(1, Ordering::AcqRel);
        Ok(HttpResponse {
            status: 200,
            headers: HashMap::default(),
            body: Bytes::from_static(b"{}"),
        })
    }

    /// Always returns `StreamingUnsupported` — the fake emulates a non-streaming
    /// env-wrapped transport (e.g. WSL tunnel before Phase G lands).
    async fn execute_stream(&self, _req: HttpRequest) -> Result<ResponseStream, HttpError> {
        self.stream_calls.fetch_add(1, Ordering::AcqRel);
        Err(HttpError::StreamingUnsupported {
            route_origin: "env_loopback".into(),
        })
    }

    /// Always `false` — `FakeHttpTransport` does not stream.
    fn can_stream(&self, _url: &Url) -> bool {
        false
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::environment::runtime::catalog::ResourceProbeOutcome;
    use crate::environment::runtime::env::{EnvId, EnvInfo, EnvSpec, EnvState};
    use crate::environment::runtime::resource::{
        ApiFlavor, Capability, HttpProbeMethod, HttpProbeRoute, ProbeSpec, ResourceDefinition,
        ResourceId, ResourceKind,
    };
    use tokio_util::sync::CancellationToken;

    fn info(label: &str) -> EnvInfo {
        EnvInfo {
            id: EnvId::new(format!("fake:{label}")),
            label: label.into(),
            spec: EnvSpec::Local {
                resources: vec![],
                host_direct_verifications: HashMap::default(),
            },
            state: EnvState::Reachable,
            enabled: true,
        }
    }

    #[tokio::test]
    async fn fake_returns_seeded_outcome() {
        let fake = FakeRemoteDispatcher::new(info("a")).with_seeded(
            "ollama",
            FakeResource::http("http://fake/11434", &[Capability::OllamaNative]),
        );
        let def = ResourceDefinition {
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
        };
        let outcome = fake.probe_resource(&def, CancellationToken::new()).await;
        let ResourceProbeOutcome::Found(ResourceDetail::HttpEndpoint { base_url, .. }) = outcome
        else {
            panic!("expected Found, got {outcome:?}")
        };
        assert_eq!(base_url, "http://fake/11434");
    }

    #[tokio::test]
    async fn fake_probe_cancelled_returns_skipped() {
        let fake = FakeRemoteDispatcher::new(info("a")).with_seeded(
            "ollama",
            FakeResource::http("http://fake/11434", &[Capability::OllamaNative]),
        );
        let def = ResourceDefinition {
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
        };
        let cancel = CancellationToken::new();
        cancel.cancel();

        let outcome = fake.probe_resource(&def, cancel).await;
        assert!(matches!(
            outcome,
            ResourceProbeOutcome::Skipped { reason } if reason == "cancelled"
        ));
    }

    #[tokio::test]
    async fn fake_capability_proof_uses_actual_route_proves() {
        let fake = FakeRemoteDispatcher::new(info("a")).with_seeded(
            "ollama",
            FakeResource::http(
                "http://fake/11434",
                &[Capability::OllamaNative, Capability::OpenaiChatCompletions],
            ),
        );
        let def = ResourceDefinition {
            id: ResourceId("ollama".into()),
            kind: ResourceKind::HttpEndpoint,
            advertised_capabilities: vec![
                Capability::OllamaNative,
                Capability::OpenaiChatCompletions,
                Capability::CodeFormatter,
            ],
            probe: ProbeSpec::Http {
                ports: vec![11434],
                routes: vec![
                    HttpProbeRoute {
                        path: "/api/version".into(),
                        method: HttpProbeMethod::Get,
                        flavor: ApiFlavor::OllamaNative,
                        proves: vec![Capability::OllamaNative],
                        models_jsonpath: None,
                        fingerprint_jsonpaths: vec![],
                    },
                    HttpProbeRoute {
                        path: "/format".into(),
                        method: HttpProbeMethod::Get,
                        flavor: ApiFlavor::Custom,
                        proves: vec![Capability::CodeFormatter],
                        models_jsonpath: None,
                        fingerprint_jsonpaths: vec![],
                    },
                ],
                timeout_ms: None,
            },
            override_lower_scope: false,
        };

        let outcome = fake.probe_resource(&def, CancellationToken::new()).await;
        let ResourceProbeOutcome::Found(ResourceDetail::HttpEndpoint {
            routes_by_capability,
            ..
        }) = outcome
        else {
            panic!("expected Found, got {outcome:?}")
        };

        let proven = routes_by_capability
            .get(&Capability::OllamaNative)
            .expect("ollama route is proven");
        assert_eq!(proven.path, "/api/version");
        assert!(!routes_by_capability.contains_key(&Capability::OpenaiChatCompletions));
        assert!(!routes_by_capability.contains_key(&Capability::CodeFormatter));
    }
}
