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
use super::env::{EnvInfo, RunId, WorkspaceBinding};
use super::error::DispatchError;
use super::plan::{ProbePlan, ProbeSummary};
use super::resource::{Capability, ProbeSpec, ResourceDefinition, ResourceId};
use super::transport::{
    EnvPath, HttpError, HttpRequest, HttpResponse, ProcessCmd, WorkspaceHandle,
};

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
#[derive(Debug)]
pub struct FakeRemoteDispatcher {
    info: EnvInfo,
    /// Seeded outcomes keyed by resource id. Protected by `parking_lot::Mutex`
    /// so the builder pattern (taking `self` by value) can mutate after
    /// construction without requiring `async` or `tokio` context.
    seeded: Mutex<HashMap<ResourceId, FakeResource>>,
    transport: Arc<FakeHttpTransport>,
}

impl FakeRemoteDispatcher {
    /// Create a new fake dispatcher for the given env.
    pub fn new(info: EnvInfo) -> Self {
        Self {
            info,
            seeded: Mutex::new(HashMap::new()),
            transport: Arc::new(FakeHttpTransport),
        }
    }

    /// Seed a resource outcome. Returns `self` for chaining.
    #[must_use]
    pub fn with_seeded(self, id: &str, res: FakeResource) -> Self {
        self.seeded.lock().insert(ResourceId(id.into()), res);
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
        _cancel: CancellationToken,
    ) -> Result<ProbeSummary, DispatchError> {
        let mut resources = HashMap::new();
        for def in &plan.defs {
            let outcome = self.probe_resource(def, CancellationToken::new()).await;
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
        _cancel: CancellationToken,
    ) -> ResourceProbeOutcome {
        // Clone the seed out of the lock immediately so the guard is dropped
        // Clone the seed while the lock is held for the minimum possible time.
        // Using map+cloned avoids naming the guard, so clippy does not see a
        // significant-drop temporary living into the match body.
        let Some(seed) = self.seeded.lock().get(&def.id).cloned() else {
            return ResourceProbeOutcome::NotFound;
        };

        match seed {
            FakeResource::Http {
                base_url,
                capabilities,
            } => {
                let mut routes_by_capability = HashMap::new();
                if let ProbeSpec::Http { routes, .. } = &def.probe
                    && let Some(first_route) = routes.first()
                {
                    for cap in &capabilities {
                        routes_by_capability
                            .entry(*cap)
                            .or_insert_with(|| ProvenRoute {
                                path: first_route.path.clone(),
                                method: first_route.method,
                                flavor: first_route.flavor,
                            });
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
    fn spawn(&self, _cmd: ProcessCmd) -> std::io::Result<crate::executor::supervisor::Supervised> {
        Err(std::io::Error::other(
            "FakeRemoteDispatcher::spawn not yet implemented",
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

    /// Supports `Translated` and `BindMount` workspace bindings only.
    ///
    /// Other bindings return `WorkspaceUnavailable` — those require real
    /// network I/O (`Sync`) or are meaningless for a fake (`Shared`).
    async fn prepare_workspace(
        &self,
        workspace_host: &Path,
        binding: &WorkspaceBinding,
        run_id: &RunId,
    ) -> Result<WorkspaceHandle, DispatchError> {
        match binding {
            WorkspaceBinding::Translated => Ok(WorkspaceHandle {
                env_path: self.translate_path(workspace_host)?,
                teardown: None,
            }),
            WorkspaceBinding::BindMount { env_path } => Ok(WorkspaceHandle {
                // "{run_id}" is a template placeholder in the stored string,
                // not a Rust format argument — suppress the lint.
                #[allow(clippy::literal_string_with_formatting_args)]
                env_path: EnvPath::new(env_path.replace("{run_id}", &run_id.0)),
                teardown: None,
            }),
            other => Err(DispatchError::WorkspaceUnavailable {
                env_id: self.info.id.to_string(),
                reason: format!("FakeRemoteDispatcher does not support {other:?}"),
            }),
        }
    }
}

// ── FakeHttpTransport ─────────────────────────────────────────────────────────

/// Non-streaming `HttpTransport` stub. Always returns `200 {}`.
///
/// Streaming is explicitly unsupported — callers testing streaming behaviour
/// must use `LocalHttpTransport` against a wiremock server.
#[derive(Debug, Default)]
pub struct FakeHttpTransport;

#[async_trait]
impl HttpTransport for FakeHttpTransport {
    /// Return a minimal `200 {}` response without touching the network.
    async fn execute(&self, _req: HttpRequest) -> Result<HttpResponse, HttpError> {
        Ok(HttpResponse {
            status: 200,
            headers: HashMap::default(),
            body: Bytes::from_static(b"{}"),
        })
    }

    /// Always returns `StreamingUnsupported` — the fake emulates a non-streaming
    /// env-wrapped transport (e.g. WSL tunnel before Phase G lands).
    async fn execute_stream(&self, _req: HttpRequest) -> Result<ResponseStream, HttpError> {
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
}
