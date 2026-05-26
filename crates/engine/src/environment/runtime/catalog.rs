//! Resource catalog: probe outcomes, resource detail, proven routes, and route origin types.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::env::{EnvId, SecretRef};
use super::resource::{ApiFlavor, Capability, HttpProbeMethod, ResourceId};

/// Snapshot of all probe results for a single environment at a point in time.
/// Shares resource ids with `ResourceDefinition`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceCatalog {
    /// The environment this catalog describes.
    pub env_id: EnvId,
    /// Registry revision at the time of probing; used for cache invalidation.
    pub registry_revision: u64,
    /// Wall-clock time at which this probe run completed.
    pub probed_at: DateTime<Utc>,
    /// Outcome per resource id.
    pub resources: HashMap<ResourceId, ResourceProbeOutcome>,
}

/// Result of a single resource probe attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ResourceProbeOutcome {
    /// The resource was reachable and its details were captured.
    Found(ResourceDetail),
    /// No process was listening on any of the declared ports / the binary was
    /// not on `PATH`.
    NotFound,
    /// Probe was deliberately skipped (e.g. env is disabled or budget
    /// exhausted before this resource was reached).
    Skipped {
        /// Human-readable explanation of why the probe was skipped.
        reason: String,
    },
    /// The resource was reachable but the probe request failed (non-2xx,
    /// parse error, etc.).
    ProbeFailed {
        /// Human-readable description of the failure.
        reason: String,
    },
    /// The per-resource deadline elapsed before a response arrived.
    TimedOut,
}

/// Concrete detail for a successfully probed resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ResourceDetail {
    /// An HTTP service (LLM inference server, etc.).
    HttpEndpoint {
        /// Base URL used to reach the service (scheme + host + port, no path).
        base_url: String,
        /// Proven routes keyed by the capability each one demonstrates.
        routes_by_capability: HashMap<Capability, ProvenRoute>,
        /// Version string extracted from the probe response, if available.
        #[serde(default)]
        version: Option<String>,
        /// Model identifiers returned by the models-list route, if any.
        #[serde(default)]
        models_list: Option<Vec<String>>,
        /// OS-keyring reference for bearer auth, if required.
        #[serde(default)]
        auth_secret_ref: Option<SecretRef>,
        /// Whether the server supports streaming responses natively (not via
        /// Ordius-side SSE reconstruction).
        #[serde(default)]
        streaming_supported_natively: bool,
        /// How Ordius reached this endpoint.
        route_origin: RouteOrigin,
    },
    /// A standalone executable.
    Binary {
        /// Absolute path to the resolved binary.
        path: String,
        /// Version string extracted from the binary's version output.
        #[serde(default)]
        version: Option<String>,
        /// Capabilities proven by running the binary.
        #[serde(default)]
        capabilities: Vec<Capability>,
    },
    /// A language runtime or toolchain binary.
    Toolchain {
        /// Human-readable toolchain name (e.g. `"rustc"`).
        name: String,
        /// Version string extracted from the binary's version output.
        #[serde(default)]
        version: Option<String>,
        /// Absolute path to the resolved executable.
        exe_path: String,
    },
}

/// A single HTTP route that was successfully probed and proven.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvenRoute {
    /// URL path component (e.g. `"/v1/models"`).
    pub path: String,
    /// HTTP method used during the successful probe.
    pub method: HttpProbeMethod,
    /// API flavor this route belongs to.
    pub flavor: ApiFlavor,
}

/// How Ordius reached a resource endpoint. Drives rebind warnings in the UI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteOrigin {
    /// Reached via the environment's own loopback (e.g. `127.0.0.1` inside a
    /// WSL distro probed with `wsl.exe --exec`).
    EnvLoopback,
    /// Reached directly from the host (service binds `0.0.0.0` or the user
    /// configured a host-direct verification).
    HostDirect,
    /// Reached via a user-configured forwarded tunnel (e.g. SSH port-forward).
    ForwardedTunnel,
    /// Reached via the container bridge network.
    ContainerBridge,
}

/// Concrete HTTP target for a (resource, capability) pair.
///
/// Returned by [`ResourceCatalog::route_for_capability`]. The merged
/// `llm` + `http` node dispatchers build the request URL by joining
/// `base_url` with `path` and applying `method`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteAddress {
    /// Scheme + host + port. No trailing slash, no path.
    pub base_url: String,
    /// Path component to append (already includes the leading `/`).
    pub path: String,
    /// HTTP method to use.
    pub method: HttpProbeMethod,
}

impl ResourceCatalog {
    /// Return the HTTP target for a given (resource id, capability) pair.
    ///
    /// Returns `None` when any of these hold:
    /// - the resource id is not in the catalog
    /// - the resource was probed but did not return [`ResourceProbeOutcome::Found`]
    /// - the proven detail is not [`ResourceDetail::HttpEndpoint`] (binaries
    ///   and toolchains have no HTTP routes)
    /// - the HTTP endpoint did not prove the requested capability
    #[must_use]
    pub fn route_for_capability(
        &self,
        id: &ResourceId,
        capability: Capability,
    ) -> Option<RouteAddress> {
        match self.resources.get(id)? {
            ResourceProbeOutcome::Found(ResourceDetail::HttpEndpoint {
                base_url,
                routes_by_capability,
                ..
            }) => {
                let route = routes_by_capability.get(&capability)?;
                Some(RouteAddress {
                    base_url: base_url.clone(),
                    path: route.path.clone(),
                    method: route.method,
                })
            },
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::environment::runtime::env::EnvId;
    use crate::environment::runtime::resource::{ApiFlavor, HttpProbeMethod};
    use chrono::Utc;
    use std::collections::HashMap;

    #[test]
    fn outcome_found_roundtrips() {
        let outcome = ResourceProbeOutcome::Found(ResourceDetail::HttpEndpoint {
            base_url: "http://127.0.0.1:11434".into(),
            routes_by_capability: HashMap::new(),
            version: Some("0.3.14".into()),
            models_list: None,
            auth_secret_ref: None,
            streaming_supported_natively: true,
            route_origin: RouteOrigin::EnvLoopback,
        });
        let s = serde_json::to_string(&outcome).unwrap();
        assert!(s.contains("\"outcome\":\"found\""));
        let back: ResourceProbeOutcome = serde_json::from_str(&s).unwrap();
        assert_eq!(outcome, back);
    }

    #[test]
    fn catalog_carries_revision() {
        let cat = ResourceCatalog {
            env_id: EnvId::local(),
            registry_revision: 7,
            probed_at: Utc::now(),
            resources: HashMap::new(),
        };
        assert_eq!(cat.registry_revision, 7);
    }

    #[test]
    fn proven_route_records_method_and_flavor() {
        let pr = ProvenRoute {
            path: "/v1/models".into(),
            method: HttpProbeMethod::Get,
            flavor: ApiFlavor::OpenaiChat,
        };
        let s = serde_json::to_string(&pr).unwrap();
        let back: ProvenRoute = serde_json::from_str(&s).unwrap();
        assert_eq!(pr, back);
    }

    #[test]
    fn route_origin_serializes_snake_case() {
        let ro = RouteOrigin::ForwardedTunnel;
        assert_eq!(serde_json::to_string(&ro).unwrap(), "\"forwarded_tunnel\"");
    }

    use crate::environment::runtime::resource::Capability;

    fn http_outcome_with_routes(
        port: u16,
        routes: Vec<(Capability, &str, HttpProbeMethod, ApiFlavor)>,
    ) -> ResourceProbeOutcome {
        let mut map = HashMap::new();
        for (cap, p, m, f) in routes {
            map.insert(
                cap,
                ProvenRoute {
                    path: p.into(),
                    method: m,
                    flavor: f,
                },
            );
        }
        ResourceProbeOutcome::Found(ResourceDetail::HttpEndpoint {
            base_url: format!("http://127.0.0.1:{port}"),
            routes_by_capability: map,
            version: None,
            models_list: None,
            auth_secret_ref: None,
            streaming_supported_natively: true,
            route_origin: RouteOrigin::EnvLoopback,
        })
    }

    #[test]
    fn route_for_capability_returns_full_address() {
        let mut catalog = ResourceCatalog {
            env_id: EnvId::local(),
            registry_revision: 1,
            probed_at: Utc::now(),
            resources: HashMap::new(),
        };
        let id = crate::environment::runtime::resource::ResourceId("ollama".into());
        catalog.resources.insert(
            id.clone(),
            http_outcome_with_routes(
                11434,
                vec![
                    (
                        Capability::OpenaiChatCompletions,
                        "/v1/chat/completions",
                        HttpProbeMethod::Get,
                        ApiFlavor::OpenaiChat,
                    ),
                    (
                        Capability::OllamaNative,
                        "/api/chat",
                        HttpProbeMethod::Get,
                        ApiFlavor::OllamaNative,
                    ),
                ],
            ),
        );

        let chat = catalog
            .route_for_capability(&id, Capability::OpenaiChatCompletions)
            .expect("chat route");
        assert_eq!(chat.base_url, "http://127.0.0.1:11434");
        assert_eq!(chat.path, "/v1/chat/completions");
        assert_eq!(chat.method, HttpProbeMethod::Get);

        let native = catalog
            .route_for_capability(&id, Capability::OllamaNative)
            .expect("native route");
        assert_eq!(native.path, "/api/chat");
    }

    #[test]
    fn route_for_capability_missing_capability_returns_none() {
        let mut catalog = ResourceCatalog {
            env_id: EnvId::local(),
            registry_revision: 1,
            probed_at: Utc::now(),
            resources: HashMap::new(),
        };
        let id = crate::environment::runtime::resource::ResourceId("ollama".into());
        catalog.resources.insert(
            id.clone(),
            http_outcome_with_routes(
                11434,
                vec![(
                    Capability::OpenaiChatCompletions,
                    "/v1/chat/completions",
                    HttpProbeMethod::Get,
                    ApiFlavor::OpenaiChat,
                )],
            ),
        );
        assert!(
            catalog
                .route_for_capability(&id, Capability::OllamaNative)
                .is_none()
        );
    }

    #[test]
    fn route_for_capability_unknown_resource_returns_none() {
        let catalog = ResourceCatalog {
            env_id: EnvId::local(),
            registry_revision: 1,
            probed_at: Utc::now(),
            resources: HashMap::new(),
        };
        let id = crate::environment::runtime::resource::ResourceId("ghost".into());
        assert!(
            catalog
                .route_for_capability(&id, Capability::OpenaiChatCompletions)
                .is_none()
        );
    }

    #[test]
    fn route_for_capability_non_http_resource_returns_none() {
        let mut catalog = ResourceCatalog {
            env_id: EnvId::local(),
            registry_revision: 1,
            probed_at: Utc::now(),
            resources: HashMap::new(),
        };
        let id = crate::environment::runtime::resource::ResourceId("claude-code".into());
        catalog.resources.insert(
            id.clone(),
            ResourceProbeOutcome::Found(ResourceDetail::Binary {
                path: "/usr/local/bin/claude".into(),
                version: Some("1.2.3".into()),
                capabilities: vec![Capability::CliAgentPrint],
            }),
        );
        // Binary is not an HTTP endpoint; the helper deliberately returns None.
        assert!(
            catalog
                .route_for_capability(&id, Capability::CliAgentPrint)
                .is_none()
        );
    }

    #[test]
    fn route_for_capability_unreached_resource_returns_none() {
        let mut catalog = ResourceCatalog {
            env_id: EnvId::local(),
            registry_revision: 1,
            probed_at: Utc::now(),
            resources: HashMap::new(),
        };
        let id = crate::environment::runtime::resource::ResourceId("vllm".into());
        catalog
            .resources
            .insert(id.clone(), ResourceProbeOutcome::NotFound);
        assert!(
            catalog
                .route_for_capability(&id, Capability::OpenaiChatCompletions)
                .is_none()
        );
    }
}
