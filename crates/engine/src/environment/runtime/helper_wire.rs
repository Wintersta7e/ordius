//! Helper-probe wire-format conversion shared between WSL and SSH dispatchers.
//!
//! The functions here translate between `ordius_helper::protocol` types (the
//! JSON wire format read/written by the helper binary) and the engine's own
//! catalog types (`ResourceProbeOutcome`, `ResourceDetail`, etc.).  By living
//! here rather than inside either dispatcher module they ensure the two
//! dispatchers produce identical probe plans and catalog outcomes.

use std::collections::HashMap;
use std::time::Duration;

use ordius_helper::protocol::{
    HttpProbeMethodV1, HttpProbeRouteV1, ProbeDetailV1, ProbeOutcomeBodyV1, ProbePlanV1,
    ResourceKindV1, ResourceSpecV1,
};

use crate::environment::runtime::catalog::{
    ProvenRoute, ResourceDetail, ResourceProbeOutcome, RouteOrigin,
};
use crate::environment::runtime::error::DispatchError;
use crate::environment::runtime::plan::ProbePlan;
use crate::environment::runtime::resource::{
    Capability, HttpProbeMethod, ProbeSpec, ResourceDefinition, ResourceKind,
};

/// Translate a `ResourceDefinition` into the helper's `ResourceSpecV1` wire type.
/// Returns `Err(reason)` if the definition's `kind` and `probe` disagree.
pub fn resource_spec_v1_from_def(def: &ResourceDefinition) -> Result<ResourceSpecV1, String> {
    let kind = match &def.probe {
        ProbeSpec::Http { ports, routes, .. } => {
            if def.kind != ResourceKind::HttpEndpoint {
                return Err(format!(
                    "resource {} has HTTP probe but {:?} kind",
                    def.id, def.kind
                ));
            }
            ResourceKindV1::Http {
                bases: ports
                    .iter()
                    .map(|port| format!("http://127.0.0.1:{port}"))
                    .collect(),
                routes: routes
                    .iter()
                    .map(|route| HttpProbeRouteV1 {
                        path: route.path.clone(),
                        method: match route.method {
                            HttpProbeMethod::Get => HttpProbeMethodV1::Get,
                            HttpProbeMethod::Head => HttpProbeMethodV1::Head,
                        },
                        proves: route
                            .proves
                            .iter()
                            .map(|cap| capability_to_wire(*cap))
                            .collect(),
                        expect_status: Vec::new(),
                        fingerprint_jsonpaths: route.fingerprint_jsonpaths.clone(),
                    })
                    .collect(),
            }
        },
        ProbeSpec::Binary {
            bin,
            extra_search_paths,
            ..
        } => {
            if def.kind != ResourceKind::Binary {
                return Err(format!(
                    "resource {} has binary probe but {:?} kind",
                    def.id, def.kind
                ));
            }
            ResourceKindV1::Binary {
                bin: bin.clone(),
                extra_search_paths: extra_search_paths.clone(),
            }
        },
        ProbeSpec::Toolchain {
            bin,
            version_args,
            version_regex,
            extra_search_paths,
            ..
        } => {
            if def.kind != ResourceKind::Toolchain {
                return Err(format!(
                    "resource {} has toolchain probe but {:?} kind",
                    def.id, def.kind
                ));
            }
            ResourceKindV1::Toolchain {
                bin: bin.clone(),
                version_args: version_args.clone(),
                version_regex: version_regex.clone(),
                extra_search_paths: extra_search_paths.clone(),
            }
        },
    };

    Ok(ResourceSpecV1 {
        id: def.id.0.clone(),
        kind,
    })
}

/// Build a `ProbePlanV1` from an engine `ProbePlan`. Returns
/// `DispatchError::PlanBuild` if any definition cannot be translated.
pub fn build_wire_plan(plan: &ProbePlan) -> Result<ProbePlanV1, DispatchError> {
    let mut resources = Vec::with_capacity(plan.defs.len());
    for def in &plan.defs {
        let spec = resource_spec_v1_from_def(def)
            .map_err(|e| DispatchError::PlanBuild(format!("plan build: {e}")))?;
        resources.push(spec);
    }
    Ok(ProbePlanV1 {
        version: 1,
        per_resource_timeout_ms: duration_millis_u64(plan.per_resource_timeout),
        max_concurrency: u32::try_from(plan.max_concurrency).unwrap_or(u32::MAX),
        overall_budget_ms: duration_millis_u64(plan.overall_budget),
        resources,
    })
}

/// Translate a helper `ProbeOutcomeBodyV1` into an engine `ResourceProbeOutcome`.
///
/// `host_direct_verified` controls whether an HTTP service found inside the
/// environment is tagged `RouteOrigin::HostDirect` (the host can reach it
/// directly) or `RouteOrigin::EnvLoopback` (only accessible from within the
/// env).  WSL callers pass `host_direct.contains_key(&def.id)`; SSH callers
/// always pass `false` (compute-first; no host-direct path today).
pub fn wire_outcome_to_engine(
    body: ProbeOutcomeBodyV1,
    def: &ResourceDefinition,
    host_direct_verified: bool,
) -> ResourceProbeOutcome {
    match body {
        ProbeOutcomeBodyV1::Found(detail) => {
            wire_detail_to_engine(detail, def, host_direct_verified)
        },
        ProbeOutcomeBodyV1::NotFound => ResourceProbeOutcome::NotFound,
        ProbeOutcomeBodyV1::Skipped { reason } => ResourceProbeOutcome::Skipped { reason },
        ProbeOutcomeBodyV1::ProbeFailed { reason } => ResourceProbeOutcome::ProbeFailed { reason },
        ProbeOutcomeBodyV1::TimedOut => ResourceProbeOutcome::TimedOut,
    }
}

/// Translate a helper `ProbeDetailV1` into an engine `ResourceProbeOutcome`.
pub fn wire_detail_to_engine(
    detail: ProbeDetailV1,
    def: &ResourceDefinition,
    host_direct_verified: bool,
) -> ResourceProbeOutcome {
    match detail {
        ProbeDetailV1::HttpEndpoint {
            base_url,
            proven_routes,
        } => {
            let route_origin = if host_direct_verified {
                RouteOrigin::HostDirect
            } else {
                RouteOrigin::EnvLoopback
            };
            ResourceProbeOutcome::Found(ResourceDetail::HttpEndpoint {
                base_url,
                routes_by_capability: routes_by_capability_from_wire(&proven_routes, def),
                version: None,
                models_list: None,
                auth_secret_ref: None,
                streaming_supported_natively: false,
                route_origin,
            })
        },
        ProbeDetailV1::Binary { path } => ResourceProbeOutcome::Found(ResourceDetail::Binary {
            path,
            version: None,
            capabilities: def.advertised_capabilities.clone(),
        }),
        ProbeDetailV1::Toolchain { path, version } => match &def.probe {
            ProbeSpec::Binary { .. } => ResourceProbeOutcome::Found(ResourceDetail::Binary {
                path,
                version: Some(version),
                capabilities: def.advertised_capabilities.clone(),
            }),
            ProbeSpec::Http { .. } | ProbeSpec::Toolchain { .. } => {
                ResourceProbeOutcome::Found(ResourceDetail::Toolchain {
                    name: def.id.0.clone(),
                    version: Some(version),
                    exe_path: path,
                })
            },
        },
    }
}

/// Rebuild a `routes_by_capability` map from helper wire routes and the
/// original probe definition.
pub fn routes_by_capability_from_wire(
    proven_routes: &[ordius_helper::protocol::ProvenRouteV1],
    def: &ResourceDefinition,
) -> HashMap<Capability, ProvenRoute> {
    let ProbeSpec::Http { routes, .. } = &def.probe else {
        return HashMap::new();
    };

    let mut by_capability = HashMap::new();
    for wire_route in proven_routes {
        for wire_cap in &wire_route.capabilities {
            let Some(capability) = capability_from_wire(wire_cap) else {
                continue;
            };
            let Some(route) = routes
                .iter()
                .find(|route| route.path == wire_route.path && route.proves.contains(&capability))
            else {
                tracing::debug!(
                    capability = %wire_cap,
                    path = %wire_route.path,
                    "dropping helper proven route absent from original probe definition"
                );
                continue;
            };

            by_capability
                .entry(capability)
                .or_insert_with(|| ProvenRoute {
                    path: wire_route.path.clone(),
                    method: route.method,
                    flavor: route.flavor,
                });
        }
    }
    by_capability
}

/// Serialize a `Capability` to its `snake_case` wire string.
pub fn capability_to_wire(capability: Capability) -> String {
    match serde_json::to_value(capability).expect("capability serializes as JSON") {
        serde_json::Value::String(value) => value,
        other => unreachable!("capability serialized to non-string JSON value: {other:?}"),
    }
}

/// Deserialize a capability from its `snake_case` wire string.
/// Returns `None` and logs a debug message for unknown values.
pub fn capability_from_wire(value: &str) -> Option<Capability> {
    serde_json::from_value(serde_json::Value::String(value.to_string()))
        .inspect_err(|err| {
            tracing::debug!(
                capability = value,
                error = %err,
                "dropping helper proven route with unknown capability"
            );
        })
        .ok()
}

/// Convert a `Duration` to milliseconds clamped to `u64::MAX`.
pub fn duration_millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::environment::runtime::resource::{
        ApiFlavor, Capability, HttpProbeMethod, HttpProbeRoute, ProbeSpec, ResourceDefinition,
        ResourceId, ResourceKind,
    };

    fn http_def() -> ResourceDefinition {
        ResourceDefinition {
            id: ResourceId("ollama".into()),
            kind: ResourceKind::HttpEndpoint,
            advertised_capabilities: vec![Capability::OllamaNative],
            probe: ProbeSpec::Http {
                ports: vec![11434],
                routes: vec![HttpProbeRoute {
                    path: "/api/version".into(),
                    method: HttpProbeMethod::Get,
                    proves: vec![Capability::OllamaNative],
                    flavor: ApiFlavor::OllamaNative,
                    models_jsonpath: None,
                    fingerprint_jsonpaths: vec!["$.version".into()],
                }],
                timeout_ms: Some(500),
            },
            override_lower_scope: false,
        }
    }

    #[test]
    fn helper_wire_plan_builds_loopback_bases() {
        let def = http_def();
        let spec = resource_spec_v1_from_def(&def).unwrap();
        let ordius_helper::protocol::ResourceKindV1::Http { bases, routes } = spec.kind else {
            panic!("expected http");
        };
        assert_eq!(bases, vec!["http://127.0.0.1:11434"]);
        assert_eq!(routes[0].path, "/api/version");
        assert_eq!(routes[0].proves, vec!["ollama_native"]);
    }

    #[test]
    fn helper_wire_outcome_maps_http_to_env_loopback() {
        let def = http_def();
        let outcome = wire_outcome_to_engine(
            ordius_helper::protocol::ProbeOutcomeBodyV1::Found(
                ordius_helper::protocol::ProbeDetailV1::HttpEndpoint {
                    base_url: "http://127.0.0.1:11434".into(),
                    proven_routes: vec![ordius_helper::protocol::ProvenRouteV1 {
                        capabilities: vec!["ollama_native".into()],
                        path: "/api/version".into(),
                        status: 200,
                        fingerprint: "abc".into(),
                    }],
                },
            ),
            &def,
            false,
        );
        assert!(matches!(
            outcome,
            ResourceProbeOutcome::Found(ResourceDetail::HttpEndpoint {
                route_origin: RouteOrigin::EnvLoopback,
                ..
            })
        ));
    }
}
