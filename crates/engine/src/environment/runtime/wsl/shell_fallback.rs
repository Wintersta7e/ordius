//! Constrained POSIX-sh probe runner for WSL when `ordius-helper`
//! bootstrap fails or no embedded triple matches.
//!
//! Limits: HTTP probes only (`SHELL_FALLBACK_SCRIPT` calls curl/wget against
//! a single URL).  Binary / Toolchain probes surface as `Skipped` so the
//! catalog still records every requested id.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use chrono::Utc;
use tokio::process::Command;

use crate::environment::runtime::catalog::{
    ProvenRoute, ResourceCatalog, ResourceDetail, ResourceProbeOutcome, RouteOrigin,
};
use crate::environment::runtime::env::EnvId;
use crate::environment::runtime::error::DispatchError;
use crate::environment::runtime::helper::SHELL_FALLBACK_SCRIPT;
use crate::environment::runtime::resource::{
    HttpProbeMethod, HttpProbeRoute, ProbeSpec, ResourceDefinition,
};

const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Probe a single HTTP resource via the shell-fallback runner.
///
/// Iterates each declared port + route combination, building
/// `routes_by_capability` from each successful 2xx response.  Returns
/// `Found(HttpEndpoint { route_origin: EnvLoopback, .. })` on the first
/// port where at least one route succeeds, mirroring the local
/// dispatcher's port-walk semantics.
pub async fn probe_http_resource(distro: &str, def: &ResourceDefinition) -> ResourceProbeOutcome {
    let ProbeSpec::Http { ports, routes, .. } = &def.probe else {
        return ResourceProbeOutcome::Skipped {
            reason: "shell fallback covers HTTP probes only".into(),
        };
    };

    let mut any_timeout = false;
    let mut first_error: Option<String> = None;

    for &port in ports {
        let base_url = format!("http://127.0.0.1:{port}");
        let mut routes_by_capability: HashMap<_, _> = HashMap::default();
        let mut any_2xx = false;

        for route in routes {
            match run_one_route(distro, &base_url, route).await {
                ShellProbe::Status(code) if (200..300).contains(&code) => {
                    any_2xx = true;
                    for cap in &route.proves {
                        routes_by_capability
                            .entry(*cap)
                            .or_insert_with(|| ProvenRoute {
                                path: route.path.clone(),
                                method: route.method,
                                flavor: route.flavor,
                            });
                    }
                },
                ShellProbe::Status(_) => {},
                ShellProbe::TimedOut => any_timeout = true,
                ShellProbe::Error(reason) => {
                    first_error.get_or_insert(reason);
                },
                ShellProbe::Skipped(reason) => return ResourceProbeOutcome::Skipped { reason },
            }
        }

        if any_2xx {
            return ResourceProbeOutcome::Found(ResourceDetail::HttpEndpoint {
                base_url,
                routes_by_capability,
                version: None,
                models_list: None,
                auth_secret_ref: None,
                streaming_supported_natively: false,
                route_origin: RouteOrigin::EnvLoopback,
            });
        }
    }

    if any_timeout {
        return ResourceProbeOutcome::TimedOut;
    }
    if let Some(reason) = first_error {
        return ResourceProbeOutcome::ProbeFailed { reason };
    }
    ResourceProbeOutcome::NotFound
}

/// Probe an entire `ProbePlan`-like list in shell-fallback mode.
///
/// Iterates HTTP resources only; binary / toolchain resources surface as
/// `Skipped` so the returned catalog still has an entry for every id the
/// caller passed.
pub async fn probe_plan_shell_fallback(
    distro: &str,
    env_id: EnvId,
    registry_revision: u64,
    defs: &[ResourceDefinition],
    overall_budget: Duration,
) -> Result<ResourceCatalog, DispatchError> {
    let started = Instant::now();
    let mut resources: HashMap<_, _> = HashMap::default();
    for def in defs {
        if started.elapsed() >= overall_budget {
            resources.insert(
                def.id.clone(),
                ResourceProbeOutcome::Skipped {
                    reason: "overall budget elapsed".into(),
                },
            );
            continue;
        }
        let outcome = match &def.probe {
            ProbeSpec::Http { .. } => probe_http_resource(distro, def).await,
            ProbeSpec::Binary { .. } | ProbeSpec::Toolchain { .. } => {
                ResourceProbeOutcome::Skipped {
                    reason: "shell fallback covers HTTP probes only".into(),
                }
            },
        };
        resources.insert(def.id.clone(), outcome);
    }
    Ok(ResourceCatalog {
        env_id,
        registry_revision,
        probed_at: Utc::now(),
        resources,
    })
}

enum ShellProbe {
    Status(u16),
    TimedOut,
    Error(String),
    Skipped(String),
}

async fn run_one_route(distro: &str, base_url: &str, route: &HttpProbeRoute) -> ShellProbe {
    if !matches!(route.method, HttpProbeMethod::Get) {
        return ShellProbe::Skipped("shell fallback supports GET probes only".into());
    }
    let result = tokio::time::timeout(
        PROBE_TIMEOUT,
        Command::new("wsl.exe")
            .args([
                "-d",
                distro,
                "--exec",
                "/bin/sh",
                "-c",
                SHELL_FALLBACK_SCRIPT,
                "--",
                base_url,
                &route.path,
            ])
            .output(),
    )
    .await;

    let output = match result {
        Ok(Ok(o)) => o,
        Ok(Err(e)) => return ShellProbe::Error(format!("shell-fallback spawn: {e}")),
        Err(_) => return ShellProbe::TimedOut,
    };
    if !output.status.success() {
        return ShellProbe::Error(format!(
            "shell-fallback exited with {:?}",
            output.status.code()
        ));
    }
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let parsed: serde_json::Value = match serde_json::from_str(&stdout) {
        Ok(v) => v,
        Err(_) => {
            return ShellProbe::Error(format!(
                "shell-fallback emitted unparseable output: {stdout}"
            ));
        },
    };
    if parsed.get("error").is_some() {
        return ShellProbe::Error("shell-fallback reported no curl/wget available in env".into());
    }
    let status_code: u16 = parsed
        .get("status")
        .and_then(serde_json::Value::as_u64)
        .map_or(0, |n| u16::try_from(n).unwrap_or(0));
    ShellProbe::Status(status_code)
}

#[cfg(test)]
mod tests {
    //! These tests do not invoke `wsl.exe` (the real probe runs are
    //! covered by the gated WSL integration test landing in T21).  They
    //! pin the shape of the module's public types so changes to the
    //! catalog or resource enums surface here at compile time rather
    //! than at the T17/T18 call sites.

    use super::*;
    use crate::environment::runtime::env::EnvId;
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

    fn binary_def() -> ResourceDefinition {
        ResourceDefinition {
            id: ResourceId("rg".into()),
            kind: ResourceKind::Binary,
            advertised_capabilities: vec![],
            probe: ProbeSpec::Binary {
                bin: "rg".into(),
                version_args: vec!["--version".into()],
                version_regex: r"(\S+)".into(),
                extra_search_paths: vec![],
                timeout_ms: None,
            },
            override_lower_scope: false,
        }
    }

    #[tokio::test]
    async fn empty_defs_yields_empty_catalog() {
        let cat = probe_plan_shell_fallback(
            "Ubuntu",
            EnvId::wsl("Ubuntu"),
            1,
            &[],
            Duration::from_secs(1),
        )
        .await
        .expect("ok");
        assert!(cat.resources.is_empty());
        assert_eq!(cat.registry_revision, 1);
    }

    #[tokio::test]
    async fn binary_resource_surfaces_as_skipped() {
        let defs = [binary_def()];
        let cat = probe_plan_shell_fallback(
            "Ubuntu",
            EnvId::wsl("Ubuntu"),
            1,
            &defs,
            Duration::from_secs(1),
        )
        .await
        .expect("ok");
        match cat.resources.get(&ResourceId("rg".into())) {
            Some(ResourceProbeOutcome::Skipped { reason }) => {
                assert!(reason.contains("HTTP"));
            },
            other => panic!("expected Skipped, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn elapsed_budget_skips_remaining() {
        // A zero-budget run records every def as `Skipped`.  This avoids
        // touching wsl.exe entirely and pins the budget short-circuit.
        let defs = [http_def(), binary_def()];
        let cat =
            probe_plan_shell_fallback("Ubuntu", EnvId::wsl("Ubuntu"), 1, &defs, Duration::ZERO)
                .await
                .expect("ok");
        for def in &defs {
            match cat.resources.get(&def.id) {
                Some(ResourceProbeOutcome::Skipped { reason }) => {
                    assert_eq!(reason, "overall budget elapsed");
                },
                other => panic!("expected Skipped, got {other:?}"),
            }
        }
    }
}
