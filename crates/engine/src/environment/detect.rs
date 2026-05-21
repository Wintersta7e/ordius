//! Top-level detection orchestration.

use super::custom::probe_custom_namespace;
use super::local::{build_local_record, probe_local_namespace};
use super::types::{
    DiscoveredEndpoint, EnvironmentReport, HostPlatform, NamespaceInfo, NamespaceKind,
    NamespaceProbeResult, NamespaceState, WslState,
};
use super::wsl::{enumerate_running_distros, enumerate_wsl_distros, probe_wsl_namespace};
use crate::db::DbPool;
use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

/// Overall budget for an environment-detection cycle. Phases 2-4 fan
/// namespace probes out under this deadline.
pub(super) const TOTAL_DETECTION_BUDGET: Duration = Duration::from_secs(4);

/// Extra grace window for any spawned probe to wind down once the
/// main budget has elapsed.
pub(super) const TEARDOWN_GRACE: Duration = Duration::from_secs(3);

/// Detect the host operating-system family and (if WSL) the distro
/// name. Pure-sync; no network or non-trivial filesystem IO beyond a
/// single read of `/proc/version` on Linux.
#[must_use]
pub fn detect_platform() -> (HostPlatform, Option<String>) {
    if cfg!(target_os = "windows") {
        return (HostPlatform::Windows, None);
    }
    if cfg!(target_os = "macos") {
        return (HostPlatform::MacOs, None);
    }
    if cfg!(target_os = "linux") {
        if let Ok(distro) = std::env::var("WSL_DISTRO_NAME")
            && !distro.is_empty()
        {
            return (HostPlatform::Wsl, Some(distro));
        }
        if let Ok(proc) = std::fs::read_to_string("/proc/version")
            && proc.to_ascii_lowercase().contains("microsoft")
        {
            return (HostPlatform::Wsl, None);
        }
        return (HostPlatform::Linux, None);
    }
    (HostPlatform::Other, None)
}

/// Detect the host environment: Local + every reachable namespace.
///
/// Sets a hard deadline before any IO, then probes Local, enumerates
/// WSL distros + user-added Custom namespaces (read from the
/// `namespace_overrides` table), spawns one probe task per namespace
/// into a [`JoinSet`], drains cooperatively at the probe-budget
/// deadline, and detaches any tasks still alive at the hard deadline
/// so their teardown completes on the runtime independently of this
/// function returning.
///
/// `home` is currently unused at the orchestrator level but reserved
/// for future side-channel writes (probe-history caches, etc.);
/// `pool` is consulted for `namespace_overrides`.
pub async fn detect(home: &Path, pool: &DbPool) -> EnvironmentReport {
    let _ = home; // reserved for future probe-history writes
    // 1. Hard deadline FIRST, before any I/O.
    let hard_deadline = Instant::now() + TOTAL_DETECTION_BUDGET + TEARDOWN_GRACE;
    // 2. Probe-budget deadline — the inner loop joins until this elapses.
    let probe_budget_deadline = Instant::now() + TOTAL_DETECTION_BUDGET;

    // 3. Platform.
    let (host_platform, host_wsl_distro) = detect_platform();

    // 4. Local probe (always; not gated on WSL enumeration).
    let local_outcome =
        tokio::time::timeout_at(hard_deadline.into(), probe_local_namespace()).await;
    let (local_namespace, local_endpoints) = build_local_record(local_outcome);

    let cycle_cancel = CancellationToken::new();

    // 5. Enumerate WSL/Custom. On timeout return partial(Local).
    let Ok(target_namespaces) = tokio::time::timeout_at(
        hard_deadline.into(),
        enumerate_target_namespaces(host_platform, pool),
    )
    .await
    else {
        return EnvironmentReport::partial(
            host_platform,
            host_wsl_distro,
            local_namespace,
            local_endpoints,
        );
    };

    let mut set: JoinSet<(String, NamespaceProbeResult)> = JoinSet::new();
    let mut results: HashMap<String, NamespaceProbeResult> = HashMap::new();

    // 6. Spawn per-namespace probe tasks.
    for ns in &target_namespaces {
        if !ns.enabled {
            results.insert(ns.id.clone(), NamespaceProbeResult::Disabled);
            continue;
        }
        let ns_clone = ns.clone();
        let tok = cycle_cancel.child_token();
        set.spawn(async move {
            let result = match &ns_clone.kind {
                NamespaceKind::WslDistro { name, .. } => {
                    probe_wsl_namespace(&ns_clone, name, tok).await
                },
                NamespaceKind::Custom { host } => {
                    let host_str = host.0.to_string();
                    probe_custom_namespace(&ns_clone, &host_str, tok).await
                },
                _ => NamespaceProbeResult::Unreachable {
                    reason: "unexpected kind in target list".into(),
                },
            };
            (ns_clone.id.clone(), result)
        });
    }

    let mut timed_out = false;
    loop {
        match tokio::time::timeout_at(probe_budget_deadline.into(), set.join_next()).await {
            Ok(Some(Ok((id, res)))) => {
                results.insert(id, res);
            },
            Ok(Some(Err(join_err))) => {
                tracing::warn!(error = %join_err, "probe task panicked");
            },
            Ok(None) => break,
            Err(_) => {
                timed_out = true;
                cycle_cancel.cancel();
                let inline_drain = async {
                    while let Some(joined) = set.join_next().await {
                        if let Ok((id, res)) = joined {
                            results.insert(id, res);
                        }
                    }
                };
                if tokio::time::timeout_at(hard_deadline.into(), inline_drain)
                    .await
                    .is_err()
                {
                    // Move remaining tasks into a detached drain so cancel
                    // futures complete on the runtime independently of
                    // `detect()` returning.
                    tokio::spawn(async move { while set.join_next().await.is_some() {} });
                }
                break;
            },
        }
    }

    build_report(
        host_platform,
        host_wsl_distro,
        local_namespace,
        local_endpoints,
        target_namespaces,
        &results,
        timed_out,
    )
}

/// Enumerate every non-Local target the current host can probe.
///
/// WSL distros (Windows + WSL hosts) are enumerated via `wsl.exe` and
/// have their `enabled` flag applied from `namespace_overrides`.
/// Custom namespaces materialize from the same table: every row whose
/// id starts with `custom:` becomes a [`NamespaceInfo`] with a
/// [`NamespaceKind::Custom`] host. Invalid host strings are silently
/// skipped (a defensive guard; `add_custom` validates on insert, but
/// out-of-band edits to the DB shouldn't crash the probe).
///
/// Returns an empty Vec on hosts that cannot reach WSL **and** have
/// no Custom overrides (macOS, non-WSL Linux without user-added
/// remotes).
async fn enumerate_target_namespaces(platform: HostPlatform, pool: &DbPool) -> Vec<NamespaceInfo> {
    let mut out = Vec::new();
    let overrides = crate::namespaces::list(pool).unwrap_or_default();

    if matches!(platform, HostPlatform::Windows | HostPlatform::Wsl) {
        let distros = enumerate_wsl_distros().await;
        let running = enumerate_running_distros().await;
        for d in distros {
            let actually_running =
                matches!(d.state, WslState::Running) && running.contains(&d.name);
            let state = if actually_running {
                WslState::Running
            } else {
                WslState::Stopped
            };
            let id = format!("wsl:{}", d.name);
            let enabled = overrides
                .iter()
                .find(|o| o.namespace_id == id)
                .is_none_or(|o| o.enabled);
            let initial_reachable = if !enabled {
                NamespaceState::Disabled
            } else if matches!(state, WslState::Stopped) {
                NamespaceState::Stopped
            } else {
                NamespaceState::Reachable
            };
            out.push(NamespaceInfo {
                id,
                label: format!("WSL: {}", d.name),
                kind: NamespaceKind::WslDistro {
                    name: d.name,
                    state,
                },
                enabled,
                reachable: initial_reachable,
            });
        }
    }

    for o in &overrides {
        if !o.namespace_id.starts_with("custom:") {
            continue;
        }
        let (Some(label), Some(host_str)) = (&o.custom_label, &o.custom_host) else {
            continue;
        };
        let Ok(host) = crate::namespaces::validate_host(host_str) else {
            continue;
        };
        out.push(NamespaceInfo {
            id: o.namespace_id.clone(),
            label: label.clone(),
            kind: NamespaceKind::Custom { host },
            enabled: o.enabled,
            reachable: if o.enabled {
                NamespaceState::Reachable
            } else {
                NamespaceState::Disabled
            },
        });
    }
    out
}

/// Fold the per-namespace probe results into a single
/// [`EnvironmentReport`]. Local is always first; the remainder
/// preserve the order returned by [`enumerate_target_namespaces`].
fn build_report(
    host_platform: HostPlatform,
    host_wsl_distro: Option<String>,
    local_namespace: NamespaceInfo,
    local_endpoints: Vec<DiscoveredEndpoint>,
    target_namespaces: Vec<NamespaceInfo>,
    results: &HashMap<String, NamespaceProbeResult>,
    timed_out: bool,
) -> EnvironmentReport {
    let mut namespaces = vec![local_namespace];
    let mut endpoints = local_endpoints;
    for ns in target_namespaces {
        match results.get(&ns.id) {
            Some(NamespaceProbeResult::Done {
                namespace,
                endpoints: eps,
            }) => {
                namespaces.push(namespace.clone());
                endpoints.extend(eps.iter().cloned());
            },
            Some(NamespaceProbeResult::Unreachable { reason }) => {
                let mut adjusted = ns;
                adjusted.reachable = NamespaceState::Unreachable {
                    reason: reason.clone(),
                };
                namespaces.push(adjusted);
            },
            Some(NamespaceProbeResult::Stopped) => {
                let mut adjusted = ns;
                adjusted.reachable = NamespaceState::Stopped;
                namespaces.push(adjusted);
            },
            Some(NamespaceProbeResult::Disabled) => {
                let mut adjusted = ns;
                adjusted.reachable = NamespaceState::Disabled;
                namespaces.push(adjusted);
            },
            None => {
                let mut adjusted = ns;
                adjusted.reachable = NamespaceState::Unreachable {
                    reason: "detection deadline exceeded".into(),
                };
                namespaces.push(adjusted);
            },
        }
    }
    EnvironmentReport {
        platform: host_platform,
        wsl_distro: host_wsl_distro,
        namespaces,
        endpoints,
        timed_out,
    }
}
