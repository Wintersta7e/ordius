//! Top-level detection orchestration.

use super::local::{build_local_record, probe_local_namespace};
use super::types::{EnvironmentReport, HostPlatform};
use crate::db::DbPool;
use std::path::Path;
use std::time::{Duration, Instant};

/// Overall budget for an environment-detection cycle. Phases 2-4 will
/// fan namespace probes out under this deadline; phase 1b only has
/// the Local probe to honor.
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

/// Phase 1b: Local only. WSL/Custom probing added in phases 2-4.
pub async fn detect(_home: &Path, _pool: &DbPool) -> EnvironmentReport {
    let hard_deadline = Instant::now() + TOTAL_DETECTION_BUDGET + TEARDOWN_GRACE;
    let (host_platform, host_wsl_distro) = detect_platform();

    let local_outcome =
        tokio::time::timeout_at(hard_deadline.into(), probe_local_namespace()).await;
    let (local_namespace, local_endpoints) = build_local_record(local_outcome);

    EnvironmentReport {
        platform: host_platform,
        wsl_distro: host_wsl_distro,
        namespaces: vec![local_namespace],
        endpoints: local_endpoints,
        timed_out: false,
    }
}
