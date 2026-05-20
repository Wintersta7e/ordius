//! Host environment discovery — fires once at `Engine::new`.
//!
//! The GUI shows the user where they are (Windows / WSL / macOS /
//! Linux) and which local LLM endpoints are reachable without making
//! them configure anything.
//!
//! Detection is deliberately cheap: a single `std::env` lookup for the
//! platform, plus a handful of 500 ms HTTP probes against well-known
//! local ports. Anything slower belongs behind a "Refresh" button in
//! the GUI, not on the boot path.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Host operating-system family with WSL split out from Linux so the
/// GUI can show the correct platform chip.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum HostPlatform {
    /// Native Windows host.
    Windows,
    /// Linux running under WSL (detected via `WSL_DISTRO_NAME` or
    /// `microsoft` in `/proc/version`).
    Wsl,
    /// Native Linux host (not WSL).
    Linux,
    /// macOS host.
    MacOs,
    /// Anything else (BSD, unknown).
    Other,
}

/// One LLM endpoint discovered during the boot probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveredEndpoint {
    /// Short kind tag (`ollama`, `lm-studio`, `llamacpp`, `openai-compat`).
    pub kind: String,
    /// Human-friendly label for the GUI ("Ollama (localhost:11434)").
    pub name: String,
    /// Base URL the user can paste into an endpoint config.
    pub base_url: String,
}

/// Snapshot of where the engine is running and what local LLM
/// services responded during the boot probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentReport {
    /// Host platform family.
    pub platform: HostPlatform,
    /// Populated only when `platform == Wsl`; the distro name, e.g.
    /// `Ubuntu-24.04`.
    pub wsl_distro: Option<String>,
    /// Endpoints that responded to a 500 ms HTTP probe. Empty if no
    /// local LLM service is reachable.
    pub endpoints: Vec<DiscoveredEndpoint>,
}

impl EnvironmentReport {
    /// Default report with no probes — useful when the boot probe
    /// times out or is disabled.
    #[must_use]
    pub const fn platform_only(platform: HostPlatform, wsl_distro: Option<String>) -> Self {
        Self {
            platform,
            wsl_distro,
            endpoints: Vec::new(),
        }
    }
}

/// Probe targets. Each entry is `(kind, base_url, probe_path)`.
const PROBES: &[(&str, &str, &str)] = &[
    ("ollama", "http://127.0.0.1:11434", "/api/version"),
    ("lm-studio", "http://127.0.0.1:1234", "/v1/models"),
    ("llamacpp", "http://127.0.0.1:8080", "/v1/models"),
    ("openai-compat", "http://127.0.0.1:8000", "/v1/models"),
];

const PROBE_TIMEOUT_MS: u64 = 500;

/// Detect the host platform and probe well-known local LLM endpoints
/// in parallel. Never errors — failures become "endpoint not present".
pub async fn detect() -> EnvironmentReport {
    let (platform, wsl_distro) = detect_platform();
    let endpoints = probe_endpoints().await;
    EnvironmentReport {
        platform,
        wsl_distro,
        endpoints,
    }
}

fn detect_platform() -> (HostPlatform, Option<String>) {
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

async fn probe_endpoints() -> Vec<DiscoveredEndpoint> {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_millis(PROBE_TIMEOUT_MS))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "environment: build probe client failed");
            return Vec::new();
        },
    };

    let futures = PROBES.iter().map(|(kind, base, probe_path)| {
        let client = client.clone();
        let url = format!("{base}{probe_path}");
        async move {
            match client.get(&url).send().await {
                Ok(resp) if resp.status().is_success() => Some(DiscoveredEndpoint {
                    kind: (*kind).to_string(),
                    name: format!("{} ({})", pretty_name(kind), strip_scheme(base)),
                    base_url: (*base).to_string(),
                }),
                _ => None,
            }
        }
    });

    futures::future::join_all(futures)
        .await
        .into_iter()
        .flatten()
        .collect()
}

fn pretty_name(kind: &str) -> &'static str {
    match kind {
        "ollama" => "Ollama",
        "lm-studio" => "LM Studio",
        "llamacpp" => "llama.cpp",
        "openai-compat" => "OpenAI-compatible",
        _ => "Endpoint",
    }
}

fn strip_scheme(url: &str) -> &str {
    url.trim_start_matches("http://")
        .trim_start_matches("https://")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn platform_detection_returns_known_variant() {
        let (p, _) = detect_platform();
        assert!(matches!(
            p,
            HostPlatform::Windows
                | HostPlatform::Wsl
                | HostPlatform::Linux
                | HostPlatform::MacOs
                | HostPlatform::Other
        ));
    }

    #[tokio::test]
    async fn probe_handles_no_endpoints_gracefully() {
        // No process should be listening on every probed port at once
        // in a CI environment; the call should still return without
        // panicking and the platform field should be populated.
        let r = detect().await;
        assert!(matches!(
            r.platform,
            HostPlatform::Windows
                | HostPlatform::Wsl
                | HostPlatform::Linux
                | HostPlatform::MacOs
                | HostPlatform::Other
        ));
    }

    #[test]
    fn strip_scheme_works() {
        assert_eq!(strip_scheme("http://127.0.0.1:11434"), "127.0.0.1:11434");
        assert_eq!(strip_scheme("https://example.com"), "example.com");
    }
}
