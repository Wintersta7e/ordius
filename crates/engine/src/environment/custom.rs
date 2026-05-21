//! Probe a user-added Custom namespace.
//!
//! Reuses the four well-known LLM ports from the WSL/Local probes
//! (Ollama 11434, LM Studio 1234, llama.cpp 8080, openai-compat 8000).
//! Each port gets a 500 ms reqwest GET; responding ports surface as
//! [`DiscoveredEndpoint::Direct`] since a Custom namespace is, by
//! definition, directly reachable from the host (otherwise the user
//! wouldn't have a way to type its hostname).
//!
//! Cancellation is cooperative: the orchestrator's per-cycle token
//! is cloned per probe and selected against `client.get(...).send()`.

use super::types::{DiscoveredEndpoint, NamespaceInfo, NamespaceProbeResult, NamespaceState};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const PROBE_TIMEOUT: Duration = Duration::from_millis(500);

/// `(service_kind, port, well_known_health_path)` tuples — matches the
/// inventory used by `wsl::STATIC_SCRIPT` and `local::probe_local_namespace`.
const PROBES: &[(&str, u16, &str)] = &[
    ("ollama", 11434, "/api/version"),
    ("lm-studio", 1234, "/v1/models"),
    ("llamacpp", 8080, "/v1/models"),
    ("openai-compat", 8000, "/v1/models"),
];

/// Probe a Custom namespace by spawning one reqwest GET per well-known
/// LLM port against `host`. Endpoints that respond surface as
/// [`DiscoveredEndpoint::Direct`].
///
/// `host` is rendered into the URL as `http://{host}:<port>/...`. IPv6
/// hosts must already be bracketed (`[::1]`); `url::Host::to_string()`
/// handles this on the caller side.
///
/// If at least one probe responds, the namespace surfaces as
/// [`NamespaceState::Reachable`]. Otherwise the namespace surfaces as
/// [`NamespaceState::Unreachable`] with reason `"no probes responded"`.
pub(super) async fn probe_custom_namespace(
    ns: &NamespaceInfo,
    host: &str,
    cancel: CancellationToken,
) -> NamespaceProbeResult {
    let client = match reqwest::Client::builder().timeout(PROBE_TIMEOUT).build() {
        Ok(c) => c,
        Err(e) => {
            return NamespaceProbeResult::Unreachable {
                reason: format!("client build: {e}"),
            };
        },
    };

    let futures = PROBES.iter().map(|(kind, port, path)| {
        let client = client.clone();
        let base = format!("http://{host}:{port}");
        let url = format!("{base}{path}");
        let kind = (*kind).to_string();
        let ns_id = ns.id.clone();
        let cancel = cancel.clone();
        async move {
            tokio::select! {
                resp = client.get(&url).send() => match resp {
                    Ok(r) if r.status().is_success() => Some(DiscoveredEndpoint::Direct {
                        kind: kind.clone(),
                        name: format!("{kind} ({})", base.trim_start_matches("http://")),
                        namespace_id: ns_id,
                        callable_url: base.clone(),
                        observed_url: base,
                        co_visible_in: Vec::new(),
                    }),
                    _ => None,
                },
                () = cancel.cancelled() => None,
            }
        }
    });

    let endpoints: Vec<DiscoveredEndpoint> = futures::future::join_all(futures)
        .await
        .into_iter()
        .flatten()
        .collect();

    let reachable = if endpoints.is_empty() {
        NamespaceState::Unreachable {
            reason: "no probes responded".into(),
        }
    } else {
        NamespaceState::Reachable
    };
    NamespaceProbeResult::Done {
        namespace: NamespaceInfo {
            reachable,
            ..ns.clone()
        },
        endpoints,
    }
}
