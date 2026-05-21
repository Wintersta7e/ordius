//! Probe the host's own loopback for the four known LLM kinds.

use super::types::{
    DiscoveredEndpoint, LocalProbeOutcome, NamespaceInfo, NamespaceKind, NamespaceState,
};
use std::time::Duration;

const PROBE_TIMEOUT: Duration = Duration::from_millis(500);

const PROBES: &[(&str, &str, &str)] = &[
    ("ollama", "http://127.0.0.1:11434", "/api/version"),
    ("lm-studio", "http://127.0.0.1:1234", "/v1/models"),
    ("llamacpp", "http://127.0.0.1:8080", "/v1/models"),
    ("openai-compat", "http://127.0.0.1:8000", "/v1/models"),
];

pub(super) async fn probe_local_namespace() -> LocalProbeOutcome {
    let client = match reqwest::Client::builder().timeout(PROBE_TIMEOUT).build() {
        Ok(c) => c,
        Err(e) => {
            return LocalProbeOutcome::Error {
                reason: format!("client build: {e}"),
            };
        },
    };

    let futures = PROBES.iter().map(|(kind, base, probe_path)| {
        let client = client.clone();
        let url = format!("{base}{probe_path}");
        let kind = (*kind).to_string();
        let base = (*base).to_string();
        async move {
            match client.get(&url).send().await {
                Ok(resp) if resp.status().is_success() => Some(DiscoveredEndpoint::Direct {
                    kind: kind.clone(),
                    name: pretty_name(&kind, &base),
                    namespace_id: "local".to_string(),
                    callable_url: base.clone(),
                    observed_url: base,
                    co_visible_in: Vec::new(),
                }),
                _ => None,
            }
        }
    });

    let endpoints: Vec<_> = futures::future::join_all(futures)
        .await
        .into_iter()
        .flatten()
        .collect();

    LocalProbeOutcome::Done {
        reachable: true,
        endpoints,
    }
}

pub(super) fn build_local_record(
    outer: Result<LocalProbeOutcome, tokio::time::error::Elapsed>,
) -> (NamespaceInfo, Vec<DiscoveredEndpoint>) {
    let template = |state: NamespaceState| NamespaceInfo {
        id: "local".to_string(),
        label: "Local (this machine)".to_string(),
        kind: NamespaceKind::Local,
        enabled: true,
        reachable: state,
    };

    match outer {
        Err(_) => (
            template(NamespaceState::Unreachable {
                reason: "backstop deadline exceeded".to_string(),
            }),
            Vec::new(),
        ),
        Ok(LocalProbeOutcome::Error { reason }) => {
            (template(NamespaceState::Unreachable { reason }), Vec::new())
        },
        Ok(LocalProbeOutcome::Done { endpoints, .. }) => (
            // Reachable regardless of endpoint count — the loopback IS
            // reachable; LLM services may or may not be running. This
            // is the most common state on a fresh host.
            template(NamespaceState::Reachable),
            endpoints,
        ),
    }
}

fn pretty_name(kind: &str, base: &str) -> String {
    let label = match kind {
        "ollama" => "Ollama",
        "lm-studio" => "LM Studio",
        "llamacpp" => "llama.cpp",
        "openai-compat" => "OpenAI-compatible",
        _ => "Endpoint",
    };
    let stripped = base
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    format!("{label} ({stripped})")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn probe_returns_done_with_no_endpoints_when_nothing_listening() {
        // Test env has no services listening on the probed ports.
        let outcome = probe_local_namespace().await;
        assert!(matches!(outcome, LocalProbeOutcome::Done { .. }));
    }

    #[test]
    fn build_local_record_error_arm_is_unreachable() {
        let (ns, eps) = build_local_record(Ok(LocalProbeOutcome::Error {
            reason: "boom".into(),
        }));
        assert_eq!(ns.id, "local");
        assert!(matches!(ns.reachable, NamespaceState::Unreachable { .. }));
        assert!(eps.is_empty());
    }

    #[test]
    fn build_local_record_no_endpoints_is_reachable_empty() {
        let (ns, eps) = build_local_record(Ok(LocalProbeOutcome::Done {
            reachable: true,
            endpoints: Vec::new(),
        }));
        assert!(matches!(ns.reachable, NamespaceState::Reachable));
        assert!(eps.is_empty());
    }
}
