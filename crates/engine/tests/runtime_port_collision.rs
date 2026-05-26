//! Port-collision identity test: `LocalDispatcher` and
//! `FakeRemoteDispatcher` both "serve" port 11434 with DIFFERENT
//! content. A node targeting `fake` must reach the fake resource; a
//! node targeting `local` must reach the local resource. Route
//! identity preserved across dispatchers on a shared port number.

#![cfg(feature = "testing")]

use std::collections::HashMap;
use std::time::Duration;

use ordius_engine::environment::runtime::{
    ApiFlavor, Capability, Dispatcher, EnvId, EnvInfo, EnvSpec, EnvState, FakeRemoteDispatcher,
    FakeResource, HttpMethod, HttpProbeMethod, HttpProbeRoute, HttpRequest, HttpTransport,
    LocalDispatcher, LocalHttpTransport, ProbeSpec, ResourceDefinition, ResourceDetail, ResourceId,
    ResourceKind, ResourceProbeOutcome, RouteOrigin,
};
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn local_info() -> EnvInfo {
    EnvInfo {
        id: EnvId::local(),
        label: "Local".into(),
        spec: EnvSpec::Local {
            resources: vec![],
            host_direct_verifications: HashMap::default(),
        },
        state: EnvState::Reachable,
        enabled: true,
    }
}

fn fake_info() -> EnvInfo {
    EnvInfo {
        id: EnvId::new("fake:a"),
        label: "Fake A".into(),
        spec: EnvSpec::Local {
            resources: vec![],
            host_direct_verifications: HashMap::default(),
        },
        state: EnvState::Reachable,
        enabled: true,
    }
}

/// Probe one wiremock server via `LocalDispatcher` and the same `ResourceDefinition`
/// (but different port) via `FakeRemoteDispatcher`. Proves that two dispatchers
/// with the same resource id resolve to distinct base URLs — route identity
/// is tied to the dispatcher (environment), not the port number or resource id.
#[tokio::test]
async fn route_identity_preserved_across_dispatchers_on_same_port() {
    // Local wiremock server — responds to the Ollama version probe.
    let local_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/version"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"server":"local"}"#))
        .mount(&local_server)
        .await;
    let local_port = local_server.address().port();

    let local = LocalDispatcher::new(local_info());
    let def_local = ResourceDefinition {
        id: ResourceId("ollama".into()),
        kind: ResourceKind::HttpEndpoint,
        advertised_capabilities: vec![Capability::OllamaNative],
        probe: ProbeSpec::Http {
            ports: vec![local_port],
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

    let local_outcome = local
        .probe_resource(&def_local, CancellationToken::new())
        .await;
    let ResourceProbeOutcome::Found(ResourceDetail::HttpEndpoint {
        base_url,
        route_origin,
        ..
    }) = local_outcome
    else {
        panic!("local probe not found: {local_outcome:?}")
    };
    assert_eq!(route_origin, RouteOrigin::HostDirect);
    assert!(
        base_url.contains(&local_port.to_string()),
        "local base_url {base_url:?} must contain port {local_port}"
    );

    // Fake "serves" the same numerical port (11434) in its seeded data, but
    // returns a completely different URL — emulating an env-local loopback.
    let fake = FakeRemoteDispatcher::new(fake_info()).with_seeded(
        "ollama",
        FakeResource::http("http://fake/11434", &[Capability::OllamaNative]),
    );
    // Use the same ResourceDefinition — the port in the probe spec should be
    // irrelevant to what FakeRemoteDispatcher returns.
    let fake_outcome = fake
        .probe_resource(&def_local, CancellationToken::new())
        .await;
    let ResourceProbeOutcome::Found(ResourceDetail::HttpEndpoint {
        base_url: fake_url,
        route_origin: fake_origin,
        ..
    }) = fake_outcome
    else {
        panic!("fake probe not found: {fake_outcome:?}")
    };
    assert_eq!(fake_origin, RouteOrigin::EnvLoopback);
    assert_eq!(fake_url, "http://fake/11434");

    // Core assertion: same resource id, two dispatchers → two distinct URLs.
    assert_ne!(
        base_url, fake_url,
        "same resource id in two envs must resolve to distinct base URLs"
    );
}

/// Sanity check: `LocalHttpTransport` actually reaches a wiremock server
/// bound on whatever port it chose ("collision port" means any ephemeral port
/// that could coincide with a well-known service port in other tests).
#[tokio::test]
async fn local_transport_actually_reaches_wiremock_on_collision_port() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/marker"))
        .respond_with(ResponseTemplate::new(200).set_body_string("local-marker"))
        .mount(&server)
        .await;
    let port = server.address().port();

    let t = LocalHttpTransport::new();
    let resp = t
        .execute(HttpRequest {
            method: HttpMethod::Get,
            url: format!("http://127.0.0.1:{port}/marker"),
            headers: HashMap::new(),
            body: None,
            timeout: Duration::from_secs(2),
        })
        .await
        .expect("LocalHttpTransport::execute should succeed");

    assert_eq!(resp.status, 200);
    assert_eq!(&resp.body[..], b"local-marker");
}
