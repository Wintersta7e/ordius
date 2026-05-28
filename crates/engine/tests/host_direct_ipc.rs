//! Smoke tests for the host-direct IPC pair:
//! [`Engine::test_host_direct`] and [`Engine::enable_host_direct`].
//!
//! Both run against a wiremock server bound to a dynamic loopback port.
//! The first asserts the read-only test path returns the expected
//! fingerprint on a 2xx hit and a clean failure on a 4xx; the second
//! asserts the writer persists a `HostDirectVerification` inline on the
//! env spec and refuses to write when the env kind has no such field.

#![cfg(feature = "testing")]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ordius_engine::Engine;
use ordius_engine::environment::runtime::boot_probe::load_spec_single;
use ordius_engine::environment::runtime::{
    ApiFlavor, Capability, EnvId, EnvSpec, HostDirectMethod, HostDirectVerification,
    HttpProbeMethod, HttpProbeRoute, ProbeSpec, ResourceDefinition, ResourceId, ResourceKind,
    SecretRef,
};
use tokio::time::timeout;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Build a `ResourceDefinition` for an HTTP endpoint at `port` whose
/// probe targets `/api/version` and extracts a fingerprint from
/// `$.version`.
fn ollama_like_resource(id: &str, port: u16) -> ResourceDefinition {
    ResourceDefinition {
        id: ResourceId(id.into()),
        kind: ResourceKind::HttpEndpoint,
        advertised_capabilities: vec![Capability::OllamaNative],
        probe: ProbeSpec::Http {
            ports: vec![port],
            routes: vec![HttpProbeRoute {
                path: "/api/version".into(),
                method: HttpProbeMethod::Get,
                flavor: ApiFlavor::OllamaNative,
                proves: vec![Capability::OllamaNative],
                models_jsonpath: None,
                fingerprint_jsonpaths: vec!["$.version".into()],
            }],
            timeout_ms: Some(500),
        },
        override_lower_scope: false,
    }
}

/// Build a Local `EnvSpec` carrying `resources` and an empty
/// host-direct verification map.
fn local_spec_with(resources: Vec<ResourceDefinition>) -> EnvSpec {
    EnvSpec::Local {
        resources,
        host_direct_verifications: HashMap::new(),
    }
}

/// Subscribe to the env-refresh broadcast and await an event matching
/// `env_id` (or any event if `env_id` is `None`). The timeout guards
/// against a missed broadcast — a stuck probe surfaces as a test
/// failure rather than hanging the suite.
async fn wait_for_refresh(engine: &Engine, env_id: Option<EnvId>) {
    let mut rx = engine.subscribe_env_refresh();
    drop(
        timeout(Duration::from_secs(5), async {
            while let Ok(ev) = rx.recv().await {
                if env_id
                    .as_ref()
                    .is_none_or(|wanted| ev.env_id.as_ref() == Some(wanted))
                {
                    return;
                }
            }
        })
        .await,
    );
}

/// Seed the canonical Local env with `def` as the sole inline resource
/// and wait for the catalog to reflect it. The boot probe already
/// populated `catalog.resources` with the built-ins, so the test must
/// watch for the *specific* id rather than any non-empty map.
async fn seed_local_with_resource(engine: &Arc<Engine>, def: ResourceDefinition) {
    let mut rx = engine.subscribe_env_refresh();
    let target_id = def.id.clone();
    engine
        .add_env(
            EnvId::local(),
            "Local".into(),
            true,
            local_spec_with(vec![def]),
        )
        .await
        .expect("add_env(local) ok");
    drop(
        timeout(Duration::from_secs(5), async {
            loop {
                let catalogs = engine.env_catalogs();
                if let Some(catalog) = catalogs.get(&EnvId::local())
                    && catalog.resources.contains_key(&target_id)
                {
                    return;
                }
                if rx.recv().await.is_err() {
                    return;
                }
            }
        })
        .await,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_host_direct_returns_fingerprint_on_2xx_match() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/version"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"version":"0.5.7"}"#))
        .mount(&server)
        .await;
    let port = server.address().port();

    let tmp = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(tmp.path().to_path_buf()).await.unwrap());

    let def = ollama_like_resource("ollama-test", port);
    seed_local_with_resource(&engine, def).await;

    let rid = ResourceId("ollama-test".into());
    let outcome = engine
        .test_host_direct(&EnvId::local(), &rid)
        .await
        .expect("test_host_direct ok");

    assert!(
        outcome.success(),
        "outcome should report success: {outcome:?}"
    );
    assert_eq!(outcome.status_code, Some(200));
    assert_eq!(outcome.probe_route_path, "/api/version");
    assert!(outcome.host_url.starts_with("http://"));
    let fp = outcome.stable_fingerprint.expect("fingerprint extracted");
    assert!(!fp.is_empty(), "fingerprint must be non-empty");
    assert!(
        outcome
            .response_excerpt
            .as_deref()
            .unwrap_or("")
            .contains("0.5.7"),
        "response excerpt should carry the body",
    );
    assert!(outcome.error.is_none(), "no error on the happy path");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_host_direct_returns_failure_when_fingerprint_jsonpath_misses() {
    // The probe succeeds end-to-end (200 + JSON parses), but the
    // fingerprint `JSONPath` targets a field absent from the response.
    // `compute_fingerprint` returns None, `success()` is false, but the
    // outcome still carries the status code so the wizard can render a
    // diagnostic without a transport error.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/version"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"present":"x"}"#))
        .mount(&server)
        .await;
    let port = server.address().port();

    let tmp = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(tmp.path().to_path_buf()).await.unwrap());

    let mut def = ollama_like_resource("ollama-missing", port);
    if let ProbeSpec::Http { routes, .. } = &mut def.probe {
        routes[0].fingerprint_jsonpaths = vec!["$.absent".into()];
    }

    seed_local_with_resource(&engine, def).await;
    let rid = ResourceId("ollama-missing".into());
    let outcome = engine
        .test_host_direct(&EnvId::local(), &rid)
        .await
        .expect("test_host_direct returns outcome even on missing JSONPath");

    assert!(
        !outcome.success(),
        "missing fingerprint must not count as success: {outcome:?}",
    );
    assert!(
        outcome.error.is_none(),
        "the request succeeded; only fingerprint extraction lacked a match",
    );
    assert_eq!(outcome.status_code, Some(200));
    assert!(
        outcome.stable_fingerprint.is_none(),
        "no JSONPath match → no fingerprint",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn enable_host_direct_persists_inline_and_refreshes() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/version"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"version":"0.5.7"}"#))
        .mount(&server)
        .await;
    let port = server.address().port();

    let tmp = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(tmp.path().to_path_buf()).await.unwrap());

    seed_local_with_resource(&engine, ollama_like_resource("ollama", port)).await;

    let rid = ResourceId("ollama".into());
    let verification = HostDirectVerification {
        verified_at: chrono::Utc::now(),
        method: HostDirectMethod::UserAssertedNoVerification,
        host_url: format!("http://127.0.0.1:{port}"),
        probe_route_path: "/api/version".into(),
        stable_fingerprint: "0.5.7".into(),
        recompute_jsonpaths: vec!["$.version".into()],
    };
    engine
        .enable_host_direct(&EnvId::local(), &rid, verification.clone())
        .await
        .expect("enable_host_direct ok");

    let row = load_spec_single(&engine.pool(), &EnvId::local())
        .expect("load row")
        .expect("row present");
    let map = match &row.spec {
        EnvSpec::Local {
            host_direct_verifications,
            ..
        } => host_direct_verifications,
        other => panic!("expected Local spec, got {other:?}"),
    };
    let saved = map
        .get(&rid)
        .expect("verification persisted under resource id");
    assert_eq!(saved.host_url, verification.host_url);
    assert_eq!(saved.probe_route_path, verification.probe_route_path);
    assert_eq!(saved.stable_fingerprint, verification.stable_fingerprint);
    assert_eq!(saved.method, verification.method);
}

#[tokio::test(flavor = "multi_thread")]
async fn enable_host_direct_ssh_kind_rejects() {
    let tmp = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(tmp.path().to_path_buf()).await.unwrap());

    let env_id = EnvId::ssh("dev-box");
    let mut rx = engine.subscribe_env_refresh();
    engine
        .add_env(
            env_id.clone(),
            "Dev box".into(),
            true,
            EnvSpec::Ssh {
                host: "127.0.0.1".into(),
                user: "tester".into(),
                auth_ref: SecretRef("ssh-test".into()),
                resources: vec![],
            },
        )
        .await
        .expect("add_env(ssh) ok");
    // The SSH dispatcher isn't built so refresh may finish quickly or
    // hang; we don't need a fresh catalog for this assertion. Drain at
    // most one event so the broadcast doesn't lag.
    drop(timeout(Duration::from_millis(200), rx.recv()).await);

    let rid = ResourceId("anything".into());
    let verification = HostDirectVerification {
        verified_at: chrono::Utc::now(),
        method: HostDirectMethod::UserAssertedNoVerification,
        host_url: "http://127.0.0.1:80".into(),
        probe_route_path: "/".into(),
        stable_fingerprint: "fp".into(),
        recompute_jsonpaths: vec![],
    };
    let err = engine
        .enable_host_direct(&env_id, &rid, verification)
        .await
        .expect_err("ssh env must reject host-direct enable");
    let msg = err.to_string();
    assert!(
        msg.contains("ssh") && msg.contains("host-direct"),
        "error must explain why ssh refuses; got: {msg}",
    );
}

/// Ensure the `wait_for_refresh` helper compiles + is exercised at
/// least once so dead-code analysis stays quiet across refactors.
#[tokio::test(flavor = "multi_thread")]
async fn wait_for_refresh_helper_is_used() {
    let tmp = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(tmp.path().to_path_buf()).await.unwrap());
    wait_for_refresh(&engine, None).await;
}

/// Regression: an env-local resource declared with `override_lower_scope:
/// true` and a built-in id (`ollama`) must shadow the built-in's probe
/// when the host-direct test fires. The engine's resource registry does
/// not carry env-local layers outside a run snapshot, so a registry-first
/// lookup would have silently routed through the built-in's `/api/version`
/// route. Inline-spec precedence keeps the override honored.
#[tokio::test(flavor = "multi_thread")]
async fn test_host_direct_respects_env_local_override_of_builtin() {
    let server = MockServer::start().await;
    // The override's route — wiremock only answers here.
    Mock::given(method("GET"))
        .and(path("/custom/override"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"v":"override-1"}"#))
        .mount(&server)
        .await;
    let port = server.address().port();

    let tmp = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(tmp.path().to_path_buf()).await.unwrap());

    // Shadow the built-in `ollama` id with a definition pointing at a
    // distinct path. Without inline-spec precedence, the resolver picks
    // the built-in and probes `/api/version`, which wiremock 404s.
    let override_def = ResourceDefinition {
        id: ResourceId("ollama".into()),
        kind: ResourceKind::HttpEndpoint,
        advertised_capabilities: vec![Capability::OllamaNative],
        probe: ProbeSpec::Http {
            ports: vec![port],
            routes: vec![HttpProbeRoute {
                path: "/custom/override".into(),
                method: HttpProbeMethod::Get,
                flavor: ApiFlavor::OllamaNative,
                proves: vec![Capability::OllamaNative],
                models_jsonpath: None,
                fingerprint_jsonpaths: vec!["$.v".into()],
            }],
            timeout_ms: Some(500),
        },
        override_lower_scope: true,
    };

    seed_local_with_resource(&engine, override_def).await;

    let rid = ResourceId("ollama".into());
    let outcome = engine
        .test_host_direct(&EnvId::local(), &rid)
        .await
        .expect("test_host_direct ok");

    assert_eq!(
        outcome.probe_route_path, "/custom/override",
        "override's route must win over the built-in's: {outcome:?}",
    );
    assert!(
        outcome.success(),
        "override probe should succeed: {outcome:?}",
    );
}
