//! Smoke tests for `Engine::{add,remove}_env_local_resource`.
//!
//! These exercise the env-local resource CRUD IPC: each writer acquires
//! `env_refresh_lock`, mutates `EnvSpec::resources` inline, persists the
//! row, and re-runs the per-env refresh so the run-snapshot registry
//! sees the change on the next probe.

use std::sync::Arc;

use ordius_engine::Engine;
use ordius_engine::environment::runtime::boot_probe::load_spec_single;
use ordius_engine::environment::runtime::{
    Capability, EnvId, EnvSpec, ProbeSpec, ResourceDefinition, ResourceId, ResourceKind,
};

/// Build a WSL distro spec carrying no inline resources.
fn empty_wsl_spec(name: &str) -> EnvSpec {
    EnvSpec::WslDistro {
        name: name.to_string(),
        resources: vec![],
        host_direct_verifications: std::collections::HashMap::new(),
    }
}

/// Build a synthetic HTTP resource at the given id. Probe targets a port
/// nothing listens on so the call never blocks on a real service.
fn http_resource(id: &str, override_lower: bool) -> ResourceDefinition {
    ResourceDefinition {
        id: ResourceId(id.to_string()),
        kind: ResourceKind::HttpEndpoint,
        advertised_capabilities: vec![Capability::OpenaiChatCompletions],
        probe: ProbeSpec::Http {
            ports: vec![1],
            routes: vec![],
            timeout_ms: Some(50),
        },
        override_lower_scope: override_lower,
    }
}

/// Re-read `env_specs.spec_json` for `env_id` and return the resource
/// ids in declaration order. Lets each test assert on the persisted
/// state, not just the in-memory registry.
fn persisted_resource_ids(engine: &Engine, env_id: &EnvId) -> Vec<String> {
    let row = load_spec_single(&engine.pool(), env_id)
        .expect("load_spec_single ok")
        .expect("env row present");
    row.spec
        .resources()
        .iter()
        .map(|r| r.id.0.clone())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn add_env_local_resource_appends_inline_and_refreshes() {
    let tmp = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(tmp.path().to_path_buf()).await.unwrap());

    let env_id = EnvId::wsl("Ubuntu");
    engine
        .add_env(
            env_id.clone(),
            "WSL: Ubuntu".into(),
            true,
            empty_wsl_spec("Ubuntu"),
        )
        .await
        .expect("seed env");

    engine
        .add_env_local_resource(&env_id, http_resource("my-llm", false))
        .await
        .expect("add resource");

    let ids = persisted_resource_ids(&engine, &env_id);
    assert_eq!(ids, vec!["my-llm".to_string()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn add_env_local_resource_collision_errors_and_preserves_spec() {
    let tmp = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(tmp.path().to_path_buf()).await.unwrap());

    let env_id = EnvId::wsl("Ubuntu");
    engine
        .add_env(
            env_id.clone(),
            "WSL: Ubuntu".into(),
            true,
            empty_wsl_spec("Ubuntu"),
        )
        .await
        .expect("seed env");

    engine
        .add_env_local_resource(&env_id, http_resource("my-llm", false))
        .await
        .expect("first add");

    let err = engine
        .add_env_local_resource(&env_id, http_resource("my-llm", false))
        .await
        .expect_err("second add must collide");
    let msg = err.to_string();
    assert!(
        msg.contains("my-llm") && msg.contains("already"),
        "collision error must mention the offending id; got: {msg}",
    );

    let ids = persisted_resource_ids(&engine, &env_id);
    assert_eq!(
        ids,
        vec!["my-llm".to_string()],
        "spec must hold a single inline resource after the rejected add",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn remove_env_local_resource_drops_inline_and_refreshes() {
    let tmp = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(tmp.path().to_path_buf()).await.unwrap());

    let env_id = EnvId::wsl("Ubuntu");
    engine
        .add_env(
            env_id.clone(),
            "WSL: Ubuntu".into(),
            true,
            empty_wsl_spec("Ubuntu"),
        )
        .await
        .expect("seed env");
    engine
        .add_env_local_resource(&env_id, http_resource("my-llm", false))
        .await
        .expect("add resource");

    engine
        .remove_env_local_resource(&env_id, &ResourceId("my-llm".to_string()))
        .await
        .expect("remove resource");

    let ids = persisted_resource_ids(&engine, &env_id);
    assert!(
        ids.is_empty(),
        "inline resources must be empty after removal"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn remove_env_local_resource_missing_id_errors() {
    let tmp = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(tmp.path().to_path_buf()).await.unwrap());

    let env_id = EnvId::wsl("Ubuntu");
    engine
        .add_env(
            env_id.clone(),
            "WSL: Ubuntu".into(),
            true,
            empty_wsl_spec("Ubuntu"),
        )
        .await
        .expect("seed env");

    let err = engine
        .remove_env_local_resource(&env_id, &ResourceId("ghost".to_string()))
        .await
        .expect_err("removing an unknown id must error");
    let msg = err.to_string();
    assert!(
        msg.contains("ghost") && msg.contains("not declared"),
        "remove error must mention the missing id; got: {msg}",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn add_env_local_resource_unknown_env_errors() {
    let tmp = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(tmp.path().to_path_buf()).await.unwrap());

    let missing = EnvId::wsl("Nowhere");
    let err = engine
        .add_env_local_resource(&missing, http_resource("my-llm", false))
        .await
        .expect_err("add against an unknown env must error");
    assert!(
        matches!(err, ordius_engine::EngineError::EnvUnknown(ref id) if id == &missing),
        "expected EnvUnknown, got: {err}",
    );
}
