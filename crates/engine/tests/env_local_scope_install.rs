//! Phase E Task 14: env-local resources land at `ScopeKey::EnvLocal` in the
//! per-run snapshot's registry clone — NOT in the engine-level registry —
//! and resolve only when walking from the owning env's scope chain.

use std::sync::Arc;

use ordius_engine::Engine;
use ordius_engine::environment::runtime::registry::ScopeKey;
use ordius_engine::environment::runtime::{EnvId, ResourceId, WorkflowId};

/// Pre-seed `env_specs` with a WSL distro spec carrying an env-local
/// resource definition. `unique` is the WSL distro name; `resource_id` is
/// the resource id to install at that env's `EnvLocal` layer. `override_lower`
/// controls the `override_lower_scope` flag in the persisted JSON.
fn seed_wsl_env_spec(
    tmp: &tempfile::TempDir,
    distro: &str,
    resource_id: &str,
    override_lower: bool,
) {
    let db_path = tmp.path().join("runs.db");
    let pool = ordius_engine::db::open(&db_path).unwrap();
    let conn = pool.get().unwrap();
    let spec_json = serde_json::json!({
        "type": "wsl_distro",
        "name": distro,
        "resources": [{
            "id": resource_id,
            "kind": "http_endpoint",
            "advertised_capabilities": [],
            "probe": {
                "kind": "http",
                "ports": [11434],
                "routes": [],
                "timeout_ms": null,
            },
            "override_lower_scope": override_lower,
        }],
        "host_direct_verifications": {},
    });
    let env_id = format!("wsl:{distro}");
    let label = format!("WSL: {distro}");
    conn.execute(
        "INSERT INTO env_specs (id, label, enabled, spec_json, created_at, updated_at)
         VALUES (?1, ?2, 1, ?3, 0, 0)",
        rusqlite::params![env_id, label, spec_json.to_string()],
    )
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn env_local_resource_resolves_in_run_snapshot() {
    let tmp = tempfile::TempDir::new().unwrap();
    seed_wsl_env_spec(&tmp, "Ubuntu", "my-llm", false);

    let engine = Arc::new(Engine::new(tmp.path().to_path_buf()).await.unwrap());

    let wsl_env = EnvId::new("wsl:Ubuntu");
    let snap = engine
        .build_run_snapshot(
            "run-1",
            WorkflowId("wf".to_string()),
            EnvId::local(),
            std::slice::from_ref(&wsl_env),
        )
        .expect("snapshot builds");

    let resource = ResourceId("my-llm".to_string());
    let (def, scope) = snap
        .registry
        .resolve(&resource, &wsl_env, None)
        .expect("env-local resource visible from its own env's scope chain");
    assert_eq!(def.id, resource);
    assert!(
        matches!(scope, ScopeKey::EnvLocal { id } if id == wsl_env),
        "resource must resolve at the EnvLocal layer keyed to wsl:Ubuntu",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn env_local_resource_invisible_from_other_envs() {
    let tmp = tempfile::TempDir::new().unwrap();
    seed_wsl_env_spec(&tmp, "Ubuntu", "my-llm", false);

    let engine = Arc::new(Engine::new(tmp.path().to_path_buf()).await.unwrap());

    let wsl_env = EnvId::new("wsl:Ubuntu");
    let snap = engine
        .build_run_snapshot(
            "run-2",
            WorkflowId("wf".to_string()),
            EnvId::local(),
            std::slice::from_ref(&wsl_env),
        )
        .expect("snapshot builds");

    let resource = ResourceId("my-llm".to_string());
    assert!(
        snap.registry
            .resolve(&resource, &EnvId::local(), None)
            .is_none(),
        "env-local resource scoped to wsl:Ubuntu must not be visible from local",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn env_local_resource_invisible_from_engine_registry() {
    let tmp = tempfile::TempDir::new().unwrap();
    seed_wsl_env_spec(&tmp, "Ubuntu", "my-llm", false);

    let engine = Arc::new(Engine::new(tmp.path().to_path_buf()).await.unwrap());

    // The engine's bare registry snapshot must not contain the env-local
    // resource — only the per-run snapshot does. This pins the
    // "engine registry stays unchanged" contract.
    let engine_snap = engine.resource_registry().snapshot();
    let wsl_scope = ScopeKey::EnvLocal {
        id: EnvId::new("wsl:Ubuntu"),
    };
    assert!(
        engine_snap
            .layers
            .get(&wsl_scope)
            .is_none_or(|layer| { !layer.contains_key(&ResourceId("my-llm".to_string())) }),
        "engine registry must not carry env-local defs from EnvSpec.resources",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn env_local_shadow_without_override_errors() {
    let tmp = tempfile::TempDir::new().unwrap();
    // `ollama` is a builtin id; without override_lower_scope the overlay
    // install must reject the env-local entry.
    seed_wsl_env_spec(&tmp, "Ubuntu", "ollama", false);

    let engine = Arc::new(Engine::new(tmp.path().to_path_buf()).await.unwrap());

    let wsl_env = EnvId::new("wsl:Ubuntu");
    let result = engine.build_run_snapshot(
        "run-3",
        WorkflowId("wf".to_string()),
        EnvId::local(),
        std::slice::from_ref(&wsl_env),
    );
    let Err(err) = result else {
        panic!("expected OverrideRequired, got Ok");
    };
    let msg = format!("{err}");
    assert!(
        msg.contains("ollama") && msg.contains("override_lower_scope"),
        "OverrideRequired surfaces with the offending id and the override flag hint; got: {msg}",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn env_local_shadow_with_override_succeeds() {
    let tmp = tempfile::TempDir::new().unwrap();
    seed_wsl_env_spec(&tmp, "Ubuntu", "ollama", true);

    let engine = Arc::new(Engine::new(tmp.path().to_path_buf()).await.unwrap());

    let wsl_env = EnvId::new("wsl:Ubuntu");
    let snap = engine
        .build_run_snapshot(
            "run-4",
            WorkflowId("wf".to_string()),
            EnvId::local(),
            std::slice::from_ref(&wsl_env),
        )
        .expect("override_lower_scope: true installs over the builtin");

    let resource = ResourceId("ollama".to_string());
    let (_def, scope) = snap
        .registry
        .resolve(&resource, &wsl_env, None)
        .expect("ollama still resolves; env-local layer wins over builtin");
    assert!(
        matches!(scope, ScopeKey::EnvLocal { id } if id == wsl_env),
        "env-local override should win over the builtin layer",
    );
}
