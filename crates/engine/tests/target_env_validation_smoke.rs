//! Phase E Task 8 smoke: workflow load rejects `target_env` ids that
//! are not registered with the engine, and distinguishes "disabled" from
//! "unknown" so the IPC layer can render "Re-enable in Settings" guidance.
//!
//! Local (`EnvId::local()`) is always treated as valid — the boot probe
//! synthesises it even when `env_specs` is empty, so workflows that
//! omit `target_env` or set it to `local` continue to load.

use ordius_engine::Engine;
use ordius_engine::workflows::WorkflowsError;
use tempfile::TempDir;

fn write_workflow(home: &TempDir, id: &str, body: &str) {
    let dir = home.path().join("workflows");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(format!("{id}.json")), body).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn workflow_with_unknown_target_env_rejected_at_load() {
    let tmp = TempDir::new().unwrap();
    let engine = Engine::new(tmp.path().to_path_buf()).await.unwrap();

    write_workflow(
        &tmp,
        "bad",
        r#"{
            "id": "bad",
            "name": "bad",
            "schema_version": 1,
            "nodes": [{
                "id": "n1",
                "type": "http",
                "name": "n1",
                "target_env": "ssh:not-set-up",
                "config": {"url": "https://example.com"}
            }],
            "edges": []
        }"#,
    );

    let err = engine.load_workflow_for_run(tmp.path(), "bad").unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("ssh:not-set-up"), "{msg}");
    assert!(msg.contains("target_env"), "{msg}");
}

#[tokio::test(flavor = "multi_thread")]
async fn workflow_with_disabled_target_env_surfaces_distinct_error() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("runs.db");

    // Pre-seed a disabled env spec so the boot probe stages it into
    // `env_disabled_specs` instead of the active registry. Migrations
    // (and the env_specs DDL) run on the first `db::open` here.
    {
        let pool = ordius_engine::db::open(&db_path).unwrap();
        let conn = pool.get().unwrap();
        conn.execute(
            "INSERT INTO env_specs (id, label, enabled, spec_json, created_at, updated_at)
             VALUES ('wsl:Disabled', 'WSL: Disabled', 0,
                     '{\"type\":\"wsl_distro\",\"name\":\"Disabled\",\"resources\":[],\"host_direct_verifications\":{}}',
                     0, 0)",
            [],
        )
        .unwrap();
    }

    let engine = Engine::new(tmp.path().to_path_buf()).await.unwrap();

    write_workflow(
        &tmp,
        "uses-disabled",
        r#"{
            "id": "uses-disabled",
            "name": "uses-disabled",
            "schema_version": 1,
            "nodes": [{
                "id": "n1",
                "type": "http",
                "name": "n1",
                "target_env": "wsl:Disabled",
                "config": {"url": "https://example.com"}
            }],
            "edges": []
        }"#,
    );

    let err = engine
        .load_workflow_for_run(tmp.path(), "uses-disabled")
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        matches!(
            err,
            ordius_engine::EngineError::Workflows(WorkflowsError::TargetEnvDisabled { .. })
        ),
        "expected TargetEnvDisabled, got {err:?}",
    );
    assert!(msg.contains("wsl:Disabled"), "{msg}");
    assert!(msg.contains("Re-enable"), "{msg}");
}

#[tokio::test(flavor = "multi_thread")]
async fn workflow_with_target_env_local_loads_successfully() {
    let tmp = TempDir::new().unwrap();
    let engine = Engine::new(tmp.path().to_path_buf()).await.unwrap();

    write_workflow(
        &tmp,
        "uses-local",
        r#"{
            "id": "uses-local",
            "name": "uses-local",
            "schema_version": 1,
            "nodes": [{
                "id": "n1",
                "type": "http",
                "name": "n1",
                "target_env": "local",
                "config": {"url": "https://example.com"}
            }],
            "edges": []
        }"#,
    );

    let (_wf, _warnings) = engine
        .load_workflow_for_run(tmp.path(), "uses-local")
        .expect("workflow with target_env: local should load");
}

#[tokio::test(flavor = "multi_thread")]
async fn workflow_default_env_unknown_rejected_at_load() {
    let tmp = TempDir::new().unwrap();
    let engine = Engine::new(tmp.path().to_path_buf()).await.unwrap();

    write_workflow(
        &tmp,
        "bad-default",
        r#"{
            "id": "bad-default",
            "name": "bad-default",
            "schema_version": 1,
            "default_env": "ssh:nowhere",
            "nodes": [{
                "id": "n1",
                "type": "http",
                "name": "n1",
                "config": {"url": "https://example.com"}
            }],
            "edges": []
        }"#,
    );

    let err = engine
        .load_workflow_for_run(tmp.path(), "bad-default")
        .unwrap_err();
    assert!(
        matches!(
            err,
            ordius_engine::EngineError::Workflows(WorkflowsError::DefaultEnvUnknown { .. })
        ),
        "expected DefaultEnvUnknown, got {err:?}",
    );
    assert!(format!("{err}").contains("ssh:nowhere"));
}
