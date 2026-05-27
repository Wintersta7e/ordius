//! Phase E Task 19: `compose` / `parallel` reject child workflows whose
//! `target_env`s are not part of the parent run's `RunSnapshot` scope, and
//! reject children that declare their own `resources:` block.
//!
//! Child workflows inherit the parent's frozen dispatchers + catalogs;
//! envs the parent never referenced cannot be reached without rebuilding
//! the snapshot mid-run (deferred). Phase E rejects at child-load with a
//! `NodeError::Config` carrying the engine's `ChildEnvNotInScope` /
//! `ChildResourcesUnsupported` message.

use ordius_engine::Engine;
use ordius_engine::types::{Node, Pos, Trigger, Workflow};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

/// Pre-seed `env_specs` with a (probe-stub) WSL distro spec keyed
/// `wsl:<distro>`. The dispatcher won't actually probe anything in tests
/// — what matters is that the engine's `env_registry` contains the env id
/// so `build_run_snapshot` can include it when a parent workflow references
/// it via `target_env`.
fn seed_wsl_env_spec(tmp: &TempDir, distro: &str) {
    let db_path = tmp.path().join("runs.db");
    let pool = ordius_engine::db::open(&db_path).unwrap();
    let conn = pool.get().unwrap();
    let spec_json = serde_json::json!({
        "type": "wsl_distro",
        "name": distro,
        "resources": [],
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

fn delay_node(id: &str, ms: u64) -> Node {
    Node {
        id: id.into(),
        ty: "delay".into(),
        name: String::new(),
        config: HashMap::from([("ms".into(), serde_json::json!(ms))]),
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
        target_env: None,
    }
}

fn delay_node_with_target_env(id: &str, ms: u64, target_env: &str) -> Node {
    Node {
        id: id.into(),
        ty: "delay".into(),
        name: String::new(),
        config: HashMap::from([("ms".into(), serde_json::json!(ms))]),
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
        target_env: Some(ordius_engine::types::EnvId::new(target_env)),
    }
}

fn compose_node(id: &str, child_workflow_id: &str) -> Node {
    Node {
        id: id.into(),
        ty: "compose".into(),
        name: String::new(),
        config: HashMap::from([("workflow_id".into(), serde_json::json!(child_workflow_id))]),
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
        target_env: None,
    }
}

fn workflow(id: &str, nodes: Vec<Node>) -> Workflow {
    Workflow {
        id: id.into(),
        name: id.into(),
        schema_version: 1,
        created_at: None,
        updated_at: None,
        variables: HashMap::new(),
        triggers: vec![Trigger::Manual],
        nodes,
        edges: vec![],
        resources: vec![],
        default_env: None,
    }
}

/// Drain `node_runs.error` rows so we can assert the rejection reason
/// landed on the compose node.
fn last_node_error_messages(engine: &Engine) -> Vec<String> {
    let conn = engine.pool().get().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT error FROM node_runs
             WHERE error IS NOT NULL
             ORDER BY started_at",
        )
        .unwrap();
    stmt.query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(std::result::Result::unwrap)
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn compose_child_with_target_env_outside_parent_scope_fails() {
    // Engine has `wsl:Fake` registered, but the parent workflow never
    // references it, so the parent's snapshot only contains `local`. A
    // child node with `target_env: wsl:Fake` must fail BL-H at child-load.
    let tmp = TempDir::new().unwrap();
    seed_wsl_env_spec(&tmp, "Fake");
    let engine = Arc::new(Engine::new(tmp.path().to_path_buf()).await.unwrap());

    let child = workflow(
        "child",
        vec![delay_node_with_target_env("return", 5, "wsl:Fake")],
    );
    ordius_engine::workflows::save(engine.home(), &child).unwrap();

    let parent = workflow("parent", vec![compose_node("invoke", "child")]);
    let summary = engine
        .run_workflow(Arc::new(parent), HashMap::new(), "test", false, None)
        .await
        .expect("parent run returns summary even on child env mismatch");
    assert_eq!(summary.status, "error", "compose must fail BL-H rejection");

    let messages = last_node_error_messages(&engine);
    let joined = messages.join("\n");
    assert!(
        joined.contains("wsl:Fake"),
        "expected ChildEnvNotInScope error mentioning wsl:Fake, got: {joined}",
    );
    assert!(
        joined.contains("not in the parent run's scope"),
        "expected human-readable BL-H hint, got: {joined}",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn compose_child_with_resources_block_fails() {
    // Child carries its own `resources:` block — unsupported in Phase E.
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(tmp.path().to_path_buf()).await.unwrap());

    let mut child = workflow("child", vec![delay_node("return", 5)]);
    child.resources.push(
        serde_json::from_value(serde_json::json!({
            "id": "child-private-llm",
            "kind": "http_endpoint",
            "advertised_capabilities": [],
            "probe": {
                "kind": "http",
                "ports": [11434],
                "routes": [],
                "timeout_ms": null
            },
            "override_lower_scope": false
        }))
        .unwrap(),
    );
    ordius_engine::workflows::save(engine.home(), &child).unwrap();

    let parent = workflow("parent", vec![compose_node("invoke", "child")]);
    let summary = engine
        .run_workflow(Arc::new(parent), HashMap::new(), "test", false, None)
        .await
        .expect("parent run returns summary even on child resources rejection");
    assert_eq!(
        summary.status, "error",
        "compose must fail ChildResourcesUnsupported",
    );

    let joined = last_node_error_messages(&engine).join("\n");
    assert!(
        joined.contains("1 resource(s)"),
        "expected count in error, got: {joined}",
    );
    assert!(
        joined.contains("not supported in Phase E"),
        "expected ChildResourcesUnsupported hint, got: {joined}",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn compose_child_with_env_in_parent_scope_runs() {
    // Parent pulls `wsl:Fake` into scope via its own (non-running) node;
    // the child can then reference it. Because the engine has only a
    // stubbed env spec (no real WSL probe), we use `delay` nodes — they
    // ignore `target_env` at dispatch — to keep the run executable. The
    // BL-H check passes because the env id is present in the parent's
    // snapshot.
    let tmp = TempDir::new().unwrap();
    seed_wsl_env_spec(&tmp, "Fake");
    let engine = Arc::new(Engine::new(tmp.path().to_path_buf()).await.unwrap());

    let child = workflow(
        "child",
        vec![delay_node_with_target_env("return", 5, "wsl:Fake")],
    );
    ordius_engine::workflows::save(engine.home(), &child).unwrap();

    // Parent has a placeholder node with `target_env: wsl:Fake` to pull it
    // into scope. `compose-invoke` then runs the child.
    let parent = workflow(
        "parent",
        vec![
            delay_node_with_target_env("pull-env", 1, "wsl:Fake"),
            compose_node("invoke", "child"),
        ],
    );
    let summary = engine
        .run_workflow(Arc::new(parent), HashMap::new(), "test", false, None)
        .await
        .expect("parent run completes");
    assert_eq!(
        summary.status, "done",
        "child env present in parent snapshot should run",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn compose_child_with_origin_host_requires_local_in_parent() {
    // Child has an `http` node with `origin: host`. `envs_in_scope()`
    // adds `EnvId::local()` for any origin:host http/llm node. The parent
    // snapshot ALWAYS includes `local` as `default_env`, so this passes
    // BL-H. We assert the check does NOT spuriously reject when only the
    // synthetic-local-from-origin contribution is at play.
    //
    // The http node uses a literal localhost URL that will fail at
    // network time — we only care that BL-H validation lets the child
    // *start*, not that the HTTP call succeeds. Pin status on the
    // node-error rows so we know we got past the compose pre-check.
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(tmp.path().to_path_buf()).await.unwrap());

    let child = workflow(
        "child",
        vec![Node {
            id: "return".into(),
            ty: "http".into(),
            name: String::new(),
            config: HashMap::from([
                (
                    "url".into(),
                    serde_json::json!("http://127.0.0.1:1/never-listens"),
                ),
                ("origin".into(), serde_json::json!("host")),
                ("timeout_ms".into(), serde_json::json!(50_u64)),
            ]),
            pos: Pos::default(),
            timeout_ms: Some(200),
            retry: None,
            continue_on_error: false,
            target_env: None,
        }],
    );
    ordius_engine::workflows::save(engine.home(), &child).unwrap();

    let parent = workflow("parent", vec![compose_node("invoke", "child")]);
    let summary = engine
        .run_workflow(Arc::new(parent), HashMap::new(), "test", false, None)
        .await
        .expect("parent run returns summary");

    // The http call to a dead port will fail, so status is "error" — but
    // the failure must NOT be the BL-H rejection. The child must have
    // gotten past child-load.
    let joined = last_node_error_messages(&engine).join("\n");
    assert!(
        !joined.contains("not in the parent run's scope"),
        "BL-H must not reject when only the origin:host synthetic-local \
         contribution drives envs_in_scope; got: {joined}",
    );
    // Some failure surfaced (the http call) — confirm we reached the
    // child run by way of the compose node, not the BL-H short circuit.
    assert_eq!(
        summary.status, "error",
        "the literal-URL http call should fail, but only at dispatch time",
    );
}
