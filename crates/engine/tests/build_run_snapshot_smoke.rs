//! Smoke tests for `Engine::build_run_snapshot` + `Workflow::envs_in_scope`.
//!
//! Validates the per-run freeze contract: dispatcher + catalog + spec
//! presence for every env in scope, and the `EnvUnknown` rejection path.

use std::collections::HashMap;
use std::sync::Arc;

use ordius_engine::Engine;
use ordius_engine::environment::runtime::{EnvId, WorkflowId};
use ordius_engine::error::EngineError;
use ordius_engine::types::{Edge, Node, Pos, Workflow};
use tempfile::TempDir;

fn make_node(id: &str, ty: &str) -> Node {
    Node {
        id: id.to_string(),
        ty: ty.to_string(),
        name: id.to_string(),
        config: HashMap::new(),
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
        target_env: None,
    }
}

fn make_workflow(id: &str, nodes: Vec<Node>) -> Workflow {
    Workflow {
        id: id.to_string(),
        name: id.to_string(),
        schema_version: 1,
        created_at: None,
        updated_at: None,
        variables: HashMap::new(),
        triggers: Vec::new(),
        nodes,
        edges: Vec::<Edge>::new(),
        resources: Vec::new(),
        default_env: None,
    }
}

#[test]
fn envs_in_scope_collects_default_target_and_host_origin() {
    let mut node_targeted = make_node("a", "shell");
    node_targeted.target_env = Some(EnvId::new("wsl:Ubuntu"));

    let mut node_host = make_node("b", "http");
    node_host
        .config
        .insert("origin".to_string(), serde_json::json!("host"));

    let mut node_plain = make_node("c", "llm");
    node_plain
        .config
        .insert("origin".to_string(), serde_json::json!("target"));

    let mut wf = make_workflow("wf", vec![node_targeted, node_host, node_plain]);
    wf.default_env = Some(EnvId::new("ssh:dev"));

    let scope = wf.envs_in_scope();
    // Sorted by string: local < ssh:dev < wsl:Ubuntu.
    assert_eq!(
        scope,
        vec![
            EnvId::local(),
            EnvId::new("ssh:dev"),
            EnvId::new("wsl:Ubuntu"),
        ],
        "scope must include default_env, every target_env, and local (because of origin=host)",
    );
}

#[test]
fn envs_in_scope_origin_host_only_for_http_and_llm() {
    let mut node_shell = make_node("a", "shell");
    node_shell
        .config
        .insert("origin".to_string(), serde_json::json!("host"));

    let wf = make_workflow("wf", vec![node_shell]);
    assert!(
        wf.envs_in_scope().is_empty(),
        "origin=host on a non-http/llm node must NOT pull `local` into scope",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn build_run_snapshot_happy_path_local() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(tmp.path().to_path_buf()).await.unwrap());

    // Two nodes: one targets local explicitly, one uses default_env fallback.
    let mut targeted = make_node("a", "shell");
    targeted.target_env = Some(EnvId::local());
    let plain = make_node("b", "shell");

    let mut wf = make_workflow("wf", vec![targeted, plain]);
    wf.default_env = Some(EnvId::local());

    let scope = wf.envs_in_scope();
    assert_eq!(scope, vec![EnvId::local()]);

    let snap = engine
        .build_run_snapshot(
            "run-1",
            WorkflowId("wf".to_string()),
            EnvId::local(),
            &scope,
        )
        .expect("snapshot builds for the seeded local env");

    assert_eq!(snap.run_id, "run-1");
    assert_eq!(snap.default_env, EnvId::local());
    assert!(
        snap.dispatchers.contains_key(&EnvId::local()),
        "Local dispatcher must be in scope",
    );
    assert!(
        snap.catalogs.contains_key(&EnvId::local()),
        "Local catalog must be in scope",
    );
    assert!(
        snap.specs.contains_key(&EnvId::local()),
        "Local spec must be in scope",
    );
    // Default env must always be included even if not in envs_in_scope.
    let snap2 = engine
        .build_run_snapshot("run-2", WorkflowId("wf".to_string()), EnvId::local(), &[])
        .expect("snapshot builds with empty extra scope");
    assert!(snap2.dispatchers.contains_key(&EnvId::local()));
}

#[tokio::test(flavor = "multi_thread")]
async fn build_run_snapshot_unknown_env_errors() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(tmp.path().to_path_buf()).await.unwrap());

    let unknown = EnvId::new("ssh:nope");
    let result = engine.build_run_snapshot(
        "run-x",
        WorkflowId("wf".to_string()),
        EnvId::local(),
        std::slice::from_ref(&unknown),
    );

    match result {
        Err(EngineError::EnvUnknown(id)) => assert_eq!(id, unknown),
        Err(other) => panic!("expected EnvUnknown, got {other:?}"),
        Ok(_) => panic!("expected EnvUnknown, got Ok"),
    }
}
