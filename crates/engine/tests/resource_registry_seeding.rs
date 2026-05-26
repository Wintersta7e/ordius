//! Black-box integration: full boot path seeds the `ResourceRegistry`.
//!
//! Builds an `Engine` against a tempdir that already contains
//! `resources.toml` and a workflow file with a `resources:` block, then
//! verifies each scope is populated and visible to the appropriate
//! `(env, workflow?)` look-up.

use ordius_engine::Engine;
use ordius_engine::environment::runtime::{EnvId, ResourceId, ScopeKey, WorkflowId};
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread")]
async fn engine_new_seeds_builtins_and_user_globals() {
    let dir = TempDir::new().unwrap();
    std::fs::write(
        dir.path().join("resources.toml"),
        r#"
[[resource]]
id = "user-llm"
override_lower_scope = false
kind = "http_endpoint"
advertised_capabilities = ["openai_chat_completions"]

[resource.probe]
kind = "http"
ports = [9000]

[[resource.probe.routes]]
path = "/v1/models"
method = "get"
flavor = "openai_chat"
proves = ["openai_chat_completions"]
fingerprint_jsonpaths = ["$.version"]
"#,
    )
    .unwrap();

    let engine = Engine::new(dir.path().to_path_buf()).await.expect("new");
    let snap = engine.resource_registry().snapshot();

    let builtin = snap.layers.get(&ScopeKey::Builtin).expect("builtin layer");
    assert!(builtin.contains_key(&ResourceId("ollama".into())));
    assert!(builtin.contains_key(&ResourceId("claude-code".into())));
    assert!(builtin.contains_key(&ResourceId("rust".into())));

    let user = snap
        .layers
        .get(&ScopeKey::UserGlobal)
        .expect("user-global layer");
    assert!(user.contains_key(&ResourceId("user-llm".into())));

    let (_, scope) = snap
        .resolve(&ResourceId("user-llm".into()), &EnvId::local(), None)
        .expect("user-llm resolves");
    assert_eq!(scope, ScopeKey::UserGlobal);
}

#[tokio::test(flavor = "multi_thread")]
async fn workflow_load_installs_scope() {
    let dir = TempDir::new().unwrap();
    let wf_dir = dir.path().join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(
        wf_dir.join("with-res.json"),
        r#"{
            "id": "with-res",
            "name": "with resources",
            "nodes": [],
            "edges": [],
            "resources": [
                {
                    "id": "wf-private",
                    "kind": "http_endpoint",
                    "advertised_capabilities": ["openai_chat_completions"],
                    "probe": {
                        "kind": "http",
                        "ports": [8765],
                        "routes": []
                    },
                    "override_lower_scope": false
                }
            ]
        }"#,
    )
    .unwrap();

    let engine = Engine::new(dir.path().to_path_buf()).await.expect("new");
    let registry = engine.resource_registry();
    let (wf, _warnings) =
        ordius_engine::workflows::load_in_registry(dir.path(), "with-res", &registry)
            .expect("load_in_registry");
    assert_eq!(wf.resources.len(), 1);

    let snap = registry.snapshot();
    let wf_id = WorkflowId("with-res".into());
    let layer = snap
        .layers
        .get(&ScopeKey::Workflow { id: wf_id.clone() })
        .expect("wf scope");
    assert!(layer.contains_key(&ResourceId("wf-private".into())));

    // Visible to this workflow on the local env...
    let (_def, scope) = snap
        .resolve(
            &ResourceId("wf-private".into()),
            &EnvId::local(),
            Some(&wf_id),
        )
        .expect("visible to with-res");
    assert!(matches!(scope, ScopeKey::Workflow { .. }));

    // ...but invisible to a different workflow id.
    let other = WorkflowId("other-wf".into());
    assert!(
        snap.resolve(
            &ResourceId("wf-private".into()),
            &EnvId::local(),
            Some(&other),
        )
        .is_none()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn workflow_delete_drops_scope() {
    let dir = TempDir::new().unwrap();
    let wf_dir = dir.path().join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(
        wf_dir.join("doomed.json"),
        r#"{
            "id": "doomed",
            "name": "doomed",
            "nodes": [],
            "edges": [],
            "resources": [
                {
                    "id": "doomed-llm",
                    "kind": "http_endpoint",
                    "advertised_capabilities": [],
                    "probe": { "kind": "http", "ports": [9000], "routes": [] },
                    "override_lower_scope": false
                }
            ]
        }"#,
    )
    .unwrap();

    let engine = Engine::new(dir.path().to_path_buf()).await.expect("new");
    let registry = engine.resource_registry();
    let (_wf, _warnings) =
        ordius_engine::workflows::load_in_registry(dir.path(), "doomed", &registry).expect("load");

    let removed = ordius_engine::workflows::delete_in_registry(dir.path(), "doomed", &registry)
        .expect("delete");
    assert!(removed);

    let snap = registry.snapshot();
    assert!(!snap.layers.contains_key(&ScopeKey::Workflow {
        id: WorkflowId("doomed".into()),
    }));
}
