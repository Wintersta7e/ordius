//! Verifies `Engine::new` picks up manifest files dropped into
//! `<home>/node-types/` and registers them alongside the v1.0
//! built-ins. Spec: `docs/03-node-types.md` "Engine startup".

use ordius_engine::Engine;
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread")]
async fn engine_picks_up_manifest_from_home() {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("node-types")).unwrap();
    std::fs::write(
        dir.path().join("node-types/echoer.yaml"),
        r#"
id: echoer
name: Echoer
category: data
inputs:
  - { name: text, type: string, required: true }
outputs:
  - { name: text, type: string }
config: []
execution:
  backend: subprocess
  command: [sh, -c, 'printf %s "$1"', '--', '{{inputs.text}}']
  output_parse: text
"#,
    )
    .unwrap();
    let engine = Engine::new(dir.path().to_path_buf()).await.unwrap();
    let registry = engine.registry();
    assert!(
        registry.get("echoer").is_some(),
        "manifest 'echoer' should be in the registry alongside the built-ins",
    );
    // Built-ins still present.
    assert!(registry.get("delay").is_some());
    assert!(registry.get("shell").is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn engine_starts_when_node_types_dir_absent() {
    let dir = TempDir::new().unwrap();
    // No node-types/ dir at all.
    let engine = Engine::new(dir.path().to_path_buf()).await.unwrap();
    // v1.0 (8) + v1.1 in-progress (kv at minimum).
    assert!(
        engine.registry().ids().len() >= 8,
        "expected the v1.0+ built-ins to be registered with no manifests",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn manifest_cannot_override_a_builtin_id() {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("node-types")).unwrap();
    // Use id "shell" — duplicate of the built-in.
    std::fs::write(
        dir.path().join("node-types/shell.yaml"),
        r"
id: shell
name: Hostile
category: data
execution:
  backend: subprocess
  command: [echo, hijack]
  output_parse: text
",
    )
    .unwrap();
    let engine = Engine::new(dir.path().to_path_buf()).await.unwrap();
    let shell = engine.registry().get("shell").unwrap();
    assert_eq!(
        shell.name, "Shell",
        "the built-in must win over a duplicate-id manifest",
    );
}
