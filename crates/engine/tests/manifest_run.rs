//! End-to-end: a workflow node typed against a YAML manifest runs
//! through `Engine::run_workflow` and lands its output in
//! `node_outputs`. The substitution engine resolves the manifest's
//! `{{config.input}}` template against the node's config map before
//! the subprocess runs.

use ordius_engine::Engine;
use ordius_engine::types::{Node, Pos, Workflow};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn manifest_node_runs_in_workflow() {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("node-types")).unwrap();

    // Fake "summariser" that just wraps printf so the test stays
    // self-contained (no real LLM / external tool dependency). The
    // manifest declares one config field (`input`) which the
    // command interpolates as `$1` after a `--` separator — the
    // standard argv-with-positional-args pattern for subprocess
    // manifests.
    std::fs::write(
        dir.path().join("node-types/summariser.yaml"),
        r#"
id: summariser
name: Summariser
category: integration
inputs: []
outputs:
  - { name: text, type: string }
config:
  - { name: input, label: Input, type: string, required: true }
execution:
  backend: subprocess
  command: [sh, -c, 'printf "%s [summarised]" "$1"', '--', '{{config.input}}']
  output_parse: text
"#,
    )
    .unwrap();

    let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());
    assert!(
        engine.registry().get("summariser").is_some(),
        "Engine::new should have registered the manifest",
    );

    let wf = Arc::new(Workflow {
        id: "test".into(),
        name: "test".into(),
        schema_version: 1,
        created_at: None,
        updated_at: None,
        variables: HashMap::new(),
        triggers: vec![],
        nodes: vec![Node {
            id: "n".into(),
            ty: "summariser".into(),
            name: "s".into(),
            config: HashMap::from([("input".into(), serde_json::json!("hi"))]),
            pos: Pos::default(),
            timeout_ms: None,
            retry: None,
            continue_on_error: false,
        }],
        edges: vec![],
    });

    let summary = engine
        .run_workflow(wf, HashMap::new(), "test", true, None)
        .await
        .expect("run completes");
    assert_eq!(
        summary.status, "done",
        "manifest-driven run should finish ok"
    );

    let val: String = engine
        .pool()
        .get()
        .unwrap()
        .query_row(
            "SELECT value_inline FROM node_outputs \
             WHERE run_id=? AND node_id='n' AND port_name='text'",
            rusqlite::params![&summary.run_id],
            |r| r.get(0),
        )
        .unwrap();
    // value_inline stores the JSON-serialised PortValue. For a
    // String port, that's a JSON string literal with surrounding
    // quotes plus the manifest's substituted output.
    assert!(
        val.contains("hi [summarised]"),
        "expected printf output through the manifest, got: {val:?}",
    );
}
