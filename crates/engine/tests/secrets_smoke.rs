//! End-to-end: `{{secrets.X}}` resolves at execution time and the
//! resolved value is redacted in `node:output` events before they
//! reach `SQLite`.

use ordius_engine::checkpoints::CheckpointRegistry;
use ordius_engine::db::open;
use ordius_engine::emitter::Emitter;
use ordius_engine::events::EventType;
use ordius_engine::executor::{InProcessExecutor, NodeExecutor, RunContext, wrap_process_env};
use ordius_engine::recorder::RunRecorder;
use ordius_engine::registry::Registry;
use ordius_engine::secrets::Store;
use ordius_engine::types::{Node, PortValue, Pos, Workflow};
use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn template_resolves_secret_and_redacts_in_event_log() {
    keyring::use_sample_store(&HashMap::from([("persist", "false")])).unwrap();

    let dir = tempfile::TempDir::new().unwrap();
    let pool = open(dir.path().join("t.db")).unwrap();
    let secrets = Arc::new(Store::with_index_path(
        dir.path().join("secrets-index.json"),
    ));
    secrets.set("MY_KEY", "supersecret").unwrap();

    let node = Node {
        id: "render".into(),
        ty: "transform".into(),
        name: "render".into(),
        config: HashMap::from([
            ("op".into(), serde_json::json!("template")),
            (
                "template".into(),
                serde_json::json!("key={{secrets.MY_KEY}}"),
            ),
        ]),
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
    };
    let wf = Workflow {
        id: "secrets-demo".into(),
        name: "Secrets demo".into(),
        schema_version: 1,
        created_at: None,
        updated_at: None,
        variables: HashMap::new(),
        triggers: vec![],
        nodes: vec![node.clone()],
        edges: vec![],
    };

    let rec =
        Arc::new(RunRecorder::start(pool.clone(), &wf, "{}", &HashMap::new(), "test").unwrap());
    let (em, _rx) = Emitter::new(rec.clone());
    let em = Arc::new(em);
    let ctx = RunContext {
        run_id: rec.run_id.clone(),
        workflow_id: wf.id.clone(),
        workflow_name: wf.name.clone(),
        started_at_iso: String::new(),
        workspace: dir.path().to_path_buf(),
        variables: HashMap::new(),
        recorder: rec.clone(),
        emitter: em.clone(),
        secrets_store: Some(secrets.clone()),
        env: wrap_process_env(),
        current_inputs: HashMap::new(),
        upstream_outputs: HashMap::new(),
        checkpoints: Arc::new(CheckpointRegistry::new()),
        iteration: 1,
        attempt: std::sync::atomic::AtomicU32::new(1),
    };

    let executor = InProcessExecutor::new();
    let registry = Registry::with_v1_0_builtins();
    let nt = registry.get(&node.ty).expect("transform is registered");

    let outs = executor
        .run(&node, &nt, &ctx, CancellationToken::new())
        .await
        .unwrap();
    let rendered = match outs.get("text").expect("text output") {
        PortValue::String(s) => s.clone(),
        other => panic!("expected String, got {other:?}"),
    };

    // The rendered string itself carries the raw secret — that's
    // what makes the redaction step necessary.
    assert!(
        rendered.contains("supersecret"),
        "renderer should produce the raw value; got {rendered:?}",
    );

    em.emit_node(
        EventType::NodeOutput,
        node.id.clone(),
        1,
        1,
        HashMap::from([("text".into(), serde_json::json!(rendered))]),
    );
    rec.finalize("done", None).unwrap();

    let conn = pool.get().unwrap();
    let payload_json: String = conn
        .query_row(
            "SELECT payload_json FROM run_events WHERE run_id=? AND type='node:output'",
            [&rec.run_id],
            |r| r.get(0),
        )
        .unwrap();
    assert!(
        payload_json.contains("<redacted:MY_KEY>"),
        "expected redaction marker in persisted payload: {payload_json}",
    );
    assert!(
        !payload_json.contains("supersecret"),
        "expected raw secret to be absent from persisted payload: {payload_json}",
    );
}
