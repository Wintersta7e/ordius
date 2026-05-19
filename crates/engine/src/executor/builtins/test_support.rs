//! Test-only fixtures shared by built-in test modules.

use crate::db::open;
use crate::emitter::Emitter;
use crate::executor::RunContext;
use crate::recorder::RunRecorder;
use crate::types::{Category, ExecutionBackend, ExecutionSpec, NodeType, OutputParse, Workflow};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

/// Build a self-contained `RunContext` backed by a fresh
/// `SQLite` database in a temporary directory. The returned
/// `TempDir` must be kept alive for the duration of the test
/// — drop it and the database file disappears.
///
/// The broadcast receiver from `Emitter::new` is discarded:
/// `Emitter::emit` ignores send failures, so executors are
/// happy emitting against an empty subscriber set.
pub(super) fn make_ctx() -> (RunContext, TempDir) {
    let dir = TempDir::new().unwrap();
    let pool = open(dir.path().join("t.db")).unwrap();
    let wf = Workflow {
        id: "w".into(),
        name: String::new(),
        schema_version: 1,
        created_at: None,
        updated_at: None,
        variables: HashMap::new(),
        triggers: vec![],
        nodes: vec![],
        edges: vec![],
    };
    let rec = Arc::new(RunRecorder::start(pool, &wf, "{}", &HashMap::new(), "test").unwrap());
    let (em, _) = Emitter::new(rec.clone());
    let ctx = RunContext {
        run_id: rec.run_id.clone(),
        workflow_id: "w".into(),
        workflow_name: String::new(),
        started_at_iso: String::new(),
        workspace: dir.path().to_path_buf(),
        variables: HashMap::new(),
        recorder: rec,
        emitter: Arc::new(em),
        secrets_store: None,
        current_inputs: HashMap::new(),
        upstream_outputs: HashMap::new(),
    };
    (ctx, dir)
}

/// Minimal in-process `NodeType` used by built-in tests. All
/// optional spec fields are empty — tests that need wired ports
/// or config schemas should build their own.
pub(super) fn dummy_node_type(id: &str, category: Category) -> NodeType {
    NodeType {
        id: id.into(),
        name: String::new(),
        category,
        tags: vec![],
        icon: String::new(),
        description: String::new(),
        inputs: vec![],
        outputs: vec![],
        config: vec![],
        execution: ExecutionSpec {
            backend: ExecutionBackend::InProcess,
            command: vec![],
            stdin_template: None,
            env: HashMap::new(),
            timeout_ms: None,
            output_parse: OutputParse::Text,
            output_map: HashMap::new(),
        },
    }
}
