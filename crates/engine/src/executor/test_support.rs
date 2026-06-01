//! Test-only fixtures shared by every executor test module
//! (`builtins::*` and the subprocess executor). Visibility is
//! `pub(crate)` so any test under `executor/` can pull them in
//! without each child module re-declaring its own copy.

use crate::checkpoints::CheckpointRegistry;
use crate::db::open;
use crate::emitter::Emitter;
use crate::environment::runtime::dispatcher::Dispatcher;
use crate::environment::runtime::env::{EnvInfo, EnvSpec, EnvState};
use crate::environment::runtime::local::LocalDispatcher;
use crate::environment::runtime::{EnvId, ResourceRegistry, RunSnapshot, WorkflowId};
use crate::events::RunEvent;
use crate::executor::{RunContext, wrap_process_env};
use crate::recorder::RunRecorder;
use crate::types::{
    Category, ExecutionBackend, ExecutionSpec, Node, NodeType, OutputParse, Pos, Workflow,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use tempfile::TempDir;
use tokio::sync::broadcast;

/// Build a minimal `Arc<RunSnapshot>` for tests that need a
/// `RunContext` without booting a full `Engine`. The snapshot
/// installs a `LocalDispatcher` for `EnvId::local()` so legacy
/// literal-URL paths can send through the per-env transport;
/// catalogs and specs remain empty. Tests that exercise non-local
/// dispatcher selection should build via a real `Engine` instead.
pub(super) fn test_run_snapshot(run_id: &str, workflow_id: &str) -> Arc<RunSnapshot> {
    let local_env_id = EnvId::local();
    let local_info = EnvInfo {
        id: local_env_id.clone(),
        label: "local".into(),
        spec: EnvSpec::Local {
            resources: Vec::new(),
            host_direct_verifications: HashMap::new(),
        },
        state: EnvState::Reachable,
        enabled: true,
    };
    let local_dispatcher: Arc<dyn Dispatcher> = Arc::new(LocalDispatcher::new(local_info));
    let mut dispatchers: HashMap<EnvId, Arc<dyn Dispatcher>> = HashMap::new();
    dispatchers.insert(local_env_id.clone(), local_dispatcher);
    Arc::new(RunSnapshot {
        run_id: run_id.to_string(),
        workflow_id: WorkflowId(workflow_id.to_string()),
        default_env: local_env_id,
        registry: ResourceRegistry::new().snapshot(),
        dispatchers: Arc::new(dispatchers),
        catalogs: Arc::new(HashMap::new()),
        specs: Arc::new(HashMap::new()),
    })
}

/// Build a self-contained `RunContext` backed by a fresh `SQLite`
/// database in a temporary directory. Returns the broadcast
/// receiver alongside so streaming tests can drain `node:output`
/// events; tests that only need the context discard it with `_rx`.
///
/// The returned `TempDir` must be kept alive for the duration of
/// the test — drop it and the database file disappears.
pub(super) fn make_ctx() -> (RunContext, broadcast::Receiver<RunEvent>, TempDir) {
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
        resources: vec![],
        default_env: None,
    };
    let rec = Arc::new(RunRecorder::start(pool, &wf, "{}", &HashMap::new(), "test").unwrap());
    let (em, rx) = Emitter::new(rec.clone());
    let run_snapshot = test_run_snapshot(&rec.run_id, "w");
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
        env: wrap_process_env(),
        current_inputs: HashMap::new(),
        upstream_outputs: HashMap::new(),
        checkpoints: Arc::new(CheckpointRegistry::new()),
        events: Arc::new(crate::events_registry::EventRegistry::new()),
        run_snapshot,
        engine: std::sync::Weak::new(),
        compose_depth: 0,
        iteration: 1,
        attempt: AtomicU32::new(1),
        auto_resume: false,
        workspace_manager: std::sync::Arc::new(
            crate::environment::runtime::workspace::WorkspaceManager::new(),
        ),
    };
    (ctx, rx, dir)
}

/// Minimal in-process `NodeType` used by the in-process built-in
/// tests. All optional spec fields are empty — tests that need
/// wired ports or config schemas should build their own.
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
        skip_config_templates: false,
    }
}

/// Subprocess-backend `NodeType` with the given argv as the
/// execution command. The id is constant so test assertions can
/// distinguish it from the `shell` built-in's special-cased id.
pub(super) fn subprocess_node_type(command: Vec<String>) -> NodeType {
    NodeType {
        id: "test_subprocess".into(),
        name: String::new(),
        category: Category::Execution,
        tags: vec![],
        icon: String::new(),
        description: String::new(),
        inputs: vec![],
        outputs: vec![],
        config: vec![],
        execution: ExecutionSpec {
            backend: ExecutionBackend::Subprocess,
            command,
            stdin_template: None,
            env: HashMap::new(),
            timeout_ms: None,
            output_parse: OutputParse::Text,
            output_map: HashMap::new(),
        },
        skip_config_templates: false,
    }
}

/// Trivial `Node` referencing the `test_subprocess` type id. Tests
/// mutate `config` / `ty` as needed before passing it to an executor.
pub(super) fn trivial_subprocess_node() -> Node {
    Node {
        id: "n1".into(),
        ty: "test_subprocess".into(),
        name: String::new(),
        config: HashMap::new(),
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
        target_env: None,
    }
}
