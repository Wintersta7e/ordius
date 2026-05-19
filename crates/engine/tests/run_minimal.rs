//! First end-to-end executable workflow.
//!
//! Two `delay` nodes chained by one forward edge. Run via the
//! scheduler + `InProcessExecutor` + recorder stack and assert
//! `SQLite` ends up with two `node_runs` rows and `runs.status` = `done`.

use ordius_engine::checkpoints::CheckpointRegistry;
use ordius_engine::db::open;
use ordius_engine::emitter::Emitter;
use ordius_engine::events::EventType;
use ordius_engine::executor::{InProcessExecutor, NodeExecutor, RunContext, wrap_process_env};
use ordius_engine::recorder::{NodeRunRow, RunRecorder};
use ordius_engine::registry::Registry;
use ordius_engine::scheduler::Scheduler;
use ordius_engine::types::{Edge, EdgeType, Node, Pos, Workflow};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;

fn now_ms() -> i64 {
    let dur = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    i64::try_from(dur.as_millis()).unwrap()
}

fn delay_node(id: &str, ms: u64) -> Node {
    Node {
        id: id.into(),
        ty: "delay".into(),
        name: id.into(),
        config: HashMap::from([("ms".into(), serde_json::json!(ms))]),
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
    }
}

fn forward_edge(id: &str, from: &str, to: &str) -> Edge {
    Edge {
        id: id.into(),
        from_node_id: from.into(),
        from_port: "x".into(),
        to_node_id: to.into(),
        to_port: "y".into(),
        kind: EdgeType::Forward,
        max_iterations: None,
        branch: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_delay_nodes_run_end_to_end() {
    let dir = tempfile::TempDir::new().unwrap();
    let pool = open(dir.path().join("t.db")).unwrap();

    let wf = Workflow {
        id: "two-delays".into(),
        name: "Two delays".into(),
        schema_version: 1,
        created_at: None,
        updated_at: None,
        variables: HashMap::new(),
        triggers: vec![],
        nodes: vec![delay_node("a", 20), delay_node("b", 20)],
        edges: vec![forward_edge("e", "a", "b")],
    };

    let registry = Registry::with_v1_0_builtins();
    let rec =
        Arc::new(RunRecorder::start(pool.clone(), &wf, "{}", &HashMap::new(), "test").unwrap());
    let (em, _rx) = Emitter::new(rec.clone());
    let em = Arc::new(em);
    em.emit_workflow(EventType::WorkflowStarted, HashMap::new());

    let ctx = RunContext {
        run_id: rec.run_id.clone(),
        workflow_id: wf.id.clone(),
        workflow_name: wf.name.clone(),
        started_at_iso: String::new(),
        workspace: dir.path().to_path_buf(),
        variables: HashMap::new(),
        recorder: rec.clone(),
        emitter: em.clone(),
        secrets_store: None,
        env: wrap_process_env(),
        current_inputs: HashMap::new(),
        upstream_outputs: HashMap::new(),
        checkpoints: Arc::new(CheckpointRegistry::new()),
        iteration: 1,
        attempt: std::sync::atomic::AtomicU32::new(1),
        auto_resume: false,
    };
    let executor = InProcessExecutor::new();
    let mut sched = Scheduler::new(&wf);

    while !sched.is_done() {
        let ready = sched.ready();
        if ready.is_empty() {
            break;
        }
        for n in ready {
            let nt = registry.get(&n.ty).expect("registered");
            sched.start_node(&n.id);
            em.emit_node(EventType::NodeStarted, n.id.clone(), 1, 1, HashMap::new());
            let started = now_ms();
            let res = executor.run(n, &nt, &ctx, CancellationToken::new()).await;
            let finished = now_ms();
            match res {
                Ok(_) => {
                    rec.record_node_run(&NodeRunRow {
                        node_id: &n.id,
                        iteration: 1,
                        attempt: 1,
                        node_type: &n.ty,
                        status: "done",
                        started_at: Some(started),
                        finished_at: Some(finished),
                        duration_ms: Some(finished - started),
                        output_summary: None,
                        error: None,
                    })
                    .unwrap();
                    em.emit_node(EventType::NodeDone, n.id.clone(), 1, 1, HashMap::new());
                    sched.complete_node(&n.id);
                },
                Err(e) => {
                    em.emit_node(
                        EventType::NodeError,
                        n.id.clone(),
                        1,
                        1,
                        HashMap::from([("error".into(), serde_json::json!(e.to_string()))]),
                    );
                    sched.fail_node(&n.id);
                },
            }
        }
    }
    em.emit_workflow(EventType::WorkflowDone, HashMap::new());
    rec.finalize("done", None).unwrap();

    let conn = pool.get().unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM node_runs WHERE run_id=?",
            [&rec.run_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 2);
    let status: String = conn
        .query_row("SELECT status FROM runs WHERE id=?", [&rec.run_id], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(status, "done");
}
