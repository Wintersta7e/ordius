//! End-to-end smoke test for the recorder + emitter stack.
//!
//! Open a fresh database, start a run, emit a handful of events,
//! finalise, and assert the SQL contents match expectations. No
//! executor involvement — this exists to verify the persistence
//! layer in isolation before Phase 4 wires up node execution.

use ordius_engine::db::open;
use ordius_engine::emitter::Emitter;
use ordius_engine::events::EventType;
use ordius_engine::recorder::RunRecorder;
use ordius_engine::types::Workflow;
use std::collections::HashMap;
use std::sync::Arc;

fn empty_demo_workflow() -> Workflow {
    Workflow {
        id: "demo".into(),
        name: "Demo".into(),
        schema_version: 1,
        created_at: None,
        updated_at: None,
        variables: HashMap::new(),
        triggers: vec![],
        nodes: vec![],
        edges: vec![],
        resources: vec![],
        default_env: None,
    }
}

#[test]
fn end_to_end_minimal_run_persists() {
    let f = tempfile::NamedTempFile::new().unwrap();
    let pool = open(f.path()).unwrap();
    let wf = empty_demo_workflow();
    let rec =
        Arc::new(RunRecorder::start(pool.clone(), &wf, "{}", &HashMap::new(), "cli").unwrap());
    let (em, _rx) = Emitter::new(rec.clone());

    em.emit_workflow(EventType::WorkflowStarted, HashMap::new());
    em.emit_node(EventType::NodeStarted, "n1", 1, 1, HashMap::new());
    em.emit_node(EventType::NodeDone, "n1", 1, 1, HashMap::new());

    rec.finalize("done", None).unwrap();

    let conn = pool.get().unwrap();
    let event_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM run_events WHERE run_id=?",
            [&rec.run_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(event_count, 3);

    let status: String = conn
        .query_row("SELECT status FROM runs WHERE id=?", [&rec.run_id], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(status, "done");

    // seq counter should have advanced once per emit, starting from 0.
    let max_seq: i64 = conn
        .query_row(
            "SELECT MAX(seq) FROM run_events WHERE run_id=?",
            [&rec.run_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(max_seq, 2);

    // Sanity-check the wire tag ordering matches emit order.
    let tags: Vec<String> = {
        let mut stmt = conn
            .prepare("SELECT type FROM run_events WHERE run_id=? ORDER BY seq")
            .unwrap();
        stmt.query_map([&rec.run_id], |r| r.get::<_, String>(0))
            .unwrap()
            .filter_map(Result::ok)
            .collect()
    };
    assert_eq!(
        tags,
        vec![
            "workflow:started".to_string(),
            "node:started".to_string(),
            "node:done".to_string(),
        ],
    );
}
