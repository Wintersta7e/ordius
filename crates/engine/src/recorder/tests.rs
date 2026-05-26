use super::*;
use crate::db::open;
use crate::events::{EventType, RunEvent};

fn empty_wf() -> Workflow {
    Workflow {
        id: "w1".into(),
        name: "demo".into(),
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
fn start_and_finalize_a_run() {
    let f = tempfile::NamedTempFile::new().unwrap();
    let pool = open(f.path()).unwrap();
    let rec =
        RunRecorder::start(pool.clone(), &empty_wf(), "{}", &HashMap::new(), "manual").unwrap();
    rec.finalize("done", None).unwrap();
    let conn = pool.get().unwrap();
    let (status, finished, duration): (String, Option<i64>, Option<i64>) = conn
        .query_row(
            "SELECT status, finished_at, duration_ms FROM runs WHERE id=?",
            [&rec.run_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(status, "done");
    assert!(finished.is_some());
    assert!(duration.is_some());
}

#[test]
fn start_writes_snapshot_referenced_by_run() {
    let f = tempfile::NamedTempFile::new().unwrap();
    let pool = open(f.path()).unwrap();
    let rec =
        RunRecorder::start(pool.clone(), &empty_wf(), "{}", &HashMap::new(), "manual").unwrap();
    let conn = pool.get().unwrap();
    let snap_id: String = conn
        .query_row(
            "SELECT workflow_snapshot_id FROM runs WHERE id=?",
            [&rec.run_id],
            |r| r.get(0),
        )
        .unwrap();
    let wf_id: String = conn
        .query_row(
            "SELECT workflow_id FROM run_snapshots WHERE id=?",
            [&snap_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(wf_id, "w1");
}

#[test]
fn next_seq_is_monotonic_from_zero() {
    let f = tempfile::NamedTempFile::new().unwrap();
    let pool = open(f.path()).unwrap();
    let rec = RunRecorder::start(pool, &empty_wf(), "{}", &HashMap::new(), "manual").unwrap();
    assert_eq!(rec.next_seq(), 0);
    assert_eq!(rec.next_seq(), 1);
    assert_eq!(rec.next_seq(), 2);
}

#[test]
fn record_node_run_and_output_persists() {
    let f = tempfile::NamedTempFile::new().unwrap();
    let pool = open(f.path()).unwrap();
    let rec =
        RunRecorder::start(pool.clone(), &empty_wf(), "{}", &HashMap::new(), "manual").unwrap();
    rec.record_node_run(&NodeRunRow {
        node_id: "n1",
        iteration: 1,
        attempt: 1,
        node_type: "delay",
        status: "done",
        started_at: Some(0),
        finished_at: Some(50),
        duration_ms: Some(50),
        output_summary: None,
        error: None,
    })
    .unwrap();
    rec.record_node_output("n1", 1, 1, "x", Some(r#"{"v":42}"#), None)
        .unwrap();
    let conn = pool.get().unwrap();
    let (status, dur): (String, i64) = conn
        .query_row(
            "SELECT status, duration_ms FROM node_runs WHERE node_id='n1'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(status, "done");
    assert_eq!(dur, 50);
    let port: String = conn
        .query_row(
            "SELECT port_name FROM node_outputs WHERE node_id='n1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(port, "x");
}

#[test]
fn record_node_run_updates_in_place() {
    let f = tempfile::NamedTempFile::new().unwrap();
    let pool = open(f.path()).unwrap();
    let rec =
        RunRecorder::start(pool.clone(), &empty_wf(), "{}", &HashMap::new(), "manual").unwrap();
    rec.record_node_run(&NodeRunRow {
        node_id: "n1",
        iteration: 1,
        attempt: 1,
        node_type: "delay",
        status: "running",
        started_at: Some(0),
        finished_at: None,
        duration_ms: None,
        output_summary: None,
        error: None,
    })
    .unwrap();
    rec.record_node_run(&NodeRunRow {
        node_id: "n1",
        iteration: 1,
        attempt: 1,
        node_type: "delay",
        status: "done",
        started_at: Some(0),
        finished_at: Some(100),
        duration_ms: Some(100),
        output_summary: Some("ok"),
        error: None,
    })
    .unwrap();
    let conn = pool.get().unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM node_runs WHERE node_id='n1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
    let status: String = conn
        .query_row("SELECT status FROM node_runs WHERE node_id='n1'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(status, "done");
}

#[test]
fn second_lock_acquisition_fails_until_release() {
    let f = tempfile::NamedTempFile::new().unwrap();
    let pool = open(f.path()).unwrap();
    let rec1 =
        RunRecorder::start(pool.clone(), &empty_wf(), "{}", &HashMap::new(), "manual").unwrap();
    assert!(rec1.try_acquire_lock().unwrap());
    let rec2 = RunRecorder::start(pool, &empty_wf(), "{}", &HashMap::new(), "manual").unwrap();
    assert!(!rec2.try_acquire_lock().unwrap());
    rec1.release_lock().unwrap();
    assert!(rec2.try_acquire_lock().unwrap());
}

#[test]
fn release_lock_is_no_op_when_not_held() {
    let f = tempfile::NamedTempFile::new().unwrap();
    let pool = open(f.path()).unwrap();
    let rec = RunRecorder::start(pool, &empty_wf(), "{}", &HashMap::new(), "manual").unwrap();
    // No prior acquire — release should silently succeed.
    rec.release_lock().unwrap();
}

#[test]
fn sweep_removes_old_lock_and_marks_run_stopped() {
    let f = tempfile::NamedTempFile::new().unwrap();
    let pool = open(f.path()).unwrap();
    let rec =
        RunRecorder::start(pool.clone(), &empty_wf(), "{}", &HashMap::new(), "manual").unwrap();
    assert!(rec.try_acquire_lock().unwrap());
    let two_days_ms: i64 = 2 * 24 * 3600 * 1000;
    pool.get()
        .unwrap()
        .execute(
            "UPDATE workflow_locks SET acquired_at = acquired_at - ?",
            [two_days_ms],
        )
        .unwrap();
    let one_day_ms: i64 = 24 * 3600 * 1000;
    let swept = super::sweep_stale_locks(&pool, one_day_ms).unwrap();
    assert_eq!(swept, 1);
    let status: String = pool
        .get()
        .unwrap()
        .query_row("SELECT status FROM runs WHERE id=?", [&rec.run_id], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(status, "stopped");
    let lock_count: i64 = pool
        .get()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM workflow_locks", [], |r| r.get(0))
        .unwrap();
    assert_eq!(lock_count, 0);
}

#[test]
fn sweep_leaves_fresh_lock_alone() {
    let f = tempfile::NamedTempFile::new().unwrap();
    let pool = open(f.path()).unwrap();
    let rec =
        RunRecorder::start(pool.clone(), &empty_wf(), "{}", &HashMap::new(), "manual").unwrap();
    assert!(rec.try_acquire_lock().unwrap());
    let one_day_ms: i64 = 24 * 3600 * 1000;
    let swept = super::sweep_stale_locks(&pool, one_day_ms).unwrap();
    assert_eq!(swept, 0);
    let lock_count: i64 = pool
        .get()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM workflow_locks", [], |r| r.get(0))
        .unwrap();
    assert_eq!(lock_count, 1);
}

#[test]
fn record_event_persists_with_type_tag() {
    let f = tempfile::NamedTempFile::new().unwrap();
    let pool = open(f.path()).unwrap();
    let rec =
        RunRecorder::start(pool.clone(), &empty_wf(), "{}", &HashMap::new(), "manual").unwrap();
    let ev = RunEvent {
        ty: EventType::NodeStarted,
        seq: rec.next_seq(),
        emitted_at: 1_716_045_600_000,
        run_id: rec.run_id.clone(),
        node_id: Some("n1".into()),
        iteration: Some(1),
        attempt: Some(1),
        payload: HashMap::new(),
    };
    rec.record_event(&ev).unwrap();
    let conn = pool.get().unwrap();
    let (ty, node_id): (String, String) = conn
        .query_row(
            "SELECT type, node_id FROM run_events WHERE run_id=? AND seq=?",
            rusqlite::params![&rec.run_id, 0_i64],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(ty, "node:started");
    assert_eq!(node_id, "n1");
}

/// Regression: `record_node_run` used `INSERT OR REPLACE`, whose
/// REPLACE step is "delete the conflicting row, then insert" —
/// which cascades through `node_outputs`'s `ON DELETE CASCADE` FK
/// and wipes every output that landed between the running and
/// done writes. The fix is to use `ON CONFLICT DO UPDATE` so the
/// status transition updates the row in place without deletion.
#[test]
fn status_transition_does_not_cascade_delete_outputs() {
    let f = tempfile::NamedTempFile::new().unwrap();
    let pool = open(f.path()).unwrap();
    let rec =
        RunRecorder::start(pool.clone(), &empty_wf(), "{}", &HashMap::new(), "manual").unwrap();

    // Mimic the run loop: write the running row, then a port, then
    // the done row. The output must survive the running→done
    // transition.
    rec.record_node_run(&NodeRunRow {
        node_id: "n1",
        iteration: 1,
        attempt: 1,
        node_type: "shell",
        status: "running",
        started_at: Some(1),
        finished_at: None,
        duration_ms: None,
        output_summary: None,
        error: None,
    })
    .unwrap();
    rec.record_node_output("n1", 1, 1, "text", Some("\"hello\""), None)
        .unwrap();
    rec.record_node_run(&NodeRunRow {
        node_id: "n1",
        iteration: 1,
        attempt: 1,
        node_type: "shell",
        status: "done",
        started_at: Some(1),
        finished_at: Some(2),
        duration_ms: Some(1),
        output_summary: None,
        error: None,
    })
    .unwrap();

    let conn = pool.get().unwrap();
    let outputs_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM node_outputs WHERE run_id=? AND node_id='n1'",
            [&rec.run_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        outputs_count, 1,
        "running→done transition must not cascade-delete the output row",
    );
    // node_runs row should now reflect the new status (UPDATE in place).
    let status: String = conn
        .query_row(
            "SELECT status FROM node_runs WHERE run_id=? AND node_id='n1'",
            [&rec.run_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(status, "done");
}
