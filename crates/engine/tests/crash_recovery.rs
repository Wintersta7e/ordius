//! End-to-end: `Engine::new` sweeps stale `workflow_locks` left
//! behind by a prior crash and post-hoc marks the orphaned `runs`
//! row as `stopped`. The Phase 7.5 stub already wires
//! `sweep_stale_locks` into `Engine::new`; this test verifies the
//! behaviour against a pre-seeded post-crash database state.

use ordius_engine::db::open;
use ordius_engine::Engine;

#[tokio::test(flavor = "multi_thread")]
async fn engine_startup_sweeps_stale_locks() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("runs.db");
    let pool = open(&db_path).unwrap();

    // Seed: snapshot + running run + stale lock with a PID that's
    // certainly not an ordius process (sweep treats both age AND
    // non-ordius-PID as stale, so a 999_999 holder is enough).
    let conn = pool.get().unwrap();
    conn.execute(
        "INSERT INTO run_snapshots (id, workflow_id, created_at, workflow_json, node_specs_json) \
         VALUES (?,?,?,?,?)",
        rusqlite::params!["s1", "w1", 0_i64, "{}", "{}"],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO runs (id, workflow_id, workflow_name, status, started_at, variables_json, \
                           trigger_kind, workflow_snapshot_id) \
         VALUES (?,?,?,?,?,?,?,?)",
        rusqlite::params!["r1", "w1", "n", "running", 0_i64, "{}", "manual", "s1"],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO workflow_locks (workflow_id, run_id, holder_pid, acquired_at) \
         VALUES (?,?,?,?)",
        rusqlite::params!["w1", "r1", 999_999_i64, 0_i64],
    )
    .unwrap();
    drop(conn);

    // Engine::new must invoke sweep_stale_locks as part of its
    // construction sequence.
    let _engine = Engine::new(dir.path().to_path_buf()).await.unwrap();

    let conn = pool.get().unwrap();
    let lock_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM workflow_locks", [], |r| r.get(0))
        .unwrap();
    assert_eq!(lock_count, 0, "stale lock should have been swept");

    let status: String = conn
        .query_row("SELECT status FROM runs WHERE id='r1'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(status, "stopped", "orphaned run should be marked stopped");

    let error_tail: Option<String> = conn
        .query_row("SELECT error_tail FROM runs WHERE id='r1'", [], |r| r.get(0))
        .unwrap();
    assert!(
        error_tail.is_some_and(|s| s.contains("crashed")),
        "error_tail should explain the cause",
    );
}
