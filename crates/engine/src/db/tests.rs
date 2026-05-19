use super::open;

const EXPECTED_TABLES: &[&str] = &[
    "kv_store",
    "node_outputs",
    "node_runs",
    "run_events",
    "run_snapshots",
    "runs",
    "schema_version",
    "workflow_locks",
];

#[test]
fn open_creates_schema() {
    let f = tempfile::NamedTempFile::new().unwrap();
    let pool = open(f.path()).unwrap();
    let conn = pool.get().unwrap();
    let names: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .filter_map(std::result::Result::ok)
        .collect();
    for expected in EXPECTED_TABLES {
        assert!(
            names.iter().any(|n| n == expected),
            "missing table: {expected}",
        );
    }
}

#[test]
fn open_seeds_schema_version_to_one() {
    let f = tempfile::NamedTempFile::new().unwrap();
    let pool = open(f.path()).unwrap();
    let conn = pool.get().unwrap();
    let version: i64 = conn
        .query_row("SELECT version FROM schema_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(version, 1);
}

#[test]
fn open_is_idempotent() {
    let f = tempfile::NamedTempFile::new().unwrap();
    drop(open(f.path()).unwrap());
    // Re-open: schema is already at v1, migration should no-op.
    let pool = open(f.path()).unwrap();
    let conn = pool.get().unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM schema_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);
}
