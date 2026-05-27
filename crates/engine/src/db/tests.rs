use super::open;

const EXPECTED_TABLES: &[&str] = &[
    "env_specs",
    "kv_store",
    "migrated_custom_namespaces",
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
fn namespace_overrides_dropped_by_v3() {
    let f = tempfile::NamedTempFile::new().unwrap();
    let pool = open(f.path()).unwrap();
    let conn = pool.get().unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE name='namespace_overrides'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 0, "v3 must drop namespace_overrides");
}

#[test]
fn open_seeds_schema_version_to_latest() {
    let f = tempfile::NamedTempFile::new().unwrap();
    let pool = open(f.path()).unwrap();
    let conn = pool.get().unwrap();
    let version: i64 = conn
        .query_row("SELECT MAX(version) FROM schema_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(version, 3);
}

#[test]
fn open_is_idempotent() {
    let f = tempfile::NamedTempFile::new().unwrap();
    drop(open(f.path()).unwrap());
    // Re-open: schema is already at v3, migration should no-op.
    let pool = open(f.path()).unwrap();
    let conn = pool.get().unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM schema_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 3);
}

#[test]
fn migration_v3_idempotent() {
    let f = tempfile::NamedTempFile::new().unwrap();
    drop(open(f.path()).unwrap());
    drop(open(f.path()).unwrap());
    let pool = open(f.path()).unwrap();
    let conn = pool.get().unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE name='env_specs'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
}
