//! `SQLite` schema migrations.
//! Spec: `docs/04-storage-and-format.md` "Run history (`SQLite`)".

/// Single bundled migration that brings a fresh database up to
/// schema version 1. Idempotent — re-applying on an already-v1
/// database is a no-op via `IF NOT EXISTS` / `OR IGNORE`.
const MIGRATION_V1: &str = r"
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY);

CREATE TABLE IF NOT EXISTS run_snapshots (
  id              TEXT PRIMARY KEY,
  workflow_id     TEXT NOT NULL,
  created_at      INTEGER NOT NULL,
  workflow_json   TEXT NOT NULL,
  node_specs_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
  id                   TEXT PRIMARY KEY,
  workflow_id          TEXT NOT NULL,
  workflow_name        TEXT NOT NULL,
  status               TEXT NOT NULL,
  started_at           INTEGER NOT NULL,
  finished_at          INTEGER,
  duration_ms          INTEGER,
  variables_json       TEXT NOT NULL,
  error_tail           TEXT,
  trigger_kind         TEXT,
  workflow_snapshot_id TEXT NOT NULL REFERENCES run_snapshots(id)
);

CREATE TABLE IF NOT EXISTS node_runs (
  run_id          TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  node_id         TEXT NOT NULL,
  iteration       INTEGER NOT NULL DEFAULT 1,
  attempt         INTEGER NOT NULL DEFAULT 1,
  node_type       TEXT NOT NULL,
  status          TEXT NOT NULL,
  started_at      INTEGER,
  finished_at     INTEGER,
  duration_ms     INTEGER,
  output_summary  TEXT,
  error           TEXT,
  PRIMARY KEY (run_id, node_id, iteration, attempt)
);

CREATE TABLE IF NOT EXISTS node_outputs (
  run_id       TEXT NOT NULL,
  node_id      TEXT NOT NULL,
  iteration    INTEGER NOT NULL DEFAULT 1,
  attempt      INTEGER NOT NULL DEFAULT 1,
  port_name    TEXT NOT NULL,
  value_inline TEXT,
  value_path   TEXT,
  PRIMARY KEY (run_id, node_id, iteration, attempt, port_name),
  FOREIGN KEY (run_id, node_id, iteration, attempt)
    REFERENCES node_runs(run_id, node_id, iteration, attempt) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS run_events (
  run_id       TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  seq          INTEGER NOT NULL,
  emitted_at   INTEGER NOT NULL,
  type         TEXT NOT NULL,
  node_id      TEXT,
  iteration    INTEGER,
  attempt      INTEGER,
  channel      TEXT,
  payload_json TEXT NOT NULL,
  PRIMARY KEY (run_id, seq)
);

CREATE TABLE IF NOT EXISTS workflow_locks (
  workflow_id  TEXT PRIMARY KEY,
  run_id       TEXT NOT NULL,
  holder_pid   INTEGER NOT NULL,
  acquired_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS kv_store (
  workflow_id  TEXT NOT NULL,
  key          TEXT NOT NULL,
  value        TEXT NOT NULL,
  updated_at   INTEGER NOT NULL,
  PRIMARY KEY (workflow_id, key)
);

CREATE INDEX IF NOT EXISTS runs_workflow_started_idx ON runs (workflow_id, started_at DESC);
CREATE INDEX IF NOT EXISTS runs_status_idx           ON runs (status, started_at DESC);
CREATE INDEX IF NOT EXISTS node_runs_status_idx      ON node_runs (status, started_at DESC);
CREATE INDEX IF NOT EXISTS node_runs_type_idx        ON node_runs (node_type, status);
CREATE INDEX IF NOT EXISTS run_events_type_idx       ON run_events (type, run_id, seq);
CREATE INDEX IF NOT EXISTS run_events_node_idx       ON run_events (run_id, node_id, seq);

INSERT OR IGNORE INTO schema_version (version) VALUES (1);
";

/// Schema v2: per-namespace overrides table. Additive only.
/// CHECK constraint enforces that custom rows carry both `custom_label`
/// and `custom_host`, while non-custom rows (e.g. `wsl:Ubuntu`, `local`)
/// must have neither.
const MIGRATION_V2: &str = r"
CREATE TABLE IF NOT EXISTS namespace_overrides (
    namespace_id    TEXT PRIMARY KEY,
    enabled         INTEGER NOT NULL DEFAULT 1,
    custom_label    TEXT,
    custom_host     TEXT,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL,
    CHECK (
        (namespace_id LIKE 'custom:%' AND custom_label IS NOT NULL AND custom_host IS NOT NULL)
        OR
        (namespace_id NOT LIKE 'custom:%' AND custom_label IS NULL AND custom_host IS NULL)
    )
);
INSERT OR IGNORE INTO schema_version (version) VALUES (2);
";

/// Schema v3: persisted `EnvSpec` rows replace the session-C
/// `namespace_overrides` table. Local + WSL rows port directly into
/// `env_specs`; `custom:` rows land in `migrated_custom_namespaces`
/// for the Phase F UI to re-surface once SSH/Container dispatchers
/// ship. `namespace_overrides` is dropped at the end — Phase E owns
/// the canonical env shape from this point on.
const MIGRATION_V3: &str = r"
CREATE TABLE IF NOT EXISTS env_specs (
    id          TEXT PRIMARY KEY,
    label       TEXT NOT NULL,
    enabled     INTEGER NOT NULL DEFAULT 1,
    spec_json   TEXT NOT NULL,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS migrated_custom_namespaces (
    id           TEXT PRIMARY KEY,
    host         TEXT NOT NULL,
    label        TEXT NOT NULL,
    enabled      INTEGER NOT NULL,
    migrated_at  INTEGER NOT NULL
);

INSERT INTO env_specs (id, label, enabled, spec_json, created_at, updated_at)
SELECT 'local', 'Local', enabled,
       json_object('type','local','resources',json_array(),'host_direct_verifications',json_object()),
       unixepoch()*1000, unixepoch()*1000
FROM namespace_overrides
WHERE namespace_id = 'local'
LIMIT 1;

INSERT INTO env_specs (id, label, enabled, spec_json, created_at, updated_at)
SELECT 'wsl:' || substr(namespace_id, 5),
       'WSL: ' || substr(namespace_id, 5),
       enabled,
       json_object(
         'type', 'wsl_distro',
         'name', substr(namespace_id, 5),
         'resources', json_array(),
         'host_direct_verifications', json_object()
       ),
       unixepoch()*1000, unixepoch()*1000
FROM namespace_overrides
WHERE namespace_id LIKE 'wsl:%';

INSERT INTO migrated_custom_namespaces (id, host, label, enabled, migrated_at)
SELECT namespace_id, custom_host, custom_label, enabled, unixepoch()*1000
FROM namespace_overrides
WHERE namespace_id LIKE 'custom:%' AND custom_host IS NOT NULL;

DROP TABLE namespace_overrides;

INSERT OR IGNORE INTO schema_version (version) VALUES (3);
";

/// Apply the bundled migration set to `conn`. Idempotent.
///
/// V1 is unconditionally re-run because all of its statements are
/// `IF NOT EXISTS` / `INSERT OR IGNORE`; this also guarantees the
/// `schema_version` row exists before we read it. Then `MAX(version)`
/// gates later versions so they only run once.
pub(super) fn apply(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    conn.execute_batch(MIGRATION_V1)?;
    let v: i64 = conn.query_row(
        "SELECT COALESCE(MAX(version), 0) FROM schema_version",
        [],
        |r| r.get(0),
    )?;
    if v < 2 {
        conn.execute_batch(MIGRATION_V2)?;
    }
    if v < 3 {
        conn.execute_batch(MIGRATION_V3)?;
    }
    Ok(())
}
