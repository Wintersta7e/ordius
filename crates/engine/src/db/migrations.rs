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

/// Apply the bundled migration set to `conn`. Idempotent.
pub(super) fn apply(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    conn.execute_batch(MIGRATION_V1)
}
