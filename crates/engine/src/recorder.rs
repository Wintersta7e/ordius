//! `RunRecorder`: owns the per-run `SQLite` write path.
//!
//! All writes for a given run flow through a single recorder
//! instance. The recorder is not `Clone` — to share it across
//! tasks, wrap it in `Arc<RunRecorder>`.

use crate::db::DbPool;
use crate::events::{EventType, RunEvent};
use crate::types::Workflow;
use crate::{EngineError, Result};
use chrono::Utc;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

/// Borrowing row payload for [`RunRecorder::record_node_run`].
///
/// Keyed by `(run_id, node_id, iteration, attempt)` — the same
/// row is updated as a node transitions `running → done | error`.
pub struct NodeRunRow<'a> {
    /// Node id.
    pub node_id: &'a str,
    /// 1-based loop iteration index.
    pub iteration: u32,
    /// 1-based retry attempt index.
    pub attempt: u32,
    /// Built-in or manifest type name.
    pub node_type: &'a str,
    /// `pending` / `running` / `done` / `error` / `skipped`.
    pub status: &'a str,
    /// Wall-clock start time, Unix epoch milliseconds.
    pub started_at: Option<i64>,
    /// Wall-clock finish time, Unix epoch milliseconds.
    pub finished_at: Option<i64>,
    /// Difference between `finished_at` and `started_at`.
    pub duration_ms: Option<i64>,
    /// Short human-readable output summary (first line, truncated).
    pub output_summary: Option<&'a str>,
    /// Error message if `status == "error"`.
    pub error: Option<&'a str>,
}

/// Per-run recorder. Persists the run snapshot, run row, per-node
/// status updates, and the event stream into the `SQLite` database
/// behind the supplied `r2d2` pool.
pub struct RunRecorder {
    pool: DbPool,
    /// Unique id assigned to this run.
    pub run_id: String,
    /// Workflow id this run belongs to.
    pub workflow_id: String,
    seq: AtomicU64,
}

impl RunRecorder {
    /// Begin a new run.
    ///
    /// Inserts the workflow snapshot first (the run row references
    /// it via foreign key), then the run row itself with status
    /// `running`. Returns a recorder ready to accept events and
    /// per-node updates.
    pub fn start(
        pool: DbPool,
        wf: &Workflow,
        node_specs_json: &str,
        variables: &HashMap<String, String>,
        trigger_kind: &str,
    ) -> Result<Self> {
        let run_id = Uuid::new_v4().to_string();
        let snapshot_id = Uuid::new_v4().to_string();
        let now = Utc::now().timestamp_millis();
        let wf_json = serde_json::to_string(wf).map_err(|e| EngineError::Db(e.to_string()))?;
        let vars_json =
            serde_json::to_string(variables).map_err(|e| EngineError::Db(e.to_string()))?;
        let conn = pool.get()?;
        conn.prepare_cached(
            "INSERT INTO run_snapshots (id, workflow_id, created_at, workflow_json, node_specs_json) \
             VALUES (?,?,?,?,?)",
        )?
        .execute(rusqlite::params![
            &snapshot_id,
            &wf.id,
            now,
            &wf_json,
            node_specs_json,
        ])?;
        conn.prepare_cached(
            "INSERT INTO runs (id, workflow_id, workflow_name, status, started_at, \
                               variables_json, trigger_kind, workflow_snapshot_id) \
             VALUES (?,?,?,?,?,?,?,?)",
        )?
        .execute(rusqlite::params![
            &run_id,
            &wf.id,
            &wf.name,
            "running",
            now,
            &vars_json,
            trigger_kind,
            &snapshot_id,
        ])?;
        Ok(Self {
            pool,
            run_id,
            workflow_id: wf.id.clone(),
            seq: AtomicU64::new(0),
        })
    }

    /// Return the next monotonic sequence number for this run.
    /// Starts at 0 and increments by one per call.
    pub fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst)
    }

    /// Persist an emitted event to the `run_events` table.
    pub fn record_event(&self, ev: &RunEvent) -> Result<()> {
        // Empty payload is the common case (workflow lifecycle +
        // most node events) — skip the serializer state machine.
        let payload_json = if ev.payload.is_empty() {
            String::from("{}")
        } else {
            serde_json::to_string(&ev.payload).map_err(|e| EngineError::Db(e.to_string()))?
        };
        let seq_i64 =
            i64::try_from(ev.seq).map_err(|e| EngineError::Db(format!("seq overflow: {e}")))?;
        // `channel` is meaningful only on NodeOutput events.
        let channel = if ev.ty == EventType::NodeOutput {
            ev.payload
                .get("channel")
                .and_then(serde_json::Value::as_str)
        } else {
            None
        };
        let conn = self.pool.get()?;
        conn.prepare_cached(
            "INSERT INTO run_events \
               (run_id, seq, emitted_at, type, node_id, iteration, attempt, channel, payload_json) \
             VALUES (?,?,?,?,?,?,?,?,?)",
        )?
        .execute(rusqlite::params![
            &self.run_id,
            seq_i64,
            ev.emitted_at,
            ev.ty.wire_tag(),
            &ev.node_id,
            ev.iteration,
            ev.attempt,
            channel,
            &payload_json,
        ])?;
        Ok(())
    }

    /// Insert or update a row in `node_runs`. `INSERT OR REPLACE`
    /// so the same `(run_id, node_id, iteration, attempt)` key
    /// accepts updates as the node transitions through statuses.
    ///
    /// Uses `INSERT ... ON CONFLICT DO UPDATE` rather than
    /// `INSERT OR REPLACE`: the latter deletes the conflicting row
    /// first, which would cascade through `node_outputs`'s
    /// `ON DELETE CASCADE` FK and wipe the per-port output rows
    /// the run loop wrote between the running→done transition.
    pub fn record_node_run(&self, row: &NodeRunRow<'_>) -> Result<()> {
        let conn = self.pool.get()?;
        conn.prepare_cached(
            "INSERT INTO node_runs \
               (run_id, node_id, iteration, attempt, node_type, status, \
                started_at, finished_at, duration_ms, output_summary, error) \
             VALUES (?,?,?,?,?,?,?,?,?,?,?) \
             ON CONFLICT(run_id, node_id, iteration, attempt) DO UPDATE SET \
               node_type = excluded.node_type, \
               status = excluded.status, \
               started_at = excluded.started_at, \
               finished_at = excluded.finished_at, \
               duration_ms = excluded.duration_ms, \
               output_summary = excluded.output_summary, \
               error = excluded.error",
        )?
        .execute(rusqlite::params![
            &self.run_id,
            row.node_id,
            row.iteration,
            row.attempt,
            row.node_type,
            row.status,
            row.started_at,
            row.finished_at,
            row.duration_ms,
            row.output_summary,
            row.error,
        ])?;
        Ok(())
    }

    /// Insert or update an entry in `node_outputs` for one
    /// `(run, node, iteration, attempt, port_name)` tuple.
    /// Exactly one of `value_inline` or `value_path` should be
    /// provided — large values live on disk and `value_path`
    /// points at them.
    pub fn record_node_output(
        &self,
        node_id: &str,
        iteration: u32,
        attempt: u32,
        port_name: &str,
        value_inline: Option<&str>,
        value_path: Option<&str>,
    ) -> Result<()> {
        let conn = self.pool.get()?;
        conn.prepare_cached(
            "INSERT OR REPLACE INTO node_outputs \
               (run_id, node_id, iteration, attempt, port_name, value_inline, value_path) \
             VALUES (?,?,?,?,?,?,?)",
        )?
        .execute(rusqlite::params![
            &self.run_id,
            node_id,
            iteration,
            attempt,
            port_name,
            value_inline,
            value_path,
        ])?;
        Ok(())
    }

    /// Try to acquire the workflow-level lock for this run's
    /// workflow id. Returns `Ok(true)` if the lock was claimed,
    /// `Ok(false)` if another run already holds it. The lock is
    /// the `workflow_id` `PRIMARY KEY` in `workflow_locks`; the
    /// implementation relies on `INSERT` raising a constraint
    /// violation when the key is taken.
    pub fn try_acquire_lock(&self) -> Result<bool> {
        let conn = self.pool.get()?;
        let now = Utc::now().timestamp_millis();
        let pid = i64::from(std::process::id());
        let res = conn.execute(
            "INSERT INTO workflow_locks (workflow_id, run_id, holder_pid, acquired_at) \
             VALUES (?,?,?,?)",
            rusqlite::params![&self.workflow_id, &self.run_id, pid, now],
        );
        match res {
            Ok(_) => Ok(true),
            Err(rusqlite::Error::SqliteFailure(e, _))
                if e.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                Ok(false)
            },
            Err(e) => Err(e.into()),
        }
    }

    /// Release the workflow lock held by this run. No-op if the
    /// lock has already been released or was never held by this
    /// `run_id`.
    pub fn release_lock(&self) -> Result<()> {
        let conn = self.pool.get()?;
        conn.execute(
            "DELETE FROM workflow_locks WHERE workflow_id=? AND run_id=?",
            rusqlite::params![&self.workflow_id, &self.run_id],
        )?;
        Ok(())
    }

    /// Mark the run finished. Updates `status`, `finished_at`,
    /// `duration_ms`, and optionally `error_tail`.
    pub fn finalize(&self, status: &str, error_tail: Option<&str>) -> Result<()> {
        let finished_at = Utc::now().timestamp_millis();
        let conn = self.pool.get()?;
        conn.execute(
            "UPDATE runs SET status=?, finished_at=?, duration_ms=?-started_at, error_tail=? \
             WHERE id=?",
            rusqlite::params![status, finished_at, finished_at, error_tail, &self.run_id],
        )?;
        Ok(())
    }
}

/// Sweep `workflow_locks` for stale entries.
///
/// A lock is considered stale if its holder PID no longer belongs
/// to an ordius process, or if it was acquired more than
/// `max_age_ms` milliseconds ago. Stale locks are deleted and any
/// associated runs that are still in status `running` are
/// post-hoc marked `stopped` with an error tail so the history
/// viewer reflects them correctly. Returns the number of locks
/// swept.
pub fn sweep_stale_locks(pool: &DbPool, max_age_ms: i64) -> Result<usize> {
    let conn = pool.get()?;
    let now = Utc::now().timestamp_millis();
    let rows: Vec<(String, String, i64, i64)> = {
        let mut stmt = conn
            .prepare("SELECT workflow_id, run_id, holder_pid, acquired_at FROM workflow_locks")?;
        let iter = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?;
        iter.filter_map(std::result::Result::ok).collect()
    };
    let mut swept = 0;
    for (wf_id, run_id, pid_i64, acquired) in rows {
        let pid_alive = u32::try_from(pid_i64).ok().is_some_and(pid_is_ordius);
        let stale = (now - acquired) > max_age_ms || !pid_alive;
        if stale {
            conn.execute("DELETE FROM workflow_locks WHERE workflow_id=?", [&wf_id])?;
            conn.execute(
                "UPDATE runs SET status='stopped', \
                                  error_tail='engine crashed during run' \
                 WHERE id=? AND status='running'",
                [&run_id],
            )?;
            swept += 1;
        }
    }
    Ok(swept)
}

#[cfg(unix)]
fn pid_is_ordius(pid: u32) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/comm")).is_ok_and(|s| s.contains("ordius"))
}

#[cfg(windows)]
fn pid_is_ordius(pid: u32) -> bool {
    use std::os::windows::process::CommandExt;
    let out = std::process::Command::new("tasklist")
        .args(["/FI", &format!("PID eq {pid}"), "/NH"])
        .creation_flags(0x0800_0000) // CREATE_NO_WINDOW
        .output();
    matches!(out, Ok(o) if !o.stdout.is_empty())
}

#[cfg(test)]
mod tests;
