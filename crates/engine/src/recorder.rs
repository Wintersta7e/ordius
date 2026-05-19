//! `RunRecorder`: owns the per-run `SQLite` write path.
//!
//! All writes for a given run flow through a single recorder
//! instance. The recorder is not `Clone` — to share it across
//! tasks, wrap it in `Arc<RunRecorder>`.

use crate::db::DbPool;
use crate::events::RunEvent;
use crate::types::Workflow;
use crate::{EngineError, Result};
use chrono::Utc;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

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
        let conn = pool.get().map_err(|e| EngineError::Db(e.to_string()))?;
        conn.execute(
            "INSERT INTO run_snapshots (id, workflow_id, created_at, workflow_json, node_specs_json) \
             VALUES (?,?,?,?,?)",
            rusqlite::params![&snapshot_id, &wf.id, now, &wf_json, node_specs_json],
        )
        .map_err(|e| EngineError::Db(e.to_string()))?;
        conn.execute(
            "INSERT INTO runs (id, workflow_id, workflow_name, status, started_at, \
                               variables_json, trigger_kind, workflow_snapshot_id) \
             VALUES (?,?,?,?,?,?,?,?)",
            rusqlite::params![
                &run_id,
                &wf.id,
                &wf.name,
                "running",
                now,
                &vars_json,
                trigger_kind,
                &snapshot_id,
            ],
        )
        .map_err(|e| EngineError::Db(e.to_string()))?;
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
        let payload_json =
            serde_json::to_string(&ev.payload).map_err(|e| EngineError::Db(e.to_string()))?;
        let ty_str = serde_json::to_value(ev.ty)
            .map_err(|e| EngineError::Db(e.to_string()))?
            .as_str()
            .expect("EventType serialises to a JSON string")
            .to_string();
        let seq_i64 =
            i64::try_from(ev.seq).map_err(|e| EngineError::Db(format!("seq overflow: {e}")))?;
        let channel = ev
            .payload
            .get("channel")
            .and_then(serde_json::Value::as_str);
        let conn = self
            .pool
            .get()
            .map_err(|e| EngineError::Db(e.to_string()))?;
        conn.execute(
            "INSERT INTO run_events \
               (run_id, seq, emitted_at, type, node_id, iteration, attempt, channel, payload_json) \
             VALUES (?,?,?,?,?,?,?,?,?)",
            rusqlite::params![
                &self.run_id,
                seq_i64,
                ev.emitted_at,
                &ty_str,
                &ev.node_id,
                ev.iteration,
                ev.attempt,
                channel,
                &payload_json,
            ],
        )
        .map_err(|e| EngineError::Db(e.to_string()))?;
        Ok(())
    }

    /// Mark the run finished. Updates `status`, `finished_at`,
    /// `duration_ms`, and optionally `error_tail`.
    pub fn finalize(&self, status: &str, error_tail: Option<&str>) -> Result<()> {
        let finished_at = Utc::now().timestamp_millis();
        let conn = self
            .pool
            .get()
            .map_err(|e| EngineError::Db(e.to_string()))?;
        conn.execute(
            "UPDATE runs SET status=?, finished_at=?, duration_ms=?-started_at, error_tail=? \
             WHERE id=?",
            rusqlite::params![status, finished_at, finished_at, error_tail, &self.run_id],
        )
        .map_err(|e| EngineError::Db(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
