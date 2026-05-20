//! Trigger runner: long-lived task that fires workflow runs in
//! response to declared `Trigger` values (cron schedules in v1.2.3a;
//! file-watch in a later phase).
//!
//! [`Engine::start_triggers`] spawns a tokio task that holds a
//! per-workflow registration list. Every 30 seconds the list is
//! refreshed from `<home>/workflows/`, so new or removed workflow
//! files get picked up without restarting the runner. A
//! per-second tick checks all registered cron expressions; any whose
//! next fire time has passed since the last tick triggers a new run
//! via `Engine::run_workflow(.., trigger_kind="schedule", ..)`.
//!
//! Failures (cron parse errors, run lock contention, transient
//! `start_run` errors) are logged via tracing but do NOT terminate
//! the runner — the next tick keeps trying.

use crate::Engine;
use crate::types::Trigger;
use chrono::{DateTime, Utc};
use cron::Schedule as CronSchedule;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Handle returned by [`Engine::start_triggers`]. Drop or call
/// [`Self::stop`] to shut the runner down; the daemon CLI just awaits
/// `join` after a Ctrl-C.
pub struct TriggersHandle {
    cancel: CancellationToken,
    /// Background task that drives the schedule + (future) file-watch loops.
    /// `pub` so the daemon CLI / GUI host can `.await` it on shutdown.
    pub join: JoinHandle<()>,
}

impl TriggersHandle {
    /// Signal the runner to stop. The returned `join` handle resolves
    /// when the task exits.
    pub fn stop(&self) {
        self.cancel.cancel();
    }
}

/// Per-workflow schedule registration tracked across ticks.
struct ScheduleReg {
    workflow_id: String,
    schedule: CronSchedule,
    vars: HashMap<String, String>,
    /// Most recent fire time we already dispatched. `None` until the
    /// first dispatch, so we never backfire on registration.
    last_fired: Option<DateTime<Utc>>,
}

impl Engine {
    /// Start the trigger runner. Spawns a tokio task that periodically
    /// rescans `<home>/workflows/` and fires schedule triggers whose
    /// cron expression's next time has passed.
    #[must_use]
    pub fn start_triggers(self: &Arc<Self>) -> TriggersHandle {
        let engine = Arc::clone(self);
        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();
        let join = tokio::spawn(async move {
            run_triggers_loop(engine, cancel_for_task).await;
        });
        TriggersHandle { cancel, join }
    }
}

async fn run_triggers_loop(engine: Arc<Engine>, cancel: CancellationToken) {
    let mut schedules: Vec<ScheduleReg> = Vec::new();
    let mut last_scan: Option<std::time::Instant> = None;
    let rescan_interval = Duration::from_secs(30);
    let mut tick = tokio::time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            () = cancel.cancelled() => {
                tracing::info!("trigger runner stopping");
                return;
            }
            _ = tick.tick() => {
                let needs_rescan = last_scan
                    .is_none_or(|t| t.elapsed() >= rescan_interval);
                if needs_rescan {
                    refresh_schedules(&engine, &mut schedules);
                    last_scan = Some(std::time::Instant::now());
                }
                fire_due_schedules(&engine, &mut schedules);
            }
        }
    }
}

/// Reload `<home>/workflows/` and update the registration list.
/// Preserves `last_fired` for workflow ids we've already seen so that
/// rescans don't trigger spurious backfires.
fn refresh_schedules(engine: &Arc<Engine>, current: &mut Vec<ScheduleReg>) {
    let home = engine.home();
    let Ok((workflows, errors)) = crate::workflows::list(home) else {
        tracing::warn!("trigger runner: workflows::list failed; keeping prior schedules");
        return;
    };
    for (path, err) in &errors {
        tracing::warn!(path = %path.display(), error = %err, "trigger runner: skip");
    }
    let mut next: Vec<ScheduleReg> = Vec::new();
    for wf in workflows {
        for trig in &wf.triggers {
            let Trigger::Schedule { cron, vars } = trig else {
                continue;
            };
            let schedule = match CronSchedule::from_str(cron) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(
                        workflow_id = %wf.id,
                        cron = %cron,
                        error = %e,
                        "trigger runner: invalid cron expression — skipping"
                    );
                    continue;
                },
            };
            // Preserve last_fired for workflows we already tracked so
            // a rescan doesn't accidentally retrigger a recent run.
            let last_fired = current
                .iter()
                .find(|r| r.workflow_id == wf.id && r.schedule.to_string() == schedule.to_string())
                .and_then(|r| r.last_fired);
            next.push(ScheduleReg {
                workflow_id: wf.id.clone(),
                schedule,
                vars: vars.clone(),
                last_fired,
            });
        }
    }
    *current = next;
}

fn fire_due_schedules(engine: &Arc<Engine>, schedules: &mut [ScheduleReg]) {
    let now = Utc::now();
    for reg in schedules.iter_mut() {
        // Start the search from either the last fire time or the past
        // second, so we never backfire history but always catch the
        // next upcoming slot.
        let after = reg
            .last_fired
            .unwrap_or_else(|| now - chrono::Duration::seconds(1));
        let Some(next_fire) = reg.schedule.after(&after).next() else {
            continue;
        };
        if next_fire > now {
            continue;
        }
        // Due — dispatch a run.
        match crate::workflows::load(engine.home(), &reg.workflow_id) {
            Ok(wf) => {
                let result =
                    engine.start_run(Arc::new(wf), reg.vars.clone(), "schedule", false, None);
                match result {
                    Ok(handle) => {
                        tracing::info!(
                            workflow_id = %reg.workflow_id,
                            run_id = %handle.run_id,
                            "trigger runner: scheduled run started",
                        );
                        reg.last_fired = Some(next_fire);
                    },
                    Err(crate::EngineError::AlreadyRunning { .. }) => {
                        tracing::info!(
                            workflow_id = %reg.workflow_id,
                            "trigger runner: previous run still active; skipping fire",
                        );
                        // Mark as fired so we don't busy-loop on the
                        // same overdue slot every tick.
                        reg.last_fired = Some(next_fire);
                    },
                    Err(e) => {
                        tracing::warn!(
                            workflow_id = %reg.workflow_id,
                            error = %e,
                            "trigger runner: start_run failed",
                        );
                    },
                }
            },
            Err(e) => {
                tracing::warn!(
                    workflow_id = %reg.workflow_id,
                    error = %e,
                    "trigger runner: workflow load failed",
                );
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Node, Pos, Trigger as WfTrigger, Workflow};
    use tempfile::TempDir;

    fn delay_node(id: &str, ms: u64) -> Node {
        Node {
            id: id.into(),
            ty: "delay".into(),
            name: String::new(),
            config: HashMap::from([("ms".into(), serde_json::json!(ms))]),
            pos: Pos::default(),
            timeout_ms: None,
            retry: None,
            continue_on_error: false,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn schedule_runs_workflow_every_second() {
        // Cron `* * * * * *` (6 fields) → every second.
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());
        let wf = Workflow {
            id: "tick".into(),
            name: "tick".into(),
            schema_version: 1,
            created_at: None,
            updated_at: None,
            variables: HashMap::new(),
            triggers: vec![WfTrigger::Schedule {
                cron: "* * * * * *".into(),
                vars: HashMap::new(),
            }],
            nodes: vec![delay_node("step", 5)],
            edges: vec![],
        };
        crate::workflows::save(engine.home(), &wf).unwrap();

        let handle = engine.start_triggers();
        // Wait for at least one tick + run to land in `runs`.
        for _ in 0..60 {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let conn = engine.pool().get().unwrap();
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM runs WHERE workflow_id='tick' AND trigger_kind='schedule'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            if count >= 1 {
                handle.stop();
                drop(handle.join.await);
                return;
            }
        }
        panic!("scheduled run never started");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invalid_cron_is_logged_not_fatal() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());
        let wf = Workflow {
            id: "bad".into(),
            name: "bad".into(),
            schema_version: 1,
            created_at: None,
            updated_at: None,
            variables: HashMap::new(),
            triggers: vec![WfTrigger::Schedule {
                cron: "not a cron".into(),
                vars: HashMap::new(),
            }],
            nodes: vec![delay_node("step", 5)],
            edges: vec![],
        };
        crate::workflows::save(engine.home(), &wf).unwrap();

        let handle = engine.start_triggers();
        // Give the runner a couple of ticks to scan + skip the bad cron.
        tokio::time::sleep(Duration::from_millis(2500)).await;
        handle.stop();
        drop(handle.join.await);

        // No runs should have been recorded for the bad-cron workflow.
        let conn = engine.pool().get().unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM runs WHERE workflow_id='bad'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }
}
