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
use notify::RecursiveMode;
use notify_debouncer_mini::new_debouncer;
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
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

    // file-watch infrastructure: one mpsc receiver fed by all
    // debouncer threads. Registrations are populated on the FIRST
    // rescan only — adding/removing FileWatch triggers after daemon
    // start requires a restart (documented limitation; live reload
    // is v1.x polish).
    let (fw_tx, mut fw_rx) = mpsc::channel::<FileWatchEvent>(64);
    let mut file_watches: Vec<FileWatchReg> = Vec::new();
    let mut watchers_started = false;

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
                    if !watchers_started {
                        start_file_watchers(&engine, &mut file_watches, &fw_tx);
                        watchers_started = true;
                    }
                    last_scan = Some(std::time::Instant::now());
                }
                fire_due_schedules(&engine, &mut schedules);
            }
            Some(ev) = fw_rx.recv() => {
                fire_file_watch(&engine, &file_watches, &ev);
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

/// One debouncer's registration. The debouncer thread is owned via
/// the `_keepalive` field — dropping it stops the watcher.
struct FileWatchReg {
    workflow_id: String,
    /// Glob patterns this trigger fires on. The watcher sees raw paths
    /// from notify; we match against these patterns here.
    patterns: Vec<glob::Pattern>,
    vars: HashMap<String, String>,
    /// Holds the debouncer + its underlying watcher alive. Dropped
    /// when the runner exits.
    _keepalive: Box<dyn std::any::Any + Send + Sync>,
}

/// Event forwarded from a debouncer thread to the runner. Carries the
/// raw path so the runner can match against the right registration's
/// patterns (a single mpsc serves all watchers).
struct FileWatchEvent {
    paths: Vec<PathBuf>,
}

/// Walk all workflows, build per-trigger debouncers, push registrations.
fn start_file_watchers(
    engine: &Arc<Engine>,
    registrations: &mut Vec<FileWatchReg>,
    tx: &mpsc::Sender<FileWatchEvent>,
) {
    let home = engine.home();
    let Ok((workflows, errors)) = crate::workflows::list(home) else {
        tracing::warn!("trigger runner: workflows::list failed; no file watchers started");
        return;
    };
    for (path, err) in &errors {
        tracing::warn!(path = %path.display(), error = %err, "trigger runner: skip");
    }
    for wf in workflows {
        for trig in &wf.triggers {
            let Trigger::FileWatch {
                paths,
                debounce_ms,
                vars,
            } = trig
            else {
                continue;
            };
            let patterns: Vec<glob::Pattern> = paths
                .iter()
                .filter_map(|p| match glob::Pattern::new(p) {
                    Ok(g) => Some(g),
                    Err(e) => {
                        tracing::warn!(
                            workflow_id = %wf.id,
                            pattern = %p,
                            error = %e,
                            "trigger runner: invalid file-watch glob",
                        );
                        None
                    },
                })
                .collect();
            if patterns.is_empty() {
                continue;
            }
            let tx = tx.clone();
            let mut debouncer = match new_debouncer(
                Duration::from_millis(*debounce_ms),
                move |res: notify_debouncer_mini::DebounceEventResult| match res {
                    Ok(events) => {
                        let paths: Vec<PathBuf> = events.into_iter().map(|e| e.path).collect();
                        // blocking_send is fine inside the debouncer's own
                        // thread; if the receiver is gone the runner is
                        // shutting down and we silently drop.
                        drop(tx.blocking_send(FileWatchEvent { paths }));
                    },
                    Err(err) => {
                        tracing::warn!(error = ?err, "trigger runner: file watcher error");
                    },
                },
            ) {
                Ok(d) => d,
                Err(e) => {
                    tracing::warn!(
                        workflow_id = %wf.id,
                        error = ?e,
                        "trigger runner: debouncer init failed",
                    );
                    continue;
                },
            };
            // Watch each path. For globs we watch the deepest concrete
            // ancestor; the patterns filter the inbound events.
            let mut watch_roots: Vec<PathBuf> = Vec::new();
            for raw in paths {
                let root = concrete_root_for_glob(raw);
                if !watch_roots.contains(&root) {
                    watch_roots.push(root);
                }
            }
            let mut all_ok = true;
            for root in &watch_roots {
                if let Err(e) = debouncer.watcher().watch(root, RecursiveMode::Recursive) {
                    tracing::warn!(
                        workflow_id = %wf.id,
                        path = %root.display(),
                        error = ?e,
                        "trigger runner: watch() failed",
                    );
                    all_ok = false;
                }
            }
            if !all_ok {
                continue;
            }
            registrations.push(FileWatchReg {
                workflow_id: wf.id.clone(),
                patterns,
                vars: vars.clone(),
                _keepalive: Box::new(debouncer),
            });
            tracing::info!(
                workflow_id = %wf.id,
                roots = ?watch_roots,
                "trigger runner: file watcher registered",
            );
        }
    }
}

/// Drop everything before the rightmost path segment that contains a
/// glob metacharacter; that prefix is what we ask notify to watch.
fn concrete_root_for_glob(raw: &str) -> PathBuf {
    let p = std::path::Path::new(raw);
    let mut prefix = PathBuf::new();
    for comp in p.components() {
        let s = comp.as_os_str().to_string_lossy();
        if s.contains(['*', '?', '[']) {
            break;
        }
        prefix.push(comp.as_os_str());
    }
    if prefix.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        prefix
    }
}

fn fire_file_watch(engine: &Arc<Engine>, registrations: &[FileWatchReg], ev: &FileWatchEvent) {
    for reg in registrations {
        let any_match = ev
            .paths
            .iter()
            .any(|p| reg.patterns.iter().any(|g| g.matches_path(p)));
        if !any_match {
            continue;
        }
        match crate::workflows::load(engine.home(), &reg.workflow_id) {
            Ok(wf) => {
                let result =
                    engine.start_run(Arc::new(wf), reg.vars.clone(), "file-watch", false, None);
                match result {
                    Ok(handle) => {
                        tracing::info!(
                            workflow_id = %reg.workflow_id,
                            run_id = %handle.run_id,
                            "trigger runner: file-watch run started",
                        );
                    },
                    Err(crate::EngineError::AlreadyRunning { .. }) => {
                        tracing::info!(
                            workflow_id = %reg.workflow_id,
                            "trigger runner: previous run still active; skipping fire",
                        );
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn file_watch_fires_run_on_matching_write() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());
        // Watch a separate temp dir so notify events don't get clouded
        // by the engine writing its own files (kv_store, etc.).
        let watch_dir = TempDir::new().unwrap();
        let watch_pattern = format!("{}/*.txt", watch_dir.path().to_string_lossy());
        let wf = Workflow {
            id: "fw".into(),
            name: "fw".into(),
            schema_version: 1,
            created_at: None,
            updated_at: None,
            variables: HashMap::new(),
            triggers: vec![WfTrigger::FileWatch {
                paths: vec![watch_pattern.clone()],
                debounce_ms: 100,
                vars: HashMap::new(),
            }],
            nodes: vec![delay_node("step", 5)],
            edges: vec![],
        };
        crate::workflows::save(engine.home(), &wf).unwrap();

        let handle = engine.start_triggers();
        // Wait until the runner has done its first scan + spawned the watcher.
        tokio::time::sleep(Duration::from_millis(1500)).await;

        // Now write into the watched dir.
        std::fs::write(watch_dir.path().join("hello.txt"), "trigger me").unwrap();

        let mut saw_run = false;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let conn = engine.pool().get().unwrap();
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM runs WHERE workflow_id='fw' AND trigger_kind='file-watch'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            if count >= 1 {
                saw_run = true;
                break;
            }
        }
        handle.stop();
        drop(handle.join.await);
        assert!(saw_run, "file-watch should have fired a run");
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
