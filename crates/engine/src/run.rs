//! `Engine::start_run` and `Engine::run_workflow` — the workflow
//! run entry points.
//!
//! `start_run` is non-async: validates, acquires the workflow
//! lock, registers the run's broadcast sender + cancel token on
//! the engine, and spawns the run loop, returning a `RunHandle`.
//! `run_workflow` is the convenience wrapper that awaits
//! `handle.join`.
//!
//! `run_loop_inner` drives the scheduler to completion: for each
//! ready batch it dispatches via [`crate::executor::Dispatcher`],
//! persists outputs into `node_outputs`, updates the run-loop's
//! authoritative `upstream_outputs`, emits node lifecycle events,
//! routes `condition` branches + bumps loop iteration counters,
//! drains skipped nodes, and finally selects one of
//! `workflow:done` / `workflow:error` / `workflow:stopped`.

use crate::emitter::Emitter;
use crate::events::{EventType, RunEvent};
use crate::executor::builtins::condition::NODE_TYPE_ID as CONDITION_NODE_TYPE_ID;
use crate::executor::{Dispatcher, NodeError, NodeExecutor, RunContext, wrap_process_env};
use crate::recorder::{NodeRunRow, RunRecorder};
use crate::scheduler::Scheduler;
use crate::types::{BackoffStrategy, EdgeType, PortValue, RetryOn, RetryPolicy, Workflow};
use crate::{Engine, EngineError, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

/// Cap on retry backoff sleeps. Workflows should fail loud rather
/// than silently park on a multi-minute delay.
const BACKOFF_CAP: Duration = Duration::from_mins(1);

/// Terminal summary returned by the run loop.
#[derive(Debug, Clone)]
pub struct RunSummary {
    /// Unique run id.
    pub run_id: String,
    /// Terminal status (`done` / `error` / `stopped`).
    pub status: String,
    /// Number of `node_runs` rows persisted across the run.
    pub node_runs: usize,
}

/// Handle returned by [`Engine::start_run`] — the caller can
/// subscribe to events via `event_rx` and join the run loop via
/// `join`.
pub struct RunHandle {
    /// Unique run id.
    pub run_id: String,
    /// Fresh broadcast receiver — the run task pushes events here
    /// as well as into the recorder.
    pub event_rx: broadcast::Receiver<RunEvent>,
    /// Task handle for the run loop. Awaiting it surfaces the
    /// `RunSummary` (or a join error if the task panicked).
    pub join: tokio::task::JoinHandle<Result<RunSummary>>,
}

impl Engine {
    /// Start a workflow run. Returns immediately with a
    /// [`RunHandle`]; the run loop spawns on tokio.
    ///
    /// Errors:
    /// - [`EngineError::Validation`] if the workflow fails
    ///   structural validation.
    /// - [`EngineError::AlreadyRunning`] if another run already
    ///   holds the workflow's lock.
    pub fn start_run(
        self: &Arc<Self>,
        wf: Arc<Workflow>,
        variables: HashMap<String, String>,
        trigger_kind: &str,
        auto_resume_checkpoints: bool,
    ) -> Result<RunHandle> {
        crate::validation::validate(&wf)?;

        let rec = Arc::new(RunRecorder::start(
            self.pool(),
            &wf,
            "{}",
            &variables,
            trigger_kind,
        )?);
        if !rec.try_acquire_lock()? {
            return Err(EngineError::AlreadyRunning {
                id: wf.id.clone(),
                run_id: rec.run_id.clone(),
            });
        }

        let (em, rx) = Emitter::new(rec.clone());
        let em = Arc::new(em);

        self.run_senders
            .lock()
            .expect("engine run_senders mutex poisoned")
            .insert(rec.run_id.clone(), em.broadcast_sender());
        let cancel = CancellationToken::new();
        self.run_tokens
            .lock()
            .expect("engine run_tokens mutex poisoned")
            .insert(rec.run_id.clone(), cancel.clone());

        em.emit(EventType::WorkflowStarted, None, None, None, HashMap::new());

        let workspace = self.home().join("workspaces").join(&rec.run_id);
        if let Err(e) = std::fs::create_dir_all(&workspace) {
            tracing::warn!(
                error = ?e, path = %workspace.display(),
                "could not create run workspace; falling back to engine home",
            );
        }

        let run_id = rec.run_id.clone();
        let engine = Arc::clone(self);
        let join = tokio::spawn(async move {
            let res = engine
                .run_loop_inner(
                    wf,
                    rec.clone(),
                    em,
                    workspace,
                    variables,
                    cancel,
                    auto_resume_checkpoints,
                )
                .await;
            engine
                .run_senders
                .lock()
                .expect("engine run_senders mutex poisoned")
                .remove(&rec.run_id);
            engine
                .run_tokens
                .lock()
                .expect("engine run_tokens mutex poisoned")
                .remove(&rec.run_id);
            let release = rec.release_lock();
            drop(release);
            res
        });

        Ok(RunHandle {
            run_id,
            event_rx: rx,
            join,
        })
    }

    /// Convenience wrapper that starts a run and awaits its
    /// completion. Callers that need to stream events should use
    /// [`Self::start_run`] directly so they can subscribe before
    /// the run begins.
    pub async fn run_workflow(
        self: &Arc<Self>,
        wf: Arc<Workflow>,
        variables: HashMap<String, String>,
        trigger_kind: &str,
        auto_resume_checkpoints: bool,
    ) -> Result<RunSummary> {
        let handle = self.start_run(wf, variables, trigger_kind, auto_resume_checkpoints)?;
        handle
            .join
            .await
            .map_err(|e| EngineError::Db(format!("run task join: {e}")))?
    }

    /// Drives the scheduler to completion: dispatches each ready
    /// batch through [`Dispatcher`], persists outputs, emits node
    /// lifecycle events, routes condition branches + loop fires,
    /// and selects the terminal `workflow:*` event.
    ///
    /// `continue_on_error` advances the scheduler even on
    /// failure; the `had_unhandled_failure` bit distinguishes
    /// `done` from `error` at the end because
    /// [`Scheduler::is_done`] returns true once no Ready/Running
    /// nodes remain, which a cascaded failure also satisfies.
    // Still over clippy's 100-line line limit even after extracting
    // persist_node_outputs / finalize_node_run / route_condition_outputs
    // — the dispatch loop's per-node context construction + scheduler
    // dance still dominate.
    #[allow(clippy::too_many_lines)]
    async fn run_loop_inner(
        &self,
        wf: Arc<Workflow>,
        recorder: Arc<RunRecorder>,
        emitter: Arc<Emitter>,
        workspace: PathBuf,
        variables: HashMap<String, String>,
        cancel: CancellationToken,
        _auto_resume: bool,
    ) -> Result<RunSummary> {
        let mut upstream_outputs: HashMap<(String, String), PortValue> = HashMap::new();
        let mut iterations: HashMap<String, u32> = HashMap::new();
        let mut node_runs_count: usize = 0;
        // Tracks whether any unhandled node failure occurred (i.e.
        // a NodeError on a node without continue_on_error). The
        // scheduler's `is_done()` returns true once no Ready /
        // Running nodes remain, which a failure also satisfies, so
        // we need our own bit to distinguish "done" from "error".
        let mut had_unhandled_failure = false;

        let mut sched = Scheduler::new(&wf);
        let dispatcher = Dispatcher::new();

        while !sched.is_done() && !sched.is_stalled() {
            if cancel.is_cancelled() {
                break;
            }

            let ready: Vec<crate::types::Node> = sched.ready().into_iter().cloned().collect();
            if ready.is_empty() {
                break;
            }

            for node in ready {
                let mut current_inputs: HashMap<String, PortValue> = HashMap::new();
                for edge in wf
                    .edges
                    .iter()
                    .filter(|e| e.to_node_id == node.id && e.kind == EdgeType::Forward)
                {
                    let key = (edge.from_node_id.clone(), edge.from_port.clone());
                    if let Some(v) = upstream_outputs.get(&key) {
                        current_inputs.insert(edge.to_port.clone(), v.clone());
                    }
                }

                let ctx = RunContext {
                    run_id: recorder.run_id.clone(),
                    workflow_id: wf.id.clone(),
                    workflow_name: wf.name.clone(),
                    started_at_iso: String::new(),
                    workspace: workspace.clone(),
                    variables: variables.clone(),
                    recorder: recorder.clone(),
                    emitter: emitter.clone(),
                    secrets_store: None,
                    env: wrap_process_env(),
                    current_inputs,
                    upstream_outputs: upstream_outputs.clone(),
                    checkpoints: self.checkpoints(),
                };

                let nt = self
                    .registry()
                    .get(&node.ty)
                    .ok_or_else(|| EngineError::Node {
                        node_id: node.id.clone(),
                        message: format!("unknown node type '{}'", node.ty),
                    })?;

                let iteration = *iterations.entry(node.id.clone()).or_insert(1);
                let retry_policy = node.retry.clone().unwrap_or_default();
                let effective_timeout = node.timeout_ms.or(nt.execution.timeout_ms);

                sched.start_node(&node.id);

                let (res, attempt, started_at, finished_at) = run_with_retry(
                    &dispatcher,
                    &recorder,
                    &emitter,
                    &node,
                    &nt,
                    &ctx,
                    iteration,
                    &retry_policy,
                    effective_timeout,
                    &cancel,
                    &mut node_runs_count,
                )
                .await?;
                let duration = finished_at - started_at;

                match res {
                    Ok(outputs) => {
                        persist_node_outputs(
                            &recorder,
                            &mut upstream_outputs,
                            &node.id,
                            iteration,
                            attempt,
                            &outputs,
                        )?;
                        finalize_node_run(
                            &recorder,
                            &emitter,
                            &node,
                            iteration,
                            attempt,
                            started_at,
                            finished_at,
                            "done",
                            None,
                            EventType::NodeDone,
                            HashMap::from([
                                ("finished_at".into(), serde_json::json!(finished_at)),
                                ("duration_ms".into(), serde_json::json!(duration)),
                            ]),
                        )?;
                        sched.complete_node(&node.id);
                        route_condition_outputs(
                            &mut sched,
                            &mut iterations,
                            &emitter,
                            &node,
                            iteration,
                            &outputs,
                        );
                    },
                    Err(e) => {
                        let msg = e.to_string();
                        finalize_node_run(
                            &recorder,
                            &emitter,
                            &node,
                            iteration,
                            attempt,
                            started_at,
                            finished_at,
                            "error",
                            Some(&msg),
                            EventType::NodeError,
                            HashMap::from([("error".into(), serde_json::json!(msg))]),
                        )?;
                        if node.continue_on_error {
                            sched.complete_node(&node.id);
                        } else {
                            sched.fail_node(&node.id);
                            had_unhandled_failure = true;
                        }
                    },
                }
            }

            // Mid-loop drain so node:skipped events arrive in
            // causal order with the condition that triggered them.
            for skipped_id in sched.drain_newly_skipped() {
                emitter.emit_node(EventType::NodeSkipped, skipped_id, 1, 1, HashMap::new());
            }
        }

        for skipped_id in sched.drain_newly_skipped() {
            emitter.emit_node(EventType::NodeSkipped, skipped_id, 1, 1, HashMap::new());
        }

        let (status, terminal_event, error_tail) = if cancel.is_cancelled() {
            ("stopped", EventType::WorkflowStopped, None)
        } else if had_unhandled_failure {
            ("error", EventType::WorkflowError, Some("node failed"))
        } else if sched.is_done() {
            ("done", EventType::WorkflowDone, None)
        } else {
            // Loop exited with at least one node still in a non-
            // terminal state — the scheduler reports stalled.
            (
                "error",
                EventType::WorkflowError,
                Some("workflow stalled: no Ready or Running nodes"),
            )
        };

        let mut payload: HashMap<String, serde_json::Value> = HashMap::new();
        if let Some(reason) = error_tail {
            payload.insert("error".into(), serde_json::json!(reason));
        }
        emitter.emit(terminal_event, None, None, None, payload);
        recorder.finalize(status, error_tail)?;

        Ok(RunSummary {
            run_id: recorder.run_id.clone(),
            status: status.into(),
            node_runs: node_runs_count,
        })
    }
}

/// Drive one node to a terminal result, honoring its `RetryPolicy`
/// and per-attempt timeout budget. Each attempt writes its own
/// `node_runs` row keyed on `(run, node, iteration, attempt)`;
/// intermediate failures emit `node:retry` and sleep for
/// `compute_backoff(policy, attempt)` (cancellable). Returns
/// `(final_result, final_attempt, started_at, finished_at)` so
/// the caller's `finalize_node_run` writes the row that records
/// the terminal outcome.
#[allow(clippy::too_many_arguments)]
async fn run_with_retry(
    dispatcher: &Dispatcher,
    recorder: &RunRecorder,
    emitter: &Emitter,
    node: &crate::types::Node,
    nt: &crate::types::NodeType,
    ctx: &RunContext,
    iteration: u32,
    policy: &RetryPolicy,
    timeout_ms: Option<u64>,
    cancel: &CancellationToken,
    node_runs_count: &mut usize,
) -> Result<(
    std::result::Result<crate::executor::NodeOutputs, NodeError>,
    u32,
    i64,
    i64,
)> {
    let mut attempt: u32 = 1;
    loop {
        let started_at = chrono::Utc::now().timestamp_millis();
        recorder.record_node_run(&NodeRunRow {
            node_id: &node.id,
            iteration,
            attempt,
            node_type: &node.ty,
            status: "running",
            started_at: Some(started_at),
            finished_at: None,
            duration_ms: None,
            output_summary: None,
            error: None,
        })?;
        *node_runs_count += 1;
        emitter.emit_node(
            EventType::NodeStarted,
            node.id.clone(),
            iteration,
            attempt,
            HashMap::new(),
        );

        // Child token so a timeout for this attempt doesn't cancel
        // the run-wide token (which would prevent further retries).
        let attempt_cancel = cancel.child_token();
        let res = dispatch_one_attempt(dispatcher, node, nt, ctx, attempt_cancel, timeout_ms).await;
        let finished_at = chrono::Utc::now().timestamp_millis();

        let should_retry = match &res {
            Ok(_) => false,
            Err(_) if cancel.is_cancelled() => false,
            Err(e) => attempt < policy.max_attempts && retry_matches(policy.retry_on, e),
        };

        if !should_retry {
            return Ok((res, attempt, started_at, finished_at));
        }

        // Mid-retry: persist this attempt's failure + emit node:retry.
        let msg = res
            .as_ref()
            .err()
            .map(ToString::to_string)
            .unwrap_or_default();
        recorder.record_node_run(&NodeRunRow {
            node_id: &node.id,
            iteration,
            attempt,
            node_type: &node.ty,
            status: "error",
            started_at: Some(started_at),
            finished_at: Some(finished_at),
            duration_ms: Some(finished_at - started_at),
            output_summary: None,
            error: Some(&msg),
        })?;
        emitter.emit_node(
            EventType::NodeRetry,
            node.id.clone(),
            iteration,
            attempt,
            HashMap::from([
                ("prev_error".into(), serde_json::json!(msg)),
                ("next_attempt".into(), serde_json::json!(attempt + 1)),
            ]),
        );

        let delay = compute_backoff(policy, attempt);
        tokio::select! {
            () = tokio::time::sleep(delay) => {}
            () = cancel.cancelled() => {
                return Ok((Err(NodeError::Cancelled), attempt, started_at, finished_at));
            }
        }
        attempt += 1;
    }
}

/// Dispatch a single attempt under an optional per-attempt
/// timeout. On elapsed, cancels the attempt token AND awaits the
/// executor future's teardown with a bounded grace window — that
/// way the subprocess executor's `cancel.cancelled()` arm runs
/// (SIGTERM → SIGKILL to `-pgid` on Unix; `TerminateJobObject` on
/// Windows) before this function returns. Returning early with
/// `tokio::time::timeout(budget, inner).await` drops `inner`
/// without giving its cancellation arm a chance to fire, so any
/// in-flight child processes outlive the run.
async fn dispatch_one_attempt(
    dispatcher: &Dispatcher,
    node: &crate::types::Node,
    nt: &crate::types::NodeType,
    ctx: &RunContext,
    attempt_cancel: CancellationToken,
    timeout_ms: Option<u64>,
) -> std::result::Result<crate::executor::NodeOutputs, NodeError> {
    let Some(ms) = timeout_ms else {
        return dispatcher.run(node, nt, ctx, attempt_cancel).await;
    };
    let budget = Duration::from_millis(ms);
    let run_fut = dispatcher.run(node, nt, ctx, attempt_cancel.clone());
    tokio::pin!(run_fut);

    tokio::select! {
        biased;
        result = &mut run_fut => result,
        () = tokio::time::sleep(budget) => {
            attempt_cancel.cancel();
            // Grace window for the executor's cancellation arm to
            // finish. Subprocess Unix waits 2s between SIGTERM and
            // SIGKILL; the line-reader drain after wait() bumps the
            // worst-case envelope. 5s covers both with margin.
            let grace = Duration::from_secs(5);
            let teardown = tokio::time::timeout(grace, &mut run_fut).await;
            drop(teardown);
            Err(NodeError::Timeout(ms))
        }
    }
}

fn compute_backoff(policy: &RetryPolicy, attempt: u32) -> Duration {
    let base = u128::from(policy.backoff_ms);
    let cap = BACKOFF_CAP.as_millis();
    let ms = match policy.backoff_strategy {
        BackoffStrategy::Fixed => base,
        BackoffStrategy::Linear => base.saturating_mul(u128::from(attempt)),
        BackoffStrategy::Exponential => {
            let shift = attempt.saturating_sub(1).min(20);
            base.saturating_mul(1u128 << shift)
        },
    };
    Duration::from_millis(u64::try_from(ms.min(cap)).unwrap_or(u64::MAX))
}

const fn retry_matches(retry_on: RetryOn, err: &NodeError) -> bool {
    let is_timeout = matches!(err, NodeError::Timeout(_));
    match retry_on {
        RetryOn::Error => !is_timeout,
        RetryOn::Timeout => is_timeout,
        RetryOn::Both => true,
    }
}

/// Persist every output port + populate the run-loop's
/// authoritative `upstream_outputs` map. Inline JSON only today;
/// the >8 KB file-back fallback is not yet wired.
///
/// `attempt` is the attempt number that produced these outputs —
/// not always `1`. A node that succeeds on its second retry stores
/// its outputs under `(run, node, iteration, 2)`, matching the
/// `node:done` event the run loop emits with `attempt=2`. Earlier
/// failed attempts have their own `node_runs` rows but no
/// `node_outputs` (failures don't produce outputs).
fn persist_node_outputs(
    recorder: &RunRecorder,
    upstream_outputs: &mut HashMap<(String, String), PortValue>,
    node_id: &str,
    iteration: u32,
    attempt: u32,
    outputs: &crate::executor::NodeOutputs,
) -> Result<()> {
    for (port_name, value) in outputs {
        let value_json = serde_json::to_string(value)
            .map_err(|e| EngineError::Db(format!("encode output: {e}")))?;
        recorder.record_node_output(
            node_id,
            iteration,
            attempt,
            port_name,
            Some(&value_json),
            None,
        )?;
        upstream_outputs.insert((node_id.to_string(), port_name.clone()), value.clone());
    }
    Ok(())
}

/// Shared `done` / `error` finalisation: update the `node_runs`
/// row, then emit the terminal `node:*` event with the same shape
/// both arms used to build by hand.
#[allow(clippy::too_many_arguments)] // the row + event payload genuinely needs all of these
fn finalize_node_run(
    recorder: &RunRecorder,
    emitter: &Emitter,
    node: &crate::types::Node,
    iteration: u32,
    attempt: u32,
    started_at: i64,
    finished_at: i64,
    status: &str,
    error: Option<&str>,
    event_type: EventType,
    event_payload: HashMap<String, serde_json::Value>,
) -> Result<()> {
    recorder.record_node_run(&NodeRunRow {
        node_id: &node.id,
        iteration,
        attempt,
        node_type: &node.ty,
        status,
        started_at: Some(started_at),
        finished_at: Some(finished_at),
        duration_ms: Some(finished_at - started_at),
        output_summary: None,
        error,
    })?;
    emitter.emit_node(
        event_type,
        node.id.clone(),
        iteration,
        attempt,
        event_payload,
    );
    Ok(())
}

/// Route a freshly-completed condition node's `branch` output
/// into the scheduler + fire any loop edge keyed off that branch.
/// No-op for non-condition nodes or condition outputs missing the
/// `branch` String port.
fn route_condition_outputs(
    sched: &mut crate::scheduler::Scheduler<'_>,
    iterations: &mut HashMap<String, u32>,
    emitter: &Emitter,
    node: &crate::types::Node,
    iteration: u32,
    outputs: &crate::executor::NodeOutputs,
) {
    if node.ty != CONDITION_NODE_TYPE_ID {
        return;
    }
    let Some(PortValue::String(branch)) = outputs.get("branch") else {
        return;
    };
    let branch = branch.clone();
    sched.resolve_condition(&node.id, &branch);
    let Some(fire) = sched.try_loop(&node.id, &branch) else {
        return;
    };
    let mut payload: HashMap<String, serde_json::Value> = HashMap::with_capacity(2);
    payload.insert("iteration".into(), serde_json::json!(fire.iteration));
    payload.insert("reset_nodes".into(), serde_json::json!(fire.reset_nodes));
    emitter.emit_node(EventType::NodeLoop, node.id.clone(), iteration, 1, payload);
    for reset_id in fire.reset_nodes {
        let entry = iterations.entry(reset_id).or_insert(1);
        *entry += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Node, Pos};
    use tempfile::TempDir;

    fn minimal_workflow() -> Workflow {
        Workflow {
            id: "wf1".into(),
            name: "minimal".into(),
            schema_version: 1,
            created_at: None,
            updated_at: None,
            variables: HashMap::new(),
            triggers: vec![],
            nodes: vec![Node {
                id: "n1".into(),
                ty: "delay".into(),
                name: String::new(),
                config: HashMap::from([("ms".into(), serde_json::json!(1))]),
                pos: Pos::default(),
                timeout_ms: None,
                retry: None,
                continue_on_error: false,
            }],
            edges: vec![],
        }
    }

    /// Regression test for the bug Codex flagged: `persist_node_outputs`
    /// hard-coded `attempt=1` when recording `node_outputs`. After a
    /// retry that succeeds on attempt 2 the `node:done` event reported
    /// `attempt=2` but the persisted row was still keyed under
    /// `(run, node, iteration, 1)`. With the fix the attempt threaded
    /// from `run_with_retry`'s return propagates into the row, so
    /// `runs show <id>` and the new wire-level subscribers can
    /// reconcile event attempt numbers against persisted output rows.
    #[test]
    fn persist_node_outputs_records_actual_attempt() {
        let dir = TempDir::new().unwrap();
        let pool = crate::db::open(dir.path().join("runs.db")).unwrap();
        let wf = minimal_workflow();
        let recorder =
            RunRecorder::start(pool.clone(), &wf, "{}", &HashMap::new(), "test").unwrap();
        let run_id = recorder.run_id.clone();

        // Seed the node_runs row for (iteration=1, attempt=2) so the
        // foreign-key on node_outputs is satisfied if the schema
        // enforces it.
        recorder
            .record_node_run(&NodeRunRow {
                node_id: "n1",
                iteration: 1,
                attempt: 2,
                node_type: "delay",
                status: "done",
                started_at: Some(0),
                finished_at: Some(1),
                duration_ms: Some(1),
                output_summary: None,
                error: None,
            })
            .unwrap();

        let mut outputs: crate::executor::NodeOutputs = HashMap::new();
        outputs.insert("text".to_string(), PortValue::String("hello".into()));
        let mut upstream: HashMap<(String, String), PortValue> = HashMap::new();
        persist_node_outputs(&recorder, &mut upstream, "n1", 1, 2, &outputs).unwrap();

        let conn = pool.get().unwrap();
        let attempt: u32 = conn
            .prepare(
                "SELECT attempt FROM node_outputs WHERE run_id=? AND node_id=? AND port_name=?",
            )
            .unwrap()
            .query_row(rusqlite::params![&run_id, "n1", "text"], |r| r.get(0))
            .unwrap();
        assert_eq!(
            attempt, 2,
            "persisted output's attempt column must reflect the attempt that produced it, not a hard-coded 1",
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn start_run_dispatches_minimal_workflow_to_done() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());
        let wf = Arc::new(minimal_workflow());

        let handle = engine
            .start_run(wf, HashMap::new(), "test", false)
            .expect("start_run");
        let summary = handle.join.await.expect("join").expect("run ok");
        assert_eq!(summary.status, "done");
        assert_eq!(summary.node_runs, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn three_node_chain_runs_in_order_with_three_node_runs_rows() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());

        let mk_delay = |id: &str| Node {
            id: id.into(),
            ty: "delay".into(),
            name: String::new(),
            config: HashMap::from([("ms".into(), serde_json::json!(5))]),
            pos: Pos::default(),
            timeout_ms: None,
            retry: None,
            continue_on_error: false,
        };
        let mk_edge = |id: &str, from: &str, to: &str| crate::types::Edge {
            id: id.into(),
            from_node_id: from.into(),
            from_port: "out".into(),
            to_node_id: to.into(),
            to_port: "in".into(),
            kind: EdgeType::Forward,
            max_iterations: None,
            branch: None,
        };
        let wf = Arc::new(Workflow {
            id: "wf_chain".into(),
            name: "chain".into(),
            schema_version: 1,
            created_at: None,
            updated_at: None,
            variables: HashMap::new(),
            triggers: vec![],
            nodes: vec![mk_delay("a"), mk_delay("b"), mk_delay("c")],
            edges: vec![mk_edge("e1", "a", "b"), mk_edge("e2", "b", "c")],
        });

        let summary = engine
            .run_workflow(wf, HashMap::new(), "test", false)
            .await
            .expect("run ok");
        assert_eq!(summary.status, "done");
        assert_eq!(summary.node_runs, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn start_run_rejects_concurrent_same_workflow() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());
        let wf = Arc::new(minimal_workflow());

        let h1 = engine
            .start_run(wf.clone(), HashMap::new(), "test", false)
            .expect("first run starts");
        let second = engine.start_run(wf.clone(), HashMap::new(), "test", false);
        match second {
            Err(EngineError::AlreadyRunning { .. }) => {},
            Ok(_) => panic!("expected AlreadyRunning, got Ok(RunHandle)"),
            Err(e) => panic!("expected AlreadyRunning, got Err({e})"),
        }
        h1.join.await.expect("join").expect("first run completes");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn failing_node_finalises_run_as_error() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());

        // delay node missing required `ms` → NodeError::Config → run = error.
        let wf = Arc::new(Workflow {
            id: "wf_fail".into(),
            name: "fail".into(),
            schema_version: 1,
            created_at: None,
            updated_at: None,
            variables: HashMap::new(),
            triggers: vec![],
            nodes: vec![Node {
                id: "broken".into(),
                ty: "delay".into(),
                name: String::new(),
                config: HashMap::new(),
                pos: Pos::default(),
                timeout_ms: None,
                retry: None,
                continue_on_error: false,
            }],
            edges: vec![],
        });

        let summary = engine
            .run_workflow(wf, HashMap::new(), "test", false)
            .await
            .expect("run completes (even on node failure)");
        assert_eq!(summary.status, "error");
        assert_eq!(summary.node_runs, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cancelled_run_finalises_as_stopped() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());

        // long delay so cancellation arrives before it finishes
        let wf = Arc::new(Workflow {
            id: "wf_cancel".into(),
            name: "cancel".into(),
            schema_version: 1,
            created_at: None,
            updated_at: None,
            variables: HashMap::new(),
            triggers: vec![],
            nodes: vec![Node {
                id: "slow".into(),
                ty: "delay".into(),
                name: String::new(),
                config: HashMap::from([("ms".into(), serde_json::json!(5_000))]),
                pos: Pos::default(),
                timeout_ms: None,
                retry: None,
                continue_on_error: false,
            }],
            edges: vec![],
        });

        let handle = engine
            .start_run(wf, HashMap::new(), "test", false)
            .expect("start_run");
        let run_id = handle.run_id.clone();
        // Let dispatch start before cancelling.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            engine.cancel_run(&run_id),
            "cancel_run should find the active run"
        );
        let summary = handle.join.await.expect("join").expect("run ok");
        assert_eq!(summary.status, "stopped");
    }

    /// Regression test for the bug Codex flagged: a per-attempt
    /// timeout that drops the executor future via
    /// `tokio::time::timeout` cancels its inner select on cancel
    /// without firing the subprocess executor's SIGTERM-then-SIGKILL
    /// arm. `kill_on_drop` masks the bug for direct shell sequences
    /// but a backgrounded grandchild reparents to init and keeps
    /// running. The fixed `dispatch_one_attempt` cancels the attempt
    /// token first, which routes through the executor's Unix arm
    /// (`kill(-pgid, SIGTERM)` → grace → `SIGKILL`) and reaches the
    /// whole process group. Skipped on non-Unix targets.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn timeout_actually_kills_subprocess_group() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());

        // Background shell touches the marker after the outer shell
        // would have been killed by `kill_on_drop` alone. Only a
        // pgid-wide SIGTERM/SIGKILL reaches it.
        let marker = dir.path().join("survived");
        let command = format!(r#"sh -c "sleep 0.5; touch '{}'" & wait"#, marker.display());

        let wf = Arc::new(Workflow {
            id: "wf_subproc_kill".into(),
            name: "subproc_kill".into(),
            schema_version: 1,
            created_at: None,
            updated_at: None,
            variables: HashMap::new(),
            triggers: vec![],
            nodes: vec![Node {
                id: "shell".into(),
                ty: "shell".into(),
                name: String::new(),
                config: HashMap::from([("command".into(), serde_json::json!(command))]),
                pos: Pos::default(),
                timeout_ms: Some(50),
                retry: None,
                continue_on_error: false,
            }],
            edges: vec![],
        });

        let summary = engine
            .run_workflow(wf, HashMap::new(), "test", false)
            .await
            .expect("run completes");
        assert_eq!(summary.status, "error", "expected timeout → run error");

        // Wait longer than the backgrounded `sleep 0.5` so any
        // pgid-survivor would have time to `touch` the marker.
        tokio::time::sleep(std::time::Duration::from_millis(800)).await;
        assert!(
            !marker.exists(),
            "subprocess group survived its timeout: \
             dispatch_one_attempt dropped the executor future before \
             the SIGTERM-to-pgid arm could fire",
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn per_node_timeout_short_circuits_long_delay() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());

        let wf = Arc::new(Workflow {
            id: "wf_timeout".into(),
            name: "timeout".into(),
            schema_version: 1,
            created_at: None,
            updated_at: None,
            variables: HashMap::new(),
            triggers: vec![],
            nodes: vec![Node {
                id: "slow".into(),
                ty: "delay".into(),
                name: String::new(),
                config: HashMap::from([("ms".into(), serde_json::json!(5_000))]),
                pos: Pos::default(),
                timeout_ms: Some(50),
                retry: None,
                continue_on_error: false,
            }],
            edges: vec![],
        });

        let start = std::time::Instant::now();
        let summary = engine
            .run_workflow(wf, HashMap::new(), "test", false)
            .await
            .expect("run completes (timeout is a node-level failure)");
        let elapsed = start.elapsed();
        assert_eq!(summary.status, "error");
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "timeout should fire within ~50ms, took {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn retry_on_timeout_exhausts_max_attempts() {
        // Subprocess non-zero exits are locked-in as Ok-with-exit_code
        // per Phase 6, so we can't trigger retry from a flaky shell.
        // A delay node with a tight timeout_ms gives every attempt a
        // real NodeError::Timeout, which retry_on=Both matches.
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());

        let wf = Arc::new(Workflow {
            id: "wf_retry_timeout".into(),
            name: "retry-timeout".into(),
            schema_version: 1,
            created_at: None,
            updated_at: None,
            variables: HashMap::new(),
            triggers: vec![],
            nodes: vec![Node {
                id: "flake".into(),
                ty: "delay".into(),
                name: String::new(),
                config: HashMap::from([("ms".into(), serde_json::json!(500))]),
                pos: Pos::default(),
                timeout_ms: Some(20),
                retry: Some(RetryPolicy {
                    max_attempts: 3,
                    backoff_ms: 5,
                    backoff_strategy: BackoffStrategy::Fixed,
                    retry_on: RetryOn::Both,
                }),
                continue_on_error: false,
            }],
            edges: vec![],
        });

        let summary = engine
            .run_workflow(wf, HashMap::new(), "test", false)
            .await
            .expect("run completes once retry budget is exhausted");
        assert_eq!(summary.status, "error");
        // 3 attempts → 3 node_runs rows (one per attempt for the same iteration).
        assert_eq!(summary.node_runs, 3);
    }

    #[test]
    fn compute_backoff_exponential_capped() {
        let policy = RetryPolicy {
            max_attempts: 99,
            backoff_ms: 100,
            backoff_strategy: BackoffStrategy::Exponential,
            retry_on: RetryOn::Error,
        };
        assert_eq!(compute_backoff(&policy, 1), Duration::from_millis(100));
        assert_eq!(compute_backoff(&policy, 2), Duration::from_millis(200));
        assert_eq!(compute_backoff(&policy, 3), Duration::from_millis(400));
        assert_eq!(compute_backoff(&policy, 30), BACKOFF_CAP);
    }

    #[test]
    fn retry_matches_honors_policy_kind() {
        assert!(retry_matches(
            RetryOn::Error,
            &NodeError::Subprocess("x".into())
        ));
        assert!(!retry_matches(RetryOn::Error, &NodeError::Timeout(10)));
        assert!(retry_matches(RetryOn::Timeout, &NodeError::Timeout(10)));
        assert!(!retry_matches(
            RetryOn::Timeout,
            &NodeError::Subprocess("x".into())
        ));
        assert!(retry_matches(RetryOn::Both, &NodeError::Timeout(10)));
        assert!(retry_matches(
            RetryOn::Both,
            &NodeError::Subprocess("x".into())
        ));
    }
}
