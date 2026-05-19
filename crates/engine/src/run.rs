//! `Engine::start_run` and `Engine::run_workflow` — the workflow
//! run entry points.
//!
//! `start_run` is non-async, validates + acquires the workflow
//! lock + spawns the run loop, returning a `RunHandle` the caller
//! can subscribe to or join. `run_workflow` is the convenience
//! wrapper that just awaits `handle.join`.
//!
//! The run loop itself (`run_loop_inner`) is a stub at this
//! checkpoint — it emits `workflow:stopped` and finalises the
//! recorder. Dispatch, condition / loop handling, and terminal
//! event selection arrive in subsequent commits.

use crate::emitter::Emitter;
use crate::events::{EventType, RunEvent};
use crate::executor::{Dispatcher, NodeExecutor, RunContext, wrap_process_env};
use crate::recorder::{NodeRunRow, RunRecorder};
use crate::scheduler::Scheduler;
use crate::types::{EdgeType, PortValue, Workflow};
use crate::{Engine, EngineError, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

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

    /// Scheduler-driven dispatch loop.
    ///
    /// For each batch of ready nodes:
    ///   1. Assemble `current_inputs` from incoming forward edges
    ///      against the run's authoritative `upstream_outputs`.
    ///   2. Insert a `node_runs` row with status `running`.
    ///   3. Dispatch via [`Dispatcher`].
    ///   4. On Ok: persist outputs + update the upstream map +
    ///      mark the row `done` + emit `node:done`. Then call
    ///      `sched.complete_node`.
    ///   5. On Err: mark the row `error` + emit `node:error`.
    ///      Honor `continue_on_error` by still calling
    ///      `complete_node` (logically done for scheduling).
    ///
    /// Condition / loop handling and terminal-event selection
    /// arrive in subsequent commits.
    #[allow(clippy::too_many_lines)] // dispatch + persistence + event emission all live here
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
                let started_at = chrono::Utc::now().timestamp_millis();
                sched.start_node(&node.id);
                recorder.record_node_run(&NodeRunRow {
                    node_id: &node.id,
                    iteration,
                    attempt: 1,
                    node_type: &node.ty,
                    status: "running",
                    started_at: Some(started_at),
                    finished_at: None,
                    duration_ms: None,
                    output_summary: None,
                    error: None,
                })?;
                node_runs_count += 1;
                emitter.emit_node(
                    EventType::NodeStarted,
                    node.id.clone(),
                    iteration,
                    1,
                    HashMap::new(),
                );

                let res = dispatcher.run(&node, &nt, &ctx, cancel.clone()).await;
                let finished_at = chrono::Utc::now().timestamp_millis();
                let duration = finished_at - started_at;

                match res {
                    Ok(outputs) => {
                        for (port_name, value) in &outputs {
                            let value_json = serde_json::to_string(value)
                                .map_err(|e| EngineError::Db(format!("encode output: {e}")))?;
                            recorder.record_node_output(
                                &node.id,
                                iteration,
                                1,
                                port_name,
                                Some(&value_json),
                                None,
                            )?;
                            upstream_outputs
                                .insert((node.id.clone(), port_name.clone()), value.clone());
                        }
                        recorder.record_node_run(&NodeRunRow {
                            node_id: &node.id,
                            iteration,
                            attempt: 1,
                            node_type: &node.ty,
                            status: "done",
                            started_at: Some(started_at),
                            finished_at: Some(finished_at),
                            duration_ms: Some(duration),
                            output_summary: None,
                            error: None,
                        })?;
                        emitter.emit_node(
                            EventType::NodeDone,
                            node.id.clone(),
                            iteration,
                            1,
                            HashMap::from([
                                ("finished_at".into(), serde_json::json!(finished_at)),
                                ("duration_ms".into(), serde_json::json!(duration)),
                            ]),
                        );
                        sched.complete_node(&node.id);

                        // condition node: route branch + maybe fire a loop.
                        if node.ty == "condition"
                            && let Some(PortValue::String(branch)) = outputs.get("branch")
                        {
                            let branch = branch.clone();
                            sched.resolve_condition(&node.id, &branch);
                            if let Some(fire) = sched.try_loop(&node.id, &branch) {
                                let mut payload: HashMap<String, serde_json::Value> =
                                    HashMap::with_capacity(2);
                                payload
                                    .insert("iteration".into(), serde_json::json!(fire.iteration));
                                payload.insert(
                                    "reset_nodes".into(),
                                    serde_json::json!(fire.reset_nodes.clone()),
                                );
                                emitter.emit_node(
                                    EventType::NodeLoop,
                                    node.id.clone(),
                                    iteration,
                                    1,
                                    payload,
                                );
                                for reset_id in fire.reset_nodes {
                                    let entry = iterations.entry(reset_id).or_insert(1);
                                    *entry += 1;
                                }
                            }
                        }
                    },
                    Err(e) => {
                        let msg = e.to_string();
                        recorder.record_node_run(&NodeRunRow {
                            node_id: &node.id,
                            iteration,
                            attempt: 1,
                            node_type: &node.ty,
                            status: "error",
                            started_at: Some(started_at),
                            finished_at: Some(finished_at),
                            duration_ms: Some(duration),
                            output_summary: None,
                            error: Some(&msg),
                        })?;
                        emitter.emit_node(
                            EventType::NodeError,
                            node.id.clone(),
                            iteration,
                            1,
                            HashMap::from([("error".into(), serde_json::json!(msg))]),
                        );
                        if node.continue_on_error {
                            sched.complete_node(&node.id);
                        } else {
                            sched.fail_node(&node.id);
                            had_unhandled_failure = true;
                        }
                    },
                }
            }

            // Drain freshly-skipped nodes between batches so events
            // arrive in roughly causal order (a condition that just
            // selected its branch immediately skips the other).
            for skipped_id in sched.drain_newly_skipped() {
                emitter.emit_node(EventType::NodeSkipped, skipped_id, 1, 1, HashMap::new());
            }
        }

        // Final flush in case nodes transitioned to Skipped on the
        // very last batch (or via fail_node's cascade).
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
}
