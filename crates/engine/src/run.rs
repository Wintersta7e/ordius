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
use crate::environment::runtime::env::WorkspaceBinding;
use crate::environment::runtime::workspace::{RunOutcome, RunScope, WorkspaceManager};
use crate::events::{EventType, RunEvent};
use crate::executor::builtins::condition::NODE_TYPE_ID as CONDITION_NODE_TYPE_ID;
use crate::executor::{Dispatcher, NodeError, NodeExecutor, RunContext, wrap_process_env};
use crate::recorder::{NodeRunRow, RunRecorder};
use crate::scheduler::Scheduler;
use crate::types::{
    BackoffStrategy, EdgeType, ExecutionBackend, PortValue, RetryOn, RetryPolicy, Workflow,
};
use crate::{Engine, EngineError, Result};
use futures::FutureExt; // `catch_unwind` on the run-loop future
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

/// Map a terminal `RunSummary::status` to a [`RunOutcome`].
fn outcome_from_status(status: &str) -> RunOutcome {
    match status {
        "stopped" => RunOutcome::CancelledByUser,
        "done" => RunOutcome::Completed,
        _ => RunOutcome::Failed,
    }
}

/// Run workspace teardown, isolating any panic from a transport dependency so
/// it cannot skip the run's sender/token/lock cleanup (which would leak the
/// workflow lock). `teardown_all` is panic-free by contract; this is
/// belt-and-suspenders for the cleanup path.
async fn isolate_teardown(wm: Arc<WorkspaceManager>, outcome: RunOutcome, run_id: &str) {
    if std::panic::AssertUnwindSafe(wm.teardown_all(outcome))
        .catch_unwind()
        .await
        .is_err()
    {
        tracing::error!(run_id, "workspace teardown panicked; continued run cleanup");
    }
}

/// RAII guard that drops the run's entry from `Engine::run_snapshots`
/// when the spawned run task returns or unwinds.
///
/// Held inside `tokio::spawn`'s future so any drop path — normal
/// completion, `?`-propagation, or a panic unwind — cleans up the
/// snapshot map. Without this, a panicking run loop would leak its
/// `Arc<RunSnapshot>` and pin every per-env catalog past run exit.
struct RunSnapshotGuard {
    engine: Arc<Engine>,
    run_id: String,
}

impl Drop for RunSnapshotGuard {
    fn drop(&mut self) {
        if let Ok(mut g) = self.engine.run_snapshots.lock() {
            g.remove(&self.run_id);
        }
    }
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
    /// Test-only seam: the run's `WorkspaceManager`, so tests can
    /// observe the `RunOutcome` handed to `teardown_all` after join.
    #[cfg(any(test, feature = "testing"))]
    pub workspace_manager: Arc<WorkspaceManager>,
}

impl Engine {
    /// Allocate a per-run scratch dir under `<home>/workspaces/`.
    /// `prefix` (empty for primary runs) distinguishes children. On
    /// create failure, fall back to engine home so subprocess CWDs
    /// stay valid instead of failing with `No such file or directory`.
    fn ensure_run_workspace(&self, run_id: &str, prefix: &str) -> PathBuf {
        let path = self
            .home()
            .join("workspaces")
            .join(format!("{prefix}{run_id}"));
        match std::fs::create_dir_all(&path) {
            Ok(()) => path,
            Err(e) => {
                tracing::warn!(
                    error = ?e, path = %path.display(),
                    "could not create run workspace; falling back to engine home",
                );
                self.home().to_path_buf()
            },
        }
    }

    /// Install the workflow scope, build the per-run snapshot, and create
    /// the recorder row while acquiring the workflow lock.
    ///
    /// Failures between the scope install and the recorder write
    /// restore the prior scope so a snapshot construction error
    /// (e.g. `EnvUnknown`) does not wipe a previously valid scope.
    fn prepare_run_snapshot_and_recorder(
        &self,
        wf: &Workflow,
        variables: &HashMap<String, String>,
        trigger_kind: &str,
        run_id: &str,
    ) -> Result<(
        Option<Arc<RunRecorder>>,
        Arc<crate::environment::runtime::RunSnapshot>,
    )> {
        // Capture the prior workflow scope before re-installing so a
        // failure between here and the recorder write can restore it.
        let workflow_id = crate::environment::runtime::WorkflowId(wf.id.clone());
        let prior_scope = crate::environment::runtime::snapshot_workflow_scope(
            &workflow_id,
            &self.resource_registry,
        );

        // Best-effort rollback when a subsequent step fails. The
        // discarded result is fine — the rollback only matters when the
        // underlying error has already been packaged into the EngineError
        // we're about to return.
        let restore_prior_scope = || {
            drop(crate::environment::runtime::install_workflow_resources(
                &workflow_id,
                &prior_scope,
                &self.resource_registry,
            ));
        };

        // Safety net: ensure the workflow's `resources:` block is installed
        // under `ScopeKey::Workflow { id }` before any node executes.
        // Production callers funnel through `load_workflow_for_run`, but
        // programmatic `Workflow` construction (tests, future entry points)
        // bypasses that path and would otherwise hit a "resource not in
        // registry" failure at dispatch time. Replace-then-install keeps
        // re-running an already-loaded workflow idempotent.
        if let Err(e) = crate::environment::runtime::install_workflow_resources(
            &workflow_id,
            &wf.resources,
            &self.resource_registry,
        ) {
            restore_prior_scope();
            return Err(e.into());
        }

        let envs_in_scope = wf.envs_in_scope();
        let default_env = wf
            .default_env
            .clone()
            .unwrap_or_else(crate::environment::runtime::EnvId::local);
        let run_snapshot =
            match self.build_run_snapshot(run_id, workflow_id.clone(), default_env, &envs_in_scope)
            {
                Ok(snap) => snap,
                Err(e) => {
                    restore_prior_scope();
                    return Err(e);
                },
            };

        let rec = match RunRecorder::start_with_run_id_and_lock(
            self.pool(),
            wf,
            "{}",
            variables,
            trigger_kind,
            run_id,
        ) {
            Ok(Some(r)) => Some(Arc::new(r)),
            Ok(None) => {
                restore_prior_scope();
                None
            },
            Err(e) => {
                restore_prior_scope();
                return Err(e);
            },
        };

        Ok((rec, run_snapshot))
    }

    /// Start a workflow run. Returns immediately with a
    /// [`RunHandle`]; the run loop spawns on tokio.
    ///
    /// Errors:
    /// - [`EngineError::Validation`] if the workflow fails
    ///   structural validation.
    /// - [`EngineError::AlreadyRunning`] if another run already
    ///   holds the workflow's lock.
    // Threading the root run_cancel into `run_loop_inner` pushed this one
    // line past clippy's limit; the body is already as flat as it gets.
    #[allow(clippy::too_many_lines)]
    pub fn start_run(
        self: &Arc<Self>,
        wf: Arc<Workflow>,
        variables: HashMap<String, String>,
        trigger_kind: &str,
        auto_resume_checkpoints: bool,
        workspace_override: Option<std::path::PathBuf>,
    ) -> Result<RunHandle> {
        crate::validation::validate(&wf)?;

        // Mint the run id eagerly so the snapshot can be keyed on it
        // before any DB write. No `runs` row exists yet.
        let run_id = RunRecorder::generate_run_id();
        let (rec, run_snapshot) =
            self.prepare_run_snapshot_and_recorder(&wf, &variables, trigger_kind, &run_id)?;
        let Some(rec) = rec else {
            return Err(EngineError::AlreadyRunning {
                id: wf.id.clone(),
                run_id,
            });
        };

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

        em.emit(
            EventType::WorkflowStarted,
            None,
            None,
            None,
            HashMap::from([
                ("workflow_id".into(), serde_json::json!(wf.id)),
                ("workflow_name".into(), serde_json::json!(wf.name)),
                ("trigger_kind".into(), serde_json::json!(trigger_kind)),
            ]),
        );

        let workspace =
            workspace_override.unwrap_or_else(|| self.ensure_run_workspace(&rec.run_id, ""));

        // Insert SYNCHRONOUSLY before `tokio::spawn` so any subscriber
        // that calls `Engine::run_snapshot(run_id)` immediately after
        // `start_run` returns observes the entry. Inserting from inside
        // the spawned task would race the subscriber to a `None` read.
        self.run_snapshots
            .lock()
            .expect("engine run_snapshots mutex poisoned")
            .insert(rec.run_id.clone(), Arc::clone(&run_snapshot));

        let run_id = rec.run_id.clone();
        let engine = Arc::clone(self);
        let snap_run_id = rec.run_id.clone();
        let workspace_manager = Arc::new(WorkspaceManager::new());
        // Clones for the teardown path: the manager (moved into the run
        // loop) and the cancel token (used to classify a panic that
        // unwinds before a `RunSummary` is produced).
        let wm_teardown = Arc::clone(&workspace_manager);
        let cancel_for_outcome = cancel.clone();
        #[cfg(any(test, feature = "testing"))]
        let wm_handle = Arc::clone(&workspace_manager);
        let join = tokio::spawn(async move {
            // RAII guard: removes the `run_snapshots` entry on Drop, so
            // a panicking run loop or `?`-propagation also cleans up.
            let _snap_guard = RunSnapshotGuard {
                engine: Arc::clone(&engine),
                run_id: snap_run_id,
            };
            // Run the loop under `catch_unwind` so teardown fires on a
            // panic too. `AssertUnwindSafe` is sound here: on a panic we
            // re-`resume_unwind` rather than observing the (possibly
            // inconsistent) captured state, so behaviour is unchanged.
            let caught = std::panic::AssertUnwindSafe(engine.run_loop_inner(
                wf,
                rec.clone(),
                em,
                workspace,
                variables,
                // Local cancel == root run_cancel at top level; they diverge
                // only inside child workflows (see `run_child_workflow`).
                cancel.clone(),
                cancel,
                auto_resume_checkpoints,
                0,
                run_snapshot,
                workspace_manager,
            ))
            .catch_unwind()
            .await;

            // Classify the outcome for teardown, then run teardown
            // (workspace write-back + ephemeral cleanup) BEFORE the
            // sender/token/lock cleanup. `teardown_all` is best-effort
            // and panic-free.
            let outcome = match &caught {
                Ok(Ok((summary, _outputs))) => outcome_from_status(&summary.status),
                Ok(Err(_)) => RunOutcome::Failed,
                Err(_) => {
                    if cancel_for_outcome.is_cancelled() {
                        RunOutcome::CancelledByUser
                    } else {
                        RunOutcome::Failed
                    }
                },
            };
            isolate_teardown(wm_teardown, outcome, &rec.run_id).await;

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
            drop(rec.release_lock());

            // Re-propagate a panic so the join handle still reports it;
            // otherwise hand back the run loop's `Result<RunSummary>`.
            match caught {
                Ok(res) => res.map(|(summary, _outputs)| summary),
                Err(panic) => std::panic::resume_unwind(panic),
            }
        });

        Ok(RunHandle {
            run_id,
            event_rx: rx,
            join,
            #[cfg(any(test, feature = "testing"))]
            workspace_manager: wm_handle,
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
        workspace_override: Option<std::path::PathBuf>,
    ) -> Result<RunSummary> {
        let handle = self.start_run(
            wf,
            variables,
            trigger_kind,
            auto_resume_checkpoints,
            workspace_override,
        )?;
        handle
            .join
            .await
            .map_err(|e| EngineError::Db(format!("run task join: {e}")))?
    }

    /// Run another workflow as a sub-frame of the current run.
    ///
    /// Caller (the `compose` built-in) supplies the child workflow,
    /// explicit variable map, a parent cancellation token, and the
    /// depth this child sits at. Returns the child's run summary plus
    /// the final `upstream_outputs` map so the caller can extract
    /// specific outputs from sink nodes.
    ///
    /// The child gets its own `run_id` + recorder row (separate
    /// `runs` entry) but reuses the engine's pool, registry,
    /// secrets store, checkpoint registry, and event registry. The
    /// workflow lock is intentionally not acquired so the same
    /// child workflow can be composed in parallel from a parent.
    ///
    /// `parent_snapshot` is inherited rather than rebuilt so the
    /// dispatchers + catalogs visible to the child match what the
    /// parent run started with. Per-env validation against the
    /// child workflow's `target_env`s lands in a later task.
    #[allow(clippy::too_many_arguments)]
    pub async fn run_child_workflow(
        self: &Arc<Self>,
        child_wf: Arc<Workflow>,
        variables: HashMap<String, String>,
        parent_cancel: &CancellationToken,
        run_cancel: CancellationToken,
        compose_depth: u32,
        workspace_override: Option<PathBuf>,
        scratch_prefix: &str,
        parent_snapshot: Arc<crate::environment::runtime::RunSnapshot>,
        workspace_manager: Arc<WorkspaceManager>,
    ) -> Result<(RunSummary, HashMap<(String, String), PortValue>)> {
        crate::validation::validate(&child_wf)?;

        // BL-H check 1: child workflows cannot carry their own `resources:`
        // block in Phase E. Children inherit the parent's frozen registry
        // snapshot; a child-side overlay would require rebuilding the
        // snapshot mid-run (or registry-overlay-on-overlay), both deferred.
        if !child_wf.resources.is_empty() {
            return Err(EngineError::ChildResourcesUnsupported {
                parent_run_id: parent_snapshot.run_id.clone(),
                child_workflow_id: child_wf.id.clone(),
                resource_count: child_wf.resources.len(),
            });
        }

        // BL-H check 2: every env the child references must be in the
        // parent's snapshot. `envs_in_scope()` walks node `target_env`s,
        // `default_env`, and adds `local` for any `http`/`llm` node with
        // `origin: host` (those route through the host loopback dispatcher,
        // which always lives at `EnvId::local()`).
        let parent_envs: std::collections::HashSet<&crate::environment::runtime::EnvId> =
            parent_snapshot.dispatchers.keys().collect();
        for env in child_wf.envs_in_scope() {
            if !parent_envs.contains(&env) {
                return Err(EngineError::ChildEnvNotInScope {
                    parent_run_id: parent_snapshot.run_id.clone(),
                    child_workflow_id: child_wf.id.clone(),
                    env_id: env,
                });
            }
        }

        let rec = Arc::new(RunRecorder::start(
            self.pool(),
            &child_wf,
            "{}",
            &variables,
            "compose",
        )?);
        let (em, _rx) = Emitter::new(rec.clone());
        let em = Arc::new(em);
        em.emit(
            EventType::WorkflowStarted,
            None,
            None,
            None,
            HashMap::from([
                ("workflow_id".into(), serde_json::json!(child_wf.id)),
                ("workflow_name".into(), serde_json::json!(child_wf.name)),
                ("trigger_kind".into(), serde_json::json!("compose")),
                ("compose_depth".into(), serde_json::json!(compose_depth)),
            ]),
        );
        // Inherit parent workspace when provided; else allocate a
        // per-child scratch dir distinguishable from primary runs
        // by the caller-supplied prefix (`compose-`, `parallel-`).
        let workspace = workspace_override
            .unwrap_or_else(|| self.ensure_run_workspace(&rec.run_id, scratch_prefix));
        let child_cancel = parent_cancel.child_token();
        self.run_loop_inner(
            child_wf,
            rec,
            em,
            workspace,
            variables,
            // `child_cancel` is the LOCAL token (a child of the
            // fail-fast/timeout parent); `run_cancel` is the run's ROOT
            // token, forwarded UNCHANGED so write-back-skip logic sees
            // only a genuine user cancel.
            child_cancel,
            run_cancel,
            false,
            compose_depth,
            parent_snapshot,
            workspace_manager,
        )
        .await
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
    #[allow(clippy::too_many_arguments)]
    async fn run_loop_inner(
        self: &Arc<Self>,
        wf: Arc<Workflow>,
        recorder: Arc<RunRecorder>,
        emitter: Arc<Emitter>,
        workspace: PathBuf,
        variables: HashMap<String, String>,
        cancel: CancellationToken,
        run_cancel: CancellationToken,
        auto_resume: bool,
        compose_depth: u32,
        run_snapshot: Arc<crate::environment::runtime::RunSnapshot>,
        workspace_manager: Arc<WorkspaceManager>,
    ) -> Result<(RunSummary, HashMap<(String, String), PortValue>)> {
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

            for mut node in ready {
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

                // Apply config-level template substitution uniformly so every
                // built-in's string fields can reference {{vars.X}} /
                // {{inputs.X}} / {{nodes.A.outputs.B}} etc. without the
                // executor opting in. Strings without `{{` skip the parser
                // (no cost). NodeTypes can opt out via
                // `skip_config_templates` when their config holds deferred
                // templates (the `agent` built-in's body_template, etc.).
                // Wrapped in a sync helper to keep non-Send &dyn Fn
                // closures from tainting the run-loop future.
                let nt_for_skip = self.registry().get(&node.ty);
                let skip = nt_for_skip
                    .as_ref()
                    .is_some_and(|n| n.skip_config_templates);
                let template_result = if skip {
                    Ok(node.config.clone())
                } else {
                    let effective_env = node
                        .target_env
                        .clone()
                        .unwrap_or_else(|| run_snapshot.default_env.clone());
                    substitute_node_config(
                        &node.config,
                        &variables,
                        &upstream_outputs,
                        &current_inputs,
                        &self.secrets_store(),
                        &emitter,
                        &run_snapshot,
                        &effective_env,
                        &recorder.run_id,
                        &workspace,
                        &wf.id,
                        &wf.name,
                    )
                };
                match template_result {
                    Ok(resolved) => node.config = resolved,
                    Err(e) => {
                        emitter.emit_node(
                            EventType::NodeError,
                            node.id.clone(),
                            iterations.get(&node.id).copied().unwrap_or(1),
                            1,
                            HashMap::from([(
                                "error".into(),
                                serde_json::json!(format!("template: {e}")),
                            )]),
                        );
                        sched.fail_node(&node.id);
                        if !node.continue_on_error {
                            had_unhandled_failure = true;
                        }
                        continue;
                    },
                }

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

                let ctx = RunContext {
                    run_id: recorder.run_id.clone(),
                    workflow_id: wf.id.clone(),
                    workflow_name: wf.name.clone(),
                    started_at_iso: String::new(),
                    workspace: workspace.clone(),
                    variables: variables.clone(),
                    recorder: recorder.clone(),
                    emitter: emitter.clone(),
                    secrets_store: Some(self.secrets_store()),
                    env: wrap_process_env(),
                    current_inputs,
                    upstream_outputs: upstream_outputs.clone(),
                    checkpoints: self.checkpoints(),
                    events: self.events(),
                    run_snapshot: Arc::clone(&run_snapshot),
                    engine: Arc::downgrade(self),
                    compose_depth,
                    iteration,
                    attempt: std::sync::atomic::AtomicU32::new(1),
                    auto_resume,
                    workspace_manager: Arc::clone(&workspace_manager),
                    env_cwd: parking_lot::Mutex::new(None),
                    run_cancel: run_cancel.clone(),
                };

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
                            &emitter,
                            &mut upstream_outputs,
                            &node.id,
                            iteration,
                            attempt,
                            &outputs,
                            self.home(),
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

        Ok((
            RunSummary {
                run_id: recorder.run_id.clone(),
                status: status.into(),
                node_runs: node_runs_count,
            },
            upstream_outputs,
        ))
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
///
/// For a `Subprocess`-backend node bound to a `Sync` workspace this also owns
/// per-node workspace reconciliation: a per-key execution lease (so two nodes
/// sharing the same `(env, host_ws)` never reset it concurrently), a
/// `reconcile_in` reset before each attempt (records `ctx.env_cwd`), and a
/// `reconcile_out` write-back after the final attempt. Non-`Sync`/non-`Subprocess`
/// nodes take no lease and `reconcile_in` degrades to a plain `translate_path`.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
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
    let effective_env = node
        .target_env
        .clone()
        .unwrap_or_else(|| ctx.run_snapshot.default_env.clone());
    let binding = ctx.run_snapshot.workspace_binding(&effective_env);
    let is_subprocess = nt.execution.backend == ExecutionBackend::Subprocess;
    let synced = is_subprocess && matches!(binding, WorkspaceBinding::Sync { .. });
    // Held across every attempt + the post-loop `reconcile_out`; dropped on
    // each `return`. `None` (no lease) for non-synced nodes.
    //
    // The lease key `(effective_env, ctx.workspace)` matches the `WorkspaceState`
    // key (which `reconcile_in` builds as `(dispatcher.info().id, host_ws)`) only
    // because `ctx.run_snapshot.dispatcher(effective_env).info().id == effective_env`
    // — the snapshot maps each EnvId to a dispatcher reporting that same id. The
    // lease and the per-key state therefore serialise on the same key.
    let _lease = if synced {
        Some(
            ctx.workspace_manager
                .acquire_execution_lease((effective_env.clone(), ctx.workspace.clone()))
                .await,
        )
    } else {
        None
    };
    let run_scope = RunScope {
        run_id: &ctx.run_id,
        workflow_id: &ctx.workflow_id,
        workflow_name: &ctx.workflow_name,
        started_at_iso: &ctx.started_at_iso,
    };

    let mut attempt: u32 = 1;
    loop {
        // Publish the current attempt to the shared `RunContext` so
        // executors that emit intra-attempt events (subprocess
        // line-by-line stdout, `llm` SSE deltas, `checkpoint` pauses)
        // report the same `attempt` the run loop will persist for
        // this attempt's `node_runs` row.
        ctx.attempt
            .store(attempt, std::sync::atomic::Ordering::Relaxed);
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
            HashMap::from([
                ("node_type".into(), serde_json::json!(node.ty)),
                ("started_at".into(), serde_json::json!(started_at)),
            ]),
        );

        // Child token so a timeout for this attempt doesn't cancel
        // the run-wide token (which would prevent further retries).
        let attempt_cancel = cancel.child_token();
        let res = dispatch_attempt(
            dispatcher,
            node,
            nt,
            ctx,
            is_subprocess,
            &effective_env,
            &binding,
            &run_scope,
            attempt_cancel,
            timeout_ms,
        )
        .await;
        let finished_at = chrono::Utc::now().timestamp_millis();

        let should_retry = match &res {
            Ok(_) => false,
            Err(_) if cancel.is_cancelled() => false,
            Err(e) => attempt < policy.max_attempts && retry_matches(policy.retry_on, e),
        };

        if !should_retry {
            // Final attempt for a synced subprocess node: write the remote delta
            // back to the host. A timed-out or fail-fasted node still writes back
            // (those cancel only the local `cancel`/`attempt_cancel`, not the
            // run's root token); ONLY a genuine user cancel (`ctx.run_cancel`)
            // skips. A write-back failure replaces `res` with a `Subprocess` error.
            if synced
                && !ctx.run_cancel.is_cancelled()
                && let Err(e) = reconcile_out_final(ctx, &binding, &effective_env).await
            {
                return Ok((Err(e), attempt, started_at, finished_at));
            }
            return Ok((res, attempt, started_at, finished_at));
        }

        // Mid-retry: persist this attempt's failure + emit node:retry.
        persist_attempt_failure(
            recorder,
            emitter,
            node,
            iteration,
            attempt,
            started_at,
            finished_at,
            &res,
        )?;

        let delay = compute_backoff(policy, attempt);
        tokio::select! {
            () = tokio::time::sleep(delay) => {}
            () = cancel.cancelled() => {
                // A fail-fast/timeout (local or group cancel, NOT a user cancel)
                // during the retry backoff still ends the node — but it must write
                // the remote delta back first, exactly like the final-attempt path
                // above. ONLY a genuine user cancel (`ctx.run_cancel`) skips.
                if synced
                    && !ctx.run_cancel.is_cancelled()
                    && let Err(e) = reconcile_out_final(ctx, &binding, &effective_env).await
                {
                    return Ok((Err(e), attempt, started_at, finished_at));
                }
                return Ok((Err(NodeError::Cancelled), attempt, started_at, finished_at));
            }
        }
        attempt += 1;
    }
}

/// Record this attempt's failing `node_runs` row and emit `node:retry`.
/// Factored out of `run_with_retry`'s loop so the mid-retry bookkeeping doesn't
/// inflate the driver.
#[allow(clippy::too_many_arguments)]
fn persist_attempt_failure(
    recorder: &RunRecorder,
    emitter: &Emitter,
    node: &crate::types::Node,
    iteration: u32,
    attempt: u32,
    started_at: i64,
    finished_at: i64,
    res: &std::result::Result<crate::executor::NodeOutputs, NodeError>,
) -> Result<()> {
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
    Ok(())
}

/// Dispatch one attempt of `node`. For a `Subprocess`-backend node this first
/// resolves the per-env dispatcher from the run snapshot and runs `reconcile_in`
/// (a remote-reset for a `Sync` workspace, a plain `translate_path` otherwise),
/// recording the env-side cwd on the context before the executor reads it. A
/// missing env-dispatcher or a `reconcile_in` failure is returned as an inner
/// `NodeError` so the caller's retry / continue-on-error logic still applies —
/// never a hard early return. `dispatcher` (the executor router) is distinct
/// from the snapshot's per-env dispatcher used only for reconcile.
#[allow(clippy::too_many_arguments)]
async fn dispatch_attempt(
    dispatcher: &Dispatcher,
    node: &crate::types::Node,
    nt: &crate::types::NodeType,
    ctx: &RunContext,
    is_subprocess: bool,
    effective_env: &crate::environment::runtime::EnvId,
    binding: &WorkspaceBinding,
    run_scope: &RunScope<'_>,
    attempt_cancel: CancellationToken,
    timeout_ms: Option<u64>,
) -> std::result::Result<crate::executor::NodeOutputs, NodeError> {
    if !is_subprocess {
        return dispatch_one_attempt(dispatcher, node, nt, ctx, attempt_cancel, timeout_ms).await;
    }
    let Some(env_dispatcher) = ctx.run_snapshot.dispatcher(effective_env).cloned() else {
        return Err(NodeError::Config(format!(
            "env '{}' not in run snapshot scope",
            effective_env.as_str()
        )));
    };
    match ctx
        .workspace_manager
        .reconcile_in(env_dispatcher.as_ref(), binding, &ctx.workspace, run_scope)
        .await
    {
        Ok(cwd) => {
            ctx.set_env_cwd(cwd);
            dispatch_one_attempt(dispatcher, node, nt, ctx, attempt_cancel, timeout_ms).await
        },
        Err(e) => Err(NodeError::Subprocess(format!("reconcile_in: {e}"))),
    }
}

/// Write back the remote workspace delta for a synced subprocess node after its
/// final attempt. A no-op when the env-dispatcher is absent from the snapshot;
/// surfaces a `reconcile_out` failure as a `NodeError` for the caller to record.
async fn reconcile_out_final(
    ctx: &RunContext,
    binding: &WorkspaceBinding,
    effective_env: &crate::environment::runtime::EnvId,
) -> std::result::Result<(), NodeError> {
    let Some(env_dispatcher) = ctx.run_snapshot.dispatcher(effective_env).cloned() else {
        return Ok(());
    };
    ctx.workspace_manager
        .reconcile_out(env_dispatcher.as_ref(), binding, &ctx.workspace)
        .await
        .map_err(|e| NodeError::Subprocess(format!("reconcile_out: {e}")))
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
///
/// Persisted values are redacted against the emitter's accumulated
/// secret values the same way `node:output` events are — otherwise
/// a `{{secrets.X}}` substitution that lands on a port would leak
/// the raw secret into `node_outputs.value_inline`.
/// Outputs that serialise larger than this byte count get spilled to
/// disk rather than inlined into the `node_outputs.value_inline`
/// column. The threshold is per-spec ("v1.x outputs > 8 KB spill to
/// disk"). The dispatch loop still holds the full value in
/// `upstream_outputs` so downstream nodes see no difference; spill is
/// purely a persistence / history-viewer concern.
const OUTPUT_SPILL_THRESHOLD_BYTES: usize = 8 * 1024;

fn persist_node_outputs(
    recorder: &RunRecorder,
    emitter: &Emitter,
    upstream_outputs: &mut HashMap<(String, String), PortValue>,
    node_id: &str,
    iteration: u32,
    attempt: u32,
    outputs: &crate::executor::NodeOutputs,
    home: &std::path::Path,
) -> Result<()> {
    let secrets = emitter.accumulated_secrets();
    for (port_name, value) in outputs {
        let value_for_storage = if secrets.is_empty() {
            value.clone()
        } else {
            redact_port_value(value, &secrets)
        };
        let value_json = serde_json::to_string(&value_for_storage)
            .map_err(|e| EngineError::Db(format!("encode output: {e}")))?;
        let spilled_path = maybe_spill_output(
            home,
            &recorder.run_id,
            node_id,
            port_name,
            iteration,
            attempt,
            &value_json,
        );
        match spilled_path {
            Some(path) => {
                recorder.record_node_output(
                    node_id,
                    iteration,
                    attempt,
                    port_name,
                    None,
                    Some(&path),
                )?;
            },
            None => {
                recorder.record_node_output(
                    node_id,
                    iteration,
                    attempt,
                    port_name,
                    Some(&value_json),
                    None,
                )?;
            },
        }
        // upstream_outputs feeds downstream executors verbatim — we
        // need the original (un-redacted) value there, otherwise a
        // chained transform that consumes the secret can't see it.
        // Redaction is a presentation/persistence concern only.
        upstream_outputs.insert((node_id.to_string(), port_name.clone()), value.clone());
    }
    Ok(())
}

/// If `value_json` exceeds [`OUTPUT_SPILL_THRESHOLD_BYTES`], write it
/// to `<home>/output-cache/<run_id>/<node>-<port>-<iter>-<attempt>.json`
/// and return the absolute path as a string. Returns `None` for small
/// values (keep inline) or when the write fails (fall back to inline —
/// the recorder won't crash the run if disk is full).
fn maybe_spill_output(
    home: &std::path::Path,
    run_id: &str,
    node_id: &str,
    port: &str,
    iteration: u32,
    attempt: u32,
    value_json: &str,
) -> Option<String> {
    if value_json.len() <= OUTPUT_SPILL_THRESHOLD_BYTES {
        return None;
    }
    let dir = home.join("output-cache").join(run_id);
    if let Err(e) = std::fs::create_dir_all(&dir) {
        tracing::warn!(
            error = ?e, path = %dir.display(),
            "output spill: could not create cache dir; falling back to inline",
        );
        return None;
    }
    let safe_node = sanitise_for_filename(node_id);
    let safe_port = sanitise_for_filename(port);
    let path = dir.join(format!(
        "{safe_node}-{safe_port}-{iteration}-{attempt}.json"
    ));
    if let Err(e) = std::fs::write(&path, value_json) {
        tracing::warn!(
            error = ?e, path = %path.display(),
            "output spill: write failed; falling back to inline",
        );
        return None;
    }
    Some(path.to_string_lossy().into_owned())
}

fn sanitise_for_filename(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Apply `redact_secrets` to every string anywhere inside a
/// `PortValue`. Numbers / bools / null are returned verbatim; JSON
/// values walk recursively. The recursion depth is bounded by
/// `serde_json`'s own depth limit, so adversarial inputs can't blow
/// the stack here any more than they can during deserialisation.
fn redact_port_value(value: &PortValue, secrets: &[(String, String)]) -> PortValue {
    match value {
        PortValue::String(s) => PortValue::String(crate::secrets::redact_secrets(s, secrets)),
        PortValue::Json(j) => PortValue::Json(redact_json_value(j, secrets)),
        other => other.clone(),
    }
}

fn redact_json_value(value: &serde_json::Value, secrets: &[(String, String)]) -> serde_json::Value {
    match value {
        serde_json::Value::String(s) => {
            serde_json::Value::String(crate::secrets::redact_secrets(s, secrets))
        },
        serde_json::Value::Array(items) => serde_json::Value::Array(
            items
                .iter()
                .map(|i| redact_json_value(i, secrets))
                .collect(),
        ),
        serde_json::Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, v) in map {
                out.insert(k.clone(), redact_json_value(v, secrets));
            }
            serde_json::Value::Object(out)
        },
        other => other.clone(),
    }
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

/// Sync helper that builds a `SubstitutionContext` and applies it to a
/// node's config in a single stack frame — keeps the non-Send `&dyn Fn`
/// resolvers from tainting the surrounding async run-loop future.
#[allow(clippy::too_many_arguments)]
fn substitute_node_config(
    config: &HashMap<String, serde_json::Value>,
    variables: &HashMap<String, String>,
    upstream_outputs: &HashMap<(String, String), PortValue>,
    current_inputs: &HashMap<String, PortValue>,
    secrets_store: &Arc<crate::secrets::Store>,
    emitter: &Arc<crate::emitter::Emitter>,
    run_snapshot: &Arc<crate::environment::runtime::RunSnapshot>,
    effective_env: &crate::environment::runtime::EnvId,
    run_id: &str,
    workspace: &std::path::Path,
    workflow_id: &str,
    workflow_name: &str,
) -> std::result::Result<HashMap<String, serde_json::Value>, crate::template::TemplateError> {
    let secrets_resolver = |name: &str| -> Option<String> {
        let value = secrets_store.get(name).ok()?;
        emitter.register_secret(name.to_string(), value.clone());
        Some(value)
    };
    let kv_resolver = |_: &str| None;
    let env_resolver = wrap_process_env();
    let env_allow = crate::template::default_env_allowlist();
    let resources_resolver = crate::template::build_run_snapshot_resources_resolver(
        Arc::clone(&run_snapshot.registry),
        run_snapshot.workflow_id.clone(),
        effective_env.clone(),
        Arc::clone(&run_snapshot.catalogs),
    );
    let original_config = config.clone();
    let sub_ctx = crate::template::SubstitutionContext {
        vars: variables,
        secrets: &secrets_resolver,
        upstream_outputs,
        current_inputs,
        current_config: &original_config,
        kv: &kv_resolver,
        env: &*env_resolver,
        env_allowlist: &env_allow,
        resources: &resources_resolver,
        run_id,
        workspace,
        started_at_iso: "",
        workflow_id,
        workflow_name,
    };
    crate::template::substitute_in_config(&original_config, &sub_ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::environment::runtime::{
        ProbeSpec, ResourceDefinition, ResourceId, ResourceKind, ScopeKey, WorkflowId,
    };
    use crate::types::{Node, Pos};
    use tempfile::TempDir;

    fn test_resource(id: &str) -> ResourceDefinition {
        ResourceDefinition {
            id: ResourceId(id.into()),
            kind: ResourceKind::HttpEndpoint,
            advertised_capabilities: vec![],
            probe: ProbeSpec::Http {
                ports: vec![9999],
                routes: vec![],
                timeout_ms: None,
            },
            override_lower_scope: false,
        }
    }

    fn blocking_workflow_with_resource(resource_id: &str) -> Workflow {
        Workflow {
            id: "wf_a".into(),
            name: format!("workflow with {resource_id}"),
            schema_version: 1,
            created_at: None,
            updated_at: None,
            variables: HashMap::new(),
            triggers: vec![],
            nodes: vec![Node {
                id: "slow".into(),
                ty: "delay".into(),
                name: String::new(),
                config: HashMap::from([("ms".into(), serde_json::json!(60_000))]),
                pos: Pos::default(),
                timeout_ms: None,
                retry: None,
                continue_on_error: false,
                target_env: None,
            }],
            edges: vec![],
            resources: vec![test_resource(resource_id)],
            default_env: None,
        }
    }

    fn workflow_scope_resource_visible(
        engine: &Engine,
        workflow_id: &str,
        resource_id: &str,
    ) -> bool {
        engine
            .resource_registry
            .snapshot()
            .layers
            .get(&ScopeKey::Workflow {
                id: WorkflowId(workflow_id.into()),
            })
            .is_some_and(|layer| layer.contains_key(&ResourceId(resource_id.into())))
    }

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
                target_env: None,
            }],
            edges: vec![],
            resources: vec![],
            default_env: None,
        }
    }

    /// Regression test for Codex finding G: the `auto_resume` flag
    /// passed to `start_run` must reach `CheckpointExecutor` so
    /// `ordius run --yes` actually short-circuits checkpoint nodes
    /// without a per-node `config.auto_resume`. Run loop name was
    /// `_auto_resume`, the value was discarded, and the executor
    /// only consulted node-level config — so the flag was a no-op
    /// from the CLI surface.
    #[tokio::test(flavor = "multi_thread")]
    async fn run_level_auto_resume_short_circuits_checkpoint() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());

        // Single checkpoint node WITHOUT config.auto_resume — the
        // only thing that should let this finish is run-level
        // auto_resume reaching the executor. Without the fix the
        // node would park forever waiting for an external resume.
        let wf = Arc::new(Workflow {
            id: "wf_autoresume".into(),
            name: "auto".into(),
            schema_version: 1,
            created_at: None,
            updated_at: None,
            variables: HashMap::new(),
            triggers: vec![],
            nodes: vec![Node {
                id: "cp".into(),
                ty: "checkpoint".into(),
                name: String::new(),
                config: HashMap::new(),
                pos: Pos::default(),
                timeout_ms: None,
                retry: None,
                continue_on_error: false,
                target_env: None,
            }],
            edges: vec![],
            resources: vec![],
            default_env: None,
        });

        let summary = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            engine.run_workflow(wf, HashMap::new(), "test", true, None),
        )
        .await
        .expect("must not park — auto_resume should short-circuit")
        .expect("run completes");
        assert_eq!(summary.status, "done");
    }

    /// Regression test for Codex finding D: `persist_node_outputs`
    /// must redact `PortValue::String` against the emitter's
    /// accumulated secrets, otherwise a `{{secrets.X}}` substitution
    /// that lands on a port would persist the raw value in
    /// `node_outputs.value_inline`. The emitter already redacts
    /// `node:output` events, but the recorder write path was
    /// silently bypassing redaction.
    #[test]
    fn persist_node_outputs_redacts_secrets_in_string_ports() {
        let dir = TempDir::new().unwrap();
        let pool = crate::db::open(dir.path().join("runs.db")).unwrap();
        let wf = minimal_workflow();
        let recorder =
            RunRecorder::start(pool.clone(), &wf, "{}", &HashMap::new(), "test").unwrap();
        let run_id = recorder.run_id.clone();
        recorder
            .record_node_run(&NodeRunRow {
                node_id: "n1",
                iteration: 1,
                attempt: 1,
                node_type: "transform",
                status: "done",
                started_at: Some(0),
                finished_at: Some(1),
                duration_ms: Some(1),
                output_summary: None,
                error: None,
            })
            .unwrap();
        let (em, _rx) = Emitter::new(Arc::new(recorder));
        em.register_secret("MY_KEY".into(), "deadbeef".into());

        // Output port carries a value that contains the secret as a
        // substring. The persisted value_inline must come out
        // redacted; upstream_outputs feeds downstream executors and
        // stays verbatim so dataflow keeps working.
        let mut outputs: crate::executor::NodeOutputs = HashMap::new();
        outputs.insert(
            "text".to_string(),
            PortValue::String("key=deadbeef trailing".into()),
        );
        let mut upstream: HashMap<(String, String), PortValue> = HashMap::new();
        let recorder2 =
            RunRecorder::start(pool.clone(), &wf, "{}", &HashMap::new(), "test").unwrap();
        let run_id2 = recorder2.run_id.clone();
        recorder2
            .record_node_run(&NodeRunRow {
                node_id: "n1",
                iteration: 1,
                attempt: 1,
                node_type: "transform",
                status: "done",
                started_at: Some(0),
                finished_at: Some(1),
                duration_ms: Some(1),
                output_summary: None,
                error: None,
            })
            .unwrap();
        persist_node_outputs(
            &recorder2,
            &em,
            &mut upstream,
            "n1",
            1,
            1,
            &outputs,
            dir.path(),
        )
        .unwrap();

        let conn = pool.get().unwrap();
        let stored: String = conn
            .prepare("SELECT value_inline FROM node_outputs WHERE run_id=? AND node_id=? AND port_name=?")
            .unwrap()
            .query_row(rusqlite::params![&run_id2, "n1", "text"], |r| r.get(0))
            .unwrap();
        assert!(
            !stored.contains("deadbeef"),
            "raw secret leaked into node_outputs row: {stored}",
        );
        assert!(
            stored.contains("<redacted:MY_KEY>"),
            "expected redaction marker, got: {stored}",
        );
        // upstream_outputs is the inter-node dataflow channel — it
        // must NOT be redacted, otherwise a chained transform that
        // consumes the secret would read the redaction marker.
        let still_raw = upstream
            .get(&("n1".to_string(), "text".to_string()))
            .unwrap();
        match still_raw {
            PortValue::String(s) => assert!(s.contains("deadbeef"), "upstream got redacted: {s}"),
            other => panic!("expected PortValue::String, got {other:?}"),
        }
        drop(run_id);
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
        let (em, _rx) = Emitter::new(Arc::new(
            RunRecorder::start(pool.clone(), &wf, "{}", &HashMap::new(), "test").unwrap(),
        ));
        persist_node_outputs(
            &recorder,
            &em,
            &mut upstream,
            "n1",
            1,
            2,
            &outputs,
            dir.path(),
        )
        .unwrap();

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

    /// Regression test for Codex finding E: subprocess / llm /
    /// checkpoint events were emitted with iteration=0 attempt=0
    /// from inside the executors, and workflow:started / node:started
    /// carried empty payloads. After the fix iteration + attempt
    /// flow through `RunContext` to those emit sites, and the run
    /// loop populates metadata in the started-event payloads.
    #[tokio::test(flavor = "multi_thread")]
    async fn started_events_carry_metadata_and_real_iteration_attempt() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());
        let wf = Arc::new(minimal_workflow());

        let handle = engine
            .start_run(wf, HashMap::new(), "test", false, None)
            .expect("start_run");
        let mut rx = handle.event_rx;

        let summary = handle.join.await.expect("join").expect("run ok");
        assert_eq!(summary.status, "done");

        // Drain the broadcast and verify the two started events.
        let mut wf_started: Option<RunEvent> = None;
        let mut node_started: Option<RunEvent> = None;
        while let Ok(ev) = rx.try_recv() {
            match ev.ty {
                EventType::WorkflowStarted => wf_started = Some(ev),
                EventType::NodeStarted => node_started = Some(ev),
                _ => {},
            }
        }

        let wf_ev = wf_started.expect("workflow:started event present");
        assert_eq!(
            wf_ev.payload.get("workflow_id").and_then(|v| v.as_str()),
            Some("wf1"),
            "workflow:started payload must include workflow_id",
        );
        assert_eq!(
            wf_ev.payload.get("trigger_kind").and_then(|v| v.as_str()),
            Some("test"),
            "workflow:started payload must include trigger_kind",
        );

        let node_ev = node_started.expect("node:started event present");
        assert_eq!(
            node_ev.iteration,
            Some(1),
            "node:started must carry the real iteration (1), not 0",
        );
        assert_eq!(
            node_ev.attempt,
            Some(1),
            "node:started must carry the real attempt (1), not 0",
        );
        assert_eq!(
            node_ev.payload.get("node_type").and_then(|v| v.as_str()),
            Some("delay"),
            "node:started payload must include node_type",
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn start_run_dispatches_minimal_workflow_to_done() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());
        let wf = Arc::new(minimal_workflow());

        let handle = engine
            .start_run(wf, HashMap::new(), "test", false, None)
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
            target_env: None,
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
            resources: vec![],
            default_env: None,
        });

        let summary = engine
            .run_workflow(wf, HashMap::new(), "test", false, None)
            .await
            .expect("run ok");
        assert_eq!(summary.status, "done");
        assert_eq!(summary.node_runs, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn start_run_rejects_concurrent_same_workflow() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());
        // 60s delay so h1 cannot complete (and release the workflow lock)
        // before the test thread reaches the second start_run call.
        // `minimal_workflow`'s 1ms delay races the test thread under
        // default test-threads parallelism.
        let wf = Arc::new(Workflow {
            id: "wf_concurrent_reject".into(),
            name: "concurrent reject".into(),
            schema_version: 1,
            created_at: None,
            updated_at: None,
            variables: HashMap::new(),
            triggers: vec![],
            nodes: vec![Node {
                id: "slow".into(),
                ty: "delay".into(),
                name: String::new(),
                config: HashMap::from([("ms".into(), serde_json::json!(60_000))]),
                pos: Pos::default(),
                timeout_ms: None,
                retry: None,
                continue_on_error: false,
                target_env: None,
            }],
            edges: vec![],
            resources: vec![],
            default_env: None,
        });

        let h1 = engine
            .start_run(wf.clone(), HashMap::new(), "test", false, None)
            .expect("first run starts");
        let second = engine.start_run(wf.clone(), HashMap::new(), "test", false, None);
        match second {
            Err(EngineError::AlreadyRunning { .. }) => {},
            Ok(_) => panic!("expected AlreadyRunning, got Ok(RunHandle)"),
            Err(e) => panic!("expected AlreadyRunning, got Err({e})"),
        }
        assert!(engine.cancel_run(&h1.run_id));
        let summary = h1.join.await.expect("join").expect("first run completes");
        assert_eq!(summary.status, "stopped");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn start_run_lock_conflict_does_not_insert_orphan_run() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());
        let wf = Arc::new(Workflow {
            id: "wf_lock_conflict".into(),
            name: "lock conflict".into(),
            schema_version: 1,
            created_at: None,
            updated_at: None,
            variables: HashMap::new(),
            triggers: vec![],
            nodes: vec![Node {
                id: "slow".into(),
                ty: "delay".into(),
                name: String::new(),
                config: HashMap::from([("ms".into(), serde_json::json!(60_000))]),
                pos: Pos::default(),
                timeout_ms: None,
                retry: None,
                continue_on_error: false,
                target_env: None,
            }],
            edges: vec![],
            resources: vec![],
            default_env: None,
        });

        let first = engine
            .start_run(wf.clone(), HashMap::new(), "test", false, None)
            .expect("first run starts");
        let second = engine.start_run(wf.clone(), HashMap::new(), "test", false, None);
        assert!(
            matches!(second, Err(EngineError::AlreadyRunning { .. })),
            "second start should report the workflow lock conflict",
        );

        let conn = engine.pool().get().unwrap();
        let row_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM runs WHERE workflow_id=?",
                [&wf.id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            row_count, 1,
            "lock conflict must not leave a second run row behind",
        );
        let running_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM runs WHERE workflow_id=? AND status='running'",
                [&wf.id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(running_count, 1);
        drop(conn);

        assert!(engine.cancel_run(&first.run_id));
        let summary = first
            .join
            .await
            .expect("join")
            .expect("first run completes");
        assert_eq!(summary.status, "stopped");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn start_run_lock_conflict_restores_prior_workflow_resources() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());
        let original_resource = "test-rollback-resource";
        let replacement_resource = "test-replacement-resource";
        let wf_a = Arc::new(blocking_workflow_with_resource(original_resource));
        let wf_b = Arc::new(blocking_workflow_with_resource(replacement_resource));

        let first = engine
            .start_run(wf_a.clone(), HashMap::new(), "test", false, None)
            .expect("first run starts");

        let conn = engine.pool().get().unwrap();
        let lock_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM workflow_locks WHERE workflow_id=?",
                [&wf_a.id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(lock_count, 1, "first run should hold the workflow lock");
        drop(conn);

        assert!(
            workflow_scope_resource_visible(&engine, "wf_a", original_resource),
            "first run should install the original workflow resource",
        );

        let second = engine.start_run(wf_b.clone(), HashMap::new(), "test", false, None);
        assert!(
            matches!(second, Err(EngineError::AlreadyRunning { .. })),
            "second start should report the workflow lock conflict",
        );

        assert!(
            workflow_scope_resource_visible(&engine, "wf_a", original_resource),
            "lock conflict should preserve the previously installed resource",
        );
        assert!(
            !workflow_scope_resource_visible(&engine, "wf_a", replacement_resource),
            "lock conflict should not leave the rejected workflow resource installed",
        );

        assert!(engine.cancel_run(&first.run_id));
        let summary = first
            .join
            .await
            .expect("join")
            .expect("first run completes");
        assert_eq!(summary.status, "stopped");
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
                target_env: None,
            }],
            edges: vec![],
            resources: vec![],
            default_env: None,
        });

        let summary = engine
            .run_workflow(wf, HashMap::new(), "test", false, None)
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
                target_env: None,
            }],
            edges: vec![],
            resources: vec![],
            default_env: None,
        });

        let handle = engine
            .start_run(wf, HashMap::new(), "test", false, None)
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

    /// A clean run drives `WorkspaceManager::teardown_all` with
    /// `RunOutcome::Completed`. Observed via the test-only
    /// `last_outcome` seam, exposed on the handle under `cfg(test)`.
    #[tokio::test(flavor = "multi_thread")]
    async fn teardown_records_completed_outcome_on_clean_run() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());
        let wf = Arc::new(minimal_workflow());

        let handle = engine
            .start_run(wf, HashMap::new(), "test", false, None)
            .expect("start_run");
        let manager = Arc::clone(&handle.workspace_manager);
        let summary = handle.join.await.expect("join").expect("run ok");
        assert_eq!(summary.status, "done");
        assert_eq!(
            *manager.last_outcome.lock().unwrap(),
            Some(RunOutcome::Completed),
        );
    }

    /// A user-cancelled run drives `teardown_all` with
    /// `RunOutcome::CancelledByUser`.
    #[tokio::test(flavor = "multi_thread")]
    async fn teardown_records_cancelled_outcome_on_cancel() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());

        // Long delay so cancellation arrives before it finishes.
        let wf = Arc::new(Workflow {
            id: "wf_cancel_outcome".into(),
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
                target_env: None,
            }],
            edges: vec![],
            resources: vec![],
            default_env: None,
        });

        let handle = engine
            .start_run(wf, HashMap::new(), "test", false, None)
            .expect("start_run");
        let run_id = handle.run_id.clone();
        let manager = Arc::clone(&handle.workspace_manager);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(engine.cancel_run(&run_id), "cancel_run should find the run");
        let summary = handle.join.await.expect("join").expect("run ok");
        assert_eq!(summary.status, "stopped");
        assert_eq!(
            *manager.last_outcome.lock().unwrap(),
            Some(RunOutcome::CancelledByUser),
        );
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
                target_env: None,
            }],
            edges: vec![],
            resources: vec![],
            default_env: None,
        });

        let summary = engine
            .run_workflow(wf, HashMap::new(), "test", false, None)
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
                target_env: None,
            }],
            edges: vec![],
            resources: vec![],
            default_env: None,
        });

        let start = std::time::Instant::now();
        let summary = engine
            .run_workflow(wf, HashMap::new(), "test", false, None)
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
                target_env: None,
            }],
            edges: vec![],
            resources: vec![],
            default_env: None,
        });

        let summary = engine
            .run_workflow(wf, HashMap::new(), "test", false, None)
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
