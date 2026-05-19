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
use crate::recorder::RunRecorder;
use crate::types::Workflow;
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

    /// Stub run loop — finalises immediately as `stopped`. The
    /// real scheduler-driven dispatch lands in the next commit;
    /// the async signature is kept because that body awaits node
    /// dispatches and the cancellation token.
    #[allow(clippy::unused_async)]
    async fn run_loop_inner(
        &self,
        _wf: Arc<Workflow>,
        recorder: Arc<RunRecorder>,
        emitter: Arc<Emitter>,
        _workspace: PathBuf,
        _variables: HashMap<String, String>,
        _cancel: CancellationToken,
        _auto_resume: bool,
    ) -> Result<RunSummary> {
        emitter.emit(
            EventType::WorkflowStopped,
            None,
            None,
            None,
            HashMap::new(),
        );
        recorder.finalize("stopped", Some("run loop not yet implemented"))?;
        Ok(RunSummary {
            run_id: recorder.run_id.clone(),
            status: "stopped".into(),
            node_runs: 0,
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
    async fn start_run_returns_handle_and_completes_as_stopped() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());
        let wf = Arc::new(minimal_workflow());

        let handle = engine
            .start_run(wf, HashMap::new(), "test", false)
            .expect("start_run");
        let summary = handle.join.await.expect("join").expect("run ok");
        assert_eq!(summary.status, "stopped");
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
            Err(EngineError::AlreadyRunning { .. }) => {}
            Ok(_) => panic!("expected AlreadyRunning, got Ok(RunHandle)"),
            Err(e) => panic!("expected AlreadyRunning, got Err({e})"),
        }
        h1.join.await.expect("join").expect("first run completes");
    }
}
