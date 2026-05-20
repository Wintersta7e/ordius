//! `RunContext`: per-run shared state passed to every executor.
//!
//! Carries the recorder + emitter so executors can stream
//! stdout/stderr or checkpoint events, and the wired inputs
//! assembled by the run-loop from upstream forward edges into
//! the current node.

use crate::checkpoints::CheckpointRegistry;
use crate::emitter::Emitter;
use crate::recorder::RunRecorder;
use crate::secrets::Store;
use crate::types::PortValue;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;

/// Shared env resolver type for [`RunContext::env`]. Send + Sync
/// so executors holding `&RunContext` across threads stay happy.
pub type EnvResolver = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;

/// Build an env resolver that reads the current process env via
/// [`std::env::var`]. Use as the default `RunContext::env`.
#[must_use]
pub fn wrap_process_env() -> EnvResolver {
    Arc::new(|name: &str| std::env::var(name).ok())
}

/// Shared per-run state.
pub struct RunContext {
    /// Run id this context belongs to.
    pub run_id: String,
    /// Workflow id this context belongs to.
    pub workflow_id: String,
    /// Workflow name (`{{workflow.name}}` source).
    pub workflow_name: String,
    /// ISO-8601 run start time (`{{run.startedAt}}` source).
    pub started_at_iso: String,
    /// Workspace directory for this run (tmp/scratch space).
    pub workspace: PathBuf,
    /// User-supplied workflow variables (template substitution input).
    pub variables: HashMap<String, String>,
    /// Per-run `SQLite` recorder.
    pub recorder: Arc<RunRecorder>,
    /// Event emitter. Executors call `.emit()` to push
    /// `node:output` / `node:paused` / etc.
    pub emitter: Arc<Emitter>,
    /// Secrets store for `{{secrets.X}}` resolution. `None` means
    /// secret lookups always fail (useful for tests that don't
    /// touch secrets).
    pub secrets_store: Option<Arc<Store>>,
    /// Resolver for `{{env.NAME}}` lookups after the allowlist
    /// guard. Production callers pass [`wrap_process_env`];
    /// tests inject a map-backed closure so they don't depend on
    /// process env state.
    pub env: EnvResolver,
    /// Wired input data assembled by the run-loop from upstream
    /// forward edges into this node. Read by executors via
    /// `ctx.current_inputs.get(port_name)` and by the template
    /// substitution engine for `{{inputs.<port>}}` forms.
    pub current_inputs: HashMap<String, PortValue>,
    /// All upstream node outputs produced earlier in the run,
    /// keyed by `(node_id, port_name)`. Snapshot at dispatch time;
    /// updated by the run-loop after each successful node completion.
    pub upstream_outputs: HashMap<(String, String), PortValue>,
    /// Live checkpoint registry — the `checkpoint` built-in
    /// registers a receiver here and parks until an external
    /// caller (CLI / GUI) signals resume / cancel.
    pub checkpoints: Arc<CheckpointRegistry>,
    /// Live event registry — the `wait_event` built-in registers
    /// a waiter here and parks until an external caller delivers
    /// the matching event via `Engine::deliver_event`.
    pub events: Arc<crate::events_registry::EventRegistry>,
    /// Weak handle back to the engine — built-ins that need to
    /// invoke sub-workflows (`compose`, `parallel`) upgrade and call
    /// `Engine::run_child_workflow`. Weak so the per-node context
    /// doesn't extend the engine lifetime past shutdown.
    pub engine: std::sync::Weak<crate::Engine>,
    /// Compose recursion depth — 0 for top-level runs, +1 per
    /// `compose` frame. The `compose` built-in compares this against
    /// its `max_depth` config to reject `A -> B -> A` cycles.
    pub compose_depth: u32,
    /// Run-level auto-resume flag. When `true`, every `checkpoint`
    /// node short-circuits without parking — set by the CLI's
    /// `run --yes`. Per-node `config.auto_resume` still overrides
    /// when present, so a workflow can mark specific checkpoints
    /// as test-only auto-resume regardless of the run flag.
    pub auto_resume: bool,
    /// Iteration of this node within the current run (1-indexed).
    /// Bumped by the scheduler when a loop edge re-fires the node.
    /// Read by executors that emit `node:output` / `node:paused`
    /// events mid-dispatch so the event coordinates match what the
    /// run loop will record for the surrounding `node_runs` row.
    pub iteration: u32,
    /// Attempt of this iteration (1-indexed). Updated per retry by
    /// `run_with_retry` before each call into the executor, so any
    /// in-attempt event emitted by the executor (line-by-line
    /// `node:output` from subprocess, SSE delta from `llm`,
    /// `node:paused` from `checkpoint`) reports the same attempt
    /// number the run loop will later persist for the `node_runs`
    /// row and the terminal `node:done` / `node:error` event.
    pub attempt: AtomicU32,
}

/// Build the closure that `template::SubstitutionContext::secrets`
/// expects.
///
/// The closure looks the name up on `ctx.secrets_store` (or
/// returns `None` when no store is configured) and — crucially —
/// registers the resolved value on `ctx.emitter` so any later
/// occurrence of it in `node:output` events is redacted. Every
/// executor that does template substitution must route through
/// this helper, otherwise `{{secrets.X}}` would silently leak.
pub fn make_secrets_resolver(ctx: &RunContext) -> impl Fn(&str) -> Option<String> + 'static {
    let secrets_store = ctx.secrets_store.clone();
    let emitter = ctx.emitter.clone();
    move |name: &str| {
        let store = secrets_store.as_ref()?;
        let value = store.get(name).ok()?;
        emitter.register_secret(name.to_string(), value.clone());
        Some(value)
    }
}
