//! `RunContext`: per-run shared state passed to every executor.
//!
//! Carries the recorder + emitter so executors can stream
//! stdout/stderr or checkpoint events, and the wired inputs
//! assembled by the run-loop from upstream forward edges into
//! the current node.

use crate::emitter::Emitter;
use crate::recorder::RunRecorder;
use crate::secrets::Store;
use crate::types::PortValue;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

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
    /// Wired input data assembled by the run-loop from upstream
    /// forward edges into this node. Read by executors via
    /// `ctx.current_inputs.get(port_name)` and by the template
    /// substitution engine for `{{inputs.<port>}}` forms.
    pub current_inputs: HashMap<String, PortValue>,
    /// All upstream node outputs produced earlier in the run,
    /// keyed by `(node_id, port_name)`. Snapshot at dispatch time;
    /// updated by the run-loop after each successful node completion.
    pub upstream_outputs: HashMap<(String, String), PortValue>,
}
