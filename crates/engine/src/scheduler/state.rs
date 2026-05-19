//! Per-node state in a workflow run. Spec: `docs/02-engine-model.md`.

/// Per-node state in a workflow run. Drives the scheduler's
/// edge-activation state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeState {
    /// Awaiting upstream completion(s).
    Pending,
    /// All upstream forward edges satisfied; eligible for dispatch.
    Ready,
    /// An executor is currently working on this node.
    Running,
    /// Executor completed successfully.
    Done,
    /// Executor failed.
    Error,
    /// Upstream failed and propagated, or condition branch unselected.
    Skipped,
}
