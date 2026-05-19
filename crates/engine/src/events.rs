//! Run events. Spec: `docs/02-engine-model.md` "Event emission".
//! Wire format is `snake_case`; the discriminator field is `type`.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Envelope for every event emitted during a workflow run.
///
/// Wire format pinned to `snake_case`; the variant tag lives in the
/// `type` field (renamed from the Rust field `ty` via serde) and
/// uses colon-separated identifiers (e.g. `workflow:started`,
/// `node:done`). Optional positional fields are omitted from the
/// JSON when `None`, and additional payload data is flattened
/// into the top-level object via `serde(flatten)`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunEvent {
    /// Discriminator tag identifying the event variant.
    #[serde(rename = "type")]
    pub ty: EventType,
    /// Monotonic per-run sequence number, starting at 0.
    pub seq: u64,
    /// Wall-clock emission time, Unix epoch in milliseconds.
    pub emitted_at: i64,
    /// Run id this event belongs to.
    pub run_id: String,
    /// Node id this event refers to, when applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    /// Loop iteration (1-based) the event was emitted under.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iteration: Option<u32>,
    /// Retry attempt (1-based) the event was emitted under.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt: Option<u32>,
    /// Free-form payload data, flattened into the top-level JSON
    /// object alongside the typed fields.
    #[serde(flatten)]
    pub payload: HashMap<String, serde_json::Value>,
}

/// Discriminator for [`RunEvent`] variants. Wire tags are
/// colon-separated (`workflow:started`, `node:loop`) so they
/// remain readable in `tail -f`-style log views.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EventType {
    /// Workflow started executing.
    #[serde(rename = "workflow:started")]
    WorkflowStarted,
    /// Workflow finished successfully.
    #[serde(rename = "workflow:done")]
    WorkflowDone,
    /// Workflow terminated due to an error.
    #[serde(rename = "workflow:error")]
    WorkflowError,
    /// Workflow was cancelled or stopped externally.
    #[serde(rename = "workflow:stopped")]
    WorkflowStopped,
    /// Node started executing.
    #[serde(rename = "node:started")]
    NodeStarted,
    /// Node produced an output value on one of its ports.
    #[serde(rename = "node:output")]
    NodeOutput,
    /// Node completed successfully.
    #[serde(rename = "node:done")]
    NodeDone,
    /// Node failed.
    #[serde(rename = "node:error")]
    NodeError,
    /// Node was skipped (upstream failed or branch unselected).
    #[serde(rename = "node:skipped")]
    NodeSkipped,
    /// Node is being retried after a transient failure.
    #[serde(rename = "node:retry")]
    NodeRetry,
    /// Loop edge fired, beginning a new iteration.
    #[serde(rename = "node:loop")]
    NodeLoop,
    /// Node paused at a checkpoint, awaiting external resume.
    #[serde(rename = "node:paused")]
    NodePaused,
    /// Paused node resumed execution.
    #[serde(rename = "node:resumed")]
    NodeResumed,
}

impl EventType {
    /// Wire-format tag for this event, e.g. `"node:started"`.
    /// Pinned by the `#[serde(rename = "...")]` attributes — the
    /// match arms here MUST stay aligned with them
    /// (`wire_tag_matches_serde` test guards the parity).
    #[must_use]
    pub const fn wire_tag(self) -> &'static str {
        match self {
            Self::WorkflowStarted => "workflow:started",
            Self::WorkflowDone => "workflow:done",
            Self::WorkflowError => "workflow:error",
            Self::WorkflowStopped => "workflow:stopped",
            Self::NodeStarted => "node:started",
            Self::NodeOutput => "node:output",
            Self::NodeDone => "node:done",
            Self::NodeError => "node:error",
            Self::NodeSkipped => "node:skipped",
            Self::NodeRetry => "node:retry",
            Self::NodeLoop => "node:loop",
            Self::NodePaused => "node:paused",
            Self::NodeResumed => "node:resumed",
        }
    }
}

#[cfg(test)]
mod tests;
