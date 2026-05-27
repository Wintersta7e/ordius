//! Per-run event emitter. Persists every event to `SQLite` via the
//! recorder AND fans it out over a `tokio` broadcast channel for
//! live subscribers (CLI stdout streaming, GUI Tauri command).

use crate::events::{EventType, RunEvent};
use crate::recorder::RunRecorder;
use crate::secrets::redact_secrets;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

/// Buffer depth of the broadcast channel. Slow consumers that fall
/// behind by more than this many events see `RecvError::Lagged`
/// per `tokio::sync::broadcast` semantics — the recorder remains
/// authoritative, so this is intentional rather than a data loss
/// concern.
const CHANNEL_BUFFER: usize = 1024;

/// Per-run event emitter.
///
/// Combines persistence (via [`RunRecorder`]) with live
/// broadcasting to all subscribers. Tracks accessed secrets so
/// `node:output` events have their `text` payload redacted
/// before reaching the recorder or the broadcast channel.
pub struct Emitter {
    recorder: Arc<RunRecorder>,
    tx: broadcast::Sender<RunEvent>,
    redaction: Mutex<Vec<(String, String)>>,
}

impl Emitter {
    /// Build an emitter and return the initial receiver from the
    /// broadcast channel.
    #[must_use]
    pub fn new(recorder: Arc<RunRecorder>) -> (Self, broadcast::Receiver<RunEvent>) {
        let (tx, rx) = broadcast::channel(CHANNEL_BUFFER);
        (
            Self {
                recorder,
                tx,
                redaction: Mutex::new(Vec::new()),
            },
            rx,
        )
    }

    /// Subscribe an additional receiver to the broadcast channel.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<RunEvent> {
        self.tx.subscribe()
    }

    /// Clone of the underlying broadcast sender — used by the
    /// engine to register the channel in its per-run subscriber
    /// map so external callers can subscribe later without holding
    /// the emitter directly.
    #[must_use]
    pub fn broadcast_sender(&self) -> broadcast::Sender<RunEvent> {
        self.tx.clone()
    }

    /// Register a secret `(name, value)` pair so the emitter can
    /// redact every occurrence of `value` from emitted
    /// `node:output` `text` payloads. Dedupes on the
    /// `(name, value)` pair so a rotated secret keeps its prior
    /// value on the redaction list — otherwise the old value
    /// would leak in subsequent emits.
    pub fn register_secret(&self, name: String, value: String) {
        let mut lock = self
            .redaction
            .lock()
            .expect("emitter redaction mutex poisoned");
        if !lock.iter().any(|(n, v)| n == &name && v == &value) {
            lock.push((name, value));
        }
    }

    /// Snapshot of every secret the run has touched so far. The
    /// run loop reuses this list to redact `node_outputs` writes
    /// the same way emitted events get redacted — otherwise a
    /// `{{secrets.X}}` value ending up in a port's `PortValue::String`
    /// would land in `SQLite` verbatim.
    #[must_use]
    pub fn accumulated_secrets(&self) -> Vec<(String, String)> {
        self.redaction
            .lock()
            .expect("emitter redaction mutex poisoned")
            .clone()
    }

    /// Emit a workflow-level event with no associated node. Thin
    /// wrapper around [`Self::emit`] that fills the node-scoped
    /// positional fields with `None`.
    pub fn emit_workflow(&self, ty: EventType, payload: HashMap<String, serde_json::Value>) {
        self.emit(ty, None, None, None, payload);
    }

    /// Emit a `stream:fallback` event signalling that an `llm` (or
    /// similar) executor requested streaming under `StreamMode::Auto`
    /// but the resolved route could not stream, so the run silently
    /// downgraded to a non-streaming request. Payload carries `url`
    /// and `reason` so consumers can surface the decision in the UI
    /// without reconstructing dispatch state.
    pub fn emit_stream_fallback(
        &self,
        node_id: impl Into<String>,
        iteration: u32,
        attempt: u32,
        url: impl Into<String>,
        reason: impl Into<String>,
    ) {
        let mut payload: HashMap<String, serde_json::Value> = HashMap::with_capacity(2);
        payload.insert("url".into(), serde_json::Value::String(url.into()));
        payload.insert("reason".into(), serde_json::Value::String(reason.into()));
        self.emit_node(
            EventType::StreamFallback,
            node_id,
            iteration,
            attempt,
            payload,
        );
    }

    /// Emit a node-scoped event. Convenience wrapper around
    /// [`Self::emit`] that takes the node id by `impl Into<String>`
    /// and requires both `iteration` and `attempt` — these are
    /// always meaningful on node events.
    pub fn emit_node(
        &self,
        ty: EventType,
        node_id: impl Into<String>,
        iteration: u32,
        attempt: u32,
        payload: HashMap<String, serde_json::Value>,
    ) {
        self.emit(
            ty,
            Some(node_id.into()),
            Some(iteration),
            Some(attempt),
            payload,
        );
    }

    /// Emit an event with full positional control over the
    /// optional fields. Prefer [`Self::emit_workflow`] or
    /// [`Self::emit_node`] for the common cases.
    pub fn emit(
        &self,
        ty: EventType,
        node_id: Option<String>,
        iteration: Option<u32>,
        attempt: Option<u32>,
        mut payload: HashMap<String, serde_json::Value>,
    ) {
        if ty == EventType::NodeOutput {
            self.redact_node_output_text(&mut payload);
        }
        let ev = RunEvent {
            ty,
            seq: self.recorder.next_seq(),
            emitted_at: chrono::Utc::now().timestamp_millis(),
            run_id: self.recorder.run_id.clone(),
            node_id,
            iteration,
            attempt,
            payload,
        };
        if let Err(e) = self.recorder.record_event(&ev) {
            tracing::warn!(error = ?e, "failed to persist event");
        }
        // No subscribers is fine — the recorder remains authoritative.
        drop(self.tx.send(ev));
    }

    fn redact_node_output_text(&self, payload: &mut HashMap<String, serde_json::Value>) {
        let Some(text_val) = payload.get_mut("text") else {
            return;
        };
        let Some(text) = text_val.as_str() else {
            return;
        };
        let snapshot = {
            let lock = self
                .redaction
                .lock()
                .expect("emitter redaction mutex poisoned");
            if lock.is_empty() {
                return;
            }
            lock.clone()
        };
        let redacted = redact_secrets(text, &snapshot);
        *text_val = serde_json::Value::String(redacted);
    }
}
