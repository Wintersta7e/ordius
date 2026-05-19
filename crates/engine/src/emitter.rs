//! Per-run event emitter. Persists every event to `SQLite` via the
//! recorder AND fans it out over a `tokio` broadcast channel for
//! live subscribers (CLI stdout streaming, GUI Tauri command).

use crate::events::{EventType, RunEvent};
use crate::recorder::RunRecorder;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Buffer depth of the broadcast channel. Slow consumers that fall
/// behind by more than this many events see `RecvError::Lagged`
/// per `tokio::sync::broadcast` semantics — the recorder remains
/// authoritative, so this is intentional rather than a data loss
/// concern.
const CHANNEL_BUFFER: usize = 1024;

/// Per-run event emitter. Combines persistence (via [`RunRecorder`])
/// with live broadcasting to all subscribers.
pub struct Emitter {
    recorder: Arc<RunRecorder>,
    tx: broadcast::Sender<RunEvent>,
}

impl Emitter {
    /// Build an emitter and return the initial receiver from the
    /// broadcast channel.
    #[must_use]
    pub fn new(recorder: Arc<RunRecorder>) -> (Self, broadcast::Receiver<RunEvent>) {
        let (tx, rx) = broadcast::channel(CHANNEL_BUFFER);
        (Self { recorder, tx }, rx)
    }

    /// Subscribe an additional receiver to the broadcast channel.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<RunEvent> {
        self.tx.subscribe()
    }

    /// Emit an event. Persists it via the recorder (logging a
    /// warning on failure) and fans it out to subscribers. A
    /// failed broadcast send simply means no receivers are
    /// currently attached and is intentionally ignored.
    pub fn emit(
        &self,
        ty: EventType,
        node_id: Option<String>,
        iteration: Option<u32>,
        attempt: Option<u32>,
        payload: HashMap<String, serde_json::Value>,
    ) {
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
}
