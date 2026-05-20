//! Live registry mapping `(run_id, event_name)` → payload oneshot.
//!
//! The `wait_event` built-in registers a receiver here and parks
//! until an external caller (CLI / GUI / webhook trigger) signals
//! via [`EventRegistry::deliver`]. Parallel-to [`CheckpointRegistry`]
//! but keyed by event name and carries a JSON payload back.

use std::collections::HashMap;
use std::sync::Mutex;
use tokio::sync::oneshot;

/// Thread-safe map of pending event waiters.
pub struct EventRegistry {
    senders: Mutex<HashMap<(String, String), oneshot::Sender<serde_json::Value>>>,
}

impl EventRegistry {
    /// Empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            senders: Mutex::new(HashMap::new()),
        }
    }

    /// Register a waiter for `(run_id, event_name)` and return the
    /// receiver the executor should `.await`. If a waiter already
    /// exists for the pair it is overwritten — the previous
    /// receiver will observe its sender drop (closed channel).
    pub fn register(&self, run_id: &str, event_name: &str) -> oneshot::Receiver<serde_json::Value> {
        let (tx, rx) = oneshot::channel();
        self.senders
            .lock()
            .expect("events mutex poisoned")
            .insert((run_id.to_string(), event_name.to_string()), tx);
        rx
    }

    /// Deliver a payload to a parked waiter. Returns `true` if a
    /// sender was present and the payload was delivered.
    pub fn deliver(&self, run_id: &str, event_name: &str, payload: serde_json::Value) -> bool {
        self.senders
            .lock()
            .expect("events mutex poisoned")
            .remove(&(run_id.to_string(), event_name.to_string()))
            .is_some_and(|tx| tx.send(payload).is_ok())
    }
}

impl Default for EventRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_then_deliver_returns_payload() {
        let reg = EventRegistry::new();
        let rx = reg.register("r1", "approved");
        let delivered = reg.deliver("r1", "approved", serde_json::json!({"by": "alice"}));
        assert!(delivered);
        let payload = rx.await.unwrap();
        assert_eq!(payload["by"], "alice");
    }

    #[tokio::test]
    async fn deliver_without_waiter_returns_false() {
        let reg = EventRegistry::new();
        assert!(!reg.deliver("r1", "nope", serde_json::json!(null)));
    }
}
