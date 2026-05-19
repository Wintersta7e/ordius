//! Live registry mapping `(run_id, node_id)` → resume oneshot.
//!
//! The `checkpoint` built-in registers a receiver here and parks
//! until an external caller (CLI / GUI) signals via
//! [`CheckpointRegistry::resume`]. The signal travels through a
//! `tokio::sync::oneshot` so the executor task wakes deterministically.

use std::collections::HashMap;
use std::sync::Mutex;
use tokio::sync::oneshot;

/// Resume verdict sent from the outside world to a parked
/// `checkpoint` node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resume {
    /// User approved — continue with the workflow.
    Continue,
    /// User declined / cancelled — the executor returns
    /// `NodeError::Cancelled` and the run terminates as `stopped`.
    Cancel,
}

/// Thread-safe map of pending checkpoints.
///
/// Holds the *sender* side of a oneshot per parked node; the
/// node task owns the receiver. `register` allocates the channel
/// and stores the sender; `resume` removes and fires it.
pub struct CheckpointRegistry {
    senders: Mutex<HashMap<(String, String), oneshot::Sender<Resume>>>,
}

impl CheckpointRegistry {
    /// Empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            senders: Mutex::new(HashMap::new()),
        }
    }

    /// Register a checkpoint and return the receiver the executor
    /// should `.await` on. If a sender already exists for this
    /// `(run_id, node_id)` pair it is overwritten — the previous
    /// receiver will simply observe its sender being dropped (a
    /// closed-channel error), which the executor interprets as
    /// cancellation per [`Resume::Cancel`].
    pub fn register(&self, run_id: &str, node_id: &str) -> oneshot::Receiver<Resume> {
        let (tx, rx) = oneshot::channel();
        self.senders
            .lock()
            .expect("checkpoints mutex poisoned")
            .insert((run_id.to_string(), node_id.to_string()), tx);
        rx
    }

    /// Deliver a [`Resume`] to a parked checkpoint. Returns
    /// `true` if a sender was present and the message was
    /// delivered, `false` if no such checkpoint exists or the
    /// receiver has already dropped.
    pub fn resume(&self, run_id: &str, node_id: &str, r: Resume) -> bool {
        self.senders
            .lock()
            .expect("checkpoints mutex poisoned")
            .remove(&(run_id.to_string(), node_id.to_string()))
            .is_some_and(|tx| tx.send(r).is_ok())
    }
}

impl Default for CheckpointRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_then_resume_delivers_continue() {
        let reg = CheckpointRegistry::new();
        let rx = reg.register("r1", "n1");
        assert!(reg.resume("r1", "n1", Resume::Continue));
        assert_eq!(rx.await.unwrap(), Resume::Continue);
    }

    #[tokio::test]
    async fn resume_unknown_returns_false() {
        let reg = CheckpointRegistry::new();
        assert!(!reg.resume("r1", "n1", Resume::Continue));
    }

    #[tokio::test]
    async fn double_register_drops_first_receiver() {
        let reg = CheckpointRegistry::new();
        let rx_old = reg.register("r1", "n1");
        let rx_new = reg.register("r1", "n1");
        // First receiver sees closed channel (sender overwritten).
        assert!(rx_old.await.is_err());
        // New receiver still wins the resume.
        assert!(reg.resume("r1", "n1", Resume::Cancel));
        assert_eq!(rx_new.await.unwrap(), Resume::Cancel);
    }
}
