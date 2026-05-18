//! Node = an instance of a node type, placed on the workflow graph.
//! Spec: docs/04-storage-and-format.md "Workflow JSON shape".

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// An instance of a node-type placed on the workflow graph. Carries its
/// runtime configuration and any per-node overrides (timeout, retry,
/// continue-on-error).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Node {
    /// Stable identifier within the workflow.
    pub id: String,
    /// Reference to a registered node-type id (built-in or manifest).
    #[serde(rename = "type")]
    pub ty: String,
    /// Human-readable label (user-editable in the GUI).
    pub name: String,
    /// Node-type-specific configuration values.
    #[serde(default)]
    pub config: HashMap<String, serde_json::Value>,
    /// Canvas position (GUI-only; ignored by the engine).
    #[serde(default)]
    pub pos: Pos,
    /// Per-node timeout override (milliseconds). `None` means use the
    /// node-type's default.
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    /// Per-node retry override. `None` means no retry.
    #[serde(default)]
    pub retry: Option<RetryPolicy>,
    /// If true, downstream edges still activate even when this node
    /// errors. Default false (errors halt the branch).
    #[serde(default)]
    pub continue_on_error: bool,
}

/// Canvas position for the GUI. Engine ignores; loader preserves.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct Pos {
    /// X coordinate (canvas pixels).
    #[serde(default)]
    pub x: f64,
    /// Y coordinate (canvas pixels).
    #[serde(default)]
    pub y: f64,
}

/// Retry policy for a node. Defaults: 1 attempt (no retry), 1s backoff,
/// exponential strategy, retry on error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Total attempts including the first run. `1` means no retry.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// Initial backoff in milliseconds. Strategy scales this.
    #[serde(default = "default_backoff_ms")]
    pub backoff_ms: u64,
    /// How backoff grows between attempts.
    #[serde(default = "default_backoff")]
    pub backoff_strategy: BackoffStrategy,
    /// Which failure kinds trigger a retry.
    #[serde(default)]
    pub retry_on: RetryOn,
}

const fn default_max_attempts() -> u32 {
    1
}

const fn default_backoff_ms() -> u64 {
    1000
}

const fn default_backoff() -> BackoffStrategy {
    BackoffStrategy::Exponential
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: default_max_attempts(),
            backoff_ms: default_backoff_ms(),
            backoff_strategy: default_backoff(),
            retry_on: RetryOn::default(),
        }
    }
}

/// Backoff growth pattern between retry attempts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackoffStrategy {
    /// `backoff_ms * 2^(attempt-1)` — doubles each attempt.
    Exponential,
    /// `backoff_ms * attempt` — grows linearly.
    Linear,
    /// `backoff_ms` constant for every attempt.
    Fixed,
}

/// Which failure kinds trigger a retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RetryOn {
    /// Retry only on node errors (default).
    #[default]
    Error,
    /// Retry only on timeouts.
    Timeout,
    /// Retry on errors and timeouts.
    Both,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_minimal_json_roundtrip() {
        let n: Node =
            serde_json::from_str(r#"{"id":"n1","type":"shell","name":"step","config":{}}"#)
                .unwrap();
        assert_eq!(n.id, "n1");
        assert_eq!(n.ty, "shell");
        assert!(!n.continue_on_error);
        assert!(n.timeout_ms.is_none());
        assert!(n.retry.is_none());
    }

    #[test]
    fn retry_defaults_match_spec() {
        let p = RetryPolicy::default();
        assert_eq!(p.max_attempts, 1);
        assert_eq!(p.backoff_ms, 1000);
        assert_eq!(p.backoff_strategy, BackoffStrategy::Exponential);
        assert_eq!(p.retry_on, RetryOn::Error);
    }
}
