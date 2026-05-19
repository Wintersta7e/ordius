//! Shared helpers for in-process built-in executors.

use crate::executor::NodeError;
use std::collections::HashMap;

/// Fetch a required string config field. The error message uses
/// `prefix` (typically the node type id) so callers get
/// `"transform: 'input' (string) required"` rather than a
/// nondescript "config error".
pub(super) fn config_str<'a>(
    cfg: &'a HashMap<String, serde_json::Value>,
    key: &str,
    prefix: &str,
) -> Result<&'a str, NodeError> {
    cfg.get(key)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| NodeError::Config(format!("{prefix}: '{key}' (string) required")))
}

/// Optional string config field — `None` when missing or when the
/// value isn't a JSON string.
pub(super) fn config_str_opt<'a>(
    cfg: &'a HashMap<String, serde_json::Value>,
    key: &str,
) -> Option<&'a str> {
    cfg.get(key).and_then(serde_json::Value::as_str)
}

/// Optional string config field with a `default` if missing /
/// wrong type.
pub(super) fn config_str_or<'a>(
    cfg: &'a HashMap<String, serde_json::Value>,
    key: &str,
    default: &'a str,
) -> &'a str {
    cfg.get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or(default)
}

/// Optional `u64` config field with a `default` if missing /
/// wrong type.
pub(super) fn config_u64_or(
    cfg: &HashMap<String, serde_json::Value>,
    key: &str,
    default: u64,
) -> u64 {
    cfg.get(key)
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(default)
}

/// Optional `f64` config field with a `default` if missing /
/// wrong type.
pub(super) fn config_f64_or(
    cfg: &HashMap<String, serde_json::Value>,
    key: &str,
    default: f64,
) -> f64 {
    cfg.get(key)
        .and_then(serde_json::Value::as_f64)
        .unwrap_or(default)
}

/// Optional `bool` config field with a `default` if missing /
/// wrong type.
pub(super) fn config_bool_or(
    cfg: &HashMap<String, serde_json::Value>,
    key: &str,
    default: bool,
) -> bool {
    cfg.get(key)
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(default)
}
