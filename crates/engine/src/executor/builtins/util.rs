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
