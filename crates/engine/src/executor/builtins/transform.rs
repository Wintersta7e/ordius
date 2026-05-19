//! `transform` built-in (Phase 4 cut): `jsonpath`, `regex_extract`,
//! `regex_replace`.
//!
//! The `template` op lands in Phase 5 alongside the unified
//! substitution engine. Single primary output: `text` (string).

use super::util::config_str;
use crate::executor::{NodeError, NodeExecutor, NodeOutputs, RunContext};
use crate::types::{Node, NodeType, PortValue};
use async_trait::async_trait;
use jsonpath_rust::JsonPath;
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;

/// Three ops: `jsonpath`, `regex_extract`, `regex_replace`.
/// Output port `text` carries the resulting string.
pub struct TransformExecutor;

#[async_trait]
impl NodeExecutor for TransformExecutor {
    fn supports(&self, nt: &NodeType) -> bool {
        nt.id == "transform"
    }

    async fn run(
        &self,
        node: &Node,
        _nt: &NodeType,
        _ctx: &RunContext,
        cancel: CancellationToken,
    ) -> Result<NodeOutputs, NodeError> {
        if cancel.is_cancelled() {
            return Err(NodeError::Cancelled);
        }
        let op = node
            .config
            .get("op")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                NodeError::Config(
                    "transform: 'op' required: jsonpath|regex_extract|regex_replace".into(),
                )
            })?;
        let result = match op {
            "jsonpath" => apply_jsonpath(&node.config)?,
            "regex_extract" => apply_regex_extract(&node.config)?,
            "regex_replace" => apply_regex_replace(&node.config)?,
            other => {
                return Err(NodeError::Config(format!(
                    "transform: unknown op '{other}'"
                )));
            },
        };
        let mut out = NodeOutputs::new();
        out.insert("text".into(), PortValue::String(result));
        Ok(out)
    }
}

fn apply_jsonpath(cfg: &HashMap<String, serde_json::Value>) -> Result<String, NodeError> {
    let input = config_str(cfg, "input", "transform")?;
    let expr = config_str(cfg, "expr", "transform")?;
    let val: serde_json::Value = serde_json::from_str(input)
        .map_err(|e| NodeError::Config(format!("transform.jsonpath: invalid input JSON: {e}")))?;
    let matched = val
        .query(expr)
        .map_err(|e| NodeError::Config(format!("transform.jsonpath: invalid expression: {e}")))?;
    serde_json::to_string(&matched).map_err(|e| NodeError::Other(format!("encode: {e}")))
}

fn apply_regex_extract(cfg: &HashMap<String, serde_json::Value>) -> Result<String, NodeError> {
    let input = config_str(cfg, "input", "transform")?;
    let pattern = config_str(cfg, "pattern", "transform")?;
    let re = regex::Regex::new(pattern)
        .map_err(|e| NodeError::Config(format!("transform.regex_extract: invalid pattern: {e}")))?;
    Ok(re
        .find(input)
        .map(|m| m.as_str().to_string())
        .unwrap_or_default())
}

fn apply_regex_replace(cfg: &HashMap<String, serde_json::Value>) -> Result<String, NodeError> {
    let input = config_str(cfg, "input", "transform")?;
    let pattern = config_str(cfg, "pattern", "transform")?;
    let replacement = cfg
        .get("replacement")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    let re = regex::Regex::new(pattern)
        .map_err(|e| NodeError::Config(format!("transform.regex_replace: invalid pattern: {e}")))?;
    Ok(re.replace_all(input, replacement).into_owned())
}

#[cfg(test)]
mod tests;
