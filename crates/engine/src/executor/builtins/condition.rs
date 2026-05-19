//! `condition` built-in: branch evaluator. Emits port `branch`
//! whose value is the string `"true"` or `"false"`. The scheduler's
//! `resolve_condition` consumes that string to pick edges with a
//! matching `branch` label.

use super::util::config_str;
use crate::executor::{NodeError, NodeExecutor, NodeOutputs, RunContext};
use crate::types::{Node, NodeType, PortValue};
use async_trait::async_trait;
use jsonpath_rust::JsonPath;
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;

/// Four modes: `boolean`, `exit_code`, `regex`, `jsonpath`.
pub struct ConditionExecutor;

#[async_trait]
impl NodeExecutor for ConditionExecutor {
    fn supports(&self, nt: &NodeType) -> bool {
        nt.id == "condition"
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
        let mode = node
            .config
            .get("mode")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                NodeError::Config(
                    "condition: 'mode' required: boolean|exit_code|regex|jsonpath".into(),
                )
            })?;
        let truthy = match mode {
            "boolean" => eval_boolean(&node.config)?,
            "exit_code" => eval_exit_code(&node.config)?,
            "regex" => eval_regex(&node.config)?,
            "jsonpath" => eval_jsonpath(&node.config)?,
            other => {
                return Err(NodeError::Config(format!(
                    "condition: unknown mode '{other}'"
                )));
            },
        };
        let mut out = NodeOutputs::new();
        out.insert(
            "branch".into(),
            PortValue::String(if truthy {
                "true".into()
            } else {
                "false".into()
            }),
        );
        Ok(out)
    }
}

fn eval_boolean(cfg: &HashMap<String, serde_json::Value>) -> Result<bool, NodeError> {
    cfg.get("value")
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| NodeError::Config("condition.boolean: 'value' (bool) required".into()))
}

fn eval_exit_code(cfg: &HashMap<String, serde_json::Value>) -> Result<bool, NodeError> {
    let code = cfg
        .get("exit_code")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| {
            NodeError::Config("condition.exit_code: 'exit_code' (int) required".into())
        })?;
    Ok(code == 0)
}

fn eval_regex(cfg: &HashMap<String, serde_json::Value>) -> Result<bool, NodeError> {
    let input = config_str(cfg, "input", "condition")?;
    let pattern = config_str(cfg, "pattern", "condition")?;
    let re = regex::Regex::new(pattern).map_err(|e| NodeError::Other(format!("regex: {e}")))?;
    Ok(re.is_match(input))
}

fn eval_jsonpath(cfg: &HashMap<String, serde_json::Value>) -> Result<bool, NodeError> {
    let input = config_str(cfg, "input", "condition")?;
    let expr = config_str(cfg, "expr", "condition")?;
    let val: serde_json::Value =
        serde_json::from_str(input).map_err(|e| NodeError::Other(format!("input parse: {e}")))?;
    let matched = val
        .query(expr)
        .map_err(|e| NodeError::Other(format!("jsonpath: {e}")))?;
    Ok(!matched.is_empty()
        && !matched
            .iter()
            .all(|v| v.is_null() || v.as_bool() == Some(false)))
}

#[cfg(test)]
mod tests;
