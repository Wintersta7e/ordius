//! `condition` built-in: branch evaluator. Emits port `branch`
//! whose value is the string `"true"` or `"false"`. The scheduler's
//! `resolve_condition` consumes that string to pick edges with a
//! matching `branch` label.

use super::util::config_str;
use crate::executor::{NodeError, NodeExecutor, NodeOutputs, RunContext};
use crate::types::{Node, NodeType, PortValue};
use async_trait::async_trait;
use jsonpath_rust::JsonPath;
use std::cmp::Ordering;
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;

/// Modes: `boolean`, `exit_code`, `regex`, `jsonpath`, `compare`.
#[allow(unreachable_pub)]
pub const NODE_TYPE_ID: &str = "condition";

/// Branch evaluator: reads its config's `mode` and emits the
/// `branch` String port the scheduler uses to route downstream.
pub struct ConditionExecutor;

#[async_trait]
impl NodeExecutor for ConditionExecutor {
    fn supports(&self, nt: &NodeType) -> bool {
        nt.id == NODE_TYPE_ID
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
                    "condition: 'mode' required: boolean|exit_code|regex|jsonpath|compare".into(),
                )
            })?;
        let truthy = match mode {
            "boolean" => eval_boolean(&node.config)?,
            "exit_code" => eval_exit_code(&node.config)?,
            "regex" => eval_regex(&node.config)?,
            "jsonpath" => eval_jsonpath(&node.config)?,
            "compare" => eval_compare(&node.config)?,
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
    let re = regex::Regex::new(pattern)
        .map_err(|e| NodeError::Config(format!("condition.regex: invalid pattern: {e}")))?;
    Ok(re.is_match(input))
}

/// `compare` mode: `op` selects the operator, `left`/`right` are the
/// values. Strings compare lexicographically; numbers compare
/// numerically; mixed types are a config error. `contains` /
/// `starts_with` / `ends_with` are string-only.
fn eval_compare(cfg: &HashMap<String, serde_json::Value>) -> Result<bool, NodeError> {
    let op = config_str(cfg, "op", "condition.compare")?;
    let left = cfg
        .get("left")
        .ok_or_else(|| NodeError::Config("condition.compare: 'left' required".into()))?;
    let right = cfg
        .get("right")
        .ok_or_else(|| NodeError::Config("condition.compare: 'right' required".into()))?;
    match op {
        "eq" => Ok(left == right),
        "neq" => Ok(left != right),
        "lt" | "le" | "gt" | "ge" => compare_ordered(left, right, op),
        "contains" => string_op(left, right, |a, b| a.contains(b)),
        "starts_with" => string_op(left, right, |a, b| a.starts_with(b)),
        "ends_with" => string_op(left, right, |a, b| a.ends_with(b)),
        other => Err(NodeError::Config(format!(
            "condition.compare: unknown op '{other}'"
        ))),
    }
}

fn compare_ordered(
    left: &serde_json::Value,
    right: &serde_json::Value,
    op: &str,
) -> Result<bool, NodeError> {
    let ord = if let (Some(a), Some(b)) = (left.as_f64(), right.as_f64()) {
        a.partial_cmp(&b)
    } else if let (Some(a), Some(b)) = (left.as_str(), right.as_str()) {
        // Template substitution turns `{{nodes.X.outputs.text}}` into a
        // JSON string, so numeric values arrive quoted. When BOTH sides
        // parse as numbers, compare numerically; otherwise fall back to
        // lexicographic string ordering (real words, mixed digit/word).
        match (a.parse::<f64>(), b.parse::<f64>()) {
            (Ok(na), Ok(nb)) => na.partial_cmp(&nb),
            _ => Some(a.cmp(b)),
        }
    } else {
        return Err(NodeError::Config(format!(
            "condition.compare.{op}: left and right must both be numbers or both strings"
        )));
    };
    let Some(ord) = ord else {
        // NaN comparisons fall here; any ordering predicate is false.
        return Ok(false);
    };
    Ok(match op {
        "lt" => ord == Ordering::Less,
        "le" => ord != Ordering::Greater,
        "gt" => ord == Ordering::Greater,
        "ge" => ord != Ordering::Less,
        _ => unreachable!("op was validated above"),
    })
}

fn string_op<F>(
    left: &serde_json::Value,
    right: &serde_json::Value,
    f: F,
) -> Result<bool, NodeError>
where
    F: FnOnce(&str, &str) -> bool,
{
    let a = left.as_str().ok_or_else(|| {
        NodeError::Config("condition.compare: left must be a string for this op".into())
    })?;
    let b = right.as_str().ok_or_else(|| {
        NodeError::Config("condition.compare: right must be a string for this op".into())
    })?;
    Ok(f(a, b))
}

fn eval_jsonpath(cfg: &HashMap<String, serde_json::Value>) -> Result<bool, NodeError> {
    let input = config_str(cfg, "input", "condition")?;
    let expr = config_str(cfg, "expr", "condition")?;
    let val: serde_json::Value = serde_json::from_str(input)
        .map_err(|e| NodeError::Config(format!("condition.jsonpath: invalid input JSON: {e}")))?;
    let matched = val
        .query(expr)
        .map_err(|e| NodeError::Config(format!("condition.jsonpath: invalid expression: {e}")))?;
    Ok(!matched.is_empty()
        && !matched
            .iter()
            .all(|v| v.is_null() || v.as_bool() == Some(false)))
}

#[cfg(test)]
mod tests;
