//! `transform` built-in: `template`, `jsonpath`, `regex_extract`,
//! `regex_replace`. Single primary output: `text` (string).

use super::util::config_str;
use crate::executor::{NodeError, NodeExecutor, NodeOutputs, RunContext};
use crate::types::{Node, NodeType, PortValue};
use async_trait::async_trait;
use jsonpath_rust::JsonPath;
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;

/// Three ops: `jsonpath`, `regex_extract`, `regex_replace`.
/// Output port `text` carries the resulting string.
#[allow(unreachable_pub)]
pub const NODE_TYPE_ID: &str = "transform";

/// `JSONPath` / regex / template transformer; emits the result on
/// the `text` port.
pub struct TransformExecutor;

#[async_trait]
impl NodeExecutor for TransformExecutor {
    fn supports(&self, nt: &NodeType) -> bool {
        nt.id == NODE_TYPE_ID
    }

    async fn run(
        &self,
        node: &Node,
        _nt: &NodeType,
        ctx: &RunContext,
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
                    "transform: 'op' required: template|jsonpath|regex_extract|regex_replace"
                        .into(),
                )
            })?;
        let result = match op {
            "template" => apply_template(node, ctx)?,
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

fn apply_template(node: &Node, ctx: &RunContext) -> Result<String, NodeError> {
    let tmpl = node
        .config
        .get("template")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            NodeError::Config("transform.template: 'template' (string) required".into())
        })?;
    let secrets_resolver = crate::executor::context::make_secrets_resolver(ctx);
    let kv_resolver = |_: &str| None;
    let env_allow = crate::template::default_env_allowlist();
    let sub_ctx = crate::template::SubstitutionContext {
        vars: &ctx.variables,
        secrets: &secrets_resolver,
        upstream_outputs: &ctx.upstream_outputs,
        current_inputs: &ctx.current_inputs,
        current_config: &node.config,
        kv: &kv_resolver,
        env: &*ctx.env,
        env_allowlist: &env_allow,
        run_id: &ctx.run_id,
        workspace: &ctx.workspace,
        started_at_iso: &ctx.started_at_iso,
        workflow_id: &ctx.workflow_id,
        workflow_name: &ctx.workflow_name,
    };
    crate::template::substitute(tmpl, &sub_ctx).map_err(|e| NodeError::Template(e.to_string()))
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
