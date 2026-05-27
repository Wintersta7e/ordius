//! `compose` built-in: invoke another saved workflow as a sub-frame.
//!
//! Loads `<home>/workflows/<workflow_id>.json` at run-time, runs it via
//! `Engine::run_child_workflow` (separate `run_id` + recorder row, no
//! workflow-lock acquisition so siblings can compose the same child
//! concurrently), and surfaces the child's final outputs on this node.
//!
//! Output extraction: by convention the child's terminal node is
//! named `return`; its port values become this node's outputs. An
//! explicit `output_map` config overrides the convention.
//!
//! Recursion: `ctx.compose_depth` increments per frame; the executor
//! rejects further compose calls beyond `max_depth` (default 8).

use super::util::{config_str, config_u64_or};
use crate::executor::{NodeError, NodeExecutor, NodeOutputs, RunContext};
use crate::types::{Node, NodeType, PortValue};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

const DEFAULT_MAX_DEPTH: u64 = 8;
const DEFAULT_SINK_NODE_ID: &str = "return";

#[allow(unreachable_pub)]
pub const NODE_TYPE_ID: &str = "compose";

/// Compose executor — see module docs.
pub struct ComposeExecutor;

#[async_trait]
impl NodeExecutor for ComposeExecutor {
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
        let workflow_id = config_str(&node.config, "workflow_id", "compose")?.to_string();
        let max_depth = config_u64_or(&node.config, "max_depth", DEFAULT_MAX_DEPTH);
        let next_depth = u64::from(ctx.compose_depth).saturating_add(1);
        if next_depth > max_depth {
            return Err(NodeError::Config(format!(
                "compose: max_depth ({max_depth}) exceeded — possible A→B→A cycle"
            )));
        }

        let vars = vars_from_config(node, ctx)?;

        let engine = ctx.engine.upgrade().ok_or_else(|| {
            NodeError::Other("compose: engine handle gone (shutdown in progress)".into())
        })?;

        let child_wf = crate::workflows::load(engine.home(), &workflow_id)
            .map_err(|e| NodeError::Config(format!("compose: load workflow: {e}")))?;

        let (summary, outputs) = engine
            .run_child_workflow(
                Arc::new(child_wf),
                vars,
                &cancel,
                u32::try_from(next_depth).unwrap_or(u32::MAX),
                Some(ctx.workspace.clone()),
                "compose-",
                Arc::clone(&ctx.run_snapshot),
            )
            .await
            .map_err(|e| NodeError::Other(format!("compose: child run: {e}")))?;

        let mut out = NodeOutputs::new();
        out.insert(
            "child_run_id".into(),
            PortValue::String(summary.run_id.clone()),
        );
        out.insert("status".into(), PortValue::String(summary.status.clone()));
        out.insert("outputs".into(), PortValue::Json(outputs_to_json(&outputs)));

        // Map specific (node_id, port) values onto named output ports
        // per config.output_map, or fall back to the `return` sink.
        if let Some(map) = node.config.get("output_map").and_then(|v| v.as_object()) {
            for (out_name, spec) in map {
                let (node_id, port) = parse_output_spec(spec)?;
                if let Some(val) = outputs.get(&(node_id, port)) {
                    out.insert(out_name.clone(), val.clone());
                }
            }
        } else {
            for ((nid, port), val) in &outputs {
                if nid == DEFAULT_SINK_NODE_ID {
                    out.insert(port.clone(), val.clone());
                }
            }
        }

        if summary.status != "done" {
            return Err(NodeError::Other(format!(
                "compose: child run terminated with status={}",
                summary.status
            )));
        }

        Ok(out)
    }
}

/// Build the child's variable map. Each value is a string; templates
/// are substituted using the parent's context so callers can pass
/// `{{inputs.x}}` / `{{vars.y}}` / `{{nodes.N.outputs.P}}`.
fn vars_from_config(node: &Node, ctx: &RunContext) -> Result<HashMap<String, String>, NodeError> {
    let raw = match node.config.get("vars") {
        None => return Ok(HashMap::new()),
        Some(serde_json::Value::Object(map)) => map,
        Some(_) => {
            return Err(NodeError::Config(
                "compose: 'vars' must be an object of {name: string}".into(),
            ));
        },
    };
    let secrets_resolver = crate::executor::context::make_secrets_resolver(ctx);
    let kv_resolver = |_: &str| None;
    let env_allow = crate::template::default_env_allowlist();
    let effective_env = node
        .target_env
        .clone()
        .unwrap_or_else(|| ctx.run_snapshot.default_env.clone());
    let resources_resolver: crate::template::BoxedResourceResolver =
        crate::template::build_run_snapshot_resources_resolver(
            std::sync::Arc::clone(&ctx.run_snapshot.registry),
            ctx.run_snapshot.workflow_id.clone(),
            effective_env,
            std::sync::Arc::clone(&ctx.run_snapshot.catalogs),
        );
    let empty_config: HashMap<String, serde_json::Value> = HashMap::new();
    let sub_ctx = crate::template::SubstitutionContext {
        vars: &ctx.variables,
        secrets: &secrets_resolver,
        upstream_outputs: &ctx.upstream_outputs,
        current_inputs: &ctx.current_inputs,
        current_config: &empty_config,
        kv: &kv_resolver,
        env: &*ctx.env,
        env_allowlist: &env_allow,
        resources: &resources_resolver,
        run_id: &ctx.run_id,
        workspace: &ctx.workspace,
        started_at_iso: &ctx.started_at_iso,
        workflow_id: &ctx.workflow_id,
        workflow_name: &ctx.workflow_name,
    };
    let mut out = HashMap::with_capacity(raw.len());
    for (name, val) in raw {
        let s = val
            .as_str()
            .ok_or_else(|| NodeError::Config(format!("compose: vars.{name} must be a string")))?;
        let rendered = crate::template::substitute(s, &sub_ctx)
            .map_err(|e| NodeError::Template(e.to_string()))?;
        out.insert(name.clone(), rendered);
    }
    Ok(out)
}

fn parse_output_spec(spec: &serde_json::Value) -> Result<(String, String), NodeError> {
    let obj = spec.as_object().ok_or_else(|| {
        NodeError::Config(
            "compose.output_map: each entry must be an object with node_id + port".into(),
        )
    })?;
    let node_id = obj
        .get("node_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            NodeError::Config("compose.output_map: 'node_id' required on each entry".into())
        })?
        .to_string();
    let port = obj
        .get("port")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            NodeError::Config("compose.output_map: 'port' required on each entry".into())
        })?
        .to_string();
    Ok((node_id, port))
}

fn outputs_to_json(outputs: &HashMap<(String, String), PortValue>) -> serde_json::Value {
    let mut grouped: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
    for ((node_id, port), value) in outputs {
        let entry = grouped
            .entry(node_id.clone())
            .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
        if let serde_json::Value::Object(map) = entry
            && let Ok(v) = serde_json::to_value(value)
        {
            map.insert(port.clone(), v);
        }
    }
    serde_json::Value::Object(grouped)
}
