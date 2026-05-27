//! `parallel` built-in: compose-map. Fans out a named child workflow
//! once per item in a configurable array, runs them concurrently up to
//! `max_concurrent`, and joins their outputs into a `results` JSON array.
//!
//! Modeled as Codex recommended in the B8 design pass: v1.1 ships only
//! the `all` mode, fail-fast cancellation of siblings, no per-port
//! merge strategies. Items can come either from a typed input port
//! `items` (a `PortValue::Json` array) or `config.items` (a static
//! JSON array); the input port wins when both exist.

use super::util::{config_str, config_str_or, config_u64_or};
use crate::executor::{NodeError, NodeExecutor, NodeOutputs, RunContext};
use crate::types::{Node, NodeType, PortValue};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

const DEFAULT_MAX_CONCURRENT: u64 = 4;
const DEFAULT_ITEM_VAR: &str = "item";
const DEFAULT_INDEX_VAR: &str = "index";

#[allow(unreachable_pub)]
pub const NODE_TYPE_ID: &str = "parallel";

/// Join semantics for the fan-out:
/// - `All`: wait for every child, fail-fast on first error (v1.1 default).
/// - `Any`: succeed on first success, cancel siblings; if every child
///   fails, return Err.
/// - `Race`: take the first finisher regardless of status, cancel siblings.
#[derive(Debug, Clone, Copy)]
enum ParallelMode {
    All,
    Any,
    Race,
}

/// Parallel fan-out executor — see module docs.
pub struct ParallelExecutor;

#[async_trait]
impl NodeExecutor for ParallelExecutor {
    fn supports(&self, nt: &NodeType) -> bool {
        nt.id == NODE_TYPE_ID
    }

    #[allow(clippy::too_many_lines)]
    async fn run(
        &self,
        node: &Node,
        _nt: &NodeType,
        ctx: &RunContext,
        cancel: CancellationToken,
    ) -> Result<NodeOutputs, NodeError> {
        let workflow_id = config_str(&node.config, "workflow_id", "parallel")?.to_string();
        let item_var = config_str_or(&node.config, "item_var", DEFAULT_ITEM_VAR).to_string();
        let index_var = config_str_or(&node.config, "index_var", DEFAULT_INDEX_VAR).to_string();
        let max_concurrent = usize::try_from(config_u64_or(
            &node.config,
            "max_concurrent",
            DEFAULT_MAX_CONCURRENT,
        ))
        .unwrap_or(4)
        .max(1);
        let mode = match config_str_or(&node.config, "mode", "all") {
            "all" => ParallelMode::All,
            "any" => ParallelMode::Any,
            "race" => ParallelMode::Race,
            other => {
                return Err(NodeError::Config(format!(
                    "parallel: unknown mode '{other}' — expected all|any|race"
                )));
            },
        };

        let items = resolve_items(node, ctx)?;
        if items.is_empty() {
            let mut out = NodeOutputs::new();
            out.insert("results".into(), PortValue::Json(serde_json::json!([])));
            return Ok(out);
        }

        let engine = ctx.engine.upgrade().ok_or_else(|| {
            NodeError::Other("parallel: engine handle gone (shutdown in progress)".into())
        })?;
        let child_wf = crate::workflows::load(engine.home(), &workflow_id)
            .map_err(|e| NodeError::Config(format!("parallel: load workflow: {e}")))?;
        let child_wf = Arc::new(child_wf);

        let base_vars = base_vars_from_config(&node.config, ctx)?;
        let next_depth = ctx.compose_depth.saturating_add(1);
        // Cancellation that covers the whole fan-out: any child error
        // fires this to drop siblings.
        let group_cancel = cancel.child_token();

        let mut joinset: JoinSet<(usize, Result<serde_json::Value, String>)> = JoinSet::new();
        let mut next_index = 0usize;
        let mut results: HashMap<usize, serde_json::Value> = HashMap::with_capacity(items.len());

        // Pre-fill the joinset up to the concurrency cap.
        while next_index < items.len() && joinset.len() < max_concurrent {
            spawn_one(
                &mut joinset,
                &items,
                next_index,
                &engine,
                Arc::clone(&child_wf),
                &base_vars,
                &item_var,
                &index_var,
                &group_cancel,
                next_depth,
                &ctx.workspace,
                &ctx.run_snapshot,
            );
            next_index += 1;
        }

        let mut errors: HashMap<usize, String> = HashMap::new();
        let mut winner: Option<serde_json::Value> = None;
        // Drain results, spawning replacement children as slots free.
        while let Some(joined) = joinset.join_next().await {
            let (idx, outcome) = joined
                .map_err(|e| NodeError::Other(format!("parallel: child task join error: {e}")))?;
            match (&mode, outcome) {
                (ParallelMode::All, Ok(value)) => {
                    results.insert(idx, value);
                },
                (ParallelMode::All, Err(err)) => {
                    // Fail-fast: cancel siblings before bubbling up.
                    group_cancel.cancel();
                    while joinset.join_next().await.is_some() {}
                    return Err(NodeError::Other(format!(
                        "parallel: child[{idx}] failed: {err}"
                    )));
                },
                (ParallelMode::Any, Ok(value)) => {
                    // First success wins; cancel siblings + drain.
                    winner = Some(value);
                    group_cancel.cancel();
                    while joinset.join_next().await.is_some() {}
                    break;
                },
                (ParallelMode::Any, Err(err)) => {
                    // Track error; keep going — maybe another child succeeds.
                    errors.insert(idx, err);
                },
                (ParallelMode::Race, outcome) => {
                    // First to finish wins, regardless of success/fail.
                    // Use a synthetic value that mirrors success rows when
                    // possible, or wrap the failure for the Err arm.
                    winner = Some(match outcome {
                        Ok(v) => v,
                        Err(e) => serde_json::json!({
                            "index": idx,
                            "status": "error",
                            "error": e,
                        }),
                    });
                    group_cancel.cancel();
                    while joinset.join_next().await.is_some() {}
                    break;
                },
            }
            if next_index < items.len() {
                spawn_one(
                    &mut joinset,
                    &items,
                    next_index,
                    &engine,
                    Arc::clone(&child_wf),
                    &base_vars,
                    &item_var,
                    &index_var,
                    &group_cancel,
                    next_depth,
                    &ctx.workspace,
                    &ctx.run_snapshot,
                );
                next_index += 1;
            }
        }

        let ordered: Vec<serde_json::Value> = match mode {
            ParallelMode::All => (0..items.len())
                .map(|i| results.remove(&i).unwrap_or(serde_json::Value::Null))
                .collect(),
            ParallelMode::Any => {
                if let Some(v) = winner {
                    vec![v]
                } else {
                    // No success — surface the per-child errors.
                    let mut sorted: Vec<_> = errors.into_iter().collect();
                    sorted.sort_by_key(|(i, _)| *i);
                    return Err(NodeError::Other(format!(
                        "parallel.any: all {} children failed: {}",
                        sorted.len(),
                        sorted
                            .iter()
                            .map(|(i, e)| format!("[{i}]: {e}"))
                            .collect::<Vec<_>>()
                            .join("; ")
                    )));
                }
            },
            ParallelMode::Race => winner.into_iter().collect(),
        };
        let mut out = NodeOutputs::new();
        out.insert(
            "results".into(),
            PortValue::Json(serde_json::json!(ordered)),
        );
        Ok(out)
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_one(
    joinset: &mut JoinSet<(usize, Result<serde_json::Value, String>)>,
    items: &[serde_json::Value],
    index: usize,
    engine: &Arc<crate::Engine>,
    child_wf: Arc<crate::types::Workflow>,
    base_vars: &HashMap<String, String>,
    item_var: &str,
    index_var: &str,
    group_cancel: &CancellationToken,
    next_depth: u32,
    workspace: &std::path::Path,
    parent_snapshot: &Arc<crate::environment::runtime::RunSnapshot>,
) {
    let item = items[index].clone();
    let engine = Arc::clone(engine);
    let workspace = workspace.to_path_buf();
    let parent_snapshot = Arc::clone(parent_snapshot);
    let mut vars = base_vars.clone();
    let item_str = match &item {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    vars.insert(item_var.to_string(), item_str);
    vars.insert(index_var.to_string(), index.to_string());
    let child_cancel = group_cancel.clone();
    joinset.spawn(async move {
        let res = engine
            .run_child_workflow(
                child_wf,
                vars,
                &child_cancel,
                next_depth,
                Some(workspace),
                "parallel-",
                parent_snapshot,
            )
            .await;
        match res {
            Ok((summary, outputs)) => {
                let mut entry = serde_json::Map::new();
                entry.insert("index".into(), serde_json::json!(index));
                entry.insert("item".into(), item);
                entry.insert("run_id".into(), serde_json::json!(summary.run_id));
                entry.insert("status".into(), serde_json::json!(summary.status));
                entry.insert("outputs".into(), outputs_to_json(&outputs));
                if summary.status == "done" {
                    (index, Ok(serde_json::Value::Object(entry)))
                } else {
                    (index, Err(format!("status={}", summary.status)))
                }
            },
            Err(e) => (index, Err(e.to_string())),
        }
    });
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

fn resolve_items(node: &Node, ctx: &RunContext) -> Result<Vec<serde_json::Value>, NodeError> {
    // Input port `items` takes precedence over config.items.
    if let Some(input) = ctx.current_inputs.get("items") {
        return match input {
            PortValue::Json(serde_json::Value::Array(items)) => Ok(items.clone()),
            _ => Err(NodeError::Config(
                "parallel: input port 'items' must be a JSON array".into(),
            )),
        };
    }
    let Some(value) = node.config.get("items") else {
        return Err(NodeError::Config(
            "parallel: provide an `items` input port or config.items array".into(),
        ));
    };
    match value {
        serde_json::Value::Array(items) => Ok(items.clone()),
        _ => Err(NodeError::Config(
            "parallel: config.items must be an array".into(),
        )),
    }
}

fn base_vars_from_config(
    config: &HashMap<String, serde_json::Value>,
    ctx: &RunContext,
) -> Result<HashMap<String, String>, NodeError> {
    let raw = match config.get("vars") {
        None => return Ok(HashMap::new()),
        Some(serde_json::Value::Object(map)) => map,
        Some(_) => {
            return Err(NodeError::Config(
                "parallel: 'vars' must be an object of {name: string}".into(),
            ));
        },
    };
    let secrets_resolver = crate::executor::context::make_secrets_resolver(ctx);
    let kv_resolver = |_: &str| None;
    let env_allow = crate::template::default_env_allowlist();
    let resources_resolver: crate::template::BoxedResourceResolver =
        if let Some(engine) = ctx.engine.upgrade() {
            Box::new(crate::template::build_resources_resolver(
                engine.resource_registry(),
                ctx.workflow_id.clone(),
            ))
        } else {
            Box::new(|_, _| None)
        };
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
            .ok_or_else(|| NodeError::Config(format!("parallel: vars.{name} must be a string")))?;
        let rendered = crate::template::substitute(s, &sub_ctx)
            .map_err(|e| NodeError::Template(e.to_string()))?;
        out.insert(name.clone(), rendered);
    }
    Ok(out)
}
