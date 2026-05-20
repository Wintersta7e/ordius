//! `kv` built-in: get / set / delete on the per-workflow key-value
//! store (`SQLite` `kv_store` table, keyed by `(workflow_id, key)`).

use crate::executor::{NodeError, NodeExecutor, NodeOutputs, RunContext};
use crate::types::{Node, NodeType, PortValue};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

#[allow(unreachable_pub)]
pub const NODE_TYPE_ID: &str = "kv";

/// Built-in `kv` executor — workflow-scoped persistent storage.
pub struct KvExecutor;

#[async_trait]
impl NodeExecutor for KvExecutor {
    fn supports(&self, nt: &NodeType) -> bool {
        nt.id == NODE_TYPE_ID
    }

    async fn run(
        &self,
        node: &Node,
        _nt: &NodeType,
        ctx: &RunContext,
        _cancel: CancellationToken,
    ) -> Result<NodeOutputs, NodeError> {
        let op = super::util::config_str(&node.config, "op", "kv")?;
        let key = super::util::config_str(&node.config, "key", "kv")?;
        let workflow_id = ctx.workflow_id.clone();
        let pool = ctx.recorder.pool().clone();
        match op {
            "get" => kv_get(&pool, &workflow_id, key),
            "set" => {
                let value = super::util::config_str(&node.config, "value", "kv.set")?;
                kv_set(&pool, &workflow_id, key, value)
            },
            "delete" => kv_delete(&pool, &workflow_id, key),
            other => Err(NodeError::Config(format!(
                "kv: unknown op '{other}' — get|set|delete"
            ))),
        }
    }
}

fn kv_get(pool: &crate::db::DbPool, wf: &str, key: &str) -> Result<NodeOutputs, NodeError> {
    let conn = pool
        .get()
        .map_err(|e| NodeError::Other(format!("kv: pool: {e}")))?;
    let value: Option<String> = conn
        .query_row(
            "SELECT value FROM kv_store WHERE workflow_id = ? AND key = ?",
            rusqlite::params![wf, key],
            |r| r.get::<_, String>(0),
        )
        .ok();
    let mut out = NodeOutputs::new();
    out.insert("exists".into(), PortValue::Boolean(value.is_some()));
    out.insert(
        "value".into(),
        value.map_or(PortValue::Json(serde_json::Value::Null), PortValue::String),
    );
    Ok(out)
}

fn kv_set(
    pool: &crate::db::DbPool,
    wf: &str,
    key: &str,
    value: &str,
) -> Result<NodeOutputs, NodeError> {
    let conn = pool
        .get()
        .map_err(|e| NodeError::Other(format!("kv: pool: {e}")))?;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_i64, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
    conn.execute(
        "INSERT INTO kv_store (workflow_id, key, value, updated_at) VALUES (?, ?, ?, ?) \
         ON CONFLICT (workflow_id, key) DO UPDATE SET value = excluded.value, \
         updated_at = excluded.updated_at",
        rusqlite::params![wf, key, value, now_ms],
    )
    .map_err(|e| NodeError::Other(format!("kv.set: {e}")))?;
    let mut out = NodeOutputs::new();
    out.insert("value".into(), PortValue::String(value.to_string()));
    Ok(out)
}

fn kv_delete(pool: &crate::db::DbPool, wf: &str, key: &str) -> Result<NodeOutputs, NodeError> {
    let conn = pool
        .get()
        .map_err(|e| NodeError::Other(format!("kv: pool: {e}")))?;
    let removed = conn
        .execute(
            "DELETE FROM kv_store WHERE workflow_id = ? AND key = ?",
            rusqlite::params![wf, key],
        )
        .map_err(|e| NodeError::Other(format!("kv.delete: {e}")))?;
    let mut out = NodeOutputs::new();
    out.insert("existed".into(), PortValue::Boolean(removed > 0));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::test_support::make_ctx;
    use crate::types::{Category, ExecutionBackend, ExecutionSpec, OutputParse};
    use serde_json::json;
    use std::collections::HashMap;

    fn kv_nt() -> NodeType {
        NodeType {
            id: NODE_TYPE_ID.into(),
            name: "KV".into(),
            category: Category::Data,
            tags: vec![],
            icon: "database".into(),
            description: "kv".into(),
            inputs: vec![],
            outputs: vec![],
            config: vec![],
            execution: ExecutionSpec {
                backend: ExecutionBackend::InProcess,
                command: vec![],
                stdin_template: None,
                env: HashMap::new(),
                timeout_ms: None,
                output_parse: OutputParse::Text,
                output_map: HashMap::new(),
            },
            skip_config_templates: false,
        }
    }

    fn kv_node(extra: serde_json::Value) -> Node {
        let mut config = HashMap::new();
        if let serde_json::Value::Object(map) = extra {
            for (k, v) in map {
                config.insert(k, v);
            }
        }
        Node {
            id: "n1".into(),
            ty: NODE_TYPE_ID.into(),
            name: "kv".into(),
            config,
            pos: crate::types::Pos { x: 0.0, y: 0.0 },
            timeout_ms: None,
            retry: None,
            continue_on_error: false,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn set_then_get_round_trip() {
        let (ctx, _rx, _td) = make_ctx();
        let exec = KvExecutor;

        let set_out = exec
            .run(
                &kv_node(json!({"op": "set", "key": "k1", "value": "v1"})),
                &kv_nt(),
                &ctx,
                CancellationToken::new(),
            )
            .await
            .expect("set");
        assert_eq!(set_out.get("value"), Some(&PortValue::String("v1".into())),);

        let get_out = exec
            .run(
                &kv_node(json!({"op": "get", "key": "k1"})),
                &kv_nt(),
                &ctx,
                CancellationToken::new(),
            )
            .await
            .expect("get");
        assert_eq!(get_out.get("exists"), Some(&PortValue::Boolean(true)));
        assert_eq!(get_out.get("value"), Some(&PortValue::String("v1".into())),);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_missing_returns_null_value() {
        let (ctx, _rx, _td) = make_ctx();
        let exec = KvExecutor;
        let out = exec
            .run(
                &kv_node(json!({"op": "get", "key": "absent"})),
                &kv_nt(),
                &ctx,
                CancellationToken::new(),
            )
            .await
            .expect("get");
        assert_eq!(out.get("exists"), Some(&PortValue::Boolean(false)));
        assert_eq!(
            out.get("value"),
            Some(&PortValue::Json(serde_json::Value::Null)),
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delete_existing_then_missing() {
        let (ctx, _rx, _td) = make_ctx();
        let exec = KvExecutor;
        exec.run(
            &kv_node(json!({"op": "set", "key": "k2", "value": "x"})),
            &kv_nt(),
            &ctx,
            CancellationToken::new(),
        )
        .await
        .expect("set");
        let del1 = exec
            .run(
                &kv_node(json!({"op": "delete", "key": "k2"})),
                &kv_nt(),
                &ctx,
                CancellationToken::new(),
            )
            .await
            .expect("delete-existing");
        assert_eq!(del1.get("existed"), Some(&PortValue::Boolean(true)));
        let del2 = exec
            .run(
                &kv_node(json!({"op": "delete", "key": "k2"})),
                &kv_nt(),
                &ctx,
                CancellationToken::new(),
            )
            .await
            .expect("delete-missing");
        assert_eq!(del2.get("existed"), Some(&PortValue::Boolean(false)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unknown_op_returns_config_error() {
        let (ctx, _rx, _td) = make_ctx();
        let exec = KvExecutor;
        let err = exec
            .run(
                &kv_node(json!({"op": "frob", "key": "k"})),
                &kv_nt(),
                &ctx,
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, NodeError::Config(_)));
    }
}
