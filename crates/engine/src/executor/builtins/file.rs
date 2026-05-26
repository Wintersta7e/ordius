//! `file` built-in: read / write / append / list / glob / stat.
//!
//! Paths in `config.path` (or `config.pattern` for `glob`) are
//! resolved relative to `ctx.workspace` unless absolute. All IO
//! goes through `tokio::fs`.

use crate::executor::{NodeError, NodeExecutor, NodeOutputs, RunContext};
use crate::types::{Node, NodeType, PortValue};
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use tokio_util::sync::CancellationToken;

#[allow(unreachable_pub)]
pub const NODE_TYPE_ID: &str = "file";

/// File-system built-in: read / write / append / list / glob / stat.
pub struct FileExecutor;

#[async_trait]
impl NodeExecutor for FileExecutor {
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
        let op = node
            .config
            .get("op")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                NodeError::Config("file: 'op' required — read|write|append|list|glob|stat".into())
            })?;
        match op {
            "read" => op_read(node, ctx).await,
            "write" => op_write(node, ctx, false).await,
            "append" => op_write(node, ctx, true).await,
            "list" => op_list(node, ctx).await,
            "glob" => op_glob(node, ctx),
            "stat" => op_stat(node, ctx).await,
            other => Err(NodeError::Config(format!(
                "file: unknown op '{other}' — read|write|append|list|glob|stat"
            ))),
        }
    }
}

fn config_path<'a>(node: &'a Node, key: &str, op: &str) -> Result<&'a str, NodeError> {
    super::util::config_str(&node.config, key, &format!("file.{op}"))
}

fn resolve(ctx: &RunContext, raw: &str) -> PathBuf {
    let p = Path::new(raw);
    if p.is_absolute() {
        p.to_path_buf()
    } else {
        ctx.workspace.join(p)
    }
}

async fn op_read(node: &Node, ctx: &RunContext) -> Result<NodeOutputs, NodeError> {
    let raw = config_path(node, "path", "read")?;
    let path = resolve(ctx, raw);
    let bytes = tokio::fs::read(&path).await.map_err(|e| NodeError::Io {
        context: format!("file.read: open {}", path.display()),
        source: e,
    })?;
    let mut out = NodeOutputs::new();
    match std::str::from_utf8(&bytes) {
        Ok(s) => {
            out.insert("text".into(), PortValue::String(s.to_string()));
        },
        Err(_) => {
            // Binary payload: hand the caller the path so they can
            // stream it themselves or pass it to a binary-aware node.
            out.insert("bytes".into(), PortValue::File(path.display().to_string()));
        },
    }
    Ok(out)
}

async fn op_write(node: &Node, ctx: &RunContext, append: bool) -> Result<NodeOutputs, NodeError> {
    let op_name = if append { "append" } else { "write" };
    let raw = config_path(node, "path", op_name)?;
    let content = super::util::config_str(&node.config, "content", &format!("file.{op_name}"))?;
    let path = resolve(ctx, raw);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| NodeError::Io {
                context: format!("file.{op_name}: mkdir {}", parent.display()),
                source: e,
            })?;
    }
    if append {
        use tokio::io::AsyncWriteExt;
        let mut f = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|e| NodeError::Io {
                context: format!("file.append: open {}", path.display()),
                source: e,
            })?;
        f.write_all(content.as_bytes())
            .await
            .map_err(|e| NodeError::Io {
                context: format!("file.append: write {}", path.display()),
                source: e,
            })?;
    } else {
        tokio::fs::write(&path, content)
            .await
            .map_err(|e| NodeError::Io {
                context: format!("file.write: write {}", path.display()),
                source: e,
            })?;
    }
    let mut out = NodeOutputs::new();
    out.insert("path".into(), PortValue::String(path.display().to_string()));
    Ok(out)
}

async fn op_list(node: &Node, ctx: &RunContext) -> Result<NodeOutputs, NodeError> {
    let raw = config_path(node, "path", "list")?;
    let path = resolve(ctx, raw);
    let mut rd = tokio::fs::read_dir(&path)
        .await
        .map_err(|e| NodeError::Io {
            context: format!("file.list: read_dir {}", path.display()),
            source: e,
        })?;
    let mut entries: Vec<serde_json::Value> = Vec::new();
    while let Some(entry) = rd.next_entry().await.map_err(|e| NodeError::Io {
        context: format!("file.list: iterate {}", path.display()),
        source: e,
    })? {
        entries.push(serde_json::Value::String(
            entry.path().display().to_string(),
        ));
    }
    entries.sort_by(|a, b| a.as_str().unwrap_or("").cmp(b.as_str().unwrap_or("")));
    let mut out = NodeOutputs::new();
    out.insert(
        "entries".into(),
        PortValue::Json(serde_json::Value::Array(entries)),
    );
    Ok(out)
}

fn op_glob(node: &Node, ctx: &RunContext) -> Result<NodeOutputs, NodeError> {
    let raw = config_path(node, "pattern", "glob")?;
    let pattern_path = resolve(ctx, raw);
    let pattern = pattern_path.to_string_lossy().into_owned();
    let glob_iter = glob::glob(&pattern)
        .map_err(|e| NodeError::Config(format!("file.glob: invalid pattern: {e}")))?;
    let mut entries: Vec<serde_json::Value> = Vec::new();
    for res in glob_iter {
        let p = res.map_err(|e| NodeError::Io {
            context: format!("file.glob: walk {pattern}"),
            source: e.into_error(),
        })?;
        entries.push(serde_json::Value::String(p.display().to_string()));
    }
    entries.sort_by(|a, b| a.as_str().unwrap_or("").cmp(b.as_str().unwrap_or("")));
    let mut out = NodeOutputs::new();
    out.insert(
        "entries".into(),
        PortValue::Json(serde_json::Value::Array(entries)),
    );
    Ok(out)
}

async fn op_stat(node: &Node, ctx: &RunContext) -> Result<NodeOutputs, NodeError> {
    let raw = config_path(node, "path", "stat")?;
    let path = resolve(ctx, raw);
    let md = tokio::fs::metadata(&path)
        .await
        .map_err(|e| NodeError::Io {
            context: format!("file.stat: metadata {}", path.display()),
            source: e,
        })?;
    let size = md.len();
    let is_dir = md.is_dir();
    let modified_ms = md
        .modified()
        .ok()
        .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
        .and_then(|d| i64::try_from(d.as_millis()).ok())
        .unwrap_or(0);
    let info = serde_json::json!({
        "size": size,
        "modified_at": modified_ms,
        "is_dir": is_dir,
    });
    let mut out = NodeOutputs::new();
    out.insert("info".into(), PortValue::Json(info));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::test_support::make_ctx;
    use crate::types::{Category, ExecutionBackend, ExecutionSpec, OutputParse, Pos};
    use std::collections::HashMap;

    fn file_nt() -> NodeType {
        NodeType {
            id: NODE_TYPE_ID.into(),
            name: String::new(),
            category: Category::Data,
            tags: vec![],
            icon: String::new(),
            description: String::new(),
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

    fn file_node(config: serde_json::Value) -> Node {
        let config: HashMap<String, serde_json::Value> = serde_json::from_value(config).unwrap();
        Node {
            id: "n".into(),
            ty: NODE_TYPE_ID.into(),
            name: String::new(),
            config,
            pos: Pos::default(),
            timeout_ms: None,
            retry: None,
            continue_on_error: false,
            target_env: None,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_then_read_roundtrips() {
        let (ctx, _rx, _dir) = make_ctx();
        let write_node = file_node(serde_json::json!({
            "op": "write",
            "path": "hello.txt",
            "content": "hi there\nline two",
        }));
        let out = FileExecutor
            .run(&write_node, &file_nt(), &ctx, CancellationToken::new())
            .await
            .expect("write should succeed");
        assert!(matches!(out.get("path"), Some(PortValue::String(_))));

        let read_node = file_node(serde_json::json!({
            "op": "read",
            "path": "hello.txt",
        }));
        let out = FileExecutor
            .run(&read_node, &file_nt(), &ctx, CancellationToken::new())
            .await
            .expect("read should succeed");
        assert_eq!(
            out.get("text"),
            Some(&PortValue::String("hi there\nline two".into()))
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn append_extends_existing_file() {
        let (ctx, _rx, _dir) = make_ctx();
        for content in ["first ", "second"] {
            let node = file_node(serde_json::json!({
                "op": "append",
                "path": "log.txt",
                "content": content,
            }));
            FileExecutor
                .run(&node, &file_nt(), &ctx, CancellationToken::new())
                .await
                .expect("append should succeed");
        }
        let read = file_node(serde_json::json!({"op":"read","path":"log.txt"}));
        let out = FileExecutor
            .run(&read, &file_nt(), &ctx, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            out.get("text"),
            Some(&PortValue::String("first second".into()))
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn list_returns_sorted_entries() {
        let (ctx, _rx, _dir) = make_ctx();
        for name in ["b.txt", "a.txt", "c.txt"] {
            FileExecutor
                .run(
                    &file_node(serde_json::json!({
                        "op": "write", "path": name, "content": "x",
                    })),
                    &file_nt(),
                    &ctx,
                    CancellationToken::new(),
                )
                .await
                .unwrap();
        }
        let out = FileExecutor
            .run(
                &file_node(serde_json::json!({"op":"list","path":"."})),
                &file_nt(),
                &ctx,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let entries = match out.get("entries").unwrap() {
            PortValue::Json(serde_json::Value::Array(a)) => a.clone(),
            other => panic!("expected Json array, got {other:?}"),
        };
        // Workspace also contains the `t.db` SQLite file from
        // make_ctx, so we filter to *.txt.
        let txt: Vec<String> = entries
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .filter(|s| {
                Path::new(s)
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("txt"))
            })
            .collect();
        assert_eq!(txt.len(), 3);
        let names: Vec<&str> = txt
            .iter()
            .map(|s| s.rsplit_once('/').map_or(s.as_str(), |(_, n)| n))
            .collect();
        assert_eq!(names, vec!["a.txt", "b.txt", "c.txt"]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn glob_picks_matching_paths() {
        let (ctx, _rx, _dir) = make_ctx();
        for name in ["a.md", "b.txt", "c.md"] {
            FileExecutor
                .run(
                    &file_node(serde_json::json!({
                        "op": "write", "path": name, "content": "x",
                    })),
                    &file_nt(),
                    &ctx,
                    CancellationToken::new(),
                )
                .await
                .unwrap();
        }
        let out = FileExecutor
            .run(
                &file_node(serde_json::json!({"op":"glob","pattern":"*.md"})),
                &file_nt(),
                &ctx,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let entries = match out.get("entries").unwrap() {
            PortValue::Json(serde_json::Value::Array(a)) => a.clone(),
            other => panic!("expected Json array, got {other:?}"),
        };
        assert_eq!(entries.len(), 2, "got {entries:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stat_reports_size_and_dirness() {
        let (ctx, _rx, _dir) = make_ctx();
        FileExecutor
            .run(
                &file_node(serde_json::json!({
                    "op":"write","path":"sized.bin","content":"abcdef",
                })),
                &file_nt(),
                &ctx,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let out = FileExecutor
            .run(
                &file_node(serde_json::json!({"op":"stat","path":"sized.bin"})),
                &file_nt(),
                &ctx,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let info = match out.get("info").unwrap() {
            PortValue::Json(v) => v.clone(),
            other => panic!("expected Json, got {other:?}"),
        };
        assert_eq!(info["size"], serde_json::json!(6));
        assert_eq!(info["is_dir"], serde_json::json!(false));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unknown_op_fails_config() {
        let (ctx, _rx, _dir) = make_ctx();
        let err = FileExecutor
            .run(
                &file_node(serde_json::json!({"op":"unfurl","path":"x"})),
                &file_nt(),
                &ctx,
                CancellationToken::new(),
            )
            .await
            .expect_err("unknown op must fail");
        assert!(matches!(err, NodeError::Config(_)), "got {err:?}");
    }
}
