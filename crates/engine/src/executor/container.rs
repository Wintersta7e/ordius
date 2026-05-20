//! Container executor: shells out to `docker` to run a container per
//! invocation.
//!
//! v1.1 uses the Docker CLI rather than `bollard` — fastest reliable
//! path for a personal-tool single-user setup, works with Docker
//! Desktop on Windows, and reuses the same line-buffered streaming +
//! grace-then-kill cancellation shape the subprocess executor has.

use crate::executor::{NodeError, NodeExecutor, NodeOutputs, RunContext};
use crate::types::{Node, NodeType, PortValue};
use async_trait::async_trait;
use std::collections::HashMap;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

const DEFAULT_GRACE_SECS: u64 = 5;
const CHANNEL_STDOUT: &str = "stdout";
const CHANNEL_STDERR: &str = "stderr";

/// Container executor — see module docs.
pub struct ContainerExecutor;

#[async_trait]
impl NodeExecutor for ContainerExecutor {
    fn supports(&self, _nt: &NodeType) -> bool {
        // Dispatcher routes by `ExecutionBackend::Container`, not by
        // node-type id, so any node declared with the container
        // backend lands here.
        true
    }

    #[allow(clippy::too_many_lines)]
    async fn run(
        &self,
        node: &Node,
        _nt: &NodeType,
        ctx: &RunContext,
        cancel: CancellationToken,
    ) -> Result<NodeOutputs, NodeError> {
        let image = node
            .config
            .get("image")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| NodeError::Config("container: 'image' (string) required".into()))?;
        let command = parse_command(&node.config)?;
        let workdir = config_str_or(&node.config, "workdir", "/workspace");
        let network = config_str_or(&node.config, "network", "none");
        let mount_workspace = node
            .config
            .get("mount_workspace")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true);
        let workspace_mode = config_str_or(&node.config, "workspace_mode", "rw");
        let grace_secs = node
            .config
            .get("stop_grace_secs")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(DEFAULT_GRACE_SECS);
        let env_pairs = parse_env(&node.config)?;
        let container_name = format!(
            "ordius-{}-{}-{}-{}",
            sanitise_for_docker(&ctx.run_id),
            sanitise_for_docker(&node.id),
            ctx.iteration,
            ctx.attempt.load(std::sync::atomic::Ordering::SeqCst),
        );

        let mut docker_args: Vec<String> = vec![
            "run".into(),
            "--rm".into(),
            "--name".into(),
            container_name.clone(),
            "--workdir".into(),
            workdir.into(),
            "--network".into(),
            network.into(),
        ];
        if mount_workspace {
            let mount = format!(
                "{}:{}:{}",
                ctx.workspace.to_string_lossy(),
                "/workspace",
                workspace_mode,
            );
            docker_args.push("-v".into());
            docker_args.push(mount);
        }
        for (k, v) in &env_pairs {
            docker_args.push("-e".into());
            docker_args.push(format!("{k}={v}"));
        }
        docker_args.push(image.into());
        docker_args.extend(command);

        let mut child = Command::new("docker")
            .args(&docker_args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| NodeError::Other(format!("container: spawn docker: {e}")))?;

        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let stdout_task = stream_lines(
            stdout,
            ctx.clone_for_streaming(),
            node.id.clone(),
            CHANNEL_STDOUT,
        );
        let stderr_task = stream_lines(
            stderr,
            ctx.clone_for_streaming(),
            node.id.clone(),
            CHANNEL_STDERR,
        );

        let status = tokio::select! {
            r = child.wait() => r.map_err(|e| NodeError::Other(format!("container: wait: {e}")))?,
            () = cancel.cancelled() => {
                // Grace → kill. Best-effort; ignore secondary failures.
                drop(
                    Command::new("docker")
                        .args(["stop", "--time", &grace_secs.to_string(), &container_name])
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .status()
                        .await,
                );
                drop(
                    Command::new("docker")
                        .args(["kill", &container_name])
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .status()
                        .await,
                );
                drop(child.wait().await);
                return Err(NodeError::Cancelled);
            }
        };

        drop(stdout_task.await);
        drop(stderr_task.await);

        let exit_code = status.code().unwrap_or(-1);
        let mut out = NodeOutputs::new();
        out.insert("exit_code".into(), PortValue::Number(f64::from(exit_code)));
        // `text` mirrors shell's contract; container doesn't capture
        // stdout into a port value because we stream it. Provide an
        // empty placeholder so downstream nodes can still wire it.
        out.insert("text".into(), PortValue::String(String::new()));
        Ok(out)
    }
}

fn config_str_or<'a>(
    cfg: &'a HashMap<String, serde_json::Value>,
    key: &str,
    fallback: &'a str,
) -> &'a str {
    cfg.get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or(fallback)
}

fn parse_command(config: &HashMap<String, serde_json::Value>) -> Result<Vec<String>, NodeError> {
    let Some(value) = config.get("command") else {
        return Ok(Vec::new());
    };
    let arr = value
        .as_array()
        .ok_or_else(|| NodeError::Config("container: 'command' must be a string array".into()))?;
    let mut out = Vec::with_capacity(arr.len());
    for (i, item) in arr.iter().enumerate() {
        let s = item
            .as_str()
            .ok_or_else(|| NodeError::Config(format!("container.command[{i}] must be a string")))?;
        out.push(s.to_string());
    }
    Ok(out)
}

fn parse_env(
    config: &HashMap<String, serde_json::Value>,
) -> Result<Vec<(String, String)>, NodeError> {
    let Some(value) = config.get("env") else {
        return Ok(Vec::new());
    };
    let obj = value.as_object().ok_or_else(|| {
        NodeError::Config("container: 'env' must be an object of {NAME: value}".into())
    })?;
    let mut out = Vec::with_capacity(obj.len());
    for (k, v) in obj {
        let s = v
            .as_str()
            .ok_or_else(|| NodeError::Config(format!("container.env.{k} must be a string")))?;
        out.push((k.clone(), s.to_string()));
    }
    Ok(out)
}

fn sanitise_for_docker(input: &str) -> String {
    input
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn stream_lines<R: tokio::io::AsyncRead + Unpin + Send + 'static>(
    pipe: Option<R>,
    ctx_streaming: StreamingCtx,
    node_id: String,
    channel: &'static str,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let Some(pipe) = pipe else {
            return;
        };
        let mut reader = BufReader::new(pipe).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            let mut payload: HashMap<String, serde_json::Value> = HashMap::with_capacity(2);
            payload.insert("channel".into(), serde_json::json!(channel));
            payload.insert("text".into(), serde_json::json!(line));
            ctx_streaming.emitter.emit(
                crate::events::EventType::NodeOutput,
                Some(node_id.clone()),
                Some(ctx_streaming.iteration),
                Some(ctx_streaming.attempt),
                payload,
            );
        }
    })
}

/// Snapshot of the bits of `RunContext` we need to push streaming
/// events from background reader tasks (which can't borrow `&RunContext`).
struct StreamingCtx {
    emitter: std::sync::Arc<crate::emitter::Emitter>,
    iteration: u32,
    attempt: u32,
}

impl RunContext {
    /// Build a streaming-time snapshot for tasks that emit events from
    /// background pipes. Internal-only helper for the container
    /// executor; reusing the full `RunContext` from a spawn would require
    /// it to be `Clone`, which we deliberately avoid.
    fn clone_for_streaming(&self) -> StreamingCtx {
        StreamingCtx {
            emitter: self.emitter.clone(),
            iteration: self.iteration,
            attempt: self.attempt.load(std::sync::atomic::Ordering::SeqCst),
        }
    }
}

/// Sentinel duration the docker-stop default uses if the config field
/// isn't supplied. Kept as a `Duration` rather than seconds because
/// future code may grow more precise scheduling.
#[allow(dead_code)]
const DEFAULT_GRACE: Duration = Duration::from_secs(DEFAULT_GRACE_SECS);
