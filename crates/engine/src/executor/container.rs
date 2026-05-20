//! Container executor: native Docker via `bollard`.
//!
//! v1.2 promoted the backend from `docker` CLI shell-out to the
//! `bollard` async client per the original B10 design pass. We get
//! typed errors, structured log streaming, and explicit image-pull
//! progress events — at the cost of taking a daemon-connection
//! dependency at run time (the previous CLI version inherited the
//! same dep transitively).
//!
//! Config compatibility is preserved (same fields, same defaults).
//! The new `pull` field — `"missing"` (default), `"always"`,
//! `"never"` — controls whether the executor pulls the image before
//! creating the container.

use crate::events::EventType;
use crate::executor::{NodeError, NodeExecutor, NodeOutputs, RunContext};
use crate::types::{Node, NodeType, PortValue};
use async_trait::async_trait;
use bollard::Docker;
use bollard::container::{
    Config as ContainerConfig, CreateContainerOptions, LogOutput, LogsOptions,
    RemoveContainerOptions, StopContainerOptions, WaitContainerOptions,
};
use bollard::image::CreateImageOptions;
use bollard::secret::HostConfig;
use futures::StreamExt;
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;

const DEFAULT_GRACE_SECS: u64 = 5;
const CHANNEL_STDOUT: &str = "stdout";
const CHANNEL_STDERR: &str = "stderr";
const CHANNEL_PULL: &str = "container_pull";

/// Container executor — see module docs.
pub struct ContainerExecutor;

#[async_trait]
impl NodeExecutor for ContainerExecutor {
    fn supports(&self, _nt: &NodeType) -> bool {
        // Dispatcher routes by ExecutionBackend::Container, not by id.
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
            .ok_or_else(|| NodeError::Config("container: 'image' (string) required".into()))?
            .to_string();
        let command = parse_command(&node.config)?;
        let workdir = config_str_or(&node.config, "workdir", "/workspace").to_string();
        let network = config_str_or(&node.config, "network", "none").to_string();
        let mount_workspace = node
            .config
            .get("mount_workspace")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true);
        let workspace_mode = config_str_or(&node.config, "workspace_mode", "rw").to_string();
        let grace_secs = node
            .config
            .get("stop_grace_secs")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(DEFAULT_GRACE_SECS);
        let env_pairs = parse_env(&node.config)?;
        let pull_mode = config_str_or(&node.config, "pull", "missing").to_string();
        let container_name = format!(
            "ordius-{}-{}-{}-{}",
            sanitise_for_docker(&ctx.run_id),
            sanitise_for_docker(&node.id),
            ctx.iteration,
            ctx.attempt.load(std::sync::atomic::Ordering::SeqCst),
        );

        let docker = Docker::connect_with_local_defaults()
            .map_err(|e| NodeError::Other(format!("container: connect docker: {e}")))?;

        // Pull the image if the policy requires it. `missing` checks
        // inspect_image first; `always` pulls unconditionally; `never`
        // skips and trusts the image already exists locally.
        let needs_pull = match pull_mode.as_str() {
            "always" => true,
            "never" => false,
            _ => docker.inspect_image(&image).await.is_err(),
        };
        if needs_pull {
            pull_image(&docker, &image, ctx, &node.id, &cancel).await?;
        }

        let env_strings: Vec<String> = env_pairs.iter().map(|(k, v)| format!("{k}={v}")).collect();
        let binds = if mount_workspace {
            Some(vec![format!(
                "{}:{}:{}",
                ctx.workspace.to_string_lossy(),
                "/workspace",
                workspace_mode,
            )])
        } else {
            None
        };
        let host_config = HostConfig {
            binds,
            network_mode: Some(network.clone()),
            auto_remove: Some(true),
            ..Default::default()
        };
        let config: ContainerConfig<String> = ContainerConfig {
            image: Some(image.clone()),
            cmd: if command.is_empty() {
                None
            } else {
                Some(command)
            },
            env: if env_strings.is_empty() {
                None
            } else {
                Some(env_strings)
            },
            working_dir: Some(workdir),
            host_config: Some(host_config),
            ..Default::default()
        };
        let create_opts = CreateContainerOptions {
            name: container_name.clone(),
            platform: None,
        };
        let created = docker
            .create_container::<String, String>(Some(create_opts), config)
            .await
            .map_err(|e| NodeError::Other(format!("container: create: {e}")))?;
        let id = created.id;

        docker
            .start_container::<String>(&id, None)
            .await
            .map_err(|e| NodeError::Other(format!("container: start: {e}")))?;

        // Stream logs concurrently with wait_container; on cancel,
        // stop + kill + drain.
        let logs_opts = LogsOptions::<String> {
            follow: true,
            stdout: true,
            stderr: true,
            since: 0,
            until: 0,
            timestamps: false,
            tail: "all".into(),
        };
        let mut logs_stream = docker.logs(&id, Some(logs_opts));
        let mut wait_stream =
            docker.wait_container::<String>(&id, None::<WaitContainerOptions<String>>);
        let mut exit_code: i64 = -1;

        loop {
            tokio::select! {
                log = logs_stream.next() => {
                    match log {
                        Some(Ok(LogOutput::StdOut { message })) => {
                            emit_stream_line(ctx, &node.id, CHANNEL_STDOUT, &message);
                        },
                        Some(Ok(LogOutput::StdErr { message })) => {
                            emit_stream_line(ctx, &node.id, CHANNEL_STDERR, &message);
                        },
                        Some(Ok(_)) | None => {},
                        Some(Err(e)) => {
                            tracing::warn!(error = %e, "container: log stream error");
                        },
                    }
                },
                w = wait_stream.next() => {
                    if let Some(Ok(resp)) = w {
                        exit_code = resp.status_code;
                    }
                    // Drain any remaining log chunks.
                    while let Some(line) = logs_stream.next().await {
                        match line {
                            Ok(LogOutput::StdOut { message }) => {
                                emit_stream_line(ctx, &node.id, CHANNEL_STDOUT, &message);
                            },
                            Ok(LogOutput::StdErr { message }) => {
                                emit_stream_line(ctx, &node.id, CHANNEL_STDERR, &message);
                            },
                            _ => {},
                        }
                    }
                    break;
                },
                () = cancel.cancelled() => {
                    // Grace stop, then kill if still running, then remove.
                    drop(docker
                        .stop_container(
                            &id,
                            Some(StopContainerOptions {
                                t: i64::try_from(grace_secs).unwrap_or(5),
                            }),
                        )
                        .await);
                    drop(docker.kill_container::<String>(&id, None).await);
                    drop(docker
                        .remove_container(
                            &id,
                            Some(RemoveContainerOptions {
                                force: true,
                                ..Default::default()
                            }),
                        )
                        .await);
                    return Err(NodeError::Cancelled);
                }
            }
        }

        let mut out = NodeOutputs::new();
        // Container exit codes fit in i32; i64 → f64 narrowing above
        // 2^53 is academic for this domain.
        #[allow(clippy::cast_precision_loss)]
        let exit_code_f = exit_code as f64;
        out.insert("exit_code".into(), PortValue::Number(exit_code_f));
        // `text` mirrors shell's contract; container streams stdout so
        // there's no buffered text. Empty placeholder keeps the port
        // wired for downstream nodes that expect it.
        out.insert("text".into(), PortValue::String(String::new()));
        Ok(out)
    }
}

async fn pull_image(
    docker: &Docker,
    image: &str,
    ctx: &RunContext,
    node_id: &str,
    cancel: &CancellationToken,
) -> Result<(), NodeError> {
    let opts = CreateImageOptions::<String> {
        from_image: image.to_string(),
        ..Default::default()
    };
    let mut stream = docker.create_image(Some(opts), None, None);
    loop {
        tokio::select! {
            ev = stream.next() => match ev {
                Some(Ok(info)) => {
                    let status = info.status.unwrap_or_default();
                    if !status.is_empty() {
                        let mut payload: HashMap<String, serde_json::Value> = HashMap::with_capacity(2);
                        payload.insert("channel".into(), serde_json::json!(CHANNEL_PULL));
                        payload.insert("text".into(), serde_json::json!(status));
                        ctx.emitter.emit(
                            EventType::NodeOutput,
                            Some(node_id.to_string()),
                            Some(ctx.iteration),
                            Some(ctx.attempt.load(std::sync::atomic::Ordering::SeqCst)),
                            payload,
                        );
                    }
                },
                Some(Err(e)) => {
                    return Err(NodeError::Other(format!(
                        "container: pull '{image}': {e}"
                    )));
                },
                None => break,
            },
            () = cancel.cancelled() => return Err(NodeError::Cancelled),
        }
    }
    Ok(())
}

fn emit_stream_line(ctx: &RunContext, node_id: &str, channel: &str, message: &[u8]) {
    let text = String::from_utf8_lossy(message);
    // Bollard delivers raw bytes per docker frame; split on newlines so
    // the GUI run viewer gets one event per logical log line.
    for line in text.lines() {
        if line.is_empty() {
            continue;
        }
        let mut payload: HashMap<String, serde_json::Value> = HashMap::with_capacity(2);
        payload.insert("channel".into(), serde_json::json!(channel));
        payload.insert("text".into(), serde_json::json!(line));
        ctx.emitter.emit(
            EventType::NodeOutput,
            Some(node_id.to_string()),
            Some(ctx.iteration),
            Some(ctx.attempt.load(std::sync::atomic::Ordering::SeqCst)),
            payload,
        );
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
