//! Subprocess-backed executor.
//!
//! Spawns child processes and supervises them with platform-native
//! process trees via [`super::supervisor`]:
//!
//! - **Unix:** `pre_exec` calls `setsid()` so each child becomes
//!   its own process-group leader. Cancellation sends signals to
//!   the negative PID, which the kernel delivers to every member
//!   of the group.
//! - **Windows:** spawn `CREATE_SUSPENDED` + assign to a Job Object
//!   with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` before resuming the
//!   main thread, so the descendant tree dies atomically on
//!   `TerminateJobObject`.
//!
//! Every argv element, environment value, and stdin template is
//! resolved through the unified [`crate::template::substitute`]
//! engine before spawn. Stdout/stderr are line-buffered and
//! forwarded to the run's [`Emitter`] as `node:output` events.
//! [`parse_outputs`] then maps the accumulated stdout + exit status
//! onto the node's declared output ports.

use crate::emitter::Emitter;
use crate::environment::runtime::transport::{
    ProcessCmd, ProcessExit, ProcessPipe, Stdio as ProcessStdio,
};
use crate::events::EventType;
use crate::executor::{NodeError, NodeExecutor, NodeOutputs, RunContext};
use crate::template::{SubstitutionContext, default_env_allowlist, substitute};
use crate::types::{ExecutionBackend, Node, NodeType, OutputParse, PortValue};
use async_trait::async_trait;
use bytes::Bytes;
use jsonpath_rust::JsonPath;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

// ---- Contract strings shared with the registry + downstream consumers ----

/// `NodeType.id` of the `shell` built-in. The executor special-cases
/// this id to wrap `config.command` in a per-platform shell argv;
/// the registry uses it as the registration key.
pub(crate) const SHELL_NODE_TYPE_ID: &str = "shell";

/// Output-port name carrying trimmed stdout in `Text` mode.
pub(crate) const PORT_TEXT: &str = "text";

/// Output-port name carrying the child's exit status as a `Number`.
pub(crate) const PORT_EXIT_CODE: &str = "exit_code";

/// `node:output` payload key tagging the originating stream.
const KEY_CHANNEL: &str = "channel";

/// `node:output` payload key carrying the line text.
const KEY_TEXT: &str = "text";

/// `KEY_CHANNEL` value for stdout-sourced events.
const CHANNEL_STDOUT: &str = "stdout";

/// `KEY_CHANNEL` value for stderr-sourced events.
const CHANNEL_STDERR: &str = "stderr";

/// Executor for nodes whose `ExecutionSpec::backend` is
/// [`ExecutionBackend::Subprocess`].
pub struct SubprocessExecutor;

#[async_trait]
impl NodeExecutor for SubprocessExecutor {
    fn supports(&self, nt: &NodeType) -> bool {
        nt.execution.backend == ExecutionBackend::Subprocess
    }

    async fn run(
        &self,
        node: &Node,
        nt: &NodeType,
        ctx: &RunContext,
        cancel: CancellationToken,
    ) -> Result<NodeOutputs, NodeError> {
        // Resolve every templated input synchronously, in a separate
        // stack frame, so the SubstitutionContext + its closures
        // fully drop before any await — otherwise their non-Send
        // `&dyn Fn` references would taint the run() future.
        let ResolvedInputs {
            argv,
            env_pairs,
            stdin_body,
        } = resolve_templated_inputs(node, nt, ctx)?;

        let (program, argv_rest) = argv
            .split_first()
            .ok_or_else(|| NodeError::Config("execution.command is empty".into()))?;

        // Select the dispatcher for the node's target_env (default = run's
        // default_env) and translate the host workspace into an env-side
        // path. Failures propagate loudly — no silent fallback to the host
        // namespace.
        let effective_env = node
            .target_env
            .clone()
            .unwrap_or_else(|| ctx.run_snapshot.default_env.clone());
        let dispatcher = ctx
            .run_snapshot
            .dispatcher(&effective_env)
            .ok_or_else(|| {
                NodeError::Config(format!(
                    "shell: env '{}' not in run snapshot scope",
                    effective_env.as_str()
                ))
            })?
            .clone();
        let cwd = dispatcher.translate_path(&ctx.workspace).map_err(|e| {
            NodeError::Config(format!(
                "shell: env '{}' cannot translate workspace path '{}': {e}",
                effective_env.as_str(),
                ctx.workspace.display(),
            ))
        })?;

        let process_cmd = ProcessCmd {
            program: program.clone(),
            args: argv_rest.to_vec(),
            env: env_pairs.into_iter().collect(),
            cwd: Some(cwd),
            stdin: stdin_body.map(|s| Bytes::from(s.into_bytes())),
            stdout: ProcessStdio::Piped,
            stderr: ProcessStdio::Piped,
        };

        let mut proc = dispatcher
            .spawn(process_cmd)
            .await
            .map_err(|e| NodeError::Subprocess(format!("spawn: {e}")))?;

        let emitter = ctx.emitter.clone();
        // Snapshot iteration + attempt at spawn time so every line
        // event tags the same coordinates the run loop will record
        // for this attempt's node_runs row. attempt is read from the
        // shared atomic the retry loop updates per attempt.
        let iteration = ctx.iteration;
        let attempt = ctx.attempt.load(std::sync::atomic::Ordering::Relaxed);
        let stdout_handle = spawn_line_reader(
            proc.take_stdout(),
            emitter.clone(),
            node.id.clone(),
            iteration,
            attempt,
            CHANNEL_STDOUT,
        );
        let stderr_handle = spawn_line_reader(
            proc.take_stderr(),
            emitter,
            node.id.clone(),
            iteration,
            attempt,
            CHANNEL_STDERR,
        );

        let outcome = tokio::select! {
            wait_res = proc.wait() => Outcome::Exit(wait_res),
            () = cancel.cancelled() => {
                drop(proc.cancel().await);
                Outcome::Cancelled
            }
        };

        let stdout_lines = stdout_handle.await.unwrap_or_default();
        let _stderr_lines = stderr_handle.await.unwrap_or_default();

        match outcome {
            Outcome::Cancelled => Err(NodeError::Cancelled),
            Outcome::Exit(Err(e)) => Err(NodeError::Subprocess(format!("wait: {e}"))),
            Outcome::Exit(Ok(exit)) => parse_outputs(nt, &stdout_lines, &exit),
        }
    }
}

// =====================================================================
// Shared types
// =====================================================================

/// Wrap a user-authored shell script into the per-platform argv
/// that runs it: `bash -c <script>` on Unix, `cmd /C <script>` on
/// Windows. The script reaches the shell verbatim so compound
/// forms (`for` / `if` / pipes) parse correctly.
#[cfg(unix)]
fn shell_argv_for_platform(script: String) -> Vec<String> {
    vec!["bash".into(), "-c".into(), script]
}

#[cfg(windows)]
fn shell_argv_for_platform(script: String) -> Vec<String> {
    vec!["cmd".into(), "/C".into(), script]
}

#[cfg(not(any(unix, windows)))]
fn shell_argv_for_platform(_script: String) -> Vec<String> {
    Vec::new()
}

/// Owned substitution outputs handed from the sync prep phase
/// into the async spawn phase.
struct ResolvedInputs {
    argv: Vec<String>,
    env_pairs: Vec<(String, String)>,
    stdin_body: Option<String>,
}

/// Resolve every templated input on the node's `ExecutionSpec` in
/// a single sync pass. Returns owned strings so the caller's
/// future doesn't have to hold the `SubstitutionContext` (which
/// borrows non-Send `&dyn Fn` closures) across any await.
fn resolve_templated_inputs(
    node: &Node,
    nt: &NodeType,
    ctx: &RunContext,
) -> Result<ResolvedInputs, NodeError> {
    // Wraps secrets-store reads with emitter.register_secret so
    // resolved values are redacted out of any later node:output
    // events — otherwise `{{secrets.X}}` interpolation would
    // silently leak through the child's stdout.
    let secrets_resolver = crate::executor::context::make_secrets_resolver(ctx);
    // KV-store lookups are not yet wired; the resolver always
    // returns None so `{{kv.X}}` fails loud via TemplateError::Undefined.
    let kv_resolver = |_: &str| -> Option<String> { None };
    let env_allowlist = default_env_allowlist();
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

    let subctx = SubstitutionContext {
        vars: &ctx.variables,
        secrets: &secrets_resolver,
        upstream_outputs: &ctx.upstream_outputs,
        current_inputs: &ctx.current_inputs,
        current_config: &node.config,
        kv: &kv_resolver,
        env: &*ctx.env,
        env_allowlist: &env_allowlist,
        resources: &resources_resolver,
        run_id: &ctx.run_id,
        workspace: &ctx.workspace,
        started_at_iso: &ctx.started_at_iso,
        workflow_id: &ctx.workflow_id,
        workflow_name: &ctx.workflow_name,
    };

    let argv: Vec<String> = if nt.id == SHELL_NODE_TYPE_ID {
        // shell built-in: config.command is the user's free-form
        // shell script, substituted once and then handed to the
        // platform shell as -c / /C.
        //
        // We pass cmd_str as the script (not via positional $1)
        // so compound shell forms (for/while/if/pipes) parse the
        // text as code, with parameter expansion happening after.
        let raw = node
            .config
            .get("command")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| NodeError::Config("shell: config.command (string) required".into()))?;
        let cmd_str = substitute(raw, &subctx).map_err(|e| NodeError::Template(e.to_string()))?;
        shell_argv_for_platform(cmd_str)
    } else {
        nt.execution
            .command
            .iter()
            .map(|s| substitute(s, &subctx).map_err(|e| NodeError::Template(e.to_string())))
            .collect::<Result<_, _>>()?
    };

    let env_pairs: Vec<(String, String)> = nt
        .execution
        .env
        .iter()
        .map(|(k, v)| {
            substitute(v, &subctx)
                .map(|sub| (k.clone(), sub))
                .map_err(|e| NodeError::Template(e.to_string()))
        })
        .collect::<Result<_, _>>()?;

    let stdin_body: Option<String> = nt
        .execution
        .stdin_template
        .as_deref()
        .map(|t| substitute(t, &subctx).map_err(|e| NodeError::Template(e.to_string())))
        .transpose()?;

    Ok(ResolvedInputs {
        argv,
        env_pairs,
        stdin_body,
    })
}

/// Outcome of the `tokio::select!` between `proc.wait()` and `cancel`.
enum Outcome {
    Exit(Result<ProcessExit, crate::environment::runtime::DispatchError>),
    Cancelled,
}

/// Spawn a tokio task that reads `pipe` line-by-line, emits each
/// line as a `node:output` event tagged with `channel`, and returns
/// the lines after EOF. EOF arrives when the child closes its end
/// of the pipe — which happens when the child exits.
fn spawn_line_reader(
    pipe: Option<ProcessPipe>,
    emitter: Arc<Emitter>,
    node_id: String,
    iteration: u32,
    attempt: u32,
    channel: &'static str,
) -> JoinHandle<Vec<String>> {
    tokio::spawn(async move {
        let Some(p) = pipe else {
            return Vec::new();
        };
        let mut acc = Vec::new();
        let mut reader = BufReader::new(p).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            // Pre-size for the exact (channel, text) pair we insert
            // below — avoids the default 0 → 4 grow on a per-line
            // hot path.
            let mut payload: HashMap<String, serde_json::Value> = HashMap::with_capacity(2);
            payload.insert(
                KEY_CHANNEL.into(),
                serde_json::Value::String(channel.into()),
            );
            payload.insert(KEY_TEXT.into(), serde_json::Value::String(line.clone()));
            emitter.emit_node(
                EventType::NodeOutput,
                node_id.clone(),
                iteration,
                attempt,
                payload,
            );
            acc.push(line);
        }
        acc
    })
}

/// Turn the child's accumulated stdout + exit status into output
/// ports per `execution.output_parse`:
///
/// - `Text`: trimmed stdout → `text` (`PortValue::String`).
/// - `Json`: parse stdout as JSON; each `(port_name, jsonpath)`
///   entry in `output_map` evaluates the `JSONPath`, takes the
///   first match, and stores it on `port_name` as
///   `PortValue::Json`. Parse failures or unmatched paths fail
///   the node loudly.
///
/// `exit_code` is always populated.
fn parse_outputs(
    nt: &NodeType,
    stdout_lines: &[String],
    exit: &ProcessExit,
) -> Result<NodeOutputs, NodeError> {
    let mut outputs = NodeOutputs::new();

    outputs.insert(
        PORT_EXIT_CODE.into(),
        PortValue::Number(f64::from(exit.code)),
    );

    let joined = stdout_lines.join("\n");

    match nt.execution.output_parse {
        OutputParse::Text => {
            outputs.insert(
                PORT_TEXT.into(),
                PortValue::String(joined.trim_end().to_string()),
            );
        },
        OutputParse::Json => {
            let parsed: serde_json::Value = serde_json::from_str(joined.trim())
                .map_err(|e| NodeError::Other(format!("json: parse stdout: {e}")))?;
            for (port_name, expr) in &nt.execution.output_map {
                let matched = parsed.query(expr).map_err(|e| {
                    NodeError::Other(format!(
                        "json: output_map[{port_name}]: invalid JSONPath '{expr}': {e}"
                    ))
                })?;
                let first = matched.into_iter().next().ok_or_else(|| {
                    NodeError::Other(format!(
                        "json: output_map[{port_name}]: no match for '{expr}'"
                    ))
                })?;
                outputs.insert(port_name.clone(), PortValue::Json(first.clone()));
            }
        },
    }

    Ok(outputs)
}

// =====================================================================
// Tests
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::RunEvent;
    use crate::executor::test_support::{
        make_ctx as test_ctx, subprocess_node_type, trivial_subprocess_node as trivial_node,
    };
    #[cfg(unix)]
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::sync::broadcast;

    fn collect_output(rx: &mut broadcast::Receiver<RunEvent>) -> Vec<(String, String)> {
        let mut out = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            if ev.ty == EventType::NodeOutput {
                let channel = ev
                    .payload
                    .get("channel")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let text = ev
                    .payload
                    .get("text")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                out.push((channel, text));
            }
        }
        out
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn unix_run_completes_for_quick_command() {
        let (ctx, _rx, _dir) = test_ctx();
        let nt = subprocess_node_type(vec!["true".into()]);
        let node = trivial_node();
        let res = SubprocessExecutor
            .run(&node, &nt, &ctx, CancellationToken::new())
            .await
            .expect("true should exit 0");
        // text mode now populates `text` (empty here since `true` prints nothing) + `exit_code`.
        assert_eq!(res.get("text"), Some(&PortValue::String(String::new())));
        assert_eq!(res.get("exit_code"), Some(&PortValue::Number(0.0)));
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn unix_cancel_kills_process_group() {
        let (ctx, _rx, _dir) = test_ctx();
        let nt = subprocess_node_type(vec![
            "bash".into(),
            "-c".into(),
            "sleep 30; echo done".into(),
        ]);
        let node = trivial_node();

        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();
        let ctx_arc = Arc::new(ctx);
        let ctx_for_task = ctx_arc.clone();
        let nt_for_task = nt.clone();
        let node_for_task = node.clone();

        let handle = tokio::spawn(async move {
            SubprocessExecutor
                .run(&node_for_task, &nt_for_task, &ctx_for_task, cancel_for_task)
                .await
        });

        tokio::time::sleep(Duration::from_millis(150)).await;
        cancel.cancel();

        let res = tokio::time::timeout(Duration::from_secs(3), handle)
            .await
            .expect("cancel must surface within 3s")
            .expect("spawned task must not panic");

        assert!(matches!(res, Err(NodeError::Cancelled)), "got {res:?}");
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn unix_stdout_emits_one_event_per_line_in_order() {
        let (ctx, mut rx, _dir) = test_ctx();
        let nt = subprocess_node_type(vec![
            "bash".into(),
            "-c".into(),
            "for i in 1 2 3; do echo \"line $i\"; done".into(),
        ]);
        let node = trivial_node();

        SubprocessExecutor
            .run(&node, &nt, &ctx, CancellationToken::new())
            .await
            .expect("loop should exit 0");

        let out = collect_output(&mut rx);
        let stdout_texts: Vec<&str> = out
            .iter()
            .filter(|(c, _)| c == "stdout")
            .map(|(_, t)| t.as_str())
            .collect();
        assert_eq!(stdout_texts, vec!["line 1", "line 2", "line 3"]);
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn unix_stderr_events_carry_channel_stderr() {
        let (ctx, mut rx, _dir) = test_ctx();
        let nt = subprocess_node_type(vec![
            "bash".into(),
            "-c".into(),
            "echo to-stderr 1>&2".into(),
        ]);
        let node = trivial_node();

        SubprocessExecutor
            .run(&node, &nt, &ctx, CancellationToken::new())
            .await
            .expect("should exit 0");

        let out = collect_output(&mut rx);
        let saw = out.iter().any(|(c, t)| c == "stderr" && t == "to-stderr");
        assert!(
            saw,
            "expected stderr event with text 'to-stderr', got {out:?}"
        );
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn argv_positional_passes_substituted_values_as_whole_tokens() {
        let (ctx, mut rx, _dir) = test_ctx();
        let mut nt = subprocess_node_type(vec![
            "sh".into(),
            "-c".into(),
            "echo \"arg1=$1 arg2=$2\"".into(),
            "--".into(),
            "{{config.first}}".into(),
            "{{config.second}}".into(),
        ]);
        // Verify env substitution along the same path.
        nt.execution
            .env
            .insert("SOMEVAR".into(), "v={{config.first}}".into());

        let mut node = trivial_node();
        node.config
            .insert("first".into(), serde_json::Value::String("hi there".into()));
        // Second arg contains {{evil}} — must NOT be re-parsed by
        // substitute() once it's been put into argv.
        node.config.insert(
            "second".into(),
            serde_json::Value::String("{{evil}}".into()),
        );

        SubprocessExecutor
            .run(&node, &nt, &ctx, CancellationToken::new())
            .await
            .expect("argv positional should run");

        let out = collect_output(&mut rx);
        let stdout: Vec<&str> = out
            .iter()
            .filter(|(c, _)| c == "stdout")
            .map(|(_, t)| t.as_str())
            .collect();
        assert_eq!(stdout, vec!["arg1=hi there arg2={{evil}}"], "got {out:?}");
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn stdin_template_feeds_child_then_closes() {
        let (ctx, mut rx, _dir) = test_ctx();
        let mut nt = subprocess_node_type(vec![
            "sh".into(),
            "-c".into(),
            "cat".into(), // echo stdin to stdout
        ]);
        nt.execution.stdin_template = Some("hello {{config.name}}\nsecond line".into());

        let mut node = trivial_node();
        node.config
            .insert("name".into(), serde_json::Value::String("world".into()));

        SubprocessExecutor
            .run(&node, &nt, &ctx, CancellationToken::new())
            .await
            .expect("cat should exit 0 once stdin closes");

        let out = collect_output(&mut rx);
        let stdout: Vec<&str> = out
            .iter()
            .filter(|(c, _)| c == "stdout")
            .map(|(_, t)| t.as_str())
            .collect();
        assert_eq!(stdout, vec!["hello world", "second line"], "got {out:?}");
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn text_mode_sets_text_and_exit_code_ports() {
        let (ctx, _rx, _dir) = test_ctx();
        let nt = subprocess_node_type(vec![
            "sh".into(),
            "-c".into(),
            "echo first; echo second".into(),
        ]);
        let node = trivial_node();

        let res = SubprocessExecutor
            .run(&node, &nt, &ctx, CancellationToken::new())
            .await
            .expect("should exit 0");

        assert_eq!(
            res.get("text"),
            Some(&PortValue::String("first\nsecond".into()))
        );
        assert_eq!(res.get("exit_code"), Some(&PortValue::Number(0.0)));
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn json_mode_runs_output_map_jsonpath() {
        let (ctx, _rx, _dir) = test_ctx();
        let mut nt = subprocess_node_type(vec![
            "sh".into(),
            "-c".into(),
            r#"echo '{"x":42,"y":"hi"}'"#.into(),
        ]);
        nt.execution.output_parse = OutputParse::Json;
        nt.execution
            .output_map
            .insert("result".into(), "$.x".into());
        nt.execution
            .output_map
            .insert("greeting".into(), "$.y".into());
        let node = trivial_node();

        let res = SubprocessExecutor
            .run(&node, &nt, &ctx, CancellationToken::new())
            .await
            .expect("should exit 0");

        assert_eq!(
            res.get("result"),
            Some(&PortValue::Json(serde_json::json!(42)))
        );
        assert_eq!(
            res.get("greeting"),
            Some(&PortValue::Json(serde_json::json!("hi")))
        );
        assert_eq!(res.get("exit_code"), Some(&PortValue::Number(0.0)));
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn json_mode_malformed_stdout_fails_node() {
        let (ctx, _rx, _dir) = test_ctx();
        let mut nt = subprocess_node_type(vec!["sh".into(), "-c".into(), "echo not json".into()]);
        nt.execution.output_parse = OutputParse::Json;
        let node = trivial_node();

        let err = SubprocessExecutor
            .run(&node, &nt, &ctx, CancellationToken::new())
            .await
            .expect_err("malformed JSON in json mode must fail");

        match err {
            NodeError::Other(msg) => assert!(msg.contains("json"), "msg = {msg}"),
            other => panic!("expected NodeError::Other, got {other:?}"),
        }
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial]
    async fn secret_interpolated_into_argv_is_redacted_in_stdout_events() {
        use crate::secrets::Store;

        keyring::use_sample_store(&HashMap::from([("persist", "false")])).unwrap();
        let store_dir = TempDir::new().unwrap();
        let store = Arc::new(Store::with_index_path(
            store_dir.path().join("secrets-index.json"),
        ));
        store.set("UNIQUE_TOK", "redaction-sentinel-9X7Y3").unwrap();

        let (mut ctx, mut rx, _dir) = test_ctx();
        ctx.secrets_store = Some(store);

        let nt = subprocess_node_type(vec![
            "sh".into(),
            "-c".into(),
            "echo leaked={{secrets.UNIQUE_TOK}}".into(),
        ]);
        let node = trivial_node();

        SubprocessExecutor
            .run(&node, &nt, &ctx, CancellationToken::new())
            .await
            .expect("should exit 0");

        let outs = collect_output(&mut rx);
        let stdout: Vec<&str> = outs
            .iter()
            .filter(|(c, _)| c == "stdout")
            .map(|(_, t)| t.as_str())
            .collect();
        assert_eq!(stdout.len(), 1, "exactly one stdout line, got {outs:?}");
        let line = stdout[0];
        assert!(
            !line.contains("redaction-sentinel-9X7Y3"),
            "raw secret leaked: {line:?}"
        );
        assert!(
            line.contains("<redacted:UNIQUE_TOK>"),
            "expected redaction marker, got {line:?}"
        );
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn shell_builtin_runs_bash_with_substituted_command() {
        use crate::registry::Registry;

        let (ctx, _rx, _dir) = test_ctx();
        let reg = Registry::with_v1_0_builtins();
        let nt = reg.get("shell").expect("shell built-in registered");

        let mut node = trivial_node();
        node.ty = "shell".into();
        node.config.insert(
            "command".into(),
            serde_json::Value::String("echo hello {{vars.who}}".into()),
        );

        let mut ctx_owned = ctx;
        ctx_owned.variables.insert("who".into(), "ordius".into());

        let res = SubprocessExecutor
            .run(&node, &nt, &ctx_owned, CancellationToken::new())
            .await
            .expect("shell should exit 0");

        assert_eq!(
            res.get("text"),
            Some(&PortValue::String("hello ordius".into()))
        );
        assert_eq!(res.get("exit_code"), Some(&PortValue::Number(0.0)));
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn shell_builtin_supports_compound_forms() {
        use crate::registry::Registry;

        let (ctx, _rx, _dir) = test_ctx();
        let reg = Registry::with_v1_0_builtins();
        let nt = reg.get("shell").expect("shell built-in registered");

        let mut node = trivial_node();
        node.ty = "shell".into();
        node.config.insert(
            "command".into(),
            serde_json::Value::String("for i in 1 2 3; do echo \"row $i\"; done | wc -l".into()),
        );

        let res = SubprocessExecutor
            .run(&node, &nt, &ctx, CancellationToken::new())
            .await
            .expect("compound shell should run");

        // `wc -l` prints a count; strip surrounding whitespace then compare.
        let text = match res.get("text") {
            Some(PortValue::String(s)) => s.trim().to_string(),
            other => panic!("expected text port, got {other:?}"),
        };
        assert_eq!(text, "3");
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn shell_builtin_missing_command_errors() {
        use crate::registry::Registry;

        let (ctx, _rx, _dir) = test_ctx();
        let reg = Registry::with_v1_0_builtins();
        let nt = reg.get("shell").expect("shell built-in registered");

        let mut node = trivial_node();
        node.ty = "shell".into();
        // config.command intentionally absent.

        let err = SubprocessExecutor
            .run(&node, &nt, &ctx, CancellationToken::new())
            .await
            .expect_err("missing command must fail");

        match err {
            NodeError::Config(msg) => {
                assert!(msg.contains("command"), "msg = {msg}");
            },
            other => panic!("expected NodeError::Config, got {other:?}"),
        }
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn exit_code_propagates_nonzero() {
        let (ctx, _rx, _dir) = test_ctx();
        let nt = subprocess_node_type(vec!["sh".into(), "-c".into(), "exit 7".into()]);
        let node = trivial_node();

        let res = SubprocessExecutor
            .run(&node, &nt, &ctx, CancellationToken::new())
            .await
            .expect("non-zero exit is a successful run from the executor's POV");

        assert_eq!(res.get("exit_code"), Some(&PortValue::Number(7.0)));
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn parse_outputs_uses_process_exit_code() {
        let nt = subprocess_node_type(vec!["sh".into(), "-c".into(), "exit 9".into()]);
        let outputs = parse_outputs(
            &nt,
            &[],
            &crate::environment::runtime::transport::ProcessExit {
                code: 9,
                signal: None,
            },
        )
        .expect("parse outputs");

        assert_eq!(outputs.get("exit_code"), Some(&PortValue::Number(9.0)));
    }

    #[cfg(windows)]
    #[tokio::test(flavor = "multi_thread")]
    async fn windows_resume_runs_suspended_child() {
        let (ctx, _rx, _dir) = test_ctx();
        let nt = subprocess_node_type(vec!["cmd".into(), "/C".into(), "echo hi".into()]);
        let node = trivial_node();
        let res = SubprocessExecutor
            .run(&node, &nt, &ctx, CancellationToken::new())
            .await;
        assert!(res.is_ok(), "child should resume and exit 0: {res:?}");
    }
}
