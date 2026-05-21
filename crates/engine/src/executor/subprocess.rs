//! Subprocess-backed executor.
//!
//! Spawns child processes and supervises them with platform-native
//! process trees:
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
//!
//! The crate-level `unsafe_code = "deny"` lint is overridden here —
//! `pre_exec` on Unix and the Win32 Job-Object / Toolhelp32 calls
//! on Windows both need raw FFI.
#![allow(unsafe_code)]

use crate::emitter::Emitter;
use crate::events::EventType;
#[cfg(unix)]
use crate::executor::supervisor::CANCEL_GRACE;
use crate::executor::{NodeError, NodeExecutor, NodeOutputs, RunContext};
use crate::template::{SubstitutionContext, default_env_allowlist, substitute};
use crate::types::{ExecutionBackend, Node, NodeType, OutputParse, PortValue};
use async_trait::async_trait;
use jsonpath_rust::JsonPath;
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader};
use tokio::process::Command;
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

        let mut cmd = Command::new(program);
        cmd.args(argv_rest);
        cmd.current_dir(&ctx.workspace);
        for (k, v) in &env_pairs {
            cmd.env(k, v);
        }
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        if stdin_body.is_some() {
            cmd.stdin(Stdio::piped());
        }
        cmd.kill_on_drop(true);

        #[cfg(unix)]
        configure_unix_command(&mut cmd);
        #[cfg(windows)]
        configure_windows_command(&mut cmd);

        let mut sup = platform_spawn(cmd)?;

        if let (Some(body), Some(mut stdin)) = (stdin_body, sup.child.stdin.take()) {
            // Writer runs on its own task so a child that reads stdin
            // fully before producing stdout can't deadlock us (we
            // hold the stdout read loop below).
            tokio::spawn(async move {
                let write_res = stdin.write_all(body.as_bytes()).await;
                drop(write_res);
                let shutdown_res = stdin.shutdown().await;
                drop(shutdown_res);
            });
        }

        let emitter = ctx.emitter.clone();
        // Snapshot iteration + attempt at spawn time so every line
        // event tags the same coordinates the run loop will record
        // for this attempt's node_runs row. attempt is read from the
        // shared atomic the retry loop updates per attempt.
        let iteration = ctx.iteration;
        let attempt = ctx.attempt.load(std::sync::atomic::Ordering::Relaxed);
        let stdout_handle = spawn_line_reader(
            sup.child.stdout.take(),
            emitter.clone(),
            node.id.clone(),
            iteration,
            attempt,
            CHANNEL_STDOUT,
        );
        let stderr_handle = spawn_line_reader(
            sup.child.stderr.take(),
            emitter,
            node.id.clone(),
            iteration,
            attempt,
            CHANNEL_STDERR,
        );

        let outcome = tokio::select! {
            wait_res = sup.child.wait() => Outcome::Exit(wait_res),
            () = cancel.cancelled() => {
                platform_cancel(&mut sup).await;
                Outcome::Cancelled
            }
        };

        let stdout_lines = stdout_handle.await.unwrap_or_default();
        let _stderr_lines = stderr_handle.await.unwrap_or_default();

        match outcome {
            Outcome::Cancelled => Err(NodeError::Cancelled),
            Outcome::Exit(Err(e)) => Err(NodeError::Subprocess(format!("wait: {e}"))),
            Outcome::Exit(Ok(status)) => parse_outputs(nt, &stdout_lines, status),
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

    let subctx = SubstitutionContext {
        vars: &ctx.variables,
        secrets: &secrets_resolver,
        upstream_outputs: &ctx.upstream_outputs,
        current_inputs: &ctx.current_inputs,
        current_config: &node.config,
        kv: &kv_resolver,
        env: &*ctx.env,
        env_allowlist: &env_allowlist,
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

/// Outcome of the `tokio::select!` between `child.wait()` and `cancel`.
enum Outcome {
    Exit(std::io::Result<std::process::ExitStatus>),
    Cancelled,
}

/// Spawn a tokio task that reads `pipe` line-by-line, emits each
/// line as a `node:output` event tagged with `channel`, and returns
/// the lines after EOF. EOF arrives when the child closes its end
/// of the pipe — which happens when the child exits.
fn spawn_line_reader<R>(
    pipe: Option<R>,
    emitter: Arc<Emitter>,
    node_id: String,
    iteration: u32,
    attempt: u32,
    channel: &'static str,
) -> JoinHandle<Vec<String>>
where
    R: AsyncRead + Send + Unpin + 'static,
{
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
    status: std::process::ExitStatus,
) -> Result<NodeOutputs, NodeError> {
    let mut outputs = NodeOutputs::new();

    let code = status.code().unwrap_or(-1);
    outputs.insert(PORT_EXIT_CODE.into(), PortValue::Number(f64::from(code)));

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
// Unix
// =====================================================================

/// Per-platform supervised child. On Unix this is just the child
/// and its PID; on Windows we additionally hold the Job Object
/// handle that owns the whole tree.
#[cfg(unix)]
struct Supervised {
    child: tokio::process::Child,
    pid: u32,
}

#[cfg(unix)]
fn platform_spawn(mut cmd: Command) -> Result<Supervised, NodeError> {
    let child = cmd
        .spawn()
        .map_err(|e| NodeError::Subprocess(format!("spawn: {e}")))?;
    let pid = child
        .id()
        .ok_or_else(|| NodeError::Subprocess("child has no pid (already reaped)".into()))?;
    Ok(Supervised { child, pid })
}

#[cfg(unix)]
async fn platform_cancel(sup: &mut Supervised) {
    use nix::sys::signal::Signal;

    let _soft = kill_unix_pgroup(sup.pid, Signal::SIGTERM);
    let grace = tokio::time::sleep(CANCEL_GRACE);
    tokio::select! {
        wait_res = sup.child.wait() => { drop(wait_res); }
        () = grace => {
            let _hard = kill_unix_pgroup(sup.pid, Signal::SIGKILL);
            let reap = sup.child.wait().await;
            drop(reap);
        }
    }
}

/// Configure a Unix `Command` so the child becomes the leader of
/// its own process group via `setsid()` in the post-fork /
/// pre-spawn window.
///
/// Negative PIDs passed to `kill(2)` then signal the whole group,
/// which is how subprocess cancellation reaches grandchildren the
/// child spawned itself.
#[cfg(unix)]
pub(crate) fn configure_unix_command(cmd: &mut Command) {
    // SAFETY: `setsid()` is async-signal-safe and the only call we
    // make between `fork` and the child's `execve`. No allocation,
    // no locks, no non-reentrant libc functions.
    unsafe {
        cmd.pre_exec(|| {
            nix::unistd::setsid()
                .map(|_| ())
                .map_err(|e| std::io::Error::from_raw_os_error(e as i32))
        });
    }
}

/// Send `signal` to the process group led by `pid`. A negative PID
/// passed to `kill(2)` targets every member of the group, so a
/// SIGTERM here also reaches any grandchildren the original child
/// spawned (provided they did not call `setsid` themselves to
/// detach into a new group).
#[cfg(unix)]
fn kill_unix_pgroup(pid: u32, signal: nix::sys::signal::Signal) -> std::io::Result<()> {
    use nix::sys::signal::kill;
    use nix::unistd::Pid;

    let pid_i32 = i32::try_from(pid)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "pid overflows i32"))?;
    kill(Pid::from_raw(-pid_i32), signal).map_err(|e| std::io::Error::from_raw_os_error(e as i32))
}

// =====================================================================
// Windows
// =====================================================================

#[cfg(windows)]
struct Supervised {
    child: tokio::process::Child,
    job: windows::Win32::Foundation::HANDLE,
}

// SAFETY: Windows kernel HANDLEs are safe to use from any thread —
// concurrency is enforced by the kernel — and we hold this one
// uniquely (no aliasing). HANDLE only fails Send/Sync automatically
// because its inner field is a raw pointer.
#[cfg(windows)]
unsafe impl Send for Supervised {}
#[cfg(windows)]
unsafe impl Sync for Supervised {}

#[cfg(windows)]
impl Drop for Supervised {
    fn drop(&mut self) {
        // SAFETY: `job` is a live kernel handle we own.
        let close = unsafe { windows::Win32::Foundation::CloseHandle(self.job) };
        drop(close);
    }
}

#[cfg(windows)]
fn platform_spawn(mut cmd: Command) -> Result<Supervised, NodeError> {
    let sup = win_job::spawn_supervised(&mut cmd)?;
    let pid = sup
        .child
        .id()
        .ok_or_else(|| NodeError::Subprocess("child has no pid (already reaped)".into()))?;
    win_job::resume_initial_thread(pid)?;
    Ok(sup)
}

#[cfg(windows)]
async fn platform_cancel(sup: &mut Supervised) {
    // TerminateJobObject hard-kills every process in the job
    // atomically — analog of kill(-pgid, SIGKILL).
    win_job::terminate_job(sup.job);
    let reap = sup.child.wait().await;
    drop(reap);
}

/// Windows command-configuration hook. Job-Object setup happens
/// post-spawn (see [`win_job::spawn_supervised`]); the pre-spawn
/// hook itself is a no-op placeholder retained for symmetry with
/// the Unix path.
#[cfg(windows)]
pub(crate) fn configure_windows_command(_cmd: &mut Command) {}

#[cfg(windows)]
mod win_job {
    //! Windows process supervision via Job Objects.
    //!
    //! Spawning a child with `CREATE_SUSPENDED | CREATE_NEW_PROCESS_GROUP`
    //! and assigning it to a fresh Job Object with the
    //! `KILL_ON_JOB_CLOSE` limit before its main thread runs means
    //! every descendant the program later spawns inherits the job
    //! membership automatically. Closing or terminating the job then
    //! terminates the whole tree atomically.

    use super::{Command, NodeError, Supervised};
    use windows::Win32::Foundation::{CloseHandle, FALSE, HANDLE};
    use windows::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
    };
    use windows::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
        SetInformationJobObject, TerminateJobObject,
    };
    use windows::Win32::System::Threading::{
        CREATE_NEW_PROCESS_GROUP, CREATE_SUSPENDED, OpenThread, ResumeThread, THREAD_SUSPEND_RESUME,
    };

    pub(super) fn spawn_supervised(cmd: &mut Command) -> Result<Supervised, NodeError> {
        cmd.creation_flags(CREATE_SUSPENDED.0 | CREATE_NEW_PROCESS_GROUP.0);
        let child = cmd
            .spawn()
            .map_err(|e| NodeError::Subprocess(format!("spawn: {e}")))?;

        // SAFETY: both args are `None` (no SECURITY_ATTRIBUTES, no
        // name), so there are no pointer preconditions to satisfy.
        // The returned HANDLE is owned by us and closed on every
        // error path below + on `Supervised::drop` on success.
        let job = unsafe { CreateJobObjectW(None, None) }
            .map_err(|e| NodeError::Subprocess(format!("CreateJobObjectW: {e}")))?;

        let mut info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        let info_size = u32::try_from(std::mem::size_of_val(&info))
            .expect("JOBOBJECT_EXTENDED_LIMIT_INFORMATION fits in u32");
        // SAFETY: `info` is a fully-initialized stack struct; the
        // pointer is valid for `info_size` bytes; the job handle was
        // just created.
        let set_res = unsafe {
            SetInformationJobObject(
                job,
                JobObjectExtendedLimitInformation,
                std::ptr::addr_of!(info).cast(),
                info_size,
            )
        };
        if let Err(e) = set_res {
            // SAFETY: closing the job we just created.
            let close = unsafe { CloseHandle(job) };
            drop(close);
            return Err(NodeError::Subprocess(format!(
                "SetInformationJobObject: {e}"
            )));
        }

        let raw = match child.raw_handle() {
            Some(h) => h,
            None => {
                // SAFETY: closing the job we just created.
                let close = unsafe { CloseHandle(job) };
                drop(close);
                return Err(NodeError::Subprocess("child has no raw handle".into()));
            },
        };
        let child_handle = HANDLE(raw.cast());

        // SAFETY: `child_handle` borrows from the `Child` we just
        // spawned and are about to return — the OS handle stays
        // valid for the call.
        let assign_res = unsafe { AssignProcessToJobObject(job, child_handle) };
        if let Err(e) = assign_res {
            // SAFETY: closing the job we just created.
            let close = unsafe { CloseHandle(job) };
            drop(close);
            return Err(NodeError::Subprocess(format!(
                "AssignProcessToJobObject: {e}"
            )));
        }

        Ok(Supervised { child, job })
    }

    pub(super) fn resume_initial_thread(pid: u32) -> Result<(), NodeError> {
        // SAFETY: snapshot is an owned kernel handle; closed below.
        let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) }
            .map_err(|e| NodeError::Subprocess(format!("CreateToolhelp32Snapshot: {e}")))?;

        let mut entry = THREADENTRY32 {
            dwSize: u32::try_from(std::mem::size_of::<THREADENTRY32>())
                .expect("THREADENTRY32 fits in u32"),
            ..Default::default()
        };

        // SAFETY: `entry` has `dwSize` set; `snapshot` is live.
        let mut step = unsafe { Thread32First(snapshot, &mut entry) };
        let mut found_tid: Option<u32> = None;
        while step.is_ok() {
            if entry.th32OwnerProcessID == pid {
                found_tid = Some(entry.th32ThreadID);
                break;
            }
            // SAFETY: same invariants as the First call.
            step = unsafe { Thread32Next(snapshot, &mut entry) };
        }

        // SAFETY: closing the snapshot we created.
        let close_snap = unsafe { CloseHandle(snapshot) };
        drop(close_snap);

        let tid = found_tid
            .ok_or_else(|| NodeError::Subprocess(format!("no thread found for pid {pid}")))?;

        // SAFETY: OpenThread returns an owned handle; closed below.
        let thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, FALSE, tid) }
            .map_err(|e| NodeError::Subprocess(format!("OpenThread tid={tid}: {e}")))?;

        // ResumeThread returns the previous suspend count, or
        // `u32::MAX` (cast of -1) on failure.
        // SAFETY: `thread` is a live kernel handle we just opened.
        let prev = unsafe { ResumeThread(thread) };

        // SAFETY: closing the thread handle we opened.
        let close_thread = unsafe { CloseHandle(thread) };
        drop(close_thread);

        if prev == u32::MAX {
            return Err(NodeError::Subprocess("ResumeThread failed".into()));
        }
        Ok(())
    }

    pub(super) fn terminate_job(job: HANDLE) {
        // SAFETY: `job` is a live kernel handle the caller owns.
        let res = unsafe { TerminateJobObject(job, 1) };
        drop(res);
    }
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
