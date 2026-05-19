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
//! forwarded to the run's [`Emitter`] as `node:output` events; the
//! full stdout is also accumulated for the eventual `output_parse`
//! step.
//!
//! The OS-level guarantees replace the `taskkill /T` workarounds
//! the engine used in earlier prototypes.
#![allow(unsafe_code)]

use crate::emitter::Emitter;
use crate::events::EventType;
use crate::executor::{NodeError, NodeExecutor, NodeOutputs, RunContext};
use crate::template::{SubstitutionContext, default_env_allowlist, substitute};
use crate::types::{ExecutionBackend, Node, NodeType};
use async_trait::async_trait;
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
#[cfg(unix)]
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Grace period between SIGTERM and SIGKILL on Unix. Long enough
/// for shells and Python interpreters to flush stdout, short
/// enough that a hung child doesn't block run finalization.
///
/// Windows cancellation uses `TerminateJobObject`, which is hard-
/// kill only — no grace window — so this constant is Unix-only.
#[cfg(unix)]
const CANCEL_GRACE: Duration = Duration::from_secs(2);

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

        // ---- Build the command --------------------------------------------
        let (program, argv_rest) = argv
            .split_first()
            .ok_or_else(|| NodeError::Config("execution.command is empty".into()))?;

        let mut cmd = Command::new(program);
        cmd.args(argv_rest);
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

        // ---- Spawn (platform-specific) ------------------------------------
        let mut sup = platform_spawn(cmd)?;

        // ---- Stdin writer (if templated) ----------------------------------
        if let (Some(body), Some(mut stdin)) = (stdin_body, sup.child.stdin.take()) {
            // Writer runs on its own task so a child that reads
            // stdin fully before producing stdout can't deadlock
            // us (we're holding the read loop for stdout below).
            tokio::spawn(async move {
                let write_res = stdin.write_all(body.as_bytes()).await;
                drop(write_res);
                let shutdown_res = stdin.shutdown().await;
                drop(shutdown_res);
            });
        }

        // ---- Stream stdout/stderr -----------------------------------------
        let emitter = ctx.emitter.clone();
        let stdout_handle = spawn_line_reader(
            sup.child.stdout.take(),
            emitter.clone(),
            node.id.clone(),
            "stdout",
        );
        let stderr_handle =
            spawn_line_reader(sup.child.stderr.take(), emitter, node.id.clone(), "stderr");

        // ---- Wait / cancel ------------------------------------------------
        let outcome = tokio::select! {
            wait_res = sup.child.wait() => Outcome::Exit(wait_res),
            () = cancel.cancelled() => {
                platform_cancel(&mut sup).await;
                Outcome::Cancelled
            }
        };

        let _stdout_lines = stdout_handle.await.unwrap_or_default();
        let _stderr_lines = stderr_handle.await.unwrap_or_default();

        finalize_outcome(outcome)
    }
}

// =====================================================================
// Shared types
// =====================================================================

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
    let secrets_resolver = |name: &str| -> Option<String> {
        ctx.secrets_store.as_ref().and_then(|s| s.get(name).ok())
    };
    let kv_resolver = |_: &str| -> Option<String> { None }; // KV node arrives in Phase 7
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

    let argv: Vec<String> = nt
        .execution
        .command
        .iter()
        .map(|s| substitute(s, &subctx).map_err(|e| NodeError::Template(e.to_string())))
        .collect::<Result<_, _>>()?;

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
            let mut payload: HashMap<String, serde_json::Value> = HashMap::new();
            payload.insert("channel".into(), serde_json::Value::String(channel.into()));
            payload.insert("text".into(), serde_json::Value::String(line.clone()));
            emitter.emit_node(EventType::NodeOutput, node_id.clone(), 0, 0, payload);
            acc.push(line);
        }
        acc
    })
}

/// Map a select-outcome to an executor result. The stdout
/// accumulator used by the `output_parse` step is wired in
/// alongside it; for now successful exits return an empty
/// `NodeOutputs`.
fn finalize_outcome(outcome: Outcome) -> Result<NodeOutputs, NodeError> {
    match outcome {
        Outcome::Cancelled => Err(NodeError::Cancelled),
        Outcome::Exit(Err(e)) => Err(NodeError::Subprocess(format!("wait: {e}"))),
        Outcome::Exit(Ok(_status)) => Ok(NodeOutputs::new()),
    }
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
    //! terminates the whole tree atomically — no `taskkill /T`
    //! workarounds, no race with grand-children we can't see.

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

        // SAFETY: CreateJobObjectW returns an owned kernel handle.
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
    use crate::db::open;
    use crate::events::RunEvent;
    use crate::executor::wrap_process_env;
    use crate::recorder::RunRecorder;
    use crate::types::{Category, ExecutionSpec, Node, NodeType, OutputParse, Pos, Workflow};
    use tempfile::TempDir;
    use tokio::sync::broadcast;

    fn test_ctx() -> (RunContext, broadcast::Receiver<RunEvent>, TempDir) {
        let dir = TempDir::new().unwrap();
        let pool = open(dir.path().join("t.db")).unwrap();
        let wf = Workflow {
            id: "w".into(),
            name: String::new(),
            schema_version: 1,
            created_at: None,
            updated_at: None,
            variables: HashMap::new(),
            triggers: vec![],
            nodes: vec![],
            edges: vec![],
        };
        let rec = Arc::new(RunRecorder::start(pool, &wf, "{}", &HashMap::new(), "test").unwrap());
        let (em, rx) = Emitter::new(rec.clone());
        let ctx = RunContext {
            run_id: rec.run_id.clone(),
            workflow_id: "w".into(),
            workflow_name: String::new(),
            started_at_iso: String::new(),
            workspace: dir.path().to_path_buf(),
            variables: HashMap::new(),
            recorder: rec,
            emitter: Arc::new(em),
            secrets_store: None,
            env: wrap_process_env(),
            current_inputs: HashMap::new(),
            upstream_outputs: HashMap::new(),
        };
        (ctx, rx, dir)
    }

    fn subprocess_node_type(command: Vec<String>) -> NodeType {
        NodeType {
            id: "test_subprocess".into(),
            name: String::new(),
            category: Category::Execution,
            tags: vec![],
            icon: String::new(),
            description: String::new(),
            inputs: vec![],
            outputs: vec![],
            config: vec![],
            execution: ExecutionSpec {
                backend: ExecutionBackend::Subprocess,
                command,
                stdin_template: None,
                env: HashMap::new(),
                timeout_ms: None,
                output_parse: OutputParse::Text,
                output_map: HashMap::new(),
            },
        }
    }

    fn trivial_node() -> Node {
        Node {
            id: "n1".into(),
            ty: "test_subprocess".into(),
            name: String::new(),
            config: HashMap::new(),
            pos: Pos::default(),
            timeout_ms: None,
            retry: None,
            continue_on_error: false,
        }
    }

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
        assert!(res.is_empty(), "no outputs wired yet (output_parse step)");
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
