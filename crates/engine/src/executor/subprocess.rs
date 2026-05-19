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
//! The OS-level guarantees replace the `taskkill /T` workarounds
//! the engine used in earlier prototypes.
#![allow(unsafe_code)]

use crate::executor::{NodeError, NodeExecutor, NodeOutputs, RunContext};
use crate::types::{ExecutionBackend, Node, NodeType};
use async_trait::async_trait;
#[cfg(unix)]
use std::time::Duration;
use tokio::process::Command;
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
///
/// The argv-substitution / streaming / output-parsing wiring lands
/// in later Phase-6 tasks; today this is the bare spawn + cancel
/// loop dispatched per OS.
pub struct SubprocessExecutor;

#[async_trait]
impl NodeExecutor for SubprocessExecutor {
    fn supports(&self, nt: &NodeType) -> bool {
        nt.execution.backend == ExecutionBackend::Subprocess
    }

    async fn run(
        &self,
        _node: &Node,
        nt: &NodeType,
        _ctx: &RunContext,
        cancel: CancellationToken,
    ) -> Result<NodeOutputs, NodeError> {
        let (program, args) = nt
            .execution
            .command
            .split_first()
            .ok_or_else(|| NodeError::Config("execution.command is empty".into()))?;

        let mut cmd = Command::new(program);
        cmd.args(args);
        cmd.kill_on_drop(true);

        #[cfg(unix)]
        {
            configure_unix_command(&mut cmd);
            run_unix(cmd, cancel).await
        }
        #[cfg(windows)]
        {
            configure_windows_command(&mut cmd);
            run_windows(cmd, cancel).await
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = (cmd, cancel);
            Err(NodeError::Subprocess(
                "subprocess executor: unsupported platform".into(),
            ))
        }
    }
}

// =====================================================================
// Unix
// =====================================================================

#[cfg(unix)]
async fn run_unix(mut cmd: Command, cancel: CancellationToken) -> Result<NodeOutputs, NodeError> {
    let mut child = cmd
        .spawn()
        .map_err(|e| NodeError::Subprocess(format!("spawn: {e}")))?;
    let pid = child
        .id()
        .ok_or_else(|| NodeError::Subprocess("child has no pid (already reaped)".into()))?;

    tokio::select! {
        wait_res = child.wait() => {
            let _status = wait_res
                .map_err(|e| NodeError::Subprocess(format!("wait: {e}")))?;
            // Output parsing + exit_code port land in Task 6.8.
            Ok(NodeOutputs::new())
        }
        () = cancel.cancelled() => {
            cancel_unix_child(&mut child, pid).await;
            Err(NodeError::Cancelled)
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

/// SIGTERM the whole process group, give it [`CANCEL_GRACE`], then
/// SIGKILL the group if the child hasn't exited yet.
#[cfg(unix)]
async fn cancel_unix_child(child: &mut tokio::process::Child, pid: u32) {
    use nix::sys::signal::Signal;

    let _soft = kill_unix_pgroup(pid, Signal::SIGTERM);
    let grace = tokio::time::sleep(CANCEL_GRACE);
    tokio::select! {
        wait_res = child.wait() => { drop(wait_res); }
        () = grace => {
            let _hard = kill_unix_pgroup(pid, Signal::SIGKILL);
            let reap = child.wait().await;
            drop(reap);
        }
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

/// Windows command-configuration hook. Job-Object setup happens
/// post-spawn (see [`win_job::spawn_supervised`]), so the
/// pre-spawn hook itself is a no-op placeholder retained for
/// symmetry with the Unix path.
#[cfg(windows)]
pub(crate) fn configure_windows_command(_cmd: &mut Command) {}

#[cfg(windows)]
async fn run_windows(
    mut cmd: Command,
    cancel: CancellationToken,
) -> Result<NodeOutputs, NodeError> {
    let mut sup = win_job::spawn_supervised(&mut cmd)?;
    let pid = sup
        .child
        .id()
        .ok_or_else(|| NodeError::Subprocess("child has no pid (already reaped)".into()))?;
    win_job::resume_initial_thread(pid)?;

    tokio::select! {
        wait_res = sup.child.wait() => {
            let _status = wait_res
                .map_err(|e| NodeError::Subprocess(format!("wait: {e}")))?;
            Ok(NodeOutputs::new())
        }
        () = cancel.cancelled() => {
            // TerminateJobObject hard-kills every process in the
            // job atomically — the analog of `kill(-pgid, SIGKILL)`.
            // No grace period because the call is unconditional;
            // workflows that need cooperative shutdown should
            // honor cancellation in their own scripts.
            win_job::terminate_job(sup.job);
            // Reap so the OS releases the child's handles before
            // run finalization. Supervised's Drop closes the job.
            let reap = sup.child.wait().await;
            drop(reap);
            Err(NodeError::Cancelled)
        }
    }
}

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

    use super::{Command, NodeError};
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

    /// A spawned-suspended child and the Job Object that owns it.
    /// The child's main thread is still parked at this point;
    /// `resume_initial_thread` will let it actually run.
    ///
    /// Closing the job handle on drop kills every still-running
    /// process in the tree (via `KILL_ON_JOB_CLOSE`), which is the
    /// safety net even if cancellation didn't run explicitly.
    pub(super) struct Supervised {
        pub(super) child: tokio::process::Child,
        pub(super) job: HANDLE,
    }

    // SAFETY: Windows kernel HANDLEs are safe to use from any
    // thread — concurrency is enforced by the kernel — and we hold
    // this one uniquely (no aliasing). HANDLE only fails Send/Sync
    // automatically because its inner field is a raw pointer.
    unsafe impl Send for Supervised {}
    unsafe impl Sync for Supervised {}

    impl Drop for Supervised {
        fn drop(&mut self) {
            // SAFETY: `job` is a live kernel handle we own.
            let close = unsafe { CloseHandle(self.job) };
            drop(close);
        }
    }

    /// Spawn `cmd` with the main thread suspended, create a fresh
    /// Job Object configured to kill its members on close, and
    /// assign the child to that job before any user code runs.
    ///
    /// On any failure after the child has been spawned we close the
    /// job handle so the OS can clean up.
    pub(super) fn spawn_supervised(cmd: &mut Command) -> Result<Supervised, NodeError> {
        cmd.creation_flags(CREATE_SUSPENDED.0 | CREATE_NEW_PROCESS_GROUP.0);
        let child = cmd
            .spawn()
            .map_err(|e| NodeError::Subprocess(format!("spawn: {e}")))?;

        // SAFETY: CreateJobObjectW returns an owned kernel handle.
        // Ownership is transferred to `Supervised` on success and
        // explicitly closed on every error path below.
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

    /// Resume the (single) main thread of a child spawned with
    /// `CREATE_SUSPENDED`. The child has exactly one thread until
    /// it runs, so the first thread we find owned by `pid` is the
    /// one to wake up.
    ///
    /// The alternative — opening `CreateProcessW` ourselves to
    /// capture the thread handle returned in `PROCESS_INFORMATION`
    /// — would require bypassing `tokio::process::Command` entirely,
    /// which is a much larger surgery.
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

    /// Terminate every process in `job` with exit code 1. The
    /// child's `wait()` future then resolves naturally with an
    /// abnormal exit status.
    pub(super) fn terminate_job(job: HANDLE) {
        // SAFETY: `job` is a live kernel handle the caller owns.
        // Errors from TerminateJobObject (e.g. job already empty)
        // are not actionable — we cancel best-effort.
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
    use crate::emitter::Emitter;
    use crate::executor::wrap_process_env;
    use crate::recorder::RunRecorder;
    use crate::types::{Category, ExecutionSpec, Node, NodeType, OutputParse, Pos, Workflow};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn test_ctx() -> (RunContext, TempDir) {
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
        let (em, _) = Emitter::new(rec.clone());
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
        (ctx, dir)
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
            id: "n".into(),
            ty: "test_subprocess".into(),
            name: String::new(),
            config: HashMap::new(),
            pos: Pos::default(),
            timeout_ms: None,
            retry: None,
            continue_on_error: false,
        }
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn unix_spawn_smoke() {
        use std::process::Stdio;

        let mut cmd = Command::new("sh");
        cmd.args(["-c", "echo hi"]).stdout(Stdio::piped());
        configure_unix_command(&mut cmd);

        let mut child = cmd.spawn().expect("spawn sh -c 'echo hi'");
        let status = child.wait().await.expect("wait for child");
        assert!(status.success(), "child should exit 0");
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn unix_run_completes_for_quick_command() {
        let (ctx, _dir) = test_ctx();
        let nt = subprocess_node_type(vec!["true".into()]);
        let node = trivial_node();
        let res = SubprocessExecutor
            .run(&node, &nt, &ctx, CancellationToken::new())
            .await
            .expect("true should exit 0");
        assert!(res.is_empty(), "no outputs wired yet (Task 6.8)");
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn unix_cancel_kills_process_group() {
        let (ctx, _dir) = test_ctx();
        // bash -c "sleep 30; echo done" — the inner sleep is what
        // would survive a naive `child.kill()` if cancellation only
        // killed the bash leader. setsid + kill(-pgid) reaches it.
        let nt = subprocess_node_type(vec![
            "bash".into(),
            "-c".into(),
            "sleep 30; echo done".into(),
        ]);
        let node = trivial_node();

        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();
        let nt_for_task = nt.clone();
        let node_for_task = node.clone();
        let ctx_arc = Arc::new(ctx);
        let ctx_for_task = ctx_arc.clone();

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

    #[cfg(windows)]
    #[tokio::test(flavor = "multi_thread")]
    async fn windows_resume_runs_suspended_child() {
        let (ctx, _dir) = test_ctx();
        let nt = subprocess_node_type(vec!["cmd".into(), "/C".into(), "echo hi".into()]);
        let node = trivial_node();
        let res = SubprocessExecutor
            .run(&node, &nt, &ctx, CancellationToken::new())
            .await;
        assert!(res.is_ok(), "child should resume and exit 0: {res:?}");
    }
}
