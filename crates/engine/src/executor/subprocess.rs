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
use std::time::Duration;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

/// Grace period between the soft (`SIGTERM` / `TerminateJobObject`)
/// signal and the hard kill. Long enough for shells and Python
/// interpreters to flush stdout, short enough that a hung child
/// doesn't block run finalization.
const CANCEL_GRACE: Duration = Duration::from_secs(2);

/// Executor for nodes whose `ExecutionSpec::backend` is
/// [`ExecutionBackend::Subprocess`].
///
/// The argv-substitution / streaming / output-parsing wiring lands
/// in later Phase-6 tasks; today this is the bare spawn + cancel
/// loop.
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
        configure_unix_command(&mut cmd);
        #[cfg(windows)]
        configure_windows_command(&mut cmd);

        let mut child = cmd
            .spawn()
            .map_err(|e| NodeError::Subprocess(format!("spawn {program}: {e}")))?;

        #[cfg(unix)]
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
                cancel_child(&mut child, {
                    #[cfg(unix)] { pid }
                    #[cfg(not(unix))] { () }
                }).await;
                Err(NodeError::Cancelled)
            }
        }
    }
}

/// Cancel a running child: soft-terminate the whole process tree,
/// give it [`CANCEL_GRACE`], then hard-kill if still alive.
///
/// On Unix the soft signal is `SIGTERM` to `-pgid` and the hard
/// signal is `SIGKILL` to `-pgid`; Windows uses
/// [`tokio::process::Child::kill`] today (Job-Object termination
/// arrives in Task 6.5).
#[cfg(unix)]
async fn cancel_child(child: &mut tokio::process::Child, pid: u32) {
    use nix::sys::signal::Signal;

    // Best-effort signaling: ESRCH on a child that just exited is
    // expected; nothing actionable beyond what wait() already tells us.
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

#[cfg(windows)]
async fn cancel_child(child: &mut tokio::process::Child, _pid: ()) {
    // Real Job-Object cancellation lands later in this phase; today
    // we fall back to killing just the leader.
    let killed = child.kill().await;
    drop(killed);
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

/// Windows command-configuration hook. Job-Object setup happens
/// post-spawn, so the pre-spawn hook is currently a no-op
/// placeholder.
#[cfg(windows)]
#[allow(dead_code)] // wired into SubprocessExecutor::run alongside Job Object setup
pub(crate) fn configure_windows_command(_cmd: &mut Command) {}

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

        // Let bash actually start before we cancel.
        tokio::time::sleep(Duration::from_millis(150)).await;
        cancel.cancel();

        let res = tokio::time::timeout(Duration::from_secs(3), handle)
            .await
            .expect("cancel must surface within 3s")
            .expect("spawned task must not panic");

        assert!(matches!(res, Err(NodeError::Cancelled)), "got {res:?}");
    }
}
