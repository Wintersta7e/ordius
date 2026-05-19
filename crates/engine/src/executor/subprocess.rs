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
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

/// Executor for nodes whose `ExecutionSpec::backend` is
/// [`ExecutionBackend::Subprocess`].
///
/// The real spawn / cancel / output handling lands incrementally
/// across Phase 6; this stub returns [`NodeError::NotImplemented`]
/// until the wiring is complete.
pub struct SubprocessExecutor;

#[async_trait]
impl NodeExecutor for SubprocessExecutor {
    fn supports(&self, nt: &NodeType) -> bool {
        nt.execution.backend == ExecutionBackend::Subprocess
    }

    async fn run(
        &self,
        _node: &Node,
        _nt: &NodeType,
        _ctx: &RunContext,
        _cancel: CancellationToken,
    ) -> Result<NodeOutputs, NodeError> {
        Err(NodeError::NotImplemented)
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
#[allow(dead_code)] // wired into SubprocessExecutor::run next; kept module-level for testability
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

/// Windows command-configuration hook. Job-Object setup happens
/// post-spawn, so the pre-spawn hook is currently a no-op
/// placeholder.
#[cfg(windows)]
#[allow(dead_code)] // wired into SubprocessExecutor::run alongside Job Object setup
pub(crate) fn configure_windows_command(_cmd: &mut Command) {}

#[cfg(test)]
mod tests {
    use super::*;

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
}
