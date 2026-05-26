//! Timeout-bounded `wsl.exe` execution with explicit kill + reap.
//!
//! Wrapping `tokio::time::timeout` around `Command::output()` was the
//! original Round-1 fix, but tokio's `Child` does NOT `kill_on_drop` by
//! default — when the timeout fires the future is dropped, but the
//! underlying process can continue until it exits on its own. These
//! helpers spawn explicitly, drain stdout/stderr concurrently with
//! `wait()`, and on timeout/error: `SIGKILL` the child, await its
//! status, and abort the reader handles before returning.
//!
//! `run_with_timeout_and_stdin` additionally writes a stdin payload
//! INSIDE the timeout scope so a stalled child can't hang the parent
//! on `write_all`.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};

/// Failure raised by [`run_with_timeout`] and [`run_with_timeout_and_stdin`].
#[derive(Debug, thiserror::Error)]
pub enum WslExecError {
    /// `Command::spawn` failed (wsl.exe not on PATH, etc.).
    #[error("spawn failed: {0}")]
    Spawn(#[source] std::io::Error),
    /// The bounded operation did not complete within the deadline. The child
    /// was killed and reaped before this error was returned.
    #[error("timed out after {0:?}")]
    TimedOut(Duration),
    /// `child.wait()` or `child.stdin.write_all()` failed mid-flight.
    #[error("{0}")]
    Io(String),
}

/// Spawn a `wsl.exe` command and run it under `timeout`.
///
/// Drains stdout/stderr concurrently with `wait()`. On timeout or wait error:
/// SIGKILL + reap + abort reader handles before returning. Caller's `Command`
/// MUST configure argv before passing it in; stdout/stderr are forced to
/// `piped()` here so the drainers can observe EOF cleanly.
pub async fn run_with_timeout(
    mut cmd: Command,
    timeout: Duration,
) -> Result<std::process::Output, WslExecError> {
    cmd.kill_on_drop(true);
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    let child = cmd.spawn().map_err(WslExecError::Spawn)?;
    drain_and_wait(child, timeout, None).await
}

/// Like `run_with_timeout` but pipes `stdin_bytes` to the child's stdin first.
/// The stdin write happens INSIDE the same timeout scope so a stalled child
/// can't hang `write_all` indefinitely.
pub async fn run_with_timeout_and_stdin(
    mut cmd: Command,
    timeout: Duration,
    stdin_bytes: Vec<u8>,
) -> Result<std::process::Output, WslExecError> {
    cmd.kill_on_drop(true);
    cmd.stdin(std::process::Stdio::piped());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    let child = cmd.spawn().map_err(WslExecError::Spawn)?;
    drain_and_wait(child, timeout, Some(stdin_bytes)).await
}

async fn drain_and_wait(
    mut child: Child,
    timeout: Duration,
    stdin_bytes: Option<Vec<u8>>,
) -> Result<std::process::Output, WslExecError> {
    let stdout_handle = child.stdout.take().map(|mut s| {
        tokio::spawn(async move {
            let mut buf = Vec::new();
            drop(s.read_to_end(&mut buf).await);
            buf
        })
    });
    let stderr_handle = child.stderr.take().map(|mut s| {
        tokio::spawn(async move {
            let mut buf = Vec::new();
            drop(s.read_to_end(&mut buf).await);
            buf
        })
    });
    let stdin_taken = child.stdin.take();

    let work = async {
        if let (Some(bytes), Some(mut stdin)) = (stdin_bytes, stdin_taken) {
            stdin
                .write_all(&bytes)
                .await
                .map_err(|e| WslExecError::Io(format!("write stdin: {e}")))?;
            // BrokenPipe just means the child closed stdin early; that is
            // legitimate (the child may have already finished consuming).
            if let Err(e) = stdin.shutdown().await
                && e.kind() != std::io::ErrorKind::BrokenPipe
            {
                return Err(WslExecError::Io(format!("shutdown stdin: {e}")));
            }
        }
        child
            .wait()
            .await
            .map_err(|e| WslExecError::Io(format!("wait child: {e}")))
    };

    let status = match tokio::time::timeout(timeout, work).await {
        Ok(Ok(status)) => status,
        Ok(Err(e)) => {
            abort_handles(stdout_handle.as_ref(), stderr_handle.as_ref());
            drop(child.kill().await);
            drop(child.wait().await);
            return Err(e);
        },
        Err(_) => {
            abort_handles(stdout_handle.as_ref(), stderr_handle.as_ref());
            drop(child.kill().await);
            drop(child.wait().await);
            return Err(WslExecError::TimedOut(timeout));
        },
    };

    let stdout = match stdout_handle {
        Some(h) => h.await.unwrap_or_default(),
        None => Vec::new(),
    };
    let stderr = match stderr_handle {
        Some(h) => h.await.unwrap_or_default(),
        None => Vec::new(),
    };
    Ok(std::process::Output {
        status,
        stdout,
        stderr,
    })
}

fn abort_handles(
    stdout: Option<&tokio::task::JoinHandle<Vec<u8>>>,
    stderr: Option<&tokio::task::JoinHandle<Vec<u8>>>,
) {
    if let Some(h) = stdout {
        h.abort();
    }
    if let Some(h) = stderr {
        h.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quick_cmd(args: &[&str]) -> Command {
        // Use `/usr/bin/true` and friends on Unix so tests pass without WSL.
        // Tests assert the timeout/cleanup semantics, not wsl.exe behavior.
        #[cfg(unix)]
        {
            let mut c = Command::new("/bin/sh");
            c.arg("-c");
            c.arg(args.join(" "));
            c
        }
        #[cfg(not(unix))]
        {
            let mut c = Command::new("cmd.exe");
            c.args(["/c", &args.join(" ")]);
            c
        }
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn run_with_timeout_completes_under_deadline() {
        let out = run_with_timeout(quick_cmd(&["echo hi"]), Duration::from_secs(5))
            .await
            .expect("should complete");
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "hi");
        assert!(out.status.success());
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn run_with_timeout_kills_overrunning_child() {
        let start = std::time::Instant::now();
        let err = run_with_timeout(quick_cmd(&["sleep 30"]), Duration::from_millis(150))
            .await
            .expect_err("should time out");
        assert!(matches!(err, WslExecError::TimedOut(_)));
        // Must return promptly after the deadline, not after the sleep.
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "elapsed {:?}",
            start.elapsed()
        );
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn run_with_timeout_and_stdin_delivers_payload() {
        let out = run_with_timeout_and_stdin(
            quick_cmd(&["cat"]),
            Duration::from_secs(5),
            b"hello\n".to_vec(),
        )
        .await
        .expect("should complete");
        assert_eq!(String::from_utf8_lossy(&out.stdout), "hello\n");
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn run_with_timeout_and_stdin_tolerates_broken_pipe() {
        // `true` exits without reading stdin; the write may succeed (buffered)
        // and the shutdown may surface BrokenPipe — both are legitimate.
        let out = run_with_timeout_and_stdin(
            quick_cmd(&["true"]),
            Duration::from_secs(5),
            b"ignored".to_vec(),
        )
        .await
        .expect("BrokenPipe must not surface as Err");
        assert!(out.status.success());
    }
}
