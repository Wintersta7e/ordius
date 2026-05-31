//! SSH exec-channel process implementation.
//!
//! [`open_helper_exec`] opens a russh exec channel that runs
//! `<helper-path> exec --argv-json`, writes the [`ExecRequestV1`] JSON to the
//! channel's stdin, then hands the channel to a background **demux task**.
//!
//! russh delivers stdout, stderr, and the exit status interleaved on a single
//! [`russh::ChannelMsg`] stream (`channel.wait().await`). The demux task routes
//! `Data{..}` → an stdout pipe, `ExtendedData{ext == 1, ..}` → an stderr pipe,
//! and `ExitStatus{..}` / `ExitSignal{..}` → the exit channel. The two pipes are
//! exposed to the caller as separate [`ProcessPipe`] readers.
//!
//! ## Cancel
//!
//! russh / OpenSSH do not reliably forward a client channel "signal" request to
//! the server process, so [`SshExecHandle::cancel`] does **not** rely on
//! `channel.signal(..)`. Instead it closes the channel: sshd then sends SIGHUP
//! to the remote helper, and the helper's signal-forwarder (T3) kills the
//! remote child's process group. Closing one channel kills one process — the
//! shared connection is left intact.

use async_trait::async_trait;
use base64::Engine as _;
use ordius_helper::protocol::ExecRequestV1;
use tokio::io::AsyncWriteExt as _;
use tokio::sync::{Notify, oneshot};

use crate::environment::runtime::error::DispatchError;
use crate::environment::runtime::transport::{EnvProcess, ProcessCmd, ProcessExit, ProcessPipe};

use super::connection::{SshConnection, SshConnectionLike as _};

/// Capacity of each demux pipe (stdout/stderr). Output backs up here when the
/// reader is slow; russh's own channel window provides additional buffering.
const PIPE_CAPACITY: usize = 64 * 1024;

/// SSH extended-data type code for stderr (`SSH_EXTENDED_DATA_STDERR`).
const SSH_EXTENDED_DATA_STDERR: u32 = 1;

/// Build the helper exec request from an env-neutral process command.
///
/// `stdin` bytes (if any) are base64-encoded with the standard RFC 4648
/// alphabet (padded), matching [`ExecRequestV1::stdin_b64`].
pub fn exec_request_from_cmd(cmd: &ProcessCmd) -> ExecRequestV1 {
    ExecRequestV1 {
        version: 1,
        program: cmd.program.clone(),
        args: cmd.args.clone(),
        env: cmd.env.clone(),
        cwd: cmd.cwd.as_ref().map(|p| p.as_str().to_string()),
        stdin_b64: cmd
            .stdin
            .as_ref()
            .map(|bytes| base64::engine::general_purpose::STANDARD.encode(bytes.as_ref())),
    }
}

/// POSIX single-quote a string for safe inclusion in a remote shell command.
///
/// Wraps `value` in single quotes and escapes any embedded single quote via the
/// canonical `'\''` sequence. Pure; covered by `ssh_posix_single_quote_*` tests.
pub fn posix_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

/// Process backed by a russh exec channel running `ordius-helper exec --argv-json`.
pub struct SshProcess {
    env_id: String,
    stdout: Option<ProcessPipe>,
    stderr: Option<ProcessPipe>,
    inner: Box<dyn SshExecHandle>,
}

/// Handle over a live SSH exec channel: await its exit, or cancel it.
///
/// Split out behind a trait so the demux/channel wiring can be swapped for a
/// fake in unit tests without opening a real session.
#[async_trait]
pub trait SshExecHandle: Send {
    /// Await the remote process's exit status.
    async fn wait_exit(&mut self) -> Result<ProcessExit, DispatchError>;
    /// Best-effort cancel: closes the channel so sshd SIGHUPs the remote helper.
    async fn cancel(&mut self) -> Result<(), DispatchError>;
}

impl SshProcess {
    /// Assemble a process from its two output pipes and an exec handle.
    pub fn new(
        env_id: impl Into<String>,
        stdout: Option<ProcessPipe>,
        stderr: Option<ProcessPipe>,
        inner: Box<dyn SshExecHandle>,
    ) -> Self {
        Self {
            env_id: env_id.into(),
            stdout,
            stderr,
            inner,
        }
    }
}

#[async_trait]
impl EnvProcess for SshProcess {
    fn take_stdout(&mut self) -> Option<ProcessPipe> {
        self.stdout.take()
    }

    fn take_stderr(&mut self) -> Option<ProcessPipe> {
        self.stderr.take()
    }

    async fn wait(&mut self) -> Result<ProcessExit, DispatchError> {
        self.inner.wait_exit().await
    }

    async fn cancel(&mut self) -> Result<(), DispatchError> {
        self.inner.cancel().await.map_err(|e| match e {
            DispatchError::EnvLost { .. } => DispatchError::EnvLost {
                env_id: self.env_id.clone(),
            },
            other => other,
        })
    }
}

// ── russh exec handle ─────────────────────────────────────────────────────────

/// Production [`SshExecHandle`] backed by a spawned demux task.
///
/// `wait_exit` awaits the exit `oneshot` the demux task fills when the channel
/// closes. `cancel` fires `cancel` (a [`Notify`]) which the demux task selects
/// on to close the channel.
struct RusshExecHandle {
    /// Filled by the demux task when the channel reaches EOF/close.
    exit_rx: Option<oneshot::Receiver<ProcessExit>>,
    /// Notifies the demux task to close the channel (cancel path).
    cancel: std::sync::Arc<Notify>,
}

#[async_trait]
impl SshExecHandle for RusshExecHandle {
    async fn wait_exit(&mut self) -> Result<ProcessExit, DispatchError> {
        // Used both when `wait` was already consumed and when the demux task
        // dropped the sender without an explicit exit status (abnormal close).
        let abnormal = ProcessExit {
            code: -1,
            signal: None,
        };
        match self.exit_rx.take() {
            // `Err` means the demux task closed the channel without reporting an
            // ExitStatus — surface the abnormal exit rather than blocking.
            Some(rx) => Ok(rx.await.unwrap_or(abnormal)),
            None => Ok(abnormal),
        }
    }

    async fn cancel(&mut self) -> Result<(), DispatchError> {
        // Wake the demux task; it closes the channel (best-effort). Idempotent:
        // a second cancel is a harmless extra notification.
        self.cancel.notify_one();
        Ok(())
    }
}

/// Open a helper exec channel on `conn` and return a process handle.
///
/// Serialises the command into an [`ExecRequestV1`], opens a session channel,
/// runs `<quoted-helper> exec --argv-json`, writes the request JSON to stdin,
/// EOFs stdin, then spawns a demux task that streams stdout/stderr and the exit
/// status. The `Handle` mutex is released as soon as the channel is open — all
/// subsequent channel I/O happens on the owned [`russh::Channel`], which is
/// independently `Send + Sync`.
pub async fn open_helper_exec(
    conn: std::sync::Arc<SshConnection>,
    helper_path: &str,
    cmd: ProcessCmd,
) -> Result<SshProcess, DispatchError> {
    let request = exec_request_from_cmd(&cmd);
    let request_json = serde_json::to_vec(&request)
        .map_err(|e| DispatchError::PlanBuild(format!("serialize exec request: {e}")))?;
    let remote_cmd = format!("{} exec --argv-json", posix_single_quote(helper_path));
    open_command_channel(conn, &remote_cmd, request_json).await
}

/// Open a helper **probe** channel on `conn` and return a process handle.
///
/// Runs `<quoted-helper> probe`, writes the serialized probe plan
/// (`ProbePlanV1` JSON) to stdin, then streams the helper's JSONL probe
/// outcomes on stdout. Same channel/demux machinery as [`open_helper_exec`];
/// the only differences are the remote subcommand and the stdin payload.
pub async fn open_helper_probe(
    conn: std::sync::Arc<SshConnection>,
    helper_path: &str,
    plan_json: Vec<u8>,
) -> Result<SshProcess, DispatchError> {
    let remote_cmd = format!("{} probe", posix_single_quote(helper_path));
    open_command_channel(conn, &remote_cmd, plan_json).await
}

/// Shared channel plumbing: open a session, run `remote_cmd`, write `stdin`,
/// EOF, then spawn a demux task that streams stdout/stderr + the exit status.
///
/// The `Handle` mutex is released as soon as the channel is open; all further
/// channel I/O happens on the owned [`russh::Channel`], which is independently
/// `Send + Sync`.
async fn open_command_channel(
    conn: std::sync::Arc<SshConnection>,
    remote_cmd: &str,
    stdin: Vec<u8>,
) -> Result<SshProcess, DispatchError> {
    let env_id = conn.id().to_string();

    // Open the session + exec while holding the Handle lock, then release it.
    let channel = {
        let handle = conn.handle().await;
        let map_lost = |what: &str, e: russh::Error| {
            // A channel-open / exec failure on an established session means the
            // session is gone or unusable; mark it closed so the cache opens a
            // fresh one next time, and surface EnvUnreachable.
            conn.mark_closed();
            DispatchError::EnvUnreachable {
                env_id: env_id.clone(),
                reason: format!("{what}: {e}"),
            }
        };

        let channel = handle
            .channel_open_session()
            .await
            .map_err(|e| map_lost("open exec channel", e))?;
        channel
            .exec(true, remote_cmd.as_bytes())
            .await
            .map_err(|e| map_lost("exec helper", e))?;
        // Write the request to the remote helper's stdin, then signal EOF so the
        // helper's `read_to_end` on stdin returns and it begins executing.
        channel
            .data_bytes(stdin)
            .await
            .map_err(|e| map_lost("write exec request", e))?;
        channel
            .eof()
            .await
            .map_err(|e| map_lost("eof exec stdin", e))?;
        // Release the Handle lock now: all further channel I/O is on the owned
        // `Channel`, which is independently `Send + Sync`.
        drop(handle);
        channel
    };

    // Two host-side pipes: the demux task writes into the *_w halves; the caller
    // reads the *_r halves as ProcessPipe stdout/stderr.
    let (out_w, out_r) = tokio::io::duplex(PIPE_CAPACITY);
    let (err_w, err_r) = tokio::io::duplex(PIPE_CAPACITY);

    let (exit_tx, exit_rx) = oneshot::channel::<ProcessExit>();
    let cancel = std::sync::Arc::new(Notify::new());

    tokio::spawn(demux_channel(
        channel,
        out_w,
        err_w,
        exit_tx,
        cancel.clone(),
    ));

    let handle = RusshExecHandle {
        exit_rx: Some(exit_rx),
        cancel,
    };

    // Coerce (not cast) the concrete duplex readers into the boxed trait-object
    // pipe type the EnvProcess contract expects.
    let stdout_pipe: ProcessPipe = Box::pin(out_r);
    let stderr_pipe: ProcessPipe = Box::pin(err_r);

    Ok(SshProcess::new(
        env_id,
        Some(stdout_pipe),
        Some(stderr_pipe),
        Box::new(handle),
    ))
}

/// Demux loop: own the channel, route messages to the pipes + exit channel.
///
/// Runs until `channel.wait()` returns `None` (channel fully closed) or `cancel`
/// fires. Output-pipe write errors are tolerated: a caller that drops a pipe
/// reader early (e.g. only reads stdout) is legitimate and must not stall the
/// loop — exit detection still needs to run.
async fn demux_channel(
    mut channel: russh::Channel<russh::client::Msg>,
    mut out_w: tokio::io::DuplexStream,
    mut err_w: tokio::io::DuplexStream,
    exit_tx: oneshot::Sender<ProcessExit>,
    cancel: std::sync::Arc<Notify>,
) {
    use russh::ChannelMsg;

    let mut exit: Option<ProcessExit> = None;

    loop {
        tokio::select! {
            // Cancel: close the channel so sshd SIGHUPs the remote helper, then
            // keep draining until the channel reports closed.
            () = cancel.notified() => {
                drop(channel.eof().await);
                drop(channel.close().await);
            }
            msg = channel.wait() => {
                let Some(msg) = msg else { break };
                match msg {
                    ChannelMsg::Data { data } => {
                        // Tolerate a dropped reader; keep looping for exit status.
                        drop(out_w.write_all(&data).await);
                    }
                    ChannelMsg::ExtendedData { data, ext }
                        if ext == SSH_EXTENDED_DATA_STDERR =>
                    {
                        drop(err_w.write_all(&data).await);
                    }
                    ChannelMsg::ExitStatus { exit_status } => {
                        exit = Some(ProcessExit {
                            #[allow(clippy::cast_possible_wrap)]
                            code: exit_status as i32,
                            signal: None,
                        });
                    }
                    ChannelMsg::ExitSignal { signal_name, .. } => {
                        let name = format!("{signal_name:?}");
                        // No numeric code on a signal exit; use the POSIX
                        // convention shell uses (128 + signum is unavailable
                        // here, so flag via a non-zero code + signal name).
                        exit = Some(ProcessExit {
                            code: -1,
                            signal: Some(name),
                        });
                    }
                    // Eof/Close/WindowAdjusted/Success/etc.: keep draining until
                    // wait() returns None.
                    _ => {}
                }
            }
        }
    }

    // Closing the writer halves signals EOF to the caller's stdout/stderr readers.
    drop(out_w);
    drop(err_w);

    let final_exit = exit.unwrap_or(ProcessExit {
        code: -1,
        signal: None,
    });
    // Receiver may have been dropped (caller never awaited wait); ignore.
    drop(exit_tx.send(final_exit));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn posix_single_quote_wraps_plain_value() {
        assert_eq!(posix_single_quote("/usr/bin/helper"), "'/usr/bin/helper'");
    }

    #[test]
    fn posix_single_quote_escapes_embedded_quote() {
        // a'b -> 'a'\''b'
        assert_eq!(posix_single_quote("a'b"), "'a'\\''b'");
    }

    #[test]
    fn posix_single_quote_handles_spaces_and_shell_metachars() {
        // The whole value is one quoted token; $ ; & are inert inside quotes.
        assert_eq!(
            posix_single_quote("/opt/ord ius/$x;rm -rf /"),
            "'/opt/ord ius/$x;rm -rf /'"
        );
    }

    #[test]
    fn exec_request_omits_stdin_when_absent() {
        use crate::environment::runtime::transport::Stdio;
        let cmd = ProcessCmd {
            program: "ls".into(),
            args: vec![],
            env: std::collections::HashMap::new(),
            cwd: None,
            stdin: None,
            stdout: Stdio::Piped,
            stderr: Stdio::Piped,
        };
        let req = exec_request_from_cmd(&cmd);
        assert_eq!(req.version, 1);
        assert!(req.stdin_b64.is_none());
        assert!(req.cwd.is_none());
    }
}
