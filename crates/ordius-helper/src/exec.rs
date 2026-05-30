//! Exec subcommand: reads an `ExecRequestV1` from stdin, runs the requested
//! argv-only command, forwards stdout/stderr in real time, exits with the
//! child's status code.

use crate::protocol::ExecRequestV1;
use anyhow::Context;
use std::io::BufRead;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

/// Sane PATH to inject when the caller does not supply one.  Covers the
/// standard FHS binary directories found on every Linux / macOS target.
const DEFAULT_EXEC_PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

/// Determine the PATH to give the child.
///
/// If the request's `env` map contains an explicit `PATH` entry, that wins
/// (caller override).  Otherwise fall back to [`DEFAULT_EXEC_PATH`] so that
/// bare program names (e.g. `sh`, `python3`) resolve even after `env_clear`.
fn path_for_child(req: &ExecRequestV1) -> String {
    req.env
        .get("PATH")
        .cloned()
        .unwrap_or_else(|| DEFAULT_EXEC_PATH.to_string())
}

/// Resolve a program name to its absolute path using the helper's own PATH,
/// then falling back to `DEFAULT_EXEC_PATH`.
///
/// This must happen *before* `Command::new` because `execvp` resolves bare
/// names through the *child's* env PATH (set via `env_clear` + explicit PATH),
/// not the helper's own environment.  If the caller supplied PATH=/custom/bin,
/// the child would fail to find `sh` even though the helper can see it.  By
/// resolving to an absolute path first, the child's PATH only controls what
/// *the child itself* can find — it never affects program lookup.
fn resolve_program(program: &str) -> std::path::PathBuf {
    // Already absolute or relative — pass through unchanged.
    if program.contains('/') {
        return program.into();
    }
    // Try the helper's own PATH first, then DEFAULT_EXEC_PATH.
    let search_path = std::env::var("PATH").unwrap_or_else(|_| DEFAULT_EXEC_PATH.to_string());
    for dir in search_path.split(':') {
        let candidate = std::path::Path::new(dir).join(program);
        if candidate.is_file() {
            return candidate;
        }
    }
    // Fallback: let the OS report the error at spawn time.
    program.into()
}

/// Apply `env_clear` then set PATH (resolved above) followed by all other
/// env vars from the request, skipping PATH a second time so it is never
/// double-set.  PATH is committed exactly once via `path_for_child` (which
/// already honors an explicit request PATH); the loop skip ensures no later
/// key in the map can silently overwrite it.
fn apply_request_env(cmd: &mut Command, req: &ExecRequestV1) {
    cmd.env_clear();
    cmd.env("PATH", path_for_child(req));
    for (k, v) in &req.env {
        if k != "PATH" {
            cmd.env(k, v);
        }
    }
}

/// Put the child in its own process group so `terminate_process_group` can
/// send signals to the whole tree (child + any grandchildren it spawns).
#[cfg(unix)]
fn configure_child_group(cmd: &mut Command) {
    use std::os::unix::process::CommandExt;
    cmd.process_group(0);
}

#[cfg(not(unix))]
fn configure_child_group(_cmd: &mut Command) {}

/// Send SIGTERM to the process group, wait `grace`, then send SIGKILL.
/// Passing `pgid = 0` is a no-op (guard against the pre-spawn race window).
///
/// Uses `nix` syscalls directly rather than the external `kill` binary —
/// `kill -TERM -<pgid>` without `--` silently no-ops on util-linux, and the
/// helper must work on remote/minimal boxes that may not have `kill` at all.
#[cfg(unix)]
fn terminate_process_group(pgid: u32, grace: Duration) {
    if pgid == 0 {
        return;
    }
    let pgrp = nix::unistd::Pid::from_raw(-i32::try_from(pgid).unwrap_or(i32::MAX));
    let _ = nix::sys::signal::kill(pgrp, nix::sys::signal::Signal::SIGTERM);
    std::thread::sleep(grace);
    let _ = nix::sys::signal::kill(pgrp, nix::sys::signal::Signal::SIGKILL);
}

/// Spawn a background thread that forwards SIGTERM/SIGINT/SIGHUP to the
/// child's process group and then exits with `128 + signum`.  The thread
/// reads `child_pgid` after it was stored by the caller, so it guards the
/// zero value (spawn race window) before acting.  Known limitation: a signal
/// that arrives after handler registration but before `child_pgid.store`
/// loads zero, skips the kill, and lets the helper exit — leaving the
/// just-spawned child orphaned to init.  Acceptable for a short-lived helper.
///
/// Must be installed BEFORE `cmd.spawn()` to ensure no signal is lost.
#[cfg(unix)]
fn install_signal_forwarder(child_pgid: Arc<AtomicU32>) -> anyhow::Result<()> {
    use signal_hook::consts::signal::{SIGHUP, SIGINT, SIGTERM};
    use signal_hook::iterator::Signals;

    let mut signals = Signals::new([SIGHUP, SIGINT, SIGTERM])?;
    std::thread::spawn(move || {
        if let Some(sig) = signals.forever().next() {
            let pgid = child_pgid.load(Ordering::SeqCst);
            if pgid != 0 {
                terminate_process_group(pgid, Duration::from_secs(2));
            }
            std::process::exit(128 + sig);
        }
    });
    Ok(())
}

#[cfg(not(unix))]
fn install_signal_forwarder(_child_pgid: Arc<AtomicU32>) -> anyhow::Result<()> {
    Ok(())
}

fn wait_child(mut child: Child) -> anyhow::Result<std::process::ExitStatus> {
    child.wait().context("wait on child")
}

/// Read one `ExecRequestV1` from `input` and exec it.
///
/// Returns `Ok(())` only when the child exited with status 0; otherwise
/// this process exits with the child's status code (so the parent observing
/// helper stdout/stderr gets identical wire semantics to a direct
/// `ssh -t host -- program args`).
pub fn run<R: BufRead>(mut input: R) -> anyhow::Result<()> {
    let mut buf = String::new();
    input
        .read_to_string(&mut buf)
        .context("read exec request from stdin")?;
    let req: ExecRequestV1 = serde_json::from_str(&buf).context("parse exec request from stdin")?;
    anyhow::ensure!(
        req.version == 1,
        "unsupported exec request version: {}",
        req.version
    );
    anyhow::ensure!(
        !req.program.is_empty(),
        "exec request has empty program field"
    );

    let child_pgid = Arc::new(AtomicU32::new(0));
    install_signal_forwarder(Arc::clone(&child_pgid))?;

    let program_path = resolve_program(&req.program);
    let mut cmd = Command::new(&program_path);
    cmd.args(&req.args);
    apply_request_env(&mut cmd, &req);
    configure_child_group(&mut cmd);
    if let Some(cwd) = req.cwd.as_deref() {
        cmd.current_dir(cwd);
    }
    let stdin_bytes = if let Some(b64) = req.stdin_b64.as_deref() {
        let decoded = decode_b64(b64).context("decode stdin_b64")?;
        cmd.stdin(Stdio::piped());
        Some(decoded)
    } else {
        cmd.stdin(Stdio::null());
        None
    };
    cmd.stdout(Stdio::inherit());
    cmd.stderr(Stdio::inherit());

    let mut child = cmd
        .spawn()
        .with_context(|| format!("spawn `{}` failed", req.program))?;
    child_pgid.store(child.id(), Ordering::SeqCst);

    if let Some(bytes) = stdin_bytes
        && let Some(mut child_stdin) = child.stdin.take()
    {
        use std::io::Write;
        // BrokenPipe = the child closed stdin before consuming the full
        // payload (legitimate for e.g. `head -n 1`).  Other I/O errors are
        // unusual but we still wait() and treat the child's exit status as
        // the authoritative outcome.
        drop(child_stdin.write_all(&bytes));
        drop(child_stdin); // close stdin so child reads EOF
    }
    let status = wait_child(child)?;
    // POSIX shell / ssh convention: signal-killed children report 128 + signum.
    // On Windows there's no signal concept so the fallback stays at 1.
    #[cfg(unix)]
    let code = {
        use std::os::unix::process::ExitStatusExt;
        status
            .code()
            .unwrap_or_else(|| 128 + status.signal().unwrap_or(0))
    };
    #[cfg(not(unix))]
    let code = status.code().unwrap_or(1);
    if code == 0 {
        Ok(())
    } else {
        std::process::exit(code);
    }
}

fn decode_b64(s: &str) -> anyhow::Result<Vec<u8>> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(s.as_bytes())
        .context("invalid base64 stdin payload")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::ExecRequestV1;
    use std::collections::HashMap;

    #[test]
    fn rejects_unsupported_version() {
        let req = ExecRequestV1 {
            version: 99,
            program: "echo".into(),
            args: vec!["x".into()],
            env: HashMap::new(),
            cwd: None,
            stdin_b64: None,
        };
        let s = serde_json::to_string(&req).unwrap();
        let err = run(s.as_bytes()).unwrap_err();
        assert!(err.to_string().contains("unsupported exec request version"));
    }

    #[test]
    fn rejects_empty_program() {
        let req = ExecRequestV1 {
            version: 1,
            program: String::new(),
            args: vec![],
            env: HashMap::new(),
            cwd: None,
            stdin_b64: None,
        };
        let s = serde_json::to_string(&req).unwrap();
        let err = run(s.as_bytes()).unwrap_err();
        assert!(err.to_string().contains("empty program"));
    }

    // Note: the success path can't easily run as a unit test without calling
    // a real binary that exits 0; that's exercised in T5's integration test
    // file (`tests/smoke.rs`) where echo / true are reliably available.

    #[test]
    fn decode_b64_handles_empty() {
        let v = decode_b64("").unwrap();
        assert!(v.is_empty());
    }

    #[test]
    fn decode_b64_rejects_invalid() {
        let err = decode_b64("not!valid?base64").unwrap_err();
        assert!(err.to_string().contains("invalid base64"));
    }

    #[cfg(unix)]
    #[test]
    fn terminate_process_group_kills_background_child() {
        use std::os::unix::process::CommandExt;
        use std::process::Stdio;
        use std::time::Duration;

        let mut cmd = Command::new("sh");
        cmd.args(["-c", "sleep 30 & sleep 30"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        cmd.process_group(0);
        let mut child = cmd.spawn().expect("spawn child group");
        let pgid = child.id();

        terminate_process_group(pgid, Duration::from_millis(50));
        drop(child.wait());

        // Give init time to reap the orphaned (now-killed) children.
        std::thread::sleep(Duration::from_millis(100));
        let probe = nix::unistd::Pid::from_raw(-i32::try_from(pgid).unwrap_or(i32::MAX));
        match nix::sys::signal::kill(probe, None) {
            Err(nix::errno::Errno::ESRCH) => {}, // expected: group gone
            other => panic!("process group {pgid} still alive: {other:?}"),
        }
    }
}
