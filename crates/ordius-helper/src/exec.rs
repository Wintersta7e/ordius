//! Exec subcommand: reads an `ExecRequestV1` from stdin, runs the requested
//! argv-only command, forwards stdout/stderr in real time, exits with the
//! child's status code.

use crate::protocol::ExecRequestV1;
use anyhow::Context;
use std::io::BufRead;
use std::process::{Command, Stdio};

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

    let mut cmd = Command::new(&req.program);
    cmd.args(&req.args);
    cmd.env_clear();
    cmd.envs(req.env.iter());
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
    let status = child.wait().context("wait on child")?;
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
}
