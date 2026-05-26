//! Helper bootstrap into a WSL distro: push embedded bytes, sha256-verify,
//! atomic install, chmod +x. Result is the env-side path of the installed
//! helper binary.

use crate::environment::runtime::error::DispatchError;
use crate::environment::runtime::helper::{
    HelperTarget, helper_bytes_for_triple, verify_target_integrity,
};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

const PROBE_TIMEOUT: Duration = Duration::from_secs(3);
const PUSH_TIMEOUT: Duration = Duration::from_secs(20);

/// Pushed-helper state for one env.
#[derive(Debug, Clone)]
pub struct BootstrappedHelper {
    /// Target triple detected inside the WSL distro.
    pub triple: String,
    /// Absolute helper path inside the WSL distro.
    pub env_side_path: String,
}

/// Failure while detecting, pushing, verifying, or installing the helper.
#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    /// Failed to detect the distro's target triple.
    #[error("triple probe failed: {0}")]
    TripleProbe(String),
    /// No embedded helper matched the detected target triple.
    #[error("no embedded helper for env triple `{triple}`")]
    NoEmbeddedTriple {
        /// Target triple detected inside the WSL distro.
        triple: String,
    },
    /// Embedded helper bytes did not match the build-time manifest.
    #[error("embedded helper failed integrity self-check (triple `{triple}`)")]
    IntegritySelfCheck {
        /// Target triple whose embedded helper failed integrity verification.
        triple: String,
    },
    /// Failed while pushing bytes or invoking an install-side helper command.
    #[error("push failed: {0}")]
    Push(String),
    /// Env-side sha256 did not match the embedded manifest.
    #[error("sha256 verify failed in env (expected {expected}, got {actual})")]
    ShaMismatch {
        /// Expected sha256 hex from the embedded helper manifest.
        expected: String,
        /// Actual sha256 hex reported by `sha256sum` inside the WSL distro.
        actual: String,
    },
    /// Failed to mark the installed helper executable.
    #[error("chmod +x failed: {0}")]
    Chmod(String),
}

impl From<BootstrapError> for DispatchError {
    fn from(e: BootstrapError) -> Self {
        Self::HelperBootstrap(e.to_string())
    }
}

/// Detect the env's target triple via `uname -m` + `uname -s`.
pub async fn probe_env_triple(distro: &str) -> Result<String, BootstrapError> {
    let out = tokio::time::timeout(
        PROBE_TIMEOUT,
        Command::new("wsl.exe")
            .args([
                "-d",
                distro,
                "--exec",
                "/bin/sh",
                "-c",
                "printf '%s-%s' \"$(uname -m)\" \"$(uname -s | tr 'A-Z' 'a-z')\"",
            ])
            .output(),
    )
    .await
    .map_err(|_| BootstrapError::TripleProbe("timed out".into()))?
    .map_err(|e| BootstrapError::TripleProbe(e.to_string()))?;
    if !out.status.success() {
        return Err(BootstrapError::TripleProbe(format!(
            "uname exited with {:?}",
            out.status.code()
        )));
    }
    let raw = String::from_utf8_lossy(&out.stdout).trim().to_string();
    let normalized = match raw.as_str() {
        "x86_64-linux" => "x86_64-unknown-linux-musl".to_string(),
        "aarch64-linux" => "aarch64-unknown-linux-musl".to_string(),
        other => other.to_string(),
    };
    validate_triple(&normalized)?;
    Ok(normalized)
}

/// Reject triples whose characters fall outside `[A-Za-z0-9_-]` — they are
/// interpolated into shell-visible paths (e.g. `/tmp/.ordius/helper-…`) and
/// argv lists that re-enter the WSL distro. Real Rust target triples never
/// contain other characters.
fn validate_triple(raw: &str) -> Result<(), BootstrapError> {
    if raw.is_empty() {
        return Err(BootstrapError::TripleProbe("empty triple".into()));
    }
    if !raw
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(BootstrapError::TripleProbe(format!(
            "triple contains unexpected characters: {raw:?}"
        )));
    }
    Ok(())
}

/// Push helper bytes into the env, verify sha256, rename, and chmod +x.
///
/// Returns the env-side absolute path.
pub async fn bootstrap_helper(
    distro: &str,
    triple: &str,
) -> Result<BootstrappedHelper, BootstrapError> {
    let target: &'static HelperTarget =
        helper_bytes_for_triple(triple).ok_or_else(|| BootstrapError::NoEmbeddedTriple {
            triple: triple.to_string(),
        })?;
    if !verify_target_integrity(target) {
        return Err(BootstrapError::IntegritySelfCheck {
            triple: triple.to_string(),
        });
    }
    validate_triple(triple)?;
    let keep_basename = format!("helper-{}-{}", env!("CARGO_PKG_VERSION"), triple);
    let final_path = format!("/tmp/.ordius/{keep_basename}");
    let tmp_path = format!("{final_path}.tmp");
    push_bytes(distro, &tmp_path, target.bytes).await?;
    verify_sha_in_env(distro, &tmp_path, target.sha256).await?;
    atomic_install(distro, &tmp_path, &final_path).await?;
    chmod_exec(distro, &final_path).await?;
    cleanup_old_helpers(distro, triple, &keep_basename).await;
    Ok(BootstrappedHelper {
        triple: triple.to_string(),
        env_side_path: final_path,
    })
}

/// Best-effort: drop leftover helper binaries from prior engine versions for
/// the same triple. Uses `find` via `wsl.exe --exec` so no shell is involved;
/// the glob pattern is interpreted by `find -name`, not by a shell. Failures
/// are swallowed — the just-installed helper is already usable. `triple` has
/// been validated against `[A-Za-z0-9_-]+`.
async fn cleanup_old_helpers(distro: &str, triple: &str, keep_basename: &str) {
    let name_pattern = format!("helper-*-{triple}");
    let cmd_future = Command::new("wsl.exe")
        .args([
            "-d",
            distro,
            "--exec",
            "find",
            "/tmp/.ordius",
            "-maxdepth",
            "1",
            "-type",
            "f",
            "-name",
            &name_pattern,
            "-not",
            "-name",
            keep_basename,
            "-delete",
        ])
        .output();
    drop(tokio::time::timeout(PUSH_TIMEOUT, cmd_future).await);
}

async fn push_bytes(distro: &str, dest: &str, bytes: &[u8]) -> Result<(), BootstrapError> {
    let quoted_dest = shell_quote(dest);
    let cmd_str =
        format!("mkdir -p \"$(dirname {quoted_dest})\" && dd of={quoted_dest} status=none");
    let mut cmd = Command::new("wsl.exe");
    cmd.args(["-d", distro, "--exec", "/bin/sh", "-c", &cmd_str]);
    cmd.stdin(std::process::Stdio::piped());
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::piped());
    let mut child = cmd
        .spawn()
        .map_err(|e| BootstrapError::Push(e.to_string()))?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(bytes)
            .await
            .map_err(|e| BootstrapError::Push(format!("write stdin: {e}")))?;
        stdin
            .shutdown()
            .await
            .map_err(|e| BootstrapError::Push(format!("shutdown stdin: {e}")))?;
    }
    let status = tokio::time::timeout(PUSH_TIMEOUT, child.wait())
        .await
        .map_err(|_| BootstrapError::Push("push timed out".into()))?
        .map_err(|e| BootstrapError::Push(e.to_string()))?;
    if !status.success() {
        return Err(BootstrapError::Push(format!(
            "dd exited with {:?}",
            status.code()
        )));
    }
    Ok(())
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

async fn verify_sha_in_env(
    distro: &str,
    path: &str,
    expected_hex: &str,
) -> Result<(), BootstrapError> {
    let out = tokio::time::timeout(
        PUSH_TIMEOUT,
        Command::new("wsl.exe")
            .args(["-d", distro, "--exec", "sha256sum", path])
            .output(),
    )
    .await
    .map_err(|_| BootstrapError::Push("sha256sum timed out".into()))?
    .map_err(|e| BootstrapError::Push(format!("sha256sum spawn: {e}")))?;
    if !out.status.success() {
        return Err(BootstrapError::Push(format!(
            "sha256sum exited with {:?}",
            out.status.code()
        )));
    }
    let text = String::from_utf8_lossy(&out.stdout);
    let actual = text
        .split_whitespace()
        .next()
        .ok_or_else(|| BootstrapError::Push("sha256sum produced empty output".into()))?;
    if !actual.eq_ignore_ascii_case(expected_hex) {
        return Err(BootstrapError::ShaMismatch {
            expected: expected_hex.into(),
            actual: actual.into(),
        });
    }
    Ok(())
}

async fn atomic_install(distro: &str, src: &str, dst: &str) -> Result<(), BootstrapError> {
    let out = tokio::time::timeout(
        PUSH_TIMEOUT,
        Command::new("wsl.exe")
            .args(["-d", distro, "--exec", "mv", "-f", src, dst])
            .output(),
    )
    .await
    .map_err(|_| BootstrapError::Push("mv timed out".into()))?
    .map_err(|e| BootstrapError::Push(format!("mv spawn: {e}")))?;
    if !out.status.success() {
        return Err(BootstrapError::Push(format!(
            "mv exited with {:?}",
            out.status.code()
        )));
    }
    Ok(())
}

async fn chmod_exec(distro: &str, path: &str) -> Result<(), BootstrapError> {
    let out = tokio::time::timeout(
        PUSH_TIMEOUT,
        Command::new("wsl.exe")
            .args(["-d", distro, "--exec", "chmod", "+x", path])
            .output(),
    )
    .await
    .map_err(|_| BootstrapError::Chmod("chmod timed out".into()))?
    .map_err(|e| BootstrapError::Chmod(e.to_string()))?;
    if !out.status.success() {
        return Err(BootstrapError::Chmod(format!(
            "chmod exited with {:?}",
            out.status.code()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn triple_normalization_x86_64_linux() {
        let raw = "x86_64-linux";
        let normalized = match raw {
            "x86_64-linux" => "x86_64-unknown-linux-musl",
            other => other,
        };
        assert_eq!(normalized, "x86_64-unknown-linux-musl");
    }

    #[test]
    fn bootstrap_error_maps_to_dispatch_error() {
        let e: DispatchError = BootstrapError::NoEmbeddedTriple {
            triple: "wasm32-unknown".into(),
        }
        .into();
        assert!(matches!(e, DispatchError::HelperBootstrap(_)));
    }

    #[test]
    fn shell_quote_wraps_plain_value_in_single_quotes() {
        assert_eq!(shell_quote("plain"), "'plain'");
    }

    #[test]
    fn shell_quote_escapes_embedded_single_quote() {
        // POSIX rule: close, escape, reopen — `with'quote` becomes
        // `'with'\''quote'` so the shell sees a single literal value.
        assert_eq!(shell_quote("with'quote"), "'with'\\''quote'");
    }

    #[test]
    fn shell_quote_handles_empty_string() {
        assert_eq!(shell_quote(""), "''");
    }

    #[test]
    fn validate_triple_accepts_real_targets() {
        assert!(validate_triple("x86_64-unknown-linux-musl").is_ok());
        assert!(validate_triple("aarch64-unknown-linux-musl").is_ok());
    }

    #[test]
    fn validate_triple_rejects_path_traversal() {
        let err = validate_triple("../../etc/passwd").unwrap_err();
        assert!(matches!(err, BootstrapError::TripleProbe(_)));
    }

    #[test]
    fn validate_triple_rejects_empty() {
        let err = validate_triple("").unwrap_err();
        assert!(matches!(err, BootstrapError::TripleProbe(_)));
    }

    #[test]
    fn validate_triple_rejects_shell_metacharacters() {
        for bad in ["foo;rm -rf", "foo$bar", "foo`baz`", "foo bar", "foo'bar"] {
            assert!(
                validate_triple(bad).is_err(),
                "triple {bad:?} should have been rejected"
            );
        }
    }
}
