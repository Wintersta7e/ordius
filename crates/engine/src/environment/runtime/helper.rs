//! Embedded `ordius-helper` binaries + constrained shell fallback.
//!
//! Cross-compile builds drop binaries under `crates/engine/embedded/helper/
//! <target-triple>/ordius-helper` (see `build-helpers.sh`); the build script
//! emits the `HELPER_MANIFEST` constant we include below.  If no targets
//! are embedded, the manifest is empty and dispatchers must engage the
//! [`SHELL_FALLBACK_SCRIPT`] runner for probe-only access.
//!
//! ### Bootstrap contract
//! Per spec §3, the engine pushes the helper bytes into the target env's
//! `<env-tmp>/.ordius/helper-<version>-<triple>.tmp`, verifies sha256 inside
//! the env (or reads back via the transport and hashes on host side),
//! renames to the final path, and `chmod +x`'s it.  This module exposes
//! only the *data* + a sha-verifier; the per-env mechanics live in each
//! dispatcher (Phase B: `wsl::bootstrap`).

use sha2::{Digest, Sha256};
use std::fmt::Write as _;

/// Per-target embedded helper artifact.  Bytes lifetime = `'static`.
#[derive(Clone, Copy)]
pub struct HelperTarget {
    /// Target triple, e.g. `"x86_64-unknown-linux-musl"`.
    pub triple: &'static str,
    /// Raw helper binary contents.
    pub bytes: &'static [u8],
    /// Hex-encoded SHA-256 of `bytes`, recomputed at build time.
    pub sha256: &'static str,
    /// Byte length of `bytes`.  Stored explicitly to keep manifest
    /// inspection cheap without re-counting bytes.
    pub size: usize,
}

impl std::fmt::Debug for HelperTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HelperTarget")
            .field("triple", &self.triple)
            .field("size", &self.size)
            .field("sha256", &self.sha256)
            .field("bytes", &format_args!("[{} bytes elided]", self.size))
            .finish()
    }
}

/// Cross-compiled helper bundle embedded in the engine binary.
#[derive(Debug, Clone, Copy)]
pub struct HelperManifest {
    /// Helper version (matches the engine's `Cargo.toml` version).
    pub version: &'static str,
    /// All target binaries present in this build.  Empty in dev builds
    /// where no cross-compile has been run.
    pub targets: &'static [HelperTarget],
}

include!(concat!(env!("OUT_DIR"), "/helper_manifest.rs"));

/// Look up the embedded helper artifact for an env's detected triple.
pub fn helper_bytes_for_triple(triple: &str) -> Option<&'static HelperTarget> {
    HELPER_MANIFEST.targets.iter().find(|t| t.triple == triple)
}

/// Re-hash a target's bytes and confirm the build-time sha256 matches.
/// Cheap (~tens of microseconds for ~3 MB on a modern CPU) but called rarely.
pub fn verify_target_integrity(target: &HelperTarget) -> bool {
    let mut hasher = Sha256::new();
    hasher.update(target.bytes);
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in &digest {
        write!(&mut hex, "{:02x}", *byte).unwrap();
    }
    hex == target.sha256
}

/// Constrained POSIX-sh probe-only runner.  Pushed inline (no helper binary)
/// when bootstrap fails or when no embedded target matches.
///
/// Limits: only handles built-in HTTP resource probes against fixed paths
/// the engine sends in args.  User-defined custom resources surface as
/// `ProbeFailed { reason: "ordius-helper unavailable; custom resources require helper" }`.
///
/// Invocation: `wsl.exe -d <name> --exec /bin/sh -c '<script>' -- <base_url> <path>`.
pub const SHELL_FALLBACK_SCRIPT: &str = r#"set -u
base=$1
path=$2
url=$base$path
if command -v curl >/dev/null 2>&1; then
  code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 1 "$url" 2>/dev/null)
  case "$code" in ''|*[!0-9]*) code=0 ;; esac
  printf '{"status":%d}\n' "$code"
elif command -v wget >/dev/null 2>&1; then
  if wget --spider -q -T 1 -t 1 "$url" 2>/dev/null; then
    printf '{"status":200}\n'
  else
    printf '{"status":0}\n'
  fi
else
  printf '{"error":"no-probe-tool"}\n'
fi
"#;

/// Zero-route liveness shell script. Fires HEAD against `$base/` with a
/// 1-second budget; emits `{"status":<code>}` for any HTTP response or
/// `{"status":0}` on connection failure / timeout.
///
/// Used by [`shell_fallback::probe_http_resource`] when a `ProbeSpec::Http`
/// has no declared routes — the caller only needs proof the port answers.
///
/// Invocation: `wsl.exe -d <name> --exec /bin/sh -c '<script>' -- <base_url>`.
pub const SHELL_FALLBACK_HEAD_SCRIPT: &str = r#"set -u
base=$1
url="${base%/}/"
if command -v curl >/dev/null 2>&1; then
  code=$(curl -s -I -o /dev/null -w "%{http_code}" --connect-timeout 1 --max-time 1 "$url" 2>/dev/null)
  case "$code" in ''|*[!0-9]*) code=0 ;; esac
  printf '{"status":%d}\n' "$code"
elif command -v wget >/dev/null 2>&1; then
  if wget --spider -q -T 1 -t 1 "$url" 2>/dev/null; then
    printf '{"status":200}\n'
  else
    printf '{"status":0}\n'
  fi
else
  printf '{"error":"no-probe-tool"}\n'
fi
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_version_matches_crate() {
        assert_eq!(HELPER_MANIFEST.version, env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn empty_manifest_is_valid() {
        // In dev (no cross-compile run), targets is empty; engine should
        // still build + lookup should return None for any triple.
        if HELPER_MANIFEST.targets.is_empty() {
            assert!(helper_bytes_for_triple("x86_64-unknown-linux-musl").is_none());
        }
    }

    #[test]
    fn embedded_targets_pass_integrity_check() {
        for t in HELPER_MANIFEST.targets {
            assert!(
                verify_target_integrity(t),
                "embedded helper for {} failed sha256 self-check",
                t.triple
            );
        }
    }

    #[test]
    fn shell_fallback_script_uses_argv_positional() {
        assert!(SHELL_FALLBACK_SCRIPT.contains("base=$1"));
        assert!(SHELL_FALLBACK_SCRIPT.contains("path=$2"));
    }

    #[test]
    fn shell_fallback_head_script_takes_single_base_argv() {
        // The HEAD script only needs base — `path` is hard-coded to "/".
        assert!(SHELL_FALLBACK_HEAD_SCRIPT.contains("base=$1"));
        assert!(
            !SHELL_FALLBACK_HEAD_SCRIPT.contains("path=$2"),
            "HEAD liveness script must not consume a second positional"
        );
        assert!(
            SHELL_FALLBACK_HEAD_SCRIPT.contains("curl")
                && SHELL_FALLBACK_HEAD_SCRIPT.contains("-I"),
            "HEAD liveness script must use curl -I"
        );
        assert!(SHELL_FALLBACK_HEAD_SCRIPT.contains("--connect-timeout 1"));
        assert!(SHELL_FALLBACK_HEAD_SCRIPT.contains("--max-time 1"));
        // Same status-coercion contract as the GET script so the engine-side
        // serde_json parser stays uniform.
        assert!(SHELL_FALLBACK_HEAD_SCRIPT.contains("printf '{\"status\":%d}"));
    }

    #[test]
    fn shell_fallback_emits_valid_json_on_curl_paths() {
        // Sanity-check that the shell script's printf format won't produce
        // leading-zero numbers serde_json rejects.  We can't run sh here, but
        // we can assert the script uses %d (which coerces) not %s (which
        // passes the string through verbatim).
        assert!(SHELL_FALLBACK_SCRIPT.contains("printf '{\"status\":%d}"));
        assert!(!SHELL_FALLBACK_SCRIPT.contains("|| echo 000"));
    }

    #[test]
    fn helper_target_debug_elides_bytes() {
        let target = HelperTarget {
            triple: "x86_64-test",
            bytes: &[0u8; 1000],
            sha256: "deadbeef",
            size: 1000,
        };
        let dbg = format!("{target:?}");
        assert!(dbg.contains("1000 bytes elided"));
        assert!(
            !dbg.contains("0, 0, 0, 0"),
            "Debug should not dump raw bytes"
        );
    }
}
