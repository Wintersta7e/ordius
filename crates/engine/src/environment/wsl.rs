//! WSL distro enumeration + in-distro probe.
//!
//! The detect-orchestration path that wires `enumerate_wsl_distros` /
//! `enumerate_running_distros` into the namespace fan-out lands in a
//! later phase; until then the public surface is dead from the
//! compiler's perspective. `dead_code` is silenced module-wide so the
//! incremental commits in this series keep the workspace clippy gate
//! green.

#![allow(dead_code)]

use super::types::{
    DiscoveredEndpoint, NamespaceInfo, NamespaceProbeResult, NamespaceState, ReachHint, WslState,
};
use crate::executor::supervisor;
use std::collections::HashSet;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

const ENUM_TIMEOUT: Duration = Duration::from_millis(1500);
const RUNNING_TIMEOUT: Duration = Duration::from_millis(500);
const MAX_LIST_STDOUT: usize = 64 * 1024;

/// POSIX shell script (dash-tested) dispatched into a WSL distro via
/// `wsl.exe -d <name> --exec /bin/sh -c`. Probes the four well-known
/// LLM ports on the distro's loopback and emits a single JSON object.
/// `BusyBox` `curl` is rejected by the version-banner check; `wget`
/// fallback covers `Alpine`-style minimal distros.
pub(super) const STATIC_SCRIPT: &str = r#"set -u
M=
if command -v curl >/dev/null 2>&1 \
   && curl --version 2>/dev/null | head -1 | grep -qi '^curl '; then
  M=curl
elif command -v wget >/dev/null 2>&1; then
  M=wget
fi
if [ -z "$M" ]; then printf '{"error":"no-probe-tool"}\n'; exit 0; fi
probe() {
  code=0
  if [ "$M" = curl ]; then
    if out=$(curl -s -o /dev/null -w "%{http_code}" --max-time 0.5 "$1" 2>/dev/null); then
      case "$out" in
        000) ;;
        [1-5][0-9][0-9]) code=$out ;;
      esac
    fi
  elif wget --spider -q -T 1 -t 1 "$1" 2>/dev/null; then
    code=200
  fi
  printf '%s' "$code"
}
o=$(probe http://127.0.0.1:11434/api/version)
l=$(probe http://127.0.0.1:1234/v1/models)
c=$(probe http://127.0.0.1:8080/v1/models)
x=$(probe http://127.0.0.1:8000/v1/models)
printf '{"ollama":%s,"lm-studio":%s,"llamacpp":%s,"openai-compat":%s}\n' "$o" "$l" "$c" "$x"
"#;

/// One row of `wsl.exe -l --verbose` after filtering and parsing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WslDistroEntry {
    /// Distro name as reported by `wsl.exe` (e.g. `Ubuntu-24.04`).
    pub name: String,
    /// Running / Stopped at the moment the command ran.
    pub state: WslState,
    /// WSL major version (1 or 2). Defaults to 2 on unparseable input.
    pub version: u8,
}

/// `wsl.exe` administrative output (`-l --verbose`, `-l --running
/// --quiet`) varies in encoding by Windows build. Try four orderings:
/// UTF-16 LE without BOM, UTF-16 LE with BOM, UTF-8 with BOM, UTF-8
/// plain.
pub(super) fn decode_wsl_output(bytes: &[u8]) -> String {
    // UTF-16 LE without BOM (measured on Win 11 26200)
    if bytes.len() >= 2 && bytes[1] == 0 && bytes[0] != 0xFE && bytes[0] != 0xFF {
        return decode_utf16_le(bytes);
    }
    // UTF-16 LE with BOM
    if bytes.starts_with(&[0xFF, 0xFE]) {
        return decode_utf16_le(&bytes[2..]);
    }
    // UTF-8 with BOM
    if bytes.starts_with(&[0xEF, 0xBB, 0xBF]) {
        return String::from_utf8_lossy(&bytes[3..]).into_owned();
    }
    // UTF-8 plain (or fallback)
    String::from_utf8_lossy(bytes).into_owned()
}

fn decode_utf16_le(bytes: &[u8]) -> String {
    let u16s: Vec<u16> = bytes
        .chunks_exact(2)
        .map(|c| u16::from_le_bytes([c[0], c[1]]))
        .collect();
    String::from_utf16_lossy(&u16s)
}

/// Parse `wsl.exe -l --verbose` output. Skips header line, filters
/// the docker-desktop family.
pub(super) fn parse_verbose(bytes: &[u8]) -> Vec<WslDistroEntry> {
    let text = decode_wsl_output(bytes);
    let mut out = Vec::new();
    for (i, raw_line) in text.lines().enumerate() {
        let line = raw_line.trim_end_matches('\r').trim();
        if i == 0 || line.is_empty() {
            continue; // header or blank
        }
        let trimmed = line.trim_start_matches('*').trim();
        let cols: Vec<&str> = trimmed.split_whitespace().collect();
        if cols.len() < 3 {
            continue;
        }
        let name = cols[0].to_string();
        if is_filtered(&name) {
            continue;
        }
        let state = match cols[1] {
            "Running" => WslState::Running,
            "Stopped" => WslState::Stopped,
            _ => continue,
        };
        let version: u8 = cols[2].parse().unwrap_or(2);
        out.push(WslDistroEntry {
            name,
            state,
            version,
        });
    }
    out
}

/// Parse `wsl.exe -l --running --quiet`: one distro name per line.
/// Same encoding fallback. Used for the pre-probe race re-check.
pub(super) fn parse_running_quiet(bytes: &[u8]) -> Vec<String> {
    let text = decode_wsl_output(bytes);
    text.lines()
        .map(|l| l.trim_end_matches('\r').trim())
        .filter(|l| !l.is_empty())
        .filter(|l| !is_filtered(l))
        .map(str::to_string)
        .collect()
}

fn is_filtered(name: &str) -> bool {
    matches!(name, "docker-desktop" | "docker-desktop-data")
}

/// Spawn `wsl.exe -l --verbose`, parse the result into entries.
///
/// Returns an empty vec on any failure path: missing `wsl.exe`
/// (e.g. non-Windows hosts), spawn errors, stdout read errors, or
/// the bounded timeout firing. The caller treats all failure modes
/// the same — no WSL distros to probe.
pub async fn enumerate_wsl_distros() -> Vec<WslDistroEntry> {
    let Some(bytes) = run_wsl_list(&["-l", "--verbose"], ENUM_TIMEOUT).await else {
        return Vec::new();
    };
    parse_verbose(&bytes)
}

/// Return the set of currently-running distro names.
///
/// Runs `wsl.exe -l --running --quiet`. Used as the pre-probe race
/// re-check so we don't poke a distro that exited between the
/// verbose enumeration and the probe dispatch. Same failure
/// semantics as [`enumerate_wsl_distros`]: empty set on any error.
pub async fn enumerate_running_distros() -> HashSet<String> {
    let Some(bytes) = run_wsl_list(&["-l", "--running", "--quiet"], RUNNING_TIMEOUT).await else {
        return HashSet::new();
    };
    parse_running_quiet(&bytes).into_iter().collect()
}

async fn run_wsl_list(args: &[&str], budget: Duration) -> Option<Vec<u8>> {
    let mut cmd = Command::new("wsl.exe");
    for a in args {
        cmd.arg(a);
    }
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

    let mut sup = match supervisor::spawn(cmd) {
        Ok(s) => s,
        Err(e) => {
            tracing::debug!(error = %e, "wsl.exe spawn failed");
            return None;
        },
    };

    let mut stdout = sup.child_mut().stdout.take().expect("piped");
    let mut buf = Vec::with_capacity(MAX_LIST_STDOUT);

    let mut limited = (&mut stdout).take(MAX_LIST_STDOUT as u64);
    let read_outcome = tokio::time::timeout(budget, limited.read_to_end(&mut buf)).await;
    let _ = supervisor::cancel(&mut sup).await;

    match read_outcome {
        Ok(Ok(_)) => Some(buf),
        Ok(Err(e)) => {
            tracing::debug!(error = %e, "wsl.exe stdout read failed");
            None
        },
        Err(_) => {
            tracing::warn!("wsl.exe {:?} timed out after {:?}", args, budget);
            None
        },
    }
}

const PROBE_BUDGET: Duration = Duration::from_secs(2);
const MAX_PROBE_STDOUT: usize = 8 * 1024;
const MAX_PROBE_STDERR: usize = 8 * 1024;

/// Probe a single WSL distro by dispatching [`STATIC_SCRIPT`] via
/// `wsl.exe -d <name> --exec /bin/sh -c`. Reads both stdout and
/// stderr with bounded capacity, races a per-probe timeout against
/// the external `cancel` token, then captures the child's exit code
/// via supervised cancel. Maps the outcome to a single
/// [`NamespaceProbeResult`] for the orchestrator to slot into its
/// results map.
///
/// All failure modes (spawn error, read error, non-zero exit, timeout,
/// outer-cancel) collapse to [`NamespaceProbeResult::Unreachable`]
/// with a human-readable reason. A successful exit with parseable
/// JSON returns [`NamespaceProbeResult::Done`].
pub(super) async fn probe_wsl_namespace(
    ns: &NamespaceInfo,
    distro_name: &str,
    cancel: CancellationToken,
) -> NamespaceProbeResult {
    let mut cmd = Command::new("wsl.exe");
    cmd.arg("-d")
        .arg(distro_name)
        .arg("--exec")
        .arg("/bin/sh")
        .arg("-c")
        .arg(STATIC_SCRIPT)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut sup = match supervisor::spawn(cmd) {
        Ok(s) => s,
        Err(e) => {
            return NamespaceProbeResult::Unreachable {
                reason: format!("spawn wsl.exe: {e}"),
            };
        },
    };

    let mut stdout = sup.child_mut().stdout.take().expect("piped");
    let mut stderr = sup.child_mut().stderr.take().expect("piped");
    let mut stdout_buf = Vec::with_capacity(MAX_PROBE_STDOUT);
    let mut stderr_buf = Vec::with_capacity(MAX_PROBE_STDERR);

    let mut stdout_limited = (&mut stdout).take(MAX_PROBE_STDOUT as u64);
    let mut stderr_limited = (&mut stderr).take(MAX_PROBE_STDERR as u64);
    let read_stdout = stdout_limited.read_to_end(&mut stdout_buf);
    let read_stderr = stderr_limited.read_to_end(&mut stderr_buf);
    let read_both = futures::future::try_join(read_stdout, read_stderr);

    let read_outcome: Result<Result<(usize, usize), std::io::Error>, ()> = tokio::select! {
        r = tokio::time::timeout(PROBE_BUDGET, read_both) => match r {
            Ok(Ok(pair)) => Ok(Ok(pair)),
            Ok(Err(e)) => Ok(Err(e)),
            Err(_) => Err(()),
        },
        () = cancel.cancelled() => Err(()),
    };

    let exit_code = supervisor::cancel(&mut sup).await;

    if !stderr_buf.is_empty() {
        tracing::debug!(
            distro = distro_name,
            stderr = %String::from_utf8_lossy(&stderr_buf),
            "wsl probe stderr",
        );
    }

    match (read_outcome, exit_code) {
        (Ok(Ok(_)), Some(0)) => parse_probe_output(ns, distro_name, &stdout_buf),
        (Ok(Ok(_)), Some(code)) => NamespaceProbeResult::Unreachable {
            reason: format!("wsl probe failed (exit {code}); see tracing::debug for stderr"),
        },
        (Ok(Ok(_)), None) => NamespaceProbeResult::Unreachable {
            reason: "wsl probe failed (no exit code)".into(),
        },
        (Ok(Err(e)), _) => NamespaceProbeResult::Unreachable {
            reason: format!("wsl probe stdout read: {e}"),
        },
        (Err(()), _) => NamespaceProbeResult::Unreachable {
            reason: "wsl probe timed out or outer-cancelled".into(),
        },
    }
}

#[derive(serde::Deserialize)]
struct ProbeJson {
    error: Option<String>,
    ollama: Option<u16>,
    #[serde(rename = "lm-studio")]
    lm_studio: Option<u16>,
    llamacpp: Option<u16>,
    #[serde(rename = "openai-compat")]
    openai_compat: Option<u16>,
}

fn parse_probe_output(
    ns: &NamespaceInfo,
    distro_name: &str,
    stdout: &[u8],
) -> NamespaceProbeResult {
    let parsed: ProbeJson = match serde_json::from_slice(stdout) {
        Ok(p) => p,
        Err(e) => {
            return NamespaceProbeResult::Unreachable {
                reason: format!("malformed probe output: {e}"),
            };
        },
    };
    if let Some(err) = parsed.error {
        return NamespaceProbeResult::Done {
            namespace: NamespaceInfo {
                reachable: NamespaceState::NotProbeable { reason: err },
                ..ns.clone()
            },
            endpoints: Vec::new(),
        };
    }
    let mut endpoints = Vec::new();
    for (kind, code, port) in [
        ("ollama", parsed.ollama, 11434_u16),
        ("lm-studio", parsed.lm_studio, 1234),
        ("llamacpp", parsed.llamacpp, 8080),
        ("openai-compat", parsed.openai_compat, 8000),
    ] {
        if matches!(code, Some(c) if (100..600).contains(&c)) {
            let observed = format!("http://127.0.0.1:{port}");
            endpoints.push(DiscoveredEndpoint::OnlyViaNamespace {
                kind: kind.to_string(),
                name: format!(
                    "{kind} ({} via {distro_name})",
                    observed.trim_start_matches("http://"),
                ),
                namespace_id: ns.id.clone(),
                observed_url: observed,
                hint: ReachHint::WslLoopbackBound,
                co_visible_in: Vec::new(),
            });
        }
    }
    NamespaceProbeResult::Done {
        namespace: NamespaceInfo {
            reachable: NamespaceState::Reachable,
            ..ns.clone()
        },
        endpoints,
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::NamespaceKind;
    use super::{
        DiscoveredEndpoint, NamespaceInfo, NamespaceProbeResult, NamespaceState, WslDistroEntry,
        WslState, parse_probe_output, parse_running_quiet, parse_verbose,
    };

    fn utf16_le_no_bom(s: &str) -> Vec<u8> {
        s.encode_utf16().flat_map(u16::to_le_bytes).collect()
    }

    fn utf16_le_with_bom(s: &str) -> Vec<u8> {
        let mut out = vec![0xFF, 0xFE];
        out.extend(s.encode_utf16().flat_map(u16::to_le_bytes));
        out
    }

    fn utf8_with_bom(s: &str) -> Vec<u8> {
        let mut out = vec![0xEF, 0xBB, 0xBF];
        out.extend(s.as_bytes());
        out
    }

    const SAMPLE: &str = "  NAME              STATE           VERSION\r\n\
* Ubuntu            Running         2\r\n\
  docker-desktop    Running         2\r\n\
  Debian            Stopped         2\r\n";

    #[test]
    fn parse_verbose_utf16_le_no_bom() {
        let entries = parse_verbose(&utf16_le_no_bom(SAMPLE));
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "Ubuntu");
        assert_eq!(entries[0].state, WslState::Running);
        assert_eq!(entries[1].name, "Debian");
        assert_eq!(entries[1].state, WslState::Stopped);
    }

    #[test]
    fn parse_verbose_utf16_le_with_bom() {
        let entries = parse_verbose(&utf16_le_with_bom(SAMPLE));
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn parse_verbose_utf8_with_bom() {
        let entries = parse_verbose(&utf8_with_bom(SAMPLE));
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn parse_verbose_utf8_plain() {
        let entries = parse_verbose(SAMPLE.as_bytes());
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn parse_verbose_lf_only() {
        let sample_lf = SAMPLE.replace("\r\n", "\n");
        let entries = parse_verbose(sample_lf.as_bytes());
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn parse_running_quiet_strips_crlf_and_filters() {
        let bytes = utf16_le_no_bom("Ubuntu\r\ndocker-desktop\r\n");
        let names = parse_running_quiet(&bytes);
        assert_eq!(names, vec!["Ubuntu"]);
    }

    #[test]
    fn parse_verbose_empty_input() {
        assert_eq!(parse_verbose(b""), Vec::<WslDistroEntry>::new());
    }

    #[test]
    fn parse_probe_output_emits_only_via_namespace() {
        let ns = NamespaceInfo {
            id: "wsl:Ubuntu".into(),
            label: "WSL: Ubuntu".into(),
            kind: NamespaceKind::WslDistro {
                name: "Ubuntu".into(),
                state: WslState::Running,
            },
            enabled: true,
            reachable: NamespaceState::Reachable,
        };
        let json = br#"{"ollama":200,"lm-studio":0,"llamacpp":200,"openai-compat":0}"#;
        let result = parse_probe_output(&ns, "Ubuntu", json);
        match result {
            NamespaceProbeResult::Done { endpoints, .. } => {
                assert_eq!(endpoints.len(), 2);
                for ep in &endpoints {
                    assert!(matches!(ep, DiscoveredEndpoint::OnlyViaNamespace { .. }));
                }
            },
            _ => panic!("expected Done"),
        }
    }

    #[test]
    fn parse_probe_output_no_probe_tool_is_not_probeable() {
        let ns = NamespaceInfo {
            id: "wsl:Alpine".into(),
            label: "WSL: Alpine".into(),
            kind: NamespaceKind::WslDistro {
                name: "Alpine".into(),
                state: WslState::Running,
            },
            enabled: true,
            reachable: NamespaceState::Reachable,
        };
        let json = br#"{"error":"no-probe-tool"}"#;
        let result = parse_probe_output(&ns, "Alpine", json);
        match result {
            NamespaceProbeResult::Done {
                namespace,
                endpoints,
            } => {
                assert!(matches!(
                    namespace.reachable,
                    NamespaceState::NotProbeable { .. }
                ));
                assert!(endpoints.is_empty());
            },
            _ => panic!("expected Done with NotProbeable"),
        }
    }

    #[test]
    fn parse_probe_output_malformed_is_unreachable() {
        let ns = NamespaceInfo {
            id: "wsl:Garbage".into(),
            label: "WSL: Garbage".into(),
            kind: NamespaceKind::WslDistro {
                name: "Garbage".into(),
                state: WslState::Running,
            },
            enabled: true,
            reachable: NamespaceState::Reachable,
        };
        let result = parse_probe_output(&ns, "Garbage", b"this is not json");
        assert!(matches!(result, NamespaceProbeResult::Unreachable { .. }));
    }
}
