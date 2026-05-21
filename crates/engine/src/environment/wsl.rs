//! WSL distro enumeration + in-distro probe.
//!
//! The detect-orchestration path that wires `enumerate_wsl_distros` /
//! `enumerate_running_distros` into the namespace fan-out lands in a
//! later phase; until then the public surface is dead from the
//! compiler's perspective. `dead_code` is silenced module-wide so the
//! incremental commits in this series keep the workspace clippy gate
//! green.

#![allow(dead_code)]

use super::types::WslState;
use crate::executor::supervisor;
use std::collections::HashSet;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::process::Command;

const ENUM_TIMEOUT: Duration = Duration::from_millis(1500);
const RUNNING_TIMEOUT: Duration = Duration::from_millis(500);
const MAX_LIST_STDOUT: usize = 64 * 1024;

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

#[cfg(test)]
mod tests {
    use super::{WslDistroEntry, WslState, parse_running_quiet, parse_verbose};

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
}
