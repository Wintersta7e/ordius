//! WSL distro enumeration via `wsl.exe -l --verbose`.

use std::collections::HashSet;
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::process::Command;

const ENUM_TIMEOUT: Duration = Duration::from_millis(1500);
const MAX_LIST_STDOUT: usize = 64 * 1024;

/// State of a WSL distribution at enumeration time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WslState {
    /// Distribution is currently running.
    Running,
    /// Distribution is installed but stopped.
    Stopped,
    /// Distribution is in a state `wsl.exe` reported but we do not classify.
    Unknown,
}

/// One parsed row of `wsl.exe -l --verbose`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WslDistro {
    /// Distro name as reported by `wsl.exe` (e.g. `Ubuntu-24.04`).
    pub name: String,
    /// Running / Stopped / Unknown at the moment the command ran.
    pub state: WslState,
    /// WSL major version (1 or 2). Defaults to 2 on unparseable input.
    pub wsl_version: u8,
}

/// Run `wsl.exe -l --verbose` and parse the result.
pub async fn enumerate() -> Vec<WslDistro> {
    let Ok(Ok(output)) = tokio::time::timeout(ENUM_TIMEOUT, run_wsl_list()).await else {
        return Vec::new();
    };
    parse(&output)
}

/// Lookup helper — true if the named distro is currently running.
pub async fn is_running(name: &str) -> bool {
    let running = enumerate_running().await;
    running.contains(name)
}

/// Subset of `enumerate()` that returns only the currently-running distro names.
pub async fn enumerate_running() -> HashSet<String> {
    enumerate()
        .await
        .into_iter()
        .filter(|d| d.state == WslState::Running)
        .map(|d| d.name)
        .collect()
}

async fn run_wsl_list() -> std::io::Result<Vec<u8>> {
    let mut cmd = Command::new("wsl.exe");
    cmd.args(["-l", "--verbose"]);
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::null());
    let mut child = cmd.spawn()?;
    let mut buf = Vec::with_capacity(8192);
    if let Some(out) = child.stdout.take() {
        let _read = out.take(MAX_LIST_STDOUT as u64).read_to_end(&mut buf).await;
    }
    let _status = child.wait().await?;
    Ok(buf)
}

fn parse(bytes: &[u8]) -> Vec<WslDistro> {
    let text = decode_wsl_output(bytes);
    let mut out = Vec::new();
    for (idx, line) in text.lines().enumerate() {
        if idx == 0 {
            continue;
        }
        let trimmed = line.trim_start_matches(['*', ' ']);
        let mut cols = trimmed.split_whitespace();
        let name = match cols.next() {
            Some(n) => n.to_string(),
            None => continue,
        };
        let state_raw = cols.next().unwrap_or("");
        let ver_raw = cols.next().unwrap_or("2");
        let state = match state_raw.to_ascii_lowercase().as_str() {
            "running" => WslState::Running,
            "stopped" => WslState::Stopped,
            _ => WslState::Unknown,
        };
        let wsl_version: u8 = ver_raw.parse().unwrap_or(2);
        if name.is_empty() {
            continue;
        }
        out.push(WslDistro {
            name,
            state,
            wsl_version,
        });
    }
    out
}

fn decode_wsl_output(bytes: &[u8]) -> String {
    if let Some(s) = decode_utf16_le(bytes) {
        return s;
    }
    if let Ok(s) = std::str::from_utf8(bytes) {
        return s.to_string();
    }
    if let Some(s) = decode_utf16_be(bytes) {
        return s;
    }
    String::from_utf8_lossy(bytes).into_owned()
}

fn decode_utf16_le(bytes: &[u8]) -> Option<String> {
    if bytes.len() < 2 {
        return None;
    }
    let units: Vec<u16> = bytes
        .chunks_exact(2)
        .map(|p| u16::from_le_bytes([p[0], p[1]]))
        .collect();
    String::from_utf16(&units)
        .ok()
        .filter(|s| s.contains(|c: char| c.is_ascii_graphic()))
}

fn decode_utf16_be(bytes: &[u8]) -> Option<String> {
    if bytes.len() < 2 {
        return None;
    }
    let units: Vec<u16> = bytes
        .chunks_exact(2)
        .map(|p| u16::from_be_bytes([p[0], p[1]]))
        .collect();
    String::from_utf16(&units)
        .ok()
        .filter(|s| s.contains(|c: char| c.is_ascii_graphic()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utf16_le(s: &str) -> Vec<u8> {
        s.encode_utf16().flat_map(u16::to_le_bytes).collect()
    }

    #[test]
    fn parses_ubuntu_running_v2() {
        let raw = "  NAME            STATE           VERSION\n* Ubuntu          Running         2\n  Debian          Stopped         2\n";
        let bytes = utf16_le(raw);
        let distros = parse(&bytes);
        assert_eq!(distros.len(), 2);
        assert_eq!(distros[0].name, "Ubuntu");
        assert_eq!(distros[0].state, WslState::Running);
        assert_eq!(distros[0].wsl_version, 2);
        assert_eq!(distros[1].name, "Debian");
        assert_eq!(distros[1].state, WslState::Stopped);
    }

    #[test]
    fn unknown_state_does_not_panic() {
        let raw = "  NAME    STATE    VERSION\n  Weird   Hmm      2\n";
        let bytes = utf16_le(raw);
        let distros = parse(&bytes);
        assert_eq!(distros.len(), 1);
        assert_eq!(distros[0].state, WslState::Unknown);
    }

    #[test]
    fn empty_input_yields_empty_vec() {
        assert!(parse(&[]).is_empty());
    }

    #[test]
    fn utf8_input_falls_through_le_check() {
        let raw = "  NAME    STATE     VERSION\n  Plain   Running   2\n";
        let distros = parse(raw.as_bytes());
        assert_eq!(distros.len(), 1);
        assert_eq!(distros[0].name, "Plain");
    }

    #[test]
    fn star_prefix_does_not_become_distro_name() {
        let raw = "  NAME    STATE     VERSION\n* DefaultDistro   Running   2\n";
        let bytes = utf16_le(raw);
        let distros = parse(&bytes);
        assert_eq!(distros.len(), 1);
        assert_eq!(distros[0].name, "DefaultDistro");
    }
}
