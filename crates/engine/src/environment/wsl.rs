//! WSL distro enumeration + in-distro probe.
//!
//! Parser-only in this module checkpoint; the subprocess-spawning
//! callers land in the next commit and discharge the `dead_code`
//! allow.

#![allow(dead_code)]

use super::types::WslState;

/// One row of `wsl.exe -l --verbose` after filtering and parsing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct WslDistroEntry {
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
