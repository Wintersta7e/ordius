//! Host ↔ env path translation for WSL.

use crate::environment::runtime::error::DispatchError;
use crate::environment::runtime::transport::EnvPath;
use std::path::Path;

/// Translate a host-side path to its WSL-side equivalent.
pub fn translate_path(distro: &str, host_path: &Path) -> Result<EnvPath, DispatchError> {
    let host_path_str = host_path.to_string_lossy().into_owned();
    let s = host_path
        .to_str()
        .ok_or_else(|| DispatchError::PathTranslation {
            host_path: host_path_str.clone(),
            reason: "host path not valid UTF-8".into(),
        })?;

    if let Some(drive_path) = strip_drive_prefix(s) {
        return Ok(EnvPath::new(drive_path));
    }

    if let Some((other_distro, rest)) = strip_wsl_prefix(s) {
        if other_distro != distro {
            return Err(DispatchError::PathTranslation {
                host_path: host_path_str,
                reason: format!(
                    "host path references WSL distro `{other_distro}`, but this dispatcher \
                     targets `{distro}`"
                ),
            });
        }
        return Ok(EnvPath::new(rest));
    }

    Err(DispatchError::PathTranslation {
        host_path: host_path_str,
        reason: format!(
            "no inline rule maps host path into WSL distro `{distro}`; \
             consider calling translate_path_via_wslpath for ambiguous cases"
        ),
    })
}

fn strip_drive_prefix(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    if bytes.len() < 3 {
        return None;
    }
    let drive = bytes[0];
    if !(drive.is_ascii_alphabetic() && bytes[1] == b':' && (bytes[2] == b'\\' || bytes[2] == b'/'))
    {
        return None;
    }
    let drive_lower = (drive as char).to_ascii_lowercase();
    let rest = &s[3..].replace('\\', "/");
    Some(format!("/mnt/{drive_lower}/{rest}"))
}

fn strip_wsl_prefix(s: &str) -> Option<(&str, String)> {
    for prefix in ["\\\\wsl$\\", "\\\\wsl.localhost\\"] {
        if let Some(rest) = s.strip_prefix(prefix) {
            let mut parts = rest.splitn(2, '\\');
            let distro = parts.next()?;
            let path = parts.next().unwrap_or("");
            let unix_path = format!("/{}", path.replace('\\', "/"));
            return Some((distro, unix_path));
        }
    }
    None
}

/// Shell out to `wslpath -u` inside the distro. Slow path (~50-200 ms); only
/// invoke when the inline rules cannot resolve the host path.
pub async fn translate_path_via_wslpath(
    distro: &str,
    host_path: &Path,
) -> Result<EnvPath, DispatchError> {
    let host_path_str = host_path.to_string_lossy().into_owned();
    let s = host_path
        .to_str()
        .ok_or_else(|| DispatchError::PathTranslation {
            host_path: host_path_str.clone(),
            reason: "host path not valid UTF-8".into(),
        })?;
    let mut cmd = tokio::process::Command::new("wsl.exe");
    // `--` terminates wslpath's option parsing so a host path that happens to
    // start with `-` is treated as a positional argument, not a flag.
    cmd.args(["-d", distro, "--exec", "wslpath", "-u", "--", s]);
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::null());
    let out = cmd
        .output()
        .await
        .map_err(|e| DispatchError::PathTranslation {
            host_path: host_path_str.clone(),
            reason: format!("wslpath spawn failed: {e}"),
        })?;
    if !out.status.success() {
        return Err(DispatchError::PathTranslation {
            host_path: host_path_str,
            reason: format!("wslpath -u exited with {:?}", out.status.code()),
        });
    }
    let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if text.is_empty() {
        return Err(DispatchError::PathTranslation {
            host_path: host_path_str,
            reason: "wslpath -u produced empty output".into(),
        });
    }
    Ok(EnvPath::new(text))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn drive_letter_translates() {
        let p = PathBuf::from(r"C:\foo\bar");
        let env = translate_path("Ubuntu", &p).unwrap();
        assert_eq!(env.as_str(), "/mnt/c/foo/bar");
    }

    #[test]
    fn drive_letter_lowercase() {
        let p = PathBuf::from(r"E:\Renpy\Game");
        let env = translate_path("Ubuntu", &p).unwrap();
        assert_eq!(env.as_str(), "/mnt/e/Renpy/Game");
    }

    #[test]
    fn wsl_prefix_strips_distro_segment() {
        let p = PathBuf::from(r"\\wsl$\Ubuntu\home\me\code");
        let env = translate_path("Ubuntu", &p).unwrap();
        assert_eq!(env.as_str(), "/home/me/code");
    }

    #[test]
    fn wsl_localhost_alias_strips() {
        let p = PathBuf::from(r"\\wsl.localhost\Ubuntu-24.04\home\me");
        let env = translate_path("Ubuntu-24.04", &p).unwrap();
        assert_eq!(env.as_str(), "/home/me");
    }

    #[test]
    fn wsl_prefix_with_wrong_distro_errors() {
        let p = PathBuf::from(r"\\wsl$\Debian\home\me");
        let err = translate_path("Ubuntu", &p).unwrap_err();
        assert!(matches!(err, DispatchError::PathTranslation { .. }));
    }

    #[test]
    fn unsupported_path_errors() {
        let p = PathBuf::from(r"\\unknown\share\path");
        let err = translate_path("Ubuntu", &p).unwrap_err();
        assert!(format!("{err:?}").contains("no inline rule"));
    }

    #[test]
    fn drive_letter_with_spaces() {
        let p = PathBuf::from(r"C:\Users\my name\file with spaces");
        let env = translate_path("Ubuntu", &p).unwrap();
        assert_eq!(env.as_str(), "/mnt/c/Users/my name/file with spaces");
    }

    #[test]
    fn drive_letter_root_only() {
        let p = PathBuf::from(r"C:\");
        let env = translate_path("Ubuntu", &p).unwrap();
        assert_eq!(env.as_str(), "/mnt/c/");
    }

    #[test]
    fn unix_style_mnt_cifs_path_errors() {
        // `/mnt/cifs/...` is a unix path (no drive letter, no `\\wsl…`
        // prefix), so neither inline rule matches and we surface a
        // structured `PathTranslation` error rather than silently
        // misinterpreting it as a `/mnt/c/...` mapping.
        let p = PathBuf::from("/mnt/cifs/share/file");
        let err = translate_path("Ubuntu", &p).unwrap_err();
        assert!(matches!(err, DispatchError::PathTranslation { .. }));
    }
}
