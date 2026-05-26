//! Search-path expansion shared by `LocalDispatcher::which_with_fallback`.
//!
//! Inputs are AgentDeck-shaped patterns like `~/.nvm/versions/node/*/bin`.
//! `~` is replaced by the current user's home directory; `*` segments are
//! expanded with the `glob` crate. Plain paths pass through unchanged.

use std::path::PathBuf;

/// Expand a slice of search-path patterns into concrete directories.
///
/// `home` is the path used to replace a leading `~/` (or a bare `~`).
/// Glob patterns are expanded; failed expansions are silently dropped.
///
/// Order is preserved: earlier entries in `patterns` come first in the
/// output. Within a single glob pattern, results are sorted by `glob`'s
/// default (alphabetic), which keeps probe behaviour deterministic across
/// runs (so two `.nvm` node versions produce the same priority each boot).
pub fn expand(patterns: &[String], home: &std::path::Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for raw in patterns {
        let expanded = expand_tilde(raw, home);
        if expanded.contains('*') || expanded.contains('?') || expanded.contains('[') {
            // Glob expansion. Errors (invalid pattern, unreadable directory)
            // drop the pattern silently — a probe miss is preferable to a
            // boot failure when one of half-a-dozen optional install dirs
            // doesn't exist.
            if let Ok(iter) = glob::glob(&expanded) {
                let mut matches: Vec<PathBuf> = iter.filter_map(Result::ok).collect();
                matches.sort();
                out.extend(matches);
            }
        } else {
            out.push(PathBuf::from(expanded));
        }
    }
    out
}

/// Replace a leading `~` (alone or as `~/...`) with `home`. Other tildes
/// — including `~user` — are passed through unchanged because we don't
/// resolve other users' home directories.
fn expand_tilde(raw: &str, home: &std::path::Path) -> String {
    if raw == "~" {
        return home.display().to_string();
    }
    if let Some(rest) = raw.strip_prefix("~/") {
        return home.join(rest).display().to_string();
    }
    raw.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn plain_paths_pass_through() {
        let home = TempDir::new().unwrap();
        let out = expand(
            &["/usr/local/bin".into(), "/opt/homebrew/bin".into()],
            home.path(),
        );
        assert_eq!(out.len(), 2);
        assert_eq!(out[0], PathBuf::from("/usr/local/bin"));
        assert_eq!(out[1], PathBuf::from("/opt/homebrew/bin"));
    }

    #[test]
    fn tilde_expands_to_home() {
        let home = TempDir::new().unwrap();
        let out = expand(&["~/.cargo/bin".into()], home.path());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0], home.path().join(".cargo/bin"));
    }

    #[test]
    fn bare_tilde_expands_to_home() {
        let home = TempDir::new().unwrap();
        let out = expand(&["~".into()], home.path());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0], PathBuf::from(home.path().display().to_string()));
    }

    #[test]
    fn glob_finds_concrete_subdirectories() {
        let home = TempDir::new().unwrap();
        // Layout: <home>/.nvm/versions/node/v18.20.0/bin, .../v20.10.0/bin
        for v in ["v18.20.0", "v20.10.0"] {
            fs::create_dir_all(home.path().join(".nvm/versions/node").join(v).join("bin")).unwrap();
        }
        let out = expand(&["~/.nvm/versions/node/*/bin".into()], home.path());
        assert_eq!(out.len(), 2);
        // Sorted alphabetically — v18 before v20.
        assert!(out[0].ends_with("v18.20.0/bin"));
        assert!(out[1].ends_with("v20.10.0/bin"));
    }

    #[test]
    fn glob_with_no_match_silently_drops() {
        let home = TempDir::new().unwrap();
        let out = expand(&["~/.does-not-exist/*/bin".into()], home.path());
        assert!(out.is_empty());
    }

    #[test]
    fn invalid_glob_silently_drops() {
        // Unterminated bracket — `glob::Pattern::new` rejects it.
        let home = TempDir::new().unwrap();
        let out = expand(&["~/[".into()], home.path());
        assert!(out.is_empty());
    }
}
