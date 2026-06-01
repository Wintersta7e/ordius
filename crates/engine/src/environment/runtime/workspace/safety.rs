//! Workspace upload safety helpers.
//!
//! Pure, synchronous, no-I/O (except `walk_workspace` and `hash_file`).
//! Used by the workspace sync manager to validate roots, filter paths,
//! enforce caps, and build per-file manifests before any bytes leave the host.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::environment::runtime::error::DispatchError;

// ── 1. Env-root validation ────────────────────────────────────────────────────

/// Validate that `expanded` is a safe, absolute path to use as an env root.
///
/// Rejects: empty string; path components equal to `..`; any NUL or ASCII
/// control byte; a leading `~`; anything that doesn't start with `/`.
pub fn validate_env_root(expanded: &str) -> Result<(), DispatchError> {
    if expanded.is_empty() {
        return Err(DispatchError::Unsupported(
            "env root must not be empty".into(),
        ));
    }
    if expanded.starts_with('~') {
        return Err(DispatchError::Unsupported(
            "env root must not start with `~`; expand home paths before calling this".into(),
        ));
    }
    if !expanded.starts_with('/') {
        return Err(DispatchError::Unsupported(format!(
            "env root must be an absolute path (got `{expanded}`)"
        )));
    }
    if expanded.bytes().any(|b| b == 0 || b.is_ascii_control()) {
        return Err(DispatchError::Unsupported(
            "env root contains NUL or ASCII control bytes".into(),
        ));
    }
    // Walk path components and reject any bare `..`.
    for component in Path::new(expanded).components() {
        use std::path::Component;
        if matches!(component, Component::ParentDir) {
            return Err(DispatchError::Unsupported(format!(
                "env root contains `..` component: `{expanded}`"
            )));
        }
    }
    Ok(())
}

// ── 2. Ignore rules ───────────────────────────────────────────────────────────

/// Default-ignored path prefixes (checked against the first path segment or
/// multi-segment leading prefix).
const DEFAULT_IGNORED_DIRS: &[&str] = &[".git", ".ordius", "target", "node_modules"];

/// Default-ignored exact filenames.
const DEFAULT_IGNORED_NAMES: &[&str] = &[".env"];

/// Default-ignored file extensions (without the leading `.`).
const DEFAULT_IGNORED_EXTS: &[&str] = &["pem", "key"];

/// Returns `true` when `rel` should be ignored by default.
///
/// Matches:
/// - A path whose first segment (or any leading prefix) is one of the
///   [`DEFAULT_IGNORED_DIRS`] — e.g. `.git` or `.git/HEAD` match, but
///   `mygit/x` does **not**.
/// - A filename that is exactly `.env`.
/// - A filename whose extension is `pem` or `key`.
pub fn is_default_ignored(rel: &str) -> bool {
    let rel = rel.trim_start_matches('/');

    // Check directory-prefix rules: the first path segment must match.
    let first_segment = rel.split('/').next().unwrap_or("");
    if DEFAULT_IGNORED_DIRS.contains(&first_segment) {
        return true;
    }

    // Check filename rules.
    let file_name = rel.split('/').next_back().unwrap_or("");
    if DEFAULT_IGNORED_NAMES.contains(&file_name) {
        return true;
    }
    if file_name
        .rsplit('.')
        .next()
        .is_some_and(|ext| DEFAULT_IGNORED_EXTS.contains(&ext))
    {
        return true;
    }

    false
}

/// Returns `true` when `rel` should be ignored (default rules OR user globs).
///
/// `user_globs` entries are matched as simple glob patterns against the full
/// relative path.  Supports `*` as a wildcard that matches any sequence of
/// non-separator characters within a single path segment.  If the `glob`
/// crate is available as a dep (it is in this workspace), it is used for
/// pattern matching; otherwise a simple hand-rolled fallback applies.
pub fn should_ignore(rel: &str, user_globs: &[String]) -> bool {
    if is_default_ignored(rel) {
        return true;
    }
    for pattern in user_globs {
        if glob_matches(pattern, rel) {
            return true;
        }
    }
    false
}

/// Simple `*`-glob match.  `*` matches any sequence of characters that does
/// **not** contain `/`.  `**` is not supported — use a path-prefix pattern
/// for directory matches.
fn glob_matches(pattern: &str, s: &str) -> bool {
    // Use the `glob` crate if the pattern looks like it needs it; otherwise
    // fall back to a hand-rolled split-on-`*` matcher that avoids the crate's
    // path-parsing overhead for simple patterns.
    if pattern.contains("**") || pattern.contains('[') || pattern.contains('{') {
        // Delegate to the `glob` crate for complex patterns.
        if let Ok(pat) = glob::Pattern::new(pattern) {
            return pat.matches(s);
        }
        return false;
    }

    // Hand-rolled: split pattern on `*`, ensure each literal chunk appears
    // in order and that `*` doesn't span a `/`.
    let parts: Vec<&str> = pattern.splitn(usize::MAX, '*').collect();
    if parts.len() == 1 {
        return pattern == s;
    }

    let mut remaining = s;

    // The first part must be a prefix.
    if !remaining.starts_with(parts[0]) {
        return false;
    }
    remaining = &remaining[parts[0].len()..];

    for (i, chunk) in parts[1..].iter().enumerate() {
        let is_last = i == parts.len() - 2;
        if is_last && chunk.is_empty() {
            // Trailing `*`: matches anything that doesn't cross a segment
            // boundary is already handled — accept.
            return !remaining.contains('/');
        }
        if chunk.is_empty() {
            // Middle `**`-ish empty chunk — just continue.
            continue;
        }
        // Find `chunk` in `remaining`, but the skipped prefix must not contain `/`.
        let search_in = if remaining.contains('/') {
            // Only allow the wildcard to span up to the next `/`.
            remaining.split('/').next().unwrap_or(remaining)
        } else {
            remaining
        };
        if let Some(pos) = search_in.find(*chunk) {
            remaining = &remaining[pos + chunk.len()..];
        } else {
            return false;
        }
    }

    remaining.is_empty() || parts.last() == Some(&"")
}

// ── 3. Upload caps ────────────────────────────────────────────────────────────

/// Hard caps applied during a workspace upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadCaps {
    /// Maximum total bytes across all uploaded files.
    pub max_bytes: u64,
    /// Maximum number of files.
    pub max_files: usize,
}

impl Default for UploadCaps {
    fn default() -> Self {
        Self {
            max_bytes: 512 * 1024 * 1024, // 512 MiB
            max_files: 50_000,
        }
    }
}

/// Running accumulator that enforces [`UploadCaps`].
///
/// Call [`CapTracker::add`] once per file; it returns an error as soon as
/// either cap is exceeded.
#[derive(Debug)]
pub struct CapTracker {
    caps: UploadCaps,
    total_bytes: u64,
    total_files: usize,
}

impl CapTracker {
    /// Create a new tracker with the given caps.
    pub const fn new(caps: UploadCaps) -> Self {
        Self {
            caps,
            total_bytes: 0,
            total_files: 0,
        }
    }

    /// Record one file of `bytes` size.  Returns `Err` if either cap is
    /// exceeded after this addition.
    pub fn add(&mut self, bytes: u64) -> Result<(), DispatchError> {
        self.total_files += 1;
        self.total_bytes += bytes;

        if self.total_files > self.caps.max_files {
            return Err(DispatchError::Unsupported(format!(
                "workspace upload exceeds max_files cap ({} > {})",
                self.total_files, self.caps.max_files
            )));
        }
        if self.total_bytes > self.caps.max_bytes {
            return Err(DispatchError::Unsupported(format!(
                "workspace upload exceeds max_bytes cap ({} > {} bytes)",
                self.total_bytes, self.caps.max_bytes
            )));
        }
        Ok(())
    }

    /// Total bytes accumulated so far.
    pub const fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Total file count accumulated so far.
    pub const fn total_files(&self) -> usize {
        self.total_files
    }
}

// ── 4. Workspace walk ─────────────────────────────────────────────────────────

/// A single file entry produced by [`walk_workspace`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalkEntry {
    /// Forward-slash relative path from the workspace root.
    pub rel_path: String,
    /// Absolute path on the host.
    pub abs: PathBuf,
    /// File size in bytes.
    pub size: u64,
    /// Unix permissions bits (e.g. `0o644`).  Always `0o644` on non-Unix.
    pub mode: u32,
}

/// Recursively walk `host_ws`, yielding regular files only.
///
/// - Skips symlinks entirely (no follow, no yield).
///   // TODO(H-later): symlink handling
/// - Skips any path where [`should_ignore`](should_ignore) returns `true`, and
///   does **not** descend into ignored directories.
/// - Returns paths with forward-slash separators relative to `host_ws`.
pub fn walk_workspace(host_ws: &Path) -> Result<Vec<WalkEntry>, DispatchError> {
    let mut entries = Vec::new();
    walk_dir_recursive(host_ws, host_ws, &mut entries)?;
    entries.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
    Ok(entries)
}

fn walk_dir_recursive(
    root: &Path,
    dir: &Path,
    out: &mut Vec<WalkEntry>,
) -> Result<(), DispatchError> {
    let read_dir = std::fs::read_dir(dir).map_err(|e| DispatchError::WorkspaceUnavailable {
        env_id: "<host>".into(),
        reason: format!("read_dir `{}`: {e}", dir.display()),
    })?;

    for entry in read_dir {
        let entry = entry.map_err(|e| DispatchError::WorkspaceUnavailable {
            env_id: "<host>".into(),
            reason: format!("dir entry error in `{}`: {e}", dir.display()),
        })?;

        let abs = entry.path();
        let ft = entry
            .file_type()
            .map_err(|e| DispatchError::WorkspaceUnavailable {
                env_id: "<host>".into(),
                reason: format!("file_type `{}`: {e}", abs.display()),
            })?;

        // TODO(H-later): symlink handling
        if ft.is_symlink() {
            continue;
        }

        // Build rel_path with forward slashes.
        let rel_os = abs
            .strip_prefix(root)
            .map_err(|_| DispatchError::WorkspaceUnavailable {
                env_id: "<host>".into(),
                reason: format!(
                    "path `{}` is unexpectedly outside root `{}`",
                    abs.display(),
                    root.display()
                ),
            })?;
        let rel = rel_os
            .components()
            .map(|c| c.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/");

        // Apply ignore rules before deciding whether to descend or yield.
        if should_ignore(&rel, &[]) {
            continue;
        }

        if ft.is_dir() {
            walk_dir_recursive(root, &abs, out)?;
        } else if ft.is_file() {
            let meta =
                std::fs::metadata(&abs).map_err(|e| DispatchError::WorkspaceUnavailable {
                    env_id: "<host>".into(),
                    reason: format!("metadata `{}`: {e}", abs.display()),
                })?;

            let size = meta.len();
            let mode = unix_mode(&meta);

            out.push(WalkEntry {
                rel_path: rel,
                abs,
                size,
                mode,
            });
        }
    }

    Ok(())
}

#[cfg(unix)]
fn unix_mode(meta: &std::fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    meta.permissions().mode()
}

#[cfg(not(unix))]
fn unix_mode(_meta: &std::fs::Metadata) -> u32 {
    0o644
}

// ── 5. Manifest ───────────────────────────────────────────────────────────────

/// Per-file manifest entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEntry {
    /// Lowercase hex SHA-256 of the file contents.
    pub sha256_hex: String,
    /// File size in bytes.
    pub size: u64,
    /// Unix permission bits.
    pub mode: u32,
}

/// Manifest: maps forward-slash relative path → [`FileEntry`].
pub type Manifest = HashMap<String, FileEntry>;

/// Hash the file at `abs` and return a lowercase hex SHA-256 string.
pub fn hash_file(abs: &Path) -> Result<String, DispatchError> {
    let bytes = std::fs::read(abs).map_err(|e| DispatchError::WorkspaceUnavailable {
        env_id: "<host>".into(),
        reason: format!("read `{}` for hashing: {e}", abs.display()),
    })?;

    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    let digest = hasher.finalize();

    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in &digest {
        write!(&mut hex, "{:02x}", *byte).unwrap();
    }
    Ok(hex)
}

/// Hash every entry in `entries` and return a [`Manifest`].
pub fn build_manifest(host_ws: &Path, entries: &[WalkEntry]) -> Result<Manifest, DispatchError> {
    let _ = host_ws; // root provided for context; abs paths in WalkEntry are self-contained
    let mut manifest = Manifest::new();
    for entry in entries {
        let sha256_hex = hash_file(&entry.abs)?;
        manifest.insert(
            entry.rel_path.clone(),
            FileEntry {
                sha256_hex,
                size: entry.size,
                mode: entry.mode,
            },
        );
    }
    Ok(manifest)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    // ── validate_env_root ──

    #[test]
    fn validate_env_root_rejects_empty() {
        assert!(validate_env_root("").is_err());
    }

    #[test]
    fn validate_env_root_rejects_dotdot() {
        let err = validate_env_root("/a/../b").unwrap_err();
        assert!(err.to_string().contains(".."), "got: {err}");
    }

    #[test]
    fn validate_env_root_rejects_tilde() {
        assert!(validate_env_root("~/x").is_err());
    }

    #[test]
    fn validate_env_root_rejects_control_byte() {
        let s = "/tmp/ordi\x01us";
        assert!(validate_env_root(s).is_err());
    }

    #[test]
    fn validate_env_root_rejects_nul() {
        let s = "/tmp/ordi\x00us";
        assert!(validate_env_root(s).is_err());
    }

    #[test]
    fn validate_env_root_accepts_valid() {
        assert!(validate_env_root("/tmp/ordius-abc").is_ok());
    }

    // ── is_default_ignored ──

    #[test]
    fn ignored_git_dir() {
        assert!(is_default_ignored(".git/HEAD"));
    }

    #[test]
    fn ignored_bare_git() {
        assert!(is_default_ignored(".git"));
    }

    #[test]
    fn ignored_dotenv() {
        assert!(is_default_ignored(".env"));
    }

    #[test]
    fn ignored_pem() {
        assert!(is_default_ignored("x/y.pem"));
    }

    #[test]
    fn ignored_node_modules() {
        assert!(is_default_ignored("node_modules/p/i.js"));
    }

    #[test]
    fn not_ignored_src() {
        assert!(!is_default_ignored("src/main.rs"));
    }

    #[test]
    fn not_ignored_mygit() {
        // `mygit` starts with the prefix `mygit`, not `.git` — must not match.
        assert!(!is_default_ignored("mygit/x"));
    }

    // ── walk_workspace ──

    #[test]
    fn walk_skips_ignored_yields_rest() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        // Create: a.txt, sub/b.txt, .git/HEAD, .env
        std::fs::write(root.join("a.txt"), b"hello").unwrap();
        std::fs::create_dir(root.join("sub")).unwrap();
        std::fs::write(root.join("sub").join("b.txt"), b"world").unwrap();
        std::fs::create_dir(root.join(".git")).unwrap();
        std::fs::write(root.join(".git").join("HEAD"), b"ref: refs/heads/main").unwrap();
        std::fs::write(root.join(".env"), b"SECRET=x").unwrap();

        let entries = walk_workspace(root).unwrap();
        let paths: Vec<&str> = entries.iter().map(|e| e.rel_path.as_str()).collect();

        assert!(paths.contains(&"a.txt"), "missing a.txt; got {paths:?}");
        assert!(
            paths.contains(&"sub/b.txt"),
            "missing sub/b.txt; got {paths:?}"
        );
        assert!(
            !paths.contains(&".git/HEAD"),
            ".git/HEAD must be ignored; got {paths:?}"
        );
        assert!(
            !paths.contains(&".env"),
            ".env must be ignored; got {paths:?}"
        );
        assert_eq!(paths.len(), 2, "expected exactly 2 entries; got {paths:?}");
    }

    // ── CapTracker ──

    #[test]
    fn cap_tracker_rejects_over_max_files() {
        let caps = UploadCaps {
            max_bytes: u64::MAX,
            max_files: 2,
        };
        let mut t = CapTracker::new(caps);
        t.add(1).unwrap();
        t.add(1).unwrap();
        let err = t.add(1).unwrap_err();
        assert!(err.to_string().contains("max_files"), "got: {err}");
    }

    #[test]
    fn cap_tracker_rejects_over_max_bytes() {
        let caps = UploadCaps {
            max_bytes: 10,
            max_files: usize::MAX,
        };
        let mut t = CapTracker::new(caps);
        t.add(8).unwrap();
        let err = t.add(4).unwrap_err();
        assert!(err.to_string().contains("max_bytes"), "got: {err}");
    }

    // ── hash_file ──

    #[test]
    fn hash_file_stable_and_correct() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("data.bin");
        // SHA-256 of empty input is well-known.
        std::fs::File::create(&p).unwrap();
        let hex = hash_file(&p).unwrap();
        assert_eq!(
            hex,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );

        // Known bytes: SHA-256("abc") per NIST.
        let mut f = std::fs::OpenOptions::new().write(true).open(&p).unwrap();
        f.write_all(b"abc").unwrap();
        drop(f);
        let hex2 = hash_file(&p).unwrap();
        assert_eq!(
            hex2,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
