//! Workspace upload safety helpers.
//!
//! Pure, synchronous, no-I/O (except `walk_workspace`, `hash_file`, and
//! `read_within_caps`).
//! Used by the workspace sync manager to validate roots, filter paths,
//! enforce caps, and build per-file manifests before any bytes leave the host.

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

/// Whether `rel` is a safe *relative* path to write into the host workspace.
///
/// Safe = non-empty, with only normal (or `.`) components — no `..`, no
/// absolute root, no drive prefix. Guards write-back against a malicious or
/// compromised remote returning a traversal path that would escape the
/// workspace.
#[must_use]
pub fn is_safe_relative(rel: &str) -> bool {
    use std::path::Component;
    !rel.is_empty()
        && Path::new(rel)
            .components()
            .all(|c| matches!(c, Component::Normal(_) | Component::CurDir))
}

/// Whether writing `host_ws/rel` would traverse an existing symlink or a
/// component that cannot be classified.
///
/// Thin bool view over [`classify_artifact_path`]: `true` only when every
/// existing component of `host_ws/<rel>` is a real (non-symlink) entry that can
/// be `lstat`-ed. A symlink component — which could redirect a write outside
/// the workspace — or a `symlink_metadata` error other than `NotFound` (e.g. a
/// permission error) yields `false`. This **fails closed**: a component we
/// cannot classify is never treated as safe to write through, matching the
/// rules [`classify_artifact_path`] applies during write-back. Missing
/// components are fine; the caller creates them as real directories. Complements
/// [`is_safe_relative`], which only inspects the path string (`..`), not the
/// live host filesystem.
#[must_use]
pub fn host_target_is_symlink_safe(host_ws: &Path, rel: &str) -> bool {
    matches!(classify_artifact_path(host_ws, rel), ArtifactPathState::Ok)
}

/// Encode `s` as a single filesystem-safe path segment (base64url, no pad).
///
/// Run/env ids become path components under `.ordius/diverged/...`. An SSH env
/// id is literally `ssh:<label>` — the `:` is a drive/ADS separator on the
/// Windows host, and other ids may carry `/` or `.`. base64url (`A–Z a–z 0–9
/// _ -`, no `=` padding) maps any input to exactly one safe segment, and the
/// encoding is injective so distinct ids never collide. Round-trips via
/// `URL_SAFE_NO_PAD.decode`.
#[must_use]
pub fn encode_segment(s: &str) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(s.as_bytes())
}

/// Classification of a candidate host artifact path produced by
/// [`classify_artifact_path`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactPathState {
    /// Every existing component is a real (non-symlink) entry — safe to create
    /// any missing components and write under this path.
    Ok,
    /// An existing component is a symlink — writing would traverse it and could
    /// escape the workspace. Reject.
    Symlink,
    /// A component's metadata could not be read for a reason other than "does
    /// not exist" (e.g. a permission error). Fail closed and reject.
    Unreadable,
}

/// Classify whether `host_ws/rel` is safe to create-and-write into, returning
/// a tri-state instead of [`host_target_is_symlink_safe`]'s bool.
///
/// Walks the [`Component::Normal`](std::path::Component::Normal) parts of `rel`
/// onto `host_ws`. For each existing component:
/// - a symlink → [`ArtifactPathState::Symlink`];
/// - a `symlink_metadata` error that is **not** `NotFound` →
///   [`ArtifactPathState::Unreadable`] (fail closed);
/// - `NotFound` → the path doesn't exist yet; the caller creates it as a real
///   dir, so keep walking (deeper components are also `NotFound`).
///
/// Any non-`Normal`/non-`CurDir` component (e.g. `..`, an absolute root) is
/// treated as [`ArtifactPathState::Unreadable`]: callers should reject such
/// `rel`s via [`is_safe_relative`] first, but this fails closed regardless.
///
/// All components clear → [`ArtifactPathState::Ok`].
#[must_use]
pub fn classify_artifact_path(host_ws: &Path, rel: &str) -> ArtifactPathState {
    use std::io::ErrorKind;
    use std::path::Component;

    let mut cur = host_ws.to_path_buf();
    for comp in Path::new(rel).components() {
        match comp {
            Component::Normal(c) => {
                cur.push(c);
                match std::fs::symlink_metadata(&cur) {
                    Ok(md) if md.file_type().is_symlink() => return ArtifactPathState::Symlink,
                    Ok(_) => {},
                    Err(e) if e.kind() == ErrorKind::NotFound => {},
                    Err(_) => return ArtifactPathState::Unreadable,
                }
            },
            Component::CurDir => {},
            // is_safe_relative rejects these; fail closed if one slips through.
            _ => return ArtifactPathState::Unreadable,
        }
    }
    ArtifactPathState::Ok
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

    /// Bytes still allowed before `max_bytes` is exceeded.
    pub const fn remaining_bytes(&self) -> u64 {
        self.caps.max_bytes.saturating_sub(self.total_bytes)
    }
}

/// Read `abs` bounded by the tracker's remaining byte budget, then account the
/// ACTUAL bytes read against `tracker`.
///
/// Enforcing the cap on the bytes actually read (not stale walk metadata)
/// closes a TOCTOU: a file that grew between the walk and the read cannot blow
/// past `max_bytes` or OOM the host — at most `remaining + 1` bytes are read,
/// and `tracker.add` then rejects the file if it exceeds the budget.
pub fn read_within_caps(abs: &Path, tracker: &mut CapTracker) -> Result<Vec<u8>, DispatchError> {
    use std::io::Read as _;

    let limit = tracker.remaining_bytes().saturating_add(1);
    let file = std::fs::File::open(abs).map_err(|e| DispatchError::WorkspaceUnavailable {
        env_id: "<host>".into(),
        reason: format!("open `{}` for upload: {e}", abs.display()),
    })?;
    let mut buf = Vec::new();
    file.take(limit)
        .read_to_end(&mut buf)
        .map_err(|e| DispatchError::WorkspaceUnavailable {
            env_id: "<host>".into(),
            reason: format!("read `{}` for upload: {e}", abs.display()),
        })?;
    tracker.add(buf.len() as u64)?;
    Ok(buf)
}

// ── 4. Workspace walk ─────────────────────────────────────────────────────────

/// Whether a [`WalkEntry`] is a regular file or a directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    /// A regular file.
    File,
    /// A directory.
    Dir,
}

/// A single entry produced by [`walk_workspace`] — a regular file or directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalkEntry {
    /// Forward-slash relative path from the workspace root.
    pub rel_path: String,
    /// Absolute path on the host.
    pub abs: PathBuf,
    /// Whether this entry is a file or a directory.
    pub kind: EntryKind,
    /// File size in bytes. Zero for directories.
    pub size: u64,
    /// Unix permissions bits (e.g. `0o644`).  Always `0o644` on non-Unix.
    pub mode: u32,
}

/// Recursively walk `host_ws`, yielding regular files and directories.
///
/// - Yields a [`EntryKind::Dir`] entry for each non-ignored directory and a
///   [`EntryKind::File`] entry for each regular file. The workspace root itself
///   (rel `""`) is never yielded.
/// - Skips symlinks entirely (no follow, no yield).
///   // TODO(H-later): symlink handling
/// - Skips any path where [`should_ignore`](should_ignore) returns `true`, and
///   does **not** descend into ignored directories.
/// - Returns paths with forward-slash separators relative to `host_ws`, sorted
///   so shallower paths precede the entries nested under them.
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
            let meta =
                std::fs::metadata(&abs).map_err(|e| DispatchError::WorkspaceUnavailable {
                    env_id: "<host>".into(),
                    reason: format!("metadata `{}`: {e}", abs.display()),
                })?;
            // Record the directory itself (root is never an iterated entry here),
            // then descend into it.
            out.push(WalkEntry {
                rel_path: rel,
                abs: abs.clone(),
                kind: EntryKind::Dir,
                size: 0,
                mode: unix_mode(&meta),
            });
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
                kind: EntryKind::File,
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

/// A snapshot of a synced workspace tree: regular files plus the directories
/// that hold them (including empty ones).
///
/// `files` maps forward-slash relative path → [`FileEntry`]; `dirs` is the set
/// of forward-slash relative directory paths (the workspace root is never
/// included). Both are ordered so callers can walk shallow-first.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Manifest {
    /// Forward-slash relative path → per-file metadata.
    pub files: std::collections::BTreeMap<String, FileEntry>,
    /// Forward-slash relative directory paths (root excluded).
    pub dirs: std::collections::BTreeSet<String>,
}

impl Manifest {
    /// An empty manifest.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

/// Lowercase hex SHA-256 of `bytes`.
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();

    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in &digest {
        write!(&mut hex, "{:02x}", *byte).unwrap();
    }
    hex
}

/// Hash the file at `abs` and return a lowercase hex SHA-256 string.
pub fn hash_file(abs: &Path) -> Result<String, DispatchError> {
    let bytes = std::fs::read(abs).map_err(|e| DispatchError::WorkspaceUnavailable {
        env_id: "<host>".into(),
        reason: format!("read `{}` for hashing: {e}", abs.display()),
    })?;
    Ok(sha256_hex(&bytes))
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

    // ── is_safe_relative ──

    #[test]
    fn is_safe_relative_accepts_plain_rejects_traversal() {
        assert!(is_safe_relative("a.txt"));
        assert!(is_safe_relative("sub/b.txt"));
        assert!(is_safe_relative("./a.txt"));
        assert!(!is_safe_relative(""), "empty is unsafe");
        assert!(!is_safe_relative("../escape"), "parent dir is unsafe");
        assert!(!is_safe_relative("a/../../escape"), "embedded .. is unsafe");
        assert!(!is_safe_relative("/etc/passwd"), "absolute is unsafe");
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
        assert!(
            !paths.contains(&".git"),
            ".git dir must be ignored; got {paths:?}"
        );
        // The non-ignored directory is also yielded (as a Dir entry).
        assert!(paths.contains(&"sub"), "missing sub dir; got {paths:?}");
        // Exactly two files + one directory survive the ignore rules.
        assert_eq!(paths.len(), 3, "expected exactly 3 entries; got {paths:?}");
    }

    #[test]
    fn walk_workspace_yields_dirs_excluding_root() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        // Create: a.txt, sub/b.txt, empty/ (an empty directory).
        std::fs::write(root.join("a.txt"), b"hello").unwrap();
        std::fs::create_dir(root.join("sub")).unwrap();
        std::fs::write(root.join("sub").join("b.txt"), b"world").unwrap();
        std::fs::create_dir(root.join("empty")).unwrap();

        let entries = walk_workspace(root).unwrap();

        // The workspace root is never yielded.
        assert!(
            entries.iter().all(|e| !e.rel_path.is_empty()),
            "root (rel_path == \"\") must never be yielded; got {entries:?}"
        );

        let kind_of = |rel: &str| {
            entries.iter().find(|e| e.rel_path == rel).map_or_else(
                || panic!("missing entry {rel}; got {entries:?}"),
                |e| e.kind,
            )
        };

        assert_eq!(kind_of("a.txt"), EntryKind::File, "a.txt is a file");
        assert_eq!(kind_of("sub/b.txt"), EntryKind::File, "sub/b.txt is a file");
        assert_eq!(kind_of("sub"), EntryKind::Dir, "sub is a dir");
        assert_eq!(kind_of("empty"), EntryKind::Dir, "empty is a dir");
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

    // ── read_within_caps ──

    #[test]
    fn read_within_caps_returns_bytes_within_budget() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("ok.bin");
        std::fs::write(&p, b"hello").unwrap();
        let mut t = CapTracker::new(UploadCaps {
            max_bytes: 100,
            max_files: 100,
        });
        let bytes = read_within_caps(&p, &mut t).unwrap();
        assert_eq!(bytes, b"hello");
        assert_eq!(t.total_bytes(), 5);
        assert_eq!(t.total_files(), 1);
    }

    #[test]
    fn read_within_caps_enforces_byte_cap_on_actual_size() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("big.bin");
        // 100 real bytes, cap allows 50 — the file's actual size is enforced,
        // not any earlier walk stat.
        std::fs::write(&p, vec![7u8; 100]).unwrap();
        let mut t = CapTracker::new(UploadCaps {
            max_bytes: 50,
            max_files: 100,
        });
        let err = read_within_caps(&p, &mut t).unwrap_err();
        assert!(err.to_string().contains("max_bytes"), "got: {err}");
    }

    // ── encode_segment ──

    #[test]
    fn encode_segment_is_safe_single_segment() {
        use base64::Engine as _;

        let encoded = encode_segment("ssh:host");

        // Every char must be in the base64url alphabet (no `/`, `.`, `:`, `%`,
        // `+`, `=`) so the result is one filesystem-safe path segment.
        assert!(
            encoded
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-'),
            "encoded segment has unsafe chars: {encoded}"
        );
        assert!(!encoded.is_empty(), "encoded segment must be non-empty");

        // Round-trips back to the original bytes.
        let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(&encoded)
            .unwrap();
        assert_eq!(decoded, b"ssh:host");

        // Distinct inputs produce distinct outputs.
        assert_ne!(encode_segment("a"), encode_segment("b"));
    }

    // ── classify_artifact_path ──

    #[test]
    fn classify_artifact_path_classifies_states() {
        let tmp = tempfile::tempdir().unwrap();
        let host_ws = tmp.path();

        // A fully-missing path is Ok — the caller creates the components.
        assert_eq!(
            classify_artifact_path(host_ws, ".ordius/diverged/x/y/z.txt"),
            ArtifactPathState::Ok,
        );

        // A symlinked component anywhere along the walk is rejected.
        #[cfg(unix)]
        {
            let link_src = host_ws.join("real-target");
            std::fs::create_dir(&link_src).unwrap();
            std::os::unix::fs::symlink(&link_src, host_ws.join(".ordius")).unwrap();
            assert_eq!(
                classify_artifact_path(host_ws, ".ordius/diverged/x"),
                ArtifactPathState::Symlink,
            );
        }

        // A non-Normal/non-CurDir component (e.g. `..`) is treated defensively
        // as Unreadable — callers should have rejected it via is_safe_relative
        // first, but classify_artifact_path fails closed.
        assert_eq!(
            classify_artifact_path(host_ws, "../escape"),
            ArtifactPathState::Unreadable,
        );
    }

    // An Unreadable component requires `symlink_metadata` to fail with a
    // non-NotFound error. On Unix we force this by removing read+execute from a
    // parent directory so the OS denies the lstat of a child. Skipped when run
    // as root (root bypasses the permission bits, so the error never occurs).
    #[cfg(unix)]
    #[test]
    fn classify_artifact_path_reports_unreadable_on_permission_error() {
        use std::os::unix::fs::PermissionsExt as _;

        // unsafe libc::getuid is unavailable; detect root via a sentinel write.
        if running_as_root() {
            eprintln!("skipping: running as root, permission bits are bypassed");
            return;
        }

        let tmp = tempfile::tempdir().unwrap();
        let host_ws = tmp.path();

        // host_ws/blocked exists; strip its read+exec so lstat of a child fails
        // with EACCES (a non-NotFound error) rather than NotFound.
        let blocked = host_ws.join("blocked");
        std::fs::create_dir(&blocked).unwrap();
        std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o000)).unwrap();

        let state = classify_artifact_path(host_ws, "blocked/child");

        // Restore perms so tempdir cleanup can recurse.
        std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o755)).unwrap();

        assert_eq!(state, ArtifactPathState::Unreadable, "got {state:?}");
    }

    // host_target_is_symlink_safe must fail closed on the same EACCES that makes
    // classify_artifact_path report Unreadable: a component we cannot lstat is
    // not safe to write through.
    #[cfg(unix)]
    #[test]
    fn host_target_is_symlink_safe_fails_closed_on_permission_error() {
        use std::os::unix::fs::PermissionsExt as _;

        if running_as_root() {
            eprintln!("skipping: running as root, permission bits are bypassed");
            return;
        }

        let tmp = tempfile::tempdir().unwrap();
        let host_ws = tmp.path();

        let blocked = host_ws.join("blocked");
        std::fs::create_dir(&blocked).unwrap();
        std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o000)).unwrap();

        let safe = host_target_is_symlink_safe(host_ws, "blocked/child");

        // Restore perms so tempdir cleanup can recurse.
        std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o755)).unwrap();

        assert!(!safe, "EACCES component must be treated as unsafe");
    }

    /// True when the test process can lstat inside a 0o000 directory it owns —
    /// the signature of running as root (permission bits are bypassed).
    #[cfg(unix)]
    fn running_as_root() -> bool {
        use std::os::unix::fs::PermissionsExt as _;
        let probe = tempfile::tempdir().unwrap();
        let dir = probe.path().join("p");
        std::fs::create_dir(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o000)).unwrap();
        let can_read = std::fs::symlink_metadata(dir.join("anything")).is_ok()
            || std::fs::read_dir(&dir).is_ok();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        can_read
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
