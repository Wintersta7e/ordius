//! Transport seam for workspace file sync. Separate from `ssh::bootstrap::SftpOps`
//! (helper-bootstrap-specific). Object-safe; opened per phase by a factory.

use async_trait::async_trait;

use super::super::error::DispatchError;

// ── Supporting types ──────────────────────────────────────────────────────────

/// Whether a filesystem entry is a regular file, directory, or symbolic link.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    /// Regular file.
    File,
    /// Directory.
    Dir,
    /// Symbolic link (not followed).
    Symlink,
}

/// Lightweight metadata for a single filesystem entry returned by
/// [`WorkspaceTransport::list_tree`] and [`WorkspaceTransport::stat`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMeta {
    /// Path relative to the workspace root passed to the originating call.
    pub rel_path: String,
    /// Whether this entry is a file, directory, or symlink.
    pub kind: FileKind,
    /// Size in bytes. Zero for directories.
    pub size: u64,
    /// Unix permission bits (e.g. `0o644`). Zero when unavailable.
    pub mode: u32,
}

/// Outcome of trying to atomically claim a lock directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockOutcome {
    /// We created the lock dir — caller owns the lock.
    Acquired,
    /// The lock dir already exists as a directory — another run holds it.
    Contended,
}

// ── Factory trait ─────────────────────────────────────────────────────────────

/// Opens a fresh [`WorkspaceTransport`] per reconcile phase.
///
/// A single SFTP/connection session must not be held for an entire run because
/// idle sessions can time out. Callers request a new transport at the start of
/// each sync phase and drop it when done. Object-safe via `Arc<dyn ..>`.
#[async_trait]
pub trait WorkspaceTransportFactory: Send + Sync {
    /// Open a fresh transport session for one reconcile phase.
    async fn open(&self) -> Result<Box<dyn WorkspaceTransport>, DispatchError>;
}

// ── Transport trait ───────────────────────────────────────────────────────────

/// File operations against one environment's filesystem.
///
/// Paths are relative to the env-side workspace root the caller resolved.
/// Implementations need not be `Clone` — the factory reopens as needed.
#[async_trait]
pub trait WorkspaceTransport: Send {
    /// Create a directory (and parents) at `rel`. No-op if it already exists.
    async fn mkdir(&self, rel: &str) -> Result<(), DispatchError>;

    /// Write `bytes` to the file at `rel`, creating or replacing it.
    async fn upload_file(&self, rel: &str, bytes: &[u8]) -> Result<(), DispatchError>;

    /// Read and return the full contents of the file at `rel`.
    async fn download_file(&self, rel: &str) -> Result<Vec<u8>, DispatchError>;

    /// Recursive listing of the entries under `rel` (`""` = root).
    /// Directories and symlinks are included alongside regular files. Whether
    /// `rel` itself appears in the result is implementation-defined (the SFTP
    /// transport lists contents only; the in-memory fake includes the root) —
    /// callers must not rely on its presence or absence.
    async fn list_tree(&self, rel: &str) -> Result<Vec<FileMeta>, DispatchError>;

    /// Return metadata for `rel`, or `None` if the path does not exist.
    /// Does not follow symlinks — a symlink is reported as [`FileKind::Symlink`].
    async fn stat(&self, rel: &str) -> Result<Option<FileMeta>, DispatchError>;

    /// Return the target of the symlink at `rel`.
    async fn read_link(&self, rel: &str) -> Result<String, DispatchError>;

    /// Rename / move `from` to `to` (atomic on the same filesystem).
    async fn rename(&self, from: &str, to: &str) -> Result<(), DispatchError>;

    /// Remove the regular file at `rel`.
    async fn remove_file(&self, rel: &str) -> Result<(), DispatchError>;

    /// Remove the (empty) directory at `rel`.
    async fn remove_dir(&self, rel: &str) -> Result<(), DispatchError>;

    /// Set Unix permission bits on `rel`.
    async fn set_permissions(&self, rel: &str, mode: u32) -> Result<(), DispatchError>;

    /// Atomically create the lock directory at `rel` (a single, exclusive create —
    /// NOT the idempotent mkdir-p). `Acquired` = we created it; `Contended` = it
    /// already exists as a directory. A non-directory entry at `rel`, or an
    /// unrecoverable transport error, returns `Err`.
    async fn try_acquire_lock_dir(&self, rel: &str) -> Result<LockOutcome, DispatchError>;
}

// ── In-memory fake ────────────────────────────────────────────────────────────

/// In-memory `WorkspaceTransport` for unit tests.
///
/// Backed by a shared `Arc<Mutex<..>>` so clones share the same state.
/// `mkdir` records a dir entry; `upload_file` records bytes; `list_tree`
/// returns all entries whose `rel_path` is rooted under the given prefix;
/// `set_permissions` records the mode but is otherwise a no-op;
/// `read_link` returns the stored target for symlinks seeded via
/// [`FakeWorkspaceTransportFactory::seed_symlink`], or `Unsupported` for
/// non-symlink paths. Symlink semantics are no-follow (like real SFTP lstat).
///
/// Gated on `#[cfg(any(test, feature = "testing"))]` to match the existing
/// fake convention in `fake.rs`.
#[cfg(any(test, feature = "testing"))]
mod fake_impl {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use async_trait::async_trait;
    use parking_lot::Mutex;

    use super::{
        DispatchError, FileKind, FileMeta, LockOutcome, WorkspaceTransport,
        WorkspaceTransportFactory,
    };

    #[derive(Debug, Default, Clone)]
    struct FakeFs {
        /// file path → contents
        files: BTreeMap<String, Vec<u8>>,
        /// file path → mode
        modes: BTreeMap<String, u32>,
        /// explicit dir entries (mkdir)
        dirs: BTreeSet<String>,
        /// symlink path → target (no-follow semantics, like real SFTP)
        symlinks: BTreeMap<String, String>,
        /// Test-only error hook: when set, `download_file` returns an error
        /// instead of bytes. Lets a test drive a *write-back* failure
        /// (`list_remote_files` downloads each file and propagates the error)
        /// WITHOUT also breaking `list_tree` — so `remove_tree` (which lists +
        /// removes, never downloads) still works. That isolation is what lets the
        /// preserve-on-failure test prove cleanup was *skipped*, not merely failed.
        /// Toggled via [`FakeWorkspaceTransport::set_fail_download`].
        fail_download: bool,
    }

    /// In-memory fake for [`WorkspaceTransport`]. Clones share state.
    #[derive(Debug, Clone, Default)]
    pub struct FakeWorkspaceTransport {
        inner: Arc<Mutex<FakeFs>>,
    }

    impl FakeWorkspaceTransport {
        /// Test-only error hook: when `fail` is `true`, every subsequent
        /// `download_file` call returns an error instead of bytes. Used to drive a
        /// write-back failure in teardown tests — the write-back paths download
        /// each remote file and propagate the error, while `list_tree`/`stat`/
        /// `remove_*` stay healthy (so `remove_tree` still works and a test can
        /// distinguish "cleanup skipped" from "cleanup failed").
        pub fn set_fail_download(&self, fail: bool) {
            self.inner.lock().fail_download = fail;
        }

        /// Create the dir at `rel`, erroring if ANY entry already exists there
        /// (exclusive — models a single non-idempotent `mkdir`). Called by
        /// `try_acquire_lock_dir`.
        pub(super) fn create_dir_exclusive(&self, rel: &str) -> Result<(), DispatchError> {
            let mut fs = self.inner.lock();
            if fs.dirs.contains(rel) || fs.files.contains_key(rel) || fs.symlinks.contains_key(rel)
            {
                return Err(DispatchError::WorkspaceUnavailable {
                    env_id: "fake".into(),
                    reason: format!("already exists: {rel}"),
                });
            }
            fs.dirs.insert(rel.to_string());
            drop(fs);
            Ok(())
        }
    }

    #[async_trait]
    impl WorkspaceTransport for FakeWorkspaceTransport {
        async fn mkdir(&self, rel: &str) -> Result<(), DispatchError> {
            self.inner.lock().dirs.insert(rel.to_owned());
            Ok(())
        }

        async fn upload_file(&self, rel: &str, bytes: &[u8]) -> Result<(), DispatchError> {
            let mut fs = self.inner.lock();
            // Ensure parent dir entries exist implicitly.
            if let Some(parent) = rel.rsplit_once('/').map(|(p, _)| p) {
                fs.dirs.insert(parent.to_owned());
            }
            fs.files.insert(rel.to_owned(), bytes.to_vec());
            drop(fs);
            Ok(())
        }

        async fn download_file(&self, rel: &str) -> Result<Vec<u8>, DispatchError> {
            let fs = self.inner.lock();
            // Test-only error hook: simulate a transport read failure so a test can
            // drive a write-back error (the write-back paths download each file and
            // propagate the error via `?`) while listing/removal stay healthy.
            if fs.fail_download {
                return Err(DispatchError::WorkspaceUnavailable {
                    env_id: "fake".into(),
                    reason: format!("injected download failure for: {rel}"),
                });
            }
            fs.files
                .get(rel)
                .cloned()
                .ok_or_else(|| DispatchError::WorkspaceUnavailable {
                    env_id: "fake".into(),
                    reason: format!("file not found: {rel}"),
                })
        }

        async fn list_tree(&self, rel: &str) -> Result<Vec<FileMeta>, DispatchError> {
            let prefix = if rel.is_empty() {
                String::new()
            } else {
                format!("{rel}/")
            };

            let mut entries: Vec<FileMeta> = Vec::new();

            {
                let fs = self.inner.lock();
                for path in &fs.dirs {
                    if rel.is_empty() || path == rel || path.starts_with(&prefix) {
                        entries.push(FileMeta {
                            rel_path: path.clone(),
                            kind: FileKind::Dir,
                            size: 0,
                            mode: *fs.modes.get(path).unwrap_or(&0o755),
                        });
                    }
                }
                for (path, data) in &fs.files {
                    if rel.is_empty() || path.starts_with(&prefix) {
                        entries.push(FileMeta {
                            rel_path: path.clone(),
                            kind: FileKind::File,
                            size: data.len() as u64,
                            mode: *fs.modes.get(path).unwrap_or(&0o644),
                        });
                    }
                }
                // Symlinks are emitted as Symlink entries (no-follow, like real SFTP).
                for path in fs.symlinks.keys() {
                    if rel.is_empty() || path.starts_with(&prefix) {
                        entries.push(FileMeta {
                            rel_path: path.clone(),
                            kind: FileKind::Symlink,
                            size: 0,
                            mode: *fs.modes.get(path).unwrap_or(&0o777),
                        });
                    }
                }
            }

            Ok(entries)
        }

        async fn stat(&self, rel: &str) -> Result<Option<FileMeta>, DispatchError> {
            let fs = self.inner.lock();
            // Check symlinks first — no-follow semantics, like real SFTP lstat.
            if fs.symlinks.contains_key(rel) {
                return Ok(Some(FileMeta {
                    rel_path: rel.to_owned(),
                    kind: FileKind::Symlink,
                    size: 0,
                    mode: *fs.modes.get(rel).unwrap_or(&0o777),
                }));
            }
            let result = fs.files.get(rel).map_or_else(
                || {
                    fs.dirs.contains(rel).then(|| FileMeta {
                        rel_path: rel.to_owned(),
                        kind: FileKind::Dir,
                        size: 0,
                        mode: *fs.modes.get(rel).unwrap_or(&0o755),
                    })
                },
                |data| {
                    Some(FileMeta {
                        rel_path: rel.to_owned(),
                        kind: FileKind::File,
                        size: data.len() as u64,
                        mode: *fs.modes.get(rel).unwrap_or(&0o644),
                    })
                },
            );
            drop(fs);
            Ok(result)
        }

        async fn read_link(&self, rel: &str) -> Result<String, DispatchError> {
            self.inner
                .lock()
                .symlinks
                .get(rel)
                .cloned()
                .ok_or_else(|| DispatchError::Unsupported(format!("not a symlink: {rel}")))
        }

        async fn rename(&self, from: &str, to: &str) -> Result<(), DispatchError> {
            let mut fs = self.inner.lock();

            // `from` must name an existing entry, like real `rename(2)`.
            if !fs.files.contains_key(from)
                && !fs.dirs.contains(from)
                && !fs.symlinks.contains_key(from)
            {
                drop(fs);
                return Err(DispatchError::WorkspaceUnavailable {
                    env_id: "fake".into(),
                    reason: format!("rename source not found: {from}"),
                });
            }

            // Renaming a directory moves its whole subtree atomically server-side
            // (POSIX `rename(2)` relinks the inode; children's paths change with
            // it). Reparent `from` and every descendant: `from` → `to`, and any
            // `from/<rest>` → `to/<rest>`, across files, dirs, and symlinks.
            let child_prefix = format!("{from}/");
            let reparent = |k: &str| -> Option<String> {
                if k == from {
                    Some(to.to_owned())
                } else {
                    k.strip_prefix(&child_prefix)
                        .map(|rest| format!("{to}/{rest}"))
                }
            };

            let moved_files: Vec<(String, String)> = fs
                .files
                .keys()
                .filter_map(|k| reparent(k).map(|nk| (k.clone(), nk)))
                .collect();
            for (old, new) in moved_files {
                if let Some(data) = fs.files.remove(&old) {
                    fs.files.insert(new.clone(), data);
                }
                if let Some(mode) = fs.modes.remove(&old) {
                    fs.modes.insert(new, mode);
                }
            }

            let moved_dirs: Vec<(String, String)> = fs
                .dirs
                .iter()
                .filter_map(|k| reparent(k).map(|nk| (k.clone(), nk)))
                .collect();
            for (old, new) in moved_dirs {
                fs.dirs.remove(&old);
                fs.dirs.insert(new.clone());
                if let Some(mode) = fs.modes.remove(&old) {
                    fs.modes.insert(new, mode);
                }
            }

            let moved_links: Vec<(String, String)> = fs
                .symlinks
                .keys()
                .filter_map(|k| reparent(k).map(|nk| (k.clone(), nk)))
                .collect();
            for (old, new) in moved_links {
                if let Some(target) = fs.symlinks.remove(&old) {
                    fs.symlinks.insert(new.clone(), target);
                }
                if let Some(mode) = fs.modes.remove(&old) {
                    fs.modes.insert(new, mode);
                }
            }

            drop(fs);
            Ok(())
        }

        async fn remove_file(&self, rel: &str) -> Result<(), DispatchError> {
            let mut fs = self.inner.lock();
            // A symlink is also removed by remove_file (unlinks the entry, no-follow).
            if fs.symlinks.remove(rel).is_some() {
                fs.modes.remove(rel);
                return Ok(());
            }
            fs.files
                .remove(rel)
                .ok_or_else(|| DispatchError::WorkspaceUnavailable {
                    env_id: "fake".into(),
                    reason: format!("file not found: {rel}"),
                })?;
            fs.modes.remove(rel);
            drop(fs);
            Ok(())
        }

        async fn remove_dir(&self, rel: &str) -> Result<(), DispatchError> {
            let mut fs = self.inner.lock();
            if !fs.dirs.contains(rel) {
                drop(fs);
                return Err(DispatchError::WorkspaceUnavailable {
                    env_id: "fake".into(),
                    reason: format!("dir not found: {rel}"),
                });
            }
            // POSIX ENOTEMPTY: fail if any file, dir, or symlink is a strict child.
            let child_prefix = format!("{rel}/");
            let has_children = fs.files.keys().any(|k| k.starts_with(&child_prefix))
                || fs.dirs.iter().any(|k| k.starts_with(&child_prefix))
                || fs.symlinks.keys().any(|k| k.starts_with(&child_prefix));
            if has_children {
                drop(fs);
                return Err(DispatchError::WorkspaceUnavailable {
                    env_id: "fake".into(),
                    reason: format!("directory not empty: {rel}"),
                });
            }
            fs.dirs.remove(rel);
            fs.modes.remove(rel);
            drop(fs);
            Ok(())
        }

        async fn set_permissions(&self, rel: &str, mode: u32) -> Result<(), DispatchError> {
            self.inner.lock().modes.insert(rel.to_owned(), mode);
            Ok(())
        }

        async fn try_acquire_lock_dir(&self, rel: &str) -> Result<LockOutcome, DispatchError> {
            // Reuse the exclusive create: Ok => we made it; Err(exists) => check kind.
            {
                let fs = self.inner.lock();
                if fs.files.contains_key(rel) || fs.symlinks.contains_key(rel) {
                    return Err(DispatchError::WorkspaceUnavailable {
                        env_id: "fake".into(),
                        reason: format!("lock path exists but is not a directory: {rel}"),
                    });
                }
                if fs.dirs.contains(rel) {
                    return Ok(LockOutcome::Contended);
                }
            }
            self.create_dir_exclusive(rel)?;
            Ok(LockOutcome::Acquired)
        }
    }

    /// Factory that hands out state-sharing clones of one [`FakeWorkspaceTransport`].
    ///
    /// Each `open()` returns a clone backed by the same `Arc<Mutex<FakeFs>>`, so
    /// files written through one transport are visible through the next — the
    /// behaviour a real per-phase reconnect needs for upload-then-teardown tests.
    #[derive(Debug, Clone, Default)]
    pub struct FakeWorkspaceTransportFactory {
        transport: FakeWorkspaceTransport,
    }

    impl FakeWorkspaceTransportFactory {
        /// Wrap an existing transport, letting the caller keep a state-sharing handle.
        #[must_use]
        pub const fn new(transport: FakeWorkspaceTransport) -> Self {
            Self { transport }
        }

        /// Seed a symlink entry in the backing store.
        ///
        /// Records `rel` → `target` as a symlink (no-follow semantics). The
        /// symlink will appear in `list_tree`, `stat` (as `FileKind::Symlink`),
        /// and `read_link` without following the target. `remove_file` unlinks
        /// the entry itself, not the target.
        pub fn seed_symlink(&self, rel: &str, target: &str) {
            self.transport
                .inner
                .lock()
                .symlinks
                .insert(rel.to_owned(), target.to_owned());
        }
    }

    #[async_trait]
    impl WorkspaceTransportFactory for FakeWorkspaceTransportFactory {
        async fn open(&self) -> Result<Box<dyn WorkspaceTransport>, DispatchError> {
            Ok(Box::new(self.transport.clone()))
        }
    }
}

#[cfg(any(test, feature = "testing"))]
pub use fake_impl::{FakeWorkspaceTransport, FakeWorkspaceTransportFactory};

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fake_models_symlinks() {
        let factory = FakeWorkspaceTransportFactory::default();
        // Seed a regular file and a symlink at the same level.
        factory.seed_symlink("root/link.txt", "../target.txt");
        let t = factory.open().await.unwrap();

        // upload a regular file alongside the symlink
        t.upload_file("root/file.txt", b"data").await.unwrap();

        // list_tree includes the symlink as FileKind::Symlink
        let listing = t.list_tree("root").await.unwrap();
        let link_entry = listing
            .iter()
            .find(|m| m.rel_path == "root/link.txt")
            .expect("symlink must appear in list_tree");
        assert_eq!(
            link_entry.kind,
            FileKind::Symlink,
            "list_tree must report Symlink"
        );

        // stat reports Symlink (no-follow)
        let meta = t
            .stat("root/link.txt")
            .await
            .unwrap()
            .expect("stat must return Some");
        assert_eq!(
            meta.kind,
            FileKind::Symlink,
            "stat must report Symlink (no-follow)"
        );

        // read_link returns the stored target
        let target = t.read_link("root/link.txt").await.unwrap();
        assert_eq!(target, "../target.txt");

        // read_link on a non-symlink returns an error
        assert!(
            t.read_link("root/file.txt").await.is_err(),
            "read_link on file must error"
        );

        // remove_file unlinks the symlink entry itself, not the target
        t.remove_file("root/link.txt").await.unwrap();
        assert!(
            t.stat("root/link.txt").await.unwrap().is_none(),
            "symlink must be gone after remove_file"
        );
        // The regular file is untouched
        assert!(t.stat("root/file.txt").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn fake_transport_round_trip() {
        let t = FakeWorkspaceTransport::default();
        t.mkdir("a").await.unwrap();
        t.upload_file("a/f.txt", b"hello").await.unwrap();
        let got = t.download_file("a/f.txt").await.unwrap();
        assert_eq!(got, b"hello");
        let listing = t.list_tree("a").await.unwrap();
        assert!(
            listing
                .iter()
                .any(|m| m.rel_path == "a/f.txt" && m.kind == FileKind::File)
        );
        let md = t.stat("a/f.txt").await.unwrap().unwrap();
        assert_eq!(md.size, 5);
        t.remove_file("a/f.txt").await.unwrap();
        assert!(t.stat("a/f.txt").await.unwrap().is_none());
    }

    // Renaming a directory must move its whole subtree (like real `rename(2)`),
    // not just the directory entry — children's paths move with the parent and
    // the old paths disappear.
    #[tokio::test]
    async fn fake_rename_moves_directory_subtree() {
        let factory = FakeWorkspaceTransportFactory::default();
        factory.seed_symlink("d/link", "../x");
        let t = factory.open().await.unwrap();
        t.mkdir("d").await.unwrap();
        t.mkdir("d/sub").await.unwrap();
        t.upload_file("d/a.txt", b"a").await.unwrap();
        t.upload_file("d/sub/b.txt", b"b").await.unwrap();

        t.rename("d", "d.bak").await.unwrap();

        // The whole subtree moved under the new name.
        assert_eq!(t.download_file("d.bak/a.txt").await.unwrap(), b"a");
        assert_eq!(t.download_file("d.bak/sub/b.txt").await.unwrap(), b"b");
        assert_eq!(
            t.stat("d.bak/sub").await.unwrap().unwrap().kind,
            FileKind::Dir
        );
        assert_eq!(t.read_link("d.bak/link").await.unwrap(), "../x");

        // Nothing remains at the old paths.
        assert!(t.stat("d").await.unwrap().is_none());
        assert!(t.stat("d/a.txt").await.unwrap().is_none());
        assert!(t.stat("d/sub/b.txt").await.unwrap().is_none());
        assert!(t.stat("d/link").await.unwrap().is_none());

        // A missing source still errors.
        assert!(t.rename("nope", "x").await.is_err());
    }

    // `remove_dir` must fail on a non-empty directory (POSIX ENOTEMPTY) so
    // tests that probe for an obstruction don't pass spuriously.
    #[tokio::test]
    async fn fake_remove_dir_fails_on_non_empty() {
        let t = FakeWorkspaceTransport::default();
        t.mkdir("d").await.unwrap();
        t.upload_file("d/child.txt", b"x").await.unwrap();

        // Non-empty: must fail.
        assert!(
            t.remove_dir("d").await.is_err(),
            "remove_dir on non-empty directory must fail"
        );

        // Remove the child, then the now-empty dir succeeds and stat returns None.
        t.remove_file("d/child.txt").await.unwrap();
        t.remove_dir("d").await.unwrap();
        assert!(
            t.stat("d").await.unwrap().is_none(),
            "dir must be gone after removing contents + remove_dir"
        );
    }

    // `create_dir_exclusive` must fail if the path already exists (as dir, file,
    // or symlink) and succeed exactly once on a fresh path.
    #[tokio::test]
    async fn fake_create_dir_exclusive_rejects_existing() {
        let t = FakeWorkspaceTransport::default();

        // First call: succeeds and the dir is visible.
        t.create_dir_exclusive("d").unwrap();
        assert!(
            t.stat("d").await.unwrap().is_some(),
            "dir must exist after create_dir_exclusive"
        );

        // Second call on the same path: must fail.
        assert!(
            t.create_dir_exclusive("d").is_err(),
            "create_dir_exclusive on existing dir must fail"
        );

        // Also fails when a file already occupies the path.
        t.upload_file("f.txt", b"x").await.unwrap();
        assert!(
            t.create_dir_exclusive("f.txt").is_err(),
            "create_dir_exclusive where a file exists must fail"
        );
    }

    // `try_acquire_lock_dir` claims the lock on the first call (`Acquired`),
    // reports `Contended` on a second call against the now-held dir, and errors
    // hard when a non-directory entry already occupies the lock path.
    #[tokio::test]
    async fn fake_try_acquire_lock_dir_acquire_then_contend() {
        let t = FakeWorkspaceTransport::default();

        // First call: we create the lock dir.
        assert_eq!(
            t.try_acquire_lock_dir("r/.ordius.lock").await.unwrap(),
            LockOutcome::Acquired,
            "first acquire must report Acquired"
        );

        // Second call: the dir is held, so we are contended.
        assert_eq!(
            t.try_acquire_lock_dir("r/.ordius.lock").await.unwrap(),
            LockOutcome::Contended,
            "second acquire must report Contended"
        );

        // A non-directory entry at the lock path is a hard error.
        let t2 = FakeWorkspaceTransport::default();
        t2.upload_file("r/.ordius.lock", b"x").await.unwrap();
        assert!(
            t2.try_acquire_lock_dir("r/.ordius.lock").await.is_err(),
            "try_acquire_lock_dir over a file must error"
        );
    }
}
