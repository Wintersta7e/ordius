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

    use super::{DispatchError, FileKind, FileMeta, WorkspaceTransport, WorkspaceTransportFactory};

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
    }

    /// In-memory fake for [`WorkspaceTransport`]. Clones share state.
    #[derive(Debug, Clone, Default)]
    pub struct FakeWorkspaceTransport {
        inner: Arc<Mutex<FakeFs>>,
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
            self.inner.lock().files.get(rel).cloned().ok_or_else(|| {
                DispatchError::WorkspaceUnavailable {
                    env_id: "fake".into(),
                    reason: format!("file not found: {rel}"),
                }
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
            if let Some(data) = fs.files.remove(from) {
                fs.files.insert(to.to_owned(), data);
                if let Some(mode) = fs.modes.remove(from) {
                    fs.modes.insert(to.to_owned(), mode);
                }
                drop(fs);
                return Ok(());
            }
            if fs.dirs.remove(from) {
                fs.dirs.insert(to.to_owned());
                drop(fs);
                return Ok(());
            }
            drop(fs);
            Err(DispatchError::WorkspaceUnavailable {
                env_id: "fake".into(),
                reason: format!("rename source not found: {from}"),
            })
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
            if fs.dirs.remove(rel) {
                fs.modes.remove(rel);
                drop(fs);
                Ok(())
            } else {
                drop(fs);
                Err(DispatchError::WorkspaceUnavailable {
                    env_id: "fake".into(),
                    reason: format!("dir not found: {rel}"),
                })
            }
        }

        async fn set_permissions(&self, rel: &str, mode: u32) -> Result<(), DispatchError> {
            self.inner.lock().modes.insert(rel.to_owned(), mode);
            Ok(())
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
}
