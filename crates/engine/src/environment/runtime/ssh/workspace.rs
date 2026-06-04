//! [`WorkspaceTransport`] and [`WorkspaceTransportFactory`] backed by russh SFTP.
//!
//! `SshSftpTransportFactory::open` opens a fresh SFTP channel per reconcile
//! phase so idle sessions don't linger. `SshSftpTransport` wraps the session
//! in an `Arc` (required by `WorkspaceTransport: Send`) and maps every SFTP
//! error to `DispatchError::EnvUnreachable`.

use std::sync::Arc;

use async_trait::async_trait;
use russh_sftp::client::SftpSession;
use russh_sftp::client::error::Error as SftpError;
use russh_sftp::protocol::{FileAttributes, StatusCode};
use tokio::io::AsyncWriteExt as _;

use crate::environment::runtime::error::DispatchError;
use crate::environment::runtime::workspace::safety;
use crate::environment::runtime::workspace::transport::{
    FileKind, FileMeta, WorkspaceTransport, WorkspaceTransportFactory,
};

use super::connection::{RusshConnector, SshConnectionCache};
use super::dispatcher::open_sftp_session;

// ── Error mapping ─────────────────────────────────────────────────────────────

/// `true` when `e` is an SFTP "no such file" status.
fn is_not_found(e: &SftpError) -> bool {
    matches!(
        e,
        SftpError::Status(s) if s.status_code == StatusCode::NoSuchFile
    )
}

/// Map a russh-sftp error to `DispatchError::EnvUnreachable`.
fn sftp_to_dispatch(env_id: &str, op: &str, e: &SftpError) -> DispatchError {
    DispatchError::EnvUnreachable {
        env_id: env_id.to_string(),
        reason: format!("sftp {op}: {e}"),
    }
}

// ── Factory ───────────────────────────────────────────────────────────────────

/// Opens a fresh [`SshSftpTransport`] per reconcile phase.
pub struct SshSftpTransportFactory {
    cache: Arc<SshConnectionCache<RusshConnector>>,
    /// Identifier carried into error messages.
    env_id: String,
}

impl SshSftpTransportFactory {
    /// Build a factory that borrows connections from `cache`.
    ///
    /// `env_id` is included in error messages (e.g. `"ssh:host:port"`).
    pub fn new(cache: Arc<SshConnectionCache<RusshConnector>>, env_id: impl Into<String>) -> Self {
        Self {
            cache,
            env_id: env_id.into(),
        }
    }
}

#[async_trait]
impl WorkspaceTransportFactory for SshSftpTransportFactory {
    async fn open(&self) -> Result<Box<dyn WorkspaceTransport>, DispatchError> {
        let conn = self.cache.connection().await?;
        let session = open_sftp_session(&conn).await?;
        Ok(Box::new(SshSftpTransport {
            session: Arc::new(session),
            env_id: self.env_id.clone(),
        }))
    }
}

// ── Transport ─────────────────────────────────────────────────────────────────

/// SFTP-backed [`WorkspaceTransport`] for one reconcile phase.
pub struct SshSftpTransport {
    session: Arc<SftpSession>,
    env_id: String,
}

impl SshSftpTransport {
    fn err(&self, op: &str, e: &SftpError) -> DispatchError {
        sftp_to_dispatch(&self.env_id, op, e)
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Convert SFTP `FileAttributes` to [`FileMeta`] for a given relative path.
fn attrs_to_meta(rel_path: String, attrs: &FileAttributes) -> FileMeta {
    let ft = attrs.file_type();
    let kind = if ft.is_dir() {
        FileKind::Dir
    } else if ft.is_symlink() {
        FileKind::Symlink
    } else {
        FileKind::File
    };
    FileMeta {
        rel_path,
        kind,
        size: attrs.size.unwrap_or(0),
        mode: attrs.permissions.unwrap_or(0),
    }
}

#[async_trait]
impl WorkspaceTransport for SshSftpTransport {
    async fn mkdir(&self, rel: &str) -> Result<(), DispatchError> {
        // Walk each path component, creating directories that don't yet exist.
        // `create_dir` returns an error if the directory already exists, so we
        // check existence first and only create when absent.  Real errors
        // (permission denied, I/O failure, etc.) are propagated.
        //
        // Preserve a leading `/` so absolute paths round-trip correctly through
        // the SFTP session (which interprets them as server-absolute paths).
        let absolute = rel.starts_with('/');
        let parts: Vec<&str> = rel.split('/').filter(|s| !s.is_empty()).collect();
        let mut so_far = if absolute {
            String::from("/")
        } else {
            String::new()
        };
        for part in &parts {
            if !so_far.is_empty() && !so_far.ends_with('/') {
                so_far.push('/');
            }
            so_far.push_str(part);
            let exists = self
                .session
                .try_exists(&so_far)
                .await
                .map_err(|ref e| self.err("try_exists (mkdir)", e))?;
            if !exists {
                let create_result = self.session.create_dir(so_far.clone()).await;
                if let Err(ref create_err) = create_result {
                    // Race-tolerant: a concurrent run may have created this
                    // component between our check and create_dir; tolerate it
                    // if it's now a directory (Codex R2).
                    match self.session.symlink_metadata(&so_far).await {
                        Ok(attrs) if attrs.file_type().is_dir() => {
                            // Another writer raced us; the directory exists — continue.
                        },
                        _ => return Err(self.err("create_dir (mkdir)", create_err)),
                    }
                }
            }
        }
        Ok(())
    }

    async fn upload_file(&self, rel: &str, bytes: &[u8]) -> Result<(), DispatchError> {
        // Ensure parent directory exists before writing.  SFTP `write` does not
        // create parent dirs, so a missing parent → SSH_FX_NO_SUCH_FILE.
        if let Some(parent) = rel.rsplit_once('/').map(|(p, _)| p) {
            self.mkdir(parent).await?;
        }
        // Atomic write: create/truncate a .tmp path, flush, then rename over
        // the target.  Use `create` (O_CREAT|O_TRUNC|O_WRONLY) rather than
        // `write` (O_WRONLY only) so the file is created when it doesn't yet
        // exist — `write` requires a pre-existing file.
        let tmp = format!("{rel}.ordius.tmp");
        let mut file = self
            .session
            .create(tmp.clone())
            .await
            .map_err(|ref e| self.err("create (tmp)", e))?;
        file.write_all(bytes)
            .await
            .map_err(|e| self.err("write_all", &e.into()))?;
        file.shutdown()
            .await
            .map_err(|e| self.err("flush (tmp)", &e.into()))?;
        self.session
            .rename(tmp, rel)
            .await
            .map_err(|ref e| self.err("rename (upload atomic)", e))?;
        Ok(())
    }

    async fn download_file(&self, rel: &str) -> Result<Vec<u8>, DispatchError> {
        self.session
            .read(rel)
            .await
            .map_err(|ref e| self.err("read", e))
    }

    async fn list_tree(&self, rel: &str) -> Result<Vec<FileMeta>, DispatchError> {
        let mut results = Vec::new();
        list_tree_recursive(&self.session, rel, rel, &self.env_id, &mut results).await?;
        Ok(results)
    }

    async fn stat(&self, rel: &str) -> Result<Option<FileMeta>, DispatchError> {
        match self.session.symlink_metadata(rel).await {
            Ok(attrs) => Ok(Some(attrs_to_meta(rel.to_string(), &attrs))),
            Err(ref e) if is_not_found(e) => Ok(None),
            Err(ref e) => Err(self.err("symlink_metadata", e)),
        }
    }

    async fn read_link(&self, rel: &str) -> Result<String, DispatchError> {
        self.session
            .read_link(rel)
            .await
            .map_err(|ref e| self.err("read_link", e))
    }

    async fn rename(&self, from: &str, to: &str) -> Result<(), DispatchError> {
        self.session
            .rename(from, to)
            .await
            .map_err(|ref e| self.err("rename", e))
    }

    async fn remove_file(&self, rel: &str) -> Result<(), DispatchError> {
        self.session
            .remove_file(rel)
            .await
            .map_err(|ref e| self.err("remove_file", e))
    }

    async fn remove_dir(&self, rel: &str) -> Result<(), DispatchError> {
        self.session
            .remove_dir(rel)
            .await
            .map_err(|ref e| self.err("remove_dir", e))
    }

    async fn set_permissions(&self, rel: &str, mode: u32) -> Result<(), DispatchError> {
        let attrs = FileAttributes {
            permissions: Some(mode),
            ..Default::default()
        };
        self.session
            .set_metadata(rel, attrs)
            .await
            .map_err(|ref e| self.err("set_metadata", e))
    }
}

/// Recursive tree walk: reads `dir` and appends a `FileMeta` for every entry
/// *within* `dir` (the directory itself is not listed), descending into
/// subdirectories without following symlinks.
///
/// `root` is the path passed to the top-level [`WorkspaceTransport::list_tree`]
/// call.  Each entry's `rel_path` is the full remote SFTP path (as returned by
/// `entry.path()`); callers strip the `root/` prefix to obtain the
/// root-relative segment.  We compute the same root-relative segment here to
/// apply the reserved-dir filter before emitting or recursing.
async fn list_tree_recursive(
    session: &SftpSession,
    dir: &str,
    root: &str,
    env_id: &str,
    results: &mut Vec<FileMeta>,
) -> Result<(), DispatchError> {
    let prefix = format!("{root}/");

    let entries = session
        .read_dir(dir)
        .await
        .map_err(|ref e| sftp_to_dispatch(env_id, "read_dir", e))?;

    for entry in entries {
        let path = entry.path();
        let ft = entry.file_type();
        let meta = entry.metadata();

        // Compute the root-relative segment (same stripping callers apply).
        let root_rel = path.strip_prefix(&prefix).unwrap_or(path.as_str());

        // Never descend into / emit reserved trees (.ordius.lock, .git, ...) —
        // the lock dir must never reach a manifest (Codex R2 MAJOR §9.2).
        if safety::is_reserved_remote_rel(root_rel) {
            continue;
        }

        let kind = if ft.is_dir() {
            FileKind::Dir
        } else if ft.is_symlink() {
            FileKind::Symlink
        } else {
            FileKind::File
        };

        results.push(FileMeta {
            rel_path: path.clone(),
            kind,
            size: meta.size.unwrap_or(0),
            mode: meta.permissions.unwrap_or(0),
        });

        // Recurse into directories but not symlinks (don't follow).
        if ft.is_dir() {
            Box::pin(list_tree_recursive(session, &path, root, env_id, results)).await?;
        }
    }

    Ok(())
}
