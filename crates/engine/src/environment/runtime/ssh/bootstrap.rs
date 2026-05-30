//! SFTP bootstrap of embedded `ordius-helper` into a remote home cache.
//!
//! The bootstrap writes the helper bytes to a `.tmp` path, verifies the
//! sha256 via SFTP readback, sets mode 0o755, then atomically renames to the
//! final path under `~/.cache/ordius/helper-<version>-<triple>`.
//!
//! Because SFTP v3 `rename` does NOT overwrite an existing destination, we
//! remove the destination best-effort before renaming (see correction 2).

use async_trait::async_trait;

use crate::environment::runtime::error::DispatchError;

// ── Installed state ───────────────────────────────────────────────────────────

/// Installed helper state for one SSH env.
#[derive(Debug, Clone)]
pub struct SshBootstrappedHelper {
    /// Target triple detected on the remote.
    pub triple: String,
    /// Absolute helper path on the remote host.
    pub env_side_path: String,
}

// ── Helper-bytes source (injectable) ─────────────────────────────────────────

/// A single embedded helper artifact: raw bytes + expected sha256 hex.
#[derive(Debug, Clone)]
pub struct EmbeddedHelperArtifact {
    /// Raw binary content of the helper.
    pub bytes: Vec<u8>,
    /// Hex-encoded expected SHA-256 of `bytes`.
    pub expected_sha256: String,
}

/// Injectable source of embedded helper bytes.
///
/// The production implementation delegates to the build-embedded
/// [`crate::environment::runtime::helper::helper_bytes_for_triple`] +
/// [`crate::environment::runtime::helper::verify_target_integrity`].
///
/// Tests inject fake bytes by providing a `FakeHelperSource`.
pub trait EmbeddedHelperSource: Send + Sync + 'static {
    /// Return the helper bytes for the given triple, or `None` if no
    /// embedded helper is available for that target.
    fn artifact_for(&self, triple: &str) -> Option<EmbeddedHelperArtifact>;
}

/// Production source: reads from the build-embedded manifest.
pub struct ManifestHelperSource;

impl EmbeddedHelperSource for ManifestHelperSource {
    fn artifact_for(&self, triple: &str) -> Option<EmbeddedHelperArtifact> {
        use crate::environment::runtime::helper::{
            helper_bytes_for_triple, verify_target_integrity,
        };
        let target = helper_bytes_for_triple(triple)?;
        if !verify_target_integrity(target) {
            return None; // integrity self-check failed; treat as absent
        }
        Some(EmbeddedHelperArtifact {
            bytes: target.bytes.to_vec(),
            expected_sha256: target.sha256.to_string(),
        })
    }
}

// ── SFTP operations trait ─────────────────────────────────────────────────────

/// Minimal SFTP operations needed by helper bootstrap.
///
/// All methods take `&self` — the production implementation wraps
/// an `Arc<russh_sftp::client::SftpSession>`.
#[async_trait]
pub trait SftpOps: Clone + Send + Sync + 'static {
    /// Resolve `.` on the remote to an absolute path (the home directory).
    /// Returns a `String` — russh-sftp's `canonicalize` returns `String`,
    /// not `PathBuf` (correction 1).
    async fn canonicalize_home(&self) -> Result<String, DispatchError>;

    /// Create (or truncate) the file at `path` and write `bytes` into it.
    async fn write_file(&self, path: &str, bytes: &[u8]) -> Result<(), DispatchError>;

    /// Read the file at `path` via SFTP and return its hex-encoded SHA-256.
    async fn sha256_file(&self, path: &str) -> Result<String, DispatchError>;

    /// Set the Unix permission bits on `path` (e.g. `0o755`).
    async fn chmod(&self, path: &str, mode: u32) -> Result<(), DispatchError>;

    /// Rename `src` to `dst`.
    ///
    /// **Note:** SFTP v3 rename does NOT overwrite an existing destination.
    /// Callers must remove the destination before calling this when it may
    /// already exist (correction 2).
    async fn rename(&self, src: &str, dst: &str) -> Result<(), DispatchError>;

    /// Remove `path` on the remote (best-effort; ignore "not found" errors).
    ///
    /// Used to clear the destination before a rename (correction 2).
    async fn remove_file(&self, path: &str) -> Result<(), DispatchError>;
}

// ── Bootstrapper ─────────────────────────────────────────────────────────────

/// Bootstraps `ordius-helper` into a remote home cache via SFTP.
pub struct SshBootstrapper<S, H = ManifestHelperSource>
where
    S: SftpOps,
    H: EmbeddedHelperSource,
{
    sftp: S,
    helper_source: H,
}

impl<S> SshBootstrapper<S, ManifestHelperSource>
where
    S: SftpOps,
{
    /// Create a bootstrapper backed by the build-embedded helper manifest.
    pub const fn new(sftp: S) -> Self {
        Self {
            sftp,
            helper_source: ManifestHelperSource,
        }
    }
}

impl<S, H> SshBootstrapper<S, H>
where
    S: SftpOps,
    H: EmbeddedHelperSource,
{
    /// Create a bootstrapper with a custom (injectable) helper source.
    ///
    /// Used in tests to inject fake bytes without requiring a real
    /// cross-compiled helper.
    pub const fn with_helper_source(sftp: S, helper_source: H) -> Self {
        Self {
            sftp,
            helper_source,
        }
    }

    /// Bootstrap the helper binary on the remote, returning its installed path.
    ///
    /// Steps:
    /// 1. Look up the embedded helper for `triple`.
    /// 2. Resolve the remote home via `canonicalize(".")`.
    /// 3. Compute cache path: `~/.cache/ordius/helper-<version>-<triple>`.
    /// 4. Write `.tmp`, sha256-verify, chmod 0o755.
    /// 5. Remove destination best-effort (SFTP v3 rename won't overwrite).
    /// 6. Rename `.tmp` → final path.
    pub async fn bootstrap(&self, triple: &str) -> Result<SshBootstrappedHelper, DispatchError> {
        let artifact = self.helper_source.artifact_for(triple).ok_or_else(|| {
            DispatchError::HelperBootstrap(format!("no embedded helper for env triple `{triple}`"))
        })?;

        let home = self.sftp.canonicalize_home().await?;
        let home = home.trim_end_matches('/');
        let base = format!(
            "{}/.cache/ordius/helper-{}-{}",
            home,
            env!("CARGO_PKG_VERSION"),
            triple
        );
        let tmp = format!("{base}.tmp");

        // Write bytes to the .tmp path.
        self.sftp.write_file(&tmp, &artifact.bytes).await?;

        // Verify sha256 via SFTP readback.
        let actual_sha = self.sftp.sha256_file(&tmp).await?;
        if !actual_sha.eq_ignore_ascii_case(&artifact.expected_sha256) {
            return Err(DispatchError::HelperBootstrap(format!(
                "remote helper sha256 mismatch: expected {}, got {}",
                artifact.expected_sha256, actual_sha
            )));
        }

        // Mark executable.
        self.sftp.chmod(&tmp, 0o755).await?;

        // Remove destination best-effort before rename (SFTP v3 won't overwrite).
        drop(self.sftp.remove_file(&base).await);

        // Atomic rename .tmp → final path.
        self.sftp.rename(&tmp, &base).await?;

        Ok(SshBootstrappedHelper {
            triple: triple.to_string(),
            env_side_path: base,
        })
    }
}

// ── Production russh-sftp adapter ────────────────────────────────────────────

/// Production [`SftpOps`] wrapping a `russh_sftp` [`SftpSession`].
///
/// All method signatures are confirmed against the T1 spike output and the
/// russh-sftp 2.3.0 source.
// confirm signature against the T1 spike output
pub use prod::RusshSftp;

mod prod {
    use std::fmt::Write as _;
    use std::sync::Arc;

    use async_trait::async_trait;
    use sha2::{Digest, Sha256};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use russh_sftp::client::SftpSession;
    use russh_sftp::protocol::FileAttributes;

    use super::SftpOps;
    use crate::environment::runtime::error::DispatchError;

    fn sftp_err(e: &russh_sftp::client::error::Error) -> DispatchError {
        DispatchError::HelperBootstrap(format!("sftp error: {e}"))
    }

    /// SFTP operations adapter backed by a live russh-sftp session.
    #[derive(Clone)]
    pub struct RusshSftp {
        session: Arc<SftpSession>,
    }

    impl RusshSftp {
        /// Wrap a live `SftpSession` in the production SFTP ops adapter.
        pub fn new(session: SftpSession) -> Self {
            Self {
                session: Arc::new(session),
            }
        }
    }

    #[async_trait]
    impl SftpOps for RusshSftp {
        // confirm signature against the T1 spike output
        async fn canonicalize_home(&self) -> Result<String, DispatchError> {
            // canonicalize returns String (not PathBuf) — correction 1.
            let home: String = self
                .session
                .canonicalize(".")
                .await
                .map_err(|ref e| sftp_err(e))?;
            Ok(home)
        }

        async fn write_file(&self, path: &str, bytes: &[u8]) -> Result<(), DispatchError> {
            // confirm signature against the T1 spike output
            let mut file = self
                .session
                .create(path)
                .await
                .map_err(|ref e| sftp_err(e))?;
            file.write_all(bytes)
                .await
                .map_err(|e| DispatchError::HelperBootstrap(format!("sftp write error: {e}")))?;
            file.shutdown()
                .await
                .map_err(|e| DispatchError::HelperBootstrap(format!("sftp shutdown error: {e}")))?;
            Ok(())
        }

        async fn sha256_file(&self, path: &str) -> Result<String, DispatchError> {
            // Read back via SFTP and hash on the host side.
            // confirm signature against the T1 spike output
            let mut file = self.session.open(path).await.map_err(|ref e| sftp_err(e))?;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf)
                .await
                .map_err(|e| DispatchError::HelperBootstrap(format!("sftp read error: {e}")))?;
            let digest = Sha256::digest(&buf);
            let mut hex = String::with_capacity(digest.len() * 2);
            for byte in &digest {
                write!(&mut hex, "{:02x}", *byte).unwrap();
            }
            Ok(hex)
        }

        async fn chmod(&self, path: &str, mode: u32) -> Result<(), DispatchError> {
            // Use set_metadata with FileAttributes::empty() + permissions field.
            // confirm signature against the T1 spike output
            let mut attrs = FileAttributes::empty();
            attrs.permissions = Some(mode);
            self.session
                .set_metadata(path, attrs)
                .await
                .map_err(|ref e| sftp_err(e))?;
            Ok(())
        }

        async fn rename(&self, src: &str, dst: &str) -> Result<(), DispatchError> {
            // confirm signature against the T1 spike output
            self.session
                .rename(src, dst)
                .await
                .map_err(|ref e| sftp_err(e))?;
            Ok(())
        }

        async fn remove_file(&self, path: &str) -> Result<(), DispatchError> {
            // confirm signature against the T1 spike output
            self.session
                .remove_file(path)
                .await
                .map_err(|ref e| sftp_err(e))?;
            Ok(())
        }
    }
}

// ── FakeSftp (tests + testing feature) ───────────────────────────────────────

#[cfg(any(test, feature = "testing"))]
pub use fake::FakeSftp;

#[cfg(any(test, feature = "testing"))]
mod fake {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;

    use super::{EmbeddedHelperArtifact, EmbeddedHelperSource, SftpOps, SshBootstrapper};
    use crate::environment::runtime::error::DispatchError;

    /// Inner mutable state for [`FakeSftp`].
    #[derive(Default)]
    struct FakeSftpInner {
        /// Files written: path → bytes.
        writes: HashMap<String, Vec<u8>>,
        /// Chmod calls: path → mode.
        modes: Vec<(String, u32)>,
        /// Rename calls: (src, dst).
        renames: Vec<(String, String)>,
        /// Remove calls: path.
        removes: Vec<String>,
        /// Files written, used for sha256 readback.
        sha_override: Option<String>,
    }

    /// Fake SFTP implementation for unit tests.
    ///
    /// Injects fake helper bytes so the bootstrapper can be exercised without
    /// a real cross-compiled helper present.
    #[derive(Clone)]
    pub struct FakeSftp {
        home: String,
        inner: Arc<Mutex<FakeSftpInner>>,
        /// Per-triple fake helper bytes: triple → (bytes, expected sha256).
        embedded: Arc<Mutex<HashMap<String, EmbeddedHelperArtifact>>>,
    }

    impl FakeSftp {
        /// Create a new fake backed by `home` as the canonicalized home path.
        pub fn new(home: impl Into<String>) -> Self {
            Self {
                home: home.into(),
                inner: Arc::new(Mutex::new(FakeSftpInner::default())),
                embedded: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        /// Override the sha256 that `sha256_file` returns for ALL files.
        ///
        /// By default, `sha256_file` computes the sha256 of the bytes that
        /// were uploaded via `write_file`.  Use this to inject a specific hash
        /// (e.g. when you also inject `with_embedded` with a matching hash).
        #[must_use]
        pub fn with_uploaded_sha(self, sha: impl Into<String>) -> Self {
            self.inner.lock().unwrap().sha_override = Some(sha.into());
            self
        }

        /// Register fake embedded bytes for `triple`.
        ///
        /// The bootstrapper will call `artifact_for(triple)` on the injected
        /// source; registering here makes the [`FakeHelperSource`] return
        /// this entry.
        #[must_use]
        pub fn with_embedded(
            self,
            triple: impl Into<String>,
            bytes: &[u8],
            sha: impl Into<String>,
        ) -> Self {
            self.embedded.lock().unwrap().insert(
                triple.into(),
                EmbeddedHelperArtifact {
                    bytes: bytes.to_vec(),
                    expected_sha256: sha.into(),
                },
            );
            self
        }

        /// Snapshot of all rename calls: `(src, dst)`.
        pub fn renames(&self) -> Vec<(String, String)> {
            self.inner.lock().unwrap().renames.clone()
        }

        /// Snapshot of all chmod calls: `(path, mode)`.
        pub fn modes(&self) -> Vec<(String, u32)> {
            self.inner.lock().unwrap().modes.clone()
        }

        /// Snapshot of all `remove_file` calls.
        pub fn removes(&self) -> Vec<String> {
            self.inner.lock().unwrap().removes.clone()
        }

        /// Build a [`FakeHelperSource`] that returns the embedded bytes
        /// registered via [`with_embedded`].
        pub fn helper_source(&self) -> FakeHelperSource {
            FakeHelperSource {
                embedded: self.embedded.clone(),
            }
        }

        /// Build a bootstrapper wired to this fake SFTP + the fake helper source.
        pub fn bootstrapper(&self) -> SshBootstrapper<Self, FakeHelperSource> {
            SshBootstrapper::with_helper_source(self.clone(), self.helper_source())
        }
    }

    #[async_trait]
    impl SftpOps for FakeSftp {
        async fn canonicalize_home(&self) -> Result<String, DispatchError> {
            Ok(self.home.clone())
        }

        async fn write_file(&self, path: &str, bytes: &[u8]) -> Result<(), DispatchError> {
            self.inner
                .lock()
                .unwrap()
                .writes
                .insert(path.to_string(), bytes.to_vec());
            Ok(())
        }

        async fn sha256_file(&self, _path: &str) -> Result<String, DispatchError> {
            // Extract data under the lock, then drop the guard before any
            // further computation so the lock is held as briefly as possible.
            let (sha_override, written_bytes) = {
                let inner = self.inner.lock().unwrap();
                (inner.sha_override.clone(), inner.writes.get(_path).cloned())
            };
            if let Some(sha) = sha_override {
                return Ok(sha);
            }
            // Compute from the written bytes if available.
            if let Some(bytes) = written_bytes {
                let digest = Sha256::digest(&bytes);
                let mut hex = String::with_capacity(digest.len() * 2);
                for byte in &digest {
                    write!(&mut hex, "{:02x}", *byte).unwrap();
                }
                return Ok(hex);
            }
            Err(DispatchError::HelperBootstrap(format!(
                "fake: no bytes written for path `{_path}`"
            )))
        }

        async fn chmod(&self, path: &str, mode: u32) -> Result<(), DispatchError> {
            self.inner
                .lock()
                .unwrap()
                .modes
                .push((path.to_string(), mode));
            Ok(())
        }

        async fn rename(&self, src: &str, dst: &str) -> Result<(), DispatchError> {
            self.inner
                .lock()
                .unwrap()
                .renames
                .push((src.to_string(), dst.to_string()));
            Ok(())
        }

        async fn remove_file(&self, path: &str) -> Result<(), DispatchError> {
            self.inner.lock().unwrap().removes.push(path.to_string());
            // Always succeeds (simulating best-effort ignore-not-found).
            Ok(())
        }
    }

    /// Fake [`EmbeddedHelperSource`] backed by the fake SFTP's embedded registry.
    pub struct FakeHelperSource {
        embedded: Arc<Mutex<HashMap<String, EmbeddedHelperArtifact>>>,
    }

    impl EmbeddedHelperSource for FakeHelperSource {
        fn artifact_for(&self, triple: &str) -> Option<EmbeddedHelperArtifact> {
            self.embedded.lock().unwrap().get(triple).cloned()
        }
    }
}
