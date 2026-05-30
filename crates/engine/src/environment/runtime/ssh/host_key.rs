//! SSH host-key pinning and TOFU enrollment helpers.

use chrono::{DateTime, Utc};

use crate::environment::runtime::SshHostKeyPin;

/// Normalized host key presented by russh during the transport handshake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresentedHostKey {
    /// SSH key algorithm, for example `ssh-ed25519`.
    pub algorithm: String,
    /// SHA-256 fingerprint string, for example `SHA256:abc123`.
    pub sha256: String,
    /// Full OpenSSH public-key line.
    pub public_key_openssh: String,
}

impl PresentedHostKey {
    /// Convert a presented host key into a persisted inline pin.
    pub fn to_pin(&self, pinned_at: DateTime<Utc>) -> SshHostKeyPin {
        SshHostKeyPin {
            algorithm: self.algorithm.clone(),
            sha256: self.sha256.clone(),
            public_key_openssh: self.public_key_openssh.clone(),
            pinned_at,
        }
    }
}

/// Return `true` when the presented host key matches any trusted pin.
///
/// All three fields must agree — algorithm, fingerprint, and the full
/// OpenSSH public-key line. A mismatch on any field rejects the pin.
pub fn matches_any_pin(presented: &PresentedHostKey, pins: &[SshHostKeyPin]) -> bool {
    pins.iter().any(|pin| {
        pin.algorithm == presented.algorithm
            && pin.sha256 == presented.sha256
            && pin.public_key_openssh == presented.public_key_openssh
    })
}

// ── russh API adapter ────────────────────────────────────────────────────────

/// Build a [`PresentedHostKey`] from the raw key russh hands to
/// [`check_server_key`].
///
/// Uses the exact methods confirmed against the spike output in T1:
/// - `key.algorithm().as_str()` → `"ssh-ed25519"`
/// - `key.fingerprint(HashAlg::default()).to_string()` → `"SHA256:…"`
/// - `key.to_openssh()` → `Result<String>` (ssh-key 0.7 re-export)
// confirm signature against the T1 spike output
fn from_russh_key(
    key: &russh::keys::ssh_key::PublicKey,
) -> Result<PresentedHostKey, russh::keys::ssh_key::Error> {
    let algorithm = key.algorithm().as_str().to_string();
    let sha256 = key
        .fingerprint(russh::keys::ssh_key::HashAlg::default())
        .to_string();
    let public_key_openssh = key.to_openssh()?;
    Ok(PresentedHostKey {
        algorithm,
        sha256,
        public_key_openssh,
    })
}

/// Policy used by [`HostKeyHandler`] when russh presents the server key.
#[derive(Debug, Clone)]
pub enum HostKeyPolicy {
    /// Normal dispatch: presented key must match an existing inline pin.
    Pinned {
        /// Trusted host-key pins to match against.
        pins: Vec<SshHostKeyPin>,
    },
    /// Enrollment: accept exactly one presented key and capture it for
    /// persistence.
    Enroll,
}

/// russh client handler that enforces Ordius host-key policy.
///
/// Pass to [`russh::client::connect`] as the handler argument.
/// After `connect` returns (handshake complete, before auth), retrieve the
/// captured key with [`Self::captured_key`].
pub struct HostKeyHandler {
    policy: HostKeyPolicy,
    captured: std::sync::Arc<tokio::sync::Mutex<Option<PresentedHostKey>>>,
}

impl HostKeyHandler {
    /// Build a handler that accepts only keys matching one of `pins`.
    pub fn pinned(pins: Vec<SshHostKeyPin>) -> Self {
        Self {
            policy: HostKeyPolicy::Pinned { pins },
            captured: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    /// Build a handler that accepts any key and stores it for TOFU enrollment.
    pub fn enroll() -> Self {
        Self {
            policy: HostKeyPolicy::Enroll,
            captured: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    /// Return a shared handle to the captured key (set after `connect` returns
    /// in `Enroll` mode).
    pub fn captured_key(&self) -> std::sync::Arc<tokio::sync::Mutex<Option<PresentedHostKey>>> {
        std::sync::Arc::clone(&self.captured)
    }
}

impl russh::client::Handler for HostKeyHandler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        server_public_key: &russh::keys::ssh_key::PublicKey,
    ) -> Result<bool, Self::Error> {
        let presented =
            from_russh_key(server_public_key).map_err(|_| russh::Error::WrongServerSig)?;
        match &self.policy {
            HostKeyPolicy::Pinned { pins } => Ok(matches_any_pin(&presented, pins)),
            HostKeyPolicy::Enroll => {
                *self.captured.lock().await = Some(presented);
                Ok(true)
            },
        }
    }
}
