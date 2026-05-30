//! SSH authentication helpers.

use std::path::PathBuf;

use thiserror::Error;

use crate::environment::runtime::{SecretRef, SshAuth};
use crate::secrets::Store;

/// Resolved auth material. Secret values must not be logged.
#[derive(Clone, PartialEq, Eq)]
pub enum ResolvedSshAuth {
    /// Password auth value.
    Password(String),
    /// Private key path plus optional passphrase.
    KeyFile {
        /// Host-side private key path.
        path: PathBuf,
        /// Optional private-key passphrase.
        passphrase: Option<String>,
    },
}

impl std::fmt::Debug for ResolvedSshAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Password(_) => f.debug_tuple("Password").field(&"<redacted>").finish(),
            Self::KeyFile { path, passphrase } => f
                .debug_struct("KeyFile")
                .field("path", path)
                .field("passphrase", &passphrase.as_ref().map(|_| "<redacted>"))
                .finish(),
        }
    }
}

/// Auth resolution failure.
#[derive(Debug, Error)]
pub enum SshAuthError {
    /// Secret store lookup failed.
    #[error("secret `{name}`: {reason}")]
    Secret {
        /// The secret name or path that failed.
        name: String,
        /// Human-readable reason for the failure.
        reason: String,
    },
    /// Private key file could not be loaded (missing, bad format, wrong passphrase).
    #[error("key file `{path}`: {reason}")]
    KeyLoad {
        /// Path to the key file.
        path: String,
        /// Human-readable reason for the failure.
        reason: String,
    },
    /// The russh transport returned an error during the auth exchange.
    #[error("auth transport error: {0}")]
    Transport(String),
    /// Authentication was rejected by the server.
    #[error("authentication rejected by server: remaining_methods={remaining}, partial={partial}")]
    Rejected {
        /// Remaining allowed methods as reported by the server.
        remaining: String,
        /// Whether a partial success was indicated.
        partial: bool,
    },
    /// Deferred auth method was requested.
    #[error("SSH agent auth is deferred")]
    AgentDeferred,
}

fn secret(store: &Store, secret_ref: &SecretRef) -> Result<String, SshAuthError> {
    store
        .get(secret_ref.0.as_str())
        .map_err(|e| SshAuthError::Secret {
            name: secret_ref.0.clone(),
            reason: e.to_string(),
        })
}

/// Resolve persisted SSH auth config into concrete, non-logged material.
pub fn resolve_auth_material(
    store: &Store,
    auth: &SshAuth,
) -> Result<ResolvedSshAuth, SshAuthError> {
    match auth {
        SshAuth::Password { secret_ref } => {
            Ok(ResolvedSshAuth::Password(secret(store, secret_ref)?))
        },
        SshAuth::KeyFile {
            path,
            passphrase_ref,
        } => {
            let passphrase = passphrase_ref
                .as_ref()
                .map(|r| secret(store, r))
                .transpose()?;
            Ok(ResolvedSshAuth::KeyFile {
                path: PathBuf::from(path),
                passphrase,
            })
        },
        SshAuth::Agent { .. } => Err(SshAuthError::AgentDeferred),
    }
}

/// Authenticate an established russh session.
///
/// Uses the exact auth calls confirmed against the T1 spike output.
/// Secret values are consumed by value and never logged.
pub async fn authenticate_session<H>(
    session: &mut russh::client::Handle<H>,
    user: &str,
    auth: ResolvedSshAuth,
) -> Result<(), SshAuthError>
where
    H: russh::client::Handler,
{
    match auth {
        ResolvedSshAuth::Password(password) => {
            // confirm signature against the T1 spike output
            let result = session
                .authenticate_password(user, password)
                .await
                .map_err(|e| SshAuthError::Transport(e.to_string()))?;
            require_auth_success(result)
        },
        ResolvedSshAuth::KeyFile { path, passphrase } => {
            // confirm signature against the T1 spike output
            // load_secret_key: fn(&Path, Option<&str>) -> Result<PrivateKey, keys::Error>
            let private_key =
                russh::keys::load_secret_key(&path, passphrase.as_deref()).map_err(|e| {
                    SshAuthError::KeyLoad {
                        path: path.display().to_string(),
                        reason: e.to_string(),
                    }
                })?;
            // PrivateKeyWithHashAlg::new(Arc<PrivateKey>, Option<HashAlg>) — None is fine for
            // Ed25519/ECDSA (hash_alg is ignored); for RSA None → sha-rsa (SHA-1).
            let key =
                russh::keys::PrivateKeyWithHashAlg::new(std::sync::Arc::new(private_key), None);
            // authenticate_publickey: takes key BY VALUE (not _with / external signer)
            let result = session
                .authenticate_publickey(user, key)
                .await
                .map_err(|e| SshAuthError::Transport(e.to_string()))?;
            require_auth_success(result)
        },
    }
}

/// Map `AuthResult::Success` to `Ok(())`; any other variant is an auth
/// failure. `russh::client::AuthResult` has variants `Success` and `Failure { .. }`.
fn require_auth_success(result: russh::client::AuthResult) -> Result<(), SshAuthError> {
    match result {
        russh::client::AuthResult::Success => Ok(()),
        russh::client::AuthResult::Failure {
            remaining_methods,
            partial_success,
        } => Err(SshAuthError::Rejected {
            remaining: format!("{remaining_methods:?}"),
            partial: partial_success,
        }),
    }
}
