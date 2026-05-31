//! SSH connection target and full dispatcher config extraction helpers.
//!
//! [`extract_target`] is the minimal helper used by T5 enrollment.
//! [`SshConfig`] pulls everything [`SshDispatcher`] / [`RusshConnector`] need
//! from an [`EnvSpec::Ssh`]; [`SshConfig::from_spec`] is the boot-probe entry
//! point that returns `None` for any non-SSH spec.
//!
//! [`SshDispatcher`]: super::dispatcher::SshDispatcher
//! [`RusshConnector`]: super::connection::RusshConnector

use crate::environment::runtime::{EnvSpec, SshAuth, SshHostKeyPin};

/// Errors that can occur when extracting a connection target.
#[derive(Debug)]
pub struct ResolveError(pub String);

impl std::fmt::Display for ResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SSH target error: {}", self.0)
    }
}

impl std::error::Error for ResolveError {}

/// Extract the `(host, port)` tuple from an SSH `EnvSpec`.
///
/// Returns [`ResolveError`] when `spec` is not `EnvSpec::Ssh`.
/// No DNS resolution is performed here — the caller passes the tuple
/// directly to `russh::client::connect`, which resolves asynchronously.
pub fn extract_target(spec: &EnvSpec) -> Result<(String, u16), ResolveError> {
    match spec {
        EnvSpec::Ssh { host, port, .. } => Ok((host.clone(), *port)),
        _ => Err(ResolveError("spec is not an SSH environment".to_string())),
    }
}

/// All fields [`SshDispatcher`] / [`RusshConnector`] need from an
/// [`EnvSpec::Ssh`].
///
/// [`SshDispatcher`]: super::dispatcher::SshDispatcher
/// [`RusshConnector`]: super::connection::RusshConnector
#[derive(Debug, Clone)]
pub struct SshConfig {
    /// SSH host name or address.
    pub host: String,
    /// SSH TCP port.
    pub port: u16,
    /// SSH user name.
    pub user: String,
    /// Authentication configuration (references secrets by name).
    pub auth: SshAuth,
    /// Trusted inline host-key pins.
    pub host_key_pins: Vec<SshHostKeyPin>,
}

impl SshConfig {
    /// Build an [`SshConfig`] from an [`EnvSpec`], returning `None` for any
    /// variant other than [`EnvSpec::Ssh`].
    #[must_use]
    pub fn from_spec(spec: &EnvSpec) -> Option<Self> {
        let EnvSpec::Ssh {
            host,
            port,
            user,
            auth,
            host_key_pins,
            ..
        } = spec
        else {
            return None;
        };
        Some(Self {
            host: host.clone(),
            port: *port,
            user: user.clone(),
            auth: auth.clone(),
            host_key_pins: host_key_pins.clone(),
        })
    }
}
