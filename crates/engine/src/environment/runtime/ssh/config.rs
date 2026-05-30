//! SSH connection target and full dispatcher config extraction helpers.
//!
//! [`extract_target`] is the minimal helper used by T5 enrollment.
//! [`extract_dispatcher_config`] is the T7 addition that pulls everything
//! [`SshDispatcher`] / [`RusshConnector`] need from an [`EnvSpec::Ssh`].

use std::sync::Arc;

use crate::environment::runtime::{EnvSpec, SshAuth, SshHostKeyPin};
use crate::secrets::Store;

use super::connection::RusshConnector;

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

/// All fields needed to build a [`RusshConnector`] from an [`EnvSpec::Ssh`].
pub struct SshDispatcherConfig {
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

/// Extract all fields needed by [`SshDispatcher`] from an [`EnvSpec::Ssh`].
///
/// Returns [`ResolveError`] when `spec` is not `EnvSpec::Ssh`.
pub fn extract_dispatcher_config(spec: &EnvSpec) -> Result<SshDispatcherConfig, ResolveError> {
    match spec {
        EnvSpec::Ssh {
            host,
            port,
            user,
            auth,
            host_key_pins,
            ..
        } => Ok(SshDispatcherConfig {
            host: host.clone(),
            port: *port,
            user: user.clone(),
            auth: auth.clone(),
            host_key_pins: host_key_pins.clone(),
        }),
        _ => Err(ResolveError("spec is not an SSH environment".to_string())),
    }
}

/// Build a [`RusshConnector`] directly from an [`EnvSpec::Ssh`] and a secrets
/// store.
///
/// Convenience wrapper for boot-probe and dispatcher construction. Returns
/// [`ResolveError`] when `spec` is not `EnvSpec::Ssh`.
pub fn build_connector(
    env_id: &str,
    spec: &EnvSpec,
    secrets: Arc<Store>,
) -> Result<RusshConnector, ResolveError> {
    let cfg = extract_dispatcher_config(spec)?;
    Ok(RusshConnector {
        env_id: env_id.to_string(),
        host: cfg.host,
        port: cfg.port,
        user: cfg.user,
        auth: cfg.auth,
        host_key_pins: cfg.host_key_pins,
        secrets,
    })
}
