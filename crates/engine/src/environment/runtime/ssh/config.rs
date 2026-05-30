//! SSH connection target extraction helpers.
//!
//! This module is intentionally minimal for T5. The full `SshConfig`
//! (auth, keep-alive, timeouts) is assembled in T7 once the connection
//! cache is in place. Here we only extract the host/port tuple that
//! enrollment passes directly to `russh::client::connect`, which
//! accepts any `tokio::net::ToSocketAddrs` — async DNS is handled
//! internally by tokio/russh, so no blocking `getaddrinfo` call is needed.

use crate::environment::runtime::EnvSpec;

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
