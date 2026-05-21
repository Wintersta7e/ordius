//! User-managed namespace overrides + custom-host validation.
//!
//! Backed by the `namespace_overrides` table (schema v2). Two surfaces:
//!
//! - [`validate_host`]: gate-keep arbitrary user input before it lands
//!   in the DB. Rejects URL syntax (scheme / path / credentials /
//!   query / fragment), control characters, the empty string, and the
//!   literal `localhost` (any case) since the Local namespace already
//!   covers loopback.
//! - CRUD helpers (added in a later task): `list`, `add_custom`,
//!   `remove_custom`, `set_enabled`.

use crate::environment::NamespaceHost;
use thiserror::Error;

/// All failure modes surfaced by this module.
#[derive(Debug, Error)]
pub enum NamespacesError {
    /// Host string failed validation (URL syntax, control chars,
    /// localhost, empty, or `url::Host::parse` rejected it).
    #[error("invalid host: {0}")]
    InvalidHost(String),
    /// Lookup or delete target id is not in the overrides table.
    #[error("not found: {0}")]
    NotFound(String),
    /// `rusqlite` error from the underlying query.
    #[error("db: {0}")]
    Db(#[from] rusqlite::Error),
    /// `r2d2` error acquiring a pooled connection.
    #[error("pool: {0}")]
    Pool(#[from] r2d2::Error),
}

/// Validate a user-supplied host string for a Custom namespace.
///
/// On success, returns a [`NamespaceHost`] wrapping a canonicalized
/// [`url::Host`] (IPv4 / IPv6 / domain). On failure, returns
/// [`NamespacesError::InvalidHost`] with a short reason.
///
/// # Errors
/// - Empty input.
/// - Contains URL syntax: `://`, `/`, `@`, `?`, `#`, or any control char.
/// - `url::Host::parse` rejects the input (malformed IPv6, etc.).
/// - The canonical form equals `localhost` (case-insensitive) — the
///   Local namespace already represents loopback.
pub fn validate_host(input: &str) -> Result<NamespaceHost, NamespacesError> {
    if input.is_empty() {
        return Err(NamespacesError::InvalidHost("empty".into()));
    }
    if input.contains("://")
        || input.contains('/')
        || input.contains('@')
        || input.contains('?')
        || input.contains('#')
        || input.chars().any(char::is_control)
    {
        return Err(NamespacesError::InvalidHost("contains URL syntax".into()));
    }
    let host = url::Host::parse(input).map_err(|e| NamespacesError::InvalidHost(e.to_string()))?;
    if let url::Host::Domain(ref d) = host
        && d.eq_ignore_ascii_case("localhost")
    {
        return Err(NamespacesError::InvalidHost(
            "use the local namespace instead".into(),
        ));
    }
    Ok(NamespaceHost(host))
}

#[cfg(test)]
mod tests {
    use super::{NamespacesError, validate_host};

    #[test]
    fn validate_accepts_ipv4() {
        assert!(validate_host("192.0.2.1").is_ok());
    }

    #[test]
    fn validate_accepts_ipv6_bracketed() {
        assert!(validate_host("[2001:db8::1]").is_ok());
    }

    #[test]
    fn validate_accepts_domain() {
        assert!(validate_host("gpu-box.example.com").is_ok());
    }

    #[test]
    fn validate_rejects_localhost() {
        assert!(matches!(
            validate_host("localhost"),
            Err(NamespacesError::InvalidHost(_))
        ));
        assert!(matches!(
            validate_host("LOCALHOST"),
            Err(NamespacesError::InvalidHost(_))
        ));
        assert!(matches!(
            validate_host("LocalHost"),
            Err(NamespacesError::InvalidHost(_))
        ));
    }

    #[test]
    fn validate_rejects_scheme() {
        assert!(matches!(
            validate_host("http://example.com"),
            Err(NamespacesError::InvalidHost(_))
        ));
    }

    #[test]
    fn validate_rejects_path() {
        assert!(matches!(
            validate_host("example.com/api"),
            Err(NamespacesError::InvalidHost(_))
        ));
    }

    #[test]
    fn validate_rejects_credentials() {
        assert!(matches!(
            validate_host("user@example.com"),
            Err(NamespacesError::InvalidHost(_))
        ));
    }

    #[test]
    fn validate_rejects_query_fragment() {
        assert!(matches!(
            validate_host("example.com?q=1"),
            Err(NamespacesError::InvalidHost(_))
        ));
        assert!(matches!(
            validate_host("example.com#frag"),
            Err(NamespacesError::InvalidHost(_))
        ));
    }

    #[test]
    fn validate_rejects_control_chars() {
        assert!(matches!(
            validate_host("ex\nample.com"),
            Err(NamespacesError::InvalidHost(_))
        ));
        assert!(matches!(
            validate_host("ex\tample.com"),
            Err(NamespacesError::InvalidHost(_))
        ));
    }

    #[test]
    fn validate_rejects_empty() {
        assert!(matches!(
            validate_host(""),
            Err(NamespacesError::InvalidHost(_))
        ));
    }
}
