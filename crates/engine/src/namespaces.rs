//! User-managed namespace overrides + custom-host validation.
//!
//! Backed by the `namespace_overrides` table (schema v2). Two surfaces:
//!
//! - [`validate_host`]: gate-keep arbitrary user input before it lands
//!   in the DB. Rejects URL syntax (scheme / path / credentials /
//!   query / fragment), control characters, the empty string, and the
//!   literal `localhost` (any case) since the Local namespace already
//!   covers loopback.
//! - CRUD: [`list`], [`add_custom`], [`remove_custom`], [`set_enabled`].

use crate::db::DbPool;
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

/// One row from the `namespace_overrides` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceOverride {
    /// Namespace id: `local`, `wsl:<distro>`, or `custom:<slug>`.
    pub namespace_id: String,
    /// User toggle. Disabled namespaces skip probing and surface as
    /// [`NamespaceState::Disabled`](crate::environment::NamespaceState::Disabled)
    /// in the report.
    pub enabled: bool,
    /// Human-readable label for the namespace. Only populated for
    /// `custom:` rows; `NULL` (here `None`) for `local`/`wsl:*`.
    pub custom_label: Option<String>,
    /// Original user-supplied host (e.g. `192.0.2.10`, `gpu-box.lan`).
    /// Only populated for `custom:` rows.
    pub custom_host: Option<String>,
    /// Insertion epoch (seconds).
    pub created_at: i64,
    /// Last-update epoch (seconds).
    pub updated_at: i64,
}

/// Load every row in `namespace_overrides`, ordered by id.
///
/// Returns an empty Vec when no overrides exist. Used by
/// `environment::detect` to apply the `enabled` flag to WSL/Local
/// namespaces and to materialize Custom namespaces.
///
/// # Errors
/// Pool acquisition or `SELECT` failure.
pub fn list(pool: &DbPool) -> Result<Vec<NamespaceOverride>, NamespacesError> {
    let conn = pool.get()?;
    let mut stmt = conn.prepare(
        "SELECT namespace_id, enabled, custom_label, custom_host, created_at, updated_at
         FROM namespace_overrides ORDER BY namespace_id",
    )?;
    let rows = stmt
        .query_map([], |r| {
            Ok(NamespaceOverride {
                namespace_id: r.get(0)?,
                enabled: r.get::<_, i64>(1)? != 0,
                custom_label: r.get(2)?,
                custom_host: r.get(3)?,
                created_at: r.get(4)?,
                updated_at: r.get(5)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Insert a Custom namespace row.
///
/// `host_input` is run through [`validate_host`] before any DB IO.
/// The namespace id is `custom:<slug(label)>`; collisions surface as a
/// `UNIQUE` constraint violation from `rusqlite`.
///
/// # Errors
/// - [`NamespacesError::InvalidHost`] if `host_input` fails validation.
/// - DB / pool errors otherwise.
pub fn add_custom(
    pool: &DbPool,
    label: &str,
    host_input: &str,
) -> Result<NamespaceOverride, NamespacesError> {
    let _host = validate_host(host_input)?;
    let conn = pool.get()?;
    let id = format!("custom:{}", slug(label));
    let now = chrono::Utc::now().timestamp();
    conn.execute(
        "INSERT INTO namespace_overrides
         (namespace_id, enabled, custom_label, custom_host, created_at, updated_at)
         VALUES (?1, 1, ?2, ?3, ?4, ?4)",
        rusqlite::params![id, label, host_input, now],
    )?;
    Ok(NamespaceOverride {
        namespace_id: id,
        enabled: true,
        custom_label: Some(label.to_string()),
        custom_host: Some(host_input.to_string()),
        created_at: now,
        updated_at: now,
    })
}

/// Delete a Custom namespace row.
///
/// Rejects ids that don't start with `custom:` — only user-added
/// namespaces are removable. `local` / `wsl:*` rows can only be
/// toggled via [`set_enabled`].
///
/// # Errors
/// - [`NamespacesError::InvalidHost`] if `id` isn't `custom:*`.
/// - [`NamespacesError::NotFound`] if no row matches.
/// - DB / pool errors otherwise.
pub fn remove_custom(pool: &DbPool, id: &str) -> Result<(), NamespacesError> {
    if !id.starts_with("custom:") {
        return Err(NamespacesError::InvalidHost(
            "remove_custom only accepts custom: ids".into(),
        ));
    }
    let conn = pool.get()?;
    let changed = conn.execute(
        "DELETE FROM namespace_overrides WHERE namespace_id = ?1",
        rusqlite::params![id],
    )?;
    if changed == 0 {
        return Err(NamespacesError::NotFound(id.to_string()));
    }
    Ok(())
}

/// Upsert the `enabled` flag for `id`.
///
/// Works for any namespace id, including `local` and `wsl:*` which
/// may not have a row yet. For those ids the upsert inserts a row
/// with NULL `custom_label` / `custom_host` (the CHECK constraint
/// requires the prefix to be non-`custom:%` for that combination).
///
/// # Errors
/// - DB / pool errors. `NotFound` is returned only if the upsert
///   somehow reports zero rows changed — defensive; sqlite's
///   `INSERT ... ON CONFLICT DO UPDATE` should always touch a row.
pub fn set_enabled(pool: &DbPool, id: &str, enabled: bool) -> Result<(), NamespacesError> {
    let conn = pool.get()?;
    let now = chrono::Utc::now().timestamp();
    let upsert = conn.execute(
        "INSERT INTO namespace_overrides (namespace_id, enabled, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?3)
         ON CONFLICT(namespace_id) DO UPDATE
           SET enabled = excluded.enabled, updated_at = excluded.updated_at",
        rusqlite::params![id, i64::from(enabled), now],
    )?;
    if upsert == 0 {
        return Err(NamespacesError::NotFound(id.to_string()));
    }
    Ok(())
}

/// Convert a user label into an ASCII slug.
///
/// Alphanumerics are lowercased; everything else becomes `-`; runs
/// of dashes are not collapsed (sqlite collation will still treat
/// `gpu--box` as a unique id, so accidental collisions are unlikely).
/// Leading and trailing dashes are trimmed.
fn slug(label: &str) -> String {
    label
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
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

    // DB-touching tests removed: schema v3 migration drops namespace_overrides
    // and migrates rows into env_specs + migrated_custom_namespaces. The legacy
    // CRUD path is dead until namespaces.rs itself is deleted in Task 15.
    // validate_host tests above remain valid (pure-function, no DB).
}
