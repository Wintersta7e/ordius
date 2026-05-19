//! OS keyring access for workflow secrets.
//!
//! Wraps the `keyring-core` API and maintains a sidecar JSON
//! index of known secret names since the keyring backends have
//! no portable enumeration. Production callers build a default
//! [`Store`] once at startup; tests build one with an explicit
//! sidecar path under their temporary directory.

use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Default service name used when registering credentials with the OS keyring.
pub const DEFAULT_SERVICE: &str = "ordius";

/// Failure modes for [`Store`] operations.
#[derive(Debug, Error)]
pub enum SecretError {
    /// Backend keyring error.
    #[error("keyring: {0}")]
    Keyring(#[from] keyring_core::Error),
    /// Sidecar index read/write/parse error.
    #[error("sidecar {context}: {source}")]
    Sidecar {
        /// What was being attempted (`"reading"`, `"writing"`, `"parsing"`).
        context: String,
        /// Underlying `io::Error` or `serde_json::Error`.
        #[source]
        source: std::io::Error,
    },
    /// Sidecar JSON could not be parsed.
    #[error("sidecar parse: {0}")]
    SidecarParse(String),
    /// Could not determine a home directory for the default sidecar.
    #[error("home directory not found")]
    HomeNotFound,
}

/// Keyring-backed secrets store with an on-disk name index.
pub struct Store {
    sidecar: PathBuf,
}

impl Store {
    /// Build a store pointed at `~/.ordius/secrets-index.json`.
    /// Reads `HOME` (Unix) or `USERPROFILE` (Windows) to locate
    /// the home directory.
    pub fn default_for_user() -> Result<Self, SecretError> {
        let home = std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .ok_or(SecretError::HomeNotFound)?;
        let sidecar = PathBuf::from(home)
            .join(".ordius")
            .join("secrets-index.json");
        Ok(Self { sidecar })
    }

    /// Build a store that maintains its name index at `index_path`.
    /// Used by tests and any caller that wants a non-default
    /// sidecar location.
    #[must_use]
    pub const fn with_index_path(index_path: PathBuf) -> Self {
        Self {
            sidecar: index_path,
        }
    }

    /// Read the secret named `name` from the OS keyring.
    pub fn get(&self, name: &str) -> Result<String, SecretError> {
        let _ = self;
        let entry = keyring_core::Entry::new(DEFAULT_SERVICE, name)?;
        Ok(entry.get_password()?)
    }

    /// Store `value` under `name` in the OS keyring and add the
    /// name to the sidecar index.
    pub fn set(&self, name: &str, value: &str) -> Result<(), SecretError> {
        let entry = keyring_core::Entry::new(DEFAULT_SERVICE, name)?;
        entry.set_password(value)?;
        self.update_index(|names| {
            if !names.iter().any(|n| n == name) {
                names.push(name.into());
                names.sort();
            }
        })
    }

    /// Remove the secret named `name` from the keyring and from
    /// the sidecar index.
    pub fn delete(&self, name: &str) -> Result<(), SecretError> {
        let entry = keyring_core::Entry::new(DEFAULT_SERVICE, name)?;
        entry.delete_credential()?;
        self.update_index(|names| {
            names.retain(|n| n != name);
        })
    }

    /// Names known to the sidecar index. Returns an empty `Vec`
    /// when the sidecar doesn't exist yet (fresh install).
    pub fn list(&self) -> Result<Vec<String>, SecretError> {
        if !self.sidecar.exists() {
            return Ok(Vec::new());
        }
        let data = fs::read_to_string(&self.sidecar).map_err(|e| SecretError::Sidecar {
            context: "reading".into(),
            source: e,
        })?;
        serde_json::from_str(&data).map_err(|e| SecretError::SidecarParse(e.to_string()))
    }

    fn update_index<F: FnOnce(&mut Vec<String>)>(&self, mutate: F) -> Result<(), SecretError> {
        let mut names = self.list()?;
        mutate(&mut names);
        ensure_parent_dir(&self.sidecar)?;
        let data = serde_json::to_string_pretty(&names)
            .map_err(|e| SecretError::SidecarParse(e.to_string()))?;
        fs::write(&self.sidecar, data).map_err(|e| SecretError::Sidecar {
            context: "writing".into(),
            source: e,
        })?;
        Ok(())
    }
}

fn ensure_parent_dir(path: &Path) -> Result<(), SecretError> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
        && !parent.exists()
    {
        fs::create_dir_all(parent).map_err(|e| SecretError::Sidecar {
            context: "creating parent".into(),
            source: e,
        })?;
    }
    Ok(())
}

/// Redact known secret values from `text`, replacing every
/// occurrence of each value with `<redacted:NAME>`.
///
/// Replacements happen longest-value-first so when one secret's
/// value is a substring of another, the longer match wins. Empty
/// values are skipped — they'd otherwise replace every empty
/// position in the string. Defence against accidental leakage
/// only; an adversarial workflow that base64-encodes a secret
/// before printing can still defeat this.
#[must_use]
pub fn redact_secrets(text: &str, named_secrets: &[(String, String)]) -> String {
    let mut entries: Vec<&(String, String)> = named_secrets.iter().collect();
    entries.sort_by_key(|(_, v)| std::cmp::Reverse(v.len()));
    let mut out = text.to_string();
    for (name, value) in entries {
        // Skip empty values (would match every position) and
        // anything not actually present — `String::replace`
        // allocates a fresh String even when it finds no
        // matches, and most lines won't contain any given
        // secret.
        if !value.is_empty() && out.contains(value.as_str()) {
            out = out.replace(value.as_str(), &format!("<redacted:{name}>"));
        }
    }
    out
}

#[cfg(test)]
mod tests;
