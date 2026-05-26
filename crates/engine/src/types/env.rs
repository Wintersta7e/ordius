//! `EnvId` — symbolic identifier for an environment target on a node.
//!
//! Phase D introduces this as a transparent newtype around `String`.
//! Phase E will validate against the env registry at workflow load
//! time and constrain the format (`local`, `wsl:<distro>`,
//! `ssh:<label>`, `container:<label>`). Phase D treats the value as
//! opaque text and only checks well-formedness (non-empty, no
//! leading/trailing whitespace) via [`EnvId::try_new`].
//!
//! Spec: docs/plans/environment-runtime.md §4 ("Node additions").

use serde::{Deserialize, Serialize};

/// Symbolic env identifier. Untouched by the engine in Phase D beyond
/// well-formedness; Phase E adds env-registry lookup.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EnvId(pub String);

impl EnvId {
    /// Construct from a string slice. Returns `None` on empty / whitespace-only
    /// input; trims surrounding whitespace.
    #[must_use]
    pub fn try_new(s: &str) -> Option<Self> {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(Self(trimmed.to_string()))
        }
    }

    /// View as `&str` — convenience for matchers + display.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for EnvId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_transparent_roundtrip() {
        let id = EnvId("wsl:Ubuntu".to_string());
        let s = serde_json::to_string(&id).unwrap();
        assert_eq!(s, r#""wsl:Ubuntu""#);
        let back: EnvId = serde_json::from_str(&s).unwrap();
        assert_eq!(back, id);
    }

    #[test]
    fn try_new_trims_and_rejects_empty() {
        assert_eq!(EnvId::try_new("  local  ").unwrap().as_str(), "local");
        assert!(EnvId::try_new("").is_none());
        assert!(EnvId::try_new("   ").is_none());
    }

    #[test]
    fn display_renders_inner() {
        assert_eq!(format!("{}", EnvId("ssh:box".into())), "ssh:box");
    }
}
