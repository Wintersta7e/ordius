//! Errors raised by the runtime layer.
//!
//! `DispatchError` covers all failures that can occur when dispatching a node
//! to an environment (unreachable env, missing resource, missing capability,
//! workspace setup, path translation, and spawn). `RegistryError` covers
//! conflicts and look-up failures inside the `ResourceRegistry`.

use thiserror::Error;

/// Error raised when dispatching a node to a named environment.
///
/// Variants map 1-to-1 to failure modes in `Dispatcher::dispatch` and
/// `Dispatcher::prepare_workspace`. Wired into `EngineError::Dispatch`
/// via `#[from]`.
#[derive(Debug, Error)]
pub enum DispatchError {
    /// The target environment is not reachable at dispatch time.
    #[error("env unreachable ({env_id}): {reason}")]
    EnvUnreachable {
        /// Environment id string (e.g. `"wsl:Ubuntu"`).
        env_id: String,
        /// Human-readable reason for the failure.
        reason: String,
    },

    /// The environment was reachable at dispatch start but disconnected
    /// mid-run (e.g. the WSL distro was terminated).
    #[error("env lost mid-run ({env_id})")]
    EnvLost {
        /// Environment id string.
        env_id: String,
    },

    /// Helper bootstrap into the env failed (push, sha256-verify, chmod).
    #[error("helper bootstrap: {0}")]
    HelperBootstrap(String),

    /// Probe / spawn cancelled by the caller.
    #[error("cancelled")]
    Cancelled,

    /// The requested resource id is not registered for the environment.
    #[error("resource {id} not in env {env_id}; available: {available:?}")]
    ResourceMissing {
        /// Resource id that was requested.
        id: String,
        /// Environment id where the resource was expected.
        env_id: String,
        /// Ids of resources that are registered in that environment.
        available: Vec<String>,
    },

    /// The resource exists but does not expose the required capability.
    #[error("capability {required:?} not proven on {id}@{env_id}; proven: {proven:?}")]
    CapabilityMissing {
        /// Resource id.
        id: String,
        /// Environment id.
        env_id: String,
        /// The capability that was required.
        required: String,
        /// Capabilities that were actually proven during the last probe.
        proven: Vec<String>,
    },

    /// The workspace directory could not be prepared in the environment
    /// (e.g. translation from WSL path failed, rsync refused, etc.).
    #[error("workspace unavailable on {env_id}: {reason}")]
    WorkspaceUnavailable {
        /// Environment id.
        env_id: String,
        /// Reason the workspace could not be prepared.
        reason: String,
    },

    /// A host path could not be translated to an env-side path.
    #[error("path translation failed ({host_path}): {reason}")]
    PathTranslation {
        /// The host-side path that failed translation.
        host_path: String,
        /// Reason for the failure.
        reason: String,
    },

    /// The dispatcher knows about this operation but a later phase implements it.
    #[error("not implemented: {0}")]
    NotImplemented(String),

    /// Spawning a process inside the environment failed.
    #[error("spawn failed in {env_id}: {source}")]
    Spawn {
        /// Environment id where the spawn was attempted.
        env_id: String,
        /// Underlying `io::Error` from the spawn attempt.
        #[source]
        source: std::io::Error,
    },

    /// A host ↔ env path translation rule didn't match.
    #[error("path translation: {0}")]
    Translation(String),

    /// The dispatcher does not handle this binding/operation for the given env type.
    #[error("operation unsupported: {0}")]
    Unsupported(String),
}

/// Error raised by `ResourceRegistry` operations.
///
/// Returned from registry insertion and look-up methods. Not wired into
/// `EngineError` directly — callers convert as needed.
#[derive(Debug, Error)]
pub enum RegistryError {
    /// A resource with the same id already exists at a lower (or equal)
    /// scope and `override_lower_scope` was not set on the new definition.
    #[error(
        "resource {id} already declared at {existing_scope}; \
         pass override_lower_scope to shadow"
    )]
    OverrideRequired {
        /// Resource id that clashed.
        id: String,
        /// Scope name where the existing definition lives (e.g. `"builtin"`).
        existing_scope: String,
    },

    /// No resource with the given id is registered for the environment.
    #[error("resource {id} not found in registry for env {env_id}")]
    NotFound {
        /// Resource id that was looked up.
        id: String,
        /// Environment id the look-up was scoped to.
        env_id: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_error_displays() {
        let e = DispatchError::EnvUnreachable {
            env_id: "wsl:Ubuntu".into(),
            reason: "wsl.exe not found".into(),
        };
        let s = format!("{e}");
        assert!(s.contains("wsl:Ubuntu"));
        assert!(s.contains("wsl.exe not found"));
    }

    #[test]
    fn registry_error_override_clash() {
        let e = RegistryError::OverrideRequired {
            id: "ollama".into(),
            existing_scope: "builtin".into(),
        };
        assert!(format!("{e}").contains("override_lower_scope"));
    }
}
