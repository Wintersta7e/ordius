//! Environment identity, spec, state, and workspace binding types.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::resource::{ResourceDefinition, ResourceId};

/// Stable identifier for an environment. Persisted in the `environments`
/// table as TEXT. Format-typed for centralized parsing.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EnvId(String);

impl EnvId {
    /// Canonical identifier for the local host environment.
    pub const LOCAL: &'static str = "local";

    /// Creates an environment identifier from its persisted string form.
    pub fn new(raw: impl Into<String>) -> Self {
        Self(raw.into())
    }

    /// Creates the canonical local environment identifier.
    pub fn local() -> Self {
        Self(Self::LOCAL.to_string())
    }

    /// Creates an identifier for a WSL distribution.
    pub fn wsl(name: &str) -> Self {
        Self(format!("wsl:{name}"))
    }

    /// Creates an identifier for an SSH environment.
    pub fn ssh(label: &str) -> Self {
        Self(format!("ssh:{label}"))
    }

    /// Creates an identifier for a container environment.
    pub fn container(label: &str) -> Self {
        Self(format!("container:{label}"))
    }

    /// Returns the persisted string representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the broad environment kind implied by the identifier prefix.
    pub fn kind(&self) -> EnvKind {
        match self.0.as_str() {
            Self::LOCAL => EnvKind::Local,
            s if s.starts_with("wsl:") => EnvKind::Wsl,
            s if s.starts_with("ssh:") => EnvKind::Ssh,
            s if s.starts_with("container:") => EnvKind::Container,
            _ => EnvKind::Unknown,
        }
    }
}

impl std::fmt::Display for EnvId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Broad category parsed from an [`EnvId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvKind {
    /// The local host environment.
    Local,
    /// A Windows Subsystem for Linux distribution.
    Wsl,
    /// A remote host reached over SSH.
    Ssh,
    /// A container-backed environment.
    Container,
    /// An unrecognized identifier prefix.
    Unknown,
}

/// Newtype around an OS-keyring secret reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretRef(
    /// Secret lookup key in the OS keyring.
    pub String,
);

/// Newtype for a run id, kept here so `env.rs` does not depend on `run.rs`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RunId(
    /// Stable run identifier.
    pub String,
);

/// Newtype for a workflow id.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkflowId(
    /// Stable workflow identifier.
    pub String,
);

/// Host-to-container bind mount definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BindMount {
    /// Host path to expose.
    pub host: String,
    /// Container path where the host path is mounted.
    pub container: String,
    /// Whether the mount is read-only from inside the container.
    #[serde(default)]
    pub read_only: bool,
}

/// File synchronization strategy for non-shared workspaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncStrategy {
    /// Use `rsync` for synchronization.
    #[default]
    Rsync,
    /// Use SFTP for synchronization.
    Sftp,
}

/// Persisted environment spec. Inline `resources` is the single source of
/// truth for env-local resources (no separate table; see spec sections 3 and 8).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EnvSpec {
    /// Local host environment.
    Local {
        /// Resources scoped to this environment.
        #[serde(default)]
        resources: Vec<ResourceDefinition>,
        /// Host-direct verification records keyed by resource id.
        #[serde(default)]
        host_direct_verifications: HashMap<ResourceId, HostDirectVerification>,
    },
    /// Windows Subsystem for Linux distribution environment.
    WslDistro {
        /// Distribution name.
        name: String,
        /// Resources scoped to this environment.
        #[serde(default)]
        resources: Vec<ResourceDefinition>,
        /// Host-direct verification records keyed by resource id.
        #[serde(default)]
        host_direct_verifications: HashMap<ResourceId, HostDirectVerification>,
    },
    /// Remote SSH environment.
    Ssh {
        /// SSH host name or address.
        host: String,
        /// SSH user name.
        user: String,
        /// Reference to the authentication secret.
        auth_ref: SecretRef,
        /// Resources scoped to this environment.
        #[serde(default)]
        resources: Vec<ResourceDefinition>,
    },
    /// Container-backed environment.
    Container {
        /// Container image reference.
        image: String,
        /// Bind mounts to apply when starting the container.
        mounts: Vec<BindMount>,
        /// Environment variables passed to the container.
        env: HashMap<String, String>,
        /// Whether the container should be kept alive between operations.
        keep_alive: bool,
        /// Workspace binding strategy for the container.
        workspace_binding: WorkspaceBinding,
        /// Resources scoped to this environment.
        #[serde(default)]
        resources: Vec<ResourceDefinition>,
        /// Host-direct verification records keyed by resource id.
        #[serde(default)]
        host_direct_verifications: HashMap<ResourceId, HostDirectVerification>,
    },
}

impl EnvSpec {
    /// Short token identifying the variant — `local`, `wsl`, `ssh`, or
    /// `container`. Matches the `EnvId` prefix conventions so the two can
    /// be cross-checked at writer boundaries.
    #[must_use]
    pub const fn kind_str(&self) -> &'static str {
        match self {
            Self::Local { .. } => "local",
            Self::WslDistro { .. } => "wsl",
            Self::Ssh { .. } => "ssh",
            Self::Container { .. } => "container",
        }
    }

    /// Borrow the inline `resources` vec immutably. Every variant carries
    /// this field today; the helper saves callers from a 4-arm match when
    /// they only need read access.
    #[must_use]
    pub fn resources(&self) -> &[ResourceDefinition] {
        match self {
            Self::Local { resources, .. }
            | Self::WslDistro { resources, .. }
            | Self::Ssh { resources, .. }
            | Self::Container { resources, .. } => resources,
        }
    }

    /// Borrow the inline `resources` vec mutably. Writers (the env-local
    /// resource add / remove IPC) use this to mutate the resources in
    /// place before re-serialising the spec to `env_specs.spec_json`.
    pub const fn resources_mut(&mut self) -> &mut Vec<ResourceDefinition> {
        match self {
            Self::Local { resources, .. }
            | Self::WslDistro { resources, .. }
            | Self::Ssh { resources, .. }
            | Self::Container { resources, .. } => resources,
        }
    }
}

/// How the workflow's workspace directory reaches inside an environment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkspaceBinding {
    /// The same filesystem path is visible in both environments.
    Shared,
    /// The path can be translated deterministically between environments.
    Translated,
    /// The workspace is available through a bind mount.
    BindMount {
        /// Path inside the target environment.
        env_path: String,
    },
    /// The workspace is synchronized to a path inside the target environment.
    Sync {
        /// Template for the path inside the target environment.
        env_path_template: String,
        /// Synchronization mechanism.
        strategy: SyncStrategy,
        /// Policy for writing changed files back after a run.
        write_back: WriteBackPolicy,
    },
    /// The environment cannot access the workflow workspace.
    Unsupported,
}

/// Policy for copying environment-side workspace changes back to the host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "policy", rename_all = "snake_case")]
pub enum WriteBackPolicy {
    /// Do not copy changes back.
    None,
    /// Copy changes back when conflicts are not detected; otherwise diverge.
    SafeOrDiverge {
        /// Conflict detection mode.
        mode: ConflictDetect,
        /// Ignore patterns for write-back.
        #[serde(default)]
        ignore: Vec<String>,
        /// Maximum number of files considered during write-back.
        #[serde(default = "default_max_files")]
        max_files: usize,
    },
    /// Copy changes back without conflict checks.
    Force {
        /// Ignore patterns for write-back.
        #[serde(default)]
        ignore: Vec<String>,
    },
}

/// Conflict detection mode for safe write-back.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictDetect {
    /// Compare against a captured manifest.
    #[default]
    Manifest,
    /// Compare file checksums.
    Checksum,
    /// Compare modification time and file size.
    MtimeSize,
}

/// Default maximum number of files to consider during safe write-back.
pub const fn default_max_files() -> usize {
    5_000
}

/// Current reachability and enablement state for an environment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum EnvState {
    /// The environment can currently be reached.
    Reachable,
    /// Reachability is currently being checked.
    Probing,
    /// The environment could not be reached.
    Unreachable {
        /// Human-readable reason for the unreachable state.
        reason: String,
    },
    /// The environment is explicitly disabled.
    Disabled,
}

/// Catalog entry for an environment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvInfo {
    /// Environment identifier.
    pub id: EnvId,
    /// Human-readable display label.
    pub label: String,
    /// Persisted environment spec.
    pub spec: EnvSpec,
    /// Current environment state.
    pub state: EnvState,
    /// Whether the environment is enabled for scheduling.
    pub enabled: bool,
}

/// Record that a host-direct resource route was verified.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostDirectVerification {
    /// Time the verification was performed.
    pub verified_at: DateTime<Utc>,
    /// Verification method used.
    pub method: HostDirectMethod,
    /// Host URL that was verified.
    pub host_url: String,
    /// Probe route path used for the verification.
    pub probe_route_path: String,
    /// Stable fingerprint derived from the verified route.
    pub stable_fingerprint: String,
    /// `JSONPath` expressions to recompute for future verification.
    pub recompute_jsonpaths: Vec<String>,
}

/// Method used to verify host-direct resource access.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostDirectMethod {
    /// Verified through WSL mirrored networking.
    WslMirroredNetworking,
    /// Verified by explicitly rebinding the service to all interfaces.
    ExplicitRebindToAllInterfaces,
    /// User asserted that no automated verification is required.
    UserAssertedNoVerification,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_id_roundtrips_through_serde() {
        let id = EnvId::wsl("Ubuntu");
        let s = serde_json::to_string(&id).unwrap();
        assert_eq!(s, "\"wsl:Ubuntu\"");
        let back: EnvId = serde_json::from_str(&s).unwrap();
        assert_eq!(back, id);
    }

    #[test]
    fn env_id_kind_parses_prefix() {
        assert_eq!(EnvId::new("local").kind(), EnvKind::Local);
        assert_eq!(EnvId::wsl("Ubuntu").kind(), EnvKind::Wsl);
        assert_eq!(EnvId::ssh("dev-box").kind(), EnvKind::Ssh);
        assert_eq!(EnvId::container("builds").kind(), EnvKind::Container);
        assert_eq!(EnvId::new("garbage").kind(), EnvKind::Unknown);
    }

    #[test]
    fn env_spec_local_roundtrips() {
        let spec = EnvSpec::Local {
            resources: vec![],
            host_direct_verifications: HashMap::default(),
        };
        let s = serde_json::to_string(&spec).unwrap();
        let back: EnvSpec = serde_json::from_str(&s).unwrap();
        assert_eq!(spec, back);
    }

    #[test]
    fn workspace_binding_translated_roundtrips() {
        let wb = WorkspaceBinding::Translated;
        let s = serde_json::to_string(&wb).unwrap();
        assert!(s.contains("translated"));
        let back: WorkspaceBinding = serde_json::from_str(&s).unwrap();
        assert_eq!(wb, back);
    }

    #[test]
    fn write_back_policy_safe_or_diverge_defaults() {
        let pol = WriteBackPolicy::SafeOrDiverge {
            mode: ConflictDetect::default(),
            ignore: vec![],
            max_files: default_max_files(),
        };
        assert!(matches!(pol, WriteBackPolicy::SafeOrDiverge { .. }));
        drop(serde_json::to_string(&pol).unwrap());
    }
}
