//! Wire types for the environment probe.

use serde::{Deserialize, Serialize};

/// Host operating-system family with WSL split out from Linux so the
/// GUI can show the correct platform chip.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum HostPlatform {
    /// Native Windows host.
    Windows,
    /// Linux running under WSL.
    Wsl,
    /// Native Linux host (not WSL).
    Linux,
    /// macOS host.
    MacOs,
    /// Anything else (BSD, unknown).
    Other,
}

/// Lifecycle state of a WSL distro at probe time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WslState {
    /// Distro is running and the loopback inside it is reachable.
    Running,
    /// Distro exists but is not currently running.
    Stopped,
}

/// What kind of namespace an `NamespaceInfo` describes. Discriminated
/// on the wire by the `kind` field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub enum NamespaceKind {
    /// The host's own loopback / direct network.
    Local,
    /// A WSL distro reachable from the host.
    WslDistro {
        /// Distro name (e.g. `Ubuntu-24.04`).
        name: String,
        /// Whether the distro is currently running.
        state: WslState,
    },
    /// Reserved: the Windows host as seen from inside WSL.
    WindowsHost {
        /// Gateway IP used to reach the host.
        gateway_ip: String,
    },
    /// User-supplied custom namespace (remote machine, VM, etc.).
    Custom {
        /// Host name or IP of the custom namespace.
        host: NamespaceHost,
    },
}

/// Newtype around `url::Host<String>` so we can derive serde on it
/// without leaking the third-party type into wire docs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceHost(pub url::Host<String>);

/// One namespace as returned by the environment probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceInfo {
    /// Stable id (`local`, `wsl-<distro>`, or a UUID for custom).
    pub id: String,
    /// Human-friendly label for the GUI.
    pub label: String,
    /// Discriminator for what this namespace is.
    pub kind: NamespaceKind,
    /// Whether the user has enabled probing this namespace.
    pub enabled: bool,
    /// Reachability state at probe time.
    pub reachable: NamespaceState,
}

/// Reachability of a namespace at probe time. Discriminated on the
/// wire by the `state` field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "state")]
pub enum NamespaceState {
    /// Namespace responded successfully.
    Reachable,
    /// Namespace could not be reached (network error, timeout, …).
    Unreachable {
        /// Human-readable failure reason.
        reason: String,
    },
    /// User disabled this namespace; not probed this cycle.
    Disabled,
    /// WSL distro exists but isn't running.
    Stopped,
    /// Namespace is structurally not probeable from the current host.
    NotProbeable {
        /// Why probing is not possible from here.
        reason: String,
    },
}

/// One LLM endpoint discovered during a probe. Discriminated on the
/// wire by the `type` field (distinct from `kind`, which is the
/// service kind like `ollama`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum DiscoveredEndpoint {
    /// Endpoint is directly callable from the host using `callable_url`.
    Direct {
        /// Service kind (`ollama`, `lm-studio`, `llamacpp`, `openai-compat`).
        kind: String,
        /// Human-friendly label for the GUI.
        name: String,
        /// Id of the namespace this endpoint lives in.
        namespace_id: String,
        /// URL the host can call directly.
        callable_url: String,
        /// URL as observed inside the namespace.
        observed_url: String,
        /// Other namespace ids that also see this endpoint.
        co_visible_in: Vec<String>,
    },
    /// Endpoint is reachable only by routing through a namespace.
    OnlyViaNamespace {
        /// Service kind (`ollama`, `lm-studio`, `llamacpp`, `openai-compat`).
        kind: String,
        /// Human-friendly label for the GUI.
        name: String,
        /// Id of the namespace this endpoint lives in.
        namespace_id: String,
        /// URL as observed inside the namespace.
        observed_url: String,
        /// Why this endpoint isn't directly callable.
        hint: ReachHint,
        /// Other namespace ids that also see this endpoint.
        co_visible_in: Vec<String>,
    },
}

/// Why an endpoint isn't directly callable from the host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReachHint {
    /// WSL service bound to the distro's loopback only.
    WslLoopbackBound,
    /// Service bound to the Windows host from inside WSL.
    WindowsHostBound,
    /// Custom namespace is unreachable.
    CustomUnreachable,
}

/// Full snapshot returned by the environment probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentReport {
    /// Host platform family.
    pub platform: HostPlatform,
    /// Populated only when `platform == Wsl`; the distro name.
    pub wsl_distro: Option<String>,
    /// Discovered namespaces (Local always present in phase 1b).
    pub namespaces: Vec<NamespaceInfo>,
    /// Endpoints aggregated across all namespaces.
    pub endpoints: Vec<DiscoveredEndpoint>,
    /// `true` if the overall probe deadline expired before all
    /// namespaces finished.
    pub timed_out: bool,
}

impl EnvironmentReport {
    /// Build a partial report when the overall probe deadline fires.
    /// Local is always populated separately so its always-present
    /// invariant is type-enforced.
    #[must_use]
    pub fn partial(
        platform: HostPlatform,
        wsl_distro: Option<String>,
        local_namespace: NamespaceInfo,
        local_endpoints: Vec<DiscoveredEndpoint>,
    ) -> Self {
        Self {
            platform,
            wsl_distro,
            namespaces: vec![local_namespace],
            endpoints: local_endpoints,
            timed_out: true,
        }
    }
}

/// WSL/Custom probe-task return value; Local is passed separately into
/// `build_report` so its always-present invariant is type-enforced.
///
/// Reserved for phases 2-4; constructed by the WSL/custom probers.
#[derive(Debug, Clone)]
pub enum NamespaceProbeResult {
    /// Namespace responded and yielded a (possibly empty) endpoint list.
    Done {
        /// The namespace record to slot into the report.
        namespace: NamespaceInfo,
        /// Endpoints discovered inside this namespace.
        endpoints: Vec<DiscoveredEndpoint>,
    },
    /// Probe failed for this namespace.
    Unreachable {
        /// Human-readable failure reason.
        reason: String,
    },
    /// WSL distro exists but is not running.
    Stopped,
    /// User has disabled this namespace.
    Disabled,
}

/// Outcome of probing the host's own loopback.
#[derive(Debug, Clone)]
pub enum LocalProbeOutcome {
    /// Local loopback was reachable; `endpoints` may still be empty.
    Done {
        /// Whether the loopback itself answered (always `true` for now).
        reachable: bool,
        /// Endpoints discovered on the host loopback.
        endpoints: Vec<DiscoveredEndpoint>,
    },
    /// Local probe never ran (e.g. client builder failed).
    Error {
        /// Human-readable failure reason.
        reason: String,
    },
}
