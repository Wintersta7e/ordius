//! camelCase DTOs at the Tauri boundary.
//!
//! The engine speaks `snake_case` on disk and on the wire; the
//! TypeScript frontend wants `camelCase` per the design handoff
//! (`docs/UI/Ordius - Handoff.html` §10). Every type exposed to a
//! Tauri command lives here with `#[serde(rename_all =
//! "camelCase")]` so the boundary conversion is centralised and
//! the engine types stay untouched.

use ordius_engine::events::RunEvent;
use ordius_engine::settings::{ModelEndpoint, Settings};
use ordius_engine::system_status::{EndpointStatus, SystemStatus};
use ordius_engine::types::{NodeType, Workflow};
use ordius_engine::workspaces::Workspace;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;

/// Re-keys an inner engine type to/from `camelCase` on the wire.
///
/// Keeps the inner type's on-disk `snake_case` format untouched. Used
/// for `Workflow`/`NodeType`/etc. where defining a parallel DTO tree
/// would balloon to hundreds of lines of trivial field-by-field
/// `From` impls.
#[derive(Debug, Clone)]
pub struct JsonCamel<T>(pub T);

impl<T: Serialize> Serialize for JsonCamel<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let v = serde_json::to_value(&self.0).map_err(serde::ser::Error::custom)?;
        rename_keys(v, snake_to_camel).serialize(serializer)
    }
}

impl<'de, T: DeserializeOwned> Deserialize<'de> for JsonCamel<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let v = serde_json::Value::deserialize(deserializer)?;
        let renamed = rename_keys(v, camel_to_snake);
        let inner: T = serde_json::from_value(renamed).map_err(serde::de::Error::custom)?;
        Ok(Self(inner))
    }
}

/// Walk a JSON value and rename every object key through `f`. Arrays
/// recurse; primitives pass through.
fn rename_keys(value: serde_json::Value, f: fn(&str) -> String) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.into_iter()
                .map(|(k, v)| (f(&k), rename_keys(v, f)))
                .collect(),
        ),
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(|v| rename_keys(v, f)).collect())
        },
        other => other,
    }
}

/// `snake_case` → `camelCase`. Only converts canonical `snake_case`
/// identifiers (lowercase ASCII alnum + underscores, starting with a
/// lowercase letter). Any other shape — uppercase, hyphens, dots,
/// non-ASCII — passes through unchanged so user-controlled map keys
/// (e.g. variable names like `API_KEY`, kebab-case slugs, env var
/// names) round-trip safely.
fn snake_to_camel(input: &str) -> String {
    if !is_canonical_snake(input) {
        return input.to_string();
    }
    let mut out = String::with_capacity(input.len());
    let mut upper_next = false;
    for ch in input.chars() {
        if ch == '_' {
            upper_next = true;
        } else if upper_next {
            out.extend(ch.to_uppercase());
            upper_next = false;
        } else {
            out.push(ch);
        }
    }
    out
}

/// `camelCase` → `snake_case`. Only converts canonical camelCase
/// identifiers (ASCII alnum, lowercase start, at least one uppercase
/// boundary). Non-camel inputs round-trip as-is.
fn camel_to_snake(input: &str) -> String {
    if !is_canonical_camel(input) {
        return input.to_string();
    }
    let mut out = String::with_capacity(input.len() + 2);
    for (i, ch) in input.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.extend(ch.to_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

fn is_canonical_snake(s: &str) -> bool {
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_lowercase() {
        return false;
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn is_canonical_camel(s: &str) -> bool {
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_lowercase() {
        return false;
    }
    let mut has_upper = false;
    for c in chars {
        if !c.is_ascii_alphanumeric() {
            return false;
        }
        if c.is_ascii_uppercase() {
            has_upper = true;
        }
    }
    has_upper
}

/// One workflow row in the Home grid + recent-tab dropdown.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SavedWorkflowDto {
    /// Stable identifier (filename stem).
    pub id: String,
    /// Display name.
    pub name: String,
    /// Number of triggers declared. 0 → manual-only.
    pub triggers_count: usize,
    /// Number of nodes on the graph (for the dashboard subtitle).
    pub nodes_count: usize,
}

impl From<&Workflow> for SavedWorkflowDto {
    fn from(wf: &Workflow) -> Self {
        Self {
            id: wf.id.clone(),
            name: wf.name.clone(),
            triggers_count: wf.triggers.len(),
            nodes_count: wf.nodes.len(),
        }
    }
}

/// Full workflow JSON the editor consumes. Serialises with
/// `camelCase` keys via [`JsonCamel`]; the engine type retains its
/// `snake_case` on-disk format.
pub type WorkflowDto = JsonCamel<Workflow>;

/// Envelope returned from `load_workflow`.
///
/// Carries the workflow plus any non-fatal lint warnings the loader
/// emitted (e.g. loopback URLs targeting a non-local env) so the
/// editor can render them inline without re-running the validator.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadWorkflowDto {
    /// The workflow itself, camelCased for the webview.
    pub workflow: WorkflowDto,
    /// Per-node warnings emitted during load. May be empty.
    pub warnings: Vec<WorkflowWarningDto>,
}

/// One workflow-load warning surfaced to the editor. Wire shape
/// mirrors `ordius_engine::workflows::WorkflowWarning`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowWarningDto {
    /// Id of the node the warning applies to.
    pub node_id: String,
    /// Snake-case discriminant matching `WorkflowWarningKind`.
    /// Unknown future variants serialise as `"unknown"` so the
    /// frontend can render them with a generic label.
    pub kind: String,
    /// Human-readable explanation suitable for surfacing in the UI.
    pub message: String,
}

impl From<ordius_engine::workflows::WorkflowWarning> for WorkflowWarningDto {
    fn from(w: ordius_engine::workflows::WorkflowWarning) -> Self {
        use ordius_engine::workflows::WorkflowWarningKind;
        let kind = match w.kind {
            WorkflowWarningKind::LoopbackUrlInRemoteEnv => "loopback_url_in_remote_env",
            // The engine marks `WorkflowWarningKind` as
            // `#[non_exhaustive]`; new variants must not break this
            // boundary, so fall back to a generic discriminant.
            _ => "unknown",
        };
        Self {
            node_id: w.node_id,
            kind: kind.to_string(),
            message: w.message,
        }
    }
}

/// Payload returned from `run_workflow` — the frontend immediately
/// uses `runId` to subscribe to the live event stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunStartedDto {
    /// Newly-created run id.
    pub run_id: String,
}

/// One row in `runs ls` / history page list.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunRowDto {
    /// Run id.
    pub run_id: String,
    /// Workflow this run was executed against.
    pub workflow_id: String,
    /// Terminal status — `done` | `error` | `stopped` | `running`.
    pub status: String,
    /// Unix millis the run started.
    pub started_at: i64,
    /// Unix millis the run finished, or null if still running.
    pub finished_at: Option<i64>,
    /// Total run duration in milliseconds.
    pub duration_ms: Option<i64>,
    /// How the run was triggered (`cli` | `gui` | `schedule` | `webhook` | `file-watch` | `manual`).
    pub trigger_kind: String,
}

/// One `node_runs` row inside a run's detail view.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeRunRowDto {
    /// Node id within the workflow.
    pub node_id: String,
    /// Loop iteration (1-based).
    pub iteration: u32,
    /// Retry attempt (1-based).
    pub attempt: u32,
    /// Node-type id at run time.
    pub node_type: String,
    /// Status string.
    pub status: String,
    /// Started timestamp (epoch ms).
    pub started_at: Option<i64>,
    /// Finished timestamp.
    pub finished_at: Option<i64>,
    /// Total duration.
    pub duration_ms: Option<i64>,
    /// Error string on failure.
    pub error: Option<String>,
}

/// Full run detail returned by `get_run`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunDetailDto {
    /// Header row.
    #[serde(flatten)]
    pub row: RunRowDto,
    /// Per-node history rows, ordered by `started_at`.
    pub node_runs: Vec<NodeRunRowDto>,
}

/// Camel-case node-type spec for the palette + properties panel.
pub type NodeTypeDto = JsonCamel<NodeType>;

/// One registered secret. Values are never exposed — just the name
/// + a first/last 4-char preview the GUI can show.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretMetaDto {
    /// Secret name as stored in the keyring.
    pub name: String,
}

/// One workspace entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceDto {
    /// UUID id.
    pub id: String,
    /// Display name.
    pub name: String,
    /// Absolute path (already canonicalised).
    pub path: String,
}

impl From<Workspace> for WorkspaceDto {
    fn from(w: Workspace) -> Self {
        Self {
            id: w.id,
            name: w.name,
            path: w.path.display().to_string(),
        }
    }
}

/// Settings DTO. Mirrors `engine::settings::Settings` field-for-field
/// but with camelCase serde rename.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SettingsDto {
    /// `dark` | `light`.
    pub theme: String,
    /// `left` | `right`.
    pub palette_side: String,
    /// `bezier` | `orthogonal` | `straight`.
    pub edge_style: String,
    /// `comfortable` | `rich`.
    pub density: String,
    /// `dots` | `lines` | `off`.
    pub grid: String,
    /// `jewel` | `citrus` | `glacier`.
    pub color_scheme: String,
    /// Max concurrent runs.
    pub max_concurrent_runs: u32,
    /// Retention days.
    pub retention_days: u32,
    /// Registered model endpoints.
    pub model_endpoints: Vec<ModelEndpointDto>,
}

impl From<Settings> for SettingsDto {
    fn from(s: Settings) -> Self {
        Self {
            theme: s.theme,
            palette_side: s.palette_side,
            edge_style: s.edge_style,
            density: s.density,
            grid: s.grid,
            color_scheme: s.color_scheme,
            max_concurrent_runs: s.max_concurrent_runs,
            retention_days: s.retention_days,
            model_endpoints: s
                .model_endpoints
                .into_iter()
                .map(ModelEndpointDto::from)
                .collect(),
        }
    }
}

impl From<SettingsDto> for Settings {
    fn from(s: SettingsDto) -> Self {
        Self {
            theme: s.theme,
            palette_side: s.palette_side,
            edge_style: s.edge_style,
            density: s.density,
            grid: s.grid,
            color_scheme: s.color_scheme,
            max_concurrent_runs: s.max_concurrent_runs,
            retention_days: s.retention_days,
            model_endpoints: s
                .model_endpoints
                .into_iter()
                .map(ModelEndpoint::from)
                .collect(),
        }
    }
}

/// Model endpoint DTO.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelEndpointDto {
    /// UUID id.
    pub id: String,
    /// Display name.
    pub name: String,
    /// Base URL.
    pub base_url: String,
    /// Optional `{{secrets.X}}` template referencing the API key.
    pub api_key_secret: Option<String>,
}

impl From<ModelEndpoint> for ModelEndpointDto {
    fn from(e: ModelEndpoint) -> Self {
        Self {
            id: e.id,
            name: e.name,
            base_url: e.base_url,
            api_key_secret: e.api_key_secret,
        }
    }
}

impl From<ModelEndpointDto> for ModelEndpoint {
    fn from(e: ModelEndpointDto) -> Self {
        Self {
            id: e.id,
            name: e.name,
            base_url: e.base_url,
            api_key_secret: e.api_key_secret,
        }
    }
}

/// System status snapshot for the Home left-rail.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SystemStatusDto {
    /// `runs.db` byte size.
    pub runs_db_bytes: u64,
    /// Workspaces dir byte size.
    pub workspaces_bytes: u64,
    /// Engine version string.
    pub engine_version: String,
    /// Endpoint reachability hints.
    pub endpoints: Vec<EndpointStatusDto>,
}

impl From<SystemStatus> for SystemStatusDto {
    fn from(s: SystemStatus) -> Self {
        Self {
            runs_db_bytes: s.runs_db_bytes,
            workspaces_bytes: s.workspaces_bytes,
            engine_version: s.engine_version.to_string(),
            endpoints: s
                .endpoints
                .into_iter()
                .map(EndpointStatusDto::from)
                .collect(),
        }
    }
}

/// Per-endpoint reachability DTO.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointStatusDto {
    /// Endpoint id.
    pub id: String,
    /// Endpoint name.
    pub name: String,
    /// `ok` | `down` | `unknown`.
    pub state: String,
}

impl From<EndpointStatus> for EndpointStatusDto {
    fn from(e: EndpointStatus) -> Self {
        Self {
            id: e.id,
            name: e.name,
            state: e.state,
        }
    }
}

/// Args for `run_workflow`. Variables map keys are workflow var
/// names, values come from the `RunDialog` form.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunWorkflowArgs {
    /// Workflow id to run.
    pub workflow_id: String,
    /// Variable overrides (`{{vars.X}}` substitution targets).
    #[serde(default)]
    pub variables: HashMap<String, String>,
    /// Workspace id to run against. `None` → use engine home.
    #[serde(default)]
    pub workspace_id: Option<String>,
    /// Auto-resume `checkpoint` nodes (`RunDialog` "Yes to all" check).
    #[serde(default)]
    pub auto_resume: bool,
}

/// camelCase `RunEvent` shape forwarded over the streaming channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunEventDto {
    /// Wire-tag (`workflow:started`, `node:done`, …).
    #[serde(rename = "type")]
    pub ty: String,
    /// Monotonic per-run sequence number.
    pub seq: u64,
    /// Wall-clock emission time, Unix epoch milliseconds.
    pub emitted_at: i64,
    /// Run id.
    pub run_id: String,
    /// Optional node id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    /// Loop iteration (1-based) the event was emitted under.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iteration: Option<u32>,
    /// Retry attempt (1-based) the event was emitted under.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt: Option<u32>,
    /// Free-form payload. Keys arrive `snake_case` from the engine
    /// (`workflow:started` → `workflow_id` / `workflow_name` /
    /// `trigger_kind`) so we re-key here on emit.
    #[serde(flatten)]
    pub payload: HashMap<String, serde_json::Value>,
}

impl From<RunEvent> for RunEventDto {
    fn from(ev: RunEvent) -> Self {
        // Convert payload keys snake_case → camelCase shallowly. The
        // engine payloads use snake_case so the recorder JSON column
        // stays stable; the wire layer is camelCase per the handoff.
        let payload = ev
            .payload
            .into_iter()
            .map(|(k, v)| (snake_to_camel(&k), v))
            .collect();
        Self {
            ty: ev.ty.wire_tag().to_string(),
            seq: ev.seq,
            emitted_at: ev.emitted_at,
            run_id: ev.run_id,
            node_id: ev.node_id,
            iteration: ev.iteration,
            attempt: ev.attempt,
            payload,
        }
    }
}

// ─── Environment / EnvSpec IPC ─────────────────────────────────────

/// Snapshot returned by every `environment_*` command. One row per env
/// (active + disabled), each with its resolved state + the resources the
/// boot probe / refresh observed.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnvSnapshotIpc {
    /// One entry per env id in the engine's `env_registry` and
    /// `env_disabled_specs`.
    pub envs: Vec<EnvEntryIpc>,
}

/// One env's view as the GUI consumes it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnvEntryIpc {
    /// `EnvId::as_str()` form (`local`, `wsl:Ubuntu`, `custom:dev`, …).
    pub id: String,
    /// Display label.
    pub label: String,
    /// Broad category derived from the id prefix.
    pub kind: EnvKindIpc,
    /// Whether the env is enabled for scheduling.
    pub enabled: bool,
    /// Reachability / lifecycle state.
    pub state: EnvStateIpc,
    /// Resources the latest probe observed. Empty for `Disabled` envs and
    /// for envs whose catalog hasn't populated yet.
    pub resources: Vec<EnvResourceIpc>,
}

/// Broad env category — mirrors `ordius_engine::environment::runtime::EnvKind`
/// minus the `Unknown` variant (callers should never see that on the wire).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvKindIpc {
    /// The local host environment.
    Local,
    /// A Windows Subsystem for Linux distribution.
    WslDistro,
    /// A remote host reached over SSH.
    Ssh,
    /// A container-backed environment.
    Container,
}

/// Reachability / lifecycle state for the IPC envelope. Disabled is
/// surfaced for `env_specs` rows whose `enabled` flag is `false`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum EnvStateIpc {
    /// The env was reached on the last refresh.
    Reachable,
    /// A refresh is in flight for this env.
    Probing,
    /// The env was unreachable; carries the human-readable reason.
    Unreachable {
        /// Why the probe failed (transport error, timeout, missing dispatcher, …).
        reason: String,
    },
    /// The env's `env_specs` row has `enabled = 0`.
    Disabled,
}

/// One probed resource as the GUI consumes it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnvResourceIpc {
    /// Resource id (matches the `EnvSpec`'s inline definition).
    pub id: String,
    /// Probe kind: `http_endpoint`, `binary`, or `toolchain`.
    pub kind: String,
    /// Outcome of the most recent probe.
    pub state: ResourceStateIpc,
    /// Base URL the resource is reachable at — `Some` only for HTTP routes
    /// that resolved successfully.
    pub base_url: Option<String>,
    /// Probe-reported version string, when available.
    pub version: Option<String>,
    /// How the route was reached (`env_loopback`, `host_direct`, …) —
    /// `Some` only for HTTP routes that resolved successfully.
    pub route_origin: Option<String>,
}

/// Outcome of a resource probe. Mirrors
/// `ordius_engine::environment::runtime::catalog::ResourceProbeOutcome` for
/// the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ResourceStateIpc {
    /// The resource was reachable.
    Found,
    /// No process / binary was discovered for this resource.
    NotFound,
    /// The probe was deliberately skipped.
    Skipped {
        /// Human-readable reason the probe was skipped.
        reason: String,
    },
    /// The per-resource deadline elapsed.
    TimedOut,
    /// The resource was reachable but the probe request failed.
    ProbeFailed {
        /// Human-readable failure description.
        reason: String,
    },
}

/// Payload accepted by `environment_add`.
///
/// `spec` is the raw JSON form of
/// `ordius_engine::environment::runtime::EnvSpec`; parsing happens
/// engine-side so the desktop crate stays decoupled from the spec's
/// internal `serde(tag)` shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnvAddIpc {
    /// Stable env id (e.g. `wsl:Ubuntu`, `custom:dev`).
    pub id: String,
    /// Display label shown in the env picker.
    pub label: String,
    /// Whether the env starts enabled (default `true` on the GUI form).
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Raw `EnvSpec` JSON. The command parses this into `EnvSpec` before
    /// inserting; malformed payloads surface as a typed error.
    pub spec: serde_json::Value,
}

const fn default_enabled() -> bool {
    true
}

/// Payload accepted by `environment_add_resource`.
///
/// `definition` is a `ResourceDefinition` rendered in `camelCase` keys
/// at the Tauri boundary — `JsonCamel` rewrites them to `snake_case`
/// before handing the value to serde, so any mismatch surfaces as a
/// typed deserialize error with the offending field path. Set
/// `overrideLowerScope: true` on the definition when shadowing a
/// built-in id.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnvAddResourceIpc {
    /// Env id the resource is added to.
    pub env_id: String,
    /// Typed `ResourceDefinition` wrapped so `camelCase` keys land here
    /// the same way the rest of the IPC surface does.
    pub definition: JsonCamel<ordius_engine::environment::runtime::ResourceDefinition>,
}

// ─── Resource picker definitions ─────────────────────────────────

/// One resource definition + its current probe outcome.
///
/// Scoped to an `(env_id, workflow_id?)` context. Used by the workflow
/// editor's Resource Picker, which needs full capability + scope info
/// that the [`EnvResourceIpc`] snapshot strips to keep the global poll
/// cheap.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnvDefinitionIpc {
    /// Stable resource id (matches the `ResourceDefinition::id`).
    pub id: String,
    /// `"http_endpoint" | "binary" | "toolchain"`.
    pub kind: String,
    /// Scope where this resource was declared. One of `"builtin"`,
    /// `"user_global"`, `"env_local"`, `"workflow"`.
    pub scope: String,
    /// Capabilities the definition advertises (snake-case strings
    /// matching `Capability`'s serde rename).
    pub advertised_capabilities: Vec<String>,
    /// Capabilities the latest probe *proved*. Subset of advertised.
    /// Empty for non-`Found` outcomes.
    pub proven_capabilities: Vec<String>,
    /// Outcome of the latest probe of this resource. `Unknown` means the
    /// catalog has no entry for this resource at all (cache miss
    /// distinct from `NotFound`).
    pub outcome: ResourceProbeOutcomeIpc,
    /// `RouteOrigin` (snake-case) when outcome is `Found` AND kind is
    /// `http_endpoint`; `None` otherwise.
    pub route_origin: Option<String>,
    /// Base URL when outcome is `Found` AND kind is `http_endpoint`.
    pub base_url: Option<String>,
    /// Version string when the probe captured one.
    pub version: Option<String>,
}

/// Probe outcome flattened for the wire.
///
/// Mirrors `ordius_engine::environment::runtime::ResourceProbeOutcome`
/// plus an `Unknown` variant for resources absent from the catalog
/// (cache miss distinct from `NotFound`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "outcome", content = "reason", rename_all = "snake_case")]
pub enum ResourceProbeOutcomeIpc {
    /// The resource was reachable and details were captured.
    Found,
    /// No process / binary was discovered for this resource.
    NotFound,
    /// The probe was deliberately skipped; carries the reason.
    Skipped(String),
    /// The per-resource deadline elapsed.
    TimedOut,
    /// The probe request failed; carries the reason.
    ProbeFailed(String),
    /// The catalog has no entry for this resource (cache miss).
    Unknown,
}

/// Listing returned by `environment_definitions(env_id, workflow_id?)`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnvDefinitionListIpc {
    /// Env the definitions are scoped to.
    pub env_id: String,
    /// Workflow scope, when the caller asked for workflow-visible defs.
    pub workflow_id: Option<String>,
    /// Registry revision captured at snapshot time. UI uses it to
    /// debounce/invalidate cached pickers.
    pub registry_revision: u64,
    /// One row per resource visible to `(env_id, workflow_id?)`. Order
    /// matches `ResourceRegistry::visible_to` (highest precedence
    /// scope first).
    pub definitions: Vec<EnvDefinitionIpc>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use ordius_engine::events::EventType;

    #[test]
    fn snake_to_camel_handles_common_keys() {
        assert_eq!(snake_to_camel("workflow_id"), "workflowId");
        assert_eq!(snake_to_camel("node_type"), "nodeType");
        assert_eq!(snake_to_camel("started_at"), "startedAt");
        assert_eq!(snake_to_camel("text"), "text");
        // Non-canonical inputs (starts with underscore) round-trip unchanged
        // so user-defined keys aren't mangled.
        assert_eq!(snake_to_camel("__double"), "__double");
    }

    #[test]
    fn run_event_dto_renames_top_level_fields() {
        let ev = RunEvent {
            ty: EventType::NodeDone,
            seq: 7,
            emitted_at: 1_700_000_000_000,
            run_id: "r1".into(),
            node_id: Some("n1".into()),
            iteration: Some(2),
            attempt: Some(3),
            payload: HashMap::from([
                (
                    "finished_at".to_string(),
                    serde_json::json!(1_700_000_001_000_i64),
                ),
                ("duration_ms".to_string(), serde_json::json!(1000_i64)),
            ]),
        };
        let dto: RunEventDto = ev.into();
        let json = serde_json::to_string(&dto).unwrap();
        assert!(json.contains(r#""runId":"r1""#));
        assert!(json.contains(r#""nodeId":"n1""#));
        assert!(json.contains(r#""emittedAt":1700000000000"#));
        assert!(json.contains(r#""finishedAt":1700000001000"#));
        assert!(json.contains(r#""durationMs":1000"#));
        assert!(json.contains(r#""type":"node:done""#));
    }

    #[test]
    fn settings_round_trip_preserves_fields() {
        let original = Settings {
            theme: "light".into(),
            max_concurrent_runs: 16,
            ..Settings::default()
        };
        let dto: SettingsDto = original.clone().into();
        let back: Settings = dto.into();
        assert_eq!(back, original);
    }

    #[test]
    fn secret_meta_serializes_camel_case() {
        let dto = SecretMetaDto {
            name: "API_KEY".into(),
        };
        let json = serde_json::to_string(&dto).unwrap();
        assert_eq!(json, r#"{"name":"API_KEY"}"#);
    }

    #[test]
    fn camel_to_snake_roundtrip() {
        assert_eq!(camel_to_snake("workflowId"), "workflow_id");
        assert_eq!(camel_to_snake("nodeType"), "node_type");
        assert_eq!(camel_to_snake("fromNodeId"), "from_node_id");
        assert_eq!(camel_to_snake("text"), "text");
        // Round-trip: snake -> camel -> snake is identity for canonical inputs.
        for s in ["workflow_id", "from_port", "continue_on_error", "x"] {
            assert_eq!(camel_to_snake(&snake_to_camel(s)), s);
        }
    }

    #[test]
    fn json_camel_renames_workflow_keys() {
        use ordius_engine::types::{Trigger, Workflow};
        let wf = Workflow {
            id: "w1".into(),
            name: "n".into(),
            schema_version: 1,
            variables: HashMap::new(),
            triggers: vec![Trigger::Manual],
            nodes: vec![],
            edges: vec![],
            created_at: None,
            updated_at: None,
            resources: vec![],
            default_env: None,
        };
        let dto = JsonCamel(wf);
        let json = serde_json::to_string(&dto).unwrap();
        assert!(json.contains(r#""schemaVersion":1"#));
        assert!(!json.contains("schema_version"));
        // And deserialize back from camelCase JSON.
        let parsed: JsonCamel<Workflow> = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.0.schema_version, 1);
        assert_eq!(parsed.0.id, "w1");
    }

    #[test]
    fn json_camel_renames_nested_object_keys() {
        // Object keys at every depth get re-keyed, not just the top level.
        let inner = serde_json::json!({"snake_case_key": {"nested_snake": 1}});
        let renamed = rename_keys(inner, snake_to_camel);
        assert_eq!(
            renamed,
            serde_json::json!({"snakeCaseKey": {"nestedSnake": 1}})
        );
    }

    #[test]
    fn rename_preserves_user_defined_keys() {
        // Variable names like API_KEY (uppercase), env vars like HOME,
        // kebab-case slugs ("file-watch"), and arbitrary user keys must
        // round-trip unchanged through the walker.
        assert_eq!(snake_to_camel("API_KEY"), "API_KEY");
        assert_eq!(snake_to_camel("HOME"), "HOME");
        assert_eq!(snake_to_camel("file-watch"), "file-watch");
        assert_eq!(snake_to_camel("MY_VAR_2"), "MY_VAR_2");
        assert_eq!(camel_to_snake("API_KEY"), "API_KEY");
        assert_eq!(camel_to_snake("file-watch"), "file-watch");
        // Identifiers that look like canonical snake/camel still convert.
        assert_eq!(snake_to_camel("workflow_id"), "workflowId");
        assert_eq!(camel_to_snake("workflowId"), "workflow_id");
        // No-underscore lowercase: nothing to do either way.
        assert_eq!(snake_to_camel("text"), "text");
        assert_eq!(camel_to_snake("text"), "text");
    }
}
