// TypeScript counterparts of the camelCase DTOs that
// `crates/desktop/src/dto.rs` returns from Tauri commands.
//
// Keep these in lock-step: any rename here without a matching
// `#[serde(rename = ...)]` change on the Rust side will result in
// a silent `undefined` on the consuming component. The engine
// itself still speaks snake_case on disk; the Rust dto.rs layer
// does the conversion.

export type Category =
  | "execution"
  | "llm"
  | "data"
  | "control"
  | "integration";

export type ConfigFieldType =
  | "string"
  | "number"
  | "boolean"
  | "textarea"
  | "select"
  | "file"
  | "secret";

export type ExecutionBackend = "in_process" | "subprocess" | "container";

export type OutputParse = "text" | "json";

export type PortType =
  | "string"
  | "number"
  | "boolean"
  | "json"
  | "binary"
  | "file"
  | "stream";

export interface PortDef {
  name: string;
  type: PortType;
  required: boolean;
}

export interface ConfigFieldDef {
  name: string;
  label: string;
  type: ConfigFieldType;
  default?: unknown;
  required: boolean;
}

export interface ExecutionSpec {
  backend: ExecutionBackend;
  command: string[];
  stdinTemplate?: string | null;
  env: Record<string, string>;
  timeoutMs?: number | null;
  outputParse: OutputParse;
  outputMap: Record<string, string>;
}

export interface NodeType {
  id: string;
  name: string;
  category: Category;
  tags: string[];
  icon: string;
  description: string;
  inputs: PortDef[];
  outputs: PortDef[];
  config: ConfigFieldDef[];
  execution: ExecutionSpec;
}

export interface Pos {
  x: number;
  y: number;
}

export interface RetryPolicy {
  maxAttempts: number;
  backoffMs: number;
  backoffStrategy: "exponential" | "linear" | "fixed";
  retryOn: "error" | "timeout" | "both";
}

export interface Node {
  id: string;
  type: string;
  name: string;
  config: Record<string, unknown>;
  pos: Pos;
  timeoutMs?: number | null;
  retry?: RetryPolicy | null;
  continueOnError: boolean;
}

export type EdgeType = "forward" | "loop";

export interface Edge {
  id: string;
  fromNodeId: string;
  fromPort: string;
  toNodeId: string;
  toPort: string;
  edgeType: EdgeType;
  maxIterations?: number | null;
  branch?: string | null;
}

export type Trigger =
  | { type: "manual" }
  | { type: "schedule"; cron: string; vars?: Record<string, string> }
  | {
      type: "file-watch";
      paths: string[];
      debounceMs?: number;
      vars?: Record<string, string>;
    }
  | { type: "webhook"; secretToken?: string };

export interface Workflow {
  id: string;
  name: string;
  schemaVersion: number;
  createdAt?: string | null;
  updatedAt?: string | null;
  variables: Record<string, string>;
  triggers: Trigger[];
  nodes: Node[];
  edges: Edge[];
}

/**
 * Non-fatal lint emitted by the engine's workflow loader. The `kind`
 * is the snake-case discriminant (`loopback_url_in_remote_env`, plus
 * `unknown` for forward-compat with `#[non_exhaustive]` engine
 * variants); render `message` verbatim.
 */
export interface WorkflowWarningIpc {
  nodeId: string;
  kind: string;
  message: string;
}

/** Envelope returned by `load_workflow`. */
export interface LoadWorkflowResultIpc {
  workflow: Workflow;
  warnings: WorkflowWarningIpc[];
}

// ─── DTOs that only exist at the Tauri boundary ─────────────────

export interface SavedWorkflow {
  id: string;
  name: string;
  triggersCount: number;
  nodesCount: number;
  /** Dominant category from contained nodes — used for card accent. */
  category?: Category;
  /** Optional one-liner shown under the card title. */
  description?: string;
}

export interface RunStarted {
  runId: string;
}

export interface RunRow {
  runId: string;
  workflowId: string;
  status: "running" | "done" | "error" | "stopped";
  startedAt: number;
  finishedAt: number | null;
  durationMs: number | null;
  triggerKind: string;
}

export interface NodeRunRow {
  nodeId: string;
  iteration: number;
  attempt: number;
  nodeType: string;
  status: string;
  startedAt: number | null;
  finishedAt: number | null;
  durationMs: number | null;
  error: string | null;
}

export interface RunDetail extends RunRow {
  nodeRuns: NodeRunRow[];
}

export interface SecretMeta {
  name: string;
}

export interface Workspace {
  id: string;
  name: string;
  path: string;
}

export interface ModelEndpoint {
  id: string;
  name: string;
  baseUrl: string;
  apiKeySecret: string | null;
}

export interface Settings {
  theme: "dark" | "light";
  paletteSide: "left" | "right";
  edgeStyle: "bezier" | "orthogonal" | "straight";
  density: "comfortable" | "rich";
  grid: "dots" | "lines" | "off";
  colorScheme: "jewel" | "citrus" | "glacier";
  maxConcurrentRuns: number;
  retentionDays: number;
  modelEndpoints: ModelEndpoint[];
}

export interface EndpointStatus {
  id: string;
  name: string;
  state: "ok" | "down" | "unknown";
}

export interface SystemStatus {
  runsDbBytes: number;
  workspacesBytes: number;
  engineVersion: string;
  endpoints: EndpointStatus[];
}

// ─── Environment / EnvSpec IPC ───────────────────────────────────
//
// Mirrors `crates/desktop/src/dto.rs` (`EnvSnapshotIpc` family). The
// engine's `env_registry` + per-env `ResourceCatalog`s are summarised
// into a flat env list; each row carries its kind, lifecycle state,
// and the resources the latest probe observed.

/** Broad env category — matches `EnvKindIpc` (snake_case on the wire). */
export type EnvKindIpc = "local" | "wsl_distro" | "ssh" | "container";

/** Reachability / lifecycle state for the IPC envelope. */
export type EnvStateIpc =
  | { state: "reachable" }
  | { state: "probing" }
  | { state: "unreachable"; reason: string }
  | { state: "disabled" };

/** Outcome of a single resource probe. */
export type ResourceStateIpc =
  | { state: "found" }
  | { state: "not_found" }
  | { state: "skipped"; reason: string }
  | { state: "timed_out" }
  | { state: "probe_failed"; reason: string };

/** How the route was reached. Snake-case enum on the wire. */
export type RouteOriginIpc =
  | "env_loopback"
  | "host_direct"
  | "forwarded_tunnel"
  | "container_bridge";

/** One probed resource as the GUI consumes it. */
export interface EnvResourceIpc {
  id: string;
  /**
   * Probe kind serialised by the engine as a free-form string (e.g.
   * `"http_endpoint"`, `"binary"`, `"toolchain"`). Treated as opaque
   * by the UI — surface the value verbatim where helpful.
   */
  kind: string;
  state: ResourceStateIpc;
  baseUrl: string | null;
  version: string | null;
  routeOrigin: RouteOriginIpc | null;
}

/** One env's view as the GUI consumes it. */
export interface EnvEntryIpc {
  /** Stable env id (`local`, `wsl:Ubuntu`, `custom:dev`, …). */
  id: string;
  label: string;
  kind: EnvKindIpc;
  enabled: boolean;
  state: EnvStateIpc;
  resources: EnvResourceIpc[];
}

/** Snapshot returned by every `environment_*` command. */
export interface EnvSnapshotIpc {
  envs: EnvEntryIpc[];
}

/**
 * Payload accepted by `environment_add`. `spec` is the raw JSON form
 * of `ordius_engine::environment::runtime::EnvSpec` — the desktop
 * crate parses it server-side so the wire shape stays opaque here.
 */
export interface EnvAddIpc {
  id: string;
  label: string;
  enabled: boolean;
  spec: unknown;
}

/**
 * Payload accepted by `addEnvironmentResource`. `definition` is the
 * raw JSON form of `ordius_engine::environment::runtime::ResourceDefinition`
 * — the engine parses it server-side so the wire shape stays opaque
 * here. Set `overrideLowerScope: true` on the inner JSON when the new
 * resource shadows a built-in id.
 */
export interface EnvAddResourceIpc {
  envId: string;
  /**
   * Raw JSON form of `ResourceDefinition`. Must include `id`, `kind`,
   * `probe`, `advertisedCapabilities`, and optionally
   * `overrideLowerScope: true` when shadowing a built-in.
   */
  definition: unknown;
}

// ─── Host-direct verification ────────────────────────────────────
// Mirrors `crates/desktop/src/dto.rs` (`HostDirectTestResultIpc` and
// `HostDirectVerificationIpc`). The wizard fires `testHostDirect`
// first, then commits the response into `enableHostDirect` after the
// user confirms.

/** Outcome of `testHostDirect`. */
export interface HostDirectTestResultIpc {
  /** `true` only when 2xx AND a fingerprint was extracted. */
  success: boolean;
  /** Status code; `null` on transport error / timeout. */
  statusCode: number | null;
  /** Base URL the probe was issued against. */
  hostUrl: string;
  /** Probe route path appended to `hostUrl`. */
  probeRoutePath: string;
  /** Stable fingerprint derived from the response body; `null` on failure. */
  stableFingerprint: string | null;
  /** Up to ~2 KB of response body (truncated). `null` on failure or non-UTF-8. */
  responseExcerpt: string | null;
  /** Populated when `success` is `false`. */
  error: string | null;
}

/**
 * Verification record sent to `enableHostDirect`. The desktop crate
 * re-renames keys through `JsonCamel` before persisting, so the wire
 * form here is `camelCase`.
 */
export interface HostDirectVerificationIpc {
  /** ISO-8601 timestamp captured when the user committed the record. */
  verifiedAt: string;
  /** Verification method tag. */
  method:
    | "wsl_mirrored_networking"
    | "explicit_rebind_to_all_interfaces"
    | "user_asserted_no_verification";
  /** Host URL the wizard verified. */
  hostUrl: string;
  /** Probe route path that produced the fingerprint. */
  probeRoutePath: string;
  /** Stable fingerprint captured during the test. */
  stableFingerprint: string;
  /** `JSONPath` expressions used to recompute the fingerprint on refresh. */
  recomputeJsonpaths: string[];
}

// ─── Resource picker definitions ─────────────────────────────────
// Mirrors `crates/desktop/src/dto.rs` (`EnvDefinitionListIpc` family).
// The workflow editor's Resource Picker needs full capability + scope
// info that `EnvResourceIpc` strips, so this is a separate endpoint.

/**
 * Probe outcome flattened for the wire. `unknown` represents a cache
 * miss (no catalog entry for the resource at all), distinct from
 * `not_found` which means the probe ran and found nothing.
 */
export type ResourceProbeOutcomeIpc =
  | { outcome: "found" }
  | { outcome: "not_found" }
  | { outcome: "skipped"; reason: string }
  | { outcome: "timed_out" }
  | { outcome: "probe_failed"; reason: string }
  | { outcome: "unknown" };

/**
 * One resource definition + its current probe outcome, scoped to an
 * `(envId, workflowId?)` context.
 */
export interface EnvDefinitionIpc {
  /** Stable resource id (matches the engine's `ResourceDefinition::id`). */
  id: string;
  /** Probe kind. */
  kind: "http_endpoint" | "binary" | "toolchain";
  /** Scope where the definition was declared. */
  scope: "builtin" | "user_global" | "env_local" | "workflow";
  /** Capabilities the definition advertises (snake-case wire strings). */
  advertisedCapabilities: string[];
  /** Capabilities the latest probe proved. Subset of `advertisedCapabilities`. */
  provenCapabilities: string[];
  outcome: ResourceProbeOutcomeIpc;
  /**
   * Route origin when outcome is `found` AND kind is `http_endpoint`;
   * `null` otherwise.
   */
  routeOrigin: RouteOriginIpc | null;
  /** Base URL when outcome is `found` AND kind is `http_endpoint`. */
  baseUrl: string | null;
  /** Version string when the probe captured one. */
  version: string | null;
}

/** Listing returned by `listEnvironmentDefinitions(envId, workflowId?)`. */
export interface EnvDefinitionListIpc {
  envId: string;
  workflowId: string | null;
  /** Registry revision captured at snapshot time, for cache invalidation. */
  registryRevision: number;
  /**
   * One row per resource visible to `(envId, workflowId?)`. Order
   * matches the engine's `visible_to` precedence (highest scope first).
   */
  definitions: EnvDefinitionIpc[];
}

export interface RunWorkflowArgs {
  workflowId: string;
  variables?: Record<string, string>;
  workspaceId?: string | null;
  autoResume?: boolean;
}

// One frame of the run streaming channel. Discriminator is `type`
// matching the engine's wire tag (`workflow:started`, `node:done`,
// etc.). Other fields land via serde flatten.
export type RunEventType =
  | "workflow:started"
  | "workflow:done"
  | "workflow:error"
  | "workflow:stopped"
  | "node:started"
  | "node:output"
  | "node:done"
  | "node:error"
  | "node:skipped"
  | "node:retry"
  | "node:loop"
  | "node:paused"
  | "node:resumed"
  // Synthesized by the desktop crate when the broadcast subscriber
  // falls behind; payload carries `dropped: number`.
  | "stream:lagged";

export interface RunEvent {
  type: RunEventType;
  seq: number;
  emittedAt: number;
  runId: string;
  nodeId?: string;
  iteration?: number;
  attempt?: number;
  // Flattened payload — varies per event type. Common keys:
  //  workflow:started → workflowId, workflowName, triggerKind
  //  node:started     → nodeType, startedAt
  //  node:done        → finishedAt, durationMs
  //  node:output      → channel ("stdout"/"stderr"/"llm"), text
  //  node:error       → error
  //  node:retry       → prevError, nextAttempt
  [key: string]: unknown;
}
