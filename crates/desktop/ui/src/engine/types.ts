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

export type HostPlatform = "windows" | "wsl" | "linux" | "mac-os" | "other";

export type WslState = "running" | "stopped";

export type NamespaceKind =
  | { kind: "local" }
  | { kind: "wsl-distro"; name: string; state: WslState }
  | { kind: "windows-host"; gatewayIp: string }
  | { kind: "custom"; host: string };

export type NamespaceState =
  | { state: "reachable" }
  | { state: "unreachable"; reason: string }
  | { state: "disabled" }
  | { state: "stopped" }
  | { state: "not-probeable"; reason: string };

export interface NamespaceInfo {
  id: string;
  label: string;
  kind: NamespaceKind;
  enabled: boolean;
  reachable: NamespaceState;
}

export type ReachHint =
  | "wsl-loopback-bound"
  | "windows-host-bound"
  | "custom-unreachable";

export type DiscoveredEndpoint =
  | {
      type: "direct";
      kind: string;
      name: string;
      namespaceId: string;
      callableUrl: string;
      observedUrl: string;
      coVisibleIn: string[];
    }
  | {
      type: "only-via-namespace";
      kind: string;
      name: string;
      namespaceId: string;
      observedUrl: string;
      hint: ReachHint;
      coVisibleIn: string[];
    };

export interface EnvironmentReport {
  platform: HostPlatform;
  wslDistro: string | null;
  namespaces: NamespaceInfo[];
  endpoints: DiscoveredEndpoint[];
  timedOut: boolean;
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
