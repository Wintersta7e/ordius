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
  stdin_template?: string | null;
  env: Record<string, string>;
  timeout_ms?: number | null;
  output_parse: OutputParse;
  output_map: Record<string, string>;
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
  max_attempts: number;
  backoff_ms: number;
  backoff_strategy: "exponential" | "linear" | "fixed";
  retry_on: "error" | "timeout" | "both";
}

export interface Node {
  id: string;
  type: string;
  name: string;
  config: Record<string, unknown>;
  pos: Pos;
  timeout_ms?: number | null;
  retry?: RetryPolicy | null;
  continue_on_error: boolean;
}

export type EdgeType = "forward" | "loop";

export interface Edge {
  id: string;
  from_node_id: string;
  from_port: string;
  to_node_id: string;
  to_port: string;
  edge_type: EdgeType;
  max_iterations?: number | null;
  branch?: string | null;
}

export type Trigger =
  | { type: "manual" }
  | { type: "schedule"; cron: string; vars?: Record<string, string> }
  | {
      type: "file-watch";
      paths: string[];
      debounce_ms?: number;
      vars?: Record<string, string>;
    }
  | { type: "webhook"; secret_token?: string };

export interface Workflow {
  id: string;
  name: string;
  schema_version: number;
  created_at?: string | null;
  updated_at?: string | null;
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
