// Typed wrappers around the 18 Tauri commands `crates/desktop/src/commands.rs`
// registers. Every screen calls into here rather than touching
// `@tauri-apps/api` directly — that way command renames are caught
// by tsc and the rest of the codebase doesn't import any Tauri
// internals.

import { invoke, Channel } from "@tauri-apps/api/core";
import type {
  EnvAddIpc,
  EnvSnapshotIpc,
  LoadWorkflowResultIpc,
  NodeType,
  RunDetail,
  RunEvent,
  RunRow,
  RunStarted,
  RunWorkflowArgs,
  SavedWorkflow,
  SecretMeta,
  Settings,
  SystemStatus,
  Workflow,
  Workspace,
} from "./types";

// ─── Workflows ───────────────────────────────────────────────────

export function listWorkflows(): Promise<SavedWorkflow[]> {
  return invoke("list_workflows");
}

export function loadWorkflow(id: string): Promise<LoadWorkflowResultIpc> {
  return invoke("load_workflow", { id });
}

export function saveWorkflow(workflow: Workflow): Promise<void> {
  return invoke("save_workflow", { workflow });
}

export function validateWorkflow(workflow: Workflow): Promise<void> {
  return invoke("validate_workflow", { workflow });
}

export function duplicateWorkflow(id: string): Promise<Workflow> {
  return invoke("duplicate_workflow", { id });
}

export function deleteWorkflow(id: string): Promise<boolean> {
  return invoke("delete_workflow", { id });
}

// ─── Runs ────────────────────────────────────────────────────────

export interface RunHandle {
  /** New run id. */
  runId: string;
  /** Streaming events for this run. Already wired to the backend. */
  events: Channel<RunEvent>;
}

/**
 * Start a workflow run. Returns the new id plus the event channel
 * the backend will stream events through. The frontend should
 * attach `.onmessage` to the channel BEFORE calling `runWorkflow`
 * — Tauri channels buffer messages until a handler attaches, so
 * the leading `workflow:started` event survives the round-trip.
 */
export async function runWorkflow(
  args: RunWorkflowArgs,
  onEvent: (ev: RunEvent) => void,
): Promise<RunHandle> {
  const channel = new Channel<RunEvent>();
  channel.onmessage = onEvent;
  const { runId } = await invoke<RunStarted>("run_workflow", { args, channel });
  return { runId, events: channel };
}

/**
 * Deliver an event payload to a `wait_event` node parked in this run.
 * Returns true if the payload was delivered to an active waiter.
 */
export function deliverEvent(
  runId: string,
  event: string,
  payload: unknown,
): Promise<boolean> {
  return invoke("deliver_event", { runId, event, payload });
}

export function stopRun(runId: string): Promise<boolean> {
  return invoke("stop_run", { runId });
}

export function listRuns(
  filters: {
    workflow?: string;
    status?: string;
    limit?: number;
  } = {},
): Promise<RunRow[]> {
  return invoke("list_runs", filters);
}

export function getRun(runId: string): Promise<RunDetail> {
  return invoke("get_run", { runId });
}

// ─── Nodes ───────────────────────────────────────────────────────

export function listNodeTypes(): Promise<NodeType[]> {
  return invoke("list_node_types");
}

// ─── Workspaces ──────────────────────────────────────────────────

export function listWorkspaces(): Promise<Workspace[]> {
  return invoke("list_workspaces");
}

export function addWorkspace(name: string, path: string): Promise<Workspace> {
  return invoke("add_workspace", { name, path });
}

export function removeWorkspace(id: string): Promise<void> {
  return invoke("remove_workspace", { id });
}

export function renameWorkspace(id: string, name: string): Promise<Workspace> {
  return invoke("rename_workspace", { id, name });
}

// ─── Secrets ─────────────────────────────────────────────────────

export function listSecrets(): Promise<SecretMeta[]> {
  return invoke("list_secrets");
}

export function addSecret(name: string, value: string): Promise<void> {
  return invoke("add_secret", { name, value });
}

export function removeSecret(name: string): Promise<void> {
  return invoke("remove_secret", { name });
}

// ─── Settings ────────────────────────────────────────────────────

export function getSettings(): Promise<Settings> {
  return invoke("get_settings");
}

export function setSettings(settings: Settings): Promise<void> {
  return invoke("set_settings", { settings });
}

// ─── System status ───────────────────────────────────────────────

export function systemStatus(): Promise<SystemStatus> {
  return invoke("system_status");
}

// ─── Environment ─────────────────────────────────────────────────

/** List every registered env (active + disabled) with its latest probe. */
export function listEnvironments(): Promise<EnvSnapshotIpc> {
  return invoke("environment_list");
}

/**
 * Re-probe one env (`envId = "..."`) or every enabled env
 * (`envId` omitted). Returns the pre-probe snapshot; the probe runs in
 * the background and lands on a subsequent `listEnvironments()`.
 */
export function refreshEnvironment(
  envId?: string,
): Promise<EnvSnapshotIpc> {
  return invoke("environment_refresh", { envId });
}

/** Insert a new env spec and schedule its initial probe. */
export function addEnvironment(spec: EnvAddIpc): Promise<EnvSnapshotIpc> {
  return invoke("environment_add", { spec });
}

/** Delete an env spec and tear down its registry + catalog entry. */
export function removeEnvironment(envId: string): Promise<EnvSnapshotIpc> {
  return invoke("environment_remove", { envId });
}

/** Toggle the `enabled` flag on an env spec. */
export function setEnvironmentEnabled(
  envId: string,
  enabled: boolean,
): Promise<EnvSnapshotIpc> {
  return invoke("environment_set_enabled", { envId, enabled });
}
