import type { RunRow } from "../engine/types";

const MIN = 60 * 1000;
const HOUR = 60 * MIN;
const DAY = 24 * HOUR;

interface Seed {
  workflowId: string;
  status: RunRow["status"];
  startOffsetMs: number;
  durationMs: number | null;
  triggerKind: string;
}

const SEEDS: Seed[] = [
  { workflowId: "wf-critique", status: "done", startOffsetMs: 8 * MIN, durationMs: 5180, triggerKind: "manual" },
  { workflowId: "wf-bakeoff", status: "running", startOffsetMs: 12 * MIN, durationMs: null, triggerKind: "manual" },
  { workflowId: "wf-rag", status: "done", startOffsetMs: 45 * MIN, durationMs: 2410, triggerKind: "cli" },
  { workflowId: "wf-critique", status: "error", startOffsetMs: HOUR, durationMs: 4720, triggerKind: "manual" },
  { workflowId: "wf-nightly", status: "done", startOffsetMs: 2 * HOUR, durationMs: 184000, triggerKind: "schedule" },
  { workflowId: "wf-rag", status: "done", startOffsetMs: 3 * HOUR, durationMs: 2110, triggerKind: "cli" },
  { workflowId: "wf-critique", status: "done", startOffsetMs: 4 * HOUR, durationMs: 5040, triggerKind: "manual" },
  { workflowId: "wf-bakeoff", status: "stopped", startOffsetMs: 5 * HOUR, durationMs: 12410, triggerKind: "manual" },
  { workflowId: "wf-nightly", status: "error", startOffsetMs: 7 * HOUR, durationMs: 9280, triggerKind: "schedule" },
  { workflowId: "wf-rag", status: "done", startOffsetMs: 1 * DAY + 30 * MIN, durationMs: 2050, triggerKind: "cli" },
  { workflowId: "wf-build", status: "done", startOffsetMs: 1 * DAY + 2 * HOUR, durationMs: 31000, triggerKind: "manual" },
  { workflowId: "wf-digest", status: "done", startOffsetMs: 1 * DAY + 4 * HOUR, durationMs: 18020, triggerKind: "schedule" },
  { workflowId: "wf-critique", status: "done", startOffsetMs: 1 * DAY + 6 * HOUR, durationMs: 4870, triggerKind: "manual" },
  { workflowId: "wf-rag", status: "done", startOffsetMs: 2 * DAY + 1 * HOUR, durationMs: 2230, triggerKind: "cli" },
  { workflowId: "wf-nightly", status: "done", startOffsetMs: 2 * DAY + 3 * HOUR, durationMs: 174500, triggerKind: "schedule" },
];

export function demoHistoryRuns(now: number): RunRow[] {
  return SEEDS.map((s, i) => {
    const startedAt = now - s.startOffsetMs;
    const finishedAt =
      s.durationMs != null ? startedAt + s.durationMs : null;
    return {
      runId: `run_${String(28 - i).padStart(5, "0")}`,
      workflowId: s.workflowId,
      status: s.status,
      startedAt,
      finishedAt,
      durationMs: s.durationMs,
      triggerKind: s.triggerKind,
    };
  });
}
