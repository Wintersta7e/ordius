// Live run viewer panel.
//
// Sits on the right of the editor when mode === 'run'. Shows the
// streaming event log (auto-scrolling) plus a per-node status
// summary. Wraps the Tauri Channel-stream the Editor sets up.

import { useEffect, useRef } from "react";
import type { JSX } from "react";

import type { RunEvent } from "../../engine/types";
import { BracketHeader } from "../palette/BracketHeader";
import { fmtDuration } from "../../lib/format";

export type NodeRunStatus =
  | "pending"
  | "running"
  | "done"
  | "error"
  | "skipped";

export interface LiveRunState {
  runId: string | null;
  startedAt: number | null;
  finishedAt: number | null;
  status: "running" | "done" | "error" | "stopped" | null;
  statusByNode: Record<string, NodeRunStatus>;
  activeEdges: Set<string>;
  traveledEdges: Set<string>;
  events: RunEvent[];
}

export function emptyRunState(): LiveRunState {
  return {
    runId: null,
    startedAt: null,
    finishedAt: null,
    status: null,
    statusByNode: {},
    activeEdges: new Set(),
    traveledEdges: new Set(),
    events: [],
  };
}

/** Fold a new event into the live run state. */
export function reduceRunEvent(state: LiveRunState, event: RunEvent): LiveRunState {
  const next: LiveRunState = {
    ...state,
    events: [...state.events, event],
  };
  switch (event.type) {
    case "workflow:started":
      next.runId = event.runId;
      next.startedAt = event.emittedAt;
      next.status = "running";
      next.statusByNode = {};
      next.activeEdges = new Set();
      next.traveledEdges = new Set();
      break;
    case "workflow:done":
      next.status = "done";
      next.finishedAt = event.emittedAt;
      break;
    case "workflow:error":
      next.status = "error";
      next.finishedAt = event.emittedAt;
      break;
    case "workflow:stopped":
      next.status = "stopped";
      next.finishedAt = event.emittedAt;
      break;
    case "node:started":
      if (event.nodeId) {
        next.statusByNode = {
          ...state.statusByNode,
          [event.nodeId]: "running",
        };
      }
      break;
    case "node:done":
      if (event.nodeId) {
        next.statusByNode = {
          ...state.statusByNode,
          [event.nodeId]: "done",
        };
      }
      break;
    case "node:error":
      if (event.nodeId) {
        next.statusByNode = {
          ...state.statusByNode,
          [event.nodeId]: "error",
        };
      }
      break;
    case "node:skipped":
      if (event.nodeId) {
        next.statusByNode = {
          ...state.statusByNode,
          [event.nodeId]: "skipped",
        };
      }
      break;
    default:
      break;
  }
  return next;
}

const STATUS_COLOR: Record<string, string> = {
  done: "var(--ok)",
  error: "var(--err)",
  running: "var(--info)",
  stopped: "var(--warn)",
  pending: "var(--txt-faint)",
  skipped: "var(--txt-faint)",
};

interface Props {
  state: LiveRunState;
  onStop: () => void;
}

export function RunPanel({ state, onStop }: Props): JSX.Element {
  const logRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const el = logRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [state.events.length]);

  const elapsed =
    state.startedAt != null
      ? (state.finishedAt ?? Date.now()) - state.startedAt
      : null;

  return (
    <div
      style={{
        height: "100%",
        display: "flex",
        flexDirection: "column",
        background: "var(--bg-panel)",
        borderLeft: "1px solid var(--line)",
        minHeight: 0,
      }}
    >
      <BracketHeader
        label="run"
        suffix={state.runId ? state.runId.slice(0, 8) : "idle"}
      />

      {/* Header summary */}
      <div
        style={{
          padding: "12px 14px",
          borderBottom: "1px solid var(--line-soft)",
          display: "flex",
          flexDirection: "column",
          gap: 6,
          fontFamily: "var(--mono)",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <span
            style={{
              width: 8,
              height: 8,
              borderRadius: 8,
              background:
                state.status != null ? STATUS_COLOR[state.status] : "var(--line)",
              boxShadow:
                state.status === "running"
                  ? "0 0 8px var(--info)"
                  : "none",
              animation:
                state.status === "running"
                  ? "pulse 1.1s ease-in-out infinite"
                  : undefined,
            }}
          />
          <span
            style={{
              fontSize: 11,
              fontWeight: 600,
              letterSpacing: "0.08em",
              textTransform: "uppercase",
              color:
                state.status != null
                  ? STATUS_COLOR[state.status]
                  : "var(--txt-faint)",
            }}
          >
            {state.status ?? "no run"}
          </span>
          <div style={{ flex: 1 }} />
          {state.status === "running" ? (
            <button
              type="button"
              className="btn"
              style={{
                color: "var(--err)",
                borderColor: "var(--err)",
                height: 22,
                padding: "0 10px",
              }}
              onClick={onStop}
            >
              stop
            </button>
          ) : null}
        </div>
        {elapsed != null ? (
          <div
            className="num"
            style={{
              fontSize: 11,
              color: "var(--txt-dim)",
            }}
          >
            elapsed · {fmtDuration(elapsed)}
          </div>
        ) : (
          <div style={{ fontSize: 11, color: "var(--txt-faint)" }}>
            press <span style={{ color: "var(--accent)" }}>▶ run</span> to start
          </div>
        )}
      </div>

      {/* Event log */}
      <div
        ref={logRef}
        style={{
          flex: 1,
          overflow: "auto",
          padding: "8px 0",
          fontFamily: "var(--mono)",
          fontSize: 10.5,
          minHeight: 0,
        }}
      >
        {state.events.length === 0 ? (
          <div
            style={{
              padding: "14px 14px",
              color: "var(--txt-faint)",
              fontSize: 11,
            }}
          >
            no events yet.
          </div>
        ) : (
          state.events.map((event) => (
            <EventRow key={`${event.runId}-${event.seq}`} event={event} />
          ))
        )}
      </div>
    </div>
  );
}

function EventRow({ event }: { event: RunEvent }): JSX.Element {
  const color = eventColor(event.type);
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "44px 1fr",
        gap: 8,
        padding: "3px 14px",
        borderBottom: "1px solid var(--line-soft)",
        lineHeight: 1.4,
      }}
    >
      <span
        className="num"
        style={{
          color: "var(--txt-faint)",
          fontSize: 9.5,
        }}
        title={new Date(event.emittedAt).toISOString()}
      >
        +{(event.seq + "").padStart(2, "0")}
      </span>
      <span style={{ color: "var(--txt)" }}>
        <span style={{ color, fontWeight: 600 }}>{event.type}</span>
        {event.nodeId ? (
          <>
            {" "}
            <span style={{ color: "var(--accent)" }}>{event.nodeId}</span>
          </>
        ) : null}
        {summariseEvent(event)}
      </span>
    </div>
  );
}

function eventColor(type: string): string {
  if (type === "node:done" || type === "workflow:done") return "var(--ok)";
  if (type === "node:error" || type === "workflow:error") return "var(--err)";
  if (type === "node:retry") return "var(--warn)";
  if (type === "node:output") return "var(--accent)";
  if (type === "workflow:stopped" || type === "stream:lagged") return "var(--warn)";
  if (type === "node:started" || type === "workflow:started") return "var(--info)";
  return "var(--txt-dim)";
}

function summariseEvent(event: RunEvent): string {
  switch (event.type) {
    case "workflow:started": {
      const wf = event["workflowId"];
      return wf ? ` · ${String(wf)}` : "";
    }
    case "node:done": {
      const dur = event["durationMs"];
      return dur != null ? ` · ${String(dur)}ms` : "";
    }
    case "node:error": {
      const err = event["error"];
      return err ? ` · ${String(err).slice(0, 80)}` : "";
    }
    case "node:output": {
      const text = event["text"];
      const channel = event["channel"];
      if (text == null) return "";
      const snippet = String(text).slice(0, 80);
      return ` · ${channel ? `[${String(channel)}] ` : ""}${snippet}`;
    }
    case "node:retry": {
      const attempt = event["nextAttempt"];
      return attempt != null ? ` · attempt ${String(attempt)}` : "";
    }
    case "stream:lagged": {
      const dropped = event["dropped"];
      return dropped != null
        ? ` · ${String(dropped)} dropped — refresh from history for accuracy`
        : "";
    }
    default:
      return "";
  }
}
