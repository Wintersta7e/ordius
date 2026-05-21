// One tile in the Home grid: category stripe + name + last-run
// badge + trigger pills + node/edge counts + workspace hint.

import { useState } from "react";
import type { JSX } from "react";

import type { Category } from "../../engine/types";
import { catColor } from "../../data/categories";
import { Ic } from "../icons";
import { fmtAgo, fmtDuration } from "../../lib/format";

interface LastRun {
  status: "running" | "done" | "error" | "stopped";
  startedAt: number;
  durationMs: number | null;
}

export interface WorkflowCardData {
  id: string;
  name: string;
  /**
   * Subtitle / description. The engine doesn't model this today
   * (workflows have an id + display name only); the home card
   * derives it as `${triggerCount} triggers · ${nodeCount} nodes`
   * until a description field lands in v1.1+.
   */
  desc: string;
  category: Category;
  triggerKinds: string[];
  nodeCount: number;
  /** Cross-referenced against `listRuns()`; may be null on never-run. */
  lastRun: LastRun | null;
  /** Total persisted runs for this workflow. */
  totalRuns: number;
}

interface Props {
  workflow: WorkflowCardData;
  onOpen: (id: string) => void;
  onRun: (id: string) => void;
  /** Optional. Caller provides the confirmation + IPC call; the
   * card surfaces only the affordance. */
  onDelete?: (id: string) => void;
  /** Optional. Caller handles the IPC + opens the clone. */
  onDuplicate?: (id: string) => void;
}

const STATUS_COLOR: Record<string, string> = {
  done: "var(--ok)",
  error: "var(--err)",
  running: "var(--info)",
  stopped: "var(--warn)",
};

const TRIGGER_GLYPH: Record<string, string> = {
  manual: "●",
  cli: "$",
  gui: "▸",
  schedule: "⏱",
  webhook: "↯",
  "file-watch": "◫",
};

export function WorkflowCard({
  workflow,
  onOpen,
  onRun,
  onDelete,
  onDuplicate,
}: Props): JSX.Element {
  const [hover, setHover] = useState(false);
  const w = workflow;
  const base = catColor(w.category, "base");
  const tint = catColor(w.category, "tint");
  const border = catColor(w.category, "border");
  const glow = catColor(w.category, "glow");
  const status = w.lastRun?.status ?? null;
  const statusColor = status ? STATUS_COLOR[status] : "var(--line)";
  const isRunning = status === "running";

  const handleClick = (event: React.MouseEvent) => {
    event.preventDefault();
    onOpen(w.id);
  };

  return (
    <a
      href="#editor"
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      onClick={handleClick}
      style={{
        display: "block",
        textDecoration: "none",
        position: "relative",
        background: "var(--bg-panel)",
        border: `1px solid ${hover ? border : "var(--line)"}`,
        borderRadius: 3,
        padding: "14px 16px 12px",
        color: "inherit",
        transition: "border-color .15s, box-shadow .15s, transform .15s",
        boxShadow: hover ? `0 12px 30px -16px ${glow}` : "none",
        cursor: "pointer",
        overflow: "hidden",
      }}
    >
      {/* Category stripe top */}
      <div
        style={{
          position: "absolute",
          top: 0,
          left: 0,
          right: 0,
          height: 2,
          background: base,
          boxShadow: isRunning
            ? `0 0 10px ${glow}`
            : hover
              ? `0 0 8px ${glow}`
              : "none",
        }}
      />

      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <span
          style={{
            width: 20,
            height: 20,
            flexShrink: 0,
            display: "inline-flex",
            alignItems: "center",
            justifyContent: "center",
            background: tint,
            border: `1px solid ${border}`,
            borderRadius: 2,
            color: base,
            fontFamily: "var(--mono)",
            fontSize: 11,
            fontWeight: 700,
          }}
        >
          {w.category[0]?.toUpperCase()}
        </span>
        <span
          style={{
            flex: 1,
            minWidth: 0,
            fontFamily: "var(--mono)",
            fontSize: 13.5,
            fontWeight: 600,
            color: "var(--txt)",
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
          }}
        >
          {w.name}
        </span>
        <button
          type="button"
          onClick={(event) => {
            event.preventDefault();
            event.stopPropagation();
            onRun(w.id);
          }}
          title="Run now"
          style={{
            appearance: "none",
            border: 0,
            width: 26,
            height: 26,
            borderRadius: 2,
            background: hover ? "var(--accent)" : "var(--bg-input)",
            color: hover ? "var(--btn-primary-fg)" : "var(--txt-dim)",
            display: "inline-flex",
            alignItems: "center",
            justifyContent: "center",
            cursor: "pointer",
            transition: "background .15s, color .15s",
          }}
        >
          {Ic["play"]?.({ size: 10 })}
        </button>
        {onDuplicate ? (
          <button
            type="button"
            onClick={(event) => {
              event.preventDefault();
              event.stopPropagation();
              onDuplicate(w.id);
            }}
            title="Duplicate workflow"
            aria-label={`Duplicate workflow ${w.name}`}
            style={{
              appearance: "none",
              border: 0,
              width: 22,
              height: 22,
              borderRadius: 2,
              background: "transparent",
              color: hover ? "var(--accent)" : "var(--txt-faint)",
              display: "inline-flex",
              alignItems: "center",
              justifyContent: "center",
              cursor: "pointer",
              transition: "color .15s",
              fontFamily: "var(--mono)",
              fontSize: 13,
            }}
          >
            ⎘
          </button>
        ) : null}
        {onDelete ? (
          <button
            type="button"
            onClick={(event) => {
              event.preventDefault();
              event.stopPropagation();
              onDelete(w.id);
            }}
            title="Delete workflow"
            aria-label={`Delete workflow ${w.name}`}
            style={{
              appearance: "none",
              border: 0,
              width: 22,
              height: 22,
              borderRadius: 2,
              background: "transparent",
              color: hover ? "var(--err)" : "var(--txt-faint)",
              display: "inline-flex",
              alignItems: "center",
              justifyContent: "center",
              cursor: "pointer",
              transition: "color .15s",
            }}
          >
            {Ic["x"]?.({ size: 12 })}
          </button>
        ) : null}
      </div>

      <p
        style={{
          margin: "8px 0 12px",
          fontSize: 11.5,
          lineHeight: 1.5,
          color: "var(--txt-dim)",
          height: 32,
          overflow: "hidden",
          display: "-webkit-box",
          WebkitLineClamp: 2,
          WebkitBoxOrient: "vertical",
        }}
      >
        {w.desc}
      </p>

      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          padding: "8px 10px",
          marginBottom: 8,
          background: "var(--bg-canvas)",
          border: "1px solid var(--line-soft)",
          borderRadius: 2,
          fontFamily: "var(--mono)",
          fontSize: 11,
        }}
      >
        <span
          style={{
            width: 7,
            height: 7,
            borderRadius: 7,
            background: statusColor,
            boxShadow: isRunning ? `0 0 8px ${statusColor}` : "none",
            animation: isRunning ? "pulse 1.1s ease-in-out infinite" : undefined,
          }}
        />
        <span
          style={{
            color: statusColor,
            fontWeight: 600,
            fontSize: 10,
            letterSpacing: "0.08em",
            textTransform: "uppercase",
          }}
        >
          {status ?? "never run"}
        </span>
        {w.lastRun ? (
          <>
            <span style={{ color: "var(--txt-faint)" }}>·</span>
            <span style={{ color: "var(--txt-dim)" }} className="num">
              {fmtAgo(w.lastRun.startedAt)}
            </span>
            <span style={{ color: "var(--txt-faint)" }}>·</span>
            <span style={{ color: "var(--txt-dim)" }} className="num">
              {fmtDuration(w.lastRun.durationMs)}
            </span>
          </>
        ) : null}
        <div style={{ flex: 1 }} />
        <span style={{ color: "var(--txt-faint)" }} className="num">
          {w.totalRuns} runs
        </span>
      </div>

      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 10,
          fontFamily: "var(--mono)",
          fontSize: 10,
          color: "var(--txt-faint)",
        }}
      >
        <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
          {w.triggerKinds.length === 0 ? (
            <span
              style={{
                padding: "1px 6px",
                borderRadius: 2,
                border: "1px solid var(--line)",
                color: "var(--txt-dim)",
                fontSize: 9.5,
                letterSpacing: "0.04em",
              }}
            >
              <span style={{ color: base }}>{TRIGGER_GLYPH["manual"]}</span>{" "}
              manual
            </span>
          ) : (
            w.triggerKinds.map((trigger, i) => (
              <span
                key={`${trigger}-${i}`}
                style={{
                  padding: "1px 6px",
                  borderRadius: 2,
                  border: "1px solid var(--line)",
                  color: "var(--txt-dim)",
                  fontSize: 9.5,
                  letterSpacing: "0.04em",
                }}
              >
                <span style={{ color: base }}>{TRIGGER_GLYPH[trigger] ?? "·"}</span>{" "}
                {trigger}
              </span>
            ))
          )}
        </div>
        <div style={{ flex: 1 }} />
        <span className="num">{w.nodeCount}n</span>
      </div>
    </a>
  );
}

interface NewProps {
  onClick: () => void;
}

/** Dashed "+" tile that opens a blank workflow in the editor. */
export function NewWorkflowCard({ onClick }: NewProps): JSX.Element {
  const [hover, setHover] = useState(false);
  return (
    <a
      href="#new"
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      onClick={(event) => {
        event.preventDefault();
        onClick();
      }}
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        gap: 10,
        textDecoration: "none",
        background: hover ? "var(--bg-panel)" : "transparent",
        border: `1px dashed ${hover ? "var(--accent)" : "var(--line)"}`,
        borderRadius: 3,
        padding: 24,
        minHeight: 180,
        color: hover ? "var(--txt)" : "var(--txt-dim)",
        transition: "all .15s",
        cursor: "pointer",
      }}
    >
      <div
        style={{
          width: 36,
          height: 36,
          borderRadius: 2,
          display: "inline-flex",
          alignItems: "center",
          justifyContent: "center",
          background: hover ? "var(--accent)" : "var(--bg-input)",
          border: `1px solid ${hover ? "var(--accent)" : "var(--line)"}`,
          color: hover ? "var(--btn-primary-fg)" : "var(--txt-dim)",
          fontSize: 20,
          fontFamily: "var(--mono)",
          transition: "all .15s",
        }}
      >
        +
      </div>
      <div style={{ fontFamily: "var(--mono)", fontSize: 12.5, fontWeight: 500 }}>
        new workflow
      </div>
      <div
        style={{
          fontSize: 10.5,
          color: "var(--txt-faint)",
          textAlign: "center",
          maxWidth: 240,
        }}
      >
        start blank, or pick a template from the editor
      </div>
    </a>
  );
}
