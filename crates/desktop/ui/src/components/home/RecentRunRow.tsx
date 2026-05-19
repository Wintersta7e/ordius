// One row in the bottom "recent runs" table on Home.

import type { JSX } from "react";
import { useState } from "react";

import type { RunRow } from "../../engine/types";
import { fmtAgo, fmtDuration } from "../../lib/format";

interface Props {
  run: RunRow;
  workflowName: string;
  last: boolean;
  now: number;
}

const STATUS_COLOR: Record<string, string> = {
  done: "var(--ok)",
  error: "var(--err)",
  running: "var(--info)",
  stopped: "var(--warn)",
};

export function RecentRunRow({ run, workflowName, last, now }: Props): JSX.Element {
  const [hover, setHover] = useState(false);
  const c = STATUS_COLOR[run.status] ?? "var(--line)";
  return (
    <div
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      style={{
        display: "grid",
        gridTemplateColumns: "24px 1.4fr 1fr 110px 90px 130px",
        gap: 10,
        alignItems: "center",
        padding: "11px 16px",
        textDecoration: "none",
        color: "inherit",
        borderBottom: last ? "none" : "1px solid var(--line-soft)",
        background: hover ? "var(--bg-hover)" : "transparent",
        fontFamily: "var(--mono)",
        fontSize: 12,
      }}
    >
      <span
        style={{
          width: 8,
          height: 8,
          borderRadius: 8,
          background: c,
          boxShadow: run.status === "running" ? `0 0 8px ${c}` : "none",
          animation:
            run.status === "running"
              ? "pulse 1.1s ease-in-out infinite"
              : undefined,
        }}
      />
      <div
        style={{
          display: "flex",
          alignItems: "baseline",
          gap: 8,
          minWidth: 0,
        }}
      >
        <span
          style={{
            color: "var(--txt)",
            fontWeight: 500,
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
          }}
        >
          {workflowName}
        </span>
        <span
          style={{
            fontSize: 9.5,
            color: c,
            letterSpacing: "0.08em",
            textTransform: "uppercase",
            fontWeight: 600,
          }}
        >
          {run.status}
        </span>
      </div>
      <div
        style={{
          fontSize: 11,
          color: "var(--txt-faint)",
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}
      >
        trigger: {run.triggerKind}
      </div>
      <span style={{ color: "var(--txt-dim)" }} className="num">
        {fmtAgo(run.startedAt, now)}
      </span>
      <span style={{ color: "var(--txt-dim)" }} className="num">
        {fmtDuration(run.durationMs)}
      </span>
      <span
        style={{
          color: "var(--txt-faint)",
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}
        className="num"
        title={run.runId}
      >
        {run.runId.length > 14 ? `${run.runId.slice(0, 12)}…` : run.runId}
      </span>
    </div>
  );
}
