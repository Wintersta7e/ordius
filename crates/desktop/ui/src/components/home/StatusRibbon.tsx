// 22px bottom ribbon: brand cell + page cell + count cell + tail.

import type { JSX, ReactNode } from "react";

interface RibbonProps {
  /** Workflow count for the count cell. */
  workflowCount: number;
  /** Run count for the count cell. */
  runCount: number;
  /** Trailing right-aligned status (e.g. "tauri 2.0 · idle ▸"). */
  tail?: string;
}

export function StatusRibbon({
  workflowCount,
  runCount,
  tail = "tauri 2 · idle ▸",
}: RibbonProps): JSX.Element {
  return (
    <footer
      style={{
        height: 22,
        display: "flex",
        alignItems: "center",
        background: "var(--bg-elevated)",
        borderTop: "1px solid var(--line)",
        fontFamily: "var(--mono)",
        fontSize: 10.5,
        color: "var(--txt-faint)",
      }}
    >
      <Cell tone="accent">◆ ordius</Cell>
      <Cell>home</Cell>
      <Cell>
        {workflowCount} workflows · {runCount} runs
      </Cell>
      <div style={{ flex: 1 }} />
      <Cell tone="muted">{tail}</Cell>
    </footer>
  );
}

interface CellProps {
  children: ReactNode;
  tone?: "accent" | "muted";
}

function Cell({ children, tone }: CellProps): JSX.Element {
  const colorMap: Record<string, string> = {
    accent: "var(--accent)",
    muted: "var(--txt-soft)",
  };
  const color = tone ? (colorMap[tone] ?? "var(--txt-dim)") : "var(--txt-dim)";
  return (
    <span
      className="num"
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 4,
        padding: "0 10px",
        height: "100%",
        borderRight: "1px solid var(--line-soft)",
        color,
      }}
    >
      {children}
    </span>
  );
}
