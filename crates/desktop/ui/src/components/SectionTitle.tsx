// Bracketed section header: `├ WORKFLOWS · 12 saved ──────── filter ┤`
// — the canonical chrome rule the design handoff calls "bracketed
// ASCII section headers".

import type { JSX, ReactNode } from "react";

interface Props {
  label: string;
  count?: string;
  right?: ReactNode;
}

export function SectionTitle({ label, count, right }: Props): JSX.Element {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 10,
        padding: "0 2px",
      }}
    >
      <span style={{ color: "var(--accent)" }}>├</span>
      <span
        style={{
          fontFamily: "var(--mono)",
          fontSize: 11,
          fontWeight: 700,
          color: "var(--txt)",
          letterSpacing: "0.18em",
          textTransform: "uppercase",
        }}
      >
        {label}
      </span>
      {count !== undefined ? (
        <span
          className="num"
          style={{
            fontFamily: "var(--mono)",
            fontSize: 10,
            color: "var(--txt-faint)",
          }}
        >
          {count}
        </span>
      ) : null}
      <span
        style={{
          flex: 1,
          height: 1,
          background:
            "linear-gradient(90deg, var(--line) 0%, var(--line-soft) 100%)",
        }}
      />
      {right}
    </div>
  );
}
