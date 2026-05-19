// Bracketed section header reused across panels. `┌ NODES ── 19 types ┐`

import type { JSX } from "react";

interface Props {
  label: string;
  suffix?: string;
}

export function BracketHeader({ label, suffix }: Props): JSX.Element {
  return (
    <div
      style={{
        padding: "12px 14px 10px",
        borderBottom: "1px solid var(--line)",
        display: "flex",
        alignItems: "center",
        gap: 8,
        color: "var(--txt-faint)",
        fontFamily: "var(--mono)",
        fontSize: 10,
      }}
    >
      <span style={{ color: "var(--accent)", fontSize: 13 }}>┌</span>
      <span
        style={{
          color: "var(--txt)",
          fontWeight: 700,
          textTransform: "uppercase",
          letterSpacing: "0.20em",
          fontSize: 11,
        }}
      >
        {label}
      </span>
      <span
        style={{
          flex: 1,
          height: 1,
          alignSelf: "center",
          background:
            "linear-gradient(90deg, var(--line) 0%, var(--line-soft) 100%)",
        }}
      />
      {suffix ? (
        <span
          className="num"
          style={{ fontSize: 10, color: "var(--txt-soft)" }}
        >
          {suffix}
        </span>
      ) : null}
      <span style={{ color: "var(--accent)", fontSize: 13 }}>┐</span>
    </div>
  );
}
