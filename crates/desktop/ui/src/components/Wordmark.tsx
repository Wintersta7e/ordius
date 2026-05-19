// Ordius brand mark + wordmark.
//
// The mark is a tiny DAG inside a rounded frame: filled source
// node, outline target node, single diagonal edge. The frame
// doubles as a node-card silhouette — the product is "graphs
// inside frames", literally.

import type { JSX } from "react";

interface MarkProps {
  size?: number;
  color?: string;
  accent?: string;
}

export function OrdiusMark({
  size = 22,
  color = "currentColor",
  accent,
}: MarkProps): JSX.Element {
  const a = accent ?? color;
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      aria-hidden="true"
    >
      <rect
        x="2"
        y="2"
        width="20"
        height="20"
        rx="2"
        stroke={color}
        strokeWidth="1.5"
      />
      <line
        x1="7.5"
        y1="7.5"
        x2="16.5"
        y2="16.5"
        stroke={color}
        strokeWidth="1.5"
        strokeLinecap="round"
      />
      <circle cx="7.5" cy="7.5" r="2.6" fill={a} />
      <circle
        cx="16.5"
        cy="16.5"
        r="2.0"
        fill="var(--bg-elevated)"
        stroke={color}
        strokeWidth="1.5"
      />
    </svg>
  );
}

interface WordmarkProps {
  size?: "sm" | "md" | "lg";
  running?: boolean;
}

export function OrdiusWordmark({
  size = "md",
  running = false,
}: WordmarkProps): JSX.Element {
  const heights = { sm: 14, md: 17, lg: 24 } as const;
  const px = heights[size];
  const markSize = Math.round(px * 1.25);
  return (
    <div
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: px * 0.55,
      }}
    >
      <OrdiusMark size={markSize} accent="var(--accent)" />
      <span
        style={{
          fontFamily: "var(--mono)",
          fontWeight: 700,
          fontSize: px,
          letterSpacing: "0.16em",
          lineHeight: 1,
          color: "var(--txt)",
        }}
      >
        ORDIUS
      </span>
      {running ? (
        <span
          style={{
            width: 7,
            height: 7,
            borderRadius: 7,
            background: "var(--ok)",
            boxShadow: "0 0 10px var(--ok)",
            marginLeft: 4,
            animation: "pulse 1.1s ease-in-out infinite",
          }}
        />
      ) : null}
    </div>
  );
}
