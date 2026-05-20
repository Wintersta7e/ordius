// Single-line warning banner used across the route shells (and the
// editor canvas overlay) to surface the browser-preview notice or
// other transient errors. Two visual variants — inline (full-width
// block sat in the document flow) and overlay (absolutely
// positioned floater used inside the editor canvas).

import type { CSSProperties, JSX } from "react";

interface Props {
  message: string;
  variant?: "inline" | "overlay";
  /** When true, the user can interact with the banner (eg copy text). */
  interactive?: boolean;
  /** `warn` (default) renders a `!` accent; `ok` renders a `✓` accent. */
  tone?: "warn" | "ok";
}

export function NoticeBanner({
  message,
  variant = "inline",
  interactive = false,
  tone = "warn",
}: Props): JSX.Element {
  const accent = tone === "ok" ? "var(--ok)" : "var(--warn)";
  const glyph = tone === "ok" ? "✓ " : "! ";
  const baseStyle: CSSProperties = {
    padding: "8px 12px",
    fontFamily: "var(--mono)",
    fontSize: 11,
    color: accent,
    background: "var(--bg-canvas)",
    border: "1px dashed var(--line)",
    borderRadius: 3,
  };

  if (variant === "overlay") {
    return (
      <div
        style={{
          ...baseStyle,
          position: "absolute",
          top: 10,
          left: 10,
          right: 10,
          pointerEvents: interactive ? "auto" : "none",
        }}
      >
        <span style={{ color: accent }}>{glyph}</span>
        {message}
      </div>
    );
  }

  return (
    <div style={{ ...baseStyle, marginBottom: 18 }}>
      <span style={{ color: accent }}>{glyph}</span>
      {message}
    </div>
  );
}
