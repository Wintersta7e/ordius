// Single-line warning banner used across the route shells (and the
// editor canvas overlay) to surface the browser-preview notice or
// other transient errors. Two visual variants — inline (full-width
// block sat in the document flow) and overlay (absolutely
// positioned floater used inside the editor canvas).
//
// `kind` selects the accent color tier (info / warn / error). For
// back-compat with the earlier `tone` prop, omitting `kind` falls
// back to `tone` (`"warn"` default, `"ok"` for success). New call
// sites should prefer `kind`; the editor's load-warning stack uses
// it directly.

import type { CSSProperties, JSX } from "react";

export type NoticeKind = "info" | "warn" | "error";

export interface NoticeBannerProps {
  message: string;
  /** Accent tier. Overrides `tone` when set. Defaults to `"warn"`
   * via the legacy `tone` fallback so existing call sites are
   * unaffected. */
  kind?: NoticeKind;
  /** Short label rendered as a mono chip before the message —
   * used by the editor's load-warning stack to surface the node id. */
  title?: string;
  /** Renders an inline dismiss `×` button when provided. The
   * dismissal is intentionally session-local; reload re-surfaces
   * the warning. */
  onDismiss?: () => void;
  variant?: "inline" | "overlay";
  /** When true, the user can interact with the banner (eg copy text). */
  interactive?: boolean;
  /** Legacy fallback when `kind` is unset. `warn` renders a `!`
   * accent; `ok` renders a `✓` accent. Prefer `kind` for new code. */
  tone?: "warn" | "ok";
}

export function NoticeBanner({
  message,
  kind,
  title,
  onDismiss,
  variant = "inline",
  interactive = false,
  tone = "warn",
}: NoticeBannerProps): JSX.Element {
  const resolved: NoticeKind | "ok" = kind ?? (tone === "ok" ? "ok" : "warn");
  const accent =
    resolved === "ok"
      ? "var(--ok)"
      : resolved === "info"
        ? "var(--info)"
        : resolved === "error"
          ? "var(--err)"
          : "var(--warn)";
  const glyph =
    resolved === "ok"
      ? "✓ "
      : resolved === "info"
        ? "i "
        : resolved === "error"
          ? "✕ "
          : "! ";
  // Faint accent-tinted background so warn/error/info banners read
  // as distinct tiers without inventing new tokens. Falls back to
  // `--bg-canvas` for the legacy `tone`-driven path.
  const softBg = `color-mix(in srgb, ${accent} 8%, var(--bg-canvas))`;
  const baseStyle: CSSProperties = {
    padding: "8px 12px",
    fontFamily: "var(--mono)",
    fontSize: 11,
    color: accent,
    background: softBg,
    border: `1px solid ${accent}`,
    borderRadius: 3,
    display: "flex",
    alignItems: "center",
    gap: 8,
  };

  const body = (
    <>
      <span style={{ color: accent, flex: "0 0 auto" }}>{glyph}</span>
      {title ? (
        <span
          style={{
            padding: "1px 6px",
            border: `1px solid ${accent}`,
            borderRadius: 2,
            color: accent,
            fontWeight: 600,
            letterSpacing: "0.02em",
            flex: "0 0 auto",
          }}
        >
          {title}
        </span>
      ) : null}
      <span style={{ flex: "1 1 auto", color: "var(--txt)" }}>{message}</span>
      {onDismiss ? (
        <button
          type="button"
          aria-label="Dismiss"
          onClick={onDismiss}
          style={{
            flex: "0 0 auto",
            background: "transparent",
            border: "none",
            color: accent,
            cursor: "pointer",
            fontFamily: "var(--mono)",
            fontSize: 13,
            lineHeight: 1,
            padding: "2px 6px",
          }}
        >
          ×
        </button>
      ) : null}
    </>
  );

  if (variant === "overlay") {
    return (
      <div
        style={{
          ...baseStyle,
          position: "absolute",
          top: 10,
          left: 10,
          right: 10,
          pointerEvents: interactive || onDismiss ? "auto" : "none",
        }}
      >
        {body}
      </div>
    );
  }

  return <div style={{ ...baseStyle, marginBottom: 8 }}>{body}</div>;
}
