// Single-line warning banner used across the route shells (and the
// editor canvas overlay) to surface the browser-preview notice or
// other transient errors. Two visual variants — inline (full-width
// block sat in the document flow) and overlay (absolutely
// positioned floater used inside the editor canvas).
//
// Two render paths gated on whether `kind` is provided:
//   - Legacy path (no `kind`): dashed-border + canvas-bg, message
//     coloured by `tone`. Used by Home / Settings / History / the
//     editor's overlay error+validate banners.
//   - Tiered path (`kind` set): accent-tinted bg, solid border, flex
//     layout with optional `title` chip + `×` dismiss button. Used
//     by the editor's workflow-warning stack (and future Phase F
//     callers).

import type { CSSProperties, JSX } from "react";

export type NoticeKind = "info" | "warn" | "error";

export interface NoticeBannerProps {
  message: string;
  /** Accent tier. When set, switches to the tiered look (solid
   * border, accent-tinted fill, optional title chip + dismiss). */
  kind?: NoticeKind;
  /** Short label rendered as a mono chip before the message —
   * used by the editor's load-warning stack to surface the node id.
   * Only rendered on the tiered path (requires `kind`). */
  title?: string;
  /** Renders an inline dismiss `×` button when provided. The
   * dismissal is intentionally session-local; reload re-surfaces
   * the warning. Only rendered on the tiered path (requires `kind`). */
  onDismiss?: () => void;
  variant?: "inline" | "overlay";
  /** When true, the user can interact with the banner (eg copy text). */
  interactive?: boolean;
  /** Legacy fallback when `kind` is unset. `warn` renders a `!`
   * accent; `ok` renders a `✓` accent. */
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
  if (kind !== undefined) {
    return renderTiered({
      message,
      kind,
      title,
      onDismiss,
      variant,
      interactive,
    });
  }
  return renderLegacy({ message, variant, interactive, tone });
}

interface LegacyArgs {
  message: string;
  variant: "inline" | "overlay";
  interactive: boolean;
  tone: "warn" | "ok";
}

function renderLegacy({
  message,
  variant,
  interactive,
  tone,
}: LegacyArgs): JSX.Element {
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

interface TieredArgs {
  message: string;
  kind: NoticeKind;
  title: string | undefined;
  onDismiss: (() => void) | undefined;
  variant: "inline" | "overlay";
  interactive: boolean;
}

function renderTiered({
  message,
  kind,
  title,
  onDismiss,
  variant,
  interactive,
}: TieredArgs): JSX.Element {
  const accent =
    kind === "info"
      ? "var(--info)"
      : kind === "error"
        ? "var(--err)"
        : "var(--warn)";
  const glyph = kind === "info" ? "i " : kind === "error" ? "✕ " : "! ";
  // Faint accent-tinted background so warn/error/info banners read
  // as distinct tiers without inventing new tokens.
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
