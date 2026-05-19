// Editor + Home top bar.
//
// Phase 1.4 wires the Home variant: wordmark, page label, history /
// settings / theme buttons on the right.

import type { JSX } from "react";

import { OrdiusWordmark } from "../Wordmark";
import { Ic } from "../icons";

interface Props {
  /** Label rendered after the wordmark divider (e.g. "home"). */
  pageLabel: string;
  /** Current theme; toggled by the rightmost button. */
  theme: "dark" | "light";
  /** Called when the user clicks the theme toggle. */
  onThemeToggle: () => void;
}

export function TopBar({ pageLabel, theme, onThemeToggle }: Props): JSX.Element {
  return (
    <header
      style={{
        height: 44,
        display: "grid",
        gridTemplateColumns: "1fr auto 1fr",
        alignItems: "center",
        background: "var(--bg-elevated)",
        borderBottom: "1px solid var(--line)",
        padding: "0 12px",
        gap: 12,
      }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: 14 }}>
        <OrdiusWordmark size="md" />
        <span style={{ color: "var(--line)" }}>│</span>
        <span
          style={{
            fontSize: 12.5,
            color: "var(--txt)",
            fontWeight: 500,
            letterSpacing: "0.08em",
            textTransform: "uppercase",
          }}
        >
          {pageLabel}
        </span>
      </div>
      <div />
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          justifyContent: "flex-end",
        }}
      >
        <button
          type="button"
          className="btn ghost icon"
          title="Run history"
          aria-label="Open run history"
        >
          {Ic["log"]?.({ size: 14 })}
        </button>
        <button
          type="button"
          className="btn ghost icon"
          title="Settings"
          aria-label="Open settings"
        >
          {Ic["cog"]?.({ size: 14 })}
        </button>
        <button
          type="button"
          className="btn ghost icon"
          title={`Switch to ${theme === "dark" ? "light" : "dark"} mode`}
          aria-label="Toggle theme"
          onClick={onThemeToggle}
        >
          {theme === "dark" ? Ic["moon"]?.({ size: 14 }) : Ic["sun"]?.({ size: 14 })}
        </button>
      </div>
    </header>
  );
}
