// Editor + Run-mode top bar.
//
// Three columns: brand on the left, mode tabs in the centre,
// action buttons on the right. Different from the Home TopBar
// because the Editor needs Save/Validate/Run plus the
// Editor/Run mode toggle that switches between authoring and
// live-run-viewer panels.

import type { JSX } from "react";

import { OrdiusWordmark } from "../Wordmark";
import { Ic } from "../icons";
import { WorkspaceSelector } from "./WorkspaceSelector";
import type { Route } from "../../lib/router";
import type { Workspace } from "../../engine/types";

export type EditorMode = "editor" | "run";

interface Props {
  mode: EditorMode;
  onModeChange: (mode: EditorMode) => void;
  theme: "dark" | "light";
  onThemeToggle: () => void;
  /** True while a run is active for the current workflow. */
  running: boolean;
  onRun: () => void;
  onStop: () => void;
  onSave: () => void;
  onValidate: () => void;
  onNavigate: (route: Route) => void;
  /** Workspaces the user has registered. */
  workspaces: Workspace[];
  /** Currently selected workspace id (null = home). */
  workspaceId: string | null;
  onWorkspaceChange: (id: string | null) => void;
  /** Called after the user creates a workspace inline so the parent reloads. */
  onWorkspacesChanged?: () => void;
}

export function EditorTopBar({
  mode,
  onModeChange,
  theme,
  onThemeToggle,
  running,
  onRun,
  onStop,
  onSave,
  onValidate,
  onNavigate,
  workspaces,
  workspaceId,
  onWorkspaceChange,
  onWorkspacesChanged,
}: Props): JSX.Element {
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
        <OrdiusWordmark size="md" running={running} />
        <span style={{ color: "var(--line)" }}>│</span>
        <button
          type="button"
          className="btn ghost"
          style={{ height: 24, padding: "0 8px", fontSize: 11 }}
          title="Home"
          onClick={() => onNavigate({ kind: "home" })}
        >
          <span style={{ color: "var(--txt-faint)" }}>⌂</span> home
        </button>
        <span style={{ color: "var(--line)" }}>│</span>
        <WorkspaceSelector
          workspaces={workspaces}
          workspaceId={workspaceId}
          onChange={onWorkspaceChange}
          {...(onWorkspacesChanged ? { onWorkspacesChanged } : {})}
          onOpenManage={() => onNavigate({ kind: "settings" })}
        />
      </div>

      <div
        style={{
          display: "flex",
          alignItems: "center",
          padding: 2,
          gap: 1,
          background: "var(--bg)",
          border: "1px solid var(--line)",
          borderRadius: 4,
        }}
      >
        <ModeTab
          id="editor"
          current={mode}
          onClick={onModeChange}
          label="editor"
        />
        {running ? (
          <ModeTab
            id="run"
            current={mode}
            onClick={onModeChange}
            label="run"
            badge="●"
          />
        ) : (
          <ModeTab
            id="run"
            current={mode}
            onClick={onModeChange}
            label="run"
          />
        )}
      </div>

      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 6,
          justifyContent: "flex-end",
        }}
      >
        <button
          type="button"
          className="btn"
          title="Validate workflow"
          onClick={onValidate}
        >
          {Ic["check"]?.({ size: 12 })} validate
        </button>
        <button
          type="button"
          className="btn"
          title="Save (⌘S)"
          onClick={onSave}
        >
          {Ic["save"]?.({ size: 12 })} save
        </button>
        <Sep />
        {running ? (
          <button
            type="button"
            className="btn"
            title="Stop run"
            onClick={onStop}
            style={{
              color: "var(--err)",
              borderColor: "var(--err)",
            }}
          >
            {Ic["stop"]?.({ size: 12 })} stop
          </button>
        ) : (
          <button
            type="button"
            className="btn primary"
            title="Run (⌘R)"
            onClick={onRun}
          >
            {Ic["play"]?.({ size: 12 })} run
          </button>
        )}
        <Sep />
        <button
          type="button"
          className="btn ghost icon"
          title="Run history"
          onClick={() => onNavigate({ kind: "history" })}
        >
          {Ic["log"]?.({ size: 14 })}
        </button>
        <button
          type="button"
          className="btn ghost icon"
          title="Settings"
          onClick={() => onNavigate({ kind: "settings" })}
        >
          {Ic["cog"]?.({ size: 14 })}
        </button>
        <button
          type="button"
          className="btn ghost icon"
          title="Toggle theme"
          onClick={onThemeToggle}
        >
          {theme === "dark"
            ? Ic["moon"]?.({ size: 14 })
            : Ic["sun"]?.({ size: 14 })}
        </button>
      </div>
    </header>
  );
}

function Sep(): JSX.Element {
  return (
    <span
      style={{
        width: 1,
        alignSelf: "stretch",
        background: "var(--line)",
        margin: "4px 2px",
      }}
    />
  );
}

interface ModeTabProps {
  id: EditorMode;
  current: EditorMode;
  onClick: (id: EditorMode) => void;
  label: string;
  badge?: string;
}

function ModeTab({
  id,
  current,
  onClick,
  label,
  badge,
}: ModeTabProps): JSX.Element {
  const active = id === current;
  return (
    <button
      type="button"
      onClick={() => onClick(id)}
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 5,
        border: 0,
        background: active ? "var(--bg-elevated)" : "transparent",
        color: active ? "var(--txt)" : "var(--txt-dim)",
        fontFamily: "var(--mono)",
        fontSize: 11.5,
        fontWeight: active ? 600 : 400,
        padding: "5px 14px",
        borderRadius: 3,
        cursor: "pointer",
        height: 26,
      }}
    >
      {label}
      {badge ? (
        <span
          style={{
            color: "var(--info)",
            fontSize: 8,
            lineHeight: 1,
          }}
        >
          {badge}
        </span>
      ) : null}
    </button>
  );
}

