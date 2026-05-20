// One row of open-document tabs across the top of the editor.
//
// Click a tab to switch, × to close, "+" to open a new blank
// workflow. Active tab gets the accent strip + cyan glow; running
// tabs get a pulsing info dot; dirty tabs get a warn dot.

import { useState } from "react";
import type { JSX } from "react";

export interface WorkflowTab {
  /** Workflow id (also the tab key). */
  id: string;
  /** Display name. */
  name: string;
  /** Has unsaved changes. */
  dirty: boolean;
  /** Workflow is currently running. */
  running: boolean;
}

interface Props {
  tabs: WorkflowTab[];
  activeId: string | null;
  onActivate: (id: string) => void;
  onClose: (id: string) => void;
  onNew: () => void;
}

export function WorkflowTabStrip({
  tabs,
  activeId,
  onActivate,
  onClose,
  onNew,
}: Props): JSX.Element {
  return (
    <div
      style={{
        height: 30,
        display: "flex",
        alignItems: "stretch",
        background: "var(--bg)",
        borderBottom: "1px solid var(--line)",
        paddingRight: 8,
        flexShrink: 0,
        overflow: "hidden",
      }}
    >
      {tabs.map((tab) => (
        <Tab
          key={tab.id}
          tab={tab}
          active={tab.id === activeId}
          onClick={() => onActivate(tab.id)}
          onClose={() => onClose(tab.id)}
        />
      ))}
      <button
        type="button"
        onClick={onNew}
        title="New workflow"
        style={{
          appearance: "none",
          border: 0,
          background: "transparent",
          width: 30,
          color: "var(--txt-faint)",
          cursor: "pointer",
          fontFamily: "var(--mono)",
          fontSize: 16,
          lineHeight: 1,
          display: "inline-flex",
          alignItems: "center",
          justifyContent: "center",
        }}
      >
        +
      </button>
      <div style={{ flex: 1 }} />
    </div>
  );
}

interface TabProps {
  tab: WorkflowTab;
  active: boolean;
  onClick: () => void;
  onClose: () => void;
}

function Tab({ tab, active, onClick, onClose }: TabProps): JSX.Element {
  const [hover, setHover] = useState(false);
  return (
    <button
      type="button"
      role="tab"
      aria-selected={active}
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      onClick={onClick}
      style={{
        position: "relative",
        display: "inline-flex",
        alignItems: "center",
        gap: 8,
        padding: "0 14px",
        height: "100%",
        minWidth: 140,
        maxWidth: 240,
        cursor: "pointer",
        background: active
          ? "var(--bg-elevated)"
          : hover
            ? "var(--bg-hover)"
            : "transparent",
        color: active ? "var(--txt)" : "var(--txt-dim)",
        fontFamily: "var(--mono)",
        fontSize: 11.5,
        border: "none",
        borderRight: "1px solid var(--line-soft)",
        textAlign: "left",
      }}
    >
      {active ? (
        <div
          style={{
            position: "absolute",
            top: 0,
            left: 0,
            right: 0,
            height: 2,
            background: "var(--accent)",
            boxShadow: "0 0 6px var(--accent)",
          }}
        />
      ) : null}
      <span
        style={{
          width: 8,
          height: 8,
          flexShrink: 0,
          borderRadius: 8,
          background: tab.running
            ? "var(--info)"
            : tab.dirty
              ? "var(--warn)"
              : active
                ? "var(--ok)"
                : "var(--line-strong)",
          boxShadow: tab.running ? "0 0 8px var(--info)" : "none",
          animation: tab.running ? "pulse 1.1s ease-in-out infinite" : undefined,
        }}
      />
      <span
        style={{
          flex: 1,
          minWidth: 0,
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
          fontWeight: active ? 500 : 400,
        }}
      >
        {tab.name}
      </span>
      {hover || active ? (
        <button
          type="button"
          aria-label={`close ${tab.name}`}
          onClick={(event) => {
            event.stopPropagation();
            onClose();
          }}
          title="Close"
          style={{
            width: 16,
            height: 16,
            borderRadius: 2,
            display: "inline-flex",
            alignItems: "center",
            justifyContent: "center",
            color: "var(--txt-faint)",
            fontSize: 14,
            lineHeight: 1,
            background: "transparent",
            border: "none",
            padding: 0,
            cursor: "pointer",
          }}
        >
          ×
        </button>
      ) : null}
    </button>
  );
}
