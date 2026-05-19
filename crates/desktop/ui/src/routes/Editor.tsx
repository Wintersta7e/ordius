// Editor route — workflow authoring centerpiece.
//
// Phase 1.5a wires the shell: top bar, tab strip, three-column
// palette / canvas / properties layout, and the editor/run mode
// toggle. Canvas content + palette items + properties form live
// in subsequent sub-phases (1.5b/c/d). For now each column shows
// a placeholder consistent with the design tokens so the layout
// can be reviewed visually.

import { useCallback, useEffect, useState } from "react";
import type { JSX } from "react";

import {
  type Workflow,
  loadWorkflow,
  listWorkflows,
} from "../engine";
import { EditorTopBar, type EditorMode } from "../components/chrome/EditorTopBar";
import {
  WorkflowTabStrip,
  type WorkflowTab,
} from "../components/chrome/WorkflowTabStrip";
import { StatusRibbon } from "../components/home/StatusRibbon";
import type { Route } from "../lib/router";

interface Props {
  workflowId: string | undefined;
  theme: "dark" | "light";
  onThemeToggle: () => void;
  onNavigate: (route: Route) => void;
}

export function Editor({
  workflowId,
  theme,
  onThemeToggle,
  onNavigate,
}: Props): JSX.Element {
  const [mode, setMode] = useState<EditorMode>("editor");
  const [tabs, setTabs] = useState<WorkflowTab[]>([]);
  const [activeId, setActiveId] = useState<string | null>(workflowId ?? null);
  const [workflow, setWorkflow] = useState<Workflow | null>(null);
  const [error, setError] = useState<string | null>(null);

  const insideTauri =
    typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;

  useEffect(() => {
    if (!workflowId) return;
    if (!insideTauri) {
      setError(
        "running in browser preview · engine commands disabled — launch via `tauri dev` to open real workflows",
      );
      // Synthesise a placeholder tab so the chrome has something
      // to render even in browser-preview mode.
      setTabs((existing) => {
        if (existing.some((t) => t.id === workflowId)) return existing;
        return [
          ...existing,
          { id: workflowId, name: workflowId, dirty: false, running: false },
        ];
      });
      setActiveId(workflowId);
      return;
    }
    let cancelled = false;
    void (async () => {
      try {
        const wf = await loadWorkflow(workflowId);
        if (cancelled) return;
        setWorkflow(wf);
        setTabs((existing) => {
          if (existing.some((t) => t.id === wf.id)) return existing;
          return [
            ...existing,
            { id: wf.id, name: wf.name, dirty: false, running: false },
          ];
        });
        setActiveId(wf.id);
        setError(null);
      } catch (e: unknown) {
        if (!cancelled) setError(String(e));
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [workflowId, insideTauri]);

  // No tabs at all → ask the engine for the first available workflow
  // so the editor isn't visually empty on entry.
  useEffect(() => {
    if (tabs.length > 0 || workflowId) return;
    if (!insideTauri) return;
    let cancelled = false;
    void (async () => {
      try {
        const wfs = await listWorkflows();
        if (cancelled || wfs.length === 0) return;
        const first = wfs[0];
        if (first) {
          onNavigate({ kind: "editor", workflowId: first.id });
        }
      } catch {
        // ignored — empty state is fine here.
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [tabs.length, workflowId, insideTauri, onNavigate]);

  const handleActivate = useCallback((id: string) => {
    setActiveId(id);
  }, []);

  const handleClose = useCallback(
    (id: string) => {
      setTabs((existing) => {
        const next = existing.filter((t) => t.id !== id);
        if (id === activeId) {
          setActiveId(next[next.length - 1]?.id ?? null);
        }
        return next;
      });
    },
    [activeId],
  );

  const handleNewTab = useCallback(() => {
    // Real new-workflow flow lands in Phase 1.5 final tasks.
    onNavigate({ kind: "editor" });
  }, [onNavigate]);

  const placeholderMessage =
    activeId == null
      ? "no workflow open — pick one from Home or hit + to start blank"
      : workflow
        ? `canvas for ${workflow.name} (${workflow.nodes.length} nodes) — Phase 1.5b`
        : `loading ${activeId}…`;

  return (
    <div
      style={{
        display: "grid",
        gridTemplateRows: "44px 30px 1fr 22px",
        height: "100vh",
        minHeight: 720,
        background: "var(--bg)",
      }}
    >
      <EditorTopBar
        mode={mode}
        onModeChange={setMode}
        theme={theme}
        onThemeToggle={onThemeToggle}
        running={false}
        onRun={() => console.warn("run dialog lands in Phase 1.9", activeId)}
        onStop={() => console.warn("stop wired in Phase 1.6", activeId)}
        onSave={() => console.warn("save wired in Phase 1.5e", activeId)}
        onValidate={() => console.warn("validate wired in Phase 1.5e", activeId)}
        onNavigate={onNavigate}
      />
      <WorkflowTabStrip
        tabs={tabs}
        activeId={activeId}
        onActivate={handleActivate}
        onClose={handleClose}
        onNew={handleNewTab}
      />

      <main
        style={{
          display: "grid",
          gridTemplateColumns: "220px 1fr 320px",
          minHeight: 0,
          overflow: "hidden",
        }}
      >
        {/* Palette placeholder */}
        <aside
          style={{
            background: "var(--bg-panel)",
            borderRight: "1px solid var(--line)",
            display: "flex",
            flexDirection: "column",
            minHeight: 0,
          }}
        >
          <ColumnHeader label="palette" suffix="phase 1.5c" />
          <div
            style={{
              flex: 1,
              padding: 14,
              fontFamily: "var(--mono)",
              fontSize: 11,
              color: "var(--txt-faint)",
              lineHeight: 1.55,
            }}
          >
            categorised node-type list lands here. drag onto canvas or
            click to drop at viewport centre.
          </div>
        </aside>

        {/* Canvas placeholder */}
        <section
          style={{
            background: "var(--bg-canvas)",
            position: "relative",
            backgroundImage:
              "radial-gradient(var(--grid-minor) 1px, transparent 1px)",
            backgroundSize: "24px 24px",
            backgroundPosition: "12px 12px",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            overflow: "hidden",
            minWidth: 0,
          }}
        >
          {/* Corner registration marks (the canvas's identity) */}
          <Corner pos="tl" />
          <Corner pos="tr" />
          <Corner pos="bl" />
          <Corner pos="br" />

          {error ? (
            <div
              style={{
                maxWidth: 540,
                padding: "12px 14px",
                fontFamily: "var(--mono)",
                fontSize: 11,
                color: "var(--warn)",
                background: "var(--bg-canvas)",
                border: "1px dashed var(--line)",
                borderRadius: 3,
              }}
            >
              <span style={{ color: "var(--warn)" }}>! </span>
              {error}
            </div>
          ) : (
            <div
              style={{
                textAlign: "center",
                fontFamily: "var(--mono)",
                fontSize: 12,
                color: "var(--txt-faint)",
                maxWidth: 460,
              }}
            >
              <div
                style={{
                  fontSize: 14,
                  color: "var(--txt-dim)",
                  marginBottom: 8,
                  letterSpacing: "0.04em",
                }}
              >
                {placeholderMessage}
              </div>
              <div>
                ┌ CANVAS ─────────── phase 1.5b ─┐ <br />
                │ pan + zoom · render nodes/edges │ <br />
                │ pin ports · loop edges · grid   │ <br />
                └─────────────────────────────────┘
              </div>
            </div>
          )}
        </section>

        {/* Properties placeholder */}
        <aside
          style={{
            background: "var(--bg-panel)",
            borderLeft: "1px solid var(--line)",
            display: "flex",
            flexDirection: "column",
            minHeight: 0,
          }}
        >
          <ColumnHeader label="properties" suffix="phase 1.5d" />
          <div
            style={{
              flex: 1,
              padding: 14,
              fontFamily: "var(--mono)",
              fontSize: 11,
              color: "var(--txt-faint)",
              lineHeight: 1.55,
            }}
          >
            datasheet panel for the selected node, or workflow
            properties when nothing is selected. pin tables + per-config
            field renderers from the engine's NodeType.config spec.
          </div>
        </aside>
      </main>

      <StatusRibbon
        workflowCount={tabs.length}
        runCount={0}
        tail={`mode: ${mode} · ${activeId ?? "no workflow"}`}
      />
    </div>
  );
}

function ColumnHeader({
  label,
  suffix,
}: {
  label: string;
  suffix?: string;
}): JSX.Element {
  return (
    <div
      style={{
        padding: "10px 14px",
        borderBottom: "1px solid var(--line-soft)",
        display: "flex",
        alignItems: "center",
        gap: 8,
        background: "var(--bg-elevated)",
      }}
    >
      <span style={{ color: "var(--accent)", fontSize: 12 }}>┌</span>
      <span
        style={{
          fontFamily: "var(--mono)",
          fontSize: 10,
          fontWeight: 700,
          color: "var(--txt)",
          letterSpacing: "0.18em",
          textTransform: "uppercase",
        }}
      >
        {label}
      </span>
      <div style={{ flex: 1 }} />
      {suffix ? (
        <span
          className="num"
          style={{
            fontFamily: "var(--mono)",
            fontSize: 9.5,
            color: "var(--txt-faint)",
          }}
        >
          {suffix}
        </span>
      ) : null}
    </div>
  );
}

function Corner({ pos }: { pos: "tl" | "tr" | "bl" | "br" }): JSX.Element {
  const at = {
    tl: { top: 8, left: 8 },
    tr: { top: 8, right: 8, transform: "scaleX(-1)" },
    bl: { bottom: 8, left: 8, transform: "scaleY(-1)" },
    br: { bottom: 8, right: 8, transform: "scale(-1, -1)" },
  } as const;
  return (
    <svg
      width="14"
      height="14"
      viewBox="0 0 14 14"
      style={{ position: "absolute", ...at[pos] }}
      aria-hidden="true"
    >
      <polyline
        points="1 1 1 6 6 6 6 1 1 1"
        fill="none"
        stroke="var(--grid-major)"
        strokeWidth="1.2"
      />
      <circle cx="3.5" cy="3.5" r="1.2" fill="var(--grid-major)" />
    </svg>
  );
}
