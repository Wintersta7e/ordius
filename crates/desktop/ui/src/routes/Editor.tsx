// Editor route — workflow authoring centerpiece.
//
// Phase 1.5a wired the chrome shell. Phase 1.5b adds the working
// canvas: pan/zoom, dot grid, render nodes/edges from the loaded
// workflow, single-click select, drag-to-move. Palette + properties
// land in 1.5c / 1.5d.

import { useCallback, useEffect, useState } from "react";
import type { JSX } from "react";

import {
  type NodeType,
  type Workflow,
  listNodeTypes,
  listWorkflows,
  loadWorkflow,
} from "../engine";
import { Canvas } from "../components/canvas";
import {
  DEMO_NODE_TYPES,
  DEMO_WORKFLOW,
} from "../components/canvas/demoWorkflow";
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
  const [nodeTypes, setNodeTypes] = useState<NodeType[]>([]);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const insideTauri =
    typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;

  // Load node-type catalog once (under Tauri) — used by the Canvas
  // for port lookup and category tinting.
  useEffect(() => {
    if (!insideTauri) {
      setNodeTypes(DEMO_NODE_TYPES);
      return;
    }
    let cancelled = false;
    void (async () => {
      try {
        const types = await listNodeTypes();
        if (!cancelled) setNodeTypes(types);
      } catch (e: unknown) {
        if (!cancelled) setError(String(e));
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [insideTauri]);

  // Load the workflow when workflowId changes.
  useEffect(() => {
    if (!workflowId) return;
    if (!insideTauri) {
      setError(
        "running in browser preview · engine commands disabled — launch via `tauri dev` to open real workflows",
      );
      // Fall back to a baked-in demo workflow so the canvas has
      // something to render for visual review.
      setWorkflow(DEMO_WORKFLOW);
      setTabs((existing) => {
        if (existing.some((t) => t.id === DEMO_WORKFLOW.id)) return existing;
        return [
          ...existing,
          {
            id: DEMO_WORKFLOW.id,
            name: DEMO_WORKFLOW.name,
            dirty: false,
            running: false,
          },
        ];
      });
      setActiveId(DEMO_WORKFLOW.id);
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

  // No tabs yet → ask the engine for the first available workflow
  // so the editor lands on something instead of empty.
  useEffect(() => {
    if (tabs.length > 0 || workflowId) return;
    if (!insideTauri) {
      // In browser preview, surface the demo workflow.
      onNavigate({ kind: "editor", workflowId: DEMO_WORKFLOW.id });
      return;
    }
    let cancelled = false;
    void (async () => {
      try {
        const wfs = await listWorkflows();
        if (cancelled || wfs.length === 0) return;
        const first = wfs[0];
        if (first) onNavigate({ kind: "editor", workflowId: first.id });
      } catch {
        /* empty workflow list is fine */
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [tabs.length, workflowId, insideTauri, onNavigate]);

  const handleActivate = useCallback((id: string) => setActiveId(id), []);

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
    onNavigate({ kind: "editor" });
  }, [onNavigate]);

  const handleMoveNode = useCallback(
    (id: string, pos: { x: number; y: number }) => {
      setWorkflow((wf) => {
        if (!wf) return wf;
        return {
          ...wf,
          nodes: wf.nodes.map((n) => (n.id === id ? { ...n, pos } : n)),
        };
      });
      setTabs((existing) =>
        existing.map((t) =>
          t.id === activeId ? { ...t, dirty: true } : t,
        ),
      );
    },
    [activeId],
  );

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
        {/* Palette placeholder — Phase 1.5c */}
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

        {/* Canvas — Phase 1.5b */}
        <section style={{ position: "relative", minWidth: 0 }}>
          {workflow ? (
            <Canvas
              workflow={workflow}
              nodeTypes={nodeTypes}
              selectedId={selectedNodeId}
              onSelect={setSelectedNodeId}
              onMoveNode={handleMoveNode}
              density="standard"
              edgeStyle="orthogonal"
            />
          ) : (
            <div
              style={{
                position: "absolute",
                inset: 0,
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                background: "var(--bg-canvas)",
                color: "var(--txt-faint)",
                fontFamily: "var(--mono)",
                fontSize: 12,
              }}
            >
              {activeId == null
                ? "no workflow open — pick one from Home or hit + to start blank"
                : `loading ${activeId}…`}
            </div>
          )}
          {error ? (
            <div
              style={{
                position: "absolute",
                top: 10,
                left: 10,
                right: 10,
                padding: "8px 12px",
                fontFamily: "var(--mono)",
                fontSize: 11,
                color: "var(--warn)",
                background: "var(--bg-canvas)",
                border: "1px dashed var(--line)",
                borderRadius: 3,
                pointerEvents: "none",
              }}
            >
              <span style={{ color: "var(--warn)" }}>! </span>
              {error}
            </div>
          ) : null}
        </section>

        {/* Properties placeholder — Phase 1.5d */}
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
            {selectedNodeId ? (
              <>
                selected node:{" "}
                <span style={{ color: "var(--accent)" }}>{selectedNodeId}</span>
                <br />
                datasheet panel + per-config field renderers land in 1.5d.
              </>
            ) : (
              <>
                datasheet panel for the selected node, or workflow
                properties when nothing is selected. pin tables + per-
                config field renderers from the engine's NodeType.config
                spec.
              </>
            )}
          </div>
        </aside>
      </main>

      <StatusRibbon
        workflowCount={tabs.length}
        runCount={0}
        tail={`mode: ${mode} · ${activeId ?? "no workflow"} · ${
          workflow?.nodes.length ?? 0
        }n ${workflow?.edges.length ?? 0}e`}
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
