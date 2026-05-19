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
  type Workspace,
  listNodeTypes,
  listWorkflows,
  listWorkspaces,
  loadWorkflow,
  runWorkflow,
  saveWorkflow,
  stopRun,
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
import { Palette } from "../components/palette";
import { PropertiesPanel } from "../components/properties";
import {
  RunPanel,
  emptyRunState,
  reduceRunEvent,
  type LiveRunState,
} from "../components/run";
import { RunDialog } from "../components/dialogs";
import { NoticeBanner } from "../components/NoticeBanner";
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
  const [runState, setRunState] = useState<LiveRunState>(emptyRunState);
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [runDialogOpen, setRunDialogOpen] = useState(false);

  const insideTauri =
    typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;

  // Workspace catalog → RunDialog's workspace picker.
  useEffect(() => {
    if (!insideTauri) return;
    let cancelled = false;
    void (async () => {
      try {
        const ws = await listWorkspaces();
        if (!cancelled) setWorkspaces(ws);
      } catch {
        /* non-fatal — RunDialog falls back to engine home */
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [insideTauri]);

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

  const handlePatchNode = useCallback(
    (id: string, patch: Partial<Workflow["nodes"][number]>) => {
      setWorkflow((wf) => {
        if (!wf) return wf;
        return {
          ...wf,
          nodes: wf.nodes.map((n) => (n.id === id ? { ...n, ...patch } : n)),
        };
      });
      setTabs((existing) =>
        existing.map((t) => (t.id === activeId ? { ...t, dirty: true } : t)),
      );
    },
    [activeId],
  );

  const handlePatchWorkflow = useCallback(
    (patch: Partial<Workflow>) => {
      setWorkflow((wf) => (wf ? { ...wf, ...patch } : wf));
      setTabs((existing) =>
        existing.map((t) => (t.id === activeId ? { ...t, dirty: true } : t)),
      );
    },
    [activeId],
  );

  const handleDeleteNode = useCallback(
    (id: string) => {
      setWorkflow((wf) => {
        if (!wf) return wf;
        return {
          ...wf,
          nodes: wf.nodes.filter((n) => n.id !== id),
          edges: wf.edges.filter(
            (e) => e.from_node_id !== id && e.to_node_id !== id,
          ),
        };
      });
      setSelectedNodeId((current) => (current === id ? null : current));
      setTabs((existing) =>
        existing.map((t) => (t.id === activeId ? { ...t, dirty: true } : t)),
      );
    },
    [activeId],
  );

  // Drop a fresh node from the palette into the open workflow.
  // For Phase 1.5c the new node lands below the lowest existing
  // node so it's always visible; drag-from-palette with a
  // cursor-tracking ghost is a v1.x polish item.
  const handleRun = useCallback(() => {
    if (!workflow) return;
    setRunDialogOpen(true);
  }, [workflow]);

  const handleRunConfirm = useCallback(
    async (input: {
      variables: Record<string, string>;
      workspaceId: string | null;
      autoResume: boolean;
    }) => {
      if (!workflow) return;
      setRunDialogOpen(false);
      setRunState(emptyRunState());
      setMode("run");
      if (!insideTauri) return;
      try {
        await runWorkflow(
          {
            workflowId: workflow.id,
            variables: input.variables,
            workspaceId: input.workspaceId,
            autoResume: input.autoResume,
          },
          (event) => {
            setRunState((current) => reduceRunEvent(current, event));
          },
        );
      } catch (e: unknown) {
        setError(String(e));
      }
    },
    [workflow, insideTauri],
  );

  const handleStop = useCallback(async () => {
    if (!runState.runId) return;
    if (!insideTauri) return;
    try {
      await stopRun(runState.runId);
    } catch (e: unknown) {
      setError(String(e));
    }
  }, [runState.runId, insideTauri]);

  const handleSave = useCallback(async () => {
    if (!workflow) return;
    if (!insideTauri) {
      console.warn("save requires the Tauri host");
      return;
    }
    try {
      await saveWorkflow(workflow);
      setTabs((existing) =>
        existing.map((t) => (t.id === activeId ? { ...t, dirty: false } : t)),
      );
    } catch (e: unknown) {
      setError(String(e));
    }
  }, [workflow, activeId, insideTauri]);

  const handleAddNode = useCallback(
    (typeId: string) => {
      const nodeType = nodeTypes.find((t) => t.id === typeId);
      setWorkflow((wf) => {
        if (!wf) return wf;
        const nextId = synthesiseNodeId(typeId, wf.nodes);
        const pos = nextDropPosition(wf.nodes);
        return {
          ...wf,
          nodes: [
            ...wf.nodes,
            {
              id: nextId,
              type: typeId,
              name: nodeType?.name ?? typeId,
              config: {},
              pos,
              continue_on_error: false,
            },
          ],
        };
      });
      setTabs((existing) =>
        existing.map((t) =>
          t.id === activeId ? { ...t, dirty: true } : t,
        ),
      );
    },
    [nodeTypes, activeId],
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
        running={runState.status === "running"}
        onRun={handleRun}
        onStop={handleStop}
        onSave={handleSave}
        onValidate={() => console.warn("validate wired with engine `validate` in 1.5e")}
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
        {/* Palette — Phase 1.5c */}
        <Palette nodeTypes={nodeTypes} onAdd={handleAddNode} />

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
              runState={
                runState.status != null
                  ? {
                      statusByNode: runState.statusByNode,
                      activeEdges: runState.activeEdges,
                      traveledEdges: runState.traveledEdges,
                    }
                  : undefined
              }
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
          {error ? <NoticeBanner message={error} variant="overlay" /> : null}
        </section>

        {/* Right column — Properties in editor mode, RunPanel in run mode */}
        {mode === "run" ? (
          <RunPanel state={runState} onStop={handleStop} />
        ) : workflow ? (
          <PropertiesPanel
            workflow={workflow}
            selectedNode={
              selectedNodeId
                ? workflow.nodes.find((n) => n.id === selectedNodeId) ?? null
                : null
            }
            nodeTypes={nodeTypes}
            onPatchNode={handlePatchNode}
            onPatchWorkflow={handlePatchWorkflow}
            onDeleteNode={handleDeleteNode}
          />
        ) : (
          <aside
            style={{
              background: "var(--bg-panel)",
              borderLeft: "1px solid var(--line)",
              padding: 14,
              fontFamily: "var(--mono)",
              fontSize: 11,
              color: "var(--txt-faint)",
            }}
          >
            no workflow open
          </aside>
        )}
      </main>

      <StatusRibbon
        workflowCount={tabs.length}
        runCount={0}
        tail={`mode: ${mode} · ${activeId ?? "no workflow"} · ${
          workflow?.nodes.length ?? 0
        }n ${workflow?.edges.length ?? 0}e`}
      />

      <RunDialog
        open={runDialogOpen}
        workflowName={workflow?.name ?? activeId ?? "(none)"}
        variableDefaults={workflow?.variables ?? {}}
        workspaces={workspaces}
        defaultWorkspaceId={null}
        autoResume
        onConfirm={(input) => void handleRunConfirm(input)}
        onCancel={() => setRunDialogOpen(false)}
      />
    </div>
  );
}

/** Pick an id like `delay-3` that doesn't collide with existing nodes. */
function synthesiseNodeId(
  typeId: string,
  existing: ReadonlyArray<{ id: string }>,
): string {
  const ids = new Set(existing.map((n) => n.id));
  let counter = 1;
  let candidate = `${typeId}-${counter}`;
  while (ids.has(candidate)) {
    counter += 1;
    candidate = `${typeId}-${counter}`;
  }
  return candidate;
}

/** Drop new nodes below the lowest existing node so they don't overlap. */
function nextDropPosition(
  existing: ReadonlyArray<{ pos: { x: number; y: number } }>,
): { x: number; y: number } {
  if (existing.length === 0) return { x: 80, y: 80 };
  const maxY = existing.reduce((acc, n) => Math.max(acc, n.pos.y), 0);
  return { x: 80, y: maxY + 220 };
}
