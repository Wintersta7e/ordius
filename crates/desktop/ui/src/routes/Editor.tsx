// Editor route — workflow authoring centerpiece.
//
// Phase 1.5a wired the chrome shell. Phase 1.5b adds the working
// canvas: pan/zoom, dot grid, render nodes/edges from the loaded
// workflow, single-click select, drag-to-move. Palette + properties
// land in 1.5c / 1.5d.

import { useCallback, useEffect, useRef, useState } from "react";
import type { JSX } from "react";

import {
  type NodeType,
  type Workflow,
  type WorkflowWarningIpc,
  type Workspace,
  listNodeTypes,
  listWorkflows,
  listWorkspaces,
  loadWorkflow,
  runWorkflow,
  saveWorkflow,
  stopRun,
  validateWorkflow,
} from "../engine";
import { Canvas, type CanvasHandle } from "../components/canvas";
import { portTip } from "../components/canvas/types";
import { attachWindowDrag } from "../lib/windowDrag";
import {
  DEMO_NODE_TYPES,
  DEMO_WORKFLOW,
} from "../components/canvas/demoWorkflow";
import { EditorTopBar, type EditorMode } from "../components/chrome/EditorTopBar";
import { Resizer } from "../components/chrome/Resizer";
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
  runOnOpen?: boolean;
  theme: "dark" | "light";
  onThemeToggle: () => void;
  onNavigate: (route: Route) => void;
}

export function Editor({
  workflowId,
  runOnOpen = false,
  theme,
  onThemeToggle,
  onNavigate,
}: Props): JSX.Element {
  const [mode, setMode] = useState<EditorMode>("editor");
  const [tabs, setTabs] = useState<WorkflowTab[]>([]);
  const [activeId, setActiveId] = useState<string | null>(workflowId ?? null);
  const [workflow, setWorkflow] = useState<Workflow | null>(null);
  // Per-load, non-fatal lint warnings surfaced by the engine loader
  // (e.g. `loopback_url_in_remote_env`). Rendered as a dismissable
  // stack between the chrome and the canvas. Dismissal is session-
  // local; reload re-fetches the list from the IPC.
  const [warnings, setWarnings] = useState<WorkflowWarningIpc[]>([]);
  const [nodeTypes, setNodeTypes] = useState<NodeType[]>([]);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [selectedEdgeId, setSelectedEdgeId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [validateNotice, setValidateNotice] = useState<string | null>(null);
  const [runState, setRunState] = useState<LiveRunState>(emptyRunState);
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [workspaceId, setWorkspaceId] = useState<string | null>(null);
  const [runDialogOpen, setRunDialogOpen] = useState(false);
  const [paletteW, setPaletteW] = useState<number>(() => readStoredWidth("ordius.layout.paletteW", 220, PALETTE_MIN, PALETTE_MAX));
  const [propsW, setPropsW] = useState<number>(() => readStoredWidth("ordius.layout.propsW", 320, PROPS_MIN, PROPS_MAX));

  useEffect(() => {
    try {
      window.localStorage.setItem("ordius.layout.paletteW", String(paletteW));
    } catch {
      // localStorage may be disabled (private mode, embedded surface) — silently skip.
    }
  }, [paletteW]);
  useEffect(() => {
    try {
      window.localStorage.setItem("ordius.layout.propsW", String(propsW));
    } catch {
      // see above.
    }
  }, [propsW]);

  // Re-navigating to the same workflow without runOnOpen must not
  // re-trigger the dialog, so guard against repeats per workflow id.
  const runOnOpenConsumed = useRef<string | null>(null);
  useEffect(() => {
    if (!workflow || !runOnOpen) return;
    if (runOnOpenConsumed.current === workflow.id) return;
    runOnOpenConsumed.current = workflow.id;
    setRunDialogOpen(true);
  }, [workflow, runOnOpen]);

  // Cmd/Ctrl+P → focus the palette filter input. Matches the keystroke
  // advertised on the empty-canvas hint; preventDefault stops the
  // webview's native print dialog.
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      const cmd = event.metaKey || event.ctrlKey;
      if (!cmd || event.altKey || event.shiftKey) return;
      if (event.key !== "p" && event.key !== "P") return;
      const filter = document.querySelector<HTMLInputElement>(
        'input[aria-label="Filter node types"]',
      );
      if (!filter) return;
      event.preventDefault();
      filter.focus();
      filter.select();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  // Palette drag-to-canvas state. `drag` tracks the in-flight gesture
  // and powers the floating ghost near the cursor; `dropPreview`
  // mirrors that as world-space coords for the canvas crosshair (only
  // set when the cursor is over the canvas surface).
  const canvasHandleRef = useRef<CanvasHandle | null>(null);
  const [drag, setDrag] = useState<PaletteDrag | null>(null);
  const [dropPreview, setDropPreview] = useState<
    { x: number; y: number; label: string } | null
  >(null);

  // Port→port edge drag state. `connect` carries the source endpoint
  // plus the live cursor world position so Canvas can render a
  // preview line straight to the cursor.
  const [connect, setConnect] = useState<{
    fromNodeId: string;
    fromPort: string;
    cursorWorld: { x: number; y: number };
  } | null>(null);

  // Tears down window listeners if the editor unmounts mid-drag.
  const activeDragDetach = useRef<(() => void) | null>(null);
  useEffect(
    () => () => {
      activeDragDetach.current?.();
      activeDragDetach.current = null;
    },
    [],
  );

  const insideTauri =
    typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;

  // Workspace catalog → RunDialog's workspace picker.
  const reloadWorkspaces = useCallback(async () => {
    if (!insideTauri) return;
    try {
      const ws = await listWorkspaces();
      setWorkspaces(ws);
    } catch {
      /* non-fatal — RunDialog falls back to engine home */
    }
  }, [insideTauri]);
  useEffect(() => {
    void reloadWorkspaces();
  }, [reloadWorkspaces]);

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

  // Load the workflow when workflowId changes, or seed a fresh
  // blank workflow when the editor is opened via "+ new workflow"
  // (no id yet — user hasn't saved).
  useEffect(() => {
    if (!workflowId) {
      const fresh = makeBlankWorkflow();
      setWorkflow(fresh);
      setWarnings([]);
      setTabs((existing) => {
        if (existing.some((t) => t.id === fresh.id)) return existing;
        return [
          ...existing,
          { id: fresh.id, name: fresh.name, dirty: true, running: false },
        ];
      });
      setActiveId(fresh.id);
      return;
    }
    if (!insideTauri) {
      setError(
        "running in browser preview · engine commands disabled — launch via `tauri dev` to open real workflows",
      );
      // No engine → no warnings to surface.
      setWarnings([]);
      // Unsaved tabs (id starts with `new-`) have no on-disk source,
      // so re-activating one in browser preview should keep its
      // blank canvas — not silently swap in the demo fixture.
      if (workflowId.startsWith("new-")) {
        setWorkflow({ ...makeBlankWorkflow(), id: workflowId });
        setActiveId(workflowId);
        return;
      }
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
        const result = await loadWorkflow(workflowId);
        if (cancelled) return;
        const wf = result.workflow;
        setWorkflow(wf);
        setWarnings(result.warnings);
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

  const handleActivate = useCallback(
    (id: string) => {
      setActiveId(id);
      // Navigate so the load useEffect refires against the activated
      // tab's workflow id. Without this, switching tabs leaves the
      // canvas showing the previously-loaded workflow.
      if (id !== workflowId) {
        onNavigate({ kind: "editor", workflowId: id });
      }
    },
    [workflowId, onNavigate],
  );

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
            (e) => e.fromNodeId !== id && e.toNodeId !== id,
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

  const handleDeleteEdge = useCallback(
    (id: string) => {
      setWorkflow((wf) => {
        if (!wf) return wf;
        return { ...wf, edges: wf.edges.filter((e) => e.id !== id) };
      });
      setSelectedEdgeId((current) => (current === id ? null : current));
      setTabs((existing) =>
        existing.map((t) => (t.id === activeId ? { ...t, dirty: true } : t)),
      );
    },
    [activeId],
  );

  const handleSelectEdge = useCallback((id: string | null) => {
    setSelectedEdgeId(id);
    setSelectedNodeId(null);
  }, []);

  const handleSelectNode = useCallback((id: string | null) => {
    setSelectedNodeId(id);
    setSelectedEdgeId(null);
  }, []);

  // Delete/Backspace removes the selected node or edge — but only
  // when the user isn't typing into an input/textarea/select.
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.key !== "Delete" && event.key !== "Backspace") return;
      const t = event.target as HTMLElement | null;
      if (
        t &&
        (t.tagName === "INPUT" ||
          t.tagName === "TEXTAREA" ||
          t.tagName === "SELECT" ||
          t.isContentEditable)
      ) {
        return;
      }
      if (selectedEdgeId) {
        event.preventDefault();
        handleDeleteEdge(selectedEdgeId);
      } else if (selectedNodeId) {
        event.preventDefault();
        handleDeleteNode(selectedNodeId);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [selectedEdgeId, selectedNodeId, handleDeleteEdge, handleDeleteNode]);

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

  const handleValidate = useCallback(async () => {
    if (!workflow) return;
    if (!insideTauri) {
      setError("validate requires the desktop host");
      return;
    }
    try {
      await validateWorkflow(workflow);
      setError(null);
      setValidateNotice("workflow validation passed");
      window.setTimeout(() => setValidateNotice(null), 2500);
    } catch (e: unknown) {
      setValidateNotice(null);
      setError(String(e));
    }
  }, [workflow, insideTauri]);

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
      let toSave = workflow;
      // Fresh blank workflows ship with id `new-<ts>` so the editor
      // has something to point at; rename to a slug of the user's
      // chosen name on first save so the on-disk filename is stable.
      if (workflow.id.startsWith("new-")) {
        const slug = slugify(workflow.name);
        if (!slug) {
          setError("rename the workflow before saving (id is empty)");
          return;
        }
        const existing = await listWorkflows();
        const taken = new Set(existing.map((w) => w.id));
        let candidate = slug;
        let counter = 2;
        while (taken.has(candidate)) {
          candidate = `${slug}-${counter}`;
          counter += 1;
          if (counter > 999) {
            setError("could not find a free id; rename the workflow");
            return;
          }
        }
        toSave = { ...workflow, id: candidate };
        setWorkflow(toSave);
        setActiveId(candidate);
        setTabs((existing) =>
          existing.map((t) =>
            t.id === workflow.id ? { ...t, id: candidate, dirty: false } : t,
          ),
        );
        onNavigate({ kind: "editor", workflowId: candidate });
      } else {
        setTabs((existing) =>
          existing.map((t) => (t.id === activeId ? { ...t, dirty: false } : t)),
        );
      }
      await saveWorkflow(toSave);
    } catch (e: unknown) {
      setError(String(e));
    }
  }, [workflow, activeId, insideTauri, onNavigate]);

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
              continueOnError: false,
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

  /** Spawn at an explicit world-space position, used by drag-to-drop. */
  const handleAddNodeAt = useCallback(
    (typeId: string, pos: { x: number; y: number }) => {
      const nodeType = nodeTypes.find((t) => t.id === typeId);
      setWorkflow((wf) => {
        if (!wf) return wf;
        const nextId = synthesiseNodeId(typeId, wf.nodes);
        return {
          ...wf,
          nodes: [
            ...wf.nodes,
            {
              id: nextId,
              type: typeId,
              name: nodeType?.name ?? typeId,
              config: {},
              pos: { x: Math.round(pos.x), y: Math.round(pos.y) },
              continueOnError: false,
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

  /** Start a palette drag. Window-level pointer + key listeners are
   * attached synchronously here (rather than in a useEffect) so a
   * rapid press-and-release still hits our pointerup handler. */
  const handleBeginDrag = useCallback(
    (typeId: string, startX: number, startY: number) => {
      const nodeType = nodeTypes.find((t) => t.id === typeId);
      const label = nodeType?.name ?? typeId;
      const initial: PaletteDrag = {
        typeId,
        label,
        startX,
        startY,
        x: startX,
        y: startY,
        active: false,
      };
      let current: PaletteDrag = initial;
      setDrag(initial);

      const finish = (commit: boolean, upX: number, upY: number) => {
        activeDragDetach.current?.();
        activeDragDetach.current = null;
        setDrag(null);
        setDropPreview(null);
        if (!commit) return;
        const handle = canvasHandleRef.current;
        if (!current.active) {
          handleAddNode(current.typeId);
          return;
        }
        if (handle && handle.containsScreenPoint(upX, upY)) {
          const world = handle.screenToWorld(upX, upY);
          handleAddNodeAt(current.typeId, {
            x: world.x - NODE_HALF_W,
            y: world.y - NODE_HALF_H,
          });
        }
      };

      activeDragDetach.current = attachWindowDrag({
        onMove(event) {
          const dx = event.clientX - current.startX;
          const dy = event.clientY - current.startY;
          const nextActive =
            current.active || Math.hypot(dx, dy) >= DRAG_THRESHOLD_PX;
          current = {
            ...current,
            x: event.clientX,
            y: event.clientY,
            active: nextActive,
          };
          setDrag(current);
          const handle = canvasHandleRef.current;
          if (
            nextActive &&
            handle &&
            handle.containsScreenPoint(event.clientX, event.clientY)
          ) {
            const world = handle.screenToWorld(event.clientX, event.clientY);
            setDropPreview({
              x: world.x - NODE_HALF_W,
              y: world.y - NODE_HALF_H,
              label: current.label,
            });
          } else {
            setDropPreview(null);
          }
        },
        onUp(event) {
          finish(true, event.clientX, event.clientY);
        },
        onKey(event) {
          if (event.key === "Escape") finish(false, 0, 0);
        },
      });
    },
    [nodeTypes, handleAddNode, handleAddNodeAt],
  );

  /** Create an edge from output port → input port. Validates against
   * self-loops and duplicates; silently no-ops on rejection so a bad
   * drop just dismisses the preview. */
  const handleCreateEdge = useCallback(
    (
      from: { nodeId: string; port: string },
      to: { nodeId: string; port: string },
    ) => {
      if (from.nodeId === to.nodeId) return;
      setWorkflow((wf) => {
        if (!wf) return wf;
        const dup = wf.edges.some(
          (e) =>
            e.fromNodeId === from.nodeId &&
            e.fromPort === from.port &&
            e.toNodeId === to.nodeId &&
            e.toPort === to.port,
        );
        if (dup) return wf;
        const id = synthesiseEdgeId(wf.edges);
        return {
          ...wf,
          edges: [
            ...wf.edges,
            {
              id,
              fromNodeId: from.nodeId,
              fromPort: from.port,
              toNodeId: to.nodeId,
              toPort: to.port,
              edgeType: "forward",
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
    [activeId],
  );

  /** Start an edge drag from an output port. Pointer listeners are
   * attached synchronously here (same pattern as palette drag) so a
   * rapid press-release without movement still hits pointerup. */
  const handleBeginConnect = useCallback(
    (fromNodeId: string, fromPort: string, startX: number, startY: number) => {
      const handle = canvasHandleRef.current;
      const initialWorld = handle?.screenToWorld(startX, startY) ?? {
        x: 0,
        y: 0,
      };
      setConnect({
        fromNodeId,
        fromPort,
        cursorWorld: initialWorld,
      });

      const finish = (commit: boolean, upX: number, upY: number) => {
        activeDragDetach.current?.();
        activeDragDetach.current = null;
        setConnect(null);
        if (!commit) return;
        const target = document.elementFromPoint(upX, upY) as
          | HTMLElement
          | null;
        if (!target) return;
        const portEl = target.closest<HTMLElement>("[data-port-node-id]");
        if (!portEl) return;
        const side = portEl.dataset["portSide"];
        const toNodeId = portEl.dataset["portNodeId"];
        const toPort = portEl.dataset["portName"];
        if (side !== "in" || !toNodeId || !toPort) return;
        handleCreateEdge(
          { nodeId: fromNodeId, port: fromPort },
          { nodeId: toNodeId, port: toPort },
        );
      };

      activeDragDetach.current = attachWindowDrag({
        onMove(event) {
          const live = canvasHandleRef.current;
          if (!live) return;
          const world = live.screenToWorld(event.clientX, event.clientY);
          setConnect((current) =>
            current ? { ...current, cursorWorld: world } : current,
          );
        },
        onUp(event) {
          finish(true, event.clientX, event.clientY);
        },
        onKey(event) {
          if (event.key === "Escape") finish(false, 0, 0);
        },
      });
    },
    [handleCreateEdge],
  );

  // Derive the world-space preview line endpoints (source port tip
  // + live cursor) from the connect state. Returns null when no
  // edge drag is in flight.
  const connectPreview = (() => {
    if (!connect) return null;
    const fromNode = workflow?.nodes.find((n) => n.id === connect.fromNodeId);
    if (!fromNode) return null;
    const fromType = nodeTypes.find((t) => t.id === fromNode.type);
    const fromPorts = fromType?.outputs.map((p) => p.name) ?? [
      connect.fromPort,
    ];
    const from = portTip(fromNode, fromPorts, connect.fromPort, "out", "standard");
    return { from, to: connect.cursorWorld };
  })();

  // Splice an `auto` row in for the workflow-warning stack so the
  // banners ride above the main column without squeezing the canvas.
  const gridTemplateRows =
    warnings.length > 0
      ? "44px 30px auto 1fr 22px"
      : "44px 30px 1fr 22px";

  return (
    <div
      style={{
        display: "grid",
        gridTemplateRows,
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
        onValidate={handleValidate}
        onNavigate={onNavigate}
        workspaces={workspaces}
        workspaceId={workspaceId}
        onWorkspaceChange={setWorkspaceId}
        onWorkspacesChanged={() => void reloadWorkspaces()}
      />
      <WorkflowTabStrip
        tabs={tabs}
        activeId={activeId}
        onActivate={handleActivate}
        onClose={handleClose}
        onNew={handleNewTab}
      />

      {warnings.length > 0 ? (
        <section
          aria-label="Workflow load warnings"
          style={{
            display: "flex",
            flexDirection: "column",
            gap: 6,
            padding: "8px 12px",
            background: "var(--bg-panel)",
            borderBottom: "1px solid var(--line)",
          }}
        >
          {warnings.map((w) => (
            <NoticeBanner
              key={`${w.nodeId}:${w.kind}`}
              kind="warn"
              title={w.nodeId}
              message={w.message}
              onDismiss={() =>
                setWarnings((prev) => prev.filter((x) => x !== w))
              }
            />
          ))}
        </section>
      ) : null}

      <main
        style={{
          display: "grid",
          gridTemplateColumns: `${paletteW}px 5px 1fr 5px ${propsW}px`,
          minHeight: 0,
          overflow: "hidden",
        }}
      >
        {/* Palette — Phase 1.5c */}
        <Palette
          nodeTypes={nodeTypes}
          onAdd={handleAddNode}
          onBeginDrag={handleBeginDrag}
        />
        <Resizer
          ariaLabel="Resize palette"
          onResize={(dx) => setPaletteW((w) => clampWidth(w + dx, PALETTE_MIN, PALETTE_MAX))}
        />

        {/* Canvas — Phase 1.5b */}
        <section style={{ position: "relative", minWidth: 0 }}>
          {workflow ? (
            <Canvas
              ref={canvasHandleRef}
              workflow={workflow}
              nodeTypes={nodeTypes}
              selectedId={selectedNodeId}
              onSelect={handleSelectNode}
              selectedEdgeId={selectedEdgeId}
              onEdgeSelect={handleSelectEdge}
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
              dropPreview={dropPreview}
              onPortConnectStart={handleBeginConnect}
              connectPreview={connectPreview}
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
            <NoticeBanner message={error} variant="overlay" />
          ) : validateNotice ? (
            <NoticeBanner
              message={validateNotice}
              variant="overlay"
              tone="ok"
            />
          ) : null}
        </section>

        <Resizer
          ariaLabel="Resize properties panel"
          onResize={(dx) => setPropsW((w) => clampWidth(w - dx, PROPS_MIN, PROPS_MAX))}
        />

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
        defaultWorkspaceId={workspaceId}
        autoResume
        onConfirm={(input) => void handleRunConfirm(input)}
        onCancel={() => setRunDialogOpen(false)}
      />

      {drag?.active ? (
        <div
          aria-hidden="true"
          style={{
            position: "fixed",
            left: drag.x + 14,
            top: drag.y + 14,
            zIndex: 1000,
            pointerEvents: "none",
            fontFamily: "var(--mono)",
            fontSize: 11,
            color: "var(--accent)",
            background: "var(--bg-elevated)",
            border: "1px solid var(--accent)",
            padding: "3px 8px",
            borderRadius: 3,
            boxShadow: "0 4px 12px -4px rgba(0,0,0,.4)",
            letterSpacing: "0.04em",
            whiteSpace: "nowrap",
          }}
        >
          + {drag.label}
        </div>
      ) : null}
    </div>
  );
}

const PALETTE_MIN = 180;
const PALETTE_MAX = 480;
const PROPS_MIN = 240;
const PROPS_MAX = 560;

/** Pixels of cursor travel before a palette press becomes a drag.
 * Below this, the gesture is treated as a click (cascade-spawn). */
const DRAG_THRESHOLD_PX = 4;
/** Half-width / half-height of a standard NodeCard footprint —
 * lets us centre the drop on the cursor instead of top-lefting it. */
const NODE_HALF_W = 116;
const NODE_HALF_H = 80;

interface PaletteDrag {
  typeId: string;
  label: string;
  startX: number;
  startY: number;
  x: number;
  y: number;
  active: boolean;
}

function clampWidth(v: number, lo: number, hi: number): number {
  return Math.max(lo, Math.min(hi, v));
}

function readStoredWidth(
  key: string,
  fallback: number,
  lo: number,
  hi: number,
): number {
  try {
    const raw = window.localStorage.getItem(key);
    if (raw == null) return fallback;
    const n = Number.parseInt(raw, 10);
    return Number.isFinite(n) ? clampWidth(n, lo, hi) : fallback;
  } catch {
    return fallback;
  }
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

/** Pick an id like `e-7` that doesn't collide with existing edges. */
function synthesiseEdgeId(existing: ReadonlyArray<{ id: string }>): string {
  const ids = new Set(existing.map((e) => e.id));
  let counter = 1;
  let candidate = `e-${counter}`;
  while (ids.has(candidate)) {
    counter += 1;
    candidate = `e-${counter}`;
  }
  return candidate;
}

/** Seed a fresh, unsaved workflow with a unique id + manual trigger. */
function makeBlankWorkflow(): Workflow {
  const stamp = new Date()
    .toISOString()
    .replace(/[-:T]/g, "")
    .slice(0, 14);
  return {
    id: `new-${stamp}`,
    name: "untitled",
    schemaVersion: 1,
    variables: {},
    triggers: [{ type: "manual" }],
    nodes: [],
    edges: [],
  };
}

/** Drop new nodes below the lowest existing node so they don't overlap. */
function nextDropPosition(
  existing: ReadonlyArray<{ pos: { x: number; y: number } }>,
): { x: number; y: number } {
  if (existing.length === 0) return { x: 80, y: 80 };
  const maxY = existing.reduce((acc, n) => Math.max(acc, n.pos.y), 0);
  return { x: 80, y: maxY + 220 };
}

/**
 * ASCII slug for a workflow name. Lowercases, collapses non-alnum to
 * single `-`, trims edges, caps at 128 chars to match the engine
 * boundary validator.
 */
function slugify(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 128);
}
