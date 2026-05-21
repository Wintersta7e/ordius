// DAG canvas — pan/zoom + dot grid + edges + node cards.
//
// Owns its own pan/zoom state; reads workflow + selection from
// props. Wheel zoom is cursor-anchored; empty-space drag pans;
// node drag updates positions via onMoveNode.

import {
  forwardRef,
  useCallback,
  useEffect,
  useImperativeHandle,
  useMemo,
  useRef,
  useState,
} from "react";
import type {
  ForwardedRef,
  JSX,
  MouseEvent as ReactMouseEvent,
  WheelEvent,
} from "react";

import type {
  Edge,
  Node,
  NodeType,
  Workflow,
} from "../../engine/types";
import { Ic } from "../icons";
import { NodeCard, type NodeRunStatus } from "./NodeCard";
import {
  type Density,
  edgePath,
  loopPath,
  nodeCategory,
  nodeTypeIndex,
  portTip,
} from "./types";

interface RunState {
  statusByNode: Record<string, NodeRunStatus>;
  activeEdges: Set<string>;
  traveledEdges: Set<string>;
}

/** Imperative handle exposed to parents that need to translate cursor
 * positions into the canvas's world space (e.g. palette drag-to-drop). */
export interface CanvasHandle {
  screenToWorld(screenX: number, screenY: number): { x: number; y: number };
  /** Returns true if a screen-space point is over the canvas surface. */
  containsScreenPoint(screenX: number, screenY: number): boolean;
}

interface Props {
  workflow: Workflow;
  nodeTypes: NodeType[];
  selectedId: string | null;
  onSelect: (id: string | null) => void;
  onMoveNode: (id: string, pos: { x: number; y: number }) => void;
  density?: Density;
  edgeStyle?: "bezier" | "orthogonal" | "straight";
  runState?: RunState | undefined;
  /** When set, renders a drop-indicator crosshair at the given world
   * coordinates — used while a palette item is being dragged over. */
  dropPreview?: { x: number; y: number; label?: string } | null;
}

interface PanState {
  startX: number;
  startY: number;
  panX: number;
  panY: number;
}

interface DragState {
  id: string;
  startX: number;
  startY: number;
  origX: number;
  origY: number;
}

export const Canvas = forwardRef(function CanvasInner(
  {
    workflow,
    nodeTypes,
    selectedId,
    onSelect,
    onMoveNode,
    density = "standard",
    edgeStyle = "orthogonal",
    runState,
    dropPreview,
  }: Props,
  ref: ForwardedRef<CanvasHandle>,
): JSX.Element {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [pan, setPan] = useState({ x: 80, y: 80 });
  const [zoom, setZoom] = useState(0.85);
  const panState = useRef<PanState | null>(null);
  const dragState = useRef<DragState | null>(null);

  const typesById = useMemo(() => nodeTypeIndex(nodeTypes), [nodeTypes]);

  useImperativeHandle(
    ref,
    (): CanvasHandle => ({
      screenToWorld(screenX, screenY) {
        const el = containerRef.current;
        if (!el) return { x: 0, y: 0 };
        const rect = el.getBoundingClientRect();
        const px = screenX - rect.left;
        const py = screenY - rect.top;
        return { x: (px - pan.x) / zoom, y: (py - pan.y) / zoom };
      },
      containsScreenPoint(screenX, screenY) {
        const el = containerRef.current;
        if (!el) return false;
        const rect = el.getBoundingClientRect();
        return (
          screenX >= rect.left &&
          screenX <= rect.right &&
          screenY >= rect.top &&
          screenY <= rect.bottom
        );
      },
    }),
    [pan, zoom],
  );

  const fitToWorkflow = useCallback(() => {
    const el = containerRef.current;
    const nodes = workflow.nodes;
    if (!el || nodes.length === 0) return;
    const rect = el.getBoundingClientRect();
    const xs = nodes.map((n) => n.pos.x);
    const ys = nodes.map((n) => n.pos.y);
    const xs2 = nodes.map((n) => n.pos.x + 232);
    const ys2 = nodes.map((n) => n.pos.y + 162);
    const minX = Math.min(...xs);
    const maxX = Math.max(...xs2);
    const minY = Math.min(...ys);
    const maxY = Math.max(...ys2);
    const wWorld = maxX - minX + 120;
    const hWorld = maxY - minY + 200;
    const usableW = rect.width - 40;
    const usableH = rect.height - 40;
    if (usableW < 100 || usableH < 100) return;
    const fitZoom = Math.min(usableW / wWorld, usableH / hWorld, 1);
    setZoom(fitZoom);
    setPan({
      x: 20 + (usableW - wWorld * fitZoom) / 2 - minX * fitZoom,
      y: 20 + (usableH - hWorld * fitZoom) / 2 - minY * fitZoom,
    });
  }, [workflow.nodes]);

  // Auto-fit once on mount when we have nodes.
  const fittedRef = useRef(false);
  useEffect(() => {
    if (fittedRef.current || workflow.nodes.length === 0) return;
    fittedRef.current = true;
    const id = window.setTimeout(fitToWorkflow, 50);
    return () => window.clearTimeout(id);
  }, [fitToWorkflow, workflow.nodes.length]);

  // Pan with empty-space drag.
  const onMouseDown = (event: ReactMouseEvent<HTMLDivElement>) => {
    if (event.button !== 0 && event.button !== 1) return;
    panState.current = {
      startX: event.clientX,
      startY: event.clientY,
      panX: pan.x,
      panY: pan.y,
    };
  };

  useEffect(() => {
    const move = (event: globalThis.MouseEvent) => {
      if (!panState.current) return;
      const dx = event.clientX - panState.current.startX;
      const dy = event.clientY - panState.current.startY;
      setPan({ x: panState.current.panX + dx, y: panState.current.panY + dy });
    };
    const up = () => {
      panState.current = null;
    };
    window.addEventListener("mousemove", move);
    window.addEventListener("mouseup", up);
    return () => {
      window.removeEventListener("mousemove", move);
      window.removeEventListener("mouseup", up);
    };
  }, []);

  // Wheel zoom — cursor-anchored.
  const onWheel = (event: WheelEvent<HTMLDivElement>) => {
    event.preventDefault();
    const el = containerRef.current;
    if (!el) return;
    const rect = el.getBoundingClientRect();
    const px = event.clientX - rect.left;
    const py = event.clientY - rect.top;
    const factor = event.deltaY < 0 ? 1.08 : 1 / 1.08;
    const next = Math.max(0.25, Math.min(2.0, zoom * factor));
    const wx = (px - pan.x) / zoom;
    const wy = (py - pan.y) / zoom;
    setZoom(next);
    setPan({ x: px - wx * next, y: py - wy * next });
  };

  // Node drag.
  const onNodeDragStart = (
    event: ReactMouseEvent,
    id: string,
  ) => {
    const node = workflow.nodes.find((n) => n.id === id);
    if (!node) return;
    dragState.current = {
      id,
      startX: event.clientX,
      startY: event.clientY,
      origX: node.pos.x,
      origY: node.pos.y,
    };
  };

  useEffect(() => {
    const move = (event: globalThis.MouseEvent) => {
      if (!dragState.current) return;
      const dx = (event.clientX - dragState.current.startX) / zoom;
      const dy = (event.clientY - dragState.current.startY) / zoom;
      onMoveNode(dragState.current.id, {
        x: dragState.current.origX + dx,
        y: dragState.current.origY + dy,
      });
    };
    const up = () => {
      dragState.current = null;
    };
    window.addEventListener("mousemove", move);
    window.addEventListener("mouseup", up);
    return () => {
      window.removeEventListener("mousemove", move);
      window.removeEventListener("mouseup", up);
    };
  }, [zoom, onMoveNode]);

  // Dot grid background — scales with zoom.
  const gridStyle = useMemo<React.CSSProperties>(() => {
    const baseSize = 24 * zoom;
    const majorSize = baseSize * 5;
    const offX = pan.x % baseSize;
    const offY = pan.y % baseSize;
    const majOffX = pan.x % majorSize;
    const majOffY = pan.y % majorSize;
    const dotR = Math.max(0.6, 1.0 * zoom);
    const dotR2 = Math.max(1.0, 1.4 * zoom);
    return {
      background:
        `radial-gradient(circle, var(--grid-major) ${dotR2}px, transparent ${dotR2 + 0.5}px) ${majOffX}px ${majOffY}px/${majorSize}px ${majorSize}px,` +
        `radial-gradient(circle, var(--grid-minor) ${dotR}px, transparent ${dotR + 0.5}px) ${offX}px ${offY}px/${baseSize}px ${baseSize}px,` +
        " var(--bg-canvas)",
    };
  }, [pan, zoom]);

  const handleBackgroundClick = () => onSelect(null);

  return (
    <div
      ref={containerRef}
      style={{
        position: "relative",
        overflow: "hidden",
        width: "100%",
        height: "100%",
      }}
      onMouseDown={onMouseDown}
      onWheel={onWheel}
    >
      <div
        style={{
          position: "absolute",
          inset: 0,
          cursor: panState.current ? "grabbing" : "default",
          ...gridStyle,
        }}
        onMouseDown={handleBackgroundClick}
      >
        {/* World transform — everything inside is in world space */}
        <div
          style={{
            position: "absolute",
            left: 0,
            top: 0,
            transform: `translate(${pan.x}px, ${pan.y}px) scale(${zoom})`,
            transformOrigin: "0 0",
            width: 1,
            height: 1,
          }}
        >
          <EdgeLayer
            workflow={workflow}
            typesById={typesById}
            edgeStyle={edgeStyle}
            density={density}
            runState={runState}
          />
          {workflow.nodes.map((node) => {
            const nodeType = typesById.get(node.type);
            const category = nodeCategory(node, typesById);
            const status = runState?.statusByNode[node.id];
            return (
              <NodeCard
                key={node.id}
                node={node}
                nodeType={nodeType}
                category={category}
                selected={selectedId === node.id}
                density={density}
                runStatus={status}
                onSelect={onSelect}
                onDragStart={onNodeDragStart}
              />
            );
          })}
          {dropPreview ? (
            <div
              style={{
                position: "absolute",
                left: dropPreview.x,
                top: dropPreview.y,
                width: 232,
                height: 162,
                border: "2px dashed var(--accent)",
                borderRadius: 6,
                background:
                  "color-mix(in srgb, var(--accent) 10%, transparent)",
                pointerEvents: "none",
                boxSizing: "border-box",
              }}
              aria-hidden="true"
            >
              {dropPreview.label ? (
                <div
                  className="num"
                  style={{
                    position: "absolute",
                    left: 0,
                    top: -22,
                    fontFamily: "var(--mono)",
                    fontSize: 11,
                    color: "var(--accent)",
                    background: "var(--bg-panel)",
                    padding: "2px 8px",
                    border: "1px solid var(--accent)",
                    borderRadius: 3,
                    whiteSpace: "nowrap",
                    letterSpacing: "0.04em",
                  }}
                >
                  + {dropPreview.label}
                </div>
              ) : null}
            </div>
          ) : null}
        </div>

        <Corner pos="tl" />
        <Corner pos="tr" />
        <Corner pos="bl" />
        <Corner pos="br" />
      </div>

      <CoordReadout pan={pan} zoom={zoom} />
      <CanvasWatermark workflow={workflow} />
      <ZoomControls
        zoom={zoom}
        onZoomIn={() => setZoom((z) => Math.min(2.0, z * 1.15))}
        onZoomOut={() => setZoom((z) => Math.max(0.25, z / 1.15))}
        onFit={fitToWorkflow}
      />

      {workflow.nodes.length === 0 ? <EmptyCanvasHint /> : null}

      <style>{`
        @keyframes pulse {
          0%, 100% { opacity: 1; transform: scale(1); }
          50%      { opacity: 0.55; transform: scale(1.25); }
        }
      `}</style>
    </div>
  );
});

interface EdgeLayerProps {
  workflow: Workflow;
  typesById: Map<string, NodeType>;
  edgeStyle: "bezier" | "orthogonal" | "straight";
  density: Density;
  runState?: RunState | undefined;
}

function EdgeLayer({
  workflow,
  typesById,
  edgeStyle,
  density,
  runState,
}: EdgeLayerProps): JSX.Element {
  return (
    <svg
      width={4000}
      height={2400}
      style={{
        position: "absolute",
        left: 0,
        top: 0,
        pointerEvents: "none",
        overflow: "visible",
      }}
      aria-hidden="true"
    >
      <defs>
        <marker
          id="ord-arrow"
          viewBox="0 0 10 10"
          refX="9"
          refY="5"
          markerWidth="7"
          markerHeight="7"
          orient="auto"
        >
          <path d="M 0 0 L 10 5 L 0 10 z" fill="var(--txt-soft)" />
        </marker>
        <marker
          id="ord-arrow-active"
          viewBox="0 0 10 10"
          refX="9"
          refY="5"
          markerWidth="7"
          markerHeight="7"
          orient="auto"
        >
          <path d="M 0 0 L 10 5 L 0 10 z" fill="var(--accent)" />
        </marker>
        <marker
          id="ord-arrow-loop"
          viewBox="0 0 10 10"
          refX="9"
          refY="5"
          markerWidth="7"
          markerHeight="7"
          orient="auto"
        >
          <path d="M 0 0 L 10 5 L 0 10 z" fill="var(--warn)" />
        </marker>
      </defs>
      {workflow.edges.map((edge) =>
        renderEdge(edge, workflow.nodes, typesById, edgeStyle, density, runState),
      )}
    </svg>
  );
}

function renderEdge(
  edge: Edge,
  nodes: Node[],
  typesById: Map<string, NodeType>,
  style: "bezier" | "orthogonal" | "straight",
  density: Density,
  runState?: RunState | undefined,
): JSX.Element | null {
  const from = nodes.find((n) => n.id === edge.fromNodeId);
  const to = nodes.find((n) => n.id === edge.toNodeId);
  if (!from || !to) return null;
  const fromType = typesById.get(from.type);
  const toType = typesById.get(to.type);
  const fromPorts = fromType?.outputs.map((p) => p.name) ?? [edge.fromPort];
  const toPorts = toType?.inputs.map((p) => p.name) ?? [edge.toPort];
  const a = portTip(from, fromPorts, edge.fromPort, "out", density);
  const b = portTip(to, toPorts, edge.toPort, "in", density);
  const isLoop = edge.edgeType === "loop";
  const active = runState?.activeEdges.has(edge.id) ?? false;
  const traveled = runState?.traveledEdges.has(edge.id) ?? false;

  const baseColor = isLoop
    ? "var(--warn)"
    : traveled
      ? "oklch(0.78 0.18 305)"
      : "var(--txt-soft)";
  const color = active ? "var(--accent)" : baseColor;
  const sw = active ? 2.2 : traveled ? 1.6 : 1.2;
  const d = isLoop ? loopPath(a, b) : edgePath(style, a, b);

  return (
    <path
      key={edge.id}
      d={d}
      fill="none"
      stroke={color}
      strokeWidth={sw}
      strokeDasharray={isLoop ? "6 4" : active ? "4 3" : "none"}
      markerEnd={`url(#${active ? "ord-arrow-active" : isLoop ? "ord-arrow-loop" : "ord-arrow"})`}
      style={{
        filter: active ? "drop-shadow(0 0 6px var(--accent))" : "none",
        transition: "stroke .2s, stroke-width .2s",
      }}
    />
  );
}

interface CoordProps {
  pan: { x: number; y: number };
  zoom: number;
}

function CoordReadout({ pan, zoom }: CoordProps): JSX.Element {
  const wx = (-pan.x / zoom).toFixed(0).padStart(4, " ");
  const wy = (-pan.y / zoom).toFixed(0).padStart(4, " ");
  return (
    <div
      className="num"
      style={{
        position: "absolute",
        top: 8,
        left: 10,
        zIndex: 9,
        display: "inline-flex",
        alignItems: "center",
        gap: 6,
        fontFamily: "var(--mono)",
        fontSize: 10,
        color: "var(--txt-faint)",
        pointerEvents: "none",
      }}
    >
      <span>
        x{wx} y{wy}
      </span>
      <span style={{ color: "var(--line-strong)" }}>·</span>
      <span>{Math.round(zoom * 100)}%</span>
    </div>
  );
}

function CanvasWatermark({ workflow }: { workflow: Workflow }): JSX.Element {
  return (
    <div
      style={{
        position: "absolute",
        left: 12,
        bottom: 16,
        fontFamily: "var(--mono)",
        color: "var(--txt-faint)",
        opacity: 0.55,
        pointerEvents: "none",
        fontSize: 10,
        lineHeight: 1.4,
      }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <span
          style={{
            display: "inline-block",
            width: 10,
            height: 10,
            border: "1.5px solid currentColor",
            borderRadius: 2,
          }}
        />
        <span style={{ letterSpacing: "0.16em" }}>ORDIUS · CANVAS</span>
      </div>
      <div style={{ marginTop: 4, opacity: 0.7 }}>workflow:{workflow.name}</div>
      <div style={{ opacity: 0.7 }} className="num">
        {workflow.nodes.length}n · {workflow.edges.length}e · schema v
        {workflow.schemaVersion}
      </div>
    </div>
  );
}

interface ZoomProps {
  zoom: number;
  onZoomIn: () => void;
  onZoomOut: () => void;
  onFit: () => void;
}

function ZoomControls({
  zoom,
  onZoomIn,
  onZoomOut,
  onFit,
}: ZoomProps): JSX.Element {
  return (
    <div
      style={{
        position: "absolute",
        right: 16,
        bottom: 16,
        zIndex: 20,
        display: "flex",
        gap: 6,
        padding: 4,
        background: "var(--bg-elevated)",
        border: "1px solid var(--line)",
        borderRadius: 6,
        boxShadow: "0 6px 18px -8px rgba(0,0,0,.5)",
      }}
    >
      <button
        type="button"
        className="btn ghost icon"
        title="Zoom out"
        onClick={onZoomOut}
      >
        −
      </button>
      <div
        className="num"
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          width: 54,
          fontSize: 11,
          color: "var(--txt-dim)",
        }}
      >
        {Math.round(zoom * 100)}%
      </div>
      <button
        type="button"
        className="btn ghost icon"
        title="Zoom in"
        onClick={onZoomIn}
      >
        +
      </button>
      <span style={{ width: 1, background: "var(--line)" }} />
      <button
        type="button"
        className="btn ghost icon"
        title="Fit to view"
        onClick={onFit}
      >
        {Ic["check"]?.({ size: 12 })}
      </button>
    </div>
  );
}

function EmptyCanvasHint(): JSX.Element {
  const isMac =
    typeof navigator !== "undefined" &&
    /Mac|iPhone|iPad/.test(navigator.platform);
  const chord = isMac ? "⌘P" : "Ctrl+P";
  return (
    <div
      style={{
        position: "absolute",
        inset: 0,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        pointerEvents: "none",
        fontFamily: "var(--mono)",
      }}
      aria-hidden="true"
    >
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          gap: 14,
          textAlign: "center",
        }}
      >
        <div
          style={{
            width: 88,
            height: 60,
            border: "1.5px dashed var(--line-strong)",
            borderRadius: 3,
            background: "var(--bg-panel)",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            color: "var(--txt-faint)",
            fontSize: 22,
          }}
        >
          +
        </div>
        <div
          style={{
            fontSize: 11.5,
            color: "var(--txt-dim)",
            maxWidth: 340,
            lineHeight: 1.55,
          }}
        >
          <div
            style={{
              color: "var(--txt)",
              fontWeight: 600,
              marginBottom: 4,
            }}
          >
            This canvas is empty.
          </div>
          drag a node from the palette on the left, or press{" "}
          <span
            style={{
              border: "1px solid var(--line)",
              padding: "0 5px",
              borderRadius: 2,
              background: "var(--bg-input)",
              color: "var(--txt)",
            }}
          >
            {chord}
          </span>{" "}
          to search node types.
        </div>
      </div>
      <div
        style={{
          position: "absolute",
          left: 12,
          top: "50%",
          transform: "translateY(-50%)",
          display: "flex",
          alignItems: "center",
          gap: 6,
          fontSize: 10,
          color: "var(--accent)",
          letterSpacing: "0.10em",
          textTransform: "uppercase",
        }}
      >
        <span style={{ fontSize: 16 }}>◀</span>
        <span>palette</span>
      </div>
    </div>
  );
}

function Corner({ pos }: { pos: "tl" | "tr" | "bl" | "br" }): JSX.Element {
  const at = {
    tl: { top: 10, left: 10 },
    tr: { top: 10, right: 10, transform: "scaleX(-1)" },
    bl: { bottom: 10, left: 10, transform: "scaleY(-1)" },
    br: { bottom: 10, right: 10, transform: "scale(-1, -1)" },
  } as const;
  return (
    <svg
      width="14"
      height="14"
      viewBox="0 0 14 14"
      style={{ position: "absolute", ...at[pos], opacity: 0.5 }}
      aria-hidden="true"
    >
      <polyline
        points="1 1 1 6 6 6 6 1 1 1"
        fill="none"
        stroke="var(--line-strong)"
        strokeWidth="1.2"
      />
      <circle cx="3.5" cy="3.5" r="1.2" fill="var(--line-strong)" />
    </svg>
  );
}
