// One node card on the canvas.
//
// Cards are absolutely positioned in world space; the parent
// applies the pan/zoom transform. Header is category-tinted with
// an icon + type id + status LED; body shows the workflow-given
// node name + a part-number footer.

import type { JSX, MouseEvent } from "react";

import type { Category } from "../../engine/types";
import { CATEGORIES, catColor } from "../../data/categories";
import { NodeIcon } from "../icons";
import {
  type Density,
  type Node,
  type NodeType,
  NODE_H,
  NODE_W,
  PIN_W,
  idHash,
  portY,
} from "./types";

export type NodeRunStatus = "running" | "done" | "error" | "skipped" | "pending";

interface Props {
  node: Node;
  /** Looked-up engine NodeType — may be undefined for manifest-loaded
   * types the GUI hasn't fetched yet. Card falls back gracefully. */
  nodeType?: NodeType | undefined;
  category: Category;
  selected: boolean;
  density: Density;
  runStatus?: NodeRunStatus | undefined;
  onSelect: (id: string) => void;
  onDragStart: (event: MouseEvent, id: string) => void;
  /** Fires when an output pin is grabbed for edge-creation. */
  onPortConnectStart?: (
    nodeId: string,
    portName: string,
    screenX: number,
    screenY: number,
  ) => void;
}

export function NodeCard({
  node,
  nodeType,
  category,
  selected,
  density,
  runStatus,
  onSelect,
  onDragStart,
  onPortConnectStart,
}: Props): JSX.Element {
  const cat = CATEGORIES[category];
  const w = NODE_W[density];
  const h = NODE_H[density];
  const glow = runStatus === "running";
  const headerH = density === "compact" ? 30 : 40;
  const base = catColor(category, "base");
  const tint = catColor(category, "tint");
  const border = catColor(category, "border");
  const glowCol = catColor(category, "glow");

  const STATUS_COLORS: Record<NodeRunStatus, string> = {
    running: "var(--info)",
    done: "var(--ok)",
    error: "var(--err)",
    skipped: "var(--txt-faint)",
    pending: "transparent",
  };
  const statusColor = runStatus ? STATUS_COLORS[runStatus] : undefined;

  const borderColor = selected
    ? "var(--accent)"
    : glow
      ? glowCol
      : "var(--line)";
  const shadow = selected
    ? "0 0 0 1px var(--accent), 0 12px 32px -10px var(--accent-soft)"
    : glow
      ? `0 0 0 1px ${glowCol}, 0 0 24px -2px ${glowCol}`
      : "0 1px 0 rgba(0,0,0,.18), 0 8px 18px -12px rgba(0,0,0,.6)";

  const handleMouseDown = (event: MouseEvent) => {
    event.stopPropagation();
    onSelect(node.id);
    onDragStart(event, node.id);
  };

  const displayName = node.name || node.id;
  const typeLabel = nodeType?.name ?? node.type;
  const partNumber = computePartNumber(node.id);

  return (
    <div
      data-node-id={node.id}
      onMouseDown={handleMouseDown}
      style={{
        position: "absolute",
        left: node.pos.x,
        top: node.pos.y,
        width: w,
        height: h,
        background: "var(--bg-panel)",
        border: `1px solid ${borderColor}`,
        borderRadius: 3,
        boxShadow: shadow,
        color: "var(--txt)",
        userSelect: "none",
        cursor: "grab",
        overflow: "visible",
        transition: "box-shadow .15s, border-color .15s",
      }}
    >
      {/* Header band — category-tinted */}
      <div
        style={{
          height: headerH,
          padding: "0 10px",
          display: "flex",
          alignItems: "center",
          gap: 9,
          background: `linear-gradient(180deg, ${tint} 0%, transparent 100%)`,
          borderBottom: `1px solid ${border}`,
          borderTopLeftRadius: 2,
          borderTopRightRadius: 2,
          position: "relative",
        }}
      >
        <div
          style={{
            position: "absolute",
            left: 0,
            top: 0,
            bottom: 0,
            width: 3,
            background: base,
            borderTopLeftRadius: 2,
            borderBottomLeftRadius: 2,
            boxShadow: glow ? `0 0 10px ${glowCol}` : "none",
          }}
        />
        <div
          style={{
            width: density === "compact" ? 22 : 28,
            height: density === "compact" ? 22 : 28,
            marginLeft: 3,
            borderRadius: 2,
            background: glow ? base : tint,
            border: `1px solid ${glow ? base : border}`,
            display: "inline-flex",
            alignItems: "center",
            justifyContent: "center",
            color: glow ? "var(--btn-primary-fg)" : base,
            flexShrink: 0,
            boxShadow: glow
              ? `0 0 14px ${glowCol}, inset 0 0 0 1px oklch(1 0 0 / .25)`
              : "none",
            transition: "background .15s, color .15s, box-shadow .15s",
          }}
        >
          <NodeIcon
            category={category}
            size={density === "compact" ? 14 : 18}
            color="currentColor"
            sw={1.7}
          />
        </div>
        <span
          style={{
            fontSize: 11.5,
            color: base,
            fontWeight: 700,
            letterSpacing: "0.01em",
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
            minWidth: 0,
          }}
        >
          {typeLabel}
        </span>
        <div style={{ flex: 1 }} />
        <span
          style={{
            fontSize: 9.5,
            color: "var(--txt-faint)",
            fontVariantNumeric: "tabular-nums",
          }}
        >
          #{idHash(node.id)}
        </span>
        {statusColor && runStatus !== "pending" ? (
          <span
            style={{
              width: 8,
              height: 8,
              borderRadius: 8,
              background: statusColor,
              boxShadow: glow
                ? `0 0 10px ${statusColor}, 0 0 0 2px oklch(0.13 0.005 245 / .8)`
                : "none",
              animation: glow ? "pulse 1.1s ease-in-out infinite" : undefined,
            }}
          />
        ) : null}
      </div>

      {/* Body — workflow-given name + part-number footer */}
      {density !== "compact" ? (
        <div style={{ padding: "7px 10px 4px", minHeight: 0 }}>
          <div
            style={{
              fontFamily: "var(--display)",
              fontWeight: 600,
              fontSize: density === "rich" ? 14 : 13,
              letterSpacing: "-0.005em",
              color: "var(--txt)",
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
            }}
            title={displayName}
          >
            {displayName}
          </div>
          {density === "rich" && nodeType?.description ? (
            <div
              style={{
                marginTop: 4,
                fontSize: 10.5,
                color: "var(--txt-dim)",
                lineHeight: 1.4,
                display: "-webkit-box",
                WebkitLineClamp: 2,
                WebkitBoxOrient: "vertical",
                overflow: "hidden",
              }}
            >
              {nodeType.description}
            </div>
          ) : null}
        </div>
      ) : null}

      {/* Part-number footer ribbon (rich + standard only) */}
      {density !== "compact" ? (
        <div
          style={{
            position: "absolute",
            left: 0,
            right: 0,
            bottom: 0,
            height: 18,
            padding: "0 10px",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            background: "var(--bg-input)",
            borderTop: `1px solid ${border}`,
            borderBottomLeftRadius: 2,
            borderBottomRightRadius: 2,
            fontFamily: "var(--mono)",
            fontSize: 9.5,
            color: "var(--txt-faint)",
          }}
        >
          <span style={{ letterSpacing: "0.08em" }}>
            {cat.sigil}-{partNumber}
          </span>
          <span>
            {nodeType?.inputs.length ?? 0}↓ {nodeType?.outputs.length ?? 0}↑
          </span>
        </div>
      ) : null}

      {/* Input pins (left side) */}
      {nodeType?.inputs.map((port) => (
        <Pin
          key={`in-${port.name}`}
          y={portY(nodeType.inputs.map((p) => p.name), port.name, density)}
          label={port.name}
          side="in"
          color={base}
          nodeId={node.id}
        />
      ))}
      {/* Output pins (right side) */}
      {nodeType?.outputs.map((port) => (
        <Pin
          key={`out-${port.name}`}
          y={portY(nodeType.outputs.map((p) => p.name), port.name, density)}
          label={port.name}
          side="out"
          color={base}
          cardWidth={w}
          nodeId={node.id}
          {...(onPortConnectStart ? { onConnectStart: onPortConnectStart } : {})}
        />
      ))}
    </div>
  );
}

interface PinProps {
  y: number;
  label: string;
  side: "in" | "out";
  color: string;
  cardWidth?: number;
  nodeId: string;
  /** Output-pin only — fires when the user starts dragging from
   * this pin to wire it to another node. */
  onConnectStart?: (
    nodeId: string,
    portName: string,
    screenX: number,
    screenY: number,
  ) => void;
}

function Pin({
  y,
  label,
  side,
  color,
  cardWidth = 0,
  nodeId,
  onConnectStart,
}: PinProps): JSX.Element {
  const isOut = side === "out";
  return (
    <div
      style={{
        position: "absolute",
        top: y,
        left: isOut ? cardWidth : -PIN_W,
        display: "flex",
        flexDirection: isOut ? "row" : "row-reverse",
        alignItems: "center",
        gap: 4,
        height: 12,
        pointerEvents: "none",
      }}
    >
      <div
        data-port-node-id={nodeId}
        data-port-name={label}
        data-port-side={side}
        onPointerDown={
          side === "out" && onConnectStart
            ? (event) => {
                if (event.button !== 0) return;
                event.stopPropagation();
                onConnectStart(nodeId, label, event.clientX, event.clientY);
              }
            : undefined
        }
        style={{
          width: PIN_W,
          height: 4,
          background: color,
          boxShadow: `0 0 4px ${color}`,
          pointerEvents: "auto",
          cursor: "crosshair",
          flexShrink: 0,
        }}
      />
      <span
        style={{
          fontSize: 9,
          color: "var(--txt-soft)",
          background: "var(--bg-panel)",
          padding: "1px 5px",
          borderRadius: 1,
          border: `1px solid ${color}`,
          whiteSpace: "nowrap",
          fontVariantNumeric: "tabular-nums",
          letterSpacing: "0.02em",
        }}
      >
        {label}
      </span>
    </div>
  );
}

function computePartNumber(id: string): string {
  let h = 0;
  for (const ch of id) {
    h = ((h << 5) - h + ch.charCodeAt(0)) | 0;
  }
  return String((Math.abs(h) % 99) + 1).padStart(2, "0");
}
