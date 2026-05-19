// Shared constants + helpers for the workflow canvas.

import type { Category, Edge, Node, NodeType } from "../../engine/types";

/** Visual density of node cards. Settings → Appearance picks it. */
export type Density = "compact" | "standard" | "rich";

/** Card pixel dimensions per density. */
export const NODE_W: Record<Density, number> = {
  compact: 168,
  standard: 208,
  rich: 232,
};

export const NODE_H: Record<Density, number> = {
  compact: 56,
  standard: 108,
  rich: 162,
};

/** Pin protrusion outside the card edge. */
export const PIN_W = 12;

/** Last 4 chars of an alpha-stripped id — used as a part-number badge. */
export function idHash(id: string): string {
  return id.replace(/[^a-z0-9]/gi, "").slice(-4).toLowerCase();
}

/** Cache-friendly category → NodeType lookup. */
export function nodeTypeIndex(types: NodeType[]): Map<string, NodeType> {
  const map = new Map<string, NodeType>();
  for (const t of types) map.set(t.id, t);
  return map;
}

/** Y-offset of a port within a node card, in world pixels. */
export function portY(
  ports: string[],
  port: string,
  density: Density,
): number {
  const headerH = density === "compact" ? 30 : 40;
  const footerH = density === "compact" ? 0 : 18;
  const portArea = NODE_H[density] - headerH - footerH - 10;
  const idx = ports.indexOf(port);
  if (idx < 0) return NODE_H[density] / 2;
  const step = portArea / Math.max(ports.length, 1);
  return headerH + 6 + step * idx + step / 2;
}

/** World-space position of a port's pin tip. */
export function portTip(
  node: Node,
  ports: string[],
  port: string,
  side: "in" | "out",
  density: Density,
): { x: number; y: number } {
  const pinExtend = density === "compact" ? 0 : PIN_W - 1;
  return {
    x: node.pos.x + (side === "out" ? NODE_W[density] + pinExtend : -pinExtend),
    y: node.pos.y + portY(ports, port, density) + 2,
  };
}

/** SVG `d` for a connection between two world points. */
export function edgePath(
  style: "bezier" | "orthogonal" | "straight",
  a: { x: number; y: number },
  b: { x: number; y: number },
): string {
  const dx = b.x - a.x;
  if (style === "straight") return `M ${a.x} ${a.y} L ${b.x} ${b.y}`;
  if (style === "orthogonal") {
    const mid = a.x + Math.max(40, dx / 2);
    return `M ${a.x} ${a.y} L ${mid} ${a.y} L ${mid} ${b.y} L ${b.x} ${b.y}`;
  }
  const hx = Math.max(40, Math.abs(dx) * 0.5);
  return `M ${a.x} ${a.y} C ${a.x + hx} ${a.y}, ${b.x - hx} ${b.y}, ${b.x} ${b.y}`;
}

/** Loop edges arc below and bend back so they're visually distinct. */
export function loopPath(
  a: { x: number; y: number },
  b: { x: number; y: number },
): string {
  const dropY = Math.max(a.y, b.y) + 220;
  const midX = (a.x + b.x) / 2;
  return `M ${a.x} ${a.y} C ${a.x + 80} ${a.y + 80}, ${a.x + 80} ${dropY}, ${midX} ${dropY} S ${b.x - 80} ${dropY}, ${b.x} ${b.y}`;
}

/** Resolve a node's category for visual tinting. */
export function nodeCategory(
  node: Node,
  index: Map<string, NodeType>,
): Category {
  return index.get(node.type)?.category ?? "control";
}

/** Convenience reference re-export — Edge is shared with the engine. */
export type { Edge, Node, NodeType };
