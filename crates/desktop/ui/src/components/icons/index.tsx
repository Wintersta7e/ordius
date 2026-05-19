// Per-category glyphs, the UI icon set, and per-node-type icons.
//
// Ported from `docs/UI/js/icons.jsx`. All monoline SVG drawn with
// currentColor so a single style cascade tints everything.

import type { CSSProperties, JSX, ReactNode } from "react";

import type { Category } from "../../engine/types";
import { CATEGORIES } from "../../data/categories";

interface GlyphProps {
  kind: "circle" | "square" | "diamond" | "triangle" | "hex";
  size?: number;
  fill?: string;
  stroke?: string;
  strokeWidth?: number;
  style?: CSSProperties;
}

/** Category shape badge (circle / square / hex / diamond / triangle). */
export function Glyph({
  kind,
  size = 14,
  fill = "none",
  stroke = "currentColor",
  strokeWidth = 1.5,
  style,
}: GlyphProps): JSX.Element | null {
  const c = size / 2;
  const r = size * 0.42;
  const baseProps = {
    width: size,
    height: size,
    viewBox: `0 0 ${size} ${size}`,
    style,
    "aria-hidden": true,
  } as const;
  if (kind === "circle") {
    return (
      <svg {...baseProps}>
        <circle
          cx={c}
          cy={c}
          r={r}
          fill={fill}
          stroke={stroke}
          strokeWidth={strokeWidth}
        />
      </svg>
    );
  }
  if (kind === "square") {
    return (
      <svg {...baseProps}>
        <rect
          x={c - r}
          y={c - r}
          width={r * 2}
          height={r * 2}
          fill={fill}
          stroke={stroke}
          strokeWidth={strokeWidth}
        />
      </svg>
    );
  }
  if (kind === "diamond") {
    return (
      <svg {...baseProps}>
        <polygon
          points={`${c},${c - r} ${c + r},${c} ${c},${c + r} ${c - r},${c}`}
          fill={fill}
          stroke={stroke}
          strokeWidth={strokeWidth}
        />
      </svg>
    );
  }
  if (kind === "triangle") {
    return (
      <svg {...baseProps}>
        <polygon
          points={`${c},${c - r} ${c + r * 0.92},${c + r * 0.7} ${c - r * 0.92},${c + r * 0.7}`}
          fill={fill}
          stroke={stroke}
          strokeWidth={strokeWidth}
        />
      </svg>
    );
  }
  // hex
  const pts: string[] = [];
  for (let i = 0; i < 6; i += 1) {
    const a = (Math.PI / 3) * i - Math.PI / 6;
    pts.push(
      `${(c + r * Math.cos(a)).toFixed(2)},${(c + r * Math.sin(a)).toFixed(2)}`,
    );
  }
  return (
    <svg {...baseProps}>
      <polygon
        points={pts.join(" ")}
        fill={fill}
        stroke={stroke}
        strokeWidth={strokeWidth}
      />
    </svg>
  );
}

/** Tiny base SVG used by `Ic` icons (24×24 viewBox, rounded caps). */
interface SvgProps {
  children: ReactNode;
  size?: number;
  stroke?: string;
  sw?: number;
  fill?: string;
  style?: CSSProperties;
}

export function SVG({
  children,
  size = 16,
  stroke = "currentColor",
  sw = 1.7,
  fill = "none",
  style,
}: SvgProps): JSX.Element {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill={fill}
      stroke={stroke}
      strokeWidth={sw}
      strokeLinecap="round"
      strokeLinejoin="round"
      style={style}
      aria-hidden="true"
    >
      {children}
    </svg>
  );
}

/** Common chrome / topbar icons. */
type IconRenderer = (props?: Partial<SvgProps>) => JSX.Element;

export const Ic: Record<string, IconRenderer> = {
  play: (p) => (
    <SVG {...p}>
      <polygon
        points="5 3 19 12 5 21 5 3"
        fill="currentColor"
        stroke="none"
      />
    </SVG>
  ),
  stop: (p) => (
    <SVG {...p}>
      <rect
        x="5"
        y="5"
        width="14"
        height="14"
        rx="1"
        fill="currentColor"
        stroke="none"
      />
    </SVG>
  ),
  check: (p) => (
    <SVG {...p}>
      <polyline points="4 12 10 18 20 6" />
    </SVG>
  ),
  x: (p) => (
    <SVG {...p}>
      <line x1="5" y1="5" x2="19" y2="19" />
      <line x1="19" y1="5" x2="5" y2="19" />
    </SVG>
  ),
  search: (p) => (
    <SVG {...p}>
      <circle cx="11" cy="11" r="6" />
      <line x1="16" y1="16" x2="21" y2="21" />
    </SVG>
  ),
  save: (p) => (
    <SVG {...p}>
      <path d="M5 4h11l3 3v13H5z" />
      <path d="M7 4v6h9V4" />
      <path d="M7 14h10" />
    </SVG>
  ),
  rerun: (p) => (
    <SVG {...p}>
      <path d="M4 12a8 8 0 0 1 14-5l2-2v6h-6l2-2A6 6 0 0 0 6 12" />
      <path d="M20 12a8 8 0 0 1-14 5l-2 2v-6h6l-2 2a6 6 0 0 0 12-3" />
    </SVG>
  ),
  log: (p) => (
    <SVG {...p}>
      <line x1="4" y1="6" x2="20" y2="6" />
      <line x1="4" y1="12" x2="20" y2="12" />
      <line x1="4" y1="18" x2="14" y2="18" />
    </SVG>
  ),
  cog: (p) => (
    <SVG {...p}>
      <circle cx="12" cy="12" r="3" />
      <path d="M12 1.5v3M12 19.5v3M4.2 4.2l2.1 2.1M17.7 17.7l2.1 2.1M1.5 12h3M19.5 12h3M4.2 19.8l2.1-2.1M17.7 6.3l2.1-2.1" />
    </SVG>
  ),
  moon: (p) => (
    <SVG {...p}>
      <path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z" />
    </SVG>
  ),
  sun: (p) => (
    <SVG {...p}>
      <circle cx="12" cy="12" r="4" />
      <path d="M12 2v3M12 19v3M4.2 4.2l2.1 2.1M17.7 17.7l2.1 2.1M2 12h3M19 12h3M4.2 19.8l2.1-2.1M17.7 6.3l2.1-2.1" />
    </SVG>
  ),
  chevR: (p) => (
    <SVG {...p}>
      <polyline points="9 6 15 12 9 18" />
    </SVG>
  ),
  arrowR: (p) => (
    <SVG {...p}>
      <line x1="4" y1="12" x2="20" y2="12" />
      <polyline points="14 6 20 12 14 18" />
    </SVG>
  ),
  bolt: (p) => (
    <SVG {...p}>
      <polygon
        points="13 2 4 14 11 14 9 22 20 9 13 9 13 2"
        fill="currentColor"
        stroke="none"
      />
    </SVG>
  ),
};

/**
 * Render a fallback glyph keyed by category. The engine returns
 * each `NodeType.icon` as a Lucide id today; mapping those into
 * crisp SVGs is a v1.1+ polish item, so for now we lean on the
 * category glyph for everything.
 */
export function NodeIcon({
  category,
  size = 16,
  color = "currentColor",
  sw = 1.7,
}: {
  category: Category;
  size?: number;
  color?: string;
  sw?: number;
}): JSX.Element {
  const meta = CATEGORIES[category];
  return <Glyph kind={meta.glyph} size={size} stroke={color} strokeWidth={sw} />;
}
