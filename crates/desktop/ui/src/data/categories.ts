// Per-category visual identity + the `catColor()` helper.
//
// Ported verbatim from `docs/UI/js/data.jsx`. The engine returns
// the actual node-type catalog at runtime via `list_node_types`;
// here we only carry the design-side metadata the engine doesn't
// model (jewel-tone hue, primary glyph shape, one-letter sigil).

import type { Category } from "../engine/types";

/** Per-category metadata. */
export interface CategoryMeta {
  /** Lower-case category id, matches the engine's enum. */
  id: Category;
  /** Human label rendered in palette headers. */
  label: string;
  /** Hue (oklch H component) for jewel-tone derivatives. */
  hue: number;
  /** Primary glyph shape rendered next to node names. */
  glyph: "square" | "hex" | "circle" | "diamond" | "triangle";
  /** One-letter sigil ("E", "L", "D", "C", "I"). */
  sigil: string;
  /** Subtitle / palette tooltip. */
  desc: string;
}

/** Lookup table by category id. */
export const CATEGORIES: Record<Category, CategoryMeta> = {
  execution: {
    id: "execution",
    label: "execution",
    hue: 70,
    glyph: "square",
    sigil: "E",
    desc: "run things",
  },
  llm: {
    id: "llm",
    label: "llm",
    hue: 305,
    glyph: "hex",
    sigil: "L",
    desc: "language models",
  },
  data: {
    id: "data",
    label: "data",
    hue: 200,
    glyph: "circle",
    sigil: "D",
    desc: "shape & store",
  },
  control: {
    id: "control",
    label: "control",
    hue: 25,
    glyph: "diamond",
    sigil: "C",
    desc: "flow & branch",
  },
  integration: {
    id: "integration",
    label: "integration",
    hue: 152,
    glyph: "triangle",
    sigil: "I",
    desc: "outbound calls",
  },
};

/** Categories in canonical palette order. */
export const CATEGORY_LIST: CategoryMeta[] = [
  CATEGORIES.execution,
  CATEGORIES.llm,
  CATEGORIES.data,
  CATEGORIES.control,
  CATEGORIES.integration,
];

/** Which colour role a `catColor()` call returns. */
export type CatColorKind =
  /** Bright accent (hover glow). */
  | "glow"
  /** Standard accent (node card border, palette dot). */
  | "base"
  /** Muted accent (inactive surfaces). */
  | "dim"
  /** Tinted background fill (24% alpha card body). */
  | "tint"
  /** Border colour (42% alpha). */
  | "border"
  /** Filled chip background (palette swatch). */
  | "fill";

interface ToneTuning {
  glow: [number, number];
  base: [number, number];
  dim: [number, number];
  tintL: number;
  tintC: number;
  tintA: number;
  bordL: number;
  bordC: number;
  bordA: number;
  fillL: number;
  fillC: number;
}

const DARK_TONE: ToneTuning = {
  glow: [0.78, 0.22],
  base: [0.74, 0.19],
  dim: [0.5, 0.11],
  tintL: 0.74,
  tintC: 0.19,
  tintA: 0.14,
  bordL: 0.74,
  bordC: 0.19,
  bordA: 0.42,
  fillL: 0.3,
  fillC: 0.09,
};

const LIGHT_TONE: ToneTuning = {
  glow: [0.5, 0.22],
  base: [0.46, 0.22],
  dim: [0.4, 0.14],
  tintL: 0.66,
  tintC: 0.18,
  tintA: 0.14,
  bordL: 0.5,
  bordC: 0.22,
  bordA: 0.42,
  fillL: 0.85,
  fillC: 0.1,
};

/** Pick a tone tuning by the current `data-theme` attribute. */
function activeTone(): ToneTuning {
  if (
    typeof document !== "undefined" &&
    document.documentElement.dataset["theme"] === "light"
  ) {
    return LIGHT_TONE;
  }
  return DARK_TONE;
}

/**
 * Resolve a per-category oklch colour string suitable for direct
 * CSS use. Theme-aware via the `data-theme` attribute on
 * `<html>` — light mode shifts to lower lightness so colours read
 * against the engineering-paper light surfaces.
 */
export function catColor(catId: Category, kind: CatColorKind = "base"): string {
  const meta = CATEGORIES[catId];
  const t = activeTone();
  switch (kind) {
    case "glow":
      return `oklch(${t.glow[0]} ${t.glow[1]} ${meta.hue})`;
    case "base":
      return `oklch(${t.base[0]} ${t.base[1]} ${meta.hue})`;
    case "dim":
      return `oklch(${t.dim[0]} ${t.dim[1]} ${meta.hue})`;
    case "tint":
      return `oklch(${t.tintL} ${t.tintC} ${meta.hue} / ${t.tintA})`;
    case "border":
      return `oklch(${t.bordL} ${t.bordC} ${meta.hue} / ${t.bordA})`;
    case "fill":
      return `oklch(${t.fillL} ${t.fillC} ${meta.hue})`;
  }
}
