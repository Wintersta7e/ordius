// Node-type palette — categorised, collapsible, filterable list of
// every NodeType the engine has registered. Click an item to drop
// a new node at the viewport centre (drag-to-canvas with ghost
// preview is deferred per the design handoff's v1.x list).

import { useMemo, useState } from "react";
import type { JSX } from "react";

import type { Category, NodeType } from "../../engine/types";
import { CATEGORIES, CATEGORY_LIST, catColor } from "../../data/categories";
import { Ic, NodeIcon } from "../icons";
import { BracketHeader } from "./BracketHeader";

interface Props {
  nodeTypes: NodeType[];
  onAdd: (typeId: string) => void;
}

export function Palette({ nodeTypes, onAdd }: Props): JSX.Element {
  const [query, setQuery] = useState("");
  const [collapsed, setCollapsed] = useState<Record<string, boolean>>({});

  const filtered = useMemo<NodeType[]>(() => {
    const needle = query.trim().toLowerCase();
    if (!needle) return nodeTypes;
    return nodeTypes.filter(
      (t) =>
        t.id.toLowerCase().includes(needle) ||
        t.name.toLowerCase().includes(needle) ||
        t.description.toLowerCase().includes(needle),
    );
  }, [nodeTypes, query]);

  const byCategory = useMemo<Record<Category, NodeType[]>>(() => {
    const map: Record<Category, NodeType[]> = {
      execution: [],
      llm: [],
      data: [],
      control: [],
      integration: [],
    };
    for (const t of filtered) {
      map[t.category].push(t);
    }
    return map;
  }, [filtered]);

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
        background: "var(--bg-panel)",
        borderRight: "1px solid var(--line)",
        minHeight: 0,
      }}
    >
      <BracketHeader label="nodes" suffix={`${nodeTypes.length} types`} />

      <div
        style={{
          padding: "6px 10px 8px",
          borderBottom: "1px solid var(--line-soft)",
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 6,
            background: "var(--bg-input)",
            border: "1px solid var(--line)",
            padding: "4px 8px",
            borderRadius: 3,
          }}
        >
          <span style={{ color: "var(--txt-faint)", fontSize: 11 }}>/</span>
          <input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="filter…"
            aria-label="Filter node types"
            style={{
              flex: 1,
              background: "transparent",
              border: 0,
              outline: "none",
              color: "var(--txt)",
              fontFamily: "var(--mono)",
              fontSize: 11.5,
              padding: 0,
            }}
          />
          {query ? (
            <button
              type="button"
              onClick={() => setQuery("")}
              title="Clear filter"
              style={{
                background: "transparent",
                border: 0,
                color: "var(--txt-faint)",
                cursor: "pointer",
                padding: 0,
                display: "inline-flex",
              }}
            >
              {Ic["x"]?.({ size: 11 })}
            </button>
          ) : null}
        </div>
      </div>

      <div style={{ flex: 1, overflow: "auto", padding: "2px 0 60px" }}>
        {CATEGORY_LIST.map((cat) => {
          const items = byCategory[cat.id];
          if (query && items.length === 0) return null;
          const open = !collapsed[cat.id];
          return (
            <div key={cat.id}>
              <button
                type="button"
                onClick={() =>
                  setCollapsed((current) => ({
                    ...current,
                    [cat.id]: open,
                  }))
                }
                style={{
                  width: "100%",
                  display: "flex",
                  alignItems: "center",
                  gap: 8,
                  padding: "8px 10px 8px 8px",
                  background: "transparent",
                  border: 0,
                  cursor: "pointer",
                  color: "var(--txt)",
                  fontFamily: "var(--mono)",
                  fontSize: 11.5,
                  textAlign: "left",
                  borderTop: "1px solid var(--line-soft)",
                }}
              >
                <span
                  style={{
                    width: 3,
                    height: 18,
                    background: catColor(cat.id, "base"),
                    boxShadow: `0 0 6px ${catColor(cat.id, "glow")}`,
                    flexShrink: 0,
                  }}
                />
                <span
                  style={{
                    color: catColor(cat.id, "base"),
                    fontWeight: 600,
                    letterSpacing: "0.04em",
                  }}
                >
                  {cat.label}
                </span>
                <span
                  className="num"
                  style={{ color: "var(--txt-faint)", fontSize: 10 }}
                >
                  [{items.length}]
                </span>
                <div style={{ flex: 1 }} />
                <span
                  style={{
                    color: "var(--txt-faint)",
                    display: "inline-flex",
                  }}
                >
                  {open
                    ? Ic["chevR"]?.({ size: 11 })
                    : Ic["chevR"]?.({ size: 11 })}
                </span>
              </button>

              {open ? (
                <div style={{ paddingBottom: 4 }}>
                  {items.map((t, idx) => (
                    <PaletteItem
                      key={t.id}
                      type={t}
                      idx={idx}
                      onAdd={onAdd}
                    />
                  ))}
                  {items.length === 0 ? (
                    <div
                      style={{
                        padding: "4px 14px 8px",
                        color: "var(--txt-faint)",
                        fontSize: 10.5,
                      }}
                    >
                      no matches
                    </div>
                  ) : null}
                </div>
              ) : null}
            </div>
          );
        })}
      </div>
    </div>
  );
}

interface ItemProps {
  type: NodeType;
  idx: number;
  onAdd: (typeId: string) => void;
}

function PaletteItem({ type, idx, onAdd }: ItemProps): JSX.Element {
  const [hover, setHover] = useState(false);
  const base = catColor(type.category, "base");
  const tint = catColor(type.category, "tint");
  return (
    <div
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      onClick={() => onAdd(type.id)}
      title={`${type.id} — ${type.description}`}
      style={{
        display: "flex",
        alignItems: "center",
        gap: 10,
        padding: "8px 12px 8px 10px",
        cursor: "grab",
        background: hover ? "var(--bg-hover)" : "transparent",
        borderLeft: `2px solid ${hover ? base : "transparent"}`,
        position: "relative",
      }}
    >
      <span
        className="num"
        style={{
          fontSize: 9.5,
          color: "var(--txt-faint)",
          flexShrink: 0,
          minWidth: 16,
          textAlign: "right",
        }}
      >
        {String(idx + 1).padStart(2, "0")}
      </span>
      <span
        style={{
          width: 22,
          height: 22,
          flexShrink: 0,
          display: "inline-flex",
          alignItems: "center",
          justifyContent: "center",
          color: base,
          background: hover ? tint : "transparent",
          borderRadius: 3,
          transition: "background .12s",
        }}
      >
        <NodeIcon category={type.category} size={16} color={base} sw={1.7} />
      </span>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ display: "flex", gap: 6, alignItems: "baseline" }}>
          <span
            style={{
              fontSize: 12,
              color: "var(--txt)",
              fontWeight: 500,
            }}
          >
            {type.name || type.id}
          </span>
          <span
            style={{
              fontSize: 9,
              color: "var(--txt-faint)",
              letterSpacing: "0.05em",
              textTransform: "uppercase",
            }}
          >
            {CATEGORIES[type.category].sigil}
          </span>
        </div>
        <div
          style={{
            fontSize: 10.5,
            color: "var(--txt-faint)",
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
            fontFamily: "var(--mono)",
          }}
        >
          {type.id}
        </div>
      </div>
    </div>
  );
}
