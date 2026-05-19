// Compact inline-flex segmented control. Sized to content, used
// everywhere the user picks one of a small set of mutually
// exclusive options (sort orders, status filters, time ranges).

import type { JSX } from "react";

export interface PillOption<T extends string> {
  id: T;
  label: string;
}

interface Props<T extends string> {
  value: T;
  options: ReadonlyArray<PillOption<T> | T>;
  onChange: (id: T) => void;
}

export function PillRow<T extends string>({
  value,
  options,
  onChange,
}: Props<T>): JSX.Element {
  return (
    <div
      style={{
        display: "inline-flex",
        background: "var(--bg-input)",
        border: "1px solid var(--line)",
        borderRadius: 3,
        padding: 2,
        fontFamily: "var(--mono)",
      }}
    >
      {options.map((o) => {
        const id = typeof o === "string" ? o : o.id;
        const label = typeof o === "string" ? o : o.label;
        const active = id === value;
        return (
          <button
            key={id}
            type="button"
            onClick={() => onChange(id)}
            style={{
              appearance: "none",
              border: 0,
              background: active ? "var(--bg-active)" : "transparent",
              color: active ? "var(--txt)" : "var(--txt-dim)",
              fontFamily: "var(--mono)",
              fontSize: 11,
              padding: "3px 10px",
              height: 20,
              borderRadius: 2,
              cursor: "pointer",
            }}
          >
            {label}
          </button>
        );
      })}
    </div>
  );
}
