// Form primitives reused across the properties panel + dialogs.
//
// Visually anchored to tokens.css — every primitive reads from
// CSS variables so the cascade swaps for dark/light without
// touching the components.

import type { ChangeEvent, JSX, ReactNode } from "react";
import { useState } from "react";

interface SectionProps {
  label: string;
  suffix?: string;
  children: ReactNode;
}

export function Section({ label, suffix, children }: SectionProps): JSX.Element {
  return (
    <div>
      <div
        style={{
          padding: "18px 16px 10px",
          display: "flex",
          alignItems: "center",
          gap: 8,
          borderTop: "1px solid var(--line-soft)",
          marginTop: 6,
        }}
      >
        <span style={{ color: "var(--txt-faint)" }}>├</span>
        <span
          style={{
            fontSize: 10,
            fontWeight: 700,
            textTransform: "uppercase",
            letterSpacing: "0.16em",
            color: "var(--txt-soft)",
          }}
        >
          {label}
        </span>
        <span
          style={{
            flex: 1,
            height: 1,
            alignSelf: "center",
            background:
              "linear-gradient(90deg, var(--line) 0%, var(--line-soft) 100%)",
          }}
        />
        {suffix ? (
          <span
            style={{
              fontSize: 10,
              color: "var(--txt-faint)",
              fontFamily: "var(--mono)",
              letterSpacing: "0.02em",
            }}
          >
            {suffix}
          </span>
        ) : null}
      </div>
      <div>{children}</div>
    </div>
  );
}

interface FieldProps {
  label: string;
  hint?: string | undefined;
  children: ReactNode;
}

export function Field({ label, hint, children }: FieldProps): JSX.Element {
  return (
    <div style={{ padding: "6px 16px 10px" }}>
      <div
        style={{
          display: "flex",
          alignItems: "baseline",
          gap: 6,
          marginBottom: 5,
        }}
      >
        <span
          style={{
            fontSize: 10.5,
            color: "var(--txt-faint)",
            letterSpacing: "0.02em",
          }}
        >
          {label}
        </span>
        {hint ? (
          <span
            style={{
              fontSize: 9.5,
              color: "var(--txt-soft)",
              fontFamily: "var(--mono)",
              opacity: 0.7,
            }}
          >
            {hint}
          </span>
        ) : null}
      </div>
      {children}
    </div>
  );
}

interface InputProps {
  value: string;
  onChange?: ((v: string) => void) | undefined;
  placeholder?: string;
  disabled?: boolean;
}

export function TextInput({
  value,
  onChange,
  placeholder,
  disabled,
}: InputProps): JSX.Element {
  const [focused, setFocused] = useState(false);
  return (
    <input
      value={value}
      onChange={(event: ChangeEvent<HTMLInputElement>) =>
        onChange?.(event.target.value)
      }
      placeholder={placeholder}
      disabled={disabled}
      onFocus={() => setFocused(true)}
      onBlur={() => setFocused(false)}
      style={{
        width: "100%",
        background: "var(--bg-input)",
        border: `1px solid ${focused ? "var(--accent)" : "var(--line)"}`,
        color: "var(--txt)",
        fontFamily: "var(--mono)",
        fontSize: 12,
        padding: "7px 10px",
        borderRadius: 3,
        outline: "none",
        opacity: disabled ? 0.5 : 1,
      }}
    />
  );
}

export function TextArea({
  value,
  onChange,
  placeholder,
  disabled,
}: InputProps): JSX.Element {
  const [focused, setFocused] = useState(false);
  const rows = Math.min(8, Math.max(3, value.split("\n").length));
  return (
    <textarea
      value={value}
      placeholder={placeholder}
      disabled={disabled}
      rows={rows}
      onChange={(event: ChangeEvent<HTMLTextAreaElement>) =>
        onChange?.(event.target.value)
      }
      onFocus={() => setFocused(true)}
      onBlur={() => setFocused(false)}
      style={{
        width: "100%",
        background: "var(--bg-input)",
        border: `1px solid ${focused ? "var(--accent)" : "var(--line)"}`,
        color: "var(--txt)",
        fontFamily: "var(--mono)",
        fontSize: 11.5,
        padding: "7px 10px",
        borderRadius: 3,
        outline: "none",
        resize: "vertical",
        lineHeight: 1.5,
        opacity: disabled ? 0.5 : 1,
      }}
    />
  );
}

interface NumberInputProps {
  value: number | string;
  onChange?: (v: number) => void;
  placeholder?: string;
}

export function NumberInput({
  value,
  onChange,
  placeholder,
}: NumberInputProps): JSX.Element {
  const [focused, setFocused] = useState(false);
  return (
    <input
      type="number"
      value={value}
      placeholder={placeholder}
      onChange={(event) => {
        const parsed = Number(event.target.value);
        if (!Number.isNaN(parsed)) onChange?.(parsed);
      }}
      onFocus={() => setFocused(true)}
      onBlur={() => setFocused(false)}
      style={{
        width: "100%",
        background: "var(--bg-input)",
        border: `1px solid ${focused ? "var(--accent)" : "var(--line)"}`,
        color: "var(--txt)",
        fontFamily: "var(--mono)",
        fontSize: 12,
        padding: "7px 10px",
        borderRadius: 3,
        outline: "none",
      }}
    />
  );
}

interface ToggleProps {
  value: boolean;
  onChange?: (v: boolean) => void;
}

export function Toggle({ value, onChange }: ToggleProps): JSX.Element {
  return (
    <button
      type="button"
      onClick={() => onChange?.(!value)}
      aria-pressed={value}
      style={{
        appearance: "none",
        border: `1px solid ${value ? "var(--accent)" : "var(--line)"}`,
        background: value ? "var(--accent)" : "var(--bg-input)",
        width: 36,
        height: 20,
        borderRadius: 12,
        padding: 2,
        cursor: "pointer",
        display: "inline-flex",
        alignItems: "center",
        justifyContent: value ? "flex-end" : "flex-start",
        transition: "background .12s, justify-content .12s",
      }}
    >
      <span
        style={{
          width: 14,
          height: 14,
          borderRadius: 7,
          background: value ? "var(--btn-primary-fg)" : "var(--txt-dim)",
          display: "block",
        }}
      />
    </button>
  );
}

interface SegRowProps {
  value: string;
  options: ReadonlyArray<string | { id: string; label: string }>;
  onChange?: (v: string) => void;
}

export function SegRow({ value, options, onChange }: SegRowProps): JSX.Element {
  return (
    <div
      style={{
        display: "flex",
        background: "var(--bg-input)",
        border: "1px solid var(--line)",
        borderRadius: 3,
        padding: 3,
      }}
    >
      {options.map((option) => {
        const id = typeof option === "string" ? option : option.id;
        const label = typeof option === "string" ? option : option.label;
        const active = id === value;
        return (
          <button
            key={id}
            type="button"
            onClick={() => onChange?.(id)}
            style={{
              flex: 1,
              height: 24,
              padding: 0,
              border: 0,
              borderRadius: 2,
              background: active ? "var(--bg-active)" : "transparent",
              color: active ? "var(--txt)" : "var(--txt-dim)",
              fontSize: 11,
              fontFamily: "var(--mono)",
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

export function Mono({ value }: { value: string }): JSX.Element {
  return (
    <div
      style={{
        background: "var(--bg-input)",
        border: "1px solid var(--line-soft)",
        color: "var(--txt-dim)",
        fontFamily: "var(--mono)",
        fontSize: 11.5,
        padding: "7px 10px",
        borderRadius: 3,
        overflow: "hidden",
        textOverflow: "ellipsis",
        whiteSpace: "nowrap",
      }}
      title={value}
    >
      {value}
    </div>
  );
}

interface KVProps {
  k: string;
  v: string | number;
}

export function KV({ k, v }: KVProps): JSX.Element {
  return (
    <div
      style={{
        display: "flex",
        justifyContent: "space-between",
        padding: "5px 16px",
        fontSize: 11.5,
      }}
    >
      <span style={{ color: "var(--txt-faint)" }}>{k}</span>
      <span className="num" style={{ color: "var(--txt-dim)" }}>
        {v}
      </span>
    </div>
  );
}

interface SpecPillProps {
  label: string;
  value: number | string;
}

export function SpecPill({ label, value }: SpecPillProps): JSX.Element {
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "baseline",
        gap: 4,
        padding: "1px 6px",
        borderRadius: 2,
        border: "1px solid var(--line)",
        background: "var(--bg-elevated)",
        fontFamily: "var(--mono)",
      }}
    >
      <span style={{ color: "var(--txt-faint)", fontSize: 9 }}>{label}</span>
      <span
        className="num"
        style={{ color: "var(--txt)", fontSize: 10.5, fontWeight: 600 }}
      >
        {value}
      </span>
    </span>
  );
}

const TYPE_HUES: Record<string, { hue: number; label: string }> = {
  string: { hue: 200, label: "str" },
  number: { hue: 280, label: "num" },
  boolean: { hue: 25, label: "bool" },
  json: { hue: 200, label: "json" },
  binary: { hue: 70, label: "bin" },
  file: { hue: 152, label: "file" },
  stream: { hue: 25, label: "stream" },
  any: { hue: 0, label: "any" },
};

export function TypePill({ type }: { type: string }): JSX.Element {
  const def = TYPE_HUES[type] ?? { hue: 0, label: type };
  const isNeutral = def.hue === 0;
  return (
    <span
      style={{
        fontSize: 9.5,
        fontFamily: "var(--mono)",
        padding: "1px 6px",
        borderRadius: 2,
        color: isNeutral ? "var(--txt-faint)" : `oklch(0.78 0.16 ${def.hue})`,
        background: isNeutral
          ? "var(--bg-elevated)"
          : `oklch(0.78 0.16 ${def.hue} / 0.10)`,
        border: `1px solid ${
          isNeutral ? "var(--line)" : `oklch(0.78 0.16 ${def.hue} / 0.40)`
        }`,
        letterSpacing: "0.04em",
      }}
    >
      {def.label}
    </span>
  );
}
