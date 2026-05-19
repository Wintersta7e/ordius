// RunDialog — pre-flight modal asking for workflow variable values
// before kicking off `run_workflow`. Mirrors the design handoff's
// modal layout: bracket-style header, variable rows with $ prefix,
// workspace picker, auto-resume toggle, equivalent CLI command at
// the bottom (with copy), and ⏎ / esc keyboard hints.

import { useEffect, useMemo, useState } from "react";
import type { JSX, KeyboardEvent as ReactKeyboardEvent } from "react";

import type { Workspace } from "../../engine/types";
import { Field, SegRow, TextInput } from "../properties/primitives";

interface Props {
  open: boolean;
  workflowName: string;
  /** workflow.variables — name → default value. */
  variableDefaults: Record<string, string>;
  workspaces: Workspace[];
  defaultWorkspaceId?: string | null;
  autoResume: boolean;
  onConfirm: (input: {
    variables: Record<string, string>;
    workspaceId: string | null;
    autoResume: boolean;
  }) => void;
  onCancel: () => void;
}

export function RunDialog({
  open,
  workflowName,
  variableDefaults,
  workspaces,
  defaultWorkspaceId,
  autoResume: defaultAutoResume,
  onConfirm,
  onCancel,
}: Props): JSX.Element | null {
  const [vars, setVars] = useState<Record<string, string>>(variableDefaults);
  const [workspaceId, setWorkspaceId] = useState<string | null>(
    defaultWorkspaceId ?? null,
  );
  const [autoResume, setAutoResume] = useState(defaultAutoResume);
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    if (open) {
      setVars(variableDefaults);
      setWorkspaceId(defaultWorkspaceId ?? null);
      setAutoResume(defaultAutoResume);
      setCopied(false);
    }
  }, [open, variableDefaults, defaultWorkspaceId, defaultAutoResume]);

  const cliCommand = useMemo(() => {
    const parts = ["$", "ordius", "run", workflowName];
    if (workspaceId) parts.push("--workspace", workspaceId);
    for (const [name, value] of Object.entries(vars)) {
      if (!value) continue;
      const safe = /^[A-Za-z0-9_./:-]+$/.test(value) ? value : `"${value}"`;
      parts.push("--var", `${name}=${safe}`);
    }
    if (autoResume) parts.push("--auto-resume");
    return parts.join(" ");
  }, [workflowName, workspaceId, vars, autoResume]);

  const handleConfirm = () => {
    onConfirm({ variables: vars, workspaceId, autoResume });
  };

  const handleKey = (event: ReactKeyboardEvent<HTMLDivElement>) => {
    if (event.key === "Escape") {
      event.preventDefault();
      onCancel();
    } else if (
      event.key === "Enter" &&
      (event.ctrlKey || event.metaKey || event.target === event.currentTarget)
    ) {
      event.preventDefault();
      handleConfirm();
    }
  };

  const handleCopy = () => {
    void navigator.clipboard?.writeText(cliCommand).then(() => {
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1200);
    });
  };

  if (!open) return null;

  return (
    <div
      style={{
        position: "fixed",
        inset: 0,
        background: "oklch(0 0 0 / 0.55)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        zIndex: 100,
        backdropFilter: "blur(4px)",
      }}
      onClick={onCancel}
      onKeyDown={handleKey}
      tabIndex={-1}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-label={`Run workflow ${workflowName}`}
        onClick={(event) => event.stopPropagation()}
        style={{
          width: 560,
          maxWidth: "94vw",
          background: "var(--bg-panel)",
          border: "1px solid var(--line-strong)",
          borderRadius: 4,
          boxShadow: "0 30px 80px -20px rgba(0,0,0,0.7)",
          display: "flex",
          flexDirection: "column",
          fontFamily: "var(--mono)",
          color: "var(--txt)",
        }}
      >
        <header
          style={{
            padding: "12px 16px",
            borderBottom: "1px solid var(--line)",
            background: "var(--bg-elevated)",
            display: "flex",
            alignItems: "center",
            gap: 8,
            fontSize: 10,
            letterSpacing: "0.18em",
            textTransform: "uppercase",
            color: "var(--accent)",
          }}
        >
          <span>┌</span>
          <span>run</span>
          <span style={{ color: "var(--txt-faint)" }}>·</span>
          <span style={{ color: "var(--txt)" }}>{workflowName}</span>
          <span style={{ marginLeft: "auto", color: "var(--txt-faint)" }}>
            <button
              type="button"
              className="btn ghost icon"
              onClick={onCancel}
              title="Close"
              style={{ height: 18, width: 18, fontSize: 11, padding: 0 }}
            >
              ×
            </button>
          </span>
          <span>┐</span>
        </header>

        <div
          style={{
            padding: "14px 18px 18px",
            maxHeight: "60vh",
            overflow: "auto",
          }}
        >
          {Object.keys(variableDefaults).length === 0 ? (
            <div
              style={{
                padding: "8px 0 12px",
                fontSize: 12,
                color: "var(--txt-faint)",
              }}
            >
              no variables to fill — press{" "}
              <span style={{ color: "var(--accent)" }}>run</span> below.
            </div>
          ) : (
            <div
              style={{
                display: "flex",
                alignItems: "center",
                gap: 6,
                margin: "4px 0 10px",
                fontSize: 10,
                color: "var(--accent)",
                letterSpacing: "0.18em",
                textTransform: "uppercase",
              }}
            >
              <span>├</span>
              <span>variables</span>
              <span
                style={{
                  marginLeft: "auto",
                  color: "var(--txt-faint)",
                }}
              >
                {Object.keys(variableDefaults).length} declared
              </span>
            </div>
          )}

          {Object.entries(variableDefaults).map(([name, defaultValue]) => (
            <Field
              key={name}
              label={
                <span>
                  <span style={{ color: "var(--accent)" }}>$</span> {name}
                </span>
              }
              hint={defaultValue ? `default: ${defaultValue}` : "no default"}
            >
              <TextInput
                value={vars[name] ?? defaultValue ?? ""}
                onChange={(value) =>
                  setVars((current) => ({ ...current, [name]: value }))
                }
              />
            </Field>
          ))}

          {workspaces.length > 0 ? (
            <Field label="workspace">
              <SegRow
                value={workspaceId ?? "default"}
                options={[
                  { id: "default", label: "(home)" },
                  ...workspaces.map((w) => ({ id: w.id, label: w.name })),
                ]}
                onChange={(value) =>
                  setWorkspaceId(value === "default" ? null : value)
                }
              />
            </Field>
          ) : null}

          <Field
            label="auto-resume checkpoints"
            hint="answer 'yes' to every pause node"
          >
            <SegRow
              value={autoResume ? "yes" : "no"}
              options={["no", "yes"]}
              onChange={(value) => setAutoResume(value === "yes")}
            />
          </Field>

          <div
            style={{
              marginTop: 16,
              border: "1px solid var(--line)",
              borderRadius: 3,
              background: "var(--bg-input)",
              display: "flex",
              alignItems: "center",
              gap: 8,
              padding: "8px 10px",
              fontSize: 11,
            }}
          >
            <span
              style={{
                color: "var(--txt-faint)",
                whiteSpace: "nowrap",
                fontSize: 10,
                letterSpacing: "0.12em",
                textTransform: "uppercase",
              }}
            >
              equivalent
            </span>
            <code
              style={{
                flex: 1,
                color: "var(--txt-dim)",
                whiteSpace: "nowrap",
                overflow: "hidden",
                textOverflow: "ellipsis",
                fontSize: 11,
              }}
              title={cliCommand}
            >
              {cliCommand}
            </code>
            <button
              type="button"
              className="btn ghost"
              onClick={handleCopy}
              style={{ height: 22, padding: "0 10px", fontSize: 10 }}
            >
              {copied ? "copied" : "copy"}
            </button>
          </div>
        </div>

        <footer
          style={{
            padding: "10px 18px",
            borderTop: "1px solid var(--line)",
            background: "var(--bg-elevated)",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            gap: 8,
            fontSize: 10,
            color: "var(--txt-faint)",
            letterSpacing: "0.08em",
            textTransform: "uppercase",
          }}
        >
          <span>
            <kbd style={kbdStyle}>esc</kbd> close
            <span style={{ margin: "0 8px", color: "var(--line)" }}>·</span>
            <kbd style={kbdStyle}>⌘ ⏎</kbd> run
          </span>
          <span style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <button type="button" className="btn ghost" onClick={onCancel}>
              cancel
            </button>
            <button
              type="button"
              className="btn primary"
              onClick={handleConfirm}
              style={{ height: 28, padding: "0 16px" }}
            >
              ▶ run now
            </button>
          </span>
        </footer>
      </div>
    </div>
  );
}

const kbdStyle = {
  display: "inline-flex",
  alignItems: "center",
  height: 16,
  padding: "0 5px",
  background: "var(--bg-input)",
  border: "1px solid var(--line)",
  borderRadius: 2,
  fontSize: 9.5,
  color: "var(--txt-dim)",
  marginRight: 4,
  fontFamily: "var(--mono)",
} as const;
