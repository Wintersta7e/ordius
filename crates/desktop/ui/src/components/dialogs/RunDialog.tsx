// RunDialog — pre-flight modal asking for workflow variable values
// before kicking off `run_workflow`. Mirrors the design handoff's
// modal layout: dialog header with category accent, body with one
// row per variable, footer with Cancel + primary Run buttons.

import { useEffect, useState } from "react";
import type { JSX } from "react";

import type { Workspace } from "../../engine/types";
import { Field, SegRow, TextInput } from "../properties/primitives";
import { Ic } from "../icons";

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

  useEffect(() => {
    if (open) {
      setVars(variableDefaults);
      setWorkspaceId(defaultWorkspaceId ?? null);
      setAutoResume(defaultAutoResume);
    }
  }, [open, variableDefaults, defaultWorkspaceId, defaultAutoResume]);

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
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-label={`Run workflow ${workflowName}`}
        onClick={(event) => event.stopPropagation()}
        style={{
          width: 520,
          maxWidth: "92vw",
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
            padding: "16px 18px",
            borderBottom: "1px solid var(--line)",
            background: "var(--bg-elevated)",
            display: "flex",
            alignItems: "center",
            gap: 10,
          }}
        >
          <span style={{ color: "var(--accent)" }}>
            {Ic["play"]?.({ size: 14 })}
          </span>
          <div style={{ flex: 1, minWidth: 0 }}>
            <div
              style={{
                fontSize: 10,
                color: "var(--accent)",
                letterSpacing: "0.18em",
                textTransform: "uppercase",
              }}
            >
              run workflow
            </div>
            <div
              style={{
                fontFamily: "var(--display)",
                fontSize: 16,
                fontWeight: 600,
                color: "var(--txt)",
                marginTop: 2,
                overflow: "hidden",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
              }}
            >
              {workflowName}
            </div>
          </div>
          <button
            type="button"
            className="btn ghost icon"
            onClick={onCancel}
            title="Close"
          >
            {Ic["x"]?.({ size: 12 })}
          </button>
        </header>

        <div style={{ padding: "12px 6px 18px", maxHeight: "60vh", overflow: "auto" }}>
          {Object.keys(variableDefaults).length === 0 ? (
            <div
              style={{
                padding: "12px 18px",
                fontSize: 12,
                color: "var(--txt-faint)",
              }}
            >
              no variables to fill — press <span style={{ color: "var(--accent)" }}>run</span> below.
            </div>
          ) : (
            Object.entries(variableDefaults).map(([name, defaultValue]) => (
              <Field
                key={name}
                label={name}
                hint={defaultValue ? `default: ${defaultValue}` : "no default"}
              >
                <TextInput
                  value={vars[name] ?? defaultValue ?? ""}
                  onChange={(value) =>
                    setVars((current) => ({ ...current, [name]: value }))
                  }
                />
              </Field>
            ))
          )}

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
        </div>

        <footer
          style={{
            padding: "12px 18px",
            borderTop: "1px solid var(--line)",
            background: "var(--bg-elevated)",
            display: "flex",
            alignItems: "center",
            justifyContent: "flex-end",
            gap: 8,
          }}
        >
          <button type="button" className="btn ghost" onClick={onCancel}>
            cancel
          </button>
          <button
            type="button"
            className="btn primary"
            onClick={() =>
              onConfirm({ variables: vars, workspaceId, autoResume })
            }
            style={{ height: 28, padding: "0 14px" }}
          >
            {Ic["play"]?.({ size: 11 })} run
          </button>
        </footer>
      </div>
    </div>
  );
}
