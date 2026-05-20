// Right-side datasheet panel.
//
// Shows node properties when a node is selected: header with the
// type's category accent, pin tables for inputs/outputs, per-
// ConfigFieldDef parameter renderers, execution overrides, and a
// "remove node" button. Falls back to workflow-level metadata +
// variables + triggers when nothing is selected.

import type { JSX } from "react";

import type {
  ConfigFieldDef,
  Node,
  NodeType,
  PortDef,
  Workflow,
} from "../../engine/types";
import { CATEGORIES, catColor } from "../../data/categories";
import { NodeIcon, Ic } from "../icons";
import { BracketHeader } from "../palette/BracketHeader";
import {
  Field,
  KV,
  Mono,
  NumberInput,
  Section,
  SegRow,
  SpecPill,
  TextArea,
  TextInput,
  Toggle,
  TypePill,
} from "./primitives";

interface Props {
  workflow: Workflow;
  selectedNode: Node | null;
  nodeTypes: NodeType[];
  onPatchNode: (id: string, patch: Partial<Node>) => void;
  onPatchWorkflow: (patch: Partial<Workflow>) => void;
  onDeleteNode: (id: string) => void;
}

export function PropertiesPanel({
  workflow,
  selectedNode,
  nodeTypes,
  onPatchNode,
  onPatchWorkflow,
  onDeleteNode,
}: Props): JSX.Element {
  const nodeType =
    selectedNode != null
      ? nodeTypes.find((t) => t.id === selectedNode.type) ?? null
      : null;

  return (
    <div
      style={{
        height: "100%",
        display: "flex",
        flexDirection: "column",
        background: "var(--bg-panel)",
        borderLeft: "1px solid var(--line)",
        minHeight: 0,
      }}
    >
      <BracketHeader
        label={selectedNode ? `node · ${selectedNode.name || selectedNode.id}` : "workflow"}
        suffix={selectedNode ? "inst" : "meta"}
      />
      <div style={{ flex: 1, overflow: "auto" }}>
        {selectedNode && nodeType ? (
          <NodeProps
            node={selectedNode}
            nodeType={nodeType}
            onPatch={onPatchNode}
            onDelete={onDeleteNode}
          />
        ) : (
          <WorkflowProps workflow={workflow} onPatch={onPatchWorkflow} />
        )}
      </div>
    </div>
  );
}

// ─── Node properties ──────────────────────────────────────────────

interface NodePropsProps {
  node: Node;
  nodeType: NodeType;
  onPatch: (id: string, patch: Partial<Node>) => void;
  onDelete: (id: string) => void;
}

function NodeProps({
  node,
  nodeType,
  onPatch,
  onDelete,
}: NodePropsProps): JSX.Element {
  const cat = CATEGORIES[nodeType.category];
  const base = catColor(nodeType.category, "base");
  const tint = catColor(nodeType.category, "tint");
  const border = catColor(nodeType.category, "border");
  const configCount = Object.keys(node.config ?? {}).length;
  const definedFieldNames = new Set(nodeType.config.map((f) => f.name));
  const adhocEntries = Object.entries(node.config ?? {}).filter(
    ([key]) => !definedFieldNames.has(key),
  );

  return (
    <div>
      {/* Datasheet header */}
      <div
        style={{
          padding: "16px 16px 14px",
          background: `linear-gradient(180deg, ${tint} 0%, transparent 100%)`,
          borderBottom: `1px solid ${border}`,
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
            boxShadow: `0 0 8px ${catColor(nodeType.category, "glow")}`,
          }}
        />
        <div style={{ display: "flex", alignItems: "flex-start", gap: 14 }}>
          <div
            style={{
              width: 52,
              height: 52,
              flexShrink: 0,
              borderRadius: 3,
              background: tint,
              border: `1px solid ${border}`,
              display: "inline-flex",
              alignItems: "center",
              justifyContent: "center",
              color: base,
            }}
          >
            <NodeIcon
              category={nodeType.category}
              size={30}
              color={base}
              sw={1.7}
            />
          </div>
          <div style={{ flex: 1, minWidth: 0 }}>
            <div
              style={{
                fontSize: 9.5,
                color: base,
                fontWeight: 700,
                letterSpacing: "0.18em",
                textTransform: "uppercase",
              }}
            >
              {cat.label}
            </div>
            <div
              style={{
                fontSize: 15,
                color: "var(--txt)",
                fontWeight: 700,
                fontFamily: "var(--mono)",
                marginTop: 4,
                letterSpacing: "0.01em",
              }}
            >
              {nodeType.id}
            </div>
            {nodeType.description ? (
              <div
                style={{
                  fontSize: 11.5,
                  color: "var(--txt-dim)",
                  marginTop: 6,
                  lineHeight: 1.5,
                }}
              >
                {nodeType.description}
              </div>
            ) : null}
          </div>
        </div>

        <div
          style={{
            marginTop: 14,
            display: "flex",
            alignItems: "center",
            gap: 10,
            fontSize: 10,
            color: "var(--txt-soft)",
          }}
        >
          <SpecPill label="in" value={nodeType.inputs.length} />
          <SpecPill label="out" value={nodeType.outputs.length} />
          <SpecPill label="cfg" value={configCount} />
        </div>
      </div>

      {/* INSTANCE */}
      <Section label="instance">
        <Field label="display name">
          <TextInput
            value={node.name}
            onChange={(value) => onPatch(node.id, { name: value })}
          />
        </Field>
        <Field label="instance id">
          <Mono value={node.id} />
        </Field>
      </Section>

      {/* PINOUT */}
      <Section
        label="pinout"
        suffix={`${nodeType.inputs.length + nodeType.outputs.length} pins`}
      >
        <PinTable
          ports={nodeType.inputs}
          side="in"
          color={base}
          border={border}
        />
        <div style={{ height: 8 }} />
        <PinTable
          ports={nodeType.outputs}
          side="out"
          color={base}
          border={border}
        />
      </Section>

      {/* PARAMETERS */}
      <Section label="parameters" suffix={`${configCount} params`}>
        {nodeType.config.length === 0 && adhocEntries.length === 0 ? (
          <div
            style={{
              padding: "6px 16px 12px",
              color: "var(--txt-faint)",
              fontSize: 11,
            }}
          >
            no parameters
          </div>
        ) : null}
        {nodeType.config.map((field) => (
          <ConfigFieldRow
            key={field.name}
            field={field}
            value={node.config?.[field.name]}
            onChange={(value) =>
              onPatch(node.id, {
                config: { ...node.config, [field.name]: value },
              })
            }
          />
        ))}
        {adhocEntries.map(([key, value]) => (
          <Field key={key} label={key} hint="custom">
            <TextInput
              value={String(value ?? "")}
              onChange={(next) =>
                onPatch(node.id, {
                  config: { ...node.config, [key]: next },
                })
              }
            />
          </Field>
        ))}
      </Section>

      {/* EXECUTION */}
      <Section label="execution">
        <Field label="continue on error">
          <Toggle
            value={node.continueOnError}
            onChange={(value) =>
              onPatch(node.id, { continueOnError: value })
            }
          />
        </Field>
        <Field label="timeout (ms)" hint="0 → none">
          <NumberInput
            value={node.timeoutMs ?? 0}
            onChange={(value) =>
              onPatch(node.id, { timeoutMs: value > 0 ? value : null })
            }
          />
        </Field>
      </Section>

      {/* DANGER */}
      <div style={{ padding: "14px 16px 22px" }}>
        <button
          type="button"
          className="btn"
          onClick={() => onDelete(node.id)}
          style={{
            width: "100%",
            justifyContent: "center",
            height: 30,
            color: "var(--err)",
            borderColor: "var(--err)",
          }}
        >
          {Ic["x"]?.({ size: 11 })} remove node
        </button>
      </div>
    </div>
  );
}

// ─── Workflow properties ──────────────────────────────────────────

interface WorkflowPropsProps {
  workflow: Workflow;
  onPatch: (patch: Partial<Workflow>) => void;
}

function WorkflowProps({ workflow, onPatch }: WorkflowPropsProps): JSX.Element {
  const loopCount = workflow.edges.filter((e) => e.edgeType === "loop").length;
  return (
    <div>
      <div style={{ padding: 10 }}>
        <Field label="name">
          <TextInput
            value={workflow.name}
            onChange={(value) => onPatch({ name: value })}
          />
        </Field>
        <Field label="id">
          <Mono value={workflow.id} />
        </Field>
      </div>

      <Section label="variables">
        {Object.entries(workflow.variables).length === 0 ? (
          <div
            style={{
              padding: "6px 16px 12px",
              color: "var(--txt-faint)",
              fontSize: 11,
            }}
          >
            no variables
          </div>
        ) : null}
        {Object.entries(workflow.variables).map(([key, value]) => (
          <Field key={key} label={key}>
            <TextInput
              value={value}
              onChange={(next) =>
                onPatch({
                  variables: { ...workflow.variables, [key]: next },
                })
              }
            />
          </Field>
        ))}
      </Section>

      <Section label="triggers">
        <div style={{ padding: "0 16px 10px" }}>
          {workflow.triggers.length === 0 ? (
            <div style={{ color: "var(--txt-faint)", fontSize: 11 }}>
              manual-only (no triggers declared)
            </div>
          ) : (
            workflow.triggers.map((trigger, idx) => (
              <div
                key={`${trigger.type}-${idx}`}
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: 8,
                  padding: "6px 0",
                  borderBottom: "1px solid var(--line-soft)",
                  fontFamily: "var(--mono)",
                  fontSize: 11.5,
                }}
              >
                <span style={{ color: "var(--accent)" }}>·</span>
                <span style={{ color: "var(--txt)", minWidth: 86 }}>
                  {trigger.type}
                </span>
                <span style={{ color: "var(--txt-faint)", fontSize: 10 }}>
                  {summariseTrigger(trigger)}
                </span>
              </div>
            ))
          )}
        </div>
      </Section>

      <Section label="stats">
        <KV k="nodes" v={workflow.nodes.length} />
        <KV k="edges" v={workflow.edges.length} />
        <KV k="loops" v={loopCount} />
        <KV k="schema" v={`v${workflow.schemaVersion}`} />
      </Section>
    </div>
  );
}

function summariseTrigger(trigger: Workflow["triggers"][number]): string {
  switch (trigger.type) {
    case "manual":
      return "run from button or ⌘R";
    case "schedule":
      return `cron: ${trigger.cron}`;
    case "file-watch":
      return `paths: ${trigger.paths.join(", ")}`;
    case "webhook":
      return trigger.secretToken ? "secret-gated" : "open endpoint";
    default:
      return "";
  }
}

// ─── Per-field rendering ──────────────────────────────────────────

interface ConfigFieldRowProps {
  field: ConfigFieldDef;
  value: unknown;
  onChange: (next: unknown) => void;
}

function ConfigFieldRow({
  field,
  value,
  onChange,
}: ConfigFieldRowProps): JSX.Element {
  const hint = field.required ? "required" : undefined;
  const currentString = stringifyValue(value, field);

  switch (field.type) {
    case "string":
      return (
        <Field label={field.label} hint={hint}>
          <TextInput value={currentString} onChange={onChange} />
        </Field>
      );
    case "textarea":
      return (
        <Field label={field.label} hint={hint}>
          <TextArea value={currentString} onChange={onChange} />
        </Field>
      );
    case "number":
      return (
        <Field label={field.label} hint={hint}>
          <NumberInput
            value={typeof value === "number" ? value : Number(currentString) || 0}
            onChange={(next) => onChange(next)}
          />
        </Field>
      );
    case "boolean":
      return (
        <Field label={field.label} hint={hint}>
          <Toggle value={Boolean(value)} onChange={(next) => onChange(next)} />
        </Field>
      );
    case "select": {
      const opts = extractSelectOptions(field.default);
      return (
        <Field label={field.label} hint={hint}>
          {opts.length > 0 ? (
            <SegRow
              value={currentString}
              options={opts}
              onChange={(next) => onChange(next)}
            />
          ) : (
            <TextInput value={currentString} onChange={onChange} />
          )}
        </Field>
      );
    }
    case "file":
      return (
        <Field label={field.label} hint={hint ?? "file path"}>
          <TextInput value={currentString} onChange={onChange} />
        </Field>
      );
    case "secret":
      return (
        <Field label={field.label} hint={hint ?? "secret name"}>
          <TextInput
            value={currentString}
            onChange={onChange}
            placeholder="{{secrets.NAME}}"
          />
        </Field>
      );
  }
}

function stringifyValue(value: unknown, field: ConfigFieldDef): string {
  if (value == null) {
    if (field.default == null) return "";
    if (typeof field.default === "string") return field.default;
    if (typeof field.default === "number" || typeof field.default === "boolean") {
      return String(field.default);
    }
    return "";
  }
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return JSON.stringify(value);
}

function extractSelectOptions(defaultValue: unknown): string[] {
  if (Array.isArray(defaultValue)) {
    return defaultValue.filter((v): v is string => typeof v === "string");
  }
  return [];
}

// ─── Pin table ────────────────────────────────────────────────────

interface PinTableProps {
  ports: PortDef[];
  side: "in" | "out";
  color: string;
  border: string;
}

function PinTable({ ports, side, color, border }: PinTableProps): JSX.Element {
  const headerLabel = side === "in" ? "INPUTS" : "OUTPUTS";
  return (
    <div style={{ padding: "0 16px 8px" }}>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "28px 1fr auto",
          gap: 8,
          padding: "6px 8px",
          fontSize: 9.5,
          color: "var(--txt-faint)",
          letterSpacing: "0.14em",
          textTransform: "uppercase",
          background: "var(--bg-canvas)",
          borderTop: `1px solid ${border}`,
          borderBottom: "1px solid var(--line-soft)",
        }}
      >
        <span style={{ color, fontWeight: 600 }}>
          {side === "in" ? "◀" : "▶"}
        </span>
        <span>{headerLabel}</span>
        <span>type</span>
      </div>
      {ports.length === 0 ? (
        <div
          style={{
            padding: "8px 8px",
            color: "var(--txt-faint)",
            fontSize: 11,
            fontStyle: "italic",
          }}
        >
          — none —
        </div>
      ) : null}
      {ports.map((port, idx) => (
        <div
          key={port.name}
          style={{
            display: "grid",
            gridTemplateColumns: "28px 1fr auto",
            gap: 8,
            padding: "8px 8px",
            fontFamily: "var(--mono)",
            fontSize: 11.5,
            borderBottom:
              idx < ports.length - 1 ? "1px solid var(--line-soft)" : "none",
            alignItems: "center",
          }}
        >
          <span
            className="num"
            style={{
              color: "var(--txt-faint)",
              fontSize: 10,
            }}
          >
            {String(idx + 1).padStart(2, "0")}
          </span>
          <span
            style={{
              display: "flex",
              alignItems: "center",
              gap: 8,
              color: "var(--txt)",
            }}
          >
            <span
              style={{
                width: 10,
                height: 2,
                background: color,
                boxShadow: `0 0 4px ${color}`,
              }}
            />
            {port.name}
            {port.required ? (
              <span
                style={{
                  color: "var(--accent)",
                  fontSize: 9,
                  marginLeft: 4,
                }}
              >
                *
              </span>
            ) : null}
          </span>
          <TypePill type={port.type} />
        </div>
      ))}
    </div>
  );
}
