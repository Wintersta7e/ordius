// Grouped <select> over the resource definitions visible to
// (envId, workflowId?). Drives the workflow editor's llm + http
// node config sections.
//
// The list comes from `listEnvironmentDefinitions`, which already
// joins each definition with its current probe outcome + the
// capabilities the latest probe proved. Options that don't prove
// the caller-supplied `capabilityFilter` render disabled so the
// user can see what's available but can't select something the
// llm/http node would later fail to dispatch through.

import { useEffect, useState } from "react";
import type { JSX } from "react";

import { listEnvironmentDefinitions } from "../../engine";
import type {
  EnvDefinitionIpc,
  EnvDefinitionListIpc,
} from "../../engine/types";

export interface ResourcePickerProps {
  /** Env the resource will be resolved against. */
  envId: string;
  /** Optional workflow id for workflow-scope overrides. */
  workflowId?: string | undefined;
  /** Currently selected resource id, or `null` for unset. */
  value: string | null;
  onChange: (next: string | null) => void;
  /** When set, options whose latest probe didn't prove this
   * capability render disabled. Snake-case, matches the engine's
   * `Capability` serde (e.g. `openai_chat_completions`). */
  capabilityFilter?: string;
  /** Placeholder shown when value is null. */
  placeholder?: string;
}

export function ResourcePicker({
  envId,
  workflowId,
  value,
  onChange,
  capabilityFilter,
  placeholder = "(no resource)",
}: ResourcePickerProps): JSX.Element {
  const [list, setList] = useState<EnvDefinitionListIpc | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setList(null);
    setErr(null);
    listEnvironmentDefinitions(envId, workflowId)
      .then((next) => {
        if (!cancelled) setList(next);
      })
      .catch((e: unknown) => {
        if (!cancelled) setErr(String(e));
      });
    return () => {
      cancelled = true;
    };
  }, [envId, workflowId]);

  if (err) {
    return (
      <div
        style={{
          color: "var(--err)",
          fontSize: 11,
          fontFamily: "var(--mono)",
        }}
      >
        {err}
      </div>
    );
  }
  if (!list) {
    return (
      <div
        style={{
          color: "var(--txt-faint)",
          fontSize: 11,
          fontFamily: "var(--mono)",
        }}
      >
        loading…
      </div>
    );
  }

  const groups = groupByKind(list.definitions);
  const selected = list.definitions.find((def) => def.id === value) ?? null;

  return (
    <div>
      <select
        value={value ?? ""}
        onChange={(event) => onChange(event.target.value || null)}
        style={{
          width: "100%",
          background: "var(--bg-input)",
          border: "1px solid var(--line)",
          color: "var(--txt)",
          fontFamily: "var(--mono)",
          fontSize: 12,
          padding: "7px 10px",
          borderRadius: 3,
          outline: "none",
        }}
      >
        <option value="">{placeholder}</option>
        {groups.map((group) => (
          <optgroup key={group.kind} label={group.label}>
            {group.items.map((def) => {
              const disabled =
                capabilityFilter !== undefined &&
                !def.provenCapabilities.includes(capabilityFilter);
              return (
                <option key={def.id} value={def.id} disabled={disabled}>
                  {chipFor(def.outcome.outcome)} {def.id}
                  {def.scope !== "builtin" ? ` · ${def.scope}` : ""}
                </option>
              );
            })}
          </optgroup>
        ))}
      </select>
      {selected ? (
        <div
          style={{
            marginTop: 4,
            fontSize: 10,
            color: "var(--txt-faint)",
            fontFamily: "var(--mono)",
          }}
        >
          {summarise(selected)}
        </div>
      ) : null}
    </div>
  );
}

interface KindGroup {
  kind: string;
  label: string;
  items: EnvDefinitionIpc[];
}

function groupByKind(defs: EnvDefinitionIpc[]): KindGroup[] {
  const groups = new Map<string, KindGroup>();
  for (const def of defs) {
    let group = groups.get(def.kind);
    if (!group) {
      group = { kind: def.kind, label: labelForKind(def.kind), items: [] };
      groups.set(def.kind, group);
    }
    group.items.push(def);
  }
  return Array.from(groups.values());
}

function labelForKind(kind: string): string {
  switch (kind) {
    case "http_endpoint":
      return "LLM / HTTP endpoints";
    case "binary":
      return "CLI agents + binaries";
    case "toolchain":
      return "Toolchains";
    default:
      return kind;
  }
}

function chipFor(outcome: string): string {
  switch (outcome) {
    case "found":
      return "●";
    case "not_found":
      return "○";
    case "skipped":
      return "—";
    case "timed_out":
      return "⌛";
    case "probe_failed":
      return "✕";
    default:
      return "?";
  }
}

function summarise(def: EnvDefinitionIpc): string {
  if (def.outcome.outcome === "found") {
    const caps = def.provenCapabilities.join(", ");
    const url = def.baseUrl ? ` · ${def.baseUrl}` : "";
    return `proven: ${caps || "(none)"}${url}`;
  }
  if (
    def.outcome.outcome === "skipped" ||
    def.outcome.outcome === "probe_failed"
  ) {
    return `${def.outcome.outcome.replace(/_/g, " ")}: ${def.outcome.reason}`;
  }
  return def.outcome.outcome.replace(/_/g, " ");
}
