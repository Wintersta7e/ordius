// Shared <select> over the env snapshot used by the workflow editor.
//
// Renders as a styled native select so keyboard nav + accessibility
// come free. Drives two callers:
//   - workflow header default-env field (no inheritLabel)
//   - per-node target_env field (inheritLabel = "(workflow default: X)")
// Selecting the inherit sentinel calls back with `null` so callers
// can clear the field rather than choosing an explicit env id.

import type { JSX } from "react";

import type { EnvEntryIpc, EnvSnapshotIpc } from "../../engine/types";

export interface EnvPickerProps {
  /** Snapshot to enumerate. `null` while still loading. */
  envs: EnvSnapshotIpc | null;
  /** Currently selected env id, or `null` to mean inherit/default. */
  value: string | null;
  onChange: (next: string | null) => void;
  /** When set, prepend an inherit sentinel as the first option. The
   * label is shown verbatim (e.g. "(workflow default: wsl:Ubuntu)"). */
  inheritLabel?: string;
  /** Include disabled envs in the list. Default false. */
  includeDisabled?: boolean;
}

export function EnvPicker({
  envs,
  value,
  onChange,
  inheritLabel,
  includeDisabled = false,
}: EnvPickerProps): JSX.Element {
  const options =
    envs?.envs.filter((env) => includeDisabled || env.enabled) ?? [];
  return (
    <select
      value={value ?? "__inherit__"}
      onChange={(event) =>
        onChange(
          event.target.value === "__inherit__" ? null : event.target.value,
        )
      }
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
      {inheritLabel ? (
        <option value="__inherit__">{inheritLabel}</option>
      ) : null}
      {options.map((env) => (
        <option key={env.id} value={env.id}>
          {labelFor(env)}
        </option>
      ))}
      {options.length === 0 && !inheritLabel ? (
        <option value="">no envs available</option>
      ) : null}
    </select>
  );
}

function labelFor(env: EnvEntryIpc): string {
  const stateChip =
    env.state.state === "reachable"
      ? "●"
      : env.state.state === "probing"
        ? "◌"
        : env.state.state === "unreachable"
          ? "✕"
          : "○";
  return `${stateChip} ${env.label}`;
}
