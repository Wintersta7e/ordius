// Three left-rail cards: Now running · Workspace · System.
//
// Cards are deliberately stretched: the rail itself is sticky so it
// doesn't scroll with the workflow grid (only the grid scrolls).

import type { JSX, ReactNode } from "react";

import type { SystemStatus, Workspace } from "../../engine/types";
import { fmtBytes, fmtDuration } from "../../lib/format";

export interface RunningWorkflow {
  id: string;
  name: string;
  /** Run id of the active run for this workflow. */
  runId: string;
  /** Started at (epoch ms) — used to render elapsed time. */
  startedAt: number;
}

interface Props {
  running: RunningWorkflow[];
  workspace: Workspace | null;
  status: SystemStatus | null;
  now: number;
}

export function LeftRail({
  running,
  workspace,
  status,
  now,
}: Props): JSX.Element {
  return (
    <aside
      style={{
        display: "flex",
        flexDirection: "column",
        gap: 14,
        minHeight: 0,
      }}
    >
      <RailCard
        label="now running"
        suffix={running.length === 0 ? "idle" : `${running.length} active`}
      >
        {running.length === 0 ? (
          <div
            style={{
              padding: "14px 14px",
              color: "var(--txt-faint)",
              fontSize: 11,
              lineHeight: 1.5,
            }}
          >
            <span style={{ color: "var(--txt-soft)" }}>·</span> nothing running.
            trigger a workflow from the right.
          </div>
        ) : (
          <div>
            {running.map((w) => (
              <RunningRow key={w.id} workflow={w} now={now} />
            ))}
          </div>
        )}
      </RailCard>

      <RailCard label="workspace">
        <div style={{ padding: "12px 14px 14px" }}>
          {workspace ? (
            <>
              <div
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: 8,
                  marginBottom: 6,
                }}
              >
                <span
                  style={{
                    width: 22,
                    height: 22,
                    borderRadius: 3,
                    display: "inline-flex",
                    alignItems: "center",
                    justifyContent: "center",
                    background: "var(--accent-soft)",
                    border: "1px solid var(--accent)",
                    color: "var(--accent)",
                    fontSize: 13,
                    flexShrink: 0,
                  }}
                >
                  ▸
                </span>
                <span
                  style={{
                    fontFamily: "var(--mono)",
                    fontSize: 12.5,
                    fontWeight: 600,
                    color: "var(--txt)",
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    whiteSpace: "nowrap",
                    minWidth: 0,
                    flex: 1,
                  }}
                >
                  {workspace.name}
                </span>
              </div>
              <div
                style={{
                  fontSize: 10.5,
                  color: "var(--txt-faint)",
                  fontFamily: "var(--mono)",
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                }}
                title={workspace.path}
              >
                {workspace.path}
              </div>
            </>
          ) : (
            <div
              style={{
                fontSize: 11,
                color: "var(--txt-faint)",
                lineHeight: 1.5,
              }}
            >
              No workspace registered yet. Add one from Settings →
              Workspaces to bind a project directory.
            </div>
          )}
        </div>
      </RailCard>

      <RailCard label="system">
        <div style={{ padding: "4px 0" }}>
          {status ? (
            <>
              <SysRow
                label="engine"
                detail={`v${status.engineVersion}`}
                state="ok"
              />
              {status.endpoints.length === 0 ? (
                <SysRow
                  label="endpoints"
                  detail="none registered yet"
                  state="unknown"
                />
              ) : (
                status.endpoints.map((endpoint) => (
                  <SysRow
                    key={endpoint.id}
                    label={endpoint.name}
                    detail={endpoint.state}
                    state={endpoint.state}
                  />
                ))
              )}
              <SysRow
                label="runs db"
                detail={fmtBytes(status.runsDbBytes)}
                state="ok"
              />
              <SysRow
                label="workspaces"
                detail={fmtBytes(status.workspacesBytes)}
                state="ok"
                last
              />
            </>
          ) : (
            <div
              style={{
                padding: "14px",
                color: "var(--txt-faint)",
                fontSize: 11,
              }}
            >
              loading…
            </div>
          )}
        </div>
      </RailCard>

      <div
        style={{
          padding: "12px 14px",
          background: "var(--bg-canvas)",
          border: "1px dashed var(--line)",
          borderRadius: 3,
          fontFamily: "var(--mono)",
          fontSize: 10.5,
          color: "var(--txt-faint)",
          lineHeight: 1.6,
        }}
      >
        <div style={{ color: "var(--txt-soft)", marginBottom: 4 }}>
          $ ordius run &lt;id&gt;
        </div>
        <div>any workflow on this page runs from the cli too.</div>
      </div>
    </aside>
  );
}

function RailCard({
  label,
  suffix,
  children,
}: {
  label: string;
  suffix?: string;
  children: ReactNode;
}): JSX.Element {
  return (
    <div
      style={{
        background: "var(--bg-panel)",
        border: "1px solid var(--line)",
        borderRadius: 3,
        overflow: "hidden",
      }}
    >
      <div
        style={{
          padding: "10px 14px",
          borderBottom: "1px solid var(--line-soft)",
          display: "flex",
          alignItems: "center",
          gap: 8,
          background: "var(--bg-elevated)",
        }}
      >
        <span style={{ color: "var(--accent)", fontSize: 12 }}>┌</span>
        <span
          style={{
            fontFamily: "var(--mono)",
            fontSize: 10,
            fontWeight: 700,
            color: "var(--txt)",
            letterSpacing: "0.18em",
            textTransform: "uppercase",
          }}
        >
          {label}
        </span>
        <div style={{ flex: 1 }} />
        {suffix ? (
          <span
            className="num"
            style={{
              fontFamily: "var(--mono)",
              fontSize: 9.5,
              color: "var(--txt-faint)",
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

function RunningRow({
  workflow,
  now,
}: {
  workflow: RunningWorkflow;
  now: number;
}): JSX.Element {
  const elapsed = Math.max(0, now - workflow.startedAt);
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 8,
        padding: "10px 14px",
        borderBottom: "1px solid var(--line-soft)",
        fontFamily: "var(--mono)",
      }}
    >
      <span
        style={{
          width: 8,
          height: 8,
          borderRadius: 8,
          background: "var(--info)",
          boxShadow: "0 0 8px var(--info)",
          animation: "pulse 1.1s ease-in-out infinite",
          flexShrink: 0,
        }}
      />
      <div style={{ flex: 1, minWidth: 0 }}>
        <div
          style={{
            fontSize: 11.5,
            color: "var(--txt)",
            fontWeight: 500,
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
          }}
        >
          {workflow.name}
        </div>
        <div
          style={{
            fontSize: 9.5,
            color: "var(--txt-faint)",
          }}
        >
          <span className="num">running · {fmtDuration(elapsed)}</span>
        </div>
      </div>
    </div>
  );
}

function SysRow({
  label,
  detail,
  state,
  last,
}: {
  label: string;
  detail: string;
  state: "ok" | "down" | "unknown";
  last?: boolean;
}): JSX.Element {
  const colorMap: Record<string, string> = {
    ok: "var(--ok)",
    down: "var(--err)",
    unknown: "var(--txt-faint)",
  };
  const color = colorMap[state] ?? "var(--line)";
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 8,
        padding: "7px 14px",
        borderBottom: last ? "none" : "1px solid var(--line-soft)",
        fontFamily: "var(--mono)",
      }}
    >
      <span
        style={{
          width: 6,
          height: 6,
          borderRadius: 6,
          background: color,
          boxShadow: state === "ok" ? `0 0 4px ${color}` : "none",
          flexShrink: 0,
        }}
      />
      <span style={{ fontSize: 11, color: "var(--txt)", minWidth: 84 }}>
        {label}
      </span>
      <span
        style={{
          fontSize: 10,
          color: "var(--txt-faint)",
          flex: 1,
          minWidth: 0,
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}
      >
        {detail}
      </span>
    </div>
  );
}
