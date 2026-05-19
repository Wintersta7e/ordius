// History route — every recorded run, grouped by day, with
// per-row drill-down into node_runs + error tail.

import { useCallback, useEffect, useMemo, useState } from "react";
import type { JSX } from "react";

import {
  type RunDetail,
  type RunRow,
  getRun,
  listRuns,
} from "../engine";
import { TopBar } from "../components/chrome/TopBar";
import { SectionTitle } from "../components/SectionTitle";
import { StatusRibbon } from "../components/home/StatusRibbon";
import { fmtDuration } from "../lib/format";
import type { Route } from "../lib/router";

interface Props {
  theme: "dark" | "light";
  onThemeToggle: () => void;
  onNavigate: (route: Route) => void;
}

type StatusFilter = "all" | "done" | "error" | "stopped" | "running";

const STATUS_COLOR: Record<string, string> = {
  done: "var(--ok)",
  error: "var(--err)",
  running: "var(--info)",
  stopped: "var(--warn)",
};

export function History({ theme, onThemeToggle, onNavigate }: Props): JSX.Element {
  const [runs, setRuns] = useState<RunRow[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("all");
  const [expanded, setExpanded] = useState<string | null>(null);
  const [details, setDetails] = useState<Record<string, RunDetail>>({});

  const insideTauri =
    typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;

  useEffect(() => {
    if (!insideTauri) {
      setError(
        "running in browser preview · engine commands disabled — launch via `tauri dev` to see real runs",
      );
      return;
    }
    let cancelled = false;
    void (async () => {
      try {
        const rows = await listRuns({ limit: 200 });
        if (!cancelled) setRuns(rows);
      } catch (e: unknown) {
        if (!cancelled) setError(String(e));
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [insideTauri]);

  const stats = useMemo(() => {
    const total = runs.length;
    const done = runs.filter((r) => r.status === "done").length;
    const failed = runs.filter((r) => r.status === "error").length;
    const stopped = runs.filter((r) => r.status === "stopped").length;
    return { total, done, failed, stopped };
  }, [runs]);

  const filtered = useMemo(() => {
    if (statusFilter === "all") return runs;
    return runs.filter((r) => r.status === statusFilter);
  }, [runs, statusFilter]);

  const byDay = useMemo(() => {
    const groups = new Map<string, RunRow[]>();
    for (const run of filtered) {
      const day = new Date(run.startedAt).toISOString().slice(0, 10);
      const existing = groups.get(day) ?? [];
      existing.push(run);
      groups.set(day, existing);
    }
    return Array.from(groups.entries()).sort((a, b) =>
      b[0].localeCompare(a[0]),
    );
  }, [filtered]);

  const handleExpand = useCallback(
    async (runId: string) => {
      if (expanded === runId) {
        setExpanded(null);
        return;
      }
      setExpanded(runId);
      if (details[runId] || !insideTauri) return;
      try {
        const detail = await getRun(runId);
        setDetails((current) => ({ ...current, [runId]: detail }));
      } catch (e: unknown) {
        setError(String(e));
      }
    },
    [expanded, details, insideTauri],
  );

  return (
    <div
      style={{
        display: "grid",
        gridTemplateRows: "44px 1fr 22px",
        height: "100vh",
        minHeight: 720,
        background: "var(--bg)",
      }}
    >
      <TopBar pageLabel="history" theme={theme} onThemeToggle={onThemeToggle} />

      <main
        style={{
          overflow: "auto",
          padding: "24px 36px 32px",
        }}
      >
        <div style={{ maxWidth: 1280, margin: "0 auto" }}>
          <h1
            style={{
              fontFamily: "var(--display)",
              fontWeight: 600,
              fontSize: 28,
              margin: 0,
              color: "var(--txt)",
              letterSpacing: "-0.01em",
            }}
          >
            run history
          </h1>

          {error ? (
            <div
              style={{
                margin: "16px 0",
                padding: "8px 12px",
                fontFamily: "var(--mono)",
                fontSize: 11,
                color: "var(--warn)",
                background: "var(--bg-canvas)",
                border: "1px dashed var(--line)",
                borderRadius: 3,
              }}
            >
              <span style={{ color: "var(--warn)" }}>! </span>
              {error}
            </div>
          ) : null}

          {/* Stats strip */}
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(4, 1fr)",
              gap: 14,
              marginTop: 16,
              marginBottom: 22,
            }}
          >
            <StatCell label="total" value={stats.total} />
            <StatCell label="done" value={stats.done} color="var(--ok)" />
            <StatCell label="errors" value={stats.failed} color="var(--err)" />
            <StatCell label="stopped" value={stats.stopped} color="var(--warn)" />
          </div>

          {/* Filter bar */}
          <SectionTitle
            label="runs"
            count={`${filtered.length} shown`}
            right={
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
                {(["all", "done", "error", "stopped", "running"] as StatusFilter[]).map(
                  (s) => (
                    <button
                      key={s}
                      type="button"
                      onClick={() => setStatusFilter(s)}
                      style={{
                        appearance: "none",
                        border: 0,
                        background:
                          statusFilter === s
                            ? "var(--bg-active)"
                            : "transparent",
                        color:
                          statusFilter === s ? "var(--txt)" : "var(--txt-dim)",
                        fontFamily: "var(--mono)",
                        fontSize: 11,
                        padding: "3px 10px",
                        height: 20,
                        borderRadius: 2,
                        cursor: "pointer",
                      }}
                    >
                      {s}
                    </button>
                  ),
                )}
              </div>
            }
          />

          {/* Day-grouped table */}
          <div
            style={{
              marginTop: 14,
              background: "var(--bg-panel)",
              border: "1px solid var(--line)",
              borderRadius: 3,
              overflow: "hidden",
            }}
          >
            {byDay.length === 0 ? (
              <div
                style={{
                  padding: "30px 16px",
                  fontFamily: "var(--mono)",
                  fontSize: 12,
                  color: "var(--txt-faint)",
                  textAlign: "center",
                }}
              >
                no runs to show.
              </div>
            ) : (
              byDay.map(([day, dayRuns]) => (
                <div key={day}>
                  <div
                    style={{
                      padding: "8px 16px",
                      background: "var(--bg-elevated)",
                      borderTop: "1px solid var(--line)",
                      borderBottom: "1px solid var(--line-soft)",
                      fontFamily: "var(--mono)",
                      fontSize: 10,
                      color: "var(--txt-faint)",
                      letterSpacing: "0.08em",
                      textTransform: "uppercase",
                    }}
                  >
                    {day} · <span className="num">{dayRuns.length} runs</span>
                  </div>
                  {dayRuns.map((run) => (
                    <RunRowView
                      key={run.runId}
                      run={run}
                      expanded={expanded === run.runId}
                      detail={details[run.runId]}
                      onToggle={() => handleExpand(run.runId)}
                      onOpenWorkflow={() =>
                        onNavigate({
                          kind: "editor",
                          workflowId: run.workflowId,
                        })
                      }
                    />
                  ))}
                </div>
              ))
            )}
          </div>
        </div>
      </main>

      <StatusRibbon
        workflowCount={0}
        runCount={runs.length}
        tail="history"
      />
    </div>
  );
}

function StatCell({
  label,
  value,
  color,
}: {
  label: string;
  value: number;
  color?: string;
}): JSX.Element {
  return (
    <div
      style={{
        padding: "14px 16px",
        background: "var(--bg-panel)",
        border: "1px solid var(--line)",
        borderRadius: 3,
      }}
    >
      <div
        style={{
          fontSize: 10,
          color: "var(--txt-faint)",
          letterSpacing: "0.12em",
          textTransform: "uppercase",
        }}
      >
        {label}
      </div>
      <div
        className="num"
        style={{
          fontFamily: "var(--mono)",
          fontSize: 28,
          fontWeight: 600,
          color: color ?? "var(--txt)",
          marginTop: 4,
          letterSpacing: "-0.01em",
        }}
      >
        {value}
      </div>
    </div>
  );
}

interface RunRowProps {
  run: RunRow;
  expanded: boolean;
  detail: RunDetail | undefined;
  onToggle: () => void;
  onOpenWorkflow: () => void;
}

function RunRowView({
  run,
  expanded,
  detail,
  onToggle,
  onOpenWorkflow,
}: RunRowProps): JSX.Element {
  const color = STATUS_COLOR[run.status] ?? "var(--line)";
  const startedIso = new Date(run.startedAt).toLocaleString();
  return (
    <div>
      <button
        type="button"
        onClick={onToggle}
        style={{
          appearance: "none",
          border: 0,
          width: "100%",
          background: expanded ? "var(--bg-hover)" : "transparent",
          padding: "10px 16px",
          textAlign: "left",
          cursor: "pointer",
          display: "grid",
          gridTemplateColumns: "24px 1.4fr 1fr 120px 100px 140px",
          gap: 10,
          alignItems: "center",
          fontFamily: "var(--mono)",
          fontSize: 12,
          color: "var(--txt)",
          borderBottom: "1px solid var(--line-soft)",
        }}
      >
        <span
          style={{
            width: 8,
            height: 8,
            borderRadius: 8,
            background: color,
            boxShadow: run.status === "running" ? `0 0 8px ${color}` : "none",
            animation:
              run.status === "running" ? "pulse 1.1s ease-in-out infinite" : undefined,
          }}
        />
        <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
          {run.workflowId}
        </span>
        <span
          style={{
            fontSize: 9.5,
            color,
            letterSpacing: "0.08em",
            textTransform: "uppercase",
            fontWeight: 600,
          }}
        >
          {run.status}
        </span>
        <span style={{ color: "var(--txt-dim)" }}>{startedIso}</span>
        <span className="num" style={{ color: "var(--txt-dim)" }}>
          {fmtDuration(run.durationMs)}
        </span>
        <span
          className="num"
          style={{
            color: "var(--txt-faint)",
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
          }}
          title={run.runId}
        >
          {run.runId.slice(0, 8)}
        </span>
      </button>
      {expanded ? (
        <div
          style={{
            padding: "12px 16px 14px",
            background: "var(--bg-canvas)",
            borderBottom: "1px solid var(--line)",
            fontFamily: "var(--mono)",
          }}
        >
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 12,
              marginBottom: 10,
            }}
          >
            <button
              type="button"
              className="btn"
              onClick={onOpenWorkflow}
              style={{ height: 24 }}
            >
              open workflow →
            </button>
            <span
              className="num"
              style={{ fontSize: 10, color: "var(--txt-faint)" }}
            >
              trigger: {run.triggerKind} · runId: {run.runId}
            </span>
          </div>
          {detail ? (
            <table
              style={{
                width: "100%",
                borderCollapse: "collapse",
                fontSize: 11,
              }}
            >
              <thead>
                <tr style={{ color: "var(--txt-faint)" }}>
                  <th style={th}>node</th>
                  <th style={th}>iter</th>
                  <th style={th}>attempt</th>
                  <th style={th}>status</th>
                  <th style={th}>duration</th>
                  <th style={th}>error</th>
                </tr>
              </thead>
              <tbody>
                {detail.nodeRuns.map((nr, idx) => (
                  <tr
                    key={`${nr.nodeId}-${nr.iteration}-${nr.attempt}-${idx}`}
                    style={{ borderTop: "1px solid var(--line-soft)" }}
                  >
                    <td style={td}>{nr.nodeId}</td>
                    <td style={td} className="num">
                      {nr.iteration}
                    </td>
                    <td style={td} className="num">
                      {nr.attempt}
                    </td>
                    <td style={{ ...td, color: STATUS_COLOR[nr.status] ?? "var(--txt)" }}>
                      {nr.status}
                    </td>
                    <td style={td} className="num">
                      {fmtDuration(nr.durationMs)}
                    </td>
                    <td style={{ ...td, color: "var(--err)" }}>
                      {nr.error ?? ""}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <div style={{ color: "var(--txt-faint)", fontSize: 11 }}>
              loading…
            </div>
          )}
        </div>
      ) : null}
    </div>
  );
}

const th: React.CSSProperties = {
  textAlign: "left",
  padding: "4px 6px",
  fontSize: 9.5,
  letterSpacing: "0.08em",
  textTransform: "uppercase",
  fontWeight: 600,
};

const td: React.CSSProperties = {
  padding: "5px 6px",
  fontSize: 11,
  verticalAlign: "top",
};
