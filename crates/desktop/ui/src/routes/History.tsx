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
import { demoHistoryRuns } from "../data/demoHistory";
import { NoticeBanner } from "../components/NoticeBanner";

interface Props {
  theme: "dark" | "light";
  onThemeToggle: () => void;
  onNavigate: (route: Route) => void;
}

type StatusFilter = "all" | "done" | "error" | "stopped" | "running";
type TimeRange = "24h" | "7d" | "all";
const RANGE_MS: Record<TimeRange, number | null> = {
  "24h": 24 * 60 * 60 * 1000,
  "7d": 7 * 24 * 60 * 60 * 1000,
  all: null,
};

const TRIGGER_GLYPH: Record<string, string> = {
  manual: "•",
  cli: "$",
  schedule: "@",
  api: "▦",
};

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
  const [timeRange, setTimeRange] = useState<TimeRange>("7d");
  const [query, setQuery] = useState("");
  const [expanded, setExpanded] = useState<string | null>(null);
  const [details, setDetails] = useState<Record<string, RunDetail>>({});

  const insideTauri =
    typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;

  useEffect(() => {
    if (!insideTauri) {
      setError(
        "running in browser preview · engine commands disabled — launch via `tauri dev` to see real runs",
      );
      setRuns(demoHistoryRuns(Date.now()));
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
    const finished = runs.filter(
      (r) => r.status === "done" || r.status === "error",
    );
    const successRate =
      finished.length === 0 ? null : (done / finished.length) * 100;
    const durations = finished
      .map((r) => r.durationMs ?? 0)
      .filter((d) => d > 0);
    const avgDurationMs =
      durations.length === 0
        ? null
        : durations.reduce((a, b) => a + b, 0) / durations.length;
    return { total, done, failed, stopped, successRate, avgDurationMs };
  }, [runs]);

  const filtered = useMemo(() => {
    const now = Date.now();
    const rangeMs = RANGE_MS[timeRange];
    const cutoff = rangeMs == null ? null : now - rangeMs;
    const q = query.trim().toLowerCase();
    return runs.filter((r) => {
      if (statusFilter !== "all" && r.status !== statusFilter) return false;
      if (cutoff != null && r.startedAt < cutoff) return false;
      if (q.length > 0) {
        const hay = `${r.workflowId} ${r.runId} ${r.triggerKind}`.toLowerCase();
        if (!hay.includes(q)) return false;
      }
      return true;
    });
  }, [runs, statusFilter, timeRange, query]);

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
            <div style={{ marginTop: 16 }}>
              <NoticeBanner message={error} />
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
            <StatCell label="total runs" value={String(stats.total)} />
            <StatCell
              label="success rate"
              value={
                stats.successRate == null
                  ? "—"
                  : `${Math.round(stats.successRate)}%`
              }
              color="var(--warn)"
            />
            <StatCell
              label="avg duration"
              value={
                stats.avgDurationMs == null
                  ? "—"
                  : fmtDuration(stats.avgDurationMs)
              }
            />
            <StatCell
              label="errors"
              value={String(stats.failed)}
              color="var(--err)"
            />
          </div>

          {/* Search + time-range row */}
          <div
            style={{
              display: "flex",
              gap: 12,
              alignItems: "center",
              marginBottom: 14,
              padding: "10px 12px",
              background: "var(--bg-panel)",
              border: "1px solid var(--line)",
              borderRadius: 3,
            }}
          >
            <div style={{ position: "relative", flex: 1 }}>
              <span
                style={{
                  position: "absolute",
                  left: 10,
                  top: "50%",
                  transform: "translateY(-50%)",
                  color: "var(--txt-faint)",
                  fontSize: 11,
                  fontFamily: "var(--mono)",
                }}
              >
                ⌕
              </span>
              <input
                type="search"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="search workflow, run id, or trigger…"
                style={{
                  width: "100%",
                  height: 28,
                  padding: "0 10px 0 28px",
                  background: "var(--bg-input)",
                  border: "1px solid var(--line)",
                  borderRadius: 3,
                  fontFamily: "var(--mono)",
                  fontSize: 11.5,
                  color: "var(--txt)",
                }}
              />
            </div>
            <span
              style={{
                fontSize: 10,
                color: "var(--txt-faint)",
                letterSpacing: "0.08em",
                textTransform: "uppercase",
              }}
            >
              range
            </span>
            <div
              style={{
                display: "inline-flex",
                background: "var(--bg-input)",
                border: "1px solid var(--line)",
                borderRadius: 3,
                padding: 2,
              }}
            >
              {(["24h", "7d", "all"] as TimeRange[]).map((r) => (
                <button
                  key={r}
                  type="button"
                  onClick={() => setTimeRange(r)}
                  style={{
                    appearance: "none",
                    border: 0,
                    background:
                      timeRange === r ? "var(--bg-active)" : "transparent",
                    color: timeRange === r ? "var(--txt)" : "var(--txt-dim)",
                    fontFamily: "var(--mono)",
                    fontSize: 11,
                    padding: "3px 10px",
                    height: 20,
                    borderRadius: 2,
                    cursor: "pointer",
                  }}
                >
                  {r === "24h" ? "24h" : r === "7d" ? "7 days" : "all time"}
                </button>
              ))}
            </div>
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
  value: string;
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
          gridTemplateColumns: "24px 1.4fr 1fr 120px 100px 100px 140px",
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
          style={{
            display: "inline-flex",
            alignItems: "center",
            gap: 6,
            color: "var(--txt-dim)",
            fontSize: 11.5,
          }}
        >
          <span style={{ color: "var(--accent)" }}>
            {TRIGGER_GLYPH[run.triggerKind] ?? "·"}
          </span>
          {run.triggerKind}
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
          {run.runId.length > 14 ? `${run.runId.slice(0, 12)}…` : run.runId}
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
