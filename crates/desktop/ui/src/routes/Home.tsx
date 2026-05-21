// Home route — workflow grid + left rail + recent runs.
//
// Wires real engine data through the Tauri command layer:
//   * listWorkflows  → grid cards (joined with listRuns for last-run badge)
//   * listRuns       → recent-runs strip + per-workflow last-run join
//   * systemStatus   → left-rail "system" card
//   * listWorkspaces → left-rail "workspace" card
//
// The engine doesn't model per-workflow category / description /
// star yet (Workflow has id + name only). Until that lands the
// card derives a category from the first node on the graph
// (which we'd have to load) — for the Home grid we settle for
// "control" as a neutral default. The desc field is derived from
// the trigger + node counts.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { JSX } from "react";

import {
  type RunRow,
  type SavedWorkflow,
  type EnvironmentReport,
  type SystemStatus,
  type Workflow,
  type Workspace,
  deleteWorkflow,
  duplicateWorkflow,
  listRuns,
  listWorkflows,
  listWorkspaces,
  saveWorkflow,
  systemEnvironment,
  systemStatus,
  validateWorkflow,
} from "../engine";
import { navigate as navigateRoute } from "../lib/router";
import { TopBar } from "../components/chrome/TopBar";
import { Hero } from "../components/home/Hero";
import { LeftRail, type RunningWorkflow } from "../components/home/LeftRail";
import {
  NewWorkflowCard,
  WorkflowCard,
  type WorkflowCardData,
} from "../components/home/WorkflowCard";
import { RecentRunRow } from "../components/home/RecentRunRow";
import { SectionTitle } from "../components/SectionTitle";
import { StatusRibbon } from "../components/home/StatusRibbon";
import { demoHomeData } from "../data/demoHome";
import { NoticeBanner } from "../components/NoticeBanner";
import { PillRow } from "../components/PillRow";

type SortKey = "recent" | "name" | "runs";

interface SortOption {
  id: SortKey;
  label: string;
}

const SORT_OPTIONS: SortOption[] = [
  { id: "recent", label: "recent" },
  { id: "name", label: "a → z" },
  { id: "runs", label: "runs" },
];

interface Props {
  theme: "dark" | "light";
  onThemeToggle: () => void;
}

export function Home({ theme, onThemeToggle }: Props): JSX.Element {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(id);
  }, []);

  const [workflows, setWorkflows] = useState<SavedWorkflow[]>([]);
  const [runs, setRuns] = useState<RunRow[]>([]);
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [status, setStatus] = useState<SystemStatus | null>(null);
  const [environment, setEnvironment] = useState<EnvironmentReport | null>(
    null,
  );
  const [error, setError] = useState<string | null>(null);
  const [sort, setSort] = useState<SortKey>("recent");

  const reload = useCallback(async () => {
    // When the page is loaded outside Tauri (plain Vite dev preview
    // in a browser, no host injecting window.__TAURI_INTERNALS__),
    // every invoke throws. Friendly banner > raw TypeError stack.
    const insideTauri =
      typeof window !== "undefined" &&
      "__TAURI_INTERNALS__" in window;
    if (!insideTauri) {
      setError(
        "running in browser preview · engine commands disabled — launch via `tauri dev` to load real data",
      );
      const demo = demoHomeData(Date.now());
      setWorkflows(demo.workflows);
      setRuns(demo.runs);
      setWorkspaces(demo.workspaces);
      setStatus(demo.status);
      setEnvironment({
        platform: "wsl",
        wslDistro: "Ubuntu-24.04",
        endpoints: [
          {
            kind: "ollama",
            name: "Ollama (127.0.0.1:11434)",
            baseUrl: "http://127.0.0.1:11434",
          },
        ],
      });
      return;
    }
    try {
      const [wfs, allRuns, wsList, sys, env] = await Promise.all([
        listWorkflows(),
        listRuns({ limit: 100 }),
        listWorkspaces(),
        systemStatus(),
        systemEnvironment(),
      ]);
      setWorkflows(wfs);
      setRuns(allRuns);
      setWorkspaces(wsList);
      setStatus(sys);
      setEnvironment(env);
      setError(null);
    } catch (e: unknown) {
      setError(String(e));
    }
  }, []);

  useEffect(() => {
    void reload();
  }, [reload]);

  const runsByWorkflow = useMemo(() => {
    const map = new Map<string, RunRow[]>();
    for (const run of runs) {
      const arr = map.get(run.workflowId) ?? [];
      arr.push(run);
      map.set(run.workflowId, arr);
    }
    return map;
  }, [runs]);

  const cards: WorkflowCardData[] = useMemo(() => {
    return workflows.map((wf) => {
      const workflowRuns = runsByWorkflow.get(wf.id) ?? [];
      const last = workflowRuns[0]; // already sorted DESC by startedAt
      return {
        id: wf.id,
        name: wf.name,
        desc:
          wf.description ??
          `${wf.nodesCount} nodes · ${
            wf.triggersCount === 0 ? "manual-only" : `${wf.triggersCount} triggers`
          }`,
        category: wf.category ?? "control",
        triggerKinds:
          wf.triggersCount === 0
            ? ["manual"]
            : new Array(wf.triggersCount).fill("manual"),
        nodeCount: wf.nodesCount,
        lastRun: last
          ? {
              status: last.status,
              startedAt: last.startedAt,
              durationMs: last.durationMs,
            }
          : null,
        totalRuns: workflowRuns.length,
      };
    });
  }, [workflows, runsByWorkflow]);

  const sortedCards = useMemo(() => {
    const arr = [...cards];
    switch (sort) {
      case "name":
        arr.sort((a, b) => a.name.localeCompare(b.name));
        break;
      case "runs":
        arr.sort((a, b) => b.totalRuns - a.totalRuns);
        break;
      case "recent":
      default:
        arr.sort((a, b) => {
          const ta = a.lastRun?.startedAt ?? 0;
          const tb = b.lastRun?.startedAt ?? 0;
          return tb - ta;
        });
        break;
    }
    return arr;
  }, [cards, sort]);

  const runningWorkflows: RunningWorkflow[] = useMemo(() => {
    return runs
      .filter((r) => r.status === "running")
      .map((r) => {
        const wf = workflows.find((w) => w.id === r.workflowId);
        return {
          id: r.workflowId,
          name: wf?.name ?? r.workflowId,
          runId: r.runId,
          startedAt: r.startedAt,
        };
      });
  }, [runs, workflows]);

  const recentRuns = useMemo(() => runs.slice(0, 10), [runs]);
  const activeWorkspace = workspaces[0] ?? null;

  const handleOpen = useCallback((id: string) => {
    navigateRoute({ kind: "editor", workflowId: id });
  }, []);
  const handleRun = useCallback((id: string) => {
    console.warn("run dialog lands in Phase 1.9", { id });
  }, []);
  const handleDuplicate = useCallback(async (id: string) => {
    const insideTauri =
      typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;
    if (!insideTauri) {
      setError("duplicate requires the desktop host");
      return;
    }
    try {
      const clone = await duplicateWorkflow(id);
      setError(null);
      const next = await listWorkflows();
      setWorkflows(next);
      // Open the freshly-cloned workflow so the user can rename
      // immediately if they want.
      navigateRoute({ kind: "editor", workflowId: clone.id });
    } catch (e: unknown) {
      setError(`duplicate failed: ${String(e)}`);
    }
  }, []);
  const handleDelete = useCallback(async (id: string) => {
    const insideTauri =
      typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;
    if (!insideTauri) {
      setError("delete requires the desktop host");
      return;
    }
    const ok = window.confirm(
      `Delete workflow "${id}"?\n\nThis removes the on-disk JSON. Saved run history is kept.`,
    );
    if (!ok) return;
    try {
      const deleted = await deleteWorkflow(id);
      if (!deleted) {
        setError(`delete: workflow ${id} not found`);
        return;
      }
      setError(null);
      const next = await listWorkflows();
      setWorkflows(next);
    } catch (e: unknown) {
      setError(`delete failed: ${String(e)}`);
    }
  }, []);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const handleImport = useCallback(() => {
    fileInputRef.current?.click();
  }, []);

  const handleImportFile = useCallback(
    async (event: React.ChangeEvent<HTMLInputElement>) => {
      const file = event.target.files?.[0];
      // Reset so re-importing the same file fires onChange again.
      event.target.value = "";
      if (!file) return;
      const insideTauri =
        typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;
      if (!insideTauri) {
        setError("import requires the desktop host");
        return;
      }
      try {
        const text = await file.text();
        const parsed: Workflow = JSON.parse(text);
        await validateWorkflow(parsed);
        await saveWorkflow(parsed);
        setError(null);
        // Refresh the workflow grid so the new card appears.
        const next = await listWorkflows();
        setWorkflows(next);
      } catch (e: unknown) {
        setError(`import failed: ${String(e)}`);
      }
    },
    [],
  );
  const handleNewWorkflow = useCallback(() => {
    navigateRoute({ kind: "editor" });
  }, []);

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
      <TopBar pageLabel="home" theme={theme} onThemeToggle={onThemeToggle} />

      <main
        style={{
          overflow: "hidden",
          display: "flex",
          flexDirection: "column",
        }}
      >
        <div
          style={{
            maxWidth: 1280,
            margin: "0 auto",
            width: "100%",
            padding: "32px 36px 0",
            flexShrink: 0,
          }}
        >
          <Hero
            workspaceName={activeWorkspace?.name ?? "default"}
            workflowCount={workflows.length}
            runCount={runs.length}
            runningCount={runningWorkflows.length}
            onImport={handleImport}
            onNew={handleNewWorkflow}
          />
        </div>

        {error ? (
          <div
            style={{
              maxWidth: 1280,
              margin: "8px auto 0",
              width: "100%",
              paddingInline: 36,
            }}
          >
            <NoticeBanner message={error} />
          </div>
        ) : null}

        <div
          style={{
            maxWidth: 1280,
            margin: "0 auto",
            width: "100%",
            padding: "24px 36px 0",
            flex: 1,
            minHeight: 0,
            display: "grid",
            gridTemplateColumns: "260px 1fr",
            gap: 28,
          }}
        >
          <LeftRail
            running={runningWorkflows}
            workspace={activeWorkspace}
            status={status}
            environment={environment}
            now={now}
          />

          <div
            style={{
              display: "flex",
              flexDirection: "column",
              minHeight: 0,
              overflow: "hidden",
            }}
          >
            <SectionTitle
              label="workflows"
              count={`${sortedCards.length} saved`}
              right={
                <PillRow value={sort} options={SORT_OPTIONS} onChange={setSort} />
              }
            />

            <div
              style={{
                flex: 1,
                minHeight: 0,
                overflow: "auto",
                marginTop: 14,
                paddingRight: 4,
                marginRight: -4,
              }}
            >
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: "repeat(auto-fill, minmax(340px, 1fr))",
                  gap: 14,
                  paddingBottom: 4,
                }}
              >
                {sortedCards.map((card) => (
                  <WorkflowCard
                    key={card.id}
                    workflow={card}
                    onOpen={handleOpen}
                    onRun={handleRun}
                    onDelete={(id) => void handleDelete(id)}
                    onDuplicate={(id) => void handleDuplicate(id)}
                  />
                ))}
                <NewWorkflowCard onClick={handleNewWorkflow} />
              </div>
            </div>

            <section style={{ marginTop: 24, marginBottom: 24, flexShrink: 0 }}>
              <SectionTitle
                label="recent runs"
                count={`last ${recentRuns.length}`}
              />
              <div
                style={{
                  marginTop: 14,
                  background: "var(--bg-panel)",
                  border: "1px solid var(--line)",
                  borderRadius: 3,
                  overflow: "hidden",
                  maxHeight: 220,
                  overflowY: "auto",
                }}
              >
                {recentRuns.length === 0 ? (
                  <div
                    style={{
                      padding: "20px 16px",
                      fontFamily: "var(--mono)",
                      fontSize: 11,
                      color: "var(--txt-faint)",
                    }}
                  >
                    no runs yet — trigger a workflow and they'll land here.
                  </div>
                ) : (
                  recentRuns.map((run, idx) => {
                    const wf = workflows.find((w) => w.id === run.workflowId);
                    return (
                      <RecentRunRow
                        key={run.runId}
                        run={run}
                        workflowName={wf?.name ?? run.workflowId}
                        last={idx === recentRuns.length - 1}
                        now={now}
                      />
                    );
                  })
                )}
              </div>
            </section>
          </div>
        </div>
      </main>

      <StatusRibbon workflowCount={workflows.length} runCount={runs.length} />

      <input
        ref={fileInputRef}
        type="file"
        accept="application/json,.json,.yaml,.yml"
        style={{ display: "none" }}
        onChange={(event) => void handleImportFile(event)}
      />
    </div>
  );
}

