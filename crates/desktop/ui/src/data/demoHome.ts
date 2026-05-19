import type {
  RunRow,
  SavedWorkflow,
  SystemStatus,
  Workspace,
} from "../engine/types";

const HOUR = 60 * 60 * 1000;

export function demoHomeData(now: number): {
  workflows: SavedWorkflow[];
  runs: RunRow[];
  workspaces: Workspace[];
  status: SystemStatus;
} {
  const workflows: SavedWorkflow[] = [
    {
      id: "wf-critique",
      name: "code-critique-loop",
      triggersCount: 1,
      nodesCount: 7,
      category: "execution",
      description: "Read source, fan out to N reviewers, apply patches, test, commit.",
    },
    {
      id: "wf-bakeoff",
      name: "model-bake-off",
      triggersCount: 1,
      nodesCount: 9,
      category: "llm",
      description: "Same prompt fan-out to N models, merge results, transform diff, pick best.",
    },
    {
      id: "wf-rag",
      name: "rag-query",
      triggersCount: 2,
      nodesCount: 5,
      category: "data",
      description: "Query → embed → retrieve → llm → answer. Wired against the personal-notes corpus.",
    },
    {
      id: "wf-nightly",
      name: "nightly-etl",
      triggersCount: 1,
      nodesCount: 6,
      category: "integration",
      description: "Pulls logs from blob store, summarises with local LLM, writes daily digest to kv-store.",
    },
    {
      id: "wf-build",
      name: "build-and-deploy",
      triggersCount: 1,
      nodesCount: 6,
      category: "execution",
      description: "git → shell test → condition → container build → http deploy hook.",
    },
    {
      id: "wf-digest",
      name: "scheduled-digest",
      triggersCount: 1,
      nodesCount: 4,
      category: "llm",
      description: "Cron → llm summarises yesterday's logs → desktop notification.",
    },
  ];

  const runs: RunRow[] = [
    {
      runId: "run_00028",
      workflowId: "wf-critique",
      status: "done",
      startedAt: now - 8 * 60 * 1000,
      finishedAt: now - 8 * 60 * 1000 + 5180,
      durationMs: 5180,
      triggerKind: "manual",
    },
    {
      runId: "run_00027",
      workflowId: "wf-bakeoff",
      status: "running",
      startedAt: now - 12 * 60 * 1000,
      finishedAt: null,
      durationMs: null,
      triggerKind: "manual",
    },
    {
      runId: "run_00026",
      workflowId: "wf-rag",
      status: "done",
      startedAt: now - 45 * 60 * 1000,
      finishedAt: now - 45 * 60 * 1000 + 2410,
      durationMs: 2410,
      triggerKind: "cli",
    },
    {
      runId: "run_00025",
      workflowId: "wf-critique",
      status: "error",
      startedAt: now - HOUR,
      finishedAt: now - HOUR + 4720,
      durationMs: 4720,
      triggerKind: "manual",
    },
    {
      runId: "run_00024",
      workflowId: "wf-nightly",
      status: "done",
      startedAt: now - 2 * HOUR,
      finishedAt: now - 2 * HOUR + 184000,
      durationMs: 184000,
      triggerKind: "schedule",
    },
    {
      runId: "run_00023",
      workflowId: "wf-rag",
      status: "done",
      startedAt: now - 3 * HOUR,
      finishedAt: now - 3 * HOUR + 2110,
      durationMs: 2110,
      triggerKind: "cli",
    },
  ];

  const workspaces: Workspace[] = [
    { id: "ws-josh", name: "josh", path: "~/code/project-a" },
  ];

  const status: SystemStatus = {
    runsDbBytes: 184 * 1024 * 1024,
    workspacesBytes: 68 * 1024 * 1024,
    engineVersion: "preview",
    endpoints: [
      { id: "ollama", name: "ollama", state: "ok" },
      { id: "openai", name: "openai api", state: "ok" },
      { id: "anthropic", name: "anthropic api", state: "ok" },
      { id: "docker", name: "docker daemon", state: "ok" },
    ],
  };

  return { workflows, runs, workspaces, status };
}
