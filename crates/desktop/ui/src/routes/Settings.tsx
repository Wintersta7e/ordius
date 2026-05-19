// Settings route — appearance, concurrency, retention, secrets,
// workspaces, model endpoints, about. Loads/saves through the
// engine's get_settings / set_settings commands.

import { useCallback, useEffect, useState } from "react";
import type { JSX } from "react";

import {
  type SecretMeta,
  type Settings as SettingsShape,
  type SystemStatus,
  type Workspace,
  addSecret,
  addWorkspace,
  getSettings,
  listSecrets,
  listWorkspaces,
  removeSecret,
  removeWorkspace,
  setSettings,
  systemStatus,
} from "../engine";
import { TopBar } from "../components/chrome/TopBar";
import { SectionTitle } from "../components/SectionTitle";
import { StatusRibbon } from "../components/home/StatusRibbon";
import { NoticeBanner } from "../components/NoticeBanner";
import {
  Field,
  KV,
  Mono,
  NumberInput,
  SegRow,
  TextInput,
} from "../components/properties/primitives";
import { fmtBytes } from "../lib/format";
import type { Route } from "../lib/router";

type SectionId =
  | "secrets"
  | "workspaces"
  | "retention"
  | "concurrency"
  | "models"
  | "appearance"
  | "about";

const SECTIONS: Array<{
  id: SectionId;
  label: string;
  description: string;
}> = [
  { id: "secrets", label: "Secrets", description: "API keys, tokens, passwords" },
  { id: "workspaces", label: "Workspaces", description: "Project folders workflows run against" },
  { id: "retention", label: "Retention", description: "Run history & workspace cleanup" },
  { id: "concurrency", label: "Concurrency", description: "Parallel workflow & node limits" },
  { id: "models", label: "Models", description: "Default LLM endpoints" },
  { id: "appearance", label: "Appearance", description: "Theme & visual preferences" },
  { id: "about", label: "About", description: "Version, paths, license" },
];

interface Props {
  theme: "dark" | "light";
  onThemeToggle: () => void;
  onNavigate: (route: Route) => void;
}

export function Settings({ theme, onThemeToggle }: Props): JSX.Element {
  const [active, setActive] = useState<SectionId>("secrets");
  const [settings, setSettingsState] = useState<SettingsShape | null>(null);
  const [secrets, setSecretsState] = useState<SecretMeta[]>([]);
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [status, setStatus] = useState<SystemStatus | null>(null);
  const [error, setError] = useState<string | null>(null);

  const insideTauri =
    typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;

  const reload = useCallback(async () => {
    if (!insideTauri) {
      setError(
        "running in browser preview · engine commands disabled — launch via `tauri dev` to edit real settings",
      );
      setSettingsState({
        theme,
        paletteSide: "left",
        edgeStyle: "bezier",
        density: "comfortable",
        grid: "dots",
        colorScheme: "jewel",
        maxConcurrentRuns: 3,
        retentionDays: 30,
        modelEndpoints: [],
      });
      setStatus({
        runsDbBytes: 0,
        workspacesBytes: 0,
        engineVersion: "preview",
        endpoints: [],
      });
      return;
    }
    try {
      const [s, sec, ws, sys] = await Promise.all([
        getSettings(),
        listSecrets(),
        listWorkspaces(),
        systemStatus(),
      ]);
      setSettingsState(s);
      setSecretsState(sec);
      setWorkspaces(ws);
      setStatus(sys);
      setError(null);
    } catch (e: unknown) {
      setError(String(e));
    }
  }, [insideTauri]);

  useEffect(() => {
    void reload();
  }, [reload]);

  const patchSettings = useCallback(
    async (patch: Partial<SettingsShape>) => {
      if (!settings) return;
      const next = { ...settings, ...patch };
      setSettingsState(next);
      if (!insideTauri) return;
      try {
        await setSettings(next);
      } catch (e: unknown) {
        setError(String(e));
      }
    },
    [settings, insideTauri],
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
      <TopBar pageLabel="settings" theme={theme} onThemeToggle={onThemeToggle} />

      <main
        style={{
          display: "grid",
          gridTemplateColumns: "200px 1fr",
          minHeight: 0,
          overflow: "hidden",
        }}
      >
        <aside
          style={{
            background: "var(--bg-panel)",
            borderRight: "1px solid var(--line)",
            padding: "12px 0",
            display: "flex",
            flexDirection: "column",
          }}
        >
          <div
            style={{
              padding: "0 18px 10px",
              fontFamily: "var(--mono)",
              fontSize: 10,
              letterSpacing: "0.18em",
              textTransform: "uppercase",
              color: "var(--accent)",
              display: "flex",
              alignItems: "center",
              gap: 6,
            }}
          >
            <span style={{ color: "var(--accent)" }}>┌</span>
            <span style={{ flex: 1 }}>sections</span>
            <span style={{ color: "var(--txt-faint)" }}>
              {SECTIONS.length} total
            </span>
            <span style={{ color: "var(--accent)" }}>┐</span>
          </div>
          {SECTIONS.map((section, i) => {
            const isActive = active === section.id;
            const num = String(i + 1).padStart(2, "0");
            return (
              <button
                key={section.id}
                type="button"
                onClick={() => setActive(section.id)}
                style={{
                  appearance: "none",
                  border: 0,
                  background: isActive ? "var(--bg-active)" : "transparent",
                  color: isActive ? "var(--txt)" : "var(--txt-dim)",
                  padding: "10px 18px",
                  textAlign: "left",
                  fontFamily: "var(--mono)",
                  cursor: "pointer",
                  borderLeft: `3px solid ${
                    isActive ? "var(--accent)" : "transparent"
                  }`,
                  display: "flex",
                  alignItems: "flex-start",
                  gap: 10,
                }}
              >
                <span
                  style={{
                    color: "var(--txt-faint)",
                    fontSize: 10,
                    paddingTop: 2,
                  }}
                >
                  {num}
                </span>
                <span style={{ flex: 1, minWidth: 0 }}>
                  <span
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: 6,
                      fontSize: 12.5,
                      fontWeight: 500,
                      letterSpacing: "0.02em",
                      color: isActive ? "var(--txt)" : "var(--txt)",
                    }}
                  >
                    {section.label}
                    {isActive ? (
                      <span
                        style={{ color: "var(--accent)", marginLeft: "auto" }}
                      >
                        ▸
                      </span>
                    ) : null}
                  </span>
                  <span
                    style={{
                      display: "block",
                      fontSize: 10.5,
                      color: "var(--txt-faint)",
                      marginTop: 2,
                      lineHeight: 1.3,
                    }}
                  >
                    {section.description}
                  </span>
                </span>
              </button>
            );
          })}
        </aside>

        <section
          style={{
            overflow: "auto",
            padding: "24px 36px 32px",
            maxWidth: 920,
            width: "100%",
            margin: "0 auto",
          }}
        >
          {error ? <NoticeBanner message={error} /> : null}

          {(() => {
            const idx = SECTIONS.findIndex((s) => s.id === active);
            if (idx < 0) return null;
            return (
              <div
                style={{
                  fontFamily: "var(--mono)",
                  fontSize: 10,
                  letterSpacing: "0.18em",
                  textTransform: "uppercase",
                  color: "var(--accent)",
                  marginBottom: 4,
                }}
              >
                section · {String(idx + 1).padStart(2, "0")}
              </div>
            );
          })()}
          {active === "appearance" ? (
            <AppearanceSection settings={settings} onPatch={patchSettings} />
          ) : null}
          {active === "secrets" ? (
            <SecretsSection
              secrets={secrets}
              onReload={reload}
              insideTauri={insideTauri}
            />
          ) : null}
          {active === "workspaces" ? (
            <WorkspacesSection
              workspaces={workspaces}
              onReload={reload}
              insideTauri={insideTauri}
            />
          ) : null}
          {active === "retention" ? (
            <RetentionSection settings={settings} onPatch={patchSettings} />
          ) : null}
          {active === "concurrency" ? (
            <ConcurrencySection settings={settings} onPatch={patchSettings} />
          ) : null}
          {active === "models" ? <ModelsSection settings={settings} /> : null}
          {active === "about" ? <AboutSection status={status} /> : null}
        </section>
      </main>

      <StatusRibbon
        workflowCount={0}
        runCount={0}
        tail={`settings · ${active}`}
      />
    </div>
  );
}

// ─── Appearance ──────────────────────────────────────────────────

function AppearanceSection({
  settings,
  onPatch,
}: {
  settings: SettingsShape | null;
  onPatch: (patch: Partial<SettingsShape>) => Promise<void>;
}): JSX.Element {
  if (!settings) return <Loading />;
  return (
    <div>
      <Heading text="Appearance" />
      <Card>
        <Field label="theme">
          <SegRow
            value={settings.theme}
            options={["dark", "light"]}
            onChange={(v) =>
              void onPatch({ theme: v as SettingsShape["theme"] })
            }
          />
        </Field>
        <Field label="palette side">
          <SegRow
            value={settings.paletteSide}
            options={["left", "right"]}
            onChange={(v) =>
              void onPatch({ paletteSide: v as SettingsShape["paletteSide"] })
            }
          />
        </Field>
        <Field label="edge style">
          <SegRow
            value={settings.edgeStyle}
            options={["bezier", "orthogonal", "straight"]}
            onChange={(v) =>
              void onPatch({ edgeStyle: v as SettingsShape["edgeStyle"] })
            }
          />
        </Field>
        <Field label="density">
          <SegRow
            value={settings.density}
            options={["comfortable", "rich"]}
            onChange={(v) =>
              void onPatch({ density: v as SettingsShape["density"] })
            }
          />
        </Field>
        <Field label="grid">
          <SegRow
            value={settings.grid}
            options={["dots", "lines", "off"]}
            onChange={(v) =>
              void onPatch({ grid: v as SettingsShape["grid"] })
            }
          />
        </Field>
        <Field label="category palette">
          <SegRow
            value={settings.colorScheme}
            options={["jewel", "citrus", "glacier"]}
            onChange={(v) =>
              void onPatch({
                colorScheme: v as SettingsShape["colorScheme"],
              })
            }
          />
        </Field>
      </Card>
    </div>
  );
}

// ─── Secrets ─────────────────────────────────────────────────────

function SecretsSection({
  secrets,
  onReload,
  insideTauri,
}: {
  secrets: SecretMeta[];
  onReload: () => Promise<void>;
  insideTauri: boolean;
}): JSX.Element {
  const [name, setName] = useState("");
  const [value, setValue] = useState("");
  const [busy, setBusy] = useState(false);

  const handleAdd = async () => {
    if (!insideTauri || !name.trim() || !value) return;
    setBusy(true);
    try {
      await addSecret(name.trim(), value);
      setName("");
      setValue("");
      await onReload();
    } finally {
      setBusy(false);
    }
  };

  const handleRemove = async (n: string) => {
    if (!insideTauri) return;
    setBusy(true);
    try {
      await removeSecret(n);
      await onReload();
    } finally {
      setBusy(false);
    }
  };

  return (
    <div>
      <Heading text="Secrets" sub="OS-keyring storage. Values never appear in the UI." />
      <Card>
        <Field label="name">
          <TextInput
            value={name}
            onChange={setName}
            placeholder="API_KEY"
          />
        </Field>
        <Field label="value">
          <TextInput
            value={value}
            onChange={setValue}
            placeholder="hidden after submit"
          />
        </Field>
        <div style={{ padding: "8px 16px 14px" }}>
          <button
            type="button"
            className="btn primary"
            disabled={!name.trim() || !value || busy || !insideTauri}
            onClick={handleAdd}
            style={{ height: 28 }}
          >
            store secret
          </button>
        </div>
      </Card>

      <SectionTitle label="stored" count={`${secrets.length} secrets`} />
      <div
        style={{
          marginTop: 12,
          background: "var(--bg-panel)",
          border: "1px solid var(--line)",
          borderRadius: 3,
        }}
      >
        {secrets.length === 0 ? (
          <div
            style={{
              padding: "16px",
              color: "var(--txt-faint)",
              fontSize: 11,
              fontFamily: "var(--mono)",
            }}
          >
            no secrets stored.
          </div>
        ) : (
          secrets.map((secret) => (
            <div
              key={secret.name}
              style={{
                display: "flex",
                alignItems: "center",
                padding: "10px 14px",
                borderBottom: "1px solid var(--line-soft)",
                fontFamily: "var(--mono)",
                fontSize: 12,
                color: "var(--txt)",
              }}
            >
              <span style={{ flex: 1 }}>{secret.name}</span>
              <button
                type="button"
                className="btn"
                onClick={() => void handleRemove(secret.name)}
                disabled={busy}
                style={{
                  height: 22,
                  padding: "0 10px",
                  color: "var(--err)",
                  borderColor: "var(--err)",
                }}
              >
                remove
              </button>
            </div>
          ))
        )}
      </div>
    </div>
  );
}

// ─── Workspaces ──────────────────────────────────────────────────

function WorkspacesSection({
  workspaces,
  onReload,
  insideTauri,
}: {
  workspaces: Workspace[];
  onReload: () => Promise<void>;
  insideTauri: boolean;
}): JSX.Element {
  const [name, setName] = useState("");
  const [path, setPath] = useState("");
  const [busy, setBusy] = useState(false);

  const handleAdd = async () => {
    if (!insideTauri || !name.trim() || !path.trim()) return;
    setBusy(true);
    try {
      await addWorkspace(name.trim(), path.trim());
      setName("");
      setPath("");
      await onReload();
    } finally {
      setBusy(false);
    }
  };

  const handleRemove = async (id: string) => {
    if (!insideTauri) return;
    setBusy(true);
    try {
      await removeWorkspace(id);
      await onReload();
    } finally {
      setBusy(false);
    }
  };

  return (
    <div>
      <Heading text="Workspaces" sub="Project directories bound to runs." />
      <Card>
        <Field label="name">
          <TextInput value={name} onChange={setName} placeholder="my-project" />
        </Field>
        <Field label="path" hint="absolute filesystem path">
          <TextInput
            value={path}
            onChange={setPath}
            placeholder="/home/josh/code/my-project"
          />
        </Field>
        <div style={{ padding: "8px 16px 14px" }}>
          <button
            type="button"
            className="btn primary"
            disabled={!name.trim() || !path.trim() || busy || !insideTauri}
            onClick={handleAdd}
            style={{ height: 28 }}
          >
            register workspace
          </button>
        </div>
      </Card>

      <SectionTitle label="registered" count={`${workspaces.length} workspaces`} />
      <div
        style={{
          marginTop: 12,
          background: "var(--bg-panel)",
          border: "1px solid var(--line)",
          borderRadius: 3,
        }}
      >
        {workspaces.length === 0 ? (
          <div
            style={{
              padding: "16px",
              color: "var(--txt-faint)",
              fontSize: 11,
              fontFamily: "var(--mono)",
            }}
          >
            no workspaces yet.
          </div>
        ) : (
          workspaces.map((workspace) => (
            <div
              key={workspace.id}
              style={{
                padding: "10px 14px",
                borderBottom: "1px solid var(--line-soft)",
                fontFamily: "var(--mono)",
                fontSize: 12,
                color: "var(--txt)",
                display: "grid",
                gridTemplateColumns: "1fr auto",
                gap: 10,
                alignItems: "center",
              }}
            >
              <div style={{ minWidth: 0 }}>
                <div>{workspace.name}</div>
                <div
                  style={{
                    fontSize: 10,
                    color: "var(--txt-faint)",
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                  }}
                  title={workspace.path}
                >
                  {workspace.path}
                </div>
              </div>
              <button
                type="button"
                className="btn"
                onClick={() => void handleRemove(workspace.id)}
                disabled={busy}
                style={{
                  height: 22,
                  padding: "0 10px",
                  color: "var(--err)",
                  borderColor: "var(--err)",
                }}
              >
                remove
              </button>
            </div>
          ))
        )}
      </div>
    </div>
  );
}

// ─── Concurrency ─────────────────────────────────────────────────

function ConcurrencySection({
  settings,
  onPatch,
}: {
  settings: SettingsShape | null;
  onPatch: (patch: Partial<SettingsShape>) => Promise<void>;
}): JSX.Element {
  if (!settings) return <Loading />;
  return (
    <div>
      <Heading
        text="Concurrency"
        sub="How many runs can launch in parallel before new ones queue."
      />
      <Card>
        <Field label="max concurrent runs" hint="default 4">
          <NumberInput
            value={settings.maxConcurrentRuns}
            onChange={(v) =>
              void onPatch({ maxConcurrentRuns: Math.max(1, Math.round(v)) })
            }
          />
        </Field>
      </Card>
    </div>
  );
}

function RetentionSection({
  settings,
  onPatch,
}: {
  settings: SettingsShape | null;
  onPatch: (patch: Partial<SettingsShape>) => Promise<void>;
}): JSX.Element {
  if (!settings) return <Loading />;
  return (
    <div>
      <Heading
        text="Retention"
        sub="How long run history and recorded events stick around before cleanup."
      />
      <Card>
        <Field label="retention (days)" hint="0 → keep forever">
          <NumberInput
            value={settings.retentionDays}
            onChange={(v) =>
              void onPatch({ retentionDays: Math.max(0, Math.round(v)) })
            }
          />
        </Field>
      </Card>
    </div>
  );
}

// ─── Models ──────────────────────────────────────────────────────

function ModelsSection({
  settings,
}: {
  settings: SettingsShape | null;
}): JSX.Element {
  if (!settings) return <Loading />;
  return (
    <div>
      <Heading
        text="Model endpoints"
        sub="OpenAI-compatible URLs the llm node can target. Adding endpoints is a Phase 1.10 polish item."
      />
      {settings.modelEndpoints.length === 0 ? (
        <Card>
          <div
            style={{
              padding: "16px",
              color: "var(--txt-faint)",
              fontSize: 11,
              fontFamily: "var(--mono)",
            }}
          >
            no endpoints registered. point the llm node at any
            OpenAI-compatible URL via `{`{{secrets.OPENAI_KEY}}`}` for now.
          </div>
        </Card>
      ) : (
        settings.modelEndpoints.map((endpoint) => (
          <Card key={endpoint.id}>
            <Field label="name">
              <Mono value={endpoint.name} />
            </Field>
            <Field label="base url">
              <Mono value={endpoint.baseUrl} />
            </Field>
            <Field label="api key secret">
              <Mono value={endpoint.apiKeySecret ?? "(none)"} />
            </Field>
          </Card>
        ))
      )}
    </div>
  );
}

// ─── About ───────────────────────────────────────────────────────

function AboutSection({
  status,
}: {
  status: SystemStatus | null;
}): JSX.Element {
  return (
    <div>
      <Heading text="About" />
      <Card>
        <KV k="engine version" v={status?.engineVersion ?? "—"} />
        <KV
          k="runs.db"
          v={status ? fmtBytes(status.runsDbBytes) : "—"}
        />
        <KV
          k="workspaces dir"
          v={status ? fmtBytes(status.workspacesBytes) : "—"}
        />
        <KV
          k="endpoint health"
          v={
            status && status.endpoints.length > 0
              ? `${status.endpoints.filter((e) => e.state === "ok").length}/${status.endpoints.length} ok`
              : "no endpoints"
          }
        />
        <KV k="gui stack" v="tauri 2 · vite · react 18 · typescript" />
      </Card>
    </div>
  );
}

// ─── Layout helpers ──────────────────────────────────────────────

function Heading({
  text,
  sub,
}: {
  text: string;
  sub?: string;
}): JSX.Element {
  return (
    <div style={{ marginBottom: 16 }}>
      <h1
        style={{
          fontFamily: "var(--display)",
          fontWeight: 600,
          fontSize: 24,
          margin: 0,
          color: "var(--txt)",
          letterSpacing: "-0.01em",
        }}
      >
        {text}
      </h1>
      {sub ? (
        <p
          style={{
            margin: "4px 0 0",
            color: "var(--txt-dim)",
            fontSize: 12,
            lineHeight: 1.5,
            maxWidth: 560,
          }}
        >
          {sub}
        </p>
      ) : null}
    </div>
  );
}

function Card({ children }: { children: React.ReactNode }): JSX.Element {
  return (
    <div
      style={{
        background: "var(--bg-panel)",
        border: "1px solid var(--line)",
        borderRadius: 3,
        marginBottom: 18,
      }}
    >
      {children}
    </div>
  );
}

function Loading(): JSX.Element {
  return (
    <div
      style={{
        padding: 14,
        color: "var(--txt-faint)",
        fontSize: 11,
        fontFamily: "var(--mono)",
      }}
    >
      loading…
    </div>
  );
}
