// Settings route — appearance, concurrency, retention, secrets,
// workspaces, model endpoints, about. Loads/saves through the
// engine's get_settings / set_settings commands.

import { useCallback, useEffect, useState } from "react";
import type { JSX } from "react";
import { listen } from "@tauri-apps/api/event";

import {
  type SecretMeta,
  type EnvEntryIpc,
  type EnvKindIpc,
  type EnvSnapshotIpc,
  type EnvResourceIpc,
  type HostDirectTestResultIpc,
  type HostDirectVerificationIpc,
  type Settings as SettingsShape,
  type SystemStatus,
  type Workspace,
  addEnvironmentResource,
  addSecret,
  addWorkspace,
  enableHostDirect,
  getSettings,
  listEnvironments,
  listSecrets,
  listWorkspaces,
  refreshEnvironment,
  removeEnvironment,
  renameWorkspace,
  removeSecret,
  removeWorkspace,
  setEnvironmentEnabled,
  setSettings,
  systemStatus,
  testHostDirect,
} from "../engine";
import { TopBar } from "../components/chrome/TopBar";
import { SectionTitle } from "../components/SectionTitle";
import { StatusRibbon } from "../components/home/StatusRibbon";
import { NoticeBanner } from "../components/NoticeBanner";
import {
  Field,
  KV,
  NumberInput,
  SegRow,
  TextInput,
} from "../components/properties/primitives";
import { fmtBytes } from "../lib/format";
import type { Route } from "../lib/router";

type SectionId =
  | "secrets"
  | "environments"
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
  { id: "environments", label: "Environments", description: "Environments probed for LLM services" },
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
  onThemeChange: (theme: "dark" | "light") => void;
  onNavigate: (route: Route) => void;
}

export function Settings({
  theme,
  onThemeToggle,
  onThemeChange,
}: Props): JSX.Element {
  const [active, setActive] = useState<SectionId>("secrets");
  const [settings, setSettingsState] = useState<SettingsShape | null>(null);
  const [secrets, setSecretsState] = useState<SecretMeta[]>([]);
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [status, setStatus] = useState<SystemStatus | null>(null);
  const [environment, setEnvironment] = useState<EnvSnapshotIpc | null>(null);
  const [error, setError] = useState<string | null>(null);

  const activeIndex = SECTIONS.findIndex((s) => s.id === active);
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
      setEnvironment({
        envs: [
          {
            id: "local",
            label: "Local (this machine)",
            kind: "local",
            enabled: true,
            state: { state: "reachable" },
            resources: [
              {
                id: "ollama",
                kind: "http_endpoint",
                state: { state: "found" },
                baseUrl: "http://127.0.0.1:11434",
                version: null,
                routeOrigin: "env_loopback",
              },
            ],
          },
        ],
      });
      setWorkspaces([
        {
          id: "demo-ws-1",
          name: "code-review-loop",
          path: "/home/user/code/project-a",
        },
        {
          id: "demo-ws-2",
          name: "personal-notes",
          path: "/home/user/notes",
        },
      ]);
      return;
    }
    try {
      const [s, sec, ws, sys, env] = await Promise.all([
        getSettings(),
        listSecrets(),
        listWorkspaces(),
        systemStatus(),
        listEnvironments(),
      ]);
      setSettingsState(s);
      setSecretsState(sec);
      setWorkspaces(ws);
      setStatus(sys);
      setEnvironment(env);
      setError(null);
    } catch (e: unknown) {
      setError(String(e));
    }
  }, [insideTauri]);

  useEffect(() => {
    void reload();
  }, [reload]);

  useEffect(() => {
    if (!insideTauri) return;
    let disposed = false;
    const unlisten = listen("env_refresh_completed", () => {
      void reload();
    }).catch((e: unknown) => {
      if (!disposed) {
        setError(String(e));
      }
      return null;
    });
    return () => {
      disposed = true;
      void unlisten.then((removeListener) => {
        if (removeListener) {
          removeListener();
        }
      });
    };
  }, [insideTauri, reload]);

  const patchSettings = useCallback(
    async (patch: Partial<SettingsShape>) => {
      if (!settings) return;
      const next = { ...settings, ...patch };
      setSettingsState(next);
      if (patch.theme && patch.theme !== theme) {
        onThemeChange(patch.theme);
      }
      if (!insideTauri) return;
      try {
        await setSettings(next);
      } catch (e: unknown) {
        setError(String(e));
      }
    },
    [settings, theme, onThemeChange, insideTauri],
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

          {activeIndex >= 0 ? (
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
              section · {String(activeIndex + 1).padStart(2, "0")}
            </div>
          ) : null}
          {active === "appearance" ? (
            <AppearanceSection settings={settings} onPatch={patchSettings} />
          ) : null}
          {active === "secrets" ? (
            <SecretsSection
              secrets={secrets}
              onReload={reload}
              onError={setError}
              insideTauri={insideTauri}
            />
          ) : null}
          {active === "environments" ? (
            <EnvironmentsSection
              environment={environment}
              onEnvironmentChange={setEnvironment}
              onError={setError}
              insideTauri={insideTauri}
            />
          ) : null}
          {active === "workspaces" ? (
            <WorkspacesSection
              workspaces={workspaces}
              onReload={reload}
              onError={setError}
              insideTauri={insideTauri}
            />
          ) : null}
          {active === "retention" ? (
            <RetentionSection settings={settings} onPatch={patchSettings} />
          ) : null}
          {active === "concurrency" ? (
            <ConcurrencySection settings={settings} onPatch={patchSettings} />
          ) : null}
          {active === "models" ? (
            <ModelsSection
              settings={settings}
              secrets={secrets}
              environment={environment}
              onPatch={patchSettings}
            />
          ) : null}
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
  onError,
  insideTauri,
}: {
  secrets: SecretMeta[];
  onReload: () => Promise<void>;
  onError: (msg: string) => void;
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
    } catch (e) {
      onError(`add secret: ${String(e)}`);
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
    } catch (e) {
      onError(`remove secret: ${String(e)}`);
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
            type="password"
            autoComplete="new-password"
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

// ─── Environments ────────────────────────────────────────────────

function EnvironmentsSection({
  environment,
  onEnvironmentChange,
  onError,
  insideTauri,
}: {
  environment: EnvSnapshotIpc | null;
  onEnvironmentChange: (env: EnvSnapshotIpc) => void;
  onError: (msg: string) => void;
  insideTauri: boolean;
}): JSX.Element | null {
  const [busy, setBusy] = useState(false);
  const [opError, setOpError] = useState<string | null>(null);

  if (!environment) return null;

  const handleToggle = async (id: string, enabled: boolean) => {
    setBusy(true);
    setOpError(null);
    try {
      const env = await setEnvironmentEnabled(id, enabled);
      onEnvironmentChange(env);
    } catch (e) {
      setOpError(String(e));
    } finally {
      setBusy(false);
    }
  };

  const handleRemove = async (id: string) => {
    if (!window.confirm(`Remove ${id}?`)) return;
    setBusy(true);
    setOpError(null);
    try {
      const env = await removeEnvironment(id);
      onEnvironmentChange(env);
    } catch (e) {
      setOpError(String(e));
    } finally {
      setBusy(false);
    }
  };

  const handleRefresh = async (id?: string) => {
    setBusy(true);
    setOpError(null);
    try {
      const env = await refreshEnvironment(id);
      onEnvironmentChange(env);
    } catch (e) {
      setOpError(String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div>
      <Heading
        text="Environments"
        sub="environments probed for LLM endpoints, binaries, and toolchains"
      />
      <Card>
        {environment.envs.map((env) => (
          <EnvRow
            key={env.id}
            env={env}
            busy={busy}
            onToggle={(enabled) => void handleToggle(env.id, enabled)}
            onRemove={() => void handleRemove(env.id)}
            onRefresh={() => void handleRefresh(env.id)}
            onResourceAdded={onEnvironmentChange}
            onError={onError}
            insideTauri={insideTauri}
          />
        ))}
        {opError ? (
          <div
            style={{
              padding: "8px 14px",
              color: "var(--warn)",
              fontSize: 11,
              fontFamily: "var(--mono)",
              borderTop: "1px solid var(--line-soft)",
            }}
          >
            {opError}
          </div>
        ) : null}
        <div
          style={{
            padding: "10px 14px",
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            gap: 10,
          }}
        >
          <div style={{ display: "flex", gap: 8 }}>
            <button
              type="button"
              className="btn"
              disabled
              title="Remote SSH environments are coming in a later release."
              style={{
                height: 22,
                padding: "0 10px",
                opacity: 0.5,
                cursor: "not-allowed",
              }}
            >
              + Add SSH environment
            </button>
            <button
              type="button"
              className="btn"
              disabled
              title="Docker / container environments are coming in a later release."
              style={{
                height: 22,
                padding: "0 10px",
                opacity: 0.5,
                cursor: "not-allowed",
              }}
            >
              + Add Container environment
            </button>
          </div>
          <button
            type="button"
            className="btn"
            onClick={() => void handleRefresh()}
            disabled={busy}
            style={{ height: 22, padding: "0 10px" }}
          >
            ↻ Refresh all
          </button>
        </div>
      </Card>
    </div>
  );
}

function EnvRow({
  env,
  busy,
  onToggle,
  onRemove,
  onRefresh,
  onResourceAdded,
  onError,
  insideTauri,
}: {
  env: EnvEntryIpc;
  busy: boolean;
  onToggle: (enabled: boolean) => void;
  onRemove: () => void;
  onRefresh: () => void;
  onResourceAdded: (snap: EnvSnapshotIpc) => void;
  onError: (msg: string) => void;
  insideTauri: boolean;
}): JSX.Element {
  const isLocal = env.id === "local";
  // Drawer is collapsed by default so a long resource list doesn't
  // dominate the Environments section once several envs are registered.
  const [open, setOpen] = useState(false);
  return (
    <div
      style={{
        padding: "10px 14px",
        borderBottom: "1px solid var(--line-soft)",
        fontFamily: "var(--mono)",
        fontSize: 12,
        color: "var(--txt)",
      }}
    >
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "auto auto 1fr auto auto auto",
          gap: 10,
          alignItems: "center",
        }}
      >
        <button
          type="button"
          onClick={() => setOpen((v) => !v)}
          aria-label={`${open ? "collapse" : "expand"} ${env.label}`}
          style={{
            appearance: "none",
            background: "transparent",
            border: 0,
            color: "var(--txt-soft)",
            fontSize: 12,
            cursor: "pointer",
            padding: 2,
            width: 16,
            lineHeight: 1,
          }}
        >
          {open ? "▼" : "▸"}
        </button>
        <input
          type="checkbox"
          checked={env.enabled}
          onChange={(e) => onToggle(e.target.checked)}
          disabled={busy || isLocal}
          aria-label={`enable ${env.label}`}
        />
        <div style={{ minWidth: 0 }}>
          <span style={{ marginRight: 8 }}>{env.label}</span>
          <span
            style={{
              fontSize: 10,
              color: "var(--txt-faint)",
              letterSpacing: "0.06em",
              textTransform: "uppercase",
            }}
          >
            {formatEnvKind(env.kind)}
          </span>
        </div>
        <EnvStatePill state={env.state} />
        <button
          type="button"
          className="btn"
          onClick={onRefresh}
          disabled={busy || !env.enabled}
          style={{ height: 22, padding: "0 10px" }}
          title="re-probe this environment"
        >
          ↻
        </button>
        {isLocal ? (
          <span />
        ) : (
          <button
            type="button"
            className="btn"
            onClick={onRemove}
            disabled={busy}
            style={{ height: 22, padding: "0 10px" }}
            aria-label={`remove ${env.label}`}
          >
            ✕
          </button>
        )}
      </div>
      {open ? (
        <>
          {env.resources.length > 0 ? (
            <div
              style={{
                marginTop: 6,
                display: "flex",
                flexDirection: "column",
                gap: 2,
              }}
            >
              {env.resources.map((res) => (
                <EnvResourceRow
                  key={res.id}
                  resource={res}
                  envId={env.id}
                  envKind={env.kind}
                  onEnvironmentChange={onResourceAdded}
                />
              ))}
            </div>
          ) : (
            <div
              style={{
                marginTop: 6,
                padding: "8px 14px",
                color: "var(--txt-faint)",
                fontSize: 11,
              }}
            >
              no resources probed. press ↻ above to refresh.
            </div>
          )}
          <AddResourceForm
            envId={env.id}
            onAdded={onResourceAdded}
            onError={onError}
            insideTauri={insideTauri}
          />
        </>
      ) : null}
    </div>
  );
}

function EnvStatePill({ state }: { state: EnvEntryIpc["state"] }): JSX.Element {
  const palette: Record<
    string,
    { color: string; bg: string; label: string }
  > = {
    reachable: {
      color: "var(--ok)",
      bg: "var(--ok-soft)",
      label: "reachable",
    },
    probing: {
      color: "var(--info)",
      bg: "var(--info-soft)",
      label: "probing…",
    },
    unreachable: {
      color: "var(--err)",
      bg: "var(--err-soft)",
      label: "unreachable",
    },
    disabled: {
      color: "var(--txt-faint)",
      bg: "transparent",
      label: "disabled",
    },
  };
  const skin = palette[state.state] ?? palette.disabled!;
  const tooltip = state.state === "unreachable" ? state.reason : undefined;
  return (
    <span
      title={tooltip}
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 6,
        fontSize: 10.5,
        color: skin.color,
        background: skin.bg,
        padding: "2px 8px",
        borderRadius: 999,
        fontFamily: "var(--mono)",
        whiteSpace: "nowrap",
      }}
    >
      <span
        style={{
          width: 6,
          height: 6,
          borderRadius: 6,
          background: skin.color,
        }}
      />
      {skin.label}
    </span>
  );
}

function EnvResourceRow({
  resource,
  envId,
  envKind,
  onEnvironmentChange,
}: {
  resource: EnvResourceIpc;
  envId: string;
  envKind: EnvKindIpc;
  onEnvironmentChange: (snap: EnvSnapshotIpc) => void;
}): JSX.Element {
  const stateLabel = resource.state.state;
  const stateReason =
    resource.state.state === "skipped" || resource.state.state === "probe_failed"
      ? resource.state.reason
      : null;
  const color =
    stateLabel === "found"
      ? "var(--ok)"
      : stateLabel === "timed_out" || stateLabel === "probe_failed"
        ? "var(--warn)"
        : "var(--txt-faint)";
  // HostDirect wizard appears only for WSL Found loopback HTTP endpoints —
  // exactly the case where direct reqwest routing can replace wrapped curl.
  const showWizard =
    envKind === "wsl_distro" &&
    resource.kind === "http_endpoint" &&
    resource.state.state === "found" &&
    resource.routeOrigin === "env_loopback";
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: showWizard ? "auto 1fr auto auto" : "auto 1fr auto",
        gap: 8,
        alignItems: "center",
        paddingLeft: 26,
        fontSize: 10.5,
        color: "var(--txt-faint)",
      }}
    >
      <span style={{ color: "var(--txt-soft)" }}>·</span>
      <span style={{ overflow: "hidden", textOverflow: "ellipsis" }}>
        <span style={{ color: "var(--txt)" }}>{resource.id}</span>
        <span style={{ color: "var(--txt-faint)", margin: "0 6px" }}>·</span>
        <span>{resource.kind.replace(/_/g, " ")}</span>
        {resource.baseUrl ? (
          <>
            <span style={{ color: "var(--txt-faint)", margin: "0 6px" }}>·</span>
            <span style={{ color: "var(--txt-dim)" }}>
              {resource.baseUrl.replace(/^https?:\/\//, "")}
            </span>
          </>
        ) : null}
        {resource.version ? (
          <>
            <span style={{ color: "var(--txt-faint)", margin: "0 6px" }}>·</span>
            <span>v{resource.version}</span>
          </>
        ) : null}
      </span>
      <span style={{ color, whiteSpace: "nowrap" }} title={stateReason ?? undefined}>
        {stateLabel.replace(/_/g, " ")}
      </span>
      {showWizard ? (
        <HostDirectWizard
          envId={envId}
          resourceId={resource.id}
          onComplete={onEnvironmentChange}
        />
      ) : null}
    </div>
  );
}

type AddResourceKind = "http_endpoint" | "binary" | "toolchain";

function buildDefinition(args: {
  kind: AddResourceKind;
  id: string;
  port: string;
  routePath: string;
  bin: string;
  versionArgs: string;
  versionRegex: string;
}): unknown {
  const id = args.id.trim();
  if (args.kind === "http_endpoint") {
    return {
      id,
      kind: "http_endpoint",
      advertisedCapabilities: ["openai_chat_completions"],
      probe: {
        kind: "http",
        ports: [Number(args.port)],
        routes: [
          {
            path: args.routePath,
            method: "get",
            flavor: "openai_chat",
            proves: ["openai_chat_completions"],
            modelsJsonpath: null,
            fingerprintJsonpaths: [],
          },
        ],
        timeoutMs: null,
      },
      overrideLowerScope: false,
    };
  }
  if (args.kind === "binary") {
    return {
      id,
      kind: "binary",
      advertisedCapabilities: ["cli_agent_print"],
      probe: {
        kind: "binary",
        bin: args.bin,
        versionArgs: args.versionArgs.split(/\s+/).filter(Boolean),
        versionRegex: args.versionRegex,
        extraSearchPaths: [],
        timeoutMs: null,
      },
      overrideLowerScope: false,
    };
  }
  return {
    id,
    kind: "toolchain",
    advertisedCapabilities: [],
    probe: {
      kind: "toolchain",
      bin: args.bin,
      versionArgs: args.versionArgs.split(/\s+/).filter(Boolean),
      versionRegex: args.versionRegex,
      timeoutMs: null,
    },
    overrideLowerScope: false,
  };
}

function AddResourceForm({
  envId,
  onAdded,
  onError,
  insideTauri,
}: {
  envId: string;
  onAdded: (snap: EnvSnapshotIpc) => void;
  onError: (msg: string) => void;
  insideTauri: boolean;
}): JSX.Element {
  const [kind, setKind] = useState<AddResourceKind>("http_endpoint");
  const [id, setId] = useState("");
  const [port, setPort] = useState("8080");
  const [routePath, setRoutePath] = useState("/v1/models");
  const [bin, setBin] = useState("");
  const [versionArgs, setVersionArgs] = useState("--version");
  const [versionRegex, setVersionRegex] = useState(
    String.raw`(\d+\.\d+\.\d+)`,
  );
  const [busy, setBusy] = useState(false);

  const trimmedId = id.trim();
  const disabled = !trimmedId || busy || !insideTauri;

  const handleAdd = async () => {
    if (disabled) return;
    setBusy(true);
    try {
      const definition = buildDefinition({
        kind,
        id: trimmedId,
        port,
        routePath,
        bin,
        versionArgs,
        versionRegex,
      });
      const snap = await addEnvironmentResource({ envId, definition });
      onAdded(snap);
      setId("");
    } catch (e) {
      onError(`add resource: ${String(e)}`);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div
      style={{
        marginTop: 10,
        paddingTop: 10,
        borderTop: "1px solid var(--line-soft)",
      }}
    >
      <Field label="kind">
        <SegRow
          value={kind}
          options={["http_endpoint", "binary", "toolchain"]}
          onChange={(v) => setKind(v as AddResourceKind)}
        />
      </Field>
      <Field label="id">
        <TextInput
          value={id}
          onChange={setId}
          placeholder="my-endpoint"
          disabled={busy}
        />
      </Field>
      {kind === "http_endpoint" ? (
        <>
          <Field label="port">
            <TextInput
              value={port}
              onChange={setPort}
              placeholder="8080"
              disabled={busy}
            />
          </Field>
          <Field label="probe route">
            <TextInput
              value={routePath}
              onChange={setRoutePath}
              placeholder="/v1/models"
              disabled={busy}
            />
          </Field>
        </>
      ) : (
        <>
          <Field label="binary">
            <TextInput
              value={bin}
              onChange={setBin}
              placeholder={kind === "binary" ? "claude" : "node"}
              disabled={busy}
            />
          </Field>
          <Field label="version flag">
            <TextInput
              value={versionArgs}
              onChange={setVersionArgs}
              placeholder="--version"
              disabled={busy}
            />
          </Field>
          <Field label="version regex">
            <TextInput
              value={versionRegex}
              onChange={setVersionRegex}
              placeholder={String.raw`(\d+\.\d+\.\d+)`}
              disabled={busy}
            />
          </Field>
        </>
      )}
      <div style={{ padding: "8px 16px 6px" }}>
        <button
          type="button"
          className="btn primary"
          disabled={disabled}
          onClick={() => void handleAdd()}
          style={{ height: 26 }}
        >
          add resource
        </button>
      </div>
    </div>
  );
}

type HostDirectPhase = "idle" | "testing" | "result" | "enabling";

function HostDirectWizard({
  envId,
  resourceId,
  onComplete,
}: {
  envId: string;
  resourceId: string;
  onComplete: (snap: EnvSnapshotIpc) => void;
}): JSX.Element {
  const [phase, setPhase] = useState<HostDirectPhase>("idle");
  const [result, setResult] = useState<HostDirectTestResultIpc | null>(null);
  const [err, setErr] = useState<string | null>(null);

  const runTest = async (): Promise<void> => {
    setPhase("testing");
    setErr(null);
    try {
      const out = await testHostDirect(envId, resourceId);
      setResult(out);
      setPhase("result");
    } catch (e) {
      setErr(String(e));
      setPhase("idle");
    }
  };

  const enable = async (): Promise<void> => {
    if (!result || !result.success || !result.stableFingerprint) return;
    setPhase("enabling");
    setErr(null);
    try {
      const verification: HostDirectVerificationIpc = {
        verifiedAt: new Date().toISOString(),
        method: "user_asserted_no_verification",
        hostUrl: result.hostUrl,
        probeRoutePath: result.probeRoutePath,
        stableFingerprint: result.stableFingerprint,
        // Backend recomputes via the probe spec's fingerprint_jsonpaths;
        // we don't have them on the wire here so pass an empty list.
        recomputeJsonpaths: [],
      };
      const snap = await enableHostDirect(envId, resourceId, verification);
      onComplete(snap);
      setPhase("idle");
      setResult(null);
    } catch (e) {
      setErr(String(e));
      setPhase("result");
    }
  };

  if (phase === "testing") {
    return (
      <span
        style={{
          color: "var(--info)",
          fontFamily: "var(--mono)",
          fontSize: 10.5,
          whiteSpace: "nowrap",
        }}
      >
        testing…
      </span>
    );
  }

  if (phase === "enabling") {
    return (
      <span
        style={{
          color: "var(--info)",
          fontFamily: "var(--mono)",
          fontSize: 10.5,
          whiteSpace: "nowrap",
        }}
      >
        enabling…
      </span>
    );
  }

  if (phase === "result" && result) {
    if (result.success && result.stableFingerprint) {
      return (
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            gap: 4,
            alignItems: "flex-end",
            fontFamily: "var(--mono)",
            fontSize: 10.5,
          }}
        >
          <span style={{ color: "var(--ok)", whiteSpace: "nowrap" }}>
            ✓ direct probe ok ·{" "}
            <span style={{ color: "var(--txt-dim)" }}>
              {result.stableFingerprint.slice(0, 12)}
            </span>
          </span>
          {err ? (
            <span style={{ color: "var(--warn)", whiteSpace: "nowrap" }}>
              {err}
            </span>
          ) : null}
          <button
            type="button"
            className="btn"
            onClick={() => void enable()}
            style={{ height: 22, padding: "0 10px" }}
          >
            enable host-direct routing
          </button>
        </div>
      );
    }
    return (
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          gap: 4,
          alignItems: "flex-end",
          fontFamily: "var(--mono)",
          fontSize: 10.5,
        }}
      >
        <span
          style={{ color: "var(--warn)", whiteSpace: "nowrap" }}
          title={result.error ?? undefined}
        >
          ✗ {result.error ?? "probe failed"}
        </span>
        <button
          type="button"
          className="btn"
          onClick={() => void runTest()}
          style={{ height: 22, padding: "0 10px" }}
        >
          retry
        </button>
      </div>
    );
  }

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        gap: 4,
        alignItems: "flex-end",
      }}
    >
      <button
        type="button"
        className="btn"
        onClick={() => void runTest()}
        style={{ height: 22, padding: "0 10px" }}
        title="probe the resource directly from the host process"
      >
        test direct access
      </button>
      {err ? (
        <span
          style={{
            color: "var(--warn)",
            fontFamily: "var(--mono)",
            fontSize: 10.5,
            whiteSpace: "nowrap",
          }}
          title={err}
        >
          {err.length > 60 ? `${err.slice(0, 57)}…` : err}
        </span>
      ) : null}
    </div>
  );
}

function formatEnvKind(kind: EnvEntryIpc["kind"]): string {
  switch (kind) {
    case "local":
      return "host";
    case "wsl_distro":
      return "wsl";
    case "ssh":
      return "ssh";
    case "container":
      return "container";
    default:
      return kind;
  }
}

// ─── Workspaces ──────────────────────────────────────────────────

function WorkspacesSection({
  workspaces,
  onReload,
  onError,
  insideTauri,
}: {
  workspaces: Workspace[];
  onReload: () => Promise<void>;
  onError: (msg: string) => void;
  insideTauri: boolean;
}): JSX.Element {
  const [name, setName] = useState("");
  const [path, setPath] = useState("");
  const [busy, setBusy] = useState(false);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [draftName, setDraftName] = useState("");

  const handleAdd = async () => {
    if (!insideTauri || !name.trim() || !path.trim()) return;
    setBusy(true);
    try {
      await addWorkspace(name.trim(), path.trim());
      setName("");
      setPath("");
      await onReload();
    } catch (e) {
      onError(`add workspace: ${String(e)}`);
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
    } catch (e) {
      onError(`remove workspace: ${String(e)}`);
    } finally {
      setBusy(false);
    }
  };

  const startRename = (workspace: Workspace) => {
    setEditingId(workspace.id);
    setDraftName(workspace.name);
  };

  const cancelRename = () => {
    setEditingId(null);
    setDraftName("");
  };

  const commitRename = async () => {
    if (!insideTauri || !editingId) return;
    const next = draftName.trim();
    if (!next) {
      onError("rename workspace: name must be non-empty");
      return;
    }
    setBusy(true);
    try {
      await renameWorkspace(editingId, next);
      await onReload();
      setEditingId(null);
      setDraftName("");
    } catch (e) {
      onError(`rename workspace: ${String(e)}`);
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
            placeholder="/home/user/code/my-project"
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
          workspaces.map((workspace) => {
            const editing = editingId === workspace.id;
            return (
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
                  {editing ? (
                    <input
                      value={draftName}
                      onChange={(event) => setDraftName(event.target.value)}
                      onKeyDown={(event) => {
                        if (event.key === "Enter") void commitRename();
                        if (event.key === "Escape") cancelRename();
                      }}
                      autoFocus
                      disabled={busy}
                      aria-label={`rename workspace ${workspace.name}`}
                      style={{
                        width: "100%",
                        background: "var(--bg-input)",
                        border: "1px solid var(--accent)",
                        color: "var(--txt)",
                        fontFamily: "var(--mono)",
                        fontSize: 12,
                        padding: "3px 6px",
                        outline: "none",
                        borderRadius: 2,
                      }}
                    />
                  ) : (
                    <div>{workspace.name}</div>
                  )}
                  <div
                    style={{
                      fontSize: 10,
                      color: "var(--txt-faint)",
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      marginTop: editing ? 4 : 0,
                    }}
                    title={workspace.path}
                  >
                    {workspace.path}
                  </div>
                </div>
                <div style={{ display: "flex", gap: 6 }}>
                  {editing ? (
                    <>
                      <button
                        type="button"
                        className="btn primary"
                        onClick={() => void commitRename()}
                        disabled={busy || !draftName.trim()}
                        style={{ height: 22, padding: "0 10px" }}
                      >
                        save
                      </button>
                      <button
                        type="button"
                        className="btn"
                        onClick={cancelRename}
                        disabled={busy}
                        style={{ height: 22, padding: "0 10px" }}
                      >
                        cancel
                      </button>
                    </>
                  ) : (
                    <>
                      <button
                        type="button"
                        className="btn"
                        onClick={() => startRename(workspace)}
                        disabled={busy}
                        style={{ height: 22, padding: "0 10px" }}
                      >
                        rename
                      </button>
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
                    </>
                  )}
                </div>
              </div>
            );
          })
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

/**
 * Walk the env snapshot and lift every found HTTP resource into a flat
 * list the Models view can render. Non-reachable envs and non-http-endpoint
 * resources are excluded; loopback routes surface with `hostReachable: false`
 * so the UI can suppress the save button + show the warning chip.
 */
function collectDetectedRoutes(env: EnvSnapshotIpc | null): DetectedRoute[] {
  if (!env) return [];
  const out: DetectedRoute[] = [];
  for (const e of env.envs) {
    if (e.state.state !== "reachable") continue;
    for (const r of e.resources) {
      if (r.kind !== "http_endpoint") continue;
      if (r.state.state !== "found") continue;
      if (!r.baseUrl) continue;
      out.push({
        envId: e.id,
        envLabel: e.label,
        resourceId: r.id,
        baseUrl: r.baseUrl,
        routeOrigin: r.routeOrigin ?? "unknown",
        // Only loopback routes inside a non-local env hide behind a
        // dispatcher. Everything else (host_direct, forwarded_tunnel,
        // container_bridge, or env_loopback on the local env itself) is
        // already reachable from the GUI process.
        hostReachable:
          e.id === "local" || r.routeOrigin !== "env_loopback",
      });
    }
  }
  return out;
}

interface DetectedRoute {
  envId: string;
  envLabel: string;
  resourceId: string;
  baseUrl: string;
  /**
   * Routes whose `routeOrigin` is anything other than `env_loopback` are
   * directly reachable from the host process — the `Save` button writes
   * them straight into the model endpoints list. Loopback-only routes
   * surface a warning chip instead (they're reachable from the env-side
   * dispatcher but not from the GUI process).
   */
  hostReachable: boolean;
  routeOrigin: string;
}

function ModelsSection({
  settings,
  secrets,
  environment,
  onPatch,
}: {
  settings: SettingsShape | null;
  secrets: SecretMeta[];
  environment: EnvSnapshotIpc | null;
  onPatch: (patch: Partial<SettingsShape>) => Promise<void>;
}): JSX.Element {
  const [name, setName] = useState("");
  const [baseUrl, setBaseUrl] = useState("");
  const [apiKeySecret, setApiKeySecret] = useState("");
  const [busy, setBusy] = useState(false);

  if (!settings) return <Loading />;

  const endpoints = settings.modelEndpoints;

  const handleAdd = async () => {
    if (!name.trim() || !baseUrl.trim() || busy) return;
    setBusy(true);
    try {
      const newId = `ep-${Date.now().toString(36)}`;
      const next = [
        ...endpoints,
        {
          id: newId,
          name: name.trim(),
          baseUrl: baseUrl.trim(),
          apiKeySecret: apiKeySecret.trim() || null,
        },
      ];
      await onPatch({ modelEndpoints: next });
      setName("");
      setBaseUrl("");
      setApiKeySecret("");
    } finally {
      setBusy(false);
    }
  };

  const handleRemove = async (id: string) => {
    if (busy) return;
    setBusy(true);
    try {
      await onPatch({
        modelEndpoints: endpoints.filter((e) => e.id !== id),
      });
    } finally {
      setBusy(false);
    }
  };

  const registeredUrls = new Set(endpoints.map((e) => e.baseUrl));
  const detected = collectDetectedRoutes(environment);

  const handleSaveDiscovered = async (kind: string, baseUrl: string) => {
    if (busy) return;
    setBusy(true);
    try {
      const next = [
        ...endpoints,
        {
          id: `ep-${Date.now().toString(36)}`,
          name: kind,
          baseUrl,
          apiKeySecret: null,
        },
      ];
      await onPatch({ modelEndpoints: next });
    } finally {
      setBusy(false);
    }
  };

  return (
    <div>
      {detected.length > 0 ? (
        <>
          <Heading
            text="Detected on this host"
            sub="probed at boot · one row per resolved http endpoint"
          />
          <Card>
            {detected.map((route) => {
              const saved = registeredUrls.has(route.baseUrl);
              return (
                <div
                  key={`${route.envId}::${route.resourceId}::${route.baseUrl}`}
                  style={{
                    display: "grid",
                    gridTemplateColumns: "1fr 1fr 110px",
                    gap: 10,
                    alignItems: "center",
                    padding: "10px 14px",
                    borderBottom: "1px solid var(--line-soft)",
                    fontFamily: "var(--mono)",
                    fontSize: 12,
                    color: "var(--txt)",
                  }}
                >
                  <span>
                    {route.resourceId}
                    <span style={{ color: "var(--txt-faint)", marginLeft: 8 }}>
                      ({route.envLabel})
                    </span>
                  </span>
                  <span style={{ color: "var(--txt-dim)" }}>{route.baseUrl}</span>
                  {route.hostReachable ? (
                    saved ? (
                      <span style={{ color: "var(--accent)", fontSize: 11 }}>
                        ✓ saved
                      </span>
                    ) : (
                      <button
                        type="button"
                        className="btn"
                        onClick={() =>
                          void handleSaveDiscovered(route.resourceId, route.baseUrl)
                        }
                        disabled={busy}
                        style={{ height: 22, padding: "0 10px" }}
                      >
                        save endpoint
                      </button>
                    )
                  ) : (
                    <span
                      title={
                        `This resource is reachable from ${route.envLabel} but is bound ` +
                        `to a loopback interface (${route.routeOrigin}), so the GUI process ` +
                        `cannot reach it directly. Workflows targeting this env still work ` +
                        `via the env-side dispatcher.\n\n` +
                        `To expose it to the host, bind the service to 0.0.0.0 or set ` +
                        `WSL networkingMode = mirrored in your .wslconfig.`
                      }
                      style={{ color: "var(--warn)", fontSize: 11 }}
                    >
                      ⚠ via {route.envLabel}
                    </span>
                  )}
                </div>
              );
            })}
          </Card>
        </>
      ) : null}

      <Heading
        text="Model endpoints"
        sub="OpenAI-compatible URLs the llm node can target."
      />
      <Card>
        <Field label="name">
          <TextInput
            value={name}
            onChange={setName}
            placeholder="openai-prod"
          />
        </Field>
        <Field label="base url">
          <TextInput
            value={baseUrl}
            onChange={setBaseUrl}
            placeholder="https://api.openai.com/v1"
          />
        </Field>
        <Field label="api key secret" hint="name of a stored secret (optional)">
          <SecretPicker
            value={apiKeySecret}
            secrets={secrets}
            onChange={setApiKeySecret}
          />
        </Field>
        <div style={{ padding: "8px 16px 14px" }}>
          <button
            type="button"
            className="btn primary"
            disabled={!name.trim() || !baseUrl.trim() || busy}
            onClick={() => void handleAdd()}
            style={{ height: 28 }}
          >
            add endpoint
          </button>
        </div>
      </Card>

      <SectionTitle
        label="registered"
        count={`${endpoints.length} endpoint${endpoints.length === 1 ? "" : "s"}`}
      />
      <div
        style={{
          marginTop: 12,
          background: "var(--bg-panel)",
          border: "1px solid var(--line)",
          borderRadius: 3,
        }}
      >
        {endpoints.length === 0 ? (
          <div
            style={{
              padding: "16px",
              color: "var(--txt-faint)",
              fontSize: 11,
              fontFamily: "var(--mono)",
            }}
          >
            no endpoints registered.
          </div>
        ) : (
          endpoints.map((endpoint) => (
            <div
              key={endpoint.id}
              style={{
                display: "grid",
                gridTemplateColumns: "1fr 1fr 1fr 80px",
                gap: 10,
                alignItems: "center",
                padding: "10px 14px",
                borderBottom: "1px solid var(--line-soft)",
                fontFamily: "var(--mono)",
                fontSize: 12,
                color: "var(--txt)",
              }}
            >
              <span>{endpoint.name}</span>
              <span style={{ color: "var(--txt-dim)" }}>{endpoint.baseUrl}</span>
              <span style={{ color: "var(--txt-dim)" }}>
                {endpoint.apiKeySecret ?? "(no secret)"}
              </span>
              <button
                type="button"
                className="btn"
                onClick={() => void handleRemove(endpoint.id)}
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

function SecretPicker({
  value,
  secrets,
  onChange,
}: {
  value: string;
  secrets: SecretMeta[];
  onChange: (v: string) => void;
}): JSX.Element {
  return (
    <select
      value={value}
      onChange={(event) => onChange(event.target.value)}
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
      <option value="">(none)</option>
      {secrets.map((s) => (
        <option key={s.name} value={s.name}>
          {s.name}
        </option>
      ))}
    </select>
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
