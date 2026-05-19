import { useState } from "react";

// Phase 1 placeholder. Surfaces the design tokens against a dark
// IDE chrome so the cascade is verifiably wired before any of the
// real screens land. Theme toggle exists only so we can smoke-test
// both palettes from day one.
function App() {
  const [theme, setTheme] = useState<"dark" | "light">("dark");

  const toggleTheme = () => {
    const next = theme === "dark" ? "light" : "dark";
    setTheme(next);
    document.documentElement.dataset["theme"] = next;
  };

  return (
    <div
      style={{
        height: "100vh",
        display: "grid",
        gridTemplateRows: "44px 1fr",
        fontFamily: "var(--mono)",
      }}
    >
      <header
        style={{
          background: "var(--bg-elevated)",
          borderBottom: "1px solid var(--line)",
          display: "flex",
          alignItems: "center",
          padding: "0 16px",
          gap: 12,
        }}
      >
        <span
          style={{
            fontFamily: "var(--display)",
            fontWeight: 600,
            fontSize: 14,
            letterSpacing: "-0.005em",
          }}
        >
          ◆ ordius
        </span>
        <span style={{ color: "var(--txt-faint)", fontSize: 11 }}>
          v0.1.0 · scaffold
        </span>
        <span style={{ flex: 1 }} />
        <button className="btn" type="button" onClick={toggleTheme}>
          theme: {theme}
        </button>
      </header>

      <main
        style={{
          padding: 48,
          background: "var(--bg)",
          display: "grid",
          gap: 24,
          alignContent: "start",
        }}
      >
        <div>
          <h1
            style={{
              fontFamily: "var(--display)",
              fontWeight: 600,
              fontSize: 32,
              letterSpacing: "-0.015em",
              margin: 0,
              color: "var(--txt)",
            }}
          >
            Ordius — v1.1 scaffold
          </h1>
          <p
            style={{
              color: "var(--txt-dim)",
              marginTop: 8,
              fontSize: 13,
              lineHeight: 1.55,
            }}
          >
            Tauri 2 host + Vite/React/TypeScript shell. Design tokens loaded
            from <code>tokens.css</code>; fonts bundled via{" "}
            <code>@fontsource</code>; theme cascade toggles between dark IDE and
            engineering-paper light.
          </p>
        </div>

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(5, 1fr)",
            gap: 10,
            maxWidth: 760,
          }}
        >
          {(
            [
              ["execution", 70],
              ["llm", 305],
              ["data", 200],
              ["control", 25],
              ["integration", 152],
            ] as const
          ).map(([name, hue]) => (
            <div
              key={name}
              style={{
                background: "var(--bg-panel)",
                border: "1px solid var(--line)",
                padding: 14,
                borderRadius: 3,
                textAlign: "center",
              }}
            >
              <div
                style={{
                  width: 22,
                  height: 22,
                  borderRadius: 3,
                  margin: "0 auto 8px",
                  background: `oklch(0.74 0.19 ${hue})`,
                }}
              />
              <div
                style={{
                  fontSize: 10,
                  color: "var(--txt-faint)",
                  letterSpacing: "0.12em",
                  textTransform: "uppercase",
                }}
              >
                {name}
              </div>
            </div>
          ))}
        </div>

        <pre
          style={{
            fontFamily: "var(--mono)",
            fontSize: 11.5,
            color: "var(--txt-dim)",
            background: "var(--bg-canvas)",
            border: "1px solid var(--line)",
            borderRadius: 3,
            padding: "12px 14px",
            margin: 0,
            maxWidth: 760,
          }}
        >
{`┌ NEXT ─────────────────── phase 1.2 ─┐
│ wire 14 Tauri commands + run channel │
│ TS types mirror engine snake/camel   │
│ home / editor / history / settings   │
└──────────────────────────────────────┘`}
        </pre>
      </main>
    </div>
  );
}

export default App;
