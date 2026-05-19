import { useCallback, useEffect, useState } from "react";

import { Editor } from "./routes/Editor";
import { Home } from "./routes/Home";
import { bindNavigate, type Route } from "./lib/router";

// Phase 1.5a: two wired routes — Home and Editor. History +
// Settings land in 1.7 / 1.8. Theme + routing state live at the
// App root so navigation between routes doesn't flicker the
// theme cascade.
function App() {
  const [theme, setTheme] = useState<"dark" | "light">("dark");
  const [route, setRoute] = useState<Route>({ kind: "home" });

  useEffect(() => {
    document.documentElement.dataset["theme"] = theme;
  }, [theme]);

  // Publish the setter to the global navigate() helper so non-React
  // code (run dialog, command palette, etc.) can navigate too.
  useEffect(() => bindNavigate(setRoute), []);

  const toggleTheme = useCallback(() => {
    setTheme((t) => (t === "dark" ? "light" : "dark"));
  }, []);

  switch (route.kind) {
    case "editor":
      return (
        <Editor
          workflowId={route.workflowId}
          theme={theme}
          onThemeToggle={toggleTheme}
          onNavigate={setRoute}
        />
      );
    case "history":
    case "settings":
      // Phases 1.7 / 1.8 — fall through to Home for now.
      return <Home theme={theme} onThemeToggle={toggleTheme} />;
    case "home":
    default:
      return <Home theme={theme} onThemeToggle={toggleTheme} />;
  }
}

export default App;
