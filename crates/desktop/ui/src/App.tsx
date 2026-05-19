import { useCallback, useEffect, useState } from "react";

import { Editor } from "./routes/Editor";
import { History } from "./routes/History";
import { Home } from "./routes/Home";
import { Settings } from "./routes/Settings";
import { bindNavigate, type Route } from "./lib/router";

// All four v1.1 routes wired. Theme + routing state live at the
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
      return (
        <History
          theme={theme}
          onThemeToggle={toggleTheme}
          onNavigate={setRoute}
        />
      );
    case "settings":
      return (
        <Settings
          theme={theme}
          onThemeToggle={toggleTheme}
          onThemeChange={setTheme}
          onNavigate={setRoute}
        />
      );
    case "home":
    default:
      return <Home theme={theme} onThemeToggle={toggleTheme} />;
  }
}

export default App;
