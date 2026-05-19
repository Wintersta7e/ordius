import { useEffect, useState } from "react";

import { Home } from "./routes/Home";

// Phase 1.4: Home is the only wired route. Theme state lives at
// the App root so Home's TopBar toggle can flip the data-theme
// attribute and every CSS-token cascade follows.
function App() {
  const [theme, setTheme] = useState<"dark" | "light">("dark");

  useEffect(() => {
    document.documentElement.dataset["theme"] = theme;
  }, [theme]);

  const toggleTheme = () => {
    setTheme((t) => (t === "dark" ? "light" : "dark"));
  };

  return <Home theme={theme} onThemeToggle={toggleTheme} />;
}

export default App;
