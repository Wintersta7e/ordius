import React from "react";
import ReactDOM from "react-dom/client";
// Bundle the two display + body fonts so the app never has to wait
// on a Google Fonts CDN at runtime (and works offline).
import "@fontsource/jetbrains-mono/300.css";
import "@fontsource/jetbrains-mono/400.css";
import "@fontsource/jetbrains-mono/500.css";
import "@fontsource/jetbrains-mono/600.css";
import "@fontsource/jetbrains-mono/700.css";
import "@fontsource/space-grotesk/400.css";
import "@fontsource/space-grotesk/500.css";
import "@fontsource/space-grotesk/600.css";
import "@fontsource/space-grotesk/700.css";
import "./styles/tokens.css";
import App from "./App";

const root = document.getElementById("root");
if (!root) {
  throw new Error("missing #root element");
}
ReactDOM.createRoot(root).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
