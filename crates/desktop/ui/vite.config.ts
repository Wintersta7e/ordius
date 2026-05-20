import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Tauri-flavoured Vite config.
//   - Port pinned to 1420 (matches devUrl in tauri.conf.json).
//   - `clearScreen: false` keeps Rust compile output visible.
//   - `envPrefix` exposes TAURI_ENV_* env vars to client code.
//   - Sourcemaps when Tauri is in debug, esbuild minify in release.
export default defineConfig({
  plugins: [react()],
  clearScreen: false,
  server: {
    port: 1420,
    strictPort: true,
    host: false,
    hmr: {
      protocol: "ws",
      host: "localhost",
      port: 1421,
    },
    watch: {
      // Tauri's Rust side has its own watcher; don't double-watch.
      ignored: ["**/src-tauri/**"],
      // /mnt/c lives on the Windows side under WSL2 — inotify doesn't
      // see writes there, so polling is required for HMR.
      usePolling: true,
      interval: 400,
    },
  },
  // `envPrefix` entries are literal string prefixes, NOT globs — a
  // trailing `*` would match `TAURI_ENV_*` verbatim and miss
  // `TAURI_ENV_PLATFORM` / `TAURI_ENV_DEBUG`.
  envPrefix: ["VITE_", "TAURI_ENV_"],
  build: {
    target:
      process.env["TAURI_ENV_PLATFORM"] === "windows"
        ? "chrome105"
        : "safari13",
    minify: process.env["TAURI_ENV_DEBUG"] === "true" ? false : "esbuild",
    sourcemap: process.env["TAURI_ENV_DEBUG"] === "true",
  },
});
