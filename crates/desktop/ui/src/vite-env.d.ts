/// <reference types="vite/client" />

// CSS side-effect imports — Vite resolves them, but the TS server
// needs a shape declaration to stop complaining about ".css"
// imports without a default export.
declare module "*.css";
declare module "@fontsource/*";
