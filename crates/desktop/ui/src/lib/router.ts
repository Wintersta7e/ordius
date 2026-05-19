// Tiny router. Tauri only has one window so a routing library is
// overkill — we just need a Route discriminated union + a
// state machine that App.tsx owns.
//
// The published `navigate` function is bound at App startup so
// non-React code (e.g. the run dialog's "open editor" button) can
// trigger navigation too.

/** Routes wired in the GUI. */
export type Route =
  | { kind: "home" }
  | { kind: "editor"; workflowId?: string | undefined }
  | { kind: "history" }
  | { kind: "settings" };

/** Single global `navigate` setter — bound by App on first render
 * via `bindNavigate`. Non-React callers (run dialog, etc.) import
 * `navigate` and call it directly; React routes get `navigate` as
 * a prop. */
let bound: ((next: Route) => void) | null = null;

/** Called from App.tsx during its mount effect to register the
 * single setter. Throws if called twice — would indicate two App
 * instances and a wiring bug. */
export function bindNavigate(setter: (next: Route) => void): () => void {
  bound = setter;
  return () => {
    bound = null;
  };
}

/** Trigger a route change from anywhere in the tree. */
export function navigate(next: Route): void {
  if (bound) {
    bound(next);
  } else if (typeof console !== "undefined") {
    console.warn("navigate() before bindNavigate", next);
  }
}
