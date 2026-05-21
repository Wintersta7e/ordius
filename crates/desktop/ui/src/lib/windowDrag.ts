// Window-level pointer-drag attach used by both palette → spawn
// and output-pin → input-pin edge flows. Attach is synchronous so
// a rapid press-release inside the same React event handler still
// reaches our pointerup listener.

export interface WindowDragHandlers {
  onMove(event: PointerEvent): void;
  onUp(event: PointerEvent): void;
  onKey?(event: KeyboardEvent): void;
}

export function attachWindowDrag(handlers: WindowDragHandlers): () => void {
  const { onMove, onUp, onKey } = handlers;
  window.addEventListener("pointermove", onMove, { passive: true });
  window.addEventListener("pointerup", onUp, { passive: true });
  if (onKey) window.addEventListener("keydown", onKey);
  return () => {
    window.removeEventListener("pointermove", onMove);
    window.removeEventListener("pointerup", onUp);
    if (onKey) window.removeEventListener("keydown", onKey);
  };
}
