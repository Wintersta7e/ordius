// Small formatting helpers. Real-time clock (no NOW constant like
// the prototype — we want clocks to advance live).

const SECOND = 1000;
const MINUTE = 60_000;
const HOUR = 3_600_000;
const DAY = 86_400_000;

/** "5m ago" / "3h ago" / "2d ago" / "just now". */
export function fmtAgo(epochMs: number, now: number = Date.now()): string {
  const delta = Math.max(0, now - epochMs);
  if (delta < MINUTE) return "just now";
  if (delta < HOUR) return `${Math.floor(delta / MINUTE)}m ago`;
  if (delta < DAY) return `${Math.floor(delta / HOUR)}h ago`;
  return `${Math.floor(delta / DAY)}d ago`;
}

/** "350ms" / "1.20s" / "1m 5s" / "1.5h". */
export function fmtDuration(ms: number | null | undefined): string {
  if (ms == null) return "—";
  if (ms < SECOND) return `${ms}ms`;
  if (ms < 60 * SECOND) return `${(ms / SECOND).toFixed(2)}s`;
  if (ms < HOUR)
    return `${Math.floor(ms / MINUTE)}m ${Math.floor((ms % MINUTE) / SECOND)}s`;
  return `${(ms / HOUR).toFixed(1)}h`;
}

/** "184 MB" / "1.2 GB" — base-1024, one decimal above MB. */
export function fmtBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
  return `${(bytes / 1024 / 1024 / 1024).toFixed(2)} GB`;
}
