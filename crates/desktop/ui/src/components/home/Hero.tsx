// Top-of-Home hero — workspace badge + headline + counts + CTA buttons.
//
// HeroParticles deferred to v1.1.10 polish.

import type { JSX } from "react";

import { Ic } from "../icons";

interface Props {
  /** Active workspace name shown in the eyebrow. */
  workspaceName: string;
  /** Total saved workflows. */
  workflowCount: number;
  /** Total persisted run rows. */
  runCount: number;
  /** Number of runs currently in `status='running'`. */
  runningCount: number;
  /** Click → workflows import flow (Phase 1.9). */
  onImport: () => void;
  /** Click → new-workflow editor route (Phase 1.5). */
  onNew: () => void;
}

export function Hero({
  workspaceName,
  workflowCount,
  runCount,
  runningCount,
  onImport,
  onNew,
}: Props): JSX.Element {
  return (
    <div style={{ position: "relative", padding: "4px 0" }}>
      <div
        style={{
          position: "relative",
          zIndex: 2,
          display: "grid",
          gridTemplateColumns: "1fr auto",
          gap: 24,
          alignItems: "center",
          padding: "12px 0",
        }}
      >
        <div>
          <div
            style={{
              fontSize: 10,
              color: "var(--accent)",
              fontWeight: 700,
              letterSpacing: "0.22em",
              textTransform: "uppercase",
            }}
          >
            workspace · {workspaceName}
          </div>
          <h1
            style={{
              margin: "6px 0 6px",
              fontFamily: "var(--display)",
              fontWeight: 600,
              fontSize: 36,
              color: "var(--txt)",
              letterSpacing: "-0.015em",
            }}
          >
            What are we running today?
          </h1>
          <p
            style={{
              margin: 0,
              color: "var(--txt-dim)",
              fontSize: 14,
              maxWidth: 560,
              lineHeight: 1.5,
            }}
          >
            <Num>{workflowCount}</Num> saved workflows · <Num>{runCount}</Num>{" "}
            total runs
            {runningCount > 0 ? (
              <>
                {" "}
                ·{" "}
                <span style={{ color: "var(--info)" }} className="num">
                  {runningCount}
                </span>{" "}
                running now
              </>
            ) : null}
          </p>
        </div>
        <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
          <button type="button" className="btn" onClick={onImport}>
            {Ic["search"]?.({ size: 14 })} import…
          </button>
          <button
            type="button"
            className="btn primary"
            onClick={onNew}
            style={{ height: 36, padding: "0 18px", fontSize: 12.5 }}
          >
            <span style={{ fontSize: 16, lineHeight: 1 }}>+</span> new workflow
          </button>
        </div>
      </div>
    </div>
  );
}

function Num({ children }: { children: number }): JSX.Element {
  return (
    <span
      className="num"
      style={{ color: "var(--txt)", fontFamily: "var(--mono)" }}
    >
      {children}
    </span>
  );
}
