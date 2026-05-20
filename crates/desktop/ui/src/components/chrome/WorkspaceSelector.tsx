import { useEffect, useRef, useState } from "react";
import type { JSX } from "react";

import type { Workspace } from "../../engine/types";
import { addWorkspace as addWorkspaceCmd } from "../../engine/commands";

interface Props {
  workspaces: Workspace[];
  workspaceId: string | null;
  onChange: (id: string | null) => void;
  /** Called after a successful inline create so the parent can refresh. */
  onWorkspacesChanged?: () => void;
  /** Optional navigator hook — open Settings → Workspaces. */
  onOpenManage?: () => void;
}

export function WorkspaceSelector({
  workspaces,
  workspaceId,
  onChange,
  onWorkspacesChanged,
  onOpenManage,
}: Props): JSX.Element {
  const [open, setOpen] = useState(false);
  const [adding, setAdding] = useState(false);
  const [newName, setNewName] = useState("");
  const [newPath, setNewPath] = useState("");
  const [err, setErr] = useState<string | null>(null);
  const rootRef = useRef<HTMLDivElement>(null);

  const active = workspaceId
    ? workspaces.find((w) => w.id === workspaceId) ?? null
    : null;
  const label = active ? active.name : "(home)";

  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (rootRef.current && !rootRef.current.contains(e.target as Node)) {
        setOpen(false);
        setAdding(false);
        setErr(null);
      }
    };
    document.addEventListener("mousedown", onDown);
    return () => document.removeEventListener("mousedown", onDown);
  }, [open]);

  async function handleAdd() {
    if (!newName.trim() || !newPath.trim()) {
      setErr("name and path are required");
      return;
    }
    try {
      const ws = await addWorkspaceCmd(newName.trim(), newPath.trim());
      onChange(ws.id);
      setAdding(false);
      setNewName("");
      setNewPath("");
      setErr(null);
      setOpen(false);
      onWorkspacesChanged?.();
    } catch (e: unknown) {
      setErr(String(e));
    }
  }

  return (
    <div ref={rootRef} style={{ position: "relative" }}>
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        title={active?.path ?? "no workspace bound — runs use the engine home"}
        style={{
          display: "inline-flex",
          alignItems: "center",
          gap: 4,
          height: 24,
          padding: "0 8px",
          fontFamily: "var(--mono)",
          fontSize: 11,
          color: "var(--txt)",
          background: open ? "var(--bg-panel)" : "var(--bg)",
          border: "1px solid var(--line)",
          borderRadius: 4,
          cursor: "pointer",
        }}
      >
        <span style={{ color: "var(--accent)", fontSize: 11 }}>↗</span>
        <span style={{ color: "var(--txt-dim)" }}>workspace</span>
        <span style={{ color: "var(--txt-faint)" }}>·</span>
        <span>{label}</span>
        <span style={{ color: "var(--txt-faint)", marginLeft: 2 }}>▾</span>
      </button>

      {open ? (
        <div
          role="dialog"
          aria-label="Workspaces"
          style={{
            position: "absolute",
            top: 28,
            left: 0,
            minWidth: 320,
            background: "var(--bg-panel)",
            border: "1px solid var(--line)",
            borderRadius: 4,
            boxShadow: "0 8px 24px rgba(0,0,0,.45)",
            zIndex: 60,
            fontFamily: "var(--mono)",
            fontSize: 11,
            color: "var(--txt)",
          }}
        >
          <RowButton
            label="(home)"
            detail="runs use the engine home"
            active={workspaceId == null}
            onClick={() => {
              onChange(null);
              setOpen(false);
            }}
          />
          {workspaces.map((w) => (
            <RowButton
              key={w.id}
              label={w.name}
              detail={w.path}
              active={w.id === workspaceId}
              onClick={() => {
                onChange(w.id);
                setOpen(false);
              }}
            />
          ))}
          <div
            style={{
              borderTop: "1px solid var(--line-soft)",
              padding: 8,
              display: "flex",
              flexDirection: "column",
              gap: 6,
            }}
          >
            {adding ? (
              <>
                <input
                  value={newName}
                  onChange={(e) => setNewName(e.target.value)}
                  placeholder="workspace name"
                  style={inputStyle}
                  autoFocus
                />
                <input
                  value={newPath}
                  onChange={(e) => setNewPath(e.target.value)}
                  placeholder={"absolute path (e.g. C:\\Users\\you\\project)"}
                  style={inputStyle}
                />
                {err ? (
                  <div style={{ color: "var(--danger, #e66)", fontSize: 10 }}>
                    {err}
                  </div>
                ) : null}
                <div style={{ display: "flex", gap: 6 }}>
                  <button
                    type="button"
                    onClick={() => void handleAdd()}
                    style={primaryBtnStyle}
                  >
                    add
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      setAdding(false);
                      setErr(null);
                    }}
                    style={ghostBtnStyle}
                  >
                    cancel
                  </button>
                </div>
              </>
            ) : (
              <button
                type="button"
                onClick={() => setAdding(true)}
                style={ghostBtnStyle}
              >
                + new workspace…
              </button>
            )}
            {onOpenManage ? (
              <button
                type="button"
                onClick={() => {
                  setOpen(false);
                  onOpenManage();
                }}
                style={linkBtnStyle}
              >
                manage in settings →
              </button>
            ) : null}
          </div>
        </div>
      ) : null}
    </div>
  );
}

function RowButton({
  label,
  detail,
  active,
  onClick,
}: {
  label: string;
  detail: string;
  active: boolean;
  onClick: () => void;
}): JSX.Element {
  return (
    <button
      type="button"
      onClick={onClick}
      style={{
        display: "block",
        width: "100%",
        textAlign: "left",
        padding: "8px 12px",
        background: active ? "var(--accent-soft, rgba(50,200,200,.08))" : "transparent",
        border: "none",
        borderBottom: "1px solid var(--line-soft)",
        cursor: "pointer",
        color: active ? "var(--accent)" : "var(--txt)",
        fontFamily: "var(--mono)",
        fontSize: 11,
      }}
    >
      <div style={{ fontWeight: active ? 600 : 400 }}>{label}</div>
      <div
        style={{
          color: "var(--txt-faint)",
          fontSize: 10,
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}
        title={detail}
      >
        {detail}
      </div>
    </button>
  );
}

const inputStyle: React.CSSProperties = {
  background: "var(--bg)",
  border: "1px solid var(--line)",
  borderRadius: 3,
  padding: "6px 8px",
  color: "var(--txt)",
  fontFamily: "var(--mono)",
  fontSize: 11,
};

const primaryBtnStyle: React.CSSProperties = {
  flex: 1,
  height: 26,
  background: "var(--accent)",
  border: "1px solid var(--accent)",
  color: "var(--bg)",
  cursor: "pointer",
  fontFamily: "var(--mono)",
  fontSize: 11,
  borderRadius: 3,
};

const ghostBtnStyle: React.CSSProperties = {
  height: 26,
  background: "transparent",
  border: "1px dashed var(--line)",
  color: "var(--txt)",
  cursor: "pointer",
  fontFamily: "var(--mono)",
  fontSize: 11,
  borderRadius: 3,
  padding: "0 10px",
};

const linkBtnStyle: React.CSSProperties = {
  height: 22,
  background: "transparent",
  border: "none",
  color: "var(--txt-dim)",
  cursor: "pointer",
  fontFamily: "var(--mono)",
  fontSize: 10,
  textAlign: "left",
  padding: 0,
  textDecoration: "underline",
};
