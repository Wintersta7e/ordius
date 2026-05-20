import { useEffect, useRef, useState } from "react";

type Props = {
  onResize: (dx: number) => void;
  ariaLabel?: string;
};

export function Resizer({ onResize, ariaLabel }: Props): JSX.Element {
  const dragRef = useRef<{ x: number } | null>(null);
  const [hover, setHover] = useState(false);
  const [drag, setDrag] = useState(false);

  const onDown = (e: React.MouseEvent<HTMLDivElement>) => {
    e.preventDefault();
    dragRef.current = { x: e.clientX };
    setDrag(true);
    document.body.style.cursor = "col-resize";
  };

  useEffect(() => {
    if (!drag) return;
    const mv = (e: MouseEvent) => {
      if (!dragRef.current) return;
      const dx = e.clientX - dragRef.current.x;
      if (dx !== 0) {
        onResize(dx);
        dragRef.current.x = e.clientX;
      }
    };
    const up = () => {
      setDrag(false);
      dragRef.current = null;
      document.body.style.cursor = "";
    };
    window.addEventListener("mousemove", mv);
    window.addEventListener("mouseup", up);
    return () => {
      window.removeEventListener("mousemove", mv);
      window.removeEventListener("mouseup", up);
    };
  }, [drag, onResize]);

  const active = hover || drag;
  return (
    <div
      role="separator"
      aria-orientation="vertical"
      aria-label={ariaLabel ?? "Resize panel"}
      onMouseDown={onDown}
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      style={{
        width: 5,
        flexShrink: 0,
        cursor: "col-resize",
        position: "relative",
        zIndex: 5,
        background: "transparent",
        userSelect: "none",
      }}
    >
      <div
        style={{
          position: "absolute",
          top: 0,
          bottom: 0,
          left: 2,
          width: 1,
          background: active ? "var(--accent)" : "var(--line)",
          boxShadow: active ? "0 0 6px var(--accent)" : "none",
          transition: "background .12s, box-shadow .12s",
        }}
      />
    </div>
  );
}
