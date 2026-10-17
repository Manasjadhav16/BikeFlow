import { useEffect, useRef, useState } from "react";

/** Minimal fixed-row-height virtual list: only the visible rows (+ overscan) are in the DOM. */
export default function VirtualList({ items, rowHeight, render, overscan = 6, className = "", style }) {
  const ref = useRef(null);
  const [top, setTop] = useState(0);
  const [height, setHeight] = useState(400);
  useEffect(() => {
    const el = ref.current;
    if (!el) return undefined;
    const ro = new ResizeObserver(() => setHeight(el.clientHeight));
    ro.observe(el);
    setHeight(el.clientHeight);
    return () => ro.disconnect();
  }, []);
  const first = Math.max(0, Math.floor(top / rowHeight) - overscan);
  const last = Math.min(items.length, Math.ceil((top + height) / rowHeight) + overscan);
  return (
    <div ref={ref} className={className} style={{ overflowY: "auto", position: "relative", ...style }}
         onScroll={(e) => setTop(e.currentTarget.scrollTop)}>
      <div style={{ height: items.length * rowHeight, position: "relative" }}>
        {items.slice(first, last).map((it, i) => (
          <div key={it.key ?? first + i} style={{ position: "absolute", top: (first + i) * rowHeight, left: 0, right: 0, height: rowHeight }}>
            {render(it, first + i)}
          </div>
        ))}
      </div>
    </div>
  );
}
