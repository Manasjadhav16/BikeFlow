import { useMemo, useState } from "react";
import { STATUS, STATUS_ORDER } from "../lib/format";

export default function FilterPanel({ stations, visible, setVisible, onPick }) {
  const [q, setQ] = useState("");
  const counts = useMemo(() => {
    const c = {};
    for (const s of stations) c[s.status] = (c[s.status] || 0) + 1;
    return c;
  }, [stations]);
  const results = useMemo(() => {
    if (q.trim().length < 2) return [];
    const t = q.toLowerCase();
    return stations.filter((s) => s.name?.toLowerCase().includes(t)).slice(0, 12);
  }, [q, stations]);
  const toggle = (k) => {
    const n = new Set(visible);
    n.has(k) ? n.delete(k) : n.add(k);
    setVisible(n);
  };
  return (
    <div className="left-panel glass">
      <div className="panel-title"><h3>Stations</h3><span className="dim num">{stations.length.toLocaleString()}</span></div>
      <div className="search">
        <input placeholder="Search a station…" value={q} onChange={(e) => setQ(e.target.value)} aria-label="Search stations" />
      </div>
      {results.length > 0 && (
        <div className="search-results">
          {results.map((s) => (
            <button key={s.id} onClick={() => { onPick(s); setQ(""); }}>{s.name} <span className="dim">· {s.bikes} bikes</span></button>
          ))}
        </div>
      )}
      <div className="filters">
        {STATUS_ORDER.map((k) => (
          <button key={k} className={`filter ${visible.has(k) ? "" : "off"}`} onClick={() => toggle(k)} aria-pressed={visible.has(k)}>
            <span className="sw" style={{ background: STATUS[k].color }} />
            {STATUS[k].label}
            <span className="cnt">{(counts[k] || 0).toLocaleString()}</span>
          </button>
        ))}
      </div>
      <div className="legend-note">Marker size grows with the probability. Pulsing rings: HIGH alerts (P ≥ 70%).</div>
    </div>
  );
}
