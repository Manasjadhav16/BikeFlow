import { STATUS, nyTime, pct } from "../lib/format";

export default function Ticker({ items, onPick }) {
  if (!items?.length) return <div className="ticker"><span className="tag">Alert feed</span><span className="dim">No forward-looking alerts in the last 3 hours.</span></div>;
  const row = items.map((it, i) => (
    <span key={`${it.station_id}-${it.as_of}-${i}`} className="ticker-item" onClick={() => onPick(it)}>
      <span className="t">{nyTime(it.as_of)}</span>
      <span className="d" style={{ background: STATUS[it.type === "empty" ? "risk_empty" : "risk_full"].color }} />
      <b>{it.name}</b> {it.type === "empty" ? "may run out of bikes" : "may fill up"} · {pct(it.probability)} {it.level === "HIGH" ? "· HIGH" : ""}
    </span>
  ));
  return (
    <div className="ticker" aria-label="Newest alerts">
      <span className="tag">Alert feed</span>
      <div className="ticker-viewport">
        <div className="ticker-track" style={{ animationDuration: `${Math.max(30, items.length * 6)}s` }}>{row}{row}</div>
      </div>
    </div>
  );
}
