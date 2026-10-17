import { useEffect, useState } from "react";
import { api } from "../lib/api";
import { STATUS, nyTime, pct } from "../lib/format";
import Sparkline from "./Sparkline";

export default function StationDrawer({ id, source, at, onClose }) {
  const [d, setD] = useState(null);
  const [err, setErr] = useState(null);
  useEffect(() => {
    let alive = true;
    setErr(null);
    api(`station/${id}`, { source, at }).then((x) => alive && setD(x)).catch((e) => alive && setErr(e.message));
    return () => { alive = false; };
  }, [id, source, at]);
  if (err) return <aside className="drawer glass"><div className="hd"><h2>Station</h2><button className="close" onClick={onClose}>×</button></div><section className="muted">{err}</section></aside>;
  if (!d) return <aside className="drawer glass"><div className="loading">Loading station…</div></aside>;
  const st = STATUS[d.status];
  const cap = Math.max(d.cap || 0, d.bikes + d.docks, 1);
  const p = d.status === "risk_full" || d.status === "full_now" ? d.pf : d.pe;
  return (
    <aside className="drawer glass" aria-label={`Station ${d.name}`}>
      <div className="hd">
        <div>
          <span className="chip" style={{ background: `${st.color}22`, color: st.color, boxShadow: `inset 0 0 0 1px ${st.color}88` }}>{st.label}{d.level !== "none" ? ` · ${d.level}` : ""}</span>
          <h2 style={{ marginTop: 8 }}>{d.name}</h2>
          <div className="dim" style={{ fontSize: 12 }}>#{d.sid} · last report {nyTime(new Date(d.lr * 1000).toISOString())} NY</div>
        </div>
        <button className="close" onClick={onClose} aria-label="Close">×</button>
      </div>
      <section>
        <div className="big-sentence">{d.sentence}</div>
        <div className="probbar" style={{ marginTop: 12 }}><div style={{ width: `${Math.round((p ?? 0) * 100)}%`, background: st.color }} /></div>
      </section>
      <section>
        <h4>Bikes and docks</h4>
        <div className="gauge"><div className="b" style={{ width: `${(d.bikes / cap) * 100}%` }} /><div className="d" style={{ width: `${(d.docks / cap) * 100}%` }} /></div>
        <div className="gauge-legend"><span><b className="num">{d.bikes}</b> bikes{d.ebikes ? <span className="dim"> ({d.ebikes} e-bikes)</span> : null}</span><span><b className="num">{d.docks}</b> free docks</span></div>
      </section>
      <section>
        <h4>Bikes available · last 3 hours</h4>
        <Sparkline points={d.sparkline} cap={d.cap} />
      </section>
      <section>
        <h4>Next 30 minutes (expected)</h4>
        <div className="stat-grid">
          <div className="stat"><div className="v num">{d.ed}</div><div className="l">Departures</div></div>
          <div className="stat"><div className="v num">{d.ea}</div><div className="l">Arrivals</div></div>
          <div className="stat"><div className="v num">{pct(d.pe)}</div><div className="l">P(runs out)</div></div>
          <div className="stat"><div className="v num">{pct(d.pf)}</div><div className="l">P(fills up)</div></div>
        </div>
        <div className="dim" style={{ fontSize: 12, marginTop: 8 }}>Historical average for this weekday and quarter-hour × weather factor <b className="num" style={{ color: "var(--text)" }}>×{d.wf}</b>.</div>
      </section>
      <section>
        <h4>Suggested rebalancing</h4>
        {d.suggestion ? <div className="suggest">{d.suggestion.text}.<div className="dim" style={{ marginTop: 4 }}>Suggestion from expected demand; not an operator instruction.</div></div>
          : <div className="dim" style={{ fontSize: 13 }}>No move suggested for this station right now.</div>}
      </section>
    </aside>
  );
}
