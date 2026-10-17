import { CartesianGrid, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import VirtualList from "../components/VirtualList";
import { useData } from "../lib/data";
import { useMode } from "../lib/mode";
import { STATUS, nyDateTime, nyTime, pct } from "../lib/format";

const COLS = "120px minmax(200px, 2fr) 96px 80px 80px 72px 96px";

export default function Reality() {
  const { resolved } = useData();
  const { source } = useMode();
  const s = resolved?.summary;
  const eps = resolved?.episodes ?? [];
  const curve = resolved?.curve ?? [];
  return (
    <div className="page"><div className="page-inner">
      <div>
        <h1>Prediction vs reality</h1>
        <p className="lede">Each forward-looking alert episode (same station and risk, consecutive minutes) is checked against the recorded
          station status: did it actually reach ≤ 1 bike (empty) or ≤ 1 free dock (full) within 30 minutes? {source === "replay" ? "Replay window." : "Live: last 24 hours."}</p>
      </div>
      <div className="cards-row">
        <div className="metric"><div className="v">{s?.alerts_so_far ?? "–"}</div><div className="l">Alert episodes so far</div></div>
        <div className="metric"><div className="v">{s?.resolved ?? "–"}</div><div className="l">Resolved (30 min passed)</div></div>
        <div className="metric"><div className="v">{s?.pending ?? "–"}</div><div className="l">Pending</div></div>
        <div className="metric"><div className="v" style={{ color: "#fca5a5" }}>{pct(s?.precision_high)}</div><div className="l">Precision · HIGH ({s?.resolved_high ?? 0})</div></div>
        <div className="metric"><div className="v" style={{ color: "#fcd34d" }}>{pct(s?.precision_medium)}</div><div className="l">Precision · MEDIUM ({s?.resolved_medium ?? 0})</div></div>
        <div className="metric"><div className="v" style={{ color: "var(--good)" }}>{pct(s?.precision_all)}</div><div className="l">Precision · all</div></div>
      </div>
      <div className="caveat">Operator vans move bikes between stations: a restock or clear-out can turn a correct alert into a “miss” (or the reverse) regardless of rider demand.</div>
      <div className="grid-2">
        <div className="card">
          <h3>Running precision vs number of alerts</h3>
          <p className="sub">Share of resolved alert episodes that came true, as more alerts accumulate.</p>
          <div style={{ height: 280 }}>
            {curve.length < 2 ? <div className="empty-state">Not enough resolved alerts yet{source === "live" ? " — try Replay" : ""}.</div> : (
              <ResponsiveContainer>
                <LineChart data={curve} margin={{ top: 8, right: 16, bottom: 8, left: 0 }}>
                  <CartesianGrid stroke="rgba(148,163,184,0.12)" vertical={false} />
                  <XAxis dataKey="n" stroke="#7d8aa0" fontSize={12} label={{ value: "resolved alerts", position: "insideBottomRight", offset: -4, fill: "#7d8aa0", fontSize: 11 }} />
                  <YAxis domain={[0, 1]} tickFormatter={(v) => `${Math.round(v * 100)}%`} stroke="#7d8aa0" fontSize={12} />
                  <ReferenceLine y={0.5} stroke="rgba(245,158,11,0.5)" strokeDasharray="4 4" />
                  <Tooltip contentStyle={{ background: "#101622", border: "1px solid rgba(148,163,184,0.3)", borderRadius: 8 }} formatter={(v) => pct(v, 1)} labelFormatter={(n) => `${n} alerts`} />
                  <Line type="monotone" dataKey="precision" stroke="#22d3ee" strokeWidth={2} dot={false} isAnimationActive={false} />
                </LineChart>
              </ResponsiveContainer>
            )}
          </div>
        </div>
        <div className="card">
          <h3>How to read this</h3>
          <p className="muted" style={{ marginTop: 0 }}>An alert says “at least this probability of reaching the threshold within 30 minutes”. A MEDIUM alert (50–70%) that
            comes true about 6 times in 10 is well calibrated, not a failure. Recall (events caught) is scored separately in
            <span className="mono"> outputs/metrics/alert_scores.json</span>; the simple rule “alert when ≤ 2 bikes” catches more events but with far lower precision.</p>
          <p className="dim" style={{ fontSize: 12 }}>Resolution uses the collector’s recorded status for the 30 minutes after each alert ({s?.data_end ? `data until ${nyDateTime(s.data_end)}` : "…"}).</p>
        </div>
      </div>
      <div className="card" style={{ padding: 0, overflow: "hidden" }}>
        <div className="vt-head" style={{ gridTemplateColumns: COLS }}><span>Alerted (NY)</span><span>Station</span><span>Risk</span><span>Level</span><span>P</span><span>Outcome</span><span>Reached at</span></div>
        {eps.length === 0 ? <div className="empty-state">No alerts yet.</div> : (
          <VirtualList items={eps.map((e, i) => ({ ...e, key: `${e.station_id}-${e.as_of}-${i}` }))} rowHeight={48} style={{ height: 420 }} render={(e) => (
            <div className="vt-row" style={{ gridTemplateColumns: COLS, cursor: "default" }}>
              <span className="num">{nyTime(e.as_of)}</span>
              <span className="ellipsis"><b>{e.name}</b></span>
              <span style={{ color: STATUS[e.type === "empty" ? "risk_empty" : "risk_full"].color, fontWeight: 600 }}>{e.type === "empty" ? "Runs out" : "Fills up"}</span>
              <span><span className={`chip ${e.level.toLowerCase()}`}>{e.level}</span></span>
              <span className="num">{pct(e.probability)}</span>
              <span>{e.hit === null ? <span className="chip pending">pending</span> : e.hit ? <span className="chip hit">hit</span> : <span className="chip miss">miss</span>}</span>
              <span className="num dim">{e.hit_at ? nyTime(e.hit_at) : "–"}</span>
            </div>
          )} />
        )}
      </div>
    </div></div>
  );
}
