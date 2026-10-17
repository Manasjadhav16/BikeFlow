import { useNavigate } from "react-router-dom";
import VirtualList from "../components/VirtualList";
import { usePoll } from "../lib/hooks";
import { useMode } from "../lib/mode";
import { STATUS, nyDateTime, pct } from "../lib/format";

const COLS = "32px minmax(150px, 2fr) 76px 84px minmax(120px, 1.3fr) 48px 52px 104px 56px";

export default function AlertsPage() {
  const { source, at, flyTo } = useMode();
  const nav = useNavigate();
  const params = { source, at };
  const live = source === "live";
  const alerts = usePoll("alerts", params, { interval: live ? 30_000 : 0 });
  const reb = usePoll("rebalance", params, { interval: live ? 30_000 : 0 });
  const list = alerts.data?.alerts ?? [];
  const go = (a) => { flyTo({ id: a.id, lat: a.lat, lng: a.lng }); nav("/"); };
  return (
    <div className="page"><div className="page-inner">
      <div>
        <h1>Forward-looking alerts</h1>
        <p className="lede">Stations likely to run out of bikes or fill up in the next 30 minutes, ranked by probability
          ({alerts.data ? nyDateTime(alerts.data.as_of) : "…"} New York). Stations that are already empty or full are on the map, not here.</p>
      </div>
      <div className="grid-3">
        <div className="card scroll-x" style={{ padding: 0 }}>
          <div className="vt-head" style={{ gridTemplateColumns: COLS }}>
            <span>#</span><span>Station</span><span>Risk</span><span>Level</span><span>Probability</span><span>Bikes</span><span>Docks</span><span>Exp. out / in</span><span>Weather</span>
          </div>
          {list.length === 0 ? <div className="empty-state">{alerts.error ?? "No forward-looking alerts right now."}</div> : (
            <VirtualList items={list.map((a) => ({ ...a, key: a.id }))} rowHeight={48} style={{ height: "calc(100vh - 260px)" }} render={(a, i) => (
              <div className="vt-row" style={{ gridTemplateColumns: COLS }} onClick={() => go(a)} title="Show on map">
                <span className="dim num">{i + 1}</span>
                <span className="ellipsis"><b>{a.name}</b></span>
                <span style={{ color: STATUS[a.status].color, fontWeight: 600 }}>{a.status === "risk_empty" ? "Runs out" : "Fills up"}</span>
                <span><span className={`chip ${a.level.toLowerCase()}`}>{a.level}</span></span>
                <span className="pbar"><span className="track"><span className="fill" style={{ display: "block", width: `${a.p * 100}%`, background: STATUS[a.status].color }} /></span><b className="num">{pct(a.p)}</b></span>
                <span className="num">{a.bikes}</span><span className="num">{a.docks}</span>
                <span className="num">{a.ed} / {a.ea}</span><span className="num">×{a.wf}</span>
              </div>
            )} />
          )}
        </div>
        <div className="card">
          <h3>Rebalancing suggestions</h3>
          <p className="sub">{reb.data?.note ?? "…"}</p>
          <div style={{ display: "grid", gap: 8 }}>
            {(reb.data?.suggestions ?? []).map((m) => (
              <div className="move" key={`${m.from_id}-${m.to_id}`} onClick={() => go({ id: m.to_id, lat: m.to_lat, lng: m.to_lng })} style={{ cursor: "pointer" }}>
                <div><b>Move {m.bikes_to_move} bike{m.bikes_to_move === 1 ? "" : "s"}</b> <span className="dim">· {m.distance_m} m</span></div>
                <div className="n num">{m.bikes_to_move}</div>
                <div className="muted" style={{ fontSize: 13 }}>{m.from_name} <span className="dim">({m.from_bikes} bikes)</span> → {m.to_name} <span className="dim">({m.to_bikes} bikes, {pct(m.to_probability)} risk)</span></div>
              </div>
            ))}
            {reb.data && reb.data.suggestions.length === 0 && <div className="dim">No at-risk empty station has a nearby donor right now.</div>}
          </div>
        </div>
      </div>
    </div></div>
  );
}
