import VirtualList from "./VirtualList";
import { STATUS, pct } from "../lib/format";

export default function AlertPanel({ alerts, selected, onPick, asOf }) {
  return (
    <div className="right-panel glass">
      <div className="panel-title">
        <h3>Forward-looking alerts</h3>
        <span className="chip medium">{alerts.length}</span>
      </div>
      {alerts.length === 0 ? (
        <div className="empty-state">No station is at risk of running empty or full in the next 30 minutes{asOf ? "" : " (waiting for data)"}.</div>
      ) : (
        <VirtualList className="alist" items={alerts.map((a) => ({ ...a, key: a.id }))} rowHeight={72} render={(a) => (
          <div className={`arow ${selected === a.id ? "sel" : ""}`} onClick={() => onPick(a)}>
            <div className="bar" style={{ background: STATUS[a.status].color }} />
            <div style={{ minWidth: 0 }}>
              <div className="nm">{a.name}</div>
              <div className="sub">{a.status === "risk_empty" ? `${a.bikes} bikes left` : `${a.docks} docks free`} · {a.ed} out / {a.ea} in expected</div>
            </div>
            <div>
              <div className="pct" style={{ color: STATUS[a.status].color }}>{pct(a.p)}</div>
              <div className="lvl" style={{ color: a.level === "HIGH" ? "#fca5a5" : "#fcd34d" }}>{a.level} · {a.status === "risk_empty" ? "EMPTY" : "FULL"}</div>
            </div>
          </div>
        )} />
      )}
    </div>
  );
}
