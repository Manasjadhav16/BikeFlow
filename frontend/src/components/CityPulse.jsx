/** Hourly "bikes out on the street" (estimated from recorded docked totals), current hour highlighted. */
export default function CityPulse({ pulse }) {
  const h = pulse?.hourly ?? [];
  const W = 288, H = 96;
  const max = Math.max(1, ...h.map((x) => x.in_use_est));
  const bw = h.length ? (W - 8) / h.length : 0;
  return (
    <div className="pulse-panel glass">
      <div className="panel-title"><h3>City pulse</h3><span className="dim" style={{ fontSize: 11 }}>bikes out · per hour</span></div>
      <div className="pulse-body">
        {h.length === 0 ? <div className="dim" style={{ fontSize: 12 }}>Waiting for recorded data…</div> : (
          <svg className="pulse-svg" width="100%" viewBox={`0 0 ${W} ${H}`} role="img" aria-label="Estimated bikes out on the street per hour">
            {h.map((x, i) => {
              const bh = (x.in_use_est / max) * (H - 28);
              const cur = x.hour === pulse.current_hour;
              return (
                <g key={x.hour}>
                  <rect className={`bar ${cur ? "cur" : ""}`} x={4 + i * bw + 1} y={H - 16 - bh} width={Math.max(2, bw - 2)} height={bh} rx="2" />
                  {cur && <text x={4 + i * bw + bw / 2} y={H - 20 - bh} textAnchor="middle" style={{ fill: "#e8edf5", fontWeight: 700 }}>{x.in_use_est.toLocaleString()}</text>}
                  {(i % Math.ceil(h.length / 6) === 0) && <text x={4 + i * bw + bw / 2} y={H - 4} textAnchor="middle">{x.label.slice(4, 6)}h</text>}
                </g>
              );
            })}
          </svg>
        )}
      </div>
    </div>
  );
}
