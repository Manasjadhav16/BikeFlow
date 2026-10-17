import { useCountUp } from "../lib/hooks";
import { age, nyTime, pct, weatherLabel } from "../lib/format";

function Kpi({ label, value, color, fmt = (v) => Math.round(v).toLocaleString("en-US"), unit, className = "" }) {
  const shown = useCountUp(typeof value === "number" ? value : null);
  return (
    <div className={`kpi ${className}`}>
      <div className="v" style={{ color }}>
        {typeof value === "number" ? fmt(shown) : value ?? "–"}{unit && <span className="unit">{unit}</span>}
      </div>
      <div className="l">{label}</div>
    </div>
  );
}

export default function KpiStrip({ state, resolved, source }) {
  const k = state?.kpis;
  const hit = resolved?.summary?.precision_all;
  return (
    <div className="kpis">
      <Kpi label="Stations monitored" value={k?.in_service} />
      <Kpi label="Empty now" value={k?.empty_now} color="var(--empty)" />
      <Kpi label="Full now" value={k?.full_now} color="#60a5fa" />
      <Kpi label="At risk · 30 min" value={k?.at_risk} color="var(--risk-empty)" />
      <Kpi label="Alert hit rate" value={hit ?? (resolved ? "n/a" : null)} fmt={(v) => pct(v)} className="hide-sm" />
      {source === "replay"
        ? <Kpi label="Replay time (NY)" value={state ? `${new Date(state.as_of).toLocaleDateString("en-US", { timeZone: "America/New_York", weekday: "short" })} ${nyTime(state.as_of)}` : null} className="weather" />
        : <Kpi label="Data age" value={state ? age(state.freshness_s) : null} className="weather" />}
      <Kpi label="Weather · demand ×" value={state?.weather ? `${weatherLabel(state.weather)} · ×${(state.weather.factor_dep ?? 1).toFixed(2)}` : null} className="weather hide-sm" />
    </div>
  );
}
