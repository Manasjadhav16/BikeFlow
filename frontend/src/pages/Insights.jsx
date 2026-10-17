import { Fragment } from "react";
import { Bar, BarChart, CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, ComposedChart, Tooltip, XAxis, YAxis, LabelList } from "recharts";
import { usePoll } from "../lib/hooks";

const TT = { contentStyle: { background: "#101622", border: "1px solid rgba(148,163,184,0.3)", borderRadius: 8 }, labelStyle: { color: "#e8edf5" } };
const AX = { stroke: "#7d8aa0", fontSize: 12 };
const GRID = <CartesianGrid stroke="rgba(148,163,184,0.12)" vertical={false} />;
const k = (v) => (v >= 1e6 ? `${(v / 1e6).toFixed(1)}M` : v >= 1e3 ? `${Math.round(v / 1e3)}k` : v);
const LABELS = { "2025-12-25": "Christmas", "2026-01-25": "Jan 25 storm", "2026-02-23": "Feb 23 shutdown", "2026-09-27": "Sep 27" };
const NAMES = { persistence: "Persistence", hist_avg: "Historical avg", linear_regression: "Linear reg.", random_forest: "Random forest", gbt: "GBT", model: "GBT + weather", no_weather: "GBT, no weather", stage2_net: "Stage 2 net" };
const DOW = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

function StormDot({ cx, cy, payload }) {
  if (cx === undefined || cy === undefined || cy === null || payload?.storm === null || payload?.storm === undefined) return null;
  const label = LABELS[payload.date];
  return (
    <g>
      <circle cx={cx} cy={cy} r={4} fill="#ef4444" stroke="#0b0f17" strokeWidth={1.5} />
      {label && <text x={cx} y={cy - 10} fill="#fca5a5" fontSize={11} textAnchor="middle" fontWeight={600}>{label}</text>}
    </g>
  );
}

function ModelTable({ rows, cols, metricKeys, best = "min" }) {
  return (
    <table className="t">
      <thead><tr><th>Segment</th>{cols.map((c) => <th key={c}>{NAMES[c] ?? c}</th>)}</tr></thead>
      <tbody>
        {rows.map((r) => metricKeys.map((mk) => {
          const vals = cols.map((c) => r[c]?.[mk]).filter((v) => typeof v === "number");
          const top = best === "min" ? Math.min(...vals) : Math.max(...vals);
          return (
            <tr key={`${r.label}-${mk}`}>
              <td>{r.label} <span className="dim">· {mk.toUpperCase()}</span></td>
              {cols.map((c) => { const v = r[c]?.[mk]; return <td key={c} className={v === top ? "best" : ""}>{typeof v === "number" ? v.toFixed(3) : "–"}</td>; })}
            </tr>
          );
        }))}
      </tbody>
    </table>
  );
}

export default function Insights() {
  const { data: d, error } = usePoll("insights", {}, { interval: 0 });
  if (error) return <div className="page"><div className="loading">{error}</div></div>;
  if (!d) return <div className="page"><div className="loading">Loading insights…</div></div>;
  const storm = new Set(d.disrupted_days.map((x) => x.date));
  const daily = d.daily.map((x) => ({ ...x, storm: storm.has(x.date) ? x.rides : null }));
  const monthly = Object.entries(d.rides_per_month).map(([m, v]) => ({ m, label: new Date(`${m}-15`).toLocaleString("en-US", { month: "short" }), rides: v }));
  const heatMax = Math.max(...d.heatmap.map((h) => h.departures));
  const heat = {};
  for (const h of d.heatmap) heat[`${h.dow}-${h.hour}`] = h.departures;
  const s2 = d.stage2.segments.map((s) => ({ ...s, label: { overall: "Overall", top20_busiest: "Busiest 20%", rush_hours: "Rush hours" }[s.segment] ?? s.segment }));
  const s2cols = ["persistence", "hist_avg", "linear_regression", "random_forest", "gbt"];
  const ev = d.stage2b.events.map((e) => ({ ...e, label: `${e.event.replace("drain", "Drain").replace("fill", "Fill")} (${(e.base_rate * 100).toFixed(1)}%)` }));
  return (
    <div className="page"><div className="page-inner">
      <div>
        <h1>Insights</h1>
        <p className="lede">{d.summary_numbers.total_departures.toLocaleString()} rides across {d.summary_numbers.stations.toLocaleString()} stations and {d.summary_numbers.days} days (Oct 2025 – Sep 2026).</p>
      </div>

      <div className="card">
        <h3>Daily rides, with storm and disruption days</h3>
        <p className="sub">Red dots: the {d.disrupted_days.length} days below 40% of the local same-weekday median (storms, holidays, the Feb 22–24 shutdown).</p>
        <div style={{ height: 300 }}>
          <ResponsiveContainer>
            <ComposedChart data={daily} margin={{ top: 24, right: 24, bottom: 8, left: 0 }}>
              {GRID}
              <XAxis dataKey="date" {...AX} tickFormatter={(v) => new Date(v).toLocaleString("en-US", { month: "short" })} interval={30} />
              <YAxis {...AX} tickFormatter={k} />
              <Tooltip {...TT} formatter={(v) => v?.toLocaleString()} />
              <Line dataKey="rides" stroke="#22d3ee" dot={false} strokeWidth={1.5} isAnimationActive={false} name="Rides" />
              <Line dataKey="storm" stroke="none" dot={<StormDot />} activeDot={false} isAnimationActive={false} name="Disrupted day" />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="grid-2">
        <div className="card">
          <h3>Monthly rides</h3>
          <p className="sub">Summer peaks above 5M rides a month; January is about a third of that.</p>
          <div style={{ height: 260 }}>
            <ResponsiveContainer>
              <BarChart data={monthly} margin={{ top: 16, right: 8, bottom: 0, left: 0 }}>
                {GRID}<XAxis dataKey="label" {...AX} /><YAxis {...AX} tickFormatter={k} />
                <Tooltip {...TT} formatter={(v) => v.toLocaleString()} />
                <Bar dataKey="rides" fill="#38bdf8" radius={[4, 4, 0, 0]} isAnimationActive={false}><LabelList dataKey="rides" position="top" formatter={k} fill="#a9b4c6" fontSize={11} /></Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
        <div className="card">
          <h3>Weekday vs weekend, by hour</h3>
          <p className="sub">Average departures per day. Weekdays peak at {d.weekday_vs_weekend.weekday.peak_hour}:00, weekends at {d.weekday_vs_weekend.weekend.peak_hour}:00.</p>
          <div style={{ height: 260 }}>
            <ResponsiveContainer>
              <LineChart data={d.hourly_profile} margin={{ top: 8, right: 16, bottom: 0, left: 0 }}>
                {GRID}<XAxis dataKey="hour" {...AX} /><YAxis {...AX} tickFormatter={k} />
                <Tooltip {...TT} formatter={(v) => Math.round(v).toLocaleString()} labelFormatter={(h) => `${h}:00`} />
                <Legend wrapperStyle={{ fontSize: 12 }} />
                <Line dataKey="weekday" name="Weekday" stroke="#22d3ee" strokeWidth={2} dot={false} isAnimationActive={false} />
                <Line dataKey="weekend" name="Weekend" stroke="#f59e0b" strokeWidth={2} dot={false} isAnimationActive={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      <div className="grid-2">
        <div className="card">
          <h3>Departures by hour and day of week</h3>
          <p className="sub">Expected departures in an average week (historical average used by the alert engine), all stations.</p>
          <div className="heat" role="img" aria-label="Heatmap of departures by hour and day of week">
            <span />{Array.from({ length: 24 }, (_, h) => <span key={h} style={{ textAlign: "center" }}>{h % 3 === 0 ? h : ""}</span>)}
            {DOW.map((dn, i) => (
              <Fragment key={dn}>
                <span className="lbl">{dn}</span>
                {Array.from({ length: 24 }, (_, h) => {
                  const v = heat[`${i + 1}-${h}`] ?? 0;
                  const a = Math.pow(v / heatMax, 0.8);
                  return <span key={`${dn}-${h}`} className="c" title={`${dn} ${h}:00 · ${Math.round(v).toLocaleString()} departures`} style={{ background: `rgba(34, 211, 238, ${0.06 + a * 0.9})` }} />;
                })}
              </Fragment>
            ))}
          </div>
        </div>
        <div className="card">
          <h3>Busiest stations</h3>
          <p className="sub">Departures + arrivals over 12 months.</p>
          <div style={{ height: 330 }}>
            <ResponsiveContainer>
              <BarChart data={d.busiest_stations.slice(0, 10)} layout="vertical" margin={{ top: 0, right: 48, bottom: 0, left: 8 }}>
                <XAxis type="number" hide /><YAxis type="category" dataKey="name" width={190} {...AX} />
                <Tooltip {...TT} formatter={(v) => v.toLocaleString()} />
                <Bar dataKey="trips" fill="#38bdf8" radius={[0, 4, 4, 0]} isAnimationActive={false}><LabelList dataKey="trips" position="right" formatter={k} fill="#a9b4c6" fontSize={11} /></Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      <div className="card">
        <h3>What the models taught us</h3>
        <p className="muted" style={{ marginTop: 0, maxWidth: 1100 }}>
          The <b>historical average</b> for each station, weekday and hour is a strong baseline. Random forest and gradient-boosted trees
          beat it by only about 1.5% on 30-minute net change (test month {d.stage2.test_month}; predicting zero everywhere scores MAE {d.stage2.always_zero.mae}),
          and on the alert events that matter (strong drains and fills) the historical average has the <b>higher average precision</b> and is better calibrated.
          Weather clearly helps on <b>stormy and disrupted days</b>, which is why the live engine uses the historical average with a weather factor instead of a heavier model.
        </p>
        <div className="grid-2">
          <div style={{ overflowX: "auto" }}>
            <h4 className="dim" style={{ margin: "8px 0" }}>Stage 2 · 30-min net bike change, test {d.stage2.test_month} (lower is better)</h4>
            <ModelTable rows={s2} cols={s2cols} metricKeys={["mae", "rmse"]} />
          </div>
          <div style={{ overflowX: "auto" }}>
            <h4 className="dim" style={{ margin: "8px 0" }}>Stage 2b · strong drain/fill events, test {d.stage2b.test_month} (average precision, higher is better)</h4>
            <ModelTable rows={ev} cols={d.stage2b.approaches} metricKeys={["ap"]} best="max" />
          </div>
        </div>
      </div>
    </div></div>
  );
}
