import { CartesianGrid, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import VirtualList from "../components/VirtualList";
import { useData } from "../lib/data";
import { usePoll } from "../lib/hooks";
import { useMode } from "../lib/mode";
import { STATUS, nyDateTime, nyTime, pct } from "../lib/format";

const COLS = "120px minmax(200px, 2fr) 96px 80px 80px 72px 96px";

const p0 = (x) => (x === null || x === undefined ? "–" : `${Math.round(x * 100)}%`);
const n0 = (x) => (x === null || x === undefined ? "–" : x.toLocaleString("en-US"));

/** Model alerts vs the "<= 2 bikes (or docks)" rule, precision and recall (src/score_alerts.py logic). */
function ScoreTable({ d }) {
  const row = (label, sub, e, f, dim) => (
    <tr className={dim ? "base" : ""}>
      <td><b>{label}</b>{sub && <div className="dim" style={{ fontSize: 12 }}>{sub}</div>}</td>
      <td>{p0(e.p)} <span className="dim">({n0(e.n)})</span></td><td>{e.r === undefined ? "" : p0(e.r)}</td>
      <td>{p0(f.p)} <span className="dim">({n0(f.n)})</span></td><td>{f.r === undefined ? "" : p0(f.r)}</td>
    </tr>
  );
  const E = d.empty, F = d.full;
  return (
    <div style={{ overflowX: "auto" }}>
      <table className="t">
        <thead>
          <tr><th rowSpan={2}>Who raises the alert</th><th colSpan={2}>Runs out of bikes (empty)</th><th colSpan={2}>Fills up (full)</th></tr>
          <tr><th>Precision (alerts)</th><th>Recall</th><th>Precision (alerts)</th><th>Recall</th></tr>
        </thead>
        <tbody>
          {row("BikeFlow alerts", "forward-looking, HIGH + MEDIUM",
               { p: E.model_risk_all.precision, n: E.model_risk_all.alerts, r: E.model_recall },
               { p: F.model_risk_all.precision, n: F.model_risk_all.alerts, r: F.model_recall })}
          {row("· HIGH only", "P ≥ 70%", { p: E.model_high.precision, n: E.model_high.alerts }, { p: F.model_high.precision, n: F.model_high.alerts }, true)}
          {row("· MEDIUM only", "P 50–70%", { p: E.model_medium.precision, n: E.model_medium.alerts }, { p: F.model_medium.precision, n: F.model_medium.alerts }, true)}
          {row("Simple rule: ≤ 2 bikes (or docks)", "all stations at or below 2, incl. already empty/full",
               { p: E.baseline_all.precision, n: E.baseline_all.alerts, r: E.baseline_recall },
               { p: F.baseline_all.precision, n: F.baseline_all.alerts, r: F.baseline_recall })}
          {row("· stations at exactly 2", "the rule's real predictions (not yet empty/full)",
               { p: E.baseline_exactly_2.precision, n: E.baseline_exactly_2.alerts }, { p: F.baseline_exactly_2.precision, n: F.baseline_exactly_2.alerts }, true)}
        </tbody>
      </table>
      <p className="dim" style={{ fontSize: 12, margin: "8px 0 0" }}>
        Precision: share of alerts where the station really reached ≤ 1 bike (or ≤ 1 free dock) within 30 minutes. Recall: share of the
        {" "}{n0(E.events)} empty and {n0(F.events)} full events (a station dropping to ≤ 1) that had an alert of that type for that station in the 30 minutes before.
        BikeFlow's recall counts any of its alerts, including “empty now”/“full now”. Alert counts are station-minutes (one per scoring time while a
        station stays on alert), so they are larger than the episode counts further down. {n0(d.evaluation_times)} scoring times, {n0(d.station_times)} station checks.
      </p>
    </div>
  );
}

function tradeOff(d) {
  const E = d.empty, F = d.full;
  return `BikeFlow's probability alerts are more precise and fire much less often: ${p0(E.model_risk_all.precision)} (empty) and ${p0(F.model_risk_all.precision)} (full) of its forward-looking alerts came true, ` +
    `against ${p0(E.baseline_exactly_2.precision)} and ${p0(F.baseline_exactly_2.precision)} when the simple rule fires on a station that still has 2 bikes or docks — so its real predictions are wrong more often than right. ` +
    `The simple rule catches more events (${p0(E.baseline_recall)} and ${p0(F.baseline_recall)} recall, against ${p0(E.model_recall)} and ${p0(F.model_recall)}) because it fires on every nearly-empty station, ` +
    `and it only looks accurate overall (${p0(E.baseline_all.precision)}) because most of its alerts are stations that are already empty or full.`;
}

function ScoringSections() {
  const replay = usePoll("scores", { source: "replay" }, { interval: 0 }).data;
  const live = usePoll("scores", { source: "live" }, { interval: 60_000 }).data;
  return (
    <>
      <div className="card">
        <h3>Replay: one recorded 3-hour evening rush window</h3>
        <p className="sub">{replay?.window?.start ? `${nyDateTime(replay.window.start)} – ${nyTime(replay.window.end)} New York, scored against the recorded station status.` : "…"}</p>
        {replay?.empty ? <><ScoreTable d={replay} /><p className="muted" style={{ marginBottom: 0 }}>{tradeOff(replay)}</p></>
          : <div className="empty-state">{replay ? "No replay scores yet (run scripts/make_replay.sh)." : "Loading…"}</div>}
      </div>
      <div className="card">
        <h3>Live scoring</h3>
        <p className="sub">Alerts from the live engine since its last restart{live?.since ? ` (${nyDateTime(live.since)} New York)` : ""}, scored on the collected live status data.
          Only alerts older than 30 minutes count. Scoring runs in a separate low-priority process at most every 10 minutes and only reads the live data.</p>
        {live?.enough ? <><ScoreTable d={live} /><p className="muted" style={{ marginBottom: 0 }}>{tradeOff(live)}</p></>
          : <div className="empty-state" style={{ textAlign: "left", padding: "8px 0" }}>
              <b>Not enough data yet</b> — {n0(live?.resolved_risk_alerts ?? 0)} resolved forward-looking alerts so far; {live?.min_resolved ?? 50} are needed.
              {live?.computing ? " Scoring is running now." : ""} {live?.data_end ? `Live status collected up to ${nyTime(live.data_end)} New York.` : ""}
            </div>}
      </div>
    </>
  );
}

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
        <p className="lede">Every alert is checked against the recorded station status: did the station really reach ≤ 1 bike (empty) or ≤ 1 free dock
          (full) within 30 minutes? Below: how BikeFlow's alerts compare with a simple rule, first on one recorded rush hour, then live.</p>
      </div>
      <ScoringSections />
      <h2 style={{ margin: "8px 0 0", fontSize: 18 }}>Alert episodes · {source === "replay" ? "replay window" : "live, last 24 hours"}</h2>
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
            comes true about 6 times in 10 is well calibrated, not a failure. An <b>episode</b> is one station staying on alert for consecutive minutes, counted once.
            For recall and the comparison with the simple “≤ 2 bikes (or docks)” rule, see the tables above.</p>
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
