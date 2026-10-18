import { Bar, BarChart, CartesianGrid, Cell, LabelList, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import VirtualList from "../components/VirtualList";
import { useData } from "../lib/data";
import { usePoll } from "../lib/hooks";
import { useMode } from "../lib/mode";
import { STATUS, nyDateTime, nyTime, pct } from "../lib/format";

const COLS = "120px minmax(200px, 2fr) 96px 80px 80px 72px 96px";

const p0 = (x) => (x === null || x === undefined ? "–" : `${Math.round(x * 100)}%`);
const n0 = (x) => (x === null || x === undefined ? "–" : x.toLocaleString("en-US"));
const p1 = (x) => (x === null || x === undefined || Number.isNaN(x) ? "–" : `${(x * 100).toFixed(1)}%`);
// F1 from precision and recall (2PR / (P + R)); undefined when either is missing.
const f1of = (p, r) => (p === null || p === undefined || r === null || r === undefined ? undefined : (p + r > 0 ? (2 * p * r) / (p + r) : 0));

/** Model alerts vs the "<= 2 bikes (or docks)" rule, precision and recall (src/score_alerts.py logic). */
function ScoreTable({ d }) {
  const row = (label, sub, e, f, dim) => (
    <tr className={dim ? "base" : ""}>
      <td><b>{label}</b>{sub && <div className="dim" style={{ fontSize: 12 }}>{sub}</div>}</td>
      <td>{p0(e.p)} <span className="dim">({n0(e.n)})</span></td><td>{e.r === undefined ? "" : p0(e.r)}</td>
      <td>{e.r === undefined ? "" : p0(f1of(e.p, e.r))}</td>
      <td>{p0(f.p)} <span className="dim">({n0(f.n)})</span></td><td>{f.r === undefined ? "" : p0(f.r)}</td>
      <td>{f.r === undefined ? "" : p0(f1of(f.p, f.r))}</td>
    </tr>
  );
  const E = d.empty, F = d.full;
  return (
    <div style={{ overflowX: "auto" }}>
      <table className="t">
        <thead>
          <tr><th rowSpan={2}>Who raises the alert</th><th colSpan={3}>Runs out of bikes (empty)</th><th colSpan={3}>Fills up (full)</th></tr>
          <tr><th>Precision (alerts)</th><th>Recall</th><th>F1</th><th>Precision (alerts)</th><th>Recall</th><th>F1</th></tr>
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
  // Only claim "wrong more often than right" when the simple rule's real predictions really are below 50%.
  const ruleMostlyWrong = E.baseline_exactly_2.precision < 0.5 && F.baseline_exactly_2.precision < 0.5;
  const ours = `${p0(E.model_risk_all.precision)} and ${p0(F.model_risk_all.precision)}`;
  return "BikeFlow's probability alerts are more precise and fire much less often. " +
    `When the simple rule makes a real prediction (a station that still has 2 bikes or docks), it is right only ${p0(E.baseline_exactly_2.precision)} (empty) and ${p0(F.baseline_exactly_2.precision)} (full) of the time` +
    (ruleMostlyWrong ? `, so it is wrong more often than right, while BikeFlow's forward-looking alerts came true ${ours} of the time. `
                     : `, against ${ours} for BikeFlow's forward-looking alerts. `) +
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
              <b>Not enough data yet:</b> {n0(live?.resolved_risk_alerts ?? 0)} resolved forward-looking alerts so far; {live?.min_resolved ?? 50} are needed.
              {live?.computing ? " Scoring is running now." : ""} {live?.data_end ? `Live status collected up to ${nyTime(live.data_end)} New York.` : ""}
            </div>}
      </div>
    </>
  );
}

const MODELS = [
  ["persistence", "Persistence (never alerts)"],
  ["simple_rule", "Simple rule (≤ 2 bikes or docks)"],
  ["engine_score", "Alert engine (historical average × weather)"],
  ["logistic_regression", "Logistic regression"],
  ["random_forest", "Random forest"],
  ["gbt", "Gradient boosted trees"],
];
const TT = { contentStyle: { background: "#101622", border: "1px solid rgba(148,163,184,0.3)", borderRadius: 8 }, labelStyle: { color: "#e8edf5" } };
const AX = { stroke: "#7d8aa0", fontSize: 12 };
const nyRange = (r) => (r ? `${r.from} to ${r.to}` : "–");

function ModelTable({ label, R }) {
  const rows = MODELS.filter(([k]) => R.models[k]).map(([k, name]) => ({ k, name, m: R.models[k], t: R.models[k].test }));
  const bestF1 = Math.max(...rows.map((r) => r.t.f1 ?? -1));
  const bestAp = Math.max(...rows.map((r) => r.m.pr_auc ?? -1));
  const bars = rows.map((r) => ({ name: r.name.split(" (")[0], f1: r.t.f1 ?? 0, best: r.t.f1 === bestF1 }));
  const color = label === "empty" ? "var(--risk-empty)" : "var(--risk-full)";
  return (
    <div className="card">
      <h3 style={{ color }}>{label === "empty" ? "EMPTY within 30 minutes (≤ 1 bike)" : "FULL within 30 minutes (≤ 1 free dock)"}</h3>
      <p className="sub">
        Rows: train {n0(R.rows.train)} ({n0(R.rows.train_subsampled)} after subsampling negatives), validation {n0(R.rows.validation)}, test {n0(R.rows.test)}.
        {" "}Positives (base rate): train {n0(R.positives.train)} ({p1(R.positives.train / R.rows.train)}),
        validation {n0(R.positives.validation)} ({p1(R.validation_base_rate)}), test {n0(R.positives.test)} ({p1(R.test_base_rate)}).
      </p>
      <div style={{ display: "grid", gridTemplateColumns: "minmax(0, 2fr) minmax(280px, 1fr)", gap: 24, alignItems: "start" }}>
      <div>
      <div style={{ overflowX: "auto" }}>
        <table className="t">
          <thead><tr><th>Model</th><th>Precision</th><th>Recall</th><th>F1</th><th>PR-AUC</th><th>TP</th><th>FP</th><th>FN</th><th>TN</th></tr></thead>
          <tbody>
            {rows.map(({ k, name, m, t }) => (
              <tr key={k}>
                <td><b>{name}</b></td>
                <td>{p0(t.precision)}</td><td>{p0(t.recall)}</td>
                <td className={t.f1 === bestF1 ? "best" : ""}>{t.f1 === null || t.f1 === undefined ? "–" : t.f1.toFixed(3)}</td>
                <td className={m.pr_auc === bestAp ? "best" : ""}>{m.pr_auc === null || m.pr_auc === undefined ? "–" : m.pr_auc.toFixed(3)}</td>
                <td>{n0(t.confusion.tp)}</td><td>{n0(t.confusion.fp)}</td><td>{n0(t.confusion.fn)}</td><td>{n0(t.confusion.tn)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="dim" style={{ fontSize: 12, margin: "8px 0 0" }}>Green: best F1 and best PR-AUC in this table. Thresholds for the scored models were chosen on the validation block, then applied unchanged to test.</p>
      </div>
      <div>
      <h4 className="dim" style={{ margin: "0 0 4px", fontSize: 12, textTransform: "uppercase", letterSpacing: 0.5 }}>F1 on the test block</h4>
      <div style={{ height: 220 }}>
        <ResponsiveContainer>
          <BarChart data={bars} layout="vertical" margin={{ top: 0, right: 48, bottom: 0, left: 8 }}>
            <CartesianGrid stroke="rgba(148,163,184,0.12)" horizontal={false} />
            <XAxis type="number" domain={[0, 1]} ticks={[0, 0.2, 0.4, 0.6, 0.8, 1]} {...AX} tickFormatter={(v) => v.toFixed(1)} />
            <YAxis type="category" dataKey="name" width={170} {...AX} />
            <Tooltip {...TT} formatter={(v) => v.toFixed(3)} />
            <Bar dataKey="f1" radius={[0, 4, 4, 0]} isAnimationActive={false}>
              {bars.map((b) => <Cell key={b.name} fill={b.best ? "#22d3ee" : "rgba(56,189,248,0.45)"} />)}
              <LabelList dataKey="f1" position="right" formatter={(v) => v.toFixed(2)} fill="#a9b4c6" fontSize={11} />
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
      </div>
      </div>
    </div>
  );
}

function ModelComparison() {
  const { data: d, error } = usePoll("models", {}, { interval: 60_000 });
  const head = <h2 style={{ margin: "8px 0 0", fontSize: 18, display: "flex", alignItems: "center", gap: 12 }}>Model comparison <span className="chip medium">PRELIMINARY</span></h2>;
  if (error) return <>{head}<div className="card muted">{error}</div></>;
  if (!d) return <>{head}<div className="card muted">Loading…</div></>;
  if (!d.available) return <>{head}<div className="card muted">{d.message}</div></>;
  const ranges = d.split?.ranges ?? {};
  const acc = (lab) => {
    const c = d.results[lab]?.models?.persistence?.test?.confusion;
    const total = c ? c.tp + c.fp + c.fn + c.tn : 0;
    return total ? (c.tp + c.tn) / total : null;
  };
  return (
    <>
      {head}
      <div className="card">
        <p className="muted" style={{ marginTop: 0 }}>
          Nowcast experiment on the collected live snapshots: will a station that is not yet empty (or full) be empty (or full) at any snapshot in
          the next 30 minutes? Six approaches on the same test rows. {d.status}
        </p>
        <div className="cards-row" style={{ marginBottom: 8 }}>
          {["train", "validation", "test"].map((part) => (
            <div className="metric" key={part}>
              <div className="l">{part}</div>
              <div style={{ fontWeight: 600, marginTop: 4 }}>{nyRange(ranges[part])}</div>
              <div className="dim" style={{ fontSize: 12 }}>{n0(ranges[part]?.snapshots)} snapshots</div>
            </div>
          ))}
        </div>
        <p className="dim" style={{ fontSize: 12, margin: 0 }}>Split: {d.split?.rule}. Computed {d.computed_at ? nyDateTime(d.computed_at) : "–"} (New York).</p>
      </div>
      {["empty", "full"].filter((l) => d.results[l]).map((l) => <ModelTable key={l} label={l} R={d.results[l]} />)}
      <div className="grid-2">
        <div className="card">
          <h3>How to read these numbers</h3>
          <ul className="muted" style={{ margin: 0, paddingLeft: 20, display: "grid", gap: 8 }}>
            <li><b style={{ color: "var(--text)" }}>Precision</b>: the share of alerts that came true.</li>
            <li><b style={{ color: "var(--text)" }}>Recall</b>: the share of real events that had an alert.</li>
            <li><b style={{ color: "var(--text)" }}>F1</b>: one number combining precision and recall (high only when both are high).</li>
            <li><b style={{ color: "var(--text)" }}>PR-AUC</b>: how well a model ranks real events above non-events across all thresholds; a useless model scores about the base rate.</li>
            <li><b style={{ color: "var(--text)" }}>Why not accuracy</b>: events are rare, so a model that never alerts looks excellent. Persistence never alerts, yet it is {p1(acc("empty"))} (empty) and {p1(acc("full"))} (full) accurate on the test block.</li>
          </ul>
        </div>
        <div className="card">
          <h3>Honest note</h3>
          <p className="muted" style={{ margin: 0 }}>
            The test window was short and quiet ({nyRange(ranges.test)}, {n0(ranges.test?.snapshots)} snapshots), so the absolute
            numbers are low and the ranking may change. This comparison will be rerun on more live data; the page updates automatically when it is.
          </p>
        </div>
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
            {curve.length < 2 ? <div className="empty-state">Not enough resolved alerts yet{source === "live" ? ". Try Replay" : ""}.</div> : (
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
      <ModelComparison />
    </div></div>
  );
}
