const NODES = [
  { id: "gbfs", x: 40, y: 150, w: 150, t: "Citi Bike GBFS", s: "station_status · ~2 min" },
  { id: "col", x: 240, y: 150, w: 150, t: "Collector", s: "Python · every 60 s" },
  { id: "kafka", x: 440, y: 150, w: 150, t: "Kafka", s: "topic station_status" },
  { id: "spark", x: 640, y: 150, w: 190, t: "Spark Structured Streaming", s: "state per station · 60 s batches" },
  { id: "sinks", x: 880, y: 150, w: 160, t: "Alerts", s: "Kafka · Parquet · latest state" },
  { id: "api", x: 1090, y: 150, w: 140, t: "FastAPI", s: "read-only /api" },
  { id: "ui", x: 1090, y: 300, w: 140, t: "Dashboard", s: "React · Leaflet" },
  { id: "hist", x: 560, y: 20, w: 180, t: "Batch history", s: "12 months of trips → expected demand" },
  { id: "wx", x: 780, y: 20, w: 150, t: "Weather", s: "Open-Meteo · 15-min cache" },
  { id: "parq", x: 240, y: 300, w: 150, t: "Status Parquet", s: "ground truth for scoring" },
];
const EDGES = [
  ["M190,180 H240", 0], ["M390,180 H440", 0.3], ["M590,180 H640", 0.6], ["M830,180 H880", 0.9], ["M1040,180 H1090", 1.2],
  ["M1160,210 V300", 1.5], ["M650,80 V150", 0.2], ["M855,80 C855,120 760,120 760,150", 0.8], ["M315,210 V300", 0.4],
];

export default function HowItWorks() {
  const box = (n) => (
    <g key={n.id}>
      <rect x={n.x} y={n.y} width={n.w} height="60" rx="12" fill="#101622" stroke="rgba(148,163,184,0.35)" />
      <text x={n.x + n.w / 2} y={n.y + 26} textAnchor="middle" fill="#e8edf5" fontSize="14" fontWeight="700">{n.t}</text>
      <text x={n.x + n.w / 2} y={n.y + 45} textAnchor="middle" fill="#7d8aa0" fontSize="11">{n.s}</text>
    </g>
  );
  return (
    <div className="page"><div className="page-inner">
      <div>
        <h1>How it works</h1>
        <p className="lede">Live station counts stream through Kafka into a Spark Structured Streaming job. Every minute, it compares each
          station’s bikes and docks with the demand expected in the next 30 minutes and raises an alert when running empty or full becomes likely.</p>
      </div>
      <div className="card arch">
        <svg viewBox="0 0 1260 380" role="img" aria-label="Architecture: collector to Kafka to Spark Structured Streaming to alerts to the dashboard">
          <defs>
            <marker id="arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="rgba(148,163,184,0.6)" /></marker>
          </defs>
          {EDGES.map(([d], i) => <path key={`e${i}`} id={`p${i}`} d={d} fill="none" stroke="rgba(148,163,184,0.45)" strokeWidth="2" markerEnd="url(#arr)" />)}
          {EDGES.map(([, delay], i) => (
            <circle key={`d${i}`} r="4" fill={i >= 6 && i <= 7 ? "#f59e0b" : "#22d3ee"}>
              <animateMotion dur="1.8s" begin={`${delay}s`} repeatCount="indefinite"><mpath href={`#p${i}`} /></animateMotion>
            </circle>
          ))}
          {NODES.map(box)}
          <text x="660" y="250" fill="#7d8aa0" fontSize="11">watermark 5 min · applyInPandasWithState · foreachBatch</text>
        </svg>
      </div>
      <div className="grid-2">
        <div className="card formula">
          <h3>The alert, in plain words</h3>
          <ol>
            <li><b>Now</b> is the newest report time in the stream (so a replay behaves exactly like live).</li>
            <li><b>Expected flow:</b> for the next 30 minutes, take the station’s historical average departures and arrivals for this weekday and quarter-hour.</li>
            <li><b>Weather:</b> multiply by a factor for the current rain and temperature (e.g. ×0.43 in moderate rain at 10–20 °C, ×1.37 on a dry summer evening).</li>
            <li><b>Chance of running empty:</b> treat departures and arrivals as random counts and ask how likely it is that departures exceed arrivals by at least <i>bikes now − 1</i> (a Skellam distribution). Same for full, with docks.</li>
            <li><b>Level:</b> HIGH at 70% or more, MEDIUM at 50% or more. Out-of-service and silent stations never alert; stations already at ≤ 1 bike or dock are shown as empty/full now, not as predictions.</li>
          </ol>
        </div>
        <div className="card">
          <h3>Assumptions worth knowing</h3>
          <ul className="muted" style={{ margin: 0, paddingLeft: 20, display: "grid", gap: 8 }}>
            <li><b style={{ color: "var(--text)" }}>Operator vans</b> move bikes between stations; the model only knows rider trips.</li>
            <li>The feed refreshes about <b style={{ color: "var(--text)" }}>every 2 minutes</b>, so the map can lag reality by that much.</li>
            <li>The demand model is the <b style={{ color: "var(--text)" }}>historical average</b>: in testing it beat machine-learning models on these alert events.</li>
            <li>Departures and arrivals are assumed independent; group rides make the tails less certain than the numbers suggest.</li>
          </ul>
        </div>
      </div>
    </div></div>
  );
}
