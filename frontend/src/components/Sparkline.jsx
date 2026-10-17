/** Tiny SVG step chart of bikes available (no chart library needed). */
export default function Sparkline({ points, width = 340, height = 72, cap }) {
  if (!points || points.length < 2) return <div className="dim" style={{ fontSize: 12 }}>Not enough recorded history yet.</div>;
  const ts = points.map((p) => new Date(p.t).getTime());
  const t0 = ts[0], t1 = ts[ts.length - 1] || t0 + 1;
  const maxV = Math.max(cap || 0, ...points.map((p) => p.bikes), 1);
  const x = (t) => ((t - t0) / Math.max(1, t1 - t0)) * (width - 8) + 4;
  const y = (v) => height - 14 - (v / maxV) * (height - 22);
  let d = `M ${x(ts[0])} ${y(points[0].bikes)}`;
  for (let i = 1; i < points.length; i++) d += ` H ${x(ts[i])} V ${y(points[i].bikes)}`;
  const area = `${d} V ${height - 14} H ${x(ts[0])} Z`;
  const last = points[points.length - 1];
  const fmt = (t) => new Date(t).toLocaleTimeString("en-US", { timeZone: "America/New_York", hour: "2-digit", minute: "2-digit", hour12: false });
  return (
    <svg width="100%" viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Bikes available, last hours">
      <defs><linearGradient id="spk" x1="0" x2="0" y1="0" y2="1"><stop offset="0" stopColor="#22d3ee" stopOpacity="0.35" /><stop offset="1" stopColor="#22d3ee" stopOpacity="0" /></linearGradient></defs>
      <line x1="4" x2={width - 4} y1={y(1)} y2={y(1)} stroke="#ef4444" strokeDasharray="3 3" strokeOpacity="0.6" />
      <path d={area} fill="url(#spk)" />
      <path d={d} fill="none" stroke="#22d3ee" strokeWidth="2" />
      <circle cx={x(t1)} cy={y(last.bikes)} r="3.5" fill="#22d3ee" />
      <text x="4" y={height - 2} fontSize="10" fill="#7d8aa0">{fmt(t0)}</text>
      <text x={width - 4} y={height - 2} fontSize="10" fill="#7d8aa0" textAnchor="end">{fmt(t1)} NY</text>
      <text x={width - 4} y="10" fontSize="10" fill="#a9b4c6" textAnchor="end">{last.bikes} bikes</text>
    </svg>
  );
}
