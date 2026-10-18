import { SPEEDS, useMode } from "../lib/mode";
import { nyDateTime } from "../lib/format";

export default function ReplayBar() {
  const { timeline, idx, setIdx, playing, setPlaying, speed, setSpeed } = useMode();
  const n = timeline?.times?.length ?? 0;
  if (!n) return <div className="replay glass"><span className="muted">{timeline?.error ? "No replay yet: run scripts/make_replay.sh" : "Loading replay…"}</span></div>;
  return (
    <div className="replay glass" aria-label="Replay controls">
      <button className="play" onClick={() => setPlaying(!playing)} aria-label={playing ? "Pause" : "Play"}>{playing ? "❚❚" : "▶"}</button>
      <input type="range" min={0} max={n - 1} value={idx} onChange={(e) => { setPlaying(false); setIdx(+e.target.value); }} aria-label="Replay time" />
      <div className="speed">{SPEEDS.map((s) => <button key={s} className={speed === s ? "on" : ""} onClick={() => setSpeed(s)}>{s}×</button>)}</div>
      <div className="time">{nyDateTime(timeline.times[idx])}</div>
    </div>
  );
}
