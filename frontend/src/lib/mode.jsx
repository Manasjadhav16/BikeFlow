import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import { api } from "./api";

const ModeContext = createContext(null);
export const SPEEDS = [1, 2, 4, 8]; // replay steps (~one feed refresh, ~2 min of data) per second

/** Live/Replay mode, replay timeline + playback, selected station and map focus requests. */
export function ModeProvider({ children }) {
  const [source, setSource] = useState("live");
  const [timeline, setTimeline] = useState(null);
  const [idx, setIdx] = useState(0);
  const [playing, setPlaying] = useState(false);
  const [speed, setSpeed] = useState(2);
  const [selected, setSelected] = useState(null);
  const [focus, setFocus] = useState(null); // {lat, lng, id, n}
  const focusN = useRef(0);

  useEffect(() => {
    if (source !== "replay" || timeline) return;
    api("timeline").then((t) => { setTimeline(t); setIdx(Math.floor(t.times.length * 0.55)); }).catch(() => setTimeline({ times: [], error: true }));
  }, [source, timeline]);

  useEffect(() => {
    if (!playing || source !== "replay" || !timeline?.times?.length) return undefined;
    const t = setInterval(() => {
      setIdx((i) => {
        if (i + 1 >= timeline.times.length) { setPlaying(false); return i; }
        return i + 1;
      });
    }, 1000 / speed);
    return () => clearInterval(t);
  }, [playing, speed, source, timeline]);

  const at = source === "replay" && timeline?.times?.length ? timeline.times[idx] : undefined;
  const flyTo = useCallback((s) => { focusN.current += 1; setFocus({ ...s, n: focusN.current }); setSelected(s.id); }, []);

  const value = useMemo(() => ({
    source, setSource, timeline, idx, setIdx, playing, setPlaying, speed, setSpeed, at,
    selected, setSelected, focus, flyTo,
  }), [source, timeline, idx, playing, speed, at, selected, focus, flyTo]);
  return <ModeContext.Provider value={value}>{children}</ModeContext.Provider>;
}

export const useMode = () => useContext(ModeContext);
