import { useEffect, useRef, useState } from "react";
import { api } from "./api";

export const POLL_MS = 30_000;

/** Fetch `path` now and every POLL_MS while the tab is visible; refetch when deps change. */
export function usePoll(path, params, { enabled = true, interval = POLL_MS } = {}) {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const key = JSON.stringify([path, params]);
  const seq = useRef(0);
  useEffect(() => {
    if (!enabled) return undefined;
    let timer;
    const run = async () => {
      const mine = ++seq.current;
      try {
        const d = await api(path, params);
        if (mine === seq.current) { setData(d); setError(null); }
      } catch (e) {
        if (mine === seq.current) setError(e.message);
      }
    };
    run();
    if (interval) {
      timer = setInterval(() => { if (document.visibilityState === "visible") run(); }, interval);
    }
    return () => clearInterval(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, enabled, interval]);
  return { data, error };
}

/** Animate a number towards `value` (count-up) with requestAnimationFrame. */
export function useCountUp(value, ms = 700) {
  const [shown, setShown] = useState(value ?? 0);
  const from = useRef(value ?? 0);
  useEffect(() => {
    if (value === null || value === undefined || Number.isNaN(value)) return undefined;
    const start = performance.now();
    const a = from.current;
    let raf;
    const step = (now) => {
      const k = Math.min(1, (now - start) / ms);
      const e = 1 - Math.pow(1 - k, 3);
      setShown(a + (value - a) * e);
      if (k < 1) raf = requestAnimationFrame(step);
      else from.current = value;
    };
    raf = requestAnimationFrame(step);
    return () => cancelAnimationFrame(raf);
  }, [value, ms]);
  return shown;
}
