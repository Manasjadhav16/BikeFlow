import { createContext, useContext, useEffect, useRef, useState } from "react";
import { api } from "./api";
import { usePoll } from "./hooks";
import { useMode } from "./mode";

const DataContext = createContext(null);

/** Shared data: live = polled every 30 s; replay = fetched when the slider time changes. */
export function DataProvider({ children }) {
  const { source, at } = useMode();
  const live = source === "live";
  const [replayState, setReplayState] = useState(null);
  const [replayErr, setReplayErr] = useState(null);
  const seq = useRef(0);

  const liveState = usePoll("state", { source: "live" }, { enabled: live });
  const resolved = usePoll("resolved", { source }, { interval: live ? 30_000 : 0 });
  const ticker = usePoll("ticker", { source, at }, { enabled: live || !!at, interval: live ? 30_000 : 0 });
  const pulse = usePoll("pulse", { source, at }, { enabled: live || !!at, interval: live ? 60_000 : 0 });

  useEffect(() => {
    if (live || !at) return;
    const mine = ++seq.current;
    api("state", { source: "replay", at })
      .then((d) => { if (mine === seq.current) { setReplayState(d); setReplayErr(null); } })
      .catch((e) => { if (mine === seq.current) setReplayErr(e.message); });
  }, [live, at]);

  const state = live ? liveState.data : replayState;
  const error = live ? liveState.error : replayErr;
  return (
    <DataContext.Provider value={{ state, error, resolved: resolved.data, ticker: ticker.data, pulse: pulse.data }}>
      {children}
    </DataContext.Provider>
  );
}
export const useData = () => useContext(DataContext);
