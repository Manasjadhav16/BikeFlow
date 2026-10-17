import { lazy, Suspense } from "react";
import { NavLink, Route, Routes } from "react-router-dom";
import KpiStrip from "./components/KpiStrip";
import { DataProvider, useData } from "./lib/data";
import { useMode } from "./lib/mode";
import { age } from "./lib/format";
import LiveMap from "./pages/LiveMap";
import AlertsPage from "./pages/AlertsPage";
import HowItWorks from "./pages/HowItWorks";

const Reality = lazy(() => import("./pages/Reality"));
const Insights = lazy(() => import("./pages/Insights"));

function TopBar() {
  const { source, setSource, setPlaying } = useMode();
  const { state, resolved } = useData();
  return (
    <header className="topbar">
      <div className="brand"><span className="brand-dot" />BikeFlow <small>NYC</small></div>
      <nav className="nav">
        <NavLink to="/" end>Live map</NavLink>
        <NavLink to="/alerts">Alerts</NavLink>
        <NavLink to="/reality">Prediction vs reality</NavLink>
        <NavLink to="/insights">Insights</NavLink>
        <NavLink to="/how">How it works</NavLink>
      </nav>
      <div className="spacer" />
      <KpiStrip state={state} resolved={resolved} source={source} />
      <div className="mode-toggle" role="group" aria-label="Data source">
        <button className={source === "live" ? "on" : ""} onClick={() => { setPlaying(false); setSource("live"); }}>
          <span className={`live-dot ${state?.stale ? "stale" : ""}`} />Live
        </button>
        <button className={source === "replay" ? "on" : ""} onClick={() => setSource("replay")}>Replay</button>
      </div>
    </header>
  );
}

function Banners() {
  const { source, setSource } = useMode();
  const { state, error } = useData();
  if (source === "live" && state?.stale) {
    return <div className="banner" role="alert">Live data is {age(state.freshness_s)} old: the collector or stream may be paused.
      <button onClick={() => setSource("replay")}>Open replay</button></div>;
  }
  if (error && !state) return <div className="banner" role="alert">{error}</div>;
  return null;
}

export default function App() {
  return (
    <DataProvider>
      <div className="shell">
        <TopBar />
        <Banners />
        <Suspense fallback={<div className="page"><div className="loading">Loading…</div></div>}>
          <Routes>
            <Route path="/" element={<LiveMap />} />
            <Route path="/alerts" element={<AlertsPage />} />
            <Route path="/reality" element={<Reality />} />
            <Route path="/insights" element={<Insights />} />
            <Route path="/how" element={<HowItWorks />} />
          </Routes>
        </Suspense>
      </div>
    </DataProvider>
  );
}
