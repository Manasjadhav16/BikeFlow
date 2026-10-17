import { useMemo, useState } from "react";
import MapView from "../components/MapView";
import FilterPanel from "../components/FilterPanel";
import AlertPanel from "../components/AlertPanel";
import StationDrawer from "../components/StationDrawer";
import Ticker from "../components/Ticker";
import CityPulse from "../components/CityPulse";
import ReplayBar from "../components/ReplayBar";
import { STATUS_ORDER } from "../lib/format";
import { useData } from "../lib/data";
import { useMode } from "../lib/mode";

export default function LiveMap() {
  const { state, ticker, pulse } = useData();
  const { source, at, selected, setSelected, focus, flyTo } = useMode();
  const [visible, setVisible] = useState(() => new Set(STATUS_ORDER));
  const stations = useMemo(() => state?.stations ?? [], [state]);
  const alerts = useMemo(() => stations.filter((s) => s.status === "risk_empty" || s.status === "risk_full").sort((a, b) => b.p - a.p), [stations]);
  const pick = (s) => {
    const st = stations.find((x) => x.id === (s.id ?? s.station_id));
    if (st) flyTo({ id: st.id, lat: st.lat, lng: st.lng });
  };
  return (
    <div className="mapwrap">
      <MapView stations={stations} visible={visible} selected={selected} focus={focus} onSelect={(s) => setSelected(s.id)} />
      <FilterPanel stations={stations} visible={visible} setVisible={setVisible} onPick={pick} />
      {selected
        ? <StationDrawer id={selected} source={source} at={at} onClose={() => setSelected(null)} />
        : <AlertPanel alerts={alerts} selected={selected} onPick={pick} asOf={state?.as_of} />}
      <CityPulse pulse={pulse} />
      {source === "replay" && <ReplayBar />}
      <Ticker items={ticker?.items} onPick={pick} />
    </div>
  );
}
