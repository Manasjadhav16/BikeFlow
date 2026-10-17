import { useEffect, useRef } from "react";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import { STATUS, pct } from "../lib/format";

const CENTER = [40.733, -73.975];
// Standard OpenStreetMap tiles (no API key), darkened with a CSS filter on the tile pane only
// (see styles.css), so station markers keep their true colours.
const TILES = "https://tile.openstreetmap.org/{z}/{x}/{y}.png";
const ATTRIBUTION = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors';

function radius(s, zoom) {
  const scale = zoom >= 16 ? 1.9 : zoom >= 15 ? 1.55 : zoom >= 14 ? 1.25 : zoom <= 11 ? 0.65 : zoom <= 12 ? 0.85 : 1;
  let r;
  if (s.status === "risk_empty" || s.status === "risk_full") r = 4 + 7 * (s.p ?? 0);
  else if (s.status === "empty_now" || s.status === "full_now") r = 4;
  else if (s.status === "out_of_service") r = 2;
  else r = 2.6;
  return r * scale;
}

function tip(s) {
  return `<b>${s.name}</b><br/>${STATUS[s.status]?.label ?? s.status} · ${s.bikes} bikes · ${s.docks} docks<br/>` +
    `P(empty) ${pct(s.pe)} · P(full) ${pct(s.pf)}<br/>next 30 min: ${s.ed} out / ${s.ea} in · weather ×${s.wf}`;
}

/**
 * All stations on one Leaflet canvas renderer (no DOM node per station). Markers are created once
 * and restyled on every data update; only HIGH alerts get a (DOM) pulsing ring.
 */
export default function MapView({ stations, visible, selected, focus, onSelect }) {
  const el = useRef(null);
  const map = useRef(null);
  const renderer = useRef(null);
  const markers = useRef(new Map());
  const rings = useRef(null);
  const selRing = useRef(null);
  const latest = useRef({ stations: [], visible, onSelect });
  latest.current = { stations, visible, onSelect };

  useEffect(() => {
    const m = L.map(el.current, { preferCanvas: true, zoomControl: false, minZoom: 10, maxZoom: 18, zoomSnap: 0.5 })
      .setView(CENTER, 13);
    L.control.zoom({ position: "bottomright" }).addTo(m);
    L.tileLayer(TILES, { attribution: ATTRIBUTION, maxZoom: 19, className: "bf-tiles" }).addTo(m);
    renderer.current = L.canvas({ padding: 0.4, tolerance: 6 });
    rings.current = L.layerGroup().addTo(m);
    m.on("zoomend", () => restyle());
    map.current = m;
    window.__bfmap = m; // for the headless performance check
    return () => { m.remove(); map.current = null; markers.current.clear(); };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function restyle() {
    const m = map.current;
    if (!m) return;
    const zoom = m.getZoom();
    const { stations: list, visible: vis } = latest.current;
    const seen = new Set();
    for (const s of list) {
      seen.add(s.id);
      const color = STATUS[s.status]?.color ?? "#7fa392";
      const style = {
        radius: radius(s, zoom), fillColor: color, color: s.status === "ok" ? "rgba(0,0,0,0.35)" : "rgba(255,255,255,0.55)",
        weight: s.status === "ok" || s.status === "out_of_service" ? 0.5 : 1, fillOpacity: s.status === "ok" ? 0.75 : 0.95,
      };
      let mk = markers.current.get(s.id);
      if (!mk) {
        mk = L.circleMarker([s.lat, s.lng], { renderer: renderer.current, ...style, bubblingMouseEvents: false });
        mk.bindTooltip("", { className: "bf-tip", direction: "top", offset: [0, -6] });
        mk.on("click", () => latest.current.onSelect?.(mk.__s));
        markers.current.set(s.id, mk);
      } else {
        mk.setStyle(style);
        mk.setRadius(style.radius);
      }
      mk.__s = s;
      mk.setTooltipContent(tip(s));
      const show = vis.has(s.status);
      if (show && !m.hasLayer(mk)) mk.addTo(m);
      if (!show && m.hasLayer(mk)) mk.remove();
    }
    for (const [id, mk] of markers.current) if (!seen.has(id)) { mk.remove(); markers.current.delete(id); }
    // pulsing rings: HIGH forward-looking alerts only (a handful of DOM nodes)
    rings.current.clearLayers();
    for (const s of list) {
      if (s.level === "HIGH" && vis.has(s.status)) {
        const c = STATUS[s.status].color;
        L.marker([s.lat, s.lng], {
          interactive: false, keyboard: false,
          icon: L.divIcon({ className: "pulse-ring", html: `<span style="--c:${c}"></span><span style="--c:${c}"></span>`, iconSize: [0, 0] }),
        }).addTo(rings.current);
      }
    }
  }

  useEffect(restyle, [stations, visible]);

  useEffect(() => {
    const m = map.current;
    if (!m) return;
    if (selRing.current) { selRing.current.remove(); selRing.current = null; }
    const s = stations.find((x) => x.id === selected);
    if (s) {
      selRing.current = L.circleMarker([s.lat, s.lng], {
        renderer: renderer.current, radius: 14, color: "#ffffff", weight: 2, fill: false, interactive: false, dashArray: "3 3",
      }).addTo(m);
    }
  }, [selected, stations]);

  useEffect(() => {
    if (focus && map.current) map.current.flyTo([focus.lat, focus.lng], Math.max(map.current.getZoom(), 15.5), { duration: 1.1 });
  }, [focus]);

  return <div id="map" ref={el} />;
}
