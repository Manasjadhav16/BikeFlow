export const STATUS = {
  empty_now: { label: "Empty now", color: "#ef4444" },
  full_now: { label: "Full now", color: "#3b82f6" },
  risk_empty: { label: "Risk of empty", color: "#f59e0b" },
  risk_full: { label: "Risk of full", color: "#a855f7" },
  ok: { label: "OK", color: "#7fa392" },
  out_of_service: { label: "Out of service", color: "#475569" },
};
export const STATUS_ORDER = ["empty_now", "full_now", "risk_empty", "risk_full", "ok", "out_of_service"];

const NY = "America/New_York";
export const nyTime = (iso, opts = {}) =>
  iso ? new Date(iso).toLocaleTimeString("en-US", { timeZone: NY, hour: "2-digit", minute: "2-digit", hour12: false, ...opts }) : "–";
export const nyDateTime = (iso) =>
  iso ? new Date(iso).toLocaleString("en-US", { timeZone: NY, weekday: "short", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", hour12: false }) : "–";
export const pct = (p, d = 0) => (p === null || p === undefined ? "–" : `${(p * 100).toFixed(d)}%`);
export const int = (n) => (n === null || n === undefined ? "–" : Math.round(n).toLocaleString("en-US"));
export function age(s) {
  if (s === null || s === undefined) return "–";
  if (s < 60) return `${Math.round(s)}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${Math.round(s % 60)}s`;
  return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
}
export function weatherLabel(w) {
  if (!w || w.temperature_2m === null || w.temperature_2m === undefined) return "–";
  const rain = w.precipitation > 0 ? `${w.precipitation} mm/h` : "dry";
  return `${Math.round(w.temperature_2m)}°C · ${rain}`;
}
