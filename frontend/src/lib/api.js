// Thin API client. The Vite dev server proxies /api to the FastAPI backend on :8000.
export async function api(path, params = {}) {
  const q = new URLSearchParams(Object.entries(params).filter(([, v]) => v !== undefined && v !== null && v !== ""));
  const r = await fetch(`/api/${path}${q.toString() ? `?${q}` : ""}`);
  if (!r.ok) {
    let detail = r.statusText;
    try { detail = (await r.json()).detail || detail; } catch { /* not JSON */ }
    throw new Error(`${r.status}: ${detail}`);
  }
  return r.json();
}
