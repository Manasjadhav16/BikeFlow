"""Headless end-to-end check of the dashboard (Playwright + Chromium).

Loads every page, saves screenshots to docs/screenshots/, records browser console errors,
measures map smoothness (frames per second while panning and zooming with every station
drawn) and the memory of the dashboard next to the running pipeline.

Needs the dashboard running (scripts/start_dashboard.sh).
    .venv/bin/python scripts/dashboard_e2e.py
"""
import json
import subprocess
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parent.parent
SHOTS = ROOT / "docs" / "screenshots"
URL = "http://127.0.0.1:5173"

FPS_JS = """async ([ms, action]) => {
  const m = window.__bfmap; const frames = []; let last = performance.now(); let run = true;
  const tick = (t) => { frames.push(t - last); last = t; if (run) requestAnimationFrame(tick); };
  requestAnimationFrame(tick);
  const t0 = performance.now();
  if (action === 'pan') { const id = setInterval(() => m.panBy([60, 30], {animate: false}), 50);
    await new Promise(r => setTimeout(r, ms)); clearInterval(id); }
  else { m.setZoom(12); await new Promise(r => setTimeout(r, 300));
    for (let z = 12; z <= 15; z += 0.5) { m.setZoom(z); await new Promise(r => setTimeout(r, ms / 8)); }
    for (let z = 15; z >= 13; z -= 0.5) { m.setZoom(z); await new Promise(r => setTimeout(r, ms / 8)); } }
  run = false; const dt = (performance.now() - t0) / 1000;
  const sorted = frames.slice(2).sort((a, b) => a - b);
  return { fps: +(frames.length / dt).toFixed(1), p95_frame_ms: +sorted[Math.floor(sorted.length * 0.95)].toFixed(1),
           long_frames_over_50ms: frames.filter(f => f > 50).length, frames: frames.length };
}"""


def rss_report() -> dict:
    out = subprocess.run(["ps", "-axo", "pid=,rss=,command="], capture_output=True, text=True).stdout
    groups = {"dashboard backend (uvicorn)": "uvicorn backend.app.main", "dashboard frontend (vite)": "vite/bin/vite.js",
              "headless chromium (test)": "chrome-headless-shell", "alert engine (Spark JVM)": "bikeflow-stream-alerts",
              "alert engine (python driver)": "-m src.stream_alerts", "Kafka broker": "kafka.Kafka",
              "collector": "-m src.collector"}
    res = {}
    for name, pat in groups.items():
        kb = sum(int(l.split()[1]) for l in out.splitlines() if pat in l and "grep" not in l)
        res[name] = round(kb / 1024)
    java = [l for l in out.splitlines() if "java" in l and "org.apache.spark.deploy.SparkSubmit" in l]
    res["Spark JVMs (all)"] = round(sum(int(l.split()[1]) for l in java) / 1024)
    return res


def main() -> None:
    SHOTS.mkdir(parents=True, exist_ok=True)
    errors, report = [], {"screenshots": [], "pages": {}}
    with sync_playwright() as p:
        b = p.chromium.launch()
        page = b.new_page(viewport={"width": 1920, "height": 1080}, device_scale_factor=1)
        page.on("console", lambda m: m.type == "error" and errors.append(m.text))
        page.on("pageerror", lambda e: errors.append(str(e)))

        def shot(name):
            path = SHOTS / name
            page.screenshot(path=str(path))
            report["screenshots"].append(str(path.relative_to(ROOT)))

        # 1. live map
        t0 = time.time()
        page.goto(URL)
        page.wait_for_function("window.__bfmap && document.querySelectorAll('.leaflet-tile-loaded').length > 6", timeout=30000)
        page.wait_for_selector(".kpi .v", timeout=30000)
        page.wait_for_timeout(1500)  # count-up animation + tiles
        report["pages"]["map_load_s"] = round(time.time() - t0, 2)
        report["stations_on_map"] = page.evaluate("Object.values(window.__bfmap._layers).filter(l => l._radius !== undefined && l.options.renderer).length")
        shot("01_live_map.png")
        report["smoothness_live"] = {"pan": page.evaluate(FPS_JS, [3000, "pan"]), "zoom": page.evaluate(FPS_JS, [3000, "zoom"])}

        # 2. replay mode (rush hour), play a few steps
        page.click("text=Replay")
        page.wait_for_selector(".replay input[type=range]", timeout=30000)
        page.wait_for_timeout(2500)
        page.click(".replay .play")
        page.wait_for_timeout(3000)
        page.click(".replay .play")
        page.wait_for_timeout(1500)
        page.evaluate("() => { window.__bfmap.setView([40.735, -73.99], 13.5); }")
        page.wait_for_timeout(1500)
        shot("02_replay_mode.png")
        report["smoothness_replay"] = {"pan": page.evaluate(FPS_JS, [3000, "pan"])}

        # 3. station drawer (top alert in the right panel)
        page.evaluate("() => { window.__bfmap.setView([40.735, -73.99], 13.5); }")
        page.click(".arow >> nth=0")
        page.wait_for_selector(".drawer svg", timeout=30000)
        page.wait_for_timeout(2000)  # fly-to animation
        shot("03_station_drawer.png")
        report["drawer_sentence"] = page.inner_text(".drawer .big-sentence")
        page.click(".drawer .close")

        # 4-7. other pages (replay data: richer than a quiet live minute)
        for path, name, wait in (("/alerts", "04_alerts.png", ".vt-row"), ("/reality", "05_prediction_vs_reality.png", ".recharts-line"),
                                 ("/insights", "06_insights.png", ".recharts-bar-rectangle"), ("/how", "07_how_it_works.png", ".arch svg")):
            t0 = time.time()
            page.click(f"a[href='{path}']")
            page.wait_for_selector(wait, timeout=30000)
            page.wait_for_timeout(1500)
            report["pages"][path] = round(time.time() - t0, 2)
            if path == "/insights":
                page.screenshot(path=str(SHOTS / name), full_page=False)
                page.evaluate("() => { document.querySelector('.page').scrollTo(0, 1000); }")
                page.wait_for_timeout(800)
                page.screenshot(path=str(SHOTS / "06b_insights_models.png"))
                report["screenshots"] += [f"docs/screenshots/{name}", "docs/screenshots/06b_insights_models.png"]
            else:
                shot(name)

        # 8. laptop / projector size, live map
        page.set_viewport_size({"width": 1366, "height": 768})
        page.click("a[href='/']")
        page.click("text=Live")
        page.wait_for_timeout(2500)
        shot("08_live_map_1366x768.png")
        report["memory_mb"] = rss_report()
        b.close()
    report["console_errors"] = errors
    (ROOT / "outputs" / "metrics" / "dashboard_e2e.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
