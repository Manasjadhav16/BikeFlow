"""BikeFlow dashboard API (read-only). Run from the project root:

    .venv/bin/uvicorn backend.app.main:app --port 8000

Every endpoint that shows live data takes ?source=live (default) or ?source=replay
(scripts/make_replay.sh output); replay endpoints also take ?at=<ISO time> for the time slider.
"""
import json
import subprocess
import sys
import time
from typing import Literal

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from . import logic, store

app = FastAPI(title="BikeFlow API", version="1.0")
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(CORSMiddleware, allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
                   allow_methods=["GET"], allow_headers=["*"])

Source = Literal["live", "replay"]
STALE_AFTER_S = 5 * 60
LIVE_SCORES = store.ROOT / "data" / "dashboard" / "live_alert_scores.json"
LIVE_SCORING_PID = store.ROOT / "data" / "dashboard" / "live_scoring.pid"
LIVE_SCORING_EVERY_S = 10 * 60
MIN_RESOLVED = 50


def _live_scoring_running() -> bool:
    return store.pid_alive(LIVE_SCORING_PID)


def _start_live_scoring() -> None:
    """Score the live alerts in a separate, low-priority process (never in the API process, never
    blocking a request). It only reads data/live and writes data/dashboard/live_alert_scores.json."""
    if _live_scoring_running():
        return
    LIVE_SCORES.parent.mkdir(parents=True, exist_ok=True)
    cmd = ["nice", "-n", "15", sys.executable, "-m", "src.score_alerts", "--since-restart",
           "--alerts-out", str(store.LIVE), "--status-dir", str(store.LIVE / "status"), "--output", str(LIVE_SCORES)]
    proc = subprocess.Popen(cmd, cwd=store.ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                            start_new_session=True)
    LIVE_SCORING_PID.write_text(str(proc.pid))


def _state(source: str, at: str | None):
    df, rec = store.state(source, at)
    if df.empty:
        raise HTTPException(503, f"no {source} state yet" + (" (run scripts/make_replay.sh)" if source == "replay" else
                                                             " (is the alert engine running?)"))
    recs = logic.station_records(df)
    as_of = df["as_of"].iloc[0]
    return recs, rec, as_of


@app.get("/api/state")
def get_state(source: Source = "live", at: str | None = None):
    recs, rec, as_of = _state(source, at)
    age = None if source == "replay" else round(store.now() - logic.to_epoch(as_of))
    return {"source": source, "as_of": as_of, "processed_at": (rec or {}).get("processed_at"),
            "freshness_s": age, "stale": bool(age is not None and age > STALE_AFTER_S),
            "weather": logic.weather_of(rec), "kpis": logic.kpis(recs), "stations": recs}


@app.get("/api/alerts")
def get_alerts(source: Source = "live", at: str | None = None, limit: int = Query(500, le=3000)):
    recs, rec, as_of = _state(source, at)
    al = sorted((r for r in recs if r["status"] in ("risk_empty", "risk_full")), key=lambda r: -r["p"])
    for r in al:
        r["sentence"] = logic.sentence({**r, "p_empty": r["pe"], "p_full": r["pf"]})
    return {"source": source, "as_of": as_of, "count": len(al), "weather": logic.weather_of(rec), "alerts": al[:limit]}


@app.get("/api/resolved")
def get_resolved(source: Source = "live", limit: int = Query(300, le=2000)):
    return {"source": source, **logic.resolve(source, limit),
            "caveat": "Operator vans move bikes between stations; a restock or clear-out can change the outcome "
                      "of an alert regardless of rider demand."}


def _score_summary(s: dict) -> dict:
    """Model vs simple-rule baseline, precision and recall, from a src/score_alerts.py result."""
    if not s or not s.get("evaluation_times") or "model" not in s:
        return {"enough": False, "resolved_risk_alerts": (s or {}).get("resolved_risk_alerts") or 0}
    out = {"resolved_risk_alerts": s["resolved_risk_alerts"], "evaluation_times": s["evaluation_times"],
           "station_times": s["station_times_in_service"], "base_rate": s["base_rate"]}
    for k in ("empty", "full"):
        m, b, r = s["model"][k], s["baseline_persistence"][k], s["recall"][k]
        risk = [m["risk_alerts"][lv] for lv in ("HIGH", "MEDIUM")]
        n, h = sum(x["alerts"] for x in risk), sum(x["hits"] for x in risk)
        out[k] = {
            "model_high": m["risk_alerts"]["HIGH"], "model_medium": m["risk_alerts"]["MEDIUM"],
            "model_risk_all": {"alerts": n, "hits": h, "precision": round(h / n, 4) if n else None},
            "baseline_all": b["all"], "baseline_exactly_2": b["exactly 2"],
            "events": r["events"], "model_recall": r["model_recall"], "baseline_recall": r["baseline_recall"],
        }
    out["enough"] = out["resolved_risk_alerts"] >= MIN_RESOLVED
    return out


@app.get("/api/scores")
def get_scores(source: Source = "replay"):
    """Precision and recall of the alerts vs the "<= 2 bikes (or docks)" rule, src/score_alerts.py logic."""
    if source == "replay":
        p = store.METRICS / "replay_alert_scores.json"
        s = json.loads(p.read_text()) if p.exists() else {}
        meta = store.replay_meta()
        return {"source": "replay", "label": "one recorded 3-hour evening rush window",
                "window": {"start": meta.get("window_start"), "end": meta.get("window_end")},
                "since": s.get("since"), "data_end": s.get("data_end"), **_score_summary(s)}
    s = json.loads(LIVE_SCORES.read_text()) if LIVE_SCORES.exists() else {}
    age = time.time() - store.mtime(LIVE_SCORES) if LIVE_SCORES.exists() else None
    if age is None or age > LIVE_SCORING_EVERY_S:
        _start_live_scoring()
    return {"source": "live", "label": "alerts from the live engine since its last restart",
            "since": s.get("since"), "data_end": s.get("data_end"), "computed_at": s.get("computed_at"),
            "computing": _live_scoring_running(), "min_resolved": MIN_RESOLVED,
            "note": s.get("note"), **_score_summary(s)}


@app.get("/api/models")
def get_models():
    """PRELIMINARY nowcast model comparison, read fresh from outputs/metrics/nowcast_metrics.json on every
    request (no cache), so rerunning `python -m src.nowcast` updates the dashboard."""
    p = store.METRICS / "nowcast_metrics.json"
    if not p.exists():
        return {"available": False,
                "message": "Model comparison not available yet: run `python -m src.nowcast` to create "
                           "outputs/metrics/nowcast_metrics.json."}
    try:
        d = json.loads(p.read_text())
    except ValueError:
        return {"available": False, "message": "nowcast_metrics.json is being written or is not valid JSON; try again."}
    for r in d.get("results", {}).values():  # PR curves are only needed for the static chart
        for m in r.get("models", {}).values():
            m.pop("pr_curve", None)
    return {"available": True, **d}


@app.get("/api/rebalance")
def get_rebalance(source: Source = "live", at: str | None = None):
    recs, _, as_of = _state(source, at)
    moves = logic.rebalance(recs)
    return {"source": source, "as_of": as_of, "count": len(moves), "suggestions": moves,
            "note": "Suggestion based on the model's expected demand (historical average x weather) for the next "
                    "30 minutes: each at-risk station is paired with the nearest station (<= 2 km) that keeps at "
                    "least 5 bikes above its own expected departures. Greedy, no optimisation."}


@app.get("/api/station/{station_id}")
def get_station(station_id: str, source: Source = "live", at: str | None = None):
    recs, _, as_of = _state(source, at)
    d = logic.station_detail(source, recs, station_id, logic.to_epoch(at or as_of))
    if d is None:
        raise HTTPException(404, "unknown station")
    return {"source": source, "as_of": as_of, **d}


@app.get("/api/ticker")
def get_ticker(source: Source = "live", at: str | None = None, limit: int = Query(30, le=200)):
    return {"source": source, "items": logic.ticker(source, logic.to_epoch(at), limit)}


@app.get("/api/pulse")
def get_pulse(source: Source = "live", at: str | None = None):
    return {"source": source, **logic.pulse(source, logic.to_epoch(at))}


@app.get("/api/timeline")
def get_timeline():
    tl = store.replay_timeline()
    if not tl:
        raise HTTPException(503, "no replay yet: run scripts/make_replay.sh")
    return {"meta": store.replay_meta(), "times": [b["as_of"] for b in tl]}


@app.get("/api/insights")
def get_insights():
    def build():
        m = store.METRICS
        eda = json.loads((m / "eda_summary.json").read_text())
        dis = json.loads((m / "disrupted_days.json").read_text())
        daily = pd.concat([pd.read_parquet(p) for p in sorted((store.PROCESSED / "demand_parts").glob("*/daily.parquet"))])
        daily = daily.groupby("date", as_index=False)["departures"].sum().sort_values("date")
        exp = pd.read_parquet(store.PROCESSED / "reference" / "expected_demand.parquet",
                              columns=["day_of_week", "hour", "exp_departures"])
        heat = exp.groupby(["day_of_week", "hour"])["exp_departures"].sum().round(0)
        mr = json.loads((m / "model_results.json").read_text())
        ae = json.loads((m / "alert_eval.json").read_text())
        fe = json.loads((m / "flow_eval.json").read_text())
        s2 = mr["results"]["test"]["excl_disrupted"]
        stage2 = [{"segment": seg, "rows": v["rows"],
                   **{k: {"mae": x["mae"], "rmse": x["rmse"]} for k, x in v.items() if k != "rows"}}
                  for seg, v in s2.items()]
        t2b = fe["test"]["excl_disrupted"]
        events = [{"event": k.replace("strong_", "").replace("_k", " k="), "base_rate": v["model"]["base_rate"],
                   **{a: {"ap": v[a]["average_precision"], "r60": v[a]["recall_at_60_precision"],
                          "r70": v[a]["recall_at_70_precision"]} for a in fe["approaches"]}}
                  for k, v in t2b.items() if k.startswith("strong_")]
        return {
            "rides_per_month": eda["rides_per_month"],
            "daily": [{"date": str(d), "rides": int(r)} for d, r in zip(daily["date"], daily["departures"])],
            "disrupted_days": dis["auto_days"],
            "hourly_profile": eda["hourly_profile_avg_departures_per_day"],
            "weekday_vs_weekend": eda["weekday_vs_weekend"],
            "busiest_stations": eda["busiest_stations"][:15],
            "heatmap": [{"dow": int(d), "hour": int(h), "departures": float(v)} for (d, h), v in heat.items()],
            "stage2": {"test_month": mr["results"]["test"]["month"], "segments": stage2,
                       "always_zero": ae["test"]["reference_always_zero"], "best": mr["best_model_by_validation_mae"]},
            "stage2b": {"test_month": fe["test"]["month"], "events": events,
                        "deviance": t2b["poisson_deviance"], "approaches": fe["approaches"]},
            "summary_numbers": {"days": eda["days"], "stations": eda["stations"],
                                "total_departures": eda["total_departures"]},
        }

    paths = [store.METRICS / f for f in ("eda_summary.json", "model_results.json", "flow_eval.json")]
    return store.cached(("insights",), tuple(store.mtime(p) for p in paths), build)


@app.get("/api/health")
def get_health():
    out = {}
    for source in ("live", "replay"):
        b = store.batches(source)
        last = b[-1] if b else None
        out[source] = None if not last else {
            "last_batch_id": last["batch_id"], "as_of": last["as_of"], "processed_at": last["processed_at"],
            "batch_seconds": last.get("driver_seconds"), "rows_in_batch": last.get("station_updates"),
            "in_service": last.get("in_service"), "batches_total": len(b),
            "age_s": round(store.now() - logic.to_epoch(last["processed_at"])) if source == "live" else None,
        }
    poll = store.collector_last_poll()
    out["collector"] = None if not poll else {
        "last_poll": poll["time"], "age_s": round(store.now() - logic.to_epoch(poll["time"])),
        "http_status": poll.get("http_status"), "latency_ms": poll.get("latency_ms"),
        "stations": poll.get("stations"), "kafka": poll.get("kafka")}
    out["processes"] = {"collector": store.pid_alive(store.LIVE / "collector.pid"),
                        "stream": store.pid_alive(store.LIVE / "stream_alerts.pid")}
    out["stale"] = bool(out["live"] and out["live"]["age_s"] is not None and out["live"]["age_s"] > STALE_AFTER_S)
    return out
