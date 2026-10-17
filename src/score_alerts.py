"""Score the alert engine against what actually happened in the recorded status data.

Evaluation times are the as-of times of the alert batches (<out>/alert_batches.jsonl); only
times at least --horizon minutes before the end of the recorded data are scored. At each time t
and for every in-service station:

    outcome_empty = bikes available reached <= 1 at some report in (t, t + 30 min]
    outcome_full  = docks available reached <= 1 at some report in (t, t + 30 min]

Precision: share of alerts followed by the outcome, by level (HIGH / MEDIUM), split into
risk alerts (station not yet at the threshold) and already-at-threshold alerts.
Recall: share of actual events, meaning a station going from bikes > 1 to bikes <= 1 (or
docks), caught by an alert of that type for that station in the 30 minutes before the event.
Baseline (persistence rule): alert "empty" if bikes <= 2 now, "full" if docks <= 2 now,
scored the same way at the same evaluation times.

--since ISO restricts scoring to alert batches whose data time is at/after that moment (e.g. the
replay window start); --since-restart uses the live engine's latest restart (the newest batch 0 in
alert_batches.jsonl). Both also limit how much status data is loaded. Read-only: the inputs are
never modified, so this can run next to a live engine.

    python -m src.score_alerts --alerts-out data/live --status-dir data/live/status
"""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.dataset as ds

from src.common import METRICS_DIR, ROOT, write_json
from src.replay import STALE_S, epoch_s


def _dataset(status_dir: Path):
    # exclude files being written (hidden temp names) and anything unreadable
    return ds.dataset(str(status_dir), format="parquet", partitioning="hive", exclude_invalid_files=True)


def _date_filter(lo: int | None):
    if lo is None:
        return None
    first_day = pd.Timestamp(lo, unit="s", tz="UTC").strftime("%Y-%m-%d")
    return ds.field("date") >= first_day


def first_poll_epoch(status_dir: Path, lo: int | None = None) -> int:
    f = _dataset(status_dir).to_table(columns=["fetched_at"], filter=_date_filter(lo)).to_pandas()
    e = epoch_s(f["fetched_at"])
    return int(e[e >= lo].min() if lo is not None and (e >= lo).any() else e.min())


def load_status(status_dir: Path, lo: int | None = None) -> pd.DataFrame:
    """Recorded status; with lo, only reports from lo - 1 h onwards (and their date partitions)."""
    t = _dataset(status_dir).to_table(columns=[
        "fetched_at", "station_id", "num_bikes_available", "num_docks_available", "is_installed",
        "is_renting", "is_returning", "last_reported"], filter=_date_filter(lo - 3600 if lo else None)).to_pandas()
    if lo is not None:
        t = t[t["last_reported"] >= lo - 3600]
    t = t[t["last_reported"] >= epoch_s(t["fetched_at"]) - STALE_S]
    t = t.drop_duplicates(["station_id", "last_reported"]).sort_values(["station_id", "last_reported"])
    t["in_service"] = t["is_installed"] & t["is_renting"] & t["is_returning"]
    return t.rename(columns={"num_bikes_available": "bikes", "num_docks_available": "docks"})


def evaluate(status: pd.DataFrame, times: np.ndarray, horizon: int):
    """Station x time grid: current bikes/docks/in-service and the 30-min outcomes."""
    out = []
    for sid, g in status.groupby("station_id", sort=False):
        T = g["last_reported"].to_numpy()
        B, D, S = g["bikes"].to_numpy(), g["docks"].to_numpy(), g["in_service"].to_numpy()
        now_i = np.searchsorted(T, times, side="right") - 1
        lo = now_i + 1
        hi = np.searchsorted(T, times + horizon, side="right")
        for j, t in enumerate(times):
            i = now_i[j]
            if i < 0:
                continue
            fut_b, fut_d = B[lo[j]:hi[j]], D[lo[j]:hi[j]]
            out.append((sid, t, B[i], D[i], bool(S[i]), bool(len(fut_b) and fut_b.min() <= 1),
                        bool(len(fut_d) and fut_d.min() <= 1)))
    return pd.DataFrame(out, columns=["station_id", "t", "bikes", "docks", "in_service", "out_empty", "out_full"])


def events(status: pd.DataFrame, lo: int, hi: int) -> pd.DataFrame:
    """Transitions into empty (bikes <= 1 after > 1) or full, for in-service stations, in [lo, hi]."""
    ev = []
    for col, kind in (("bikes", "empty"), ("docks", "full")):
        prev = status.groupby("station_id")[col].shift()
        hit = (status[col] <= 1) & (prev > 1) & status["in_service"]
        e = status.loc[hit, ["station_id", "last_reported"]].assign(type=kind)
        ev.append(e[(e["last_reported"] >= lo) & (e["last_reported"] <= hi)])
    return pd.concat(ev, ignore_index=True).rename(columns={"last_reported": "tau"})


def precision(df: pd.DataFrame, outcome: str) -> dict:
    return {"alerts": int(len(df)), "hits": int(df[outcome].sum()),
            "precision": round(float(df[outcome].mean()), 4) if len(df) else None}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--alerts-out", type=Path, default=ROOT / "data" / "live", help="stream_alerts --out-dir")
    ap.add_argument("--status-dir", type=Path, default=ROOT / "data" / "live" / "status")
    ap.add_argument("--horizon-min", type=int, default=30)
    ap.add_argument("--output", type=Path, default=METRICS_DIR / "alert_scores.json")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--since", help="only score alert batches with data time >= this ISO time")
    g.add_argument("--since-restart", action="store_true", help="only alerts since the engine's latest restart")
    a = ap.parse_args()
    H = a.horizon_min * 60

    batches = [json.loads(line) for line in open(a.alerts_out / "alert_batches.jsonl")]
    since = None
    if a.since:
        since = int(epoch_s(pd.Series([a.since]))[0])
    elif a.since_restart:
        restarts = [b for b in batches if b["batch_id"] == 0]
        since = int(epoch_s(pd.Series([restarts[-1]["processed_at"]]))[0]) if restarts else None
    status = load_status(a.status_dir, since)
    data_end = int(status["last_reported"].max())
    times = np.array(sorted({int(epoch_s(pd.Series([b["as_of"]]))[0]) for b in batches}), dtype=np.int64)
    # Score only times after the first poll (earlier "now"s come from old reports sent first)
    # and at least one horizon before the end of the recording.
    start = first_poll_epoch(a.status_dir, since)
    if since is not None:
        start = max(start, since)
    scored = times[(times >= start) & (times <= data_end - H)]
    res = {"status_dir": str(a.status_dir), "data_start": None, "data_end": None, "since": None,
           "evaluation_times": int(len(scored)), "horizon_min": a.horizon_min}
    iso = lambda x: datetime.fromtimestamp(int(x), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")  # noqa: E731
    res["data_start"], res["data_end"] = iso(start), iso(data_end)
    res["since"] = iso(since) if since is not None else None
    res["computed_at"] = iso(pd.Timestamp.now(tz="UTC").timestamp())
    if not len(scored):
        res["note"] = f"no alert batch is {a.horizon_min} min older than the end of the recorded data"
        write_json(a.output, res)
        print(json.dumps(res, indent=2))
        return

    grid = evaluate(status, scored, H)
    grid = grid[grid["in_service"]]
    al = pd.read_parquet(a.alerts_out / "alerts")
    al["t"] = epoch_s(al["as_of"])
    al = al[al["t"].isin(scored)]
    m = grid.merge(al[["station_id", "t", "alert_type", "level", "already_at_threshold", "probability"]],
                   on=["station_id", "t"], how="left")
    res["station_times_in_service"] = int(len(grid))
    res["base_rate"] = {"empty": round(float(grid["out_empty"].mean()), 4) if len(grid) else None,
                        "full": round(float(grid["out_full"].mean()), 4) if len(grid) else None}
    model = {}
    for kind, outcome in (("empty", "out_empty"), ("full", "out_full")):
        a_k = m[m["alert_type"] == kind]
        model[kind] = {
            "all_levels": precision(a_k, outcome),
            **{lvl: precision(a_k[a_k["level"] == lvl], outcome) for lvl in ("HIGH", "MEDIUM")},
            "risk_alerts": {lvl: precision(a_k[(a_k["level"] == lvl) & ~a_k["already_at_threshold"].astype(bool)],
                                           outcome) for lvl in ("HIGH", "MEDIUM")},
            "already_at_threshold": precision(a_k[a_k["already_at_threshold"].astype(bool)], outcome),
        }
    base = {}
    for kind, col, outcome in (("empty", "bikes", "out_empty"), ("full", "docks", "out_full")):
        b = grid[grid[col] <= 2]
        base[kind] = {"rule": f"{col} <= 2 now", "all": precision(b, outcome),
                      "already_at_threshold (<= 1)": precision(b[b[col] <= 1], outcome),
                      "exactly 2": precision(b[b[col] == 2], outcome)}
    # Recall: events after the first scored time + horizon (so an alert could have preceded them).
    ev = events(status, int(scored.min()) + 1, int(scored.max()) + H)
    rec = {}
    for kind, col in (("empty", "bikes"), ("full", "docks")):
        e = ev[ev["type"] == kind]
        mk = m[m["alert_type"] == kind][["station_id", "t"]]
        bk = grid[grid[col] <= 2][["station_id", "t"]]

        def caught(flags):
            j = e.merge(flags, on="station_id", how="left")
            ok = (j["t"] < j["tau"]) & (j["t"] >= j["tau"] - H)
            return int(j[ok].drop_duplicates(["station_id", "tau"]).shape[0])

        n = len(e)
        rec[kind] = {"events": n,
                     "model_caught": caught(mk), "model_recall": round(caught(mk) / n, 4) if n else None,
                     "baseline_caught": caught(bk), "baseline_recall": round(caught(bk) / n, 4) if n else None}
    res.update(model=model, baseline_persistence=base, recall=rec)
    res["resolved_risk_alerts"] = int(sum(model[k]["risk_alerts"][lvl]["alerts"] for k in ("empty", "full")
                                         for lvl in ("HIGH", "MEDIUM")))
    write_json(a.output, res)
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
