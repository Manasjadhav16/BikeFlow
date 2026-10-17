"""Read-only access to what the pipeline writes, with small mtime-based caches.

Live:   data/live/latest_state.parquet, data/live/alerts/, data/live/alert_batches.jsonl,
        data/live/status/ (collector), outputs/metrics/collector_health.jsonl
Replay: data/replay_out/ (scripts/make_replay.sh): the same files plus state_history/ and
        source/status/
Nothing here ever writes to disk.
"""
import json
import os
import threading
import time
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds

ROOT = Path(__file__).resolve().parents[2]
LIVE = ROOT / "data" / "live"
REPLAY = ROOT / "data" / "replay_out"
METRICS = ROOT / "outputs" / "metrics"
PROCESSED = ROOT / "data" / "processed"

_lock = threading.Lock()
_cache: dict = {}


def base(source: str) -> Path:
    return REPLAY if source == "replay" else LIVE


def status_dir(source: str) -> Path:
    return REPLAY / "source" / "status" if source == "replay" else LIVE / "status"


def cached(key, stamp, build):
    """Return the cached value for key unless `stamp` (e.g. file mtime) changed."""
    with _lock:
        hit = _cache.get(key)
        if hit and hit[0] == stamp:
            return hit[1]
    val = build()
    with _lock:
        _cache[key] = (stamp, val)
    return val


def mtime(p: Path) -> float:
    try:
        return p.stat().st_mtime
    except OSError:
        return 0.0


def epoch(iso: pd.Series) -> pd.Series:
    return (pd.to_datetime(iso, utc=True) - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta(seconds=1)


# ---------- batches / state ----------
def batches(source: str) -> list[dict]:
    p = base(source) / "alert_batches.jsonl"

    def build():
        if not p.exists():
            return []
        out = []
        with open(p) as f:
            for line in f:
                try:
                    out.append(json.loads(line))
                except ValueError:
                    pass  # a line being written right now
        return out

    return cached(("batches", source), mtime(p), build)


def replay_meta() -> dict:
    p = REPLAY / "replay_meta.json"
    return json.loads(p.read_text()) if p.exists() else {}


def replay_timeline() -> list[dict]:
    meta = replay_meta()
    start = meta.get("window_start", "")
    return [{"batch_id": b["batch_id"], "as_of": b["as_of"]} for b in batches("replay") if b["as_of"] >= start]


def state(source: str, at: str | None = None) -> tuple[pd.DataFrame, dict | None]:
    """Station state table and the batch record it belongs to."""
    if source == "replay":
        tl = replay_timeline()
        if not tl:
            return pd.DataFrame(), None
        pick = tl[-1]
        if at:
            ok = [b for b in tl if b["as_of"] <= at]
            pick = ok[-1] if ok else tl[0]
        p = REPLAY / "state_history" / f"batch-{pick['batch_id']:06d}.parquet"
        rec = next((b for b in batches("replay") if b["batch_id"] == pick["batch_id"]), None)
    else:
        p = LIVE / "latest_state.parquet"
        bl = batches("live")
        rec = bl[-1] if bl else None
    if not p.exists():
        return pd.DataFrame(), rec
    df = cached(("state", str(p)), mtime(p), lambda: pd.read_parquet(p))
    return df, rec


def alerts_history(source: str) -> pd.DataFrame:
    d = base(source) / "alerts"
    files = sorted(d.glob("date=*/batch-*.parquet"))
    stamp = (len(files), mtime(files[-1]) if files else 0)

    def build():
        if not files:
            return pd.DataFrame()
        cols = ["as_of", "batch_id", "station_id", "short_id", "name", "lat", "lng", "bikes", "docks",
                "alert_type", "alert_kind", "probability", "level", "exp_departures_30m", "exp_arrivals_30m",
                "weather_factor_dep"]
        df = pd.concat([pd.read_parquet(f, columns=cols) for f in files], ignore_index=True)
        df["t"] = epoch(df["as_of"])
        return df

    return cached(("alerts", source), stamp, build)


# ---------- recorded status ----------
def status_files(source: str) -> list[Path]:
    return sorted(f for f in status_dir(source).glob("date=*/hour=*/*.parquet") if not f.name.startswith("."))


def status_window(source: str, lo: int, hi: int, station: str | None = None) -> pd.DataFrame:
    """Recorded reports with last_reported in [lo, hi] (optionally one station)."""
    sd = status_dir(source)
    if not sd.exists():
        return pd.DataFrame()
    days = sorted({pd.Timestamp(x, unit="s", tz="UTC").strftime("%Y-%m-%d")
                   for x in range(lo - 3600, hi + 3600, 3600)})
    dset = ds.dataset(str(sd), format="parquet", partitioning="hive", exclude_invalid_files=True)
    filt = ds.field("date").isin(days)
    if station:
        filt = filt & (ds.field("station_id") == station)
    t = dset.to_table(filter=filt, columns=["fetched_at", "station_id", "num_bikes_available",
                                            "num_docks_available", "is_installed", "is_renting",
                                            "is_returning", "last_reported"]).to_pandas()
    t = t[(t["last_reported"] >= lo) & (t["last_reported"] <= hi)]
    return t.drop_duplicates(["station_id", "last_reported"]).sort_values(["station_id", "last_reported"])


def collector_last_poll() -> dict | None:
    p = METRICS / "collector_health.jsonl"
    if not p.exists():
        return None
    with open(p, "rb") as f:  # read only the tail
        f.seek(0, os.SEEK_END)
        f.seek(max(0, f.tell() - 20000))
        lines = f.read().decode(errors="ignore").strip().split("\n")
    for line in reversed(lines):
        try:
            r = json.loads(line)
        except ValueError:
            continue
        if r.get("event", "").startswith("poll"):
            return r
    return None


def pid_alive(pid_file: Path) -> bool:
    try:
        os.kill(int(pid_file.read_text().strip()), 0)
        return True
    except (OSError, ValueError):
        return False


def now() -> float:
    return time.time()
