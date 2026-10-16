"""Live Citi Bike GBFS collector: plain Python (requests + pyarrow), no Spark.

Every poll interval (60 s, or the feed's ttl if longer) it fetches station_status.json, found via
the GBFS discovery file. A station row is buffered only if that station's last_reported changed
since the last poll. If the whole feed's last_updated has not moved, the poll writes nothing. Every
poll is logged to outputs/metrics/collector_health.jsonl (time, HTTP status, latency, station count,
rows kept/skipped, retries, error).

Buffered rows are flushed every 5 minutes (and on Ctrl+C / SIGTERM) into
data/live/status/date=YYYY-MM-DD/hour=HH/part-<flush time>-<pid>.parquet. Date and hour are UTC,
taken from fetched_at. Each flush adds new files, so a restart simply continues appending. The
de-duplication state (station -> last_reported) is saved at every flush, so a restart does not
rewrite every station. station_information.json is saved at start and every 24 h to
data/live/station_info/.

    python -m src.collector --once            # one poll + flush (test)
    python -m src.collector --max-minutes 10  # bounded run
    python -m src.collector                   # until Ctrl+C / SIGTERM
"""
import argparse
import json
import os
import signal
import threading
import time
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from src.common import METRICS_DIR, ROOT

DISCOVERY_URL = "https://gbfs.citibikenyc.com/gbfs/gbfs.json"
LIVE_DIR = ROOT / "data" / "live"
STATUS_DIR = LIVE_DIR / "status"
INFO_DIR = LIVE_DIR / "station_info"
STATE_JSON = LIVE_DIR / "collector_state.json"
HEALTH_JSONL = METRICS_DIR / "collector_health.jsonl"
POLL_S = 60
FLUSH_S = 300
INFO_EVERY_S = 24 * 3600
TIMEOUT_S = 10
MAX_RETRIES = 5
INT_FIELDS = ["num_bikes_available", "num_ebikes_available", "num_docks_available",
              "num_bikes_disabled", "num_docks_disabled"]
BOOL_FIELDS = ["is_installed", "is_renting", "is_returning"]
SCHEMA = pa.schema(
    [("fetched_at", pa.string()), ("station_id", pa.string())]
    + [(f, pa.int32()) for f in INT_FIELDS]
    + [(f, pa.bool_()) for f in BOOL_FIELDS]
    + [("last_reported", pa.int64())]
)
HEADERS = {"User-Agent": "BikeFlow-collector/1.0 (research; polls every 60s)"}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


class Collector:
    def __init__(self):
        self.stop = threading.Event()
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.urls: dict[str, str] = {}
        self.buffer: list[dict] = []
        self.last_reported: dict[str, int] = {}
        self.feed_last_updated: int | None = None
        self.interval = POLL_S
        self.totals = {"polls": 0, "ok_polls": 0, "rows_written": 0, "rows_skipped": 0, "files": 0}
        if STATE_JSON.exists():
            try:
                st = json.loads(STATE_JSON.read_text())
                self.last_reported = {k: int(v) for k, v in st.get("last_reported", {}).items()}
                self.feed_last_updated = st.get("feed_last_updated")
            except (ValueError, OSError):
                pass  # corrupt state: start fresh (worst case: one poll re-writes every station)

    # ---------- HTTP with retries ----------
    def get_json(self, url: str) -> tuple[dict | None, dict]:
        """GET with timeout and exponential backoff (1, 2, 4, 8, 16 s); never raises."""
        meta = {"retries": 0, "http_status": None, "latency_ms": None, "error": None}
        for attempt in range(MAX_RETRIES + 1):
            t0 = time.monotonic()
            try:
                r = self.session.get(url, timeout=TIMEOUT_S)
                meta["latency_ms"] = round((time.monotonic() - t0) * 1000, 1)
                meta["http_status"] = r.status_code
                r.raise_for_status()
                return r.json(), meta
            except (requests.RequestException, ValueError) as e:
                meta["error"] = f"{type(e).__name__}: {str(e)[:200]}"
            if attempt == MAX_RETRIES or self.stop.wait(2 ** attempt):
                break
            meta["retries"] = attempt + 1
        return None, meta

    def discover(self) -> bool:
        d, meta = self.get_json(DISCOVERY_URL)
        try:
            feeds = d["data"]["en"]["feeds"] if "en" in d.get("data", {}) else d["data"]["feeds"]
            self.urls = {f["name"]: f["url"] for f in feeds}
            return "station_status" in self.urls
        except (TypeError, KeyError):
            self.log({"event": "discovery_failed", **meta})
            return False

    # ---------- logging / storage ----------
    def log(self, rec: dict) -> None:
        HEALTH_JSONL.parent.mkdir(parents=True, exist_ok=True)
        with open(HEALTH_JSONL, "a") as f:
            f.write(json.dumps({"time": iso(utcnow()), **rec}) + "\n")

    def save_station_info(self) -> None:
        url = self.urls.get("station_information")
        if not url:
            return
        d, meta = self.get_json(url)
        if d is None:
            self.log({"event": "station_info_failed", **meta})
            return
        INFO_DIR.mkdir(parents=True, exist_ok=True)
        path = INFO_DIR / f"station_information_{utcnow():%Y%m%dT%H%M%SZ}.json"
        path.write_text(json.dumps(d))
        self.log({"event": "station_info_saved", "file": path.name,
                  "stations": len(d.get("data", {}).get("stations", [])), **meta})

    def flush(self) -> None:
        if self.buffer:
            stamp, groups = utcnow(), {}
            for row in self.buffer:
                groups.setdefault((row["fetched_at"][:10], row["fetched_at"][11:13]), []).append(row)
            for (day, hour), rows in groups.items():
                d = STATUS_DIR / f"date={day}" / f"hour={hour}"
                d.mkdir(parents=True, exist_ok=True)
                tmp = d / f".part-{stamp:%Y%m%dT%H%M%S}-{os.getpid()}.parquet.tmp"
                pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), tmp, compression="zstd")
                tmp.rename(d / tmp.name[1:].removesuffix(".tmp"))  # atomic: readers never see half a file
                self.totals["files"] += 1
            self.totals["rows_written"] += len(self.buffer)
            self.buffer = []
        STATE_JSON.parent.mkdir(parents=True, exist_ok=True)
        STATE_JSON.write_text(json.dumps({"saved_at": iso(utcnow()), "feed_last_updated": self.feed_last_updated,
                                          "last_reported": self.last_reported}))

    # ---------- one poll ----------
    def poll(self) -> None:
        self.totals["polls"] += 1
        fetched = utcnow()
        d, meta = self.get_json(self.urls["station_status"])
        rec = {"event": "poll", **meta, "stations": None, "rows_kept": 0, "rows_skipped": 0,
               "feed_last_updated": None, "ttl": None, "feed_unchanged": False}
        try:
            if d is None:
                rec["event"] = "poll_skipped"  # retries exhausted: skip this cycle
                return
            stations = d["data"]["stations"]
            rec.update(stations=len(stations), feed_last_updated=d.get("last_updated"), ttl=d.get("ttl"))
            self.interval = max(POLL_S, int(d.get("ttl") or 0))
            if d.get("last_updated") is not None and d.get("last_updated") == self.feed_last_updated:
                rec["feed_unchanged"] = True  # nothing new published since the previous poll
                rec["rows_skipped"] = len(stations)
                self.totals["rows_skipped"] += len(stations)
                self.totals["ok_polls"] += 1
                return
            self.feed_last_updated = d.get("last_updated")
            ts = iso(fetched)
            for s in stations:
                sid, lr = s.get("station_id"), s.get("last_reported")
                if sid is None or lr is None:
                    continue
                if self.last_reported.get(sid) == lr:
                    rec["rows_skipped"] += 1
                    continue
                self.last_reported[sid] = lr
                row = {"fetched_at": ts, "station_id": sid, "last_reported": int(lr)}
                for f in INT_FIELDS:
                    row[f] = None if s.get(f) is None else int(s[f])
                for f in BOOL_FIELDS:
                    row[f] = None if s.get(f) is None else bool(s[f])
                self.buffer.append(row)
                rec["rows_kept"] += 1
            self.totals["rows_skipped"] += rec["rows_skipped"]
            self.totals["ok_polls"] += 1
        except (KeyError, TypeError, ValueError) as e:  # malformed payload: log and move on
            rec.update(event="poll_bad_payload", error=f"{type(e).__name__}: {str(e)[:200]}")
        finally:
            self.log(rec)

    # ---------- main loop ----------
    def run(self, once: bool, max_minutes: float | None) -> None:
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: self.stop.set())
        while not self.discover():
            if self.stop.wait(60):
                return
        self.log({"event": "start", "pid": os.getpid(), "urls": self.urls, "once": once,
                  "max_minutes": max_minutes, "restored_stations": len(self.last_reported)})
        start = time.monotonic()
        deadline = start + max_minutes * 60 if max_minutes else None
        next_flush = start + FLUSH_S
        next_info = start  # fetch station_information at start
        try:
            while not self.stop.is_set():
                tick = time.monotonic()
                if tick >= next_info:
                    self.save_station_info()
                    next_info = tick + INFO_EVERY_S
                self.poll()
                if once:
                    break
                if time.monotonic() >= next_flush:
                    self.flush()
                    next_flush = time.monotonic() + FLUSH_S
                wake = tick + self.interval
                if deadline and wake > deadline:
                    break
                self.stop.wait(max(0.0, wake - time.monotonic()))
        finally:
            self.flush()
            self.log({"event": "stop", "elapsed_s": round(time.monotonic() - start, 1), **self.totals})
            print(json.dumps({"event": "stop", **self.totals}), flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--once", action="store_true", help="one poll, flush, exit")
    ap.add_argument("--max-minutes", type=float, help="stop after this many minutes")
    a = ap.parse_args()
    Collector().run(once=a.once, max_minutes=a.max_minutes)


if __name__ == "__main__":
    main()
