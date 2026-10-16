"""Replay recorded GBFS status rows into Kafka in event-time order, at an adjustable speed.

Reads data/live/status/date=*/hour=*/*.parquet (or --source, e.g. a copy) one hour partition at
a time, keeping a one-partition lookahead so rows are emitted in global last_reported order (a
row's event time can be a little earlier than its poll hour). Messages have the same JSON format
as `collector --kafka` (key = station_id, Kafka timestamp and "event_time" = last_reported,
"source": "replay").

    --speed 60   60x faster than real time (1 hour of events in 1 minute)
    --speed 0    as fast as possible

Rows whose last_reported is more than 6 h older than the poll that saw them (decommissioned or
placeholder stations, e.g. last_reported = 86400) are skipped and counted. Pacing starts 15 min
before the first poll: older reports from rarely-reporting stations are sent at once (still in
order) instead of making a fast replay sit idle for hours of event time.

    python -m src.replay --source data/replay_copy/status --speed 600 --topic station_status
"""
import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from src.collector import STATUS_DIR

STALE_S = 6 * 3600
EPOCH = pd.Timestamp("1970-01-01", tz="UTC")


def epoch_s(iso_utc: pd.Series) -> pd.Series:
    """Epoch seconds from ISO strings; independent of the datetime64 resolution pandas picks."""
    return (pd.to_datetime(iso_utc, utc=True) - EPOCH) // pd.Timedelta(seconds=1)


def partitions(source: Path) -> list[Path]:
    return sorted(p for p in source.glob("date=*/hour=*") if p.is_dir())


def load(part: Path) -> pd.DataFrame:
    files = sorted(f for f in part.glob("*.parquet") if not f.name.startswith((".", "_")))
    if not files:
        return pd.DataFrame()
    df = pd.concat([pq.read_table(f).to_pandas() for f in files], ignore_index=True)
    df["_stale"] = df["last_reported"] < epoch_s(df["fetched_at"]) - STALE_S
    return df


def rows_in_order(source: Path, stats: dict):
    """Yield rows sorted by last_reported across hour partitions (one-partition lookahead)."""
    pending = pd.DataFrame()
    parts = partitions(source)
    for i, part in enumerate(parts):
        df = load(part)
        if df.empty:
            continue
        stats["skipped_stale"] += int(df["_stale"].sum())
        df = df[~df["_stale"]].drop(columns="_stale")
        pending = pd.concat([pending, df], ignore_index=True) if len(pending) else df
        last = i == len(parts) - 1
        # Rows earlier than everything in the next partition can be emitted safely.
        cutoff = None if last else load_min_event(parts[i + 1])
        pending = pending.sort_values(["last_reported", "station_id"], kind="stable")
        ready = pending if cutoff is None else pending[pending["last_reported"] < cutoff]
        pending = pending.iloc[len(ready):]
        yield from ready.to_dict("records")
    if len(pending):
        yield from pending.to_dict("records")


def load_min_event(part: Path):
    files = [f for f in part.glob("*.parquet") if not f.name.startswith((".", "_"))]
    mins = [pq.read_table(f, columns=["last_reported", "fetched_at"]).to_pandas() for f in files]
    if not mins:
        return None
    m = pd.concat(mins)
    m = m[m["last_reported"] >= epoch_s(m["fetched_at"]) - STALE_S]
    return int(m["last_reported"].min()) if len(m) else None


def first_poll(source: Path) -> int | None:
    parts = partitions(source)
    for part in parts:
        files = [f for f in part.glob("*.parquet") if not f.name.startswith((".", "_"))]
        if files:
            f = pd.concat([pq.read_table(x, columns=["fetched_at"]).to_pandas() for x in files])
            return int(epoch_s(f["fetched_at"]).min())
    return None


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", type=Path, default=STATUS_DIR, help="status dir (date=/hour= partitions)")
    ap.add_argument("--topic", default="station_status")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--speed", type=float, default=60.0, help="x real time; 0 = as fast as possible")
    a = ap.parse_args()

    from confluent_kafka import Producer
    stats = {"sent": 0, "delivered": 0, "failed": 0, "skipped_stale": 0}

    def delivered(err, _msg):
        stats["failed" if err else "delivered"] += 1

    p = Producer({"bootstrap.servers": a.bootstrap, "client.id": "bikeflow-replay", "linger.ms": 50,
                  "broker.address.family": "v4",
                  "compression.type": "zstd", "queue.buffering.max.messages": 500_000})
    t_wall0, t_event0, first_ev, last_ev = time.monotonic(), None, None, None
    fp = first_poll(a.source)
    anchor = fp - 900 if fp else None
    print(f"[{datetime.now():%H:%M:%S}] replay START source={a.source} topic={a.topic} speed={a.speed:g}x",
          flush=True)
    for row in rows_in_order(a.source, stats):
        ev = int(row["last_reported"])
        if t_event0 is None:
            t_event0, first_ev = (max(ev, anchor) if anchor else ev), ev
        if a.speed > 0:
            wait = t_wall0 + (ev - t_event0) / a.speed - time.monotonic()
            if wait > 0:
                p.poll(0)
                time.sleep(wait)
        msg = {k: (None if pd.isna(v) else (bool(v) if isinstance(v, (bool,)) else v)) for k, v in row.items()
               if k not in ("date", "hour")}
        for k, v in msg.items():  # numpy scalars -> Python for json
            if hasattr(v, "item"):
                msg[k] = v.item()
        msg["event_time"] = datetime.fromtimestamp(ev, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        msg["source"] = "replay"
        while True:
            try:
                p.produce(a.topic, key=row["station_id"], value=json.dumps(msg), timestamp=ev * 1000,
                          on_delivery=delivered)
                break
            except BufferError:
                p.poll(0.5)
        stats["sent"] += 1
        last_ev = ev
        if stats["sent"] % 20000 == 0:
            p.poll(0)
            print(f"  sent {stats['sent']:,} (event time {msg['event_time']})", flush=True)
    p.flush(30)
    wall = time.monotonic() - t_wall0
    span = (last_ev - first_ev) if first_ev is not None else 0
    print(json.dumps({**stats, "event_span_min": round(span / 60, 1), "wall_s": round(wall, 1),
                      "effective_speed": round(span / wall, 1) if wall else None}), flush=True)
    print(f"[{datetime.now():%H:%M:%S}] replay END", flush=True)


if __name__ == "__main__":
    main()
