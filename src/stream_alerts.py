"""BikeFlow alert engine: Kafka -> Spark Structured Streaming -> empty/full risk alerts.

Pipeline
  1. Read JSON station rows from the Kafka topic station_status (written by
     `collector --kafka` or src/replay.py). Event time = last_reported, watermark 5 minutes.
  2. Per station (applyInPandasWithState): keep the last 35 minutes of (event time, bikes,
     docks); output current bikes/docks, service flags and the change in bikes available over
     the last 5, 15 and 30 minutes. A station silent for 2 h (event time) is dropped from state.
  3. Every micro-batch (foreachBatch, default 60 s):
       now      = the latest event time seen (data time, so a replay behaves like live)
       expected = historical departures/arrivals for [now, now + 30 min) from
                  data/processed/reference/expected_demand.parquet (the three overlapping
                  15-minute buckets, weighted by overlap), times the weather factor for the
                  conditions at `now` (src/weather_live.py)
       P(empty) = P(D - A >= bikes_now - 1),  P(full) = P(A - D >= docks_now - 1)
                  with D ~ Poisson(mu_dep), A ~ Poisson(mu_arr): Skellam (scipy)
       level    = HIGH if P >= 0.7, MEDIUM if P >= 0.5
     Stations with is_installed / is_renting / is_returning false are out of service (no
     alert); stations not heard from for 60 minutes are stale (no alert).
  4. Alerts go to the Kafka topic bikeflow_alerts, to Parquet under <out>/alerts/date=.../,
     and every station's current risk is written to <out>/latest_state.parquet (replaced
     atomically each batch). One JSON line per batch goes to <out>/alert_batches.jsonl.

    python -m src.stream_alerts                                   # live: latest offsets, 60 s
    python -m src.stream_alerts --starting-offsets earliest --trigger-seconds 5 \\
        --max-offsets 6000 --out-dir data/test_stream --checkpoint data/checkpoints/test --available-now
"""
import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupStateTimeout
from scipy.stats import skellam

from src.common import ROOT, ResourceMonitor, check_disk, get_spark, stop_spark
from src.reference_tables import REF_DIR
from src.weather_live import weather_at, weather_factor

KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
TZ = "America/New_York"
KEEP_S = 35 * 60          # state window per station
EXPIRE_S = 2 * 3600       # drop a station's state after this much event-time silence
STALE_S = 60 * 60         # no alert if the station's latest report is older than this
HIGH, MEDIUM = 0.7, 0.5
IN_SCHEMA = ("fetched_at string, station_id string, num_bikes_available int, num_ebikes_available int, "
             "num_docks_available int, num_bikes_disabled int, num_docks_disabled int, is_installed boolean, "
             "is_renting boolean, is_returning boolean, last_reported long, event_time string, source string")
# History is stored as a compact JSON string: array columns in the state schema break the PySpark
# Python worker (KeyError 'elementType' with this PySpark/pandas combination).
STATE_SCHEMA = "hist string, ebikes int, installed boolean, renting boolean, returning boolean"
OUT_SCHEMA = ("station_id string, last_reported long, bikes int, docks int, ebikes int, is_installed boolean, "
              "is_renting boolean, is_returning boolean, change_5m int, change_15m int, change_30m int, n_obs int")
STATE_FIELDS = [c.split()[0] for c in STATE_SCHEMA.split(", ")]
OUT_SCHEMA_COLS = [c.split()[0] for c in OUT_SCHEMA.split(", ")]
# Bump when STATE_SCHEMA or the stateful logic changes incompatibly: an older checkpoint is then moved aside.
STATE_VERSION = "2"
IN_DEFAULTS = {"num_ebikes_available": None, "is_installed": None, "is_renting": None, "is_returning": None,
               "num_bikes_available": None, "num_docks_available": None}
OUT_DEFAULTS = {"ebikes": None, "is_installed": None, "is_renting": None, "is_returning": None,
                "change_5m": None, "change_15m": None, "change_30m": None, "n_obs": 0}


def station_state(key, pdfs, state):
    """applyInPandasWithState: merge new reports into the station's 35-minute history."""
    if state.hasTimedOut:
        state.remove()
        return
    new = pd.concat(list(pdfs), ignore_index=True)
    for col, default in IN_DEFAULTS.items():  # tolerate messages from older producers
        if col not in new.columns:
            new[col] = default
    hist, eb, inst, rent, ret = read_state(state)
    latest_new = new.sort_values("last_reported").iloc[-1]
    if not len(hist) or latest_new["last_reported"] >= hist["last_reported"].max():
        eb, inst, rent, ret = (latest_new["num_ebikes_available"], latest_new["is_installed"],
                               latest_new["is_renting"], latest_new["is_returning"])
    h = (pd.concat([hist, new[["last_reported", "num_bikes_available", "num_docks_available"]]])
         .dropna(subset=["last_reported"]).astype({"last_reported": "int64"})
         .drop_duplicates("last_reported", keep="last").sort_values("last_reported"))
    now = int(h["last_reported"].iloc[-1])
    h = h[h["last_reported"] >= now - KEEP_S]
    t = h["last_reported"].to_numpy()
    b = h["num_bikes_available"].to_numpy()

    def change(sec):
        i = np.searchsorted(t, now - sec, side="right") - 1
        return None if i < 0 else int(b[-1] - b[i])

    hist_json = json.dumps([t.tolist(), [int(x) for x in b], [int(x) for x in h["num_docks_available"]]],
                           separators=(",", ":"))
    state.update((hist_json, None if pd.isna(eb) else int(eb), None if pd.isna(inst) else bool(inst),
                  None if pd.isna(rent) else bool(rent), None if pd.isna(ret) else bool(ret)))
    state.setTimeoutTimestamp((now + EXPIRE_S) * 1000)
    yield pd.DataFrame([{
        "station_id": key[0], "last_reported": now, "bikes": int(b[-1]),
        "docks": int(h["num_docks_available"].iloc[-1]), "ebikes": None if pd.isna(eb) else int(eb),
        "is_installed": inst, "is_renting": rent, "is_returning": ret,
        "change_5m": change(300), "change_15m": change(900), "change_30m": change(1800), "n_obs": len(h),
    }])


def read_state(state):
    """Station state with defaults: tolerates a missing state, a shorter/older state tuple and a
    history string that is missing or not valid JSON (it then starts a fresh history)."""
    empty = pd.DataFrame({"last_reported": pd.Series(dtype="int64"),
                          "num_bikes_available": pd.Series(dtype="int64"),
                          "num_docks_available": pd.Series(dtype="int64")})
    if not state.exists:
        return empty, None, None, None, None
    vals = list(state.get) + [None] * len(STATE_FIELDS)
    rec = dict(zip(STATE_FIELDS, vals))
    hist = empty
    try:
        t, b, d = json.loads(rec["hist"] or "[[],[],[]]")[:3]
        if len(t) == len(b) == len(d):
            hist = pd.DataFrame({"last_reported": t, "num_bikes_available": b, "num_docks_available": d})
    except (TypeError, ValueError):
        pass
    return hist, rec["ebikes"], rec["installed"], rec["renting"], rec["returning"]


def seed_latest(path: Path) -> pd.DataFrame:
    """Seed the driver's station table from the last latest_state.parquet on restart.
    That file holds the processed table (not the stateful step's raw output), so take the columns
    that exist and fill the rest with defaults; an unreadable file means an empty start."""
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_parquet(path)
    except Exception as e:  # noqa: BLE001 - a corrupt file must not stop the engine
        print(f"[{datetime.now():%H:%M:%S}] could not read {path.name} ({type(e).__name__}); starting empty",
              flush=True)
        return pd.DataFrame()
    if "station_id" not in df.columns or "last_reported" not in df.columns:
        return pd.DataFrame()
    for col, default in OUT_DEFAULTS.items():
        if col not in df.columns:
            df[col] = default
    print(f"[{datetime.now():%H:%M:%S}] seeded {len(df):,} stations from {path.name}", flush=True)
    return df[OUT_SCHEMA_COLS].set_index("station_id")


class AlertEngine:
    """Driver-side part of foreachBatch: latest state, expected demand, Skellam, sinks."""

    def __init__(self, out_dir: Path, bootstrap: str, alert_topic: str | None, state_history: bool = False):
        self.out_dir = out_dir
        self.state_history = state_history  # also keep every batch's full state (replay/demo time slider)
        (out_dir / "alerts").mkdir(parents=True, exist_ok=True)
        exp = pd.read_parquet(REF_DIR / "expected_demand.parquet")
        self.meta = pd.read_parquet(REF_DIR / "stations_live.parquet").set_index("gbfs_station_id")
        ids = sorted(exp["gbfs_station_id"].unique())
        self.idx = {g: i for i, g in enumerate(ids)}
        slot = (exp["day_of_week"] - 1) * 96 + exp["hour"] * 4 + exp["quarter"]
        self.E_dep = np.zeros((len(ids), 672))
        self.E_arr = np.zeros((len(ids), 672))
        rows = exp["gbfs_station_id"].map(self.idx).to_numpy()
        self.E_dep[rows, slot.to_numpy()] = exp["exp_departures"].to_numpy()
        self.E_arr[rows, slot.to_numpy()] = exp["exp_arrivals"].to_numpy()
        self.latest = seed_latest(out_dir / "latest_state.parquet")
        self.producer = None
        if alert_topic:
            from confluent_kafka import Producer
            self.producer = Producer({"bootstrap.servers": bootstrap, "client.id": "bikeflow-alerts",
                                      "broker.address.family": "v4", "linger.ms": 50})
        self.alert_topic = alert_topic
        self.durations: list[float] = []
        self.seen_updates, self.last_update_wall = False, time.time()

    def expected_next_30m(self, now: int):
        """Overlap-weighted sum of the 15-min buckets covering [now, now + 30 min) in NYC time."""
        b0 = now - now % 900
        w0 = (b0 + 900 - now) / 900
        mu_d, mu_a = np.zeros(len(self.idx)), np.zeros(len(self.idx))
        used = []
        for start, w in ((b0, w0), (b0 + 900, 1.0), (b0 + 1800, 1.0 - w0)):
            if w <= 0:
                continue
            ny = pd.Timestamp(start, unit="s", tz="UTC").tz_convert(TZ)
            k = ny.dayofweek * 96 + ny.hour * 4 + ny.minute // 15
            mu_d += w * self.E_dep[:, k]
            mu_a += w * self.E_arr[:, k]
            used.append(f"{ny:%a %H:%M} x{w:.2f}")
        return mu_d, mu_a, used

    def process(self, batch_df, batch_id: int) -> None:
        t0 = time.time()
        upd = batch_df.toPandas()  # one row per station updated in this batch (<= ~2.5k)
        if len(upd):
            self.seen_updates, self.last_update_wall = True, time.time()
        if len(upd):
            upd = upd.set_index("station_id")
            self.latest = upd if self.latest.empty else pd.concat(
                [self.latest[~self.latest.index.isin(upd.index)], upd])
        if self.latest.empty:
            return
        st = self.latest.copy()
        now = int(st["last_reported"].max())
        mu_d_all, mu_a_all, used = self.expected_next_30m(now)
        w = weather_at(now)
        f_dep, f_arr, winfo = weather_factor(w)

        st = st[st.index.isin(self.idx)].copy()  # stations with history (live-matched)
        rows = st.index.map(self.idx).to_numpy()
        st["exp_departures_30m"] = mu_d_all[rows] * f_dep
        st["exp_arrivals_30m"] = mu_a_all[rows] * f_arr
        mu_d = np.clip(st["exp_departures_30m"].to_numpy(), 1e-9, None)
        mu_a = np.clip(st["exp_arrivals_30m"].to_numpy(), 1e-9, None)
        k_empty = st["bikes"].to_numpy() - 1
        k_full = st["docks"].to_numpy() - 1
        # P(X >= k) = sf(k - 1); k <= 0 means the station is already at/below the threshold.
        st["p_empty"] = np.where(k_empty <= 0, 1.0, np.nan_to_num(skellam.sf(k_empty - 1, mu_d, mu_a)))
        st["p_full"] = np.where(k_full <= 0, 1.0, np.nan_to_num(skellam.sf(k_full - 1, mu_a, mu_d)))
        in_service = (st["is_installed"].fillna(False) & st["is_renting"].fillna(False)
                      & st["is_returning"].fillna(False)).astype(bool)
        stale = st["last_reported"] < now - STALE_S
        st["status"] = np.where(~in_service, "out_of_service", np.where(stale, "stale", "ok"))
        st["alert_type"] = np.where(st["p_empty"] >= st["p_full"], "empty", "full")
        st["probability"] = st[["p_empty", "p_full"]].max(axis=1)
        st["level"] = np.where(st["status"] != "ok", "none",
                               np.where(st["probability"] >= HIGH, "HIGH",
                                        np.where(st["probability"] >= MEDIUM, "MEDIUM", "none")))
        meta = self.meta.reindex(st.index)
        st = st.assign(short_id=meta["station_id"], name=meta["name"], lat=meta["lat"], lng=meta["lng"],
                       capacity=meta["capacity"], weather_factor_dep=f_dep, weather_factor_arr=f_arr,
                       temperature_2m=(w or {}).get("temperature_2m"), precipitation=(w or {}).get("precipitation"),
                       as_of=datetime.fromtimestamp(now, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                       processed_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                       batch_id=batch_id).reset_index().rename(columns={"index": "station_id"})
        st["already_at_threshold"] = np.where(st["alert_type"] == "empty", st["bikes"] <= 1, st["docks"] <= 1)
        # The formula gives P = 1 when the station is already at the threshold (bikes or docks <= 1);
        # label those separately from forward-looking risk alerts.
        st["alert_kind"] = np.where(st["already_at_threshold"], "already_", "risk_") + st["alert_type"]
        cols = ["as_of", "processed_at", "batch_id", "station_id", "short_id", "name", "lat", "lng", "capacity",
                "bikes", "docks", "ebikes", "last_reported", "status", "alert_type", "probability", "level",
                "p_empty", "p_full", "already_at_threshold", "alert_kind", "exp_departures_30m", "exp_arrivals_30m",
                "weather_factor_dep", "weather_factor_arr", "temperature_2m", "precipitation",
                "change_5m", "change_15m", "change_30m", "is_installed", "is_renting", "is_returning"]
        st = st[cols]
        alerts = st[st["level"] != "none"]

        day = st["as_of"].iloc[0][:10]
        if len(alerts):
            d = self.out_dir / "alerts" / f"date={day}"
            d.mkdir(parents=True, exist_ok=True)
            tmp = d / f".batch-{batch_id:06d}.parquet.tmp"
            alerts.to_parquet(tmp, index=False)
            tmp.rename(d / f"batch-{batch_id:06d}.parquet")
            if self.producer:
                for rec in alerts.to_dict("records"):
                    self.producer.produce(self.alert_topic, key=rec["station_id"],
                                          value=json.dumps(rec, default=str))
                self.producer.flush(10)
        tmp = self.out_dir / ".latest_state.parquet.tmp"
        st.to_parquet(tmp, index=False)
        tmp.replace(self.out_dir / "latest_state.parquet")
        if self.state_history:
            h = self.out_dir / "state_history"
            h.mkdir(exist_ok=True)
            st.to_parquet(h / f"batch-{batch_id:06d}.parquet", index=False)

        dur = time.time() - t0
        self.durations.append(dur)
        counts = alerts["level"].value_counts().to_dict()
        risk = alerts[~alerts["already_at_threshold"].astype(bool)]
        rcounts = risk["level"].value_counts().to_dict()
        rec = {"batch_id": batch_id, "as_of": st["as_of"].iloc[0], "processed_at": st["processed_at"].iloc[0],
               "station_updates": len(upd), "stations_known": len(st),
               "in_service": int((st["status"] == "ok").sum()), "alerts_high": counts.get("HIGH", 0),
               "alerts_medium": counts.get("MEDIUM", 0),
               "risk_alerts_high": rcounts.get("HIGH", 0), "risk_alerts_medium": rcounts.get("MEDIUM", 0),
               "already_empty": int((alerts["alert_kind"] == "already_empty").sum()),
               "already_full": int((alerts["alert_kind"] == "already_full").sum()),
               "expected_buckets": used,
               "weather": {"factor_dep": f_dep, "factor_arr": f_arr, **winfo,
                           "temperature_2m": (w or {}).get("temperature_2m"),
                           "precipitation": (w or {}).get("precipitation")},
               "driver_seconds": round(dur, 2)}
        with open(self.out_dir / "alert_batches.jsonl", "a") as f:
            f.write(json.dumps(rec, default=str) + "\n")
        ny = pd.Timestamp(now, unit="s", tz="UTC").tz_convert(TZ)
        print(f"[{datetime.now():%H:%M:%S}] batch {batch_id}: {len(upd):,} station updates, data time "
              f"{ny:%a %Y-%m-%d %H:%M} NY, {rec['in_service']:,} in service; risk alerts: {rec['risk_alerts_high']} "
              f"HIGH, {rec['risk_alerts_medium']} MEDIUM; already empty {rec['already_empty']}, already full "
              f"{rec['already_full']}; weather x{f_dep:.2f} ({winfo.get('table_cell')}); "
              f"alert step {dur:.1f}s", flush=True)



def checkpoint_guard(ck: Path) -> None:
    """Move a checkpoint written by an incompatible state version aside (never delete it).
    A checkpoint without a version file is assumed compatible and gets stamped; if Spark still
    rejects it, scripts/start_stream.sh moves it aside and retries once."""
    marker = ck / "bikeflow_state_version"
    found = marker.read_text().strip() if marker.exists() else None
    if ck.exists() and found is not None and found != STATE_VERSION:
        aside = ck.with_name(f"{ck.name}.incompatible-{datetime.now():%Y%m%d-%H%M%S}")
        ck.rename(aside)
        print(f"[{datetime.now():%H:%M:%S}] checkpoint state version {found!r} != {STATE_VERSION!r}: "
              f"moved to {aside.name}, starting fresh", flush=True)
    ck.mkdir(parents=True, exist_ok=True)
    (ck / "bikeflow_state_version").write_text(STATE_VERSION + "\n")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default="127.0.0.1:9092")
    ap.add_argument("--topic", default="station_status")
    ap.add_argument("--alert-topic", default="bikeflow_alerts", help="'' to disable the Kafka alert sink")
    ap.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"])
    ap.add_argument("--trigger-seconds", type=int, default=60)
    ap.add_argument("--max-offsets", type=int, help="max Kafka messages per micro-batch")
    ap.add_argument("--out-dir", type=Path, default=ROOT / "data" / "live")
    ap.add_argument("--checkpoint", type=Path, default=ROOT / "data" / "checkpoints" / "stream_alerts")
    ap.add_argument("--available-now", action="store_true", help="process what is in Kafka, then stop (tests)")
    ap.add_argument("--state-history", action="store_true",
                    help="also write every batch's full station state to <out>/state_history/ (replay demos)")
    ap.add_argument("--stop-when-idle", type=int, metavar="SECONDS",
                    help="stop when no station update has arrived for this many seconds, once data was "
                         "seen (replay tests). Spark runs no micro-batch when Kafka has no new data, so this "
                         "is wall-clock time since the last batch that had updates.")
    a = ap.parse_args()

    check_disk("stream_alerts")
    checkpoint_guard(a.checkpoint.resolve())
    out_dir = a.out_dir.resolve()
    engine = AlertEngine(out_dir, a.bootstrap, a.alert_topic or None, state_history=a.state_history)
    spark = get_spark("bikeflow-stream-alerts", driver_memory="2g", cores=2, shuffle_partitions=16,
                      packages=KAFKA_PKG)
    try:  # always clean up this session's scratch dir, even if the query fails to start
        print(f"[{datetime.now():%H:%M:%S}] alert engine START: topic {a.topic} ({a.starting_offsets}), "
              f"trigger {a.trigger_seconds}s, out {out_dir}, checkpoint {a.checkpoint}", flush=True)
        reader = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", a.bootstrap)
                  .option("subscribe", a.topic).option("startingOffsets", a.starting_offsets)
                  .option("failOnDataLoss", "false"))
        if a.max_offsets:
            reader = reader.option("maxOffsetsPerTrigger", a.max_offsets)
        rows = (reader.load()
                .select(F.from_json(F.col("value").cast("string"), IN_SCHEMA).alias("j")).select("j.*")
                .filter(F.col("station_id").isNotNull() & F.col("last_reported").isNotNull())
                .withColumn("event_time", F.timestamp_seconds("last_reported"))
                .withWatermark("event_time", "5 minutes"))
        per_station = rows.groupBy("station_id").applyInPandasWithState(
            station_state, OUT_SCHEMA, STATE_SCHEMA, "update", GroupStateTimeout.EventTimeTimeout)
        writer = (per_station.writeStream.outputMode("update").foreachBatch(engine.process)
                  .option("checkpointLocation", str(a.checkpoint.resolve())))
        writer = writer.trigger(availableNow=True) if a.available_now else \
            writer.trigger(processingTime=f"{a.trigger_seconds} seconds")
        t0 = time.time()
        with ResourceMonitor(spark, interval=1.0, include_children=True) as mon:
            q = writer.start()
            try:
                if a.stop_when_idle:
                    while not q.awaitTermination(5):
                        idle = time.time() - engine.last_update_wall if engine.seen_updates else 0
                        if idle >= a.stop_when_idle:
                            print(f"[{datetime.now():%H:%M:%S}] no new data for {idle:.0f}s: stopping", flush=True)
                            q.stop()
                            break
                else:
                    q.awaitTermination()
            except KeyboardInterrupt:
                q.stop()
        prog = [p for p in (q.recentProgress or [])]
        durs = [p["durationMs"].get("triggerExecution", 0) / 1000 for p in prog
                if p.get("durationMs") and p.get("numInputRows", 0) > 0]  # batches that had data
        summary = {"batches": len(engine.durations), "wall_s": round(time.time() - t0, 1),
                   "micro_batch_s": {"mean": round(float(np.mean(durs)), 2) if durs else None,
                                     "max": round(float(np.max(durs)), 2) if durs else None},
                   "alert_step_s": {"mean": round(float(np.mean(engine.durations)), 2) if engine.durations else None,
                                    "max": round(float(np.max(engine.durations)), 2) if engine.durations else None},
                   "peak_rss_gb_spark_and_python_workers": round(mon.peak_rss_gb, 2),
                   "peak_swap_gb": round(mon.peak_swap_gb, 2)}
        (out_dir / "run_summary.json").write_text(json.dumps(summary, indent=2))
        print(f"[{datetime.now():%H:%M:%S}] alert engine END: {json.dumps(summary)}", flush=True)
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
