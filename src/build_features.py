"""Build the Stage 2 feature table from demand_15min, one target month at a time.

One row = (station, ts), where ts is the prediction time: the end of the latest complete
15-minute slot. With slot t = [ts-15m, ts):

    change(t)                = arrivals(t) - departures(t)        (bikes gained in slot t)
    bike_change_last_15m     = change(t)                          (past only)
    bike_change_last_30m     = change(t-1) + change(t)            (past only)
    hist_same_slot_4w        = mean over k=1..4 of target(ts - k weeks); null unless all
                               4 weeks exist (needs data from 4+ weeks back)
    target bike_change_next_30m = change(t+1) + change(t+2)       (the next 30 min)
    hour, day_of_week (ISO), month, is_weekend                    (of ts, NYC time)
    station_idx, lat, lng                                         (static)
    disrupted                 = any of slots t-1..t+2 is in a disrupted window/day

Only stations matched to the live GBFS feed are kept. Each station is densified with
zero-activity slots between its first and last observed ride (not before or after).
Target month M is built from demand months M-1, M and M+1, so the lags, leads and
4-week history at the month edges are exact. Output: data/processed/features/
(year=/month= of ts). Station index -> models/station_index.json.

    python -m src.build_features --months 2025-11     # one month (dry run)
    python -m src.build_features                      # every month with 4 weeks of history
"""
import argparse
import time

import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.aggregate_demand import DISRUPTED_JSON, disrupted_col, month_path, shift
from src.common import (DATA_PROCESSED, DEMAND_DIR, ROOT, STATIONS_DIR, ResourceMonitor, check_disk,
                        get_spark, log_progress, read_json, stop_spark, write_json)
from src.aggregate_demand import record_progress

FEATURES_DIR = DATA_PROCESSED / "features"
STATION_INDEX_JSON = ROOT / "models" / "station_index.json"
FEATURES_MARKER = "_BIKEFLOW_FEATURES"
SLOTS_PER_WEEK = 7 * 96
FEATURE_COLS = ["hour", "day_of_week", "month", "is_weekend", "station_idx", "lat", "lng",
                "bike_change_last_15m", "bike_change_last_30m", "hist_same_slot_4w"]
TARGET = "bike_change_next_30m"


def demand_months() -> list[str]:
    return sorted(
        f"{int(p.parent.name.split('=')[1]):04d}-{int(p.name.split('=')[1]):02d}"
        for p in DEMAND_DIR.glob("year=*/month=*") if (p / "_BIKEFLOW_DEMAND").exists()
    )


def station_table(spark, months: list[str]) -> pd.DataFrame:
    """GBFS-matched stations with a stable index and their first/last observed slot."""
    st = pd.read_parquet(STATIONS_DIR / "stations.parquet")
    st = st[st["matched"]].sort_values("station_id").reset_index(drop=True)
    st["station_idx"] = st.index.astype(int)
    span = pd.concat([
        spark.read.parquet(str(month_path(DEMAND_DIR, m))).groupBy("station_id")
        .agg(F.min("bucket_start").alias("first_slot"), F.max("bucket_start").alias("last_slot"))
        .toPandas() for m in months])  # ~2.5k rows per month
    span = span.groupby("station_id").agg(first_slot=("first_slot", "min"), last_slot=("last_slot", "max"))
    st = st.merge(span, on="station_id", how="inner")
    STATION_INDEX_JSON.parent.mkdir(parents=True, exist_ok=True)
    write_json(STATION_INDEX_JSON, {
        "note": "station_idx used by the model; GBFS-matched stations sorted by trip short id",
        "stations": st[["station_id", "station_idx", "gbfs_station_id", "name", "lat", "lng"]].to_dict("records"),
    })
    return st[["station_id", "station_idx", "lat", "lng", "first_slot", "last_slot"]]


def features_month(spark, ym: str, months: list[str], stations, spec: dict) -> int:
    load = [m for m in (shift(ym, -1), ym, shift(ym, 1)) if m in months]
    lo = F.to_timestamp(F.lit(f"{load[0]}-01 00:00:00"))
    hi = F.to_timestamp(F.lit(f"{shift(load[-1], 1)}-01 00:00:00")) - F.expr("INTERVAL 15 MINUTES")
    demand = (spark.read.parquet(*[str(month_path(DEMAND_DIR, m)) for m in load])
              .select("station_id", "bucket_start", "arrivals", "departures"))

    # Dense grid: every 15-min slot between the station's first and last ride, within the load window.
    grid = (stations
            .withColumn("g0", F.greatest(F.col("first_slot"), lo))
            .withColumn("g1", F.least(F.col("last_slot"), hi))
            .filter(F.col("g0") <= F.col("g1"))
            .withColumn("bucket_start", F.explode(F.expr("sequence(g0, g1, INTERVAL 15 MINUTES)")))
            .select("station_id", "station_idx", "lat", "lng", "bucket_start"))
    dense = (grid.join(demand, ["station_id", "bucket_start"], "left")
             .fillna(0, ["arrivals", "departures"])
             .withColumn("change", F.col("arrivals") - F.col("departures"))
             .withColumn("slot_disrupted", disrupted_col("bucket_start", spec).cast("int")))

    w = Window.partitionBy("station_id").orderBy("bucket_start")
    f = (dense
         .withColumn("bike_change_last_15m", F.col("change"))
         .withColumn("bike_change_last_30m", F.col("change") + F.lag("change", 1).over(w))
         .withColumn(TARGET, F.lead("change", 1).over(w) + F.lead("change", 2).over(w))
         .withColumn("disrupted", F.max("slot_disrupted").over(w.rowsBetween(-1, 2)) == 1))
    # Same slot 1..4 weeks earlier: the target there only uses slots < ts - 1 week + 30 min, all past.
    past = [F.lag(TARGET, k * SLOTS_PER_WEEK).over(w) for k in range(1, 5)]
    f = f.withColumn("hist_same_slot_4w", sum(past) / F.lit(4.0))

    ts = F.col("bucket_start") + F.expr("INTERVAL 15 MINUTES")
    dow = ((F.dayofweek("ts") + 5) % 7) + 1  # ISO: Monday=1 .. Sunday=7
    out = (f.withColumn("ts", ts)
           .filter((F.col("ts") >= F.to_timestamp(F.lit(f"{ym}-01 00:00:00")))
                   & (F.col("ts") < F.to_timestamp(F.lit(f"{shift(ym, 1)}-01 00:00:00"))))
           .filter(F.col(TARGET).isNotNull() & F.col("bike_change_last_30m").isNotNull())
           .withColumn("hour", F.hour("ts"))
           .withColumn("day_of_week", dow)
           .withColumn("is_weekend", (dow >= 6).cast("int"))
           .withColumn("month", F.month("ts"))
           .withColumn("year", F.year("ts"))
           .select("station_id", "ts", *FEATURE_COLS, TARGET, "disrupted", "year"))
    out.write.mode("overwrite").partitionBy("year", "month").parquet(str(FEATURES_DIR))
    part = FEATURES_DIR / f"year={int(ym[:4])}" / f"month={int(ym[5:])}"
    (part / FEATURES_MARKER).touch()
    return spark.read.parquet(str(part)).count()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--months", nargs="+", metavar="YYYY-MM", help="only these target months")
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()

    months = demand_months()
    spec = read_json(DISRUPTED_JSON)
    if not spec:
        raise SystemExit(f"{DISRUPTED_JSON} missing: run src.aggregate_demand first")
    # 4 weeks of history are needed, so the first loaded month only feeds history.
    targets = a.months or months[1:]
    bad = [m for m in targets if m not in months[1:]]
    if bad:
        raise SystemExit(f"no demand (or no prior month for history) for: {bad}")

    spark = get_spark("bikeflow-features")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    try:
        stations = spark.createDataFrame(station_table(spark, months))
        stations = F.broadcast(stations)
        for m in targets:
            part = FEATURES_DIR / f"year={int(m[:4])}" / f"month={int(m[5:])}"
            if not a.force and (part / FEATURES_MARKER).exists():
                print(f"{m}: done, skipping")
                continue
            check_disk(f"features {m}")
            t0 = time.time()
            with ResourceMonitor(spark) as mon:
                rows = features_month(spark, m, months, stations, spec)
            record_progress(log_progress("features", m, rows, time.time() - t0, mon))
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
