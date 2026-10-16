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

Stage 2b additions:
    bike_change_last_60m      = change(t-3) + ... + change(t)
    cell_bike_change_last_30m = mean bike_change_last_30m of the OTHER stations in the same
                                ~500 m grid cell at the same ts (0 if the station is alone)
    is_holiday                = ts date is a US federal holiday (actual or observed date)
    temperature_2m, precipitation, wind_speed_10m = Open-Meteo hourly record at the latest
                                hour <= ts (precipitation = sum over the hour before it)
    precip_last_3h            = precipitation over the 3 hours before that hour mark
    targets departures_next_30m / arrivals_next_30m over slots t+1, t+2
    departures_last_30m / arrivals_last_30m: persistence baseline only (not model features:
                                live GBFS shows net stock changes, not departures/arrivals)

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
from datetime import date, timedelta

from pathlib import Path

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
                "bike_change_last_15m", "bike_change_last_30m", "hist_same_slot_4w"]  # Stage 2
TARGET = "bike_change_next_30m"
WEATHER_COLS = ["temperature_2m", "precipitation", "wind_speed_10m", "precip_last_3h"]
FLOW_FEATURES_NO_WEATHER = FEATURE_COLS + ["bike_change_last_60m", "cell_bike_change_last_30m", "is_holiday"]
FLOW_FEATURES = FLOW_FEATURES_NO_WEATHER + WEATHER_COLS
FLOW_TARGETS = ["departures_next_30m", "arrivals_next_30m"]
CELL_LAT_DEG, CELL_LNG_DEG = 0.0045, 0.0059  # ~500 m x ~500 m at NYC latitude
WEATHER_PARQUET = DATA_PROCESSED / "weather" / "weather_hourly.parquet"


def federal_holidays(years) -> set[str]:
    """US federal holidays (actual date and, for fixed dates on a weekend, the observed date)."""
    def nth(y, m, wd, n):  # n-th weekday wd (Mon=0) of month; n=-1 for the last
        if n > 0:
            d = date(y, m, 1)
            return d + timedelta(days=(wd - d.weekday()) % 7 + 7 * (n - 1))
        d = date(y, m + 1, 1) - timedelta(days=1) if m < 12 else date(y, 12, 31)
        return d - timedelta(days=(d.weekday() - wd) % 7)
    out = set()
    for y in years:
        for m, d in ((1, 1), (6, 19), (7, 4), (11, 11), (12, 25)):
            day = date(y, m, d)
            out.add(day)
            if day.weekday() == 5:
                out.add(day - timedelta(days=1))
            elif day.weekday() == 6:
                out.add(day + timedelta(days=1))
        out |= {nth(y, 1, 0, 3), nth(y, 2, 0, 3), nth(y, 5, 0, -1), nth(y, 9, 0, 1),
                nth(y, 10, 0, 2), nth(y, 11, 3, 4)}  # MLK, Presidents, Memorial, Labor, Columbus, Thanksgiving
    return {str(d) for d in out}


def weather_table() -> pd.DataFrame:
    """Hourly weather keyed by the hour mark (UTC instant), with precip over the previous 3 hours."""
    w = pd.read_parquet(WEATHER_PARQUET).sort_values("hour_utc")
    w["precip_last_3h"] = w["precipitation"].rolling(3, min_periods=1).sum()  # hours H-2, H-1, H
    # Time-zone-free join key; independent of the datetime64 resolution pandas picks.
    w["hour_epoch"] = (w["hour_utc"] - pd.Timestamp("1970-01-01")) // pd.Timedelta(seconds=1)
    return w.drop(columns=["hour_utc"])


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


def features_month(spark, ym: str, months: list[str], stations, spec: dict, weather, holidays,
                   with_cell: bool = True) -> int:
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
         .withColumn("bike_change_last_60m", F.col("bike_change_last_30m")
                     + F.lag("change", 2).over(w) + F.lag("change", 3).over(w))
         .withColumn("departures_last_30m", F.col("departures") + F.lag("departures", 1).over(w))
         .withColumn("arrivals_last_30m", F.col("arrivals") + F.lag("arrivals", 1).over(w))
         .withColumn(TARGET, F.lead("change", 1).over(w) + F.lead("change", 2).over(w))
         .withColumn("departures_next_30m", F.lead("departures", 1).over(w) + F.lead("departures", 2).over(w))
         .withColumn("arrivals_next_30m", F.lead("arrivals", 1).over(w) + F.lead("arrivals", 2).over(w))
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
           .withColumn("is_holiday", F.date_format("ts", "yyyy-MM-dd").isin(sorted(holidays)).cast("int"))
           # Weather: the latest hourly record at or before ts (hour marks align in UTC and NYC).
           .withColumn("hour_epoch", F.unix_timestamp(F.date_trunc("hour", "ts")))
           .join(weather, "hour_epoch", "left")
           .drop("hour_epoch"))
    if with_cell:
        # Mean last-30-min change of the OTHER stations in the same ~500 m cell, same ts.
        cell = F.concat_ws("_", F.floor(F.col("lat") / CELL_LAT_DEG), F.floor(F.col("lng") / CELL_LNG_DEG))
        out = out.withColumn("cell", cell)
        agg = out.groupBy("cell", "ts").agg(F.sum("bike_change_last_30m").alias("cs"), F.count(F.lit(1)).alias("cn"))
        out = (out.join(agg, ["cell", "ts"], "left")
               .withColumn("cell_bike_change_last_30m",
                           F.when(F.col("cn") > 1, (F.col("cs") - F.col("bike_change_last_30m")) / (F.col("cn") - 1))
                           .otherwise(F.lit(0.0)))
               .drop("cell", "cs", "cn"))
    else:
        out = out.withColumn("cell_bike_change_last_30m", F.lit(None).cast("double"))
    out = out.select("station_id", "ts", *FLOW_FEATURES, TARGET, *FLOW_TARGETS,
                     "departures_last_30m", "arrivals_last_30m", "disrupted", "year")
    out.write.mode("overwrite").partitionBy("year", "month").parquet(str(FEATURES_DIR))
    part = FEATURES_DIR / f"year={int(ym[:4])}" / f"month={int(ym[5:])}"
    (part / FEATURES_MARKER).touch()
    return spark.read.parquet(str(part)).count()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--months", nargs="+", metavar="YYYY-MM", help="only these target months")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--no-cell", action="store_true", help="skip the grid-cell feature (timing comparison)")
    ap.add_argument("--out", help="write to this directory instead of data/processed/features")
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

    global FEATURES_DIR
    if a.out:
        FEATURES_DIR = Path(a.out)
    spark = get_spark("bikeflow-features")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    try:
        stations = spark.createDataFrame(station_table(spark, months))
        stations = F.broadcast(stations)
        weather = F.broadcast(spark.createDataFrame(weather_table()))
        holidays = federal_holidays({int(m[:4]) for m in months})
        for m in targets:
            part = FEATURES_DIR / f"year={int(m[:4])}" / f"month={int(m[5:])}"
            if not a.force and (part / FEATURES_MARKER).exists():
                print(f"{m}: done, skipping")
                continue
            check_disk(f"features {m}")
            t0 = time.time()
            with ResourceMonitor(spark) as mon:
                rows = features_month(spark, m, months, stations, spec, weather, holidays,
                                      with_cell=not a.no_cell)
            record_progress(log_progress("features", m, rows, time.time() - t0, mon))
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
