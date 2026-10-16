"""Reference tables for the live alert engine (batch, build once).

1. data/processed/reference/expected_demand.parquet
   Expected departures and arrivals per 15-minute bucket for each (station, day_of_week, hour,
   quarter) in New York time, for the stations that match the live GBFS feed, over all 12 months
   of demand_15min with disrupted slots excluded. demand_15min is sparse (only buckets with
   activity), so the mean divides the summed counts by the number of non-disrupted occurrences
   of that slot between the station's first and last observed ride (zero-activity slots count).

2. data/processed/reference/weather_adjustment.json (+ .parquet)
   Ratio of actual to expected system-wide departures and arrivals by precipitation bin x
   temperature band, using the Stage 2b hourly weather (record at the latest hour mark <= the
   bucket start; precipitation = previous hour). US federal holidays and the Feb 22-24
   shutdown window are excluded; weather-driven low days are kept (they are the signal). Each
   cell is shrunk toward the product of its two marginal ratios with weight n_hours / (n_hours
   + 24), then clipped to [0.2, 1.6].

3. data/processed/reference/stations_live.parquet
   Live station metadata (GBFS id, trip short id, name, lat, lon, capacity).

    python -m src.reference_tables
"""
import json
import time

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

from src.aggregate_demand import DISRUPTED_JSON, month_path
from src.build_features import demand_months, federal_holidays, weather_table
from src.collector import INFO_DIR
from src.common import (DATA_PROCESSED, DEMAND_DIR, METRICS_DIR, check_disk, get_spark, read_json,
                        stop_spark, write_json)
from src.live_map import mapping_table

REF_DIR = DATA_PROCESSED / "reference"
TZ = "America/New_York"
PRECIP_BINS = ["none", "light", "moderate", "heavy"]          # 0 | (0, 1) | [1, 4) | >= 4 mm/h
TEMP_BANDS = ["below 0", "0-10", "10-20", "20-28", "28+"]     # degC
SMOOTH_HOURS = 24
CLIP = (0.2, 1.6)


def precip_bin(p):
    p = np.asarray(p, dtype=float)
    return np.select([p <= 0, p < 1, p < 4], PRECIP_BINS[:3], default=PRECIP_BINS[3])


def temp_band(t):
    t = np.asarray(t, dtype=float)
    return np.select([t < 0, t < 10, t < 20, t < 28], TEMP_BANDS[:4], default=TEMP_BANDS[4])


def live_stations() -> pd.DataFrame:
    infos = sorted(INFO_DIR.glob("station_information_*.json"))
    if not infos:
        raise SystemExit(f"no live station_information in {INFO_DIR}; run the collector once first")
    info = pd.DataFrame(json.loads(infos[-1].read_text())["data"]["stations"])
    info = info.rename(columns={"station_id": "gbfs_station_id", "lon": "lng"})
    m = mapping_table()[["gbfs_station_id", "station_id"]]
    out = info.merge(m, on="gbfs_station_id", how="inner")
    return out[["gbfs_station_id", "station_id", "name", "lat", "lng", "capacity"]].assign(
        station_info_file=infos[-1].name)


def epochs(s: pd.Series) -> np.ndarray:
    return ((s - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta(seconds=1)).to_numpy()


def main() -> None:
    t_start = time.time()
    check_disk("reference tables")
    REF_DIR.mkdir(parents=True, exist_ok=True)
    live = live_stations()
    ids = sorted(live["station_id"])
    months = demand_months()
    spec = read_json(DISRUPTED_JSON)
    print(f"live stations matched to trip ids: {len(ids):,}; months: {months[0]}..{months[-1]}", flush=True)

    spark = get_spark("bikeflow-reference", driver_memory="2g", cores=2, shuffle_partitions=16)
    try:
        sums, spans, totals = [], [], []
        for m in months:
            d = (spark.read.parquet(str(month_path(DEMAND_DIR, m)))
                 .filter(F.col("station_id").isin(ids))
                 .withColumn("t", F.unix_timestamp("bucket_start"))
                 .withColumn("quarter", (F.minute("bucket_start") / 15).cast("int")))
            sums.append(d.filter(~F.col("disrupted_day"))
                        .groupBy("station_id", "day_of_week", "hour", "quarter")
                        .agg(F.sum("departures").alias("dep"), F.sum("arrivals").alias("arr")))
            spans.append(d.groupBy("station_id").agg(F.min("t").alias("first"), F.max("t").alias("last")).toPandas())
            totals.append(d.groupBy("t").agg(F.sum("departures").alias("dep"), F.sum("arrivals").alias("arr"))
                          .toPandas())  # <= 3k rows per month
            print(f"  aggregated {m}", flush=True)
        u = sums[0]
        for x in sums[1:]:
            u = u.unionByName(x)
        tmp = REF_DIR / "_sums_tmp"
        (u.groupBy("station_id", "day_of_week", "hour", "quarter")
         .agg(F.sum("dep").alias("dep"), F.sum("arr").alias("arr"))
         .write.mode("overwrite").parquet(str(tmp)))
    finally:
        stop_spark(spark)
    sums = pd.read_parquet(tmp)  # <= 2.5k stations x 672 slots
    spans = pd.concat(spans).groupby("station_id").agg(first=("first", "min"), last=("last", "max"))
    totals = pd.concat(totals).groupby("t").sum()

    # ---- calendar of 15-min slots (UTC epochs) with NYC slot type and disrupted flag ----
    t0 = pd.Timestamp(f"{months[0]}-01", tz=TZ).tz_convert("UTC")
    t1 = (pd.Timestamp(f"{months[-1]}-01", tz=TZ) + pd.offsets.MonthBegin(1)).tz_convert("UTC")
    slots = np.arange(epochs(pd.Series([t0]))[0], epochs(pd.Series([t1]))[0], 900)
    ny = pd.to_datetime(slots, unit="s", utc=True).tz_convert(TZ)
    dow = ny.dayofweek.to_numpy() + 1
    stype = (dow - 1) * 96 + ny.hour.to_numpy() * 4 + ny.minute.to_numpy() // 15
    dates = ny.strftime("%Y-%m-%d").to_numpy()
    disrupted = np.isin(dates, spec["days"])
    win = np.zeros(len(slots), bool)
    for lo, hi in spec["manual_windows"]:
        a, b = (epochs(pd.Series([pd.Timestamp(x, tz=TZ).tz_convert("UTC")]))[0] for x in (lo, hi))
        win |= (slots >= a) & (slots < b)
    disrupted |= win

    # ---- expected demand: sums / non-disrupted occurrences within each station's span ----
    st = spans.reindex(ids)
    first, last = st["first"].to_numpy(), st["last"].to_numpy()
    n = np.zeros((len(ids), 672), dtype=np.int32)
    for k in range(672):
        arr = slots[(stype == k) & ~disrupted]
        n[:, k] = np.searchsorted(arr, last, "right") - np.searchsorted(arr, first, "left")
    grid = pd.DataFrame({"station_id": np.repeat(ids, 672), "slot": np.tile(np.arange(672), len(ids)),
                         "n_slots": n.ravel()})
    grid["day_of_week"] = grid["slot"] // 96 + 1
    grid["hour"] = (grid["slot"] % 96) // 4
    grid["quarter"] = grid["slot"] % 4
    grid = grid.merge(sums, on=["station_id", "day_of_week", "hour", "quarter"], how="left").fillna({"dep": 0, "arr": 0})
    ok = grid["n_slots"] > 0
    grid["exp_departures"] = np.where(ok, grid["dep"] / grid["n_slots"].clip(lower=1), 0.0)
    grid["exp_arrivals"] = np.where(ok, grid["arr"] / grid["n_slots"].clip(lower=1), 0.0)
    exp = grid.merge(live[["station_id", "gbfs_station_id"]], on="station_id")
    cols = ["gbfs_station_id", "station_id", "day_of_week", "hour", "quarter",
            "exp_departures", "exp_arrivals", "n_slots"]
    exp[cols].to_parquet(REF_DIR / "expected_demand.parquet", index=False)
    live.to_parquet(REF_DIR / "stations_live.parquet", index=False)

    # ---- weather adjustment ----
    E_dep = grid["exp_departures"].to_numpy().reshape(len(ids), 672)
    E_arr = grid["exp_arrivals"].to_numpy().reshape(len(ids), 672)
    exp_dep, exp_arr = np.zeros(len(slots)), np.zeros(len(slots))
    day_idx = pd.Series(np.arange(len(slots))).groupby(dates).indices
    for day, idx in day_idx.items():
        active = (first <= slots[idx].max()) & (last >= slots[idx].min())
        exp_dep[idx] = E_dep[active][:, stype[idx]].sum(axis=0)
        exp_arr[idx] = E_arr[active][:, stype[idx]].sum(axis=0)
    act = totals.reindex(slots, fill_value=0)
    holidays = federal_holidays({int(m[:4]) for m in months})
    keep = ~np.isin(dates, sorted(holidays)) & ~win
    w = weather_table().set_index("hour_epoch")
    hour_mark = slots - slots % 3600
    wx = w.reindex(hour_mark)
    df = pd.DataFrame({"hour_mark": hour_mark, "act_dep": act["dep"].to_numpy(), "act_arr": act["arr"].to_numpy(),
                       "exp_dep": exp_dep, "exp_arr": exp_arr,
                       "precip": wx["precipitation"].to_numpy(), "temp": wx["temperature_2m"].to_numpy()})[keep]
    df = df.dropna(subset=["precip", "temp"])
    df["precip_bin"], df["temp_band"] = precip_bin(df["precip"]), temp_band(df["temp"])

    def ratios(g):
        return pd.Series({"n_hours": g["hour_mark"].nunique(), "act_dep": g["act_dep"].sum(),
                          "exp_dep": g["exp_dep"].sum(), "act_arr": g["act_arr"].sum(), "exp_arr": g["exp_arr"].sum()})

    cell = df.groupby(["precip_bin", "temp_band"]).apply(ratios, include_groups=False).reset_index()
    pm = df.groupby("precip_bin").apply(ratios, include_groups=False)
    tm = df.groupby("temp_band").apply(ratios, include_groups=False)
    overall = ratios(df)
    rows = []
    for _, c in cell.iterrows():
        r = {"precip_bin": c["precip_bin"], "temp_band": c["temp_band"], "n_hours": int(c["n_hours"])}
        for x in ("dep", "arr"):
            raw = c[f"act_{x}"] / c[f"exp_{x}"]
            r_all = overall[f"act_{x}"] / overall[f"exp_{x}"]
            prior = (pm.loc[c["precip_bin"], f"act_{x}"] / pm.loc[c["precip_bin"], f"exp_{x}"]) * \
                    (tm.loc[c["temp_band"], f"act_{x}"] / tm.loc[c["temp_band"], f"exp_{x}"]) / r_all
            sm = (c["n_hours"] * raw + SMOOTH_HOURS * prior) / (c["n_hours"] + SMOOTH_HOURS)
            r[f"ratio_{x}_raw"] = round(float(raw), 4)
            r[f"factor_{x}"] = round(float(np.clip(sm, *CLIP)), 4)
        rows.append(r)
    table = pd.DataFrame(rows)
    order = {b: i for i, b in enumerate(PRECIP_BINS)}, {b: i for i, b in enumerate(TEMP_BANDS)}
    table = table.sort_values(["precip_bin", "temp_band"], key=lambda s: s.map(order[0] if s.name == "precip_bin"
                                                                                  else order[1]))
    adj = {
        "method": "factor = smoothed (actual / expected) system-wide 15-min departures/arrivals; "
                  f"shrunk toward product of marginal ratios with weight n/(n+{SMOOTH_HOURS}), clipped {CLIP}",
        "precip_bins_mm_per_hour": {"none": "0", "light": "(0, 1)", "moderate": "[1, 4)", "heavy": ">= 4"},
        "temp_bands_c": {"below 0": "< 0", "0-10": "[0, 10)", "10-20": "[10, 20)", "20-28": "[20, 28)", "28+": ">= 28"},
        "excluded": "US federal holidays and the 2026-02-22 20:00 - 02-24 06:00 shutdown window",
        "overall_ratio": {"dep": round(float(overall["act_dep"] / overall["exp_dep"]), 4),
                          "arr": round(float(overall["act_arr"] / overall["exp_arr"]), 4)},
        "marginal_precip": {b: round(float(pm.loc[b, "act_dep"] / pm.loc[b, "exp_dep"]), 4) for b in pm.index},
        "marginal_temp": {b: round(float(tm.loc[b, "act_dep"] / tm.loc[b, "exp_dep"]), 4) for b in tm.index},
        "table": table.to_dict("records"),
    }
    write_json(REF_DIR / "weather_adjustment.json", adj)
    write_json(METRICS_DIR / "weather_adjustment.json", adj)  # reviewable copy (small)
    table.to_parquet(REF_DIR / "weather_adjustment.parquet", index=False)
    import shutil
    shutil.rmtree(tmp, ignore_errors=True)
    print(f"expected_demand: {len(exp):,} rows ({exp['station_id'].nunique():,} stations x 672 slots); "
          f"mean exp departures per bucket {exp['exp_departures'].mean():.3f}")
    print(table.to_string(index=False))
    print(f"done in {time.time() - t_start:.0f}s")


if __name__ == "__main__":
    main()
