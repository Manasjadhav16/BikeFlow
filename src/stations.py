"""Build the station dimension from clean trips and map it to GBFS station UUIDs.

Trip files use short ids like "5329.03"; GBFS station_information uses UUID station_id
with the short id in short_name. Writes data/processed/stations/ and
outputs/metrics/station_mapping.json.

Memory: Spark aggregates one clean month at a time into small per-station tables
(data/processed/stations_parts/YYYY-MM/); they are combined in pandas. Name = most
frequent name over all months; lat/lng = trip-weighted mean of the monthly medians
(docked-station coordinates are fixed, so this equals the global median in practice).
"""
import math
import re
import time

import pandas as pd
import requests
from pyspark.sql import functions as F

from src.aggregate_demand import clean_months, month_path, record_progress
from src.common import (DATA_PROCESSED, DATA_RAW, METRICS_DIR, STATIONS_DIR, TRIPS_CLEAN_DIR,
                        ResourceMonitor, check_disk, get_spark, log_progress, stop_spark, write_json)

PARTS_DIR = DATA_PROCESSED / "stations_parts"

GBFS_STATION_INFO = "https://gbfs.lyft.com/gbfs/1.1/bkn/en/station_information.json"


def pad_short_name(short_name: str) -> str:
    """GBFS has the same lost-trailing-zero ids as the trips ("6131.1" for "6131.10"); see clean.py."""
    return short_name + "0" if re.fullmatch(r"\d+\.\d", short_name) else short_name


def fetch_gbfs() -> tuple[list[dict], int]:
    r = requests.get(GBFS_STATION_INFO, timeout=60)
    r.raise_for_status()
    dest = DATA_RAW / "gbfs" / "station_information.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(r.content)
    d = r.json()
    return d["data"]["stations"], d["last_updated"]


def haversine_m(lat1, lng1, lat2, lng2) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = (math.sin((p2 - p1) / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lng2 - lng1) / 2) ** 2)
    return 2 * 6_371_000 * math.asin(math.sqrt(a))


def stations_month(spark, ym: str) -> int:
    """Per-station partials for one source month; returns trips read."""
    trips = spark.read.parquet(str(month_path(TRIPS_CLEAN_DIR, ym)))
    ends = trips.select(
        F.col("start_station_id").alias("station_id"), F.col("start_station_name").alias("name"),
        F.col("start_lat").alias("lat"), F.col("start_lng").alias("lng"),
        F.lit(1).alias("dep"), F.lit(0).alias("arr"),
    ).unionByName(trips.filter(F.col("end_station_id").isNotNull()).select(
        F.col("end_station_id").alias("station_id"), F.col("end_station_name").alias("name"),
        F.col("end_lat").alias("lat"), F.col("end_lng").alias("lng"),
        F.lit(0).alias("dep"), F.lit(1).alias("arr"),
    ))
    parts = PARTS_DIR / ym
    parts.mkdir(parents=True, exist_ok=True)
    ends.groupBy("station_id", "name").count().toPandas() \
        .to_parquet(parts / "names.parquet", index=False)  # ~2.6k rows
    stats = ends.groupBy("station_id").agg(
        F.percentile_approx("lat", 0.5).alias("lat"),
        F.percentile_approx("lng", 0.5).alias("lng"),
        F.sum("dep").alias("departures"),
        F.sum("arr").alias("arrivals"),
    ).toPandas()  # ~2.5k rows
    stats.to_parquet(parts / "stats.parquet", index=False)
    return int(stats["departures"].sum())


def combine(months: list[str]) -> pd.DataFrame:
    names = (pd.concat([pd.read_parquet(PARTS_DIR / m / "names.parquet") for m in months])
             .dropna(subset=["name"]).groupby(["station_id", "name"], as_index=False)["count"].sum())
    top_name = names.sort_values(["station_id", "count", "name"], ascending=[True, False, True]) \
        .drop_duplicates("station_id").set_index("station_id")["name"]
    stats = pd.concat([pd.read_parquet(PARTS_DIR / m / "stats.parquet") for m in months])
    stats["w"] = stats["departures"] + stats["arrivals"]
    stats["wlat"], stats["wlng"] = stats["lat"] * stats["w"], stats["lng"] * stats["w"]
    dim = stats.groupby("station_id").agg(
        departures=("departures", "sum"), arrivals=("arrivals", "sum"),
        w=("w", "sum"), wlat=("wlat", "sum"), wlng=("wlng", "sum"))
    dim["lat"], dim["lng"] = dim["wlat"] / dim["w"], dim["wlng"] / dim["w"]
    dim["name"] = top_name
    dim["n_names"] = names.groupby("station_id")["name"].nunique()
    dim["n_names"] = dim["n_names"].fillna(0).astype(int)
    dim["trips"] = dim["departures"] + dim["arrivals"]
    return dim.reset_index()[["station_id", "name", "n_names", "lat", "lng", "departures", "arrivals", "trips"]]


def main() -> None:
    months = clean_months()
    spark = get_spark("bikeflow-stations")
    try:
        for m in months:
            check_disk(f"stations {m}")
            t0 = time.time()
            with ResourceMonitor(spark) as mon:
                n = stations_month(spark, m)
            record_progress(log_progress("stations", m, n, time.time() - t0, mon))
    finally:
        stop_spark(spark)
    dim = combine(months)

    gbfs, last_updated = fetch_gbfs()
    for s in gbfs:
        if s.get("short_name"):
            s["short_name"] = pad_short_name(s["short_name"])
    g = {s["short_name"]: s for s in gbfs if s.get("short_name")}
    dim["gbfs_station_id"] = dim["station_id"].map(lambda k: g.get(k, {}).get("station_id"))
    dim["gbfs_name"] = dim["station_id"].map(lambda k: g.get(k, {}).get("name"))
    dim["capacity"] = dim["station_id"].map(lambda k: g.get(k, {}).get("capacity"))
    dim["gbfs_lat"] = dim["station_id"].map(lambda k: g.get(k, {}).get("lat"))
    dim["gbfs_lng"] = dim["station_id"].map(lambda k: g.get(k, {}).get("lon"))
    dim["matched"] = dim["gbfs_station_id"].notna()
    m = dim[dim["matched"]]
    dist = [haversine_m(a, b, c, d) for a, b, c, d in zip(m["lat"], m["lng"], m["gbfs_lat"], m["gbfs_lng"])]
    dim.loc[dim["matched"], "gbfs_distance_m"] = dist
    dim = dim.sort_values("trips", ascending=False).reset_index(drop=True)

    STATIONS_DIR.mkdir(parents=True, exist_ok=True)
    dim.to_parquet(STATIONS_DIR / "stations.parquet", index=False)

    unmatched = dim[~dim["matched"]]
    trip_ids = set(dim["station_id"])
    gbfs_without_trips = [s for s in gbfs if s.get("short_name") not in trip_ids]
    dist_s = m.assign(d=dist)["d"] if len(m) else None
    report = {
        "gbfs_url": GBFS_STATION_INFO,
        "gbfs_last_updated": last_updated,
        "months": months,
        "trip_stations": len(dim),
        "gbfs_stations": len(gbfs),
        "matched_stations": int(dim["matched"].sum()),
        "match_rate_by_station": round(dim["matched"].mean(), 4),
        "match_rate_by_trips": round(m["trips"].sum() / dim["trips"].sum(), 4),
        "gbfs_stations_without_trips": len(gbfs_without_trips),
        "gbfs_duplicate_short_names": len(gbfs) - len(g),
        "matched_name_agreement_rate": round((m["name"] == m["gbfs_name"]).mean(), 4) if len(m) else None,
        "matched_distance_m": {
            "median": round(float(dist_s.median()), 1),
            "p95": round(float(dist_s.quantile(0.95)), 1),
            "over_200m": int((dist_s > 200).sum()),
        } if dist_s is not None else None,
        "stations_with_multiple_names": int((dim["n_names"] > 1).sum()),
        "top_unmatched_by_trips": unmatched.head(20)[["station_id", "name", "trips", "lat", "lng"]]
        .to_dict("records"),
        "sample_gbfs_without_trips": [
            {k: s.get(k) for k in ("short_name", "name", "station_id", "capacity")}
            for s in gbfs_without_trips[:20]
        ],
    }
    write_json(METRICS_DIR / "station_mapping.json", report)
    print(f"trip stations {report['trip_stations']:,} | GBFS {report['gbfs_stations']:,} | "
          f"matched {report['matched_stations']:,} "
          f"({report['match_rate_by_station']:.1%} of stations, {report['match_rate_by_trips']:.1%} of trips)")


if __name__ == "__main__":
    main()
