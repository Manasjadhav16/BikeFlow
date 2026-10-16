"""Aggregate clean trips into per-station 15-minute demand, one month at a time, then
build the EDA summary and charts from the small per-month aggregates.

For target month T, departures are rides whose started_at falls in T (bucketed by start
station) and arrivals are rides whose ended_at falls in T (bucketed by end station). Both
are read from source months T-1, T and T+1, so rides that cross a month boundary land in
the right bucket; boundary rides present in two source files are counted once (see
docs/data_notes.md). Rides with no end station count as departures only.

Output rows are sparse: a (station, bucket) row exists only if it had at least one
departure or arrival. Times are America/New_York.

Memory: each Spark job touches at most 3 source months; the EDA step only combines
small pandas tables (daily, hourly, per-station) saved per month under demand_parts/.

    python -m src.aggregate_demand --months 2026-06      # one month (dry run)
    python -m src.aggregate_demand                       # every clean month not yet done
    python -m src.aggregate_demand --finalize-only       # rebuild summary + charts
"""
import argparse
import time

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from src.common import (CHARTS_DIR, DATA_PROCESSED, DEMAND_DIR, METRICS_DIR, STATIONS_DIR,  # noqa: E402
                        TRIPS_CLEAN_DIR, ResourceMonitor, check_disk, get_spark, log_progress,
                        read_json, stop_spark, write_json)

BUCKET = "15 minutes"
PARTS_DIR = DATA_PROCESSED / "demand_parts"
DEMAND_MARKER = "_BIKEFLOW_DEMAND"
PROGRESS_JSON = METRICS_DIR / "build_progress.json"
DISRUPTED_JSON = METRICS_DIR / "disrupted_days.json"
# Disruptions confirmed by inspection (docs/data_notes.md §7). Times are NYC local.
MANUAL_WINDOWS = [("2026-02-22 20:00:00", "2026-02-24 06:00:00")]  # system-wide shutdown
MANUAL_DAYS = ["2026-01-25", "2026-09-27"]
DISRUPTED_RATIO = 0.4   # day rides < 40% of the local same-weekday median
DISRUPTED_WEEKS = 4     # median over the same weekday within +-4 weeks (excluding the day)
WEEKDAY_COLOR, WEEKEND_COLOR = "#2a78d6", "#eb6834"
INK, INK_MUTED, GRID = "#0b0b0b", "#52514e", "#e4e3df"
GRID_LINE = "#a9a8a2"  # muted line for raw daily values under the 7-day mean


def clean_months() -> list[str]:
    """Months in trips_clean that passed verification (marker present)."""
    return sorted(
        f"{int(p.parent.name.split('=')[1]):04d}-{int(p.name.split('=')[1]):02d}"
        for p in TRIPS_CLEAN_DIR.glob("year=*/month=*") if (p / "_BIKEFLOW_CLEAN").exists()
    )


def month_path(base, ym: str):
    return base / f"year={int(ym[:4])}" / f"month={int(ym[5:])}"


def shift(ym: str, k: int) -> str:
    return (pd.Period(ym, "M") + k).strftime("%Y-%m")


def record_progress(rec: dict) -> None:
    log = read_json(PROGRESS_JSON)
    log.setdefault("records", []).append(rec)
    write_json(PROGRESS_JSON, log)


def compute_disrupted(spark, months: list[str]) -> dict:
    """Disrupted days: manual windows/days plus any day with rides below DISRUPTED_RATIO of the
    median of the same weekday within +-DISRUPTED_WEEKS weeks. The median is local, not
    year-wide: a year-wide weekday median flags ordinary winter days (50 days, 20 of them in
    February) instead of disruptions."""
    parts = [spark.read.parquet(str(month_path(TRIPS_CLEAN_DIR, m)))
             .groupBy(F.to_date("started_at").alias("date")).count().toPandas() for m in months]  # <=32 rows each
    days = pd.date_range(months[0] + "-01", pd.Timestamp(months[-1] + "-01") + pd.offsets.MonthEnd(0))
    rides = pd.concat(parts).assign(date=lambda x: pd.to_datetime(x["date"])).groupby("date")["count"].sum()
    rides = rides.reindex(days, fill_value=0)
    auto = []
    for t in days:
        nb = [t + pd.Timedelta(weeks=k) for k in range(-DISRUPTED_WEEKS, DISRUPTED_WEEKS + 1) if k]
        med = rides[[x for x in nb if x in rides.index]].median()
        if med > 0 and rides[t] < DISRUPTED_RATIO * med:
            auto.append({"date": str(t.date()), "rides": int(rides[t]), "local_weekday_median": float(med),
                         "ratio": round(float(rides[t] / med), 3)})
    spec = {
        "rule": f"day rides < {DISRUPTED_RATIO:.0%} of the median of the same weekday within "
                f"+-{DISRUPTED_WEEKS} weeks (day itself excluded); plus manual windows/days",
        "manual_windows": [list(w) for w in MANUAL_WINDOWS],
        "manual_days": MANUAL_DAYS,
        "auto_days": auto,
        "days": sorted(set(MANUAL_DAYS) | {a["date"] for a in auto}),
    }
    write_json(DISRUPTED_JSON, spec)
    return spec


def disrupted_col(col: str, spec: dict):
    """True if the 15-min bucket starting at `col` is in a disrupted window or day (NYC time)."""
    flag = F.to_date(col).isin(spec["days"])
    for lo, hi in spec["manual_windows"]:
        flag = flag | ((F.col(col) >= F.to_timestamp(F.lit(lo))) & (F.col(col) < F.to_timestamp(F.lit(hi))))
    return flag


def demand_month(spark, ym: str, available: list[str], spec: dict) -> int:
    """Build demand_15min for one target month and its small EDA parts. Returns demand rows."""
    sources = [str(month_path(TRIPS_CLEAN_DIR, m)) for m in (shift(ym, -1), ym, shift(ym, 1)) if m in available]
    trips = spark.read.option("basePath", str(TRIPS_CLEAN_DIR)).parquet(*sources)
    # Files are split by ended_at month, except April 2026 (split by started_at), so a ride that
    # crosses that boundary is in two files. Only such boundary rides can repeat across files
    # (ride_id is verified unique within a month), so dedupe just those: a few thousand rows.
    outside = ((F.year("started_at") != F.col("year")) | (F.month("started_at") != F.col("month"))
               | (F.year("ended_at") != F.col("year")) | (F.month("ended_at") != F.col("month")))
    trips = trips.filter(~outside).unionByName(trips.filter(outside).dropDuplicates(["ride_id"]))
    # String bounds are parsed in the session time zone (NYC); range filters push down to Parquet.
    lo = F.to_timestamp(F.lit(f"{ym}-01 00:00:00"))
    hi = F.to_timestamp(F.lit(f"{shift(ym, 1)}-01 00:00:00"))

    dep = (trips.filter((F.col("started_at") >= lo) & (F.col("started_at") < hi))
           .groupBy(F.col("start_station_id").alias("station_id"),
                    F.window("started_at", BUCKET).start.alias("bucket_start"))
           .agg(F.count(F.lit(1)).alias("departures")))
    arr = (trips.filter(F.col("end_station_id").isNotNull() & (F.col("ended_at") >= lo) & (F.col("ended_at") < hi))
           .groupBy(F.col("end_station_id").alias("station_id"),
                    F.window("ended_at", BUCKET).start.alias("bucket_start"))
           .agg(F.count(F.lit(1)).alias("arrivals")))
    dow = ((F.dayofweek("bucket_start") + 5) % 7) + 1  # ISO: Monday=1 .. Sunday=7
    demand = (
        dep.join(arr, ["station_id", "bucket_start"], "full_outer")
        .fillna(0, ["departures", "arrivals"])
        .withColumn("net_outflow", F.col("departures") - F.col("arrivals"))
        .withColumn("hour", F.hour("bucket_start"))
        .withColumn("day_of_week", dow)
        .withColumn("is_weekend", dow >= 6)
        .withColumn("disrupted_day", disrupted_col("bucket_start", spec))
        .withColumn("year", F.lit(int(ym[:4])))
        .withColumn("month", F.lit(int(ym[5:])))
        .select("station_id", "bucket_start", "departures", "arrivals", "net_outflow",
                "hour", "day_of_week", "month", "is_weekend", "disrupted_day", "year")
    )
    demand.write.mode("overwrite").partitionBy("year", "month").parquet(str(DEMAND_DIR))

    # Small per-month aggregates for EDA, read back from the partition just written.
    d = spark.read.parquet(str(month_path(DEMAND_DIR, ym))).withColumn("date", F.to_date("bucket_start"))
    parts = PARTS_DIR / ym
    parts.mkdir(parents=True, exist_ok=True)
    daily = (d.groupBy("date", "is_weekend")
             .agg(F.sum("departures").alias("departures"), F.sum("arrivals").alias("arrivals"),
                  F.count(F.lit(1)).alias("rows"))
             .toPandas())  # <= 31 rows
    daily.to_parquet(parts / "daily.parquet", index=False)
    d.groupBy("is_weekend", "hour").agg(F.sum("departures").alias("dep")).toPandas() \
        .to_parquet(parts / "hourly.parquet", index=False)  # 48 rows
    am = (~F.col("is_weekend")) & F.col("hour").between(7, 9)
    d.groupBy("station_id").agg(
        F.sum("departures").alias("departures"), F.sum("arrivals").alias("arrivals"),
        F.sum("net_outflow").alias("net_outflow"),
        F.sum(F.when(am, F.col("net_outflow")).otherwise(0)).alias("am_net_outflow"),
    ).toPandas().to_parquet(parts / "stations.parquet", index=False)  # ~2.5k rows
    (month_path(DEMAND_DIR, ym) / DEMAND_MARKER).touch()
    return int(daily["rows"].sum())


def finalize(months: list[str]) -> dict:
    """EDA summary + charts from the per-month parts (pandas only, no Spark)."""
    daily = pd.concat([pd.read_parquet(PARTS_DIR / m / "daily.parquet") for m in months])
    daily = daily.sort_values("date").reset_index(drop=True)
    hourly_raw = pd.concat([pd.read_parquet(PARTS_DIR / m / "hourly.parquet") for m in months])
    hourly_raw = hourly_raw.groupby(["is_weekend", "hour"], as_index=False)["dep"].sum()
    by_station = (pd.concat([pd.read_parquet(PARTS_DIR / m / "stations.parquet") for m in months])
                  .groupby("station_id", as_index=False).sum())

    dq = read_json(METRICS_DIR / "data_quality.json")
    quarantined = sorted(dq.get("quarantined_months", {}))
    per_month = {m: v for m, v in dq.get("per_month", {}).items() if m in months}
    # Rides present in two source files are counted once in demand (see demand_month).
    obs = dq.get("observations", {})
    dupes = sum(obs.get("cross_month_duplicate_ride_ids_adjacent", {}).values())
    dupes_end = sum(obs.get("cross_month_duplicate_ride_ids_adjacent_with_end_station", {}).values())
    expected_dep = sum(v["output"] for v in per_month.values()) - dupes
    expected_arr = sum(v["output"] - v["end_station_missing"] for v in per_month.values()) - dupes_end

    names = pd.read_parquet(STATIONS_DIR / "stations.parquet", columns=["station_id", "name"])
    by_station = by_station.merge(names, on="station_id", how="left")
    by_station["trips"] = by_station["departures"] + by_station["arrivals"]

    n_days = daily.groupby("is_weekend")["date"].nunique()
    hourly = hourly_raw.pivot(index="hour", columns="is_weekend", values="dep").reindex(range(24)).fillna(0)
    hourly = pd.DataFrame({
        "hour": range(24),
        "weekday": (hourly[False] / n_days[False]).values,
        "weekend": (hourly[True] / n_days[True]).values,
    })
    top = by_station.nlargest(15, "trips").copy()
    top["label"] = top["name"].fillna(top["station_id"])
    am_days = n_days[False]
    am_cols = ["station_id", "name", "am_net_outflow"]
    rows, n_st, n_d = int(daily["rows"].sum()), len(by_station), int(daily["date"].nunique())
    lo, hi = pd.Timestamp(months[0] + "-01"), pd.Timestamp(months[-1] + "-01") + pd.offsets.MonthEnd(0)
    dep_total, arr_total = int(daily["departures"].sum()), int(daily["arrivals"].sum())
    monthly = daily.assign(m=pd.to_datetime(daily["date"]).dt.strftime("%Y-%m")).groupby("m")["departures"].sum()

    summary = {
        "date_range": [str(lo.date()), str(hi.date())],
        "months": months,
        "quarantined_months": quarantined,
        "rows": rows,
        "stations": n_st,
        "days": n_d,
        "total_departures": dep_total,
        "total_arrivals": arr_total,
        "rides_per_month": {k: int(v) for k, v in monthly.items()},
        "dense_grid_rows": n_st * n_d * 96,
        "sparsity_nonzero_share": round(rows / (n_st * n_d * 96), 4),
        "cross_file_duplicate_rides_counted_once": dupes,
        "dropped_edge_rides": {
            "note": "clean rides whose start (departure) or end (arrival) falls outside the loaded months",
            "departures": expected_dep - dep_total,
            "arrivals": expected_arr - arr_total,
        },
        "busiest_stations": top[["station_id", "name", "departures", "arrivals", "trips"]].to_dict("records"),
        "hourly_profile_avg_departures_per_day": hourly.round(1).to_dict("records"),
        "weekday_vs_weekend": {
            label: {
                "days": int(n_days[flag]),
                "total_departures": int(daily.loc[daily["is_weekend"] == flag, "departures"].sum()),
                "avg_departures_per_day": round(float(daily.loc[daily["is_weekend"] == flag, "departures"].mean()), 1),
                "peak_hour": int(hourly.loc[hourly[label].idxmax(), "hour"]),
            } for label, flag in (("weekday", False), ("weekend", True))
        },
        "weekday_am_peak_7_10": {
            "note": "avg net_outflow per weekday 07:00-09:59; positive = bikes drain, negative = docks fill",
            "top_draining": by_station.nlargest(10, "am_net_outflow")[am_cols]
            .assign(am_net_outflow=lambda x: (x["am_net_outflow"] / am_days).round(1)).to_dict("records"),
            "top_filling": by_station.nsmallest(10, "am_net_outflow")[am_cols]
            .assign(am_net_outflow=lambda x: (x["am_net_outflow"] / am_days).round(1)).to_dict("records"),
        },
        "disrupted_days": read_json(DISRUPTED_JSON).get("days", []),
        "busiest_day": {k: str(v) for k, v in daily.loc[daily["departures"].idxmax(), ["date", "departures"]].items()},
        "quietest_day": {k: str(v) for k, v in daily.loc[daily["departures"].idxmin(), ["date", "departures"]].items()},
    }
    summary["charts"] = charts(hourly, top, daily) + [monthly_chart(daily, months, quarantined)]
    write_json(METRICS_DIR / "eda_summary.json", summary)
    print(f"demand rows {rows:,} | stations {n_st:,} | days {n_d} | nonzero share of dense grid "
          f"{summary['sparsity_nonzero_share']:.1%} | dropped edge rides {summary['dropped_edge_rides']}")
    return summary


def style_axes(ax, title, xlabel, ylabel):
    ax.set_title(title, loc="left", fontsize=13, color=INK, pad=12)
    ax.set_xlabel(xlabel, color=INK_MUTED)
    ax.set_ylabel(ylabel, color=INK_MUTED)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)
    ax.tick_params(colors=INK_MUTED)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def monthly_chart(daily: pd.DataFrame, months: list[str], quarantined: list[str]) -> str:
    """Monthly totals (seasonality) above daily rides with a 7-day mean (single-day dips)."""
    d = daily.assign(date=pd.to_datetime(daily["date"])).set_index("date").sort_index()
    # Reindex to every calendar day so a missing/quarantined month shows as a gap, not a bridge.
    d = d.reindex(pd.date_range(months[0] + "-01", pd.Timestamp(months[-1] + "-01") + pd.offsets.MonthEnd(0)))
    by_month = d["departures"].groupby(d.index.strftime("%Y-%m")).sum(min_count=1)
    all_months = pd.period_range(months[0], months[-1], freq="M").strftime("%Y-%m")
    by_month = by_month.reindex(all_months)

    fig, (top, bot) = plt.subplots(2, 1, figsize=(11, 7.5), height_ratios=[1, 1.15])
    x = range(len(by_month))
    top.bar(x, by_month.fillna(0), color=WEEKDAY_COLOR, width=0.7)
    for i, (ym, v) in enumerate(by_month.items()):
        label = f"{v / 1e6:.1f}M" if pd.notna(v) else ("quarantined" if ym in quarantined else "no data")
        top.text(i, 0 if pd.isna(v) else v, label, ha="center", va="bottom", fontsize=8,
                 color=INK if pd.notna(v) else INK_MUTED)
    top.set_xticks(list(x), [pd.Timestamp(m + "-01").strftime("%b\n%Y") for m in by_month.index])
    top.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v / 1e6:.0f}M"))
    style_axes(top, "Rides per month", "", "Rides")

    bot.plot(d.index, d["departures"], color=GRID_LINE, linewidth=1, label="Daily rides")
    bot.plot(d.index, d["departures"].rolling(7, center=True, min_periods=4).mean(),
             color=WEEKDAY_COLOR, linewidth=2, label="7-day mean")
    marks = {d["departures"].idxmin()}
    if pd.Timestamp("2026-09-27") in d.index:
        marks.add(pd.Timestamp("2026-09-27"))
    for day in sorted(marks):
        v = d.loc[day, "departures"]
        bot.scatter([day], [v], color=WEEKEND_COLOR, s=30, zorder=3)
        bot.annotate(f"{day:%b %-d}: {v / 1000:,.0f}k", (day, v), xytext=(-10, 0), textcoords="offset points",
                     ha="right", va="center", fontsize=9, color=INK)
    bot.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v / 1000:,.0f}k"))
    bot.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%b %Y"))
    style_axes(bot, "Rides per day", "", "Rides")
    bot.legend(frameon=False, loc="upper left")
    fig.tight_layout()
    p = CHARTS_DIR / "rides_by_month.png"
    fig.savefig(p, dpi=150); plt.close(fig)
    return str(p)


def charts(hourly: pd.DataFrame, top: pd.DataFrame, daily: pd.DataFrame) -> list[str]:
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    paths = []

    fig, ax = plt.subplots(figsize=(9, 5))
    for col, label, color in (("weekday", "Weekday", WEEKDAY_COLOR), ("weekend", "Weekend", WEEKEND_COLOR)):
        ax.plot(hourly["hour"], hourly[col], color=color, linewidth=2, marker="o", markersize=4, label=label)
        peak = hourly.loc[hourly[col].idxmax()]
        ax.annotate(f"{label} peak {peak[col]:,.0f}", (peak["hour"], peak[col]), xytext=(6, 6),
                    textcoords="offset points", fontsize=9, color=INK)
    ax.set_xticks(range(0, 24, 2))
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    style_axes(ax, "Average departures per day, by hour", "Hour of day (NYC local)", "Departures / day")
    ax.legend(frameon=False)
    p = CHARTS_DIR / "hourly_profile_weekday_vs_weekend.png"
    fig.tight_layout(); fig.savefig(p, dpi=150); plt.close(fig); paths.append(str(p))

    t = top.iloc[::-1]
    fig, ax = plt.subplots(figsize=(9, 6))
    ax.barh(t["label"], t["trips"], color=WEEKDAY_COLOR, height=0.7)
    for y, v in enumerate(t["trips"]):
        ax.text(v, y, f" {v:,.0f}", va="center", fontsize=8, color=INK_MUTED)
    style_axes(ax, f"Busiest {len(t)} stations (departures + arrivals)", "Trips", "")
    ax.grid(axis="y", visible=False); ax.grid(axis="x", color=GRID, linewidth=0.8)
    ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v / 1000:,.0f}k"))
    p = CHARTS_DIR / "busiest_stations.png"
    fig.tight_layout(); fig.savefig(p, dpi=150); plt.close(fig); paths.append(str(p))

    fig, ax = plt.subplots(figsize=(10, 4.5))
    # Reindex to every calendar day so a missing/quarantined month shows as a gap, not a bridge.
    line = daily.assign(date=pd.to_datetime(daily["date"])).set_index("date")["departures"]
    line = line.reindex(pd.date_range(line.index.min(), line.index.max()))
    ax.plot(line.index, line.values, color=WEEKDAY_COLOR, linewidth=2)
    wk = daily[daily["is_weekend"]]
    ax.scatter(pd.to_datetime(wk["date"]), wk["departures"], color=WEEKEND_COLOR, s=8 if len(daily) > 120 else 18,
               zorder=3, label="Weekend day")
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v / 1000:,.0f}k"))
    style_axes(ax, "Daily departures", "", "Departures")
    ax.legend(frameon=False, loc="lower left")
    fig.autofmt_xdate()
    p = CHARTS_DIR / "daily_departures.png"
    fig.tight_layout(); fig.savefig(p, dpi=150); plt.close(fig); paths.append(str(p))
    return paths


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--months", nargs="+", metavar="YYYY-MM", help="only these target months")
    ap.add_argument("--force", action="store_true", help="rebuild months that are already done")
    ap.add_argument("--finalize-only", action="store_true")
    a = ap.parse_args()

    available = clean_months()
    targets = a.months or available
    missing = [m for m in targets if m not in available]
    if missing:
        raise SystemExit(f"not in trips_clean (or not verified): {missing}")

    if not a.finalize_only:
        todo = [m for m in targets
                if a.force or not (month_path(DEMAND_DIR, m) / DEMAND_MARKER).exists()
                or not (PARTS_DIR / m / "stations.parquet").exists()]
        if todo:
            spark = get_spark("bikeflow-demand")
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            try:
                spec = compute_disrupted(spark, available)
                print(f"disrupted days: {len(spec['days'])} + windows {spec['manual_windows']}", flush=True)
                for m in todo:
                    check_disk(f"demand {m}")
                    t0 = time.time()
                    with ResourceMonitor(spark) as mon:
                        rows = demand_month(spark, m, available, spec)
                    record_progress(log_progress("demand", m, rows, time.time() - t0, mon))
            finally:
                stop_spark(spark)

    if a.months and not a.finalize_only:
        return  # partial run: summary/charts are built once every month is done
    finalize(available)


if __name__ == "__main__":
    main()
