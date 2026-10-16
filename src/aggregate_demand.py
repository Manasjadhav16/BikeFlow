"""Aggregate clean trips into per-station 15-minute demand, plus an EDA summary and charts.

Output rows are sparse: a (station, bucket) row exists only if it had at least one
departure or arrival. Departures are bucketed on started_at by start station,
arrivals on ended_at by end station (rides with no end station count as departures only).
Buckets outside the ingested source months are dropped (edge rides spilling into
the neighbouring month). Times are local America/New_York.
"""
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from src.common import (CHARTS_DIR, DEMAND_DIR, METRICS_DIR, STATIONS_DIR, TRIPS_CLEAN_DIR,  # noqa: E402
                        check_disk, get_spark, write_json)

BUCKET = "15 minutes"
WEEKDAY_COLOR, WEEKEND_COLOR = "#2a78d6", "#eb6834"
INK, INK_MUTED, GRID = "#0b0b0b", "#52514e", "#e4e3df"


def build_demand(spark):
    trips = spark.read.parquet(str(TRIPS_CLEAN_DIR))
    months = trips.select("year", "month").distinct().orderBy("year", "month").collect()
    lo = pd.Timestamp(year=months[0]["year"], month=months[0]["month"], day=1)
    hi = pd.Timestamp(year=months[-1]["year"], month=months[-1]["month"], day=1) + pd.offsets.MonthBegin(1)

    dep = (trips.groupBy(F.col("start_station_id").alias("station_id"),
                         F.window("started_at", BUCKET).start.alias("bucket_start"))
           .agg(F.count(F.lit(1)).alias("departures")))
    arr = (trips.filter(F.col("end_station_id").isNotNull())
           .groupBy(F.col("end_station_id").alias("station_id"),
                    F.window("ended_at", BUCKET).start.alias("bucket_start"))
           .agg(F.count(F.lit(1)).alias("arrivals")))
    demand = (
        dep.join(arr, ["station_id", "bucket_start"], "full_outer")
        .fillna(0, ["departures", "arrivals"])
        # String bounds are parsed in the Spark session time zone (NYC), not the host's.
        .withColumn("in_range", (F.col("bucket_start") >= F.to_timestamp(F.lit(str(lo))))
                    & (F.col("bucket_start") < F.to_timestamp(F.lit(str(hi)))))
    )
    return demand, lo, hi


def add_features(df):
    dow = ((F.dayofweek("bucket_start") + 5) % 7) + 1  # ISO: Monday=1 .. Sunday=7
    return (
        df.withColumn("net_outflow", F.col("departures") - F.col("arrivals"))
        .withColumn("hour", F.hour("bucket_start"))
        .withColumn("day_of_week", dow)
        .withColumn("is_weekend", dow >= 6)
        .withColumn("year", F.year("bucket_start"))
        .withColumn("month", F.month("bucket_start"))
        .withColumn("date", F.to_date("bucket_start"))
    )


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
    ax.plot(daily["date"], daily["departures"], color=WEEKDAY_COLOR, linewidth=2)
    wk = daily[daily["is_weekend"]]
    ax.scatter(wk["date"], wk["departures"], color=WEEKEND_COLOR, s=18, zorder=3, label="Weekend day")
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v / 1000:,.0f}k"))
    style_axes(ax, "Daily departures", "", "Departures")
    ax.legend(frameon=False, loc="lower left")
    fig.autofmt_xdate()
    p = CHARTS_DIR / "daily_departures.png"
    fig.tight_layout(); fig.savefig(p, dpi=150); plt.close(fig); paths.append(str(p))
    return paths


def main() -> None:
    check_disk("aggregate_demand")
    spark = get_spark("bikeflow-demand")
    demand, lo, hi = build_demand(spark)
    edge = demand.filter(~F.col("in_range")).agg(
        F.count(F.lit(1)).alias("rows"), F.sum("departures").alias("dep"), F.sum("arrivals").alias("arr")).first()
    out = add_features(demand.filter("in_range").drop("in_range")).select(
        "station_id", "bucket_start", "departures", "arrivals", "net_outflow",
        "hour", "day_of_week", "month", "is_weekend", "year", "date")
    out.drop("date").write.mode("overwrite").partitionBy("year", "month").parquet(str(DEMAND_DIR))

    d = spark.read.parquet(str(DEMAND_DIR)).withColumn("date", F.to_date("bucket_start"))
    totals = d.agg(F.count(F.lit(1)).alias("rows"), F.sum("departures").alias("dep"),
                   F.sum("arrivals").alias("arr"), F.countDistinct("station_id").alias("stations"),
                   F.countDistinct("date").alias("days")).first()
    daily = (d.groupBy("date", "is_weekend")
             .agg(F.sum("departures").alias("departures"), F.sum("arrivals").alias("arrivals"))
             .orderBy("date").toPandas())
    hourly_raw = d.groupBy("is_weekend", "hour").agg(F.sum("departures").alias("dep")).toPandas()
    by_station = (d.groupBy("station_id")
                  .agg(F.sum("departures").alias("departures"), F.sum("arrivals").alias("arrivals"),
                       F.sum("net_outflow").alias("net_outflow"))
                  .toPandas())
    # Morning-peak (7-9am weekday) net outflow: where bikes drain vs. pile up at rush hour.
    am = (d.filter(~F.col("is_weekend") & F.col("hour").between(7, 9))
          .groupBy("station_id").agg(F.sum("net_outflow").alias("am_net_outflow")).toPandas())
    spark.stop()

    names = pd.read_parquet(STATIONS_DIR / "stations.parquet", columns=["station_id", "name"])
    by_station = by_station.merge(names, on="station_id", how="left").merge(am, on="station_id", how="left")
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

    summary = {
        "date_range": [str(lo.date()), str((hi - pd.Timedelta(days=1)).date())],
        "rows": int(totals["rows"]),
        "stations": int(totals["stations"]),
        "days": int(totals["days"]),
        "total_departures": int(totals["dep"]),
        "total_arrivals": int(totals["arr"]),
        "dense_grid_rows": int(totals["stations"]) * int(totals["days"]) * 96,
        "sparsity_nonzero_share": round(int(totals["rows"]) / (int(totals["stations"]) * int(totals["days"]) * 96), 4),
        "dropped_edge_buckets": {"rows": edge["rows"], "departures": edge["dep"] or 0, "arrivals": edge["arr"] or 0},
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
        "busiest_day": {k: str(v) for k, v in daily.loc[daily["departures"].idxmax(), ["date", "departures"]].items()},
        "quietest_day": {k: str(v) for k, v in daily.loc[daily["departures"].idxmin(), ["date", "departures"]].items()},
    }
    summary["charts"] = charts(hourly, top, daily)
    write_json(METRICS_DIR / "eda_summary.json", summary)
    print(f"demand rows {summary['rows']:,} | stations {summary['stations']:,} | days {summary['days']} | "
          f"nonzero share of dense grid {summary['sparsity_nonzero_share']:.1%}")
    print(f"dropped edge buckets: {summary['dropped_edge_buckets']}")


if __name__ == "__main__":
    main()
