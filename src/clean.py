"""Clean trips: drop bad timestamps / durations / missing start station, dedupe on ride_id,
flag (keep) rides with no end station. Station ids that lost their trailing zero
("5980.1" for "5980.10", present in every month) are restored first. Writes data/processed/trips_clean/ and
outputs/metrics/data_quality.json with row counts before/after each rule.
"""
from pyspark.sql import functions as F

from src.common import METRICS_DIR, TRIPS_CLEAN_DIR, TRIPS_DIR, check_disk, get_spark, write_json

MAX_DURATION_SEC = 24 * 3600
ONE_DECIMAL_ID = r"^\d+\.\d$"
STATION_ID_COLS = ("start_station_id", "end_station_id")


def normalize_station_id(col: str):
    c = F.col(col)
    return F.when(c.rlike(ONE_DECIMAL_ID), F.concat(c, F.lit("0"))).otherwise(c)


def rules():
    """Drop rules, applied in order; each only sees rows that survived the previous ones."""
    return [
        ("null_or_invalid_timestamp", F.col("started_at").isNull() | F.col("ended_at").isNull()),
        ("ended_at_not_after_started_at", F.col("ended_at") <= F.col("started_at")),
        ("duration_over_24h", F.col("duration_sec") > MAX_DURATION_SEC),
        ("null_start_station_id", F.col("start_station_id").isNull()),
    ]


def main() -> None:
    check_disk("clean")
    spark = get_spark("bikeflow-clean")
    df = spark.read.parquet(str(TRIPS_DIR)).withColumn(
        "duration_sec", F.col("ended_at").cast("long") - F.col("started_at").cast("long"))
    RULES = rules()

    # One pass: for each rule, count rows it removes among rows that passed earlier rules.
    survived = F.lit(True)
    aggs = [F.count(F.lit(1)).alias("input")] + [
        F.sum(F.col(c).rlike(ONE_DECIMAL_ID).cast("long")).alias(f"normalized_{c}") for c in STATION_ID_COLS]
    for name, bad in RULES:
        hit = survived & F.coalesce(bad, F.lit(False))
        aggs.append(F.sum(hit.cast("long")).alias(name))
        survived = survived & ~F.coalesce(bad, F.lit(False))
    by_month = df.groupBy("year", "month").agg(*aggs).orderBy("year", "month").collect()

    kept = df
    for c in STATION_ID_COLS:
        kept = kept.withColumn(c, normalize_station_id(c))
    for _, bad in RULES:
        kept = kept.filter(~F.coalesce(bad, F.lit(False)))
    clean = (
        kept.dropDuplicates(["ride_id"])
        .withColumn("end_station_missing", F.col("end_station_id").isNull())
    )
    TRIPS_CLEAN_DIR.parent.mkdir(parents=True, exist_ok=True)
    clean.write.mode("overwrite").partitionBy("year", "month").parquet(str(TRIPS_CLEAN_DIR))

    out = spark.read.parquet(str(TRIPS_CLEAN_DIR))
    final = out.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.col("end_station_missing").cast("long")).alias("end_station_missing"),
        F.sum(F.col("ride_id").isNull().cast("long")).alias("null_ride_id"),
        F.sum(((F.month("started_at") != F.col("month")) | (F.year("started_at") != F.col("year")))
              .cast("long")).alias("started_outside_source_month"),
        F.expr("percentile_approx(duration_sec, array(0.5, 0.95, 0.99))").alias("duration_p50_p95_p99"),
        F.sum((F.col("duration_sec") < 60).cast("long")).alias("duration_under_60s"),
    ).first().asDict()
    rideable = {r["rideable_type"]: r["count"] for r in out.groupBy("rideable_type").count().collect()}
    member = {r["member_casual"]: r["count"] for r in out.groupBy("member_casual").count().collect()}

    total = {k: sum(r[k] for r in by_month) for k in ["input"] + [n for n, _ in RULES]}
    steps, n = [], total["input"]
    for name, _ in RULES:
        steps.append({"rule": name, "before": n, "removed": total[name], "after": n - total[name]})
        n -= total[name]
    steps.append({"rule": "dedupe_ride_id", "before": n, "removed": n - final["rows"], "after": final["rows"]})

    report = {
        "input_rows": total["input"],
        "output_rows": final["rows"],
        "rules": steps,
        "station_id_normalized": {
            f"{c}_one_decimal_padded": sum(r[f"normalized_{c}"] for r in by_month) for c in STATION_ID_COLS},
        "flags": {
            "end_station_missing": final["end_station_missing"],
            "end_station_missing_pct": round(100 * final["end_station_missing"] / final["rows"], 3),
        },
        "observations": {
            "null_ride_id_in_output": final["null_ride_id"],
            "started_outside_source_month": final["started_outside_source_month"],
            "duration_sec_p50_p95_p99": final["duration_p50_p95_p99"],
            "duration_under_60s": final["duration_under_60s"],
            "rideable_type": rideable,
            "member_casual": member,
        },
        "per_source_month": [r.asDict() for r in by_month],
    }
    write_json(METRICS_DIR / "data_quality.json", report)
    for s in steps:
        print(f"{s['rule']:32s} {s['before']:>12,} -> {s['after']:>12,}  (-{s['removed']:,})")
    print(f"end_station_missing flagged: {final['end_station_missing']:,}")
    spark.stop()


if __name__ == "__main__":
    main()
