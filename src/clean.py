"""Clean trips one source month at a time, then delete that month's raw Parquet.

Per month: inspect the raw partition (rows, started_at range, days covered); a month
that looks partial is quarantined (kept raw, excluded, recorded) unless --include'd.
Otherwise restore station ids that lost their trailing zero ("5980.1" -> "5980.10",
present in every month), drop bad timestamps / durations / missing start station,
dedupe on ride_id, flag (keep) rides with no end station, write
data/processed/trips_clean/year=Y/month=M, verify the row count independently and
only then delete data/processed/trips/year=Y/month=M.

outputs/metrics/data_quality.json holds per-month counts before/after each rule and
totals over all clean months.
"""
import argparse
import calendar
import shutil
import time
from datetime import date, datetime, timezone

from pyspark.sql import functions as F

from src.common import (METRICS_DIR, RAW_SIZES_JSON, TRIPS_CLEAN_DIR, TRIPS_DIR, ResourceMonitor,
                        check_disk, dir_size_bytes, get_spark, log_progress, read_json, stop_spark,
                        write_json)

MAX_DURATION_SEC = 24 * 3600
ONE_DECIMAL_ID = r"^\d+\.\d$"
STATION_ID_COLS = ("start_station_id", "end_station_id")
DQ_JSON = METRICS_DIR / "data_quality.json"
RAW_DONE_MARKER = "_BIKEFLOW_DONE"
CLEAN_MARKER = "_BIKEFLOW_CLEAN"
MIN_RIDES_PER_DAY = 100  # a calendar day below this counts as "not covered"
MAX_OUTSIDE_MONTH_SHARE = 0.01


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


def partition(base, ym: str):
    return base / f"year={int(ym[:4])}" / f"month={int(ym[5:])}"


def read_month(spark, base, ym: str):
    return spark.read.option("basePath", str(base)).parquet(str(partition(base, ym)))


def inspect_month(spark, ym: str) -> dict:
    """Coverage check on a raw month: is every calendar day present with real volume?"""
    y, m = int(ym[:4]), int(ym[5:])
    df = read_month(spark, TRIPS_DIR, ym)
    # Format in Spark: collected timestamps would be rendered in the host's time zone, not NYC.
    ts_fmt = "yyyy-MM-dd HH:mm:ss.SSS"
    s = df.agg(F.count(F.lit(1)).alias("rows"),
               F.date_format(F.min("started_at"), ts_fmt).alias("min_started_at"),
               F.date_format(F.max("started_at"), ts_fmt).alias("max_started_at")).first()
    per_day = {r["d"]: r["n"] for r in df.filter((F.year("started_at") == y) & (F.month("started_at") == m))
               .groupBy(F.to_date("started_at").alias("d")).agg(F.count(F.lit(1)).alias("n")).collect()}
    n_days = calendar.monthrange(y, m)[1]
    days = [date(y, m, d) for d in range(1, n_days + 1)]
    thin = [str(d) for d in days if per_day.get(d, 0) < MIN_RIDES_PER_DAY]
    outside = s["rows"] - sum(per_day.values())
    issues = []
    if thin:
        issues.append(f"{len(thin)} of {n_days} days have < {MIN_RIDES_PER_DAY} rides "
                      f"(first {thin[0]}, last {thin[-1]})")
    if s["rows"] and outside / s["rows"] > MAX_OUTSIDE_MONTH_SHARE:
        issues.append(f"{outside:,} rides ({outside / s['rows']:.1%}) start outside {ym}")
    return {
        "rows": s["rows"],
        "min_started_at": s["min_started_at"],
        "max_started_at": s["max_started_at"],
        "calendar_days": n_days,
        "days_covered": n_days - len(thin),
        "days_not_covered": thin,
        "rides_per_day_min_max": [min(per_day.values(), default=0), max(per_day.values(), default=0)],
        "started_outside_month": outside,
        "issues": issues,
    }


def clean_month(spark, ym: str) -> dict:
    """Clean one raw month into trips_clean, verify, return per-rule counts."""
    df = read_month(spark, TRIPS_DIR, ym).withColumn(
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
    aggs += [F.countDistinct(F.when(survived, F.col("ride_id"))).alias("distinct_ride_ids"),
             F.max((survived & F.col("ride_id").isNull()).cast("int")).alias("null_ride_id")]
    stats = df.agg(*aggs).first().asDict()

    kept = df
    for c in STATION_ID_COLS:
        kept = kept.withColumn(c, normalize_station_id(c))
    for _, bad in RULES:
        kept = kept.filter(~F.coalesce(bad, F.lit(False)))
    clean = (
        kept.dropDuplicates(["ride_id"])
        .withColumn("end_station_missing", F.col("end_station_id").isNull())
        .drop("year", "month")
    )
    out = partition(TRIPS_CLEAN_DIR, ym)
    clean.write.mode("overwrite").parquet(str(out))

    written = spark.read.parquet(str(out)).agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.col("end_station_missing").cast("long")).alias("end_station_missing")).first()
    expected = stats["distinct_ride_ids"] + (stats["null_ride_id"] or 0)
    if written["rows"] != expected:
        raise RuntimeError(f"{ym}: wrote {written['rows']:,} clean rows, expected {expected:,}; raw kept")

    after_rules = stats["input"] - sum(stats[n] for n, _ in RULES)
    return {
        "input": stats["input"],
        **{n: stats[n] for n, _ in RULES},
        "dedupe_ride_id": after_rules - written["rows"],
        "output": written["rows"],
        "end_station_missing": written["end_station_missing"],
        **{f"normalized_{c}": stats[f"normalized_{c}"] for c in STATION_ID_COLS},
        "verified": True,
    }


def process_month(spark, ym: str, include: bool = False) -> str:
    """Inspect -> clean -> verify -> delete raw. Returns 'cleaned' or 'quarantined'."""
    raw, out = partition(TRIPS_DIR, ym), partition(TRIPS_CLEAN_DIR, ym)
    check_disk(f"clean {ym}")
    dq = read_json(DQ_JSON)
    dq.setdefault("per_month", {})
    dq.setdefault("quarantined_months", {})
    inspection = inspect_month(spark, ym)
    if inspection["issues"] and not include:
        dq["quarantined_months"][ym] = {
            "reason": "; ".join(inspection["issues"]),
            "action": f"raw Parquet kept, excluded from trips_clean; include with: "
                      f"python -m src.clean --include {ym}",
            "inspection": inspection,
        }
        dq["per_month"].pop(ym, None)
        write_json(DQ_JSON, dq)
        if out.exists():
            shutil.rmtree(out)
        print(f"  {ym}: QUARANTINED - {dq['quarantined_months'][ym]['reason']}")
        return "quarantined"

    raw_bytes = dir_size_bytes(raw)
    stats = clean_month(spark, ym)
    stats["inspection"] = inspection
    if include and inspection["issues"]:
        stats["included_despite"] = inspection["issues"]
    dq["per_month"][ym] = stats
    dq["quarantined_months"].pop(ym, None)
    write_json(DQ_JSON, dq)
    (out / CLEAN_MARKER).touch()

    sizes = read_json(RAW_SIZES_JSON)
    entry = sizes.setdefault(ym, {})
    entry.update({
        "raw_rows": stats["input"],
        "raw_parquet_bytes": raw_bytes,
        "clean_rows": stats["output"],
        "clean_parquet_bytes": dir_size_bytes(out),
        "raw_parquet_deleted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    })
    entry.pop("parquet_bytes", None)  # superseded by raw_parquet_bytes
    write_json(RAW_SIZES_JSON, dict(sorted(sizes.items())))
    shutil.rmtree(raw)
    print(f"  {ym}: {stats['input']:,} raw -> {stats['output']:,} clean (verified), raw parquet deleted")
    return "cleaned"


def summarize(spark) -> dict:
    """Totals over every clean month plus whole-dataset observations."""
    dq = read_json(DQ_JSON)
    per_month = dict(sorted(dq.get("per_month", {}).items()))
    rule_names = [n for n, _ in rules()]
    tot = lambda k: sum(m[k] for m in per_month.values())  # noqa: E731
    steps, n = [], tot("input")
    for name in rule_names + ["dedupe_ride_id"]:
        steps.append({"rule": name, "before": n, "removed": tot(name), "after": n - tot(name)})
        n -= tot(name)

    # Memory-light: one month per Spark job, never a global distinct over all rows.
    months = list(per_month)
    obs = {}
    for ym in months:
        out = read_month(spark, TRIPS_CLEAN_DIR, ym)
        y, m = int(ym[:4]), int(ym[5:])
        r = out.agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(F.col("ride_id").isNull().cast("long")).alias("null_ride_id"),
            F.sum(((F.month("started_at") != m) | (F.year("started_at") != y)).cast("long"))
            .alias("started_outside_source_month"),
            F.expr("percentile_approx(duration_sec, array(0.5, 0.95, 0.99))").alias("duration_p50_p95_p99"),
            F.sum((F.col("duration_sec") < 60).cast("long")).alias("duration_under_60s"),
        ).first().asDict()
        r["rideable_type"] = {x["rideable_type"]: x["count"] for x in out.groupBy("rideable_type").count().collect()}
        r["member_casual"] = {x["member_casual"]: x["count"] for x in out.groupBy("member_casual").count().collect()}
        if r["rows"] != per_month[ym]["output"]:
            raise RuntimeError(f"{ym}: trips_clean has {r['rows']:,} rows, verified count was "
                               f"{per_month[ym]['output']:,}")
        obs[ym] = r
    # Exact cross-month duplicate check on adjacent months (where boundary rides could repeat);
    # within-month uniqueness is already verified per month.
    adjacent_dupes, adjacent_dupes_with_end = {}, {}
    for a, b in zip(months, months[1:]):
        ids = lambda ym: read_month(spark, TRIPS_CLEAN_DIR, ym).select("ride_id", "end_station_id")  # noqa: E731
        r = ids(a).join(ids(b).drop("end_station_id"), "ride_id").agg(
            F.count(F.lit(1)).alias("n"), F.count("end_station_id").alias("with_end")).first()
        adjacent_dupes[f"{a}/{b}"], adjacent_dupes_with_end[f"{a}/{b}"] = r["n"], r["with_end"]

    def merged(key):
        out = {}
        for r in obs.values():
            for k, v in r[key].items():
                out[k] = out.get(k, 0) + v
        return out

    final = {"rows": sum(r["rows"] for r in obs.values())}
    if final["rows"] != n:
        raise RuntimeError(f"trips_clean has {final['rows']:,} rows but per-month stats sum to {n:,}")
    rideable, member = merged("rideable_type"), merged("member_casual")
    final.update(
        null_ride_id=sum(r["null_ride_id"] for r in obs.values()),
        started_outside_source_month=sum(r["started_outside_source_month"] for r in obs.values()),
        duration_under_60s=sum(r["duration_under_60s"] for r in obs.values()),
    )

    report = {
        "months_included": list(per_month),
        "quarantined_months": dq.get("quarantined_months", {}),
        "input_rows": tot("input"),
        "output_rows": final["rows"],
        "rules": steps,
        "station_id_normalized": {f"{c}_one_decimal_padded": tot(f"normalized_{c}") for c in STATION_ID_COLS},
        "flags": {
            "end_station_missing": tot("end_station_missing"),
            "end_station_missing_pct": round(100 * tot("end_station_missing") / final["rows"], 3),
        },
        "observations": {
            "cross_month_duplicate_ride_ids_adjacent": adjacent_dupes,
            "cross_month_duplicate_ride_ids_adjacent_with_end_station": adjacent_dupes_with_end,
            "null_ride_id_in_output": final["null_ride_id"],
            "started_outside_source_month": final["started_outside_source_month"],
            "duration_sec_p50_p95_p99_by_month": {k: r["duration_p50_p95_p99"] for k, r in obs.items()},
            "duration_under_60s": final["duration_under_60s"],
            "rideable_type": rideable,
            "member_casual": member,
        },
        "per_month": per_month,
    }
    write_json(DQ_JSON, report)
    for s in steps:
        print(f"{s['rule']:32s} {s['before']:>12,} -> {s['after']:>12,}  (-{s['removed']:,})")
    print(f"end_station_missing flagged: {report['flags']['end_station_missing']:,}")
    return report


def pending_months() -> list[str]:
    """Raw months fully converted but not yet cleaned."""
    return sorted(
        f"{int(p.parent.name.split('=')[1]):04d}-{int(p.name.split('=')[1]):02d}"
        for p in TRIPS_DIR.glob("year=*/month=*") if (p / RAW_DONE_MARKER).exists()
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--include", nargs="*", default=[], metavar="YYYY-MM",
                    help="clean these months even if inspection flags them")
    ap.add_argument("--summary-only", action="store_true",
                    help="only rebuild data_quality.json totals; do not inspect or clean anything")
    a = ap.parse_args()
    spark = get_spark("bikeflow-clean")
    try:
        if not a.summary_only:
            for ym in pending_months():
                process_month(spark, ym, include=ym in a.include)
        t0 = time.time()
        with ResourceMonitor(spark) as mon:
            report = summarize(spark)
        log_progress("clean-summary", "all", report["output_rows"], time.time() - t0, mon)
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
