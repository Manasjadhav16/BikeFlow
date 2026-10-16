"""Train and evaluate the 30-minute bike-change model (PySpark MLlib).

Time-based split, never random: train 2025-11 .. 2026-07, validate 2026-08, test 2026-09.

Training uses a deterministic sample of station-days (hash of station_id + date), without
disrupted rows and without rows lacking 4 weeks of history. Evaluation always uses EVERY row
of the validation and test months. Missing history (new stations) is imputed with 0 there,
and the count is reported. Headline metrics exclude disrupted rows; metrics including them
are reported separately.

Baselines: persistence (next 30 min = last 30 min) and the historical mean of the target per
station x weekday x hour over the full training months.
Models: LinearRegression, RandomForestRegressor, GBTRegressor on the same features.

    python -m src.train_model --dry-run --train-months 2025-11   # timed 1-month dry run
    python -m src.train_model --sample-pct 10
"""
import argparse
import shutil
import time

import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.sql import functions as F

from src.aggregate_demand import record_progress
from src.build_features import FEATURE_COLS, FEATURES_DIR, STATION_INDEX_JSON, TARGET
from src.common import (DATA_PROCESSED, METRICS_DIR, ROOT, ResourceMonitor, check_disk, get_spark,
                        log_progress, read_json, stop_spark, write_json)

TRAIN_MONTHS = ["2025-11", "2025-12", "2026-01", "2026-02", "2026-03",
                "2026-04", "2026-05", "2026-06", "2026-07"]
VAL_MONTH, TEST_MONTH = "2026-08", "2026-09"
RUSH_HOURS = list(range(7, 10)) + list(range(16, 19))  # 07:00-09:59 and 16:00-18:59, NYC
SAMPLE_DIR = DATA_PROCESSED / "train_sample"
PRED_DIR = DATA_PROCESSED / "predictions"
MODELS_DIR = ROOT / "models"
SEED = 42


def month_part(ym: str):
    return FEATURES_DIR / f"year={int(ym[:4])}" / f"month={int(ym[5:])}"


def read_month(spark, ym: str):
    # basePath keeps the year/month partition columns (month is a model feature).
    return spark.read.option("basePath", str(FEATURES_DIR)).parquet(str(month_part(ym)))


def make_models() -> dict:
    asm = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
    return {
        "linear_regression": Pipeline(stages=[asm, LinearRegression(
            featuresCol="features", labelCol=TARGET, regParam=0.0)]),
        "random_forest": Pipeline(stages=[asm, RandomForestRegressor(
            featuresCol="features", labelCol=TARGET, numTrees=40, maxDepth=10, minInstancesPerNode=20,
            subsamplingRate=0.5, featureSubsetStrategy="onethird", seed=SEED)]),
        "gbt": Pipeline(stages=[asm, GBTRegressor(
            featuresCol="features", labelCol=TARGET, maxIter=50, maxDepth=6, stepSize=0.1,
            subsamplingRate=0.5, seed=SEED)]),
    }


def build_sample(spark, months: list[str], pct: float) -> int:
    """Write the station-day training sample to SAMPLE_DIR, one month at a time."""
    shutil.rmtree(SAMPLE_DIR, ignore_errors=True)
    keep = (F.abs(F.xxhash64("station_id", F.to_date("ts"), F.lit(SEED))) % 10000) < int(pct * 100)
    total = 0
    for m in months:
        t0 = time.time()
        s = (read_month(spark, m)
             .filter(~F.col("disrupted") & F.col("hist_same_slot_4w").isNotNull() & keep)
             .select(*FEATURE_COLS, TARGET))
        s.write.mode("append").parquet(str(SAMPLE_DIR))
        n = spark.read.parquet(str(SAMPLE_DIR)).count() - total
        total += n
        record_progress(log_progress("train-sample", m, n, time.time() - t0))
    return total


def hist_baseline(spark, months: list[str]):
    """Mean target per station x weekday x hour over the FULL training months (not the sample)."""
    parts = [read_month(spark, m).filter(~F.col("disrupted"))
             .groupBy("station_id", "day_of_week", "hour")
             .agg(F.sum(TARGET).alias("s"), F.count(F.lit(1)).alias("n")) for m in months]
    df = parts[0]
    for p in parts[1:]:
        df = df.unionByName(p)
    out = df.groupBy("station_id", "day_of_week", "hour").agg((F.sum("s") / F.sum("n")).alias("pred_hist_avg"))
    path = DATA_PROCESSED / "hist_baseline"
    out.write.mode("overwrite").parquet(str(path))
    return spark.read.parquet(str(path))


def busiest_stations(months: list[str], share: float = 0.2) -> list[str]:
    """Top `share` of model stations by departures + arrivals over the training months."""
    st = pd.concat([pd.read_parquet(DATA_PROCESSED / "demand_parts" / m / "stations.parquet") for m in months])
    st = st.groupby("station_id")[["departures", "arrivals"]].sum()
    model_stations = {s["station_id"] for s in read_json(STATION_INDEX_JSON)["stations"]}
    act = (st["departures"] + st["arrivals"])[st.index.isin(model_stations)].sort_values(ascending=False)
    return list(act.index[: int(round(len(act) * share))])


def predict_month(spark, ym: str, fitted: dict, hist, top20: list[str], save: bool):
    """Predictions of every model + baselines for every row of a month; returns error sums."""
    df = (read_month(spark, ym)
          .withColumn("hist_imputed", F.col("hist_same_slot_4w").isNull())
          .fillna(0.0, ["hist_same_slot_4w"])
          .join(F.broadcast(hist), ["station_id", "day_of_week", "hour"], "left")
          .withColumn("hist_baseline_missing", F.col("pred_hist_avg").isNull())
          .fillna(0.0, ["pred_hist_avg"])
          .withColumn("pred_persistence", F.col("bike_change_last_30m").cast("double"))
          .withColumn("top20", F.col("station_id").isin(top20)))
    for name, model in fitted.items():
        df = model.transform(df).withColumnRenamed("prediction", f"pred_{name}").drop("features")
    preds = ["pred_persistence", "pred_hist_avg"] + [f"pred_{n}" for n in fitted]
    cols = ["station_id", "ts", "hour", "day_of_week", TARGET, "disrupted", "top20",
            "hist_imputed", "hist_baseline_missing", *preds]
    if save:
        path = PRED_DIR / f"month={ym}"
        df.select(*cols).write.mode("overwrite").parquet(str(path))
        df = spark.read.parquet(str(path))
    aggs = [F.count(F.lit(1)).alias("n"), F.sum(F.col("hist_imputed").cast("long")).alias("hist_imputed"),
            F.sum(F.col("hist_baseline_missing").cast("long")).alias("hist_baseline_missing")]
    for p in preds:
        err = F.col(p) - F.col(TARGET)
        aggs += [F.sum(F.abs(err)).alias(f"{p}__abs"), F.sum(err * err).alias(f"{p}__sq")]
    return df.groupBy("disrupted", "top20", "hour").agg(*aggs).toPandas(), preds  # <= 96 rows


def metrics_table(sums: pd.DataFrame, preds: list[str]) -> dict:
    segs = {
        "overall": sums,
        "top20_busiest": sums[sums["top20"]],
        "rush_hours": sums[sums["hour"].isin(RUSH_HOURS)],
    }
    out = {}
    for scope, keep in (("excl_disrupted", False), ("incl_disrupted", True)):
        out[scope] = {}
        for seg, df in segs.items():
            d = df if keep else df[~df["disrupted"]]
            n = d["n"].sum()
            out[scope][seg] = {"rows": int(n), **{
                p.removeprefix("pred_"): {"mae": round(d[f"{p}__abs"].sum() / n, 4),
                                          "rmse": round((d[f"{p}__sq"].sum() / n) ** 0.5, 4)} for p in preds}}
    d = sums[~sums["disrupted"]].groupby("hour").sum(numeric_only=True)
    out["mae_by_hour_excl_disrupted"] = {
        p.removeprefix("pred_"): {int(h): round(r[f"{p}__abs"] / r["n"], 4) for h, r in d.iterrows()} for p in preds}
    out["eval_rows_hist_feature_imputed"] = int(sums["hist_imputed"].sum())
    out["eval_rows_hist_baseline_missing"] = int(sums["hist_baseline_missing"].sum())
    return out


def importances(fitted: dict) -> dict:
    out = {}
    for name, pipe in fitted.items():
        m = pipe.stages[-1]
        if hasattr(m, "featureImportances"):
            out[name] = dict(sorted(zip(FEATURE_COLS, map(float, m.featureImportances.toArray())),
                                    key=lambda kv: -kv[1]))
        else:
            out[name] = {"coefficients": dict(zip(FEATURE_COLS, map(float, m.coefficients))),
                         "intercept": float(m.intercept)}
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sample-pct", type=float, default=10.0, help="percent of station-days to train on")
    ap.add_argument("--train-months", nargs="+", default=TRAIN_MONTHS)
    ap.add_argument("--models", nargs="+", default=["linear_regression", "random_forest", "gbt"])
    ap.add_argument("--dry-run", action="store_true", help="evaluate on validation only; save nothing")
    a = ap.parse_args()

    check_disk("train")
    spark = get_spark("bikeflow-train")
    timings = {}
    try:
        t0 = time.time()
        n_sample = build_sample(spark, a.train_months, a.sample_pct)
        timings["sample_s"] = round(time.time() - t0, 1)
        t0 = time.time()
        hist = hist_baseline(spark, a.train_months)
        top20 = busiest_stations(a.train_months)
        timings["baselines_s"] = round(time.time() - t0, 1)

        train = spark.read.parquet(str(SAMPLE_DIR))
        fitted = {}
        for name, pipe in make_models().items():
            if name not in a.models:
                continue
            check_disk(f"fit {name}")
            t0 = time.time()
            with ResourceMonitor(spark) as mon:
                fitted[name] = pipe.fit(train)
            timings[f"fit_{name}_s"] = round(time.time() - t0, 1)
            record_progress(log_progress(f"fit-{name}", "+".join(a.train_months[:1]) + "..", n_sample,
                                         time.time() - t0, mon))

        results = {}
        for ym, split in ((VAL_MONTH, "validation"), (TEST_MONTH, "test")):
            if a.dry_run and split == "test":
                continue
            check_disk(f"predict {ym}")
            t0 = time.time()
            with ResourceMonitor(spark) as mon:
                sums, preds = predict_month(spark, ym, fitted, hist, top20, save=not a.dry_run)
            results[split] = {"month": ym, **metrics_table(sums, preds)}
            timings[f"predict_{split}_s"] = round(time.time() - t0, 1)
            record_progress(log_progress(f"predict-{split}", ym, int(sums["n"].sum()), time.time() - t0, mon))

        val = results["validation"]["excl_disrupted"]["overall"]
        best = min(fitted, key=lambda m: val[m]["mae"])
        report = {
            "split": {"train": a.train_months, "validation": VAL_MONTH, "test": TEST_MONTH},
            "sample": {"pct_station_days": a.sample_pct, "rows": n_sample, "seed": SEED,
                       "filters": "not disrupted, hist_same_slot_4w present"},
            "features": FEATURE_COLS, "target": TARGET,
            "rush_hours": RUSH_HOURS, "top20_busiest_stations": len(top20),
            "models": {n: {k.name: v for k, v in p.stages[-1].extractParamMap().items()
                           if k.name not in ("featuresCol", "labelCol", "predictionCol")}
                       for n, p in fitted.items()},
            "best_model_by_validation_mae": best,
            "timings_s": timings,
            "results": results,
        }
        print(f"best model (validation MAE): {best}")
        if a.dry_run:
            write_json(METRICS_DIR / "model_results_dryrun.json", report)
            return
        write_json(METRICS_DIR / "model_results.json", report)
        write_json(METRICS_DIR / "feature_importances.json", importances(fitted))
        shutil.rmtree(MODELS_DIR / "best_model", ignore_errors=True)
        fitted[best].save(str(MODELS_DIR / "best_model"))
        hist.coalesce(1).write.mode("overwrite").parquet(str(MODELS_DIR / "hist_baseline"))
        write_json(MODELS_DIR / "model_meta.json", {
            "best_model": best, "features": FEATURE_COLS, "target": TARGET,
            "trained_on": a.train_months, "sample_pct": a.sample_pct,
            "validation_mae": val[best]["mae"],
            "station_index": "models/station_index.json", "hist_baseline": "models/hist_baseline/",
        })
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
