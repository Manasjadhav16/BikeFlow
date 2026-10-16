"""Stage 2b: GBT regressors for expected departures / arrivals over the next 30 minutes.

One invocation fits ONE model (target x variant) in its own Spark session, so the departures
and arrivals models run one after the other and never share a JVM heap.

    variant "weather"     FLOW_FEATURES (Stage 2 features + 60-min change, grid cell,
                          holiday, weather)
    variant "no_weather"  FLOW_FEATURES_NO_WEATHER (same minus the 4 weather columns)

The training sample is the same deterministic station-day hash as Stage 2 (seed 42), so a
10% sample is a subset of the 20% one. Disrupted rows and rows with missing history are
excluded from training. Every row of the validation (2026-08) and test (2026-09) months is
predicted, with missing features imputed as 0. Predictions are clipped at 0 and written to
data/processed/predictions_v2/<variant>_<target>/month=YYYY-MM.

    python -m src.train_flow --target departures --variant weather --sample-pct 20 --max-iter 60
    python -m src.train_flow ... --dry-run --train-months 2025-11      # timing projection
"""
import argparse
import shutil
import time
from datetime import datetime

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import functions as F

from src.build_features import FEATURES_DIR, FLOW_FEATURES, FLOW_FEATURES_NO_WEATHER
from src.common import (DATA_PROCESSED, METRICS_DIR, ROOT, ResourceMonitor, check_disk, get_spark,
                        read_json, stop_spark, write_json)
from src.train_model import SEED, TEST_MONTH, TRAIN_MONTHS, VAL_MONTH

TARGETS = {"departures": "departures_next_30m", "arrivals": "arrivals_next_30m"}
VARIANTS = {"weather": FLOW_FEATURES, "no_weather": FLOW_FEATURES_NO_WEATHER}
PRED_V2_DIR = DATA_PROCESSED / "predictions_v2"
FLOW_MODELS_DIR = ROOT / "models" / "flow"
TRAINING_JSON = METRICS_DIR / "flow_training.json"
IMPORTANCES_JSON = METRICS_DIR / "flow_feature_importances.json"


def stamp(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def read_month(spark, ym: str):
    return spark.read.option("basePath", str(FEATURES_DIR)).parquet(
        str(FEATURES_DIR / f"year={int(ym[:4])}" / f"month={int(ym[5:])}"))


def sample_dir(pct: float, months: list[str]):
    return DATA_PROCESSED / "train_sample_v2" / f"pct={pct:g}_{months[0]}_{months[-1]}_{len(months)}m"


def build_sample(spark, months: list[str], pct: float):
    """Station-day sample with every flow feature and both targets; reused by all fits at this pct."""
    out = sample_dir(pct, months)
    if (out / "_DONE").exists():
        return out
    shutil.rmtree(out, ignore_errors=True)
    keep = (F.abs(F.xxhash64("station_id", F.to_date("ts"), F.lit(SEED))) % 10000) < int(pct * 100)
    for m in months:
        (read_month(spark, m)
         .filter(~F.col("disrupted") & keep)
         .dropna(subset=FLOW_FEATURES)
         .select(*FLOW_FEATURES, *TARGETS.values())
         .write.mode("append").parquet(str(out)))
    (out / "_DONE").touch()
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--target", choices=TARGETS, required=True)
    ap.add_argument("--variant", choices=VARIANTS, required=True)
    ap.add_argument("--sample-pct", type=float, default=20.0)
    ap.add_argument("--max-iter", type=int, default=60)
    ap.add_argument("--max-depth", type=int, default=6)
    ap.add_argument("--train-months", nargs="+", default=TRAIN_MONTHS)
    ap.add_argument("--dry-run", action="store_true", help="fit + predict validation only; project full time")
    a = ap.parse_args()
    name, label, feats = f"{a.variant}_{a.target}", TARGETS[a.target], VARIANTS[a.variant]

    stamp(f"START {name} (pct={a.sample_pct:g}, iter={a.max_iter}, depth={a.max_depth}, "
          f"months={len(a.train_months)}{', DRY RUN' if a.dry_run else ''})")
    check_disk(f"train {name}")
    spark = get_spark(f"bikeflow-{name}")
    rec = {"target": label, "variant": a.variant, "features": feats, "sample_pct": a.sample_pct,
           "max_iter": a.max_iter, "max_depth": a.max_depth, "train_months": a.train_months}
    try:
        t0 = time.time()
        sdir = build_sample(spark, a.train_months, a.sample_pct)
        train = spark.read.parquet(str(sdir)).select(*feats, label)
        rec["sample_rows"] = train.count()
        rec["sample_s"] = round(time.time() - t0, 1)
        stamp(f"  sample ready: {rec['sample_rows']:,} rows ({rec['sample_s']}s)")

        pipe = Pipeline(stages=[
            VectorAssembler(inputCols=feats, outputCol="features"),
            GBTRegressor(featuresCol="features", labelCol=label, maxIter=a.max_iter, maxDepth=a.max_depth,
                         stepSize=0.1, subsamplingRate=0.5, seed=SEED)])
        t0 = time.time()
        with ResourceMonitor(spark) as mon:
            model = pipe.fit(train)
        rec.update(fit_s=round(time.time() - t0, 1), fit_peak_rss_gb=round(mon.peak_rss_gb, 2),
                   fit_peak_swap_gb=round(mon.peak_swap_gb, 2))
        stamp(f"  fit done in {rec['fit_s']}s (peak RSS {rec['fit_peak_rss_gb']} GB, swap {rec['fit_peak_swap_gb']} GB)")

        for ym in ([VAL_MONTH] if a.dry_run else [VAL_MONTH, TEST_MONTH]):
            check_disk(f"predict {name} {ym}")
            t0 = time.time()
            df = read_month(spark, ym).fillna(0.0, subset=feats)
            pred = (model.transform(df)
                    .select("station_id", "ts", F.greatest(F.col("prediction"), F.lit(0.0)).alias("pred")))
            path = PRED_V2_DIR / ("dryrun_" + name if a.dry_run else name) / f"month={ym}"
            pred.write.mode("overwrite").parquet(str(path))
            rec[f"predict_{ym}_s"] = round(time.time() - t0, 1)
            stamp(f"  predicted {ym} in {rec[f'predict_{ym}_s']}s")

        gbt = model.stages[-1]
        imp = dict(sorted(zip(feats, map(float, gbt.featureImportances.toArray())), key=lambda kv: -kv[1]))
        if a.dry_run:
            months_full = len(TRAIN_MONTHS)
            proj = rec["fit_s"] * months_full / len(a.train_months) + 2 * rec[f"predict_{VAL_MONTH}_s"]
            rec["projected_full_run_min"] = round(proj / 60, 1)
            stamp(f"  projected full run: {rec['projected_full_run_min']} min "
                  f"(fit x{months_full}/{len(a.train_months)} months + 2 prediction months)")
            log = read_json(TRAINING_JSON)
            log.setdefault("dry_runs", {})[name] = rec
            write_json(TRAINING_JSON, log)
            return
        shutil.rmtree(FLOW_MODELS_DIR / name, ignore_errors=True)
        model.save(str(FLOW_MODELS_DIR / name))
        log = read_json(TRAINING_JSON)
        log.setdefault("runs", {})[name] = rec
        write_json(TRAINING_JSON, log)
        imps = read_json(IMPORTANCES_JSON)
        imps[name] = imp
        write_json(IMPORTANCES_JSON, imps)
    finally:
        stop_spark(spark)
        stamp(f"END {name}")


if __name__ == "__main__":
    main()
