"""Evaluate "strong drain" / "strong fill" alerts built from the 30-minute predictions.

    strong drain: predicted bike change over the next 30 min <= -X
    strong fill:  predicted bike change over the next 30 min >= +X

Each is scored against the actual change (same threshold) as precision and recall, for
every model and both baselines, on the validation and test months (disrupted rows
excluded). These are *flow* alerts only. Real "station will be empty/full" alerts need
the live bike and dock counts from GBFS station_status, which Stage 3 adds; a strong drain
at a station holding 30 bikes is not an empty-station risk.

    python -m src.eval_alerts
"""
import time

from pyspark.sql import functions as F

from src.build_features import TARGET
from src.common import METRICS_DIR, ResourceMonitor, get_spark, log_progress, read_json, stop_spark, write_json
from src.train_model import PRED_DIR, TEST_MONTH, VAL_MONTH

THRESHOLDS = [3, 5, 8]  # |change| >= 3 / 5 / 8 bikes in 30 min: ~7% / ~2.4% / ~0.5% of rows


def evaluate(spark, ym: str) -> dict:
    df = spark.read.parquet(str(PRED_DIR / f"month={ym}")).filter(~F.col("disrupted"))
    preds = [c for c in df.columns if c.startswith("pred_")]
    y = F.col(TARGET)
    aggs = [F.count(F.lit(1)).alias("n"), F.sum(F.abs(y)).alias("zero__abs"), F.sum(y * y).alias("zero__sq")]
    for x in THRESHOLDS:
        aggs += [F.sum((y <= -x).cast("long")).alias(f"drain{x}__actual"),
                 F.sum((y >= x).cast("long")).alias(f"fill{x}__actual")]
        for p in preds:
            yhat = F.col(p)
            aggs += [F.sum((yhat <= -x).cast("long")).alias(f"drain{x}__{p}__pred"),
                     F.sum(((yhat <= -x) & (y <= -x)).cast("long")).alias(f"drain{x}__{p}__tp"),
                     F.sum((yhat >= x).cast("long")).alias(f"fill{x}__{p}__pred"),
                     F.sum(((yhat >= x) & (y >= x)).cast("long")).alias(f"fill{x}__{p}__tp")]
    r = df.agg(*aggs).first().asDict()
    out = {"rows": r["n"],
           # Context for the error metrics: the target is 0 in ~60% of rows.
           "reference_always_zero": {"mae": round(r["zero__abs"] / r["n"], 4),
                                     "rmse": round((r["zero__sq"] / r["n"]) ** 0.5, 4)}}
    for kind in ("drain", "fill"):
        for x in THRESHOLDS:
            key = f"strong_{kind}_X{x}"
            actual = r[f"{kind}{x}__actual"]
            out[key] = {"actual_events": actual, "actual_rate": round(actual / r["n"], 5)}
            for p in preds:
                pred, tp = r[f"{kind}{x}__{p}__pred"], r[f"{kind}{x}__{p}__tp"]
                out[key][p.removeprefix("pred_")] = {
                    "alerts": pred,
                    "precision": round(tp / pred, 4) if pred else None,
                    "recall": round(tp / actual, 4) if actual else None,
                }
    return out


def main() -> None:
    spark = get_spark("bikeflow-alerts")
    try:
        res = {}
        for ym, split in ((VAL_MONTH, "validation"), (TEST_MONTH, "test")):
            t0 = time.time()
            with ResourceMonitor(spark) as mon:
                res[split] = {"month": ym, **evaluate(spark, ym)}
            log_progress("alerts", ym, res[split]["rows"], time.time() - t0, mon)
    finally:
        stop_spark(spark)
    best = read_json(METRICS_DIR / "model_results.json").get("best_model_by_validation_mae")
    write_json(METRICS_DIR / "alert_eval.json", {
        "definition": "strong drain: predicted change next 30 min <= -X; strong fill: >= +X; "
                      "scored against the actual change with the same X; disrupted rows excluded",
        "thresholds": THRESHOLDS,
        "best_model": best,
        "caveat": "Flow alerts only. Empty/full-station alerts need live bikes/docks available "
                  "(GBFS station_status) and are built in Stage 3.",
        **res,
    })
    t = res["test"]
    for kind in ("drain", "fill"):
        for x in THRESHOLDS:
            k = t[f"strong_{kind}_X{x}"]
            line = " | ".join(f"{m}: P={k[m]['precision']} R={k[m]['recall']}"
                              for m in ("persistence", "hist_avg", best) if m in k)
            print(f"test strong {kind} X={x} (actual {k['actual_events']:,}): {line}")


if __name__ == "__main__":
    main()
