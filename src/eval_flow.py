"""Stage 2b evaluation: Skellam event probabilities from expected departures/arrivals.

For each approach we get mu_dep and mu_arr for the next 30 min, then
    P(strong drain_k) = P(D - A >= k),  P(strong fill_k) = P(A - D >= k),  D~Pois(mu_dep), A~Pois(mu_arr)
(Skellam), for k = 2, 3, 5. Events: actual departures - arrivals >= k (drain), the reverse (fill).

Approaches (same Skellam construction for all):
    model         GBT departures + GBT arrivals, with weather (src.train_flow, variant weather)
    no_weather    same GBTs without weather features (ablation; smaller sample/fewer iterations)
    hist_avg      station x weekday x hour mean departures / arrivals over the training months
                  (fallback: weekday x hour mean over all stations)
    persistence   departures / arrivals of the last 30 minutes
    stage2_net    Stage 2 random forest net prediction n plus the historical total T:
                  T' = max(T, |n|), mu_arr = (T'+n)/2, mu_dep = (T'-n)/2

Metrics (validation 2026-08, test 2026-09; disrupted rows excluded, and reported separately):
PR curve, average precision, recall at 60%/70% precision, calibration table, Poisson deviance
(mean per row, mu floored at 0.01 so persistence's zero predictions stay finite).

Phase 1 (Spark, per month) writes the joined mu table; phase 2 streams it with pyarrow in
1M-row batches (scipy), so memory stays bounded.

    python -m src.eval_flow
"""
import time
from datetime import datetime

import numpy as np
import pyarrow.dataset as ds
from pyspark.sql import functions as F
from scipy.stats import skellam

from src.build_features import FEATURES_DIR
from src.common import DATA_PROCESSED, METRICS_DIR, ROOT, get_spark, stop_spark, write_json
from src.train_flow import PRED_V2_DIR
from src.train_model import PRED_DIR, TEST_MONTH, TRAIN_MONTHS, VAL_MONTH

KS = [2, 3, 5]
APPROACHES = ["model", "no_weather", "hist_avg", "persistence", "stage2_net"]
EVAL_INPUT = DATA_PROCESSED / "flow_eval_input"
HIST_FLOW = DATA_PROCESSED / "hist_flow_baseline"
NBINS = 1000
DEV_FLOOR = 0.01
CAL_EDGES = [0, 0.02, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0001]


def stamp(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def read_month(spark, ym):
    return spark.read.option("basePath", str(FEATURES_DIR)).parquet(
        str(FEATURES_DIR / f"year={int(ym[:4])}" / f"month={int(ym[5:])}"))


def hist_flow(spark):
    """Mean departures/arrivals per station x weekday x hour over the full training months."""
    parts = [read_month(spark, m).filter(~F.col("disrupted"))
             .groupBy("station_id", "day_of_week", "hour")
             .agg(F.sum("departures_next_30m").alias("sd"), F.sum("arrivals_next_30m").alias("sa"),
                  F.count(F.lit(1)).alias("n")) for m in TRAIN_MONTHS]
    df = parts[0]
    for p in parts[1:]:
        df = df.unionByName(p)
    df = df.groupBy("station_id", "day_of_week", "hour").agg(F.sum("sd").alias("sd"), F.sum("sa").alias("sa"),
                                                              F.sum("n").alias("n"))
    df.write.mode("overwrite").parquet(str(HIST_FLOW))
    return spark.read.parquet(str(HIST_FLOW))


def build_input(spark, ym: str, hist):
    base = read_month(spark, ym).select(
        "station_id", "ts", "hour", "day_of_week", "disrupted",
        F.col("departures_next_30m").alias("y_dep"), F.col("arrivals_next_30m").alias("y_arr"),
        F.col("departures_last_30m").cast("double").alias("persistence_dep"),
        F.col("arrivals_last_30m").cast("double").alias("persistence_arr"))
    for variant, col in (("weather", "model"), ("no_weather", "no_weather")):
        for tgt, suf in (("departures", "dep"), ("arrivals", "arr")):
            p = spark.read.parquet(str(PRED_V2_DIR / f"{variant}_{tgt}" / f"month={ym}"))
            base = base.join(p.withColumnRenamed("pred", f"{col}_{suf}"), ["station_id", "ts"], "left")
    st_hist = hist.select("station_id", "day_of_week", "hour", (F.col("sd") / F.col("n")).alias("h_dep"),
                          (F.col("sa") / F.col("n")).alias("h_arr"))
    glob = (hist.groupBy("day_of_week", "hour")
            .agg((F.sum("sd") / F.sum("n")).alias("g_dep"), (F.sum("sa") / F.sum("n")).alias("g_arr")))
    s2 = spark.read.parquet(str(PRED_DIR / f"month={ym}")).select("station_id", "ts", F.col("pred_random_forest").alias("n2"))
    base = (base.join(F.broadcast(st_hist), ["station_id", "day_of_week", "hour"], "left")
            .join(F.broadcast(glob), ["day_of_week", "hour"], "left")
            .withColumn("hist_missing", F.col("h_dep").isNull())
            .withColumn("hist_avg_dep", F.coalesce("h_dep", "g_dep"))
            .withColumn("hist_avg_arr", F.coalesce("h_arr", "g_arr"))
            .join(s2, ["station_id", "ts"], "left")
            .withColumn("s2_missing", F.col("n2").isNull())
            .withColumn("n2", F.coalesce("n2", F.col("hist_avg_arr") - F.col("hist_avg_dep")))
            .withColumn("T", F.greatest(F.col("hist_avg_dep") + F.col("hist_avg_arr"), F.abs("n2")))
            .withColumn("stage2_net_arr", (F.col("T") + F.col("n2")) / 2)
            .withColumn("stage2_net_dep", (F.col("T") - F.col("n2")) / 2))
    cols = ["station_id", "ts", "hour", "disrupted", "y_dep", "y_arr", "hist_missing", "s2_missing"]
    cols += [f"{a}_{s}" for a in APPROACHES for s in ("dep", "arr")]
    base.select(*cols).fillna(0.0).write.mode("overwrite").parquet(str(EVAL_INPUT / f"month={ym}"))


def score_month(ym: str) -> dict:
    """Stream the mu table; accumulate histograms (count, positives, sum p) and deviance sums."""
    shape = (len(APPROACHES), 2, len(KS), 2, NBINS)  # approach, direction, k, disrupted, bin
    cnt, pos, sp = np.zeros(shape), np.zeros(shape), np.zeros(shape)
    dev = np.zeros((len(APPROACHES), 2, 2))  # approach, target(dep/arr), disrupted
    n = np.zeros(2)
    extra = {"hist_missing": 0, "s2_missing": 0}
    dset = ds.dataset(str(EVAL_INPUT / f"month={ym}"), format="parquet")
    for batch in dset.to_batches(batch_size=1_000_000):
        b = batch.to_pydict() if False else {c: batch.column(c).to_numpy(zero_copy_only=False) for c in batch.schema.names
                                              if c not in ("station_id", "ts")}
        dis = b["disrupted"].astype(int)
        n += np.bincount(dis, minlength=2)
        extra["hist_missing"] += int(b["hist_missing"].sum())
        extra["s2_missing"] += int(b["s2_missing"].sum())
        yd, ya = b["y_dep"].astype(float), b["y_arr"].astype(float)
        for i, a in enumerate(APPROACHES):
            md = np.clip(b[f"{a}_dep"].astype(float), 1e-9, None)
            ma = np.clip(b[f"{a}_arr"].astype(float), 1e-9, None)
            for t, (y, mu) in enumerate(((yd, md), (ya, ma))):
                mu_f = np.maximum(mu, DEV_FLOOR)
                with np.errstate(divide="ignore", invalid="ignore"):
                    term = np.where(y > 0, y * np.log(y / mu_f), 0.0) - (y - mu_f)
                dev[i, t] += 2 * np.bincount(dis, weights=term, minlength=2)
            for d, (m1, m2, event) in enumerate(((md, ma, yd - ya), (ma, md, ya - yd))):
                for j, k in enumerate(KS):
                    p = np.nan_to_num(skellam.sf(k - 1, m1, m2), nan=0.0)
                    bins = np.minimum((p * NBINS).astype(int), NBINS - 1)
                    idx = dis * NBINS + bins
                    cnt[i, d, j] += np.bincount(idx, minlength=2 * NBINS).reshape(2, NBINS)
                    pos[i, d, j] += np.bincount(idx, weights=(event >= k).astype(float), minlength=2 * NBINS).reshape(2, NBINS)
                    sp[i, d, j] += np.bincount(idx, weights=p, minlength=2 * NBINS).reshape(2, NBINS)
    return {"cnt": cnt, "pos": pos, "sp": sp, "dev": dev, "n": n, "extra": extra}


def pr_stats(c, p):
    """PR curve from per-bin counts/positives; thresholds descend from the top bin."""
    c, p = c[::-1].cumsum(), p[::-1].cumsum()
    total = p[-1]
    keep = c > 0
    prec, rec = np.where(keep, p / np.maximum(c, 1), np.nan), p / max(total, 1)
    ap = float(np.nansum(np.diff(np.concatenate([[0], rec])) * np.nan_to_num(prec)))
    def rec_at(x):
        ok = (prec >= x) & keep
        return round(float(rec[ok].max()), 4) if ok.any() else 0.0
    thresholds = (np.arange(NBINS)[::-1]) / NBINS
    return {"events": int(total), "base_rate": None, "average_precision": round(ap, 4),
            "recall_at_60_precision": rec_at(0.6), "recall_at_70_precision": rec_at(0.7)}, prec, rec, thresholds


def summarize(s: dict, scope: int | None) -> tuple[dict, dict]:
    """scope 0 = excl. disrupted, 1 = disrupted only."""
    sel = lambda a: a[..., scope, :]  # noqa: E731
    cnt, pos, sp = sel(s["cnt"]), sel(s["pos"]), sel(s["sp"])
    rows = s["n"][scope]
    out, curves = {"rows": int(rows)}, {}
    if rows == 0:
        return out, curves
    out["poisson_deviance"] = {a: {"departures": round(float(s["dev"][i, 0, scope] / rows), 4),
                                   "arrivals": round(float(s["dev"][i, 1, scope] / rows), 4)}
                               for i, a in enumerate(APPROACHES)}
    for d, kind in enumerate(("drain", "fill")):
        for j, k in enumerate(KS):
            key = f"strong_{kind}_k{k}"
            out[key] = {}
            for i, a in enumerate(APPROACHES):
                st, prec, rec, thr = pr_stats(cnt[i, d, j], pos[i, d, j])
                st["base_rate"] = round(st["events"] / rows, 5)
                cal = []
                for lo, hi in zip(CAL_EDGES[:-1], CAL_EDGES[1:]):
                    b = slice(int(lo * NBINS), min(int(np.ceil(hi * NBINS)), NBINS))
                    nb = cnt[i, d, j][b].sum()
                    if nb:
                        cal.append({"bin": f"{lo:.2f}-{min(hi, 1):.2f}", "rows": int(nb),
                                    "mean_predicted": round(float(sp[i, d, j][b].sum() / nb), 4),
                                    "observed_rate": round(float(pos[i, d, j][b].sum() / nb), 4)})
                st["calibration"] = cal
                out[key][a] = st
                if k == 3:
                    ok = ~np.isnan(prec)
                    step = max(1, ok.sum() // 200)
                    curves[f"{kind}_{a}"] = {"precision": np.round(prec[ok][::step], 4).tolist(),
                                             "recall": np.round(rec[ok][::step], 4).tolist(),
                                             "threshold": np.round(thr[ok][::step], 4).tolist()}
    return out, curves


def main() -> None:
    stamp("START eval_flow phase 1 (Spark: join predictions + baselines)")
    spark = get_spark("bikeflow-eval-flow")
    try:
        hist = hist_flow(spark)
        for ym in (VAL_MONTH, TEST_MONTH):
            t0 = time.time()
            build_input(spark, ym, hist)
            stamp(f"  {ym} mu table written ({time.time() - t0:.0f}s)")
        hist.coalesce(1).write.mode("overwrite").parquet(str(ROOT / "models" / "hist_flow_baseline"))
    finally:
        stop_spark(spark)
    stamp("END eval_flow phase 1")

    stamp("START eval_flow phase 2 (Skellam probabilities, metrics)")
    res, pr = {}, {}
    for ym, split in ((VAL_MONTH, "validation"), (TEST_MONTH, "test")):
        t0 = time.time()
        s = score_month(ym)
        excl, curves = summarize(s, 0)
        only, _ = summarize(s, 1)
        res[split] = {"month": ym, "excl_disrupted": excl, "disrupted_only": only, **s["extra"]}
        if split == "test":
            pr = curves
        stamp(f"  {ym} scored ({time.time() - t0:.0f}s)")
    write_json(METRICS_DIR / "flow_eval.json", {
        "approaches": APPROACHES, "k": KS, "deviance_mu_floor": DEV_FLOOR, **res})
    write_json(METRICS_DIR / "flow_pr_curves_k3_test.json", pr)
    stamp("END eval_flow phase 2")
    t = res["test"]["excl_disrupted"]
    for kind in ("drain", "fill"):
        for k in KS:
            r = t[f"strong_{kind}_k{k}"]
            print(f"test {kind} k={k} (events {r['model']['events']:,}): " + " | ".join(
                f"{a}: AP={r[a]['average_precision']} R@60={r[a]['recall_at_60_precision']} "
                f"R@70={r[a]['recall_at_70_precision']}" for a in APPROACHES))
    print("test Poisson deviance:", t["poisson_deviance"])


if __name__ == "__main__":
    main()
