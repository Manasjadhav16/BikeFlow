"""PRELIMINARY nowcast experiment on the collected live snapshots.

Question: for a station at snapshot time t, will it be EMPTY (<= 1 bike) or FULL (<= 1 free
dock) at any snapshot in (t, t + 30 min]? Two labels; each only for rows where the station is
not already empty / not already full at t (a real forward-looking prediction).

Snapshots are the collector's poll times (data/live/status, read-only). The collector writes a
station only when its report changed, so a station's state at t is its latest report at or
before t. A row is built only when snapshots cover [t - 30 min, t] (for the change features)
and [t, t + 30 min] (for the label) with no gap longer than 5 minutes; the rest is dropped
and counted. Stations out of service or silent for more than 60 minutes at t are excluded.

Features: bikes, docks, capacity, change in bikes over 5 / 15 / 30 min, expected departures and
arrivals for the next 30 min (the alert engine's lookup: historical average for the overlapping
15-min buckets x weather factor), temperature, precipitation, hour and weekday (New York).

Split by time, never randomly: with >= 3 usable New York days, test = latest full day,
validation = the day before, train = the rest; otherwise a chronological 60/20/20 split of the
usable snapshot times (reported as a fallback). Negatives are subsampled for training only.

Models: persistence (no alert), the rule bikes <= 2 / docks <= 2, the alert engine score
(Skellam), and Spark MLlib logistic regression, random forest (small) and GBT. Thresholds for
scored models are chosen on validation (max F1). Metrics on the full test set.

    python -m src.nowcast
"""
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.dataset as ds
from scipy.stats import skellam

from src.common import CHARTS_DIR, METRICS_DIR, ROOT, check_disk, get_spark, stop_spark, write_json
from src.reference_tables import REF_DIR
from src.weather_live import weather_at, weather_factor

STATUS_DIR = ROOT / "data" / "live" / "status"           # read-only
SCRATCH = ROOT / "data" / "scratch_nowcast"              # this job's own Spark scratch root
OUT_JSON = METRICS_DIR / "nowcast_metrics.json"
TZ = "America/New_York"
H = 30 * 60
MAX_GAP = 5 * 60
SILENT_S = 60 * 60
STALE_REPORT_S = 6 * 3600
NEG_PER_POS = 4
SEED = 42
FEATURES = ["bikes", "docks", "capacity", "chg_5m", "chg_15m", "chg_30m", "exp_dep_30m", "exp_arr_30m",
            "temperature", "precipitation", "hour", "weekday"]


def epoch(s: pd.Series) -> np.ndarray:
    return ((pd.to_datetime(s, utc=True) - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta(seconds=1)).to_numpy()


def stamp(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


# ---------------- snapshots and station state ----------------
def load_reports() -> pd.DataFrame:
    t = ds.dataset(str(STATUS_DIR), format="parquet", partitioning="hive", exclude_invalid_files=True).to_table(
        columns=["fetched_at", "station_id", "num_bikes_available", "num_docks_available", "is_installed",
                 "is_renting", "is_returning", "last_reported"]).to_pandas()
    t["f"] = epoch(t["fetched_at"])
    t = t[t["last_reported"] >= t["f"] - STALE_REPORT_S]  # placeholder / decommissioned records
    return t.sort_values(["station_id", "f"]).drop_duplicates(["station_id", "f"], keep="last")


def coverage(S: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """For each snapshot: are [t - H, t] and [t, t + H] covered with no gap > MAX_GAP?"""
    def ok(a, b):
        x = S[(S >= a) & (S <= b)]
        return len(x) > 0 and np.diff(np.concatenate([[a], x, [b]])).max() <= MAX_GAP
    return np.array([ok(t - H, t) for t in S]), np.array([ok(t, t + H) for t in S])


def state_grid(rep: pd.DataFrame, S: np.ndarray, ids: list[str]) -> dict:
    """Station x snapshot arrays: latest report at or before each snapshot (NaN before the first)."""
    n, m = len(ids), len(S)
    g = {k: np.full((n, m), np.nan) for k in ("bikes", "docks", "in_service", "lr")}
    for r, (sid, x) in enumerate(rep[rep["station_id"].isin(ids)].groupby("station_id")):
        r = ids.index(sid) if ids[r] != sid else r
        i = np.searchsorted(x["f"].to_numpy(), S, side="right") - 1
        have = i >= 0
        ii = i[have]
        g["bikes"][r, have] = x["num_bikes_available"].to_numpy()[ii]
        g["docks"][r, have] = x["num_docks_available"].to_numpy()[ii]
        svc = (x["is_installed"] & x["is_renting"] & x["is_returning"]).to_numpy().astype(float)
        g["in_service"][r, have] = svc[ii]
        g["lr"][r, have] = x["last_reported"].to_numpy()[ii]
    return g


# ---------------- the alert engine's expected-demand lookup ----------------
def expected_matrices(ids: list[str]):
    exp = pd.read_parquet(REF_DIR / "expected_demand.parquet")
    exp = exp[exp["gbfs_station_id"].isin(ids)]
    row = exp["gbfs_station_id"].map({g: i for i, g in enumerate(ids)}).to_numpy()
    slot = ((exp["day_of_week"] - 1) * 96 + exp["hour"] * 4 + exp["quarter"]).to_numpy()
    E_dep, E_arr = np.zeros((len(ids), 672)), np.zeros((len(ids), 672))
    E_dep[row, slot], E_arr[row, slot] = exp["exp_departures"].to_numpy(), exp["exp_arrivals"].to_numpy()
    return E_dep, E_arr


def expected_next_30m(E_dep, E_arr, now: int):
    """Same as AlertEngine.expected_next_30m: overlap-weighted buckets covering [now, now + 30 min)."""
    b0 = now - now % 900
    w0 = (b0 + 900 - now) / 900
    mu_d, mu_a = np.zeros(E_dep.shape[0]), np.zeros(E_dep.shape[0])
    for start, w in ((b0, w0), (b0 + 900, 1.0), (b0 + 1800, 1.0 - w0)):
        if w <= 0:
            continue
        ny = pd.Timestamp(start, unit="s", tz="UTC").tz_convert(TZ)
        k = ny.dayofweek * 96 + ny.hour * 4 + ny.minute // 15
        mu_d += w * E_dep[:, k]
        mu_a += w * E_arr[:, k]
    return mu_d, mu_a


# ---------------- assemble rows ----------------
def build_rows():
    rep = load_reports()
    S = np.array(sorted(rep["f"].unique()))
    past_ok, fut_ok = coverage(S)
    live = pd.read_parquet(REF_DIR / "stations_live.parquet")
    ids = sorted(set(live["gbfs_station_id"]) & set(rep["station_id"]))
    cap = live.set_index("gbfs_station_id")["capacity"].reindex(ids).to_numpy(dtype=float)
    g = state_grid(rep, S, ids)
    E_dep, E_arr = expected_matrices(ids)
    counts = {"snapshots": int(len(S)), "stations_with_history": len(ids),
              "stations_in_feed": int(rep["station_id"].nunique()),
              "snapshots_dropped_past_gap": int((~past_ok).sum()),
              "snapshots_dropped_future_gap": int((past_ok & ~fut_ok).sum()),
              "snapshots_usable": int((past_ok & fut_ok).sum())}
    frames, dropped_station_rows = [], {"unknown_state": 0, "out_of_service_or_silent": 0}
    for i in np.where(past_ok & fut_ok)[0]:
        t = int(S[i])
        b, d = g["bikes"][:, i], g["docks"][:, i]
        known = ~np.isnan(b)
        live_ok = known & (g["in_service"][:, i] == 1) & (g["lr"][:, i] >= t - SILENT_S)
        dropped_station_rows["unknown_state"] += int((~known).sum())
        dropped_station_rows["out_of_service_or_silent"] += int((known & ~live_ok).sum())
        j = np.searchsorted(S, t + H, side="right")
        fut_b, fut_d = g["bikes"][:, i + 1:j], g["docks"][:, i + 1:j]
        idx = {x: max(np.searchsorted(S, t - x, side="right") - 1, 0) for x in (300, 900, 1800)}
        w = weather_at(t)
        f_dep, f_arr, _ = weather_factor(w)
        mu_d, mu_a = expected_next_30m(E_dep, E_arr, t)
        ny = pd.Timestamp(t, unit="s", tz="UTC").tz_convert(TZ)
        with np.errstate(invalid="ignore"):
            frames.append(pd.DataFrame({
                "station_id": np.array(ids)[live_ok], "t": t,
                "bikes": b[live_ok], "docks": d[live_ok],
                "capacity": np.where(np.isnan(cap), b + d, cap)[live_ok],
                "chg_5m": (b - g["bikes"][:, idx[300]])[live_ok], "chg_15m": (b - g["bikes"][:, idx[900]])[live_ok],
                "chg_30m": (b - g["bikes"][:, idx[1800]])[live_ok],
                "exp_dep_30m": (mu_d * f_dep)[live_ok], "exp_arr_30m": (mu_a * f_arr)[live_ok],
                "temperature": (w or {}).get("temperature_2m", np.nan), "precipitation": (w or {}).get("precipitation", np.nan),
                "hour": ny.hour, "weekday": ny.dayofweek + 1,
                "y_empty": (np.nanmin(fut_b, axis=1) <= 1)[live_ok], "y_full": (np.nanmin(fut_d, axis=1) <= 1)[live_ok],
            }))
    df = pd.concat(frames, ignore_index=True)
    df[["chg_5m", "chg_15m", "chg_30m"]] = df[["chg_5m", "chg_15m", "chg_30m"]].fillna(0)
    counts["dropped_station_rows"] = dropped_station_rows
    counts["rows"] = int(len(df))
    return df, S, past_ok & fut_ok, counts


def engine_scores(df: pd.DataFrame) -> pd.DataFrame:
    mu_d = np.clip(df["exp_dep_30m"].to_numpy(), 1e-9, None)
    mu_a = np.clip(df["exp_arr_30m"].to_numpy(), 1e-9, None)
    k_e, k_f = df["bikes"].to_numpy() - 1, df["docks"].to_numpy() - 1
    df["engine_empty"] = np.where(k_e <= 0, 1.0, np.nan_to_num(skellam.sf(k_e - 1, mu_d, mu_a)))
    df["engine_full"] = np.where(k_f <= 0, 1.0, np.nan_to_num(skellam.sf(k_f - 1, mu_a, mu_d)))
    return df


# ---------------- split ----------------
def time_split(df: pd.DataFrame, usable_t: np.ndarray) -> tuple[pd.Series, dict]:
    ny_day = pd.to_datetime(usable_t, unit="s", utc=True).tz_convert(TZ).strftime("%Y-%m-%d")
    per_day = pd.Series(ny_day).value_counts().sort_index()
    full_days = [d for d, n in per_day.items() if n >= 0.8 * 24 * 30]  # >= 80% of a day's ~1-2 min snapshots
    if len(full_days) >= 3:
        test_day, val_day = full_days[-1], full_days[-2]
        day = pd.to_datetime(df["t"], unit="s", utc=True).dt.tz_convert(TZ).dt.strftime("%Y-%m-%d")
        split = np.where(day == test_day, "test", np.where(day == val_day, "validation",
                                                            np.where(day < val_day, "train", "unused")))
        rule = f"test = latest full NY day ({test_day}), validation = {val_day}, train = earlier"
    else:
        ts = np.sort(usable_t)
        c1, c2 = ts[int(len(ts) * 0.6)], ts[int(len(ts) * 0.8)]
        split = np.where(df["t"] < c1, "train", np.where(df["t"] < c2, "validation", "test"))
        rule = ("FALLBACK: fewer than 3 usable full New York days, so a chronological 60/20/20 split of the "
                "usable snapshot times")
    split = pd.Series(split, index=df.index)
    ranges = {}
    for part in ("train", "validation", "test"):
        tt = df.loc[split == part, "t"]
        if len(tt):
            ranges[part] = {k: pd.Timestamp(int(v), unit="s", tz="UTC").tz_convert(TZ).strftime("%Y-%m-%d %H:%M %Z")
                            for k, v in (("from", tt.min()), ("to", tt.max()))}
            ranges[part]["snapshots"] = int(tt.nunique())
    return split, {"rule": rule, "usable_snapshots_per_ny_day": per_day.to_dict(), "ranges": ranges}


# ---------------- metrics ----------------
def average_precision(y: np.ndarray, s: np.ndarray) -> float:
    """Area under the PR curve (step-wise, ties handled by grouping equal scores)."""
    pos = y.sum()
    if pos == 0:
        return float("nan")
    order = np.argsort(-s, kind="stable")
    ys, ss = y[order], s[order]
    last = np.r_[np.where(np.diff(ss) != 0)[0], len(ss) - 1]  # end of each tie group
    tp = np.cumsum(ys)[last]
    k = last + 1
    prec, rec = tp / k, tp / pos
    return float(np.sum(np.diff(np.r_[0, rec]) * prec))


def pr_curve(y, s, points=120):
    order = np.argsort(-s, kind="stable")
    ys, ss = y[order], s[order]
    last = np.r_[np.where(np.diff(ss) != 0)[0], len(ss) - 1]
    tp = np.cumsum(ys)[last]
    prec, rec = tp / (last + 1), tp / max(y.sum(), 1)
    step = max(1, len(rec) // points)
    return {"recall": np.round(rec[::step], 4).tolist(), "precision": np.round(prec[::step], 4).tolist()}


def at_threshold(y, s, thr) -> dict:
    p = s >= thr
    tp, fp = int((p & y).sum()), int((p & ~y).sum())
    fn, tn = int((~p & y).sum()), int((~p & ~y).sum())
    prec = tp / (tp + fp) if tp + fp else None
    rec = tp / (tp + fn) if tp + fn else None
    f1 = 2 * prec * rec / (prec + rec) if prec and rec else 0.0
    return {"threshold": None if thr is None else round(float(thr), 4), "precision": None if prec is None else round(prec, 4),
            "recall": None if rec is None else round(rec, 4), "f1": round(f1, 4),
            "confusion": {"tp": tp, "fp": fp, "fn": fn, "tn": tn}, "alerts": tp + fp}


def best_threshold(y, s) -> float:
    cands = np.unique(np.quantile(s, np.linspace(0.5, 0.9999, 400)))
    f1 = [at_threshold(y, s, c)["f1"] for c in cands]
    return float(cands[int(np.argmax(f1))])


# ---------------- Spark MLlib ----------------
def fit_predict(spark, train: pd.DataFrame, val: pd.DataFrame, test: pd.DataFrame, label: str) -> dict:
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import GBTClassifier, LogisticRegression, RandomForestClassifier
    from pyspark.ml.feature import StandardScaler, VectorAssembler
    from pyspark.ml.functions import vector_to_array

    cols = FEATURES + [label]
    to_spark = lambda d: spark.createDataFrame(d[cols].astype({label: "int"}).astype("float64").astype({label: "int"}))  # noqa: E731
    tr = to_spark(train).cache()
    asm = VectorAssembler(inputCols=FEATURES, outputCol="raw")
    models = {
        "logistic_regression": Pipeline(stages=[asm, StandardScaler(inputCol="raw", outputCol="features"),
                                                LogisticRegression(featuresCol="features", labelCol=label, regParam=0.01,
                                                                   maxIter=100)]),
        "random_forest": Pipeline(stages=[VectorAssembler(inputCols=FEATURES, outputCol="features"),
                                          RandomForestClassifier(featuresCol="features", labelCol=label, numTrees=30,
                                                                 maxDepth=8, seed=SEED)]),
        "gbt": Pipeline(stages=[VectorAssembler(inputCols=FEATURES, outputCol="features"),
                                GBTClassifier(featuresCol="features", labelCol=label, maxIter=40, maxDepth=5, seed=SEED)]),
    }
    out = {}
    for name, pipe in models.items():
        t0 = time.time()
        m = pipe.fit(tr)
        fit_s = time.time() - t0
        scores = {}
        for part, d in (("validation", val), ("test", test)):
            pr = m.transform(to_spark(d)).select(vector_to_array("probability")[1].alias("p")).toPandas()
            scores[part] = pr["p"].to_numpy()
        out[name] = {"scores": scores, "fit_s": round(fit_s, 1)}
        stamp(f"  {label}: {name} fitted in {fit_s:.0f}s")
    tr.unpersist()
    return out


def chart(results: dict, path: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    colors = {"engine_score": "#2a78d6", "logistic_regression": "#1baf7a", "random_forest": "#eb6834",
              "gbt": "#6250d6", "simple_rule": "#52514e", "persistence": "#a9a8a2"}
    fig, axes = plt.subplots(1, 3, figsize=(17, 5.2))
    for ax, label in zip(axes[:2], ("empty", "full")):
        r = results[label]
        for name, c in colors.items():
            m = r["models"][name]
            if m.get("pr_curve"):
                ax.plot(m["pr_curve"]["recall"], m["pr_curve"]["precision"], color=c, lw=2, label=f"{name} (AP {m['pr_auc']:.3f})")
            elif name == "simple_rule" and m["test"]["precision"] is not None:
                ax.scatter([m["test"]["recall"]], [m["test"]["precision"]], color=c, s=60, zorder=3,
                           label=f"simple rule (P {m['test']['precision']:.2f}, R {m['test']['recall']:.2f})")
        ax.axhline(r["test_base_rate"], color="#a9a8a2", ls=":", lw=1)
        ax.text(1, r["test_base_rate"], f"base rate {r['test_base_rate']:.1%}", ha="right", va="bottom", fontsize=8)
        ax.set_xlim(0, 1); ax.set_ylim(0, 1)
        ax.set_title(f"{label.upper()} within 30 min: precision-recall (test)", loc="left", fontsize=12)
        ax.set_xlabel("Recall"); ax.set_ylabel("Precision"); ax.grid(alpha=0.3)
        ax.legend(fontsize=8, frameon=False, loc="upper right")
    ax = axes[2]
    names = ["persistence", "simple_rule", "engine_score", "logistic_regression", "random_forest", "gbt"]
    x = np.arange(len(names))
    for k, (label, off) in enumerate((("empty", -0.2), ("full", 0.2))):
        f1 = [results[label]["models"][n]["test"]["f1"] for n in names]
        ax.bar(x + off, f1, width=0.4, color=["#ef4444", "#3b82f6"][k], label=label)
        for xi, v in zip(x + off, f1):
            ax.text(xi, v, f"{v:.2f}", ha="center", va="bottom", fontsize=8)
    ax.set_xticks(x, [n.replace("_", "\n") for n in names], fontsize=9)
    ax.set_title("F1 on test (threshold chosen on validation)", loc="left", fontsize=12)
    ax.legend(frameon=False); ax.grid(axis="y", alpha=0.3)
    fig.suptitle("PRELIMINARY nowcast: one ~11-hour evening of live snapshots (see docs/nowcast_notes.md)", x=0.01,
                 ha="left", fontsize=13, color="#b45309")
    fig.tight_layout()
    fig.savefig(path, dpi=140)
    plt.close(fig)


def main() -> None:
    t_all = time.time()
    check_disk("nowcast")
    stamp("START build rows (pandas, read-only on data/live/status)")
    df, S, usable, counts = build_rows()
    df = engine_scores(df)
    split, split_info = time_split(df, S[usable])
    df["split"] = split
    stamp(f"END build rows: {len(df):,} station-snapshot rows")

    check_disk("nowcast spark")
    SCRATCH.mkdir(parents=True, exist_ok=True)
    spark = get_spark("bikeflow-nowcast", driver_memory="3g", cores=3, shuffle_partitions=32, scratch_root=SCRATCH)
    results, rng = {}, np.random.default_rng(SEED)
    try:
        for label, not_already, rule_col, eng in (("empty", df["bikes"] >= 2, "bikes", "engine_empty"),
                                                   ("full", df["docks"] >= 2, "docks", "engine_full")):
            y = f"y_{label}"
            d = df[not_already]
            tr, va, te = (d[d["split"] == p] for p in ("train", "validation", "test"))
            pos = tr[tr[y]]
            neg = tr[~tr[y]]
            neg = neg.iloc[rng.choice(len(neg), size=min(len(neg), NEG_PER_POS * len(pos)), replace=False)]
            tr_s = pd.concat([pos, neg]).sample(frac=1, random_state=SEED)
            stamp(f"{label}: train {len(tr):,} rows ({len(pos):,} positive) -> subsampled {len(tr_s):,}; "
                  f"validation {len(va):,}; test {len(te):,}")
            yv, yt = va[y].to_numpy(), te[y].to_numpy()
            ml = fit_predict(spark, tr_s, va, te, y)
            scored = {"engine_score": (va[eng].to_numpy(), te[eng].to_numpy())}
            scored.update({n: (m["scores"]["validation"], m["scores"]["test"]) for n, m in ml.items()})
            models = {
                "persistence": {"test": at_threshold(yt, np.zeros(len(yt)), 0.5), "pr_auc": round(float(yt.mean()), 4),
                                "note": "predicts the state stays as is: never alerts"},
                "simple_rule": {"test": at_threshold(yt, (te[rule_col] <= 2).to_numpy().astype(float), 0.5),
                                "pr_auc": round(average_precision(yt, (te[rule_col] <= 2).to_numpy().astype(float)), 4),
                                "note": f"alert if {rule_col} <= 2 (among rows not already at <= 1, i.e. exactly 2)"},
            }
            for name, (sv, st) in scored.items():
                thr = best_threshold(yv, sv)
                models[name] = {"validation": at_threshold(yv, sv, thr), "test": at_threshold(yt, st, thr),
                                "pr_auc": round(average_precision(yt, st), 4), "pr_curve": pr_curve(yt, st)}
                if name in ml:
                    models[name]["fit_s"] = ml[name]["fit_s"]
            models["engine_score"]["test_at_engine_medium_0.5"] = at_threshold(yt, te[eng].to_numpy(), 0.5)
            results[label] = {"rows": {"train": int(len(tr)), "train_subsampled": int(len(tr_s)),
                                       "validation": int(len(va)), "test": int(len(te))},
                              "positives": {"train": int(tr[y].sum()), "validation": int(yv.sum()), "test": int(yt.sum())},
                              "test_base_rate": round(float(yt.mean()), 4) if len(yt) else None,
                              "validation_base_rate": round(float(yv.mean()), 4) if len(yv) else None,
                              "models": models}
    finally:
        stop_spark(spark)

    report = {
        "status": "PRELIMINARY: about 11 hours of usable live snapshots (one evening); not a reliable evaluation",
        "computed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "setup": {"horizon_min": 30, "max_gap_min": MAX_GAP / 60, "silent_station_min": SILENT_S / 60,
                  "negatives_per_positive_in_train": NEG_PER_POS, "features": FEATURES,
                  "threshold": "chosen on validation to maximise F1 (scored models only)"},
        "data": counts, "split": split_info, "results": results,
        "runtime_s": round(time.time() - t_all, 1),
    }
    write_json(OUT_JSON, report)
    chart(results, CHARTS_DIR / "nowcast_comparison.png")
    stamp(f"DONE in {report['runtime_s']:.0f}s -> {OUT_JSON}")


if __name__ == "__main__":
    main()
