"""Stage 2 charts from saved predictions and metrics (pandas only, no Spark):

    pred_vs_actual_week.png   3 busiest stations, one test week: actual vs best model vs hist avg
    mae_by_hour.png           test MAE by hour of day, every model and both baselines
    model_comparison.png      test MAE and RMSE, models vs baselines

    python -m src.model_charts
"""
import json

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
import pyarrow.dataset as ds  # noqa: E402

from src.aggregate_demand import GRID, INK, style_axes  # noqa: E402
from src.build_features import STATION_INDEX_JSON, TARGET  # noqa: E402
from src.common import CHARTS_DIR, METRICS_DIR  # noqa: E402
from src.train_model import PRED_DIR, TEST_MONTH, TRAIN_MONTHS, busiest_stations  # noqa: E402

WEEK = ("2026-09-14", "2026-09-21")  # Mon-Sun, no disrupted days
TZ = "America/New_York"
# Fixed categorical order (validated reference palette); baselines in neutral grays.
COLORS = {"persistence": "#a9a8a2", "hist_avg": "#52514e", "linear_regression": "#1baf7a",
          "random_forest": "#2a78d6", "gbt": "#eb6834"}
LABELS = {"persistence": "Persistence (baseline)", "hist_avg": "Historical avg (baseline)",
          "linear_regression": "Linear regression", "random_forest": "Random forest", "gbt": "GBT"}
ACTUAL = "#0b0b0b"


def pred_vs_actual(best: str) -> str:
    stations = busiest_stations(TRAIN_MONTHS)[:3]
    names = {s["station_id"]: s["name"] for s in json.loads(STATION_INDEX_JSON.read_text())["stations"]}
    lo = pd.Timestamp(WEEK[0], tz=TZ).tz_convert("UTC").tz_localize(None)
    hi = pd.Timestamp(WEEK[1], tz=TZ).tz_convert("UTC").tz_localize(None)
    d = ds.dataset(str(PRED_DIR / f"month={TEST_MONTH}"), format="parquet")
    t = d.to_table(columns=["station_id", "ts", TARGET, f"pred_{best}", "pred_hist_avg"],
                   filter=ds.field("station_id").isin(stations)).to_pandas()  # 3 stations x 1 month
    t = t[(t["ts"] >= lo) & (t["ts"] < hi)].copy()
    t["ts"] = t["ts"].dt.tz_localize("UTC").dt.tz_convert(TZ).dt.tz_localize(None)

    fig, axes = plt.subplots(3, 1, figsize=(12, 9), sharex=True)
    for ax, sid in zip(axes, stations):
        s = t[t["station_id"] == sid].sort_values("ts")
        ax.plot(s["ts"], s[TARGET], color=ACTUAL, linewidth=0.8, alpha=0.55, label="Actual")
        ax.plot(s["ts"], s["pred_hist_avg"], color=COLORS["hist_avg"], linewidth=1.2, linestyle="--",
                label=LABELS["hist_avg"])
        ax.plot(s["ts"], s[f"pred_{best}"], color=COLORS[best], linewidth=1.6, label=f"{LABELS[best]} (best)")
        ax.axhline(0, color=GRID, linewidth=1)
        style_axes(ax, f"{names.get(sid, sid)}  ({sid})", "", "Bike change, next 30 min")
        ax.title.set_fontsize(11)
    axes[0].legend(frameon=False, loc="upper right", ncol=3, fontsize=9)
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%a %b %-d"))
    fig.suptitle(f"Predicted vs actual 30-min bike change, busiest stations, week of {WEEK[0]} (test)",
                 x=0.01, ha="left", fontsize=13, color=INK)
    fig.tight_layout()
    p = CHARTS_DIR / "pred_vs_actual_week.png"
    fig.savefig(p, dpi=150); plt.close(fig)
    return str(p)


def mae_by_hour(res: dict) -> str:
    by_hour = res["results"]["test"]["mae_by_hour_excl_disrupted"]
    fig, ax = plt.subplots(figsize=(10, 5.5))
    for m, series in by_hour.items():
        h = sorted(series, key=int)
        ax.plot([int(x) for x in h], [series[x] for x in h], color=COLORS[m], linewidth=2,
                linestyle="--" if m in ("persistence", "hist_avg") else "-", label=LABELS[m])
    ax.set_xticks(range(0, 24, 2))
    style_axes(ax, f"Test MAE by hour of day ({TEST_MONTH}, disrupted rows excluded)",
               "Hour of day (NYC local)", "MAE (bikes, next 30 min)")
    ax.legend(frameon=False, loc="upper left")
    fig.tight_layout()
    p = CHARTS_DIR / "mae_by_hour.png"
    fig.savefig(p, dpi=150); plt.close(fig)
    return str(p)


def model_comparison(res: dict) -> str:
    o = res["results"]["test"]["excl_disrupted"]["overall"]
    alerts = METRICS_DIR / "alert_eval.json"
    zero = json.loads(alerts.read_text())["test"]["reference_always_zero"] if alerts.exists() else None
    order = ["persistence", "hist_avg", "linear_regression", "random_forest", "gbt"]
    order = [m for m in order if m in o]
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.8))
    for ax, metric in zip(axes, ("mae", "rmse")):
        vals = [o[m][metric] for m in order]
        y = range(len(order))[::-1]
        ax.barh(list(y), vals, color=[COLORS[m] for m in order], height=0.65)
        for yi, v in zip(y, vals):  # inside the bar, clear of the reference line
            ax.text(v * 0.97, yi, f"{v:.3f}", va="center", ha="right", fontsize=9, color="white",
                    fontweight="bold")
        ax.set_yticks(list(y), [LABELS[m] for m in order])
        style_axes(ax, metric.upper(), "", "")
        ax.grid(axis="y", visible=False); ax.grid(axis="x", color=GRID, linewidth=0.8)
        ax.set_xlim(0, max(vals) * 1.18)
        if zero:  # what predicting 0 everywhere scores: the target is 0 in ~60% of rows
            ax.axvline(zero[metric], color=INK, linewidth=1, linestyle=":")
            ax.text(zero[metric], len(order) - 0.45, f"always 0: {zero[metric]:.3f}", fontsize=8,
                    color=INK, ha="center", va="bottom")
    fig.suptitle(f"Test month {TEST_MONTH}: 30-min bike-change error, all rows (disrupted excluded)",
                 x=0.01, ha="left", fontsize=13, color=INK)
    fig.tight_layout()
    p = CHARTS_DIR / "model_comparison.png"
    fig.savefig(p, dpi=150); plt.close(fig)
    return str(p)


def main() -> None:
    res = json.loads((METRICS_DIR / "model_results.json").read_text())
    best = res["best_model_by_validation_mae"]
    for p in (pred_vs_actual(best), mae_by_hour(res), model_comparison(res)):
        print(p)


if __name__ == "__main__":
    main()
