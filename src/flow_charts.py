"""Stage 2b charts (pandas/pyarrow only, no Spark):

    pr_curves_k3.png            precision-recall, strong drain / fill with k=3 (test)
    calibration_k3.png          predicted probability vs observed frequency (test)
    flow_feature_importances.png  GBT importances, departures and arrivals models
    rain_vs_dry_day.png         a rainy vs a dry weekday at a busy station: expected departures

    python -m src.flow_charts
"""
import json

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
import pyarrow.dataset as ds  # noqa: E402

from src.aggregate_demand import GRID, INK, INK_MUTED, style_axes  # noqa: E402
from src.common import CHARTS_DIR, METRICS_DIR  # noqa: E402
from src.eval_flow import EVAL_INPUT  # noqa: E402
from src.train_model import TEST_MONTH, TRAIN_MONTHS, VAL_MONTH, busiest_stations  # noqa: E402

TZ = "America/New_York"
COLORS = {"model": "#2a78d6", "no_weather": "#1baf7a", "hist_avg": "#52514e", "persistence": "#a9a8a2",
          "stage2_net": "#eb6834"}
LABELS = {"model": "GBT dep/arr + weather", "no_weather": "GBT dep/arr, no weather",
          "hist_avg": "Historical avg (baseline)", "persistence": "Persistence (baseline)",
          "stage2_net": "Stage 2 net model"}
DASH = {"hist_avg": "--", "persistence": ":"}


def pr_curves() -> str:
    pr = json.loads((METRICS_DIR / "flow_pr_curves_k3_test.json").read_text())
    ev = json.loads((METRICS_DIR / "flow_eval.json").read_text())["test"]["excl_disrupted"]
    fig, axes = plt.subplots(1, 2, figsize=(12, 5), sharey=True)
    for ax, kind, title in ((axes[0], "drain", "Strong drain: departures - arrivals >= 3"),
                            (axes[1], "fill", "Strong fill: arrivals - departures >= 3")):
        for a in ("model", "hist_avg", "persistence"):
            c = pr[f"{kind}_{a}"]
            ap = ev[f"strong_{kind}_k3"][a]["average_precision"]
            ax.plot(c["recall"], c["precision"], color=COLORS[a], linestyle=DASH.get(a, "-"), linewidth=2,
                    label=f"{LABELS[a]} (AP {ap:.3f})")
        base = ev[f"strong_{kind}_k3"]["model"]["base_rate"]
        ax.axhline(base, color=GRID, linewidth=1)
        ax.text(1.0, base, f"base rate {base:.1%}", ha="right", va="bottom", fontsize=8, color=INK_MUTED)
        for y in (0.6, 0.7):
            ax.axhline(y, color=GRID, linewidth=0.8, linestyle=":")
        ax.set_xlim(0, 1); ax.set_ylim(0, 1)
        style_axes(ax, title, "Recall", "Precision" if kind == "drain" else "")
        ax.legend(frameon=False, loc="upper right", fontsize=9)
    fig.suptitle(f"Precision-recall, k=3, test month {TEST_MONTH} (disrupted excluded)", x=0.01, ha="left",
                 fontsize=13, color=INK)
    fig.tight_layout()
    p = CHARTS_DIR / "pr_curves_k3.png"
    fig.savefig(p, dpi=150); plt.close(fig)
    return str(p)


def calibration() -> str:
    ev = json.loads((METRICS_DIR / "flow_eval.json").read_text())["test"]["excl_disrupted"]
    fig, axes = plt.subplots(1, 2, figsize=(12, 5.2), sharey=True)
    for ax, kind in zip(axes, ("drain", "fill")):
        ax.plot([0, 1], [0, 1], color=GRID, linewidth=1.2)
        for a in ("model", "hist_avg", "persistence"):
            cal = ev[f"strong_{kind}_k3"][a]["calibration"]
            x = [c["mean_predicted"] for c in cal]
            y = [c["observed_rate"] for c in cal]
            ax.plot(x, y, color=COLORS[a], linestyle=DASH.get(a, "-"), marker="o", markersize=5, linewidth=2,
                    label=LABELS[a])
        ax.set_xlim(0, 1); ax.set_ylim(0, 1)
        style_axes(ax, f"Strong {kind}, k=3", "Predicted probability (bin mean)",
                   "Observed frequency" if kind == "drain" else "")
        ax.legend(frameon=False, loc="upper left", fontsize=9)
    fig.suptitle(f"Calibration, test month {TEST_MONTH} (disrupted excluded; diagonal = perfect)",
                 x=0.01, ha="left", fontsize=13, color=INK)
    fig.tight_layout()
    p = CHARTS_DIR / "calibration_k3.png"
    fig.savefig(p, dpi=150); plt.close(fig)
    return str(p)


def importances() -> str:
    imp = json.loads((METRICS_DIR / "flow_feature_importances.json").read_text())
    fig, axes = plt.subplots(1, 2, figsize=(12, 6))
    weather = {"temperature_2m", "precipitation", "wind_speed_10m", "precip_last_3h"}
    for ax, name, title in ((axes[0], "weather_departures", "Departures model"),
                            (axes[1], "weather_arrivals", "Arrivals model")):
        s = pd.Series(imp[name]).sort_values()
        ax.barh(s.index, s.values, color=["#eb6834" if f in weather else "#2a78d6" for f in s.index], height=0.7)
        for y, v in enumerate(s.values):
            ax.text(v, y, f" {v:.3f}", va="center", fontsize=8, color=INK_MUTED)
        style_axes(ax, title, "GBT feature importance", "")
        ax.grid(axis="y", visible=False); ax.grid(axis="x", color=GRID, linewidth=0.8)
        ax.set_xlim(0, s.max() * 1.2)
    fig.suptitle("Feature importance (orange = weather)", x=0.01, ha="left", fontsize=13, color=INK)
    fig.tight_layout()
    p = CHARTS_DIR / "flow_feature_importances.png"
    fig.savefig(p, dpi=150); plt.close(fig)
    return str(p)


def rain_vs_dry() -> str:
    """Rainiest daytime weekday in val/test vs the closest dry same-weekday, at the busiest station."""
    w = pd.read_parquet("data/processed/weather/weather_hourly.parquet")
    w["local"] = w["hour_utc"].dt.tz_localize("UTC").dt.tz_convert(TZ)
    w = w[(w["local"].dt.strftime("%Y-%m").isin([VAL_MONTH, TEST_MONTH])) & w["local"].dt.hour.between(7, 20)
          & (w["local"].dt.dayofweek < 5)]
    disrupted = set(json.loads((METRICS_DIR / "disrupted_days.json").read_text())["days"])
    daily = w.groupby(w["local"].dt.date)["precipitation"].sum()
    daily = daily[~daily.index.astype(str).isin(disrupted)]
    rainy = daily.idxmax()
    same_wd = daily[[d for d in daily.index if d.weekday() == rainy.weekday() and d != rainy]]
    dry = min(same_wd[same_wd == 0].index, key=lambda d: abs((d - rainy).days))
    sid = busiest_stations(TRAIN_MONTHS)[0]

    fig, axes = plt.subplots(1, 2, figsize=(13, 4.8), sharey=True)
    for ax, day, tag in ((axes[0], rainy, f"Rainy: {daily[rainy]:.1f} mm, 07-21h"), (axes[1], dry, "Dry: 0 mm")):
        ym = f"{day:%Y-%m}"
        t = ds.dataset(str(EVAL_INPUT / f"month={ym}"), format="parquet").to_table(
            columns=["station_id", "ts", "y_dep", "model_dep", "no_weather_dep", "hist_avg_dep"],
            filter=ds.field("station_id") == sid).to_pandas()
        t["local"] = t["ts"].dt.tz_localize("UTC").dt.tz_convert(TZ)
        t = t[t["local"].dt.date == day].sort_values("local")
        x = t["local"].dt.tz_localize(None)
        ax.plot(x, t["y_dep"], color=INK, linewidth=0.9, alpha=0.5, label="Actual departures (next 30 min)")
        ax.plot(x, t["hist_avg_dep"], color=COLORS["hist_avg"], linestyle="--", linewidth=1.5, label=LABELS["hist_avg"])
        ax.plot(x, t["no_weather_dep"], color=COLORS["no_weather"], linewidth=1.5, label=LABELS["no_weather"])
        ax.plot(x, t["model_dep"], color=COLORS["model"], linewidth=2, label=LABELS["model"])
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        style_axes(ax, f"{day:%a %b %-d %Y} - {tag}", "NYC local time", "Departures, next 30 min" if day == rainy else "")
    axes[0].legend(frameon=False, loc="upper left", fontsize=8)
    fig.suptitle(f"Expected departures at the busiest station ({sid}), rainy vs dry weekday", x=0.01, ha="left",
                 fontsize=13, color=INK)
    fig.tight_layout()
    p = CHARTS_DIR / "rain_vs_dry_day.png"
    fig.savefig(p, dpi=150); plt.close(fig)
    return str(p)


def main() -> None:
    for f in (pr_curves, calibration, importances, rain_vs_dry):
        print(f())


if __name__ == "__main__":
    main()
