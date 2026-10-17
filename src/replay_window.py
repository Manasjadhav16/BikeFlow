"""Pick the busiest recorded window in collected status data (for replay demos).

Busyness = total absolute change in bikes available across stations, summed per 15 minutes
(each station's consecutive reports). Prints the best window's UTC start/end as JSON.

    python -m src.replay_window --status-dir data/live/status --hours 3
"""
import argparse
import json

import numpy as np
import pandas as pd
import pyarrow.dataset as ds

from src.replay import STALE_S, epoch_s


def busiest_window(status_dir: str, hours: float = 3.0) -> dict:
    t = ds.dataset(status_dir, format="parquet", partitioning="hive").to_table(
        columns=["fetched_at", "station_id", "num_bikes_available", "last_reported"]).to_pandas()
    t["f"] = epoch_s(t["fetched_at"])
    t = t[t["last_reported"] >= t["f"] - STALE_S].sort_values(["station_id", "last_reported"])
    t["delta"] = t.groupby("station_id")["num_bikes_available"].diff().abs()
    q = (t["f"] // 900) * 900
    act = t.groupby(q)["delta"].sum()
    grid = np.arange(act.index.min(), act.index.max() + 900, 900)
    act = act.reindex(grid, fill_value=0)
    n = int(hours * 4)
    if len(act) <= n:
        start, end = int(grid[0]), int(grid[-1]) + 900
    else:
        roll = act.rolling(n).sum()
        stop = int(roll.idxmax())
        start, end = stop - (n - 1) * 900, stop + 900
    iso = lambda x: pd.Timestamp(x, unit="s", tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")  # noqa: E731
    return {"start": iso(start), "end": iso(end), "hours": hours,
            "recorded_from": iso(grid[0]), "recorded_to": iso(int(grid[-1]) + 900),
            "bike_changes_in_window": int(act.loc[start:end - 900].sum()),
            "bike_changes_total": int(act.sum())}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--status-dir", default="data/live/status")
    ap.add_argument("--hours", type=float, default=3.0)
    a = ap.parse_args()
    print(json.dumps(busiest_window(a.status_dir, a.hours)))


if __name__ == "__main__":
    main()
