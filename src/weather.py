"""Download hourly NYC weather (Open-Meteo archive, no key) and save it as a small Parquet.

Hourly temperature_2m (°C), precipitation (mm, sum over the PRECEDING hour) and
wind_speed_10m (km/h) at 40.71, -74.01, timezone America/New_York. Timestamps are requested
as unix time so DST changes cannot create duplicate or missing local hours.
Live prediction (Stage 3) will use the Open-Meteo forecast endpoint's `current` values,
mapped the same way.

    python -m src.weather [--start 2025-10-01 --end 2026-09-30]
"""
import argparse

import pandas as pd
import requests

from src.common import DATA_PROCESSED, METRICS_DIR, write_json

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
WEATHER_DIR = DATA_PROCESSED / "weather"
VARS = ["temperature_2m", "precipitation", "wind_speed_10m"]


def fetch(start: str, end: str) -> pd.DataFrame:
    r = requests.get(ARCHIVE_URL, timeout=120, params={
        "latitude": 40.71, "longitude": -74.01, "start_date": start, "end_date": end,
        "hourly": ",".join(VARS), "timezone": "America/New_York", "timeformat": "unixtime",
    })
    r.raise_for_status()
    h = r.json()["hourly"]
    df = pd.DataFrame({"hour_utc": pd.to_datetime(h["time"], unit="s"), **{v: h[v] for v in VARS}})
    return df.sort_values("hour_utc").reset_index(drop=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", default="2025-10-01")
    ap.add_argument("--end", default="2026-09-30")
    a = ap.parse_args()
    df = fetch(a.start, a.end)
    gaps = int((df["hour_utc"].diff().dropna() != pd.Timedelta(hours=1)).sum())
    WEATHER_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(WEATHER_DIR / "weather_hourly.parquet", index=False)
    summary = {
        "source": ARCHIVE_URL, "lat": 40.71, "lon": -74.01, "timezone": "America/New_York",
        "hours": len(df), "first_utc": str(df["hour_utc"].min()), "last_utc": str(df["hour_utc"].max()),
        "hour_gaps": gaps, "missing_values": {v: int(df[v].isna().sum()) for v in VARS},
        "precip_hours_gt_0": int((df["precipitation"] > 0).sum()),
        "max_hourly_precip_mm": float(df["precipitation"].max()),
        "temp_range_c": [float(df["temperature_2m"].min()), float(df["temperature_2m"].max())],
    }
    write_json(METRICS_DIR / "weather_summary.json", summary)
    print(summary)


if __name__ == "__main__":
    main()
