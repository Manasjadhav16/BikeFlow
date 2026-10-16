"""Current NYC weather from the Open-Meteo forecast endpoint (no key), cached for 15 minutes.

Uses the hourly series (past 7 days + today) so the same lookup serves live "now" and replays
of recent recordings. Mapped exactly like training (src/build_features.py): the record at the
latest hour mark <= the time of interest; temperature/wind are instantaneous at that mark,
precipitation is the sum over the hour before it; precip_last_3h sums the 3 hourly values
ending at that mark. The forecast endpoint's `current` block is kept for reference.

    from src.weather_live import weather_at, weather_factor
    w = weather_at(time.time()); f_dep, f_arr, info = weather_factor(w)
    python -m src.weather_live      # print current conditions + factor
"""
import json
import time

import numpy as np
import requests

from src.common import DATA_PROCESSED, ROOT

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
CACHE_JSON = ROOT / "data" / "cache" / "weather_forecast.json"
CACHE_S = 15 * 60
ADJ_JSON = DATA_PROCESSED / "reference" / "weather_adjustment.json"
VARS = ["temperature_2m", "precipitation", "wind_speed_10m"]
_mem: dict = {}


def _fetch() -> dict:
    r = requests.get(FORECAST_URL, timeout=10, params={
        "latitude": 40.71, "longitude": -74.01, "hourly": ",".join(VARS), "current": ",".join(VARS),
        "timezone": "America/New_York", "timeformat": "unixtime", "past_days": 7, "forecast_days": 1,
    })
    r.raise_for_status()
    return {"fetched_at": time.time(), "data": r.json()}


def forecast(max_age_s: float = CACHE_S) -> dict | None:
    """Cached forecast payload (memory, then disk, then network). None if unavailable."""
    global _mem
    if _mem and time.time() - _mem["fetched_at"] < max_age_s:
        return _mem
    try:
        disk = json.loads(CACHE_JSON.read_text())
        if time.time() - disk["fetched_at"] < max_age_s:
            _mem = disk
            return _mem
    except (OSError, ValueError, KeyError):
        pass
    try:
        _mem = _fetch()
        CACHE_JSON.parent.mkdir(parents=True, exist_ok=True)
        tmp = CACHE_JSON.with_suffix(".tmp")
        tmp.write_text(json.dumps(_mem))
        tmp.replace(CACHE_JSON)
        return _mem
    except (requests.RequestException, ValueError):
        return _mem or None  # stale cache is better than nothing


def weather_at(ts_epoch: float) -> dict | None:
    """Weather at the latest hour mark <= ts (same mapping as training); None if out of range."""
    f = forecast()
    if not f:
        return None
    h = f["data"]["hourly"]
    times = np.asarray(h["time"], dtype=np.int64)
    i = int(np.searchsorted(times, ts_epoch, side="right")) - 1
    if i < 0 or ts_epoch - times[i] > 3600:
        return None
    precip = np.asarray(h["precipitation"], dtype=float)
    return {"hour_mark_epoch": int(times[i]), "temperature_2m": h["temperature_2m"][i],
            "precipitation": h["precipitation"][i], "wind_speed_10m": h["wind_speed_10m"][i],
            "precip_last_3h": float(np.nansum(precip[max(0, i - 2): i + 1])),
            "source": "open-meteo forecast (hourly)", "cache_age_s": round(time.time() - f["fetched_at"])}


def _adjustment() -> dict:
    if "adj" not in _mem:
        _mem["adj"] = json.loads(ADJ_JSON.read_text())
    return _mem["adj"]


def weather_factor(w: dict | None) -> tuple[float, float, dict]:
    """(factor_departures, factor_arrivals, info) from the smoothed adjustment table.
    No weather -> 1.0. A precip/temp cell never seen in training -> nearest temperature band in
    the same precipitation bin."""
    if not w or w.get("temperature_2m") is None or w.get("precipitation") is None:
        return 1.0, 1.0, {"precip_bin": None, "temp_band": None, "note": "no weather: factor 1.0"}
    from src.reference_tables import TEMP_BANDS, precip_bin, temp_band
    pb, tb = str(precip_bin([w["precipitation"]])[0]), str(temp_band([w["temperature_2m"]])[0])
    rows = [r for r in _adjustment()["table"] if r["precip_bin"] == pb]
    if not rows:
        return 1.0, 1.0, {"precip_bin": pb, "temp_band": tb, "note": "bin not in table: factor 1.0"}
    row = min(rows, key=lambda r: abs(TEMP_BANDS.index(r["temp_band"]) - TEMP_BANDS.index(tb)))
    return row["factor_dep"], row["factor_arr"], {"precip_bin": pb, "temp_band": tb,
                                                  "table_cell": f"{row['precip_bin']}/{row['temp_band']}"}


def main() -> None:
    w = weather_at(time.time())
    print(json.dumps({"weather": w, "factor": weather_factor(w)}, indent=2, default=str))


if __name__ == "__main__":
    main()
