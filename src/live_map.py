"""Map live GBFS station_id (UUID) to the trip short id used by the models.

The primary mapping is the existing station table, data/processed/stations/stations.parquet
(gbfs_station_id <-> station_id). If it is missing, models/station_index.json is used (model
stations only). As a cross-check, the live station_information short_name (padded like
src/stations.py) is reported too: it shows live stations the table cannot map, such as
stations opened after the trip data.

    python -m src.live_map    # report against the newest live status + station_info files
"""
import json
import re

import pandas as pd
import pyarrow.dataset as ds

from src.collector import INFO_DIR, STATUS_DIR
from src.common import ROOT, STATIONS_DIR

STATION_INDEX_JSON = ROOT / "models" / "station_index.json"  # same file as src.build_features (no pyspark import)


def pad_short_name(short_name: str) -> str:
    """Restore a lost trailing zero ("6131.1" -> "6131.10"); same rule as src.stations."""
    return short_name + "0" if re.fullmatch(r"\d+\.\d", short_name) else short_name


def mapping_table() -> pd.DataFrame:
    """gbfs_station_id -> station_id (trip short id) and whether the model uses the station."""
    model_ids = {s["station_id"] for s in json.loads(STATION_INDEX_JSON.read_text())["stations"]}
    path = STATIONS_DIR / "stations.parquet"
    if path.exists():
        t = pd.read_parquet(path, columns=["station_id", "gbfs_station_id", "name"])
        t = t[t["gbfs_station_id"].notna()]
    else:
        t = pd.DataFrame(json.loads(STATION_INDEX_JSON.read_text())["stations"])[
            ["station_id", "gbfs_station_id", "name"]]
    t["in_model"] = t["station_id"].isin(model_ids)
    return t.drop_duplicates("gbfs_station_id")


def map_live_ids(gbfs_ids) -> pd.DataFrame:
    """One row per live GBFS id: station_id (None if unmatched) and in_model."""
    live = pd.DataFrame({"gbfs_station_id": pd.Series(list(gbfs_ids), dtype="string").unique()})
    out = live.merge(mapping_table(), on="gbfs_station_id", how="left")
    out["in_model"] = out["in_model"].fillna(False).astype(bool)
    return out


def report(gbfs_ids, station_info: dict | None = None) -> dict:
    m = map_live_ids(gbfs_ids)
    table = mapping_table()
    rep = {
        "live_stations": len(m),
        "matched": int(m["station_id"].notna().sum()),
        "match_rate": round(float(m["station_id"].notna().mean()), 4),
        "matched_model_stations": int(m["in_model"].sum()),
        "model_stations_not_live": int((~table.loc[table["in_model"], "gbfs_station_id"].isin(m["gbfs_station_id"])).sum()),
        "unmatched_examples": m.loc[m["station_id"].isna(), "gbfs_station_id"].head(10).tolist(),
    }
    if station_info:
        info = {s["station_id"]: s for s in station_info["data"]["stations"]}
        un = m[m["station_id"].isna()]
        via = [pad_short_name(info[g]["short_name"]) for g in un["gbfs_station_id"]
               if g in info and info[g].get("short_name")]
        rep["unmatched_with_live_short_name"] = len(via)
        rep["unmatched_names"] = [info[g]["name"] for g in un["gbfs_station_id"] if g in info][:10]
        # Cross-check: does the live short_name agree with the table for matched stations?
        mm = m[m["station_id"].notna() & m["gbfs_station_id"].isin(list(info))]
        agree = [pad_short_name(info[g].get("short_name") or "") == sid
                 for g, sid in zip(mm["gbfs_station_id"], mm["station_id"])]
        rep["short_name_agreement_rate"] = round(sum(agree) / max(len(agree), 1), 4)
    return rep


def main() -> None:
    ids = ds.dataset(str(STATUS_DIR), format="parquet", partitioning="hive").to_table(
        columns=["station_id"]).column("station_id").to_pylist()
    infos = sorted(INFO_DIR.glob("station_information_*.json"))
    info = json.loads(infos[-1].read_text()) if infos else None
    print(json.dumps(report(set(ids), info), indent=2))


if __name__ == "__main__":
    main()
