"""Dashboard logic over the stored pipeline output (pure functions + small caches)."""
import json
import math

import numpy as np
import pandas as pd

from . import store

HORIZON = 30 * 60
EPISODE_GAP = 10 * 60
TZ = "America/New_York"


def to_epoch(iso: str | None) -> int | None:
    return None if not iso else int(pd.Timestamp(iso).timestamp())


def iso(t) -> str | None:
    return None if t is None or pd.isna(t) else pd.Timestamp(int(t), unit="s", tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------- status ----------
def dash_status(df: pd.DataFrame) -> pd.Series:
    in_service = df["status"] == "ok"
    risk = df["level"].isin(["HIGH", "MEDIUM"]) & df["alert_kind"].str.startswith("risk")
    return pd.Series(np.select(
        [~in_service, df["bikes"] <= 1, df["docks"] <= 1, risk & (df["alert_type"] == "empty"),
         risk & (df["alert_type"] == "full")],
        ["out_of_service", "empty_now", "full_now", "risk_empty", "risk_full"], default="ok"), index=df.index)


def sentence(r) -> str:
    s, pe, pf = r["status"], r["p_empty"], r["p_full"]
    if s == "risk_empty":
        return f"{pe:.0%} chance this station runs out of bikes within 30 minutes"
    if s == "risk_full":
        return f"{pf:.0%} chance this station fills up (no free docks) within 30 minutes"
    if s == "empty_now":
        return f"Empty now: {int(r['bikes'])} bike{'s' if r['bikes'] != 1 else ''} available"
    if s == "full_now":
        return f"Full now: {int(r['docks'])} free dock{'s' if r['docks'] != 1 else ''}"
    if s == "out_of_service":
        return "Out of service (not renting or not returning bikes), or not reporting"
    return f"Low risk: {pe:.0%} chance of running out, {pf:.0%} of filling up in the next 30 minutes"


def station_records(df: pd.DataFrame) -> list[dict]:
    d = df.assign(status=dash_status(df))
    d["probability"] = np.where(d["status"].isin(["risk_full", "full_now"]), d["p_full"],
                                np.where(d["status"].isin(["risk_empty", "empty_now"]), d["p_empty"],
                                         d[["p_empty", "p_full"]].max(axis=1)))
    out = pd.DataFrame({
        "id": d["station_id"], "sid": d["short_id"], "name": d["name"],
        "lat": d["lat"].round(5), "lng": d["lng"].round(5), "bikes": d["bikes"], "docks": d["docks"],
        "ebikes": d["ebikes"], "cap": d["capacity"], "status": d["status"],
        "level": np.where(d["status"].str.startswith("risk"), d["level"], "none"),
        "p": d["probability"].round(4), "pe": d["p_empty"].round(4), "pf": d["p_full"].round(4),
        "ed": d["exp_departures_30m"].round(2), "ea": d["exp_arrivals_30m"].round(2),
        "wf": d["weather_factor_dep"].round(3), "lr": d["last_reported"],
        "c15": d["change_15m"], "c30": d["change_30m"],
    })
    return json.loads(out.to_json(orient="records"))


def kpis(recs: list[dict]) -> dict:
    s = pd.Series([r["status"] for r in recs])
    lv = pd.Series([r["level"] for r in recs])
    return {"stations": len(recs), "in_service": int((s != "out_of_service").sum()),
            "empty_now": int((s == "empty_now").sum()), "full_now": int((s == "full_now").sum()),
            "risk_empty": int((s == "risk_empty").sum()), "risk_full": int((s == "risk_full").sum()),
            "at_risk": int(s.str.startswith("risk").sum()),
            "high": int((lv == "HIGH").sum()), "medium": int((lv == "MEDIUM").sum())}


def weather_of(rec: dict | None) -> dict | None:
    return None if not rec else rec.get("weather")


# ---------- resolved alerts (same outcome rule as src/score_alerts.py) ----------
def episodes(al: pd.DataFrame) -> pd.DataFrame:
    """Risk alerts collapsed into episodes: same station + type, consecutive batches < 10 min apart."""
    r = al[al["alert_kind"].str.startswith("risk")].sort_values(["station_id", "alert_type", "t"]).copy()
    if r.empty:
        return r
    new = (r["station_id"] != r["station_id"].shift()) | (r["alert_type"] != r["alert_type"].shift()) | \
          (r["t"] - r["t"].shift() > EPISODE_GAP)
    r["ep"] = new.cumsum()
    g = r.groupby("ep")
    first = g.first()
    first["t_end"] = g["t"].max()
    first["n_batches"] = g.size()
    first["level"] = np.where(g["level"].apply(lambda x: (x == "HIGH").any()), "HIGH", "MEDIUM")
    first["probability"] = g["probability"].max()
    return first.reset_index(drop=True)


def resolve(source: str, limit: int = 300) -> dict:
    al = store.alerts_history(source)
    files = store.status_files(source)
    stamp = (len(al), len(files), store.mtime(files[-1]) if files else 0)

    def build():
        if al.empty:
            return {"episodes": [], "summary": {"alerts_so_far": 0}, "curve": []}
        a = al
        if source == "live":  # keep the live computation bounded: last 24 h of alerts
            a = al[al["t"] >= al["t"].max() - 24 * 3600]
        ep = episodes(a)
        if ep.empty:
            return {"episodes": [], "summary": {"alerts_so_far": 0}, "curve": []}
        st = store.status_window(source, int(ep["t"].min()), int(ep["t"].max()) + HORIZON)
        data_end = int(st["last_reported"].max()) if len(st) else 0
        groups = {k: g for k, g in st.groupby("station_id")} if len(st) else {}
        outcome, when = [], []
        for _, e in ep.iterrows():
            if e["t"] > data_end - HORIZON:
                outcome.append(None); when.append(None)
                continue
            g = groups.get(e["station_id"])
            col = "num_bikes_available" if e["alert_type"] == "empty" else "num_docks_available"
            if g is None:
                outcome.append(False); when.append(None)
                continue
            fut = g[(g["last_reported"] > e["t"]) & (g["last_reported"] <= e["t"] + HORIZON)]
            hit = fut[fut[col] <= 1]
            outcome.append(bool(len(hit)))
            when.append(int(hit["last_reported"].iloc[0]) if len(hit) else None)
        ep["hit"], ep["hit_at"] = outcome, when
        res = ep[ep["hit"].notna()].sort_values("t")
        summ = {"alerts_so_far": int(len(ep)), "resolved": int(len(res)), "pending": int(ep["hit"].isna().sum()),
                "data_end": iso(data_end), "horizon_min": 30, "unit": "alert episode"}
        for lvl in ("HIGH", "MEDIUM"):
            x = res[res["level"] == lvl]
            summ[f"precision_{lvl.lower()}"] = round(float(x["hit"].mean()), 4) if len(x) else None
            summ[f"resolved_{lvl.lower()}"] = int(len(x))
        summ["precision_all"] = round(float(res["hit"].mean()), 4) if len(res) else None
        curve = []
        hits = res["hit"].astype(int).cumsum().to_numpy()
        for i, (n, h) in enumerate(zip(range(1, len(res) + 1), hits)):
            if n <= 10 or n % max(1, len(res) // 60) == 0 or n == len(res):
                curve.append({"n": n, "precision": round(h / n, 4), "t": iso(res["t"].iloc[i])})
        rows = ep.sort_values("t", ascending=False).head(limit)
        recs = [{"station_id": r.station_id, "name": r.name, "lat": r.lat, "lng": r.lng, "type": r.alert_type,
                 "level": r.level, "probability": round(float(r.probability), 4), "as_of": iso(r.t),
                 "until": iso(r.t_end), "bikes": int(r.bikes), "docks": int(r.docks),
                 "hit": None if r.hit is None or pd.isna(r.hit) else bool(r.hit), "hit_at": iso(r.hit_at),
                 "exp_departures_30m": round(float(r.exp_departures_30m), 2),
                 "exp_arrivals_30m": round(float(r.exp_arrivals_30m), 2)} for r in rows.itertuples()]
        return {"episodes": recs, "summary": summ, "curve": curve}

    return store.cached(("resolved", source), stamp, build)


# ---------- rebalancing ----------
def haversine_m(lat1, lng1, lat2, lng2):
    p1, p2 = np.radians(lat1), np.radians(lat2)
    a = np.sin((p2 - p1) / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(np.radians(lng2 - lng1) / 2) ** 2
    return 2 * 6_371_000 * np.arcsin(np.sqrt(a))


def rebalance(recs: list[dict], max_km: float = 2.0, buffer: int = 5) -> list[dict]:
    """Greedy: highest-risk empty stations first, each takes from the nearest donor with surplus."""
    df = pd.DataFrame(recs)
    if df.empty:
        return []
    recv = df[df["status"] == "risk_empty"].sort_values("p", ascending=False)
    don = df[(df["status"] == "ok") & (df["pe"] < 0.1) & (df["bikes"] >= 8)].copy()
    don["surplus"] = (don["bikes"] - np.ceil(don["ed"]) - buffer).clip(lower=0).astype(int)
    don = don[don["surplus"] > 0]
    out = []
    for r in recv.itertuples():
        if don.empty:
            break
        need = int(max(3, math.ceil(r.ed - r.ea) + 3 - r.bikes))
        need = int(min(need, max(r.docks - 1, 0)))
        if need <= 0:
            continue
        dist = haversine_m(r.lat, r.lng, don["lat"].to_numpy(), don["lng"].to_numpy())
        don["dist"] = dist
        cand = don[(don["dist"] <= max_km * 1000) & (don["surplus"] > 0)].sort_values("dist")
        if cand.empty:
            continue
        d = cand.iloc[0]
        move = int(min(need, d["surplus"]))
        don.loc[d.name, "surplus"] -= move
        out.append({
            "to_id": r.id, "to_name": r.name, "to_lat": r.lat, "to_lng": r.lng, "to_bikes": int(r.bikes),
            "to_probability": r.p, "to_level": r.level, "to_expected_departures": r.ed,
            "to_expected_arrivals": r.ea,
            "from_id": d["id"], "from_name": d["name"], "from_lat": d["lat"], "from_lng": d["lng"],
            "from_bikes": int(d["bikes"]), "from_expected_departures": float(d["ed"]),
            "bikes_to_move": move, "distance_m": int(round(d["dist"])),
            "text": f"Move {move} bike{'s' if move != 1 else ''} from {d['name']} to {r.name}, "
                    f"{int(round(d['dist'], -1))} m away",
        })
    return out


# ---------- city pulse ----------
def pulse(source: str, at: int | None) -> dict:
    files = store.status_files(source)
    stamp = (len(files), store.mtime(files[-1]) if files else 0)

    def build():
        if not files:
            return None
        import pyarrow.dataset as ds
        dset = ds.dataset([str(f) for f in (files if source == "replay" else files[-24 * 12:])], format="parquet")
        t = dset.to_table(columns=["fetched_at", "station_id", "num_bikes_available", "last_reported",
                                   "is_installed"]).to_pandas()
        t = t[t["is_installed"] & (t["last_reported"] >= store.epoch(t["fetched_at"]) - 6 * 3600)]
        t["bin"] = (t["last_reported"] // 900 + 1) * 900
        last = t.sort_values("last_reported").groupby(["station_id", "bin"])["num_bikes_available"].last()
        wide = last.unstack("bin")
        bins = np.arange(wide.columns.min(), wide.columns.max() + 900, 900)
        wide = wide.reindex(columns=bins).ffill(axis=1).bfill(axis=1)
        total = wide.sum(axis=0)
        total = total.iloc[1:-1] if len(total) > 3 else total  # drop partial edge bins
        peak = float(total.max())
        series = [{"t": iso(b), "docked": int(v), "in_use_est": int(peak - v)} for b, v in total.items()]
        ny = pd.to_datetime(total.index, unit="s", utc=True).tz_convert(TZ)
        hourly = (pd.DataFrame({"h": ny.floor("h"), "v": peak - total.to_numpy()})
                  .groupby("h")["v"].mean().round())
        return {"series": series, "hourly": [{"hour": h.strftime("%Y-%m-%dT%H:%M:%S%z"), "label": h.strftime("%a %H:00"),
                                              "in_use_est": int(v)} for h, v in hourly.items()],
                "peak_docked": int(peak),
                "note": "bikes out on the street, estimated as (peak docked total in the recording) - "
                        "(docked now); excludes bikes in depots and rebalancing vans"}

    res = store.cached(("pulse", source), stamp, build)
    if not res:
        return {"series": [], "hourly": [], "current_hour": None}
    cur = pd.Timestamp(at or store.now(), unit="s", tz="UTC").tz_convert(TZ).floor("h")
    return {**res, "current_hour": cur.strftime("%Y-%m-%dT%H:%M:%S%z")}


# ---------- one station ----------
def station_detail(source: str, recs: list[dict], station_id: str, at: int | None, hours: float = 3) -> dict | None:
    r = next((x for x in recs if x["id"] == station_id), None)
    if r is None:
        return None
    end = at or int(store.now())
    hist = store.status_window(source, end - int(hours * 3600), end, station=station_id)
    spark = [{"t": iso(x.last_reported), "bikes": int(x.num_bikes_available), "docks": int(x.num_docks_available)}
             for x in hist.itertuples()]
    moves = [m for m in rebalance(recs) if station_id in (m["to_id"], m["from_id"])]
    row = {**r, "status": r["status"], "p_empty": r["pe"], "p_full": r["pf"], "bikes": r["bikes"], "docks": r["docks"]}
    return {**r, "sentence": sentence(row), "sparkline": spark, "suggestion": moves[0] if moves else None}


def ticker(source: str, at: int | None, limit: int = 30, window_s: int = 3 * 3600) -> list[dict]:
    al = store.alerts_history(source)
    if al.empty:
        return []
    end = at or int(al["t"].max())
    ep = episodes(al[(al["t"] <= end) & (al["t"] >= end - window_s)])
    if ep.empty:
        return []
    ep = ep.sort_values("t", ascending=False).head(limit)
    return [{"as_of": iso(e.t), "station_id": e.station_id, "name": e.name, "type": e.alert_type,
             "level": e.level, "probability": round(float(e.probability), 3), "bikes": int(e.bikes),
             "docks": int(e.docks), "lat": e.lat, "lng": e.lng} for e in ep.itertuples()]
