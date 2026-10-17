"""Spark-free tests for the alert engine's restart and state handling.

    .venv/bin/python tests/test_stream_alerts.py
"""
import json
import shutil
import sys
import tempfile
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src import stream_alerts as sa  # noqa: E402

LIVE_STATE = Path(__file__).resolve().parent.parent / "data" / "live" / "latest_state.parquet"


class FakeState:
    """Minimal stand-in for Spark's GroupState."""
    def __init__(self, value=None):
        self.value, self.hasTimedOut, self.timeout = value, False, None

    @property
    def exists(self):
        return self.value is not None

    @property
    def get(self):
        return self.value

    def update(self, v):
        self.value = v

    def setTimeoutTimestamp(self, ms):
        self.timeout = ms

    def remove(self):
        self.value = None


def batch(t0=1_792_200_000, n=3, drop=()):
    df = pd.DataFrame({"station_id": ["s1"] * n, "last_reported": [t0 + 120 * i for i in range(n)],
                       "num_bikes_available": [10 - i for i in range(n)], "num_docks_available": [5 + i for i in range(n)],
                       "num_ebikes_available": [2] * n, "is_installed": [True] * n, "is_renting": [True] * n,
                       "is_returning": [True] * n})
    return df.drop(columns=list(drop))


def run(state, pdf):
    out = list(sa.station_state(("s1",), iter([pdf]), state))
    return out[0].iloc[0] if out else None


def test_seed_latest():
    tmp = Path(tempfile.mkdtemp())
    try:
        assert sa.seed_latest(tmp / "missing.parquet").empty
        if LIVE_STATE.exists():  # the exact file that crashed the restart (read-only)
            df = sa.seed_latest(LIVE_STATE)
            assert len(df) > 2000 and list(df.columns) == sa.OUT_SCHEMA_COLS[1:]
            assert (df["n_obs"] == 0).all()
        old = pd.DataFrame({"station_id": ["a", "b"], "last_reported": [1, 2], "bikes": [3, 4], "docks": [5, 6]})
        old.to_parquet(tmp / "old.parquet")
        df = sa.seed_latest(tmp / "old.parquet")
        assert list(df.index) == ["a", "b"] and df["change_15m"].isna().all() and (df["n_obs"] == 0).all()
        (tmp / "bad.parquet").write_bytes(b"not parquet")
        assert sa.seed_latest(tmp / "bad.parquet").empty
        pd.DataFrame({"x": [1]}).to_parquet(tmp / "nokey.parquet")
        assert sa.seed_latest(tmp / "nokey.parquet").empty
    finally:
        shutil.rmtree(tmp)


def test_state_fresh_and_current():
    st = FakeState()
    r = run(st, batch())
    assert r["bikes"] == 8 and r["n_obs"] == 3 and st.exists
    r2 = run(st, batch(t0=1_792_200_000 + 360, n=1))  # history: 10, 9, 8 bikes, then 10
    assert r2["n_obs"] == 4 and r2["bikes"] == 10
    assert r2["change_5m"] == 0 and pd.isna(r2["change_15m"])  # 5 min ago: 10 bikes; 15 min: no data


def test_state_older_or_broken():
    hist = json.dumps([[1_792_199_000], [7], [9]])
    for value in [(hist,), (hist, 1), (None, None, None, None, None), ("not json", 1, True, True, True),
                  (json.dumps([[1, 2], [3], [4]]), 0, True, True, True)]:
        r = run(FakeState(value), batch())
        assert r is not None and r["bikes"] == 8, value


def test_input_missing_columns():
    r = run(FakeState(), batch(drop=("num_ebikes_available", "is_renting")))
    assert r is not None and r["bikes"] == 8 and pd.isna(r["ebikes"])


def test_checkpoint_guard():
    tmp = Path(tempfile.mkdtemp())
    try:
        ck = tmp / "ck"
        sa.checkpoint_guard(ck)  # new: created + stamped
        assert (ck / "bikeflow_state_version").read_text().strip() == sa.STATE_VERSION
        (ck / "offsets").mkdir()
        sa.checkpoint_guard(ck)  # same version: kept
        assert (ck / "offsets").exists()
        (ck / "bikeflow_state_version").write_text("0\n")
        sa.checkpoint_guard(ck)  # older version: moved aside, fresh dir
        aside = [p for p in tmp.iterdir() if p.name.startswith("ck.incompatible-")]
        assert len(aside) == 1 and (aside[0] / "offsets").exists() and not (ck / "offsets").exists()
        legacy = tmp / "legacy"
        (legacy / "commits").mkdir(parents=True)
        sa.checkpoint_guard(legacy)  # unversioned: assumed compatible, stamped, kept
        assert (legacy / "commits").exists() and (legacy / "bikeflow_state_version").exists()
    finally:
        shutil.rmtree(tmp)


if __name__ == "__main__":
    tests = [v for k, v in dict(globals()).items() if k.startswith("test_")]
    for t in tests:
        t()
        print(f"ok  {t.__name__}")
    print(f"{len(tests)} tests passed")
