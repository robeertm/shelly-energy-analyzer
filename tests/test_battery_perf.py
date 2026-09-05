"""Battery status: vectorized extraction + minute-bucketing stays correct.

Regression guard for the 16.58.1 speedup — get_battery_status used to iterrows
over ~300k raw samples and return a 300k-point timeline (~11 s / 800 KB). It now
vectorizes the extraction and buckets to 1-minute mean power (energy-preserving),
so the SOC estimate and cycle detection are unchanged while the point count and
payload drop ~30×.
"""
import os
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.services.battery import get_battery_status
from shelly_analyzer.io.config import BatteryConfig


class _FakeDB:
    def __init__(self, df):
        self._df = df

    def query_samples(self, key, start, end):
        return self._df


def _make_df(n, poll_s=2, ts_unit="s", tz=None):
    base = int(time.time()) - 7 * 86400
    ts = np.arange(base, base + n * poll_s, poll_s)[:n]
    tcol = pd.to_datetime(ts, unit="s")
    # coerce to the requested datetime resolution (s/ms/us/ns)
    tcol = tcol.astype(f"datetime64[{ts_unit}]")
    if tz:
        tcol = tcol.tz_localize("UTC").tz_convert(tz)
    pw = (np.sin(np.arange(n) / 21600.0 * np.pi) * 3000).astype(float)
    return pd.DataFrame({"timestamp": tcol, "total_power": pw})


def test_soc_timeline_is_bucketed_and_bounded():
    df = _make_df(302400)  # 7 days @ 2 s
    cfg = BatteryConfig(enabled=True, device_key="battery", capacity_kwh=10.0)
    status = get_battery_status(_FakeDB(df), cfg)
    # bucketed to ~minute resolution → far fewer than the raw sample count
    assert 0 < len(status.soc_timeline) <= 11000
    # crucially: NOT collapsed to a single bucket (the ÷1e9 resolution bug)
    assert len(status.soc_timeline) > 1000
    socs = [s[1] for s in status.soc_timeline]
    assert min(socs) >= 0.0 and max(socs) <= 100.0
    # timeline timestamps are real epoch seconds, not a collapsed constant
    assert status.soc_timeline[0][0] > 1_000_000_000
    # cycle detection still runs on the (bucketed) timeline
    assert status.cycle_count >= 1


def test_datetime_resolutions_and_tz_do_not_collapse():
    """Regression: datetime64[s]/[ms]/[ns] and tz-aware must all bucket by minute
    (a hardcoded ÷1e9 collapsed [s]-resolution to one point)."""
    cfg = BatteryConfig(enabled=True, device_key="battery", capacity_kwh=10.0)
    for unit in ("s", "ms", "us", "ns"):
        df = _make_df(50000, ts_unit=unit)
        st = get_battery_status(_FakeDB(df), cfg)
        assert len(st.soc_timeline) > 500, f"collapsed for datetime64[{unit}]"
        assert st.soc_timeline[0][0] > 1_000_000_000, f"bad epoch for [{unit}]"
    # tz-aware column
    df_tz = _make_df(50000, ts_unit="ns", tz="Europe/Berlin")
    st = get_battery_status(_FakeDB(df_tz), cfg)
    assert len(st.soc_timeline) > 500


def test_empty_and_disabled():
    cfg_off = BatteryConfig(enabled=False, device_key="")
    assert get_battery_status(_FakeDB(pd.DataFrame()), cfg_off).soc_timeline == []
    cfg = BatteryConfig(enabled=True, device_key="battery", capacity_kwh=10.0)
    empty = pd.DataFrame({"timestamp": pd.to_datetime([], unit="s"),
                          "total_power": []})
    assert get_battery_status(_FakeDB(empty), cfg).soc_timeline == []


def test_epoch_int_timestamps_supported():
    # older callers pass raw epoch ints rather than datetime64
    base = int(time.time()) - 3600
    ts = list(range(base, base + 600, 2))
    df = pd.DataFrame({"timestamp": ts,
                       "total_power": [2000.0] * len(ts)})
    cfg = BatteryConfig(enabled=True, device_key="battery", capacity_kwh=10.0)
    status = get_battery_status(_FakeDB(df), cfg)
    assert len(status.soc_timeline) >= 1


def test_only_the_columns_it_reads_are_selected():
    """The samples table carries 65 columns; the battery reads two.

    Measured on a live installation: /api/battery took 15 s, of which 0.13 s
    was the computation — the rest was SELECT * over ~300k rows of a 65-column
    table.  On a synthetic DB with only two columns populated the difference is
    already 1.64 s vs 0.11 s; where every column carries values it is far
    larger.
    """
    import sqlite3
    import tempfile
    from shelly_analyzer.io.database import EnergyDB

    d = tempfile.mkdtemp(prefix="batcols-")
    db = EnergyDB(os.path.join(d, "energy.db"))
    cols = [r[1] for r in db._conn().execute("PRAGMA table_info(samples)")]
    assert len(cols) > 20, "schema unexpectedly narrow: %d columns" % len(cols)

    base = int(time.time()) - 3600
    rows = [("battery", base + i, float(i)) for i in range(500)]
    conn = db._conn()
    conn.executemany(
        "INSERT INTO samples (device_key, timestamp, total_power) VALUES (?,?,?)", rows)
    conn.commit()

    wide = db.query_samples("battery", base, base + 3600)
    narrow = db.query_samples("battery", base, base + 3600,
                              columns=("timestamp", "total_power"))
    assert len(narrow) == len(wide) == 500
    assert set(narrow.columns) <= {"timestamp", "total_power"}, list(narrow.columns)
    # same numbers, fewer columns
    assert narrow["total_power"].tolist() == wide["total_power"].tolist()

    # a column the table does not have must not turn into an SQL error
    odd = db.query_samples("battery", base, base + 3600,
                           columns=("timestamp", "no_such_column"))
    assert len(odd) == 500

    # and the caller actually asks for the subset
    src = open(os.path.join(os.path.dirname(__file__), "..", "src", "shelly_analyzer",
                            "services", "battery.py"), encoding="utf-8").read()
    assert 'columns=("timestamp", "total_power")' in src, "battery still pulls the wide table"
    print("OK  the battery reads two columns, not sixty-five")

if __name__ == "__main__":
    # Running this file directly must actually run its tests — six of the eleven
    # test files had no such block, so `python3 tests/<file>.py` imported them,
    # exited 0 and ran nothing.  Every test_* in the module, in source order.
    import inspect as _inspect
    import sys as _sys
    _mod = _sys.modules[__name__]
    _fns = [(n, f) for n, f in vars(_mod).items()
            if n.startswith("test_") and callable(f)]
    _fns.sort(key=lambda kv: _inspect.getsourcelines(kv[1])[1])
    for _n, _f in _fns:
        _f()
        print("OK  %s" % _n)
    print("\n%d tests passed (%s)." % (len(_fns), os.path.basename(__file__)))
