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


def _make_df(n, poll_s=2):
    base = int(time.time()) - 7 * 86400
    ts = np.arange(base, base + n * poll_s, poll_s)[:n]
    pw = (np.sin(np.arange(n) / 21600.0 * np.pi) * 3000).astype(float)
    return pd.DataFrame({"timestamp": pd.to_datetime(ts, unit="s"),
                         "total_power": pw})


def test_soc_timeline_is_bucketed_and_bounded():
    df = _make_df(302400)  # 7 days @ 2 s
    cfg = BatteryConfig(enabled=True, device_key="battery", capacity_kwh=10.0)
    status = get_battery_status(_FakeDB(df), cfg)
    # bucketed to ~minute resolution → far fewer than the raw sample count
    assert 0 < len(status.soc_timeline) <= 11000
    socs = [s[1] for s in status.soc_timeline]
    assert min(socs) >= 0.0 and max(socs) <= 100.0
    # cycle detection still runs on the (bucketed) timeline
    assert status.cycle_count >= 1


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
