"""Caching of the EV-Log window read (perf: the tab took ~1 min to load).

``/api/ev_sessions`` used to run ``read_device_df`` — a ``SELECT *`` over the
whole raw wallbox window — on every request, dominating latency.  The endpoint
now memoises the window DataFrame keyed on the device's newest DB timestamp
(``ActionDispatcher._ev_read_window_df``) and folds the live-store tail into a
cheap fingerprint (``_ev_live_fingerprint``) so idle re-loads hit the cache
while an in-progress charge still recomputes.

These tests bind the real methods onto lightweight fakes so we exercise the
actual caching logic without standing up a full AppContext/SQLite stack.
"""
import os
import sys
import threading
import types

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.web.action_dispatch import ActionDispatcher


class _FakeDB:
    def __init__(self, max_ts):
        self._max_ts = max_ts

    def max_timestamp(self, device_key):
        return self._max_ts


class _FakeStorage:
    def __init__(self, max_ts):
        self.db = _FakeDB(max_ts)
        self.read_calls = 0

    def read_device_df(self, device_key, start_ts=None, end_ts=None):
        self.read_calls += 1
        return pd.DataFrame({"timestamp": [start_ts or 0], "total_power": [7000.0]})


class _FakeLiveStore:
    def __init__(self, snap):
        self._snap = snap

    def snapshot(self):
        return self._snap


def _make(max_ts, live_snap=None):
    """A minimal object carrying just the fields the cache helpers touch."""
    obj = types.SimpleNamespace()
    obj.storage = _FakeStorage(max_ts)
    obj.live_store = _FakeLiveStore(live_snap) if live_snap is not None else None
    obj._ev_window_cache = {}
    obj._ev_window_lock = threading.Lock()
    obj._ev_window_ttl = 900.0
    # Bind the real methods onto the fake.
    obj._ev_read_window_df = types.MethodType(ActionDispatcher._ev_read_window_df, obj)
    obj._ev_live_fingerprint = types.MethodType(ActionDispatcher._ev_live_fingerprint, obj)
    return obj


def test_window_read_is_cached_while_db_unchanged():
    obj = _make(max_ts=1_700_000_000)
    a = obj._ev_read_window_df("wb", 30)
    b = obj._ev_read_window_df("wb", 30)
    # Second call served from cache — the expensive SELECT * ran only once.
    assert obj.storage.read_calls == 1
    assert a is b


def test_window_read_busts_when_new_samples_land():
    obj = _make(max_ts=1_700_000_000)
    obj._ev_read_window_df("wb", 30)
    assert obj.storage.read_calls == 1
    # Autosync appended fresh samples → newest DB timestamp advanced.
    obj.storage.db._max_ts = 1_700_003_600
    obj._ev_read_window_df("wb", 30)
    assert obj.storage.read_calls == 2


def test_window_read_keyed_per_window_size():
    obj = _make(max_ts=1_700_000_000)
    obj._ev_read_window_df("wb", 30)
    obj._ev_read_window_df("wb", 7)
    obj._ev_read_window_df("wb", 30)  # 30d again → cached
    # 30d and 7d are distinct cache entries; the repeated 30d is a hit.
    assert obj.storage.read_calls == 2


def test_live_fingerprint_changes_with_new_points():
    snap = {"wb": [{"ts": 100, "power_total_w": 7000}]}
    obj = _make(max_ts=1, live_snap=snap)
    fp1 = obj._ev_live_fingerprint("wb")
    # A new live point arrives (active charge) → fingerprint must differ so the
    # response cache recomputes.
    snap["wb"].append({"ts": 102, "power_total_w": 7100})
    fp2 = obj._ev_live_fingerprint("wb")
    assert fp1 != fp2
    assert fp2 == (2, 102)


def test_live_fingerprint_stable_when_idle():
    snap = {"wb": [{"ts": 100, "power_total_w": 7000}]}
    obj = _make(max_ts=1, live_snap=snap)
    assert obj._ev_live_fingerprint("wb") == obj._ev_live_fingerprint("wb")


def test_live_fingerprint_no_store():
    obj = _make(max_ts=1, live_snap=None)
    assert obj._ev_live_fingerprint("wb") == (0, 0)
