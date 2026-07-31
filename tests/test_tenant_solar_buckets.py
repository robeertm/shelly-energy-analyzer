"""Per-bucket tenant solar share — the CO₂-intensity blend behind the Plots chart."""
import os
import sys
import types

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.services.energy_balance import tenant_solar_share_buckets as tsb


class _FakeDB:
    """query_hourly(key, start_ts, end_ts) → hourly kWh frame from a dict."""

    def __init__(self, series):
        # series: {key: {hour_ts: kwh}}
        self.series = series

    def query_hourly(self, key, start_ts, end_ts):
        rows = [(h, k) for h, k in self.series.get(key, {}).items()
                if start_ts <= h < end_ts]
        return pd.DataFrame(rows, columns=["hour_ts", "kwh"])


def _cfg(grid_key="grid", pv_key="pv"):
    solar = types.SimpleNamespace(
        grid_meter_device_key=grid_key,
        pv_meter_device_key="",
        pv_production_device_key=pv_key,
    )
    return types.SimpleNamespace(solar=solar, pv_source=None)


# Sign convention: grid + import / − export; PV ≥ 0; battery + charge / − discharge.

def test_full_grid_night_bucket():
    # PV 0, grid importing → tenant fully on the grid (0.0)
    db = _FakeDB({"grid": {0: 2.0}, "pv": {0: 0.0}})
    assert tsb(db, _cfg(), [(0, 3600)]) == [0.0]


def test_full_pv_export_bucket():
    # PV covers load and exports → pool is all PV_direct → 1.0
    db = _FakeDB({"grid": {0: -1.0}, "pv": {0: 3.0}})
    # pv_direct = 3 - export(1) = 2 ; pool = 2 + grid_import(0) = 2 → 1.0
    assert tsb(db, _cfg(), [(0, 3600)]) == [1.0]


def test_mixed_bucket_two_thirds_solar():
    # pv_direct 2 + grid_import 1 → 2/3 solar
    db = _FakeDB({"grid": {0: 1.0}, "pv": {0: 2.0}})
    out = tsb(db, _cfg(), [(0, 3600)])
    assert round(out[0], 2) == 0.67


def test_daily_bucket_aggregates_over_hours():
    # hour 0: full sun (pv_direct 2, import 0), hour 1: night (import 2)
    db = _FakeDB({"grid": {0: -0.5, 1: 2.0}, "pv": {0: 2.5, 1: 0.0}})
    # h0: pv_direct = 2.5-0.5 = 2, pool = 2 ; h1: pv_direct 0, pool 2
    # Σpvd/Σpool = 2 / 4 = 0.5
    out = tsb(db, _cfg(), [(0, 7200)])
    assert round(out[0], 2) == 0.5


def test_no_meter_configured_is_zero():
    db = _FakeDB({})
    assert tsb(db, _cfg(grid_key="", pv_key=""), [(0, 3600), (3600, 7200)]) == [0.0, 0.0]


def test_none_bucket_defaults_to_grid():
    db = _FakeDB({"grid": {0: 1.0}, "pv": {0: 0.0}})
    assert tsb(db, _cfg(), [(None, None)]) == [0.0]
