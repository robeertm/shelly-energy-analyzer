"""Per-bucket tenant solar share — the green/red colouring behind the Plots chart.

The tenant is grid-parallel and served **last**: only genuine PV surplus (PV left
over once the whole house — including any battery discharge — is covered) counts
as green. Per hour ``tenant_pv = max(0, tenant_load − grid_import − battery_discharge)``
and the bucket share is ``Σ tenant_pv / Σ tenant_load``.
"""
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


def _cfg(grid_key="grid", pv_key="pv", with_battery=False, tenant_key="ten"):
    solar = types.SimpleNamespace(
        grid_meter_device_key=grid_key,
        pv_meter_device_key="",
        pv_production_device_key=pv_key,
    )
    pv_source = None
    if with_battery:
        # Battery series resolves to the "battery" key when a source is mapped.
        pv_source = types.SimpleNamespace(
            enabled=True,
            grid_power_entity="", mqtt_grid_power_topic="",
            pv_power_entity="", mqtt_pv_power_topic="",
            battery_power_entity="sensor.batt", mqtt_battery_power_topic="",
        )
    tenants = []
    if tenant_key:
        tenants = [types.SimpleNamespace(name="Mieter", tenant_id="m1",
                                         device_keys=[tenant_key])]
    tenant = types.SimpleNamespace(enabled=bool(tenant_key), tenants=tenants)
    return types.SimpleNamespace(solar=solar, pv_source=pv_source, tenant=tenant)


# Sign convention: grid + import / − export; PV ≥ 0; battery + charge / − discharge.

def test_full_grid_night_bucket():
    # PV 0, grid importing → tenant fully on the grid (0.0)
    db = _FakeDB({"grid": {0: 2.0}, "pv": {0: 0.0}, "ten": {0: 1.0}})
    assert tsb(db, _cfg(), [(0, 3600)]) == [0.0]


def test_full_pv_export_bucket():
    # PV covers the house and exports → the tenant's whole load is PV surplus → 1.0
    db = _FakeDB({"grid": {0: -1.0}, "pv": {0: 3.0}, "ten": {0: 1.0}})
    assert tsb(db, _cfg(), [(0, 3600)]) == [1.0]


def test_mixed_bucket_two_thirds_solar():
    # tenant 3, grid import 1 → 2 kWh is PV surplus → 2/3 solar
    db = _FakeDB({"grid": {0: 1.0}, "pv": {0: 5.0}, "ten": {0: 3.0}})
    out = tsb(db, _cfg(), [(0, 3600)])
    assert round(out[0], 2) == 0.67


def test_battery_discharge_leaves_no_surplus():
    # The reported bug: high PV but the house drains the battery to cover its own
    # deficit → no PV surplus for the tenant → grid/red (0.0), not green.
    db = _FakeDB({"grid": {0: 0.05}, "pv": {0: 5.4},
                  "battery": {0: -2.7}, "ten": {0: 0.25}})
    assert tsb(db, _cfg(with_battery=True), [(0, 3600)]) == [0.0]


def test_daily_bucket_aggregates_over_hours():
    # hour 0: full sun exporting (tenant fully PV); hour 1: night (tenant grid)
    db = _FakeDB({"grid": {0: -0.5, 1: 2.0}, "pv": {0: 2.5, 1: 0.0},
                  "ten": {0: 1.0, 1: 1.0}})
    # Σtenant_pv = 1 (h0) + 0 (h1) ; Σtenant_load = 2 → 0.5
    out = tsb(db, _cfg(), [(0, 7200)])
    assert round(out[0], 2) == 0.5


def test_no_meter_configured_is_zero():
    db = _FakeDB({})
    assert tsb(db, _cfg(grid_key="", pv_key=""), [(0, 3600), (3600, 7200)]) == [0.0, 0.0]


def test_none_bucket_defaults_to_grid():
    db = _FakeDB({"grid": {0: 1.0}, "pv": {0: 0.0}, "ten": {0: 1.0}})
    assert tsb(db, _cfg(), [(None, None)]) == [0.0]


def test_no_tenant_load_is_zero():
    # No tenant consumption in the bucket → nothing to colour (0.0).
    db = _FakeDB({"grid": {0: -2.0}, "pv": {0: 5.0}, "ten": {}})
    assert tsb(db, _cfg(), [(0, 3600)]) == [0.0]
