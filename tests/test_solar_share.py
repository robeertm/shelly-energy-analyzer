"""Instantaneous solar share — the tenant PV(green)/grid(red) signal.

The tenant is grid-parallel and only benefits from genuine PV *surplus* — the
power the property actually exports. It is green only while the property exports
(``grid_w < 0``); any grid import, even while the battery charges, means no
surplus reaches the tenant, so ``share = max(0, -grid_w) / tenant_load``.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.services.energy_balance import instantaneous_solar_share as ss


def test_full_pv_when_grid_exporting():
    # Grid exporting more than the tenant draws → fully PV-fed surplus.
    assert ss(pv_w=5000, grid_w=-2000, batt_w=0, tenant_load_w=500) == 1.0


def test_full_grid_at_night():
    # No export → nothing green to attribute, tenant is on the grid.
    assert ss(pv_w=0, grid_w=1500, batt_w=0, tenant_load_w=500) == 0.0


def test_import_while_battery_charges_is_grid():
    # The reported bug: grid IMPORTS while the battery charges — no surplus is
    # exported, so the tenant must read grid/red even though PV is high.
    assert ss(pv_w=5379, grid_w=23, batt_w=631, tenant_load_w=364) == 0.0


def test_import_is_grid():
    # Any grid import means no surplus for the tenant.
    assert ss(pv_w=2000, grid_w=100, batt_w=0, tenant_load_w=300) == 0.0


def test_battery_discharge_is_grid():
    # Battery discharging to cover an owner deficit → no export → grid/red.
    assert ss(pv_w=5379, grid_w=44, batt_w=-2683, tenant_load_w=233) == 0.0


def test_partial_surplus_over_tenant_load():
    # Export smaller than the tenant's own draw → only that fraction is green.
    # tenant 500, export 300 → 0.6.
    assert round(ss(pv_w=4000, grid_w=-300, batt_w=0, tenant_load_w=500), 2) == 0.6


def test_no_data_is_zero():
    assert ss(pv_w=0, grid_w=0, batt_w=0, tenant_load_w=0) == 0.0


def test_export_without_tenant_load_is_full():
    # Surplus exists but no tenant load to colour → treated as fully green.
    assert ss(pv_w=5000, grid_w=-3000, batt_w=0, tenant_load_w=0) == 1.0


def test_clamped_and_robust_to_none():
    assert ss(pv_w=None, grid_w=None, batt_w=None, tenant_load_w=None) == 0.0
    assert ss(pv_w=1000, grid_w=-9999, batt_w=0, tenant_load_w=500) == 1.0
