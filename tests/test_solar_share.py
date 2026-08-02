"""Instantaneous solar share — the tenant PV(green)/grid(red) signal.

The tenant is grid-parallel and served **last**: it only counts as PV-fed when
genuine PV surplus spills past the whole house. Every watt of grid import AND
battery discharge is owner-priority and charged to the tenant first, so
``share = max(0, tenant_load − grid_import − battery_discharge) / tenant_load``.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.services.energy_balance import instantaneous_solar_share as ss


def test_full_pv_when_grid_exporting():
    # PV high, grid exporting (real surplus), no battery draw → fully PV-covered.
    assert ss(pv_w=5000, grid_w=-2000, batt_w=0, tenant_load_w=500) == 1.0


def test_full_grid_at_night():
    # No PV → nothing green to attribute, tenant is on the grid.
    assert ss(pv_w=0, grid_w=1500, batt_w=0, tenant_load_w=500) == 0.0


def test_mixed_supply():
    # Tenant draws 300 W, property imports 100 W → 200 W is PV surplus → 2/3.
    assert round(ss(pv_w=2000, grid_w=100, batt_w=0, tenant_load_w=300), 2) == 0.67


def test_battery_discharge_means_no_surplus_for_tenant():
    # The reported bug: PV is high but the house drains the battery to cover its
    # own deficit — there is NO PV surplus for the tenant, so it must read grid.
    assert ss(pv_w=5379, grid_w=44, batt_w=-2683, tenant_load_w=233) == 0.0


def test_pv_going_to_battery_not_counted_as_direct():
    # PV charges the battery while the grid covers load → tenant is on grid.
    assert ss(pv_w=2000, grid_w=500, batt_w=2000, tenant_load_w=300) == 0.0


def test_partial_surplus_over_battery_discharge():
    # Small battery discharge, tenant load exceeds grid+discharge → partial PV.
    # tenant 500, grid import 100, discharge 100 → 300 W surplus → 0.6.
    assert round(ss(pv_w=4000, grid_w=100, batt_w=-100, tenant_load_w=500), 2) == 0.6


def test_no_data_is_zero():
    assert ss(pv_w=0, grid_w=0, batt_w=0, tenant_load_w=0) == 0.0


def test_no_tenant_load_is_zero():
    # Nothing to colour when the tenant draws nothing.
    assert ss(pv_w=5000, grid_w=-3000, batt_w=0, tenant_load_w=0) == 0.0


def test_clamped_and_robust_to_none():
    assert ss(pv_w=None, grid_w=None, batt_w=None, tenant_load_w=None) == 0.0
    assert ss(pv_w=1000, grid_w=-9999, batt_w=0, tenant_load_w=500) == 1.0
