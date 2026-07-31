"""Instantaneous solar share — the tenant PV(green)/grid(red) signal."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.services.energy_balance import instantaneous_solar_share as ss


def test_full_pv_when_grid_exporting():
    # PV high, grid exporting (surplus) → fully PV-covered
    assert ss(pv_w=5000, grid_w=-2000, batt_w=0) == 1.0


def test_full_grid_at_night():
    # no PV, importing → fully from the grid
    assert ss(pv_w=0, grid_w=1500, batt_w=0) == 0.0


def test_mixed_supply():
    # PV 2000 direct + grid import 1000 → 2/3 solar
    assert round(ss(pv_w=2000, grid_w=1000, batt_w=0), 2) == 0.67


def test_pv_going_to_battery_not_counted_as_direct():
    # PV charges the battery while the grid covers load → tenant is on grid
    assert ss(pv_w=2000, grid_w=500, batt_w=2000) == 0.0


def test_no_data_is_zero():
    assert ss(pv_w=0, grid_w=0, batt_w=0) == 0.0


def test_clamped_and_robust_to_none():
    assert 0.0 <= ss(pv_w=None, grid_w=None, batt_w=None) <= 1.0
    assert ss(pv_w=1000, grid_w=-9999, batt_w=0) == 1.0
