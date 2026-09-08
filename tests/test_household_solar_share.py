"""The HOUSEHOLD's solar share — the number the Live header actually means.

Reported 2026-09-08 from a live screenshot: the header said "SOLAR SHARE 0 %"
while the PV strip right below it drew 1216 W. Both were true statements about
different things — the header was showing ``instantaneous_solar_share``, which
answers the question for a TENANT circuit: a tenant is served last and never
touches the battery, so it is green only while the property EXPORTS. With the
house importing even a few watts that is zero by definition, and PV does not
enter the formula at all.

The household is served FIRST, so its share is plain self-consumption. These
two must never be confused again, so both are tested side by side here.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.services.energy_balance import (
    household_solar_share as hs,
    instantaneous_solar_share as ts,
)


# ── The reported case ────────────────────────────────────────────────────
def test_the_reported_screenshot():
    """Live values read off the running installation: PV 1075 W, grid +12 W
    (importing), tenant 280 W. The tenant reads 0 %, and rightly so — but the
    house was running almost entirely on its own roof."""
    assert ts(pv_w=1075, grid_w=12, batt_w=0, tenant_load_w=280) == 0.0
    share = hs(pv_w=1075, grid_w=12, batt_w=0)
    assert share is not None and 0.98 < share <= 1.0, share


def test_the_two_shares_are_not_the_same_question():
    """Half the house on PV, half on the grid: the household is 50 % solar, the
    tenant gets nothing because there is no surplus to reach it."""
    assert hs(pv_w=1000, grid_w=1000, batt_w=0) == 0.5
    assert ts(pv_w=1000, grid_w=1000, batt_w=0, tenant_load_w=400) == 0.0


# ── The household number itself ──────────────────────────────────────────
def test_night_is_zero():
    assert hs(pv_w=0, grid_w=2400, batt_w=0) == 0.0


def test_exported_power_is_not_self_consumption():
    """3 kW from the roof, 2 kW leaving the property: only the kilowatt that
    stayed served the house — and it served all of it."""
    assert hs(pv_w=3000, grid_w=-2000, batt_w=0) == 1.0


def test_charging_the_battery_is_not_serving_the_house():
    """PV 3000, 2000 of it going into the battery, 500 imported. The house is
    served by 1000 W of PV and 500 W of grid → two thirds solar."""
    share = hs(pv_w=3000, grid_w=500, batt_w=2000)
    assert abs(share - (1000.0 / 1500.0)) < 1e-9, share


def test_battery_discharge_counts_as_load_not_as_solar():
    """Stored sunshine is not sunshine now — the CO2 attribution keeps the two
    apart for the same reason, and so does this number. Evening: no PV, the
    battery carries the house → 0 % SOLAR share (autarky is a separate figure)."""
    assert hs(pv_w=0, grid_w=0, batt_w=-1800) == 0.0


def test_battery_discharge_alongside_pv():
    """PV 500 direct, battery adding 500: half the current draw is live solar."""
    assert hs(pv_w=500, grid_w=0, batt_w=-500) == 0.5


def test_no_load_is_undefined_not_zero():
    """Everything off: a share of nothing is not 0 %, it is no answer at all —
    and the card must be able to tell the difference."""
    assert hs(pv_w=0, grid_w=0, batt_w=0) is None


def test_it_never_leaves_the_unit_interval():
    """Meters disagree by a few watts all the time; that must not produce 140 %."""
    for pv, grid, batt in ((5000, -6000, 0), (100, -50, -900), (0, -10, 0),
                           (3000, 20, 5000), (-5, 10, 0)):
        share = hs(pv_w=pv, grid_w=grid, batt_w=batt)
        assert share is None or 0.0 <= share <= 1.0, (pv, grid, batt, share)


def test_a_broken_meter_yields_no_answer_rather_than_a_broken_payload():
    """NaN would be serialised as bare `NaN` in the JSON response, which strict
    parsers reject — the card must rather show nothing."""
    for bad in (None, "", "abc", float("nan")):
        assert hs(pv_w=bad, grid_w=bad, batt_w=bad) is None, bad
    assert hs(pv_w=1000, grid_w=float("nan"), batt_w=0) is None
