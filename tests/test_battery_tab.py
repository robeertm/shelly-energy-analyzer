"""Battery tab (17.1): cycles from turning points, insights from the chain,
the measured SOC curve.

Rule under test: a cycle is trough → peak → trough on the SOC curve, an
evening dip is not a turning point, and the round-trip efficiency is
corrected for what is still inside when the second trough sits higher. The
daily "charged from the sun / from the grid / discharged" of the tab must be
the same numbers the CO₂ chain carries — one rule for every figure.
"""
import os
import sys
import time
import types
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.services.battery import detect_cycles, get_battery_status  # noqa: E402
from shelly_analyzer.services.battery_insights import compute_battery_insights  # noqa: E402
from shelly_analyzer.io.config import BatteryConfig  # noqa: E402

H = 3600
_TZ = ZoneInfo("Europe/Berlin")


def _tl(points, step_s=60):
    """(soc, power_w) per minute → timeline rows (ts, soc, power, mode)."""
    out = []
    for i, (soc, pw) in enumerate(points):
        out.append((i * step_s, float(soc), float(pw), "charging" if pw > 50 else ("discharging" if pw < -50 else "idle")))
    return out


def test_cycle_is_trough_peak_trough_and_a_dip_is_not_a_turning_point():
    pts = []
    # charge 40 → 100 over 60 min at 6 kW, dip 100 → 96 (a cooker), back to 100,
    # then discharge to 40 over 120 min at 3 kW, then idle.
    pts += [(40 + i, 6000) for i in range(61)]
    pts += [(100 - i, -2000) for i in range(5)] + [(96 + i, 2000) for i in range(5)]
    pts += [(100 - i / 2, -3000) for i in range(121)]
    pts += [(40, 0)] * 30
    cycles = detect_cycles(_tl(pts), capacity_kwh=10.0)
    assert len(cycles) == 1, [c.depth_pct for c in cycles]
    c = cycles[0]
    assert c.depth_pct == 60.0
    # the dip and its recharge count as throughput, not as a cycle of their own
    assert abs(c.charge_kwh - (6.0 + 2000 * 5 / 60 / 1000)) < 0.05
    assert abs(c.discharge_kwh - (6.0 + 2000 * 5 / 60 / 1000)) < 0.11   # ± one minute bucket


def test_efficiency_is_corrected_for_what_stays_inside():
    # charge 10 kWh (20 → 100 on a 12.5 kWh battery), discharge only to 60:
    # 5 kWh stay inside, so a 4 kWh discharge is 4 / (10 − 5) = 80 %, not 40 %.
    pts = [(20 + i * 80 / 100, 6000) for i in range(101)]            # 100 min @ 6 kW = 10 kWh
    pts += [(100 - i * 40 / 80, -3000) for i in range(81)]            # 80 min @ 3 kW = 4 kWh
    pts += [(60, 0)] * 5
    cycles = detect_cycles(_tl(pts), capacity_kwh=12.5)
    assert len(cycles) == 1
    assert 78.0 <= cycles[0].efficiency_pct <= 82.0


class _ChainDB:
    """hourly series + measured SOC rows, the two things the tab reads."""

    def __init__(self, series, soc_rows=None, samples=None):
        self.series = series
        self.soc_rows = soc_rows or []
        self.samples = samples

    def query_hourly(self, key, start_ts=None, end_ts=None, compensate=True):
        rows = [(h, k) for h, k in self.series.get(key, {}).items()
                if (start_ts is None or h >= start_ts) and (end_ts is None or h <= end_ts)]
        return pd.DataFrame(rows, columns=["hour_ts", "kwh"])

    def query_co2_intensity(self, zone, start_ts, end_ts):
        return pd.DataFrame([], columns=["hour_ts", "zone", "intensity_g_per_kwh", "source", "fetched_at"])

    def query_battery_state(self, key, start_ts, end_ts):
        rows = [r for r in self.soc_rows if start_ts <= r[0] <= end_ts]
        return pd.DataFrame(rows, columns=["timestamp", "soc_pct", "power_w"])

    def query_samples(self, key, start, end, columns=None):
        return self.samples


def _cfg():
    solar = types.SimpleNamespace(
        enabled=True, grid_meter_device_key="grid", pv_meter_device_key="",
        pv_production_device_key="pv", battery_device_key="battery",
        grid_display_device_key="", pv_embodied_g_per_kwh=40.0,
        battery_manufacturing_g_per_kwh=20.0, battery_embodied_g_per_kwh=60.0,
        feed_in_tariff_eur_per_kwh=0.08)
    return types.SimpleNamespace(
        solar=solar, pv_source=None,
        tenant=types.SimpleNamespace(enabled=False, tenants=[]),
        battery=types.SimpleNamespace(capacity_kwh=10.0, efficiency_pct=95.0),
        co2=types.SimpleNamespace(bidding_zone="DE_LU"),
        pricing=types.SimpleNamespace(co2_intensity_g_per_kwh=380.0, unit_price_gross=lambda: 0.30),
        devices=[types.SimpleNamespace(key="haus", name="Haus", parent="", subtract_from_parent_display=False, kind="em", phases=3)],
    )


def test_daily_balance_equals_the_chain_and_money_is_price_minus_forgone():
    now = datetime(2026, 9, 18, 12, 0, tzinfo=_TZ)
    d0 = now.replace(hour=0, minute=0)
    h_noon = int((d0 - timedelta(days=1)).replace(hour=12).timestamp())
    h_night = int((d0 - timedelta(days=1)).replace(hour=22).timestamp())
    h_grid = int((d0 - timedelta(days=2)).replace(hour=3).timestamp())
    # yesterday: noon PV 8 → 2 direct, 6 into the battery; 22:00 4 out of the battery.
    # the day before at 03:00: 2 kWh charged from the grid (no PV).
    series = {"grid": {h_noon: 0.0, h_night: 0.0, h_grid: 3.0}, "pv": {h_noon: 8.0, h_night: 0.0, h_grid: 0.0},
              "battery": {h_noon: 6.0, h_night: -4.0, h_grid: 2.0}, "haus": {h_noon: 2.0, h_night: 4.0, h_grid: 1.0}}
    status = types.SimpleNamespace(capacity_kwh=10.0, soc_timeline=[], cycles=[], equivalent_cycles=0.0,
                                   total_discharged_kwh=4.0, measured_since_ts=0)
    ins = compute_battery_insights(_ChainDB(series), _cfg(), status, now=now)
    assert ins["has_chain"]
    by = {d["date"]: d for d in ins["daily"]}
    y = by[(d0 - timedelta(days=1)).strftime("%Y-%m-%d")]
    assert abs(y["charge_pv_kwh"] - 6.0) < 1e-6 and y["charge_grid_kwh"] == 0.0 and abs(y["discharge_kwh"] - 4.0) < 1e-6
    g = by[(d0 - timedelta(days=2)).strftime("%Y-%m-%d")]
    assert abs(g["charge_grid_kwh"] - 2.0) < 1e-6
    # money: 4 kWh out × 0.30 − 6 kWh PV in × 0.08 − 2 kWh grid in × 0.30
    m = ins["money"]
    assert abs(m["net_eur"] - (4 * 0.30 - 6 * 0.08 - 2 * 0.30)) < 0.01
    assert m["discharged_kwh"] == 4.0
    # the hour-of-day rhythm carries the same kWh, spread over the 7-day window
    hp = {h["hour"]: h for h in ins["hour_profile"]}
    assert abs(hp[12]["charge_kwh"] * 7 - 6.0) < 0.01 and abs(hp[22]["discharge_kwh"] * 7 - 4.0) < 0.01


def test_measured_soc_rows_replace_the_integrated_curve_from_their_first_row():
    now = int(time.time())
    base = now - 6 * H
    ts = list(range(base, now, 60))
    samples = pd.DataFrame({"timestamp": pd.to_datetime(ts, unit="s"), "total_power": [1000.0] * len(ts)})
    # measured rows for the last 3 hours, a curve the integration could never produce (falling at +1 kW)
    soc_rows = [(t, 90.0 - (t - (now - 3 * H)) / (3 * H) * 30.0, 1000.0) for t in ts if t >= now - 3 * H]
    cfg = BatteryConfig(enabled=True, device_key="battery", capacity_kwh=10.0)
    status = get_battery_status(_ChainDB({}, soc_rows, samples), cfg)
    assert status.measured_since_ts == soc_rows[0][0]
    tl = dict(status.soc_timeline)
    assert abs(tl[soc_rows[-1][0]] - round(soc_rows[-1][1], 1)) < 0.11     # the measured value, verbatim
    # no step where the integrated head meets the first measured row
    before = [s for t, s in status.soc_timeline if t < status.measured_since_ts][-1]
    assert abs(before - 90.0) < 1.0
