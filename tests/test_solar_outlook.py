"""PV expectation: the roof calibrates itself on its own recent output against
the irradiance that fell on it; storage and grid follow from a simulation of
the house's typical day. No panel data needed."""
import os
import sys
import types
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.services import solar_forecast as sf  # noqa: E402
from shelly_analyzer.services.solar_overview import _seasonal_scale  # noqa: E402

TZ = ZoneInfo("Europe/Berlin")


class _DB:
    def __init__(self, series):
        self.series = series

    def query_hourly(self, key, start_ts=None, end_ts=None, compensate=True):
        rows = [(h, k) for h, k in self.series.get(key, {}).items()
                if (start_ts is None or h >= start_ts) and (end_ts is None or h <= end_ts)]
        return pd.DataFrame(rows, columns=["hour_ts", "kwh"])

    def query_co2_intensity(self, zone, start_ts, end_ts):
        return pd.DataFrame([], columns=["hour_ts", "zone", "intensity_g_per_kwh", "source", "fetched_at"])


def _cfg():
    return types.SimpleNamespace(
        solar=types.SimpleNamespace(enabled=True, grid_meter_device_key="grid", pv_meter_device_key="",
                                    pv_production_device_key="pv", battery_device_key="battery",
                                    grid_display_device_key="", pv_embodied_g_per_kwh=40.0,
                                    battery_manufacturing_g_per_kwh=20.0, battery_kwh=10.0,
                                    feed_in_tariff_eur_per_kwh=0.08,
                                    effective_feed_in_for_date=lambda d: 0.08),
        pv_source=None, tenant=types.SimpleNamespace(enabled=False, tenants=[]),
        battery=types.SimpleNamespace(capacity_kwh=10.0, efficiency_pct=95.0, max_charge_rate_kw=5.0, max_discharge_rate_kw=5.0),
        weather=types.SimpleNamespace(lat=51.0, lon=13.5),
        co2=types.SimpleNamespace(bidding_zone="DE_LU"),
        pricing=types.SimpleNamespace(co2_intensity_g_per_kwh=380.0, unit_price_gross=lambda: 0.30),
        devices=[],
    )


def test_calibrated_forecast_scales_with_irradiance(monkeypatch):
    now = datetime(2026, 6, 15, 12, 0, tzinfo=TZ)
    now_h = int(now.timestamp()) // 3600 * 3600
    # 14 days of history: PV = 0.01 kWh per W/m²·h (a ~10 kWp roof), a flat 600 W/m² noon
    irr, pv, grid, batt = {}, {}, {}, {}
    for d in range(-14, 8):
        for h in range(24):
            ts = now_h - 12 * 3600 + d * 86400 + h * 3600
            g = 600.0 if 9 <= h <= 15 else 0.0
            irr[ts] = (g if d < 0 else g * 0.5, 20.0, 30.0)   # forecast days: half the sun
            if d < 0:
                pv[ts] = g * 0.01
                grid[ts] = 0.0
                batt[ts] = 0.0
    monkeypatch.setattr(sf, "_fetch_irradiance", lambda *a, **k: irr)
    db = _DB({"pv": pv, "grid": grid, "battery": batt})
    r = sf.compute_solar_forecast(db, _cfg(), days=3, now=now)
    assert r["available"]
    assert abs(r["calibration"]["kwh_per_kwhm2"] - 10.0) < 0.5          # 0.01 kWh per W/m²·h = 10 kWh per kWh/m²
    full = [d for d in r["days"] if not d["partial"]]
    assert full and all(abs(d["pv_kwh"] - 7 * 600 * 0.5 * 0.01) < 0.6 for d in full)  # 7 sun hours × 300 W/m² × ratio


def test_seasonal_scale_does_not_promise_a_winter_from_a_summer():
    summer = {datetime(2026, 8, 1).date() + timedelta(days=i) for i in range(31)}
    s = _seasonal_scale(summer)
    assert 8.0 < s < 9.5           # August is 11.5 % of the year → ×8.7, not ×11.8 (365/31)
    year = {datetime(2026, 1, 1).date() + timedelta(days=i) for i in range(365)}
    assert abs(_seasonal_scale(year) - 1.0) < 1e-9
