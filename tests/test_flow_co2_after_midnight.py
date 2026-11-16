"""Just after midnight "today" has no consumption yet: CO2 is unknown, never 0 g/kWh (17.10.4).

The Energy-flow tab patches its numbers in place.  With a page left open over midnight
(or a first look before the day's first hour was synced) the intensity is ``None``, but the
CO2 tile wrote ``(intensity || 0)`` and showed "0.00 kg · 0 g/kWh".  Every clock here is fixed.
"""
import os
import re
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.dirname(__file__))

pytest.importorskip("pandas")
import test_plugs_only_site as T  # noqa: E402
from shelly_analyzer.services.energy_flow import compute_energy_flow  # noqa: E402

TZ = ZoneInfo("Europe/Berlin")
ROOT = Path(__file__).resolve().parent.parent


def _flow(hh, mm, lag_min):
    """Today at hh:mm on a fixed clock; the meters last synced ``lag_min`` minutes ago."""
    now = datetime(2026, 11, 10, hh, mm, tzinfo=TZ)
    w0 = int(datetime(2026, 11, 9, 20, 0, tzinfo=TZ).timestamp())
    n = int((now.timestamp() - w0) / 60) - lag_min
    with tempfile.TemporaryDirectory() as tmp:
        cfg, st, _ = T._build(tmp, [T._em("grid", 10), T._em("pv", 11), T._em("house", 12)],
                              {"solar": {"grid_meter_device_key": "grid", "pv_production_device_key": "pv"}})
        T._fill_meters(st, [("grid", 1000.0), ("pv", 0.0), ("house", 1000.0)], w0, minutes=n)
        return compute_energy_flow(st.db, cfg, "today", now=now)


def test_no_hour_yet_means_unknown_intensity_not_zero():
    h = _flow(0, 5, lag_min=30)["house"]          # nothing synced since 23:35
    assert h["load"] == 0 and h["intensity"] is None and h["autarky_pct"] is None


@pytest.mark.parametrize("hh,mm", [(0, 59), (1, 5), (12, 0)])
def test_with_measured_hours_the_intensity_is_the_real_one(hh, mm):
    h = _flow(hh, mm, lag_min=0)["house"]
    assert h["load"] > 0 and h["intensity"] == 380.0 and h["co2_kg"] > 0


def test_footprint_has_no_zero_intensity_without_load():
    from shelly_analyzer.services.energy_balance import Co2Breakdown
    empty = Co2Breakdown().as_dict()
    assert empty["effective_intensity"] is None and empty["autarky_pct"] is None and empty["solar_share_pct"] is None
    full = Co2Breakdown(load_kwh=10.0, property_kg=3.8, effective_intensity=380.0).as_dict()
    assert full["effective_intensity"] == 380.0


def test_the_page_never_writes_zero_for_an_unknown_intensity():
    src = (ROOT / "src/shelly_analyzer/services/webdash.py").read_text(encoding="utf-8")
    assert "(H.intensity || 0)" not in src
    assert "(F.effective_intensity||0)" not in src and "(c.effective_intensity || 0)" not in src
    assert "web.flow.co2_pending" in src
    i18n = (ROOT / "src/shelly_analyzer/i18n.py").read_text(encoding="utf-8")
    assert len(re.findall(r'"web\.flow\.co2_pending":', i18n)) == 9      # all nine languages
