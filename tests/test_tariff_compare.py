"""Tariff comparison (services/tariff_compare.py) — the money maths.

The comparison page tells a person whether switching to another tariff would
save money. Every rule here is a number that ends up in euros on that page, so
the cases use hand-computed values (not "whatever the code returns"):

* a fixed tariff is annualised from the measured hours (kWh x 8760 / hours);
* a time-of-use tariff charges the high rate only inside [start, end) — the
  boundary hour belongs to the low rate on the end side;
* a spot tariff adds the markup first and VAT on top of the sum;
* the household hours are summed per wall-clock hour across the primary meters
  (sub-meters and the grid meter must not be counted twice);
* the saving is "current minus candidate", and the cheapest option sorts first.
"""
import os
import sys
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.services import tariff_compare as tc


# -- the three simulators ---------------------------------------------------

def test_fixed_tariff_is_annualised_from_the_measured_hours():
    # 24 h at 1 kWh = 24 kWh -> 8760 kWh a year at 0.30 plus a 100 base fee.
    assert tc.simulate_fixed_tariff([1.0] * 24, 0.30, 100.0) == pytest.approx(8760 * 0.30 + 100.0)
    # 48 h with the same hourly use must give the same annual figure (not double).
    assert tc.simulate_fixed_tariff([1.0] * 48, 0.30, 100.0) == pytest.approx(8760 * 0.30 + 100.0)


def test_no_data_means_zero_not_a_phantom_base_fee():
    assert tc.simulate_fixed_tariff([], 0.30, 100.0) == 0.0
    assert tc.simulate_tou_tariff([], [], 0.35, 0.22, 6, 22, 100.0) == 0.0
    assert tc.simulate_spot_tariff([], [], 15.0, 100.0) == 0.0


def test_tou_boundary_hour_start_is_high_end_is_low():
    kwh = [1.0] * 24
    hours = list(range(24))
    annual = tc.simulate_tou_tariff(kwh, hours, ht_price=0.40, nt_price=0.20,
                                    ht_start=6, ht_end=22, base_fee_eur_per_year=0.0)
    # 16 high hours (6..21) and 8 low hours (0..5, 22, 23) per day.
    per_day = 16 * 0.40 + 8 * 0.20
    assert annual == pytest.approx(per_day * 365)
    # Only hour 6 — the first high hour — is charged high; hour 22 is already low.
    assert tc.simulate_tou_tariff([1.0], [6], 0.40, 0.20, 6, 22, 0.0) == pytest.approx(8760 * 0.40)
    assert tc.simulate_tou_tariff([1.0], [22], 0.40, 0.20, 6, 22, 0.0) == pytest.approx(8760 * 0.20)
    assert tc.simulate_tou_tariff([1.0], [5], 0.40, 0.20, 6, 22, 0.0) == pytest.approx(8760 * 0.20)


def test_spot_tariff_applies_vat_on_spot_plus_markup():
    # 10 ct spot + 5 ct markup = 15 ct net, 19 % VAT -> 17.85 ct per kWh.
    one_hour = tc.simulate_spot_tariff([1.0], [10.0], 5.0, 0.0, vat_rate=0.19)
    assert one_hour == pytest.approx(8760 * 0.1785)
    # VAT is part of the price, so a 0 % country pays exactly the net sum.
    assert tc.simulate_spot_tariff([1.0], [10.0], 5.0, 0.0, vat_rate=0.0) == pytest.approx(8760 * 0.15)
    # Negative spot prices reduce the bill (a real market situation).
    assert tc.simulate_spot_tariff([2.0], [-5.0], 0.0, 0.0, vat_rate=0.0) == pytest.approx(8760 * 2.0 * -0.05)


def test_spot_markup_added_before_vat_not_after():
    # (spot + markup) * 1.19 is NOT spot * 1.19 + markup.
    right = tc.simulate_spot_tariff([1.0], [10.0], 5.0, 0.0, 0.19) / 8760
    wrong = (10.0 * 1.19 + 5.0) / 100.0
    assert right != pytest.approx(wrong)


# -- country mapping ----------------------------------------------------------

@pytest.mark.parametrize("zone,country", [
    ("DE-LU", "DE"), ("DE_LU", "DE"), ("SE-4", "SE"), ("SE4", "SE"), ("NO2", "NO"),
    ("DK1", "DK"), ("US-CAL-CISO", "US"), ("AT", "AT"), ("de-lu", "DE"),
    ("  fr ", "FR"), ("", ""), (None, ""), ("12345", ""),
])
def test_country_from_bidding_zone(zone, country):
    assert tc._country_from_zone(zone) == country


def test_templates_fall_back_to_generic_for_unknown_country():
    generic = tc.get_tariff_templates_for_country("")
    assert generic == tc.GENERIC_EU_TARIFF_TEMPLATES
    assert tc.get_tariff_templates_for_country("ZZ") == tc.GENERIC_EU_TARIFF_TEMPLATES
    de = tc.get_tariff_templates_for_country("de")
    assert de == tc.TARIFF_TEMPLATES_BY_COUNTRY["DE"]
    # The caller may edit the list — it must be a copy, not the shared table.
    de.clear()
    assert tc.TARIFF_TEMPLATES_BY_COUNTRY["DE"]


def test_every_template_is_priced_by_a_known_type_with_its_fields():
    for country, items in list(tc.TARIFF_TEMPLATES_BY_COUNTRY.items()) + [("generic", tc.GENERIC_EU_TARIFF_TEMPLATES)]:
        for t in items:
            assert t["tariff_type"] in ("fixed", "tou", "spot"), (country, t["name"])
            assert t["base_fee_eur_per_year"] >= 0, (country, t["name"])
            if t["tariff_type"] == "fixed":
                assert t["price_eur_per_kwh"] > 0, (country, t["name"])
            if t["tariff_type"] == "tou":
                assert 0 <= t["ht_start"] < t["ht_end"] <= 24, (country, t["name"])
                assert t["ht_price"] >= t["nt_price"] > 0, (country, t["name"])


# -- the household aggregation + full comparison ------------------------------

class FakeDB:
    def __init__(self, hourly, spot=None):
        self.hourly = hourly      # device_key -> DataFrame(hour_ts, kwh)
        self.spot = spot

    def query_hourly(self, key, start, end):
        if key == "boom":
            raise RuntimeError("db down")
        return self.hourly.get(key)

    def query_spot_prices(self, zone, start, end):
        if self.spot is None:
            raise RuntimeError("no spot table")
        return self.spot


def _dev(key, kind="em", parent=""):
    return SimpleNamespace(key=key, kind=kind, parent=parent)


def _hours(n, kwh, first_ts=1_700_000_000 - 1_700_000_000 % 3600 - 3600 * 100):
    return pd.DataFrame({"hour_ts": [first_ts + i * 3600 for i in range(n)], "kwh": [kwh] * n})


def test_household_sums_per_hour_and_skips_submeters_grid_and_switches():
    cfg = SimpleNamespace(
        devices=[_dev("main"), _dev("sub", parent="main"), _dev("grid"), _dev("plug", kind="switch"),
                 _dev("second")],
        solar=SimpleNamespace(grid_meter_device_key="grid", pv_meter_device_key=""),
    )
    db = FakeDB({
        "main": _hours(3, 1.0), "second": _hours(3, 0.5), "sub": _hours(3, 9.0),
        "grid": _hours(3, 9.0), "plug": _hours(3, 9.0),
    })
    kwh, hts = tc._household_hourly(db, cfg, 0, 2_000_000_000)
    # main + second at the same wall-clock hour count once each: 1.5 kWh per hour.
    assert kwh == [pytest.approx(1.5)] * 3
    assert hts == sorted(hts) and len(hts) == 3


def test_household_ignores_negative_and_broken_rows_and_failing_meters():
    cfg = SimpleNamespace(devices=[_dev("a"), _dev("boom"), _dev("empty")], solar=None)
    df = pd.DataFrame({"hour_ts": [3600, 7200, 10800], "kwh": [1.0, -2.0, None]})
    db = FakeDB({"a": df, "empty": pd.DataFrame()})
    kwh, hts = tc._household_hourly(db, cfg, 0, 100000)
    assert kwh == [1.0] and hts == [3600]     # feed-in (negative) and NaN never count as use


def test_household_falls_back_to_all_meters_when_everything_is_a_submeter():
    cfg = SimpleNamespace(devices=[_dev("x", parent="y"), _dev("y", parent="x")], solar=None)
    db = FakeDB({"x": _hours(2, 1.0), "y": _hours(2, 1.0)})
    kwh, _ = tc._household_hourly(db, cfg, 0, 2_000_000_000)
    assert kwh == [pytest.approx(2.0)] * 2


def test_spot_hours_without_a_price_use_the_average_not_zero():
    # Two quarter-hour slots in hour 0 (40 and 60 EUR/MWh -> 4 and 6 ct -> 5 ct),
    # nothing for hour 1 -> falls back to the 5 ct average of what we do know.
    spot = pd.DataFrame({"slot_ts": [0, 900, 7200], "price_eur_mwh": [40.0, 60.0, 100.0]})
    got = tc._hourly_spot_ct(FakeDB({}, spot=spot), SimpleNamespace(spot_price=SimpleNamespace(bidding_zone="DE-LU")),
                             [0, 3600, 7200], 0, 10000)
    assert got[0] == pytest.approx(5.0)
    assert got[2] == pytest.approx(10.0)
    assert got[1] == pytest.approx((5.0 + 10.0) / 2)


def test_spot_lookup_survives_a_missing_price_table():
    got = tc._hourly_spot_ct(FakeDB({}, spot=None), SimpleNamespace(spot_price=SimpleNamespace(bidding_zone="DE-LU")),
                             [0, 3600], 0, 10000)
    assert got == [5.0, 5.0]


def _compare(monkeypatch, per_hour_kwh, hours=48, zone="DE-LU", spot=None, **kw):
    # Pin "now" so the 90-day window contains the generated hours.
    now = 1_760_000_000
    monkeypatch.setattr("time.time", lambda: now)
    first = (now // 3600) * 3600 - hours * 3600
    df = pd.DataFrame({"hour_ts": [first + i * 3600 for i in range(hours)], "kwh": [per_hour_kwh] * hours})
    cfg = SimpleNamespace(devices=[_dev("m")], solar=None,
                          spot_price=SimpleNamespace(bidding_zone=zone))
    return tc.compare_tariffs(FakeDB({"m": df}, spot=spot), cfg, **kw)


def test_compare_current_tariff_and_savings_sign(monkeypatch):
    res = _compare(monkeypatch, 0.5, current_price_eur_per_kwh=0.40, current_base_fee_eur_per_year=100.0)
    cur = [r for r in res if r.is_current]
    assert len(cur) == 1
    # 0.5 kWh x 8760 h = 4380 kWh a year at 0.40 + 100.
    assert cur[0].annual_cost_eur == pytest.approx(4380 * 0.40 + 100.0, abs=0.01)
    assert cur[0].monthly_avg_eur == pytest.approx(cur[0].annual_cost_eur / 12, abs=0.01)
    assert cur[0].effective_price_ct == pytest.approx(40.0)
    assert cur[0].savings_vs_current_eur == 0.0
    for r in res:
        assert r.savings_vs_current_eur == pytest.approx(cur[0].annual_cost_eur - r.annual_cost_eur, abs=0.02)
    # Cheapest first.
    costs = [r.annual_cost_eur for r in res]
    assert costs == sorted(costs)
    # A cheaper fixed template exists in the DE list (0.3199 + 143.88) and must show a saving
    # against 0.40/kWh; the 0.3485 one too.
    tmpl = next(t for t in tc.TARIFF_TEMPLATES_BY_COUNTRY["DE"]
                if t["tariff_type"] == "fixed" and t["price_eur_per_kwh"] == 0.3199)
    cheaper = next(r for r in res if r.name == tmpl["name"])
    assert cheaper.annual_cost_eur == pytest.approx(4380 * 0.3199 + 143.88, abs=0.01)
    assert cheaper.savings_vs_current_eur > 0


def test_compare_uses_the_country_of_the_configured_zone(monkeypatch):
    res_at = _compare(monkeypatch, 0.5, zone="AT")
    names = {r.name for r in res_at} - {"Current tariff"}
    assert names == {t["name"] for t in tc.TARIFF_TEMPLATES_BY_COUNTRY["AT"]}
    assert names != {t["name"] for t in tc.TARIFF_TEMPLATES_BY_COUNTRY["DE"]}
    res_unknown = _compare(monkeypatch, 0.5, zone="")
    assert {r.name for r in res_unknown} - {"Current tariff"} == {t["name"] for t in tc.GENERIC_EU_TARIFF_TEMPLATES}


def test_compare_tou_follows_local_clock_not_utc(monkeypatch):
    # 48 h of flat use: whatever the time zone, 16 of 24 hours are high rate.
    res = _compare(monkeypatch, 1.0, hours=48)
    tou = next(r for r in res if r.tariff_type == "tou")
    expected = 365 * (16 * 0.35 + 8 * 0.22) + 140.0
    assert tou.annual_cost_eur == pytest.approx(expected, abs=0.05)


def test_compare_spot_templates_use_the_stored_prices_and_vat(monkeypatch):
    now = 1_760_000_000
    first = (now // 3600) * 3600 - 48 * 3600
    spot = pd.DataFrame({"slot_ts": [first + i * 3600 for i in range(48)],
                         "price_eur_mwh": [100.0] * 48})        # 10 ct/kWh flat
    res = _compare(monkeypatch, 1.0, spot=spot, vat_rate=0.19)
    tmpl = next(t for t in tc.TARIFF_TEMPLATES_BY_COUNTRY["DE"]
                if t["tariff_type"] == "spot" and t["spot_markup_ct"] == 16.0)
    row = next(r for r in res if r.name == tmpl["name"])
    assert row.annual_cost_eur == pytest.approx(8760 * (10.0 + 16.0) * 1.19 / 100.0 + 107.88, abs=0.05)


def test_compare_without_any_data_returns_nothing(monkeypatch):
    monkeypatch.setattr("time.time", lambda: 1_760_000_000)
    cfg = SimpleNamespace(devices=[_dev("m")], solar=None, spot_price=SimpleNamespace(bidding_zone="DE-LU"))
    assert tc.compare_tariffs(FakeDB({"m": pd.DataFrame()}), cfg) == []
    assert tc._get_consumption_stats(FakeDB({"m": pd.DataFrame()}), cfg) is None


def test_consumption_stats_annualise_like_the_comparison(monkeypatch):
    now = 1_760_000_000
    monkeypatch.setattr("time.time", lambda: now)
    first = (now // 3600) * 3600 - 48 * 3600
    df = pd.DataFrame({"hour_ts": [first + i * 3600 for i in range(48)], "kwh": [0.25] * 48})
    cfg = SimpleNamespace(devices=[_dev("m")], solar=None, spot_price=SimpleNamespace(bidding_zone="DE-LU"))
    st = tc._get_consumption_stats(FakeDB({"m": df}), cfg)
    assert st["total_kwh"] == pytest.approx(12.0)
    assert st["hours"] == 48 and st["days"] == pytest.approx(2.0)
    assert st["annual_kwh"] == pytest.approx(0.25 * 8760)


def test_local_hour_of_day_matches_the_local_clock():
    # 2026-07-01 12:00 UTC is 14:00 on the local clock (summer time) — TOU must see 14, not 12.
    ts = int(datetime(2026, 7, 1, 12, tzinfo=ZoneInfo("UTC")).timestamp())
    assert datetime.fromtimestamp(ts, tz=tc._LOCAL_TZ).hour == 14
