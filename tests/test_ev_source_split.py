"""Where a charge's energy came from — and therefore what it cost.

Up to v16.77 every charged kWh was priced at the consumer tariff. On a house
with PV that is wrong by a factor of ten: a surplus charge buys almost nothing
from the grid. These tests pin the attribution down for each way a car can be
fed — sun, battery, grid, and every mixture — and pin down the two things that
must never happen: a charge nobody measured being *presented* as free solar,
and the three parts not adding up to the kWh the user sees.
"""
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.services.energy_balance import (
    SourceSplit, consumer_source_split,
)
from shelly_analyzer.services.ev_charging_log import (
    ChargingSession, group_sessions_into_charges, price_by_source,
)

BASE = 1_700_000_000
STEP = 60          # the sample spacing of a real Shelly EM in this app
HOUR = 3600


class FakeDb:
    """Serves constant-power series at ``STEP`` spacing, per device key."""

    def __init__(self, watts, spacing=STEP):
        self.watts = dict(watts)          # key -> W (a number) or (ts[], w[])
        self.spacing = spacing
        self.reads = []                   # every (key, cols) actually asked for

    def query_samples(self, device_key, start_ts=None, end_ts=None, columns=None):
        self.reads.append((device_key, tuple(columns or ())))
        v = self.watts.get(device_key)
        if v is None:
            return pd.DataFrame(columns=["timestamp", "total_power"])
        a, b = int(start_ts), int(end_ts)
        ts = np.arange(a - (a % self.spacing), b + self.spacing, self.spacing)
        if isinstance(v, tuple):
            src_ts, src_w = v
            idx = np.searchsorted(src_ts, ts, side="right") - 1
            ok = idx >= 0
            ts, w = ts[ok], np.asarray(src_w)[idx[ok]]
        else:
            w = np.full(ts.size, float(v))
        return pd.DataFrame({"timestamp": ts, "total_power": w})


class Cfg:
    """Minimal config with a grid meter plus a bridged PV/battery source."""

    def __init__(self, tenant_key=""):
        self.solar = type("S", (), {
            "grid_meter_device_key": "grid", "pv_meter_device_key": "",
            "pv_production_device_key": "pv",
            "feed_in_tariff_eur_per_kwh": 0.082, "feed_in_schedule": [],
        })()
        self.pv_source = type("P", (), {
            "enabled": True, "grid_power_entity": "", "mqtt_grid_power_topic": "",
            "pv_power_entity": "x", "mqtt_pv_power_topic": "",
            "battery_power_entity": "x", "mqtt_battery_power_topic": "",
        })()
        if tenant_key:
            td = type("T", (), {"name": "Tenant", "tenant_id": "t",
                                "device_keys": [tenant_key]})()
            self.tenant = type("TC", (), {"enabled": True, "tenants": [td]})()
        else:
            self.tenant = type("TC", (), {"enabled": False, "tenants": []})()


# Integration runs over the n-1 intervals *between* n samples, so an hour
# sampled every 60 s covers 3540 s — absolute kWh land 1.7 % low by
# construction. Fractions are unaffected; near() covers the absolute checks.
def near(got, want, rel=0.03):
    return abs(got - want) <= rel * abs(want) + 1e-9


def _split(watts, cfg=None, start=BASE, end=BASE + HOUR, tenant_key=""):
    db = FakeDb(watts)
    out = consumer_source_split(db, cfg or Cfg(tenant_key), [(start, end)],
                                load_key="wb")
    return out[0], db


# ── the four pure cases ─────────────────────────────────────────────────────

def test_pure_surplus_charge_is_all_solar():
    """Sun 11 kW, car 7 kW, house exports the rest: nothing bought."""
    sp, _ = _split({"wb": 7000, "grid": -4000, "pv": 11000, "battery": 0})
    fs, fb, fg = sp.fractions()
    assert round(fs, 3) == 1.0 and fb == 0.0 and fg == 0.0
    assert near(sp.load_kwh, 7.0), sp.load_kwh
    print("OK  pure surplus charge → 100 % solar")


def test_night_charge_is_all_grid():
    """No sun, battery idle, car 7 kW: every kWh is bought."""
    sp, _ = _split({"wb": 7000, "grid": 7400, "pv": 0, "battery": 0})
    fs, fb, fg = sp.fractions()
    assert fs == 0.0 and fb == 0.0 and round(fg, 3) == 1.0
    print("OK  night charge → 100 % grid")


def test_battery_fed_charge_is_all_battery():
    """No sun, no import: the house battery carries the car."""
    sp, _ = _split({"wb": 3000, "grid": 0, "pv": 0, "battery": -3400})
    fs, fb, fg = sp.fractions()
    assert fs == 0.0 and round(fb, 3) == 1.0 and fg == 0.0
    print("OK  battery-fed charge → 100 % battery")


def test_solar_and_battery_together():
    """Sun 4 kW (all consumed), battery adds 3 kW, no import: no grid share."""
    sp, _ = _split({"wb": 6000, "grid": 0, "pv": 4000, "battery": -3000})
    fs, fb, fg = sp.fractions()
    assert fg == 0.0
    # 4 kW direct PV against 3 kW discharge — the ratio the meters show.
    assert abs(fs - 4.0 / 7.0) < 0.01 and abs(fb - 3.0 / 7.0) < 0.01
    print("OK  solar + battery → split 4:3, no grid")


def test_partly_grid_when_the_sun_is_not_enough():
    """Sun 3 kW, house draws 9 kW, so 6 kW is imported: two thirds bought."""
    sp, _ = _split({"wb": 9000, "grid": 6000, "pv": 3000, "battery": 0})
    fs, fb, fg = sp.fractions()
    assert abs(fs - 1.0 / 3.0) < 0.01 and abs(fg - 2.0 / 3.0) < 0.01
    print("OK  partial surplus → 1/3 solar, 2/3 grid")


# ── the traps ───────────────────────────────────────────────────────────────

def test_pv_charging_the_battery_is_not_credited_to_the_car():
    """PV 10 kW of which 6 kW goes into the battery: only 4 kW reached loads.

    Without subtracting the battery charge, the car would be credited with
    solar the battery took — free energy counted twice.
    """
    sp, _ = _split({"wb": 4000, "grid": 0, "pv": 10000, "battery": 6000})
    fs, fb, fg = sp.fractions()
    assert round(fs, 3) == 1.0 and fb == 0.0
    assert near(sp.solar_kwh, 4.0), sp.solar_kwh
    print("OK  PV going into the battery is not credited to the car")


def test_exported_pv_is_not_credited_to_the_car():
    """PV 10 kW, 6 kW exported: the car may only claim the 4 kW that stayed."""
    sp, _ = _split({"wb": 4000, "grid": -6000, "pv": 10000, "battery": 0})
    assert near(sp.solar_kwh, 4.0), sp.solar_kwh
    print("OK  exported PV is not credited to the car")


def test_the_tenant_takes_its_slice_of_the_pool_first():
    """A grid-parallel tenant is served from the same bus and is not the owner's.

    Sun 8 kW reaches the loads, tenant draws 4 kW, car 4 kW: the car may claim
    only the half of the sun that was left for the owner.
    """
    cfg = Cfg(tenant_key="ten")
    sp, _ = _split({"wb": 4000, "grid": 0, "pv": 8000, "battery": 0, "ten": 4000},
                   cfg=cfg)
    assert near(sp.solar_kwh, 4.0), sp.solar_kwh
    assert round(sp.fractions()[0], 3) == 1.0
    print("OK  tenant load is deducted before the car's share")


def test_a_charge_bigger_than_the_owner_bus_is_capped_at_its_own_energy():
    """The consumer's share is capped at 1: it can never claim more than it drew."""
    sp, _ = _split({"wb": 20000, "grid": 0, "pv": 5000, "battery": 0})
    # Only 5 kW of supply is attributable; the car's own energy is 20 kWh, and
    # the parts are shares — the caller normalises them onto the measured kWh.
    assert sp.solar_kwh <= 5.05
    assert round(sp.fractions()[0], 3) == 1.0
    print("OK  a consumer never claims more supply than existed")


def test_a_window_the_meters_did_not_see_yields_no_split():
    """No grid samples in the window ⇒ coverage 0 ⇒ no attribution at all."""
    db = FakeDb({"wb": 7000})              # grid/pv/battery absent
    sp = consumer_source_split(db, Cfg(), [(BASE, BASE + HOUR)], load_key="wb")[0]
    assert sp.fractions() is None and sp.coverage == 0.0
    print("OK  an unmeasured window produces no split")


def test_no_supply_meter_configured_yields_nothing():
    """A grid-only home must fall straight through to flat pricing."""
    cfg = Cfg()
    cfg.solar.grid_meter_device_key = ""
    cfg.pv_source.enabled = False
    db = FakeDb({"wb": 7000, "grid": 100})
    out = consumer_source_split(db, cfg, [(BASE, BASE + HOUR)], load_key="wb")
    assert out[0].fractions() is None
    assert not db.reads, "it queried the database although nothing is configured"
    print("OK  grid-only home: no split, and no query at all")


def test_only_the_power_column_is_read():
    """The samples table has ~40 columns; SELECT * over a charge is 20× the data."""
    _, db = _split({"wb": 7000, "grid": -1000, "pv": 8000, "battery": 0})
    assert db.reads, "nothing was read"
    for key, cols in db.reads:
        assert cols == ("total_power",), f"{key} was read with {cols}"
    print(f"OK  all {len(db.reads)} reads restricted to total_power")


def test_a_data_gap_is_not_integrated_as_power():
    """A three-hour hole between two samples must not become three hours of load."""
    ts = np.array([BASE, BASE + 3 * HOUR], dtype="int64")
    db = FakeDb({"grid": -4000, "pv": 11000, "battery": 0})
    out = consumer_source_split(db, Cfg(), [(BASE, BASE + 3 * HOUR)],
                                load_ts=ts, load_w=np.array([7000.0, 7000.0]))
    # capped at the 600 s gap cap, not 3 h
    assert out[0].load_kwh <= 7000 * 600 / 3600 / 1000 + 0.01, out[0].load_kwh
    print("OK  a data gap is capped, not integrated")


# ── pricing ─────────────────────────────────────────────────────────────────

def _sess(kwh=10.0, price=0.30):
    return ChargingSession(session_id="s1", device_key="wb", start_ts=BASE,
                           end_ts=BASE + HOUR, energy_kwh=kwh, peak_power_w=7000,
                           avg_power_w=7000, cost_eur=round(kwh * price, 2))


def test_a_surplus_charge_costs_nothing():
    se = _sess()
    n = price_by_source([se], [SourceSplit(solar_kwh=10.0, window_s=HOUR,
                                           covered_s=HOUR)], 0.30)
    assert n == 1 and se.cost_eur == 0.0 and se.cost_model == "source"
    assert se.solar_kwh == 10.0 and se.grid_kwh == 0.0
    print("OK  a fully solar charge costs 0.00 €")


def test_a_grid_charge_keeps_the_full_price():
    se = _sess()
    price_by_source([se], [SourceSplit(grid_kwh=10.0, window_s=HOUR,
                                       covered_s=HOUR)], 0.30)
    assert se.cost_eur == 3.0 and se.grid_kwh == 10.0
    print("OK  a fully grid charge still costs the full tariff")


def test_the_three_parts_always_add_up_to_the_charge():
    """Rounding must never make solar + battery + grid miss the shown kWh."""
    for kwh in (0.37, 1.01, 9.05, 13.18, 43.55, 7.777):
        for fr in ((0.7, 0.21, 0.09), (1 / 3, 1 / 3, 1 / 3), (0.999, 0.0005, 0.0005)):
            se = _sess(kwh)
            sp = SourceSplit(solar_kwh=fr[0], battery_kwh=fr[1], grid_kwh=fr[2],
                             window_s=HOUR, covered_s=HOUR)
            price_by_source([se], [sp], 0.30)
            total = round(se.solar_kwh + se.battery_kwh + se.grid_kwh, 3)
            assert total == round(se.energy_kwh, 3), (kwh, fr, total)
    print("OK  the parts add up to the whole for every case tried")


def test_a_thin_window_is_not_sold_as_solar():
    """20 % coverage is a guess. It keeps the flat tariff and says 'fixed'."""
    se = _sess()
    n = price_by_source([se], [SourceSplit(solar_kwh=2.0, window_s=HOUR,
                                           covered_s=0.2 * HOUR)], 0.30)
    assert n == 0 and se.cost_model == "fixed" and se.cost_eur == 3.0
    assert se.solar_kwh == 0.0
    print("OK  a barely-measured window keeps the flat tariff")


def test_feed_in_valuation_charges_the_opportunity_cost():
    se = _sess()
    price_by_source([se], [SourceSplit(solar_kwh=10.0, window_s=HOUR,
                                       covered_s=HOUR)], 0.30,
                    solar_price_eur_per_kwh=0.082)
    assert se.cost_eur == 0.82
    print("OK  feed-in valuation charges the lost export revenue")


def test_the_feed_in_tariff_may_depend_on_the_charge_date():
    """A tariff that changed must price each charge with its own day's rate."""
    a, b = _sess(), _sess()
    b.start_ts = BASE + 400 * 86400
    price_by_source([a, b],
                    [SourceSplit(solar_kwh=10.0, window_s=HOUR, covered_s=HOUR)] * 2,
                    0.30,
                    solar_price_eur_per_kwh=lambda s: 0.10 if s.start_ts == BASE else 0.05)
    assert a.cost_eur == 1.0 and b.cost_eur == 0.5
    print("OK  a dated feed-in tariff is applied per charge")


def test_a_merged_charge_is_the_sum_of_its_parts():
    """Grouping must add the members up, never re-attribute the whole span —
    the pauses in between are exactly when the car drew nothing."""
    parts = []
    for i, (kwh, s, ba, g) in enumerate([(4.0, 4.0, 0.0, 0.0), (3.0, 1.0, 2.0, 0.0),
                                         (2.0, 0.0, 0.0, 2.0)]):
        se = ChargingSession(session_id=f"s{i}", device_key="wb",
                             start_ts=BASE + i * 5400, end_ts=BASE + i * 5400 + 1800,
                             energy_kwh=kwh, peak_power_w=7000, avg_power_w=6000,
                             cost_eur=round(g * 0.30, 2), cost_model="source",
                             solar_kwh=s, battery_kwh=ba, grid_kwh=g)
        parts.append(se)
    grp = group_sessions_into_charges(parts, max_gap_s=4 * HOUR)
    assert len(grp) == 1
    g0 = grp[0]
    assert g0.solar_kwh == 5.0 and g0.battery_kwh == 2.0 and g0.grid_kwh == 2.0
    assert round(g0.solar_kwh + g0.battery_kwh + g0.grid_kwh, 3) == g0.energy_kwh
    assert g0.cost_model == "source" and g0.cost_eur == 0.60
    print("OK  a merged charge sums its members exactly")


def test_grouping_without_pricing_stays_at_zero():
    """A flat-priced log must not grow phantom solar in its groups."""
    parts = [ChargingSession(session_id=f"s{i}", device_key="wb",
                             start_ts=BASE + i * 5400, end_ts=BASE + i * 5400 + 1800,
                             energy_kwh=4.0, peak_power_w=7000, avg_power_w=6000,
                             cost_eur=1.20) for i in range(2)]
    g0 = group_sessions_into_charges(parts, max_gap_s=4 * HOUR)[0]
    assert g0.solar_kwh == 0.0 and g0.cost_model == "fixed"
    print("OK  an unpriced log keeps zeros and 'fixed'")


# ── the promise that this is usable in every language ───────────────────────

def test_every_new_string_exists_in_all_nine_languages():
    """Including the two settings fields: a select whose options fall back to
    English is exactly how the settings page stayed English for seven of the
    nine languages before v16.76."""
    from shelly_analyzer.i18n import LANGS, t as _t
    keys = [
        "web.ev.src_solar", "web.ev.src_battery", "web.ev.src_grid",
        "web.ev.surplus_full", "web.ev.surplus_mostly", "web.ev.surplus_hint",
        "web.ev.source_title", "web.ev.source_sub", "web.ev.self_supplied",
        "web.ev.of_charged", "web.ev.saved_vs_grid", "web.ev.vs_all_grid",
        "web.ev.src_no_meter", "web.ev.src_no_data", "web.ev.src_error",
        "settings.field.ev_charging.cost_source_mode",
        "settings.hint.ev_charging.cost_source_mode",
        "settings.opts.ev_charging.cost_source_mode.auto",
        "settings.opts.ev_charging.cost_source_mode.split",
        "settings.opts.ev_charging.cost_source_mode.flat",
        "settings.field.ev_charging.solar_cost_model",
        "settings.hint.ev_charging.solar_cost_model",
        "settings.opts.ev_charging.solar_cost_model.free",
        "settings.opts.ev_charging.solar_cost_model.feed_in",
        "settings.opts.ev_charging.solar_cost_model.full",
    ]
    missing = [(l, k) for l in LANGS for k in keys if _t(l, k) == k]
    assert not missing, f"untranslated: {missing[:8]}"
    # Really translated, not the English text copied across.
    for k in ("web.ev.source_title", "settings.opts.ev_charging.cost_source_mode.auto"):
        for l in ("de", "fr", "ru"):
            assert _t(l, k) != _t("en", k), f"{l}/{k} is still the English string"
    # The argument slots must survive every translation, or the number is lost.
    for l in LANGS:
        assert "{n}" in _t(l, "web.ev.source_sub") and "{m}" in _t(l, "web.ev.source_sub"), l
        assert "{c}" in _t(l, "web.ev.vs_all_grid"), l
    print(f"OK  {len(keys)} strings in all {len(LANGS)} languages, placeholders intact")


def test_the_settings_page_offers_exactly_the_modes_the_config_accepts():
    """A select option the loader rejects would silently reset itself to auto."""
    import os as _os
    import re as _re
    from shelly_analyzer.io.config import EvChargingConfig
    p = _os.path.join(_os.path.dirname(__file__), "..", "src", "shelly_analyzer",
                      "web", "templates", "settings.html")
    html = open(p, encoding="utf-8").read()
    for field, allowed in (("cost_source_mode", {"auto", "split", "flat"}),
                           ("solar_cost_model", {"free", "feed_in", "full"})):
        line = [l for l in html.splitlines() if f'ev_charging.{field}"' in l]
        assert len(line) == 1, f"{field} appears {len(line)}× in the settings page"
        offered = set(_re.findall(r'\["(\w+)","', line[0]))
        assert offered == allowed, f"{field}: page offers {offered}, loader takes {allowed}"
    assert EvChargingConfig().cost_source_mode == "auto"
    assert EvChargingConfig().solar_cost_model == "free"
    print("OK  the settings page and the config loader agree on every option")


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items())
           if k.startswith("test_") and callable(v)]
    for f in fns:
        f()
    print(f"\n{len(fns)} Tests bestanden")
