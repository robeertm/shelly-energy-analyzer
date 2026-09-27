"""The balance a daily/monthly REPORT prints (v17.5).

Rule under test: a report may not sum every configured meter. A grid meter is
the supply, a PV series is generation, a battery series is storage, a meter
behind another meter is already inside it, and a tenant circuit is somebody
else's consumption. ``report_consumption()`` is the one place that knows this,
so the digest text, the digest PDF and the manual export cannot drift apart.

Two installations are modelled, both real shapes:

* **solar house** — grid meter, PV, battery, a tenant circuit, and a wallbox
  metered behind the house meter (``subtract_from_parent_display``). Every trap
  at once.
* **grid-only house** — two meters whose ``parent`` is a *main-meter id* rather
  than a device, no PV, no tenant. Here the old naive sum was already right, so
  the fix must not move a single number.

``_naive_total()`` reproduces the pre-17.5 report exactly (a signed sum over
every non-switch device) so each assertion is measured against the broken
state, not just against itself.
"""
import os
import sys
import types

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.services.energy_balance import (  # noqa: E402
    consumer_keys, device_role, report_consumption)

H = 3600
DAY = 24 * H
T0 = 1_700_000_000 - (1_700_000_000 % DAY)   # a midnight, stable across runs

UNIT = 0.30          # EUR/kWh consumer tariff
FEED_IN = 0.082      # EUR/kWh export


class _FakeDB:
    """Hourly rollups only — the single method the balance core reads."""

    def __init__(self, series):
        self.series = series

    def query_hourly(self, key, start_ts=None, end_ts=None, compensate=True):
        rows = [(h, k) for h, k in self.series.get(key, {}).items()
                if (start_ts is None or h >= start_ts) and (end_ts is None or h <= end_ts)]
        rows.sort()
        return pd.DataFrame(rows, columns=["hour_ts", "kwh"])

    def query_samples(self, *a, **k):
        return pd.DataFrame(columns=["ts", "power_w"])


def _dev(key, name=None, parent="", sub=False, kind="em"):
    return types.SimpleNamespace(key=key, name=name or key, parent=parent,
                                 subtract_from_parent_display=sub,
                                 deduct_from_parent=False, kind=kind, phases=3)


def _flat(total, hours=24, start=T0):
    """Spread ``total`` evenly over ``hours`` hourly buckets from ``start``."""
    per = total / float(hours)
    return {start + i * H: per for i in range(hours)}


# ── the solar house ─────────────────────────────────────────────────────────
# A physical day: the sun shines 08–16, so export and battery charging happen
# INSIDE those hours and the grid is only imported outside them. An unphysical
# series (exporting at midnight) makes the chain clamp and proves nothing.
#
#   grid    +6.0 imported (00–07, 18–23)   −3.0 exported (10–14)
#   pv      14.0 produced (08–16)
#   batt    +2.0 charged  (11–13)          −1.5 discharged (19–21)
#   haus    14.5 gross, wallbox 3.0 inside it → 11.5 net
#   tenant   2.0
#
#   load = 6 − 3 + 14 − 2 + 1.5 = 16.5 kWh
#   circuits = 11.5 + 3 + 2      = 16.5 kWh   → the two agree, as they should
_SOLAR_SERIES = {
    "grid": {**_flat(6.0 * 8 / 14.0, 8, T0), **_flat(-3.0, 5, T0 + 10 * H),
             **_flat(6.0 * 6 / 14.0, 6, T0 + 18 * H)},
    "pv": _flat(14.0, 9, T0 + 8 * H),
    "battery": {**_flat(2.0, 3, T0 + 11 * H), **_flat(-1.5, 3, T0 + 19 * H)},
    "haus": _flat(14.5),
    "wallbox": _flat(3.0),
    "ten": _flat(2.0),
}
_SOLAR_LOAD = 6.0 - 3.0 + 14.0 - 2.0 + 1.5          # 16.5
_SOLAR_NAIVE = 14.5 + 3.0 + 2.0 + (6.0 - 3.0) + 14.0 + (2.0 - 1.5)   # 37.0


def _cfg_solar():
    solar = types.SimpleNamespace(
        enabled=True, grid_meter_device_key="grid", pv_meter_device_key="",
        pv_production_device_key="pv", battery_device_key="battery",
        grid_display_device_key="", pv_embodied_g_per_kwh=40.0,
        battery_manufacturing_g_per_kwh=20.0, battery_embodied_g_per_kwh=60.0,
        battery_feeds_tenants=False, feed_in_tariff_eur_per_kwh=FEED_IN)
    return types.SimpleNamespace(
        solar=solar,
        pv_source=types.SimpleNamespace(enabled=False),
        tenant=types.SimpleNamespace(enabled=True, tenants=[
            types.SimpleNamespace(name="Tenant", tenant_id="t1", device_keys=["ten"])]),
        battery=types.SimpleNamespace(capacity_kwh=10.0, efficiency_pct=95.0,
                                      device_key="battery"),
        co2=types.SimpleNamespace(bidding_zone="DE_LU", enabled=True),
        pricing=types.SimpleNamespace(co2_intensity_g_per_kwh=380.0),
        devices=[_dev("haus", "House"), _dev("wallbox", "Wallbox", parent="haus", sub=True),
                 _dev("ten", "Tenant"), _dev("grid", "Grid"),
                 _dev("pv", "PV"), _dev("battery", "Battery"),
                 _dev("star", "Christmas star", parent="haus", kind="switch")],
    )


# ── the grid-only house ─────────────────────────────────────────────────────
# Two meters, both hanging off a MAIN METER called "settlement" — a name that is
# not a device key, so neither is behind the other and both must be summed.
_GRID_SERIES = {"haus": _flat(8.0), "wallbox": _flat(4.0)}


def _cfg_grid_only():
    solar = types.SimpleNamespace(
        enabled=False, grid_meter_device_key="", pv_meter_device_key="",
        pv_production_device_key="", battery_device_key="",
        grid_display_device_key="", feed_in_tariff_eur_per_kwh=FEED_IN)
    return types.SimpleNamespace(
        solar=solar,
        pv_source=types.SimpleNamespace(enabled=False),
        tenant=types.SimpleNamespace(enabled=False, tenants=[]),
        battery=types.SimpleNamespace(capacity_kwh=0.0, efficiency_pct=95.0, device_key=""),
        co2=types.SimpleNamespace(bidding_zone="DE_LU", enabled=False),
        pricing=types.SimpleNamespace(co2_intensity_g_per_kwh=380.0),
        devices=[_dev("haus", "House", parent="settlement"),
                 _dev("wallbox", "Wallbox", parent="settlement"),
                 _dev("boiler", "Boiler", parent="haus", kind="switch")],
    )


def _naive_total(db, cfg, start_ts, end_ts):
    """The pre-17.5 report total: signed sum over every non-switch device."""
    total = 0.0
    for d in cfg.devices:
        if str(getattr(d, "kind", "em")) == "switch":
            continue
        df = db.query_hourly(d.key, start_ts=start_ts, end_ts=end_ts)
        if df is not None and not df.empty:
            total += float(pd.to_numeric(df["kwh"], errors="coerce").fillna(0).sum())
    return total


def _rc(series, cfg):
    return report_consumption(_FakeDB(series), cfg, T0, T0 + DAY,
                              unit_price=UNIT, feed_in_tariff=FEED_IN)


# ═══════════════════════ the solar house ════════════════════════════════════

def test_solar_haus_total_ist_die_versorgungsbilanz():
    """The headline total is the supply identity, not a sum of meters."""
    rc = _rc(_SOLAR_SERIES, _cfg_solar())
    assert rc.basis == "supply"
    assert abs(rc.total_kwh - _SOLAR_LOAD) < 1e-6, rc.total_kwh


def test_solar_haus_der_alte_weg_lag_um_faktor_zwei_daneben():
    """Counter-test: the old sum more than doubles the real consumption."""
    db, cfg = _FakeDB(_SOLAR_SERIES), _cfg_solar()
    alt = _naive_total(db, cfg, T0, T0 + DAY)
    assert abs(alt - _SOLAR_NAIVE) < 1e-6, alt
    rc = _rc(_SOLAR_SERIES, cfg)
    assert alt > rc.total_kwh * 2, (alt, rc.total_kwh)


def test_solar_haus_wallbox_steckt_im_hauszaehler():
    """The house circuit is reported net of the wallbox metered behind it."""
    rc = _rc(_SOLAR_SERIES, _cfg_solar())
    assert abs(rc.kwh_by_key["haus"] - 11.5) < 1e-6, rc.kwh_by_key
    # The wallbox is still counted once — the parent gave it up.
    assert abs(rc.kwh_by_key["wallbox"] - 3.0) < 1e-6


def test_solar_haus_versorgungszaehler_sind_benannt_nicht_verschwiegen():
    """Grid/PV/battery are excluded WITH a reason, so a reader can check."""
    rc = _rc(_SOLAR_SERIES, _cfg_solar())
    assert rc.excluded.get("grid") == "grid"
    assert rc.excluded.get("pv") == "pv"
    assert rc.excluded.get("battery") == "battery"
    for k in ("grid", "pv", "battery"):
        assert k not in rc.keys


def test_solar_haus_mieter_getrennt_vom_eigner():
    rc = _rc(_SOLAR_SERIES, _cfg_solar())
    assert abs(rc.tenant_kwh - 2.0) < 1e-6
    assert abs(rc.owner_kwh - (_SOLAR_LOAD - 2.0)) < 1e-6
    assert abs(rc.tenant_billed_eur - 2.0 * UNIT) < 1e-6


def test_solar_haus_kosten_sind_die_netzposition():
    """Cost = import·tariff − export·feed-in, not consumption·tariff."""
    rc = _rc(_SOLAR_SERIES, _cfg_solar())
    erwartet = 6.0 * UNIT - 3.0 * FEED_IN
    assert abs(rc.total_cost - erwartet) < 1e-6, rc.total_cost
    # The old way charged full tariff on generation and on double counts.
    alt = _naive_total(_FakeDB(_SOLAR_SERIES), _cfg_solar(), T0, T0 + DAY) * UNIT
    assert alt > rc.total_cost * 4, (alt, rc.total_cost)


def test_solar_haus_stundenprofil_summiert_auf_die_gesamtmenge():
    """The profile and the headline figure may not disagree."""
    rc = _rc(_SOLAR_SERIES, _cfg_solar())
    assert rc.hours_total, "kein Stundenprofil"
    assert abs(sum(rc.hours_total.values()) - rc.total_kwh) < 0.05, sum(rc.hours_total.values())


def test_solar_haus_eigner_anteil_unter_eins():
    """With PV the owner does not pay the full tariff on every kWh."""
    rc = _rc(_SOLAR_SERIES, _cfg_solar())
    assert 0.0 <= rc.owner_share < 1.0, rc.owner_share
    haus = rc.cost_for("haus", rc.kwh_by_key["haus"])
    assert haus < rc.kwh_by_key["haus"] * UNIT
    # The tenant always pays full tariff, sun or no sun.
    assert abs(rc.cost_for("ten", 2.0) - 2.0 * UNIT) < 1e-9


# ═══════════════════════ the grid-only house ════════════════════════════════

def test_netz_haus_zahlen_bleiben_unveraendert():
    """A main-meter name is not a device: both meters still count."""
    db, cfg = _FakeDB(_GRID_SERIES), _cfg_grid_only()
    rc = _rc(_GRID_SERIES, cfg)
    alt = _naive_total(db, cfg, T0, T0 + DAY)
    assert abs(alt - 12.0) < 1e-6, alt
    assert rc.basis == "devices"
    assert abs(rc.total_kwh - alt) < 1e-6, (rc.total_kwh, alt)


def test_netz_haus_kosten_bleiben_menge_mal_preis():
    rc = _rc(_GRID_SERIES, _cfg_grid_only())
    assert abs(rc.owner_share - 1.0) < 1e-9
    assert abs(rc.total_cost - 12.0 * UNIT) < 1e-6, rc.total_cost


def test_netz_haus_nichts_wird_ausgeschlossen():
    rc = _rc(_GRID_SERIES, _cfg_grid_only())
    assert sorted(rc.keys) == ["haus", "wallbox"], rc.keys
    assert rc.excluded == {}, rc.excluded
    assert rc.tenant_kwh == 0.0


def test_netz_haus_ohne_bilanz_kein_erfundener_netzwert():
    """No grid meter → no invented import/export figures in the report."""
    rc = _rc(_GRID_SERIES, _cfg_grid_only())
    assert rc.balance is not None
    assert rc.balance.has_grid_meter is False
    assert rc.balance.has_pv is False
    assert rc.grid_cost_eur == 0.0
    assert rc.feed_in_revenue_eur == 0.0


# ═══════════════════════ shared guarantees ═════════════════════════════════

def test_schalter_zaehlen_nie_mit():
    """A switch sits inside its circuit — counting it would double it."""
    for series, cfg in ((_SOLAR_SERIES, _cfg_solar()), (_GRID_SERIES, _cfg_grid_only())):
        rc = _rc(series, cfg)
        assert not any(str(getattr(d, "kind", "em")) == "switch" and d.key in rc.keys
                       for d in cfg.devices)


def test_rollen_stimmen_mit_dem_bilanzkern_ueberein():
    """The report's exclusions must follow device_role, not a second guess."""
    cfg = _cfg_solar()
    assert device_role(cfg, "grid") == "grid"
    assert device_role(cfg, "pv") == "pv"
    assert device_role(cfg, "battery") == "battery"
    assert device_role(cfg, "ten") == "tenant"
    assert device_role(cfg, "haus") == "owner"
    assert set(consumer_keys(cfg)) == set(_rc(_SOLAR_SERIES, cfg).keys)


def test_as_dict_ist_vollstaendig_fuer_den_bericht():
    d = _rc(_SOLAR_SERIES, _cfg_solar()).as_dict()
    for feld in ("basis", "total_kwh", "total_cost", "owner_kwh", "tenant_kwh",
                 "grid_cost_eur", "feed_in_revenue_eur", "owner_share", "balance"):
        assert feld in d, feld
    assert d["balance"]["pv_production_kwh"] == 14.0
    assert d["balance"]["grid_import_kwh"] == 6.0
    assert d["balance"]["grid_export_kwh"] == 3.0
    assert d["balance"]["has_battery"] is True, "Shelly-gemessene Batterie muss zaehlen"
