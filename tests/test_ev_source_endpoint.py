"""The whole EV-Log endpoint, on a real database.

The unit tests pin the physics down; this one pins the *wiring* down — config
round-trip, the reuse of the already-read wallbox frame, the response cache's
freshness token, and the payload the browser actually receives. It builds a
small house in SQLite: sun in the afternoon, a battery, a grid meter and a
wallbox that charges twice — once at noon on surplus, once at three in the
morning off the grid.
"""
import json
import os
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.io.config import load_config, save_config
from shelly_analyzer.io.storage import Storage
from shelly_analyzer.web.action_dispatch import ActionDispatcher

STEP = 60
PRICE = 0.30


def _house(day0: datetime, days: int):
    """Power series for a house with PV, battery, grid meter and a wallbox.

    Per day: the car charges 12:00–14:00 while the sun gives 11 kW (surplus),
    and again 03:00–04:00 in the dark (grid). Base load 500 W throughout.
    """
    rows = {k: [] for k in ("wallbox", "grid", "pv", "battery")}
    for d in range(days):
        t = day0 + timedelta(days=d)
        for minute in range(24 * 60):
            ts = t + timedelta(minutes=minute)
            h = ts.hour + ts.minute / 60.0
            pv = 11000.0 if 10 <= h < 16 else 0.0
            wb = 7000.0 if 12 <= h < 14 else (7000.0 if 3 <= h < 4 else 0.0)
            base = 500.0
            load = wb + base
            if pv > 0:
                batt = 0.0
                grid = load - pv           # negative ⇒ exporting
            elif 3 <= h < 4:
                batt = 0.0                 # battery empty at 3 a.m.
                grid = load                # everything bought
            else:
                batt = -min(load, 2000.0)  # battery covers the evening base load
                grid = load + batt
            for key, w in (("wallbox", wb), ("grid", grid), ("pv", pv), ("battery", batt)):
                rows[key].append((ts, w))
    return rows


def _build(tmp: Path, mode="auto", solar_model="free"):
    st = Storage(tmp / "data")
    day0 = (datetime.now() - timedelta(days=4)).replace(hour=0, minute=0, second=0,
                                                        microsecond=0)
    for key, pts in _house(day0, 4).items():
        df = pd.DataFrame({"timestamp": [p[0] for p in pts],
                           "total_power": [p[1] for p in pts],
                           "energy_kwh": [abs(p[1]) * STEP / 3600.0 / 1000.0 for p in pts]})
        st.db.insert_dataframe(key, df)

    cfg_path = tmp / "config.json"
    cfg_path.write_text(json.dumps({
        "devices": [{"key": "wallbox", "name": "Wallbox", "host": "1.2.3.4", "kind": "em"},
                    {"key": "grid", "name": "Grid", "host": "1.2.3.5", "kind": "em"}],
        "pricing": {"electricity_price_eur_per_kwh": PRICE, "vat_enabled": False},
        "solar": {"enabled": True, "grid_meter_device_key": "grid",
                  "pv_production_device_key": "pv", "battery_kwh": 10.0,
                  "feed_in_tariff_eur_per_kwh": 0.08},
        "pv_source": {"enabled": True, "source_type": "homeassistant",
                      "pv_power_entity": "sensor.pv", "battery_power_entity": "sensor.bat"},
        "ev_charging": {"enabled": True, "wallbox_device_key": "wallbox",
                        "detection_threshold_w": 1500, "min_session_minutes": 5,
                        "group_sessions": False,
                        "cost_source_mode": mode, "solar_cost_model": solar_model},
    }))
    cfg = load_config(cfg_path)
    d = ActionDispatcher(cfg, st, None, out_dir=tmp, cfg_path=cfg_path, lang="en")
    return d


def _sessions(d, days=7):
    r = d.dispatch("ev_sessions", {"days": str(days)})
    assert r.get("ok"), r
    return r["data"]


def test_the_config_survives_a_round_trip():
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        d = _build(tmp, mode="split", solar_model="feed_in")
        assert d.cfg.ev_charging.cost_source_mode == "split"
        assert d.cfg.ev_charging.solar_cost_model == "feed_in"
        save_config(d.cfg, tmp / "again.json")
        again = load_config(tmp / "again.json")
        assert again.ev_charging.cost_source_mode == "split"
        assert again.ev_charging.solar_cost_model == "feed_in"
        print("OK  the two new settings load, save and load again")


def test_a_nonsense_mode_falls_back_to_auto():
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        (Path(tmp) / "c.json").write_text(json.dumps(
            {"devices": [],
             "ev_charging": {"cost_source_mode": "sunshine", "solar_cost_model": "🎉"}}))
        cfg = load_config(Path(tmp) / "c.json")
        assert cfg.ev_charging.cost_source_mode == "auto"
        assert cfg.ev_charging.solar_cost_model == "free"
        print("OK  an unknown mode falls back instead of breaking startup")


def test_the_noon_charge_is_free_and_the_night_charge_is_not():
    with tempfile.TemporaryDirectory() as tmp:
        d = _build(Path(tmp))
        data = _sessions(d)
        assert data["source_pricing"]["active"], data["source_pricing"]
        noon, night = [], []
        for s in data["sessions"]:
            (noon if datetime.fromtimestamp(s["start_ts"]).hour >= 10 else night).append(s)
        assert noon and night, (len(noon), len(night))
        for s in noon:
            assert s["cost_model"] == "source"
            assert s["grid_kwh"] < 0.2 * s["energy_kwh"], s
            assert s["cost_eur"] <= 0.5, s
        for s in night:
            assert s["cost_model"] == "source"
            assert s["grid_kwh"] > 0.9 * s["energy_kwh"], s
            assert abs(s["cost_eur"] - s["energy_kwh"] * PRICE) < 0.1, s
        print(f"OK  {len(noon)} surplus charges free, {len(night)} night charges billed")


def test_the_totals_add_up_to_the_energy():
    with tempfile.TemporaryDirectory() as tmp:
        d = _build(Path(tmp))
        data = _sessions(d)
        parts = (data["total_solar_kwh"] + data["total_battery_kwh"] + data["total_grid_kwh"])
        assert abs(parts - data["total_kwh"]) < 0.05, (parts, data["total_kwh"])
        assert data["cost_if_all_grid"] > data["total_cost"] > 0
        for s in data["sessions"]:
            tot = s["solar_kwh"] + s["battery_kwh"] + s["grid_kwh"]
            assert abs(tot - s["energy_kwh"]) < 0.002, s
        print("OK  every session and the totals add up to the shown kWh")


def test_flat_mode_reproduces_the_old_numbers():
    with tempfile.TemporaryDirectory() as tmp:
        d = _build(Path(tmp), mode="flat")
        data = _sessions(d)
        assert not data["source_pricing"]["active"]
        assert data["source_pricing"]["reason"] == "off"
        for s in data["sessions"]:
            assert s["cost_model"] == "fixed"
            assert abs(s["cost_eur"] - s["energy_kwh"] * PRICE) < 0.01, s
            assert s["solar_kwh"] == 0.0
        assert abs(data["total_cost"] - data["total_kwh"] * PRICE) < 0.1
        print("OK  'off' gives exactly the pre-16.78 numbers")


def test_the_feed_in_model_charges_the_opportunity_cost():
    with tempfile.TemporaryDirectory() as tmp:
        d = _build(Path(tmp), solar_model="feed_in")
        data = _sessions(d)
        noon = [s for s in data["sessions"]
                if datetime.fromtimestamp(s["start_ts"]).hour >= 10]
        assert noon
        for s in noon:
            want = s["grid_kwh"] * PRICE + (s["solar_kwh"] + s["battery_kwh"]) * 0.08
            assert abs(s["cost_eur"] - want) < 0.02, (s, want)
        print("OK  the feed-in model prices solar at the export tariff")


def test_changing_the_setting_invalidates_the_cached_answer():
    """The response cache keyed only on data would keep serving flat prices."""
    with tempfile.TemporaryDirectory() as tmp:
        d = _build(Path(tmp))
        first = _sessions(d)
        assert first["source_pricing"]["active"]
        object.__setattr__(d.cfg.ev_charging, "cost_source_mode", "flat")
        second = _sessions(d)
        assert not second["source_pricing"]["active"], "served a stale cached payload"
        assert second["total_cost"] > first["total_cost"]
        print("OK  flipping the setting is visible immediately, not after the TTL")


def test_a_home_without_a_pv_series_keeps_flat_pricing():
    with tempfile.TemporaryDirectory() as tmp:
        d = _build(Path(tmp))
        object.__setattr__(d.cfg.solar, "pv_production_device_key", "")
        object.__setattr__(d.cfg.pv_source, "pv_power_entity", "")
        object.__setattr__(d.cfg.pv_source, "battery_power_entity", "")
        data = _sessions(d)
        assert not data["source_pricing"]["active"]
        assert data["source_pricing"]["reason"] == "no_meter"
        for s in data["sessions"]:
            assert s["cost_model"] == "fixed"
        print("OK  no PV/battery series ⇒ flat pricing and an honest reason")


def test_the_split_does_not_blow_up_the_endpoint():
    """It must stay a fraction of the endpoint, not double it."""
    import time
    with tempfile.TemporaryDirectory() as tmp:
        d = _build(Path(tmp))
        t0 = time.time(); _sessions(d); warm = time.time() - t0
        object.__setattr__(d.cfg.ev_charging, "cost_source_mode", "flat")
        t0 = time.time(); _sessions(d); flat = time.time() - t0
        object.__setattr__(d.cfg.ev_charging, "cost_source_mode", "auto")
        t0 = time.time(); _sessions(d); split = time.time() - t0
        print(f"OK  endpoint {flat*1000:.0f} ms flat → {split*1000:.0f} ms with the split "
              f"(first run {warm*1000:.0f} ms)")
        assert split < max(1.0, flat * 4 + 0.3), (flat, split)


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items())
           if k.startswith("test_") and callable(v)]
    for f in fns:
        f()
    print(f"\n{len(fns)} Tests bestanden")
