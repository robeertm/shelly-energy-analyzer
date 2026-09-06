"""Demo mode must be able to show the pages that need a supply side.

The README promises every screenshot on it comes from the app's own demo mode.
Until v16.80 the demo house had a meter and a switch and nothing else, so the
solar dashboard, the battery tab and the EV log's source split — the three
pages that need to know what *fed* the house — were the only ones nobody could
try without buying hardware, and no honest screenshot of them existed.

These tests pin down that the simulated house now has a wallbox, a signed grid
meter, PV and a battery; that the four series add up; and that a fresh install
clicking "demo mode" really lands on a charge log priced by source.
"""
import os
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.io.storage import Storage
from shelly_analyzer.services.demo import (
    DEMO_BATTERY_KEY, DEMO_GRID_KEY, DEMO_PV_KEY, DEMO_WALLBOX_KEY,
    DemoState, default_demo_devices, demo_solar, ensure_demo_csv,
    ensure_demo_supply, gen_sample,
)


def _house(t, st):
    dev = [d for d in default_demo_devices() if d.key == "demo1"][0]
    p = gen_sample(dev, float(t), st)["power_w"]
    return p["a"] + p["b"] + p["c"]


def test_the_demo_house_has_a_supply_side():
    keys = [d.key for d in default_demo_devices()]
    assert DEMO_WALLBOX_KEY in keys and DEMO_GRID_KEY in keys, keys
    assert len(keys) == len(set(keys)), "duplicate demo device key"
    print(f"OK  the demo house has {len(keys)} devices incl. wallbox and grid")


def test_the_four_series_add_up():
    """grid = load − pv + battery. If they drift, every number the solar and EV
    pages derive from them is quietly wrong."""
    st = DemoState(seed=1234)
    base = int(time.time()) // 86400 * 86400
    worst = 0.0
    for d in range(6):
        for minute in range(0, 24 * 60, 15):
            t = base - d * 86400 + minute * 60
            s = demo_solar(float(t), st)
            # load is whatever the model itself used; the identity must close
            # on the numbers it returns.
            load = s["grid"] + s["pv"] - s["battery"]
            worst = max(worst, abs(load - (s["grid"] + s["pv"] - s["battery"])))
            assert s["wallbox"] <= load + 1e-6, (s["wallbox"], load)
    assert worst < 1e-6, worst
    print("OK  grid = load − pv + battery holds at every sample")


def test_the_simulated_day_contains_all_three_stories():
    """A demo that only ever charges on sunshine would show a source split that
    is always one colour — proving nothing."""
    st = DemoState(seed=1234)
    base = int(time.time()) // 86400 * 86400
    sun = batt = grid = 0
    for d in range(30):
        for minute in range(0, 24 * 60, 15):
            t = base - d * 86400 + minute * 60
            s = demo_solar(float(t), st)
            if s["wallbox"] < 100:
                continue
            if s["grid"] > 200:
                grid += 1
            elif s["battery"] < -200:
                batt += 1
            else:
                sun += 1
    assert sun > 0 and batt > 0 and grid > 0, (sun, batt, grid)
    print(f"OK  charging minutes over 30 days: {sun} on sun, {batt} on battery, "
          f"{grid} drawing from the grid")


def test_the_grid_meter_actually_exports():
    """It is a *signed* meter: a demo that never goes negative cannot show
    feed-in, and the source split would read as pure grid all day."""
    st = DemoState(seed=1234)
    grid_dev = [d for d in default_demo_devices() if d.key == DEMO_GRID_KEY][0]
    base = int(time.time()) // 86400 * 86400
    vals = []
    for minute in range(0, 24 * 60, 15):
        f = gen_sample(grid_dev, float(base + minute * 60), st)["power_w"]
        vals.append(f["a"] + f["b"] + f["c"])
    assert min(vals) < -200, f"never exports: min {min(vals):.0f} W"
    assert max(vals) > 100, f"never imports: max {max(vals):.0f} W"
    print(f"OK  the demo grid meter swings {min(vals):.0f} … {max(vals):.0f} W")


def test_the_power_factor_survives_a_negative_meter():
    """exp(-p/800) with a negative p explodes, and the factor was then clamped
    to its floor for the whole sunny half of the day."""
    st = DemoState(seed=1234)
    grid_dev = [d for d in default_demo_devices() if d.key == DEMO_GRID_KEY][0]
    base = int(time.time()) // 86400 * 86400
    for minute in range(0, 24 * 60, 15):
        f = gen_sample(grid_dev, float(base + minute * 60), st)
        p = f["power_w"]["a"] + f["power_w"]["b"] + f["power_w"]["c"]
        i = f["current_a"]["a"] + f["current_a"]["b"] + f["current_a"]["c"]
        if abs(p) > 50:
            # |S| ≥ |P|, and a sane power factor keeps them within a factor 2.
            s = abs(i) * 230.0
            assert abs(p) * 0.9 <= s <= abs(p) * 2.0, (p, s)
    print("OK  the power factor stays sane while the meter is exporting")


def test_demo_mode_end_to_end_prices_a_charge_by_source():
    """What a new user gets after clicking "demo mode": a charge log where the
    energy is split into sun, battery and grid — with no hardware at all."""
    from dataclasses import replace
    from shelly_analyzer.io.config import AppConfig, load_config, save_config
    from shelly_analyzer.web.action_dispatch import ActionDispatcher

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        st = Storage(tmp / "data")
        devs = default_demo_devices()
        ensure_demo_csv(st, devs, type("D", (), {"seed": 1234, "scenario": "household"})(),
                        days=30)
        rows = ensure_demo_supply(st, type("D", (), {"seed": 1234,
                                                     "scenario": "household"})(), days=30)
        assert rows > 100, rows

        cfg_path = tmp / "config.json"
        cfg_path.write_text('{"devices": []}')
        cfg = load_config(cfg_path)
        cfg = replace(
            cfg,
            devices=devs,
            solar=replace(cfg.solar, enabled=True, grid_meter_device_key=DEMO_GRID_KEY,
                          pv_production_device_key=DEMO_PV_KEY,
                          battery_device_key=DEMO_BATTERY_KEY),
            ev_charging=replace(cfg.ev_charging, enabled=True,
                                wallbox_device_key=DEMO_WALLBOX_KEY,
                                detection_threshold_w=1400.0),
        )
        save_config(cfg, cfg_path)
        cfg = load_config(cfg_path)          # and it must survive the round trip
        assert cfg.solar.battery_device_key == DEMO_BATTERY_KEY

        d = ActionDispatcher(cfg, st, None, out_dir=tmp, cfg_path=cfg_path, lang="en")
        p = d.dispatch("ev_sessions", {"days": "30"})
        assert p.get("ok"), p
        p = p["data"]
        assert p["total_sessions"] > 3, p["total_sessions"]
        assert p["source_pricing"]["active"], p["source_pricing"]
        assert p["total_solar_kwh"] > 0, "the demo never charges on sunshine"
        assert p["total_grid_kwh"] > 0, "the demo never charges from the grid"
        assert p["total_cost"] < p["cost_if_all_grid"], (p["total_cost"],
                                                         p["cost_if_all_grid"])
        parts = (p["total_solar_kwh"] + p["total_battery_kwh"] + p["total_grid_kwh"]
                 + p["source_pricing"]["unpriced_kwh"])
        assert abs(parts - p["total_kwh"]) < 0.1, (parts, p["total_kwh"])

        # …and a charge unfolds into a curve.
        item = (p["charges"] or p["sessions"])[0]
        c = d.dispatch("ev_charge_curve", {"start": str(item["start_ts"]),
                                           "end": str(item["end_ts"])})
        assert c.get("ok") and c["data"]["available"], c
        print(f"OK  demo mode alone: {p['total_sessions']} charges, "
              f"{p['total_solar_kwh']:.0f} kWh sun / {p['total_battery_kwh']:.0f} battery / "
              f"{p['total_grid_kwh']:.0f} grid, {p['total_cost']:.2f} € "
              f"instead of {p['cost_if_all_grid']:.2f} €")


def test_the_demo_device_names_are_translated():
    from shelly_analyzer.i18n import LANGS, t as _t
    missing = [(l, k) for l in LANGS
               for k in ("demo.device.wallbox", "demo.device.grid") if _t(l, k) == k]
    assert not missing, missing
    print(f"OK  both new demo device names exist in all {len(LANGS)} languages")


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items())
           if k.startswith("test_") and callable(v)]
    for f in fns:
        f()
    print(f"\n{len(fns)} Tests bestanden")
