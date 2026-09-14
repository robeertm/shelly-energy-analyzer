"""The charge-log link a car app pulls from — measured from both sides.

A link like this has two ways of being wrong, and only one of them is loud:

* it hands out too little — the car app finds nothing and the user shrugs;
* it hands out too much — an unfinished charge, a zero that reads as "no sun",
  or the charge log to anyone who asks. Nobody notices until the numbers are
  already wrong in the other program.

So every rule here is checked in both directions: the door opens for the right
key AND stays shut for every other one; a finished charge is offered AND a
running one is not; a measured split is reported AND an unmeasured one comes
back as "not measured" rather than as zero.
"""
import json
import os
import sys
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.io.config import load_config, save_config
from shelly_analyzer.io.storage import Storage
from shelly_analyzer.services import ev_link
from shelly_analyzer.web.action_dispatch import ActionDispatcher

STEP = 60
PRICE = 0.30

def pruefe(name, ist, soll):
    """One checked claim. Printed either way, failed loudly when wrong.

    The passes are printed too, deliberately: half of these say a rule must NOT
    fire, and in a silent suite a check that was accidentally deleted looks
    exactly like one that passed.
    """
    print(("  OK   " if ist == soll else "  FEHL ") + name + "   ist=%r soll=%r" % (ist, soll))
    assert ist == soll, "%s: ist=%r soll=%r" % (name, ist, soll)


def _house(day0: datetime, days: int, last_charge_ends_at=None):
    """A house with PV, battery, grid meter and a wallbox.

    Per day the car charges 12:00–14:00 on surplus and 03:00–04:00 off the grid.
    ``last_charge_ends_at`` appends one more charge that is still running at
    that moment — the case the settle window exists for.
    """
    rows = {k: [] for k in ("wallbox", "grid", "pv", "battery")}
    for d in range(days):
        t = day0 + timedelta(days=d)
        for minute in range(24 * 60):
            ts = t + timedelta(minutes=minute)
            h = ts.hour + ts.minute / 60.0
            pv = 11000.0 if 10 <= h < 16 else 0.0
            wb = 7000.0 if (12 <= h < 14 or 3 <= h < 4) else 0.0
            base = 500.0
            load = wb + base
            if pv > 0:
                batt, grid = 0.0, load - pv
            elif 3 <= h < 4:
                batt, grid = 0.0, load
            else:
                batt = -min(load, 2000.0)
                grid = load + batt
            for key, w in (("wallbox", wb), ("grid", grid), ("pv", pv), ("battery", batt)):
                rows[key].append((ts, w))
    if last_charge_ends_at is not None:
        t = last_charge_ends_at - timedelta(minutes=90)
        for minute in range(90):
            ts = t + timedelta(minutes=minute)
            for key, w in (("wallbox", 7000.0), ("grid", 7500.0), ("pv", 0.0), ("battery", 0.0)):
                rows[key].append((ts, w))
    return rows


def _build(tmp: Path, *, link_enabled=True, token="s3cret", settle=20,
           running_now=False, with_sources=True):
    st = Storage(tmp / "data")
    day0 = (datetime.now() - timedelta(days=5)).replace(hour=0, minute=0, second=0,
                                                        microsecond=0)
    ends = datetime.now() if running_now else None
    for key, pts in _house(day0, 4, last_charge_ends_at=ends).items():
        df = pd.DataFrame({"timestamp": [p[0] for p in pts],
                           "total_power": [p[1] for p in pts],
                           "energy_kwh": [abs(p[1]) * STEP / 3600.0 / 1000.0 for p in pts]})
        st.db.insert_dataframe(key, df)

    cfg_path = tmp / "config.json"
    solar = ({"enabled": True, "grid_meter_device_key": "grid",
              "pv_production_device_key": "pv", "battery_kwh": 10.0,
              "feed_in_tariff_eur_per_kwh": 0.08} if with_sources else {"enabled": False})
    pv_source = ({"enabled": True, "source_type": "homeassistant",
                  "pv_power_entity": "sensor.pv", "battery_power_entity": "sensor.bat"}
                 if with_sources else {"enabled": False})
    cfg_path.write_text(json.dumps({
        "devices": [{"key": "wallbox", "name": "Wallbox Garage", "host": "1.2.3.4", "kind": "em"},
                    {"key": "grid", "name": "Grid", "host": "1.2.3.5", "kind": "em"}],
        "pricing": {"electricity_price_eur_per_kwh": PRICE, "vat_enabled": False},
        "solar": solar,
        "pv_source": pv_source,
        "ev_charging": {"enabled": True, "wallbox_device_key": "wallbox",
                        "detection_threshold_w": 1500, "min_session_minutes": 5,
                        "group_sessions": True, "cost_source_mode": "auto",
                        "solar_cost_model": "free",
                        "link_enabled": link_enabled, "link_token": token,
                        "link_settle_minutes": settle},
    }))
    cfg = load_config(cfg_path)
    d = ActionDispatcher(cfg, st, None, out_dir=tmp, cfg_path=cfg_path, lang="en")
    return cfg, d


def test_01_the_key_opens_this_door_and_no_other():
    print("== The key opens this door and no other ==")
    with tempfile.TemporaryDirectory() as tmp:
        cfg, _ = _build(Path(tmp))
        pruefe("the configured key opens it", ev_link.token_ok(cfg, "s3cret"), True)
        pruefe("a wrong key does not", ev_link.token_ok(cfg, "s3crea"), False)
        pruefe("no key does not", ev_link.token_ok(cfg, ""), False)
        pruefe("None does not", ev_link.token_ok(cfg, None), False)
        # 🔴 The one that would have been silently open: a config with no token,
        # asked with no token. Both sides empty — a plain == would have said yes.
        cfg_empty, _ = _build(Path(tmp) / "b", token="")
        pruefe("empty config key is not opened by an empty header",
               ev_link.token_ok(cfg_empty, ""), False)
        pruefe("empty config key is not opened by anything",
               ev_link.token_ok(cfg_empty, "s3cret"), False)
        cfg_off, _ = _build(Path(tmp) / "c", link_enabled=False)
        pruefe("the right key with the link switched off stays shut",
               ev_link.token_ok(cfg_off, "s3cret"), False)


def test_02_only_a_charge_that_is_over():
    print("== Only a charge that is over ==")
    with tempfile.TemporaryDirectory() as tmp:
        cfg, d = _build(Path(tmp), running_now=True, settle=20)
        r = ev_link.handle("charges", {"days": "7"}, cfg, d.dispatch)
        assert r["ok"], r
        data = r["data"]
        now = int(time.time())
        latest = max((c["end_ts"] for c in data["charges"]), default=0)
        pruefe("nothing newer than the settle window is handed out",
               latest <= now - 20 * 60, True)
        pruefe("and the one held back is counted, not hidden",
               data["pending_settle"] >= 1, True)
        pruefe("finished charges are still there", len(data["charges"]) >= 6, True)
        # Same question twice: a reader polls, and must not be handed new ids for
        # charges it already filed.
        r2 = ev_link.handle("charges", {"days": "7"}, cfg, d.dispatch)
        ids1 = [c["id"] for c in data["charges"]]
        ids2 = [c["id"] for c in r2["data"]["charges"]]
        pruefe("ids are stable between two polls", ids1, ids2)
        pruefe("ids are unique", len(set(ids1)), len(ids1))
        pruefe("newest first", ids1 == [c["id"] for c in
                                        sorted(data["charges"], key=lambda c: -c["start_ts"])], True)


def test_03_what_the_numbers_say_and_what_they_refuse_to_say():
    print("== What the numbers say, and what they refuse to say ==")
    with tempfile.TemporaryDirectory() as tmp:
        cfg, d = _build(Path(tmp))
        data = ev_link.handle("charges", {"days": "7"}, cfg, d.dispatch)["data"]
        measured = [c for c in data["charges"] if c["cost_model"] == "source"]
        pruefe("the split is reported where it was measured", len(measured) > 0, True)
        ok_sum = all(abs((c["solar_kwh"] + c["battery_kwh"] + c["grid_kwh"])
                         - c["energy_kwh"]) < 0.002 for c in measured)
        pruefe("sun + battery + grid add up to the charge exactly", ok_sum, True)
        noon = [c for c in measured if datetime.fromtimestamp(c["start_ts"]).hour >= 10]
        night = [c for c in measured if datetime.fromtimestamp(c["start_ts"]).hour < 10]
        pruefe("the midday charge is almost all sun",
               all(c["grid_kwh"] < 0.2 * c["energy_kwh"] for c in noon) and bool(noon), True)
        pruefe("the 3 a.m. charge is almost all grid",
               all(c["grid_kwh"] > 0.9 * c["energy_kwh"] for c in night) and bool(night), True)
        pruefe("coverage is reported", all(c["coverage"] > 0.5 for c in measured), True)


def test_04_no_measurement_is_said_out_loud_not_passed_off_as_zero():
    print("== No measurement is said out loud, not passed off as zero ==")
    with tempfile.TemporaryDirectory() as tmp:
        cfg, d = _build(Path(tmp), with_sources=False)
        data = ev_link.handle("charges", {"days": "7"}, cfg, d.dispatch)["data"]
        pruefe("there are charges to report", len(data["charges"]) > 0, True)
        # 🔴 The trap: reporting solar_kwh=0.0 here would read in the other program
        # as "charged at night", when the truth is "nobody measured".
        pruefe("the split is null, never 0.0",
               all(c["solar_kwh"] is None and c["battery_kwh"] is None
                   and c["grid_kwh"] is None for c in data["charges"]), True)
        pruefe("and the cost model says why",
               all(c["cost_model"] == "fixed" for c in data["charges"]), True)
        pruefe("info admits there is no sun measurement", data["sources"]["solar"], False)


def test_05_since_the_poll_a_car_app_actually_makes():
    print("== since: the poll a car app actually makes ==")
    with tempfile.TemporaryDirectory() as tmp:
        cfg, d = _build(Path(tmp))
        alle = ev_link.handle("charges", {"days": "7"}, cfg, d.dispatch)["data"]["charges"]
        mitte = sorted(c["end_ts"] for c in alle)[len(alle) // 2]
        teil = ev_link.handle("charges", {"since": str(mitte)}, cfg, d.dispatch)["data"]["charges"]
        pruefe("since returns the newer half", all(c["end_ts"] >= mitte for c in teil), True)
        pruefe("and nothing that was already fetched", len(teil) < len(alle), True)
        pruefe("the ids are the same objects, not new ones",
               set(c["id"] for c in teil) <= set(c["id"] for c in alle), True)


def test_06_info_and_the_routes_that_do_not_exist():
    print("== info, and the routes that do not exist ==")
    with tempfile.TemporaryDirectory() as tmp:
        cfg, d = _build(Path(tmp))
        inf = ev_link.handle("info", {}, cfg, d.dispatch, version="16.85.0")["data"]
        pruefe("it names the wallbox it speaks for", inf["wallbox"]["device_key"], "wallbox")
        pruefe("with its display name", inf["wallbox"]["name"], "Wallbox Garage")
        pruefe("it reports the version", inf["version"], "16.85.0")
        pruefe("it reports the sources it has",
               (inf["sources"]["grid"], inf["sources"]["solar"], inf["sources"]["battery"]),
               (True, True, True))
        pruefe("an unknown route is an error, not an empty answer",
               ev_link.handle("charges/all/everything", {}, cfg, d.dispatch)["ok"], False)
        # The curve the car app draws is the tab's own payload, unreshaped.
        c0 = ev_link.handle("charges", {"days": "7"}, cfg, d.dispatch)["data"]["charges"][0]
        kurve = ev_link.handle("curve", {"start": str(c0["start_ts"]),
                                         "end": str(c0["end_ts"])}, cfg, d.dispatch)
        pruefe("the curve comes back", kurve["ok"], True)
        pruefe("with a load series", len(kurve["data"].get("load_w") or []) > 0, True)
        pruefe("and the three bands", all(k in kurve["data"]
                                          for k in ("solar_w", "battery_w", "grid_w")), True)
        pruefe("a nonsense window is refused",
               ev_link.handle("curve", {"start": "0", "end": "0"}, cfg, d.dispatch)["ok"], False)


def test_07_the_settings_survive_a_round_trip():
    print("== The settings survive a round trip ==")
    with tempfile.TemporaryDirectory() as tmp:
        cfg, _ = _build(Path(tmp), settle=45)
        save_config(cfg, Path(tmp) / "again.json")
        again = load_config(Path(tmp) / "again.json")
        pruefe("link_enabled", again.ev_charging.link_enabled, True)
        pruefe("link_token", again.ev_charging.link_token, "s3cret")
        pruefe("link_settle_minutes", again.ev_charging.link_settle_minutes, 45)
