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
           running_now=False, last_charge_ends_at=None, with_sources=True,
           erzeugung_ohne_zaehler=False):
    st = Storage(tmp / "data")
    day0 = (datetime.now() - timedelta(days=5)).replace(hour=0, minute=0, second=0,
                                                        microsecond=0)
    ends = datetime.now() if running_now else last_charge_ends_at
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
    if erzeugung_ohne_zaehler:
        # The third kind of house, and the one the "null, never 0.0" rule is
        # really about: it HAS a PV series, but nothing that could attribute a
        # charge to it. Calling that "all grid" would be a guess about the
        # weather; calling the no-generation house "all grid" is not.
        solar = {"enabled": True, "pv_production_device_key": "pv",
                 "battery_kwh": 10.0}
        pv_source = {"enabled": False}
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
        r = ev_link.handle("charges", {"days": "7"}, cfg, d.dispatch, presented_token="s3cret")
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
        r2 = ev_link.handle("charges", {"days": "7"}, cfg, d.dispatch, presented_token="s3cret")
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
        data = ev_link.handle("charges", {"days": "7"}, cfg, d.dispatch, presented_token="s3cret")["data"]
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
    # Two houses that both lack a split, and must NOT be answered the same way.
    with tempfile.TemporaryDirectory() as tmp:
        # (a) No PV, no battery anywhere. "No sun" is not a missing measurement
        #     here, it is the house — so the charge is grid, stated outright.
        cfg, d = _build(Path(tmp), with_sources=False)
        data = ev_link.handle("charges", {"days": "7"}, cfg, d.dispatch, presented_token="s3cret")["data"]
        pruefe("there are charges to report", len(data["charges"]) > 0, True)
        pruefe("the house says so in one word", data["source_split"], "grid_only")
        pruefe("every kWh is grid",
               all(abs(c["grid_kwh"] - c["energy_kwh"]) < 0.001
                   and c["solar_kwh"] == 0.0 and c["battery_kwh"] == 0.0
                   for c in data["charges"]), True)
        pruefe("and the cost model still says the price was a fixed one",
               all(c["cost_model"] == "fixed" for c in data["charges"]), True)
        pruefe("info admits there is no sun measurement", data["sources"]["solar"], False)

        # (b) Generation exists, but nothing attributes this window.
        # 🔴 The trap: reporting solar_kwh=0.0 HERE would read in the other
        # program as "charged at night", when the truth is "nobody measured".
        cfg2, d2 = _build(Path(tmp) / "pv-ohne-zaehler", erzeugung_ohne_zaehler=True)
        data2 = ev_link.handle("charges", {"days": "7"}, cfg2, d2.dispatch, presented_token="s3cret")["data"]
        pruefe("there are charges there too", len(data2["charges"]) > 0, True)
        pruefe("the house says THAT in a different word", data2["source_split"], "unknown")
        pruefe("the split is null, never 0.0",
               all(c["solar_kwh"] is None and c["battery_kwh"] is None
                   and c["grid_kwh"] is None for c in data2["charges"]), True)


def test_05_since_the_poll_a_car_app_actually_makes():
    print("== since: the poll a car app actually makes ==")
    with tempfile.TemporaryDirectory() as tmp:
        cfg, d = _build(Path(tmp))
        alle = ev_link.handle("charges", {"days": "7"}, cfg, d.dispatch, presented_token="s3cret")["data"]["charges"]
        mitte = sorted(c["end_ts"] for c in alle)[len(alle) // 2]
        teil = ev_link.handle("charges", {"since": str(mitte)}, cfg, d.dispatch, presented_token="s3cret")["data"]["charges"]
        # The overlap is the settle window plus an hour — anything older than
        # that was already handed out on the poll ``since`` names.
        floor = mitte - 20 * 60 - ev_link.SINCE_GRACE_S
        pruefe("since returns the newer half (plus the settle overlap)",
               all(c["end_ts"] >= floor for c in teil), True)
        pruefe("and nothing that was already fetched", len(teil) < len(alle), True)
        pruefe("the ids are the same objects, not new ones",
               set(c["id"] for c in teil) <= set(c["id"] for c in alle), True)


def test_05b_a_charge_that_was_settling_on_the_last_poll_is_not_lost():
    """The gap that swallowed a real charge (2026-09-15, 6.2 kWh, PV surplus).

    A reader polls every 30 min and asks "since my last poll". A charge that
    ended a minute before poll 1 is withheld there (still settling). Poll 2
    asks since=poll 1 — and the charge, now settled, ended BEFORE that. With
    ``end_ts < since`` as the filter it was never offered again: on two out of
    three charges the routine poll delivered nothing, only the backfill did.
    """
    print("== A charge that was settling on the last poll is not lost ==")
    import types
    with tempfile.TemporaryDirectory() as tmp:
        ende = datetime.now().replace(microsecond=0) - timedelta(minutes=1)
        cfg, d = _build(Path(tmp), settle=20, last_charge_ends_at=ende)
        echte_zeit = ev_link.time

        def _uhr(jetzt):
            # Only the clock moves; everything else the module asks ``time``
            # for stays the real thing.
            return types.SimpleNamespace(**{**{k: getattr(echte_zeit, k)
                                               for k in dir(echte_zeit)
                                               if not k.startswith('_')},
                                            'time': lambda: jetzt})
        # The clock of the test is the log's own: the fixture writes naive
        # local minutes and the log reads them back on its own terms, so the
        # charge's end is taken from what ``ev_sessions`` reports, not from
        # ``ende``. The polls then run on a stubbed clock around that end.
        roh = d.dispatch("ev_sessions", {"days": "7"})["data"]
        letzte = max(int(e["end_ts"]) for e in (roh.get("charges") or roh.get("sessions")))
        try:
            # Poll 1: the charge ended a minute ago — withheld, and said so.
            poll1 = letzte + 60
            ev_link.time = _uhr(poll1)
            r1 = ev_link.handle("charges", {"days": "7"}, cfg, d.dispatch, presented_token="s3cret")["data"]
            pruefe("poll 1 holds the fresh charge back", r1["pending_settle"], 1)
            pruefe("and does not hand it out",
                   any(c["end_ts"] >= letzte - 60 for c in r1["charges"]), False)
            # Poll 2, half an hour later, asking since poll 1.
            ev_link.time = _uhr(poll1 + 1800)
            r2 = ev_link.handle("charges", {"since": str(poll1)}, cfg, d.dispatch, presented_token="s3cret")["data"]
            frisch = [c for c in r2["charges"] if c["end_ts"] >= letzte - 120]
            pruefe("poll 2 hands the settled charge out", len(frisch), 1)
            pruefe("nothing is pending any more", r2["pending_settle"], 0)
            pruefe("it is the 90-minute charge that was running",
                   5000 <= frisch[0]["duration_s"] <= 5600, True)
            # And the charges poll 1 already delivered are not all repeated:
            # the overlap is the settle window plus an hour, not the whole log.
            pruefe("the overlap stays narrow", len(r2["charges"]) < len(r1["charges"]), True)
            # Hours later, a poll asking since a poll two hours after poll 2:
            # the charge is older than the overlap now and rightly not offered
            # a third time.
            poll_n = poll1 + 1800 + 7200
            ev_link.time = _uhr(poll_n + 1800)
            r3 = ev_link.handle("charges", {"since": str(poll_n)}, cfg, d.dispatch, presented_token="s3cret")["data"]
            pruefe("once out of the overlap it is not re-offered",
                   any(c["id"] == frisch[0]["id"] for c in r3["charges"]), False)
        finally:
            ev_link.time = echte_zeit


def test_06_info_and_the_routes_that_do_not_exist():
    print("== info, and the routes that do not exist ==")
    with tempfile.TemporaryDirectory() as tmp:
        cfg, d = _build(Path(tmp))
        inf = ev_link.handle("info", {}, cfg, d.dispatch, version="16.85.0", presented_token="s3cret")["data"]
        pruefe("it names the wallbox it speaks for", inf["wallbox"]["device_key"], "wallbox")
        pruefe("with its display name", inf["wallbox"]["name"], "Wallbox Garage")
        pruefe("it reports the version", inf["version"], "16.85.0")
        pruefe("it reports the sources it has",
               (inf["sources"]["grid"], inf["sources"]["solar"], inf["sources"]["battery"]),
               (True, True, True))
        pruefe("an unknown route is an error, not an empty answer",
               ev_link.handle("charges/all/everything", {}, cfg, d.dispatch, presented_token="s3cret")["ok"], False)
        # The curve the car app draws is the tab's own payload, unreshaped.
        c0 = ev_link.handle("charges", {"days": "7"}, cfg, d.dispatch, presented_token="s3cret")["data"]["charges"][0]
        kurve = ev_link.handle("curve", {"start": str(c0["start_ts"]),
                                         "end": str(c0["end_ts"])}, cfg, d.dispatch, presented_token="s3cret")
        pruefe("the curve comes back", kurve["ok"], True)
        pruefe("with a load series", len(kurve["data"].get("load_w") or []) > 0, True)
        pruefe("and the three bands", all(k in kurve["data"]
                                          for k in ("solar_w", "battery_w", "grid_w")), True)
        pruefe("a nonsense window is refused",
               ev_link.handle("curve", {"start": "0", "end": "0"}, cfg, d.dispatch, presented_token="s3cret")["ok"], False)


def test_07_the_settings_survive_a_round_trip():
    print("== The settings survive a round trip ==")
    with tempfile.TemporaryDirectory() as tmp:
        cfg, _ = _build(Path(tmp), settle=45)
        save_config(cfg, Path(tmp) / "again.json")
        again = load_config(Path(tmp) / "again.json")
        pruefe("link_enabled", again.ev_charging.link_enabled, True)
        pruefe("link_token", again.ev_charging.link_token, "s3cret")
        pruefe("link_settle_minutes", again.ev_charging.link_settle_minutes, 45)


def test_08_the_switch_and_the_key_close_the_door_by_themselves():
    print("== The switch and the key close the door by themselves ==")
    # 🔴 Found while rolling this out, not while writing it: this installation
    #    has no web token, so the auth guard is never registered and EVERY route
    #    is open. The link answered regardless of the switch — a switch that
    #    changes nothing is a promise the program does not keep. So the service
    #    asks for both itself, however the rest of the app is protected.
    with tempfile.TemporaryDirectory() as tmp:
        cfg, d = _build(Path(tmp))
        ok = ev_link.handle("info", {}, cfg, d.dispatch, presented_token="s3cret")
        pruefe("with switch and key: answers", ok["ok"], True)
        pruefe("without a key: refused",
               ev_link.handle("info", {}, cfg, d.dispatch)["ok"], False)
        pruefe("with the wrong key: refused",
               ev_link.handle("info", {}, cfg, d.dispatch,
                              presented_token="s3crea")["ok"], False)
        pruefe("and charges are refused too, not just info",
               ev_link.handle("charges", {"days": "7"}, cfg, d.dispatch)["ok"], False)

        cfg_off, d_off = _build(Path(tmp) / "off", link_enabled=False)
        r = ev_link.handle("info", {}, cfg_off, d_off.dispatch, presented_token="s3cret")
        pruefe("switch off, right key: still refused", r["ok"], False)
        pruefe("and it says WHY, so nobody hunts the network",
               "switched off" in r["error"], True)


def test_09_the_curve_no_longer_hangs_on_the_solar_equipment():
    print("== The curve no longer hangs on the solar equipment ==")
    # Robert, who has a wallbox and nothing else: "auch nur netzlader … dann ist
    # die ganze kurve halt rot, aber ich habe den exakten ladeverlauf". Before,
    # a house without a grid meter or without PV got NO curve at all — although
    # the wallbox meter knows every charge minute by minute. Only the COLOURING
    # needs the supply meters.
    with tempfile.TemporaryDirectory() as tmp:
        def _kurve(cfg, d):
            c0 = ev_link.handle("charges", {"days": "7"}, cfg, d.dispatch,
                                presented_token="s3cret")["data"]["charges"][0]
            return ev_link.handle("curve", {"start": str(c0["start_ts"]),
                                            "end": str(c0["end_ts"])},
                                  cfg, d.dispatch, presented_token="s3cret")["data"]

        # (a) A house with nothing but the wallbox: one red band, and it must
        #     meet the measured load curve everywhere.
        cfg, d = _build(Path(tmp), with_sources=False)
        k = _kurve(cfg, d)
        pruefe("there is a curve at all", k["available"], True)
        pruefe("and it says what its bands mean", k["split"], "grid_only")
        pruefe("the load series is there", len(k["load_w"]) > 2, True)
        pruefe("grid IS the load curve",
               all(abs(g - l) < 0.05 for g, l in zip(k["grid_w"], k["load_w"])), True)
        pruefe("sun and battery stay empty",
               (max(k["solar_w"]), max(k["battery_w"])), (0.0, 0.0))
        pruefe("and none of it is marked unmeasured",
               all(k["measured"]), True)
        pruefe("the grid ran as long as the charge did",
               abs(k["seconds"]["grid"] - k["seconds"]["total"]) <= 120, True)

        # (b) Generation without attribution: the course, and no colours.
        #     Painting this red would be a guess about the weather.
        cfg2, d2 = _build(Path(tmp) / "pv-ohne-zaehler", erzeugung_ohne_zaehler=True)
        k2 = _kurve(cfg2, d2)
        pruefe("the course is handed out here too", len(k2["load_w"]) > 2, True)
        pruefe("but it is not called grid", k2["split"], "unknown")
        pruefe("no band claims anything",
               (max(k2["grid_w"]), max(k2["solar_w"]), max(k2["battery_w"])),
               (0.0, 0.0, 0.0))
        pruefe("and every point is marked unmeasured",
               any(k2["measured"]), False)

        # (c) The full house is untouched by all of this.
        cfg3, d3 = _build(Path(tmp) / "voll")
        k3 = _kurve(cfg3, d3)
        pruefe("a metered house still gets the real split", k3["split"], "measured")
        pruefe("and its bands are not all grid", max(k3["solar_w"]) > 0.0, True)
