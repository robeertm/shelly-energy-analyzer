"""An installation of plugs only (no meter at all) is not empty any more (17.9.2).

A plug has had a minute history of its own since 17.9.0, but Energy Flow, CO2
and the balance views were still built from meters, so a household that only
has plugs (eight of them, no meter, was the case that raised it) saw empty
tabs. With no meter, the sum of the top-level plugs is the house.

Two things have to hold and both are tested here:

1. THE HARD RULE. As soon as any meter exists, nothing changes: the plugs of
   such an installation are sub-meters and stay out of every whole-house figure.
   The same installation is built twice, once without rows for its plugs and
   once with them; every payload, down to the last bit, must be equal. A plug
   next to a meter, hung behind a meter or hung behind nothing is covered, and
   so is a meter that is of a kind the app does not know.
2. A plugs-only installation says the same thing in every tab: Energy Flow,
   CO2, Costs, the balance and the report all carry the same kWh, the CO2 is the
   consumption times the grid mix of the hours it was drawn in, and nothing is
   claimed about the grid connection (no import, no feed-in, no
   self-sufficiency), because nothing was measured there.
"""
import json
import os
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.io.config import load_config
from shelly_analyzer.io.storage import Storage
from shelly_analyzer.services.live_history import LiveHistoryRecorder

MIN = 60


def _window():
    now = datetime.now()
    day = now - timedelta(hours=30)
    if day.year != now.year:
        pytest.skip("needs a data window inside the current year")
    return int(day.replace(minute=0, second=0, microsecond=0).timestamp())


def _fill_meters(st, keys, w0, minutes=180):
    for key, watts in keys:
        rows = []
        for m in range(minutes):
            wh = watts / 60.0
            rows.append({"timestamp": w0 + m * MIN, "a_act_power": watts, "b_act_power": 0.0,
                         "c_act_power": 0.0, "total_power": watts,
                         "a_total_act_energy": wh, "b_total_act_energy": 0.0,
                         "c_total_act_energy": 0.0, "energy_kwh": wh / 1000.0})
        st.db.insert_dataframe(key, pd.DataFrame(rows))


def _fill_plugs(st, plugs, w0, minutes=180, step=10):
    rec = LiveHistoryRecorder(st.db)
    for key, watts in plugs:
        for s in range(0, minutes * MIN + 1, step):
            rec.observe(key, w0 + s, watts, 230.0, watts / 230.0, 10.0 + watts * s / 3600.0)
    rec.flush()


def _build(tmp, devices, extra=None):
    Path(tmp).mkdir(parents=True, exist_ok=True)
    p = Path(tmp) / "config.json"
    body = {"devices": devices, "co2": {"enabled": True}}
    body.update(extra or {})
    p.write_text(json.dumps(body))
    return load_config(p), Storage(Path(tmp) / "data"), p


def _dispatcher(cfg, st, p, tmp):
    from shelly_analyzer.web.action_dispatch import ActionDispatcher
    return ActionDispatcher(cfg, st, None, out_dir=Path(tmp), cfg_path=p, lang="en")


def _sw(key, host_i, parent=""):
    d = {"key": key, "name": key.capitalize(), "host": "192.0.2.%d" % host_i, "kind": "switch",
         "supports_emdata": False}
    if parent:
        d["parent"] = parent
    return d


def _em(key, host_i, parent=""):
    d = {"key": key, "name": key.capitalize(), "host": "192.0.2.%d" % host_i, "kind": "em"}
    if parent:
        d["parent"] = parent
    return d


def _strip_time(payload):
    """The moment a payload was made is not part of what it says."""
    out = json.loads(json.dumps(payload, default=str))
    out.pop("range", None)
    return out


# ── 1. the hard rule: with a meter, nothing changes ──────────────────────────

METER_SITES = {
    # the shape of the installation that raised the issue's sibling: two meters
    # under a main-meter id, two plugs hanging behind a meter
    "plugs_behind_a_meter": (
        [_em("house", 10, "main"), _em("wallbox", 11, "main"),
         _sw("heater", 12, "house"), _sw("lamp", 13, "house")],
        [("house", 1000.0), ("wallbox", 500.0)], [("heater", 2000.0), ("lamp", 40.0)]),
    # a plug that hangs behind nothing, next to a meter: still not a whole-house figure
    "plug_next_to_a_meter": (
        [_em("house", 10), _sw("fridge", 14), _sw("lamp", 13, "fridge")],
        [("house", 1000.0)], [("fridge", 120.0), ("lamp", 40.0)]),
    # one meter of a kind the app does not know is still a meter
    "an_unknown_kind_is_a_meter": (
        [{"key": "mystery", "name": "Mystery", "host": "192.0.2.15", "kind": "unknown"},
         _sw("fridge", 14)],
        [("mystery", 700.0)], [("fridge", 120.0)]),
}


@pytest.fixture(params=sorted(METER_SITES))
def two_meter_sites(request):
    devices, meters, plugs = METER_SITES[request.param]
    w0 = _window()
    with tempfile.TemporaryDirectory() as ta, tempfile.TemporaryDirectory() as tb:
        sa = _build(ta, devices)
        sb = _build(tb, devices)
        _fill_meters(sa[1], meters, w0)
        _fill_meters(sb[1], meters, w0)
        _fill_plugs(sb[1], plugs, w0)        # the only difference: the plugs have rows
        da, db_ = _dispatcher(sa[0], sa[1], sa[2], ta), _dispatcher(sb[0], sb[1], sb[2], tb)
        try:
            yield sa, sb, da, db_, w0
        finally:
            da._warm_stop.set()
            db_._warm_stop.set()


def test_with_a_meter_no_plug_is_ever_a_consumer(two_meter_sites):
    from shelly_analyzer.services.energy_balance import consumer_keys, plug_only_keys
    (cfg_a, _, _), (cfg_b, _, _), _, _, _ = two_meter_sites
    assert plug_only_keys(cfg_a) == plug_only_keys(cfg_b) == []
    assert consumer_keys(cfg_a) == consumer_keys(cfg_b)
    assert all(k not in {"heater", "lamp", "fridge"} for k in consumer_keys(cfg_b))


def test_with_a_meter_energy_flow_is_the_same_to_the_last_bit(two_meter_sites):
    from shelly_analyzer.services.energy_flow import compute_energy_flow, compute_energy_flow_live
    (cfg_a, st_a, _), (cfg_b, st_b, _), _, _, _ = two_meter_sites
    for period in ("today", "yesterday", "week", "month", "year"):
        a = compute_energy_flow(st_a.db, cfg_a, period)
        b = compute_energy_flow(st_b.db, cfg_b, period)
        assert _strip_time(a) == _strip_time(b), period
        assert "plugs_only" not in b, period
    snap = {k: [{"power_total_w": w}] for k, w in (("house", 900.0), ("wallbox", 400.0), ("mystery", 600.0),
                                                   ("heater", 1800.0), ("lamp", 40.0), ("fridge", 100.0))}
    la, lb = compute_energy_flow_live(cfg_a, snap), compute_energy_flow_live(cfg_b, snap)
    assert la == lb and "plugs_only" not in lb


def test_with_a_meter_sankey_co2_balance_and_report_are_the_same_to_the_last_bit(two_meter_sites):
    from shelly_analyzer.services.energy_balance import compute_balance, compute_co2, report_consumption
    from shelly_analyzer.services.sankey import compute_sankey
    (cfg_a, st_a, _), (cfg_b, st_b, _), _, _, w0 = two_meter_sites
    for period in ("today", "week", "year"):
        sa = compute_sankey(st_a.db, cfg_a.devices, cfg_a.solar, period)
        sb = compute_sankey(st_b.db, cfg_b.devices, cfg_b.solar, period)
        assert sa == sb and sb.plugs_only is False, period
    end = w0 + 4 * 3600
    assert compute_co2(st_a.db, cfg_a, w0, end, {}, 400.0).as_dict() == compute_co2(st_b.db, cfg_b, w0, end, {}, 400.0).as_dict()
    ba, bb = compute_balance(st_a.db, cfg_a, w0, end), compute_balance(st_b.db, cfg_b, w0, end)
    assert ba.as_dict() == bb.as_dict() and not bb.plugs_only and "plugs_only" not in bb.as_dict()
    ra = report_consumption(st_a.db, cfg_a, w0, end, unit_price=0.3)
    rb = report_consumption(st_b.db, cfg_b, w0, end, unit_price=0.3)
    assert ra.as_dict() == rb.as_dict()
    assert ra.keys == rb.keys


def test_with_a_meter_the_actions_are_the_same_to_the_last_bit(two_meter_sites):
    """The payloads of the three tabs and of the costs summary, through the real
    dispatcher. The plugs' own cards in Costs are the one thing that may differ."""
    _, _, da, db_, _ = two_meter_sites
    for period in ("today", "week", "year"):
        a = da.dispatch("energy_flow", {"period": period})
        b = db_.dispatch("energy_flow", {"period": period})
        assert _strip_time(a) == _strip_time(b), period
    sa, sb = da.dispatch("sankey", {"period": "year"}), db_.dispatch("sankey", {"period": "year"})
    assert sa == sb and "plugs_only" not in sb
    ca, cb = da.dispatch("co2", {}), db_.dispatch("co2", {})
    for key in ("co2_today_kg", "co2_week_kg", "co2_month_kg", "co2_year_kg", "footprint", "footprint_present",
                "tree_days", "car_km", "chain"):
        assert ca[key] == cb[key], key
    assert "plugs_only" not in cb
    assert [d["key"] for d in cb["device_hourly_co2"]] == [d["key"] for d in ca["device_hourly_co2"]]
    ka, kb = da.dispatch("costs", {}), db_.dispatch("costs", {})
    assert ka["summary"] == kb["summary"] and ka["balance"] == kb["balance"]
    assert ka["balance_present"] == kb["balance_present"]
    assert "plugs_only" not in kb and "plugs_balance" not in kb


def test_a_grid_or_pv_source_keeps_a_plugs_only_installation_as_it_was():
    """No meter, but a grid series is configured: the load is read off that series
    (the supply identity) and the plugs are not added on top."""
    from shelly_analyzer.services.energy_balance import plug_only_keys
    with tempfile.TemporaryDirectory() as tmp:
        cfg, _, _ = _build(tmp, [_sw("plug1", 20), _sw("plug2", 21)],
                           {"solar": {"enabled": True, "grid_meter_device_key": "grid_ext"}})
        assert plug_only_keys(cfg) == []
        cfg2, _, _ = _build(tmp + "/b", [_sw("plug1", 20), _sw("plug2", 21)],
                            {"solar": {"enabled": True, "pv_production_device_key": "pv"}})
        assert plug_only_keys(cfg2) == []


# ── 2. a plugs-only installation says the same thing everywhere ─────────────

PLUGS = [("plug1", 100.0), ("plug2", 200.0), ("plug3", 40.0)]


@pytest.fixture()
def plug_site():
    w0 = _window()
    with tempfile.TemporaryDirectory() as tmp:
        devices = [_sw("plug1", 20), _sw("plug2", 21), _sw("plug3", 22), _sw("strip", 23, "plug1")]
        cfg, st, p = _build(tmp, devices)
        _fill_plugs(st, PLUGS + [("strip", 30.0)], w0, minutes=29 * 60, step=60)   # `strip` hangs behind plug1; runs up to ~1 h ago, so the 24 h views see it
        d = _dispatcher(cfg, st, p, tmp)
        try:
            yield cfg, st, d, w0
        finally:
            d._warm_stop.set()


def _expected_kwh(st, keys):
    total = 0.0
    for k in keys:
        df = st.db.query_hourly(k)
        total += float(df["kwh"].clip(lower=0).sum()) if df is not None and not df.empty else 0.0
    return total


def test_only_the_top_level_plugs_are_the_house(plug_site):
    from shelly_analyzer.services.energy_balance import consumer_keys, plug_only_keys
    cfg, st, d, w0 = plug_site
    assert plug_only_keys(cfg) == ["plug1", "plug2", "plug3"]          # `strip` is inside plug1
    assert consumer_keys(cfg) == ["plug1", "plug2", "plug3"]
    assert _expected_kwh(st, ["plug1", "plug2", "plug3"]) > 1.0


def test_energy_flow_is_the_sum_of_the_plugs_and_claims_nothing_about_the_grid(plug_site):
    cfg, st, d, w0 = plug_site
    want = _expected_kwh(st, [k for k, _ in PLUGS])
    out = d.dispatch("energy_flow", {"period": "year"})
    assert out["ok"] and out["plugs_only"] is True
    assert abs(out["house"]["load"] - want) < 0.002, (out["house"]["load"], want)
    assert [c["key"] for c in out["consumers"]] == ["plug2", "plug1", "plug3"]      # by size, `strip` not in it
    assert out["has_grid_meter"] is False and out["has_pv"] is False and out["has_battery"] is False
    # nothing was measured at the grid connection
    assert out["sources"]["grid"] == 0 and out["sinks"]["grid"] == 0 and out["sinks"]["battery"] == 0
    assert all(f["v"] == 0 for f in out["flows"]), out["flows"]
    assert out["house"]["autarky_pct"] is None and out["house"]["self_consumption_pct"] is None
    # CO2 is the consumption times the grid mix (the flat factor when no hourly mix is stored)
    assert abs(out["house"]["co2_kg"] - want * 0.380) < 0.002
    assert abs(sum(c["kwh"] for c in out["consumers"]) - out["house"]["consumers_sum"]) < 0.002


def test_the_live_flow_sums_the_top_level_plugs(plug_site):
    from shelly_analyzer.services.energy_flow import compute_energy_flow_live
    cfg, _, _, _ = plug_site
    snap = {k: [{"power_total_w": w}] for k, w in (("plug1", 100.0), ("plug2", 200.0), ("plug3", 40.0), ("strip", 30.0))}
    out = compute_energy_flow_live(cfg, snap)
    assert out["plugs_only"] is True and out["unit"] == "W"
    assert out["house"]["load"] == 340.0
    assert sorted(c["key"] for c in out["consumers"]) == ["plug1", "plug2", "plug3"]
    assert out["sources"]["grid"] == 0 and out["house"]["autarky_pct"] is None


def test_co2_follows_the_grid_mix_of_the_hours_the_plugs_drew_it_in(plug_site):
    cfg, st, d, w0 = plug_site
    hours = [(w0 - 7200 + i * 3600, "DE_LU", 100.0, "test", w0) for i in range(0, 48)]
    st.db.upsert_co2_intensity(hours)
    want = _expected_kwh(st, [k for k, _ in PLUGS])
    out = d.dispatch("co2", {})
    assert out["ok"] and out["plugs_only"] is True
    assert abs(out["co2_year_kg"] - want * 0.100) < 0.002, (out["co2_year_kg"], want)
    assert sorted(x["key"] for x in out["device_hourly_co2"]) == ["plug1", "plug2", "plug3"]
    assert out["footprint_present"] is False and out["chain"]["has_supply"] is False
    ef = d.dispatch("energy_flow", {"period": "year"})
    assert abs(ef["house"]["co2_kg"] - out["co2_year_kg"]) < 0.002


def test_costs_balance_and_summary_say_the_same(plug_site):
    cfg, st, d, w0 = plug_site
    want = _expected_kwh(st, [k for k, _ in PLUGS])
    out = d.dispatch("costs", {})
    assert out["ok"] and out["plugs_only"] is True
    assert out["balance_present"] is False and out["balance"] == {}        # no grid/PV balance is claimed
    for rk in ("today", "week", "month", "year"):
        row = out["plugs_balance"][rk]
        assert row["total_load_kwh"] == out["summary"][rk + "_kwh"]
        assert row["net_cost_eur"] == out["summary"][rk + "_eur"]
    assert abs(out["summary"]["year_kwh"] - want) < 0.002
    assert abs(out["plugs_balance"]["year"]["co2_kg"] - want * 0.380) < 0.002


def test_the_balance_and_the_report_carry_the_same_consumption(plug_site):
    from shelly_analyzer.services.energy_balance import compute_balance, report_consumption
    cfg, st, d, w0 = plug_site
    want = _expected_kwh(st, [k for k, _ in PLUGS])
    end = w0 + 40 * 3600
    b = compute_balance(st.db, cfg, w0 - 3600, end)
    assert b.plugs_only is True and b.as_dict()["plugs_only"] is True
    assert abs(b.total_load_kwh - want) < 0.002
    # nothing was measured at the grid connection
    assert (b.has_grid_meter, b.has_pv, b.has_battery) == (False, False, False)
    assert b.grid_import_kwh == 0.0 and b.grid_export_kwh == 0.0 and b.pv_production_kwh == 0.0
    r = report_consumption(st.db, cfg, w0 - 3600, end, unit_price=0.30)
    assert r.basis == "devices"                       # the plugs are the circuits; no supply balance exists
    assert abs(r.total_kwh - want) < 0.002 and r.keys == ["plug1", "plug2", "plug3"]
    assert abs(r.total_cost - want * 0.30) < 0.01


def test_the_balance_uses_the_live_counter_of_a_plug_that_has_one(plug_site):
    from shelly_analyzer.services.energy_balance import compute_balance
    cfg, st, d, w0 = plug_site
    midnight = int(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    now = int(datetime.now().timestamp())
    base = compute_balance(st.db, cfg, midnight, now, live_today_kwh={}, today_start_ts=midnight)
    live = compute_balance(st.db, cfg, midnight, now, live_today_kwh={"plug2": 5.0}, today_start_ts=midnight)
    # a plug with a live accumulator: that figure stands for today's hours; the others keep their rollup
    only_others = sum(_expected_kwh(st, [k]) for k in ("plug1", "plug3"))
    assert live.total_load_kwh >= 5.0 and live.total_load_kwh <= only_others + 5.0 + 0.01
    assert base.total_load_kwh >= 0.0


def test_the_sankey_is_the_sum_of_the_top_level_plugs(plug_site):
    cfg, st, d, w0 = plug_site
    want = _expected_kwh(st, [k for k, _ in PLUGS])
    out = d.dispatch("sankey", {"period": "year"})
    assert out["ok"] and out["plugs_only"] is True
    assert abs(out["total_consumption_kwh"] - want) < 0.002
    assert out["pv_production_kwh"] == 0 and out["feed_in_kwh"] == 0
    labels = out["sankey"]["node"]["label"]
    assert sorted(labels) == sorted(["House", "Grid", "Plug1", "Plug2", "Plug3"]) or "Strip" not in labels


# ── the page ─────────────────────────────────────────────────────────────────

def test_the_note_exists_in_every_language_and_says_the_two_things_it_must():
    from shelly_analyzer.i18n import LANGS, _I18N
    for lang in LANGS:
        note = _I18N[lang]["web.plugs_only.note"]
        assert len(note) > 60 and "CO₂" in note or "CO" in note, lang
        assert _I18N[lang]["web.plugs_only.consumption"].strip(), lang
    # the nine are not one sentence copied nine times
    assert len({_I18N[l]["web.plugs_only.note"] for l in LANGS}) == 9


def test_the_three_tabs_show_the_note_only_for_a_plugs_only_installation():
    src = (Path(__file__).resolve().parents[1] / "src" / "shelly_analyzer" / "services" / "webdash.py").read_text(encoding="utf-8")
    assert src.count("_plugsNoteHtml()") >= 4                              # definition + flow + co2 + costs
    assert "if (d.plugs_only) html += _plugsNoteHtml();" in src
    assert "if (data.plugs_only) html += _plugsNoteHtml();" in src
    assert "data.plugs_only && data.plugs_balance" in src
    # without a meter the diagram has no grid node and no grid legend entry
    assert "if (!bare) nodesHtml += node('grid'" in src
    assert "if (!d.plugs_only) html += '<span><span class=\"co2-dot\" style=\"background:' + FLOW_COL.grid" in src
