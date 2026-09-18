"""The one-bus supply chain behind every CO₂ figure (v17).

Rule under test: within an hour the house is one bus — every consumed kWh,
owner or tenant, carries the hour's mix of grid import (grid mix), direct PV
(panel manufacturing) and battery discharge (what was stored + storage
manufacturing). Export is charged nothing. Owner + tenant = house.
"""
import os
import sys
import types

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.services.energy_balance import (  # noqa: E402
    build_supply_chain, compute_co2, compute_grid_cost_share, consumer_keys,
    consumer_hourly, device_role, live_mix)

H = 3600


class _FakeDB:
    def __init__(self, series, intensity=None):
        self.series = series
        self.intensity = intensity or {}

    def query_hourly(self, key, start_ts=None, end_ts=None, compensate=True):
        rows = [(h, k) for h, k in self.series.get(key, {}).items()
                if (start_ts is None or h >= start_ts) and (end_ts is None or h <= end_ts)]
        return pd.DataFrame(rows, columns=["hour_ts", "kwh"])

    def query_co2_intensity(self, zone, start_ts, end_ts):
        rows = [(h, zone, v, "test", 0) for h, v in self.intensity.items() if start_ts <= h < end_ts]
        return pd.DataFrame(rows, columns=["hour_ts", "zone", "intensity_g_per_kwh", "source", "fetched_at"])


def _dev(key, name=None, parent="", sub=False, kind="em"):
    return types.SimpleNamespace(key=key, name=name or key, parent=parent,
                                 subtract_from_parent_display=sub, kind=kind, phases=3)


def _cfg(battery=True, tenant=True, devices=None, pv_mfg=40.0, bat_mfg=20.0, eff=95.0,
         feeds_tenants=False):
    solar = types.SimpleNamespace(
        enabled=True, grid_meter_device_key="grid", pv_meter_device_key="",
        pv_production_device_key="pv", battery_device_key="battery" if battery else "",
        grid_display_device_key="", pv_embodied_g_per_kwh=pv_mfg,
        battery_manufacturing_g_per_kwh=bat_mfg, battery_embodied_g_per_kwh=60.0,
        battery_feeds_tenants=feeds_tenants)
    tenants = [types.SimpleNamespace(name="Mieter", tenant_id="m1", device_keys=["ten"])] if tenant else []
    return types.SimpleNamespace(
        solar=solar, pv_source=None,
        tenant=types.SimpleNamespace(enabled=tenant, tenants=tenants),
        battery=types.SimpleNamespace(capacity_kwh=10.0, efficiency_pct=eff),
        co2=types.SimpleNamespace(bidding_zone="DE_LU"),
        pricing=types.SimpleNamespace(co2_intensity_g_per_kwh=380.0),
        devices=devices if devices is not None else [_dev("haus"), _dev("wallbox", parent="haus", sub=True), _dev("ten"), _dev("netz")],
    )


def test_pure_export_hour_costs_nothing_and_everyone_gets_pv():
    # PV 5, house uses 2 (direct), 3 exported; tenant used 1 of the 2.
    db = _FakeDB({"grid": {0: -3.0}, "pv": {0: 5.0}, "battery": {0: 0.0}, "ten": {0: 1.0}}, {0: 500.0})
    r = compute_co2(db, _cfg(), 0, H, {0: 500.0}, 500.0)
    assert abs(r.property_kg - 2.0 * 40 / 1000) < 1e-9
    assert abs(r.tenant_kg - 1.0 * 40 / 1000) < 1e-9
    assert abs(r.owner_kg + r.tenant_kg - r.property_kg) < 1e-9
    assert r.export_kwh == 3.0 and r.grid_kg == 0.0
    assert abs(r.export_saved_kg - 3.0 * (500 - 40) / 1000) < 1e-9


def test_battery_night_carries_the_sun_it_stored_plus_manufacturing():
    # Hour 0: PV 10, house 2 direct, 8 charged. Hour 1: night, 4 discharged, no grid.
    db = _FakeDB({"grid": {0: 0.0, H: 0.0}, "pv": {0: 10.0, H: 0.0},
                  "battery": {0: 8.0, H: -4.0}, "ten": {0: 0.5, H: 1.0}}, {0: 300.0, H: 500.0})
    ch = build_supply_chain(db, _cfg(), 0, 2 * H, {0: 300.0, H: 500.0}, 400.0, warmup_s=0)
    hm1 = ch.hours[H]
    # stored origin = 40 g / 0.95 efficiency, plus 20 g manufacturing per kWh out
    assert abs(hm1.bat_stored - 40 / 0.95) < 1e-6
    assert abs(hm1.bat_int - (40 / 0.95 + 20)) < 1e-6
    # night on the battery: every OWNER kWh = battery (the house mix carries
    # the tenant's grid kWh on top, see below)
    assert abs(hm1.mix_for("owner") - hm1.bat_int) < 1e-6
    r = compute_co2(db, _cfg(), 0, 2 * H, {0: 300.0, H: 500.0}, 400.0, chain=ch)
    # The tenant hangs grid-parallel (default): the battery is the owner's,
    # so the tenant's night kWh is a GRID kWh even though the utility meter
    # imported nothing that hour (attribution, not a meter) — and the house
    # is owner bus + tenant bus, so the identity holds.
    assert abs(r.tenant_kg - (0.5 * 40 + 1.0 * 500.0) / 1000) < 1e-9
    assert abs(r.grid_kg - 0.5) < 1e-9 and r.property_kg > 0
    assert abs(r.owner_kg - (1.5 * 40 + 4.0 * hm1.bat_int) / 1000) < 1e-9
    assert abs(r.owner_kg + r.tenant_kg - r.property_kg) < 1e-9
    # With an import that covers it, the night kWh is a grid kWh — never the battery.
    db_g = _FakeDB({"grid": {0: 0.0, H: 1.0}, "pv": {0: 10.0, H: 0.0},
                    "battery": {0: 8.0, H: -4.0}, "ten": {0: 0.5, H: 1.0}}, {0: 300.0, H: 500.0})
    ch_g = build_supply_chain(db_g, _cfg(), 0, 2 * H, {0: 300.0, H: 500.0}, 400.0, warmup_s=0)
    r_g = compute_co2(db_g, _cfg(), 0, 2 * H, {0: 300.0, H: 500.0}, 400.0, chain=ch_g)
    assert abs(r_g.tenant_kg - (0.5 * 40 + 1.0 * 500.0) / 1000) < 1e-9
    assert ch_g.split(H, "tenant") == (1.0, 0.0, 0.0)
    assert abs(r_g.owner_kg + r_g.tenant_kg - r_g.property_kg) < 1e-9
    # Behind the house bus (battery_feeds_tenants True): battery-fed like everybody else.
    cfg1 = _cfg(feeds_tenants=True)
    ch1 = build_supply_chain(db, cfg1, 0, 2 * H, {0: 300.0, H: 500.0}, 400.0, warmup_s=0)
    r1 = compute_co2(db, cfg1, 0, 2 * H, {0: 300.0, H: 500.0}, 400.0, chain=ch1)
    assert abs(ch1.hours[H].mix - hm1.bat_int) < 1e-6           # one bus: every kWh = battery
    assert abs(r1.tenant_kg - (0.5 * 40 + 1.0 * hm1.bat_int) / 1000) < 1e-9
    assert r1.property_kg < r.property_kg                     # physics: that kWh was battery, not grid


def test_grid_charged_battery_carries_the_grid_mix():
    db = _FakeDB({"grid": {0: 6.0, H: 0.0}, "pv": {0: 0.0, H: 0.0},
                  "battery": {0: 5.0, H: -2.0}, "ten": {}}, {0: 600.0, H: 300.0})
    ch = build_supply_chain(db, _cfg(tenant=False), 0, 2 * H, {0: 600.0, H: 300.0}, 400.0, warmup_s=0)
    h0, h1 = ch.hours[0], ch.hours[H]
    assert h0.bch_grid == 5.0 and h0.gi_load == 1.0          # 1 kWh served loads, 5 charged
    assert abs(h1.bat_stored - 600 / 0.95) < 1e-6              # what went in was 600 g grid
    assert abs(h1.mix - (600 / 0.95 + 20)) < 1e-6


def test_mixed_hour_is_one_bus_for_owner_and_tenant():
    # PV 3 direct, grid 1, battery 1 → load 5; behind the house bus the
    # tenant's 2 kWh get the same mix as the owner's 3.
    db = _FakeDB({"grid": {0: 1.0}, "pv": {0: 3.0}, "battery": {0: -1.0}, "ten": {0: 2.0}}, {0: 400.0})
    cfg = _cfg(feeds_tenants=True)
    ch = build_supply_chain(db, cfg, 0, H, {0: 400.0}, 400.0, warmup_s=0)
    hm = ch.hours[0]
    assert abs(hm.load - 5.0) < 1e-9 and not hm.two_bus
    r = compute_co2(db, cfg, 0, H, {0: 400.0}, 400.0, chain=ch)
    assert abs(r.tenant_kg * 1000 - 2.0 * hm.mix) < 1e-6
    assert abs(r.owner_kg * 1000 - 3.0 * hm.mix) < 1e-6
    # never negative, sums by origin equal the total
    assert abs(r.grid_kg + r.pv_embodied_kg + r.battery_origin_kg + r.battery_embodied_kg - r.property_kg) < 1e-9


def test_grid_parallel_tenant_sees_pv_surplus_and_grid_but_never_the_battery():
    # Default wiring, the tenant is served last: PV 3 direct, import 2,
    # discharge 1 → load 6, tenant 2. Import and discharge both mean there was
    # no surplus for those kWh, so of its 2 kWh max(0, 2 − 2 − 1) = 0 is PV —
    # all grid. The owner keeps the 3 PV, the whole battery kWh and what is
    # left of the import (0).
    db = _FakeDB({"grid": {0: 2.0}, "pv": {0: 3.0}, "battery": {0: -1.0}, "ten": {0: 2.0}}, {0: 400.0})
    cfg = _cfg()
    ch = build_supply_chain(db, cfg, 0, H, {0: 400.0}, 400.0, warmup_s=0)
    hm = ch.hours[0]
    assert hm.two_bus and hm.ten_load == 2.0 and hm.ten_pv == 0.0 and hm.ten_grid == 2.0
    assert ch.split(0, "tenant") == (1.0, 0.0, 0.0)
    og, op, ob = ch.split(0, "owner")
    assert abs(og) < 1e-9 and abs(op - 0.75) < 1e-9 and abs(ob - 0.25) < 1e-9
    r = compute_co2(db, cfg, 0, H, {0: 400.0}, 400.0, chain=ch)
    assert abs(r.tenant_kg * 1000 - 2.0 * 400.0) < 1e-6
    assert abs(r.property_kg * 1000 - hm.grams) < 1e-6        # import covered the tenant: house = bus total
    assert abs(r.owner_kg + r.tenant_kg - r.property_kg) < 1e-9
    # device grams by role add up to the house, owner + tenant
    dg = (ch.device_grams({0: 4.0}, "owner")["g"] + ch.device_grams({0: 2.0}, "tenant")["g"])
    assert abs(dg - hm.grams) < 1e-6
    # a sunny hour with surplus: PV 6, export 1, tenant 2, no battery → the
    # tenant's 2 kWh are PV (2 − 0 − 0), the owner's 3 too.
    db2 = _FakeDB({"grid": {0: -1.0}, "pv": {0: 6.0}, "battery": {0: 0.0}, "ten": {0: 2.0}}, {0: 400.0})
    ch2 = build_supply_chain(db2, cfg, 0, H, {0: 400.0}, 400.0, warmup_s=0)
    assert ch2.split(0, "tenant") == (0.0, 1.0, 0.0)
    assert abs(ch2.intensity(0, "tenant") - 40.0) < 1e-9
    # device_grams follows the role
    assert abs(ch.device_grams({0: 2.0}, "tenant")["g"] - 800.0) < 1e-9
    assert abs(ch.device_grams({0: 2.0}, "tenant")["kwh_bat"]) < 1e-9
    assert ch.device_grams({0: 1.0}, "owner")["kwh_bat"] > 0


def test_consumers_each_meter_once_and_net_of_a_flagged_child():
    cfg = _cfg()
    assert device_role(cfg, "netz") == "owner"    # a Shelly not named as the grid meter is a consumer
    cfg.solar.grid_display_device_key = "netz"
    assert device_role(cfg, "netz") == "grid"
    assert device_role(cfg, "grid") == "grid" and device_role(cfg, "pv") == "pv"
    assert device_role(cfg, "ten") == "tenant" and device_role(cfg, "wallbox") == "owner"
    assert consumer_keys(cfg) == ["haus", "wallbox", "ten"]
    db = _FakeDB({"haus": {0: 5.0}, "wallbox": {0: 3.0}})
    assert consumer_hourly(db, cfg, "haus", 0, H) == {0: 2.0}


def test_device_grams_sum_to_the_house_when_meters_agree():
    db = _FakeDB({"grid": {0: 1.0}, "pv": {0: 3.0}, "battery": {0: -1.0},
                  "haus": {0: 4.0}, "wallbox": {0: 1.0}, "ten": {0: 1.0}}, {0: 400.0})
    cfg = _cfg()
    ch = build_supply_chain(db, cfg, 0, H, {0: 400.0}, 400.0, warmup_s=0)
    total = sum(ch.device_grams(consumer_hourly(db, cfg, k, 0, H))["g"] for k in consumer_keys(cfg))
    r = compute_co2(db, cfg, 0, H, {0: 400.0}, 400.0, chain=ch)
    assert abs(total / 1000 - r.property_kg) < 1e-9


def test_grid_only_home_reduces_to_the_flat_grid_mix():
    cfg = _cfg(devices=[_dev("haus"), _dev("ten")])
    cfg.solar.grid_meter_device_key = ""
    cfg.solar.pv_production_device_key = ""
    cfg.solar.battery_device_key = ""
    db = _FakeDB({"haus": {0: 2.0}, "ten": {0: 1.0}}, {0: 400.0})
    r = compute_co2(db, cfg, 0, H, {0: 400.0}, 400.0)
    assert not r.has_solar
    assert abs(r.property_kg - 3.0 * 400 / 1000) < 1e-9
    assert abs(r.tenant_kg - 0.4) < 1e-9


def test_owner_cost_share_follows_the_same_bus():
    # load 6 = pv 3 + grid 2 + battery 1; tenant 2 → behind the house bus the
    # owner's 4 pay the grid fraction 1/3 …
    db = _FakeDB({"grid": {0: 2.0}, "pv": {0: 3.0}, "battery": {0: -1.0}, "ten": {0: 2.0}}, {0: 400.0})
    assert abs(compute_grid_cost_share(db, _cfg(feeds_tenants=True), [(0, H)])[0] - 1 / 3) < 1e-9
    # … grid-parallel (default) the tenant took the whole import, the owner pays nothing
    assert abs(compute_grid_cost_share(db, _cfg(), [(0, H)])[0] - 0.0) < 1e-9
    # grid-charged battery is paid in the charging hour, discharge is then free
    db2 = _FakeDB({"grid": {0: 6.0}, "pv": {0: 0.0}, "battery": {0: 5.0}, "ten": {}}, {0: 400.0})
    assert abs(compute_grid_cost_share(db2, _cfg(tenant=False), [(0, H)])[0] - 1.0) < 1e-9


def test_live_mix_matches_the_hourly_rule():
    m = live_mix(pv_w=3000, grid_w=1000, batt_w=-1000, grid_intensity=400, pv_mfg=40, battery_intensity=62)
    assert abs(m["load_w"] - 5000) < 1e-9
    assert abs(m["intensity"] - (1000 * 400 + 3000 * 40 + 1000 * 62) / 5000) < 1e-6
    assert live_mix(0, 0, 0, 400)["intensity"] == 400          # nothing drawn → grid mix
    assert live_mix(2000, -2000, 0, 400)["load_w"] == 0        # all exported, nothing consumed


def test_the_one_second_live_answer_prices_consumers_on_the_house_mix():
    """/api/co2_live refreshes the live table every second. In 17.0/17.1 it
    still priced every device — the grid meter included — at the grid mix,
    and overwrote the one-bus rates /api/co2 had rendered a second earlier."""
    import json
    import tempfile
    import time as _time
    from pathlib import Path
    from shelly_analyzer.io.config import load_config
    from shelly_analyzer.io.storage import Storage
    from shelly_analyzer.web.action_dispatch import ActionDispatcher

    class _Store:
        def snapshot(self):
            return {"house": [{"power_total_w": 1000.0}], "flat": [{"power_total_w": 500.0}],
                    "grid": [{"power_total_w": -200.0}], "pv": [{"power_total_w": 1700.0}],
                    "battery": [{"power_total_w": 0.0}]}

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        st = Storage(tmp / "data")
        now = int(_time.time())
        st.db.upsert_co2_intensity([((now // 3600) * 3600, "DE_LU", 500.0, "test", now)])
        p = tmp / "config.json"
        p.write_text(json.dumps({
            "devices": [{"key": "house", "name": "House", "host": "1.2.3.4", "kind": "em"},
                        {"key": "flat", "name": "Flat", "host": "1.2.3.6", "kind": "em"},
                        {"key": "grid", "name": "Grid", "host": "1.2.3.5", "kind": "em"}],
            "co2": {"enabled": True, "bidding_zone": "DE_LU"},
            "solar": {"enabled": True, "grid_meter_device_key": "grid", "pv_production_device_key": "pv",
                      "battery_device_key": "battery", "pv_embodied_g_per_kwh": 40.0},
            "tenant": {"enabled": True, "tenants": [{"tenant_id": "f", "name": "Flat", "device_keys": ["flat"]}]},
        }))
        d = ActionDispatcher(load_config(p), st, _Store(), out_dir=tmp, cfg_path=p, lang="en")
        r = d.dispatch("co2_live", {})
        assert r.get("ok"), r
        names = [x["name"] for x in r["device_rates"]]
        assert names == ["House", "Flat"], names          # the grid meter is not a consumer
        # 1500 W load fed by 1700 W PV, 200 W exported: everything is PV → 40 g/kWh, not 500
        assert r["live_mix"]["intensity"] < 100.0, r["live_mix"]
        for x in r["device_rates"]:
            assert abs(x["co2_g_h"] - x["watts"] * r["live_mix"]["intensity"] / 1000.0) < 0.2


def test_energy_flow_of_a_grid_only_home_carries_the_consumers_grams():
    """A home without a grid meter (has_supply False): the house figure of the
    energy flow must be the consumers' grams, not zero — the same number the
    CO₂ tab shows for the property."""
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from shelly_analyzer.services.energy_flow import compute_energy_flow
    tz = ZoneInfo("Europe/Berlin")
    now = datetime(2026, 9, 18, 15, 0, tzinfo=tz)
    h1 = int(now.replace(hour=8, minute=0).timestamp())
    h2 = int(now.replace(hour=12, minute=0).timestamp())
    cfg = _cfg(devices=[_dev("haus"), _dev("ten")])
    cfg.solar.grid_meter_device_key = ""
    cfg.solar.pv_production_device_key = ""
    cfg.solar.battery_device_key = ""
    db = _FakeDB({"haus": {h1: 2.0, h2: 3.0}, "ten": {h1: 0.5, h2: 0.2}}, {h1: 400.0, h2: 300.0})
    ef = compute_energy_flow(db, cfg, "today", now=now)
    assert not ef["has_supply"]
    want_g = 2.5 * 400 + 3.2 * 300
    assert abs(sum(c["co2_g"] for c in ef["consumers"]) - want_g) < 1e-6
    assert abs(ef["house"]["co2_kg"] - want_g / 1000.0) < 1e-6
    assert abs(ef["house"]["intensity"] - want_g / 5.7) < 0.1
    r = compute_co2(db, cfg, h1, h2 + H, {h1: 400.0, h2: 300.0}, 380.0)
    assert abs(r.property_kg - ef["house"]["co2_kg"]) < 1e-6


def test_live_mix_for_role_gives_the_tenant_export_and_grid_only():
    from shelly_analyzer.services.energy_balance import live_mix_for_role
    cfg = _cfg()
    # night: battery 1000 W covers the house, grid 200 W, tenant 500 W →
    # tenant all grid, owner all battery
    t = live_mix_for_role(cfg, "tenant", 0, 200, -1000, 500, 400, 40, 62)
    o = live_mix_for_role(cfg, "owner", 0, 200, -1000, 500, 400, 40, 62)
    assert t["battery"] == 0.0 and t["grid"] == 1.0 and t["intensity"] == 400
    assert abs(o["battery"] - 1.0) < 1e-9 and abs(o["intensity"] - 62) < 1e-9
    # exporting 300 W with the tenant at 500 W → 60 % PV for the tenant (live tile rule)
    t2 = live_mix_for_role(cfg, "tenant", 4000, -300, 0, 500, 400, 40, 62)
    assert abs(t2["pv"] - 0.6) < 1e-9 and abs(t2["intensity"] - (0.6 * 40 + 0.4 * 400)) < 1e-9
    # behind the house bus: the house mix for everybody
    cfg1 = _cfg(feeds_tenants=True)
    assert live_mix_for_role(cfg1, "tenant", 0, 200, -1000, 500, 400, 40, 62) == live_mix(0, 200, -1000, 400, 40, 62)
    # the tenant's meter reads more than the bus carries (two measuring
    # systems): the owner's draw keeps the house mix, never falls to the grid
    o3 = live_mix_for_role(cfg, "owner", 280, 0, 0, 462, 400, 40, 62)
    assert abs(o3["intensity"] - 40) < 1e-9 and o3["grid"] == 0.0
