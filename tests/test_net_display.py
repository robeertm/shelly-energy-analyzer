"""Net "meter behind meter" display subtraction — generic, config-driven.

Run: python3 tests/test_net_display.py  (no pytest dependency)

Covers the transform where a device wired physically behind another (e.g. a
wallbox on a circuit fed through the house meter) is virtually subtracted from
its parent's DISPLAYED power/energy on the Live view and Plots page, while the
child keeps its own tile. Pure display: stored samples are never touched and a
raw/gross view shows everything again.
"""
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.io.config import (  # noqa: E402
    AppConfig, DeviceConfig, MainMeter, load_config, save_config,
)
from shelly_analyzer.services.net_display import (  # noqa: E402
    net_display_children, apply_live_subtraction, apply_history_subtraction,
)


def test_config_roundtrip_flag():
    d = tempfile.mkdtemp()
    p = os.path.join(d, "config.json")
    # Legacy config: flag absent → defaults to False.
    json.dump({"version": "16.0.0",
               "devices": [{"key": "haus", "name": "Haus", "host": "1.1.1.1", "kind": "em"}]},
              open(p, "w"))
    assert load_config(p).devices[0].subtract_from_parent_display is False

    cfg = AppConfig(
        main_meters=[MainMeter(id="grid", name="Hauptzähler", serial="")],
        devices=[
            DeviceConfig(key="haus", name="Haus", host="1.1.1.1", kind="em", parent="grid"),
            DeviceConfig(key="wallbox", name="Wallbox", host="1.1.1.2", kind="em",
                         parent="haus", subtract_from_parent_display=True),
        ],
    )
    save_config(cfg, p)
    back = load_config(p)
    flags = {x.key: x.subtract_from_parent_display for x in back.devices}
    assert flags == {"haus": False, "wallbox": True}, flags
    print("OK  config roundtrip (subtract_from_parent_display persists)")


def test_children_map():
    devs = [
        DeviceConfig(key="grid", name="", host="", kind="em"),  # not a main-meter here, still a device
        DeviceConfig(key="haus", name="Haus", host="", kind="em", parent="grid"),
        DeviceConfig(key="wallbox", name="WB", host="", kind="em",
                     parent="haus", subtract_from_parent_display=True),
        # tenant behind a HAND-READ main meter id (not a device) → no display sub
        DeviceConfig(key="mieter", name="M", host="", kind="em",
                     parent="handmeter", subtract_from_parent_display=True),
        # flagged but no parent → ignored
        DeviceConfig(key="lonely", name="L", host="", kind="em",
                     subtract_from_parent_display=True),
        # flagged self-reference → ignored
        DeviceConfig(key="selfref", name="S", host="", kind="em",
                     parent="selfref", subtract_from_parent_display=True),
    ]
    m = net_display_children(devs)
    assert m == {"haus": ["wallbox"]}, m
    # Nothing flagged → empty (cheap skip).
    assert net_display_children([DeviceConfig(key="a", name="", host="", kind="em")]) == {}
    print("OK  children map (only device-parent, flagged; ignores main-meter/self/none)")


def test_live_subtraction_basic():
    # Live numbers: Haus tile reads house+wallbox; wallbox its own.
    tiles = [
        {"key": "haus", "power_w": 5968.8, "today_kwh": 14.089},
        {"key": "wallbox", "power_w": 5466.7, "today_kwh": 5.323},
        {"key": "solar", "power_w": -177.2, "today_kwh": -27.34},
    ]
    submap = net_display_children([
        DeviceConfig(key="haus", name="", host="", kind="em", parent="solar"),
        DeviceConfig(key="wallbox", name="", host="", kind="em",
                     parent="haus", subtract_from_parent_display=True),
        DeviceConfig(key="solar", name="", host="", kind="em", parent="handmeter"),
    ])
    apply_live_subtraction(tiles, submap)
    by = {t["key"]: t for t in tiles}
    assert abs(by["haus"]["power_w"] - 502.1) < 1e-6, by["haus"]["power_w"]
    assert abs(by["haus"]["today_kwh"] - 8.766) < 1e-6, by["haus"]["today_kwh"]
    # child + unrelated device untouched
    assert by["wallbox"]["power_w"] == 5466.7
    assert by["solar"]["power_w"] == -177.2
    assert by["haus"]["net_of_children"] == ["wallbox"]
    print("OK  live subtraction (haus net = raw − wallbox; others untouched)")


def test_live_subtraction_chain():
    # A behind B behind C: C_net = C − B_raw, B_net = B − A_raw (full raw removed
    # at each level, using pre-subtraction values).
    tiles = [
        {"key": "C", "power_w": 1000.0, "today_kwh": 10.0},
        {"key": "B", "power_w": 600.0, "today_kwh": 6.0},
        {"key": "A", "power_w": 200.0, "today_kwh": 2.0},
    ]
    submap = net_display_children([
        DeviceConfig(key="C", name="", host="", kind="em"),
        DeviceConfig(key="B", name="", host="", kind="em", parent="C",
                     subtract_from_parent_display=True),
        DeviceConfig(key="A", name="", host="", kind="em", parent="B",
                     subtract_from_parent_display=True),
    ])
    apply_live_subtraction(tiles, submap)
    by = {t["key"]: t for t in tiles}
    assert by["C"]["power_w"] == 400.0, by["C"]["power_w"]   # 1000 − 600(raw)
    assert by["B"]["power_w"] == 400.0, by["B"]["power_w"]   # 600 − 200(raw)
    assert by["A"]["power_w"] == 200.0
    print("OK  live subtraction chain (full raw removed per level)")


def test_empty_and_raw_are_noops():
    tiles = [{"key": "haus", "power_w": 100.0, "today_kwh": 1.0}]
    apply_live_subtraction(tiles, {})           # empty submap
    assert tiles[0]["power_w"] == 100.0
    apply_live_subtraction([], {"haus": ["x"]})  # empty tiles
    print("OK  empty submap / empty tiles are no-ops (raw bypass = skip caller)")


def test_history_subtraction():
    hist = {
        "haus": [{"ts": 1000, "w": 5000.0}, {"ts": 2000, "w": 5200.0}, {"ts": 3000, "w": 400.0}],
        "wallbox": [{"ts": 1005, "w": 4600.0}, {"ts": 2000, "w": 4700.0}, {"ts": 3000, "w": 0.0}],
        "solar": [{"ts": 1000, "w": -200.0}],
    }
    apply_history_subtraction(hist, {"haus": ["wallbox"]})
    ws = [p["w"] for p in hist["haus"]]
    # ts1000 -> nearest wallbox ts1005 (5 ms) = 4600 -> 400; ts2000 -> 4700 -> 500; ts3000 -> 0 -> 400
    assert abs(ws[0] - 400) < 1e-9 and abs(ws[1] - 500) < 1e-9 and abs(ws[2] - 400) < 1e-9, ws
    assert [p["w"] for p in hist["wallbox"]] == [4600.0, 4700.0, 0.0], "child untouched"
    assert hist["solar"][0]["w"] == -200.0, "unrelated untouched"
    print("OK  history subtraction (nearest-ts align; child + unrelated untouched)")


def test_history_subtraction_gap():
    # Child sample too far from the parent point (> tolerance) → not subtracted.
    hist = {
        "haus": [{"ts": 100000, "w": 3000.0}],
        "wallbox": [{"ts": 100000 + 30000, "w": 2000.0}],  # 30 s away, tol 10 s
    }
    apply_history_subtraction(hist, {"haus": ["wallbox"]})
    assert hist["haus"][0]["w"] == 3000.0, "gap beyond tolerance must leave gross"
    print("OK  history subtraction respects tolerance (gap → gross)")


# ── v16.73: whole-house totals count each METER once ─────────────────────
#
# 🔴 Reported from a live installation. The Live hero read 4 288 W while the
# tiles showed a house meter at 2 281 W, a wallbox at 46 W and a water heater at
# 1 960 W — the heater is wired behind the house meter, so it was already in the
# 2 281 W.
# The same error ran through the Costs summary (22.558 kWh where 11.311 was
# right, euros with it) and the Sankey diagram.
#
# The distinction that matters: subtracting a child from a PARENT'S OWN TILE is
# a display preference and stays opt-in. A whole-house TOTAL is not a
# preference — it follows the wiring.

def test_household_keys_counts_each_meter_once():
    from shelly_analyzer.services.net_display import household_keys

    def dev(k, parent="", sub=False):
        return DeviceConfig(key=k, name=k, host="", parent=parent,
                            subtract_from_parent_display=sub)

    robert = [dev("haus"), dev("wall"), dev("boil", "haus"), dev("light", "haus")]
    assert household_keys(robert) == {"haus", "wall"}, household_keys(robert)

    # With display-subtraction on, the parent no longer holds the children, so
    # they must be counted or the total loses them.
    netcfg = [dev("haus"), dev("wall"), dev("boil", "haus", True), dev("light", "haus", True)]
    assert household_keys(netcfg) == {"haus", "wall", "boil", "light"}

    # A main-meter parent is not a tile and cannot double-count.
    assert household_keys([dev("haus", "siedlung"), dev("wall", "siedlung")]) == {"haus", "wall"}
    # Junk must never drop a real meter.
    assert household_keys([dev("a", "a")]) == {"a"}
    assert household_keys([dev("a", "ghost")]) == {"a"}
    assert household_keys([]) == set()
    print("OK  household_keys counts each meter once, either configuration")


def test_costs_summary_excludes_submeters():
    """The summary must sum the counting set, not every row."""
    src = open(os.path.join(os.path.dirname(__file__), "..", "src", "shelly_analyzer",
                            "web", "action_dispatch.py"), encoding="utf-8").read()
    assert "_hh_keys = household_keys(_cost_devices)" in src, \
        "the costs summary does not consult the wiring"
    blk = src[src.index("# Build summary from device totals"):]
    blk = blk[:blk.index("_costs_payload = {")]
    sums = [l for l in blk.splitlines() if "_kwh\"] = round(sum(" in l or "_eur\"] = round(sum(" in l]
    assert len(sums) == 2, sums
    for line in sums:
        assert "for d in _summable" in line, f"still sums every row: {line.strip()}"
    # the self-consumption figure uses the same set
    assert "_hk_c" in src, "the solar self-consumption still sums every row"
    print("OK  the Costs summary and the solar figure count each meter once")


def test_sankey_follows_the_wiring_not_the_display_flag():
    """A flow diagram is not a display preference: energy that reaches the house
    through the house meter must not also arrive on its own arrow."""
    import pandas as pd
    from shelly_analyzer.services.net_display import flow_children, net_display_children
    from shelly_analyzer.services.sankey import compute_sankey

    class DB:
        def query_hourly(self, key, start_ts=None, end_ts=None):
            return pd.DataFrame({"kwh": [{"haus": 4.30, "wall": 0.59,
                                          "boil": 1.90, "light": 0.10}.get(key, 0.0)]})

    devs = [DeviceConfig(key=k, name=k, host="", parent=p)
            for k, p in (("haus", ""), ("wall", ""), ("boil", "haus"), ("light", "haus"))]

    # The display map is empty here — that is exactly the configuration Robert
    # runs, and it is why the diagram over-counted.
    assert net_display_children(devs) == {}
    assert flow_children(devs) == {"haus": ["boil", "light"]}

    gross = compute_sankey(DB(), devs, None, "today", net_children=net_display_children(devs))
    fixed = compute_sankey(DB(), devs, None, "today", net_children=flow_children(devs))
    assert abs(gross.total_consumption_kwh - 6.89) < 0.01, gross.total_consumption_kwh
    assert abs(fixed.total_consumption_kwh - 4.89) < 0.01, fixed.total_consumption_kwh

    disp = open(os.path.join(os.path.dirname(__file__), "..", "src", "shelly_analyzer",
                             "web", "action_dispatch.py"), encoding="utf-8").read()
    assert "_submap_sk = {} if _raw_sk else flow_children(" in disp, \
        "the sankey call site still uses the display-preference map"
    print("OK  the Sankey follows the wiring: 6.89 -> 4.89 kWh")


def test_per_device_views_keep_the_display_preference():
    """The opposite guard: subtracting a child from its parent's OWN series is a
    preference, and turning the wiring rule loose there would change what the
    Plots/Heatmap/Live tiles show without anyone asking."""
    disp = open(os.path.join(os.path.dirname(__file__), "..", "src", "shelly_analyzer",
                             "web", "action_dispatch.py"), encoding="utf-8").read()
    api = open(os.path.join(os.path.dirname(__file__), "..", "src", "shelly_analyzer",
                            "web", "blueprints", "api_state.py"), encoding="utf-8").read()
    assert disp.count("net_display_children") >= 2, \
        "the per-device views no longer use the opt-in map"
    assert "net_display_children" in api
    assert "flow_children" not in api, "a tile changed to the wiring rule unasked"
    print("OK  per-device views still honour the opt-in display flag")


if __name__ == "__main__":
    test_config_roundtrip_flag()
    test_children_map()
    test_live_subtraction_basic()
    test_live_subtraction_chain()
    test_empty_and_raw_are_noops()
    test_history_subtraction()
    test_history_subtraction_gap()
    test_household_keys_counts_each_meter_once()
    test_costs_summary_excludes_submeters()
    test_sankey_follows_the_wiring_not_the_display_flag()
    test_per_device_views_keep_the_display_preference()
    print("\nAll net-display tests passed.")
