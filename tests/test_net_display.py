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
    # Mike's live numbers: Haus tile reads house+wallbox; wallbox its own.
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


if __name__ == "__main__":
    test_config_roundtrip_flag()
    test_children_map()
    test_live_subtraction_basic()
    test_live_subtraction_chain()
    test_empty_and_raw_are_noops()
    test_history_subtraction()
    test_history_subtraction_gap()
    print("\nAll net-display tests passed.")
