"""Meter cascade: config roundtrip + generic meter calibration.

Run: python3 tests/test_meter_cascade.py  (no pytest dependency)

Covers the fix for the "calibrate against a single sub-meter is meaningless when
several sub-meters share one main meter" problem: a main meter is calibrated
against the SUM of its DIRECT measured (em) children, so sub-sub meters (switches
behind a sub-meter) are never double counted.
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pandas as pd  # noqa: E402
from flask import Flask  # noqa: E402

from shelly_analyzer.io.config import (  # noqa: E402
    AppConfig, DeviceConfig, MainMeter, load_config, save_config,
)
from shelly_analyzer.web.blueprints import devices as devmod  # noqa: E402


def test_config_roundtrip():
    import json
    d = tempfile.mkdtemp()
    p = os.path.join(d, "config.json")
    # Legacy config: no main_meters / no parent → must load with safe defaults.
    json.dump({"version": "16.0.0",
               "devices": [{"key": "s1", "name": "Haus", "host": "1.1.1.1", "kind": "em"}]},
              open(p, "w"))
    cfg = load_config(p)
    assert cfg.devices[0].parent == ""
    assert list(cfg.main_meters) == []
    # New config with hierarchy → survives save + reload unchanged.
    cfg2 = AppConfig(
        main_meters=[MainMeter(id="grid", name="Hauptzähler", serial="1EMH-1")],
        devices=[
            DeviceConfig(key="s1", name="Haus", host="1.1.1.1", kind="em", parent="grid"),
            DeviceConfig(key="s2", name="Wallbox", host="1.1.1.2", kind="em", parent="grid"),
            DeviceConfig(key="s3", name="Boiler", host="1.1.1.3", kind="switch", parent="s1"),
        ],
    )
    save_config(cfg2, p)
    back = load_config(p)
    assert {m.id: (m.name, m.serial) for m in back.main_meters} == {"grid": ("Hauptzähler", "1EMH-1")}
    assert {x.key: x.parent for x in back.devices} == {"s1": "grid", "s2": "grid", "s3": "s1"}
    print("OK  config roundtrip (legacy defaults + hierarchy persistence)")


class _FakeDB:
    """Raw kWh per device, optionally per-interval: kwh[dkey] is either a scalar
    (same for any interval) or a dict {(t0, t1): kwh}."""
    def __init__(self, kwh):
        self.kwh = kwh

    def query_hourly(self, dkey, start_ts=None, end_ts=None, compensate=True):
        v = self.kwh.get(dkey, 0.0)
        if isinstance(v, dict):
            v = v.get((start_ts, end_ts), 0.0)
        return pd.DataFrame({"kwh": [v]})


class _FakeState:
    def __init__(self, cfg, db, path):
        self.cfg = cfg
        self.storage = type("S", (), {"db": db})()
        self.lang = "de"
        self._cfg_path = path
        self._bg = None

    def reload_config(self, cfg):
        pass


def _app(cfg, db):
    state = _FakeState(cfg, db, os.path.join(tempfile.mkdtemp(), "config.json"))
    app = Flask(__name__)
    app.register_blueprint(devmod.bp)
    app.extensions["state"] = state
    return app, state


def test_calibrate_meter_sums_direct_children():
    cfg = AppConfig(
        main_meters=[MainMeter(id="grid", name="Hauptzähler")],
        devices=[
            DeviceConfig(key="s1", name="Haus", host="", kind="em", parent="grid"),
            DeviceConfig(key="s2", name="Wallbox", host="", kind="em", parent="grid"),
            DeviceConfig(key="s3", name="Boiler", host="", kind="switch", parent="s1"),
            DeviceConfig(key="s4", name="Weihnacht", host="", kind="switch", parent="s1"),
        ],
    )
    db = _FakeDB({"s1": 185.5, "s2": 86.5, "s3": 40.0, "s4": 5.0})  # sum(em)=272
    app, state = _app(cfg, db)
    with app.test_client() as c:
        # Real case: meter Δ 272 = Haus + Wallbox → ~0 %.
        j = c.post("/api/compensation/calibrate_meter", json={
            "meter_id": "grid", "start": 1000, "end": 2000,
            "meter_start": 0, "meter_end": 272, "effective_from": 1500}).get_json()
        assert j["ok"] and abs(j["percent"]) < 0.01, j
        assert set(j["children"]) == {"s1", "s2"}, j["children"]  # switches excluded
        # Meter 300 vs 272 → +10.29 %, applied to BOTH em children, switches untouched.
        exp = (300 / 272 - 1) * 100
        j = c.post("/api/compensation/calibrate_meter", json={
            "meter_id": "grid", "start": 1000, "end": 2000,
            "meter_start": 0, "meter_end": 300, "effective_from": 1500}).get_json()
        assert abs(j["percent"] - exp) < 0.01, j
        comp = {d.key: d.compensation_percent for d in state.cfg.devices}
        assert abs(comp["s1"] - exp) < 0.01 and abs(comp["s2"] - exp) < 0.01, comp
        assert comp["s3"] == 0.0 and comp["s4"] == 0.0, comp
        # Meter with no direct em children → clean error, no nonsense.
        j = c.post("/api/compensation/calibrate_meter", json={
            "meter_id": "nope", "start": 1000, "end": 2000,
            "meter_start": 0, "meter_end": 100, "effective_from": 1500}).get_json()
        assert not j["ok"], j
    print("OK  calibrate_meter sums direct em children only (no double count)")


def test_meter_crud():
    cfg = AppConfig(devices=[DeviceConfig(key="s1", name="Haus", host="", kind="em")])
    app, state = _app(cfg, _FakeDB({}))
    with app.test_client() as c:
        mid = c.post("/api/meters", json={"name": "Hauptzähler", "serial": "AB1"}).get_json()["id"]
        assert any(m.id == mid for m in state.cfg.main_meters)
        c.put("/api/devices/s1", json={"parent": mid})
        assert next(d for d in state.cfg.devices if d.key == "s1").parent == mid
        c.delete(f"/api/meters/{mid}")
        assert not any(m.id == mid for m in state.cfg.main_meters)
        # Child detached, not orphaned to a dead parent.
        assert next(d for d in state.cfg.devices if d.key == "s1").parent == ""
    print("OK  meter CRUD + parent assignment + detach-on-delete")


def test_reading_log():
    """Reading log: single reading = baseline (no factor); a second reading derives
    a factor for the interval on both em children; an OLDER reading inserted later
    creates a factor for the earlier interval too. Config roundtrips readings."""
    from shelly_analyzer.io.config import MeterReading
    cfg = AppConfig(
        main_meters=[MainMeter(id="grid", name="Hauptzähler")],
        devices=[
            DeviceConfig(key="s1", name="Haus", host="", kind="em", parent="grid"),
            DeviceConfig(key="s2", name="Wallbox", host="", kind="em", parent="grid"),
        ],
    )
    # Interval (100,200): Haus 120 + Wallbox 80 = 200 raw. (50,100): 50+50 = 100 raw.
    db = _FakeDB({
        "s1": {(100, 200): 120.0, (50, 100): 50.0},
        "s2": {(100, 200): 80.0, (50, 100): 50.0},
    })
    app, state = _app(cfg, db)
    with app.test_client() as c:
        # 1) single reading = baseline, no factor
        j = c.post("/api/meters/grid/reading", json={"ts": 100, "kwh": 1000.0}).get_json()
        assert j["ok"] and j["readings"] == 1, j
        comp = {d.key: d.compensation_percent for d in state.cfg.devices}
        assert comp["s1"] == 0.0 and comp["s2"] == 0.0, ("baseline should not set a factor", comp)
        # 2) second reading at 200: meter delta 1210-1000=210 vs raw 200 → +5 %
        j = c.post("/api/meters/grid/reading", json={"ts": 200, "kwh": 1210.0}).get_json()
        assert j["ok"] and j["readings"] == 2, j
        for d in state.cfg.devices:
            e = [x for x in d.compensation_history if int(x.effective_from_ts) == 100]
            assert len(e) == 1 and abs(e[0].percent - 5.0) < 0.01, (d.key, d.compensation_history)
            # single interval → weighted pre-factor equals that interval (+5 %), at ts=1
            pre = [x for x in d.compensation_history if str(x.note).endswith(":pre")]
            assert len(pre) == 1 and int(pre[0].effective_from_ts) == 1, (d.key, pre)
            assert abs(pre[0].percent - 5.0) < 0.01, pre[0].percent
        # 3) insert an OLDER reading at 50: meter delta 1000-900=100 vs raw 100 → 0 %
        j = c.post("/api/meters/grid/reading", json={"ts": 50, "kwh": 900.0}).get_json()
        assert j["ok"] and j["readings"] == 3, j
        for d in state.cfg.devices:
            real = [x for x in d.compensation_history if not str(x.note).endswith(":pre")]
            got = sorted(int(x.effective_from_ts) for x in real)
            assert got == [50, 100], ("both intervals present", d.key, got)
            e0 = [x for x in real if int(x.effective_from_ts) == 50][0]
            assert abs(e0.percent - 0.0) < 0.01, e0.percent
            # weighted overall: total meter Δ (1210-900=310) / total raw (100+200=300) → +3.333 %
            pre = [x for x in d.compensation_history if str(x.note).endswith(":pre")][0]
            assert int(pre.effective_from_ts) == 1, pre.effective_from_ts
            assert abs(pre.percent - (310.0 / 300.0 - 1.0) * 100.0) < 0.01, pre.percent
        # readings survive save + reload
        cfg2 = load_config(state._cfg_path)
        rd = {int(r.ts): r.kwh for r in cfg2.main_meters[0].readings}
        assert rd == {50: 900.0, 100: 1000.0, 200: 1210.0}, rd
        # delete the middle reading → intervals recompute (now just [50,200])
        j = c.delete("/api/meters/grid/reading/100").get_json()
        assert j["ok"] and j["readings"] == 2, j
    print("OK  reading log: baseline, derived factor, back-dated insert, roundtrip, delete")


if __name__ == "__main__":
    test_config_roundtrip()
    test_calibrate_meter_sums_direct_children()
    test_meter_crud()
    test_reading_log()
    print("\nALL METER-CASCADE TESTS PASSED")
