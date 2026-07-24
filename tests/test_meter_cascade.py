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
    def __init__(self, kwh):
        self.kwh = kwh

    def query_hourly(self, dkey, start_ts=None, end_ts=None):
        return pd.DataFrame({"kwh": [self.kwh.get(dkey, 0.0)]})


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


if __name__ == "__main__":
    test_config_roundtrip()
    test_calibrate_meter_sums_direct_children()
    test_meter_crud()
    print("\nALL METER-CASCADE TESTS PASSED")
