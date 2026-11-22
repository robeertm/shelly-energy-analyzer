"""Device status after an address change or a restart (GitHub issue #23, follow-up).

After 17.11.7 had fixed the 0 W rows, the reporter tested an outage by
changing a plug's Host/IP in Settings -> Devices to an address where nothing
answers. Two things were wrong on screen:

* the device card in Settings kept saying "Online". ``/api/devices`` called a
  device online as soon as *any* reading of it sat in the live buffer -- that
  buffer holds two hours and is restored after a restart. The age rule the
  Live tab got in 17.11.6 was never applied there. The last reading also came
  from the *old* address, so even a fresh one proved nothing about the new;
* right after a restart the Live card said "Switch: Off" for a plug that was
  on. The restart cache stores measurements, not the switch state, so the
  state is unknown until the first answer -- and the card had only two
  branches, so "unknown" was printed as "Off".

Measured with a simulated plug: after saving, the old address received no
further request -- the poller did switch. A third problem turned up on the way:
every device save restarts the background services, and the restart cleared
the stop signal before the old live-feed thread had seen it, so each save left
one more feed thread running. That is pinned here too.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.io.config import DeviceConfig  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
WEBDASH = ROOT / "src" / "shelly_analyzer" / "services" / "webdash.py"
SETTINGS = ROOT / "src" / "shelly_analyzer" / "web" / "templates" / "settings.html"


@pytest.fixture(autouse=True)
def _process_up_for_a_while(monkeypatch):
    from shelly_analyzer.web.blueprints import api_state as mod
    monkeypatch.setattr(mod, "_PROCESS_START_S", time.time() - 3600)


class _State:
    def __init__(self, cfg, meta, store):
        self.cfg = cfg
        self.devices_meta = meta
        self.live_store = store
        self.lang = "en"
        self._cfg_path = None

    def reload_config(self, cfg):
        self.cfg = cfg


def _app(poll_seconds=1.0, ages=None, tmp_cfg=None):
    from flask import Flask
    from shelly_analyzer.io.config import AppConfig, UiConfig
    from shelly_analyzer.services.webdash import LivePoint, LiveStateStore
    from shelly_analyzer.web.blueprints import api_state, devices

    cfg = AppConfig(
        devices=[
            DeviceConfig(key="fresh", name="Fridge", host="192.0.2.10", kind="switch"),
            DeviceConfig(key="gone", name="Utility room", host="192.0.2.53", kind="switch"),
            DeviceConfig(key="newdev", name="New plug", host="192.0.2.30", kind="switch"),
        ],
        ui=UiConfig(live_poll_seconds=poll_seconds),
    )
    meta = [{"key": d.key, "name": d.name, "kind": d.kind} for d in cfg.devices]
    store = LiveStateStore()
    now = int(time.time())
    for key, age in (ages if ages is not None else {"fresh": 2, "gone": 600}).items():
        store.update(key, LivePoint(ts=now - age, power_total_w=35.0, va=230.0, vb=0.0,
                                    vc=0.0, ia=0.15, ib=0.0, ic=0.0, raw={"output": True}))
    app = Flask(__name__)
    app.register_blueprint(api_state.bp)
    app.register_blueprint(devices.bp)
    st = _State(cfg, meta, store)
    if tmp_cfg is not None:
        st._cfg_path = tmp_cfg
    app.extensions["state"] = st
    return app


def _listed(app):
    with app.test_client() as c:
        j = c.get("/api/devices").get_json()
    return {d["key"]: d for d in j["devices"]}


def _live(app):
    with app.test_client() as c:
        j = c.get("/api/state").get_json()
    return {d["key"]: d for d in j["devices"]}


# ── /api/devices knows the age of the last reading ────────────────────────


def test_api_devices_does_not_call_a_stale_device_online():
    devs = _listed(_app())
    assert devs["fresh"]["online"] is True
    assert devs["fresh"]["sample_age_s"] < 30
    # Ten minutes without an answer: not online, and the age is reported.
    assert devs["gone"]["online"] is False
    assert 590 <= devs["gone"]["sample_age_s"] <= 620
    # Never answered: neither online nor an age.
    assert devs["newdev"]["online"] is False
    assert devs["newdev"]["sample_age_s"] is None


def test_restart_cache_is_not_an_outage(monkeypatch):
    from shelly_analyzer.web.blueprints import api_state as mod
    monkeypatch.setattr(mod, "_PROCESS_START_S", time.time() - 5)
    assert _listed(_app())["gone"]["online"] is True
    monkeypatch.setattr(mod, "_PROCESS_START_S", time.time() - 3600)
    assert _listed(_app())["gone"]["online"] is False


def test_slow_poll_interval_stretches_the_limit_in_settings_too():
    devs = _listed(_app(poll_seconds=60.0, ages={"gone": 200}))
    assert devs["gone"]["online"] is True        # limit max(120, 4 x 60) = 240 s


def test_one_rule_for_settings_and_live_tab():
    app = _app()
    listed, live = _listed(app), _live(app)
    for key in ("fresh", "gone"):
        assert listed[key]["online"] is (not live[key]["offline"]), key


# ── address change ────────────────────────────────────────────────────────


def _change_host(app, key, host):
    with app.test_client() as c:
        r = c.put("/api/devices/" + key, json={"host": host})
    assert r.get_json() == {"ok": True}


def test_reading_from_the_old_address_does_not_make_the_new_one_online(tmp_path, caplog):
    import logging
    app = _app(tmp_cfg=tmp_path / "config.json")
    assert _listed(app)["fresh"]["online"] is True
    with caplog.at_level(logging.INFO):
        _change_host(app, "fresh", "192.0.2.199")
    # The change is written and named in the log, old -> new.
    assert "192.0.2.10 -> 192.0.2.199" in caplog.text
    assert json.loads((tmp_path / "config.json").read_text())["devices"][0]["host"] == "192.0.2.199"
    # Right after saving: the 2 s old reading came from the old address.
    d = _listed(app)["fresh"]
    assert d["online"] is False and d["waiting"] is True
    # Live card: not offline yet -- the new address gets a moment to answer.
    assert _live(app)["fresh"]["offline"] is False


def test_new_address_that_never_answers_becomes_offline(tmp_path):
    from shelly_analyzer.web.blueprints.api_state import host_change_grace_s
    # Last answer of the old address 30 s ago -- well inside the 120 s limit,
    # so only the address change can make it "not live".
    app = _app(ages={"fresh": 30, "gone": 600}, tmp_cfg=tmp_path / "config.json")
    _change_host(app, "fresh", "192.0.2.199")
    st = app.extensions["state"]
    # Pretend the change was a while ago: the new address had its chance.
    st._host_changed_at["fresh"] = time.time() - host_change_grace_s(1.0) - 5
    d = _listed(app)["fresh"]
    assert d["online"] is False and d["waiting"] is False
    assert _live(app)["fresh"]["offline"] is True


def test_first_answer_from_the_new_address_is_online_again(tmp_path):
    from shelly_analyzer.services.webdash import LivePoint
    app = _app(tmp_cfg=tmp_path / "config.json")
    _change_host(app, "fresh", "192.0.2.199")
    st = app.extensions["state"]
    st._host_changed_at["fresh"] = time.time() - 1
    st.live_store.update("fresh", LivePoint(ts=int(time.time()) + 1, power_total_w=12.0,
                                            va=230.0, vb=0.0, vc=0.0, ia=0.05, ib=0.0,
                                            ic=0.0, raw={"output": True}))
    d = _listed(app)["fresh"]
    assert d["online"] is True and d["waiting"] is False


def test_renaming_alone_is_no_address_change(tmp_path):
    app = _app(tmp_cfg=tmp_path / "config.json")
    with app.test_client() as c:
        assert c.put("/api/devices/fresh", json={"name": "Kitchen"}).get_json()["ok"]
    assert _listed(app)["fresh"]["online"] is True


def test_reading_is_stale_helper():
    from shelly_analyzer.web.blueprints.api_state import reading_is_stale
    now = time.time()
    assert reading_is_stale(now - 600, 1.0, now) is True
    assert reading_is_stale(now - 2, 1.0, now) is False
    assert reading_is_stale(0, 1.0, now) is False
    assert reading_is_stale(None, 1.0, now) is False
    assert reading_is_stale("junk", 1.0, now) is False
    # before an address change, once the grace has passed
    assert reading_is_stale(now - 30, 1.0, now, since=now - 20) is True
    assert reading_is_stale(now - 30, 1.0, now, since=now - 3) is False
    assert reading_is_stale(now - 1, 1.0, now, since=now - 20) is False


# ── settings page refreshes the status in place ───────────────────────────


def test_settings_page_shows_waiting_and_refreshes_status():
    s = SETTINGS.read_text(encoding="utf-8")
    assert "function _devStatusHTML(d)" in s
    assert "settings.devcard.waiting" in s
    assert "data-devstatus=" in s and "data-devdot=" in s
    assert "setInterval(refreshDeviceStatus" in s
    # The old two-way status literal is gone.
    assert "${d.online?'<span style=\"color:var(--ok)\">●" not in s


# ── unknown switch state is not "Off" ─────────────────────────────────────


def _js_function(src, name):
    a = src.index("function %s(" % name)
    depth, i = 0, src.index("{{", a)
    while True:
        if src.startswith("{{", i):
            depth += 1; i += 2; continue
        if src.startswith("}}", i):
            depth -= 1; i += 2
            if depth == 0:
                break
            continue
        i += 1
    return src[a:i].replace("{{", "{").replace("}}", "}")


def test_unknown_switch_state_is_a_dash_in_node():
    node = shutil.which("node")
    if not node:
        pytest.skip("node not installed")
    s = WEBDASH.read_text(encoding="utf-8")
    fn = _js_function(s, "_switchStateHTML").replace("\\\\u2014", "\\u2014")
    prog = r"""
const I18N = {"live.switch.on": "Ein", "live.switch.off": "Aus",
              "live.switch.unknown_title": "Schalterzustand noch unbekannt"};
function t(k, fb) { return I18N[k] || fb || k; }
function esc(x) { return String(x).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/"/g,'&quot;'); }
""" + fn + r"""
console.log(JSON.stringify({
  on: _switchStateHTML({switch_on: true}),
  off: _switchStateHTML({switch_on: false}),
  nul: _switchStateHTML({switch_on: null}),
  none: _switchStateHTML({}),
}));
"""
    with tempfile.TemporaryDirectory() as tmp:
        f = Path(tmp) / "t.js"
        f.write_text(prog, encoding="utf-8")
        res = subprocess.run([node, str(f)], capture_output=True, text=True, timeout=30)
    assert res.returncode == 0, res.stderr
    out = json.loads(res.stdout)
    assert 'class="switch-state on"' in out["on"] and ">Ein<" in out["on"]
    assert 'class="switch-state off"' in out["off"] and ">Aus<" in out["off"]
    for k in ("nul", "none"):
        assert 'class="switch-state unknown"' in out[k], out[k]
        assert "—" in out[k] and "Aus" not in out[k]
        assert 'title="Schalterzustand noch unbekannt"' in out[k]


def test_cards_use_the_three_way_switch_state():
    s = WEBDASH.read_text(encoding="utf-8")
    card = _js_function(s, "devCardHTML")
    assert "_switchStateHTML(d)" in card
    assert "(d.switch_on ? 'on' : 'off')" not in card
    ctrl = _js_function(s, "_ctrlDeviceCard")
    assert "live.switch.unknown" in ctrl
    assert ".switch-state.unknown" in s


def test_new_texts_exist_in_every_language():
    from shelly_analyzer.i18n import t

    langs = ("en", "de", "es", "fr", "pt", "it", "pl", "cs", "ru")
    for lang in langs:
        assert t(lang, "live.switch.unknown") == "—", lang
        for key in ("live.switch.unknown_title", "settings.devcard.waiting"):
            assert t(lang, key) not in ("", key), (lang, key)
    for key in ("live.switch.unknown_title", "settings.devcard.waiting"):
        assert len({t(lang, key) for lang in langs}) == len(langs), key


# ── a device save must not leave an extra live-feed thread behind ─────────


class _FakePoller:
    def __init__(self, *a, **kw):
        import queue
        self.samples = queue.Queue()

    def start(self):
        pass

    def stop(self):
        pass


def test_device_save_does_not_leak_feed_threads(monkeypatch, tmp_path):
    from shelly_analyzer.io.config import AppConfig
    from shelly_analyzer.services import live as live_mod
    from shelly_analyzer.services.webdash import LiveStateStore
    from shelly_analyzer.web.background import BackgroundServiceManager

    monkeypatch.setattr(live_mod, "MultiLivePoller", _FakePoller)
    cfg = AppConfig(devices=[DeviceConfig(key="p", name="P", host="192.0.2.1", kind="switch")])
    mgr = BackgroundServiceManager(cfg, storage=None, live_store=LiveStateStore(), out_dir=tmp_path)
    # Only the live poller + feed thread matter here; everything else is a no-op.
    for name in dir(mgr):
        if name.startswith("_start_") and name != "_start_live_poller":
            monkeypatch.setattr(mgr, name, lambda *a, **k: None)
    for name in ("_init_nilm_learners", "_write_runtime_devices_meta",
                 "_flush_switch_history", "_save_live_history"):
        monkeypatch.setattr(mgr, name, lambda *a, **k: None)

    def feeders():
        return [t for t in threading.enumerate()
                if t.is_alive() and ("_feed_loop" in t.name or t.name.startswith("LiveFeed"))]

    before = len(feeders())
    mgr.start_all()
    try:
        for _ in range(3):           # three device saves
            mgr.reload(cfg)
        deadline = time.time() + 5
        while time.time() < deadline and len(feeders()) - before > 1:
            time.sleep(0.1)
        assert len(feeders()) - before == 1, [t.name for t in feeders()]
    finally:
        mgr.stop_all()
    deadline = time.time() + 5
    while time.time() < deadline and len(feeders()) > before:
        time.sleep(0.1)
    assert len(feeders()) == before
