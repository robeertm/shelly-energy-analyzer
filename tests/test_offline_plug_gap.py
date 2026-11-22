"""An unreachable plug is a gap, not a plug drawing 0 W (GitHub issue #23).

A plug behind a Wi-Fi extender was really offline for hours. The history
still got one row per minute with ``total_power = 0.0`` and
``a_voltage = NULL``, and every ~43 s the log said "Live poll recovered"
although the plug never answered.

Both came from one place: ``get_switch_status()`` could not fail. When
``Switch.GetStatus`` and both fallbacks failed it RETURNED
``{"error": ...}``. The poller parsed that as 0 W / 0 V, counted it as a
success (hence "recovered"), and the minute history averaged the zeros in
(hence 0 W, and NULL voltage because the mean was 0 V; a flapping minute got
~160 V from two good readings and one failed poll).

Meters were never affected (their RPC raises), and the existing outage test
only drove ``kind="em"`` -- which is why nobody saw it.

What is pinned here:

* the transport: a dead plug RAISES (with the real cause), it no longer burns
  three rounds of retries on the same dead host, and every readable answer
  still comes through unchanged (Gen2, Gen1 fallback, odd firmware);
* the poller: a dead plug yields no sample, says "failed", never "recovered";
  "recovered" appears once, after a real answer;
* the history: no sample means no minute row, and a minute with one good
  reading is not dragged towards 0 V;
* the live view: ``/api/state`` flags a stale device as offline (a fresh,
  a just-added and a synthetic PV/battery device never), the card shows
  "Offline" and no power figure, and the Aurora "now" sum leaves it out.
"""
import json
import logging
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.io import http as http_mod  # noqa: E402
from shelly_analyzer.io.config import DeviceConfig, DownloadConfig  # noqa: E402
from shelly_analyzer.io.http import HttpConfig, ShellyHttp, get_switch_status  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
WEBDASH = ROOT / "src" / "shelly_analyzer" / "services" / "webdash.py"


# ── transport ────────────────────────────────────────────────────────────


def _client():
    return ShellyHttp(HttpConfig(timeout_seconds=0.5, retries=1, backoff_base_seconds=0.0))


class _Resp:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status_code = status

    def json(self):
        return self._payload


def _patch_requests(monkeypatch, answers):
    """``answers``: url-suffix -> payload, or an exception instance to raise."""
    calls = []

    def fake(self, method, url, params=None, json_body=None, headers=None):
        calls.append(url)
        for suffix, ans in answers.items():
            if url.endswith(suffix):
                if isinstance(ans, BaseException):
                    raise ans
                return _Resp(ans)
        raise requests.exceptions.HTTPError("404 for " + url)

    monkeypatch.setattr(ShellyHttp, "_do_request", fake)
    return calls


def test_unreachable_plug_raises_instead_of_returning_an_error_dict(monkeypatch):
    calls = _patch_requests(monkeypatch, {
        "/rpc/Switch.GetStatus": requests.exceptions.ConnectionError("could not connect"),
        "/rpc/Shelly.GetStatus": requests.exceptions.ConnectionError("could not connect"),
        "/status": requests.exceptions.ConnectionError("could not connect"),
    })
    with pytest.raises(requests.exceptions.ConnectionError) as ei:
        get_switch_status(_client(), "10.0.0.9", 0)
    assert "could not connect" in str(ei.value)       # the real cause, not a stand-in
    # A dead host is not asked three different ways: the fallbacks go to the
    # SAME host and only tripled the time until the outage was reported.
    assert calls == ["http://10.0.0.9/rpc/Switch.GetStatus"], calls


def test_timeout_is_not_retried_through_the_fallbacks(monkeypatch):
    calls = _patch_requests(monkeypatch, {
        "/rpc/Switch.GetStatus": requests.exceptions.ReadTimeout("timed out"),
    })
    with pytest.raises(requests.exceptions.Timeout):
        get_switch_status(_client(), "10.0.0.9", 0)
    assert len(calls) == 1, calls


def test_every_path_failing_raises_never_returns_a_fake_reading(monkeypatch):
    # Reachable, but nothing readable anywhere (HTTP errors / junk shapes).
    _patch_requests(monkeypatch, {
        "/rpc/Switch.GetStatus": requests.exceptions.HTTPError("500"),
        "/rpc/Shelly.GetStatus": {"sys": {"uptime": 5}},
        "/status": {"wifi_sta": {"connected": True}},
    })
    with pytest.raises(requests.exceptions.HTTPError):
        get_switch_status(_client(), "10.0.0.9", 0)


def test_error_payload_alone_is_not_a_reading(monkeypatch):
    _patch_requests(monkeypatch, {
        "/rpc/Switch.GetStatus": {"error": {"code": -105, "message": "Argument 'id' not found"}},
    })
    with pytest.raises(RuntimeError, match="no readable switch status"):
        get_switch_status(_client(), "10.0.0.9", 3)


def test_real_answers_still_come_through_unchanged(monkeypatch):
    real = {"id": 0, "output": True, "apower": 512.3, "voltage": 231.4,
            "current": 2.31, "aenergy": {"total": 1234.5}}
    _patch_requests(monkeypatch, {"/rpc/Switch.GetStatus": real})
    assert get_switch_status(_client(), "h", 0) == real

    # Odd firmware: measurements but no state key -> still a reading.
    measure_only = {"id": 0, "apower": 7.0, "voltage": 230.0}
    _patch_requests(monkeypatch, {"/rpc/Switch.GetStatus": measure_only})
    assert get_switch_status(_client(), "h", 0) == measure_only

    # Gen2 fallback through Shelly.GetStatus.
    _patch_requests(monkeypatch, {
        "/rpc/Switch.GetStatus": requests.exceptions.HTTPError("500"),
        "/rpc/Shelly.GetStatus": {"switch:0": {"output": False, "apower": 0.0, "voltage": 229.9}},
    })
    got = get_switch_status(_client(), "h", 0)
    assert got["output"] is False and got["voltage"] == 229.9

    # Gen1: /rpc is a 404, /status has the relay.
    _patch_requests(monkeypatch, {
        "/status": {"relays": [{"ison": True}], "meters": [{"power": 40.0}]},
    })
    got = get_switch_status(_client(), "h", 0)
    assert got["ison"] is True and got["_source"] == "/status"


# ── poller ───────────────────────────────────────────────────────────────


class _Collect(logging.Handler):
    def __init__(self):
        super().__init__()
        self.lines = []

    def emit(self, record):
        self.lines.append(record.getMessage())


def _dead_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()   # nothing listens -> connection refused, fast
    return port


def _run_poller(dev, seconds):
    from shelly_analyzer.services.live import MultiLivePoller

    log = logging.getLogger("shelly_analyzer.services.live")
    h = _Collect()
    log.addHandler(h)
    old = log.level
    log.setLevel(logging.INFO)
    p = MultiLivePoller(
        [dev],
        DownloadConfig(timeout_seconds=0.5, retries=1, backoff_base_seconds=0.0),
        poll_seconds=0.2,
    )
    try:
        p.start()
        time.sleep(seconds)
    finally:
        p.stop()
        log.removeHandler(h)
        log.setLevel(old)
    samples = []
    while not p.samples.empty():
        samples.append(p.samples.get_nowait())
    return samples, h.lines


def test_dead_plug_gives_no_sample_and_never_says_recovered():
    dev = DeviceConfig(key="plug", name="Utility room", host=f"127.0.0.1:{_dead_port()}",
                       kind="switch", em_id=0)
    samples, lines = _run_poller(dev, 2.5)
    assert samples == [], [s.power_w for s in samples]
    assert any("Live poll failed for Utility room" in ln for ln in lines), lines
    assert not any("recovered" in ln for ln in lines), lines


def test_recovered_only_after_a_real_answer(monkeypatch):
    from shelly_analyzer.services import live as live_mod

    state = {"n": 0}

    def flaky(client, host, sid):
        state["n"] += 1
        if state["n"] <= 3:
            raise requests.exceptions.ConnectionError("could not connect")
        return {"output": True, "apower": 120.0, "voltage": 230.0, "current": 0.6}

    monkeypatch.setattr(live_mod, "get_switch_status", flaky)
    dev = DeviceConfig(key="plug", name="Utility room", host="10.0.0.9", kind="switch", em_id=0)
    samples, lines = _run_poller(dev, 4.0)
    rec = [ln for ln in lines if "recovered" in ln]
    assert len(rec) == 1, lines
    assert samples and all(s.power_w["total"] == 120.0 for s in samples)
    assert all(s.voltage_v["a"] == 230.0 for s in samples)


# ── minute history ───────────────────────────────────────────────────────


def test_a_minute_with_one_good_reading_is_not_pulled_towards_zero_volts(tmp_path):
    """With the fix the failed polls of a flapping minute produce no sample,
    so the minute is the mean of the real readings only -- not 159.53 V."""
    from shelly_analyzer.io.storage import Storage
    from shelly_analyzer.services.live_history import LiveHistoryRecorder

    db = Storage(tmp_path).db
    rec = LiveHistoryRecorder(db)
    t0 = 1_800_000_000 - 1_800_000_000 % 60
    rec.observe("plug", t0 + 5, 40.0, 239.0, 0.2)
    rec.observe("plug", t0 + 35, 42.0, 240.0, 0.2)
    # ...the plug drops out; nothing is observed for the next 10 minutes...
    rec.observe("plug", t0 + 660, 41.0, 238.0, 0.2)
    rec.flush()
    con = sqlite3.connect(str(tmp_path / "energy.db"))
    try:
        rows = con.execute(
            "SELECT timestamp, total_power, a_voltage FROM samples "
            "WHERE device_key='plug' ORDER BY timestamp").fetchall()
    finally:
        con.close()
    stamps = [r[0] for r in rows]
    assert stamps == [t0, t0 + 660], rows                  # a gap, no 0-W minutes
    assert abs(rows[0][2] - 239.5) < 1e-6, rows            # not dragged to ~160 V
    assert all(r[1] > 0 for r in rows), rows


# ── /api/state ───────────────────────────────────────────────────────────


class _State:
    def __init__(self, cfg, meta, store):
        self.cfg = cfg
        self.devices_meta = meta
        self.live_store = store
        self.lang = "en"


def _state_app(poll_seconds=1.0, ages=None):
    from flask import Flask
    from shelly_analyzer.io.config import AppConfig, UiConfig
    from shelly_analyzer.services.webdash import LivePoint, LiveStateStore
    from shelly_analyzer.web.blueprints import api_state as mod

    cfg = AppConfig(
        devices=[
            DeviceConfig(key="fresh", name="Fridge", host="", kind="switch"),
            DeviceConfig(key="gone", name="Utility room", host="", kind="switch"),
            DeviceConfig(key="newdev", name="New plug", host="", kind="switch"),
        ],
        ui=UiConfig(live_poll_seconds=poll_seconds),
    )
    meta = [{"key": d.key, "name": d.name, "kind": d.kind} for d in cfg.devices]
    store = LiveStateStore()
    now = int(time.time())
    for key, age in (ages or {"fresh": 2, "gone": 600, "pv": 3600}).items():
        store.update(key, LivePoint(ts=now - age, power_total_w=35.0, va=230.0, vb=0.0,
                                    vc=0.0, ia=0.15, ib=0.0, ic=0.0,
                                    raw={"output": True}))
    app = Flask(__name__)
    app.register_blueprint(mod.bp)
    app.extensions["state"] = _State(cfg, meta, store)
    return app


def _devices(app):
    with app.test_client() as c:
        j = c.get("/api/state").get_json()
    return {d["key"]: d for d in j["devices"]}


def test_api_state_flags_a_stale_device_as_offline():
    devs = _devices(_state_app())
    assert devs["gone"]["offline"] is True
    assert 590 <= devs["gone"]["sample_age_s"] <= 620
    assert devs["gone"]["appliances"] == []          # no appliance guess from a stale reading
    assert devs["fresh"]["offline"] is False and devs["fresh"]["sample_age_s"] < 30
    # Just added, no reading yet: a placeholder, not "offline".
    assert devs["newdev"].get("pending") is True and devs["newdev"]["offline"] is False
    # The external PV source has its own rhythm and is never a Shelly "offline".
    assert devs["pv"]["offline"] is False


def test_slow_poll_interval_stretches_the_offline_limit():
    from shelly_analyzer.web.blueprints.api_state import LIVE_STALE_AFTER_S, live_stale_after_s

    assert live_stale_after_s(1.0) == LIVE_STALE_AFTER_S == 120.0
    assert live_stale_after_s(60.0) == 240.0
    assert live_stale_after_s("junk") == 120.0
    devs = _devices(_state_app(poll_seconds=60.0, ages={"gone": 200}))
    assert devs["gone"]["offline"] is False


# ── live card + Aurora sum ───────────────────────────────────────────────


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


def test_live_card_shows_offline_and_no_power_figure_in_node():
    node = shutil.which("node")
    if not node:
        pytest.skip("node not installed")
    s = WEBDASH.read_text(encoding="utf-8")
    fns = "\n".join(_js_function(s, n) for n in ("_pwrText", "_offBadgeTitle", "_offBadgeHTML"))
    prog = r"""
const I18N = {"live.cards.offline": "Hors ligne",
              "live.cards.offline_title": "Derniere mesure il y a {min} min."};
function t(k, fbOrVars, maybeVars) {
  let fb = (typeof fbOrVars === 'string') ? fbOrVars : undefined;
  let vars = (typeof fbOrVars === 'object') ? fbOrVars : maybeVars;
  let s = I18N[k] || fb || k;
  if (vars) Object.keys(vars).forEach(function(kk){ s = s.split('{'+kk+'}').join(String(vars[kk])); });
  return s;
}
function esc(x) { return String(x).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/"/g,'&quot;'); }
function fmt(v, n) { return Number(v).toFixed(n); }
function _pwrVal(d) { return d.power_w; }
""" + fns + r"""
console.log(JSON.stringify({
  off_power: _pwrText({power_w: 35, offline: true}),
  on_power: _pwrText({power_w: 35, offline: false}),
  badge: _offBadgeHTML({offline: true, sample_age_s: 610}),
  none: _offBadgeHTML({offline: false, sample_age_s: 3}),
}));
"""
    with tempfile.TemporaryDirectory() as tmp:
        f = Path(tmp) / "t.js"
        f.write_text(prog, encoding="utf-8")
        res = subprocess.run([node, str(f)], capture_output=True, text=True, timeout=30)
    assert res.returncode == 0, res.stderr
    out = json.loads(res.stdout)
    assert out["off_power"] == "—"           # no "0 W", no stale figure
    assert out["on_power"] == "35 W"
    assert "Hors ligne" in out["badge"] and "10 min" in out["badge"]
    assert 'class="off-badge"' in out["badge"]
    assert out["none"] == ""


def test_card_update_adds_and_removes_the_badge():
    s = WEBDASH.read_text(encoding="utf-8")
    upd = _js_function(s, "updateDeviceCard")
    assert "off-badge" in upd and "offEl.remove()" in upd
    assert "classList.toggle('dev-offline'" in upd
    assert "d.offline ? 'pwr-off'" in upd                # no load colour on the dash
    assert "_offBadgeHTML(d)" in _js_function(s, "devCardHTML")


def test_aurora_now_sum_leaves_an_offline_device_out():
    js = (ROOT / "src" / "shelly_analyzer" / "web" / "static" / "aurora.js").read_text(encoding="utf-8")
    assert "var w = d.offline ? 0 : (Number(d.power_w) || 0);" in js
    assert "if (!d.offline && (!out.top" in js           # never the "biggest draw"
    css = (ROOT / "src" / "shelly_analyzer" / "web" / "static" / "aurora.css").read_text(encoding="utf-8")
    assert '.off-badge' in css


def test_offline_texts_exist_in_every_language():
    from shelly_analyzer.i18n import t

    langs = ("en", "de", "es", "fr", "pt", "it", "pl", "cs", "ru")
    for lang in langs:
        assert t(lang, "live.cards.offline") not in ("", "live.cards.offline"), lang
        title = t(lang, "live.cards.offline_title")
        assert "{min}" in title and title != "live.cards.offline_title", lang
    assert len({t(lang, "live.cards.offline_title") for lang in langs}) == len(langs)
