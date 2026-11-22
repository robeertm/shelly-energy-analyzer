"""Device status in the first minutes after a start (GitHub issue #23, second follow-up).

17.11.7 stopped a false "Offline" right after a restart: the live view is
restored from a cache written before the restart, so every reading in it is
old until the poller has answered once. It did so by switching the age check
off for the first ``max(120 s, 4 x poll interval)`` -- and during that grace an
old cached reading counted as current. The reporter retested 17.11.8 with a
plug that did not answer after a restart:

* Settings -> Devices said "Online" (``/api/devices`` -> ``online: true``);
* the Live card showed the last reading from before the restart (0 W, the
  device behind the plug was already off) without any badge.

A cached reading always predates the start of the process; a reading from the
running poller never does. So during the grace a device whose newest reading
is older than the start is *waiting* -- the third state a device already had
after a Host/IP change: "Waiting for the first reading" in Settings, a grey
"Waiting" badge and no power figure on the Live card, and it is left out of
the Aurora "Right now" total and "Biggest draw". After the grace the stale rule
takes over as before, so there is still no false "Offline" right after a start.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.io.config import DeviceConfig  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
WEBDASH = ROOT / "src" / "shelly_analyzer" / "services" / "webdash.py"
AURORA_JS = ROOT / "src" / "shelly_analyzer" / "web" / "static" / "aurora.js"
AURORA_CSS = ROOT / "src" / "shelly_analyzer" / "web" / "static" / "aurora.css"
LANGS = ("en", "de", "es", "fr", "pt", "it", "pl", "cs", "ru")


class _State:
    def __init__(self, cfg, meta, store):
        self.cfg = cfg
        self.devices_meta = meta
        self.live_store = store
        self.lang = "en"
        self._cfg_path = None


def _app(ages, extra_meta=(), pv_source=False):
    from flask import Flask
    from shelly_analyzer.io.config import AppConfig, UiConfig
    from shelly_analyzer.services.webdash import LivePoint, LiveStateStore
    from shelly_analyzer.web.blueprints import api_state, devices

    cfg = AppConfig(
        devices=[
            DeviceConfig(key="plug", name="Utility room", host="192.0.2.53", kind="switch"),
            DeviceConfig(key="newdev", name="New plug", host="192.0.2.30", kind="switch"),
        ],
        ui=UiConfig(live_poll_seconds=1.0),
    )
    meta = [{"key": d.key, "name": d.name, "kind": d.kind} for d in cfg.devices]
    meta += list(extra_meta)
    store = LiveStateStore()
    now = int(time.time())
    for key, age in ages.items():
        store.update(key, LivePoint(ts=now - age, power_total_w=35.0, va=230.0, vb=0.0,
                                    vc=0.0, ia=0.15, ib=0.0, ic=0.0, raw={"output": True}))
    app = Flask(__name__)
    app.register_blueprint(api_state.bp)
    app.register_blueprint(devices.bp)
    app.extensions["state"] = _State(cfg, meta, store)
    return app


def _listed(app):
    with app.test_client() as c:
        return {d["key"]: d for d in c.get("/api/devices").get_json()["devices"]}


def _live(app):
    with app.test_client() as c:
        return {d["key"]: d for d in c.get("/api/state").get_json()["devices"]}


def _started(monkeypatch, ago):
    from shelly_analyzer.web.blueprints import api_state as mod
    monkeypatch.setattr(mod, "_PROCESS_START_S", time.time() - ago)


# ── Settings -> Devices (/api/devices) ────────────────────────────────────


def test_cached_reading_in_the_grace_is_waiting_not_online(monkeypatch):
    _started(monkeypatch, 5)
    plug = _listed(_app({"plug": 600}))["plug"]
    assert plug["online"] is False            # was True before the fix
    assert plug["waiting"] is True
    assert 590 <= plug["sample_age_s"] <= 620


def test_first_answer_after_the_start_is_online(monkeypatch):
    _started(monkeypatch, 5)
    plug = _listed(_app({"plug": 0}))["plug"]
    assert plug["online"] is True and plug["waiting"] is False


def test_after_the_grace_the_stale_rule_takes_over(monkeypatch):
    _started(monkeypatch, 121)
    devs = _listed(_app({"plug": 600}))
    assert devs["plug"]["online"] is False and devs["plug"]["waiting"] is False
    # Never answered at all: no data, not waiting any more.
    assert devs["newdev"]["online"] is False and devs["newdev"]["waiting"] is False


def test_never_answered_device_waits_during_the_grace(monkeypatch):
    _started(monkeypatch, 5)
    newdev = _listed(_app({"plug": 0}))["newdev"]
    assert newdev["online"] is False and newdev["waiting"] is True
    assert newdev["sample_age_s"] is None


# ── Live tab (/api/state) ─────────────────────────────────────────────────


def test_live_card_is_waiting_not_offline_in_the_grace(monkeypatch):
    _started(monkeypatch, 5)
    meta_pv = [{"key": "pv", "name": "", "kind": "em"}]
    live = _live(_app({"plug": 600, "pv": 3600}, extra_meta=meta_pv))
    assert live["plug"]["waiting"] is True    # missing before the fix
    assert live["plug"]["offline"] is False   # still no false "Offline"
    assert live["plug"]["appliances"] == []
    # Configured but no sample yet: placeholder card, waiting as well.
    assert live["newdev"]["pending"] is True and live["newdev"]["waiting"] is True
    # Pushed external values have no age rule.
    assert live["pv"]["waiting"] is False and live["pv"]["offline"] is False


def test_live_card_after_the_grace_is_offline(monkeypatch):
    _started(monkeypatch, 3600)
    live = _live(_app({"plug": 600}))
    assert live["plug"]["offline"] is True and live["plug"]["waiting"] is False


def test_settings_and_live_tab_agree(monkeypatch):
    for ago in (5, 3600):
        _started(monkeypatch, ago)
        app = _app({"plug": 600})
        listed, live = _listed(app)["plug"], _live(app)["plug"]
        assert listed["waiting"] is live["waiting"], ago
        assert listed["online"] is (not live["offline"] and not live["waiting"]), ago


# ── the rule itself ──────────────────────────────────────────────────────


def test_awaiting_first_reading_rule(monkeypatch):
    from shelly_analyzer.web.blueprints import api_state as mod
    now = time.time()
    monkeypatch.setattr(mod, "_PROCESS_START_S", now - 5)
    f = mod.awaiting_first_reading
    assert f(None, 1.0, now) is True          # nothing yet, grace running
    assert f("junk", 1.0, now) is True
    assert f(0, 1.0, now) is True
    assert f(now - 600, 1.0, now) is True     # from the restart cache
    assert f(int(now), 1.0, now) is False     # answered after the start
    # A first answer in the very second of the start (points carry whole seconds).
    assert f(int(now - 5), 1.0, now) is False
    monkeypatch.setattr(mod, "_PROCESS_START_S", now - 3600)
    assert f(now - 600, 1.0, now) is False    # grace over
    assert f(0, 1.0, now) is False
    # Host change: readings before it wait while the new address has time.
    assert f(now - 30, 1.0, now, since=now - 5) is True
    assert f(now - 30, 1.0, now, since=now - 60) is False
    assert f(now - 1, 1.0, now, since=now - 5) is False
    # A slow poll interval stretches the start grace (max(120, 4 x 60) = 240 s).
    monkeypatch.setattr(mod, "_PROCESS_START_S", now - 200)
    assert f(now - 600, 60.0, now) is True
    assert f(now - 600, 1.0, now) is False


# ── live card in the browser ─────────────────────────────────────────────


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


def test_live_card_shows_waiting_and_no_power_figure_in_node():
    node = shutil.which("node")
    if not node:
        pytest.skip("node not installed")
    s = WEBDASH.read_text(encoding="utf-8")
    fns = "\n".join(_js_function(s, n) for n in
                    ("_pwrText", "_offBadgeTitle", "_waitBadgeTitle", "_offBadgeHTML"))
    prog = r"""
const I18N = {"live.cards.offline": "Offline",
              "live.cards.offline_title": "Last reading {min} min ago.",
              "live.cards.waiting": "Wartet",
              "live.cards.waiting_title": "Seit dem Start noch keine Antwort."};
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
  wait_power: _pwrText({power_w: 35, waiting: true}),
  live_power: _pwrText({power_w: 35}),
  wait_badge: _offBadgeHTML({waiting: true, sample_age_s: 600}),
  off_badge: _offBadgeHTML({offline: true, sample_age_s: 610}),
  none: _offBadgeHTML({sample_age_s: 1}),
}));
"""
    with tempfile.TemporaryDirectory() as tmp:
        f = Path(tmp) / "t.js"
        f.write_text(prog, encoding="utf-8")
        res = subprocess.run([node, str(f)], capture_output=True, text=True, timeout=30)
    assert res.returncode == 0, res.stderr
    out = json.loads(res.stdout)
    assert out["wait_power"] == "—"           # no cached figure passed off as live
    assert out["live_power"] == "35 W"
    assert 'class="off-badge wait"' in out["wait_badge"]
    assert "Wartet" in out["wait_badge"] and 'title="Seit dem Start' in out["wait_badge"]
    assert "⚠ Offline" in out["off_badge"] and "10 min" in out["off_badge"]
    assert out["none"] == ""


def test_card_update_and_aurora_treat_waiting_like_offline():
    s = WEBDASH.read_text(encoding="utf-8")
    upd = _js_function(s, "updateDeviceCard")
    assert "if (d.offline || d.waiting)" in upd and "'off-badge wait'" in upd
    assert "offEl.remove()" in upd
    assert "(d.offline || d.waiting) ? 'pwr-off'" in _js_function(s, "devCardHTML")
    assert "(d.offline || d.waiting) ? '\\u2014'" in _js_function(s, "_ctrlDeviceCard")
    assert ".off-badge.wait" in s
    js = AURORA_JS.read_text(encoding="utf-8")
    assert "var w = (d.offline || d.waiting) ? 0 : (Number(d.power_w) || 0);" in js
    assert "if (!d.offline && !d.waiting && (!out.top" in js
    assert "var lift = (d.offline || d.waiting) ? 0 :" in js
    assert ".off-badge.wait" in AURORA_CSS.read_text(encoding="utf-8")


def test_waiting_texts_exist_in_every_language():
    from shelly_analyzer.i18n import t
    for key in ("live.cards.waiting", "live.cards.waiting_title"):
        vals = {lang: t(lang, key) for lang in LANGS}
        for lang, v in vals.items():
            assert v and v != key, (key, lang)
        # Translated, not English copies (en is the reference).
        others = [v for lang, v in vals.items() if lang != "en"]
        assert vals["en"] not in others, key
