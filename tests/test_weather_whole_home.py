"""The weather correlation is about the whole home, not the first device (17.11.3).

A home of eight plugs and no meter opened the weather tab and every
temperature zone read 0.004 kWh/h: the 4 W of the first plug in the list.
The action took ``cfg.devices[0]`` whenever no ``device_key`` was sent, and the
tab never sent one. Now the default is the sum of the consumers, each counted
once (the rule costs, CO2 and energy flow already follow), and the tab offers
a device list to look at one device on purpose. The kWh axes follow the size of
the values instead of fixed decimals that turned every tick into 0.00.
"""
import json
import os
import re
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.io.config import load_config  # noqa: E402
from shelly_analyzer.io.storage import Storage  # noqa: E402
from shelly_analyzer.services import weather as wx  # noqa: E402

LANGS = ("de", "en", "es", "fr", "pt", "it", "pl", "cs", "ru")
HOURS = 8
MIN = 60

# The first device in the list is the small one, like the 4 W fibre box that
# raised it; "strip" hangs behind it and is already inside its reading.
PLUGS = [("ont", 4.0, ""), ("fridge", 120.0, ""), ("heat", 800.0, ""), ("strip", 30.0, "ont")]


@pytest.fixture(autouse=True)
def _fresh_caches():
    wx._CURRENT_CACHE.clear()
    wx._HISTORY_DONE.clear()
    yield
    wx._CURRENT_CACHE.clear()
    wx._HISTORY_DONE.clear()


def _no_net(*_a, **_kw):
    raise requests.ConnectionError("offline in tests")


def _sw(key, parent=""):
    d = {"key": key, "name": key.capitalize(), "host": "192.0.2.%d" % (10 + len(key)),
         "kind": "switch", "supports_emdata": False}
    if parent:
        d["parent"] = parent
    return d


def _start_hour():
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return int((now - timedelta(hours=HOURS + 2)).timestamp())


def _fill(st, key, watts, h0):
    rows = []
    for m in range(HOURS * 60):
        wh = watts / 60.0
        rows.append({"timestamp": h0 + m * MIN, "a_act_power": watts, "b_act_power": 0.0,
                     "c_act_power": 0.0, "total_power": watts,
                     "a_total_act_energy": wh, "b_total_act_energy": 0.0,
                     "c_total_act_energy": 0.0, "energy_kwh": wh / 1000.0})
    st.db.insert_dataframe(key, pd.DataFrame(rows))


def _site(tmp, plugs=PLUGS, extra_devices=()):
    p = Path(tmp) / "config.json"
    devices = [_sw(k, parent) for k, _w, parent in plugs] + list(extra_devices)
    p.write_text(json.dumps({
        "devices": devices,
        "weather": {"enabled": True, "source": "open_meteo", "lat": 52.52, "lon": 13.41},
    }))
    cfg, st = load_config(p), Storage(Path(tmp) / "data")
    h0 = _start_hour()
    for k, w, _parent in plugs:
        _fill(st, k, w, h0)
    st.db.upsert_weather([(h0 + i * 3600, 2.0 + i * 2.5, 80.0, 3.0, 50.0, 1012.0, "Overcast", h0)
                          for i in range(HOURS)])
    from shelly_analyzer.web.action_dispatch import ActionDispatcher
    return ActionDispatcher(cfg, st, None, out_dir=Path(tmp), cfg_path=p, lang="en")


def _run(params):
    with tempfile.TemporaryDirectory() as tmp:
        ad = _site(tmp)
        try:
            with mock.patch("requests.get", side_effect=_no_net):
                return ad.dispatch("weather_correlation", params)
        finally:
            ad._warm_stop.set()


HOME_KWH_PER_H = (4.0 + 120.0 + 800.0) / 1000.0   # strip is inside ont


def test_default_is_the_whole_home_not_the_first_device():
    out = _run({})
    assert out["ok"] is True, out
    assert out["scope"] == "home"
    assert sorted(out["home_keys"]) == ["fridge", "heat", "ont"]
    assert out["device_count"] == 3
    c = out["correlation"]
    assert c is not None
    assert c["data_points"] == HOURS
    assert c["total_kwh"] == pytest.approx(HOME_KWH_PER_H * HOURS, rel=1e-3)
    # every paired hour is the sum of the three top-level plugs
    for p in out["paired"]:
        assert p["kwh"] == pytest.approx(HOME_KWH_PER_H, rel=1e-3)
    zones = {z["range"]: z for z in out["comfort_zones"]}
    assert zones and all(z["avg_kwh"] == pytest.approx(HOME_KWH_PER_H, rel=1e-3)
                         for z in zones.values())


def test_a_device_behind_another_is_not_added_twice():
    out = _run({})
    assert "strip" not in out["home_keys"]
    assert all(p["kwh"] < HOME_KWH_PER_H + 0.03 - 1e-6 for p in out["paired"])


def test_one_device_on_purpose():
    out = _run({"device_key": "fridge"})
    assert out["scope"] == "device"
    assert out["device_key"] == "fridge" and out["device_name"] == "Fridge"
    assert out["correlation"]["total_kwh"] == pytest.approx(0.120 * HOURS, rel=1e-3)
    for p in out["paired"]:
        assert p["kwh"] == pytest.approx(0.120, rel=1e-3)


def test_unknown_device_key_falls_back_to_the_whole_home():
    out = _run({"device_key": "gone"})
    assert out["scope"] == "home"
    assert out["correlation"]["total_kwh"] == pytest.approx(HOME_KWH_PER_H * HOURS, rel=1e-3)


def test_the_list_offers_every_device():
    out = _run({})
    assert [d["key"] for d in out["devices"]] == ["ont", "fridge", "heat", "strip"]
    assert out["home_names"] == ["Ont", "Fridge", "Heat"]


def test_small_totals_keep_their_digits():
    out = _run({"device_key": "ont"})
    # 4 W for 8 h = 0.032 kWh; rounding to one decimal read 0.0
    assert out["correlation"]["total_kwh"] == pytest.approx(0.032, abs=1e-4)


def test_with_a_meter_the_meter_is_the_home():
    """A metered home: the meter is the home, the plug behind it is not added."""
    with tempfile.TemporaryDirectory() as tmp:
        ad = _site(tmp, plugs=[("lamp", 40.0, "house")],
                   extra_devices=[{"key": "house", "name": "House", "host": "192.0.2.50", "kind": "em"}])
        try:
            _fill(ad.storage, "house", 1500.0, _start_hour())
            with mock.patch("requests.get", side_effect=_no_net):
                out = ad.dispatch("weather_correlation", {})
        finally:
            ad._warm_stop.set()
    assert out["scope"] == "home"
    assert out["home_keys"] == ["house"]
    for p in out["paired"]:
        assert p["kwh"] == pytest.approx(1.5, rel=1e-3)


# ── texts and tab ────────────────────────────────────────────────────────────

def test_new_texts_in_all_languages():
    from shelly_analyzer.i18n import t
    for key in ("web.weather.scope_all", "web.weather.scope_hint"):
        vals = [t(lang, key) for lang in LANGS]
        assert all(v and v != key for v in vals), (key, vals)
        assert len(set(vals)) == len(LANGS), (key, vals)
    for lang in LANGS:
        assert "{n}" in t(lang, "web.weather.scope_hint"), lang


def _tab_js():
    src = Path(__file__).resolve().parents[1] / "src" / "shelly_analyzer" / "services" / "webdash.py"
    s = src.read_text(encoding="utf-8")
    a = s.index("WEATHER TAB")
    b = s.index("COMPARE TAB", a)
    return s, s[a:b]


def test_tab_sends_the_picked_device_and_offers_whole_home():
    s, js = _tab_js()
    assert 'id="weather-device"' in s and 'id="weather-controls"' in s
    assert "device_key=" in js
    assert "web.weather.scope_all" in js and "web.weather.scope_hint" in js


def test_kwh_axes_follow_the_size_of_the_values():
    _s, js = _tab_js()
    assert "function _wxKwhTick(" in js
    assert len(re.findall(r"_wxKwhTick\(", js)) >= 4   # definition + three charts
    # no fixed-decimal kWh tick left in the weather charts
    assert "(maxK * i / 4).toFixed(" not in js
    assert "(maxK2 * gi / 4).toFixed(" not in js
    assert "(minK + (maxK - minK) * i / 4).toFixed(" not in js


def test_tick_and_value_format_in_node():
    import shutil
    import subprocess
    node = shutil.which("node")
    if not node:
        pytest.skip("node not installed")
    _s, js = _tab_js()
    fns = []
    for name in ("_wxKwh", "_wxKwhTick", "_wxDeg"):
        a = js.index("function %s(" % name)
        depth, i = 0, js.index("{{", a)
        # the template doubles braces; walk to the matching close
        while True:
            if js.startswith("{{", i):
                depth += 1; i += 2; continue
            if js.startswith("}}", i):
                depth -= 1; i += 2
                if depth == 0:
                    break
                continue
            i += 1
        fns.append(js[a:i].replace("{{", "{").replace("}}", "}").replace("\\\\u", "\\u"))
    prog = "\n".join(fns) + r"""
const out = {
  ticks_small: [1,2,3,4].map(i => _wxKwhTick(0.0044 * i / 4, 0.0044 / 4)),
  ticks_home: [1,2,3,4].map(i => _wxKwhTick(0.33 * i / 4, 0.33 / 4)),
  ticks_big: [1,2,3,4].map(i => _wxKwhTick(12 * i / 4, 12 / 4)),
  v: [_wxKwh(0.004), _wxKwh(0.32), _wxKwh(7.123), _wxKwh(0)],
  zero: _wxKwhTick(0, 0.0011),
  deg: [_wxDeg(-0.3), _wxDeg(-1.6), _wxDeg(12.4)],
};
console.log(JSON.stringify(out));
"""
    with tempfile.TemporaryDirectory() as tmp:
        f = Path(tmp) / "t.js"
        f.write_text(prog, encoding="utf-8")
        res = subprocess.run([node, str(f)], capture_output=True, text=True, timeout=30)
    assert res.returncode == 0, res.stderr
    out = json.loads(res.stdout)
    for k in ("ticks_small", "ticks_home", "ticks_big"):
        assert len(set(out[k])) == 4, out[k]          # four different tick texts
        assert all(float(x) > 0 for x in out[k]), out[k]
    assert out["ticks_small"][0] == "0.0011"
    assert out["v"] == ["0.0040", "0.32", "7.12", "0"]
    assert out["zero"] == "0"
    assert out["deg"] == ["0", "-2", "12"]       # never "-0"
