"""Follow-up of the plugs-only report (17.9.3).

* Without a live CO2 source (no provider key, no forecast) the CO2 tab said
  "0 g/kWh · unknown", its device cards used a hard-coded 380 g/kWh and the
  energy flow the value from the settings. Now every place uses the value from
  the settings and the answer says so (``current_source == "configured"``).
* The standby analysis skipped every switch, so an installation of plugs only
  had "no standby data" despite days of minute history.
* The Plots page showed "Error: superseded" when a newer selection cancelled a
  running request: Chromium rejects ``fetch`` with the abort REASON (a string),
  not with an AbortError.
"""
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.io.config import load_config  # noqa: E402
from shelly_analyzer.io.storage import Storage  # noqa: E402
from shelly_analyzer.services.live_history import LiveHistoryRecorder  # noqa: E402

PLUGS = [("p0", 0.0), ("p1", 12.0), ("p2", 60.0), ("p3", 5.0)]


def _site(tmp, extra=None):
    devices = [{"key": k, "name": k.upper(), "host": f"192.0.2.{10 + i}", "kind": "switch",
                "supports_emdata": False} for i, (k, _w) in enumerate(PLUGS)]
    body = {"devices": devices, "co2": {"enabled": True},
            "pricing": {"electricity_price_eur_per_kwh": 0.30, "co2_intensity_g_per_kwh": 300.0}}
    body.update(extra or {})
    p = Path(tmp) / "config.json"
    p.write_text(json.dumps(body))
    cfg, st = load_config(p), Storage(Path(tmp) / "data")
    # 12 hours of minute history per plug, ending now
    now = int(datetime.now().timestamp()) // 60 * 60
    rec = LiveHistoryRecorder(st.db)
    for key, watts in PLUGS:
        for s in range(0, 12 * 3600 + 1, 30):
            ts = now - 12 * 3600 + s
            rec.observe(key, ts, watts, 230.0, watts / 230.0)
    rec.flush()
    from shelly_analyzer.web.action_dispatch import ActionDispatcher
    return ActionDispatcher(cfg, st, None, out_dir=Path(tmp), cfg_path=p, lang="en")


def test_co2_without_live_source_uses_the_set_value_everywhere():
    with tempfile.TemporaryDirectory() as tmp:
        ad = _site(tmp)
        live = ad.dispatch("co2_live", {})
        full = ad.dispatch("co2", {})
        assert live["ok"] and full["ok"]
        for r in (live, full):
            assert r["current_intensity"] == 300.0, r["current_intensity"]
            assert r["current_source"] == "configured"
        # device bars of the CO2 tab: the set value, not a hard-coded 380
        ints = {b["intensity"] for d in full.get("device_hourly_co2", []) for b in d.get("bars", [])}
        assert ints and ints == {300.0}, ints
        # energy flow and the CO2 tab agree on today's kg
        flow = ad.dispatch("energy_flow", {"period": "today"})
        assert flow["house"]["intensity"] == 300.0
        assert abs(flow["house"]["co2_kg"] - full["co2_today_kg"]) < 0.01


def test_co2_live_data_still_wins():
    with tempfile.TemporaryDirectory() as tmp:
        ad = _site(tmp)
        h = int(datetime.now().timestamp()) // 3600 * 3600
        ad.storage.db.upsert_co2_intensity([(h - 3600, "DE_LU", 222.0, "entsoe", h), (h, "DE_LU", 222.0, "entsoe", h)])
        r = ad.dispatch("co2_live", {})
        assert r["current_intensity"] == 222.0 and r["current_source"] != "configured"


def test_standby_reads_the_plugs_of_a_plugs_only_site():
    with tempfile.TemporaryDirectory() as tmp:
        ad = _site(tmp)
        r = ad.dispatch("standby", {})
        assert r["ok"]
        keys = {d["device_key"] for d in r["devices"]}
        assert {"p1", "p2", "p3"} <= keys, keys
        assert r["total_annual_standby_kwh"] > 0


def test_standby_keeps_plugs_out_when_a_meter_exists():
    with tempfile.TemporaryDirectory() as tmp:
        ad = _site(tmp)
        cfgp = Path(tmp) / "config.json"
        body = json.loads(cfgp.read_text())
        body["devices"].append({"key": "house", "name": "House", "host": "192.0.2.99", "kind": "em", "phases": 3})
        cfgp.write_text(json.dumps(body))
        ad.reload(load_config(cfgp))
        r = ad.dispatch("standby", {})
        assert not ({"p0", "p1", "p2", "p3"} & {d["device_key"] for d in r["devices"]})


HARNESS = r"""
const fs = require('fs');
eval(fs.readFileSync(process.argv[2], 'utf8'));
// Chromium: an aborted fetch rejects with signal.reason itself.
globalThis.fetch = (url, opts) => new Promise((res, rej) => {
  opts.signal.addEventListener('abort', () => rej(opts.signal.reason));
  setTimeout(() => res({ ok: true, json: async () => ({ url }) }), 50);
});
(async () => {
  const first = fetchJsonWithTimeout('/a').then(() => 'resolved', e => (e && e.superseded) ? 'superseded' : 'error:' + (e && e.message || e));
  const second = fetchJsonWithTimeout('/b').then(d => d.url, e => 'error:' + (e && e.message || e));
  console.log(JSON.stringify([await first, await second]));
})();
"""


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_plots_superseded_request_is_not_an_error():
    from shelly_analyzer.services.webdash import _PLOTS_TEMPLATE
    src = _PLOTS_TEMPLATE
    start = src.index("let __plots_fetch_ctrl = null;")
    end = src.index("function cssVar(", start)
    code = src[start:end]
    with tempfile.TemporaryDirectory() as td:
        cp = os.path.join(td, "f.js")
        Path(cp).write_text(code, encoding="utf-8")
        hp = os.path.join(td, "h.js")
        Path(hp).write_text(HARNESS, encoding="utf-8")
        res = subprocess.run(["node", hp, cp], capture_output=True, text=True, timeout=60)
    assert res.returncode == 0, res.stderr
    assert json.loads(res.stdout) == ["superseded", "/b"], res.stdout
