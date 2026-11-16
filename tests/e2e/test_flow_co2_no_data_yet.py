"""The Energy-flow CO2 tile says "no data yet" instead of 0 g/kWh (17.10.4).

Feeds the real renderer a day that has no consumption yet (the intensity is null) after it drew a
normal day, i.e. the in-place patch path of a page left open over midnight.  Fixed payloads, no clock.
"""
import os
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

DAY = {"ok": True, "unit": "kWh", "period": "today", "has_supply": True, "has_pv": False, "has_battery": False,
       "has_grid_meter": True, "sources": {"pv": 0, "battery": 0, "grid": 5}, "sinks": {"house": 5, "battery": 0, "grid": 0},
       "flows": [{"from": "grid", "to": "house", "v": 5}],
       "house": {"load": 5, "consumers_sum": 5, "autarky_pct": 0, "self_consumption_pct": None, "co2_kg": 1.9, "intensity": 380.0},
       "consumers": [{"key": "h", "name": "House", "role": "owner", "kwh": 5, "grid": 5, "pv": 0, "battery": 0, "co2_g": 1900}]}
EMPTY = dict(DAY, sources={"pv": 0, "battery": 0, "grid": 0}, sinks={"house": 0, "battery": 0, "grid": 0},
             flows=[{"from": "grid", "to": "house", "v": 0}],
             house={"load": 0, "consumers_sum": 0, "autarky_pct": None, "self_consumption_pct": None, "co2_kg": 0.0, "intensity": None},
             consumers=[{"key": "h", "name": "House", "role": "owner", "kwh": 0, "grid": 0, "pv": 0, "battery": 0, "co2_g": 0}])


@pytest.mark.skipif(os.environ.get("SEA_E2E") != "1", reason="set SEA_E2E=1 to run the browser checks")
def test_unknown_intensity_reads_no_data_yet():
    pytest.importorskip("playwright")
    import common
    from installations import materialize
    why = common.browser_available("chromium")
    if why:
        pytest.skip("no usable browser: " + why)
    from playwright.sync_api import sync_playwright

    tmp = common.make_tmp("sea-e2e-flowco2-")
    try:
        materialize("f", tmp)
        with common.Server(tmp) as srv, sync_playwright() as pw:
            br = common.launch_browser(pw, "chromium")
            try:
                page = br.new_context(viewport={"width": int(os.environ.get("SEA_FLOW_W", "1200")), "height": 900}).new_page()
                page.goto(srv.url + "/", wait_until="load")
                page.wait_for_function("typeof renderEnergyFlow === 'function'", timeout=30000)
                shot = os.environ.get("SEA_FLOW_SHOT")
                texts = page.evaluate("""([day, empty]) => {
                  const host = document.createElement('div'); host.id = 'probe-flow'; document.body.prepend(host);
                  const read = () => Array.from(host.querySelectorAll('[data-ef^="co2."]')).map(e => e.textContent);
                  renderEnergyFlow(day, host); const a = read();
                  renderEnergyFlow(empty, host); const b = read();
                  return {a, b};
                }""", [DAY, EMPTY])
                assert texts["a"] == ["1.90 kg", "380 g/kWh"], texts
                assert texts["b"][0] == "–" and "0 g/kWh" not in " ".join(texts["b"]), texts
                if shot:
                    page.locator("#probe-flow").screenshot(path=shot)
            finally:
                br.close()
    finally:
        common.rm_tree(tmp)
