"""The Live insight cards, actually executed.

Reported 2026-09-08 from a screenshot of a running installation: the card row
said "PV now 0 W" while the PV strip right below it drew 1216 W, and "Drawing
now … all from the grid" while the roof carried half the house.

Both cards read the right variables for the wrong question, and neither fault
is visible in the source at a glance — so this test renders the real template,
runs the real script in node and calls the real card builders with a payload
shaped like the one the running installation returns.
"""
import json
import os
import re
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.services.webdash import _HTML_TEMPLATE, _render_template
from tests.test_ev_log_js import HARNESS, _script

# One installation, one moment: the roof makes 1216 W, 637 W come from the
# grid, the battery gives 306 W. Half the house is on solar; the tenant gets
# nothing, because nothing is being exported.
STATE = {
    "solar_share_home": 0.5,
    "solar_share_now": 0.0,
    "devices": [
        {"key": "pv", "flow_role": "pv", "power_w": 1216},
        {"key": "grid", "flow_role": "grid", "power_w": 637},
        {"key": "battery", "flow_role": "battery", "power_w": -306},
        {"key": "house", "power_w": 1200},
        {"key": "tenant", "power_w": 400, "is_tenant": True},
    ],
}
CTX = {
    "state": STATE,
    # What /api/solar really answers with: the house, the grid and the tenant —
    # no PV meter, and no power field on any of them.
    "solar": {"configured": True,
              "devices": [{"key": "haus", "name": "House"},
                          {"key": "grid", "name": "Grid"},
                          {"key": "tenant", "name": "Tenant"}]},
    "sankey": {"pv_production_kwh": 43.84, "self_consumption_kwh": 7.9},
}

TAIL = r"""
const out = {};
for (var i = 0; i < _INSIGHT_PROVIDERS.length; i++) {
  var p = _INSIGHT_PROVIDERS[i];
  var card = null;
  try { card = p.build(CTX); } catch (e) { card = { error: String(e) }; }
  out[p.id] = card;
}
process.stdout.write('<<<JSON>>>' + JSON.stringify(out));
"""


def _cards(ctx=None) -> dict:
    src = (HARNESS + "\nconst CTX = " + json.dumps(ctx or CTX) + ";\n"
           + _script() + "\n" + TAIL)
    p = subprocess.run(["node", "-e", src], capture_output=True, text=True, timeout=180)
    assert p.returncode == 0, f"node failed:\n{p.stderr[-3000:]}"
    assert "<<<JSON>>>" in p.stdout, p.stdout[-2000:]
    return json.loads(p.stdout.split("<<<JSON>>>", 1)[1])


def test_pv_card_shows_what_the_strip_shows():
    """The instantaneous value must come from the live meter — the very source
    the PV strip draws — so the two can never contradict each other."""
    pv = _cards()["pv"]
    assert pv, "the PV card did not build at all"
    assert "error" not in pv, pv
    assert pv["value"] == "1.22 kW", pv["value"]


def test_pv_card_is_not_zero_when_the_solar_payload_has_no_pv_meter():
    """The exact reported shape: /api/solar lists three devices, none of them
    the PV meter and none carrying a power field. Summing those gave 0 W."""
    assert not any(d.get("pv_w") or d.get("power_w") for d in CTX["solar"]["devices"])
    assert _cards()["pv"]["value"] != "0 W"


def test_drawing_now_reports_the_households_share_not_the_tenants():
    grid = _cards()["grid"]
    assert grid and "error" not in grid, grid
    assert "50" in grid["sub"], grid["sub"]


def test_a_fraction_is_not_a_percent():
    """`share.toFixed(0)` on a 0..1 value printed "1 % from the roof" for a
    house running half on solar. Latent until now, because the value it was
    reading was always exactly 0."""
    sub = _cards()["grid"]["sub"]
    assert "1 %" not in sub and "1%" not in sub, sub


def test_the_tenant_gets_its_own_card_with_its_own_meaning():
    t = _cards()["tenant"]
    assert t and "error" not in t, t
    assert t["value"] == "0 %", t
    assert t["tab"] == "tenants", t["tab"]


def test_no_tenant_configured_means_no_tenant_card():
    ctx = json.loads(json.dumps(CTX))
    for d in ctx["state"]["devices"]:
        d.pop("is_tenant", None)
    assert _cards(ctx)["tenant"] is None


def test_the_grid_card_still_says_grid_when_the_sun_is_down():
    ctx = json.loads(json.dumps(CTX))
    ctx["state"]["solar_share_home"] = 0.0
    ctx["state"]["devices"][0]["power_w"] = 0
    cards = _cards(ctx)
    assert "%" not in cards["grid"]["sub"], cards["grid"]["sub"]
