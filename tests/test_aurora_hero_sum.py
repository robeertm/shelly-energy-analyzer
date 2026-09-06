"""The Aurora hero's household sum, actually executed in node.

This one number has been wrong twice on real installations and right in every
review: once it counted a sub-meter twice (4 288 W on a house pulling 2 327 W),
and once it summed the GRID meter as if it were an appliance while dropping the
house meter that hung off it — 35.0 kWh / 2.80 € on a day that had really used
47.7 kWh for 2.04 €. So the rule gets executed against real-shaped payloads
rather than read.
"""
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from test_ev_log_js import HARNESS                      # noqa: E402

ROOT = os.path.join(os.path.dirname(__file__), "..")
AURORA = os.path.join(ROOT, "src", "shelly_analyzer", "web", "static", "aurora.js")


def _sum(devices):
    """Load the real aurora.js in node and ask it what the house is drawing."""
    src = open(AURORA, encoding="utf-8").read()
    script = (
        HARNESS
        # The skin bails out unless the page says it is Aurora.
        + "\ndocument.documentElement.getAttribute = (k) =>"
          " (k === 'data-skin' ? 'aurora' : null);\n"
        # boot() touches far more of the DOM than the stub has; the export we
        # need is assigned long before it runs, so a boot failure is fine.
        + "try {\n" + src + "\n} catch (e) { globalThis.__bootError = String(e); }\n"
        + "if (typeof window.__auHeroSum !== 'function')"
          " { console.log(JSON.stringify({error: 'no __auHeroSum',"
          " boot: globalThis.__bootError || null})); process.exit(0); }\n"
        + "console.log(JSON.stringify(window.__auHeroSum("
        + json.dumps(devices) + ")));\n"
    )
    r = subprocess.run([sys.executable and "node", "-e", script],
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr[-1500:]
    out = json.loads(r.stdout.strip().splitlines()[-1])
    assert "error" not in out, out
    return out


def _dev(key, w, kwh, cost, role=None, parent="", net=None):
    d = {"key": key, "name": key, "power_w": w, "today_kwh": kwh,
         "cost_today": cost, "flow_role": role, "parent": parent}
    if net is not None:
        d["net_of_children"] = net
    return d


# The real payload shape from an installation with PV, a battery, a signed grid
# meter, a house meter hanging off it, a wallbox behind the house and a
# grid-parallel tenant.
MIT_PV = [
    _dev("solar",   0.6,   2.882, 1.16, role="grid"),
    _dev("mieter",  326.7, 2.997, 0.91),
    _dev("haus",    493.9, 15.538, 0.39, parent="solar", net=["wallbox"]),
    _dev("wallbox", 4.9,   29.163, 0.74, parent="haus"),
    _dev("pv",      50.0,  46.940, 0.0,  role="pv"),
    _dev("battery", -491.0, 9.320, 0.0,  role="battery"),
]


def test_grid_meter_is_not_an_appliance():
    s = _sum(MIT_PV)
    assert abs(s["kwh"] - (15.538 + 29.163 + 3.001)) < 0.01, s
    assert abs(s["cost"] - (0.39 + 0.74 + 0.91)) < 0.005, s
    assert abs(s["totalW"] - (326.7 + 493.9 + 4.9)) < 0.1, s
    print("OK  Netz zaehlt nicht mit: %.3f kWh / %.2f EUR" % (s["kwh"], s["cost"]))


def test_the_house_behind_the_grid_meter_is_counted():
    """The old bug: haus.parent == the grid meter, so it was skipped as a
    "sub-meter". A signed grid meter reads net of PV and battery — it never
    contains the house consumption."""
    s = _sum(MIT_PV)
    assert s["kwh"] > 40, "the house fell out of the total again: %s" % s
    assert s["top"] and s["top"]["key"] == "haus", s["top"]
    print("OK  Haus ist drin und ist der groesste Verbraucher")


def test_without_generation_the_grid_meter_is_the_house_meter():
    """No PV, no battery: the grid reading IS the consumption, and anything
    metered behind it stays a sub-meter."""
    ohne_pv = [
        _dev("netz",    2000.0, 20.0, 6.05, role="grid"),
        _dev("wallbox", 1500.0, 11.0, 3.33, parent="netz"),
    ]
    s = _sum(ohne_pv)
    assert abs(s["kwh"] - 20.0) < 0.001, s
    assert abs(s["totalW"] - 2000.0) < 0.001, s
    print("OK  ohne Erzeugung bleibt der Netzzaehler der Hauszaehler")


def test_grid_only_installation_still_shows_something():
    s = _sum([_dev("netz", 800.0, 9.0, 2.7, role="grid")])
    assert abs(s["kwh"] - 9.0) < 0.001, s
    print("OK  reine Netzzaehler-Anlage zeigt weiter ihren Wert")


def test_a_sub_meter_is_still_only_counted_once():
    """The 2 327-vs-4 288-W regression must stay fixed."""
    kaskade = [
        _dev("netz",   2327.0, 20.0, 6.05, role="grid"),
        _dev("haus",   2281.0, 18.0, 5.45, parent="netz"),
        _dev("boiler", 1960.0, 12.0, 3.63, parent="haus"),
    ]
    s = _sum(kaskade)
    assert abs(s["totalW"] - 2327.0) < 0.001, "sub-meter counted twice: %s" % s
    print("OK  Unterzaehler wird weiter genau einmal gezaehlt")


def test_net_of_children_still_adds_the_child_back():
    kaskade = [
        _dev("netz",  2327.0, 20.0, 6.05, role="grid"),
        _dev("haus",   321.0,  6.0, 1.82, parent="netz", net=["boiler"]),
        _dev("boiler", 1960.0, 12.0, 3.63, parent="haus"),
    ]
    s = _sum(kaskade)
    assert abs(s["totalW"] - 2327.0) < 0.001, s
    print("OK  bei Netto-Anzeige zaehlt das Kind wieder mit")


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items())
           if k.startswith("test_") and callable(v)]
    for f in fns:
        f()
    print("\n%d Tests bestanden" % len(fns))
