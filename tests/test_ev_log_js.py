"""The EV-Log tab's JavaScript, actually executed.

v16.44.3 shipped a dashboard whose plots were all blank because a `const` was
used one line above its declaration. `node --check` parses that happily — a
temporal-dead-zone error only exists at run time. So this test renders the real
template, runs the real script in node, calls the real render function with a
real-shaped payload and reads what came out.
"""
import json
import os
import re
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.services.webdash import _HTML_TEMPLATE, _render_template

NUMBERS = {"refresh_ms": 5000, "window_min": 60, "days": 30}
JSON_VALS = {"window_options_json": "[60,180]", "devices_json": "[]",
             "i18n_json": "{}"}


def _script() -> str:
    """The dashboard's main <script> block, with placeholders filled in.

    Single-letter placeholders are left alone: they are not template keys but
    the `{n}`/`{c}` argument slots inside i18n fallback strings, which `t()`
    substitutes at run time.
    """
    names = set(re.findall(r"(?<!\{)\{([a-z0-9_]+)\}(?!\})", _HTML_TEMPLATE))
    values = {}
    for k in names:
        if len(k) < 2:
            continue
        values[k] = JSON_VALS.get(k, NUMBERS.get(k, "x"))
    html = _render_template(_HTML_TEMPLATE, values)
    blocks = re.findall(r"<script[^>]*>(.*?)</script>", html, re.S)
    main = max(blocks, key=len)
    assert len(main) > 100_000, f"main script block is only {len(main)} bytes"
    return main


HARNESS = r"""
// Enough of a browser for the dashboard's top level to run.
const _el = () => ({
  innerHTML: '', textContent: '', value: '', checked: false, hidden: false, disabled: false,
  style: { setProperty(){}, removeProperty(){}, getPropertyValue(){ return '' } },
  dataset: {}, children: [], childNodes: [], parentNode: null, firstChild: null,
  offsetWidth: 800, offsetHeight: 400, clientWidth: 800, clientHeight: 400,
  scrollTop: 0, scrollLeft: 0, scrollWidth: 800, scrollHeight: 400,
  classList: { add(){}, remove(){}, toggle(){}, contains(){ return false } },
  addEventListener(){}, removeEventListener(){}, dispatchEvent(){ return true },
  setAttribute(){}, getAttribute(){ return null }, removeAttribute(){}, hasAttribute(){ return false },
  appendChild(c){ return c }, removeChild(c){ return c }, insertBefore(c){ return c },
  replaceChildren(){}, cloneNode(){ return _el() }, contains(){ return false },
  querySelector(){ return null }, querySelectorAll(){ return [] }, closest(){ return null },
  matches(){ return false }, remove(){}, focus(){}, blur(){}, click(){}, select(){},
  scrollIntoView(){}, getBoundingClientRect(){ return {top:0,left:0,width:800,height:400,right:800,bottom:400} },
  getContext(){ return _ctx() }, toDataURL(){ return '' },
});
// A 2-D canvas context that swallows everything: the tab draws a heatmap.
const _ctx = () => new Proxy({}, { get: (_, k) =>
  (k === 'canvas') ? _el() :
  (k === 'measureText') ? (() => ({ width: 10 })) :
  (k === 'createLinearGradient' || k === 'createPattern') ? (() => ({ addColorStop(){} })) :
  (k === 'getImageData') ? (() => ({ data: new Uint8ClampedArray(4) })) :
  (typeof k === 'string' && /^(fillStyle|strokeStyle|font|lineWidth|textAlign|textBaseline|globalAlpha)$/.test(k))
    ? '' : (() => {}) });
globalThis.document = {
  documentElement: _el(), body: _el(), head: _el(),
  // Every lookup answers with a fresh stub: the dashboard's top level wires
  // listeners onto real elements, and a null would stop the script before a
  // single render function was even defined.
  getElementById(){ return _el() }, querySelector(){ return _el() }, querySelectorAll(){ return [] },
  createElement(){ return _el() }, addEventListener(){}, cookie: '',
};
globalThis.window = globalThis;
globalThis.addEventListener = () => {};
globalThis.removeEventListener = () => {};
globalThis.dispatchEvent = () => true;
globalThis.getComputedStyle = () => ({ getPropertyValue: () => '' });
globalThis.scrollTo = () => {};
globalThis.clearInterval = () => {};
globalThis.clearTimeout = () => {};
globalThis.URL = globalThis.URL || function(){ return {} };
globalThis.localStorage = { _d:{}, getItem(k){return this._d[k]??null}, setItem(k,v){this._d[k]=String(v)}, removeItem(k){delete this._d[k]} };
globalThis.sessionStorage = globalThis.localStorage;
globalThis.navigator = { language: 'de-DE', userAgent: 'node', onLine: true };
globalThis.location = { href: 'http://x/', protocol: 'http:', host: 'x', search: '', hash: '' };
globalThis.fetch = () => Promise.resolve({ ok:true, json: () => Promise.resolve({ok:true,data:{}}) });
globalThis.setInterval = () => 0; globalThis.setTimeout = (f) => 0;
globalThis.requestAnimationFrame = () => 0;
globalThis.alert = () => {}; globalThis.confirm = () => false;
globalThis.matchMedia = () => ({ matches:false, addListener(){}, addEventListener(){} });
globalThis.Chart = function(){ return { destroy(){}, update(){} } };
"""

TAIL = r"""
const out = { innerHTML: '' };
renderEvLog(PAYLOAD, out);
process.stdout.write('<<<HTML>>>' + out.innerHTML);
"""


def _run(payload: dict) -> str:
    src = (HARNESS + "\nconst PAYLOAD = " + json.dumps(payload) + ";\n"
           + _script() + "\n" + TAIL)
    p = subprocess.run(["node", "-e", src], capture_output=True, text=True, timeout=120)
    assert p.returncode == 0, f"node failed:\n{p.stderr[-3000:]}"
    assert "<<<HTML>>>" in p.stdout, p.stdout[-2000:]
    return p.stdout.split("<<<HTML>>>", 1)[1]


def _charge(**kw):
    base = {"group_id": "g1", "start_ts": 1786023600, "end_ts": 1786036440,
            "energy_kwh": 14.17, "peak_power_w": 7000, "avg_power_w": 3900,
            "cost_eur": 0.0, "session_count": 1, "solar_kwh": 14.17,
            "battery_kwh": 0.0, "grid_kwh": 0.0, "cost_model": "source",
            "sessions": []}
    base.update(kw)
    base["sessions"] = [dict(base, session_id=base["group_id"])]
    return base


def _payload(charges, **kw):
    sessions = [s for c in charges for s in c["sessions"]]
    d = {"total_sessions": len(sessions), "total_kwh": round(sum(c["energy_kwh"] for c in charges), 2),
         "total_cost": round(sum(c["cost_eur"] for c in charges), 2),
         "avg_kwh_per_session": 10.0, "avg_duration_min": 120, "window_days": 30,
         "grouping_enabled": True, "charge_count": len(charges), "charges": charges,
         "monthly_kwh": [], "sessions": sessions,
         "total_solar_kwh": round(sum(c["solar_kwh"] for c in charges), 2),
         "total_battery_kwh": round(sum(c["battery_kwh"] for c in charges), 2),
         "total_grid_kwh": round(sum(c["grid_kwh"] for c in charges), 2),
         "cost_if_all_grid": round(sum(c["energy_kwh"] for c in charges) * 0.3025, 2),
         "source_pricing": {"mode": "auto", "active": True, "priced": len(sessions),
                            "reason": "ok", "solar_cost_model": "free",
                            "price_eur_kwh": 0.3025}}
    d.update(kw)
    return d


def test_the_tab_renders_with_a_source_split():
    """A real installation's mix: mostly sun, some battery, a little grid."""
    charges = [
        _charge(group_id="a", solar_kwh=14.17),
        _charge(group_id="b", energy_kwh=43.55, solar_kwh=36.19, battery_kwh=6.31,
                grid_kwh=1.05, cost_eur=0.32, start_ts=1785666180, end_ts=1785689760),
        _charge(group_id="c", energy_kwh=1.01, solar_kwh=0.0, battery_kwh=0.0,
                grid_kwh=1.01, cost_eur=0.31, start_ts=1785727620, end_ts=1785728460),
    ]
    html = _run(_payload(charges))
    assert "Where the energy came from" in html, "the source card is missing"
    for colour in ("#fdd835", "#22c55e", "#ef4444"):
        assert colour in html, f"no {colour} segment rendered"
    assert "Surplus charge" in html, "a 100 % solar charge got no surplus badge"
    assert "Saved" in html and "vs." in html
    print("OK  the EV tab renders solar/battery/grid segments and the badge")


def test_a_grid_charge_gets_no_surplus_badge():
    html = _run(_payload([_charge(group_id="n", energy_kwh=1.01, solar_kwh=0.0,
                                  grid_kwh=1.01, cost_eur=0.31)]))
    assert "Surplus charge" not in html and "Mostly surplus" not in html
    assert "#ef4444" in html
    print("OK  a night charge is red and carries no surplus badge")


def test_an_unattributed_charge_keeps_the_neutral_bar():
    """cost_model 'fixed' ⇒ no colours, no badge, no source card — a charge
    nobody measured must not look like free sunshine."""
    c = _charge(group_id="u", cost_model="fixed", solar_kwh=0.0, cost_eur=4.29)
    html = _run(_payload([c], total_solar_kwh=0.0, total_battery_kwh=0.0,
                         total_grid_kwh=0.0,
                         source_pricing={"mode": "auto", "active": False, "priced": 0,
                                         "reason": "no_data", "solar_cost_model": "free",
                                         "price_eur_kwh": 0.3025}))
    assert "#fdd835" not in html and "Surplus" not in html
    assert "Where the energy came from" not in html
    assert "keep the full tariff" in html, "the user is not told why there is no split"
    print("OK  an unattributed charge stays neutral and explains itself")


def test_a_home_without_the_meters_is_told_what_to_configure():
    html = _run(_payload([_charge(cost_model="fixed", solar_kwh=0.0, cost_eur=4.29)],
                         total_solar_kwh=0.0, total_battery_kwh=0.0, total_grid_kwh=0.0,
                         source_pricing={"mode": "auto", "active": False, "priced": 0,
                                         "reason": "no_meter", "solar_cost_model": "free",
                                         "price_eur_kwh": 0.3025}))
    assert "No grid meter" in html and "Settings" in html
    print("OK  a home without meters is told which setting to fill in")


def test_flat_mode_says_nothing_at_all():
    """Switched off means switched off: no card, no note, no colours."""
    html = _run(_payload([_charge(cost_model="fixed", solar_kwh=0.0, cost_eur=4.29)],
                         total_solar_kwh=0.0, total_battery_kwh=0.0, total_grid_kwh=0.0,
                         source_pricing={"mode": "flat", "active": False, "priced": 0,
                                         "reason": "off", "solar_cost_model": "free",
                                         "price_eur_kwh": 0.3025}))
    assert "Where the energy came from" not in html
    assert "No grid meter" not in html and "full tariff" not in html
    print("OK  flat mode renders exactly the old tab")


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items())
           if k.startswith("test_") and callable(v)]
    for f in fns:
        f()
    print(f"\n{len(fns)} Tests bestanden")
