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
globalThis.__paint = { fills: [], strokes: [], texts: [] };
// A 2-D canvas context that swallows everything: the tab draws a heatmap.
// It also records the fill colours, so a test can ask what was actually
// painted instead of only that nothing threw.
const _ctx = () => { const st = { fillStyle: '', strokeStyle: '' }; return new Proxy(st, {
  set: (o, k, v) => { o[k] = v; return true },
  get: (o, k) =>
  (k === 'fillStyle' || k === 'strokeStyle') ? o[k] :
  (k === 'fill') ? (() => globalThis.__paint.fills.push(String(o.fillStyle))) :
  (k === 'stroke') ? (() => globalThis.__paint.strokes.push(String(o.strokeStyle))) :
  (k === 'fillText') ? ((t) => globalThis.__paint.texts.push(String(t))) :
  (k === 'fillRect') ? (() => globalThis.__paint.fills.push('rect:' + String(o.fillStyle))) :
  (k === 'canvas') ? _el() :
  (k === 'measureText') ? (() => ({ width: 10 })) :
  (k === 'createLinearGradient' || k === 'createPattern') ? (() => ({ addColorStop(){} })) :
  (k === 'getImageData') ? (() => ({ data: new Uint8ClampedArray(4) })) :
  (typeof k === 'string' && /^(font|lineWidth|textAlign|textBaseline|globalAlpha)$/.test(k))
    ? '' : (() => {}) }) };
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


def _run(payload: dict, _lang: str = "en") -> str:
    script = _script()
    if _lang != "en":
        from shelly_analyzer.i18n import get_lang_map
        script = script.replace("const I18N = {};", "const I18N = "
                                + json.dumps(get_lang_map(_lang), ensure_ascii=False) + ";")
    src = (HARNESS + "\nconst PAYLOAD = " + json.dumps(payload) + ";\n"
           + script + "\n" + TAIL)
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


def _payload(charges, _lang=None, **kw):
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
         "source_pricing": {"mode": "auto", "active": True, "priced": len(charges),
                            "entries": len(charges), "unpriced_kwh": 0.0,
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


def test_the_unmeasured_remainder_is_named_on_screen():
    """Otherwise the three parts visibly miss the total with no explanation."""
    charges = [_charge(group_id="a", solar_kwh=14.17),
               _charge(group_id="b", cost_model="fixed", solar_kwh=0.0,
                       energy_kwh=13.18, cost_eur=3.99)]
    d = _payload(charges)
    d["source_pricing"].update(priced=1, entries=2, unpriced_kwh=13.18)
    d["total_solar_kwh"] = 14.17
    d["total_battery_kwh"] = d["total_grid_kwh"] = 0.0
    html = _run(d)
    assert "1 of 2 charges measured" in html, "the count is wrong or missing"
    assert "13.2 kWh not measured" in html, "the uncovered energy is not named"
    print("OK  the tab names the uncovered kWh and counts in charges")


def test_every_coloured_bar_has_a_named_legend():
    """Robert: „was ist gelb grün und rot für ein anteil, legende fehlt".

    Three colours carried the whole meaning and only a hover title explained
    them — useless on a phone, and red against green is exactly the pair a
    colour-blind reader cannot separate. Every bar must have a dot-and-name
    key within reach: under the overview bar, above the list, and on each card.
    """
    charges = [_charge(group_id="a", energy_kwh=29.0, solar_kwh=19.38,
                       battery_kwh=6.44, grid_kwh=3.14, cost_eur=0.95),
               _charge(group_id="b", energy_kwh=1.01, solar_kwh=0.0,
                       battery_kwh=0.0, grid_kwh=1.01, cost_eur=0.31)]
    html = _run(_payload(charges))

    # A legend chip is a coloured dot immediately followed by its name.
    chips = re.findall(r"border-radius:50%;background:(#[0-9a-f]{6})[^>]*>"
                       r"</span>\s*([A-Za-zÄÖÜäöüß]{2,20})", html)
    mapping = {}
    for col, label in chips:
        mapping.setdefault(col, set()).add(label)
    for colour, want in (("#fdd835", "Solar"), ("#22c55e", "Battery"),
                         ("#ef4444", "Grid")):
        assert colour in mapping, f"{want} has no labelled dot anywhere"
        assert mapping[colour] == {want}, f"{colour} is labelled {mapping[colour]}"

    # …and each of the three colours must be named more than once: once for the
    # whole list is not enough when 50 cards scroll past it.
    for colour in ("#fdd835", "#22c55e", "#ef4444"):
        n = len(re.findall(r"border-radius:50%;background:" + colour, html))
        assert n >= 3, f"{colour} is explained only {n}× (overview, key, cards)"

    # No coloured bar may sit far from a key.
    bar = html.index('background:#fdd835"')
    keys = [m.start() for m in re.finditer(r"border-radius:50%;background:#fdd835", html)]
    assert any(abs(k - bar) < 4000 for k in keys), "a bar with no legend in reach"
    print(f"OK  {len(chips)} legend chips; each colour named exactly once, everywhere")


def test_the_legend_is_translated_not_english_everywhere():
    """A key that only reads in English is not a key for a German user."""
    from shelly_analyzer.i18n import get_lang_map
    charges = [_charge(group_id="a", energy_kwh=29.0, solar_kwh=19.38,
                       battery_kwh=6.44, grid_kwh=3.14, cost_eur=0.95)]
    html = _run(_payload(charges), _lang="de")
    assert "Akku" in html and "Netz" in html, "the German labels are missing"
    assert not re.search(r"background:#22c55e[^>]*></span>\s*Battery", html), \
        "the battery chip is still English in a German page"
    print("OK  the legend speaks the page's language")


CURVE_TAIL = r"""
const out = { innerHTML: '' };
renderEvLog(PAYLOAD, out);
// Unfold the first charge the way a click does, with the fetch answered from
// the payload we planted — then paint it and report what landed on the canvas.
globalThis.fetch = () => Promise.resolve({ ok: true, json: () => Promise.resolve({ok: true, data: CURVE}) });
globalThis.document.getElementById = (id) => (globalThis.__boxes[id] = globalThis.__boxes[id] || _el());
evToggleCurve('a', CURVE.start_ts, CURVE.end_ts).then(() => {
  process.stdout.write('<<<HTML>>>' + (globalThis.__boxes['evc-a'] || {}).innerHTML +
    '\n<<<PAINT>>>' + JSON.stringify(globalThis.__paint));
});
"""


def _run_curve(payload, curve):
    src = (HARNESS + "\nglobalThis.__boxes = {};\n"
           + "const PAYLOAD = " + json.dumps(payload) + ";\n"
           + "const CURVE = " + json.dumps(curve) + ";\n"
           + _script() + "\n" + CURVE_TAIL)
    p = subprocess.run(["node", "-e", src], capture_output=True, text=True, timeout=120)
    assert p.returncode == 0, f"node failed:\n{p.stderr[-3000:]}"
    body, paint = p.stdout.split("<<<HTML>>>", 1)[1].split("\n<<<PAINT>>>", 1)
    return body, json.loads(paint)


def _curve(n=60):
    """A charge that starts on sun, dips onto the battery, then pulls the grid."""
    ts = [1786023600 + i * 60 for i in range(n)]
    sol, bat, grid, load = [], [], [], []
    for i in range(n):
        if i < n // 3:
            s_, b_, g_ = 6600.0, 0.0, 0.0
        elif i < 2 * n // 3:
            s_, b_, g_ = 3000.0, 3600.0, 0.0
        else:
            s_, b_, g_ = 1000.0, 600.0, 5000.0
        sol.append(s_); bat.append(b_); grid.append(g_); load.append(s_ + b_ + g_)
    return {"available": True, "start_ts": ts[0], "end_ts": ts[-1], "ts": ts,
            "load_w": load, "solar_w": sol, "battery_w": bat, "grid_w": grid,
            "measured": [True] * n, "points": n, "raw_points": n,
            "seconds": {"solar": n * 60, "battery": 2 * n // 3 * 60,
                        "grid": n // 3 * 60, "total": n * 60}}


def test_a_charge_unfolds_into_a_curve_coloured_by_source():
    """Robert: „wenn man so ladevorgangs balken anklickt und er aufklappt und
    die ladekurve zeigt und diese auch je nach stromart … farblich einfärbt"."""
    charges = [_charge(group_id="a", energy_kwh=29.0, solar_kwh=19.4,
                       battery_kwh=6.5, grid_kwh=3.1, cost_eur=0.94)]
    body, paint = _run_curve(_payload(charges), _curve())
    assert "<canvas" in body, "no canvas was placed in the panel"
    for colour in ("#fdd835", "#22c55e", "#ef4444"):
        assert colour in paint["fills"], f"the {colour} band was never filled"
    # The bands are stacked, so exactly three area fills plus the load stroke.
    assert paint["strokes"], "the measured wallbox curve was not drawn"
    assert any("kW" in t for t in paint["texts"]), "no power axis was labelled"
    # …and the panel says how long each source flowed, named.
    for word in ("Solar", "Battery", "Grid"):
        assert word in body, f"{word} is not named under the curve"
    print(f"OK  the curve paints {len(paint['fills'])} bands, "
          f"{len(paint['texts'])} axis labels, and names every source")


def test_a_charge_without_a_measurement_says_so_instead_of_drawing():
    charges = [_charge(group_id="a", solar_kwh=14.17)]
    body, paint = _run_curve(_payload(charges),
                             {"available": False, "start_ts": 1, "end_ts": 2})
    assert "<canvas" not in body
    assert "No source measurement" in body
    assert "#fdd835" not in paint["fills"], "it painted a band with no data"
    print("OK  an unmeasured charge explains itself instead of drawing a curve")


def test_the_hover_readout_names_all_three_at_once():
    """The case Robert asked about: sun, battery and grid feeding at the same
    minute. The readout must show all three, not pick a winner."""
    charges = [_charge(group_id="a", energy_kwh=29.0, solar_kwh=19.4,
                       battery_kwh=6.5, grid_kwh=3.1, cost_eur=0.94)]
    curve = _curve()
    src = (HARNESS + "\nglobalThis.__boxes = {};\n"
           + "const PAYLOAD = " + json.dumps(_payload(charges)) + ";\n"
           + "const CURVE = " + json.dumps(curve) + ";\n" + _script() + r"""
const out = { innerHTML: '' };
renderEvLog(PAYLOAD, out);
globalThis.fetch = () => Promise.resolve({ ok:true, json: () => Promise.resolve({ok:true,data:CURVE}) });
globalThis.document.getElementById = (id) => (globalThis.__boxes[id] = globalThis.__boxes[id] || _el());
evToggleCurve('a', CURVE.start_ts, CURVE.end_ts).then(() => {
  _evPaintCurve('a', 700);          // hover near the end, where all three run
  process.stdout.write('<<<TIP>>>' + (globalThis.__boxes['evct-a']||{}).innerHTML);
});
""")
    p = subprocess.run(["node", "-e", src], capture_output=True, text=True, timeout=120)
    assert p.returncode == 0, p.stderr[-2000:]
    tip = p.stdout.split("<<<TIP>>>", 1)[1]
    for colour in ("#fdd835", "#22c55e", "#ef4444"):
        assert colour in tip, f"the readout omits {colour}"
    assert tip.count("kW") >= 4, tip[:300]
    print("OK  the hover readout gives all three sources for that minute")


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
