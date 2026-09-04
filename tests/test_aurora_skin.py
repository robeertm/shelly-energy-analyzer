"""Aurora skin — the guarantees that matter.

Run: python3 tests/test_aurora_skin.py   (no pytest dependency)

The central promise of this feature is that turning the skin OFF leaves the
classic dashboard exactly as it was.  That promise rests on two structural
facts, and the first two tests below are the ones that keep it true:

  · ``_inject_skin(html, "classic")`` is the identity function, so there is
    exactly one code path difference between the skins and it is that call;
  · every rule in ``aurora.css`` is scoped to ``html[data-skin="aurora"]``, an
    attribute the classic page never carries, so no rule can match.

The rest cover the config round-trip, the colour rule, the i18n coverage, and
the four product bugs found while building the skin.
"""
import ast
import json
import os
import re
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.io.config import (  # noqa: E402
    AppConfig, DeviceConfig, UiConfig, load_config, save_config,
)
from shelly_analyzer.i18n import LANGS, resolve_name, t as _t  # noqa: E402
from shelly_analyzer.web import _inject_skin, active_skin, skin_asset  # noqa: E402

SRC = os.path.join(os.path.dirname(__file__), "..", "src", "shelly_analyzer")
CSS_PATH = os.path.join(SRC, "web", "static", "aurora.css")
JS_PATH = os.path.join(SRC, "web", "static", "aurora.js")

SAMPLE = ('<!doctype html>\n<html lang="en">\n<head><title>x</title></head>\n'
          '<body><div id="app"></div></body>\n</html>')


class _State:
    def __init__(self, skin):
        self.cfg = AppConfig(devices=[], ui=UiConfig(skin=skin))


# ── The promise ───────────────────────────────────────────────────────────

def test_classic_injection_is_identity():
    out = _inject_skin(SAMPLE, "classic")
    assert out == SAMPLE, "the classic page must come back byte-for-byte"
    # Anything unrecognised must also be a no-op rather than a half-applied skin.
    for weird in ("", "AURORA", "nonsense", None):
        assert _inject_skin(SAMPLE, weird) == SAMPLE, f"skin={weird!r} changed the page"
    print("OK  classic injection is the identity function")


def test_every_css_rule_is_scoped_to_the_skin():
    """The leak guard: one unscoped rule would reach the classic dashboard."""
    src = open(CSS_PATH, encoding="utf-8").read()
    body = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    assert body.count("{") == body.count("}"), "unbalanced braces in aurora.css"

    depth, buf, at_stack, unscoped, rules = 0, "", [], [], 0
    for ch in body:
        if ch == "{":
            sel, buf = buf.strip(), ""
            if sel.startswith("@"):
                at_stack.append(sel)
            else:
                rules += 1
                if not any(a.startswith("@keyframes") for a in at_stack):
                    for part in sel.split(","):
                        p = part.strip()
                        if p and not p.startswith('html[data-skin="aurora"]'):
                            unscoped.append(p)
            depth += 1
        elif ch == "}":
            depth -= 1
            if at_stack and depth == len(at_stack) - 1:
                at_stack.pop()
            buf = ""
        else:
            buf += ch
    assert not unscoped, f"{len(unscoped)} rule(s) escape the skin: {unscoped[:5]}"
    assert rules > 100, f"only {rules} rules parsed — did the file get truncated?"
    print(f"OK  all {rules} CSS rules are scoped to html[data-skin=\"aurora\"]")


def test_aurora_injection():
    out = _inject_skin(SAMPLE, "aurora")
    assert 'data-skin="aurora"' in out, "the root element must carry the skin"
    assert out.index('data-skin="aurora"') < out.index("</head>")
    assert "/static/aurora.css?v=" in out, "stylesheet not linked"
    assert "/static/aurora.js?v=" in out, "script not linked"
    assert out.index("aurora.css") < out.index("</head>"), "stylesheet must be in <head>"
    assert out.index("aurora.js") > out.index("<body>"), "script must come after the markup"
    # Version-stamped so an update busts the immutable cache.
    from shelly_analyzer import __version__
    assert f"aurora.css?v={__version__}" in out
    # Applying it twice must not double up.
    assert _inject_skin(out, "aurora") == out, "injection is not idempotent"
    print("OK  aurora injection: stamped, ordered, idempotent")


def test_active_skin_falls_back_to_classic():
    assert active_skin(_State("aurora")) == "aurora"
    assert active_skin(_State("classic")) == "classic"

    class Broken:
        @property
        def cfg(self):
            raise RuntimeError("config unreadable")
    # A page with no styling beats a page that half-applied one.
    assert active_skin(Broken()) == "classic"
    print("OK  an unreadable config falls back to classic, not to a half-skin")


# ── Config ────────────────────────────────────────────────────────────────

def test_skin_config_roundtrip():
    assert UiConfig.skin == "aurora", "Aurora is the default from v16.71.0"
    with tempfile.TemporaryDirectory() as d:
        p = os.path.join(d, "config.json")
        # Absent from the file → the default.
        json.dump({"devices": [], "ui": {}}, open(p, "w"))
        assert load_config(p).ui.skin == "aurora"
        for written, expected in (("classic", "classic"), ("aurora", "aurora"),
                                  ("CLASSIC", "classic"), ("  classic  ", "classic"),
                                  ("banana", "aurora"), ("", "aurora")):
            json.dump({"devices": [], "ui": {"skin": written}}, open(p, "w"))
            got = load_config(p).ui.skin
            assert got == expected, f"skin {written!r} loaded as {got!r}, expected {expected!r}"
        # And it survives a save.
        cfg = load_config(p)
        cfg = AppConfig(devices=[], ui=UiConfig(skin="classic"))
        save_config(cfg, p)
        assert json.load(open(p))["ui"]["skin"] == "classic", "skin missing from the saved file"
        assert load_config(p).ui.skin == "classic"
    print("OK  ui.skin round-trips and normalises")


# ── Assets ────────────────────────────────────────────────────────────────

def test_assets_are_served_and_sane():
    css = skin_asset("aurora.css").decode("utf-8")
    js = skin_asset("aurora.js").decode("utf-8")
    assert len(css) > 20000, f"aurora.css is only {len(css)} bytes"
    assert len(js) > 10000, f"aurora.js is only {len(js)} bytes"
    # Nothing else may be read through this door.
    for bad in ("../config.json", "aurora.css/../../x", "settings.html", "aurora.png"):
        try:
            skin_asset(bad)
        except FileNotFoundError:
            continue
        raise AssertionError(f"skin_asset served {bad!r}")
    print("OK  both assets served; the route refuses anything else")


def test_js_is_a_single_iife():
    js = open(JS_PATH, encoding="utf-8").read()
    assert js.lstrip().startswith("/*"), "the file should open with its explanation"
    assert "(function () {" in js and js.rstrip().endswith("})();"), \
        "the script must be wrapped so it leaks nothing into the page's globals"
    assert '"use strict"' in js
    # It must bail out when the skin is not active, however it got loaded.
    assert 'getAttribute("data-skin") !== "aurora"' in js
    assert "window.__auroraSkin" in js, "no double-run guard"
    print("OK  aurora.js is one guarded IIFE")


def test_js_never_replaces_the_dashboards_own_functions():
    """Every hook wraps and calls through — the skin must not take over rendering."""
    js = open(JS_PATH, encoding="utf-8").read()
    assert "base.apply(this, arguments)" in js, "renderLive must call through"
    assert "wrapped.__auWrapped = true" in js, "hooks must be marked to avoid double wrapping"
    assert js.count("__auWrapped") >= 4, "each hook needs an installed-check and a mark"
    print("OK  the render hook wraps and calls through")


def test_navigation_rail_stays_reachable():
    """Two ways the rail could hide the tab you are on.

    1. The base centres it with ``justify-content: center``.  On a scrolling
       flex container whose content is wider than the box, centring overflows
       BOTH sides — and the left overflow cannot be scrolled to, because
       scrollLeft 0 is already the leftmost position.  With 24 tabs that put
       Live and Plots permanently off-screen.
    2. Following clicks is not enough: the dashboard restores the last tab from
       localStorage on load and the command palette switches panes without a
       click, so the rail stayed where it was."""
    css = open(CSS_PATH, encoding="utf-8").read()
    assert "justify-content: safe center" in css, \
        "the rail centres unsafely again — the first tabs become unreachable"
    i = css.index("justify-content: center")
    j = css.index("justify-content: safe center")
    assert i < j, "the plain value must come first as the fallback"

    js = open(JS_PATH, encoding="utf-8").read()
    fn = js[js.index("function followNav("):]
    fn = fn[:fn.index("\n  }")]
    assert "MutationObserver" in fn and 'attributeFilter: ["class"]' in fn, \
        "the rail follows clicks only — a restored or palette-driven tab is missed"
    print("OK  the rail centres safely and follows the active tab however it moved")


# ── The colour rule ───────────────────────────────────────────────────────

def test_colour_rule_documented_thresholds():
    """The rule that keeps tint/text pairs readable lives in exactly one place."""
    js = open(JS_PATH, encoding="utf-8").read()
    assert js.count("function mapColour(") == 1, "the colour rule must exist once"
    m = re.search(r"hsl\[1\] < ([\d.]+) \|\| hsl\[2\] < ([\d.]+) \|\| hsl\[2\] > ([\d.]+)", js)
    assert m, "the grey/extreme guard is gone"
    sat, lo, hi = (float(x) for x in m.groups())
    assert 0.2 < sat < 0.5 and 0.05 < lo < 0.3 and 0.8 < hi < 0.99, (sat, lo, hi)
    # The identity band is what separates "a chart colour" from "a tint".
    assert re.search(r"hsl\[2\] >= 0\.\d+ && hsl\[2\] <= 0\.\d+", js), \
        "the mid-tone identity band is gone — tint/text pairs will collapse"
    # Both input notations must be understood, or half the dashboard slips past.
    assert "rgba?\\(" in js or "rgba?(" in js
    assert "hsla?" in js, "hsl() inputs would bypass the mapper"
    assert "function hslToRgb" in js
    print("OK  the colour rule keeps its guard, its identity band and both notations")


def test_no_generated_selector_table_left_behind():
    """The rule is implemented once, in JS — not duplicated as CSS selectors."""
    css = open(CSS_PATH, encoding="utf-8").read()
    hexes = re.findall(r'\[style\*="[a-z-]+:\s*#[0-9a-f]{6}"\]', css)
    assert not hexes, f"{len(hexes)} generated colour selectors are back in the stylesheet"
    print("OK  no duplicated colour table in the CSS")


# ── i18n ──────────────────────────────────────────────────────────────────

def test_skin_strings_exist_in_every_language():
    keys = ["aurora.hero.title", "aurora.load.idle", "aurora.load.low",
            "aurora.load.working", "aurora.load.high", "aurora.load.peak",
            "aurora.stat.today", "aurora.stat.cost", "aurora.stat.solar",
            "aurora.stat.biggest", "settings.field.ui.skin",
            "settings.opts.ui.skin.aurora", "settings.opts.ui.skin.classic",
            "settings.hint.ui.skin"]
    missing = [(l, k) for l in LANGS for k in keys if _t(l, k) == k]
    assert not missing, f"untranslated: {missing[:6]}"
    # A real translation, not the English string copied over.
    assert _t("de", "aurora.load.working") != _t("en", "aurora.load.working")
    print(f"OK  {len(keys)} skin strings present in all {len(LANGS)} languages")


def test_battery_tab_is_translated():
    """The Battery tab used to be hard-coded German whatever the language."""
    keys = ["battery.title", "battery.soc", "battery.stored", "battery.power",
            "battery.status", "battery.eta", "battery.eta.full", "battery.eta.empty",
            "battery.mode.charging", "battery.mode.discharging", "battery.mode.idle",
            "battery.src.measured", "battery.src.estimated", "battery.soc_history",
            "battery.today", "battery.period", "battery.days", "battery.charged",
            "battery.discharged", "battery.cycles", "battery.efficiency"]
    missing = [(l, k) for l in LANGS for k in keys if _t(l, k) == k]
    assert not missing, f"untranslated: {missing[:6]}"
    assert _t("en", "battery.title") == "Battery storage"
    assert _t("de", "battery.title") == "Batteriespeicher"

    # And the renderer must no longer carry the German literals.
    webdash = open(os.path.join(SRC, "services", "webdash.py"), encoding="utf-8").read()
    for german in ("Batteriespeicher '", "'Ladestand'", "'Vollzyklen'",
                   "'Wirkungsgrad'", "'Gespeichert'", "Ladestand-Verlauf ("):
        assert german not in webdash, f"hard-coded German left in the renderer: {german!r}"
    print(f"OK  {len(keys)} battery strings translated; no German literals left")


def test_english_ui_never_shows_german():
    """The Battery tab was German in an English UI; so was the Weather tab's
    "no data".  The invariant: no key the dashboard asks for may fall back to a
    German literal when the language is English."""
    webdash = open(os.path.join(SRC, "services", "webdash.py"), encoding="utf-8").read()
    tpl = webdash[webdash.index('_HTML_TEMPLATE = """'):webdash.index('_PLOTS_TEMPLATE = """')]
    calls = {}
    for m in re.finditer(r"""\bt\(\s*'([a-z0-9_.]+)'\s*,\s*'((?:[^'\\]|\\.)*)'""", tpl):
        calls.setdefault(m.group(1), m.group(2))
    assert len(calls) > 400, f"only {len(calls)} t() calls found — did the scan break?"

    from shelly_analyzer.i18n import _I18N
    german = re.compile(r"[äöüÄÖÜß]|\b(der|die|das|und|nicht|werden|Bitte|Geräte|"
                        r"keine|Keine|Zähler|löschen|eingeben|Rohdaten|Verbrauch)\b")
    leaks = [(k, v) for k, v in calls.items()
             if k not in _I18N.get("en", {}) and german.search(v)]
    assert not leaks, ("keys with no English translation and a German fallback:\n  "
                       + "\n  ".join(f"{k}: {v[:60]!r}" for k, v in leaks[:8]))
    print(f"OK  {len(calls)} dashboard keys — none falls back to German in English")


def test_dispatcher_speaks_the_configured_language():
    """ActionDispatcher defaults to lang="de" and nothing used to tell it
    otherwise, so every string it translates server-side came back German for
    every user — the Weather tab's "no data" among them."""
    main = open(os.path.join(SRC, "__main__.py"), encoding="utf-8").read()
    assert "ActionDispatcher(" in main
    call = main[main.index("ActionDispatcher("):]
    call = call[:call.index(")\n")]
    assert "lang=" in call, "the dispatcher is still constructed without a language"

    ctx = open(os.path.join(SRC, "web", "app_context.py"), encoding="utf-8").read()
    assert "dispatcher.reload(cfg, lang=" in ctx, "a config reload drops the language again"
    assert "dispatcher.lang = new_lang" in ctx, "changing the language leaves the dispatcher behind"
    print("OK  the dispatcher is told the language at boot, on reload and on change")


def test_resolve_name():
    # A demo device carries a translation key as its name.
    assert resolve_name("en", "demo.device.house_3p") == "Demo House (3-phase)"
    assert resolve_name("de", "demo.device.house_3p") == "Demo Haus (3-phasig)"
    # Anything a person typed comes back untouched, dots and all.
    for name in ("Keller", "Pool 2.0", "Haus", "wallbox.garage", "", "A.B.C"):
        assert resolve_name("en", name) == name, f"user name {name!r} was rewritten"
    print("OK  resolve_name translates keys and leaves real names alone")


def test_demo_device_names_resolve_once_at_load():
    """One call site: the config loader.  Everything downstream inherits it."""
    src = open(os.path.join(SRC, "io", "config.py"), encoding="utf-8").read()
    assert "_resolve_device_name" in src
    calls = 0
    for path, _, files in os.walk(SRC):
        for f in files:
            if f.endswith(".py"):
                calls += open(os.path.join(path, f), encoding="utf-8").read().count(
                    "resolve_name(")
    # i18n.py defines it (1) + config.py calls it (1) + the helper's own call (1)
    assert calls <= 4, f"resolve_name is called from {calls} places — keep it to one"

    with tempfile.TemporaryDirectory() as d:
        p = os.path.join(d, "config.json")
        json.dump({"devices": [{"key": "demo1", "name": "demo.device.house_3p",
                                "host": "demo://house"}],
                   "ui": {"language": "en"}}, open(p, "w"))
        assert load_config(p).devices[0].name == "Demo House (3-phase)"
        json.dump({"devices": [{"key": "demo1", "name": "demo.device.house_3p",
                                "host": "demo://house"}],
                   "ui": {"language": "de"}}, open(p, "w"))
        assert load_config(p).devices[0].name == "Demo Haus (3-phasig)"
    print("OK  device names resolve at load, in the configured language")


# ── The product bugs the skin work turned up ──────────────────────────────

def test_demo_mode_gets_the_demo_poller():
    """``DemoMultiLivePoller`` existed but had no call site: demo mode showed a
    live view that never updated, because ``demo://…`` is not a real host."""
    src = open(os.path.join(SRC, "web", "background.py"), encoding="utf-8").read()
    assert "DemoMultiLivePoller" in src, "the demo poller is unreachable again"
    assert 'startswith("demo://")' in src
    tree = ast.parse(src)
    names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
    assert "DemoMultiLivePoller" in names and "MultiLivePoller" in names
    print("OK  demo devices are routed to the generator, not to HTTP")


def test_demo_house_never_draws_nothing():
    """The random walk used to be wider than the night base load, so
    ``max(0.0, …)`` pinned the house at exactly 0 W for hours every evening."""
    import time
    from shelly_analyzer.services.demo import (
        DemoState, default_demo_devices, gen_sample)
    devs = default_demo_devices()
    st = DemoState(seed=1234, scenario="household")
    lt = time.localtime()
    midnight = time.time() - (lt.tm_hour * 3600 + lt.tm_min * 60 + lt.tm_sec)
    lo, hi = 1e9, 0.0
    for k in range(0, 86400, 60):          # a whole day, one sample a minute
        w = gen_sample(devs[0], midnight + k, st)["power_w"].get("total", 0.0)
        lo, hi = min(lo, w), max(hi, w)
    assert lo >= 40.0, f"the demo house fell to {lo:.1f} W — a house has a standby floor"
    assert 1500 < hi < 8000, f"peak {hi:.0f} W is not a household peak"
    print(f"OK  demo house stays between {lo:.0f} W and {hi:.0f} W all day")


def test_no_local_import_shadows_a_module_global():
    """``import time as _t`` inside ``dispatch()`` made ``_t`` local to the whole
    method, so the translator call 1 300 lines earlier raised UnboundLocalError
    and the Weather tab showed a Python error instead of "no data"."""
    bad = []
    for path, _, files in os.walk(SRC):
        for f in files:
            if not f.endswith(".py"):
                continue
            full = os.path.join(path, f)
            tree = ast.parse(open(full, encoding="utf-8").read())
            for node in ast.walk(tree):
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                # Parameters are bound at call time, and a `global`/`nonlocal`
                # name is not local at all — neither can be shadowed this way.
                a = node.args
                exempt = {x.arg for x in
                          a.posonlyargs + a.args + a.kwonlyargs
                          + ([a.vararg] if a.vararg else [])
                          + ([a.kwarg] if a.kwarg else [])}
                for n in ast.walk(node):
                    if isinstance(n, (ast.Global, ast.Nonlocal)):
                        exempt.update(n.names)
                # Only an IMPORT binding is checked: that is the shadow that
                # silently swallows a module-level name for the whole function.
                # A plain assignment before its own use is ordinary Python and
                # is caught by the interpreter the first time the branch runs.
                # 🔴 ast.walk() is not source order, so the EARLIEST binding has
                # to be taken with min() — setdefault() records whichever node the
                # walk happened to reach first and flags perfectly ordinary code.
                binds = {}
                for n in ast.walk(node):
                    if isinstance(n, (ast.Import, ast.ImportFrom)):
                        for al in n.names:
                            nm = al.asname or al.name.split(".")[0]
                            if nm not in exempt:
                                binds[nm] = min(binds.get(nm, n.lineno), n.lineno)
                for n in ast.walk(node):
                    if (isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)
                            and n.id in binds and n.lineno < binds[n.id]):
                        bad.append(f"{os.path.basename(full)}:{n.lineno} reads "
                                   f"{n.id!r} before it is bound at line {binds[n.id]} "
                                   f"in {node.name}()")
    assert not bad, "names read before their local binding:\n  " + "\n  ".join(bad[:8])
    print("OK  no function reads a name before its own local binding")


if __name__ == "__main__":
    test_classic_injection_is_identity()
    test_every_css_rule_is_scoped_to_the_skin()
    test_aurora_injection()
    test_active_skin_falls_back_to_classic()
    test_skin_config_roundtrip()
    test_assets_are_served_and_sane()
    test_js_is_a_single_iife()
    test_js_never_replaces_the_dashboards_own_functions()
    test_navigation_rail_stays_reachable()
    test_colour_rule_documented_thresholds()
    test_no_generated_selector_table_left_behind()
    test_skin_strings_exist_in_every_language()
    test_battery_tab_is_translated()
    test_english_ui_never_shows_german()
    test_dispatcher_speaks_the_configured_language()
    test_resolve_name()
    test_demo_device_names_resolve_once_at_load()
    test_demo_mode_gets_the_demo_poller()
    test_demo_house_never_draws_nothing()
    test_no_local_import_shadows_a_module_global()
    print("\nAll Aurora skin tests passed.")
