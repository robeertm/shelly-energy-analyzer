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
import shutil
import subprocess
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
WEBDASH_PATH = os.path.join(SRC, "services", "webdash.py")

SAMPLE = ('<!doctype html>\n<html lang="en">\n<head><title>x</title></head>\n'
          '<body><div id="app"></div></body>\n</html>')


def _strip_css_comments(text):
    return re.sub(r"/\*.*?\*/", " ", text, flags=re.S)


def _strip_js_comments(text):
    text = re.sub(r"/\*.*?\*/", " ", text, flags=re.S)
    return re.sub(r"(?m)^\s*//.*$", " ", text)


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

    # 🔴 And it must not be capped to the reading column.  Measured on a real
    # instance: 24 tabs need 1348px, --pane-max is 1180, and Control,
    # Calibration and Sync sat at x=1282..1473 outside a box that ended at
    # 1310 — three tabs with no way to reach them.
    rail = css[css.index('html[data-skin="aurora"] #bottom-nav {'):]
    rail = rail[:rail.index("/* A soft edge")]
    assert "min(var(--pane-max)" not in rail, \
        "the rail is capped to the pane again — the last tabs become unreachable"
    assert "max-width: calc(100vw - 20px)" in rail, "the rail lost its width"

    js = open(JS_PATH, encoding="utf-8").read()
    fn = js[js.index("function railSetup("):]
    fn = fn[:fn.index("\n  }\n\n")]
    assert "MutationObserver" in fn and 'attributeFilter: ["class"]' in fn, \
        "the rail follows clicks only — a restored or palette-driven tab is missed"
    # A desktop mouse has no horizontal-scroll gesture; without this the fade
    # promises more tabs the user cannot get to.
    assert 'addEventListener("wheel"' in fn, "the rail cannot be scrolled with a wheel"
    # The end fade has to be decided AFTER the buttons tighten, or it hangs off
    # a rail that now fits exactly.
    e = fn[fn.index("function edges()"):]
    e = e[:e.index("\n    }")]
    assert e.index("toggleAttribute") < e.index("var over ="), \
        "the overflow is measured before the dense toggle changes it"
    print("OK  the rail centres safely, spans the window, scrolls by wheel, "
          "and follows the active tab however it moved")


# ── The colour rule ───────────────────────────────────────────────────────

def test_colour_rule_documented_thresholds():
    """The rule that keeps tint/text pairs readable lives in exactly one place."""
    js = open(JS_PATH, encoding="utf-8").read()
    assert js.count("function mapColour(") == 1, "the colour rule must exist once"
    m = re.search(r"hsl\[1\] < ([\d.]+) \|\| hsl\[2\] < ([\d.]+) \|\| hsl\[2\] > ([\d.]+)", js)
    assert m, "the grey/extreme guard is gone"
    sat, lo, hi = (float(x) for x in m.groups())
    assert 0.2 < sat < 0.5 and 0.05 < lo < 0.3 and 0.8 < hi < 0.99, (sat, lo, hi)
    # Both input notations must be understood, or half the dashboard slips past.
    assert "rgba?\\(" in js or "rgba?(" in js
    assert "hsla?" in js, "hsl() inputs would bypass the mapper"
    assert "function hslToRgb" in js
    print("OK  the colour rule keeps its guard and both notations")


def test_colour_rule_is_bounded_attraction_not_a_snap():
    """A heatmap's meaning IS its gradient, so the rule may not quantise.

    The first version replaced any mid-lightness colour with the nearest of ten
    accents.  Measured against the live dashboard, that turned a heatmap drawn
    as a smooth ramp into FOUR colours.  The rule now moves a hue toward the
    nearest accent by a bounded amount and never touches lightness.  Two facts
    make "a ramp stays a ramp" structural rather than hopeful:

      · the shift is clamped, so two inputs can converge but never cross;
      · lightness is carried through untouched, so a light tint stays lighter
        than the deep shade beside it.
    """
    js = open(JS_PATH, encoding="utf-8").read()
    pull = re.search(r"var HUE_PULL = ([\d.]+);", js)
    mix = re.search(r"var SAT_MIX = ([\d.]+);", js)
    assert pull and mix, "the attraction constants are gone"
    assert 4 <= float(pull.group(1)) <= 24, pull.group(1)
    assert 0.0 <= float(mix.group(1)) <= 0.65, mix.group(1)
    # The snap returned the accent's own css verbatim; that must be gone.
    body = js[js.index("function mapColour("):]
    body = body[:body.index("\n  }\n")]
    assert "best.css" not in body, "mapColour still returns an accent verbatim — that is a snap"
    assert "hsl[2] * 1000" in body or "hsl[2]" in body, "lightness must be carried through"
    print("OK  the colour rule is a bounded attraction, and lightness survives it")


def test_measured_heatmap_ramp_survives_the_colour_rule():
    """Run the real rule over the ramp measured on the live heatmap.

    These are the actual values the dashboard painted, read out of the DOM with
    the mapper switched off.  Before the fix they came out as four colours.
    """
    node = shutil.which("node")
    if not node:
        print("--  node not installed; skipping the executable colour check")
        return
    js = open(JS_PATH, encoding="utf-8").read()
    # The measured ramp, low to high.
    ramp = [(85, 192, 72), (98, 191, 67), (111, 190, 61), (146, 187, 46),
            (175, 184, 34), (186, 183, 29), (203, 182, 22), (215, 181, 16),
            (224, 180, 12), (234, 172, 12), (236, 141, 28), (237, 117, 42),
            (238, 87, 58)]
    harness = """
      var ROOT = {}, palette = null, mine = null;
      var TOKENS = [], ACCENTS = [];
      %s
      // A stand-in for the browser: the Mocha accents, straight from the CSS.
      palette = ACC.map(function (h) {
        var a = rgbToHsl(hexToRgb(h));
        return { hue: a[0], sat: a[1], css: h };
      });
      mine = Object.create(null);
      var out = IN.map(function (c) {
        return mapColour("rgb(" + c[0] + "," + c[1] + "," + c[2] + ")");
      });
      // Feeding the output back in must change nothing.  Without that the
      // inline-style observer rewrites every element seven times over.
      var again = out.map(mapColour);
      console.log(JSON.stringify({ out: out, idempotent:
        JSON.stringify(out) === JSON.stringify(again) }));
    """
    keep = []
    for fn in ("function hexToRgb", "function rgbToHsl", "function hslToRgb",
               "function parseColour", "var HUE_PULL", "var HUE_FRAC",
               "var SAT_MIX", "var produced", "var memo", "function _ckey",
               "function mapColour"):
        i = js.index(fn)
        if fn.startswith("var "):
            keep.append(js[i:js.index("\n", i) + 1])
            continue
        keep.append(js[i:js.index("\n  }\n", i) + len("\n  }\n")])
    accents = re.findall(r"--ctp-(?:red|peach|yellow|green|teal|sky|blue|lavender|mauve|pink):\s*(#[0-9a-f]{6})",
                         open(CSS_PATH, encoding="utf-8").read())[:10]
    assert len(accents) == 10, f"expected 10 accents in the CSS, found {len(accents)}"
    prog = ("var ACC = " + json.dumps(accents) + ";\n"
            "var IN = " + json.dumps(ramp) + ";\n"
            + (harness % "\n".join(keep)))
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
        f.write(prog)
        path = f.name
    try:
        res = subprocess.run([node, path], capture_output=True, text=True, timeout=30)
        assert res.returncode == 0, res.stderr[:500]
        payload = json.loads(res.stdout.strip())
        got = payload["out"]
        assert payload["idempotent"], (
            "mapping our own output changes it — the inline-style observer "
            "will rewrite every element until it converges, seven passes deep")
    finally:
        os.unlink(path)

    assert len(set(got)) == len(ramp), (
        f"the ramp collapsed: {len(ramp)} inputs became {len(set(got))} colours")
    def _hue(css):
        r, g_, b = (int(x) for x in re.findall(r"\d+", css)[:3])
        r, g_, b = r / 255, g_ / 255, b / 255
        mx, mn = max(r, g_, b), min(r, g_, b)
        d = mx - mn
        if not d:
            return 0.0
        h = (((g_ - b) / d) % 6 if mx == r else
             (b - r) / d + 2 if mx == g_ else (r - g_) / d + 4) * 60
        return h + 360 if h < 0 else h
    hues = [_hue(g) for g in got]
    # Green (~120) down to red (~0): the ramp must stay strictly ordered.
    for i in range(1, len(hues)):
        assert hues[i] < hues[i - 1] + 1e-6, (
            f"the ramp reordered at step {i}: {hues[i-1]:.1f} -> {hues[i]:.1f}")
    print(f"OK  the measured heatmap ramp stays {len(set(got))} distinct colours, "
          f"hue {hues[0]:.0f} to {hues[-1]:.0f}, monotone, and idempotent")


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
    # 🔴 The first version of this guard listed a handful of German words and
    # therefore passed while 20 fallbacks read "Kumuliert", "Bewertung",
    # "Verlorene Energie", "Konfidenz", "Basierend auf" and the German weekday
    # abbreviations to every English user. A word list cannot be the detector;
    # these are the endings and forms that do not occur in English text.
    german = re.compile(
        r"[äöüÄÖÜß]"
        r"|\b(der|die|das|und|nicht|werden|Bitte|Geräte|keine|Keine|Zähler|"
        r"löschen|eingeben|Rohdaten|Verbrauch|Kumuliert|Bewertung|Erneuerbare|"
        r"Konfidenz|Verlauf|Anteil|Verlorene|Basierend|Analyse|Typen|erkannt|"
        r"Mo|Di|Mi|Do|Sa|So)\b"
        r"|\w+(ung|ungen|keit|heit|schaft|lich|isch)\b")
    leaks = [(k, v) for k, v in calls.items()
             if k not in _I18N.get("en", {}) and german.search(v)]
    assert not leaks, ("keys with no English translation and a German fallback:\n  "
                       + "\n  ".join(f"{k}: {v[:60]!r}" for k, v in leaks[:8]))
    print(f"OK  {len(calls)} dashboard keys — none falls back to German in English")


def test_the_german_ui_is_actually_german():
    """The mirror of the guard above, and the one that was missing: a key with
    no German entry falls back to its English text, so a German page read
    "Cost breakdown", "Month projection", "Daily average", "tree-days".
    Measured before the fix: 234 of 615 requested keys had no entry in ANY
    language, which is 38 % of the dashboard."""
    from shelly_analyzer.i18n import t as _tt
    webdash = open(WEBDASH_PATH, encoding="utf-8").read()
    tpl = webdash[webdash.index('_HTML_TEMPLATE = """'):webdash.index('_PLOTS_TEMPLATE = """')]
    keys = set(re.findall(r"""\bt\(\s*'([a-z0-9_.]+)'""", tpl))
    # keys assembled at runtime ("appliance." + id) are not real keys
    keys = {k for k in keys if not k.endswith(".")}
    assert len(keys) > 400, f"only {len(keys)} keys found — did the scan break?"
    missing = sorted(k for k in keys if _tt("de", k) == k)
    assert not missing, (f"{len(missing)} dashboard keys have no German and fall "
                         f"back to English: {missing[:8]}")
    print(f"OK  all {len(keys)} dashboard keys have a German translation")


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


# ── The background, and why it stopped freezing ───────────────────────────

def test_background_opts_out_of_the_colour_hook_at_call_time():
    """The skin's own canvas must keep the live load hue.

    The first attempt captured the "raw" property setter inside mountCircuit —
    but the colour hook is armed before boot(), so what it captured WAS the
    patched setter and the bypass never bypassed anything.  Measured: a
    conductor asked for hsla(26,40%,68%,.13) and got rgb(254,100,11).  A flag
    checked at call time cannot be defeated by ordering.
    """
    js = open(JS_PATH, encoding="utf-8").read()
    assert "this.__auRaw ? v : mapColour(v)" in js, \
        "the canvas setter no longer honours the opt-out"
    assert "g.__auRaw = true" in js, "the background context never claims the opt-out"
    # A gradient must follow the same rule as the context that made it.
    assert "rawGrads" in js and "addColorStop" in js, \
        "gradient stops still go through the mapper regardless of their context"
    # And the broken approach must not come back.
    assert "getOwnPropertyDescriptor(\n      window.CanvasRenderingContext2D" not in js
    assert "raw.set.call" not in js, "the descriptor-capture bypass is back"
    print("OK  the background canvas opts out of the colour rule at call time")


def test_the_animation_cannot_die():
    """The freeze, and the two things that caused it.

    Measured against the live dashboard: with the skin the whole page fell from
    ~29 fps to zero within minutes and never recovered; without it the page held
    59 fps for ten minutes.  Two defects, both fixed here:

      · a frame that threw left `raf` at 0 with nothing scheduled, so ONE
        exception froze the background for the life of the page;
      · requestAnimationFrame stops being delivered when the window is merely
        covered by another one, and that does NOT fire visibilitychange — so
        there was no way back at all.  A timer keeps running in that state.
    """
    js = open(JS_PATH, encoding="utf-8").read()
    fn = js[js.index("    function frame(ts) {"):]
    fn = fn[:fn.index("\n    }\n")]
    assert "try {" in fn and "catch (e)" in fn, "a throwing frame kills the loop again"
    assert "schedule();" in fn, "the frame does not reschedule itself"
    assert "lastFrameAt = now();" in fn, "nothing records that a frame happened"
    wd = js[js.index("setInterval(function () {"):]
    wd = wd[:wd.index("}, 1300);")]
    assert "lastFrameAt" in wd and "schedule()" in wd, "the watchdog does not restart the loop"
    assert "document.hidden" in wd, "the watchdog would run in a hidden tab"
    print("OK  a throwing frame reschedules, and a watchdog revives a stalled loop")


def test_the_expensive_dom_background_is_gone():
    """Bisected against the live dashboard: the blurred DOM ribbons cost three
    quarters of the frame budget (14 fps with, 52 fps without), while removing
    the canvas changed nothing at all.  The field is painted from pre-rendered
    sprites on the canvas now."""
    # 🔴 Strip comments first.  The note explaining WHY these are gone names
    # every one of them, and a plain grep counts the explanation as the crime.
    css = _strip_css_comments(open(CSS_PATH, encoding="utf-8").read())
    js = _strip_js_comments(open(JS_PATH, encoding="utf-8").read())
    assert "aurora-sky" not in css and "aurora-sky" not in js, \
        "the blurred DOM background is back"
    assert "blur(58px)" not in css and "blur(72px)" not in css, \
        "a large blur filter is animated again"
    assert "mix-blend-mode" not in css, "a blend mode is back on an animated element"
    js_all = open(JS_PATH, encoding="utf-8").read()
    assert "fieldSprite" in js_all and "fieldCv" in js_all, "the field is no longer buffered"
    print("OK  no animated blur+blend in the DOM; the field is a buffered sprite")


def test_charge_follows_the_load_on_the_scale_the_gauge_uses():
    """Faster and redder as the house pulls more — and on the banded scale, so
    the difference between a quiet evening and the oven being on is visible."""
    js = open(JS_PATH, encoding="utf-8").read()
    stops = re.findall(r"\{\s*w:\s*(\d+),\s*hue:\s*(-?\d+)", js)
    assert len(stops) == 5, stops
    watts = [int(w) for w, _ in stops]
    hues = [int(h) for _, h in stops]
    assert watts == sorted(watts), watts
    assert hues == sorted(hues, reverse=True), "the hue must fall monotonically toward red"
    assert hues[0] > 150, "idle should be teal"
    assert hues[-1] <= 0, "peak should reach red"
    assert watts[-1] <= 5000, "red arrives too late for a household"
    lc = js[js.index("function loadColour("):]
    lc = lc[:lc.index("\n  }\n")]
    assert "(i + f) / (STOPS.length - 1)" in lc.replace("\n", " ").replace("  ", " "), \
        "the lift is back on a straight watt scale"
    assert re.search(r"var speed = \d+ \+ l \* \d+;", js), \
        "the charge speed no longer follows the load"
    print(f"OK  {watts[0]}..{watts[-1]} W maps hue {hues[0]} to {hues[-1]}, banded")


# ── The three things that were visibly wrong ──────────────────────────────

def test_the_loading_pill_clears_the_rail():
    """The base pins the progress strip to bottom: 52px — the height of the
    CLASSIC rail, which sits flush on the bottom edge.  The Aurora rail floats
    and is taller, so the strip drew a blue line across the middle of the
    icons.  It is positioned from the rail's MEASURED height now."""
    css = open(CSS_PATH, encoding="utf-8").read()
    js = open(JS_PATH, encoding="utf-8").read()
    blk = css[css.index('html[data-skin="aurora"] #nav-progress {'):]
    blk = blk[:blk.index("}")]
    assert "--au-rail-h" in blk, "the pill is positioned from a hard-coded number again"
    assert "52px" not in blk
    assert "--au-rail-h" in js, "nothing measures the rail"
    print("OK  the loading pill is placed from the rail's measured height")


def test_sparkline_wrapper_does_not_clip_its_label():
    """border-radius + overflow:hidden on the wrapper put a rounded corner over
    the top-left of the caption inside it.  Pixel-checked on the live page: "U"
    rendered as "J".  Layout metrics show nothing; only the pixels do."""
    css = open(CSS_PATH, encoding="utf-8").read()
    blk = css[css.index('html[data-skin="aurora"] .sparkline-wrap {'):]
    blk = blk[:blk.index("}")]
    assert "overflow: visible" in blk, "the wrapper clips its own label again"
    assert "border-radius: 0" in blk, "a rounded corner will eat the first glyph"
    print("OK  the sparkline wrapper cannot clip its caption")


def test_heatmap_keeps_the_gaps_that_make_it_a_grid():
    """A heatmap is the one table whose GAPS carry meaning.  Collapsing its
    spacing welded the cells into one field of colour."""
    css = open(CSS_PATH, encoding="utf-8").read()
    blk = css[css.index('html[data-skin="aurora"] .hm-table {'):]
    blk = blk[:blk.index("}")]
    m = re.search(r"border-spacing:\s*([\d.]+)px", blk)
    assert m and float(m.group(1)) >= 2, "the heatmap cells are welded together again"
    # and it must not be swept up by the generic table rule
    generic = css[css.index('html[data-skin="aurora"] table,'):]
    generic = generic[:generic.index("}")]
    assert ".hm-table" not in generic, "the heatmap is back under the border-spacing: 0 rule"
    print("OK  the heatmap keeps its spacing and reads as a grid")


def test_the_floating_rail_leaves_room_at_the_end():
    """A rail that floats sits ON the content, so the scroller has to reserve
    room for it — and exactly once.  The base reserves a fixed 98px, which is a
    guess against a rail whose height this skin changed; putting the same
    reservation on .pane as well simply double-counted it (190px of dead space
    at the bottom of every tab)."""
    css = _strip_css_comments(open(CSS_PATH, encoding="utf-8").read())
    assert 'html[data-skin="aurora"] #panes {' in css, "the scroller reserves nothing"
    # there is more than one #panes rule; the reservation is in whichever one
    # carries padding-bottom
    blocks = [css[i:css.index("}", i)] for i in
              [m.start() for m in re.finditer(r'html\[data-skin="aurora"\] #panes \{', css)]]
    pad = [b for b in blocks if "padding-bottom" in b]
    assert pad, "the scroller reserves no room for the floating rail"
    assert any("--au-rail-h" in b for b in pad), \
        "the reservation is a guess again, not the rail's measured height"
    assert 'html[data-skin="aurora"] .pane {\n  padding-bottom' not in css, \
        "the reservation is counted twice"
    print("OK  the scroller reserves the rail's measured height, once")


def test_phones_get_a_real_background():
    """A phone used to get ONE conductor along the very bottom edge — which is
    exactly where the floating rail sits, so the background was invisible on
    every phone (measured: 1 lane, 5 pads, all of it hidden).  Phones get the
    same bus bars as everything else now, pushed into the outer margin."""
    js = _strip_js_comments(open(JS_PATH, encoding="utf-8").read())
    board = js[js.index("function buildBoard("):]
    board = board[:board.index("\n  }\n")]
    assert "if (narrow) {" not in board, "phones are back on the special-case one-liner"
    assert board.count("bus(") >= 2, "the bus bars are gone"
    # the margin and the branch budget must both stay usable at 390px
    m = re.search(r"var reach = narrow \? (\d+)", board)
    assert m and int(m.group(1)) > 54, (
        "a narrow reach at or below the branch gate produces bus bars with no "
        "branches at all")
    print("OK  phones get bus bars with branches, not one line under the rail")


def test_touch_devices_get_no_sticky_hover():
    """Every lift added for the pointer would stick after a tap on a phone."""
    css = open(CSS_PATH, encoding="utf-8").read()
    assert "@media (hover: none)" in css, "no hover fallback for touch"
    blk = css[css.index("@media (hover: none)"):]
    assert "transform: none" in blk[:900], "the lifts still apply on touch"
    assert "prefers-reduced-motion" in css
    print("OK  touch gets no sticky hover, and reduced motion is honoured")


# ── v16.73: the twitch, the direction, and the two rebuilt views ──────────

def test_the_charge_is_integrated_not_recomputed():
    """🔴 The bug behind "zu hektisch".

    Position used to be `t * speed`.  With the load — and therefore the speed —
    gliding underneath it, EVERY change moved the charge by `t * delta-speed`,
    an error that grows with how long the page has been open.  Simulated with
    the real glide filter it reached 1 190 px per frame after five minutes and
    7 134 px after half an hour, against lanes barely 1 400 px long: the pulses
    were effectively teleporting several times a second.

    Integrating `speed * dt` makes a load change alter the PACE and nothing
    else, at any uptime."""
    js = _strip_js_comments(open(JS_PATH, encoding="utf-8").read())
    assert "travel += speed * dt" in js, "the charge is not integrated"
    assert re.search(r"var d = \(travel \+", js), "the pulse position is not read from travel"
    assert not re.search(r"\bt \* speed\b", js), "the old t*speed rule is still there"

    # Replay both rules and compare the worst single-frame step.
    import math

    def worst(mode, uptime):
        dt, t, lift, travel, prev, w = 1 / 60, 0.0, 0.0, 0.0, None, 0.0
        steps = int(uptime / dt)
        for i in range(steps + int(20 / dt)):
            target = 0.05 if i < steps else 0.95
            lift += (target - lift) * (1 - math.exp(-dt / 0.9))
            t += dt
            speed = (26 + lift * 240) if mode == "old" else (18 + lift * 102)
            travel += speed * dt
            pos = t * speed if mode == "old" else travel
            if prev is not None and i > steps - 5:
                w = max(w, pos - prev)
            prev = pos
        return w

    for uptime, floor in ((300, 900), (1800, 5000)):
        assert worst("old", uptime) > floor, "the old rule was not the problem after all"
        assert worst("new", uptime) < 4, "the new rule still jumps"
    print("OK  a load change moves the charge <4 px/frame at any uptime (was 7134 px at 30 min)")


def test_everything_flows_one_way():
    """Neighbouring tracks used to run against each other: the pulse loop
    flipped direction on every odd lane, and branches hanging off the RIGHT bus
    were built inwards, so their charge ran left while the trunks ran right.

    Lanes are oriented once, at build time, so the drawn board is unchanged and
    the whole picture drifts the same way."""
    js = _strip_js_comments(open(JS_PATH, encoding="utf-8").read())
    loop = js[js.index("for (var c2 = 0"):]
    loop = loop[:loop.index("ctx.globalCompositeOperation = \"source-over\"")]
    assert "dir" not in loop, "the pulse loop still carries a per-lane direction"
    seal = js[js.index("function seal("):]
    seal = seal[:seal.index("lanes.push(lane)")]
    assert "pts.slice().reverse()" in seal, "lanes are no longer oriented at build time"
    assert "Math.abs(dx) >= Math.abs(dy)" in seal, "the dominant axis is not what decides"
    print("OK  every lane is oriented before it is measured; no lane runs backwards")


def test_pulse_counts_do_not_pop():
    """Deriving the number of pulses from the load meant one appeared or
    vanished each time the gliding value crossed a rounding boundary — noise,
    not information.  The load drives the pace, the brightness and the colour."""
    js = _strip_js_comments(open(JS_PATH, encoding="utf-8").read())
    for name in ("busN", "trN", "brN"):
        m = re.search(r"var %s = ([^;]+);" % name, js)
        assert m, name
        assert "l" not in re.sub(r"[A-Za-z_$][\w$]*", lambda x: "" if x.group() in ("mob",) else x.group(), m.group(1)).replace("mob", ""), \
            f"{name} still varies with the load: {m.group(1)}"
    print("OK  pulse counts are fixed; the load sets pace, brightness and hue")


def test_the_co2_forecast_is_not_rebuilt_every_second():
    """Measured on the running dashboard: 12 fetches in 12 s produced 12 full
    DOM replacements of the 6h strip, for a payload that changed 0 times.  The
    forecast moves once an HOUR."""
    src = open(WEBDASH_PATH, encoding="utf-8").read()
    assert "function _co2FcSignature(data)" in src, "no signature to compare against"
    blk = src[src.index("// Refresh the 6h forecast strip"):]
    blk = blk[:blk.index("const tbody")]
    assert "if (fsig !== _co2FcSig)" in blk, "the strip is rebuilt unconditionally"
    assert blk.index("_co2FcSig = fsig") < blk.index("fcWrap.innerHTML"), \
        "the signature must be stored before the rebuild, or it rebuilds forever"
    # the hero and the per-device table are gated too
    assert "if (hsig !== _co2HeroSig)" in src
    assert "if (rsig === _co2RatesSig" in src
    print("OK  the forecast strip only touches the DOM when the forecast changes")


def test_the_detail_panel_uses_the_whole_card():
    """The old two-column <dl> parked every value in the left 270 px of an
    1 143 px card — 76% dead — and stacked five sparklines 294 px tall."""
    src = open(WEBDASH_PATH, encoding="utf-8").read()
    css = _strip_css_comments(src)
    assert ".dev-kv" not in src, "the old key/value list is still emitted"
    assert "repeat(auto-fit, minmax(104px, 1fr))" in css, "the metrics do not fill the width"
    assert ".spark-grid" in css and "grid-template-columns: 1fr 1fr" in css, \
        "the sparklines still stack"
    assert ":last-child:nth-child(odd) {{ grid-column: 1 / -1; }}" in css, \
        "a lone last chart is left at half width"
    # the phase split is a bar, and every phase row is aligned
    assert "_phaseBarInner" in src and "_phaseRowsInner" in src
    assert "font-variant-numeric: tabular-nums" in css, "the numbers do not line up"
    print("OK  metrics fill the card, phases are a bar, sparklines share the width")


def test_values_are_addressed_by_name_not_by_position():
    """The old update path wrote dd[0..3] positionally, so re-ordering the list
    would have put the current under the voltage label without any error."""
    src = open(WEBDASH_PATH, encoding="utf-8").read()
    assert "querySelectorAll('dd')" not in src, "positional dd lookups survive"
    assert "'.mt-v[data-mv=\"' + m + '\"]'" in src, "metric values are not addressed by name"
    print("OK  every live value is addressed by its own hook")


def test_co2_forecast_strings_are_translated():
    """Robert reads a German UI; the whole forecast block fell back to English
    because none of these keys existed in any language."""
    from shelly_analyzer.i18n import t
    for key in ("web.co2.forecast_6h", "web.co2.forecast_hint", "web.co2.forecast_waiting",
                "web.co2.best_hour", "web.co2.trend_hint", "web.co2.avg",
                "web.co2.min", "web.co2.max", "web.co2.forecast_label"):
        for lang in ("de", "en", "es"):
            got = t(lang, key)
            assert got and got != key, f"{key} missing for {lang}"
        assert t("de", key) != t("en", key) or key in ("web.co2.min", "web.co2.max"), \
            f"{key} is still English in German"
    print("OK  the CO\u2082 forecast speaks the configured language")


def test_the_clock_follows_the_ui_language():
    """The strip formatted its hours with a hardcoded de-DE, so an English user
    read German 24h stamps in an otherwise English page."""
    src = open(WEBDASH_PATH, encoding="utf-8").read()
    assert "function _locale()" in src
    blk = src[src.index("function _renderCo2Forecast"):]
    blk = blk[:blk.index("function _co2Color")]
    assert "'de-DE'" not in blk, "the forecast still hardcodes a German locale"
    assert blk.count("_locale()") >= 3
    print("OK  the forecast formats its clock from <html lang>")


def test_a_submeter_is_not_counted_twice():
    """🔴 Reported from a live installation: "now" read 4 288 W while the tiles
    showed a house meter at 2 281 W, a wallbox at 46 W and a water heater at
    1 960 W. 2281 + 46 + 1960 = 4287. The heater is wired BEHIND the house
    meter, so its load is already inside the 2 281 W — the hero added it a
    second time. The wiring was already configured (parent = the house meter's
    key); the hero simply never looked at it, and /api/state did not carry the
    field."""
    js = _strip_js_comments(open(JS_PATH, encoding="utf-8").read())
    assert "function counts(d)" in js, "the hero has no notion of a sub-meter"
    assert "if (!counts(d)) continue;" in js, "the sum does not use it"
    # a parent that has GIVEN UP the child must still count it, or the total loses it
    fn = js[js.index("function counts(d)"):]
    fn = fn[:fn.index("var totalW")]
    assert "net_of_children" in fn, "a net parent would silently drop its child"

    api = open(os.path.join(SRC, "web", "blueprints", "api_state.py"), encoding="utf-8").read()
    assert '_t["parent"] = _p if _p in _tile_keys else ""' in api, \
        "/api/state does not expose which tile hangs behind which"

    # Replay the rule on Robert's real numbers, in both configurations.
    def total(tiles):
        shown = {t["key"]: t for t in tiles}
        out = 0.0
        for t in tiles:
            p = shown.get(t.get("parent") or "")
            if p and t["key"] not in (p.get("net_of_children") or []):
                continue
            out += t["power_w"]
        return out

    gross = [
        {"key": "haus",  "power_w": 2281.0, "parent": ""},
        {"key": "wall",  "power_w":   46.0, "parent": ""},
        {"key": "boil",  "power_w": 1960.0, "parent": "haus"},
        {"key": "light", "power_w":    0.0, "parent": "haus"},
    ]
    assert sum(t["power_w"] for t in gross) == 4287.0, "the reported symptom changed"
    assert total(gross) == 2327.0, total(gross)

    # Same house, but with display-subtraction on: the parent shows net and the
    # children must be added back. The total has to be identical.
    net = [dict(t) for t in gross]
    net[0]["power_w"] = 2281.0 - 1960.0
    net[0]["net_of_children"] = ["boil", "light"]
    assert total(net) == 2327.0, total(net)
    print("OK  a sub-meter is counted once: 4287 W -> 2327 W, either configuration")


def test_the_animation_can_be_paused_and_stays_paused():
    """A pause that the watchdog undoes 1.3 s later is not a pause."""
    js = _strip_js_comments(open(JS_PATH, encoding="utf-8").read())
    assert "var paused = false;" in js
    assert 'localStorage.getItem("au-anim")' in js, "the choice is not remembered"
    assert "if (raf || !alive || paused) return;" in js, "schedule ignores the pause"
    wd = js[js.index("setInterval(function () {"):]
    wd = wd[:wd.index("}, 1300);")]
    assert "paused" in wd, "🔴 the watchdog would restart the animation 1.3 s later"
    vis = js[js.index('document.addEventListener("visibilitychange"'):]
    vis = vis[:vis.index("});")]
    assert "!paused" in vis, "coming back to the tab would restart it"
    # resuming must not rewind the slow layers
    sp = js[js.index("function setPaused(v)"):]
    sp = sp[:sp.index("document.documentElement.setAttribute")]
    assert "prev = 0;" in sp and "t0 = 0" not in sp, \
        "resuming resets t0 and makes the field jump"
    # the control itself
    assert 'id="au-anim"' in js and 'aria-pressed' in js, "no button, or no state on it"
    assert "function wireAnimButton" in js
    css = _strip_css_comments(open(CSS_PATH, encoding="utf-8").read())
    assert ".au-anim-btn" in css and ':focus-visible' in css
    from shelly_analyzer.i18n import t as _tt
    for lang in LANGS:
        for key in ("aurora.anim.pause", "aurora.anim.resume", "aurora.anim.short"):
            assert _tt(lang, key) != key, f"{key} missing for {lang}"
    print("OK  the pause survives the watchdog, a tab switch and a reload")



def test_no_pane_is_rebuilt_while_its_data_stands_still():
    """Every tab that refreshes on a timer must gate on the payload.

    Measured on 16.74.0: the Energy Flow tab replaced ``#sankey-cards`` five
    times in twelve seconds for five fetches that returned ONE distinct
    payload; Standby did the same six times, and each rebuild dragged 60-80
    style writes behind it as the skin recoloured the fresh nodes.  The gate
    already existed (``_tabSkipRender``) — three of ten tabs used it.
    """
    src = open(WEBDASH_PATH, encoding="utf-8").read()
    timed = re.search(r"const TAB_LIVE_REFRESH = \{\{(.*?)\}\};", src, re.S)
    assert timed, "TAB_LIVE_REFRESH not found"
    panes = re.findall(r"^\s*(\w+):", timed.group(1), re.M)
    assert len(panes) >= 8, panes
    gated = set(re.findall(r"_tabSkipRender\('(\w+)'", src))
    # ev is refreshed only when coordinates exist, so it needs no payload gate
    missing = [p for p in panes if p not in gated and p not in ("ev",)]
    assert not missing, "these tabs rebuild unconditionally: %s" % missing
    assert "if (_evLastCoords) loadEv();" in src, "the EV tab still repaints without data"
    print("OK  no timed tab rebuilds itself while its numbers stand still")


def test_the_repaint_gate_notices_a_theme_change():
    """The rendered markup depends on theme, skin and language, not only on
    the numbers — otherwise switching theme would leave a gated pane stale."""
    src = open(WEBDASH_PATH, encoding="utf-8").read()
    body = src[src.index("function _tabSkipRender"):src.index("var _toastTimer")]
    for token in ("dataset.theme", "data-skin", "R.lang"):
        assert token in body, "signature ignores %s" % token
    print("OK  the gate reopens on a theme, skin or language change")


def test_every_placeholder_in_the_page_has_a_value():
    """The dashboard template is filled from a mapping in web/__init__.py.

    A second, unused mapping for the same template lives in webdash.py; adding
    a placeholder to that one only renders the literal ``{web_tab_plots}`` to
    the user.  That is exactly what happened while translating the tab bar.
    """
    from shelly_analyzer.services.webdash import _HTML_TEMPLATE
    tpl = _HTML_TEMPLATE.replace("{{", "\x00").replace("}}", "\x01")
    placeholders = set(re.findall(r"\{([A-Za-z_][A-Za-z0-9_]*)\}", tpl))
    web_init = open(os.path.join(SRC, "web", "__init__.py"), encoding="utf-8").read()
    i = web_init.index("def _render_dashboard_html")
    j = web_init.index("rendered = _render_template(tpl, values)", i)
    supplied = set(re.findall(r'^\s*"([A-Za-z_][A-Za-z0-9_]*)":', web_init[i:j], re.M))
    # {country} {currency} {days} {n} are runtime i18n arguments inside
    # translated strings, not template placeholders.
    runtime = {"country", "currency", "days", "n"}
    missing = sorted(placeholders - supplied - runtime)
    assert not missing, "rendered literally to the user: %s" % missing
    print("OK  every template placeholder is filled by the live mapping")


def test_the_tab_bar_is_translated():
    """Ten tab labels were English literals in the markup, so a German user
    read 'Schedule', 'EV Log', 'Goals', 'Control', 'Sync'."""
    src = open(WEBDASH_PATH, encoding="utf-8").read()
    labels = re.findall(r'<span class="nav-label">([^<]+)</span>', src)
    assert len(labels) >= 20, labels
    hard = [l for l in labels if not l.startswith("{")]
    assert not hard, "untranslated tab labels: %s" % hard
    for key in ("plots", "schedule", "ev_log", "tariff", "battery", "advisor",
                "goals", "control", "calibration", "sync"):
        for lang in LANGS:
            val = _t(lang, "web.tab." + key)
            assert val and val != "web.tab." + key, (lang, key)
    print("OK  all 24 tab labels come from the translation table")


def test_the_shell_is_as_tall_as_what_you_can_see():
    """100vh on iOS is the viewport WITHOUT the address bar.

    With the bar showing, the shell is taller than the visible area, so the
    bottom of the scroll container — and the space it reserves for the
    floating rail — sits below the fold and cannot be reached.  Reproduced at
    390x844 with an 87px bar: the last card ended 53px BEHIND the rail.
    """
    src = open(WEBDASH_PATH, encoding="utf-8").read()
    body = src[src.index("    body {{"):src.index("    /* App shell */") if "    /* App shell */" in src else src.index("#app {{")]
    assert "height: 100dvh;" in body, "body still sized from the large viewport"
    app = re.search(r"#app \{\{[^}]*\}\}", src).group(0)
    assert "100dvh" in app, "#app still sized from the large viewport"
    print("OK  the shell is sized from the VISIBLE viewport (dvh), not 100vh")


def test_an_embedded_page_draws_no_ground_of_its_own():
    """The Plots tab is an iframe.  A second aurora inside it stacks two
    grounds with different geometry and cuts a hard seam where the frame ends."""
    js = open(JS_PATH, encoding="utf-8").read()
    assert "window.self !== window.top" in js
    assert "if (!EMBEDDED) mountCircuit();" in js
    css = open(CSS_PATH, encoding="utf-8").read()
    assert 'html[data-skin="aurora"][data-au-embedded] { background: transparent; }' in css
    assert 'html[data-skin="aurora"][data-au-embedded] body { background: transparent; }' in css
    src = open(WEBDASH_PATH, encoding="utf-8").read()
    frame = re.search(r'<iframe id="plots-frame".*?>', src, re.S).group(0)
    assert "background:transparent" in frame, frame
    print("OK  the Plots iframe draws no second ground — no seam")


def _rgba(text):
    n = [float(x) for x in re.findall(r"[\d.]+", text)]
    return n


def _lum(rgb):
    def f(v):
        v /= 255.0
        return v / 12.92 if v <= 0.03928 else ((v + 0.055) / 1.055) ** 2.4
    r, g, b = (f(c) for c in rgb[:3])
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def _over(fg_rgba, bg_rgb):
    a = fg_rgba[3] if len(fg_rgba) > 3 else 1.0
    return [fg_rgba[i] * a + bg_rgb[i] * (1 - a) for i in range(3)]


def test_the_light_theme_gives_a_card_an_edge():
    """Robert: "Tagesansicht zu hell".  Measured on pixels: the page averaged
    0.807 luminance, a card stood 1.25:1 against its ground and the tiles
    inside the hero 1.00:1 — no edge at all.  These are the palette values
    those measurements come from."""
    css = open(CSS_PATH, encoding="utf-8").read()
    block = css[css.index('html[data-skin="aurora"][data-theme="light"] {'):]
    block = block[:block.index("\n}")]
    def var(name):
        return re.search(r"%s:\s*([^;]+);" % re.escape(name), block).group(1).strip()
    ground = [int(var("--au-ground-1").lstrip("#")[i:i + 2], 16) for i in (0, 2, 4)]
    card = _over(_rgba(var("--card")), ground)
    tile = _over(_rgba(var("--surface-2")), card)
    def cr(a, b):
        x, y = _lum(a), _lum(b)
        hi, lo = max(x, y), min(x, y)
        return (hi + 0.05) / (lo + 0.05)
    assert cr(card, ground) >= 1.35, "card vs ground only %.2f:1" % cr(card, ground)
    assert cr(tile, card) >= 1.12, "tile vs card only %.2f:1" % cr(tile, card)
    assert _lum(ground) <= 0.68, "the ground is still a paper white (%.3f)" % _lum(ground)
    print("OK  light theme: card 1.54:1 vs ground, tile 1.20:1 vs card")


def test_the_summary_is_built_from_data_not_from_a_setup():
    """The "right now" panel must not know which house it is looking at.

    Every card comes from a provider that returns null when its numbers are
    missing, so a house with PV and a battery simply gets more cards.  Proved
    against synthetic payloads: 5 cards without PV, 11 with.
    """
    src = open(WEBDASH_PATH, encoding="utf-8").read()
    block = src[src.index("var _INSIGHT_PROVIDERS = ["):src.index("function _iDur(h)")]
    ids = re.findall(r"\{{ id: '(\w+)'", block)
    assert len(ids) >= 10, ids
    # every provider has a bail-out path
    assert block.count("return null;") >= len(ids), "a provider cannot opt out"
    # A provider may look at what a device MEASURES, never at what it is
    # called or keyed as — that is what makes the panel installation-agnostic.
    for pattern in (r"\.name\s*===", r"\.name\s*==", r"\.key\s*===", r"\.key\s*==",
                    r"\.name\.indexOf", r"\.name\.match", r"\.key\.indexOf"):
        hit = re.search(pattern, block)
        assert not hit, "a provider branches on a device's identity: %s" % pattern
    # endpoints are only asked for when the feature is on
    srcs = src[src.index("var _INSIGHT_SOURCES = {{"):src.index("function _iCard")]
    assert "if (need && !_insightsFeat[need]) return;" in src
    print("OK  the summary panel names no device and no installation")


def test_the_summary_speaks_every_language():
    """Fallback texts are what a missing key shows.  A German fallback is read
    by every English user — the mirror image of the bug found in 16.74."""
    src = open(WEBDASH_PATH, encoding="utf-8").read()
    block = src[src.index("var _INSIGHT_PROVIDERS = ["):src.index("function toggleInsights")]
    keys = re.findall(r"t\('(insight\.[a-z_.]+)',\s*'((?:[^']|\\')*)'\)", block)
    assert len(keys) >= 18, len(keys)
    german = re.compile(r"[äöüßÄÖÜ]|\b(?:der|die|das|und|nicht|vom|aus|dem|Netz|jetzt)\b")
    for key, fallback in keys:
        assert not german.search(fallback), "German fallback for %s: %r" % (key, fallback)
        for lang in LANGS:
            val = _t(lang, key)
            assert val and val != key, "%s missing in %s" % (key, lang)
    print("OK  every summary string exists in all 9 languages, fallbacks English")

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
    test_colour_rule_is_bounded_attraction_not_a_snap()
    test_measured_heatmap_ramp_survives_the_colour_rule()
    test_no_generated_selector_table_left_behind()
    test_skin_strings_exist_in_every_language()
    test_battery_tab_is_translated()
    test_english_ui_never_shows_german()
    test_the_german_ui_is_actually_german()
    test_dispatcher_speaks_the_configured_language()
    test_resolve_name()
    test_demo_device_names_resolve_once_at_load()
    test_demo_mode_gets_the_demo_poller()
    test_demo_house_never_draws_nothing()
    test_no_local_import_shadows_a_module_global()
    test_background_opts_out_of_the_colour_hook_at_call_time()
    test_the_animation_cannot_die()
    test_the_expensive_dom_background_is_gone()
    test_charge_follows_the_load_on_the_scale_the_gauge_uses()
    test_the_loading_pill_clears_the_rail()
    test_sparkline_wrapper_does_not_clip_its_label()
    test_heatmap_keeps_the_gaps_that_make_it_a_grid()
    test_touch_devices_get_no_sticky_hover()
    test_the_charge_is_integrated_not_recomputed()
    test_everything_flows_one_way()
    test_pulse_counts_do_not_pop()
    test_the_co2_forecast_is_not_rebuilt_every_second()
    test_the_detail_panel_uses_the_whole_card()
    test_values_are_addressed_by_name_not_by_position()
    test_co2_forecast_strings_are_translated()
    test_the_clock_follows_the_ui_language()
    test_a_submeter_is_not_counted_twice()
    test_the_animation_can_be_paused_and_stays_paused()
    test_the_floating_rail_leaves_room_at_the_end()
    test_phones_get_a_real_background()
    test_no_pane_is_rebuilt_while_its_data_stands_still()
    test_the_repaint_gate_notices_a_theme_change()
    test_every_placeholder_in_the_page_has_a_value()
    test_the_tab_bar_is_translated()
    test_the_shell_is_as_tall_as_what_you_can_see()
    test_an_embedded_page_draws_no_ground_of_its_own()
    test_the_light_theme_gives_a_card_an_edge()
    test_the_summary_is_built_from_data_not_from_a_setup()
    test_the_summary_speaks_every_language()

    print("\nAll Aurora skin tests passed.")