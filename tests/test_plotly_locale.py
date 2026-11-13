# -*- coding: utf-8 -*-
"""The Plots tab's own Plotly locale, in every language, with the real Plotly.

17.8.5 taught Plotly the UI language's month and weekday names. It registered
that locale under the plain language code — and for English that is 'en', the
name of Plotly's BUILT-IN locale. Plotly fills every format key a locale leaves
out (periods, dateTime, decimal, grouping, ...) from exactly that built-in 'en';
with it replaced by a partial copy, the keys stayed undefined and every date
axis died with "t.charAt is not a function". Only the English UI was hit, so
the German/Russian screenshots used to check 17.8.5 looked perfect.

Two checks:

* ``test_locale_format_is_complete`` runs the real dashboard script in node
  and replays Plotly's own format lookup (built-in 'en' as the last resort).
  No browser needed, so it runs in CI.
* ``test_real_plotly_draws_dates`` loads the real plotly.min.js that the app
  serves into Chromium and draws a date axis in all nine languages. Skipped
  when Playwright, Chromium or the plotly package is missing.
"""
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shelly_analyzer.i18n import LANGS as _LANGS  # noqa: E402

LANGS = list(_LANGS)

# The keys Plotly's d3 formatter reads; what plotly.js 4 ships in its 'en'.
REQUIRED = {
    "days": 7, "shortDays": 7, "months": 12, "shortMonths": 12, "periods": 2,
    "dateTime": str, "date": str, "time": str, "decimal": str, "thousands": str,
    "grouping": list, "currency": 2, "year": str, "month": str, "dayMonth": str,
    "dayMonthYear": str,
}
BUILTIN_EN = {
    "days": ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"],
    "shortDays": ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"],
    "months": ["January", "February", "March", "April", "May", "June", "July",
               "August", "September", "October", "November", "December"],
    "shortMonths": ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
    "periods": ["AM", "PM"], "dateTime": "%a %b %e %X %Y", "date": "%d/%m/%Y", "time": "%H:%M:%S",
    "decimal": ".", "thousands": ",", "grouping": [3], "currency": ["$", ""], "year": "%Y",
    "month": "%b %Y", "dayMonth": "%b %-d", "dayMonthYear": "%b %-d, %Y",
}


def _locale_js() -> str:
    """The locale helpers exactly as the Plots page serves them."""
    from shelly_analyzer.services.webdash import _PLOTS_TEMPLATE, _render_template
    names = set(re.findall(r"(?<!\{)\{([a-z0-9_]+)\}(?!\})", _PLOTS_TEMPLATE))
    html = _render_template(_PLOTS_TEMPLATE, {k: "0" for k in names if len(k) > 1})
    src = "\n".join(re.findall(r"<script[^>]*>(.*?)</script>", html, re.S))
    start = src.index("function _plotlyLocaleFormat(") if "function _plotlyLocaleFormat(" in src \
        else src.index("function _registerPlotlyLocale(")
    end = src.index("function refreshDevChipStyles(")
    return src[start:end]


# Plotly's getFormatObj (src/plots/plots.js), reduced to the registry part.
LOOKUP = r"""
const fs = require('fs');
const [codePath, lang, builtinJson] = process.argv.slice(2);
const registry = { 'en': { format: JSON.parse(builtinJson) }, 'en-US': { format: { date: '%m/%d/%Y' } } };
globalThis.window = globalThis;
globalThis.document = { documentElement: { lang } };
globalThis.Plotly = { register(m) { registry[m.name] = { format: m.format, dictionary: m.dictionary }; } };
globalThis.isMobileView = () => true;
eval(fs.readFileSync(codePath, 'utf8'));
const cfg = plotlyConfig();
let locale = cfg.locale || 'en-US';
const keys = KEYS;
const out = {};
let done = false;
const include = (f) => { let fin = true; for (const k of keys) { if (out[k] === undefined) { if (f[k] !== undefined) out[k] = f[k]; else fin = false; } } if (fin) done = true; };
for (let i = 0; i < 2; i++) {
  const f = (registry[locale] || {}).format; if (f) include(f);
  const base = locale.split('-')[0];
  if (done || base === locale) break;
  locale = base;
}
if (!done) include(registry.en.format);
console.log(JSON.stringify({ locale: cfg.locale, registered: Object.keys(registry), fmt: out, own: (registry[cfg.locale] || {}).format || {} }));
"""


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
@pytest.mark.parametrize("lang", LANGS)
def test_locale_format_is_complete(lang):
    code = _locale_js()
    with tempfile.TemporaryDirectory() as td:
        cp = os.path.join(td, "locale.js")
        with open(cp, "w", encoding="utf-8") as fh:
            fh.write(code)
        hp = os.path.join(td, "lookup.js")
        with open(hp, "w", encoding="utf-8") as fh:
            fh.write(LOOKUP.replace("KEYS", json.dumps(list(REQUIRED))))
        res = subprocess.run(["node", hp, cp, lang, json.dumps(BUILTIN_EN)],
                             capture_output=True, text=True, timeout=60)
    assert res.returncode == 0, res.stderr
    got = json.loads(res.stdout)
    fmt = got["fmt"]
    # Our own locale carries every key itself — it never relies on a fallback.
    missing = [k for k in REQUIRED if k not in got["own"]]
    assert not missing, f"{lang}: registered locale lacks {missing}"
    # Plotly's own English must survive: it is the fallback of every locale.
    assert got["locale"] not in ("en", "en-US"), f"{lang}: locale registered over a built-in name"
    for key, want in REQUIRED.items():
        assert key in fmt, f"{lang}: format key {key!r} missing — Plotly would crash"
        val = fmt[key]
        if want is str:
            assert isinstance(val, str) and val, f"{lang}: {key} must be a non-empty string, got {val!r}"
        elif want is list:
            assert isinstance(val, list) and val, f"{lang}: {key} must be a list"
        else:
            assert isinstance(val, list) and len(val) == want and all(isinstance(x, str) for x in val), \
                f"{lang}: {key} must be {want} strings, got {val!r}"
    # The months are really the language's own.
    if lang != "en":
        assert fmt["months"] != BUILTIN_EN["months"], f"{lang}: month names stayed English"


def _plotly_js_path():
    try:
        import plotly  # noqa: F401
    except Exception:
        return None
    p = os.path.join(os.path.dirname(plotly.__file__), "package_data", "plotly.min.js")
    return p if os.path.exists(p) else None


def _chromium():
    exe = os.environ.get("SEA_CHROMIUM") or shutil.which("chromium") or shutil.which("chromium-browser")
    return exe


def test_real_plotly_draws_dates():
    pw = pytest.importorskip("playwright.sync_api")
    pjs = _plotly_js_path()
    if not pjs:
        pytest.skip("plotly package not installed")
    code = _locale_js()
    with open(pjs, encoding="utf-8") as fh:
        plotly_src = fh.read()
    fails = []
    with pw.sync_playwright() as p:
        try:
            exe = _chromium()
            browser = p.chromium.launch(executable_path=exe) if exe else p.chromium.launch()
        except Exception as e:  # no browser on this machine
            pytest.skip(f"no chromium: {e}")
        try:
            for lang in LANGS:
                page = browser.new_page()
                errs = []
                page.on("pageerror", lambda e, errs=errs: errs.append(str(e)))
                page.set_content(f"<html lang='{lang}'><body><div id='g' style='width:700px;height:320px'></div></body></html>")
                page.add_script_tag(content=plotly_src)
                page.add_script_tag(content="window.isMobileView = () => true;\n" + code)
                res = page.evaluate("""async () => {
                  try {
                    const cfg = plotlyConfig();
                    await Plotly.newPlot('g', [
                      {x:['2026-10-25 01:00','2026-11-12 10:00','2026-11-13 10:00'], y:[1.5, 2.25, 1234.5], type:'scatter', name:'W'},
                      {x:['2026-11-01','2026-11-02'], y:[0.2, 0.3], type:'bar', name:'kWh'}
                    ], {xaxis:{type:'date'}, hovermode:'x unified'}, cfg);
                    // hover text goes through the date formatter as well
                    Plotly.Fx.hover('g', [{curveNumber:0, pointNumber:1}]);
                    const ticks = Array.from(document.querySelectorAll('#g .xtick text')).map(t => t.textContent);
                    return {ok:true, ticks};
                  } catch (e) { return {ok:false, err: String(e && e.message || e)}; }
                }""")
                if not res.get("ok"):
                    fails.append(f"{lang}: {res.get('err')}")
                elif not res.get("ticks"):
                    fails.append(f"{lang}: no x-axis ticks drawn")
                elif any(not t or "undefined" in t or "NaN" in t for t in res["ticks"]):
                    fails.append(f"{lang}: broken tick labels {res['ticks']}")
                if errs:
                    fails.append(f"{lang}: page error {errs}")
                page.close()
        finally:
            browser.close()
    assert not fails, "\n".join(fails)
