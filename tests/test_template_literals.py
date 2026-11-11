# -*- coding: utf-8 -*-
"""No visible English (or German) literals in the page templates.

The key-parity test (test_i18n_parity.py) proves every language has every key.
It cannot see text that never went through the table: an ``<option>incremental
</option>``, a ``'Total: ' + ...`` in a script, a ``title="Menu"``.  Those show
up as English in a Russian UI and nothing ever fails.  This test reads the three
page templates in webdash.py and fails on any visible text that is not a
placeholder, a ``t('key', fallback)`` call, a number, a symbol or an allowed
proper name.

If it fails on something legitimate (a product name, a unit, a date mask) add it
to ALLOWED below with a reason; if it is a sentence, give it a key in i18n.py.
"""
from __future__ import annotations

import os
import re
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer import i18n  # noqa: E402
from shelly_analyzer.services import webdash  # noqa: E402

TEMPLATES = ("_HTML_TEMPLATE", "_PLOTS_TEMPLATE", "_CONTROL_TEMPLATE")

# Visible text that is allowed to stay as written.
ALLOWED = {
    # product names and language names in their own language
    "Shelly Energy Analyzer", "⚡ Shelly Analyzer",
    "Deutsch", "English", "Español", "Français", "Italiano", "Polski", "Português", "Čeština", "Русский",
    # symbols and unit labels (units.js localises them where a language writes them differently)
    "kWh", "VAR", "cos φ", "Hz", "EUR",
    # input masks
    "YYYY-MM-DD",
    # plots page: the <option> text is replaced from the table at start-up
    # (setTxt in the plots script); the static word is only the no-JS fallback
    "custom", "days", "hours", "weeks", "months", "phases", "total",
}

# Words that mark a script string as a sentence for a reader.
SENTENCE = re.compile(
    r"\b(?:No|Not|Error|Loading|Total|Click|Select|Save|Cancel|Delete|Failed|Unknown|Never|Today|"
    r"Yesterday|Tomorrow|None|Requests|Received|Sent|Category|Computing|Generating|Status|Last|Period|"
    r"Cheapest|Average|Start|Prices|Updated|Same|Your|Fixed)\b"
)
STRING = re.compile(r"""'((?:[^'\\\n]|\\.)*)'|"((?:[^"\\\n]|\\.)*)\"""")
PLACEHOLDER = re.compile(r"\{[^{}]*\}|@@\w+@@|&\w+;|\$\{[^}]*\}|\\u[0-9a-fA-F]{4}")


def _template(name: str) -> str:
    return getattr(webdash, name)


def _strip_t_calls(js: str) -> str:
    """Drop ``t('key', 'fallback'`` fallbacks, also when they span lines."""
    return re.sub(
        r"""\bt\(\s*(['"])[^'"]+\1\s*,\s*(['"])(?:[^'"\\]|\\.)*\2""", "t(KEY", js, flags=re.S
    )


def _visible_words(text: str) -> bool:
    text = PLACEHOLDER.sub("", text)
    return bool(re.search(r"[A-Za-z]{3,}", text))


@pytest.mark.parametrize("name", TEMPLATES)
def test_html_text_and_attributes_are_placeholders(name):
    src = _template(name)
    for pat in (r"(?s)<script.*?</script>", r"(?s)<style.*?</style>", r"(?s)<!--.*?-->"):
        src = re.sub(pat, "", src)
    found = set()
    for m in re.finditer(r">([^<>]+)<", src):
        s = m.group(1).strip()
        if s and _visible_words(s):
            found.add(s)
    for m in re.finditer(r'(?:title|placeholder|aria-label|alt)="([^"]*)"', src):
        s = m.group(1)
        if _visible_words(s):
            found.add(s)
    bad = sorted(s for s in found if s not in ALLOWED)
    assert not bad, f"{name}: visible text that is not translated: {bad[:12]}"


@pytest.mark.parametrize("name", TEMPLATES)
def test_script_strings_are_not_english_sentences(name):
    src = _template(name)
    scripts = re.findall(r"(?s)<script[^>]*>(.*?)</script>", src)
    bad = []
    for sc in scripts:
        sc = re.sub(r"(?s)/\*.*?\*/", "", sc)
        sc = re.sub(r"(?m)^\s*//.*$", "", sc)
        sc = re.sub(r"(?m)\s//\s.*$", "", sc)
        sc = _strip_t_calls(sc)
        for m in STRING.finditer(sc):
            v = m.group(1) if m.group(1) is not None else m.group(2)
            txt = re.sub(r"<[^>]*>", " ", v)
            txt = PLACEHOLDER.sub("", txt).strip()
            if " " in txt and SENTENCE.search(txt) and txt not in ALLOWED:
                bad.append(v[:90])
    assert not bad, f"{name}: script text that is not translated: {sorted(set(bad))[:10]}"


@pytest.mark.parametrize("name", TEMPLATES)
def test_keys_used_by_templates_exist(name):
    """t('some.key', 'fallback') must name a real key — otherwise every
    language silently shows the fallback."""
    en = i18n._I18N["en"]
    keys = set(re.findall(r"""\bt\(\s*['"]([a-z][a-z0-9_]*(?:\.[a-z0-9_]+)+)['"]""", _template(name)))
    missing = sorted(k for k in keys if k not in en)
    assert not missing, f"{name}: keys without a table entry: {missing[:10]}"


def test_unit_labels_exist_in_every_language():
    tokens = ("kwh", "kw", "mwh", "wh", "kwp", "w", "var", "va", "v", "a", "hz", "kg", "g", "ct", "s",
              "b", "kb", "mb", "gb", "min", "h", "d")
    for lang in i18n.LANGS:
        table = i18n._I18N[lang]
        missing = [t for t in tokens if "web.unit." + t not in table]
        assert not missing, f"{lang}: web.unit.* missing {missing}"


def test_russian_writes_units_the_russian_way():
    ru = i18n._I18N["ru"]
    assert ru["web.unit.kwh"] == "кВт·ч" and ru["web.unit.w"] == "Вт" and ru["web.unit.kg"] == "кг"
