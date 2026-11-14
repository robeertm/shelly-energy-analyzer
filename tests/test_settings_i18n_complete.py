# -*- coding: utf-8 -*-
"""The settings page shows no English in a non-English UI.

The field catalog in settings.html (sections, groups, fields, hints, list item
titles, list sub-fields, select options) is English fallback text. Every
visible string is looked up through T() under a stable key; a missing key
quietly falls back to the English literal, so a German user reads English.
This test derives every key from the catalog and demands that it exists in
all nine languages and that no non-English language still carries the English
text, except for tokens that are the same word everywhere.

Key scheme (mirrors the helpers in settings.html):
  settings.section.<id>                         section title
  settings.sectionhint.<id>                     section hint
  settings.group.<basic|notify|...>             navigation group
  settings.field.<key>                          field label
  settings.hint.<key>                           field hint
  settings.item.<key>                           list entry title
  settings.opts.<key>.<value>                   select option
  settings.field.<parent>.<sub>                 list sub-field label
  settings.hint.<parent>.<sub>                  list sub-field hint
  settings.opts.<parent>.<sub>.<value>          list sub-field select option
"""
from __future__ import annotations

import json
import os
import re
import sys

import pytest

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(ROOT, "src"))

from shelly_analyzer import i18n  # noqa: E402

TEMPLATE = os.path.join(ROOT, "src", "shelly_analyzer", "web", "templates", "settings.html")
LANGS = ("de", "en", "es", "fr", "pt", "it", "pl", "cs", "ru")
OTHER = [l for l in LANGS if l != "en"]

# Same word in every language (product names, protocol names, units, technical tokens).
SAME_EVERYWHERE = {
    "Port", "URL", "Telegram", "Webhook", "InfluxDB", "Prometheus", "OpenAI", "Anthropic",
    "kWp", "Broker", "Chat ID", "→ Telegram", "→ Webhook", "Ollama URL", "TOU", "Spot",
    "MQTT (Home Assistant)", "Home Assistant (REST)", "Huawei SUN2000 / LUNA2000",
    "SunSpec (SolarEdge, SMA, Fronius Gen24, Kostal, Sungrow, GoodWe, SolaX)",
    "CO₂ / ENTSO-E + Electricity Maps", "CO₂ / ENTSO-E", "REST API v1", "Solar / PV", "Token (v2)",
}
# Cognates: the native word is spelled like the English one in these languages only.
SAME_IN = {
    "Alarm": {"de", "pl", "cs"},
    "Name": {"de"},
    "Design": {"de", "fr", "pt", "it"},
    "Version": {"de", "fr"},
    "Updates": {"de"},
    "Download / CSV": {"de", "it"},
    "Zone": {"de", "fr"},
    "Model": {"pl", "cs"},
    "Mode": {"fr"},
    "Type": {"fr"},
    "Provider": {"fr"},
    "Tariff": {"fr"},
    "Incremental": {"es", "pt"},
    "Optional": {"de"},
    "Bucket/DB": {"de", "it", "cs"},
    "Org (v2)": {"de"},
    "API (EU)": {"de", "cs"},
    "Interval (min)": {"cs"},
    "Modbus: host / IP": {"de", "es", "it", "pl"},
    "Modbus: port": {"de", "pl", "cs"},
    "Fronius Solar API (local)": {"es", "pt"},
    "Ollama (local)": {"es", "fr", "pt"},
    "Ollama (lokal)": {"de"},
    "Notifications": {"fr"},
    "Anomalies": {"fr"},
    "E-mail": {"fr", "pt", "it", "pl", "cs"},
    "Password": {"it"},
    "Latitude": {"fr", "pt"},
    "Longitude": {"fr", "pt"},
    "Persons": {"fr"},
    "Email": {"fr", "pt", "it", "pl", "cs"},
    "E-Mail": {"de", "fr", "pt", "it", "pl", "cs"},
    "→ Email": {"fr", "pt", "it", "pl", "cs"},
    "→ E-mail": {"fr", "pt", "it", "pl", "cs"},
    "HA Auto-Discovery": {"de"},
    "Scenario": {"it"},
}

_STR = r'"((?:[^"\\]|\\.)*)"'


def _un(s: str) -> str:
    return json.loads('"' + s + '"')


def _prop(line: str, name: str):
    m = re.search(r"(?<![\w.])" + name + r":" + _STR, line)
    return _un(m.group(1)) if m else None


def _opts(line: str):
    i = line.find("opts:[")
    if i < 0:
        return []
    return [(a, _un(b)) for a, b in re.findall(r'\["([^"]*)",' + _STR + r"\]", line[i:])]


def catalog() -> list[tuple[str, str]]:
    """(i18n key, English fallback text) for every visible catalog string."""
    src = open(TEMPLATE, encoding="utf-8").read()
    a = src.index("const URDB_FIELDS")
    b = src.index("\n];\n", src.index("const SECTIONS")) + 3
    out: list[tuple[str, str]] = []
    group_keys = {"Basic": "basic", "Notifications": "notify", "Integrations": "integrations",
                  "Energy": "energy", "Features": "features", "Advanced": "advanced"}
    seen_groups = set(re.findall(r'group:"([^"]+)"', src[a:b]))
    for g in sorted(seen_groups):
        out.append(("settings.group." + group_keys[g], g))
    parent = None
    for line in src[a:b].splitlines():
        s = line.strip()
        m = re.match(r'\{id:"([^"]+)"', s)
        if m:
            sid = m.group(1)
            title = _prop(s, "title")
            if title is not None:
                out.append(("settings.section." + sid, title))
            hint = _prop(s, "hint")
            if hint is not None:
                out.append(("settings.sectionhint." + sid, hint))
            parent = None
            continue
        m = re.match(r'\{key:"([^"]+)"', s)
        if not m:
            continue
        key = m.group(1)
        indent = len(line) - len(line.lstrip())
        if indent >= 6 and parent:
            base = parent + "." + key
            if _prop(s, "label") is not None:
                out.append(("settings.field." + base, _prop(s, "label")))
            if _prop(s, "hint") is not None:
                out.append(("settings.hint." + base, _prop(s, "hint")))
            for v, lbl in _opts(s):
                out.append(("settings.opts." + base + "." + v, lbl))
            continue
        parent = key if re.search(r'type:"list"', s) else None
        if _prop(s, "label") is not None:
            out.append(("settings.field." + key, _prop(s, "label")))
        if _prop(s, "hint") is not None:
            out.append(("settings.hint." + key, _prop(s, "hint")))
        if _prop(s, "itemLabel") is not None:
            out.append(("settings.item." + key, _prop(s, "itemLabel")))
        if "opts:Object.entries" not in s:
            for v, lbl in _opts(s):
                out.append(("settings.opts." + key + "." + v, lbl))
    return out


CATALOG = catalog()


def test_catalog_parser_sees_the_catalog():
    keys = {k for k, _ in CATALOG}
    assert len(CATALOG) > 400
    for must in (
        "settings.hint.solar.pv_production_device_key",
        "settings.field.solar.feed_in_schedule",
        "settings.item.pricing.tariff_schedule",
        "settings.field.pricing.tariff_schedule.start_date",
        "settings.opts.tariff_compare.custom_tariffs.tariff_type.fixed",
        "settings.hint.pv_surplus.consumers.priority",
        "settings.sectionhint.co2",
        "settings.group.energy",
    ):
        assert must in keys, must


@pytest.mark.parametrize("lang", LANGS)
def test_every_catalog_key_exists(lang):
    table = i18n._I18N[lang]
    missing = sorted({k for k, _ in CATALOG if k not in table})
    assert not missing, f"{lang}: {len(missing)} settings keys missing, e.g. {missing[:6]}"


@pytest.mark.parametrize("lang", OTHER)
def test_no_english_left_in_other_languages(lang):
    en = i18n._I18N["en"]
    bad = []
    for k, fallback in CATALOG:
        english = en.get(k, fallback)
        val = i18n._I18N[lang].get(k)
        if val is None or val != english:
            continue
        if english in SAME_EVERYWHERE or lang in SAME_IN.get(english, set()):
            continue
        bad.append(f"{k} = {val!r}")
    assert not bad, f"{lang}: {len(bad)} settings strings still English, e.g. {bad[:6]}"


def test_render_paths_go_through_T():
    src = open(TEMPLATE, encoding="utf-8").read()
    for raw in ("esc(sf.label)", "esc(sf.hint)", '"(keine)"', ">No devices<"):
        assert raw not in src, f"untranslated render path: {raw}"
    assert "tSubHint(" in src and "tSubOpt(" in src and "tItem(" in src
