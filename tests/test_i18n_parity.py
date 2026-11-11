# -*- coding: utf-8 -*-
"""Every language carries exactly the English key set, with the same placeholders.

Why this exists: the extra languages are built as `_mk_lang(overrides)` on top of
a *snapshot* of English taken at import time (`_BASE_EN`). Every English key that
was added to the table afterwards never reached fr/pt/it/pl/cs/ru, and es/de were
filled by hand. Nothing failed — `t()` quietly falls back to English, so the
gap only shows up as a foreign-language sentence in the UI (and, for invoices,
a translation with different placeholders raised a silent fallback).

This test fails the moment a key is added to `en` without all other languages,
a placeholder set differs, or a value is empty.
"""
from __future__ import annotations

import os
import re
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer import i18n  # noqa: E402

PLACEHOLDER = re.compile(r"\{[^{}]*\}|%\([^)]+\)[sdif]|%[sdif]")
OTHER = [l for l in i18n.LANGS if l != "en"]


def _table(lang: str) -> dict:
    return i18n._I18N[lang]


def test_languages_are_the_nine_shipped_ones():
    assert set(i18n.LANGS) == {"de", "en", "es", "fr", "pt", "it", "pl", "cs", "ru"}


@pytest.mark.parametrize("lang", OTHER)
def test_same_keys_as_english(lang):
    en, other = set(_table("en")), set(_table(lang))
    missing, extra = sorted(en - other), sorted(other - en)
    assert not missing, f"{lang}: {len(missing)} keys missing, e.g. {missing[:8]}"
    assert not extra, f"{lang}: {len(extra)} keys not in English, e.g. {extra[:8]}"


@pytest.mark.parametrize("lang", OTHER)
def test_same_placeholders_as_english(lang):
    en, other = _table("en"), _table(lang)
    bad = [
        k for k in en
        if k in other and sorted(PLACEHOLDER.findall(en[k])) != sorted(PLACEHOLDER.findall(other[k]))
    ]
    assert not bad, f"{lang}: placeholder mismatch in {bad[:8]}"


@pytest.mark.parametrize("lang", i18n.LANGS)
def test_no_empty_values(lang):
    empty = [k for k, v in _table(lang).items() if not str(v).strip()]
    assert not empty, f"{lang}: empty values {empty[:8]}"


def test_settings_page_keys_exist():
    """T('key', fallback) in settings.html never falls back for a real key."""
    here = os.path.join(os.path.dirname(__file__), "..", "src", "shelly_analyzer", "web", "templates", "settings.html")
    src = open(here, encoding="utf-8").read()
    used = set(re.findall(r"""\bT\(\s*["']([a-z][a-z0-9_]*(?:\.[a-z0-9_]+)+)["']""", src))
    en = _table("en")
    # dynamic prefixes (`cat_` + id) and `settings.list.item_n` (its fallback is the
    # field-specific item label, which a generic key would replace) are deliberate.
    exempt = {"settings.list.item_n"}
    missing = sorted(k for k in used if k not in en and k not in exempt and not k.endswith("_"))
    assert not missing, f"settings.html asks for keys without a table entry: {missing}"
