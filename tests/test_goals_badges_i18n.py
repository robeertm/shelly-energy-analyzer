# -*- coding: utf-8 -*-
"""Goal badges and the goals pane speak the app language.

BADGE_DEFS carry English fallback text; the page resolves each badge through
web.goals.badge.<id>.name / .desc (client-side t()). Every language needs both
keys and none may keep the English text (de and fr are checked explicitly).
"""
from __future__ import annotations

import os
import re
import sys

import pytest

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(ROOT, "src"))

from shelly_analyzer import i18n  # noqa: E402
from shelly_analyzer.services.gamification import BADGE_DEFS  # noqa: E402

LANGS = ("de", "en", "es", "fr", "pt", "it", "pl", "cs", "ru")


@pytest.mark.parametrize("lang", LANGS)
def test_every_badge_has_name_and_desc(lang):
    for b in BADGE_DEFS:
        for part in ("name", "desc"):
            key = f"web.goals.badge.{b['id']}.{part}"
            assert i18n._I18N[lang].get(key, "").strip(), f"{lang}: {key} missing"


@pytest.mark.parametrize("lang", [l for l in LANGS if l != "en"])
def test_badges_are_not_english(lang):
    for b in BADGE_DEFS:
        for part in ("name", "desc"):
            key = f"web.goals.badge.{b['id']}.{part}"
            assert i18n._I18N[lang][key] != b[part], f"{lang}: {key} still English"


def test_english_table_matches_badge_defs():
    for b in BADGE_DEFS:
        assert i18n._I18N["en"][f"web.goals.badge.{b['id']}.name"] == b["name"]
        assert i18n._I18N["en"][f"web.goals.badge.{b['id']}.desc"] == b["desc"]


@pytest.mark.parametrize("lang", ["de", "fr"])
def test_resolved_like_the_page_does(lang):
    # What the page shows: t(key, fallback) -> the language table, English only as a fallback.
    for b in BADGE_DEFS:
        shown = i18n.get_lang_map(lang, "web.goals.badge.").get(f"web.goals.badge.{b['id']}.name", b["name"])
        assert shown != b["name"]


def test_goals_pane_resolves_badges_and_levels_through_t():
    src = open(os.path.join(ROOT, "src", "shelly_analyzer", "services", "webdash.py"), encoding="utf-8").read()
    assert "'web.goals.badge.' + b.badge_id + '.name'" in src
    assert "'web.goals.badge.' + b.badge_id + '.desc'" in src
    assert "t('web.goals.rank.gold'" in src and "t('web.goals.level'" in src


@pytest.mark.parametrize("lang", LANGS)
def test_short_history_note_has_n_placeholder(lang):
    assert i18n._I18N[lang]["web.costs.short_history"].count("{n}") == 1
