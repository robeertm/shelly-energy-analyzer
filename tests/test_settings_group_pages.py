# -*- coding: utf-8 -*-
"""Issue #25: the settings page shows one group of cards at a time.

Before, every card of every group was on one long page and the menu entries
were plain anchors: nothing marked where you were, and the menu order (Basic,
Advanced, ...) differed from the page order (most Advanced cards came last).

Now a group heading in the menu is a link (#grp-<key>) that shows only that
group's cards, #grp-all shows every card, and the old card links (#sec-<id>)
pick their group by themselves. This test checks the template structure, runs
the pure group logic in node and checks the new texts in all nine languages.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys

import pytest

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(ROOT, "src"))

from shelly_analyzer import i18n  # noqa: E402

TEMPLATE = os.path.join(ROOT, "src", "shelly_analyzer", "web", "templates", "settings.html")
LANGS = ("de", "en", "es", "fr", "pt", "it", "pl", "cs", "ru")


def _html() -> str:
    with open(TEMPLATE, encoding="utf-8") as f:
        return f.read()


def _sections(html: str):
    """(id, group) of every SECTIONS entry, in file order."""
    block = html[html.index("const SECTIONS = ["):]
    block = block[: block.index("\n];")]
    return re.findall(r'^\s*\{id:"([^"]+)",\s*title:"[^"]*",\s*group:"([^"]+)"', block, re.M)


def _group_keys_block(html: str) -> str:
    start = html.index("const GROUP_KEYS = {")
    return html[start: html.index("};", start) + 2]


def _logic_block(html: str) -> str:
    return html[html.index("// GROUP-LOGIC-BEGIN"): html.index("// GROUP-LOGIC-END")]


def test_structure_present():
    html = _html()
    for needle in ('href="#grp-all"', 'href="#grp-${key}"', "applyGroup(", "hashchange",
                   'T("settings.nav.show_all"', 'T("settings.nav.groups"', 'id = "grp-filter"'):
        assert needle in html, needle


def test_cards_are_hidden_not_removed():
    """Saving reads the cards from the page: the filter must hide, never drop them."""
    html = _html()
    render_all = html[html.index("function renderAll()"):]
    render_all = render_all[: render_all.index("\n}\n")]
    # every card is still rendered, only reordered by group
    assert "ordered.map(renderSection)" in render_all
    assert "{display:none!important}" in html


def test_every_group_has_a_key_and_a_name():
    html = _html()
    secs = _sections(html)
    assert len(secs) >= 40
    keys_src = _group_keys_block(html)
    for _, grp in secs:
        key = re.search(r'"%s":"([a-z]+)"' % re.escape(grp), keys_src)
        key = key.group(1) if key else grp.lower()
        for lang in LANGS:
            assert i18n._I18N[lang].get("settings.group." + key), (lang, key)


def test_new_texts_in_all_languages():
    for key in ("settings.nav.show_all", "settings.nav.groups"):
        vals = {lang: i18n._I18N[lang].get(key) for lang in LANGS}
        assert all(vals.values()), (key, vals)
        assert len(set(vals.values())) == len(LANGS), (key, vals)


@pytest.mark.skipif(not shutil.which("node"), reason="node not installed")
def test_group_logic_in_node():
    html = _html()
    secs = [{"id": i, "title": i, "group": g} for i, g in _sections(html)]
    script = (
        "const SECTIONS = %s;\n%s\n%s\n" % (json.dumps(secs), _group_keys_block(html), _logic_block(html))
        + """
const out = {
  order: groupOrder(),
  updates: groupFromHash("#sec-updates"),
  tools: groupFromHash("#sec-tools_exports"),
  battery: groupFromHash("#sec-battery"),
  telegram: groupFromHash("#sec-telegram"),
  notify: groupFromHash("#grp-notify"),
  all: groupFromHash("#grp-all"),
  bogus: groupFromHash("#grp-nope"),
  unknownSec: groupFromHash("#sec-nope"),
  empty: groupFromHash(""),
  other: groupFromHash("#foo"),
  hiddenAll: hiddenSectionIds("all").length,
  hiddenNotify: hiddenSectionIds("notify"),
};
console.log(JSON.stringify(out));
"""
    )
    r = subprocess.run(["node", "-e", script], capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)
    assert out["order"] == ["basic", "advanced", "notify", "integrations", "energy", "features"]
    assert out["updates"] == "advanced"
    assert out["tools"] == "advanced"
    assert out["battery"] == "energy"
    assert out["telegram"] == "notify"
    assert out["notify"] == "notify"
    assert out["all"] == "all"
    for k in ("bogus", "unknownSec", "empty", "other"):
        assert out[k] is None, k
    assert out["hiddenAll"] == 0
    notify_ids = {i for i, g in _sections(html) if g == "Notifications"}
    assert notify_ids and not (notify_ids & set(out["hiddenNotify"]))
    assert len(out["hiddenNotify"]) == len(secs) - len(notify_ids)
