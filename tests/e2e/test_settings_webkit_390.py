"""Plan O1: the settings page must not scroll sideways at 390 px in WebKit (iPhone) either.

WebKit let the flex column grow to the scrolling pane strip (417 px), Chromium did not.
Skipped without playwright or a WebKit build.
"""
import subprocess
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
pytest.importorskip("playwright.sync_api")
from common import ROOT, Server, make_tmp, rm_tree  # noqa: E402


def test_settings_390_webkit_no_hscroll():
    from playwright.sync_api import sync_playwright
    tmp = make_tmp("sea-o1-")
    try:
        subprocess.run([sys.executable, str(ROOT / "scripts" / "simulate_dataset.py"), str(tmp),
                        "--skin", "aurora", "--lang", "en", "--days", "3"], check=True, stdout=subprocess.DEVNULL)
        with Server(tmp) as s, sync_playwright() as p:
            try:
                b = p.webkit.launch()
            except Exception as e:
                pytest.skip(f"no webkit: {e}")
            pg = b.new_context(viewport={"width": 390, "height": 844}, is_mobile=True, has_touch=True).new_page()
            pg.goto(s.url + "/settings")
            pg.wait_for_timeout(2000)
            over = pg.evaluate("document.documentElement.scrollWidth - document.documentElement.clientWidth")
            b.close()
        assert over <= 1, f"settings scrolls sideways by {over}px"
    finally:
        rm_tree(tmp)
