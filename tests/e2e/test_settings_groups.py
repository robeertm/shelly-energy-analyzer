"""Issue #25: the settings page shows one group of cards at a time, in a real browser.

/settings opens on the first group (Basic settings), a group link shows only that
group's cards, a card link (#sec-<id>, also from a bookmark or the dashboard)
switches to the card's group, "Show all" brings back every card. At 390 px the
group links wrap into a grid of touch-sized tabs that never scrolls sideways.
Skipped without playwright or a browser build.
"""
import subprocess
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
pytest.importorskip("playwright.sync_api")
from common import ROOT, Server, launch_browser, make_tmp, rm_tree  # noqa: E402

VISIBLE_IDS = """() => [...document.querySelectorAll('#main > section[id^="sec-"]')]
  .filter(e => getComputedStyle(e).display !== 'none').map(e => e.id)"""


@pytest.fixture(scope="module")
def server():
    tmp = make_tmp("sea-grp-")
    try:
        subprocess.run([sys.executable, str(ROOT / "scripts" / "simulate_dataset.py"), str(tmp),
                        "--skin", "aurora", "--lang", "en", "--days", "2"], check=True, stdout=subprocess.DEVNULL)
        with Server(tmp) as s:
            yield s
    finally:
        rm_tree(tmp)


def _open(pg, url):
    pg.goto(url)
    pg.wait_for_selector("#nav a.grp", timeout=60000)
    pg.wait_for_timeout(400)


def test_groups_desktop(server):
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        try:
            b = launch_browser(p, "chromium")
        except Exception as e:
            pytest.skip(f"no chromium: {e}")
        try:
            pg = b.new_context(viewport={"width": 1512, "height": 900}).new_page()
            _open(pg, server.url + "/settings")
            vis = pg.evaluate(VISIBLE_IDS)
            assert "sec-ui" in vis and "sec-devices" in vis
            assert "sec-telegram" not in vis and "sec-updates" not in vis and "sec-battery" not in vis
            assert pg.locator('#nav a.grp[data-grp="basic"]').get_attribute("aria-current") == "page"

            # a card link of another group switches to that group and marks the card
            pg.click('#nav a.sec[data-sec="updates"]')
            pg.wait_for_timeout(400)
            vis = pg.evaluate(VISIBLE_IDS)
            assert "sec-updates" in vis and "sec-ui" not in vis and "sec-battery" not in vis
            assert "active" in pg.locator('#nav a.sec[data-sec="updates"]').get_attribute("class")
            assert pg.locator('#nav a.grp[data-grp="advanced"]').get_attribute("aria-current") == "page"

            # a group link shows exactly that group
            pg.click('#nav a.grp[data-grp="notify"]')
            pg.wait_for_timeout(400)
            vis = pg.evaluate(VISIBLE_IDS)
            assert set(vis) == {"sec-telegram", "sec-email", "sec-webhook", "sec-alerts", "sec-anomaly"}
            assert pg.evaluate("location.hash") == "#grp-notify"

            # "Show all" brings back every card, in menu order (Advanced right after Basic)
            pg.click("#nav a.grp-all")
            pg.wait_for_timeout(400)
            vis = pg.evaluate(VISIBLE_IDS)
            assert len(vis) > 40
            assert vis.index("sec-updates") < vis.index("sec-telegram") < vis.index("sec-battery")

            # back button returns to the previous group
            pg.go_back()
            pg.wait_for_timeout(400)
            assert set(pg.evaluate(VISIBLE_IDS)) == {"sec-telegram", "sec-email", "sec-webhook", "sec-alerts", "sec-anomaly"}

            # a deep link from a bookmark or the dashboard picks the group by itself
            _open(pg, server.url + "/settings#sec-tools_exports")
            vis = pg.evaluate(VISIBLE_IDS)
            assert "sec-tools_exports" in vis and "sec-ui" not in vis
            assert pg.locator("#sec-tools_exports").bounding_box() is not None
            _open(pg, server.url + "/settings#grp-energy")
            vis = pg.evaluate(VISIBLE_IDS)
            assert "sec-battery" in vis and "sec-ui" not in vis and "sec-updates" not in vis
        finally:
            b.close()


def test_groups_phone_webkit(server):
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        try:
            b = launch_browser(p, "webkit")
        except Exception as e:
            pytest.skip(f"no webkit: {e}")
        try:
            pg = b.new_context(viewport={"width": 390, "height": 844}, is_mobile=True, has_touch=True).new_page()
            _open(pg, server.url + "/settings")
            over = pg.evaluate("document.documentElement.scrollWidth - document.documentElement.clientWidth")
            assert over <= 1, f"settings scrolls sideways by {over}px"
            nav = pg.evaluate("""() => { const n = document.getElementById('nav');
              return {sw: n.scrollWidth, cw: n.clientWidth,
                tabs: [...n.querySelectorAll('a')].filter(a => getComputedStyle(a).display !== 'none')
                  .map(a => { const r = a.getBoundingClientRect();
                    return {g: a.dataset.grp || '', h: r.height, w: r.width, l: r.left, r: r.right,
                            fits: a.scrollWidth <= a.clientWidth + 1}; })}; }""")
            assert nav["sw"] <= nav["cw"] + 1, "tab grid scrolls sideways"
            tabs = nav["tabs"]
            assert {t["g"] for t in tabs} == {"all", "basic", "advanced", "notify", "integrations", "energy", "features"}
            for t in tabs:
                assert t["h"] >= 44 - 0.5 and t["w"] >= 44, t
                assert t["fits"], t
            # edges flush: the left and right edges of the grid line up
            lefts = sorted({round(t["l"]) for t in tabs})
            rights = sorted({round(t["r"]) for t in tabs})
            assert len(lefts) <= 2 and len(rights) <= 2, (lefts, rights)
            pg.tap('#nav a.grp[data-grp="energy"]')
            pg.wait_for_timeout(500)
            vis = pg.evaluate(VISIBLE_IDS)
            assert "sec-battery" in vis and "sec-ui" not in vis
        finally:
            b.close()
