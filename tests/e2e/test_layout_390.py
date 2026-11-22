"""Phone layout guard (plan H1): at 390 px no pane scrolls sideways, nothing sticks out, every
button/link/select is >= 44 px high and every text input >= 16 px (iOS zooms below that).

Walks all dashboard panes and the settings page of a simulated installation, in English and German
(German is the long-text case).  Skipped without playwright or a browser.
"""
import json
import subprocess
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1] / "scripts"))

pytest.importorskip("playwright.sync_api")
from common import ROOT, Server, make_tmp, rm_tree  # noqa: E402
from layout_audit import audit_pane  # noqa: E402

PANES = ("live costs heatmap nilm goals standby forecast co2 calibration solar sankey plots battery "
         "ev_log advisor anomalies compare control smart_sched sync tariff tenants weather").split()


@pytest.mark.parametrize("lang", ["en", "de"])
def test_phone_layout_390(lang):
    from playwright.sync_api import sync_playwright
    tmp = make_tmp("sea-h1-")
    try:
        subprocess.run([sys.executable, str(ROOT / "scripts" / "simulate_dataset.py"), str(tmp),
                        "--skin", "aurora", "--lang", lang, "--days", "10"], check=True, stdout=subprocess.DEVNULL)
        problems = []
        with Server(tmp) as s, sync_playwright() as p:
            try:
                b = p.chromium.launch()
            except Exception as e:  # no browser installed
                pytest.skip(f"no browser: {e}")
            ctx = b.new_context(viewport={"width": 390, "height": 844}, is_mobile=True, has_touch=True, locale=lang)
            pg = ctx.new_page()
            pg.goto(s.url)
            pg.wait_for_timeout(2500)
            for pane in PANES + ["settings"]:
                if pane == "settings":
                    pg.goto(s.url + "/settings")
                    pg.wait_for_timeout(2500)
                else:
                    pg.evaluate("(p)=>{const b=[...document.querySelectorAll('.nav-btn')].find(b=>(b.getAttribute('onclick')||'').includes(\"'\"+p+\"'\"));switchPane(p,b)}", pane)
                    pg.wait_for_timeout(1500)
                r = audit_pane(pg)
                for key in ("hscroll", "clipped", "small_targets", "small_inputs"):
                    if r[key]:
                        problems.append(f"{pane}/{key}: {r[key] if key == 'hscroll' else r[key][:3]}")
            b.close()
        assert not problems, "\n".join(problems)
    finally:
        rm_tree(tmp)
