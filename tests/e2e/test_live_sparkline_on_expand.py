"""A device's detail graphs are drawn at full width the moment the panel opens.

Issue 19 (follow-up): the detail canvases sit in a ``display: none`` panel.  They were
drawn there at a 200 px fallback and stretched by CSS when the panel opened, so they
looked blurred until the next live update.  Skipped without playwright or a browser (the
source-level guard in test_walkthrough_findings_17_10.py always runs).
"""
import os
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))


@pytest.mark.skipif(os.environ.get("SEA_E2E") != "1", reason="set SEA_E2E=1 to run the browser checks")
def test_detail_graphs_are_sharp_right_after_opening():
    pytest.importorskip("playwright")
    import common
    from installations import materialize
    why = common.browser_available("chromium")
    if why:
        pytest.skip("no usable browser: " + why)
    from playwright.sync_api import sync_playwright

    tmp = common.make_tmp("sea-e2e-sparkexpand-")
    try:
        materialize("f", tmp)
        with common.Server(tmp) as srv, sync_playwright() as pw:
            br = common.launch_browser(pw, "chromium")
            try:
                page = br.new_context(viewport={"width": 1200, "height": 800}).new_page()
                page.goto(srv.url + "/", wait_until="load")
                page.wait_for_selector(".dev-card .dev-expand", state="attached", timeout=30000)
                # Let at least one live update fill the history buffers.
                page.wait_for_timeout(4000)
                res = page.evaluate("""() => {
                  const card = Array.from(document.querySelectorAll('.dev-card'))
                    .find(c => c.querySelector('.dev-expand canvas'));
                  if (!card) return {err: 'no card with detail canvases'};
                  const exp = card.querySelector('.dev-expand');
                  if (exp.classList.contains('open')) return {err: 'already open'};
                  card.querySelector('.dev-header').click();
                  const dpr = window.devicePixelRatio || 1;
                  const out = [];
                  exp.querySelectorAll('canvas').forEach(c => out.push(
                    {id: c.id, shown: c.offsetWidth, drawn: c.width / dpr}));
                  return {canvases: out};
                }""")
                assert "err" not in res, res
                assert res["canvases"], "the card has no detail canvases"
                for c in res["canvases"]:
                    assert c["shown"] > 0, c
                    assert abs(c["drawn"] - c["shown"]) <= 1, f"stretched, not redrawn: {c}"
            finally:
                br.close()
    finally:
        common.rm_tree(tmp)
