"""The Aurora background draws about 30 times a second whatever the display runs at.

Issue 19: a 166 Hz screen got 166 draws a second because the cap had been lost.  This
drives the real page in a real browser with a *virtual* display: requestAnimationFrame
is replaced by a queue that the test ticks by hand at 144 / 60 Hz timestamps, so the
count does not depend on how busy the machine is.  A draw is one ``clearRect`` on
``#aurora-circuit``.  Skipped without playwright or a browser (the source-level guard
in test_aurora_skin.py always runs).
"""
import os
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

VIRTUAL_DISPLAY = """
(() => {
  let q = new Map(), id = 0; window.__vts = performance.now(); window.__draws = 0;
  window.requestAnimationFrame = cb => { q.set(++id, cb); return id; };
  window.cancelAnimationFrame = i => { q.delete(i); };
  window.__tick = (n, step) => {
    for (let i = 0; i < n; i++) {
      window.__vts += step; const run = Array.from(q.values()); q = new Map();
      run.forEach(c => { try { c(window.__vts); } catch (e) {} });
    }
  };
  const cl = CanvasRenderingContext2D.prototype.clearRect;
  CanvasRenderingContext2D.prototype.clearRect = function () {
    try { if (this.canvas && this.canvas.id === 'aurora-circuit') window.__draws++; } catch (e) {}
    return cl.apply(this, arguments);
  };
})();
"""


def _draws_per_second(page, hz, seconds=2):
    page.evaluate("window.__tick(30, 1000/60)")        # settle the first frames
    d0 = page.evaluate("window.__draws")
    total, step = int(hz * seconds), 1000.0 / hz
    while total > 0:
        n = min(60, total)
        page.evaluate("([n, s]) => window.__tick(n, s)", [n, step])
        total -= n
    return (page.evaluate("window.__draws") - d0) / seconds


@pytest.mark.skipif(os.environ.get("SEA_E2E") != "1", reason="set SEA_E2E=1 to run the browser checks")
def test_the_canvas_is_capped_on_fast_displays():
    pytest.importorskip("playwright")
    import common
    from installations import materialize
    why = common.browser_available("chromium")
    if why:
        pytest.skip("no usable browser: " + why)
    from playwright.sync_api import sync_playwright

    tmp = common.make_tmp("sea-e2e-framelimit-")
    try:
        materialize("f", tmp)                           # simulated house, aurora skin
        with common.Server(tmp) as srv, sync_playwright() as pw:
            br = common.launch_browser(pw, "chromium")
            try:
                page = br.new_context(viewport={"width": 1200, "height": 800}).new_page()
                page.add_init_script(VIRTUAL_DISPLAY)
                page.goto(srv.url + "/", wait_until="load")
                page.wait_for_selector("#aurora-circuit", state="attached", timeout=20000)

                fast = _draws_per_second(page, 144, 1)
                assert 24 <= fast <= 36, f"144 Hz display drew {fast}/s, want ~30"
                at60 = _draws_per_second(page, 60, 1)
                assert 24 <= at60 <= 36, f"60 Hz display drew {at60}/s, want ~30"
                very_fast = _draws_per_second(page, 240, 1)
                assert very_fast <= 36, f"240 Hz display drew {very_fast}/s"

                # Paused by the user: the loop really stops.
                page.evaluate("window.__auCircuit.setPaused(true)")
                page.evaluate("window.__tick(10, 1000/60)")
                before = page.evaluate("window.__draws")
                page.evaluate("window.__tick(120, 1000/60)")
                assert page.evaluate("window.__draws") == before, "a paused canvas kept drawing"
            finally:
                br.close()
    finally:
        common.rm_tree(tmp)
