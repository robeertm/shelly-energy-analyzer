#!/usr/bin/env python3
"""Capture the Aurora-skin screenshots of docs/screenshots from a simulated installation.

    python scripts/capture_screenshots_aurora.py [--out docs/screenshots] [--only aurora_desktop_live ...]

Same rules as ``capture_screenshots.py``: simulated data only (``simulate_dataset.py``), English,
no browser frame, sizes taken from the previous set (desktop 1440x900, phone 390x844 at 2x, EV log
pictures larger), every file stamped as simulated.  Look at every picture before committing it.
Playwright runs in its own interpreter; pass it with ``--python`` if the server needs another one.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import screenshot_marker  # noqa: E402

PORT = 8798
URL = f"http://localhost:{PORT}/"
# name -> (pane, scroll-to-text or None)
DESK = {
    "live": ("live", None), "costs": ("costs", None), "heatmap": ("heatmap", None), "nilm": ("nilm", None),
    "goals": ("goals", None), "standby": ("standby", None), "forecast": ("forecast", None), "co2": ("co2", None),
    "calibration": ("calibration", None), "solar": ("solar", None), "energy_flow": ("sankey", None),
    "plots": ("plots", None), "battery": ("battery", None),
    "co2_origin": ("co2", "origin"), "solar_days": ("solar", "last 30 days"), "forecast_solar": ("solar", "days ahead"),
    "battery_days": ("battery", "day by day"), "battery_cycles": ("battery", "cycles & use"),
}
PHONE = {"live": "live", "costs": "costs", "heatmap": "heatmap", "goals": "goals", "solar": "solar",
         "energy_flow": "sankey", "battery": "battery", "co2_origin": "co2"}
LIGHT = {"live": "live", "costs": "costs", "solar": "solar", "battery": "battery"}


def serve(tmp: Path, py: str):
    subprocess.run([py, str(ROOT / "scripts" / "simulate_dataset.py"), str(tmp), "--skin", "aurora", "--lang", "en"], check=True)
    env = {**os.environ, "PYTHONPATH": str(ROOT / "src")}
    cfg = tmp / "config.json"
    proc = subprocess.Popen([py, "-m", "shelly_analyzer", "--config", str(cfg), "--port", str(PORT), "--no-ssl"],
                            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(90):
        try:
            urllib.request.urlopen(URL, timeout=2)
            return proc
        except Exception:
            time.sleep(1)
    proc.terminate()
    raise SystemExit("server did not come up")


def go(pg, pane, text=None, settle=9000):
    pg.goto(URL)
    pg.wait_for_timeout(2500)
    pg.evaluate(f"switchPane('{pane}', [...document.querySelectorAll('.nav-btn')].find(b=>(b.getAttribute('onclick')||'').includes(\"'{pane}'\")))")
    pg.wait_for_timeout(settle)
    for _ in range(60):  # the "Updating tabs" toast must be gone before the picture
        if not pg.evaluate("document.body.innerText.includes('Updating tabs')"):
            break
        pg.wait_for_timeout(1000)
    if text == "curve":
        pg.wait_for_selector('button[onclick^="evToggleCurve"]', timeout=60000)
        pg.evaluate("""()=>{const bs=[...document.querySelectorAll('button[onclick^="evToggleCurve"]')];
          const mixed=bs.find(b=>/Solar \\d+%[\\s\\S]*Battery \\d+%[\\s\\S]*Grid \\d+%/.test(b.parentElement.innerText));
          (mixed||bs[0]).click();}""")
        pg.wait_for_timeout(2500)
        pg.evaluate("document.querySelector('[id^=evc-]:not([style*=none])').scrollIntoView({block:'center'})")
        pg.wait_for_timeout(1500)
    elif text:
        scroll_to(pg, pane, text)


def scroll_to(pg, pane, text):
    pg.evaluate("""([pane,t])=>{const root=document.querySelector('#pane-'+pane)||document;
      const h=[...root.querySelectorAll('*')].find(e=>e.offsetParent!==null&&e.children.length==0&&e.innerText&&getComputedStyle(e).textTransform=='uppercase'&&e.innerText.toLowerCase().includes(t));
      if(h){const c=h.closest('.card,.panel,section')||h;c.scrollIntoView({block:'start'});}}""", [pane, text])
    pg.wait_for_timeout(2000)


def capture(out: Path, only: set, py_shot=None):
    from playwright.sync_api import sync_playwright
    want = lambda n: not only or n in only
    with sync_playwright() as p:
        b = p.chromium.launch()
        ctx = b.new_context(viewport={'width': 1440, 'height': 900}, device_scale_factor=2, color_scheme='dark', locale='en-US')
        pg = ctx.new_page()
        for n, (pane, text) in DESK.items():
            if want('aurora_desktop_' + n):
                go(pg, pane, text, 150000 if n == 'live' else 9000); pg.screenshot(path=out / f'aurora_desktop_{n}.png')
        if want('aurora_desktop_settings'):
            pg.goto(URL + 'settings'); pg.wait_for_timeout(5000); pg.screenshot(path=out / 'aurora_desktop_settings.png')
        ctx.close()
        ctx = b.new_context(viewport={'width': 1920, 'height': 1400}, device_scale_factor=2, color_scheme='dark', locale='en-US')
        pg = ctx.new_page()
        for n, t in (('ev_log', None), ('ev_curve', 'curve')):
            if want('aurora_desktop_' + n):
                go(pg, 'ev_log', t); pg.screenshot(path=out / f'aurora_desktop_{n}.png')
        ctx.close()
        ctx = b.new_context(viewport={'width': 1440, 'height': 900}, device_scale_factor=2, color_scheme='light', locale='en-US')
        pg = ctx.new_page()
        for n, pane in LIGHT.items():
            if want('aurora_light_' + n):
                go(pg, pane, None, 150000 if n == 'live' else 9000); pg.screenshot(path=out / f'aurora_light_{n}.png')
        ctx.close()
        ctx = b.new_context(viewport={'width': 1920, 'height': 1400}, device_scale_factor=2, color_scheme='light', locale='en-US')
        pg = ctx.new_page()
        if want('aurora_light_ev_log'):
            go(pg, 'ev_log'); pg.screenshot(path=out / 'aurora_light_ev_log.png')
        ctx.close()
        ctx = b.new_context(viewport={'width': 390, 'height': 844}, device_scale_factor=2, is_mobile=True, has_touch=True, color_scheme='dark', locale='en-US')
        pg = ctx.new_page()
        for n, pane in PHONE.items():
            if want('aurora_phone_' + n):
                go(pg, pane, 'origin' if n == 'co2_origin' else None, 150000 if n == 'live' else 9000); pg.screenshot(path=out / f'aurora_phone_{n}.png')
        ctx.close()
        ctx = b.new_context(viewport={'width': 430, 'height': 932}, device_scale_factor=3, is_mobile=True, has_touch=True, color_scheme='dark', locale='en-US')
        pg = ctx.new_page()
        for n, t in (('ev_log', None), ('ev_curve', 'curve')):
            if want('aurora_phone_' + n):
                go(pg, 'ev_log', t); pg.screenshot(path=out / f'aurora_phone_{n}.png')
        ctx.close()
        b.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(ROOT / "docs" / "screenshots"))
    ap.add_argument("--only", nargs="*", default=[])
    ap.add_argument("--python", default=sys.executable, help="interpreter that runs the app")
    a = ap.parse_args()
    out = Path(a.out); out.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp:
        proc = serve(Path(tmp), a.python)
        try:
            capture(out, set(a.only))
        finally:
            proc.terminate(); proc.wait(timeout=15)
    for f in sorted(out.glob("aurora_*.png")):
        screenshot_marker.stamp(f)


if __name__ == "__main__":
    main()
