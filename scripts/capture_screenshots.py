#!/usr/bin/env python3
"""Capture the classic-skin screenshots of docs/screenshots from a simulated installation.

    pip install -r requirements.txt playwright && playwright install chromium
    python scripts/capture_screenshots.py [--out docs/screenshots] [--only desktop_09_standby ...]

Builds the installation with ``scripts/simulate_dataset.py`` in a temporary directory, serves
it on localhost, takes every picture in English, with no browser or OS frame, and stamps each
file as "simulated" (``scripts/screenshot_marker.py``), which ``tests/test_screenshots_simulated.py``
requires.  Nothing here reads a real installation.  Look at every picture before committing it.
"""
from __future__ import annotations

import argparse
import json
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

PORT = 8799
URL = f"http://localhost:{PORT}/"

D=[('01_live','live'),('02_costs','costs'),('03_heatmap','heatmap'),('04_plots','plots'),('05_weather','weather'),('06_co2','co2'),('07_anomalies','anomalies'),('08_forecast','forecast'),('09_standby','standby'),('10_energy_flow','sankey'),('11_compare','compare'),('12_goals','goals'),('13_schedule','smart_sched'),('14_nilm','nilm'),('16_tariff','tariff'),('17_control','control')]
M=[('01_live','live'),('02_costs','costs'),('03_heatmap','heatmap'),('05_weather','weather'),('06_co2','co2'),('07_anomalies','anomalies'),('08_forecast','forecast'),('09_standby','standby'),('10_energy_flow','sankey'),('12_goals','goals'),('14_nilm','nilm')]
DARK=['01_live','02_costs','03_heatmap','06_co2']
def fix_weather(route):
    r=route.fetch(); d=r.json()
    d['current_stale']=False; d['current_as_of']=None
    route.fulfill(response=r, body=json.dumps(d))
def go(pg,pane):
    pg.route('**/api/weather_correlation*', fix_weather)
    pg.goto(URL); pg.wait_for_timeout(1500)
    pg.evaluate(f"switchPane('{pane}', [...document.querySelectorAll('.nav-btn')].find(b=>(b.getAttribute('onclick')||'').includes(\"'{pane}'\")))")
    pg.wait_for_timeout(8000)
def tall(pg,w):
    h=pg.evaluate("(()=>{const p=document.querySelector('.pane.active');return p?p.scrollHeight:0})()")
    pg.set_viewport_size({'width':w,'height':max(700,min(3200,h+230))}); pg.wait_for_timeout(2500)

def serve(tmp: Path):
    subprocess.run([sys.executable, str(ROOT / "scripts" / "simulate_dataset.py"), str(tmp), "--skin", "classic", "--lang", "en"], check=True)
    env = {**os.environ, "PYTHONPATH": str(ROOT / "src")}
    proc = subprocess.Popen([sys.executable, "-m", "shelly_analyzer", "--config", str(tmp / "config.json"), "--port", str(PORT), "--no-ssl"],
                            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(60):
        try:
            urllib.request.urlopen(URL, timeout=2)
            return proc
        except Exception:
            time.sleep(1)
    proc.terminate()
    raise SystemExit("server did not come up")


def capture(OUT: Path, ONLY: set) -> None:
    from playwright.sync_api import sync_playwright

    def want(n): return not ONLY or n in ONLY
    def tall(pg,w):
        h=pg.evaluate("(()=>{const p=document.querySelector('.pane.active');return p?p.scrollHeight:0})()")
        pg.set_viewport_size({'width':w,'height':max(700,min(3200,h+230))}); pg.wait_for_timeout(2500)
    
    def go(pg,pane):
        pg.route('**/api/weather_correlation*', fix_weather)
        pg.goto(URL); pg.wait_for_timeout(1500)
        pg.evaluate(f"switchPane('{pane}', [...document.querySelectorAll('.nav-btn')].find(b=>(b.getAttribute('onclick')||'').includes(\"'{pane}'\")))")
        pg.wait_for_timeout(8000)
    
    with sync_playwright() as p:
        b=p.chromium.launch()
        ctx=b.new_context(viewport={'width':1510,'height':900},device_scale_factor=2,color_scheme='light',locale='en-US')
        pg=ctx.new_page()
        for n,pane in D:
            if not want('desktop_'+n): continue
            pg.set_viewport_size({'width':1510,'height':900})
            if pane=='plots':
                pg.set_viewport_size({'width':1510,'height':1000})
                pg.goto(URL+'plots?raw=0'); pg.wait_for_timeout(3000)
                pg.select_option('#view','kwh'); pg.select_option('#mode','days'); pg.select_option('#preset','30d'); pg.wait_for_timeout(1500)
                for nm in ('Apartment','Wallbox','Grid','Water heater','Garden lights'):
                    pg.locator(f'text="{nm}"').first.click()
                pg.wait_for_timeout(6000)
                h=pg.evaluate("document.scrollingElement.scrollHeight"); pg.set_viewport_size({'width':1510,'height':min(3200,h)}); pg.wait_for_timeout(2500)
            else:
                go(pg,pane)
                if pane=='compare':
                    pg.select_option('#cmp-db','flat'); pg.click('text=Compare >> nth=0'); pg.wait_for_timeout(6000)
                tall(pg,1510)
            pg.screenshot(path=OUT/f'desktop_{n}.png')
        if want('desktop_01_live_detail'):
            pg.set_viewport_size({'width':1510,'height':1205})
            pg.goto(URL); pg.evaluate("localStorage.setItem('sea_pane','live')"); pg.goto(URL); pg.wait_for_timeout(30000)
            pg.evaluate("document.getElementById('panes').scrollTop=0")
            pg.evaluate("openDetailChart('house','w','Power (W)')"); pg.wait_for_timeout(1500)
            pg.screenshot(path=OUT/'desktop_01_live_detail.png')
        if want('desktop_15_export'):
            pg.set_viewport_size({'width':1510,'height':1200})
            pg.goto(URL+'settings#sec-tools_exports'); pg.wait_for_timeout(5000)
            pg.evaluate("document.querySelector('#sec-tools_exports').scrollIntoView()"); pg.wait_for_timeout(500)
            box=pg.locator('#sec-tools_exports').bounding_box()
            pg.screenshot(path=OUT/'desktop_15_export.png', clip={'x':box['x']-10,'y':box['y']-10,'width':box['width']+20,'height':540})
        ctx.close()
        ctx=b.new_context(viewport={'width':1920,'height':1080},device_scale_factor=2,color_scheme='dark',locale='en-US')
        pg=ctx.new_page()
        for n,pane in D:
            if n in DARK and want('desktop_'+n+'_dark'):
                go(pg,pane); pg.screenshot(path=OUT/f'desktop_{n}_dark.png')
        ctx.close()
        for scheme in ('light','dark'):
            ctx=b.new_context(viewport={'width':430,'height':932},device_scale_factor=3,is_mobile=True,has_touch=True,color_scheme=scheme,locale='en-US')
            pg=ctx.new_page()
            for n,pane in M:
                if scheme=='dark' and n not in DARK: continue
                nm=f'mobile_{n}'+('_dark' if scheme=='dark' else '')
                if want(nm):
                    go(pg,pane); pg.screenshot(path=OUT/(nm+'.png'))
            ctx.close()
        if want('widget_web'):
            ctx = b.new_context(viewport={'width': 430, 'height': 760}, device_scale_factor=3, color_scheme='light', locale='en-US')
            pg = ctx.new_page()
            pg.goto(URL + 'w'); pg.wait_for_timeout(6000)
            h = pg.evaluate("Math.ceil([...document.querySelectorAll('body > *, .card, a')].reduce((m,e)=>Math.max(m,e.getBoundingClientRect().bottom),0))")
            pg.screenshot(path=OUT/'widget_web.png', clip={'x': 0, 'y': 0, 'width': 430, 'height': min(760, h + 14)})
            ctx.close()
        b.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(ROOT / "docs" / "screenshots"))
    ap.add_argument("--only", nargs="*", default=[])
    a = ap.parse_args()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp:
        proc = serve(Path(tmp))
        try:
            capture(out, set(a.only))
        finally:
            proc.terminate()
            proc.wait(timeout=15)
    for p in sorted(out.glob("*.png")):
        screenshot_marker.stamp(p)


if __name__ == "__main__":
    main()
