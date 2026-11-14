#!/usr/bin/env python3
"""Customer walkthrough: open every page of the dashboard the way a customer would.

    python tests/e2e/walkthrough.py --report out.json [--quick] [--langs de,en]
                                    [--installations a,b] [--browsers chromium,webkit] [--jobs 2]

For every cell of the matrix (installation x language x browser x viewport x theme) the
server of that installation is started once (languages are switched at run time), then
every pane of the main page, every combination the Plots selects offer, the control,
settings and setup pages are visited.  Findings are collected for console errors, page
errors, failed or >= 400 requests, broken text (NaN, undefined, raw i18n keys ...), the
Plots error banner, blank panes and tracebacks in the server log.
"""
from __future__ import annotations

import argparse
import http.client
import json
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, List, Optional

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from common import LANGS, Server, launch_browser, make_tmp, rm_tree, server_tracebacks, free_port  # noqa: E402
from installations import INSTALLATIONS, materialize  # noqa: E402

DESKTOP = {"name": "desktop", "viewport": {"width": 1512, "height": 900}, "mobile": False}
MOBILE = {"name": "mobile", "viewport": {"width": 390, "height": 844}, "mobile": True}

SEVERITY_ORDER = {"high": 3, "medium": 2, "low": 1, "info": 0}

TEXT_PATTERNS = [
    ("nan-in-text", "medium", re.compile(r"\bNaN\b")),
    ("undefined-in-text", "medium", re.compile(r"\bundefined\b")),
    ("null-in-text", "medium", re.compile(r"\bnull\b")),
    ("infinity-in-text", "medium", re.compile(r"-?\bInfinity\b")),
    ("object-in-text", "medium", re.compile(r"\[object [A-Za-z]+\]")),
    ("raw-i18n-key", "medium", re.compile(r"\b(?:web|settings|setup|plots|control)\.[a-z0-9_]+(?:\.[a-z0-9_]+)+\b")),
    ("zero-intensity", "medium", re.compile(r"(?<![\d.,])0(?:[.,]0+)?\s?g(?:\sCO.?)?/kWh")),
    ("error-banner", "high", re.compile(r"Web Dashboard Error")),
]
# A dotted file name or version number is not a key.
KEY_FALSE_POSITIVE = re.compile(r"\.(?:py|js|css|json|html|md|txt|png|csv|pdf)$")


class Collector:
    def __init__(self):
        self.findings: List[dict] = []
        self.external: List[dict] = []
        self._seen = set()
        self._lock = threading.Lock()
        self.visits = 0

    def add(self, ctx: dict, page: str, kind: str, severity: str, detail: str, url: str = "", external: bool = False):
        detail = detail.strip()[:600]
        key = (ctx.get("installation"), ctx.get("lang"), ctx.get("browser"), ctx.get("viewport"), ctx.get("theme"),
               page, kind, detail)
        with self._lock:
            if key in self._seen:
                return
            self._seen.add(key)
            rec = {**ctx, "page": page, "kind": kind, "severity": severity, "detail": detail, "url": url}
            (self.external if external else self.findings).append(rec)


def _is_local(url: str) -> bool:
    return url.startswith(("http://127.0.0.1", "http://localhost", "data:", "blob:", "about:"))


class Visitor:
    """One browser context with all listeners wired to the collector."""

    def __init__(self, browser, base_url: str, ctx: dict, vp: dict, theme: str, col: Collector,
                 settle_ms: int, prefix: str = "", flag_external_as_finding: bool = False):
        o = dict(viewport=vp["viewport"], color_scheme=theme, locale="en-US")
        if vp["mobile"]:
            o.update(device_scale_factor=3, is_mobile=True, has_touch=True)
        self.context = browser.new_context(**o)
        self.page = self.context.new_page()
        self.base = base_url
        self.prefix = prefix
        self.ctx = ctx
        self.col = col
        self.settle = settle_ms
        self.label = "?"
        self.mobile = vp["mobile"]
        self.ingress_mode = flag_external_as_finding
        p = self.page
        # No external network from the browser; anything that tries is recorded separately.
        self.context.route("**/*", self._route)
        p.on("console", self._on_console)
        p.on("pageerror", self._on_pageerror)
        p.on("requestfailed", self._on_requestfailed)
        p.on("response", self._on_response)
        self.inflight = 0
        p.on("request", lambda r: self._bump(1))
        p.on("requestfinished", lambda r: self._bump(-1))
        p.on("requestfailed", lambda r: self._bump(-1))
        p.set_default_timeout(20000)

    def _on_pageerror(self, e):
        text = str(e)
        if "due to access control checks" in text or "Load request cancelled" in text:
            # WebKit reports a fetch that was cancelled by a navigation as an uncaught error.
            self.col.add(self.ctx, self.label, "webkit-cancelled-fetch", "info", text, self.page.url, external=True)
            return
        self.col.add(self.ctx, self.label, "pageerror", "high", text, self.page.url)

    def _route(self, route):
        url = route.request.url
        if _is_local(url):
            route.continue_()
        else:
            self.col.add(self.ctx, self.label, "external-request", "info",
                         f"blocked external request {url[:160]}", url, external=True)
            route.abort()

    def _on_console(self, msg):
        if msg.type != "error":
            return
        text = msg.text
        loc = (msg.location or {}).get("url", "")
        if text.startswith("Failed to load resource"):
            return  # the response / requestfailed handlers record these with the URL
        if loc and not _is_local(loc):
            return
        self.col.add(self.ctx, self.label, "console-error", "high", text, loc or self.page.url)

    def _on_requestfailed(self, req):
        if not _is_local(req.url):
            return
        err = (req.failure or "") if isinstance(req.failure, str) else str(req.failure)
        if "ERR_ABORTED" in err or "cancel" in err.lower():
            return  # navigations and cancelled polls
        self.col.add(self.ctx, self.label, "request-failed", "medium", f"{req.method} {_short(req.url)}: {err}", req.url)

    def _on_response(self, resp):
        url = resp.url
        if not _is_local(url) or resp.status < 400:
            return
        path = url.split("//", 1)[-1].split("/", 1)[-1]
        if path.startswith("favicon.ico"):
            return
        sev = "high" if resp.status >= 500 else "medium"
        kind = "http-5xx" if resp.status >= 500 else "http-4xx"
        if self.ingress_mode and resp.status == 404:
            kind, sev = "ingress-404", "medium"
        self.col.add(self.ctx, self.label, kind, sev, f"{resp.status} {resp.request.method} {_short(url)}", url)

    # --- helpers ---------------------------------------------------------------------
    def goto(self, path: str, label: str):
        self.label = label
        self.col.visits += 1
        self.page.goto(self.base + self.prefix + path, wait_until="load", timeout=60000)

    def _bump(self, d: int):
        self.inflight = max(0, self.inflight + d)

    def settle_now(self, extra_ms: int = 0, fixed: bool = True):
        """Wait until no request has been in flight for 350 ms (cap 4 s), then a fixed delay for lazy charts."""
        quiet_since = None
        t0 = time.time()
        while time.time() - t0 < 1.2:
            if self.inflight == 0:
                quiet_since = quiet_since or time.time()
                if time.time() - quiet_since > 0.35:
                    break
            else:
                quiet_since = None
            self.page.wait_for_timeout(100)
        self.page.wait_for_timeout((self.settle if fixed else 0) + extra_ms)

    def visible_text(self, selector: Optional[str]) -> str:
        js = ("(sel)=>{const e=sel?document.querySelector(sel):document.body;return e?e.innerText:''}")
        try:
            return self.page.evaluate(js, selector)
        except Exception as e:
            return f"<<evaluate failed: {e}>>"

    def check_text(self, text: str, label: str, selector: str = ""):
        for kind, sev, rx in TEXT_PATTERNS:
            for m in rx.finditer(text):
                s = m.group(0)
                if kind == "raw-i18n-key" and KEY_FALSE_POSITIVE.search(s):
                    continue
                a, b = max(0, m.start() - 50), min(len(text), m.end() + 50)
                snippet = text[a:b].replace("\n", " | ")
                self.col.add(self.ctx, label, kind, sev, f"'{s}' in: ...{snippet}...", self.page.url)
                break
        if self.mobile:
            try:
                over = self.page.evaluate("document.documentElement.scrollWidth - window.innerWidth")
                if over > 4:
                    self.col.add(self.ctx, label, "horizontal-overflow", "low", f"page is {over}px wider than the viewport", self.page.url)
            except Exception:
                pass

    def close(self):
        try:
            self.context.close()
        except Exception:
            pass


def _short(u: str) -> str:
    u = u.split("//", 1)[-1]
    u = u.split("/", 1)[-1] if "/" in u else u
    return "/" + u[:200]


# --- English baseline for the "untranslated?" heuristic ---------------------------------
_EN_LINES: Dict[tuple, set] = {}
_EN_LOCK = threading.Lock()


USER_DATA = ("Boiler", "Freezer", "Lamp", "Router", "Coffee machine", "Heater", "Idle plug", "House", "Apartment",
             "Wallbox", "Water heater", "Garden lights", "Grid", "sea-e2e", "Traceback", "File \"", "INFO", "WARNING", "ERROR", "DEBUG",
             "Shelly Energy Analyzer", "Home Assistant")


def _lines(text: str):
    """Candidate phrases: at least three real words, mostly letters, no user data, no log lines."""
    for ln in text.splitlines():
        ln = ln.strip()
        if len(ln) < 18 or any(u in ln for u in USER_DATA):
            continue
        if len(re.findall(r"[A-Za-z]{3,}", ln)) < 3 or re.search(r"\w\.\w+\(|self\.|\w_\w+\(", ln):
            continue
        if sum(c.isdigit() for c in ln) > 0.15 * len(ln):
            continue
        yield ln


def untranslated_check(v: Visitor, inst: str, pane: str, text: str):
    lang = v.ctx["lang"]
    key = (inst, pane)
    if lang == "en":
        with _EN_LOCK:
            _EN_LINES.setdefault(key, set()).update(_lines(text))
        return
    with _EN_LOCK:
        base = set(_EN_LINES.get(key, ()))
    hits = [ln for ln in _lines(text) if ln in base]
    if hits:
        v.col.add(v.ctx, pane, "untranslated?", "low", "; ".join(hits[:4]), v.page.url)


# --- page visits ------------------------------------------------------------------------

def discover_panes(v: Visitor) -> List[str]:
    return v.page.evaluate(
        """()=>{const out=[];for(const b of document.querySelectorAll('.nav-btn')){
        const m=(b.getAttribute('onclick')||'').match(/switchPane\\(\\s*['"]([^'"]+)['"]/);
        if(m&&!out.includes(m[1]))out.push(m[1]);}return out}""")


def visit_panes(v: Visitor, inst: str, only: Optional[List[str]] = None) -> int:
    v.goto("/", "main")
    v.settle_now()
    panes = discover_panes(v)
    n = 0
    if not panes:
        v.col.add(v.ctx, "main", "no-navigation", "high", "no .nav-btn found on the main page", v.page.url)
    for pane in panes:
        if only and pane not in only:
            continue
        label = f"pane:{pane}"
        v.label = label
        try:
            v.page.evaluate(
                "(p)=>{const b=[...document.querySelectorAll('.nav-btn')].find(b=>(b.getAttribute('onclick')||'').includes(\"'\"+p+\"'\"));switchPane(p,b)}",
                pane)
        except Exception as e:
            v.col.add(v.ctx, label, "pageerror", "high", f"switchPane failed: {e}", v.page.url)
            continue
        v.settle_now(extra_ms=500 if pane in ("heatmap", "sankey", "compare") else 0)
        text = v.visible_text(".pane.active")
        if len(text.strip()) < 3:  # one retry: a slow lazy pane is not a blank pane
            v.page.wait_for_timeout(2500)
            text = v.visible_text(".pane.active")
        n += 1
        if len(text.strip()) < 3 and not v.page.evaluate("!!document.querySelector('.pane.active iframe, .pane.active canvas, .pane.active svg')"):
            v.col.add(v.ctx, label, "blank-pane", "high", "visible pane has no text", v.page.url)
        v.check_text(text, label)
        untranslated_check(v, inst, label, text)
    return n


def plot_combos(v: Visitor, level: str):
    """Option combinations the Plots selects offer."""
    opts = v.page.evaluate(
        """()=>{const g=id=>{const e=document.getElementById(id);return e?[...e.options].map(o=>o.value):[]};
        return {view:g('view'),metric:g('metric'),series:g('series'),mode:g('mode'),preset:g('preset')}}""")
    views = opts["view"] or ["timeseries"]
    combos = []
    if level == "minimal":
        if "timeseries" in views:
            combos.append({"view": "timeseries", "metric": "W", "series": "total", "preset": "24h"})
        if "kwh" in views:
            combos.append({"view": "kwh", "mode": "days", "preset": "7d"})
        return combos
    presets_ts = [p for p in opts["preset"] if p != "custom"]
    if level == "reduced":
        presets_ts = [p for p in ("1h", "7d") if p in presets_ts] or presets_ts[:2]
    for view in views:
        if view == "timeseries":
            for metric in opts["metric"] or ["W"]:
                for series in (opts["series"] or ["total"]):
                    if level == "reduced" and (metric not in ("W", "V") or (series != "total" and metric != "W")):
                        continue
                    for preset in (presets_ts if (metric == "W") else presets_ts[:1]):
                        if level == "full" and series == "phases" and metric != "W":
                            continue
                        combos.append({"view": view, "metric": metric, "series": series, "preset": preset})
        else:
            for mode in opts["mode"] or ["days"]:
                for preset in ([p for p in ("24h", "7d", "30d") if p in opts["preset"]] if level == "full" else ["7d"]):
                    if level == "reduced" and mode not in ("hours", "days"):
                        continue
                    combos.append({"view": view, "mode": mode, "preset": preset})
    return combos


def visit_plots(v: Visitor, inst: str, level: str, has_data: bool) -> int:
    if level == "none":
        return 0
    v.goto("/plots?raw=0", "plots")
    v.settle_now(extra_ms=1000)
    n = 0
    for c in plot_combos(v, level):
        label = "plots:" + "/".join(f"{k}={c[k]}" for k in c)
        v.label = label
        cur = "?"
        try:
            for sid in ("view", "metric", "series", "mode", "preset"):
                if sid in c:
                    cur = sid
                    v.page.select_option(f"#{sid}", c[sid], timeout=10000)
                    v.settle_now(fixed=False)  # a customer changes one select at a time
        except Exception as e:
            v.col.add(v.ctx, label, "select-failed", "low", f"#{cur}: " + str(e).splitlines()[0], v.page.url)
            continue
        v.settle_now(extra_ms=300)
        text = v.visible_text(None)
        n += 1
        v.check_text(text, label)
        if has_data and c["view"] == "timeseries":
            drawn = v.page.evaluate("document.querySelectorAll('.js-plotly-plot').length")
            if not drawn and len(text.strip()) < 400:
                v.col.add(v.ctx, label, "plot-missing", "medium", "no chart drawn and no explanation shown", v.page.url)
    # Changing the range again while a chart is still loading (customers do this).
    v.label = "plots:rapid-switch"
    try:
        v.page.select_option("#view", "timeseries", timeout=3000)
        for pr in ("30d", "1h", "7d", "24h"):
            if pr in [o for o in v.page.evaluate("[...document.getElementById('preset').options].map(o=>o.value)")]:
                v.page.select_option("#preset", pr, timeout=3000)
                v.page.wait_for_timeout(60)
        v.settle_now(extra_ms=300)
        v.check_text(v.visible_text(None), "plots:rapid-switch")
    except Exception:
        pass
    if level == "minimal":
        return n
    # series legend: pick every device checkbox once, then the "all/none" case
    v.label = "plots:devices"
    try:
        boxes = v.page.locator("#devices input[type=checkbox], .dev-list input[type=checkbox]")
        for i in range(min(boxes.count(), 10)):
            boxes.nth(i).click(timeout=2000)
            v.page.wait_for_timeout(200)
        v.settle_now(extra_ms=300)
        v.check_text(v.visible_text(None), "plots:devices")
    except Exception:
        pass
    return n


def visit_simple_pages(v: Visitor, inst: str, which=("control", "settings", "setup")) -> None:
    for name in which:
        label = name
        v.goto("/" + name, label)
        v.settle_now(extra_ms=800)
        if name == "settings":
            try:  # expand lazy sections by scrolling the whole page
                h = v.page.evaluate("document.documentElement.scrollHeight")
                for y in range(0, int(h), 700):
                    v.page.evaluate("(y)=>window.scrollTo(0,y)", y)
                    v.page.wait_for_timeout(60)
                v.page.evaluate("window.scrollTo(0,0)")
            except Exception:
                pass
        text = v.visible_text(None)
        if len(text.strip()) < 3:
            v.col.add(v.ctx, label, "blank-pane", "high", "page has no text", v.page.url)
        v.check_text(text, label)
        untranslated_check(v, inst, label, text)


# --- the matrix -------------------------------------------------------------------------

def build_matrix(quick: bool, langs: Optional[List[str]], insts: List[str], browsers: List[str]):
    """Cells grouped by installation: list of dicts."""
    cells = []

    def add(inst, lang, browser, vp, theme, plots, pages=True):
        cells.append({"installation": inst, "lang": lang, "browser": browser, "vp": vp, "theme": theme, "plots": plots,
                      "pages": pages})

    if quick:
        add("a", "en", "chromium", DESKTOP, "dark", "minimal")
        add("a", "en", "chromium", MOBILE, "light", "none")
        add("e", "en", "chromium", DESKTOP, "dark", "minimal")
        add("e", "de", "chromium", DESKTOP, "dark", "none")
        add("f", "en", "chromium", DESKTOP, "dark", "none")
    else:
        for inst in INSTALLATIONS:
            if inst == "a":
                langs_i = LANGS
            elif inst == "b":
                langs_i = ("en", "de", "pl", "ru")
            else:
                langs_i = ("en", "de")
            for lang in langs_i:
                if lang == "en":
                    level = "full" if inst == "a" else "reduced"
                else:
                    level = "reduced" if (lang == "de" and inst in ("a", "b")) else "minimal"
                add(inst, lang, "chromium", DESKTOP, "dark", level, pages=inst in ("a", "b") or lang == "en")
            if inst == "a":
                add(inst, "en", "chromium", MOBILE, "light", "reduced")
                add(inst, "en", "webkit", DESKTOP, "light", "minimal")
            add(inst, "en", "webkit", MOBILE, "dark" if inst == "a" else "light", "minimal", pages=inst in ("a", "b", "e"))
    out = []
    for c in cells:
        if c["installation"] not in insts or c["browser"] not in browsers:
            continue
        if langs and c["lang"] not in langs:
            continue
        out.append(c)
    # English first within an installation, so the untranslated heuristic has its baseline.
    out.sort(key=lambda c: (c["installation"], c["lang"] != "en"))
    return out


def run_installation(inst: str, cells: list, col: Collector, quick: bool, log=print):
    from playwright.sync_api import sync_playwright
    tmp = make_tmp(f"sea-e2e-{inst}-")
    t0 = time.time()
    label, builder, env = INSTALLATIONS[inst]
    try:
        materialize(inst, tmp)
        has_data = inst != "e"
        log(f"[{inst}] built '{label}' in {time.time() - t0:.0f}s, {len(cells)} cells")
        with Server(tmp, env) as srv, sync_playwright() as pw:
            browsers = {}
            try:
                probe = srv.probe_dashboard_language_switch("fr")
                if probe:
                    col.add({"installation": inst, "lang": "fr", "browser": "-", "viewport": "-", "theme": "-"},
                            "language-dropdown", "language-switch-ineffective", "high", probe)
                srv.set_language("en")
                cur_lang = "en"
                for cell in cells:
                    if cell["lang"] != cur_lang:
                        srv.set_language(cell["lang"])
                        cur_lang = cell["lang"]
                    if cell["browser"] not in browsers:
                        try:
                            browsers[cell["browser"]] = launch_browser(pw, cell["browser"])
                        except Exception as e:
                            browsers[cell["browser"]] = None
                            log(f"[{inst}] browser {cell['browser']} unavailable: {str(e).splitlines()[0]}")
                    b = browsers[cell["browser"]]
                    if b is None:
                        continue
                    ctx = {"installation": inst, "lang": cell["lang"], "browser": cell["browser"],
                           "viewport": cell["vp"]["name"], "theme": cell["theme"]}
                    t1 = time.time()
                    v = Visitor(b, srv.url, ctx, cell["vp"], cell["theme"], col, 300 if quick else 350)
                    try:
                        visit_panes(v, inst)
                        visit_plots(v, inst, cell["plots"], has_data)
                        if cell.get("pages", True):
                            visit_simple_pages(v, inst, ("control", "settings") if (quick or cell["lang"] != "en") else ("control", "settings", "setup"))
                    except Exception as e:
                        col.add(ctx, v.label, "walkthrough-crash", "high", f"{type(e).__name__}: {str(e).splitlines()[0]}", "")
                    finally:
                        v.close()
                    log(f"[{inst}] {ctx['lang']}/{ctx['browser']}/{ctx['viewport']}/{ctx['theme']} {time.time() - t1:.0f}s")
            finally:
                for b in browsers.values():
                    if b:
                        try:
                            b.close()
                        except Exception:
                            pass
        ctx = {"installation": inst, "lang": "-", "browser": "-", "viewport": "-", "theme": "-"}
        for exc, frame in server_tracebacks(srv.log_text()):
            offline = re.search(r"Connection refused|ProxyError|Max retries|timed out|Name or service|Network is unreachable", exc)
            col.add(ctx, "server-log", "server-traceback-offline" if offline else "server-traceback",
                    "info" if offline else "high", f"{exc}  @ {frame}", external=bool(offline))
    finally:
        rm_tree(tmp)


# --- ingress pass -----------------------------------------------------------------------
INGRESS_PREFIX = "/api/hassio_ingress/abc123token"


class _IngressProxy:
    """Mimics the Home Assistant ingress path: only <prefix>/... reaches the app, with the header."""

    def __init__(self, target_port: int):
        self.target = target_port
        self.port = free_port()
        self.misses: List[str] = []
        outer = self

        class H(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def _do(self):
                path = self.path
                if not path.startswith(INGRESS_PREFIX + "/") and path != INGRESS_PREFIX:
                    outer.misses.append(path)
                    body = b"not found (outside ingress prefix)"
                    self.send_response(404)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                inner = path[len(INGRESS_PREFIX):] or "/"
                length = int(self.headers.get("Content-Length") or 0)
                data = self.rfile.read(length) if length else None
                hdrs = {k: v for k, v in self.headers.items() if k.lower() not in ("host", "connection", "accept-encoding")}
                hdrs["X-Ingress-Path"] = INGRESS_PREFIX
                c = http.client.HTTPConnection("127.0.0.1", outer.target, timeout=60)
                try:
                    c.request(self.command, inner, body=data, headers=hdrs)
                    r = c.getresponse()
                    body = r.read()
                    self.send_response(r.status)
                    for k, v in r.getheaders():
                        if k.lower() in ("transfer-encoding", "connection", "content-length", "content-encoding"):
                            continue
                        self.send_header(k, v)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                finally:
                    c.close()

            do_GET = do_POST = do_PUT = do_DELETE = _do

        self.httpd = ThreadingHTTPServer(("127.0.0.1", self.port), H)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *a):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=10)


def run_ingress(col: Collector, quick: bool, log=print):
    from playwright.sync_api import sync_playwright
    tmp = make_tmp("sea-e2e-ingress-")
    try:
        materialize("a", tmp)
        with Server(tmp) as srv, _IngressProxy(srv.port) as proxy, sync_playwright() as pw:
            b = launch_browser(pw, "chromium")
            try:
                ctx = {"installation": "a+ingress", "lang": "en", "browser": "chromium", "viewport": "desktop", "theme": "light"}
                v = Visitor(b, f"http://127.0.0.1:{proxy.port}", ctx, DESKTOP, "light", col, 300 if quick else 350,
                            prefix=INGRESS_PREFIX, flag_external_as_finding=True)
                try:
                    visit_panes(v, "a+ingress", only=["live", "costs", "plots", "control"] if quick else None)
                    visit_plots(v, "a+ingress", "minimal" if quick else "reduced", True)
                    visit_simple_pages(v, "a+ingress", ("control", "settings"))
                finally:
                    v.close()
            finally:
                b.close()
            paths = sorted({p.split("?")[0] for p in proxy.misses})
            if paths:
                col.add({"installation": "a+ingress", "lang": "en", "browser": "chromium", "viewport": "desktop",
                         "theme": "light"}, "ingress", "ingress-absolute-path", "medium",
                        f"{len(paths)} paths escape the ingress prefix (absolute URLs in the page code): " + ", ".join(paths[:60]),
                        paths[0])
    finally:
        rm_tree(tmp)


def summarize(findings: List[dict]) -> List[dict]:
    """Group by root symptom: kind + normalised detail (digits and quoted values stripped)."""
    groups: Dict[tuple, dict] = {}
    for f in findings:
        norm = re.sub(r"\d+", "#", f["detail"])[:160]
        norm = re.sub(r"\.\.\..*\.\.\.", "...", norm) if f["kind"].endswith("-in-text") else norm
        page_key = f["page"].split(":")[0] if f["kind"].endswith("-in-text") else f["page"]
        if f["installation"].endswith("+ingress"):
            page_key = "(any)"
        key = (f["kind"], page_key, norm)
        g = groups.setdefault(key, {"kind": f["kind"], "severity": f["severity"], "page": f["page"],
                                     "detail": f["detail"], "url": f["url"], "count": 0,
                                     "installations": set(), "langs": set(), "browsers": set(), "viewports": set()})
        g["count"] += 1
        for k, s in (("installations", "installation"), ("langs", "lang"), ("browsers", "browser"), ("viewports", "viewport")):
            g[k].add(f[s])
    out = []
    for g in groups.values():
        for k in ("installations", "langs", "browsers", "viewports"):
            g[k] = sorted(g[k])
        out.append(g)
    out.sort(key=lambda g: (-SEVERITY_ORDER[g["severity"]], g["kind"], g["page"]))
    return out


def run(quick=False, langs=None, installations=None, browsers=None, jobs=None, ingress=True, log=print) -> dict:
    t0 = time.time()
    jobs = jobs or (2 if quick else 3)
    insts = installations or list(INSTALLATIONS)
    browsers = browsers or ["chromium", "webkit"]
    col = Collector()
    matrix = build_matrix(quick, langs, insts, browsers)
    by_inst: Dict[str, list] = {}
    for c in matrix:
        by_inst.setdefault(c["installation"], []).append(c)
    with ThreadPoolExecutor(max_workers=max(1, jobs)) as ex:
        futs = [ex.submit(run_installation, i, cs, col, quick, log) for i, cs in by_inst.items()]
        if ingress and "a" in insts and (not langs or "en" in langs):
            futs.append(ex.submit(run_ingress, col, quick, log))
        for f in futs:
            try:
                f.result()
            except Exception as e:
                col.add({"installation": "?", "lang": "-", "browser": "-", "viewport": "-", "theme": "-"}, "runner",
                        "runner-crash", "high", f"{type(e).__name__}: {str(e).splitlines()[0] if str(e) else ''}")
    return {"mode": "quick" if quick else "full", "seconds": round(time.time() - t0, 1), "cells": len(matrix),
            "visits": col.visits, "findings": col.findings, "groups": summarize(col.findings),
            "external_requests": summarize(col.external)}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--report", default="walkthrough-report.json")
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--langs", default="")
    ap.add_argument("--installations", default="")
    ap.add_argument("--browsers", default="chromium,webkit")
    ap.add_argument("--jobs", type=int, default=0, help="parallel installations (default 2 quick, 3 full)")
    ap.add_argument("--no-ingress", action="store_true")
    a = ap.parse_args(argv)
    rep = run(a.quick, [x for x in a.langs.split(",") if x] or None, [x for x in a.installations.split(",") if x] or None,
              [x for x in a.browsers.split(",") if x], a.jobs or None, not a.no_ingress, log=lambda s: print(s, flush=True))
    Path(a.report).write_text(json.dumps(rep, indent=1, default=list), encoding="utf-8")
    print(f"{rep['cells']} cells, {rep['visits']} page visits, {len(rep['findings'])} findings in "
          f"{len(rep['groups'])} groups, {rep['seconds']} s -> {a.report}")
    for g in rep["groups"]:
        print(f"  [{g['severity']}] {g['kind']} {g['page']} x{g['count']}: {g['detail'][:140]}")
    return 1 if any(SEVERITY_ORDER[f["severity"]] >= 2 for f in rep["findings"]) else 0


if __name__ == "__main__":
    raise SystemExit(main())
