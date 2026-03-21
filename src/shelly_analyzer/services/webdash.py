from __future__ import annotations

import json
import html
import inspect
import time
import zipfile
import socket
import math
import pkgutil
import threading
import sys
from collections import deque
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs

from shelly_analyzer.i18n import get_lang_map, normalize_lang, t as _t
from shelly_analyzer.services.appliance_detector import identify_appliance as _identify_appliance

# Best-effort project root derived from this file location (more reliable than CWD on macOS Finder launches).
try:
    _CODE_PROJECT_ROOT = Path(__file__).resolve().parents[3]
except Exception:
    _CODE_PROJECT_ROOT = Path.cwd()


def _load_devices_meta_file(p: Path) -> List[Dict[str, Any]]:
    """Load device metadata from a JSON file.

    Supported formats:
      - {"devices_meta": [...]} or {"devices": [...]}
      - a raw list: [...]
    """
    try:
        raw = p.read_text(encoding="utf-8")
        obj = json.loads(raw) if raw else {}
    except Exception:
        return []
    devs: Any = None
    if isinstance(obj, dict):
        devs = obj.get("devices_meta") or obj.get("devices")
    elif isinstance(obj, list):
        devs = obj
    if not isinstance(devs, list):
        return []
    meta: List[Dict[str, Any]] = []
    for d in devs:
        if not isinstance(d, dict):
            continue
        k = str(d.get("key") or "").strip()
        if not k:
            continue
        n = str(d.get("name") or k).strip() or k
        kind = str(d.get("kind") or "").strip()
        try:
            phases = int(d.get("phases") or 3)
        except Exception:
            phases = 3
        meta.append({"key": k, "name": n, "kind": kind, "phases": phases})
    return meta


class QuietHTTPServer(HTTPServer):
    """HTTP server that suppresses common benign disconnect tracebacks.

    Browsers and mobile devices frequently reset connections (e.g. navigation,
    refresh, captive portals). The default socketserver implementation prints
    noisy tracebacks for these cases.
    """

    def handle_error(self, request, client_address) -> None:  # type: ignore[override]
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionResetError, BrokenPipeError, ConnectionAbortedError)):
            return
        return super().handle_error(request, client_address)


def _local_ip_guess() -> str:
    """Best-effort LAN IP discovery (no external calls)."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Doesn't send packets; used only to pick a route.
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


@dataclass
class LivePoint:
    ts: int
    power_total_w: float
    va: float
    vb: float
    vc: float
    ia: float
    ib: float
    ic: float
    pa: float = 0.0
    pb: float = 0.0
    pc: float = 0.0
    q_total_var: float = 0.0
    qa: float = 0.0
    qb: float = 0.0
    qc: float = 0.0
    cosphi_total: float = 0.0
    pfa: float = 0.0
    pfb: float = 0.0
    pfc: float = 0.0
    kwh_today: float = 0.0
    cost_today: float = 0.0
    freq_hz: float = 50.0
    i_n: float = 0.0


def _safe_f(v: float) -> float:
    """Return v if finite, else 0.0. Fields are already float — no float() cast needed."""
    return v if math.isfinite(v) else 0.0


class LiveStateStore:
    """Thread-safe in-memory store for the web dashboard."""

    def __init__(self, max_points: int = 900) -> None:
        self.max_points = int(max_points)
        self._lock = threading.Lock()
        # deque with maxlen: O(1) append + automatic truncation, no manual slice needed.
        self._by_device: Dict[str, Deque[LivePoint]] = {}

    def set_max_points(self, max_points: int) -> None:
        """Adjust the in-memory retention size.

        This is used when the live window changes (e.g. from the mobile dashboard).
        """
        max_points = int(max(50, max_points))
        with self._lock:
            self.max_points = max_points
            for k, dq in self._by_device.items():
                if dq.maxlen != max_points:
                    self._by_device[k] = deque(dq, maxlen=max_points)

    def update(self, device_key: str, point: LivePoint) -> None:
        with self._lock:
            if device_key not in self._by_device:
                self._by_device[device_key] = deque(maxlen=self.max_points)
            self._by_device[device_key].append(point)

    def snapshot(self) -> Dict[str, List[Dict[str, Any]]]:
        # Hold lock only long enough to copy references — serialize outside.
        with self._lock:
            snap = {k: list(dq) for k, dq in self._by_device.items()}
        out: Dict[str, List[Dict[str, Any]]] = {}
        for k, arr in snap.items():
            out[k] = [
                {
                    "ts": p.ts,
                    # Ensure JSON-safe floats (no NaN/Inf) because browsers reject them.
                    "power_total_w": _safe_f(p.power_total_w),
                    "pa": _safe_f(p.pa),
                    "pb": _safe_f(p.pb),
                    "pc": _safe_f(p.pc),
                    "va": _safe_f(p.va),
                    "vb": _safe_f(p.vb),
                    "vc": _safe_f(p.vc),
                    "ia": _safe_f(p.ia),
                    "ib": _safe_f(p.ib),
                    "ic": _safe_f(p.ic),
                    "q_total_var": _safe_f(p.q_total_var),
                    "qa": _safe_f(p.qa),
                    "qb": _safe_f(p.qb),
                    "qc": _safe_f(p.qc),
                    "cosphi_total": _safe_f(p.cosphi_total),
                    "pfa": _safe_f(p.pfa),
                    "pfb": _safe_f(p.pfb),
                    "pfc": _safe_f(p.pfc),
                    "kwh_today": _safe_f(p.kwh_today),
                    "cost_today": _safe_f(p.cost_today),
                    "freq_hz": _safe_f(p.freq_hz),
                    "i_n": _safe_f(p.i_n),
                }
                for p in arr
            ]
        # Appliance hints for the latest reading per device
        appliances: Dict[str, List[Dict[str, Any]]] = {}
        for k, arr in snap.items():
            if arr:
                try:
                    matches = _identify_appliance(arr[-1].power_total_w)[:3]
                    appliances[k] = [
                        {"icon": sig.icon, "id": sig.id, "conf": conf}
                        for sig, conf in matches
                    ]
                except Exception:
                    appliances[k] = []
        if appliances:
            out["_appliances"] = appliances
        return out


def _render_template(tpl: str, values: Dict[str, str]) -> str:
    """Render our HTML templates safely.

    We intentionally avoid Python's str.format here because the embedded CSS/JS
    contains many braces which are easy to miss/escape and would break startup.

    The templates use simple placeholders like `{refresh_ms}` and keep literal
    braces escaped as `{{` and `}}`. Rendering is:
      1) replace `{key}` placeholders
      2) unescape `{{` -> `{` and `}}` -> `}`
    """
    out = tpl
    for k, v in values.items():
        out = out.replace("{" + k + "}", str(v))
    # turn escaped braces back into literal braces
    out = out.replace("{{", "{").replace("}}", "}")
    return out


def _render_template_tokens(tpl: str, values: Dict[str, str]) -> str:
    """Render templates that must keep literal braces intact.

    We use token placeholders like @@key@@ and do NOT perform any brace
    unescaping. This avoids subtle JS/CSS syntax breakage on pages that
    contain many braces (e.g. Plotly pages).
    """
    out = tpl
    for k, v in values.items():
        out = out.replace("@@" + k + "@@", str(v))
    return out


def _plotly_min_js_bytes() -> bytes:
    """Return plotly.min.js bytes from the python `plotly` package.

    This makes the plots page work offline and without any CDN, firewall rules,
    or extra Chrome/Kaleido installs.
    """
    try:
        data = pkgutil.get_data("plotly", "package_data/plotly.min.js")
        if data:
            try:
                # Some plotly distributions ship a UMD wrapper that assigns the factory
                # to `root.moduleName` instead of `root.Plotly`. Our /plots page expects
                # `window.Plotly`, so we normalize the global name.
                data = data.replace(b"root.moduleName", b"root.Plotly")
            except Exception:
                pass
        return data or b""
    except Exception:
        return b""


_HTML_TEMPLATE = """<!doctype html>
<html lang="{lang}">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover" />
  <title>Shelly Energy Analyzer</title>
  <style>
    /* ── Theme variables ── */
    :root {{
      --bg: #f6f7fb;
      --card: #ffffff;
      --fg: #111827;
      --muted: #4b5563;
      --accent: #2563eb;
      --border: rgba(17,24,39,0.12);
      --chipbg: rgba(17,24,39,0.06);
      --pwr-low: #16a34a;
      --pwr-med: #d97706;
      --pwr-high: #dc2626;
    }}
    :root[data-theme="dark"] {{
      --bg: #0b0f14;
      --card: #121821;
      --fg: #e8eef6;
      --muted: #9fb0c3;
      --accent: #6aa7ff;
      --border: rgba(255,255,255,0.08);
      --chipbg: rgba(255,255,255,0.04);
    }}
    /* ── Reset / base ── */
    *, *::before, *::after {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: -apple-system, system-ui, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      background: var(--bg);
      color: var(--fg);
      overflow: hidden;
      height: 100vh;
    }}
    /* ── App shell ── */
    #app {{ display: flex; flex-direction: column; height: 100vh; }}
    #hdr {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 10px 14px;
      background: var(--card);
      border-bottom: 1px solid var(--border);
      flex-shrink: 0;
    }}
    #panes {{
      flex: 1;
      overflow-y: auto;
      overflow-x: hidden;
      padding: 10px;
      padding-bottom: 80px;
    }}
    /* ── Panes ── */
    .pane {{ display: none; animation: fadeIn 0.2s ease; }}
    .pane.active {{ display: block; }}
    @keyframes fadeIn {{ from {{ opacity: 0; transform: translateY(4px); }} to {{ opacity: 1; transform: translateY(0); }} }}
    /* ── Bottom nav ── */
    #bottom-nav {{
      position: fixed;
      bottom: 0; left: 0; right: 0;
      display: grid;
      grid-template-columns: repeat(6,1fr);
      background: var(--card);
      border-top: 1px solid var(--border);
      padding-bottom: env(safe-area-inset-bottom, 0);
      z-index: 100;
    }}
    .nav-btn {{
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      min-height: 56px;
      background: none;
      border: none;
      color: var(--muted);
      font-size: 10px;
      cursor: pointer;
      padding: 6px 2px;
      gap: 2px;
      transition: color 0.15s;
    }}
    .nav-btn .nav-icon {{ font-size: 20px; line-height: 1; }}
    .nav-btn.active {{ color: var(--accent); }}
    /* ── Icon buttons ── */
    .icon-btn {{
      background: none;
      border: 1px solid var(--border);
      border-radius: 10px;
      color: var(--fg);
      cursor: pointer;
      font-size: 16px;
      padding: 6px 10px;
      min-height: 36px;
    }}
    /* ── Cards ── */
    .card {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 14px;
      margin-bottom: 10px;
    }}
    .card-title {{
      font-size: 13px;
      font-weight: 700;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.5px;
      margin-bottom: 10px;
    }}
    .card-grid {{
      display: grid;
      gap: 10px;
      grid-template-columns: 1fr;
    }}
    @media (min-width: 700px) {{
      .card-grid {{ grid-template-columns: 1fr 1fr; }}
    }}
    /* ── Metric grid ── */
    .metric-grid {{
      display: grid;
      grid-template-columns: repeat(2,1fr);
      gap: 8px;
    }}
    @media (min-width: 600px) {{
      .metric-grid {{ grid-template-columns: repeat(4,1fr); }}
    }}
    .metric-card {{
      background: var(--bg);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 12px;
      text-align: center;
    }}
    .metric-label {{
      font-size: 11px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: .5px;
      margin-bottom: 4px;
    }}
    .metric-value {{
      font-size: 22px;
      font-weight: 700;
      color: var(--accent);
    }}
    .metric-sub {{ font-size: 12px; color: var(--muted); margin-top: 2px; }}
    /* ── Power colour ── */
    .pwr-low {{ color: var(--pwr-low); }}
    .pwr-med {{ color: var(--pwr-med); }}
    .pwr-high {{ color: var(--pwr-high); }}
    /* ── Controls row ── */
    .controls-row {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      align-items: center;
      margin-bottom: 10px;
    }}
    select, input[type=date], input[type=text] {{
      background: var(--card);
      color: var(--fg);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 8px 10px;
      font-size: 13px;
      min-height: 44px;
      font-family: inherit;
    }}
    .btn {{
      background: var(--chipbg);
      color: var(--fg);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 8px 14px;
      min-height: 44px;
      font-size: 13px;
      cursor: pointer;
      font-family: inherit;
    }}
    .btn-outline {{ border: 1px solid var(--border); background: var(--card); color: var(--fg); }}
    .btn-accent {{ background: var(--accent); color: #fff; border: none; }}
    .btn-sm {{ min-height: 36px; padding: 6px 10px; font-size: 12px; }}
    /* ── Device live cards ── */
    .dev-card {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 14px;
      margin-bottom: 10px;
      cursor: pointer;
    }}
    .dev-header {{ display: flex; justify-content: space-between; align-items: flex-start; gap: 8px; }}
    .dev-name {{ font-size: 14px; font-weight: 700; flex: 1; min-width: 0; }}
    .dev-power {{ font-size: 26px; font-weight: 700; }}
    .dev-meta {{ font-size: 12px; color: var(--muted); margin-top: 4px; display: flex; gap: 12px; flex-wrap: wrap; }}
    .dev-expand {{ display: none; margin-top: 12px; border-top: 1px solid var(--border); padding-top: 10px; }}
    .dev-expand.open {{ display: block; }}
    .dev-kv {{ display: grid; grid-template-columns: auto 1fr; gap: 4px 12px; font-size: 12px; }}
    .dev-kv dt {{ color: var(--muted); }}
    .dev-kv dd {{ margin: 0; font-weight: 600; }}
    /* NILM chips */
    .appl-list {{ display: flex; flex-wrap: wrap; gap: 5px; margin-top: 8px; }}
    .appl-chip {{
      font-size: 11px;
      background: rgba(106,167,255,0.10);
      border: 1px solid rgba(106,167,255,0.20);
      border-radius: 10px;
      padding: 3px 9px;
    }}
    /* Sparkline canvas */
    .sparkline-wrap {{ margin-top: 10px; }}
    canvas.sparkline {{
      width: 100%;
      height: 56px;
      display: block;
      border-radius: 8px;
      background: var(--chipbg);
    }}
    /* ── Heatmap ── */
    .hm-calendar {{ overflow-x: auto; padding-bottom: 4px; }}
    .hm-grid {{ display: flex; gap: 2px; }}
    .hm-week {{ display: flex; flex-direction: column; gap: 2px; }}
    .hm-day {{
      width: 12px;
      height: 12px;
      border-radius: 2px;
      background: var(--chipbg);
      position: relative;
    }}
    .hm-month-labels {{ display: flex; gap: 2px; font-size: 9px; color: var(--muted); margin-bottom: 3px; }}
    .hm-month-labels span {{ overflow: hidden; }}
    /* Hourly heatmap table */
    .hm-table-wrap {{ overflow-x: auto; }}
    .hm-table {{ border-collapse: collapse; font-size: 9px; }}
    .hm-cell {{
      width: 26px;
      height: 26px;
      text-align: center;
      vertical-align: middle;
      font-size: 9px;
      border: none;
    }}
    .hm-head {{ font-size: 9px; color: var(--muted); text-align: center; padding: 0 2px; }}
    /* Tooltip */
    #hm-tooltip {{
      position: fixed;
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 6px 10px;
      font-size: 12px;
      pointer-events: none;
      display: none;
      z-index: 200;
      white-space: nowrap;
    }}
    /* ── Anomaly events ── */
    .event-card {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 12px 14px;
      margin-bottom: 8px;
      display: flex;
      gap: 12px;
      align-items: flex-start;
    }}
    .event-dot {{
      width: 10px;
      height: 10px;
      border-radius: 50%;
      background: var(--accent);
      margin-top: 4px;
      flex-shrink: 0;
    }}
    .event-body {{ flex: 1; min-width: 0; }}
    .event-type {{ font-size: 13px; font-weight: 700; }}
    .event-meta {{ font-size: 11px; color: var(--muted); margin-top: 2px; }}
    /* ── Status badge ── */
    .badge {{
      display: inline-block;
      font-size: 11px;
      font-weight: 700;
      padding: 3px 8px;
      border-radius: 999px;
      text-transform: uppercase;
      letter-spacing: .5px;
    }}
    .badge-green {{ background: rgba(22,163,74,0.15); color: #16a34a; }}
    .badge-red {{ background: rgba(220,38,38,0.12); color: #dc2626; }}
    .badge-yellow {{ background: rgba(217,119,6,0.12); color: #d97706; }}
    /* ── Chart canvas ── */
    canvas.bar-chart {{
      width: 100%;
      height: 220px;
      display: block;
      border-radius: 10px;
    }}
    /* ── Loading / error ── */
    .loading-msg {{ color: var(--muted); font-size: 13px; padding: 20px 0; text-align: center; }}
    .error-msg {{ color: var(--pwr-high); font-size: 13px; padding: 20px 0; text-align: center; }}
    .info-msg {{ color: var(--muted); font-size: 13px; padding: 20px 0; text-align: center; }}
    /* ── Compare delta ── */
    .delta-grid {{ display: grid; grid-template-columns: repeat(2,1fr); gap: 8px; margin-bottom: 10px; }}
    @media (min-width: 500px) {{ .delta-grid {{ grid-template-columns: repeat(4,1fr); }} }}
    /* ── Summary row ── */
    .summary-row {{ display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 10px; }}
    .summary-chip {{
      background: var(--chipbg);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 6px 12px;
      font-size: 12px;
    }}
    .summary-chip b {{ color: var(--accent); }}
  </style>
</head>
<body>
<script>
(function(){{
  let th = localStorage.getItem('sea_theme');
  if(!th) th = window.matchMedia('(prefers-color-scheme:dark)').matches ? 'dark' : 'light';
  document.documentElement.dataset.theme = th;
}})();
</script>
<div id="app">
  <header id="hdr">
    <span id="hdr-title" style="font-weight:700;font-size:15px">⚡ Shelly Analyzer</span>
    <div id="hdr-actions" style="display:flex;gap:8px;align-items:center">
      <span id="live-stamp" style="font-size:11px;color:var(--muted)"></span>
      <button id="btn-freeze" class="icon-btn" title="Freeze/Resume">▶</button>
      <button id="btn-theme" class="icon-btn" title="Toggle theme">☀</button>
    </div>
  </header>

  <div id="panes">
    <!-- Live -->
    <div id="pane-live" class="pane active">
      <div id="live-grid" class="card-grid"></div>
    </div>

    <!-- Costs -->
    <div id="pane-costs" class="pane">
      <div id="costs-content"><p class="loading-msg">Loading…</p></div>
    </div>

    <!-- Heatmap -->
    <div id="pane-heatmap" class="pane">
      <div class="controls-row">
        <select id="hm-device"></select>
        <select id="hm-unit">
          <option value="kWh">kWh</option>
          <option value="eur">€</option>
        </select>
        <select id="hm-year"></select>
        <button class="btn btn-outline" onclick="loadHeatmap()">↻</button>
      </div>
      <div id="hm-calendar-wrap"></div>
      <div id="hm-hourly-wrap" style="margin-top:14px"></div>
    </div>

    <!-- Solar -->
    <div id="pane-solar" class="pane">
      <div class="controls-row" id="solar-periods"></div>
      <div id="solar-content"><p class="loading-msg">Loading…</p></div>
    </div>

    <!-- Compare -->
    <div id="pane-compare" class="pane">
      <div id="cmp-controls" style="margin-bottom:10px"></div>
      <div id="cmp-quick" style="margin-bottom:10px"></div>
      <div id="cmp-result"></div>
    </div>

    <!-- Anomalies -->
    <div id="pane-anomalies" class="pane">
      <div id="anom-content"><p class="loading-msg">Loading…</p></div>
    </div>
  </div>

  <nav id="bottom-nav">
    <button class="nav-btn active" onclick="switchPane('live',this)">
      <span class="nav-icon">📡</span>
      <span class="nav-label">Live</span>
    </button>
    <button class="nav-btn" onclick="switchPane('costs',this)">
      <span class="nav-icon">💰</span>
      <span class="nav-label">Kosten</span>
    </button>
    <button class="nav-btn" onclick="switchPane('heatmap',this)">
      <span class="nav-icon">🔥</span>
      <span class="nav-label">Heatmap</span>
    </button>
    <button class="nav-btn" onclick="switchPane('solar',this)">
      <span class="nav-icon">☀️</span>
      <span class="nav-label">Solar</span>
    </button>
    <button class="nav-btn" onclick="switchPane('compare',this)">
      <span class="nav-icon">🔀</span>
      <span class="nav-label">Vergleich</span>
    </button>
    <button class="nav-btn" onclick="switchPane('anomalies',this)">
      <span class="nav-icon">🔍</span>
      <span class="nav-label">Anomalien</span>
    </button>
  </nav>
</div>

<div id="hm-tooltip"></div>

<script>
/* ── Injected constants ── */
const REFRESH_MS = {refresh_ms};
const WINDOW_MIN = {window_min};
const WINDOW_OPTIONS = {window_options_json};
const DEVICES = {devices_json};
const I18N = {i18n_json};
function t(k, fb) {{ return (I18N && I18N[k]) ? I18N[k] : (fb || k); }}

/* ── State ── */
let frozen = false;
let liveTimer = null;
let currentPane = 'live';
let sparkData = {{}};   // key -> [{{"ts":..,"w":..}}]
let cmpChart = null;

/* ── Theme ── */
document.getElementById('btn-theme').addEventListener('click', function() {{
  const root = document.documentElement;
  const next = root.dataset.theme === 'dark' ? 'light' : 'dark';
  root.dataset.theme = next;
  localStorage.setItem('sea_theme', next);
  this.textContent = next === 'dark' ? '☀' : '🌙';
}});
(function() {{
  const th = document.documentElement.dataset.theme;
  document.getElementById('btn-theme').textContent = th === 'dark' ? '☀' : '🌙';
}})();

/* ── Tab switching ── */
function switchPane(name, btn) {{
  document.querySelectorAll('.pane').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
  const pane = document.getElementById('pane-' + name);
  if (pane) pane.classList.add('active');
  if (btn) btn.classList.add('active');
  currentPane = name;
  localStorage.setItem('sea_pane', name);
  onPaneActivated(name);
}}

function onPaneActivated(name) {{
  if (name === 'live') {{
    startLive();
  }} else {{
    stopLive();
    if (name === 'costs') loadCosts();
    else if (name === 'heatmap') initHeatmap();
    else if (name === 'solar') initSolar();
    else if (name === 'compare') initCompare();
    else if (name === 'anomalies') loadAnomalies();
  }}
}}

/* ──────────────────────────────────────────────
   LIVE TAB
────────────────────────────────────────────── */
function pwrClass(w) {{
  if (w < 100) return 'pwr-low';
  if (w < 500) return 'pwr-med';
  return 'pwr-high';
}}
function fmt(v, dec, unit) {{
  if (v === null || v === undefined || isNaN(v)) return '—';
  return v.toFixed(dec) + (unit ? ' ' + unit : '');
}}

function startLive() {{
  if (liveTimer) return;
  tick(true);
  liveTimer = setInterval(function() {{ if (!frozen) tick(false); }}, REFRESH_MS);
  document.getElementById('btn-freeze').addEventListener('click', toggleFreeze);
}}
function stopLive() {{
  if (liveTimer) {{ clearInterval(liveTimer); liveTimer = null; }}
}}
function toggleFreeze() {{
  frozen = !frozen;
  document.getElementById('btn-freeze').textContent = frozen ? '▶' : '⏸';
}}

async function tick(first) {{
  try {{
    const r = await fetch('/api/state');
    if (!r.ok) return;
    const data = await r.json();
    renderLive(data, first);
    const stamp = document.getElementById('live-stamp');
    const d = new Date();
    stamp.textContent = d.toLocaleTimeString();
  }} catch(e) {{
    // silent retry
  }}
}}

function renderLive(data, first) {{
  const grid = document.getElementById('live-grid');
  const devices = data.devices || [];

  // Init sparkline buffers
  devices.forEach(function(d) {{
    if (!sparkData[d.key]) sparkData[d.key] = [];
    const buf = sparkData[d.key];
    buf.push({{ ts: Date.now(), w: d.power_w || 0 }});
    if (buf.length > 60) buf.shift();
  }});

  if (first || grid.children.length !== devices.length) {{
    grid.innerHTML = '';
    devices.forEach(function(d) {{
      const card = buildDeviceCard(d);
      grid.appendChild(card);
    }});
  }} else {{
    devices.forEach(function(d, i) {{
      updateDeviceCard(grid.children[i], d);
    }});
  }}
}}

function buildDeviceCard(d) {{
  const div = document.createElement('div');
  div.className = 'dev-card';
  div.id = 'dc-' + d.key;
  div.innerHTML = devCardHTML(d);
  div.querySelector('.dev-header').addEventListener('click', function() {{
    const exp = div.querySelector('.dev-expand');
    exp.classList.toggle('open');
  }});
  return div;
}}

function devCardHTML(d) {{
  const pc = pwrClass(d.power_w || 0);
  const phases = (d.phases && d.phases.length > 0) ? d.phases : null;
  let phaseHtml = '';
  if (phases) {{
    phaseHtml = '<dl class="dev-kv">';
    phases.forEach(function(ph, i) {{
      phaseHtml += '<dt>Phase ' + (i+1) + '</dt><dd>' + fmt(ph.voltage_v,1,'V') + ' · ' + fmt(ph.current_a,2,'A') + ' · ' + fmt(ph.power_w,0,'W') + '</dd>';
    }});
    phaseHtml += '</dl>';
  }}
  const nilm = d.appliances && d.appliances.length ? '<div class="appl-list">' + d.appliances.map(function(a) {{ return '<span class="appl-chip">' + a + '</span>'; }}).join('') + '</div>' : '';
  return (
    '<div class="dev-header">' +
      '<div>' +
        '<div class="dev-name">' + esc(d.name || d.key) + '</div>' +
        '<div class="dev-meta">' +
          '<span>' + fmt(d.today_kwh, 3) + ' kWh</span>' +
          (d.cost_today !== undefined ? '<span>' + fmt(d.cost_today, 2) + ' €</span>' : '') +
        '</div>' +
      '</div>' +
      '<div class="dev-power ' + pc + '">' + fmt(d.power_w, 0) + ' W</div>' +
    '</div>' +
    '<div class="sparkline-wrap"><canvas class="sparkline" id="sp-' + d.key + '"></canvas></div>' +
    '<div class="dev-expand">' +
      '<dl class="dev-kv">' +
        '<dt>Voltage</dt><dd>' + fmt(d.voltage_v, 1, 'V') + '</dd>' +
        '<dt>Current</dt><dd>' + fmt(d.current_a, 2, 'A') + '</dd>' +
        '<dt>cos φ</dt><dd>' + (d.pf !== undefined ? fmt(d.pf, 2) : '—') + '</dd>' +
        '<dt>Freq</dt><dd>' + (d.freq_hz !== undefined ? fmt(d.freq_hz, 1, 'Hz') : '—') + '</dd>' +
      '</dl>' +
      phaseHtml +
      nilm +
    '</div>'
  );
}}

function updateDeviceCard(card, d) {{
  const pc = pwrClass(d.power_w || 0);
  const pw = card.querySelector('.dev-power');
  if (pw) {{ pw.textContent = fmt(d.power_w, 0) + ' W'; pw.className = 'dev-power ' + pc; }}
  const meta = card.querySelector('.dev-meta');
  if (meta) {{
    const spans = meta.querySelectorAll('span');
    if (spans[0]) spans[0].textContent = fmt(d.today_kwh, 3) + ' kWh';
    if (spans[1] && d.cost_today !== undefined) spans[1].textContent = fmt(d.cost_today, 2) + ' €';
  }}
  // Redraw sparkline
  const sp = card.querySelector('canvas.sparkline');
  if (sp && sparkData[d.key]) drawSparkline(sp, sparkData[d.key].map(function(p) {{ return p.w; }}));
}}

/* ──────────────────────────────────────────────
   SPARKLINE
────────────────────────────────────────────── */
function drawSparkline(canvas, values) {{
  const dpr = window.devicePixelRatio || 1;
  const W = canvas.offsetWidth || 200;
  const H = canvas.offsetHeight || 56;
  canvas.width = W * dpr;
  canvas.height = H * dpr;
  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, W, H);
  if (!values || values.length < 2) return;
  const max = Math.max(...values, 1);
  const min = 0;
  const pad = 4;
  const sx = (W - pad*2) / (values.length - 1);
  // Fill
  ctx.beginPath();
  values.forEach(function(v, i) {{
    const x = pad + i * sx;
    const y = H - pad - ((v - min) / (max - min)) * (H - pad*2);
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  }});
  ctx.lineTo(pad + (values.length-1)*sx, H - pad);
  ctx.lineTo(pad, H - pad);
  ctx.closePath();
  const cs = getComputedStyle(document.documentElement);
  const accent = cs.getPropertyValue('--accent').trim() || '#2563eb';
  ctx.fillStyle = accent + '28';
  ctx.fill();
  // Line
  ctx.beginPath();
  values.forEach(function(v, i) {{
    const x = pad + i * sx;
    const y = H - pad - ((v - min) / (max - min)) * (H - pad*2);
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  }});
  ctx.strokeStyle = accent;
  ctx.lineWidth = 1.5;
  ctx.stroke();
}}

/* ──────────────────────────────────────────────
   COSTS TAB
────────────────────────────────────────────── */
async function loadCosts() {{
  const el = document.getElementById('costs-content');
  el.innerHTML = '<p class="loading-msg">Loading…</p>';
  try {{
    const r = await fetch('/api/costs');
    if (!r.ok) throw new Error(r.status);
    const data = await r.json();
    renderCosts(data, el);
  }} catch(e) {{
    el.innerHTML = '<p class="error-msg">Error loading costs: ' + e.message + '</p>';
  }}
}}

function renderCosts(data, el) {{
  if (!data || !data.devices || data.devices.length === 0) {{
    el.innerHTML = '<p class="info-msg">No cost data available.</p>';
    return;
  }}
  let html = '';
  if (data.summary) {{
    const s = data.summary;
    html += '<div class="card" style="margin-bottom:10px"><div class="card-title">Summary</div>' +
      '<div class="metric-grid">' +
      metricCardHtml('Today', fmt(s.today_eur,2,'€'), fmt(s.today_kwh,3,'kWh')) +
      metricCardHtml('Week', fmt(s.week_eur,2,'€'), fmt(s.week_kwh,3,'kWh')) +
      metricCardHtml('Month', fmt(s.month_eur,2,'€'), fmt(s.month_kwh,3,'kWh')) +
      metricCardHtml('Year', fmt(s.year_eur,2,'€'), fmt(s.year_kwh,3,'kWh')) +
      '</div></div>';
  }}
  html += '<div class="card-grid">';
  data.devices.forEach(function(d) {{
    html += '<div class="card">' +
      '<div class="card-title">' + esc(d.name || d.key) + '</div>' +
      '<div class="metric-grid">' +
      metricCardHtml('Today', fmt(d.today_eur,2,'€'), fmt(d.today_kwh,3,'kWh')) +
      metricCardHtml('Week', fmt(d.week_eur,2,'€'), '') +
      metricCardHtml('Month', fmt(d.month_eur,2,'€'), '') +
      metricCardHtml('Year (proj.)', fmt(d.year_eur,2,'€'), '') +
      '</div></div>';
  }});
  html += '</div>';
  el.innerHTML = html;
}}

function metricCardHtml(label, value, sub) {{
  return '<div class="metric-card">' +
    '<div class="metric-label">' + esc(label) + '</div>' +
    '<div class="metric-value">' + esc(value) + '</div>' +
    (sub ? '<div class="metric-sub">' + esc(sub) + '</div>' : '') +
    '</div>';
}}

/* ──────────────────────────────────────────────
   HEATMAP TAB
────────────────────────────────────────────── */
function initHeatmap() {{
  const sel = document.getElementById('hm-device');
  if (sel.children.length === 0) {{
    DEVICES.forEach(function(d) {{
      const opt = document.createElement('option');
      opt.value = d.key;
      opt.textContent = d.name || d.key;
      sel.appendChild(opt);
    }});
  }}
  const ySel = document.getElementById('hm-year');
  if (ySel.children.length === 0) {{
    const now = new Date().getFullYear();
    for (let y = now; y >= now - 4; y--) {{
      const opt = document.createElement('option');
      opt.value = y;
      opt.textContent = y;
      ySel.appendChild(opt);
    }}
  }}
  loadHeatmap();
}}

async function loadHeatmap() {{
  const device = document.getElementById('hm-device').value;
  const year = document.getElementById('hm-year').value;
  const unit = document.getElementById('hm-unit').value;
  const calWrap = document.getElementById('hm-calendar-wrap');
  const hrWrap = document.getElementById('hm-hourly-wrap');
  calWrap.innerHTML = '<p class="loading-msg">Loading…</p>';
  hrWrap.innerHTML = '';
  if (!device) {{ calWrap.innerHTML = '<p class="info-msg">Select a device.</p>'; return; }}
  try {{
    const r = await fetch('/api/heatmap?device=' + encodeURIComponent(device) + '&year=' + year + '&unit=' + unit);
    if (!r.ok) throw new Error(r.status);
    const data = await r.json();
    renderHeatmapCalendar(data, calWrap, unit);
    renderHeatmapHourly(data, hrWrap, unit);
  }} catch(e) {{
    calWrap.innerHTML = '<p class="error-msg">Error: ' + e.message + '</p>';
  }}
}}

function renderHeatmapCalendar(data, el, unit) {{
  const daily = data.daily || {{}};
  const year = parseInt(document.getElementById('hm-year').value);
  const start = new Date(year, 0, 1);
  // Align to Monday
  while (start.getDay() !== 1) start.setDate(start.getDate() - 1);
  const end = new Date(year, 11, 31);
  while (end.getDay() !== 0) end.setDate(end.getDate() + 1);

  const vals = Object.values(daily).filter(function(v) {{ return v > 0; }});
  const maxVal = vals.length ? Math.max(...vals) : 1;

  const weeks = [];
  let cur = new Date(start);
  while (cur <= end) {{
    const week = [];
    for (let d = 0; d < 7; d++) {{
      week.push(new Date(cur));
      cur.setDate(cur.getDate() + 1);
    }}
    weeks.push(week);
  }}

  // Month labels
  const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  let monthLabelHtml = '<div class="hm-month-labels">';
  let lastMonth = -1;
  weeks.forEach(function(week) {{
    const m = week[0].getMonth();
    if (m !== lastMonth && week[0].getFullYear() === year) {{
      monthLabelHtml += '<span style="width:' + (12+2) + 'px">' + monthNames[m] + '</span>';
      lastMonth = m;
    }} else {{
      monthLabelHtml += '<span style="width:' + (12+2) + 'px"></span>';
    }}
  }});
  monthLabelHtml += '</div>';

  let gridHtml = '<div class="hm-grid">';
  weeks.forEach(function(week) {{
    gridHtml += '<div class="hm-week">';
    week.forEach(function(day) {{
      const key = day.toISOString().slice(0,10);
      const v = daily[key] || 0;
      const ratio = maxVal > 0 ? v / maxVal : 0;
      const alpha = Math.round(ratio * 200);
      const label = unit === 'eur' ? fmt(v,2,'€') : fmt(v,3,'kWh');
      const inYear = day.getFullYear() === year;
      const bg = inYear && v > 0 ? 'rgba(37,99,235,' + (0.12 + ratio*0.75).toFixed(2) + ')' : '';
      gridHtml += '<div class="hm-day" style="' + (bg ? 'background:' + bg + ';' : '') + '" data-date="' + key + '" data-val="' + label + '"></div>';
    }});
    gridHtml += '</div>';
  }});
  gridHtml += '</div>';

  el.innerHTML = '<div class="hm-calendar">' + monthLabelHtml + gridHtml + '</div>';

  // Tooltip
  el.querySelectorAll('.hm-day').forEach(function(cell) {{
    cell.addEventListener('mousemove', function(e) {{
      showHmTooltip(e, cell.dataset.date + ': ' + cell.dataset.val);
    }});
    cell.addEventListener('touchstart', function(e) {{
      e.preventDefault();
      showHmTooltip(e.touches[0], cell.dataset.date + ': ' + cell.dataset.val);
    }}, {{passive:false}});
    cell.addEventListener('mouseleave', hideHmTooltip);
    cell.addEventListener('touchend', hideHmTooltip);
  }});
}}

function renderHeatmapHourly(data, el, unit) {{
  const hourly = data.hourly;
  if (!hourly) return;
  const days = ['Mo','Tu','We','Th','Fr','Sa','Su'];
  const vals = [];
  for (let d = 0; d < 7; d++) for (let h = 0; h < 24; h++) {{
    const v = (hourly[d] && hourly[d][h]) ? hourly[d][h] : 0;
    if (v > 0) vals.push(v);
  }}
  const maxVal = vals.length ? Math.max(...vals) : 1;

  let html = '<div class="card"><div class="card-title">Hourly Pattern</div><div class="hm-table-wrap"><table class="hm-table"><thead><tr><th class="hm-head"></th>';
  for (let h = 0; h < 24; h++) html += '<th class="hm-head">' + h + '</th>';
  html += '</tr></thead><tbody>';
  for (let d = 0; d < 7; d++) {{
    html += '<tr><td class="hm-head" style="padding-right:4px">' + days[d] + '</td>';
    for (let h = 0; h < 24; h++) {{
      const v = (hourly[d] && hourly[d][h]) ? hourly[d][h] : 0;
      const ratio = maxVal > 0 ? v / maxVal : 0;
      const label = v > 0 ? (unit === 'eur' ? fmt(v,2) : fmt(v,3)) : '';
      const bg = v > 0 ? 'rgba(37,99,235,' + (0.10 + ratio*0.80).toFixed(2) + ')' : 'transparent';
      const title = days[d] + ' ' + h + 'h: ' + (unit === 'eur' ? fmt(v,2,'€') : fmt(v,3,'kWh'));
      html += '<td class="hm-cell" style="background:' + bg + '" data-tip="' + title + '">' + label + '</td>';
    }}
    html += '</tr>';
  }}
  html += '</tbody></table></div></div>';
  el.innerHTML = html;

  el.querySelectorAll('.hm-cell[data-tip]').forEach(function(cell) {{
    cell.addEventListener('mousemove', function(e) {{ showHmTooltip(e, cell.dataset.tip); }});
    cell.addEventListener('mouseleave', hideHmTooltip);
    cell.addEventListener('touchstart', function(e) {{
      e.preventDefault();
      showHmTooltip(e.touches[0], cell.dataset.tip);
    }}, {{passive:false}});
    cell.addEventListener('touchend', hideHmTooltip);
  }});
}}

function showHmTooltip(e, text) {{
  const tt = document.getElementById('hm-tooltip');
  tt.textContent = text;
  tt.style.display = 'block';
  tt.style.left = (e.clientX + 10) + 'px';
  tt.style.top = (e.clientY - 30) + 'px';
}}
function hideHmTooltip() {{
  document.getElementById('hm-tooltip').style.display = 'none';
}}

/* ──────────────────────────────────────────────
   SOLAR TAB
────────────────────────────────────────────── */
let solarPeriod = 'today';
function initSolar() {{
  const row = document.getElementById('solar-periods');
  if (row.children.length === 0) {{
    ['today','week','month','year'].forEach(function(p) {{
      const btn = document.createElement('button');
      btn.className = 'btn btn-outline btn-sm';
      btn.textContent = p.charAt(0).toUpperCase() + p.slice(1);
      btn.dataset.period = p;
      btn.addEventListener('click', function() {{
        solarPeriod = p;
        row.querySelectorAll('.btn').forEach(function(b) {{ b.classList.remove('btn-accent'); }});
        btn.classList.add('btn-accent');
        loadSolar(p);
      }});
      row.appendChild(btn);
    }});
    row.children[0].classList.add('btn-accent');
  }}
  loadSolar(solarPeriod);
}}

async function loadSolar(period) {{
  const el = document.getElementById('solar-content');
  el.innerHTML = '<p class="loading-msg">Loading…</p>';
  try {{
    const r = await fetch('/api/solar?period=' + period);
    if (!r.ok) throw new Error(r.status);
    const data = await r.json();
    renderSolar(data, el);
  }} catch(e) {{
    el.innerHTML = '<p class="error-msg">Error: ' + e.message + '</p>';
  }}
}}

function renderSolar(data, el) {{
  if (data.configured === false || data.enabled === false) {{
    el.innerHTML = '<p class="info-msg">Solar monitoring is not configured.</p>';
    return;
  }}
  const fields = [
    ['PV Production', fmt(data.pv_kwh,3,'kWh'), ''],
    ['Feed-in', fmt(data.feed_in_kwh,3,'kWh'), ''],
    ['Grid Draw', fmt(data.grid_kwh,3,'kWh'), ''],
    ['Self-Consumption', fmt(data.self_kwh,3,'kWh'), ''],
    ['Autarky', fmt(data.autarky_pct,1,'%'), ''],
    ['Revenue', fmt(data.revenue_eur,2,'€'), ''],
    ['Savings', fmt(data.savings_eur,2,'€'), ''],
  ];
  let html = '<div class="card"><div class="metric-grid">';
  fields.forEach(function(f) {{ html += metricCardHtml(f[0], f[1], f[2]); }});
  html += '</div></div>';
  el.innerHTML = html;
}}

/* ──────────────────────────────────────────────
   COMPARE TAB
────────────────────────────────────────────── */
let cmpInitialized = false;
function initCompare() {{
  if (cmpInitialized) return;
  cmpInitialized = true;

  const ctrl = document.getElementById('cmp-controls');
  const today = new Date().toISOString().slice(0,10);
  const monthAgo = new Date(Date.now() - 30*86400000).toISOString().slice(0,10);

  const devOptions = DEVICES.map(function(d) {{
    return '<option value="' + esc(d.key) + '">' + esc(d.name||d.key) + '</option>';
  }}).join('');

  ctrl.innerHTML =
    '<div class="card">' +
      '<div class="card-title">Device A</div>' +
      '<div class="controls-row">' +
        '<select id="cmp-da">' + devOptions + '</select>' +
        '<input type="date" id="cmp-fa" value="' + monthAgo + '">' +
        '<input type="date" id="cmp-ta" value="' + today + '">' +
      '</div>' +
      '<div class="card-title" style="margin-top:8px">Device B</div>' +
      '<div class="controls-row">' +
        '<select id="cmp-db">' + devOptions + '</select>' +
        '<input type="date" id="cmp-fb" value="' + monthAgo + '">' +
        '<input type="date" id="cmp-tb" value="' + today + '">' +
      '</div>' +
      '<div class="controls-row" style="margin-top:8px">' +
        '<select id="cmp-unit"><option value="kWh">kWh</option><option value="eur">€</option></select>' +
        '<select id="cmp-gran"><option value="total">Total</option><option value="daily">Daily</option><option value="monthly">Monthly</option></select>' +
        '<button class="btn btn-accent" onclick="loadCompare()">Compare</button>' +
      '</div>' +
    '</div>';

  // Quick presets
  const quick = document.getElementById('cmp-quick');
  const presets = [['month','Month'],['quarter','Quarter'],['halfyear','Half Year'],['year','Year']];
  let qhtml = '<div class="controls-row">';
  presets.forEach(function(p) {{
    qhtml += '<button class="btn btn-outline btn-sm" onclick="loadComparePreset(\'' + p[0] + '\')">' + p[1] + '</button>';
  }});
  qhtml += '</div>';
  quick.innerHTML = qhtml;
}}

async function loadComparePreset(preset) {{
  const result = document.getElementById('cmp-result');
  result.innerHTML = '<p class="loading-msg">Loading…</p>';
  try {{
    const url = '/api/compare?preset=' + preset +
      '&device_a=' + encodeURIComponent((document.getElementById('cmp-da')||{{}}).value||'') +
      '&device_b=' + encodeURIComponent((document.getElementById('cmp-db')||{{}}).value||'') +
      '&unit=' + ((document.getElementById('cmp-unit')||{{}}).value||'kWh') +
      '&gran=' + ((document.getElementById('cmp-gran')||{{}}).value||'total');
    const r = await fetch(url);
    if (!r.ok) throw new Error(r.status);
    const data = await r.json();
    renderCompare(data, result);
  }} catch(e) {{
    result.innerHTML = '<p class="error-msg">Error: ' + e.message + '</p>';
  }}
}}

async function loadCompare() {{
  const result = document.getElementById('cmp-result');
  result.innerHTML = '<p class="loading-msg">Loading…</p>';
  try {{
    const da = document.getElementById('cmp-da').value;
    const fa = document.getElementById('cmp-fa').value;
    const ta = document.getElementById('cmp-ta').value;
    const db = document.getElementById('cmp-db').value;
    const fb = document.getElementById('cmp-fb').value;
    const tb = document.getElementById('cmp-tb').value;
    const unit = document.getElementById('cmp-unit').value;
    const gran = document.getElementById('cmp-gran').value;
    const url = '/api/compare?device_a=' + encodeURIComponent(da) +
      '&from_a=' + fa + '&to_a=' + ta +
      '&device_b=' + encodeURIComponent(db) +
      '&from_b=' + fb + '&to_b=' + tb +
      '&unit=' + unit + '&gran=' + gran;
    const r = await fetch(url);
    if (!r.ok) throw new Error(r.status);
    const data = await r.json();
    renderCompare(data, result);
  }} catch(e) {{
    result.innerHTML = '<p class="error-msg">Error: ' + e.message + '</p>';
  }}
}}

function renderCompare(data, el) {{
  if (!data) {{ el.innerHTML = '<p class="info-msg">No data.</p>'; return; }}
  const unit = data.unit || 'kWh';
  const ta = data.total_a || 0;
  const tb = data.total_b || 0;
  const delta = ta - tb;
  const pct = tb !== 0 ? ((delta / Math.abs(tb)) * 100) : 0;

  let html = '<div class="card">' +
    '<div class="delta-grid">' +
    metricCardHtml(data.label_a || 'Device A', fmt(ta,3,unit), '') +
    metricCardHtml(data.label_b || 'Device B', fmt(tb,3,unit), '') +
    metricCardHtml('Delta', (delta >= 0 ? '+' : '') + fmt(delta,3,unit), '') +
    metricCardHtml('Δ %', (pct >= 0 ? '+' : '') + fmt(pct,1,'%'), '') +
    '</div></div>';

  // Bar chart
  if (data.series_a && data.series_b && data.labels) {{
    html += '<div class="card"><canvas class="bar-chart" id="cmp-canvas"></canvas></div>';
  }}
  el.innerHTML = html;

  if (data.series_a && data.series_b && data.labels) {{
    const canvas = document.getElementById('cmp-canvas');
    drawBars(canvas, data.labels, [
      {{ values: data.series_a, color: 'rgba(37,99,235,0.75)', label: data.label_a || 'A' }},
      {{ values: data.series_b, color: 'rgba(217,119,6,0.75)', label: data.label_b || 'B' }}
    ], {{ unit: unit }});
  }}
}}

/* ──────────────────────────────────────────────
   BAR CHART
────────────────────────────────────────────── */
function drawBars(canvas, labels, series, opts) {{
  opts = opts || {{}};
  const unit = opts.unit || '';
  const dpr = window.devicePixelRatio || 1;
  const W = canvas.offsetWidth || 300;
  const H = canvas.offsetHeight || 220;
  canvas.width = W * dpr;
  canvas.height = H * dpr;
  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, W, H);

  const cs = getComputedStyle(document.documentElement);
  const fg = cs.getPropertyValue('--fg').trim() || '#111';
  const muted = cs.getPropertyValue('--muted').trim() || '#888';
  const border = cs.getPropertyValue('--border').trim() || 'rgba(0,0,0,0.1)';

  const padL = 48, padR = 12, padT = 16, padB = 54;
  const cW = W - padL - padR;
  const cH = H - padT - padB;

  const allVals = [];
  series.forEach(function(s) {{ allVals.push(...s.values); }});
  const maxV = Math.max(...allVals, 0.001);

  // Y grid
  const yTicks = 5;
  ctx.strokeStyle = border;
  ctx.lineWidth = 1;
  ctx.fillStyle = muted;
  ctx.font = '10px system-ui';
  ctx.textAlign = 'right';
  for (let i = 0; i <= yTicks; i++) {{
    const v = (maxV / yTicks) * i;
    const y = padT + cH - (v / maxV) * cH;
    ctx.beginPath();
    ctx.moveTo(padL, y);
    ctx.lineTo(padL + cW, y);
    ctx.stroke();
    ctx.fillText(v.toFixed(1), padL - 4, y + 3);
  }}

  // Bars
  const n = labels.length;
  const groupW = cW / n;
  const barW = Math.max(2, Math.min(18, (groupW - 4) / series.length));

  series.forEach(function(s, si) {{
    ctx.fillStyle = s.color;
    labels.forEach(function(lbl, li) {{
      const v = s.values[li] || 0;
      const bh = (v / maxV) * cH;
      const x = padL + li * groupW + (groupW - barW * series.length) / 2 + si * barW;
      const y = padT + cH - bh;
      ctx.fillRect(x, y, barW - 1, bh);
    }});
  }});

  // X labels
  ctx.fillStyle = muted;
  ctx.textAlign = 'center';
  ctx.font = '9px system-ui';
  labels.forEach(function(lbl, li) {{
    const x = padL + li * groupW + groupW / 2;
    const maxLbl = 8;
    const text = lbl.length > maxLbl ? lbl.slice(0, maxLbl) + '…' : lbl;
    ctx.save();
    ctx.translate(x, padT + cH + 8);
    ctx.rotate(-Math.PI / 4);
    ctx.fillText(text, 0, 0);
    ctx.restore();
  }});

  // Legend
  ctx.textAlign = 'left';
  ctx.font = '10px system-ui';
  series.forEach(function(s, i) {{
    const lx = padL + i * 90;
    ctx.fillStyle = s.color;
    ctx.fillRect(lx, H - 14, 10, 10);
    ctx.fillStyle = fg;
    ctx.fillText(s.label, lx + 14, H - 5);
  }});
}}

/* ──────────────────────────────────────────────
   ANOMALIES TAB
────────────────────────────────────────────── */
async function loadAnomalies() {{
  const el = document.getElementById('anom-content');
  el.innerHTML = '<p class="loading-msg">Loading…</p>';
  try {{
    const r = await fetch('/api/anomalies');
    if (!r.ok) throw new Error(r.status);
    const data = await r.json();
    renderAnomalies(data, el);
  }} catch(e) {{
    el.innerHTML = '<p class="error-msg">Error: ' + e.message + '</p>';
  }}
}}

function renderAnomalies(data, el) {{
  let html = '';
  // Status badge
  const enabled = data.enabled !== false;
  html += '<div style="margin-bottom:10px">' +
    '<span class="badge ' + (enabled ? 'badge-green' : 'badge-red') + '">' +
    (enabled ? 'Enabled' : 'Disabled') + '</span>' +
    (data.model ? ' <span class="badge badge-yellow">' + esc(data.model) + '</span>' : '') +
    '</div>';

  const events = data.events || [];
  if (events.length === 0) {{
    html += '<p class="info-msg">No anomaly events found.</p>';
    el.innerHTML = html;
    return;
  }}
  events.forEach(function(ev) {{
    const ts = ev.timestamp ? new Date(ev.timestamp).toLocaleString() : '';
    const sigma = ev.sigma !== undefined ? ' · σ=' + fmt(ev.sigma, 2) : '';
    const val = ev.value !== undefined ? ' · ' + fmt(ev.value, 1) + ' W' : '';
    html += '<div class="event-card">' +
      '<div class="event-dot"></div>' +
      '<div class="event-body">' +
        '<div class="event-type">' + esc(ev.anomaly_type || ev.type || 'Anomaly') + '</div>' +
        '<div class="event-meta">' +
          esc(ev.device_name || ev.device || '') +
          (ts ? ' · ' + ts : '') +
          val + sigma +
        '</div>' +
        (ev.description ? '<div style="font-size:12px;margin-top:4px;color:var(--muted)">' + esc(ev.description) + '</div>' : '') +
      '</div></div>';
  }});
  el.innerHTML = html;
}}

/* ──────────────────────────────────────────────
   UTILITIES
────────────────────────────────────────────── */
function esc(s) {{
  if (s === null || s === undefined) return '';
  return String(s)
    .replace(/&/g,'&amp;')
    .replace(/</g,'&lt;')
    .replace(/>/g,'&gt;')
    .replace(/"/g,'&quot;');
}}

/* ──────────────────────────────────────────────
   BOOT
────────────────────────────────────────────── */
(function() {{
  // Restore last pane
  const last = localStorage.getItem('sea_pane');
  if (last && last !== 'live') {{
    const btn = document.querySelector('.nav-btn[onclick*="' + last + '"]');
    if (btn) switchPane(last, btn);
    else startLive();
  }} else {{
    startLive();
  }}
}})();
</script>
</body>
</html>
"""


_PLOTS_TEMPLATE = """<!doctype html>
<html lang="@@lang@@">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover" />
  <title>@@plots_title@@</title>
  <style>
    :root {
      --bg: #f6f7fb;
      --card: #ffffff;
      --fg: #111827;
      --muted: #4b5563;
      --border: rgba(17,24,39,0.12);
      --chipbg: rgba(17,24,39,0.03);
      --accent: #2563eb;
    }
    :root[data-theme="dark"] {
      --bg: #0b0f14;
      --card: #121821;
      --fg: #e8eef6;
      --muted: #9fb0c3;
      --border: rgba(255,255,255,0.08);
      --chipbg: rgba(255,255,255,0.02);
      --accent: #6aa7ff;
    }
    html, body { height: 100%; margin: 0; background: var(--bg); color: var(--fg);
      font-family: -apple-system, system-ui, Segoe UI, Roboto, Helvetica, Arial, sans-serif; }
    .wrap { padding: 10px; box-sizing: border-box; }
    /* Top bar: sticky for mobile convenience */
    .topbar {
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:10px;
      margin: 6px 2px 8px;
      position: sticky;
      top: 0;
      z-index: 5;
      background: color-mix(in srgb, var(--bg) 82%, transparent);
      backdrop-filter: blur(8px);
      padding: 6px 2px;
    }
    .title { font-size: 16px; font-weight: 650; margin: 0; }
    .nav { display:flex; gap:8px; align-items:center; }
    .nav a,
    .nav button {
      text-decoration:none;
      font-size: 13px;
      padding: 7px 10px;
      border-radius: 10px;
      border: 1px solid var(--border);
      background: var(--card);
      color: var(--fg);
      font-weight: 650;
      cursor:pointer;
    }
    .nav .ico { font-size: 14px; line-height: 1; }
    .nav .lab { display:inline; }
    .nav a:active, .nav button:active { transform: translateY(1px); }

    /* Mobile: stack controls for better usability */
    @media (max-width: 520px) {
      .wrap { padding: 8px; }
      .topbar { flex-direction: column; align-items: flex-start; }
      .nav { width: 100%; flex-wrap: wrap; }
      .nav .lab { display:none; }
      .controls { gap: 8px; }
      .ctrl { min-width: 100%; }
      select, input { width: 100%; }
      .plot { height: 320px; }
    }
    .meta { font-size: 12px; color: var(--muted); margin: 0 2px 10px; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 14px; padding: 10px; margin: 10px 0; }
    .plot { width: 100%; height: 360px; }
    @media (min-width: 900px) { .plot { height: 420px; } }
    @media (min-width: 1400px) { .plot { height: 520px; } }

    .controls { display:flex; flex-wrap:wrap; gap:10px; align-items:flex-end; }
    .ctrl { display:flex; flex-direction:column; gap:6px; min-width: 160px; }
    label { font-size: 12px; color: var(--muted); }
    select, input {
      font-size: 14px;
      padding: 8px 10px;
      border-radius: 10px;
      border: 1px solid var(--border);
      background: var(--card);
      color: var(--fg);
      box-sizing: border-box;
    }

    .devs { display:flex; flex-wrap:wrap; gap:8px; }
    .devchip { display:inline-flex; align-items:center; gap:6px; padding: 6px 10px; border-radius: 999px; border: 1px solid var(--border); background: var(--chipbg); }
    .devchip { cursor:pointer; user-select:none; }
    .devchip input { position:absolute; opacity:0; width:1px; height:1px; pointer-events:none; }
    .devchip.selected { background: rgba(106,167,255,0.18); border-color: rgba(106,167,255,0.45); }
    .hint { font-size: 12px; color: var(--muted); margin-top: 4px; }
  </style>
  <!-- Plotly is served locally to avoid CDN/Firewall/Offline issues -->
  <script defer src="/static/plotly.min.js" onerror="window.__plotly_load_error='plotly.min.js load failed' ;"></script>
</head>
<body>
<script>
// Apply persisted theme (shared with Live/Control pages)
(function(){
  try {
    const ls = localStorage.getItem('sea_web_theme');
    if (ls) document.documentElement.dataset.theme = ls;
    else if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches)
      document.documentElement.dataset.theme = 'dark';
  } catch (e) {}
})();
</script>

  <div class="wrap">
    <div class="topbar">
      <div class="title">@@plots_title@@</div>
      <div class="nav">
        <a id="nav_live" href="/"><span class="ico">🏠</span> <span class="lab">@@web_nav_live@@</span></a>
        <a id="nav_control" href="/control"><span class="ico">⚙︎</span> <span class="lab">@@web_nav_control@@</span></a>
        <button id="btn_theme" type="button" title="@@web_btn_theme@@">@@web_btn_theme@@</button>
      </div>
    </div>

    <div class="card">
      <div class="controls">
        <div class="ctrl">
          <label id="lblView">@@lbl_view@@</label>
          <select id="view">
            <option value="timeseries">W/V/A</option>
            <option value="kwh">kWh</option>
          </select>
        </div>

        <div class="ctrl" id="ctrlMetric">
          <label id="lblMetric">@@lbl_metric@@</label>
          <select id="metric">
            <option value="W">W</option>
            <option value="V">V</option>
            <option value="A">A</option>
            <option value="VAR">VAR</option>
            <option value="COSPHI">cos φ</option>
          </select>
        </div>

        <div class="ctrl" id="ctrlSeries">
          <label id="lblSeries">@@lbl_series@@</label>
          <select id="series">
            <option value="total">total</option>
            <option value="phases">phases</option>
          </select>
        </div>

<div class="ctrl" id="ctrlSmooth" style="display:none">
  <label id="lblSmooth">@@lbl_smooth@@</label>
  <input id="smooth_s" type="number" min="0" step="1" value="0" />
</div>

<div class="ctrl" id="ctrlDeadband" style="display:none">
  <label id="lblDeadband">@@lbl_deadband@@</label>
  <input id="deadband_var" type="number" min="0" step="1" value="0" />
</div>

<div class="ctrl" id="ctrlSignHold" style="display:none">
  <label id="lblSignHold">@@lbl_signhold@@</label>
  <input id="sign_hold_s" type="number" min="0" step="1" value="0" />
</div>

        <div class="ctrl" id="ctrlMode" style="display:none">
          <label id="lblMode">@@lbl_mode@@</label>
          <select id="mode">
            <option value="hours">hours</option>
            <option value="days">days</option>
            <option value="weeks">weeks</option>
            <option value="months">months</option>
          </select>
        </div>

        <div class="ctrl">
          <label id="lblRange">@@lbl_range@@</label>
          <select id="preset">
            <option value="1h">1h</option>
            <option value="6h">6h</option>
            <option value="24h">24h</option>
            <option value="7d">7d</option>
            <option value="30d">30d</option>
            <option value="custom">custom</option>
          </select>
        </div>

        <div class="ctrl" id="ctrlStart" style="display:none">
          <label id="lblFrom">@@lbl_from@@</label>
          <input id="start" placeholder="YYYY-MM-DD" />
        </div>
        <div class="ctrl" id="ctrlEnd" style="display:none">
          <label id="lblTo">@@lbl_to@@</label>
          <input id="end" placeholder="YYYY-MM-DD" />
        </div>

        <div class="ctrl" style="min-width: 120px;">
          <div class="hint" id="hintDevs">@@hint_max2@@</div>
        </div>
      </div>

      <div class="hint" style="margin-top:10px" id="lblDevices">@@lbl_devices@@</div>
      <div class="devs" id="devs">@@devices_html@@</div>
    </div>

    <div class="meta" id="meta"></div>
    <div class="card"><div id="plot1" class="plot"></div></div>
    <div class="card" id="card2" style="display:none"><div id="plot2" class="plot"></div></div>
  </div>

<!-- JSON payloads are injected as inert text to avoid JS parse errors even if placeholders aren't replaced -->
<script type="application/json" id="i18n_json">@@i18n_json@@</script>
<script type="application/json" id="devices_json">@@devices_json@@</script>

<script>
function safeJsonParse(id, fallback){
  try {
    const el = document.getElementById(id);
    const txt = (el && el.textContent) ? el.textContent.trim() : '';
    if (!txt) return fallback;
    return JSON.parse(txt);
  } catch (e) {
    return fallback;
  }
}

// i18n + boot device list
const I18N = safeJsonParse('i18n_json', {});
const BOOT_DEVICES = safeJsonParse('devices_json', []);

function renderDevicesFromBoot(){
  const host = document.getElementById('devs');
  if (!host) return;
  // If server already rendered devices, keep them
  if (host.querySelectorAll('input[type=checkbox]').length > 0) return;
  if (!Array.isArray(BOOT_DEVICES) || BOOT_DEVICES.length === 0){
    host.innerHTML = "<div class='hint'>" + esc(t('web.plots.no_devices')) + "</div>";
    return;
  }
  host.innerHTML = "";
  BOOT_DEVICES.forEach(d=>{
    const k = (d && d.key) ? String(d.key) : "";
    if (!k) return;
    const n = (d && d.name) ? String(d.name) : k;
    const lab = document.createElement('label');
    lab.className = 'devchip';
    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.value = k;
    const span = document.createElement('span');
    span.textContent = n;
    lab.appendChild(cb);
    lab.appendChild(span);
    host.appendChild(lab);
  });
}
function t(k){ return (I18N && I18N[k]) ? I18N[k] : k; }

// Theme (shared with Live/Control)
try {
  const LS_THEME = 'sea_web_theme';
  let theme = localStorage.getItem(LS_THEME);
  if (!theme) {
    theme = (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) ? 'dark' : 'light';
  }
  document.documentElement.dataset.theme = theme;
} catch (e) {}

function toggleTheme(){
  try {
    const LS_THEME = 'sea_web_theme';
    const cur = document.documentElement.dataset.theme || 'light';
    const nxt = (cur === 'dark') ? 'light' : 'dark';
    document.documentElement.dataset.theme = nxt;
    localStorage.setItem(LS_THEME, nxt);
    updateThemeButton();
    // re-render plots with matching colors
    try { if (window.__scheduleApplyPlots) window.__scheduleApplyPlots(50); } catch (e) {}
  } catch (e) {}
}

function updateThemeButton(){
  const btn = document.getElementById('btn_theme');
  if (!btn) return;
  // Show an icon; keep the translated text in title/aria label
  const cur = document.documentElement.dataset.theme || 'light';
  btn.setAttribute('aria-label', t('web.btn.theme'));
  btn.setAttribute('title', t('web.btn.theme'));
  btn.textContent = (cur === 'dark') ? '☀︎' : '☾';
}

function qp() {
  const u = new URL(window.location.href);
  const o = {};
  u.searchParams.forEach((v,k)=>{o[k]=v;});
  return o;
}
function setQP(newParams) {
  const u = new URL(window.location.href);
  Object.keys(newParams).forEach(k=>{
    const v = newParams[k];
    if (v === null || v === undefined || v === '') u.searchParams.delete(k);
    else u.searchParams.set(k, String(v));
  });
  history.replaceState(null, '', u.toString());
}
function esc(s){
  return String(s||"").replace(/[&<>"']/g, function(c){
    return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];
  });
}

function sleep(ms){ return new Promise(r=>setTimeout(r, ms)); }
async function waitPlotly(timeoutMs=12000){
  const t0 = Date.now();
  while (typeof window.Plotly === 'undefined') {
    if (typeof window.moduleName !== 'undefined') { window.Plotly = window.moduleName; break; }
    if (window.__plotly_load_error) throw new Error(t('web.err.plotly_timeout') + ' (' + window.__plotly_load_error + ')');
    if (Date.now()-t0 > timeoutMs) throw new Error(t('web.err.plotly_timeout'));
    await sleep(50);
  }
}

async function fetchJsonWithTimeout(url, timeoutMs=12000){
  const ctrl = new AbortController();
  const to = setTimeout(()=>ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, {signal: ctrl.signal, cache: 'no-store'});
    if (!res.ok) throw new Error('HTTP ' + res.status);
    return await res.json();
  } finally {
    clearTimeout(to);
  }
}

function cssVar(name){
  try { return getComputedStyle(document.documentElement).getPropertyValue(name).trim(); } catch (e) { return ''; }
}
function plotlyBaseLayout(extra){
  const fg = cssVar('--fg') || '#111827';
  const border = cssVar('--border') || 'rgba(0,0,0,0.12)';
  const grid = border;
  const base = {
    paper_bgcolor: 'rgba(0,0,0,0)',
    plot_bgcolor: 'rgba(0,0,0,0)',
    font: { color: fg },
    margin: { l:55, r:20, t:30, b:50 },
    xaxis: { gridcolor: grid, zerolinecolor: grid },
    yaxis: { gridcolor: grid, zerolinecolor: grid },
    legend: { orientation: 'h', font: { color: fg } },
  };
  return Object.assign(base, extra || {});
}


function refreshDevChipStyles(){
  document.querySelectorAll('#devs .devchip').forEach(lab=>{
    const cb = lab.querySelector('input[type=checkbox]');
    if (!cb) return;
    lab.classList.toggle('selected', !!cb.checked);
  });
}

function getSelectedDevices(){
  const cbs = document.querySelectorAll('#devs input[type=checkbox]');
  const sel = [];
  cbs.forEach(cb=>{ if (cb.checked) sel.push(cb.value); });
  return sel.slice(0,2);
}
function setHint(){
  const n = getSelectedDevices().length;
  const el = document.getElementById('hintDevs');
  if (el) el.textContent = t('web.plots.max2') + ' ('+n+'/2)';
}

function enforceMax2(changedCb){
  const all = Array.from(document.querySelectorAll('#devs input[type=checkbox]')).filter(cb=>cb.checked);
  if (all.length <= 2) return;
  if (changedCb) { changedCb.checked = false; return; }
  for (let i=2;i<all.length;i++) all[i].checked = false;
}

function bindDeviceEvents(){
  const cbs = document.querySelectorAll('#devs input[type=checkbox]');
  cbs.forEach(cb=>{
    cb.addEventListener('change', ()=>{
      enforceMax2(cb);
      refreshDevChipStyles();
      setHint();
      try {
        if (window.__scheduleApplyPlots) window.__scheduleApplyPlots(80);
      } catch (e) {}
    });
  });
  refreshDevChipStyles();
  setHint();
}

function setDevicesFromParams(params){
  const cbs = Array.from(document.querySelectorAll('#devs input[type=checkbox]'));
  const want = (params.devices ? String(params.devices).split(',').map(s=>s.trim()).filter(Boolean).slice(0,2) : []);
  if (want.length){
    cbs.forEach(cb=>{ cb.checked = want.includes(cb.value); });
  } else {
    let count = 0;
    cbs.forEach(cb=>{ cb.checked = (count < 2); if (cb.checked) count++; });
  }
  enforceMax2(null);
  refreshDevChipStyles();
  setHint();
}


function syncViewControls(){
  const view = document.getElementById('view').value;
  document.getElementById('ctrlMetric').style.display = (view === 'timeseries') ? '' : 'none';
  document.getElementById('ctrlSeries').style.display = (view === 'timeseries') ? '' : 'none';
  document.getElementById('ctrlMode').style.display = (view === 'kwh') ? '' : 'none';
  syncFilterControls();
}
function syncPresetControls(){
  const p = document.getElementById('preset').value;
  const show = (p === 'custom');
  document.getElementById('ctrlStart').style.display = show ? '' : 'none';
  document.getElementById('ctrlEnd').style.display = show ? '' : 'none';
}


function syncFilterControls(){
  try {
    const view = document.getElementById('view').value;
    const metric = String(document.getElementById('metric').value || '').toUpperCase();
    const isTs = (view === 'timeseries');
    const isVar = (metric === 'VAR');
    const isPf = (metric === 'COSPHI');
    document.getElementById('ctrlSmooth').style.display = (isTs && (isVar || isPf)) ? '' : 'none';
    document.getElementById('ctrlSignHold').style.display = (isTs && (isVar || isPf)) ? '' : 'none';
    document.getElementById('ctrlDeadband').style.display = (isTs && isVar) ? '' : 'none';
  } catch (e) {}
}

document.getElementById('view').addEventListener('change', ()=>{ syncViewControls(); });
document.getElementById('preset').addEventListener('change', ()=>{ syncPresetControls(); });

function presetToLenUnit(p){
  if (p.endsWith('h')) return [parseFloat(p.replace('h','')), 'hours'];
  if (p.endsWith('d')) return [parseFloat(p.replace('d','')), 'days'];
  return [24, 'hours'];
}


function toMs(ts){
  try { const t = new Date(ts); const ms = t.getTime(); return isNaN(ms) ? null : ms; } catch (e) { return null; }
}
function isNum(v){ return typeof v === 'number' && isFinite(v); }
function median(arr){
  const a = arr.filter(isNum).slice().sort((x,y)=>x-y);
  if (!a.length) return null;
  const mid = Math.floor(a.length/2);
  return (a.length%2) ? a[mid] : (a[mid-1]+a[mid])/2;
}
function rollingMean(y, win){
  win = Math.max(1, parseInt(win||1,10));
  const out = new Array(y.length);
  let sum = 0.0, count = 0;
  const q = [];
  for (let i=0;i<y.length;i++){
    const v = y[i];
    q.push(v);
    if (isNum(v)) { sum += v; count += 1; }
    if (q.length > win){
      const old = q.shift();
      if (isNum(old)) { sum -= old; count -= 1; }
    }
    out[i] = (count>0) ? (sum / count) : null;
  }
  return out;
}
function applyDeadband(y, thr){
  thr = parseFloat(thr||0);
  if (!(thr>0)) return y;
  const out = y.slice();
  for (let i=0;i<out.length;i++){
    const v = out[i];
    if (isNum(v) && Math.abs(v) < thr) out[i] = 0;
  }
  return out;
}
function stabilizeSign(x, y, holdSeconds){
  const holdMs = Math.max(0, parseFloat(holdSeconds||0) * 1000.0);
  if (!(holdMs>0)) return y;

  const tms = x.map(toMs);
  // Fallback to point-based hold if timestamps are missing
  const diffs = [];
  for (let i=1;i<tms.length;i++){
    const a=tms[i-1], b=tms[i];
    if (isNum(a) && isNum(b) && b>a) diffs.push(b-a);
  }
  const dt = median(diffs); // ms
  const holdPts = (dt && dt>0) ? Math.max(1, Math.round(holdMs/dt)) : Math.max(3, Math.round(holdMs/1000.0)); // rough fallback

  function sgn(v){ if (!isNum(v) || v===0) return 0; return (v>0) ? 1 : -1; }

  const out = y.slice();
  let stable = 0;

  // init stable sign from first non-zero
  for (let i=0;i<out.length;i++){
    const s = sgn(out[i]);
    if (s!==0) { stable = s; break; }
  }

  let cand = 0;
  let candSince = null;
  let candCount = 0;

  for (let i=0;i<out.length;i++){
    const v = out[i];
    const s = sgn(v);

    if (s===0){
      // Keep zeros as-is; do not reset stable sign.
      continue;
    }

    if (stable===0){
      stable = s;
      cand = 0; candSince = null; candCount = 0;
      continue;
    }

    if (s === stable){
      cand = 0; candSince = null; candCount = 0;
      continue;
    }

    // new sign differs
    const ti = tms[i];
    if (cand !== s){
      cand = s;
      candSince = isNum(ti) ? ti : null;
      candCount = 1;
    } else {
      candCount += 1;
    }

    let ok = false;
    if (isNum(ti) && isNum(candSince)){
      ok = (ti - candSince) >= holdMs;
    } else {
      ok = candCount >= holdPts;
    }

    if (ok){
      stable = cand;
      cand = 0; candSince = null; candCount = 0;
    } else {
      // enforce stable sign
      out[i] = Math.abs(v) * stable;
    }
  }
  return out;
}
function applyVarCosphiFilters(x, y, metricKey, opts){
  const m = String(metricKey||'').toUpperCase();
  const isVar = (m==='VAR' || m==='Q');
  const isPf = (m==='COSPHI' || m==='PF' || m==='POWERFACTOR');
  if (!(isVar || isPf)) return y;

  const smoothS = Math.max(0, parseFloat(opts.smooth_s||0));
  const deadband = Math.max(0, parseFloat(opts.deadband_var||0));
  const signHoldS = Math.max(0, parseFloat(opts.sign_hold_s||0));

  // normalize y -> numbers/null
  let yy = (y||[]).map(v=>{
    const n = (v===null || v===undefined || v==='') ? null : Number(v);
    return isFinite(n) ? n : null;
  });

  // Convert smooth seconds -> points
  if (smoothS > 0){
    const tms = (x||[]).map(toMs);
    const diffs = [];
    for (let i=1;i<tms.length;i++){
      const a=tms[i-1], b=tms[i];
      if (isNum(a) && isNum(b) && b>a) diffs.push(b-a);
    }
    const dt = median(diffs); // ms
    const win = (dt && dt>0) ? Math.max(1, Math.round((smoothS*1000.0)/dt)) : Math.max(1, Math.round(smoothS));
    yy = rollingMean(yy, win);
  }

  if (isVar && deadband > 0){
    yy = applyDeadband(yy, deadband);
  }

  if (signHoldS > 0){
    yy = stabilizeSign(x||[], yy, signHoldS);
  }

  return yy;
}

async function loadData() {
  document.getElementById('meta').textContent = t('web.loading');
  await waitPlotly();

  const params = qp();
  const view = params.view || 'timeseries';
  const qs = new URLSearchParams(params);
  const data = await fetchJsonWithTimeout('/api/plots_data?' + qs.toString());
  if (!data || !data.ok) {
    document.getElementById('meta').innerHTML = t('web.error') + ': ' + esc(data && data.error ? data.error : t('web.unknown'));
    return;
  }
  const title = (data && data.title) ? data.title : '';
  document.getElementById('meta').textContent = title;

  if ((data.view || view) === 'kwh') {
    const traces = [];
    (data.traces || []).forEach(tr=>{
      traces.push({type:'bar', name: tr.name, x: data.labels, y: tr.y});
    });
    Plotly.newPlot(
      'plot1',
      traces,
      plotlyBaseLayout({margin:{l:55,r:20,t:30,b:90}, barmode:'group', xaxis:{tickangle:45}, yaxis:{title:'kWh'}}),
      {responsive:true, displaylogo:false}
    );
    document.getElementById('card2').style.display = 'none';
    return;
  }

  const devs = data.devices || [];
  if (!devs || devs.length === 0) {
    document.getElementById('meta').textContent = t('web.plots.no_data');
    return;
  }

  function plotInto(div, dev) {
    if (!dev) return;
    const metric = (data.metric_label || data.metric || 'W');
    const metricKey = String((data.metric || '') || metric || 'W').toUpperCase();
    const params = qp();
    const wantSeries = (params.series || data.series || 'total').toLowerCase();
    const xs = dev.x || [];
    const ys = dev.y || [];

    const opts = {
      smooth_s: parseFloat((params.smooth_s||params.smooth||0) || 0) || 0,
      deadband_var: parseFloat((params.deadband_var||params.deadband||0) || 0) || 0,
      sign_hold_s: parseFloat((params.sign_hold_s||params.signhold||0) || 0) || 0,
    };

    function hasPhases(){
      if (!dev.phases) return false;
      const keys = Object.keys(dev.phases);
      for (let i=0;i<keys.length;i++){
        const p = dev.phases[keys[i]];
        if (p && Array.isArray(p.y) && p.y.length) return true;
      }
      return false;
    }

    let traces = [];
    const phasesOk = hasPhases();

    if (wantSeries === 'phases' && phasesOk) {
      // Plot phases only (L1/L2/L3). No extra 'total' line -> clearer.
      Object.keys(dev.phases).forEach(k=>{
        const p = dev.phases[k];
        if (!p) return;
        traces.push({type:'scatter', mode:'lines', name: k, x: p.x || xs, y: applyVarCosphiFilters((p.x || xs), (p.y || []), metricKey, opts)});
      });
    } else {
      // Total only (default). If phases selected but unavailable, fall back silently.
      traces = [{type:'scatter', mode:'lines', name: t('web.plots.series.total'), x: xs, y: applyVarCosphiFilters(xs, ys, metricKey, opts)}];
    }

    Plotly.newPlot(
      div,
      traces,
      plotlyBaseLayout({xaxis:{title:t('web.axis.time')}, yaxis:{title: metric}}),
      {responsive:true, displaylogo:false}
    );
  }

  plotInto('plot1', devs[0]);
  if (devs.length > 1) {
    document.getElementById('card2').style.display = '';
    plotInto('plot2', devs[1]);
  } else {
    document.getElementById('card2').style.display = 'none';
  }
}

async function init() {
  // Theme toggle
  try {
    const b = document.getElementById('btn_theme');
    if (b) b.addEventListener('click', ()=>{ toggleTheme(); });
    updateThemeButton();
  } catch (e) {}

  // Bind device pills (server-rendered) or render from BOOT_DEVICES fallback
  renderDevicesFromBoot();
  bindDeviceEvents();

  // Translate option labels (some <option> text is static in HTML)
  try {
    const opt = (sel, val) => document.querySelector(sel + " option[value='"+val+"']");
    const setTxt = (sel, val, key, fallback) => {
      const o = opt(sel, val);
      if (o) o.textContent = t(key) || fallback || o.textContent;
    };
    setTxt('#view', 'timeseries', 'plots.view.timeseries', 'W/V/A');
    setTxt('#view', 'kwh', 'plots.view.energy', 'kWh');
    setTxt('#preset', 'custom', 'common.custom', 'custom');

    setTxt('#mode', 'hours', 'plots.mode.hours', 'hours');
    setTxt('#mode', 'days', 'plots.mode.days', 'days');
    setTxt('#mode', 'weeks', 'plots.mode.weeks', 'weeks');
    setTxt('#mode', 'months', 'plots.mode.months', 'months');

    setTxt('#series', 'total', 'web.plots.series.total', 'total');
    setTxt('#series', 'phases', 'web.plots.series.phases', 'phases');
  } catch (e) {}

  const params = qp();
  // defaults
  const view = (params.view || 'timeseries');
  document.getElementById('view').value = view;
  syncViewControls();

  // preset/custom
  let preset = params.preset || '';
  if (!preset) preset = (params.start || params.end) ? 'custom' : '1h';
  document.getElementById('preset').value = preset;
  syncPresetControls();

  if (params.start) document.getElementById('start').value = params.start;
  if (params.end) document.getElementById('end').value = params.end;

  // metric/mode/series
  if (params.metric) document.getElementById('metric').value = String(params.metric).toUpperCase();
  if (params.mode) document.getElementById('mode').value = String(params.mode);
  if (params.series) document.getElementById('series').value = String(params.series);
  // filters (VAR/cosφ smoothing/deadband/sign-hold)
  if (params.smooth_s || params.smooth) document.getElementById('smooth_s').value = String(params.smooth_s || params.smooth);
  if (params.deadband_var || params.deadband) document.getElementById('deadband_var').value = String(params.deadband_var || params.deadband);
  if (params.sign_hold_s || params.signhold) document.getElementById('sign_hold_s').value = String(params.sign_hold_s || params.signhold);
  syncFilterControls();

  // devices (from query or default first two)
  setDevicesFromParams(params);

  // Build params from controls and (re)load the plot.
  let __apply_timer = null;
  async function applyNow(){
    const view = document.getElementById('view').value;
    const metric = document.getElementById('metric').value;
    const series = document.getElementById('series').value;
    const mode = document.getElementById('mode').value;
    const preset = document.getElementById('preset').value;
    const devs = getSelectedDevices();

    const smooth_s = parseFloat(document.getElementById('smooth_s') ? document.getElementById('smooth_s').value : '0') || 0;
    const deadband_var = parseFloat(document.getElementById('deadband_var') ? document.getElementById('deadband_var').value : '0') || 0;
    const sign_hold_s = parseFloat(document.getElementById('sign_hold_s') ? document.getElementById('sign_hold_s').value : '0') || 0;

    const out = {
      view,
      devices: devs.join(','),
      lang: '@@lang@@',
      preset,
      smooth_s: (smooth_s>0) ? smooth_s : '',
      deadband_var: (deadband_var>0) ? deadband_var : '',
      sign_hold_s: (sign_hold_s>0) ? sign_hold_s : '',
    };

    if (view === 'timeseries') {
      out.metric = metric;
      out.series = series;
      if (preset !== 'custom') {
        const [ln, unit] = presetToLenUnit(preset);
        out.len = ln;
        out.unit = unit;
        out.start = '';
        out.end = '';
      } else {
        out.start = document.getElementById('start').value;
        out.end = document.getElementById('end').value;
        out.len = '';
        out.unit = '';
      }
      out.mode = '';
    } else {
      // kwh
      out.mode = mode;
      out.metric = '';
      out.series = '';
      out.len = '';
      out.unit = '';
      if (preset !== 'custom') {
        const [ln, unit] = presetToLenUnit(preset);
        // for kwh we use start/end to define window (last ln unit)
        out.len = ln;
        out.unit = unit;
        out.start = '';
        out.end = '';
      } else {
        out.start = document.getElementById('start').value;
        out.end = document.getElementById('end').value;
      }
    }

    setQP(out);
    // Keep navigation links in sync (Live / Control should keep lang + token if present)
    try {
      const p = qp();
      const navQs = new URLSearchParams();
      navQs.set('lang', out.lang || '@@lang@@');
      if (p.t) navQs.set('t', p.t);
      const qs = navQs.toString();
      const suf = qs ? ('?' + qs) : '';
      const nl = document.getElementById('nav_live');
      const nc = document.getElementById('nav_control');
      if (nl) nl.setAttribute('href', '/' + suf);
      if (nc) nc.setAttribute('href', '/control' + suf);
    } catch (e) {}
    await loadData();

  }

  function scheduleApply(delayMs=180){
    try { if (__apply_timer) clearTimeout(__apply_timer); } catch (e) {}
    __apply_timer = setTimeout(()=>{ applyNow().catch(e=>{
      document.getElementById('meta').innerHTML = t('web.error') + ': ' + esc(e && e.message ? e.message : String(e));
    }); }, delayMs);
  }

  // Expose scheduleApply for device pill change handler
  window.__scheduleApplyPlots = scheduleApply;

  // Auto-apply on any change
  document.getElementById('view').addEventListener('change', ()=>{ syncViewControls(); scheduleApply(50); });
  document.getElementById('metric').addEventListener('change', ()=>{ syncFilterControls(); scheduleApply(50); });
  document.getElementById('series').addEventListener('change', ()=>{ scheduleApply(50); });
  document.getElementById('mode').addEventListener('change', ()=>{ scheduleApply(50); });
  document.getElementById('preset').addEventListener('change', ()=>{ syncPresetControls(); scheduleApply(50); });
  document.getElementById('start').addEventListener('input', ()=>{ if (document.getElementById('preset').value==='custom') scheduleApply(420); });
  document.getElementById('end').addEventListener('input', ()=>{ if (document.getElementById('preset').value==='custom') scheduleApply(420); });
  try {
    const s1 = document.getElementById('smooth_s');
    const s2 = document.getElementById('deadband_var');
    const s3 = document.getElementById('sign_hold_s');
    if (s1) s1.addEventListener('input', ()=>{ scheduleApply(250); });
    if (s2) s2.addEventListener('input', ()=>{ scheduleApply(250); });
    if (s3) s3.addEventListener('input', ()=>{ scheduleApply(250); });
  } catch (e) {}

  // Initial load (writes explicit params into the URL)
  await applyNow();
}

init().catch(e=>{
  document.getElementById('meta').innerHTML = t('web.error') + ': ' + esc(e && e.message ? e.message : String(e));
});
</script>
</body>
</html>
"""


_CONTROL_TEMPLATE = """<!doctype html>
<html lang="{lang}">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover" />
  <title>{web_control_title}</title>
  <style>
    :root {{
      /* Light theme (default) */
      --bg: #f6f7fb;
      --card: #ffffff;
      --fg: #111827;
      --muted: #4b5563;
      --accent: #2563eb;
      --border: rgba(17,24,39,0.12);
      --chipbg: rgba(17,24,39,0.04);
    }}
    :root[data-theme="dark"] {{
      --bg: #0b0f14;
      --card: #121821;
      --fg: #e8eef6;
      --muted: #9fb0c3;
      --accent: #6aa7ff;
      --border: rgba(255,255,255,0.08);
      --chipbg: rgba(255,255,255,0.02);
    }}
    html, body {{
      height: 100%;
      margin: 0;
      background: var(--bg);
      color: var(--fg);
      font-family: -apple-system, system-ui, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
    }}
    .wrap {{ padding: 10px; box-sizing: border-box; }}
    .topbar {{
      display:flex;
      align-items:flex-start;
      justify-content:flex-start;
      gap:10px;
      margin: 6px 2px 10px;
      flex-wrap: wrap;
    }}
    .toptext {{ flex: 1 1 100%; min-width: 0; }}
    .navrow {{
      display:flex;
      flex-wrap: wrap;
      gap:10px;
      align-items:center;
    }}
    .title {{ font-size: 16px; font-weight: 650; }}
    .meta {{ font-size: 12px; color: var(--muted); }}
    a.navlink {{
      color: var(--muted);
      text-decoration: none;
      font-size: 12px;
      padding: 6px 10px;
      border-radius: 999px;
      border: 1px solid var(--border);
      background: var(--chipbg);
    }}
    a.navlink.active {{ color: var(--fg); border-color: rgba(106,167,255,0.35); }}
    .grid {{ display:grid; grid-template-columns: 1fr; gap: 10px; }}
    @media (min-width: 900px) {{ .grid {{ grid-template-columns: 1fr 1fr; }} }}
    .card {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 10px;
      box-sizing: border-box;
      min-width: 0;
    }}
    .card h2 {{ margin:0 0 8px; font-size:14px; font-weight:650; }}
    label {{ font-size: 12px; color: var(--muted); }}
    input, select {{
      font-size: 13px;
      padding: 6px 8px;
      border-radius: 10px;
      border: 1px solid var(--border);
      background: var(--chipbg);
      color: var(--fg);
    }}
    button {{
      font-size: 13px;
      padding: 8px 10px;
      border-radius: 12px;
      border: 1px solid rgba(106,167,255,0.35);
      background: rgba(106,167,255,0.12);
      color: var(--fg);
      cursor: pointer;
    }}
    button:disabled {{ opacity: 0.5; cursor: default; }}
    .row {{ display:flex; flex-wrap:wrap; gap:8px; align-items:center; }}
    .row > * {{ flex: 0 0 auto; }}
    /* Button rows: keep buttons aligned on mobile + desktop.
       Mobile: 2 per row (last one spans full width if odd).
       Desktop/tablet: 3 per row. */
    .btnrow {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px;
      align-items: stretch;
    }}
    .btnrow button {{ width: 100%; }}
    .btnrow button:last-child:nth-child(odd) {{ grid-column: 1 / -1; }}
    @media (min-width: 700px) {{
      .btnrow {{ grid-template-columns: repeat(3, minmax(0, 1fr)); }}
      .btnrow button:last-child:nth-child(odd) {{ grid-column: auto; }}
    }}
    .log {{
      margin-top: 8px;
      font-size: 12px;
      color: var(--muted);
      white-space: pre-wrap;
      max-height: 240px;
      overflow:auto;
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 8px;
      background: var(--chipbg);
    }}
    .files a {{ color: var(--accent); text-decoration: none; }}
    .jobslist {{ display: grid; gap: 8px; margin-top: 8px; }}
    .jobcard {{
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 8px;
      background: var(--chipbg);
    }}
    .jobhead {{ display:flex; justify-content:space-between; gap:10px; align-items:baseline; }}
    .jobtitle {{ font-size: 13px; color: var(--fg); font-weight: 650; }}
    .jobmeta {{ font-size: 12px; color: var(--muted); }}
    .progrow {{ display:flex; gap:8px; align-items:center; margin-top: 6px; }}
    progress {{ width: 100%; height: 12px; }}
    .jobfiles {{ margin-top: 6px; display:flex; flex-wrap:wrap; gap:8px; }}
    .jobfiles a {{ color: var(--accent); text-decoration: none; font-size: 12px; }}
    .thumbs {{ display:grid; grid-template-columns: 1fr; gap: 8px; margin-top: 8px; }}
    @media (min-width: 700px) {{ .thumbs {{ grid-template-columns: 1fr 1fr; }} }}
    img.thumb {{ width: 100%; border-radius: 12px; border: 1px solid var(--border); }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="topbar">
      <div class="toptext">
        <div class="title">{web_control_title}</div>
        <div class="meta">{web_control_meta}</div>
      </div>
      <div class="navrow">
        <a class="navlink active" id="nav_control" href="/control">{web_nav_control}</a>
        <a class="navlink" id="btn_theme" href="#">🌓</a>
        <a class="navlink" id="nav_live" href="/">{web_nav_live}</a>
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <h2>{web_control_sync}</h2>
        <div class="row">
          <label for="sync_mode">{web_control_mode}</label>
          <select id="sync_mode">
            <option value="incremental">incremental</option>
            <option value="day">day</option>
            <option value="week">week</option>
            <option value="month">month</option>
            <option value="custom">custom</option>
          </select>
          <label for="sync_start">{web_control_start}</label>
          <input id="sync_start" placeholder="01.10.2025" />
          <button id="btn_sync">{web_control_btn_sync}</button>
        </div>
        <div class="log" id="sync_log">–</div>
      </div>

      <div class="card">
        <h2>{web_control_plots}</h2>
        <div class="row" style="margin-bottom:8px">
          <a class="navlink" id="open_plotly" href="/plots">{web_control_open_plotly}</a>
        </div>
        <div class="row">
          <label for="plot_mode">{web_control_mode}</label>
          <select id="plot_mode">
            <option value="days">days</option>
            <option value="weeks">weeks</option>
            <option value="months">months</option>
            <option value="all">all</option>
          </select>
          <label for="plot_start">{web_control_plots_from}</label>
          <input id="plot_start" placeholder="YYYY-MM-DD" />
          <label for="plot_end">{web_control_plots_to}</label>
          <input id="plot_end" placeholder="YYYY-MM-DD" />
          <button id="btn_plots">{web_control_btn_plots}</button>
        </div>
        <div class="thumbs" id="plot_thumbs"></div>
      </div>

      <div class="card">
        <h2>{web_control_export}</h2>
        <div class="row">
          <label for="exp_start">{web_control_plots_from}</label>
          <input id="exp_start" placeholder="YYYY-MM-DD" />
          <label for="exp_end">{web_control_plots_to}</label>
          <input id="exp_end" placeholder="YYYY-MM-DD" />
        </div>
        <div class="row" style="margin-top:8px">
          <label for="inv_period">{web_control_invoice}</label>
          <select id="inv_period">
            <option value="custom">custom</option>
            <option value="day">day</option>
            <option value="week">week</option>
            <option value="month">month</option>
            <option value="year">year</option>
          </select>
          <label for="inv_anchor">{web_control_anchor}</label>
          <input id="inv_anchor" placeholder="TT.MM.JJJJ" />
          <span class="meta">{web_control_custom_note}</span>
        </div>
        <div class="btnrow" style="margin-top:8px">
          <button id="btn_summary">{web_control_btn_summary}</button>
          <button id="btn_invoices">{web_control_btn_invoices}</button>
          <button id="btn_bundle">{web_control_btn_bundle}</button>
          <button id="btn_report_day">{web_control_btn_report_day}</button>
          <button id="btn_report_month">{web_control_btn_report_month}</button>
        </div>
        <div class="log files" id="export_log">–</div>
      </div>

      <div class="card">
        <h2>{web_control_jobs}</h2>
        <div class="meta">{web_control_jobs_meta}</div>
        <div class="jobslist" id="jobs"></div>
      </div>
    </div>
  </div>

<script>
// Theme (shared with Live page)
const LS_THEME = "sea_web_theme";
function setTheme(theme){
  const v = (theme==='dark')?'dark':'light';
  document.documentElement.dataset.theme = v;
  localStorage.setItem(LS_THEME, v);
  updateThemeBtn();
}
function updateThemeBtn(){
  const el = document.getElementById('btn_theme');
  if(!el) return;
  const cur = document.documentElement.dataset.theme || 'dark';
  el.textContent = (cur==='dark') ? '🌙' : '☀️';
  el.title = (cur==='dark') ? 'Dark mode' : 'Light mode';
}
(function initTheme(){
  let theme = localStorage.getItem(LS_THEME);
  if(!theme){ theme = (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) ? 'dark' : 'light'; }
  document.documentElement.dataset.theme = theme;
  updateThemeBtn();
})();
(function bindTheme(){
  const el = document.getElementById('btn_theme');
  if(el) el.addEventListener('click', (ev)=>{ ev.preventDefault(); const cur=document.documentElement.dataset.theme||'dark'; setTheme(cur==='dark'?'light':'dark'); });
})();

// keep token across navigation links
try {
  const qs = window.location.search || "";
  if (qs) {
    document.getElementById("nav_live").setAttribute("href", "/" + qs);
    document.getElementById("nav_control").setAttribute("href", "/control" + qs);
    const op = document.getElementById("open_plotly");
    if (op) op.setAttribute("href", "/plots" + qs);
  }
} catch (e) {}

function qs() { return ""; }

async function api(path, opts) {
  const u = path + qs();
  const r = await fetch(u, opts || {cache:"no-store"});
  if (!r.ok) {
    let t = "";
    try { t = await r.text(); } catch(e) {}
    throw new Error(`HTTP ${r.status}: ${t || r.statusText}`);
  }
  return r;
}

async function run(action, params) {
  const r = await api("/api/run", {
    method: "POST",
    headers: {"Content-Type":"application/json"},
    body: JSON.stringify({action, params: params || {} })
  });
  try {
    return await r.json();
  } catch (e) {
    const t = await r.text();
    throw new Error(t || "Antwort ist kein JSON");
  }
}

function esc(s) {
  return String(s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
}

function renderJob(j) {
  const t = j.started_at ? new Date(j.started_at*1000).toLocaleString() : "";
  const st = j.status || "";
  const a = j.action || "";
  const pct = (j.progress_overall !== undefined && j.progress_overall !== null) ? parseInt(j.progress_overall,10) : 0;
  const err = j.error ? `<div class="jobmeta"><b>Fehler:</b> ${esc(j.error)}</div>` : "";
  let progLines = "";
  const prog = j.progress || {};
  const keys = Object.keys(prog);
  if (keys.length) {
    progLines = keys.map(k => {
      const p = prog[k] || {};
      const pd = parseInt(p.done||0,10);
      const pt = parseInt(p.total||1,10);
      const pp = parseInt(p.percent||0,10);
      const pm = esc(p.message||"");
      return `<div class="jobmeta">${esc(k)}: ${pp}% (${pd}/${pt}) ${pm?"• "+pm:""}</div>`;
    }).join("");
  }
  let files = "";
  const r = j.result || {};
  if (r && r.files && Array.isArray(r.files)) {
    files = `<div class="jobfiles">` + r.files.map(f => {
      const url = (f.url||"") + qs();
      const name = esc(f.name||"file");
      return `<a href="${url}" target="_blank">${name}</a>`;
    }).join("") + `</div>`;
  }
  return `
    <div class="jobcard">
      <div class="jobhead">
        <div class="jobtitle">#${esc(j.id)} • ${esc(a)}</div>
        <div class="jobmeta">${esc(st)} • ${esc(t)}</div>
      </div>
      <div class="progrow">
        <progress max="100" value="${isNaN(pct)?0:pct}"></progress>
        <div class="jobmeta">${isNaN(pct)?0:pct}%</div>
      </div>
      ${progLines}
      ${err}
      ${files}
    </div>
  `;
}

async function refreshJobs() {
  try {
    const r = await api("/api/jobs", {cache:"no-store"});
    const j = await r.json();
    const arr = (j && j.jobs) ? j.jobs : [];
    const el = document.getElementById("jobs");
    if (!el) return;
    if (!arr.length) { el.innerHTML = `<div class="jobmeta">–</div>`; return; }
    el.innerHTML = arr.map(renderJob).join("");
  } catch (e) {}
}

setInterval(refreshJobs, 1500);
refreshJobs();

document.getElementById("btn_sync").addEventListener("click", async ()=>{
  const mode = document.getElementById("sync_mode").value;
  const start = document.getElementById("sync_start").value;
  document.getElementById("sync_log").textContent = "Starte …";
  try {
    const res = await run("sync", {mode, start_date: start});
    document.getElementById("sync_log").textContent = JSON.stringify(res, null, 2);
  } catch (e) {
    document.getElementById("sync_log").textContent = "Fehler: " + (e && e.message ? e.message : String(e));
  }
});

document.getElementById("btn_plots").addEventListener("click", async ()=>{
  const mode = document.getElementById("plot_mode").value;
  const start = document.getElementById("plot_start").value;
  const end = document.getElementById("plot_end").value;
  const thumbs = document.getElementById("plot_thumbs");
  thumbs.innerHTML = "";
  try {
    const res = await run("plots", {mode, start, end});
    if (res && res.files) {
      res.files.forEach(f => {
        const img = document.createElement("img");
        img.className = "thumb";
        img.src = f.url + qs();
        img.alt = f.name;
        thumbs.appendChild(img);
      });
    } else {
      thumbs.innerHTML = `<div class="jobmeta">${esc(JSON.stringify(res||{}, null, 2))}</div>`;
    }
  } catch (e) {
    thumbs.innerHTML = `<div class="jobmeta">Fehler: ${esc(e && e.message ? e.message : String(e))}</div>`;
  }
});

document.getElementById("btn_summary").addEventListener("click", async ()=>{
  const start = document.getElementById("exp_start").value;
  const end = document.getElementById("exp_end").value;
  const el = document.getElementById("export_log");
  el.innerHTML = "";
  try {
    const res = await run("export_summary", {start, end});
    if (res && res.files) {
      res.files.forEach(f=>{
        const a = document.createElement("a");
        a.href = f.url + qs();
        a.textContent = f.name;
        a.target = "_blank";
        el.appendChild(a);
        el.appendChild(document.createElement("br"));
      });
    } else {
      el.textContent = JSON.stringify(res, null, 2);
    }
  } catch (e) {
    el.textContent = "Fehler: " + (e && e.message ? e.message : String(e));
  }
});

document.getElementById("btn_invoices").addEventListener("click", async ()=>{
  const start = document.getElementById("exp_start").value;
  const end = document.getElementById("exp_end").value;
  const period = document.getElementById("inv_period").value;
  const anchor = document.getElementById("inv_anchor").value;
  const el = document.getElementById("export_log");
  el.innerHTML = "";
  try {
    const res = await run("export_invoices", {start, end, period, anchor});
    if (res && res.files) {
      res.files.forEach(f=>{
        const a = document.createElement("a");
        a.href = f.url + qs();
        a.textContent = f.name;
        a.target = "_blank";
        el.appendChild(a);
        el.appendChild(document.createElement("br"));
      });
    } else {
      el.textContent = JSON.stringify(res, null, 2);
    }
  } catch (e) {
    el.textContent = "Fehler: " + (e && e.message ? e.message : String(e));
  }
});

document.getElementById("btn_bundle").addEventListener("click", async ()=>{
  const el = document.getElementById("export_log");
  el.innerHTML = "";
  try {
    const res = await run("bundle", {hours: 48});
    if (res && res.files) {
      res.files.forEach(f=>{
        const a = document.createElement("a");
        a.href = f.url + qs();
        a.textContent = f.name;
        a.target = "_blank";
        el.appendChild(a);
        el.appendChild(document.createElement("br"));
      });
    } else {
      el.textContent = JSON.stringify(res, null, 2);
    }
  } catch (e) {
    el.textContent = "Fehler: " + (e && e.message ? e.message : String(e));
  }
});

document.getElementById("btn_report_day").addEventListener("click", async ()=>{
  const anchor = document.getElementById("inv_anchor").value;
  const el = document.getElementById("export_log");
  el.textContent = "Starte Tagesreport … (siehe Jobs unten)";
  try {
    const res = await run("report", {period: "day", anchor});
    if (res && res.job && res.job.id) {
      el.textContent = `Job #${res.job.id} gestartet. Unten bei Jobs erscheint der Download.`;
    } else {
      el.textContent = JSON.stringify(res, null, 2);
    }
  } catch (e) {
    el.textContent = "Fehler: " + (e && e.message ? e.message : String(e));
  }
});

document.getElementById("btn_report_month").addEventListener("click", async ()=>{
  const anchor = document.getElementById("inv_anchor").value;
  const el = document.getElementById("export_log");
  el.textContent = "Starte Monatsreport … (siehe Jobs unten)";
  try {
    const res = await run("report", {period: "month", anchor});
    if (res && res.job && res.job.id) {
      el.textContent = `Job #${res.job.id} gestartet. Unten bei Jobs erscheint der Download.`;
    } else {
      el.textContent = JSON.stringify(res, null, 2);
    }
  } catch (e) {
    el.textContent = "Fehler: " + (e && e.message ? e.message : String(e));
  }
});
</script>
</body>
</html>
"""


class _Handler(BaseHTTPRequestHandler):
    # set by server factory
    store: LiveStateStore
    html_bytes: bytes
    dashboard: "LiveWebDashboard"


    def _send_forbidden(self) -> None:
        """Return a helpful HTML page instead of an empty 403.

        Users often open the base URL without the token; this page explains how
        to access the dashboard safely and lets them paste the token.
        """
        lang = normalize_lang(getattr(self.dashboard, 'lang', 'de'))
        title = _t(lang, 'web.token.denied_title')
        protected = _t(lang, 'web.token.protected')
        placeholder = _t(lang, 'web.token.placeholder')
        btn_open = _t(lang, 'web.token.open')
        tip = _t(lang, 'web.token.tip')

        body = """<!doctype html>
<html lang="__LANG__">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>__TITLE__</title>
  <style>
    body{font-family:-apple-system,system-ui,Segoe UI,Roboto,Helvetica,Arial,sans-serif;padding:18px;background:#0b0f14;color:#e8eef6}
    .card{background:#121821;border:1px solid rgba(255,255,255,.08);border-radius:14px;padding:14px;max-width:720px}
    .muted{color:#9fb0c3;font-size:13px;line-height:1.35}
    code{background:rgba(255,255,255,.06);padding:2px 6px;border-radius:8px}
    input{width:100%;box-sizing:border-box;padding:10px;border-radius:10px;border:1px solid rgba(255,255,255,.15);background:rgba(255,255,255,.04);color:#e8eef6;font-size:16px;margin-top:10px}
    button{margin-top:10px;padding:10px 12px;border-radius:12px;border:1px solid rgba(106,167,255,.35);background:rgba(106,167,255,.12);color:#e8eef6;font-size:16px;cursor:pointer}
  </style>
</head>
<body>
  <div class="card">
    <h2 style="margin:0 0 8px">__TITLE__</h2>
    <div class="muted">__PROTECTED__</div>
    <input id="tok" placeholder="__PLACEHOLDER__" />
    <button onclick="go()">__BTN_OPEN__</button>
    <div class="muted" style="margin-top:10px">__TIP__</div>
  </div>
  <script>
    function go(){
      const t = (document.getElementById('tok').value||'').trim();
      if(!t) return;
      location.href = '/?t=' + encodeURIComponent(t);
    }
  </script>
</body>
</html>"""

        # Safe token replacement (avoid f-string / format braces in CSS/JS)
        body = (body
                .replace("__LANG__", str(lang))
                .replace("__TITLE__", str(title))
                .replace("__PROTECTED__", str(protected))
                .replace("__PLACEHOLDER__", str(placeholder))
                .replace("__BTN_OPEN__", str(btn_open))
                .replace("__TIP__", str(tip)))
        b = body.encode('utf-8')
        self.send_response(403)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Content-Length', str(len(b)))
        self.end_headers()
        try:
            self.wfile.write(b)
        except Exception:
            pass

    def do_GET(self) -> None:
        try:
            parsed0 = urlparse(self.path)
            path_only = parsed0.path or "/"

            # Serve Plotly JS locally (offline-safe). Requires python `plotly` package.
            if path_only == "/static/plotly.min.js":
                body = _plotly_min_js_bytes()
                if not body:
                    msg = (
                        "/* plotly.min.js not available. Install the python package 'plotly' via start.command */"
                    ).encode("utf-8")
                    self.send_response(404)
                    self.send_header("Content-Type", "application/javascript; charset=utf-8")
                    self.send_header("Cache-Control", "no-store")
                    self.send_header("Content-Length", str(len(msg)))
                    self.end_headers()
                    self.wfile.write(msg)
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/javascript; charset=utf-8")
                self.send_header("Cache-Control", "public, max-age=3600")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            # Serve generated files (plots/pdfs/zip) from the project's export directory.
            if path_only.startswith("/files/"):
                try:
                    parsed = urlparse(self.path)
                    rel = parsed.path[len("/files/") :]
                    body, ctype = self.dashboard.read_file_bytes(rel)
                    self.send_response(200)
                    self.send_header("Content-Type", ctype)
                    self.send_header("Cache-Control", "no-store")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                except FileNotFoundError:
                    self.send_response(404)
                    self.end_headers()
                    return
                except Exception:
                    self.send_response(500)
                    self.end_headers()
                    return

            if path_only.startswith("/api/jobs"):
                payload = self.dashboard.get_jobs()
                body = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path_only.startswith("/api/job"):
                # /api/job?id=<n>
                try:
                    parsed = urlparse(self.path)
                    qs = parse_qs(parsed.query or "")
                    jid = int((qs.get("id") or [""])[0])
                except Exception:
                    jid = -1
                payload = self.dashboard.get_job(jid)
                body = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path_only.startswith("/api/state"):
                raw_snap = self.store.snapshot()
                # Build devices array expected by the v9 JS frontend.
                # raw_snap format: {"device_key": [...points], "_appliances": {key: [...]}}
                appliances_map: Dict[str, List[Any]] = raw_snap.get("_appliances", {})  # type: ignore[assignment]
                dev_meta_by_key: Dict[str, Dict[str, Any]] = {
                    d.get("key", ""): d
                    for d in (self.dashboard.devices_meta or [])
                    if isinstance(d, dict) and d.get("key")
                }
                devices_list: List[Dict[str, Any]] = []
                for dkey, points in raw_snap.items():
                    if dkey.startswith("_") or not isinstance(points, list) or not points:
                        continue
                    latest: Dict[str, Any] = points[-1]
                    meta = dev_meta_by_key.get(dkey, {})
                    name = str(meta.get("name") or dkey)
                    va = float(latest.get("va") or 0)
                    vb = float(latest.get("vb") or 0)
                    vc = float(latest.get("vc") or 0)
                    ia = float(latest.get("ia") or 0)
                    ib = float(latest.get("ib") or 0)
                    ic = float(latest.get("ic") or 0)
                    pa = float(latest.get("pa") or 0)
                    pb = float(latest.get("pb") or 0)
                    pc = float(latest.get("pc") or 0)
                    non_zero_v = [v for v in [va, vb, vc] if v > 0]
                    voltage_v = sum(non_zero_v) / len(non_zero_v) if non_zero_v else 0.0
                    current_a = ia + ib + ic if (ib > 0 or ic > 0) else ia
                    phases: List[Dict[str, float]] = []
                    if vb > 0 or vc > 0:
                        if va > 0:
                            phases.append({"voltage_v": va, "current_a": ia, "power_w": pa})
                        if vb > 0:
                            phases.append({"voltage_v": vb, "current_a": ib, "power_w": pb})
                        if vc > 0:
                            phases.append({"voltage_v": vc, "current_a": ic, "power_w": pc})
                    raw_appl = appliances_map.get(dkey, [])
                    appl_strs: List[str] = [
                        f"{a.get('icon', '')} {a.get('id', '')}".strip()
                        for a in raw_appl
                        if isinstance(a, dict)
                    ]
                    devices_list.append({
                        "key": dkey,
                        "name": name,
                        "power_w": float(latest.get("power_total_w") or 0),
                        "today_kwh": float(latest.get("kwh_today") or 0),
                        "cost_today": float(latest.get("cost_today") or 0),
                        "voltage_v": voltage_v,
                        "current_a": current_a,
                        "pf": float(latest.get("cosphi_total") or 0),
                        "freq_hz": float(latest.get("freq_hz") or 50),
                        "phases": phases,
                        "appliances": appl_strs,
                    })
                payload = {"devices": devices_list}
                body = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return


            if path_only.startswith("/api/costs"):
                try:
                    payload = self.dashboard.on_action("costs", {})
                except Exception as e:
                    payload = {"ok": False, "error": str(e)}
                body = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path_only.startswith("/api/config"):
                payload = self.dashboard.get_config()
                body = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path_only.startswith("/api/plots_data"):
                # /api/plots_data?... -> delegated to app callback (no Tk)
                try:
                    parsed = urlparse(self.path)
                    qs = parse_qs(parsed.query or "")
                    params: Dict[str, Any] = {k: (v[0] if isinstance(v, list) and v else v) for k, v in qs.items()}
                    payload = self.dashboard.on_action("plots_data", params)
                except Exception as e:
                    payload = {"ok": False, "error": str(e)}
                body = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            # ── New v9.0.0 API endpoints ──────────────────────────────────────
            if path_only.startswith("/api/heatmap"):
                try:
                    parsed = urlparse(self.path)
                    qs = parse_qs(parsed.query or "")
                    params_hm: Dict[str, Any] = {k: (v[0] if isinstance(v, list) and v else v) for k, v in qs.items()}
                    payload = self.dashboard.on_action("heatmap", params_hm)
                except Exception as e:
                    payload = {"ok": False, "error": str(e)}
                body = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path_only.startswith("/api/solar"):
                try:
                    parsed = urlparse(self.path)
                    qs = parse_qs(parsed.query or "")
                    params_sol: Dict[str, Any] = {k: (v[0] if isinstance(v, list) and v else v) for k, v in qs.items()}
                    payload = self.dashboard.on_action("solar", params_sol)
                except Exception as e:
                    payload = {"ok": False, "error": str(e)}
                body = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path_only.startswith("/api/compare"):
                try:
                    parsed = urlparse(self.path)
                    qs = parse_qs(parsed.query or "")
                    params_cmp: Dict[str, Any] = {k: (v[0] if isinstance(v, list) and v else v) for k, v in qs.items()}
                    payload = self.dashboard.on_action("compare", params_cmp)
                except Exception as e:
                    payload = {"ok": False, "error": str(e)}
                body = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path_only.startswith("/api/anomalies"):
                try:
                    payload = self.dashboard.on_action("anomalies", {})
                except Exception as e:
                    payload = {"ok": False, "error": str(e)}
                body = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path_only == "/plots" or path_only.startswith("/plots/"):
                body = self.dashboard.plots_html_bytes
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path_only.startswith("/control"):
                body = self.dashboard.control_html_bytes
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path_only == "/" or path_only.startswith("/index.html"):
                body = self.html_bytes
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_response(404)
            self.end_headers()
            return
        except Exception as e:
            msg = str(e)
            body = ("<!doctype html><html><body style='font-family:system-ui;padding:16px'>"                    "<h3>Web-Dashboard Fehler</h3>"                    "<pre style='white-space:pre-wrap'>" + msg + "</pre></body></html>").encode("utf-8")
            try:
                self.send_response(500)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                pass
            return

    def do_POST(self) -> None:
        try:
            parsed0 = urlparse(self.path)
            path_only = parsed0.path or "/"

            def _read_body_text() -> str:
                """Read request body as text.

                Some clients (notably mobile Safari in certain cases) may send
                `Transfer-Encoding: chunked` instead of a Content-Length.
                BaseHTTPRequestHandler does not decode chunked bodies for us,
                so we handle both.
                """
                te = (self.headers.get("Transfer-Encoding", "") or "").lower()
                if "chunked" in te:
                    chunks: List[bytes] = []
                    while True:
                        line = self.rfile.readline().strip()
                        if not line:
                            break
                        try:
                            size = int(line.split(b";", 1)[0], 16)
                        except Exception:
                            size = 0
                        if size <= 0:
                            # consume trailer / final CRLF
                            try:
                                self.rfile.readline()
                            except Exception:
                                pass
                            break
                        chunks.append(self.rfile.read(size))
                        # consume CRLF
                        try:
                            self.rfile.read(2)
                        except Exception:
                            pass
                    return b"".join(chunks).decode("utf-8", errors="replace")

                try:
                    length = int(self.headers.get("Content-Length", "0") or "0")
                except Exception:
                    length = 0
                if length <= 0:
                    return ""
                try:
                    return self.rfile.read(length).decode("utf-8", errors="replace")
                except Exception:
                    return ""

            if path_only.startswith("/api/run"):
                raw = _read_body_text() or "{}"
                try:
                    obj = json.loads(raw)
                except Exception:
                    obj = {}
                action = str(obj.get("action", "") or "")
                params = obj.get("params") if isinstance(obj.get("params"), dict) else {}
                try:
                    # Most actions are executed asynchronously (jobs) so the
                    # browser UI can track progress. However, switch status
                    # reads/toggles are lightweight and the Live Dashboard
                    # expects an immediate result to update the "Schalter" pill.
                    if action in {"get_switch", "set_switch", "toggle_switch", "get_freeze", "set_freeze", "toggle_freeze"}:
                        if not self.dashboard.on_action:
                            payload = {"ok": False, "error": "Remote actions not available"}
                        else:
                            payload = self.dashboard.on_action(action, params)  # type: ignore[misc]
                    else:
                        payload = self.dashboard.submit_action(action, params)
                    body = json.dumps(payload).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Cache-Control", "no-store")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                except Exception as e:
                    body = json.dumps({"ok": False, "error": str(e)}).encode("utf-8")
                    self.send_response(500)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Cache-Control", "no-store")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

            if path_only.startswith("/api/set_window"):
                minutes: Optional[int] = None
                try:
                    parsed = urlparse(self.path)
                    qs = parse_qs(parsed.query or "")
                    if "minutes" in qs:
                        minutes = int(qs["minutes"][0])
                except Exception:
                    minutes = None

                if minutes is None:
                    try:
                        raw = _read_body_text() or "{}"
                        obj = json.loads(raw)
                        if isinstance(obj, dict) and "minutes" in obj:
                            minutes = int(obj["minutes"])
                    except Exception:
                        minutes = None

                if minutes is None:
                    body = json.dumps({"ok": False, "error": "missing minutes"}).encode("utf-8")
                    self.send_response(400)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Cache-Control", "no-store")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

                try:
                    minutes = self.dashboard.set_window_minutes(minutes)
                    payload = {"ok": True, "window_minutes": minutes}
                    body = json.dumps(payload).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Cache-Control", "no-store")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                except Exception as e:
                    body = json.dumps({"ok": False, "error": str(e)}).encode("utf-8")
                    self.send_response(500)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Cache-Control", "no-store")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

            self.send_response(404)
            self.end_headers()
        except Exception as e:
            msg = str(e)
            body = ("<!doctype html><html><body style='font-family:system-ui;padding:16px'>"                    "<h3>Web-Dashboard Fehler</h3>"                    "<pre style='white-space:pre-wrap'>" + msg + "</pre></body></html>").encode("utf-8")
            try:
                self.send_response(500)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                pass
            return

    def log_message(self, format: str, *args: Any) -> None:
        # Silence noisy default logging.
        return


class LiveWebDashboard:
    """Simple local HTTP server exposing a responsive live dashboard.

    - Starts on 0.0.0.0:<port> so phones on the same Wi‑Fi can access it.
    - Writes a copy of the HTML to `live_dashboard.html` in the working directory.
    """

    def __init__(
        self,
        store: LiveStateStore,
        *,
        port: int = 8765,
        refresh_seconds: float = 1.0,
        window_minutes: int = 10,
        devices: List[Tuple[str, str]],
        devices_meta: Optional[List[Dict[str, Any]]] = None,
        out_dir: Optional[Path] = None,
        available_windows: Optional[List[int]] = None,
        on_window_change: Optional[Callable[[int], None]] = None,
        on_action: Optional[Callable[[str, Dict[str, Any]], Dict[str, Any]]] = None,
        lang: str = "de",
    ) -> None:
        self.store = store
        self.port = int(port)
        self.refresh_seconds = float(refresh_seconds)
        self.window_minutes = int(window_minutes)
        self.token = ""  # auth disabled
        self.devices = list(devices or [])
        # Optional richer device metadata for the browser UI (e.g. kind="switch")
        self.devices_meta = list(devices_meta or [])
        self.out_dir = Path(out_dir) if out_dir else Path.cwd()

        # Recover device metadata (needed by /plots) from multiple possible roots.
        # The desktop app may be launched with an unexpected CWD (macOS Finder),
        # so we try both the provided out_dir, the current working directory,
        # and the project root inferred from this source file.
        self._devices_debug: List[str] = []

        def _candidate_roots() -> List[Path]:
            roots: List[Path] = []
            for r in [self.out_dir, Path.cwd(), _CODE_PROJECT_ROOT]:
                try:
                    rp = Path(r).resolve()
                    if rp not in roots:
                        roots.append(rp)
                except Exception:
                    continue
            return roots

        def _recover_meta() -> List[Dict[str, Any]]:
            roots = _candidate_roots()

            # 1) runtime devices file (written by the desktop app)
            for root in roots:
                p = (root / "data" / "runtime" / "devices_meta.json")
                try:
                    self._devices_debug.append(f"rt:{p}={'OK' if p.exists() else 'MISS'}")
                except Exception:
                    pass
                if p.exists():
                    meta = _load_devices_meta_file(p)
                    if meta:
                        return meta

            # 2) config.json (legacy fallback)
            for root in roots:
                p = (root / "config.json")
                try:
                    self._devices_debug.append(f"cfg:{p}={'OK' if p.exists() else 'MISS'}")
                except Exception:
                    pass
                if not p.exists():
                    continue
                try:
                    raw = p.read_text(encoding="utf-8")
                    obj = json.loads(raw) if raw else {}
                except Exception:
                    continue
                devs = obj.get("devices") if isinstance(obj, dict) else None
                if not isinstance(devs, list):
                    continue
                meta: List[Dict[str, Any]] = []
                for d in devs:
                    if not isinstance(d, dict):
                        continue
                    k = str(d.get("key") or "").strip()
                    if not k:
                        continue
                    n = str(d.get("name") or k).strip() or k
                    kind = str(d.get("kind") or "").strip()
                    try:
                        phases = int(d.get("phases") or 3)
                    except Exception:
                        phases = 3
                    meta.append({"key": k, "name": n, "kind": kind, "phases": phases})
                if meta:
                    return meta

            return []

        # If the caller passed only (key,name) pairs (or nothing), try to enrich/recover metadata.
        if not self.devices_meta:
            meta = _recover_meta()
            if meta:
                self.devices_meta = meta
        if (not self.devices) and self.devices_meta:
            self.devices = [(m["key"], m["name"]) for m in self.devices_meta]

        self.available_windows = (
            [int(x) for x in (available_windows or [5, 10, 15, 30, 60, 120]) if int(x) > 0]
        )
        if int(self.window_minutes) not in self.available_windows:
            self.available_windows.append(int(self.window_minutes))
            self.available_windows = sorted(set(self.available_windows))
        self.on_window_change = on_window_change
        self.on_action = on_action
        # In-memory job store for remote actions (progress + results)
        self._jobs_lock = threading.Lock()
        self._jobs_by_id: Dict[int, Dict[str, Any]] = {}
        self._jobs_order: List[int] = []  # newest last
        self._job_seq = 0

        self._thread: Optional[threading.Thread] = None
        self._httpd: Optional[HTTPServer] = None

        self.lang = normalize_lang(lang)
        # Use the full i18n map (de -> en -> lang). The /plots page needs
        # common.* and btn.* keys as well, not only web.*.
        web_i18n = get_lang_map(self.lang)

        html = _render_template(
            _HTML_TEMPLATE,
            {
                "lang": self.lang,
                "web_live_title": _t(self.lang, "web.live.title"),
                "web_live_meta": _t(self.lang, "web.live.meta"),
                "web_nav_live": _t(self.lang, "web.nav.live"),
                "web_nav_control": _t(self.lang, "web.nav.control"),
                "web_pill_window": _t(self.lang, "web.pill.window"),
                "web_pill_url": _t(self.lang, "web.pill.url"),
                "refresh_ms": str(int(max(250, self.refresh_seconds * 1000))),
                "window_min": str(int(max(1, self.window_minutes))),
                "window_options_json": json.dumps(self.available_windows),
                "devices_json": json.dumps(
                    (self.devices_meta or [{"key": k, "name": n} for (k, n) in (self.devices or [])])
                ),
                "i18n_json": json.dumps(web_i18n),
            },
        )
        self._html_bytes = html.encode("utf-8")

        self._control_bytes = _render_template(
            _CONTROL_TEMPLATE,
            {
                "lang": self.lang,
                "web_nav_live": _t(self.lang, "web.nav.live"),
                "web_nav_control": _t(self.lang, "web.nav.control"),
                "web_control_title": _t(self.lang, "web.control.title"),
                "web_control_meta": _t(self.lang, "web.control.meta"),
                "web_control_sync": _t(self.lang, "web.control.sync"),
                "web_control_plots": _t(self.lang, "web.control.plots"),
                "web_control_open_plotly": _t(self.lang, "web.control.open_plotly"),
                "web_control_export": _t(self.lang, "web.control.export"),
                "web_control_jobs": _t(self.lang, "web.control.jobs"),
                "web_control_mode": _t(self.lang, "web.control.mode"),
                "web_control_start": _t(self.lang, "web.control.start"),
                "web_control_btn_sync": _t(self.lang, "web.control.btn.sync"),
                "web_control_plots_from": _t(self.lang, "web.control.plots.from"),
                "web_control_plots_to": _t(self.lang, "web.control.plots.to"),
                "web_control_btn_plots": _t(self.lang, "web.control.btn.plots"),
                "web_control_invoice": _t(self.lang, "web.control.invoice"),
                "web_control_anchor": _t(self.lang, "web.control.anchor"),
                "web_control_custom_note": _t(self.lang, "web.control.custom_note"),
                # Backwards compat: the UI button id is "btn_summary", but the translation key is "web.control.btn.pdf".
                "web_control_btn_summary": _t(self.lang, "web.control.btn.pdf"),
                "web_control_btn_invoices": _t(self.lang, "web.control.btn.invoices"),
                "web_control_btn_bundle": _t(self.lang, "web.control.btn.bundle"),
                "web_control_btn_report_day": _t(self.lang, "web.control.btn.report_day"),
                "web_control_btn_report_month": _t(self.lang, "web.control.btn.report_month"),
                "web_control_jobs_meta": _t(self.lang, "web.control.jobs.meta"),
            },
        ).encode("utf-8")

        # Plotly plots page (used by desktop Plots tab)
        # Build device pills as server-rendered HTML so the page stays usable even if JS fails.
        _devs = (self.devices_meta or [{"key": k, "name": n} for (k, n) in (self.devices or [])])
        _parts = []
        for d in _devs:
            try:
                k = html.escape(str(d.get("key", "") or ""))
                n = html.escape(str(d.get("name", "") or k))
            except Exception:
                continue
            if not k:
                continue
            _parts.append(f'<label class="devchip"><input type="checkbox" value="{k}"/><span>{n}</span></label>')
        devices_html = '\n'.join(_parts)
        if not devices_html.strip():
            dbg = ''
            try:
                dbg = ' | '.join([str(x) for x in (getattr(self, '_devices_debug', []) or [])][-12:])
            except Exception:
                dbg = ''
            devices_html = f"<div class='hint'>{_t(self.lang, 'web.plots.no_devices')}</div>" + (f"<!-- {html.escape(dbg)} -->" if dbg else '')


        # Plotly page uses @@tokens@@ to avoid brace-unescape issues.
        self._plots_bytes = _render_template_tokens(
            _PLOTS_TEMPLATE,
            {
                "lang": self.lang,
                "plots_title": _t(self.lang, "web.plots.title"),
                "web_nav_live": _t(self.lang, "web.nav.live"),
                "web_nav_control": _t(self.lang, "web.nav.control"),
                "web_btn_theme": _t(self.lang, "web.btn.theme"),
                # Server-render control labels so the page stays usable even if JS fails.
                "lbl_view": _t(self.lang, "web.plots.view"),
                "lbl_metric": _t(self.lang, "web.plots.metric"),
                "lbl_series": _t(self.lang, "web.plots.series"),
                "lbl_smooth": _t(self.lang, "web.plots.filter.smooth"),
                "lbl_deadband": _t(self.lang, "web.plots.filter.deadband"),
                "lbl_signhold": _t(self.lang, "web.plots.filter.signhold"),
                "lbl_mode": _t(self.lang, "web.plots.kwh_mode"),
                "lbl_range": _t(self.lang, "web.plots.range"),
                "lbl_from": _t(self.lang, "common.from"),
                "lbl_to": _t(self.lang, "common.to"),
                "lbl_devices": _t(self.lang, "web.plots.devices"),
                "hint_max2": _t(self.lang, "web.plots.max2"),
                "btn_apply": _t(self.lang, "btn.apply"),
                "devices_html": devices_html,
                "i18n_json": json.dumps(web_i18n),
                "devices_json": json.dumps(
                    (self.devices_meta or [{"key": k, "name": n} for (k, n) in (self.devices or [])])
                ),
            },
        ).encode("utf-8")

    def read_file_bytes(self, rel_path: str) -> Tuple[bytes, str]:
        """Serve files from <project>/exports only.

        `rel_path` is the part after /files/.
        """
        rel_path = (rel_path or "").lstrip("/")
        # Only allow under exports/ (or a subfolder inside it)
        root = (self.out_dir / "exports").resolve()
        p = (root / rel_path).resolve()
        if root not in p.parents and p != root:
            raise FileNotFoundError(rel_path)
        if not p.exists() or not p.is_file():
            raise FileNotFoundError(rel_path)
        data = p.read_bytes()
        ext = p.suffix.lower()
        ctype = {
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".pdf": "application/pdf",
            ".zip": "application/zip",
            ".json": "application/json; charset=utf-8",
            ".txt": "text/plain; charset=utf-8",
            ".csv": "text/csv; charset=utf-8",
        }.get(ext, "application/octet-stream")
        return data, ctype

    @property
    def control_html_bytes(self) -> bytes:
        return self._control_bytes

    @property
    def plots_html_bytes(self) -> bytes:
        return getattr(self, "_plots_bytes", b"")

    def get_jobs(self) -> Dict[str, Any]:
        return {"jobs": self.list_jobs()}

    def get_job(self, job_id: int) -> Dict[str, Any]:
        with self._jobs_lock:
            j = self._jobs_by_id.get(int(job_id))
            return {"job": j} if j else {"job": None}

    def read_file_bytes(self, rel_path: str) -> Tuple[bytes, str]:
        """Read a file below out_dir/exports and return (bytes, content_type)."""
        rel = str(rel_path).lstrip("/")
        # Only allow files under exports/
        root = (self.out_dir / "exports").resolve()
        path = (root / rel).resolve()
        if root not in path.parents and path != root:
            raise FileNotFoundError(rel)
        if not path.exists() or not path.is_file():
            raise FileNotFoundError(rel)
        data = path.read_bytes()
        ext = path.suffix.lower()
        ctype = "application/octet-stream"
        if ext in {".png"}:
            ctype = "image/png"
        elif ext in {".jpg", ".jpeg"}:
            ctype = "image/jpeg"
        elif ext in {".pdf"}:
            ctype = "application/pdf"
        elif ext in {".zip"}:
            ctype = "application/zip"
        elif ext in {".json"}:
            ctype = "application/json; charset=utf-8"
        elif ext in {".txt", ".log"}:
            ctype = "text/plain; charset=utf-8"
        return data, ctype

    
    def _read_analyzer_heartbeat(self) -> tuple[bool, int | None]:
        """Return (running, ts) based on a heartbeat file written by the desktop app.

        If the heartbeat is older than ~20s we consider the analyzer OFF.
        """
        hb = (self.out_dir / 'data' / 'runtime' / 'analyzer_heartbeat.json')
        try:
            raw = hb.read_text(encoding='utf-8')
            obj = json.loads(raw) if raw else {}
            ts = int(obj.get('ts') or 0)
            if ts <= 0:
                return (False, None)
            running = (int(time.time()) - ts) <= 20
            return (bool(running), ts)
        except Exception:
            return (False, None)

    def get_config(self) -> Dict[str, Any]:
        running, hb_ts = self._read_analyzer_heartbeat()
        # NOTE: This endpoint is used by multiple pages (Live + Plots).
        # Keep existing fields stable and add optional metadata.
        devices_meta = self.devices_meta or [{"key": k, "name": n} for (k, n) in (self.devices or [])]
        return {
            "window_minutes": int(self.window_minutes),
            "refresh_seconds": float(self.refresh_seconds),
            "available_windows": list(self.available_windows),
            "analyzer_running": bool(running),
            "analyzer_heartbeat_ts": hb_ts,
            # For /plots device picker
            "devices_meta": devices_meta,
            "lang": self.lang,
        }

    def set_window_minutes(self, minutes: int) -> int:
        minutes = int(minutes)
        if minutes <= 0:
            raise ValueError("minutes must be > 0")
        # accept values not in the preset list (but keep the list for the UI)
        if minutes not in self.available_windows:
            self.available_windows.append(minutes)
            self.available_windows = sorted(set(self.available_windows))

        self.window_minutes = minutes

        # Make sure the store keeps enough points even for fast polling (down to ~0.5s).
        # This is conservative; the browser will still filter to the selected time window.
        approx_points = int(minutes * 60 * 2) + 100
        try:
            self.store.set_max_points(max(self.store.max_points, approx_points))
        except Exception:
            pass

        if self.on_window_change:
            try:
                self.on_window_change(minutes)
            except Exception:
                pass
        return minutes

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return

        # Write a copy for convenience.
        try:
            p = self.out_dir / "live_dashboard.html"
            p.write_bytes(self._html_bytes)
        except Exception:
            pass

        handler = type("LiveDashHandler", (_Handler,), {})
        handler.store = self.store
        handler.html_bytes = self._html_bytes
        handler.control_bytes = self._control_bytes
        handler.dashboard = self

        # Bind server. If the configured port is already in use, try the next ones.
        last_err: Optional[Exception] = None
        for p in range(int(self.port), int(self.port) + 20):
            try:
                self._httpd = QuietHTTPServer(("0.0.0.0", int(p)), handler)
                self.port = int(p)
                last_err = None
                break
            except OSError as e:
                last_err = e
                continue
        if self._httpd is None:
            raise last_err or OSError("could not bind web dashboard")

        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        try:
            if self._httpd:
                self._httpd.shutdown()
        except Exception:
            pass
        self._httpd = None
        self._thread = None

    def url(self) -> str:
        ip = _local_ip_guess()
        return f"http://{ip}:{self.port}/"

    def _check_token(self, handler: BaseHTTPRequestHandler) -> bool:
        """Authorization disabled (LAN-only)."""
        return True

    def _upsert_job(self, job: Dict[str, Any]) -> None:
        jid = int(job.get("id") or 0)
        if jid <= 0:
            return
        with self._jobs_lock:
            if jid not in self._jobs_by_id:
                self._jobs_order.append(jid)
            self._jobs_by_id[jid] = job
            # keep last 200 jobs
            if len(self._jobs_order) > 200:
                drop = self._jobs_order[: len(self._jobs_order) - 200]
                self._jobs_order = self._jobs_order[len(self._jobs_order) - 200 :]
                for d in drop:
                    self._jobs_by_id.pop(d, None)

    def update_job(self, job_id: int, **fields: Any) -> None:
        with self._jobs_lock:
            j = self._jobs_by_id.get(int(job_id))
            if not j:
                return
            j2 = {**j, **fields}
            self._jobs_by_id[int(job_id)] = j2

    def update_progress(self, job_id: int, device_key: str, done: int, total: int, message: str = "") -> None:
        device_key = str(device_key or "")
        done = int(max(0, done))
        total = int(max(1, total))
        pct = int(round(100 * (done / total)))
        with self._jobs_lock:
            j = self._jobs_by_id.get(int(job_id))
            if not j:
                return
            prog = dict(j.get("progress") or {})
            prog[device_key] = {"done": done, "total": total, "percent": pct, "message": str(message or "")}
            # compute overall percent as average over devices that have progress
            vals = [int(v.get("percent") or 0) for v in prog.values() if isinstance(v, dict)]
            overall = int(round(sum(vals) / len(vals))) if vals else 0
            self._jobs_by_id[int(job_id)] = {**j, "progress": prog, "progress_overall": overall}

    def list_jobs(self) -> List[Dict[str, Any]]:
        with self._jobs_lock:
            arr = [self._jobs_by_id[jid] for jid in self._jobs_order if jid in self._jobs_by_id]
            return list(reversed(arr))

    def submit_action(self, action: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Run an action asynchronously and return job metadata."""
        if not self.on_action:
            raise RuntimeError("Remote actions not available")
        with self._jobs_lock:
            self._job_seq += 1
            job_id = self._job_seq
        job: Dict[str, Any] = {
            "id": job_id,
            "action": action,
            "params": params,
            "status": "running",
            "started_at": int(time.time()),
            "progress_overall": 0,
        }
        self._upsert_job(job)

        def progress_cb(device_key: str, done: int, total: int, message: str = "") -> None:
            self.update_progress(job_id, device_key=device_key, done=done, total=total, message=message)

        def runner() -> None:
            try:
                # Allow optional progress callback if the action handler supports it.
                res: Dict[str, Any]
                if self.on_action is None:
                    raise RuntimeError("Remote actions not available")
                try:
                    sig = inspect.signature(self.on_action)
                    if len(sig.parameters) >= 3:
                        res = self.on_action(action, params, progress_cb)  # type: ignore[misc]
                    else:
                        res = self.on_action(action, params)
                except (ValueError, TypeError):
                    res = self.on_action(action, params)  # type: ignore[misc]

                job2 = {**job, "status": "done", "ended_at": int(time.time()), "result": res, "progress_overall": 100}
                self._upsert_job(job2)
            except Exception as e:
                job2 = {**job, "status": "error", "ended_at": int(time.time()), "error": str(e)}
                self._upsert_job(job2)

        threading.Thread(target=runner, daemon=True).start()
        return {"ok": True, "job": job}


def _escape(s: str) -> str:
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )
