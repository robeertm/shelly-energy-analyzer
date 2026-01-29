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
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs

from shelly_analyzer.i18n import get_lang_map, normalize_lang, t as _t

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
    q_total_var: float = 0.0
    qa: float = 0.0
    qb: float = 0.0
    qc: float = 0.0
    cosphi_total: float = 0.0
    pfa: float = 0.0
    pfb: float = 0.0
    pfc: float = 0.0
    kwh_today: float = 0.0


class LiveStateStore:
    """Thread-safe in-memory store for the web dashboard."""

    def __init__(self, max_points: int = 900) -> None:
        self.max_points = int(max_points)
        self._lock = threading.Lock()
        self._by_device: Dict[str, List[LivePoint]] = {}

    def set_max_points(self, max_points: int) -> None:
        """Adjust the in-memory retention size.

        This is used when the live window changes (e.g. from the mobile dashboard).
        """
        max_points = int(max(50, max_points))
        with self._lock:
            self.max_points = max_points
            for k, arr in self._by_device.items():
                if len(arr) > self.max_points:
                    del arr[: len(arr) - self.max_points]


    def update(self, device_key: str, point: LivePoint) -> None:
        with self._lock:
            arr = self._by_device.setdefault(device_key, [])
            arr.append(point)
            if len(arr) > self.max_points:
                del arr[: len(arr) - self.max_points]

    def snapshot(self) -> Dict[str, List[Dict[str, Any]]]:
        with self._lock:
            out: Dict[str, List[Dict[str, Any]]] = {}
            for k, arr in self._by_device.items():
                out[k] = [
                    {
                        "ts": p.ts,
                        # Ensure JSON-safe floats (no NaN/Inf) because browsers reject them.
                        "power_total_w": (p.power_total_w if math.isfinite(float(p.power_total_w)) else 0.0),
                        "va": (p.va if math.isfinite(float(p.va)) else 0.0),
                        "vb": (p.vb if math.isfinite(float(p.vb)) else 0.0),
                        "vc": (p.vc if math.isfinite(float(p.vc)) else 0.0),
                        "ia": (p.ia if math.isfinite(float(p.ia)) else 0.0),
                        "ib": (p.ib if math.isfinite(float(p.ib)) else 0.0),
                        "ic": (p.ic if math.isfinite(float(p.ic)) else 0.0),
                        "q_total_var": (p.q_total_var if math.isfinite(float(p.q_total_var)) else 0.0),
                        "qa": (p.qa if math.isfinite(float(p.qa)) else 0.0),
                        "qb": (p.qb if math.isfinite(float(p.qb)) else 0.0),
                        "qc": (p.qc if math.isfinite(float(p.qc)) else 0.0),
                        "cosphi_total": (p.cosphi_total if math.isfinite(float(p.cosphi_total)) else 0.0),
                        "pfa": (p.pfa if math.isfinite(float(p.pfa)) else 0.0),
                        "pfb": (p.pfb if math.isfinite(float(p.pfb)) else 0.0),
                        "pfc": (p.pfc if math.isfinite(float(p.pfc)) else 0.0),
                        "kwh_today": (p.kwh_today if math.isfinite(float(p.kwh_today)) else 0.0),
                    }
                    for p in arr
                ]
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
  <title>{web_live_title}</title>
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

      /* Canvas chart colors (live page) */
      --plot-grid: rgba(17,24,39,0.10);
      --plot-tick: rgba(17,24,39,0.70);
      --plot-title: rgba(17,24,39,0.90);
      --plot-wait: rgba(17,24,39,0.55);

      --plot-line1: rgba(37,99,235,0.92);
      --plot-line2: rgba(217,119,6,0.92);
      --plot-line3: rgba(22,163,74,0.92);
    }}
    :root[data-theme="dark"] {{
      --bg: #0b0f14;
      --card: #121821;
      --fg: #e8eef6;
      --muted: #9fb0c3;
      --accent: #6aa7ff;
      --border: rgba(255,255,255,0.08);
      --chipbg: rgba(255,255,255,0.02);

      /* Canvas chart colors (live page) */
      --plot-grid: rgba(255,255,255,0.09);
      --plot-tick: rgba(255,255,255,0.55);
      --plot-title: rgba(255,255,255,0.85);
      --plot-wait: rgba(255,255,255,0.55);

      --plot-line1: rgba(106,167,255,0.90);
      --plot-line2: rgba(255,180,84,0.90);
      --plot-line3: rgba(136,240,179,0.90);
    }}
    html, body {{
      height: 100%;
      margin: 0;
      background: var(--bg);
      color: var(--fg);
      font-family: -apple-system, system-ui, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
    }}
    .wrap {{
      padding: 10px;
      box-sizing: border-box;
    }}
    .topbar {{
      display: flex;
      align-items: center;
      justify-content: flex-start;
      gap: 10px;
      margin: 6px 2px 10px;
      flex-wrap: wrap;
    }}
    .left {{
      min-width: 0;
    }}
    .right {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      justify-content: flex-start;
      align-items: center;
    }}
    a.navlink {{
      color: var(--muted);
      text-decoration: none;
      font-size: 12px;
      padding: 6px 10px;
      border-radius: 999px;
      border: 1px solid var(--border);
      background: var(--chipbg);
    }}
    a.navlink.active {{
      color: var(--fg);
      border-color: rgba(106,167,255,0.35);
    }}
    .urlpill span {{
      display: inline-block;
      max-width: min(55vw, 340px);
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      vertical-align: bottom;
    }}
    select {{
      font-size: 13px;
      padding: 6px 8px;
      border-radius: 10px;
      border: 1px solid var(--border);
      background: var(--card);
      color: var(--fg);
      min-width: 76px;
    }}
    @media (max-width: 420px) {{
      .urlpill span {{
        max-width: 80vw;
      }}
    }}
    .title {{
      font-size: 16px;
      font-weight: 650;
      letter-spacing: 0.2px;
    }}
    .meta {{
      font-size: 12px;
      color: var(--muted);
    }}
    .grid {{
      display: grid;
      grid-template-columns: 1fr;
      gap: 10px;
    }}
    @media (min-width: 900px) {{
      .grid {{
        grid-template-columns: 1fr 1fr;
      }}
    }}
    .card {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 10px;
      box-sizing: border-box;
      min-width: 0;
    }}
    .card h2 {{
      margin: 0 0 8px;
      font-size: 14px;
      font-weight: 650;
      color: var(--fg);
    }}
    .row {{
      display: grid;
      grid-template-columns: 1fr;
      gap: 10px;
    }}
    canvas {{
      width: 100%;
      height: 160px;
      display: block;
      background: var(--chipbg);
      border: 1px solid var(--border);
      border-radius: 12px;
    }}
    @media (orientation: landscape) and (max-width: 900px) {{
      canvas { height: 130px; }
    }}
    /* Compact mode: sparklines (power + current) */
    :root[data-compact="1"] .kv { display: none; }
    :root[data-compact="1"] canvas { height: 92px; }
    :root[data-compact="1"] canvas.voltagePlot { display: none; }
    :root[data-compact="1"] .row { gap: 8px; }
    .kv {{
      display: grid;
      grid-template-columns: auto 1fr;
      gap: 6px 10px;
      font-size: 12px;
      color: var(--muted);
      margin: 8px 2px 2px;
    }}
    .kv b {{
      color: var(--fg);
      font-weight: 650;
    }}
    .pill {{
      display: inline-flex;
      gap: 8px;
      align-items: center;
      padding: 6px 10px;
      border-radius: 999px;
      border: 1px solid var(--border);
      background: var(--chipbg);
      font-size: 12px;
      color: var(--muted);
      white-space: nowrap;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="topbar">
      <div class="left">
        <div class="title">{web_live_title}</div>
        <div class="meta">{web_live_meta} <span id="refresh_s"></span>s • <span id="auto_state">–</span> • <span id="stamp">–</span></div>
      </div>
      <div class="right">
        <a class="navlink active" id="nav_live" href="/">{web_nav_live}</a>
        <a class="navlink" id="nav_control" href="/control">{web_nav_control}</a>
        <a class="navlink" id="btn_theme" href="#">🌓</a>
        <a class="navlink" id="btn_auto" href="#">⏸</a>
        <a class="navlink" id="btn_refresh" href="#">↻</a>
        <a class="navlink" id="btn_compact" href="#">▦</a>
        <div class="pill">{web_pill_window}: <select id="win_sel"></select> min</div>
        <div class="pill">shelly_analyzer: <b id="analyzer_state">–</b></div>
        <div class="pill urlpill">{web_pill_url}: <span id="url"></span></div>
      </div>
    </div>


    <div class="grid" id="grid"></div>
  </div>

<script>
const REFRESH_MS = {refresh_ms};
const WINDOW_MIN = {window_min};
const WINDOW_OPTIONS = {window_options_json};
const DEVICES = {devices_json};
const I18N = {i18n_json};
function t(k){ return (I18N && I18N[k]) ? I18N[k] : k; }

let windowMin = WINDOW_MIN;
let pendingWindow = null;

document.getElementById("refresh_s").textContent = (REFRESH_MS/1000).toFixed(1).replace(/\\.0$/, "");
function qs() { return ""; }
document.getElementById("url").textContent = window.location.origin + "/";

// Navigation links are simple (no auth/token)


// Theme + Auto-Refresh (persisted)
const LS_THEME = "sea_web_theme";
const LS_AUTO  = "sea_web_autorefresh";
const LS_COMPACT = "sea_web_compact";

let autoRefresh = (localStorage.getItem(LS_AUTO) ?? "1") !== "0";
let frozen = false;
let compactMode = (localStorage.getItem(LS_COMPACT) ?? "0") === "1";

function setTheme(theme) {
  const t0 = (theme === "dark") ? "dark" : "light";
  document.documentElement.dataset.theme = t0;
  localStorage.setItem(LS_THEME, t0);
  updateThemeBtn();
  // Redraw charts with theme-appropriate colors immediately.
  try {
    if (window.__SEA_LAST_STATE) renderState(window.__SEA_LAST_STATE);
  } catch (e) {}
}

function updateThemeBtn() {
  const el = document.getElementById("btn_theme");
  if (!el) return;
  const theme = document.documentElement.dataset.theme || "dark";
  el.textContent = (theme === "dark") ? "🌙" : "☀️";
  el.title = (theme === "dark") ? (t('web.theme.dark') || 'Dark') : (t('web.theme.light') || 'Light');
}

function updateAutoBtn() {
  const el = document.getElementById("btn_auto");
  const st = document.getElementById("auto_state");
  if (el) {
    // This button controls the app's Freeze state.
    // Not frozen => show pause icon; frozen => show play icon.
    if (frozen) el.classList.add('active'); else el.classList.remove('active');
    el.textContent = frozen ? '▶' : '⏸';
    el.title = frozen ? (t('web.freeze.off') || 'Freeze aus (weiter)') : (t('web.freeze.on') || 'Freeze an (pause)');
  }
  if (st) st.textContent = frozen ? (t('web.freeze') || 'Freeze') : (autoRefresh ? 'Auto' : 'Manual');
}

function updateCompactBtn() {
  const el = document.getElementById("btn_compact");
  if (!el) return;
  if (compactMode) el.classList.add('active'); else el.classList.remove('active');
  el.title = compactMode ? (t('web.compact.on') || "Compact on") : (t('web.compact.off') || "Compact off");
}

function applyCompact(redraw=true) {
  document.documentElement.dataset.compact = compactMode ? "1" : "0";
  try { localStorage.setItem(LS_COMPACT, compactMode ? "1" : "0"); } catch (e) {}
  updateCompactBtn();
  if (redraw) {
    try { if (window.__SEA_LAST_STATE) renderState(window.__SEA_LAST_STATE); } catch (e) {}
  }
}

// Init theme
(function initTheme(){
  let theme = localStorage.getItem(LS_THEME);
  if (!theme) {
    theme = (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) ? 'dark' : 'light';
  }
  document.documentElement.dataset.theme = theme;
  updateThemeBtn();
})();

// Init compact mode
(function initCompact(){
  try { compactMode = (localStorage.getItem(LS_COMPACT) ?? "0") === "1"; } catch (e) { compactMode = false; }
  document.documentElement.dataset.compact = compactMode ? "1" : "0";
  updateCompactBtn();
})();

updateAutoBtn();

// Init freeze state from the app (so the web pause button matches the UI)
(async function initFreezeState(){
  try {
    const r = await apiRun('get_freeze', {});
    if (r && r.ok) {
      frozen = !!r.freeze;
      // If app is frozen, ensure web auto-refresh is paused too.
      autoRefresh = autoRefresh && !frozen;
      updateAutoBtn();
    }
  } catch (e) {
    // ignore
  }
})();

// Keep freeze state in sync if Freeze is toggled in the desktop app.
// Requirement: no matter where Freeze is pressed (web or app), both must stop/resume.
let _freezePrevAuto = null;
async function syncFreezeState() {
  try {
    const r = await apiRun('get_freeze', {});
    if (!(r && r.ok)) return;
    const newFrozen = !!r.freeze;
    if (newFrozen === frozen) return;

    // External change detected (likely via desktop UI)
    if (newFrozen) {
      _freezePrevAuto = autoRefresh;
      frozen = true;
      autoRefresh = false;
    } else {
      frozen = false;
      if (_freezePrevAuto !== null) {
        autoRefresh = !!_freezePrevAuto;
        _freezePrevAuto = null;
      }
    }

    try { localStorage.setItem(LS_AUTO, autoRefresh ? "1" : "0"); } catch (e) {}
    updateAutoBtn();
    if (!frozen && autoRefresh) tick(true);
  } catch (e) {
    // ignore
  }
}

setInterval(syncFreezeState, 1000);

// Button handlers
(function bindTopbarButtons(){
  const bt = document.getElementById('btn_theme');
  if (bt) bt.addEventListener('click', (ev)=>{ ev.preventDefault(); const cur = document.documentElement.dataset.theme||'dark'; setTheme(cur==='dark'?'light':'dark'); });
  const ba = document.getElementById('btn_auto');
  if (ba) ba.addEventListener('click', async (ev)=>{
    ev.preventDefault();
    try {
      const r = await apiRun('toggle_freeze', {});
      if (r && r.ok) {
        frozen = !!r.freeze;
        // When frozen, we also pause web auto-refresh.
        autoRefresh = !frozen;
        try { localStorage.setItem(LS_AUTO, autoRefresh ? "1" : "0"); } catch (e) {}
        updateAutoBtn();
        if (autoRefresh) tick(true);
      }
    } catch (e) {
      // ignore
    }
  });
  const br = document.getElementById('btn_refresh');
  if (br) br.addEventListener('click', (ev)=>{ ev.preventDefault(); tick(true); });
  const bc = document.getElementById('btn_compact');
  if (bc) bc.addEventListener('click', (ev)=>{ ev.preventDefault(); compactMode = !compactMode; applyCompact(true); });
})();

function escapeHtml(s) {
  return String(s || "").replace(/[&<>"']/g, c => ({
    '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;'
  }[c]));
}

function makeId(key) {
  return String(key || "dev").replace(/[^a-zA-Z0-9_-]/g, "_");
}

const gridEl = document.getElementById("grid");
const UI = {}; // device_key -> {p,v,c,kv,swState,swBtn}

async function apiRun(action, params) {
  const r = await fetch("/api/run" + qs(), {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    // NOTE: this template is rendered via Python .format(), so braces must be escaped.
    // The extra '}}}}' here is intentional to survive formatting and produce valid JS:
    //   JSON.stringify({action: ..., params: params||{}})
    body: JSON.stringify({action: String(action||""), params: params||{}}}})
  });
  return await r.json();
}

function parseSwitchOn(res) {
  try {
    if (!res) return null;
    const cand = [
      res.on, res.is_on, res.output,
      res.state, res.status,
      res.result && res.result.on,
      res.result && res.result.is_on,
      res.result && res.result.output,
      res.result && res.result.state,
      res.result && res.result.status,
    ];
    for (const v of cand) {
      if (v === undefined || v === null) continue;
      if (typeof v === 'boolean') return v;
      const s = String(v).toLowerCase();
      if (s === 'on' || s === 'true' || s === '1' || s === 'enabled') return true;
      if (s === 'off' || s === 'false' || s === '0' || s === 'disabled') return false;
      const n = Number(v);
      if (!isNaN(n)) return n !== 0;
    }
  } catch (e) {}
  return null;
}

function buildCards() {
  if (!gridEl) return;
  gridEl.innerHTML = "";
  (DEVICES || []).forEach(dev => {
    const id = makeId(dev.key);
    const isSwitch = String(dev.kind || "").toLowerCase() === "switch";
    const card = document.createElement("div");
    card.className = "card";
    card.innerHTML = `
      <h2>${escapeHtml(dev.name || dev.key)}</h2>
      ${isSwitch ? `<div class="row" style="margin:6px 0 8px;gap:10px;">
        <div class="pill">${escapeHtml(t('web.switch'))}: <b id="sw_${id}">–</b></div>
        <button id="swbtn_${id}" type="button">${escapeHtml(t('web.switch.toggle'))}</button>
      </div>` : ``}
      <div class="row">
        <canvas id="p_${id}" class="powerPlot"></canvas>
        <canvas id="v_${id}" class="voltagePlot"></canvas>
        <canvas id="c_${id}" class="currentPlot"></canvas>
      </div>
      <div class="kv" id="kv_${id}"></div>
    `;
    gridEl.appendChild(card);
    UI[dev.key] = {
      p: document.getElementById(`p_${id}`),
      v: document.getElementById(`v_${id}`),
      c: document.getElementById(`c_${id}`),
      kv: document.getElementById(`kv_${id}`),
      swState: document.getElementById(`sw_${id}`),
      swBtn: document.getElementById(`swbtn_${id}`),
    };

    // Wire switch toggle (if present)
    if (isSwitch) {
      const btn = UI[dev.key].swBtn;
      if (btn) {
        btn.addEventListener('click', async () => {
          try {
            btn.disabled = true;
            const res = await apiRun('toggle_switch', {device_key: dev.key});
            if (res && res.ok && UI[dev.key].swState) {
              const on = parseSwitchOn(res);
              if (on !== null) UI[dev.key].swState.textContent = on ? t('web.switch.on') : t('web.switch.off');
              UI[dev.key]._swLastFetch = Date.now();
            }
          } catch (e) {
            // ignore
          } finally {
            btn.disabled = false;
          }
        });
      }
    }
  });
}

buildCards();

const winSel = document.getElementById("win_sel");

function renderWindowOptions(opts, selected) {{
  if (!winSel) return;
  winSel.innerHTML = "";
  const uniq = Array.from(new Set(opts.map(x=>parseInt(x,10)).filter(x=>x>0))).sort((a,b)=>a-b);
  uniq.forEach(m => {{
    const o = document.createElement("option");
    o.value = String(m);
    o.textContent = String(m);
    if (m === selected) o.selected = true;
    winSel.appendChild(o);
  }});
}}

async function syncConfig() {{
  try {{
    const r = await fetch("/api/config" + qs(), {{cache:"no-store"}});
    const cfg = await r.json();
    // If the user just changed the window, keep the UI stable until the server confirms.
    if (pendingWindow !== null) {{
      const sm = parseInt((cfg && cfg.window_minutes) ? cfg.window_minutes : pendingWindow, 10);
      if (!isNaN(sm) && sm === pendingWindow) {{
        windowMin = pendingWindow;
        pendingWindow = null;
      }}
    }}
    if (pendingWindow === null && cfg && cfg.window_minutes) {{
      const m = parseInt(cfg.window_minutes, 10);
      if (!isNaN(m) && m>0) windowMin = m;
    }}
    const opts = (cfg && cfg.available_windows) ? cfg.available_windows : WINDOW_OPTIONS;
    renderWindowOptions(opts, pendingWindow !== null ? pendingWindow : windowMin);
    // Analyzer status (heartbeat from desktop app)
    try {
      const el = document.getElementById("analyzer_state");
      if (el && cfg) {
        const on = !!cfg.analyzer_running;
        el.textContent = on ? t('web.switch.on') : t('web.switch.off');
      }
    } catch (e) {}
  } catch (e) {
    renderWindowOptions(WINDOW_OPTIONS, windowMin);
    try {
      const el = document.getElementById("analyzer_state");
      if (el) el.textContent = t('web.switch.off');
    } catch (e2) {}
  }
}

if (winSel) {{
  winSel.addEventListener("change", async () => {{
    const m = parseInt(winSel.value, 10);
    if (!m || isNaN(m)) return;
    // Optimistically apply locally so the UI doesn't "jump back" while the request is in-flight.
    windowMin = m;
    pendingWindow = m;
    try {{
      await fetch("/api/set_window" + qs(), {{
        method: "POST",
        headers: {{"Content-Type":"application/json"}},
        body: JSON.stringify({{minutes: m}})
      }});
    }} catch (e) {{}}
    await syncConfig();
  }});
}}

// initial render + periodic sync for bidirectional updates
syncConfig();
setInterval(syncConfig, Math.max(2000, REFRESH_MS*3));

function fmt(n, d=1) {{
  if (n === null || n === undefined || isNaN(n)) return "–";
  return Number(n).toFixed(d);
}}

function cssVar(name, fallback) {{
  try {{
    const v = getComputedStyle(document.documentElement).getPropertyValue(name);
    const s = (v || "").trim();
    return s || fallback;
  }} catch (e) {{
    return fallback;
  }}
}}

function palettePower() {{
  return [cssVar('--plot-line1', 'rgba(37,99,235,0.92)')];
}}
function palette3() {{
  return [
    cssVar('--plot-line1', 'rgba(37,99,235,0.92)'),
    cssVar('--plot-line2', 'rgba(217,119,6,0.92)'),
    cssVar('--plot-line3', 'rgba(22,163,74,0.92)')
  ];
}}

function drawLineChart(canvas, tsMs, seriesList, yLabel, colors) {{
  const ctx = canvas.getContext("2d");
  const w = canvas.width  = canvas.clientWidth  * devicePixelRatio;
  const h = canvas.height = canvas.clientHeight * devicePixelRatio;
  ctx.clearRect(0, 0, w, h);

  const colGrid  = cssVar('--plot-grid',  'rgba(17,24,39,0.10)');
  const colTick  = cssVar('--plot-tick',  'rgba(17,24,39,0.70)');
  const colTitle = cssVar('--plot-title', 'rgba(17,24,39,0.90)');
  const colWait  = cssVar('--plot-wait',  'rgba(17,24,39,0.55)');

  const padL = 46 * devicePixelRatio;
  const padR = 10 * devicePixelRatio;
  const padT = 10 * devicePixelRatio;
  const padB = 38 * devicePixelRatio;

  const plotW = w - padL - padR;
  const plotH = h - padT - padB;

  // compute min/max from all series
  let ymin = Infinity, ymax = -Infinity, n = 0;
  seriesList.forEach(s => {{
    (s.values || []).forEach(v => {{
      if (v === null || v === undefined || isNaN(v)) return;
      ymin = Math.min(ymin, v);
      ymax = Math.max(ymax, v);
      n++;
    }});
  }});

  if (!isFinite(ymin) || !isFinite(ymax) || n < 2 || !tsMs || tsMs.length < 2) {{
    ctx.fillStyle = colWait;
    const fs = Math.round(12 * devicePixelRatio);
    ctx.font = fs + "px system-ui";
    ctx.textAlign = "left";
    ctx.textBaseline = "top";
    ctx.fillText("Warte auf Daten …", padL, padT + 8*devicePixelRatio);
    return;
  }}

  // headroom
  const span = Math.max(1e-6, ymax - ymin);
  ymin -= 0.06 * span;
  ymax += 0.08 * span;

  const tmin = tsMs[0];
  const tmax = tsMs[tsMs.length - 1];
  const tspan = Math.max(1, tmax - tmin);

  function xFromT(t) {{ return padL + ((t - tmin) / tspan) * plotW; }}
  function yFromV(v) {{ return padT + (1 - ((v - ymin) / (ymax - ymin))) * plotH; }}

  // grid + y labels
  ctx.strokeStyle = colGrid;
  ctx.fillStyle = colTick;
  const yTicks = 4;
  ctx.textAlign = "right";
  ctx.textBaseline = "middle";
  ctx.font = Math.round(11 * devicePixelRatio) + "px system-ui";
  for (let i = 0; i <= yTicks; i++) {{
    const y = padT + (plotH * i / yTicks);
    ctx.beginPath();
    ctx.moveTo(padL, y);
    ctx.lineTo(padL + plotW, y);
    ctx.stroke();
    const v = ymax - (ymax - ymin) * i / yTicks;
    ctx.fillText(fmt(v, 1), padL - 6*devicePixelRatio, y);
  }}

  // x grid + x labels (time)
  const tickCount = (plotW > 900*devicePixelRatio) ? 6 : ((plotW > 620*devicePixelRatio) ? 5 : 4);
  ctx.textAlign = "center";
  ctx.textBaseline = "top";
  ctx.font = Math.round(11 * devicePixelRatio) + "px system-ui";
  for (let i = 0; i <= tickCount; i++) {{
    const t = tmin + (tspan * i / tickCount);
    const x = xFromT(t);
    ctx.beginPath();
    ctx.moveTo(x, padT);
    ctx.lineTo(x, padT + plotH);
    ctx.stroke();

    const d = new Date(t);
    const lab = d.toLocaleTimeString([], {{hour: "2-digit", minute: "2-digit"}});
    ctx.fillText(lab, x, padT + plotH + 6*devicePixelRatio);
  }}

  // title
  ctx.textAlign = "left";
  ctx.textBaseline = "top";
  ctx.fillStyle = colTitle;
  ctx.font = Math.round(12 * devicePixelRatio) + "px system-ui";
  ctx.fillText(yLabel, padL, 2*devicePixelRatio);

  // series
  seriesList.forEach((s, idx) => {{
    const vals = s.values || [];
    if (vals.length < 2) return;
    const col = (colors && colors[idx]) ? colors[idx] : "rgba(106,167,255,0.9)";
    ctx.strokeStyle = col;
    ctx.lineWidth = Math.max(1, 1.6 * devicePixelRatio);
    ctx.beginPath();
    let started = false;
    for (let i = 0; i < vals.length && i < tsMs.length; i++) {{
      const v = vals[i];
      if (v === null || v === undefined || isNaN(v)) {{ started = false; continue; }}
      const x = xFromT(tsMs[i]);
      const y = yFromV(v);
      if (!started) {{ ctx.moveTo(x, y); started = true; }}
      else {{ ctx.lineTo(x, y); }}
    }}
    ctx.stroke();
  }});
}}

function kv(el, last, dev) {{
  const single = (parseInt((dev && dev.phases !== undefined) ? dev.phases : 3, 10) <= 1);
  const u = single ? `${fmt(last.va)} V` : `${fmt(last.va)} / ${fmt(last.vb)} / ${fmt(last.vc)} V`;
  const i = single ? `${fmt(last.ia)} A` : `${fmt(last.ia)} / ${fmt(last.ib)} / ${fmt(last.ic)} A`;
  const q = single ? `${fmt(last.q_total_var, 0)} VAR` : `${fmt(last.q_total_var, 0)} VAR (${fmt(last.qa,0)}/${fmt(last.qb,0)}/${fmt(last.qc,0)})`;
  const pf = single ? `${fmt(last.cosphi_total, 3)}` : `${fmt(last.cosphi_total, 3)} (${fmt(last.pfa,3)}/${fmt(last.pfb,3)}/${fmt(last.pfc,3)})`;
  el.innerHTML = `
    <b>${t('web.kv.power')}</b><span>${fmt(last.power_total_w, 0)} W</span>
    <b>${t('web.kv.kwh_today')}</b><span>${fmt(last.kwh_today, 3)} kWh</span>
    <b>${t('web.kv.u')}</b><span>${u}</span>
    <b>${t('web.kv.i')}</b><span>${i}</span>
    <b>${t('web.kv.var')}</b><span>${q}</span>
    <b>${t('web.kv.cosphi')}</b><span>${pf}</span>
  `;
}}



// Stable min/max bucket sampling (anchored to time), to avoid "jiggling" peaks.
function stableSample(arr, target) {{
  if (!arr || arr.length <= target) return arr || [];
  const n = arr.length;
  const t0 = parseInt(arr[0].ts, 10);
  const t1 = parseInt(arr[n-1].ts, 10);
  const span = Math.max(1, t1 - t0);
  const buckets = Math.max(10, Math.min(target, 2000));
  const bw = span / buckets;
  const out = [];
  let i = 0;
  for (let b = 0; b < buckets && i < n; b++) {{
    const end = (b === buckets-1) ? (t1 + 1) : (t0 + (b+1)*bw);
    let minIdx = -1, maxIdx = -1;
    let minV = Infinity, maxV = -Infinity;
    const startI = i;
    while (i < n && parseInt(arr[i].ts, 10) < end) {{
      const v = Number(arr[i].power_total_w);
      if (!isNaN(v)) {{
        if (v < minV) {{ minV = v; minIdx = i; }}
        if (v > maxV) {{ maxV = v; maxIdx = i; }}
      }}
      i++;
    }}
    if (i === startI) continue;
    if (minIdx >= 0 && maxIdx >= 0) {{
      if (minIdx === maxIdx) out.push(arr[minIdx]);
      else if (minIdx < maxIdx) {{ out.push(arr[minIdx]); out.push(arr[maxIdx]); }}
      else {{ out.push(arr[maxIdx]); out.push(arr[minIdx]); }}
    }} else {{
      out.push(arr[startI]);
    }}
  }}
  // Ensure last point is included.
  if (out.length && out[out.length-1].ts !== arr[n-1].ts) out.push(arr[n-1]);
  out.sort((a,b) => (a.ts||0) - (b.ts||0));
  // Dedupe identical timestamps.
  return out.filter((p, idx) => idx === 0 || p.ts !== out[idx-1].ts);
}

function pickDev(data, dev) {{
  const arr0 = data[dev.key] || [];
  const n0 = arr0.length;
  const last = n0 ? arr0[n0-1] : null;

  // Filter to selected time window (minutes) based on latest timestamp.
  let arr = arr0;
  if (last && last.ts) {{
    const cutoff = parseInt(last.ts, 10) - (parseInt(windowMin, 10) * 60);
    arr = arr0.filter(x => x && x.ts !== undefined && parseInt(x.ts, 10) >= cutoff);
  }}

  // Downsample for drawing performance, but keep it *stable* (time-bucketed)
  // so peaks don't appear/disappear when n changes each refresh.
  const sampled = stableSample(arr, 900);

  return {{
    arr: sampled,
    last: last
  }};
}}

function renderState(data) {
  if (!data) return;
  try {
    document.getElementById("stamp").textContent = new Date().toLocaleString();
  } catch (e) {}

  (DEVICES || []).forEach(dev => {
    const ui = UI[dev.key];
    if (!ui) return;
    // Switch state refresh (keep in sync with real device state)
    try {
      const isSwitch = String(dev.kind || "").toLowerCase() === "switch";
      if (isSwitch && ui.swState) {
        const now = Date.now();
        const lastFetch = ui._swLastFetch || 0;
        if (!lastFetch || (now - lastFetch) > 5000) {
          ui._swLastFetch = now;
          apiRun('get_switch', {device_key: dev.key}).then(res => {
            if (res && res.ok && ui.swState) {
              const on = parseSwitchOn(res);
              if (on !== null) ui.swState.textContent = on ? t('web.switch.on') : t('web.switch.off');
            }
          }).catch(()=>{});
        }
      }
    } catch (e) {}

    const picked = pickDev(data, dev);
    if (picked.last) kv(ui.kv, picked.last, dev);
    const ts = picked.arr.map(x=>x.ts*1000);
    const sparkOpts = compactMode ? {compact:true} : null;
    const isSingle = (parseInt(dev.phases || 3, 10) <= 1);
    // Show/hide elements in compact mode
    try { if (ui.v) ui.v.style.display = compactMode ? "none" : ""; } catch (e) {}
    try { if (ui.kv) ui.kv.style.display = compactMode ? "none" : ""; } catch (e) {}

    // Power
    drawLineChart(ui.p, ts, [{values: picked.arr.map(x=>x.power_total_w)}], compactMode ? "W" : t('web.chart.power'), palettePower(), sparkOpts);
    // Voltage only in normal mode
    if (!compactMode) {
      const vSeries = isSingle ? [
        {values: picked.arr.map(x=>x.va)},
      ] : [
        {values: picked.arr.map(x=>x.va)},
        {values: picked.arr.map(x=>x.vb)},
        {values: picked.arr.map(x=>x.vc)},
      ];
      drawLineChart(ui.v, ts, vSeries, isSingle ? t('web.chart.voltage.1p') : t('web.chart.voltage'), palette3());
    }
    // Current: compact mode shows TOTAL only
    if (compactMode) {
      drawLineChart(ui.c, ts, [{values: picked.arr.map(x=>x.ia)}], "A", palettePower(), sparkOpts);
    } else {
      const cSeries = isSingle ? [
        {values: picked.arr.map(x=>x.ia)},
      ] : [
        {values: picked.arr.map(x=>x.ia)},
        {values: picked.arr.map(x=>x.ib)},
        {values: picked.arr.map(x=>x.ic)},
      ];
      drawLineChart(ui.c, ts, cSeries, isSingle ? t('web.chart.current.1p') : t('web.chart.current'), palette3());
    }
  });
}

async function tick(force=false) {{
  // Pause updates when app is frozen (web pause button triggers app freeze)
  if (frozen) return;
  if (!force && !autoRefresh) return;
  try {{
    const r = await fetch("/api/state" + qs());
    const data = await r.json();

    // cache latest state so we can redraw instantly on theme changes
    window.__SEA_LAST_STATE = data;
    renderState(data);
  }} catch (e) {{
    // keep silent; next tick will retry
  }}
}}

tick(true);
setInterval(()=>tick(false), REFRESH_MS);
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
                payload = self.store.snapshot()
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
