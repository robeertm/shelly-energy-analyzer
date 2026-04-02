from __future__ import annotations

import gzip
import json
import logging
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
from dataclasses import dataclass, field
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


def _ensure_ssl_cert(cert_dir: Path) -> Tuple[Path, Path]:
    """Generate a self-signed TLS certificate if none exists.

    Returns (cert_path, key_path).  Uses ``openssl`` CLI which is available
    on macOS, Linux, and most Raspberry Pi installations.
    """
    cert_dir.mkdir(parents=True, exist_ok=True)
    cert_path = cert_dir / "server.crt"
    key_path = cert_dir / "server.key"
    if cert_path.exists() and key_path.exists():
        return cert_path, key_path

    import subprocess
    logger = logging.getLogger(__name__)
    logger.info("Generating self-signed TLS certificate for HTTPS …")
    try:
        subprocess.run(
            [
                "openssl", "req", "-x509", "-newkey", "rsa:2048",
                "-keyout", str(key_path),
                "-out", str(cert_path),
                "-days", "3650",
                "-nodes",
                "-subj", "/CN=Shelly Energy Analyzer",
            ],
            check=True,
            capture_output=True,
            timeout=30,
        )
        logger.info("TLS certificate created: %s", cert_path)
    except Exception as e:
        logger.warning("Failed to generate TLS certificate: %s", e)
        raise
    return cert_path, key_path


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
    raw: Dict[str, Any] = field(default_factory=dict)


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
        _ml_clusters = getattr(self, "_nilm_clusters", [])
        for k, arr in snap.items():
            if arr:
                try:
                    matches = list(_identify_appliance(arr[-1].power_total_w)[:3])
                    # Boost confidence with ML-learned clusters
                    if _ml_clusters and matches:
                        for i, (sig, conf) in enumerate(matches):
                            for cl in _ml_clusters:
                                if cl.get("matched_appliance") == sig.id and cl.get("count", 0) >= 5:
                                    matches[i] = (sig, min(1.0, conf + 0.15))
                                    break
                    appliances[k] = [
                        {"icon": sig.icon, "id": sig.id, "conf": conf}
                        for sig, conf in matches
                    ]
                except Exception:
                    appliances[k] = []
        if appliances:
            out["_appliances"] = appliances
        # Extract switch state from latest raw sample per device
        switch_states: Dict[str, Optional[bool]] = {}
        for k, arr in snap.items():
            if arr and hasattr(arr[-1], "raw") and isinstance(arr[-1].raw, dict):
                raw = arr[-1].raw
                sw = None
                for _sk in ("output", "ison", "on", "is_on"):
                    if _sk in raw:
                        sw = bool(raw[_sk])
                        break
                if sw is None and "relays" in raw:
                    rl = raw["relays"]
                    if isinstance(rl, list) and rl:
                        sw = any(bool(r.get("ison") or r.get("on")) for r in rl if isinstance(r, dict))
                switch_states[k] = sw
        if switch_states:
            out["_switch_states"] = switch_states
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
      -webkit-overflow-scrolling: touch;
      overflow-x: hidden;
      padding: 10px;
      padding-bottom: calc(120px + env(safe-area-inset-bottom, 0px));
    }}
    /* ── Panes ── */
    .pane {{ display: none; animation: fadeIn 0.2s ease; }}
    .pane.active {{ display: block; }}
    @keyframes fadeIn {{ from {{ opacity: 0; transform: translateY(4px); }} to {{ opacity: 1; transform: translateY(0); }} }}
    /* ── Bottom nav ── */
    #bottom-nav {{
      position: fixed;
      bottom: 0; left: 0; right: 0;
      display: flex;
      overflow-x: auto;
      -webkit-overflow-scrolling: touch;
      scrollbar-width: none;
      background: var(--card);
      border-top: 1px solid var(--border);
      padding-bottom: env(safe-area-inset-bottom, 0);
      z-index: 100;
    }}
    #bottom-nav::-webkit-scrollbar {{ display: none; }}
    .nav-btn {{
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      min-height: 48px;
      min-width: 56px;
      flex: 0 0 auto;
      background: none;
      border: none;
      color: var(--muted);
      font-size: 9px;
      cursor: pointer;
      padding: 4px 6px;
      gap: 1px;
      transition: color 0.15s;
    }}
    .nav-btn .nav-icon {{ font-size: 18px; line-height: 1; }}
    .nav-btn .nav-label {{ white-space: nowrap; font-size: 9px; }}
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
    .dev-kv {{ display: grid; grid-template-columns: minmax(100px, auto) 1fr; gap: 4px 12px; font-size: 12px; }}
    .dev-kv dt {{ color: var(--muted); min-width: 100px; }}
    .dev-kv dd {{ margin: 0; font-weight: 600; }}
    /* Switch toggle row */
    .switch-row {{
      display: flex;
      align-items: center;
      gap: 8px;
      margin-top: 8px;
      padding: 6px 10px;
      background: var(--chipbg);
      border-radius: 10px;
      font-size: 13px;
    }}
    .switch-label {{ color: var(--muted); font-weight: 600; }}
    .switch-state {{
      font-weight: 700;
      padding: 2px 8px;
      border-radius: 6px;
      font-size: 12px;
    }}
    .switch-state.on {{ color: #16a34a; background: rgba(22,163,74,0.12); }}
    .switch-state.off {{ color: #dc2626; background: rgba(220,38,38,0.12); }}
    .switch-btn {{
      margin-left: auto;
      padding: 4px 14px;
      border: 1px solid var(--border);
      border-radius: 8px;
      background: var(--card);
      color: var(--fg);
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
      transition: background 0.15s;
    }}
    .switch-btn:hover {{ background: var(--chipbg); }}
    .switch-btn:disabled {{ opacity: 0.5; cursor: wait; }}
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
    .sparkline-label {{ font-size: 10px; color: var(--muted); margin-bottom: 2px; }}
    canvas.sparkline {{
      width: 100%;
      height: 56px;
      display: block;
      border-radius: 8px;
      background: var(--chipbg);
    }}
    canvas.sparkline-sm {{
      width: 100%;
      height: 40px;
      display: block;
      border-radius: 6px;
      background: var(--chipbg);
    }}
    /* ── EV Charger Grid ── */
    .ev-grid {{ display: flex; flex-wrap: wrap; gap: 6px; padding: 8px 0; }}
    .ev-brick {{
      width: calc(33.333% - 4px);
      min-width: 90px;
      max-width: 140px;
      aspect-ratio: 1;
      border-radius: 10px;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      cursor: pointer;
      font-size: 11px;
      color: #fff;
      text-align: center;
      padding: 6px;
      transition: transform .1s;
      box-shadow: 0 1px 3px rgba(0,0,0,0.18);
    }}
    .ev-brick:active {{ transform: scale(0.95); }}
    .ev-brick.ev-green {{ background: #16a34a; }}
    .ev-brick.ev-yellow {{ background: #d97706; }}
    .ev-brick.ev-red {{ background: #dc2626; }}
    .ev-brick.ev-gray {{ background: #6b7280; }}
    .ev-brick-name {{ font-weight: 700; font-size: 11px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 100%; }}
    .ev-brick-dist {{ font-size: 14px; font-weight: 700; opacity: 0.95; margin-top: 3px; }}
    .ev-brick-info {{ font-size: 14px; font-weight: 700; margin-top: 2px; }}
    .ev-conn-grid {{ display: flex; flex-wrap: wrap; gap: 4px; margin-top: 8px; }}
    .ev-conn-brick {{
      padding: 8px 12px;
      border-radius: 8px;
      font-size: 11px;
      color: #fff;
      text-align: center;
      min-width: 70px;
      flex: 1;
    }}
    .ev-conn-brick.ev-green {{ background: #16a34a; }}
    .ev-conn-brick.ev-yellow {{ background: #d97706; }}
    .ev-conn-brick.ev-red {{ background: #dc2626; }}
    .ev-conn-brick.ev-gray {{ background: #6b7280; }}
    /* ── Chart detail modal ── */
    .chart-detail-panel {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 16px;
      width: min(96vw, 720px);
      height: min(85vh, 500px);
      display: flex;
      flex-direction: column;
      gap: 8px;
    }}
    .chart-detail-legend {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      font-size: 12px;
      color: var(--fg);
    }}
    .chart-detail-legend-item {{ display: flex; align-items: center; gap: 5px; }}
    .chart-detail-legend-dot {{ width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }}
    #chart-detail-canvas {{
      flex: 1;
      width: 100%;
      cursor: grab;
      touch-action: none;
      border-radius: 6px;
      min-height: 0;
    }}
    #chart-detail-canvas:active {{ cursor: grabbing; }}
    .sparkline-wrap[data-metric] {{ cursor: pointer; transition: opacity .15s; }}
    .sparkline-wrap[data-metric]:hover {{ opacity: .7; }}
    /* ── Heatmap ── */
    .hm-calendar {{ overflow-x: auto; -webkit-overflow-scrolling: touch; padding-bottom: 6px; }}
    .hm-grid {{ display: flex; flex-wrap: nowrap; gap: 2px; }}
    .hm-week {{ display: flex; flex-direction: column; gap: 2px; flex-shrink: 0; }}
    .hm-day {{
      border-radius: 2px;
      background: var(--chipbg);
      position: relative;
      flex-shrink: 0;
    }}
    .hm-month-labels {{ display: flex; flex-wrap: nowrap; gap: 2px; font-size: 9px; color: var(--muted); margin-bottom: 3px; }}
    .hm-month-labels span {{ overflow: visible; white-space: nowrap; flex-shrink: 0; }}
    /* Hourly heatmap table */
    .hm-table-wrap {{ overflow-x: auto; -webkit-overflow-scrolling: touch; width: 100%; }}
    .hm-table {{ border-collapse: separate; border-spacing: 2px; font-size: 9px; }}
    .hm-cell {{
      border-radius: 2px;
      padding: 0;
      border: none;
    }}
    .hm-head {{ font-size: 9px; color: var(--muted); text-align: center; padding: 0 0 2px 0; font-weight: normal; }}
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
    /* ── Live settings modal ── */
    .modal-overlay {{
      display: none;
      position: fixed;
      inset: 0;
      background: rgba(0,0,0,0.5);
      z-index: 200;
      align-items: center;
      justify-content: center;
    }}
    .modal-overlay.open {{ display: flex; }}
    .modal-panel {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 16px;
      width: min(92vw, 380px);
      max-height: 80vh;
      overflow-y: auto;
      -webkit-overflow-scrolling: touch;
    }}
    .modal-header {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 14px;
      font-weight: 700;
      font-size: 15px;
    }}
    .settings-device-row {{
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 9px 0;
      border-bottom: 1px solid var(--border);
    }}
    .settings-device-row:last-child {{ border-bottom: none; }}
    .settings-device-name {{ flex: 1; font-size: 14px; }}
    .settings-btn {{
      background: none;
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 3px 9px;
      cursor: pointer;
      color: var(--fg);
      font-size: 13px;
      line-height: 1.2;
    }}
    .settings-btn:disabled {{ opacity: 0.2; cursor: default; }}
    .toggle-vis {{
      width: 18px;
      height: 18px;
      cursor: pointer;
      accent-color: var(--accent);
      flex-shrink: 0;
    }}
    /* ── Loading / error ── */
    .loading-msg {{ color: var(--muted); font-size: 13px; padding: 20px 0; text-align: center; }}
    /* ── Export pane ── */
    .exp-sections {{ display: grid; grid-template-columns: 1fr; gap: 10px; }}
    @media (min-width: 700px) {{ .exp-sections {{ grid-template-columns: 1fr 1fr; }} }}
    .exp-section {{ border: 1px solid var(--border); border-radius: 12px; padding: 10px; background: var(--chipbg); }}
    .exp-section h3 {{ margin: 0 0 8px; font-size: 12px; font-weight: 650; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; }}
    .exp-quick {{ display: flex; flex-wrap: wrap; gap: 4px; margin-bottom: 8px; }}
    .exp-quick button {{ font-size: 11px; padding: 4px 10px; border-radius: 999px; border: 1px solid var(--border); background: var(--chipbg); color: var(--muted); cursor: pointer; min-height: auto; }}
    .exp-quick button:hover {{ color: var(--fg); border-color: rgba(106,167,255,0.35); }}
    .exp-field {{ display: flex; align-items: center; gap: 6px; margin-bottom: 6px; }}
    .exp-field label {{ min-width: 30px; font-size: 12px; color: var(--muted); }}
    .exp-field input, .exp-field select {{ flex: 1; min-width: 0; }}
    .exp-actions {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 6px; }}
    @media (min-width: 700px) {{ .exp-actions {{ grid-template-columns: repeat(3, 1fr); }} }}
    .exp-actions button {{ display: flex; flex-direction: column; align-items: center; gap: 2px; padding: 10px 6px; font-size: 12px; text-align: center; min-height: 54px; justify-content: center; }}
    .exp-actions button .eico {{ font-size: 18px; line-height: 1; }}
    .exp-actions button .elbl {{ font-size: 11px; line-height: 1.2; }}
    .exp-actions button:disabled {{ opacity: 0.5; cursor: default; }}
    .exp-actions button.busy {{ position: relative; color: transparent; }}
    .exp-actions button.busy::after {{ content: ""; position: absolute; width: 16px; height: 16px; border: 2px solid var(--border); border-top-color: var(--accent); border-radius: 50%; animation: exp-spin 0.6s linear infinite; }}
    @keyframes exp-spin {{ to {{ transform: rotate(360deg); }} }}
    .exp-placeholder {{ color: var(--muted); font-size: 12px; padding: 12px 0; text-align: center; }}
    .exp-file-card {{ display: flex; align-items: center; gap: 10px; padding: 10px; border: 1px solid var(--border); border-radius: 12px; background: var(--card); margin-bottom: 6px; overflow: hidden; }}
    .exp-file-icon {{ font-size: 24px; flex: 0 0 auto; }}
    .exp-file-info {{ flex: 1; min-width: 0; overflow: hidden; }}
    .exp-file-name {{ font-size: 12px; font-weight: 600; color: var(--fg); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
    .exp-file-meta {{ font-size: 11px; color: var(--muted); margin-top: 2px; }}
    .exp-file-btn {{ display: inline-flex; align-items: center; justify-content: center; gap: 4px; font-size: 12px; padding: 8px 14px; border-radius: 10px; border: 1px solid rgba(106,167,255,0.35); background: rgba(106,167,255,0.12); color: var(--accent); text-decoration: none; font-weight: 600; white-space: nowrap; cursor: pointer; min-height: 36px; flex: 0 0 auto; }}
    .exp-file-btn:hover {{ background: rgba(106,167,255,0.22); }}
    .exp-job-card {{ padding: 10px; border: 1px solid var(--border); border-radius: 12px; background: var(--card); margin-bottom: 6px; }}
    .exp-job-head {{ display: flex; justify-content: space-between; align-items: center; gap: 8px; margin-bottom: 4px; }}
    .exp-job-title {{ font-size: 13px; font-weight: 600; color: var(--fg); }}
    .exp-job-status {{ font-size: 11px; padding: 2px 8px; border-radius: 999px; flex: 0 0 auto; }}
    .exp-job-status.running {{ background: rgba(106,167,255,0.15); color: var(--accent); }}
    .exp-job-status.done {{ background: rgba(34,197,94,0.15); color: #22c55e; }}
    .exp-job-status.error {{ background: rgba(239,68,68,0.15); color: #ef4444; }}
    .exp-job-progress {{ width: 100%; height: 6px; border-radius: 3px; appearance: none; -webkit-appearance: none; }}
    .exp-job-progress::-webkit-progress-bar {{ background: var(--chipbg); border-radius: 3px; }}
    .exp-job-progress::-webkit-progress-value {{ background: var(--accent); border-radius: 3px; }}
    .exp-job-msg {{ font-size: 11px; color: var(--muted); margin-top: 3px; }}
    .exp-job-files {{ display: grid; grid-template-columns: 1fr; gap: 4px; margin-top: 6px; }}
    .exp-job-file-link {{ display: flex; align-items: center; gap: 6px; font-size: 12px; padding: 8px 10px; border-radius: 10px; border: 1px solid rgba(106,167,255,0.25); background: rgba(106,167,255,0.06); color: var(--accent); text-decoration: none; overflow: hidden; }}
    .exp-job-file-link span {{ overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
    .exp-job-file-link:hover {{ background: rgba(106,167,255,0.12); }}
    .exp-info-card {{ display: flex; align-items: center; gap: 10px; padding: 10px; border: 1px solid rgba(106,167,255,0.25); border-radius: 12px; background: rgba(106,167,255,0.06); margin-bottom: 6px; color: var(--accent); font-size: 12px; }}
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
      <button id="btn-freeze" class="icon-btn" title="{web_btn_freeze_title}">▶</button>
      <button id="btn-live-settings" class="icon-btn" title="{web_btn_settings_title}" onclick="openLiveSettings()">⚙</button>
      <button id="btn-theme" class="icon-btn" title="{web_btn_theme_title}">☀</button>
    </div>
  </header>

  <div id="panes">
    <!-- Live -->
    <div id="pane-live" class="pane active">
      <div id="live-timescale" style="display:flex;gap:6px;flex-wrap:wrap;padding:0 0 8px 0"></div>
      <div id="live-grid" class="card-grid"></div>
      <div id="nilm-status" style="padding:8px 12px;font-size:11px;color:var(--muted);display:none">
        <span id="nilm-badge" style="background:var(--chipbg);border-radius:8px;padding:3px 8px;font-size:10px"></span>
      </div>
    </div>

    <!-- Costs -->
    <div id="pane-costs" class="pane">
      <div id="costs-content"><p class="loading-msg">{web_loading}</p></div>
    </div>

    <!-- Heatmap -->
    <div id="pane-heatmap" class="pane">
      <div class="controls-row">
        <select id="hm-device"></select>
        <select id="hm-unit">
          <option value="kWh">kWh</option>
          <option value="co2">g CO₂</option>
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
      <div id="solar-content"><p class="loading-msg">{web_loading}</p></div>
    </div>

    <!-- Weather -->
    <div id="pane-weather" class="pane">
      <div id="weather-content"><p class="loading-msg">{web_loading}</p></div>
    </div>

    <!-- Compare -->
    <div id="pane-compare" class="pane">
      <div id="cmp-controls" style="margin-bottom:10px"></div>
      <div id="cmp-quick" style="margin-bottom:10px"></div>
      <div id="cmp-result"></div>
    </div>

    <!-- CO₂ -->
    <div id="pane-co2" class="pane">
      <div id="co2-content"><p class="loading-msg">{web_loading}</p></div>
    </div>

    <!-- Anomalies -->
    <div id="pane-anomalies" class="pane">
      <div id="anom-content"><p class="loading-msg">{web_loading}</p></div>
    </div>

    <!-- Forecast -->
    <div id="pane-forecast" class="pane">
      <div class="controls-row">
        <select id="forecast-device" onchange="loadForecast()" style="padding:8px;border-radius:10px;border:1px solid var(--border);background:var(--card);color:var(--fg);font-size:13px;min-height:36px"></select>
      </div>
      <div id="forecast-cards"></div>
    </div>

    <!-- Standby -->
    <div id="pane-standby" class="pane">
      <div id="standby-cards"></div>
      <div id="standby-table-wrap"></div>
    </div>

    <!-- Sankey / Energy Flow -->
    <div id="pane-sankey" class="pane">
      <div class="controls-row" id="sankey-periods"></div>
      <div id="sankey-cards"></div>
    </div>

    <!-- EV Chargers -->
    <div id="pane-ev" class="pane">
      <div class="controls-row" style="flex-wrap:wrap;gap:6px">
        <input id="ev-city" type="text" placeholder="{web_ev_city_placeholder}" style="flex:1;min-width:120px;padding:5px 8px;border:1px solid var(--border);border-radius:8px;background:var(--card);color:var(--fg);font-size:13px" onkeydown="if(event.key==='Enter')loadEv()">
        <select id="ev-radius" onchange="loadEv()" style="font-size:12px">
          <option value="500" selected>500 m</option>
          <option value="1000">1 km</option>
          <option value="2000">2 km</option>
          <option value="5000">5 km</option>
          <option value="10000">10 km</option>
        </select>
        <select id="ev-minkw" onchange="loadEv()" style="font-size:12px">
          <option value="0">{web_ev_all_power}</option>
          <option value="11">\u226511 kW</option>
          <option value="22">\u226522 kW</option>
          <option value="50">\u226550 kW</option>
          <option value="150">\u2265150 kW</option>
        </select>
        <select id="ev-plug" onchange="loadEv()" style="font-size:12px">
          <option value="">{web_ev_all_plugs}</option>
          <option value="typ 2">Typ 2</option>
          <option value="ccs">CCS</option>
          <option value="chademo">CHAdeMO</option>
          <option value="schuko">Schuko</option>
        </select>
        <button class="btn btn-outline" onclick="loadEv()">\u21bb</button>
      </div>
      <div id="ev-apikey-row" style="display:none;padding:6px 0">
        <div style="font-size:11px;color:var(--muted);margin-bottom:4px">{web_ev_apikey_hint}</div>
        <div style="display:flex;gap:6px">
          <input id="ev-apikey" type="text" placeholder="API Key" style="flex:1;padding:5px 8px;border:1px solid var(--border);border-radius:8px;background:var(--card);color:var(--fg);font-size:12px">
          <button class="btn btn-outline" onclick="_evSaveKey()" style="font-size:12px">{web_ev_save}</button>
        </div>
      </div>
      <div id="ev-grid-wrap"></div>
    </div>

    <!-- Export -->
    <div id="pane-export" class="pane">
      <div class="exp-sections">
        <div class="exp-section">
          <h3>{exp_daterange}</h3>
          <div class="exp-quick" id="exp-quick-dates"></div>
          <div class="exp-field">
            <label for="exp-start">{exp_from}</label>
            <input id="exp-start" type="date" />
          </div>
          <div class="exp-field">
            <label for="exp-end">{exp_to}</label>
            <input id="exp-end" type="date" />
          </div>
        </div>
        <div class="exp-section">
          <h3>{exp_invoice_settings}</h3>
          <div class="exp-field">
            <label for="exp-inv-period">{exp_invoice}</label>
            <select id="exp-inv-period">
              <option value="custom">custom</option>
              <option value="day">day</option>
              <option value="week">week</option>
              <option value="month" selected>month</option>
              <option value="year">year</option>
            </select>
          </div>
          <div class="exp-field">
            <label for="exp-inv-anchor">{exp_anchor}</label>
            <input id="exp-inv-anchor" type="date" />
          </div>
          <span style="font-size:11px;color:var(--muted)">{exp_custom_note}</span>
          <div class="exp-field" style="margin-top:6px">
            <label for="exp-bundle-h">{exp_bundle_hours}</label>
            <input id="exp-bundle-h" type="number" value="48" min="1" max="8760" style="width:80px;flex:0 0 80px;" />
          </div>
        </div>
      </div>
      <div class="exp-section" style="margin-top:10px">
        <h3>{exp_actions}</h3>
        <div class="exp-actions">
          <button id="exp-btn-summary"><span class="eico">📄</span><span class="elbl">{exp_btn_pdf}</span></button>
          <button id="exp-btn-invoices"><span class="eico">🧾</span><span class="elbl">{exp_btn_invoices}</span></button>
          <button id="exp-btn-excel"><span class="eico">📊</span><span class="elbl">{exp_btn_excel}</span></button>
          <button id="exp-btn-report-day"><span class="eico">📅</span><span class="elbl">{exp_btn_report_day}</span></button>
          <button id="exp-btn-report-month"><span class="eico">📆</span><span class="elbl">{exp_btn_report_month}</span></button>
          <button id="exp-btn-bundle"><span class="eico">📦</span><span class="elbl">{exp_btn_bundle}</span></button>
        </div>
      </div>
      <div class="exp-section" style="margin-top:10px" id="exp-results-section">
        <h3>{exp_results}</h3>
        <div id="exp-results">
          <div class="exp-placeholder" id="exp-results-ph">{exp_no_results}</div>
        </div>
      </div>
      <div class="exp-section" style="margin-top:10px" id="exp-jobs-section">
        <h3>{exp_jobs}</h3>
        <div id="exp-jobs-list">
          <div class="exp-placeholder">–</div>
        </div>
      </div>
    </div>
  </div>

  <nav id="bottom-nav">
    <button class="nav-btn active" onclick="switchPane('live',this)">
      <span class="nav-icon">📡</span>
      <span class="nav-label">{web_tab_live}</span>
    </button>
    <button class="nav-btn" onclick="switchPane('costs',this)">
      <span class="nav-icon">💰</span>
      <span class="nav-label">{web_tab_costs}</span>
    </button>
    <button class="nav-btn" onclick="switchPane('heatmap',this)">
      <span class="nav-icon">🔥</span>
      <span class="nav-label">{web_tab_heatmap}</span>
    </button>
    <button class="nav-btn" onclick="switchPane('solar',this)">
      <span class="nav-icon">☀️</span>
      <span class="nav-label">{web_tab_solar}</span>
    </button>
    <button class="nav-btn" onclick="switchPane('weather',this)">
      <span class="nav-icon">🌡️</span>
      <span class="nav-label">{web_tab_weather}</span>
    </button>
    <button class="nav-btn" onclick="switchPane('compare',this)">
      <span class="nav-icon">🔀</span>
      <span class="nav-label">{web_tab_compare}</span>
    </button>
    <button class="nav-btn" onclick="switchPane('co2',this)">
      <span class="nav-icon">🌍</span>
      <span class="nav-label">{web_tab_co2}</span>
    </button>
    <button class="nav-btn" onclick="switchPane('anomalies',this)">
      <span class="nav-icon">🔍</span>
      <span class="nav-label">{web_tab_anomalies}</span>
    </button>
    <button class="nav-btn" onclick="switchPane('forecast',this)">
      <span class="nav-icon">📈</span>
      <span class="nav-label">{web_tab_forecast}</span>
    </button>
    <button class="nav-btn" onclick="switchPane('standby',this)">
      <span class="nav-icon">🔌</span>
      <span class="nav-label">{web_tab_standby}</span>
    </button>
    <button class="nav-btn" onclick="switchPane('sankey',this)">
      <span class="nav-icon">⚡</span>
      <span class="nav-label">{web_tab_sankey}</span>
    </button>
    <button class="nav-btn" onclick="switchPane('ev',this)">
      <span class="nav-icon">🔌</span>
      <span class="nav-label">{web_tab_ev}</span>
    </button>
    <button class="nav-btn" onclick="switchPane('export',this)">
      <span class="nav-icon">📥</span>
      <span class="nav-label">{web_tab_export}</span>
    </button>
  </nav>
</div>

<div id="hm-tooltip"></div>

<div id="ev-detail-modal" class="modal-overlay" onclick="if(event.target===this)this.classList.remove('open')">
  <div class="modal-panel" style="width:min(94vw,420px);-webkit-overflow-scrolling:touch">
    <div class="modal-header">
      <span id="ev-detail-title" style="font-weight:700"></span>
      <button class="modal-close" onclick="document.getElementById('ev-detail-modal').classList.remove('open')">&times;</button>
    </div>
    <div id="ev-detail-body"></div>
  </div>
</div>

<div id="live-settings-modal" class="modal-overlay">
  <div class="modal-panel">
    <div class="modal-header">
      <span>{web_dash_device_order}</span>
      <button class="icon-btn" onclick="closeLiveSettings()">✕</button>
    </div>
    <div id="live-settings-list"></div>
    <div style="text-align:right;margin-top:12px">
      <button class="btn btn-accent" onclick="closeLiveSettings()">{web_dash_done}</button>
    </div>
  </div>
</div>

<div id="chart-detail-modal" class="modal-overlay" onclick="closeDetailChartIfBg(event)">
  <div class="chart-detail-panel">
    <div class="modal-header">
      <span id="chart-detail-title"></span>
      <button class="icon-btn" onclick="closeDetailChart()">✕</button>
    </div>
    <div class="chart-detail-legend" id="chart-detail-legend"></div>
    <canvas id="chart-detail-canvas"></canvas>
  </div>
</div>

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
let sparkData = {{}};   // key -> [{{"ts":..,"w":..,"v":..,"a":..,"phases":[...]}}]
let cmpChart = null;
let liveWindowSec = 60;
const MAX_HIST_PTS = Math.ceil(7200000 / REFRESH_MS);

/* ── Theme ── */
document.getElementById('btn-theme').addEventListener('click', function() {{
  const root = document.documentElement;
  const next = root.dataset.theme === 'dark' ? 'light' : 'dark';
  root.dataset.theme = next;
  localStorage.setItem('sea_theme', next);
  this.textContent = next === 'dark' ? '☀' : '🌙';
  // Redraw spot chart with new theme colours
  if (window._lastSpotChart && window._lastSpotFixedCt != null) {{
    _drawSpotChart(window._lastSpotChart, window._lastSpotFixedCt);
  }}
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
  // Stop polling when leaving export tab
  if (name !== 'export' && typeof _expStopJobsPolling === 'function') _expStopJobsPolling();
  if (name !== 'co2' && typeof _stopCo2LiveRates === 'function') _stopCo2LiveRates();
  if (name === 'live') {{
    startLive();
  }} else {{
    stopLive();
    if (name === 'costs') loadCosts();
    else if (name === 'heatmap') initHeatmap();
    else if (name === 'solar') initSolar();
    else if (name === 'weather') initWeather();
    else if (name === 'co2') loadCo2();
    else if (name === 'compare') initCompare();
    else if (name === 'anomalies') loadAnomalies();
    else if (name === 'forecast') loadForecast();
    else if (name === 'standby') loadStandby();
    else if (name === 'sankey') loadSankey();
    else if (name === 'ev') {{ _evInitKeyRow(); loadEv(); }}
    else if (name === 'export') initExport();
  }}
}}

/* ──────────────────────────────────────────────
   DEVICE SETTINGS (order & visibility)
────────────────────────────────────────────── */
let _lsOrder = null;
let _lsHidden = null;

function _loadLsSettings() {{
  try {{ _lsOrder = JSON.parse(localStorage.getItem('device_order') || 'null'); }} catch(e) {{ _lsOrder = null; }}
  try {{ _lsHidden = JSON.parse(localStorage.getItem('hidden_devices') || '[]'); }} catch(e) {{ _lsHidden = []; }}
  if (!Array.isArray(_lsHidden)) _lsHidden = [];
}}

function _saveLsSettings() {{
  try {{ localStorage.setItem('device_order', JSON.stringify(_lsOrder)); }} catch(e) {{}}
  try {{ localStorage.setItem('hidden_devices', JSON.stringify(_lsHidden)); }} catch(e) {{}}
}}

function openLiveSettings() {{
  _loadLsSettings();
  const allKeys = DEVICES.map(function(d) {{ return d.key; }});
  const order = (_lsOrder && _lsOrder.length) ? _lsOrder.slice() : allKeys.slice();
  allKeys.forEach(function(k) {{ if (order.indexOf(k) === -1) order.push(k); }});
  _lsOrder = order;

  const list = document.getElementById('live-settings-list');
  list.innerHTML = '';
  order.forEach(function(key, idx) {{
    const dev = DEVICES.find(function(d) {{ return d.key === key; }});
    if (!dev) return;
    const visible = _lsHidden.indexOf(key) === -1;
    const row = document.createElement('div');
    row.className = 'settings-device-row';
    row.dataset.key = key;

    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.className = 'toggle-vis';
    cb.checked = visible;
    (function(k) {{
      cb.addEventListener('change', function() {{ toggleDeviceVis(k); }});
    }})(key);

    const nameSpan = document.createElement('span');
    nameSpan.className = 'settings-device-name';
    nameSpan.textContent = dev.name || key;

    const upBtn = document.createElement('button');
    upBtn.className = 'settings-btn';
    upBtn.textContent = '\u25b2';
    if (idx > 0) {{
      (function(k) {{ upBtn.addEventListener('click', function() {{ moveDeviceUp(k); }}); }})(key);
    }} else {{
      upBtn.disabled = true;
    }}

    const dnBtn = document.createElement('button');
    dnBtn.className = 'settings-btn';
    dnBtn.textContent = '\u25bc';
    if (idx < order.length - 1) {{
      (function(k) {{ dnBtn.addEventListener('click', function() {{ moveDeviceDown(k); }}); }})(key);
    }} else {{
      dnBtn.disabled = true;
    }}

    row.appendChild(cb);
    row.appendChild(nameSpan);
    row.appendChild(upBtn);
    row.appendChild(dnBtn);
    list.appendChild(row);
  }});

  document.getElementById('live-settings-modal').classList.add('open');
}}

function closeLiveSettings() {{
  document.getElementById('live-settings-modal').classList.remove('open');
  _loadLsSettings();
}}

function toggleDeviceVis(key) {{
  const idx = _lsHidden.indexOf(key);
  if (idx === -1) _lsHidden.push(key);
  else _lsHidden.splice(idx, 1);
  _saveLsSettings();
}}

function moveDeviceUp(key) {{
  const idx = _lsOrder.indexOf(key);
  if (idx <= 0) return;
  _lsOrder.splice(idx - 1, 0, _lsOrder.splice(idx, 1)[0]);
  _saveLsSettings();
  openLiveSettings();
}}

function moveDeviceDown(key) {{
  const idx = _lsOrder.indexOf(key);
  if (idx === -1 || idx >= _lsOrder.length - 1) return;
  _lsOrder.splice(idx + 1, 0, _lsOrder.splice(idx, 1)[0]);
  _saveLsSettings();
  openLiveSettings();
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

function initTimescaleBtns() {{
  const wrap = document.getElementById('live-timescale');
  if (!wrap || wrap.children.length) return;
  const scales = [{{l:'1min',s:60}},{{l:'5min',s:300}},{{l:'15min',s:900}},{{l:'30min',s:1800}},{{l:'1h',s:3600}},{{l:'2h',s:7200}}];
  scales.forEach(function(sc) {{
    const btn = document.createElement('button');
    btn.className = 'btn btn-sm' + (liveWindowSec === sc.s ? ' btn-accent' : '');
    btn.textContent = sc.l;
    btn.dataset.sec = sc.s;
    btn.addEventListener('click', function() {{
      liveWindowSec = sc.s;
      wrap.querySelectorAll('.btn').forEach(function(b) {{
        b.className = 'btn btn-sm' + (parseInt(b.dataset.sec) === liveWindowSec ? ' btn-accent' : '');
      }});
      if (!frozen) tick(false);
    }});
    wrap.appendChild(btn);
  }});
}}
let _historyLoaded = false;
async function loadHistory() {{
  if (_historyLoaded) return;
  _historyLoaded = true;
  try {{
    const r = await fetch('/api/history');
    if (!r.ok) return;
    const data = await r.json();
    const hist = data.history || {{}};
    for (const key in hist) {{
      if (!sparkData[key]) sparkData[key] = [];
      // Prepend server-side history; keep within MAX_HIST_PTS
      const merged = hist[key].concat(sparkData[key]);
      sparkData[key] = merged.slice(-MAX_HIST_PTS);
    }}
    // Redraw sparklines if cards are already in the DOM
    const grid = document.getElementById('live-grid');
    if (grid && grid.children.length > 0) {{
      for (const key in sparkData) {{
        const buf = sparkData[key];
        if (!buf || !buf.length) continue;
        const sp = document.getElementById('sp-' + key);
        if (sp) drawSparkline(sp, wndVals(buf, 'w'));
        const spv = document.getElementById('sp-v-' + key);
        if (spv) drawSparkline(spv, wndVals(buf, 'v'), '#f59e0b', true);
        const spa = document.getElementById('sp-a-' + key);
        if (spa) drawSparkline(spa, wndVals(buf, 'a'), '#10b981', true);
        const spq = document.getElementById('sp-q-' + key);
        if (spq) drawSparkline(spq, wndVals(buf, 'q'), '#ef4444', true);
      }}
    }}
  }} catch(e) {{ /* silent */ }}
}}
function startLive() {{
  if (liveTimer) return;
  initTimescaleBtns();
  tick(true);
  loadHistory();
  liveTimer = setInterval(function() {{ if (!frozen) tick(false); }}, REFRESH_MS);
  var _fb = document.getElementById('btn-freeze');
  _fb.removeEventListener('click', toggleFreeze);
  _fb.addEventListener('click', toggleFreeze);
  // NILM learning status (update every 30s)
  _updateNilmStatus();
  if (!window._nilmTimer) window._nilmTimer = setInterval(_updateNilmStatus, 30000);
}}
function _updateNilmStatus() {{
  fetch('/api/nilm_status').then(function(r) {{ return r.json(); }}).then(function(d) {{
    var el = document.getElementById('nilm-status');
    var badge = document.getElementById('nilm-badge');
    if (!el || !badge) return;
    el.style.display = 'block';
    if (d.cluster_count > 0) {{
      var top = (d.clusters || []).slice(0, 3).map(function(c) {{
        return (c.icon || '') + ' ' + Math.round(c.centroid_w || 0) + 'W x' + (c.count || 0);
      }}).join('  ');
      badge.textContent = 'NILM ML: ' + d.cluster_count + ' {t_patterns}  |  ' + top;
    }} else {{
      var tc = d.transition_count || 0;
      badge.textContent = tc > 0
        ? 'NILM ML: {t_learning} (' + tc + ' {t_transitions}' + (tc < 10 ? ', min. 10' : '') + ')'
        : 'NILM ML: {t_waiting}';
    }}
  }}).catch(function() {{}});
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
  const hidden = _lsHidden || [];
  const order = _lsOrder;

  let devices = (data.devices || []).filter(function(d) {{
    return hidden.indexOf(d.key) === -1;
  }});
  if (order && order.length) {{
    devices.sort(function(a, b) {{
      const ia = order.indexOf(a.key);
      const ib = order.indexOf(b.key);
      if (ia === -1 && ib === -1) return 0;
      if (ia === -1) return 1;
      if (ib === -1) return -1;
      return ia - ib;
    }});
  }}

  // Init sparkline buffers
  devices.forEach(function(d) {{
    if (!sparkData[d.key]) sparkData[d.key] = [];
    const buf = sparkData[d.key];
    buf.push({{ ts: Date.now(), w: d.power_w || 0, v: d.voltage_v || 0, a: d.current_a || 0, phases: d.phases ? d.phases.slice() : [], i_n: d.i_n || 0, q: d.q_total_var || 0, q_phases: d.q_phases ? d.q_phases.slice() : [] }});
    if (buf.length > MAX_HIST_PTS) buf.shift();
  }});

  const firstKey = devices.length > 0 ? devices[0].key : null;
  const firstCardId = grid.children.length > 0 ? grid.children[0].id : null;
  const needsRebuild = first || grid.children.length !== devices.length ||
    (firstKey && firstCardId !== 'dc-' + firstKey);
  if (needsRebuild) {{
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
  if (_cdState) requestAnimationFrame(_drawDetailChart);
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
  var swBtn = div.querySelector('.switch-btn');
  if (swBtn) {{
    swBtn.addEventListener('click', function(e) {{
      e.stopPropagation();
      var dk = swBtn.dataset.devkey;
      swBtn.disabled = true;
      fetch('/api/run', {{method:'POST', headers:{{'Content-Type':'application/json'}}, body:JSON.stringify({{action:'toggle_switch',params:{{device_key:dk}}}}) }})
        .then(function(r) {{ return r.json(); }})
        .then(function(d) {{
          if (d.ok) {{
            var row = document.getElementById('sw-' + dk);
            if (row) {{
              var st = row.querySelector('.switch-state');
              if (st) {{
                st.className = 'switch-state ' + (d.on ? 'on' : 'off');
                st.textContent = d.on ? t('live.switch.on', 'On') : t('live.switch.off', 'Off');
              }}
            }}
          }}
        }})
        .catch(function() {{}})
        .finally(function() {{ swBtn.disabled = false; }});
    }});
  }}
  div.querySelectorAll('.sparkline-wrap[data-metric]').forEach(function(wrap) {{
    wrap.addEventListener('click', function(e) {{
      e.stopPropagation();
      const metric = wrap.dataset.metric;
      const devKey = wrap.dataset.devkey;
      let title;
      if (metric === 'w') title = t('web.chart.power', 'Power (W)');
      else if (metric === 'v') title = t('web.chart.voltage', 'Voltage (V)');
      else if (metric === 'a') title = t('web.chart.current', 'Current (A)');
      else if (metric === 'q') title = t('web.chart.reactive', 'Reactive Power (VAR)');
      else title = t('web.chart.neutral_current', 'I\u2099 Neutral (A)');
      openDetailChart(devKey, metric, title);
    }});
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
      phaseHtml += '<dt>' + t('web.dash.phase', 'Phase') + ' ' + (i+1) + '</dt><dd>' + fmt(ph.voltage_v,1,'V') + ' \xb7 ' + fmt(ph.current_a,2,'A') + ' \xb7 ' + fmt(ph.power_w,0,'W') + '</dd>';
    }});
    phaseHtml += '</dl>';
  }}
  const nilm = d.appliances && d.appliances.length ? '<div class="appl-list">' + d.appliances.map(function(a) {{ return '<span class="appl-chip">' + esc(a.icon + ' ' + t('appliance.' + a.id + '.name', a.id)) + '</span>'; }}).join('') + '</div>' : '';
  const inHtml = (d.i_n && d.i_n > 0.01) ? '<dl class="dev-kv" id="kv-in-' + d.key + '"><dt>I\u2099 (N)</dt><dd>' + fmt(d.i_n, 2, 'A') + '</dd></dl>' : '';
  const qHtml = '<dl class="dev-kv" id="kv-q-' + d.key + '"><dt>' + t('web.kv.var', 'Reactive power') + '</dt><dd>' + fmt(d.q_total_var || 0, 1, 'VAR') + '</dd></dl>';
  let balanceHtml = '';
  if (phases && phases.length > 1) {{
    const totalP = phases.reduce(function(s, ph) {{ return s + Math.abs(ph.power_w || 0); }}, 0) || 1;
    balanceHtml = '<dl class="dev-kv" id="kv-bal-' + d.key + '"><dt>' + t('web.kv.balance', 'Phase balance') + '</dt><dd>';
    balanceHtml += phases.map(function(ph, i) {{
      const pct = Math.round(Math.abs(ph.power_w || 0) / totalP * 100);
      return 'L' + (i+1) + '&nbsp;' + pct + '%';
    }}).join(' \xb7 ');
    balanceHtml += '</dd></dl>';
  }}
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
    (d.kind === 'switch' ? '<div class="switch-row" id="sw-' + d.key + '"><span class="switch-label">' + t('live.cards.switch', 'Switch') + ':</span> <span class="switch-state ' + (d.switch_on ? 'on' : 'off') + '">' + (d.switch_on ? t('live.switch.on', 'On') : t('live.switch.off', 'Off')) + '</span> <button class="switch-btn" data-devkey="' + d.key + '">' + t('live.switch.toggle', 'Toggle') + '</button></div>' : '') +
    '<div class="sparkline-wrap" data-metric="w" data-devkey="' + d.key + '"><canvas class="sparkline" id="sp-' + d.key + '"></canvas></div>' +
    '<div class="dev-expand">' +
      '<dl class="dev-kv">' +
        '<dt>' + t('web.kv.u', 'Voltage') + '</dt><dd>' + fmt(d.voltage_v, 1, 'V') + '</dd>' +
        '<dt>' + t('web.kv.i', 'Current') + '</dt><dd>' + fmt(d.current_a, 2, 'A') + '</dd>' +
        '<dt>cos \u03c6</dt><dd>' + (d.pf !== undefined ? fmt(d.pf, 2) : '\u2014') + '</dd>' +
        '<dt>' + t('web.kv.freq', 'Freq') + '</dt><dd>' + (d.freq_hz !== undefined ? fmt(d.freq_hz, 1, 'Hz') : '\u2014') + '</dd>' +
      '</dl>' +
      qHtml +
      balanceHtml +
      phaseHtml +
      inHtml +
      '<div class="sparkline-wrap" style="margin-top:8px" data-metric="v" data-devkey="' + d.key + '"><div class="sparkline-label">' + t('web.kv.u', 'Voltage') + '</div><canvas class="sparkline-sm" id="sp-v-' + d.key + '"></canvas></div>' +
      '<div class="sparkline-wrap" style="margin-top:6px" data-metric="a" data-devkey="' + d.key + '"><div class="sparkline-label">' + t('web.kv.i', 'Current') + '</div><canvas class="sparkline-sm" id="sp-a-' + d.key + '"></canvas></div>' +
      '<div class="sparkline-wrap" style="margin-top:6px" data-metric="q" data-devkey="' + d.key + '"><div class="sparkline-label">' + t('web.kv.var', 'Reactive power') + ' (VAR)</div><canvas class="sparkline-sm" id="sp-q-' + d.key + '"></canvas></div>' +
      (phases ? '<div class="sparkline-wrap" style="margin-top:6px" data-metric="in" data-devkey="' + d.key + '"><div class="sparkline-label">' + t('web.chart.neutral_current', 'I\u2099 Neutral (A)') + '</div><canvas class="sparkline-sm" id="sp-in-' + d.key + '"></canvas></div>' : '') +
    '</div>' +
    nilm
  );
}}

function wndVals(buf, field) {{
  if (!buf || !buf.length) return [];
  const cutoff = Date.now() - liveWindowSec * 1000;
  const pts = buf.filter(function(p) {{ return p.ts >= cutoff; }});
  return pts.map(function(p) {{ return p[field] || 0; }});
}}
function wndPhaseSeries(buf, field) {{
  if (!buf || !buf.length) return [];
  const cutoff = Date.now() - liveWindowSec * 1000;
  const pts = buf.filter(function(p) {{ return p.ts >= cutoff; }});
  if (!pts.length) return [];
  let maxPh = 0;
  pts.forEach(function(p) {{ if (p.phases && p.phases.length > maxPh) maxPh = p.phases.length; }});
  if (!maxPh) return [];
  const series = [];
  for (let i = 0; i < maxPh; i++) {{
    series.push(pts.map(function(p) {{ return (p.phases && p.phases[i]) ? (p.phases[i][field] || 0) : 0; }}));
  }}
  return series;
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
  const buf = sparkData[d.key];
  // Main power sparkline
  const sp = document.getElementById('sp-' + d.key);
  if (sp && buf) drawSparkline(sp, wndVals(buf, 'w'));
  // Voltage sparkline (relative scale so variation is visible)
  const spv = document.getElementById('sp-v-' + d.key);
  if (spv && buf) drawSparkline(spv, wndVals(buf, 'v'), '#f59e0b', true);
  // Current sparkline
  const spa = document.getElementById('sp-a-' + d.key);
  if (spa && buf) drawSparkline(spa, wndVals(buf, 'a'), '#10b981', true);
  // Reactive power sparkline
  const spq = document.getElementById('sp-q-' + d.key);
  if (spq && buf) drawSparkline(spq, wndVals(buf, 'q'), '#ef4444', true);
  // Neutral current sparkline
  const spin = document.getElementById('sp-in-' + d.key);
  if (spin && buf) drawSparkline(spin, wndVals(buf, 'i_n'), '#a855f7', true);
  // Update expand section detail values (voltage, current, cos φ, freq, phases)
  const exp = card.querySelector('.dev-expand');
  if (exp) {{
    const firstKv = exp.querySelector('.dev-kv');
    if (firstKv) {{
      const kvDds = firstKv.querySelectorAll('dd');
      if (kvDds[0]) kvDds[0].textContent = fmt(d.voltage_v, 1, 'V');
      if (kvDds[1]) kvDds[1].textContent = fmt(d.current_a, 2, 'A');
      if (kvDds[2]) kvDds[2].textContent = d.pf !== undefined ? fmt(d.pf, 2) : '\u2014';
      if (kvDds[3]) kvDds[3].textContent = d.freq_hz !== undefined ? fmt(d.freq_hz, 1, 'Hz') : '\u2014';
    }}
    const phases = d.phases && d.phases.length > 0 ? d.phases : null;
    if (phases) {{
      const allKvs = exp.querySelectorAll('.dev-kv');
      const phaseDl = allKvs.length > 1 ? allKvs[1] : null;
      if (phaseDl) {{
        const pDds = phaseDl.querySelectorAll('dd');
        phases.forEach(function(ph, i) {{
          if (pDds[i]) pDds[i].textContent = fmt(ph.voltage_v,1,'V') + ' \xb7 ' + fmt(ph.current_a,2,'A') + ' \xb7 ' + fmt(ph.power_w,0,'W');
        }});
      }}
    }}
    // Update I_N neutral current if present
    const inDl = exp.querySelector('#kv-in-' + d.key);
    if (inDl) {{
      const inDd = inDl.querySelector('dd');
      if (inDd) inDd.textContent = fmt(d.i_n, 2, 'A');
    }}
    // Update reactive power
    const qDl = exp.querySelector('#kv-q-' + d.key);
    if (qDl) {{
      const qDd = qDl.querySelector('dd');
      if (qDd) qDd.textContent = fmt(d.q_total_var || 0, 1, 'VAR');
    }}
    // Update phase balance
    const balDl = exp.querySelector('#kv-bal-' + d.key);
    if (balDl && phases) {{
      const balDd = balDl.querySelector('dd');
      if (balDd) {{
        const totalP = phases.reduce(function(s, ph) {{ return s + Math.abs(ph.power_w || 0); }}, 0) || 1;
        balDd.innerHTML = phases.map(function(ph, i) {{
          const pct = Math.round(Math.abs(ph.power_w || 0) / totalP * 100);
          return 'L' + (i+1) + '&nbsp;' + pct + '%';
        }}).join(' \xb7 ');
      }}
    }}
  }}
  // Update switch state
  if (d.kind === 'switch' && d.switch_on !== undefined && d.switch_on !== null) {{
    var swRow = document.getElementById('sw-' + d.key);
    if (swRow) {{
      var st = swRow.querySelector('.switch-state');
      if (st) {{
        st.className = 'switch-state ' + (d.switch_on ? 'on' : 'off');
        st.textContent = d.switch_on ? t('live.switch.on', 'On') : t('live.switch.off', 'Off');
      }}
    }}
  }}
  // Update appliance chips (outside expand)
  var applEl = card.querySelector('.appl-list');
  if (d.appliances && d.appliances.length) {{
    var newHtml = d.appliances.map(function(a) {{ return '<span class="appl-chip">' + esc(a.icon + ' ' + t('appliance.' + a.id + '.name', a.id)) + '</span>'; }}).join('');
    if (applEl) {{ applEl.innerHTML = newHtml; }}
    else {{ card.insertAdjacentHTML('beforeend', '<div class="appl-list">' + newHtml + '</div>'); }}
  }} else if (applEl) {{
    applEl.remove();
  }}
}}

/* ──────────────────────────────────────────────
   SPARKLINE
────────────────────────────────────────────── */
function drawSparkline(canvas, values, color, relMin) {{
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
  const min = relMin ? Math.min(...values) * 0.98 : 0;
  const range = max - min || 1;
  const pad = 4;
  const sx = (W - pad*2) / (values.length - 1);
  const cs = getComputedStyle(document.documentElement);
  const accent = color || cs.getPropertyValue('--accent').trim() || '#2563eb';
  // Fill
  ctx.beginPath();
  values.forEach(function(v, i) {{
    const x = pad + i * sx;
    const y = H - pad - ((v - min) / range) * (H - pad*2);
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  }});
  ctx.lineTo(pad + (values.length-1)*sx, H - pad);
  ctx.lineTo(pad, H - pad);
  ctx.closePath();
  ctx.fillStyle = accent + '28';
  ctx.fill();
  // Line
  ctx.beginPath();
  values.forEach(function(v, i) {{
    const x = pad + i * sx;
    const y = H - pad - ((v - min) / range) * (H - pad*2);
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  }});
  ctx.strokeStyle = accent;
  ctx.lineWidth = 1.5;
  ctx.stroke();
}}
function drawMultiSparkline(canvas, seriesArr, colors) {{
  const dpr = window.devicePixelRatio || 1;
  const W = canvas.offsetWidth || 200;
  const H = canvas.offsetHeight || 40;
  canvas.width = W * dpr;
  canvas.height = H * dpr;
  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, W, H);
  if (!seriesArr || !seriesArr.length) return;
  const n = seriesArr[0].length;
  if (n < 2) return;
  let allMin = Infinity, allMax = -Infinity;
  seriesArr.forEach(function(s) {{
    s.forEach(function(v) {{ if (v < allMin) allMin = v; if (v > allMax) allMax = v; }});
  }});
  if (!isFinite(allMin)) allMin = 0;
  if (!isFinite(allMax) || allMax <= allMin) allMax = allMin + 1;
  const range = allMax - allMin;
  const pad = 4;
  const sx = (W - pad*2) / (n - 1);
  seriesArr.forEach(function(s, si) {{
    const col = (colors && colors[si]) ? colors[si] : '#888';
    ctx.beginPath();
    for (let i = 0; i < s.length; i++) {{
      const x = pad + i * sx;
      const y = H - pad - ((s[i] - allMin) / range) * (H - pad*2);
      i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
    }}
    ctx.strokeStyle = col;
    ctx.lineWidth = 1.5;
    ctx.stroke();
  }});
}}

/* ──────────────────────────────────────────────
   CHART DETAIL MODAL  (click mini-plot → zoom)
────────────────────────────────────────────── */
const _PHASE_COLORS = ['#e05c5c','#5ca0e0','#5ce077'];
let _cdState = null;

function openDetailChart(devKey, metric, title) {{
  const buf = sparkData[devKey];
  if (!buf || buf.length < 2) return;
  _cdState = {{ devKey: devKey, metric: metric, xScale: 1.0, xOffset: 0,
    dragging: false, dragX: 0, dragOff: 0, pinchDist: null, pinchScale: 1 }};
  document.getElementById('chart-detail-title').textContent = title;
  _buildDetailLegend(devKey, metric);
  document.getElementById('chart-detail-modal').classList.add('open');
  requestAnimationFrame(_drawDetailChart);
}}

function closeDetailChart() {{
  document.getElementById('chart-detail-modal').classList.remove('open');
  _cdState = null;
}}

function closeDetailChartIfBg(e) {{
  if (e.target.id === 'chart-detail-modal') closeDetailChart();
}}

function _buildDetailLegend(devKey, metric) {{
  const buf = sparkData[devKey];
  const legend = document.getElementById('chart-detail-legend');
  legend.innerHTML = '';
  let maxPh = 0;
  if (buf) buf.forEach(function(p) {{ if (p.phases && p.phases.length > maxPh) maxPh = p.phases.length; }});
  const cs = getComputedStyle(document.documentElement);
  const accent = cs.getPropertyValue('--accent').trim() || '#2563eb';
  const totalColor = metric === 'v' ? '#f59e0b' : metric === 'a' ? '#10b981' : metric === 'q' ? '#ef4444' : accent;
  const items = [];
  if (metric === 'ph' || metric === 'v') {{
    for (let i = 0; i < maxPh; i++) items.push({{ label: 'L'+(i+1), color: _PHASE_COLORS[i]||'#888' }});
  }} else if (metric === 'in') {{
    items.push({{ label: t('web.chart.neutral_current', 'I\u2099 Neutral (A)'), color: '#a855f7' }});
  }} else if (metric === 'q') {{
    items.push({{ label: t('web.plots.series.total','Total'), color: '#ef4444' }});
    for (let i = 0; i < maxPh; i++) items.push({{ label: 'L'+(i+1), color: _PHASE_COLORS[i]||'#888' }});
  }} else if (maxPh > 0) {{
    items.push({{ label: t('web.plots.series.total','Total'), color: totalColor }});
    for (let i = 0; i < maxPh; i++) items.push({{ label: 'L'+(i+1), color: _PHASE_COLORS[i]||'#888' }});
  }}
  items.forEach(function(item) {{
    const el = document.createElement('div');
    el.className = 'chart-detail-legend-item';
    el.innerHTML = '<div class="chart-detail-legend-dot" style="background:'+item.color+'"></div><span>'+esc(item.label)+'</span>';
    legend.appendChild(el);
  }});
}}

function _drawDetailChart() {{
  if (!_cdState) return;
  const canvas = document.getElementById('chart-detail-canvas');
  if (!canvas || !canvas.offsetWidth) return;
  const buf = sparkData[_cdState.devKey];
  if (!buf || buf.length < 2) return;
  const dpr = window.devicePixelRatio || 1;
  const W = canvas.offsetWidth;
  const H = canvas.offsetHeight || 300;
  canvas.width = W * dpr;
  canvas.height = H * dpr;
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, W, H);
  const cs = getComputedStyle(document.documentElement);
  const mutedColor = cs.getPropertyValue('--muted').trim() || '#64748b';
  const borderColor = cs.getPropertyValue('--border').trim() || '#334155';
  const accent = cs.getPropertyValue('--accent').trim() || '#2563eb';
  const metric = _cdState.metric;
  const n = buf.length;
  const xScale = Math.max(1.0, Math.min(n/2, _cdState.xScale));
  const visCount = Math.max(2, Math.round(n / xScale));
  let startIdx = Math.round(_cdState.xOffset || 0);
  startIdx = Math.max(0, Math.min(n - visCount, startIdx));
  const endIdx = Math.min(n, startIdx + visCount);
  const visPts = buf.slice(startIdx, endIdx);
  if (visPts.length < 2) return;
  let maxPh = 0;
  buf.forEach(function(p) {{ if (p.phases && p.phases.length > maxPh) maxPh = p.phases.length; }});
  const totalColorMap = {{ w: accent, v: '#f59e0b', a: '#10b981', q: '#ef4444' }};
  const series = [], colors = [];
  if (metric === 'ph') {{
    for (let i = 0; i < maxPh; i++) {{
      series.push(visPts.map(function(p) {{ return (p.phases&&p.phases[i]) ? (p.phases[i].power_w||0) : 0; }}));
      colors.push(_PHASE_COLORS[i]||'#888');
    }}
  }} else if (metric === 'in') {{
    series.push(visPts.map(function(p) {{ return p.i_n||0; }}));
    colors.push('#a855f7');
  }} else if (metric === 'q') {{
    series.push(visPts.map(function(p) {{ return p.q||0; }}));
    colors.push('#ef4444');
    for (let i = 0; i < maxPh; i++) {{
      series.push(visPts.map(function(p) {{ return (p.q_phases&&p.q_phases[i]) ? (p.q_phases[i].var||0) : 0; }}));
      colors.push(_PHASE_COLORS[i]||'#888');
    }}
  }} else if (metric === 'v' && maxPh > 0) {{
    for (let i = 0; i < maxPh; i++) {{
      series.push(visPts.map(function(p) {{ return (p.phases&&p.phases[i]) ? (p.phases[i].voltage_v||0) : 0; }}));
      colors.push(_PHASE_COLORS[i]||'#888');
    }}
  }} else {{
    const phField = metric==='w' ? 'power_w' : metric==='v' ? 'voltage_v' : 'current_a';
    series.push(visPts.map(function(p) {{ return p[metric]||0; }}));
    colors.push(totalColorMap[metric]||accent);
    for (let i = 0; i < maxPh; i++) {{
      series.push(visPts.map(function(p) {{ return (p.phases&&p.phases[i]) ? (p.phases[i][phField]||0) : 0; }}));
      colors.push(_PHASE_COLORS[i]||'#888');
    }}
  }}
  if (!series.length) return;
  let allMin = Infinity, allMax = -Infinity;
  series.forEach(function(s) {{ s.forEach(function(v) {{ if (v<allMin) allMin=v; if (v>allMax) allMax=v; }}); }});
  if (!isFinite(allMin)) allMin = 0;
  if (!isFinite(allMax)||allMax<=allMin) allMax = allMin+1;
  if (metric !== 'v') allMin = Math.min(0, allMin);
  const yPad = (allMax-allMin)*0.06;
  const yMin = allMin-yPad, yMax = allMax+yPad, yRange = yMax-yMin||1;
  const padL=52, padR=12, padT=12, padB=34;
  const cW=W-padL-padR, cH=H-padT-padB;
  function toX(i) {{ return padL+(i/(visPts.length-1))*cW; }}
  function toY(v) {{ return padT+cH-((v-yMin)/yRange)*cH; }}
  // Grid + Y-axis labels
  for (let g=0; g<=4; g++) {{
    const v = yMin+(yMax-yMin)*(g/4);
    const y = toY(v);
    ctx.strokeStyle=borderColor; ctx.lineWidth=0.5;
    ctx.beginPath(); ctx.moveTo(padL,y); ctx.lineTo(W-padR,y); ctx.stroke();
    ctx.fillStyle=mutedColor; ctx.font='10px system-ui,sans-serif'; ctx.textAlign='right';
    ctx.fillText(_fmtAxisVal(v,metric), padL-4, y+4);
  }}
  // X-axis time labels
  const xIdxs = [0, Math.floor(visPts.length/4), Math.floor(visPts.length/2), Math.floor(visPts.length*3/4), visPts.length-1];
  ctx.fillStyle=mutedColor; ctx.font='10px system-ui,sans-serif'; ctx.textAlign='center';
  xIdxs.forEach(function(i) {{
    if (i>=0&&i<visPts.length) {{
      const d2=new Date(visPts[i].ts);
      const lbl=String(d2.getHours()).padStart(2,'0')+':'+String(d2.getMinutes()).padStart(2,'0')+':'+String(d2.getSeconds()).padStart(2,'0');
      ctx.fillText(lbl, toX(i), H-padB+14);
    }}
  }});
  // Series lines — only dash si===0 when it is the aggregate total (w/a), not a phase line (v/ph/in)
  const _hasTotalSeries = (metric==='w'||metric==='a'||metric==='q') && maxPh>0;
  series.forEach(function(s, si) {{
    ctx.setLineDash((si===0&&_hasTotalSeries) ? [5,3] : []);
    ctx.strokeStyle=colors[si];
    ctx.lineWidth=(si===0&&_hasTotalSeries) ? 1.5 : 2;
    ctx.beginPath();
    s.forEach(function(v,i) {{ i===0 ? ctx.moveTo(toX(i),toY(v)) : ctx.lineTo(toX(i),toY(v)); }});
    ctx.stroke();
  }});
  ctx.setLineDash([]);
  // Zoom % indicator when zoomed
  if (xScale > 1.05) {{
    const pct = Math.round((endIdx-startIdx)/n*100);
    ctx.fillStyle=mutedColor; ctx.font='10px system-ui,sans-serif'; ctx.textAlign='right';
    ctx.fillText(pct+'%', W-padR, padT+10);
  }}
}}

function _fmtAxisVal(v, metric) {{
  if (metric==='w'||metric==='ph') {{ if (Math.abs(v)>=1000) return (v/1000).toFixed(1)+'k'; return Math.round(v)+''; }}
  if (metric==='v') return v.toFixed(0);
  if (metric==='a'||metric==='in') return v.toFixed(2);
  if (metric==='q') {{ if (Math.abs(v)>=1000) return (v/1000).toFixed(1)+'k'; return Math.round(v)+''; }}
  return Math.round(v)+'';
}}

(function() {{
  function _cdSetup() {{
    const canvas = document.getElementById('chart-detail-canvas');
    if (!canvas) return;
    canvas.addEventListener('wheel', function(e) {{
      e.preventDefault();
      if (!_cdState) return;
      const rect = canvas.getBoundingClientRect();
      const cx = (e.clientX-rect.left)/rect.width;
      const buf = sparkData[_cdState.devKey]; if (!buf) return;
      const n = buf.length;
      const factor = e.deltaY > 0 ? 0.75 : 1.33;
      const oldScale = Math.max(1.0, _cdState.xScale);
      const newScale = Math.min(n/2, Math.max(1.0, oldScale*factor));
      const oldVis = n/oldScale, newVis = n/newScale;
      _cdState.xOffset = (_cdState.xOffset||0) + cx*(oldVis-newVis);
      _cdState.xScale = newScale;
      requestAnimationFrame(_drawDetailChart);
    }}, {{passive: false}});
    canvas.addEventListener('mousedown', function(e) {{
      if (!_cdState) return;
      _cdState.dragging = true; _cdState.dragX = e.clientX; _cdState.dragOff = _cdState.xOffset||0;
    }});
    window.addEventListener('mousemove', function(e) {{
      if (!_cdState||!_cdState.dragging) return;
      const buf = sparkData[_cdState.devKey]; if (!buf) return;
      const n = buf.length;
      const visCount = n/Math.max(1.0, _cdState.xScale||1);
      const pxPerPt = (canvas.offsetWidth-64)/visCount;
      _cdState.xOffset = (_cdState.dragOff||0) - (e.clientX-_cdState.dragX)/pxPerPt;
      requestAnimationFrame(_drawDetailChart);
    }});
    window.addEventListener('mouseup', function() {{ if (_cdState) _cdState.dragging = false; }});
    canvas.addEventListener('touchstart', function(e) {{
      if (!_cdState) return;
      if (e.touches.length === 1) {{
        _cdState.dragging = true; _cdState.dragX = e.touches[0].clientX;
        _cdState.dragOff = _cdState.xOffset||0; _cdState.pinchDist = null;
      }} else if (e.touches.length === 2) {{
        _cdState.dragging = false;
        _cdState.pinchDist = Math.hypot(e.touches[0].clientX-e.touches[1].clientX, e.touches[0].clientY-e.touches[1].clientY);
        _cdState.pinchScale = _cdState.xScale||1;
      }}
    }}, {{passive: true}});
    canvas.addEventListener('touchmove', function(e) {{
      if (!_cdState) return;
      if (e.touches.length === 1 && _cdState.dragging) {{
        const buf = sparkData[_cdState.devKey]; if (!buf) return;
        const n = buf.length;
        const visCount = n/Math.max(1.0, _cdState.xScale||1);
        const pxPerPt = (canvas.offsetWidth-64)/visCount;
        _cdState.xOffset = (_cdState.dragOff||0) - (e.touches[0].clientX-_cdState.dragX)/pxPerPt;
        requestAnimationFrame(_drawDetailChart);
      }} else if (e.touches.length === 2 && _cdState.pinchDist) {{
        const dist = Math.hypot(e.touches[0].clientX-e.touches[1].clientX, e.touches[0].clientY-e.touches[1].clientY);
        const buf = sparkData[_cdState.devKey]; if (!buf) return;
        const n = buf.length;
        _cdState.xScale = Math.min(n/2, Math.max(1.0, (_cdState.pinchScale||1)*(dist/_cdState.pinchDist)));
        requestAnimationFrame(_drawDetailChart);
      }}
    }}, {{passive: true}});
    canvas.addEventListener('touchend', function() {{ if (_cdState) {{ _cdState.dragging = false; _cdState.pinchDist = null; }} }}, {{passive: true}});
  }}
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', _cdSetup);
  else _cdSetup();
}})();

/* ──────────────────────────────────────────────
   COSTS TAB
────────────────────────────────────────────── */
async function loadCosts() {{
  const el = document.getElementById('costs-content');
  el.innerHTML = '<p class="loading-msg">' + t('web.loading', 'Loading\u2026') + '</p>';
  try {{
    const r = await fetch('/api/costs');
    if (!r.ok) throw new Error(r.status);
    const data = await r.json();
    renderCosts(data, el);
  }} catch(e) {{
    el.innerHTML = '<p class="error-msg">' + t('web.error', 'Error') + ': ' + e.message + '</p>';
  }}
}}

function renderCosts(data, el) {{
  if (!data || !data.devices || data.devices.length === 0) {{
    el.innerHTML = '<p class="info-msg">' + t('web.dash.no_cost_data', 'No cost data available.') + '</p>';
    return;
  }}
  const co2Active = data.co2_g_per_kwh > 0;
  const spotActive = !!data.spot_enabled;
  let html = '';
  if (data.summary) {{
    const s = data.summary;
    html += '<div class="card" style="margin-bottom:10px"><div class="card-title">' + t('web.dash.summary', 'Summary') + '</div>' +
      '<div class="metric-grid">' +
      metricCardHtml(t('web.costs.today', 'Today'), fmt(s.today_eur,2,'\u20ac'), fmt(s.today_kwh,3,'kWh')) +
      metricCardHtml(t('web.costs.week', 'Week'), fmt(s.week_eur,2,'\u20ac'), fmt(s.week_kwh,3,'kWh')) +
      metricCardHtml(t('web.costs.month', 'Month'), fmt(s.month_eur,2,'\u20ac'), fmt(s.month_kwh,3,'kWh')) +
      metricCardHtml(t('web.costs.year', 'Year'), fmt(s.year_eur,2,'\u20ac'), fmt(s.year_kwh,3,'kWh')) +
      '</div></div>';
  }}
  function spotSub(key, d) {{
    var v = d[key + '_spot_eur'];
    var f = d[key + '_eur'];
    if (!spotActive || v == null || v <= 0) return '';
    var diff = v - f;
    var arrow = diff > 0 ? '\u2191' : '\u2193';
    return arrow + ' ' + Math.abs(diff).toFixed(2) + ' \u20ac';
  }}
  // 24h Spot Price Chart (at top, before device cards)
  if (data.spot_enabled && data.spot_chart && data.spot_chart.length > 0) {{
    var fixedCt = data.fixed_ct_per_kwh || 0;
    html += '<div class="card" style="margin-bottom:10px">' +
      '<div style="font-size:12px;font-weight:650;color:#ff9800;margin-bottom:6px;text-transform:uppercase;letter-spacing:0.5px">' +
      '\u26a1 ' + t('spot.chart.title', 'Spot Price 24h') + '</div>' +
      '<canvas id="spot-24h-chart" style="width:100%;height:160px"></canvas>' +
      '<div id="spot-chart-labels" style="display:flex;justify-content:space-between;font-size:10px;color:var(--muted);margin-top:2px;padding:0 4px"></div>' +
      '</div>';
  }}

  html += '<div class="card-grid">';
  data.devices.forEach(function(d) {{
    // Fixed tariff section
    var fixedLabel = '<div style="font-size:11px;color:#2196F3;margin-bottom:4px">\U0001f4b2 ' + t('plots.dynprice.fixed', 'Festpreis') + '</div>';
    var fixedGrid = '<div class="metric-grid">' +
      metricCardHtml(t('web.costs.today', 'Today'), fmt(d.today_eur,2,'\u20ac'), fmt(d.today_kwh,3,'kWh')) +
      metricCardHtml(t('web.costs.week', 'Week'), fmt(d.week_eur,2,'\u20ac'), fmt(d.week_kwh,3,'kWh')) +
      metricCardHtml(t('web.costs.month', 'Month'), fmt(d.month_eur,2,'\u20ac'), fmt(d.month_kwh,3,'kWh')) +
      metricCardHtml(t('web.costs.projected', 'Prognose'), fmt(d.proj_eur,2,'\u20ac'), fmt(d.proj_kwh,1,'kWh')) +
      '</div>';
    // Dynamic tariff section
    var dynSection = spotActive
      ? '<div style="margin-top:8px;padding-top:8px;border-top:1px solid var(--border)">' +
          '<div style="font-size:11px;color:#ff9800;margin-bottom:4px">\u26a1 ' + t('spot.cost_label', 'Dyn. Tarif') + '</div>' +
          '<div class="metric-grid">' +
          metricCardHtml(t('web.costs.today', 'Today'), fmt(d.today_spot_eur,2,'\u20ac'), spotSub('today',d)) +
          metricCardHtml(t('web.costs.week', 'Week'), fmt(d.week_spot_eur,2,'\u20ac'), spotSub('week',d)) +
          metricCardHtml(t('web.costs.month', 'Month'), fmt(d.month_spot_eur,2,'\u20ac'), spotSub('month',d)) +
          metricCardHtml(t('web.costs.projected', 'Prognose'), fmt(d.proj_spot_eur||0,2,'\u20ac'), '') +
          '</div></div>'
      : '';
    // CO2 section
    var co2Section = co2Active
      ? '<div style="margin-top:8px;padding-top:8px;border-top:1px solid var(--border)">' +
          '<div style="font-size:11px;color:var(--muted);margin-bottom:4px">CO\u2082</div>' +
          '<div class="metric-grid">' +
          metricCardHtml(t('web.costs.today', 'Today'), fmt(d.today_co2_kg,3,'kg'), '') +
          metricCardHtml(t('web.costs.week', 'Week'), fmt(d.week_co2_kg,3,'kg'), '') +
          metricCardHtml(t('web.costs.month', 'Month'), fmt(d.month_co2_kg,3,'kg'), '') +
          metricCardHtml(t('web.costs.projected', 'Prognose'), fmt(d.proj_co2_kg,2,'kg'), '') +
          '</div></div>'
      : '';
    // Card: fixed costs → dynamic costs → CO2
    html += '<div class="card">' +
      '<div class="card-title">' + esc(d.name || d.key) + '</div>' +
      fixedLabel + fixedGrid +
      dynSection +
      co2Section +
      '</div>';
  }});
  html += '</div>';

  // Show upcoming tariff changes if any
  if (data.tariff_schedule && data.tariff_schedule.length > 0) {{
    html += '<div class="card" style="margin-top:10px"><div style="font-size:12px;font-weight:650;color:var(--muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:0.5px">' + t('settings.pricing.tariff_schedule', 'Tariff Schedule') + '</div>';
    html += '<table style="width:100%;font-size:12px;border-collapse:collapse">';
    html += '<tr style="border-bottom:1px solid var(--border)"><th style="text-align:left;padding:4px">' + t('settings.pricing.tariff_start_date', 'Start Date') + '</th><th style="text-align:right;padding:4px">' + t('settings.pricing.tariff_price', 'Price') + '</th><th style="text-align:right;padding:4px">' + t('settings.pricing.tariff_base_fee', 'Base Fee') + '</th></tr>';
    data.tariff_schedule.sort(function(a,b) {{ return a.start_date < b.start_date ? -1 : 1; }}).forEach(function(tp) {{
      var today = new Date().toISOString().slice(0,10);
      var active = tp.start_date <= today;
      var style = active ? 'font-weight:bold' : 'color:var(--muted)';
      html += '<tr style="border-bottom:1px solid var(--border);' + style + '"><td style="padding:4px">' + esc(tp.start_date) + (active ? ' \u2713' : '') + '</td><td style="text-align:right;padding:4px">' + tp.price.toFixed(4) + ' \u20ac/kWh</td><td style="text-align:right;padding:4px">' + tp.base_fee.toFixed(2) + ' \u20ac/' + t('web.costs.year', 'year') + '</td></tr>';
    }});
    html += '</table></div>';
  }}

  el.innerHTML = html;

  // Draw spot chart if present
  if (data.spot_enabled && data.spot_chart && data.spot_chart.length > 0) {{
    _drawSpotChart(data.spot_chart, data.fixed_ct_per_kwh || 0);
  }}
}}

function _drawSpotChart(hourly, fixedCt) {{
  window._lastSpotChart = hourly;
  window._lastSpotFixedCt = fixedCt;
  var canvas = document.getElementById('spot-24h-chart');
  if (!canvas) return;
  var rect = canvas.getBoundingClientRect();
  var dpr = window.devicePixelRatio || 1;
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  var ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);
  var W = rect.width, H = rect.height;
  var pad = {{top: 10, right: 10, bottom: 24, left: 44}};
  var plotW = W - pad.left - pad.right;
  var plotH = H - pad.top - pad.bottom;

  // Determine value range (ct/kWh)
  var vals = hourly.map(function(h) {{ return h.total_ct; }});
  var minV = Math.min.apply(null, vals.concat([fixedCt]));
  var maxV = Math.max.apply(null, vals.concat([fixedCt]));
  var range = maxV - minV;
  if (range < 1) range = 1;
  minV = Math.max(0, minV - range * 0.1);
  maxV = maxV + range * 0.15;
  var vRange = maxV - minV;

  // Background
  var isDark = document.documentElement.dataset.theme === 'dark';
  ctx.fillStyle = isDark ? '#111' : '#fff';
  ctx.fillRect(0, 0, W, H);

  // Grid
  ctx.strokeStyle = isDark ? 'rgba(255,255,255,0.1)' : 'rgba(0,0,0,0.08)';
  ctx.lineWidth = 0.5;
  for (var gi = 0; gi <= 4; gi++) {{
    var gy = pad.top + plotH * (1 - gi / 4);
    ctx.beginPath(); ctx.moveTo(pad.left, gy); ctx.lineTo(W - pad.right, gy); ctx.stroke();
    var gv = minV + vRange * gi / 4;
    ctx.fillStyle = isDark ? '#aaa' : '#666';
    ctx.font = '10px sans-serif';
    ctx.textAlign = 'right';
    ctx.fillText(gv.toFixed(1), pad.left - 4, gy + 3);
  }}

  // Y-axis label
  ctx.save();
  ctx.translate(10, pad.top + plotH / 2);
  ctx.rotate(-Math.PI / 2);
  ctx.fillStyle = isDark ? '#aaa' : '#666';
  ctx.font = '10px sans-serif';
  ctx.textAlign = 'center';
  ctx.fillText('ct/kWh', 0, 0);
  ctx.restore();

  // Fixed price line (dashed)
  var fixedY = pad.top + plotH * (1 - (fixedCt - minV) / vRange);
  ctx.setLineDash([6, 3]);
  ctx.strokeStyle = '#2196F3';
  ctx.lineWidth = 1.5;
  ctx.beginPath(); ctx.moveTo(pad.left, fixedY); ctx.lineTo(W - pad.right, fixedY); ctx.stroke();
  ctx.setLineDash([]);
  ctx.fillStyle = '#2196F3';
  ctx.font = '10px sans-serif';
  ctx.textAlign = 'left';
  ctx.fillText(t('plots.dynprice.fixed', 'Fixed') + ' ' + fixedCt.toFixed(1), pad.left + 4, fixedY - 4);

  // Bars
  var barW = Math.max(1, (plotW / hourly.length) - 1);
  var now = Date.now() / 1000;
  for (var i = 0; i < hourly.length; i++) {{
    var h = hourly[i];
    var x = pad.left + (i / hourly.length) * plotW;
    var v = h.total_ct;
    var barH = Math.max(1, ((v - minV) / vRange) * plotH);
    var y = pad.top + plotH - barH;

    // Color: green if cheaper than fixed, orange/red if more expensive
    var ratio = fixedCt > 0 ? v / fixedCt : 1;
    if (ratio <= 0.7) ctx.fillStyle = '#4caf50';
    else if (ratio <= 0.9) ctx.fillStyle = '#8bc34a';
    else if (ratio <= 1.0) ctx.fillStyle = '#ffeb3b';
    else if (ratio <= 1.2) ctx.fillStyle = '#ff9800';
    else ctx.fillStyle = '#e53935';

    // Future hours: slightly transparent
    if (h.ts > now) ctx.globalAlpha = 0.5;
    else ctx.globalAlpha = 0.85;
    ctx.fillRect(x, y, barW, barH);
    ctx.globalAlpha = 1.0;
  }}

  // X-axis labels
  var lblEl = document.getElementById('spot-chart-labels');
  if (lblEl && hourly.length > 0) {{
    var first = new Date(hourly[0].ts * 1000);
    var last = new Date(hourly[hourly.length - 1].ts * 1000);
    var mid = hourly.length > 1 ? new Date(hourly[Math.floor(hourly.length / 2)].ts * 1000) : first;
    function _fmtH(d) {{ return ('0' + d.getHours()).slice(-2) + ':00 ' + ('0' + d.getDate()).slice(-2) + '.' + ('0' + (d.getMonth() + 1)).slice(-2); }}
    lblEl.innerHTML = '<span>' + _fmtH(first) + '</span><span>' + _fmtH(mid) + '</span><span>' + _fmtH(last) + '</span>';
  }}

  // Touch/hover tooltip (remove old one on redraw)
  var _oldTip = canvas.parentElement.querySelector('.spot-tip');
  if (_oldTip) _oldTip.remove();
  var _spotTip = document.createElement('div');
  _spotTip.className = 'spot-tip';
  var _tipBg = isDark ? '#2a1a00' : '#fff3e0';
  var _tipFg = isDark ? '#eee' : '#333';
  _spotTip.style.cssText = 'position:absolute;display:none;background:' + _tipBg + ';border:1px solid #ff9800;border-radius:6px;padding:4px 8px;font-size:11px;pointer-events:none;z-index:99;white-space:nowrap;color:' + _tipFg;
  canvas.parentElement.style.position = 'relative';
  canvas.parentElement.appendChild(_spotTip);

  function _spotHover(e) {{
    var r = canvas.getBoundingClientRect();
    var x = (e.touches ? e.touches[0].clientX : e.clientX) - r.left;
    var idx = Math.round((x - pad.left) / plotW * hourly.length);
    if (idx < 0 || idx >= hourly.length) {{ _spotTip.style.display = 'none'; return; }}
    var h = hourly[idx];
    var d = new Date(h.ts * 1000);
    var hStr = ('0' + d.getHours()).slice(-2) + ':00 ' + ('0' + d.getDate()).slice(-2) + '.' + ('0' + (d.getMonth() + 1)).slice(-2);
    _spotTip.innerHTML = '<b>' + hStr + '</b><br>Spot: ' + h.raw_ct.toFixed(1) + ' ct/kWh<br>Total: ' + h.total_ct.toFixed(1) + ' ct/kWh';
    _spotTip.style.display = 'block';
    var tipX = Math.min(x, r.width - 120);
    _spotTip.style.left = tipX + 'px';
    _spotTip.style.top = '4px';
  }}
  if (canvas._spotHover) {{
    canvas.removeEventListener('mousemove', canvas._spotHover);
    canvas.removeEventListener('touchmove', canvas._spotHover);
  }}
  canvas._spotHover = _spotHover;
  canvas.addEventListener('mousemove', _spotHover);
  canvas.addEventListener('touchmove', _spotHover);
  canvas.addEventListener('mouseleave', function() {{ _spotTip.style.display = 'none'; }});
  canvas.addEventListener('touchend', function() {{ _spotTip.style.display = 'none'; }});
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
let _hmResizeInit = false;
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
  if (!_hmResizeInit) {{
    _hmResizeInit = true;
    let _hmResizeTimer = null;
    window.addEventListener('resize', function() {{
      clearTimeout(_hmResizeTimer);
      _hmResizeTimer = setTimeout(function() {{
        if (currentPane === 'heatmap') loadHeatmap();
      }}, 200);
    }});
  }}
  loadHeatmap();
}}

async function loadHeatmap() {{
  const device = document.getElementById('hm-device').value;
  const year = document.getElementById('hm-year').value;
  const unit = document.getElementById('hm-unit').value;
  const calWrap = document.getElementById('hm-calendar-wrap');
  const hrWrap = document.getElementById('hm-hourly-wrap');
  calWrap.innerHTML = '<p class="loading-msg">' + t('web.loading', 'Loading\u2026') + '</p>';
  hrWrap.innerHTML = '';
  if (!device) {{ calWrap.innerHTML = '<p class="info-msg">' + t('web.dash.select_device', 'Select a device.') + '</p>'; return; }}
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

/* Green→Yellow→Red gradient: 0=green, 0.5=yellow, 1=red */
function ratioColor(ratio) {{
  if (ratio <= 0.5) {{
    const t = ratio * 2;
    const r = Math.round(34 + (234 - 34) * t);
    const g = Math.round(197 + (179 - 197) * t);
    const b = Math.round(94 + (8 - 94) * t);
    return 'rgb(' + r + ',' + g + ',' + b + ')';
  }} else {{
    const t = (ratio - 0.5) * 2;
    const r = Math.round(234 + (239 - 234) * t);
    const g = Math.round(179 + (68 - 179) * t);
    const b = Math.round(8 + (68 - 8) * t);
    return 'rgb(' + r + ',' + g + ',' + b + ')';
  }}
}}

/* Yellow→Orange→Red gradient for CO₂ mode */
function ratioCo2Color(ratio) {{
  if (ratio <= 0.5) {{
    const t = ratio * 2;
    const r = Math.round(255);
    const g = Math.round(235 + (167 - 235) * t);
    const b = Math.round(132 + (38 - 132) * t);
    return 'rgb(' + r + ',' + g + ',' + b + ')';
  }} else {{
    const t = (ratio - 0.5) * 2;
    const r = Math.round(255 + (215 - 255) * t);
    const g = Math.round(167 + (25 - 167) * t);
    const b = Math.round(38 + (28 - 38) * t);
    return 'rgb(' + r + ',' + g + ',' + b + ')';
  }}
}}

function hmColorFn(unit) {{ return unit === 'co2' ? ratioCo2Color : ratioColor; }}
function hmFmtVal(v, unit) {{
  if (unit === 'co2') return fmt(v, 1, 'g CO\u2082');
  if (unit === 'eur') return fmt(v, 2, '\u20ac');
  return fmt(v, 3, 'kWh');
}}

function renderHeatmapCalendar(data, el, unit) {{
  // API returns calendar as [{date, value}, ...] — convert to dict
  const calArr = data.calendar || [];
  const daily = {{}};
  calArr.forEach(function(item) {{ daily[item.date] = item.value; }});
  const year = parseInt(document.getElementById('hm-year').value);
  const start = new Date(year, 0, 1);
  // Align to Monday
  while (start.getDay() !== 1) start.setDate(start.getDate() - 1);
  const vals = Object.values(daily).filter(function(v) {{ return v > 0; }});
  const maxVal = vals.length ? Math.max(...vals) : 1;

  // Generate weeks: always produce full weeks (Mon-Sun) until Dec 31 is covered
  const weeks = [];
  let cur = new Date(start);
  let coveredDec31 = false;
  while (!coveredDec31) {{
    const week = [];
    for (let d = 0; d < 7; d++) {{
      week.push(new Date(cur));
      if (cur.getFullYear() === year && cur.getMonth() === 11 && cur.getDate() === 31) coveredDec31 = true;
      cur.setDate(cur.getDate() + 1);
    }}
    weeks.push(week);
  }}

  // Dynamic cell size: prefer readable size, allow horizontal scroll on narrow screens
  const pane = el.closest('.pane') || document.body;
  const availW = pane.clientWidth - 32;
  const numWeeks = weeks.length;
  const calCellFromW = Math.floor((availW - (numWeeks - 1) * 2) / numWeeks);
  // Minimum 10px for readability; container scrolls horizontally if needed
  const calCellSize = Math.max(10, Math.min(calCellFromW, 18));
  const cellGap = 2;

  // Month labels — always short (3 chars) for readability
  const _dtfMonth = new Intl.DateTimeFormat(document.documentElement.lang || 'de', {{month: 'short'}});
  const monthNames = Array.from({{length: 12}}, function(_, i) {{ return _dtfMonth.format(new Date(2000, i, 1)); }});
  const _lblFontSize = '9px';
  let monthLabelHtml = '<div class="hm-month-labels" style="font-size:' + _lblFontSize + '">';
  let lastMonth = -1;
  weeks.forEach(function(week) {{
    const m = week[0].getMonth();
    if (m !== lastMonth && week[0].getFullYear() === year) {{
      monthLabelHtml += '<span style="width:' + calCellSize + 'px">' + monthNames[m] + '</span>';
      lastMonth = m;
    }} else {{
      monthLabelHtml += '<span style="width:' + calCellSize + 'px"></span>';
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
      const label = hmFmtVal(v, unit);
      const inYear = day.getFullYear() === year;
      const _colorFn = hmColorFn(unit);
      const bg = inYear && v > 0 ? _colorFn(ratio) : 'var(--chipbg)';
      gridHtml += '<div class="hm-day" style="width:' + calCellSize + 'px;height:' + calCellSize + 'px;background:' + bg + '" data-date="' + key + '" data-val="' + label + '"></div>';
    }});
    gridHtml += '</div>';
  }});
  gridHtml += '</div>';

  el.innerHTML = '<div class="card"><div class="card-title">' + t('web.hm.year_overview', 'Jahres\xfcbersicht') + '</div><div class="hm-calendar">' + monthLabelHtml + gridHtml + '</div></div>';

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
  const _dtfDay = new Intl.DateTimeFormat(document.documentElement.lang || 'de', {{weekday: 'short'}});
  const days = Array.from({{length: 7}}, function(_, i) {{ return _dtfDay.format(new Date(2001, 0, 1 + i)); }});
  const vals = [];
  for (let d = 0; d < 7; d++) for (let h = 0; h < 24; h++) {{
    const v = (hourly[d] && hourly[d][h]) ? hourly[d][h] : 0;
    if (v > 0) vals.push(v);
  }}
  const maxVal = vals.length ? Math.max(...vals) : 1;

  // Dynamic cell size: 24 cols + label col; use full available width
  const pane = el.closest('.pane') || document.body;
  const availW = pane.clientWidth - 32;
  const labelW = 22;
  const cellFromW = Math.floor((availW - labelW - 2 * 25) / 24);
  // Height constraint: reserve same ~290px overhead; remaining split between cal (7) and hourly (7)
  const availHHr = window.innerHeight - 290;
  const cellFromH = Math.floor((availHHr - 20) / 7 - 2); // 20=head row, 2=gap
  const cellSize = Math.max(8, Math.min(cellFromW, cellFromH));

  let html = '<div class="card"><div class="card-title">' + t('web.dash.hourly_pattern', 'Hourly Pattern') + '</div>';
  html += '<div class="hm-table-wrap"><table class="hm-table" style="table-layout:fixed;width:' + (labelW + 2 + 24 * (cellSize + 2)) + 'px"><thead><tr>';
  html += '<th style="width:' + labelW + 'px"></th>';
  for (let h = 0; h < 24; h++) {{
    const lbl = (h % 3 === 0) ? String(h) : '';
    html += '<th class="hm-head" style="width:' + cellSize + 'px">' + lbl + '</th>';
  }}
  html += '</tr></thead><tbody>';
  for (let d = 0; d < 7; d++) {{
    html += '<tr><td style="width:' + labelW + 'px;font-size:9px;color:var(--muted);text-align:right;padding-right:3px;white-space:nowrap">' + days[d] + '</td>';
    for (let h = 0; h < 24; h++) {{
      const v = (hourly[d] && hourly[d][h]) ? hourly[d][h] : 0;
      const ratio = maxVal > 0 ? v / maxVal : 0;
      const _colorFn2 = hmColorFn(unit);
      const bg = v > 0 ? _colorFn2(ratio) : 'var(--chipbg)';
      const title = days[d] + ' ' + h + 'h: ' + hmFmtVal(v, unit);
      html += '<td class="hm-cell" style="width:' + cellSize + 'px;height:' + cellSize + 'px;background:' + bg + '" data-tip="' + title + '"></td>';
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
   CO₂ TAB
────────────────────────────────────────────── */
let _co2LiveTimer = null;

let _co2Range = '24h';
async function loadCo2(range) {{
  if (range) _co2Range = range;
  const el = document.getElementById('co2-content');
  el.innerHTML = '<p class="loading-msg">' + t('web.loading', 'Loading\u2026') + '</p>';
  try {{
    const r = await fetch('/api/co2?range=' + _co2Range);
    if (!r.ok) throw new Error(r.status);
    const data = await r.json();
    data._range = _co2Range;
    renderCo2(data, el);
    _startCo2LiveRates();
  }} catch(e) {{
    el.innerHTML = '<p class="error-msg">Error: ' + e.message + '</p>';
  }}
}}

function _startCo2LiveRates() {{
  _stopCo2LiveRates();
  _co2LiveTimer = setInterval(_refreshCo2LiveRates, 1000);
}}
function _stopCo2LiveRates() {{
  if (_co2LiveTimer) {{ clearInterval(_co2LiveTimer); _co2LiveTimer = null; }}
}}

async function _refreshCo2LiveRates() {{
  if (currentPane !== 'co2') {{ _stopCo2LiveRates(); return; }}
  try {{
    const r = await fetch('/api/co2_live');
    if (!r.ok) return;
    const data = await r.json();
    if (!data || !data.ok) return;
    // Update hero intensity
    const hero = document.getElementById('co2-hero-value');
    if (hero && data.current_intensity !== undefined) {{
      hero.innerHTML = data.current_intensity.toFixed(0) + ' <span style="font-size:16px">g/kWh</span>';
      hero.style.color = _co2Color(data.current_intensity, data.green_threshold || 150, data.dirty_threshold || 400);
    }}
    // Update timestamp
    const heroTs = document.getElementById('co2-hero-ts');
    if (heroTs && data.intensity_hour_ts) {{
      const _d = new Date(data.intensity_hour_ts * 1000);
      const _tsStr = _d.toLocaleDateString('de-DE', {{day:'2-digit',month:'2-digit',year:'numeric'}}) + ' ' + _d.toLocaleTimeString('de-DE', {{hour:'2-digit',minute:'2-digit'}});
      heroTs.textContent = heroTs.textContent.replace(/\xb7[^\xb7]*$/, '\xb7 ' + _tsStr);
    }}
    // Update device rates table
    const tbody = document.getElementById('co2-rates-tbody');
    if (tbody && data.device_rates) {{
      let rows = '';
      data.device_rates.forEach(function(r) {{
        rows += '<tr style="border-bottom:1px solid var(--border)"><td style="padding:4px">' + esc(r.name) + '</td><td style="text-align:right;padding:4px">' + r.watts.toFixed(0) + '</td><td style="text-align:right;padding:4px;font-weight:600">' + r.co2_g_h.toFixed(1) + '</td></tr>';
      }});
      tbody.innerHTML = rows;
    }}
  }} catch(e) {{}}
}}

function _co2Color(val, green, dirty) {{
  if (val <= green) return '#4caf50';
  if (val >= dirty) return '#e53935';
  const ratio = (val - green) / (dirty - green);
  if (ratio < 0.5) {{
    const r = Math.round(255 * ratio * 2);
    return 'rgb(' + r + ',175,80)';
  }}
  const g = Math.round(175 * (1 - (ratio - 0.5) * 2));
  return 'rgb(229,' + g + ',53)';
}}

function renderCo2(data, el) {{
  if (!data || !data.enabled) {{
    el.innerHTML = '<p class="info-msg">' + t('web.co2.not_enabled', 'CO\u2082 tracking is not enabled. Enable it in Settings \u2192 ENTSO-E.') + '</p>';
    return;
  }}
  const green = data.green_threshold || 150;
  const dirty = data.dirty_threshold || 400;
  const ci = data.current_intensity || 0;
  const ciColor = _co2Color(ci, green, dirty);
  const srcLabel = data.current_source === 'entsoe_cbf' ? 'ENTSO-E + Cross-Border' : (data.current_source === 'entsoe' ? 'ENTSO-E' : data.current_source);

  // ── Live intensity hero card ──
  let html = '<div class="card" style="text-align:center;padding:16px">';
  html += '<div style="font-size:12px;font-weight:650;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px">' + t('web.co2.current', 'Current Grid CO\u2082') + '</div>';
  html += '<div id="co2-hero-value" style="font-size:42px;font-weight:700;color:' + ciColor + '">' + ci.toFixed(0) + ' <span style="font-size:16px">g/kWh</span></div>';
  var _heroTs = data.intensity_hour_ts ? new Date(data.intensity_hour_ts * 1000) : null;
  var _heroTsStr = _heroTs ? _heroTs.toLocaleDateString('de-DE', {{day:'2-digit',month:'2-digit',year:'numeric'}}) + ' ' + _heroTs.toLocaleTimeString('de-DE', {{hour:'2-digit',minute:'2-digit'}}) : '';
  html += '<div id="co2-hero-ts" style="font-size:11px;color:var(--muted);margin-top:2px">' + esc(data.zone || '') + ' \xb7 ' + esc(srcLabel) + (_heroTsStr ? ' \xb7 ' + _heroTsStr : '') + '</div>';
  html += '</div>';

  // ── Summary cards ──
  html += '<div class="card" style="margin-top:8px"><div class="metric-grid">';
  html += metricCardHtml(t('web.costs.today', 'Today'), fmt(data.co2_today_kg, 2, 'kg'), 'CO\u2082');
  html += metricCardHtml(t('web.costs.week', 'Week'), fmt(data.co2_week_kg, 2, 'kg'), 'CO\u2082');
  html += metricCardHtml(t('web.costs.month', 'Month'), fmt(data.co2_month_kg, 2, 'kg'), 'CO\u2082');
  html += metricCardHtml(t('web.costs.year', 'Year'), fmt(data.co2_year_kg, 2, 'kg'), 'CO\u2082');
  html += metricCardHtml(t('web.co2.trees', 'Trees (eq.)'), (data.tree_days||0).toFixed(0) + ' ' + t('web.dash.tree_days', 'tree-days'), '🌳');
  html += metricCardHtml(t('web.co2.car', 'Car km avoided'), (data.car_km||0).toFixed(0) + ' km', '🚗');
  html += '</div></div>';

  // ── Intensity chart with range selector ──
  const hourly = data.hourly || [];
  if (hourly.length > 0) {{
    html += '<div class="card" style="margin-top:8px">';
    const curRange = data._range || '24h';
    html += '<div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">';
    html += '<div style="font-size:12px;font-weight:650;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px">' + t('web.co2.chart_title', 'CO\u2082 Intensity') + '</div>';
    ['24h','7d','30d','all'].forEach(function(r) {{
      const active = r === curRange ? 'background:var(--accent);color:#fff;' : 'background:var(--chipbg);color:var(--fg);';
      html += '<button onclick="loadCo2(\\u0027' + r + '\\u0027)" style="border:none;border-radius:8px;padding:4px 10px;font-size:11px;cursor:pointer;' + active + '">' + r + '</button>';
    }});
    html += '</div>';
    html += '<canvas id="co2-chart" height="160" style="width:100%"></canvas>';

    // ── Heatmap strip ──
    html += '<div style="display:flex;gap:1px;margin-top:8px;border-radius:6px;overflow:hidden;height:24px" id="co2-heatmap">';
    hourly.forEach(function(h) {{
      const c = _co2Color(h.intensity, green, dirty);
      html += '<div style="flex:1;background:' + c + '" title="' + esc(h.hour) + ': ' + h.intensity.toFixed(0) + ' g/kWh"></div>';
    }});
    html += '</div>';
    html += '<div style="display:flex;justify-content:space-between;font-size:10px;color:var(--muted);margin-top:2px"><span>' + esc(hourly[0].hour) + '</span><span>' + esc(hourly[hourly.length-1].hour) + '</span></div>';
    html += '</div>';
  }}

  // ── Device CO₂ rates ──
  const rates = data.device_rates || [];
  if (rates.length > 0) {{
    html += '<div class="card" style="margin-top:8px">';
    html += '<div style="font-size:12px;font-weight:650;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:6px">' + t('web.co2.device_rates', 'CO\u2082 per Device (live)') + '</div>';
    html += '<table style="width:100%;font-size:13px;border-collapse:collapse">';
    html += '<tr style="border-bottom:1px solid var(--border)"><th style="text-align:left;padding:4px">' + t('web.dash.device', 'Device') + '</th><th style="text-align:right;padding:4px">W</th><th style="text-align:right;padding:4px">g CO\u2082/h</th></tr>';
    html += '<tbody id="co2-rates-tbody">';
    rates.forEach(function(r) {{
      html += '<tr style="border-bottom:1px solid var(--border)"><td style="padding:4px">' + esc(r.name) + '</td><td style="text-align:right;padding:4px">' + r.watts.toFixed(0) + '</td><td style="text-align:right;padding:4px;font-weight:600">' + r.co2_g_h.toFixed(1) + '</td></tr>';
    }});
    html += '</tbody>';
    html += '</table></div>';
  }}

  // ── Fuel mix ──
  const mix = data.fuel_mix || {{}};
  const mixKeys = Object.keys(mix);
  if (mixKeys.length > 0) {{
    html += '<div class="card" style="margin-top:8px">';
    html += '<div style="font-size:12px;font-weight:650;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:6px">' + t('web.co2.fuel_mix', 'Generation Mix') + (data.fuel_mix_hour ? ' (' + esc(data.fuel_mix_hour) + ')' : '') + '</div>';

    // Stacked bar
    html += '<div style="display:flex;border-radius:6px;overflow:hidden;height:20px;margin-bottom:8px">';
    const fuelColors = {{biomass:'#8bc34a',lignite:'#795548',coal_gas:'#9e9e9e',gas:'#ff9800',hard_coal:'#616161',oil:'#212121',oil_shale:'#424242',peat:'#a1887f',geothermal:'#ff5722',hydro_pumped:'#29b6f6',hydro_run:'#0288d1',hydro_reservoir:'#01579b',marine:'#00bcd4',nuclear:'#7c4dff',other_renewable:'#66bb6a',solar:'#fdd835',waste:'#bdbdbd',wind_offshore:'#26c6da',wind_onshore:'#4dd0e1',other:'#e0e0e0'}};
    mixKeys.forEach(function(k) {{
      const m = mix[k];
      if (m.share_pct > 0.5) {{
        const bgc = fuelColors[k] || '#999';
        html += '<div style="flex:' + m.share_pct + ';background:' + bgc + '" title="' + esc(m.name) + ': ' + m.share_pct.toFixed(1) + '%"></div>';
      }}
    }});
    html += '</div>';

    // Table
    html += '<table style="width:100%;font-size:12px;border-collapse:collapse">';
    html += '<tr style="border-bottom:1px solid var(--border)"><th style="text-align:left;padding:3px">' + t('web.co2.fuel', 'Fuel') + '</th><th style="text-align:right;padding:3px">MW</th><th style="text-align:right;padding:3px">%</th><th style="text-align:right;padding:3px">g/kWh</th></tr>';
    mixKeys.forEach(function(k) {{
      const m = mix[k];
      html += '<tr style="border-bottom:1px solid var(--border)"><td style="padding:3px;font-size:11px">' + esc(m.name) + '</td><td style="text-align:right;padding:3px">' + m.mw.toFixed(0) + '</td><td style="text-align:right;padding:3px">' + m.share_pct.toFixed(1) + '</td><td style="text-align:right;padding:3px">' + m.factor.toFixed(0) + '</td></tr>';
    }});
    html += '</table></div>';
  }}

  // ── 24h CO₂ per device (bar charts) ──
  const devHourly = data.device_hourly_co2 || [];
  if (devHourly.length > 0) {{
    devHourly.forEach(function(dev, idx) {{
      const bars = dev.bars || [];
      if (bars.length === 0) return;
      const maxCo2 = Math.max.apply(null, bars.map(function(b) {{ return b.co2_g; }})) || 1;
      html += '<div class="card" style="margin-top:8px">';
      html += '<div style="display:flex;justify-content:space-between;align-items:baseline">';
      html += '<div style="font-size:12px;font-weight:650;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px">' + esc(dev.name) + '</div>';
      html += '<div style="font-size:11px;color:var(--muted)">' + (dev.total_co2_g / 1000).toFixed(2) + ' kg CO\u2082 (24h)</div>';
      html += '</div>';
      html += '<div style="display:flex;align-items:flex-end;gap:1px;height:60px;margin-top:6px">';
      bars.forEach(function(b) {{
        const pct = maxCo2 > 0 ? (b.co2_g / maxCo2 * 100) : 0;
        const c = _co2Color(b.intensity, green, dirty);
        html += '<div style="flex:1;background:' + c + ';min-height:1px;height:' + Math.max(1, pct) + '%;border-radius:2px 2px 0 0;opacity:0.85" title="' + esc(b.hour) + ': ' + b.co2_g.toFixed(0) + ' g CO\u2082 (' + b.kwh.toFixed(3) + ' kWh)"></div>';
      }});
      html += '</div>';
      html += '<div style="display:flex;justify-content:space-between;font-size:10px;color:var(--muted);margin-top:2px"><span>' + esc(bars[0].hour) + '</span><span>' + esc(bars[bars.length-1].hour) + '</span></div>';
      html += '</div>';
    }});
  }}

  el.innerHTML = html;

  // ── Draw 24h chart on canvas (deferred to ensure layout) ──
  if (hourly.length > 1) {{
    requestAnimationFrame(function() {{ _drawCo2Chart(hourly, green, dirty); }});
  }}
}}

function _drawCo2Chart(hourly, green, dirty) {{
    const canvas = document.getElementById('co2-chart');
    if (canvas) {{
      const ctx = canvas.getContext('2d');
      const dpr = window.devicePixelRatio || 1;
      const rect = canvas.getBoundingClientRect();
      canvas.width = rect.width * dpr;
      canvas.height = rect.height * dpr;
      ctx.scale(dpr, dpr);
      const W = rect.width, H = rect.height;
      const pad = {{top: 10, right: 10, bottom: 24, left: 44}};
      const cW = W - pad.left - pad.right;
      const cH = H - pad.top - pad.bottom;

      const vals = hourly.map(function(h) {{ return h.intensity; }});
      const maxV = Math.max(dirty * 1.1, Math.max.apply(null, vals) * 1.1);
      const minV = 0;

      // Grid lines
      ctx.strokeStyle = getComputedStyle(document.body).getPropertyValue('--border') || '#e0e0e0';
      ctx.lineWidth = 0.5;
      for (let i = 0; i <= 4; i++) {{
        const y = pad.top + cH - (cH * i / 4);
        ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(pad.left + cW, y); ctx.stroke();
        ctx.fillStyle = getComputedStyle(document.body).getPropertyValue('--muted') || '#999';
        ctx.font = '10px sans-serif';
        ctx.textAlign = 'right';
        ctx.fillText((minV + (maxV - minV) * i / 4).toFixed(0), pad.left - 4, y + 3);
      }}

      // Threshold lines
      function drawThreshold(val, color) {{
        const y = pad.top + cH - (cH * (val - minV) / (maxV - minV));
        if (y >= pad.top && y <= pad.top + cH) {{
          ctx.setLineDash([4, 3]);
          ctx.strokeStyle = color;
          ctx.lineWidth = 1;
          ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(pad.left + cW, y); ctx.stroke();
          ctx.setLineDash([]);
        }}
      }}
      drawThreshold(green, '#4caf50');
      drawThreshold(dirty, '#e53935');

      // Area fill + line
      ctx.beginPath();
      hourly.forEach(function(h, i) {{
        const x = pad.left + (cW * i / (hourly.length - 1));
        const y = pad.top + cH - (cH * (h.intensity - minV) / (maxV - minV));
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
      }});
      // Close area
      const lastX = pad.left + cW;
      ctx.lineTo(lastX, pad.top + cH);
      ctx.lineTo(pad.left, pad.top + cH);
      ctx.closePath();
      ctx.fillStyle = 'rgba(76,175,80,0.12)';
      ctx.fill();

      // Line with gradient color
      hourly.forEach(function(h, i) {{
        if (i === 0) return;
        const x0 = pad.left + (cW * (i-1) / (hourly.length - 1));
        const y0 = pad.top + cH - (cH * (hourly[i-1].intensity - minV) / (maxV - minV));
        const x1 = pad.left + (cW * i / (hourly.length - 1));
        const y1 = pad.top + cH - (cH * (h.intensity - minV) / (maxV - minV));
        ctx.strokeStyle = _co2Color((hourly[i-1].intensity + h.intensity) / 2, green, dirty);
        ctx.lineWidth = 2.5;
        ctx.beginPath(); ctx.moveTo(x0, y0); ctx.lineTo(x1, y1); ctx.stroke();
      }});

      // X-axis labels
      ctx.fillStyle = getComputedStyle(document.body).getPropertyValue('--muted') || '#999';
      ctx.font = '10px sans-serif';
      ctx.textAlign = 'center';
      const step = Math.max(1, Math.floor(hourly.length / 6));
      hourly.forEach(function(h, i) {{
        if (i % step === 0 || i === hourly.length - 1) {{
          const x = pad.left + (cW * i / (hourly.length - 1));
          ctx.fillText(h.hour, x, pad.top + cH + 14);
        }}
      }});
    }}
}}

/* ──────────────────────────────────────────────
   SOLAR TAB
────────────────────────────────────────────── */
let solarPeriod = 'today';
function initSolar() {{
  const row = document.getElementById('solar-periods');
  if (row.children.length === 0) {{
    const _periodLbls = {{
      'today': t('web.costs.today', 'Today'),
      'week': t('web.costs.week', 'Week'),
      'month': t('web.costs.month', 'Month'),
      'year': t('web.costs.year', 'Year'),
    }};
    ['today','week','month','year'].forEach(function(p) {{
      const btn = document.createElement('button');
      btn.className = 'btn btn-outline btn-sm';
      btn.textContent = _periodLbls[p] || (p.charAt(0).toUpperCase() + p.slice(1));
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
  el.innerHTML = '<p class="loading-msg">' + t('web.loading', 'Loading\u2026') + '</p>';
  try {{
    const r = await fetch('/api/solar?period=' + period);
    if (!r.ok) throw new Error(r.status);
    const data = await r.json();
    renderSolar(data, el);
  }} catch(e) {{
    el.innerHTML = '<p class="error-msg">Error: ' + e.message + '</p>';
  }}
}}

function _solarSettingsHtml(data) {{
  // Config can come as data.config (when not configured) or directly on data (when configured)
  const cfg = data.config || {{
    enabled: data.configured !== false,
    pv_meter_device_key: data.pv_meter_device_key || '',
    feed_in_tariff: data.feed_in_tariff || 0.082,
    kw_peak: data.kw_peak || 0,
    battery_kwh: data.battery_kwh || 0,
    co2_production_kg_per_kwp: data.co2_production_kg_per_kwp || 1000,
  }};
  const devs = data.devices || DEVICES || [];
  const devOpts = '<option value="">' + t('web.dash.none', '(none)') + '</option>' +
    devs.map(function(d) {{ return '<option value="' + esc(d.key) + '"' + (d.key === (cfg.pv_meter_device_key||'') ? ' selected' : '') + '>' + esc(d.name||d.key) + '</option>'; }}).join('');
  return '<div class="card" style="margin-top:8px" id="solar-cfg-panel">' +
    '<div style="font-size:12px;font-weight:650;color:var(--muted);margin-bottom:8px;text-transform:uppercase;letter-spacing:0.5px">⚙️ ' + t('web.dash.solar_settings', 'Solar / PV Settings') + '</div>' +
    '<div style="display:grid;grid-template-columns:1fr 1fr;gap:6px">' +
    '<label style="display:flex;align-items:center;gap:6px;grid-column:1/-1"><input type="checkbox" id="scfg-enabled"' + (cfg.enabled ? ' checked' : '') + '/> ' + t('web.dash.solar_enabled', 'PV/Solar active') + '</label>' +
    '<div><label style="font-size:11px;color:var(--muted)">' + t('web.dash.solar_pv_meter', 'PV meter') + '</label><select id="scfg-pv" style="width:100%">' + devOpts + '</select></div>' +
    '<div><label style="font-size:11px;color:var(--muted)">' + t('web.dash.solar_tariff', 'Feed-in (€/kWh)') + '</label><input id="scfg-tariff" type="number" step="0.001" value="' + (cfg.feed_in_tariff||0.082) + '" style="width:100%"/></div>' +
    '<div><label style="font-size:11px;color:var(--muted)">' + t('web.dash.solar_kwp', 'kWp installed') + '</label><input id="scfg-kwp" type="number" step="0.1" value="' + (cfg.kw_peak||0) + '" style="width:100%"/></div>' +
    '<div><label style="font-size:11px;color:var(--muted)">' + t('web.dash.solar_battery', 'Battery (kWh)') + '</label><input id="scfg-bat" type="number" step="0.1" value="' + (cfg.battery_kwh||0) + '" style="width:100%"/></div>' +
    '<div><label style="font-size:11px;color:var(--muted)">' + t('web.dash.solar_co2prod', 'CO₂/kWp (kg)') + '</label><input id="scfg-co2p" type="number" step="10" value="' + (cfg.co2_production_kg_per_kwp||1000) + '" style="width:100%"/></div>' +
    '<div style="grid-column:1/-1;text-align:right;margin-top:4px"><button class="btn btn-accent btn-sm" onclick="saveSolarCfg()">' + t('web.dash.save', 'Save') + '</button></div>' +
    '</div></div>';
}}
async function saveSolarCfg() {{
  try {{
    const p = {{
      enabled: document.getElementById('scfg-enabled').checked,
      pv_meter_device_key: document.getElementById('scfg-pv').value,
      feed_in_tariff: parseFloat(document.getElementById('scfg-tariff').value)||0.082,
      kw_peak: parseFloat(document.getElementById('scfg-kwp').value)||0,
      battery_kwh: parseFloat(document.getElementById('scfg-bat').value)||0,
      co2_production_kg_per_kwp: parseFloat(document.getElementById('scfg-co2p').value)||1000,
    }};
    const r = await fetch('/api/run', {{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{action:'save_solar_config',params:p}})}});
    const j = await r.json();
    if (j && j.ok) {{ loadSolar(solarPeriod); }}
    else {{ alert('Error: ' + (j.error||'unknown')); }}
  }} catch(e) {{ alert('Error: ' + e.message); }}
}}

function renderSolar(data, el) {{
  if (data.configured === false || data.enabled === false) {{
    el.innerHTML = '<p class="info-msg">' + t('web.dash.solar_not_configured', 'Solar monitoring is not configured.') + '</p>' + _solarSettingsHtml(data);
    return;
  }}
  // Energy metrics
  const fields = [
    [t('web.dash.pv_production', 'PV Production'), fmt(data.pv_kwh,3,'kWh'), '☀️'],
    [t('web.dash.feed_in', 'Feed-in'), fmt(data.feed_in_kwh,3,'kWh'), '🔼'],
    [t('web.dash.grid_draw', 'Grid Draw'), fmt(data.grid_kwh,3,'kWh'), '🔽'],
    [t('web.dash.self_consumption', 'Self-Consumption'), fmt(data.self_kwh,3,'kWh'), '🏠'],
    [t('web.dash.autarky', 'Autarky'), fmt(data.autarky_pct,1,'%'), '🌟'],
    [t('web.dash.revenue', 'Revenue'), fmt(data.revenue_eur,2,'\u20ac'), '💶'],
    [t('web.dash.savings', 'Savings'), fmt(data.savings_eur,2,'\u20ac'), '💰'],
  ];
  let html = '<div class="card"><div class="metric-grid">';
  fields.forEach(function(f) {{ html += metricCardHtml(f[0], f[1], f[2]); }});
  html += '</div></div>';

  // CO₂ section
  if (data.co2_saved_kg !== undefined) {{
    const co2Fields = [
      [t('web.dash.co2_saved', 'CO\u2082 saved'), fmt(data.co2_saved_kg,2,'kg'), '🌱'],
      [t('web.dash.co2_grid', 'CO\u2082 grid'), fmt(data.co2_grid_kg,2,'kg'), '🏭'],
      [t('web.dash.co2_intensity', 'Grid intensity'), fmt(data.co2_intensity_g_per_kwh,0,'g/kWh'), data.co2_source === 'entsoe' ? '📡' : '📊'],
    ];
    // Equivalent: trees absorb ~22 kg CO₂/year → per day ~0.06 kg
    const treeDays = data.co2_saved_kg > 0 ? (data.co2_saved_kg / 22.0 * 365).toFixed(0) : '0';
    const carKm = data.co2_saved_kg > 0 ? (data.co2_saved_kg / 0.170).toFixed(0) : '0';
    co2Fields.push([t('web.dash.co2_equiv_trees', 'Trees (eq.)'), treeDays + ' ' + t('web.dash.tree_days', 'tree-days'), '🌳']);
    co2Fields.push([t('web.dash.co2_equiv_car', 'Car km avoided'), carKm + ' km', '🚗']);

    html += '<div class="card" style="margin-top:8px"><div style="font-size:12px;font-weight:650;color:var(--muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:0.5px">CO\u2082 ' + t('web.dash.solar_impact', 'Impact') + '</div><div class="metric-grid">';
    co2Fields.forEach(function(f) {{ html += metricCardHtml(f[0], f[1], f[2]); }});
    html += '</div></div>';
  }}

  // System info (if kw_peak configured)
  if (data.kw_peak > 0) {{
    html += '<div class="card" style="margin-top:8px"><div style="font-size:12px;font-weight:650;color:var(--muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:0.5px">' + t('web.dash.solar_system', 'System') + '</div><div class="metric-grid">';
    html += metricCardHtml(t('web.dash.kw_peak', 'kWp installed'), fmt(data.kw_peak,1,'kWp'), '⚡');
    if (data.battery_kwh > 0) {{
      html += metricCardHtml(t('web.dash.battery', 'Battery'), fmt(data.battery_kwh,1,'kWh'), '🔋');
    }}
    if (data.co2_embodied_kg > 0) {{
      html += metricCardHtml(t('web.dash.co2_embodied', 'CO\u2082 embodied'), fmt(data.co2_embodied_kg,0,'kg'), '🏗️');
    }}
    html += '</div></div>';
  }}

  // Settings toggle at bottom
  html += '<div style="margin-top:8px;text-align:center"><button class="btn btn-outline btn-sm" id="solar-cfg-btn">⚙️ ' + t('web.dash.solar_settings', 'Settings') + '</button></div>';
  html += '<div id="solar-cfg-toggle" style="display:none">' + _solarSettingsHtml(data) + '</div>';

  el.innerHTML = html;
  // Bind settings toggle after DOM update
  const _cfgBtn = document.getElementById('solar-cfg-btn');
  if (_cfgBtn) {{
    _cfgBtn.addEventListener('click', function() {{
      const panel = document.getElementById('solar-cfg-toggle');
      if (panel) panel.style.display = panel.style.display === 'none' ? 'block' : 'none';
    }});
  }}
}}

/* ──────────────────────────────────────────────
   WEATHER TAB
────────────────────────────────────────────── */
let weatherLoaded = false;
function initWeather() {{
  loadWeather();
}}
async function loadWeather() {{
  const el = document.getElementById('weather-content');
  el.innerHTML = '<p class="loading-msg">' + t('web.loading', 'Loading\u2026') + '</p>';
  try {{
    const r = await fetch('/api/weather_correlation');
    if (!r.ok) throw new Error(r.status);
    const d = await r.json();
    renderWeather(d, el);
  }} catch(e) {{
    el.innerHTML = '<p class="error-msg">Error: ' + e.message + '</p>';
  }}
}}

function renderWeather(d, el) {{
  if (!d.ok) {{
    el.innerHTML = '<p class="info-msg">' + (d.error || t('web.weather.no_data', 'No weather data.')) + '</p>';
    return;
  }}
  var html = '';

  // Current weather cards
  if (d.current) {{
    html += '<div class="metric-grid">';
    html += '<div class="card"><div class="metric-label">\U0001f321\ufe0f ' + t('web.weather.temp', 'Temperature') + '</div><div class="metric-value">' + (d.current.temp_c != null ? d.current.temp_c.toFixed(1) + ' \u00b0C' : '\u2013') + '</div></div>';
    html += '<div class="card"><div class="metric-label">\U0001f4a7 ' + t('web.weather.humidity', 'Humidity') + '</div><div class="metric-value">' + (d.current.humidity_pct != null ? Math.round(d.current.humidity_pct) + '%' : '\u2013') + '</div></div>';
    html += '<div class="card"><div class="metric-label">\U0001f4a8 ' + t('web.weather.wind', 'Wind') + '</div><div class="metric-value">' + (d.current.wind_speed_ms != null ? d.current.wind_speed_ms.toFixed(1) + ' m/s' : '\u2013') + '</div></div>';
    html += '<div class="card"><div class="metric-label">\u2601\ufe0f ' + t('web.weather.clouds', 'Cloud cover') + '</div><div class="metric-value">' + (d.current.clouds_pct != null ? Math.round(d.current.clouds_pct) + '%' : '\u2013') + '</div></div>';
    html += '</div>';
  }}

  // Correlation metrics
  if (d.correlation) {{
    var c = d.correlation;
    html += '<div class="metric-grid" style="margin-top:10px">';
    html += '<div class="card"><div class="metric-label">' + t('web.weather.pearson', 'Pearson r') + '</div><div class="metric-value">' + (c.r_value != null ? c.r_value.toFixed(3) : '\u2013') + '</div></div>';
    html += '<div class="card"><div class="metric-label">' + t('web.weather.hdd', 'HDD') + '</div><div class="metric-value">' + (c.hdd != null ? c.hdd.toFixed(1) : '\u2013') + '</div></div>';
    html += '<div class="card"><div class="metric-label">' + t('web.weather.cdd', 'CDD') + '</div><div class="metric-value">' + (c.cdd != null ? c.cdd.toFixed(1) : '\u2013') + '</div></div>';
    html += '<div class="card"><div class="metric-label">' + t('web.weather.kwh_hdd', 'kWh/HDD') + '</div><div class="metric-value">' + (c.kwh_per_hdd != null ? c.kwh_per_hdd.toFixed(2) : '\u2013') + '</div></div>';
    html += '<div class="card"><div class="metric-label">' + t('web.weather.kwh_cdd', 'kWh/CDD') + '</div><div class="metric-value">' + (c.kwh_per_cdd != null ? c.kwh_per_cdd.toFixed(2) : '\u2013') + '</div></div>';
    html += '</div>';

    // Interpretation
    var interp = '';
    if (c.r_value != null) {{
      if (c.r_value < -0.4) interp = t('web.weather.heating', 'Strong heating correlation') + ' (r = ' + c.r_value.toFixed(2) + ')';
      else if (c.r_value > 0.4) interp = t('web.weather.cooling', 'Strong cooling correlation') + ' (r = ' + c.r_value.toFixed(2) + ')';
      else interp = t('web.weather.none', 'No significant weather dependency') + ' (r = ' + c.r_value.toFixed(2) + ')';
    }}
    if (interp) html += '<p style="color:var(--muted);font-size:12px;margin:8px 0 0">' + interp + '</p>';
  }}

  // Charts
  if (d.paired && d.paired.length >= 3) {{
    html += '<div class="card" style="margin-top:10px"><div style="font-size:12px;font-weight:650;color:var(--muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:0.5px">' + t('web.weather.scatter', 'Temperature vs. Consumption') + '</div>';
    html += '<canvas id="weather-scatter" style="width:100%;height:220px"></canvas></div>';

    html += '<div class="card" style="margin-top:10px"><div style="font-size:12px;font-weight:650;color:var(--muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:0.5px">' + t('web.weather.timeline', 'Timeline') + '</div>';
    html += '<canvas id="weather-timeline" style="width:100%;height:220px"></canvas></div>';

    // Legend for scatter
    html += '<div style="display:flex;gap:10px;justify-content:center;margin-top:6px;font-size:10px;color:var(--muted)">';
    html += '<span>\U0001f319 0\u20136h</span><span>\U0001f305 6\u201312h</span><span>\u2600\ufe0f 12\u201318h</span><span>\U0001f306 18\u201324h</span>';
    html += '</div>';
  }} else if (d.paired) {{
    html += '<p style="color:var(--muted);font-size:12px;margin-top:10px">' + t('web.weather.few_data', 'Only ' + d.paired.length + ' data points.').replace('{{n}}', d.paired.length) + '</p>';
  }}

  el.innerHTML = html;

  // Draw charts after DOM update
  if (d.paired && d.paired.length >= 3) {{
    setTimeout(function() {{ _drawWeatherCharts(d); }}, 50);
  }}
}}

function _weatherHourColor(h) {{
  // twilight-shifted: night=dark blue, morning=warm orange, noon=bright yellow, evening=purple
  var hue = (240 + h * 15) % 360;
  var sat = 70;
  var lgt = (h >= 6 && h <= 20) ? 55 : 35;
  return 'hsl(' + hue + ',' + sat + '%,' + lgt + '%)';
}}

function _drawWeatherCharts(d) {{
  var pts = d.paired;
  var muted = getComputedStyle(document.body).getPropertyValue('--muted') || '#999';
  var border = getComputedStyle(document.body).getPropertyValue('--border') || '#e0e0e0';

  // --- Scatter plot ---
  var scatterEl = document.getElementById('weather-scatter');
  if (scatterEl) {{
    var dpr = window.devicePixelRatio || 1;
    var rect = scatterEl.getBoundingClientRect();
    scatterEl.width = rect.width * dpr;
    scatterEl.height = rect.height * dpr;
    var ctx = scatterEl.getContext('2d');
    ctx.scale(dpr, dpr);
    var W = rect.width, H = rect.height;
    var pad = {{top: 12, right: 16, bottom: 28, left: 48}};
    var cW = W - pad.left - pad.right;
    var cH = H - pad.top - pad.bottom;

    var temps = pts.map(function(p) {{ return p.temp; }});
    var kwhs = pts.map(function(p) {{ return p.kwh; }});
    var minT = Math.min.apply(null, temps) - 1;
    var maxT = Math.max.apply(null, temps) + 1;
    var minK = 0;
    var maxK = Math.max.apply(null, kwhs) * 1.1 || 1;

    // Grid
    ctx.strokeStyle = border; ctx.lineWidth = 0.5;
    ctx.fillStyle = muted; ctx.font = '10px sans-serif'; ctx.textAlign = 'right';
    for (var i = 0; i <= 4; i++) {{
      var gy = pad.top + cH - (cH * i / 4);
      ctx.beginPath(); ctx.moveTo(pad.left, gy); ctx.lineTo(pad.left + cW, gy); ctx.stroke();
      ctx.fillText((minK + (maxK - minK) * i / 4).toFixed(2), pad.left - 4, gy + 3);
    }}
    ctx.textAlign = 'center';
    for (var j = 0; j <= 4; j++) {{
      var gx = pad.left + (cW * j / 4);
      ctx.fillText((minT + (maxT - minT) * j / 4).toFixed(0) + '\u00b0', gx, pad.top + cH + 16);
    }}

    // Axis labels
    ctx.fillStyle = muted; ctx.font = '10px sans-serif';
    ctx.save(); ctx.translate(12, pad.top + cH / 2); ctx.rotate(-Math.PI/2);
    ctx.textAlign = 'center'; ctx.fillText('kWh', 0, 0); ctx.restore();
    ctx.textAlign = 'center'; ctx.fillText('\u00b0C', pad.left + cW / 2, H - 2);

    // Points colored by hour
    pts.forEach(function(p) {{
      var x = pad.left + cW * (p.temp - minT) / (maxT - minT);
      var y = pad.top + cH - cH * (p.kwh - minK) / (maxK - minK);
      ctx.beginPath();
      ctx.arc(x, y, 3.5, 0, Math.PI * 2);
      ctx.fillStyle = _weatherHourColor(p.hour_of_day || 0);
      ctx.globalAlpha = 0.7;
      ctx.fill();
    }});
    ctx.globalAlpha = 1.0;

    // Regression line
    if (d.correlation && d.correlation.slope != null) {{
      var sl = d.correlation.slope, ic = d.correlation.intercept;
      var x1 = pad.left;
      var x2 = pad.left + cW;
      var t1 = minT, t2 = maxT;
      var y1 = pad.top + cH - cH * ((sl * t1 + ic) - minK) / (maxK - minK);
      var y2 = pad.top + cH - cH * ((sl * t2 + ic) - minK) / (maxK - minK);
      ctx.setLineDash([6, 3]);
      ctx.strokeStyle = '#e74c3c';
      ctx.lineWidth = 1.5;
      ctx.globalAlpha = 0.7;
      ctx.beginPath(); ctx.moveTo(x1, y1); ctx.lineTo(x2, y2); ctx.stroke();
      ctx.setLineDash([]);
      ctx.globalAlpha = 1.0;
    }}
  }}

  // --- Timeline chart ---
  var timeEl = document.getElementById('weather-timeline');
  if (timeEl) {{
    var sorted = pts.slice().sort(function(a, b) {{ return a.ts - b.ts; }});
    if (sorted.length > 72) sorted = sorted.slice(sorted.length - 72);

    var dpr2 = window.devicePixelRatio || 1;
    var rect2 = timeEl.getBoundingClientRect();
    timeEl.width = rect2.width * dpr2;
    timeEl.height = rect2.height * dpr2;
    var ctx2 = timeEl.getContext('2d');
    ctx2.scale(dpr2, dpr2);
    var W2 = rect2.width, H2 = rect2.height;
    var pad2 = {{top: 12, right: 48, bottom: 28, left: 48}};
    var cW2 = W2 - pad2.left - pad2.right;
    var cH2 = H2 - pad2.top - pad2.bottom;

    var kwhs2 = sorted.map(function(p) {{ return p.kwh; }});
    var temps2 = sorted.map(function(p) {{ return p.temp; }});
    var maxK2 = Math.max.apply(null, kwhs2) * 1.15 || 1;
    var minT2 = Math.min.apply(null, temps2) - 1;
    var maxT2 = Math.max.apply(null, temps2) + 1;
    var n = sorted.length;
    var barW = Math.max(1, (cW2 / n) - 1);

    // Grid + Y-axis left (kWh)
    ctx2.strokeStyle = border; ctx2.lineWidth = 0.5;
    ctx2.fillStyle = '#3498db'; ctx2.font = '10px sans-serif'; ctx2.textAlign = 'right';
    for (var gi = 0; gi <= 4; gi++) {{
      var gy2 = pad2.top + cH2 - (cH2 * gi / 4);
      ctx2.beginPath(); ctx2.moveTo(pad2.left, gy2); ctx2.lineTo(pad2.left + cW2, gy2); ctx2.stroke();
      ctx2.fillText((maxK2 * gi / 4).toFixed(2), pad2.left - 4, gy2 + 3);
    }}
    // Y-axis right (°C)
    ctx2.fillStyle = '#e74c3c'; ctx2.textAlign = 'left';
    for (var gj = 0; gj <= 4; gj++) {{
      var gy3 = pad2.top + cH2 - (cH2 * gj / 4);
      ctx2.fillText((minT2 + (maxT2 - minT2) * gj / 4).toFixed(0) + '\u00b0', pad2.left + cW2 + 4, gy3 + 3);
    }}

    // Bars (kWh)
    ctx2.fillStyle = 'rgba(52,152,219,0.6)';
    sorted.forEach(function(p, i) {{
      var x = pad2.left + (cW2 * i / n) + 1;
      var bh = cH2 * (p.kwh / maxK2);
      ctx2.fillRect(x, pad2.top + cH2 - bh, barW, bh);
    }});

    // Temperature line
    ctx2.strokeStyle = '#e74c3c'; ctx2.lineWidth = 2; ctx2.globalAlpha = 0.85;
    ctx2.beginPath();
    sorted.forEach(function(p, i) {{
      var x = pad2.left + (cW2 * (i + 0.5) / n);
      var y = pad2.top + cH2 - cH2 * (p.temp - minT2) / (maxT2 - minT2);
      if (i === 0) ctx2.moveTo(x, y); else ctx2.lineTo(x, y);
    }});
    ctx2.stroke();
    ctx2.globalAlpha = 1.0;

    // X-axis labels (date+hour) – fewer on narrow screens
    ctx2.fillStyle = muted; ctx2.font = '9px sans-serif'; ctx2.textAlign = 'center';
    var maxLabels = W2 < 400 ? 4 : (W2 < 600 ? 5 : 8);
    var step = Math.max(1, Math.floor(n / maxLabels));
    for (var li = 0; li < n; li += step) {{
      var dt = new Date(sorted[li].ts * 1000);
      var lbl = ('0'+dt.getDate()).slice(-2) + '.' + ('0'+(dt.getMonth()+1)).slice(-2);
      if (W2 >= 400) lbl += ' ' + ('0'+dt.getHours()).slice(-2) + 'h';
      var lx = pad2.left + (cW2 * (li + 0.5) / n);
      ctx2.fillText(lbl, lx, pad2.top + cH2 + 16);
    }}

    // Axis labels
    ctx2.fillStyle = '#3498db'; ctx2.font = '10px sans-serif';
    ctx2.save(); ctx2.translate(12, pad2.top + cH2 / 2); ctx2.rotate(-Math.PI/2);
    ctx2.textAlign = 'center'; ctx2.fillText('kWh', 0, 0); ctx2.restore();
    ctx2.fillStyle = '#e74c3c';
    ctx2.save(); ctx2.translate(W2 - 6, pad2.top + cH2 / 2); ctx2.rotate(Math.PI/2);
    ctx2.textAlign = 'center'; ctx2.fillText('\u00b0C', 0, 0); ctx2.restore();
  }}
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
      '<div class="card-title">' + t('web.dash.device_a', 'Device A') + '</div>' +
      '<div class="controls-row">' +
        '<select id="cmp-da">' + devOptions + '</select>' +
        '<input type="date" id="cmp-fa" value="' + monthAgo + '">' +
        '<input type="date" id="cmp-ta" value="' + today + '">' +
      '</div>' +
      '<div class="card-title" style="margin-top:8px">' + t('web.dash.device_b', 'Device B') + '</div>' +
      '<div class="controls-row">' +
        '<select id="cmp-db">' + devOptions + '</select>' +
        '<input type="date" id="cmp-fb" value="' + monthAgo + '">' +
        '<input type="date" id="cmp-tb" value="' + today + '">' +
      '</div>' +
      '<div class="controls-row" style="margin-top:8px">' +
        '<select id="cmp-unit"><option value="kWh">kWh</option><option value="eur">\u20ac</option></select>' +
        '<select id="cmp-gran"><option value="total">' + t('web.dash.gran.total', 'Total') + '</option><option value="daily">' + t('web.dash.gran.daily', 'Daily') + '</option><option value="weekly">' + t('web.dash.gran.weekly', 'Weekly') + '</option><option value="monthly">' + t('web.dash.gran.monthly', 'Monthly') + '</option></select>' +
        '<button class="btn btn-accent" onclick="loadCompare()">' + t('web.dash.compare', 'Compare') + '</button>' +
      '</div>' +
    '</div>';

  // Quick presets
  const quick = document.getElementById('cmp-quick');
  const presets = [
    ['month', t('web.costs.month', 'Month')],
    ['quarter', t('web.dash.quarter', 'Quarter')],
    ['halfyear', t('web.dash.halfyear', 'Half Year')],
    ['year', t('web.costs.year', 'Year')],
  ];
  let qhtml = '<div class="controls-row">';
  presets.forEach(function(p) {{
    qhtml += '<button class="btn btn-outline btn-sm" onclick="loadComparePreset(\\'' + p[0] + '\\')">' + p[1] + '</button>';
  }});
  qhtml += '<label style="display:inline-flex;align-items:center;gap:4px;margin-left:12px;font-size:12px;cursor:pointer">' +
    '<input type="checkbox" id="cmp-spot" onchange="loadCompare()"> \u26a1 ' + t('compare.vs_dynamic', 'vs. Dynamic Tariff') + '</label>';
  qhtml += '</div>';
  quick.innerHTML = qhtml;
}}

async function loadComparePreset(preset) {{
  const result = document.getElementById('cmp-result');
  result.innerHTML = '<p class="loading-msg">' + t('web.loading', 'Loading\u2026') + '</p>';
  try {{
    // Auto-granularity per preset: month→daily, quarter→weekly, halfyear/year→monthly
    const autoGran = preset === 'month' ? 'daily'
      : preset === 'quarter' ? 'weekly'
      : (preset === 'halfyear' || preset === 'year') ? 'monthly'
      : ((document.getElementById('cmp-gran')||{{}}).value||'total');
    const spotChk = document.getElementById('cmp-spot');
    const spotMode = spotChk && spotChk.checked ? '&mode=spot' : '';
    const url = '/api/compare?preset=' + preset +
      '&device_a=' + encodeURIComponent((document.getElementById('cmp-da')||{{}}).value||'') +
      '&device_b=' + encodeURIComponent((document.getElementById('cmp-db')||{{}}).value||'') +
      '&unit=' + ((document.getElementById('cmp-unit')||{{}}).value||'kWh') +
      '&gran=' + autoGran + spotMode;
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
  result.innerHTML = '<p class="loading-msg">' + t('web.loading', 'Loading\u2026') + '</p>';
  try {{
    const da = document.getElementById('cmp-da').value;
    const fa = document.getElementById('cmp-fa').value;
    const ta = document.getElementById('cmp-ta').value;
    const db = document.getElementById('cmp-db').value;
    const fb = document.getElementById('cmp-fb').value;
    const tb = document.getElementById('cmp-tb').value;
    const unit = document.getElementById('cmp-unit').value;
    const gran = document.getElementById('cmp-gran').value;
    const spotChk2 = document.getElementById('cmp-spot');
    const spotMode2 = spotChk2 && spotChk2.checked ? '&mode=spot' : '';
    const url = '/api/compare?device_a=' + encodeURIComponent(da) +
      '&from_a=' + fa + '&to_a=' + ta +
      '&device_b=' + encodeURIComponent(db) +
      '&from_b=' + fb + '&to_b=' + tb +
      '&unit=' + unit + '&gran=' + gran + spotMode2;
    const r = await fetch(url);
    if (!r.ok) throw new Error(r.status);
    const data = await r.json();
    renderCompare(data, result);
  }} catch(e) {{
    result.innerHTML = '<p class="error-msg">Error: ' + e.message + '</p>';
  }}
}}

function renderCompare(data, el) {{
  if (!data) {{ el.innerHTML = '<p class="info-msg">' + t('web.no_data', 'No data.') + '</p>'; return; }}
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

  // Bar chart — always shown (series for daily/monthly, totals comparison for "total" granularity)
  html += '<div class="card"><canvas class="bar-chart" id="cmp-canvas"></canvas></div>';
  el.innerHTML = html;

  const canvas = document.getElementById('cmp-canvas');
  if (data.values_a && data.values_b && data.labels && data.labels.length > 1) {{
    drawBars(canvas, data.labels, [
      {{ values: data.values_a, color: 'rgba(37,99,235,0.75)', label: data.label_a || data.device_a || 'A' }},
      {{ values: data.values_b, color: 'rgba(217,119,6,0.75)', label: data.label_b || data.device_b || 'B' }}
    ], {{ unit: unit }});
  }} else {{
    // Total-only: simple side-by-side comparison of A vs B
    const lA = data.label_a || data.device_a || 'A';
    const lB = data.label_b || data.device_b || 'B';
    drawBars(canvas, [lA, lB], [
      {{ values: [ta, 0], color: 'rgba(37,99,235,0.75)', label: lA }},
      {{ values: [0, tb], color: 'rgba(217,119,6,0.75)', label: lB }}
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
  el.innerHTML = '<p class="loading-msg">' + t('web.loading', 'Loading\u2026') + '</p>';
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
    (enabled ? t('web.dash.anomaly_enabled', 'Enabled') : t('web.dash.anomaly_disabled', 'Disabled')) + '</span>' +
    (data.model ? ' <span class="badge badge-yellow">' + esc(data.model) + '</span>' : '') +
    '</div>';

  const events = data.events || [];
  if (events.length === 0) {{
    html += '<p class="info-msg">' + t('web.dash.no_anomalies', 'No anomaly events found.') + '</p>';
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
        '<div class="event-type">' + t('anomaly.type.' + (ev.anomaly_type || ev.type || ''), (ev.anomaly_type || ev.type || 'Anomaly')) + '</div>' +
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
   SIMPLE CANVAS BAR CHART (like CO₂ tab)
────────────────────────────────────────────── */
function _drawBarChart(canvasId, labels, values, options) {{
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);
  const W = rect.width, H = rect.height;
  const pad = options.pad || {{top: 14, right: 14, bottom: 28, left: 44}};
  const cW = W - pad.left - pad.right;
  const cH = H - pad.top - pad.bottom;
  if (cW <= 0 || cH <= 0) return;
  const maxV = Math.max.apply(null, values.concat([0.01])) * 1.15;
  const colors = options.colors || values.map(function() {{ return '#3498db'; }});
  const barW = Math.max(1, (cW / values.length) - 2);
  const fg = getComputedStyle(document.body).getPropertyValue('--muted') || '#999';
  const border = getComputedStyle(document.body).getPropertyValue('--border') || '#e0e0e0';
  // Grid
  ctx.strokeStyle = border; ctx.lineWidth = 0.5;
  for (let i = 0; i <= 4; i++) {{
    const y = pad.top + cH - (cH * i / 4);
    ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(pad.left + cW, y); ctx.stroke();
    ctx.fillStyle = fg; ctx.font = '10px sans-serif'; ctx.textAlign = 'right';
    ctx.fillText((maxV * i / 4).toFixed(options.decimals !== undefined ? options.decimals : 1), pad.left - 4, y + 3);
  }}
  // Bars
  values.forEach(function(v, i) {{
    const x = pad.left + (cW * i / values.length) + 1;
    const h = (v / maxV) * cH;
    ctx.fillStyle = colors[i] || '#3498db';
    ctx.fillRect(x, pad.top + cH - h, barW, h);
  }});
  // X labels
  ctx.fillStyle = fg; ctx.font = '9px sans-serif'; ctx.textAlign = 'center';
  const step = Math.max(1, Math.floor(labels.length / Math.min(labels.length, 12)));
  labels.forEach(function(lbl, i) {{
    if (i % step === 0 || i === labels.length - 1) {{
      const x = pad.left + (cW * i / values.length) + barW / 2;
      ctx.fillText(lbl, x, pad.top + cH + 14);
    }}
  }});
  // Threshold line
  if (options.threshold !== undefined) {{
    const y = pad.top + cH - (cH * options.threshold / maxV);
    ctx.setLineDash([4, 3]); ctx.strokeStyle = options.thresholdColor || '#e53935'; ctx.lineWidth = 1.5;
    ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(pad.left + cW, y); ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = options.thresholdColor || '#e53935'; ctx.font = '9px sans-serif'; ctx.textAlign = 'left';
    ctx.fillText(options.thresholdLabel || '', pad.left + cW + 2, y + 3);
  }}
  // Title
  if (options.title) {{
    ctx.fillStyle = fg; ctx.font = 'bold 11px sans-serif'; ctx.textAlign = 'left';
    ctx.fillText(options.title, pad.left, pad.top - 2);
  }}
}}

/* ──────────────────────────────────────────────
   FORECAST TAB
────────────────────────────────────────────── */
async function loadForecast() {{
  const sel = document.getElementById('forecast-device');
  if (!sel.options.length) {{
    const devs = typeof DEVICES !== 'undefined' ? DEVICES : [];
    devs.forEach(function(d) {{
      const o = document.createElement('option');
      o.value = d.key || d[0] || ''; o.textContent = d.name || d[1] || '';
      sel.appendChild(o);
    }});
    if (sel.options.length) sel.selectedIndex = 0;
  }}
  const dk = sel.value || '';
  const cont = document.getElementById('forecast-cards');
  cont.innerHTML = '<p class="loading-msg">Loading\u2026</p>';
  try {{
    const r = await fetch('/api/forecast?device_key=' + encodeURIComponent(dk));
    if (!r.ok) throw new Error(r.status);
    const d = await r.json();
    if (d.no_data) {{ cont.innerHTML = '<p class="info-msg">Not enough data for forecast (min. 3 days).</p>'; return; }}
    renderForecast(d);
  }} catch(e) {{
    cont.innerHTML = '<p class="error-msg">Error: ' + e.message + '</p>';
  }}
}}
function renderForecast(d) {{
  const trendStr = d.trend_pct_per_month > 0.5 ? '\u2197 +' + d.trend_pct_per_month.toFixed(1) + '%/M' : d.trend_pct_per_month < -0.5 ? '\u2198 ' + d.trend_pct_per_month.toFixed(1) + '%/M' : '\u2192 stable';
  let html = '<div class="card" style="margin-bottom:10px"><div class="card-title">' + esc(d.device_name || '') + '</div><div class="metric-grid">' +
    metricCardHtml('\u2300 Daily', (d.avg_daily_kwh||0).toFixed(1) + ' kWh', '') +
    metricCardHtml('Trend', trendStr, '') +
    metricCardHtml('Next Month', (d.forecast_next_month_kwh||0).toFixed(0) + ' kWh', fmt(d.forecast_next_month_cost||0, 2, '\u20ac')) +
    metricCardHtml('Next Year', (d.forecast_year_kwh||0).toFixed(0) + ' kWh', fmt(d.forecast_year_cost||0, 0, '\u20ac')) +
    '</div></div>';
  // Main chart canvas
  html += '<div class="card"><canvas id="fc-main-chart" height="180" style="width:100%"></canvas></div>';
  // Profile charts
  html += '<div class="card-grid">' +
    '<div class="card"><canvas id="fc-weekday-chart" height="140" style="width:100%"></canvas></div>' +
    '<div class="card"><canvas id="fc-hourly-chart" height="140" style="width:100%"></canvas></div>' +
    '</div>';
  document.getElementById('forecast-cards').innerHTML = html;
  // Draw main chart (history + forecast bars)
  requestAnimationFrame(function() {{
    const allDates = (d.history_dates || []).concat(d.forecast_dates || []);
    const allVals = (d.history_kwh || []).concat(d.forecast_kwh || []);
    const hLen = (d.history_dates || []).length;
    const colors = allVals.map(function(_, i) {{ return i < hLen ? '#3498db' : 'rgba(231,76,60,0.7)'; }});
    const labels = allDates.map(function(dt) {{ return dt.substring(5); }});
    _drawBarChart('fc-main-chart', labels, allVals, {{ colors: colors, title: 'kWh/day \u2013 History + Forecast', decimals: 1 }});
    // Weekday profile
    if (d.weekday_profile) {{
      const days = ['Mo','Tu','We','Th','Fr','Sa','Su'];
      const vals = days.map(function(_, i) {{ return d.weekday_profile[String(i)] || d.weekday_profile[i] || 1.0; }});
      const wdColors = vals.map(function(v) {{ return v > 1.1 ? '#e74c3c' : v < 0.9 ? '#27ae60' : '#3498db'; }});
      _drawBarChart('fc-weekday-chart', days, vals, {{ colors: wdColors, title: 'Weekday Profile', threshold: 1.0, thresholdColor: 'rgba(128,128,128,0.4)', thresholdLabel: '1.0', decimals: 2 }});
    }}
    // Hourly profile
    if (d.hourly_profile) {{
      const hrs = Array.from({{ length: 24 }}, function(_, i) {{ return String(i); }});
      const vals = hrs.map(function(h) {{ return d.hourly_profile[h] || d.hourly_profile[parseInt(h)] || 1.0; }});
      const hColors = vals.map(function(v) {{ return v > 1.3 ? '#e74c3c' : v > 1.1 ? '#f39c12' : v < 0.7 ? '#27ae60' : '#3498db'; }});
      _drawBarChart('fc-hourly-chart', hrs, vals, {{ colors: hColors, title: '24h Profile', threshold: 1.0, thresholdColor: 'rgba(128,128,128,0.4)', thresholdLabel: '1.0', decimals: 2 }});
    }}
  }});
}}

/* ──────────────────────────────────────────────
   STANDBY TAB
────────────────────────────────────────────── */
async function loadStandby() {{
  const cont = document.getElementById('standby-cards');
  cont.innerHTML = '<p class="loading-msg">Loading\u2026</p>';
  try {{
    const r = await fetch('/api/standby');
    if (!r.ok) throw new Error(r.status);
    const data = await r.json();
    renderStandby(data);
  }} catch(e) {{
    cont.innerHTML = '<p class="error-msg">Error: ' + e.message + '</p>';
  }}
}}
function renderStandby(d) {{
  let html = '<div class="card" style="margin-bottom:10px"><div class="card-title">Standby Summary</div><div class="metric-grid">' +
    metricCardHtml('Annual Cost', (d.total_annual_standby_cost||0).toFixed(0) + ' \u20ac', 'Standby total') +
    metricCardHtml('Annual kWh', (d.total_annual_standby_kwh||0).toFixed(0) + ' kWh', 'Standby total') +
    '</div></div>';
  document.getElementById('standby-cards').innerHTML = html;
  const wrap = document.getElementById('standby-table-wrap');
  if (!d.devices || !d.devices.length) {{
    let diagHtml = '<p class="info-msg">' + t('web.standby.no_data', 'No standby data available. The analysis requires at least 6 hours of data in the last {days} days.').replace('{{days}}', d.analysis_days || 30) + '</p>';
    if (d.diagnostic) {{
      diagHtml += '<div style="font-size:11px;color:var(--muted);margin-top:8px">';
      Object.keys(d.diagnostic).forEach(function(k) {{
        var di = d.diagnostic[k];
        diagHtml += '<div>' + esc(di.name) + ': ' + di.hourly_rows + ' hourly / ' + di.sample_rows + ' samples</div>';
      }});
      diagHtml += '</div>';
    }}
    wrap.innerHTML = '<div class="card">' + diagHtml + '</div>';
    return;
  }}
  // Device cards
  let cards = '<div class="card-grid">';
  d.devices.forEach(function(dev) {{
    const riskColor = dev.risk === 'high' ? '#dc2626' : dev.risk === 'medium' ? '#d97706' : '#16a34a';
    cards += '<div class="card">' +
      '<div class="card-title">' + esc(dev.device_name) + ' <span style="color:' + riskColor + '">\u25cf ' + dev.risk.toUpperCase() + '</span></div>' +
      '<div class="metric-grid">' +
      metricCardHtml('Base load', dev.base_load_w + ' W', '') +
      metricCardHtml('kWh/year', dev.annual_standby_kwh + '', '') +
      metricCardHtml('\u20ac/year', dev.annual_standby_cost + ' \u20ac', '') +
      metricCardHtml('Share', dev.standby_share_pct + '%', '') +
      '</div></div>';
  }});
  cards += '</div>';
  // Charts
  cards += '<div class="card-grid">' +
    '<div class="card"><canvas id="standby-cost-chart" height="160" style="width:100%"></canvas></div>' +
    '<div class="card"><canvas id="standby-24h-chart" height="160" style="width:100%"></canvas></div>' +
    '</div>';
  wrap.innerHTML = cards;
  requestAnimationFrame(function() {{
    // Cost bar chart
    const names = d.devices.map(function(x) {{ return x.device_name.substring(0, 12); }});
    const costs = d.devices.map(function(x) {{ return x.annual_standby_cost; }});
    const cColors = d.devices.map(function(x) {{ return x.risk === 'high' ? '#dc2626' : x.risk === 'medium' ? '#d97706' : '#16a34a'; }});
    _drawBarChart('standby-cost-chart', names, costs, {{ colors: cColors, title: 'Standby \u20ac/year', decimals: 0 }});
    // 24h profile first device
    if (d.devices.length && d.devices[0].hourly_profile) {{
      const hp = d.devices[0].hourly_profile;
      const hrs = Array.from({{ length: 24 }}, function(_, i) {{ return String(i); }});
      const hColors = hrs.map(function(h) {{ return parseInt(h) <= 5 || parseInt(h) >= 22 ? '#34495e' : '#3498db'; }});
      _drawBarChart('standby-24h-chart', hrs, hp, {{ colors: hColors, title: '24h: ' + d.devices[0].device_name, threshold: d.devices[0].base_load_w, thresholdColor: '#dc2626', thresholdLabel: 'Standby', decimals: 0 }});
    }}
  }});
}}

/* ──────────────────────────────────────────────
   SANKEY / ENERGY FLOW TAB
────────────────────────────────────────────── */
let _sankeyPeriod = 'today';
function initSankeyPeriods() {{
  const el = document.getElementById('sankey-periods');
  if (el.children.length) return;
  const labels = {{ today: 'Today', week: 'Week', month: 'Month', year: 'Year' }};
  ['today','week','month','year'].forEach(function(p) {{
    const btn = document.createElement('button');
    btn.className = 'btn btn-outline btn-sm' + (p === _sankeyPeriod ? ' btn-accent' : '');
    btn.textContent = labels[p] || p;
    btn.addEventListener('click', function() {{
      _sankeyPeriod = p;
      el.querySelectorAll('.btn').forEach(function(b) {{ b.classList.remove('btn-accent'); }});
      btn.classList.add('btn-accent');
      loadSankey();
    }});
    el.appendChild(btn);
  }});
}}
async function loadSankey() {{
  initSankeyPeriods();
  const cont = document.getElementById('sankey-cards');
  cont.innerHTML = '<p class="loading-msg">Loading\u2026</p>';
  try {{
    const r = await fetch('/api/sankey?period=' + _sankeyPeriod);
    if (!r.ok) throw new Error(r.status);
    const data = await r.json();
    renderSankey(data);
  }} catch(e) {{
    cont.innerHTML = '<p class="error-msg">Error: ' + e.message + '</p>';
  }}
}}
function renderSankey(d) {{
  let html = '<div class="card" style="margin-bottom:10px"><div class="card-title">Energy Flow</div><div class="metric-grid">' +
    metricCardHtml('Grid', (d.grid_import_kwh||0).toFixed(2) + ' kWh', '') +
    metricCardHtml('Total', (d.total_consumption_kwh||0).toFixed(2) + ' kWh', '') +
    metricCardHtml('PV', (d.pv_production_kwh||0).toFixed(2) + ' kWh', '') +
    metricCardHtml('Feed-in', (d.feed_in_kwh||0).toFixed(2) + ' kWh', '') +
    '</div></div>';
  if (!d.sankey || !d.sankey.node || !d.sankey.link || !d.sankey.link.value || d.sankey.link.value.length === 0) {{
    html += '<div class="card"><p class="info-msg">No energy flow data for this period.</p></div>';
    document.getElementById('sankey-cards').innerHTML = html;
    return;
  }}
  html += '<div class="card" style="padding:8px"><canvas id="sankey-flow-canvas" style="width:100%;height:340px"></canvas></div>';
  document.getElementById('sankey-cards').innerHTML = html;
  requestAnimationFrame(function() {{ _drawSankeyFlow('sankey-flow-canvas', d); }});
}}

function _drawSankeyFlow(canvasId, d) {{
  const cv = document.getElementById(canvasId);
  if (!cv) return;
  const dpr = window.devicePixelRatio || 1;
  const W = cv.offsetWidth;
  const H = cv.offsetHeight || 340;
  cv.width = W * dpr;
  cv.height = H * dpr;
  const ctx = cv.getContext('2d');
  ctx.scale(dpr, dpr);

  const isDark = document.documentElement.dataset.theme === 'dark';
  const fg = isDark ? '#e0e0e0' : '#333';
  const bg = isDark ? '#111' : '#fff';

  const nodeLabels = d.sankey.node.label || [];
  const nodeColors = d.sankey.node.color || [];
  const linkSrc = d.sankey.link.source || [];
  const linkTgt = d.sankey.link.target || [];
  const linkVal = d.sankey.link.value || [];

  // Build sources and consumers from link data
  const houseIdx = nodeLabels.indexOf('House');
  const sources = [];
  const consumers = [];
  const feedIn = [];
  linkSrc.forEach(function(s, i) {{
    if (linkTgt[i] === houseIdx && linkVal[i] > 0.001) {{
      sources.push({{ name: nodeLabels[s], kwh: linkVal[i], color: nodeColors[s] || '#e53935' }});
    }}
    if (s === houseIdx && linkVal[i] > 0.001) {{
      const tName = nodeLabels[linkTgt[i]];
      if (tName === 'Feed-in') {{
        feedIn.push({{ name: tName, kwh: linkVal[i], color: nodeColors[linkTgt[i]] || '#43A047' }});
      }} else {{
        consumers.push({{ name: tName, kwh: linkVal[i], color: nodeColors[linkTgt[i]] || '#3498db' }});
      }}
    }}
    // PV → Feed-in (not through house)
    if (s !== houseIdx && linkTgt[i] !== houseIdx && linkVal[i] > 0.001) {{
      const tName = nodeLabels[linkTgt[i]];
      if (tName === 'Feed-in') {{
        feedIn.push({{ name: tName, kwh: linkVal[i], color: nodeColors[linkTgt[i]] || '#43A047' }});
      }}
    }}
  }});
  sources.sort(function(a, b) {{ return b.kwh - a.kwh; }});
  consumers.sort(function(a, b) {{ return b.kwh - a.kwh; }});
  const topConsumers = consumers.slice(0, 10);

  const total = Math.max(d.total_consumption_kwh || 0.01, 0.01);

  // Layout constants (in pixels)
  const PAD_X = 10, PAD_Y = 30;
  const SRC_X = PAD_X, SRC_W = W * 0.15;
  const HOUSE_X = W * 0.30, HOUSE_W = W * 0.16;
  const TGT_X = W * 0.58, TGT_W = W * 0.40;
  const TOP = PAD_Y + 14, BOT = H - 20;
  const usable = BOT - TOP;
  const GAP = 4;

  // Helper: draw rounded rect
  function roundRect(x, y, w, h, r) {{
    r = Math.min(r, w / 2, h / 2);
    ctx.beginPath();
    ctx.moveTo(x + r, y);
    ctx.lineTo(x + w - r, y);
    ctx.quadraticCurveTo(x + w, y, x + w, y + r);
    ctx.lineTo(x + w, y + h - r);
    ctx.quadraticCurveTo(x + w, y + h, x + w - r, y + h);
    ctx.lineTo(x + r, y + h);
    ctx.quadraticCurveTo(x, y + h, x, y + h - r);
    ctx.lineTo(x, y + r);
    ctx.quadraticCurveTo(x, y, x + r, y);
    ctx.closePath();
  }}

  // Helper: draw bezier flow band
  function flowBand(x0, y0, h0, x1, y1, h1, color, alpha) {{
    const dx = (x1 - x0) * 0.4;
    ctx.save();
    ctx.globalAlpha = alpha || 0.3;
    ctx.fillStyle = color;
    ctx.beginPath();
    ctx.moveTo(x0, y0 - h0 / 2);
    ctx.bezierCurveTo(x0 + dx, y0 - h0 / 2, x1 - dx, y1 - h1 / 2, x1, y1 - h1 / 2);
    ctx.lineTo(x1, y1 + h1 / 2);
    ctx.bezierCurveTo(x1 - dx, y1 + h1 / 2, x0 + dx, y0 + h0 / 2, x0, y0 + h0 / 2);
    ctx.closePath();
    ctx.fill();
    ctx.restore();
  }}

  // --- Column headers ---
  ctx.font = 'bold 11px sans-serif';
  ctx.fillStyle = fg;
  ctx.textAlign = 'center';
  ctx.fillText('Sources', SRC_X + SRC_W / 2, TOP - 6);
  ctx.fillText('Consumers', TGT_X + TGT_W / 2, TOP - 6);

  // --- Source nodes ---
  const nSrc = Math.max(sources.length, 1);
  const srcTotalH = usable - (nSrc - 1) * GAP;
  const srcCy = [], srcH = [];
  let yCursor = TOP;
  for (let i = 0; i < sources.length; i++) {{
    const s = sources[i];
    const h = Math.max(20, (s.kwh / total) * srcTotalH);
    const cy = yCursor + h / 2;
    srcCy.push(cy); srcH.push(h);
    ctx.fillStyle = s.color;
    ctx.globalAlpha = 0.92;
    roundRect(SRC_X, yCursor, SRC_W, h, 6);
    ctx.fill();
    ctx.globalAlpha = 1;
    // Label
    ctx.fillStyle = '#fff';
    ctx.textAlign = 'center';
    if (h > 28) {{
      ctx.font = 'bold 9px sans-serif';
      ctx.fillText(s.name, SRC_X + SRC_W / 2, cy - 2);
      ctx.font = '8px sans-serif';
      const pct = (s.kwh / total * 100).toFixed(0);
      ctx.fillText(s.kwh.toFixed(1) + ' kWh (' + pct + '%)', SRC_X + SRC_W / 2, cy + 10);
    }} else {{
      ctx.font = 'bold 8px sans-serif';
      ctx.fillText(s.name + ' ' + s.kwh.toFixed(1), SRC_X + SRC_W / 2, cy + 3);
    }}
    yCursor += h + GAP;
  }}

  // --- House node ---
  const houseCy = (TOP + BOT) / 2;
  const houseH = Math.min(60, usable * 0.22);
  ctx.fillStyle = isDark ? 'rgba(21,101,192,0.6)' : 'rgba(227,242,253,0.8)';
  roundRect(HOUSE_X, houseCy - houseH / 2, HOUSE_W, houseH, 10);
  ctx.fill();
  ctx.strokeStyle = '#1976D2';
  ctx.lineWidth = 1.5;
  ctx.stroke();
  ctx.fillStyle = fg;
  ctx.globalAlpha = 0.7;
  ctx.font = '9px sans-serif';
  ctx.textAlign = 'center';
  ctx.fillText('Total', HOUSE_X + HOUSE_W / 2, houseCy - 4);
  ctx.globalAlpha = 1;
  ctx.font = 'bold 12px sans-serif';
  ctx.fillText((d.total_consumption_kwh || 0).toFixed(1) + ' kWh', HOUSE_X + HOUSE_W / 2, houseCy + 12);

  // --- Consumer nodes ---
  const nTgt = Math.max(topConsumers.length, 1);
  const tgtTotalH = usable - (nTgt - 1) * GAP;
  const tgtCy = [], tgtH = [];
  yCursor = TOP;
  for (let i = 0; i < topConsumers.length; i++) {{
    const c = topConsumers[i];
    const h = Math.max(18, (c.kwh / total) * tgtTotalH);
    const cy = yCursor + h / 2;
    tgtCy.push(cy); tgtH.push(h);
    ctx.fillStyle = c.color;
    ctx.globalAlpha = 0.88;
    roundRect(TGT_X, yCursor, TGT_W, h, 6);
    ctx.fill();
    ctx.globalAlpha = 1;
    ctx.fillStyle = '#fff';
    ctx.textAlign = 'center';
    ctx.font = 'bold 9px sans-serif';
    const label = c.name.length > 20 ? c.name.substring(0, 18) + '..' : c.name;
    const pct = (c.kwh / total * 100).toFixed(0);
    ctx.fillText(label + '   ' + c.kwh.toFixed(1) + ' kWh (' + pct + '%)', TGT_X + TGT_W / 2, cy + 3);
    yCursor += h + GAP;
  }}

  // --- Feed-in label ---
  if (feedIn.length && feedIn[0].kwh > 0.001) {{
    ctx.fillStyle = '#43A047';
    ctx.font = 'bold 10px sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText('Feed-in: ' + feedIn[0].kwh.toFixed(2) + ' kWh', HOUSE_X + HOUSE_W / 2, BOT + 12);
  }}

  // --- Flow bands: Sources → House ---
  let hBandY = houseCy - houseH / 2;
  for (let i = 0; i < sources.length; i++) {{
    const bHouse = Math.max(4, (sources[i].kwh / total) * houseH);
    const bandCy = hBandY + bHouse / 2;
    flowBand(SRC_X + SRC_W, srcCy[i], srcH[i] * 0.85, HOUSE_X, bandCy, bHouse, sources[i].color, 0.28);
    hBandY += bHouse;
  }}

  // --- Flow bands: House → Consumers ---
  hBandY = houseCy - houseH / 2;
  for (let i = 0; i < topConsumers.length; i++) {{
    const bHouse = Math.max(4, (topConsumers[i].kwh / total) * houseH);
    const bandCy = hBandY + bHouse / 2;
    flowBand(HOUSE_X + HOUSE_W, bandCy, bHouse, TGT_X, tgtCy[i], tgtH[i] * 0.85, topConsumers[i].color, 0.28);
    hBandY += bHouse;
  }}
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
   EV CHARGER TAB
────────────────────────────────────────────── */
let _evLastCoords = null;
let _evApiKey = localStorage.getItem('sea_ocm_key') || '';

function _evInitKeyRow() {{
  const row = document.getElementById('ev-apikey-row');
  const inp = document.getElementById('ev-apikey');
  if (!row || !inp) return;
  if (!_evApiKey) {{ row.style.display = 'block'; }}
  inp.value = _evApiKey;
}}
function _evSaveKey() {{
  const inp = document.getElementById('ev-apikey');
  if (!inp) return;
  _evApiKey = inp.value.trim();
  localStorage.setItem('sea_ocm_key', _evApiKey);
  document.getElementById('ev-apikey-row').style.display = 'none';
  loadEv();
}}
function _evShowKeyRow() {{
  const row = document.getElementById('ev-apikey-row');
  if (row) row.style.display = 'block';
}}

async function _evGeocode(city) {{
  // Use Nominatim (OpenStreetMap) for free geocoding
  const r = await fetch('https://nominatim.openstreetmap.org/search?format=json&limit=1&q=' + encodeURIComponent(city));
  if (!r.ok) return null;
  const data = await r.json();
  if (data && data.length > 0) return {{ lat: parseFloat(data[0].lat), lon: parseFloat(data[0].lon) }};
  return null;
}}

async function loadEv() {{
  const wrap = document.getElementById('ev-grid-wrap');
  if (!wrap) return;
  wrap.innerHTML = '<p class="loading-msg">' + t('web.ev.loading', 'Loading chargers\u2026') + '</p>';

  const cityInput = document.getElementById('ev-city');
  const cityVal = (cityInput ? cityInput.value.trim() : '');

  // Priority: 1) City input, 2) GPS, 3) Cached coords
  if (cityVal) {{
    const geo = await _evGeocode(cityVal);
    if (geo) {{
      _evLastCoords = geo;
    }} else {{
      wrap.innerHTML = '<p class="error-msg">' + t('web.ev.city_not_found', 'City not found.') + '</p>';
      return;
    }}
  }} else if (!_evLastCoords) {{
    // Try GPS
    if (navigator.geolocation) {{
      try {{
        const pos = await new Promise(function(resolve, reject) {{
          navigator.geolocation.getCurrentPosition(resolve, reject, {{
            enableHighAccuracy: true, timeout: 10000, maximumAge: 60000
          }});
        }});
        _evLastCoords = {{ lat: pos.coords.latitude, lon: pos.coords.longitude }};
      }} catch(e) {{
        wrap.innerHTML = '<p class="info-msg">' + t('web.ev.enter_city', 'Enter a city name or allow GPS access.') + '</p>';
        return;
      }}
    }} else {{
      wrap.innerHTML = '<p class="info-msg">' + t('web.ev.enter_city', 'Enter a city name or allow GPS access.') + '</p>';
      return;
    }}
  }}

  const radius = document.getElementById('ev-radius').value || '500';
  const minKw = document.getElementById('ev-minkw').value || '0';
  const plugFilter = document.getElementById('ev-plug').value || '';
  let url = '/api/ev_chargers?lat=' + _evLastCoords.lat + '&lon=' + _evLastCoords.lon + '&radius=' + radius;
  if (_evApiKey) url += '&key=' + encodeURIComponent(_evApiKey);
  if (minKw !== '0') url += '&min_kw=' + minKw;
  if (plugFilter) url += '&plug=' + encodeURIComponent(plugFilter);
  try {{
    const r = await fetch(url);
    if (!r.ok) throw new Error(r.status);
    const data = await r.json();
    if (!data.ok) {{
      if ((data.error || '').indexOf('403') !== -1 || (data.error || '').indexOf('Forbidden') !== -1) {{
        _evShowKeyRow();
        wrap.innerHTML = '<p class="error-msg">' + t('web.ev.need_key', 'API key required. Get a free key at openchargemap.org/site/develop/api') + '</p>';
        return;
      }}
      throw new Error(data.error || 'unknown');
    }}
    _evRenderGrid(data.stations || [], data.sources || []);
  }} catch(e) {{
    if (e.message === '403') {{
      _evShowKeyRow();
      wrap.innerHTML = '<p class="error-msg">' + t('web.ev.need_key', 'API key required. Get a free key at openchargemap.org/site/develop/api') + '</p>';
    }} else {{
      wrap.innerHTML = '<p class="error-msg">Error: ' + esc(e.message) + '</p>';
    }}
  }}
}}

function _evRenderGrid(stations, sources) {{
  const wrap = document.getElementById('ev-grid-wrap');
  if (!stations.length) {{
    wrap.innerHTML = '<p class="info-msg">' + t('web.ev.no_results', 'No chargers found') + '</p>';
    return;
  }}
  wrap._evStations = stations;
  const srcMap = {{ocm:'OpenChargeMap', bna:'Bundesnetzagentur', osm:'OpenStreetMap'}};
  const srcLabel = (sources || []).map(function(s) {{ return srcMap[s] || s; }}).join(' + ');
  let html = '<div style="font-size:10px;color:var(--muted);margin-bottom:4px">' + stations.length + ' Stationen' + (srcLabel ? ' \xb7 ' + srcLabel : '') + '</div>';
  html += '<div class="ev-grid">';
  stations.forEach(function(s) {{
    const cls = s.status === 'available' ? 'ev-green' : s.status === 'occupied' ? 'ev-yellow' : s.status === 'unavailable' ? 'ev-red' : 'ev-gray';
    const distLabel = s.distance_m < 1000 ? s.distance_m + ' m' : (s.distance_m / 1000).toFixed(1) + ' km';
    let statusLabel;
    if (s.status === 'available') statusLabel = s.free_connectors + '/' + s.total_connectors + ' ' + t('web.ev.free', 'free');
    else if (s.status === 'occupied') statusLabel = '0/' + s.total_connectors + ' ' + t('web.ev.free', 'free');
    else if (s.status === 'unavailable') statusLabel = t('web.ev.unavailable', 'unavailable');
    else statusLabel = s.total_connectors + ' ' + t('web.ev.connectors', 'Connectors');
    var srcBadge = s.source === 'enbw' ? ' \u26a1' : '';
    html += '<div class="ev-brick ' + cls + '" onclick="_evShowDetail(' + s.id + ')">' +
      '<div class="ev-brick-name">' + esc(s.name || '?') + '</div>' +
      '<div class="ev-brick-dist">' + distLabel + '</div>' +
      '<div class="ev-brick-info">' + statusLabel + srcBadge + '</div>' +
      '</div>';
  }});
  html += '</div>';
  wrap.innerHTML = html;
}}

function _evShowDetail(stationId) {{
  const wrap = document.getElementById('ev-grid-wrap');
  const stations = wrap._evStations || [];
  const s = stations.find(function(x) {{ return x.id === stationId; }});
  if (!s) return;

  document.getElementById('ev-detail-title').textContent = s.name || 'Station #' + s.id;
  const distLabel = s.distance_m < 1000 ? s.distance_m + ' m' : (s.distance_m / 1000).toFixed(1) + ' km';
  let body = '<div style="font-size:12px;color:var(--muted);margin-bottom:6px">' + esc(s.address || '') + '</div>';
  body += '<div style="font-size:12px;margin-bottom:10px">' + t('web.ev.distance', 'Distance') + ': <b>' + distLabel + '</b></div>';
  body += '<div style="font-weight:700;margin-bottom:6px">' + t('web.ev.connectors', 'Connectors') + ' (' + s.total_connectors + ')</div>';
  body += '<div class="ev-conn-grid">';
  (s.connectors || []).forEach(function(c) {{
    const cls = c.status === 'free' ? 'ev-green' : c.status === 'occupied' ? 'ev-yellow' : c.status === 'unavailable' ? 'ev-red' : 'ev-gray';
    const statusTxt = c.status === 'free' ? t('web.ev.free', 'free') : c.status === 'occupied' ? t('web.ev.occupied', 'occupied') : c.status === 'unavailable' ? t('web.ev.unavailable', 'unavailable') : t('web.ev.unknown', 'unknown');
    let since = '';
    if (c.status_since) {{
      const d = new Date(c.status_since);
      if (!isNaN(d.getTime())) since = '<br><span style="font-size:10px;opacity:0.85">' + t('web.ev.since', 'since') + ' ' + d.toLocaleDateString() + ' ' + d.toLocaleTimeString([], {{hour:'2-digit',minute:'2-digit'}}) + '</span>';
    }}
    body += '<div class="ev-conn-brick ' + cls + '"><b>' + esc(c.type || '?') + '</b>' + (c.kw ? '<br>' + c.kw + ' kW' : '') + '<br>' + statusTxt + since + '</div>';
  }});
  body += '</div>';

  document.getElementById('ev-detail-body').innerHTML = body;
  document.getElementById('ev-detail-modal').classList.add('open');
}}

/* ──────────────────────────────────────────────
   EXPORT PANE
────────────────────────────────────────────── */
let _expInited = false;
let _expJobsTimer = null;

function _expFileIcon(name) {{
  const n = (name||'').toLowerCase();
  if (n.endsWith('.pdf')) return '📄';
  if (n.endsWith('.xlsx') || n.endsWith('.xls')) return '📊';
  if (n.endsWith('.zip')) return '📦';
  if (n.endsWith('.png') || n.endsWith('.jpg') || n.endsWith('.jpeg') || n.endsWith('.svg')) return '🖼️';
  return '📎';
}}

function _expRenderFileCard(f) {{
  const icon = _expFileIcon(f.name);
  const url = f.url || '';
  const name = f.name || 'file';
  const ext = name.split('.').pop().toUpperCase();
  return `<div class="exp-file-card">
    <div class="exp-file-icon">${{icon}}</div>
    <div class="exp-file-info">
      <div class="exp-file-name">${{name}}</div>
      <div class="exp-file-meta">${{ext}}</div>
    </div>
    <a class="exp-file-btn" href="${{url}}" target="_blank">{exp_open_file}</a>
  </div>`;
}}

function _expShowResults(files) {{
  const el = document.getElementById('exp-results');
  const ph = document.getElementById('exp-results-ph');
  if (!files || !files.length) return;
  if (ph) ph.style.display = 'none';
  const html = files.map(f => _expRenderFileCard(f)).join('');
  el.insertAdjacentHTML('afterbegin', html);
}}

function _expShowJobAccepted(jobId) {{
  const el = document.getElementById('exp-results');
  const ph = document.getElementById('exp-results-ph');
  if (ph) ph.style.display = 'none';
  el.insertAdjacentHTML('afterbegin', `<div class="exp-info-card">{t_job_started}</div>`);
}}

function _expShowError(msg) {{
  const el = document.getElementById('exp-results');
  const ph = document.getElementById('exp-results-ph');
  if (ph) ph.style.display = 'none';
  el.insertAdjacentHTML('afterbegin', `<div class="exp-file-card">
    <div class="exp-file-icon">⚠️</div>
    <div class="exp-file-info">
      <div class="exp-file-name" style="color:#ef4444">{exp_job_error}</div>
      <div class="exp-file-meta">${{msg}}</div>
    </div>
  </div>`);
}}

function _expHandleResult(res) {{
  if (res && res.files && res.files.length) _expShowResults(res.files);
  else if (res && res.job && res.job.id) _expShowJobAccepted(res.job.id);
  else if (res && res.ok) _expShowJobAccepted(res.job ? res.job.id : '?');
  else if (res && res.error) _expShowError(res.error);
  else _expShowError(JSON.stringify(res));
}}

async function _expRefreshJobs() {{
  try {{
    const r = await fetch('/api/jobs', {{cache:'no-store'}});
    const data = await r.json();
    const arr = (data && data.jobs) ? data.jobs : [];
    const el = document.getElementById('exp-jobs-list');
    if (!el) return;
    if (!arr.length) {{
      el.innerHTML = '<div class="exp-placeholder">–</div>';
      return;
    }}
    let html = '';
    arr.forEach(j => {{
      const st = j.status || '';
      const pct = (j.progress_overall !== undefined && j.progress_overall !== null) ? parseInt(j.progress_overall,10) : 0;
      const action = j.action || '';
      const started = j.started_at ? new Date(j.started_at*1000).toLocaleString() : '';
      let stClass = 'running', stLabel = '{exp_job_running}';
      if (st === 'done' || st === 'completed') {{ stClass = 'done'; stLabel = '{exp_job_done}'; }}
      else if (st === 'error' || st === 'failed') {{ stClass = 'error'; stLabel = '{exp_job_error}'; }}
      let progHtml = '';
      const prog = j.progress || {{}};
      const keys = Object.keys(prog);
      if (keys.length) {{
        progHtml = keys.map(k => {{
          const p = prog[k] || {{}};
          const pp = parseInt(p.percent||0,10);
          const pm = p.message || '';
          return `<div class="exp-job-msg">${{k}}: ${{pp}}% ${{pm ? '– '+pm : ''}}</div>`;
        }}).join('');
      }}
      let filesHtml = '';
      const res = j.result || {{}};
      if (res.files && Array.isArray(res.files) && res.files.length) {{
        filesHtml = '<div class="exp-job-files">' + res.files.map(f => {{
          const url = f.url || '';
          const name = f.name || 'file';
          return `<a class="exp-job-file-link" href="${{url}}" target="_blank">${{_expFileIcon(name)}}<span>${{name}}</span></a>`;
        }}).join('') + '</div>';
      }}
      let errHtml = '';
      if (j.error) {{
        errHtml = `<div class="exp-job-msg" style="color:#ef4444">${{j.error}}</div>`;
      }}
      html += `<div class="exp-job-card">
        <div class="exp-job-head">
          <div class="exp-job-title">#${{j.id}} · ${{action}}</div>
          <span class="exp-job-status ${{stClass}}">${{stLabel}}</span>
        </div>
        <progress class="exp-job-progress" max="100" value="${{isNaN(pct)?0:pct}}"></progress>
        <div class="exp-job-msg">${{pct}}% · ${{started}}</div>
        ${{progHtml}}
        ${{errHtml}}
        ${{filesHtml}}
      </div>`;
    }});
    el.innerHTML = html;
  }} catch(e) {{}}
}}

function _expStartJobsPolling() {{
  if (_expJobsTimer) return;
  _expRefreshJobs();
  _expJobsTimer = setInterval(_expRefreshJobs, 2000);
}}

function _expStopJobsPolling() {{
  if (_expJobsTimer) {{ clearInterval(_expJobsTimer); _expJobsTimer = null; }}
}}

function initExport() {{
  if (!_expInited) {{
    _expInited = true;

    // Quick date presets
    const qd = document.getElementById('exp-quick-dates');
    const presets = [
      ['today', '{exp_today}'],
      ['week', '{exp_this_week}'],
      ['month', '{exp_this_month}'],
      ['year', '{exp_this_year}'],
      ['all', '{exp_all}'],
    ];
    presets.forEach(([k, lbl]) => {{
      const b = document.createElement('button');
      b.textContent = lbl;
      b.addEventListener('click', () => {{
        const now = new Date();
        const fmt = d => d.toISOString().slice(0,10);
        const eS = document.getElementById('exp-start');
        const eE = document.getElementById('exp-end');
        if (k==='today') {{ eS.value=fmt(now); eE.value=fmt(now); }}
        else if (k==='week') {{ const m=new Date(now); m.setDate(now.getDate()-now.getDay()+(now.getDay()===0?-6:1)); eS.value=fmt(m); eE.value=fmt(now); }}
        else if (k==='month') {{ eS.value=fmt(new Date(now.getFullYear(),now.getMonth(),1)); eE.value=fmt(now); }}
        else if (k==='year') {{ eS.value=fmt(new Date(now.getFullYear(),0,1)); eE.value=fmt(now); }}
        else {{ eS.value=''; eE.value=''; }}
      }});
      qd.appendChild(b);
    }});

    function setBusy(btn, on) {{
      if (on) {{ btn.disabled=true; btn.classList.add('busy'); }}
      else {{ btn.disabled=false; btn.classList.remove('busy'); }}
    }}

    async function expRun(action, params) {{
      const r = await fetch('/api/run', {{
        method: 'POST',
        headers: {{'Content-Type':'application/json'}},
        body: JSON.stringify({{action, params: params||{{}}}})
      }});
      if (!r.ok) throw new Error('HTTP '+r.status);
      return r.json();
    }}

    // Button handlers – all use _expHandleResult which handles files, jobs, and errors
    async function expClick(btn, action, params) {{
      setBusy(btn, true);
      try {{ _expHandleResult(await expRun(action, params)); }}
      catch(e) {{ _expShowError(e.message||String(e)); }}
      setBusy(btn, false);
    }}

    document.getElementById('exp-btn-summary').addEventListener('click', function() {{
      expClick(this, 'export_summary', {{start: document.getElementById('exp-start').value, end: document.getElementById('exp-end').value}});
    }});
    document.getElementById('exp-btn-invoices').addEventListener('click', function() {{
      expClick(this, 'export_invoices', {{start: document.getElementById('exp-start').value, end: document.getElementById('exp-end').value, period: document.getElementById('exp-inv-period').value, anchor: document.getElementById('exp-inv-anchor').value}});
    }});
    document.getElementById('exp-btn-excel').addEventListener('click', function() {{
      expClick(this, 'export_excel', {{start: document.getElementById('exp-start').value, end: document.getElementById('exp-end').value}});
    }});
    document.getElementById('exp-btn-bundle').addEventListener('click', function() {{
      expClick(this, 'bundle', {{hours: parseInt(document.getElementById('exp-bundle-h').value)||48}});
    }});
    document.getElementById('exp-btn-report-day').addEventListener('click', function() {{
      expClick(this, 'report', {{period:'day', anchor: document.getElementById('exp-inv-anchor').value}});
    }});
    document.getElementById('exp-btn-report-month').addEventListener('click', function() {{
      expClick(this, 'report', {{period:'month', anchor: document.getElementById('exp-inv-anchor').value}});
    }});
  }}
  // Start jobs polling when export tab is active
  _expStartJobsPolling();
}}

/* ──────────────────────────────────────────────
   BOOT
────────────────────────────────────────────── */
_loadLsSettings();
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
    const ls = localStorage.getItem('sea_theme');
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
  const LS_THEME = 'sea_theme';
  let theme = localStorage.getItem(LS_THEME);
  if (!theme) {
    theme = (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) ? 'dark' : 'light';
  }
  document.documentElement.dataset.theme = theme;
} catch (e) {}

function toggleTheme(){
  try {
    const LS_THEME = 'sea_theme';
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
      -webkit-overflow-scrolling: touch;
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
    /* Export card redesign */
    .export-card {{ grid-column: 1 / -1; }}
    .export-sections {{ display: grid; grid-template-columns: 1fr; gap: 10px; }}
    @media (min-width: 700px) {{ .export-sections {{ grid-template-columns: 1fr 1fr; }} }}
    .export-section {{ border: 1px solid var(--border); border-radius: 12px; padding: 10px; background: var(--chipbg); }}
    .export-section h3 {{ margin: 0 0 8px; font-size: 12px; font-weight: 650; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; }}
    .quick-dates {{ display: flex; flex-wrap: wrap; gap: 4px; margin-bottom: 8px; }}
    .quick-dates button {{ font-size: 11px; padding: 4px 10px; border-radius: 999px; border: 1px solid var(--border); background: var(--chipbg); color: var(--muted); cursor: pointer; }}
    .quick-dates button:hover {{ color: var(--fg); border-color: rgba(106,167,255,0.35); }}
    .export-actions {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 6px; }}
    @media (min-width: 700px) {{ .export-actions {{ grid-template-columns: repeat(3, 1fr); }} }}
    .export-actions button {{ display: flex; flex-direction: column; align-items: center; gap: 2px; padding: 10px 6px; font-size: 12px; text-align: center; min-height: 54px; justify-content: center; }}
    .export-actions button .btn-icon {{ font-size: 18px; line-height: 1; }}
    .export-actions button .btn-label {{ font-size: 11px; line-height: 1.2; }}
    .export-actions button:disabled {{ opacity: 0.5; cursor: default; }}
    .export-actions button.loading {{ position: relative; color: transparent; }}
    .export-actions button.loading::after {{ content: ""; position: absolute; width: 16px; height: 16px; border: 2px solid var(--border); border-top-color: var(--accent); border-radius: 50%; animation: btn-spin 0.6s linear infinite; }}
    @keyframes btn-spin {{ to {{ transform: rotate(360deg); }} }}
    .inline-field {{ display: flex; align-items: center; gap: 6px; margin-bottom: 6px; }}
    .inline-field label {{ min-width: 30px; }}
    .inline-field input, .inline-field select {{ flex: 1; min-width: 0; }}
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
        <h2>📊 Network Traffic</h2>
        <div id="traffic-rate" style="font-size:16px;font-weight:bold;margin-bottom:8px">↓ 0 B/s  ↑ 0 B/s</div>
        <div id="traffic-total" style="font-size:12px;color:var(--muted);margin-bottom:8px"></div>
        <div style="max-height:220px;overflow-y:auto;margin-bottom:8px">
        <table style="width:100%;font-size:13px;border-collapse:collapse" id="traffic-table">
          <thead><tr style="border-bottom:1px solid var(--border);text-align:left">
            <th style="padding:4px 8px">Category</th>
            <th style="padding:4px 8px;text-align:center">Requests</th>
            <th style="padding:4px 8px;text-align:right">↓ Received</th>
            <th style="padding:4px 8px;text-align:right">↑ Sent</th>
          </tr></thead>
          <tbody id="traffic-tbody"></tbody>
        </table>
        </div>
        <canvas id="traffic-chart" style="width:100%;height:120px"></canvas>
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

      <div class="card export-card">
        <h2>{web_control_export}</h2>
        <div class="export-sections">
          <div class="export-section">
            <h3>{web_control_export_daterange}</h3>
            <div class="quick-dates">
              <button data-quick="today">{web_control_export_today}</button>
              <button data-quick="week">{web_control_export_this_week}</button>
              <button data-quick="month">{web_control_export_this_month}</button>
              <button data-quick="year">{web_control_export_this_year}</button>
              <button data-quick="all">{web_control_export_all}</button>
            </div>
            <div class="inline-field">
              <label for="exp_start">{web_control_plots_from}</label>
              <input id="exp_start" type="date" />
            </div>
            <div class="inline-field">
              <label for="exp_end">{web_control_plots_to}</label>
              <input id="exp_end" type="date" />
            </div>
          </div>
          <div class="export-section">
            <h3>{web_control_export_invoice_settings}</h3>
            <div class="inline-field">
              <label for="inv_period">{web_control_invoice}</label>
              <select id="inv_period">
                <option value="custom">custom</option>
                <option value="day">day</option>
                <option value="week">week</option>
                <option value="month" selected>month</option>
                <option value="year">year</option>
              </select>
            </div>
            <div class="inline-field">
              <label for="inv_anchor">{web_control_anchor}</label>
              <input id="inv_anchor" type="date" />
            </div>
            <span class="meta">{web_control_custom_note}</span>
            <div class="inline-field" style="margin-top:6px">
              <label for="bundle_hours">{web_control_export_bundle_hours}</label>
              <input id="bundle_hours" type="number" value="48" min="1" max="8760" style="width:80px;flex:0 0 80px;" />
            </div>
          </div>
        </div>

        <div class="export-section" style="margin-top:10px">
          <h3>{web_control_export_actions}</h3>
          <div class="export-actions">
            <button id="btn_summary"><span class="btn-icon">📄</span><span class="btn-label">{web_control_btn_summary}</span></button>
            <button id="btn_invoices"><span class="btn-icon">🧾</span><span class="btn-label">{web_control_btn_invoices}</span></button>
            <button id="btn_excel"><span class="btn-icon">📊</span><span class="btn-label">{web_control_btn_excel}</span></button>
            <button id="btn_report_day"><span class="btn-icon">📅</span><span class="btn-label">{web_control_btn_report_day}</span></button>
            <button id="btn_report_month"><span class="btn-icon">📆</span><span class="btn-label">{web_control_btn_report_month}</span></button>
            <button id="btn_bundle"><span class="btn-icon">📦</span><span class="btn-label">{web_control_btn_bundle}</span></button>
          </div>
        </div>

        <div class="export-section" style="margin-top:10px">
          <h3>{web_control_export_preview}</h3>
          <div id="ctrl_export_results">
            <div class="ctrl-exp-ph" id="ctrl_export_ph" style="color:var(--muted);font-size:12px;text-align:center;padding:12px 0">{web_control_export_no_preview}</div>
          </div>
        </div>
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
const LS_THEME = "sea_theme";
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

function qs() { return window.location.search || ""; }

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
  const err = j.error ? `<div class="jobmeta"><b>{t_error_label}:</b> ${{esc(j.error)}}</div>` : "";
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
  document.getElementById("sync_log").textContent = "{t_starting}";
  try {
    const res = await run("sync", {mode, start_date: start});
    document.getElementById("sync_log").textContent = JSON.stringify(res, null, 2);
  } catch (e) {
    document.getElementById("sync_log").textContent = "{t_error_label}: " + (e && e.message ? e.message : String(e));
  }
});

// --- Network Traffic ---
const _trafficCatIcons = {shelly:'🔌',entsoe:'🌿',spot_price:'⚡',weather:'🌡️',telegram:'💬',github:'🔄',local:'🏠',other:'📡'};
const _trafficCatLabels = {shelly:'Shelly Devices',entsoe:'ENTSO-E API',spot_price:'Spot Prices',weather:'OpenWeather',telegram:'Telegram',github:'GitHub',local:'Local/Web',other:'Other'};
function _fmtBytes(n) {{
  if (n < 1024) return n + ' B';
  if (n < 1048576) return (n/1024).toFixed(1) + ' KB';
  if (n < 1073741824) return (n/1048576).toFixed(1) + ' MB';
  return (n/1073741824).toFixed(2) + ' GB';
}}
function _fmtRate(bps) {{
  if (bps < 1024) return bps.toFixed(0) + ' B/s';
  if (bps < 1048576) return (bps/1024).toFixed(1) + ' KB/s';
  return (bps/1048576).toFixed(1) + ' MB/s';
}}
async function _refreshTraffic() {{
  try {{
    const r = await fetch('/api/traffic');
    const d = await r.json();
    const rateEl = document.getElementById('traffic-rate');
    const totalEl = document.getElementById('traffic-total');
    const tbody = document.getElementById('traffic-tbody');
    if (!rateEl) return;
    rateEl.textContent = '\u2193 ' + _fmtRate(d.rate_recv_bps||0) + '  \u2191 ' + _fmtRate(d.rate_sent_bps||0);
    const hrs = Math.floor((d.uptime_s||0)/3600);
    const mins = Math.floor(((d.uptime_s||0)%3600)/60);
    const uptime = hrs ? hrs+'h '+mins+'m' : mins+'m';
    totalEl.textContent = 'Total: \u2193 ' + _fmtBytes(d.total_received||0) + '  \u2191 ' + _fmtBytes(d.total_sent||0) + '  |  ' + (d.total_requests||0) + ' Requests  |  ' + uptime;
    tbody.innerHTML = '';
    const cats = Object.entries(d.categories||{{}}).sort((a,b) => (b[1].received||0)-(a[1].received||0));
    for (const [cat, data] of cats) {{
      const icon = _trafficCatIcons[cat] || '📡';
      const label = _trafficCatLabels[cat] || cat;
      const tr = document.createElement('tr');
      tr.style.borderBottom = '1px solid var(--border)';
      tr.innerHTML = '<td style="padding:4px 8px">' + icon + ' ' + label + '</td>'
        + '<td style="padding:4px 8px;text-align:center">' + (data.requests||0) + '</td>'
        + '<td style="padding:4px 8px;text-align:right">' + _fmtBytes(data.received||0) + '</td>'
        + '<td style="padding:4px 8px;text-align:right">' + _fmtBytes(data.sent||0) + '</td>';
      tbody.appendChild(tr);
    }}
    // Draw live traffic rate chart
    _drawTrafficRateChart(d.rate_history);
  }} catch(e) {{}}
}}
function _drawTrafficRateChart(hist) {{
  const cv = document.getElementById('traffic-chart');
  if (!cv || !hist || !hist.ts || !hist.ts.length) return;
  const dpr = window.devicePixelRatio || 1;
  const W = cv.offsetWidth;
  const H = cv.offsetHeight || 120;
  cv.width = W * dpr;
  cv.height = H * dpr;
  const ctx = cv.getContext('2d');
  ctx.scale(dpr, dpr);
  const isDark = document.documentElement.dataset.theme === 'dark';
  const fg = isDark ? '#bbb' : '#555';
  const gridC = isDark ? 'rgba(255,255,255,0.08)' : 'rgba(0,0,0,0.08)';

  const ts = hist.ts;   // seconds ago (negative)
  const recv = hist.recv;
  const sent = hist.sent;
  const maxVal = Math.max(1, Math.max(...recv), Math.max(...sent));

  const padL = 50, padR = 10, padT = 10, padB = 22;
  const cW = W - padL - padR;
  const cH = H - padT - padB;
  const minT = ts[0] || -3600;
  const maxT = ts[ts.length - 1] || 0;
  const rangeT = Math.max(1, maxT - minT);

  function xOf(t) {{ return padL + ((t - minT) / rangeT) * cW; }}
  function yOf(v) {{ return padT + cH - (v / maxVal) * cH; }}

  // Grid lines
  ctx.strokeStyle = gridC;
  ctx.lineWidth = 0.5;
  for (let i = 0; i <= 4; i++) {{
    const y = padT + (cH * i / 4);
    ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(padL + cW, y); ctx.stroke();
  }}

  // Draw filled area + line for recv
  function drawSeries(vals, lineColor, fillColor) {{
    ctx.globalAlpha = 0.25;
    ctx.fillStyle = fillColor;
    ctx.beginPath();
    ctx.moveTo(xOf(ts[0]), yOf(0));
    for (let i = 0; i < vals.length; i++) ctx.lineTo(xOf(ts[i]), yOf(vals[i]));
    ctx.lineTo(xOf(ts[vals.length - 1]), yOf(0));
    ctx.closePath();
    ctx.fill();
    ctx.globalAlpha = 1;
    ctx.strokeStyle = lineColor;
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    for (let i = 0; i < vals.length; i++) {{
      if (i === 0) ctx.moveTo(xOf(ts[i]), yOf(vals[i]));
      else ctx.lineTo(xOf(ts[i]), yOf(vals[i]));
    }}
    ctx.stroke();
  }}
  drawSeries(recv, '#2196F3', '#2196F3');
  drawSeries(sent, '#FF9800', '#FF9800');

  // Y-axis labels
  ctx.fillStyle = fg;
  ctx.font = '9px sans-serif';
  ctx.textAlign = 'right';
  ctx.textBaseline = 'middle';
  for (let i = 0; i <= 4; i++) {{
    const v = maxVal * (4 - i) / 4;
    ctx.fillText(_fmtRate(v), padL - 4, padT + (cH * i / 4));
  }}

  // X-axis labels (minutes ago)
  ctx.textAlign = 'center';
  ctx.textBaseline = 'top';
  const steps = [0, -1, -2, -3, -4, -5];
  for (const m of steps) {{
    const t = m * 60;
    if (t >= minT && t <= maxT) {{
      ctx.fillText(m === 0 ? 'now' : m + 'm', xOf(t), padT + cH + 4);
    }}
  }}

  // Legend
  ctx.font = '9px sans-serif';
  ctx.textAlign = 'left';
  ctx.fillStyle = '#2196F3';
  ctx.fillRect(padL + 4, padT + 2, 12, 3);
  ctx.fillStyle = fg;
  ctx.fillText('\u2193 Down', padL + 20, padT + 1);
  ctx.fillStyle = '#FF9800';
  ctx.fillRect(padL + 70, padT + 2, 12, 3);
  ctx.fillStyle = fg;
  ctx.fillText('\u2191 Up', padL + 86, padT + 1);
}}
setInterval(_refreshTraffic, 500);
_refreshTraffic();

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
    thumbs.innerHTML = `<div class="jobmeta">{t_error_label}: ${{esc(e && e.message ? e.message : String(e))}}</div>`;
  }
});

// --- Quick date presets ---
document.querySelectorAll(".quick-dates button[data-quick]").forEach(btn => {
  btn.addEventListener("click", () => {
    const q = btn.dataset.quick;
    const now = new Date();
    const fmt = d => d.toISOString().slice(0,10);
    const elS = document.getElementById("exp_start");
    const elE = document.getElementById("exp_end");
    if (q === "today") {
      elS.value = fmt(now); elE.value = fmt(now);
    } else if (q === "week") {
      const mon = new Date(now); mon.setDate(now.getDate() - now.getDay() + (now.getDay()===0?-6:1));
      elS.value = fmt(mon); elE.value = fmt(now);
    } else if (q === "month") {
      const ms = new Date(now.getFullYear(), now.getMonth(), 1);
      elS.value = fmt(ms); elE.value = fmt(now);
    } else if (q === "year") {
      const ys = new Date(now.getFullYear(), 0, 1);
      elS.value = fmt(ys); elE.value = fmt(now);
    } else if (q === "all") {
      elS.value = ""; elE.value = "";
    }
  });
});

// --- Export helpers ---
function setButtonLoading(btn, loading) {
  if (loading) { btn.disabled = true; btn.classList.add("loading"); }
  else { btn.disabled = false; btn.classList.remove("loading"); }
}
function _ctrlFileIcon(name) {
  const n = (name||"").toLowerCase();
  if (n.endsWith(".pdf")) return "📄";
  if (n.endsWith(".xlsx") || n.endsWith(".xls")) return "📊";
  if (n.endsWith(".zip")) return "📦";
  if (n.endsWith(".png") || n.endsWith(".jpg") || n.endsWith(".jpeg")) return "🖼️";
  return "📎";
}
function _ctrlFileCard(f) {
  const icon = _ctrlFileIcon(f.name);
  const url = (f.url||"") + qs();
  const name = esc(f.name || "file");
  const ext = (f.name||"").split(".").pop().toUpperCase();
  return `<div style="display:flex;align-items:center;gap:10px;padding:10px;border:1px solid var(--border);border-radius:12px;background:var(--card);margin-bottom:6px">
    <div style="font-size:28px">${icon}</div>
    <div style="flex:1;min-width:0">
      <div style="font-size:13px;font-weight:600;word-break:break-all">${name}</div>
      <div style="font-size:11px;color:var(--muted)">${ext}</div>
    </div>
    <a href="${url}" target="_blank" style="display:inline-flex;align-items:center;gap:4px;font-size:12px;padding:8px 16px;border-radius:10px;border:1px solid rgba(106,167,255,0.35);background:rgba(106,167,255,0.12);color:var(--accent);text-decoration:none;font-weight:600;white-space:nowrap;min-height:40px">Öffnen</a>
  </div>`;
}
function showResults(files) {
  const el = document.getElementById("ctrl_export_results");
  const ph = document.getElementById("ctrl_export_ph");
  if (!files || !files.length) return;
  if (ph) ph.style.display = "none";
  el.insertAdjacentHTML("afterbegin", files.map(f => _ctrlFileCard(f)).join(""));
}
function showJobAccepted(jobId) {
  const el = document.getElementById("ctrl_export_results");
  const ph = document.getElementById("ctrl_export_ph");
  if (ph) ph.style.display = "none";
  el.insertAdjacentHTML("afterbegin", `<div style="display:flex;align-items:center;gap:10px;padding:10px;border:1px solid rgba(106,167,255,0.25);border-radius:12px;background:rgba(106,167,255,0.06);margin-bottom:6px;color:var(--accent);font-size:12px">✓ Job #${esc(String(jobId))} gestartet – siehe Jobs unten.</div>`);
}
function showError(msg) {
  const el = document.getElementById("ctrl_export_results");
  const ph = document.getElementById("ctrl_export_ph");
  if (ph) ph.style.display = "none";
  el.insertAdjacentHTML("afterbegin", `<div style="display:flex;align-items:center;gap:10px;padding:10px;border:1px solid var(--border);border-radius:12px;background:var(--card);margin-bottom:6px">
    <div style="font-size:24px">⚠️</div>
    <div style="flex:1;min-width:0">
      <div style="font-size:13px;font-weight:600;color:#ef4444">{t_error_label}</div>
      <div style="font-size:11px;color:var(--muted)">${esc(msg)}</div>
    </div>
  </div>`);
}
function handleResult(res) {
  if (res && res.files && res.files.length) showResults(res.files);
  else if (res && res.job && res.job.id) showJobAccepted(res.job.id);
  else if (res && res.ok) showJobAccepted(res.job ? res.job.id : "?");
  else if (res && res.error) showError(res.error);
  else showError(JSON.stringify(res));
}

// --- Export button handlers ---
async function ctrlExport(btn, action, params) {
  setButtonLoading(btn, true);
  try { handleResult(await run(action, params)); }
  catch (e) { showError(e&&e.message?e.message:String(e)); }
  setButtonLoading(btn, false);
}

document.getElementById("btn_summary").addEventListener("click", function(){
  ctrlExport(this, "export_summary", {start: document.getElementById("exp_start").value, end: document.getElementById("exp_end").value});
});
document.getElementById("btn_invoices").addEventListener("click", function(){
  ctrlExport(this, "export_invoices", {start: document.getElementById("exp_start").value, end: document.getElementById("exp_end").value, period: document.getElementById("inv_period").value, anchor: document.getElementById("inv_anchor").value});
});
document.getElementById("btn_excel").addEventListener("click", function(){
  ctrlExport(this, "export_excel", {start: document.getElementById("exp_start").value, end: document.getElementById("exp_end").value});
});
document.getElementById("btn_bundle").addEventListener("click", function(){
  ctrlExport(this, "bundle", {hours: parseInt(document.getElementById("bundle_hours").value) || 48});
});
document.getElementById("btn_report_day").addEventListener("click", function(){
  ctrlExport(this, "report", {period: "day", anchor: document.getElementById("inv_anchor").value});
});
document.getElementById("btn_report_month").addEventListener("click", function(){
  ctrlExport(this, "report", {period: "month", anchor: document.getElementById("inv_anchor").value});
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
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
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
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
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
                switch_states_map: Dict[str, Any] = raw_snap.get("_switch_states", {})  # type: ignore[assignment]
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
                    appl_objs: List[Dict[str, str]] = [
                        {"icon": a.get("icon", ""), "id": a.get("id", "")}
                        for a in raw_appl
                        if isinstance(a, dict)
                    ]
                    qa_val = float(latest.get("qa") or 0)
                    qb_val = float(latest.get("qb") or 0)
                    qc_val = float(latest.get("qc") or 0)
                    q_phases: List[Dict[str, float]] = []
                    if vb > 0 or vc > 0:
                        if va > 0:
                            q_phases.append({"var": qa_val})
                        if vb > 0:
                            q_phases.append({"var": qb_val})
                        if vc > 0:
                            q_phases.append({"var": qc_val})
                    dev_kind = str(meta.get("kind") or "em")
                    switch_on = switch_states_map.get(dkey) if dev_kind == "switch" else None

                    devices_list.append({
                        "key": dkey,
                        "name": name,
                        "kind": dev_kind,
                        "power_w": float(latest.get("power_total_w") or 0),
                        "today_kwh": float(latest.get("kwh_today") or 0),
                        "cost_today": float(latest.get("cost_today") or 0),
                        "voltage_v": voltage_v,
                        "current_a": current_a,
                        "pf": float(latest.get("cosphi_total") or 0),
                        "freq_hz": float(latest.get("freq_hz") or 50),
                        "phases": phases,
                        "q_phases": q_phases,
                        "appliances": appl_objs,
                        "i_n": float(latest.get("i_n") or 0),
                        "q_total_var": float(latest.get("q_total_var") or 0),
                        "switch_on": switch_on,
                    })
                payload = {"devices": devices_list}
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return


            if path_only.startswith("/api/history"):
                # Return all stored sparkline history so the browser can pre-populate
                # sparklines on first page load instead of starting from empty buffers.
                raw_snap = self.store.snapshot()
                hist: Dict[str, List[Dict[str, Any]]] = {}
                for dkey, points in raw_snap.items():
                    if dkey.startswith("_") or not isinstance(points, list) or not points:
                        continue
                    pts_out = []
                    for p in points:
                        va = float(p.get("va") or 0)
                        vb = float(p.get("vb") or 0)
                        vc = float(p.get("vc") or 0)
                        ia = float(p.get("ia") or 0)
                        ib = float(p.get("ib") or 0)
                        ic = float(p.get("ic") or 0)
                        pa = float(p.get("pa") or 0)
                        pb = float(p.get("pb") or 0)
                        pc = float(p.get("pc") or 0)
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
                        h_qa = float(p.get("qa") or 0)
                        h_qb = float(p.get("qb") or 0)
                        h_qc = float(p.get("qc") or 0)
                        h_q_phases: List[Dict[str, float]] = []
                        if vb > 0 or vc > 0:
                            if va > 0:
                                h_q_phases.append({"var": h_qa})
                            if vb > 0:
                                h_q_phases.append({"var": h_qb})
                            if vc > 0:
                                h_q_phases.append({"var": h_qc})
                        pts_out.append({
                            # ts from LivePoint is UNIX seconds; JS expects milliseconds
                            "ts": int(p.get("ts") or 0) * 1000,
                            "w": float(p.get("power_total_w") or 0),
                            "v": voltage_v,
                            "a": current_a,
                            "phases": phases,
                            "i_n": float(p.get("i_n") or 0),
                            "q": float(p.get("q_total_var") or 0),
                            "q_phases": h_q_phases,
                        })
                    hist[dkey] = pts_out
                body = json.dumps({"history": hist}, ensure_ascii=False).encode("utf-8")
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
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path_only.startswith("/api/config"):
                payload = self.dashboard.get_config()
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
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
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
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
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
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
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path_only.startswith("/api/weather_correlation"):
                try:
                    parsed = urlparse(self.path)
                    qs = parse_qs(parsed.query or "")
                    params_wc: Dict[str, Any] = {k: (v[0] if isinstance(v, list) and v else v) for k, v in qs.items()}
                    payload = self.dashboard.on_action("weather_correlation", params_wc)
                except Exception as e:
                    payload = {"ok": False, "error": str(e)}
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path_only == "/api/co2_live":
                try:
                    payload = self.dashboard.on_action("co2_live", {})
                except Exception as e:
                    payload = {"ok": False, "error": str(e)}
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path_only.startswith("/api/co2"):
                try:
                    _co2_qs = parse_qs(urlparse(self.path).query or "")
                    _co2_params = {k: (v[0] if isinstance(v, list) and v else v) for k, v in _co2_qs.items()}
                    payload = self.dashboard.on_action("co2", _co2_params)
                except Exception as e:
                    payload = {"ok": False, "error": str(e)}
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
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
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
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
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path_only.startswith("/api/nilm_status"):
                try:
                    store = self.dashboard.store
                    clusters = getattr(store, "_nilm_clusters", [])
                    trans_count = getattr(store, "_nilm_transition_count", 0)
                    payload = {
                        "ok": True,
                        "cluster_count": len(clusters),
                        "transition_count": trans_count,
                        "clusters": clusters[:10],
                    }
                except Exception as e:
                    payload = {"ok": False, "error": str(e), "cluster_count": 0, "transition_count": 0, "clusters": []}
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path_only.startswith("/api/forecast"):
                try:
                    _fp = urlparse(self.path)
                    _fqs = parse_qs(_fp.query or "")
                    _fparams = {k: (v[0] if isinstance(v, list) and v else v) for k, v in _fqs.items()}
                    payload = self.dashboard.on_action("forecast", _fparams)
                except Exception as e:
                    payload = {"ok": False, "error": str(e)}
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path_only.startswith("/api/standby"):
                try:
                    payload = self.dashboard.on_action("standby", {})
                except Exception as e:
                    payload = {"ok": False, "error": str(e)}
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path_only.startswith("/api/sankey"):
                try:
                    _sp = urlparse(self.path)
                    _sqs = parse_qs(_sp.query or "")
                    _sparams = {k: (v[0] if isinstance(v, list) and v else v) for k, v in _sqs.items()}
                    payload = self.dashboard.on_action("sankey", _sparams)
                except Exception as e:
                    payload = {"ok": False, "error": str(e)}
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path_only.startswith("/api/ev_chargers"):
                try:
                    _ep = urlparse(self.path)
                    _eqs = parse_qs(_ep.query or "")
                    _eparams = {k: (v[0] if isinstance(v, list) and v else v) for k, v in _eqs.items()}
                    lat = float(_eparams.get("lat", 0))
                    lon = float(_eparams.get("lon", 0))
                    radius = max(100, min(10000, int(_eparams.get("radius", 500))))
                    api_key = str(_eparams.get("key", "") or "")
                    min_kw = float(_eparams.get("min_kw", 0) or 0)
                    plug = str(_eparams.get("plug", "") or "")
                    from shelly_analyzer.services.ev_charger import fetch_ev_chargers
                    payload = fetch_ev_chargers(lat, lon, radius_m=radius, api_key=api_key, min_kw=min_kw, plug_filter=plug)
                except Exception as e:
                    payload = {"ok": False, "error": str(e), "stations": []}
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path_only.startswith("/api/traffic"):
                try:
                    from shelly_analyzer.services.traffic import TrafficMonitor
                    payload = TrafficMonitor.get().snapshot()
                except Exception as e:
                    payload = {"error": str(e)}
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path_only == "/plots" or path_only.startswith("/plots/"):
                _ae = self.headers.get("Accept-Encoding", "")
                _gz = self.dashboard.plots_html_bytes_gz
                _use_gz = "gzip" in _ae and bool(_gz)
                body = _gz if _use_gz else self.dashboard.plots_html_bytes
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                if _use_gz:
                    self.send_header("Content-Encoding", "gzip")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path_only.startswith("/control"):
                _ae = self.headers.get("Accept-Encoding", "")
                _gz = self.dashboard.control_html_bytes_gz
                _use_gz = "gzip" in _ae and bool(_gz)
                body = _gz if _use_gz else self.dashboard.control_html_bytes
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                if _use_gz:
                    self.send_header("Content-Encoding", "gzip")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path_only == "/" or path_only.startswith("/index.html"):
                _ae = self.headers.get("Accept-Encoding", "")
                _gz = getattr(self, "html_bytes_gz", None)
                _use_gz = "gzip" in _ae and bool(_gz)
                body = _gz if _use_gz else self.html_bytes
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                if _use_gz:
                    self.send_header("Content-Encoding", "gzip")
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
                self.send_header("Pragma", "no-cache")
                self.send_header("Expires", "0")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_response(404)
            self.end_headers()
            return
        except Exception as e:
            msg = str(e)
            body = ("<!doctype html><html><body style='font-family:system-ui;padding:16px'>"                    "<h3>Web-Dashboard Error</h3>"                    "<pre style='white-space:pre-wrap'>" + msg + "</pre></body></html>").encode("utf-8")
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
                    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
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
                    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
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
            body = ("<!doctype html><html><body style='font-family:system-ui;padding:16px'>"                    "<h3>Web-Dashboard Error</h3>"                    "<pre style='white-space:pre-wrap'>" + msg + "</pre></body></html>").encode("utf-8")
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
                # Tab nav labels
                "web_tab_live": _t(self.lang, "web.tab.live"),
                "web_tab_costs": _t(self.lang, "web.tab.costs"),
                "web_tab_heatmap": _t(self.lang, "web.tab.heatmap"),
                "web_tab_solar": _t(self.lang, "web.tab.solar"),
                "web_tab_weather": _t(self.lang, "web.tab.weather"),
                "web_tab_compare": _t(self.lang, "web.tab.compare"),
                "web_tab_co2": _t(self.lang, "web.tab.co2"),
                "web_tab_anomalies": _t(self.lang, "web.tab.anomalies"),
                "web_tab_forecast": _t(self.lang, "web.tab.forecast"),
                "web_tab_standby": _t(self.lang, "web.tab.standby"),
                "web_tab_sankey": _t(self.lang, "web.tab.sankey"),
                "web_tab_ev": _t(self.lang, "web.tab.ev"),
                "web_ev_radius": _t(self.lang, "web.ev.radius"),
                "web_ev_city_placeholder": _t(self.lang, "web.ev.city_placeholder"),
                "web_ev_all_power": _t(self.lang, "web.ev.all_power"),
                "web_ev_all_plugs": _t(self.lang, "web.ev.all_plugs"),
                "web_ev_apikey_hint": _t(self.lang, "web.ev.apikey_hint"),
                "web_ev_save": _t(self.lang, "web.ev.save"),
                "t_patterns": _t(self.lang, "web.nilm.patterns"),
                "t_learning": _t(self.lang, "web.nilm.learning"),
                "t_transitions": _t(self.lang, "web.nilm.transitions"),
                "t_waiting": _t(self.lang, "web.nilm.waiting"),
                "web_tab_export": _t(self.lang, "web.tab.export"),
                # Export pane
                "exp_daterange": _t(self.lang, "web.control.export.daterange"),
                "exp_from": _t(self.lang, "web.control.plots.from"),
                "exp_to": _t(self.lang, "web.control.plots.to"),
                "exp_invoice_settings": _t(self.lang, "web.control.export.invoice_settings"),
                "exp_invoice": _t(self.lang, "web.control.invoice"),
                "exp_anchor": _t(self.lang, "web.control.anchor"),
                "exp_custom_note": _t(self.lang, "web.control.custom_note"),
                "exp_bundle_hours": _t(self.lang, "web.control.export.bundle_hours"),
                "exp_actions": _t(self.lang, "web.control.export.actions"),
                "exp_btn_pdf": _t(self.lang, "web.control.btn.pdf"),
                "exp_btn_invoices": _t(self.lang, "web.control.btn.invoices"),
                "exp_btn_excel": _t(self.lang, "web.control.btn.excel"),
                "exp_btn_bundle": _t(self.lang, "web.control.btn.bundle"),
                "exp_btn_report_day": _t(self.lang, "web.control.btn.report_day"),
                "exp_btn_report_month": _t(self.lang, "web.control.btn.report_month"),
                "exp_preview": _t(self.lang, "web.control.export.preview"),
                "exp_no_preview": _t(self.lang, "web.control.export.no_preview"),
                "exp_today": _t(self.lang, "web.control.export.today"),
                "exp_this_week": _t(self.lang, "web.control.export.this_week"),
                "exp_this_month": _t(self.lang, "web.control.export.this_month"),
                "exp_this_year": _t(self.lang, "web.control.export.this_year"),
                "exp_all": _t(self.lang, "web.control.export.all"),
                "exp_results": _t(self.lang, "web.control.export.results"),
                "exp_jobs": _t(self.lang, "web.control.export.jobs"),
                "exp_open_file": _t(self.lang, "web.control.export.open_file"),
                "exp_no_results": _t(self.lang, "web.control.export.no_results"),
                "exp_job_running": _t(self.lang, "web.control.export.job_running"),
                "exp_job_done": _t(self.lang, "web.control.export.job_done"),
                "exp_job_error": _t(self.lang, "web.control.export.job_error"),
                # Button titles
                "web_btn_freeze_title": _t(self.lang, "web.dash.freeze_resume"),
                "web_btn_settings_title": _t(self.lang, "web.dash.device_settings"),
                "web_btn_theme_title": _t(self.lang, "web.btn.theme"),
                # Loading placeholder (server-rendered initial HTML)
                "web_loading": _t(self.lang, "web.loading"),
                # Modal
                "web_dash_device_order": _t(self.lang, "web.dash.device_order"),
                "web_dash_done": _t(self.lang, "web.dash.done"),
                # Generic translated strings for JS
                "t_error_label": _t(self.lang, "web.error_label"),
                "t_starting": _t(self.lang, "web.starting"),
                "t_job_started": _t(self.lang, "web.job_started"),
                "refresh_ms": str(int(max(250, self.refresh_seconds * 1000))),
                "window_min": str(int(max(1, self.window_minutes))),
                "window_options_json": json.dumps(self.available_windows, ensure_ascii=False),
                "devices_json": json.dumps(
                    (self.devices_meta or [{"key": k, "name": n} for (k, n) in (self.devices or [])]),
                    ensure_ascii=False,
                ),
                "i18n_json": json.dumps(web_i18n, ensure_ascii=False),
            },
        )
        self._html_bytes = html.encode("utf-8")
        self._html_bytes_gz = gzip.compress(self._html_bytes, compresslevel=6)

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
                "web_control_btn_excel": _t(self.lang, "web.control.btn.excel"),
                "web_control_btn_bundle": _t(self.lang, "web.control.btn.bundle"),
                "web_control_btn_report_day": _t(self.lang, "web.control.btn.report_day"),
                "web_control_btn_report_month": _t(self.lang, "web.control.btn.report_month"),
                "web_control_export_daterange": _t(self.lang, "web.control.export.daterange"),
                "web_control_export_invoice_settings": _t(self.lang, "web.control.export.invoice_settings"),
                "web_control_export_actions": _t(self.lang, "web.control.export.actions"),
                "web_control_export_preview": _t(self.lang, "web.control.export.preview"),
                "web_control_export_no_preview": _t(self.lang, "web.control.export.no_preview"),
                "web_control_export_today": _t(self.lang, "web.control.export.today"),
                "web_control_export_this_week": _t(self.lang, "web.control.export.this_week"),
                "web_control_export_this_month": _t(self.lang, "web.control.export.this_month"),
                "web_control_export_this_year": _t(self.lang, "web.control.export.this_year"),
                "web_control_export_all": _t(self.lang, "web.control.export.all"),
                "web_control_export_bundle_hours": _t(self.lang, "web.control.export.bundle_hours"),
                "web_control_jobs_meta": _t(self.lang, "web.control.jobs.meta"),
                # Generic translated strings for JS
                "t_error_label": _t(self.lang, "web.error_label"),
                "t_starting": _t(self.lang, "web.starting"),
                "t_job_started": _t(self.lang, "web.job_started"),
            },
        ).encode("utf-8")
        self._control_bytes_gz = gzip.compress(self._control_bytes, compresslevel=6)

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
                "i18n_json": json.dumps(web_i18n, ensure_ascii=False),
                "devices_json": json.dumps(
                    (self.devices_meta or [{"key": k, "name": n} for (k, n) in (self.devices or [])]),
                    ensure_ascii=False,
                ),
            },
        ).encode("utf-8")
        self._plots_bytes_gz = gzip.compress(self._plots_bytes, compresslevel=6)

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
    def control_html_bytes_gz(self) -> bytes:
        return getattr(self, "_control_bytes_gz", b"")

    @property
    def plots_html_bytes(self) -> bytes:
        return getattr(self, "_plots_bytes", b"")

    @property
    def plots_html_bytes_gz(self) -> bytes:
        return getattr(self, "_plots_bytes_gz", b"")

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
        handler.html_bytes_gz = self._html_bytes_gz
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

        # Wrap with SSL so browsers allow Geolocation API (requires secure context)
        self._is_https = False
        try:
            import ssl as _ssl
            _cert_dir = (self.out_dir or Path(".")) / "data" / "runtime" / "ssl"
            _cert, _key = _ensure_ssl_cert(_cert_dir)
            ctx = _ssl.SSLContext(_ssl.PROTOCOL_TLS_SERVER)
            ctx.load_cert_chain(str(_cert), str(_key))
            self._httpd.socket = ctx.wrap_socket(self._httpd.socket, server_side=True)
            self._is_https = True
            logging.getLogger(__name__).info("Web dashboard HTTPS enabled (self-signed cert)")
        except Exception as e:
            logging.getLogger(__name__).warning(
                "HTTPS not available (GPS may not work): %s – falling back to HTTP", e
            )

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
        scheme = "https" if self._is_https else "http"
        return f"{scheme}://{ip}:{self.port}/"

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
