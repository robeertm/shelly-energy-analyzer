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


def _compute_i_n(measured: float, ia: float, ib: float, ic: float,
                 va: float = 0.0, vb: float = 0.0, vc: float = 0.0) -> float:
    """Return neutral-line current. Uses measured value if > 0.01 A, else
    falls back to a computed estimate from phase currents (assumes 120° phase
    offsets for 3-phase, 180° for split-phase).

    3-phase: |I_N| = sqrt(I1² + I2² + I3² − I1·I2 − I2·I3 − I1·I3)
    2-phase: |I_N| = |I1 − I2|
    1-phase: 0 (no meaningful neutral imbalance)
    """
    try:
        if measured and abs(measured) > 0.01:
            return float(measured)
        # Decide active phases by either voltage OR current being present
        # (some devices report only a single phase-voltage but all currents).
        a_on = (va > 0) or (abs(ia) > 0.01)
        b_on = (vb > 0) or (abs(ib) > 0.01)
        c_on = (vc > 0) or (abs(ic) > 0.01)
        active = sum(1 for x in (a_on, b_on, c_on) if x)
        if active >= 3:
            val = ia * ia + ib * ib + ic * ic - ia * ib - ib * ic - ia * ic
            return round(math.sqrt(val) if val > 0 else 0.0, 3)
        if active == 2:
            if a_on and b_on:
                return round(abs(ia - ib), 3)
            if a_on and c_on:
                return round(abs(ia - ic), 3)
            if b_on and c_on:
                return round(abs(ib - ic), 3)
        return 0.0
    except Exception:
        return float(measured or 0.0)


# ── Scriptable Widget JS (served via /widget.js) ───────────────────────────
_SCRIPTABLE_WIDGET_JS = r"""
// Shelly Energy Analyzer – iOS Scriptable Widget
// Load this script in the Scriptable app.
// Widget parameter: IP:PORT of your analyzer (e.g. 192.168.1.50:8765)
//
// Setup: when adding the widget, tap "Parameter"
// and enter the address, e.g. "192.168.1.50:8765"

const PARAM = args.widgetParameter || "192.168.1.50:8765";
const DARK = Device.isUsingDarkAppearance();

// Colors
const C = {
  bg:      DARK ? new Color("#1a1a1a") : new Color("#ffffff"),
  card:    DARK ? new Color("#252525") : new Color("#f5f5f5"),
  text:    DARK ? new Color("#eeeeee") : new Color("#222222"),
  muted:   DARK ? new Color("#888888") : new Color("#999999"),
  accent:  new Color("#ff9800"),
  green:   new Color("#4caf50"),
  red:     new Color("#e53935"),
  blue:    new Color("#2196F3"),
  co2green: new Color("#4caf50"),
  co2yellow: new Color("#ffeb3b"),
  co2red:   new Color("#e53935"),
};

// Try HTTPS first (self-signed cert), fall back to HTTP
let data;
let BASE;
for (const proto of ["https", "http"]) {
  BASE = proto + "://" + PARAM;
  try {
    const req = new Request(BASE + "/api/widget");
    req.timeoutInterval = 6;
    data = await req.loadJSON();
    if (data) break;
  } catch(e) {
    data = null;
  }
}
if (!data) {
  const w = new ListWidget();
  w.backgroundColor = C.bg;
  const t1 = w.addText("⚡ Offline");
  t1.font = Font.boldSystemFont(14);
  t1.textColor = C.red;
  w.addSpacer(4);
  const t2 = w.addText(PARAM);
  t2.font = Font.systemFont(11);
  t2.textColor = C.muted;
  w.addSpacer(2);
  const t3 = w.addText("Check WiFi & IP address");
  t3.font = Font.systemFont(10);
  t3.textColor = C.muted;
  Script.setWidget(w);
  Script.complete();
  return;
}

const family = config.widgetFamily || "medium";

let widget;
if (family === "small")       widget = buildSmall(data);
else if (family === "large")  widget = buildLarge(data);
else                          widget = buildMedium(data);

// Auto-refresh every 5 minutes
widget.refreshAfterDate = new Date(Date.now() + 5 * 60 * 1000);
Script.setWidget(widget);

// When tapped (running in-app): show live table + open dashboard
if (!config.runsInWidget) {
  const table = new UITable();
  table.showSeparators = true;

  function addRow(title, value, color) {
    const r = new UITableRow();
    const t = r.addText(title);
    t.widthWeight = 40;
    const v = r.addText(value);
    v.widthWeight = 60;
    if (color) v.titleColor = color;
    table.addRow(r);
  }

  addRow("⚡ Power", fmt(data.power_w, 0) + " W");
  addRow("📅 Today", fmt(data.today_kwh, 2) + " kWh  ·  " + fmt(data.today_eur, 2) + " €");
  addRow("📆 Month", fmt(data.month_kwh, 1) + " kWh  ·  " + fmt(data.month_eur, 2) + " €");
  addRow("📊 Forecast", fmt(data.proj_kwh, 0) + " kWh  ·  " + fmt(data.proj_eur, 2) + " €");
  if (data.spot_enabled && data.spot_ct != null) {
    const delta = data.spot_ct - data.fixed_ct;
    const sign = delta > 0 ? "+" : "";
    const col = delta <= 0 ? Color.green() : Color.red();
    addRow("💰 Spot price", data.spot_ct.toFixed(1) + " ct/kWh (" + sign + delta.toFixed(1) + " ct)", col);
    addRow("💰 Fixed price", data.fixed_ct.toFixed(1) + " ct/kWh", C.blue);
  }
  if (data.co2_enabled && data.co2_current != null) {
    const co2Col = co2Color(data.co2_current, data.co2_green_thr, data.co2_dirty_thr);
    addRow("🌿 CO₂ Intensity", data.co2_current.toFixed(0) + " g/kWh", co2Col);
  }
  if (data.devices && data.devices.length > 0) {
    addRow("", "");
    for (const dev of data.devices) {
      addRow("🏠 " + dev.name, fmt(dev.power_w,0) + " W  ·  " + fmt(dev.today_kwh,2) + " kWh  ·  " + fmt(dev.today_eur,2) + " €");
    }
  }

  // Open dashboard button
  const btnRow = new UITableRow();
  const btn = btnRow.addButton("🌐 Open Dashboard");
  btn.onTap = () => Safari.open(BASE);
  table.addRow(btnRow);

  await table.present();
}

Script.complete();

// ─── Small Widget: Current price + power ────────────────────────
function buildSmall(d) {
  const w = new ListWidget();
  w.backgroundColor = C.bg;
  w.setPadding(12, 14, 12, 14);
  w.url = BASE;

  // Title
  const title = w.addText("⚡ Energy");
  title.font = Font.boldSystemFont(11);
  title.textColor = C.accent;
  w.addSpacer(6);

  // Current power
  const pw = w.addText(fmt(d.power_w, 0) + " W");
  pw.font = Font.boldSystemFont(22);
  pw.textColor = C.text;

  // Today
  const tl = w.addText("Today: " + fmt(d.today_kwh, 1) + " kWh · " + fmt(d.today_eur, 2) + " €");
  tl.font = Font.systemFont(10);
  tl.textColor = C.muted;
  w.addSpacer(4);

  // Spot price
  if (d.spot_enabled && d.spot_ct != null) {
    const delta = d.spot_ct - d.fixed_ct;
    const arrow = delta > 0 ? "▲" : "▼";
    const sign = delta > 0 ? "+" : "";
    const col = delta <= 0 ? C.green : C.red;
    const sp = w.addText(d.spot_ct.toFixed(1) + " ct " + arrow + sign + delta.toFixed(1));
    sp.font = Font.boldSystemFont(13);
    sp.textColor = col;
  }

  // CO2 intensity
  if (d.co2_enabled && d.co2_current != null) {
    w.addSpacer(2);
    const co2 = w.addText("🌿 " + d.co2_current.toFixed(0) + " g/kWh");
    co2.font = Font.mediumSystemFont(10);
    co2.textColor = co2Color(d.co2_current, d.co2_green_thr, d.co2_dirty_thr);
  }

  w.addSpacer();
  const ts = w.addText(fmtTime(d.ts));
  ts.font = Font.systemFont(8);
  ts.textColor = C.muted;
  return w;
}

// ─── Medium Widget: Price + consumption + mini chart ────────────
function buildMedium(d) {
  const w = new ListWidget();
  w.backgroundColor = C.bg;
  w.setPadding(12, 14, 8, 14);
  w.url = BASE;

  // Header row
  const hStack = w.addStack();
  hStack.layoutHorizontally();
  hStack.centerAlignContent();
  const title = hStack.addText("⚡ Shelly Analyzer");
  title.font = Font.boldSystemFont(11);
  title.textColor = C.accent;
  hStack.addSpacer();
  const ts = hStack.addText(fmtTime(d.ts));
  ts.font = Font.systemFont(9);
  ts.textColor = C.muted;
  w.addSpacer(3);

  // Top row: power + spot + stats
  const topRow = w.addStack();
  topRow.layoutHorizontally();

  // Left: power + spot
  const left = topRow.addStack();
  left.layoutVertically();

  const pw = left.addText(fmt(d.power_w, 0) + " W");
  pw.font = Font.boldSystemFont(18);
  pw.textColor = C.text;

  if (d.spot_enabled && d.spot_ct != null) {
    const delta = d.spot_ct - d.fixed_ct;
    const arrow = delta > 0 ? "▲" : "▼";
    const sign = delta > 0 ? "+" : "";
    const col = delta <= 0 ? C.green : C.red;
    const sp = left.addText(d.spot_ct.toFixed(1) + " ct " + arrow + sign + delta.toFixed(1));
    sp.font = Font.boldSystemFont(11);
    sp.textColor = col;
  }

  if (d.co2_enabled && d.co2_current != null) {
    const co2t = left.addText("CO₂ " + d.co2_current.toFixed(0) + " g/kWh");
    co2t.font = Font.boldSystemFont(10);
    co2t.textColor = co2Color(d.co2_current, d.co2_green_thr, d.co2_dirty_thr);
  }

  topRow.addSpacer();

  // Right: today + month
  const right = topRow.addStack();
  right.layoutVertically();
  const tVal = right.addText(fmt(d.today_kwh, 1) + " kWh · " + fmt(d.today_eur, 2) + " €");
  tVal.font = Font.mediumSystemFont(10);
  tVal.textColor = C.text;
  const tLbl = right.addText("Today");
  tLbl.font = Font.systemFont(8);
  tLbl.textColor = C.muted;
  right.addSpacer(2);
  const mVal = right.addText(fmt(d.month_kwh, 1) + " kWh · " + fmt(d.month_eur, 2) + " €");
  mVal.font = Font.mediumSystemFont(10);
  mVal.textColor = C.text;
  const mLbl = right.addText("Month");
  mLbl.font = Font.systemFont(8);
  mLbl.textColor = C.muted;

  w.addSpacer(3);

  // Charts – full width, fixed height
  const mCW = 290;
  if (d.spot_enabled && d.spot_chart && d.spot_chart.length > 2) {
    const chartImg = drawMiniChart(d.spot_chart, d.fixed_ct, mCW, 40);
    const img = w.addImage(chartImg);
    img.imageSize = new Size(mCW, 40);
  }

  if (d.co2_enabled && d.co2_chart && d.co2_chart.length > 2) {
    w.addSpacer(1);
    const co2Img = drawCo2Chart(d.co2_chart, d.co2_green_thr, d.co2_dirty_thr, mCW, 40);
    const img2 = w.addImage(co2Img);
    img2.imageSize = new Size(mCW, 40);
  }

  return w;
}

// ─── Large Widget: Full detail ──────────────────────────────────
function buildLarge(d) {
  const w = new ListWidget();
  w.backgroundColor = C.bg;
  w.setPadding(14, 16, 10, 16);
  w.url = BASE;

  // Header
  const hStack = w.addStack();
  hStack.layoutHorizontally();
  const title = hStack.addText("⚡ Shelly Energy Analyzer");
  title.font = Font.boldSystemFont(13);
  title.textColor = C.accent;
  hStack.addSpacer();
  const ts = hStack.addText(fmtTime(d.ts));
  ts.font = Font.systemFont(9);
  ts.textColor = C.muted;
  w.addSpacer(6);

  // Power
  const pw = w.addText(fmt(d.power_w, 0) + " W");
  pw.font = Font.boldSystemFont(26);
  pw.textColor = C.text;
  w.addSpacer(4);

  // Spot price prominent
  if (d.spot_enabled && d.spot_ct != null) {
    const delta = d.spot_ct - d.fixed_ct;
    const arrow = delta > 0 ? "▲" : "▼";
    const sign = delta > 0 ? "+" : "";
    const col = delta <= 0 ? C.green : C.red;
    const spRow = w.addStack();
    spRow.layoutHorizontally();
    spRow.centerAlignContent();
    const sp1 = spRow.addText("Spot price: " + d.spot_ct.toFixed(1) + " ct/kWh");
    sp1.font = Font.boldSystemFont(14);
    sp1.textColor = col;
    spRow.addSpacer(6);
    const sp2 = spRow.addText(arrow + " " + sign + delta.toFixed(1) + " ct");
    sp2.font = Font.systemFont(11);
    sp2.textColor = col;
    w.addSpacer(2);
    const fixLbl = w.addText("Fixed price: " + d.fixed_ct.toFixed(1) + " ct/kWh");
    fixLbl.font = Font.systemFont(10);
    fixLbl.textColor = C.blue;
    w.addSpacer(4);
  }

  // Spot chart – full width, fixed height
  const lCW = 330;
  if (d.spot_enabled && d.spot_chart && d.spot_chart.length > 2) {
    const chartImg = drawMiniChart(d.spot_chart, d.fixed_ct, lCW, 70);
    const img = w.addImage(chartImg);
    img.imageSize = new Size(lCW, 70);
    w.addSpacer(4);
  }

  // CO2 section
  if (d.co2_enabled && d.co2_current != null) {
    const co2Row = w.addStack();
    co2Row.layoutHorizontally();
    co2Row.centerAlignContent();
    const co2Lbl = co2Row.addText("🌿 CO₂: " + d.co2_current.toFixed(0) + " g/kWh");
    co2Lbl.font = Font.boldSystemFont(12);
    co2Lbl.textColor = co2Color(d.co2_current, d.co2_green_thr, d.co2_dirty_thr);
    w.addSpacer(2);
  }

  // CO2 chart – full width, fixed height
  if (d.co2_enabled && d.co2_chart && d.co2_chart.length > 2) {
    const co2Img = drawCo2Chart(d.co2_chart, d.co2_green_thr, d.co2_dirty_thr, lCW, 55);
    const co2ImgW = w.addImage(co2Img);
    co2ImgW.imageSize = new Size(lCW, 55);
    w.addSpacer(4);
  }

  // Metrics grid
  const g = w.addStack();
  g.layoutHorizontally();
  addMetric(g, "Today", fmt(d.today_kwh,1) + " kWh", fmt(d.today_eur,2) + " €");
  g.addSpacer();
  addMetric(g, "Month", fmt(d.month_kwh,1) + " kWh", fmt(d.month_eur,2) + " €");
  g.addSpacer();
  addMetric(g, "Forecast", fmt(d.proj_kwh,0) + " kWh", fmt(d.proj_eur,2) + " €");

  // Spot today cost
  if (d.spot_enabled && d.spot_today_eur != null) {
    w.addSpacer(4);
    const stRow = w.addStack();
    stRow.layoutHorizontally();
    const stLbl = stRow.addText("⚡ Spot cost today: ");
    stLbl.font = Font.systemFont(10);
    stLbl.textColor = C.muted;
    const diff = d.spot_today_eur - d.today_eur;
    const stVal = stRow.addText(fmt(d.spot_today_eur,2) + " € (" + (diff >= 0 ? "+" : "") + fmt(diff,2) + " €)");
    stVal.font = Font.mediumSystemFont(10);
    stVal.textColor = diff <= 0 ? C.green : C.red;
  }

  // Per-device breakdown
  if (d.devices && d.devices.length > 0) {
    w.addSpacer(6);
    for (const dev of d.devices) {
      const dr = w.addStack();
      dr.layoutHorizontally();
      dr.centerAlignContent();
      const dn = dr.addText(dev.name);
      dn.font = Font.mediumSystemFont(10);
      dn.textColor = C.text;
      dn.lineLimit = 1;
      dr.addSpacer();
      const dv = dr.addText(fmt(dev.power_w,0) + " W  " + fmt(dev.today_kwh,1) + " kWh  " + fmt(dev.today_eur,2) + " €");
      dv.font = Font.systemFont(10);
      dv.textColor = C.muted;
    }
  }

  w.addSpacer();
  return w;
}

// ─── Helpers ────────────────────────────────────────────────────
function fmt(v, dec) {
  if (v == null) return "–";
  return Number(v).toFixed(dec);
}

function fmtTime(ts) {
  if (!ts) return "";
  const d = new Date(ts * 1000);
  return ("0" + d.getHours()).slice(-2) + ":" + ("0" + d.getMinutes()).slice(-2);
}

function addMetric(parent, label, line1, line2) {
  const s = parent.addStack();
  s.layoutVertically();
  const l = s.addText(label);
  l.font = Font.systemFont(9);
  l.textColor = C.muted;
  const v1 = s.addText(line1);
  v1.font = Font.mediumSystemFont(11);
  v1.textColor = C.text;
  if (line2) {
    const v2 = s.addText(line2);
    v2.font = Font.boldSystemFont(12);
    v2.textColor = C.text;
  }
}

// ─── CO2 Color Helper ──────────────────────────────────────────
function co2Color(val, green, dirty) {
  if (val <= green) return new Color("#4caf50");
  if (val <= (green + dirty) / 2) return new Color("#8bc34a");
  if (val <= dirty) return new Color("#ff9800");
  return new Color("#e53935");
}

// ─── Mini CO2 Chart (DrawContext) ──────────────────────────────
function drawCo2Chart(chart, greenThr, dirtyThr, W, H) {
  const dc = new DrawContext();
  dc.size = new Size(W, H);
  dc.opaque = false;
  dc.respectScreenScale = true;

  const vals = chart.map(p => p[1]);
  let minV = Math.min(...vals);
  let maxV = Math.max(...vals);
  let rng = maxV - minV;
  if (rng < 10) rng = 10;
  minV = Math.max(0, minV - rng * 0.1);
  maxV = maxV + rng * 0.15;
  const vR = maxV - minV;

  const pad = {l: 2, r: 2, t: 10, b: 2};
  const pW = W - pad.l - pad.r;
  const pH = H - pad.t - pad.b;
  const barW = Math.max(1, pW / chart.length - 1);

  const now = Date.now() / 1000;

  // Label (includes latest value if present)
  dc.setFont(Font.boldSystemFont(7));
  dc.setTextColor(DARK ? new Color("#888888") : new Color("#999999"));
  const _last = chart.length > 0 ? chart[chart.length-1][1] : null;
  const _lbl = "CO₂ g/kWh · 24h" + (_last != null ? " · " + _last.toFixed(0) : "");
  dc.drawTextInRect(_lbl, new Rect(pad.l, 0, W, 10));

  // Bars
  for (let i = 0; i < chart.length; i++) {
    const v = chart[i][1];
    const ts = chart[i][0];
    const x = pad.l + (i / chart.length) * pW;
    const barH = Math.max(1, ((v - minV) / vR) * pH);
    const y = pad.t + pH - barH;

    let hex;
    if (v <= greenThr) hex = "#4caf50";
    else if (v <= (greenThr + dirtyThr) / 2) hex = "#8bc34a";
    else if (v <= dirtyThr) hex = "#ff9800";
    else hex = "#e53935";

    const a = ts > now ? 0.45 : 0.85;
    dc.setFillColor(new Color(hex, a));
    dc.fillRect(new Rect(x, y, barW, barH));
  }

  // Green threshold line
  if (greenThr >= minV && greenThr <= maxV) {
    const gY = pad.t + pH * (1 - (greenThr - minV) / vR);
    dc.setStrokeColor(new Color("#4caf50", 0.6));
    dc.setLineWidth(0.5);
    const gp = new Path();
    gp.move(new Point(pad.l, gY));
    gp.addLine(new Point(W - pad.r, gY));
    dc.addPath(gp);
    dc.strokePath();
  }

  // Dirty threshold line
  if (dirtyThr >= minV && dirtyThr <= maxV) {
    const dY = pad.t + pH * (1 - (dirtyThr - minV) / vR);
    dc.setStrokeColor(new Color("#e53935", 0.6));
    dc.setLineWidth(0.5);
    const dp = new Path();
    dp.move(new Point(pad.l, dY));
    dp.addLine(new Point(W - pad.r, dY));
    dc.addPath(dp);
    dc.strokePath();
  }

  return dc.getImage();
}

// ─── Mini Spot Chart (DrawContext) ──────────────────────────────
function drawMiniChart(chart, fixedCt, W, H) {
  const dc = new DrawContext();
  dc.size = new Size(W, H);
  dc.opaque = false;
  dc.respectScreenScale = true;

  const vals = chart.map(p => p[1]);
  const allV = vals.concat([fixedCt]);
  let minV = Math.min(...allV);
  let maxV = Math.max(...allV);
  let rng = maxV - minV;
  if (rng < 1) rng = 1;
  minV = Math.max(0, minV - rng * 0.1);
  maxV = maxV + rng * 0.15;
  const vR = maxV - minV;

  const pad = {l: 2, r: 2, t: 2, b: 2};
  const pW = W - pad.l - pad.r;
  const pH = H - pad.t - pad.b;
  const barW = Math.max(1, pW / chart.length - 1);

  const now = Date.now() / 1000;

  // Bars
  for (let i = 0; i < chart.length; i++) {
    const v = chart[i][1];
    const ts = chart[i][0];
    const x = pad.l + (i / chart.length) * pW;
    const barH = Math.max(1, ((v - minV) / vR) * pH);
    const y = pad.t + pH - barH;

    const ratio = fixedCt > 0 ? v / fixedCt : 1;
    let hex;
    if (ratio <= 0.7) hex = "#4caf50";
    else if (ratio <= 0.9) hex = "#8bc34a";
    else if (ratio <= 1.0) hex = "#ffeb3b";
    else if (ratio <= 1.2) hex = "#ff9800";
    else hex = "#e53935";

    // Bake alpha into color (Scriptable has no setAlpha)
    const a = ts > now ? 0.45 : 0.85;
    const col = new Color(hex, a);
    dc.setFillColor(col);
    dc.fillRect(new Rect(x, y, barW, barH));
  }

  // Fixed price line (dashed = two thin lines)
  const fY = pad.t + pH * (1 - (fixedCt - minV) / vR);
  dc.setStrokeColor(new Color("#2196F3"));
  dc.setLineWidth(1);
  const path = new Path();
  path.move(new Point(pad.l, fY));
  path.addLine(new Point(W - pad.r, fY));
  dc.addPath(path);
  dc.strokePath();

  return dc.getImage();
}
"""


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
      flex: 1 1 0%;
      overflow-y: auto;
      -webkit-overflow-scrolling: touch;
      overflow-x: hidden;
      padding: 10px;
      padding-bottom: calc(120px + env(safe-area-inset-bottom, 0px));
      min-height: 0;
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
    @media (min-width: 900px) {{
      #bottom-nav {{ justify-content: center; }}
    }}
    #bottom-nav::-webkit-scrollbar {{ display: none; }}
    /* ── Hamburger (hidden on desktop, shown on mobile) ── */
    #btn-hamburger {{ display: none; }}
    #nav-drawer-overlay {{
      position: fixed; inset: 0;
      background: rgba(0,0,0,0.45);
      z-index: 200; display: none;
    }}
    #nav-drawer {{
      position: fixed; top: 0; left: 0; bottom: 0;
      width: min(78vw, 280px);
      background: var(--card);
      border-right: 1px solid var(--border);
      z-index: 201; display: none;
      overflow-y: auto;
      padding: 12px 0;
      box-shadow: 2px 0 10px rgba(0,0,0,0.25);
    }}
    body.nav-open #nav-drawer,
    body.nav-open #nav-drawer-overlay {{ display: block; }}
    .drawer-item {{
      display: flex; align-items: center; gap: 10px;
      padding: 12px 16px; color: var(--fg);
      text-decoration: none; font-size: 14px;
      border: none; background: none; width: 100%;
      text-align: left; cursor: pointer;
      border-bottom: 1px solid var(--border);
    }}
    .drawer-item.active {{ color: var(--accent); font-weight: 600; }}
    .drawer-item .drawer-ico {{ font-size: 18px; width: 22px; text-align: center; }}
    /* Hamburger disabled on mobile — bottom-nav stays visible & scrollable */
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
    /* ── Control tab toggle switch ── */
    .ctrl-toggle {{ position:relative; display:inline-block; width:44px; height:24px; }}
    .ctrl-toggle input {{ opacity:0; width:0; height:0; }}
    .ctrl-slider {{ position:absolute; cursor:pointer; top:0; left:0; right:0; bottom:0;
      background:var(--border); border-radius:24px; transition:.2s; }}
    .ctrl-slider:before {{ content:""; position:absolute; height:18px; width:18px; left:3px; bottom:3px;
      background:var(--fg); border-radius:50%; transition:.2s; }}
    .ctrl-toggle input:checked + .ctrl-slider {{ background:var(--accent); }}
    .ctrl-toggle input:checked + .ctrl-slider:before {{ transform:translateX(20px); }}
    .ctrl-range {{ -webkit-appearance:none; width:100%; height:6px; background:var(--border);
      border-radius:3px; outline:none; }}
    .ctrl-range::-webkit-slider-thumb {{ -webkit-appearance:none; width:18px; height:18px;
      background:var(--accent); border-radius:50%; cursor:pointer; }}
    .ctrl-range::-moz-range-thumb {{ width:18px; height:18px;
      background:var(--accent); border-radius:50%; cursor:pointer; border:none; }}
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
    /* Live view always single column, even on wide monitors */
    #live-grid.card-grid {{ grid-template-columns: 1fr !important; }}
    /* Desktop: limit all tab content to ~2/3 viewport width, centered.
       Live sparklines grow with viewport height so cards fill the window vertically.
       Mobile keeps fixed heights + full width. */
    @media (min-width: 900px) {{
      .pane.active {{ max-width: 80%; margin-left: auto; margin-right: auto; }}
      canvas.sparkline {{ height: clamp(56px, 11vh, 180px); }}
      canvas.sparkline-sm {{ height: clamp(40px, 8vh, 130px); }}
    }}
    /* Plots tab: iframe owns the scrolling, outer panes container must not scroll
       (applies on both desktop and mobile so iframe has a defined height everywhere) */
    #pane-plots.active {{
      max-width: 100%;
      width: 100%;
      height: calc(100dvh - 140px);   /* header + bottom-nav safe area */
      padding: 0;
      margin: 0;
    }}
    #panes:has(#pane-plots.active) {{
      overflow: hidden;
      padding: 0 !important;
      padding-bottom: 0 !important;
    }}
    @media (min-width: 900px) {{
      #pane-plots.active {{ height: calc(100vh - 96px); }}
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
    /* ── NILM Tab ── */
    .nilm-metrics {{
      display: grid;
      grid-template-columns: repeat(2,1fr);
      gap: 8px;
      margin-bottom: 12px;
    }}
    @media (min-width: 600px) {{
      .nilm-metrics {{ grid-template-columns: repeat(4,1fr); }}
    }}
    .nilm-metric-card {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 14px 10px;
      text-align: center;
    }}
    .nilm-pattern-grid {{
      display: grid;
      grid-template-columns: 1fr;
      gap: 10px;
    }}
    @media (min-width: 560px) {{
      .nilm-pattern-grid {{ grid-template-columns: 1fr 1fr; }}
    }}
    @media (min-width: 960px) {{
      .nilm-pattern-grid {{ grid-template-columns: repeat(3, 1fr); }}
    }}
    .nilm-pattern-card {{
      background: var(--bg);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 12px;
    }}
    .nilm-stat {{
      display: flex;
      flex-direction: column;
      align-items: center;
    }}
    .nilm-stat-val {{
      font-size: 15px;
      font-weight: 800;
      line-height: 1.2;
    }}
    .nilm-stat-lbl {{
      font-size: 10px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: .3px;
    }}
    .nilm-two-col {{
      display: grid;
      grid-template-columns: 1fr;
      gap: 10px;
    }}
    @media (min-width: 700px) {{
      .nilm-two-col {{ grid-template-columns: 1fr 1fr; }}
    }}
    .nilm-timeline {{
      position: relative;
      padding-left: 20px;
    }}
    .nilm-tl-item {{
      display: flex;
      gap: 10px;
      align-items: flex-start;
      padding: 6px 0;
      border-bottom: 1px solid var(--border);
    }}
    .nilm-tl-item:last-child {{ border-bottom: none; }}
    .nilm-tl-dot {{
      width: 10px;
      height: 10px;
      border-radius: 50%;
      flex-shrink: 0;
      margin-top: 4px;
    }}
    .nilm-tl-body {{ flex: 1; min-width: 0; }}
    .nilm-sig-grid {{
      display: grid;
      grid-template-columns: 1fr;
      gap: 4px;
    }}
    @media (min-width: 560px) {{
      .nilm-sig-grid {{ grid-template-columns: 1fr 1fr; }}
    }}
    @media (min-width: 960px) {{
      .nilm-sig-grid {{ grid-template-columns: repeat(3, 1fr); }}
    }}
    .nilm-sig-item {{
      display: flex;
      gap: 8px;
      align-items: center;
      padding: 6px 8px;
      background: var(--bg);
      border-radius: 8px;
    }}
    /* Standby device grid: fills full width, responsive */
    .sb-dev-grid {{
      display: grid;
      grid-template-columns: 1fr;
      gap: 10px;
    }}
    @media (min-width: 560px) {{
      .sb-dev-grid[data-cols="2"] {{ grid-template-columns: 1fr 1fr; }}
      .sb-dev-grid[data-cols="3"] {{ grid-template-columns: 1fr 1fr; }}
      .sb-dev-grid[data-cols="4"] {{ grid-template-columns: 1fr 1fr; }}
    }}
    @media (min-width: 960px) {{
      .sb-dev-grid[data-cols="3"] {{ grid-template-columns: repeat(3, 1fr); }}
      .sb-dev-grid[data-cols="4"] {{ grid-template-columns: repeat(4, 1fr); }}
    }}
    /* Override pane max-width for NILM to use more space on desktop */
    /* (80% pane width now applied globally via .pane.active rule) */
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
      <button id="btn-hamburger" class="icon-btn" title="Menu" onclick="toggleNavDrawer()">☰</button>
      <button id="btn-freeze" class="icon-btn" title="{web_btn_freeze_title}" style="display:none">▶</button>
      <button id="btn-live-settings" class="icon-btn" title="{web_btn_settings_title}" onclick="window.location.href='/settings#sec-devices'">⚙</button>
      <button id="btn-theme" class="icon-btn" title="{web_btn_theme_title}">☀</button>
    </div>
  </header>

  <div id="panes">
    <!-- Live -->
    <div id="pane-live" class="pane active">
      <div id="update-banner" style="display:none;margin:0 0 10px 0;padding:10px 14px;border-radius:10px;background:linear-gradient(90deg,#3b82f6,#22c55e);color:#fff;font-size:13px;cursor:pointer;box-shadow:0 2px 8px rgba(0,0,0,0.15)" onclick="goToUpdates()">
        <span style="font-size:16px">⬆</span>
        <strong id="update-banner-title">{web_update_banner_title}</strong>
        <span id="update-banner-tag" style="margin-left:6px;font-weight:600"></span>
        <span style="opacity:0.85;margin-left:10px">→ {web_update_banner_open}</span>
      </div>
      <div id="live-timescale" style="display:flex;gap:6px;flex-wrap:wrap;padding:0 0 8px 0"></div>
      <div id="live-grid" class="card-grid"></div>
    </div>

    <!-- Plots (historical W/V/A/VAR/cos φ, phases + totals, time ranges, kWh) -->
    <div id="pane-plots" class="pane">
      <iframe id="plots-frame" src="about:blank" loading="lazy"
        style="width:100%;height:100%;border:0;border-radius:0;background:var(--card);display:block"></iframe>
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
          <option value="eur">EUR</option>
          <option value="co2">g CO₂</option>
        </select>
        <select id="hm-year"></select>
        <button class="btn btn-outline" onclick="loadHeatmap()">↻</button>
      </div>
      <div id="hm-stats-wrap"></div>
      <div id="hm-calendar-wrap"></div>
      <div id="hm-hourly-wrap" style="margin-top:14px"></div>
      <div id="hm-charts-wrap" style="margin-top:14px"></div>
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

    <!-- New feature panes (same pattern as costs/forecast/standby) -->
    <div id="pane-smart_sched" class="pane">
      <div id="ss-content"><p class="loading-msg">{web_loading}</p></div>
    </div>
    <div id="pane-ev_log" class="pane">
      <div id="ev-content"><p class="loading-msg">{web_loading}</p></div>
    </div>
    <div id="pane-tariff" class="pane">
      <div id="tariff-content"><p class="loading-msg">{web_loading}</p></div>
    </div>
    <div id="pane-battery" class="pane">
      <div id="bat-content"><p class="loading-msg">{web_loading}</p></div>
    </div>
    <div id="pane-advisor" class="pane">
      <div id="advisor-content"><p class="loading-msg">{web_loading}</p></div>
    </div>
    <div id="pane-goals" class="pane">
      <div id="goals-content"><p class="loading-msg">{web_loading}</p></div>
    </div>
    <div id="pane-tenants" class="pane">
      <div id="tenants-content"><p class="loading-msg">{web_loading}</p></div>
    </div>
    <!-- NILM Statistics -->
    <div id="pane-nilm" class="pane">
      <div id="nilm-content"><p class="loading-msg">{web_loading}</p></div>
    </div>

    <div id="pane-control" class="pane">
      <div id="control-content"><p class="loading-msg">{web_loading}</p></div>
    </div>

    <div id="pane-sync" class="pane">
      <div class="card" style="margin-bottom:8px">
        <div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px">
          <button class="btn" onclick="syncAll('incremental')">{sync_btn_incremental}</button>
          <button class="btn" onclick="syncAll('all')">{sync_btn_full}</button>
          <button class="btn" onclick="syncAll('day')">{sync_btn_day}</button>
          <button class="btn" onclick="syncAll('week')">{sync_btn_week}</button>
          <button class="btn" onclick="syncAll('month')">{sync_btn_month}</button>
          <span style="flex:1"></span>
          <button class="btn" onclick="refreshSyncStatus()">{sync_btn_status}</button>
          <label style="display:flex;align-items:center;gap:4px;font-size:11px"><input type="checkbox" id="log-autoscroll" checked> {sync_opt_autoscroll}</label>
          <label style="display:flex;align-items:center;gap:4px;font-size:11px"><input type="checkbox" id="log-include-http" onchange="toggleLogHttp(this.checked)"> {sync_opt_http_logs}</label>
        </div>
        <div id="sync-status-panel" style="font-size:12px;color:var(--muted);margin-bottom:6px">{sync_status_loading}</div>
      </div>
      <div class="card">
        <div style="font-size:12px;font-weight:650;color:var(--muted);text-transform:uppercase;margin-bottom:6px">{sync_log}</div>
        <div id="sync-log" style="font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:11px;background:var(--bg);border-radius:6px;padding:8px;height:60vh;overflow-y:auto;white-space:pre-wrap;line-height:1.4"></div>
      </div>
    </div>

  <nav id="bottom-nav">
    <button class="nav-btn active" onclick="switchPane('live',this)">
      <span class="nav-icon">📡</span>
      <span class="nav-label">{web_tab_live}</span>
    </button>
    <button class="nav-btn" onclick="switchPane('plots',this)">
      <span class="nav-icon">📊</span>
      <span class="nav-label">Plots</span>
    </button>
    <button class="nav-btn" onclick="switchPane('costs',this)">
      <span class="nav-icon">💰</span>
      <span class="nav-label">{web_tab_costs}</span>
    </button>
    <button class="nav-btn" onclick="switchPane('heatmap',this)">
      <span class="nav-icon">🔥</span>
      <span class="nav-label">{web_tab_heatmap}</span>
    </button>
    <button class="nav-btn" data-feature="solar" onclick="switchPane('solar',this)">
      <span class="nav-icon">☀️</span>
      <span class="nav-label">{web_tab_solar}</span>
    </button>
    <button class="nav-btn" data-feature="weather" onclick="switchPane('weather',this)">
      <span class="nav-icon">🌡️</span>
      <span class="nav-label">{web_tab_weather}</span>
    </button>
    <button class="nav-btn" onclick="switchPane('compare',this)">
      <span class="nav-icon">🔀</span>
      <span class="nav-label">{web_tab_compare}</span>
    </button>
    <button class="nav-btn" data-feature="co2" onclick="switchPane('co2',this)">
      <span class="nav-icon">🌍</span>
      <span class="nav-label">{web_tab_co2}</span>
    </button>
    <button class="nav-btn" data-feature="anomalies" onclick="switchPane('anomalies',this)">
      <span class="nav-icon">🔍</span>
      <span class="nav-label">{web_tab_anomalies}</span>
    </button>
    <button class="nav-btn" data-feature="forecast" onclick="switchPane('forecast',this)">
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
    <button class="nav-btn" data-feature="ev" onclick="switchPane('ev',this)">
      <span class="nav-icon">🔌</span>
      <span class="nav-label">{web_tab_ev}</span>
    </button>
    <button class="nav-btn" onclick="switchPane('export',this)">
      <span class="nav-icon">📥</span>
      <span class="nav-label">{web_tab_export}</span>
    </button>
    <button class="nav-btn" data-feature="smart_sched" onclick="switchPane('smart_sched',this)">
      <span class="nav-icon">⏱</span>
      <span class="nav-label">Schedule</span>
    </button>
    <button class="nav-btn" data-feature="ev_log" onclick="switchPane('ev_log',this)">
      <span class="nav-icon">🚗</span>
      <span class="nav-label">EV Log</span>
    </button>
    <button class="nav-btn" data-feature="tariff" onclick="switchPane('tariff',this)">
      <span class="nav-icon">💱</span>
      <span class="nav-label">Tariff</span>
    </button>
    <button class="nav-btn" data-feature="battery" onclick="switchPane('battery',this)">
      <span class="nav-icon">🔋</span>
      <span class="nav-label">Battery</span>
    </button>
    <button class="nav-btn" data-feature="advisor" onclick="switchPane('advisor',this)">
      <span class="nav-icon">🤖</span>
      <span class="nav-label">Advisor</span>
    </button>
    <button class="nav-btn" data-feature="goals" onclick="switchPane('goals',this)">
      <span class="nav-icon">🏆</span>
      <span class="nav-label">Goals</span>
    </button>
    <button class="nav-btn" data-feature="tenants" onclick="switchPane('tenants',this)">
      <span class="nav-icon">🏘</span>
      <span class="nav-label">{web_tab_tenants}</span>
    </button>
    <button class="nav-btn" onclick="switchPane('nilm',this)">
      <span class="nav-icon">🧠</span>
      <span class="nav-label">NILM</span>
    </button>
    <button class="nav-btn" data-feature="device_control" onclick="switchPane('control',this)">
      <span class="nav-icon">🎛</span>
      <span class="nav-label">Control</span>
    </button>
    <button class="nav-btn" onclick="switchPane('sync',this)">
      <span class="nav-icon">🔄</span>
      <span class="nav-label">Sync</span>
    </button>
  </nav>

  <!-- Mobile hamburger drawer (mirrors bottom-nav) -->
  <div id="nav-drawer-overlay" onclick="toggleNavDrawer()"></div>
  <aside id="nav-drawer">
    <button class="drawer-item active" onclick="switchPaneFromDrawer('live',this)"><span class="drawer-ico">📡</span>{web_tab_live}</button>
    <button class="drawer-item" onclick="switchPaneFromDrawer('plots',this)"><span class="drawer-ico">📊</span>Plots</button>
    <button class="drawer-item" onclick="switchPaneFromDrawer('costs',this)"><span class="drawer-ico">💰</span>{web_tab_costs}</button>
    <button class="drawer-item" onclick="switchPaneFromDrawer('heatmap',this)"><span class="drawer-ico">🔥</span>{web_tab_heatmap}</button>
    <button class="drawer-item" data-feature="solar" onclick="switchPaneFromDrawer('solar',this)"><span class="drawer-ico">☀️</span>{web_tab_solar}</button>
    <button class="drawer-item" data-feature="weather" onclick="switchPaneFromDrawer('weather',this)"><span class="drawer-ico">🌡️</span>{web_tab_weather}</button>
    <button class="drawer-item" onclick="switchPaneFromDrawer('compare',this)"><span class="drawer-ico">🔀</span>{web_tab_compare}</button>
    <button class="drawer-item" data-feature="co2" onclick="switchPaneFromDrawer('co2',this)"><span class="drawer-ico">🌍</span>{web_tab_co2}</button>
    <button class="drawer-item" data-feature="anomalies" onclick="switchPaneFromDrawer('anomalies',this)"><span class="drawer-ico">🔍</span>{web_tab_anomalies}</button>
    <button class="drawer-item" data-feature="forecast" onclick="switchPaneFromDrawer('forecast',this)"><span class="drawer-ico">📈</span>{web_tab_forecast}</button>
    <button class="drawer-item" onclick="switchPaneFromDrawer('standby',this)"><span class="drawer-ico">🔌</span>{web_tab_standby}</button>
    <button class="drawer-item" onclick="switchPaneFromDrawer('sankey',this)"><span class="drawer-ico">⚡</span>{web_tab_sankey}</button>
    <button class="drawer-item" data-feature="ev" onclick="switchPaneFromDrawer('ev',this)"><span class="drawer-ico">🔌</span>{web_tab_ev}</button>
    <button class="drawer-item" onclick="switchPaneFromDrawer('export',this)"><span class="drawer-ico">📥</span>{web_tab_export}</button>
    <button class="drawer-item" data-feature="smart_sched" onclick="switchPaneFromDrawer('smart_sched',this)"><span class="drawer-ico">⏱</span>Schedule</button>
    <button class="drawer-item" data-feature="ev_log" onclick="switchPaneFromDrawer('ev_log',this)"><span class="drawer-ico">🚗</span>EV Log</button>
    <button class="drawer-item" data-feature="tariff" onclick="switchPaneFromDrawer('tariff',this)"><span class="drawer-ico">💱</span>Tariff</button>
    <button class="drawer-item" data-feature="battery" onclick="switchPaneFromDrawer('battery',this)"><span class="drawer-ico">🔋</span>Battery</button>
    <button class="drawer-item" data-feature="advisor" onclick="switchPaneFromDrawer('advisor',this)"><span class="drawer-ico">🤖</span>Advisor</button>
    <button class="drawer-item" data-feature="goals" onclick="switchPaneFromDrawer('goals',this)"><span class="drawer-ico">🏆</span>Goals</button>
    <button class="drawer-item" data-feature="tenants" onclick="switchPaneFromDrawer('tenants',this)"><span class="drawer-ico">🏘</span>{web_tab_tenants}</button>
    <button class="drawer-item" onclick="switchPaneFromDrawer('nilm',this)"><span class="drawer-ico">🧠</span>NILM</button>
    <button class="drawer-item" data-feature="device_control" onclick="switchPaneFromDrawer('control',this)"><span class="drawer-ico">🎛</span>Control</button>
    <button class="drawer-item" onclick="switchPaneFromDrawer('sync',this)"><span class="drawer-ico">🔄</span>Sync</button>
  </aside>
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

    <div style="margin-bottom:12px">
      <label style="font-size:12px;font-weight:600;color:var(--muted)">🌐 Language</label><br>
      <select id="lang-select" style="margin-top:4px;padding:6px 10px;border-radius:6px;border:1px solid var(--border);background:var(--card);color:var(--fg);font-size:13px" onchange="setLanguage(this.value)">
        <option value="de">Deutsch</option>
        <option value="en">English</option>
        <option value="es">Español</option>
        <option value="fr">Français</option>
        <option value="pt">Português</option>
        <option value="it">Italiano</option>
        <option value="pl">Polski</option>
        <option value="cs">Čeština</option>
        <option value="ru">Русский</option>
      </select>
    </div>

    <!-- iOS Widget Section -->
    <div style="margin-top:16px;padding-top:12px;border-top:1px solid var(--border)">
      <div style="font-size:12px;font-weight:650;color:#ff9800;margin-bottom:8px;text-transform:uppercase;letter-spacing:0.5px">📱 {web_widget_title}</div>
      <div style="font-size:11px;color:var(--muted);margin-bottom:8px">
        {web_widget_step1}<br>
        {web_widget_step2}<br>
        {web_widget_step3}<br>
        {web_widget_step4}<br>
        {web_widget_step5} <code id="widget-addr" style="background:var(--card-bg,#f0f0f0);padding:2px 6px;border-radius:3px;font-size:11px;user-select:all"></code>
      </div>
      <div style="display:flex;gap:8px">
        <button class="btn btn-accent" onclick="copyWidgetScript()" style="font-size:11px;padding:6px 12px">📋 {web_widget_btn_copy}</button>
        <button class="btn" onclick="window.open('/widget.js','_blank')" style="font-size:11px;padding:6px 12px;background:var(--card-bg);color:var(--fg);border:1px solid var(--border)">⬇ {web_widget_btn_download}</button>
      </div>
      <div id="widget-copy-msg" style="font-size:10px;color:#4caf50;margin-top:4px;display:none">{web_widget_copied}</div>
    </div>

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
function t(k, fbOrVars, maybeVars) {{
  let fb = (typeof fbOrVars === 'string') ? fbOrVars : undefined;
  let vars = (typeof fbOrVars === 'object') ? fbOrVars : maybeVars;
  let s = (I18N && I18N[k]) ? I18N[k] : (fb || k);
  if (vars) Object.keys(vars).forEach(function(kk){{ s = s.split('{{'+kk+'}}').join(String(vars[kk])); }});
  return s;
}}

/* ── State ── */
let frozen = false;
let liveTimer = null;
let currentPane = 'live';
// Set to true while a periodic per-tab auto-refresh is in flight so the
// loadXxx functions skip the "Loading…" spinner and avoid the visible flash.
let _quietRefresh = false;
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
function toggleNavDrawer() {{
  document.body.classList.toggle('nav-open');
}}
function switchPaneFromDrawer(name, el) {{
  // Mirror the selection to a bottom-nav button so switchPane highlights it
  document.body.classList.remove('nav-open');
  const btn = document.querySelector('.nav-btn[onclick*="\\'' + name + '\\'"]');
  switchPane(name, btn || null);
}}
function switchPane(name, btn) {{
  document.querySelectorAll('.pane').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.drawer-item').forEach(b => b.classList.remove('active'));
  const pane = document.getElementById('pane-' + name);
  if (pane) pane.classList.add('active');
  if (btn) btn.classList.add('active');
  // Highlight matching drawer item
  const drawerItem = document.querySelector('.drawer-item[onclick*="\\'' + name + '\\'"]');
  if (drawerItem) drawerItem.classList.add('active');
  currentPane = name;
  localStorage.setItem('sea_pane', name);
  // Scroll to top when switching tabs so content starts at the top
  const pc = document.getElementById('panes');
  if (pc) pc.scrollTop = 0;
  onPaneActivated(name);
}}

function onPaneActivated(name) {{
  // Stop polling when leaving export tab
  if (name !== 'export' && typeof _expStopJobsPolling === 'function') _expStopJobsPolling();
  if (name !== 'co2' && typeof _stopCo2LiveRates === 'function') _stopCo2LiveRates();
  // Always (re)arm the per-tab live refresh based on the new pane
  startTabLiveRefresh(name);
  if (name === 'live') {{
    startLive();
  }} else {{
    stopLive();
    if (name === 'plots') {{
      const fr = document.getElementById('plots-frame');
      if (fr && (!fr.src || fr.src === 'about:blank' || fr.src.endsWith('about:blank'))) {{
        fr.src = '/plots';
      }} else if (fr) {{
        // Iframe is already loaded → ask it to re-fetch fresh data
        // (e.g. after initial sync or when user returns to the tab).
        try {{ if (fr.contentWindow && fr.contentWindow.__scheduleApplyPlots) fr.contentWindow.__scheduleApplyPlots(50); }} catch(e) {{}}
      }}
    }}
    else if (name === 'costs') loadCosts();
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
    else if (name === 'smart_sched') loadSmartSched();
    else if (name === 'ev_log') loadEvLog();
    else if (name === 'tariff') loadTariff();
    else if (name === 'battery') loadBattery();
    else if (name === 'advisor') loadAdvisor();
    else if (name === 'goals') loadGoals();
    else if (name === 'tenants') loadTenants();
    else if (name === 'nilm') loadNilm();
    else if (name === 'control') loadControl();
    else if (name === 'sync') {{ initSync(); }}
    else {{ stopSyncPolling(); }}
  }}
  if (name !== 'sync') stopSyncPolling();
}}

/* ──────────────────────────────────────────────
   DEVICE CONTROL TAB
────────────────────────────────────────────── */
let _ctrlRefreshTimer = null;

function _ctrlDeviceCard(d, cfgMap) {{
  const cd = cfgMap[d.key] || {{}};
  const model = cd.model || '';
  const ml = model.toLowerCase();
  const cat = _guessCategory(ml, cd);
  const isOn = d.switch_on === true;
  const powerW = (d.power_w || 0).toFixed(1);
  let h = '';
  h += '<div class="card" id="ctrl-' + d.key + '" style="padding:14px">';
  h += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">';
  h += '<div><div style="font-weight:600;font-size:.95rem">' + esc(d.name) + '</div>';
  h += '<div style="font-size:.72rem;color:var(--muted)">' + esc(model || d.kind) + ' · ' + esc(cat) + '</div></div>';
  h += '<div style="font-size:.85rem;font-weight:600;color:var(--accent)">' + powerW + ' W</div>';
  h += '</div>';
  h += '<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">';
  h += '<span style="font-size:.82rem;color:var(--muted)">' + t('control.power', 'Power') + '</span>';
  h += '<label class="ctrl-toggle" style="flex:0 0 auto">';
  h += '<input type="checkbox" ' + (isOn ? 'checked' : '') + ' onchange="ctrlToggle(\\'' + d.key + '\\',this.checked)">';
  h += '<span class="ctrl-slider"></span></label>';
  h += '<span class="ctrl-state" id="ctrl-state-' + d.key + '" style="font-size:.82rem;font-weight:600;color:' + (isOn ? 'var(--ok)' : 'var(--muted)') + '">' + (isOn ? 'ON' : 'OFF') + '</span>';
  h += '</div>';
  if (cat === 'Dimmer' || cat === 'RGBW') {{
    h += '<div style="margin-bottom:10px">';
    h += '<div style="display:flex;justify-content:space-between;font-size:.82rem;color:var(--muted);margin-bottom:4px">';
    h += '<span>' + t('control.brightness', 'Brightness') + '</span>';
    h += '<span id="ctrl-bri-val-' + d.key + '">—</span></div>';
    h += '<input type="range" min="0" max="100" value="50" class="ctrl-range" id="ctrl-bri-' + d.key + '" ';
    h += 'onchange="ctrlSetBrightness(\\'' + d.key + '\\',this.value)" oninput="document.getElementById(\\'ctrl-bri-val-' + d.key + '\\').textContent=this.value+\\'%\\'">';
    h += '</div>';
  }}
  if (cat === 'RGBW') {{
    h += '<div style="margin-bottom:10px">';
    h += '<div style="font-size:.82rem;color:var(--muted);margin-bottom:4px">' + t('control.color', 'Color') + '</div>';
    h += '<div style="display:flex;gap:8px;align-items:center">';
    h += '<input type="color" value="#ff8800" id="ctrl-rgb-' + d.key + '" onchange="ctrlSetRgb(\\'' + d.key + '\\',this.value)" style="width:50px;height:36px;border:1px solid var(--border);border-radius:6px;cursor:pointer;background:var(--input-bg)">';
    h += '<input type="range" min="2700" max="6500" value="4000" class="ctrl-range" id="ctrl-temp-' + d.key + '" style="flex:1" ';
    h += 'onchange="ctrlSetTemp(\\'' + d.key + '\\',this.value)" oninput="document.getElementById(\\'ctrl-temp-lbl-' + d.key + '\\').textContent=this.value+\\'K\\'">';
    h += '<span id="ctrl-temp-lbl-' + d.key + '" style="font-size:.78rem;color:var(--muted);min-width:45px">4000K</span>';
    h += '</div></div>';
  }}
  if (cat === 'Cover') {{
    h += '<div style="margin-bottom:10px">';
    h += '<div style="display:flex;justify-content:space-between;font-size:.82rem;color:var(--muted);margin-bottom:4px">';
    h += '<span>' + t('control.position', 'Position') + '</span>';
    h += '<span id="ctrl-pos-val-' + d.key + '">—</span></div>';
    h += '<input type="range" min="0" max="100" value="50" class="ctrl-range" id="ctrl-pos-' + d.key + '" ';
    h += 'onchange="ctrlSetCoverPos(\\'' + d.key + '\\',this.value)" oninput="document.getElementById(\\'ctrl-pos-val-' + d.key + '\\').textContent=this.value+\\'%\\'">';
    h += '<div style="display:flex;gap:6px;margin-top:6px">';
    h += '<button class="btn" style="flex:1" onclick="ctrlCover(\\'' + d.key + '\\',\\'open\\')">▲ ' + t('control.open', 'Open') + '</button>';
    h += '<button class="btn" style="flex:1" onclick="ctrlCover(\\'' + d.key + '\\',\\'stop\\')">⏹ ' + t('control.stop', 'Stop') + '</button>';
    h += '<button class="btn" style="flex:1" onclick="ctrlCover(\\'' + d.key + '\\',\\'close\\')">▼ ' + t('control.close', 'Close') + '</button>';
    h += '</div></div>';
  }}
  h += '<div id="ctrl-msg-' + d.key + '" style="font-size:.72rem;color:var(--muted);min-height:16px"></div>';
  h += '</div>';
  return h;
}}

async function loadControl() {{
  const el = document.getElementById('control-content');
  if (!el) return;
  el.innerHTML = '<p class="loading-msg">' + t('control.loading', 'Loading devices…') + '</p>';

  try {{
    const [stateResp, cfgResp, settingsResp] = await Promise.all([
      fetch('/api/state'), fetch('/api/config'), fetch('/api/settings')
    ]);
    const data = await stateResp.json();
    const devs = data.devices || [];
    const cfgData = await cfgResp.json();
    const settings = await settingsResp.json();
    const cfgDevs = (cfgData.devices_meta || settings.devices || []);
    const cfgMap = {{}};
    cfgDevs.forEach(d => {{ cfgMap[d.key] = d; }});

    const controllable = devs.filter(d => {{
      const cd = cfgMap[d.key] || {{}};
      return d.kind === 'switch' || cd.kind === 'switch';
    }});

    if (!controllable.length) {{
      el.innerHTML = '<div class="card"><p style="color:var(--muted);text-align:center;padding:30px 0">' +
        t('control.no_devices', 'No controllable devices found. Add switches, dimmers, plugs or covers in Settings.') + '</p></div>';
      return;
    }}

    // Room grouping from settings (full config)
    const dcCfg = settings.device_control || {{}};
    const rooms = dcCfg.rooms || [];
    const assignedKeys = new Set();
    rooms.forEach(rm => {{ (rm.device_keys || []).forEach(k => assignedKeys.add(k)); }});
    const unassigned = controllable.filter(d => !assignedKeys.has(d.key));
    const devByKey = {{}};
    controllable.forEach(d => {{ devByKey[d.key] = d; }});

    let html = '';

    // Render each room
    rooms.forEach(rm => {{
      const roomDevs = (rm.device_keys || []).map(k => devByKey[k]).filter(Boolean);
      if (!roomDevs.length) return;
      const icon = rm.icon || '🏠';
      const name = rm.name || rm.room_id || t('control.room_unnamed', 'Unnamed room');
      html += '<div style="margin-bottom:20px">';
      html += '<h3 style="font-size:.95rem;margin:0 0 10px;color:var(--accent);display:flex;align-items:center;gap:6px">';
      html += '<span style="font-size:1.2rem">' + esc(icon) + '</span> ' + esc(name);
      html += '<span style="font-size:.75rem;color:var(--muted);font-weight:400">(' + roomDevs.length + ')</span></h3>';
      html += '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:12px">';
      roomDevs.forEach(d => {{ html += _ctrlDeviceCard(d, cfgMap); }});
      html += '</div></div>';
    }});

    // Unassigned devices
    if (unassigned.length) {{
      if (rooms.length) {{
        html += '<div style="margin-bottom:20px">';
        html += '<h3 style="font-size:.95rem;margin:0 0 10px;color:var(--muted);display:flex;align-items:center;gap:6px">';
        html += '<span style="font-size:1.2rem">📦</span> ' + t('control.unassigned', 'Unassigned');
        html += '<span style="font-size:.75rem;font-weight:400">(' + unassigned.length + ')</span></h3>';
      }}
      html += '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:12px">';
      unassigned.forEach(d => {{ html += _ctrlDeviceCard(d, cfgMap); }});
      html += '</div>';
      if (rooms.length) html += '</div>';
    }}

    el.innerHTML = html;

    // Fetch initial status for dimmers/covers
    controllable.forEach(d => {{
      const cd = cfgMap[d.key] || {{}};
      const cat = _guessCategory((cd.model||'').toLowerCase(), cd);
      if (cat === 'Dimmer' || cat === 'RGBW') _ctrlFetchLight(d.key);
      if (cat === 'Cover') _ctrlFetchCover(d.key);
    }});

  }} catch(e) {{
    el.innerHTML = '<div class="card"><p style="color:var(--danger)">' + t('control.error', 'Failed to load: ') + e.message + '</p></div>';
  }}
}}

function _guessCategory(ml, cd) {{
  // Detect device category from model string
  if (ml.includes('dimm') || ml.includes('shdm') || ml.includes('sndm') || ml.includes('spdm') || ml.includes('s3dm') || ml.includes('s4dm')) return 'Dimmer';
  if (ml.includes('rgbw') || ml.includes('bulb') || ml.includes('duo') || ml.includes('color') || ml.includes('spot') || ml.includes('shbl') || ml.includes('s3bl')) return 'RGBW';
  if (ml.includes('cover') || ml.includes('shutter') || ml.includes('roller') || ml.includes('spsh') || ml.includes('s3sh')) return 'Cover';
  if (ml.includes('plug') || ml.includes('shplg') || ml.includes('snpl') || ml.includes('s3pl') || ml.includes('s4pl') || ml.includes('strip')) return 'Plug';
  return 'Switch';
}}

async function ctrlToggle(key, on) {{
  const msg = document.getElementById('ctrl-msg-' + key);
  if (msg) msg.textContent = '…';
  try {{
    const r = await fetch('/api/run', {{method:'POST',headers:{{'Content-Type':'application/json'}},
      body:JSON.stringify({{action:'set_switch',params:{{device_key:key,on:on}}}}) }});
    const j = await r.json();
    const st = document.getElementById('ctrl-state-' + key);
    if (st) {{ st.textContent = j.on ? 'ON' : 'OFF'; st.style.color = j.on ? 'var(--ok)' : 'var(--muted)'; }}
    if (msg) msg.textContent = j.ok ? '✓' : (j.error || '');
  }} catch(e) {{ if (msg) msg.textContent = e.message; }}
}}

async function ctrlSetBrightness(key, val) {{
  const msg = document.getElementById('ctrl-msg-' + key);
  if (msg) msg.textContent = '…';
  try {{
    const r = await fetch('/api/run', {{method:'POST',headers:{{'Content-Type':'application/json'}},
      body:JSON.stringify({{action:'set_light',params:{{device_key:key,on:true,brightness:parseInt(val)}}}}) }});
    const j = await r.json();
    if (msg) msg.textContent = j.ok ? '✓ ' + val + '%' : (j.error || '');
  }} catch(e) {{ if (msg) msg.textContent = e.message; }}
}}

async function ctrlSetRgb(key, hex) {{
  const r = parseInt(hex.substr(1,2),16), g = parseInt(hex.substr(3,2),16), b = parseInt(hex.substr(5,2),16);
  const msg = document.getElementById('ctrl-msg-' + key);
  if (msg) msg.textContent = '…';
  try {{
    const resp = await fetch('/api/run', {{method:'POST',headers:{{'Content-Type':'application/json'}},
      body:JSON.stringify({{action:'set_light',params:{{device_key:key,on:true,rgb:[r,g,b]}}}}) }});
    const j = await resp.json();
    if (msg) msg.textContent = j.ok ? '✓' : (j.error || '');
  }} catch(e) {{ if (msg) msg.textContent = e.message; }}
}}

async function ctrlSetTemp(key, val) {{
  const msg = document.getElementById('ctrl-msg-' + key);
  if (msg) msg.textContent = '…';
  try {{
    const r = await fetch('/api/run', {{method:'POST',headers:{{'Content-Type':'application/json'}},
      body:JSON.stringify({{action:'set_light',params:{{device_key:key,on:true,temp:parseInt(val)}}}}) }});
    const j = await r.json();
    if (msg) msg.textContent = j.ok ? '✓ ' + val + 'K' : (j.error || '');
  }} catch(e) {{ if (msg) msg.textContent = e.message; }}
}}

async function ctrlCover(key, cmd) {{
  const msg = document.getElementById('ctrl-msg-' + key);
  if (msg) msg.textContent = '…';
  try {{
    const r = await fetch('/api/run', {{method:'POST',headers:{{'Content-Type':'application/json'}},
      body:JSON.stringify({{action:'cover_' + cmd,params:{{device_key:key}}}}) }});
    const j = await r.json();
    if (msg) msg.textContent = j.ok ? '✓' : (j.error || '');
    if (j.ok && j.status) _ctrlApplyCover(key, j.status);
  }} catch(e) {{ if (msg) msg.textContent = e.message; }}
}}

async function ctrlSetCoverPos(key, val) {{
  const msg = document.getElementById('ctrl-msg-' + key);
  if (msg) msg.textContent = '…';
  try {{
    const r = await fetch('/api/run', {{method:'POST',headers:{{'Content-Type':'application/json'}},
      body:JSON.stringify({{action:'cover_position',params:{{device_key:key,position:parseInt(val)}}}}) }});
    const j = await r.json();
    if (msg) msg.textContent = j.ok ? '✓ ' + val + '%' : (j.error || '');
    if (j.ok && j.status) _ctrlApplyCover(key, j.status);
  }} catch(e) {{ if (msg) msg.textContent = e.message; }}
}}

async function _ctrlFetchLight(key) {{
  try {{
    const r = await fetch('/api/run', {{method:'POST',headers:{{'Content-Type':'application/json'}},
      body:JSON.stringify({{action:'get_light',params:{{device_key:key}}}}) }});
    const j = await r.json();
    if (j.ok && j.status) {{
      const st = j.status;
      const bri = st.brightness != null ? st.brightness : (st.gain != null ? st.gain : null);
      if (bri != null) {{
        const slider = document.getElementById('ctrl-bri-' + key);
        const lbl = document.getElementById('ctrl-bri-val-' + key);
        if (slider) slider.value = bri;
        if (lbl) lbl.textContent = bri + '%';
      }}
    }}
  }} catch(e) {{}}
}}

async function _ctrlFetchCover(key) {{
  try {{
    const r = await fetch('/api/run', {{method:'POST',headers:{{'Content-Type':'application/json'}},
      body:JSON.stringify({{action:'get_cover',params:{{device_key:key}}}}) }});
    const j = await r.json();
    if (j.ok && j.status) _ctrlApplyCover(key, j.status);
  }} catch(e) {{}}
}}

function _ctrlApplyCover(key, st) {{
  const pos = st.current_pos != null ? st.current_pos : (st.roller_pos != null ? st.roller_pos : null);
  if (pos != null) {{
    const slider = document.getElementById('ctrl-pos-' + key);
    const lbl = document.getElementById('ctrl-pos-val-' + key);
    if (slider) slider.value = pos;
    if (lbl) lbl.textContent = pos + '%';
  }}
}}

/* ──────────────────────────────────────────────
   NILM STATISTICS TAB
────────────────────────────────────────────── */
let _nilmData = null;
async function loadNilm() {{
  const el = document.getElementById('nilm-content');
  if (!el) return;
  if (!_quietRefresh) el.innerHTML = '<p class="loading-msg">' + t('web.loading', 'Loading…') + '</p>';
  try {{
    const r = await fetch('/api/nilm_detail');
    if (!r.ok) throw new Error(r.status);
    _nilmData = await r.json();
    renderNilm(_nilmData, el);
  }} catch(e) {{
    el.innerHTML = '<p class="error-msg">Error: ' + e.message + '</p>';
  }}
}}

function renderNilm(data, el) {{
  if (!data.ok) {{
    el.innerHTML = '<p class="error-msg">' + esc(data.error || 'Unknown error') + '</p>';
    return;
  }}
  let html = '';

  /* ── Status Badge Row ── */
  const learning = data.transition_count < 10;
  html += '<div style="display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:14px">';
  html += '<span class="badge ' + (data.cluster_count > 0 ? 'badge-green' : learning ? 'badge-yellow' : 'badge-red') + '">' +
    (data.cluster_count > 0 ? t('web.nilm.active','NILM Active') : learning ? t('web.nilm.learning_status','Learning\u2026') : t('web.nilm.waiting_status','Waiting for data')) + '</span>';
  html += '<span class="badge badge-yellow">' + data.cluster_count + ' ' + t('web.nilm.patterns_badge','Patterns') + '</span>';
  html += '<span class="badge badge-yellow">' + data.transition_count + ' ' + t('web.nilm.transitions_badge','Transitions') + '</span>';
  html += '<span class="badge badge-yellow">' + data.device_count + ' ' + t('web.nilm.devices_badge','Devices') + '</span>';
  html += '</div>';

  /* ── Overview Metric Cards ── */
  html += '<div class="nilm-metrics">';
  html += _nilmMetricCard('🧠', t('web.nilm.detected_patterns','Detected Patterns'), data.cluster_count, t('web.nilm.cluster_from_ml','Clusters from ML k-means'));
  html += _nilmMetricCard('⚡', t('web.nilm.transitions_badge','Transitions'), data.transition_count, t('web.nilm.power_jumps','Power transitions detected'));
  html += _nilmMetricCard('📡', t('web.nilm.monitored_devices','Devices Monitored'), data.device_count, t('web.nilm.three_phase_em','3-phase EM devices'));
  const cats = Object.keys(data.categories || {{}}).length;
  html += _nilmMetricCard('📊', t('web.nilm.categories_count','Categories'), cats, t('web.nilm.diff_categories','Different appliance categories'));
  html += '</div>';

  /* ── Top 10 Patterns with mini power-profile canvas ── */
  const clusters = (data.clusters || []).slice(0, 10);
  if (clusters.length > 0) {{
    html += '<div class="card" style="margin-bottom:12px">';
    html += '<div style="font-size:14px;font-weight:700;margin-bottom:10px">' + t('web.nilm.top10','Top 10 Detected Patterns') + '</div>';
    html += '<div class="nilm-pattern-grid">';
    clusters.forEach(function(c, idx) {{
      const devName = (data.device_names || {{}})[c.device_key] || c.device_key || '?';
      const pct = data.transition_count > 0 ? Math.round(c.count / data.transition_count * 100) : 0;
      const colHue = _nilmPowerHue(c.centroid_w);
      html += '<div class="nilm-pattern-card" style="border-left:4px solid hsl(' + colHue + ',70%,50%)">';
      html += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">';
      html += '<span style="font-size:20px">' + (c.icon || '🔌') + '</span>';
      html += '<span class="badge" style="background:hsl(' + colHue + ',70%,90%);color:hsl(' + colHue + ',70%,30%);font-size:11px">#' + (idx+1) + '</span>';
      html += '</div>';
      html += '<div style="font-weight:700;font-size:14px;margin-bottom:2px">' + esc(c.label ? t('appliance.' + c.label + '.name', c.label) : c.matched_appliance ? t('appliance.' + c.matched_appliance + '.name', c.matched_appliance) : t('web.nilm.unknown','Unknown')) + '</div>';
      html += '<div style="font-size:12px;color:var(--muted);margin-bottom:6px">' + esc(devName) + '</div>';
      html += '<div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:8px">';
      html += '<div class="nilm-stat"><span class="nilm-stat-val">' + Math.round(c.centroid_w) + '</span><span class="nilm-stat-lbl">' + t('web.nilm.watt','Watt') + '</span></div>';
      html += '<div class="nilm-stat"><span class="nilm-stat-val">' + c.count + '</span><span class="nilm-stat-lbl">' + t('web.nilm.events','Events') + '</span></div>';
      html += '<div class="nilm-stat"><span class="nilm-stat-val">' + pct + '%</span><span class="nilm-stat-lbl">' + t('web.nilm.share','Share') + '</span></div>';
      if (c.std_w) html += '<div class="nilm-stat"><span class="nilm-stat-val">\u00b1' + Math.round(c.std_w) + '</span><span class="nilm-stat-lbl">' + t('web.nilm.std_w','Std W') + '</span></div>';
      if (c.typical_hour !== undefined) html += '<div class="nilm-stat"><span class="nilm-stat-val">' + String(c.typical_hour).padStart(2,'0') + ':00</span><span class="nilm-stat-lbl">' + t('web.nilm.peak','Peak') + '</span></div>';
      html += '</div>';
      // Mini power bar (visual representation of centroid_w relative to max)
      const maxW = Math.max.apply(null, clusters.map(function(x){{ return x.centroid_w; }})) || 1;
      const barPct = Math.round(c.centroid_w / maxW * 100);
      html += '<div style="background:var(--bg);border-radius:4px;height:8px;overflow:hidden">';
      html += '<div style="width:' + barPct + '%;height:100%;background:hsl(' + colHue + ',70%,55%);border-radius:4px;transition:width .3s"></div>';
      html += '</div>';
      // Mini canvas for power profile (will be drawn after DOM insertion)
      html += '<canvas class="nilm-spark" data-idx="' + idx + '" style="width:100%;height:50px;margin-top:8px"></canvas>';
      html += '</div>';
    }});
    html += '</div></div>';
  }} else {{
    html += '<div class="card" style="margin-bottom:12px;text-align:center;padding:30px">';
    html += '<div style="font-size:40px;margin-bottom:10px">🧠</div>';
    html += '<div style="font-size:16px;font-weight:600;margin-bottom:6px">' + t('web.nilm.still_learning','NILM still learning\u2026') + '</div>';
    html += '<div style="color:var(--muted);font-size:13px">' + t('web.nilm.min_transitions','At least 10 power transitions needed. Current: {{count}}', {{count: data.transition_count}}) + '</div>';
    html += '<div style="margin-top:12px;background:var(--bg);border-radius:6px;height:8px;overflow:hidden;max-width:300px;margin-left:auto;margin-right:auto">';
    const prog = Math.min(100, Math.round(data.transition_count / 10 * 100));
    html += '<div style="width:' + prog + '%;height:100%;background:var(--accent);border-radius:6px;transition:width .5s"></div>';
    html += '</div></div>';
  }}

  /* ── Hourly Activity Heatmap ── */
  const hourly = data.hourly_distribution || [];
  if (hourly.some(function(v){{ return v > 0; }})) {{
    html += '<div class="card" style="margin-bottom:12px">';
    html += '<div style="font-size:14px;font-weight:700;margin-bottom:10px">' + t('web.nilm.hourly_activity','Activity by Time of Day') + '</div>';
    html += '<canvas id="nilm-hourly-canvas" style="width:100%;height:120px"></canvas>';
    html += '</div>';
  }}

  /* ── Category Breakdown Donut ── */
  const catEntries = Object.entries(data.categories || {{}}).sort(function(a,b){{ return b[1]-a[1]; }});
  if (catEntries.length > 0) {{
    html += '<div class="nilm-two-col">';
    html += '<div class="card">';
    html += '<div style="font-size:14px;font-weight:700;margin-bottom:10px">' + t('web.nilm.categories','Categories') + '</div>';
    html += '<canvas id="nilm-cat-canvas" style="width:100%;height:200px"></canvas>';
    html += '<div id="nilm-cat-legend" style="margin-top:8px"></div>';
    html += '</div>';

    /* ── Per-Device Breakdown ── */
    html += '<div class="card">';
    html += '<div style="font-size:14px;font-weight:700;margin-bottom:10px">' + t('web.nilm.device_overview','Device Overview') + '</div>';
    const devEntries = Object.entries(data.device_stats || {{}});
    if (devEntries.length === 0) {{
      html += '<p style="color:var(--muted);font-size:13px">' + t('web.nilm.no_device_data','No device data available.') + '</p>';
    }} else {{
      devEntries.sort(function(a,b){{ return b[1].total_events - a[1].total_events; }});
      devEntries.forEach(function(de) {{
        const dk = de[0], ds = de[1];
        const dn = (data.device_names || {{}})[dk] || dk;
        html += '<div style="margin-bottom:10px;padding:8px;background:var(--bg);border-radius:8px">';
        html += '<div style="font-weight:600;font-size:13px;margin-bottom:4px">' + esc(dn) + '</div>';
        html += '<div style="display:flex;gap:12px;font-size:12px;color:var(--muted);flex-wrap:wrap">';
        html += '<span>' + ds.cluster_count + ' ' + t('web.nilm.pattern_count','Patterns') + '</span>';
        html += '<span>' + ds.total_events + ' ' + t('web.nilm.events','Events') + '</span>';
        html += '</div>';
        if (ds.top_appliances && ds.top_appliances.length > 0) {{
          html += '<div style="display:flex;gap:4px;flex-wrap:wrap;margin-top:4px">';
          ds.top_appliances.forEach(function(a) {{
            html += '<span class="appl-chip">' + (a.icon||'') + ' ' + esc(t('appliance.' + a.appliance + '.name', a.appliance)) + ' ' + Math.round(a.centroid_w) + 'W</span>';
          }});
          html += '</div>';
        }}
        html += '</div>';
      }});
    }}
    html += '</div></div>';
  }}

  /* ── Recent Transitions Timeline ── */
  const trans = (data.transitions || []).slice(0, 30);
  if (trans.length > 0) {{
    html += '<div class="card" style="margin-bottom:12px">';
    html += '<div style="font-size:14px;font-weight:700;margin-bottom:10px">' + t('web.nilm.recent_transitions','Recent Transitions') + '</div>';
    html += '<div class="nilm-timeline">';
    trans.forEach(function(tr) {{
      const dt = new Date(tr.ts * 1000);
      const timeStr = dt.toLocaleDateString() + ' ' + dt.toLocaleTimeString();
      const devName = (data.device_names || {{}})[tr.device_key] || tr.device_key || '?';
      const isOn = tr.delta_w > 0;
      const col = isOn ? '#22c55e' : '#ef4444';
      const arrow = isOn ? '▲' : '▼';
      html += '<div class="nilm-tl-item">';
      html += '<div class="nilm-tl-dot" style="background:' + col + '"></div>';
      html += '<div class="nilm-tl-body">';
      html += '<div style="display:flex;justify-content:space-between;align-items:center">';
      html += '<span style="font-weight:600;font-size:13px">' + arrow + ' ' + (isOn ? '+' : '') + Math.round(tr.delta_w) + ' W</span>';
      html += '<span style="font-size:11px;color:var(--muted)">' + timeStr + '</span>';
      html += '</div>';
      html += '<div style="font-size:12px;color:var(--muted)">' + esc(devName) + ' · ' + Math.round(tr.power_before) + ' W → ' + Math.round(tr.power_after) + ' W</div>';
      html += '</div></div>';
    }});
    html += '</div></div>';
  }}

  /* ── Appliance Signature Reference ── */
  const sigs = data.signatures || [];
  if (sigs.length > 0) {{
    html += '<div class="card" style="margin-bottom:12px">';
    html += '<details><summary style="font-size:14px;font-weight:700;cursor:pointer;margin-bottom:8px">' + t('web.nilm.signature_db','Appliance Database ({{count}} signatures)', {{count: sigs.length}}) + '</summary>';
    html += '<div class="nilm-sig-grid">';
    sigs.forEach(function(s) {{
      html += '<div class="nilm-sig-item">';
      html += '<span style="font-size:18px">' + s.icon + '</span>';
      html += '<div style="flex:1;min-width:0">';
      html += '<div style="font-weight:600;font-size:12px">' + esc(t('appliance.' + s.id + '.name', s.id)) + '</div>';
      html += '<div style="font-size:11px;color:var(--muted)">' + s.power_min + '–' + s.power_max + ' W · ' + esc(s.pattern_type) + ' · ' + s.typical_duration_min + ' min</div>';
      html += '</div></div>';
    }});
    html += '</div></details></div>';
  }}

  el.innerHTML = html;

  /* ── Draw canvases after DOM insertion ── */
  requestAnimationFrame(function() {{
    _drawNilmSparklines(clusters, data.transitions || []);
    _drawNilmHourlyChart(hourly);
    _drawNilmCategoryDonut(catEntries);
  }});
}}

function _nilmMetricCard(icon, title, value, sub) {{
  return '<div class="nilm-metric-card">' +
    '<div style="font-size:22px;margin-bottom:4px">' + icon + '</div>' +
    '<div style="font-size:24px;font-weight:800;line-height:1.1">' + value + '</div>' +
    '<div style="font-size:12px;font-weight:600;color:var(--fg)">' + esc(title) + '</div>' +
    '<div style="font-size:11px;color:var(--muted)">' + esc(sub) + '</div>' +
    '</div>';
}}

function _nilmPowerHue(watts) {{
  // Green (120) for low power → yellow (60) → red (0) for high
  const norm = Math.min(1, watts / 5000);
  return Math.round(120 * (1 - norm));
}}

function _drawNilmSparklines(clusters, transitions) {{
  clusters.forEach(function(c, idx) {{
    const canvas = document.querySelector('.nilm-spark[data-idx="' + idx + '"]');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;
    const rect = canvas.getBoundingClientRect();
    canvas.width = rect.width * dpr;
    canvas.height = rect.height * dpr;
    ctx.scale(dpr, dpr);
    const W = rect.width, H = rect.height;

    // Filter transitions matching this cluster's device + approximate power
    const cw = c.centroid_w;
    const stdW = c.std_w || cw * 0.3;
    const matching = transitions.filter(function(tr) {{
      return tr.device_key === c.device_key && Math.abs(Math.abs(tr.delta_w) - cw) <= stdW * 2;
    }}).slice(0, 50);

    if (matching.length < 2) {{
      // Draw a representative power step profile
      _drawNilmStepProfile(ctx, W, H, c);
      return;
    }}

    // Draw timeline of matching transitions
    const times = matching.map(function(tr) {{ return tr.ts; }});
    const minT = Math.min.apply(null, times);
    const maxT = Math.max.apply(null, times);
    const range = maxT - minT || 1;

    const fg = getComputedStyle(document.body).getPropertyValue('--muted') || '#999';
    const hue = _nilmPowerHue(cw);
    ctx.strokeStyle = 'hsl(' + hue + ',70%,55%)';
    ctx.lineWidth = 1.5;
    ctx.fillStyle = 'hsl(' + hue + ',70%,55%,0.15)';

    // Draw area
    ctx.beginPath();
    ctx.moveTo(0, H);
    matching.forEach(function(tr) {{
      const x = ((tr.ts - minT) / range) * W;
      const y = H - (Math.abs(tr.delta_w) / (cw * 2 || 1)) * H * 0.85;
      ctx.lineTo(x, Math.max(2, y));
    }});
    ctx.lineTo(W, H);
    ctx.closePath();
    ctx.fill();

    // Draw line
    ctx.beginPath();
    matching.forEach(function(tr, i) {{
      const x = ((tr.ts - minT) / range) * W;
      const y = H - (Math.abs(tr.delta_w) / (cw * 2 || 1)) * H * 0.85;
      if (i === 0) ctx.moveTo(x, Math.max(2, y));
      else ctx.lineTo(x, Math.max(2, y));
    }});
    ctx.stroke();

    // Draw dots
    ctx.fillStyle = 'hsl(' + hue + ',70%,50%)';
    matching.forEach(function(tr) {{
      const x = ((tr.ts - minT) / range) * W;
      const y = H - (Math.abs(tr.delta_w) / (cw * 2 || 1)) * H * 0.85;
      ctx.beginPath();
      ctx.arc(x, Math.max(2, y), 2.5, 0, Math.PI * 2);
      ctx.fill();
    }});
  }});
}}

function _drawNilmStepProfile(ctx, W, H, cluster) {{
  // Draw a stylized power step to represent this pattern
  const hue = _nilmPowerHue(cluster.centroid_w);
  const pad = 4;
  const midY = H * 0.8;
  const topY = H * 0.15;

  ctx.fillStyle = 'hsl(' + hue + ',70%,55%,0.1)';
  ctx.strokeStyle = 'hsl(' + hue + ',70%,55%)';
  ctx.lineWidth = 2;
  ctx.setLineDash([]);

  // Step up → hold → step down
  ctx.beginPath();
  ctx.moveTo(pad, midY);
  ctx.lineTo(W * 0.2, midY);
  ctx.lineTo(W * 0.2, topY);
  ctx.lineTo(W * 0.8, topY);
  ctx.lineTo(W * 0.8, midY);
  ctx.lineTo(W - pad, midY);
  ctx.stroke();

  // Fill area under step
  ctx.beginPath();
  ctx.moveTo(pad, midY);
  ctx.lineTo(W * 0.2, midY);
  ctx.lineTo(W * 0.2, topY);
  ctx.lineTo(W * 0.8, topY);
  ctx.lineTo(W * 0.8, midY);
  ctx.lineTo(W - pad, midY);
  ctx.lineTo(W - pad, H);
  ctx.lineTo(pad, H);
  ctx.closePath();
  ctx.fill();

  // Power label
  ctx.fillStyle = 'hsl(' + hue + ',70%,40%)';
  ctx.font = 'bold 10px sans-serif';
  ctx.textAlign = 'center';
  ctx.fillText(Math.round(cluster.centroid_w) + ' W', W / 2, topY - 3);
}}

function _drawNilmHourlyChart(hourly) {{
  const canvas = document.getElementById('nilm-hourly-canvas');
  if (!canvas || !hourly || hourly.length !== 24) return;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);
  const W = rect.width, H = rect.height;
  const pad = {{top: 10, right: 8, bottom: 22, left: 32}};
  const cW = W - pad.left - pad.right;
  const cH = H - pad.top - pad.bottom;
  const maxV = Math.max.apply(null, hourly.concat([1])) * 1.15;
  const barW = Math.max(2, (cW / 24) - 2);
  const fg = getComputedStyle(document.body).getPropertyValue('--muted') || '#999';
  const border = getComputedStyle(document.body).getPropertyValue('--border') || '#e0e0e0';

  // Grid lines
  ctx.strokeStyle = border;
  ctx.lineWidth = 0.5;
  for (let i = 0; i <= 4; i++) {{
    const y = pad.top + cH - (cH * i / 4);
    ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(pad.left + cW, y); ctx.stroke();
    ctx.fillStyle = fg; ctx.font = '9px sans-serif'; ctx.textAlign = 'right';
    ctx.fillText(Math.round(maxV * i / 4), pad.left - 4, y + 3);
  }}

  // Bars with gradient colors based on activity
  hourly.forEach(function(v, i) {{
    const x = pad.left + (i / 24) * cW + 1;
    const h = (v / maxV) * cH;
    const y = pad.top + cH - h;
    const hue = 200 - Math.round((v / maxV) * 160); // Blue → Red
    ctx.fillStyle = 'hsl(' + hue + ',70%,55%)';
    ctx.beginPath();
    ctx.roundRect(x, y, barW, h, [2, 2, 0, 0]);
    ctx.fill();

    // Hour labels (every 3 hours)
    if (i % 3 === 0) {{
      ctx.fillStyle = fg; ctx.font = '9px sans-serif'; ctx.textAlign = 'center';
      ctx.fillText(String(i).padStart(2, '0'), x + barW / 2, pad.top + cH + 14);
    }}
  }});
}}

function _drawNilmCategoryDonut(catEntries) {{
  const canvas = document.getElementById('nilm-cat-canvas');
  const legend = document.getElementById('nilm-cat-legend');
  if (!canvas || !catEntries || catEntries.length === 0) return;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);
  const W = rect.width, H = rect.height;
  const cx = W / 2, cy = H / 2;
  const R = Math.min(cx, cy) - 10;
  const r = R * 0.55; // inner radius for donut

  const total = catEntries.reduce(function(s, e) {{ return s + e[1]; }}, 0);
  const catColors = ['#3b82f6','#22c55e','#f59e0b','#ef4444','#8b5cf6','#ec4899','#14b8a6','#f97316','#6366f1','#06b6d4'];
  let startAngle = -Math.PI / 2;
  let legendHtml = '<div style="display:flex;flex-wrap:wrap;gap:6px">';

  catEntries.forEach(function(e, i) {{
    const name = e[0], count = e[1];
    const slice = (count / total) * Math.PI * 2;
    const col = catColors[i % catColors.length];

    ctx.fillStyle = col;
    ctx.beginPath();
    ctx.arc(cx, cy, R, startAngle, startAngle + slice);
    ctx.arc(cx, cy, r, startAngle + slice, startAngle, true);
    ctx.closePath();
    ctx.fill();

    startAngle += slice;
    const pct = Math.round(count / total * 100);
    legendHtml += '<span style="font-size:11px;display:flex;align-items:center;gap:3px">' +
      '<span style="width:10px;height:10px;border-radius:50%;background:' + col + ';display:inline-block"></span>' +
      esc(t('web.nilm.category.' + name, name)) + ' (' + pct + '%)' +
      '</span>';
  }});

  // Center text
  ctx.fillStyle = getComputedStyle(document.body).getPropertyValue('--fg') || '#111';
  ctx.font = 'bold 18px sans-serif';
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  ctx.fillText(total, cx, cy - 8);
  ctx.font = '11px sans-serif';
  ctx.fillStyle = getComputedStyle(document.body).getPropertyValue('--muted') || '#666';
  ctx.fillText('Events', cx, cy + 10);

  legendHtml += '</div>';
  if (legend) legend.innerHTML = legendHtml;
}}

/* ──────────────────────────────────────────────
   SYNC PANE (trigger + live log)
────────────────────────────────────────────── */
let _syncTimer = null;
let _syncLogSince = 0;
function initSync() {{
  refreshSyncStatus();
  _syncLogSince = 0;
  const el = document.getElementById('sync-log');
  if (el) el.textContent = '';
  pollSyncLogs();
  if (!_syncTimer) _syncTimer = setInterval(pollSyncLogs, 2000);
}}
function stopSyncPolling() {{
  if (_syncTimer) {{ clearInterval(_syncTimer); _syncTimer = null; }}
}}
function toggleLogHttp(v) {{
  fetch('/api/logs/config', {{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{include_http: !!v}})}}).catch(function() {{}});
}}
function pollSyncLogs() {{
  fetch('/api/logs?since=' + _syncLogSince + '&limit=200').then(function(r) {{ return r.json(); }}).then(function(d) {{
    // Sync checkbox with server state on first poll
    var httpCb = document.getElementById('log-include-http');
    if (httpCb && typeof d.include_http === 'boolean' && httpCb.checked !== d.include_http) httpCb.checked = d.include_http;
    const el = document.getElementById('sync-log');
    if (!el || !d.entries || !d.entries.length) return;
    const frag = d.entries.map(function(e) {{
      const ts = new Date(e.ts*1000).toLocaleTimeString();
      return ts + '  ' + e.level.padEnd(7) + ' ' + e.msg;
    }}).join('\\n') + '\\n';
    el.textContent += frag;
    _syncLogSince = d.entries[d.entries.length-1].ts;
    const cb = document.getElementById('log-autoscroll');
    if (!cb || cb.checked) el.scrollTop = el.scrollHeight;
    // Truncate buffer if very long
    if (el.textContent.length > 200000) el.textContent = el.textContent.slice(-150000);
  }}).catch(function() {{}});
}}
function syncAll(mode) {{
  const el = document.getElementById('sync-status-panel');
  if (el) el.textContent = t('web.sync.status.starting', {{mode: mode}});
  fetch('/api/sync', {{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{mode:mode}})}})
    .then(function(r) {{ return r.json(); }})
    .then(function(d) {{
      if (el) el.textContent = d.ok
        ? t('web.sync.status.running', {{mode: mode, job: (d.job_id||'?')}})
        : t('web.sync.status.error', {{err: (d.error||'?')}});
      // Refresh device status after 3s so the sync thread has time to write last_end_ts
      setTimeout(refreshSyncStatus, 3000);
    }}).catch(function(e) {{ if (el) el.textContent = t('web.sync.status.net_error', {{err: e}}); }});
}}
/* ──────────────────────────────────────────────
   TENANTS (Utility billing)
────────────────────────────────────────────── */
let _tenantsCache = null;
let _tenantsDevices = [];
function loadTenants() {{
  const el = document.getElementById('tenants-content');
  if (!el) return;
  if (!_quietRefresh) el.innerHTML = '<p class="loading-msg">Loading…</p>';
  Promise.all([
    fetch('/api/tenants').then(function(r) {{ return r.json(); }}),
    fetch('/api/config').then(function(r) {{ return r.json(); }})
  ]).then(function(results) {{
    const td = results[0] || {{}};
    const cfg = results[1] || {{}};
    _tenantsCache = td;
    _tenantsDevices = (cfg.devices_meta || cfg.devices || []).map(function(d) {{ return {{key:d.key, name:d.name||d.key}}; }});
    renderTenants();
  }}).catch(function(e) {{
    el.innerHTML = '<p style="color:var(--red)">Error: ' + (e && e.message || '?') + '</p>';
  }});
}}
function _tenantsDeviceOptions(selectedKeys) {{
  const sel = new Set(selectedKeys || []);
  return _tenantsDevices.map(function(d) {{
    return '<label style="display:inline-flex;gap:4px;align-items:center;margin:2px 6px 2px 0;font-size:11px">' +
      '<input type="checkbox" class="t-dev" value="' + esc(d.key) + '"' + (sel.has(d.key)?' checked':'') + '> ' + esc(d.name) + '</label>';
  }}).join('');
}}
function renderTenants() {{
  const el = document.getElementById('tenants-content');
  if (!el || !_tenantsCache) return;
  const td = _tenantsCache;
  let h = '';
  // Config card
  h += '<div class="card" style="margin-bottom:8px">';
  h += '<div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-bottom:8px">';
  h += '<label style="display:flex;gap:4px;align-items:center;font-size:12px"><input type="checkbox" id="t-enabled"' + (td.enabled?' checked':'') + '> Tenant billing active</label>';
  h += '<label style="font-size:12px">Billing period (months): <input type="number" id="t-period" value="' + (td.billing_period_months||12) + '" style="width:60px"></label>';
  h += '<span style="flex:1"></span>';
  h += '<button class="btn" onclick="addTenantRow()">+ Tenant</button>';
  h += '<button class="btn btn-accent" onclick="saveTenants()">💾 Save</button>';
  h += '</div>';
  h += '<div style="font-size:12px;font-weight:650;color:var(--muted);text-transform:uppercase;margin-bottom:4px">Common areas (shared meter)</div>';
  h += '<div id="t-common" style="display:flex;flex-wrap:wrap">' + _tenantsDeviceOptions(td.common_device_keys) + '</div>';
  h += '</div>';
  // Tenants list
  h += '<div id="t-list">';
  (td.tenants || []).forEach(function(t, i) {{ h += _tenantRowHtml(t, i); }});
  if (!td.tenants || !td.tenants.length) {{
    h += '<div class="card" style="color:var(--muted);font-size:12px">No tenants yet. Click "+ Tenant" to add one.</div>';
  }}
  h += '</div>';
  // Billing computation
  h += '<div class="card" style="margin-top:8px">';
  h += '<div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:8px">';
  h += '<label style="font-size:12px">From: <input type="date" id="t-start"></label>';
  h += '<label style="font-size:12px">To: <input type="date" id="t-end"></label>';
  h += '<label style="font-size:12px">Tariff: <select id="t-tariff"><option value="">Auto (Settings)</option><option value="fixed">Fixed</option><option value="dynamic">Dynamic</option></select></label>';
  h += '<button class="btn btn-accent" onclick="computeBills()">📊 Compute bills</button>';
  h += '</div>';
  h += '<div id="t-bills"></div>';
  h += '</div>';
  el.innerHTML = h;
}}
function _tenantRowHtml(t, i) {{
  const tid = esc(t.tenant_id || '');
  return '<div class="card" data-idx="' + i + '" style="margin-bottom:6px">' +
    '<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:6px">' +
    '<input class="t-name" placeholder="Name" value="' + esc(t.name||'') + '" style="flex:2;min-width:120px">' +
    '<input class="t-unit" placeholder="Unit" value="' + esc(t.unit||'') + '" style="flex:1;min-width:80px">' +
    '<input class="t-id" placeholder="ID" value="' + tid + '" style="width:80px">' +
    '<input class="t-persons" type="number" min="1" placeholder="Pers." value="' + (t.persons||1) + '" style="width:60px">' +
    '<input class="t-in" type="date" value="' + esc(t.move_in||'') + '" style="width:140px" title="Move-in">' +
    '<input class="t-out" type="date" value="' + esc(t.move_out||'') + '" style="width:140px" title="Move-out">' +
    '<button class="btn" onclick="removeTenantRow(' + i + ')" title="Delete">🗑</button>' +
    '</div>' +
    '<div style="font-size:11px;color:var(--muted);margin-bottom:2px">Assigned devices:</div>' +
    '<div class="t-devs" style="display:flex;flex-wrap:wrap">' + _tenantsDeviceOptions(t.device_keys) + '</div>' +
    '</div>';
}}
function addTenantRow() {{
  if (!_tenantsCache) return;
  _tenantsCache.tenants = _tenantsCache.tenants || [];
  _tenantsCache.tenants.push({{tenant_id:'t' + (_tenantsCache.tenants.length+1), name:'', persons:1, device_keys:[]}});
  renderTenants();
}}
function removeTenantRow(idx) {{
  if (!_tenantsCache || !_tenantsCache.tenants) return;
  _tenantsCache.tenants.splice(idx, 1);
  renderTenants();
}}
function _collectTenantsFromDom() {{
  const list = document.querySelectorAll('#t-list > [data-idx]');
  const tenants = [];
  list.forEach(function(row) {{
    const dk = Array.from(row.querySelectorAll('.t-devs .t-dev:checked')).map(function(x) {{ return x.value; }});
    tenants.push({{
      tenant_id: (row.querySelector('.t-id')||{{}}).value || '',
      name: (row.querySelector('.t-name')||{{}}).value || '',
      unit: (row.querySelector('.t-unit')||{{}}).value || '',
      persons: parseInt((row.querySelector('.t-persons')||{{}}).value || '1', 10),
      move_in: (row.querySelector('.t-in')||{{}}).value || '',
      move_out: (row.querySelector('.t-out')||{{}}).value || '',
      device_keys: dk,
    }});
  }});
  const common = Array.from(document.querySelectorAll('#t-common .t-dev:checked')).map(function(x) {{ return x.value; }});
  return {{
    enabled: !!(document.getElementById('t-enabled')||{{}}).checked,
    billing_period_months: parseInt((document.getElementById('t-period')||{{}}).value || '12', 10),
    tenants: tenants,
    common_device_keys: common,
  }};
}}
function saveTenants() {{
  const body = _collectTenantsFromDom();
  fetch('/api/tenants', {{method:'PUT',headers:{{'Content-Type':'application/json'}},body:JSON.stringify(body)}})
    .then(function(r) {{ return r.json(); }})
    .then(function(d) {{
      if (d.ok) {{ _tenantsCache = Object.assign(_tenantsCache||{{}}, body); renderTenants(); alert('Saved'); }}
      else alert('Error: ' + (d.error||'?'));
    }}).catch(function(e) {{ alert('Error: ' + e); }});
}}
function computeBills() {{
  const s = (document.getElementById('t-start')||{{}}).value || '';
  const e = (document.getElementById('t-end')||{{}}).value || '';
  const tm = (document.getElementById('t-tariff')||{{}}).value || '';
  const q = new URLSearchParams();
  if (s) q.set('period_start', s);
  if (e) q.set('period_end', e);
  if (tm) q.set('tariff_mode', tm);
  const el = document.getElementById('t-bills');
  if (el && !_quietRefresh) el.innerHTML = '<p class="loading-msg">Computing…</p>';
  fetch('/api/tenants/bill?' + q.toString()).then(function(r) {{ return r.json(); }}).then(function(d) {{
    if (!d.ok || !d.report) {{ el.innerHTML = '<p style="color:var(--red)">' + esc(d.error||'No data') + '</p>'; return; }}
    const rep = d.report;
    const tariffLabel = rep.tariff_mode === 'dynamic' ? '⚡ Dynamic tariff' : '💲 Fixed tariff';
    let h = '<div style="font-size:12px;color:var(--muted);margin-bottom:6px">Period: ' + esc(rep.period_start) + ' to ' + esc(rep.period_end)
      + ' · ' + tariffLabel + ' · ' + (rep.price_eur_per_kwh_net*100).toFixed(2) + ' ct/kWh net'
      + ' · Base fee: ' + (rep.base_fee_eur_per_year_net||0).toFixed(2) + ' €/year net'
      + ' · VAT ' + (rep.vat_rate_percent||19).toFixed(0) + '%'
      + ' · Total: ' + rep.total_kwh.toFixed(1) + ' kWh · ' + rep.total_cost.toFixed(2) + ' €</div>';
    (rep.bills || []).forEach(function(b) {{
      h += '<div class="card" style="margin-bottom:6px">';
      h += '<div style="font-weight:650;margin-bottom:4px">' + esc(b.tenant.name) + (b.tenant.unit ? ' (' + esc(b.tenant.unit) + ')' : '') + ' · ' + b.tenant.persons + ' Pers.</div>';
      h += '<table style="width:100%;font-size:11px;border-collapse:collapse">';
      h += '<tr style="border-bottom:1px solid var(--border);color:var(--muted)"><th style="text-align:left;padding:3px">Item</th><th style="text-align:right">kWh</th><th style="text-align:right">€/kWh</th><th style="text-align:right">€</th></tr>';
      (b.line_items || []).forEach(function(li) {{
        // Grundpreis / base-fee lines have kwh=0 and unit_price=0 — show them as "–"
        var kwhCell = (li.kwh && li.kwh > 0) ? li.kwh.toFixed(1) : '–';
        var priceCell = (li.unit_price && li.unit_price > 0) ? li.unit_price.toFixed(4) : '–';
        h += '<tr style="border-bottom:1px solid var(--border)"><td style="padding:3px">' + esc(li.description) + '</td><td style="text-align:right">' + kwhCell + '</td><td style="text-align:right">' + priceCell + '</td><td style="text-align:right">' + li.amount.toFixed(2) + '</td></tr>';
      }});
      h += '<tr><td colspan="3" style="padding:3px;text-align:right;color:var(--muted)">Net</td><td style="text-align:right">' + b.subtotal_net.toFixed(2) + '</td></tr>';
      h += '<tr><td colspan="3" style="padding:3px;text-align:right;color:var(--muted)">VAT</td><td style="text-align:right">' + b.vat_amount.toFixed(2) + '</td></tr>';
      h += '<tr style="font-weight:700"><td colspan="3" style="padding:3px;text-align:right">Total gross</td><td style="text-align:right">' + b.total_gross.toFixed(2) + ' €</td></tr>';
      h += '</table>';
      h += '</div>';
    }});
    el.innerHTML = h;
  }}).catch(function(e) {{ el.innerHTML = '<p style="color:var(--red)">Error: ' + e + '</p>'; }});
}}

function refreshSyncStatus() {{
  const el = document.getElementById('sync-status-panel');
  if (!el) return;
  fetch('/api/sync/status').then(function(r) {{ return r.json(); }}).then(function(d) {{
    if (!d.ok) {{ el.textContent = 'Status unavailable: ' + (d.error || '?'); return; }}
    const devs = d.devices || [];
    if (!devs.length) {{ el.textContent = 'No devices configured'; return; }}
    const parts = devs.map(function(x) {{
      const ts = x.last_sync_ts ? new Date(x.last_sync_ts*1000).toLocaleString() : 'never';
      return x.name + ': ' + ts;
    }});
    el.textContent = 'Last sync: ' + parts.join('  ·  ');
  }}).catch(function(e) {{ el.textContent = 'Status fetch failed'; }});
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

// Widget helper: show server address + copy script
(function() {{
  var addrEl = document.getElementById('widget-addr');
  if (addrEl) addrEl.textContent = location.host;
}})();
async function copyWidgetScript() {{
  try {{
    const r = await fetch('/widget.js');
    const txt = await r.text();
    await navigator.clipboard.writeText(txt);
    var msg = document.getElementById('widget-copy-msg');
    if (msg) {{ msg.style.display = 'block'; setTimeout(function(){{ msg.style.display='none'; }}, 2000); }}
  }} catch(e) {{
    // Fallback: open in new tab
    window.open('/widget.js', '_blank');
  }}
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
      // Merge server history with client buffer, dedupe by timestamp,
      // keep within MAX_HIST_PTS.
      const byTs = {{}};
      hist[key].forEach(function(p) {{ if (p && p.ts) byTs[p.ts] = p; }});
      sparkData[key].forEach(function(p) {{ if (p && p.ts) byTs[p.ts] = p; }});
      const merged = Object.values(byTs).sort(function(a,b) {{ return a.ts - b.ts; }});
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
        const spin = document.getElementById('sp-in-' + key);
        if (spin) drawSparkline(spin, wndVals(buf, 'i_n'), '#a855f7', true);
        const sphz = document.getElementById('sp-hz-' + key);
        if (sphz) drawSparkline(sphz, wndVals(buf, 'hz'), '#06b6d4', true);
      }}
    }}
  }} catch(e) {{ /* silent */ }}
}}
/* ── Persistent live polling ──────────────────────────────────────────
   /api/state is polled in the background regardless of which pane is
   active. The Live grid is only re-rendered when currentPane === 'live',
   but every successful poll dispatches a `sea-live` CustomEvent so any
   pane can keep its own live elements in sync without doing its own
   /api/state fetches. Combined with TAB_LIVE_REFRESH below this means
   anything that updates in the Live tab also updates everywhere else. */
let _liveLatest = null;
function startLive() {{
  // Live-tab specific one-shot init: timescale buttons, history fetch.
  initTimescaleBtns();
  if (!_historyLoaded) loadHistory();
  // Re-render immediately from cached data so the grid isn't blank.
  if (_liveLatest) renderLive(_liveLatest, true);
  else tick(true);
  var _fb = document.getElementById('btn-freeze');
  if (_fb) {{
    _fb.removeEventListener('click', toggleFreeze);
    _fb.addEventListener('click', toggleFreeze);
  }}
}}
function stopLive() {{
  // No-op: the persistent timer keeps running. We invalidate the history
  // cache so the next time the Live tab is opened the server window is
  // refetched (otherwise sparklines look "reset" after liveWindowSec).
  _historyLoaded = false;
}}
function startLivePolling() {{
  if (liveTimer) return;
  liveTimer = setInterval(function() {{ if (!frozen) tick(false); }}, REFRESH_MS);
}}
function toggleFreeze() {{
  frozen = !frozen;
  var _fb = document.getElementById('btn-freeze');
  if (_fb) _fb.textContent = frozen ? '▶' : '⏸';
}}

async function tick(first) {{
  try {{
    const r = await fetch('/api/state');
    if (!r.ok) return;
    const data = await r.json();
    _liveLatest = data;
    if (currentPane === 'live') renderLive(data, first);
    // Always update the live timestamp in the header (visible on the Live
    // pane only, but harmless to set when hidden).
    const stamp = document.getElementById('live-stamp');
    if (stamp) stamp.textContent = new Date().toLocaleTimeString();
    // Notify all panes that fresh live data is available so they can
    // patch their own DOM elements without doing extra fetches.
    try {{
      window.dispatchEvent(new CustomEvent('sea-live', {{detail: data}}));
    }} catch(e) {{}}
  }} catch(e) {{
    // silent retry
  }}
}}

/* ── Update check banner ──────────────────────────────────────────────
   Polls /api/updates/cached (background-thread result, no network call)
   on startup and every 30 minutes. When a newer GitHub release is
   detected, shows a banner at the top of the Live pane. Clicking it
   jumps directly to the Updates section on the settings page. */
function goToUpdates() {{
  window.location.href = '/settings#sec-updates';
}}
async function checkForUpdateBanner() {{
  try {{
    const r = await fetch('/api/updates/cached', {{ cache: 'no-store' }});
    if (!r.ok) return;
    const j = await r.json();
    const banner = document.getElementById('update-banner');
    if (!banner) return;
    if (j && j.has_update && j.latest_tag) {{
      const tag = document.getElementById('update-banner-tag');
      if (tag) tag.textContent = 'v' + String(j.latest_tag).replace(/^v/i, '');
      banner.style.display = 'block';
    }} else {{
      banner.style.display = 'none';
    }}
  }} catch(e) {{ /* silent */ }}
}}
let _updateBannerTimer = null;
function startUpdateBannerPolling() {{
  if (_updateBannerTimer) return;
  // First check after a short delay so the background thread has a
  // chance to populate the cache (first check runs ~15s after start).
  setTimeout(checkForUpdateBanner, 20000);
  // Then re-poll every 30 minutes — the server cache refreshes every 6h
  // so more frequent polling is pointless.
  _updateBannerTimer = setInterval(checkForUpdateBanner, 30 * 60 * 1000);
}}

/* ── Per-tab periodic refresh ─────────────────────────────────────────
   Each entry maps a pane id to a refresh interval in ms. While that
   pane is active and not frozen, the corresponding load function is
   re-invoked at the configured cadence so aggregated views (which can't
   be patched cheaply via sea-live) stay current.

   Only tabs that actually display values which change live (current
   power, today kWh / €, etc.) are listed. Purely historical tabs like
   heatmap, compare, anomalies, forecast, tariff, ev_log, weather and
   smart_sched are NOT refreshed because there is nothing to update.
   The CO₂ tab has its own _co2LiveTimer (1s) and is excluded too. */
const TAB_LIVE_REFRESH = {{
  costs: 5000,
  solar: 5000,
  standby: 5000,
  sankey: 3000,
  ev: 5000,
  battery: 5000,
  goals: 10000,
  nilm: 15000,
}};
let _tabRefreshTimer = null;
function _runTabRefresh(pane) {{
  if (currentPane !== pane || frozen) return;
  // Suppress the "Loading…" spinner during periodic refreshes so users
  // don't see a flash every few seconds.
  _quietRefresh = true;
  try {{
    if (pane === 'costs')        loadCosts();
    else if (pane === 'solar')   {{ try {{ loadSolar(typeof solarPeriod !== 'undefined' ? solarPeriod : 'month'); }} catch(e) {{}} }}
    else if (pane === 'standby') loadStandby();
    else if (pane === 'sankey')  loadSankey();
    else if (pane === 'ev')      loadEv();
    else if (pane === 'battery') {{ if (typeof loadBattery === 'function') loadBattery(); }}
    else if (pane === 'goals')   {{ if (typeof loadGoals === 'function') loadGoals(); }}
    else if (pane === 'nilm')    loadNilm();
  }} finally {{
    // Reset on next tick so the load function (which is async) has finished
    // its synchronous spinner-skip path before we re-enable it for manual loads.
    setTimeout(function() {{ _quietRefresh = false; }}, 0);
  }}
}}
function startTabLiveRefresh(pane) {{
  stopTabLiveRefresh();
  var ms = TAB_LIVE_REFRESH[pane];
  if (!ms) return;
  _tabRefreshTimer = setInterval(function() {{ _runTabRefresh(pane); }}, ms);
}}
function stopTabLiveRefresh() {{
  if (_tabRefreshTimer) {{ clearInterval(_tabRefreshTimer); _tabRefreshTimer = null; }}
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
    buf.push({{ ts: Date.now(), w: d.power_w || 0, v: d.voltage_v || 0, a: d.current_a || 0, phases: d.phases ? d.phases.slice() : [], i_n: d.i_n || 0, q: d.q_total_var || 0, q_phases: d.q_phases ? d.q_phases.slice() : [], hz: d.freq_hz || 0 }});
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
      else if (metric === 'hz') title = t('web.kv.freq', 'Frequency') + ' (Hz)';
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
    phaseHtml = '<dl class="dev-kv" id="kv-phases-' + d.key + '">';
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
      '<div class="sparkline-wrap" style="margin-top:6px" data-metric="hz" data-devkey="' + d.key + '"><div class="sparkline-label">' + t('web.kv.freq', 'Frequency') + ' (Hz)</div><canvas class="sparkline-sm" id="sp-hz-' + d.key + '"></canvas></div>' +
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
  // Frequency (Hz) sparkline – relative scale (grid freq varies narrowly)
  const sphz = document.getElementById('sp-hz-' + d.key);
  if (sphz && buf) drawSparkline(sphz, wndVals(buf, 'hz'), '#06b6d4', true);
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
      const phaseDl = exp.querySelector('#kv-phases-' + d.key);
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
  }} else if (metric === 'hz') {{
    items.push({{ label: t('web.kv.freq', 'Frequency') + ' (Hz)', color: '#06b6d4' }});
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
  }} else if (metric === 'hz') {{
    series.push(visPts.map(function(p) {{ return p.hz||0; }}));
    colors.push('#06b6d4');
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
  if (!_quietRefresh) el.innerHTML = '<p class="loading-msg">' + t('web.loading', 'Loading…') + '</p>';
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
    var color = diff <= 0 ? '#4caf50' : '#e53935';
    return '<span style="color:' + color + ';font-weight:bold">' + arrow + ' ' + Math.abs(diff).toFixed(2) + ' \u20ac</span>';
  }}
  // 24h Spot Price Chart (at top, before device cards)
  if (data.spot_enabled && data.spot_chart && data.spot_chart.length > 0) {{
    var fixedCt = data.fixed_ct_per_kwh || 0;
    var curSpotHtml = '';
    if (data.current_spot_ct != null && fixedCt > 0) {{
      var delta = data.current_spot_ct - fixedCt;
      var arrow = delta > 0 ? '\u25b2' : '\u25bc';
      var sign = delta > 0 ? '+' : '';
      var priceColor = delta <= 0 ? '#4caf50' : '#e53935';
      curSpotHtml = '<div style="font-size:16px;font-weight:bold;color:' + priceColor + ';margin:2px 0 8px 0;white-space:nowrap">' +
        data.current_spot_ct.toFixed(1) + ' ct/kWh ' +
        '<span style="font-size:11px;font-weight:normal">(' + arrow + sign + delta.toFixed(1) + ' ct)</span></div>';
    }}
    html += '<div class="card" style="margin-bottom:10px">' +
      '<div style="font-size:12px;font-weight:650;color:#ff9800;margin-bottom:6px;text-transform:uppercase;letter-spacing:0.5px">' +
      '\u26a1 ' + t('spot.chart.title', 'Spot Price 24h') + '</div>' +
      curSpotHtml +
      '<canvas id="spot-24h-chart" style="width:100%;height:160px"></canvas>' +
      '<div id="spot-chart-labels" style="display:flex;justify-content:space-between;font-size:10px;color:var(--muted);margin-top:2px;padding:0 4px"></div>' +
      '</div>';
  }}

  html += '<div class="card-grid">';
  data.devices.forEach(function(d) {{
    // Fixed tariff section
    var fixedLabel = '<div style="font-size:11px;color:#2196F3;margin-bottom:4px">\U0001f4b2 ' + t('plots.dynprice.fixed', 'Fixed price') + '</div>';
    var fixedGrid = '<div class="metric-grid">' +
      metricCardHtml(t('web.costs.today', 'Today'), fmt(d.today_eur,2,'\u20ac'), fmt(d.today_kwh,3,'kWh')) +
      metricCardHtml(t('web.costs.week', 'Week'), fmt(d.week_eur,2,'\u20ac'), fmt(d.week_kwh,3,'kWh')) +
      metricCardHtml(t('web.costs.month', 'Month'), fmt(d.month_eur,2,'\u20ac'), fmt(d.month_kwh,3,'kWh')) +
      metricCardHtml(t('web.costs.projected', 'Forecast'), fmt(d.proj_eur,2,'\u20ac'), fmt(d.proj_kwh,1,'kWh')) +
      '</div>';
    // Dynamic tariff section
    var dynSection = spotActive
      ? '<div style="margin-top:8px;padding-top:8px;border-top:1px solid var(--border)">' +
          '<div style="font-size:11px;color:#ff9800;margin-bottom:4px">\u26a1 ' + t('spot.cost_label', 'Dyn. tariff') + '</div>' +
          '<div class="metric-grid">' +
          metricCardHtml(t('web.costs.today', 'Today'), fmt(d.today_spot_eur,2,'\u20ac'), spotSub('today',d), true) +
          metricCardHtml(t('web.costs.week', 'Week'), fmt(d.week_spot_eur,2,'\u20ac'), spotSub('week',d), true) +
          metricCardHtml(t('web.costs.month', 'Month'), fmt(d.month_spot_eur,2,'\u20ac'), spotSub('month',d), true) +
          metricCardHtml(t('web.costs.projected', 'Forecast'), fmt(d.proj_spot_eur||0,2,'\u20ac'), spotSub('proj',d), true) +
          '</div></div>'
      : '';
    // Card: fixed costs → dynamic costs
    html += '<div class="card">' +
      '<div class="card-title">' + esc(d.name || d.key) + '</div>' +
      fixedLabel + fixedGrid +
      dynSection +
      '</div>';
  }});
  html += '</div>';

  // ── Device cost ranking + donut (side by side) ──
  const devs = data.devices || [];
  if (devs.length > 1) {{
    html += '<div class="nilm-two-col" style="margin-top:10px">';
    // Cost donut
    html += '<div class="card"><div style="font-size:14px;font-weight:700;margin-bottom:8px">' + t('web.costs.breakdown','Cost breakdown') + ' (' + t('web.costs.month','Month') + ')</div>';
    html += '<canvas id="costs-donut" style="width:100%;height:200px"></canvas>';
    html += '<div id="costs-donut-legend" style="margin-top:6px"></div></div>';
    // Ranking bar
    html += '<div class="card"><div style="font-size:14px;font-weight:700;margin-bottom:8px">' + t('web.costs.ranking','Cost ranking') + '</div>';
    html += '<canvas id="costs-ranking-bar" style="width:100%;height:200px"></canvas></div>';
    html += '</div>';
  }}

  // ── Period comparison chart (today / week / month / year) ──
  if (data.summary) {{
    html += '<div class="card" style="margin-top:10px"><div style="font-size:14px;font-weight:700;margin-bottom:8px">' + t('web.costs.period_compare','Period comparison') + '</div>';
    html += '<canvas id="costs-period-chart" style="width:100%;height:150px"></canvas></div>';
  }}

  // ── Per-device comparison bars (month kWh) ──
  if (devs.length > 1) {{
    html += '<div class="card" style="margin-top:10px"><div style="font-size:14px;font-weight:700;margin-bottom:8px">' + t('web.costs.device_compare','Device comparison (month kWh)') + '</div>';
    devs.slice().sort(function(a,b){{ return (b.month_kwh||0) - (a.month_kwh||0); }}).forEach(function(d) {{
      const maxKwh = Math.max.apply(null, devs.map(function(x){{ return x.month_kwh||0; }})) || 1;
      const pct = Math.round((d.month_kwh||0) / maxKwh * 100);
      const share = data.summary && data.summary.month_kwh > 0 ? Math.round((d.month_kwh||0) / data.summary.month_kwh * 100) : 0;
      html += '<div style="margin-bottom:6px">';
      html += '<div style="display:flex;justify-content:space-between;font-size:12px;margin-bottom:2px"><span style="font-weight:600">' + esc(d.name||d.key) + '</span><span style="color:var(--muted)">' + fmt(d.month_kwh,1) + ' kWh | ' + fmt(d.month_eur,2) + ' \u20ac | ' + share + '%</span></div>';
      html += '<div style="background:var(--bg);border-radius:4px;height:8px;overflow:hidden"><div style="width:' + pct + '%;height:100%;background:var(--accent);border-radius:4px"></div></div>';
      html += '</div>';
    }});
    html += '</div>';
  }}

  // ── Projection card ──
  if (data.summary && data.summary.proj_kwh) {{
    const s = data.summary;
    html += '<div class="card" style="margin-top:10px"><div style="font-size:14px;font-weight:700;margin-bottom:8px">' + t('web.costs.projection','Month projection') + '</div>';
    html += '<div class="nilm-metrics">';
    html += _nilmMetricCard('📅', t('web.costs.projected','Forecast'), fmt(s.proj_kwh,0) + ' kWh', fmt(s.proj_eur,0) + ' \u20ac');
    html += _nilmMetricCard('📆', t('web.costs.year_proj','Year projection'), fmt((s.proj_kwh||0)*12,0) + ' kWh', fmt((s.proj_eur||0)*12,0) + ' \u20ac');
    const lastM = s.last_month_kwh || 0;
    const projDelta = lastM > 0 ? Math.round(((s.proj_kwh||0) - lastM) / lastM * 100) : 0;
    const dIcon = projDelta > 5 ? '📈' : projDelta < -5 ? '📉' : '➡️';
    const dCol = projDelta > 5 ? '#dc2626' : projDelta < -5 ? '#16a34a' : 'var(--muted)';
    html += _nilmMetricCard(dIcon, t('web.costs.vs_last','vs. previous month'), '<span style="color:'+dCol+'">' + (projDelta>0?'+':'') + projDelta + '%</span>', fmt(lastM,0) + ' kWh last month');
    const dailyCost = (s.month_eur||0) / Math.max(1, new Date().getDate());
    html += _nilmMetricCard('💰', t('web.costs.daily_avg','Daily average'), fmt(dailyCost,2) + ' \u20ac', fmt(dailyCost * 365,0) + ' \u20ac/year');
    html += '</div></div>';
  }}

  // Show upcoming tariff changes if any
  if (data.tariff_schedule && data.tariff_schedule.length > 0) {{
    html += '<div class="card" style="margin-top:10px"><div style="font-size:12px;font-weight:650;color:var(--muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:0.5px">' + t('settings.pricing.tariff_schedule', 'Tariff Schedule') + '</div>';
    html += '<table style="width:100%;font-size:12px;border-collapse:collapse">';
    html += '<tr style="border-bottom:1px solid var(--border)"><th style="text-align:left;padding:4px">' + t('settings.pricing.tariff_start_date', 'Start Date') + '</th><th style="text-align:right;padding:4px">' + t('settings.pricing.tariff_price', 'Price') + '</th><th style="text-align:right;padding:4px">' + t('settings.pricing.tariff_base_fee', 'Base Fee') + '</th></tr>';
    data.tariff_schedule.sort(function(a,b) {{ return a.start_date < b.start_date ? -1 : 1; }}).forEach(function(tp) {{
      var today = new Date().toISOString().slice(0,10);
      var active = tp.start_date <= today;
      var style = active ? 'font-weight:bold' : 'color:var(--muted)';
      html += '<tr style="border-bottom:1px solid var(--border);' + style + '"><td style="padding:4px">' + esc(tp.start_date) + (active ? ' \u2713' : '') + '</td><td style="text-align:right;padding:4px">' + tp.price.toFixed(4) + ' \u20ac/kWh</td><td style="text-align:right;padding:4px">' + tp.base_fee.toFixed(2) + ' \u20ac/' + t('web.costs.year', 'Year') + '</td></tr>';
    }});
    html += '</table></div>';
  }}

  el.innerHTML = html;

  // Draw charts
  requestAnimationFrame(function() {{
    if (data.spot_enabled && data.spot_chart && data.spot_chart.length > 0) {{
      _drawSpotChart(data.spot_chart, data.fixed_ct_per_kwh || 0);
    }}
    if (devs.length > 1) {{
      _drawCostsDonut(devs);
      _drawCostsRanking(devs);
    }}
    if (data.summary) _drawCostsPeriodChart(data.summary);
  }});
}}

function _drawCostsDonut(devs) {{
  const canvas = document.getElementById('costs-donut');
  const legend = document.getElementById('costs-donut-legend');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio||1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width*dpr; canvas.height = rect.height*dpr;
  ctx.scale(dpr,dpr);
  const W = rect.width, H = rect.height, cx = W/2, cy = H/2, R = Math.min(cx,cy)-8, r = R*0.55;
  const colors = ['#3b82f6','#ef4444','#f59e0b','#22c55e','#8b5cf6','#ec4899','#14b8a6','#f97316','#6366f1','#06b6d4'];
  const total = devs.reduce(function(s,d){{ return s + (d.month_eur||0); }},0) || 1;
  let angle = -Math.PI/2;
  let legHtml = '<div style="display:flex;flex-wrap:wrap;gap:4px">';
  devs.forEach(function(d,i) {{
    const v = d.month_eur || 0;
    if (v <= 0) return;
    const slice = (v/total)*Math.PI*2;
    const col = colors[i%colors.length];
    ctx.fillStyle = col;
    ctx.beginPath(); ctx.arc(cx,cy,R,angle,angle+slice); ctx.arc(cx,cy,r,angle+slice,angle,true); ctx.closePath(); ctx.fill();
    angle += slice;
    legHtml += '<span style="font-size:10px;display:flex;align-items:center;gap:2px"><span style="width:8px;height:8px;border-radius:50%;background:'+col+';display:inline-block"></span>'+esc(d.name||d.key)+' ('+fmt(v,2)+'\u20ac)</span>';
  }});
  ctx.fillStyle = getComputedStyle(document.body).getPropertyValue('--fg')||'#111';
  ctx.font = 'bold 16px sans-serif'; ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
  ctx.fillText(fmt(total,2) + ' \u20ac', cx, cy-6);
  ctx.font = '10px sans-serif'; ctx.fillStyle = getComputedStyle(document.body).getPropertyValue('--muted')||'#666';
  ctx.fillText(t('web.costs.month','Month'), cx, cy+10);
  legHtml += '</div>';
  if (legend) legend.innerHTML = legHtml;
}}

function _drawCostsRanking(devs) {{
  const sorted = devs.slice().sort(function(a,b){{ return (b.month_eur||0)-(a.month_eur||0); }});
  const names = sorted.map(function(d){{ return (d.name||d.key).substring(0,14); }});
  const vals = sorted.map(function(d){{ return d.month_eur||0; }});
  const colors = ['#3b82f6','#ef4444','#f59e0b','#22c55e','#8b5cf6','#ec4899','#14b8a6','#f97316'];
  _drawBarChart('costs-ranking-bar', names, vals, {{colors: colors.slice(0, vals.length), decimals: 2}});
}}

function _drawCostsPeriodChart(s) {{
  const labels = [t('web.costs.today','Today'), t('web.costs.week','Week'), t('web.costs.month','Month'), t('web.costs.year','Year')];
  const kwh = [s.today_kwh||0, s.week_kwh||0, s.month_kwh||0, s.year_kwh||0];
  const eur = [s.today_eur||0, s.week_eur||0, s.month_eur||0, s.year_eur||0];
  // Draw as grouped bars: kWh + EUR
  const canvas = document.getElementById('costs-period-chart');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio||1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width*dpr; canvas.height = rect.height*dpr;
  ctx.scale(dpr,dpr);
  const W = rect.width, H = rect.height;
  const pad = {{top:10,right:10,bottom:24,left:44}};
  const cW = W-pad.left-pad.right, cH = H-pad.top-pad.bottom;
  const n = labels.length;
  const maxV = Math.max.apply(null, kwh.concat([0.1])) * 1.2;
  const groupW = cW / n;
  const barW = groupW * 0.35;
  const fg = getComputedStyle(document.body).getPropertyValue('--muted')||'#999';
  const border = getComputedStyle(document.body).getPropertyValue('--border')||'#e0e0e0';
  // Grid
  ctx.strokeStyle = border; ctx.lineWidth = 0.5;
  for (let i=0;i<=4;i++) {{
    const y = pad.top+cH-(cH*i/4);
    ctx.beginPath(); ctx.moveTo(pad.left,y); ctx.lineTo(pad.left+cW,y); ctx.stroke();
    ctx.fillStyle = fg; ctx.font = '9px sans-serif'; ctx.textAlign = 'right';
    ctx.fillText((maxV*i/4).toFixed(1), pad.left-4, y+3);
  }}
  // Bars
  labels.forEach(function(lbl, i) {{
    const x = pad.left + i * groupW;
    // kWh bar
    const h1 = (kwh[i]/maxV)*cH;
    ctx.fillStyle = '#3b82f6';
    ctx.beginPath();
    if (ctx.roundRect) ctx.roundRect(x+groupW*0.1, pad.top+cH-h1, barW, h1, [3,3,0,0]);
    else ctx.rect(x+groupW*0.1, pad.top+cH-h1, barW, h1);
    ctx.fill();
    // EUR label on bar
    ctx.fillStyle = '#f59e0b'; ctx.font = 'bold 9px sans-serif'; ctx.textAlign = 'center';
    ctx.fillText(fmt(eur[i],2)+'\u20ac', x+groupW*0.1+barW/2, pad.top+cH-h1-3);
    // Label
    ctx.fillStyle = fg; ctx.font = '10px sans-serif'; ctx.textAlign = 'center';
    ctx.fillText(lbl, x+groupW/2, pad.top+cH+14);
  }});
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

  // "Now" vertical marker line + label
  if (hourly.length > 1) {{
    var tFirst = hourly[0].ts;
    var tStep = (hourly.length > 1 ? (hourly[1].ts - hourly[0].ts) : 3600) || 3600;
    var tLast = hourly[hourly.length - 1].ts + tStep;
    if (now >= tFirst && now <= tLast) {{
      var frac = (now - tFirst) / (tLast - tFirst);
      var nowX = pad.left + frac * plotW;
      ctx.strokeStyle = '#ff1744';
      ctx.lineWidth = 2;
      ctx.setLineDash([4, 3]);
      ctx.beginPath(); ctx.moveTo(nowX, pad.top); ctx.lineTo(nowX, pad.top + plotH); ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = '#ff1744';
      ctx.font = '10px sans-serif';
      ctx.textAlign = 'center';
      ctx.fillText('jetzt', nowX, pad.top - 2);
    }}
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

function metricCardHtml(label, value, sub, rawSub) {{
  // Always render .metric-sub (with non-breaking space placeholder if empty)
  // so every card has the same height and values across cards align horizontally.
  var subHtml = sub ? (rawSub ? sub : esc(sub)) : '&nbsp;';
  return '<div class="metric-card">' +
    '<div class="metric-label">' + esc(label) + '</div>' +
    '<div class="metric-value">' + esc(value) + '</div>' +
    '<div class="metric-sub">' + subHtml + '</div>' +
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
      opt.value = d.key; opt.textContent = d.name || d.key;
      sel.appendChild(opt);
    }});
  }}
  const ySel = document.getElementById('hm-year');
  if (ySel.children.length === 0) {{
    const now = new Date().getFullYear();
    for (let y = now; y >= now - 4; y--) {{
      const opt = document.createElement('option');
      opt.value = y; opt.textContent = y;
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
  const stWrap = document.getElementById('hm-stats-wrap');
  const chWrap = document.getElementById('hm-charts-wrap');
  if (!_quietRefresh) calWrap.innerHTML = '<p class="loading-msg">' + t('web.loading', 'Loading…') + '</p>';
  hrWrap.innerHTML = ''; if (stWrap) stWrap.innerHTML = ''; if (chWrap) chWrap.innerHTML = '';
  if (!device) {{ calWrap.innerHTML = '<p class="info-msg">' + t('web.dash.select_device', 'Select a device.') + '</p>'; return; }}
  try {{
    const r = await fetch('/api/heatmap?device=' + encodeURIComponent(device) + '&year=' + year + '&unit=' + unit);
    if (!r.ok) throw new Error(r.status);
    const data = await r.json();
    if (data.summary && stWrap) renderHmStats(data, stWrap, unit);
    renderHeatmapCalendar(data, calWrap, unit);
    renderHeatmapHourly(data, hrWrap, unit);
    if (data.summary && chWrap) renderHmCharts(data, chWrap, unit);
  }} catch(e) {{
    calWrap.innerHTML = '<p class="error-msg">Error: ' + e.message + '</p>';
  }}
}}

/* ── Color functions ── */
function ratioColor(ratio) {{
  if (ratio <= 0.5) {{
    const t = ratio * 2;
    return 'rgb(' + Math.round(34 + 200 * t) + ',' + Math.round(197 - 18 * t) + ',' + Math.round(94 - 86 * t) + ')';
  }} else {{
    const t = (ratio - 0.5) * 2;
    return 'rgb(' + Math.round(234 + 5 * t) + ',' + Math.round(179 - 111 * t) + ',' + Math.round(8 + 60 * t) + ')';
  }}
}}
function ratioCo2Color(ratio) {{
  if (ratio <= 0.5) {{
    const t = ratio * 2;
    return 'rgb(255,' + Math.round(235 - 68 * t) + ',' + Math.round(132 - 94 * t) + ')';
  }} else {{
    const t = (ratio - 0.5) * 2;
    return 'rgb(' + Math.round(255 - 40 * t) + ',' + Math.round(167 - 142 * t) + ',' + Math.round(38 - 10 * t) + ')';
  }}
}}
function ratioEurColor(ratio) {{
  if (ratio <= 0.5) {{
    const t = ratio * 2;
    return 'rgb(' + Math.round(46 + 160 * t) + ',' + Math.round(204 - 64 * t) + ',' + Math.round(113 - 75 * t) + ')';
  }} else {{
    const t = (ratio - 0.5) * 2;
    return 'rgb(' + Math.round(206 + 25 * t) + ',' + Math.round(140 - 80 * t) + ',' + Math.round(38 + 15 * t) + ')';
  }}
}}
function hmColorFn(unit) {{ return unit === 'co2' ? ratioCo2Color : unit === 'eur' ? ratioEurColor : ratioColor; }}
function hmFmtVal(v, unit) {{
  if (unit === 'co2') return fmt(v, 1, 'g CO\u2082');
  if (unit === 'eur') return fmt(v, 2, '\u20ac');
  return fmt(v, 3, 'kWh');
}}
function hmUnitLabel(unit) {{
  if (unit === 'co2') return 'g CO\u2082';
  if (unit === 'eur') return '\u20ac';
  return 'kWh';
}}

/* ── Summary statistics cards ── */
function renderHmStats(data, el, unit) {{
  var s = data.summary;
  if (!s) return;
  var u = hmUnitLabel(unit);
  var html = '<div class="nilm-metrics" style="margin-bottom:10px">';
  html += '<div class="card"><div class="metric-label">\u2211 ' + t('web.hm.total', 'Total') + '</div><div class="metric-value">' + hmFmtVal(s.total, unit) + '</div></div>';
  html += '<div class="card"><div class="metric-label">\u00f8 ' + t('web.hm.avg_daily', 'Daily avg') + '</div><div class="metric-value">' + hmFmtVal(s.avg_daily, unit) + '</div></div>';
  html += '<div class="card"><div class="metric-label">\U0001f4c5 ' + t('web.hm.days', 'Days') + '</div><div class="metric-value">' + (s.days_with_data || 0) + '</div></div>';
  html += '<div class="card"><div class="metric-label">\u23f0 ' + t('web.hm.peak_hour', 'Peak hour') + '</div><div class="metric-value">' + (s.peak_hour != null ? s.peak_hour + ':00' : '\u2013') + '</div><div style="font-size:10px;color:var(--muted)">\u00f8 ' + hmFmtVal(s.peak_hour_avg || 0, unit) + '</div></div>';
  html += '</div>';

  // Weekday vs Weekend + Peak/Min day
  html += '<div class="nilm-metrics">';
  html += '<div class="card"><div class="metric-label">\U0001f4bc ' + t('web.hm.weekday', 'Weekday avg') + '</div><div class="metric-value">' + hmFmtVal(s.weekday_avg, unit) + '</div></div>';
  html += '<div class="card"><div class="metric-label">\U0001f3d6\ufe0f ' + t('web.hm.weekend', 'Weekend avg') + '</div><div class="metric-value">' + hmFmtVal(s.weekend_avg, unit) + '</div></div>';
  if (s.peak_day) {{
    html += '<div class="card"><div class="metric-label">\U0001f4a5 ' + t('web.hm.peak_day', 'Peak day') + '</div><div class="metric-value">' + hmFmtVal(s.peak_day.value, unit) + '</div><div style="font-size:10px;color:var(--muted)">' + s.peak_day.date + '</div></div>';
  }}
  if (s.min_day) {{
    html += '<div class="card"><div class="metric-label">\U0001f33f ' + t('web.hm.min_day', 'Best day') + '</div><div class="metric-value">' + hmFmtVal(s.min_day.value, unit) + '</div><div style="font-size:10px;color:var(--muted)">' + s.min_day.date + '</div></div>';
  }}
  html += '</div>';
  el.innerHTML = html;
}}

/* ── Calendar heatmap ── */
function renderHeatmapCalendar(data, el, unit) {{
  const calArr = data.calendar || [];
  const daily = {{}};
  calArr.forEach(function(item) {{ daily[item.date] = item.value; }});
  const year = parseInt(document.getElementById('hm-year').value);
  const start = new Date(year, 0, 1);
  while (start.getDay() !== 1) start.setDate(start.getDate() - 1);
  const vals = Object.values(daily).filter(function(v) {{ return v > 0; }});
  const maxVal = vals.length ? Math.max(...vals) : 1;

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

  const pane = el.closest('.pane') || document.body;
  const availW = pane.clientWidth - 32;
  const numWeeks = weeks.length;
  const calCellFromW = Math.floor((availW - (numWeeks - 1) * 2) / numWeeks);
  const calCellSize = Math.max(10, Math.min(calCellFromW, 18));

  const _dtfMonth = new Intl.DateTimeFormat(document.documentElement.lang || 'de', {{month: 'short'}});
  const monthNames = Array.from({{length: 12}}, function(_, i) {{ return _dtfMonth.format(new Date(2000, i, 1)); }});
  let monthLabelHtml = '<div class="hm-month-labels">';
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
  const _colorFn = hmColorFn(unit);
  weeks.forEach(function(week) {{
    gridHtml += '<div class="hm-week">';
    week.forEach(function(day) {{
      const key = day.toISOString().slice(0,10);
      const v = daily[key] || 0;
      const ratio = maxVal > 0 ? v / maxVal : 0;
      const label = hmFmtVal(v, unit);
      const inYear = day.getFullYear() === year;
      const bg = inYear && v > 0 ? _colorFn(ratio) : 'var(--chipbg)';
      gridHtml += '<div class="hm-day" style="width:' + calCellSize + 'px;height:' + calCellSize + 'px;background:' + bg + '" data-date="' + key + '" data-val="' + label + '"></div>';
    }});
    gridHtml += '</div>';
  }});
  gridHtml += '</div>';

  // Color legend
  var legendHtml = '<div style="display:flex;align-items:center;gap:4px;margin-top:6px;font-size:10px;color:var(--muted)">';
  legendHtml += '<span>' + t('web.hm.less', 'Less') + '</span>';
  for (var li = 0; li <= 4; li++) {{
    legendHtml += '<div style="width:12px;height:12px;border-radius:2px;background:' + _colorFn(li / 4) + '"></div>';
  }}
  legendHtml += '<span>' + t('web.hm.more', 'More') + '</span>';
  legendHtml += '<span style="margin-left:auto">Max: ' + hmFmtVal(maxVal, unit) + '</span>';
  legendHtml += '</div>';

  el.innerHTML = '<div class="card"><div class="card-title">' + t('web.hm.year_overview', 'Year overview') + ' ' + year + '</div><div class="hm-calendar">' + monthLabelHtml + gridHtml + '</div>' + legendHtml + '</div>';

  el.querySelectorAll('.hm-day').forEach(function(cell) {{
    cell.addEventListener('mousemove', function(e) {{ showHmTooltip(e, cell.dataset.date + ': ' + cell.dataset.val); }});
    cell.addEventListener('touchstart', function(e) {{ e.preventDefault(); showHmTooltip(e.touches[0], cell.dataset.date + ': ' + cell.dataset.val); }}, {{passive:false}});
    cell.addEventListener('mouseleave', hideHmTooltip);
    cell.addEventListener('touchend', hideHmTooltip);
  }});
}}

/* ── Hourly pattern table ── */
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

  const pane = el.closest('.pane') || document.body;
  const availW = pane.clientWidth - 32;
  const labelW = 22;
  const cellFromW = Math.floor((availW - labelW - 2 * 25) / 24);
  const availHHr = window.innerHeight - 290;
  const cellFromH = Math.floor((availHHr - 20) / 7 - 2);
  const cellSize = Math.max(8, Math.min(cellFromW, cellFromH));
  const _colorFn = hmColorFn(unit);

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
      const bg = v > 0 ? _colorFn(ratio) : 'var(--chipbg)';
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
    cell.addEventListener('touchstart', function(e) {{ e.preventDefault(); showHmTooltip(e.touches[0], cell.dataset.tip); }}, {{passive:false}});
    cell.addEventListener('touchend', hideHmTooltip);
  }});
}}

/* ── Monthly + Weekday charts ── */
function renderHmCharts(data, el, unit) {{
  var s = data.summary;
  if (!s) return;
  var html = '<div class="nilm-two-col">';
  // Monthly bar chart
  if (s.monthly && Object.keys(s.monthly).length > 0) {{
    html += '<div class="card"><div style="font-size:12px;font-weight:650;color:var(--muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:.5px">' + t('web.hm.monthly', 'Monthly Breakdown') + '</div>';
    html += '<canvas id="hm-monthly-chart" style="width:100%;height:200px"></canvas></div>';
  }}
  // Weekday bar chart
  if (s.weekday_avgs) {{
    html += '<div class="card"><div style="font-size:12px;font-weight:650;color:var(--muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:.5px">' + t('web.hm.weekday_chart', 'Weekday Pattern') + '</div>';
    html += '<canvas id="hm-weekday-chart" style="width:100%;height:200px"></canvas></div>';
  }}
  html += '</div>';
  el.innerHTML = html;

  if (s.monthly && Object.keys(s.monthly).length > 0) {{
    setTimeout(function() {{ _drawHmMonthlyChart(s.monthly, unit); }}, 50);
  }}
  if (s.weekday_avgs) {{
    setTimeout(function() {{ _drawHmWeekdayChart(s.weekday_avgs, unit); }}, 60);
  }}
}}

function _hmInitCanvas(id) {{
  var el = document.getElementById(id);
  if (!el) return null;
  var dpr = window.devicePixelRatio || 1;
  var rect = el.getBoundingClientRect();
  el.width = rect.width * dpr; el.height = rect.height * dpr;
  var ctx = el.getContext('2d'); ctx.scale(dpr, dpr);
  return {{ctx: ctx, W: rect.width, H: rect.height}};
}}

function _drawHmMonthlyChart(monthly, unit) {{
  var c = _hmInitCanvas('hm-monthly-chart');
  if (!c) return;
  var ctx = c.ctx, W = c.W, H = c.H;
  var muted = getComputedStyle(document.body).getPropertyValue('--muted') || '#999';
  var border = getComputedStyle(document.body).getPropertyValue('--border') || '#e0e0e0';
  var pad = {{top: 10, right: 12, bottom: 28, left: 48}};
  var cW = W - pad.left - pad.right, cH = H - pad.top - pad.bottom;

  var months = [];
  for (var m = 1; m <= 12; m++) {{
    months.push({{ m: m, val: monthly[String(m)] || 0 }});
  }}
  var maxV = Math.max.apply(null, months.map(function(x){{ return x.val; }})) * 1.15 || 1;
  var barW = Math.max(6, (cW / 12) * 0.7);
  var _colorFn = hmColorFn(unit);
  var _dtfMonth = new Intl.DateTimeFormat(document.documentElement.lang || 'de', {{month: 'short'}});

  // Grid
  ctx.strokeStyle = border; ctx.lineWidth = 0.5;
  ctx.fillStyle = muted; ctx.font = '10px sans-serif'; ctx.textAlign = 'right';
  for (var i = 0; i <= 4; i++) {{
    var gy = pad.top + cH - (cH * i / 4);
    ctx.beginPath(); ctx.moveTo(pad.left, gy); ctx.lineTo(pad.left + cW, gy); ctx.stroke();
    ctx.fillText(hmFmtVal(maxV * i / 4, unit), pad.left - 4, gy + 3);
  }}

  // Bars
  months.forEach(function(mo, idx) {{
    var x = pad.left + (cW * (idx + 0.5) / 12) - barW / 2;
    var bh = cH * (mo.val / maxV);
    var ratio = maxV > 0 ? mo.val / maxV : 0;
    ctx.fillStyle = _colorFn(ratio); ctx.globalAlpha = 0.85;
    ctx.fillRect(x, pad.top + cH - bh, barW, bh);
    ctx.globalAlpha = 1;
    // Value on top (short label)
    if (mo.val > 0) {{
      ctx.fillStyle = muted; ctx.font = '9px sans-serif'; ctx.textAlign = 'center';
      var lbl = unit === 'co2' ? Math.round(mo.val) + '' : unit === 'eur' ? mo.val.toFixed(0) : mo.val >= 10 ? mo.val.toFixed(0) : mo.val.toFixed(1);
      ctx.fillText(lbl, x + barW / 2, pad.top + cH - bh - 4);
    }}
  }});

  // X-axis
  ctx.fillStyle = muted; ctx.font = '9px sans-serif'; ctx.textAlign = 'center';
  months.forEach(function(mo, idx) {{
    var lbl = _dtfMonth.format(new Date(2000, mo.m - 1, 1));
    ctx.fillText(lbl, pad.left + (cW * (idx + 0.5) / 12), pad.top + cH + 16);
  }});
}}

function _drawHmWeekdayChart(weekdayAvgs, unit) {{
  var c = _hmInitCanvas('hm-weekday-chart');
  if (!c) return;
  var ctx = c.ctx, W = c.W, H = c.H;
  var muted = getComputedStyle(document.body).getPropertyValue('--muted') || '#999';
  var border = getComputedStyle(document.body).getPropertyValue('--border') || '#e0e0e0';
  var pad = {{top: 10, right: 12, bottom: 28, left: 48}};
  var cW = W - pad.left - pad.right, cH = H - pad.top - pad.bottom;

  var _dtfDay = new Intl.DateTimeFormat(document.documentElement.lang || 'de', {{weekday: 'short'}});
  var days = [];
  for (var d = 0; d < 7; d++) {{
    days.push({{ d: d, val: weekdayAvgs[String(d)] || 0, label: _dtfDay.format(new Date(2001, 0, 1 + d)) }});
  }}
  var maxV = Math.max.apply(null, days.map(function(x){{ return x.val; }})) * 1.15 || 1;
  var barW = Math.max(10, (cW / 7) * 0.6);

  // Grid
  ctx.strokeStyle = border; ctx.lineWidth = 0.5;
  ctx.fillStyle = muted; ctx.font = '10px sans-serif'; ctx.textAlign = 'right';
  for (var i = 0; i <= 4; i++) {{
    var gy = pad.top + cH - (cH * i / 4);
    ctx.beginPath(); ctx.moveTo(pad.left, gy); ctx.lineTo(pad.left + cW, gy); ctx.stroke();
    ctx.fillText(hmFmtVal(maxV * i / 4, unit), pad.left - 4, gy + 3);
  }}

  // Bars (weekday=blue, weekend=orange)
  days.forEach(function(dy, idx) {{
    var x = pad.left + (cW * (idx + 0.5) / 7) - barW / 2;
    var bh = cH * (dy.val / maxV);
    var isWeekend = idx >= 5;
    ctx.fillStyle = isWeekend ? 'rgba(230,126,34,0.8)' : 'rgba(52,152,219,0.8)';
    ctx.fillRect(x, pad.top + cH - bh, barW, bh);
    // Value
    if (dy.val > 0) {{
      ctx.fillStyle = muted; ctx.font = '9px sans-serif'; ctx.textAlign = 'center';
      ctx.fillText(hmFmtVal(dy.val, unit), x + barW / 2, pad.top + cH - bh - 4);
    }}
  }});

  // X-axis + legend
  ctx.fillStyle = muted; ctx.font = '9px sans-serif'; ctx.textAlign = 'center';
  days.forEach(function(dy, idx) {{
    ctx.fillText(dy.label, pad.left + (cW * (idx + 0.5) / 7), pad.top + cH + 16);
  }});
  // Legend
  ctx.fillStyle = 'rgba(52,152,219,0.8)'; ctx.fillRect(W - 100, 4, 10, 10);
  ctx.fillStyle = muted; ctx.textAlign = 'left'; ctx.font = '9px sans-serif';
  ctx.fillText(t('web.hm.weekday', 'Weekday'), W - 86, 13);
  ctx.fillStyle = 'rgba(230,126,34,0.8)'; ctx.fillRect(W - 100, 18, 10, 10);
  ctx.fillStyle = muted; ctx.fillText(t('web.hm.weekend', 'Weekend'), W - 86, 27);
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
  if (!_quietRefresh) el.innerHTML = '<p class="loading-msg">' + t('web.loading', 'Loading…') + '</p>';
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
    // Refresh 6h forecast strip (re-renders in place)
    const fcWrap = document.getElementById('co2-forecast-wrap');
    if (fcWrap && data.forecast !== undefined) {{
      fcWrap.innerHTML = _renderCo2Forecast(data, data.green_threshold || 150, data.dirty_threshold || 400);
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

function _renderCo2Forecast(data, green, dirty) {{
  const fc = (data && data.forecast) || [];
  if (!fc.length) {{
    return '<div class="card" style="padding:10px 14px;font-size:12px;color:var(--muted)">' +
      t('web.co2.forecast_waiting', '6h forecast being computed (trend + Open-Meteo weather) \u2026') +
      '</div>';
  }}
  let html = '<div class="card" style="padding:12px 14px">';
  html += '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">';
  html += '<div style="font-size:12px;font-weight:650;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px">' +
    t('web.co2.forecast_6h', '6h forecast (trend + weather)') + '</div>';
  const upd = data.forecast_updated_ts ? new Date(data.forecast_updated_ts * 1000) : null;
  const updStr = upd ? upd.toLocaleTimeString('de-DE', {{hour:'2-digit',minute:'2-digit'}}) : '';
  html += '<div style="font-size:10px;color:var(--muted)">' + (updStr ? '\u21bb ' + updStr : '') + '</div>';
  html += '</div>';
  html += '<div style="display:grid;grid-template-columns:repeat(' + fc.length + ',1fr);gap:6px">';
  fc.forEach(function(p) {{
    const d = new Date(p.hour_ts * 1000);
    const hh = d.toLocaleTimeString('de-DE', {{hour:'2-digit',minute:'2-digit'}});
    const col = _co2Color(p.intensity_g_per_kwh, green, dirty);
    const delta = p.intensity_g_per_kwh - p.baseline_g_per_kwh;
    const arrow = delta < -3 ? '\u2193' : (delta > 3 ? '\u2191' : '\u2192');
    const wicon = p.cloud_cover_pct < 30 ? '\u2600\ufe0f' : (p.cloud_cover_pct > 70 ? '\u2601\ufe0f' : '\u26c5');
    const windHint = p.wind_ms >= 8 ? ' \U0001F4A8' : '';
    html += '<div style="text-align:center;padding:6px 4px;border-radius:8px;background:var(--bg-alt,rgba(255,255,255,.03));border:1px solid var(--border,rgba(255,255,255,.08))">';
    html += '<div style="font-size:10px;color:var(--muted);margin-bottom:2px">' + hh + '</div>';
    html += '<div style="font-size:18px;font-weight:700;color:' + col + '">' + p.intensity_g_per_kwh.toFixed(0) + '</div>';
    html += '<div style="font-size:10px;color:var(--muted)">g/kWh ' + arrow + '</div>';
    html += '<div style="font-size:11px;margin-top:3px" title="Wolken: ' + p.cloud_cover_pct.toFixed(0) + '% \u00b7 Wind: ' + p.wind_ms.toFixed(1) + ' m/s \u00b7 ' + p.temp_c.toFixed(0) + '\u00b0C">' + wicon + ' ' + p.temp_c.toFixed(0) + '\u00b0' + windHint + '</div>';
    html += '</div>';
  }});
  html += '</div>';
  html += '<div style="font-size:10px;color:var(--muted);margin-top:6px;text-align:center">' +
    t('web.co2.forecast_hint', 'Based on 14-day trend for this hour × weather forecast (wind, sun, temperature)') + '</div>';
  html += '</div>';
  return html;
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

  // ── 6h Forecast strip (trend + weather) ──
  html += '<div id="co2-forecast-wrap" style="margin-top:8px">' + _renderCo2Forecast(data, green, dirty) + '</div>';

  // ── Summary cards ──
  html += '<div class="card" style="margin-top:8px"><div class="metric-grid">';
  html += metricCardHtml(t('web.costs.today', 'Today'), fmt(data.co2_today_kg, 2, 'kg'), 'CO\u2082');
  html += metricCardHtml(t('web.costs.week', 'Week'), fmt(data.co2_week_kg, 2, 'kg'), 'CO\u2082');
  html += metricCardHtml(t('web.costs.month', 'Month'), fmt(data.co2_month_kg, 2, 'kg'), 'CO\u2082');
  html += metricCardHtml(t('web.costs.year', 'Year'), fmt(data.co2_year_kg, 2, 'kg'), 'CO\u2082');
  html += metricCardHtml(t('web.co2.trees', 'Trees (eq.)'), (data.tree_days||0).toFixed(0) + ' ' + t('web.dash.tree_days', 'tree-days'), '🌳');
  html += metricCardHtml(t('web.co2.car', 'Car km avoided'), (data.car_km||0).toFixed(0) + ' km', '🚗');
  html += '</div></div>';

  // ── Trend + Best/Worst + Renewables row ──
  const hourly = data.hourly || [];
  if (hourly.length > 1) {{
    const vals = hourly.map(function(h){{ return h.intensity; }});
    const avgI = vals.reduce(function(s,v){{ return s+v; }},0) / vals.length;
    const minI = Math.min.apply(null, vals);
    const maxI = Math.max.apply(null, vals);
    const minH = hourly[vals.indexOf(minI)];
    const maxH = hourly[vals.indexOf(maxI)];
    // Trend: compare last 6h vs previous 6h
    const recent = vals.slice(-6);
    const prev = vals.slice(-12, -6);
    const recentAvg = recent.length ? recent.reduce(function(s,v){{return s+v;}},0)/recent.length : 0;
    const prevAvg = prev.length ? prev.reduce(function(s,v){{return s+v;}},0)/prev.length : recentAvg;
    const trendPct = prevAvg > 0 ? Math.round((recentAvg - prevAvg) / prevAvg * 100) : 0;
    const trendIcon = trendPct < -5 ? '📉' : trendPct > 5 ? '📈' : '➡️';
    const trendColor = trendPct < -5 ? '#16a34a' : trendPct > 5 ? '#dc2626' : 'var(--muted)';
    // Renewables share from fuel mix
    const mixObj = data.fuel_mix || {{}};
    const renewKeys = ['biomass','solar','wind_onshore','wind_offshore','hydro_run','hydro_reservoir','hydro_pumped','geothermal','marine','other_renewable'];
    let renewMW = 0, totalMW = 0;
    Object.keys(mixObj).forEach(function(k){{ totalMW += (mixObj[k].mw||0); if (renewKeys.indexOf(k)>=0) renewMW += (mixObj[k].mw||0); }});
    const renewPct = totalMW > 0 ? Math.round(renewMW / totalMW * 100) : 0;

    html += '<div class="nilm-two-col" style="margin-top:8px">';
    // Analytics card
    html += '<div class="card"><div style="font-size:12px;font-weight:650;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px">' + t('web.co2.analytics','CO₂ Analyse') + '</div>';
    html += '<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px">';
    html += '<div class="nilm-stat"><span class="nilm-stat-val" style="color:' + _co2Color(avgI,green,dirty) + '">' + avgI.toFixed(0) + '</span><span class="nilm-stat-lbl">' + t('web.co2.avg','Average') + ' g/kWh</span></div>';
    html += '<div class="nilm-stat"><span class="nilm-stat-val" style="color:' + _co2Color(minI,green,dirty) + '">' + minI.toFixed(0) + '</span><span class="nilm-stat-lbl">' + t('web.co2.min','Minimum') + ' (' + esc(minH.hour) + ')</span></div>';
    html += '<div class="nilm-stat"><span class="nilm-stat-val" style="color:' + _co2Color(maxI,green,dirty) + '">' + maxI.toFixed(0) + '</span><span class="nilm-stat-lbl">' + t('web.co2.max','Maximum') + ' (' + esc(maxH.hour) + ')</span></div>';
    html += '</div>';
    html += '<div style="margin-top:10px;display:flex;align-items:center;gap:8px;padding:8px;background:var(--bg);border-radius:8px">';
    html += '<span style="font-size:20px">' + trendIcon + '</span>';
    html += '<div><div style="font-weight:600;font-size:13px;color:' + trendColor + '">' + t('web.co2.trend','Trend') + ': ' + (trendPct > 0 ? '+' : '') + trendPct + '%</div>';
    html += '<div style="font-size:11px;color:var(--muted)">' + t('web.co2.trend_hint','Last 6h vs. previous 6h') + '</div></div>';
    html += '</div></div>';

    // Renewables + score card
    html += '<div class="card"><div style="font-size:12px;font-weight:650;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px">' + t('web.co2.green_score','Green-power score') + '</div>';
    // Score ring
    const scoreColor = renewPct >= 70 ? '#16a34a' : renewPct >= 40 ? '#d97706' : '#dc2626';
    html += '<div style="text-align:center">';
    html += '<canvas id="co2-renew-ring" style="width:120px;height:120px"></canvas>';
    html += '<div style="font-size:24px;font-weight:800;margin-top:-76px;position:relative;color:' + scoreColor + '">' + renewPct + '%</div>';
    html += '<div style="font-size:11px;color:var(--muted);position:relative;margin-top:2px">' + t('web.co2.renewables','Erneuerbare') + '</div>';
    html += '<div style="margin-top:28px"></div>';
    html += '</div>';
    // Current intensity score
    const score = ci <= green ? 'A' : ci <= (green + dirty) / 3 ? 'B' : ci <= (green + dirty) * 2 / 3 ? 'C' : ci <= dirty ? 'D' : 'E';
    const scoreBg = {{'A':'#16a34a','B':'#65a30d','C':'#d97706','D':'#ea580c','E':'#dc2626'}}[score];
    html += '<div style="display:flex;justify-content:center;gap:4px;margin-top:8px">';
    ['A','B','C','D','E'].forEach(function(s) {{
      const bg = s === score ? (scoreBg + '') : 'var(--bg)';
      const fg = s === score ? '#fff' : 'var(--muted)';
      html += '<div style="width:28px;height:28px;border-radius:6px;background:' + bg + ';color:' + fg + ';display:flex;align-items:center;justify-content:center;font-weight:700;font-size:13px">' + s + '</div>';
    }});
    html += '</div>';
    html += '<div style="text-align:center;font-size:11px;color:var(--muted);margin-top:4px">' + t('web.co2.rating','Bewertung') + ': ' + score + '</div>';
    html += '</div></div>';
  }}

  // ── Intensity chart with range selector ──
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
  const mixKeys = Object.keys(mix).sort(function(a,b) {{
    return (mix[b].share_pct || 0) - (mix[a].share_pct || 0);
  }});
  const fuelColors = {{biomass:'#8bc34a',lignite:'#795548',coal_gas:'#9e9e9e',gas:'#ff9800',hard_coal:'#616161',oil:'#212121',oil_shale:'#424242',peat:'#a1887f',geothermal:'#ff5722',hydro_pumped:'#29b6f6',hydro_run:'#0288d1',hydro_reservoir:'#01579b',marine:'#00bcd4',nuclear:'#7c4dff',other_renewable:'#66bb6a',solar:'#fdd835',waste:'#bdbdbd',wind_offshore:'#26c6da',wind_onshore:'#4dd0e1',other:'#e0e0e0'}};
  if (mixKeys.length > 0) {{
    html += '<div class="nilm-two-col" style="margin-top:8px">';
    // Donut chart
    html += '<div class="card">';
    html += '<div style="font-size:12px;font-weight:650;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:6px">' + t('web.co2.fuel_mix', 'Generation Mix') + (data.fuel_mix_hour ? ' (' + esc(data.fuel_mix_hour) + ')' : '') + '</div>';
    html += '<canvas id="co2-fuel-donut" style="width:100%;height:200px"></canvas>';
    html += '<div id="co2-fuel-legend" style="margin-top:6px"></div>';
    html += '</div>';

    // Table
    html += '<div class="card">';
    html += '<div style="font-size:12px;font-weight:650;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:6px">' + t('web.co2.fuel_detail', 'Generation detail') + '</div>';
    // Stacked bar on top
    html += '<div style="display:flex;border-radius:6px;overflow:hidden;height:16px;margin-bottom:8px">';
    mixKeys.forEach(function(k) {{
      const m = mix[k];
      if (m.share_pct > 0.5) {{
        html += '<div style="flex:' + m.share_pct + ';background:' + (fuelColors[k]||'#999') + '" title="' + esc(m.name) + ': ' + m.share_pct.toFixed(1) + '%"></div>';
      }}
    }});
    html += '</div>';
    html += '<div style="max-height:240px;overflow-y:auto">';
    html += '<table style="width:100%;font-size:12px;border-collapse:collapse">';
    html += '<tr style="border-bottom:1px solid var(--border)"><th style="text-align:left;padding:3px">' + t('web.co2.fuel', 'Fuel') + '</th><th style="text-align:right;padding:3px">MW</th><th style="text-align:right;padding:3px">%</th><th style="text-align:right;padding:3px">g/kWh</th></tr>';
    mixKeys.forEach(function(k) {{
      const m = mix[k];
      const barW = Math.max(2, Math.round(m.share_pct));
      html += '<tr style="border-bottom:1px solid var(--border)"><td style="padding:3px;font-size:11px"><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:' + (fuelColors[k]||'#999') + ';margin-right:4px;vertical-align:middle"></span>' + esc(m.name) + '</td><td style="text-align:right;padding:3px">' + m.mw.toFixed(0) + '</td><td style="text-align:right;padding:3px">' + m.share_pct.toFixed(1) + '</td><td style="text-align:right;padding:3px">' + m.factor.toFixed(0) + '</td></tr>';
    }});
    html += '</table></div></div>';
    html += '</div>';
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

  // ── Draw canvases (deferred to ensure layout) ──
  if (hourly.length > 1) {{
    requestAnimationFrame(function() {{
      _drawCo2Chart(hourly, green, dirty, data.forecast || []);
      _drawCo2RenewRing();
      _drawCo2FuelDonut(mixKeys, mix, fuelColors);
    }});
  }}
}}

function _drawCo2FuelDonut(mixKeys, mix, fuelColors) {{
  const canvas = document.getElementById('co2-fuel-donut');
  const legend = document.getElementById('co2-fuel-legend');
  if (!canvas || !mixKeys || !mixKeys.length) return;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width * dpr; canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);
  const W = rect.width, H = rect.height;
  const cx = W/2, cy = H/2, R = Math.min(cx,cy)-8, r = R*0.55;
  const total = mixKeys.reduce(function(s,k){{ return s + (mix[k].mw||0); }}, 0) || 1;
  let angle = -Math.PI/2;
  let legHtml = '<div style="display:flex;flex-wrap:wrap;gap:4px">';
  mixKeys.forEach(function(k) {{
    const m = mix[k];
    if (m.share_pct < 0.5) return;
    const slice = (m.mw / total) * Math.PI * 2;
    const col = fuelColors[k] || '#999';
    ctx.fillStyle = col;
    ctx.beginPath(); ctx.arc(cx,cy,R,angle,angle+slice); ctx.arc(cx,cy,r,angle+slice,angle,true); ctx.closePath(); ctx.fill();
    angle += slice;
    legHtml += '<span style="font-size:10px;display:flex;align-items:center;gap:2px"><span style="width:8px;height:8px;border-radius:50%;background:'+col+';display:inline-block"></span>'+esc(m.name)+' '+m.share_pct.toFixed(1)+'%</span>';
  }});
  ctx.fillStyle = getComputedStyle(document.body).getPropertyValue('--fg')||'#111';
  ctx.font = 'bold 14px sans-serif'; ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
  ctx.fillText(Math.round(total) + ' MW', cx, cy-6);
  ctx.font = '10px sans-serif'; ctx.fillStyle = getComputedStyle(document.body).getPropertyValue('--muted')||'#666';
  ctx.fillText('Total', cx, cy+8);
  legHtml += '</div>';
  if (legend) legend.innerHTML = legHtml;
}}

function _drawCo2RenewRing() {{
  const canvas = document.getElementById('co2-renew-ring');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width * dpr; canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);
  const W = rect.width, H = rect.height;
  const cx = W/2, cy = H/2, R = Math.min(cx,cy)-4, lw = 10;
  // Get renewable percentage from the rendered text
  const valEl = canvas.parentElement && canvas.parentElement.querySelector('div[style*="font-weight:800"]');
  const pct = valEl ? parseInt(valEl.textContent) || 0 : 0;
  const angle = (pct / 100) * Math.PI * 2;
  // Background ring
  ctx.strokeStyle = getComputedStyle(document.body).getPropertyValue('--border') || '#333';
  ctx.lineWidth = lw;
  ctx.beginPath(); ctx.arc(cx, cy, R - lw/2, 0, Math.PI * 2); ctx.stroke();
  // Filled ring
  const col = pct >= 70 ? '#16a34a' : pct >= 40 ? '#d97706' : '#dc2626';
  ctx.strokeStyle = col; ctx.lineCap = 'round';
  ctx.beginPath(); ctx.arc(cx, cy, R - lw/2, -Math.PI/2, -Math.PI/2 + angle); ctx.stroke();
}}

function _drawCo2Chart(hourly, green, dirty, forecast) {{
    forecast = forecast || [];
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

      // Total span = historical hours + forecast hours along the same x-axis
      const nHist = hourly.length;
      const nFc = forecast.length;
      const nTotal = nHist + nFc;
      const denom = Math.max(1, nTotal - 1);

      const vals = hourly.map(function(h) {{ return h.intensity; }});
      const fcVals = forecast.map(function(p) {{ return p.intensity_g_per_kwh; }});
      const allVals = vals.concat(fcVals);
      const maxV = Math.max(dirty * 1.1, (allVals.length ? Math.max.apply(null, allVals) : 0) * 1.1);
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

      // Helper: x position for index across the combined history+forecast axis
      function xAt(i) {{ return pad.left + (cW * i / denom); }}
      function yAt(v) {{ return pad.top + cH - (cH * (v - minV) / (maxV - minV)); }}

      // Area fill (history only)
      ctx.beginPath();
      hourly.forEach(function(h, i) {{
        const x = xAt(i);
        const y = yAt(h.intensity);
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
      }});
      const histEndX = xAt(nHist - 1);
      ctx.lineTo(histEndX, pad.top + cH);
      ctx.lineTo(pad.left, pad.top + cH);
      ctx.closePath();
      ctx.fillStyle = 'rgba(76,175,80,0.12)';
      ctx.fill();

      // Solid history line with gradient color
      hourly.forEach(function(h, i) {{
        if (i === 0) return;
        const x0 = xAt(i - 1);
        const y0 = yAt(hourly[i-1].intensity);
        const x1 = xAt(i);
        const y1 = yAt(h.intensity);
        ctx.strokeStyle = _co2Color((hourly[i-1].intensity + h.intensity) / 2, green, dirty);
        ctx.lineWidth = 2.5;
        ctx.beginPath(); ctx.moveTo(x0, y0); ctx.lineTo(x1, y1); ctx.stroke();
      }});

      // ── Forecast continuation: dashed line + soft fill, hour markers ──
      if (nFc > 0 && nHist > 0) {{
        // Bridge from last historical point to first forecast point
        const lastH = hourly[nHist - 1];
        const xBridge0 = xAt(nHist - 1);
        const yBridge0 = yAt(lastH.intensity);

        // Dashed line through forecast
        ctx.setLineDash([5, 4]);
        ctx.lineWidth = 2;
        let xPrev = xBridge0, yPrev = yBridge0, vPrev = lastH.intensity;
        forecast.forEach(function(p, i) {{
          const xC = xAt(nHist + i);
          const yC = yAt(p.intensity_g_per_kwh);
          ctx.strokeStyle = _co2Color((vPrev + p.intensity_g_per_kwh) / 2, green, dirty);
          ctx.beginPath(); ctx.moveTo(xPrev, yPrev); ctx.lineTo(xC, yC); ctx.stroke();
          xPrev = xC; yPrev = yC; vPrev = p.intensity_g_per_kwh;
        }});
        ctx.setLineDash([]);

        // Forecast area fill
        ctx.beginPath();
        ctx.moveTo(xBridge0, pad.top + cH);
        ctx.lineTo(xBridge0, yBridge0);
        forecast.forEach(function(p, i) {{
          ctx.lineTo(xAt(nHist + i), yAt(p.intensity_g_per_kwh));
        }});
        ctx.lineTo(xAt(nHist + nFc - 1), pad.top + cH);
        ctx.closePath();
        ctx.fillStyle = 'rgba(106,167,255,0.10)';
        ctx.fill();

        // Forecast point markers
        forecast.forEach(function(p, i) {{
          const xC = xAt(nHist + i);
          const yC = yAt(p.intensity_g_per_kwh);
          ctx.fillStyle = _co2Color(p.intensity_g_per_kwh, green, dirty);
          ctx.beginPath(); ctx.arc(xC, yC, 3, 0, Math.PI * 2); ctx.fill();
          ctx.strokeStyle = 'rgba(255,255,255,0.5)';
          ctx.lineWidth = 1;
          ctx.stroke();
        }});

        // Forecast caption above the strip
        ctx.fillStyle = '#6aa7ff';
        ctx.font = '10px sans-serif';
        ctx.textAlign = 'left';
        ctx.fillText(t('web.co2.forecast_label','\u2192 6h forecast'), xBridge0 + 4, pad.top + 10);
      }}

      // "Now" vertical marker — sits between history and forecast when forecast is present
      const nowTs = Date.now() / 1000;
      if (nHist > 1 && hourly[0].ts && hourly[nHist - 1].ts) {{
        const tFirst = hourly[0].ts;
        const tStepCo2 = hourly[1].ts - hourly[0].ts || 3600;
        const tLast = hourly[nHist - 1].ts + tStepCo2;
        let nowX = null;
        if (nFc > 0) {{
          // Anchor "now" at the boundary between history and forecast
          nowX = xAt(nHist - 1) + (xAt(nHist) - xAt(nHist - 1)) * 0.5;
        }} else if (nowTs >= tFirst && nowTs <= tLast) {{
          const fracN = (nowTs - tFirst) / (tLast - tFirst);
          nowX = pad.left + fracN * cW * (nHist - 1) / denom;
        }}
        if (nowX !== null) {{
          ctx.strokeStyle = '#ff1744';
          ctx.lineWidth = 2;
          ctx.setLineDash([4, 3]);
          ctx.beginPath(); ctx.moveTo(nowX, pad.top); ctx.lineTo(nowX, pad.top + cH); ctx.stroke();
          ctx.setLineDash([]);
          ctx.fillStyle = '#ff1744';
          ctx.font = '10px sans-serif';
          ctx.textAlign = 'center';
          ctx.fillText('jetzt', nowX, pad.top - 2);
        }}
      }}

      // X-axis labels (history + key forecast hours)
      ctx.fillStyle = getComputedStyle(document.body).getPropertyValue('--muted') || '#999';
      ctx.font = '10px sans-serif';
      ctx.textAlign = 'center';
      const step = Math.max(1, Math.floor(nHist / 6));
      hourly.forEach(function(h, i) {{
        if (i % step === 0 || i === nHist - 1) {{
          ctx.fillText(h.hour, xAt(i), pad.top + cH + 14);
        }}
      }});
      // Forecast labels: first and last forecast hour
      if (nFc > 0) {{
        ctx.fillStyle = '#6aa7ff';
        const labelHours = [0, nFc - 1];
        labelHours.forEach(function(i) {{
          if (i < 0 || i >= nFc) return;
          const ts = forecast[i].hour_ts;
          const d = new Date(ts * 1000);
          const lbl = d.toLocaleTimeString('de-DE', {{hour:'2-digit',minute:'2-digit'}});
          ctx.fillText('+' + (i + 1) + 'h', xAt(nHist + i), pad.top + cH + 14);
        }});
      }}
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
  if (!_quietRefresh) el.innerHTML = '<p class="loading-msg">' + t('web.loading', 'Loading…') + '</p>';
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
  if (!_quietRefresh) el.innerHTML = '<p class="loading-msg">' + t('web.loading', 'Loading…') + '</p>';
  try {{
    const r = await fetch('/api/weather_correlation');
    if (!r.ok) throw new Error(r.status);
    const d = await r.json();
    renderWeather(d, el);
  }} catch(e) {{
    el.innerHTML = '<p class="error-msg">Error: ' + e.message + '</p>';
  }}
}}

function _wxCard(icon, label, value) {{
  return '<div class="card"><div class="metric-label">' + icon + ' ' + label + '</div><div class="metric-value">' + value + '</div></div>';
}}
function _wxSectionTitle(text) {{
  return '<div style="font-size:12px;font-weight:650;color:var(--muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:.5px">' + text + '</div>';
}}

function renderWeather(d, el) {{
  if (!d.ok) {{
    el.innerHTML = '<p class="info-msg">' + (d.error || t('web.weather.no_data', 'No weather data.')) + '</p>';
    return;
  }}
  var html = '';
  var cur = d.current;

  // Current weather hero
  if (cur) {{
    var desc = (cur.description || '').charAt(0).toUpperCase() + (cur.description || '').slice(1);
    if (desc) {{
      html += '<div class="card" style="text-align:center;margin-bottom:10px">';
      html += '<div style="font-size:28px;font-weight:700">' + (cur.temp_c != null ? cur.temp_c.toFixed(1) + ' \u00b0C' : '\u2013') + '</div>';
      html += '<div style="font-size:13px;color:var(--muted);margin-top:2px">' + esc(desc) + '</div>';
      if (cur.feels_like_c != null) html += '<div style="font-size:11px;color:var(--muted);margin-top:2px">' + t('web.weather.feels_like', 'Feels like') + ' ' + cur.feels_like_c.toFixed(1) + ' \u00b0C</div>';
      html += '</div>';
    }}
    html += '<div class="nilm-metrics">';
    html += _wxCard('\U0001f321\ufe0f', t('web.weather.temp', 'Temperature'), cur.temp_c != null ? cur.temp_c.toFixed(1) + ' \u00b0C' : '\u2013');
    html += _wxCard('\U0001f4a7', t('web.weather.humidity', 'Humidity'), cur.humidity_pct != null ? Math.round(cur.humidity_pct) + ' %' : '\u2013');
    html += _wxCard('\U0001f4a8', t('web.weather.wind', 'Wind'), cur.wind_speed_ms != null ? cur.wind_speed_ms.toFixed(1) + ' m/s' : '\u2013');
    html += _wxCard('\u2601\ufe0f', t('web.weather.clouds', 'Cloud cover'), cur.clouds_pct != null ? Math.round(cur.clouds_pct) + ' %' : '\u2013');
    html += _wxCard('\U0001f9ed', t('web.weather.pressure', 'Pressure'), cur.pressure_hpa ? Math.round(cur.pressure_hpa) + ' hPa' : '\u2013');
    html += _wxCard('\U0001f321\ufe0f', t('web.weather.feels_like', 'Feels like'), cur.feels_like_c != null ? cur.feels_like_c.toFixed(1) + ' \u00b0C' : '\u2013');
    html += '</div>';
  }}

  // Correlation statistics
  if (d.correlation) {{
    var c = d.correlation;
    html += '<div style="margin-top:14px">' + _wxSectionTitle(t('web.weather.correlation', 'Weather-Energy Correlation')) + '</div>';
    var interpIcon = '\u2753', interpText = '', interpColor = 'var(--muted)';
    if (c.r_value != null) {{
      if (c.r_value < -0.4) {{ interpIcon = '\u2744\ufe0f'; interpText = t('web.weather.heating', 'Strong heating correlation'); interpColor = '#3498db'; }}
      else if (c.r_value > 0.4) {{ interpIcon = '\u2600\ufe0f'; interpText = t('web.weather.cooling', 'Strong cooling correlation'); interpColor = '#e67e22'; }}
      else {{ interpIcon = '\u2705'; interpText = t('web.weather.none', 'No significant weather dependency'); interpColor = '#27ae60'; }}
      interpText += ' (r = ' + c.r_value.toFixed(3) + ')';
    }}
    html += '<div class="card" style="margin-bottom:10px;display:flex;align-items:center;gap:10px">';
    html += '<span style="font-size:28px">' + interpIcon + '</span>';
    html += '<div><div style="font-size:14px;font-weight:600;color:' + interpColor + '">' + interpText + '</div>';
    html += '<div style="font-size:11px;color:var(--muted)">' + (c.data_points || 0) + ' ' + t('web.weather.data_points', 'data points') + ', ' + (c.days_covered || 0) + ' ' + t('web.weather.days', 'days') + '</div>';
    html += '</div></div>';

    html += '<div class="nilm-metrics">';
    html += _wxCard('\U0001f4ca', 'Pearson r', c.r_value != null ? c.r_value.toFixed(3) : '\u2013');
    html += _wxCard('\u2744\ufe0f', 'HDD', c.hdd != null ? c.hdd.toFixed(1) : '\u2013');
    html += _wxCard('\u2600\ufe0f', 'CDD', c.cdd != null ? c.cdd.toFixed(1) : '\u2013');
    html += _wxCard('\u26a1', t('web.weather.total_kwh', 'Total kWh'), c.total_kwh != null ? c.total_kwh.toFixed(1) : '\u2013');
    html += _wxCard('\u2744\ufe0f', 'kWh/HDD', c.kwh_per_hdd != null ? c.kwh_per_hdd.toFixed(2) : '\u2013');
    html += _wxCard('\u2600\ufe0f', 'kWh/CDD', c.kwh_per_cdd != null ? c.kwh_per_cdd.toFixed(2) : '\u2013');
    html += '</div>';

    // Temperature zone breakdown
    if (c.avg_kwh_cold != null || c.avg_kwh_mild != null || c.avg_kwh_warm != null) {{
      html += '<div class="card" style="margin-top:10px">' + _wxSectionTitle(t('web.weather.temp_zones', 'Consumption by Temperature Zone')) + '</div>';
      html += '<div class="nilm-metrics" style="margin-top:0">';
      html += _wxCard('\u2744\ufe0f', '< 10\u00b0C (' + t('web.weather.cold', 'Cold') + ')', c.avg_kwh_cold != null ? c.avg_kwh_cold.toFixed(3) + ' kWh/h' : '\u2013');
      html += _wxCard('\U0001f33f', '10\u201320\u00b0C (' + t('web.weather.mild', 'Mild') + ')', c.avg_kwh_mild != null ? c.avg_kwh_mild.toFixed(3) + ' kWh/h' : '\u2013');
      html += _wxCard('\U0001f525', '> 20\u00b0C (' + t('web.weather.warm', 'Warm') + ')', c.avg_kwh_warm != null ? c.avg_kwh_warm.toFixed(3) + ' kWh/h' : '\u2013');
      html += '</div>';
    }}

    // Humidity correlation
    if (d.humidity_corr) {{
      html += '<div class="card" style="margin-top:10px;display:flex;align-items:center;gap:10px">';
      html += '<span style="font-size:22px">\U0001f4a7</span>';
      html += '<div><div style="font-size:12px;font-weight:600">' + t('web.weather.humidity_corr', 'Humidity Correlation') + '</div>';
      html += '<div style="font-size:11px;color:var(--muted)">Pearson r = ' + d.humidity_corr.r_value.toFixed(3) + '</div></div></div>';
    }}
  }}

  // Comfort zone chart
  if (d.comfort_zones && d.comfort_zones.length > 0) {{
    html += '<div class="card" style="margin-top:10px">' + _wxSectionTitle(t('web.weather.comfort', 'Comfort Zone Analysis')) + '';
    html += '<canvas id="wx-comfort-chart" style="width:100%;height:180px"></canvas></div>';
  }}

  // Charts side by side
  if (d.paired && d.paired.length >= 3) {{
    html += '<div class="nilm-two-col" style="margin-top:10px">';
    html += '<div class="card">' + _wxSectionTitle(t('web.weather.scatter', 'Temperature vs. Consumption')) + '';
    html += '<canvas id="weather-scatter" style="width:100%;height:240px"></canvas>';
    html += '<div style="display:flex;gap:8px;justify-content:center;margin-top:6px;font-size:10px;color:var(--muted)">';
    html += '<span>\U0001f319 0\u20136h</span><span>\U0001f305 6\u201312h</span><span>\u2600\ufe0f 12\u201318h</span><span>\U0001f306 18\u201324h</span></div></div>';
    html += '<div class="card">' + _wxSectionTitle(t('web.weather.timeline', 'Hourly Timeline')) + '';
    html += '<canvas id="weather-timeline" style="width:100%;height:240px"></canvas></div>';
    html += '</div>';
  }}

  // Daily temperature range + consumption
  if (d.daily && d.daily.length >= 2) {{
    html += '<div class="card" style="margin-top:10px">' + _wxSectionTitle(t('web.weather.daily_range', 'Daily Temperature Range & Consumption')) + '';
    html += '<canvas id="wx-daily-chart" style="width:100%;height:220px"></canvas></div>';
  }}

  // Best / Worst energy days
  if (d.daily && d.daily.length >= 3) {{
    var sortedDays = d.daily.slice().sort(function(a,b){{ return a.kwh - b.kwh; }});
    var best3 = sortedDays.slice(0, 3);
    var worst3 = sortedDays.slice(-3).reverse();
    html += '<div class="nilm-two-col" style="margin-top:10px">';
    html += '<div class="card">' + _wxSectionTitle(t('web.weather.best_days', 'Best Energy Days'));
    best3.forEach(function(dy, i) {{
      var medal = ['\U0001f947','\U0001f948','\U0001f949'][i] || '';
      html += '<div style="display:flex;justify-content:space-between;align-items:center;padding:4px 0;border-bottom:1px solid var(--border);font-size:12px">';
      html += '<span>' + medal + ' ' + dy.date + '</span>';
      html += '<span style="display:flex;gap:12px"><span style="color:#3498db;font-weight:600">' + dy.kwh.toFixed(2) + ' kWh</span>';
      html += '<span style="color:#e74c3c">' + dy.temp_min.toFixed(0) + '\u2013' + dy.temp_max.toFixed(0) + '\u00b0C</span></span></div>';
    }});
    html += '</div>';
    html += '<div class="card">' + _wxSectionTitle(t('web.weather.worst_days', 'Worst Energy Days'));
    worst3.forEach(function(dy) {{
      html += '<div style="display:flex;justify-content:space-between;align-items:center;padding:4px 0;border-bottom:1px solid var(--border);font-size:12px">';
      html += '<span>' + dy.date + '</span>';
      html += '<span style="display:flex;gap:12px"><span style="color:#e74c3c;font-weight:600">' + dy.kwh.toFixed(2) + ' kWh</span>';
      html += '<span style="color:#e74c3c">' + dy.temp_min.toFixed(0) + '\u2013' + dy.temp_max.toFixed(0) + '\u00b0C</span></span></div>';
    }});
    html += '</div></div>';
  }}

  el.innerHTML = html;

  if (d.paired && d.paired.length >= 3) setTimeout(function() {{ _drawWeatherCharts(d); }}, 50);
  if (d.comfort_zones && d.comfort_zones.length > 0) setTimeout(function() {{ _drawWxComfortChart(d.comfort_zones); }}, 60);
  if (d.daily && d.daily.length >= 2) setTimeout(function() {{ _drawWxDailyChart(d.daily); }}, 70);
}}

function _weatherHourColor(h) {{
  var hue = (240 + h * 15) % 360;
  var sat = 70;
  var lgt = (h >= 6 && h <= 20) ? 55 : 35;
  return 'hsl(' + hue + ',' + sat + '%,' + lgt + '%)';
}}
function _wxTempColor(t) {{
  if (t < 0) return '#2980b9';
  if (t < 10) return '#3498db';
  if (t < 20) return '#27ae60';
  if (t < 30) return '#e67e22';
  return '#e74c3c';
}}
function _wxInitCanvas(id) {{
  var el = document.getElementById(id);
  if (!el) return null;
  var dpr = window.devicePixelRatio || 1;
  var rect = el.getBoundingClientRect();
  el.width = rect.width * dpr; el.height = rect.height * dpr;
  var ctx = el.getContext('2d'); ctx.scale(dpr, dpr);
  return {{ctx: ctx, W: rect.width, H: rect.height}};
}}

function _drawWxComfortChart(zones) {{
  var c = _wxInitCanvas('wx-comfort-chart');
  if (!c) return;
  var ctx = c.ctx, W = c.W, H = c.H;
  var muted = getComputedStyle(document.body).getPropertyValue('--muted') || '#999';
  var pad = {{top: 8, right: 16, bottom: 20, left: 60}};
  var cW = W - pad.left - pad.right;
  var cH = H - pad.top - pad.bottom;
  var n = zones.length;
  var barH = Math.min(22, (cH / n) - 4);
  var maxKwh = Math.max.apply(null, zones.map(function(z){{ return z.avg_kwh; }})) * 1.15 || 1;
  var colors = ['#2980b9','#3498db','#27ae60','#2ecc71','#f1c40f','#e67e22','#e74c3c','#c0392b'];
  zones.forEach(function(z, i) {{
    var y = pad.top + (cH * i / n) + (cH / n - barH) / 2;
    var bw = cW * (z.avg_kwh / maxKwh);
    ctx.fillStyle = colors[i % colors.length]; ctx.globalAlpha = 0.8;
    ctx.fillRect(pad.left, y, bw, barH); ctx.globalAlpha = 1;
    ctx.fillStyle = muted; ctx.font = '10px sans-serif'; ctx.textAlign = 'right';
    ctx.fillText(z.range + '\u00b0C', pad.left - 4, y + barH / 2 + 3);
    ctx.fillStyle = colors[i % colors.length]; ctx.textAlign = 'left'; ctx.font = '10px sans-serif';
    ctx.fillText(z.avg_kwh.toFixed(3) + ' kWh/h (' + z.count + ')', pad.left + bw + 4, y + barH / 2 + 3);
  }});
}}

function _drawWxDailyChart(daily) {{
  var c = _wxInitCanvas('wx-daily-chart');
  if (!c) return;
  var ctx = c.ctx, W = c.W, H = c.H;
  var muted = getComputedStyle(document.body).getPropertyValue('--muted') || '#999';
  var border = getComputedStyle(document.body).getPropertyValue('--border') || '#e0e0e0';
  var pad = {{top: 12, right: 48, bottom: 28, left: 48}};
  var cW = W - pad.left - pad.right, cH = H - pad.top - pad.bottom;
  var n = daily.length;
  var allTemps = []; daily.forEach(function(d) {{ allTemps.push(d.temp_min); allTemps.push(d.temp_max); }});
  var minT = Math.min.apply(null, allTemps) - 2, maxT = Math.max.apply(null, allTemps) + 2;
  var maxK = Math.max.apply(null, daily.map(function(d){{ return d.kwh; }})) * 1.15 || 1;
  var barW = Math.max(2, (cW / n) - 2);

  ctx.strokeStyle = border; ctx.lineWidth = 0.5;
  for (var i = 0; i <= 4; i++) {{ var gy = pad.top + cH - (cH * i / 4); ctx.beginPath(); ctx.moveTo(pad.left, gy); ctx.lineTo(pad.left + cW, gy); ctx.stroke(); }}
  ctx.fillStyle = '#3498db'; ctx.font = '10px sans-serif'; ctx.textAlign = 'right';
  for (var i = 0; i <= 4; i++) {{ var gy = pad.top + cH - (cH * i / 4); ctx.fillText((maxK * i / 4).toFixed(1), pad.left - 4, gy + 3); }}
  ctx.fillStyle = '#e74c3c'; ctx.textAlign = 'left';
  for (var i = 0; i <= 4; i++) {{ var gy = pad.top + cH - (cH * i / 4); ctx.fillText((minT + (maxT - minT) * i / 4).toFixed(0) + '\u00b0', pad.left + cW + 4, gy + 3); }}

  ctx.fillStyle = 'rgba(52,152,219,0.5)';
  daily.forEach(function(d, i) {{ var x = pad.left + (cW * i / n) + 1; var bh = cH * (d.kwh / maxK); ctx.fillRect(x, pad.top + cH - bh, barW, bh); }});

  daily.forEach(function(d, i) {{
    var x = pad.left + (cW * (i + 0.5) / n);
    var yMin = pad.top + cH - cH * (d.temp_min - minT) / (maxT - minT);
    var yMax = pad.top + cH - cH * (d.temp_max - minT) / (maxT - minT);
    ctx.strokeStyle = _wxTempColor(d.temp_avg); ctx.lineWidth = 3;
    ctx.beginPath(); ctx.moveTo(x, yMin); ctx.lineTo(x, yMax); ctx.stroke();
    var yAvg = pad.top + cH - cH * (d.temp_avg - minT) / (maxT - minT);
    ctx.beginPath(); ctx.arc(x, yAvg, 3, 0, Math.PI * 2);
    ctx.fillStyle = _wxTempColor(d.temp_avg); ctx.fill();
  }});

  ctx.fillStyle = muted; ctx.font = '9px sans-serif'; ctx.textAlign = 'center';
  var step = Math.max(1, Math.floor(n / (W < 400 ? 4 : 8)));
  for (var i = 0; i < n; i += step) {{ ctx.fillText(daily[i].date.slice(5), pad.left + (cW * (i + 0.5) / n), pad.top + cH + 16); }}
  ctx.fillStyle = '#3498db'; ctx.font = '10px sans-serif';
  ctx.save(); ctx.translate(12, pad.top + cH / 2); ctx.rotate(-Math.PI / 2); ctx.textAlign = 'center'; ctx.fillText('kWh', 0, 0); ctx.restore();
  ctx.fillStyle = '#e74c3c';
  ctx.save(); ctx.translate(W - 6, pad.top + cH / 2); ctx.rotate(Math.PI / 2); ctx.textAlign = 'center'; ctx.fillText('\u00b0C', 0, 0); ctx.restore();
}}

function _drawWeatherCharts(d) {{
  var pts = d.paired;
  var muted = getComputedStyle(document.body).getPropertyValue('--muted') || '#999';
  var border = getComputedStyle(document.body).getPropertyValue('--border') || '#e0e0e0';

  var sc = _wxInitCanvas('weather-scatter');
  if (sc) {{
    var ctx = sc.ctx, W = sc.W, H = sc.H;
    var pad = {{top: 12, right: 16, bottom: 28, left: 48}};
    var cW = W - pad.left - pad.right, cH = H - pad.top - pad.bottom;
    var temps = pts.map(function(p) {{ return p.temp; }});
    var kwhs = pts.map(function(p) {{ return p.kwh; }});
    var minT = Math.min.apply(null, temps) - 1, maxT = Math.max.apply(null, temps) + 1;
    var minK = 0, maxK = Math.max.apply(null, kwhs) * 1.1 || 1;

    ctx.strokeStyle = border; ctx.lineWidth = 0.5;
    ctx.fillStyle = muted; ctx.font = '10px sans-serif'; ctx.textAlign = 'right';
    for (var i = 0; i <= 4; i++) {{ var gy = pad.top + cH - (cH * i / 4); ctx.beginPath(); ctx.moveTo(pad.left, gy); ctx.lineTo(pad.left + cW, gy); ctx.stroke(); ctx.fillText((minK + (maxK - minK) * i / 4).toFixed(2), pad.left - 4, gy + 3); }}
    ctx.textAlign = 'center';
    for (var j = 0; j <= 4; j++) {{ var gx = pad.left + (cW * j / 4); ctx.fillText((minT + (maxT - minT) * j / 4).toFixed(0) + '\u00b0', gx, pad.top + cH + 16); }}
    ctx.fillStyle = muted; ctx.font = '10px sans-serif';
    ctx.save(); ctx.translate(12, pad.top + cH / 2); ctx.rotate(-Math.PI / 2); ctx.textAlign = 'center'; ctx.fillText('kWh', 0, 0); ctx.restore();
    ctx.textAlign = 'center'; ctx.fillText('\u00b0C', pad.left + cW / 2, H - 2);

    pts.forEach(function(p) {{
      var x = pad.left + cW * (p.temp - minT) / (maxT - minT);
      var y = pad.top + cH - cH * (p.kwh - minK) / (maxK - minK);
      ctx.beginPath(); ctx.arc(x, y, 3.5, 0, Math.PI * 2);
      ctx.fillStyle = _weatherHourColor(p.hour_of_day || 0);
      ctx.globalAlpha = 0.7; ctx.fill();
    }});
    ctx.globalAlpha = 1.0;

    if (d.correlation && d.correlation.slope != null) {{
      var sl = d.correlation.slope, ic = d.correlation.intercept;
      var y1 = pad.top + cH - cH * ((sl * minT + ic) - minK) / (maxK - minK);
      var y2 = pad.top + cH - cH * ((sl * maxT + ic) - minK) / (maxK - minK);
      ctx.setLineDash([6, 3]); ctx.strokeStyle = '#e74c3c'; ctx.lineWidth = 1.5; ctx.globalAlpha = 0.7;
      ctx.beginPath(); ctx.moveTo(pad.left, y1); ctx.lineTo(pad.left + cW, y2); ctx.stroke();
      ctx.setLineDash([]); ctx.globalAlpha = 1.0;
    }}
  }}

  var tc = _wxInitCanvas('weather-timeline');
  if (tc) {{
    var sorted = pts.slice().sort(function(a, b) {{ return a.ts - b.ts; }});
    if (sorted.length > 72) sorted = sorted.slice(sorted.length - 72);
    var ctx2 = tc.ctx, W2 = tc.W, H2 = tc.H;
    var pad2 = {{top: 12, right: 48, bottom: 28, left: 48}};
    var cW2 = W2 - pad2.left - pad2.right, cH2 = H2 - pad2.top - pad2.bottom;
    var kwhs2 = sorted.map(function(p) {{ return p.kwh; }});
    var temps2 = sorted.map(function(p) {{ return p.temp; }});
    var maxK2 = Math.max.apply(null, kwhs2) * 1.15 || 1;
    var minT2 = Math.min.apply(null, temps2) - 1, maxT2 = Math.max.apply(null, temps2) + 1;
    var n = sorted.length, barW = Math.max(1, (cW2 / n) - 1);

    ctx2.strokeStyle = border; ctx2.lineWidth = 0.5;
    ctx2.fillStyle = '#3498db'; ctx2.font = '10px sans-serif'; ctx2.textAlign = 'right';
    for (var gi = 0; gi <= 4; gi++) {{ var gy2 = pad2.top + cH2 - (cH2 * gi / 4); ctx2.beginPath(); ctx2.moveTo(pad2.left, gy2); ctx2.lineTo(pad2.left + cW2, gy2); ctx2.stroke(); ctx2.fillText((maxK2 * gi / 4).toFixed(2), pad2.left - 4, gy2 + 3); }}
    ctx2.fillStyle = '#e74c3c'; ctx2.textAlign = 'left';
    for (var gj = 0; gj <= 4; gj++) {{ var gy3 = pad2.top + cH2 - (cH2 * gj / 4); ctx2.fillText((minT2 + (maxT2 - minT2) * gj / 4).toFixed(0) + '\u00b0', pad2.left + cW2 + 4, gy3 + 3); }}
    ctx2.fillStyle = 'rgba(52,152,219,0.6)';
    sorted.forEach(function(p, i) {{ var x = pad2.left + (cW2 * i / n) + 1; var bh = cH2 * (p.kwh / maxK2); ctx2.fillRect(x, pad2.top + cH2 - bh, barW, bh); }});
    ctx2.strokeStyle = '#e74c3c'; ctx2.lineWidth = 2; ctx2.globalAlpha = 0.85;
    ctx2.beginPath();
    sorted.forEach(function(p, i) {{ var x = pad2.left + (cW2 * (i + 0.5) / n); var y = pad2.top + cH2 - cH2 * (p.temp - minT2) / (maxT2 - minT2); if (i === 0) ctx2.moveTo(x, y); else ctx2.lineTo(x, y); }});
    ctx2.stroke(); ctx2.globalAlpha = 1.0;

    ctx2.fillStyle = muted; ctx2.font = '9px sans-serif'; ctx2.textAlign = 'center';
    var maxLabels = W2 < 400 ? 4 : (W2 < 600 ? 5 : 8);
    var step = Math.max(1, Math.floor(n / maxLabels));
    for (var li = 0; li < n; li += step) {{ var dt = new Date(sorted[li].ts * 1000); var lbl = ('0'+dt.getDate()).slice(-2) + '.' + ('0'+(dt.getMonth()+1)).slice(-2); if (W2 >= 400) lbl += ' ' + ('0'+dt.getHours()).slice(-2) + 'h'; ctx2.fillText(lbl, pad2.left + (cW2 * (li + 0.5) / n), pad2.top + cH2 + 16); }}
    ctx2.fillStyle = '#3498db'; ctx2.font = '10px sans-serif';
    ctx2.save(); ctx2.translate(12, pad2.top + cH2 / 2); ctx2.rotate(-Math.PI / 2); ctx2.textAlign = 'center'; ctx2.fillText('kWh', 0, 0); ctx2.restore();
    ctx2.fillStyle = '#e74c3c';
    ctx2.save(); ctx2.translate(W2 - 6, pad2.top + cH2 / 2); ctx2.rotate(Math.PI / 2); ctx2.textAlign = 'center'; ctx2.fillText('\u00b0C', 0, 0); ctx2.restore();
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
    '<input type="checkbox" id="cmp-spot" onchange="loadCompare()"> \u26a1 ' + t('compare.vs_dynamic', 'vs. dynamic tariff') + '</label>';
  qhtml += '</div>';
  quick.innerHTML = qhtml;
}}

async function loadComparePreset(preset) {{
  const result = document.getElementById('cmp-result');
  if (!_quietRefresh) result.innerHTML = '<p class="loading-msg">' + t('web.loading', 'Loading…') + '</p>';
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
  if (!_quietRefresh) result.innerHTML = '<p class="loading-msg">' + t('web.loading', 'Loading…') + '</p>';
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
  if (!data || !data.ok) {{ el.innerHTML = '<p class="info-msg">' + t('web.no_data', 'No data.') + '</p>'; return; }}
  const unit = data.unit || 'kWh';
  const ta = data.total_a || 0;
  const tb = data.total_b || 0;
  const delta = data.delta || (ta - tb);
  const pct = data.delta_pct || (tb !== 0 ? ((delta / Math.abs(tb)) * 100) : 0);
  const lA = data.label_a || data.name_a || data.device_a || 'A';
  const lB = data.label_b || data.name_b || data.device_b || 'B';
  const deltaColor = delta > 0 ? '#dc2626' : delta < 0 ? '#16a34a' : 'var(--muted)';
  const deltaIcon = delta > 0 ? '📈' : delta < 0 ? '📉' : '➡️';

  let html = '';

  /* ── Summary metrics ── */
  html += '<div class="nilm-metrics" style="margin-bottom:12px">';
  html += _nilmMetricCard('🔵', esc(data.name_a || data.device_a || 'A'), fmt(ta,1,unit), (data.days_a||0) + ' ' + t('web.cmp.days','Days'));
  html += _nilmMetricCard('🟠', esc(data.name_b || data.device_b || 'B'), fmt(tb,1,unit), (data.days_b||0) + ' ' + t('web.cmp.days','Days'));
  html += _nilmMetricCard(deltaIcon, t('web.cmp.delta','Differenz'), '<span style="color:'+deltaColor+'">' + (delta>=0?'+':'') + fmt(delta,1,unit) + '</span>', (pct>=0?'+':'') + fmt(pct,1) + '%');
  const avgA = data.avg_a || 0, avgB = data.avg_b || 0;
  const avgDelta = avgA - avgB;
  const avgCol = avgDelta > 0 ? '#dc2626' : avgDelta < 0 ? '#16a34a' : 'var(--muted)';
  html += _nilmMetricCard('📊', t('web.cmp.daily_avg','Daily average'), fmt(avgA,2) + ' / ' + fmt(avgB,2), '<span style="color:'+avgCol+'">'+t('web.cmp.delta','Diff')+': '+(avgDelta>=0?'+':'')+fmt(avgDelta,2)+' '+unit+'</span>');
  html += '</div>';

  /* ── Visual comparison (A vs B bars) ── */
  html += '<div class="nilm-two-col" style="margin-bottom:12px">';
  // Side-by-side total comparison
  html += '<div class="card"><div style="font-size:14px;font-weight:700;margin-bottom:8px">' + t('web.cmp.total','Total comparison') + '</div>';
  const maxTotal = Math.max(ta, tb) || 1;
  html += '<div style="margin-bottom:12px">';
  html += '<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">';
  html += '<span style="font-size:11px;color:#3b82f6;min-width:20px">A</span>';
  html += '<div style="flex:1;height:24px;background:var(--bg);border-radius:6px;overflow:hidden"><div style="width:' + Math.round(ta/maxTotal*100) + '%;height:100%;background:#3b82f6;border-radius:6px;display:flex;align-items:center;padding-left:6px"><span style="font-size:10px;color:#fff;font-weight:700">' + fmt(ta,1) + ' ' + unit + '</span></div></div></div>';
  html += '<div style="display:flex;align-items:center;gap:8px">';
  html += '<span style="font-size:11px;color:#f59e0b;min-width:20px">B</span>';
  html += '<div style="flex:1;height:24px;background:var(--bg);border-radius:6px;overflow:hidden"><div style="width:' + Math.round(tb/maxTotal*100) + '%;height:100%;background:#f59e0b;border-radius:6px;display:flex;align-items:center;padding-left:6px"><span style="font-size:10px;color:#fff;font-weight:700">' + fmt(tb,1) + ' ' + unit + '</span></div></div></div>';
  html += '</div>';
  // Delta indicator
  html += '<div style="text-align:center;margin-top:12px;padding:10px;background:var(--bg);border-radius:8px">';
  html += '<div style="font-size:28px">' + deltaIcon + '</div>';
  html += '<div style="font-size:20px;font-weight:800;color:' + deltaColor + '">' + (pct>=0?'+':'') + fmt(pct,1) + '%</div>';
  html += '<div style="font-size:11px;color:var(--muted)">' + (delta>=0?'+':'') + fmt(delta,1) + ' ' + unit + '</div>';
  html += '</div></div>';

  // Daily average comparison
  html += '<div class="card"><div style="font-size:14px;font-weight:700;margin-bottom:8px">' + t('web.cmp.avg_compare','Daily average comparison') + '</div>';
  html += '<canvas id="cmp-avg-chart" style="width:100%;height:180px"></canvas></div>';
  html += '</div>';

  /* ── Time series chart ── */
  html += '<div class="card" style="margin-bottom:12px">';
  html += '<div style="display:flex;align-items:center;gap:10px;margin-bottom:8px">';
  html += '<div style="font-size:14px;font-weight:700">' + t('web.cmp.timeline','Timeline') + '</div>';
  html += '<span style="font-size:11px;display:flex;align-items:center;gap:3px"><span style="width:12px;height:4px;background:#3b82f6;border-radius:2px;display:inline-block"></span>' + esc(lA) + '</span>';
  html += '<span style="font-size:11px;display:flex;align-items:center;gap:3px"><span style="width:12px;height:4px;background:#f59e0b;border-radius:2px;display:inline-block"></span>' + esc(lB) + '</span>';
  html += '</div>';
  html += '<canvas class="bar-chart" id="cmp-canvas"></canvas></div>';

  /* ── Cumulative chart ── */
  if (data.values_a && data.values_b && data.values_a.length > 1) {{
    html += '<div class="card" style="margin-bottom:12px"><div style="font-size:14px;font-weight:700;margin-bottom:8px">' + t('web.cmp.cumulative','Kumuliert') + '</div>';
    html += '<canvas id="cmp-cumul-chart" style="width:100%;height:180px"></canvas></div>';
  }}

  el.innerHTML = html;

  /* ── Draw charts ── */
  requestAnimationFrame(function() {{
    // Main comparison bars
    const canvas = document.getElementById('cmp-canvas');
    if (data.values_a && data.values_b && data.labels && data.labels.length > 1) {{
      drawBars(canvas, data.labels, [
        {{ values: data.values_a, color: 'rgba(59,130,246,0.75)', label: lA }},
        {{ values: data.values_b, color: 'rgba(245,158,11,0.75)', label: lB }}
      ], {{ unit: unit }});
    }} else {{
      drawBars(canvas, [lA, lB], [
        {{ values: [ta, 0], color: 'rgba(59,130,246,0.75)', label: lA }},
        {{ values: [0, tb], color: 'rgba(245,158,11,0.75)', label: lB }}
      ], {{ unit: unit }});
    }}
    // Average comparison
    _drawBarChart('cmp-avg-chart', [t('web.cmp.a','A') + ' (' + fmt(avgA,2) + ')', t('web.cmp.b','B') + ' (' + fmt(avgB,2) + ')'], [avgA, avgB], {{colors: ['#3b82f6','#f59e0b'], decimals: 2}});
    // Cumulative line chart
    if (data.values_a && data.values_b && data.values_a.length > 1) {{
      _drawCmpCumulative(data);
    }}
  }});
}}

function _drawCmpCumulative(data) {{
  const canvas = document.getElementById('cmp-cumul-chart');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio||1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width*dpr; canvas.height = rect.height*dpr;
  ctx.scale(dpr,dpr);
  const W = rect.width, H = rect.height;
  const pad = {{top:8,right:8,bottom:20,left:44}};
  const cW = W-pad.left-pad.right, cH = H-pad.top-pad.bottom;

  // Build cumulative
  const cumA = [], cumB = [];
  let sa = 0, sb = 0;
  for (let i = 0; i < data.values_a.length; i++) {{
    sa += data.values_a[i]||0;
    sb += (data.values_b[i]||0);
    cumA.push(sa); cumB.push(sb);
  }}
  const maxV = Math.max(sa, sb, 0.1) * 1.1;
  const n = cumA.length;
  const fg = getComputedStyle(document.body).getPropertyValue('--muted')||'#999';
  const border = getComputedStyle(document.body).getPropertyValue('--border')||'#e0e0e0';

  // Grid
  ctx.strokeStyle = border; ctx.lineWidth = 0.5;
  for (let i=0;i<=4;i++) {{
    const y = pad.top+cH-(cH*i/4);
    ctx.beginPath(); ctx.moveTo(pad.left,y); ctx.lineTo(pad.left+cW,y); ctx.stroke();
    ctx.fillStyle = fg; ctx.font = '9px sans-serif'; ctx.textAlign = 'right';
    ctx.fillText((maxV*i/4).toFixed(0), pad.left-4, y+3);
  }}

  // Line A
  ctx.strokeStyle = '#3b82f6'; ctx.lineWidth = 2.5;
  ctx.beginPath();
  cumA.forEach(function(v,i){{
    const x = pad.left+(i/(n-1||1))*cW;
    const y = pad.top+cH-(v/maxV)*cH;
    if(i===0)ctx.moveTo(x,y);else ctx.lineTo(x,y);
  }});
  ctx.stroke();

  // Line B
  ctx.strokeStyle = '#f59e0b'; ctx.lineWidth = 2.5;
  ctx.beginPath();
  cumB.forEach(function(v,i){{
    const x = pad.left+(i/(n-1||1))*cW;
    const y = pad.top+cH-(v/maxV)*cH;
    if(i===0)ctx.moveTo(x,y);else ctx.lineTo(x,y);
  }});
  ctx.stroke();

  // End labels
  ctx.fillStyle = '#3b82f6'; ctx.font = 'bold 10px sans-serif'; ctx.textAlign = 'right';
  ctx.fillText(fmt(sa,0) + ' ' + (data.unit||''), pad.left+cW, pad.top+cH-(sa/maxV)*cH-4);
  ctx.fillStyle = '#f59e0b';
  ctx.fillText(fmt(sb,0) + ' ' + (data.unit||''), pad.left+cW, pad.top+cH-(sb/maxV)*cH+12);
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
  if (!_quietRefresh) el.innerHTML = '<p class="loading-msg">' + t('web.loading', 'Loading…') + '</p>';
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
  const enabled = data.enabled !== false;
  const events = data.events || [];
  const tc = data.type_counts || {{}};
  const dc = data.device_counts || {{}};
  const typeIcons = {{unusual_daily:'📊', night_consumption:'🌙', power_peak_time:'⚡'}};
  const typeColors = {{unusual_daily:'#3b82f6', night_consumption:'#8b5cf6', power_peak_time:'#f59e0b'}};
  const typeLabels = {{unusual_daily: t('anomaly.type.unusual_daily','Unusual daily consumption'), night_consumption: t('anomaly.type.night_consumption','Elevated night consumption'), power_peak_time: t('anomaly.type.power_peak_time','Peak at unusual time')}};

  /* ── Status badges ── */
  html += '<div style="display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:12px">';
  html += '<span class="badge ' + (enabled ? 'badge-green' : 'badge-red') + '">' +
    (enabled ? t('web.dash.anomaly_enabled', 'Enabled') : t('web.dash.anomaly_disabled', 'Disabled')) + '</span>';
  if (data.model) html += '<span class="badge badge-yellow">' + esc(data.model) + '</span>';
  if (data.sigma_threshold) html += '<span class="badge badge-yellow">\u03c3 \u2265 ' + data.sigma_threshold + '</span>';
  html += '<span class="badge badge-yellow">' + (data.total_count||0) + ' Events</span>';
  html += '</div>';

  if (events.length === 0) {{
    html += '<div class="card" style="text-align:center;padding:30px">';
    html += '<div style="font-size:40px;margin-bottom:10px">✅</div>';
    html += '<div style="font-size:16px;font-weight:600;margin-bottom:6px">' + t('web.dash.no_anomalies_title','No anomalies detected') + '</div>';
    html += '<div style="color:var(--muted);font-size:13px">' + t('web.dash.no_anomalies', 'All devices behave normally.') + '</div></div>';
    el.innerHTML = html;
    return;
  }}

  /* ── Overview metrics ── */
  html += '<div class="nilm-metrics" style="margin-bottom:12px">';
  html += _nilmMetricCard('🔍', t('web.anom.total','Total anomalies'), data.total_count || events.length, t('web.anom.last_days','Last 7 days'));
  html += _nilmMetricCard('📊', t('web.anom.types','Typen erkannt'), Object.keys(tc).length, Object.keys(tc).map(function(k){{ return (typeIcons[k]||'') + ' ' + (tc[k]||0); }}).join('  '));
  html += _nilmMetricCard('📡', t('web.anom.devices','Devices affected'), Object.keys(dc).length, t('web.anom.of_total','of all devices'));
  html += _nilmMetricCard('⚠️', t('web.anom.max_sigma','Max deviation'), (data.max_sigma||0) + '\u03c3', t('web.anom.avg','Avg') + ': ' + (data.avg_sigma||0) + '\u03c3');
  html += '</div>';

  /* ── Type breakdown + Device breakdown (side by side) ── */
  html += '<div class="nilm-two-col" style="margin-bottom:12px">';

  // Type donut
  html += '<div class="card"><div style="font-size:14px;font-weight:700;margin-bottom:8px">' + t('web.anom.by_type','By type') + '</div>';
  html += '<canvas id="anom-type-donut" style="width:100%;height:180px"></canvas>';
  html += '<div id="anom-type-legend" style="margin-top:6px"></div></div>';

  // Device breakdown
  html += '<div class="card"><div style="font-size:14px;font-weight:700;margin-bottom:8px">' + t('web.anom.by_device','By device') + '</div>';
  const devKeys = Object.keys(dc).sort(function(a,b){{ return dc[b].count - dc[a].count; }});
  if (devKeys.length === 0) {{
    html += '<p style="color:var(--muted);font-size:13px">—</p>';
  }} else {{
    const maxDc = Math.max.apply(null, devKeys.map(function(k){{ return dc[k].count; }})) || 1;
    devKeys.forEach(function(dk) {{
      const d = dc[dk];
      const pct = Math.round(d.count / maxDc * 100);
      html += '<div style="margin-bottom:8px">';
      html += '<div style="display:flex;justify-content:space-between;font-size:13px;margin-bottom:2px"><span style="font-weight:600">' + esc(d.name) + '</span><span style="color:var(--muted)">' + d.count + ' Events</span></div>';
      html += '<div style="background:var(--bg);border-radius:4px;height:8px;overflow:hidden">';
      html += '<div style="width:' + pct + '%;height:100%;background:var(--accent);border-radius:4px;transition:width .3s"></div>';
      html += '</div>';
      // Type chips
      html += '<div style="display:flex;gap:3px;margin-top:3px;flex-wrap:wrap">';
      Object.keys(d.types||{{}}).forEach(function(t2) {{
        html += '<span style="font-size:10px;background:' + (typeColors[t2]||'var(--chipbg)') + '22;color:' + (typeColors[t2]||'var(--fg)') + ';padding:1px 6px;border-radius:6px">' + (typeIcons[t2]||'') + ' ' + d.types[t2] + '</span>';
      }});
      html += '</div></div>';
    }});
  }}
  html += '</div></div>';

  /* ── Sigma distribution chart ── */
  html += '<div class="card" style="margin-bottom:12px"><div style="font-size:14px;font-weight:700;margin-bottom:8px">' + t('web.anom.sigma_dist','Sigma distribution') + '</div>';
  html += '<canvas id="anom-sigma-chart" style="width:100%;height:120px"></canvas></div>';

  /* ── Event timeline ── */
  html += '<div class="card" style="margin-bottom:12px"><div style="font-size:14px;font-weight:700;margin-bottom:8px">' + t('web.anom.timeline','Event-Timeline') + '</div>';
  html += '<div class="nilm-timeline">';
  events.slice(0, 50).forEach(function(ev) {{
    const ts = ev.timestamp ? new Date(ev.timestamp) : null;
    const timeStr = ts ? ts.toLocaleDateString() + ' ' + ts.toLocaleTimeString() : '';
    const at = ev.anomaly_type || ev.type || 'unknown';
    const icon = typeIcons[at] || '❓';
    const col = typeColors[at] || 'var(--accent)';
    const sigmaBar = ev.sigma ? Math.min(100, Math.round(ev.sigma / 5 * 100)) : 0;
    html += '<div class="nilm-tl-item">';
    html += '<div class="nilm-tl-dot" style="background:' + col + '"></div>';
    html += '<div class="nilm-tl-body">';
    html += '<div style="display:flex;justify-content:space-between;align-items:center">';
    html += '<span style="font-weight:600;font-size:13px">' + icon + ' ' + esc(typeLabels[at] || at) + '</span>';
    html += '<span style="font-size:11px;color:var(--muted)">' + timeStr + '</span>';
    html += '</div>';
    html += '<div style="font-size:12px;color:var(--muted)">' + esc(ev.device_name || '') + (ev.value !== undefined ? ' · ' + fmt(ev.value, 1) + (at === 'unusual_daily' || at === 'night_consumption' ? ' kWh' : ' W') : '') + ' · <strong>\u03c3=' + fmt(ev.sigma||0, 1) + '</strong></div>';
    if (ev.description) html += '<div style="font-size:11px;color:var(--muted);margin-top:2px">' + esc(ev.description) + '</div>';
    // Sigma bar
    html += '<div style="display:flex;gap:4px;align-items:center;margin-top:3px"><div style="flex:1;height:4px;background:var(--bg);border-radius:2px;overflow:hidden"><div style="width:' + sigmaBar + '%;height:100%;background:' + col + ';border-radius:2px"></div></div><span style="font-size:9px;color:var(--muted)">' + fmt(ev.sigma||0,1) + '\u03c3</span></div>';
    html += '</div></div>';
  }});
  html += '</div></div>';

  el.innerHTML = html;

  /* ── Draw canvases ── */
  requestAnimationFrame(function() {{
    _anomDrawTypeDonut(tc, typeColors, typeLabels, typeIcons);
    _anomDrawSigmaChart(events);
  }});
}}

function _anomDrawTypeDonut(tc, typeColors, typeLabels, typeIcons) {{
  const canvas = document.getElementById('anom-type-donut');
  const legend = document.getElementById('anom-type-legend');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio||1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width*dpr; canvas.height = rect.height*dpr;
  ctx.scale(dpr,dpr);
  const W = rect.width, H = rect.height;
  const cx = W/2, cy = H/2, R = Math.min(cx,cy)-8, r = R*0.55;
  const keys = Object.keys(tc);
  const total = keys.reduce(function(s,k){{ return s+tc[k]; }},0)||1;
  let angle = -Math.PI/2;
  let legHtml = '<div style="display:flex;flex-wrap:wrap;gap:6px">';
  keys.forEach(function(k) {{
    const count = tc[k];
    const slice = (count/total)*Math.PI*2;
    const col = typeColors[k]||'#999';
    ctx.fillStyle = col;
    ctx.beginPath(); ctx.arc(cx,cy,R,angle,angle+slice); ctx.arc(cx,cy,r,angle+slice,angle,true); ctx.closePath(); ctx.fill();
    angle += slice;
    legHtml += '<span style="font-size:10px;display:flex;align-items:center;gap:2px"><span style="width:8px;height:8px;border-radius:50%;background:'+col+';display:inline-block"></span>'+(typeIcons[k]||'')+' '+esc(typeLabels[k]||k)+' ('+count+')</span>';
  }});
  ctx.fillStyle = getComputedStyle(document.body).getPropertyValue('--fg')||'#111';
  ctx.font = 'bold 18px sans-serif'; ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
  ctx.fillText(total, cx, cy-6);
  ctx.font = '10px sans-serif'; ctx.fillStyle = getComputedStyle(document.body).getPropertyValue('--muted')||'#666';
  ctx.fillText('Events', cx, cy+10);
  legHtml += '</div>';
  if (legend) legend.innerHTML = legHtml;
}}

function _anomDrawSigmaChart(events) {{
  const canvas = document.getElementById('anom-sigma-chart');
  if (!canvas) return;
  // Bucket sigmas into bins: 2-3, 3-4, 4-5, 5+
  const bins = [0,0,0,0,0];
  const binLabels = ['2-3\u03c3','3-4\u03c3','4-5\u03c3','5-6\u03c3','6+\u03c3'];
  events.forEach(function(ev) {{
    const s = ev.sigma || ev.sigma_count || 0;
    if (s < 2) return;
    else if (s < 3) bins[0]++;
    else if (s < 4) bins[1]++;
    else if (s < 5) bins[2]++;
    else if (s < 6) bins[3]++;
    else bins[4]++;
  }});
  const colors = bins.map(function(_,i){{ return ['#3b82f6','#f59e0b','#f97316','#ef4444','#dc2626'][i]; }});
  _drawBarChart('anom-sigma-chart', binLabels, bins, {{colors:colors, decimals:0}});
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
  if (!_quietRefresh) cont.innerHTML = '<p class="loading-msg">Loading\u2026</p>';
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
  const trendPct = d.trend_pct_per_month || 0;
  const trendIcon = trendPct > 0.5 ? '📈' : trendPct < -0.5 ? '📉' : '➡️';
  const trendColor = trendPct > 0.5 ? '#dc2626' : trendPct < -0.5 ? '#16a34a' : 'var(--muted)';
  const histKwh = (d.history_kwh || []);
  const totalHistKwh = histKwh.reduce(function(s,v){{ return s+v; }}, 0);
  const histDays = histKwh.length;

  let html = '';

  /* ── Overview metrics ── */
  html += '<div class="nilm-metrics" style="margin-bottom:12px">';
  html += _nilmMetricCard('📊', t('web.fc.avg_daily','Daily average'), (d.avg_daily_kwh||0).toFixed(1) + ' kWh', esc(d.device_name||''));
  html += _nilmMetricCard(trendIcon, t('web.fc.trend','Trend'), '<span style="color:' + trendColor + '">' + (trendPct > 0 ? '+' : '') + trendPct.toFixed(1) + '%</span>', t('web.fc.per_month','per month'));
  html += _nilmMetricCard('📅', t('web.fc.next_month','Next month'), (d.forecast_next_month_kwh||0).toFixed(0) + ' kWh', fmt(d.forecast_next_month_cost||0, 2) + ' \u20ac');
  html += _nilmMetricCard('📆', t('web.fc.next_year','Next year'), (d.forecast_year_kwh||0).toFixed(0) + ' kWh', fmt(d.forecast_year_cost||0, 0) + ' \u20ac');
  html += '</div>';

  /* ── Trend + cost comparison ── */
  html += '<div class="nilm-two-col" style="margin-bottom:12px">';
  // Trend card
  html += '<div class="card"><div style="font-size:14px;font-weight:700;margin-bottom:8px">' + t('web.fc.trend_analysis','Trend-Analyse') + '</div>';
  html += '<div style="display:flex;align-items:center;gap:12px;margin-bottom:10px">';
  html += '<span style="font-size:36px">' + trendIcon + '</span>';
  html += '<div><div style="font-size:20px;font-weight:800;color:' + trendColor + '">' + (trendPct > 0 ? '+' : '') + trendPct.toFixed(1) + '% / ' + t('web.fc.month','Month') + '</div>';
  html += '<div style="font-size:12px;color:var(--muted)">' + t('web.fc.based_on','Basierend auf') + ' ' + histDays + ' ' + t('web.fc.days_data','days of data') + '</div></div></div>';
  // History summary
  html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">';
  html += '<div style="background:var(--bg);border-radius:8px;padding:8px;text-align:center"><div style="font-size:18px;font-weight:700">' + totalHistKwh.toFixed(0) + '</div><div style="font-size:10px;color:var(--muted)">kWh (' + histDays + 'd)</div></div>';
  const avgW = d.avg_daily_kwh ? (d.avg_daily_kwh * 1000 / 24).toFixed(0) : '0';
  html += '<div style="background:var(--bg);border-radius:8px;padding:8px;text-align:center"><div style="font-size:18px;font-weight:700">' + avgW + '</div><div style="font-size:10px;color:var(--muted)">' + t('web.fc.avg_w','Avg W') + '</div></div>';
  html += '</div></div>';

  // Cost projection card
  html += '<div class="card"><div style="font-size:14px;font-weight:700;margin-bottom:8px">' + t('web.fc.cost_projection','Cost projection') + '</div>';
  html += '<canvas id="fc-cost-bars" style="width:100%;height:160px"></canvas>';
  html += '</div></div>';

  /* ── Main chart with confidence band ── */
  html += '<div class="card" style="margin-bottom:12px"><div style="font-size:14px;font-weight:700;margin-bottom:8px">' + t('web.fc.history_forecast','History + forecast') + '</div>';
  html += '<div style="display:flex;gap:12px;font-size:11px;color:var(--muted);margin-bottom:6px">';
  html += '<span><span style="display:inline-block;width:12px;height:4px;background:#3b82f6;border-radius:2px;vertical-align:middle;margin-right:3px"></span>' + t('web.fc.history','Verlauf') + '</span>';
  html += '<span><span style="display:inline-block;width:12px;height:4px;background:#ef4444;border-radius:2px;vertical-align:middle;margin-right:3px"></span>' + t('web.fc.forecast','Forecast') + '</span>';
  if (d.forecast_upper && d.forecast_upper.length) html += '<span><span style="display:inline-block;width:12px;height:8px;background:rgba(239,68,68,0.15);border-radius:2px;vertical-align:middle;margin-right:3px"></span>' + t('web.fc.confidence','Konfidenz') + '</span>';
  html += '</div>';
  html += '<canvas id="fc-main-chart" style="width:100%;height:200px"></canvas></div>';

  /* ── Weekday + Hourly profiles ── */
  html += '<div class="nilm-two-col" style="margin-bottom:12px">';
  html += '<div class="card"><div style="font-size:14px;font-weight:700;margin-bottom:8px">' + t('web.fc.weekday','Weekday profile') + '</div>';
  html += '<canvas id="fc-weekday-chart" style="width:100%;height:150px"></canvas>';
  html += '<div style="font-size:11px;color:var(--muted);margin-top:4px;text-align:center">' + t('web.fc.weekday_hint','Factor relative to average (1.0 = normal)') + '</div></div>';
  html += '<div class="card"><div style="font-size:14px;font-weight:700;margin-bottom:8px">' + t('web.fc.hourly','Hourly profile') + '</div>';
  html += '<canvas id="fc-hourly-chart" style="width:100%;height:150px"></canvas>';
  html += '<div style="font-size:11px;color:var(--muted);margin-top:4px;text-align:center">' + t('web.fc.hourly_hint','Factor relative to daily average') + '</div></div>';
  html += '</div>';

  document.getElementById('forecast-cards').innerHTML = html;

  requestAnimationFrame(function() {{
    // Main chart with confidence band
    _fcDrawMainChart(d);
    // Cost projection bars
    _fcDrawCostBars(d);
    // Weekday profile
    if (d.weekday_profile) {{
      const days = [t('web.fc.mon','Mo'),t('web.fc.tue','Di'),t('web.fc.wed','Mi'),t('web.fc.thu','Do'),t('web.fc.fri','Fr'),t('web.fc.sat','Sa'),t('web.fc.sun','So')];
      const vals = days.map(function(_, i) {{ return d.weekday_profile[String(i)] || d.weekday_profile[i] || 1.0; }});
      const wdColors = vals.map(function(v) {{ return v > 1.1 ? '#e74c3c' : v < 0.9 ? '#27ae60' : '#3498db'; }});
      _drawBarChart('fc-weekday-chart', days, vals, {{ colors: wdColors, threshold: 1.0, thresholdColor: 'rgba(128,128,128,0.4)', thresholdLabel: '1.0', decimals: 2 }});
    }}
    // Hourly profile
    if (d.hourly_profile) {{
      const hrs = Array.from({{ length: 24 }}, function(_, i) {{ return String(i).padStart(2,'0'); }});
      const vals = hrs.map(function(h) {{ return d.hourly_profile[String(parseInt(h))] || d.hourly_profile[parseInt(h)] || 1.0; }});
      const hColors = vals.map(function(v) {{ return v > 1.3 ? '#e74c3c' : v > 1.1 ? '#f39c12' : v < 0.7 ? '#27ae60' : '#3498db'; }});
      _drawBarChart('fc-hourly-chart', hrs, vals, {{ colors: hColors, threshold: 1.0, thresholdColor: 'rgba(128,128,128,0.4)', thresholdLabel: '1.0', decimals: 2 }});
    }}
  }});
}}

function _fcDrawMainChart(d) {{
  const canvas = document.getElementById('fc-main-chart');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width * dpr; canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);
  const W = rect.width, H = rect.height;
  const pad = {{top:10,right:10,bottom:24,left:40}};
  const cW = W-pad.left-pad.right, cH = H-pad.top-pad.bottom;

  const hDates = d.history_dates || [];
  const hVals = d.history_kwh || [];
  const fDates = d.forecast_dates || [];
  const fVals = d.forecast_kwh || [];
  const fUp = d.forecast_upper || [];
  const fLo = d.forecast_lower || [];
  const allVals = hVals.concat(fVals).concat(fUp);
  const n = hDates.length + fDates.length;
  if (n === 0) return;
  const maxV = Math.max.apply(null, allVals.concat([0.1])) * 1.15;
  const barW = Math.max(2, (cW / n) - 1.5);
  const fg = getComputedStyle(document.body).getPropertyValue('--muted') || '#999';
  const border = getComputedStyle(document.body).getPropertyValue('--border') || '#e0e0e0';

  // Grid
  ctx.strokeStyle = border; ctx.lineWidth = 0.5;
  for (let i = 0; i <= 4; i++) {{
    const y = pad.top + cH - (cH * i / 4);
    ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(pad.left + cW, y); ctx.stroke();
    ctx.fillStyle = fg; ctx.font = '9px sans-serif'; ctx.textAlign = 'right';
    ctx.fillText((maxV * i / 4).toFixed(1), pad.left - 4, y + 3);
  }}

  // Confidence band (draw first, behind bars)
  if (fUp.length && fLo.length) {{
    ctx.fillStyle = 'rgba(239,68,68,0.12)';
    ctx.beginPath();
    for (let i = 0; i < fUp.length; i++) {{
      const x = pad.left + ((hDates.length + i) / n) * cW + barW / 2;
      const y = pad.top + cH - (fUp[i] / maxV) * cH;
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    }}
    for (let i = fLo.length - 1; i >= 0; i--) {{
      const x = pad.left + ((hDates.length + i) / n) * cW + barW / 2;
      const y = pad.top + cH - (Math.max(0, fLo[i]) / maxV) * cH;
      ctx.lineTo(x, y);
    }}
    ctx.closePath(); ctx.fill();
  }}

  // History bars
  hVals.forEach(function(v, i) {{
    const x = pad.left + (i / n) * cW + 0.5;
    const h = (v / maxV) * cH;
    ctx.fillStyle = '#3b82f6';
    ctx.beginPath();
    if (ctx.roundRect) ctx.roundRect(x, pad.top + cH - h, barW, h, [2,2,0,0]);
    else ctx.rect(x, pad.top + cH - h, barW, h);
    ctx.fill();
  }});

  // Forecast bars
  fVals.forEach(function(v, i) {{
    const x = pad.left + ((hDates.length + i) / n) * cW + 0.5;
    const h = (v / maxV) * cH;
    ctx.fillStyle = 'rgba(239,68,68,0.75)';
    ctx.beginPath();
    if (ctx.roundRect) ctx.roundRect(x, pad.top + cH - h, barW, h, [2,2,0,0]);
    else ctx.rect(x, pad.top + cH - h, barW, h);
    ctx.fill();
  }});

  // Divider line between history and forecast
  if (hDates.length > 0 && fDates.length > 0) {{
    const dx = pad.left + (hDates.length / n) * cW;
    ctx.strokeStyle = 'rgba(128,128,128,0.5)'; ctx.lineWidth = 1; ctx.setLineDash([4,3]);
    ctx.beginPath(); ctx.moveTo(dx, pad.top); ctx.lineTo(dx, pad.top + cH); ctx.stroke();
    ctx.setLineDash([]);
  }}

  // X labels (sparse)
  const allDates = hDates.concat(fDates);
  const step = Math.max(1, Math.floor(n / 10));
  ctx.fillStyle = fg; ctx.font = '9px sans-serif'; ctx.textAlign = 'center';
  allDates.forEach(function(dt, i) {{
    if (i % step === 0) {{
      const x = pad.left + (i / n) * cW + barW / 2;
      ctx.fillText(dt.substring(5), x, pad.top + cH + 14);
    }}
  }});
}}

function _fcDrawCostBars(d) {{
  const labels = [t('web.fc.month_short','Month'), t('web.fc.year_short','Year')];
  const vals = [d.forecast_next_month_cost || 0, d.forecast_year_cost || 0];
  const colors = ['#f59e0b', '#ef4444'];
  _drawBarChart('fc-cost-bars', labels, vals, {{ colors: colors, decimals: 0 }});
}}

/* ──────────────────────────────────────────────
   STANDBY TAB
────────────────────────────────────────────── */
async function loadStandby() {{
  const cont = document.getElementById('standby-cards');
  if (!_quietRefresh) cont.innerHTML = '<p class="loading-msg">' + t('web.loading','Loading…') + '</p>';
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
  const wrap = document.getElementById('standby-table-wrap');
  const devs = d.devices || [];
  const nDev = devs.length;

  /* ── Summary cards ── */
  const highCount = devs.filter(function(x){{ return x.risk==='high'; }}).length;
  const medCount = devs.filter(function(x){{ return x.risk==='medium'; }}).length;
  const lowCount = devs.filter(function(x){{ return x.risk==='low'; }}).length;
  const avgBase = nDev ? Math.round(devs.reduce(function(s,x){{ return s+x.base_load_w; }},0)/nDev) : 0;

  let html = '<div class="nilm-metrics" style="margin-bottom:12px">';
  html += _nilmMetricCard('\u26a0\ufe0f', t('web.standby.annual_cost','Standby cost/year'), (d.total_annual_standby_cost||0).toFixed(0) + ' \u20ac', t('web.standby.all_devices','All devices'));
  html += _nilmMetricCard('\u26a1', t('web.standby.annual_kwh','Standby kWh/year'), (d.total_annual_standby_kwh||0).toFixed(0) + ' kWh', t('web.standby.wasted','Verlorene Energie'));
  html += _nilmMetricCard('📊', t('web.standby.avg_base','Average base load'), avgBase + ' W', nDev + ' ' + t('web.standby.devices','Devices'));
  html += _nilmMetricCard('🚨', t('web.standby.risk_overview','Risk overview'),
    '<span style="color:#dc2626">' + highCount + '</span> / <span style="color:#d97706">' + medCount + '</span> / <span style="color:#16a34a">' + lowCount + '</span>',
    t('web.standby.high_med_low','High / Medium / Low'));
  html += '</div>';

  document.getElementById('standby-cards').innerHTML = html;

  if (!nDev) {{
    wrap.innerHTML = '<div class="card" style="text-align:center;padding:30px">' +
      '<div style="font-size:40px;margin-bottom:10px">🔌</div>' +
      '<div style="font-size:16px;font-weight:600;margin-bottom:6px">' + t('web.standby.no_data_title','No standby data') + '</div>' +
      '<div style="color:var(--muted);font-size:13px">' + t('web.standby.no_data_hint','At least 6 hours of data needed in the last {days} days.').replace('{{days}}', d.analysis_days||30) + '</div></div>';
    return;
  }}

  let cards = '';

  /* ── Cost pie + cost bar (side by side) ── */
  cards += '<div class="nilm-two-col" style="margin-bottom:10px">';
  cards += '<div class="card"><div style="font-size:14px;font-weight:700;margin-bottom:8px">' + t('web.standby.cost_breakdown','Cost breakdown') + '</div>';
  cards += '<canvas id="sb-cost-pie" style="width:100%;height:200px"></canvas>';
  cards += '<div id="sb-cost-pie-legend" style="margin-top:6px"></div></div>';
  cards += '<div class="card"><div style="font-size:14px;font-weight:700;margin-bottom:8px">' + t('web.standby.cost_ranking','Cost ranking') + '</div>';
  cards += '<canvas id="sb-cost-bar" style="width:100%;height:200px"></canvas></div>';
  cards += '</div>';

  /* ── Per-device detail cards ── */
  cards += '<div style="font-size:14px;font-weight:700;margin-bottom:8px">' + t('web.standby.per_device','Device details') + '</div>';
  cards += '<div class="sb-dev-grid" data-cols="' + Math.min(nDev, 4) + '">';
  devs.forEach(function(dev, idx) {{
    const rc = dev.risk === 'high' ? '#dc2626' : dev.risk === 'medium' ? '#d97706' : '#16a34a';
    const riskLabel = dev.risk === 'high' ? t('web.standby.risk_high','HIGH') : dev.risk === 'medium' ? t('web.standby.risk_med','MEDIUM') : t('web.standby.risk_low','LOW');
    cards += '<div class="nilm-pattern-card" style="border-left:4px solid ' + rc + '">';
    cards += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">';
    cards += '<span style="font-weight:700;font-size:14px">' + esc(dev.device_name) + '</span>';
    cards += '<span class="badge" style="background:' + rc + '22;color:' + rc + ';font-size:10px">' + riskLabel + '</span>';
    cards += '</div>';
    // Stats grid
    cards += '<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:6px;margin-bottom:8px">';
    cards += '<div class="nilm-stat"><span class="nilm-stat-val">' + dev.base_load_w + '</span><span class="nilm-stat-lbl">' + t('web.standby.base_w','Base load W') + '</span></div>';
    cards += '<div class="nilm-stat"><span class="nilm-stat-val">' + (dev.night_median_w||dev.base_load_w) + '</span><span class="nilm-stat-lbl">' + t('web.standby.night_w','Night W') + '</span></div>';
    cards += '<div class="nilm-stat"><span class="nilm-stat-val">' + (dev.standby_pct||0).toFixed(0) + '%</span><span class="nilm-stat-lbl">' + t('web.standby.time_pct','Standby time') + '</span></div>';
    cards += '<div class="nilm-stat"><span class="nilm-stat-val">' + dev.annual_standby_kwh + '</span><span class="nilm-stat-lbl">kWh/' + t('web.standby.year','Year') + '</span></div>';
    cards += '<div class="nilm-stat"><span class="nilm-stat-val">' + dev.annual_standby_cost + ' \u20ac</span><span class="nilm-stat-lbl">\u20ac/' + t('web.standby.year','Year') + '</span></div>';
    cards += '<div class="nilm-stat"><span class="nilm-stat-val">' + dev.standby_share_pct + '%</span><span class="nilm-stat-lbl">' + t('web.standby.share','Anteil') + '</span></div>';
    cards += '</div>';
    // Standby vs active mini bar
    const activePct = Math.max(0, 100 - (dev.standby_share_pct||0));
    cards += '<div style="display:flex;gap:4px;align-items:center;margin-bottom:6px;font-size:10px">';
    cards += '<span style="color:' + rc + '">' + t('web.standby.standby','Standby') + '</span>';
    cards += '<div style="flex:1;height:8px;background:var(--bg);border-radius:4px;overflow:hidden">';
    cards += '<div style="width:' + (dev.standby_share_pct||0) + '%;height:100%;background:' + rc + ';border-radius:4px"></div>';
    cards += '</div>';
    cards += '<span style="color:var(--muted)">' + t('web.standby.active','Active') + '</span></div>';
    // 24h mini profile
    cards += '<canvas class="sb-24h-mini" data-idx="' + idx + '" style="width:100%;height:60px"></canvas>';
    cards += '</div>';
  }});
  cards += '</div>';

  /* ── Full 24h comparison chart (all devices overlaid) ── */
  if (devs.length > 1) {{
    cards += '<div class="card" style="margin-top:10px"><div style="font-size:14px;font-weight:700;margin-bottom:8px">' + t('web.standby.compare_24h','24h comparison all devices') + '</div>';
    cards += '<canvas id="sb-24h-all" style="width:100%;height:180px"></canvas>';
    cards += '<div id="sb-24h-legend" style="margin-top:6px"></div></div>';
  }}

  /* ── Savings potential ── */
  cards += '<div class="card" style="margin-top:10px"><div style="font-size:14px;font-weight:700;margin-bottom:8px">' + t('web.standby.savings','Savings potential') + '</div>';
  cards += '<div style="display:grid;grid-template-columns:1fr;gap:6px">';
  devs.forEach(function(dev) {{
    if (dev.annual_standby_cost < 5) return;
    const rc = dev.risk === 'high' ? '#dc2626' : dev.risk === 'medium' ? '#d97706' : '#16a34a';
    cards += '<div style="display:flex;align-items:center;gap:10px;padding:8px;background:var(--bg);border-radius:8px">';
    cards += '<span style="font-size:16px">💡</span>';
    cards += '<div style="flex:1;min-width:0">';
    cards += '<div style="font-weight:600;font-size:13px">' + esc(dev.device_name) + '</div>';
    cards += '<div style="font-size:12px;color:var(--muted)">' + t('web.standby.savings_tip','A smart plug or timer saves') + ' <strong style="color:' + rc + '">' + dev.annual_standby_cost + ' \u20ac/' + t('web.standby.year','Year') + '</strong></div>';
    cards += '</div></div>';
  }});
  cards += '</div></div>';

  wrap.innerHTML = cards;

  /* ── Draw all canvases ── */
  requestAnimationFrame(function() {{
    _sbDrawCostPie(devs);
    _sbDrawCostBar(devs);
    _sbDraw24hMinis(devs);
    if (devs.length > 1) _sbDraw24hAll(devs);
  }});
}}

function _sbDrawCostPie(devs) {{
  const canvas = document.getElementById('sb-cost-pie');
  const legend = document.getElementById('sb-cost-pie-legend');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width * dpr; canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);
  const W = rect.width, H = rect.height;
  const cx = W/2, cy = H/2, R = Math.min(cx,cy)-10, r = R*0.55;
  const total = devs.reduce(function(s,x){{ return s+x.annual_standby_cost; }},0) || 1;
  const colors = ['#dc2626','#d97706','#16a34a','#3b82f6','#8b5cf6','#ec4899','#14b8a6','#f97316','#6366f1','#06b6d4'];
  let angle = -Math.PI/2;
  let legHtml = '<div style="display:flex;flex-wrap:wrap;gap:4px">';
  devs.forEach(function(dev,i) {{
    const slice = (dev.annual_standby_cost/total)*Math.PI*2;
    const col = colors[i%colors.length];
    ctx.fillStyle = col;
    ctx.beginPath(); ctx.arc(cx,cy,R,angle,angle+slice); ctx.arc(cx,cy,r,angle+slice,angle,true); ctx.closePath(); ctx.fill();
    angle += slice;
    legHtml += '<span style="font-size:10px;display:flex;align-items:center;gap:2px"><span style="width:8px;height:8px;border-radius:50%;background:'+col+';display:inline-block"></span>'+esc(dev.device_name)+' ('+dev.annual_standby_cost+'\u20ac)</span>';
  }});
  ctx.fillStyle = getComputedStyle(document.body).getPropertyValue('--fg')||'#111';
  ctx.font = 'bold 16px sans-serif'; ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
  ctx.fillText(total.toFixed(0) + ' \u20ac', cx, cy-6);
  ctx.font = '10px sans-serif'; ctx.fillStyle = getComputedStyle(document.body).getPropertyValue('--muted')||'#666';
  ctx.fillText('/' + t('web.standby.year','Year'), cx, cy+10);
  legHtml += '</div>';
  if (legend) legend.innerHTML = legHtml;
}}

function _sbDrawCostBar(devs) {{
  const names = devs.map(function(x){{ return x.device_name.substring(0,14); }});
  const costs = devs.map(function(x){{ return x.annual_standby_cost; }});
  const cols = devs.map(function(x){{ return x.risk==='high'?'#dc2626':x.risk==='medium'?'#d97706':'#16a34a'; }});
  _drawBarChart('sb-cost-bar', names, costs, {{colors:cols, decimals:0}});
}}

function _sbDraw24hMinis(devs) {{
  devs.forEach(function(dev, idx) {{
    const canvas = document.querySelector('.sb-24h-mini[data-idx="'+idx+'"]');
    if (!canvas || !dev.hourly_profile || dev.hourly_profile.length !== 24) return;
    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio||1;
    const rect = canvas.getBoundingClientRect();
    canvas.width = rect.width*dpr; canvas.height = rect.height*dpr;
    ctx.scale(dpr,dpr);
    const W = rect.width, H = rect.height;
    const pad = {{top:4,right:4,bottom:14,left:28}};
    const cW = W-pad.left-pad.right, cH = H-pad.top-pad.bottom;
    const hp = dev.hourly_profile;
    const maxV = Math.max.apply(null,hp.concat([1]))*1.15;
    const barW = Math.max(2,(cW/24)-1.5);
    const rc = dev.risk==='high'?'#dc2626':dev.risk==='medium'?'#d97706':'#16a34a';
    const fg = getComputedStyle(document.body).getPropertyValue('--muted')||'#999';
    // Standby threshold line
    const thY = pad.top+cH-(dev.base_load_w/maxV)*cH;
    ctx.strokeStyle = rc; ctx.lineWidth = 1; ctx.setLineDash([3,3]);
    ctx.beginPath(); ctx.moveTo(pad.left,thY); ctx.lineTo(pad.left+cW,thY); ctx.stroke();
    ctx.setLineDash([]);
    // Bars
    hp.forEach(function(v,i) {{
      const x = pad.left+(i/24)*cW+0.5;
      const h = (v/maxV)*cH;
      const y = pad.top+cH-h;
      ctx.fillStyle = v <= dev.base_load_w*2 ? rc+'88' : 'hsl(210,60%,55%)';
      ctx.beginPath();
      if (ctx.roundRect) ctx.roundRect(x,y,barW,h,[1,1,0,0]);
      else {{ ctx.rect(x,y,barW,h); }}
      ctx.fill();
      if (i%6===0) {{
        ctx.fillStyle = fg; ctx.font = '8px sans-serif'; ctx.textAlign = 'center';
        ctx.fillText(String(i).padStart(2,'0'), x+barW/2, pad.top+cH+10);
      }}
    }});
    // Y axis label
    ctx.fillStyle = fg; ctx.font = '8px sans-serif'; ctx.textAlign = 'right';
    ctx.fillText(Math.round(maxV)+'W', pad.left-3, pad.top+6);
    ctx.fillText('0', pad.left-3, pad.top+cH+2);
  }});
}}

function _sbDraw24hAll(devs) {{
  const canvas = document.getElementById('sb-24h-all');
  const legend = document.getElementById('sb-24h-legend');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio||1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width*dpr; canvas.height = rect.height*dpr;
  ctx.scale(dpr,dpr);
  const W = rect.width, H = rect.height;
  const pad = {{top:8,right:8,bottom:20,left:40}};
  const cW = W-pad.left-pad.right, cH = H-pad.top-pad.bottom;
  const colors = ['#3b82f6','#dc2626','#d97706','#16a34a','#8b5cf6','#ec4899','#14b8a6','#f97316'];
  // Find global max
  let maxV = 1;
  devs.forEach(function(dev) {{
    if (dev.hourly_profile) maxV = Math.max(maxV, Math.max.apply(null, dev.hourly_profile));
  }});
  maxV *= 1.15;
  const fg = getComputedStyle(document.body).getPropertyValue('--muted')||'#999';
  const border = getComputedStyle(document.body).getPropertyValue('--border')||'#e0e0e0';
  // Grid
  ctx.strokeStyle = border; ctx.lineWidth = 0.5;
  for (let i=0;i<=4;i++) {{
    const y = pad.top+cH-(cH*i/4);
    ctx.beginPath(); ctx.moveTo(pad.left,y); ctx.lineTo(pad.left+cW,y); ctx.stroke();
    ctx.fillStyle = fg; ctx.font = '9px sans-serif'; ctx.textAlign = 'right';
    ctx.fillText(Math.round(maxV*i/4)+'W', pad.left-4, y+3);
  }}
  // Hour labels
  for (let h=0;h<24;h+=3) {{
    const x = pad.left+(h/24)*cW;
    ctx.fillStyle = fg; ctx.font = '9px sans-serif'; ctx.textAlign = 'center';
    ctx.fillText(String(h).padStart(2,'0'), x, pad.top+cH+14);
  }}
  // Lines per device
  let legHtml = '<div style="display:flex;flex-wrap:wrap;gap:6px">';
  devs.forEach(function(dev,di) {{
    if (!dev.hourly_profile || dev.hourly_profile.length!==24) return;
    const col = colors[di%colors.length];
    ctx.strokeStyle = col; ctx.lineWidth = 2;
    ctx.beginPath();
    dev.hourly_profile.forEach(function(v,h) {{
      const x = pad.left+(h/23)*cW;
      const y = pad.top+cH-(v/maxV)*cH;
      if (h===0) ctx.moveTo(x,y); else ctx.lineTo(x,y);
    }});
    ctx.stroke();
    // Dots
    ctx.fillStyle = col;
    dev.hourly_profile.forEach(function(v,h) {{
      if (h%3!==0) return;
      const x = pad.left+(h/23)*cW;
      const y = pad.top+cH-(v/maxV)*cH;
      ctx.beginPath(); ctx.arc(x,y,2.5,0,Math.PI*2); ctx.fill();
    }});
    legHtml += '<span style="font-size:10px;display:flex;align-items:center;gap:2px"><span style="width:10px;height:3px;background:'+col+';display:inline-block;border-radius:2px"></span>'+esc(dev.device_name)+'</span>';
  }});
  legHtml += '</div>';
  if (legend) legend.innerHTML = legHtml;
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
  if (!_quietRefresh) cont.innerHTML = '<p class="loading-msg">Loading\u2026</p>';
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
  if (!_quietRefresh) wrap.innerHTML = '<p class="loading-msg">' + t('web.ev.loading', 'Loading chargers\u2026') + '</p>';

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

  /* ── Smart Schedule ── */
  window._ssDuration = 3;
  async function loadSmartSched() {{
    const el = document.getElementById('ss-content');
    if (!_quietRefresh) el.innerHTML = '<p class="loading-msg">Loading…</p>';
    try {{
      const r = await fetch('/api/smart_schedule?duration=' + window._ssDuration);
      if (!r.ok) throw new Error(r.status);
      const d = await r.json();
      if (!d.ok) throw new Error(d.error || 'unknown');
      renderSmartSched(d.data, el);
    }} catch(e) {{
      el.innerHTML = '<p class="error-msg">Error: ' + e.message + '</p>';
    }}
  }}
  function _ssFmtDateTime(ts) {{
    const d = new Date(ts*1000);
    const now = new Date();
    const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    const tomorrow = new Date(today.getTime() + 86400000);
    const dayStart = new Date(d.getFullYear(), d.getMonth(), d.getDate());
    const wday = d.toLocaleDateString(document.documentElement.lang || 'de', {{weekday:'long'}});
    const dateStr = d.toLocaleDateString([], {{day:'2-digit', month:'2-digit', year:'numeric'}});
    const timeStr = d.toLocaleTimeString([], {{hour:'2-digit', minute:'2-digit'}});
    let prefix = wday + ', ' + dateStr;
    if (dayStart.getTime() === today.getTime()) prefix = 'Today · ' + wday;
    else if (dayStart.getTime() === tomorrow.getTime()) prefix = 'Tomorrow · ' + wday;
    return {{ prefix: prefix, date: dateStr, time: timeStr }};
  }}
  function renderSmartSched(data, el) {{
    const durOpts = [1,2,3,4,6,8];
    let durSel = '<div style="display:flex;align-items:center;gap:8px;margin-bottom:10px;flex-wrap:wrap">' +
      '<span style="font-size:12px;color:var(--muted)">Duration:</span>';
    durOpts.forEach(function(h){{
      const active = h === Number(window._ssDuration);
      durSel += '<button onclick="window._ssDuration=' + h + ';loadSmartSched()" style="padding:4px 10px;border-radius:6px;border:1px solid ' + (active?'#ff9800':'var(--border)') + ';background:' + (active?'#ff9800':'transparent') + ';color:' + (active?'#fff':'var(--fg)') + ';cursor:pointer;font-size:12px;font-weight:' + (active?'600':'400') + '">' + h + ' h</button>';
    }});
    durSel += '</div>';

    const blocks = data.blocks || [];
    if (!blocks.length) {{
      el.innerHTML = durSel + '<div class="card" style="padding:14px"><p class="info-msg">No spot price data for the next 24h. Enable spot prices in settings or wait for data to be fetched.</p></div>';
      return;
    }}
    const fixed = data.fixed_ct || 0;
    const avg24 = data.avg_24h_ct || 0;
    const cheap = data.cheapest_hour_ct || 0;
    const exp = data.most_expensive_hour_ct || 0;

    // Context card – what does this tab do?
    let html = durSel;
    html += '<div class="card" style="margin-bottom:10px;padding:12px;background:linear-gradient(135deg,rgba(255,152,0,0.08),rgba(255,152,0,0.02));border-left:3px solid #ff9800">' +
      '<div style="font-size:13px;font-weight:650;color:#ff9800;margin-bottom:4px">⏱ Cheapest time windows in the next 24h</div>' +
      '<div style="font-size:12px;color:var(--muted);line-height:1.5">Start your <b>energy-hungry appliances</b> (washing machine, dishwasher, dryer, EV, heat pump) during one of the windows below — that is when spot power is cheapest. Savings are compared to your fixed price.</div>' +
      '</div>';

    // 24h context tiles
    html += '<div class="metric-grid" style="margin-bottom:10px">' +
      metricCardHtml('Ø 24h Spot', avg24.toFixed(1) + ' ct/kWh', 'Average') +
      metricCardHtml('Fixed price', fixed.toFixed(1) + ' ct/kWh', 'Your tariff') +
      metricCardHtml('Cheapest hour', cheap.toFixed(1) + ' ct/kWh', '') +
      metricCardHtml('Most expensive hour', exp.toFixed(1) + ' ct/kWh', '') +
      '</div>';

    // Top-3 blocks
    blocks.forEach(function(b, idx) {{
      const st = _ssFmtDateTime(b.start_ts);
      const en = _ssFmtDateTime(b.end_ts);
      const saveCtKwh = b.savings_vs_fixed_ct_per_kwh || 0;
      const saveTotalCt = saveCtKwh * (b.block_hours || 1); // ct saved per kWh × duration (rough multiplier)
      const saveColor = saveCtKwh > 0 ? '#4caf50' : (saveCtKwh < 0 ? '#e53935' : 'var(--muted)');
      const rank = idx === 0 ? '🥇' : (idx === 1 ? '🥈' : '🥉');
      const borderColor = idx === 0 ? '#ff9800' : 'var(--border)';
      const borderWidth = idx === 0 ? '2px' : '1px';
      const vsFixedText = saveCtKwh > 0
        ? 'Save <b style="color:#4caf50">' + saveCtKwh.toFixed(1) + ' ct/kWh</b> vs. fixed price'
        : (saveCtKwh < 0 ? '<b style="color:#e53935">' + Math.abs(saveCtKwh).toFixed(1) + ' ct/kWh more</b> than fixed price' : 'Same price as fixed tariff');

      html += '<div class="card" style="margin-bottom:8px;border:' + borderWidth + ' solid ' + borderColor + ';padding:12px">' +
        '<div style="display:flex;justify-content:space-between;align-items:start;flex-wrap:wrap;gap:10px">' +
        '<div style="flex:1;min-width:200px">' +
          '<div style="font-size:18px;font-weight:700">' + rank + ' ' + st.time + ' – ' + en.time + '</div>' +
          '<div style="font-size:12px;color:var(--muted);margin-top:2px">' + st.prefix + ' · ' + b.block_hours + ' h window</div>' +
          '<div style="font-size:12px;margin-top:6px">' + vsFixedText + '</div>' +
        '</div>' +
        '<div style="text-align:right;min-width:120px">' +
          '<div style="font-size:22px;font-weight:700;color:' + saveColor + '">' + b.avg_price_ct.toFixed(1) + '</div>' +
          '<div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px">ct/kWh Ø</div>' +
        '</div>' +
        '</div>' +
        '<div style="margin-top:10px;padding-top:8px;border-top:1px dashed var(--border);font-size:11px;color:var(--muted)">' +
          '💡 At 2 kWh usage (e.g. washing machine): ' +
          '<b style="color:' + saveColor + '">' + (saveCtKwh >= 0 ? '-' : '+') + Math.abs(saveCtKwh * 2).toFixed(0) + ' ct</b> vs. fixed price · ' +
          '<b>' + (b.avg_price_ct * 2).toFixed(0) + ' ct</b> total' +
        '</div>' +
        '</div>';
    }});

    // Footer hint
    html += '<div style="margin-top:10px;font-size:11px;color:var(--muted);text-align:center;line-height:1.5">' +
      'Prices incl. grid fee, electricity tax, CHP/§19/offshore surcharges, supplier margin and VAT from your spot-price settings. ' +
      'Zone: ' + (data.zone || '?') + ' · Updated hourly.' +
      '</div>';

    el.innerHTML = html;
  }}

  /* ── EV Log ── */
  async function loadEvLog() {{
    const el = document.getElementById('ev-content');
    if (!_quietRefresh) el.innerHTML = '<p class="loading-msg">Loading…</p>';
    try {{
      const r = await fetch('/api/ev_sessions');
      if (!r.ok) throw new Error(r.status);
      const d = await r.json();
      if (!d.ok) throw new Error(d.error || 'unknown');
      renderEvLog(d.data, el);
    }} catch(e) {{
      el.innerHTML = '<p class="error-msg">Error: ' + e.message + '</p>';
    }}
  }}
  function renderEvLog(data, el) {{
    let html = '<div class="card" style="margin-bottom:10px"><div class="card-title">🚗 Charging overview</div>' +
      '<div class="metric-grid">' +
      metricCardHtml('Charge sessions', data.total_sessions || 0) +
      metricCardHtml('Total', (data.total_kwh||0).toFixed(1) + ' kWh') +
      metricCardHtml('Cost', (data.total_cost||0).toFixed(2) + ' €') +
      metricCardHtml('Ø duration', (data.avg_duration_min||0).toFixed(0) + ' min') +
      '</div></div>';
    if (!data.sessions || !data.sessions.length) {{
      html += '<div class="card" style="padding:14px"><p class="info-msg">No charge sessions detected. Configure wallbox device in settings.</p></div>';
      el.innerHTML = html;
      return;
    }}
    html += '<div class="card" style="overflow-x:auto"><table style="width:100%;border-collapse:collapse;font-size:12px">' +
      '<thead><tr style="border-bottom:1px solid var(--border)">';
    ['Date','Time','Duration','kWh','€'].forEach(function(c){{ html += '<th style="padding:6px;color:var(--muted)">'+c+'</th>'; }});
    html += '</tr></thead><tbody>';
    data.sessions.slice(-20).reverse().forEach(function(se) {{
      const sd = new Date(se.start_ts*1000);
      html += '<tr style="border-bottom:1px solid var(--border)">' +
        '<td style="padding:4px;text-align:center">' + sd.toLocaleDateString([],{{day:'2-digit',month:'2-digit'}}) + '</td>' +
        '<td style="padding:4px;text-align:center">' + sd.toLocaleTimeString([],{{hour:'2-digit',minute:'2-digit'}}) + '</td>' +
        '<td style="padding:4px;text-align:center">' + Math.round((se.end_ts-se.start_ts)/60) + 'm</td>' +
        '<td style="padding:4px;text-align:center">' + se.energy_kwh.toFixed(1) + '</td>' +
        '<td style="padding:4px;text-align:center">' + se.cost_eur.toFixed(2) + '</td></tr>';
    }});
    html += '</tbody></table></div>';
    el.innerHTML = html;
  }}

  /* ── Tariff Comparison ── */
  async function loadTariff() {{
    const el = document.getElementById('tariff-content');
    if (!_quietRefresh) el.innerHTML = '<p class="loading-msg">Loading…</p>';
    try {{
      const r = await fetch('/api/tariff_compare');
      if (!r.ok) throw new Error(r.status);
      const d = await r.json();
      if (!d.ok) throw new Error(d.error || 'unknown');
      renderTariff(d.data, el);
    }} catch(e) {{
      el.innerHTML = '<p class="error-msg">Error: ' + e.message + '</p>';
    }}
  }}
  function renderTariff(data, el) {{
    const results = data.results || [];
    if (!results.length) {{
      el.innerHTML = '<div class="card" style="padding:14px"><p class="info-msg">No consumption data available.</p></div>';
      return;
    }}
    // Pick currency symbol from the API response so US / GB / CHF / AU users
    // don't see € on every card. Fallback is € for the EU bidding zones.
    const currency = (data.currency || 'EUR').toUpperCase();
    const symMap = {{EUR:'€', USD:'$', GBP:'£', CHF:'CHF', AUD:'A$', NOK:'kr', SEK:'kr', DKK:'kr', PLN:'zł', CZK:'Kč'}};
    const sym = symMap[currency] || currency;
    const country = data.country || '';
    const zone = data.zone || '';
    let html = '';
    // Header: show which country's templates are being compared so the
    // user knows why (e.g.) Octopus / Tibber / Holaluz are in the list.
    if (country || zone) {{
      html += '<div class="card" style="margin-bottom:10px;padding:10px;font-size:12px;color:var(--muted)">' +
        t('web.tariff.country_hint', 'Comparing against representative {country} tariffs (currency: {currency})', {{country: country || zone, currency: currency}}) +
        '</div>';
    }}
    results.forEach(function(r) {{
      const border = r.is_current ? '2px solid #ff9800' : '1px solid var(--border)';
      const sav = r.is_current ? '' : (r.savings_vs_current_eur > 0
        ? '<span style="color:#4caf50;font-weight:600">▼ ' + r.savings_vs_current_eur.toFixed(0) + ' ' + sym + '/' + t('web.tariff.year', 'year') + '</span>'
        : '<span style="color:#e53935">▲ ' + Math.abs(r.savings_vs_current_eur).toFixed(0) + ' ' + sym + '/' + t('web.tariff.year', 'year') + '</span>');
      const badge = r.is_current ? ' <span style="background:#ff9800;color:#fff;font-size:10px;padding:2px 6px;border-radius:8px">' + t('web.tariff.current', 'Current') + '</span>' : '';
      html += '<div class="card" style="border:' + border + '">' +
        '<div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:4px">' +
        '<div><b style="font-size:14px">' + esc(r.name) + '</b>' + badge +
        '<br><span style="font-size:11px;color:var(--muted)">' + esc(r.provider) + ' · ' + r.tariff_type.toUpperCase() + '</span></div>' +
        '<div style="text-align:right"><div style="font-size:17px;font-weight:700">' + r.annual_cost_eur.toFixed(0) + ' ' +
        sym + '<span style="font-size:11px;font-weight:400;color:var(--muted)">/' + t('web.tariff.year', 'year') + '</span></div>' + sav + '</div>' +
        '</div></div>';
    }});
    el.innerHTML = html;
  }}

  /* ── Battery ── */
  async function loadBattery() {{
    const el = document.getElementById('bat-content');
    if (!_quietRefresh) el.innerHTML = '<p class="loading-msg">Loading…</p>';
    try {{
      const r = await fetch('/api/battery');
      if (!r.ok) throw new Error(r.status);
      const d = await r.json();
      if (!d.ok) throw new Error(d.error || 'unknown');
      renderBattery(d.data, el);
    }} catch(e) {{
      el.innerHTML = '<p class="error-msg">Error: ' + e.message + '</p>';
    }}
  }}
  function renderBattery(data, el) {{
    const ml = {{charging:'Charging', discharging:'Discharging', idle:'Standby'}};
    el.innerHTML = '<div class="card" style="margin-bottom:10px"><div class="card-title">🔋 Battery storage</div>' +
      '<div class="metric-grid">' +
      metricCardHtml('SOC', data.soc_pct.toFixed(0) + '%') +
      metricCardHtml('Power', data.power_w.toFixed(0) + ' W') +
      metricCardHtml('Mode', ml[data.mode] || data.mode) +
      metricCardHtml('Cycles', data.cycle_count) +
      metricCardHtml('Efficiency', data.avg_efficiency_pct.toFixed(1) + '%') +
      '</div></div>';
  }}

  /* ── AI Advisor ── */
  async function loadAdvisor() {{
    const el = document.getElementById('advisor-content');
    if (!_quietRefresh) el.innerHTML = '<p class="loading-msg">Loading…</p>';
    try {{
      const r = await fetch('/api/advisor');
      if (!r.ok) throw new Error(r.status);
      const d = await r.json();
      if (!d.ok) throw new Error(d.error || 'unknown');
      renderAdvisor(d.data, el);
    }} catch(e) {{
      el.innerHTML = '<p class="error-msg">Error: ' + e.message + '</p>';
    }}
  }}
  function renderAdvisor(data, el) {{
    let html = '<div class="card" style="margin-bottom:10px;text-align:center">' +
      '<div class="card-title">🤖 ' + t('advisor.card_title','AI Energy Advisor') + '</div>' +
      '<div style="font-size:20px;font-weight:700;color:#4caf50;padding:8px 0">💰 ' +
      data.total_savings_potential_eur.toFixed(0) + ' €/year savings potential</div></div>';
    if (data.llm_summary) {{
      html += '<div class="card" style="margin-bottom:10px"><div class="card-title">🤖 ' + t('web.advisor.llm_summary', 'AI summary') + '</div>' +
        '<div style="padding:4px 0">' + esc(data.llm_summary) + '</div></div>';
    }}
    (data.tips || []).forEach(function(tip) {{
      const sav = tip.potential_savings_eur > 0
        ? '<div style="color:#4caf50;font-size:12px;margin-top:4px">💰 ' + tip.potential_savings_eur.toFixed(0) + ' €/year</div>'
        : '';
      html += '<div class="card"><div class="card-title">' + tip.icon + ' ' + esc(tip.title) + '</div>' +
        '<div style="color:var(--muted);font-size:12px">' + esc(tip.description) + '</div>' + sav + '</div>';
    }});
    el.innerHTML = html;
  }}

  /* ── Goals & Gamification ── */
  async function loadGoals() {{
    const el = document.getElementById('goals-content');
    if (!_quietRefresh) el.innerHTML = '<p class="loading-msg">' + t('web.loading', 'Loading…') + '</p>';
    try {{
      const r = await fetch('/api/goals');
      if (!r.ok) throw new Error(r.status);
      const d = await r.json();
      if (!d.ok) throw new Error(d.error || 'unknown');
      renderGoals(d.data, el);
    }} catch(e) {{
      el.innerHTML = '<p class="error-msg">Error: ' + e.message + '</p>';
    }}
  }}
  function _glBar(pct, h) {{
    h = h || 6;
    return '<div style="height:' + h + 'px;background:var(--border);border-radius:3px;margin-top:4px">' +
      '<div style="height:100%;width:' + Math.min(pct,100) + '%;background:' +
      (pct <= 100 ? '#4caf50' : '#e53935') + ';border-radius:3px;transition:width .4s"></div></div>';
  }}
  function _glCard(icon, label, value, sub) {{
    return '<div class="card"><div class="metric-label">' + icon + ' ' + label + '</div><div class="metric-value">' + value + '</div>' +
      (sub ? '<div style="font-size:10px;color:var(--muted);margin-top:2px">' + sub + '</div>' : '') + '</div>';
  }}

  function renderGoals(data, el) {{
    var html = '';
    var s = data.streak || {{}};
    var wg = data.weekly_goal || {{}};
    var mg = data.monthly_goal || {{}};

    // ── Level & XP Hero ──────────────────────────────────────
    var lvl = data.level || 1;
    var xp = data.xp || 0;
    var xpNext = data.xp_for_next || 100;
    var xpIn = data.xp_in_level || 0;
    var lpct = data.level_progress_pct || 0;
    var lvlTitle = lvl >= 20 ? '\U0001f451 Master' : lvl >= 15 ? '\U0001f48e Diamond' : lvl >= 10 ? '\U0001f947 Gold' : lvl >= 5 ? '\U0001f948 Silver' : '\U0001f949 Bronze';
    html += '<div class="card" style="text-align:center;margin-bottom:10px;position:relative;overflow:hidden">';
    html += '<div style="position:absolute;top:0;left:0;height:100%;width:' + lpct + '%;background:rgba(76,175,80,0.08);transition:width .5s"></div>';
    html += '<div style="position:relative;z-index:1">';
    html += '<div style="font-size:36px;font-weight:800;color:var(--accent)">Level ' + lvl + '</div>';
    html += '<div style="font-size:13px;color:var(--muted);margin-top:2px">' + lvlTitle + ' \u2022 ' + xp + ' XP</div>';
    html += _glBar(lpct, 8);
    html += '<div style="font-size:10px;color:var(--muted);margin-top:2px">' + xpIn + ' / ' + xpNext + ' XP ' + t('web.goals.to_next', 'to next level') + '</div>';
    html += '</div></div>';

    // ── Streak Hero ──────────────────────────────────────────
    var fire = '';
    for (var fi = 0; fi < Math.min(s.current_days || 0, 10); fi++) fire += '\U0001f525';
    if (!fire) fire = '\u2744\ufe0f';
    html += '<div class="card" style="text-align:center;margin-bottom:10px">';
    html += '<div style="font-size:24px">' + fire + '</div>';
    html += '<div style="font-size:20px;font-weight:700">' + (s.current_days || 0) + ' ' + t('web.goals.streak_days', 'Day streak') + '</div>';
    html += '<div style="font-size:11px;color:var(--muted)">' + t('web.goals.best_streak', 'Best') + ': ' + (s.best_days || 0) + ' ' + t('web.goals.days', 'Days') + '</div>';
    html += '</div>';

    // ── Goal cards (weekly + monthly) ────────────────────────
    html += '<div class="nilm-two-col">';
    // Weekly
    html += '<div class="card">';
    html += '<div style="display:flex;justify-content:space-between;align-items:center">';
    html += '<div class="card-title">\U0001f4c5 ' + t('web.goals.weekly', 'Weekly goal') + '</div>';
    html += '<span style="font-size:18px">' + (wg.achieved ? '\u2705' : '\u23f3') + '</span></div>';
    html += '<div style="font-size:22px;font-weight:700;margin:6px 0">' + (wg.actual_kwh||0).toFixed(1) + ' <span style="font-size:14px;color:var(--muted)">/ ' + (wg.target_kwh||0).toFixed(1) + ' kWh</span></div>';
    html += _glBar(wg.progress_pct||0, 10);
    html += '<div style="font-size:11px;color:var(--muted);margin-top:4px">';
    if (wg.achieved) html += '\u2705 ' + t('web.goals.achieved', 'Goal reached!') + ' \u2013 ' + (wg.remaining_kwh||0).toFixed(1) + ' kWh ' + t('web.goals.saved', 'saved');
    else html += '\u23f3 ' + t('web.goals.remaining', 'Noch') + ' ' + (wg.remaining_kwh||0).toFixed(1) + ' kWh ' + t('web.goals.to_go', 'to go');
    html += '</div></div>';
    // Monthly
    html += '<div class="card">';
    html += '<div style="display:flex;justify-content:space-between;align-items:center">';
    html += '<div class="card-title">\U0001f4c6 ' + t('web.goals.monthly', 'Monthly goal') + '</div>';
    html += '<span style="font-size:18px">' + (mg.achieved ? '\u2705' : '\u23f3') + '</span></div>';
    html += '<div style="font-size:22px;font-weight:700;margin:6px 0">' + (mg.actual_kwh||0).toFixed(1) + ' <span style="font-size:14px;color:var(--muted)">/ ' + (mg.target_kwh||0).toFixed(1) + ' kWh</span></div>';
    html += _glBar(mg.progress_pct||0, 10);
    html += '<div style="font-size:11px;color:var(--muted);margin-top:4px">';
    if (mg.achieved) html += '\u2705 ' + t('web.goals.achieved', 'Goal reached!') + ' \u2013 ' + (mg.remaining_kwh||0).toFixed(1) + ' kWh ' + t('web.goals.saved', 'saved');
    else html += '\u23f3 ' + t('web.goals.remaining', 'Noch') + ' ' + (mg.remaining_kwh||0).toFixed(1) + ' kWh ' + t('web.goals.to_go', 'to go');
    html += '</div></div>';
    html += '</div>';

    // ── Statistics overview ──────────────────────────────────
    var sav = data.savings_eur || {{}};
    html += '<div class="nilm-metrics" style="margin-top:10px">';
    html += _glCard('\U0001f4ca', t('web.goals.avg_daily', 'Daily Ø'), (data.avg_daily_kwh||0).toFixed(1) + ' kWh', '');
    html += _glCard('\U0001f3af', t('web.goals.target', 'Daily target'), (data.daily_target_kwh||0).toFixed(1) + ' kWh', '90% of Ø');
    html += _glCard('\u2705', t('web.goals.days_under', 'Days under target'), (data.days_under_target||0) + ' / ' + (data.days_total||0), '');
    html += _glCard('\U0001f4b0', t('web.goals.saved_eur', 'Saved'), (sav.weekly||0).toFixed(2) + ' \u20ac/W \u2022 ' + (sav.monthly||0).toFixed(2) + ' \u20ac/M', sav.price_kwh ? sav.price_kwh.toFixed(2) + ' \u20ac/kWh' : '');
    html += '</div>';

    // ── Daily chart (30 days) ────────────────────────────────
    if (data.daily_history && data.daily_history.length > 0) {{
      html += '<div class="card" style="margin-top:10px"><div style="font-size:12px;font-weight:650;color:var(--muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:.5px">' + t('web.goals.daily_chart', '30-day consumption') + '</div>';
      html += '<canvas id="gl-daily-chart" style="width:100%;height:200px"></canvas></div>';
    }}

    // ── Weekly trend chart ───────────────────────────────────
    if (data.weekly_history && data.weekly_history.length > 0) {{
      html += '<div class="card" style="margin-top:10px"><div style="font-size:12px;font-weight:650;color:var(--muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:.5px">' + t('web.goals.weekly_chart', 'Weekly trend') + '</div>';
      html += '<canvas id="gl-weekly-chart" style="width:100%;height:180px"></canvas></div>';
    }}

    // ── Best / Worst days ────────────────────────────────────
    if (data.best_days && data.best_days.length > 0) {{
      html += '<div class="nilm-two-col" style="margin-top:10px">';
      html += '<div class="card"><div style="font-size:12px;font-weight:650;color:var(--muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:.5px">\U0001f3c6 ' + t('web.goals.best_days', 'Top 10 best days') + '</div>';
      data.best_days.forEach(function(d, i) {{
        var medal = ['\U0001f947','\U0001f948','\U0001f949'][i] || '\U0001f539';
        var pct = data.daily_target_kwh > 0 ? Math.round(d.kwh / data.daily_target_kwh * 100) : 0;
        html += '<div style="display:flex;justify-content:space-between;align-items:center;padding:3px 0;border-bottom:1px solid var(--border);font-size:12px">';
        html += '<span>' + medal + ' ' + d.date.slice(5) + '</span>';
        html += '<span style="font-weight:600;color:#4caf50">' + d.kwh.toFixed(1) + ' kWh <span style="color:var(--muted);font-weight:400">(' + pct + '%)</span></span></div>';
      }});
      html += '</div>';
      if (data.worst_days && data.worst_days.length > 0) {{
        html += '<div class="card"><div style="font-size:12px;font-weight:650;color:var(--muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:.5px">\U0001f4a5 ' + t('web.goals.worst_days', 'Top 5 worst days') + '</div>';
        data.worst_days.forEach(function(d) {{
          var pct = data.daily_target_kwh > 0 ? Math.round(d.kwh / data.daily_target_kwh * 100) : 0;
          html += '<div style="display:flex;justify-content:space-between;align-items:center;padding:3px 0;border-bottom:1px solid var(--border);font-size:12px">';
          html += '<span>' + d.date.slice(5) + '</span>';
          html += '<span style="font-weight:600;color:#e53935">' + d.kwh.toFixed(1) + ' kWh <span style="color:var(--muted);font-weight:400">(' + pct + '%)</span></span></div>';
        }});
        html += '</div>';
      }}
      html += '</div>';
    }}

    // ── Badges ───────────────────────────────────────────────
    html += '<div class="card" style="margin-top:10px"><div style="font-size:12px;font-weight:650;color:var(--muted);margin-bottom:8px;text-transform:uppercase;letter-spacing:.5px">\U0001f3c6 ' + t('web.goals.badges', 'Abzeichen') + ' (' + (data.unlocked_count||0) + '/' + (data.total_badges||0) + ')</div>';
    html += '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:8px">';
    (data.badges || []).forEach(function(b) {{
      var unlocked = b.unlocked;
      html += '<div style="background:var(--bg);border:1px solid var(--border);border-radius:8px;padding:8px;text-align:center;opacity:' + (unlocked ? '1' : '0.5') + '">';
      html += '<div style="font-size:26px">' + b.icon + '</div>';
      html += '<div style="font-size:11px;font-weight:600;margin-top:3px">' + (unlocked ? '' : '\U0001f512 ') + esc(b.name) + '</div>';
      html += '<div style="font-size:9px;color:var(--muted);margin-top:1px">' + esc(b.description) + '</div>';
      html += _glBar(b.progress_pct || 0, 4);
      html += '<div style="font-size:9px;color:var(--muted);margin-top:2px">' + Math.round(b.progress_pct || 0) + '%</div>';
      html += '</div>';
    }});
    html += '</div></div>';

    el.innerHTML = html;

    // Draw charts
    if (data.daily_history && data.daily_history.length > 0) {{
      setTimeout(function() {{ _glDrawDailyChart(data.daily_history, data.daily_target_kwh || 0); }}, 50);
    }}
    if (data.weekly_history && data.weekly_history.length > 0) {{
      setTimeout(function() {{ _glDrawWeeklyChart(data.weekly_history); }}, 60);
    }}
  }}

  function _glInitCanvas(id) {{
    var el = document.getElementById(id);
    if (!el) return null;
    var dpr = window.devicePixelRatio || 1;
    var rect = el.getBoundingClientRect();
    el.width = rect.width * dpr; el.height = rect.height * dpr;
    var ctx = el.getContext('2d'); ctx.scale(dpr, dpr);
    return {{ctx: ctx, W: rect.width, H: rect.height}};
  }}

  function _glDrawDailyChart(daily, target) {{
    var c = _glInitCanvas('gl-daily-chart');
    if (!c) return;
    var ctx = c.ctx, W = c.W, H = c.H;
    var muted = getComputedStyle(document.body).getPropertyValue('--muted') || '#999';
    var border = getComputedStyle(document.body).getPropertyValue('--border') || '#e0e0e0';
    var pad = {{top: 10, right: 12, bottom: 28, left: 44}};
    var cW = W - pad.left - pad.right, cH = H - pad.top - pad.bottom;
    var n = daily.length;
    var maxK = Math.max(target * 1.3, Math.max.apply(null, daily.map(function(d){{ return d.kwh; }})) * 1.1) || 1;
    var barW = Math.max(2, (cW / n) - 2);

    // Grid
    ctx.strokeStyle = border; ctx.lineWidth = 0.5;
    ctx.fillStyle = muted; ctx.font = '10px sans-serif'; ctx.textAlign = 'right';
    for (var i = 0; i <= 4; i++) {{
      var gy = pad.top + cH - (cH * i / 4);
      ctx.beginPath(); ctx.moveTo(pad.left, gy); ctx.lineTo(pad.left + cW, gy); ctx.stroke();
      ctx.fillText((maxK * i / 4).toFixed(1), pad.left - 4, gy + 3);
    }}

    // Target line
    if (target > 0) {{
      var ty = pad.top + cH - cH * (target / maxK);
      ctx.strokeStyle = '#4caf50'; ctx.lineWidth = 1.5; ctx.setLineDash([6, 3]);
      ctx.beginPath(); ctx.moveTo(pad.left, ty); ctx.lineTo(pad.left + cW, ty); ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = '#4caf50'; ctx.textAlign = 'left'; ctx.font = '9px sans-serif';
      ctx.fillText(t('web.goals.target', 'Target') + ' ' + target.toFixed(1), pad.left + cW + 2, ty + 3);
    }}

    // Bars
    daily.forEach(function(d, i) {{
      var x = pad.left + (cW * i / n) + 1;
      var bh = cH * (d.kwh / maxK);
      ctx.fillStyle = d.under ? 'rgba(76,175,80,0.7)' : (d.kwh > 0 ? 'rgba(229,57,53,0.6)' : 'rgba(150,150,150,0.2)');
      ctx.fillRect(x, pad.top + cH - bh, barW, bh);
    }});

    // X-axis
    ctx.fillStyle = muted; ctx.font = '9px sans-serif'; ctx.textAlign = 'center';
    var step = Math.max(1, Math.floor(n / (W < 400 ? 5 : 10)));
    for (var i = 0; i < n; i += step) {{
      ctx.fillText(daily[i].date.slice(5), pad.left + (cW * (i + 0.5) / n), pad.top + cH + 16);
    }}
  }}

  function _glDrawWeeklyChart(weekly) {{
    var c = _glInitCanvas('gl-weekly-chart');
    if (!c) return;
    var ctx = c.ctx, W = c.W, H = c.H;
    var muted = getComputedStyle(document.body).getPropertyValue('--muted') || '#999';
    var border = getComputedStyle(document.body).getPropertyValue('--border') || '#e0e0e0';
    var pad = {{top: 10, right: 12, bottom: 28, left: 44}};
    var cW = W - pad.left - pad.right, cH = H - pad.top - pad.bottom;
    var n = weekly.length;
    var maxK = Math.max.apply(null, weekly.map(function(w){{ return Math.max(w.kwh, w.target); }})) * 1.15 || 1;
    var barW = Math.max(8, (cW / n) * 0.6);

    // Grid
    ctx.strokeStyle = border; ctx.lineWidth = 0.5;
    ctx.fillStyle = muted; ctx.font = '10px sans-serif'; ctx.textAlign = 'right';
    for (var i = 0; i <= 4; i++) {{
      var gy = pad.top + cH - (cH * i / 4);
      ctx.beginPath(); ctx.moveTo(pad.left, gy); ctx.lineTo(pad.left + cW, gy); ctx.stroke();
      ctx.fillText((maxK * i / 4).toFixed(0), pad.left - 4, gy + 3);
    }}

    // Bars + target markers
    weekly.forEach(function(w, i) {{
      var x = pad.left + (cW * (i + 0.5) / n) - barW / 2;
      var bh = cH * (w.kwh / maxK);
      var under = w.kwh <= w.target;
      ctx.fillStyle = under ? 'rgba(76,175,80,0.7)' : 'rgba(229,57,53,0.6)';
      ctx.fillRect(x, pad.top + cH - bh, barW, bh);
      // Target marker
      if (w.target > 0) {{
        var ty = pad.top + cH - cH * (w.target / maxK);
        ctx.strokeStyle = '#4caf50'; ctx.lineWidth = 2;
        ctx.beginPath(); ctx.moveTo(x - 2, ty); ctx.lineTo(x + barW + 2, ty); ctx.stroke();
      }}
      // Value on top
      ctx.fillStyle = muted; ctx.font = '9px sans-serif'; ctx.textAlign = 'center';
      ctx.fillText(w.kwh.toFixed(0), x + barW / 2, pad.top + cH - bh - 4);
    }});

    // X-axis (week labels)
    ctx.fillStyle = muted; ctx.font = '9px sans-serif'; ctx.textAlign = 'center';
    weekly.forEach(function(w, i) {{
      var lbl = w.week_start.slice(5);
      ctx.fillText(lbl, pad.left + (cW * (i + 0.5) / n), pad.top + cH + 16);
    }});
  }}

  /* ── Language selector ── */
  (function() {{
    const sel = document.getElementById('lang-select');
    if (sel) sel.value = '{lang}';
  }})();
  function setLanguage(lang) {{
    fetch('/api/run', {{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{action:'set_language',params:{{language:lang}}}})}}
    ).then(()=>window.location.reload()).catch(()=>{{}});
  }}

  // Hide tabs for disabled features
  (function() {{
    fetch('/api/config').then(function(r){{ return r.json(); }}).then(function(cfg) {{
      var ft = cfg.features || {{}};
      // Feature-to-pane mapping (feature key → pane id)
      var map = {{
        solar:'solar', weather:'weather', co2:'co2',
        anomalies:'anomalies', forecast:'forecast',
        ev:'ev', ev_log:'ev_log', smart_sched:'smart_sched',
        tariff:'tariff', battery:'battery', advisor:'advisor',
        goals:'goals', tenants:'tenants', device_control:'control'
      }};
      Object.keys(map).forEach(function(fk) {{
        if (ft[fk]) return; // enabled → keep visible
        var paneId = map[fk];
        // Hide nav button, drawer item, and pane
        document.querySelectorAll('[data-feature="' + fk + '"]').forEach(function(el) {{
          el.style.display = 'none';
        }});
        var pane = document.getElementById('pane-' + paneId);
        if (pane) pane.style.display = 'none';
      }});
      // If current pane got hidden, fall back to live
      if (currentPane && !ft[currentPane] && map[currentPane]) {{
        switchPane('live', document.querySelector('.nav-btn[onclick*="live"]'));
      }}
    }}).catch(function(){{}});
  }})();

  // Always start the persistent /api/state polling regardless of which pane
  // is initially active so cross-tab live updates work from page load.
  startLivePolling();
  // Start background polling for the update banner on the Live tab.
  startUpdateBannerPolling();

  // Restore last pane
  const last = localStorage.getItem('sea_pane');
  if (last && last !== 'live') {{
    const btn = document.querySelector('.nav-btn[onclick*="' + last + '"]');
    if (btn) switchPane(last, btn);
    else startLive();
  }} else {{
    startLive();
  }}

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
    <!-- Topbar (Live/Control links + theme toggle) is hidden when embedded as a Plots tab iframe -->
    <div class="topbar" id="topbar_std" style="display:none">
      <div class="title">@@plots_title@@</div>
      <div class="nav">
        <a id="nav_live" href="/"><span class="ico">🏠</span> <span class="lab">@@web_nav_live@@</span></a>
        <button id="btn_theme" type="button" title="@@web_btn_theme@@">@@web_btn_theme@@</button>
      </div>
    </div>
    <script>
    (function(){
      try {
        // When NOT embedded (opened directly at /plots), show the topbar.
        if (window.top === window.self) {
          var tb = document.getElementById('topbar_std');
          if (tb) tb.style.display = '';
        }
      } catch(e){}
    })();
    </script>

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
            <option value="HZ">Hz</option>
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
    <div class="card">
      <div id="plot1_title" style="font-size:13px;font-weight:650;color:var(--fg);margin:2px 2px 6px;display:none"></div>
      <div id="plot1" class="plot"></div>
    </div>
    <div class="card" id="card2" style="display:none">
      <div id="plot2_title" style="font-size:13px;font-weight:650;color:var(--fg);margin:2px 2px 6px;display:none"></div>
      <div id="plot2" class="plot"></div>
    </div>
    <div class="card" id="card_co2_1" style="display:none">
      <div id="plot_co2_1_title" style="font-size:13px;font-weight:650;color:var(--fg);margin:2px 2px 6px"></div>
      <div id="plot_co2_1" class="plot"></div>
    </div>
    <div class="card" id="card_co2_2" style="display:none">
      <div id="plot_co2_2_title" style="font-size:13px;font-weight:650;color:var(--fg);margin:2px 2px 6px"></div>
      <div id="plot_co2_2" class="plot"></div>
    </div>
    <div class="card" id="card_price_1" style="display:none">
      <div id="plot_price_1_title" style="font-size:13px;font-weight:650;color:var(--fg);margin:2px 2px 6px"></div>
      <div id="plot_price_1" class="plot"></div>
    </div>
    <div class="card" id="card_price_2" style="display:none">
      <div id="plot_price_2_title" style="font-size:13px;font-weight:650;color:var(--fg);margin:2px 2px 6px"></div>
      <div id="plot_price_2" class="plot"></div>
    </div>
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
function t(k, vars){
  let s = (I18N && I18N[k]) ? I18N[k] : k;
  if (vars) {
    Object.keys(vars).forEach(function(kk){ s = s.split('{'+kk+'}').join(String(vars[kk])); });
  }
  return s;
}

// Theme (shared with Live/Control)
try {
  const LS_THEME = 'sea_theme';
  let theme = localStorage.getItem(LS_THEME);
  if (!theme) {
    theme = (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) ? 'dark' : 'light';
  }
  document.documentElement.dataset.theme = theme;
} catch (e) {}

// Listen for theme changes made in the parent dashboard (when /plots is embedded
// as iframe in the Plots tab). The parent writes localStorage 'sea_theme' on toggle,
// which fires a storage event in this iframe since they share origin.
try {
  window.addEventListener('storage', function(ev){
    if (ev && ev.key === 'sea_theme' && ev.newValue) {
      document.documentElement.dataset.theme = ev.newValue;
      try { if (typeof updateThemeButton === 'function') updateThemeButton(); } catch(e){}
      // Re-render plots so line/grid/font colours match the new theme
      try { if (window.__scheduleApplyPlots) window.__scheduleApplyPlots(50); } catch(e){}
    }
  });
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
  // Persist params so user returns to the same view next session (localStorage
  // is shared between /plots standalone and the embedded iframe in the Plots tab).
  try {
    const obj = {};
    u.searchParams.forEach((v,k)=>{ obj[k]=v; });
    localStorage.setItem('sea_plots_qp', JSON.stringify(obj));
  } catch(e){}
}
// First-visit default: kWh view for the last 24 hours.
// If the URL has no plots-relevant params AND localStorage has something saved
// from a previous session, restore those. Otherwise fall back to the default.
function restorePlotsDefaults() {
  const u = new URL(window.location.href);
  const has = u.searchParams.has('view') || u.searchParams.has('metric') || u.searchParams.has('preset') || u.searchParams.has('mode') || u.searchParams.has('start');
  if (has) return;
  // Try last-session params from localStorage
  try {
    const saved = JSON.parse(localStorage.getItem('sea_plots_qp') || 'null');
    if (saved && typeof saved === 'object' && Object.keys(saved).length > 0) {
      Object.keys(saved).forEach(k => u.searchParams.set(k, String(saved[k])));
      history.replaceState(null, '', u.toString());
      return;
    }
  } catch(e){}
  // First-ever visit: default to kWh · hourly buckets · last 24 h
  u.searchParams.set('view', 'kwh');
  u.searchParams.set('mode', 'hours');
  u.searchParams.set('len', '24');
  u.searchParams.set('unit', 'hours');
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

let __plots_fetch_ctrl = null;
async function fetchJsonWithTimeout(url, timeoutMs=60000){
  // Abort any previous in-flight plots fetch so that the newest selection wins
  try { if (__plots_fetch_ctrl) __plots_fetch_ctrl.abort('superseded'); } catch(e) {}
  const ctrl = new AbortController();
  __plots_fetch_ctrl = ctrl;
  const to = setTimeout(()=>ctrl.abort('timeout'), timeoutMs);
  try {
    const res = await fetch(url, {signal: ctrl.signal, cache: 'no-store'});
    if (!res.ok) throw new Error('HTTP ' + res.status);
    return await res.json();
  } catch(e) {
    if (e && e.name === 'AbortError') {
      // Distinguish user-triggered supersede from real timeout
      const reason = (ctrl.signal && ctrl.signal.reason) || '';
      if (reason === 'superseded') { const err = new Error('__superseded__'); err.superseded = true; throw err; }
      throw new Error('Timeout (' + Math.round(timeoutMs/1000) + 's) — reduce time range');
    }
    throw e;
  } finally {
    clearTimeout(to);
    if (__plots_fetch_ctrl === ctrl) __plots_fetch_ctrl = null;
  }
}

function cssVar(name){
  try { return getComputedStyle(document.documentElement).getPropertyValue(name).trim(); } catch (e) { return ''; }
}
// Plots tab always uses the simple static view (no zoom/pan) regardless of
// viewport – per user preference, identical to the mobile experience.
function isMobileView(){
  return true;
}
function plotlyBaseLayout(extra){
  const fg = cssVar('--fg') || '#111827';
  const border = cssVar('--border') || 'rgba(0,0,0,0.12)';
  const grid = border;
  const mobile = isMobileView();
  const base = {
    paper_bgcolor: 'rgba(0,0,0,0)',
    plot_bgcolor: 'rgba(0,0,0,0)',
    font: { color: fg },
    margin: { l:55, r:20, t:30, b:50 },
    xaxis: { gridcolor: grid, zerolinecolor: grid, fixedrange: mobile },
    yaxis: { gridcolor: grid, zerolinecolor: grid, fixedrange: mobile },
    legend: { orientation: 'h', font: { color: fg } },
    dragmode: mobile ? false : 'zoom',
  };
  const merged = Object.assign(base, extra || {});
  // Ensure fixedrange/dragmode propagate even when xaxis/yaxis are overridden by caller
  if (mobile) {
    if (merged.xaxis) merged.xaxis.fixedrange = true;
    if (merged.yaxis) merged.yaxis.fixedrange = true;
    merged.dragmode = false;
  }
  return merged;
}
// Unified config: no mode bar / no scroll zoom / no double-click on mobile.
function plotlyConfig(extra){
  const mobile = isMobileView();
  const base = {
    responsive: true,
    displaylogo: false,
    displayModeBar: mobile ? false : 'hover',
    scrollZoom: !mobile,
    doubleClick: mobile ? false : 'reset+autosize',
    staticPlot: false,  // keep hover on mobile
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
    const xsLab = data.labels || [];
    const kwhTraces = data.traces || [];
    // Nicer x-axis: no fixed 45°, let Plotly auto-rotate + reserve bottom margin
    const kwhLayout = (yTitle, extra) => plotlyBaseLayout(Object.assign({
      margin:{l:60,r:20,t:30,b:70},
      xaxis:{ automargin:true, tickangle:'auto', nticks: Math.min(12, xsLab.length) },
      yaxis:{title: yTitle, automargin:true}
    }, extra || {}));

    function drawKwhForDevice(divId, titleDivId, tr) {
      try {
        const te = document.getElementById(titleDivId);
        if (te) { te.textContent = (tr && tr.name) ? tr.name : ''; te.style.display = (tr && tr.name) ? 'block' : 'none'; }
      } catch(e) {}
      Plotly.newPlot(
        divId,
        [{type:'bar', name: tr.name, x: xsLab, y: tr.y, marker:{color:'#6aa7ff'}}],
        kwhLayout('kWh'),
        plotlyConfig()
      );
    }

    // Traffic-light helper (green/yellow/red) by value with thresholds
    function tlColor(v, green_lt, red_ge) {
      if (v == null) return '#9ca3af';
      if (v < green_lt) return '#22c55e';   // green
      if (v >= red_ge) return '#ef4444';    // red
      return '#eab308';                       // yellow
    }
    // Percentile-based thresholds for arrays with no absolute scale (prices)
    function autoThresholds(arr) {
      const nums = arr.filter(v => v != null && !isNaN(v)).slice().sort((a,b)=>a-b);
      if (nums.length < 2) return [null, null];
      const p33 = nums[Math.floor(nums.length * 0.33)];
      const p66 = nums[Math.floor(nums.length * 0.66)];
      return [p33, p66];
    }

    // Plot 1: first device
    if (kwhTraces.length >= 1) {
      drawKwhForDevice('plot1', 'plot1_title', kwhTraces[0]);
    } else {
      Plotly.newPlot('plot1', [], kwhLayout('kWh'), plotlyConfig());
    }

    // Plot 2: second device (own card, separate, not grouped)
    if (kwhTraces.length >= 2) {
      document.getElementById('card2').style.display = '';
      drawKwhForDevice('plot2', 'plot2_title', kwhTraces[1]);
    } else {
      document.getElementById('card2').style.display = 'none';
    }

    // CO2 bar charts (per device) – colour by g/kWh intensity (green < thr, red >= dirty)
    const co2Int = data.co2_intensity_g_per_kwh || [];
    const gThr = (data.co2_green_thr != null) ? data.co2_green_thr : 150;
    const dThr = (data.co2_dirty_thr != null) ? data.co2_dirty_thr : 400;
    const co2Devs = data.co2_per_device || [];
    function renderCo2Card(idx, devData) {
      const cardEl = document.getElementById('card_co2_' + idx);
      const titleEl = document.getElementById('plot_co2_' + idx + '_title');
      const plotId = 'plot_co2_' + idx;
      if (!devData || !(devData.g||[]).some(v => v != null)) { cardEl.style.display = 'none'; return; }
      cardEl.style.display = '';
      titleEl.textContent = t('web.plots.co2.title', {
        name: (devData.name || devData.key || ''),
        zone: (data.co2_zone ? ' (' + data.co2_zone + ')' : ''),
        green: gThr, dirty: dThr
      });
      const yArr = (devData.g || []).map(v => (v==null ? 0 : v));
      const colors = co2Int.map(v => tlColor(v, gThr, dThr));
      const custom = co2Int.map((v,i) => [v==null?'—':v, yArr[i]]);
      Plotly.newPlot(
        plotId,
        [{type:'bar', name:'CO₂', x: xsLab, y: yArr, marker:{color:colors},
          customdata: custom,
          hovertemplate:'%{x}<br>%{customdata[0]} g/kWh · Σ %{customdata[1]} g<extra></extra>'}],
        kwhLayout('g CO₂'),
        plotlyConfig()
      );
    }
    renderCo2Card(1, co2Devs[0]);
    renderCo2Card(2, co2Devs[1]);

    // Price bar charts (per device) – colour by ct/kWh (percentile thresholds)
    const priceCt = data.price_ct_kwh || [];
    const [p33, p66] = autoThresholds(priceCt);
    const gThrP = (p33 != null) ? p33 : 10;
    const rThrP = (p66 != null) ? p66 : 25;
    const priceDevs = data.price_per_device || [];
    const surchInfo = data.price_surcharges_included
      ? t('web.plots.price.surcharges.on', {
          ct: (data.price_surcharge_ct || 0),
          vat: (data.price_vat_pct ? t('web.plots.price.surcharges.vat', {pct: data.price_vat_pct}) : '')
        })
      : t('web.plots.price.surcharges.off');
    const fixedCt = data.price_fixed_ct_kwh;
    function renderPriceCard(idx, devData) {
      const cardEl = document.getElementById('card_price_' + idx);
      const titleEl = document.getElementById('plot_price_' + idx + '_title');
      const plotId = 'plot_price_' + idx;
      if (!devData || !(devData.eur||[]).some(v => v != null)) { cardEl.style.display = 'none'; return; }
      cardEl.style.display = '';
      const fixInfo = (fixedCt != null) ? t('web.plots.price.fixed.info', {ct: fixedCt.toFixed(2)}) : '';
      titleEl.textContent = t('web.plots.price.title', {
        name: (devData.name || devData.key || ''),
        zone: (data.price_zone ? ' (' + data.price_zone + ')' : ''),
        green: gThrP.toFixed(1), red: rThrP.toFixed(1)
      }) + surchInfo + fixInfo;
      const yArr = (devData.eur || []).map(v => (v==null ? 0 : v));
      const colorsP = priceCt.map(v => tlColor(v, gThrP, rThrP));
      const custom = priceCt.map((v,i) => [v==null?'—':v, yArr[i]]);
      const dynName = t('web.plots.trace.dynamic');
      const fixName = t('web.plots.trace.fixed');
      const traces = [
        {type:'bar', name: dynName, x: xsLab, y: yArr, marker:{color:colorsP},
          customdata: custom,
          hovertemplate:'%{x}<br>%{customdata[0]} ct/kWh · Σ %{customdata[1]} €<extra>' + dynName + '</extra>'}
      ];
      const fxArr = (devData.eur_fixed || []).map(v => (v==null ? 0 : v));
      const hasFixed = (devData.eur_fixed || []).some(v => v != null && v > 0);
      if (hasFixed) {
        traces.push({type:'bar', name: fixName, x: xsLab, y: fxArr, marker:{color:'#9ca3af'},
          hovertemplate:'%{x}<br>' + (fixedCt != null ? fixedCt.toFixed(2) : '?') + ' ct/kWh · Σ %{y} €<extra>' + fixName + '</extra>'});
      }
      Plotly.newPlot(
        plotId,
        traces,
        kwhLayout('EUR', {barmode:'group'}),
        plotlyConfig()
      );
    }
    renderPriceCard(1, priceDevs[0]);
    renderPriceCard(2, priceDevs[1]);

    return;
  } else {
    // Hide kWh-only cards when not in kwh view
    ['card_co2_1','card_co2_2','card_price_1','card_price_2'].forEach(function(id) {
      var el = document.getElementById(id); if (el) el.style.display = 'none';
    });
  }

  const devs = data.devices || [];
  if (!devs || devs.length === 0) {
    document.getElementById('meta').textContent = t('web.plots.no_data');
    return;
  }

  function plotInto(div, dev) {
    if (!dev) return;
    // Per-plot device title (show device name above each plot)
    try {
      const titleEl = document.getElementById(div + '_title');
      if (titleEl) {
        const devName = (dev && (dev.name || dev.key)) ? String(dev.name || dev.key) : '';
        titleEl.textContent = devName;
        titleEl.style.display = devName ? 'block' : 'none';
      }
    } catch(e) {}
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
      // Plot phases (L1/L2/L3, and N for neutral current in "A" metric).
      // Ensure stable order L1 → L2 → L3 → N so Plotly assigns matching colours.
      const order = ['L1','L2','L3','N'];
      const keys = Object.keys(dev.phases).sort((a,b)=>{
        const ia = order.indexOf(a), ib = order.indexOf(b);
        return (ia<0?99:ia) - (ib<0?99:ib);
      });
      keys.forEach(k=>{
        const p = dev.phases[k];
        if (!p) return;
        const isN = (String(k).toUpperCase() === 'N');
        const label = isN ? t('web.plots.phase.n') : k;
        const tr = {
          type:'scatter', mode:'lines',
          name: label,
          x: p.x || xs,
          y: applyVarCosphiFilters((p.x || xs), (p.y || []), metricKey, opts)
        };
        if (isN) {
          tr.line = { color: '#9ca3af', width: 1.5, dash: 'dash' };
        }
        traces.push(tr);
      });
    } else {
      // Total only (default). If phases selected but unavailable, fall back silently.
      traces = [{type:'scatter', mode:'lines', name: t('web.plots.series.total'), x: xs, y: applyVarCosphiFilters(xs, ys, metricKey, opts)}];
    }

    Plotly.newPlot(
      div,
      traces,
      plotlyBaseLayout({xaxis:{title:t('web.axis.time')}, yaxis:{title: metric}}),
      plotlyConfig()
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
  // Restore last session's plots params (view/metric/range/etc.), or
  // default to kWh · hours · last 24 h on first-ever visit.
  try { restorePlotsDefaults(); } catch(e){}
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
      if (e && e.superseded) return;
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
    throw new Error(t || "Response is not JSON");
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
    <a href="${url}" target="_blank" style="display:inline-flex;align-items:center;gap:4px;font-size:12px;padding:8px 16px;border-radius:10px;border:1px solid rgba(106,167,255,0.35);background:rgba(106,167,255,0.12);color:var(--accent);text-decoration:none;font-weight:600;white-space:nowrap;min-height:40px">Open</a>
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
                        "i_n": _compute_i_n(float(latest.get("i_n") or 0), ia, ib, ic, va, vb, vc),
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
                            "i_n": _compute_i_n(float(p.get("i_n") or 0), ia, ib, ic, va, vb, vc),
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

            if path_only.startswith("/api/nilm_detail"):
                try:
                    store = self.dashboard.store
                    clusters = list(getattr(store, "_nilm_clusters", []))
                    trans_count = int(getattr(store, "_nilm_transition_count", 0))
                    transitions = list(getattr(store, "_nilm_transitions", []))
                    device_count = int(getattr(store, "_nilm_device_count", 0))
                    hourly = [0] * 24
                    import datetime as _dt
                    for tr in transitions:
                        try:
                            hourly[_dt.datetime.fromtimestamp(tr["ts"]).hour] += 1
                        except Exception:
                            pass
                    from shelly_analyzer.services.appliance_detector import APPLIANCES as _APPL
                    _cat_map = {a.id: a.category for a in _APPL}
                    categories = {}
                    for c in clusters:
                        cat = _cat_map.get(c.get("matched_appliance", ""), "unknown")
                        categories[cat] = categories.get(cat, 0) + c.get("count", 0)
                    device_stats = {}
                    for c in clusters:
                        dk = c.get("device_key", "")
                        if dk not in device_stats:
                            device_stats[dk] = {"cluster_count": 0, "total_events": 0, "top_appliances": []}
                        device_stats[dk]["cluster_count"] += 1
                        device_stats[dk]["total_events"] += c.get("count", 0)
                        if c.get("matched_appliance"):
                            device_stats[dk]["top_appliances"].append({
                                "appliance": c["matched_appliance"], "icon": c.get("icon", ""),
                                "centroid_w": c.get("centroid_w", 0), "count": c.get("count", 0),
                            })
                    sigs = [{"id": a.id, "icon": a.icon, "category": a.category,
                             "power_min": a.power_min, "power_max": a.power_max,
                             "pattern_type": a.pattern_type, "typical_duration_min": a.typical_duration_min}
                            for a in _APPL]
                    payload = {
                        "ok": True, "cluster_count": len(clusters), "transition_count": trans_count,
                        "device_count": device_count, "clusters": clusters, "transitions": transitions[:200],
                        "hourly_distribution": hourly, "device_stats": device_stats, "device_names": {},
                        "categories": categories, "signatures": sigs,
                    }
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

            # --- Smart Schedule API ---
            if path_only.startswith("/api/smart_schedule"):
                try:
                    _sp = urlparse(self.path)
                    _sqs = parse_qs(_sp.query or "")
                    _sparams = {k: (v[0] if isinstance(v, list) and v else v) for k, v in _sqs.items()}
                    payload = self.dashboard.on_action("smart_schedule", _sparams)
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

            # --- EV Sessions API ---
            if path_only.startswith("/api/ev_sessions"):
                try:
                    _sp = urlparse(self.path)
                    _sqs = parse_qs(_sp.query or "")
                    _sparams = {k: (v[0] if isinstance(v, list) and v else v) for k, v in _sqs.items()}
                    payload = self.dashboard.on_action("ev_sessions", _sparams)
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

            # --- Tariff Compare API ---
            if path_only.startswith("/api/tariff_compare"):
                try:
                    _sp = urlparse(self.path)
                    _sqs = parse_qs(_sp.query or "")
                    _sparams = {k: (v[0] if isinstance(v, list) and v else v) for k, v in _sqs.items()}
                    payload = self.dashboard.on_action("tariff_compare", _sparams)
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

            # --- Battery API ---
            if path_only.startswith("/api/battery"):
                try:
                    _sp = urlparse(self.path)
                    _sqs = parse_qs(_sp.query or "")
                    _sparams = {k: (v[0] if isinstance(v, list) and v else v) for k, v in _sqs.items()}
                    payload = self.dashboard.on_action("battery", _sparams)
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

            # --- Advisor API ---
            if path_only.startswith("/api/advisor"):
                try:
                    _sp = urlparse(self.path)
                    _sqs = parse_qs(_sp.query or "")
                    _sparams = {k: (v[0] if isinstance(v, list) and v else v) for k, v in _sqs.items()}
                    payload = self.dashboard.on_action("advisor", _sparams)
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

            # --- Goals API ---
            if path_only.startswith("/api/goals"):
                try:
                    _sp = urlparse(self.path)
                    _sqs = parse_qs(_sp.query or "")
                    _sparams = {k: (v[0] if isinstance(v, list) and v else v) for k, v in _sqs.items()}
                    payload = self.dashboard.on_action("goals", _sparams)
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

            # --- API v1 ---
            if path_only.startswith("/api/v1/"):
                try:
                    from shelly_analyzer.services import api_v1
                    payload = api_v1.handle_v1_request(self.path, self.dashboard)
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

            # --- Prometheus Metrics ---
            if path_only == "/metrics":
                try:
                    from shelly_analyzer.services.prometheus_export import generate_metrics
                    _dash = self.dashboard
                    body = generate_metrics(
                        getattr(_dash, 'live_state_store', {}),
                        getattr(getattr(_dash, 'cfg', None), 'devices', []),
                        getattr(_dash, 'cfg', None),
                    ).encode("utf-8")
                except Exception as e:
                    body = f"# error: {e}\n".encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            # --- Widget API (compact JSON for iOS Scriptable) ---
            if path_only.startswith("/api/widget"):
                try:
                    payload = self.dashboard.on_action("widget", {})
                except Exception as e:
                    payload = {"ok": False, "error": str(e)}
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            # --- Scriptable widget script (JS download) ---
            if path_only == "/widget.js":
                body = self.dashboard.get_widget_script().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/javascript; charset=utf-8")
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
                    if action == "set_language":
                        lang = str(params.get("language", "de") or "de")
                        try:
                            if hasattr(self.dashboard, "config") and self.dashboard.config:
                                self.dashboard.config["language"] = lang
                            payload = {"ok": True, "language": lang}
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
                    elif action in {"get_switch", "set_switch", "toggle_switch", "get_freeze", "set_freeze", "toggle_freeze"}:
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
        ssl_mode: str = "auto",
        ssl_cert: str = "",
        ssl_key: str = "",
        widget_domain: str = "",
        widget_devices: str = "",
    ) -> None:
        self.store = store
        self.port = int(port)
        self.ssl_mode = str(ssl_mode or "auto").strip().lower()
        self.ssl_cert = str(ssl_cert or "").strip()
        self.ssl_key = str(ssl_key or "").strip()
        self.widget_domain = str(widget_domain or "").strip()
        self.widget_devices = str(widget_devices or "").strip()
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

        _rendered_html = _render_template(
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
                "web_tab_export": _t(self.lang, "web.tab.export"),
                # New feature pane titles
                "smart_sched_title": _t(self.lang, "smart_sched.title") if _t(self.lang, "smart_sched.title") != "smart_sched.title" else "Smart scheduling",
                "ev_log_title": _t(self.lang, "ev_log.title") if _t(self.lang, "ev_log.title") != "ev_log.title" else "EV charge log",
                "tariff_title": _t(self.lang, "tariff.title") if _t(self.lang, "tariff.title") != "tariff.title" else "Tariff comparison",
                "battery_title": _t(self.lang, "battery.title") if _t(self.lang, "battery.title") != "battery.title" else "Battery storage",
                "advisor_title": _t(self.lang, "advisor.title") if _t(self.lang, "advisor.title") != "advisor.title" else "AI energy advisor",
                "goals_title": _t(self.lang, "goals.title") if _t(self.lang, "goals.title") != "goals.title" else "Goals & achievements",
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
        self._html_bytes = _rendered_html.encode("utf-8")
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
        elif ext in {".csv"}:
            ctype = "text/csv; charset=utf-8"
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

    def get_widget_script(self) -> str:
        """Return the Scriptable JS widget script with the server URL baked in."""
        script = _SCRIPTABLE_WIDGET_JS
        # Bake in the known domain:port as default
        if self.widget_domain:
            default_addr = f"{self.widget_domain}:{self.port}"
            script = script.replace(
                '192.168.1.50:8765',
                default_addr,
            )
        return script

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

        # Wrap with SSL (unless ssl_mode == "off")
        self._is_https = False
        _log = logging.getLogger(__name__)
        if self.ssl_mode != "off":
            try:
                import ssl as _ssl
                ctx = _ssl.SSLContext(_ssl.PROTOCOL_TLS_SERVER)
                if self.ssl_mode == "custom" and self.ssl_cert and self.ssl_key:
                    # User-provided certificate (e.g. Let's Encrypt)
                    ctx.load_cert_chain(self.ssl_cert, self.ssl_key)
                    _log.info("Web dashboard HTTPS enabled (custom certificate: %s)", self.ssl_cert)
                else:
                    # Auto: self-signed certificate (expiry-aware)
                    from shelly_analyzer.web.ssl_utils import ensure_ssl_cert
                    _cert_dir = (self.out_dir or Path(".")) / "data" / "runtime" / "ssl"
                    _cert, _key, _info = ensure_ssl_cert(_cert_dir)
                    ctx.load_cert_chain(str(_cert), str(_key))
                    _log.info(
                        "Web dashboard HTTPS enabled (self-signed cert, %s days remaining)",
                        _info.days_remaining if _info.days_remaining is not None else "?",
                    )
                self._httpd.socket = ctx.wrap_socket(self._httpd.socket, server_side=True)
                self._is_https = True
            except Exception as e:
                _log.warning(
                    "HTTPS not available (GPS may not work): %s – falling back to HTTP", e
                )
        else:
            _log.info("Web dashboard running in HTTP mode (SSL disabled)")

        # Auto-detect widget domain from SSL cert CN if not set
        if not self.widget_domain and self.ssl_cert and self._is_https:
            try:
                # Try cryptography library first (cross-platform)
                from cryptography import x509 as _x509
                _cert_pem = Path(self.ssl_cert).read_bytes()
                _cert_obj = _x509.load_pem_x509_certificate(_cert_pem)
                _cn_attrs = _cert_obj.subject.get_attributes_for_oid(_x509.oid.NameOID.COMMON_NAME)
                if _cn_attrs:
                    self.widget_domain = str(_cn_attrs[0].value)
                    _log.info("Widget domain auto-detected from cert: %s", self.widget_domain)
            except ImportError:
                # Fallback: openssl CLI
                try:
                    import subprocess as _sp, shutil as _sh
                    if _sh.which("openssl"):
                        _cn = _sp.check_output(
                            ["openssl", "x509", "-in", self.ssl_cert, "-noout", "-subject"],
                            timeout=5, text=True
                        ).strip()
                        if "CN=" in _cn:
                            self.widget_domain = _cn.split("CN=")[-1].strip()
                            _log.info("Widget domain auto-detected from cert: %s", self.widget_domain)
                except Exception:
                    pass
            except Exception:
                pass

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
