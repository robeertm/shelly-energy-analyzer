from __future__ import annotations
import shutil
from dataclasses import asdict
import json
import io
import math
import re
import queue
import sys
import os
import subprocess
import socket
import logging
import threading
import urllib.parse
import urllib.request
import urllib.error
import ssl
import time
from collections import deque
from datetime import datetime
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
import tkinter as tk
import tkinter.font as tkfont
from tkinter import ttk, filedialog, messagebox
import pandas as pd
import numpy as np
import requests
import qrcode
from PIL import Image, ImageTk
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
from matplotlib.figure import Figure
import matplotlib.dates as mdates
from shelly_analyzer import __version__
from shelly_analyzer.i18n import t as _t, normalize_lang, LANGS, format_date_local, format_number_local
from shelly_analyzer.core.energy import filter_by_time, calculate_energy
from shelly_analyzer.core.stats import daily_kwh, weekly_kwh, monthly_kwh
from shelly_analyzer.io.config import (
    AppConfig,
    BillingConfig,
    BillingParty,
    CsvPackConfig,
    DeviceConfig,
    DownloadConfig,
    PricingConfig,
    UiConfig,
    AlertRule,
    load_config,
    save_config,
)
from shelly_analyzer.io.storage import Storage
from shelly_analyzer.io.http import ShellyHttp, HttpConfig, get_shelly_status, get_switch_status, set_switch_state
from shelly_analyzer.io.logging_setup import get_log_path
from shelly_analyzer.services.compute import ComputedDevice, load_device, summarize
from shelly_analyzer.services.export import (
    ReportTotals,
    export_figure_png,
    export_pdf_invoice,
    export_pdf_summary,
    export_pdf_energy_report_variant1,
    export_to_excel,
    InvoiceLine,
)
from shelly_analyzer.services.live import LivePoller, MultiLivePoller, LiveSample
from shelly_analyzer.services.sync import sync_all
from shelly_analyzer.services.webdash import LivePoint, LiveStateStore, LiveWebDashboard
from shelly_analyzer.services.discovery import probe_device
from shelly_analyzer.services.mdns import discover_shelly_mdns
PLOTS_MODES = [
    ("all", "All"),
    ("days", "Days"),
    ("weeks", "Weeks"),
    ("months", "Months"),
]
AUTOSYNC_INTERVAL_OPTIONS = [1, 2, 3, 6, 12, 24]
AUTOSYNC_MODE_OPTIONS = [
    ("incremental", "Inkrementell"),
    ("day", "Day"),
    ("week", "Week"),
    ("month", "Month"),
]
INVOICE_PERIOD_OPTIONS = [
    ("custom", "Custom"),
    ("day", "Tag"),
    ("week", "Woche"),
    ("month", "Monat"),
    ("year", "Jahr"),
]
def _fmt_eur(x: float) -> str:
    return f"{x:,.2f} €".replace(",", "X").replace(".", ",").replace("X", ".")
def _fmt_kwh(x: float) -> str:
    return f"{x:,.3f} kWh".replace(",", "X").replace(".", ",").replace("X", ".")
def _parse_date_flexible(s: str) -> Optional[pd.Timestamp]:
    """Parse a date string in common formats.
    Accepts:
    - YYYY-MM-DD
    - DD.MM.YYYY
    - DD.MM.YY
    - Any pandas-parseable date/time
    """
    s = (s or "").strip()
    if not s:
        return None
    # Fast-path: common German format
    for fmt in ("%d.%m.%Y", "%d.%m.%y", "%Y-%m-%d"):
        try:
            from datetime import datetime
            return pd.Timestamp(datetime.strptime(s, fmt))
        except Exception:
            pass
    try:
        return pd.to_datetime(s, errors="raise")
    except Exception:
        return None
def _period_bounds(anchor: pd.Timestamp, period: str) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """Return inclusive [start, end] bounds for a given period containing anchor."""
    a = pd.Timestamp(anchor).to_pydatetime()
    t = pd.Timestamp(a).normalize()
    if period == "day":
        start = t
        end = t + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        return start, end
    if period == "week":
        # ISO week: Monday..Sunday
        start = t - pd.Timedelta(days=int(t.weekday()))
        end = start + pd.Timedelta(days=7) - pd.Timedelta(seconds=1)
        return start, end
    if period == "month":
        start = t.replace(day=1)
        end = (start + pd.offsets.MonthBegin(1)) - pd.Timedelta(seconds=1)
        return start, end
    if period == "year":
        start = pd.Timestamp(year=int(t.year), month=1, day=1)
        end = pd.Timestamp(year=int(t.year) + 1, month=1, day=1) - pd.Timedelta(seconds=1)
        return start, end
    # Fallback
    return t, t + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        # config loaded below; title set after language is known
        # Reasonable default; we also size plots dynamically.
        self.geometry("1400x900")
        self.project_root = Path.cwd()
        self.cfg_path = self.project_root / "config.json"
        self.cfg = load_config(self.cfg_path)
        self.lang = normalize_lang(getattr(self.cfg.ui, "language", "de"))
        self.t = lambda k, **kw: _t(self.lang, k, **kw)
        self.title(f"{self.t('app.title')} {__version__}")
        # Web dashboard authentication is intentionally disabled (LAN-only use).
        self.storage = Storage(self.project_root / "data")
        # Auto-import legacy CSV data from previous installs (best-effort), so
        # users upgrading to a new folder don't have to manually copy the data/
        # directory.
        try:
            keys = [d.key for d in getattr(self.cfg, "devices", []) if getattr(d, "key", None)]
            if keys:
                self.storage.auto_import_from_previous_installs(keys)
        except Exception:
            pass
        # Computed data
        self.computed: Dict[str, ComputedDevice] = {}
        # Background sync
        self._sync_q: "queue.Queue[str]" = queue.Queue()
        self._sync_thread: Optional[threading.Thread] = None
        # UI commands from the web dashboard (handled on Tk main thread)
        # queue items are tuples: (cmd:str, payload:any)
        self._ui_cmd_q: "queue.Queue[tuple]" = queue.Queue()
        # Autosync
        self._autosync_next_ts: Optional[float] = None
        # Live (poll all configured devices; UI shows them paged 2 at a time)
        # We keep a list for backward compatibility; in v5.8+ we usually run
        # a single MultiLivePoller in that list.
        self._live_pollers: List[Any] = []
        self._live_last_redraw = 0.0
        self._live_max_points = 0
        self._live_frozen = tk.BooleanVar(value=False)
        # Mirror freeze state as a plain bool so the web server thread can read it
        # without touching Tk variables.
        self._live_frozen_state: bool = False
        # Live diagnostics (offline detection)
        # device_key -> {last_ok_ts:int|None, last_err_ts:int|None, err_count:int, last_err:str|None}
        self._live_diag: Dict[str, Dict[str, Any]] = {}
        # Alert state per rule_id
        self._alert_state: Dict[str, Dict[str, Any]] = {}
        # Log file path (set by run_gui via logging_setup)
        self._log_path: Optional[Path] = get_log_path()
        # device_key -> metric -> ringbuffer of (ts,val)
        self._live_series: Dict[str, Dict[str, Any]] = {}
        # Today kWh (base from imported CSVs + live delta)
        # base_* are recomputed after sync/reload; live_* are accumulated incrementally.
        self._today_base_kwh: Dict[str, float] = {}
        self._today_base_last_ts: Dict[str, int] = {}
        self._today_live_state: Dict[str, Dict[str, Any]] = {}  # {date, kwh, last_ts, last_p}
        # Live Web Dashboard
        self._live_state_store: LiveStateStore = LiveStateStore(max_points=1200)
        self._live_web: Optional[LiveWebDashboard] = None
        # Plots
        self._plots_mode = tk.StringVar(value="days")
        self._plots_start = tk.StringVar(value="")
        self._plots_end = tk.StringVar(value="")
        self._plots_last_n = tk.IntVar(value=7)
        self._plots_last_unit = tk.StringVar(value="days")
        # Plots view: kWh bars vs W/V/A time series (separate engines)
        self._plots_view = tk.StringVar(value="timeseries")
        self._wva_metric = tk.StringVar(value="W")
        self._wva_len = tk.DoubleVar(value=24.0)
        self._wva_unit = tk.StringVar(value="hours")
        # Display variable so units can be localized while keeping internal values stable.
        self._wva_unit_display = tk.StringVar(value="hours")

        # Plots debug: show which CSV columns were mapped to phases/total.
        self._plots_debug_mapping_enabled = tk.BooleanVar(value=False)
        self._plots_debug_mapping_text = tk.StringVar(value="")
        # (metric, device_key) -> mapping text
        self._plots_last_mapping: Dict[Tuple[str, str], str] = {}
        self._stats_canvases: Dict[str, FigureCanvasTkAgg] = {}
        self._stats_axes: Dict[str, any] = {}
        self._stats_figs: Dict[str, Figure] = {}
        # Legacy Plotly URL-based plots (kept for compatibility; not used in classic mode)
        self._plots_url_vars: Dict[str, tk.StringVar] = {}
        self._plots_ts_pages: List[Tuple[str, List[str]]] = []
        self._plots_kwh_pages: List[Tuple[str, List[str]]] = []
        self._plots_relayout_job: Optional[str] = None
        self._plots_resize_watch_job: Optional[str] = None
        self._plots_last_size_sig: Optional[Tuple[int,int,str]] = None
        self._live_relayout_job: Optional[str] = None

        # New Plots-tab layout: metric tabs (kWh/V/A/W) and device sub-tabs.
        # metric_key -> device_key -> fig/canvas
        self._plots_figs2: Dict[str, Dict[str, Figure]] = {}
        self._plots_axes2: Dict[str, Dict[str, Any]] = {}
        self._plots_canvases2: Dict[str, Dict[str, FigureCanvasTkAgg]] = {}
        self._plots_metric_nb: Optional[ttk.Notebook] = None
        self._plots_device_nb: Dict[str, ttk.Notebook] = {}
        self._plots_device_order: Dict[str, List[str]] = {}
        # Live canvases/axes
        self._live_figs: Dict[str, Dict[str, Figure]] = {}
        self._live_axes: Dict[str, Dict[str, any]] = {}
        self._live_canvases: Dict[str, Dict[str, FigureCanvasTkAgg]] = {}
        self._build_ui()
        # Apply DPI/monitor-aware scaling for fonts and plots.
        self._init_ui_scaling()
        # Apply UI scaling for current monitor and watch for DPI changes.
        try:
            self._apply_ui_scaling(force=True)
        except Exception:
            pass
        try:
            self._start_dpi_watch()
        except Exception:
            pass
        # Resize robustness on macOS fullscreen/HiDPI: Tk sometimes delivers
        # intermediate sizes (especially when entering/leaving fullscreen).
        # We therefore schedule a short resize-watch whenever the main window
        # or the active tab changes.
        try:
            self.notebook.bind('<<NotebookTabChanged>>', lambda _e=None: self._kick_plots_resize_watch())
        except Exception:
            pass
        try:
            self.bind('<Configure>', lambda _e=None: self._kick_plots_resize_watch())
        except Exception:
            pass
        # Auto-probe existing devices once at startup so mixed device types (e.g. Plus 1PM)
        # are recognized without manual config.
        try:
            self.after(300, self._probe_devices_on_startup)
        except Exception:
            pass
        # Load existing CSV data shortly after startup so Plots/Stats have content.
        # (Do this after UI is built to avoid blocking the constructor.)
        try:
            self.after(600, self._reload_data)
        except Exception:
            pass
        self._drain_queues_loop()

    def _probe_devices_on_startup(self) -> None:
        """Best-effort device type detection for all configured devices.

        Runs in the background and writes back to config.json if detection succeeds.
        """

        def _worker() -> None:
            try:
                devs_new: List[DeviceConfig] = []
                changed = False
                for d in list(getattr(self.cfg, "devices", [])):
                    try:
                        disc = probe_device(d.host, timeout_seconds=2.0)
                    except Exception:
                        devs_new.append(d)
                        continue

                    # Keep user-provided name/key, but update technical fields.
                    nd = DeviceConfig(
                        key=d.key,
                        name=d.name,
                        host=d.host,
                        em_id=int(disc.component_id or getattr(d, "em_id", 0)),
                        kind=str(disc.kind or getattr(d, "kind", "em")),
                        gen=int(disc.gen or getattr(d, "gen", 0)),
                        model=str(disc.model or getattr(d, "model", "")),
                        phases=int(disc.phases or getattr(d, "phases", 3)),
                        supports_emdata=bool(disc.supports_emdata),
                    )
                    if nd != d:
                        changed = True
                    devs_new.append(nd)

                if changed:
                    self.cfg = AppConfig(
                        version=__version__,
                        devices=devs_new,
                        download=self.cfg.download,
                        csv_pack=self.cfg.csv_pack,
                        ui=self.cfg.ui,
                        pricing=self.cfg.pricing,
                        billing=self.cfg.billing,
                        alerts=getattr(self.cfg, 'alerts', []) or [],
                    )
                    save_config(self.cfg, self.cfg_path)

                    def _apply_vars() -> None:
                        # If the Settings tab was already built, update the cached device rows.
                        try:
                            vars_ = getattr(self, "_dev_vars", None)
                            if not vars_:
                                return
                            by_key = {r[0].get().strip(): r for r in vars_}
                            for dd in devs_new:
                                row = by_key.get(dd.key)
                                if not row:
                                    continue
                                try:
                                    row[3].set(str(dd.em_id))
                                    row[4].set(str(getattr(dd, "model", "") or ""))
                                    row[5].set(str(getattr(dd, "kind", "em") or "em"))
                                    row[6].set(str(getattr(dd, "gen", 0) or 0))
                                    row[7].set(str(getattr(dd, "phases", 3) or 3))
                                    row[8].set(bool(getattr(dd, "supports_emdata", True)))
                                except Exception:
                                    pass
                            # Rebuild Settings tab if it's visible
                            try:
                                self._clear_frame(self.tab_settings)
                                self._build_settings_tab()
                            except Exception:
                                pass
                        except Exception:
                            pass

                    try:
                        self.after(0, _apply_vars)
                    except Exception:
                        pass
            except Exception:
                pass

        threading.Thread(target=_worker, name="ShellyProbeStartup", daemon=True).start()

    def _apply_language_change(self, lang: str) -> None:
        """Apply language and rebuild all UI widgets."""
        lang = normalize_lang(lang)
        if lang == self.lang:
            return
        # Stop live to avoid widget references during rebuild
        was_live = bool(getattr(self, '_live_pollers', []))
        try:
            if was_live:
                self._stop_live()
        except Exception:
            pass
        self.lang = lang
        self.t = lambda k, **kw: _t(self.lang, k, **kw)
        try:
            self.title(f"{self.t('app.title')} {__version__}")
        except Exception:
            pass
        # Rebuild root UI
        try:
            for w in list(self.winfo_children()):
                try:
                    w.destroy()
                except Exception:
                    pass
        except Exception:
            pass
        # Reset widget caches (canvases are Tk-bound)
        try:
            self._stats_canvases = {}
            self._stats_axes = {}
            self._stats_figs = {}
            self._live_figs = {}
            self._live_axes = {}
            self._live_canvases = {}
        except Exception:
            pass
        self._build_ui()
        # Apply DPI/monitor-aware scaling for fonts and plots.
        self._init_ui_scaling()
        # Resize robustness on macOS fullscreen/HiDPI: Tk sometimes delivers
        # intermediate sizes (especially when entering/leaving fullscreen).
        # We therefore schedule a short resize-watch whenever the main window
        # or the active tab changes.
        try:
            self.notebook.bind('<<NotebookTabChanged>>', lambda _e=None: self._kick_plots_resize_watch())
        except Exception:
            pass
        try:
            self.bind('<Configure>', lambda _e=None: self._kick_plots_resize_watch())
        except Exception:
            pass
        try:
            if was_live:
                self._start_live()
        except Exception:
            pass
    # ---------------- sizing helpers ----------------
    def _screen_px(self) -> Tuple[int, int]:
        try:
            return int(self.winfo_screenwidth()), int(self.winfo_screenheight())
        except Exception:
            return 1440, 900

    # ---------------- UI scaling (Etappe 3) ----------------
    def _ui_ppi(self) -> float:
        """Return pixels-per-inch reported by Tk (best-effort)."""
        try:
            return float(self.winfo_fpixels('1i'))
        except Exception:
            return 110.0

    def _ui_scale_factor(self) -> float:
        """Return a clamped UI scale factor based on current monitor DPI.

        Baseline is ~110 PPI (typical non-Retina desktop). Retina/5K screens
        usually report ~200–240 PPI.
        """
        ppi = self._ui_ppi()
        s = ppi / 110.0
        # Keep within reasonable bounds to avoid layout explosions.
        if s < 0.85:
            s = 0.85
        if s > 2.6:
            s = 2.6
        return float(s)

    def _apply_ui_scaling(self, force: bool = False) -> None:
        """Scale Tk default fonts for the current monitor (best-effort)."""
        try:
            s = self._ui_scale_factor()
        except Exception:
            s = 1.0

        try:
            last = getattr(self, '_ui_scale_last', None)
            if (not force) and last is not None and abs(float(last) - float(s)) < 0.05:
                return
            self._ui_scale_last = float(s)
        except Exception:
            pass

        # Capture baseline font sizes once.
        try:
            if not hasattr(self, '_ui_font_base_sizes'):
                base = {}
                for name in ('TkDefaultFont', 'TkTextFont', 'TkMenuFont', 'TkHeadingFont', 'TkFixedFont'):
                    try:
                        f = tkfont.nametofont(name)
                        base[name] = int(f.cget('size'))
                    except Exception:
                        pass
                self._ui_font_base_sizes = base
        except Exception:
            pass

        base = getattr(self, '_ui_font_base_sizes', {}) or {}

        def _set(name: str, min_sz: int = 9, max_sz: int = 18):
            try:
                f = tkfont.nametofont(name)
            except Exception:
                return
            try:
                b = int(base.get(name, int(f.cget('size'))))
            except Exception:
                b = 10
            # Tk uses negative sizes for pixels on some platforms; keep sign.
            sign = -1 if b < 0 else 1
            bb = abs(int(b))
            sz = int(round(bb * s))
            sz = max(min_sz, min(max_sz, sz))
            try:
                f.configure(size=sign * sz)
            except Exception:
                pass

        _set('TkDefaultFont', 9, 18)
        _set('TkTextFont', 9, 18)
        _set('TkMenuFont', 9, 18)
        _set('TkHeadingFont', 9, 18)
        _set('TkFixedFont', 9, 18)

        # Ensure common ttk widgets follow the (possibly) updated default font.
        try:
            st = ttk.Style(self)
            df = tkfont.nametofont('TkDefaultFont')
            st.configure('.', font=df)
            st.configure('TNotebook.Tab', font=df)
        except Exception:
            pass

        # Re-derive the LiveInfo font after scaling so it stays a notch smaller.
        try:
            st = ttk.Style(self)
            base_f = tkfont.nametofont('TkDefaultFont')
            live_f = base_f.copy()
            try:
                live_f.configure(size=max(8, int(base_f.cget('size')) - 1))
            except Exception:
                pass
            self._live_info_font = live_f  # keep reference
            st.configure('LiveInfo.TLabel', font=live_f)
        except Exception:
            pass


    def _start_dpi_watch(self) -> None:
        """Watch for monitor DPI changes (e.g. moving window between screens)."""
        try:
            if getattr(self, '_dpi_watch_active', False):
                return
            self._dpi_watch_active = True
        except Exception:
            return

        state = {'ppi': None}

        def _tick():
            try:
                ppi = self._ui_ppi()
            except Exception:
                ppi = None
            changed = False
            try:
                last = state.get('ppi')
                if last is None:
                    changed = True
                elif ppi is not None and abs(float(ppi) - float(last)) >= 6.0:
                    changed = True
            except Exception:
                changed = True

            if changed:
                try:
                    state['ppi'] = ppi
                except Exception:
                    pass
                try:
                    self._apply_ui_scaling(force=True)
                except Exception:
                    pass
                # Force redraws so Matplotlib tick-label sizes update.
                try:
                    self._redraw_plots_active()
                except Exception:
                    pass
                try:
                    self._redraw_live_plots()
                except Exception:
                    pass

            try:
                self.after(1500, _tick)
            except Exception:
                try:
                    self._dpi_watch_active = False
                except Exception:
                    pass

        try:
            self.after(500, _tick)
        except Exception:
            try:
                self._dpi_watch_active = False
            except Exception:
                pass



    def _init_ui_scaling(self) -> None:
        """Apply initial UI scaling and start the DPI watcher."""
        try:
            self._apply_ui_scaling(force=True)
        except Exception:
            pass
        try:
            self._start_dpi_watch()
        except Exception:
            pass

    def _dpi_for_widget(self, widget: tk.Widget) -> int:
        """Choose a DPI that keeps plot annotations readable on smaller screens."""
        sw, sh = self._screen_px()
        dpi = 100
        if sw <= 1600 or sh <= 1000:
            dpi = 90
        if sw <= 1400 or sh <= 900:
            dpi = 80
        try:
            h = int(widget.winfo_height() or 0)
            w = int(widget.winfo_width() or 0)
        except Exception:
            h = 0
            w = 0
        if h and h < 260:
            dpi = max(70, dpi - 10)
        if w and w < 520:
            dpi = max(70, dpi - 5)
        return dpi
    def _resize_figure_to_widget(self, fig: Figure, widget: tk.Widget, dpi: int = 100, min_h_px: int = 220) -> None:
        """Resize a matplotlib figure to match the current widget size."""
        try:
            widget.update_idletasks()

            # Tk (especially on macOS HiDPI/Retina) may report *logical* pixels
            # for widget sizes, while the backing store used by TkAgg is scaled
            # (e.g. 2x). If we size the figure using only the logical pixel
            # values, matplotlib will render a smaller bitmap which then sits in
            # the top-left corner of a larger widget (lots of white space).
            #
            # We estimate the backing-store scale primarily from Tk's scaling
            # (this is typically 2.0 on macOS Retina). Using a 96-dpi baseline
            # tends to under-estimate and keeps the rendered bitmap too small.
            scale = 1.0
            try:
                s = float(self.tk.call('tk', 'scaling'))
                if s and 0.75 <= s <= 3.0:
                    scale = s
            except Exception:
                scale = 1.0

            # Fallback: derive a scale from pixels-per-inch.
            if scale <= 1.0:
                try:
                    ppi = float(widget.winfo_fpixels('1i'))
                    if ppi and ppi > 0:
                        # 72 dpi is Tk's "1.0" baseline.
                        scale = max(1.0, min(3.0, ppi / 72.0))
                except Exception:
                    pass
            # Safety margin avoids "right side clipped" issues on some Tk/HiDPI
            # combinations where the backing-store rounds differently.
            margin_x = 24
            margin_y = 18
            try:
                if sys.platform.startswith("win"):
                    margin_x = 18
                    margin_y = 14
            except Exception:
                pass
            w = max(200, int(int(widget.winfo_width()) * scale) - int(margin_x * scale))
            h = max(min_h_px, int(int(widget.winfo_height()) * scale) - int(margin_y * scale))
        except Exception:
            sw, sh = self._screen_px()
            w, h = int(sw * 0.85), int(sh * 0.25)

        # On macOS HiDPI, Tk can report logical pixels while the backing store
        # uses a different scale factor. Setting the figure DPI to the full
        # pixels-per-inch makes the plot *look* correct but often results in a
        # very small usable plotting area (especially with toolbars) because
        # the same widget size translates to fewer inches.
        #
        # We therefore keep a moderate DPI and scale fonts separately via
        # `_font_base_for_widget`, which produces better layouts across
        # monitor changes.
        try:
            ppi = float(widget.winfo_fpixels('1i'))
            if ppi and ppi > 170:
                dpi = min(int(dpi), 140)
        except Exception:
            pass

        fig.set_dpi(dpi)
        fig.set_size_inches(w / float(dpi), h / float(dpi), forward=True)


    # ---------------- Plotly (Plots tab) ----------------
    def _plotly_imports(self):
        """Lazy import Plotly and return (go, make_subplots) or (None, None).

        We keep Plotly optional so the rest of the app can still run.
        """
        try:
            cached = getattr(self, '_plotly_cached', None)
            if cached:
                return cached
        except Exception:
            pass
        try:
            import plotly.graph_objects as go  # type: ignore
            from plotly.subplots import make_subplots  # type: ignore
            cached = (go, make_subplots)
            try:
                self._plotly_cached = cached
            except Exception:
                pass
            return cached
        except Exception:
            try:
                self._plotly_cached = (None, None)
            except Exception:
                pass
            return (None, None)

    def _plotly_scale_factor(self) -> float:
        """Best-effort UI scaling factor (Retina/HiDPI).

        Tk returns a scaling where 1.0 is standard DPI and 2.0 is typical Retina.
        """
        try:
            s = float(self.tk.call('tk', 'scaling'))
            if s < 0.75:
                s = 1.0
            if s > 3.0:
                s = 3.0
            return s
        except Exception:
            return 1.0

    def _plotly_fig_to_photo(self, fig, w: int, h: int, cache_key: str = ''):
        """Render a Plotly figure to a Tk PhotoImage sized to the label.

        We render at HiDPI resolution and downscale with Pillow for crisp text.
        """
        # Small guard: prevent zero/negative sizes
        w = max(64, int(w or 0))
        h = max(64, int(h or 0))

        # Simple cache to avoid re-rendering while resizing.
        try:
            cache = getattr(self, '_plotly_photo_cache', None)
            if cache is None:
                cache = {}
                self._plotly_photo_cache = cache
            ck = f'{cache_key}:{w}x{h}' if cache_key else f'{w}x{h}'
            if ck in cache:
                return cache[ck]
        except Exception:
            cache = None
            ck = None

        scale = self._plotly_scale_factor()
        rw = int(round(w * scale))
        rh = int(round(h * scale))

        # Render to PNG bytes (requires kaleido).
        png = fig.to_image(format='png', width=rw, height=rh, scale=1)  # type: ignore
        im = Image.open(io.BytesIO(png))
        if (rw, rh) != (w, h):
            im = im.resize((w, h), Image.LANCZOS)
        photo = ImageTk.PhotoImage(im)

        # Keep cache small to avoid memory growth.
        if cache is not None and ck is not None:
            cache[ck] = photo
            try:
                if len(cache) > 48:
                    # Drop roughly half (in insertion order) to keep memory bounded.
                    for k in list(cache.keys())[:24]:
                        cache.pop(k, None)
            except Exception:
                pass
        return photo

    def _set_plot_image(self, lbl: tk.Widget, photo) -> None:
        """Attach a PhotoImage to a ttk.Label and keep a strong reference."""
        try:
            lbl.configure(image=photo)
        except Exception:
            return
        try:
            # prevent GC
            lbl._shelly_img = photo  # type: ignore[attr-defined]
        except Exception:
            pass

    def _kick_plots_resize_watch(self) -> None:
        """On macOS fullscreen, geometry settles late; force a few redraws."""
        try:
            if getattr(self, '_plots_resize_watch_active', False):
                return
            self._plots_resize_watch_active = True
        except Exception:
            return

        state = {'n': 0, 'last': None, 'stable': 0}

        def _tick():
            # stop after ~2s or when stable twice
            state['n'] += 1
            try:
                view = str(self._plots_view.get() or 'timeseries')
            except Exception:
                view = 'timeseries'
            sizes = []
            try:
                if view == 'kwh':
                    for lbl in getattr(self, '_kwh_labels', {}).values():
                        sizes.append((int(lbl.winfo_width() or 0), int(lbl.winfo_height() or 0)))
                else:
                    for lbl in getattr(self, '_ts_labels', {}).values():
                        sizes.append((int(lbl.winfo_width() or 0), int(lbl.winfo_height() or 0)))
            except Exception:
                sizes = []

            sig = tuple(sizes)
            if sig == state['last']:
                state['stable'] += 1
            else:
                state['stable'] = 0
            state['last'] = sig

            try:
                self._redraw_plots_current()
            except Exception:
                pass

            if state['n'] >= 8 or state['stable'] >= 2:
                try:
                    self._plots_resize_watch_active = False
                except Exception:
                    pass
                return
            try:
                self.after(250, _tick)
            except Exception:
                try:
                    self._plots_resize_watch_active = False
                except Exception:
                    pass

        try:
            self.after(50, _tick)
        except Exception:
            self._plots_resize_watch_active = False

    def _resize_plots_figures_only(self) -> None:
        """Fast path during resize: just redraw current plots (throttled)."""
        now = time.time()
        try:
            last = getattr(self, '_plots_fast_resize_last', 0.0)
            if now - last < 0.15:
                return
            self._plots_fast_resize_last = now
        except Exception:
            pass
        try:
            try:
                self._redraw_plots_active()
            except Exception:
                self._redraw_plots_current()
        except Exception:
            pass


    def _font_base_for_widget(self, widget: tk.Widget) -> int:
        """Return a base font size (pt) for plots based on widget + screen size.

        On smaller screens (e.g., MacBook Pro 14"), the default Tk/Matplotlib sizing can
        lead to clipped tick labels. We therefore scale down more aggressively and allow
        a smaller minimum font size.
        """
        try:
            w = max(200, int(widget.winfo_width()))
            h = max(160, int(widget.winfo_height()))
        except Exception:
            w, h = 800, 400

        sw, sh = self._screen_px()
        compact = 1.0
        # macOS laptops / small screens: scale text down a bit
        if sw <= 1600 or sh <= 1000:
            compact *= 0.85
        # very small canvases: scale down further
        if h < 260:
            compact *= 0.85
        if w < 520:
            compact *= 0.90

        # HiDPI displays (e.g. 5K Studio Display) need larger fonts to remain readable.
        # Tk reports pixels-per-inch via winfo_fpixels('1i'). A typical baseline is ~96–110.
        try:
            dpi = float(self.winfo_fpixels('1i'))
        except Exception:
            dpi = 110.0
        dpi_scale = max(0.9, min(2.6, dpi / 110.0))

        base = int((min(w, h) / 85) * compact * dpi_scale)
        # Allow larger caps so large/HiDPI monitors stay readable.
        return max(4, min(20, base))

    def _max_xlabels_for_widget(self, widget: tk.Widget) -> int:
        """Choose a sane number of x-axis labels based on available width."""
        try:
            w = max(240, int(widget.winfo_width()))
        except Exception:
            w = 800
        # ~90px per label works well with rotated HH:MM ticks
        return max(5, min(18, int(w / 90)))
    def _apply_time_axis(self, ax, widget: tk.Widget) -> None:
        """Configure a datetime x-axis so ticks stay readable and deterministic.

        Matplotlib's AutoDateLocator can emit warnings on some date ranges.
        We pick a locator based on the intended window length (live view).
        """
        try:
            win_m = int(self.cfg.ui.live_window_minutes)
        except Exception:
            win_m = 10
        max_labels = self._max_xlabels_for_widget(widget)
        try:
            if win_m <= 2:
                interval = max(1, int(round((win_m * 60) / max_labels)))
                locator = mdates.SecondLocator(interval=interval)
                formatter = mdates.DateFormatter("%H:%M:%S")
            elif win_m < 6 * 60:
                interval = max(1, int(round(win_m / max_labels)))
                locator = mdates.MinuteLocator(interval=interval)
                formatter = mdates.DateFormatter("%H:%M")
            elif win_m < 48 * 60:
                interval = max(1, int(round((win_m / 60) / max_labels)))
                locator = mdates.HourLocator(interval=interval)
                formatter = mdates.DateFormatter("%d.%m %H:%M")
            else:
                interval = max(1, int(round((win_m / 60 / 24) / max_labels)))
                locator = mdates.DayLocator(interval=interval)
                formatter = mdates.DateFormatter("%d.%m")
            ax.xaxis.set_major_locator(locator)
            ax.xaxis.set_major_formatter(formatter)
        except Exception:
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        base = self._font_base_for_widget(widget)
        ax.tick_params(axis="x", labelrotation=30, labelsize=min(30, base + 4))

    def _apply_smart_date_axis(self, ax, widget: tk.Widget, max_labels: Optional[int] = None) -> None:
        """Apply a robust datetime locator/formatter based on the plotted span.

        Avoids AutoDateLocator warnings and keeps ticks readable across
        minutes to years.
        """
        if max_labels is None:
            max_labels = self._max_xlabels_for_widget(widget)
        try:
            x0, x1 = ax.get_xlim()
            if not (math.isfinite(x0) and math.isfinite(x1)):
                return
            lo, hi = (x0, x1) if x0 <= x1 else (x1, x0)
            d0 = mdates.num2date(lo)
            d1 = mdates.num2date(hi)
            span_s = (d1 - d0).total_seconds()
        except Exception:
            return
        if span_s is None or span_s <= 0:
            # nothing to scale (single timestamp)
            locator = mdates.MinuteLocator(interval=1)
            formatter = mdates.DateFormatter("%H:%M")
            try:
                ax.xaxis.set_major_locator(locator)
                ax.xaxis.set_major_formatter(formatter)
            except Exception:
                return
            base = self._font_base_for_widget(widget)
            ax.tick_params(axis="x", labelrotation=30, labelsize=min(30, base + 4))
            return
        try:
            if span_s <= 6 * 3600:
                span_min = span_s / 60.0
                interval = max(1, int(round(span_min / max_labels)))
                locator = mdates.MinuteLocator(interval=interval)
                formatter = mdates.DateFormatter("%H:%M")
            elif span_s <= 2 * 86400:
                span_h = span_s / 3600.0
                interval = max(1, int(round(span_h / max_labels)))
                locator = mdates.HourLocator(interval=interval)
                formatter = mdates.DateFormatter("%d.%m %H:%M")
            elif span_s <= 90 * 86400:
                span_d = span_s / 86400.0
                interval = max(1, int(round(span_d / max_labels)))
                locator = mdates.DayLocator(interval=interval)
                formatter = mdates.DateFormatter("%d.%m")
            elif span_s <= 2 * 365 * 86400:
                span_m = span_s / (30.0 * 86400.0)
                interval = max(1, int(round(span_m / max_labels)))
                locator = mdates.MonthLocator(interval=interval)
                formatter = mdates.DateFormatter("%m.%Y")
            else:
                span_y = span_s / (365.0 * 86400.0)
                base_years = max(1, int(round(span_y / max_labels)))
                locator = mdates.YearLocator(base=base_years)
                formatter = mdates.DateFormatter("%Y")
            ax.xaxis.set_major_locator(locator)
            ax.xaxis.set_major_formatter(formatter)
        except Exception:
            return
        base = self._font_base_for_widget(widget)
        ax.tick_params(axis="x", labelrotation=30, labelsize=min(30, base + 4))


    def _apply_xticks(self, ax, labels: List[str], base_font: Optional[int] = None) -> None:
        """Apply readable categorical x-ticks.

        We use this for kWh bar charts (days/weeks/months), where the x-axis is
        categorical and can contain many labels. Older versions had this as a
        nested function in one code-path; other paths call it as a method. This
        method keeps both working and prevents the crash:
        `'_tkinter.tkapp' object has no attribute '_apply_xticks'`.
        """
        if not labels:
            return
        try:
            import math
            n = int(len(labels))
            max_labels = 40
            # Reduce clutter if there are too many labels.
            if n > max_labels:
                step = int(math.ceil(n / max_labels))
                ticks = list(range(0, n, max(1, step)))
                ax.set_xticks(ticks)
                ax.set_xticklabels([labels[i] for i in ticks], rotation=45, ha="right")
            else:
                ax.set_xticks(range(n))
                ax.set_xticklabels(labels, rotation=45, ha="right")

            # Remember rotation so _apply_axis_layout can compute margins.
            try:
                setattr(ax, "_shelly_xrot", 45)
            except Exception:
                pass
            if base_font is None:
                try:
                    base_font = self._font_base_for_widget(self)
                except Exception:
                    base_font = None
            if base_font is not None:
                try:
                    # Larger tick labels for better readability.
                    ax.tick_params(axis="x", labelsize=min(30, int(base_font) + 4))
                except Exception:
                    pass
        except Exception:
            # Never fail hard due to formatting.
            pass

    def _annotate_bars(self, ax, bars) -> None:
        """Annotate bars with their numeric values (kWh).

        Some plot paths call this as a method; if it is missing, Tk will try to
        resolve it on the underlying tkapp object, producing:
        `'_tkinter.tkapp' object has no attribute '_annotate_bars'`.
        This helper is intentionally defensive and must never crash the UI.
        """
        try:
            # Ensure there is enough headroom so vertical bar labels do not get clipped
            # at the top edge of the axes.
            try:
                hs = []
                for b in bars:
                    try:
                        h = float(b.get_height())
                    except Exception:
                        continue
                    if math.isfinite(h):
                        hs.append(h)
                if hs:
                    y0, y1 = ax.get_ylim()
                    hmax = max(hs)
                    hmin = min(hs)
                    # Add ~12% headroom (and a small absolute minimum) above the max bar.
                    if hmax >= 0:
                        margin = max(0.2, abs(hmax) * 0.12)
                        y1 = max(y1, hmax + margin)
                    if hmin < 0:
                        margin = max(0.2, abs(hmin) * 0.12)
                        y0 = min(y0, hmin - margin)
                    ax.set_ylim(y0, y1)
            except Exception:
                pass

            # Avoid heavy clutter on dense charts: annotate at most ~24 bars.
            try:
                n = len(bars)
            except Exception:
                n = 0
            step = 1
            if n and n > 24:
                step = max(1, int(math.ceil(n / 24)))

            # Choose a readable font size based on current widget sizing.
            try:
                base = int(self._font_base_for_widget(self))
            except Exception:
                base = 8
            fontsize = max(4, min(12, base))

            # Prefer matplotlib's built-in bar_label when available.
            if hasattr(ax, "bar_label"):
                labels = []
                for i, b in enumerate(bars):
                    if step > 1 and (i % step) != 0:
                        labels.append("")
                        continue
                    try:
                        h = float(b.get_height())
                    except Exception:
                        labels.append("")
                        continue
                    if not math.isfinite(h):
                        labels.append("")
                        continue
                    labels.append(_fmt_kwh(h))
                try:
                    ax.bar_label(bars, labels=labels, padding=2, fontsize=fontsize, rotation=90)
                    return
                except Exception:
                    # fall back to manual annotate
                    pass

            # Manual fallback (works on older matplotlib).
            try:
                y0, y1 = ax.get_ylim()
                yr = float(y1 - y0)
                off = (yr * 0.01) if yr else 0.01
            except Exception:
                off = 0.01

            for i, b in enumerate(bars):
                if step > 1 and (i % step) != 0:
                    continue
                try:
                    h = float(b.get_height())
                except Exception:
                    continue
                if not math.isfinite(h):
                    continue
                try:
                    x = float(b.get_x() + b.get_width() / 2.0)
                except Exception:
                    continue
                dy = off if h >= 0 else -off
                va = "bottom" if h >= 0 else "top"
                try:
                    ax.annotate(
                        _fmt_kwh(h),
                        (x, h),
                        xytext=(0, 3 if h >= 0 else -3),
                        textcoords="offset points",
                        ha="center",
                        va=va,
                        fontsize=fontsize,
                        rotation=90,
                        clip_on=False,
                    )
                except Exception:
                    continue
        except Exception:
            pass

    def _apply_axis_layout(self, fig: Figure, ax, widget: tk.Widget, legend: bool = False) -> None:
        base = self._font_base_for_widget(widget)
        # Reduce visual clutter on compact screens
        # Make axis labels/ticks slightly larger overall (especially noticeable on 4K/5K screens).
        ax.tick_params(axis="both", labelsize=min(30, base + 4), pad=max(1, base - 3))
        ax.xaxis.label.set_size(min(22, base + 6))
        ax.yaxis.label.set_size(min(22, base + 6))

        # Thinner spines when fonts are tiny (helps on small screens)
        try:
            lw = 0.6 if base <= 5 else 0.8
            for spine in ax.spines.values():
                spine.set_linewidth(lw)
        except Exception:
            pass

        if legend:
            try:
                leg = ax.get_legend()
                if leg is not None:
                    for t in leg.get_texts():
                        t.set_fontsize(max(4, base))
                    # Make legend frame less bulky
                    leg.get_frame().set_linewidth(0.6)
            except Exception:
                pass

        # In Tk on HiDPI / when moving between monitors, `tight_layout()` can
        # occasionally produce a layout that looks shifted/clipped.
        # We therefore rely primarily on explicit subplots_adjust below.

        # Some plot modes manage their own deterministic margins (e.g. Plots tab
        # Option A time-series). In that case, skip further subplots_adjust.
        try:
            if bool(getattr(ax, "_shelly_fixed_layout", False)):
                return
        except Exception:
            pass

        try:
            try:
                h = int(widget.winfo_height() or 0)
            except Exception:
                h = 0
            rot = 30
            try:
                rot = int(getattr(ax, "_shelly_xrot", 30))
            except Exception:
                rot = 30

            # Base bottom margin by canvas height and rotation
            if h and h < 240:
                bottom = 0.44
            elif h and h < 280:
                bottom = 0.40
            elif h and h < 340:
                bottom = 0.36
            else:
                bottom = 0.30

            # Rotation needs extra room
            bottom += (rot / 90.0) * 0.06

            # Add a little extra room based on font size.
            bottom = min(0.52, max(0.24, bottom + max(0, base - 6) * 0.010))
            # Keep a little extra room on the right; prevents edge clipping on
            # some platforms when the canvas is resized rapidly.
            fig.subplots_adjust(left=0.11, right=0.97, top=0.93, bottom=bottom)
        except Exception:
            pass

    # ---------------- UI scaffold ----------------
    def _build_ui(self) -> None:
        # Top bar: choose which two devices are shown in the UI.
        bar = ttk.Frame(self)
        bar.pack(fill="x", padx=12, pady=(10, 0))
        ttk.Label(bar, text=self.t("ui.view")).pack(side="left")
        self.device_page_label_var = tk.StringVar(value="")
        self.device_page_cb = ttk.Combobox(bar, state="readonly", width=34, textvariable=self.device_page_label_var)
        self.device_page_cb.pack(side="left", padx=(8, 0))
        self.device_page_cb.bind("<<ComboboxSelected>>", self._on_device_page_selected)

        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill="both", expand=True)
        self.tab_sync = ttk.Frame(self.notebook)
        self.tab_plots = ttk.Frame(self.notebook)
        self.tab_live = ttk.Frame(self.notebook)
        self.tab_export = ttk.Frame(self.notebook)
        self.tab_settings = ttk.Frame(self.notebook)
        # Notebook tab labels are translated based on the selected UI language.
        self.notebook.add(self.tab_sync, text=self.t("tabs.sync"))
        self.notebook.add(self.tab_plots, text=self.t("tabs.plots"))
        self.notebook.add(self.tab_live, text=self.t("tabs.live"))
        self.notebook.add(self.tab_export, text=self.t("tabs.export"))
        self.notebook.add(self.tab_settings, text=self.t("tabs.settings"))
        self._build_sync_tab()
        self._build_plots_tab()
        self._build_live_tab()
        self._build_export_tab()
        self._build_settings_tab()
        self._page_labels = []
        self._update_device_page_choices()
    # ---------------- Device paging (max 2 shown at once) ----------------
    def _get_visible_devices(self) -> List[DeviceConfig]:
        """Return the devices currently visible in the UI (max 2)."""
        try:
            page = int(getattr(self.cfg.ui, "device_page_index", 0) or 0)
        except Exception:
            page = 0
        page = max(0, page)
        start = page * 2
        return list(self.cfg.devices[start : start + 2])

    def _device_page_labels(self) -> List[str]:
        devs = list(self.cfg.devices)
        if not devs:
            return ["1: — | —"]
        labels: List[str] = []
        for i in range(0, len(devs), 2):
            left = devs[i].name
            right = devs[i + 1].name if i + 1 < len(devs) else "—"
            labels.append(f"{(i//2)+1}: {left} | {right}")
        return labels

    def _update_device_page_choices(self) -> None:
        labels = self._device_page_labels()
        self._page_labels = labels
        try:
            self.device_page_cb["values"] = labels
        except Exception:
            pass
        max_idx = max(0, len(labels) - 1)
        try:
            idx = int(getattr(self.cfg.ui, "device_page_index", 0) or 0)
        except Exception:
            idx = 0
        if idx > max_idx:
            idx = max_idx
            self.cfg = replace(self.cfg, ui=replace(self.cfg.ui, device_page_index=idx))
            try:
                save_config(self.cfg, self.cfg_path)
            except Exception:
                pass
        try:
            self.device_page_label_var.set(labels[idx])
        except Exception:
            pass

    def _on_device_page_selected(self, _evt: Any = None) -> None:
        labels = getattr(self, "_page_labels", None) or self._device_page_labels()
        sel = str(self.device_page_label_var.get())
        try:
            idx = labels.index(sel)
        except Exception:
            idx = 0
        self.cfg = replace(self.cfg, ui=replace(self.cfg.ui, device_page_index=idx))
        try:
            save_config(self.cfg, self.cfg_path)
        except Exception:
            pass
        # Do NOT stop live when switching pages. Live polling runs for all devices
        # in the background; we only swap which two devices are currently shown.
        self._rebuild_plots_tab()
        self._rebuild_live_tab()
        try:
            self._reload_data()
        except Exception:
            pass
        # Ensure live UI reflects the current live state after the rebuild.
        try:
            self._sync_live_ui_state()
        except Exception:
            pass

    def _clear_frame(self, frame: ttk.Frame) -> None:
        for child in list(frame.winfo_children()):
            try:
                child.destroy()
            except Exception:
                pass

    def _rebuild_plots_tab(self) -> None:
        self._clear_frame(self.tab_plots)
        self._build_plots_tab()

    def _rebuild_live_tab(self) -> None:
        self._clear_frame(self.tab_live)
        self._build_live_tab()

    # ---------------- Sync tab ----------------
    def _build_sync_tab(self) -> None:
        frm = self.tab_sync
        top = ttk.Frame(frm)
        top.pack(fill="x", padx=12, pady=10)
        ttk.Label(top, text=self.t("sync.header")).pack(side="left")
        btns = ttk.Frame(top)
        btns.pack(side="left", padx=14)
        ttk.Button(btns, text=self.t("sync.btn.incremental"), command=lambda: self._start_sync("incremental")).pack(side="left")
        ttk.Button(btns, text=self.t("sync.btn.day"), command=lambda: self._start_sync("day")).pack(side="left", padx=6)
        ttk.Button(btns, text=self.t("sync.btn.week"), command=lambda: self._start_sync("week")).pack(side="left")
        ttk.Button(btns, text=self.t("sync.btn.month"), command=lambda: self._start_sync("month")).pack(side="left", padx=6)
        # Fester Start (TT.MM.JJJJ)
        self.sync_start_date_var = tk.StringVar(value=time.strftime("%d.%m.%Y", time.localtime(time.time() - 30*86400)))
        ttk.Label(btns, text=self.t("sync.from")).pack(side="left", padx=(14, 4))
        ttk.Entry(btns, textvariable=self.sync_start_date_var, width=12).pack(side="left")
        ttk.Button(btns, text=self.t("sync.startdate"), command=self._start_sync_from_date).pack(side="left", padx=(6, 0))
        ttk.Button(top, text=self.t("sync.reload"), command=self._reload_data).pack(side="left", padx=12)
        # Autosync controls
        auto = ttk.LabelFrame(frm, text=self.t("settings.autosync.title"))
        auto.pack(fill="x", padx=12, pady=(0, 10))
        self.autosync_enabled_var = tk.BooleanVar(value=bool(self.cfg.ui.autosync_enabled))
        self.autosync_interval_var = tk.IntVar(value=int(self.cfg.ui.autosync_interval_hours))
        self.autosync_mode_var = tk.StringVar(value=str(self.cfg.ui.autosync_mode))
        ttk.Checkbutton(auto, text=self.t("autosync.active"), variable=self.autosync_enabled_var, command=self._on_autosync_toggle).pack(
            side="left", padx=8, pady=8
        )
        ttk.Label(auto, text=self.t("autosync.interval")).pack(side="left", padx=(12, 4))
        cmb = ttk.Combobox(
            auto,
            values=[str(x) for x in AUTOSYNC_INTERVAL_OPTIONS],
            width=5,
            textvariable=self.autosync_interval_var,
            state="readonly",
        )
        cmb.pack(side="left")
        ttk.Label(auto, text=self.t("autosync.hours")).pack(side="left", padx=(4, 12))
        ttk.Label(auto, text=self.t("autosync.mode")).pack(side="left", padx=(0, 4))
        self.autosync_mode_cmb = ttk.Combobox(
            auto,
            values=[m for m, _ in AUTOSYNC_MODE_OPTIONS],
            width=12,
            textvariable=self.autosync_mode_var,
            state="readonly",
        )
        self.autosync_mode_cmb.pack(side="left")
        self.autosync_status = tk.StringVar(value="Autosync: aus")
        ttk.Label(auto, textvariable=self.autosync_status).pack(side="left", padx=12)
        mid = ttk.Frame(frm)
        mid.pack(fill="both", expand=True, padx=12, pady=10)
        self.sync_log = tk.Text(mid, height=18)
        self.sync_log.pack(fill="both", expand=True)
        bottom = ttk.Frame(frm)
        bottom.pack(fill="x", padx=12, pady=(0, 12))
        self.sync_summary = tk.StringVar(value="Noch keine Daten geladen.")
        ttk.Label(bottom, textvariable=self.sync_summary).pack(anchor="w")
        # init autosync status
        self.after(200, self._on_autosync_toggle)
    def _log_sync(self, msg: str) -> None:
        ts = time.strftime("%H:%M:%S")
        self.sync_log.insert("end", f"[{ts}] {msg}\n")
        self.sync_log.see("end")
    def _sync_range_override(self, mode: str) -> Optional[Tuple[int, int]]:
        now = int(time.time())
        if mode == "incremental":
            return None
        if mode == "day":
            return (max(0, now - 86400), now)
        if mode == "week":
            return (max(0, now - 7 * 86400), now)
        if mode == "month":
            return (max(0, now - 30 * 86400), now)
        return None
    def _start_sync_from_date(self) -> None:
        """Start sync from a user-entered start date (TT.MM.JJJJ) until now."""
        try:
            s = (self.sync_start_date_var.get() or "").strip()
        except Exception:
            s = ""
        if not s:
            messagebox.showerror(self.t("msg.sync"), self.t("sync.err.need_date"))
            return
        try:
            from datetime import datetime
            from zoneinfo import ZoneInfo
            dt = datetime.strptime(s, "%d.%m.%Y").replace(tzinfo=ZoneInfo("Europe/Berlin"))
            start_ts = int(dt.timestamp())
        except Exception:
            messagebox.showerror(self.t("msg.sync"), self.t("sync.err.bad_date", s=s))
            return
        end_ts = int(time.time())
        if end_ts <= start_ts:
            end_ts = start_ts + 1
        self._start_sync("custom", range_override=(start_ts, end_ts), label=f"ab {s}")
    def _start_sync(self, mode: str, range_override: Optional[Tuple[int, int]] = None, label: Optional[str] = None) -> None:
        if self._sync_thread and self._sync_thread.is_alive():
            messagebox.showinfo(self.t("msg.sync"), self.t("sync.info.running"))
            return
        if range_override is None:
            range_override = self._sync_range_override(mode)
        shown = label or mode
        self._log_sync(f"Sync gestartet ({shown}) …")
        def worker():
            try:
                results = sync_all(self.cfg, self.storage, range_override=range_override, fallback_last_days=7)
                for r in results:
                    a, b = r.requested_range
                    ok_chunks = sum(1 for c in r.chunks if c.ok)
                    fail = next((c for c in r.chunks if not c.ok), None)
                    # Devices without EMData CSV support are "live-only". In that case
                    # sync_one_device returns an empty chunks list. This is not an error.
                    if fail:
                        self._sync_q.put(f"{r.device_name}: Fehler im Chunk {fail.ts}-{fail.end_ts}: {fail.error}")
                    if len(r.chunks) == 0 and a == 0 and b == 0:
                        self._sync_q.put(f"{r.device_name}: {self.t('sync.info.skipped_no_csv')}")
                    else:
                        self._sync_q.put(
                            f"{r.device_name}: {ok_chunks}/{len(r.chunks)} Chunks OK, last_end_ts={r.updated_last_end_ts} (Range {a}..{b})"
                        )
                self._sync_q.put("__SYNC_DONE__")
            except Exception as e:
                try:
                    self._sync_q.put(self.t('sync.err.generic', e=e))
                except Exception:
                    self._sync_q.put(f"Sync ERROR: {e}")
                self._sync_q.put("__SYNC_DONE__")
        self._sync_thread = threading.Thread(target=worker, daemon=True)
        self._sync_thread.start()
    def _on_autosync_toggle(self) -> None:
        enabled = bool(self.autosync_enabled_var.get())
        if not enabled:
            self._autosync_next_ts = None
            self.autosync_status.set(self.t('autosync.off'))
            return
        # schedule next run if not already scheduled
        interval_h = int(self.autosync_interval_var.get() or 12)
        interval_h = interval_h if interval_h in AUTOSYNC_INTERVAL_OPTIONS else 12
        now = time.time()
        if self._autosync_next_ts is None or self._autosync_next_ts <= now:
            self._autosync_next_ts = now + interval_h * 3600.0
        nxt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self._autosync_next_ts))
        self.autosync_status.set(self.t('autosync.next_run', nxt=nxt, interval_h=interval_h))
    def _autosync_tick(self) -> None:
        if not getattr(self, "autosync_enabled_var", None):
            return
        if not self.autosync_enabled_var.get():
            return
        interval_h = int(self.autosync_interval_var.get() or 12)
        interval_h = interval_h if interval_h in AUTOSYNC_INTERVAL_OPTIONS else 12
        mode = str(self.autosync_mode_var.get() or "incremental")
        now = time.time()
        if self._autosync_next_ts is None:
            self._autosync_next_ts = now + interval_h * 3600.0
        # If time reached and sync not running -> trigger
        if now >= float(self._autosync_next_ts):
            if not (self._sync_thread and self._sync_thread.is_alive()):
                self._log_sync(f"Autosync Trigger ({mode})")
                try:
                    self._start_sync(mode)
                except Exception as e:
                    self._log_sync(f"Autosync Fehler: {e}")
                # schedule next
                self._autosync_next_ts = now + interval_h * 3600.0
            else:
                # postpone a bit
                self._autosync_next_ts = now + 60.0
        # Update status
        try:
            self._on_autosync_toggle()
        except Exception:
            pass
    def _reload_data(self) -> None:
        try:
            self.computed = {d.key: load_device(self.storage, d) for d in self.cfg.devices}
            unit_gross = float(self.cfg.pricing.unit_price_gross())
            parts: List[str] = []
            for d in self._get_visible_devices():
                cd = self.computed.get(d.key)
                if cd is None:
                    continue
                kwh, avgp, maxp = summarize(cd.df)
                cost = kwh * unit_gross
                parts.append(f"{d.name}: {_fmt_kwh(kwh)} ({_fmt_eur(cost)}) | Ø {avgp:.0f} W, Max {maxp:.0f} W")
            self.sync_summary.set(" | ".join(parts) if parts else self.t("common.no_data"))
            self._log_sync("Daten geladen.")
            # Keep classic stats plot (kWh) and refresh the Plots tab (new metric tabs).
            self._redraw_stats_plots()
            try:
                self._redraw_plots_active()
            except Exception:
                pass
            # Refresh today's kWh base from imported data
            self._start_today_kwh_base_refresh()
        except Exception as e:
            messagebox.showerror(self.t("msg.data_load"), str(e))

    def _ensure_data_loaded(self) -> bool:
        """Ensure imported CSV data has been loaded into `self.computed`.

        The Plots tab may try to render during UI construction (before the first
        explicit reload). On macOS/Tk this can happen early, so we provide a
        small helper that loads data on-demand.

        Returns:
            True if `self.computed` is available (even if individual frames are
            empty), otherwise False.
        """

        try:
            comp = getattr(self, "computed", None)
            if isinstance(comp, dict):
                return True
        except Exception:
            pass

        try:
            self._reload_data()
        except Exception:
            return False

        try:
            comp = getattr(self, "computed", None)
            return isinstance(comp, dict)
        except Exception:
            return False

    # ---------------- Today kWh (imported + live) ----------------
    def _start_today_kwh_base_refresh(self) -> None:
        """Recompute today's base kWh from the imported CSVs.

        Runs in a background thread to keep the UI responsive.
        """

        def _job(devs: List[DeviceConfig]) -> None:
            from datetime import datetime

            today = datetime.now().date()
            res_kwh: Dict[str, float] = {}
            res_last_ts: Dict[str, int] = {}
            for d in devs:
                try:
                    df = self.storage.read_device_df(d.key)
                    # filter to today (local time)
                    df_day = df.loc[df["timestamp"].dt.date == today].copy()
                    if df_day.empty:
                        res_kwh[d.key] = 0.0
                        res_last_ts[d.key] = 0
                        continue
                    df_e = calculate_energy(df_day)
                    res_kwh[d.key] = float(df_e["energy_kwh"].fillna(0).sum())
                    # store last timestamp (unix seconds)
                    try:
                        ts = df_day["timestamp"].max()
                        res_last_ts[d.key] = int(ts.timestamp()) if ts is not None else 0
                    except Exception:
                        res_last_ts[d.key] = 0
                except Exception:
                    # If data is missing or unreadable, just keep 0 as base.
                    res_kwh[d.key] = 0.0
                    res_last_ts[d.key] = 0

            def _apply() -> None:
                self._today_base_kwh = res_kwh
                self._today_base_last_ts = res_last_ts
                # Force a UI refresh of the visible "today kWh" labels
                try:
                    self._update_today_kwh_labels()
                except Exception:
                    pass

            try:
                self.after(0, _apply)
            except Exception:
                _apply()

        try:
            devs = list(self.cfg.devices)
        except Exception:
            devs = []
        th = threading.Thread(target=_job, args=(devs,), daemon=True)
        th.start()

    def _update_today_kwh_labels(self) -> None:
        """Update the per-device live cards (if currently visible)."""
        try:
            vars_map = getattr(self, "_live_latest_vars", {}) or {}
        except Exception:
            vars_map = {}
        for d in self._get_visible_devices():
            vars_ = vars_map.get(d.key)
            if not vars_:
                continue
            try:
                total = self._get_today_kwh_total(d.key)
                vars_["kwh_today"].set(f"{total:.3f} kWh")
            except Exception:
                pass

    def _get_today_kwh_total(self, device_key: str) -> float:
        base = float(self._today_base_kwh.get(device_key, 0.0) or 0.0)
        st = self._today_live_state.get(device_key) or {}
        live_kwh = float(st.get("kwh", 0.0) or 0.0)
        return base + live_kwh

    def _accumulate_live_kwh_today(self, device_key: str, ts: int, power_w: float) -> float:
        """Accumulate live energy (kWh) for the current local day.

        We only accumulate segments that are newer than the last imported timestamp for today,
        so we don't double count after a background sync.
        """
        from datetime import datetime

        day = datetime.fromtimestamp(int(ts)).date()
        st = self._today_live_state.get(device_key)
        if not st or st.get("date") != day:
            st = {"date": day, "kwh": 0.0, "last_ts": None, "last_p": None}
            self._today_live_state[device_key] = st

        base_last = int(self._today_base_last_ts.get(device_key, 0) or 0)

        last_ts = st.get("last_ts")
        last_p = st.get("last_p")

        # Don't accumulate before the imported baseline.
        if base_last and int(ts) <= base_last:
            st["last_ts"] = int(ts)
            st["last_p"] = float(power_w)
            return self._get_today_kwh_total(device_key)

        # If our previous point was before the baseline, restart accumulation from here.
        if base_last and last_ts is not None and int(last_ts) <= base_last:
            st["last_ts"] = int(ts)
            st["last_p"] = float(power_w)
            return self._get_today_kwh_total(device_key)

        if last_ts is None or last_p is None:
            st["last_ts"] = int(ts)
            st["last_p"] = float(power_w)
            return self._get_today_kwh_total(device_key)

        dt = float(int(ts) - int(last_ts))
        if dt > 0:
            # trapezoid integration: W * h -> Wh -> kWh
            wh = (float(last_p) + float(power_w)) / 2.0 * (dt / 3600.0)
            st["kwh"] = float(st.get("kwh", 0.0) or 0.0) + (wh / 1000.0)

        st["last_ts"] = int(ts)
        st["last_p"] = float(power_w)
        return self._get_today_kwh_total(device_key)

    def _start_today_kwh_base_refresh(self) -> None:
        """Compute today's consumed energy (kWh) from imported CSVs in a background thread."""
        try:
            devices = list(self.cfg.devices)
        except Exception:
            devices = []

        def worker() -> None:
            from datetime import datetime

            today = datetime.now().date()
            base_kwh: Dict[str, float] = {}
            base_last: Dict[str, int] = {}
            for d in devices:
                try:
                    df = self.storage.read_device_df(d.key)
                    if df is None or df.empty:
                        continue
                    # Filter to local "today"
                    df2 = df.copy()
                    df2 = df2.loc[df2["timestamp"].dt.date == today]
                    if df2.empty:
                        base_kwh[d.key] = 0.0
                        continue
                    e = calculate_energy(df2, method="auto")
                    base_kwh[d.key] = float(e["energy_kwh"].fillna(0).sum())
                    try:
                        last_ts = int(pd.Timestamp(df2["timestamp"].max()).timestamp())
                        base_last[d.key] = last_ts
                    except Exception:
                        pass
                except Exception:
                    # Keep silent here; live will still work.
                    continue

            def apply() -> None:
                # Apply results on UI thread
                self._today_base_kwh.update(base_kwh)
                self._today_base_last_ts.update(base_last)
                # Reset live accumulator so we don't double-count intervals that are now in the baseline.
                try:
                    from datetime import datetime
                    today = datetime.now().date()
                    for k, last_ts in base_last.items():
                        st = self._today_live_state.get(k)
                        if not st or st.get("date") != today:
                            continue
                        st["kwh"] = 0.0
                        st["last_ts"] = int(last_ts)
                        st["last_p"] = None
                except Exception:
                    pass
                self._update_today_kwh_labels()

            try:
                self.after(0, apply)
            except Exception:
                apply()

        # Avoid running multiple base refreshes in parallel.
        if getattr(self, "_today_base_thread", None) is not None:
            try:
                if self._today_base_thread.is_alive():
                    return
            except Exception:
                pass
        try:
            self._today_base_thread = threading.Thread(target=worker, daemon=True)
            self._today_base_thread.start()
        except Exception:
            pass
    # ---------------- Plots tab ----------------
    def _build_plots_tab(self) -> None:
        frm = self.tab_plots

        # Header
        top = ttk.Frame(frm)
        top.pack(fill="x", padx=12, pady=(10, 6))
        ttk.Label(top, text=self.t("plots.header")).pack(side="left")
        ttk.Button(top, text=self.t("plots.reload"), command=self._reload_data).pack(side="left", padx=(12, 0))

        # Debug: show which CSV columns are mapped to L1/L2/L3 and totals.
        try:
            ttk.Checkbutton(
                top,
                text="Debug: Spalten-Mapping",
                variable=self._plots_debug_mapping_enabled,
                command=self._redraw_plots_active,
            ).pack(side="left", padx=(12, 0))
            ttk.Label(top, textvariable=self._plots_debug_mapping_text).pack(side="left", padx=(10, 0))
        except Exception:
            pass

        # Optional absolute time range (shared across all plot tabs)
        rng = ttk.Frame(frm)
        rng.pack(fill="x", padx=12, pady=(0, 10))
        ttk.Label(rng, text=self.t("plots.range")).pack(side="left")
        ttk.Label(rng, text=self.t("common.from")).pack(side="left", padx=(10, 4))
        ttk.Entry(rng, textvariable=self._plots_start, width=12).pack(side="left")
        ttk.Label(rng, text=self.t("common.to")).pack(side="left", padx=(10, 4))
        ttk.Entry(rng, textvariable=self._plots_end, width=12).pack(side="left")
        ttk.Label(rng, text=self.t("common.date_hint")).pack(side="left", padx=(8, 0))
        ttk.Button(rng, text=self.t("btn.apply"), command=self._redraw_plots_active).pack(side="left", padx=10)
        ttk.Button(rng, text=self.t("btn.reset"), command=self._reset_plots_range).pack(side="left")

        # Metric tabs (no W/V/A toggle anymore): kWh, V, A, W
        nb = ttk.Notebook(frm)
        nb.pack(fill="both", expand=True, padx=12, pady=(0, 12))
        self._plots_metric_nb = nb

        # Reset plot objects
        self._plots_figs2 = {}
        self._plots_axes2 = {}
        self._plots_canvases2 = {}
        self._plots_device_nb = {}
        self._plots_device_order = {}

        # Shared W/V/A/W window controls (len + unit)
        unit_map = {
            "minutes": self.t("common.minutes"),
            "hours": self.t("common.hours"),
            "days": self.t("common.days"),
        }
        inv_unit_map = {v: k for k, v in unit_map.items()}

        def _sync_unit_display() -> None:
            try:
                internal = str(self._wva_unit.get() or "hours").strip().lower()
            except Exception:
                internal = "hours"
            self._wva_unit_display.set(unit_map.get(internal, unit_map.get("hours", "hours")))

        def _set_win(val: float, unit: str) -> None:
            try:
                self._wva_len.set(float(val))
                self._wva_unit.set(str(unit))
            except Exception:
                pass
            _sync_unit_display()
            self._redraw_plots_active()

        def _make_wva_controls(parent: ttk.Frame, metric_key: str) -> None:
            row = ttk.Frame(parent)
            row.pack(fill="x", pady=(8, 6))
            ttk.Label(row, text=self.t("plots.wva.window")).pack(side="left")
            preset_fr = ttk.Frame(row)
            preset_fr.pack(side="left", padx=(10, 0))
            for txt, v, u in (("5m", 5, "minutes"), ("15m", 15, "minutes"), ("1h", 1, "hours"), ("6h", 6, "hours"), ("24h", 24, "hours"), ("7d", 7, "days"), ("30d", 30, "days")):
                ttk.Button(preset_fr, text=txt, command=lambda vv=v, uu=u: _set_win(vv, uu)).pack(side="left", padx=2)
            ttk.Label(row, text=self.t("common.custom") + ":").pack(side="left", padx=(10, 4))
            ttk.Entry(row, width=6, textvariable=self._wva_len).pack(side="left")
            _sync_unit_display()
            cb_unit = ttk.Combobox(
                row,
                state="readonly",
                width=10,
                textvariable=self._wva_unit_display,
                values=[unit_map["minutes"], unit_map["hours"], unit_map["days"]],
            )
            cb_unit.pack(side="left", padx=(6, 0))

            def _on_unit_sel(_e=None) -> None:
                try:
                    disp = str(self._wva_unit_display.get() or "").strip()
                    internal = inv_unit_map.get(disp, "hours")
                    self._wva_unit.set(internal)
                except Exception:
                    pass
                self._redraw_plots_metric(metric_key)

            try:
                cb_unit.bind("<<ComboboxSelected>>", _on_unit_sel)
            except Exception:
                pass
            ttk.Button(row, text=self.t("btn.apply"), command=lambda mk=metric_key: self._redraw_plots_metric(mk)).pack(
                side="left", padx=(10, 0)
            )

        def _make_device_notebook(parent: ttk.Frame, metric_key: str, two_axes: bool = False) -> None:
            dev_nb = ttk.Notebook(parent)
            dev_nb.pack(fill="both", expand=True)
            self._plots_device_nb[metric_key] = dev_nb
            self._plots_device_order[metric_key] = []
            self._plots_figs2[metric_key] = {}
            self._plots_axes2[metric_key] = {}
            self._plots_canvases2[metric_key] = {}

            for d in list(getattr(self.cfg, "devices", []) or []):
                tab = ttk.Frame(dev_nb)
                dev_nb.add(tab, text=d.name)

                fig = Figure(figsize=(11, 3.6), dpi=120)
                # Axes are created during redraw (because V/A uses 2 axes)
                canvas = FigureCanvasTkAgg(fig, master=tab)
                canvas.get_tk_widget().pack(fill="both", expand=True)

                self._plots_device_order[metric_key].append(d.key)
                self._plots_figs2[metric_key][d.key] = fig
                self._plots_canvases2[metric_key][d.key] = canvas
                # Resize handling
                try:
                    # IMPORTANT: Matplotlib's TkAgg backend already binds <Configure>
                    # on its Tk canvas to handle resizing (it recreates the backing
                    # PhotoImage). A plain `.bind(...)` would replace that binding,
                    # breaking auto-resize. We therefore add our handler *in addition*
                    # to the existing binding.
                    canvas.get_tk_widget().bind("<Configure>", self._on_plots_canvas_configure, add="+")
                except Exception:
                    pass

            # Redraw when switching device tabs
            try:
                dev_nb.bind("<<NotebookTabChanged>>", lambda _e, mk=metric_key: self._redraw_plots_metric(mk))
            except Exception:
                pass

        # --- kWh tab ---
        tab_kwh = ttk.Frame(nb)
        nb.add(tab_kwh, text="kWh")
        kwh_ctl = ttk.Frame(tab_kwh)
        kwh_ctl.pack(fill="x", pady=(8, 6))
        ttk.Label(kwh_ctl, text=self.t("plots.kwh.granularity")).pack(side="left")
        kwh_btns = ttk.Frame(kwh_ctl)
        kwh_btns.pack(side="left", padx=(10, 0))
        for mode in ("all", "hours", "days", "weeks", "months"):
            ttk.Button(
                kwh_btns,
                text=self.t(f"plots.mode.{mode}"),
                command=lambda m=mode: (self._plots_mode.set(m), self._redraw_plots_metric("kwh")),
            ).pack(side="left", padx=3)

        # --- Custom range: last N units (hours/days/weeks/months) ---
        last_ctl = ttk.Frame(kwh_ctl)
        last_ctl.pack(side="left", padx=(16, 0))
        ttk.Label(last_ctl, text=self.t("plots.kwh.last")).pack(side="left")

        sp_n = ttk.Spinbox(last_ctl, from_=1, to=9999, width=5, textvariable=self._plots_last_n)
        sp_n.pack(side="left", padx=(6, 0))

        # NOTE: keep names distinct from the W/V/A window unit_map above (closure)
        kwh_unit_map = {
            "hours": self.t("plots.mode.hours"),
            "days": self.t("plots.mode.days"),
            "weeks": self.t("plots.mode.weeks"),
            "months": self.t("plots.mode.months"),
        }
        kwh_inv_unit_map = {v: k for k, v in kwh_unit_map.items()}
        unit_display = tk.StringVar(
            value=kwh_unit_map.get(str(self._plots_last_unit.get() or "days"), kwh_unit_map["days"])
        )
        cb_unit = ttk.Combobox(
            last_ctl,
            width=10,
            state="readonly",
            textvariable=unit_display,
            values=list(kwh_unit_map.values()),
        )
        cb_unit.pack(side="left", padx=(6, 0))

        def _apply_last_mode() -> None:
            try:
                disp = str(unit_display.get() or "").strip()
                unit = kwh_inv_unit_map.get(disp, str(self._plots_last_unit.get() or "days"))
            except Exception:
                unit = str(self._plots_last_unit.get() or "days")
            try:
                n = int(self._plots_last_n.get() or 0)
            except Exception:
                n = 0
            n = max(1, min(9999, n))
            try:
                self._plots_last_unit.set(unit)
            except Exception:
                pass
            self._plots_mode.set(f"{unit}:{n}")
            self._redraw_plots_metric("kwh")

        try:
            cb_unit.bind("<<ComboboxSelected>>", lambda _e: _apply_last_mode())
        except Exception:
            pass
        ttk.Button(last_ctl, text=self.t("btn.apply"), command=_apply_last_mode).pack(side="left", padx=(8, 0))

        _make_device_notebook(tab_kwh, "kwh")

        # --- V tab ---
        tab_v = ttk.Frame(nb)
        nb.add(tab_v, text="V")
        _make_wva_controls(tab_v, "V")
        _make_device_notebook(tab_v, "V", two_axes=True)

        # --- A tab ---
        tab_a = ttk.Frame(nb)
        nb.add(tab_a, text="A")
        _make_wva_controls(tab_a, "A")
        _make_device_notebook(tab_a, "A", two_axes=True)

        # --- W tab ---
        tab_w = ttk.Frame(nb)
        nb.add(tab_w, text="W")
        _make_wva_controls(tab_w, "W")
        _make_device_notebook(tab_w, "W", two_axes=False)

        # --- VAR tab ---
        tab_var = ttk.Frame(nb)
        nb.add(tab_var, text="VAR")
        _make_wva_controls(tab_var, "VAR")
        _make_device_notebook(tab_var, "VAR", two_axes=True)

        # --- cos φ tab ---
        tab_pf = ttk.Frame(nb)
        nb.add(tab_pf, text="cos φ")
        _make_wva_controls(tab_pf, "COSPHI")
        _make_device_notebook(tab_pf, "COSPHI", two_axes=True)

        # Redraw when switching metric tabs
        try:
            nb.bind("<<NotebookTabChanged>>", lambda _e: self._redraw_plots_active())
        except Exception:
            pass

        # Initial draw
        try:
            self.after(300, self._redraw_plots_active)
        except Exception:
            self._redraw_plots_active()

    def _on_plots_view_changed(self) -> None:
        """Show correct controls and redraw plots (classic in-app rendering)."""
        view = str(self._plots_view.get() or "timeseries")
        if view == "kwh":
            try:
                self._plots_controls_kwh.pack(fill="x", padx=12, pady=(0, 6))
            except Exception:
                pass
            try:
                self._plots_controls_ts.pack_forget()
            except Exception:
                pass
        else:
            try:
                self._plots_controls_ts.pack(fill="x", padx=12, pady=(0, 6))
            except Exception:
                pass
            try:
                self._plots_controls_kwh.pack_forget()
            except Exception:
                pass
        self._redraw_plots_current()

    def _redraw_plots_current(self) -> None:
        view = str(self._plots_view.get() or 'timeseries')
        if view == 'kwh':
            self._redraw_plots_kwh()
        else:
            self._redraw_plots_timeseries()


    # --- New Plots tab rendering (metric tabs) ---
    def _active_metric_key(self) -> str:
        try:
            nb = getattr(self, "_plots_metric_nb", None)
            if nb is None:
                return "kwh"
            sel = nb.select()
            if not sel:
                return "kwh"
            txt = str(nb.tab(sel, "text") or "").strip()
            # We use the visible tab labels as keys.
            if txt.lower() == "kwh":
                return "kwh"
            tnorm = txt.strip()
            if tnorm.upper() in {"V", "A", "W", "VAR"}:
                return tnorm.upper()
            # cos φ tab (accept variants)
            tl = tnorm.lower().replace(" ", "").replace("φ", "phi")
            if tl in {"cosphi"} or tl.startswith("cos"):
                return "COSPHI"
            return "kwh"
        except Exception:
            return "kwh"

    def _selected_device_key(self, metric_key: str) -> Optional[str]:
        try:
            dev_nb = self._plots_device_nb.get(metric_key)
            order = self._plots_device_order.get(metric_key) or []
            if dev_nb is None or not order:
                return None
            sel = dev_nb.select()
            if not sel:
                return order[0]
            idx = int(dev_nb.index(sel))
            if 0 <= idx < len(order):
                return order[idx]
            return order[0]
        except Exception:
            return None

    def _redraw_plots_active(self) -> None:
        """Redraw the currently active metric tab (and its selected device)."""
        self._redraw_plots_metric(self._active_metric_key())

    def _redraw_plots_metric(self, metric_key: str) -> None:
        metric_key = str(metric_key or "kwh")
        if metric_key.lower() == "kwh":
            self._redraw_plots_kwh2()
        else:
            self._redraw_plots_wva2(metric_key.upper())

    def _redraw_plots_kwh2(self) -> None:
        if not self._ensure_data_loaded():
            return
        mode = str(self._plots_mode.get() or "days")

        pstart = _parse_date_flexible(self._plots_start.get())
        pend = _parse_date_flexible(self._plots_end.get())
        if pstart is not None and pend is not None and pend < pstart:
            pstart, pend = pend, pstart

        unit_gross = float(self.cfg.pricing.unit_price_gross())
        dev_key = self._selected_device_key("kwh")
        keys = [dev_key] if dev_key else list((self._plots_device_order.get("kwh") or []))

        for key in keys:
            if not key:
                continue
            dcfg = next((d for d in self.cfg.devices if d.key == key), None)
            cd = self.computed.get(key)
            fig = self._plots_figs2.get("kwh", {}).get(key)
            canvas = self._plots_canvases2.get("kwh", {}).get(key)
            if dcfg is None or cd is None or fig is None or canvas is None:
                continue

            df_use = filter_by_time(cd.df, start=pstart, end=pend)
            labels, values = self._stats_series(df_use, mode)

            w = canvas.get_tk_widget()
            self._resize_figure_to_widget(fig, w, dpi=self._dpi_for_widget(w), min_h_px=320)
            fig.clear()
            ax = fig.add_subplot(111)
            try:
                setattr(ax, "_shelly_fixed_layout", True)
            except Exception:
                pass

            ax.set_ylabel("kWh")
            bars = ax.bar(range(len(values)), values)
            base = self._font_base_for_widget(w)
            self._apply_xticks(ax, labels, base_font=base)
            total = float(sum(values))
            cost = total * unit_gross
            if pstart is None and pend is None:
                range_lbl = ""
            else:
                a = pstart.date().isoformat() if pstart is not None else "…"
                b = pend.date().isoformat() if pend is not None else "…"
                range_lbl = f" | {a}–{b}"
            ax.set_title(f"{dcfg.name} – {self._pretty_kwh_mode(mode)}{range_lbl} | {_fmt_kwh(total)} ({_fmt_eur(cost)})")
            ax.grid(True, axis="y", alpha=0.3)
            self._annotate_bars(ax, bars)
            try:
                fig.subplots_adjust(left=0.10, right=0.97, top=0.92, bottom=0.32)
            except Exception:
                pass
            self._apply_axis_layout(fig, ax, w, legend=False)
            canvas.draw_idle()

    def _redraw_plots_wva2(self, metric: str) -> None:
        if not self._ensure_data_loaded():
            return

        metric = str(metric or "W").upper().strip()
        window_delta = self._wva_window_delta()

        pstart = _parse_date_flexible(self._plots_start.get())
        pend = _parse_date_flexible(self._plots_end.get())
        if pstart is not None and pend is not None and pend < pstart:
            pstart, pend = pend, pstart

        dev_key = self._selected_device_key(metric)
        keys = [dev_key] if dev_key else list((self._plots_device_order.get(metric) or []))

        for key in keys:
            if not key:
                continue
            dcfg = next((d for d in self.cfg.devices if d.key == key), None)
            cd = self.computed.get(key)
            fig = self._plots_figs2.get(metric, {}).get(key)
            canvas = self._plots_canvases2.get(metric, {}).get(key)
            if dcfg is None or cd is None or fig is None or canvas is None:
                continue

            # Source dataframe: prefer CSV-based history, but for V/A we can
            # fall back to the in-memory live store (which contains per-phase
            # V/A) if the CSVs do not contain those columns.
            df_src = cd.df
            df_src = df_src.sort_values("timestamp") if df_src is not None and not df_src.empty else df_src

            if metric in {"V", "A"} and not self._df_has_wva_cols(df_src, metric):
                df_live = self._df_from_live_store(key)
                if df_live is not None and not df_live.empty:
                    df_src = df_live

            if pstart is None and pend is None and df_src is not None and not df_src.empty:
                pend_eff = pd.Timestamp(df_src["timestamp"].max())
                pstart_eff = pend_eff - window_delta
            else:
                pstart_eff = pstart
                pend_eff = pend
            df_use = filter_by_time(df_src, start=pstart_eff, end=pend_eff)

            w = canvas.get_tk_widget()
            self._resize_figure_to_widget(fig, w, dpi=self._dpi_for_widget(w), min_h_px=320)
            fig.clear()

            # Per-phase sub-plot: users expect phase lines not only for V/A, but also for
            # active power (W) as well as derived VAR/cosφ metrics.
            need_phase_subplot = metric in {"W", "V", "A", "VAR", "Q", "COSPHI", "PF"}
            if need_phase_subplot:
                ax1 = fig.add_subplot(211)
                ax2 = fig.add_subplot(212, sharex=ax1)
            else:
                ax1 = fig.add_subplot(111)
                ax2 = None
            try:
                setattr(ax1, "_shelly_fixed_layout", True)
            except Exception:
                pass
            if ax2 is not None:
                try:
                    setattr(ax2, "_shelly_fixed_layout", True)
                except Exception:
                    pass

            if df_use is None or df_use.empty:
                # More helpful message for V/A when no phases/history exists.
                try:
                    if metric in {"V", "A"} and not self._df_has_wva_cols(cd.df, metric):
                        ax1.set_title(self.t("plots.no_data_va_hint"))
                    else:
                        ax1.set_title(self.t("plots.no_data"))
                except Exception:
                    ax1.set_title(self.t("plots.no_data"))
                ax1.set_ylabel(metric)
                canvas.draw_idle()
                continue

            # Total series (uses robust detection; returned indexed by datetime)
            ys, ylabel = self._wva_series(df_use, metric)
            # Save mapping info for debug display
            try:
                mp = str(getattr(self, "_last_wva_mapping_text", "") or "")
                self._plots_last_mapping[(metric, key)] = mp
                if bool(self._plots_debug_mapping_enabled.get()):
                    # Show mapping for the currently drawn device
                    self._plots_debug_mapping_text.set(mp)
                else:
                    self._plots_debug_mapping_text.set("")
            except Exception:
                pass
            if ys is None or ys.empty:
                ax1.set_title(self.t("plots.no_data"))
                ax1.set_ylabel(metric)
                canvas.draw_idle()
                continue

            xs = ys.index
            ax1.plot(xs, ys.values)
            ax1.set_ylabel(ylabel)
            ax1.grid(True, axis="y", alpha=0.3)

            # If available, overlay phase lines
            if ax2 is not None:
                any_line = False
                ts = pd.to_datetime(df_use.get("timestamp"), errors="coerce")
                phase_series = self._wva_phase_series(df_use, metric)
                try:
                    if int(getattr(dcfg, "phases", 3) or 3) <= 1:
                        phase_series = {"L1": phase_series.get("L1")} if "L1" in phase_series else {}
                except Exception:
                    pass
                for lab, ser in phase_series.items():
                    try:
                        s2 = pd.to_numeric(ser, errors="coerce")
                        tmp = pd.Series(s2.to_numpy(), index=ts).dropna().sort_index()
                    except Exception:
                        continue
                    if tmp.empty:
                        continue
                    ax2.plot(tmp.index, tmp.values, label=lab)
                    any_line = True
                ax2.set_ylabel(ylabel)
                ax2.grid(True, axis="y", alpha=0.3)
                if any_line:
                    ax2.legend(loc="upper right", fontsize=max(8, self._font_base_for_widget(w) - 1))
                ax2.set_xlabel(self.t("live.time"))
                try:
                    for t in ax1.get_xticklabels():
                        t.set_visible(False)
                except Exception:
                    pass
            else:
                ax1.set_xlabel(self.t("live.time"))

            self._apply_smart_date_axis(ax1, w)
            if ax2 is not None:
                self._apply_smart_date_axis(ax2, w)

            a = pstart_eff.date().isoformat() if pstart_eff is not None else "…"
            b = pend_eff.date().isoformat() if pend_eff is not None else "…"
            ax1.set_title(f"{dcfg.name} – {ylabel} | {a}–{b}")

            try:
                if ax2 is None:
                    fig.subplots_adjust(left=0.10, right=0.97, top=0.92, bottom=0.26)
                else:
                    fig.subplots_adjust(left=0.10, right=0.97, top=0.92, bottom=0.18, hspace=0.30)
            except Exception:
                pass
            self._apply_axis_layout(fig, ax1, w, legend=False)
            if ax2 is not None:
                self._apply_axis_layout(fig, ax2, w, legend=True)
            canvas.draw_idle()


    def _reset_plots_range(self) -> None:
        """Clear the optional absolute time range (von/bis) in the Plots tab."""
        try:
            if hasattr(self, "_plots_start"):
                self._plots_start.set("")
            if hasattr(self, "_plots_end"):
                self._plots_end.set("")
        except Exception:
            pass
        try:
            self._redraw_plots_active()
        except Exception:
            self._redraw_plots_current()

    def _copy_to_clipboard(self, text: str) -> None:
        try:
            self.clipboard_clear()
            self.clipboard_append(str(text or ""))
            self.update_idletasks()
        except Exception:
            pass

    def _open_log_file(self) -> None:
        """Open today's log file in the OS default app."""
        try:
            p = self._log_path or get_log_path()
            if p is None:
                p = (self.project_root / "logs").resolve()
            # If we got a directory, try to pick the newest app_*.log
            if isinstance(p, Path) and p.exists() and p.is_dir():
                cand = sorted(p.glob("app_*.log"))
                p = cand[-1] if cand else p
            if isinstance(p, Path):
                p = p.resolve()

            if sys.platform.startswith("darwin"):
                subprocess.Popen(["open", str(p)])
            elif os.name == "nt":
                os.startfile(str(p))  # type: ignore[attr-defined]
            else:
                subprocess.Popen(["xdg-open", str(p)])
        except Exception as e:
            try:
                messagebox.showerror(self.t("common.log"), self.t('log.open_error', e=e))
            except Exception:
                pass

    def _ensure_web_dashboard(self) -> Optional[str]:
        """Ensure the local web dashboard is running and return its base URL."""
        try:
            if self._live_state_store is None:
                self._live_state_store = LiveStateStore(max_points=1200)
        except Exception:
            pass

        try:
            if self._live_web is None:
                # Always mirror current device metadata into a runtime file so the
                # web dashboard can recover it even if it was started without devices.
                try:
                    self._write_runtime_devices_meta()
                except Exception:
                    pass
                devs_all = [(d.key, d.name) for d in self.cfg.devices]
                devs_meta = [
                    {"key": d.key, "name": d.name, "kind": str(getattr(d, "kind", "") or "")}
                    for d in self.cfg.devices
                ]
                self._live_web = LiveWebDashboard(
                    self._live_state_store,
                    port=int(self.cfg.ui.live_web_port),
                    refresh_seconds=float(self.cfg.ui.live_web_refresh_seconds),
                    window_minutes=int(self.cfg.ui.live_window_minutes),
                    devices=devs_all,
                    devices_meta=devs_meta,
                    out_dir=self.project_root,
                    lang=self.lang,
                    on_window_change=self._on_web_window_change,
                    on_action=self._web_action_dispatch,
                )
                self._live_web.start()
            return self._live_web.url() if self._live_web else None
        except Exception:
            return None

    def _plots_build_url(self, device_keys: List[str], view: Optional[str] = None) -> str:
        base = self._ensure_web_dashboard() or ""
        if not base:
            return ""
        view = str(view or self._plots_view.get() or "timeseries")

        # Query params
        params: Dict[str, str] = {
            "view": view,
            "devices": ",".join([k for k in device_keys if k]),
            "lang": str(self.lang or "de"),
        }
        if view == "kwh":
            params["mode"] = str(self._plots_mode.get() or "days")
        else:
            params["metric"] = str(self._wva_metric.get() or "W")
            try:
                params["len"] = str(float(self._wva_len.get()))
            except Exception:
                params["len"] = "24"
            params["unit"] = str(self._wva_unit.get() or "hours")

        # Optional range
        try:
            if str(self._plots_start.get() or "").strip():
                params["start"] = str(self._plots_start.get()).strip()
            if str(self._plots_end.get() or "").strip():
                params["end"] = str(self._plots_end.get()).strip()
        except Exception:
            pass

        from urllib.parse import urlencode

        return base.rstrip("/") + "/plots?" + urlencode(params)

    def _update_plots_urls(self) -> None:
        """Refresh plots after control changes.

        This method name is kept for backwards compatibility (older UI code
        used it for updating Plotly/URL-based plots). In the classic in-app
        plot mode we simply redraw the current plots.
        """
        try:
            self._redraw_plots_active()
        except Exception:
            try:
                self._redraw_plots_current()
            except Exception:
                pass
    def _open_plots_web(self, page_key: str, embed: bool = False) -> None:
        """Open the Plotly Plots page for a specific 2-device page."""
        try:
            url = (self._plots_url_vars.get(page_key).get() if self._plots_url_vars.get(page_key) else "")
        except Exception:
            url = ""
        if not url:
            # Ensure at least a fresh URL
            self._update_plots_urls()
            try:
                url = (self._plots_url_vars.get(page_key).get() if self._plots_url_vars.get(page_key) else "")
            except Exception:
                url = ""
        if not url:
            try:
                messagebox.showerror(self.t("common.error"), self.t("plots.web.no_server"))
            except Exception:
                pass
            return
        if embed:
            # Open an embedded WebView in a separate process (pywebview/WebKit).
            try:
                import subprocess, sys
                title = self.t("plots.web.window")
                subprocess.Popen([sys.executable, '-m', 'shelly_analyzer.webview_runner', url, title])
                return
            except Exception:
                pass

        try:
            import webbrowser
            webbrowser.open(url)
        except Exception:
            pass




    def _redraw_plots_timeseries(self) -> None:
        """Classic in-app plots: W/V/A time series."""
        try:
            self._redraw_wva_plots()
        except Exception:
            pass


    def _redraw_plots_kwh(self) -> None:
        """Classic in-app plots: kWh bar charts."""
        try:
            self._redraw_stats_plots()
        except Exception:
            pass


    def _on_plots_canvas_configure(self, _event=None) -> None:
        """Handle window/monitor size changes for the Plots tab.

        We do a fast figure-only resize immediately, and debounce a full redraw
        (which re-filters data) only if needed.
        """
        # Start a short resize watch (fullscreen settles late on macOS).
        try:
            self._kick_plots_resize_watch()
        except Exception:
            pass

        # Fast resize so fullscreen looks correct instantly.
        try:
            self._resize_plots_figures_only()
        except Exception:
            pass

        # Debounce the heavier redraw.
        try:
            if self._plots_relayout_job:
                self.after_cancel(self._plots_relayout_job)
        except Exception:
            pass

        def _do() -> None:
            try:
                # Prefer the new metric-tab plots; fall back to legacy.
                try:
                    self._redraw_plots_active()
                except Exception:
                    self._redraw_plots_current()
            except Exception:
                pass

        try:
            self._plots_relayout_job = self.after(350, _do)
        except Exception:
            _do()

    def _wva_window_delta(self) -> pd.Timedelta:
        try:
            val = float(self._wva_len.get())
        except Exception:
            val = 24.0
        val = max(0.1, val)
        unit = str(self._wva_unit.get() or "hours").lower().strip()
        if unit.startswith("min"):
            return pd.Timedelta(minutes=val)
        if unit.startswith("day"):
            return pd.Timedelta(days=val)
        return pd.Timedelta(hours=val)

    def _df_has_wva_cols(self, df: Optional[pd.DataFrame], metric: str) -> bool:
        """Return True if *df* appears to contain voltage/current columns.

        CSV EMData downloads typically contain only power/energy. Per-phase
        voltage/current are available from live polling and from some legacy
        phases CSVs. We use this to decide whether to fall back to the live
        in-memory store for V/A plots.
        """
        if df is None or df.empty:
            return False
        m = str(metric or "").upper().strip()
        cols = [str(c).lower() for c in df.columns]
        if m == "V":
            tokens = ["voltage", "avg_voltage", "volt", "_voltage"]
            if any(any(t in c for t in tokens) for c in cols):
                return True
            # Some legacy exports use a_u/b_u/c_u
            if any(c.endswith("_u") or c.endswith("u") or "_u_" in c for c in cols):
                return True
            return False
        if m == "A":
            tokens = ["current", "avg_current", "amp", "_current"]
            if any(any(t in c for t in tokens) for c in cols):
                return True
            # Some legacy exports use a_i/b_i/c_i
            if any(c.endswith("_i") or c.endswith("i") or "_i_" in c for c in cols):
                return True
            return False
        return True

    def _df_from_live_store(self, device_key: str) -> pd.DataFrame:
        """Build a dataframe from the in-memory live dashboard store.

        Columns are named to match the CSV-based helpers:
          timestamp, ts, total_power, a_voltage/b_voltage/c_voltage,
          a_current/b_current/c_current
        """
        try:
            snap = self._live_state_store.snapshot() if self._live_state_store is not None else {}
            arr = snap.get(str(device_key), []) or []
        except Exception:
            arr = []
        if not arr:
            # If the user is on the Plots tab and requests V/A but live was
            # never started, we auto-start live polling so V/A history becomes
            # available without extra clicks.
            try:
                if not self._live_pollers:
                    self._start_live()
                    # Repaint once we have a few points.
                    try:
                        self.after(1500, self._redraw_plots_active)
                    except Exception:
                        pass
            except Exception:
                pass
            return pd.DataFrame(columns=[
                "timestamp","ts","total_power",
                "a_voltage","b_voltage","c_voltage",
                "a_current","b_current","c_current",
            ])

        df = pd.DataFrame(arr)
        if df is None or df.empty:
            return pd.DataFrame()
        try:
            ts_num = pd.to_numeric(df.get("ts"), errors="coerce")
            ts_dt = pd.to_datetime(ts_num, unit="s", errors="coerce")
        except Exception:
            ts_dt = pd.to_datetime([], errors="coerce")

        out = pd.DataFrame({
            "timestamp": ts_dt,
            "ts": ts_num if "ts_num" in locals() else pd.Series([], dtype="float64"),
            "total_power": pd.to_numeric(df.get("power_total_w"), errors="coerce"),
            "a_voltage": pd.to_numeric(df.get("va"), errors="coerce"),
            "b_voltage": pd.to_numeric(df.get("vb"), errors="coerce"),
            "c_voltage": pd.to_numeric(df.get("vc"), errors="coerce"),
            "a_current": pd.to_numeric(df.get("ia"), errors="coerce"),
            "b_current": pd.to_numeric(df.get("ib"), errors="coerce"),
            "c_current": pd.to_numeric(df.get("ic"), errors="coerce"),
        })
        out = out.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
        # Deduplicate by timestamp (keep last)
        try:
            out = out.drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)
        except Exception:
            pass
        return out
    def _wva_series(self, df: pd.DataFrame, metric: str) -> Tuple[pd.Series, str]:
        # Return a time-indexed series for the web plots (metric W/V/A).
        if df is None or df.empty:
            return pd.Series(dtype=float), metric

        metric_u = (metric or '').upper()
        cols_lower = {c.lower(): c for c in df.columns}

        # Timestamp source (keep raw values so we can infer epoch units reliably)
        if 'timestamp' in cols_lower:
            ts_raw = df[cols_lower['timestamp']]
            if pd.api.types.is_numeric_dtype(ts_raw):
                ts_num = pd.to_numeric(ts_raw, errors='coerce')
                mx = float(ts_num.dropna().max()) if ts_num.notna().any() else 0.0
                unit = 's'
                if mx > 1e15:
                    unit = 'ns'
                elif mx > 1e12:
                    unit = 'ms'
                ts = pd.to_datetime(ts_num, unit=unit, errors='coerce')
            else:
                ts = pd.to_datetime(ts_raw, errors='coerce')
        elif isinstance(df.index, pd.DatetimeIndex):
            ts = pd.to_datetime(df.index, errors='coerce')
        elif 'ts' in cols_lower:
            # epoch seconds (common in raw exports)
            ts_num = pd.to_numeric(df[cols_lower['ts']], errors='coerce')
            # infer unit roughly
            mx = float(ts_num.dropna().max()) if ts_num.notna().any() else 0.0
            unit = 's'
            if mx > 1e15:
                unit = 'ns'
            elif mx > 1e12:
                unit = 'ms'
            elif mx > 1e9:
                unit = 's'
            ts = pd.to_datetime(ts_num, unit=unit, errors='coerce')
        else:
            return pd.Series(dtype=float), metric

        # Helpers
        def first_col(candidates):
            for n in candidates:
                key = n.lower()
                if key in cols_lower:
                    return cols_lower[key]
            return None

        # NOTE: Be careful with 1-letter "kind" tokens (like "a" or "v"):
        # they match far too many unrelated columns (e.g. "phase_angle_l1").
        # Use more specific matching for phase columns.
        def phase_cols(kind: str, phase_tokens=None):
            """Best-effort phase-column picker for kind in {"power","voltage","current"}.
        
            IMPORTANT: avoid naive substring tokens like 'a_'/'c_' because they can
            match unrelated columns such as 'ac_current' and inflate totals. We
            instead look for phase markers with underscore boundaries.
            """
            out = []
            # Phase markers with clear boundaries (e.g. _l1_, _phase_a_, a_current, current_b)
            phase_re = re.compile(r"(^|_)(l1|l2|l3|phase1|phase2|phase3|phase_a|phase_b|phase_c|a|b|c)(_|$)")
            for c in df.columns:
                cl = str(c).lower()
        
                # Must look like a phase-specific column
                if not phase_re.search(cl):
                    continue
        
                # Exclude obvious non-measurement columns
                if any(bad in cl for bad in ['price', 'kwh', 'energy', 'cost', 'total_kwh']):
                    continue
        
                if kind == 'power':
                    if ('power' in cl) or ('watt' in cl) or ('active_power' in cl) or ('apower' in cl) or cl.endswith('_p') or cl.startswith('p_'):
                        out.append(c)
                elif kind == 'voltage':
                    # match voltage/volt or common 'u' naming
                    if ('voltage' in cl) or ('volt' in cl) or cl == 'u' or cl.startswith('u_') or cl.endswith('_u'):
                        out.append(c)
                elif kind == 'current':
                    # match current/ampere or common 'i' naming; avoid phase-angle columns
                    if ('current' in cl) or ('amp' in cl) or ('amps' in cl) or cl == 'i' or cl.startswith('i_') or cl.endswith('_i'):
                        if 'angle' not in cl:
                            out.append(c)
            return out


        def _collapse_phase_stat_cols(cols, kind: str):
            """Collapse per-phase statistic columns (max/min/avg) into one series per phase.

            Many Shelly 3EM CSV exports provide columns like a_max_current, a_min_current, a_avg_current
            (and the same for b/c). For plots, we must NOT aggregate max+min+avg; instead pick avg when
            available, otherwise a sensible fallback (mean of max/min).

            Returns: dict phase_label -> numeric Series. phase_label uses 'L1','L2','L3'.
            """

            def _phase_label(col: str):
                cl = str(col).lower()
                for token, lab in (
                    ('phase_a', 'L1'), ('phase_b', 'L2'), ('phase_c', 'L3'),
                    ('phase1', 'L1'), ('phase2', 'L2'), ('phase3', 'L3'),
                    ('l1', 'L1'), ('l2', 'L2'), ('l3', 'L3'),
                ):
                    if re.search(rf'(^|_)({token})(_|$)', cl):
                        return lab
                if cl.startswith('a_') or re.search(r'(^|_)(a)(_|$)', cl):
                    return 'L1'
                if cl.startswith('b_') or re.search(r'(^|_)(b)(_|$)', cl):
                    return 'L2'
                if cl.startswith('c_') or re.search(r'(^|_)(c)(_|$)', cl):
                    return 'L3'
                return None

            groups = {'L1': [], 'L2': [], 'L3': []}
            for c in cols:
                lab = _phase_label(c)
                if lab in groups:
                    groups[lab].append(c)

            if kind == 'current':
                avg_tag, max_tag, min_tag = 'avg_current', 'max_current', 'min_current'
                plain_re = re.compile(r'(^|_)current(_|$)')
            elif kind == 'voltage':
                avg_tag, max_tag, min_tag = 'avg_voltage', 'max_voltage', 'min_voltage'
                plain_re = re.compile(r'(^|_)voltage(_|$)|(^|_)volt(_|$)|(^|_)u(_|$)')
            elif kind == 'power':
                avg_tag, max_tag, min_tag = 'avg_power', 'max_power', 'min_power'
                # accept act_power/active_power/apower/power without max/min
                plain_re = re.compile(r'(^|_)(act_power|active_power|apower|power|watts|w)(_|$)')
            else:
                avg_tag, max_tag, min_tag = 'avg', 'max', 'min'
                plain_re = re.compile(r'.*')

            def _num(col):
                return pd.to_numeric(df[col], errors='coerce')

            out = {}
            for lab, cols2 in groups.items():
                if not cols2:
                    continue

                # For power totals from per-phase columns, prefer active power (act_power/active_power/apower)
                # and avoid apparent power (aprt/apparent) unless nothing else exists.
                if kind == "power":
                    try:
                        cols_act = [
                            c for c in cols2
                            if (
                                ("act_power" in str(c).lower())
                                or ("active_power" in str(c).lower())
                                or (("apower" in str(c).lower()) and ("aprt" not in str(c).lower()))
                            )
                            and ("aprt" not in str(c).lower())
                            and ("apparent" not in str(c).lower())
                        ]
                        cols_non_apparent = [c for c in cols2 if ("aprt" not in str(c).lower()) and ("apparent" not in str(c).lower())]
                        if cols_act:
                            cols2 = cols_act
                        elif cols_non_apparent:
                            cols2 = cols_non_apparent
                    except Exception:
                        pass
                cols2_l = [str(c).lower() for c in cols2]


                # Special handling for power (W): exports often provide *_max_act_power/_min_act_power/_avg_act_power
                # per phase. For plotting and total sums we must pick ONE representative series per phase:
                # prefer avg; else mean(max,min); else plain. Never fall back to summing raw max+min columns.
                if kind == "power":
                    def _is_active_power(cl: str) -> bool:
                        cl = cl or ""
                        if ("aprt" in cl) or ("apparent" in cl):
                            return False
                        return ("act_power" in cl) or ("active_power" in cl) or (("apower" in cl) and ("aprt" not in cl)) or re.search(r"(^|_)power(_|$)", cl)

                    # Narrow to active (non-apparent) columns if possible
                    try:
                        cols_active = [c for c in cols2 if _is_active_power(str(c).lower())]
                        if cols_active:
                            cols2 = cols_active
                            cols2_l = [str(c).lower() for c in cols2]
                    except Exception:
                        pass

                    # 1) avg / mean
                    cavg = next((c for c in cols2 if ("avg" in str(c).lower() or "_mean" in str(c).lower()) and _is_active_power(str(c).lower())), None)
                    if cavg is not None:
                        out[lab] = _num(cavg)
                        continue

                    # 2) max/min -> mean (act_power style)
                    cmax = next((c for c in cols2 if ("max" in str(c).lower()) and _is_active_power(str(c).lower())), None)
                    cmin = next((c for c in cols2 if ("min" in str(c).lower()) and _is_active_power(str(c).lower())), None)
                    if cmax is not None and cmin is not None:
                        out[lab] = (_num(cmax) + _num(cmin)) / 2.0
                        continue

                    # 3) plain
                    cplain = next((c for c in cols2 if _is_active_power(str(c).lower()) and not any(tag in str(c).lower() for tag in ["avg", "max", "min"])), None)
                    if cplain is not None:
                        out[lab] = _num(cplain)
                        continue

                # 1) avg
                for c in cols2:
                    if avg_tag in str(c).lower():
                        out[lab] = _num(c)
                        break
                if lab in out:
                    continue

                # 2) plain (exclude max/min/avg)
                plain = []
                for c in cols2:
                    cl = str(c).lower()
                    if (avg_tag in cl) or (max_tag in cl) or (min_tag in cl):
                        continue
                    if plain_re.search(cl):
                        plain.append(c)
                if plain:
                    out[lab] = _num(plain[0])
                    continue

                # 3) max/min -> mean
                cmax = None
                cmin = None
                for c in cols2:
                    cl = str(c).lower()
                    if (max_tag in cl) and cmax is None:
                        cmax = c
                    elif (min_tag in cl) and cmin is None:
                        cmin = c
                if cmax is not None and cmin is not None:
                    out[lab] = (_num(cmax) + _num(cmin)) / 2.0
                elif cmax is not None:
                    out[lab] = _num(cmax)
                elif cmin is not None:
                    out[lab] = _num(cmin)

            return out

        ylab = metric_u
        mapping_text = ""

        if metric_u == 'W':
            ylab = 'W'
            # Prefer average/instantaneous power columns over max/min statistics.
            power_col = first_col([
                'power_total_w',
                'total_act_power', 'total_active_power', 'total_apower',
                'total_power', 'total_w', 'total_power_w', 'total_act_power_w',
                'total_act_power_ph', 'total_power_ph',
                'avg_power', 'avg_active_power', 'avg_act_power', 'avg_apower', 'avg_w',
                'active_power', 'act_power', 'apower',
                'power', 'watts', 'w', 'power_w'
            ])
            if power_col:
                y = pd.to_numeric(df[power_col], errors='coerce')
                mapping_text = f"W: total={power_col}"
            else:
                pcs = phase_cols(
                    'power',
                    ['l1', 'l2', 'l3', 'phase1', 'phase2', 'phase3', 'phase_a', 'phase_b', 'phase_c', 'a_', '_a', 'b_', '_b', 'c_', '_c'],
                )
                if not pcs:
                    # fallback: any p1/p2/p3, power1/power2/power3
                    for c in df.columns:
                        cl = c.lower()
                        if ('power' in cl or cl.startswith('p')) and any(tag in cl for tag in ['1', '2', '3']):
                            if not any(bad in cl for bad in ['price', 'kwh', 'energy']):
                                pcs.append(c)
                if pcs:
                    # Many exports provide per-phase max/min/avg columns. For totals, we must not
                    # sum max+min+avg; instead pick avg when available (else a sensible fallback)
                    # and then sum phases.
                    collapsed = _collapse_phase_stat_cols(pcs, 'power')
                    if collapsed:
                        num = pd.DataFrame(collapsed)
                        y = num.sum(axis=1, min_count=1)
                        try:
                            mapping_text = "W: phases=" + ", ".join(
                                [f"{k}:{v.name if hasattr(v,'name') and v.name else '<calc>'}" for k, v in collapsed.items()]
                            ) + " | sum"
                        except Exception:
                            mapping_text = "W: phases=" + ", ".join(list(collapsed.keys())) + " | sum"
                    else:
                        # pd.to_numeric() only accepts 1-D inputs (Series/array). When we have
                        # multiple phase columns, coerce each column first, then aggregate.
                        num = df[pcs].apply(lambda s: pd.to_numeric(s, errors='coerce'))
                        y = num.sum(axis=1, min_count=1)
                        mapping_text = "W: sum(" + ", ".join(map(str, pcs)) + ")"
                else:
                    # Fallback: compute power from V * I if no power columns are available.
                    vcol = first_col(['avg_voltage', 'voltage', 'u', 'v', 'total_voltage'])
                    icol = first_col(['avg_current', 'total_current', 'current', 'amps', 'a', 'i'])
                    if vcol and icol:
                        y = pd.to_numeric(df[vcol], errors='coerce') * pd.to_numeric(df[icol], errors='coerce')
                        mapping_text = f"W: {vcol}*{icol}"
                    else:
                        y = pd.Series([0.0] * len(df), index=df.index)
                        mapping_text = "W: (no columns)"

        
        elif metric_u in {'VAR', 'Q', 'COSPHI', 'PF', 'POWERFACTOR'}:
            # Reactive power (VAR) and power factor (cos φ) derived from active + apparent power.
            # P = active power (W), S = apparent power (VA). Q = sign * sqrt(max(S^2 - P^2, 0)).
            want_pf = metric_u in {'COSPHI', 'PF', 'POWERFACTOR'}
            ylab = 'cos φ' if want_pf else 'VAR'

            def _num_series(colname: str) -> pd.Series:
                return pd.to_numeric(df[colname], errors='coerce')

            def _active_total_series() -> Tuple[pd.Series, str]:
                # Prefer explicit total active power columns; else sum phase-active.
                col = first_col([
                    'power_total_w',
                    'total_act_power', 'total_active_power', 'total_apower',
                    'total_power_w', 'total_act_power_w',
                    'avg_power', 'avg_active_power', 'avg_act_power', 'avg_apower', 'avg_w',
                    'active_power', 'act_power', 'apower',
                    'power', 'watts', 'w', 'power_w',
                ])
                if col:
                    return _num_series(col), f"P(total)={col}"
                pcs = phase_cols('power', ['l1','l2','l3','phase1','phase2','phase3','phase_a','phase_b','phase_c','a_','_a','b_','_b','c_','_c'])
                if pcs:
                    collapsed = _collapse_phase_stat_cols(pcs, 'power')  # kind=power -> active (non-apparent) preferred
                    if collapsed:
                        num = pd.DataFrame(collapsed)
                        return num.sum(axis=1, min_count=1), "P(phases)=" + ", ".join(list(collapsed.keys()))
                    num = df[pcs].apply(lambda s: pd.to_numeric(s, errors='coerce'))
                    return num.sum(axis=1, min_count=1), "P(sum)=" + ", ".join(map(str, pcs))
                return pd.Series([0.0]*len(df), index=df.index, dtype="float64"), "P(no cols)"

            def _apparent_total_series() -> Tuple[pd.Series, str]:
                # Prefer explicit total apparent power columns; else sum phase-apparent.
                col = first_col([
                    'total_aprt_power', 'total_apparent_power', 'total_va', 'va_total',
                    'aprt_power', 'apparent_power', 'va',
                ])
                if col:
                    return _num_series(col), f"S(total)={col}"

                # Phase apparent columns (e.g. a_max_aprt_power / a_min_aprt_power)
                pcs = [c for c in df.columns if (
                    re.search(r'(^|_)(l1|l2|l3|phase1|phase2|phase3|phase_a|phase_b|phase_c|a|b|c)(_|$)', str(c).lower())
                    and (('aprt_power' in str(c).lower()) or ('apparent' in str(c).lower()) or re.search(r'(^|_)va(_|$)', str(c).lower()))
                )]

                def _collapse_apparent(cols) -> Dict[str, pd.Series]:
                    # Similar to _collapse_phase_stat_cols(kind='power') but keeps apparent power (aprt).
                    def _phase_label(c):
                        cl = str(c).lower()
                        for token, lab in (
                            ('phase_a','L1'),('phase_b','L2'),('phase_c','L3'),
                            ('phase1','L1'),('phase2','L2'),('phase3','L3'),
                            ('l1','L1'),('l2','L2'),('l3','L3'),
                        ):
                            if re.search(rf'(^|_)({token})(_|$)', cl):
                                return lab
                        if cl.startswith('a_') or re.search(r'(^|_)(a)(_|$)', cl): return 'L1'
                        if cl.startswith('b_') or re.search(r'(^|_)(b)(_|$)', cl): return 'L2'
                        if cl.startswith('c_') or re.search(r'(^|_)(c)(_|$)', cl): return 'L3'
                        return None

                    groups={'L1':[], 'L2':[], 'L3':[]}
                    for c in cols:
                        lab=_phase_label(c)
                        if lab in groups:
                            groups[lab].append(c)

                    out={}
                    for lab, cols2 in groups.items():
                        if not cols2: 
                            continue
                        # prefer avg
                        cavg = next((c for c in cols2 if 'avg' in str(c).lower()), None)
                        if cavg is not None:
                            out[lab] = pd.to_numeric(df[cavg], errors='coerce')
                            continue
                        cmax = next((c for c in cols2 if 'max' in str(c).lower()), None)
                        cmin = next((c for c in cols2 if 'min' in str(c).lower()), None)
                        if cmax is not None and cmin is not None:
                            out[lab] = (pd.to_numeric(df[cmax], errors='coerce') + pd.to_numeric(df[cmin], errors='coerce'))/2.0
                            continue
                        cplain = next((c for c in cols2 if not any(t in str(c).lower() for t in ['avg','max','min'])), None)
                        if cplain is not None:
                            out[lab] = pd.to_numeric(df[cplain], errors='coerce')
                            continue
                        out[lab] = pd.to_numeric(df[cols2[0]], errors='coerce')
                    return out

                collapsed = _collapse_apparent(pcs) if pcs else {}
                if collapsed:
                    num = pd.DataFrame(collapsed)
                    return num.sum(axis=1, min_count=1), "S(phases)=" + ", ".join(list(collapsed.keys()))
                if pcs:
                    num = df[pcs].apply(lambda s: pd.to_numeric(s, errors='coerce'))
                    return num.sum(axis=1, min_count=1), "S(sum)=" + ", ".join(map(str, pcs))
                return pd.Series([0.0]*len(df), index=df.index, dtype="float64"), "S(no cols)"

            def _reactive_sign_series() -> Tuple[pd.Series, str]:
                # Use lag/lead reactive energy derivative to determine sign (+inductive / -capacitive).
                lag_col = first_col(['total_lag_react_energy','lag_react_energy','lag_reactive_energy'])
                lead_col = first_col(['total_lead_react_energy','lead_react_energy','lead_reactive_energy'])
                if lag_col and lead_col:
                    lag = pd.to_numeric(df[lag_col], errors='coerce')
                    lead = pd.to_numeric(df[lead_col], errors='coerce')
                    src = f"sign: d({lag_col})-d({lead_col})"
                else:
                    lag_cols = [c for c in df.columns if ('lag_react_energy' in str(c).lower())]
                    lead_cols = [c for c in df.columns if ('lead_react_energy' in str(c).lower())]
                    if lag_cols and lead_cols:
                        lag = df[lag_cols].apply(lambda s: pd.to_numeric(s, errors='coerce')).sum(axis=1, min_count=1)
                        lead = df[lead_cols].apply(lambda s: pd.to_numeric(s, errors='coerce')).sum(axis=1, min_count=1)
                        src = "sign: d(lag_sum)-d(lead_sum)"
                    else:
                        return pd.Series([1.0]*len(df), index=df.index, dtype="float64"), "sign: default(+)"
                d = lag.diff().fillna(0.0) - lead.diff().fillna(0.0)
                sgn = d.apply(lambda x: 1.0 if x > 0 else (-1.0 if x < 0 else float('nan')))
                sgn = sgn.ffill().fillna(1.0)
                return sgn, src

            P, mapP = _active_total_series()
            S, mapS = _apparent_total_series()
            sgn, mapSign = _reactive_sign_series()
            # Q magnitude
            Qmag = (S.astype('float64')**2 - P.astype('float64')**2).clip(lower=0.0) ** 0.5
            Q = sgn * Qmag

            if want_pf:
                denom = (P.astype('float64')**2 + Q.astype('float64')**2) ** 0.5
                pf = (P.abs() / denom).replace([np.inf, -np.inf], np.nan).fillna(0.0)
                pf = pf.clip(lower=0.0, upper=1.0)
                y = pf
                mapping_text = f"cosφ: {mapP}; {mapS}; {mapSign}"
            else:
                y = Q
                mapping_text = f"VAR: {mapP}; {mapS}; {mapSign}"


        elif metric_u == 'V':
            ylab = 'V'
            vcol = first_col(['avg_voltage', 'voltage', 'u', 'v', 'total_voltage'])
            if vcol:
                y = pd.to_numeric(df[vcol], errors='coerce')
                mapping_text = f"V: total={vcol}"
            else:
                vcols = phase_cols(
                    'voltage',
                    ['l1', 'l2', 'l3', 'phase1', 'phase2', 'phase3', 'phase_a', 'phase_b', 'phase_c', 'a_', '_a', 'b_', '_b', 'c_', '_c'],
                )
                if vcols:
                    collapsed = _collapse_phase_stat_cols(vcols, 'voltage')
                    if collapsed:
                        num = pd.DataFrame(collapsed)
                        try:
                            mapping_text = "V: phases=" + ", ".join([f"{k}:{v.name if hasattr(v,'name') and v.name else '<calc>'}" for k, v in collapsed.items()])
                        except Exception:
                            mapping_text = "V: phases=" + ", ".join([f"{k}" for k in collapsed.keys()])
                    else:
                        num = df[vcols].apply(lambda s: pd.to_numeric(s, errors='coerce'))
                        mapping_text = "V: mean(" + ", ".join(map(str, vcols)) + ")"
                    # Some exports fill unused phases with 0. That would break an average
                    # (e.g. 230,0,0 -> 76.6V). Treat near-zero as missing when other phases
                    # are present.
                    if num.shape[1] > 1:
                        num = num.mask(num.abs() < 1e-6)
                    y = num.mean(axis=1, skipna=True)
                else:
                    y = pd.Series([0.0] * len(df), index=df.index)
                    mapping_text = "V: (no columns)"

        elif metric_u == 'A':
            ylab = 'A'

            # Prefer per-phase currents if available. Some exports/pipelines include
            # a precomputed 'total_current'/'current' that can be wrong for single-phase
            # devices (e.g. mirrored into L1/L2/L3 -> 3x). If we have phase currents,
            # we compute total from them.
            icols = phase_cols(
                'current',
                ['l1', 'l2', 'l3', 'phase1', 'phase2', 'phase3', 'phase_a', 'phase_b', 'phase_c', 'a_', '_a', 'b_', '_b', 'c_', '_c'],
            )
            if not icols:
                for c in df.columns:
                    cl = c.lower()
                    if ('current' in cl or cl.startswith('i')) and any(tag in cl for tag in ['1', '2', '3']):
                        if not any(bad in cl for bad in ['price', 'kwh', 'energy']):
                            icols.append(c)

            if icols:
                collapsed = _collapse_phase_stat_cols(icols, 'current')
                if collapsed:
                    num = pd.DataFrame(collapsed)
                    try:
                        mapping_text = "A: phases=" + ", ".join([f"{k}:{v.name if hasattr(v,'name') and v.name else '<calc>'}" for k, v in collapsed.items()])
                    except Exception:
                        mapping_text = "A: phases=" + ", ".join([f"{k}" for k in collapsed.keys()])
                else:
                    num = df[icols].apply(lambda s: pd.to_numeric(s, errors='coerce'))
                    mapping_text = "A: sum(" + ", ".join(map(str, icols)) + ")"

                # By default, total current is the sum of phase currents.
                # However, some single-phase exports (or virtual L1/L2/L3 mappings)
                # may duplicate the same current into multiple phase columns.
                # In that case, summing would effectively multiply by the number
                # of phases (e.g. 10A -> 30A).
                dup_phases = False
                true_multiphase = False

                try:
                    # 1) Evidence from power columns (strongest signal)
                    p_total_col = first_col(['total_power', 'power', 'watts', 'w', 'active_power', 'apower', 'power_w'])
                    pcs = phase_cols(
                        'power',
                        ['l1', 'l2', 'l3', 'phase1', 'phase2', 'phase3', 'phase_a', 'phase_b', 'phase_c', 'a_', '_a', 'b_', '_b', 'c_', '_c'],
                    )
                    if p_total_col and pcs and len(pcs) >= 2:
                        p_total = pd.to_numeric(df[p_total_col], errors='coerce')
                        p_num = df[pcs].apply(lambda s: pd.to_numeric(s, errors='coerce'))
                        p_sum = p_num.sum(axis=1, min_count=1)
                        p_mean = p_num.mean(axis=1, skipna=True)

                        denom = p_total.abs().where(p_total.abs() > 1e-6)
                        rel_sum = (p_total - p_sum).abs() / denom
                        rel_mean = (p_total - p_mean).abs() / denom

                        m_sum = rel_sum.dropna().median() if rel_sum.notna().any() else float('nan')
                        m_mean = rel_mean.dropna().median() if rel_mean.notna().any() else float('nan')

                        # If sum fits well, treat as true multi-phase even if currents look similar.
                        if pd.notna(m_sum) and m_sum < 0.08:
                            true_multiphase = True

                        # If mean fits significantly better than sum, likely duplicated mapping.
                        if (not true_multiphase) and pd.notna(m_mean) and pd.notna(m_sum):
                            if (m_mean < 0.05) and (m_mean < m_sum * 0.5):
                                dup_phases = True

                    # 2) Fallback: phase currents nearly identical (but only if not true multiphase)
                    if (not dup_phases) and (not true_multiphase) and num.shape[1] >= 2:
                        base = num.iloc[:, 0]
                        diffs = []
                        for j in range(1, num.shape[1]):
                            other = num.iloc[:, j]
                            denom = base.abs().combine(other.abs(), max)
                            denom = denom.where(denom > 1e-6)
                            diffs.append(((base - other).abs() / denom))
                        if diffs:
                            rel = pd.concat(diffs, axis=1).median(axis=1, skipna=True)
                            rel = rel.dropna()
                            if len(rel) >= 20 and pd.notna(rel.median()) and float(rel.median()) < 0.02:
                                dup_phases = True

                        if not dup_phases:
                            row_max = num.max(axis=1, skipna=True)
                            row_min = num.min(axis=1, skipna=True)
                            denom = row_max.abs().where(row_max.abs() > 1e-6)
                            rr = ((row_max - row_min).abs() / denom).dropna()
                            if len(rr) >= 20 and pd.notna(rr.median()) and float(rr.median()) < 0.02:
                                dup_phases = True

                except Exception:
                    dup_phases = False
                    true_multiphase = False

                # IMPORTANT: If only one phase is actually loaded (others are ~0),
                # summing is still correct (10 + 0 + 0 = 10). The 3x bug comes from
                # duplicated values, not from summing.
                if dup_phases:
                    y = num.max(axis=1, skipna=True)
                    mapping_text += " | dup->max"
                else:
                    y = num.sum(axis=1, min_count=1)
                    mapping_text += " | sum"


            else:
                # Fallback to a single total-current column if phases are not available.
                icol = first_col(['avg_current', 'total_current', 'current', 'amps', 'a', 'i'])
                if icol:
                    y = pd.to_numeric(df[icol], errors='coerce')
                    mapping_text = f"A: total={icol}"
                else:
                    y = pd.Series([0.0] * len(df), index=df.index)
                    mapping_text = "A: (no columns)"
        elif metric_u in {"VAR", "Q"}:
            ylab = "VAR"
            # Prefer stored reactive power columns from live samples (q_total_var / qa/qb/qc).
            q_total_col = first_col(["q_total_var", "reactive_var_total", "total_reactive_var", "var_total", "q_total"])
            if q_total_col:
                y = pd.to_numeric(df[q_total_col], errors="coerce")
                mapping_text = f"VAR: total={q_total_col}"
            else:
                # Sum phases if total not available
                qa_col = first_col(["qa", "q_a", "reactive_var_a", "a_reactive_var", "a_var", "a_q"])
                qb_col = first_col(["qb", "q_b", "reactive_var_b", "b_reactive_var", "b_var", "b_q"])
                qc_col = first_col(["qc", "q_c", "reactive_var_c", "c_reactive_var", "c_var", "c_q"])
                cols = [c for c in (qa_col, qb_col, qc_col) if c]
                if cols:
                    num = pd.to_numeric(df[cols[0]], errors="coerce")
                    for c in cols[1:]:
                        num = num + pd.to_numeric(df[c], errors="coerce")
                    y = num
                    mapping_text = f"VAR: sum({','.join(cols)})"
                else:
                    y = pd.Series([0.0] * len(df), index=df.index)
                    mapping_text = "VAR: (no columns)"

        elif metric_u in {"COSPHI", "PF", "POWERFACTOR"}:
            ylab = "cos φ"
            pf_total_col = first_col(["cosphi_total", "pf_total", "power_factor_total", "cos_phi_total", "cosphi", "pf"])
            if pf_total_col:
                y = pd.to_numeric(df[pf_total_col], errors="coerce")
                mapping_text = f"cosφ: total={pf_total_col}"
            else:
                pfa_col = first_col(["pfa", "pf_a", "cosphi_a", "a_cosphi", "a_pf"])
                pfb_col = first_col(["pfb", "pf_b", "cosphi_b", "b_cosphi", "b_pf"])
                pfc_col = first_col(["pfc", "pf_c", "cosphi_c", "c_cosphi", "c_pf"])
                cols = [c for c in (pfa_col, pfb_col, pfc_col) if c]
                if cols:
                    # Simple mean of available phases
                    num = pd.to_numeric(df[cols[0]], errors="coerce")
                    if len(cols) > 1:
                        for c in cols[1:]:
                            num = num + pd.to_numeric(df[c], errors="coerce")
                        num = num / float(len(cols))
                    y = num
                    mapping_text = f"cosφ: mean({','.join(cols)})"
                else:
                    y = pd.Series([0.0] * len(df), index=df.index)
                    mapping_text = "cosφ: (no columns)"

        else:
            y = pd.Series([0.0] * len(df), index=df.index)

        out = pd.Series(y.to_numpy(), index=ts, name=metric_u)
        # Ensure the index is a proper DatetimeIndex (web plots rely on this)
        try:
            out.index = pd.to_datetime(out.index, errors='coerce')
        except Exception:
            pass
        try:
            out = out[~pd.isna(out.index)]
        except Exception:
            pass
        out = out.sort_index()
        if out.index.has_duplicates:
            out = out.groupby(level=0).mean()
        # Save mapping for optional debug display
        try:
            setattr(self, "_last_wva_mapping_text", str(mapping_text))
        except Exception:
            pass
        return out, ylab

    def _wva_phase_series(self, df: pd.DataFrame, metric: str) -> Dict[str, pd.Series]:
            """Return per-phase series for W/V/A if available.

            Keys are "L1", "L2", "L3" (best-effort based on column names).

            IMPORTANT:
            - Shelly 3EM exports often include statistic columns per phase, e.g.
              a_max_current / a_min_current / a_avg_current (and similar for voltage/power).
              For phase plots we must pick ONE representative series per phase (prefer avg),
              NOT plot/sum max+min+avg mixes.
            - For W (power), users expect *per-phase power* when "phases" is selected.
              The total line may still be shown in "total" mode.
            """
            out: Dict[str, pd.Series] = {}
            if df is None or df.empty:
                return out

            metric_u = (metric or "").upper().strip()
            if metric_u not in {"W", "V", "A", "VAR", "Q", "COSPHI", "PF", "POWERFACTOR"}:
                return out

            # Derived metrics: Reactive power (VAR) and power factor (cos φ)
            if metric_u in {"VAR", "Q", "COSPHI", "PF", "POWERFACTOR"}:
                # Reactive power (VAR) and power factor (cos φ) derived from active + apparent power.
                # IMPORTANT: For phase view we must not accidentally pick other "*power*" columns. We use
                # strict, phase-aware column patterns (a/b/c, l1/l2/l3, phase_a/b/c).
                want_pf = metric_u in {"COSPHI", "PF", "POWERFACTOR"}

                # Prefer directly stored columns from our LiveSample ingestion (qa/qb/qc and pfa/pfb/pfc).
                try:
                    cols_lut2 = {str(c).lower(): c for c in df.columns}
                    direct_map = {
                        "L1": (["pfa", "pf_a", "cosphi_a", "a_cosphi", "a_pf"] if want_pf else ["qa", "q_a", "reactive_var_a", "a_reactive_var", "a_var", "a_q"]),
                        "L2": (["pfb", "pf_b", "cosphi_b", "b_cosphi", "b_pf"] if want_pf else ["qb", "q_b", "reactive_var_b", "b_reactive_var", "b_var", "b_q"]),
                        "L3": (["pfc", "pf_c", "cosphi_c", "c_cosphi", "c_pf"] if want_pf else ["qc", "q_c", "reactive_var_c", "c_reactive_var", "c_var", "c_q"]),
                    }
                    for lab, cands in direct_map.items():
                        col = None
                        for cc in cands:
                            if str(cc).lower() in cols_lut2:
                                col = cols_lut2[str(cc).lower()]
                                break
                        if col is not None:
                            out[lab] = pd.to_numeric(df[col], errors="coerce")
                    if out:
                        return out
                except Exception:
                    pass

                cols_all = list(df.columns)
                cols_lut = {str(c).lower(): c for c in cols_all}

                # Phase token sets ordered by preference (a/b/c is the common Shelly 3EM export format).
                PHASES = [
                    ("L1", ["a", "l1", "phase_a", "phase1"]),
                    ("L2", ["b", "l2", "phase_b", "phase2"]),
                    ("L3", ["c", "l3", "phase_c", "phase3"]),
                ]

                def _first_existing(cands):
                    for cc in cands:
                        ccl = str(cc).lower()
                        if ccl in cols_lut:
                            return cols_lut[ccl]
                    return None

                def _num(col):
                    return pd.to_numeric(df[col], errors="coerce")

                def _series_for_phase(token_list, kind: str) -> Tuple[Optional[pd.Series], str]:
                    """Return one representative series for a phase.
                    kind: "act" (W) or "aprt" (VA). Preference: avg -> (max+min)/2 -> plain.
                    """
                    # Build candidate column names for each token variant.
                    # We match common variants seen in the project history.
                    avg_cands = []
                    max_cands = []
                    min_cands = []
                    plain_cands = []
                    for t in token_list:
                        if kind == "act":
                            avg_cands += [f"{t}_avg_act_power", f"{t}_avg_active_power", f"{t}_avg_power", f"{t}_act_power_avg", f"avg_act_power_{t}"]
                            max_cands += [f"{t}_max_act_power", f"{t}_max_active_power", f"{t}_max_power"]
                            min_cands += [f"{t}_min_act_power", f"{t}_min_active_power", f"{t}_min_power"]
                            plain_cands += [f"{t}_act_power", f"{t}_active_power", f"{t}_power", f"act_power_{t}", f"active_power_{t}", f"power_{t}"]
                        else:
                            avg_cands += [f"{t}_avg_aprt_power", f"{t}_avg_apparent_power", f"{t}_aprt_power_avg", f"avg_aprt_power_{t}", f"avg_apparent_power_{t}"]
                            max_cands += [f"{t}_max_aprt_power", f"{t}_max_apparent_power"]
                            min_cands += [f"{t}_min_aprt_power", f"{t}_min_apparent_power"]
                            plain_cands += [f"{t}_aprt_power", f"{t}_apparent_power", f"aprt_power_{t}", f"apparent_power_{t}"]

                        # Also accept plain prefix forms like "a_act_power" (rare but exists).
                        if kind == "act":
                            plain_cands += [f"{t}_act_power", f"{t}_active_power"]
                        else:
                            plain_cands += [f"{t}_aprt_power", f"{t}_apparent_power"]

                    c_avg = _first_existing(avg_cands)
                    if c_avg:
                        return _num(c_avg), f"avg:{c_avg}"
                    c_max = _first_existing(max_cands)
                    c_min = _first_existing(min_cands)
                    if c_max and c_min:
                        return (_num(c_max) + _num(c_min)) / 2.0, f"(max+min)/2:{c_max},{c_min}"
                    c_plain = _first_existing(plain_cands)
                    if c_plain:
                        return _num(c_plain), f"plain:{c_plain}"
                    return None, "missing"

                def _reactive_sign_for_phase(token_list) -> Tuple[pd.Series, str]:
                    # Determine sign from reactive energy derivative: d(lag)-d(lead).
                    lag_cands = []
                    lead_cands = []
                    for t in token_list:
                        lag_cands += [f"{t}_lag_react_energy", f"{t}_lag_reactive_energy", f"lag_react_energy_{t}", f"lag_reactive_energy_{t}"]
                        lead_cands += [f"{t}_lead_react_energy", f"{t}_lead_reactive_energy", f"lead_react_energy_{t}", f"lead_reactive_energy_{t}"]
                    c_lag = _first_existing(lag_cands)
                    c_lead = _first_existing(lead_cands)
                    if c_lag and c_lead:
                        d = (_num(c_lag).diff() - _num(c_lead).diff())
                        s = d.apply(lambda x: 1.0 if pd.isna(x) or x == 0 else (1.0 if x > 0 else -1.0))
                        s = s.replace(0, np.nan).ffill().fillna(1.0)
                        return s, f"sign:diff({c_lag}-{c_lead})"
                    return pd.Series(1.0, index=df.index, dtype=float), "sign:default(+)"

                # Build outputs per phase
                for lab, tokens in PHASES:
                    P, mapP = _series_for_phase(tokens, "act")
                    S, mapS = _series_for_phase(tokens, "aprt")
                    if P is None or S is None:
                        continue
                    # Avoid division by ~0 for PF and invalid sqrt for VAR.
                    S_abs = S.abs()
                    # PF
                    pf = (P.abs() / S_abs).replace([np.inf, -np.inf], np.nan)
                    pf = pf.clip(lower=0.0, upper=1.0)
                    if want_pf:
                        out[lab] = pf
                    else:
                        # VAR magnitude via sqrt(S^2 - P^2), sign via reactive energies if available.
                        mag = np.sqrt(np.maximum((S.astype(float) ** 2) - (P.astype(float) ** 2), 0.0))
                        sgn, mapSign = _reactive_sign_for_phase(tokens)
                        q = pd.Series(mag, index=P.index, dtype=float) * pd.to_numeric(sgn, errors="coerce").fillna(1.0)
                        out[lab] = q

                return out

            cand = []  # candidate columns
            kind = None
            avg_tag = max_tag = min_tag = None

            # --- W/V/A phase column selection (fix for missing 'cand'/'kind' after refactor) ---
            cols_all = list(df.columns)

            if metric_u == "W":
                kind = "power"
                # include active/apparent power candidates (apparent will be filtered later)
                cand = [
                    c for c in cols_all
                    if (
                        ("power" in str(c).lower())
                        or ("apower" in str(c).lower())
                        or re.search(r"(^|_)w(_|$)", str(c).lower())
                        or ("watt" in str(c).lower())
                    )
                ]
            elif metric_u == "V":
                kind = "voltage"
                avg_tag, max_tag, min_tag = "avg_voltage", "max_voltage", "min_voltage"
                cand = [
                    c for c in cols_all
                    if (
                        ("voltage" in str(c).lower())
                        or re.search(r"(^|_)v(_|$)", str(c).lower())
                        or ("volt" in str(c).lower())
                    )
                ]
            else:  # metric_u == "A"
                kind = "current"
                avg_tag, max_tag, min_tag = "avg_current", "max_current", "min_current"
                cand = [
                    c for c in cols_all
                    if (
                        ("current" in str(c).lower())
                        or re.search(r"(^|_)a(_|$)", str(c).lower())
                        or ("amp" in str(c).lower())
                    )
                ]

            # Safety: never crash if no candidates found
            if not cand:
                return out


            phase_re = re.compile(r"(^|_)(l1|l2|l3|phase1|phase2|phase3|phase_a|phase_b|phase_c|a|b|c)(_|$)")

            def _phase_label(col: str):
                cl = str(col).lower()
                # strict boundary matching only (DO NOT use '_a' substring, it matches '_avg_')
                for token, lab in (
                    ("phase_a", "L1"), ("phase_b", "L2"), ("phase_c", "L3"),
                    ("phase1", "L1"), ("phase2", "L2"), ("phase3", "L3"),
                    ("l1", "L1"), ("l2", "L2"), ("l3", "L3"),
                    ("a", "L1"), ("b", "L2"), ("c", "L3"),
                ):
                    if re.search(rf"(^|_)({token})(_|$)", cl):
                        return lab
                return None

            # Group columns per phase
            groups = {"L1": [], "L2": [], "L3": []}
            for c in cand:
                cl = str(c).lower()
                if not phase_re.search(cl):
                    continue
                lab = _phase_label(cl)
                if lab in groups:
                    groups[lab].append(c)

            def _num(col):
                return pd.to_numeric(df.get(col), errors="coerce")

            def _is_avg(cl: str) -> bool:
                if kind == "power":
                    return ("avg" in cl) and ("power" in cl or "act_power" in cl or "active_power" in cl or "apower" in cl or re.search(r"(^|_)w(_|$)", cl))
                return avg_tag in cl

            def _is_max(cl: str) -> bool:
                if kind == "power":
                    return ("max" in cl) and ("power" in cl)
                return max_tag in cl

            def _is_min(cl: str) -> bool:
                if kind == "power":
                    return ("min" in cl) and ("power" in cl)
                return min_tag in cl

            for lab, cols in groups.items():
                if not cols:
                    continue

                # For power, prefer active power columns (act_power/active_power/apower) over apparent (aprt/apparent).
                if kind == "power":
                    try:
                        cols_act = [
                            c for c in cols
                            if (
                                ("act_power" in str(c).lower())
                                or ("active_power" in str(c).lower())
                                or (("apower" in str(c).lower()) and ("aprt" not in str(c).lower()))
                            )
                            and ("aprt" not in str(c).lower())
                            and ("apparent" not in str(c).lower())
                        ]
                        cols_non_apparent = [c for c in cols if ("aprt" not in str(c).lower()) and ("apparent" not in str(c).lower())]
                        if cols_act:
                            cols = cols_act
                        elif cols_non_apparent:
                            cols = cols_non_apparent
                    except Exception:
                        pass

                # 1) Prefer avg_*
                chosen = None
                for c in cols:
                    if _is_avg(str(c).lower()):
                        chosen = c
                        break
                if chosen is not None:
                    out[lab] = _num(chosen)
                    continue

                # 2) Prefer a plain measurement without max/min/avg
                plain = []
                for c in cols:
                    cl = str(c).lower()
                    if _is_avg(cl) or _is_max(cl) or _is_min(cl):
                        continue
                    plain.append(c)
                if plain:
                    out[lab] = _num(plain[0])
                    continue

                # 3) Fallback: mean(max/min) if both exist
                cmax = None
                cmin = None
                for c in cols:
                    cl = str(c).lower()
                    if _is_max(cl) and cmax is None:
                        cmax = c
                    elif _is_min(cl) and cmin is None:
                        cmin = c
                if cmax is not None and cmin is not None:
                    out[lab] = (_num(cmax) + _num(cmin)) / 2.0
                elif cmax is not None:
                    out[lab] = _num(cmax)
                elif cmin is not None:
                    out[lab] = _num(cmin)

            # Ensure per-phase series are time-indexed
            try:
                if "timestamp" in df.columns:
                    ts_idx = pd.to_datetime(df["timestamp"], errors="coerce")
                elif "ts" in df.columns:
                    ts_idx = pd.to_datetime(df["ts"], errors="coerce")
                else:
                    ts_idx = None
                if ts_idx is not None:
                    ts_idx = pd.DatetimeIndex(ts_idx)
                    msk = ~pd.isna(ts_idx)
                    for kk in list(out.keys()):
                        s = out.get(kk)
                        if s is None:
                            continue
                        try:
                            s2 = pd.Series(pd.to_numeric(s, errors="coerce").to_numpy(), index=ts_idx)
                            s2 = s2[msk]
                            s2 = s2.dropna().sort_index()
                            if s2.index.has_duplicates:
                                s2 = s2.groupby(level=0).mean()
                            out[kk] = s2
                        except Exception:
                            pass
            except Exception:
                pass

            # Keep stable order
            return {k: out[k] for k in ("L1", "L2", "L3") if k in out}

    def _pretty_kwh_mode(self, mode: str) -> str:
        """Return a localized, human-friendly label for kWh stats modes.

        Supports modes like: all/days/weeks/months/hours and custom 'days:7', 'hours:24', ...
        """
        try:
            m = str(mode or "days").lower().strip()
            if ':' in m:
                unit, n = m.split(':', 1)
                unit = unit.strip()
                try:
                    n_i = int(float(n.strip()))
                except Exception:
                    n_i = 0
                n_i = max(1, n_i)
                unit_lbl = self.t(f"plots.mode.{unit}") if unit in {"hours", "days", "weeks", "months"} else unit
                try:
                    return self.t("plots.kwh.last_n").format(n=n_i, unit=unit_lbl)
                except Exception:
                    return f"{n_i} {unit_lbl}"
            # plain modes
            if m in {"all", "hours", "days", "weeks", "months"}:
                return self.t(f"plots.mode.{m}")
            return m
        except Exception:
            return str(mode or "days")



    def _stats_series(self, df: Optional[pd.DataFrame], mode: str) -> Tuple[List[str], List[float]]:
        """Return (labels, kWh_values) for stats bar charts.

        This is used by the Plots tab (kWh mode) and by the web dashboard.
        """
        mode = str(mode or "days").lower().strip()
        unit = mode
        limit_n = None
        if ':' in mode:
            try:
                unit, n_raw = mode.split(':', 1)
                unit = unit.strip()
                limit_n = int(float(n_raw.strip()))
                if limit_n <= 0:
                    limit_n = None
            except Exception:
                unit = mode
                limit_n = None
        if df is None or df.empty:
            return ([], [])
        try:
            df2 = calculate_energy(df, method="auto")
        except Exception:
            # If we can't compute energy, show nothing instead of crashing.
            return ([], [])

        s = pd.to_numeric(df2.get("energy_kwh"), errors="coerce").fillna(0.0)
        ts = pd.to_datetime(df2.get("timestamp"), errors="coerce")
        tmp = pd.DataFrame({"timestamp": ts, "energy_kwh": s}).dropna(subset=["timestamp"]).sort_values("timestamp")
        if tmp.empty:
            return ([], [])
        tmp = tmp.set_index("timestamp")

        if unit == "all":
            total = float(tmp["energy_kwh"].sum())
            return (["Total"], [total])

        if unit == "hours":
            # Pandas: 'H' is deprecated, use 'h'
            hr = tmp["energy_kwh"].resample("h").sum()
            if limit_n is not None:
                hr = hr.tail(int(limit_n))
            labels = [pd.Timestamp(x).strftime("%Y-%m-%d %H:00") for x in hr.index]
            return (labels, [float(v) for v in hr.values])

        if unit == "weeks":
            # ISO week label like 2026-W03
            wk = tmp["energy_kwh"].resample("W-MON").sum()
            if limit_n is not None:
                wk = wk.tail(int(limit_n))
            labels = [f"{int(x.isocalendar().year)}-W{int(x.isocalendar().week):02d}" for x in wk.index]
            return (labels, [float(v) for v in wk.values])

        if unit == "months":
            mo = tmp["energy_kwh"].resample("MS").sum()
            if limit_n is not None:
                mo = mo.tail(int(limit_n))
            labels = [pd.Timestamp(x).strftime("%Y-%m") for x in mo.index]
            return (labels, [float(v) for v in mo.values])

        # default: days
        day = tmp["energy_kwh"].resample("D").sum()
        if limit_n is not None:
            day = day.tail(int(limit_n))
        labels = [pd.Timestamp(x).strftime("%Y-%m-%d") for x in day.index]
        return (labels, [float(v) for v in day.values])

    def _redraw_wva_plots(self) -> None:
        """Time-series plot for W/V/A using fixed time windows (Option A)."""
        mode = "wva"
        # Apply optional plot range
        pstart = _parse_date_flexible(self._plots_start.get())
        pend = _parse_date_flexible(self._plots_end.get())
        if pstart is not None and pend is not None and pend < pstart:
            pstart, pend = pend, pstart

        metric = str(self._wva_metric.get() or "W").upper().strip()
        window_delta = self._wva_window_delta()

        for d in self._get_visible_devices():
            cd = self.computed.get(d.key)
            if cd is None:
                continue
            canvas = self._stats_canvases.get(d.key)
            fig = self._stats_figs.get(d.key)
            if canvas is None or fig is None:
                continue
            # Determine time window if no explicit range
            df = cd.df
            df = df.sort_values("timestamp") if df is not None and not df.empty else df
            if pstart is None and pend is None and df is not None and not df.empty:
                pend_eff = pd.Timestamp(df["timestamp"].max())
                pstart_eff = pend_eff - window_delta
            else:
                pstart_eff = pstart
                pend_eff = pend
            df_use = filter_by_time(df, start=pstart_eff, end=pend_eff)

            # Resize + draw
            w = canvas.get_tk_widget()
            self._resize_figure_to_widget(fig, w, dpi=self._dpi_for_widget(w), min_h_px=320)
            fig.clear()

            # For V/A: show total on top axis and phases on bottom axis.
            if metric in {"V", "A"}:
                ax1 = fig.add_subplot(211)
                ax2 = fig.add_subplot(212, sharex=ax1)
                self._stats_axes[d.key] = ax1
            else:
                ax1 = fig.add_subplot(111)
                ax2 = None
                self._stats_axes[d.key] = ax1

            # Mark axes as fixed-layout: we manage margins via subplots_adjust below.
            try:
                setattr(ax1, "_shelly_fixed_layout", True)
            except Exception:
                pass
            if ax2 is not None:
                try:
                    setattr(ax2, "_shelly_fixed_layout", True)
                except Exception:
                    pass

            if df_use is None or df_use.empty:
                ax1.set_title(self.t("plots.no_data"))
                ax1.set_ylabel(metric)
                canvas.draw_idle()
                continue

            try:
                ys, ylabel = self._wva_series(df_use, metric)
            except Exception:
                ys, ylabel = (pd.Series(dtype=float), metric)

            if ys is None or ys.empty:
                # Often means the device does not provide voltage/current in the stored CSVs.
                ax1.set_title(self.t("plots.no_data"))
                ax1.set_ylabel(metric)
                canvas.draw_idle()
                continue

            xs = ys.index
            yv = ys.values

            # Plot total
            ax1.plot(xs, yv)
            ax1.set_ylabel(ylabel)
            ax1.grid(True, axis="y", alpha=0.3)

            # Phase plot (extra)
            if ax2 is not None:
                any_line = False
                ts = pd.to_datetime(df_use.get("timestamp"), errors="coerce")
                phase_series = self._wva_phase_series(df_use, metric)
                try:
                    if int(getattr(dcfg, "phases", 3) or 3) <= 1:
                        phase_series = {"L1": phase_series.get("L1")} if "L1" in phase_series else {}
                except Exception:
                    pass
                for lab, ser in phase_series.items():
                    try:
                        s2 = pd.to_numeric(ser, errors="coerce")
                        tmp = pd.Series(s2.to_numpy(), index=ts).dropna().sort_index()
                    except Exception:
                        continue
                    if tmp is None or tmp.empty:
                        continue
                    ax2.plot(tmp.index, tmp.values, label=lab)
                    any_line = True
                ax2.set_ylabel(ylabel)
                ax2.grid(True, axis="y", alpha=0.3)
                if any_line:
                    ax2.legend(loc="upper right", fontsize=max(8, self._font_base_for_widget(w) - 1))
                ax2.set_xlabel(self.t("live.time"))
                # Hide top x labels to reduce clutter
                try:
                    for t in ax1.get_xticklabels():
                        t.set_visible(False)
                except Exception:
                    pass
            else:
                ax1.set_xlabel(self.t("live.time"))

            # Configure x-axis ticks
            self._apply_smart_date_axis(ax1, w)
            if ax2 is not None:
                self._apply_smart_date_axis(ax2, w)

            # Title
            a = pstart_eff.date().isoformat() if pstart_eff is not None else "…"
            b = pend_eff.date().isoformat() if pend_eff is not None else "…"
            ax1.set_title(f"{d.name} – {metric} | {a}–{b}")

            # Layout: no zoom/toolbar; make deterministic margins so nothing is clipped.
            try:
                if ax2 is None:
                    fig.subplots_adjust(left=0.10, right=0.97, top=0.92, bottom=0.26)
                else:
                    fig.subplots_adjust(left=0.10, right=0.97, top=0.92, bottom=0.18, hspace=0.30)
            except Exception:
                pass
            self._apply_axis_layout(fig, ax1, w, legend=False)
            if ax2 is not None:
                self._apply_axis_layout(fig, ax2, w, legend=True)
            canvas.draw_idle()
    def _redraw_stats_plots(self) -> None:
        if not self._ensure_data_loaded():
            return
        mode = self._plots_mode.get()
        # Apply optional plot range
        pstart = _parse_date_flexible(self._plots_start.get())
        pend = _parse_date_flexible(self._plots_end.get())
        if pstart is not None and pend is not None and pend < pstart:
            pstart, pend = pend, pstart
        unit_gross = float(self.cfg.pricing.unit_price_gross())
        for d in self._get_visible_devices():
            cd = self.computed.get(d.key)
            if cd is None:
                continue
            df_use = filter_by_time(cd.df, start=pstart, end=pend)
            labels, values = self._stats_series(df_use, mode)
            ax = self._stats_axes.get(d.key)
            canvas = self._stats_canvases.get(d.key)
            fig = self._stats_figs.get(d.key)
            if ax is None or canvas is None or fig is None:
                continue
            # Resize figure to current tab size (respect HiDPI)
            w = canvas.get_tk_widget()
            self._resize_figure_to_widget(fig, w, dpi=self._dpi_for_widget(w), min_h_px=320)
            ax.clear()
            ax.set_ylabel("kWh")
            bars = ax.bar(range(len(values)), values)
            base = self._font_base_for_widget(canvas.get_tk_widget())
            self._apply_xticks(ax, labels, base_font=base)
            # Use deterministic margins for the Plots tab so nothing is clipped on HiDPI
            # and when moving between monitors.
            try:
                setattr(ax, "_shelly_fixed_layout", True)
            except Exception:
                pass
            try:
                hpx = int(canvas.get_tk_widget().winfo_height() or 0)
            except Exception:
                hpx = 0
            try:
                bottom = 0.34 if hpx and hpx < 300 else 0.30
                # rotation is 45deg (set in _apply_xticks)
                bottom = min(0.52, max(0.26, bottom + 0.05))
                fig.subplots_adjust(left=0.10, right=0.97, top=0.92, bottom=bottom)
            except Exception:
                pass
            total = float(sum(values))
            cost = total * unit_gross
            if pstart is None and pend is None:
                range_lbl = ""
            else:
                a = pstart.date().isoformat() if pstart is not None else "…"
                b = pend.date().isoformat() if pend is not None else "…"
                range_lbl = f" | {a}–{b}"
            ax.set_title(f"{d.name} – {self._pretty_kwh_mode(mode)}{range_lbl} | {_fmt_kwh(total)} ({_fmt_eur(cost)})")
            ax.grid(True, axis="y", alpha=0.3)
            self._annotate_bars(ax, bars)
            self._apply_axis_layout(fig, ax, canvas.get_tk_widget(), legend=False)
            canvas.draw_idle()
    def _make_stats_figure(self, cd: ComputedDevice, mode: str, dpi: int = 140) -> Figure:
        labels, values = self._stats_series(cd.df, mode)
        fig = Figure(figsize=(11, 3.6), dpi=dpi)
        ax = fig.add_subplot(111)
        ax.set_ylabel("kWh")
        bars = ax.bar(range(len(values)), values)
        self._apply_xticks(ax, labels)
        unit_gross = float(self.cfg.pricing.unit_price_gross())
        total = float(sum(values))
        ax.set_title(f"{cd.device_name} – {self._pretty_kwh_mode(mode)} | {_fmt_kwh(total)} ({_fmt_eur(total * unit_gross)})")
        ax.grid(True, axis="y", alpha=0.3)
        self._annotate_bars(ax, bars)
        fig.tight_layout()
        return fig
    # ---------------- Live tab ----------------
    def _build_live_tab(self) -> None:
        frm = self.tab_live
        # First line: start/stop + freeze/snapshot + status + QR (keep QR visible)
        top = ttk.Frame(frm)
        top.pack(fill="x", padx=12, pady=(10, 2))
        # Second line: controls (poll/window/web/apply/log) so the top line does not get too wide
        top2 = ttk.Frame(frm)
        top2.pack(fill="x", padx=12, pady=(0, 6))
        self.btn_live_start = ttk.Button(top, text=self.t('live.start'), command=self._start_live)
        self.btn_live_stop = ttk.Button(top, text=self.t('live.stop'), command=self._stop_live, state="disabled")
        self.btn_live_start.pack(side="left")
        self.btn_live_stop.pack(side="left", padx=8)

        # Freeze/snapshot controls (Etappe 2)
        try:
            self.chk_live_freeze = ttk.Checkbutton(
                top,
                text=self.t('live.freeze') if hasattr(self, 't') else 'Freeze',
                variable=self._live_frozen,
                command=self._on_live_freeze_toggle,
            )
            self.chk_live_freeze.pack(side="left", padx=(8, 0))
        except Exception:
            pass

        try:
            self.btn_live_png = ttk.Button(top, text=self.t('live.export_png'), command=self._export_live_png)
            self.btn_live_png.pack(side="left", padx=8)
        except Exception:
            pass

        # Quick live controls (so you don't have to dive into Settings)
        self.live_poll_ctl = tk.DoubleVar(value=float(self.cfg.ui.live_poll_seconds))
        self.live_window_ctl = tk.IntVar(value=int(self.cfg.ui.live_window_minutes))
        self.live_web_ctl = tk.BooleanVar(value=bool(self.cfg.ui.live_web_enabled))
        self.live_web_refresh_ctl = tk.DoubleVar(value=float(self.cfg.ui.live_web_refresh_seconds))
        self.live_smooth_ctl = tk.BooleanVar(value=bool(getattr(self.cfg.ui, "live_smoothing_enabled", False)))
        self.live_smooth_sec_ctl = tk.IntVar(value=int(getattr(self.cfg.ui, "live_smoothing_seconds", 10)))

        ctl = ttk.Frame(top2)
        ctl.pack(side="left")

        ttk.Label(ctl, text=self.t('live.polling_s')).grid(row=0, column=0, padx=(0, 4))
        cb_poll = ttk.Combobox(ctl, width=4, state="readonly", textvariable=self.live_poll_ctl, values=[0.5, 1.0, 2.0, 5.0])
        cb_poll.grid(row=0, column=1, padx=(0, 10))

        ttk.Label(ctl, text=self.t('live.window_min')).grid(row=0, column=2, padx=(0, 4))
        cb_win = ttk.Combobox(ctl, width=4, state="readonly", textvariable=self.live_window_ctl, values=[5, 10, 15, 30, 60, 120])
        cb_win.grid(row=0, column=3, padx=(0, 10))

        ttk.Checkbutton(ctl, text=self.t('live.web'), variable=self.live_web_ctl).grid(row=0, column=4, padx=(0, 6))
        ttk.Label(ctl, text=self.t('live.update_s')).grid(row=0, column=5, padx=(0, 4))
        cb_web = ttk.Combobox(ctl, width=4, state="readonly", textvariable=self.live_web_refresh_ctl, values=[0.5, 1.0, 2.0, 5.0])
        cb_web.grid(row=0, column=6, padx=(0, 10))

        ttk.Checkbutton(ctl, text=self.t('live.smoothing'), variable=self.live_smooth_ctl).grid(row=0, column=7, padx=(0, 6))
        cb_smooth = ttk.Combobox(ctl, width=3, state="readonly", textvariable=self.live_smooth_sec_ctl, values=[0, 3, 5, 10, 20, 30])
        cb_smooth.grid(row=0, column=8, padx=(0, 10))

        # Open current log file (useful for debugging)
        try:
            ttk.Button(ctl, text=self.t('live.open_log'), command=self._open_log_file).grid(row=0, column=10, padx=(6, 0))
        except Exception:
            pass

        # Apply Live controls immediately when changed (no Apply button)
        try:
            def _live_ctl_changed(_e=None):
                self._schedule_apply_live_controls()

            for _w in (cb_poll, cb_win, cb_web, cb_smooth):
                try:
                    _w.bind('<<ComboboxSelected>>', _live_ctl_changed, add='+')
                except Exception:
                    _w.bind('<<ComboboxSelected>>', _live_ctl_changed)

            try:
                self.live_web_ctl.trace_add('write', lambda *_a: self._schedule_apply_live_controls())
            except Exception:
                pass
            try:
                self.live_smooth_ctl.trace_add('write', lambda *_a: self._schedule_apply_live_controls())
            except Exception:
                pass
        except Exception:
            pass

        self.live_status = tk.StringVar(value=self.t('live.off'))
        ttk.Label(top, textvariable=self.live_status).pack(side="left", padx=12)

        # QR code (shown when Live Web Dashboard is enabled)
        self._qr_photo = None  # keep reference
        self._qr_url = None  # current live web URL for click-to-open
        self.qr_label = ttk.Label(top)
        self.qr_label.pack(side="right", padx=8)
        # Make QR clickable: open the live web dashboard in the default browser
        try:
            self.qr_label.configure(cursor="hand2")
        except Exception:
            pass
        try:
            self.qr_label.bind("<Button-1>", self._open_qr_url, add="+")
        except Exception:
            try:
                self.qr_label.bind("<Button-1>", self._open_qr_url)
            except Exception:
                pass

        # Slightly smaller font for Live "info lines" so we gain vertical space for plots.
        try:
            st = ttk.Style(self)
            base_f = tkfont.nametofont("TkDefaultFont")
            live_f = base_f.copy()
            try:
                live_f.configure(size=max(9, int(base_f.cget("size")) - 1))
            except Exception:
                pass
            self._live_info_font = live_f  # keep reference
            st.configure("LiveInfo.TLabel", font=live_f)
        except Exception:
            pass

        body = ttk.Frame(frm)
        body.pack(fill="both", expand=True, padx=12, pady=(0, 12))
        body.columnconfigure(0, weight=1)
        body.columnconfigure(1, weight=1)
        body.rowconfigure(0, weight=1)
        self._live_figs = {}
        self._live_axes = {}
        self._live_canvases = {}
        self._live_latest_vars = {}
        self._live_switch_vars = {}
        devs = self._get_visible_devices()  # UI shows 2 columns; the page selector chooses which devices are shown.
        for col, d in enumerate(devs):
            colfrm = ttk.LabelFrame(body, text=d.name)
            colfrm.grid(row=0, column=col, sticky="nsew", padx=(0 if col == 0 else 8, 8))
            colfrm.rowconfigure(0, weight=0)
            colfrm.rowconfigure(1, weight=1)
            colfrm.rowconfigure(2, weight=1)
            colfrm.rowconfigure(3, weight=1)
            colfrm.columnconfigure(0, weight=1)
            self._live_figs[d.key] = {}
            self._live_axes[d.key] = {}
            self._live_canvases[d.key] = {}
            # Realtime values (latest sample)
            status_fr = ttk.Frame(colfrm)
            status_fr.grid(row=0, column=0, sticky="ew", padx=8, pady=(6, 0))
            status_fr.columnconfigure(0, weight=1)

            # We keep the individual vars for internal logic, but show a compact 2-line summary in the UI.
            v_power = tk.StringVar(value="–")
            v_volt = tk.StringVar(value="–")
            v_curr = tk.StringVar(value="–")
            v_kwh = tk.StringVar(value="–")
            v_stamp = tk.StringVar(value="–")
            v_line0 = tk.StringVar(value="–")  # Power + kWh today + updated
            v_line1 = tk.StringVar(value="–")  # Voltage + current
            v_line2 = tk.StringVar(value="–")  # VAR + cos φ
            self._live_latest_vars[d.key] = {
                "power": v_power,
                "voltage": v_volt,
                "current": v_curr,
                "kwh_today": v_kwh,
                "stamp": v_stamp,
                "line0": v_line0,
                "line1": v_line1,
                "line2": v_line2,
            }

            ttk.Label(status_fr, textvariable=v_line0, style="LiveInfo.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(status_fr, textvariable=v_line1, style="LiveInfo.TLabel").grid(row=1, column=0, sticky="w", pady=(2, 0))
            ttk.Label(status_fr, textvariable=v_line2, style="LiveInfo.TLabel").grid(row=2, column=0, sticky="w", pady=(2, 0))

            # Switch state + toggle (only for kind == 'switch')
            if str(getattr(d, "kind", "em")) == "switch":
                v_sw = tk.StringVar(value="–")
                self._live_switch_vars[d.key] = v_sw
                # Switch line (kept compact)
                sw_fr = ttk.Frame(status_fr)
                sw_fr.grid(row=3, column=0, sticky="ew", pady=(2, 0))
                sw_fr.columnconfigure(0, weight=1)
                ttk.Label(sw_fr, text=f"{self.t('live.cards.switch')}: ", style="LiveInfo.TLabel").pack(side="left")
                ttk.Label(sw_fr, textvariable=v_sw, style="LiveInfo.TLabel").pack(side="left")
                ttk.Button(sw_fr, text=self.t('live.switch.toggle'), command=lambda dk=d.key: self._toggle_switch(dk)).pack(side="right")

            # Power
            box_p = ttk.Labelframe(colfrm, text=self.t('live.chart.power'))
            box_p.grid(row=1, column=0, sticky="nsew", padx=8, pady=6)
            fig_p = Figure(figsize=(6, 2.2), dpi=100)
            ax_p = fig_p.add_subplot(111)
            ax_p.set_ylabel("W")
            canvas_p = FigureCanvasTkAgg(fig_p, master=box_p)
            canvas_p.get_tk_widget().pack(fill="both", expand=True)
            # Voltage (all phases)
            v_title = self.t('live.chart.voltage.1p') if int(getattr(d, 'phases', 3) or 3) <= 1 else self.t('live.chart.voltage')
            box_v = ttk.Labelframe(colfrm, text=v_title)
            box_v.grid(row=2, column=0, sticky="nsew", padx=8, pady=6)
            fig_v = Figure(figsize=(6, 2.2), dpi=100)
            ax_v = fig_v.add_subplot(111)
            ax_v.set_ylabel("V")
            canvas_v = FigureCanvasTkAgg(fig_v, master=box_v)
            canvas_v.get_tk_widget().pack(fill="both", expand=True)
            # Current (all phases)
            c_title = self.t('live.chart.current.1p') if int(getattr(d, 'phases', 3) or 3) <= 1 else self.t('live.chart.current')
            box_c = ttk.Labelframe(colfrm, text=c_title)
            box_c.grid(row=3, column=0, sticky="nsew", padx=8, pady=6)
            fig_c = Figure(figsize=(6, 2.2), dpi=100)
            ax_c = fig_c.add_subplot(111)
            ax_c.set_ylabel("A")
            canvas_c = FigureCanvasTkAgg(fig_c, master=box_c)
            canvas_c.get_tk_widget().pack(fill="both", expand=True)
            self._live_figs[d.key]["power"] = fig_p
            self._live_figs[d.key]["voltage"] = fig_v
            self._live_figs[d.key]["current"] = fig_c
            self._live_axes[d.key]["power"] = ax_p
            self._live_axes[d.key]["voltage"] = ax_v
            self._live_axes[d.key]["current"] = ax_c
            self._live_canvases[d.key]["power"] = canvas_p
            self._live_canvases[d.key]["voltage"] = canvas_v
            self._live_canvases[d.key]["current"] = canvas_c

            # Re-layout on size changes (e.g. move app between monitors)
            for _cv in (canvas_p, canvas_v, canvas_c):
                try:
                    # Same as above: don't replace Matplotlib's internal resize binding.
                    _cv.get_tk_widget().bind("<Configure>", self._on_live_canvas_configure, add="+")
                except Exception:
                    pass

        # If live is already running (e.g. user changed the device page),
        # update button states/QR/status so the new Live tab reflects reality.
        try:
            self._sync_live_ui_state()
        except Exception:
            pass

    def _on_live_canvas_configure(self, _event=None) -> None:
        """Throttle live plot relayout on window/monitor size changes."""
        try:
            if self._live_relayout_job:
                self.after_cancel(self._live_relayout_job)
        except Exception:
            pass

        def _do() -> None:
            try:
                # Only redraw layout if live is running and we have data.
                if self._live_pollers:
                    self._redraw_live_plots()
            except Exception:
                pass

        try:
            self._live_relayout_job = self.after(250, _do)
        except Exception:
            _do()

    def _on_live_freeze_toggle(self) -> None:
        """Freeze/unfreeze the live UI.

        Data collection continues; we only stop updating the UI/plots so you can
        inspect a snapshot. When unfreezing we redraw immediately and restore the
        normal status text (including the Web URL).
        """
        try:
            if self._live_frozen.get():
                # Remember current status (may include Web URL) and append Freeze.
                try:
                    self._live_status_before_freeze = self.live_status.get()
                except Exception:
                    self._live_status_before_freeze = None
                try:
                    base = self._live_status_before_freeze or self.t('live.running')
                except Exception:
                    base = self._live_status_before_freeze or self.t('live.running')
                try:
                    self.live_status.set(str(base) + ' (Freeze)')
                except Exception:
                    pass
            else:
                # Restore status and force immediate redraw
                try:
                    self._live_last_redraw = 0.0
                except Exception:
                    pass
                try:
                    self._sync_live_ui_state()
                except Exception:
                    pass
                try:
                    if self._live_pollers:
                        self._redraw_live_plots()
                except Exception:
                    pass
                try:
                    self._live_status_before_freeze = None
                except Exception:
                    pass
        except Exception:
            pass

        # Keep a plain bool mirror for the web server thread.
        try:
            self._live_frozen_state = bool(self._live_frozen.get())
        except Exception:
            pass


    # -------- Etappe 6: Report (PDF) + Current View Export --------
    def _report_anchor_date(self) -> pd.Timestamp:
        """Anchor date selection: invoice anchor -> export start -> today."""
        anchor = _parse_date_flexible(self.invoice_anchor_var.get() if hasattr(self, "invoice_anchor_var") else "")
        start, _end = self._parse_export_range()
        if anchor is None and start is not None:
            anchor = start
        if anchor is None:
            anchor = pd.Timestamp(date.today())
        return pd.Timestamp(anchor)

    def _export_day_report_pdf(self) -> None:
        self._export_energy_report_pdf(period="day")

    def _export_month_report_pdf(self) -> None:
        self._export_energy_report_pdf(period="month")

    def _export_energy_report_pdf(self, *, period: str) -> None:
        if not self._ensure_data_loaded():
            messagebox.showinfo(self.t("msg.report"), self.t("export.no_data"))
            return

        anchor = self._report_anchor_date()

        if period == "day":
            start = pd.Timestamp(anchor.date())
            end = start + pd.Timedelta(days=1)
            title = self.t("pdf.report.title.day")
            out_name = f"energy_report_day_{start.strftime('%Y%m%d')}_{time.strftime('%H%M%S')}.pdf"
        else:
            start = pd.Timestamp(anchor.replace(day=1).date())
            # first day of next month
            if start.month == 12:
                end = pd.Timestamp(date(start.year + 1, 1, 1))
            else:
                end = pd.Timestamp(date(start.year, start.month + 1, 1))
            title = self.t("pdf.report.title.month")
            out_name = f"energy_report_month_{start.strftime('%Y%m')}_{time.strftime('%H%M%S')}.pdf"

        # Human friendly, consistent date range label (end is exclusive)
        try:
            end_incl = (pd.Timestamp(end) - pd.Timedelta(seconds=1))
        except Exception:
            end_incl = pd.Timestamp(end)
        period_label = f"{format_date_local(self.lang, pd.Timestamp(start))} – {format_date_local(self.lang, pd.Timestamp(end_incl))}"

        out_dir = Path(self.export_dir.get())
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / out_name

        # Pricing (gross + net) for clear PDF header note
        try:
            unit_gross = float(self.cfg.pricing.unit_price_gross())
            unit_net = float(self.cfg.pricing.unit_price_net())
            price_incl_vat = bool(getattr(self.cfg.pricing, "price_includes_vat", True))
            vat_enabled = bool(getattr(self.cfg.pricing, "vat_enabled", True))
            vat_rate = float(getattr(self.cfg.pricing, "vat_rate_percent", 0.0))
        except Exception:
            unit_gross, unit_net, price_incl_vat, vat_enabled, vat_rate = 0.0, 0.0, True, True, 0.0

        # Pricing note (localized + clear VAT)
        gross_s = format_number_local(self.lang, unit_gross, 4)
        net_s = format_number_local(self.lang, unit_net, 4)
        vat_s = format_number_local(self.lang, vat_rate, 0)

        if (not vat_enabled) or vat_rate <= 0:
            pricing_note = self.t("pdf.pricing.no_vat", price=gross_s, vat=vat_s)
        else:
            if price_incl_vat:
                pricing_note = self.t("pdf.pricing.gross_incl_vat", gross=gross_s, net=net_s, vat=vat_s)
            else:
                pricing_note = self.t("pdf.pricing.net_excl_vat", gross=gross_s, net=net_s, vat=vat_s)

        devices_payload = []
        for d in self.cfg.devices:
            df = self.computed[d.key].df.copy()
            df = filter_by_time(df, start=start, end=end)
            devices_payload.append((d.key, d.name, df))

        export_pdf_energy_report_variant1(
            out_path=out_path,
            title=title,
            period_label=period_label,
            pricing_note=pricing_note,
            unit_price_gross=unit_gross,
            devices=devices_payload,
            lang=self.lang,
        )
        self.export_log.insert("end", f"{self.t('msg.report_written')}: {out_path}\n")
        self.export_log.see("end")

    def _export_current_view_png(self) -> None:
        self._export_current_view(fmt="png")

    def _export_current_view_pdf(self) -> None:
        self._export_current_view(fmt="pdf")

    def _export_current_view(self, *, fmt: str) -> None:
        fmt = str(fmt or "png").lower().strip()
        out_dir = Path(self.export_dir.get())
        out_dir.mkdir(parents=True, exist_ok=True)

        try:
            sel = self.notebook.select()
        except Exception:
            sel = ""

        if sel == str(self.tab_live):
            # Export current live view (visible devices)
            try:
                self._export_live_snapshot_files(out_dir, fmt=fmt)
            except Exception as e:
                messagebox.showerror(self.t("msg.export"), self.t('export.err.live_failed', e=e))
            return

        if sel == str(self.tab_plots):
            # Export currently selected plot (metric + device)
            try:
                metric_order = ["kwh", "V", "A", "W"]
                nb = self._plots_metric_nb
                if nb is None:
                    raise RuntimeError(self.t('export.err.plots_notebook'))
                idx = int(nb.index("current"))
                metric_key = metric_order[idx] if 0 <= idx < len(metric_order) else None
                if not metric_key:
                    raise RuntimeError(self.t('export.err.active_plot_tab'))
                dev_nb = self._plots_device_nb.get(metric_key)
                order = self._plots_device_order.get(metric_key, [])
                if dev_nb is None or not order:
                    raise RuntimeError(self.t('export.err.active_device'))
                didx = int(dev_nb.index("current"))
                device_key = order[didx] if 0 <= didx < len(order) else None
                if not device_key:
                    raise RuntimeError(self.t('export.err.active_device'))
                fig = self._plots_figs2.get(metric_key, {}).get(device_key)
                if fig is None:
                    raise RuntimeError(self.t('export.err.figure_unavailable'))
                # filename
                ts = time.strftime("%Y%m%d_%H%M%S")
                fname = f"current_view_{metric_key}_{device_key}_{ts}.{fmt}"
                out_path = out_dir / fname
                fig.savefig(out_path, dpi=150, bbox_inches="tight", format=fmt)
                self.export_log.insert("end", self.t('export.current_view_exported', path=out_path) + "\n")
                self.export_log.see("end")
            except Exception as e:
                messagebox.showerror(self.t("msg.export"), self.t('export.err.plots_failed', e=e))
            return

        messagebox.showinfo(self.t("msg.export"), self.t('export.current_view_info'))

    def _export_live_snapshot_files(self, out_dir: Path, *, fmt: str = "png") -> Sequence[Path]:
        """Export the current live view (visible devices) as PNG/PDF."""
        fmt = str(fmt or "png").lower().strip()
        out_dir = Path(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        # Reuse the drawing logic from _export_live_png but allow pdf
        out_paths: List[Path] = []
        for d in self._get_visible_devices():
            try:
                df = self._live_history.get(d.key)
                if df is None or df.empty:
                    continue
                df = df.sort_values("timestamp")
                # Only current window shown in UI
                win_min = int(getattr(self.cfg.ui, "live_window_minutes", 10) or 10)
                cutoff = pd.Timestamp.now() - pd.Timedelta(minutes=win_min)
                dfw = df[df["timestamp"] >= cutoff].copy()
                if dfw.empty:
                    continue

                fig = Figure(figsize=(11, 6.5), dpi=120)
                ax1 = fig.add_subplot(311)
                ax2 = fig.add_subplot(312, sharex=ax1)
                ax3 = fig.add_subplot(313, sharex=ax1)

                xs = dfw["timestamp"]

                # Power
                ax1.plot(xs, pd.to_numeric(dfw.get("total_power", 0.0), errors="coerce"), label="W")
                ax1.set_ylabel("W")
                ax1.grid(True, axis="y", alpha=0.25)

                # Voltage phases if present
                ph = int(getattr(d, "phases", 3) or 3)
                phase_cols = [("a_voltage","L1")] if ph <= 1 else [("a_voltage","L1"),("b_voltage","L2"),("c_voltage","L3")]
                for col, label in phase_cols:
                    if col in dfw.columns:
                        ax2.plot(xs, pd.to_numeric(dfw[col], errors="coerce"), label=label)
                if "avg_voltage" in dfw.columns:
                    ax2.plot(xs, pd.to_numeric(dfw["avg_voltage"], errors="coerce"), label="Ø")
                ax2.set_ylabel("V")
                ax2.grid(True, axis="y", alpha=0.25)
                try:
                    ax2.legend(loc="upper right", fontsize=8)
                except Exception:
                    pass

                # Current phases if present
                ph = int(getattr(d, "phases", 3) or 3)
                phase_cols = [("a_current","L1")] if ph <= 1 else [("a_current","L1"),("b_current","L2"),("c_current","L3")]
                for col, label in phase_cols:
                    if col in dfw.columns:
                        ax3.plot(xs, pd.to_numeric(dfw[col], errors="coerce"), label=label)
                if "avg_current" in dfw.columns:
                    ax3.plot(xs, pd.to_numeric(dfw["avg_current"], errors="coerce"), label="Σ/Ø")
                ax3.set_ylabel("A")
                ax3.grid(True, axis="y", alpha=0.25)
                try:
                    ax3.legend(loc="upper right", fontsize=8)
                except Exception:
                    pass

                fig.autofmt_xdate()

                ts = time.strftime("%Y%m%d_%H%M%S")
                out_path = out_dir / f"live_{d.key}_{ts}.{fmt}"
                fig.savefig(out_path, dpi=150, bbox_inches="tight", format=fmt)
                out_paths.append(out_path)
            except Exception:
                continue

        if out_paths:
            self.export_log.insert("end", f"Live Current View exportiert ({fmt}): {len(out_paths)} Datei(en)\n")
            self.export_log.see("end")
        return out_paths

    def _export_live_png(self) -> None:
        """Export the current live view as PNG images.

        We export one PNG per visible device containing Power/Voltage/Current.
        """
        try:
            out_dir = filedialog.askdirectory(title="Export PNG")
        except Exception:
            out_dir = None
        if not out_dir:
            return

        try:
            win_m = int(self.cfg.ui.live_window_minutes)
        except Exception:
            win_m = 10

        ts_lbl = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_paths: List[Path] = []

        def _slice(arr):
            if not arr:
                return []
            try:
                newest = int(arr[-1][0])
            except Exception:
                return list(arr)
            cutoff = newest - int(max(1, win_m)) * 60
            return [(t, v) for (t, v) in arr if int(t) >= cutoff]

        for d in self._get_visible_devices():
            metrics = self._live_series.get(d.key) or {}
            fig = Figure(figsize=(10, 7.5), dpi=max(120, self._dpi_for_widget(self)))
            ax1 = fig.add_subplot(311)
            ax2 = fig.add_subplot(312, sharex=ax1)
            ax3 = fig.add_subplot(313, sharex=ax1)

            # Power
            arr = _slice(metrics.get("total_power", []))
            if arr:
                xs = [datetime.fromtimestamp(t) for t, _ in arr]
                ys = [v for _, v in arr]
                ax1.plot(xs, ys)
            ax1.set_ylabel("W")
            ax1.grid(True, axis="y", alpha=0.3)
            ax1.set_title(f"{d.name} – Live Snapshot")

            # Voltage
            ph = int(getattr(d, "phases", 3) or 3)
            phase_keys = [("a_voltage", "L1")] if ph <= 1 else [("a_voltage", "L1"), ("b_voltage", "L2"), ("c_voltage", "L3")]
            for key, label in phase_keys:
                arr = _slice(metrics.get(key, []))
                if not arr:
                    continue
                xs = [datetime.fromtimestamp(t) for t, _ in arr]
                ys = [v for _, v in arr]
                ax2.plot(xs, ys, label=label)
            ax2.set_ylabel("V")
            ax2.grid(True, axis="y", alpha=0.3)
            try:
                ax2.legend(loc="upper right", fontsize=9)
            except Exception:
                pass

            # Current
            ph = int(getattr(d, "phases", 3) or 3)
            phase_keys = [("a_current", "L1")] if ph <= 1 else [("a_current", "L1"), ("b_current", "L2"), ("c_current", "L3")]
            for key, label in phase_keys:
                arr = _slice(metrics.get(key, []))
                if not arr:
                    continue
                xs = [datetime.fromtimestamp(t) for t, _ in arr]
                ys = [v for _, v in arr]
                ax3.plot(xs, ys, label=label)
            ax3.set_ylabel("A")
            ax3.set_xlabel(self.t("live.time"))
            ax3.grid(True, axis="y", alpha=0.3)
            try:
                ax3.legend(loc="upper right", fontsize=9)
            except Exception:
                pass

            # Time axis formatting (robust)
            for ax in (ax1, ax2, ax3):
                self._apply_smart_date_axis(ax, self, max_labels=8)
                # keep compact time labels for live snapshots
                ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
            try:
                for t in ax1.get_xticklabels():
                    t.set_visible(False)
                for t in ax2.get_xticklabels():
                    t.set_visible(False)
            except Exception:
                pass
            try:
                fig.subplots_adjust(left=0.08, right=0.98, top=0.94, bottom=0.10, hspace=0.22)
            except Exception:
                pass

            out_path = Path(out_dir) / f"live_{d.key}_{ts_lbl}.png"
            try:
                export_figure_png(fig, out_path, dpi=int(max(150, self._dpi_for_widget(self))))
                out_paths.append(out_path)
            except Exception as e:
                try:
                    messagebox.showerror(self.t("export.err.png_save_title"), self.t("export.err.png_save", e=e))
                except Exception:
                    pass

        try:
            if out_paths:
                self.live_status.set(f"PNG exportiert: {len(out_paths)} Datei(en)")
        except Exception:
            pass

    def _extract_switch_on(self, raw: Dict[str, Any]) -> Optional[bool]:
        """Best-effort extraction of on/off from Switch.GetStatus payload."""
        if not isinstance(raw, dict):
            return None
        def _coerce(v: Any) -> Optional[bool]:
            if v is None:
                return None
            if isinstance(v, bool):
                return v
            if isinstance(v, (int, float)):
                return bool(v)
            if isinstance(v, str):
                s = v.strip().lower()
                if s in ("on", "true", "1", "yes", "ein"):
                    return True
                if s in ("off", "false", "0", "no", "aus"):
                    return False
                # Unknown string; fall back to truthiness
                return bool(s)
            try:
                return bool(v)
            except Exception:
                return None

        # 1) Direct keys (typical for RPC responses)
        for k in ("output", "on", "ison", "is_on", "state"):
            if k in raw:
                return _coerce(raw.get(k))

        # 2) Gen1 /status response nests relay state under `relays` (list).
        # Many devices have multiple relays; the web UI shows a single pill per
        # configured device, so we treat the device as "on" if ANY channel is on.
        try:
            relays = raw.get("relays")
            if isinstance(relays, list) and relays:
                vals: List[Optional[bool]] = []
                for item in relays:
                    if not isinstance(item, dict):
                        continue
                    for k in ("ison", "on", "state"):
                        if k in item:
                            vals.append(_coerce(item.get(k)))
                            break
                if any(v is True for v in vals):
                    return True
                if any(v is False for v in vals):
                    return False
        except Exception:
            pass

        # 3) Some devices nest under `status` or `result` (or expose arrays).
        for parent in ("status", "result"):
            try:
                d = raw.get(parent)
                if isinstance(d, dict):
                    # Direct keys
                    for k in ("output", "on", "ison", "is_on", "state"):
                        if k in d:
                            return _coerce(d.get(k))
                    # Array-style
                    for arr_key in ("switches", "relays"):
                        arr = d.get(arr_key)
                        if isinstance(arr, list) and arr:
                            vals: List[Optional[bool]] = []
                            for item in arr:
                                if not isinstance(item, dict):
                                    continue
                                for kk in ("output", "ison", "on", "is_on", "state"):
                                    if kk in item:
                                        vals.append(_coerce(item.get(kk)))
                                        break
                            if any(v is True for v in vals):
                                return True
                            if any(v is False for v in vals):
                                return False
            except Exception:
                pass

        # 4) RPC map-style payloads (e.g. {"switch:0": {"output": true, ...}}).
        # IMPORTANT: Some devices expose multiple channels (switch:0, switch:1, ...).
        # Older logic returned the *first* channel which caused "AUS" even when another
        # channel was ON. We therefore aggregate: ON if ANY channel is ON.
        try:
            vals: List[Optional[bool]] = []
            for k, v in raw.items():
                if not (isinstance(k, str) and isinstance(v, dict)):
                    continue
                if not (k.startswith("switch:") or k.startswith("relay:")):
                    continue
                for kk in ("output", "on", "ison", "is_on", "state"):
                    if kk in v:
                        vals.append(_coerce(v.get(kk)))
                        break
            if any(v is True for v in vals):
                return True
            if any(v is False for v in vals):
                return False
        except Exception:
            pass

        return None

    def _toggle_switch(self, device_key: str) -> None:
        """Toggle a switch-type Shelly (Gen2/Plus/Pro) from the Live tab."""
        dev = next((d for d in self.cfg.devices if d.key == device_key), None)
        if dev is None:
            return
        if str(getattr(dev, "kind", "")) != "switch":
            return

        # Determine desired state from last known UI state
        cur_txt = None
        try:
            cur_txt = self._live_switch_vars.get(device_key).get()
        except Exception:
            cur_txt = None
        cur_on = None
        if isinstance(cur_txt, str):
            if "on" in cur_txt.lower() or "ein" in cur_txt.lower():
                cur_on = True
            if "off" in cur_txt.lower() or "aus" in cur_txt.lower():
                cur_on = False
        target_on = (not cur_on) if cur_on is not None else True

        def _worker() -> None:
            try:
                http = ShellyHttp(
                    HttpConfig(
                        timeout_seconds=float(self.cfg.download.timeout_seconds),
                        retries=int(self.cfg.download.retries),
                        backoff_base_seconds=float(self.cfg.download.backoff_base_seconds),
                    )
                )
                set_switch_state(http, dev.host, int(dev.em_id), bool(target_on))
                # Confirm
                st = get_switch_status(http, dev.host, int(dev.em_id))
                on2 = self._extract_switch_on(st)
            except Exception as e:
                try:
                    self.after(0, lambda: self.live_status.set(f"{dev.name}: {e}"))
                except Exception:
                    pass
                return

            def _apply() -> None:
                try:
                    v = self._live_switch_vars.get(device_key)
                    if v is not None and on2 is not None:
                        v.set(self.t("live.switch.on") if on2 else self.t("live.switch.off"))
                except Exception:
                    pass

            try:
                self.after(0, _apply)
            except Exception:
                _apply()

        try:
            threading.Thread(target=_worker, daemon=True).start()
        except Exception:
            _worker()

    def _update_qr(self, url: Optional[str]) -> None:
        """Render a QR code for the given URL in the Live top bar."""
        # Keep the URL so the QR image can be clicked to open the dashboard
        try:
            self._qr_url = url
        except Exception:
            pass
        if not url:
            try:
                self.qr_label.configure(image="")
            except Exception:
                pass
            self._qr_photo = None
            # restore default cursor when nothing to open
            try:
                self.qr_label.configure(cursor="")
            except Exception:
                pass
            return
        try:
            qr = qrcode.QRCode(border=2, box_size=4)
            qr.add_data(url)
            qr.make(fit=True)
            img = qr.make_image(fill_color="black", back_color="white").convert("RGB")
            # Dynamic size: smaller on small screens
            try:
                sw = int(self.winfo_screenwidth())
            except Exception:
                sw = 1400
            # QR size: keep it readable but avoid stealing vertical space from the plots.
            # Previously this used ~7% of screen width (clamped 90..140px). Requested: ~50% smaller.
            base = int(max(90, min(140, sw * 0.07)))
            size = max(45, base // 2)
            img = img.resize((size, size))
            photo = ImageTk.PhotoImage(img)
            self._qr_photo = photo
            self.qr_label.configure(image=photo)
        except Exception:
            self._qr_photo = None
            try:
                self.qr_label.configure(image="")
            except Exception:
                pass

    def _open_qr_url(self, _e=None) -> None:
        """Open the current Live Web Dashboard URL in the default browser.

        The QR code in the Live top bar is clickable.
"""
        url = getattr(self, '_qr_url', None)
        if not url:
            return
        try:
            import webbrowser
            webbrowser.open(str(url))
        except Exception as e:
            try:
                self.live_status.set(f"{self.t('live.web')}: {e}")
            except Exception:
                pass

    def _on_web_window_change(self, minutes: int) -> None:
        """Called from the web dashboard thread when the user changes the window in the browser."""

        def _apply() -> None:
            try:
                m = int(minutes)
            except Exception:
                return
            if m <= 0:
                return
            # NOTE: AppConfig/UiConfig are frozen dataclasses.
            # Update via replace() so the change propagates everywhere.
            try:
                self.cfg = replace(self.cfg, ui=replace(self.cfg.ui, live_window_minutes=m))
            except Exception:
                return
            # keep UI controls in sync
            try:
                # Live tab combobox uses this IntVar
                self.live_window_ctl.set(int(m))
            except Exception:
                pass
            try:
                # Backwards-compat (older settings widgets)
                self.live_window_var.set(str(m))
            except Exception:
                pass
            try:
                self.set_live_window_var.set(str(m))
            except Exception:
                pass
            # ensure the web store keeps enough points
            try:
                poll_s = max(0.2, float(self.cfg.ui.live_poll_seconds))
                retention_m = int(getattr(self.cfg.ui, "live_retention_minutes", 120))
                max_points = int((retention_m * 60.0) / poll_s) + 50
                if self._live_state_store is not None:
                    self._live_state_store.set_max_points(max_points)
            except Exception:
                pass
            # persist to config
            try:
                save_config(self.cfg, self.cfg_path)
            except Exception:
                pass

        # marshal into Tk thread
        try:
            self.root.after(0, _apply)
        except Exception:
            _apply()

    def _web_action_dispatch(
        self,
        action: str,
        params: Dict[str, Any],
        progress: Optional[Callable[[str, int, int, str], None]] = None,
    ) -> Dict[str, Any]:
        """Handle remote actions from the phone web dashboard.

        Runs in the web server thread. Must NOT touch Tk widgets.
        """
        action = str(action or "").strip()
        params = params if isinstance(params, dict) else {}
        out_root = (self.project_root / "exports").resolve()
        out_root.mkdir(parents=True, exist_ok=True)

        def _pdate(x: Any) -> Optional[pd.Timestamp]:
            try:
                return _parse_date_flexible(str(x or "").strip())
            except Exception:
                return None

        # --- Plotly plots data (JSON) ---
        if action == "plots_data":
            return self._web_plots_data(params)

        # --- Live Freeze (UI-only; web triggers a command handled on Tk thread) ---
        if action in {"get_freeze", "set_freeze", "toggle_freeze"}:
            # We must not touch Tk variables here (web server thread). Use the
            # plain-bool mirror and a command queue drained on the Tk thread.
            cur = bool(getattr(self, "_live_frozen_state", False))
            if action == "get_freeze":
                return {"ok": True, "freeze": cur}

            if action == "set_freeze":
                # Accept both {freeze:true} and {on:true} for convenience.
                if "freeze" in params:
                    desired = bool(params.get("freeze"))
                elif "on" in params:
                    desired = bool(params.get("on"))
                else:
                    return {"ok": False, "error": "missing freeze"}
                try:
                    self._ui_cmd_q.put(("set_freeze", desired))
                except Exception:
                    pass
                # Optimistic immediate response.
                self._live_frozen_state = desired
                return {"ok": True, "freeze": bool(desired)}

            # toggle
            desired = (not cur)
            try:
                self._ui_cmd_q.put(("set_freeze", desired))
            except Exception:
                pass
            self._live_frozen_state = desired
            return {"ok": True, "freeze": bool(desired)}

        # --- Switch control (Gen2/Plus/Pro) ---
        if action in {"get_switch", "set_switch", "toggle_switch"}:
            device_key = str(params.get("device_key") or "").strip()
            dev = next((d for d in self.cfg.devices if d.key == device_key), None)
            if dev is None:
                return {"ok": False, "error": "unknown device"}
            if str(getattr(dev, "kind", "")) != "switch":
                return {"ok": False, "error": "not a switch"}

            http = ShellyHttp(
                HttpConfig(
                    timeout_seconds=float(self.cfg.download.timeout_seconds),
                    retries=int(self.cfg.download.retries),
                    backoff_base_seconds=float(self.cfg.download.backoff_base_seconds),
                )
            )

            # Determine current state.
            # NOTE: Some Shellys expose multiple switch/relay channels. The config's
            # em_id is used as the preferred channel id for toggling, but for display
            # we want a correct *device* status (ON if any channel is ON) to avoid
            # showing "AUS" while another channel is actually ON.
            try:
                st = get_switch_status(http, dev.host, int(dev.em_id))
                cur_on = self._extract_switch_on(st)
                if cur_on is not True:
                    try:
                        full = get_shelly_status(http, dev.host)
                        any_on = self._extract_switch_on(full)
                        if any_on is True:
                            cur_on = True
                    except Exception:
                        pass
            except Exception as e:
                return {"ok": False, "error": str(e)}
            if action == "get_switch":
                return {"ok": True, "on": bool(cur_on)}

            target_on: Optional[bool] = None
            if action == "set_switch":
                if "on" not in params:
                    return {"ok": False, "error": "missing on"}
                target_on = bool(params.get("on"))
            else:
                target_on = (not bool(cur_on))

            try:
                set_switch_state(http, dev.host, int(dev.em_id), bool(target_on))
                st2 = get_switch_status(http, dev.host, int(dev.em_id))
                on2 = self._extract_switch_on(st2)
                return {"ok": True, "on": bool(on2)}
            except Exception as e:
                return {"ok": False, "error": str(e)}

        if action == "sync":
            mode = str(params.get("mode") or "incremental")
            start_date = str(params.get("start_date") or "").strip()
            now = int(time.time())
            range_override: Optional[Tuple[int, int]] = None
            label = mode
            if mode == "custom":
                if not start_date:
                    raise ValueError("custom requires start_date (TT.MM.JJJJ)")
                from datetime import datetime
                from zoneinfo import ZoneInfo
                dt = datetime.strptime(start_date, "%d.%m.%Y").replace(tzinfo=ZoneInfo("Europe/Berlin"))
                a = int(dt.timestamp())
                b = now
                if b <= a:
                    b = a + 1
                range_override = (a, b)
                label = f"ab {start_date}"
            elif mode == "day":
                range_override = (max(0, now - 86400), now)
            elif mode == "week":
                range_override = (max(0, now - 7 * 86400), now)
            elif mode == "month":
                range_override = (max(0, now - 30 * 86400), now)
            else:
                range_override = None

            results = sync_all(
                self.cfg,
                self.storage,
                range_override=range_override,
                fallback_last_days=7,
                progress=progress,
            )
            summary = []
            for r in results:
                ok_chunks = sum(1 for c in r.chunks if c.ok)
                err = next((c for c in r.chunks if not c.ok), None)
                summary.append(
                    {
                        "device": r.device_name,
                        "range": [int(r.requested_range[0]), int(r.requested_range[1])],
                        "ok_chunks": int(ok_chunks),
                        "total_chunks": int(len(r.chunks)),
                        "error": err.error if err else None,
                        "updated_last_end_ts": r.updated_last_end_ts,
                    }
                )
            return {"ok": True, "mode": mode, "label": label, "results": summary}

        if action == "plots":
            mode = str(params.get("mode") or "days")
            start = _pdate(params.get("start"))
            end = _pdate(params.get("end"))
            if start is not None and end is not None and end < start:
                start, end = end, start
            # Load+compute fresh from storage (no Tk dependency)
            from shelly_analyzer.services.compute import load_device
            from shelly_analyzer.core.energy import filter_by_time
            from shelly_analyzer.core.stats import daily_kwh, weekly_kwh, monthly_kwh
            from matplotlib.figure import Figure
            import math

            def _series(df: pd.DataFrame, m: str) -> Tuple[List[str], List[float]]:
                if df is None or df.empty:
                    return [], []
                if m == "all":
                    total = float(pd.to_numeric(df["energy_kwh"], errors="coerce").fillna(0.0).sum())
                    return ["Total"], [total]
                if m == "days":
                    s = daily_kwh(df)
                    return [pd.Timestamp(x).strftime("%Y-%m-%d") for x in s.index], [float(v) for v in s.values]
                if m == "weeks":
                    s = weekly_kwh(df)
                    return [str(x) for x in s.index], [float(v) for v in s.values]
                if m == "months":
                    s = monthly_kwh(df)
                    return [str(x) for x in s.index], [float(v) for v in s.values]
                return [], []

            def _apply_xticks(ax, labels: List[str]) -> None:
                if not labels:
                    return
                n = len(labels)
                max_labels = 40
                if n <= max_labels:
                    ax.set_xticks(range(n))
                    ax.set_xticklabels(labels, rotation=45, ha="right")
                    return
                step = int(math.ceil(n / max_labels))
                ticks = list(range(0, n, step))
                ax.set_xticks(ticks)
                ax.set_xticklabels([labels[i] for i in ticks], rotation=45, ha="right")

            ts = time.strftime("%Y%m%d_%H%M%S")
            web_dir = out_root / "web"
            web_dir.mkdir(parents=True, exist_ok=True)
            files: List[Dict[str, str]] = []

            devs2 = list(self.cfg.devices[:2])
            total = max(1, len(devs2))
            for idx, d in enumerate(devs2, start=1):
                if progress:
                    try:
                        progress(d.key, idx-1, total, f"Plot {mode} …")
                    except Exception:
                        pass
                cd = load_device(self.storage, d)
                df_use = filter_by_time(cd.df, start=start, end=end)
                labels, values = _series(df_use, mode)
                fig = Figure(figsize=(11, 3.6), dpi=170)
                ax = fig.add_subplot(111)
                ax.set_ylabel("kWh")
                bars = ax.bar(range(len(values)), values)
                _apply_xticks(ax, labels)
                ax.grid(True, axis="y", alpha=0.3)
                # annotate
                for b in bars:
                    try:
                        h = float(b.get_height())
                    except Exception:
                        continue
                    ax.annotate(
                        f"{h:.2f}",
                        xy=(b.get_x() + b.get_width() / 2, h),
                        xytext=(0, 3),
                        textcoords="offset points",
                        ha="center",
                        va="bottom",
                        fontsize=8,
                        rotation=90,
                    )
                # title with optional range
                rng = ""
                if start is not None or end is not None:
                    a = start.date().isoformat() if start is not None else "…"
                    b = end.date().isoformat() if end is not None else "…"
                    rng = f" | {a}–{b}"
                fig.suptitle(f"{d.name} – {mode}{rng}", fontsize=12)
                fig.tight_layout()
                safe = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in d.name).strip("_")
                out = web_dir / f"plot_{safe or d.key}_{mode}_{ts}.png"
                export_figure_png(fig, out, dpi=180)
                files.append({"name": out.name, "url": f"/files/web/{out.name}"})
                if progress:
                    try:
                        progress(d.key, idx, total, "OK")
                    except Exception:
                        pass

            return {"ok": True, "files": files}

        if action == "export_summary":
            start = _pdate(params.get("start"))
            end = _pdate(params.get("end"))
            if start is not None and end is not None and end < start:
                start, end = end, start
            # normalize date-only bounds like the GUI does
            def _looks_like_date_only(x: Any) -> bool:
                s = str(x or "").strip()
                return (":" not in s) and (len(s) <= 10)
            if end is not None and _looks_like_date_only(params.get("end")):
                end = end.normalize() + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
            if start is not None and _looks_like_date_only(params.get("start")):
                start = start.normalize()

            from shelly_analyzer.services.compute import load_device, summarize
            from shelly_analyzer.core.energy import filter_by_time

            unit_gross = float(self.cfg.pricing.unit_price_gross())
            totals: List[ReportTotals] = []
            computed_local: Dict[str, ComputedDevice] = {}
            for d in self.cfg.devices:
                cd = load_device(self.storage, d)
                computed_local[d.key] = cd
                df = filter_by_time(cd.df, start=start, end=end)
                kwh, avgp, maxp = summarize(df)
                totals.append(ReportTotals(name=d.name, kwh_total=kwh, cost_eur=kwh * unit_gross, avg_power_w=avgp, max_power_w=maxp))

            label = self.t("period.all")
            if start is not None or end is not None:
                label = f"{format_date_local(self.lang, start) if start is not None else '…'} {self.t('common.to')} {format_date_local(self.lang, end) if end is not None else '…'}"

            # embed plots (days/weeks/months) per Shelly
            ts = time.strftime("%Y%m%d_%H%M%S")
            plots_dir = out_root / "web" / "plots"
            plots_dir.mkdir(parents=True, exist_ok=True)
            plot_pages: List[Tuple[str, Path]] = []
            for d in self.cfg.devices[:2]:
                cd = computed_local.get(d.key)
                if cd is None:
                    continue
                df_f = filter_by_time(cd.df, start=start, end=end)
                tmp = ComputedDevice(device_key=cd.device_key, device_name=cd.device_name, df=df_f)
                for m in ("days", "weeks", "months"):
                    # reuse plot generator above by calling ourselves (fast)
                    # but keep title stable in PDF
                    fig = self._make_stats_figure(tmp, m)
                    safe = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in d.name).strip("_")
                    png = plots_dir / f"{safe or d.key}_{m}_{ts}.png"
                    export_figure_png(fig, png, dpi=180)
                    plot_pages.append((f"{d.name} – {m}", png))

            out = out_root / "web" / f"summary_{ts}.pdf"
            vat_part = ""
            if self.cfg.pricing.vat_enabled:
                vat_part = f" ({self.t('pdf.vat')} {format_number_local(self.lang, self.cfg.pricing.vat_rate_percent, 1)}%)"
            note = self.t(
                "pdf.summary.note",
                price=format_number_local(self.lang, unit_gross, 4),
                vat_part=vat_part,
                version=__version__,
            )
            export_pdf_summary(
                title=self.t("pdf.summary.title"),
                period_label=label,
                totals=totals,
                out_path=out,
                note=note,
                plot_pages=plot_pages,
                lang=self.lang,
            )
            return {"ok": True, "files": [{"name": out.name, "url": f"/files/web/{out.name}"}]}

        if action == "export_invoices":
            start = _pdate(params.get("start"))
            end = _pdate(params.get("end"))
            period = str(params.get("period") or "custom")
            anchor = _pdate(params.get("anchor"))
            if period != "custom":
                if anchor is None and start is not None:
                    anchor = start
                if anchor is None:
                    anchor = pd.Timestamp(date.today())
                start, end = _period_bounds(anchor, period)

            from shelly_analyzer.services.compute import load_device, summarize
            from shelly_analyzer.core.energy import filter_by_time

            inv_dir = out_root / "web" / "invoices"
            inv_dir.mkdir(parents=True, exist_ok=True)
            unit_net = float(self.cfg.pricing.unit_price_net())
            issue = date.today()
            due = issue + timedelta(days=int(self.cfg.billing.payment_terms_days))
            ts = time.strftime("%Y%m%d")
            files: List[Dict[str, str]] = []
            for d in self.cfg.devices[:2]:
                cd = load_device(self.storage, d)
                df = filter_by_time(cd.df, start=start, end=end)
                kwh, _avgp, _maxp = summarize(df)
                if start is None and end is None:
                    period_label = self.t("period.all")
                    suffix = "all"
                else:
                    period_label = f"{format_date_local(self.lang, start) if start is not None else '…'} {self.t('common.to')} {format_date_local(self.lang, end) if end is not None else '…'}"
                    if period == "day" and start is not None:
                        suffix = start.strftime("%Y%m%d")
                    elif period == "week" and start is not None:
                        iso = start.isocalendar()
                        suffix = f"W{iso.week:02d}{iso.year}"
                    elif period == "month" and start is not None:
                        suffix = start.strftime("%Y%m")
                    elif period == "year" and start is not None:
                        suffix = start.strftime("%Y")
                    else:
                        suffix = f"{(start.date().isoformat() if start is not None else 'x')}-{(end.date().isoformat() if end is not None else 'y')}"

                invoice_no = f"{self.cfg.billing.invoice_prefix}-{ts}-{d.key}-{period}-{suffix}"
                safe = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in d.name).strip("_")
                out = inv_dir / f"invoice_{invoice_no}_{safe or d.key}.pdf"
                line = InvoiceLine(
                    description=self.t("pdf.invoice.line_energy", device=d.name, period=period_label),
                    quantity=float(kwh),
                    unit="kWh",
                    unit_price_net=unit_net,
                )
                lines = [line]
                try:
                    base_year = float(getattr(self.cfg.pricing, 'base_fee_eur_per_year', 0.0))
                except Exception:
                    base_year = 0.0
                if base_year > 0:
                    # determine effective date span for proration
                    if start is None and end is None:
                        if not df.empty:
                            s_eff = pd.Timestamp(df['timestamp'].min()).normalize()
                            e_eff = pd.Timestamp(df['timestamp'].max()).normalize()
                        else:
                            s_eff = pd.Timestamp(date.today()).normalize()
                            e_eff = s_eff
                    else:
                        s_eff = pd.Timestamp(start).normalize() if start is not None else pd.Timestamp(df['timestamp'].min()).normalize()
                        e_eff = pd.Timestamp(end).normalize() if end is not None else pd.Timestamp(df['timestamp'].max()).normalize()
                    days = int((e_eff.date() - s_eff.date()).days) + 1
                    days = max(1, days)
                    base_day_net = float(self.cfg.pricing.base_fee_day_net())
                    lines.append(InvoiceLine(description=self.t("pdf.invoice.line_base_fee", days=days), quantity=float(days), unit=self.t("unit.days"), unit_price_net=base_day_net))
                export_pdf_invoice(
                    out_path=out,
                    invoice_no=invoice_no,
                    issue_date=issue,
                    due_date=due,
                    issuer={
                        "name": self.cfg.billing.issuer.name,
                        "address_lines": self.cfg.billing.issuer.address_lines,
                        "vat_id": self.cfg.billing.issuer.vat_id,
                        "email": self.cfg.billing.issuer.email,
                        "phone": self.cfg.billing.issuer.phone,
                        "iban": self.cfg.billing.issuer.iban,
                        "bic": self.cfg.billing.issuer.bic,
                    },
                    customer={
                        "name": self.cfg.billing.customer.name,
                        "address_lines": self.cfg.billing.customer.address_lines,
                    },
                    lines=lines,
                    vat_rate_percent=float(self.cfg.pricing.vat_rate_percent),
                    vat_enabled=bool(self.cfg.pricing.vat_enabled),
                    lang=self.lang,
                )
                files.append({"name": out.name, "url": f"/files/web/invoices/{out.name}"})

            return {"ok": True, "files": files}

        # --- Etappe 6 (optional): Report Button im Web-Control ---
        if action == "report":
            period = str(params.get("period") or params.get("kind") or "day").strip().lower()
            anchor = _pdate(params.get("anchor"))
            if anchor is None:
                anchor = pd.Timestamp(date.today())
            anchor = pd.Timestamp(anchor)

            if period in {"month", "mon", "m"}:
                start = pd.Timestamp(anchor.replace(day=1).date())
                # first day of next month
                if int(start.month) == 12:
                    end = pd.Timestamp(date(int(start.year) + 1, 1, 1))
                else:
                    end = pd.Timestamp(date(int(start.year), int(start.month) + 1, 1))
                title = self.t("pdf.report.title.month")
                fname = f"energy_report_month_{start.strftime('%Y%m')}_{time.strftime('%H%M%S')}.pdf"
            else:
                start = pd.Timestamp(anchor.date())
                end = start + pd.Timedelta(days=1)
                title = self.t("pdf.report.title.day")
                fname = f"energy_report_day_{start.strftime('%Y%m%d')}_{time.strftime('%H%M%S')}.pdf"

            # Human friendly, consistent date range label (end is exclusive)
            try:
                end_incl = (pd.Timestamp(end) - pd.Timedelta(seconds=1))
            except Exception:
                end_incl = pd.Timestamp(end)
            period_label = f"{format_date_local(self.lang, pd.Timestamp(start))} – {format_date_local(self.lang, pd.Timestamp(end_incl))}"

            # Pricing (gross + net) for clear PDF header note
            try:
                unit_gross = float(self.cfg.pricing.unit_price_gross())
                unit_net = float(self.cfg.pricing.unit_price_net())
                price_incl_vat = bool(getattr(self.cfg.pricing, "price_includes_vat", True))
                vat_enabled = bool(getattr(self.cfg.pricing, "vat_enabled", True))
                vat_rate = float(getattr(self.cfg.pricing, "vat_rate_percent", 0.0))
            except Exception:
                unit_gross, unit_net, price_incl_vat, vat_enabled, vat_rate = 0.0, 0.0, True, True, 0.0

            # Pricing note (localized + clear VAT)
            gross_s = format_number_local(self.lang, unit_gross, 4)
            net_s = format_number_local(self.lang, unit_net, 4)
            vat_s = format_number_local(self.lang, vat_rate, 0)

            if (not vat_enabled) or vat_rate <= 0:
                pricing_note = self.t("pdf.pricing.no_vat", price=gross_s, vat=vat_s)
            else:
                if price_incl_vat:
                    pricing_note = self.t("pdf.pricing.gross_incl_vat", gross=gross_s, net=net_s, vat=vat_s)
                else:
                    pricing_note = self.t("pdf.pricing.net_excl_vat", gross=gross_s, net=net_s, vat=vat_s)

            from shelly_analyzer.services.compute import load_device
            from shelly_analyzer.core.energy import filter_by_time
            from shelly_analyzer.services.export import export_pdf_energy_report_variant1

            rep_dir = out_root / "web" / "reports"
            rep_dir.mkdir(parents=True, exist_ok=True)
            out_path = rep_dir / fname

            devices_payload: List[Tuple[str, str, pd.DataFrame]] = []
            total = max(1, len(self.cfg.devices))
            for idx, d in enumerate(self.cfg.devices, start=1):
                if progress:
                    try:
                        progress(d.key, idx - 1, total, "Lade …")
                    except Exception:
                        pass
                cd = load_device(self.storage, d)
                df_use = filter_by_time(cd.df, start=start, end=end)
                devices_payload.append((d.key, d.name, df_use))
                if progress:
                    try:
                        progress(d.key, idx, total, "OK")
                    except Exception:
                        pass

            export_pdf_energy_report_variant1(
                out_path=out_path,
                title=title,
                period_label=period_label,
                pricing_note=pricing_note,
                unit_price_gross=unit_gross,
                devices=devices_payload,
                lang=self.lang,
            )

            return {
                "ok": True,
                "period": period,
                "period_label": period_label,
                "files": [{"name": out_path.name, "url": f"/files/web/reports/{out_path.name}"}],
            }

        if action == "bundle":
            # Create a ZIP bundle of recently generated exports (PDFs/plots) for easy sharing.
            try:
                hours = int(params.get("hours") or 48)
            except Exception:
                hours = 48
            hours = max(1, min(24 * 365, hours))
            since = time.time() - (hours * 3600)

            web_dir = out_root / "web"
            web_dir.mkdir(parents=True, exist_ok=True)
            ts = time.strftime("%Y%m%d_%H%M%S")
            zpath = web_dir / f"bundle_{ts}.zip"

            # collect files under exports/ newer than 'since'
            exp_root = out_root
            wanted_ext = {".pdf", ".png", ".jpg", ".jpeg", ".xlsx", ".csv", ".json", ".txt", ".log"}
            paths: List[Path] = []
            for p in exp_root.rglob("*"):
                try:
                    if not p.is_file():
                        continue
                    if p.suffix.lower() not in wanted_ext:
                        continue
                    if p.name.startswith("bundle_") and p.suffix.lower() == ".zip":
                        continue
                    if p.stat().st_mtime < since:
                        continue
                    paths.append(p)
                except Exception:
                    continue
            paths.sort(key=lambda x: x.stat().st_mtime if x.exists() else 0.0, reverse=True)

            total = max(1, len(paths))
            done = 0
            if progress:
                try:
                    progress("bundle", 0, total, f"ZIP (letzte {hours}h) …")
                except Exception:
                    pass

            from zipfile import ZipFile, ZIP_DEFLATED

            with ZipFile(zpath, "w", compression=ZIP_DEFLATED) as zf:
                # Add a config snapshot for convenience
                try:
                    snap = {
                        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "hours": hours,
                        "version": __version__,
                        "pricing": {
                            "unit_price_gross": float(self.cfg.pricing.unit_price_gross()),
                            "unit_price_net": float(self.cfg.pricing.unit_price_net()),
                            "vat_enabled": bool(self.cfg.pricing.vat_enabled),
                            "vat_rate_percent": float(self.cfg.pricing.vat_rate_percent),
                            "price_includes_vat": bool(self.cfg.pricing.price_includes_vat),
                        },
                        "devices": [{"key": d.key, "name": d.name, "host": d.host} for d in self.cfg.devices[:2]],
                    }
                    zf.writestr("config_snapshot.json", json.dumps(snap, indent=2, ensure_ascii=False))
                except Exception:
                    pass

                for p in paths:
                    rel = p.relative_to(exp_root)
                    try:
                        zf.write(p, arcname=str(rel))
                    except Exception:
                        pass
                    done += 1
                    if progress and (done % 5 == 0 or done == total):
                        try:
                            progress("bundle", done, total, f"{done}/{total} Dateien")
                        except Exception:
                            pass

            if progress:
                try:
                    progress("bundle", total, total, "OK")
                except Exception:
                    pass

            return {"ok": True, "files": [{"name": zpath.name, "url": f"/files/web/{zpath.name}"}], "count": len(paths), "hours": hours}

        raise ValueError(f"Unknown action: {action}")


    def _web_plots_data(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Build JSON payload for the Plotly /plots page.

        Runs in the web server thread. Must not touch Tk.
        """
        try:
            view = str(params.get("view") or "timeseries")
            devices_raw = str(params.get("devices") or "")
            dev_keys_in = [k.strip() for k in devices_raw.split(",") if k and str(k).strip()]
            lang = str(params.get("lang") or self.lang or "de")

            # Load dfs directly from storage (no cached self.computed needed)
            dev_cfgs = {d.key: d for d in self.cfg.devices}

            def _norm(s: str) -> str:
                s = (s or "").strip().lower()
                # common german/locale variants
                s = s.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
                for ch in [" ", "-", "_", "."]:
                    s = s.replace(ch, "")
                return s

            # Resolve user/device tokens (e.g. "haus") to configured keys (e.g. "shelly1").
            # Web URLs often use the human label, while storage uses keys.
            alias: Dict[str, str] = {}
            for d in self.cfg.devices:
                alias[_norm(getattr(d, "key", ""))] = d.key
                alias[_norm(getattr(d, "name", ""))] = d.key
            dev_keys: List[str] = []
            for raw in dev_keys_in:
                k = alias.get(_norm(raw), raw)
                if k and k not in dev_keys:
                    dev_keys.append(k)
            dev_keys = dev_keys[:2]

            # Ensure existing CSV data is discoverable even after key renames / folder moves.
            # This is critical for the Plotly /plots page which relies on storage directly.
            try:
                self.storage.ensure_data_for_devices(
                    [{"key": d.key, "host": getattr(d, "host", ""), "name": getattr(d, "name", d.key)} for d in self.cfg.devices]
                )
            except Exception:
                pass

            # If no devices provided (or UI didn't include them yet), fall back to the first two configured devices.
            if not dev_keys:
                dev_keys = [d.key for d in self.cfg.devices[:2] if getattr(d, "key", None)]

            start = None
            end = None
            try:
                if str(params.get("start") or "").strip():
                    start = _parse_date_flexible(str(params.get("start")))
                if str(params.get("end") or "").strip():
                    end = _parse_date_flexible(str(params.get("end")))
            except Exception:
                start = None
                end = None

            def _df_for(key: str) -> pd.DataFrame:
                try:
                    return self.storage.read_device_df(key)
                except Exception:
                    return pd.DataFrame()

            if view == "kwh":
                mode = str(params.get("mode") or "days")

                # Support preset windows (e.g. last 1h/24h/7d) in the web UI.
                # The Plotly page sends len+unit for presets. For kWh we turn
                # that into a start/end window similar to the timeseries path.
                # (Previously, kWh ignored len/unit and always aggregated the
                # full dataset unless a custom start/end was specified.)
                try:
                    if start is None and end is None and str(params.get("len") or "").strip():
                        try:
                            ln_kwh = float(params.get("len") or 24.0)
                        except Exception:
                            ln_kwh = 24.0
                        unit_kwh = str(params.get("unit") or "hours")
                        delta_kwh = pd.Timedelta(hours=ln_kwh)
                        if unit_kwh.startswith("min"):
                            delta_kwh = pd.Timedelta(minutes=ln_kwh)
                        elif unit_kwh.startswith("day"):
                            delta_kwh = pd.Timedelta(days=ln_kwh)
                        # start/end will be computed per-device from its max timestamp.
                        _kwh_preset = {"delta": delta_kwh}
                    else:
                        _kwh_preset = None
                except Exception:
                    _kwh_preset = None
                labels: List[str] = []
                traces: List[Dict[str, Any]] = []
                diag: Dict[str, Any] = {
                    "requested_devices": dev_keys,
                    "counts": {},
                    "data": getattr(self.storage, "last_data_diag", {}),
                    "base_dir": str(getattr(self.storage, "base_dir", "")),
                }
                for k in dev_keys:
                    df = _df_for(k)
                    if df is None or len(df) == 0:
                        diag["counts"][k] = 0
                        continue
                    diag["counts"][k] = int(len(df))
                    if start is not None or end is not None:
                        df = filter_by_time(df, start, end)
                    elif _kwh_preset is not None:
                        # Filter to last N units based on each device's latest timestamp.
                        try:
                            # Prefer 'timestamp' (our canonical col), fall back to 'ts'
                            col = "timestamp" if "timestamp" in df.columns else ("ts" if "ts" in df.columns else None)
                            if col:
                                end_i = pd.to_datetime(df[col], errors="coerce").max()
                                if end_i is not pd.NaT and end_i == end_i:
                                    start_i = end_i - _kwh_preset["delta"]
                                    df = filter_by_time(df, start_i, end_i)
                        except Exception:
                            pass
                    # energy per row -> aggregate
                    # energy per row -> aggregate (supports hours/days/weeks/months and custom unit:n)
                    lbls, vals = self._stats_series(df, mode)
                    s = pd.Series(vals, index=[str(x) for x in lbls], dtype="float64")


                    # Align labels across devices
                    idx = [str(x) for x in s.index.tolist()]
                    if not labels:
                        labels = idx
                    else:
                        # union
                        all_lab = sorted(set(labels).union(idx))
                        labels = all_lab
                    name = dev_cfgs.get(k).name if k in dev_cfgs else k
                    traces.append({"key": k, "name": name, "series": s})

                # Build y arrays aligned
                out_traces: List[Dict[str, Any]] = []
                for tr in traces:
                    s = tr["series"]
                    y = []
                    for lab in labels:
                        try:
                            y.append(float(s.get(lab, 0.0)))
                        except Exception:
                            y.append(0.0)
                    out_traces.append({"key": tr["key"], "name": tr["name"], "y": y})

                title = f"kWh • {mode}"
                return {"ok": True, "view": "kwh", "labels": labels, "traces": out_traces, "title": title, "diag": diag}

            # timeseries
            metric = str(params.get("metric") or "W").upper().strip()
            metric_norm = metric
            metric_label = {'W':'W','V':'V','A':'A','VAR':'VAR','Q':'VAR','COSPHI':'cos φ','PF':'cos φ','POWERFACTOR':'cos φ'}.get(metric_norm, metric_norm)

            series = str(params.get("series") or "total").lower().strip()
            series_mode = 'phases' if series.startswith('phase') else 'total'

            try:
                ln = float(params.get("len") or 24.0)
            except Exception:
                ln = 24.0
            unit = str(params.get("unit") or "hours")
            delta = pd.Timedelta(hours=ln)
            if unit.startswith("min"):
                delta = pd.Timedelta(minutes=ln)
            elif unit.startswith("day"):
                delta = pd.Timedelta(days=ln)

            out_devs: List[Dict[str, Any]] = []
            diag_ts: Dict[str, Any] = {
                "requested_devices": dev_keys,
                "counts": {},
                "data": getattr(self.storage, "last_data_diag", {}),
                "base_dir": str(getattr(self.storage, "base_dir", "")),
            }
            for k in dev_keys:
                df = _df_for(k)
                if df is None or len(df) == 0:
                    diag_ts["counts"][k] = 0
                    continue
                diag_ts["counts"][k] = int(len(df))
                if start is None and end is None:
                    try:
                        end_i = pd.to_datetime(df["ts"]).max()
                        start_i = end_i - delta
                    except Exception:
                        start_i, end_i = None, None
                    dff = filter_by_time(df, start_i, end_i)
                else:
                    dff = filter_by_time(df, start, end)

                # Build series
                s_total, ylab = self._wva_series(dff, metric)
                phases = self._wva_phase_series(dff, metric)
                # Safety: if helper returned non-datetime index, align with timestamp column
                try:
                    if not isinstance(s_total.index, pd.DatetimeIndex) and 'timestamp' in dff.columns:
                        ts_idx = pd.to_datetime(dff['timestamp'], errors='coerce')
                        s_total = pd.Series(pd.to_numeric(s_total, errors='coerce').to_numpy(), index=pd.DatetimeIndex(ts_idx)).dropna().sort_index()
                        if s_total.index.has_duplicates:
                            s_total = s_total.groupby(level=0).mean()
                    # align phases
                    if isinstance(phases, dict) and phases and 'timestamp' in dff.columns:
                        ts_idx = pd.DatetimeIndex(pd.to_datetime(dff['timestamp'], errors='coerce'))
                        msk = ~pd.isna(ts_idx)
                        for kk in list(phases.keys()):
                            try:
                                ps = phases.get(kk)
                                if ps is None:
                                    continue
                                if not isinstance(ps.index, pd.DatetimeIndex):
                                    ps = pd.Series(pd.to_numeric(ps, errors='coerce').to_numpy(), index=ts_idx)
                                ps = ps[msk].dropna().sort_index()
                                if ps.index.has_duplicates:
                                    ps = ps.groupby(level=0).mean()
                                phases[kk] = ps
                            except Exception:
                                continue
                except Exception:
                    phases = {}

                # If device is single-phase, only expose L1 in phases payload.
                try:
                    if int(getattr(dev_cfgs.get(k), "phases", 3) or 3) <= 1:
                        phases = {"L1": phases.get("L1")} if ("L1" in phases) else {}
                except Exception:
                    pass

                # Downsample for large payloads
                def _downsample(s: pd.Series) -> pd.Series:
                    try:
                        if len(s) <= 2500:
                            return s
                        # Choose a resample rule based on span
                        span = (s.index.max() - s.index.min()) if len(s) else pd.Timedelta(hours=0)
                        rule = "1min"
                        if span > pd.Timedelta(days=14):
                            rule = "30min"
                        elif span > pd.Timedelta(days=3):
                            rule = "10min"
                        elif span > pd.Timedelta(hours=12):
                            rule = "2min"
                        return s.resample(rule).mean().dropna()
                    except Exception:
                        return s

                s_total = _downsample(s_total)
                # Robust datetime serialization: the index may not always be a DatetimeIndex.
                # (e.g. object Index after certain merges/downsamping in some pandas versions)
                _idx = pd.to_datetime(getattr(s_total, "index", []), errors="coerce")
                def _iso_ts(x: Any) -> str:
                    """Return ISO 8601 for timestamps without pandas nanosecond warnings."""
                    try:
                        if x is pd.NaT or x != x:
                            return ""
                        ts = pd.Timestamp(x)
                        # pandas>=2.1: Timestamp.to_pydatetime has warn=...
                        try:
                            return ts.to_pydatetime(warn=False).isoformat()
                        except TypeError:
                            # Older pandas: drop nanoseconds explicitly to avoid noisy warnings
                            try:
                                ts = ts.floor("us")
                            except Exception:
                                pass
                            return ts.isoformat()
                    except Exception:
                        return ""

                xs = [_iso_ts(x) for x in _idx]
                ys = [float(v) if v == v else 0.0 for v in s_total.values.tolist()]  # NaN->0
                dev_name = dev_cfgs.get(k).name if k in dev_cfgs else k
                out = {"key": k, "name": dev_name, "x": xs, "y": ys}
                if series_mode == "phases":
                    try:
                        out.pop("y", None)
                    except Exception:
                        pass
                try:
                    out["mapping"] = str(getattr(self, "_last_wva_mapping_text", ""))
                except Exception:
                    out["mapping"] = ""
                if phases:
                    ph_out: Dict[str, Any] = {}
                    for pk, ps in phases.items():
                        ps = _downsample(ps)
                        ph_out[pk] = {
                                "x": [_iso_ts(x) for x in pd.to_datetime(getattr(ps, "index", []), errors="coerce")],
                            "y": [float(v) if v == v else 0.0 for v in ps.values.tolist()],
                        }
                    if series_mode == "phases":
                        out["phases"] = ph_out
                out_devs.append(out)

            title = f"{metric_label} • {ln:g} {unit}"
            return {"ok": True, "view": "timeseries", "metric": metric, "metric_label": metric_label, "series": series_mode, "devices": out_devs, "title": title, "diag": diag_ts}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def _schedule_apply_live_controls(self) -> None:
        """Debounce applying Live controls to avoid expensive restarts on every click."""
        try:
            job = getattr(self, '_apply_live_job', None)
            if job:
                self.after_cancel(job)
        except Exception:
            pass
        try:
            self._apply_live_job = self.after(180, self._apply_live_controls)
        except Exception:
            try:
                self._apply_live_controls()
            except Exception:
                pass

    def _apply_live_controls(self) -> None:
        """Apply Live controls from the Live tab and persist them to config.

        Changes should take effect immediately. We only restart live polling when
        absolutely necessary (e.g. polling interval changes). Window/smoothing
        changes update the UI and the web dashboard without restarting.
        """
        # Read new values
        try:
            poll_s = float(self.live_poll_ctl.get())
        except Exception:
            poll_s = float(self.cfg.ui.live_poll_seconds)
        try:
            win_m = int(self.live_window_ctl.get())
        except Exception:
            win_m = int(self.cfg.ui.live_window_minutes)
        web_on = bool(self.live_web_ctl.get())
        try:
            web_refresh_s = float(self.live_web_refresh_ctl.get())
        except Exception:
            web_refresh_s = float(self.cfg.ui.live_web_refresh_seconds)

        # Optional smoothing for Live plots
        try:
            smooth_on = bool(self.live_smooth_ctl.get())
        except Exception:
            smooth_on = bool(getattr(self.cfg.ui, 'live_smoothing_enabled', False))
        try:
            smooth_sec = int(self.live_smooth_sec_ctl.get())
        except Exception:
            smooth_sec = int(getattr(self.cfg.ui, 'live_smoothing_seconds', 10))

        poll_s = max(0.2, float(poll_s))
        win_m = max(1, int(win_m))
        web_refresh_s = max(0.25, float(web_refresh_s))
        smooth_sec = max(0, int(smooth_sec))

        # Detect what changed
        old_ui = self.cfg.ui

        def _f(x):
            try:
                return float(x)
            except Exception:
                return 0.0

        changed_poll = abs(_f(getattr(old_ui, 'live_poll_seconds', poll_s)) - poll_s) > 1e-9
        changed_win = int(getattr(old_ui, 'live_window_minutes', win_m)) != win_m
        changed_web_on = bool(getattr(old_ui, 'live_web_enabled', web_on)) != web_on
        changed_web_refresh = abs(_f(getattr(old_ui, 'live_web_refresh_seconds', web_refresh_s)) - web_refresh_s) > 1e-9
        changed_smooth = (
            bool(getattr(old_ui, 'live_smoothing_enabled', smooth_on)) != smooth_on
            or int(getattr(old_ui, 'live_smoothing_seconds', smooth_sec)) != smooth_sec
        )

        # Persist config
        self.cfg = replace(
            self.cfg,
            version=__version__,
            ui=replace(
                old_ui,
                live_poll_seconds=poll_s,
                live_window_minutes=win_m,
                live_web_enabled=web_on,
                live_web_refresh_seconds=web_refresh_s,
                live_smoothing_enabled=smooth_on,
                live_smoothing_seconds=smooth_sec,
            ),
        )
        try:
            save_config(self.cfg)
        except Exception:
            pass

        running = bool(self._live_pollers)
        if not running:
            try:
                self.live_status.set(self.t('live.settings_saved'))
            except Exception:
                pass
            return

        # Apply changes live
        if changed_poll:
            # Polling interval impacts the poller thread schedule -> restart
            try:
                self._stop_live()
                time.sleep(0.15)
            except Exception:
                pass
            try:
                self._start_live()
            except Exception:
                pass
            return

        # Web enable/refresh changes -> restart only the web dashboard
        if changed_web_on or changed_web_refresh:
            try:
                if self._live_web:
                    self._live_web.stop()
            except Exception:
                pass
            self._live_web = None

        # Window change -> update web dashboard window immediately if running
        if changed_win and self._live_web is not None:
            try:
                self._live_web.set_window_minutes(win_m)
            except Exception:
                pass

        # Re-sync status/QR and retention sizing; then redraw plots
        try:
            self._sync_live_ui_state()
        except Exception:
            pass

        if changed_win or changed_smooth or changed_web_on or changed_web_refresh:
            try:
                self._live_last_redraw = 0.0
            except Exception:
                pass
            try:
                if not self._live_frozen.get():
                    self._redraw_live_plots()
            except Exception:
                pass


    def _start_live(self) -> None:

        # Apply quick controls so Web/Window settings take effect even if
        # the user did not click "Apply".
        try:
            self._apply_live_controls()
        except Exception:
            pass
        # Start live polling for *all configured devices*.
        # The UI shows only two devices at a time, but polling continues for all.
        if not self._live_pollers:
            devs_all = list(self.cfg.devices)

            # Build ringbuffers sized to current retention + poll interval.
            try:
                retention_m = int(getattr(self.cfg.ui, "live_retention_minutes", 120))
                window_s = max(60, retention_m * 60)
                poll_s = max(0.2, float(self.cfg.ui.live_poll_seconds))
                max_points = int(window_s / poll_s) + 80
            except Exception:
                max_points = 1500
            self._ensure_live_series_capacity(max_points, devices=devs_all, reset=True)

            # Poll devices in parallel using a shared thread pool.
            self._live_pollers = [
                MultiLivePoller(devs_all, self.cfg.download, poll_seconds=self.cfg.ui.live_poll_seconds)
            ]
            for p in self._live_pollers:
                p.start()

            # Start a fresh incremental sync in the background when Live starts,
            # so "today" calculations have a good baseline.
            try:
                if not (self._sync_thread and self._sync_thread.is_alive()):
                    self._start_sync("incremental", label=self.t("sync.auto_live"))
            except Exception:
                pass

            # Also compute today's base kWh from existing imported data (non-blocking).
            try:
                self._start_today_kwh_base_refresh()
            except Exception:
                pass

        # Ensure buttons/QR/status are correct even if live was already running
        # and the user just switched device pages.
        self._sync_live_ui_state()

    def _stop_live(self) -> None:
        for p in self._live_pollers:
            try:
                p.stop()
            except Exception:
                pass
        self._live_pollers = []
        try:
            if self._live_web:
                self._live_web.stop()
        except Exception:
            pass
        self._live_web = None
        self._sync_live_ui_state()

    def _sync_live_ui_state(self) -> None:
        """Keep Live tab controls in sync with the actual live state.

        Live polling always runs for all configured devices; the UI only shows
        two devices per page.
        """
        running = bool(self._live_pollers)
        # Buttons
        try:
            self.btn_live_start.configure(state="disabled" if running else "normal")
            self.btn_live_stop.configure(state="normal" if running else "disabled")
        except Exception:
            pass

        if not running:
            # stop web, clear QR/status
            try:
                if self._live_web:
                    self._live_web.stop()
            except Exception:
                pass
            self._live_web = None
            self._update_qr(None)
            try:
                self.live_status.set("Live ist aus.")
            except Exception:
                pass
            return

        # Running
        # Start/stop web dashboard depending on settings
        try:
            want_web = bool(self.cfg.ui.live_web_enabled)
        except Exception:
            want_web = False

        if not want_web:
            try:
                if self._live_web:
                    self._live_web.stop()
            except Exception:
                pass
            self._live_web = None
            self._update_qr(None)
            try:
                self.live_status.set(self.t('live.running'))
            except Exception:
                pass
            return

        # Ensure store retention matches current window/polling.
        try:
            retention_m = int(getattr(self.cfg.ui, "live_retention_minutes", 120))
            window_s = max(60, retention_m * 60)
            poll_s = max(0.2, float(self.cfg.ui.live_poll_seconds))
            max_points = int(window_s / poll_s) + 50
        except Exception:
            max_points = 1200
        try:
            if self._live_state_store is None:
                self._live_state_store = LiveStateStore(max_points=max_points)
            else:
                self._live_state_store.set_max_points(max_points)
        except Exception:
            pass

        # Also adjust desktop live ringbuffers to match retention.
        try:
            self._ensure_live_series_capacity(max_points, devices=list(self.cfg.devices), reset=False)
        except Exception:
            pass

        # Start web dashboard if not running
        try:
            if self._live_web is None:
                # Mirror current device metadata into a runtime file so the web UI
                # can recover devices ...
                try:
                    self._write_runtime_devices_meta()
                except Exception:
                    pass
                devs_all = [(d.key, d.name) for d in self.cfg.devices]
                devs_meta = [
                    {
                        "key": d.key,
                        "name": d.name,
                        "kind": str(getattr(d, "kind", "") or ""),
                        "phases": int(getattr(d, "phases", 3) or 3),
                    }
                    for d in self.cfg.devices
                ]
                self._live_web = LiveWebDashboard(
                    self._live_state_store,
                    port=int(self.cfg.ui.live_web_port),
                    refresh_seconds=float(self.cfg.ui.live_web_refresh_seconds),
                    window_minutes=int(self.cfg.ui.live_window_minutes),
                    devices=devs_all,
                    devices_meta=devs_meta,
                    out_dir=self.project_root,
                    lang=self.lang,
                    on_window_change=self._on_web_window_change,
                    on_action=self._web_action_dispatch,
                )
                self._live_web.start()
            url = self._live_web.url() if self._live_web else None
            self._update_qr(url)
            if url:
                self.live_status.set(self.t('live.status.running_web', url=url))
            else:
                self.live_status.set(self.t('live.running'))
        except Exception as e:
            # Keep live polling running; just disable web.
            try:
                if self._live_web:
                    self._live_web.stop()
            except Exception:
                pass
            self._live_web = None
            self._update_qr(None)
            try:
                self.live_status.set(self.t('live.status.web_error', e=e))
            except Exception:
                pass
            try:
                messagebox.showerror(self.t('msg.live_web'), self.t('live.web_error') + "\n" + str(e))
            except Exception:
                pass

    def _ensure_live_series_capacity(
        self,
        max_points: int,
        devices: Optional[List[DeviceConfig]] = None,
        reset: bool = False,
    ) -> None:
        """Ensure live ringbuffers exist and have sufficient capacity."""
        max_points = int(max(100, max_points))
        if devices is None:
            devices = list(getattr(self.cfg, "devices", []) or [])

        # Fast path
        if (not reset) and int(self._live_max_points or 0) == max_points and self._live_series:
            return

        new_series: Dict[str, Dict[str, Any]] = {}
        for d in devices:
            prev = self._live_series.get(d.key) if (not reset) else None
            new_series[d.key] = {}
            for mk in (
                "total_power",
                "a_voltage",
                "b_voltage",
                "c_voltage",
                "a_current",
                "b_current",
                "c_current",
            ):
                if prev is not None and mk in prev:
                    try:
                        arr = list(prev[mk])[-max_points:]
                    except Exception:
                        arr = []
                else:
                    arr = []
                new_series[d.key][mk] = deque(arr, maxlen=max_points)

        self._live_series = new_series
        self._live_max_points = max_points

    def _live_drain(self) -> None:
        if not self._live_pollers:
            return
        for p in self._live_pollers:
            while True:
                try:
                    s: LiveSample = p.samples.get_nowait()
                except queue.Empty:
                    break
                # Diagnostics: mark device as online
                try:
                    d = self._live_diag.setdefault(s.device_key, {})
                    d["last_ok_ts"] = int(s.ts)
                    d["err_count"] = 0
                    d["last_err"] = None
                except Exception:
                    pass
                series = self._live_series.get(s.device_key)
                if series is None:
                    continue
                series["total_power"].append((s.ts, float(s.power_w.get("total", 0.0))))
                series["a_voltage"].append((s.ts, float(s.voltage_v.get("a", 0.0))))
                series["b_voltage"].append((s.ts, float(s.voltage_v.get("b", 0.0))))
                series["c_voltage"].append((s.ts, float(s.voltage_v.get("c", 0.0))))
                series["a_current"].append((s.ts, float(s.current_a.get("a", 0.0))))
                series["b_current"].append((s.ts, float(s.current_a.get("b", 0.0))))
                series["c_current"].append((s.ts, float(s.current_a.get("c", 0.0))))
                
                # Evaluate alert rules on incoming live samples
                try:
                    self._alerts_process_sample(s)
                except Exception:
                    pass

# Update realtime value cards (only for currently visible devices)
                # Skip UI updates when frozen (data collection continues).
                try:
                    if self._live_frozen.get():
                        vars_ = None
                    else:
                        vars_ = getattr(self, "_live_latest_vars", {}).get(s.device_key)
                    if vars_:
                        pw = float(s.power_w.get('total', 0.0))
                        va = float(s.voltage_v.get('a', 0.0)); vb = float(s.voltage_v.get('b', 0.0)); vc = float(s.voltage_v.get('c', 0.0))
                        ia = float(s.current_a.get('a', 0.0)); ib = float(s.current_a.get('b', 0.0)); ic = float(s.current_a.get('c', 0.0))
                        stamp = datetime.fromtimestamp(s.ts).strftime("%H:%M:%S")

                        dev_cfg = next((d for d in self.cfg.devices if d.key == s.device_key), None)
                        ph = int(getattr(dev_cfg, "phases", 3) or 3) if dev_cfg is not None else 3
                        if ph <= 1:
                            volt_txt = f"L1 {va:.1f} V"
                            curr_txt = f"L1 {ia:.2f} A"
                        else:
                            volt_txt = f"L1 {va:.1f} V  L2 {vb:.1f} V  L3 {vc:.1f} V"
                            curr_txt = f"L1 {ia:.2f} A  L2 {ib:.2f} A  L3 {ic:.2f} A"

                        vars_["power"].set(f"{pw:.0f} W")
                        vars_["voltage"].set(volt_txt)
                        vars_["current"].set(curr_txt)
                        vars_["stamp"].set(stamp)
                        # Switch on/off (if this device is a switch)
                        try:
                            dev = next((d for d in self.cfg.devices if d.key == s.device_key), None)
                            if dev is not None and str(getattr(dev, "kind", "")) == "switch":
                                on = self._extract_switch_on(s.raw)
                                v = getattr(self, "_live_switch_vars", {}).get(s.device_key)
                                if v is not None and on is not None:
                                    v.set(self.t("live.switch.on") if on else self.t("live.switch.off"))
                        except Exception:
                            pass
                        # Today's kWh (imported baseline + live delta)
                        total_kwh = None
                        try:
                            total_kwh = self._accumulate_live_kwh_today(s.device_key, int(s.ts), pw)
                            vars_["kwh_today"].set(f"{float(total_kwh):.3f} kWh")
                        except Exception:
                            pass

                        # Compact summary lines for the Live tab UI
                        try:
                            if total_kwh is None:
                                line0 = f"{self.t('live.cards.power')}: {pw:.0f} W   {self.t('live.cards.updated')}: {stamp}"
                            else:
                                line0 = f"{self.t('live.cards.power')}: {pw:.0f} W   {self.t('live.cards.kwh_today')}: {float(total_kwh):.3f} kWh   {self.t('live.cards.updated')}: {stamp}"
                            line1 = f"{self.t('live.cards.voltage')}: {volt_txt}   {self.t('live.cards.current')}: {curr_txt}"

                            # VAR + cos φ
                            try:
                                dev_cfg2 = next((d for d in self.cfg.devices if d.key == s.device_key), None)
                                ph2 = int(getattr(dev_cfg2, "phases", 3) or 3) if dev_cfg2 is not None else 3
                            except Exception:
                                ph2 = 3
                            try:
                                q_total = float(getattr(s, "reactive_var", {}).get("total", 0.0))
                                qa2 = float(getattr(s, "reactive_var", {}).get("a", 0.0))
                                qb2 = float(getattr(s, "reactive_var", {}).get("b", 0.0))
                                qc2 = float(getattr(s, "reactive_var", {}).get("c", 0.0))
                            except Exception:
                                q_total, qa2, qb2, qc2 = 0.0, 0.0, 0.0, 0.0
                            try:
                                pf_total = float(getattr(s, "cosphi", {}).get("total", 0.0))
                                pfa2 = float(getattr(s, "cosphi", {}).get("a", 0.0))
                                pfb2 = float(getattr(s, "cosphi", {}).get("b", 0.0))
                                pfc2 = float(getattr(s, "cosphi", {}).get("c", 0.0))
                            except Exception:
                                pf_total, pfa2, pfb2, pfc2 = 0.0, 0.0, 0.0, 0.0

                            if ph2 <= 1:
                                q_txt = f"{q_total:.0f} VAR"
                                pf_txt = f"{pf_total:.3f}"
                            else:
                                q_txt = f"{q_total:.0f} VAR ({qa2:.0f}/{qb2:.0f}/{qc2:.0f})"
                                pf_txt = f"{pf_total:.3f} ({pfa2:.3f}/{pfb2:.3f}/{pfc2:.3f})"
                            line2 = f"{self.t('web.kv.var')}: {q_txt}   {self.t('web.kv.cosphi')}: {pf_txt}"

                            if 'line0' in vars_:
                                vars_['line0'].set(line0)
                            if 'line1' in vars_:
                                vars_['line1'].set(line1)
                            if 'line2' in vars_:
                                vars_['line2'].set(line2)
                        except Exception:
                            pass
                except Exception:
                    pass
                # Update web dashboard store (if enabled)
                try:
                    # Ensure kWh is computed even if this device is not currently visible in the desktop UI
                    try:
                        total_kwh = self._accumulate_live_kwh_today(s.device_key, int(s.ts), float(s.power_w.get('total', 0.0)))
                    except Exception:
                        total_kwh = self._get_today_kwh_total(s.device_key)
                    self._live_state_store.update(
                        s.device_key,
                        LivePoint(
                            ts=int(s.ts),
                            power_total_w=float(s.power_w.get("total", 0.0)),
                            va=float(s.voltage_v.get("a", 0.0)),
                            vb=float(s.voltage_v.get("b", 0.0)),
                            vc=float(s.voltage_v.get("c", 0.0)),
                            ia=float(s.current_a.get("a", 0.0)),
                            ib=float(s.current_a.get("b", 0.0)),
                            ic=float(s.current_a.get("c", 0.0)),
                            q_total_var=float(getattr(s, "reactive_var", {}).get("total", 0.0)),
                            qa=float(getattr(s, "reactive_var", {}).get("a", 0.0)),
                            qb=float(getattr(s, "reactive_var", {}).get("b", 0.0)),
                            qc=float(getattr(s, "reactive_var", {}).get("c", 0.0)),
                            cosphi_total=float(getattr(s, "cosphi", {}).get("total", 0.0)),
                            pfa=float(getattr(s, "cosphi", {}).get("a", 0.0)),
                            pfb=float(getattr(s, "cosphi", {}).get("b", 0.0)),
                            pfc=float(getattr(s, "cosphi", {}).get("c", 0.0)),
                            kwh_today=float(total_kwh),
                        ),
                    )
                except Exception:
                    pass
            while True:
                try:
                    err = p.errors.get_nowait()
                except queue.Empty:
                    break
                # MultiLivePoller emits dicts; legacy poller emits strings.
                try:
                    if isinstance(err, dict):
                        dk = str(err.get("device_key") or "")
                        msg = str(err.get("error") or "")
                    else:
                        dk = ""
                        msg = str(err)
                except Exception:
                    dk = ""
                    msg = str(err)

                # Diagnostics: count errors so we can show Offline if needed
                try:
                    if not dk:
                        # fall back to "Name: msg" format
                        dk = msg.split(":", 1)[0].strip() if ":" in msg else msg
                    d = self._live_diag.setdefault(str(dk), {})
                    d["last_err_ts"] = int(time.time())
                    d["err_count"] = int(d.get("err_count", 0)) + 1
                    d["last_err"] = str(msg)
                except Exception:
                    pass
                try:
                    logging.getLogger(__name__).warning("Live error: %s", msg)
                except Exception:
                    pass
                try:
                    self.live_status.set(f"{msg}")
                except Exception:
                    pass

        # Offline detection: if no successful samples arrived recently, show OFFLINE in the card.
        try:
            now_ts = int(time.time())
            poll_s = float(getattr(self.cfg.ui, "live_poll_seconds", 1.0) or 1.0)
            stale_s = int(max(10.0, poll_s * 6.0))
            for dev_key, vars_ in getattr(self, "_live_latest_vars", {}).items():
                if not isinstance(vars_, dict):
                    continue
                diag = self._live_diag.get(dev_key) or {}
                last_ok = diag.get("last_ok_ts")
                if last_ok is None:
                    continue
                if (now_ts - int(last_ok)) >= stale_s:
                    stamp = datetime.fromtimestamp(int(last_ok)).strftime("%H:%M:%S")
                    try:
                        if "line0" in vars_:
                            vars_["line0"].set(f"OFFLINE – letzter OK: {stamp}")
                    except Exception:
                        pass
        except Exception:
            pass
        # Redraw at most every plot_redraw_seconds (skip when frozen).
        if not self._live_frozen.get():
            now = time.time()
            if now - self._live_last_redraw >= float(self.cfg.ui.plot_redraw_seconds):
                self._live_last_redraw = now
                self._redraw_live_plots()

    def _redraw_live_plots(self) -> None:
        """Redraw live plots (desktop view).

        Important: we plot using *datetime* values on the x-axis (not strings),
        otherwise HH:MM labels would collapse into repeated categorical values
        and the plot looks "broken".
        """
        try:
            win_m = int(self.cfg.ui.live_window_minutes)
        except Exception:
            win_m = 10

        # Optional smoothing (rolling mean over a time window).
        try:
            smooth_on = bool(getattr(self.cfg.ui, "live_smoothing_enabled", False))
            smooth_sec = int(getattr(self.cfg.ui, "live_smoothing_seconds", 10))
        except Exception:
            smooth_on = False
            smooth_sec = 0

        def _maybe_smooth(xs_dt, ys_list):
            """Optionale Glaettung fuer Live-Plots.

            Performance-Hinweis: pandas.rolling() in jedem Redraw ist auf macOS/HiDPI zu teuer
            und kann das UI ausbremsen. Wir nutzen daher einen schnellen Moving-Average ueber
            die letzten N Punkte (N aus smooth_sec / poll_seconds).
            """
            if (not smooth_on) or (smooth_sec <= 0) or (not ys_list) or (len(ys_list) < 3):
                return ys_list
            try:
                poll_s = float(getattr(self.cfg.ui, 'live_poll_seconds', 1.0) or 1.0)
                n = int(max(1, round(float(smooth_sec) / max(0.05, poll_s))))
            except Exception:
                n = int(max(1, smooth_sec))
            if n <= 1:
                return ys_list
            try:
                from collections import deque
                q = deque()
                s = 0.0
                out = []
                for y in ys_list:
                    try:
                        yv = float(y)
                    except Exception:
                        yv = 0.0
                    q.append(yv)
                    s += yv
                    if len(q) > n:
                        s -= q.popleft()
                    out.append(s / float(len(q)))
                return out
            except Exception:
                return ys_list

        def _slice_live(arr: List[Tuple[int, float]]) -> List[Tuple[int, float]]:
            if not arr:
                return arr
            try:
                newest = int(arr[-1][0])
            except Exception:
                return arr
            cutoff = newest - int(max(1, win_m)) * 60
            if cutoff <= 0:
                return arr
            if int(arr[0][0]) >= cutoff:
                return arr
            return [(t, v) for (t, v) in arr if int(t) >= cutoff]


        # Only redraw the currently visible two devices (selected via the page combobox).
        for d in self._get_visible_devices():
            metrics = self._live_series.get(d.key)
            if not metrics:
                continue

            # NOTE: FigureCanvasTkAgg already resizes the underlying figure
            # on <Configure> events of its Tk canvas. Doing our own size
            # estimation here can under/over-shoot on macOS HiDPI and results
            # in a small bitmap drawn into a larger widget (lots of white space).
            # We therefore rely on TkAgg's native resize handling and only
            # redraw the data/layout.

            # Power
            ax_p = self._live_axes.get(d.key, {}).get("power")
            canvas_p = self._live_canvases.get(d.key, {}).get("power")
            if ax_p is not None and canvas_p is not None:
                arr = _slice_live(metrics.get("total_power", []))
                ax_p.clear()
                ax_p.set_ylabel("W")
                ax_p.set_xlabel(self.t('live.time'))
                if arr:
                    xs = [datetime.fromtimestamp(t) for t, _ in arr]
                    ys = [v for _, v in arr]
                    ys = _maybe_smooth(xs, ys)
                    ax_p.plot(xs, ys)
                ax_p.grid(True, axis="y", alpha=0.3)
                self._configure_time_axis(ax_p, canvas_p.get_tk_widget(), win_m)
                self._apply_axis_layout(self._live_figs[d.key]["power"], ax_p, canvas_p.get_tk_widget(), legend=False)
                canvas_p.draw_idle()

            # Voltage (L1/L2/L3 in one plot)

            ax_v = self._live_axes.get(d.key, {}).get("voltage")
            canvas_v = self._live_canvases.get(d.key, {}).get("voltage")
            if ax_v is not None and canvas_v is not None:
                ax_v.clear()
                ax_v.set_ylabel("V")
                ax_v.set_xlabel(self.t('live.time'))
                n_series = 0
                ph = int(getattr(d, "phases", 3) or 3)
                phase_keys = [("a_voltage", "L1")] if ph <= 1 else [("a_voltage", "L1"), ("b_voltage", "L2"), ("c_voltage", "L3")]
                for key, label in phase_keys:
                    arr = _slice_live(metrics.get(key, []))
                    if not arr:
                        continue
                    xs = [datetime.fromtimestamp(t) for t, _ in arr]
                    ys = [v for _, v in arr]
                    ys = _maybe_smooth(xs, ys)
                    ax_v.plot(xs, ys, label=label)
                    n_series += 1
                if n_series > 1:
                    try:
                        base = self._font_base_for_widget(canvas_v.get_tk_widget())
                        ax_v.legend(loc="upper right", fontsize=max(7, min(14, base + 1)))
                    except Exception:
                        try:
                            ax_v.legend(loc="upper right")
                        except Exception:
                            pass
                ax_v.grid(True, axis="y", alpha=0.3)
                self._configure_time_axis(ax_v, canvas_v.get_tk_widget(), win_m)
                self._apply_axis_layout(self._live_figs[d.key]["voltage"], ax_v, canvas_v.get_tk_widget(), legend=(n_series > 1))
                canvas_v.draw_idle()

            # Current (L1/L2/L3 in one plot)

            ax_c = self._live_axes.get(d.key, {}).get("current")
            canvas_c = self._live_canvases.get(d.key, {}).get("current")
            if ax_c is not None and canvas_c is not None:
                ax_c.clear()
                ax_c.set_ylabel("A")
                ax_c.set_xlabel(self.t('live.time'))
                n_series = 0
                ph = int(getattr(d, "phases", 3) or 3)
                phase_keys = [("a_current", "L1")] if ph <= 1 else [("a_current", "L1"), ("b_current", "L2"), ("c_current", "L3")]
                for key, label in phase_keys:
                    arr = _slice_live(metrics.get(key, []))
                    if not arr:
                        continue
                    xs = [datetime.fromtimestamp(t) for t, _ in arr]
                    ys = [v for _, v in arr]
                    ys = _maybe_smooth(xs, ys)
                    ax_c.plot(xs, ys, label=label)
                    n_series += 1
                if n_series > 1:
                    try:
                        base = self._font_base_for_widget(canvas_c.get_tk_widget())
                        ax_c.legend(loc="upper right", fontsize=max(7, min(14, base + 1)))
                    except Exception:
                        try:
                            ax_c.legend(loc="upper right")
                        except Exception:
                            pass
                ax_c.grid(True, axis="y", alpha=0.3)
                self._configure_time_axis(ax_c, canvas_c.get_tk_widget(), win_m)
                self._apply_axis_layout(self._live_figs[d.key]["current"], ax_c, canvas_c.get_tk_widget(), legend=(n_series > 1))
                canvas_c.draw_idle()

    def _configure_time_axis(self, ax, widget: tk.Widget, window_minutes: int) -> None:

        """Configure x-axis for datetime plots in a robust way.


        Avoids AutoDateLocator warnings by using explicit locators based on the

        requested window length.

        """

        try:

            win_m = int(window_minutes)

        except Exception:

            win_m = 10

        max_labels = self._max_xlabels_for_widget(widget)

        try:

            if win_m <= 2:

                interval = max(1, int(round((win_m * 60) / max_labels)))

                locator = mdates.SecondLocator(interval=interval)

                formatter = mdates.DateFormatter("%H:%M:%S")

            elif win_m < 6 * 60:

                interval = max(1, int(round(win_m / max_labels)))

                locator = mdates.MinuteLocator(interval=interval)

                formatter = mdates.DateFormatter("%H:%M")

            elif win_m < 48 * 60:

                interval = max(1, int(round((win_m / 60) / max_labels)))

                locator = mdates.HourLocator(interval=interval)

                formatter = mdates.DateFormatter("%d.%m %H:%M")

            else:

                interval = max(1, int(round((win_m / 60 / 24) / max_labels)))

                locator = mdates.DayLocator(interval=interval)

                formatter = mdates.DateFormatter("%d.%m")

            ax.xaxis.set_major_locator(locator)

            ax.xaxis.set_major_formatter(formatter)

        except Exception:

            pass

        base = self._font_base_for_widget(widget)

        ax.tick_params(axis="x", labelrotation=30, labelsize=min(30, base + 4))


    def _build_export_tab(self) -> None:
        frm = self.tab_export
        top = ttk.Frame(frm)
        top.pack(fill="x", padx=12, pady=10)
        self.export_dir = tk.StringVar(value=str(Path.cwd() / "exports"))
        ttk.Label(top, text=self.t("export.dir")).pack(side="left")
        ttk.Entry(top, textvariable=self.export_dir, width=60).pack(side="left", padx=6)
        ttk.Button(top, text="…", command=self._choose_export_dir).pack(side="left")
        mid = ttk.Frame(frm)
        mid.pack(fill="x", padx=12, pady=8)
        ttk.Label(mid, text=self.t("export.range")).pack(side="left")
        self.export_start = tk.StringVar(value="")
        self.export_end = tk.StringVar(value="")
        ttk.Label(mid, text=self.t("common.from")).pack(side="left", padx=(10, 4))
        ttk.Entry(mid, textvariable=self.export_start, width=12).pack(side="left")
        ttk.Label(mid, text=self.t("common.to")).pack(side="left", padx=(10, 4))
        ttk.Entry(mid, textvariable=self.export_end, width=12).pack(side="left")
        inv = ttk.Frame(frm)
        inv.pack(fill="x", padx=12, pady=(0, 8))
        ttk.Label(inv, text=self.t("export.invoice_period")).pack(side="left")

        # Keep the config key in invoice_period_var, but show a translated label in the combobox.
        self.invoice_period_var = tk.StringVar(value="month")
        self.invoice_period_label_var = tk.StringVar(value=self.t("period.month"))
        _period_keys = ["custom", "day", "week", "month", "year"]
        _period_labels = [self.t(f"period.{k}") for k in _period_keys]
        cb = ttk.Combobox(
            inv,
            values=_period_labels,
            width=12,
            textvariable=self.invoice_period_label_var,
            state="readonly",
        )
        cb.pack(side="left", padx=6)

        def _on_period_selected(_evt=None):
            try:
                idx = _period_labels.index(self.invoice_period_label_var.get())
            except Exception:
                idx = 0
            self.invoice_period_var.set(_period_keys[idx])

        cb.bind("<<ComboboxSelected>>", _on_period_selected)
        _on_period_selected()

        ttk.Label(inv, text=self.t("export.anchor")).pack(side="left", padx=(12, 4))
        self.invoice_anchor_var = tk.StringVar(value="")
        ttk.Entry(inv, textvariable=self.invoice_anchor_var, width=12).pack(side="left")
        ttk.Label(inv, text=self.t("common.date_hint")).pack(side="left", padx=(8, 0))
        btns = ttk.Frame(frm)
        btns.pack(fill="x", padx=12, pady=10)
        ttk.Button(btns, text=self.t("export.btn.excel"), command=self._export_excel).pack(side="left")
        ttk.Button(btns, text=self.t("export.btn.pdf_summary"), command=self._export_pdf_summary_with_plots).pack(
            side="left", padx=8
        )
        ttk.Button(btns, text=self.t("export.btn.invoices"), command=self._export_invoices).pack(side="left", padx=8)
        ttk.Button(btns, text=self.t("export.btn.plots"), command=self._export_plots).pack(side="left", padx=8)

        # Etappe 6: Reports + Current View Export
        btns2 = ttk.Frame(frm)
        btns2.pack(fill="x", padx=12, pady=(0, 10))
        ttk.Button(btns2, text=self.t("export.btn.day_report_v1"), command=self._export_day_report_pdf).pack(side="left")
        ttk.Button(btns2, text=self.t("export.btn.month_report_v1"), command=self._export_month_report_pdf).pack(side="left", padx=8)
        ttk.Button(btns2, text=self.t("export.btn.current_png"), command=self._export_current_view_png).pack(side="left", padx=8)
        ttk.Button(btns2, text=self.t("export.btn.current_pdf"), command=self._export_current_view_pdf).pack(side="left", padx=8)

        self.export_log = tk.Text(frm, height=16)
        self.export_log.pack(fill="both", expand=True, padx=12, pady=(0, 12))
    def _choose_export_dir(self) -> None:
        d = filedialog.askdirectory(initialdir=self.export_dir.get() or str(Path.cwd()))
        if d:
            self.export_dir.set(d)
    def _parse_export_range(self) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
        s = self.export_start.get().strip() or None
        e = self.export_end.get().strip() or None
        start = pd.to_datetime(s) if s else None
        end = pd.to_datetime(e) if e else None
        return start, end
    def _parse_plots_range(self) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
        """Return the optional plot range from the Plots tab entries.
        Accepts YYYY-MM-DD or DD.MM.YYYY (and other pandas-parseable formats).
        If the user enters only a date for 'bis', interpret it as end-of-day.
        """
        s_raw = (self._plots_start.get() if hasattr(self, "_plots_start") else "").strip()
        e_raw = (self._plots_end.get() if hasattr(self, "_plots_end") else "").strip()
        start = _parse_date_flexible(s_raw)
        end = _parse_date_flexible(e_raw)
        def _looks_like_date_only(x: str) -> bool:
            x = (x or "").strip()
            return (":" not in x) and (len(x) <= 10)
        if end is not None and _looks_like_date_only(e_raw):
            # move to end of day (inclusive)
            end = end.normalize() + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
        if start is not None and _looks_like_date_only(s_raw):
            start = start.normalize()
        return start, end

    def _normalize_export_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return a dataframe with stable L1/L2/L3 export columns, regardless of input."""
        if df is None or df.empty:
            return pd.DataFrame(
                columns=[
                    "timestamp",
                    "W_L1","W_L2","W_L3","W_total",
                    "V_L1","V_L2","V_L3","V_avg",
                    "A_L1","A_L2","A_L3","A_total",
                    "energy_kwh","calc_method",
                ]
            )

        def pick(cands: Sequence[str]) -> pd.Series:
            for c in cands:
                if c in df.columns:
                    return pd.to_numeric(df[c], errors="coerce")
            return pd.Series([pd.NA]*len(df), index=df.index, dtype="Float64")

        # Power phase candidates (Shelly 3EM / legacy)
        W1 = pick(["a_act_power","act_power_a","a_power","power_a","a_avg_act_power","avg_act_power_a","a_avg_power","avg_power_a","a_active_power","active_power_a"])
        W2 = pick(["b_act_power","act_power_b","b_power","power_b","b_avg_act_power","avg_act_power_b","b_avg_power","avg_power_b","b_active_power","active_power_b"])
        W3 = pick(["c_act_power","act_power_c","c_power","power_c","c_avg_act_power","avg_act_power_c","c_avg_power","avg_power_c","c_active_power","active_power_c"])
        if "total_power" in df.columns:
            Wt = pd.to_numeric(df["total_power"], errors="coerce")
        else:
            Wt = (W1.fillna(0) + W2.fillna(0) + W3.fillna(0)).astype("float64")

        # Voltage phase candidates (Shelly/legacy: *_voltage or *_u)
        V1 = pick(["a_voltage","voltage_a","a_u","u_a","a_avg_voltage","avg_voltage_a"])
        V2 = pick(["b_voltage","voltage_b","b_u","u_b","b_avg_voltage","avg_voltage_b"])
        V3 = pick(["c_voltage","voltage_c","c_u","u_c","c_avg_voltage","avg_voltage_c"])
        if "avg_voltage" in df.columns:
            Vavg = pd.to_numeric(df["avg_voltage"], errors="coerce")
        else:
            Vavg = (V1 + V2 + V3) / 3.0

        # Current phase candidates (Shelly/legacy: *_current or *_i)
        A1 = pick(["a_current","current_a","a_i","i_a","a_avg_current","avg_current_a"])
        A2 = pick(["b_current","current_b","b_i","i_b","b_avg_current","avg_current_b"])
        A3 = pick(["c_current","current_c","c_i","i_c","c_avg_current","avg_current_c"])
        if "avg_current" in df.columns:
            At = pd.to_numeric(df["avg_current"], errors="coerce")
        else:
            At = (A1.fillna(0) + A2.fillna(0) + A3.fillna(0)).astype("float64")

        out = pd.DataFrame(
            {
                "timestamp": df["timestamp"] if "timestamp" in df.columns else pd.NaT,
                "W_L1": W1, "W_L2": W2, "W_L3": W3, "W_total": Wt,
                "V_L1": V1, "V_L2": V2, "V_L3": V3, "V_avg": Vavg,
                "A_L1": A1, "A_L2": A2, "A_L3": A3, "A_total": At,
                "energy_kwh": pd.to_numeric(df.get("energy_kwh", 0.0), errors="coerce"),
                "calc_method": df.get("calc_method", ""),
            }
        )
        return out

    def _export_excel(self) -> None:
        if not self._ensure_data_loaded():
            messagebox.showinfo(self.t("msg.export"), self.t("export.no_data"))
            return
        start, end = self._parse_export_range()
        out_dir = Path(self.export_dir.get())
        out_dir.mkdir(parents=True, exist_ok=True)
        sheets: Dict[str, pd.DataFrame] = {}
        for d in self.cfg.devices:
            df = self.computed[d.key].df.copy()
            df = filter_by_time(df, start=start, end=end)
            norm = self._normalize_export_df(df)
            sheets[f"{d.name}_export"] = norm[
                [
                    "timestamp",
                    "W_L1","W_L2","W_L3","W_total",
                    "V_L1","V_L2","V_L3","V_avg",
                    "A_L1","A_L2","A_L3","A_total",
                    "energy_kwh","calc_method",
                ]
            ]
        out = out_dir / f"shelly_export_{time.strftime('%Y%m%d_%H%M%S')}.xlsx"
        export_to_excel(sheets, out)
        self.export_log.insert("end", self.t("export.excel_written", path=str(out)) + "\n")
        self.export_log.see("end")

    def _export_pdf_summary_with_plots(self) -> None:
        if not self._ensure_data_loaded():
            messagebox.showinfo(self.t("msg.export"), self.t("export.no_data"))
            return
        start, end = self._parse_export_range()
        out_dir = Path(self.export_dir.get())
        out_dir.mkdir(parents=True, exist_ok=True)
        pricing = self.cfg.pricing
        unit_gross = float(pricing.unit_price_gross())
        totals: List[ReportTotals] = []
        for d in self.cfg.devices:
            df = filter_by_time(self.computed[d.key].df, start=start, end=end)
            kwh, avgp, maxp = summarize(df)
            totals.append(ReportTotals(name=d.name, kwh_total=kwh, cost_eur=kwh * unit_gross, avg_power_w=avgp, max_power_w=maxp))
        label = self.t("period.all")
        if start is not None or end is not None:
            label = f"{format_date_local(self.lang, start) if start is not None else '…'} {self.t('common.to')} {format_date_local(self.lang, end) if end is not None else '…'}"
        # Create plot images (days/weeks/months per Shelly) and embed them as pages
        plots_dir = out_dir / "plots"
        plots_dir.mkdir(parents=True, exist_ok=True)
        ts = time.strftime("%Y%m%d_%H%M%S")
        plot_pages: List[Tuple[str, Path]] = []
        for d in self.cfg.devices:
            cd = self.computed.get(d.key)
            if cd is None:
                continue
            # Apply export range to plot data as well
            df_f = filter_by_time(cd.df, start=start, end=end)
            tmp_cd = ComputedDevice(device_key=cd.device_key, device_name=cd.device_name, df=df_f)
            for mode in ("days", "weeks", "months"):
                fig = self._make_stats_figure(tmp_cd, mode)
                safe_name = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in d.name).strip("_")
                png = plots_dir / f"{safe_name or d.key}_{mode}_{ts}.png"
                export_figure_png(fig, png, dpi=180)
                plot_pages.append((f"{d.name} – {mode}", png))
        out = out_dir / f"shelly_summary_{ts}.pdf"
        vat_part = ""
        if pricing.vat_enabled:
            vat_part = f" ({self.t('pdf.vat')} {format_number_local(self.lang, pricing.vat_rate_percent, 1)}%)"
        note = self.t(
            "pdf.summary.note",
            price=format_number_local(self.lang, unit_gross, 4),
            vat_part=vat_part,
            version=__version__,
        )
        export_pdf_summary(
            title=self.t("pdf.summary.title"),
            period_label=label,
            totals=totals,
            out_path=out,
            note=note,
            plot_pages=plot_pages,
            lang=self.lang,
        )
        self.export_log.insert("end", self.t("export.pdf_summary_written", path=str(out)) + "\n")
        self.export_log.see("end")
    def _pricing_footer_note(self) -> str:
        """Return a translated pricing note for invoice PDFs."""
        try:
            pricing = self.cfg.pricing
        except Exception:
            return ""
        try:
            lang = self.lang
        except Exception:
            lang = "de"

        try:
            vat = float(getattr(pricing, "vat_rate_percent", 0.0))
        except Exception:
            vat = 0.0
        try:
            vat_enabled = bool(getattr(pricing, "vat_enabled", False))
        except Exception:
            vat_enabled = False
        try:
            price_includes_vat = bool(getattr(pricing, "price_includes_vat", True))
        except Exception:
            price_includes_vat = True

        try:
            net = float(pricing.unit_price_net())
        except Exception:
            try:
                net = float(getattr(pricing, "unit_price_eur_per_kwh", 0.0) or 0.0)
            except Exception:
                net = 0.0
        try:
            gross = float(pricing.unit_price_gross())
        except Exception:
            gross = net

        try:
            if not vat_enabled:
                return self.t("pdf.pricing.no_vat", price=_fmt_eur(net))
            if price_includes_vat:
                return self.t("pdf.pricing.gross_incl_vat", gross=_fmt_eur(gross), net=_fmt_eur(net), vat=f"{vat:.1f}")
            return self.t("pdf.pricing.net_excl_vat", net=_fmt_eur(net), gross=_fmt_eur(gross), vat=f"{vat:.1f}")
        except Exception:
            return ""

    def _export_invoices(self) -> None:
        if not self._ensure_data_loaded():
            messagebox.showinfo(self.t("msg.export"), self.t("export.no_data"))
            return
        period = str(self.invoice_period_var.get() if hasattr(self, "invoice_period_var") else "custom")
        start, end = self._parse_export_range()
        if period != "custom":
            anchor = _parse_date_flexible(self.invoice_anchor_var.get() if hasattr(self, "invoice_anchor_var") else "")
            # If no explicit anchor provided, fall back to export start, else today.
            if anchor is None and start is not None:
                anchor = start
            if anchor is None:
                anchor = pd.Timestamp(date.today())
            start, end = _period_bounds(anchor, period)
        out_dir = Path(self.export_dir.get())
        out_dir.mkdir(parents=True, exist_ok=True)
        inv_dir = out_dir / "invoices"
        inv_dir.mkdir(parents=True, exist_ok=True)
        pricing = self.cfg.pricing
        unit_net = float(pricing.unit_price_net())
        # Issue/due dates
        issue = date.today()
        due = issue + timedelta(days=int(self.cfg.billing.payment_terms_days))
        ts = time.strftime("%Y%m%d")
        for d in self.cfg.devices:
            df = filter_by_time(self.computed[d.key].df, start=start, end=end)
            kwh, _avgp, _maxp = summarize(df)
            # Human label and invoice number suffix
            if start is None and end is None:
                period_label = self.t("period.all")
                suffix = "all"
            else:
                period_label = f"{format_date_local(self.lang, start) if start is not None else '…'} {self.t('common.to')} {format_date_local(self.lang, end) if end is not None else '…'}"
                if period == "day" and start is not None:
                    suffix = start.strftime("%Y%m%d")
                elif period == "week" and start is not None:
                    iso = start.isocalendar()
                    suffix = f"W{iso.week:02d}{iso.year}"
                elif period == "month" and start is not None:
                    suffix = start.strftime("%Y%m")
                elif period == "year" and start is not None:
                    suffix = start.strftime("%Y")
                else:
                    suffix = f"{(start.date().isoformat() if start is not None else 'x')}-{(end.date().isoformat() if end is not None else 'y')}"
            invoice_no = f"{self.cfg.billing.invoice_prefix}-{ts}-{d.key}-{period}-{suffix}"
            safe_name = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in d.name).strip("_")
            out = inv_dir / f"invoice_{invoice_no}_{safe_name or d.key}.pdf"
            line = InvoiceLine(
                description=self.t("pdf.invoice.line_energy", device=d.name, period=period_label),
                quantity=float(kwh),
                unit="kWh",
                unit_price_net=unit_net,
            )
            lines = [line]
            try:
                base_year = float(getattr(pricing, 'base_fee_eur_per_year', 0.0))
            except Exception:
                base_year = 0.0
            if base_year > 0:
                if start is None and end is None:
                    if not df.empty:
                        s_eff = pd.Timestamp(df['timestamp'].min()).normalize()
                        e_eff = pd.Timestamp(df['timestamp'].max()).normalize()
                    else:
                        s_eff = pd.Timestamp(date.today()).normalize()
                        e_eff = s_eff
                else:
                    s_eff = pd.Timestamp(start).normalize() if start is not None else pd.Timestamp(df['timestamp'].min()).normalize()
                    e_eff = pd.Timestamp(end).normalize() if end is not None else pd.Timestamp(df['timestamp'].max()).normalize()
                days = int((e_eff.date() - s_eff.date()).days) + 1
                days = max(1, days)
                base_day_net = float(pricing.base_fee_day_net())
                lines.append(InvoiceLine(description=self.t("pdf.invoice.line_base_fee", days=days), quantity=float(days), unit=self.t("unit.days"), unit_price_net=base_day_net))
            export_pdf_invoice(
                out_path=out,
                invoice_no=invoice_no,
                issue_date=issue,
                due_date=due,
                issuer={
                    "name": self.cfg.billing.issuer.name,
                    "address_lines": self.cfg.billing.issuer.address_lines,
                    "vat_id": self.cfg.billing.issuer.vat_id,
                    "email": self.cfg.billing.issuer.email,
                    "phone": self.cfg.billing.issuer.phone,
                    "iban": self.cfg.billing.issuer.iban,
                    "bic": self.cfg.billing.issuer.bic,
                },
                customer={
                    "name": self.cfg.billing.customer.name,
                    "address_lines": self.cfg.billing.customer.address_lines,
                    "vat_id": self.cfg.billing.customer.vat_id,
                    "email": self.cfg.billing.customer.email,
                    "phone": self.cfg.billing.customer.phone,
                },
                vat_rate_percent=float(pricing.vat_rate_percent),
                vat_enabled=bool(pricing.vat_enabled),
                lines=lines,
                footer_note=(self._pricing_footer_note()),
                lang=self.lang,
            )
            self.export_log.insert("end", self.t("export.invoice_written", path=str(out)) + "\n")
        self.export_log.see("end")
    
    def _export_plots(self) -> None:
        if not self._ensure_data_loaded():
            messagebox.showinfo(self.t("msg.export"), self.t("export.no_data"))
            return
        # Use the time range from the Plots tab, if set.
        p_start, p_end = self._parse_plots_range()
        range_label = ""
        if p_start is not None or p_end is not None:
            a = p_start.date().isoformat() if p_start is not None else "…"
            b = p_end.date().isoformat() if p_end is not None else "…"
            range_label = f"_{a}_to_{b}"
        out_dir = Path(self.export_dir.get())
        out_dir.mkdir(parents=True, exist_ok=True)
        plots_dir = out_dir / "plots"
        plots_dir.mkdir(parents=True, exist_ok=True)
        ts = time.strftime("%Y%m%d_%H%M%S")
        pdf_path = plots_dir / f"shelly_plots{range_label}_{ts}.pdf"
        with PdfPages(pdf_path) as pdf:
            for d in self.cfg.devices:
                cd = self.computed.get(d.key)
                if cd is None:
                    continue
                df = filter_by_time(cd.df, start=p_start, end=p_end)
                cd_f = ComputedDevice(device_key=cd.device_key, device_name=cd.device_name, df=df)
                for mode in ("days", "weeks", "months"):
                    fig = self._make_stats_figure(cd_f, mode)
                    safe_name = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in d.name).strip("_")
                    png = plots_dir / f"{safe_name or d.key}_{mode}{range_label}_{ts}.png"
                    fig.savefig(png, dpi=180, bbox_inches="tight")
                    pdf.savefig(fig)
                    self.export_log.insert("end", f"Plot PNG: {png}\n")
        self.export_log.insert("end", f"Plots PDF: {pdf_path}\n")
        self.export_log.see("end")
    # ---------------- Settings tab ----------------
    def _build_settings_tab(self) -> None:
        frm = self.tab_settings
        # Split settings into subtabs to avoid overly tall pages (esp. on 14" screens).
        nb = ttk.Notebook(frm)
        nb.pack(fill="both", expand=True, padx=12, pady=10)

        tab_devices = ttk.Frame(nb)
        tab_main = ttk.Frame(nb)
        tab_advanced = ttk.Frame(nb)
        tab_expert = ttk.Frame(nb)
        tab_billing = ttk.Frame(nb)
        nb.add(tab_devices, text=self.t('settings.devices'))
        nb.add(tab_main, text=self.t('settings.main'))
        nb.add(tab_advanced, text=self.t('settings.advanced'))
        nb.add(tab_expert, text=self.t('settings.expert'))
        nb.add(tab_billing, text=self.t('tabs.billing'))

        # ---------------- Geräte ----------------

        # Scrollbar for small screens (Devices subtab can get very tall)
        devices_outer = ttk.Frame(tab_devices)
        devices_outer.pack(fill="both", expand=True)

        devices_canvas = tk.Canvas(devices_outer, highlightthickness=0)
        devices_vsb = ttk.Scrollbar(devices_outer, orient="vertical", command=devices_canvas.yview)
        devices_canvas.configure(yscrollcommand=devices_vsb.set)

        devices_vsb.pack(side="right", fill="y")
        devices_canvas.pack(side="left", fill="both", expand=True)

        devices_inner = ttk.Frame(devices_canvas)
        _devices_win = devices_canvas.create_window((0, 0), window=devices_inner, anchor="nw")

        def _devices_on_inner_config(_e=None):
            try:
                devices_canvas.configure(scrollregion=devices_canvas.bbox("all"))
            except Exception:
                pass

        def _devices_on_canvas_config(e):
            try:
                devices_canvas.itemconfigure(_devices_win, width=e.width)
            except Exception:
                pass

        devices_inner.bind("<Configure>", _devices_on_inner_config)
        devices_canvas.bind("<Configure>", _devices_on_canvas_config)

        # Mouse wheel scrolling (Windows/macOS) + Linux buttons
        def _devices_on_mousewheel(e):
            try:
                # On Windows: delta is multiple of 120, on macOS smaller
                delta = int(-1 * (e.delta / 120)) if abs(getattr(e, "delta", 0)) >= 120 else (-1 if e.delta > 0 else 1)
                devices_canvas.yview_scroll(delta, "units")
            except Exception:
                pass

        def _devices_on_button4(_e):
            try:
                devices_canvas.yview_scroll(-1, "units")
            except Exception:
                pass

        def _devices_on_button5(_e):
            try:
                devices_canvas.yview_scroll(1, "units")
            except Exception:
                pass

        def _devices_bind_wheel(_w):
            _w.bind("<Enter>", lambda _e: devices_canvas.bind_all("<MouseWheel>", _devices_on_mousewheel))
            _w.bind("<Leave>", lambda _e: devices_canvas.unbind_all("<MouseWheel>"))
            _w.bind("<Enter>", lambda _e: devices_canvas.bind_all("<Button-4>", _devices_on_button4), add="+")
            _w.bind("<Enter>", lambda _e: devices_canvas.bind_all("<Button-5>", _devices_on_button5), add="+")
            _w.bind("<Leave>", lambda _e: devices_canvas.unbind_all("<Button-4>"), add="+")
            _w.bind("<Leave>", lambda _e: devices_canvas.unbind_all("<Button-5>"), add="+")

        _devices_bind_wheel(devices_canvas)
        _devices_bind_wheel(devices_inner)

        # Build the actual UI into this inner frame
        td = devices_inner

        ttk.Label(td, text=self.t('settings.devices')).pack(anchor="w", pady=(0, 6))
        table = ttk.Frame(td)
        table.pack(fill="x")
        ttk.Label(table, text=self.t('settings.devices.key'), width=12).grid(row=0, column=0, sticky="w")
        ttk.Label(table, text=self.t('settings.devices.name'), width=20).grid(row=0, column=1, sticky="w")
        ttk.Label(table, text=self.t('settings.devices.host'), width=20).grid(row=0, column=2, sticky="w")
        ttk.Label(table, text=self.t('settings.devices.emid'), width=6).grid(row=0, column=3, sticky="w")
        ttk.Label(table, text=self.t('settings.devices.model'), width=22).grid(row=0, column=4, sticky="w")
        ttk.Label(table, text=self.t('settings.devices.kind'), width=10).grid(row=0, column=5, sticky="w")

        # Preserve entered device rows when rebuilding this tab (e.g. after "Hinzufügen").
        if not getattr(self, "_dev_vars", None):
            self._dev_vars = []
            for d in self.cfg.devices:
                self._dev_vars.append(
                    (
                        tk.StringVar(value=d.key),
                        tk.StringVar(value=d.name),
                        tk.StringVar(value=d.host),
                        tk.StringVar(value=str(getattr(d, 'em_id', 0))),
                        tk.StringVar(value=str(getattr(d, 'model', '') or '')),
                        tk.StringVar(value=str(getattr(d, 'kind', 'em') or 'em')),
                        tk.StringVar(value=str(getattr(d, 'gen', 0) or 0)),
                        tk.StringVar(value=str(getattr(d, 'phases', 3) or 3)),
                        tk.BooleanVar(value=bool(getattr(d, 'supports_emdata', True))),
                    )
                )

        for i, row in enumerate(self._dev_vars, start=1):
            v_key, v_name, v_host, v_emid, v_model, v_kind, v_gen, v_phases, v_emdata = row
            ttk.Entry(table, textvariable=v_key, width=12).grid(row=i, column=0, padx=(0, 6), pady=2, sticky="w")
            ttk.Entry(table, textvariable=v_name, width=20).grid(row=i, column=1, padx=(0, 6), pady=2, sticky="w")
            ttk.Entry(table, textvariable=v_host, width=20).grid(row=i, column=2, padx=(0, 6), pady=2, sticky="w")
            ttk.Entry(table, textvariable=v_emid, width=6).grid(row=i, column=3, padx=(0, 6), pady=2, sticky="w")
            ttk.Entry(table, textvariable=v_model, width=22, state='readonly').grid(row=i, column=4, padx=(0, 6), pady=2, sticky="w")
            ttk.Entry(table, textvariable=v_kind, width=10, state='readonly').grid(row=i, column=5, padx=(0, 6), pady=2, sticky="w")

        btnrow = ttk.Frame(td)
        btnrow.pack(fill="x", pady=(8, 0))
        ttk.Button(btnrow, text=self.t('settings.device.add'), command=self._add_device_row).pack(side="left")
        ttk.Button(btnrow, text=self.t('settings.device.add_auto'), command=self._add_device_by_ip_dialog).pack(side="left", padx=8)
        ttk.Button(btnrow, text=self.t('settings.device.check_all'), command=self._probe_all_devices_async).pack(side="left")


        # ---------------- mDNS (Shelly Auto-Discovery) ----------------
        try:
            ttk.Separator(td, orient="horizontal").pack(fill="x", pady=(12, 8))
            ttk.Label(td, text=self.t('settings.mdns.title')).pack(anchor="w", pady=(0, 6))

            mdns_wrap = ttk.Frame(td)
            mdns_wrap.pack(fill="x", pady=(0, 6))

            if not getattr(self, "_mdns_status_var", None):
                self._mdns_status_var = tk.StringVar(value=self.t('settings.mdns.hint'))
            ttk.Label(mdns_wrap, textvariable=self._mdns_status_var).pack(anchor="w")

            mdns_btns = ttk.Frame(mdns_wrap)
            mdns_btns.pack(fill="x", pady=(6, 4))
            ttk.Button(mdns_btns, text=self.t('settings.mdns.scan'), command=self._mdns_scan_async).pack(side="left")
            ttk.Button(mdns_btns, text=self.t('settings.mdns.add_selected'), command=self._mdns_add_selected).pack(side="left", padx=8)

            cols = ("name", "host", "model", "gen", "service")
            tree = ttk.Treeview(mdns_wrap, columns=cols, show="headings", height=7)
            tree.heading("name", text=self.t('settings.mdns.col_name'))
            tree.heading("host", text=self.t('settings.mdns.col_host'))
            tree.heading("model", text=self.t('settings.mdns.col_model'))
            tree.heading("gen", text=self.t('settings.mdns.col_gen'))
            tree.heading("service", text=self.t('settings.mdns.col_service'))

            tree.column("name", width=180, anchor="w")
            tree.column("host", width=140, anchor="w")
            tree.column("model", width=140, anchor="w")
            tree.column("gen", width=50, anchor="center")
            tree.column("service", width=120, anchor="w")
            tree.pack(fill="x")

            self._mdns_tree = tree
            self._mdns_refresh_tree()
        except Exception:
            pass

        
        # --- Health check / Network diagnostics ---
        health_wrap = ttk.Labelframe(td, text=self.t('settings.health.title'))
        health_wrap.pack(fill="x", pady=(8, 6))
        if not getattr(self, "_health_status_var", None):
            self._health_status_var = tk.StringVar(value=self.t('settings.health.hint'))
        ttk.Label(health_wrap, textvariable=self._health_status_var).pack(anchor="w")

        hb = ttk.Frame(health_wrap)
        hb.pack(fill="x", pady=(6, 4))
        ttk.Button(hb, text=self.t('settings.health.run'), command=self._health_check_async).pack(side="left")
        ttk.Button(hb, text=self.t('settings.health.copy'), command=self._health_copy_to_clipboard).pack(side="left", padx=8)

        hcols = ("device", "host", "tcp_ms", "http_ms", "model", "fw", "last_ok", "err_count", "last_err")
        htree = ttk.Treeview(health_wrap, columns=hcols, show="headings", height=6)
        htree.heading("device", text=self.t('settings.health.col_device'))
        htree.heading("host", text=self.t('settings.health.col_host'))
        htree.heading("tcp_ms", text=self.t('settings.health.col_tcp'))
        htree.heading("http_ms", text=self.t('settings.health.col_http'))
        htree.heading("model", text=self.t('settings.health.col_model'))
        htree.heading("fw", text=self.t('settings.health.col_fw'))
        htree.heading("last_ok", text=self.t('settings.health.col_last_ok'))
        htree.heading("err_count", text=self.t('settings.health.col_err_count'))
        htree.heading("last_err", text=self.t('settings.health.col_last_err'))

        htree.column("device", width=140, anchor="w")
        htree.column("host", width=120, anchor="w")
        htree.column("tcp_ms", width=70, anchor="e")
        htree.column("http_ms", width=70, anchor="e")
        htree.column("model", width=120, anchor="w")
        htree.column("fw", width=120, anchor="w")
        htree.column("last_ok", width=120, anchor="w")
        htree.column("err_count", width=70, anchor="e")
        htree.column("last_err", width=260, anchor="w")
        htree.pack(fill="x", pady=(4, 0))
        self._health_tree = htree

        # --- Alerts (simple local rules) ---
        alerts_wrap = ttk.Labelframe(td, text=self.t('settings.alerts.title'))
        alerts_wrap.pack(fill="x", pady=(8, 6))
        ttk.Label(alerts_wrap, text=self.t('settings.alerts.hint')).pack(anchor="w")

        ab = ttk.Frame(alerts_wrap)
        ab.pack(fill="x", pady=(6, 4))
        ttk.Button(ab, text=self.t('settings.alerts.add'), command=self._add_alert_row).pack(side="left")
        # Per-row delete is shown inside the table; removing the "remove last" button avoids surprises.

        if not getattr(self, "_alert_vars", None):
            self._alert_vars = []
            for i, r in enumerate(getattr(self.cfg, "alerts", []) or []):
                self._alert_vars.append(
                    (
                        tk.StringVar(value=str(getattr(r, "rule_id", f"rule{i+1}"))),
                        tk.BooleanVar(value=bool(getattr(r, "enabled", True))),
                        tk.StringVar(value=str(getattr(r, "device_key", "*"))),
                        tk.StringVar(value=str(getattr(r, "metric", "W"))),
                        tk.StringVar(value=str(getattr(r, "op", ">"))),
                        tk.StringVar(value=str(getattr(r, "threshold", 0.0))),
                        tk.StringVar(value=str(getattr(r, "duration_seconds", 10))),
                        tk.StringVar(value=str(getattr(r, "cooldown_seconds", 120))),
                        tk.BooleanVar(value=bool(getattr(r, "action_popup", True))),
                        tk.BooleanVar(value=bool(getattr(r, "action_beep", True))),
                        tk.BooleanVar(value=bool(getattr(r, "action_telegram", False))),
                        tk.StringVar(value=str(getattr(r, "message", ""))),
                    )
                )

        
        # Telegram settings for alerts
        tg_box = ttk.LabelFrame(alerts_wrap, text="Telegram")
        tg_box.pack(fill="x", padx=8, pady=(6, 4))

        if not hasattr(self, "_tg_enabled_var"):
            self._tg_enabled_var = tk.BooleanVar(value=bool(getattr(self.cfg.ui, "telegram_enabled", False)))
            self._tg_token_var = tk.StringVar(value=str(getattr(self.cfg.ui, "telegram_bot_token", "") or ""))
            self._tg_chatid_var = tk.StringVar(value=str(getattr(self.cfg.ui, "telegram_chat_id", "") or ""))
            self._tg_verify_var = tk.BooleanVar(value=bool(getattr(self.cfg.ui, "telegram_verify_ssl", True)))

            # Telegram detail level: simple|detailed (stored as code)
            _tg_level = str(getattr(self.cfg.ui, "telegram_detail_level", "detailed") or "detailed").strip().lower()
            if _tg_level not in {"simple", "detailed"}:
                _tg_level = "detailed"
            _tg_disp_d = self.t("settings.alerts.telegram.detail.detailed")
            _tg_disp_s = self.t("settings.alerts.telegram.detail.simple")
            self._tg_detail_display_to_code = {_tg_disp_d: "detailed", _tg_disp_s: "simple"}
            self._tg_detail_code_to_display = {"detailed": _tg_disp_d, "simple": _tg_disp_s}
            self._tg_detail_var = tk.StringVar(value=self._tg_detail_code_to_display.get(_tg_level, _tg_disp_d))

            # Scheduled summaries
            self._tg_daily_sum_var = tk.BooleanVar(value=bool(getattr(self.cfg.ui, "telegram_daily_summary_enabled", False)))
            self._tg_daily_time_var = tk.StringVar(value=str(getattr(self.cfg.ui, "telegram_daily_summary_time", "00:00") or "00:00"))
            self._tg_monthly_sum_var = tk.BooleanVar(value=bool(getattr(self.cfg.ui, "telegram_monthly_summary_enabled", False)))
            self._tg_monthly_time_var = tk.StringVar(value=str(getattr(self.cfg.ui, "telegram_monthly_summary_time", "00:00") or "00:00"))
            self._tg_sum_loadw_var = tk.StringVar(value=str(getattr(self.cfg.ui, "telegram_summary_load_w", 200.0) or 200.0))

        ttk.Checkbutton(
            tg_box,
            text=self.t("settings.alerts.telegram.enabled"),
            variable=self._tg_enabled_var,
        ).grid(row=0, column=0, padx=8, pady=6, sticky="w")

        ttk.Label(tg_box, text=self.t("settings.alerts.telegram.token") + ":").grid(row=0, column=1, padx=(12, 6), pady=6, sticky="e")
        ttk.Entry(tg_box, textvariable=self._tg_token_var, width=38).grid(row=0, column=2, padx=(0, 8), pady=6, sticky="we")

        ttk.Label(tg_box, text=self.t("settings.alerts.telegram.chat_id") + ":").grid(row=0, column=3, padx=(12, 6), pady=6, sticky="e")
        ttk.Entry(tg_box, textvariable=self._tg_chatid_var, width=18).grid(row=0, column=4, padx=(0, 8), pady=6, sticky="we")

        # Test send (uses current token/chat id settings)
        def _tg_send_test() -> None:
            try:
                # Save UI values so the sender sees them
                self._save_settings_devices()
            except Exception:
                pass

            def _worker():
                ok, err = self._telegram_send_sync("✅ Telegram Test")
                def _done():
                    if ok:
                        messagebox.showinfo("Telegram", "OK (gesendet)")
                    else:
                        messagebox.showwarning("Telegram", f"Fehler: {err or 'unbekannt'}")
                try:
                    self.root.after(0, _done)
                except Exception:
                    _done()

            try:
                threading.Thread(target=_worker, daemon=True).start()
            except Exception as _e:
                try:
                    messagebox.showwarning("Telegram", f"Fehler: {_e}")
                except Exception:
                    pass

        ttk.Button(tg_box, text=self.t("settings.alerts.telegram.test"), command=_tg_send_test).grid(
            row=0, column=5, padx=8, pady=6, sticky="e"
        )

        ttk.Checkbutton(
            tg_box,
            text=self.t("settings.alerts.telegram.verify_ssl"),
            variable=self._tg_verify_var,
        ).grid(row=1, column=0, padx=8, pady=(0, 6), sticky="w")

        ttk.Label(tg_box, text=self.t("settings.alerts.telegram.detail") + ":").grid(row=1, column=1, padx=(12, 6), pady=(0, 6), sticky="e")
        ttk.Combobox(
            tg_box,
            textvariable=self._tg_detail_var,
            values=list(self._tg_detail_display_to_code.keys()),
            width=14,
            state="readonly",
        ).grid(row=1, column=2, padx=(0, 8), pady=(0, 6), sticky="w")

        # Daily summary row
        ttk.Checkbutton(
            tg_box,
            text=self.t("settings.alerts.telegram.daily_summary"),
            variable=self._tg_daily_sum_var,
        ).grid(row=2, column=0, padx=8, pady=(0, 6), sticky="w")
        ttk.Label(tg_box, text=self.t("settings.alerts.telegram.daily_time") + ":").grid(row=2, column=1, padx=(12, 6), pady=(0, 6), sticky="e")
        ttk.Entry(tg_box, textvariable=self._tg_daily_time_var, width=8).grid(row=2, column=2, padx=(0, 8), pady=(0, 6), sticky="w")
        ttk.Button(
            tg_box,
            text=self.t("settings.alerts.telegram.daily_send_now"),
            command=self._telegram_send_daily_summary_now,
        ).grid(row=2, column=5, padx=8, pady=(0, 6), sticky="e")

        # Monthly summary row
        ttk.Checkbutton(
            tg_box,
            text=self.t("settings.alerts.telegram.monthly_summary"),
            variable=self._tg_monthly_sum_var,
        ).grid(row=3, column=0, padx=8, pady=(0, 6), sticky="w")
        ttk.Label(tg_box, text=self.t("settings.alerts.telegram.monthly_time") + ":").grid(row=3, column=1, padx=(12, 6), pady=(0, 6), sticky="e")
        ttk.Entry(tg_box, textvariable=self._tg_monthly_time_var, width=8).grid(row=3, column=2, padx=(0, 8), pady=(0, 6), sticky="w")
        ttk.Button(
            tg_box,
            text=self.t("settings.alerts.telegram.monthly_send_now"),
            command=self._telegram_send_monthly_summary_now,
        ).grid(row=3, column=5, padx=8, pady=(0, 6), sticky="e")

        # Status / countdown (shows whether summaries are active and when the next send happens)
        if not hasattr(self, "_tg_daily_status_var"):
            self._tg_daily_status_var = tk.StringVar(value="")
            self._tg_monthly_status_var = tk.StringVar(value="")

        ttk.Label(tg_box, textvariable=self._tg_daily_status_var).grid(
            row=2, column=3, columnspan=2, padx=(6, 0), pady=(0, 6), sticky="w"
        )
        ttk.Label(tg_box, textvariable=self._tg_monthly_status_var).grid(
            row=3, column=3, columnspan=2, padx=(6, 0), pady=(0, 6), sticky="w"
        )

        # allow status column to expand
        try:
            tg_box.grid_columnconfigure(3, weight=1)
            tg_box.grid_columnconfigure(4, weight=1)
        except Exception:
            pass

        # update status when toggles/times change
        def _tg_status_changed(*_a, **_k):
            try:
                self.after(50, self._telegram_update_summary_status_ui)
            except Exception:
                pass

        for _v in (self._tg_enabled_var, self._tg_daily_sum_var, self._tg_monthly_sum_var):
            try:
                _v.trace_add("write", _tg_status_changed)
            except Exception:
                pass
        for _v in (self._tg_daily_time_var, self._tg_monthly_time_var):
            try:
                _v.trace_add("write", _tg_status_changed)
            except Exception:
                pass

        # initial status + timer
        try:
            self._telegram_update_summary_status_ui()
        except Exception:
            pass

        # Summary quality filter (only evaluate VAR/cosφ when load is above threshold)
        ttk.Label(tg_box, text=self.t("settings.alerts.telegram.summary_load_w") + ":").grid(
            row=4, column=1, padx=(12, 6), pady=(0, 6), sticky="e"
        )
        ttk.Entry(tg_box, textvariable=self._tg_sum_loadw_var, width=8).grid(
            row=4, column=2, padx=(0, 8), pady=(0, 6), sticky="w"
        )
        try:
            tg_box.columnconfigure(2, weight=1)
            tg_box.columnconfigure(4, weight=1)
        except Exception:
            pass

# Alerts table
        atable = ttk.Frame(alerts_wrap)
        atable.pack(fill="x")
        headers = [
            ("id", self.t('settings.alerts.col_id'), 10),
            ("enabled", self.t('settings.alerts.col_enabled'), 8),
            ("device", self.t('settings.alerts.col_device'), 12),
            ("metric", self.t('settings.alerts.col_metric'), 8),
            ("op", self.t('settings.alerts.col_op'), 4),
            ("threshold", self.t('settings.alerts.col_threshold'), 10),
            ("duration", self.t('settings.alerts.col_duration'), 9),
            ("cooldown", self.t('settings.alerts.col_cooldown'), 9),
            ("popup", self.t('settings.alerts.col_popup'), 7),
            ("beep", self.t('settings.alerts.col_beep'), 6),
            ("tg", self.t('settings.alerts.col_telegram'), 8),
            ("msg", self.t('settings.alerts.col_message'), 24),
            ("del", "", 3),
        ]
        for j, (_k, label, _w) in enumerate(headers):
            ttk.Label(atable, text=label).grid(row=0, column=j, sticky="w", padx=(0, 6))


# Build dropdown choices for alerts (display: Name (key), stored as key)
        _dev_display = []
        self._alerts_dev_display_to_key = {}
        self._alerts_dev_key_to_display = {}

        # All devices option
        _all_disp = f"{self.t('settings.alerts.device_all')} (*)"
        _dev_display.append(_all_disp)
        self._alerts_dev_display_to_key[_all_disp] = "*"
        self._alerts_dev_key_to_display["*"] = _all_disp

        try:
            for _row in getattr(self, "_dev_vars", []) or []:
                _k = (_row[0].get() or "").strip()
                _n = (_row[1].get() or "").strip()
                if not _k:
                    continue
                _disp = f"{_n} ({_k})" if _n else _k
                if _disp not in _dev_display:
                    _dev_display.append(_disp)
                self._alerts_dev_display_to_key[_disp] = _k
                self._alerts_dev_key_to_display.setdefault(_k, _disp)
        except Exception:
            pass

        def _alerts_device_key_from_display(s: str) -> str:
            s = (s or "").strip()
            if not s:
                return "*"
            try:
                m = getattr(self, "_alerts_dev_display_to_key", {})
                if s in m:
                    return str(m.get(s) or "*")
            except Exception:
                pass
            # Fallback parse: "Name (key)"
            try:
                _m = re.search(r"\(([^()]+)\)\s*$", s)
                if _m:
                    k = (_m.group(1) or "").strip()
                    return k or "*"
            except Exception:
                pass
            return s

        self._alerts_device_key_from_display = _alerts_device_key_from_display

        _metric_choices = [
            "W", "W_L1", "W_L2", "W_L3",
            "V", "V_L1", "V_L2", "V_L3",
            "A", "A_L1", "A_L2", "A_L3",
            "VAR", "VAR_L1", "VAR_L2", "VAR_L3",
            "COSPHI", "COSPHI_L1", "COSPHI_L2", "COSPHI_L3",
        ]
        _op_choices = [">", "<", ">=", "<=", "="]
        for i, row in enumerate(getattr(self, "_alert_vars", []), start=1):
            (v_id, v_en, v_dev, v_met, v_op, v_thr, v_dur, v_cd, v_pop, v_beep, v_tg, v_msg) = row
            # Normalize device selection to display value
            try:
                cur = (v_dev.get() or '').strip()
                if cur and cur in getattr(self, '_alerts_dev_key_to_display', {}):
                    v_dev.set(self._alerts_dev_key_to_display[cur])
            except Exception:
                pass

            # Build row widgets. When a rule is enabled (active), lock editing to avoid accidental changes.
            w_id = ttk.Entry(atable, textvariable=v_id, width=10)
            w_id.grid(row=i, column=0, padx=(0, 6), pady=2, sticky="w")

            w_en = ttk.Checkbutton(atable, variable=v_en)
            w_en.grid(row=i, column=1, padx=(0, 10), pady=2, sticky="w")

            w_dev = ttk.Combobox(atable, textvariable=v_dev, values=_dev_display, width=24, state="readonly")
            w_dev.grid(row=i, column=2, padx=(0, 6), pady=2, sticky="w")

            w_met = ttk.Combobox(atable, textvariable=v_met, values=_metric_choices, width=9, state="readonly")
            w_met.grid(row=i, column=3, padx=(0, 6), pady=2, sticky="w")

            w_op = ttk.Combobox(atable, textvariable=v_op, values=_op_choices, width=4, state="readonly")
            w_op.grid(row=i, column=4, padx=(0, 6), pady=2, sticky="w")

            w_thr = ttk.Entry(atable, textvariable=v_thr, width=10)
            w_thr.grid(row=i, column=5, padx=(0, 6), pady=2, sticky="w")

            w_dur = ttk.Entry(atable, textvariable=v_dur, width=9)
            w_dur.grid(row=i, column=6, padx=(0, 6), pady=2, sticky="w")

            w_cd = ttk.Entry(atable, textvariable=v_cd, width=9)
            w_cd.grid(row=i, column=7, padx=(0, 6), pady=2, sticky="w")

            w_pop = ttk.Checkbutton(atable, variable=v_pop)
            w_pop.grid(row=i, column=8, padx=(0, 10), pady=2, sticky="w")

            w_beep = ttk.Checkbutton(atable, variable=v_beep)
            w_beep.grid(row=i, column=9, padx=(0, 10), pady=2, sticky="w")

            w_tg = ttk.Checkbutton(atable, variable=v_tg)
            w_tg.grid(row=i, column=10, padx=(0, 10), pady=2, sticky="w")

            w_msg = ttk.Entry(atable, textvariable=v_msg, width=24)
            w_msg.grid(row=i, column=11, padx=(0, 6), pady=2, sticky="we")

            # Per-row delete button (allows removing rules in the middle)
            def _del_row(_idx=i-1, _v_en=v_en, _v_id=v_id):
                try:
                    if bool(_v_en.get()):
                        if not messagebox.askyesno(
                            self.t('settings.alerts.title'),
                            self.t('settings.alerts.confirm_delete_active').format(rule=(_v_id.get() or '').strip() or f"#{_idx+1}"),
                        ):
                            return
                except Exception:
                    pass
                self._delete_alert_row(_idx)

            ttk.Button(atable, text="✖", width=2, command=_del_row).grid(row=i, column=12, padx=(0, 0), pady=2, sticky="w")

            # Lock editing for active rules (enabled=True). Use default-args to avoid late-binding bugs.
            def _toggle_row_lock(*_a, v_en=v_en, w_id=w_id, w_dev=w_dev, w_met=w_met, w_op=w_op, w_thr=w_thr, w_dur=w_dur, w_cd=w_cd, w_pop=w_pop, w_beep=w_beep, w_tg=w_tg, w_msg=w_msg):
                locked = bool(v_en.get())
                if locked:
                    try:
                        w_id.configure(state="disabled")
                        w_dev.configure(state="disabled")
                        w_met.configure(state="disabled")
                        w_op.configure(state="disabled")
                        w_thr.configure(state="disabled")
                        w_dur.configure(state="disabled")
                        w_cd.configure(state="disabled")
                        w_pop.configure(state="disabled")
                        w_beep.configure(state="disabled")
                        w_tg.configure(state="disabled")
                        w_msg.configure(state="disabled")
                    except Exception:
                        pass
                else:
                    try:
                        w_id.configure(state="normal")
                        w_dev.configure(state="readonly")
                        w_met.configure(state="readonly")
                        w_op.configure(state="readonly")
                        w_thr.configure(state="normal")
                        w_dur.configure(state="normal")
                        w_cd.configure(state="normal")
                        w_pop.configure(state="normal")
                        w_beep.configure(state="normal")
                        w_tg.configure(state="normal")
                        w_msg.configure(state="normal")
                    except Exception:
                        pass

            try:
                v_en.trace_add("write", _toggle_row_lock)
            except Exception:
                pass
            _toggle_row_lock()

# Device remove dropdown (not just last row)
        if not getattr(self, '_remove_device_choice_var', None):
            self._remove_device_choice_var = tk.StringVar(value='')
        choices = []
        for row in getattr(self, '_dev_vars', []):
            v_key, v_name, _v_host, _v_emid = row[0], row[1], row[2], row[3]
            k = (v_key.get() or '').strip()
            n = (v_name.get() or '').strip()
            if not k:
                continue
            label = f"{k} - {n}" if n and n != k else k
            choices.append(label)
        if choices and (self._remove_device_choice_var.get() not in choices):
            self._remove_device_choice_var.set(choices[-1])

        ttk.Label(btnrow, text=self.t('settings.device.remove.select') + ':').pack(side="left", padx=(12, 4))
        ttk.Combobox(btnrow, values=choices, textvariable=self._remove_device_choice_var, state='readonly', width=26).pack(side="left")
        ttk.Button(btnrow, text=self.t('settings.device.remove'), command=self._remove_selected_device_row).pack(side="left", padx=8)

        ttk.Label(btnrow, text=self.t('settings.devices.hint')).pack(side="left", padx=10)


        # ---------------- Live & Preis ----------------
        # Language
        lang_box = ttk.LabelFrame(tab_main, text=self.t('settings.language'))
        lang_box.pack(fill="x", pady=(0, 10))
        if not getattr(self, 'set_language_var', None):
            self.set_language_var = tk.StringVar(value=str(getattr(self.cfg.ui, 'language', 'de')))
        # Show language names in the current UI language
        # (Codes are stable, labels are translated)
        lang_choices = [f"{code} - {self.t('settings.language.' + code)}" for code in LANGS]
        # Keep selection stable if possible
        cur = (self.set_language_var.get() or 'de').strip().lower()
        if cur in LANGS:
            self.set_language_var.set(cur)
        ttk.Label(lang_box, text=self.t('settings.language') + ':').grid(row=0, column=0, padx=8, pady=8, sticky='w')
        self._lang_display_var = tk.StringVar(value=f"{cur} - {self.t('settings.language.' + cur)}")
        def _on_lang_pick(_evt=None):
            s = (self._lang_display_var.get() or '').strip()
            code = s.split('-', 1)[0].strip().lower() if s else 'de'
            if code not in LANGS:
                code = 'de'
            self.set_language_var.set(code)
        cb = ttk.Combobox(lang_box, values=lang_choices, textvariable=self._lang_display_var, state='readonly', width=24)
        cb.grid(row=0, column=1, padx=8, pady=8, sticky='w')
        cb.bind('<<ComboboxSelected>>', _on_lang_pick)

        pricing_box = ttk.LabelFrame(tab_main, text=self.t('settings.pricing.title'))
        pricing_box.pack(fill="x", pady=(0, 10))
        ttk.Label(pricing_box, text=self.t('settings.pricing.price')).grid(row=0, column=0, padx=8, pady=8, sticky="w")
        self.price_var = tk.StringVar(value=str(self.cfg.pricing.electricity_price_eur_per_kwh))
        ttk.Entry(pricing_box, textvariable=self.price_var, width=10).grid(row=0, column=1, padx=8, pady=8, sticky="w")
        self.price_includes_vat_var = tk.BooleanVar(value=bool(self.cfg.pricing.price_includes_vat))
        self.vat_enabled_var = tk.BooleanVar(value=bool(self.cfg.pricing.vat_enabled))
        self.vat_rate_var = tk.StringVar(value=str(self.cfg.pricing.vat_rate_percent))
        ttk.Checkbutton(pricing_box, text=self.t('settings.pricing.price_includes_vat'), variable=self.price_includes_vat_var).grid(
            row=0, column=2, padx=8, pady=8, sticky="w"
        )
        ttk.Checkbutton(pricing_box, text=self.t('settings.pricing.vat_enabled'), variable=self.vat_enabled_var).grid(
            row=1, column=0, padx=8, pady=8, sticky="w"
        )
        ttk.Label(pricing_box, text=self.t('settings.pricing.vat_rate')).grid(row=1, column=1, padx=8, pady=8, sticky="e")
        ttk.Entry(pricing_box, textvariable=self.vat_rate_var, width=6).grid(row=1, column=2, padx=8, pady=8, sticky="w")

        ttk.Label(pricing_box, text=self.t('settings.pricing.base_fee')).grid(row=2, column=0, padx=8, pady=8, sticky="w")
        self.base_fee_var = tk.StringVar(value=str(getattr(self.cfg.pricing, 'base_fee_eur_per_year', 127.51)))
        ttk.Entry(pricing_box, textvariable=self.base_fee_var, width=10).grid(row=2, column=1, padx=8, pady=8, sticky="w")
        self.base_fee_includes_vat_var = tk.BooleanVar(value=bool(getattr(self.cfg.pricing, 'base_fee_includes_vat', True)))
        ttk.Checkbutton(pricing_box, text=self.t('settings.pricing.base_fee_includes_vat'), variable=self.base_fee_includes_vat_var).grid(row=2, column=2, padx=8, pady=8, sticky="w")

        autosync_box = ttk.LabelFrame(tab_main, text=self.t('settings.autosync.title'))
        autosync_box.pack(fill="x", pady=(0, 10))
        self.set_autosync_enabled_var = tk.BooleanVar(value=bool(self.cfg.ui.autosync_enabled))
        self.set_autosync_interval_var = tk.IntVar(value=int(self.cfg.ui.autosync_interval_hours))
        self.set_autosync_mode_var = tk.StringVar(value=str(self.cfg.ui.autosync_mode))
        ttk.Checkbutton(autosync_box, text=self.t('settings.autosync.enabled'), variable=self.set_autosync_enabled_var).grid(
            row=0, column=0, padx=8, pady=8, sticky="w"
        )
        ttk.Label(autosync_box, text=self.t('settings.autosync.interval_h')).grid(row=0, column=1, padx=8, pady=8, sticky="e")
        ttk.Combobox(
            autosync_box,
            values=[str(x) for x in AUTOSYNC_INTERVAL_OPTIONS],
            width=6,
            textvariable=self.set_autosync_interval_var,
            state="readonly",
        ).grid(row=0, column=2, padx=8, pady=8, sticky="w")
        ttk.Label(autosync_box, text=self.t('settings.autosync.mode')).grid(row=0, column=3, padx=8, pady=8, sticky="e")
        ttk.Combobox(
            autosync_box,
            values=[m for m, _ in AUTOSYNC_MODE_OPTIONS],
            width=12,
            textvariable=self.set_autosync_mode_var,
            state="readonly",
        ).grid(row=0, column=4, padx=8, pady=8, sticky="w")

        live_box = ttk.LabelFrame(tab_main, text=self.t('settings.live.title'))
        live_box.pack(fill="x", pady=(0, 10))
        self.set_live_poll_var = tk.DoubleVar(value=float(self.cfg.ui.live_poll_seconds))
        self.set_live_window_var = tk.IntVar(value=int(self.cfg.ui.live_window_minutes))
        self.set_plot_redraw_var = tk.DoubleVar(value=float(self.cfg.ui.plot_redraw_seconds))
        ttk.Label(live_box, text=self.t('settings.live.polling')).grid(row=0, column=0, padx=8, pady=8, sticky="e")
        ttk.Entry(live_box, width=8, textvariable=self.set_live_poll_var).grid(row=0, column=1, padx=8, pady=8, sticky="w")
        ttk.Label(live_box, text=self.t('settings.live.window')).grid(row=0, column=2, padx=8, pady=8, sticky="e")
        ttk.Combobox(
            live_box,
            values=["5", "10", "15", "30", "60", "120"],
            width=8,
            textvariable=self.set_live_window_var,
            state="readonly",
        ).grid(row=0, column=3, padx=8, pady=8, sticky="w")
        ttk.Label(live_box, text=self.t('settings.live.redraw')).grid(row=0, column=4, padx=8, pady=8, sticky="e")
        ttk.Entry(live_box, width=8, textvariable=self.set_plot_redraw_var).grid(row=0, column=5, padx=8, pady=8, sticky="w")
        ttk.Label(live_box, text=self.t('settings.live.note')).grid(
            row=1, column=0, columnspan=6, padx=8, pady=(0, 8), sticky="w"
        )

        web_box = ttk.LabelFrame(tab_main, text=self.t('settings.web.title'))
        web_box.pack(fill="x")
        self.set_live_web_enabled_var = tk.BooleanVar(value=bool(self.cfg.ui.live_web_enabled))
        self.set_live_web_port_var = tk.IntVar(value=int(self.cfg.ui.live_web_port))
        self.set_live_web_refresh_var = tk.DoubleVar(value=float(self.cfg.ui.live_web_refresh_seconds))
        ttk.Checkbutton(web_box, text=self.t('settings.web.enable'), variable=self.set_live_web_enabled_var).grid(row=0, column=0, padx=8, pady=8, sticky="w")
        ttk.Label(web_box, text=self.t('settings.web.port')).grid(row=0, column=1, padx=8, pady=8, sticky="e")
        ttk.Entry(web_box, width=8, textvariable=self.set_live_web_port_var).grid(row=0, column=2, padx=8, pady=8, sticky="w")
        ttk.Label(web_box, text=self.t('settings.web.update')).grid(row=0, column=3, padx=8, pady=8, sticky="e")
        ttk.Entry(web_box, width=8, textvariable=self.set_live_web_refresh_var).grid(row=0, column=4, padx=8, pady=8, sticky="w")
        ttk.Label(web_box, text=self.t('settings.web.note')).grid(row=1, column=0, columnspan=5, padx=8, pady=(0, 8), sticky="w")


        # ---------------- Advanced (Config) ----------------
        ttk.Label(tab_advanced, text=self.t('settings.advanced')).pack(anchor="w", pady=(0, 6))

        # Download / Netzwerk
        download_box = ttk.LabelFrame(tab_advanced, text=self.t('settings.download.title'))
        download_box.pack(fill="x", pady=(0, 10))
        if not getattr(self, 'set_download_chunk_h_var', None):
            try:
                self.set_download_chunk_h_var = tk.IntVar(value=int(max(1, int(getattr(self.cfg.download, 'chunk_seconds', 12*3600)) // 3600)))
            except Exception:
                self.set_download_chunk_h_var = tk.IntVar(value=12)
        if not getattr(self, 'set_download_overlap_s_var', None):
            self.set_download_overlap_s_var = tk.IntVar(value=int(getattr(self.cfg.download, 'overlap_seconds', 60)))
        if not getattr(self, 'set_download_timeout_s_var', None):
            self.set_download_timeout_s_var = tk.DoubleVar(value=float(getattr(self.cfg.download, 'timeout_seconds', 8.0)))
        if not getattr(self, 'set_download_retries_var', None):
            self.set_download_retries_var = tk.IntVar(value=int(getattr(self.cfg.download, 'retries', 3)))
        if not getattr(self, 'set_download_backoff_var', None):
            self.set_download_backoff_var = tk.DoubleVar(value=float(getattr(self.cfg.download, 'backoff_base_seconds', 1.5)))

        ttk.Label(download_box, text=self.t('settings.download.chunk_h')).grid(row=0, column=0, padx=8, pady=6, sticky="w")
        ttk.Entry(download_box, textvariable=self.set_download_chunk_h_var, width=8).grid(row=0, column=1, padx=8, pady=6, sticky="w")
        ttk.Label(download_box, text=self.t('settings.download.overlap_s')).grid(row=0, column=2, padx=8, pady=6, sticky="w")
        ttk.Entry(download_box, textvariable=self.set_download_overlap_s_var, width=8).grid(row=0, column=3, padx=8, pady=6, sticky="w")
        ttk.Label(download_box, text=self.t('settings.download.timeout_s')).grid(row=1, column=0, padx=8, pady=6, sticky="w")
        ttk.Entry(download_box, textvariable=self.set_download_timeout_s_var, width=8).grid(row=1, column=1, padx=8, pady=6, sticky="w")
        ttk.Label(download_box, text=self.t('settings.download.retries')).grid(row=1, column=2, padx=8, pady=6, sticky="w")
        ttk.Entry(download_box, textvariable=self.set_download_retries_var, width=8).grid(row=1, column=3, padx=8, pady=6, sticky="w")
        ttk.Label(download_box, text=self.t('settings.download.backoff')).grid(row=2, column=0, padx=8, pady=6, sticky="w")
        ttk.Entry(download_box, textvariable=self.set_download_backoff_var, width=8).grid(row=2, column=1, padx=8, pady=6, sticky="w")
        ttk.Label(download_box, text=self.t('settings.download.note')).grid(row=3, column=0, columnspan=4, padx=8, pady=(0, 6), sticky="w")

        # CSV Pack / Dateien
        csv_box = ttk.LabelFrame(tab_advanced, text=self.t('settings.csv_pack.title'))
        csv_box.pack(fill="x", pady=(0, 10))
        if not getattr(self, 'set_csv_threshold_var', None):
            self.set_csv_threshold_var = tk.IntVar(value=int(getattr(self.cfg.csv_pack, 'threshold_count', 120)))
        if not getattr(self, 'set_csv_max_mb_var', None):
            self.set_csv_max_mb_var = tk.IntVar(value=int(getattr(self.cfg.csv_pack, 'max_megabytes', 20)))
        if not getattr(self, 'set_csv_remove_merged_var', None):
            self.set_csv_remove_merged_var = tk.BooleanVar(value=bool(getattr(self.cfg.csv_pack, 'remove_merged', False)))

        ttk.Label(csv_box, text=self.t('settings.csv_pack.threshold')).grid(row=0, column=0, padx=8, pady=6, sticky="w")
        ttk.Entry(csv_box, textvariable=self.set_csv_threshold_var, width=10).grid(row=0, column=1, padx=8, pady=6, sticky="w")
        ttk.Label(csv_box, text=self.t('settings.csv_pack.max_mb')).grid(row=0, column=2, padx=8, pady=6, sticky="w")
        ttk.Entry(csv_box, textvariable=self.set_csv_max_mb_var, width=10).grid(row=0, column=3, padx=8, pady=6, sticky="w")
        ttk.Checkbutton(csv_box, text=self.t('settings.csv_pack.remove_merged'), variable=self.set_csv_remove_merged_var).grid(
            row=1, column=0, columnspan=4, padx=8, pady=(0, 6), sticky="w"
        )

        # UI / Erweitert
        ui_adv_box = ttk.LabelFrame(tab_advanced, text=self.t('settings.ui_advanced.title'))
        ui_adv_box.pack(fill="x")

        if not getattr(self, 'set_live_retention_var', None):
            self.set_live_retention_var = tk.IntVar(value=int(getattr(self.cfg.ui, 'live_retention_minutes', 120)))
        if not getattr(self, 'set_live_web_token_var', None):
            self.set_live_web_token_var = tk.StringVar(value=str(getattr(self.cfg.ui, 'live_web_token', '') or ''))
        if not getattr(self, 'set_live_smoothing_enabled_var', None):
            self.set_live_smoothing_enabled_var = tk.BooleanVar(value=bool(getattr(self.cfg.ui, 'live_smoothing_enabled', False)))
        if not getattr(self, 'set_live_smoothing_seconds_var', None):
            self.set_live_smoothing_seconds_var = tk.IntVar(value=int(getattr(self.cfg.ui, 'live_smoothing_seconds', 10)))

        n_devices = len(getattr(self, '_dev_vars', [])) or len(getattr(self.cfg, 'devices', [])) or 1
        n_pages = max(1, int((n_devices + 1) // 2))
        if not getattr(self, 'set_start_page_var', None):
            self.set_start_page_var = tk.IntVar(value=int(getattr(self.cfg.ui, 'device_page_index', 0)) + 1)
        try:
            cur_page = int(self.set_start_page_var.get() or 1)
        except Exception:
            cur_page = 1
        cur_page = max(1, min(n_pages, cur_page))
        self.set_start_page_var.set(cur_page)

        ttk.Label(ui_adv_box, text=self.t('settings.ui_advanced.retention_min')).grid(row=0, column=0, padx=8, pady=6, sticky="w")
        ttk.Entry(ui_adv_box, textvariable=self.set_live_retention_var, width=8).grid(row=0, column=1, padx=8, pady=6, sticky="w")

        ttk.Label(ui_adv_box, text=self.t('settings.ui_advanced.web_token')).grid(row=0, column=2, padx=8, pady=6, sticky="w")
        ttk.Entry(ui_adv_box, textvariable=self.set_live_web_token_var, width=22).grid(row=0, column=3, padx=8, pady=6, sticky="w")

        ttk.Label(ui_adv_box, text=self.t('settings.ui_advanced.start_page')).grid(row=1, column=0, padx=8, pady=6, sticky="w")
        ttk.Combobox(
            ui_adv_box,
            values=[str(i) for i in range(1, n_pages + 1)],
            width=6,
            textvariable=self.set_start_page_var,
            state="readonly",
        ).grid(row=1, column=1, padx=8, pady=6, sticky="w")

        ttk.Checkbutton(ui_adv_box, text=self.t('settings.ui_advanced.smoothing_enable'), variable=self.set_live_smoothing_enabled_var).grid(
            row=1, column=2, padx=8, pady=6, sticky="w"
        )
        ttk.Label(ui_adv_box, text=self.t('settings.ui_advanced.smoothing_seconds')).grid(row=1, column=3, padx=8, pady=6, sticky="e")
        ttk.Entry(ui_adv_box, textvariable=self.set_live_smoothing_seconds_var, width=8).grid(row=1, column=4, padx=8, pady=6, sticky="w")

        ttk.Label(ui_adv_box, text=self.t('settings.ui_advanced.note')).grid(row=2, column=0, columnspan=5, padx=8, pady=(0, 6), sticky="w")

        # ---------------- Expert / Raw config ----------------
        ttk.Label(tab_expert, text=self.t('settings.expert.note')).pack(anchor="w", pady=(0, 6))
        raw_box = ttk.LabelFrame(tab_expert, text=self.t('settings.raw.title'))
        raw_box.pack(fill="both", expand=True, padx=4, pady=6)

        raw_frame = ttk.Frame(raw_box)
        raw_frame.pack(fill="both", expand=True, padx=8, pady=8)
        self.raw_config_text = tk.Text(raw_frame, height=18, width=120, wrap="none")
        ysb = ttk.Scrollbar(raw_frame, orient="vertical", command=self.raw_config_text.yview)
        xsb = ttk.Scrollbar(raw_frame, orient="horizontal", command=self.raw_config_text.xview)
        self.raw_config_text.configure(yscrollcommand=ysb.set, xscrollcommand=xsb.set)
        self.raw_config_text.grid(row=0, column=0, sticky="nsew")
        ysb.grid(row=0, column=1, sticky="ns")
        xsb.grid(row=1, column=0, sticky="ew")
        raw_frame.rowconfigure(0, weight=1)
        raw_frame.columnconfigure(0, weight=1)

        try:
            self._raw_config_load_from_disk()
        except Exception:
            pass

        raw_btns = ttk.Frame(raw_box)
        raw_btns.pack(fill="x", padx=8, pady=(0, 8))
        ttk.Button(raw_btns, text=self.t('settings.raw.reload'), command=self._raw_config_load_from_disk).pack(side="left")
        ttk.Button(raw_btns, text=self.t('settings.raw.pretty'), command=self._raw_config_pretty).pack(side="left", padx=8)
        ttk.Button(raw_btns, text=self.t('settings.raw.validate'), command=self._raw_config_validate).pack(side="left")
        ttk.Button(raw_btns, text=self.t('settings.raw.apply'), command=self._raw_config_apply).pack(side="left", padx=8)

        # ---------------- Rechnung ----------------
        billing_box = ttk.LabelFrame(tab_billing, text=self.t('billing.title'))
        billing_box.pack(fill="both", expand=True)

        issuer_box = ttk.LabelFrame(billing_box, text=self.t('billing.issuer.title'))
        issuer_box.pack(fill="x", padx=10, pady=(8, 6))
        self.bill_issuer_name = tk.StringVar(value=str(self.cfg.billing.issuer.name))
        self.bill_issuer_vat_id = tk.StringVar(value=str(self.cfg.billing.issuer.vat_id))
        self.bill_issuer_email = tk.StringVar(value=str(self.cfg.billing.issuer.email))
        self.bill_issuer_phone = tk.StringVar(value=str(self.cfg.billing.issuer.phone))
        self.bill_issuer_iban = tk.StringVar(value=str(self.cfg.billing.issuer.iban))
        self.bill_issuer_bic = tk.StringVar(value=str(self.cfg.billing.issuer.bic))
        ttk.Label(issuer_box, text=self.t('billing.field.name')).grid(row=0, column=0, padx=8, pady=4, sticky="w")
        ttk.Entry(issuer_box, textvariable=self.bill_issuer_name, width=50).grid(row=0, column=1, padx=8, pady=4, sticky="w")
        ttk.Label(issuer_box, text=self.t('billing.field.address')).grid(row=1, column=0, padx=8, pady=4, sticky="nw")
        self.bill_issuer_addr = tk.Text(issuer_box, height=3, width=50)
        self.bill_issuer_addr.grid(row=1, column=1, padx=8, pady=4, sticky="w")
        self.bill_issuer_addr.insert("1.0", "\\n".join(self.cfg.billing.issuer.address_lines or []))
        ttk.Label(issuer_box, text=self.t('billing.field.vat_id')).grid(row=0, column=2, padx=8, pady=4, sticky="w")
        ttk.Entry(issuer_box, textvariable=self.bill_issuer_vat_id, width=24).grid(row=0, column=3, padx=8, pady=4, sticky="w")
        ttk.Label(issuer_box, text=self.t('billing.field.email')).grid(row=2, column=0, padx=8, pady=4, sticky="w")
        ttk.Entry(issuer_box, textvariable=self.bill_issuer_email, width=35).grid(row=2, column=1, padx=8, pady=4, sticky="w")
        ttk.Label(issuer_box, text=self.t('billing.field.phone')).grid(row=2, column=2, padx=8, pady=4, sticky="w")
        ttk.Entry(issuer_box, textvariable=self.bill_issuer_phone, width=24).grid(row=2, column=3, padx=8, pady=4, sticky="w")
        ttk.Label(issuer_box, text=self.t('billing.field.iban')).grid(row=3, column=0, padx=8, pady=4, sticky="w")
        ttk.Entry(issuer_box, textvariable=self.bill_issuer_iban, width=35).grid(row=3, column=1, padx=8, pady=4, sticky="w")
        ttk.Label(issuer_box, text=self.t('billing.field.bic')).grid(row=3, column=2, padx=8, pady=4, sticky="w")
        ttk.Entry(issuer_box, textvariable=self.bill_issuer_bic, width=24).grid(row=3, column=3, padx=8, pady=4, sticky="w")

        cust_box = ttk.LabelFrame(billing_box, text=self.t('billing.customer.title'))
        cust_box.pack(fill="x", padx=10, pady=(6, 6))
        self.bill_cust_name = tk.StringVar(value=str(self.cfg.billing.customer.name))
        self.bill_cust_vat_id = tk.StringVar(value=str(self.cfg.billing.customer.vat_id))
        self.bill_cust_email = tk.StringVar(value=str(self.cfg.billing.customer.email))
        self.bill_cust_phone = tk.StringVar(value=str(self.cfg.billing.customer.phone))
        ttk.Label(cust_box, text=self.t('billing.field.name')).grid(row=0, column=0, padx=8, pady=4, sticky="w")
        ttk.Entry(cust_box, textvariable=self.bill_cust_name, width=50).grid(row=0, column=1, padx=8, pady=4, sticky="w")
        ttk.Label(cust_box, text=self.t('billing.field.address')).grid(row=1, column=0, padx=8, pady=4, sticky="nw")
        self.bill_cust_addr = tk.Text(cust_box, height=3, width=50)
        self.bill_cust_addr.grid(row=1, column=1, padx=8, pady=4, sticky="w")
        self.bill_cust_addr.insert("1.0", "\\n".join(self.cfg.billing.customer.address_lines or []))
        ttk.Label(cust_box, text=self.t('billing.field.vat_id_optional')).grid(row=0, column=2, padx=8, pady=4, sticky="w")
        ttk.Entry(cust_box, textvariable=self.bill_cust_vat_id, width=24).grid(row=0, column=3, padx=8, pady=4, sticky="w")
        ttk.Label(cust_box, text=self.t('billing.field.email')).grid(row=2, column=0, padx=8, pady=4, sticky="w")
        ttk.Entry(cust_box, textvariable=self.bill_cust_email, width=35).grid(row=2, column=1, padx=8, pady=4, sticky="w")
        ttk.Label(cust_box, text=self.t('billing.field.phone')).grid(row=2, column=2, padx=8, pady=4, sticky="w")
        ttk.Entry(cust_box, textvariable=self.bill_cust_phone, width=24).grid(row=2, column=3, padx=8, pady=4, sticky="w")

        inv_box = ttk.LabelFrame(billing_box, text=self.t('billing.invoice_settings.title'))
        inv_box.pack(fill="x", padx=10, pady=(6, 8))
        self.bill_invoice_prefix = tk.StringVar(value=str(self.cfg.billing.invoice_prefix))
        self.bill_payment_terms = tk.StringVar(value=str(self.cfg.billing.payment_terms_days))
        ttk.Label(inv_box, text=self.t('billing.field.prefix')).grid(row=0, column=0, padx=8, pady=6, sticky="w")
        ttk.Entry(inv_box, textvariable=self.bill_invoice_prefix, width=10).grid(row=0, column=1, padx=8, pady=6, sticky="w")
        ttk.Label(inv_box, text=self.t('billing.field.payment_terms')).grid(row=0, column=2, padx=8, pady=6, sticky="w")
        ttk.Entry(inv_box, textvariable=self.bill_payment_terms, width=6).grid(row=0, column=3, padx=8, pady=6, sticky="w")

        # bottom action bar (always visible)
        bottom = ttk.Frame(frm)
        bottom.pack(fill="x", padx=12, pady=(0, 12))
        ttk.Button(bottom, text=self.t('settings.save'), command=self._save_settings).pack(side="left")
        ttk.Button(bottom, text=self.t('settings.reload'), command=self._reload_settings).pack(side="left", padx=8)
        self.settings_status = tk.StringVar(value=f"config.json: {self.cfg_path}")
        ttk.Label(bottom, textvariable=self.settings_status).pack(side="left", padx=12)
    def _add_device_row(self) -> None:
        # Adds a new device row (not persisted until you click "Speichern").
        n = len(getattr(self, "_dev_vars", [])) + 1
        key = f"shelly{n}"
        name = f"Shelly {n}"
        v_key = tk.StringVar(value=key)
        v_name = tk.StringVar(value=name)
        v_host = tk.StringVar(value="")
        v_emid = tk.StringVar(value="0")
        # Auto-detected fields (filled by "Add by IP" or "Check all")
        v_model = tk.StringVar(value="")
        v_kind = tk.StringVar(value="em")
        v_gen = tk.StringVar(value="0")
        v_phases = tk.StringVar(value="3")
        v_emdata = tk.BooleanVar(value=True)
        self._dev_vars.append((v_key, v_name, v_host, v_emid, v_model, v_kind, v_gen, v_phases, v_emdata))
        self._clear_frame(self.tab_settings)
        self._build_settings_tab()

    def _add_device_by_ip_dialog(self) -> None:
        """Add a new device row by probing a Shelly IP/host and auto-filling type."""
        win = tk.Toplevel(self)
        win.title(self.t('settings.device.add_by_ip.title'))
        win.resizable(False, False)
        frm = ttk.Frame(win)
        frm.pack(padx=12, pady=12, fill="both", expand=True)

        ip_var = tk.StringVar(value="")
        name_var = tk.StringVar(value="")
        status_var = tk.StringVar(value="")

        ttk.Label(frm, text=self.t('settings.device.add_by_ip.ip')).grid(row=0, column=0, sticky="w", pady=(0, 6))
        ip_ent = ttk.Entry(frm, textvariable=ip_var, width=24)
        ip_ent.grid(row=0, column=1, sticky="w", pady=(0, 6))
        ttk.Label(frm, text=self.t('settings.device.add_by_ip.name_optional')).grid(row=1, column=0, sticky="w", pady=(0, 6))
        ttk.Entry(frm, textvariable=name_var, width=24).grid(row=1, column=1, sticky="w", pady=(0, 6))
        ttk.Label(frm, textvariable=status_var).grid(row=2, column=0, columnspan=2, sticky="w")

        def _do_add() -> None:
            host = (ip_var.get() or "").strip()
            if not host:
                return
            status_var.set(self.t('settings.device.add_by_ip.probing'))
            win.update_idletasks()
            try:
                disc = probe_device(host, timeout_seconds=2.5)
            except Exception as e:
                status_var.set(f"{self.t('settings.device.add_by_ip.failed')}: {e}")
                return

            n = len(getattr(self, "_dev_vars", [])) + 1
            key = f"shelly{n}"
            # Never overwrite a user-entered name; otherwise use model.
            nm = (name_var.get() or "").strip() or (disc.model.strip() or f"Shelly {n}")

            v_key = tk.StringVar(value=key)
            v_name = tk.StringVar(value=nm)
            v_host = tk.StringVar(value=str(host))
            v_emid = tk.StringVar(value=str(int(disc.component_id or 0)))
            v_model = tk.StringVar(value=str(disc.model or ""))
            v_kind = tk.StringVar(value=str(disc.kind or "unknown"))
            v_gen = tk.StringVar(value=str(int(disc.gen or 0)))
            v_phases = tk.StringVar(value=str(int(disc.phases or 1)))
            v_emdata = tk.BooleanVar(value=bool(disc.supports_emdata))

            self._dev_vars.append((v_key, v_name, v_host, v_emid, v_model, v_kind, v_gen, v_phases, v_emdata))
            try:
                win.destroy()
            except Exception:
                pass
            self._clear_frame(self.tab_settings)
            self._build_settings_tab()

        def _do_cancel() -> None:
            try:
                win.destroy()
            except Exception:
                pass

        btns = ttk.Frame(frm)
        btns.grid(row=3, column=0, columnspan=2, sticky="e", pady=(8, 0))
        ttk.Button(btns, text=self.t('settings.device.add_by_ip.add'), command=_do_add).pack(side="left")
        ttk.Button(btns, text=self.t('settings.cancel'), command=_do_cancel).pack(side="left", padx=8)

        try:
            ip_ent.focus_set()
        except Exception:
            pass

    def _probe_all_devices_async(self) -> None:
        """Probe all current device rows and update model/type/id fields in-place."""

        def _worker() -> None:
            rows = list(getattr(self, "_dev_vars", []))
            for row in rows:
                try:
                    v_key, v_name, v_host, v_emid, v_model, v_kind, v_gen, v_phases, v_emdata = row
                    host = (v_host.get() or "").strip()
                    if not host:
                        continue
                    disc = probe_device(host, timeout_seconds=2.5)

                    def _apply_one() -> None:
                        try:
                            v_emid.set(str(int(disc.component_id or 0)))
                            v_model.set(str(disc.model or ""))
                            v_kind.set(str(disc.kind or "unknown"))
                            v_gen.set(str(int(disc.gen or 0)))
                            v_phases.set(str(int(disc.phases or 1)))
                            v_emdata.set(bool(disc.supports_emdata))
                        except Exception:
                            pass

                    self.after(0, _apply_one)
                except Exception:
                    continue

            def _done() -> None:
                try:
                    self.settings_status.set(self.t('settings.device.check_all.done'))
                except Exception:
                    pass
                try:
                    self._clear_frame(self.tab_settings)
                    self._build_settings_tab()
                except Exception:
                    pass

            try:
                self.after(0, _done)
            except Exception:
                pass

        try:
            self.settings_status.set(self.t('settings.device.check_all.running'))
        except Exception:
            pass
        threading.Thread(target=_worker, name="ShellyProbeAll", daemon=True).start()

    def _remove_selected_device_row(self) -> None:
        vars_ = getattr(self, "_dev_vars", [])
        if len(vars_) <= 1:
            try:
                messagebox.showinfo(self.t('msg.devices'), self.t('settings.device.remove.need_one'))
            except Exception:
                pass
            return

        sel = ""
        try:
            sel = str(getattr(self, '_remove_device_choice_var', None).get() or '').strip()
        except Exception:
            sel = ""

        key = sel.split(' - ', 1)[0].strip() if sel else ""
        if not key and vars_:
            try:
                key = (vars_[-1][0].get() or '').strip()
            except Exception:
                key = ""

        idx = None
        for i, row in enumerate(vars_):
            try:
                v_key = row[0]
                if (v_key.get() or '').strip() == key:
                    idx = i
                    break
            except Exception:
                continue
        if idx is None:
            idx = len(vars_) - 1

        if len(vars_) <= 1:
            return
        try:
            vars_.pop(idx)
        except Exception:
            try:
                vars_.pop()
            except Exception:
                pass

        self._clear_frame(self.tab_settings)
        self._build_settings_tab()

    # Backward compatibility
    def _remove_last_device_row(self) -> None:
        self._remove_selected_device_row()
    def _raw_config_load_from_disk(self) -> None:
        try:
            txt = Path(self.cfg_path).read_text(encoding='utf-8')
        except Exception:
            try:
                txt = json.dumps(asdict(self.cfg), indent=2, ensure_ascii=False)
            except Exception:
                txt = ''
        try:
            self.raw_config_text.delete('1.0', 'end')
            self.raw_config_text.insert('1.0', txt)
        except Exception:
            pass

    def _raw_config_get_text(self) -> str:
        try:
            return str(self.raw_config_text.get('1.0', 'end')).strip()
        except Exception:
            return ''

    def _raw_config_pretty(self) -> None:
        raw = self._raw_config_get_text()
        if not raw:
            return
        try:
            obj = json.loads(raw)
            pretty = json.dumps(obj, indent=2, ensure_ascii=False)
            self.raw_config_text.delete('1.0', 'end')
            self.raw_config_text.insert('1.0', pretty + '\n')
            try:
                self.settings_status.set(self.t('settings.raw.pretty_ok'))
            except Exception:
                pass
        except Exception as e:
            messagebox.showerror(self.t('settings.expert'), f"{self.t('settings.raw.parse_error')}:\n{e}")

    def _raw_config_validate(self) -> bool:
        raw = self._raw_config_get_text()
        if not raw:
            messagebox.showwarning(self.t('settings.expert'), self.t('settings.raw.empty'))
            return False
        try:
            obj = json.loads(raw)
        except Exception as e:
            messagebox.showerror(self.t('settings.expert'), f"{self.t('settings.raw.parse_error')}:\n{e}")
            return False
        try:
            tmp_dir = self.project_root / 'data' / 'runtime'
            tmp_dir.mkdir(parents=True, exist_ok=True)
            tmp_path = tmp_dir / f"config_validate_{int(time.time())}.json"
            tmp_path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding='utf-8')
            _ = load_config(tmp_path)
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
            try:
                self.settings_status.set(self.t('settings.raw.valid_ok'))
            except Exception:
                pass
            return True
        except Exception as e:
            messagebox.showerror(self.t('settings.expert'), f"{self.t('settings.raw.invalid')}:\n{e}")
            return False

    def _raw_config_apply(self) -> None:
        if not self._raw_config_validate():
            return
        raw = self._raw_config_get_text()
        try:
            obj = json.loads(raw)
        except Exception:
            return
        try:
            ts = int(time.time())
            bak = Path(self.cfg_path).with_suffix(f".bak.{ts}.json")
            try:
                shutil.copy2(self.cfg_path, bak)
            except Exception:
                pass
        except Exception:
            pass
        try:
            Path(self.cfg_path).write_text(json.dumps(obj, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
        except Exception as e:
            messagebox.showerror(self.t('settings.expert'), f"{self.t('settings.raw.write_error')}:\n{e}")
            return
        try:
            self.cfg = load_config(self.cfg_path)
            self.lang = normalize_lang(getattr(self.cfg.ui, 'language', 'de'))
            self.t = lambda k, **kw: _t(self.lang, k, **kw)
            self.title(f"{self.t('app.title')} {__version__}")
        except Exception as e:
            messagebox.showerror(self.t('settings.expert'), f"{self.t('settings.raw.reload_error')}:\n{e}")
            return
        try:
            if hasattr(self, '_dev_vars'):
                delattr(self, '_dev_vars')
        except Exception:
            pass
        try:
            self._clear_frame(self.tab_settings)
            self._build_settings_tab()
        except Exception:
            pass
        try:
            self.settings_status.set(self.t('settings.raw.applied', path=str(self.cfg_path)))
        except Exception:
            pass
        # Mirror current devices for the web UI (used by /plots)
        try:
            self._write_runtime_devices_meta()
        except Exception:
            pass
        self._safe_reload_after_config_save()



    def _save_settings(self) -> None:
        old_lang = str(getattr(self, 'lang', 'de'))
        # Track devices removed from config so we can archive their data folders.
        old_keys = {d.key for d in getattr(self.cfg, "devices", [])}
        was_live_running = bool(getattr(self, "_live_pollers", []))

        devs: List[DeviceConfig] = []
        for row in self._dev_vars:
            v_key, v_name, v_host, v_emid, v_model, v_kind, v_gen, v_phases, v_emdata = row
            key = v_key.get().strip()
            name = v_name.get().strip() or key
            host = v_host.get().strip()
            try:
                em_id = int(v_emid.get().strip() or "0")
            except Exception:
                em_id = 0
            if key and host:
                kind = str(v_kind.get() or "em").strip().lower()
                if kind not in {"em", "switch", "unknown"}:
                    kind = "em"
                try:
                    gen = int(str(v_gen.get() or "0").strip() or "0")
                except Exception:
                    gen = 0
                model = str(v_model.get() or "").strip()
                try:
                    phases = int(str(v_phases.get() or ("3" if kind == "em" else "1")).strip() or "1")
                except Exception:
                    phases = 3 if kind == "em" else 1
                supports_emdata = bool(v_emdata.get()) if kind == "em" else False
                devs.append(
                    DeviceConfig(
                        key=key,
                        name=name,
                        host=host,
                        em_id=em_id,
                        kind=kind,
                        gen=gen,
                        model=model,
                        phases=phases,
                        supports_emdata=supports_emdata,
                    )
                )
        if not devs:
            messagebox.showerror(self.t('msg.settings'), self.t('settings.need_device'))
            return

        new_keys = {d.key for d in devs}
        removed_keys = sorted(old_keys - new_keys)
        # If live is running and devices changed, stop first so polling threads don't
        # keep referencing removed devices.
        if removed_keys and was_live_running:
            try:
                self._stop_live()
            except Exception:
                pass
        # Download / CSV pack settings
        try:
            chunk_h = int(getattr(self, 'set_download_chunk_h_var', None).get() or 12)
        except Exception:
            chunk_h = int(max(1, int(getattr(self.cfg.download, 'chunk_seconds', 12 * 3600)) // 3600))
        chunk_h = max(1, chunk_h)
        chunk_seconds = int(chunk_h) * 3600

        try:
            overlap_seconds = int(getattr(self, 'set_download_overlap_s_var', None).get() or 60)
        except Exception:
            overlap_seconds = int(getattr(self.cfg.download, 'overlap_seconds', 60))
        overlap_seconds = max(0, overlap_seconds)

        try:
            timeout_seconds = float(getattr(self, 'set_download_timeout_s_var', None).get() or 8.0)
        except Exception:
            timeout_seconds = float(getattr(self.cfg.download, 'timeout_seconds', 8.0))
        timeout_seconds = max(1.0, timeout_seconds)

        try:
            retries = int(getattr(self, 'set_download_retries_var', None).get() or 3)
        except Exception:
            retries = int(getattr(self.cfg.download, 'retries', 3))
        retries = max(0, retries)

        try:
            backoff_base = float(getattr(self, 'set_download_backoff_var', None).get() or 1.5)
        except Exception:
            backoff_base = float(getattr(self.cfg.download, 'backoff_base_seconds', 1.5))
        backoff_base = max(0.0, backoff_base)

        download = DownloadConfig(
            chunk_seconds=chunk_seconds,
            overlap_seconds=overlap_seconds,
            timeout_seconds=timeout_seconds,
            retries=retries,
            backoff_base_seconds=backoff_base,
        )

        try:
            threshold_count = int(getattr(self, 'set_csv_threshold_var', None).get() or 120)
        except Exception:
            threshold_count = int(getattr(self.cfg.csv_pack, 'threshold_count', 120))
        threshold_count = max(1, threshold_count)

        try:
            max_megabytes = int(getattr(self, 'set_csv_max_mb_var', None).get() or 20)
        except Exception:
            max_megabytes = int(getattr(self.cfg.csv_pack, 'max_megabytes', 20))
        max_megabytes = max(1, max_megabytes)

        try:
            remove_merged = bool(getattr(self, 'set_csv_remove_merged_var', None).get())
        except Exception:
            remove_merged = bool(getattr(self.cfg.csv_pack, 'remove_merged', False))

        csv_pack = CsvPackConfig(
            threshold_count=threshold_count,
            max_megabytes=max_megabytes,
            remove_merged=remove_merged,
        )


        # pricing
        try:
            price = float(self.price_var.get().strip().replace(",", "."))
        except Exception:
            price = float(self.cfg.pricing.electricity_price_eur_per_kwh)
        try:
            vat_rate = float(self.vat_rate_var.get().strip().replace(",", "."))
        except Exception:
            vat_rate = float(self.cfg.pricing.vat_rate_percent)
        try:
            base_fee = float(self.base_fee_var.get().strip().replace(",", "."))
        except Exception:
            base_fee = float(getattr(self.cfg.pricing, 'base_fee_eur_per_year', 127.51))
        pricing = PricingConfig(
            electricity_price_eur_per_kwh=price,
            base_fee_eur_per_year=base_fee,
            base_fee_includes_vat=bool(self.base_fee_includes_vat_var.get()),
            price_includes_vat=bool(self.price_includes_vat_var.get()),
            vat_enabled=bool(self.vat_enabled_var.get()),
            vat_rate_percent=vat_rate,
        )
        # UI
        # Language selection
        try:
            sel_lang = str(getattr(self, 'set_language_var', None).get() or getattr(self.cfg.ui, 'language', self.lang) or self.lang).strip().lower()
        except Exception:
            sel_lang = str(getattr(self.cfg.ui, 'language', self.lang) or self.lang).strip().lower()
        # Allow all supported UI languages
        try:
            sel_lang = normalize_lang(sel_lang)
        except Exception:
            sel_lang = 'de'
        # Retention is always at least 120 minutes
        try:
            if getattr(self, 'set_live_retention_var', None) is not None:
                live_retention_min = int(self.set_live_retention_var.get() or 120)
            else:
                live_retention_min = int(getattr(self.cfg.ui, 'live_retention_minutes', 120) or 120)
        except Exception:
            live_retention_min = 120
        live_retention_min = max(120, live_retention_min)

        # Web token (optional)
        try:
            if getattr(self, 'set_live_web_token_var', None) is not None:
                live_web_token = str(self.set_live_web_token_var.get() or '').strip()
            else:
                live_web_token = str(getattr(self.cfg.ui, 'live_web_token', '') or '')
        except Exception:
            live_web_token = str(getattr(self.cfg.ui, 'live_web_token', '') or '')

        # Start page index (0-based)
        try:
            if getattr(self, 'set_start_page_var', None) is not None:
                device_page_index = int(self.set_start_page_var.get() or 1) - 1
            else:
                device_page_index = int(getattr(self.cfg.ui, 'device_page_index', 0) or 0)
        except Exception:
            device_page_index = int(getattr(self.cfg.ui, 'device_page_index', 0) or 0)
        device_page_index = max(0, device_page_index)

        # Live smoothing
        try:
            smoothing_enabled = bool(getattr(self, 'set_live_smoothing_enabled_var', None).get())
        except Exception:
            smoothing_enabled = bool(getattr(self.cfg.ui, 'live_smoothing_enabled', False))
        try:
            smoothing_seconds = int(getattr(self, 'set_live_smoothing_seconds_var', None).get() or 10)
        except Exception:
            smoothing_seconds = int(getattr(self.cfg.ui, 'live_smoothing_seconds', 10))
        smoothing_seconds = max(1, smoothing_seconds)

        try:
            live_poll_s = float(self.set_live_poll_var.get() or 1.0)
        except Exception:
            live_poll_s = float(self.cfg.ui.live_poll_seconds)
        live_poll_s = max(0.2, live_poll_s)
        try:
            live_window_min = int(self.set_live_window_var.get() or 10)
        except Exception:
            live_window_min = int(self.cfg.ui.live_window_minutes)
        live_window_min = max(1, live_window_min)
        try:
            redraw_s = float(self.set_plot_redraw_var.get() or 1.0)
        except Exception:
            redraw_s = float(self.cfg.ui.plot_redraw_seconds)
        redraw_s = max(0.2, redraw_s)

        ui = UiConfig(
            live_poll_seconds=live_poll_s,
            language=sel_lang,
            plot_redraw_seconds=redraw_s,
            live_window_minutes=live_window_min,
            live_retention_minutes=live_retention_min,
            live_web_enabled=bool(self.set_live_web_enabled_var.get()),
            live_web_port=int(self.set_live_web_port_var.get() or 8765),
            live_web_refresh_seconds=float(self.set_live_web_refresh_var.get() or 1.0),
            live_web_token=live_web_token,
            live_smoothing_enabled=smoothing_enabled,
            live_smoothing_seconds=smoothing_seconds,

telegram_enabled=bool(getattr(self, "_tg_enabled_var", tk.BooleanVar(value=False)).get()),
telegram_bot_token=str(getattr(self, "_tg_token_var", tk.StringVar(value="")).get() or ""),
telegram_chat_id=str(getattr(self, "_tg_chatid_var", tk.StringVar(value="")).get() or ""),
telegram_verify_ssl=bool(getattr(self, "_tg_verify_var", tk.BooleanVar(value=True)).get()),
telegram_detail_level=str(
    getattr(self, "_tg_detail_display_to_code", {}).get(
        getattr(self, "_tg_detail_var", tk.StringVar(value="detailed")).get(),
        "detailed",
    )
),
telegram_daily_summary_enabled=bool(getattr(self, "_tg_daily_sum_var", tk.BooleanVar(value=False)).get()),
telegram_daily_summary_time=str(getattr(self, "_tg_daily_time_var", tk.StringVar(value="00:00")).get() or "00:00"),
telegram_monthly_summary_enabled=bool(getattr(self, "_tg_monthly_sum_var", tk.BooleanVar(value=False)).get()),
telegram_monthly_summary_time=str(getattr(self, "_tg_monthly_time_var", tk.StringVar(value="00:00")).get() or "00:00"),
telegram_daily_summary_last_sent=str(getattr(self.cfg.ui, "telegram_daily_summary_last_sent", "") or ""),
telegram_summary_load_w=float(getattr(self, "_tg_sum_loadw_var", tk.StringVar(value="200")).get() or 200.0),
telegram_monthly_summary_last_sent=str(getattr(self.cfg.ui, "telegram_monthly_summary_last_sent", "") or ""),

            autosync_enabled=bool(self.set_autosync_enabled_var.get()),
            autosync_interval_hours=int(self.set_autosync_interval_var.get() or 12),
            autosync_mode=str(self.set_autosync_mode_var.get() or "incremental"),
            device_page_index=device_page_index,
        )
        
        def _lines_from_text(w: tk.Text) -> List[str]:
            try:
                raw = w.get("1.0", "end").splitlines()
            except Exception:
                return []
            return [ln.strip() for ln in raw if ln.strip()]
        issuer = BillingParty(
            name=str(self.bill_issuer_name.get()).strip(),
            address_lines=_lines_from_text(self.bill_issuer_addr),
            vat_id=str(self.bill_issuer_vat_id.get()).strip(),
            email=str(self.bill_issuer_email.get()).strip(),
            phone=str(self.bill_issuer_phone.get()).strip(),
            iban=str(self.bill_issuer_iban.get()).strip(),
            bic=str(self.bill_issuer_bic.get()).strip(),
        )
        customer = BillingParty(
            name=str(self.bill_cust_name.get()).strip(),
            address_lines=_lines_from_text(self.bill_cust_addr),
            vat_id=str(self.bill_cust_vat_id.get()).strip(),
            email=str(self.bill_cust_email.get()).strip(),
            phone=str(self.bill_cust_phone.get()).strip(),
        )
        try:
            payment_terms = int(str(self.bill_payment_terms.get()).strip() or "14")
        except Exception:
            payment_terms = int(self.cfg.billing.payment_terms_days)
        billing = BillingConfig(
            issuer=issuer,
            customer=customer,
            invoice_prefix=str(self.bill_invoice_prefix.get()).strip() or self.cfg.billing.invoice_prefix,
            payment_terms_days=payment_terms,
        )

        # Collect alert rules from the Settings UI (persisted in config.json)
        alerts: List[AlertRule] = []
        try:
            for row in getattr(self, "_alert_vars", []) or []:
                (v_id, v_en, v_dev, v_met, v_op, v_thr, v_dur, v_cd, v_pop, v_beep, v_tg, v_msg) = row
                rid = (v_id.get() or "").strip() or f"rule{len(alerts)+1}"
                dev_disp = (v_dev.get() or "").strip()
                try:
                    devk = str(self._alerts_device_key_from_display(dev_disp) or "*").strip()
                except Exception:
                    devk = dev_disp or "*"
                met = (v_met.get() or "W").strip().upper()
                if met in {"COSΦ", "COS PHI", "COS_PHI"}:
                    met = "COSPHI"
                op = (v_op.get() or ">").strip()
                if op not in {">", "<", ">=", "<=", "=", "==", "=>", "=<"}:
                    op = ">"
                try:
                    thr = float((v_thr.get() or "0").replace(",", "."))
                except Exception:
                    thr = 0.0
                try:
                    dur = int(float((v_dur.get() or "10").replace(",", ".")))
                except Exception:
                    dur = 10
                try:
                    cd = int(float((v_cd.get() or "120").replace(",", ".")))
                except Exception:
                    cd = 120
                alerts.append(
                    AlertRule(
                        rule_id=rid,
                        enabled=bool(v_en.get()),
                        device_key=devk,
                        metric=met,
                        op=op,
                        threshold=thr,
                        duration_seconds=max(0, dur),
                        cooldown_seconds=max(0, cd),
                        action_popup=bool(v_pop.get()),
                        action_beep=bool(v_beep.get()),
                        action_telegram=bool(v_tg.get()),
                        message=str(v_msg.get() or ""),
                    )
                )
        except Exception:
            pass

        self.cfg = AppConfig(
            version=__version__,
            devices=devs,
            download=download,
            csv_pack=csv_pack,
            ui=ui,
            pricing=pricing,
            billing=billing,
            alerts=alerts,
        )
        save_config(self.cfg, self.cfg_path)

        # Archive data folders for removed devices (do not delete) - optional
        if removed_keys:
            do_archive = True
            try:
                do_archive = messagebox.askyesno(
                    self.t('settings.device.removed.title'),
                    self.t('settings.device.removed.ask_archive'),
                    default=messagebox.YES,
                )
            except Exception:
                do_archive = True

            archived = []
            if do_archive:
                for k in removed_keys:
                    try:
                        dst = self.storage.archive_device_data(k)
                    except Exception:
                        dst = None
                    if dst is not None:
                        archived.append((k, str(dst)))

            # Always drop any cached computed/live series for removed devices.
            for k in removed_keys:
                try:
                    self.computed.pop(k, None)
                except Exception:
                    pass
                try:
                    if hasattr(self, '_live_series'):
                        self._live_series.pop(k, None)
                except Exception:
                    pass

            if do_archive and archived:
                try:
                    msg = '\n'.join([f'- {k} -> {p}' for k, p in archived])
                    messagebox.showinfo(
                        self.t('settings.device.removed.title'),
                        self.t('settings.device.removed.archived') + '\n' + msg,
                    )
                except Exception:
                    pass

        self._update_device_page_choices()
        self._rebuild_plots_tab()
        self._rebuild_live_tab()
        # If Live was running before and we stopped it due to removal, restart for the
        # remaining devices so the user keeps getting updates.
        if removed_keys and was_live_running:
            try:
                self._start_live()
            except Exception:
                pass
        try:
            self.settings_status.set(self.t('settings.saved', path=str(self.cfg_path)))
        except Exception:
            self.settings_status.set(f"Saved: {self.cfg_path}")
        # Apply language change immediately
        try:
            if str(old_lang or '').strip().lower() != str(self.cfg.ui.language or '').strip().lower():
                self._apply_language_change(str(self.cfg.ui.language))
        except Exception:
            pass
        # mirror settings into sync autosync controls
        try:
            self.autosync_enabled_var.set(self.cfg.ui.autosync_enabled)
            self.autosync_interval_var.set(self.cfg.ui.autosync_interval_hours)
            self.autosync_mode_var.set(self.cfg.ui.autosync_mode)
            self._on_autosync_toggle()
        except Exception:
            pass
        self._safe_reload_after_config_save()
    def _safe_reload_after_config_save(self) -> None:
        try:
            self._reload_data()
        except Exception:
            pass
    def _reload_settings(self) -> None:
        self.cfg = load_config(self.cfg_path)
        self.lang = normalize_lang(getattr(self.cfg.ui, "language", "de"))
        self.t = lambda k, **kw: _t(self.lang, k, **kw)
        self.title(f"{self.t('app.title')} {__version__}")
        messagebox.showinfo(self.t('msg.settings'), self.t('settings.reload.done') + "\n" + self.t('settings.reload.note'))
    # ---------------- Queue draining ----------------
    def _heartbeat_tick(self) -> None:
        """Write a small heartbeat file so the web dashboard can show Analyzer AN/AUS.

        We avoid using in-memory flags because the web UI may be opened long after
        app startup and should reflect the real runtime state.
        """
        now = time.time()
        last = getattr(self, '_hb_last_write', 0.0) or 0.0
        # write at most every 5s
        if (now - float(last)) < 5.0:
            return
        setattr(self, '_hb_last_write', now)
        try:
            hb = self.project_root / 'data' / 'runtime' / 'analyzer_heartbeat.json'
            hb.parent.mkdir(parents=True, exist_ok=True)
            hb.write_text(json.dumps({'ts': int(now)}), encoding='utf-8')
        except Exception:
            # never break UI loop because of heartbeat
            return

    def _write_runtime_devices_meta(self) -> None:
        """Write device metadata to a runtime file for the web UI.

        The web dashboard may be opened from phones even if the process CWD is
        not the project folder (macOS Finder). Using a runtime file below
        data/runtime avoids relying on config.json discovery.
        """
        try:
            rt = self.project_root / 'data' / 'runtime' / 'devices_meta.json'
            rt.parent.mkdir(parents=True, exist_ok=True)
            meta = []
            for d in list(getattr(self.cfg, 'devices', []) or []):
                try:
                    meta.append({
                        'key': str(getattr(d, 'key', '') or '').strip(),
                        'name': str(getattr(d, 'name', '') or getattr(d, 'key', '') or '').strip(),
                        'kind': str(getattr(d, 'kind', '') or '').strip(),
                        'phases': int(getattr(d, 'phases', 3) or 3),
                    })
                except Exception:
                    continue
            meta = [m for m in meta if m.get('key')]
            rt.write_text(json.dumps({'ts': int(time.time()), 'devices_meta': meta}), encoding='utf-8')
        except Exception:
            return

    def _drain_queues_loop(self) -> None:
        # Sync messages
        while True:
            try:
                msg = self._sync_q.get_nowait()
            except queue.Empty:
                break
            if msg == "__SYNC_DONE__":
                self._log_sync("Sync beendet.")
                try:
                    self._reload_data()
                except Exception:
                    pass
            else:
                self._log_sync(msg)

        # UI commands from web dashboard (freeze, etc.)
        while True:
            try:
                cmd, payload = self._ui_cmd_q.get_nowait()
            except queue.Empty:
                break
            except Exception:
                break

            try:
                if cmd == "set_freeze":
                    desired = bool(payload)
                    if bool(self._live_frozen.get()) != desired:
                        self._live_frozen.set(desired)
                        self._on_live_freeze_toggle()
                elif cmd == "toggle_freeze":
                    self._live_frozen.set(not bool(self._live_frozen.get()))
                    self._on_live_freeze_toggle()
            except Exception:
                pass
        # Autosync
        try:
            self._autosync_tick()
        except Exception:
            pass
        # Live
        try:
            self._live_drain()
        except Exception:
            pass
        # Heartbeat for web status (AN/AUS)
        try:
            self._heartbeat_tick()
        except Exception:
            pass

        # Scheduled Telegram summaries
        try:
            self._telegram_summary_tick()
        except Exception:
            pass
        self.after(500, self._drain_queues_loop)
    # ---------------- mDNS Discovery (Settings -> Devices) ----------------
    def _mdns_refresh_tree(self) -> None:
        try:
            tree = getattr(self, "_mdns_tree", None)
            if not tree:
                return
            # Clear
            for iid in tree.get_children():
                try:
                    tree.delete(iid)
                except Exception:
                    pass

            devs = list(getattr(self, "_mdns_found", []) or [])
            self._mdns_map = {}
            for i, d in enumerate(devs):
                iid = f"mdns_{i}"
                self._mdns_map[iid] = d
                tree.insert(
                    "",
                    "end",
                    iid=iid,
                    values=(
                        getattr(d, "name", "") or "",
                        getattr(d, "host", "") or "",
                        getattr(d, "model", "") or "",
                        str(getattr(d, "gen", 0) or 0),
                        getattr(d, "service_type", "") or "",
                    ),
                )
        except Exception:
            pass

    def _mdns_scan_async(self) -> None:
        if getattr(self, "_mdns_scanning", False):
            return
        self._mdns_scanning = True
        try:
            if getattr(self, "_mdns_status_var", None):
                self._mdns_status_var.set(self.t('settings.mdns.scanning'))
        except Exception:
            pass

        def _worker() -> None:
            err = ""
            devs = []
            try:
                devs = discover_shelly_mdns(timeout_seconds=3.8)
            except Exception as e:
                err = str(e)
                devs = []

            def _done() -> None:
                self._mdns_scanning = False
                try:
                    if err:
                        if getattr(self, "_mdns_status_var", None):
                            self._mdns_status_var.set(f"{self.t('settings.mdns.failed')}: {err}")
                    else:
                        self._mdns_found = devs
                        if getattr(self, "_mdns_status_var", None):
                            if not devs:
                                self._mdns_status_var.set(self.t('settings.mdns.none'))
                            else:
                                self._mdns_status_var.set(self.t('settings.mdns.found_n').format(n=len(devs)))
                    self._mdns_refresh_tree()
                except Exception:
                    pass

            try:
                self.after(0, _done)
            except Exception:
                _done()

        try:
            import threading as _th
            _th.Thread(target=_worker, daemon=True).start()
        except Exception:
            _worker()

    def _mdns_add_selected(self) -> None:
        try:
            tree = getattr(self, "_mdns_tree", None)
            if not tree:
                return
            sel = tree.selection()
            if not sel:
                return
            iid = sel[0]
            dev = getattr(self, "_mdns_map", {}).get(iid)
            if not dev:
                return
            host = getattr(dev, "host", "") or ""
            if not host:
                return
            name_hint = getattr(dev, "name", "") or ""
            self._add_device_from_host(host, name_hint=name_hint)
        except Exception:
            pass


    # -----------------------------
    # Health check / diagnostics
    # -----------------------------
    def _health_check_async(self) -> None:
        if getattr(self, "_health_running", False):
            return
        self._health_running = True
        try:
            self._health_status_var.set(self.t('settings.health.running'))
        except Exception:
            pass
        t = threading.Thread(target=self._health_check_worker, daemon=True)
        t.start()

    def _health_check_worker(self) -> None:
        rows: List[Dict[str, Any]] = []
        try:
            devs = list(getattr(self.cfg, "devices", []) or [])
        except Exception:
            devs = []
        for d in devs:
            host = str(getattr(d, "host", "") or "").strip()
            key = str(getattr(d, "key", "") or "").strip()
            name = str(getattr(d, "name", "") or key).strip()
            if not host:
                continue
            try:
                r = self._health_probe_one(host)
            except Exception as e:
                r = {"tcp_ms": None, "http_ms": None, "model": "", "fw": "", "ok": False, "err": str(e)}
            # live diagnostics
            diag = {}
            try:
                diag = (getattr(self, "_live_diag", {}) or {}).get(key, {}) or {}
            except Exception:
                diag = {}
            last_ok = diag.get("last_ok_ts")
            err_count = diag.get("err_count", 0)
            last_err = diag.get("last_err")
            rows.append(
                {
                    "device": f"{name} ({key})" if key and name and name != key else (name or key or host),
                    "host": host,
                    "tcp_ms": r.get("tcp_ms"),
                    "http_ms": r.get("http_ms"),
                    "model": r.get("model", ""),
                    "fw": r.get("fw", ""),
                    "last_ok": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(last_ok))) if last_ok else "",
                    "err_count": str(err_count) if err_count is not None else "",
                    "last_err": str(last_err or ""),
                }
            )
        self._health_rows = rows

        def _apply() -> None:
            try:
                self._health_refresh_tree()
                self._health_status_var.set(self.t('settings.health.done').format(n=len(rows)))
            except Exception:
                pass
            self._health_running = False

        try:
            self.after(0, _apply)
        except Exception:
            self._health_running = False

    def _health_probe_one(self, host: str) -> Dict[str, Any]:
        out: Dict[str, Any] = {"tcp_ms": None, "http_ms": None, "model": "", "fw": "", "ok": False, "err": ""}
        # TCP connect time (port 80)
        try:
            t0 = time.perf_counter()
            with socket.create_connection((host, 80), timeout=1.5):
                pass
            out["tcp_ms"] = round((time.perf_counter() - t0) * 1000.0, 1)
        except Exception as e:
            out["err"] = f"TCP: {e}"
            return out

        # HTTP probe (prefer Gen2/3 RPC, fallback to Gen1)
        sess = None
        try:
            t0 = time.perf_counter()
            resp = requests.get(f"http://{host}/rpc/Shelly.GetDeviceInfo", timeout=2.5)
            out["http_ms"] = round((time.perf_counter() - t0) * 1000.0, 1)
            if resp.ok:
                j = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
                out["model"] = str(j.get("model", "") or j.get("app", "") or "")
                out["fw"] = str(j.get("fw_id", "") or j.get("ver", "") or j.get("fw", "") or "")
                out["ok"] = True
                return out
        except Exception:
            pass
        try:
            t0 = time.perf_counter()
            resp = requests.get(f"http://{host}/status", timeout=2.5)
            out["http_ms"] = round((time.perf_counter() - t0) * 1000.0, 1)
            if resp.ok:
                j = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
                out["model"] = str(j.get("wifi_sta", {}).get("ssid", "") or j.get("device", {}).get("type", "") or "")
                upd = j.get("update", {}) if isinstance(j.get("update"), dict) else {}
                out["fw"] = str(upd.get("old_version", "") or upd.get("new_version", "") or "")
                out["ok"] = True
        except Exception as e:
            out["err"] = out.get("err") or f"HTTP: {e}"
        return out

    def _health_refresh_tree(self) -> None:
        tree = getattr(self, "_health_tree", None)
        if not tree:
            return
        try:
            for iid in tree.get_children():
                tree.delete(iid)
        except Exception:
            pass
        for r in getattr(self, "_health_rows", []) or []:
            tree.insert(
                "",
                "end",
                values=(
                    r.get("device", ""),
                    r.get("host", ""),
                    "" if r.get("tcp_ms") is None else r.get("tcp_ms"),
                    "" if r.get("http_ms") is None else r.get("http_ms"),
                    r.get("model", ""),
                    r.get("fw", ""),
                    r.get("last_ok", ""),
                    r.get("err_count", ""),
                    r.get("last_err", ""),
                ),
            )

    def _health_copy_to_clipboard(self) -> None:
        rows = getattr(self, "_health_rows", []) or []
        if not rows:
            try:
                messagebox.showinfo(self.t('settings.health.title'), self.t('settings.health.nothing'))
            except Exception:
                pass
            return
        lines = ["device\thost\ttcp_ms\thttp_ms\tmodel\tfw\tlast_ok\terr_count\tlast_err"]
        for r in rows:
            lines.append(
                "\t".join(
                    [
                        str(r.get("device", "")),
                        str(r.get("host", "")),
                        str(r.get("tcp_ms", "")),
                        str(r.get("http_ms", "")),
                        str(r.get("model", "")),
                        str(r.get("fw", "")),
                        str(r.get("last_ok", "")),
                        str(r.get("err_count", "")),
                        str(r.get("last_err", "")),
                    ]
                )
            )
        txt = "\n".join(lines)
        try:
            self.clipboard_clear()
            self.clipboard_append(txt)
            self.update_idletasks()
            messagebox.showinfo(self.t('settings.health.title'), self.t('settings.health.copied'))
        except Exception:
            pass

    # -----------------------------
    # Alerts (simple local rules)
    # -----------------------------
    def _add_alert_row(self) -> None:
        n = len(getattr(self, "_alert_vars", [])) + 1
        if not getattr(self, "_alert_vars", None):
            self._alert_vars = []
        self._alert_vars.append(
            (
                tk.StringVar(value=f"rule{n}"),
                # New rules start disabled so they can be configured first.
                tk.BooleanVar(value=False),
                tk.StringVar(value=f"{self.t('settings.alerts.device_all')} (*)"),
                tk.StringVar(value="W"),
                tk.StringVar(value=">"),
                tk.StringVar(value="1000"),
                tk.StringVar(value="10"),
                tk.StringVar(value="120"),
                tk.BooleanVar(value=True),
                tk.BooleanVar(value=True),
                tk.BooleanVar(value=False),
                tk.StringVar(value=""),
            )
        )
        self._clear_frame(self.tab_settings)
        self._build_settings_tab()

    def _delete_alert_row(self, idx: int) -> None:
        """Delete an alert rule row by index (allows removing rules in the middle)."""
        if not getattr(self, "_alert_vars", None):
            return
        try:
            idx = int(idx)
        except Exception:
            return
        if idx < 0 or idx >= len(self._alert_vars):
            return
        try:
            self._alert_vars.pop(idx)
        except Exception:
            return
        self._clear_frame(self.tab_settings)
        self._build_settings_tab()

    def _remove_alert_row(self) -> None:
        if not getattr(self, "_alert_vars", None):
            return
        # Backwards compatible: remove last (UI uses per-row delete buttons now)
        self._delete_alert_row(len(self._alert_vars) - 1)

    
    def _telegram_send_sync(self, text: str) -> tuple[bool, str]:
        """Send a Telegram message synchronously and return (ok, error_message)."""
        try:
            if not bool(getattr(self.cfg.ui, "telegram_enabled", False)):
                return False, "Telegram ist deaktiviert"
            token = str(getattr(self.cfg.ui, "telegram_bot_token", "") or "").strip()
            chat_id = str(getattr(self.cfg.ui, "telegram_chat_id", "") or "").strip()
            if not token or not chat_id:
                return False, "Bot-Token oder Chat-ID fehlt"
        except Exception as e:
            return False, str(e)

        msg = (text or "").strip()
        if not msg:
            return False, "Leere Nachricht"

        base = f"https://api.telegram.org/bot{token}/sendMessage"
        data = urllib.parse.urlencode({"chat_id": chat_id, "text": msg}).encode("utf-8")
        req = urllib.request.Request(base, data=data, method="POST")
        try:
            verify_ssl = bool(getattr(self.cfg.ui, "telegram_verify_ssl", True))
            ctx = None
            if not verify_ssl:
                try:
                    ctx = ssl._create_unverified_context()
                except Exception:
                    ctx = None
            with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as he:
            try:
                raw = he.read()
            except Exception:
                return False, f"HTTPError: {he}"
        except Exception as e:
            return False, str(e)

        try:
            payload = json.loads(raw.decode("utf-8", errors="replace") or "{}")
            if bool(payload.get("ok", False)):
                return True, ""
            return False, str(payload.get("description") or payload)
        except Exception:
            # Fallback: treat any 200 with no JSON as success? better be conservative
            return False, raw.decode("utf-8", errors="replace")[:200]

    def _alerts_send_telegram(self, text: str) -> None:
        """Send a Telegram message in the background (used by alert rules)."""
        def _worker():
            ok, err = self._telegram_send_sync(text)
            if not ok and err:
                try:
                    logging.getLogger(__name__).warning("Telegram send failed: %s", err)
                except Exception:
                    pass
        try:
            threading.Thread(target=_worker, daemon=True).start()
        except Exception:
            pass

    
    

    # ---------------- Alert rules (Live) ----------------

    def _alerts_format_telegram_message(
        self,
        r: Any,
        s: "LiveSample",
        metric: str,
        val: float,
        op: str,
        thr: float,
        dur: int,
        cd: int,
        base_msg: str = "",
    ) -> str:
        """Build a detailed Telegram alert message (multi-line)."""
        try:
            ts_local = datetime.fromtimestamp(int(getattr(s, "ts", 0) or 0)).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            ts_local = str(getattr(s, "ts", ""))

        devkey = str(getattr(s, "device_key", "") or "")
        devname = str(getattr(s, "device_name", devkey) or devkey)

        # Resolve IP/host from config (optional)
        host = ""
        try:
            for d in getattr(self.cfg, "devices", []) or []:
                if str(getattr(d, "key", "") or "") == devkey:
                    host = str(getattr(d, "host", "") or "")
                    break
        except Exception:
            host = ""

        m = (metric or "").strip().upper()
        phase = ""
        base = m
        if m.endswith("_L1"):
            phase = "L1"; base = m[:-3]
        elif m.endswith("_L2"):
            phase = "L2"; base = m[:-3]
        elif m.endswith("_L3"):
            phase = "L3"; base = m[:-3]

        unit = {"W": "W", "V": "V", "A": "A", "VAR": "var", "COSPHI": ""}.get(base, "")
        title = "🚨 Shelly Alarm"

        rid = str(getattr(r, "rule_id", "") or "")
        if not rid:
            try:
                rid = f"{getattr(r,'device_key','*')}:{getattr(r,'metric','')}"
            except Exception:
                rid = ""

        # Per-phase snapshot (if available)
        phase_line = ""
        try:
            dct = None
            if base == "W":
                dct = getattr(s, "power_w", None)
            elif base == "V":
                dct = getattr(s, "voltage_v", None)
            elif base == "A":
                dct = getattr(s, "current_a", None)
            elif base == "VAR":
                dct = getattr(s, "reactive_var", None)
            elif base == "COSPHI":
                dct = getattr(s, "cosphi", None)

            if isinstance(dct, dict):
                a = float(dct.get("a", 0.0) or 0.0)
                b = float(dct.get("b", 0.0) or 0.0)
                c = float(dct.get("c", 0.0) or 0.0)

                if base == "V":
                    total = (a + b + c) / 3.0 if (a or b or c) else float(dct.get("total", 0.0) or 0.0)
                elif base == "A":
                    total = (a + b + c) if (a or b or c) else float(dct.get("total", 0.0) or 0.0)
                else:
                    total = float(dct.get("total", 0.0) or 0.0)

                if phase:
                    phase_line = (
                        f"Phase: {phase} | "
                        f"L1 {a:g}{(' '+unit) if unit else ''} | "
                        f"L2 {b:g}{(' '+unit) if unit else ''} | "
                        f"L3 {c:g}{(' '+unit) if unit else ''} | "
                        f"Total {total:g}{(' '+unit) if unit else ''}"
                    )
                else:
                    phase_line = (
                        f"L1 {a:g}{(' '+unit) if unit else ''} | "
                        f"L2 {b:g}{(' '+unit) if unit else ''} | "
                        f"L3 {c:g}{(' '+unit) if unit else ''} | "
                        f"Total {total:g}{(' '+unit) if unit else ''}"
                    )
        except Exception:
            phase_line = ""

        val_str = f"{val:g}{(' '+unit) if unit else ''}"
        thr_str = f"{thr:g}{(' '+unit) if unit else ''}"

        lines = [
            title,
            f"Zeit: {ts_local}",
            f"Gerät: {devname} ({devkey})" + (f" @ {host}" if host else ""),
            f"Regel: {metric} {op} {thr_str} (Dauer {int(dur)}s, Cooldown {int(cd)}s)" + (f" | ID {rid}" if rid else ""),
            f"Wert: {val_str}",
        ]
        if phase_line:
            lines.append(f"Phasen: {phase_line}")
        if base_msg:
            lines.append(f"Info: {base_msg}")
        return "\n".join(lines)

    def _alerts_value(self, s: "LiveSample", metric: str) -> float:
        """Return a numeric value for an alert metric."""
        m0 = (metric or "W").strip().upper()
        m = m0.replace(" ", "").replace("Φ", "PHI")

        # Phase mapping
        phase = None
        if m.endswith("_L1"):
            phase = "a"; base = m[:-3]
        elif m.endswith("_L2"):
            phase = "b"; base = m[:-3]
        elif m.endswith("_L3"):
            phase = "c"; base = m[:-3]
        else:
            base = m

        def _mean_abc(d: dict) -> float:
            vals = []
            for k in ("a", "b", "c"):
                try:
                    v = float(d.get(k, 0.0))
                except Exception:
                    v = 0.0
                if v != 0.0:
                    vals.append(v)
            if vals:
                return float(sum(vals) / len(vals))
            try:
                return float(d.get("total", 0.0))
            except Exception:
                return 0.0

        def _sum_abc(d: dict) -> float:
            ssum = 0.0
            ok = False
            for k in ("a", "b", "c"):
                try:
                    v = float(d.get(k, 0.0))
                except Exception:
                    v = 0.0
                if v != 0.0:
                    ok = True
                ssum += v
            if ok:
                return float(ssum)
            try:
                return float(d.get("total", 0.0))
            except Exception:
                return 0.0

        if base in {"W", "P", "POWER"}:
            if phase:
                return float(getattr(s, "power_w", {}).get(phase, 0.0))
            return float(getattr(s, "power_w", {}).get("total", 0.0))

        if base in {"VAR", "Q", "REACTIVE"}:
            if phase:
                return float(getattr(s, "reactive_var", {}).get(phase, 0.0))
            return float(getattr(s, "reactive_var", {}).get("total", 0.0))

        if base in {"COSPHI", "COSPH", "COSP", "COS", "PF", "POWERFACTOR"}:
            if phase:
                return float(getattr(s, "cosphi", {}).get(phase, 0.0))
            return float(getattr(s, "cosphi", {}).get("total", 0.0))

        if base in {"V", "VOLT", "VOLTAGE"}:
            if phase:
                return float(getattr(s, "voltage_v", {}).get(phase, 0.0))
            return _mean_abc(getattr(s, "voltage_v", {}) or {})

        if base in {"A", "AMP", "AMPS", "CURRENT"}:
            if phase:
                return float(getattr(s, "current_a", {}).get(phase, 0.0))
            return _sum_abc(getattr(s, "current_a", {}) or {})

        return float(getattr(s, "power_w", {}).get("total", 0.0))

    def _alerts_process_sample(self, s: "LiveSample") -> None:
        """Evaluate all configured alert rules for a live sample."""
        rules = list(getattr(self.cfg, "alerts", []) or [])
        if not rules:
            return

        for r in rules:
            try:
                if not getattr(r, "enabled", True):
                    continue
                devk = str(getattr(r, "device_key", "*") or "*").strip()
                if devk not in {"*", getattr(s, "device_key", "")}:
                    continue

                rid = str(getattr(r, "rule_id", "") or "")
                if not rid:
                    rid = f"{devk}:{getattr(r,'metric','W')}"

                op = str(getattr(r, "op", ">") or ">").strip()
                thr = float(getattr(r, "threshold", 0.0) or 0.0)
                dur = int(getattr(r, "duration_seconds", 10) or 0)
                cd = int(getattr(r, "cooldown_seconds", 120) or 0)

                metric = str(getattr(r, "metric", "W") or "W")
                val = self._alerts_value(s, metric)

                cond = (
                    (val > thr) if op == ">" else
                    (val < thr) if op == "<" else
                    (val >= thr) if op in {">=", "=>"} else
                    (val <= thr) if op in {"<=", "=<"} else
                    (val == thr) if op in {"=", "=="} else
                    (val > thr)
                )

                st = self._alert_state.setdefault(rid, {"start_ts": None, "triggered": False, "last_trigger_ts": 0})

                if not cond:
                    st["start_ts"] = None
                    st["triggered"] = False
                    continue

                if st["start_ts"] is None:
                    st["start_ts"] = int(getattr(s, "ts", 0) or 0)

                if st.get("triggered"):
                    continue

                now_ts = int(getattr(s, "ts", 0) or 0)
                if dur > 0 and (now_ts - int(st["start_ts"] or now_ts)) < dur:
                    continue
                if cd > 0 and (now_ts - int(st.get("last_trigger_ts", 0) or 0)) < cd:
                    continue

                st["triggered"] = True
                st["last_trigger_ts"] = now_ts

                msg_custom = str(getattr(r, "message", "") or "").strip()
                devname = str(getattr(s, "device_name", getattr(s, "device_key", "")) or getattr(s, "device_key", ""))
                msg = msg_custom or self.t("settings.alerts.fired").format(
                    device=devname, metric=metric, value=val, op=op, threshold=thr
                )

                if bool(getattr(r, "action_telegram", False)):
                    try:
                        level = str(getattr(self.cfg.ui, "telegram_detail_level", "detailed") or "detailed").strip().lower()
                        tg_msg = msg if level == "simple" else self._alerts_format_telegram_message(
                            r, s, metric, val, op, thr, dur, cd, msg_custom or ""
                        )
                        self._alerts_send_telegram(tg_msg)
                    except Exception:
                        pass

                if bool(getattr(r, "action_beep", True)):
                    try:
                        self.bell()
                    except Exception:
                        pass

                if bool(getattr(r, "action_popup", True)):
                    try:
                        messagebox.showwarning(self.t("settings.alerts.title"), msg)
                    except Exception:
                        pass

            except Exception:
                continue


# ---------------- Telegram scheduled summaries ----------------

    def _parse_hhmm(self, s: str) -> tuple[int, int]:
        try:
            s = (s or "").strip()
            m = re.match(r"^(\d{1,2})\s*:\s*(\d{1,2})$", s)
            if not m:
                return 0, 0
            hh = max(0, min(23, int(m.group(1))))
            mm = max(0, min(59, int(m.group(2))))
            return hh, mm
        except Exception:
            return 0, 0


    # ---------------- Telegram scheduled summaries (App methods) ----------------

    def _telegram_summary_tick(self) -> None:
        """Check whether a daily/monthly summary is due and send it.

        The configured HH:MM is treated as the *window boundary*.
        - Daily: sends the last complete 24h window ending at today's boundary.
        - Monthly: sends the last complete month window ending at this month's boundary.
        """
        try:
            return _telegram_summary_tick(self)  # module-level implementation
        except Exception:
            return None

    def _telegram_send_summary_daily(self, start_dt: "datetime", end_dt: "datetime",
                                    mark_sent: bool = True, sent_key: str = "") -> None:
        return _telegram_send_summary_daily(self, start_dt, end_dt, mark_sent=mark_sent, sent_key=sent_key)

    def _telegram_send_summary_month(self, start_dt: "datetime", end_dt: "datetime",
                                     mark_sent: bool = True, sent_key: str = "") -> None:
        return _telegram_send_summary_month(self, start_dt, end_dt, mark_sent=mark_sent, sent_key=sent_key)

    def _build_telegram_summary(self, kind: str, start: "datetime", end: "datetime") -> str:
        return _build_telegram_summary(self, kind, start, end)

    def _telegram_send_daily_summary_now(self) -> None:
        """Send the last complete daily window immediately (does not mark as sent)."""
        if not bool(getattr(self.cfg.ui, "telegram_enabled", False)):
            try:
                self._show_msgbox(self.t("settings.alerts.telegram.disabled"), kind="warning")
            except Exception:
                pass
            return

        try:
            now = datetime.now()
            hh, mm = self._parse_hhmm(str(getattr(self.cfg.ui, "telegram_daily_summary_time", "00:00") or "00:00"))
            boundary = datetime.combine(now.date(), datetime.min.time()).replace(hour=hh, minute=mm)
            if now < boundary:
                boundary -= timedelta(days=1)
            end_dt = boundary
            start_dt = end_dt - timedelta(days=1)

            msg = self._build_telegram_summary("daily", start_dt, end_dt)
            ok, err = self._telegram_send_sync(msg)
            if ok:
                self._show_msgbox(self.t("settings.alerts.telegram.sent_ok"), kind="info")
            else:
                self._show_msgbox(f"{self.t('settings.alerts.telegram.sent_fail')}: {err}", kind="warning")
        except Exception as e:
            try:
                self._show_msgbox(f"{self.t('settings.alerts.telegram.sent_fail')}: {e}", kind="warning")
            except Exception:
                pass

    def _telegram_send_monthly_summary_now(self) -> None:
        """Send the last complete monthly window immediately (does not mark as sent)."""
        if not bool(getattr(self.cfg.ui, "telegram_enabled", False)):
            try:
                self._show_msgbox(self.t("settings.alerts.telegram.disabled"), kind="warning")
            except Exception:
                pass
            return

        try:
            now = datetime.now()
            hh, mm = self._parse_hhmm(str(getattr(self.cfg.ui, "telegram_monthly_summary_time", "00:00") or "00:00"))

            boundary = datetime(now.year, now.month, 1, hh, mm, 0)
            if now < boundary:
                if now.month == 1:
                    y, m = now.year - 1, 12
                else:
                    y, m = now.year, now.month - 1
                boundary = datetime(y, m, 1, hh, mm, 0)

            y, m = boundary.year, boundary.month
            if m == 1:
                y0, m0 = y - 1, 12
            else:
                y0, m0 = y, m - 1

            start_dt = datetime(y0, m0, 1, hh, mm, 0)
            end_dt = boundary

            msg = self._build_telegram_summary("monthly", start_dt, end_dt)
            ok, err = self._telegram_send_sync(msg)
            if ok:
                self._show_msgbox(self.t("settings.alerts.telegram.sent_ok"), kind="info")
            else:
                self._show_msgbox(f"{self.t('settings.alerts.telegram.sent_fail')}: {err}", kind="warning")
        except Exception as e:
            try:
                self._show_msgbox(f"{self.t('settings.alerts.telegram.sent_fail')}: {e}", kind="warning")
            except Exception:
                pass

    def _telegram_update_summary_status_ui(self) -> None:
        """Update UI labels with active state, countdown, and last send result."""
        try:
            if not hasattr(self, "_tg_daily_status_var") or not hasattr(self, "_tg_monthly_status_var"):
                return

            tg_on = bool(getattr(self.cfg.ui, "telegram_enabled", False))
            now = datetime.now()

            def _fmt_td(seconds: int) -> str:
                if seconds < 0:
                    seconds = 0
                h = seconds // 3600
                m = (seconds % 3600) // 60
                s = seconds % 60
                return f"{h:02d}:{m:02d}:{s:02d}"

            def _fmt_dt(ts: int) -> str:
                try:
                    return datetime.fromtimestamp(int(ts)).strftime("%d.%m %H:%M")
                except Exception:
                    return ""

            # read persistent send-state (non-fatal)
            state = {}
            try:
                base_dir = getattr(getattr(self, "storage", None), "base_dir", None) or "."
                p_state = Path(base_dir) / "data" / "telegram_summary_state.json"
                if p_state.exists():
                    state = json.loads(p_state.read_text(encoding="utf-8")) or {}
            except Exception:
                state = {}

            def _st(k: str, default: str = "") -> str:
                try:
                    v = state.get(k, default)
                    return str(v) if v is not None else default
                except Exception:
                    return default

            def _last_attempt(prefix: str) -> str:
                try:
                    ts = int(float(_st(f"{prefix}_last_attempt_ts", "0") or 0))
                except Exception:
                    ts = 0
                if not ts:
                    return ""
                ok = _st(f"{prefix}_last_attempt_ok", "")
                err = _st(f"{prefix}_last_attempt_err", "")
                icon = "✅" if ok == "1" else "❌" if ok == "0" else "•"
                extra = ""
                if icon == "❌" and err:
                    # keep it short for the UI line
                    extra = f" ({err[:60]})"
                return f" · letzter Versuch: {_fmt_dt(ts)} {icon}{extra}"

            # grace windows
            daily_grace_s = int(getattr(self.cfg.ui, "telegram_daily_summary_grace_seconds", 7200) or 7200)
            monthly_grace_s = int(getattr(self.cfg.ui, "telegram_monthly_summary_grace_seconds", 86400) or 86400)

            # --- daily ---
            if tg_on and bool(getattr(self.cfg.ui, "telegram_daily_summary_enabled", False)):
                try:
                    hh, mm = self._parse_hhmm(str(getattr(self.cfg.ui, "telegram_daily_summary_time", "00:00") or "00:00"))
                    boundary = datetime.combine(now.date(), datetime.min.time()).replace(hour=hh, minute=mm, second=0, microsecond=0)
                    if now < boundary:
                        next_t = boundary
                        msg = f"✅ Aktiv · nächstes Senden in {_fmt_td(int((next_t-now).total_seconds()))} ({next_t.strftime('%d.%m %H:%M')})"
                    elif now <= boundary + timedelta(seconds=daily_grace_s):
                        msg = f"✅ Aktiv · fällig (Fenster bis {(boundary + timedelta(seconds=daily_grace_s)).strftime('%d.%m %H:%M')})"
                    else:
                        next_t = boundary + timedelta(days=1)
                        msg = f"✅ Aktiv · nächstes Senden in {_fmt_td(int((next_t-now).total_seconds()))} ({next_t.strftime('%d.%m %H:%M')})"
                except Exception:
                    msg = "⚠️ Aktiv · Zeit ungültig"
            else:
                msg = "⏸️ Aus" if not tg_on else "⏸️ Aus"
            msg += _last_attempt("daily")
            self._tg_daily_status_var.set(msg)

            # --- monthly ---
            if tg_on and bool(getattr(self.cfg.ui, "telegram_monthly_summary_enabled", False)):
                try:
                    hh, mm = self._parse_hhmm(str(getattr(self.cfg.ui, "telegram_monthly_summary_time", "00:00") or "00:00"))

                    # next send is 1st day of next month at hh:mm (or this month if not reached yet)
                    boundary = datetime(now.year, now.month, 1, hh, mm, 0, 0)
                    if now < boundary:
                        next_t = boundary
                    else:
                        if now.month == 12:
                            ny, nm = now.year + 1, 1
                        else:
                            ny, nm = now.year, now.month + 1
                        next_t = datetime(ny, nm, 1, hh, mm, 0, 0)

                    # if we are on boundary day and within grace window, show 'fällig'
                    if now.date() == boundary.date() and now >= boundary and now <= (boundary + timedelta(seconds=monthly_grace_s)):
                        msgm = f"✅ Aktiv · fällig (Fenster bis {(boundary + timedelta(seconds=monthly_grace_s)).strftime('%d.%m %H:%M')})"
                    else:
                        msgm = f"✅ Aktiv · nächstes Senden in {_fmt_td(int((next_t-now).total_seconds()))} ({next_t.strftime('%d.%m %H:%M')})"
                except Exception:
                    msgm = "⚠️ Aktiv · Zeit ungültig"
            else:
                msgm = "⏸️ Aus" if not tg_on else "⏸️ Aus"
            msgm += _last_attempt("month")
            self._tg_monthly_status_var.set(msgm)

            # re-arm timer (once per second)
            try:
                if hasattr(self, "_tg_status_after_id") and self._tg_status_after_id:
                    self.after_cancel(self._tg_status_after_id)
            except Exception:
                pass
            try:
                self._tg_status_after_id = self.after(1000, self._telegram_update_summary_status_ui)
            except Exception:
                pass
        except Exception:
            pass
def _telegram_summary_tick(self) -> None:
    """Send scheduled daily/monthly Telegram summaries without spamming.

    Behavior:
    - Uses a *grace window* after the configured boundary time (e.g. 00:00 + 2h).
    - When (re-)enabling, it will NOT send retroactively for an already-passed boundary.
      Instead it arms the *next* boundary and sends then.
    - Retries inside the grace window (daily: 60s default, monthly: 300s default).
    - Stores markers/results in data/telegram_summary_state.json.
    """
    if not bool(getattr(self.cfg.ui, "telegram_enabled", False)):
        return

    # --- persistent state (data/telegram_summary_state.json) ---
    try:
        base_dir = getattr(getattr(self, "storage", None), "base_dir", None)
        if not base_dir:
            base_dir = "."
        p_state = Path(base_dir) / "data" / "telegram_summary_state.json"
        p_state.parent.mkdir(parents=True, exist_ok=True)

        state = getattr(self, "_tg_summary_state", None)
        if not isinstance(state, dict):
            if p_state.exists():
                try:
                    state = json.loads(p_state.read_text(encoding="utf-8")) or {}
                except Exception:
                    state = {}
            else:
                state = {}
            self._tg_summary_state = state
    except Exception:
        p_state = None
        state = getattr(self, "_tg_summary_state", None)
        if not isinstance(state, dict):
            state = {}
            self._tg_summary_state = state

    def _state_get(k: str, default: str = "") -> str:
        try:
            v = state.get(k, default)
            return str(v) if v is not None else default
        except Exception:
            return default

    def _state_set(k: str, v: str) -> None:
        try:
            state[k] = v
        except Exception:
            return
        try:
            if p_state is not None:
                p_state.write_text(json.dumps(state, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        except Exception:
            pass

    now = datetime.now()
    now_ts = int(time.time())

    # Grace windows (seconds) – prevents midday spam when enabling late.
    daily_grace_s = int(getattr(self.cfg.ui, "telegram_daily_summary_grace_seconds", 7200) or 7200)   # 2h
    monthly_grace_s = int(getattr(self.cfg.ui, "telegram_monthly_summary_grace_seconds", 86400) or 86400)  # 24h

    # ---------------------------
    # Arm logic (enable/disable)
    # ---------------------------
    # Daily
    daily_enabled = bool(getattr(self.cfg.ui, "telegram_daily_summary_enabled", False))
    try:
        hh_d, mm_d = self._parse_hhmm(str(getattr(self.cfg.ui, "telegram_daily_summary_time", "00:00") or "00:00"))
    except Exception:
        hh_d, mm_d = 0, 0

    b_today = datetime.combine(now.date(), datetime.min.time()).replace(hour=hh_d, minute=mm_d, second=0, microsecond=0)
    next_daily_boundary = b_today if now < b_today else (b_today + timedelta(days=1))
    next_daily_key = f"{next_daily_boundary.strftime('%Y-%m-%d')}_{hh_d:02d}{mm_d:02d}"

    prev_daily_enabled = (_state_get("daily_enabled", "0") == "1")
    if daily_enabled and not prev_daily_enabled:
        # (re-)enabled: arm next boundary only (no retroactive send)
        _state_set("daily_enabled", "1")
        _state_set("daily_arm", next_daily_key)
    elif (not daily_enabled) and prev_daily_enabled:
        _state_set("daily_enabled", "0")

    # Monthly
    month_enabled = bool(getattr(self.cfg.ui, "telegram_monthly_summary_enabled", False))
    try:
        hh_m, mm_m = self._parse_hhmm(str(getattr(self.cfg.ui, "telegram_monthly_summary_time", "00:00") or "00:00"))
    except Exception:
        hh_m, mm_m = 0, 0

    this_month_boundary = datetime(now.year, now.month, 1, hh_m, mm_m, 0)
    if now < this_month_boundary:
        next_month_boundary = this_month_boundary
    else:
        # first day of next month
        if now.month == 12:
            ny, nm = now.year + 1, 1
        else:
            ny, nm = now.year, now.month + 1
        next_month_boundary = datetime(ny, nm, 1, hh_m, mm_m, 0)

    # key refers to *previous month* for the window [prev_month_start .. due_end)
    yk, mk = next_month_boundary.year, next_month_boundary.month
    if mk == 1:
        y0k, m0k = yk - 1, 12
    else:
        y0k, m0k = yk, mk - 1
    next_month_key = f"{y0k:04d}-{m0k:02d}_{hh_m:02d}{mm_m:02d}"

    prev_month_enabled = (_state_get("month_enabled", "0") == "1")
    if month_enabled and not prev_month_enabled:
        _state_set("month_enabled", "1")
        _state_set("month_arm", next_month_key)
    elif (not month_enabled) and prev_month_enabled:
        _state_set("month_enabled", "0")

    # ---------------------------
    # Daily summary send
    # ---------------------------
    if daily_enabled:
        hh, mm = hh_d, mm_d
        # most recent boundary (end of window)
        due_end = b_today
        if now < due_end:
            due_end -= timedelta(days=1)

        key = f"{due_end.strftime('%Y-%m-%d')}_{hh:02d}{mm:02d}"
        arm = _state_get("daily_arm", "") or next_daily_key  # safe default: next boundary
        last_handled = _state_get("daily_last", "")

        attempt_key = _state_get("daily_attempt_key", "")
        try:
            attempt_ts = int(float(_state_get("daily_attempt_ts", "0") or 0))
        except Exception:
            attempt_ts = 0
        retry_every_s = int(getattr(self.cfg.ui, "telegram_daily_summary_retry_seconds", 60) or 60)

        if now >= due_end:
            if now <= (due_end + timedelta(seconds=daily_grace_s)):
                # Only send if this boundary is armed (prevents retroactive sends after enabling)
                if arm == key and last_handled != key:
                    if attempt_key == key and attempt_ts and (now_ts - attempt_ts) < retry_every_s:
                        pass
                    else:
                        start_dt = due_end - timedelta(days=1)
                        end_dt = due_end
                        try:
                            msg = self._build_telegram_summary("daily", start_dt, end_dt)
                            ok, err = self._telegram_send_sync(msg)
                        except Exception as e:
                            ok, err = False, str(e)

                        # always store last attempt result (for UI)
                        _state_set("daily_last_attempt_key", key)
                        _state_set("daily_last_attempt_ts", str(now_ts))
                        _state_set("daily_last_attempt_ok", "1" if ok else "0")
                        _state_set("daily_last_attempt_err", (err or "")[:400])

                        if ok:
                            _state_set("daily_last", key)
                            _state_set("daily_attempt_key", "")
                            _state_set("daily_attempt_ts", "0")
                            _state_set("daily_last_sent_key", key)
                            _state_set("daily_last_sent_ts", str(now_ts))
                            _state_set("daily_last_sent_ok", "1")
                            _state_set("daily_last_sent_err", "")
                            # arm next boundary
                            next_due = due_end + timedelta(days=1)
                            _state_set("daily_arm", f"{next_due.strftime('%Y-%m-%d')}_{hh:02d}{mm:02d}")
                        else:
                            _state_set("daily_attempt_key", key)
                            _state_set("daily_attempt_ts", str(now_ts))
                            if err:
                                try:
                                    self.log.warning("Telegram daily summary failed: %s", err)
                                except Exception:
                                    pass
            else:
                # Too late -> do not send retroactively. If this boundary was armed, advance arm.
                if arm == key:
                    next_due = due_end + timedelta(days=1)
                    _state_set("daily_arm", f"{next_due.strftime('%Y-%m-%d')}_{hh:02d}{mm:02d}")
                # mark handled to avoid re-evaluating the same past boundary forever
                if last_handled != key:
                    _state_set("daily_last", key)

    # ---------------------------
    # Monthly summary send
    # ---------------------------
    if month_enabled:
        hh, mm = hh_m, mm_m
        due_end = this_month_boundary
        if now < due_end:
            # previous month boundary
            if now.month == 1:
                y, m_ = now.year - 1, 12
            else:
                y, m_ = now.year, now.month - 1
            due_end = datetime(y, m_, 1, hh, mm, 0)

        last_handled = _state_get("month_last", "")

        # key is previous month for this due_end
        y, m_ = due_end.year, due_end.month
        if m_ == 1:
            y0, m0 = y - 1, 12
        else:
            y0, m0 = y, m_ - 1
        key = f"{y0:04d}-{m0:02d}_{hh:02d}{mm:02d}"

        arm = _state_get("month_arm", "") or next_month_key

        attempt_key = _state_get("month_attempt_key", "")
        try:
            attempt_ts = int(float(_state_get("month_attempt_ts", "0") or 0))
        except Exception:
            attempt_ts = 0
        retry_every_s = int(getattr(self.cfg.ui, "telegram_monthly_summary_retry_seconds", 300) or 300)

        if now >= due_end:
            within_boundary_day = (now.date() == due_end.date())
            if within_boundary_day and now <= (due_end + timedelta(seconds=monthly_grace_s)):
                if arm == key and last_handled != key:
                    if attempt_key == key and attempt_ts and (now_ts - attempt_ts) < retry_every_s:
                        pass
                    else:
                        # window is previous month
                        start_dt = datetime(y0, m0, 1, hh, mm, 0)
                        end_dt = due_end
                        try:
                            msg = self._build_telegram_summary("month", start_dt, end_dt)
                            ok, err = self._telegram_send_sync(msg)
                        except Exception as e:
                            ok, err = False, str(e)

                        _state_set("month_last_attempt_key", key)
                        _state_set("month_last_attempt_ts", str(now_ts))
                        _state_set("month_last_attempt_ok", "1" if ok else "0")
                        _state_set("month_last_attempt_err", (err or "")[:400])

                        if ok:
                            _state_set("month_last", key)
                            _state_set("month_attempt_key", "")
                            _state_set("month_attempt_ts", "0")
                            _state_set("month_last_sent_key", key)
                            _state_set("month_last_sent_ts", str(now_ts))
                            _state_set("month_last_sent_ok", "1")
                            _state_set("month_last_sent_err", "")
                            # arm next month key
                            if due_end.month == 12:
                                ny, nm = due_end.year + 1, 1
                            else:
                                ny, nm = due_end.year, due_end.month + 1
                            next_due = datetime(ny, nm, 1, hh, mm, 0)
                            yk, mk = next_due.year, next_due.month
                            if mk == 1:
                                y0k, m0k = yk - 1, 12
                            else:
                                y0k, m0k = yk, mk - 1
                            _state_set("month_arm", f"{y0k:04d}-{m0k:02d}_{hh:02d}{mm:02d}")
                        else:
                            _state_set("month_attempt_key", key)
                            _state_set("month_attempt_ts", str(now_ts))
                            if err:
                                try:
                                    self.log.warning("Telegram monthly summary failed: %s", err)
                                except Exception:
                                    pass
            else:
                # prevent retroactive mid-month sends when enabling: advance arm if needed
                if arm == key and not within_boundary_day:
                    # compute next key from next month boundary
                    if due_end.month == 12:
                        ny, nm = due_end.year + 1, 1
                    else:
                        ny, nm = due_end.year, due_end.month + 1
                    next_due = datetime(ny, nm, 1, hh, mm, 0)
                    yk, mk = next_due.year, next_due.month
                    if mk == 1:
                        y0k, m0k = yk - 1, 12
                    else:
                        y0k, m0k = yk, mk - 1
                    _state_set("month_arm", f"{y0k:04d}-{m0k:02d}_{hh:02d}{mm:02d}")

                if last_handled != key and now.date() != due_end.date():
                    _state_set("month_last", key)

    def _state_get(k: str, default: str = "") -> str:
        try:
            v = state.get(k, default)
            return str(v) if v is not None else default
        except Exception:
            return default

    def _state_set(k: str, v: str) -> None:
        try:
            state[k] = v
        except Exception:
            return
        try:
            if p_state is not None:
                p_state.write_text(json.dumps(state, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        except Exception:
            pass

    now = datetime.now()

    # Grace windows (seconds) – prevents midday spam when enabling late.
    daily_grace_s = int(getattr(self.cfg.ui, "telegram_daily_summary_grace_seconds", 7200) or 7200)   # 2h
    monthly_grace_s = int(getattr(self.cfg.ui, "telegram_monthly_summary_grace_seconds", 86400) or 86400)  # 24h

    # --- Daily summary ---
    if bool(getattr(self.cfg.ui, "telegram_daily_summary_enabled", False)):
        hh, mm = self._parse_hhmm(str(getattr(self.cfg.ui, "telegram_daily_summary_time", "00:00") or "00:00"))

        due_end = datetime.combine(now.date(), datetime.min.time()).replace(hour=hh, minute=mm, second=0, microsecond=0)
        key = f"{due_end.strftime('%Y-%m-%d')}_{hh:02d}{mm:02d}"
        last = _state_get("daily_last", "")

        # simple retry-backoff to avoid spamming within the grace window
        attempt_key = _state_get("daily_attempt_key", "")
        try:
            attempt_ts = int(float(_state_get("daily_attempt_ts", "0") or 0))
        except Exception:
            attempt_ts = 0
        retry_every_s = int(getattr(self.cfg.ui, "telegram_daily_summary_retry_seconds", 60) or 60)

        if now >= due_end:
            if now <= (due_end + timedelta(seconds=daily_grace_s)):
                if last != key:
                    now_ts = int(time.time())
                    if attempt_key == key and attempt_ts and (now_ts - attempt_ts) < retry_every_s:
                        pass
                    else:
                        start_dt = due_end - timedelta(days=1)
                        end_dt = due_end
                        try:
                            msg = self._build_telegram_summary("daily", start_dt, end_dt)
                            ok, err = self._telegram_send_sync(msg)
                        except Exception as e:
                            ok, err = False, str(e)
                        if ok:
                            _state_set("daily_last", key)
                            _state_set("daily_attempt_key", "")
                            _state_set("daily_attempt_ts", "0")
                        else:
                            _state_set("daily_attempt_key", key)
                            _state_set("daily_attempt_ts", str(int(time.time())))
                            if err:
                                try:
                                    self.log.warning("Telegram daily summary failed: %s", err)
                                except Exception:
                                    pass
            else:
                # Too late -> do not send retroactively.
                if last != key:
                    _state_set("daily_last", key)

    # --- Monthly summary ---
    if bool(getattr(self.cfg.ui, "telegram_monthly_summary_enabled", False)):
        hh, mm = self._parse_hhmm(str(getattr(self.cfg.ui, "telegram_monthly_summary_time", "00:00") or "00:00"))

        due_end = datetime(now.year, now.month, 1, hh, mm, 0)
        last = _state_get("month_last", "")

        # previous month key for the window [prev_month_start .. due_end)
        y, m_ = due_end.year, due_end.month
        if m_ == 1:
            y0, m0 = y - 1, 12
        else:
            y0, m0 = y, m_ - 1
        key = f"{y0:04d}-{m0:02d}_{hh:02d}{mm:02d}"

        attempt_key = _state_get("month_attempt_key", "")
        try:
            attempt_ts = int(float(_state_get("month_attempt_ts", "0") or 0))
        except Exception:
            attempt_ts = 0
        retry_every_s = int(getattr(self.cfg.ui, "telegram_monthly_summary_retry_seconds", 300) or 300)

        if now >= due_end:
            within_boundary_day = (now.date() == due_end.date())
            if within_boundary_day and now <= (due_end + timedelta(seconds=monthly_grace_s)):
                if last != key:
                    now_ts = int(time.time())
                    if attempt_key == key and attempt_ts and (now_ts - attempt_ts) < retry_every_s:
                        pass
                    else:
                        start_dt = datetime(y0, m0, 1, hh, mm, 0)
                        end_dt = due_end
                        try:
                            msg = self._build_telegram_summary("monthly", start_dt, end_dt)
                            ok, err = self._telegram_send_sync(msg)
                        except Exception as e:
                            ok, err = False, str(e)
                        if ok:
                            _state_set("month_last", key)
                            _state_set("month_attempt_key", "")
                            _state_set("month_attempt_ts", "0")
                        else:
                            _state_set("month_attempt_key", key)
                            _state_set("month_attempt_ts", str(int(time.time())))
                            if err:
                                try:
                                    self.log.warning("Telegram monthly summary failed: %s", err)
                                except Exception:
                                    pass
            else:
                # prevent retroactive mid-month sends when enabling
                if last != key and now.date() != due_end.date():
                    _state_set("month_last", key)



def _telegram_send_summary_daily(self, start_dt: datetime, end_dt: datetime, mark_sent: bool = True, sent_key: str = "") -> None:
    msg = self._build_telegram_summary("daily", start_dt, end_dt)
    ok, err = self._telegram_send_sync(msg)
    if ok and mark_sent:
        key = sent_key or start_dt.strftime("%Y-%m-%d")
        try:
            self.cfg.ui = replace(self.cfg.ui, telegram_daily_summary_last_sent=key)
        except Exception:
            try:
                setattr(self.cfg.ui, "telegram_daily_summary_last_sent", key)
            except Exception:
                pass
    if (not ok) and err:
        try:
            self.log.warning("Telegram daily summary failed: %s", err)
        except Exception:
            pass


def _telegram_send_summary_month(self, start_dt: datetime, end_dt: datetime, mark_sent: bool = True, sent_key: str = "") -> None:
    msg = self._build_telegram_summary("monthly", start_dt, end_dt)
    ok, err = self._telegram_send_sync(msg)
    if ok and mark_sent:
        key = sent_key or start_dt.strftime("%Y-%m")
        try:
            self.cfg.ui = replace(self.cfg.ui, telegram_monthly_summary_last_sent=key)
        except Exception:
            try:
                setattr(self.cfg.ui, "telegram_monthly_summary_last_sent", key)
            except Exception:
                pass
    if (not ok) and err:
        try:
            self.log.warning("Telegram monthly summary failed: %s", err)
        except Exception:
            pass


def _build_telegram_summary(self, kind: str, start: datetime, end: datetime) -> str:
    """Build a detailed summary message for a time range.

    Notes:
    - kWh are computed from per-interval energy if present; otherwise we integrate power over time.
    - Worst cosφ and max |VAR| are only evaluated when load (W) is above a configurable threshold
      to avoid misleading idle-time values.
    """
    from shelly_analyzer.core.energy import calculate_energy

    # normalize range
    start_dt = pd.Timestamp(start)
    end_dt = pd.Timestamp(end)

    # threshold for cosφ/VAR statistics
    try:
        load_thr_w = float(getattr(self.cfg.ui, "telegram_summary_load_w", 200.0) or 200.0)
    except Exception:
        load_thr_w = 200.0

    total_kwh = 0.0
    per_dev_kwh = []  # (name, key, kwh)

    # hourly energy across ALL devices
    hourly_kwh = None  # pd.Series indexed by hour timestamp

    # Power stats (overall)
    sum_power = 0.0
    cnt_power = 0
    min_power_w = None

    max_power_dev = None
    max_power = 0.0
    max_power_ts = None

    worst_pf_dev = None
    worst_pf_val = None
    worst_pf_ts = None

    max_abs_var_dev = None
    max_abs_var = 0.0
    max_abs_var_ts = None

    # Voltage / Current (overall)
    min_v = None
    max_v = None
    max_a = None

    # Price (optional)
    price_eur = None
    try:
        price_eur = float(getattr(self.cfg.ui, "price_per_kwh", None))
    except Exception:
        price_eur = None

    # iterate devices
    for d in list(getattr(self.cfg, "devices", []) or []):
        try:
            df = self.storage.read_device_df(d.key)
        except Exception:
            continue
        if df is None or getattr(df, "empty", True):
            continue

        # Ensure timestamp column
        try:
            if "timestamp" not in df.columns:
                # try index
                t_idx = pd.to_datetime(getattr(df, "index", None), errors="coerce")
                df = df.copy()
                df["timestamp"] = t_idx
            else:
                df = df.copy()
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        except Exception:
            continue

        df = df.dropna(subset=["timestamp"])
        if df.empty:
            continue

        # filter range
        try:
            m = (df["timestamp"] >= start_dt) & (df["timestamp"] < end_dt)
            df_use = df.loc[m].copy()
        except Exception:
            df_use = df.copy()

        if df_use is None or df_use.empty:
            per_dev_kwh.append((d.name, d.key, 0.0))
            continue

        # Compute per-interval kWh + total_power
        try:
            df_e = calculate_energy(df_use, method="auto")
        except Exception:
            df_e = df_use.copy()

        # kWh
        try:
            if "energy_kwh" in df_e.columns:
                kwh = float(pd.to_numeric(df_e["energy_kwh"], errors="coerce").fillna(0.0).sum())
            else:
                kwh = 0.0
        except Exception:
            kwh = 0.0

        total_kwh += kwh
        per_dev_kwh.append((d.name, d.key, kwh))

        # Hourly kWh
        try:
            if "energy_kwh" in df_e.columns:
                tmp = df_e[["timestamp", "energy_kwh"]].copy()
                tmp["hour"] = pd.to_datetime(tmp["timestamp"], errors="coerce").dt.floor("H")
                hs = tmp.dropna(subset=["hour"]).groupby("hour")["energy_kwh"].sum()
                if hourly_kwh is None:
                    hourly_kwh = hs
                else:
                    hourly_kwh = hourly_kwh.add(hs, fill_value=0.0)
        except Exception:
            pass

        # Power stats from total_power if present
        try:
            if "total_power" in df_e.columns:
                p = pd.to_numeric(df_e["total_power"], errors="coerce")
            elif "W" in df_e.columns:
                p = pd.to_numeric(df_e["W"], errors="coerce")
            else:
                p = None

            if p is not None:
                p = p.dropna()
                if not p.empty:
                    sum_power += float(p.sum())
                    cnt_power += int(p.count())
                    pmin = float(p.min())
                    min_power_w = pmin if min_power_w is None else min(min_power_w, pmin)

                    # max power with timestamp
                    pmax = float(p.max())
                    if pmax > max_power:
                        try:
                            imax = int(p.idxmax())
                            ts = df_e.loc[imax, "timestamp"] if "timestamp" in df_e.columns else None
                        except Exception:
                            ts = None
                        max_power = pmax
                        max_power_dev = d.name
                        max_power_ts = ts
        except Exception:
            pass

        # Voltage / Current (total)
        try:
            y_v, _ = self._wva_series(df_use, "V")
            if y_v is not None and len(y_v) > 0 and y_v.notna().any():
                y_v_num = pd.to_numeric(y_v, errors="coerce").dropna()
                if not y_v_num.empty:
                    vmin = float(y_v_num.min())
                    vmax = float(y_v_num.max())
                    min_v = vmin if min_v is None else min(min_v, vmin)
                    max_v = vmax if max_v is None else max(max_v, vmax)
        except Exception:
            pass

        try:
            y_a, _ = self._wva_series(df_use, "A")
            if y_a is not None and len(y_a) > 0 and y_a.notna().any():
                y_a_num = pd.to_numeric(y_a, errors="coerce").dropna()
                if not y_a_num.empty:
                    amax = float(y_a_num.max())
                    max_a = amax if max_a is None else max(max_a, amax)
        except Exception:
            pass

        # Load-filtered cosφ / VAR stats
        try:
            # Load series (W)
            try:
                w = None
                if "total_power" in df_e.columns:
                    w = pd.Series(pd.to_numeric(df_e["total_power"], errors="coerce").values, index=df_e["timestamp"])
                else:
                    y_w, _ = self._wva_series(df_use, "W")
                    if y_w is not None:
                        w = pd.to_numeric(y_w, errors="coerce")
                if w is not None:
                    w = pd.to_numeric(w, errors="coerce").dropna()
            except Exception:
                w = None

            # cosφ
            y_pf, _ = self._wva_series(df_use, "COSPHI")
            if y_pf is not None and len(y_pf) > 0 and y_pf.notna().any() and w is not None and not w.empty:
                pf = pd.to_numeric(y_pf, errors="coerce").dropna()
                if not pf.empty:
                    common = pf.index.intersection(w.index)
                    pf2 = pf.loc[common]
                    w2 = w.loc[common]
                    pf2 = pf2.loc[w2 > load_thr_w]
                    if not pf2.empty:
                        vmin = float(pf2.min())
                        tsmin = pf2.idxmin()
                        if worst_pf_val is None or vmin < float(worst_pf_val):
                            worst_pf_val = vmin
                            worst_pf_dev = d.name
                            worst_pf_ts = tsmin

            # VAR (reactive)
            y_var, _ = self._wva_series(df_use, "VAR")
            if y_var is not None and len(y_var) > 0 and y_var.notna().any() and w is not None and not w.empty:
                q = pd.to_numeric(y_var, errors="coerce").dropna()
                if not q.empty:
                    common = q.index.intersection(w.index)
                    q2 = q.loc[common]
                    w2 = w.loc[common]
                    q2 = q2.loc[w2 > load_thr_w]
                    if not q2.empty:
                        idx = q2.abs().idxmax()
                        v = float(q2.loc[idx])
                        if abs(v) > abs(max_abs_var):
                            max_abs_var = v
                            max_abs_var_dev = d.name
                            max_abs_var_ts = idx
        except Exception:
            pass

    # Build message
    if kind == "daily":
        title = f"📊 Tages-Zusammenfassung {start_dt.date().isoformat()}"
    else:
        title = f"📊 Monats-Zusammenfassung {start_dt.strftime('%Y-%m')}"

    range_line = f"⏱ Zeitraum: {start_dt.strftime('%Y-%m-%d %H:%M')} – {end_dt.strftime('%Y-%m-%d %H:%M')}"
    lines = [title, "", range_line, ""]

    # totals
    lines.append(f"⚡ Verbrauch: {total_kwh:.3f} kWh")

    if price_eur is not None:
        try:
            cost = total_kwh * price_eur
            lines.append(f"💶 Kosten: {cost:.2f} €")
        except Exception:
            pass

    # power stats
    try:
        if cnt_power > 0:
            avg_power_w = sum_power / max(cnt_power, 1)
            lines.append(f"📈 Ø Leistung: {avg_power_w:.0f} W")
        if min_power_w is not None:
            lines.append(f"🧊 Min Leistung: {float(min_power_w):.0f} W")
        if max_v is not None and min_v is not None:
            lines.append(f"🔌 Spannung: {float(min_v):.0f}–{float(max_v):.0f} V")
        if max_a is not None:
            lines.append(f"🔋 Max Strom: {float(max_a):.1f} A")
    except Exception:
        pass

    # per device
    lines.append("")
    lines.append("🏠 Pro Gerät:")
    if per_dev_kwh:
        for name, _key, kwh in per_dev_kwh:
            lines.append(f" - {name}: {kwh:.3f} kWh")
    else:
        lines.append(" - (keine Daten)")

    # Top-3 consumers
    if total_kwh > 0 and per_dev_kwh:
        top = sorted(per_dev_kwh, key=lambda x: x[2], reverse=True)[:3]
        lines.append("")
        lines.append("🏆 Top 3 Verbraucher:")
        for name, _key, kwh in top:
            pct = (kwh / total_kwh) * 100.0 if total_kwh > 0 else 0.0
            lines.append(f" - {name}: {kwh:.3f} kWh ({pct:.1f}%)")

    # Peak/Low hour by kWh
    try:
        if hourly_kwh is not None and not hourly_kwh.empty:
            peak_h = hourly_kwh.idxmax()
            peak_val = float(hourly_kwh.max())
            # low hour: prefer >0 if available
            low_series = hourly_kwh[hourly_kwh > 0] if (hourly_kwh > 0).any() else hourly_kwh
            low_h = low_series.idxmin()
            low_val = float(low_series.min())

            def _fmt_hr(ts, val):
                try:
                    ts = pd.Timestamp(ts)
                    end_ts = ts + pd.Timedelta(hours=1)
                    return f"{ts.strftime('%Y-%m-%d %H:00')}–{end_ts.strftime('%H:00')}  ({val:.3f} kWh)"
                except Exception:
                    return f"{ts} ({val:.3f} kWh)"

            lines.append("")
            lines.append(f"🕒 Peak-Stunde: {_fmt_hr(peak_h, peak_val)}")
            lines.append(f"🕒 Niedrigste Stunde: {_fmt_hr(low_h, low_val)}")
    except Exception:
        pass

    # peak / worst pf / var
    if max_power_dev is not None and max_power > 0:
        ts = str(max_power_ts) if max_power_ts is not None else ""
        lines.append("")
        lines.append(f"🔥 Peak-Leistung: {max_power:.0f} W ({max_power_dev}) {ts}")

    if worst_pf_dev is not None and worst_pf_val is not None:
        ts = str(worst_pf_ts) if worst_pf_ts is not None else ""
        lines.append(f"📉 Schlechtester cos φ (W>{load_thr_w:.0f}): {float(worst_pf_val):.3f} ({worst_pf_dev}) {ts}")

    if max_abs_var_dev is not None:
        ts = str(max_abs_var_ts) if max_abs_var_ts is not None else ""
        lines.append(f"⚡ Max |VAR| (W>{load_thr_w:.0f}): {abs(float(max_abs_var)):.0f} var ({max_abs_var_dev}) {ts}")

    return "\n".join(lines).strip() + "\n"

    def _alerts_format_telegram_message(self, r: Any, s: LiveSample, metric: str, val: float, op: str, thr: float, dur: int, cd: int, base_msg: str = "") -> str:
            """Build a detailed Telegram alert message (multi-line)."""
            try:
                ts_local = datetime.fromtimestamp(int(getattr(s, "ts", 0) or 0)).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                ts_local = str(getattr(s, "ts", ""))

            devkey = str(getattr(s, "device_key", "") or "")
            devname = str(getattr(s, "device_name", devkey) or devkey)

            # Try to resolve IP/host from config
            host = ""
            try:
                for d in getattr(self.cfg, "devices", []) or []:
                    if str(getattr(d, "key", "") or "") == devkey:
                        host = str(getattr(d, "host", "") or "")
                        break
            except Exception:
                host = ""

            m = (metric or "").strip().upper()
            phase = ""
            base = m
            if m.endswith("_L1"):
                phase = "L1"; base = m[:-3]
            elif m.endswith("_L2"):
                phase = "L2"; base = m[:-3]
            elif m.endswith("_L3"):
                phase = "L3"; base = m[:-3]

            unit = {"W": "W", "V": "V", "A": "A", "VAR": "var", "COSPHI": ""}.get(base, "")
            title = "🚨 Shelly Alarm"

            # Rule meta
            rid = str(getattr(r, "rule_id", "") or "")
            if not rid:
                try:
                    rid = f"{getattr(r,'device_key','*')}:{getattr(r,'metric','')}"
                except Exception:
                    rid = ""

            # Per-phase snapshot (if available)
            phase_line = ""
            try:
                # dict selection
                dct = None
                if base == "W":
                    dct = getattr(s, "power_w", None)
                elif base == "V":
                    dct = getattr(s, "voltage_v", None)
                elif base == "A":
                    dct = getattr(s, "current_a", None)
                elif base == "VAR":
                    dct = getattr(s, "reactive_var", None)
                elif base == "COSPHI":
                    dct = getattr(s, "cosphi", None)

                if isinstance(dct, dict):
                    a = float(dct.get("a", 0.0) or 0.0)
                    b = float(dct.get("b", 0.0) or 0.0)
                    c = float(dct.get("c", 0.0) or 0.0)

                    if base == "V":
                        total = (a + b + c) / 3.0 if (a or b or c) else float(dct.get("total", 0.0) or 0.0)
                    elif base == "A":
                        total = (a + b + c) if (a or b or c) else float(dct.get("total", 0.0) or 0.0)
                    else:
                        total = float(dct.get("total", 0.0) or 0.0)

                    if phase:
                        phase_line = f"Phase: {phase} | L1 {a:g}{(' '+unit) if unit else ''} | L2 {b:g}{(' '+unit) if unit else ''} | L3 {c:g}{(' '+unit) if unit else ''} | Total {total:g}{(' '+unit) if unit else ''}"
                    else:
                        phase_line = f"L1 {a:g}{(' '+unit) if unit else ''} | L2 {b:g}{(' '+unit) if unit else ''} | L3 {c:g}{(' '+unit) if unit else ''} | Total {total:g}{(' '+unit) if unit else ''}"
            except Exception:
                phase_line = ""

            # Value line
            val_str = f"{val:g}{(' '+unit) if unit else ''}"
            thr_str = f"{thr:g}{(' '+unit) if unit else ''}"

            lines = [title,
                     f"Zeit: {ts_local}",
                     f"Gerät: {devname} ({devkey})" + (f" @ {host}" if host else ""),
                     f"Regel: {metric} {op} {thr_str} (Dauer {int(dur)}s, Cooldown {int(cd)}s)" + (f" | ID {rid}" if rid else ""),
                     f"Wert: {val_str}"]

            if phase_line:
                lines.append(f"Phasen: {phase_line}")

            if base_msg:
                lines.append(f"Info: {base_msg}")

            return "\n".join(lines)

    def _alerts_value(self, s: LiveSample, metric: str) -> float:
        """Return a numeric value for an alert metric.

        Supported metrics:
          - W, VAR, COSPHI (totals)
          - V, A (derived totals)
          - Phase-specific: *_L1, *_L2, *_L3 (maps to a/b/c)
            (V_L1.., A_L1.., W_L1.., VAR_L1.., COSPHI_L1..)
        """
        m0 = (metric or "W").strip().upper()
        m = m0.replace(" ", "").replace("Φ", "PHI")

        # Phase mapping
        phase = None
        if m.endswith("_L1"):
            phase = "a"; base = m[:-3]
        elif m.endswith("_L2"):
            phase = "b"; base = m[:-3]
        elif m.endswith("_L3"):
            phase = "c"; base = m[:-3]
        else:
            base = m

        # Helpers
        def _mean_abc(d: dict) -> float:
            vals = []
            for k in ("a", "b", "c"):
                try:
                    v = float(d.get(k, 0.0))
                except Exception:
                    v = 0.0
                if v != 0.0:
                    vals.append(v)
            if vals:
                return float(sum(vals) / len(vals))
            # fallback: try total if present
            try:
                return float(d.get("total", 0.0))
            except Exception:
                return 0.0

        def _sum_abc(d: dict) -> float:
            ssum = 0.0
            ok = False
            for k in ("a", "b", "c"):
                try:
                    v = float(d.get(k, 0.0))
                except Exception:
                    v = 0.0
                if v != 0.0:
                    ok = True
                ssum += v
            if ok:
                return float(ssum)
            try:
                return float(d.get("total", 0.0))
            except Exception:
                return 0.0

        # Base metrics
        if base in {"W", "P", "POWER"}:
            if phase:
                return float(s.power_w.get(phase, 0.0))
            return float(s.power_w.get("total", 0.0))

        if base in {"VAR", "Q", "REACTIVE"}:
            if phase:
                return float(s.reactive_var.get(phase, 0.0))
            return float(s.reactive_var.get("total", 0.0))

        if base in {"COSPHI", "COSPH", "COSP", "COS", "PF", "POWERFACTOR"}:
            if phase:
                return float(s.cosphi.get(phase, 0.0))
            return float(s.cosphi.get("total", 0.0))

        if base in {"V", "VOLT", "VOLTAGE"}:
            if phase:
                return float(s.voltage_v.get(phase, 0.0))
            return _mean_abc(s.voltage_v)

        if base in {"A", "AMP", "AMPS", "CURRENT"}:
            if phase:
                return float(s.current_a.get(phase, 0.0))
            return _sum_abc(s.current_a)

        # Unknown -> default to power total
        return float(s.power_w.get("total", 0.0))

    

    def _alerts_process_sample(self, s: LiveSample) -> None:
            rules = list(getattr(self.cfg, "alerts", []) or [])
            if not rules:
                return
            for r in rules:
                try:
                    if not getattr(r, "enabled", True):
                        continue
                    devk = str(getattr(r, "device_key", "*") or "*").strip()
                    if devk not in {"*", s.device_key}:
                        continue
                    rid = str(getattr(r, "rule_id", "") or "")
                    if not rid:
                        rid = f"{devk}:{getattr(r,'metric','W')}"
                    op = str(getattr(r, "op", ">") or ">").strip()
                    thr = float(getattr(r, "threshold", 0.0) or 0.0)
                    dur = int(getattr(r, "duration_seconds", 10) or 0)
                    cd = int(getattr(r, "cooldown_seconds", 120) or 0)

                    val = self._alerts_value(s, getattr(r, "metric", "W"))
                    cond = (
                        (val > thr) if op == ">" else
                        (val < thr) if op == "<" else
                        (val >= thr) if op in {">=", "=>"} else
                        (val <= thr) if op in {"<=", "=<"} else
                        (val == thr) if op in {"=", "=="} else
                        (val > thr)
                    )

                    st = self._alert_state.setdefault(rid, {"start_ts": None, "triggered": False, "last_trigger_ts": 0})
                    if not cond:
                        st["start_ts"] = None
                        st["triggered"] = False
                        continue

                    if st["start_ts"] is None:
                        st["start_ts"] = int(s.ts)
                    if st["triggered"]:
                        continue
                    if dur > 0 and int(s.ts) - int(st["start_ts"]) < dur:
                        continue
                    if cd > 0 and int(s.ts) - int(st.get("last_trigger_ts", 0)) < cd:
                        continue

                    st["triggered"] = True
                    st["last_trigger_ts"] = int(s.ts)

                    msg_custom = str(getattr(r, "message", "") or "").strip()
                    metric = str(getattr(r, "metric", "W"))
                    devname = str(getattr(s, "device_name", s.device_key) or s.device_key)
                    msg = msg_custom or self.t('settings.alerts.fired').format(device=devname, metric=metric, value=val, op=op, threshold=thr)

                    
                    if bool(getattr(r, "action_telegram", False)):
                        try:
                            # Telegram can be more detailed than popup/beep
                            level = str(getattr(self.cfg.ui, "telegram_detail_level", "detailed") or "detailed").strip().lower()
                            if level == "simple":
                                tg_msg = msg
                            else:
                                tg_msg = self._alerts_format_telegram_message(r, s, metric, val, op, thr, dur, cd, msg_custom or "")
                            self._alerts_send_telegram(tg_msg)
                        except Exception:
                            pass
                    if bool(getattr(r, "action_beep", True)):
                        try:
                            self.bell()
                        except Exception:
                            pass
                    if bool(getattr(r, "action_popup", True)):
                        try:
                            messagebox.showwarning(self.t('settings.alerts.title'), msg)
                        except Exception:
                            pass
                except Exception:
                    continue

    def _add_device_from_host(self, host: str, name_hint: str = "") -> None:
        """Add a new device row by probing a Shelly IP/host (used by mDNS discovery)."""
        host = (host or "").strip()
        if not host:
            return

        disc = None
        try:
            disc = probe_device(host, timeout_seconds=2.5)
        except Exception:
            disc = None

        # Unique key
        used = set()
        try:
            for row in getattr(self, "_dev_vars", []) or []:
                used.add((row[0].get() or "").strip())
        except Exception:
            pass
        n = 1
        while f"shelly{n}" in used:
            n += 1
        key = f"shelly{n}"

        # Name: prefer user visible instance name (mDNS) -> config override -> model
        nm = (name_hint or "").strip()
        if not nm:
            try:
                nm = (disc.model or "").strip() if disc else ""
            except Exception:
                nm = ""
        if not nm:
            nm = f"Shelly {n}"

        v_key = tk.StringVar(value=key)
        v_name = tk.StringVar(value=nm)
        v_host = tk.StringVar(value=str(host))
        v_emid = tk.StringVar(value=str(int(getattr(disc, "component_id", 0) or 0)) if disc else "0")
        v_model = tk.StringVar(value=str(getattr(disc, "model", "") or "") if disc else "")
        v_kind = tk.StringVar(value=str(getattr(disc, "kind", "unknown") or "unknown") if disc else "unknown")
        v_gen = tk.StringVar(value=str(int(getattr(disc, "gen", 0) or 0)) if disc else "0")
        v_phases = tk.StringVar(value=str(int(getattr(disc, "phases", 1) or 1)) if disc else "1")
        v_emdata = tk.BooleanVar(value=bool(getattr(disc, "supports_emdata", True)) if disc else True)

        if not getattr(self, "_dev_vars", None):
            self._dev_vars = []
        self._dev_vars.append((v_key, v_name, v_host, v_emid, v_model, v_kind, v_gen, v_phases, v_emdata))

        try:
            # Rebuild settings tab to show the new row
            self._clear_frame(self.tab_settings)
            self._build_settings_tab()
        except Exception:
            pass


def run_gui() -> None:
    # Initialize logging early so UI + background threads can report issues.
    try:
        from shelly_analyzer.io.logging_setup import setup_logging
        lp = setup_logging(Path.cwd())
    except Exception:
        lp = None

    app = App()
    try:
        app._log_path = lp  # type: ignore[attr-defined]
    except Exception:
        pass
    app.mainloop()