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
    AnomalyConfig,
    AppConfig,
    BillingConfig,
    BillingParty,
    Co2Config,
    CsvPackConfig,
    DeviceConfig,
    DownloadConfig,
    PricingConfig,
    SolarConfig,
    TouConfig,
    TouRate,
    UiConfig,
    UpdatesConfig,
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
from shelly_analyzer.services.live import LivePoller, MultiLivePoller, DemoMultiLivePoller, LiveSample
from shelly_analyzer.services.sync import sync_all
from shelly_analyzer.services.webdash import LivePoint, LiveStateStore, LiveWebDashboard
from shelly_analyzer.services.discovery import probe_device
from shelly_analyzer.services.demo import default_demo_devices, ensure_demo_csv
from shelly_analyzer.services.mdns import discover_shelly_mdns


from .._shared import _fmt_eur, _fmt_kwh, _parse_date_flexible, _period_bounds, PLOTS_MODES, AUTOSYNC_INTERVAL_OPTIONS, AUTOSYNC_MODE_OPTIONS, INVOICE_PERIOD_OPTIONS


def _tou_cost_breakdown(timestamps: "pd.Series", kwh_series: "pd.Series", pricing: PricingConfig, tou: TouConfig, tz: Any) -> "Tuple[float, Dict[str, Tuple[float, float]]]":
    """Calculate TOU cost breakdown from timestamps and per-row kWh values.

    Returns (total_cost_eur, {rate_name: (kwh, cost_eur)}).
    If TOU is disabled or no rates defined, uses flat rate under key ''.
    """
    flat_unit = float(pricing.unit_price_gross())
    total_kwh = float(pd.to_numeric(kwh_series, errors="coerce").fillna(0.0).sum())

    if not getattr(tou, "enabled", False) or not getattr(tou, "rates", None):
        total_cost = total_kwh * flat_unit
        return total_cost, {}

    rates = list(tou.rates)
    vat_rate = pricing.vat_rate()

    # Precompute gross price for each rate (respecting VAT settings of pricing config)
    def _rate_gross(rate_price: float) -> float:
        if vat_rate <= 0:
            return float(rate_price)
        return float(rate_price) if pricing.price_includes_vat else float(rate_price) * (1.0 + vat_rate)

    rate_gross_prices = [_rate_gross(r.price_eur_per_kwh) for r in rates]

    try:
        ts = pd.to_datetime(timestamps, errors="coerce")
        if ts.dt.tz is None:
            ts = ts.dt.tz_localize("UTC")
        ts = ts.dt.tz_convert(tz)
        hours = ts.dt.hour.values
        weekdays = ts.dt.weekday.values  # 0=Mon, 6=Sun
    except Exception:
        # Fallback: flat rate
        total_cost = total_kwh * flat_unit
        return total_cost, {}

    kwh_vals = pd.to_numeric(kwh_series, errors="coerce").fillna(0.0).values
    # Per-rate accumulators
    rate_kwh = [0.0] * len(rates)
    rate_cost = [0.0] * len(rates)
    unmatched_kwh = 0.0
    unmatched_cost = 0.0

    for i in range(len(kwh_vals)):
        h = int(hours[i])
        wd = int(weekdays[i])
        kw = float(kwh_vals[i])
        matched = False
        for ri, rate in enumerate(rates):
            s = int(rate.start_hour) % 24
            e = int(rate.end_hour) % 24
            if s < e:
                in_window = s <= h < e
            elif s > e:
                in_window = h >= s or h < e
            else:
                in_window = True  # full day
            if rate.weekdays_only and wd >= 5:
                continue
            if in_window:
                rate_kwh[ri] += kw
                rate_cost[ri] += kw * rate_gross_prices[ri]
                matched = True
                break
        if not matched:
            unmatched_kwh += kw
            unmatched_cost += kw * flat_unit

    breakdown: Dict[str, Tuple[float, float]] = {}
    for ri, rate in enumerate(rates):
        if rate_kwh[ri] > 1e-9:
            breakdown[rate.name] = (rate_kwh[ri], rate_cost[ri])
    if unmatched_kwh > 1e-9:
        breakdown["?"] = (unmatched_kwh, unmatched_cost)

    total_cost = sum(c for _, c in breakdown.values())
    return total_cost, breakdown


class CoreMixin:
    """Auto-generated mixin extracted from the former ui/app.py to keep files smaller."""

    def _disp_device_name(self, name: str) -> str:
            """Translate demo device name keys like 'demo.device.*' when shown in UI."""
            try:
                if isinstance(name, str) and name.startswith("demo.device."):
                    return self.t(name)
            except Exception:
                pass
            return name

    def __init__(self) -> None:
            super().__init__()
            # config loaded below; title set after language is known
            # Reasonable default; we also size plots dynamically.
            self.geometry("1400x900")
            self.project_root = Path.cwd()
            self.cfg_path = self.project_root / "config.json"
            self._first_run = not self.cfg_path.exists()
            self.cfg = load_config(self.cfg_path)
            # Setup wizard is shown when starting without a config or without any devices.
            try:
                self._setup_required = bool(self._first_run) or (len(getattr(self.cfg, 'devices', []) or []) == 0)
            except Exception:
                self._setup_required = bool(self._first_run)
            self._tabs_built = False
            self.lang = normalize_lang(getattr(self.cfg.ui, "language", "de"))
            self.t = lambda k, **kw: _t(self.lang, k, **kw)
            # UI variables that may be referenced before their tabs are built
            # (e.g., during setup wizard / early autosync).
            self.sync_summary = tk.StringVar(value=self.t("common.no_data"))
            # Updates (GitHub Releases)
            self.upd_status = tk.StringVar(value=self.t("updates.status.idle"))
            self.upd_auto = tk.BooleanVar(value=bool(getattr(getattr(self.cfg, "updates", None), "auto_install", False)))
            self._upd_latest: Optional[Any] = None
            self.sync_log = None  # will be a tk.Text once the Sync tab is built
            self._sync_log_buffer: List[str] = []
            self.title(f"{self.t('app.title')} {__version__}")
            # Web dashboard authentication is intentionally disabled (LAN-only use).
            self.storage = Storage(self.project_root / "data")
            # Auto-import legacy CSV data from previous installs (best-effort), so
            # users upgrading to a new folder don't have to manually copy the data/
            # directory.
            keys: list = []
            try:
                keys = [d.key for d in getattr(self.cfg, "devices", []) if getattr(d, "key", None)]
                if keys:
                    self.storage.auto_import_from_previous_installs(keys)
            except Exception:
                pass
            # v6.0.0: auto-migrate CSV data to SQLite DB on first run.
            try:
                if keys and self.storage.needs_migration(keys):
                    import logging as _log
                    _log.getLogger(__name__).info("Migrating CSV data to SQLite DB...")
                    migrated = self.storage.migrate_csvs_to_db(keys)
                    archived = self.storage.archive_csv_files(keys)
                    _log.getLogger(__name__).info("Migration done: %s rows, %s files archived", migrated, archived)
            except Exception:
                pass
            # v6.0.0.2: re-import from csv_archive if DB schema was expanded
            # (fills voltage min/max/avg, current min/max/avg, apparent power,
            # reactive energy columns that were missing in the initial migration).
            try:
                if keys and self.storage.needs_reimport(keys):
                    import logging as _log
                    _log.getLogger(__name__).info("Re-importing CSV data to fill expanded DB schema (v6.0.0.2)...")
                    reimported = self.storage.reimport_from_archive(keys)
                    _log.getLogger(__name__).info("Re-import done: %s", reimported)
            except Exception:
                pass
            # Demo Mode: generate realistic demo CSV data so Plots/Exports work out-of-the-box.
            try:
                if bool(getattr(getattr(self.cfg, 'demo', None), 'enabled', False)) and getattr(self.cfg, 'devices', None):
                    ensure_demo_csv(self.storage, list(getattr(self.cfg, 'devices', []) or []), getattr(self.cfg, 'demo'))
            except Exception:
                pass
            # Computed data
            self.computed: Dict[str, ComputedDevice] = {}
            # Background sync
            self._sync_q: "queue.Queue[str]" = queue.Queue()
            self._sync_thread: Optional[threading.Thread] = None
            # Progress bar state (widgets set in _build_sync_tab)
            self._progress_q: "queue.Queue[tuple]" = queue.Queue()
            self._sync_progressbar: Optional[ttk.Progressbar] = None
            self._sync_progress_label_var: Optional[tk.StringVar] = None
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
            # Anomaly detection history (in-memory)
            self._anomaly_log: List[Any] = []
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
            # Check for updates on startup (non-blocking)
            self.after(500, self._updates_check_on_startup)
            # On first run (no config yet), start in Settings → Devices.
            try:
                if bool(getattr(self, '_first_run', False)) or not list(getattr(self.cfg, 'devices', []) or []):
                    self.after(100, self._focus_settings_devices)
            except Exception:
                pass
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
                self.notebook.bind('<<NotebookTabChanged>>', lambda _e=None: (self._kick_plots_resize_watch(), self._on_tab_changed()))
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
                            updates=self.cfg.updates,
                            demo=self.cfg.demo,
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
                self.notebook.bind('<<NotebookTabChanged>>', lambda _e=None: (self._kick_plots_resize_watch(), self._on_tab_changed()))
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

    def _on_tab_changed(self) -> None:
            """Refresh the content of the newly selected tab."""
            try:
                sel = self.notebook.select()
                if sel == str(getattr(self, 'tab_costs', None)):
                    self.after(50, self._refresh_costs_tab)
                elif sel == str(getattr(self, 'tab_heatmap', None)):
                    self.after(50, self._refresh_heatmap)
                elif sel == str(getattr(self, 'tab_solar', None)):
                    self.after(50, self._refresh_solar_tab)
                elif sel == str(getattr(self, 'tab_compare', None)):
                    self.after(50, self._refresh_compare)
                elif sel == str(getattr(self, 'tab_co2', None)):
                    self.after(50, self._refresh_co2_tab)
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

    def _build_ui(self) -> None:
            # Top bar: choose which two devices are shown in the UI.
            bar = ttk.Frame(self)
            bar.pack(fill="x", padx=12, pady=(10, 0))
            ttk.Label(bar, text=self.t("ui.view")).pack(side="left")
            self.device_page_label_var = tk.StringVar(value="")
            self.device_page_cb = ttk.Combobox(bar, state="readonly", width=34, textvariable=self.device_page_label_var)
            self.device_page_cb.pack(side="left", padx=(8, 0))
            self.device_page_cb.bind("<<ComboboxSelected>>", self._on_device_page_selected)
            # Convenience: quickly cycle the device page without opening the dropdown.
            def _page_prev() -> None:
                labels = getattr(self, "_page_labels", None) or self._device_page_labels()
                if not labels:
                    return
                cur = str(self.device_page_label_var.get())
                try:
                    idx = labels.index(cur)
                except Exception:
                    idx = 0
                idx = (idx - 1) % len(labels)
                try:
                    self.device_page_label_var.set(labels[idx])
                    self._on_device_page_selected()
                except Exception:
                    pass

            def _page_next() -> None:
                labels = getattr(self, "_page_labels", None) or self._device_page_labels()
                if not labels:
                    return
                cur = str(self.device_page_label_var.get())
                try:
                    idx = labels.index(cur)
                except Exception:
                    idx = 0
                idx = (idx + 1) % len(labels)
                try:
                    self.device_page_label_var.set(labels[idx])
                    self._on_device_page_selected()
                except Exception:
                    pass

            ttk.Button(bar, text="◀", width=3, command=_page_prev).pack(side="left", padx=(6, 2))
            ttk.Button(bar, text="▶", width=3, command=_page_next).pack(side="left", padx=(0, 6))


            self.notebook = ttk.Notebook(self)
            self.notebook.pack(fill="both", expand=True)
            self.tab_sync = ttk.Frame(self.notebook)
            self.tab_plots = ttk.Frame(self.notebook)
            self.tab_live = ttk.Frame(self.notebook)
            self.tab_costs = ttk.Frame(self.notebook)
            self.tab_heatmap = ttk.Frame(self.notebook)
            self.tab_solar = ttk.Frame(self.notebook)
            self.tab_compare = ttk.Frame(self.notebook)
            self.tab_anomaly = ttk.Frame(self.notebook)
            self.tab_schedule = ttk.Frame(self.notebook)
            self.tab_co2 = ttk.Frame(self.notebook)
            self.tab_export = ttk.Frame(self.notebook)
            self.tab_settings = ttk.Frame(self.notebook)
            # Notebook tab labels are translated based on the selected UI language.
            self.notebook.add(self.tab_sync, text=self.t("tabs.sync"))
            self.notebook.add(self.tab_plots, text=self.t("tabs.plots"))
            self.notebook.add(self.tab_live, text=self.t("tabs.live"))
            self.notebook.add(self.tab_costs, text=self.t("tabs.costs"))
            self.notebook.add(self.tab_heatmap, text=self.t("tabs.heatmap"))
            self.notebook.add(self.tab_solar, text=self.t("tabs.solar"))
            self.notebook.add(self.tab_compare, text=self.t("tabs.compare"))
            self.notebook.add(self.tab_anomaly, text=self.t("tabs.anomaly"))
            self.notebook.add(self.tab_schedule, text=self.t("tabs.schedule"))
            self.notebook.add(self.tab_co2, text=self.t("tabs.co2"))
            self.notebook.add(self.tab_export, text=self.t("tabs.export"))
            self.notebook.add(self.tab_settings, text=self.t("tabs.settings"))

            # First-run / no-devices mode: show a guided Setup wizard and keep other tabs disabled
            if bool(getattr(self, "_setup_required", False)):
                self.tab_setup = ttk.Frame(self.notebook)
                # Insert setup as the first tab for guidance
                self.notebook.insert(0, self.tab_setup, text=self.t("tabs.setup"))

                # Put placeholders into disabled tabs (avoid CSV warnings on first run)
                for tab in (self.tab_sync, self.tab_plots, self.tab_live, self.tab_costs, self.tab_heatmap, self.tab_solar, self.tab_compare, self.tab_anomaly, self.tab_schedule, self.tab_co2, self.tab_export):
                    try:
                        ttk.Label(
                            tab,
                            text=self.t("setup.placeholder"),
                            justify="left",
                            wraplength=980,
                        ).pack(anchor="nw", padx=18, pady=18)
                    except Exception:
                        pass
                    try:
                        self.notebook.tab(tab, state="disabled")
                    except Exception:
                        pass

                # Build wizard + settings
                self._build_setup_wizard_tab()
                self._build_settings_tab()
                try:
                    self.notebook.select(self.tab_setup)
                except Exception:
                    pass
                self._tabs_built = False
            else:
                self._build_sync_tab()
                self._build_plots_tab()
                self._build_live_tab()
                self._build_costs_tab()
                self._build_heatmap_tab()
                self._build_solar_tab()
                self._build_compare_tab()
                self._build_anomaly_tab()
                self._build_schedule_tab()
                self._build_co2_tab()
                self._build_export_tab()
                self._build_settings_tab()
                self._schedule_init()
                self._tabs_built = True

            self._page_labels = []
            self._update_device_page_choices()

    def _get_visible_devices(self) -> List[DeviceConfig]:
            """Return the devices currently visible in the UI (max 2 for plots/live).

            When a group or 'all' is selected the first 2 devices in that selection
            are returned so that the existing plot/live widgets keep working.
            Use _get_selected_all_devices() for cost aggregation (no limit).
            """
            view_type = str(getattr(self.cfg.ui, "selected_view_type", "page") or "page")
            if view_type == "group":
                return self._get_selected_all_devices()[:2]
            if view_type == "all":
                return list(self.cfg.devices)[:2]
            # page (default)
            try:
                page = int(getattr(self.cfg.ui, "device_page_index", 0) or 0)
            except Exception:
                page = 0
            page = max(0, page)
            start = page * 2
            return list(self.cfg.devices[start : start + 2])

    def _get_selected_all_devices(self) -> List[DeviceConfig]:
            """Return ALL devices for the currently selected view (no 2-device limit).

            Used by the costs tab and Telegram summary for proper aggregation.
            """
            view_type = str(getattr(self.cfg.ui, "selected_view_type", "page") or "page")
            if view_type == "all":
                return list(self.cfg.devices)
            if view_type == "group":
                group_name = str(getattr(self.cfg.ui, "selected_view_group", "") or "")
                key_map = {d.key: d for d in self.cfg.devices}
                for g in (getattr(self.cfg, "groups", []) or []):
                    if g.name == group_name:
                        return [key_map[k] for k in g.device_keys if k in key_map]
                return list(self.cfg.devices)[:2]
            # page
            try:
                page = int(getattr(self.cfg.ui, "device_page_index", 0) or 0)
            except Exception:
                page = 0
            page = max(0, page)
            start = page * 2
            return list(self.cfg.devices[start : start + 2])

    def _build_view_entries(self) -> List[dict]:
            """Build the list of view entries for the device page dropdown.

            Each entry is a dict with keys: type ('page'|'group'|'all'), ref (page_idx or group_name), label.
            """
            entries: List[dict] = []
            devs = list(self.cfg.devices)
            # Device pages
            if not devs:
                entries.append({"type": "page", "ref": 0, "label": "1: — | —"})
            else:
                for i in range(0, len(devs), 2):
                    left = devs[i].name
                    right = devs[i + 1].name if i + 1 < len(devs) else "—"
                    entries.append({"type": "page", "ref": i // 2, "label": f"{(i//2)+1}: {left} | {right}"})
            # Groups
            prefix = self.t("groups.group_prefix")
            for g in (getattr(self.cfg, "groups", []) or []):
                if g.name:
                    entries.append({"type": "group", "ref": g.name, "label": f"{prefix}{g.name}"})
            # Total / Gesamt
            if len(devs) > 1:
                entries.append({"type": "all", "ref": None, "label": self.t("groups.all")})
            return entries

    def _device_page_labels(self) -> List[str]:
            return [e["label"] for e in self._build_view_entries()]

    def _update_device_page_choices(self) -> None:
            entries = self._build_view_entries()
            labels = [e["label"] for e in entries]
            self._page_labels = labels
            self._view_entries = entries
            try:
                self.device_page_cb["values"] = labels
            except Exception:
                pass

            # Determine the correct label for the current config state
            view_type = str(getattr(self.cfg.ui, "selected_view_type", "page") or "page")
            view_group = str(getattr(self.cfg.ui, "selected_view_group", "") or "")
            try:
                page_idx = int(getattr(self.cfg.ui, "device_page_index", 0) or 0)
            except Exception:
                page_idx = 0

            current_label = None
            for e in entries:
                if view_type == "page" and e["type"] == "page" and e["ref"] == page_idx:
                    current_label = e["label"]
                    break
                if view_type == "group" and e["type"] == "group" and e["ref"] == view_group:
                    current_label = e["label"]
                    break
                if view_type == "all" and e["type"] == "all":
                    current_label = e["label"]
                    break

            if current_label is None and labels:
                current_label = labels[0]
                # Reset to first page
                self.cfg = replace(self.cfg, ui=replace(self.cfg.ui,
                    device_page_index=0,
                    selected_view_type="page",
                    selected_view_group="",
                ))
                try:
                    save_config(self.cfg, self.cfg_path)
                except Exception:
                    pass

            try:
                self.device_page_label_var.set(current_label or "")
            except Exception:
                pass

    def _on_device_page_selected(self, _evt: Any = None) -> None:
            entries = getattr(self, "_view_entries", None)
            if not entries:
                entries = self._build_view_entries()
            sel = str(self.device_page_label_var.get())
            # Find matching entry
            entry = next((e for e in entries if e["label"] == sel), None)
            if entry is None and entries:
                entry = entries[0]

            if entry is not None:
                if entry["type"] == "page":
                    self.cfg = replace(self.cfg, ui=replace(self.cfg.ui,
                        device_page_index=int(entry["ref"]),
                        selected_view_type="page",
                        selected_view_group="",
                    ))
                elif entry["type"] == "group":
                    self.cfg = replace(self.cfg, ui=replace(self.cfg.ui,
                        selected_view_type="group",
                        selected_view_group=str(entry["ref"]),
                    ))
                elif entry["type"] == "all":
                    self.cfg = replace(self.cfg, ui=replace(self.cfg.ui,
                        selected_view_type="all",
                        selected_view_group="",
                    ))
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
            # Refresh costs to show correct aggregate
            try:
                self._refresh_costs_tab()
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

    def _rebuild_live_tab(self) -> None:
            self._clear_frame(self.tab_live)
            self._build_live_tab()

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
            # Progress bar (shown during active sync)
            prog_frm = ttk.Frame(frm)
            prog_frm.pack(fill="x", padx=12, pady=(0, 4))
            self._sync_progress_label_var = tk.StringVar(value="")
            ttk.Label(prog_frm, textvariable=self._sync_progress_label_var).pack(anchor="w")
            self._sync_progressbar = ttk.Progressbar(prog_frm, mode="determinate", maximum=100)
            self._sync_progressbar.pack(fill="x", pady=(2, 0))
            mid = ttk.Frame(frm)
            mid.pack(fill="both", expand=True, padx=12, pady=10)
            self.sync_log = tk.Text(mid, height=18)
            self.sync_log.pack(fill="both", expand=True)
            bottom = ttk.Frame(frm)
            bottom.pack(fill="x", padx=12, pady=(0, 12))
            # Reuse pre-created StringVar so other code can safely write to it
            # even before this tab exists.
            try:
                self.sync_summary.set(self.t('common.no_data'))
            except Exception:
                self.sync_summary = tk.StringVar(value=self.t('common.no_data'))
            ttk.Label(bottom, textvariable=self.sync_summary).pack(anchor="w")
            # Flush buffered log lines collected before the Sync tab existed.
            try:
                if self._sync_log_buffer:
                    self.sync_log.insert("end", "\n".join(self._sync_log_buffer) + "\n")
                    self.sync_log.see("end")
                    self._sync_log_buffer.clear()
            except Exception:
                pass
            # init autosync status
            self.after(200, self._on_autosync_toggle)

    def _log_sync(self, msg: str) -> None:
            ts = time.strftime("%H:%M:%S")
            line = f"[{ts}] {msg}"
            # During first-run setup the Sync tab may not exist yet.
            if getattr(self, "sync_log", None) is None:
                self._sync_log_buffer.append(line)
                # keep buffer bounded
                if len(self._sync_log_buffer) > 500:
                    self._sync_log_buffer = self._sync_log_buffer[-500:]
                return
            self.sync_log.insert("end", line + "\n")
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
            # Reset progress bar
            self._progress_q.put((0, 100, self.t("sync.progress.started")))
            def worker():
                total_devices = max(1, len([d for d in self.cfg.devices if getattr(d, "enabled", True)]))
                seen_devices: list = []

                def on_progress(device_key: str, done: int, total: int, msg: str) -> None:
                    if device_key not in seen_devices:
                        seen_devices.append(device_key)
                    dev_idx = seen_devices.index(device_key)
                    dev_fraction = dev_idx / total_devices
                    chunk_fraction = (done / max(1, total)) / total_devices
                    pct = min(99, int((dev_fraction + chunk_fraction) * 100))
                    lbl = self.t("sync.progress.label", dev=dev_idx + 1, total_devs=total_devices, done=done, total=total)
                    self._progress_q.put((pct, 100, lbl))

                try:
                    results = sync_all(self.cfg, self.storage, range_override=range_override, fallback_last_days=7, progress=on_progress)
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
                    self._progress_q.put((100, 100, self.t("sync.progress.done")))
                    self._sync_q.put("__SYNC_DONE__")
                except Exception as e:
                    try:
                        self._sync_q.put(self.t('sync.err.generic', e=e))
                    except Exception:
                        self._sync_q.put(f"Sync ERROR: {e}")
                    self._progress_q.put((0, 100, ""))
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
                if tnorm.upper() in {"V", "A", "W", "VAR", "HZ"}:
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
            if m in {"VAR", "Q"}:
                # Need either pre-computed Q columns or apparent power for derivation.
                tokens = ["q_total_var", "qa", "qb", "qc", "reactive_var", "aprt_power", "apparent_power"]
                return any(any(t in c for t in tokens) for c in cols)
            if m in {"COSPHI", "PF", "POWERFACTOR"}:
                # Need either pre-computed cos φ columns or apparent power for derivation.
                tokens = ["cosphi_total", "cosphi", "pfa", "pfb", "pfc", "pf_total", "aprt_power", "apparent_power"]
                return any(any(t in c for t in tokens) for c in cols)
            if m in {"HZ", "FREQ", "FREQUENCY"}:
                return any("freq_hz" in c or "frequency" in c or c == "freq" for c in cols)
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
                    "a_act_power","b_act_power","c_act_power",
                    "q_total_var","qa","qb","qc",
                    "cosphi_total","pfa","pfb","pfc",
                    "freq_hz",
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
                # Per-phase active power (W)
                "a_act_power": pd.to_numeric(df.get("pa"), errors="coerce"),
                "b_act_power": pd.to_numeric(df.get("pb"), errors="coerce"),
                "c_act_power": pd.to_numeric(df.get("pc"), errors="coerce"),
                # Pre-computed reactive power (VAR) and power factor (cos φ)
                "q_total_var": pd.to_numeric(df.get("q_total_var"), errors="coerce"),
                "qa": pd.to_numeric(df.get("qa"), errors="coerce"),
                "qb": pd.to_numeric(df.get("qb"), errors="coerce"),
                "qc": pd.to_numeric(df.get("qc"), errors="coerce"),
                "cosphi_total": pd.to_numeric(df.get("cosphi_total"), errors="coerce"),
                "pfa": pd.to_numeric(df.get("pfa"), errors="coerce"),
                "pfb": pd.to_numeric(df.get("pfb"), errors="coerce"),
                "pfc": pd.to_numeric(df.get("pfc"), errors="coerce"),
                # Grid frequency (Hz) from live store
                "freq_hz": pd.to_numeric(df.get("freq_hz"), errors="coerce"),
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
                        'total_power', 'total_w', 'total_power_w', 'total_act_power_w',
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

                # Override with pre-computed VAR/cosφ columns when P/S computation
                # produced unusable results (no apparent power data available).
                # This happens with live polling data where VAR and cosφ are already
                # computed by the device and stored directly in the DataFrame.
                if "S(no cols)" in mapping_text or mapS == "S(no cols)":
                    if want_pf:
                        _pf_col = first_col(["cosphi_total", "pf_total", "power_factor_total", "cos_phi_total", "cosphi", "pf"])
                        if _pf_col:
                            y = pd.to_numeric(df[_pf_col], errors="coerce")
                            mapping_text = f"cosφ: direct={_pf_col}"
                        else:
                            _pfa = first_col(["pfa", "pf_a", "cosphi_a", "a_cosphi", "a_pf"])
                            _pfb = first_col(["pfb", "pf_b", "cosphi_b", "b_cosphi", "b_pf"])
                            _pfc = first_col(["pfc", "pf_c", "cosphi_c", "c_cosphi", "c_pf"])
                            _pf_cols = [c for c in (_pfa, _pfb, _pfc) if c]
                            if _pf_cols:
                                _num_pf = pd.to_numeric(df[_pf_cols[0]], errors="coerce")
                                if len(_pf_cols) > 1:
                                    for _c in _pf_cols[1:]:
                                        _num_pf = _num_pf + pd.to_numeric(df[_c], errors="coerce")
                                    _num_pf = _num_pf / float(len(_pf_cols))
                                y = _num_pf
                                mapping_text = f"cosφ: mean({','.join(_pf_cols)})"
                    else:
                        _q_col = first_col(["q_total_var", "reactive_var_total", "total_reactive_var", "var_total", "q_total"])
                        if _q_col:
                            y = pd.to_numeric(df[_q_col], errors="coerce")
                            mapping_text = f"VAR: direct={_q_col}"
                        else:
                            _qa = first_col(["qa", "q_a", "reactive_var_a", "a_reactive_var", "a_var", "a_q"])
                            _qb = first_col(["qb", "q_b", "reactive_var_b", "b_reactive_var", "b_var", "b_q"])
                            _qc = first_col(["qc", "q_c", "reactive_var_c", "c_reactive_var", "c_var", "c_q"])
                            _q_cols = [c for c in (_qa, _qb, _qc) if c]
                            if _q_cols:
                                _num_q = pd.to_numeric(df[_q_cols[0]], errors="coerce")
                                for _c in _q_cols[1:]:
                                    _num_q = _num_q + pd.to_numeric(df[_c], errors="coerce")
                                y = _num_q
                                mapping_text = f"VAR: sum({','.join(_q_cols)})"

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

            elif metric_u in {"HZ", "FREQ", "FREQUENCY"}:
                ylab = "Hz"
                fq_col = first_col(["freq_hz", "frequency", "freq", "hz"])
                if fq_col:
                    y = pd.to_numeric(df[fq_col], errors="coerce")
                    mapping_text = f"Hz: {fq_col}"
                else:
                    y = pd.Series(dtype=float)
                    mapping_text = "Hz(no cols)"

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

                # Add neutral conductor current for "A" metric
                # Always compute N from phase currents using 120° displacement
                # formula. The stored n_avg_current from Shelly is unreliable
                # (often a constant ~0.014 regardless of actual load).
                if kind == "current":
                    if "L1" in out and "L2" in out and "L3" in out:
                        try:
                            ia = pd.to_numeric(out["L1"], errors="coerce").fillna(0.0)
                            ib = pd.to_numeric(out["L2"], errors="coerce").fillna(0.0)
                            ic = pd.to_numeric(out["L3"], errors="coerce").fillna(0.0)
                            n_calc = np.sqrt(np.maximum(ia**2 + ib**2 + ic**2 - ia*ib - ia*ic - ib*ic, 0.0))
                            out["N"] = n_calc
                        except Exception:
                            pass

                # Keep stable order
                result = {k: out[k] for k in ("L1", "L2", "L3") if k in out}
                if "N" in out:
                    result["N"] = out["N"]
                return result

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
            # Day/Night was moved to Settings → Appearance as global plot_theme_mode
            self.live_daynight_ctl = tk.StringVar(value="")

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

            # Day/Night toggle removed – now in Settings → Appearance

            # Open current log file (useful for debugging)
            try:
                ttk.Button(ctl, text=self.t('live.open_log'), command=self._open_log_file).grid(row=0, column=12, padx=(6, 0))
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
                # Day/Night trace removed – now global in Settings
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
                v_line3 = tk.StringVar(value="–")  # Grid frequency (Hz)
                v_appl  = tk.StringVar(value="–")  # Appliance detector
                self._live_latest_vars[d.key] = {
                    "power": v_power,
                    "voltage": v_volt,
                    "current": v_curr,
                    "kwh_today": v_kwh,
                    "stamp": v_stamp,
                    "line0": v_line0,
                    "line1": v_line1,
                    "line2": v_line2,
                    "line3": v_line3,
                    "appliance": v_appl,
                }

                ttk.Label(status_fr, textvariable=v_line0, style="LiveInfo.TLabel").grid(row=0, column=0, sticky="w")
                ttk.Label(status_fr, textvariable=v_line1, style="LiveInfo.TLabel").grid(row=1, column=0, sticky="w", pady=(2, 0))
                ttk.Label(status_fr, textvariable=v_line2, style="LiveInfo.TLabel").grid(row=2, column=0, sticky="w", pady=(2, 0))
                ttk.Label(status_fr, textvariable=v_line3, style="LiveInfo.TLabel").grid(row=3, column=0, sticky="w", pady=(2, 0))
                # Appliance detector row
                appl_fr = ttk.Frame(status_fr)
                appl_fr.grid(row=5, column=0, sticky="ew", pady=(6, 0))
                appl_fr.columnconfigure(0, weight=1)
                ttk.Label(appl_fr, textvariable=v_appl, style="LiveInfo.TLabel").pack(anchor="w")
                try:
                    _hint_lbl = ttk.Label(appl_fr, text=self.t('live.appliance.hint'), style="LiveInfo.TLabel")
                    _hint_lbl.pack(anchor="w")
                    _hint_lbl.configure(foreground="#888888")
                except Exception:
                    pass

                # Switch state + toggle (only for kind == 'switch')
                if str(getattr(d, "kind", "em")) == "switch":
                    v_sw = tk.StringVar(value="–")
                    self._live_switch_vars[d.key] = v_sw
                    # Switch line (kept compact)
                    sw_fr = ttk.Frame(status_fr)
                    sw_fr.grid(row=4, column=0, sticky="ew", pady=(2, 0))
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
                try:
                    _lbg = "#111111" if self._resolve_plot_theme() == "night" else "#FFFFFF"
                    fig_p.patch.set_facecolor(_lbg)
                    canvas_p.get_tk_widget().configure(bg=_lbg)
                except Exception:
                    pass
                canvas_p.get_tk_widget().pack(fill="both", expand=True)
                # Voltage (all phases)
                v_title = self.t('live.chart.voltage.1p') if int(getattr(d, 'phases', 3) or 3) <= 1 else self.t('live.chart.voltage')
                box_v = ttk.Labelframe(colfrm, text=v_title)
                box_v.grid(row=2, column=0, sticky="nsew", padx=8, pady=6)
                fig_v = Figure(figsize=(6, 2.2), dpi=100)
                ax_v = fig_v.add_subplot(111)
                ax_v.set_ylabel("V")
                canvas_v = FigureCanvasTkAgg(fig_v, master=box_v)
                try:
                    fig_v.patch.set_facecolor(_lbg)
                    canvas_v.get_tk_widget().configure(bg=_lbg)
                except Exception:
                    pass
                canvas_v.get_tk_widget().pack(fill="both", expand=True)
                # Current (all phases)
                c_title = self.t('live.chart.current.1p') if int(getattr(d, 'phases', 3) or 3) <= 1 else self.t('live.chart.current')
                box_c = ttk.Labelframe(colfrm, text=c_title)
                box_c.grid(row=3, column=0, sticky="nsew", padx=8, pady=6)
                fig_c = Figure(figsize=(6, 2.2), dpi=100)
                ax_c = fig_c.add_subplot(111)
                ax_c.set_ylabel("A")
                canvas_c = FigureCanvasTkAgg(fig_c, master=box_c)
                try:
                    fig_c.patch.set_facecolor(_lbg)
                    canvas_c.get_tk_widget().configure(bg=_lbg)
                except Exception:
                    pass
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
                    # Demo mode: toggle without network
                    if str(getattr(dev, 'host', '')).startswith('demo://'):
                        try:
                            for pol in list(getattr(self, '_live_pollers', []) or []):
                                if hasattr(pol, 'set_switch'):
                                    pol.set_switch(device_key, bool(target_on))
                                    break
                        except Exception:
                            pass
                        on2 = bool(target_on)
                        def _apply_demo():
                            try:
                                v = self._live_switch_vars.get(device_key)
                                if v is not None:
                                    v.set(self.t('live.switch.on') if on2 else self.t('live.switch.off'))
                            except Exception:
                                pass
                        try:
                            self.after(0, _apply_demo)
                        except Exception:
                            _apply_demo()
                        return
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
                        msg = f"{dev.name}: {e}"
                        self.after(0, lambda m=msg: self.live_status.set(m))
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

            # Day/Night mode (UI theme) - now read from global config (plot_theme_mode)
            dn_mode = str(getattr(self.cfg.ui, 'plot_theme_mode', 'auto') or 'auto').strip().lower()
            if dn_mode not in ('auto', 'day', 'night'):
                dn_mode = 'auto'


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
            changed_dn = str(getattr(old_ui, 'plot_theme_mode', 'auto')) != str(dn_mode)

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
                    plot_theme_mode=dn_mode,
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

            if changed_win or changed_smooth or changed_dn or changed_web_on or changed_web_refresh:
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
            # Reset alert state so stale start_ts from a previous live session
            # cannot cause an immediate false trigger (duration check would pass
            # because now_ts − old_start_ts is huge).
            self._alert_state.clear()
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
                # Poll devices (or generate demo samples) in the background.
                if bool(getattr(getattr(self.cfg, 'demo', None), 'enabled', False)):
                    self._live_pollers = [
                        DemoMultiLivePoller(devs_all, getattr(self.cfg, 'demo'), poll_seconds=self.cfg.ui.live_poll_seconds)
                    ]
                else:
                    self._live_pollers = [
                        MultiLivePoller(devs_all, self.cfg.download, poll_seconds=self.cfg.ui.live_poll_seconds)
                    ]
                for p in self._live_pollers:
                    p.start()

                # Start a fresh incremental sync in the background when Live starts,
                # so "today" calculations have a good baseline.
                try:
                    if bool(getattr(getattr(self.cfg, 'demo', None), 'enabled', False)):
                        # Demo mode uses generated CSVs; no network sync required.
                        raise RuntimeError('demo mode: skip sync')
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
                    "n_current",
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

    def _build_costs_tab(self) -> None:
            """Build the costs dashboard tab — one section per 3-phase device."""
            frm = self.tab_costs

            # Title + refresh button top bar
            top = ttk.Frame(frm)
            top.pack(fill="x", padx=14, pady=(12, 4))
            ttk.Label(top, text=self.t("costs.title"), font=("", 14, "bold")).pack(side="left")

            # Scrollable container for device sections
            container = ttk.Frame(frm)
            container.pack(fill="both", expand=True, padx=0, pady=0)

            canvas = tk.Canvas(container, highlightthickness=0)
            scrollbar = ttk.Scrollbar(container, orient="vertical", command=canvas.yview)
            self._cost_scroll_frame = ttk.Frame(canvas)

            self._cost_scroll_frame.bind(
                "<Configure>",
                lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
            )
            _cost_win = canvas.create_window((0, 0), window=self._cost_scroll_frame, anchor="nw")
            canvas.configure(yscrollcommand=scrollbar.set)
            canvas.bind(
                "<Configure>",
                lambda e: canvas.itemconfigure(_cost_win, width=e.width),
            )

            canvas.pack(side="left", fill="both", expand=True)
            scrollbar.pack(side="right", fill="y")

            # Mouse wheel scrolling
            def _on_mousewheel(event):
                try:
                    canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
                except Exception:
                    pass
            canvas.bind_all("<MouseWheel>", _on_mousewheel)

            # Build per-device sections
            self._cost_device_vars = {}  # {device_key: {range_key: {kwh: StringVar, eur: StringVar, co2: StringVar}, proj_kwh, proj_eur, proj_co2, cmp_text}}
            self._cost_aggregate_vars = {}  # vars for group/all aggregate section

            three_phase_devs = [d for d in (self.cfg.devices or []) if int(getattr(d, "phases", 3) or 3) >= 3 and str(getattr(d, "kind", "em")) != "switch"]

            # --- Aggregate section (group or all-devices view) ---
            view_type = str(getattr(self.cfg.ui, "selected_view_type", "page") or "page")
            view_group = str(getattr(self.cfg.ui, "selected_view_group", "") or "")
            if view_type in ("group", "all"):
                agg_title = (
                    self.t("costs.group_aggregate").format(name=view_group)
                    if view_type == "group"
                    else self.t("costs.all_devices")
                )
                agg_frame = ttk.LabelFrame(self._cost_scroll_frame, text=f"📊 {agg_title}")
                agg_frame.pack(fill="x", padx=14, pady=(8, 4))
                agg_vars: dict = {}
                cards_agg = ttk.Frame(agg_frame)
                cards_agg.pack(fill="x", padx=8, pady=4)
                cards_agg.columnconfigure((0, 1, 2, 3), weight=1)
                for col, (key, label) in enumerate([
                    ("today", self.t("costs.today")),
                    ("week", self.t("costs.this_week")),
                    ("month", self.t("costs.this_month")),
                    ("year", self.t("costs.this_year")),
                ]):
                    card = ttk.LabelFrame(cards_agg, text=label)
                    card.grid(row=0, column=col, sticky="nsew", padx=3, pady=3)
                    v_kwh = tk.StringVar(value="– kWh")
                    v_eur = tk.StringVar(value="– €")
                    v_co2 = tk.StringVar(value="")
                    v_tou = tk.StringVar(value="")
                    ttk.Label(card, textvariable=v_kwh, font=("", 10)).pack(anchor="w", padx=6, pady=(4, 0))
                    ttk.Label(card, textvariable=v_eur, font=("", 12, "bold")).pack(anchor="w", padx=6, pady=(1, 0))
                    ttk.Label(card, textvariable=v_co2, font=("", 9), foreground="#4caf50").pack(anchor="w", padx=6, pady=(0, 1))
                    ttk.Label(card, textvariable=v_tou, font=("", 8), foreground="#888888").pack(anchor="w", padx=6, pady=(0, 4))
                    agg_vars[key] = {"kwh": v_kwh, "eur": v_eur, "co2": v_co2, "tou": v_tou}
                # Projection + comparison row
                row2_agg = ttk.Frame(agg_frame)
                row2_agg.pack(fill="x", padx=8, pady=(0, 6))
                row2_agg.columnconfigure((0, 1), weight=1)
                proj_card_agg = ttk.LabelFrame(row2_agg, text=self.t("costs.projected_month"))
                proj_card_agg.grid(row=0, column=0, sticky="nsew", padx=3, pady=3)
                v_proj_kwh_agg = tk.StringVar(value="– kWh")
                v_proj_eur_agg = tk.StringVar(value="– €")
                v_proj_co2_agg = tk.StringVar(value="")
                ttk.Label(proj_card_agg, textvariable=v_proj_kwh_agg, font=("", 10)).pack(anchor="w", padx=6, pady=(4, 0))
                ttk.Label(proj_card_agg, textvariable=v_proj_eur_agg, font=("", 12, "bold")).pack(anchor="w", padx=6, pady=(1, 0))
                ttk.Label(proj_card_agg, textvariable=v_proj_co2_agg, font=("", 9), foreground="#4caf50").pack(anchor="w", padx=6, pady=(0, 4))
                agg_vars["proj_kwh"] = v_proj_kwh_agg
                agg_vars["proj_eur"] = v_proj_eur_agg
                agg_vars["proj_co2"] = v_proj_co2_agg
                cmp_card_agg = ttk.LabelFrame(row2_agg, text=self.t("costs.vs_last_month"))
                cmp_card_agg.grid(row=0, column=1, sticky="nsew", padx=3, pady=3)
                v_cmp_agg = tk.StringVar(value="–")
                ttk.Label(cmp_card_agg, textvariable=v_cmp_agg, font=("", 11, "bold")).pack(anchor="w", padx=6, pady=8)
                agg_vars["cmp_text"] = v_cmp_agg
                self._cost_aggregate_vars = agg_vars

            if not three_phase_devs:
                ttk.Label(self._cost_scroll_frame, text=self.t("costs.no_3phase"), font=("", 11)).pack(padx=14, pady=20)
            else:
                for d in three_phase_devs:
                    dev_frame = ttk.LabelFrame(self._cost_scroll_frame, text=f"⚡ {d.name}  ({d.host})")
                    dev_frame.pack(fill="x", padx=14, pady=(8, 4))

                    vars_dev = {}

                    # Row 1: Today / Week / Month / Year
                    cards = ttk.Frame(dev_frame)
                    cards.pack(fill="x", padx=8, pady=4)
                    cards.columnconfigure((0, 1, 2, 3), weight=1)

                    for col, (key, label) in enumerate([
                        ("today", self.t("costs.today")),
                        ("week", self.t("costs.this_week")),
                        ("month", self.t("costs.this_month")),
                        ("year", self.t("costs.this_year")),
                    ]):
                        card = ttk.LabelFrame(cards, text=label)
                        card.grid(row=0, column=col, sticky="nsew", padx=3, pady=3)
                        v_kwh = tk.StringVar(value="– kWh")
                        v_eur = tk.StringVar(value="– €")
                        v_co2 = tk.StringVar(value="")
                        v_tou = tk.StringVar(value="")
                        ttk.Label(card, textvariable=v_kwh, font=("", 10)).pack(anchor="w", padx=6, pady=(4, 0))
                        ttk.Label(card, textvariable=v_eur, font=("", 12, "bold")).pack(anchor="w", padx=6, pady=(1, 0))
                        ttk.Label(card, textvariable=v_co2, font=("", 9), foreground="#4caf50").pack(anchor="w", padx=6, pady=(0, 1))
                        ttk.Label(card, textvariable=v_tou, font=("", 8), foreground="#888888").pack(anchor="w", padx=6, pady=(0, 4))
                        vars_dev[key] = {"kwh": v_kwh, "eur": v_eur, "co2": v_co2, "tou": v_tou}

                    # Row 2: Projection + Previous month comparison
                    row2 = ttk.Frame(dev_frame)
                    row2.pack(fill="x", padx=8, pady=(0, 6))
                    row2.columnconfigure((0, 1), weight=1)

                    proj_card = ttk.LabelFrame(row2, text=self.t("costs.projected_month"))
                    proj_card.grid(row=0, column=0, sticky="nsew", padx=3, pady=3)
                    v_proj_kwh = tk.StringVar(value="– kWh")
                    v_proj_eur = tk.StringVar(value="– €")
                    v_proj_co2 = tk.StringVar(value="")
                    ttk.Label(proj_card, textvariable=v_proj_kwh, font=("", 10)).pack(anchor="w", padx=6, pady=(4, 0))
                    ttk.Label(proj_card, textvariable=v_proj_eur, font=("", 12, "bold")).pack(anchor="w", padx=6, pady=(1, 0))
                    ttk.Label(proj_card, textvariable=v_proj_co2, font=("", 9), foreground="#4caf50").pack(anchor="w", padx=6, pady=(0, 4))
                    vars_dev["proj_kwh"] = v_proj_kwh
                    vars_dev["proj_eur"] = v_proj_eur
                    vars_dev["proj_co2"] = v_proj_co2

                    cmp_card = ttk.LabelFrame(row2, text=self.t("costs.vs_last_month"))
                    cmp_card.grid(row=0, column=1, sticky="nsew", padx=3, pady=3)
                    v_cmp = tk.StringVar(value="–")
                    ttk.Label(cmp_card, textvariable=v_cmp, font=("", 11, "bold")).pack(anchor="w", padx=6, pady=8)
                    vars_dev["cmp_text"] = v_cmp

                    self._cost_device_vars[d.key] = vars_dev

            # Initial refresh
            self.after(500, self._refresh_costs_tab)

    def _refresh_costs_tab(self) -> None:
            """Recalculate and display cost data per 3-phase device and aggregate."""
            import logging as _log_m
            _log = _log_m.getLogger(__name__)
            try:
                from datetime import datetime, timedelta
                from zoneinfo import ZoneInfo
                tz = ZoneInfo("Europe/Berlin")
                now = datetime.now(tz)

                pricing = getattr(self.cfg, "pricing", PricingConfig())
                tou = getattr(self.cfg, "tou", TouConfig())

                # Unit price (gross) – fallback for non-TOU
                try:
                    unit = float(pricing.unit_price_gross())
                except Exception:
                    unit = float(getattr(pricing, "electricity_price_eur_per_kwh", 0.30) or 0.30)

                # CO₂ intensity (g/kWh → kg/kWh factor)
                try:
                    co2_g_per_kwh = float(getattr(pricing, "co2_intensity_g_per_kwh", 380.0) or 0.0)
                except Exception:
                    co2_g_per_kwh = 380.0

                tou_enabled = getattr(tou, "enabled", False) and bool(getattr(tou, "rates", None))

                # Time ranges
                today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
                week_start = today_start - timedelta(days=now.weekday())  # Monday
                month_start = today_start.replace(day=1)
                year_start = today_start.replace(month=1, day=1)
                last_month_start = (month_start - timedelta(days=1)).replace(day=1)

                ranges = {
                    "today": (today_start, now),
                    "week": (week_start, now),
                    "month": (month_start, now),
                    "year": (year_start, now),
                    "last_month": (last_month_start, month_start),
                }

                for d in list(getattr(self.cfg, "devices", []) or []):
                    if d.key not in getattr(self, "_cost_device_vars", {}):
                        continue

                    vars_dev = self._cost_device_vars[d.key]
                    dev_results = {}  # rng_key -> (kwh, cost, breakdown_dict)

                    for rng_key, (rng_start, rng_end) in ranges.items():
                        kwh = 0.0
                        cost = 0.0
                        breakdown: Dict[str, Tuple[float, float]] = {}
                        try:
                            cd = self.computed.get(d.key)
                            if cd is not None:
                                df = cd.df.copy()
                                if "timestamp" in df.columns:
                                    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
                                    df = df.dropna(subset=["timestamp"])
                                    try:
                                        if df["timestamp"].dt.tz is None:
                                            df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
                                        df["timestamp"] = df["timestamp"].dt.tz_convert(tz)
                                    except Exception:
                                        pass
                                    m = (df["timestamp"] >= rng_start) & (df["timestamp"] < rng_end)
                                    df_rng = df.loc[m]
                                    kwh_s = pd.to_numeric(df_rng["energy_kwh"], errors="coerce").fillna(0.0) if "energy_kwh" in df_rng.columns else pd.Series(dtype=float)
                                    kwh = float(kwh_s.sum())
                                    if tou_enabled and len(df_rng) > 0 and "energy_kwh" in df_rng.columns:
                                        cost, breakdown = _tou_cost_breakdown(df_rng["timestamp"], kwh_s, pricing, tou, tz)
                                    else:
                                        cost = kwh * unit
                        except Exception:
                            pass
                        dev_results[rng_key] = (kwh, cost, breakdown)

                    # Update cards
                    for key in ("today", "week", "month", "year"):
                        kwh, cost, breakdown = dev_results.get(key, (0.0, 0.0, {}))
                        if key in vars_dev:
                            vars_dev[key]["kwh"].set(f"{kwh:.2f} kWh")
                            vars_dev[key]["eur"].set(f"{cost:.2f} €")
                            if co2_g_per_kwh > 0:
                                co2_kg = kwh * co2_g_per_kwh / 1000.0
                                vars_dev[key]["co2"].set(f"🌿 {co2_kg:.3f} kg CO₂")
                            else:
                                vars_dev[key]["co2"].set("")
                            # TOU breakdown
                            if tou_enabled and breakdown:
                                parts = [f"{name}: {k:.2f} kWh / {c:.2f} €" for name, (k, c) in breakdown.items()]
                                vars_dev[key]["tou"].set("  |  ".join(parts))
                            else:
                                vars_dev[key]["tou"].set("")

                    # Projection
                    try:
                        import calendar
                        days_in_month = calendar.monthrange(now.year, now.month)[1]
                        days_elapsed = max(1, (now - month_start).total_seconds() / 86400.0)
                        month_kwh, month_cost, _ = dev_results.get("month", (0.0, 0.0, {}))
                        proj_kwh = month_kwh / days_elapsed * days_in_month
                        proj_cost = month_cost / days_elapsed * days_in_month
                        vars_dev["proj_kwh"].set(f"~{proj_kwh:.1f} kWh")
                        vars_dev["proj_eur"].set(f"~{proj_cost:.2f} €")
                        if co2_g_per_kwh > 0:
                            proj_co2_kg = proj_kwh * co2_g_per_kwh / 1000.0
                            vars_dev["proj_co2"].set(f"🌿 ~{proj_co2_kg:.2f} kg CO₂")
                        else:
                            vars_dev["proj_co2"].set("")
                    except Exception:
                        pass

                    # Comparison with last month
                    try:
                        last_m_kwh, last_m_cost, _ = dev_results.get("last_month", (0.0, 0.0, {}))
                        cur_m_kwh, cur_m_cost, _ = dev_results.get("month", (0.0, 0.0, {}))
                        if last_m_kwh > 0:
                            delta = ((cur_m_kwh - last_m_kwh) / last_m_kwh) * 100.0
                            arrow = "📈" if delta > 0 else "📉"
                            vars_dev["cmp_text"].set(
                                f"{arrow} {delta:+.1f}% ({last_m_kwh:.1f} kWh = {last_m_cost:.2f} €)"
                            )
                        else:
                            vars_dev["cmp_text"].set(self.t("costs.no_prev_data"))
                    except Exception:
                        pass

                # --- Aggregate section update (group/all) ---
                agg_vars = getattr(self, "_cost_aggregate_vars", {})
                if agg_vars:
                    agg_devices = self._get_selected_all_devices()
                    agg_results: Dict[str, Tuple[float, float, dict]] = {}
                    for rng_key, (rng_start, rng_end) in ranges.items():
                        tot_kwh = 0.0
                        tot_cost = 0.0
                        tot_breakdown: Dict[str, Tuple[float, float]] = {}
                        for d in agg_devices:
                            try:
                                cd = self.computed.get(d.key)
                                if cd is None:
                                    continue
                                df = cd.df.copy()
                                if "timestamp" not in df.columns:
                                    continue
                                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
                                df = df.dropna(subset=["timestamp"])
                                try:
                                    if df["timestamp"].dt.tz is None:
                                        df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
                                    df["timestamp"] = df["timestamp"].dt.tz_convert(tz)
                                except Exception:
                                    pass
                                m = (df["timestamp"] >= rng_start) & (df["timestamp"] < rng_end)
                                df_rng = df.loc[m]
                                kwh_s = pd.to_numeric(df_rng["energy_kwh"], errors="coerce").fillna(0.0) if "energy_kwh" in df_rng.columns else pd.Series(dtype=float)
                                kwh = float(kwh_s.sum())
                                if tou_enabled and len(df_rng) > 0 and "energy_kwh" in df_rng.columns:
                                    cost, breakdown = _tou_cost_breakdown(df_rng["timestamp"], kwh_s, pricing, tou, tz)
                                else:
                                    cost = kwh * unit
                                    breakdown = {}
                                tot_kwh += kwh
                                tot_cost += cost
                                for k, (bk, bc) in breakdown.items():
                                    pk, pc = tot_breakdown.get(k, (0.0, 0.0))
                                    tot_breakdown[k] = (pk + bk, pc + bc)
                            except Exception:
                                pass
                        agg_results[rng_key] = (tot_kwh, tot_cost, tot_breakdown)

                    for key in ("today", "week", "month", "year"):
                        kwh, cost, breakdown = agg_results.get(key, (0.0, 0.0, {}))
                        if key in agg_vars:
                            agg_vars[key]["kwh"].set(f"{kwh:.2f} kWh")
                            agg_vars[key]["eur"].set(f"{cost:.2f} €")
                            if co2_g_per_kwh > 0:
                                agg_vars[key]["co2"].set(f"🌿 {kwh * co2_g_per_kwh / 1000.0:.3f} kg CO₂")
                            else:
                                agg_vars[key]["co2"].set("")
                            if tou_enabled and breakdown:
                                agg_vars[key]["tou"].set("  |  ".join(
                                    f"{name}: {k:.2f} kWh / {c:.2f} €"
                                    for name, (k, c) in breakdown.items()
                                ))
                            else:
                                agg_vars[key]["tou"].set("")

                    try:
                        import calendar
                        days_in_month = calendar.monthrange(now.year, now.month)[1]
                        days_elapsed = max(1, (now - month_start).total_seconds() / 86400.0)
                        m_kwh, m_cost, _ = agg_results.get("month", (0.0, 0.0, {}))
                        proj_kwh = m_kwh / days_elapsed * days_in_month
                        proj_cost = m_cost / days_elapsed * days_in_month
                        agg_vars["proj_kwh"].set(f"~{proj_kwh:.1f} kWh")
                        agg_vars["proj_eur"].set(f"~{proj_cost:.2f} €")
                        if co2_g_per_kwh > 0:
                            agg_vars["proj_co2"].set(f"🌿 ~{proj_kwh * co2_g_per_kwh / 1000.0:.2f} kg CO₂")
                        else:
                            agg_vars["proj_co2"].set("")
                    except Exception:
                        pass

                    try:
                        last_m_kwh, last_m_cost, _ = agg_results.get("last_month", (0.0, 0.0, {}))
                        cur_m_kwh, cur_m_cost, _ = agg_results.get("month", (0.0, 0.0, {}))
                        if last_m_kwh > 0:
                            delta = ((cur_m_kwh - last_m_kwh) / last_m_kwh) * 100.0
                            arrow = "📈" if delta > 0 else "📉"
                            agg_vars["cmp_text"].set(
                                f"{arrow} {delta:+.1f}% ({last_m_kwh:.1f} kWh = {last_m_cost:.2f} €)"
                            )
                        else:
                            agg_vars["cmp_text"].set(self.t("costs.no_prev_data"))
                    except Exception:
                        pass

            except Exception as e:
                _log.warning("Costs tab refresh error: %s", e)

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

    def _pricing_footer_note(self) -> str:
        """Return a translated pricing note for invoice PDFs."""
        try:
            pricing = self.cfg.pricing
        except Exception:
            return ""
        lang = getattr(self, "lang", "de") or "de"

        def _num(name: str, default: float = 0.0) -> float:
            try:
                v = getattr(pricing, name, default)
                if callable(v):
                    v = v()
                return float(v or 0.0)
            except Exception:
                return float(default)

        try:
            vat_enabled = bool(getattr(pricing, "vat_enabled", False))
        except Exception:
            vat_enabled = False
        try:
            price_includes_vat = bool(getattr(pricing, "price_includes_vat", True))
        except Exception:
            price_includes_vat = True

        vat = _num("vat_rate_percent", 0.0)

        # Unit prices (net/gross)
        try:
            net = float(pricing.unit_price_net())
        except Exception:
            net = _num("unit_price_eur_per_kwh", 0.0)

        try:
            gross = float(pricing.unit_price_gross())
        except Exception:
            # derive gross if needed
            gross = net * (1.0 + vat / 100.0) if vat_enabled else net

        try:
            if not vat_enabled:
                return self.t("pdf.pricing.no_vat", price=_fmt_eur(net))
            if price_includes_vat:
                return self.t(
                    "pdf.pricing.gross_incl_vat",
                    gross=_fmt_eur(gross),
                    net=_fmt_eur(net),
                    vat=f"{vat:.1f}",
                )
            return self.t(
                "pdf.pricing.net_excl_vat",
                net=_fmt_eur(net),
                gross=_fmt_eur(gross),
                vat=f"{vat:.1f}",
            )
        except Exception:
            return ""
    def _export_invoices(self) -> None:
        """Export one PDF invoice per configured device (Shelly)."""
        import traceback as _tb

        if not self._ensure_data_loaded():
            messagebox.showinfo(self.t("msg.export"), self.t("export.no_data"))
            return

        period = str(self.invoice_period_var.get() if hasattr(self, "invoice_period_var") else "custom")
        start, end = self._parse_export_range()
        if period != "custom":
            anchor = _parse_date_flexible(
                self.invoice_anchor_var.get() if hasattr(self, "invoice_anchor_var") else ""
            )
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
        def _pricing_num(name: str, default: float = 0.0) -> float:
            """Return numeric pricing attribute or method value safely."""
            try:
                v = getattr(pricing, name, default)
                if callable(v):
                    v = v()
                return float(v or 0.0)
            except Exception:
                return float(default)

        unit_net = float(pricing.unit_price_net())

        # Issue/due dates
        issue = date.today()
        due = issue + timedelta(days=int(self.cfg.billing.payment_terms_days))
        ts = time.strftime("%Y%m%d")

        written = 0
        errors: list[str] = []

        # Clear log
        try:
            self.export_log.delete("1.0", "end")
        except Exception:
            pass

        for d in self.cfg.devices:
            try:
                df0 = self.computed[d.key].df
                df = filter_by_time(df0, start=start, end=end)

                # If timestamp is in the index, normalize to a column for downstream funcs
                if "timestamp" not in df.columns and isinstance(df.index, pd.DatetimeIndex):
                    df = df.copy()
                    df["timestamp"] = df.index

                if df.empty:
                    raise ValueError(self.t("export.no_data_for_device"))

                kwh, _avgp, _maxp = summarize(df)

                # Human label and invoice number suffix
                if start is None and end is None:
                    period_label = self.t("period.all")
                    suffix = "all"
                else:
                    s_eff = pd.Timestamp(start) if start is not None else pd.Timestamp(df["timestamp"].min())
                    e_eff = pd.Timestamp(end) if end is not None else pd.Timestamp(df["timestamp"].max())
                    suffix = f"{s_eff:%Y%m%d}-{e_eff:%Y%m%d}"
                    period_label = f"{s_eff:%Y-%m-%d} – {e_eff:%Y-%m-%d}"

                invoice_no = f"{self.cfg.billing.invoice_prefix}-{ts}-{d.key}"
                out = inv_dir / f"invoice_{d.key}_{suffix}.pdf"

                # Build invoice lines
                lines: list[InvoiceLine] = []
                # Energy line
                lines.append(
                    InvoiceLine(
                        description=self.t("pdf.invoice.line_energy", device=d.name, period=period_label),
                        quantity=float(kwh),
                        unit="kWh",
                        unit_price_net=float(unit_net),
                    )
                )

                # Optional base fee
                base_year = _pricing_num("base_fee_year_net", 0.0)
                if base_year > 0:
                    # derive number of days in range
                    if start is None and end is None:
                        days = 365
                    else:
                        s_eff = pd.Timestamp(start) if start is not None else pd.Timestamp(df["timestamp"].min())
                        e_eff = pd.Timestamp(end) if end is not None else pd.Timestamp(df["timestamp"].max())
                        days = int((e_eff.date() - s_eff.date()).days) + 1
                        days = max(1, days)

                    try:
                        base_day_net = float(pricing.base_fee_day_net())
                    except Exception:
                        base_day_net = float(base_year) / 365.0

                    lines.append(
                        InvoiceLine(
                            description=self.t("pdf.invoice.line_base_fee", days=days),
                            quantity=float(days),
                            unit=self.t("unit.days"),
                            unit_price_net=float(base_day_net),
                        )
                    )

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
                    },
                    customer={
                        "name": self.cfg.billing.customer.name,
                        "address_lines": self.cfg.billing.customer.address_lines,
                    },
                    period_label=period_label,
                    device_label=f"{d.name} ({d.key})",
                    vat_rate_percent=_pricing_num("vat_rate_percent", 0.0),
                    vat_enabled=bool(pricing.vat_enabled),
                    lines=lines,
                    footer_note=(self._pricing_footer_note()),
                    lang=self.lang,
                    logo_path=getattr(self.cfg.billing, "invoice_logo_path", ""),
                )

                written += 1
                self.export_log.insert("end", self.t("export.invoice_written", path=str(out)) + "\n")

            except Exception as e:
                errors.append(f"{d.key} ({d.name}): {e}")
                self.export_log.insert(
                    "end",
                    f"[ERROR] invoice for {d.key} ({d.name}) failed: {e}\n{_tb.format_exc()}\n",
                )

        self.export_log.see("end")

        if written == 0:
            messagebox.showerror(self.t("msg.export"), self.t("export.failed") + "\n" + "\n".join(errors[:5]))
        elif errors:
            messagebox.showwarning(
                self.t("msg.export"),
                self.t("export.partial") + f" ({written} OK, {len(errors)} failed)",
            )
    def _build_settings_tab(self) -> None:
            frm = self.tab_settings
            # Split settings into subtabs to avoid overly tall pages (esp. on 14" screens).
            nb = ttk.Notebook(frm)
            nb.pack(fill="both", expand=True, padx=12, pady=10)

            # Keep references so we can focus the Devices settings on first run.
            self._settings_nb = nb

            tab_devices = ttk.Frame(nb)
            self._settings_tab_devices = tab_devices
            tab_groups = ttk.Frame(nb)
            self._settings_tab_groups = tab_groups
            tab_main = ttk.Frame(nb)
            tab_advanced = ttk.Frame(nb)
            tab_expert = ttk.Frame(nb)
            tab_billing = ttk.Frame(nb)
            tab_updates = ttk.Frame(nb)
            nb.add(tab_devices, text=self.t('settings.devices'))
            nb.add(tab_groups, text=self.t('settings.groups'))
            nb.add(tab_main, text=self.t('settings.main'))
            nb.add(tab_advanced, text=self.t('settings.advanced'))

            # Scrollable container for the "Live & Preis" settings tab
            _tm_outer = ttk.Frame(tab_main)
            _tm_outer.pack(fill="both", expand=True)
            _tm_canvas = tk.Canvas(_tm_outer, highlightthickness=0)
            _tm_sb = ttk.Scrollbar(_tm_outer, orient="vertical", command=_tm_canvas.yview)
            tab_main_sf = ttk.Frame(_tm_canvas)
            tab_main_sf.bind(
                "<Configure>",
                lambda e: _tm_canvas.configure(scrollregion=_tm_canvas.bbox("all")),
            )
            _tm_win = _tm_canvas.create_window((0, 0), window=tab_main_sf, anchor="nw")
            _tm_canvas.configure(yscrollcommand=_tm_sb.set)
            _tm_canvas.bind("<Configure>", lambda e: _tm_canvas.itemconfigure(_tm_win, width=e.width))
            _tm_canvas.pack(side="left", fill="both", expand=True)
            _tm_sb.pack(side="right", fill="y")
            def _tm_mousewheel(event):
                try:
                    _tm_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
                except Exception:
                    pass
            tab_main_sf.bind("<Enter>", lambda e: _tm_canvas.bind_all("<MouseWheel>", _tm_mousewheel))
            tab_main_sf.bind("<Leave>", lambda e: _tm_canvas.unbind_all("<MouseWheel>"))
            nb.add(tab_expert, text=self.t('settings.expert'))
            nb.add(tab_billing, text=self.t('tabs.billing'))
            nb.add(tab_updates, text=self.t('settings.updates'))

        
            # ---------------- Updates ----------------
            up_outer = ttk.Frame(tab_updates)
            up_outer.pack(fill="both", expand=True, padx=12, pady=12)

            ttk.Label(up_outer, text=self.t('settings.updates'), font=('TkDefaultFont', 14, 'bold')).pack(anchor='w', pady=(0, 10))
            ttk.Label(up_outer, textvariable=self.upd_status, wraplength=900, justify='left').pack(anchor='w', pady=(0, 10))

            row = ttk.Frame(up_outer)
            row.pack(anchor='w', pady=(0, 10))

            self.btn_upd_check = ttk.Button(row, text=self.t('updates.check_now'), command=lambda: self._updates_check_async(auto_install=False))
            self.btn_upd_check.pack(side='left', padx=(0, 8))

            self.btn_upd_install = ttk.Button(row, text=self.t('updates.install'), command=self._updates_install_latest, state="disabled")
            self.btn_upd_install.pack(side='left', padx=(0, 8))

            self.btn_upd_open = ttk.Button(row, text=self.t('updates.open_release'), command=self._updates_open_release)
            self.btn_upd_open.pack(side='left')

            ttk.Checkbutton(up_outer, text=self.t('updates.auto'), variable=self.upd_auto).pack(anchor='w', pady=(6, 0))

            # --- Version history ---
            ttk.Separator(up_outer, orient='horizontal').pack(fill='x', pady=(14, 8))
            ttk.Label(up_outer, text=self.t('updates.versions_title'), font=('TkDefaultFont', 11, 'bold')).pack(anchor='w', pady=(0, 6))

            ttk.Label(up_outer, text=self.t('updates.current_version', version=__version__), foreground='gray').pack(anchor='w', pady=(0, 4))

            ver_frame = ttk.Frame(up_outer)
            ver_frame.pack(anchor='w', fill='x', pady=(0, 4))

            self.upd_release_lb = tk.Listbox(ver_frame, height=10, width=52, selectmode='single', exportselection=False)
            self.upd_release_lb.pack(side='left')

            ver_sb = ttk.Scrollbar(ver_frame, orient='vertical', command=self.upd_release_lb.yview)
            ver_sb.pack(side='left', fill='y')
            self.upd_release_lb.configure(yscrollcommand=ver_sb.set)
            self.upd_release_lb.bind('<<ListboxSelect>>', self._updates_on_release_select)

            self.upd_downgrade_var = tk.StringVar(value="")
            ttk.Label(up_outer, textvariable=self.upd_downgrade_var, foreground='orange').pack(anchor='w', pady=(2, 0))

            self.btn_upd_install_sel = ttk.Button(up_outer, text=self.t('updates.install_selected'), command=self._updates_install_selected, state='disabled')
            self.btn_upd_install_sel.pack(anchor='w', pady=(6, 0))

            # --- Changelog ---
            ttk.Separator(up_outer, orient='horizontal').pack(fill='x', pady=(14, 8))
            ttk.Label(up_outer, text=self.t('updates.changelog_title'), font=('TkDefaultFont', 11, 'bold')).pack(anchor='w', pady=(0, 6))

            cl_frame = ttk.Frame(up_outer)
            cl_frame.pack(fill='both', expand=True, pady=(0, 4))

            self.upd_changelog_text = tk.Text(cl_frame, height=14, wrap='word', state='disabled', relief='sunken', borderwidth=1)
            self.upd_changelog_text.tag_configure('h1', font=('TkDefaultFont', 13, 'bold'))
            self.upd_changelog_text.tag_configure('h2', font=('TkDefaultFont', 11, 'bold'))
            self.upd_changelog_text.tag_configure('h3', font=('TkDefaultFont', 10, 'bold'))
            cl_vsb = ttk.Scrollbar(cl_frame, orient='vertical', command=self.upd_changelog_text.yview)
            self.upd_changelog_text.configure(yscrollcommand=cl_vsb.set)
            cl_vsb.pack(side='right', fill='y')
            self.upd_changelog_text.pack(side='left', fill='both', expand=True)

            self.after(200, self._updates_fetch_changelog_async)

    # ---------------- Gruppen ----------------
            self._build_groups_settings_tab(tab_groups)

    # ---------------- Geräte ----------------

            # Scrollbar for small screens (Devices subtab can get very tall)
            devices_outer = ttk.Frame(tab_devices)
            # First run onboarding: show a quiet hint instead of many warnings.
            try:
                if bool(getattr(self, '_first_run', False)) or not list(getattr(self.cfg, 'devices', []) or []):
                    banner = ttk.Label(
                        devices_outer,
                        text=self.t('first_run.hint'),
                        justify='left'
                    )
                    banner.pack(fill='x', padx=8, pady=(0, 10))
            except Exception:
                pass
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
                            tk.BooleanVar(value=bool(getattr(r, "action_webhook", False))),
                            tk.BooleanVar(value=bool(getattr(r, "action_email", False))),
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
                self._tg_alarm_plots_var = tk.BooleanVar(value=bool(getattr(self.cfg.ui, "telegram_alarm_plots_enabled", True)))

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
                    self._save_settings()
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

            ttk.Checkbutton(
                tg_box,
                text=self.t("settings.alerts.telegram.alarm_plots"),
                variable=self._tg_alarm_plots_var,
            ).grid(row=1, column=3, columnspan=2, padx=8, pady=(0, 6), sticky="w")

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

            # --- Webhook settings ---
            wh_box = ttk.LabelFrame(alerts_wrap, text=self.t("settings.webhook.title"))
            wh_box.pack(fill="x", padx=8, pady=(6, 4))

            if not hasattr(self, "_wh_enabled_var"):
                self._wh_enabled_var = tk.BooleanVar(value=bool(getattr(self.cfg.ui, "webhook_enabled", False)))
                self._wh_url_var = tk.StringVar(value=str(getattr(self.cfg.ui, "webhook_url", "") or ""))
                self._wh_headers_var = tk.StringVar(value=str(getattr(self.cfg.ui, "webhook_custom_headers", "") or ""))
                self._wh_alarm_var = tk.BooleanVar(value=bool(getattr(self.cfg.ui, "webhook_alarm_enabled", True)))
                self._wh_daily_var = tk.BooleanVar(value=bool(getattr(self.cfg.ui, "webhook_daily_summary_enabled", False)))
                self._wh_monthly_var = tk.BooleanVar(value=bool(getattr(self.cfg.ui, "webhook_monthly_summary_enabled", False)))

            ttk.Checkbutton(
                wh_box,
                text=self.t("settings.webhook.enabled"),
                variable=self._wh_enabled_var,
            ).grid(row=0, column=0, padx=8, pady=6, sticky="w")

            ttk.Label(wh_box, text=self.t("settings.webhook.url") + ":").grid(row=0, column=1, padx=(12, 6), pady=6, sticky="e")
            ttk.Entry(wh_box, textvariable=self._wh_url_var, width=46).grid(row=0, column=2, padx=(0, 8), pady=6, sticky="we", columnspan=2)

            def _wh_send_test() -> None:
                try:
                    self._save_settings()
                except Exception:
                    pass
                def _worker():
                    payload = {
                        "type": "test",
                        "timestamp": datetime.now().isoformat(),
                        "message": "Shelly Energy Analyzer – Webhook Test",
                        "source": "shelly-energy-analyzer",
                    }
                    ok, err = self._webhook_send_sync(payload)
                    def _done():
                        if ok:
                            messagebox.showinfo("Webhook", "OK (gesendet)")
                        else:
                            messagebox.showwarning("Webhook", f"Fehler: {err or 'unbekannt'}")
                    try:
                        self.root.after(0, _done)
                    except Exception:
                        _done()
                try:
                    threading.Thread(target=_worker, daemon=True).start()
                except Exception as _e:
                    messagebox.showwarning("Webhook", f"Fehler: {_e}")

            ttk.Button(wh_box, text=self.t("settings.webhook.test"), command=_wh_send_test).grid(
                row=0, column=4, padx=8, pady=6, sticky="e"
            )

            ttk.Label(wh_box, text=self.t("settings.webhook.headers") + ":").grid(row=1, column=1, padx=(12, 6), pady=(0, 6), sticky="e")
            ttk.Entry(wh_box, textvariable=self._wh_headers_var, width=46).grid(row=1, column=2, padx=(0, 8), pady=(0, 6), sticky="we", columnspan=2)
            ttk.Label(wh_box, text=self.t("settings.webhook.headers.hint"), foreground="gray").grid(row=1, column=4, padx=(0, 8), pady=(0, 6), sticky="w")

            ttk.Checkbutton(
                wh_box,
                text=self.t("settings.webhook.alarm_enabled"),
                variable=self._wh_alarm_var,
            ).grid(row=2, column=0, padx=8, pady=(0, 6), sticky="w")
            ttk.Checkbutton(
                wh_box,
                text=self.t("settings.webhook.daily_summary_enabled"),
                variable=self._wh_daily_var,
            ).grid(row=2, column=2, padx=8, pady=(0, 6), sticky="w")
            ttk.Checkbutton(
                wh_box,
                text=self.t("settings.webhook.monthly_summary_enabled"),
                variable=self._wh_monthly_var,
            ).grid(row=2, column=4, padx=8, pady=(0, 6), sticky="w")

            ttk.Label(wh_box, text=self.t("settings.webhook.hint"), foreground="gray").grid(
                row=3, column=0, columnspan=5, padx=8, pady=(0, 6), sticky="w"
            )

            try:
                wh_box.columnconfigure(2, weight=1)
            except Exception:
                pass

            # --- E-Mail settings ---
            em_box = ttk.LabelFrame(alerts_wrap, text=self.t("settings.email.title"))
            em_box.pack(fill="x", padx=8, pady=(6, 4))

            if not hasattr(self, "_em_enabled_var"):
                self._em_enabled_var = tk.BooleanVar(value=bool(getattr(self.cfg.ui, "email_enabled", False)))
                self._em_smtp_server_var = tk.StringVar(value=str(getattr(self.cfg.ui, "email_smtp_server", "") or ""))
                self._em_smtp_port_var = tk.StringVar(value=str(getattr(self.cfg.ui, "email_smtp_port", 587)))
                self._em_smtp_user_var = tk.StringVar(value=str(getattr(self.cfg.ui, "email_smtp_user", "") or ""))
                self._em_smtp_pass_var = tk.StringVar(value=str(getattr(self.cfg.ui, "email_smtp_password", "") or ""))
                self._em_from_var = tk.StringVar(value=str(getattr(self.cfg.ui, "email_from_address", "") or ""))
                self._em_use_tls_var = tk.BooleanVar(value=bool(getattr(self.cfg.ui, "email_use_tls", True)))
                self._em_recipients_var = tk.StringVar(value=str(getattr(self.cfg.ui, "email_recipients", "") or ""))
                self._em_alarm_var = tk.BooleanVar(value=bool(getattr(self.cfg.ui, "email_alarm_enabled", True)))
                self._em_daily_var = tk.BooleanVar(value=bool(getattr(self.cfg.ui, "email_daily_summary_enabled", False)))
                self._em_daily_time_var = tk.StringVar(value=str(getattr(self.cfg.ui, "email_daily_summary_time", "00:00") or "00:00"))
                self._em_monthly_var = tk.BooleanVar(value=bool(getattr(self.cfg.ui, "email_monthly_summary_enabled", False)))
                self._em_monthly_time_var = tk.StringVar(value=str(getattr(self.cfg.ui, "email_monthly_summary_time", "00:00") or "00:00"))
                self._em_monthly_invoice_var = tk.BooleanVar(value=bool(getattr(self.cfg.ui, "email_monthly_invoice_enabled", False)))

            # Row 0: Enable + SMTP server + port + TLS
            ttk.Checkbutton(
                em_box,
                text=self.t("settings.email.enabled"),
                variable=self._em_enabled_var,
            ).grid(row=0, column=0, padx=8, pady=6, sticky="w")

            ttk.Label(em_box, text=self.t("settings.email.smtp_server") + ":").grid(row=0, column=1, padx=(12, 4), pady=6, sticky="e")
            ttk.Entry(em_box, textvariable=self._em_smtp_server_var, width=30).grid(row=0, column=2, padx=(0, 6), pady=6, sticky="we")

            ttk.Label(em_box, text=self.t("settings.email.smtp_port") + ":").grid(row=0, column=3, padx=(4, 4), pady=6, sticky="e")
            ttk.Entry(em_box, textvariable=self._em_smtp_port_var, width=6).grid(row=0, column=4, padx=(0, 6), pady=6, sticky="w")

            ttk.Checkbutton(
                em_box,
                text=self.t("settings.email.use_tls"),
                variable=self._em_use_tls_var,
            ).grid(row=0, column=5, padx=8, pady=6, sticky="w")

            # Row 1: User + Password + From
            ttk.Label(em_box, text=self.t("settings.email.smtp_user") + ":").grid(row=1, column=1, padx=(12, 4), pady=(0, 6), sticky="e")
            ttk.Entry(em_box, textvariable=self._em_smtp_user_var, width=24).grid(row=1, column=2, padx=(0, 6), pady=(0, 6), sticky="we")

            ttk.Label(em_box, text=self.t("settings.email.smtp_password") + ":").grid(row=1, column=3, padx=(4, 4), pady=(0, 6), sticky="e")
            ttk.Entry(em_box, textvariable=self._em_smtp_pass_var, width=18, show="*").grid(row=1, column=4, padx=(0, 6), pady=(0, 6), sticky="we", columnspan=2)

            # Row 2: From address + Recipients
            ttk.Label(em_box, text=self.t("settings.email.from_address") + ":").grid(row=2, column=1, padx=(12, 4), pady=(0, 6), sticky="e")
            ttk.Entry(em_box, textvariable=self._em_from_var, width=28).grid(row=2, column=2, padx=(0, 6), pady=(0, 6), sticky="we")

            ttk.Label(em_box, text=self.t("settings.email.recipients") + ":").grid(row=2, column=3, padx=(4, 4), pady=(0, 6), sticky="e")
            ttk.Entry(em_box, textvariable=self._em_recipients_var, width=30).grid(row=2, column=4, padx=(0, 6), pady=(0, 6), sticky="we", columnspan=2)

            # Row 3: Alarm + Daily + Monthly toggles + times + Test button
            ttk.Checkbutton(
                em_box,
                text=self.t("settings.email.alarm_enabled"),
                variable=self._em_alarm_var,
            ).grid(row=3, column=0, padx=8, pady=(0, 6), sticky="w")

            ttk.Checkbutton(
                em_box,
                text=self.t("settings.email.daily_summary_enabled"),
                variable=self._em_daily_var,
            ).grid(row=3, column=1, padx=(12, 4), pady=(0, 6), sticky="w", columnspan=2)
            ttk.Label(em_box, text=self.t("settings.email.daily_summary_time") + ":").grid(row=3, column=3, padx=(4, 4), pady=(0, 6), sticky="e")
            ttk.Entry(em_box, textvariable=self._em_daily_time_var, width=7).grid(row=3, column=4, padx=(0, 6), pady=(0, 6), sticky="w")
            ttk.Button(
                em_box,
                text=self.t("settings.email.daily_send_now"),
                command=self._email_send_daily_now,
            ).grid(row=3, column=5, padx=(4, 8), pady=(0, 6), sticky="e")

            ttk.Checkbutton(
                em_box,
                text=self.t("settings.email.monthly_summary_enabled"),
                variable=self._em_monthly_var,
            ).grid(row=4, column=1, padx=(12, 4), pady=(0, 6), sticky="w", columnspan=2)
            ttk.Label(em_box, text=self.t("settings.email.monthly_summary_time") + ":").grid(row=4, column=3, padx=(4, 4), pady=(0, 6), sticky="e")
            ttk.Entry(em_box, textvariable=self._em_monthly_time_var, width=7).grid(row=4, column=4, padx=(0, 6), pady=(0, 6), sticky="w")
            ttk.Button(
                em_box,
                text=self.t("settings.email.monthly_send_now"),
                command=self._email_send_monthly_now,
            ).grid(row=4, column=5, padx=(4, 8), pady=(0, 6), sticky="e")

            # Row 4b: Invoice option for monthly report
            ttk.Checkbutton(
                em_box,
                text=self.t("settings.email.monthly_invoice_enabled"),
                variable=self._em_monthly_invoice_var,
            ).grid(row=5, column=1, padx=(12, 4), pady=(0, 6), sticky="w", columnspan=3)

            # Row 6: Test button (shifted from row 5)
            def _em_send_test() -> None:
                try:
                    self._save_settings()
                except Exception:
                    pass
                def _worker():
                    ok, err = self._email_send_sync(
                        subject="Shelly Energy Analyzer – E-Mail Test",
                        body="Dies ist eine Test-E-Mail vom Shelly Energy Analyzer.\n\nIf you received this, SMTP is configured correctly.",
                    )
                    def _done():
                        if ok:
                            messagebox.showinfo(self.t("settings.email.title"), "OK – E-Mail gesendet")
                        else:
                            messagebox.showwarning(self.t("settings.email.title"), f"Fehler: {err or 'unbekannt'}")
                    try:
                        self.root.after(0, _done)
                    except Exception:
                        _done()
                try:
                    threading.Thread(target=_worker, daemon=True).start()
                except Exception as _e:
                    messagebox.showwarning(self.t("settings.email.title"), f"Fehler: {_e}")

            ttk.Button(em_box, text=self.t("settings.email.test"), command=_em_send_test).grid(
                row=6, column=0, padx=8, pady=(0, 6), sticky="w"
            )

            ttk.Label(em_box, text=self.t("settings.email.hint"), foreground="gray").grid(
                row=7, column=0, columnspan=6, padx=8, pady=(0, 6), sticky="w"
            )

            try:
                em_box.columnconfigure(2, weight=1)
                em_box.columnconfigure(4, weight=1)
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
                ("wh", self.t('settings.alerts.col_webhook'), 8),
                ("em", self.t('settings.alerts.col_email'), 8),
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
                "A", "A_L1", "A_L2", "A_L3", "A_N",
                "VAR", "VAR_L1", "VAR_L2", "VAR_L3",
                "COSPHI", "COSPHI_L1", "COSPHI_L2", "COSPHI_L3",
                "Hz",
            ]
            _op_choices = [">", "<", ">=", "<=", "="]
            for i, row in enumerate(getattr(self, "_alert_vars", []), start=1):
                (v_id, v_en, v_dev, v_met, v_op, v_thr, v_dur, v_cd, v_pop, v_beep, v_tg, v_wh, v_em, v_msg) = row
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

                w_wh = ttk.Checkbutton(atable, variable=v_wh)
                w_wh.grid(row=i, column=11, padx=(0, 10), pady=2, sticky="w")

                w_em = ttk.Checkbutton(atable, variable=v_em)
                w_em.grid(row=i, column=12, padx=(0, 10), pady=2, sticky="w")

                w_msg = ttk.Entry(atable, textvariable=v_msg, width=24)
                w_msg.grid(row=i, column=13, padx=(0, 6), pady=2, sticky="we")

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

                ttk.Button(atable, text="✖", width=2, command=_del_row).grid(row=i, column=14, padx=(0, 0), pady=2, sticky="w")

                # Lock editing for active rules (enabled=True). Use default-args to avoid late-binding bugs.
                def _toggle_row_lock(*_a, v_en=v_en, w_id=w_id, w_dev=w_dev, w_met=w_met, w_op=w_op, w_thr=w_thr, w_dur=w_dur, w_cd=w_cd, w_pop=w_pop, w_beep=w_beep, w_tg=w_tg, w_wh=w_wh, w_em=w_em, w_msg=w_msg):
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
                            w_wh.configure(state="disabled")
                            w_em.configure(state="disabled")
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
                            w_wh.configure(state="normal")
                            w_em.configure(state="normal")
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
            lang_box = ttk.LabelFrame(tab_main_sf, text=self.t('settings.language'))
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

            pricing_box = ttk.LabelFrame(tab_main_sf, text=self.t('settings.pricing.title'))
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

            ttk.Label(pricing_box, text=self.t('settings.pricing.co2_intensity')).grid(row=3, column=0, padx=8, pady=8, sticky="w")
            self.co2_intensity_var = tk.StringVar(value=str(getattr(self.cfg.pricing, 'co2_intensity_g_per_kwh', 380.0)))
            ttk.Entry(pricing_box, textvariable=self.co2_intensity_var, width=10).grid(row=3, column=1, padx=8, pady=8, sticky="w")

            def _co2_preset_menu():
                m = tk.Menu(self, tearoff=0)
                presets = [
                    ("DE ~380 g/kWh", 380.0),
                    ("AT ~120 g/kWh", 120.0),
                    ("CH ~30 g/kWh", 30.0),
                    ("FR ~60 g/kWh", 60.0),
                    ("EU ~255 g/kWh", 255.0),
                    ("Ökostrom / Green 0 g/kWh", 0.0),
                ]
                for label, val in presets:
                    m.add_command(label=label, command=lambda v=val: self.co2_intensity_var.set(str(v)))
                try:
                    btn = getattr(self, '_co2_preset_btn', None)
                    if btn:
                        x = btn.winfo_rootx()
                        y = btn.winfo_rooty() + btn.winfo_height()
                        m.tk_popup(x, y)
                except Exception:
                    pass

            btn_co2_presets = ttk.Button(pricing_box, text=self.t('settings.pricing.co2_presets'), command=_co2_preset_menu)
            btn_co2_presets.grid(row=3, column=2, padx=8, pady=8, sticky="w")
            self._co2_preset_btn = btn_co2_presets

            # ---------- TOU / Mehrtarif settings ----------
            tou_box = ttk.LabelFrame(tab_main_sf, text=self.t('settings.tou.title'))
            tou_box.pack(fill="x", pady=(0, 10))

            _tou_cfg = getattr(self.cfg, "tou", TouConfig())
            self._tou_enabled_var = tk.BooleanVar(value=bool(getattr(_tou_cfg, "enabled", False)))
            ttk.Checkbutton(
                tou_box,
                text=self.t('settings.tou.enabled'),
                variable=self._tou_enabled_var,
            ).grid(row=0, column=0, columnspan=4, padx=8, pady=(8, 2), sticky="w")

            ttk.Label(
                tou_box,
                text=self.t('settings.tou.hint'),
                foreground="gray",
            ).grid(row=1, column=0, columnspan=4, padx=8, pady=(0, 4), sticky="w")

            # Treeview for rates
            tree_frame = ttk.Frame(tou_box)
            tree_frame.grid(row=2, column=0, columnspan=3, padx=8, pady=4, sticky="nsew")
            tou_box.columnconfigure(0, weight=1)
            tou_box.rowconfigure(2, weight=1)

            tou_cols = ("name", "price", "start", "end", "weekdays")
            self._tou_tree = ttk.Treeview(
                tree_frame,
                columns=tou_cols,
                show="headings",
                height=4,
                selectmode="browse",
            )
            self._tou_tree.heading("name", text=self.t('settings.tou.col_name'))
            self._tou_tree.heading("price", text=self.t('settings.tou.col_price'))
            self._tou_tree.heading("start", text=self.t('settings.tou.col_start'))
            self._tou_tree.heading("end", text=self.t('settings.tou.col_end'))
            self._tou_tree.heading("weekdays", text=self.t('settings.tou.col_weekdays'))
            self._tou_tree.column("name", width=80, anchor="w")
            self._tou_tree.column("price", width=100, anchor="center")
            self._tou_tree.column("start", width=70, anchor="center")
            self._tou_tree.column("end", width=70, anchor="center")
            self._tou_tree.column("weekdays", width=100, anchor="center")

            _tou_vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self._tou_tree.yview)
            self._tou_tree.configure(yscrollcommand=_tou_vsb.set)
            self._tou_tree.pack(side="left", fill="both", expand=True)
            _tou_vsb.pack(side="right", fill="y")

            # Populate tree from config
            self._tou_rates_list: List[TouRate] = list(getattr(_tou_cfg, "rates", TouConfig().rates) or [])

            def _tou_refresh_tree():
                for item in self._tou_tree.get_children():
                    self._tou_tree.delete(item)
                for r in self._tou_rates_list:
                    wd = "✓" if r.weekdays_only else "–"
                    self._tou_tree.insert("", "end", values=(r.name, f"{r.price_eur_per_kwh:.4f}", r.start_hour, r.end_hour, wd))
            _tou_refresh_tree()

            def _tou_open_dialog(rate: Optional[TouRate] = None) -> Optional[TouRate]:
                """Open add/edit dialog. Returns new TouRate or None if cancelled."""
                is_edit = rate is not None
                dlg = tk.Toplevel(self)
                dlg.title(self.t('settings.tou.dialog_title_edit') if is_edit else self.t('settings.tou.dialog_title_add'))
                dlg.resizable(False, False)
                dlg.grab_set()
                dlg.transient(self)

                v_name = tk.StringVar(value=rate.name if rate else "HT")
                v_price = tk.StringVar(value=str(rate.price_eur_per_kwh) if rate else "0.35")
                v_start = tk.StringVar(value=str(rate.start_hour) if rate else "6")
                v_end = tk.StringVar(value=str(rate.end_hour) if rate else "22")
                v_wd = tk.BooleanVar(value=rate.weekdays_only if rate else False)

                pad = {"padx": 8, "pady": 4}
                ttk.Label(dlg, text=self.t('settings.tou.field_name')).grid(row=0, column=0, sticky="w", **pad)
                ttk.Entry(dlg, textvariable=v_name, width=12).grid(row=0, column=1, sticky="w", **pad)
                ttk.Label(dlg, text=self.t('settings.tou.field_price')).grid(row=1, column=0, sticky="w", **pad)
                ttk.Entry(dlg, textvariable=v_price, width=12).grid(row=1, column=1, sticky="w", **pad)
                ttk.Label(dlg, text=self.t('settings.tou.field_start')).grid(row=2, column=0, sticky="w", **pad)
                ttk.Entry(dlg, textvariable=v_start, width=6).grid(row=2, column=1, sticky="w", **pad)
                ttk.Label(dlg, text=self.t('settings.tou.field_end')).grid(row=3, column=0, sticky="w", **pad)
                ttk.Entry(dlg, textvariable=v_end, width=6).grid(row=3, column=1, sticky="w", **pad)
                ttk.Checkbutton(dlg, text=self.t('settings.tou.field_weekdays'), variable=v_wd).grid(
                    row=4, column=0, columnspan=2, sticky="w", **pad
                )

                result: List[Optional[TouRate]] = [None]

                def _ok():
                    try:
                        name = v_name.get().strip() or "HT"
                        price = float(v_price.get().strip().replace(",", "."))
                        s_h = int(v_start.get().strip()) % 24
                        e_h = int(v_end.get().strip()) % 24
                        result[0] = TouRate(name=name, price_eur_per_kwh=price, start_hour=s_h, end_hour=e_h, weekdays_only=bool(v_wd.get()))
                        dlg.destroy()
                    except Exception as ex:
                        messagebox.showerror("Error", str(ex), parent=dlg)

                btn_frame = ttk.Frame(dlg)
                btn_frame.grid(row=5, column=0, columnspan=2, pady=8)
                ttk.Button(btn_frame, text=self.t('btn.apply'), command=_ok).pack(side="left", padx=4)
                ttk.Button(btn_frame, text=self.t('btn.reset'), command=dlg.destroy).pack(side="left", padx=4)
                dlg.wait_window()
                return result[0]

            def _tou_add():
                new_rate = _tou_open_dialog()
                if new_rate is not None:
                    self._tou_rates_list.append(new_rate)
                    _tou_refresh_tree()

            def _tou_edit():
                sel = self._tou_tree.selection()
                if not sel:
                    return
                idx = self._tou_tree.index(sel[0])
                if 0 <= idx < len(self._tou_rates_list):
                    new_rate = _tou_open_dialog(self._tou_rates_list[idx])
                    if new_rate is not None:
                        self._tou_rates_list[idx] = new_rate
                        _tou_refresh_tree()

            def _tou_remove():
                sel = self._tou_tree.selection()
                if not sel:
                    return
                idx = self._tou_tree.index(sel[0])
                if 0 <= idx < len(self._tou_rates_list):
                    self._tou_rates_list.pop(idx)
                    _tou_refresh_tree()

            btn_col = ttk.Frame(tou_box)
            btn_col.grid(row=2, column=3, padx=(4, 8), pady=4, sticky="n")
            ttk.Button(btn_col, text=self.t('settings.tou.add'), command=_tou_add, width=10).pack(pady=2)
            ttk.Button(btn_col, text=self.t('settings.tou.edit'), command=_tou_edit, width=10).pack(pady=2)
            ttk.Button(btn_col, text=self.t('settings.tou.remove'), command=_tou_remove, width=10).pack(pady=2)

            autosync_box = ttk.LabelFrame(tab_main_sf, text=self.t('settings.autosync.title'))
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

            # ---------- Appearance / Theme ----------
            appearance_box = ttk.LabelFrame(tab_main_sf, text=self.t('settings.appearance.title'))
            appearance_box.pack(fill="x", pady=(0, 10))
            _theme_mode0 = str(getattr(self.cfg.ui, "plot_theme_mode", "auto") or "auto").strip().lower()
            if _theme_mode0 not in ("auto", "day", "night"):
                _theme_mode0 = "auto"
            _theme_labels = {
                "auto": self.t('settings.appearance.theme.auto'),
                "day": self.t('settings.appearance.theme.day'),
                "night": self.t('settings.appearance.theme.night'),
            }
            self._set_theme_mode_var = tk.StringVar(value=_theme_labels.get(_theme_mode0, _theme_labels["auto"]))
            self._theme_label_to_mode = {v: k for k, v in _theme_labels.items()}
            ttk.Label(appearance_box, text=self.t('settings.appearance.theme')).grid(row=0, column=0, padx=8, pady=8, sticky='w')
            ttk.Combobox(
                appearance_box,
                values=list(_theme_labels.values()),
                width=18,
                textvariable=self._set_theme_mode_var,
                state="readonly",
            ).grid(row=0, column=1, padx=8, pady=8, sticky='w')

            # ---------- Solar / PV settings ----------
            solar_box = ttk.LabelFrame(tab_main_sf, text=self.t('settings.solar.title'))
            solar_box.pack(fill="x", pady=(0, 10))
            _solar_cfg = getattr(self.cfg, "solar", None)
            self._solar_enabled_var = tk.BooleanVar(value=bool(getattr(_solar_cfg, "enabled", False)))
            ttk.Checkbutton(
                solar_box,
                text=self.t('settings.solar.enabled'),
                variable=self._solar_enabled_var,
            ).grid(row=0, column=0, columnspan=3, padx=8, pady=(8, 2), sticky="w")

            ttk.Label(solar_box, text=self.t('settings.solar.pv_meter')).grid(row=1, column=0, padx=8, pady=4, sticky="w")
            _dev_names_for_solar = [self.t('settings.solar.none')] + [d.name for d in (getattr(self.cfg, "devices", []) or [])]
            _dev_keys_for_solar = [""] + [d.key for d in (getattr(self.cfg, "devices", []) or [])]
            _cur_pv_key = str(getattr(_solar_cfg, "pv_meter_device_key", "") or "")
            _cur_pv_idx = _dev_keys_for_solar.index(_cur_pv_key) if _cur_pv_key in _dev_keys_for_solar else 0
            self._solar_pv_meter_var = tk.StringVar(value=_dev_names_for_solar[_cur_pv_idx])
            self._solar_dev_keys_list = _dev_keys_for_solar
            self._solar_dev_names_list = _dev_names_for_solar
            ttk.Combobox(
                solar_box,
                textvariable=self._solar_pv_meter_var,
                values=_dev_names_for_solar,
                width=26,
                state="readonly",
            ).grid(row=1, column=1, padx=8, pady=4, sticky="w")
            ttk.Label(
                solar_box,
                text=self.t('settings.solar.pv_meter.hint'),
                foreground="gray",
            ).grid(row=1, column=2, padx=8, pady=4, sticky="w")

            ttk.Label(solar_box, text=self.t('settings.solar.feed_in_tariff')).grid(row=2, column=0, padx=8, pady=(4, 8), sticky="w")
            self._solar_tariff_var = tk.StringVar(value=str(getattr(_solar_cfg, "feed_in_tariff_eur_per_kwh", 0.082)))
            ttk.Entry(solar_box, textvariable=self._solar_tariff_var, width=10).grid(row=2, column=1, padx=8, pady=(4, 8), sticky="w")

            # ── CO₂ / ENTSO-E settings ────────────────────────────────────────
            _co2_cfg = getattr(self.cfg, "co2", None)
            co2_box = ttk.LabelFrame(tab_main_sf, text=self.t("co2.settings.title"))
            co2_box.pack(fill="x", pady=(0, 10))

            self._co2_enabled_var = tk.BooleanVar(value=bool(getattr(_co2_cfg, "enabled", False)))
            ttk.Checkbutton(
                co2_box,
                text=self.t("co2.settings.enabled"),
                variable=self._co2_enabled_var,
            ).grid(row=0, column=0, columnspan=4, padx=8, pady=(6, 2), sticky="w")

            ttk.Label(co2_box, text=self.t("co2.settings.token")).grid(row=1, column=0, padx=8, pady=4, sticky="w")
            self._co2_token_var = tk.StringVar(value=str(getattr(_co2_cfg, "entso_e_api_token", "") or ""))
            ttk.Entry(co2_box, textvariable=self._co2_token_var, width=40, show="*").grid(row=1, column=1, columnspan=3, padx=8, pady=4, sticky="w")

            ttk.Label(co2_box, text=self.t("co2.settings.zone")).grid(row=2, column=0, padx=8, pady=4, sticky="w")
            self._co2_zone_var = tk.StringVar(value=str(getattr(_co2_cfg, "bidding_zone", "DE_LU") or "DE_LU"))
            _zone_options = [
                "AT", "BE", "BG", "CH", "CZ", "DE_LU", "DK_1", "DK_2",
                "EE", "ES", "FI", "FR", "GR", "HR", "HU", "IE_SEM",
                "IT_CNOR", "IT_CSUD", "IT_NORD", "IT_SUD", "LT", "LV",
                "NL", "NO_1", "NO_2", "NO_3", "NO_4", "NO_5",
                "PL", "PT", "RO", "RS", "SE_1", "SE_2", "SE_3", "SE_4",
                "SI", "SK", "GB",
            ]
            ttk.Combobox(
                co2_box,
                textvariable=self._co2_zone_var,
                values=_zone_options,
                width=12,
                state="readonly",
            ).grid(row=2, column=1, padx=8, pady=4, sticky="w")

            ttk.Label(co2_box, text=self.t("co2.settings.interval")).grid(row=3, column=0, padx=8, pady=4, sticky="w")
            self._co2_interval_var = tk.IntVar(value=int(getattr(_co2_cfg, "fetch_interval_hours", 1)))
            ttk.Entry(co2_box, textvariable=self._co2_interval_var, width=6).grid(row=3, column=1, padx=8, pady=4, sticky="w")

            ttk.Label(co2_box, text=self.t("co2.settings.backfill")).grid(row=3, column=2, padx=8, pady=4, sticky="w")
            self._co2_backfill_var = tk.IntVar(value=int(getattr(_co2_cfg, "backfill_days", 7)))
            ttk.Entry(co2_box, textvariable=self._co2_backfill_var, width=6).grid(row=3, column=3, padx=8, pady=4, sticky="w")

            ttk.Label(co2_box, text=self.t("co2.settings.green_threshold")).grid(row=4, column=0, padx=8, pady=4, sticky="w")
            self._co2_green_thr_var = tk.DoubleVar(value=float(getattr(_co2_cfg, "green_threshold_g_per_kwh", 150.0)))
            ttk.Entry(co2_box, textvariable=self._co2_green_thr_var, width=8).grid(row=4, column=1, padx=8, pady=4, sticky="w")

            ttk.Label(co2_box, text=self.t("co2.settings.dirty_threshold")).grid(row=4, column=2, padx=8, pady=4, sticky="w")
            self._co2_dirty_thr_var = tk.DoubleVar(value=float(getattr(_co2_cfg, "dirty_threshold_g_per_kwh", 400.0)))
            ttk.Entry(co2_box, textvariable=self._co2_dirty_thr_var, width=8).grid(row=4, column=3, padx=8, pady=4, sticky="w")

            def _co2_backfill_now():
                token = getattr(self, "_co2_token_var", tk.StringVar()).get().strip()
                zone = (getattr(self, "_co2_zone_var", tk.StringVar(value="DE_LU")).get() or "DE_LU").strip()
                try:
                    backfill_days = int(getattr(self, "_co2_backfill_var", tk.IntVar(value=7)).get() or 7)
                except Exception:
                    backfill_days = 7

                if not token:
                    self._co2_status_var.set(self.t("co2.error.no_token"))
                    return

                def _log(msg):
                    try:
                        self.after(0, lambda m=msg: self._log_sync(m))
                    except Exception:
                        pass

                def _put_progress(day, total):
                    try:
                        q = getattr(self, "_co2_progress_q", None)
                        if q is not None:
                            q.put((day, total))
                    except Exception:
                        pass

                self._co2_status_var.set(self.t("co2.status.fetching"))
                _log(f"CO₂ Backfill gestartet: {backfill_days} Tage, Zone {zone}")

                def _run():
                    import math as _math
                    import time as _time
                    from datetime import datetime as _dt, timezone as _tz
                    from shelly_analyzer.services.entsoe import EntsoeClient

                    try:
                        client = EntsoeClient(
                            api_token=token,
                            bidding_zone=zone,
                            min_request_interval=1.0,
                        )
                        now_ts = int(_time.time())
                        start_ts = (now_ts - backfill_days * 86400) // 3600 * 3600
                        end_ts = ((now_ts // 3600) + 1) * 3600
                        total_days = max(1, _math.ceil((end_ts - start_ts) / 86400))
                        d_from = _dt.fromtimestamp(start_ts, tz=_tz.utc).strftime("%Y-%m-%d")
                        d_to = _dt.fromtimestamp(end_ts, tz=_tz.utc).strftime("%Y-%m-%d")
                        _log(f"  Zeitraum: {d_from} → {d_to} ({total_days} Tage)")

                        chunk_s = 7 * 86400
                        all_rows = []
                        cursor = start_ts
                        days_fetched = 0

                        while cursor < end_ts:
                            chunk_end = min(cursor + chunk_s, end_ts)
                            c_from = _dt.fromtimestamp(cursor, tz=_tz.utc).strftime("%Y-%m-%d")
                            c_to = _dt.fromtimestamp(chunk_end, tz=_tz.utc).strftime("%Y-%m-%d")
                            _log(f"  ENTSO-E Abfrage: {c_from} bis {c_to}...")
                            _put_progress(days_fetched, total_days)
                            try:
                                rows = client.fetch_intensity(cursor, chunk_end)
                                all_rows.extend(rows)
                                _log(f"    Empfangen: {len(rows)} Datenpunkte")
                            except Exception as exc:
                                _log(f"    Fehler: {exc}")
                                try:
                                    self.after(0, lambda e=str(exc)[:80]: self._co2_status_var.set(e))
                                except Exception:
                                    pass
                                break
                            days_fetched += max(1, round((chunk_end - cursor) / 86400))
                            cursor = chunk_end

                        _put_progress(total_days, total_days)

                        if all_rows:
                            written = self.storage.db.upsert_co2_intensity(all_rows)
                            done = f"CO₂ Backfill abgeschlossen: {written} Werte importiert"
                        else:
                            done = "CO₂ Backfill abgeschlossen: 0 Werte – keine Daten empfangen"
                        _log(done)
                        try:
                            self.after(0, lambda m=done[:80]: self._co2_status_var.set(m))
                            self.after(200, self._refresh_co2_tab)
                        except Exception:
                            pass

                    except Exception as exc:
                        err = f"CO₂ Backfill Fehler: {exc}"
                        logger.exception("CO2 backfill thread error")
                        _log(err)
                        try:
                            self.after(0, lambda m=err[:80]: self._co2_status_var.set(m))
                        except Exception:
                            pass

                threading.Thread(target=_run, daemon=True).start()

            ttk.Button(
                co2_box,
                text=self.t("co2.settings.backfill_btn"),
                command=_co2_backfill_now,
            ).grid(row=5, column=0, columnspan=2, padx=8, pady=(4, 8), sticky="w")

            # ── Test Connection button ────────────────────────────────────────
            self._co2_test_status_var = tk.StringVar(value="")
            self._co2_test_label = ttk.Label(
                co2_box,
                textvariable=self._co2_test_status_var,
            )
            self._co2_test_label.grid(row=6, column=0, columnspan=4, padx=8, pady=(0, 6), sticky="w")

            def _co2_test_connection():
                token = self._co2_token_var.get().strip()
                zone = self._co2_zone_var.get().strip() or "DE_LU"
                if not token:
                    self._co2_test_status_var.set(self.t("co2.error.no_token"))
                    try:
                        self._co2_test_label.configure(foreground="#c62828")
                    except Exception:
                        pass
                    return
                self._co2_test_status_var.set("…")
                try:
                    self._co2_test_label.configure(foreground="gray")
                except Exception:
                    pass

                def _run():
                    from shelly_analyzer.services.entsoe import EntsoeClient
                    import time as _time
                    client = EntsoeClient(
                        api_token=token,
                        bidding_zone=zone,
                        min_request_interval=0,
                    )
                    end_ts = (_time.time() // 3600) * 3600
                    start_ts = int(end_ts) - 3600
                    try:
                        client.fetch_intensity(int(start_ts), int(end_ts))

                        def _ok():
                            self._co2_test_status_var.set(self.t("co2.settings.test_ok"))
                            try:
                                self._co2_test_label.configure(foreground="#2e7d32")
                            except Exception:
                                pass
                        try:
                            self.after(0, _ok)
                        except Exception:
                            _ok()
                    except Exception as exc:
                        err_str = str(exc)
                        if "401" in err_str or "nauthorized" in err_str or "ecurity" in err_str:
                            short = "Token ungültig / Invalid token"
                        elif "503" in err_str or "502" in err_str or "504" in err_str:
                            short = "ENTSO-E API nicht erreichbar (HTTP 5xx)"
                        elif "timeout" in err_str.lower() or "onnect" in err_str:
                            short = "API nicht erreichbar / Unreachable"
                        else:
                            short = err_str[:80]
                        msg = self.t("co2.settings.test_fail", err=short)

                        def _fail(m=msg):
                            self._co2_test_status_var.set(m)
                            try:
                                self._co2_test_label.configure(foreground="#c62828")
                            except Exception:
                                pass
                        try:
                            self.after(0, _fail)
                        except Exception:
                            _fail()

                threading.Thread(target=_run, daemon=True).start()

            ttk.Button(
                co2_box,
                text=self.t("co2.settings.test_btn"),
                command=_co2_test_connection,
            ).grid(row=5, column=2, columnspan=2, padx=8, pady=(4, 8), sticky="w")

            live_box = ttk.LabelFrame(tab_main_sf, text=self.t('settings.live.title'))
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

            web_box = ttk.LabelFrame(tab_main_sf, text=self.t('settings.web.title'))
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

            # Logo picker
            self.bill_logo_path = tk.StringVar(value=str(getattr(self.cfg.billing, "invoice_logo_path", "") or ""))
            ttk.Label(inv_box, text=self.t('billing.field.logo')).grid(row=1, column=0, padx=8, pady=6, sticky="w")
            ttk.Entry(inv_box, textvariable=self.bill_logo_path, width=40).grid(row=1, column=1, columnspan=2, padx=8, pady=6, sticky="w")
            def _pick_logo():
                from tkinter import filedialog
                fp = filedialog.askopenfilename(
                    title=self.t("billing.field.logo"),
                    filetypes=[("Images", "*.png *.jpg *.jpeg *.gif *.bmp"), ("All", "*.*")],
                )
                if fp:
                    self.bill_logo_path.set(fp)
            ttk.Button(inv_box, text="…", command=_pick_logo, width=3).grid(row=1, column=3, padx=8, pady=6, sticky="w")

            # bottom action bar (always visible)
            bottom = ttk.Frame(frm)
            bottom.pack(fill="x", padx=12, pady=(0, 12))
            ttk.Button(bottom, text=self.t('settings.save'), command=self._save_settings).pack(side="left")
            ttk.Button(bottom, text=self.t('settings.reload'), command=self._reload_settings).pack(side="left", padx=8)
            self.settings_status = tk.StringVar(value=f"config.json: {self.cfg_path}")
            ttk.Label(bottom, textvariable=self.settings_status).pack(side="left", padx=12)

    def _build_groups_settings_tab(self, parent: ttk.Frame) -> None:
            """Build the Groups editor subtab in Settings."""
            # Scrollable container
            outer = ttk.Frame(parent)
            outer.pack(fill="both", expand=True)
            canvas = tk.Canvas(outer, highlightthickness=0)
            vsb = ttk.Scrollbar(outer, orient="vertical", command=canvas.yview)
            canvas.configure(yscrollcommand=vsb.set)
            vsb.pack(side="right", fill="y")
            canvas.pack(side="left", fill="both", expand=True)
            inner = ttk.Frame(canvas)
            win = canvas.create_window((0, 0), window=inner, anchor="nw")
            inner.bind("<Configure>", lambda _e: canvas.configure(scrollregion=canvas.bbox("all")))
            canvas.bind("<Configure>", lambda e: canvas.itemconfigure(win, width=e.width))

            def _bind_wheel(w):
                w.bind("<Enter>", lambda _e: canvas.bind_all("<MouseWheel>", lambda e: canvas.yview_scroll(int(-1 * (e.delta / 120)), "units")))
                w.bind("<Leave>", lambda _e: canvas.unbind_all("<MouseWheel>"))

            _bind_wheel(canvas)
            _bind_wheel(inner)

            ttk.Label(inner, text=self.t("groups.title"), font=("TkDefaultFont", 13, "bold")).pack(anchor="w", padx=12, pady=(12, 4))

            # Groups container (rebuilt on add/delete)
            groups_frame = ttk.Frame(inner)
            groups_frame.pack(fill="x", padx=12, pady=4)

            status_var = tk.StringVar(value="")
            ttk.Label(inner, textvariable=status_var, foreground="#888888").pack(anchor="w", padx=12)

            # State: list of (name_var, {device_key: BooleanVar})
            if not getattr(self, "_group_vars", None):
                self._group_vars: List[Tuple[tk.StringVar, dict]] = []
                for g in (getattr(self.cfg, "groups", []) or []):
                    name_var = tk.StringVar(value=g.name)
                    dev_checks = {d.key: tk.BooleanVar(value=(d.key in g.device_keys)) for d in self.cfg.devices}
                    self._group_vars.append((name_var, dev_checks))

            def _rebuild_groups_ui() -> None:
                for child in list(groups_frame.winfo_children()):
                    try:
                        child.destroy()
                    except Exception:
                        pass
                if not self._group_vars:
                    ttk.Label(groups_frame, text=self.t("groups.no_groups"), foreground="#888888").pack(anchor="w", pady=8)
                    return
                for idx, (name_var, dev_checks) in enumerate(self._group_vars):
                    grp_frame = ttk.LabelFrame(groups_frame, text=f"#{idx + 1}")
                    grp_frame.pack(fill="x", pady=(4, 2))

                    name_row = ttk.Frame(grp_frame)
                    name_row.pack(fill="x", padx=8, pady=(6, 2))
                    ttk.Label(name_row, text=self.t("groups.name"), width=8).pack(side="left")
                    ttk.Entry(name_row, textvariable=name_var, width=30).pack(side="left", padx=4)

                    def _delete_group(i=idx) -> None:
                        try:
                            self._group_vars.pop(i)
                        except Exception:
                            pass
                        _rebuild_groups_ui()

                    ttk.Button(name_row, text=self.t("groups.delete"), command=_delete_group).pack(side="left", padx=8)

                    dev_row = ttk.Frame(grp_frame)
                    dev_row.pack(fill="x", padx=8, pady=(2, 6))
                    ttk.Label(dev_row, text=self.t("groups.devices")).pack(side="left", anchor="n", pady=2)
                    cb_frame = ttk.Frame(dev_row)
                    cb_frame.pack(side="left", padx=4)
                    for d in self.cfg.devices:
                        if d.key not in dev_checks:
                            dev_checks[d.key] = tk.BooleanVar(value=False)
                        ttk.Checkbutton(cb_frame, text=f"{d.name} ({d.key})", variable=dev_checks[d.key]).pack(anchor="w")

            _rebuild_groups_ui()

            def _add_group() -> None:
                name_var = tk.StringVar(value=f"Gruppe {len(self._group_vars) + 1}")
                dev_checks = {d.key: tk.BooleanVar(value=False) for d in self.cfg.devices}
                self._group_vars.append((name_var, dev_checks))
                _rebuild_groups_ui()

            def _save_groups() -> None:
                from shelly_analyzer.io.config import DeviceGroup as _DG
                new_groups = []
                for name_var, dev_checks in self._group_vars:
                    name = str(name_var.get() or "").strip()
                    if not name:
                        status_var.set(self.t("groups.empty_name"))
                        return
                    keys = [k for k, v in dev_checks.items() if v.get()]
                    new_groups.append(_DG(name=name, device_keys=keys))
                self.cfg = replace(self.cfg, groups=new_groups)
                try:
                    save_config(self.cfg, self.cfg_path)
                    status_var.set(self.t("groups.saved"))
                except Exception as e:
                    status_var.set(str(e))
                # Refresh dropdown to include new groups
                try:
                    self._update_device_page_choices()
                except Exception:
                    pass

            btn_row = ttk.Frame(inner)
            btn_row.pack(fill="x", padx=12, pady=(8, 4))
            ttk.Button(btn_row, text=self.t("groups.add"), command=_add_group).pack(side="left")
            ttk.Button(btn_row, text=self.t("groups.save"), command=_save_groups).pack(side="left", padx=8)

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
            try:
                co2_intensity = float(self.co2_intensity_var.get().strip().replace(",", "."))
            except Exception:
                co2_intensity = float(getattr(self.cfg.pricing, 'co2_intensity_g_per_kwh', 380.0))
            pricing = PricingConfig(
                electricity_price_eur_per_kwh=price,
                base_fee_eur_per_year=base_fee,
                base_fee_includes_vat=bool(self.base_fee_includes_vat_var.get()),
                price_includes_vat=bool(self.price_includes_vat_var.get()),
                vat_enabled=bool(self.vat_enabled_var.get()),
                vat_rate_percent=vat_rate,
                co2_intensity_g_per_kwh=co2_intensity,
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

            # Global plot theme
            try:
                _theme_label = str(getattr(self, '_set_theme_mode_var', tk.StringVar(value='')).get() or '').strip()
                _theme_map = getattr(self, '_theme_label_to_mode', {})
                plot_theme_mode = _theme_map.get(_theme_label, 'auto')
            except Exception:
                plot_theme_mode = str(getattr(self.cfg.ui, 'plot_theme_mode', 'auto') or 'auto')
            if plot_theme_mode not in ('auto', 'day', 'night'):
                plot_theme_mode = 'auto'

            ui = UiConfig(
                live_poll_seconds=live_poll_s,
                language=sel_lang,
                plot_theme_mode=plot_theme_mode,
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
    telegram_alarm_plots_enabled=bool(getattr(self, "_tg_alarm_plots_var", tk.BooleanVar(value=True)).get()),
    telegram_daily_summary_enabled=bool(getattr(self, "_tg_daily_sum_var", tk.BooleanVar(value=False)).get()),
    telegram_daily_summary_time=str(getattr(self, "_tg_daily_time_var", tk.StringVar(value="00:00")).get() or "00:00"),
    telegram_monthly_summary_enabled=bool(getattr(self, "_tg_monthly_sum_var", tk.BooleanVar(value=False)).get()),
    telegram_monthly_summary_time=str(getattr(self, "_tg_monthly_time_var", tk.StringVar(value="00:00")).get() or "00:00"),
    telegram_daily_summary_last_sent=str(getattr(self.cfg.ui, "telegram_daily_summary_last_sent", "") or ""),
    telegram_summary_load_w=float(getattr(self, "_tg_sum_loadw_var", tk.StringVar(value="200")).get() or 200.0),
    telegram_monthly_summary_last_sent=str(getattr(self.cfg.ui, "telegram_monthly_summary_last_sent", "") or ""),

    webhook_enabled=bool(getattr(self, "_wh_enabled_var", tk.BooleanVar(value=False)).get()),
    webhook_url=str(getattr(self, "_wh_url_var", tk.StringVar(value="")).get() or ""),
    webhook_custom_headers=str(getattr(self, "_wh_headers_var", tk.StringVar(value="")).get() or ""),
    webhook_alarm_enabled=bool(getattr(self, "_wh_alarm_var", tk.BooleanVar(value=True)).get()),
    webhook_daily_summary_enabled=bool(getattr(self, "_wh_daily_var", tk.BooleanVar(value=False)).get()),
    webhook_monthly_summary_enabled=bool(getattr(self, "_wh_monthly_var", tk.BooleanVar(value=False)).get()),
    email_enabled=bool(getattr(self, "_em_enabled_var", tk.BooleanVar(value=False)).get()),
    email_smtp_server=str(getattr(self, "_em_smtp_server_var", tk.StringVar(value="")).get() or ""),
    email_smtp_port=int(str(getattr(self, "_em_smtp_port_var", tk.StringVar(value="587")).get() or "587").strip() or "587"),
    email_smtp_user=str(getattr(self, "_em_smtp_user_var", tk.StringVar(value="")).get() or ""),
    email_smtp_password=str(getattr(self, "_em_smtp_pass_var", tk.StringVar(value="")).get() or ""),
    email_from_address=str(getattr(self, "_em_from_var", tk.StringVar(value="")).get() or ""),
    email_use_tls=bool(getattr(self, "_em_use_tls_var", tk.BooleanVar(value=True)).get()),
    email_recipients=str(getattr(self, "_em_recipients_var", tk.StringVar(value="")).get() or ""),
    email_alarm_enabled=bool(getattr(self, "_em_alarm_var", tk.BooleanVar(value=True)).get()),
    email_daily_summary_enabled=bool(getattr(self, "_em_daily_var", tk.BooleanVar(value=False)).get()),
    email_monthly_summary_enabled=bool(getattr(self, "_em_monthly_var", tk.BooleanVar(value=False)).get()),
    email_daily_summary_time=str(getattr(self, "_em_daily_time_var", tk.StringVar(value="00:00")).get() or "00:00"),
    email_monthly_summary_time=str(getattr(self, "_em_monthly_time_var", tk.StringVar(value="00:00")).get() or "00:00"),
    email_monthly_invoice_enabled=bool(getattr(self, "_em_monthly_invoice_var", tk.BooleanVar(value=False)).get()),

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
                invoice_logo_path=str(getattr(self, "bill_logo_path", tk.StringVar()).get()).strip(),
            )

            # Collect alert rules from the Settings UI (persisted in config.json)
            alerts: List[AlertRule] = []
            try:
                for row in getattr(self, "_alert_vars", []) or []:
                    (v_id, v_en, v_dev, v_met, v_op, v_thr, v_dur, v_cd, v_pop, v_beep, v_tg, v_wh, v_em, v_msg) = row
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
                            action_webhook=bool(v_wh.get()),
                            action_email=bool(v_em.get()),
                            message=str(v_msg.get() or ""),
                        )
                    )
            except Exception:
                pass

        
            # Updates settings (persist auto-install checkbox)
            try:
                updates = replace(getattr(self.cfg, "updates", UpdatesConfig()), auto_install=bool(self.upd_auto.get()))
            except Exception:
                updates = UpdatesConfig(auto_install=bool(self.upd_auto.get()))

            # Solar / PV config
            try:
                _pv_name = str(getattr(self, "_solar_pv_meter_var", tk.StringVar()).get() or "")
                _names_list = getattr(self, "_solar_dev_names_list", [])
                _keys_list = getattr(self, "_solar_dev_keys_list", [])
                if _pv_name in _names_list:
                    _pv_key = _keys_list[_names_list.index(_pv_name)]
                else:
                    _pv_key = ""
                _tariff_raw = str(getattr(self, "_solar_tariff_var", tk.StringVar(value="0.082")).get() or "0.082")
                try:
                    _tariff = float(_tariff_raw.replace(",", "."))
                except Exception:
                    _tariff = 0.082
                solar = SolarConfig(
                    enabled=bool(getattr(self, "_solar_enabled_var", tk.BooleanVar(value=False)).get()),
                    pv_meter_device_key=_pv_key,
                    feed_in_tariff_eur_per_kwh=_tariff,
                )
            except Exception:
                solar = getattr(self.cfg, "solar", SolarConfig())

            # TOU / Mehrtarif config
            try:
                tou = TouConfig(
                    enabled=bool(getattr(self, "_tou_enabled_var", tk.BooleanVar(value=False)).get()),
                    rates=list(getattr(self, "_tou_rates_list", TouConfig().rates) or TouConfig().rates),
                )
            except Exception:
                tou = getattr(self.cfg, "tou", TouConfig())

            # CO₂ / ENTSO-E config
            try:
                co2 = Co2Config(
                    enabled=bool(getattr(self, "_co2_enabled_var", tk.BooleanVar(value=False)).get()),
                    entso_e_api_token=str(getattr(self, "_co2_token_var", tk.StringVar()).get() or ""),
                    bidding_zone=str(getattr(self, "_co2_zone_var", tk.StringVar(value="DE_LU")).get() or "DE_LU"),
                    fetch_interval_hours=int(getattr(self, "_co2_interval_var", tk.IntVar(value=1)).get() or 1),
                    backfill_days=int(getattr(self, "_co2_backfill_var", tk.IntVar(value=7)).get() or 7),
                    show_green_dirty_hours=True,
                    green_threshold_g_per_kwh=float(getattr(self, "_co2_green_thr_var", tk.DoubleVar(value=150.0)).get() or 150.0),
                    dirty_threshold_g_per_kwh=float(getattr(self, "_co2_dirty_thr_var", tk.DoubleVar(value=400.0)).get() or 400.0),
                )
            except Exception:
                co2 = getattr(self.cfg, "co2", Co2Config())

            self.cfg = AppConfig(
                version=__version__,
                devices=devs,
                download=download,
                csv_pack=csv_pack,
                ui=ui,
                updates=updates,
                pricing=pricing,
                billing=billing,
                alerts=alerts,
                solar=solar,
                tou=tou,
                co2=co2,
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
            # Reset cached group vars so they reflect reloaded config
            self._group_vars = None
            messagebox.showinfo(self.t('msg.settings'), self.t('settings.reload.done') + "\n" + self.t('settings.reload.note'))

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
            # Progress bar updates from sync worker
            while True:
                try:
                    pct, maximum, lbl = self._progress_q.get_nowait()
                except queue.Empty:
                    break
                try:
                    if self._sync_progressbar is not None:
                        self._sync_progressbar["value"] = pct
                    if self._sync_progress_label_var is not None:
                        self._sync_progress_label_var.set(lbl)
                except Exception:
                    pass

            # Sync messages
            while True:
                try:
                    msg = self._sync_q.get_nowait()
                except queue.Empty:
                    break
                if msg == "__SYNC_DONE__":
                    self._log_sync("Sync beendet.")
                    # Clear progress bar after sync finishes
                    try:
                        if self._sync_progressbar is not None:
                            self._sync_progressbar["value"] = 0
                        if self._sync_progress_label_var is not None:
                            self._sync_progress_label_var.set("")
                    except Exception:
                        pass
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
            # Scheduled Webhook summaries
            try:
                self._webhook_summary_tick()
            except Exception:
                pass
            # Scheduled E-Mail summaries
            try:
                self._email_summary_tick()
            except Exception:
                pass

            # CO₂ import progress bar
            try:
                co2_q = getattr(self, "_co2_progress_q", None)
                if co2_q is not None:
                    day = total = None
                    while True:
                        try:
                            day, total = co2_q.get_nowait()
                        except queue.Empty:
                            break
                    # Process only the latest queued update (discard intermediate)
                    if day is not None and total is not None:
                        pb = getattr(self, "_co2_progressbar", None)
                        lv = getattr(self, "_co2_progress_label_var", None)
                        if pb is not None:
                            if total > 0 and day < total:
                                pb["value"] = int(day * 100 / total)
                                if lv is not None:
                                    lv.set(self.t("co2.import.progress", day=day, total=total))
                            else:
                                pb["value"] = 0
                                if lv is not None:
                                    lv.set(self.t("co2.import.done"))
                                def _clear_co2_progress():
                                    try:
                                        lv2 = getattr(self, "_co2_progress_label_var", None)
                                        if lv2 is not None:
                                            lv2.set("")
                                    except Exception:
                                        pass
                                try:
                                    self.after(3000, _clear_co2_progress)
                                except Exception:
                                    pass
                                # Refresh CO₂ tab so newly imported data is shown immediately
                                try:
                                    self.after(500, self._refresh_co2_tab)
                                except Exception:
                                    pass
            except Exception:
                pass

            self.after(500, self._drain_queues_loop)

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
                    tk.BooleanVar(value=False),
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


    def _webhook_send_sync(self, payload: dict) -> tuple[bool, str]:
            """Send a generic JSON webhook HTTP POST and return (ok, error_message)."""
            try:
                if not bool(getattr(self.cfg.ui, "webhook_enabled", False)):
                    return False, "Webhook ist deaktiviert"
                url = str(getattr(self.cfg.ui, "webhook_url", "") or "").strip()
                if not url:
                    return False, "Webhook-URL fehlt"
            except Exception as e:
                return False, str(e)

            try:
                body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
                req = urllib.request.Request(url, data=body, method="POST")
                req.add_header("Content-Type", "application/json")
                req.add_header("User-Agent", "ShellyEnergyAnalyzer")

                # Parse and apply custom headers (JSON object string)
                headers_str = str(getattr(self.cfg.ui, "webhook_custom_headers", "") or "").strip()
                if headers_str:
                    try:
                        custom_headers = json.loads(headers_str)
                        if isinstance(custom_headers, dict):
                            for k, v in custom_headers.items():
                                req.add_header(str(k), str(v))
                    except Exception:
                        pass

                with urllib.request.urlopen(req, timeout=10) as resp:
                    resp.read()
                return True, ""
            except urllib.error.HTTPError as he:
                try:
                    raw = he.read()
                    return False, f"HTTP {he.code}: {raw.decode('utf-8', errors='replace')[:200]}"
                except Exception:
                    return False, f"HTTPError: {he}"
            except Exception as e:
                return False, str(e)

    def _telegram_send_photo_sync(self, image_path: "Path", caption: str = "") -> tuple[bool, str]:
            """Send a Telegram photo synchronously and return (ok, error_message)."""
            try:
                if not bool(getattr(self.cfg.ui, "telegram_enabled", False)):
                    return False, "Telegram ist deaktiviert"
                token = str(getattr(self.cfg.ui, "telegram_bot_token", "") or "").strip()
                chat_id = str(getattr(self.cfg.ui, "telegram_chat_id", "") or "").strip()
                if not token or not chat_id:
                    return False, "Bot-Token oder Chat-ID fehlt"
            except Exception as e:
                return False, str(e)

            try:
                from pathlib import Path
                p = Path(image_path)
                if not p.exists() or not p.is_file():
                    return False, f"Bild nicht gefunden: {p}"
            except Exception as e:
                return False, str(e)

            import uuid
            boundary = "----shelly_analyzer_" + uuid.uuid4().hex
            try:
                img_bytes = p.read_bytes()
            except Exception as e:
                return False, str(e)

            def _part(name: str, value: str) -> bytes:
                return (
                    f"--{boundary}\r\n"
                    f'Content-Disposition: form-data; name="{name}"\r\n\r\n'
                    f"{value}\r\n"
                ).encode("utf-8")

            filename = p.name or "plot.png"
            head = (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="photo"; filename="{filename}"\r\n'
                f"Content-Type: image/png\r\n\r\n"
            ).encode("utf-8")
            tail = f"\r\n--{boundary}--\r\n".encode("utf-8")

            body = b"".join([
                _part("chat_id", chat_id),
                _part("caption", (caption or "").strip()) if (caption or "").strip() else b"",
                head,
                img_bytes,
                tail,
            ])

            url = f"https://api.telegram.org/bot{token}/sendPhoto"
            req = urllib.request.Request(url, data=body, method="POST")
            req.add_header("Content-Type", f"multipart/form-data; boundary={boundary}")
            req.add_header("Content-Length", str(len(body)))

            try:
                verify_ssl = bool(getattr(self.cfg.ui, "telegram_verify_ssl", True))
                ctx = None
                if not verify_ssl:
                    try:
                        ctx = ssl._create_unverified_context()
                    except Exception:
                        ctx = None
                with urllib.request.urlopen(req, timeout=30, context=ctx) as resp:
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
                return False, raw.decode("utf-8", errors="replace")[:200]

    def _telegram_plot_series_png(
        self,
        *,
        x: "pd.DatetimeIndex",
        y: "pd.Series",
        title: str,
        ylabel: str,
        out_path: "Path",
        style: str = "line",
    ) -> "Path":
        """Create a simple plot PNG for Telegram (headless-safe).

        style:
          - "line": time series line
          - "bar":  bars (good for kWh buckets)
        """
        from pathlib import Path
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            import matplotlib
            matplotlib.use("Agg", force=True)
            import matplotlib.pyplot as plt
            import matplotlib.dates as mdates
        except Exception:
            # If matplotlib isn't available, create an empty placeholder
            out_path.write_bytes(b"")
            return out_path

        fig = plt.figure(figsize=(10, 3.5))
        ax = fig.add_subplot(1, 1, 1)

        try:
            style_l = (style or "line").strip().lower()
        except Exception:
            style_l = "line"

        try:
            # Convert datetimes to matplotlib numbers for robust bar widths
            # Matplotlib can be picky about tz-aware timestamps; make them tz-naive (local time) first.
            try:
                import pandas as pd  # type: ignore
            except Exception:
                pd = None  # type: ignore

            try:
                if pd is not None:
                    x_idx = pd.DatetimeIndex(x)
                    if getattr(x_idx, "tz", None) is not None:
                        try:
                            x_idx = x_idx.tz_convert("Europe/Berlin")
                        except Exception:
                            pass
                        try:
                            x_idx = x_idx.tz_localize(None)
                        except Exception:
                            pass
                    x_list = list(x_idx.to_pydatetime())
                else:
                    x_list = list(getattr(x, "to_pydatetime", lambda: x)())
            except Exception:
                x_list = list(x)

            # Ensure y is numeric
            try:
                if pd is not None:
                    y_vals = list(pd.to_numeric(getattr(y, "values", y), errors="coerce").fillna(0.0))
                else:
                    y_vals = list(getattr(y, "values", y))
            except Exception:
                y_vals = list(getattr(y, "values", y))

            x_num = mdates.date2num(x_list)

            if style_l == "bar":
                if len(x_num) >= 2:
                    w = (x_num[1] - x_num[0]) * 0.9
                else:
                    w = 0.03  # ~45 minutes in days
                ax.bar(x_num, y_vals, width=w, align="center")
                ax.xaxis_date()
                try:
                    ax.set_ylim(bottom=0.0)
                except Exception:
                    pass
            else:
                ax.plot(x_num, y_vals)
                ax.xaxis_date()
        except Exception:
            # last resort
            try:
                ax.plot(list(x), list(getattr(y, "values", y)))
            except Exception:
                pass

        ax.set_title(title)
        ax.set_ylabel(ylabel)
        ax.grid(True, alpha=0.3)
        try:
            fig.autofmt_xdate()
        except Exception:
            pass
        fig.savefig(out_path, dpi=180, bbox_inches="tight")
        plt.close(fig)
        return out_path


    def _telegram_make_alarm_plots(self, device_key: str, end_ts: int, minutes: int = 10) -> list["Path"]:
            """Create last-N-minutes plots for V/A/W for a device (used for Telegram alarms).

            Prefer the in-memory live ring-buffer (last samples) so plots are attached reliably
            even when CSV writing lags behind. Falls back to CSV if live buffer is unavailable.
            """
            from pathlib import Path
            import pandas as pd
            from zoneinfo import ZoneInfo

            out: list[Path] = []
            try:
                end_ts_i = int(end_ts)
            except Exception:
                end_ts_i = 0
            try:
                minutes_i = int(minutes)
            except Exception:
                minutes_i = 10
            if minutes_i <= 0:
                minutes_i = 10
            if end_ts_i <= 0:
                try:
                    import time as _time
                    end_ts_i = int(_time.time())
                except Exception:
                    end_ts_i = 0
            start_ts_i = end_ts_i - (minutes_i * 60)

            tz_local = ZoneInfo("Europe/Berlin")
            dfw = None

            # 1) Prefer live ring buffer (populated by LivePoller) to avoid CSV lag.
            try:
                live = getattr(self, "_live_series", {}) or {}
                ser = live.get(str(device_key))
                if isinstance(ser, dict) and ser:
                    mappings = [
                        ("total_power", "total_power"),
                        ("a_voltage", "a_voltage"),
                        ("b_voltage", "b_voltage"),
                        ("c_voltage", "c_voltage"),
                        ("a_current", "a_current"),
                        ("b_current", "b_current"),
                        ("c_current", "c_current"),
                    ]
                    frames = []
                    for col, key in mappings:
                        dq = ser.get(key)
                        if not dq:
                            continue
                        try:
                            pts = [(int(t), float(v)) for (t, v) in list(dq) if int(t) >= start_ts_i and int(t) <= end_ts_i]
                        except Exception:
                            pts = []
                        if not pts:
                            continue
                        dfc = pd.DataFrame(pts, columns=["ts", col]).drop_duplicates("ts", keep="last")
                        frames.append(dfc)
                    if frames:
                        d = frames[0]
                        for other in frames[1:]:
                            d = d.merge(other, on="ts", how="outer")
                        d = d.sort_values("ts")
                        # timestamp for _wva_series
                        d["timestamp"] = pd.to_datetime(d["ts"], unit="s", utc=True, errors="coerce").dt.tz_convert(tz_local).dt.tz_localize(None)
                        dfw = d.drop(columns=["ts"]).dropna(subset=["timestamp"])
            except Exception:
                dfw = None

            # 2) Fallback to CSV if needed.
            if dfw is None or getattr(dfw, "empty", True):
                try:
                    df = self.storage.read_device_df(device_key)
                except Exception:
                    return out
                if df is None or getattr(df, "empty", True):
                    return out

                # Ensure timestamp
                try:
                    if "timestamp" in df.columns:
                        df = df.copy()
                        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
                    elif "ts" in df.columns:
                        df = df.copy()
                        df["timestamp"] = pd.to_datetime(pd.to_numeric(df["ts"], errors="coerce"), unit="s", errors="coerce")
                    else:
                        df = df.copy()
                        df["timestamp"] = pd.to_datetime(df.index, errors="coerce")
                except Exception:
                    return out
                df = df.dropna(subset=["timestamp"])
                if df.empty:
                    return out

                end_dt = pd.to_datetime(int(end_ts_i), unit="s", errors="coerce")
                if pd.isna(end_dt):
                    try:
                        end_dt = df["timestamp"].max()
                    except Exception:
                        return out
                start_dt = end_dt - pd.Timedelta(minutes=int(minutes_i))
                try:
                    msk = (df["timestamp"] >= start_dt) & (df["timestamp"] <= end_dt)
                    dfw = df.loc[msk].copy()
                except Exception:
                    dfw = df.copy()
                if dfw is None or dfw.empty:
                    return out

            # output folder inside data/
            try:
                base = Path(getattr(self.storage, "base_dir", Path.cwd() / "data"))
            except Exception:
                base = Path.cwd() / "data"
            out_dir = base / "_telegram"
            out_dir.mkdir(parents=True, exist_ok=True)

            # Generate series for W/V/A (total)
            for metric, ylabel in [("W", "W"), ("V", "V"), ("A", "A")]:
                try:
                    y, _ = self._wva_series(dfw, metric)
                    if y is None or len(y) == 0:
                        continue
                    y = pd.to_numeric(y, errors="coerce").dropna()
                    if y.empty:
                        continue
                    x = y.index
                    fn = f"alarm_{device_key}_{metric}_{int(end_ts_i)}.png"
                    p = out_dir / fn
                    title = f"{device_key} – {metric} (letzte {int(minutes_i)} min)"
                    self._telegram_plot_series_png(x=x, y=y, title=title, ylabel=ylabel, out_path=p)
                    if p.exists() and p.stat().st_size > 0:
                        out.append(p)
                except Exception:
                    continue
            return out

    def _telegram_kwh_series(self, start: "datetime", end: "datetime", freq: str = "H", device_key: str | None = None) -> "pd.Series":
        """Compute kWh grouped by hour ('H') or day ('D').
    
        - Uses Shelly CSVs and derives per-row interval energy (kWh) via calculate_energy().
        - Robust against timestamp formats (epoch s/ms/ns or ISO) and mixed tz handling.
        - If slicing with timezone-aware datetimes yields no rows, it falls back to naive slicing.
        """
        import pandas as pd
        from zoneinfo import ZoneInfo
        from shelly_analyzer.core.energy import calculate_energy
    
        tz_local = ZoneInfo("Europe/Berlin")
        tz_utc = ZoneInfo("UTC")
    
        def _to_dt(series: "pd.Series") -> "pd.Series":
            try:
                if pd.api.types.is_datetime64_any_dtype(series):
                    return series
            except Exception:
                pass
            nums = None
            try:
                nums = pd.to_numeric(series, errors="coerce")
            except Exception:
                nums = None
            if nums is not None and nums.notna().any():
                try:
                    med = float(nums.dropna().median())
                except Exception:
                    med = 0.0
                if med > 1e15:
                    out = pd.to_datetime(nums, errors="coerce", unit="ns")
                elif med > 1e12:
                    out = pd.to_datetime(nums, errors="coerce", unit="ms")
                else:
                    out = pd.to_datetime(nums, errors="coerce", unit="s")
                try:
                    if out.isna().mean() > 0.5:
                        out = pd.to_datetime(series, errors="coerce")
                except Exception:
                    pass
                return out
            return pd.to_datetime(series, errors="coerce")
    
        def _ensure_local(ts: "pd.Series", start_dt: "pd.Timestamp", end_dt: "pd.Timestamp") -> "pd.Series":
            """Ensure tz-aware Europe/Berlin.

            Problem: some CSV timestamps are naive *local time* while others are naive *UTC/epoch*.
            We pick the interpretation that yields more rows inside [start_dt, end_dt).
            """
            try:
                if getattr(ts.dt, "tz", None) is not None:
                    return ts.dt.tz_convert(tz_local)
            except Exception:
                pass

            ts_dt = pd.to_datetime(ts, errors="coerce")

            ts_as_utc = None
            ts_as_local = None
            try:
                ts_as_utc = ts_dt.dt.tz_localize(tz_utc, ambiguous="NaT", nonexistent="shift_forward").dt.tz_convert(tz_local)
            except Exception:
                ts_as_utc = None
            try:
                ts_as_local = ts_dt.dt.tz_localize(tz_local, ambiguous="NaT", nonexistent="shift_forward")
            except Exception:
                ts_as_local = None

            def _count_in(s):
                if s is None:
                    return -1
                try:
                    m = (s >= start_dt) & (s < end_dt)
                    return int(m.sum())
                except Exception:
                    return -1

            c_utc = _count_in(ts_as_utc)
            c_loc = _count_in(ts_as_local)

            # prefer the interpretation that yields more rows in the desired window
            if c_loc > c_utc:
                return ts_as_local
            if ts_as_utc is not None:
                return ts_as_utc
            if ts_as_local is not None:
                return ts_as_local
            return ts_dt
    
        start_dt = pd.Timestamp(start)
        end_dt = pd.Timestamp(end)
        try:
            if start_dt.tzinfo is None:
                start_dt = start_dt.tz_localize(tz_local)
            else:
                start_dt = start_dt.tz_convert(tz_local)
            if end_dt.tzinfo is None:
                end_dt = end_dt.tz_localize(tz_local)
            else:
                end_dt = end_dt.tz_convert(tz_local)
        except Exception:
            pass
    
        if device_key:
            class _D:
                pass
            _d = _D()
            _d.key = str(device_key)
            dev_iter = [_d]
        else:
            dev_iter = list(getattr(self.cfg, "devices", []) or [])
    
        agg: "pd.Series | None" = None
    
        for d in dev_iter:
            try:
                df = self.storage.read_device_df(str(d.key))
            except Exception:
                continue
            if df is None or getattr(df, "empty", True):
                continue
    
            try:
                if "timestamp" in df.columns:
                    ts = _to_dt(df["timestamp"])
                else:
                    ts = _to_dt(pd.Series(df.index))
                tmpdf = df.copy()
                tmpdf["timestamp"] = ts
                tmpdf = tmpdf.dropna(subset=["timestamp"])
            except Exception:
                continue
            if tmpdf.empty:
                continue
    
            try:
                tmpdf["timestamp"] = _ensure_local(pd.to_datetime(tmpdf["timestamp"], errors="coerce"), start_dt, end_dt)
            except Exception:
                pass
    
            df_use = None
            try:
                m = (tmpdf["timestamp"] >= start_dt) & (tmpdf["timestamp"] < end_dt)
                df_use = tmpdf.loc[m].copy()
            except Exception:
                df_use = tmpdf.copy()
    
            if df_use is None or df_use.empty:
                try:
                    tsn = pd.to_datetime(tmpdf["timestamp"], errors="coerce")
                    if getattr(tsn.dt, "tz", None) is not None:
                        tsn = tsn.dt.tz_convert(tz_local).dt.tz_localize(None)
                    s0 = pd.Timestamp(start_dt)
                    e0 = pd.Timestamp(end_dt)
                    try:
                        if getattr(s0, "tzinfo", None) is not None:
                            s0 = s0.tz_convert(tz_local).tz_localize(None)
                        if getattr(e0, "tzinfo", None) is not None:
                            e0 = e0.tz_convert(tz_local).tz_localize(None)
                    except Exception:
                        pass
                    m0 = (tsn >= s0) & (tsn < e0)
                    df_use = tmpdf.loc[m0].copy()
                    df_use["timestamp"] = tsn.loc[m0].values
                except Exception:
                    df_use = tmpdf.copy()
    
            if df_use is None or df_use.empty:
                continue

            # Extra-robust daily slicing: when we build hourly buckets for exactly one
            # calendar day, filter by the *local* calendar date. This avoids tz/naive
            # edge cases where comparisons against [start_dt, end_dt) can yield an
            # empty window even though data exists.
            try:
                if str(freq).upper().startswith("H"):
                    tgt = pd.Timestamp(start_dt)
                    try:
                        tgt_date = (tgt.tz_convert(tz_local).date() if getattr(tgt, "tzinfo", None) is not None else tgt.date())
                    except Exception:
                        tgt_date = tgt.date()
                    tloc = pd.to_datetime(df_use["timestamp"], errors="coerce")
                    try:
                        if getattr(tloc.dt, "tz", None) is None:
                            tloc = tloc.dt.tz_localize(tz_utc).dt.tz_convert(tz_local)
                        else:
                            tloc = tloc.dt.tz_convert(tz_local)
                    except Exception:
                        pass
                    mday = tloc.dt.date == tgt_date
                    df_use = df_use.loc[mday].copy()
                    df_use["timestamp"] = tloc.loc[mday].values
            except Exception:
                pass
    
            # --- Derive interval energy robustly ---
            # EMData CSVs often contain per-interval *Wh* values per phase (a/b/c_total_act_energy).
            # The generic calculate_energy(auto) may interpret these as cumulative counters and produce
            # near-zero/empty results for hourly buckets. If these columns are present, we compute
            # energy_kwh directly from them.
            df_e = None
            try:
                phase_cols = [c for c in ("a_total_act_energy", "b_total_act_energy", "c_total_act_energy") if c in df_use.columns]
                if phase_cols:
                    tmp = df_use.copy()
                    wh = tmp[phase_cols].sum(axis=1)
                    tmp["energy_kwh"] = pd.to_numeric(wh, errors="coerce").fillna(0.0) / 1000.0
                    df_e = tmp
            except Exception:
                df_e = None

            if df_e is None:
                try:
                    df_e = calculate_energy(df_use, method="auto")
                except Exception:
                    df_e = df_use

            if "energy_kwh" not in getattr(df_e, "columns", []):
                continue

            # Minimal diagnostics (helps debugging empty daily plots)
            try:
                if str(freq).upper().startswith("H") and (not device_key):
                    _rows = int(len(df_use))
                    _sum = float(pd.to_numeric(df_e["energy_kwh"], errors="coerce").fillna(0.0).sum())
                    print(f"[telegram] daily window rows={_rows} total_kwh={_sum:.4f}")
            except Exception:
                pass

            # Bucket using the filtered dataframe (timestamp + energy_kwh stay coupled).
            # Use resample() instead of manual floor/groupby to avoid dtype/tz corner cases
            # that could silently zero-out buckets (observed: total_kwh>0 but all buckets 0).
            try:
                tmpb = df_e[["timestamp", "energy_kwh"]].copy()
                tmpb["timestamp"] = _to_dt(tmpb["timestamp"])
                tmpb = tmpb.dropna(subset=["timestamp"])
                # Map to Europe/Berlin (choose best interpretation for naive timestamps).
                tmpb["timestamp"] = _ensure_local(pd.to_datetime(tmpb["timestamp"], errors="coerce"), start_dt, end_dt)

                # Slice strictly inside the requested window (tz-aware).
                try:
                    m2 = (tmpb["timestamp"] >= start_dt) & (tmpb["timestamp"] < end_dt)
                    tmpb = tmpb.loc[m2].copy()
                except Exception:
                    pass
                if tmpb.empty:
                    continue

                # Convert to tz-naive local for deterministic resampling/bucketing.
                try:
                    tmpb["timestamp"] = tmpb["timestamp"].dt.tz_convert(tz_local).dt.tz_localize(None)
                except Exception:
                    tmpb["timestamp"] = pd.to_datetime(tmpb["timestamp"], errors="coerce")

                tmpb["energy_kwh"] = pd.to_numeric(tmpb["energy_kwh"], errors="coerce").fillna(0.0)
                tmpb = tmpb.dropna(subset=["timestamp"]).sort_values("timestamp")
                if tmpb.empty:
                    continue

                ser = tmpb.set_index("timestamp")["energy_kwh"]

                if str(freq).upper().startswith("H"):
                    s = ser.resample("h").sum()
                else:
                    s = ser.resample("D").sum()

                s = s.sort_index()

                # Extra diagnostics: if raw window energy is >0 but buckets sum to 0, log bounds.
                try:
                    _raw_sum = float(pd.to_numeric(ser, errors="coerce").fillna(0.0).sum())
                    _buck_sum = float(pd.to_numeric(s, errors="coerce").fillna(0.0).sum())
                    if _raw_sum > 0.0 and _buck_sum == 0.0:
                        try:
                            _tmin = tmpb["timestamp"].min()
                            _tmax = tmpb["timestamp"].max()
                        except Exception:
                            _tmin = None
                            _tmax = None
                        print(f"[telegram] WARN daily bucketing produced 0 although raw_sum={_raw_sum:.4f}; ts_range={_tmin}..{_tmax}; window={start_dt}..{end_dt}")
                except Exception:
                    pass

                if agg is None:
                    agg = s
                else:
                    agg = agg.add(s, fill_value=0.0)
            except Exception:
                continue

    
        if agg is None:
            return pd.Series(dtype=float)
    
        agg = agg.sort_index()
    
        try:
            st = pd.Timestamp(start_dt)
            en = pd.Timestamp(end_dt)
            # Normalize boundaries to tz-naive local time so the generated bucket index
            # matches the tz-naive local buckets produced above.
            try:
                if getattr(st, "tzinfo", None) is not None:
                    st = st.tz_convert(tz_local).tz_localize(None)
                if getattr(en, "tzinfo", None) is not None:
                    en = en.tz_convert(tz_local).tz_localize(None)
            except Exception:
                try:
                    if getattr(st, "tzinfo", None) is not None:
                        st = st.tz_localize(None)
                    if getattr(en, "tzinfo", None) is not None:
                        en = en.tz_localize(None)
                except Exception:
                    pass

            if str(freq).upper().startswith("H"):
                idx = pd.date_range(st.floor("H"), en.floor("H") - pd.Timedelta(hours=1), freq="H")
            else:
                idx = pd.date_range(st.floor("D"), en.floor("D") - pd.Timedelta(days=1), freq="D")
            if len(idx) > 0:
                # Ensure series index is tz-naive for matching.
                try:
                    agg.index = pd.DatetimeIndex(agg.index).tz_localize(None)
                except Exception:
                    pass
                agg = agg.reindex(idx, fill_value=0.0)
        except Exception:
            pass
    
        return agg
    
    
    def _telegram_make_summary_plots(self, kind: str, start_dt: "datetime", end_dt: "datetime") -> list["Path"]:
        """Create kWh summary plots as PNGs for Telegram summaries.

        - daily: previous calendar day (hourly buckets) for [start_dt, end_dt)
        - monthly: last 30 days (daily buckets) ending at end_dt
        Additionally, per-device bar plots are generated for each configured device.
        """
        from pathlib import Path
        from datetime import timedelta
        import pandas as pd

        kind = (kind or "").strip().lower()
        out: list[Path] = []

        try:
            base = Path(getattr(self.storage, "base_dir", Path.cwd() / "data"))
        except Exception:
            base = Path.cwd() / "data"

        out_dir = base / "_telegram"
        out_dir.mkdir(parents=True, exist_ok=True)

        # Ensure we never end up with "no images" just because plotting failed.
        # We validate generated PNGs and, if needed, write a tiny valid placeholder PNG.
        def _ensure_valid_png(p: Path) -> Path:
            try:
                if p.exists() and p.is_file() and p.stat().st_size > 200:
                    return p
            except Exception:
                pass
            # minimal 1x1 transparent PNG (base64)
            try:
                import base64
                data = base64.b64decode(
                    b"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/1sQAAAAASUVORK5CYII="
                )
                p.write_bytes(data)
            except Exception:
                try:
                    p.write_bytes(b"")
                except Exception:
                    pass
            return p

        # Price (€/kWh) from PricingConfig (gross)
        unit_gross = None
        try:
            unit_gross = float(self.cfg.pricing.unit_price_gross())
        except Exception:
            try:
                unit_gross = float(getattr(getattr(self.cfg, "pricing", None), "electricity_price_eur_per_kwh", None))
            except Exception:
                unit_gross = None

        def _mk_title(prefix: str, total_kwh: float) -> str:
            t = f"{prefix} · {total_kwh:.2f} kWh"
            if unit_gross is not None:
                t += f" · {(total_kwh * unit_gross):.2f} € ({unit_gross:.4f} €/kWh)"
            return t

        def _force_index(s: "pd.Series", start_ts: "pd.Timestamp", end_ts: "pd.Timestamp", freq: str) -> "pd.Series":
            """Reindex to a full expected bucket index.

            This must NEVER return an empty series just because timezone handling is tricky.
            If we cannot build a tz-aware index, we fall back to tz-naive.
            """
            try:
                s = s if s is not None else pd.Series(dtype=float)
            except Exception:
                s = pd.Series(dtype=float)

            def _mk_idx(st: "pd.Timestamp", en: "pd.Timestamp") -> "pd.DatetimeIndex":
                if freq.upper().startswith("H"):
                    return pd.date_range(st.floor("H"), en.floor("H") - pd.Timedelta(hours=1), freq="H")
                return pd.date_range(st.floor("D"), en.floor("D") - pd.Timedelta(days=1), freq="D")

            try:
                idx = _mk_idx(start_ts, end_ts)
                if len(idx) > 0:
                    return s.reindex(idx, fill_value=0.0)
            except Exception:
                pass

            # tz-fallback: remove tz and try again
            try:
                st0 = start_ts.tz_localize(None) if getattr(start_ts, "tzinfo", None) is not None else start_ts
                en0 = end_ts.tz_localize(None) if getattr(end_ts, "tzinfo", None) is not None else end_ts
                idx = _mk_idx(st0, en0)
                if len(idx) > 0:
                    # also strip tz from series index for matching
                    try:
                        if getattr(getattr(s.index, "tz", None), "key", None) is not None:
                            s = pd.Series(s.values, index=pd.DatetimeIndex(s.index).tz_localize(None))
                    except Exception:
                        pass
                    return s.reindex(idx, fill_value=0.0)
            except Exception:
                pass

            return s

        try:
            if kind == "daily":
                # Daily = previous calendar day, hourly buckets.
                s = self._telegram_kwh_series(start_dt, end_dt, freq="H")
                try:
                    s = pd.to_numeric(s, errors="coerce").fillna(0.0)
                except Exception:
                    s = pd.Series(dtype=float)

                # Ensure tz-naive local index for plotting/reindexing.
                try:
                    _idx = pd.DatetimeIndex(s.index)
                    if _idx.tz is not None:
                        _idx = _idx.tz_convert("Europe/Berlin").tz_localize(None)
                    s.index = _idx
                except Exception:
                    pass
                try:
                    s = s.sort_index()
                except Exception:
                    pass

                # Diagnostics
                try:
                    total_kwh = float(pd.to_numeric(s, errors="coerce").fillna(0.0).sum())
                    _nz = int((pd.to_numeric(s, errors="coerce").fillna(0.0) > 0).sum())
                    print(f"[telegram] daily plot buckets nonzero={_nz} sum={total_kwh:.4f}")
                except Exception:
                    total_kwh = 0.0

                end_ts = int(pd.Timestamp(end_dt).timestamp())

                p = out_dir / f"summary_daily_total_kwh_{end_ts}.png"
                try:
                    self._telegram_plot_series_png(
                        x=s.index,
                        y=s,
                        title=_mk_title("kWh – Vortag (pro Stunde, gesamt)", total_kwh),
                        ylabel="kWh",
                        out_path=p,
                        style="bar",
                    )
                except Exception:
                    pass
                out.append(_ensure_valid_png(p))

                for d in list(getattr(self.cfg, "devices", []) or []):
                    try:
                        sd = self._telegram_kwh_series(start_dt, end_dt, freq="H", device_key=str(getattr(d, "key", "")))
                        sd = pd.to_numeric(sd, errors="coerce").fillna(0.0)
                        try:
                            sd = sd.sort_index()
                        except Exception:
                            pass
                        if sd is None or len(sd) == 0:
                            continue
                        dkwh = float(sd.sum())
                        if dkwh <= 0.0:
                            continue
                        pdv = out_dir / f"summary_daily_{str(d.key)}_kwh_{end_ts}.png"
                        try:
                            self._telegram_plot_series_png(
                                x=sd.index,
                                y=sd,
                                title=_mk_title(f"kWh – Vortag (pro Stunde, {d.name})", dkwh),
                                ylabel="kWh",
                                out_path=pdv,
                                style="bar",
                            )
                        except Exception:
                            pass
                        out.append(_ensure_valid_png(pdv))
                    except Exception:
                        continue

                return out

            if kind == "monthly":
                s_start = pd.Timestamp(end_dt) - timedelta(days=30)
                s = self._telegram_kwh_series(s_start, end_dt, freq="D")
                s = pd.to_numeric(s, errors="coerce").fillna(0.0)
                if s is None or len(s) == 0:
                    return []

                total_kwh = float(s.sum())
                end_ts = int(pd.Timestamp(end_dt).timestamp())

                p = out_dir / f"summary_30d_total_kwh_{end_ts}.png"
                self._telegram_plot_series_png(
                    x=s.index,
                    y=s,
                    title=_mk_title("kWh – letzte 30 Tage (gesamt)", total_kwh),
                    ylabel="kWh",
                    out_path=p,
                    style="bar",
                )
                out.append(_ensure_valid_png(p))

                for d in list(getattr(self.cfg, "devices", []) or []):
                    try:
                        sd = self._telegram_kwh_series(s_start, end_dt, freq="D", device_key=str(getattr(d, "key", "")))
                        sd = pd.to_numeric(sd, errors="coerce").fillna(0.0)
                        if sd is None or len(sd) == 0:
                            continue
                        dkwh = float(sd.sum())
                        if dkwh <= 0.0:
                            continue
                        pdv = out_dir / f"summary_30d_{str(d.key)}_kwh_{end_ts}.png"
                        self._telegram_plot_series_png(
                            x=sd.index,
                            y=sd,
                            title=_mk_title(f"kWh – letzte 30 Tage ({d.name})", dkwh),
                            ylabel="kWh",
                            out_path=pdv,
                            style="bar",
                        )
                        out.append(_ensure_valid_png(pdv))
                    except Exception:
                        continue

                return out
        except Exception:
            return []

        return out

    def _telegram_send_with_images(self, text: str, image_paths: list["Path"], caption_prefix: str = "") -> tuple[bool, str]:
        """Send a message and then multiple images.

        Returns:
            (ok, err)
            - ok=True when the text was sent AND (if images were provided) at least one image was sent.
            - err contains the last error message if something failed (empty on full success).
        """
        ok_text, err = self._telegram_send_sync(text)
        if not ok_text:
            return False, err

        paths = list(image_paths or [])
        if not paths:
            return True, ""

        any_ok = False
        last_err = ""
        for i, p in enumerate(paths):
            try:
                cap = ""
                if caption_prefix and i == 0:
                    cap = caption_prefix
                ok2, err2 = self._telegram_send_photo_sync(p, caption=cap)
                if ok2:
                    any_ok = True
                else:
                    last_err = err2 or last_err
            except Exception as e:
                last_err = str(e)

        if any_ok:
            return True, last_err or ""
        return False, last_err or "Keine Bilder gesendet"
    def _alerts_send_telegram(self, text: str, image_paths: list["Path"] | None = None) -> None:
        """Send a Telegram message in the background (used by alert rules)."""

        def _worker():
            try:
                if image_paths:
                    ok, err = self._telegram_send_with_images(text, list(image_paths))
                else:
                    ok, err = self._telegram_send_sync(text)
            except Exception as e:
                ok, err = False, str(e)

            if not ok and err:
                try:
                    logging.getLogger(__name__).warning("Telegram send failed: %s", err)
                except Exception:
                    pass

        try:
            threading.Thread(target=_worker, daemon=True).start()
        except Exception:
            pass

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

            unit = {"W": "W", "V": "V", "A": "A", "VAR": "var", "COSPHI": "", "HZ": "Hz"}.get(base, "")
            if m == "A_N":
                unit = "A"
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

            if base in {"HZ", "FREQ", "FREQUENCY"}:
                return float(getattr(s, "freq_hz", {}).get("total", 0.0))

            if base in {"V", "VOLT", "VOLTAGE"}:
                if phase:
                    return float(getattr(s, "voltage_v", {}).get(phase, 0.0))
                return _mean_abc(getattr(s, "voltage_v", {}) or {})

            if base in {"A", "AMP", "AMPS", "CURRENT"}:
                # Neutral current: A_N metric (or phase=='n' after suffix stripping)
                if m0 == "A_N" or phase == "n":
                    try:
                        _ia = float(getattr(s, "current_a", {}).get("a", 0.0))
                        _ib = float(getattr(s, "current_a", {}).get("b", 0.0))
                        _ic = float(getattr(s, "current_a", {}).get("c", 0.0))
                        if _ia > 0 or _ib > 0 or _ic > 0:
                            _pa = float(getattr(s, "power_w", {}).get("a", 0.0))
                            _pb = float(getattr(s, "power_w", {}).get("b", 0.0))
                            _pc = float(getattr(s, "power_w", {}).get("c", 0.0))
                            _qa = float(getattr(s, "reactive_var", {}).get("a", 0.0))
                            _qb = float(getattr(s, "reactive_var", {}).get("b", 0.0))
                            _qc = float(getattr(s, "reactive_var", {}).get("c", 0.0))
                            _phi_a = math.atan2(_qa, _pa) if (_pa or _qa) else 0.0
                            _phi_b = math.atan2(_qb, _pb) if (_pb or _qb) else 0.0
                            _phi_c = math.atan2(_qc, _pc) if (_pc or _qc) else 0.0
                            _2pi3 = 2.0 * math.pi / 3.0
                            _ta = -_phi_a
                            _tb = -_2pi3 - _phi_b
                            _tc = _2pi3 - _phi_c
                            _in_re = _ia * math.cos(_ta) + _ib * math.cos(_tb) + _ic * math.cos(_tc)
                            _in_im = _ia * math.sin(_ta) + _ib * math.sin(_tb) + _ic * math.sin(_tc)
                            return math.sqrt(_in_re * _in_re + _in_im * _in_im)
                    except Exception:
                        pass
                    return 0.0
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

                            plots: list[Path] = []
                            try:
                                if bool(getattr(self.cfg.ui, "telegram_alarm_plots_enabled", True)):
                                    plots = self._telegram_make_alarm_plots(
                                        str(getattr(s, "device_key", "") or devk),
                                        int(getattr(s, "ts", 0) or now_ts),
                                        minutes=10,
                                    )
                            except Exception:
                                plots = []

                            # Actually send the Telegram notification (previous versions built the message but never sent it)
                            try:
                                if plots:
                                    ok, err = self._telegram_send_with_images(tg_msg, plots)
                                else:
                                    ok, err = self._telegram_send_sync(tg_msg)
                                if not ok:
                                    print(f"[alerts][telegram] send failed: {err}")
                            except Exception as e:
                                print(f"[alerts][telegram] send error: {e}")
                        except Exception as e:
                            print(f"[alerts][telegram] build error: {e}")

                    if bool(getattr(r, "action_webhook", False)):
                        try:
                            if bool(getattr(self.cfg.ui, "webhook_alarm_enabled", True)):
                                wh_payload = {
                                    "type": "alarm",
                                    "timestamp": datetime.fromtimestamp(now_ts).isoformat(),
                                    "rule_id": rid,
                                    "device_key": str(getattr(s, "device_key", "") or devk),
                                    "device_name": devname,
                                    "metric": metric,
                                    "value": round(val, 4),
                                    "op": op,
                                    "threshold": round(thr, 4),
                                    "duration_seconds": dur,
                                    "message": msg,
                                    "source": "shelly-energy-analyzer",
                                }
                                def _wh_send(payload=wh_payload):
                                    ok, err = self._webhook_send_sync(payload)
                                    if not ok:
                                        print(f"[alerts][webhook] send failed: {err}")
                                threading.Thread(target=_wh_send, daemon=True).start()
                        except Exception as e:
                            print(f"[alerts][webhook] error: {e}")

                    if bool(getattr(r, "action_email", False)):
                        try:
                            if bool(getattr(self.cfg.ui, "email_alarm_enabled", True)):
                                em_subject = f"[Shelly Alarm] {metric} {op} {thr} – {devname}"
                                em_body = (
                                    f"Alarm ausgelöst: {msg}\n\n"
                                    f"Gerät: {devname} ({str(getattr(s, 'device_key', '') or devk)})\n"
                                    f"Metrik: {metric} {op} {thr}\n"
                                    f"Wert: {round(val, 4)}\n"
                                    f"Zeit: {datetime.fromtimestamp(now_ts).isoformat()}\n"
                                    f"Quelle: shelly-energy-analyzer"
                                )
                                def _em_send(subj=em_subject, body=em_body):
                                    ok, err = self._email_send_sync(subject=subj, body=body)
                                    if not ok:
                                        print(f"[alerts][email] send failed: {err}")
                                threading.Thread(target=_em_send, daemon=True).start()
                        except Exception as e:
                            print(f"[alerts][email] error: {e}")

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
                                # Daily summary: previous *calendar day* (00:00..24:00 local)
                                from zoneinfo import ZoneInfo
                                tz = ZoneInfo('Europe/Berlin')
                                now_tz = datetime.now(tz)
                                y = (now_tz.date() - timedelta(days=1))
                                start_dt = datetime.combine(y, datetime.min.time(), tzinfo=tz)
                                end_dt = datetime.combine(now_tz.date(), datetime.min.time(), tzinfo=tz)
                                try:
                                    # Sync data before building summary
                                    self._sync_before_telegram_summary()
                                    msg = self._build_telegram_summary("daily", start_dt, end_dt)
                                    imgs = self._telegram_make_summary_plots("daily", start_dt, end_dt)
                                    ok, err = self._telegram_send_with_images(msg, imgs)
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
                                            logging.getLogger(__name__).warning("Telegram daily summary failed: %s", err)
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
                                end_dt = now
                                start_dt = end_dt - timedelta(days=30)
                                try:
                                    # Sync data before building summary
                                    self._sync_before_telegram_summary()
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
                                            logging.getLogger(__name__).warning("Telegram monthly summary failed: %s", err)
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
                                # Daily summary should cover the *last 24 hours* (rolling window).
                                end_dt = now
                                start_dt = end_dt - timedelta(hours=24)
                                try:
                                    msg = self._build_telegram_summary("daily", start_dt, end_dt)
                                    imgs = self._telegram_make_summary_plots("daily", start_dt, end_dt)
                                    ok, err = self._telegram_send_with_images(msg, imgs)
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
                                            logging.getLogger(__name__).warning("Telegram daily summary failed: %s", err)
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
                                    # Sync data before building summary
                                    self._sync_before_telegram_summary()
                                    msg = self._build_telegram_summary("monthly", start_dt, end_dt)
                                    imgs = self._telegram_make_summary_plots("monthly", start_dt, end_dt)
                                    ok, err = self._telegram_send_with_images(msg, imgs)
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
                                            logging.getLogger(__name__).warning("Telegram monthly summary failed: %s", err)
                                        except Exception:
                                            pass
                    else:
                        # prevent retroactive mid-month sends when enabling
                        if last != key and now.date() != due_end.date():
                            _state_set("month_last", key)

    def _telegram_send_summary_daily(self, start_dt: datetime, end_dt: datetime, mark_sent: bool = True, sent_key: str = "") -> None:
            msg = self._build_telegram_summary("daily", start_dt, end_dt)
            imgs = self._telegram_make_summary_plots("daily", start_dt, end_dt)
            ok, err = self._telegram_send_with_images(msg, imgs)
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
                    logging.getLogger(__name__).warning("Telegram daily summary failed: %s", err)
                except Exception:
                    pass

    def _telegram_send_summary_month(self, start_dt: datetime, end_dt: datetime, mark_sent: bool = True, sent_key: str = "") -> None:
            msg = self._build_telegram_summary("monthly", start_dt, end_dt)
            imgs = self._telegram_make_summary_plots("monthly", start_dt, end_dt)
            ok, err = self._telegram_send_with_images(msg, imgs)
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
                    logging.getLogger(__name__).warning("Telegram monthly summary failed: %s", err)
                except Exception:
                    pass

    def _webhook_summary_tick(self) -> None:
            """Send scheduled daily/monthly webhook summaries (independent of Telegram)."""
            wh_daily = bool(getattr(self.cfg.ui, "webhook_daily_summary_enabled", False))
            wh_monthly = bool(getattr(self.cfg.ui, "webhook_monthly_summary_enabled", False))
            if not bool(getattr(self.cfg.ui, "webhook_enabled", False)):
                return
            if not wh_daily and not wh_monthly:
                return

            try:
                base_dir = getattr(getattr(self, "storage", None), "base_dir", None) or "."
                p_state = Path(base_dir) / "data" / "webhook_summary_state.json"
                p_state.parent.mkdir(parents=True, exist_ok=True)
                if not hasattr(self, "_wh_summary_state"):
                    try:
                        self._wh_summary_state = json.loads(p_state.read_text(encoding="utf-8")) if p_state.exists() else {}
                    except Exception:
                        self._wh_summary_state = {}
                state = self._wh_summary_state
            except Exception:
                return

            def _st_get(k: str, default: str = "") -> str:
                try:
                    return str(state.get(k, default) or default)
                except Exception:
                    return default

            def _st_set(k: str, v: str) -> None:
                try:
                    state[k] = v
                    p_state.write_text(json.dumps(state, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
                except Exception:
                    pass

            now = datetime.now()
            now_ts = int(time.time())

            # --- Daily ---
            if wh_daily:
                try:
                    hh, mm = self._parse_hhmm(str(getattr(self.cfg.ui, "telegram_daily_summary_time", "00:00") or "00:00"))
                    due = datetime.combine(now.date(), datetime.min.time()).replace(hour=hh, minute=mm)
                    key = f"{due.strftime('%Y-%m-%d')}_{hh:02d}{mm:02d}"
                    grace_s = 7200  # 2h
                    last = _st_get("daily_last", "")
                    attempt_ts_str = _st_get("daily_attempt_ts", "0")
                    try:
                        attempt_ts = int(float(attempt_ts_str or 0))
                    except Exception:
                        attempt_ts = 0
                    if now >= due and now <= (due + timedelta(seconds=grace_s)) and last != key:
                        if not (attempt_ts and (now_ts - attempt_ts) < 60):
                            _st_set("daily_attempt_ts", str(now_ts))
                            try:
                                from zoneinfo import ZoneInfo
                                tz = ZoneInfo("Europe/Berlin")
                                end_dt = datetime.now(tz)
                                start_dt = end_dt - timedelta(hours=24)
                                msg_text = self._build_telegram_summary("daily", start_dt, end_dt)
                                _co2_intensity = float(getattr(getattr(self.cfg, "pricing", None), "co2_intensity_g_per_kwh", 380.0) or 0.0)
                                payload = {
                                    "type": "daily_summary",
                                    "timestamp": datetime.now().isoformat(),
                                    "period_start": start_dt.isoformat(),
                                    "period_end": end_dt.isoformat(),
                                    "message": msg_text,
                                    "co2_intensity_g_per_kwh": _co2_intensity,
                                    "source": "shelly-energy-analyzer",
                                }
                                try:
                                    _sr = self._solar_calc_period(start_dt, end_dt)
                                    if _sr:
                                        payload["solar"] = {k: v for k, v in _sr.items() if v is not None}
                                except Exception:
                                    pass
                                ok, err = self._webhook_send_sync(payload)
                            except Exception as e:
                                ok, err = False, str(e)
                            if ok:
                                _st_set("daily_last", key)
                            elif err:
                                logging.getLogger(__name__).warning("Webhook daily summary failed: %s", err)
                except Exception as e:
                    logging.getLogger(__name__).warning("Webhook daily summary tick error: %s", e)

            # --- Monthly ---
            if wh_monthly:
                try:
                    hh, mm = self._parse_hhmm(str(getattr(self.cfg.ui, "telegram_monthly_summary_time", "00:00") or "00:00"))
                    now_m = now.replace(day=1, hour=hh, minute=mm, second=0, microsecond=0)
                    key = f"{now_m.strftime('%Y-%m')}_{hh:02d}{mm:02d}"
                    grace_s = 86400  # 24h
                    last = _st_get("monthly_last", "")
                    attempt_ts_str = _st_get("monthly_attempt_ts", "0")
                    try:
                        attempt_ts = int(float(attempt_ts_str or 0))
                    except Exception:
                        attempt_ts = 0
                    if now >= now_m and now <= (now_m + timedelta(seconds=grace_s)) and last != key:
                        if not (attempt_ts and (now_ts - attempt_ts) < 300):
                            _st_set("monthly_attempt_ts", str(now_ts))
                            try:
                                from zoneinfo import ZoneInfo
                                tz = ZoneInfo("Europe/Berlin")
                                end_dt = datetime.now(tz)
                                start_dt = end_dt - timedelta(days=30)
                                msg_text = self._build_telegram_summary("monthly", start_dt, end_dt)
                                _co2_intensity = float(getattr(getattr(self.cfg, "pricing", None), "co2_intensity_g_per_kwh", 380.0) or 0.0)
                                payload = {
                                    "type": "monthly_summary",
                                    "timestamp": datetime.now().isoformat(),
                                    "period_start": start_dt.isoformat(),
                                    "period_end": end_dt.isoformat(),
                                    "message": msg_text,
                                    "co2_intensity_g_per_kwh": _co2_intensity,
                                    "source": "shelly-energy-analyzer",
                                }
                                try:
                                    _sr = self._solar_calc_period(start_dt, end_dt)
                                    if _sr:
                                        payload["solar"] = {k: v for k, v in _sr.items() if v is not None}
                                except Exception:
                                    pass
                                ok, err = self._webhook_send_sync(payload)
                            except Exception as e:
                                ok, err = False, str(e)
                            if ok:
                                _st_set("monthly_last", key)
                            elif err:
                                logging.getLogger(__name__).warning("Webhook monthly summary failed: %s", err)
                except Exception as e:
                    logging.getLogger(__name__).warning("Webhook monthly summary tick error: %s", e)

    def _email_send_sync(self, subject: str, body: str, attachments: list | None = None) -> tuple[bool, str]:
            """Send an e-mail via SMTP and return (ok, error_message)."""
            try:
                if not bool(getattr(self.cfg.ui, "email_enabled", False)):
                    return False, "E-Mail ist deaktiviert"
                server = str(getattr(self.cfg.ui, "email_smtp_server", "") or "").strip()
                if not server:
                    return False, "SMTP-Server fehlt"
                port = int(getattr(self.cfg.ui, "email_smtp_port", 587))
                user = str(getattr(self.cfg.ui, "email_smtp_user", "") or "").strip()
                password = str(getattr(self.cfg.ui, "email_smtp_password", "") or "").strip()
                from_addr = str(getattr(self.cfg.ui, "email_from_address", "") or "").strip()
                if not from_addr:
                    from_addr = user  # fallback
                if not from_addr:
                    return False, "Absender-Adresse fehlt"
                recipients_str = str(getattr(self.cfg.ui, "email_recipients", "") or "").strip()
                if not recipients_str:
                    return False, "Empfänger fehlt"
                recipients = [a.strip() for a in recipients_str.split(",") if a.strip()]
                if not recipients:
                    return False, "Empfänger fehlt"
                use_tls = bool(getattr(self.cfg.ui, "email_use_tls", True))
            except Exception as e:
                return False, str(e)

            try:
                import smtplib
                import ssl
                from email.mime.multipart import MIMEMultipart
                from email.mime.text import MIMEText
                from email.mime.base import MIMEBase
                from email import encoders

                msg = MIMEMultipart()
                msg["From"] = from_addr
                msg["To"] = ", ".join(recipients)
                msg["Subject"] = subject
                msg.attach(MIMEText(body, "plain", "utf-8"))

                for att_path in (attachments or []):
                    try:
                        from pathlib import Path as _P
                        p = _P(att_path)
                        if p.exists() and p.is_file():
                            part = MIMEBase("application", "octet-stream")
                            part.set_payload(p.read_bytes())
                            encoders.encode_base64(part)
                            part.add_header("Content-Disposition", f'attachment; filename="{p.name}"')
                            msg.attach(part)
                    except Exception:
                        pass

                if use_tls:
                    ctx = ssl.create_default_context()
                    if port == 465:
                        # SSL/TLS
                        with smtplib.SMTP_SSL(server, port, context=ctx, timeout=15) as smtp:
                            if user and password:
                                smtp.login(user, password)
                            smtp.sendmail(from_addr, recipients, msg.as_string())
                    else:
                        # STARTTLS (port 587 etc.)
                        with smtplib.SMTP(server, port, timeout=15) as smtp:
                            smtp.ehlo()
                            smtp.starttls(context=ctx)
                            smtp.ehlo()
                            if user and password:
                                smtp.login(user, password)
                            smtp.sendmail(from_addr, recipients, msg.as_string())
                else:
                    with smtplib.SMTP(server, port, timeout=15) as smtp:
                        smtp.ehlo()
                        if user and password:
                            smtp.login(user, password)
                        smtp.sendmail(from_addr, recipients, msg.as_string())

                return True, ""
            except Exception as e:
                return False, str(e)

    def _email_summary_tick(self) -> None:
            """Send scheduled daily/monthly e-mail summaries with PDF attachments."""
            em_daily = bool(getattr(self.cfg.ui, "email_daily_summary_enabled", False))
            em_monthly = bool(getattr(self.cfg.ui, "email_monthly_summary_enabled", False))
            if not bool(getattr(self.cfg.ui, "email_enabled", False)):
                return
            if not em_daily and not em_monthly:
                return

            try:
                base_dir = getattr(getattr(self, "storage", None), "base_dir", None) or "."
                p_state = Path(base_dir) / "data" / "email_summary_state.json"
                p_state.parent.mkdir(parents=True, exist_ok=True)
                if not hasattr(self, "_em_summary_state"):
                    try:
                        self._em_summary_state = json.loads(p_state.read_text(encoding="utf-8")) if p_state.exists() else {}
                    except Exception:
                        self._em_summary_state = {}
                state = self._em_summary_state
            except Exception:
                return

            def _st_get(k: str, default: str = "") -> str:
                try:
                    return str(state.get(k, default) or default)
                except Exception:
                    return default

            def _st_set(k: str, v: str) -> None:
                try:
                    state[k] = v
                    p_state.write_text(json.dumps(state, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
                except Exception:
                    pass

            now = datetime.now()
            now_ts = int(time.time())
            lang = str(getattr(self.cfg.ui, "language", "de") or "de")

            # --- Daily ---
            if em_daily:
                try:
                    hh, mm = self._parse_hhmm(str(getattr(self.cfg.ui, "email_daily_summary_time", "00:00") or "00:00"))
                    due = datetime.combine(now.date(), datetime.min.time()).replace(hour=hh, minute=mm)
                    key = f"{due.strftime('%Y-%m-%d')}_{hh:02d}{mm:02d}"
                    grace_s = 7200  # 2h
                    last = _st_get("daily_last", "")
                    attempt_ts_str = _st_get("daily_attempt_ts", "0")
                    try:
                        attempt_ts = int(float(attempt_ts_str or 0))
                    except Exception:
                        attempt_ts = 0
                    if now >= due and now <= (due + timedelta(seconds=grace_s)) and last != key:
                        if not (attempt_ts and (now_ts - attempt_ts) < 60):
                            _st_set("daily_attempt_ts", str(now_ts))
                            try:
                                from zoneinfo import ZoneInfo
                                tz = ZoneInfo("Europe/Berlin")
                                end_dt = datetime.now(tz)
                                start_dt = end_dt - timedelta(hours=24)
                                prev_start = start_dt - timedelta(hours=24)
                                msg_text = self._build_telegram_summary("daily", start_dt, end_dt)

                                # Generate rich PDF attachment
                                _pdf_log = logging.getLogger(__name__)
                                pdf_path = None
                                try:
                                    from shelly_analyzer.services.export import export_pdf_email_daily, export_pdf_summary
                                    tmp_dir = Path(base_dir) / "data" / "email_tmp"
                                    tmp_dir.mkdir(parents=True, exist_ok=True)
                                    pdf_path = tmp_dir / f"daily_report_{due.strftime('%Y-%m-%d')}.pdf"
                                    report_data = self._build_email_report_data(
                                        start_dt, end_dt, prev_start, start_dt, report_type="daily"
                                    )
                                    _pdf_log.info("Email daily PDF: report_data totals=%s", len(report_data.totals) if report_data else "None")
                                    if report_data and report_data.totals:
                                        export_pdf_email_daily(report_data, pdf_path, lang=lang)
                                    else:
                                        totals = self._build_report_totals(start_dt, end_dt)
                                        _pdf_log.info("Email daily PDF fallback: totals=%s", len(totals) if totals else "None")
                                        if totals:
                                            export_pdf_summary(
                                                title="Shelly Energy Analyzer \u2013 Daily Report",
                                                period_label=f"{start_dt.strftime('%Y-%m-%d')} \u2013 {end_dt.strftime('%Y-%m-%d')}",
                                                totals=totals,
                                                out_path=pdf_path,
                                                lang=lang,
                                            )
                                        else:
                                            _pdf_log.warning("Email daily PDF: no data found, sending email without attachment")
                                            pdf_path = None
                                    if pdf_path and pdf_path.exists():
                                        _pdf_log.info("Email daily PDF generated: %s (%d bytes)", pdf_path, pdf_path.stat().st_size)
                                except Exception as _pdf_exc:
                                    _pdf_log.warning("Email daily PDF generation failed: %s", _pdf_exc, exc_info=True)
                                    pdf_path = None

                                attachments = [str(pdf_path)] if pdf_path and pdf_path.exists() and pdf_path.stat().st_size > 0 else []
                                ok, err = self._email_send_sync(
                                    subject=f"Shelly Energy Analyzer \u2013 Daily Report {due.strftime('%Y-%m-%d')}",
                                    body=msg_text,
                                    attachments=attachments,
                                )
                            except Exception as e:
                                ok, err = False, str(e)
                            if ok:
                                _st_set("daily_last", key)
                            elif err:
                                logging.getLogger(__name__).warning("Email daily summary failed: %s", err)
                except Exception as e:
                    logging.getLogger(__name__).warning("Email daily summary tick error: %s", e)

            # --- Monthly ---
            if em_monthly:
                try:
                    hh, mm = self._parse_hhmm(str(getattr(self.cfg.ui, "email_monthly_summary_time", "00:00") or "00:00"))
                    now_m = now.replace(day=1, hour=hh, minute=mm, second=0, microsecond=0)
                    key = f"{now_m.strftime('%Y-%m')}_{hh:02d}{mm:02d}"
                    grace_s = 86400  # 24h
                    last = _st_get("monthly_last", "")
                    attempt_ts_str = _st_get("monthly_attempt_ts", "0")
                    try:
                        attempt_ts = int(float(attempt_ts_str or 0))
                    except Exception:
                        attempt_ts = 0
                    if now >= now_m and now <= (now_m + timedelta(seconds=grace_s)) and last != key:
                        if not (attempt_ts and (now_ts - attempt_ts) < 300):
                            _st_set("monthly_attempt_ts", str(now_ts))
                            try:
                                from zoneinfo import ZoneInfo
                                tz = ZoneInfo("Europe/Berlin")
                                end_dt = datetime.now(tz)
                                start_dt = end_dt - timedelta(days=30)
                                prev_start = start_dt - timedelta(days=30)
                                msg_text = self._build_telegram_summary("monthly", start_dt, end_dt)

                                # Generate rich PDF attachment
                                _pdf_log = logging.getLogger(__name__)
                                pdf_path = None
                                try:
                                    from shelly_analyzer.services.export import export_pdf_email_monthly, export_pdf_summary
                                    tmp_dir = Path(base_dir) / "data" / "email_tmp"
                                    tmp_dir.mkdir(parents=True, exist_ok=True)
                                    pdf_path = tmp_dir / f"monthly_report_{now_m.strftime('%Y-%m')}.pdf"
                                    report_data = self._build_email_report_data(
                                        start_dt, end_dt, prev_start, start_dt, report_type="monthly"
                                    )
                                    _pdf_log.info("Email monthly PDF: report_data totals=%s", len(report_data.totals) if report_data else "None")
                                    if report_data and report_data.totals:
                                        export_pdf_email_monthly(report_data, pdf_path, lang=lang)
                                    else:
                                        totals = self._build_report_totals(start_dt, end_dt)
                                        _pdf_log.info("Email monthly PDF fallback: totals=%s", len(totals) if totals else "None")
                                        if totals:
                                            export_pdf_summary(
                                                title="Shelly Energy Analyzer \u2013 Monthly Report",
                                                period_label=f"{start_dt.strftime('%Y-%m-%d')} \u2013 {end_dt.strftime('%Y-%m-%d')}",
                                                totals=totals,
                                                out_path=pdf_path,
                                                lang=lang,
                                            )
                                        else:
                                            _pdf_log.warning("Email monthly PDF: no data found, sending email without attachment")
                                            pdf_path = None
                                    if pdf_path and pdf_path.exists():
                                        _pdf_log.info("Email monthly PDF generated: %s (%d bytes)", pdf_path, pdf_path.stat().st_size)
                                except Exception as _pdf_exc:
                                    _pdf_log.warning("Email monthly PDF generation failed: %s", _pdf_exc, exc_info=True)
                                    pdf_path = None

                                attachments = [str(pdf_path)] if pdf_path and pdf_path.exists() and pdf_path.stat().st_size > 0 else []

                                # Optionally attach invoice PDFs – one per device + combined (if >1 device)
                                tick_invoice_paths: list = []
                                if bool(getattr(self.cfg.ui, "email_monthly_invoice_enabled", False)):
                                    try:
                                        import tempfile as _tf
                                        from shelly_analyzer.services.export import (
                                            export_pdf_invoice as _exp_inv, InvoiceLine as _IL
                                        )
                                        _price_net   = self.cfg.pricing.unit_price_net() if hasattr(self.cfg.pricing, "unit_price_net") else self.cfg.pricing.unit_price_gross()
                                        _vat_enabled = bool(getattr(self.cfg.pricing, "vat_enabled", False))
                                        _vat_rate    = float(getattr(self.cfg.pricing, "vat_rate_percent", 0.0) or 0.0)
                                        _inv_totals  = (report_data.totals if report_data else None) or self._build_report_totals(start_dt, end_dt) or []
                                        _billing  = getattr(self.cfg, "billing", None)
                                        _iss      = getattr(_billing, "issuer", None) if _billing else None
                                        _cus      = getattr(_billing, "customer", None) if _billing else None
                                        _inv_pfx  = str(getattr(_billing, "invoice_prefix", "INV") or "INV") if _billing else "INV"
                                        _pay_days = int(getattr(_billing, "payment_terms_days", 14) or 14) if _billing else 14
                                        _logo     = str(getattr(_billing, "invoice_logo_path", "") or "") if _billing else ""
                                        _issuer_d = {
                                            "name":          str(getattr(_iss, "name", "") or "") if _iss else "",
                                            "address_lines": list(getattr(_iss, "address_lines", []) or []) if _iss else [],
                                            "vat_id":        str(getattr(_iss, "vat_id", "") or "") if _iss else "",
                                            "email":         str(getattr(_iss, "email", "") or "") if _iss else "",
                                            "iban":          str(getattr(_iss, "iban", "") or "") if _iss else "",
                                            "bic":           str(getattr(_iss, "bic", "") or "") if _iss else "",
                                        }
                                        _cust_d = {
                                            "name":          str(getattr(_cus, "name", "") or "") if _cus else "",
                                            "address_lines": list(getattr(_cus, "address_lines", []) or []) if _cus else [],
                                            "email":         str(getattr(_cus, "email", "") or "") if _cus else "",
                                        }
                                        _issue_dt   = end_dt.date() if hasattr(end_dt, "date") else end_dt
                                        _due_dt     = _issue_dt + timedelta(days=_pay_days)
                                        _period_lbl = f"{start_dt.strftime('%Y-%m-%d')} \u2013 {end_dt.strftime('%Y-%m-%d')}"
                                        _month_lbl  = now_m.strftime('%Y-%m')

                                        # Per-device invoices
                                        for _idx, _row in enumerate(_inv_totals, start=1):
                                            try:
                                                _tmp_d = _tf.NamedTemporaryFile(suffix=".pdf", delete=False)
                                                _dev_p = Path(_tmp_d.name)
                                                _tmp_d.close()
                                                _exp_inv(
                                                    out_path=_dev_p,
                                                    invoice_no=f"{_inv_pfx}-{_month_lbl}-{_idx:02d}",
                                                    issue_date=_issue_dt,
                                                    due_date=_due_dt,
                                                    issuer=_issuer_d,
                                                    customer=_cust_d,
                                                    vat_rate_percent=_vat_rate,
                                                    vat_enabled=_vat_enabled,
                                                    lines=[
                                                        _IL(
                                                            description=f"{_row.name} \u2013 {now_m.strftime('%B %Y')}",
                                                            quantity=round(_row.kwh_total, 3),
                                                            unit="kWh",
                                                            unit_price_net=_price_net,
                                                        )
                                                    ],
                                                    period_label=_period_lbl,
                                                    device_label=_row.name,
                                                    lang=lang,
                                                    logo_path=_logo or None,
                                                )
                                                if _dev_p.exists() and _dev_p.stat().st_size > 0:
                                                    tick_invoice_paths.append(_dev_p)
                                                    attachments.append(str(_dev_p))
                                            except Exception:
                                                pass

                                        # Combined invoice (all devices) – only if more than one device
                                        if len(_inv_totals) > 1:
                                            try:
                                                _tmp_c = _tf.NamedTemporaryFile(suffix=".pdf", delete=False)
                                                _comb_p = Path(_tmp_c.name)
                                                _tmp_c.close()
                                                _exp_inv(
                                                    out_path=_comb_p,
                                                    invoice_no=f"{_inv_pfx}-{_month_lbl}-000",
                                                    issue_date=_issue_dt,
                                                    due_date=_due_dt,
                                                    issuer=_issuer_d,
                                                    customer=_cust_d,
                                                    vat_rate_percent=_vat_rate,
                                                    vat_enabled=_vat_enabled,
                                                    lines=[
                                                        _IL(
                                                            description=f"{_r.name} \u2013 {now_m.strftime('%B %Y')}",
                                                            quantity=round(_r.kwh_total, 3),
                                                            unit="kWh",
                                                            unit_price_net=_price_net,
                                                        )
                                                        for _r in _inv_totals
                                                    ],
                                                    period_label=_period_lbl,
                                                    lang=lang,
                                                    logo_path=_logo or None,
                                                )
                                                if _comb_p.exists() and _comb_p.stat().st_size > 0:
                                                    tick_invoice_paths.append(_comb_p)
                                                    attachments.append(str(_comb_p))
                                            except Exception:
                                                pass
                                    except Exception:
                                        pass

                                ok, err = self._email_send_sync(
                                    subject=f"Shelly Energy Analyzer – Monthly Report {now_m.strftime('%Y-%m')}",
                                    body=msg_text,
                                    attachments=attachments,
                                )
                                for _inv_p in tick_invoice_paths:
                                    try:
                                        _inv_p.unlink(missing_ok=True)
                                    except Exception:
                                        pass
                            except Exception as e:
                                ok, err = False, str(e)
                            if ok:
                                _st_set("monthly_last", key)
                            elif err:
                                logging.getLogger(__name__).warning("Email monthly summary failed: %s", err)
                except Exception as e:
                    logging.getLogger(__name__).warning("Email monthly summary tick error: %s", e)

    def _email_send_daily_now(self) -> None:
            """Send the previous calendar day email report immediately (no auto-mark-as-sent)."""
            if not bool(getattr(self.cfg.ui, "email_enabled", False)):
                messagebox.showwarning(
                    self.t("settings.email.title"),
                    self.t("settings.email.enabled") + ": OFF",
                )
                return
            try:
                self._save_settings()
            except Exception:
                pass
            def _worker():
                _wlog = logging.getLogger(__name__)
                now = datetime.now()
                yesterday = now.date() - timedelta(days=1)
                start_dt = datetime.combine(yesterday, datetime.min.time())
                end_dt   = datetime.combine(now.date(), datetime.min.time())
                prev_start = start_dt - timedelta(days=1)
                prev_end   = start_dt
                try:
                    summary_text = self._build_telegram_summary("daily", start_dt, end_dt)
                    lang = str(getattr(self, "lang", None) or getattr(self.cfg.ui, "language", "de") or "de")
                    _wlog.info("_email_send_daily_now: building report data for %s to %s", start_dt, end_dt)
                    report_data = self._build_email_report_data(
                        start_dt, end_dt, prev_start, prev_end, report_type="daily"
                    )
                    _wlog.info("PDF report data: devices=%d, total_kwh=%.2f", len(report_data.totals) if report_data else 0, sum(r.kwh_total for r in report_data.totals) if report_data else 0.0)
                    import tempfile
                    tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
                    pdf_path = Path(tmp.name)
                    tmp.close()
                    if report_data and report_data.totals:
                        from shelly_analyzer.services.export import export_pdf_email_daily
                        export_pdf_email_daily(report_data, pdf_path, lang=lang)
                        _wlog.info("_email_send_daily_now: rich PDF generated, size=%d", pdf_path.stat().st_size if pdf_path.exists() else -1)
                    else:
                        from shelly_analyzer.services.export import export_pdf_summary
                        totals = self._build_report_totals(start_dt, end_dt)
                        _wlog.info("_email_send_daily_now: fallback totals=%s", len(totals) if totals else "None")
                        export_pdf_summary(
                            title=f"Daily Report {yesterday.strftime('%Y-%m-%d')}",
                            period_label=f"{start_dt.strftime('%Y-%m-%d')} \u2013 {end_dt.strftime('%Y-%m-%d')}",
                            totals=totals or [],
                            out_path=pdf_path,
                            lang=lang,
                        )
                        _wlog.info("_email_send_daily_now: fallback PDF generated, size=%d", pdf_path.stat().st_size if pdf_path.exists() else -1)
                    attachments = [str(pdf_path)] if pdf_path.exists() and pdf_path.stat().st_size > 0 else []
                    ok, err = self._email_send_sync(
                        subject=f"Shelly Energy Analyzer \u2013 Daily Report {yesterday.strftime('%Y-%m-%d')}",
                        body=summary_text,
                        attachments=attachments,
                    )
                    try:
                        pdf_path.unlink(missing_ok=True)
                    except Exception:
                        pass
                except Exception as e:
                    ok, err = False, str(e)
                def _done():
                    if ok:
                        messagebox.showinfo(self.t("settings.email.title"), "OK")
                    else:
                        messagebox.showwarning(self.t("settings.email.title"), f"Fehler: {err or 'unbekannt'}")
                try:
                    self.root.after(0, _done)
                except Exception:
                    _done()
            try:
                threading.Thread(target=_worker, daemon=True).start()
            except Exception as e:
                messagebox.showwarning(self.t("settings.email.title"), f"Fehler: {e}")

    def _email_send_monthly_now(self) -> None:
            """Send the previous calendar month email report immediately (no auto-mark-as-sent)."""
            if not bool(getattr(self.cfg.ui, "email_enabled", False)):
                messagebox.showwarning(
                    self.t("settings.email.title"),
                    self.t("settings.email.enabled") + ": OFF",
                )
                return
            try:
                self._save_settings()
            except Exception:
                pass
            def _worker():
                _wlog = logging.getLogger(__name__)
                now = datetime.now()
                if now.month == 1:
                    py, pm = now.year - 1, 12
                else:
                    py, pm = now.year, now.month - 1
                start_dt = datetime(py, pm, 1, 0, 0, 0)
                end_dt   = datetime(now.year, now.month, 1, 0, 0, 0)
                # Previous month for comparison
                if pm == 1:
                    ppy, ppm = py - 1, 12
                else:
                    ppy, ppm = py, pm - 1
                prev_start = datetime(ppy, ppm, 1, 0, 0, 0)
                prev_end   = start_dt
                try:
                    summary_text = self._build_telegram_summary("monthly", start_dt, end_dt)
                    lang = str(getattr(self, "lang", None) or getattr(self.cfg.ui, "language", "de") or "de")
                    _wlog.info("_email_send_monthly_now: building report data for %s to %s", start_dt, end_dt)
                    report_data = self._build_email_report_data(
                        start_dt, end_dt, prev_start, prev_end, report_type="monthly"
                    )
                    _wlog.info("_email_send_monthly_now: report_data totals=%s", len(report_data.totals) if report_data else "None")
                    import tempfile
                    tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
                    pdf_path = Path(tmp.name)
                    tmp.close()
                    if report_data and report_data.totals:
                        from shelly_analyzer.services.export import export_pdf_email_monthly
                        export_pdf_email_monthly(report_data, pdf_path, lang=lang)
                        _wlog.info("_email_send_monthly_now: rich PDF generated, size=%d", pdf_path.stat().st_size if pdf_path.exists() else -1)
                    else:
                        from shelly_analyzer.services.export import export_pdf_summary
                        totals = self._build_report_totals(start_dt, end_dt)
                        _wlog.info("_email_send_monthly_now: fallback totals=%s", len(totals) if totals else "None")
                        export_pdf_summary(
                            title=f"Monthly Report {start_dt.strftime('%Y-%m')}",
                            period_label=f"{start_dt.strftime('%Y-%m-%d')} \u2013 {end_dt.strftime('%Y-%m-%d')}",
                            totals=totals or [],
                            out_path=pdf_path,
                            lang=lang,
                        )
                        _wlog.info("_email_send_monthly_now: fallback PDF generated, size=%d", pdf_path.stat().st_size if pdf_path.exists() else -1)
                    attachments = [str(pdf_path)] if pdf_path.exists() and pdf_path.stat().st_size > 0 else []

                    # Optionally attach invoice PDFs – one per device + combined (if >1 device)
                    invoice_paths: list = []
                    if bool(getattr(self.cfg.ui, "email_monthly_invoice_enabled", False)):
                        try:
                            from shelly_analyzer.services.export import (
                                export_pdf_invoice, InvoiceLine, ReportTotals as _RT
                            )
                            price_net   = self.cfg.pricing.unit_price_net() if hasattr(self.cfg.pricing, "unit_price_net") else self.cfg.pricing.unit_price_gross()
                            vat_enabled = bool(getattr(self.cfg.pricing, "vat_enabled", False))
                            vat_rate    = float(getattr(self.cfg.pricing, "vat_rate_percent", 0.0) or 0.0)
                            inv_totals  = (report_data.totals if report_data else None) or self._build_report_totals(start_dt, end_dt) or []
                            # Build issuer/customer dicts from BillingConfig
                            billing = getattr(self.cfg, "billing", None)
                            _iss = getattr(billing, "issuer", None) if billing else None
                            _cus = getattr(billing, "customer", None) if billing else None
                            inv_prefix = str(getattr(billing, "invoice_prefix", "INV") or "INV") if billing else "INV"
                            pay_days   = int(getattr(billing, "payment_terms_days", 14) or 14) if billing else 14
                            logo_path  = str(getattr(billing, "invoice_logo_path", "") or "") if billing else ""
                            issuer_dict = {
                                "name":          str(getattr(_iss, "name", "") or "") if _iss else "",
                                "address_lines": list(getattr(_iss, "address_lines", []) or []) if _iss else [],
                                "vat_id":        str(getattr(_iss, "vat_id", "") or "") if _iss else "",
                                "email":         str(getattr(_iss, "email", "") or "") if _iss else "",
                                "iban":          str(getattr(_iss, "iban", "") or "") if _iss else "",
                                "bic":           str(getattr(_iss, "bic", "") or "") if _iss else "",
                            }
                            customer_dict = {
                                "name":          str(getattr(_cus, "name", "") or "") if _cus else "",
                                "address_lines": list(getattr(_cus, "address_lines", []) or []) if _cus else [],
                                "email":         str(getattr(_cus, "email", "") or "") if _cus else "",
                            }
                            issue_date  = end_dt.date()
                            from datetime import timedelta as _td
                            due_date    = issue_date + _td(days=pay_days)
                            period_lbl  = f"{start_dt.strftime('%Y-%m-%d')} \u2013 {end_dt.strftime('%Y-%m-%d')}"
                            month_lbl   = start_dt.strftime('%Y-%m')

                            # Per-device invoices
                            for idx, row in enumerate(inv_totals, start=1):
                                try:
                                    dev_lines = [
                                        InvoiceLine(
                                            description=f"{row.name} \u2013 {start_dt.strftime('%B %Y')}",
                                            quantity=round(row.kwh_total, 3),
                                            unit="kWh",
                                            unit_price_net=price_net,
                                        )
                                    ]
                                    tmp_dev = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
                                    dev_path = Path(tmp_dev.name)
                                    tmp_dev.close()
                                    export_pdf_invoice(
                                        out_path=dev_path,
                                        invoice_no=f"{inv_prefix}-{month_lbl}-{idx:02d}",
                                        issue_date=issue_date,
                                        due_date=due_date,
                                        issuer=issuer_dict,
                                        customer=customer_dict,
                                        vat_rate_percent=vat_rate,
                                        vat_enabled=vat_enabled,
                                        lines=dev_lines,
                                        period_label=period_lbl,
                                        device_label=row.name,
                                        lang=lang,
                                        logo_path=logo_path or None,
                                    )
                                    if dev_path.exists() and dev_path.stat().st_size > 0:
                                        invoice_paths.append(dev_path)
                                        attachments.append(str(dev_path))
                                except Exception:
                                    pass

                            # Combined invoice (all devices as line items) – only if more than one device
                            if len(inv_totals) > 1:
                                try:
                                    comb_lines = [
                                        InvoiceLine(
                                            description=f"{row.name} \u2013 {start_dt.strftime('%B %Y')}",
                                            quantity=round(row.kwh_total, 3),
                                            unit="kWh",
                                            unit_price_net=price_net,
                                        )
                                        for row in inv_totals
                                    ]
                                    tmp_comb = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
                                    comb_path = Path(tmp_comb.name)
                                    tmp_comb.close()
                                    export_pdf_invoice(
                                        out_path=comb_path,
                                        invoice_no=f"{inv_prefix}-{month_lbl}-000",
                                        issue_date=issue_date,
                                        due_date=due_date,
                                        issuer=issuer_dict,
                                        customer=customer_dict,
                                        vat_rate_percent=vat_rate,
                                        vat_enabled=vat_enabled,
                                        lines=comb_lines,
                                        period_label=period_lbl,
                                        lang=lang,
                                        logo_path=logo_path or None,
                                    )
                                    if comb_path.exists() and comb_path.stat().st_size > 0:
                                        invoice_paths.append(comb_path)
                                        attachments.append(str(comb_path))
                                except Exception:
                                    pass
                        except Exception:
                            pass

                    ok, err = self._email_send_sync(
                        subject=f"Shelly Energy Analyzer \u2013 Monthly Report {start_dt.strftime('%Y-%m')}",
                        body=summary_text,
                        attachments=attachments,
                    )
                    try:
                        pdf_path.unlink(missing_ok=True)
                    except Exception:
                        pass
                    for _inv_p in invoice_paths:
                        try:
                            _inv_p.unlink(missing_ok=True)
                        except Exception:
                            pass
                except Exception as e:
                    ok, err = False, str(e)
                def _done():
                    if ok:
                        messagebox.showinfo(self.t("settings.email.title"), "OK")
                    else:
                        messagebox.showwarning(self.t("settings.email.title"), f"Fehler: {err or 'unbekannt'}")
                try:
                    self.root.after(0, _done)
                except Exception:
                    _done()
            try:
                threading.Thread(target=_worker, daemon=True).start()
            except Exception as e:
                messagebox.showwarning(self.t("settings.email.title"), f"Fehler: {e}")

    def _build_report_totals(self, start_dt, end_dt):
            """Build ReportTotals list for PDF export from DB data."""
            _log = logging.getLogger(__name__)
            try:
                from shelly_analyzer.services.export import ReportTotals
                totals = []
                storage = getattr(self, "storage", None)
                if storage is None:
                    _log.warning("_build_report_totals: self.storage is None")
                    return None
                start_ts = int(start_dt.timestamp())
                end_ts = int(end_dt.timestamp())
                _log.info("_build_report_totals: querying %s to %s (ts %d..%d)", start_dt, end_dt, start_ts, end_ts)
                for dev in (self.cfg.devices or []):
                    try:
                        df = storage.read_device_df(dev.key, start_ts=start_ts, end_ts=end_ts)
                        if df is None or df.empty:
                            _log.info("_build_report_totals: no data for device %s in range", dev.key)
                            continue
                        pwr_col = None
                        for c in ("total_power", "a_act_power", "power_w"):
                            if c in df.columns:
                                pwr_col = c
                                break
                        powers = df[pwr_col].dropna().astype(float).tolist() if pwr_col else []
                        avg_w = sum(powers) / len(powers) if powers else 0.0
                        max_w = max(powers) if powers else 0.0
                        kwh = 0.0
                        for c in ("energy_kwh", "total_act", "energy_wh"):
                            if c in df.columns:
                                vals = df[c].dropna()
                                if not vals.empty:
                                    if c == "energy_kwh":
                                        kwh = float(vals.sum())
                                    else:
                                        kwh = (vals.max() - vals.min()) / 1000.0
                                    break
                        if kwh <= 0 and powers and "timestamp" in df.columns:
                            ts_col = df["timestamp"].dropna()
                            if len(ts_col) > 1:
                                total_hours = (ts_col.max() - ts_col.min()).total_seconds() / 3600.0
                                if total_hours > 0:
                                    kwh = avg_w * total_hours / 1000.0
                        price = self.cfg.pricing.unit_price_gross()
                        cost = kwh * price
                        totals.append(ReportTotals(
                            name=dev.name,
                            kwh_total=round(kwh, 3),
                            avg_power_w=round(avg_w, 1),
                            max_power_w=round(max_w, 1),
                            cost_eur=round(cost, 2),
                        ))
                    except Exception as _dev_exc:
                        _log.warning("_build_report_totals: error for device %s: %s", dev.key, _dev_exc)
                        continue
                _log.info("_build_report_totals: built %d device totals", len(totals))
                return totals if totals else None
            except Exception as _exc:
                _log.warning("_build_report_totals failed: %s", _exc, exc_info=True)
                return None

    def _build_email_report_data(self, start_dt, end_dt, prev_start_dt=None, prev_end_dt=None, report_type="daily"):
            """Build an EmailReportData object with per-device time-series and extended stats."""
            _log = logging.getLogger(__name__)
            try:
                import shelly_analyzer as _pkg
                from shelly_analyzer.services.export import ReportTotals, EmailReportData
                storage = getattr(self, "storage", None)
                if storage is None:
                    _log.warning("_build_email_report_data: self.storage is None")
                    return None

                start_ts = int(start_dt.timestamp())
                end_ts   = int(end_dt.timestamp())
                price    = self.cfg.pricing.unit_price_gross()
                co2_int  = float(getattr(getattr(self.cfg, "pricing", None), "co2_intensity_g_per_kwh", 380.0) or 380.0)

                totals:               list = []
                hourly_kwh:           list = [0.0] * 24
                daily_map:            dict = {}
                per_device_hourly:    dict = {}  # dev_name -> List[float] len 24
                per_device_daily_map: dict = {}  # dev_name -> Dict[date, float]

                _log.info("_build_email_report_data: type=%s, querying %s to %s (ts %d..%d)", report_type, start_dt, end_dt, start_ts, end_ts)
                for dev in (self.cfg.devices or []):
                    try:
                        df = storage.read_device_df(dev.key, start_ts=start_ts, end_ts=end_ts)
                        if df is None or df.empty:
                            _log.info("_build_email_report_data: no data for device %s in range", dev.key)
                            continue

                        pwr_col = None
                        for col in ("total_power", "total_act_power", "a_act_power", "power_w"):
                            if col in df.columns:
                                pwr_col = col
                                break
                        powers = df[pwr_col].dropna().astype(float).tolist() if pwr_col else []
                        avg_w  = sum(powers) / len(powers) if powers else 0.0
                        max_w  = max(powers) if powers else 0.0

                        kwh = 0.0
                        for col in ("energy_kwh", "total_act", "energy_wh"):
                            if col in df.columns:
                                vals = df[col].dropna()
                                if not vals.empty:
                                    if col == "energy_kwh":
                                        kwh = float(vals.sum())
                                    else:
                                        kwh = (vals.max() - vals.min()) / 1000.0
                                    break
                        if kwh <= 0 and powers and "timestamp" in df.columns:
                            ts_col = pd.to_datetime(df["timestamp"], errors="coerce").dropna()
                            if len(ts_col) > 1:
                                th = (ts_col.max() - ts_col.min()).total_seconds() / 3600.0
                                if th > 0:
                                    kwh = avg_w * th / 1000.0

                        totals.append(ReportTotals(
                            name=dev.name,
                            kwh_total=round(kwh, 3),
                            avg_power_w=round(avg_w, 1),
                            max_power_w=round(max_w, 1),
                            cost_eur=round(kwh * price, 2),
                        ))

                        # Build aggregate + per-device hourly/daily time series
                        if "timestamp" in df.columns:
                            try:
                                dev_hourly_vals = [0.0] * 24
                                dev_day_map: dict = {}
                                if pwr_col:
                                    tdf = df[["timestamp", pwr_col]].copy()
                                    tdf["timestamp"] = pd.to_datetime(tdf["timestamp"], errors="coerce")
                                    tdf = tdf.dropna(subset=["timestamp"]).set_index("timestamp").sort_index()
                                    tdf[pwr_col] = tdf[pwr_col].astype(float)
                                    if report_type == "daily":
                                        hrly = tdf[pwr_col].resample("h").mean()
                                        for ts_h, w_val in hrly.items():
                                            h_idx = ts_h.hour
                                            if 0 <= h_idx < 24 and not pd.isna(w_val):
                                                v = float(w_val) / 1000.0
                                                hourly_kwh[h_idx]      += v
                                                dev_hourly_vals[h_idx] += v
                                    else:
                                        # Correct kWh: resample to hourly mean (W), divide by 1000
                                        # to get kWh per hour, then sum to daily totals.
                                        # resample("D").mean() / 1000 would give kW not kWh.
                                        hrly_kwh = tdf[pwr_col].resample("h").mean() / 1000.0
                                        dly = hrly_kwh.resample("D").sum()
                                        for ts_d, kwh_val in dly.items():
                                            d_key = ts_d.date()
                                            if not pd.isna(kwh_val) and kwh_val > 0:
                                                v = float(kwh_val)
                                                daily_map[d_key] = daily_map.get(d_key, 0.0) + v
                                                dev_day_map[d_key] = dev_day_map.get(d_key, 0.0) + v
                                elif "energy_kwh" in df.columns:
                                    tdf = df[["timestamp", "energy_kwh"]].copy()
                                    tdf["timestamp"] = pd.to_datetime(tdf["timestamp"], errors="coerce")
                                    tdf = tdf.dropna(subset=["timestamp"]).set_index("timestamp").sort_index()
                                    tdf["energy_kwh"] = tdf["energy_kwh"].astype(float)
                                    if report_type == "daily":
                                        hrly = tdf["energy_kwh"].resample("h").sum()
                                        for ts_h, e_val in hrly.items():
                                            h_idx = ts_h.hour
                                            if 0 <= h_idx < 24 and not pd.isna(e_val):
                                                v = float(e_val)
                                                hourly_kwh[h_idx]      += v
                                                dev_hourly_vals[h_idx] += v
                                    else:
                                        dly = tdf["energy_kwh"].resample("D").sum()
                                        for ts_d, e_val in dly.items():
                                            d_key = ts_d.date()
                                            if not pd.isna(e_val):
                                                v = float(e_val)
                                                daily_map[d_key] = daily_map.get(d_key, 0.0) + v
                                                dev_day_map[d_key] = dev_day_map.get(d_key, 0.0) + v
                                per_device_hourly[dev.name] = dev_hourly_vals
                                per_device_daily_map[dev.name] = dev_day_map
                            except Exception as _ts_exc:
                                _log.warning("_build_email_report_data: timeseries error for device %s: %s", dev.key, _ts_exc)
                    except Exception as _dev_exc:
                        _log.warning("_build_email_report_data: error for device %s: %s", dev.key, _dev_exc)
                        continue

                # Build sorted aggregate daily_kwh list
                daily_kwh = sorted(daily_map.items(), key=lambda x: x[0])
                # Build per-device daily sorted lists
                per_device_daily = {
                    nm: sorted(dm.items(), key=lambda x: x[0])
                    for nm, dm in per_device_daily_map.items()
                }

                # Extra stats
                peak_hour = int(hourly_kwh.index(max(hourly_kwh))) if any(v > 0 for v in hourly_kwh) else -1
                valid_avgs = [r.avg_power_w for r in totals if r.avg_power_w > 0]
                avg_power_w = sum(valid_avgs) / len(valid_avgs) if valid_avgs else 0.0
                peak_power_w = max((r.max_power_w for r in totals), default=0.0)

                # Previous period totals for comparison
                prev_kwh  = 0.0
                prev_cost = 0.0
                if prev_start_dt and prev_end_dt:
                    prev_totals = self._build_report_totals(prev_start_dt, prev_end_dt)
                    if prev_totals:
                        prev_kwh  = sum(r.kwh_total for r in prev_totals)
                        prev_cost = sum(r.cost_eur  for r in prev_totals)

                # Same-weekday-last-week comparison (daily only)
                prev_same_weekday_kwh = 0.0
                if report_type == "daily":
                    try:
                        wk_start = start_dt - timedelta(days=7)
                        wk_end   = start_dt - timedelta(days=6)
                        wk_totals = self._build_report_totals(wk_start, wk_end)
                        if wk_totals:
                            prev_same_weekday_kwh = round(sum(r.kwh_total for r in wk_totals), 3)
                    except Exception:
                        pass

                # Monthly-specific stats from daily_map
                weekday_avg_kwh  = 0.0
                weekend_avg_kwh  = 0.0
                best_day_date    = None
                best_day_kwh     = 0.0
                worst_day_date   = None
                worst_day_kwh    = 0.0
                if daily_map:
                    wk_vals  = [v for d, v in daily_map.items() if d.weekday() < 5]
                    we_vals  = [v for d, v in daily_map.items() if d.weekday() >= 5]
                    weekday_avg_kwh  = round(sum(wk_vals) / len(wk_vals), 3) if wk_vals else 0.0
                    weekend_avg_kwh  = round(sum(we_vals) / len(we_vals), 3) if we_vals else 0.0
                    best_day_date  = min(daily_map, key=daily_map.get)
                    best_day_kwh   = round(daily_map[best_day_date], 3)
                    worst_day_date = max(daily_map, key=daily_map.get)
                    worst_day_kwh  = round(daily_map[worst_day_date], 3)

                total_kwh = sum(r.kwh_total for r in totals)
                co2_kg    = total_kwh * co2_int / 1000.0

                vat_rate = 0.0
                try:
                    p = getattr(self.cfg, "pricing", None)
                    if p and getattr(p, "vat_enabled", False):
                        vat_rate = float(getattr(p, "vat_rate_percent", 0.0) or 0.0) / 100.0
                except Exception:
                    pass

                version = getattr(_pkg, "__version__", "")

                _log.info("_build_email_report_data: done, totals=%d devices, total_kwh=%.3f, peak_hour=%d", len(totals), total_kwh, peak_hour)
                return EmailReportData(
                    report_type=report_type,
                    period_start=start_dt,
                    period_end=end_dt,
                    totals=totals,
                    hourly_kwh=hourly_kwh,
                    daily_kwh=daily_kwh,
                    co2_kg=round(co2_kg, 3),
                    co2_intensity_g_per_kwh=co2_int,
                    prev_kwh=round(prev_kwh, 3),
                    prev_cost_eur=round(prev_cost, 2),
                    price_per_kwh=price,
                    vat_rate=vat_rate,
                    version=version,
                    per_device_hourly=per_device_hourly,
                    per_device_daily=per_device_daily,
                    peak_hour=peak_hour,
                    avg_power_w=round(avg_power_w, 1),
                    peak_power_w=round(peak_power_w, 1),
                    prev_same_weekday_kwh=prev_same_weekday_kwh,
                    weekday_avg_kwh=weekday_avg_kwh,
                    weekend_avg_kwh=weekend_avg_kwh,
                    best_day_date=best_day_date,
                    best_day_kwh=best_day_kwh,
                    worst_day_date=worst_day_date,
                    worst_day_kwh=worst_day_kwh,
                )
            except Exception as _exc:
                _log.warning("_build_email_report_data failed: %s", _exc, exc_info=True)
                return None

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

            # Normalize range to Europe/Berlin (tz-aware) to avoid mixed tz comparisons
            try:
                from zoneinfo import ZoneInfo
                tz = ZoneInfo("Europe/Berlin")
                if getattr(start_dt, "tzinfo", None) is None:
                    start_dt = start_dt.tz_localize(tz)
                else:
                    start_dt = start_dt.tz_convert(tz)
                if getattr(end_dt, "tzinfo", None) is None:
                    end_dt = end_dt.tz_localize(tz)
                else:
                    end_dt = end_dt.tz_convert(tz)
            except Exception:
                pass


            # threshold for cosφ/VAR statistics
            try:
                load_thr_w = float(getattr(self.cfg.ui, "telegram_summary_load_w", 200.0) or 200.0)
            except Exception:
                load_thr_w = 200.0

            total_kwh = 0.0
            per_dev_kwh = []  # (name, key, kwh)
            tou_total_cost: Optional[float] = None  # None when TOU disabled
            tou_breakdown: Dict[str, Tuple[float, float]] = {}  # {name: (kwh, cost)}

            # hourly energy across ALL devices
            hourly_kwh = None  # pd.Series indexed by hour timestamp

            # Power stats (overall)
            sum_power = 0.0
            cnt_power = 0
            min_power_w = None
            all_power_values = []  # for standby detection (10th percentile)

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

            # Price (€/kWh) from PricingConfig (gross)
            unit_gross = None
            try:
                unit_gross = float(self.cfg.pricing.unit_price_gross())
            except Exception:
                try:
                    unit_gross = float(getattr(getattr(self.cfg, "pricing", None), "electricity_price_eur_per_kwh", None))
                except Exception:
                    unit_gross = None

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

                # ensure timestamp is timezone-aware for slicing (assume UTC when naive) and convert to Europe/Berlin
                try:
                    from zoneinfo import ZoneInfo
                    tz = ZoneInfo("Europe/Berlin")
                    utc = ZoneInfo("UTC")
                    if df["timestamp"].dt.tz is None:
                        df["timestamp"] = df["timestamp"].dt.tz_localize(utc)
                    df["timestamp"] = df["timestamp"].dt.tz_convert(tz)
                except Exception:
                    pass

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
                # NOTE: EMData CSVs often contain per-interval Wh per phase (a/b/c_total_act_energy).
                # For summaries we must treat these as interval energy (not cumulative counters).
                df_e = None
                try:
                    phase_cols = [c for c in ("a_total_act_energy", "b_total_act_energy", "c_total_act_energy") if c in df_use.columns]
                    if phase_cols:
                        tmp = df_use.copy()
                        wh = tmp[phase_cols].sum(axis=1)
                        tmp["energy_kwh"] = pd.to_numeric(wh, errors="coerce").fillna(0.0) / 1000.0
                        df_e = tmp
                except Exception:
                    df_e = None

                if df_e is None:
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

                # TOU cost breakdown per device
                try:
                    _tou_cfg = getattr(self.cfg, "tou", TouConfig())
                    if getattr(_tou_cfg, "enabled", False) and getattr(_tou_cfg, "rates", None) and "energy_kwh" in df_e.columns and "timestamp" in df_e.columns:
                        from zoneinfo import ZoneInfo as _ZoneInfo
                        _tz = _ZoneInfo("Europe/Berlin")
                        _, _dev_breakdown = _tou_cost_breakdown(df_e["timestamp"], pd.to_numeric(df_e["energy_kwh"], errors="coerce").fillna(0.0), self.cfg.pricing, _tou_cfg, _tz)
                        for _rname, (_rkwh, _rcost) in _dev_breakdown.items():
                            if _rname in tou_breakdown:
                                _ek, _ec = tou_breakdown[_rname]
                                tou_breakdown[_rname] = (_ek + _rkwh, _ec + _rcost)
                            else:
                                tou_breakdown[_rname] = (_rkwh, _rcost)
                        if tou_total_cost is None:
                            tou_total_cost = 0.0
                        tou_total_cost += sum(c for _, c in _dev_breakdown.values())
                except Exception:
                    pass

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
                            # Collect for standby detection (sample max 2000 points per device)
                            try:
                                vals = p.values
                                if len(vals) > 2000:
                                    import numpy as np
                                    vals = np.random.choice(vals, 2000, replace=False)
                                all_power_values.extend(float(v) for v in vals if v >= 0)
                            except Exception:
                                pass

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
                title = "📊 Tages-Zusammenfassung (letzte 24h)"
            else:
                title = "📊 Monats-Zusammenfassung (letzte 30 Tage)"

            range_line = f"⏱ Zeitraum: {start_dt.strftime('%Y-%m-%d %H:%M')} – {end_dt.strftime('%Y-%m-%d %H:%M')}"
            lines = [title, "", range_line, ""]

            # totals
            lines.append(f"⚡ Verbrauch: {total_kwh:.3f} kWh")

            if tou_total_cost is not None:
                try:
                    lines.append(f"💶 Kosten: {tou_total_cost:.2f} € (TOU)")
                    if tou_breakdown:
                        for _rname, (_rkwh, _rcost) in tou_breakdown.items():
                            lines.append(f"   ↳ {_rname}: {_rkwh:.3f} kWh · {_rcost:.2f} €")
                except Exception:
                    pass
            elif unit_gross is not None:
                try:
                    cost = total_kwh * unit_gross
                    lines.append(f"💶 Kosten: {cost:.2f} €")
                except Exception:
                    pass

            # CO₂ emissions
            try:
                co2_g_per_kwh = float(getattr(self.cfg.pricing, "co2_intensity_g_per_kwh", 380.0) or 0.0)
                if co2_g_per_kwh > 0:
                    co2_kg = total_kwh * co2_g_per_kwh / 1000.0
                    lines.append(f"🌿 CO₂: {co2_kg:.3f} kg ({co2_g_per_kwh:.0f} g/kWh)")
            except Exception:
                pass

            # Comparison with previous period (Vortag / Vormonat)
            try:
                period_delta = end_dt - start_dt
                prev_start = start_dt - period_delta
                prev_end = start_dt
                prev_kwh = 0.0
                for d in list(getattr(self.cfg, "devices", []) or []):
                    try:
                        df_prev = self.storage.read_device_df(d.key)
                        if df_prev is None or getattr(df_prev, "empty", True):
                            continue
                        df_prev = df_prev.copy()
                        if "timestamp" not in df_prev.columns:
                            continue
                        df_prev["timestamp"] = pd.to_datetime(df_prev["timestamp"], errors="coerce")
                        df_prev = df_prev.dropna(subset=["timestamp"])
                        try:
                            from zoneinfo import ZoneInfo
                            _tz = ZoneInfo("Europe/Berlin")
                            _utc = ZoneInfo("UTC")
                            if df_prev["timestamp"].dt.tz is None:
                                df_prev["timestamp"] = df_prev["timestamp"].dt.tz_localize(_utc)
                            df_prev["timestamp"] = df_prev["timestamp"].dt.tz_convert(_tz)
                        except Exception:
                            pass
                        m = (df_prev["timestamp"] >= prev_start) & (df_prev["timestamp"] < prev_end)
                        df_p = df_prev.loc[m]
                        if df_p.empty:
                            continue
                        phase_cols = [c for c in ("a_total_act_energy", "b_total_act_energy", "c_total_act_energy") if c in df_p.columns]
                        if phase_cols:
                            wh = df_p[phase_cols].sum(axis=1)
                            prev_kwh += float(pd.to_numeric(wh, errors="coerce").fillna(0.0).sum()) / 1000.0
                        elif "energy_kwh" in df_p.columns:
                            prev_kwh += float(pd.to_numeric(df_p["energy_kwh"], errors="coerce").fillna(0.0).sum())
                        else:
                            from shelly_analyzer.core.energy import calculate_energy as _calc_e
                            df_calc = _calc_e(df_p, method="auto")
                            if "energy_kwh" in df_calc.columns:
                                prev_kwh += float(pd.to_numeric(df_calc["energy_kwh"], errors="coerce").fillna(0.0).sum())
                    except Exception:
                        continue

                if prev_kwh > 0:
                    delta_pct = ((total_kwh - prev_kwh) / prev_kwh) * 100.0
                    arrow = "📈" if delta_pct > 0 else "📉"
                    prev_label = "Vortag" if kind == "daily" else "Vormonat"
                    lines.append(f"{arrow} vs. {prev_label}: {prev_kwh:.3f} kWh ({delta_pct:+.1f}%)")
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
            try:
                _co2_g = float(getattr(self.cfg.pricing, "co2_intensity_g_per_kwh", 380.0) or 0.0)
            except Exception:
                _co2_g = 0.0
            if per_dev_kwh:
                for name, _key, kwh in per_dev_kwh:
                    parts_dev = [f"{kwh:.3f} kWh"]
                    if unit_gross is not None:
                        parts_dev.append(f"{(kwh * unit_gross):.2f} €")
                    if _co2_g > 0:
                        parts_dev.append(f"{kwh * _co2_g / 1000.0:.3f} kg CO₂")
                    lines.append(f" - {name}: {' · '.join(parts_dev)}")
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

            # Standby detection: 10th percentile of power = estimated base load
            try:
                if len(all_power_values) >= 20:
                    import numpy as np
                    arr = np.array(all_power_values)
                    standby_w = float(np.percentile(arr, 10))
                    if standby_w > 0:
                        standby_kwh_year = standby_w * 24 * 365 / 1000.0
                        lines.append("")
                        lines.append(f"🔌 Standby-Grundlast: ~{standby_w:.0f} W")
                        lines.append(f"   = ~{standby_kwh_year:.0f} kWh/Jahr")
                        if unit_gross is not None:
                            standby_cost_year = standby_kwh_year * unit_gross
                            lines.append(f"   = ~{standby_cost_year:.0f} €/Jahr")
                        try:
                            _sb_co2_g = float(getattr(self.cfg.pricing, "co2_intensity_g_per_kwh", 380.0) or 0.0)
                            if _sb_co2_g > 0:
                                standby_co2_year = standby_kwh_year * _sb_co2_g / 1000.0
                                lines.append(f"   = ~{standby_co2_year:.0f} kg CO₂/Jahr")
                        except Exception:
                            pass
            except Exception:
                pass

            # Solar / PV summary (append if configured)
            try:
                solar_text = self._solar_build_summary_text(start, end)
                if solar_text:
                    lines.append("")
                    lines.append(solar_text)
            except Exception:
                pass

            # Group summaries (only if groups are configured)
            try:
                cfg_groups = list(getattr(self.cfg, "groups", []) or [])
                if cfg_groups:
                    lines.append("")
                    lines.append("📦 Gruppen:")
                    key_map = {d.key: d.name for d in self.cfg.devices}
                    for grp in cfg_groups:
                        grp_kwh = 0.0
                        grp_cost = 0.0
                        for dkey in (grp.device_keys or []):
                            try:
                                _kwh_s = self._telegram_kwh_series(start_dt, end_dt, freq="H" if kind == "daily" else "D", device_key=dkey)
                                grp_kwh += float(_kwh_s.sum()) if len(_kwh_s) > 0 else 0.0
                            except Exception:
                                pass
                        if unit_gross is not None:
                            grp_cost = grp_kwh * unit_gross
                        grp_parts = [f"{grp_kwh:.3f} kWh"]
                        if grp_cost > 0:
                            grp_parts.append(f"{grp_cost:.2f} €")
                        pct = (grp_kwh / total_kwh * 100.0) if total_kwh > 0 else 0.0
                        lines.append(f" - {grp.name}: {' · '.join(grp_parts)} ({pct:.1f}%)")
            except Exception:
                pass

            return "\n".join(lines).strip() + "\n"

    def _sync_before_telegram_summary(self) -> None:
        """Synchronize data from Shelly devices before building a Telegram summary.

        Runs synchronously because the summary must use fresh data.
        Demo devices are skipped (they generate data locally).
        """
        import logging as _logging
        _log = _logging.getLogger(__name__)

        try:
            if bool(getattr(getattr(self.cfg, "demo", None), "enabled", False)):
                try:
                    from shelly_analyzer.services.demo import ensure_demo_csv
                    ensure_demo_csv(
                        self.storage,
                        list(getattr(self.cfg, "devices", []) or []),
                        getattr(self.cfg, "demo"),
                    )
                except Exception:
                    pass
                return

            _log.info("Telegram summary: syncing data before send...")
            try:
                results = sync_all(
                    self.cfg,
                    self.storage,
                    range_override=None,
                    fallback_last_days=2,
                )
                ok_count = sum(
                    1 for r in results
                    if any(c.ok for c in r.chunks)
                )
                _log.info(
                    "Telegram summary: sync done (%d/%d devices had new data)",
                    ok_count, len(results),
                )
            except Exception as e:
                _log.warning("Telegram summary: sync failed: %s (using existing data)", e)

            try:
                from shelly_analyzer.services.compute import load_device
                self.computed = {
                    d.key: load_device(self.storage, d)
                    for d in self.cfg.devices
                }
            except Exception as e:
                _log.warning("Telegram summary: data reload failed: %s", e)

        except Exception as e:
            _log.warning("Telegram summary: pre-sync error: %s", e)

    def _telegram_send_daily_summary_now(self) -> None:
            """Send the previous calendar day immediately (does not mark as sent)."""
            if not bool(getattr(self.cfg.ui, "telegram_enabled", False)):
                try:
                    self._show_msgbox(self.t("settings.alerts.telegram.disabled"), kind="warning")
                except Exception:
                    pass
                return

            try:
                from zoneinfo import ZoneInfo
                tz = getattr(self, '_tz_berlin', None) or ZoneInfo('Europe/Berlin')
                now = datetime.now(tz)
                # Daily summary must always be "previous calendar day" (00:00..24:00 local time)
                prev_day = (now - timedelta(days=1)).date()
                start_dt = datetime.combine(prev_day, datetime.min.time(), tzinfo=tz)
                end_dt = start_dt + timedelta(days=1)
                # Sync data before building summary
                self._sync_before_telegram_summary()

                msg = self._build_telegram_summary("daily", start_dt, end_dt)
                imgs = self._telegram_make_summary_plots("daily", start_dt, end_dt)
                ok, err = self._telegram_send_with_images(msg, imgs)
                if ok:
                    messagebox.showinfo("Telegram", "OK (gesendet)")
                else:
                    messagebox.showwarning("Telegram", f"Fehler: {err}")
            except Exception as e:
                try:
                    messagebox.showwarning("Telegram", f"Fehler: {e}")
                except Exception:
                    pass

    def _telegram_send_monthly_summary_now(self) -> None:
            """Send the last 30 days immediately (does not mark as sent)."""
            if not bool(getattr(self.cfg.ui, "telegram_enabled", False)):
                try:
                    self._show_msgbox(self.t("settings.alerts.telegram.disabled"), kind="warning")
                except Exception:
                    pass
                return

            try:
                from zoneinfo import ZoneInfo
                now = datetime.now(ZoneInfo('Europe/Berlin'))
                end_dt = now
                start_dt = end_dt - timedelta(days=30)

                # Sync data before building summary
                self._sync_before_telegram_summary()

                msg = self._build_telegram_summary("monthly", start_dt, end_dt)
                imgs = self._telegram_make_summary_plots("monthly", start_dt, end_dt)
                ok, err = self._telegram_send_with_images(msg, imgs)
                if ok:
                    messagebox.showinfo("Telegram", "OK (gesendet)")
                else:
                    messagebox.showwarning("Telegram", f"Fehler: {err}")
            except Exception as e:
                try:
                    messagebox.showwarning("Telegram", f"Fehler: {e}")
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

    def _build_setup_wizard_tab(self) -> None:
            """Guided setup for first run (no config / no devices)."""
            frm = getattr(self, "tab_setup", None)
            if frm is None:
                return

            # Root layout
            root = ttk.Frame(frm)
            root.pack(fill="both", expand=True, padx=14, pady=12)

            ttk.Label(root, text=self.t("setup.title"), font=("TkDefaultFont", 15, "bold")).pack(anchor="w", pady=(0, 6))
            ttk.Label(root, text=self.t("setup.subtitle"), justify="left", wraplength=980).pack(anchor="w", pady=(0, 10))

            nb = ttk.Notebook(root)
            nb.pack(fill="both", expand=True)
            self._wiz_nb = nb

            # Steps
            step_devices = ttk.Frame(nb)
            step_telegram = ttk.Frame(nb)
            step_finish = ttk.Frame(nb)
            nb.add(step_devices, text=self.t("setup.step.devices"))
            nb.add(step_telegram, text=self.t("setup.step.telegram"))
            nb.add(step_finish, text=self.t("setup.step.finish"))

            # ---- Step 1: Devices (auto-discovery) ----
            self._wiz_found = {}  # host -> discovered dict
            self._wiz_scan_cancel = False

            dev_top = ttk.Frame(step_devices)
            dev_top.pack(fill="x", padx=10, pady=10)

            ttk.Label(dev_top, text=self.t("setup.devices.hint"), justify="left", wraplength=960).pack(anchor="w")

            # Demo mode (for users without Shellys)
            self._wiz_demo_var = tk.BooleanVar(value=bool(getattr(getattr(self.cfg, 'demo', None), 'enabled', False)))
            ttk.Checkbutton(dev_top, text='Demo mode (fake but realistic data)', variable=self._wiz_demo_var, command=self._wizard_toggle_demo).pack(anchor='w', pady=(6, 0))

            btn_row = ttk.Frame(step_devices)
            btn_row.pack(fill="x", padx=10, pady=(0, 6))

            self._wiz_status_var = tk.StringVar(value=self.t("setup.devices.status.idle"))
            ttk.Label(btn_row, textvariable=self._wiz_status_var).pack(side="left")

            ttk.Button(btn_row, text=self.t("setup.devices.btn.mdns"), command=self._wizard_discover_mdns).pack(side="right", padx=(6, 0))
            ttk.Button(btn_row, text=self.t("setup.devices.btn.ipscan"), command=self._wizard_scan_ip).pack(side="right")

            # Manual host/IP add (optional)
            man_row = ttk.Frame(step_devices)
            man_row.pack(fill="x", padx=10, pady=(0, 6))
            ttk.Label(man_row, text=self.t("setup.devices.manual.label")).pack(side="left")
            self._wiz_manual_hosts = tk.StringVar(value="")
            ent = ttk.Entry(man_row, textvariable=self._wiz_manual_hosts)
            ent.pack(side="left", fill="x", expand=True, padx=(6, 6))
            ttk.Button(man_row, text=self.t("setup.devices.manual.btn.add"), command=self._wizard_add_manual_hosts).pack(side="right")

            # List of found devices
            tree_box = ttk.Frame(step_devices)
            tree_box.pack(fill="both", expand=True, padx=10, pady=(0, 10))

            cols = ("host", "model", "kind", "gen")
            tv = ttk.Treeview(tree_box, columns=cols, show="headings", selectmode="extended")
            self._wiz_tv = tv
            # If demo mode is already enabled (config.json), pre-fill the list.
            try:
                if bool(getattr(self, '_wiz_demo_var', None).get()):
                    self._wizard_toggle_demo()
            except Exception:
                pass
            for c, w in [("host", 200), ("model", 260), ("kind", 120), ("gen", 70)]:
                tv.heading(c, text=self.t(f"setup.devices.col.{c}"))
                tv.column(c, width=w, anchor="w")
            tv.pack(side="left", fill="both", expand=True)
            sb = ttk.Scrollbar(tree_box, orient="vertical", command=tv.yview)
            sb.pack(side="right", fill="y")
            tv.configure(yscrollcommand=sb.set)

            # Add selected
            add_row = ttk.Frame(step_devices)
            add_row.pack(fill="x", padx=10, pady=(0, 10))
            ttk.Button(add_row, text=self.t("setup.devices.btn.add"), command=self._wizard_add_selected).pack(side="right")
            self._wiz_added_var = tk.StringVar(value="")
            ttk.Label(add_row, textvariable=self._wiz_added_var).pack(side="left")

            # ---- Step 2: Telegram (optional) ----
            tg = ttk.Frame(step_telegram)
            tg.pack(fill="both", expand=True, padx=12, pady=12)

            ttk.Label(tg, text=self.t("setup.telegram.hint"), justify="left", wraplength=980).pack(anchor="w", pady=(0, 10))

            self._wiz_tg_enabled = tk.BooleanVar(value=bool(getattr(self.cfg.ui, "telegram_enabled", False)))
            ttk.Checkbutton(tg, text=self.t("setup.telegram.enable"), variable=self._wiz_tg_enabled).pack(anchor="w")

            form = ttk.Frame(tg)
            form.pack(fill="x", pady=(10, 0))

            ttk.Label(form, text=self.t("settings.telegram_bot_token")).grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
            self._wiz_tg_token = tk.StringVar(value=str(getattr(self.cfg.ui, "telegram_bot_token", "") or ""))
            ttk.Entry(form, textvariable=self._wiz_tg_token, width=52).grid(row=0, column=1, sticky="w", pady=4)

            ttk.Label(form, text=self.t("settings.telegram_chat_id")).grid(row=1, column=0, sticky="w", padx=(0, 8), pady=4)
            self._wiz_tg_chat = tk.StringVar(value=str(getattr(self.cfg.ui, "telegram_chat_id", "") or ""))
            ttk.Entry(form, textvariable=self._wiz_tg_chat, width=52).grid(row=1, column=1, sticky="w", pady=4)

            def _save_tg() -> None:
                try:
                    self.cfg.ui.telegram_enabled = bool(self._wiz_tg_enabled.get())
                    self.cfg.ui.telegram_bot_token = str(self._wiz_tg_token.get() or "").strip()
                    self.cfg.ui.telegram_chat_id = str(self._wiz_tg_chat.get() or "").strip()
                    save_config(self.cfg, self.cfg_path)
                except Exception:
                    pass

            ttk.Button(tg, text=self.t("setup.telegram.save"), command=_save_tg).pack(anchor="e", pady=(10, 0))

            # ---- Step 3: Finish ----
            fin = ttk.Frame(step_finish)
            fin.pack(fill="both", expand=True, padx=12, pady=12)
            ttk.Label(fin, text=self.t("setup.finish.hint"), justify="left", wraplength=980).pack(anchor="w", pady=(0, 10))
            self._wiz_finish_summary = tk.StringVar(value="")
            ttk.Label(fin, textvariable=self._wiz_finish_summary, justify="left", wraplength=980).pack(anchor="w")

            # Navigation buttons
            nav = ttk.Frame(root)
            nav.pack(fill="x", pady=(10, 0))

            self._wiz_back_btn = ttk.Button(nav, text=self.t("setup.btn.back"), command=lambda: self._wizard_step(-1))
            self._wiz_back_btn.pack(side="left")

            self._wiz_next_btn = ttk.Button(nav, text=self.t("setup.btn.next"), command=lambda: self._wizard_step(+1))
            self._wiz_next_btn.pack(side="right")

            self._wiz_finish_btn = ttk.Button(nav, text=self.t("setup.btn.finish"), command=self._wizard_finish)
            # Only show on last page
            # We'll pack/unpack dynamically in _wizard_refresh_nav()

            self._wizard_refresh_nav()

    def _wizard_step(self, delta: int) -> None:
            nb = getattr(self, "_wiz_nb", None)
            if nb is None:
                return

            # Current index
            try:
                cur = int(nb.index("current"))
            except Exception:
                cur = 0

            # If we are leaving the device step forward and nothing was added yet,
            # auto-add the current selection from the found-device list (Treeview).
            if int(delta) > 0 and cur == 0:
                try:
                    has_devices = bool(getattr(self.cfg, "devices", []) or [])
                except Exception:
                    has_devices = False

                if not has_devices:
                    try:
                        tv = getattr(self, "_wiz_tv", None)
                        if tv is not None:
                            # If nothing is selected but we discovered devices, auto-add all found.
                            try:
                                sel = list(tv.selection() or [])
                            except Exception:
                                sel = []
                            if not sel:
                                try:
                                    children = list(tv.get_children("") or [])
                                    if children:
                                        tv.selection_set(children)
                                        sel = list(tv.selection() or [])
                                except Exception:
                                    sel = []
                            if sel:
                                self._wizard_add_selected()
                                has_devices = bool(getattr(self.cfg, "devices", []) or [])
                    except Exception:
                        pass

                # Still nothing? Stay on step 1.
                if not has_devices:
                    self._wizard_refresh_nav()
                    return

            nxt = max(0, min(2, cur + int(delta)))

            # Select by tab-id (more robust than numeric indices across Tk variants)
            try:
                tabs = list(nb.tabs())
                if 0 <= nxt < len(tabs):
                    nb.select(tabs[nxt])
            except Exception:
                # fallback
                try:
                    nb.select(nxt)
                except Exception:
                    pass

            self._wizard_refresh_nav()

    def _wizard_refresh_nav(self) -> None:
            nb = getattr(self, "_wiz_nb", None)
            if nb is None:
                return
            try:
                cur = nb.index("current")
            except Exception:
                cur = 0

            # Back disabled on first step
            try:
                self._wiz_back_btn.configure(state=("disabled" if cur <= 0 else "normal"))
            except Exception:
                pass

            # Next disabled if no devices on step 1
            if cur == 0:
                # Allow Next when at least one device is already configured OR the user selected
                # one or more discovered devices (we will auto-add on Next).
                has_devices = bool(getattr(self.cfg, "devices", []) or [])
                if not has_devices:
                    try:
                        tv = getattr(self, "_wiz_tv", None)
                        has_devices = bool(tv is not None and tv.selection())
                    except Exception:
                        has_devices = False
                try:
                    self._wiz_next_btn.configure(state=("normal" if has_devices else "disabled"))
                except Exception:
                    pass
            else:
                try:
                    self._wiz_next_btn.configure(state="normal")
                except Exception:
                    pass

            # Finish button only on last page
            try:
                if cur >= 2:
                    try:
                        self._wiz_next_btn.pack_forget()
                    except Exception:
                        pass
                    if not getattr(self, "_wiz_finish_btn_packed", False):
                        self._wiz_finish_btn.pack(side="right")
                        self._wiz_finish_btn_packed = True
                else:
                    if getattr(self, "_wiz_finish_btn_packed", False):
                        try:
                            self._wiz_finish_btn.pack_forget()
                        except Exception:
                            pass
                        self._wiz_finish_btn_packed = False
                    # Ensure next is visible
                    if not self._wiz_next_btn.winfo_ismapped():
                        self._wiz_next_btn.pack(side="right")
            except Exception:
                pass

            # Update finish summary
            try:
                if cur >= 2:
                    n = len(getattr(self.cfg, "devices", []) or [])
                    self._wiz_finish_summary.set(self.t("setup.finish.summary", n=n))
            except Exception:
                pass

    def _wizard_discover_mdns(self) -> None:
            """Discover Shelly devices via mDNS, then probe them for details."""
            if getattr(self, "_wiz_scan_running", False):
                return
            self._wiz_scan_cancel = False

            def _worker():
                hosts = []
                err = None
                try:
                    md = discover_shelly_mdns(timeout_seconds=3.5)
                    for x in md:
                        h = str(getattr(x, "host", "") or "").strip()
                        if h and h not in hosts:
                            hosts.append(h)
                except Exception as e:
                    err = str(e)

                def _ui_start():
                    if err:
                        self._wiz_status_var.set(self.t("setup.devices.status.mdns_err", err=err))
                    else:
                        self._wiz_status_var.set(self.t("setup.devices.status.mdns", n=len(hosts)))
                try:
                    self.after(0, _ui_start)
                except Exception:
                    _ui_start()

                for h in hosts:
                    if self._wiz_scan_cancel:
                        break
                    try:
                        disc = probe_device(h, timeout_seconds=1.2)
                    except Exception:
                        continue
                    try:
                        self.after(0, lambda d=disc: self._wizard_add_found(d))
                    except Exception:
                        self._wizard_add_found(disc)

                def _done():
                    self._wiz_scan_running = False
                    self._wiz_status_var.set(self.t("setup.devices.status.done", n=len(getattr(self, "_wiz_found", {}) or {})))
                try:
                    self.after(0, _done)
                except Exception:
                    _done()

            try:
                self._wiz_scan_running = True
                threading.Thread(target=_worker, daemon=True).start()
            except Exception:
                self._wiz_scan_running = False

    def _local_subnet_prefix(self) -> str:
            """Best-effort local /24 prefix (e.g. '192.168.1').

            Used by the setup wizard IP scan. Falls back to '192.168.1' if
            the local address cannot be determined.
            """
            try:
                import socket
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                try:
                    # No packets are actually sent; this is a common trick to learn the
                    # preferred outbound interface IP.
                    s.connect(("8.8.8.8", 80))
                    ip = s.getsockname()[0]
                finally:
                    try:
                        s.close()
                    except Exception:
                        pass
                parts = str(ip).split(".")
                if len(parts) == 4 and all(p.isdigit() for p in parts[:3]):
                    return ".".join(parts[:3])
            except Exception:
                pass
            return "192.168.1"

    def _wizard_scan_ip(self) -> None:
            """Quick /24 scan by probing hosts with Shelly APIs (strict detection)."""
            if getattr(self, "_wiz_scan_running", False):
                return
            self._wiz_scan_cancel = False

            prefix = self._local_subnet_prefix()
            hosts = [f"{prefix}.{i}" for i in range(1, 255)]

            def _worker():
                try:
                    import concurrent.futures

                    def _ui_start():
                        self._wiz_status_var.set(self.t("setup.devices.status.ipscan", prefix=prefix))
                    try:
                        self.after(0, _ui_start)
                    except Exception:
                        _ui_start()

                    def _probe(h: str):
                        if self._wiz_scan_cancel:
                            return None
                        try:
                            return probe_device(h, timeout_seconds=0.8)
                        except Exception:
                            return None

                    max_workers = 64
                    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
                        futs = {ex.submit(_probe, h): h for h in hosts}
                        for fut in concurrent.futures.as_completed(futs):
                            if self._wiz_scan_cancel:
                                break
                            disc = fut.result()
                            if disc is None:
                                continue
                            try:
                                self.after(0, lambda d=disc: self._wizard_add_found(d))
                            except Exception:
                                self._wizard_add_found(disc)
                except Exception:
                    pass

                def _done():
                    self._wiz_scan_running = False
                    self._wiz_status_var.set(self.t("setup.devices.status.done", n=len(getattr(self, "_wiz_found", {}) or {})))
                try:
                    self.after(0, _done)
                except Exception:
                    _done()

            try:
                self._wiz_scan_running = True
                threading.Thread(target=_worker, daemon=True).start()
            except Exception:
                self._wiz_scan_running = False

    def _wizard_add_manual_hosts(self) -> None:
            """Manually add one or multiple Shelly hosts/IPs (comma/space separated)."""
            if getattr(self, "_wiz_scan_running", False):
                return
            raw = ""
            try:
                raw = str(getattr(self, "_wiz_manual_hosts", None).get() or "")
            except Exception:
                raw = ""
            raw = raw.strip()
            if not raw:
                return

            parts = [p.strip() for p in re.split(r"[\s,;]+", raw) if p.strip()]
            if not parts:
                return

            self._wiz_scan_cancel = False

            def _worker():
                bad = []
                for h in parts:
                    if self._wiz_scan_cancel:
                        break
                    try:
                        def _ui():
                            self._wiz_status_var.set(self.t("setup.devices.status.manual", host=h))
                        self.after(0, _ui)
                    except Exception:
                        pass

                    try:
                        disc = probe_device(h, timeout_seconds=1.5)
                    except Exception:
                        bad.append(h)
                        continue

                    try:
                        self.after(0, lambda d=disc: self._wizard_add_found(d))
                    except Exception:
                        self._wizard_add_found(disc)

                def _done():
                    self._wiz_scan_running = False
                    if bad:
                        self._wiz_status_var.set(self.t("setup.devices.status.not_shelly", host=", ".join(bad[:4]) + ("..." if len(bad) > 4 else "")))
                    else:
                        self._wiz_status_var.set(self.t("setup.devices.status.done", n=len(getattr(self, "_wiz_found", {}) or {})))
                try:
                    self.after(0, _done)
                except Exception:
                    _done()

            try:
                self._wiz_scan_running = True
                threading.Thread(target=_worker, daemon=True).start()
            except Exception:
                self._wiz_scan_running = False

    def _wizard_toggle_demo(self) -> None:
            """Enable/disable Demo Mode (no Shellys needed)."""
            try:
                enabled = bool(getattr(self, '_wiz_demo_var', None).get())
            except Exception:
                enabled = False

            if enabled:
                # Create demo devices and persist them into config.json immediately
                try:
                    devs = default_demo_devices()
                    object.__setattr__(self.cfg, 'devices', devs)
                except Exception:
                    try:
                        self.cfg.devices[:] = default_demo_devices()
                    except Exception:
                        pass
                try:
                    from shelly_analyzer.io.config import DemoConfig
                    object.__setattr__(self.cfg, 'demo', DemoConfig(enabled=True, seed=1234, scenario='household'))
                except Exception:
                    pass
                try:
                    save_config(self.cfg, self.cfg_path)
                except Exception:
                    pass

                # Populate the discovery list with demo devices so the UI stays consistent
                try:
                    tv = getattr(self, '_wiz_tv', None)
                    if tv is not None:
                        for iid in list(tv.get_children('') or []):
                            tv.delete(iid)
                        self._wiz_found = {}
                        for d in (getattr(self.cfg, 'devices', []) or []):
                            disc = {
                                'host': d.host,
                                'model': getattr(d, 'model', ''),
                                'kind': getattr(d, 'kind', 'em'),
                                'gen': getattr(d, 'gen', 2),
                                'em_id': getattr(d, 'em_id', 0),
                                'phases': getattr(d, 'phases', 1),
                                'supports_emdata': getattr(d, 'supports_emdata', True),
                                'name': getattr(d, 'name', d.key),
                                'key': d.key,
                            }
                            self._wiz_found[str(d.host)] = disc
                            tv.insert('', 'end', values=(disc['host'], disc['model'], disc['kind'], disc['gen']))
                        try:
                            children = list(tv.get_children('') or [])
                            if children:
                                tv.selection_set(children)
                        except Exception:
                            pass
                except Exception:
                    pass

                try:
                    self._wiz_status_var.set('Demo mode enabled.')
                except Exception:
                    pass
            else:
                # Disable demo mode (keep devices; user can scan/add real devices).
                try:
                    from shelly_analyzer.io.config import DemoConfig
                    object.__setattr__(self.cfg, 'demo', DemoConfig(enabled=False, seed=1234, scenario='household'))
                    save_config(self.cfg, self.cfg_path)
                except Exception:
                    pass
                try:
                    self._wiz_status_var.set(self.t('setup.devices.status.idle'))
                except Exception:
                    pass

            try:
                self._wizard_refresh_nav()
            except Exception:
                pass

    def _wizard_add_found(self, disc) -> None:
            try:
                host = str(getattr(disc, "host", "") or "").strip()
                if not host:
                    return
                found = getattr(self, "_wiz_found", None)
                if found is None:
                    self._wiz_found = {}
                    found = self._wiz_found
                if host in found:
                    return
                found[host] = {
                    "host": host,
                    "model": str(getattr(disc, "model", "") or ""),
                    "kind": str(getattr(disc, "kind", "") or "unknown"),
                    "gen": int(getattr(disc, "gen", 0) or 0),
                    "component_id": int(getattr(disc, "component_id", 0) or 0),
                    "phases": int(getattr(disc, "phases", 1) or 1),
                    "supports_emdata": bool(getattr(disc, "supports_emdata", False)),
                }
            except Exception:
                return

            # Update treeview in UI thread
            def _ui():
                try:
                    tv = getattr(self, "_wiz_tv", None)
                    if tv is None:
                        return
                    # Insert
                    model = found[host]["model"]
                    kind = found[host]["kind"]
                    gen = str(found[host]["gen"])
                    tv.insert("", "end", iid=host, values=(host, model, kind, gen))
                    self._wiz_status_var.set(self.t("setup.devices.status.found", n=len(found)))
                except Exception:
                    pass

            try:
                self.after(0, _ui)
            except Exception:
                _ui()

    def _wizard_add_selected(self) -> None:
            tv = getattr(self, "_wiz_tv", None)
            if tv is None:
                return
            try:
                sel = list(tv.selection() or [])
            except Exception:
                sel = []
            if not sel:
                try:
                    self._wiz_added_var.set(self.t("setup.devices.add.none"))
                except Exception:
                    pass
                return

            existing = {str(d.host): d for d in (getattr(self.cfg, "devices", []) or [])}
            devs = list(getattr(self.cfg, "devices", []) or [])
            added = 0
            # generate keys
            used_keys = {d.key for d in devs}
            def _next_key() -> str:
                i = 1
                while True:
                    k = f"shelly{i}"
                    if k not in used_keys:
                        used_keys.add(k)
                        return k
                    i += 1

            found = getattr(self, "_wiz_found", {}) or {}
            for host in sel:
                if host in existing:
                    continue
                info = found.get(host) or {}
                key = _next_key()
                name = (info.get("model") or "").strip() or f"Shelly {host}"
                try:
                    devs.append(DeviceConfig(
                        key=key,
                        name=name,
                        host=host,
                        em_id=int(info.get("component_id") or 0),
                        kind=str(info.get("kind") or "unknown"),
                        gen=int(info.get("gen") or 0),
                        model=str(info.get("model") or ""),
                        phases=int(info.get("phases") or 1),
                        supports_emdata=bool(info.get("supports_emdata", False)),
                    ))
                    added += 1
                except Exception:
                    pass

            if added <= 0:
                try:
                    self._wiz_added_var.set(self.t("setup.devices.add.none"))
                except Exception:
                    pass
                return

            try:
                # AppConfig is frozen; mutate the devices list in-place.
                if isinstance(getattr(self.cfg, "devices", None), list):
                    self.cfg.devices[:] = devs
                save_config(self.cfg, self.cfg_path)
            except Exception:
                pass

            # Update status + enable next
            try:
                self._wiz_added_var.set(self.t("setup.devices.add.ok", n=added))
            except Exception:
                pass

            # Update device paging combobox + settings devices list
            try:
                self._update_device_page_choices()
            except Exception:
                pass

            # Also refresh Settings tab if already built
            try:
                self._clear_frame(self.tab_settings)
                self._build_settings_tab()
            except Exception:
                pass

            self._wizard_refresh_nav()

            # Best-effort probe to fill model/kind fields
            try:
                self._probe_devices_on_startup()
            except Exception:
                pass

    def _wizard_finish(self) -> None:
            """Finalize setup: enable tabs, build them and jump to Settings->Devices."""
            # Save Telegram values from wizard
            try:
                self.cfg.ui.telegram_enabled = bool(getattr(self, "_wiz_tg_enabled", tk.BooleanVar(value=False)).get())
                self.cfg.ui.telegram_bot_token = str(getattr(self, "_wiz_tg_token", tk.StringVar(value="")).get() or "").strip()
                self.cfg.ui.telegram_chat_id = str(getattr(self, "_wiz_tg_chat", tk.StringVar(value="")).get() or "").strip()
                save_config(self.cfg, self.cfg_path)
            except Exception:
                pass


            # Best effort: if user did not explicitly add devices yet, but some are discovered,
            # auto-add selected items (or all discovered) before validating.
            try:
                if not (getattr(self.cfg, "devices", []) or []):
                    tv = getattr(self, "_wiz_tv", None)
                    if tv is not None:
                        try:
                            sel = list(tv.selection() or [])
                        except Exception:
                            sel = []
                        if not sel:
                            try:
                                children = list(tv.get_children("") or [])
                                if children:
                                    tv.selection_set(children)
                            except Exception:
                                pass
                        try:
                            if tv.selection():
                                self._wizard_add_selected()
                        except Exception:
                            pass
            except Exception:
                pass
            # Require at least one device
            if not (getattr(self.cfg, "devices", []) or []):
                try:
                    messagebox.showwarning(self.t("setup.title"), self.t("setup.finish.need_device"))
                except Exception:
                    pass
                return

            # Enable tabs
            try:
                for tab in (self.tab_sync, self.tab_plots, self.tab_live, self.tab_costs, self.tab_heatmap, self.tab_compare, self.tab_export):
                    try:
                        self.notebook.tab(tab, state="normal")
                    except Exception:
                        pass
            except Exception:
                pass

            # Build main tabs (only once)
            if not bool(getattr(self, "_tabs_built", False)):
                try:
                    self._clear_frame(self.tab_sync); self._build_sync_tab()
                except Exception:
                    pass
                try:
                    self._clear_frame(self.tab_plots); self._build_plots_tab()
                except Exception:
                    pass
                try:
                    self._clear_frame(self.tab_live); self._build_live_tab()
                except Exception:
                    pass
                try:
                    self._clear_frame(self.tab_costs); self._build_costs_tab()
                except Exception:
                    pass
                try:
                    self._clear_frame(self.tab_heatmap); self._build_heatmap_tab()
                except Exception:
                    pass
                try:
                    self._clear_frame(self.tab_compare); self._build_compare_tab()
                except Exception:
                    pass
                try:
                    self._clear_frame(self.tab_export); self._build_export_tab()
                except Exception:
                    pass
                self._tabs_built = True

            # Jump to Settings -> Devices for review
            try:
                self.notebook.select(self.tab_settings)
                if hasattr(self, "_settings_nb") and hasattr(self, "_settings_tab_devices"):
                    try:
                        self._settings_nb.select(self._settings_tab_devices)
                    except Exception:
                        pass
            except Exception:
                pass

            # Remove the setup tab to avoid clutter (user can always edit in Settings)
            try:
                if hasattr(self, "tab_setup"):
                    self.notebook.forget(self.tab_setup)
            except Exception:
                pass

            # Setup is complete
            self._setup_required = False

            # Update combobox / paging
            try:
                self._update_device_page_choices()
            except Exception:
                pass