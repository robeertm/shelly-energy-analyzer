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
            self._upd_latest: Optional[ReleaseInfo] = None
            self.sync_log = None  # will be a tk.Text once the Sync tab is built
            self._sync_log_buffer: List[str] = []
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
            self.tab_export = ttk.Frame(self.notebook)
            self.tab_settings = ttk.Frame(self.notebook)
            # Notebook tab labels are translated based on the selected UI language.
            self.notebook.add(self.tab_sync, text=self.t("tabs.sync"))
            self.notebook.add(self.tab_plots, text=self.t("tabs.plots"))
            self.notebook.add(self.tab_live, text=self.t("tabs.live"))
            self.notebook.add(self.tab_export, text=self.t("tabs.export"))
            self.notebook.add(self.tab_settings, text=self.t("tabs.settings"))

            # First-run / no-devices mode: show a guided Setup wizard and keep other tabs disabled
            if bool(getattr(self, "_setup_required", False)):
                self.tab_setup = ttk.Frame(self.notebook)
                # Insert setup as the first tab for guidance
                self.notebook.insert(0, self.tab_setup, text=self.t("tabs.setup"))

                # Put placeholders into disabled tabs (avoid CSV warnings on first run)
                for tab in (self.tab_sync, self.tab_plots, self.tab_live, self.tab_export):
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
                self._build_export_tab()
                self._build_settings_tab()
                self._tabs_built = True

            self._page_labels = []
            self._update_device_page_choices()

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
            _dn_mode0 = str(getattr(self.cfg.ui, "live_daynight_mode", "all") or "all").strip().lower()
            if _dn_mode0 == "day":
                _dn_label0 = self.t('live.day')
            elif _dn_mode0 == "night":
                _dn_label0 = self.t('live.night')
            else:
                _dn_label0 = self.t('live.all')
            self.live_daynight_ctl = tk.StringVar(value=_dn_label0)

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

            ttk.Label(ctl, text=self.t('live.daynight')).grid(row=0, column=9, padx=(0, 4))
            cb_dn = ttk.Combobox(ctl, width=6, state="readonly", textvariable=self.live_daynight_ctl,
                                values=[self.t('live.all'), self.t('live.day'), self.t('live.night')])
            cb_dn.grid(row=0, column=10, padx=(0, 10))

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
                try:
                    self.live_daynight_ctl.trace_add('write', lambda *_a: self._schedule_apply_live_controls())
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

            # Day/Night mode (UI theme) - stored internally as: all|day|night
            try:
                _dn_label = str(self.live_daynight_ctl.get() or "").strip()
            except Exception:
                _dn_label = ""
            try:
                if _dn_label == self.t('live.day'):
                    dn_mode = "day"
                elif _dn_label == self.t('live.night'):
                    dn_mode = "night"
                else:
                    dn_mode = "all"
            except Exception:
                dn_mode = "all"


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
            changed_dn = str(getattr(old_ui, 'live_daynight_mode', 'all')) != str(dn_mode)

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
                    live_daynight_mode=dn_mode,
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

    def _build_settings_tab(self) -> None:
            frm = self.tab_settings
            # Split settings into subtabs to avoid overly tall pages (esp. on 14" screens).
            nb = ttk.Notebook(frm)
            nb.pack(fill="both", expand=True, padx=12, pady=10)

            # Keep references so we can focus the Devices settings on first run.
            self._settings_nb = nb

            tab_devices = ttk.Frame(nb)
            self._settings_tab_devices = tab_devices
            tab_main = ttk.Frame(nb)
            tab_advanced = ttk.Frame(nb)
            tab_expert = ttk.Frame(nb)
            tab_billing = ttk.Frame(nb)
            tab_updates = ttk.Frame(nb)
            nb.add(tab_devices, text=self.t('settings.devices'))
            nb.add(tab_main, text=self.t('settings.main'))
            nb.add(tab_advanced, text=self.t('settings.advanced'))
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

        
            # Updates settings (persist auto-install checkbox)
            try:
                updates = replace(getattr(self.cfg, "updates", UpdatesConfig()), auto_install=bool(self.upd_auto.get()))
            except Exception:
                updates = UpdatesConfig(auto_install=bool(self.upd_auto.get()))
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
                for tab in (self.tab_sync, self.tab_plots, self.tab_live, self.tab_export):
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
