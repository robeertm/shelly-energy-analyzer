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

class ScalingMixin:
    """Auto-generated mixin extracted from the former ui/app.py to keep files smaller."""

    def _screen_px(self) -> Tuple[int, int]:
            try:
                return int(self.winfo_screenwidth()), int(self.winfo_screenheight())
            except Exception:
                return 1440, 900

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
