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

class PlotsMixin:
    """Auto-generated mixin extracted from the former ui/app.py to keep files smaller."""

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
            # forward=False: we fit the figure INTO the existing Tk canvas widget
            # (not the other way around).  forward=True would tell matplotlib to
            # resize the canvas widget to match the figure, which triggers a new
            # <Configure> event → _on_plots_canvas_configure → resize again →
            # infinite feedback loop visible as 1-second size jitter.
            fig.set_size_inches(w / float(dpi), h / float(dpi), forward=False)

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
                    bottom = 0.38
                elif h and h < 340:
                    bottom = 0.28
                elif h and h < 500:
                    bottom = 0.22
                else:
                    bottom = 0.18

                # Rotation needs extra room
                bottom += (rot / 90.0) * 0.05

                # Add a little extra room based on font size.
                bottom = min(0.45, max(0.14, bottom + max(0, base - 6) * 0.008))
                # Keep a little extra room on the right; prevents edge clipping on
                # some platforms when the canvas is resized rapidly.
                fig.subplots_adjust(left=0.09, right=0.97, top=0.94, bottom=bottom)
            except Exception:
                pass

    def _rebuild_plots_tab(self) -> None:
            # Preserve current metric/device selection when rebuilding the tab UI.
            state = {"metric_idx": 0, "last_device_idx": int(getattr(self, "_plots_last_device_idx", 0))}
            try:
                nb = getattr(self, "_plots_metric_nb", None)
                if nb is not None and nb.winfo_exists():
                    state["metric_idx"] = int(nb.index("current"))
            except Exception:
                pass
            self._clear_frame(self.tab_plots)
            self._build_plots_tab()
            # Restore metric tab
            try:
                nb2 = getattr(self, "_plots_metric_nb", None)
                if nb2 is not None and nb2.winfo_exists():
                    tabs2 = list(nb2.tabs())
                    midx = int(state.get("metric_idx", 0))
                    if tabs2:
                        if midx < 0:
                            midx = 0
                        if midx >= len(tabs2):
                            midx = len(tabs2) - 1
                        nb2.select(midx)
            except Exception:
                pass
            # Restore last device idx across metrics
            try:
                self._plots_last_device_idx = int(state.get("last_device_idx", 0))
            except Exception:
                pass

    # ------------------------------------------------------------------
    #  Global plot theme helpers (used by Live + History plots)
    # ------------------------------------------------------------------

    @staticmethod
    def _system_is_dark() -> bool:
            """Detect OS-level dark mode (macOS / Windows / Linux)."""
            import sys
            try:
                if sys.platform == "darwin":
                    import subprocess
                    r = subprocess.run(
                        ["defaults", "read", "-g", "AppleInterfaceStyle"],
                        capture_output=True, text=True, timeout=2,
                    )
                    return r.stdout.strip().lower() == "dark"
                elif sys.platform == "win32":
                    import winreg
                    key = winreg.OpenKey(
                        winreg.HKEY_CURRENT_USER,
                        r"Software\Microsoft\Windows\CurrentVersion\Themes\Personalize",
                    )
                    val, _ = winreg.QueryValueEx(key, "AppsUseLightTheme")
                    winreg.CloseKey(key)
                    return val == 0
                else:
                    import subprocess
                    r = subprocess.run(
                        ["gsettings", "get", "org.gnome.desktop.interface", "color-scheme"],
                        capture_output=True, text=True, timeout=2,
                    )
                    return "dark" in r.stdout.strip().lower()
            except Exception:
                return False

    def _resolve_plot_theme(self) -> str:
            """Return 'day' or 'night' based on global config (plot_theme_mode).

            Supports: auto (system dark-mode detection with time-of-day fallback),
            day, night.
            """
            try:
                pref = str(getattr(self.cfg.ui, "plot_theme_mode", "auto") or "auto").strip().lower()
            except Exception:
                pref = "auto"
            if pref not in ("auto", "day", "night"):
                pref = "auto"

            if pref == "auto":
                try:
                    return "night" if self._system_is_dark() else "day"
                except Exception:
                    # Fallback: time-based
                    try:
                        from datetime import datetime as _dt
                        h = _dt.now().hour
                        return "day" if 6 <= h < 22 else "night"
                    except Exception:
                        return "day"
            return pref

    def _apply_plot_theme(self, fig, ax, canvas=None):
            """Apply the current global theme (day/night) to a matplotlib figure+axis."""
            try:
                theme = self._resolve_plot_theme()
                if theme == "night":
                    bg, fg, grid = "#111111", "#E6E6E6", "#AAAAAA"
                else:
                    bg, fg, grid = "#FFFFFF", "#000000", "#BBBBBB"

                # Set the Tk canvas widget background to match the figure
                # background so no white stripes appear at the edges.
                try:
                    if canvas is not None:
                        canvas.get_tk_widget().configure(bg=bg)
                except Exception:
                    pass
                try:
                    fig.patch.set_facecolor(bg)
                except Exception:
                    pass
                try:
                    ax.set_facecolor(bg)
                except Exception:
                    pass
                try:
                    ax.tick_params(axis="both", colors=fg)
                except Exception:
                    pass
                try:
                    ax.xaxis.label.set_color(fg)
                    ax.yaxis.label.set_color(fg)
                except Exception:
                    pass
                try:
                    if ax.title:
                        ax.title.set_color(fg)
                except Exception:
                    pass
                try:
                    for spine in ax.spines.values():
                        spine.set_color(fg)
                except Exception:
                    pass
                try:
                    ax.grid(True, axis="y", alpha=0.4 if theme == "night" else 0.25)
                    for gl in ax.get_ygridlines():
                        gl.set_color(grid)
                except Exception:
                    pass
                try:
                    leg = ax.get_legend()
                    if leg is not None:
                        try:
                            leg.get_frame().set_facecolor(bg)
                            leg.get_frame().set_edgecolor(grid)
                        except Exception:
                            pass
                        try:
                            for txt in leg.get_texts():
                                try:
                                    txt.set_color(fg)
                                except Exception:
                                    pass
                        except Exception:
                            pass
                except Exception:
                    pass
                try:
                    for txt in getattr(ax, "texts", []) or []:
                        try:
                            txt.set_color(fg)
                        except Exception:
                            pass
                except Exception:
                    pass
            except Exception:
                pass

    def _get_theme_colors(self) -> dict:
            """Return a dict of theme-appropriate colors for charts."""
            theme = self._resolve_plot_theme()
            if theme == "night":
                return {
                    "bg": "#111111", "fg": "#E6E6E6", "grid": "#AAAAAA",
                    "blue": "#5dade2", "red": "#ec7063", "green": "#58d68d",
                    "orange": "#f0b27a", "purple": "#bb8fce", "muted": "#888888",
                }
            return {
                "bg": "#FFFFFF", "fg": "#000000", "grid": "#BBBBBB",
                "blue": "#3498db", "red": "#e74c3c", "green": "#27ae60",
                "orange": "#f39c12", "purple": "#8e44ad", "muted": "#888888",
            }

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

            # Keep device selection stable when switching between metric tabs.
            # We remember the last selected device-tab index and apply it across all metric tabs
            # to avoid the UI feeling like parameters "jump around".
            self._plots_metric_key_order = ["kwh", "V", "A", "W", "VAR", "COSPHI", "HZ", "CO2", "DYNPRICE"]
            if not hasattr(self, "_plots_last_device_idx"):
                self._plots_last_device_idx = 0
            self._plots_syncing_tabs = False

            def _on_metric_tab_changed(_evt: Any = None) -> None:
                if getattr(self, "_plots_syncing_tabs", False):
                    return
                try:
                    midx = int(nb.index("current"))
                except Exception:
                    midx = 0
                metric_key = (
                    self._plots_metric_key_order[midx]
                    if 0 <= midx < len(self._plots_metric_key_order)
                    else "kwh"
                )
                dev_nb = self._plots_device_nb.get(metric_key)
                if dev_nb is None:
                    return

                # Remember the selected device-tab index for this metric.
                try:
                    self._plots_last_device_idx = int(dev_nb.index("current"))
                except Exception:
                    return

                # Apply the same device-tab index to all metric notebooks (if that tab exists).
                try:
                    self._plots_syncing_tabs = True
                    for _k, _nb in self._plots_device_nb.items():
                        if _nb is None:
                            continue
                        try:
                            target = self._plots_last_device_idx
                            if 0 <= target < _nb.index("end"):
                                _nb.select(target)
                        except Exception:
                            pass
                finally:
                    self._plots_syncing_tabs = False

            try:
                nb.bind("<<NotebookTabChanged>>", _on_metric_tab_changed)
            except Exception:
                pass

            # Reset plot objects
            self._plots_figs2 = {}
            self._plots_axes2 = {}
            self._plots_canvases2 = {}
            self._plots_device_nb = {}
            self._plots_device_order = {}

            # ── Single shared control bar above the metric notebook ──
            _gran_map = {
                "all": self.t("plots.mode.all"),
                "hours": self.t("plots.mode.hours"),
                "days": self.t("plots.mode.days"),
                "weeks": self.t("plots.mode.weeks"),
                "months": self.t("plots.mode.months"),
            }
            _gran_inv = {v: k for k, v in _gran_map.items()}

            ctrl_row = ttk.Frame(frm)
            ctrl_row.pack(fill="x", padx=12, pady=(0, 6))
            ttk.Label(ctrl_row, text=self.t("plots.kwh.granularity")).pack(side="left")

            self._plots_gran_display = tk.StringVar(value=_gran_map.get("days", ""))
            self._plots_last_n_shared = tk.StringVar(value="7")
            gran_cb = ttk.Combobox(
                ctrl_row, state="readonly", width=12,
                textvariable=self._plots_gran_display, values=list(_gran_map.values()),
            )
            gran_cb.pack(side="left", padx=(10, 0))
            ttk.Label(ctrl_row, text=self.t("plots.kwh.last")).pack(side="left", padx=(16, 4))
            sp_n = ttk.Spinbox(ctrl_row, from_=1, to=9999, width=5, textvariable=self._plots_last_n_shared)
            sp_n.pack(side="left")

            def _apply_shared_controls(_e=None) -> None:
                """Apply the shared granularity + last-N to the active metric tab."""
                disp = str(self._plots_gran_display.get() or "").strip()
                unit = _gran_inv.get(disp, "all")
                try:
                    n = int(self._plots_last_n_shared.get() or 7)
                except Exception:
                    n = 7
                n = max(1, min(9999, n))

                # Determine which metric tab is active
                try:
                    midx = int(nb.index("current"))
                except Exception:
                    midx = 0
                metric_key = (
                    self._plots_metric_key_order[midx]
                    if 0 <= midx < len(self._plots_metric_key_order)
                    else "kwh"
                )
                is_wva = metric_key in {"V", "A", "W", "VAR", "COSPHI", "HZ"}

                if unit == "all":
                    if is_wva:
                        self._wva_len.set(9999.0)
                        self._wva_unit.set("days")
                    elif metric_key == "kwh":
                        self._plots_mode.set("all")
                    elif metric_key == "CO2":
                        self._plots_co2_mode.set("all")
                    elif metric_key == "DYNPRICE":
                        self._plots_dynprice_mode.set("all")
                else:
                    if is_wva:
                        self._wva_len.set(float(n))
                        self._wva_unit.set(unit)
                    elif metric_key == "kwh":
                        self._plots_mode.set(f"{unit}:{n}")
                    elif metric_key == "CO2":
                        self._plots_co2_mode.set(f"{unit}:{n}")
                    elif metric_key == "DYNPRICE":
                        self._plots_dynprice_mode.set(f"{unit}:{n}")
                self._redraw_plots_active()

            # Bind all relevant events for instant refresh
            try:
                gran_cb.bind("<<ComboboxSelected>>", _apply_shared_controls)
                sp_n.bind("<Return>", _apply_shared_controls)
                sp_n.bind("<<Increment>>", _apply_shared_controls)
                sp_n.bind("<<Decrement>>", _apply_shared_controls)
                sp_n.bind("<FocusOut>", _apply_shared_controls)
                sp_n.bind("<ButtonRelease-1>", lambda _e: sp_n.after(50, _apply_shared_controls))
            except Exception:
                pass

            def _make_device_notebook(parent: ttk.Frame, metric_key: str, two_axes: bool = False) -> None:
                dev_nb = ttk.Notebook(parent)
                dev_nb.pack(fill="both", expand=True)
                self._plots_device_nb[metric_key] = dev_nb
                self._plots_device_order[metric_key] = []
                self._plots_figs2[metric_key] = {}
                self._plots_axes2[metric_key] = {}
                self._plots_canvases2[metric_key] = {}

                # Remember last selected device tab index across metrics
                def _on_device_tab_changed(evt: Any = None) -> None:
                    if getattr(self, "_plots_syncing_tabs", False):
                        return
                    try:
                        self._plots_last_device_idx = int(dev_nb.index("current"))
                    except Exception:
                        pass

                try:
                    dev_nb.bind("<<NotebookTabChanged>>", _on_device_tab_changed)
                except Exception:
                    pass

                for d in list(getattr(self.cfg, "devices", []) or []):
                    tab = ttk.Frame(dev_nb)
                    dev_nb.add(tab, text=d.name)

                    fig = Figure(figsize=(11, 5.0), dpi=96)
                    # Set initial figure background to match theme so no
                    # white flash appears before the first redraw.
                    try:
                        _init_bg = "#111111" if self._resolve_plot_theme() == "night" else "#FFFFFF"
                        fig.patch.set_facecolor(_init_bg)
                    except Exception:
                        _init_bg = "#FFFFFF"
                    # Axes are created during redraw (because V/A uses 2 axes)
                    canvas = FigureCanvasTkAgg(fig, master=tab)
                    canvas.get_tk_widget().configure(bg=_init_bg)
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
            _make_device_notebook(tab_kwh, "kwh")

            # --- V tab ---
            tab_v = ttk.Frame(nb)
            nb.add(tab_v, text="V")
            _make_device_notebook(tab_v, "V", two_axes=True)

            # --- A tab ---
            tab_a = ttk.Frame(nb)
            nb.add(tab_a, text="A")
            _make_device_notebook(tab_a, "A", two_axes=True)

            # --- W tab ---
            tab_w = ttk.Frame(nb)
            nb.add(tab_w, text="W")
            _make_device_notebook(tab_w, "W", two_axes=False)

            # --- VAR tab ---
            tab_var = ttk.Frame(nb)
            nb.add(tab_var, text="VAR")
            _make_device_notebook(tab_var, "VAR", two_axes=True)

            # --- cos φ tab ---
            tab_pf = ttk.Frame(nb)
            nb.add(tab_pf, text="cos φ")
            _make_device_notebook(tab_pf, "COSPHI", two_axes=True)

            # --- Hz tab (grid frequency) ---
            tab_hz = ttk.Frame(nb)
            nb.add(tab_hz, text="Hz")
            _make_device_notebook(tab_hz, "HZ", two_axes=False)

            # --- CO₂ tab ---
            tab_co2 = ttk.Frame(nb)
            nb.add(tab_co2, text="CO₂")
            _make_device_notebook(tab_co2, "CO2")

            # --- Dyn. Preis tab ---
            tab_dynprice = ttk.Frame(nb)
            nb.add(tab_dynprice, text=self.t("plots.dynprice.tab"))
            _make_device_notebook(tab_dynprice, "DYNPRICE")

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

    def _redraw_plots_active(self) -> None:
            """Redraw the currently active metric tab (and its selected device)."""
            self._redraw_plots_metric(self._active_metric_key())

    def _redraw_plots_metric(self, metric_key: str) -> None:
            metric_key = str(metric_key or "kwh")
            if metric_key.lower() == "kwh":
                self._redraw_plots_kwh2()
            elif metric_key.upper() == "CO2":
                self._redraw_plots_co2()
            elif metric_key.upper() == "DYNPRICE":
                self._redraw_plots_dynprice()
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
                try:
                    _ps = int(pstart.timestamp()) if pstart is not None else 0
                    _pe = int(pend.timestamp()) if pend is not None else 0
                    if _ps and _pe:
                        _co2_kg, _co2_g, _ = self._calc_co2_for_range(_ps, _pe, device_key=key, kwh_fallback=total)
                    else:
                        _co2_kg, _co2_g = total * 380.0 / 1000.0, 380.0
                    co2_lbl = f" / {_co2_kg:.2f} kg CO₂" if _co2_kg > 0 else ""
                except Exception:
                    co2_lbl = ""
                ax.set_title(f"{dcfg.name} – {self._pretty_kwh_mode(mode)}{range_lbl} | {_fmt_kwh(total)} ({_fmt_eur(cost)}){co2_lbl}")
                ax.grid(True, axis="y", alpha=0.3)
                self._annotate_bars(ax, bars)
                try:
                    fig.subplots_adjust(left=0.10, right=0.97, top=0.92, bottom=0.32)
                except Exception:
                    pass
                self._apply_axis_layout(fig, ax, w, legend=False)
                # Apply global theme to kWh history plots
                try:
                    self._apply_plot_theme(fig, ax, canvas=canvas)
                except Exception:
                    pass
                canvas.draw_idle()

    def _redraw_plots_co2(self) -> None:
            """Draw CO₂ emission bar charts: hourly energy × grid CO₂ intensity."""
            if not self._ensure_data_loaded():
                return

            mode = str(getattr(self, "_plots_co2_mode", tk.StringVar(value="hours")).get() or "hours")

            # Support "unit:n" format (e.g. "days:7") from custom range controls
            custom_n: int | None = None
            base_mode = mode
            if ":" in mode:
                parts = mode.split(":", 1)
                base_mode = parts[0]
                try:
                    custom_n = max(1, int(parts[1]))
                except Exception:
                    custom_n = None

            pstart = _parse_date_flexible(self._plots_start.get())
            pend = _parse_date_flexible(self._plots_end.get())
            if pstart is not None and pend is not None and pend < pstart:
                pstart, pend = pend, pstart

            # Determine effective time range
            now = datetime.now()
            if pend is None:
                pend_eff = now
            else:
                pend_eff = pend
            if pstart is None:
                if custom_n is not None:
                    # Custom "last N units" range
                    if base_mode == "hours":
                        pstart_eff = pend_eff - timedelta(hours=custom_n)
                    elif base_mode == "days":
                        pstart_eff = pend_eff - timedelta(days=custom_n)
                    elif base_mode == "weeks":
                        pstart_eff = pend_eff - timedelta(weeks=custom_n)
                    else:  # months
                        pstart_eff = pend_eff - timedelta(days=custom_n * 30)
                else:
                    # Default windows based on mode
                    if base_mode == "hours":
                        pstart_eff = pend_eff - timedelta(hours=24)
                    elif base_mode == "days":
                        pstart_eff = pend_eff - timedelta(days=30)
                    elif base_mode == "weeks":
                        pstart_eff = pend_eff - timedelta(weeks=12)
                    else:  # months
                        pstart_eff = pend_eff - timedelta(days=365)
            else:
                pstart_eff = pstart

            start_ts = int(pstart_eff.timestamp())
            end_ts = int(pend_eff.timestamp())

            # Get CO₂ intensity data
            co2_cfg = getattr(self.cfg, "co2", None)
            zone = getattr(co2_cfg, "bidding_zone", "DE_LU") or "DE_LU"
            db = self.storage.db
            df_co2 = db.query_co2_intensity(zone, start_ts, end_ts + 3600)

            dev_key = self._selected_device_key("CO2")
            keys = [dev_key] if dev_key else list((self._plots_device_order.get("CO2") or []))

            for key in keys:
                if not key:
                    continue
                dcfg = next((d for d in self.cfg.devices if d.key == key), None)
                fig = self._plots_figs2.get("CO2", {}).get(key)
                canvas = self._plots_canvases2.get("CO2", {}).get(key)
                if dcfg is None or fig is None or canvas is None:
                    continue

                w = canvas.get_tk_widget()
                self._resize_figure_to_widget(fig, w, dpi=self._dpi_for_widget(w), min_h_px=320)
                fig.clear()
                ax = fig.add_subplot(111)

                if df_co2.empty:
                    ax.set_title(self.t("plots.co2.no_data"))
                    try:
                        self._apply_plot_theme(fig, ax, canvas=canvas)
                    except Exception:
                        pass
                    canvas.draw_idle()
                    continue

                # Get hourly energy for this device
                df_h = db.query_hourly(key, start_ts, end_ts + 3600)
                if df_h is None or df_h.empty:
                    ax.set_title(f"{dcfg.name} – {self.t('plots.co2.no_data')}")
                    try:
                        self._apply_plot_theme(fig, ax, canvas=canvas)
                    except Exception:
                        pass
                    canvas.draw_idle()
                    continue

                # Join hourly energy with CO₂ intensity
                merged = pd.merge(
                    df_h[["hour_ts", "kwh"]],
                    df_co2[["hour_ts", "intensity_g_per_kwh"]],
                    on="hour_ts",
                    how="inner",
                )
                if merged.empty:
                    ax.set_title(f"{dcfg.name} – {self.t('plots.co2.no_data')}")
                    try:
                        self._apply_plot_theme(fig, ax, canvas=canvas)
                    except Exception:
                        pass
                    canvas.draw_idle()
                    continue

                merged["co2_g"] = merged["kwh"] * merged["intensity_g_per_kwh"]
                merged["dt"] = pd.to_datetime(merged["hour_ts"], unit="s", utc=True)
                merged = merged.sort_values("dt")

                # Aggregate by mode
                if base_mode == "hours":
                    labels = [dt.strftime("%H:%M\n%d.%m") for dt in merged["dt"]]
                    values = merged["co2_g"].tolist()
                    kwh_values = merged["kwh"].tolist()
                    avg_intensities = merged["intensity_g_per_kwh"].tolist()
                elif base_mode == "days":
                    merged["bucket"] = merged["dt"].dt.date
                    grp = merged.groupby("bucket").agg(
                        co2_g=("co2_g", "sum"), kwh=("kwh", "sum"),
                        intensity=("intensity_g_per_kwh", "mean"),
                    ).reset_index()
                    grp = grp.sort_values("bucket")
                    labels = [str(d) for d in grp["bucket"]]
                    values = grp["co2_g"].tolist()
                    kwh_values = grp["kwh"].tolist()
                    avg_intensities = grp["intensity"].tolist()
                elif base_mode == "weeks":
                    merged["bucket"] = merged["dt"].dt.isocalendar().week.astype(int)
                    merged["year"] = merged["dt"].dt.isocalendar().year.astype(int)
                    grp = merged.groupby(["year", "bucket"]).agg(
                        co2_g=("co2_g", "sum"), kwh=("kwh", "sum"),
                        intensity=("intensity_g_per_kwh", "mean"),
                    ).reset_index()
                    grp = grp.sort_values(["year", "bucket"])
                    labels = [f"KW{int(w)}" for w in grp["bucket"]]
                    values = grp["co2_g"].tolist()
                    kwh_values = grp["kwh"].tolist()
                    avg_intensities = grp["intensity"].tolist()
                else:  # months
                    merged["bucket"] = merged["dt"].dt.to_period("M")
                    grp = merged.groupby("bucket").agg(
                        co2_g=("co2_g", "sum"), kwh=("kwh", "sum"),
                        intensity=("intensity_g_per_kwh", "mean"),
                    ).reset_index()
                    grp = grp.sort_values("bucket")
                    labels = [str(p) for p in grp["bucket"]]
                    values = grp["co2_g"].tolist()
                    kwh_values = grp["kwh"].tolist()
                    avg_intensities = grp["intensity"].tolist()

                total_g = sum(values)
                total_kwh = sum(kwh_values)
                # Use kg if total > 1000g
                if total_g >= 1000:
                    unit_label = "kg CO₂"
                    values_plot = [v / 1000.0 for v in values]
                    total_display = f"{total_g / 1000.0:.2f} kg CO₂"
                    use_kg = True
                else:
                    unit_label = "g CO₂"
                    values_plot = values
                    total_display = f"{total_g:.1f} g CO₂"
                    use_kg = False

                # Color bars by average CO₂ intensity (green → yellow → red)
                co2_thresh_green = float(getattr(co2_cfg, "green_threshold", 150) or 150)
                co2_thresh_dirty = float(getattr(co2_cfg, "dirty_threshold", 400) or 400)
                bar_colors = []
                for intensity in avg_intensities:
                    try:
                        iv = float(intensity)
                    except Exception:
                        iv = 0.0
                    if iv <= co2_thresh_green:
                        bar_colors.append("#4caf50")  # green – clean
                    elif iv >= co2_thresh_dirty:
                        bar_colors.append("#e53935")  # red – dirty
                    else:
                        # Linear interpolation green → yellow → red
                        ratio = (iv - co2_thresh_green) / max(1, co2_thresh_dirty - co2_thresh_green)
                        if ratio <= 0.5:
                            # green → yellow
                            t = ratio * 2.0
                            r = int(0x4c + (0xfb - 0x4c) * t)
                            g = int(0xaf + (0xc0 - 0xaf) * t)
                            b = int(0x50 + (0x2e - 0x50) * t)
                            bar_colors.append(f"#{r:02x}{g:02x}{b:02x}")
                        else:
                            # yellow → red
                            t = (ratio - 0.5) * 2.0
                            r = int(0xfb + (0xe5 - 0xfb) * t)
                            g = int(0xc0 + (0x39 - 0xc0) * t)
                            b = int(0x2e + (0x35 - 0x2e) * t)
                            bar_colors.append(f"#{r:02x}{g:02x}{b:02x}")

                ax.set_ylabel(unit_label)
                bars = ax.bar(range(len(values_plot)), values_plot, color=bar_colors)
                base = self._font_base_for_widget(w)
                self._apply_xticks(ax, labels, base_font=base)

                # Custom annotation: show both kWh and CO₂ above each bar
                try:
                    hs = [float(b.get_height()) for b in bars if math.isfinite(float(b.get_height()))]
                    if hs:
                        y0, y1 = ax.get_ylim()
                        hmax = max(hs)
                        # Extra headroom for two-line labels (kWh + CO₂)
                        margin = max(0.5, abs(hmax) * 0.28)
                        ax.set_ylim(y0, max(y1, hmax + margin))
                except Exception:
                    pass

                try:
                    n_bars = len(bars)
                    step = 1
                    if n_bars > 31:
                        step = max(1, int(math.ceil(n_bars / 31)))
                    fontsize = max(4, min(10, int(base) - 1))

                    for i, b in enumerate(bars):
                        if step > 1 and (i % step) != 0:
                            continue
                        try:
                            h = float(b.get_height())
                        except Exception:
                            continue
                        if not math.isfinite(h):
                            continue
                        # Format CO₂ value
                        co2_raw = values[i]
                        if use_kg:
                            co2_txt = f"{co2_raw / 1000.0:.2f} kg"
                        elif co2_raw >= 100:
                            co2_txt = f"{co2_raw:.0f} g"
                        else:
                            co2_txt = f"{co2_raw:.1f} g"
                        # Format kWh value
                        kwh_v = kwh_values[i]
                        if kwh_v >= 10:
                            kwh_txt = f"{kwh_v:.1f} kWh"
                        elif kwh_v >= 1:
                            kwh_txt = f"{kwh_v:.2f} kWh"
                        else:
                            kwh_txt = f"{kwh_v:.3f} kWh"
                        label_txt = f"{kwh_txt}\n{co2_txt}"
                        ax.annotate(
                            label_txt,
                            xy=(b.get_x() + b.get_width() / 2, h),
                            xytext=(0, 4),
                            textcoords="offset points",
                            ha="center", va="bottom",
                            fontsize=fontsize,
                            linespacing=1.15,
                        )
                except Exception:
                    pass

                range_lbl = ""
                if pstart is not None or pend is not None:
                    a = pstart_eff.strftime("%Y-%m-%d")
                    b_str = pend_eff.strftime("%Y-%m-%d")
                    range_lbl = f" | {a}–{b_str}"
                ax.set_title(
                    f"{dcfg.name} – CO₂ ({self._pretty_kwh_mode(mode)}){range_lbl}"
                    f" | {_fmt_kwh(total_kwh)} | {total_display}"
                )
                ax.grid(True, axis="y", alpha=0.3)
                try:
                    fig.subplots_adjust(left=0.10, right=0.97, top=0.90, bottom=0.32)
                except Exception:
                    pass
                self._apply_axis_layout(fig, ax, w, legend=False)
                try:
                    self._apply_plot_theme(fig, ax, canvas=canvas)
                except Exception:
                    pass
                canvas.draw_idle()

    def _redraw_plots_dynprice(self) -> None:
            """Draw grouped bar chart: fixed tariff cost vs dynamic spot cost per period."""
            if not self._ensure_data_loaded():
                return

            mode = str(getattr(self, "_plots_dynprice_mode", tk.StringVar(value="days")).get() or "days")

            custom_n: int | None = None
            base_mode = mode
            if ":" in mode:
                parts = mode.split(":", 1)
                base_mode = parts[0]
                try:
                    custom_n = max(1, int(parts[1]))
                except Exception:
                    custom_n = None

            pstart = _parse_date_flexible(self._plots_start.get())
            pend = _parse_date_flexible(self._plots_end.get())
            if pstart is not None and pend is not None and pend < pstart:
                pstart, pend = pend, pstart

            now = datetime.now()
            pend_eff = pend if pend is not None else now
            if pstart is None:
                if custom_n is not None:
                    if base_mode == "hours":
                        pstart_eff = pend_eff - timedelta(hours=custom_n)
                    elif base_mode == "days":
                        pstart_eff = pend_eff - timedelta(days=custom_n)
                    elif base_mode == "weeks":
                        pstart_eff = pend_eff - timedelta(weeks=custom_n)
                    else:
                        pstart_eff = pend_eff - timedelta(days=custom_n * 30)
                else:
                    if base_mode == "hours":
                        pstart_eff = pend_eff - timedelta(hours=48)
                    elif base_mode == "days":
                        pstart_eff = pend_eff - timedelta(days=30)
                    elif base_mode == "weeks":
                        pstart_eff = pend_eff - timedelta(weeks=12)
                    else:
                        pstart_eff = pend_eff - timedelta(days=365)
            else:
                pstart_eff = pstart

            start_ts = int(pstart_eff.timestamp())
            end_ts = int(pend_eff.timestamp())

            # Spot price config
            spot_cfg = getattr(self.cfg, "spot_price", None)
            spot_enabled = getattr(spot_cfg, "enabled", False) if spot_cfg else False
            zone = getattr(spot_cfg, "bidding_zone", "DE-LU") or "DE-LU" if spot_cfg else "DE-LU"
            markup = float((spot_cfg.total_markup_ct() if hasattr(spot_cfg, "total_markup_ct") else getattr(spot_cfg, "markup_ct_per_kwh", 16.0)) if spot_cfg else 16.0) / 100.0
            pricing = getattr(self.cfg, "pricing", None)
            if spot_cfg and getattr(spot_cfg, "include_vat", True) and pricing:
                vat_rate = pricing.vat_rate()
            else:
                vat_rate = 0.0

            # Fixed tariff price
            try:
                fixed_price = float(pricing.unit_price_gross())
            except Exception:
                fixed_price = 0.30

            db = self.storage.db

            # Query spot prices for the range
            df_spot = db.query_spot_prices(zone, start_ts, end_ts + 3600) if spot_enabled else pd.DataFrame()

            dev_key = self._selected_device_key("DYNPRICE")
            keys = [dev_key] if dev_key else list((self._plots_device_order.get("DYNPRICE") or []))

            for key in keys:
                if not key:
                    continue
                dcfg = next((d for d in self.cfg.devices if d.key == key), None)
                fig = self._plots_figs2.get("DYNPRICE", {}).get(key)
                canvas = self._plots_canvases2.get("DYNPRICE", {}).get(key)
                if dcfg is None or fig is None or canvas is None:
                    continue

                w = canvas.get_tk_widget()
                self._resize_figure_to_widget(fig, w, dpi=self._dpi_for_widget(w), min_h_px=320)
                fig.clear()
                ax = fig.add_subplot(111)

                if not spot_enabled:
                    ax.set_title(self.t("plots.dynprice.not_enabled"))
                    try:
                        self._apply_plot_theme(fig, ax, canvas=canvas)
                    except Exception:
                        pass
                    canvas.draw_idle()
                    continue

                # Get hourly energy for device
                df_h = db.query_hourly(key, start_ts, end_ts + 3600)
                if df_h is None or df_h.empty:
                    ax.set_title(f"{dcfg.name} – {self.t('plots.no_data')}")
                    try:
                        self._apply_plot_theme(fig, ax, canvas=canvas)
                    except Exception:
                        pass
                    canvas.draw_idle()
                    continue

                # Build hourly spot prices (average 15-min slots per hour)
                if not df_spot.empty:
                    df_sp_h = df_spot.copy()
                    df_sp_h["hour_ts"] = (df_sp_h["slot_ts"] // 3600) * 3600
                    df_sp_h = df_sp_h.groupby("hour_ts").agg(price_eur_mwh=("price_eur_mwh", "mean")).reset_index()
                else:
                    df_sp_h = pd.DataFrame(columns=["hour_ts", "price_eur_mwh"])

                # Merge energy with spot prices
                merged = pd.merge(
                    df_h[["hour_ts", "kwh"]],
                    df_sp_h[["hour_ts", "price_eur_mwh"]],
                    on="hour_ts", how="left",
                )
                merged["dt"] = pd.to_datetime(merged["hour_ts"], unit="s", utc=True)
                merged = merged.sort_values("dt")

                # Calculate costs per hour
                merged["fixed_eur"] = merged["kwh"] * fixed_price
                merged["spot_eur_kwh"] = merged["price_eur_mwh"].fillna(0) / 1000.0
                merged["dyn_eur"] = merged["kwh"] * (merged["spot_eur_kwh"] + markup) * (1.0 + vat_rate)
                # Where no spot data, set dyn_eur to NaN
                merged.loc[merged["price_eur_mwh"].isna(), "dyn_eur"] = float("nan")

                # Aggregate by mode
                if base_mode == "hours":
                    labels = [dt.strftime("%H:%M\n%d.%m") for dt in merged["dt"]]
                    fixed_vals = merged["fixed_eur"].tolist()
                    dyn_vals = merged["dyn_eur"].tolist()
                    kwh_vals = merged["kwh"].tolist()
                elif base_mode == "days":
                    merged["bucket"] = merged["dt"].dt.date
                    grp = merged.groupby("bucket").agg(
                        fixed_eur=("fixed_eur", "sum"), dyn_eur=("dyn_eur", "sum"),
                        kwh=("kwh", "sum"),
                    ).reset_index().sort_values("bucket")
                    labels = [str(d) for d in grp["bucket"]]
                    fixed_vals = grp["fixed_eur"].tolist()
                    dyn_vals = grp["dyn_eur"].tolist()
                    kwh_vals = grp["kwh"].tolist()
                elif base_mode == "weeks":
                    merged["week"] = merged["dt"].dt.isocalendar().week.astype(int)
                    merged["year"] = merged["dt"].dt.isocalendar().year.astype(int)
                    grp = merged.groupby(["year", "week"]).agg(
                        fixed_eur=("fixed_eur", "sum"), dyn_eur=("dyn_eur", "sum"),
                        kwh=("kwh", "sum"),
                    ).reset_index().sort_values(["year", "week"])
                    labels = [f"KW{int(w)}" for w in grp["week"]]
                    fixed_vals = grp["fixed_eur"].tolist()
                    dyn_vals = grp["dyn_eur"].tolist()
                    kwh_vals = grp["kwh"].tolist()
                else:  # months
                    merged["bucket"] = merged["dt"].dt.to_period("M")
                    grp = merged.groupby("bucket").agg(
                        fixed_eur=("fixed_eur", "sum"), dyn_eur=("dyn_eur", "sum"),
                        kwh=("kwh", "sum"),
                    ).reset_index().sort_values("bucket")
                    labels = [str(p) for p in grp["bucket"]]
                    fixed_vals = grp["fixed_eur"].tolist()
                    dyn_vals = grp["dyn_eur"].tolist()
                    kwh_vals = grp["kwh"].tolist()

                import numpy as _np
                n = len(labels)
                if n == 0:
                    ax.set_title(f"{dcfg.name} – {self.t('plots.no_data')}")
                    try:
                        self._apply_plot_theme(fig, ax, canvas=canvas)
                    except Exception:
                        pass
                    canvas.draw_idle()
                    continue

                x = _np.arange(n)
                w_bar = 0.35

                # Replace NaN with 0 for display
                dyn_clean = [v if math.isfinite(v) else 0.0 for v in dyn_vals]

                bars_fixed = ax.bar(x - w_bar / 2, fixed_vals, w_bar,
                                     label=self.t("plots.dynprice.fixed"), color="#2196F3", alpha=0.85)
                bars_dyn = ax.bar(x + w_bar / 2, dyn_clean, w_bar,
                                   label=self.t("plots.dynprice.dynamic"), color="#ff9800", alpha=0.85)

                ax.set_ylabel("\u20ac")
                base = self._font_base_for_widget(w)
                self._apply_xticks(ax, labels, base_font=base)

                # Annotations: cost values above bars
                try:
                    step = max(1, n // 25)
                    fontsize = max(4, min(9, int(base) - 1))
                    for i in range(0, n, step):
                        fv = fixed_vals[i]
                        dv = dyn_clean[i]
                        if fv > 0:
                            ax.annotate(f"{fv:.2f}", xy=(x[i] - w_bar / 2, fv),
                                        xytext=(0, 3), textcoords="offset points",
                                        ha="center", va="bottom", fontsize=fontsize, color="#2196F3")
                        if dv > 0:
                            ax.annotate(f"{dv:.2f}", xy=(x[i] + w_bar / 2, dv),
                                        xytext=(0, 3), textcoords="offset points",
                                        ha="center", va="bottom", fontsize=fontsize, color="#ff9800")
                except Exception:
                    pass

                # Totals
                total_fixed = sum(fixed_vals)
                total_dyn = sum(v for v in dyn_vals if math.isfinite(v))
                total_kwh = sum(kwh_vals)
                delta = total_dyn - total_fixed
                arrow = "\u2191" if delta > 0 else "\u2193"

                range_lbl = ""
                if pstart is not None or pend is not None:
                    a = pstart_eff.strftime("%Y-%m-%d")
                    b_str = pend_eff.strftime("%Y-%m-%d")
                    range_lbl = f" | {a}\u2013{b_str}"

                ax.set_title(
                    f"{dcfg.name} \u2013 {self.t('plots.dynprice.title')} ({self._pretty_kwh_mode(mode)}){range_lbl}\n"
                    f"{_fmt_kwh(total_kwh)} | {self.t('plots.dynprice.fixed')}: {total_fixed:.2f} \u20ac | "
                    f"{self.t('plots.dynprice.dynamic')}: {total_dyn:.2f} \u20ac | "
                    f"\u0394: {delta:+.2f} \u20ac {arrow}",
                    fontsize=max(8, int(base) - 1),
                )

                leg = ax.legend(fontsize=max(7, int(base) - 1))
                ax.grid(True, axis="y", alpha=0.3)
                try:
                    fig.subplots_adjust(left=0.10, right=0.97, top=0.85, bottom=0.32)
                except Exception:
                    pass
                self._apply_axis_layout(fig, ax, w, legend=False)
                try:
                    self._apply_plot_theme(fig, ax, canvas=canvas)
                    # Re-apply legend colors after theme
                    if leg:
                        try:
                            theme = self._resolve_plot_theme()
                        except Exception:
                            theme = "day"
                        fg = "#E6E6E6" if theme == "night" else "#000000"
                        bg = "#111111" if theme == "night" else "#FFFFFF"
                        leg.get_frame().set_facecolor(bg)
                        for txt in leg.get_texts():
                            txt.set_color(fg)
                except Exception:
                    pass
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

                if metric in {"V", "A", "VAR", "Q", "COSPHI", "PF", "Hz", "HZ"} and not self._df_has_wva_cols(df_src, metric):
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
                        if lab == "N":
                            ax2.plot(tmp.index, tmp.values, label=lab, color="gray", linestyle="--", alpha=0.7)
                        else:
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

                # Apply global theme to history plots
                try:
                    self._apply_plot_theme(fig, ax1, canvas=canvas)
                    if ax2 is not None:
                        self._apply_plot_theme(fig, ax2)
                except Exception:
                    pass
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

            Important: on macOS/Tk, ``<Configure>`` events fire not only on true
            widget resizes but also on internal Tk geometry re-layouts (e.g. when
            ``update_idletasks()`` is called or when hidden notebook tabs get
            re-measured).  These spurious events all carry the *same* width/height
            as the previous event.  Reacting to them causes a jitter loop:
            the redraw calls ``update_idletasks()``, which triggers another
            ``<Configure>``, which triggers another redraw, ad infinitum.
            This loop is especially visible on Hz / V / A live-data plots which
            redraw every ~1 second, producing a continuous bottom-axis shimmer.

            Fix: ignore the event when this widget's size has not actually changed.
            A genuine resize always carries a new (w, h) pair.
            """
            # Ignore spurious <Configure> events that carry the same size as
            # before for this specific widget.  Only a true size change warrants
            # a figure redraw.
            try:
                new_w = int(getattr(_event, 'width', 0) or 0)
                new_h = int(getattr(_event, 'height', 0) or 0)
                widget_id = id(getattr(_event, 'widget', None))
                if new_w > 0 and new_h > 0:
                    sizes = getattr(self, '_plots_canvas_last_sizes', {})
                    if sizes.get(widget_id) == (new_w, new_h):
                        return  # same size for this widget – skip to avoid jitter
                    sizes[widget_id] = (new_w, new_h)
                    self._plots_canvas_last_sizes = sizes
            except Exception:
                pass

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

                # For V/A/VAR/COSPHI: show total on top axis and phases on bottom axis.
                if metric in {"W", "V", "A", "VAR", "Q", "COSPHI", "PF"}:
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
                        if lab == "N":
                            ax2.plot(tmp.index, tmp.values, label=lab, color="gray", linestyle="--", alpha=0.7)
                        else:
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
                # Apply global theme to legacy WVA plots
                try:
                    self._apply_plot_theme(fig, ax1, canvas=canvas)
                    if ax2 is not None:
                        self._apply_plot_theme(fig, ax2)
                except Exception:
                    pass
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
                try:
                    _ps = int(pstart.timestamp()) if pstart is not None else 0
                    _pe = int(pend.timestamp()) if pend is not None else 0
                    if _ps and _pe:
                        _co2_kg, _, _ = self._calc_co2_for_range(_ps, _pe, device_key=d.key, kwh_fallback=total)
                    else:
                        _co2_kg = total * 380.0 / 1000.0
                    co2_lbl = f" / {_co2_kg:.2f} kg CO₂" if _co2_kg > 0 else ""
                except Exception:
                    co2_lbl = ""
                ax.set_title(f"{d.name} – {self._pretty_kwh_mode(mode)}{range_lbl} | {_fmt_kwh(total)} ({_fmt_eur(cost)}){co2_lbl}")
                ax.grid(True, axis="y", alpha=0.3)
                self._annotate_bars(ax, bars)
                self._apply_axis_layout(fig, ax, canvas.get_tk_widget(), legend=False)
                # Apply global theme to legacy stats plots
                try:
                    self._apply_plot_theme(fig, ax, canvas=canvas)
                except Exception:
                    pass
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
            try:
                _ts_col = cd.df.get("timestamp")
                if _ts_col is not None and len(_ts_col) > 0:
                    _ps = int(pd.Timestamp(_ts_col.iloc[0]).timestamp())
                    _pe = int(pd.Timestamp(_ts_col.iloc[-1]).timestamp())
                    _co2_kg, _, _ = self._calc_co2_for_range(_ps, _pe, device_key=cd.device_key, kwh_fallback=total)
                else:
                    _co2_kg = total * 380.0 / 1000.0
                co2_lbl = f" / {_co2_kg:.2f} kg CO₂" if _co2_kg > 0 else ""
            except Exception:
                co2_lbl = ""
            ax.set_title(f"{cd.device_name} – {self._pretty_kwh_mode(mode)} | {_fmt_kwh(total)} ({_fmt_eur(total * unit_gross)}){co2_lbl}")
            ax.grid(True, axis="y", alpha=0.3)
            self._annotate_bars(ax, bars)
            fig.tight_layout()
            return fig

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

                    # Day/Night appearance – resolved via global config (plot_theme_mode).
            theme = self._resolve_plot_theme()

            def _apply_live_theme(fig, ax):
                """Apply light/dark theme to a single matplotlib axis."""
                try:
                    if theme == "night":
                        bg = "#111111"
                        fg = "#E6E6E6"
                        grid = "#AAAAAA"
                    else:
                        bg = "#FFFFFF"
                        fg = "#000000"
                        grid = "#BBBBBB"

                    try:
                        fig.patch.set_facecolor(bg)
                    except Exception:
                        pass
                    try:
                        ax.set_facecolor(bg)
                    except Exception:
                        pass
                    try:
                        ax.tick_params(axis="both", colors=fg)
                    except Exception:
                        pass
                    try:
                        ax.xaxis.label.set_color(fg)
                        ax.yaxis.label.set_color(fg)
                    except Exception:
                        pass
                    try:
                        if ax.title:
                            ax.title.set_color(fg)
                    except Exception:
                        pass
                    try:
                        for spine in ax.spines.values():
                            spine.set_color(fg)
                    except Exception:
                        pass
                    try:
                        ax.grid(True, axis="y", alpha=0.4 if theme == "night" else 0.25)
                        # Adjust grid color if possible
                        for gl in ax.get_ygridlines():
                            gl.set_color(grid)
                    except Exception:
                        pass
                    try:
                        leg = ax.get_legend()
                        if leg is not None:
                            try:
                                leg.get_frame().set_facecolor(bg)
                                leg.get_frame().set_edgecolor(grid)
                            except Exception:
                                pass
                            try:
                                for txt in leg.get_texts():
                                    try:
                                        txt.set_color(fg)
                                    except Exception:
                                        pass
                            except Exception:
                                pass
                    except Exception:
                        pass
                    try:
                        # Any additional texts/annotations on the axis
                        for txt in getattr(ax, "texts", []) or []:
                            try:
                                txt.set_color(fg)
                            except Exception:
                                pass
                    except Exception:
                        pass

                except Exception:
                    pass

            # Backwards compat: keep calls in plot code harmless
            def _filter_daynight(xs_dt, ys_list):
                return xs_dt, ys_list

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
                    _apply_live_theme(self._live_figs[d.key]['power'], ax_p)
                    if arr:
                        xs = [datetime.fromtimestamp(t) for t, _ in arr]
                        ys = [v for _, v in arr]
                        xs, ys = _filter_daynight(xs, ys)
                        ys = _maybe_smooth(xs, ys)
                        ax_p.plot(xs, ys)
                    ax_p.grid(True, axis="y", alpha=0.3)
                    self._configure_time_axis(ax_p, canvas_p.get_tk_widget(), win_m)
                    self._apply_axis_layout(self._live_figs[d.key]["power"], ax_p, canvas_p.get_tk_widget(), legend=False)
                    _apply_live_theme(self._live_figs[d.key]["power"], ax_p)
                    canvas_p.draw_idle()

                # Voltage (L1/L2/L3 in one plot)

                ax_v = self._live_axes.get(d.key, {}).get("voltage")
                canvas_v = self._live_canvases.get(d.key, {}).get("voltage")
                if ax_v is not None and canvas_v is not None:
                    ax_v.clear()
                    ax_v.set_ylabel("V")
                    ax_v.set_xlabel(self.t('live.time'))
                    _apply_live_theme(self._live_figs[d.key]['voltage'], ax_v)
                    n_series = 0
                    ph = int(getattr(d, "phases", 3) or 3)
                    phase_keys = [("a_voltage", "L1")] if ph <= 1 else [("a_voltage", "L1"), ("b_voltage", "L2"), ("c_voltage", "L3")]
                    for key, label in phase_keys:
                        arr = _slice_live(metrics.get(key, []))
                        if not arr:
                            continue
                        xs = [datetime.fromtimestamp(t) for t, _ in arr]
                        ys = [v for _, v in arr]
                        xs, ys = _filter_daynight(xs, ys)
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
                        self._apply_live_legend_toggle(ax_v, canvas_v, d.key, "voltage")
                    ax_v.grid(True, axis="y", alpha=0.3)
                    self._configure_time_axis(ax_v, canvas_v.get_tk_widget(), win_m)
                    self._apply_axis_layout(self._live_figs[d.key]["voltage"], ax_v, canvas_v.get_tk_widget(), legend=(n_series > 1))
                    _apply_live_theme(self._live_figs[d.key]["voltage"], ax_v)
                    canvas_v.draw_idle()

                # Current (L1/L2/L3/N in one plot)

                ax_c = self._live_axes.get(d.key, {}).get("current")
                canvas_c = self._live_canvases.get(d.key, {}).get("current")
                if ax_c is not None and canvas_c is not None:
                    ax_c.clear()
                    ax_c.set_ylabel("A")
                    ax_c.set_xlabel(self.t('live.time'))
                    _apply_live_theme(self._live_figs[d.key]['current'], ax_c)
                    n_series = 0
                    ph = int(getattr(d, "phases", 3) or 3)
                    phase_keys = [("a_current", "L1")] if ph <= 1 else [("a_current", "L1"), ("b_current", "L2"), ("c_current", "L3")]
                    for key, label in phase_keys:
                        arr = _slice_live(metrics.get(key, []))
                        if not arr:
                            continue
                        xs = [datetime.fromtimestamp(t) for t, _ in arr]
                        ys = [v for _, v in arr]
                        xs, ys = _filter_daynight(xs, ys)
                        ys = _maybe_smooth(xs, ys)
                        ax_c.plot(xs, ys, label=label)
                        n_series += 1
                    # Neutral current (dashed, gray) — 3-phase only
                    if ph >= 3:
                        arr_n = _slice_live(metrics.get("n_current", []))
                        if arr_n:
                            xs_n = [datetime.fromtimestamp(t) for t, _ in arr_n]
                            ys_n = [v for _, v in arr_n]
                            ys_n = _maybe_smooth(xs_n, ys_n)
                            ax_c.plot(xs_n, ys_n, label="N", linestyle="--", color="gray", linewidth=1.5)
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
                        self._apply_live_legend_toggle(ax_c, canvas_c, d.key, "current")
                    ax_c.grid(True, axis="y", alpha=0.3)
                    self._configure_time_axis(ax_c, canvas_c.get_tk_widget(), win_m)
                    self._apply_axis_layout(self._live_figs[d.key]["current"], ax_c, canvas_c.get_tk_widget(), legend=(n_series > 1))
                    _apply_live_theme(self._live_figs[d.key]['current'], ax_c)
                    canvas_c.draw_idle()

    def _apply_live_legend_toggle(self, ax, canvas, device_key: str, metric: str) -> None:
            """Apply hidden-line state and connect legend pick events.

            Stored state: self._live_hidden_lines = {(device_key, metric): set_of_labels}
            On each redraw this is called AFTER legend creation to:
              1. Hide lines whose label is in the hidden set
              2. Dim the corresponding legend entries (alpha 0.3)
              3. Connect a pick_event handler (once per canvas) that toggles visibility
            """
            try:
                if not hasattr(self, '_live_hidden_lines'):
                    self._live_hidden_lines = {}
                hidden = self._live_hidden_lines.get((device_key, metric), set())

                # Apply hidden state to plot lines
                for line in ax.get_lines():
                    lbl = line.get_label()
                    if lbl.startswith('_'):
                        continue
                    if lbl in hidden:
                        line.set_visible(False)

                # Apply to legend entries
                legend = ax.get_legend()
                if not legend:
                    return
                for leg_line, leg_text in zip(legend.get_lines(), legend.get_texts()):
                    leg_line.set_picker(True)
                    leg_line.set_pickradius(10)
                    if leg_text.get_text() in hidden:
                        leg_line.set_alpha(0.3)
                        leg_text.set_alpha(0.3)

                # Connect pick event once per canvas
                if not hasattr(self, '_live_legend_cids'):
                    self._live_legend_cids = set()
                canvas_id = id(canvas)
                if canvas_id not in self._live_legend_cids:
                    self._live_legend_cids.add(canvas_id)

                    def _on_legend_pick(event, _self=self, _dk=device_key, _met=metric, _ax=ax, _cv=canvas):
                        try:
                            if not hasattr(event, 'artist'):
                                return
                            leg = _ax.get_legend()
                            if not leg:
                                return
                            for ll, lt in zip(leg.get_lines(), leg.get_texts()):
                                if ll is event.artist:
                                    lbl = lt.get_text()
                                    hl = getattr(_self, '_live_hidden_lines', {})
                                    s = hl.get((_dk, _met), set())
                                    if lbl in s:
                                        s.discard(lbl)
                                    else:
                                        s.add(lbl)
                                    hl[(_dk, _met)] = s
                                    _self._live_hidden_lines = hl
                                    # Toggle current lines
                                    for pl in _ax.get_lines():
                                        if pl.get_label() == lbl:
                                            pl.set_visible(lbl not in s)
                                    new_alpha = 0.3 if lbl in s else 1.0
                                    ll.set_alpha(new_alpha)
                                    lt.set_alpha(new_alpha)
                                    _cv.draw_idle()
                                    break
                        except Exception:
                            pass

                    canvas.mpl_connect('pick_event', _on_legend_pick)
            except Exception:
                pass

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
