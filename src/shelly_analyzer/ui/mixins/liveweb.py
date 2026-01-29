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

class LiveWebMixin:
    """Auto-generated mixin extracted from the former ui/app.py to keep files smaller."""

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
