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
from shelly_analyzer.services.appliance_detector import identify_appliance as _identify_appliance
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
                self.after(0, _apply)
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

                from datetime import datetime as _dt_s, timedelta as _td_s
                from zoneinfo import ZoneInfo as _ZI_s
                _tz_s = _ZI_s("Europe/Berlin")

                # Determine date range
                if start is not None:
                    start_d = pd.Timestamp(start).date()
                else:
                    start_d = date.today().replace(day=1)
                if end is not None:
                    end_d = pd.Timestamp(end).date()
                else:
                    end_d = date.today()
                # end is inclusive in UI, make exclusive for report
                end_excl = end_d + _td_s(days=1)
                span_days = (end_excl - start_d).days

                # Choose daily vs monthly format
                is_monthly = span_days > 2
                report_type = "monthly" if is_monthly else "daily"

                # Previous period for comparison
                prev_span = _td_s(days=span_days)
                prev_start_d = start_d - prev_span
                prev_end_d = start_d

                start_dt = _dt_s.combine(start_d, _dt_s.min.time(), tzinfo=_tz_s)
                end_dt = _dt_s.combine(end_excl, _dt_s.min.time(), tzinfo=_tz_s)
                prev_start_dt = _dt_s.combine(prev_start_d, _dt_s.min.time(), tzinfo=_tz_s)
                prev_end_dt = _dt_s.combine(prev_end_d, _dt_s.min.time(), tzinfo=_tz_s)

                report_data = self._build_email_report_data(
                    start_dt, end_dt,
                    prev_start_dt=prev_start_dt,
                    prev_end_dt=prev_end_dt,
                    report_type=report_type,
                )

                ts = time.strftime("%Y%m%d_%H%M%S")
                out = out_root / "web" / f"summary_{ts}.pdf"

                if report_data is not None:
                    from shelly_analyzer.services.export import export_pdf_email_daily, export_pdf_email_monthly
                    if is_monthly:
                        export_pdf_email_monthly(report_data, out, lang=self.lang)
                    else:
                        export_pdf_email_daily(report_data, out, lang=self.lang)
                else:
                    # Fallback to old simple summary
                    from shelly_analyzer.services.compute import load_device, summarize
                    from shelly_analyzer.core.energy import filter_by_time
                    unit_gross = float(self.cfg.pricing.unit_price_gross())
                    totals: List[ReportTotals] = []
                    for d in self.cfg.devices:
                        cd = load_device(self.storage, d)
                        df = filter_by_time(cd.df, start=pd.Timestamp(start_d), end=pd.Timestamp(end_excl))
                        kwh, avgp, maxp = summarize(df)
                        totals.append(ReportTotals(name=d.name, kwh_total=kwh, cost_eur=kwh * unit_gross, avg_power_w=avgp, max_power_w=maxp))
                    export_pdf_summary(
                        title=self.t("pdf.summary.title"),
                        period_label=f"{start_d} – {end_d}",
                        totals=totals,
                        out_path=out,
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
                        logo_path=getattr(self.cfg.billing, "invoice_logo_path", ""),
                    )
                    files.append({"name": out.name, "url": f"/files/web/invoices/{out.name}"})

                return {"ok": True, "files": files}

            if action == "export_excel":
                start = _pdate(params.get("start"))
                end = _pdate(params.get("end"))
                if start is not None and end is not None and end < start:
                    start, end = end, start

                from shelly_analyzer.services.compute import load_device
                from shelly_analyzer.core.energy import filter_by_time

                ts = time.strftime("%Y%m%d_%H%M%S")
                web_dir = out_root / "web"
                web_dir.mkdir(parents=True, exist_ok=True)
                sheets: Dict[str, Any] = {}
                for d in self.cfg.devices[:2]:
                    cd = load_device(self.storage, d)
                    df = filter_by_time(cd.df, start=start, end=end)
                    if not df.empty:
                        sheets[d.name[:31]] = df
                out = web_dir / f"export_{ts}.xlsx"
                export_to_excel(sheets, out)
                return {"ok": True, "files": [{"name": out.name, "url": f"/files/web/{out.name}"}]}

            # --- Cost data for web dashboard ---
            if action == "costs":
                try:
                    from datetime import datetime as _dt, timedelta as _td
                    from zoneinfo import ZoneInfo as _ZI
                    _tz = _ZI("Europe/Berlin")
                    _now = _dt.now(_tz)
                    _today_start = _now.replace(hour=0, minute=0, second=0, microsecond=0)
                    _week_start = _today_start - _td(days=_now.weekday())
                    _month_start = _today_start.replace(day=1)
                    _year_start = _today_start.replace(month=1, day=1)
                    _last_month_start = (_month_start - _td(days=1)).replace(day=1)

                    try:
                        _unit = float(self.cfg.pricing.unit_price_gross())
                    except Exception:
                        _unit = float(getattr(getattr(self.cfg, "pricing", None), "electricity_price_eur_per_kwh", 0.30) or 0.30)
                    _co2_g = float(getattr(getattr(self.cfg, "pricing", None), "co2_intensity_g_per_kwh", 380.0) or 0.0)

                    # Try real ENTSO-E CO₂ data
                    _co2_cfg = getattr(self.cfg, "co2", None)
                    _co2_zone = getattr(_co2_cfg, "bidding_zone", "DE_LU") or "DE_LU"
                    _use_entsoe = False
                    try:
                        _entsoe_token = getattr(_co2_cfg, "entsoe_token", "") or ""
                        if _entsoe_token and hasattr(self.storage, "db"):
                            _use_entsoe = True
                    except Exception:
                        pass

                    def _calc_co2(dev_key: str, rng_s, rng_e, kwh_fb: float) -> float:
                        """CO₂ (kg) from ENTSO-E hourly data; fallback to static."""
                        if _use_entsoe:
                            try:
                                s_ts = int(rng_s.timestamp())
                                e_ts = int(rng_e.timestamp())
                                db = self.storage.db
                                df_co2 = db.query_co2_intensity(_co2_zone, s_ts, e_ts + 3600)
                                if not df_co2.empty:
                                    df_h = db.query_hourly(dev_key, s_ts, e_ts + 3600)
                                    if df_h is not None and not df_h.empty:
                                        merged = pd.merge(
                                            df_h[["hour_ts", "kwh"]],
                                            df_co2[["hour_ts", "intensity_g_per_kwh"]],
                                            on="hour_ts", how="inner",
                                        )
                                        if not merged.empty:
                                            return float((merged["kwh"] * merged["intensity_g_per_kwh"]).sum()) / 1000.0
                            except Exception:
                                pass
                        if _co2_g > 0:
                            return kwh_fb * _co2_g / 1000.0
                        return 0.0

                    _ranges = {
                        "today": (_today_start, _now),
                        "week": (_week_start, _now),
                        "month": (_month_start, _now),
                        "year": (_year_start, _now),
                        "last_month": (_last_month_start, _month_start),
                    }

                    _three_phase = [d for d in (self.cfg.devices or [])
                                    if int(getattr(d, "phases", 3) or 3) >= 3
                                    and str(getattr(d, "kind", "em")) != "switch"]

                    devices_out = []
                    for d in _three_phase:
                        dev_data = {"key": d.key, "name": d.name, "host": d.host}
                        for rng_key, (rng_start, rng_end) in _ranges.items():
                            kwh = 0.0
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
                                            df["timestamp"] = df["timestamp"].dt.tz_convert(_tz)
                                        except Exception:
                                            pass
                                        m = (df["timestamp"] >= rng_start) & (df["timestamp"] < rng_end)
                                        kwh = float(pd.to_numeric(df.loc[m, "energy_kwh"], errors="coerce").fillna(0.0).sum())
                            except Exception:
                                pass
                            dev_data[rng_key + "_kwh"] = round(kwh, 3)
                            dev_data[rng_key + "_eur"] = round(kwh * _unit, 2)
                            dev_data[rng_key + "_co2_kg"] = round(_calc_co2(d.key, rng_start, rng_end, kwh), 3)

                        # Projection
                        try:
                            import calendar as _cal
                            _dim = _cal.monthrange(_now.year, _now.month)[1]
                            _elapsed = max(1, (_now - _month_start).total_seconds() / 86400.0)
                            _mk = dev_data.get("month_kwh", 0.0)
                            dev_data["proj_kwh"] = round(_mk / _elapsed * _dim, 1)
                            dev_data["proj_eur"] = round(dev_data["proj_kwh"] * _unit, 2)
                            _month_co2 = _calc_co2(d.key, _month_start, _now, _mk)
                            dev_data["proj_co2_kg"] = round(_month_co2 / _elapsed * _dim, 2) if _month_co2 > 0 else 0.0
                        except Exception:
                            dev_data["proj_kwh"] = 0.0
                            dev_data["proj_eur"] = 0.0
                            dev_data["proj_co2_kg"] = 0.0

                        # vs last month
                        _lm = dev_data.get("last_month_kwh", 0.0)
                        _cm = dev_data.get("month_kwh", 0.0)
                        if _lm > 0:
                            dev_data["vs_last_pct"] = round((_cm - _lm) / _lm * 100, 1)
                        else:
                            dev_data["vs_last_pct"] = None
                        dev_data["last_month_kwh"] = round(_lm, 3)
                        dev_data["last_month_eur"] = round(_lm * _unit, 2)

                        devices_out.append(dev_data)

                    # Solar CO₂ offset for the month (if solar configured)
                    _solar_co2_saved_month_kg = 0.0
                    try:
                        _solar_cfg_c = getattr(self.cfg, "solar", None)
                        _pv_key_c = str(getattr(_solar_cfg_c, "pv_meter_device_key", "") or "") if _solar_cfg_c else ""
                        if _pv_key_c and getattr(_solar_cfg_c, "enabled", False):
                            _pv_df_c = self.storage.db.query_hourly(_pv_key_c, start_ts=int(_month_start.timestamp()), end_ts=int(_now.timestamp()))
                            if _pv_df_c is not None and not _pv_df_c.empty and "kwh" in _pv_df_c.columns:
                                _kwh_col_c = pd.to_numeric(_pv_df_c["kwh"], errors="coerce").fillna(0.0)
                                _feed_in_c = float(_kwh_col_c[_kwh_col_c < 0].abs().sum())
                                _self_kwh_c = 0.0
                                _grid_c = float(_kwh_col_c[_kwh_col_c >= 0].sum())
                                _hh_c = sum(d.get("month_kwh", 0.0) for d in devices_out)
                                _self_kwh_c = max(0.0, _hh_c - _grid_c)
                                _pv_kwh_c = _self_kwh_c + _feed_in_c
                                _solar_co2_saved_month_kg = _pv_kwh_c * _co2_g / 1000.0
                    except Exception:
                        pass

                    return {"ok": True, "devices": devices_out, "unit_eur": _unit, "co2_g_per_kwh": _co2_g, "solar_co2_saved_month_kg": round(_solar_co2_saved_month_kg, 3)}
                except Exception as e:
                    return {"ok": False, "error": str(e)}

            # --- Report Button im Web-Control (professional PDF like email reports) ---
            if action == "report":
                period = str(params.get("period") or params.get("kind") or "day").strip().lower()
                anchor = _pdate(params.get("anchor"))
                if anchor is None:
                    anchor = pd.Timestamp(date.today())
                anchor = pd.Timestamp(anchor)

                from datetime import datetime as _dt_r, timedelta as _td_r
                from zoneinfo import ZoneInfo as _ZI_r
                _tz_r = _ZI_r("Europe/Berlin")

                is_monthly = period in {"month", "mon", "m"}

                if is_monthly:
                    start_d = anchor.replace(day=1).date()
                    if start_d.month == 12:
                        end_d = date(start_d.year + 1, 1, 1)
                    else:
                        end_d = date(start_d.year, start_d.month + 1, 1)
                    fname = f"energy_report_month_{start_d.strftime('%Y%m')}_{time.strftime('%H%M%S')}.pdf"
                    # Previous month for comparison
                    prev_end_d = start_d
                    prev_start_d = (start_d - _td_r(days=1)).replace(day=1)
                    report_type = "monthly"
                else:
                    start_d = anchor.date()
                    end_d = start_d + _td_r(days=1)
                    fname = f"energy_report_day_{start_d.strftime('%Y%m%d')}_{time.strftime('%H%M%S')}.pdf"
                    # Previous day for comparison
                    prev_start_d = start_d - _td_r(days=1)
                    prev_end_d = start_d
                    report_type = "daily"

                start_dt = _dt_r.combine(start_d, _dt_r.min.time(), tzinfo=_tz_r)
                end_dt = _dt_r.combine(end_d, _dt_r.min.time(), tzinfo=_tz_r)
                prev_start_dt = _dt_r.combine(prev_start_d, _dt_r.min.time(), tzinfo=_tz_r)
                prev_end_dt = _dt_r.combine(prev_end_d, _dt_r.min.time(), tzinfo=_tz_r)

                if progress:
                    try:
                        progress("report", 0, 3, "Daten sammeln …")
                    except Exception:
                        pass

                # Build rich report data (same as email reports)
                report_data = self._build_email_report_data(
                    start_dt, end_dt,
                    prev_start_dt=prev_start_dt,
                    prev_end_dt=prev_end_dt,
                    report_type=report_type,
                )

                if progress:
                    try:
                        progress("report", 1, 3, "PDF erzeugen …")
                    except Exception:
                        pass

                rep_dir = out_root / "web" / "reports"
                rep_dir.mkdir(parents=True, exist_ok=True)
                out_path_r = rep_dir / fname

                if report_data is not None:
                    from shelly_analyzer.services.export import export_pdf_email_daily, export_pdf_email_monthly
                    if is_monthly:
                        export_pdf_email_monthly(report_data, out_path_r, lang=self.lang)
                    else:
                        export_pdf_email_daily(report_data, out_path_r, lang=self.lang)
                else:
                    # Fallback to old report if _build_email_report_data failed
                    from shelly_analyzer.services.compute import load_device
                    from shelly_analyzer.core.energy import filter_by_time
                    from shelly_analyzer.services.export import export_pdf_energy_report_variant1
                    devices_payload: List[Tuple[str, str, pd.DataFrame]] = []
                    for d in self.cfg.devices:
                        cd = load_device(self.storage, d)
                        df_use = filter_by_time(cd.df, start=pd.Timestamp(start_d), end=pd.Timestamp(end_d))
                        devices_payload.append((d.key, d.name, df_use))
                    try:
                        unit_gross = float(self.cfg.pricing.unit_price_gross())
                    except Exception:
                        unit_gross = 0.30
                    export_pdf_energy_report_variant1(
                        out_path=out_path_r,
                        title=self.t("pdf.report.title.month") if is_monthly else self.t("pdf.report.title.day"),
                        period_label=f"{start_d} – {end_d}",
                        pricing_note="",
                        unit_price_gross=unit_gross,
                        devices=devices_payload,
                        lang=self.lang,
                    )

                if progress:
                    try:
                        progress("report", 3, 3, "OK")
                    except Exception:
                        pass

                period_label = f"{format_date_local(self.lang, pd.Timestamp(start_d))} – {format_date_local(self.lang, pd.Timestamp(end_d - _td_r(days=0 if is_monthly else 0)))}"
                return {
                    "ok": True,
                    "period": period,
                    "period_label": period_label,
                    "files": [{"name": out_path_r.name, "url": f"/files/web/reports/{out_path_r.name}"}],
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

            # --- Heatmap data for web dashboard ---
            if action == "heatmap":
                try:
                    from datetime import datetime as _dt2
                    device_key = str(params.get("device") or "").strip()
                    try:
                        year = int(params.get("year") or _dt2.now().year)
                    except Exception:
                        year = _dt2.now().year
                    unit = str(params.get("unit") or "kWh").strip()
                    use_eur = (unit.lower() in ("eur", "€", "euro"))
                    use_co2 = (unit.lower() in ("co2", "g co₂", "gco2"))
                    try:
                        _unit_price = float(self.cfg.pricing.unit_price_gross())
                    except Exception:
                        _unit_price = 0.30

                    # Find device (fallback to first device)
                    if not device_key and self.cfg.devices:
                        device_key = self.cfg.devices[0].key
                    if not device_key:
                        return {"ok": False, "error": "no device"}

                    start_ts = int(_dt2(year, 1, 1).timestamp())
                    end_ts = int(_dt2(year, 12, 31, 23, 59, 59).timestamp())

                    # Load hourly data
                    try:
                        hourly_df = self.storage.db.query_hourly(device_key, start_ts=start_ts, end_ts=end_ts)
                    except Exception:
                        hourly_df = None

                    # Load CO₂ intensity data if needed
                    co2_intensity_map: Dict[int, float] = {}
                    _co2_fallback_g = 0.0
                    if use_co2:
                        try:
                            _co2_fallback_g = float(getattr(getattr(self.cfg, "pricing", None), "co2_intensity_g_per_kwh", 380.0) or 380.0)
                        except Exception:
                            _co2_fallback_g = 380.0
                        try:
                            _co2_cfg_hm = getattr(self.cfg, "co2", None)
                            _zone_hm = getattr(_co2_cfg_hm, "bidding_zone", "DE_LU") or "DE_LU"
                            _token_hm = getattr(_co2_cfg_hm, "entsoe_token", "") or ""
                            if _token_hm and hasattr(self.storage, "db"):
                                df_co2_hm = self.storage.db.query_co2_intensity(_zone_hm, start_ts, end_ts + 3600)
                                if not df_co2_hm.empty:
                                    for _, r in df_co2_hm.iterrows():
                                        co2_intensity_map[int(r["hour_ts"])] = float(r["intensity_g_per_kwh"])
                        except Exception:
                            pass

                    calendar_data: List[Dict[str, Any]] = []
                    hourly_matrix: Dict[int, Dict[int, float]] = {wd: {h: 0.0 for h in range(24)} for wd in range(7)}
                    hourly_counts: Dict[int, Dict[int, int]] = {wd: {h: 0 for h in range(24)} for wd in range(7)}

                    if hourly_df is not None and not hourly_df.empty and "hour_ts" in hourly_df.columns and "kwh" in hourly_df.columns:
                        # Calendar: aggregate to daily
                        daily_totals: Dict[str, float] = {}
                        for _, row in hourly_df.iterrows():
                            try:
                                ts_val = int(row["hour_ts"])
                                kwh_val = float(row["kwh"] or 0.0)
                                dt_local = _dt2.fromtimestamp(ts_val)
                                date_str = dt_local.strftime("%Y-%m-%d")

                                if use_co2:
                                    intensity = co2_intensity_map.get(ts_val, _co2_fallback_g)
                                    val_h = kwh_val * intensity  # g CO₂
                                else:
                                    val_h = kwh_val

                                daily_totals[date_str] = daily_totals.get(date_str, 0.0) + val_h
                                # Hourly matrix: weekday (0=Mon) × hour
                                wd = dt_local.weekday()  # 0=Mon
                                h = dt_local.hour
                                hourly_matrix[wd][h] += val_h
                                hourly_counts[wd][h] += 1
                            except Exception:
                                continue

                        for date_str, total_val in daily_totals.items():
                            if use_eur:
                                val = total_val * _unit_price
                            elif use_co2:
                                val = total_val  # already in g CO₂
                            else:
                                val = total_val
                            calendar_data.append({"date": date_str, "value": round(val, 3)})

                    # Build hourly matrix (averages) for weekday×hour heatmap
                    hourly_out: Dict[str, Dict[str, float]] = {}
                    for wd in range(7):
                        hourly_out[str(wd)] = {}
                        for h in range(24):
                            cnt = hourly_counts[wd][h]
                            avg_val = (hourly_matrix[wd][h] / cnt) if cnt > 0 else 0.0
                            if use_eur and not use_co2:
                                val = avg_val * _unit_price
                            else:
                                val = avg_val
                            hourly_out[str(wd)][str(h)] = round(val, 4)

                    devices_list = [{"key": d.key, "name": d.name} for d in self.cfg.devices]
                    return {
                        "ok": True,
                        "device_key": device_key,
                        "year": year,
                        "unit": unit,
                        "calendar": calendar_data,
                        "hourly": hourly_out,
                        "devices": devices_list,
                    }
                except Exception as e:
                    return {"ok": False, "error": str(e)}

            # --- Save solar config from web ---
            if action == "save_solar_config":
                try:
                    from shelly_analyzer.io.config import SolarConfig as _SC
                    _old = getattr(self.cfg, "solar", _SC())
                    _new = _SC(
                        enabled=bool(params.get("enabled", getattr(_old, "enabled", False))),
                        pv_meter_device_key=str(params.get("pv_meter_device_key", getattr(_old, "pv_meter_device_key", "")) or ""),
                        feed_in_tariff_eur_per_kwh=float(params.get("feed_in_tariff", getattr(_old, "feed_in_tariff_eur_per_kwh", 0.082))),
                        kw_peak=float(params.get("kw_peak", getattr(_old, "kw_peak", 0.0))),
                        battery_kwh=float(params.get("battery_kwh", getattr(_old, "battery_kwh", 0.0))),
                        co2_production_kg_per_kwp=float(params.get("co2_production_kg_per_kwp", getattr(_old, "co2_production_kg_per_kwp", 1000.0))),
                    )
                    import dataclasses
                    self.cfg = dataclasses.replace(self.cfg, solar=_new)
                    save_config(self.cfg, self.cfg_path)
                    return {"ok": True}
                except Exception as e:
                    return {"ok": False, "error": str(e)}

            # --- Solar data for web dashboard ---
            if action == "solar":
                try:
                    solar_cfg = getattr(self.cfg, "solar", None)
                    _all_devs_s = [{"key": d.key, "name": d.name} for d in self.cfg.devices]
                    _scfg_resp = {
                        "enabled": bool(getattr(solar_cfg, "enabled", False)) if solar_cfg else False,
                        "pv_meter_device_key": str(getattr(solar_cfg, "pv_meter_device_key", "") or "") if solar_cfg else "",
                        "feed_in_tariff": float(getattr(solar_cfg, "feed_in_tariff_eur_per_kwh", 0.082)) if solar_cfg else 0.082,
                        "kw_peak": float(getattr(solar_cfg, "kw_peak", 0.0) or 0.0) if solar_cfg else 0.0,
                        "battery_kwh": float(getattr(solar_cfg, "battery_kwh", 0.0) or 0.0) if solar_cfg else 0.0,
                        "co2_production_kg_per_kwp": float(getattr(solar_cfg, "co2_production_kg_per_kwp", 1000.0) or 1000.0) if solar_cfg else 1000.0,
                    }
                    if solar_cfg is None or not getattr(solar_cfg, "enabled", False):
                        return {"ok": True, "configured": False, "devices": _all_devs_s, "config": _scfg_resp}
                    pv_key = str(getattr(solar_cfg, "pv_meter_device_key", "") or "")
                    if not pv_key:
                        return {"ok": True, "configured": False, "devices": _all_devs_s, "config": _scfg_resp}

                    period = str(params.get("period") or "today").strip()
                    from datetime import datetime as _dt3, timedelta as _td3
                    from zoneinfo import ZoneInfo as _ZI3
                    _tz3 = _ZI3("Europe/Berlin")
                    _now3 = _dt3.now(_tz3)
                    _today3 = _now3.replace(hour=0, minute=0, second=0, microsecond=0)
                    if period == "week":
                        _start3 = _today3 - _td3(days=_now3.weekday())
                        _end3 = _now3
                    elif period == "month":
                        _start3 = _today3.replace(day=1)
                        _end3 = _now3
                    elif period == "year":
                        _start3 = _today3.replace(month=1, day=1)
                        _end3 = _now3
                    else:  # today
                        _start3 = _today3
                        _end3 = _now3

                    start_ts3 = int(_start3.timestamp())
                    end_ts3 = int(_end3.timestamp())

                    def _load_hourly_kwh(dev_key: str) -> float:
                        try:
                            df = self.storage.db.query_hourly(dev_key, start_ts=start_ts3, end_ts=end_ts3)
                            if df is not None and not df.empty and "kwh" in df.columns:
                                return float(pd.to_numeric(df["kwh"], errors="coerce").fillna(0.0).sum())
                        except Exception:
                            pass
                        return 0.0

                    # PV meter: split feed-in (negative power) vs grid import (positive)
                    feed_in_kwh = 0.0
                    grid_kwh = 0.0
                    try:
                        pv_df = self.storage.db.query_hourly(pv_key, start_ts=start_ts3, end_ts=end_ts3)
                        if pv_df is not None and not pv_df.empty and "kwh" in pv_df.columns:
                            kwh_col = pd.to_numeric(pv_df["kwh"], errors="coerce").fillna(0.0)
                            feed_in_kwh = float(kwh_col[kwh_col < 0].abs().sum())
                            grid_kwh = float(kwh_col[kwh_col >= 0].sum())
                    except Exception:
                        pass

                    # Household consumption (all non-PV devices)
                    household_kwh = 0.0
                    for d in self.cfg.devices:
                        if d.key == pv_key:
                            continue
                        household_kwh += _load_hourly_kwh(d.key)

                    self_kwh = max(0.0, household_kwh - grid_kwh) if household_kwh > 0 else 0.0
                    pv_kwh = self_kwh + feed_in_kwh
                    autarky_pct = (min(100.0, self_kwh / household_kwh * 100.0) if household_kwh > 0 else 0.0)

                    try:
                        feed_in_tariff = float(getattr(solar_cfg, "feed_in_tariff_eur_per_kwh", 0.082))
                        unit_price = float(self.cfg.pricing.unit_price_gross())
                    except Exception:
                        feed_in_tariff = 0.082
                        unit_price = 0.30

                    # CO₂ savings: PV production displaces grid electricity
                    co2_g_per_kwh = float(getattr(getattr(self.cfg, "pricing", None), "co2_intensity_g_per_kwh", 380.0) or 380.0)
                    # Try ENTSO-E average intensity for the period
                    _co2_source = "static"
                    try:
                        _co2_cfg_s = getattr(self.cfg, "co2", None)
                        _co2_zone_s = getattr(_co2_cfg_s, "bidding_zone", "DE_LU") or "DE_LU"
                        _co2_token_s = getattr(_co2_cfg_s, "entsoe_token", "") or ""
                        if _co2_token_s and hasattr(self.storage, "db"):
                            df_co2_s = self.storage.db.query_co2_intensity(_co2_zone_s, start_ts3, end_ts3 + 3600)
                            if df_co2_s is not None and not df_co2_s.empty and "intensity_g_per_kwh" in df_co2_s.columns:
                                avg_int = float(pd.to_numeric(df_co2_s["intensity_g_per_kwh"], errors="coerce").mean())
                                if avg_int > 0:
                                    co2_g_per_kwh = avg_int
                                    _co2_source = "entsoe"
                    except Exception:
                        pass

                    # CO₂ saved = PV production * grid intensity (what would have been emitted)
                    co2_saved_kg = pv_kwh * co2_g_per_kwh / 1000.0
                    # Grid CO₂ (what the household actually caused)
                    co2_grid_kg = grid_kwh * co2_g_per_kwh / 1000.0

                    # System info from config
                    kw_peak = float(getattr(solar_cfg, "kw_peak", 0.0) or 0.0)
                    battery_kwh_cfg = float(getattr(solar_cfg, "battery_kwh", 0.0) or 0.0)
                    co2_prod_per_kwp = float(getattr(solar_cfg, "co2_production_kg_per_kwp", 1000.0) or 1000.0)
                    # Lifetime CO₂ amortization: total embodied CO₂ vs total saved
                    co2_embodied_kg = kw_peak * co2_prod_per_kwp if kw_peak > 0 else 0.0

                    _all_devices = [{"key": d.key, "name": d.name} for d in self.cfg.devices]
                    return {
                        "ok": True,
                        "configured": True,
                        "period": period,
                        "feed_in_kwh": round(feed_in_kwh, 3),
                        "grid_kwh": round(grid_kwh, 3),
                        "self_kwh": round(self_kwh, 3),
                        "pv_kwh": round(pv_kwh, 3),
                        "autarky_pct": round(autarky_pct, 1),
                        "household_kwh": round(household_kwh, 3),
                        "revenue_eur": round(feed_in_kwh * feed_in_tariff, 2),
                        "savings_eur": round(self_kwh * unit_price, 2),
                        "co2_saved_kg": round(co2_saved_kg, 3),
                        "co2_grid_kg": round(co2_grid_kg, 3),
                        "co2_intensity_g_per_kwh": round(co2_g_per_kwh, 1),
                        "co2_source": _co2_source,
                        "kw_peak": round(kw_peak, 2),
                        "battery_kwh": round(battery_kwh_cfg, 1),
                        "co2_embodied_kg": round(co2_embodied_kg, 1),
                        "co2_production_kg_per_kwp": round(co2_prod_per_kwp, 0),
                        "feed_in_tariff": round(feed_in_tariff, 4),
                        "pv_meter_device_key": pv_key,
                        "devices": _all_devices,
                    }
                except Exception as e:
                    return {"ok": False, "error": str(e)}

            # --- Compare data for web dashboard ---
            if action == "compare":
                try:
                    from datetime import date as _date4, datetime as _dt4, timedelta as _td4
                    device_a = str(params.get("device_a") or "").strip()
                    device_b = str(params.get("device_b") or "").strip()
                    if not device_a and self.cfg.devices:
                        device_a = self.cfg.devices[0].key
                    if not device_b and self.cfg.devices:
                        device_b = self.cfg.devices[0].key

                    def _pdate4(s: Any) -> Optional[_date4]:
                        try:
                            return _parse_date_flexible(str(s or "").strip()).date()
                        except Exception:
                            return None

                    today4 = _date4.today()
                    jan1 = _date4(today4.year, 1, 1)
                    jan1_last = _date4(today4.year - 1, 1, 1)
                    dec31_last = _date4(today4.year - 1, 12, 31)

                    from_a = _pdate4(params.get("from_a")) or jan1
                    to_a = _pdate4(params.get("to_a")) or (today4 - _td4(days=1))
                    from_b = _pdate4(params.get("from_b")) or jan1_last
                    to_b = _pdate4(params.get("to_b")) or dec31_last

                    if to_a < from_a:
                        from_a, to_a = to_a, from_a
                    if to_b < from_b:
                        from_b, to_b = to_b, from_b

                    unit = str(params.get("unit") or "kWh").strip()
                    use_eur = unit.lower() in ("eur", "€", "euro")
                    try:
                        _price4 = float(self.cfg.pricing.unit_price_gross())
                    except Exception:
                        _price4 = 0.30

                    gran = str(params.get("gran") or "total").strip()

                    # Quick preset expansion
                    preset = str(params.get("preset") or "").strip()
                    if preset:
                        import calendar as _cal4
                        _now4 = today4
                        if preset == "month":
                            _ms = _date4(_now4.year, _now4.month, 1)
                            _lms = (_ms - _td4(days=1)).replace(day=1)
                            _lme = _ms - _td4(days=1)
                            from_a, to_a = _ms, _now4
                            from_b, to_b = _lms, _lme
                        elif preset == "quarter":
                            _q = (_now4.month - 1) // 3
                            _qs = _date4(_now4.year, _q * 3 + 1, 1)
                            _lqs_y = _now4.year if _q > 0 else _now4.year - 1
                            _lqs_m = (_q - 1) * 3 + 1 if _q > 0 else 10
                            _lqs = _date4(_lqs_y, _lqs_m, 1)
                            _lqe = _qs - _td4(days=1)
                            from_a, to_a = _qs, _now4
                            from_b, to_b = _lqs, _lqe
                        elif preset == "halfyear":
                            _hs = _date4(_now4.year, 1, 1) if _now4.month <= 6 else _date4(_now4.year, 7, 1)
                            _lhs = (_hs - _td4(days=1)).replace(day=1)
                            if _lhs.month > 6:
                                _lhs = _date4(_lhs.year, 7, 1)
                            else:
                                _lhs = _date4(_lhs.year, 1, 1)
                            _lhe = _hs - _td4(days=1)
                            from_a, to_a = _hs, _now4
                            from_b, to_b = _lhs, _lhe
                        elif preset == "year":
                            from_a = _date4(_now4.year, 1, 1)
                            to_a = _now4
                            from_b = _date4(_now4.year - 1, 1, 1)
                            to_b = _date4(_now4.year - 1, 12, 31)

                    # Load daily data using existing helper method
                    daily_a = self._cmp_load_daily(device_a, from_a, to_a, use_eur, _price4)
                    daily_b = self._cmp_load_daily(device_b, from_b, to_b, use_eur, _price4)

                    total_a = sum(daily_a.values())
                    total_b = sum(daily_b.values())
                    delta = total_a - total_b
                    delta_pct = ((delta / total_b * 100.0) if total_b > 0 else 0.0)

                    # Build aligned series for the chart
                    if gran == "monthly":
                        vals_a, vals_b, labels = self._cmp_align_monthly(daily_a, from_a, to_a, daily_b, from_b, to_b)
                    elif gran == "weekly":
                        vals_a, vals_b, labels = self._cmp_align_weekly(daily_a, from_a, to_a, daily_b, from_b, to_b)
                    elif gran == "daily":
                        vals_a, vals_b, labels = self._cmp_align_daily(daily_a, from_a, to_a, daily_b, from_b, to_b)
                    else:  # total
                        vals_a, vals_b, labels = [total_a], [total_b], ["A vs B"]

                    return {
                        "ok": True,
                        "device_a": device_a,
                        "device_b": device_b,
                        "from_a": from_a.isoformat(),
                        "to_a": to_a.isoformat(),
                        "from_b": from_b.isoformat(),
                        "to_b": to_b.isoformat(),
                        "unit": unit,
                        "gran": gran,
                        "labels": labels,
                        "values_a": [round(v, 3) for v in vals_a],
                        "values_b": [round(v, 3) for v in vals_b],
                        "total_a": round(total_a, 3),
                        "total_b": round(total_b, 3),
                        "delta": round(delta, 3),
                        "delta_pct": round(delta_pct, 1),
                    }
                except Exception as e:
                    return {"ok": False, "error": str(e)}

            # --- Anomaly data for web dashboard ---
            if action == "anomalies":
                try:
                    anom_cfg = getattr(self.cfg, "anomaly", None)
                    enabled = bool(getattr(anom_cfg, "enabled", False)) if anom_cfg else False

                    # Use the desktop app's anomaly log (shared state)
                    log = getattr(self, "_anomaly_log", [])
                    all_events = []
                    for ev in log:
                        ts_str = ""
                        try:
                            ts_str = ev.timestamp.isoformat() if hasattr(ev.timestamp, "isoformat") else str(ev.timestamp)
                        except Exception:
                            ts_str = str(getattr(ev, "timestamp", ""))
                        all_events.append({
                            "event_id": ev.event_id,
                            "timestamp": ts_str,
                            "device_key": ev.device_key,
                            "device_name": ev.device_name,
                            "anomaly_type": ev.anomaly_type,
                            "type": ev.anomaly_type,
                            "value": round(ev.value, 3),
                            "sigma_count": round(ev.sigma_count, 2),
                            "sigma": round(ev.sigma_count, 2),
                            "description": ev.description,
                        })

                    # If log is empty but detection is enabled, run once
                    if not all_events and enabled:
                        try:
                            from shelly_analyzer.services.anomaly import detect_anomalies as _detect
                            for d in self.cfg.devices:
                                try:
                                    cd = load_device(self.storage, d)
                                    if cd is None or cd.df is None or cd.df.empty:
                                        continue
                                    events = _detect(
                                        cd.df, d.key, d.name,
                                        sigma=float(getattr(anom_cfg, "sigma_threshold", 2.0)),
                                        min_deviation_kwh=float(getattr(anom_cfg, "min_deviation_kwh", 0.1)),
                                        window_days=int(getattr(anom_cfg, "window_days", 30)),
                                    )
                                    for ev in events:
                                        all_events.append({
                                            "event_id": ev.event_id,
                                            "timestamp": ev.timestamp.isoformat() if hasattr(ev.timestamp, "isoformat") else str(ev.timestamp),
                                            "device_key": ev.device_key,
                                            "device_name": ev.device_name,
                                            "anomaly_type": ev.anomaly_type,
                                            "type": ev.anomaly_type,
                                            "value": round(ev.value, 3),
                                            "sigma_count": round(ev.sigma_count, 2),
                                            "sigma": round(ev.sigma_count, 2),
                                            "description": ev.description,
                                        })
                                except Exception:
                                    continue
                        except Exception:
                            pass

                    all_events.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
                    return {
                        "ok": True,
                        "enabled": enabled,
                        "events": all_events[:200],
                    }
                except Exception as e:
                    return {"ok": False, "error": str(e)}

            if action == "co2_live":
                try:
                    co2_cfg = getattr(self.cfg, "co2", None)
                    if not co2_cfg or not getattr(co2_cfg, "enabled", False):
                        return {"ok": True, "enabled": False}
                    zone = str(getattr(co2_cfg, "bidding_zone", "DE_LU") or "DE_LU")
                    green_thr = float(getattr(co2_cfg, "green_threshold_g_per_kwh", 150.0))
                    dirty_thr = float(getattr(co2_cfg, "dirty_threshold_g_per_kwh", 400.0))

                    now_ts = int(time.time())
                    # Query last 3 hours to handle ENTSO-E fetch lag
                    range_start = ((now_ts // 3600) - 2) * 3600
                    df_now = self.storage.db.query_co2_intensity(zone, range_start, now_ts + 3600)
                    ci = 0.0
                    if df_now is not None and not df_now.empty:
                        ci = float(df_now.iloc[-1].get("intensity_g_per_kwh", 0))

                    device_rates = []
                    if ci > 0:
                        live_snap = {}
                        try:
                            store = getattr(self, "_live_state_store", None)
                            if store is not None:
                                snap = store.snapshot()
                                for dk, points in snap.items():
                                    if points:
                                        live_snap[dk] = points[-1].get("power_total_w", 0.0)
                        except Exception:
                            pass
                        for d in self.cfg.devices:
                            watts = abs(live_snap.get(d.key, 0.0))
                            co2_g_h = watts * ci / 1000.0
                            device_rates.append({
                                "key": d.key,
                                "name": d.name,
                                "watts": round(watts, 0),
                                "co2_g_h": round(co2_g_h, 1),
                            })

                    return {
                        "ok": True,
                        "current_intensity": round(ci, 1),
                        "green_threshold": green_thr,
                        "dirty_threshold": dirty_thr,
                        "device_rates": device_rates,
                    }
                except Exception as e:
                    return {"ok": False, "error": str(e)}

            if action == "forecast":
                try:
                    from shelly_analyzer.services.forecast import compute_forecast
                    dk = str(params.get("device_key", "") or "")
                    if not dk and self.cfg.devices:
                        dk = self.cfg.devices[0].key
                    dev_name = dk
                    for d in self.cfg.devices:
                        if d.key == dk:
                            dev_name = d.name
                            break
                    fc_cfg = getattr(self.cfg, "forecast", None)
                    price = self.cfg.pricing.unit_price_gross()
                    r = compute_forecast(
                        self.storage.db, dk, dev_name,
                        horizon_days=int(getattr(fc_cfg, "horizon_days", 30)) if fc_cfg else 30,
                        price_eur_per_kwh=price,
                        history_days=int(getattr(fc_cfg, "history_days", 90)) if fc_cfg else 90,
                    )
                    if r is None:
                        return {"ok": True, "no_data": True}
                    return {
                        "ok": True,
                        "device_key": r.device_key,
                        "device_name": r.device_name,
                        "avg_daily_kwh": r.avg_daily_kwh,
                        "trend_pct_per_month": r.trend_pct_per_month,
                        "forecast_next_month_kwh": r.forecast_next_month_kwh,
                        "forecast_next_month_cost": r.forecast_next_month_cost,
                        "forecast_year_kwh": r.forecast_year_kwh,
                        "forecast_year_cost": r.forecast_year_cost,
                        "history_dates": [str(d) for d in r.history_dates],
                        "history_kwh": r.history_kwh,
                        "forecast_dates": [str(d) for d in r.forecast_dates],
                        "forecast_kwh": r.forecast_kwh,
                        "forecast_upper": r.forecast_upper,
                        "forecast_lower": r.forecast_lower,
                        "weekday_profile": {str(k): v for k, v in r.weekday_profile.items()},
                        "hourly_profile": {str(k): v for k, v in r.hourly_profile.items()},
                    }
                except Exception as e:
                    return {"ok": False, "error": str(e)}

            if action == "standby":
                try:
                    from shelly_analyzer.services.standby import generate_standby_report
                    price = self.cfg.pricing.unit_price_gross()
                    report = generate_standby_report(self.storage.db, self.cfg.devices, price)
                    return {
                        "ok": True,
                        "total_annual_standby_kwh": report.total_annual_standby_kwh,
                        "total_annual_standby_cost": report.total_annual_standby_cost,
                        "analysis_days": report.analysis_days,
                        "devices": [
                            {
                                "device_key": d.device_key,
                                "device_name": d.device_name,
                                "base_load_w": d.base_load_w,
                                "annual_standby_kwh": d.annual_standby_kwh,
                                "annual_standby_cost": d.annual_standby_cost,
                                "standby_share_pct": d.standby_share_pct,
                                "risk": d.risk,
                                "hourly_profile": d.hourly_profile,
                            }
                            for d in report.devices
                        ],
                    }
                except Exception as e:
                    return {"ok": False, "error": str(e)}

            if action == "sankey":
                try:
                    from shelly_analyzer.services.sankey import compute_sankey, sankey_to_plotly_dict
                    period = str(params.get("period", "today") or "today")
                    data = compute_sankey(self.storage.db, self.cfg.devices, self.cfg.solar, period)
                    plotly_data = sankey_to_plotly_dict(data)
                    return {
                        "ok": True,
                        "grid_import_kwh": data.grid_import_kwh,
                        "pv_production_kwh": data.pv_production_kwh,
                        "self_consumption_kwh": data.self_consumption_kwh,
                        "feed_in_kwh": data.feed_in_kwh,
                        "total_consumption_kwh": data.total_consumption_kwh,
                        "sankey": plotly_data,
                    }
                except Exception as e:
                    return {"ok": False, "error": str(e)}

            if action == "co2":
                try:
                    from datetime import datetime as _dtc, timedelta as _tdc
                    from zoneinfo import ZoneInfo as _ZIc

                    co2_cfg = getattr(self.cfg, "co2", None)
                    enabled = bool(getattr(co2_cfg, "enabled", False)) if co2_cfg else False
                    if not enabled:
                        return {"ok": True, "enabled": False}

                    zone = str(getattr(co2_cfg, "bidding_zone", "DE_LU") or "DE_LU")
                    green_thr = float(getattr(co2_cfg, "green_threshold_g_per_kwh", 150.0))
                    dirty_thr = float(getattr(co2_cfg, "dirty_threshold_g_per_kwh", 400.0))
                    cross_border = bool(getattr(co2_cfg, "cross_border_flows", False))

                    _tzc = _ZIc("Europe/Berlin")
                    _nowc = _dtc.now(_tzc)
                    _todayc = _nowc.replace(hour=0, minute=0, second=0, microsecond=0)
                    _week_start = _todayc - _tdc(days=_nowc.weekday())
                    _month_start = _todayc.replace(day=1)
                    _year_start = _todayc.replace(month=1, day=1)

                    today_start_ts = int(_todayc.timestamp())
                    week_start_ts = int(_week_start.timestamp())
                    month_start_ts = int(_month_start.timestamp())
                    year_start_ts = int(_year_start.timestamp())
                    now_ts = int(_nowc.timestamp())

                    # Intensity data for chart (range selectable via query param)
                    co2_range = str(params.get("range", "24h")) if params else "24h"
                    if co2_range == "7d":
                        h24_start = now_ts - 7 * 86400
                    elif co2_range == "30d":
                        h24_start = now_ts - 30 * 86400
                    elif co2_range == "all":
                        oldest = self.storage.db.oldest_co2_ts(zone)
                        h24_start = oldest if oldest else now_ts - 24 * 3600
                    else:
                        h24_start = now_ts - 24 * 3600
                    h24_start = (h24_start // 3600) * 3600
                    df_24h = self.storage.db.query_co2_intensity(zone, h24_start, now_ts + 3600)

                    hourly_data = []
                    current_intensity = 0.0
                    current_source = "unknown"
                    if df_24h is not None and not df_24h.empty:
                        for _, row in df_24h.iterrows():
                            ts = int(row.get("hour_ts", 0))
                            intensity = float(row.get("intensity_g_per_kwh", 0))
                            source = str(row.get("source", ""))
                            if co2_range in ("7d", "30d", "all"):
                                hour_str = _dtc.fromtimestamp(ts, tz=_tzc).strftime("%d.%m %H:%M")
                            else:
                                hour_str = _dtc.fromtimestamp(ts, tz=_tzc).strftime("%H:%M")
                            hourly_data.append({
                                "hour": hour_str,
                                "ts": ts,
                                "intensity": round(intensity, 1),
                                "source": source,
                            })
                        # Current = most recent hour
                        last_row = df_24h.iloc[-1]
                        current_intensity = float(last_row.get("intensity_g_per_kwh", 0))
                        current_source = str(last_row.get("source", ""))

                    # CO₂ per device for today/week/month
                    # Solar config for CO₂ offset
                    _solar_cfg = getattr(self.cfg, "solar", None)
                    _pv_key = getattr(_solar_cfg, "pv_meter_device_key", "") if _solar_cfg else ""
                    _pv_on = bool(getattr(_solar_cfg, "enabled", False)) if _solar_cfg else False

                    def _device_co2(start_ts_d, end_ts_d):
                        """Calculate net CO₂ kg (grid only, PV offset subtracted)."""
                        grid_kwh = 0.0
                        pv_saved_kwh = 0.0
                        df_co2 = self.storage.db.query_co2_intensity(zone, start_ts_d, end_ts_d + 3600)
                        if df_co2 is None or df_co2.empty:
                            return 0.0
                        avg_int = float(pd.to_numeric(df_co2["intensity_g_per_kwh"], errors="coerce").mean())
                        if avg_int <= 0:
                            return 0.0
                        for d in self.cfg.devices:
                            try:
                                df_h = self.storage.db.query_hourly(d.key, start_ts=start_ts_d, end_ts=end_ts_d)
                                if df_h is None or df_h.empty or "kwh" not in df_h.columns:
                                    continue
                                if _pv_on and d.key == _pv_key:
                                    # PV meter: positive=grid import, negative=feed-in
                                    kwh_vals = pd.to_numeric(df_h["kwh"], errors="coerce").fillna(0.0)
                                    grid_kwh += float(kwh_vals.clip(lower=0).sum())
                                    pv_saved_kwh += float(kwh_vals.clip(upper=0).abs().sum())
                                else:
                                    grid_kwh += float(pd.to_numeric(df_h["kwh"], errors="coerce").fillna(0.0).clip(lower=0).sum())
                            except Exception:
                                pass
                        net_kg = max(0.0, (grid_kwh - pv_saved_kwh) * avg_int / 1000.0)
                        return net_kg

                    co2_today = _device_co2(today_start_ts, now_ts)
                    co2_week = _device_co2(week_start_ts, now_ts)
                    co2_month = _device_co2(month_start_ts, now_ts)
                    co2_year = _device_co2(year_start_ts, now_ts)

                    # Per-device live CO₂ rate (g/h) from live poller data
                    device_rates = []
                    if current_intensity > 0:
                        live_snap = {}
                        try:
                            store = getattr(self, "_live_state_store", None)
                            if store is not None:
                                snap = store.snapshot()
                                for dk, points in snap.items():
                                    if points:
                                        live_snap[dk] = points[-1].get("power_total_w", 0.0)
                        except Exception:
                            pass
                        for d in self.cfg.devices:
                            watts = abs(live_snap.get(d.key, 0.0))
                            co2_g_h = watts * current_intensity / 1000.0
                            device_rates.append({
                                "key": d.key,
                                "name": d.name,
                                "watts": round(watts, 0),
                                "co2_g_h": round(co2_g_h, 1),
                            })

                    # Fuel mix from Co2FetchService (in-memory) or DB fallback
                    fuel_mix = {}
                    fuel_mix_hour = None
                    try:
                        mix_hour, mix_data = None, {}
                        svc = getattr(self, "_co2_fetch_svc", None)
                        if svc is not None:
                            mix_hour, mix_data = svc.get_latest_mix()
                        if not mix_data:
                            mix_hour, mix_data = self.storage.db.query_latest_fuel_mix(zone)
                        if mix_hour and mix_data:
                            fuel_mix_hour = _dtc.fromtimestamp(mix_hour, tz=_tzc).strftime("%H:%M")
                            from shelly_analyzer.services.entsoe import _CO2_FACTORS, FUEL_DISPLAY_NAMES
                            total_mw = sum(mix_data.values())
                            for fuel, mw in sorted(mix_data.items(), key=lambda x: -x[1]):
                                if mw > 0:
                                    fuel_mix[fuel] = {
                                        "name": FUEL_DISPLAY_NAMES.get(fuel, fuel),
                                        "mw": round(mw, 0),
                                        "share_pct": round(mw / total_mw * 100, 1) if total_mw > 0 else 0,
                                        "factor": _CO2_FACTORS.get(fuel, 400.0),
                                    }
                    except Exception:
                        pass

                    # 24h rolling CO₂ per 3-phase device (hourly bars)
                    device_hourly_co2 = []
                    df_co2_24h = self.storage.db.query_co2_intensity(zone, h24_start, now_ts + 3600)
                    co2_by_hour = {}
                    if df_co2_24h is not None and not df_co2_24h.empty:
                        for _, row in df_co2_24h.iterrows():
                            co2_by_hour[int(row["hour_ts"])] = float(row["intensity_g_per_kwh"])
                    for d in self.cfg.devices:
                        if int(getattr(d, "phases", 3) or 3) < 3:
                            continue
                        if str(getattr(d, "kind", "em")) == "switch":
                            continue
                        try:
                            df_h = self.storage.db.query_hourly(d.key, start_ts=h24_start, end_ts=now_ts + 3600)
                            if df_h is None or df_h.empty or "kwh" not in df_h.columns:
                                continue
                            bars = []
                            for _, hrow in df_h.iterrows():
                                hts = int(hrow.get("hour_ts", 0))
                                kwh = float(pd.to_numeric(hrow.get("kwh", 0), errors="coerce") or 0)
                                if kwh < 0:
                                    kwh = 0.0
                                ci_h = co2_by_hour.get(hts, current_intensity)
                                co2_g = kwh * ci_h
                                hour_str = _dtc.fromtimestamp(hts, tz=_tzc).strftime("%H:%M")
                                bars.append({
                                    "hour": hour_str,
                                    "ts": hts,
                                    "kwh": round(kwh, 4),
                                    "co2_g": round(co2_g, 1),
                                    "intensity": round(ci_h, 1),
                                })
                            if bars:
                                total_co2_g = sum(b["co2_g"] for b in bars)
                                device_hourly_co2.append({
                                    "key": d.key,
                                    "name": d.name,
                                    "total_co2_g": round(total_co2_g, 1),
                                    "bars": bars,
                                })
                        except Exception:
                            continue

                    # Equivalents
                    tree_days = co2_month / 22.0 * 365 if co2_month > 0 else 0
                    car_km = co2_month / 0.170 if co2_month > 0 else 0

                    return {
                        "ok": True,
                        "enabled": True,
                        "zone": zone,
                        "cross_border": cross_border,
                        "green_threshold": green_thr,
                        "dirty_threshold": dirty_thr,
                        "current_intensity": round(current_intensity, 1),
                        "current_source": current_source,
                        "co2_today_kg": round(co2_today, 3),
                        "co2_week_kg": round(co2_week, 3),
                        "co2_month_kg": round(co2_month, 3),
                        "co2_year_kg": round(co2_year, 3),
                        "tree_days": round(tree_days, 0),
                        "car_km": round(car_km, 0),
                        "hourly": hourly_data,
                        "device_rates": device_rates,
                        "fuel_mix": fuel_mix,
                        "fuel_mix_hour": fuel_mix_hour,
                        "device_hourly_co2": device_hourly_co2,
                    }
                except Exception as e:
                    return {"ok": False, "error": str(e)}

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
                    # Neutral current via vector sum of phase currents (3-phase only).
                    # Uses arctan2(Q, P) per phase for the power-factor angle, then
                    # computes I_N = |I_a∠(-φ_a) + I_b∠(-120°-φ_b) + I_c∠(120°-φ_c)|.
                    try:
                        _ia = float(s.current_a.get("a", 0.0))
                        _ib = float(s.current_a.get("b", 0.0))
                        _ic = float(s.current_a.get("c", 0.0))
                        if _ia > 0 or _ib > 0 or _ic > 0:
                            _pa = float(s.power_w.get("a", 0.0))
                            _pb = float(s.power_w.get("b", 0.0))
                            _pc = float(s.power_w.get("c", 0.0))
                            _qa = float(getattr(s, "reactive_var", {}).get("a", 0.0))
                            _qb = float(getattr(s, "reactive_var", {}).get("b", 0.0))
                            _qc = float(getattr(s, "reactive_var", {}).get("c", 0.0))
                            _phi_a = math.atan2(_qa, _pa) if (_pa or _qa) else 0.0
                            _phi_b = math.atan2(_qb, _pb) if (_pb or _qb) else 0.0
                            _phi_c = math.atan2(_qc, _pc) if (_pc or _qc) else 0.0
                            _2pi3 = 2.0 * math.pi / 3.0
                            _ta, _tb, _tc = -_phi_a, -_2pi3 - _phi_b, _2pi3 - _phi_c
                            _in_re = _ia * math.cos(_ta) + _ib * math.cos(_tb) + _ic * math.cos(_tc)
                            _in_im = _ia * math.sin(_ta) + _ib * math.sin(_tb) + _ic * math.sin(_tc)
                            _i_n = math.sqrt(_in_re * _in_re + _in_im * _in_im)
                            series["n_current"].append((s.ts, _i_n))
                    except Exception:
                        pass

                    # Evaluate alert rules on incoming live samples
                    try:
                        self._alerts_process_sample(s)
                    except Exception:
                        pass

                    # Feed ML NILM transition learner (per 3-phase device)
                    try:
                        _learners = getattr(self, "_nilm_learners", {})
                        _dev_learner = _learners.get(s.device_key)
                        if _dev_learner is not None:
                            _pw_total = float(s.power_w.get("total", 0.0))
                            _dev_learner.observe(s.device_key, float(s.ts), _pw_total)
                            # Periodically re-cluster all learners (every 5 minutes)
                            import time as _time_mod
                            _now_ts = _time_mod.time()
                            _last_cluster = getattr(self, "_nilm_last_cluster_ts", 0.0)
                            if (_now_ts - _last_cluster) > 300:
                                self._nilm_last_cluster_ts = _now_ts
                                _all_clusters = []
                                _total_trans = 0
                                _per_device_info = []
                                for _dk, _lrn in _learners.items():
                                    _cls = _lrn.cluster()
                                    _nt = _lrn.get_transition_count()
                                    _total_trans += _nt
                                    _all_clusters.extend(_cls)
                                    if _cls:
                                        _dev_name = _dk
                                        for _d in self.cfg.devices:
                                            if _d.key == _dk:
                                                _dev_name = _d.name
                                                break
                                        _per_device_info.append(f"{_dev_name}: {len(_cls)} Muster/{_nt} Trans.")
                                # Push merged cluster data to web dashboard store
                                try:
                                    store = getattr(self, "_live_state_store", None)
                                    if store is not None:
                                        store._nilm_clusters = [
                                            {"matched_appliance": c.matched_appliance, "count": c.count,
                                             "centroid_w": c.centroid_w, "icon": c.icon, "label": c.label,
                                             "device_key": getattr(c, "device_key", "")}
                                            for c in _all_clusters
                                        ]
                                except Exception:
                                    pass
                                # Log progress periodically (every 30 min)
                                _last_log = getattr(self, "_nilm_last_log_ts", 0.0)
                                if _all_clusters and (_now_ts - _last_log) > 1800:
                                    self._nilm_last_log_ts = _now_ts
                                    for _info in _per_device_info:
                                        try:
                                            self._log_sync(f"NILM ML [{_info}]")
                                        except Exception:
                                            pass
                                # Update desktop status label
                                try:
                                    _nilm_var = getattr(self, "_nilm_status_var", None)
                                    if _nilm_var is not None:
                                        _n_clust = len(_all_clusters)
                                        _n_devs = len(_learners)
                                        if _n_clust > 0:
                                            _nilm_var.set(f"NILM: {_n_clust} Muster / {_total_trans} Trans. ({_n_devs} Geraete)")
                                        elif _total_trans > 0:
                                            _nilm_var.set(f"NILM: lerne... ({_total_trans} Trans. / {_n_devs} Geraete)")
                                        else:
                                            _nilm_var.set(f"NILM: lerne... ({_n_devs} Geraete)")
                                except Exception:
                                    pass
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
                                # Compute today's cost from kWh × gross price
                                cost_str = ""
                                try:
                                    if total_kwh is not None and total_kwh > 0:
                                        ug = float(self.cfg.pricing.unit_price_gross())
                                        cost_val = float(total_kwh) * ug
                                        cost_str = f"   {self.t('live.cards.cost_today')}: {cost_val:.2f} €"
                                except Exception:
                                    cost_str = ""

                                if total_kwh is None:
                                    line0 = f"{self.t('live.cards.power')}: {pw:.0f} W   {self.t('live.cards.updated')}: {stamp}"
                                else:
                                    line0 = f"{self.t('live.cards.power')}: {pw:.0f} W   {self.t('live.cards.kwh_today')}: {float(total_kwh):.3f} kWh{cost_str}   {self.t('live.cards.updated')}: {stamp}"
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
                                    line2 = f"{self.t('web.kv.var')}: {q_txt}   {self.t('web.kv.cosphi')}: {pf_txt}"
                                else:
                                    q_txt = f"{q_total:.0f} VAR ({qa2:.0f}/{qb2:.0f}/{qc2:.0f})"
                                    pf_txt = f"{pf_total:.3f} ({pfa2:.3f}/{pfb2:.3f}/{pfc2:.3f})"
                                    # Phase balance indicator (% symmetry)
                                    # Always show it, even if one phase is below 5 W.
                                    # The previous logic filtered "active" phases with >5 W which could
                                    # make the balance text disappear when L1 dropped below that threshold.
                                    balance_txt = ""
                                    try:
                                        pa2 = float(s.power_w.get("a", 0.0))
                                        pb2 = float(s.power_w.get("b", 0.0))
                                        pc2 = float(s.power_w.get("c", 0.0))
                                        phases_abs = [abs(pa2), abs(pb2), abs(pc2)]
                                        p_avg = sum(phases_abs) / 3.0
                                        if p_avg > 0.0:
                                            p_dev = max(abs(p - p_avg) for p in phases_abs)
                                            bal_pct = max(0.0, 100.0 - (p_dev / p_avg * 100.0))
                                        else:
                                            bal_pct = 100.0
                                        if bal_pct >= 90:
                                            sym = "✅"
                                        elif bal_pct >= 70:
                                            sym = "⚠️"
                                        else:
                                            sym = "❌"
                                        balance_txt = f"   {self.t('live.cards.balance')}: {bal_pct:.0f}% {sym} ({pa2:.0f}/{pb2:.0f}/{pc2:.0f} W)"
                                    except Exception:
                                        pass
                                    line2 = f"{self.t('web.kv.var')}: {q_txt}   {self.t('web.kv.cosphi')}: {pf_txt}{balance_txt}"
                                line3 = "–"
                                try:
                                    fq = float(getattr(s, "freq_hz", {}).get("total", 0.0))
                                    if fq > 1.0:
                                        line3 = f"{self.t('web.kv.freq')}: {fq:.2f} Hz"
                                except Exception:
                                    pass
                                # Neutral current (3-phase only): compute from last n_current sample
                                if ph2 >= 3:
                                    try:
                                        _ns = series.get("n_current") if series is not None else None
                                        if _ns and len(_ns) > 0:
                                            _i_n_val = _ns[-1][1]
                                            _va2 = float(s.voltage_v.get("a", 0.0))
                                            _vb2 = float(s.voltage_v.get("b", 0.0))
                                            _vc2 = float(s.voltage_v.get("c", 0.0))
                                            _v_avg = (_va2 + _vb2 + _vc2) / 3.0
                                            _s_n = _v_avg * _i_n_val
                                            _n_txt = f"   N: {_i_n_val:.2f} A / {_s_n:.0f} VA"
                                            line3 = (line3 + _n_txt) if line3 != "–" else f"N: {_i_n_val:.2f} A / {_s_n:.0f} VA"
                                    except Exception:
                                        pass

                                if 'line0' in vars_:
                                    vars_['line0'].set(line0)
                                if 'line1' in vars_:
                                    vars_['line1'].set(line1)
                                if 'line2' in vars_:
                                    vars_['line2'].set(line2)
                                if 'line3' in vars_:
                                    vars_['line3'].set(line3)
                                # Appliance detector (mode: combined / static / ml)
                                try:
                                    _appl_var = vars_.get('appliance')
                                    if _appl_var is not None:
                                        _nilm_mode = getattr(self, "_nilm_mode", {}).get(s.device_key, "combined")
                                        _dev_lrn = getattr(self, "_nilm_learners", {}).get(s.device_key)
                                        _ml_clusters = _dev_lrn.get_clusters() if _dev_lrn else []

                                        if _nilm_mode == "static":
                                            # Pure static matching
                                            _matches = _identify_appliance(pw)[:3]
                                            _mode_label = "Statisch"
                                        elif _nilm_mode == "ml":
                                            # ML-only: show learned clusters that match current power
                                            _matches = []
                                            if _ml_clusters:
                                                for _cl in sorted(_ml_clusters, key=lambda c: c.count, reverse=True):
                                                    if _cl.centroid_w > 0 and abs(pw - _cl.centroid_w) < _cl.centroid_w * 0.3 + _cl.std_w * 1.5:
                                                        # Cluster matches current power range
                                                        _dist = abs(pw - _cl.centroid_w)
                                                        _range = _cl.centroid_w * 0.3 + _cl.std_w * 1.5
                                                        _conf = max(0.2, 1.0 - _dist / _range) if _range > 0 else 0.5
                                                        # Try to find a matching built-in appliance for icon/name
                                                        _built_in = _identify_appliance(_cl.centroid_w)[:1]
                                                        if _built_in and _cl.matched_appliance:
                                                            _matches.append((_built_in[0][0], round(_conf, 3)))
                                                        else:
                                                            # Unknown learned pattern
                                                            from shelly_analyzer.services.appliance_detector import ApplianceSignature
                                                            _fake_sig = ApplianceSignature(
                                                                id=f"learned_{_cl.cluster_id}",
                                                                category="learned",
                                                                icon=_cl.icon or "🔌",
                                                                power_min=_cl.centroid_w - _cl.std_w,
                                                                power_max=_cl.centroid_w + _cl.std_w,
                                                                pattern_type="learned",
                                                                typical_duration_min=0,
                                                            )
                                                            _matches.append((_fake_sig, round(_conf, 3)))
                                                        if len(_matches) >= 3:
                                                            break
                                            _mode_label = "ML"
                                        else:
                                            # Combined: static + ML boost
                                            _matches = _identify_appliance(pw)[:3]
                                            if _ml_clusters and _matches:
                                                for _ci, (_sig, _conf) in enumerate(_matches):
                                                    for _cl in _ml_clusters:
                                                        if _cl.matched_appliance == _sig.id and _cl.count >= 5:
                                                            _matches[_ci] = (_sig, min(1.0, _conf + 0.15))
                                                            break
                                            _mode_label = "Kombiniert"

                                        if _matches:
                                            _title = self.t('live.appliance.title')
                                            _parts = []
                                            for _sig, _conf in _matches:
                                                _pct = int(_conf * 100)
                                                _dot = "🟢" if _pct >= 70 else ("🟡" if _pct >= 40 else "🔴")
                                                _name = self.t(f'appliance.{_sig.id}.name')
                                                if _name == f'appliance.{_sig.id}.name':
                                                    _name = f"{_sig.icon} {_sig.power_min:.0f}-{_sig.power_max:.0f}W"
                                                _parts.append(f"{_sig.icon} {_name} {_dot} {_pct}%")
                                            _n_ml = len(_ml_clusters)
                                            _ml_info = f" [{_mode_label} | {_n_ml} ML-Muster]" if _n_ml > 0 else f" [{_mode_label}]"
                                            _appl_var.set(f"{_title}: " + "  ·  ".join(_parts) + _ml_info)
                                        else:
                                            _n_ml = len(_ml_clusters)
                                            if _n_ml > 0:
                                                _appl_var.set(f"[{_mode_label} | {_n_ml} ML-Muster gelernt]")
                                            else:
                                                _appl_var.set("")
                                except Exception:
                                    pass
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
                        _ia = float(s.current_a.get("a", 0.0))
                        _ib = float(s.current_a.get("b", 0.0))
                        _ic = float(s.current_a.get("c", 0.0))
                        # Neutral current via vector sum with power-factor angles (same as desktop app)
                        _i_n = 0.0
                        if _ia > 0 or _ib > 0 or _ic > 0:
                            _pa = float(s.power_w.get("a", 0.0))
                            _pb = float(s.power_w.get("b", 0.0))
                            _pc = float(s.power_w.get("c", 0.0))
                            _qa = float(getattr(s, "reactive_var", {}).get("a", 0.0))
                            _qb = float(getattr(s, "reactive_var", {}).get("b", 0.0))
                            _qc = float(getattr(s, "reactive_var", {}).get("c", 0.0))
                            _phi_a = math.atan2(_qa, _pa) if (_pa or _qa) else 0.0
                            _phi_b = math.atan2(_qb, _pb) if (_pb or _qb) else 0.0
                            _phi_c = math.atan2(_qc, _pc) if (_pc or _qc) else 0.0
                            _2pi3 = 2.0 * math.pi / 3.0
                            _ta, _tb, _tc = -_phi_a, -_2pi3 - _phi_b, _2pi3 - _phi_c
                            _in_re = _ia * math.cos(_ta) + _ib * math.cos(_tb) + _ic * math.cos(_tc)
                            _in_im = _ia * math.sin(_ta) + _ib * math.sin(_tb) + _ic * math.sin(_tc)
                            _i_n = math.sqrt(_in_re * _in_re + _in_im * _in_im)
                        self._live_state_store.update(
                            s.device_key,
                            LivePoint(
                                ts=int(s.ts),
                                power_total_w=float(s.power_w.get("total", 0.0)),
                                pa=float(s.power_w.get("a", 0.0)),
                                pb=float(s.power_w.get("b", 0.0)),
                                pc=float(s.power_w.get("c", 0.0)),
                                va=float(s.voltage_v.get("a", 0.0)),
                                vb=float(s.voltage_v.get("b", 0.0)),
                                vc=float(s.voltage_v.get("c", 0.0)),
                                ia=_ia,
                                ib=_ib,
                                ic=_ic,
                                q_total_var=float(getattr(s, "reactive_var", {}).get("total", 0.0)),
                                qa=float(getattr(s, "reactive_var", {}).get("a", 0.0)),
                                qb=float(getattr(s, "reactive_var", {}).get("b", 0.0)),
                                qc=float(getattr(s, "reactive_var", {}).get("c", 0.0)),
                                cosphi_total=float(getattr(s, "cosphi", {}).get("total", 0.0)),
                                pfa=float(getattr(s, "cosphi", {}).get("a", 0.0)),
                                pfb=float(getattr(s, "cosphi", {}).get("b", 0.0)),
                                pfc=float(getattr(s, "cosphi", {}).get("c", 0.0)),
                                freq_hz=float(getattr(s, "freq_hz", {}).get("total", 0.0)),
                                kwh_today=float(total_kwh),
                                cost_today=float(total_kwh) * float(getattr(getattr(self.cfg, 'pricing', None), 'unit_price_gross', lambda: 0.0)()) if total_kwh else 0.0,
                                i_n=_i_n,
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
