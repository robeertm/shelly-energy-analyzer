"""CO₂ intensity tab mixin for Shelly Energy Analyzer.

Provides:
- Live section: current grid CO₂ intensity (colour-coded), per-device CO₂ rate
- Historical chart: 24 h intensity line + per-device stacked area
- Summary cards: total CO₂ today / week / month, equivalents
- 24 h green/dirty hours heatmap strip
- Settings section for ENTSO-E API token, bidding zone, thresholds, backfill
"""
from __future__ import annotations

import logging
import queue
import time
import threading
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import ttk
import numpy as np
import pandas as pd
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

logger = logging.getLogger(__name__)

# g CO₂ per passenger-km for an average European petrol car (ICCT 2023)
_CAR_G_PER_KM = 170.0
# kg CO₂ absorbed by an average tree per year
_TREE_KG_PER_YEAR = 22.0


class Co2Mixin:
    """CO₂ intensity tab: live intensity, history chart, summary, heatmap."""

    # ── Lifecycle ────────────────────────────────────────────────────────────

    def _co2_service_init(self) -> None:
        """Start the background CO₂ fetch service (called once after app init)."""
        from shelly_analyzer.services.entsoe import Co2FetchService
        self._co2_progress_q: "queue.Queue[tuple]" = queue.Queue()
        self._co2_fetch_svc = Co2FetchService(
            db=self.storage.db,
            get_config=lambda: self.cfg,
        )
        self._co2_fetch_svc.set_progress_callback(
            lambda day, total: self._co2_progress_q.put((day, total))
        )
        self._co2_fetch_svc.set_log_callback(
            lambda msg: self.after(0, lambda m=msg: self._log_sync(m))
        )
        self._co2_fetch_svc.start()
        self.protocol("WM_DELETE_WINDOW", self._co2_on_close)

    def _co2_on_close(self) -> None:
        """Stop the fetch service on window close."""
        try:
            self._co2_fetch_svc.stop()
        except Exception:
            pass
        try:
            self.destroy()
        except Exception:
            pass

    # ── Tab builder ──────────────────────────────────────────────────────────

    def _build_co2_tab(self) -> None:
        """Build the CO₂ intensity overview tab."""
        frm = self.tab_co2

        # ── Title bar ────────────────────────────────────────────────────────
        top = ttk.Frame(frm)
        top.pack(fill="x", padx=14, pady=(12, 4))
        ttk.Label(top, text=self.t("co2.title"), font=("", 14, "bold")).pack(side="left")

        self._co2_status_var = tk.StringVar(value="")
        ttk.Label(
            top,
            textvariable=self._co2_status_var,
            foreground="gray",
        ).pack(side="right", padx=(0, 4))

        # ── Import progress bar ───────────────────────────────────────────────
        prog_frm = ttk.Frame(frm)
        prog_frm.pack(fill="x", padx=14, pady=(0, 2))
        self._co2_progress_label_var = tk.StringVar(value="")
        ttk.Label(
            prog_frm,
            textvariable=self._co2_progress_label_var,
            foreground="gray",
        ).pack(anchor="w")
        self._co2_progressbar = ttk.Progressbar(
            prog_frm,
            mode="determinate",
            maximum=100,
        )
        self._co2_progressbar.pack(fill="x", pady=(2, 0))

        # ── Scrollable content ───────────────────────────────────────────────
        outer = ttk.Frame(frm)
        outer.pack(fill="both", expand=True)

        canvas = tk.Canvas(outer, highlightthickness=0)
        scrollbar = ttk.Scrollbar(outer, orient="vertical", command=canvas.yview)
        self._co2_scroll_frame = ttk.Frame(canvas)
        self._co2_scroll_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all")),
        )
        _win = canvas.create_window((0, 0), window=self._co2_scroll_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.bind("<Configure>", lambda e: canvas.itemconfigure(_win, width=e.width))
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        def _on_mw(event):
            try:
                canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
            except Exception:
                pass
        canvas.bind_all("<MouseWheel>", _on_mw)

        sf = self._co2_scroll_frame

        # ── Live intensity card ───────────────────────────────────────────────
        live_outer = ttk.LabelFrame(sf, text=self.t("co2.live.label"))
        live_outer.pack(fill="x", padx=14, pady=(8, 4))
        live_inner = ttk.Frame(live_outer)
        live_inner.pack(fill="x", padx=10, pady=8)

        self._co2_live_intensity_var = tk.StringVar(value="–")
        self._co2_live_label = ttk.Label(
            live_inner,
            textvariable=self._co2_live_intensity_var,
            font=("", 24, "bold"),
        )
        self._co2_live_label.pack(side="left", padx=(0, 8))
        ttk.Label(live_inner, text=self.t("co2.live.unit"), foreground="gray").pack(
            side="left", anchor="s", pady=(0, 4)
        )

        self._co2_live_band_var = tk.StringVar(value="")
        self._co2_live_band_label = ttk.Label(
            live_inner,
            textvariable=self._co2_live_band_var,
            font=("", 11, "bold"),
        )
        self._co2_live_band_label.pack(side="left", padx=(16, 0))

        # Per-device rates (created dynamically in refresh)
        self._co2_device_rates_frame = ttk.Frame(live_outer)
        self._co2_device_rates_frame.pack(fill="x", padx=10, pady=(0, 8))

        # ── Summary cards ─────────────────────────────────────────────────────
        summary_frame = ttk.LabelFrame(sf, text=self.t("co2.summary.title"))
        summary_frame.pack(fill="x", padx=14, pady=(4, 4))
        summary_inner = ttk.Frame(summary_frame)
        summary_inner.pack(fill="x", padx=8, pady=8)
        summary_inner.columnconfigure((0, 1, 2), weight=1)

        self._co2_summary_vars: Dict[str, tk.StringVar] = {}
        for col, (key, label_key) in enumerate([
            ("today", "co2.summary.today"),
            ("week", "co2.summary.week"),
            ("month", "co2.summary.month"),
        ]):
            card = ttk.LabelFrame(summary_inner, text=self.t(label_key))
            card.grid(row=0, column=col, sticky="nsew", padx=4, pady=2)
            v_kg = tk.StringVar(value="–")
            v_car = tk.StringVar(value="")
            v_tree = tk.StringVar(value="")
            ttk.Label(card, textvariable=v_kg, font=("", 13, "bold")).pack(anchor="center", pady=(6, 0))
            ttk.Label(card, text=self.t("co2.summary.unit_kg"), foreground="gray", font=("", 8)).pack(anchor="center")
            ttk.Label(card, textvariable=v_car, foreground="gray", font=("", 8)).pack(anchor="center")
            ttk.Label(card, textvariable=v_tree, foreground="gray", font=("", 8)).pack(anchor="center", pady=(0, 6))
            self._co2_summary_vars[f"{key}_kg"] = v_kg
            self._co2_summary_vars[f"{key}_car"] = v_car
            self._co2_summary_vars[f"{key}_tree"] = v_tree

        # ── 24 h intensity chart ──────────────────────────────────────────────
        chart_frame = ttk.LabelFrame(sf, text=self.t("co2.chart.title"))
        chart_frame.pack(fill="both", padx=14, pady=(4, 4))

        self._co2_fig = Figure(figsize=(10, 3.0), dpi=96)
        self._co2_ax = self._co2_fig.add_subplot(111)
        self._co2_canvas_widget = FigureCanvasTkAgg(self._co2_fig, master=chart_frame)
        self._co2_canvas_widget.get_tk_widget().pack(fill="both", expand=True, padx=4, pady=4)

        # ── Green/dirty hours heatmap strip ───────────────────────────────────
        heatmap_frame = ttk.LabelFrame(sf, text=self.t("co2.heatmap.title"))
        heatmap_frame.pack(fill="x", padx=14, pady=(4, 12))

        self._co2_heatmap_fig = Figure(figsize=(10, 0.7), dpi=96)
        self._co2_heatmap_ax = self._co2_heatmap_fig.add_subplot(111)
        self._co2_heatmap_canvas = FigureCanvasTkAgg(self._co2_heatmap_fig, master=heatmap_frame)
        self._co2_heatmap_canvas.get_tk_widget().pack(fill="x", expand=True, padx=4, pady=4)

        # Trigger initial refresh after a short delay
        self.after(800, self._refresh_co2_tab)

        # Start the progress-bar polling loop
        self.after(500, self._co2_poll_progress)

    # ── Refresh ───────────────────────────────────────────────────────────────

    def _refresh_co2_tab(self) -> None:
        """Refresh all CO₂ tab sections from the database.

        The ``co2.enabled`` flag controls background auto-fetching but must NOT
        block displaying data that is already stored locally.  This method always
        queries the DB so that a manual backfill (which bypasses the enabled
        check) becomes visible immediately.
        """
        # Schedule periodic auto-refresh so new data appears automatically
        try:
            self.after(60_000, self._refresh_co2_tab)
        except Exception:
            pass

        try:
            cfg = self.cfg
            co2_cfg = getattr(cfg, "co2", None)

            # Read display settings with safe fallbacks – no early return for
            # enabled=False so that manually-imported data is always shown.
            zone = getattr(co2_cfg, "bidding_zone", "DE_LU") or "DE_LU"
            # Also try the zone that is currently selected in the UI (may differ
            # from the saved config if the user changed it without saving).
            ui_zone = ""
            try:
                ui_zone = (getattr(self, "_co2_zone_var", None) or tk.StringVar()).get().strip()
            except Exception:
                pass
            green_thr = getattr(co2_cfg, "green_threshold_g_per_kwh", 150.0) if co2_cfg else 150.0
            dirty_thr = getattr(co2_cfg, "dirty_threshold_g_per_kwh", 400.0) if co2_cfg else 400.0

            db = self.storage.db
            now_ts = int(time.time())
            start_24h = now_ts - 86400
            df = db.query_co2_intensity(zone, start_24h, now_ts + 3600)

            # If saved-config zone returned nothing, try the zone shown in the UI
            # (happens when the user changed the zone and ran a backfill without
            # saving settings first).
            if df.empty and ui_zone and ui_zone != zone:
                df2 = db.query_co2_intensity(ui_zone, start_24h, now_ts + 3600)
                if not df2.empty:
                    zone = ui_zone
                    df = df2

            if df.empty:
                self._co2_live_intensity_var.set("–")
                self._co2_live_band_var.set(self.t("co2.error.no_data"))
                self._co2_status_var.set(self.t("co2.error.no_data"))
                self._co2_clear_charts()
                return

            # ── Live intensity ────────────────────────────────────────────────
            current_hour_ts = (now_ts // 3600) * 3600
            current_row = df[df["hour_ts"] <= current_hour_ts].tail(1)
            if not current_row.empty:
                intensity = float(current_row["intensity_g_per_kwh"].iloc[0])
                self._co2_live_intensity_var.set(f"{intensity:.0f}")
                band, color = self._co2_band(intensity, green_thr, dirty_thr)
                self._co2_live_band_var.set(band)
                try:
                    self._co2_live_band_label.configure(foreground=color)
                    self._co2_live_label.configure(foreground=color)
                except Exception:
                    pass
            else:
                intensity = float(df["intensity_g_per_kwh"].iloc[-1])
                self._co2_live_intensity_var.set(f"{intensity:.0f}")

            # ── Per-device CO₂ rates ──────────────────────────────────────────
            self._co2_update_device_rates(intensity)

            # ── Summary cards ─────────────────────────────────────────────────
            self._co2_update_summary(zone, now_ts)

            # ── Intensity chart ───────────────────────────────────────────────
            self._co2_draw_intensity_chart(df, green_thr, dirty_thr)

            # ── Heatmap ───────────────────────────────────────────────────────
            self._co2_draw_heatmap(df, green_thr, dirty_thr)

            # Status
            latest_ts = int(df["hour_ts"].max())
            latest_dt = datetime.fromtimestamp(latest_ts, tz=timezone.utc).strftime("%H:%M")
            self._co2_status_var.set(self.t("co2.status.ok", ts=latest_dt))

        except Exception:
            logger.exception("Co2Mixin: refresh error")

    def _co2_poll_progress(self) -> None:
        """Drain the progress queue and update the progress bar / status label.

        Runs on the Tk main thread via after(); reschedules itself every 400 ms.
        """
        try:
            last_day: Optional[int] = None
            last_total: Optional[int] = None
            while True:
                try:
                    day, total = self._co2_progress_q.get_nowait()
                    last_day, last_total = day, total
                except queue.Empty:
                    break

            if last_day is not None and last_total is not None:
                total = last_total
                day = last_day
                if total > 0 and day < total:
                    pct = min(99, int(day / total * 100))
                    self._co2_progressbar["value"] = pct
                    self._co2_progress_label_var.set(
                        self.t("co2.status.importing", day=day, total=total)
                    )
                else:
                    # Fetch round complete
                    svc = getattr(self, "_co2_fetch_svc", None)
                    err = getattr(svc, "last_error", None) if svc else None
                    self._co2_progressbar["value"] = 0
                    if err:
                        self._co2_progress_label_var.set(f"⚠ {err[:120]}")
                        try:
                            self._co2_status_var.set(f"⚠ {err[:80]}")
                        except Exception:
                            pass
                    else:
                        self._co2_progress_label_var.set("")
                        self.after(300, self._refresh_co2_tab)
        except Exception:
            logger.debug("Co2Mixin: progress poll error", exc_info=True)
        try:
            self.after(400, self._co2_poll_progress)
        except Exception:
            pass

    def _co2_show_disabled(self) -> None:
        self._co2_live_intensity_var.set("–")
        self._co2_live_band_var.set("")
        token = getattr(getattr(self.cfg, "co2", None), "entso_e_api_token", "") or ""
        if not token:
            self._co2_status_var.set(self.t("co2.error.no_token"))
        else:
            self._co2_status_var.set(self.t("co2.error.no_data"))
        self._co2_clear_charts()

    def _co2_clear_charts(self) -> None:
        try:
            self._co2_ax.cla()
            self._co2_canvas_widget.draw_idle()
        except Exception:
            pass
        try:
            self._co2_heatmap_ax.cla()
            self._co2_heatmap_canvas.draw_idle()
        except Exception:
            pass

    def _co2_band(
        self, intensity: float, green_thr: float, dirty_thr: float
    ) -> Tuple[str, str]:
        """Return (label, hex-colour) for an intensity value."""
        if intensity <= green_thr:
            return self.t("co2.live.green"), "#2e7d32"
        if intensity >= dirty_thr:
            return self.t("co2.live.red"), "#c62828"
        return self.t("co2.live.yellow"), "#f9a825"

    def _co2_update_device_rates(self, current_intensity: float) -> None:
        """Refresh the per-device CO₂ rate labels."""
        for widget in self._co2_device_rates_frame.winfo_children():
            widget.destroy()

        try:
            devices = getattr(self.cfg, "devices", []) or []
            if not devices:
                return
            db = self.storage.db
            now_ts = int(time.time())
            start_ts = now_ts - 3600
            for dev in devices:
                try:
                    df_s = db.query_samples(dev.key, start_ts, now_ts)
                    if df_s is None or df_s.empty:
                        avg_w = 0.0
                    else:
                        avg_w = float(df_s["total_power"].dropna().tail(10).mean() or 0.0)
                    co2_g_h = avg_w * current_intensity / 1000.0
                    text = f"{dev.name or dev.key}: {co2_g_h:.0f} {self.t('co2.live.device_rate')}"
                    ttk.Label(
                        self._co2_device_rates_frame,
                        text=text,
                        font=("", 9),
                    ).pack(side="left", padx=(0, 14))
                except Exception:
                    pass
        except Exception:
            pass

    def _co2_update_summary(self, zone: str, now_ts: int) -> None:
        """Update the summary cards for today / week / month."""
        from zoneinfo import ZoneInfo
        try:
            tz = ZoneInfo("Europe/Berlin")
        except Exception:
            tz = timezone.utc

        now_dt = datetime.fromtimestamp(now_ts, tz=tz)
        today_start = now_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        week_start = today_start - timedelta(days=now_dt.weekday())
        month_start = today_start.replace(day=1)

        periods = {
            "today": int(today_start.timestamp()),
            "week": int(week_start.timestamp()),
            "month": int(month_start.timestamp()),
        }

        db = self.storage.db
        devices = getattr(self.cfg, "devices", []) or []

        for key, start_ts in periods.items():
            try:
                df_co2 = db.query_co2_intensity(zone, start_ts, now_ts + 3600)
                if df_co2.empty:
                    self._co2_summary_vars[f"{key}_kg"].set("–")
                    self._co2_summary_vars[f"{key}_car"].set("")
                    self._co2_summary_vars[f"{key}_tree"].set("")
                    continue

                # Total energy consumption in kWh across all devices
                total_kwh = 0.0
                for dev in devices:
                    try:
                        df_h = db.query_hourly(dev.key, start_ts, now_ts + 3600)
                        if df_h is not None and not df_h.empty:
                            total_kwh += float(df_h["kwh"].sum())
                    except Exception:
                        pass

                avg_intensity = float(df_co2["intensity_g_per_kwh"].mean())
                total_kg = total_kwh * avg_intensity / 1000.0

                car_km = total_kg * 1000.0 / _CAR_G_PER_KM
                trees = total_kg / _TREE_KG_PER_YEAR

                self._co2_summary_vars[f"{key}_kg"].set(f"{total_kg:.2f}")
                self._co2_summary_vars[f"{key}_car"].set(
                    f"🚗 {car_km:.0f} {self.t('co2.summary.car_km')}"
                )
                self._co2_summary_vars[f"{key}_tree"].set(
                    f"🌳 {trees:.1f} {self.t('co2.summary.trees')}"
                )
            except Exception:
                logger.debug("Co2Mixin: summary error for %s", key, exc_info=True)

    def _co2_draw_intensity_chart(
        self,
        df: pd.DataFrame,
        green_thr: float,
        dirty_thr: float,
    ) -> None:
        ax = self._co2_ax
        ax.cla()

        try:
            df = df.copy()
            df["dt"] = pd.to_datetime(df["hour_ts"], unit="s", utc=True)
            df = df.sort_values("dt")

            x = df["dt"].values
            y = df["intensity_g_per_kwh"].values

            ax.fill_between(x, y, alpha=0.18, color="#1976d2")
            ax.plot(x, y, color="#1976d2", linewidth=1.5, label=self.t("co2.chart.intensity"))

            # threshold lines
            ax.axhline(green_thr, color="#2e7d32", linewidth=0.8, linestyle="--", alpha=0.6)
            ax.axhline(dirty_thr, color="#c62828", linewidth=0.8, linestyle="--", alpha=0.6)

            ax.set_ylabel(self.t("co2.chart.intensity"), fontsize=8)
            ax.tick_params(labelsize=7)
            self._co2_fig.autofmt_xdate(rotation=30, ha="right")
            self._co2_fig.tight_layout(pad=0.5)
            self._co2_canvas_widget.draw_idle()
        except Exception:
            logger.debug("Co2Mixin: chart draw error", exc_info=True)

    def _co2_draw_heatmap(
        self,
        df: pd.DataFrame,
        green_thr: float,
        dirty_thr: float,
    ) -> None:
        ax = self._co2_heatmap_ax
        ax.cla()

        try:
            df = df.copy().sort_values("hour_ts")
            intensities = df["intensity_g_per_kwh"].values
            hours = df["hour_ts"].values

            colors = []
            for v in intensities:
                if v <= green_thr:
                    colors.append("#4caf50")
                elif v >= dirty_thr:
                    colors.append("#f44336")
                else:
                    frac = (v - green_thr) / max(dirty_thr - green_thr, 1)
                    r = int(0x4c + frac * (0xf4 - 0x4c))
                    g = int(0xaf + frac * (0x43 - 0xaf))
                    b = int(0x50 + frac * (0x36 - 0x50))
                    colors.append(f"#{r:02x}{g:02x}{b:02x}")

            ax.barh(
                [0] * len(intensities),
                [1] * len(intensities),
                left=list(range(len(intensities))),
                color=colors,
                height=0.9,
            )
            ax.set_xlim(0, max(len(intensities), 24))
            ax.set_ylim(-0.6, 0.6)
            ax.set_yticks([])

            # x-tick labels: every 6 hours
            xticks = list(range(0, len(hours), max(1, len(hours) // 4)))
            xlabels = []
            for i in xticks:
                if i < len(hours):
                    dt = datetime.fromtimestamp(int(hours[i]), tz=timezone.utc)
                    xlabels.append(dt.strftime("%H:00"))
                else:
                    xlabels.append("")
            ax.set_xticks(xticks)
            ax.set_xticklabels(xlabels, fontsize=7)
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
            ax.spines["left"].set_visible(False)

            self._co2_heatmap_fig.tight_layout(pad=0.2)
            self._co2_heatmap_canvas.draw_idle()
        except Exception:
            logger.debug("Co2Mixin: heatmap draw error", exc_info=True)
