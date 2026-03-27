"""Solar/PV integration tab mixin for Shelly Energy Analyzer.

Provides:
- Solar overview tab with key metrics (feed-in, grid consumption, self-consumption, autarky)
- Daily bar chart showing PV flow breakdown
- Statistics for today / this week / this month / this year
- Feed-in revenue and cost savings calculation
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd
import tkinter as tk
from tkinter import ttk
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

logger = logging.getLogger(__name__)


class SolarMixin:
    """Solar/PV tab: metrics, charts, and statistics."""

    def _build_solar_tab(self) -> None:
        """Build the Solar/PV overview tab."""
        frm = self.tab_solar

        # ── Top bar ──────────────────────────────────────────────────────────
        top = ttk.Frame(frm)
        top.pack(fill="x", padx=14, pady=(12, 4))
        ttk.Label(top, text=self.t("solar.title"), font=("", 14, "bold")).pack(side="left")

        # ── Period selector ──────────────────────────────────────────────────
        ctrl = ttk.Frame(frm)
        ctrl.pack(fill="x", padx=14, pady=(0, 6))
        ttk.Label(ctrl, text=self.t("solar.period")).pack(side="left", padx=(0, 6))
        self._solar_period_var = tk.StringVar(value="today")
        period_options = [
            ("today", self.t("solar.period.today")),
            ("week", self.t("solar.period.week")),
            ("month", self.t("solar.period.month")),
            ("year", self.t("solar.period.year")),
        ]
        self._solar_period_labels = {code: label for code, label in period_options}
        self._solar_period_codes = {label: code for code, label in period_options}
        self._solar_period_display_var = tk.StringVar(value=period_options[0][1])
        period_cb = ttk.Combobox(
            ctrl,
            textvariable=self._solar_period_display_var,
            values=[label for _, label in period_options],
            width=16,
            state="readonly",
        )
        period_cb.current(0)
        period_cb.pack(side="left")
        period_cb.bind(
            "<<ComboboxSelected>>",
            lambda _e: self.after(50, self._refresh_solar_tab),
        )

        # ── Scrollable content area ──────────────────────────────────────────
        outer = ttk.Frame(frm)
        outer.pack(fill="both", expand=True)

        canvas = tk.Canvas(outer, highlightthickness=0)
        scrollbar = ttk.Scrollbar(outer, orient="vertical", command=canvas.yview)
        self._solar_scroll_frame = ttk.Frame(canvas)

        self._solar_scroll_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all")),
        )
        _sol_win = canvas.create_window((0, 0), window=self._solar_scroll_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.bind(
            "<Configure>",
            lambda e: canvas.itemconfigure(_sol_win, width=e.width),
        )

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        def _on_mousewheel(event):
            try:
                canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
            except Exception:
                pass

        canvas.bind_all("<MouseWheel>", _on_mousewheel)

        # ── Metric cards (created once, updated on refresh) ──────────────────
        cards_frame = ttk.Frame(self._solar_scroll_frame)
        cards_frame.pack(fill="x", padx=14, pady=(8, 4))
        cards_frame.columnconfigure((0, 1, 2, 3), weight=1)

        self._solar_vars: Dict[str, tk.StringVar] = {}

        card_defs = [
            ("feed_in",          "solar.card.feed_in",          "🔼"),
            ("grid_consumption", "solar.card.grid_consumption",  "🔽"),
            ("self_consumption", "solar.card.self_consumption",  "🏠"),
            ("autarky",          "solar.card.autarky",           "🌟"),
            ("pv_production",    "solar.card.pv_production",     "☀️"),
            ("feed_in_revenue",  "solar.card.feed_in_revenue",   "💶"),
            ("cost_savings",     "solar.card.cost_savings",      "💰"),
        ]

        for col, (key, label_key, icon) in enumerate(card_defs[:4]):
            card = ttk.LabelFrame(cards_frame, text=f"{icon} {self.t(label_key)}")
            card.grid(row=0, column=col, sticky="nsew", padx=3, pady=3)
            v = tk.StringVar(value="–")
            ttk.Label(card, textvariable=v, font=("", 13, "bold")).pack(
                anchor="center", padx=8, pady=8
            )
            self._solar_vars[key] = v

        cards_frame2 = ttk.Frame(self._solar_scroll_frame)
        cards_frame2.pack(fill="x", padx=14, pady=(0, 4))
        cards_frame2.columnconfigure((0, 1, 2), weight=1)

        for col, (key, label_key, icon) in enumerate(card_defs[4:]):
            card = ttk.LabelFrame(cards_frame2, text=f"{icon} {self.t(label_key)}")
            card.grid(row=0, column=col, sticky="nsew", padx=3, pady=3)
            v = tk.StringVar(value="–")
            ttk.Label(card, textvariable=v, font=("", 13, "bold")).pack(
                anchor="center", padx=8, pady=8
            )
            self._solar_vars[key] = v

        # CO₂ cards row
        co2_cards_frame = ttk.Frame(self._solar_scroll_frame)
        co2_cards_frame.pack(fill="x", padx=14, pady=(0, 4))
        co2_cards_frame.columnconfigure((0, 1, 2, 3), weight=1)

        co2_card_defs = [
            ("co2_saved",    "solar.card.co2_saved",    "🌱"),
            ("co2_grid",     "solar.card.co2_grid",     "🏭"),
            ("co2_avoided_trees", "solar.card.co2_trees", "🌳"),
            ("co2_avoided_car",   "solar.card.co2_car",   "🚗"),
        ]
        for col, (key, label_key, icon) in enumerate(co2_card_defs):
            card = ttk.LabelFrame(co2_cards_frame, text=f"{icon} {self.t(label_key)}")
            card.grid(row=0, column=col, sticky="nsew", padx=3, pady=3)
            v = tk.StringVar(value="–")
            ttk.Label(card, textvariable=v, font=("", 13, "bold")).pack(
                anchor="center", padx=8, pady=8
            )
            self._solar_vars[key] = v

        # Info label (shown when meter/devices missing)
        self._solar_info_var = tk.StringVar(value="")
        self._solar_info_label = ttk.Label(
            self._solar_scroll_frame,
            textvariable=self._solar_info_var,
            foreground="gray",
            justify="left",
            wraplength=900,
        )
        self._solar_info_label.pack(anchor="w", padx=14, pady=(2, 4))

        # ── Chart area (daily bar chart) ─────────────────────────────────────
        chart_frame = ttk.LabelFrame(
            self._solar_scroll_frame, text=self.t("solar.chart.title")
        )
        chart_frame.pack(fill="both", expand=True, padx=14, pady=(4, 12))

        self._solar_fig = Figure(figsize=(10, 3.5), dpi=96)
        self._solar_ax = self._solar_fig.add_subplot(111)
        self._solar_canvas_widget = FigureCanvasTkAgg(self._solar_fig, master=chart_frame)
        self._solar_canvas_widget.get_tk_widget().pack(fill="both", expand=True)

        # Initial refresh after short delay
        self.after(600, self._refresh_solar_tab)

    # ── Data calculation ─────────────────────────────────────────────────────

    def _solar_get_period_bounds(self) -> Tuple[datetime, datetime]:
        """Return (start, end) for the currently selected period."""
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Europe/Berlin")
        now = datetime.now(tz)
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        label = self._solar_period_display_var.get()
        code = self._solar_period_codes.get(label, "today")
        if code == "week":
            start = today_start - timedelta(days=now.weekday())
        elif code == "month":
            start = today_start.replace(day=1)
        elif code == "year":
            start = today_start.replace(month=1, day=1)
        else:
            start = today_start
        return start, now

    def _solar_calc_period(
        self, start: datetime, end: datetime
    ) -> Optional[Dict[str, float]]:
        """Calculate solar metrics for the given time range.

        Returns dict with keys:
            feed_in_kwh, grid_kwh, self_kwh, pv_kwh, autarky_pct,
            household_kwh (may be None if no other devices)
        Returns None if solar is not configured.
        """
        solar_cfg = getattr(self.cfg, "solar", None)
        if solar_cfg is None or not getattr(solar_cfg, "enabled", False):
            return None

        pv_key = str(getattr(solar_cfg, "pv_meter_device_key", "") or "")
        if not pv_key:
            return None

        # Find PV meter device
        pv_device = next(
            (d for d in (getattr(self.cfg, "devices", []) or []) if d.key == pv_key),
            None,
        )
        if pv_device is None:
            return None

        try:
            from zoneinfo import ZoneInfo
            tz = ZoneInfo("Europe/Berlin")
            utc = ZoneInfo("UTC")
            start_dt = pd.Timestamp(start)
            end_dt = pd.Timestamp(end)
        except Exception:
            return None

        def _load_device_df(device_key: str) -> Optional[pd.DataFrame]:
            try:
                cd = self.computed.get(device_key)
                if cd is not None:
                    df = cd.df.copy()
                else:
                    df = self.storage.read_device_df(device_key)
                if df is None or getattr(df, "empty", True):
                    return None
                if "timestamp" not in df.columns:
                    idx = pd.to_datetime(getattr(df, "index", None), errors="coerce")
                    df = df.copy()
                    df["timestamp"] = idx
                else:
                    df = df.copy()
                    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
                df = df.dropna(subset=["timestamp"])
                if df["timestamp"].dt.tz is None:
                    df["timestamp"] = df["timestamp"].dt.tz_localize(utc)
                df["timestamp"] = df["timestamp"].dt.tz_convert(tz)
                m = (df["timestamp"] >= start_dt) & (df["timestamp"] < end_dt)
                return df.loc[m].copy() if not df.empty else None
            except Exception:
                return None

        # ── PV meter: split positive (import) vs negative (export/feed-in) ──
        pv_df = _load_device_df(pv_key)
        feed_in_kwh = 0.0
        grid_kwh = 0.0

        if pv_df is not None and not pv_df.empty:
            # Determine power column
            for pcol in ("total_power", "W"):
                if pcol in pv_df.columns:
                    pv_df["_power"] = pd.to_numeric(pv_df[pcol], errors="coerce").fillna(0.0)
                    break
            else:
                pv_df["_power"] = 0.0

            # Calculate per-interval energy if not already present
            if "energy_kwh" not in pv_df.columns:
                try:
                    from shelly_analyzer.core.energy import calculate_energy
                    pv_df = calculate_energy(pv_df, method="auto")
                except Exception:
                    pv_df["energy_kwh"] = 0.0

            # Also check EMData columns (per-interval Wh sums)
            phase_cols = [
                c for c in ("a_total_act_energy", "b_total_act_energy", "c_total_act_energy")
                if c in pv_df.columns
            ]
            if phase_cols:
                wh = pd.to_numeric(pv_df[phase_cols].sum(axis=1), errors="coerce").fillna(0.0)
                pv_df["energy_kwh"] = wh / 1000.0

            e = pd.to_numeric(pv_df["energy_kwh"], errors="coerce").fillna(0.0)
            p = pv_df["_power"]

            # Negative power intervals → feed-in (export to grid)
            mask_export = p < 0
            feed_in_kwh = float(e[mask_export].abs().sum())
            # Positive power intervals → grid consumption (import from grid)
            mask_import = p >= 0
            grid_kwh = float(e[mask_import].sum())

        # ── Other devices: household consumption ─────────────────────────────
        other_keys = [
            d.key for d in (getattr(self.cfg, "devices", []) or [])
            if d.key != pv_key
        ]
        household_kwh: Optional[float] = None
        if other_keys:
            total_other = 0.0
            for key in other_keys:
                df_o = _load_device_df(key)
                if df_o is None or df_o.empty:
                    continue
                if "energy_kwh" not in df_o.columns:
                    try:
                        from shelly_analyzer.core.energy import calculate_energy
                        df_o = calculate_energy(df_o, method="auto")
                    except Exception:
                        df_o["energy_kwh"] = 0.0
                phase_cols_o = [
                    c for c in ("a_total_act_energy", "b_total_act_energy", "c_total_act_energy")
                    if c in df_o.columns
                ]
                if phase_cols_o:
                    wh_o = pd.to_numeric(df_o[phase_cols_o].sum(axis=1), errors="coerce").fillna(0.0)
                    df_o["energy_kwh"] = wh_o / 1000.0
                e_o = pd.to_numeric(df_o["energy_kwh"], errors="coerce").fillna(0.0)
                total_other += float(e_o.sum())
            household_kwh = total_other

        # ── Derived metrics ──────────────────────────────────────────────────
        # self_consumption = household - grid_import  (energy from PV used locally)
        self_kwh: Optional[float] = None
        pv_kwh: Optional[float] = None
        autarky_pct: Optional[float] = None

        if household_kwh is not None and household_kwh >= 0:
            self_kwh = max(0.0, household_kwh - grid_kwh)
            pv_kwh = self_kwh + feed_in_kwh
            if household_kwh > 0:
                autarky_pct = min(100.0, self_kwh / household_kwh * 100.0)
            else:
                autarky_pct = 100.0 if feed_in_kwh > 0 else 0.0

        return {
            "feed_in_kwh": feed_in_kwh,
            "grid_kwh": grid_kwh,
            "self_kwh": self_kwh,
            "pv_kwh": pv_kwh,
            "autarky_pct": autarky_pct,
            "household_kwh": household_kwh,
        }

    def _solar_calc_daily_profile(
        self, start: datetime, end: datetime
    ) -> Optional[pd.DataFrame]:
        """Return daily breakdown DataFrame for the bar chart."""
        solar_cfg = getattr(self.cfg, "solar", None)
        if solar_cfg is None or not getattr(solar_cfg, "enabled", False):
            return None
        pv_key = str(getattr(solar_cfg, "pv_meter_device_key", "") or "")
        if not pv_key:
            return None

        try:
            from zoneinfo import ZoneInfo
            tz = ZoneInfo("Europe/Berlin")
            utc = ZoneInfo("UTC")
            start_dt = pd.Timestamp(start)
            end_dt = pd.Timestamp(end)
        except Exception:
            return None

        try:
            cd = self.computed.get(pv_key)
            if cd is not None:
                df = cd.df.copy()
            else:
                df = self.storage.read_device_df(pv_key)
            if df is None or getattr(df, "empty", True):
                return None

            if "timestamp" not in df.columns:
                df["timestamp"] = pd.to_datetime(getattr(df, "index", None), errors="coerce")
            else:
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            df = df.dropna(subset=["timestamp"])
            if df["timestamp"].dt.tz is None:
                df["timestamp"] = df["timestamp"].dt.tz_localize(utc)
            df["timestamp"] = df["timestamp"].dt.tz_convert(tz)
            m = (df["timestamp"] >= start_dt) & (df["timestamp"] < end_dt)
            df = df.loc[m].copy()
            if df.empty:
                return None

            # Power column
            for pcol in ("total_power", "W"):
                if pcol in df.columns:
                    df["_power"] = pd.to_numeric(df[pcol], errors="coerce").fillna(0.0)
                    break
            else:
                df["_power"] = 0.0

            # Energy
            phase_cols = [c for c in ("a_total_act_energy", "b_total_act_energy", "c_total_act_energy") if c in df.columns]
            if phase_cols:
                wh = pd.to_numeric(df[phase_cols].sum(axis=1), errors="coerce").fillna(0.0)
                df["energy_kwh"] = wh / 1000.0
            elif "energy_kwh" not in df.columns:
                try:
                    from shelly_analyzer.core.energy import calculate_energy
                    df = calculate_energy(df, method="auto")
                except Exception:
                    df["energy_kwh"] = 0.0

            df["_date"] = df["timestamp"].dt.floor("D")
            df["_feed_in"] = df["energy_kwh"].abs().where(df["_power"] < 0, 0.0)
            df["_grid"] = df["energy_kwh"].where(df["_power"] >= 0, 0.0)

            daily = df.groupby("_date").agg(
                feed_in=("_feed_in", "sum"),
                grid=("_grid", "sum"),
            ).reset_index().rename(columns={"_date": "date"})
            return daily
        except Exception as exc:
            logger.debug("Solar daily profile error: %s", exc)
            return None

    # ── Refresh ──────────────────────────────────────────────────────────────

    def _refresh_solar_tab(self) -> None:
        """Recalculate all solar metrics and update the UI."""
        try:
            solar_cfg = getattr(self.cfg, "solar", None)
            configured = (
                solar_cfg is not None
                and bool(getattr(solar_cfg, "enabled", False))
                and bool(str(getattr(solar_cfg, "pv_meter_device_key", "") or ""))
            )

            if not configured:
                for v in self._solar_vars.values():
                    v.set("–")
                self._solar_info_var.set(self.t("solar.not_configured"))
                self._solar_ax.clear()
                self._solar_canvas_widget.draw_idle()
                return

            start, end = self._solar_get_period_bounds()
            result = self._solar_calc_period(start, end)

            if result is None:
                for v in self._solar_vars.values():
                    v.set("–")
                self._solar_info_var.set(self.t("solar.not_configured"))
                return

            feed_in = result["feed_in_kwh"]
            grid = result["grid_kwh"]
            self_kwh = result["self_kwh"]
            pv_kwh = result["pv_kwh"]
            autarky = result["autarky_pct"]
            household = result["household_kwh"]

            # Pricing
            try:
                unit_price = float(self.cfg.pricing.unit_price_gross())
            except Exception:
                unit_price = float(getattr(getattr(self.cfg, "pricing", None), "electricity_price_eur_per_kwh", 0.30) or 0.30)
            try:
                tariff = float(getattr(solar_cfg, "feed_in_tariff_eur_per_kwh", 0.082))
            except Exception:
                tariff = 0.082

            # Update cards
            self._solar_vars["feed_in"].set(f"{feed_in:.2f} kWh")
            self._solar_vars["grid_consumption"].set(f"{grid:.2f} kWh")

            if self_kwh is not None:
                self._solar_vars["self_consumption"].set(f"{self_kwh:.2f} kWh")
                self._solar_vars["cost_savings"].set(f"{self_kwh * unit_price:.2f} €")
            else:
                self._solar_vars["self_consumption"].set("–")
                self._solar_vars["cost_savings"].set("–")

            if autarky is not None:
                self._solar_vars["autarky"].set(f"{autarky:.1f} %")
            else:
                self._solar_vars["autarky"].set("–")

            if pv_kwh is not None:
                self._solar_vars["pv_production"].set(f"{pv_kwh:.2f} kWh")
            else:
                self._solar_vars["pv_production"].set("–")

            self._solar_vars["feed_in_revenue"].set(f"{feed_in * tariff:.2f} €")

            # CO₂ calculations
            co2_g_per_kwh = 380.0
            try:
                co2_g_per_kwh = float(getattr(getattr(self.cfg, "pricing", None), "co2_intensity_g_per_kwh", 380.0) or 380.0)
                # Try ENTSO-E average
                if hasattr(self, "_calc_co2_for_range"):
                    start_ts = int(start.timestamp())
                    end_ts = int(end.timestamp())
                    co2_result = self._calc_co2_for_range(start_ts, end_ts)
                    if co2_result and len(co2_result) >= 3 and co2_result[1] > 0:
                        co2_g_per_kwh = co2_result[1]
            except Exception:
                pass

            co2_saved_kg = (pv_kwh or 0.0) * co2_g_per_kwh / 1000.0
            co2_grid_kg = (grid or 0.0) * co2_g_per_kwh / 1000.0
            self._solar_vars["co2_saved"].set(f"{co2_saved_kg:.2f} kg")
            self._solar_vars["co2_grid"].set(f"{co2_grid_kg:.2f} kg")
            # Equivalents: 22 kg CO₂/tree/year, 170 g CO₂/km car
            tree_days = co2_saved_kg / 22.0 * 365 if co2_saved_kg > 0 else 0.0
            car_km = co2_saved_kg / 0.170 if co2_saved_kg > 0 else 0.0
            self._solar_vars["co2_avoided_trees"].set(f"{tree_days:.0f} Baumt.")
            self._solar_vars["co2_avoided_car"].set(f"{car_km:.0f} km")

            # Info label
            if household is None:
                self._solar_info_var.set(self.t("solar.no_other_devices"))
            else:
                self._solar_info_var.set("")

            # Update chart
            self._refresh_solar_chart(start, end)

        except Exception as exc:
            logger.warning("Solar tab refresh error: %s", exc)

    def _refresh_solar_chart(self, start: datetime, end: datetime) -> None:
        """Redraw the daily bar chart."""
        try:
            ax = self._solar_ax
            ax.clear()

            daily = self._solar_calc_daily_profile(start, end)
            if daily is None or daily.empty:
                ax.text(
                    0.5, 0.5, self.t("plots.no_data"),
                    ha="center", va="center", transform=ax.transAxes, color="gray"
                )
                self._solar_canvas_widget.draw_idle()
                return

            import matplotlib.dates as mdates
            dates = pd.to_datetime(daily["date"])
            x = np.arange(len(dates))

            bar_w = 0.35
            ax.bar(x - bar_w / 2, daily["feed_in"].values, bar_w,
                   label=self.t("solar.chart.feed_in"), color="#f59e0b", alpha=0.85)
            ax.bar(x + bar_w / 2, daily["grid"].values, bar_w,
                   label=self.t("solar.chart.grid"), color="#3b82f6", alpha=0.85)

            # Apply theme
            try:
                self._apply_plot_theme(self._solar_fig, ax)
            except Exception:
                pass

            ax.set_xticks(x)
            if len(dates) <= 14:
                ax.set_xticklabels(
                    [d.strftime("%d.%m.") for d in dates],
                    rotation=30,
                    ha="right",
                    fontsize=8,
                )
            else:
                ax.set_xticklabels(
                    [d.strftime("%d.%m.") if i % max(1, len(dates) // 12) == 0 else ""
                     for i, d in enumerate(dates)],
                    rotation=30,
                    ha="right",
                    fontsize=8,
                )

            ax.set_ylabel("kWh", fontsize=9)
            ax.legend(fontsize=8, loc="upper right")
            self._solar_fig.tight_layout()
            self._solar_canvas_widget.draw_idle()
        except Exception as exc:
            logger.debug("Solar chart error: %s", exc)

    # ── Summary text for Telegram / Webhook ──────────────────────────────────

    def _solar_build_summary_text(self, start: datetime, end: datetime) -> str:
        """Return a formatted solar summary string for Telegram/Webhook."""
        try:
            result = self._solar_calc_period(start, end)
            if result is None:
                return ""

            solar_cfg = getattr(self.cfg, "solar", None)
            tariff = float(getattr(solar_cfg, "feed_in_tariff_eur_per_kwh", 0.082) or 0.082)
            try:
                unit_price = float(self.cfg.pricing.unit_price_gross())
            except Exception:
                unit_price = 0.30

            feed_in = result["feed_in_kwh"]
            grid = result["grid_kwh"]
            self_kwh = result["self_kwh"]
            pv_kwh = result["pv_kwh"]
            autarky = result["autarky_pct"]

            lines = [f"\n☀️ Solar:"]
            lines.append(f"  {self.t('solar.summary.feed_in')}: {feed_in:.2f} kWh  (+{feed_in * tariff:.2f} €)")
            lines.append(f"  {self.t('solar.summary.grid')}: {grid:.2f} kWh")
            if self_kwh is not None:
                lines.append(f"  {self.t('solar.summary.self')}: {self_kwh:.2f} kWh  (↓{self_kwh * unit_price:.2f} €)")
            if autarky is not None:
                lines.append(f"  {self.t('solar.summary.autarky')}: {autarky:.1f} %")
            if pv_kwh is not None:
                lines.append(f"  {self.t('solar.summary.pv')}: {pv_kwh:.2f} kWh")
            return "\n".join(lines)
        except Exception:
            return ""
