"""Heatmap tab mixin for Shelly Energy Analyzer.

Provides two visualizations:
- Calendar heatmap (GitHub-contribution-graph style): daily kWh/€ for a full year
- Weekday × Hour heatmap: shows when consumption is typically highest
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
import tkinter as tk
from tkinter import ttk
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

logger = logging.getLogger(__name__)


class HeatmapMixin:
    """Heatmap tab: calendar heatmap + weekday×hour heatmap."""

    def _build_heatmap_tab(self) -> None:
        """Build the heatmap tab UI."""
        frm = self.tab_heatmap

        # ── Top bar: title + refresh ──────────────────────────────────────────
        top = ttk.Frame(frm)
        top.pack(fill="x", padx=14, pady=(12, 4))
        ttk.Label(top, text=self.t("heatmap.title"), font=("", 14, "bold")).pack(side="left")

        # ── Controls: device, unit, year ──────────────────────────────────────
        ctrl = ttk.Frame(frm)
        ctrl.pack(fill="x", padx=14, pady=(0, 8))

        ttk.Label(ctrl, text=self.t("heatmap.device") + ":").pack(side="left", padx=(0, 4))
        self._heatmap_device_var = tk.StringVar()
        dev_names = [d.name for d in (getattr(self.cfg, "devices", []) or [])]
        self._heatmap_dev_keys = [d.key for d in (getattr(self.cfg, "devices", []) or [])]
        self._heatmap_dev_names = dev_names
        dev_cb = ttk.Combobox(
            ctrl, textvariable=self._heatmap_device_var,
            values=dev_names, width=26, state="readonly",
        )
        if dev_names:
            dev_cb.current(0)
        dev_cb.pack(side="left", padx=(0, 14))

        ttk.Label(ctrl, text=self.t("heatmap.unit") + ":").pack(side="left", padx=(0, 4))
        self._heatmap_unit_var = tk.StringVar(value="kWh")
        unit_cb = ttk.Combobox(
            ctrl, textvariable=self._heatmap_unit_var,
            values=["kWh", "€", "g CO₂"], width=7, state="readonly",
        )
        unit_cb.current(0)
        unit_cb.pack(side="left", padx=(0, 14))

        ttk.Label(ctrl, text=self.t("heatmap.year") + ":").pack(side="left", padx=(0, 4))
        cur_year = datetime.now().year
        self._heatmap_year_var = tk.StringVar(value=str(cur_year))
        years = [str(y) for y in range(cur_year, cur_year - 6, -1)]
        year_cb = ttk.Combobox(
            ctrl, textvariable=self._heatmap_year_var,
            values=years, width=8, state="readonly",
        )
        year_cb.current(0)
        year_cb.pack(side="left", padx=(0, 14))

        # Bind combobox changes to auto-refresh
        dev_cb.bind("<<ComboboxSelected>>", lambda _e: self.after(50, self._refresh_heatmap))
        unit_cb.bind("<<ComboboxSelected>>", lambda _e: self.after(50, self._refresh_heatmap))
        year_cb.bind("<<ComboboxSelected>>", lambda _e: self.after(50, self._refresh_heatmap))

        # ── Content area (fills available space in both directions) ──────────
        outer = ttk.Frame(frm)
        outer.pack(fill="both", expand=True)
        outer.rowconfigure(0, weight=1)
        outer.rowconfigure(1, weight=1)
        outer.columnconfigure(0, weight=1)

        scroll = outer

        # Resolve initial background colour so plots don't flash white
        try:
            _init_bg = "#111111" if self._resolve_plot_theme() == "night" else "#FFFFFF"
        except Exception:
            _init_bg = "#FFFFFF"

        # ── Section 1: Calendar heatmap ───────────────────────────────────────
        sec1 = ttk.LabelFrame(scroll, text=self.t("heatmap.calendar.title"))
        sec1.pack(fill="both", expand=True, padx=12, pady=(10, 4))

        self._heatmap_cal_fig = Figure(figsize=(14, 2.8), dpi=100)
        self._heatmap_cal_fig.patch.set_facecolor(_init_bg)
        self._heatmap_cal_ax = self._heatmap_cal_fig.add_subplot(111)
        self._heatmap_cal_canvas = FigureCanvasTkAgg(self._heatmap_cal_fig, master=sec1)
        self._heatmap_cal_canvas.get_tk_widget().configure(bg=_init_bg)
        self._heatmap_cal_canvas.get_tk_widget().pack(fill="both", expand=True, padx=4, pady=4)

        # ── Section 2: Hour × Weekday heatmap ────────────────────────────────
        sec2 = ttk.LabelFrame(scroll, text=self.t("heatmap.hourly.title"))
        sec2.pack(fill="both", expand=True, padx=12, pady=(4, 12))

        self._heatmap_hourly_fig = Figure(figsize=(14, 3.5), dpi=100)
        self._heatmap_hourly_fig.patch.set_facecolor(_init_bg)
        self._heatmap_hourly_ax = self._heatmap_hourly_fig.add_subplot(111)
        self._heatmap_hourly_canvas = FigureCanvasTkAgg(self._heatmap_hourly_fig, master=sec2)
        self._heatmap_hourly_canvas.get_tk_widget().configure(bg=_init_bg)
        self._heatmap_hourly_canvas.get_tk_widget().pack(fill="both", expand=True, padx=4, pady=4)

        # ── Tooltip labels (float over the canvas) ────────────────────────────
        _tt_opts = dict(
            text="", bg="#1e1e1e", fg="#ffffff", font=("", 9),
            padx=7, pady=4, relief="flat", bd=0,
        )
        self._heatmap_tooltip_cal = tk.Label(
            self._heatmap_cal_canvas.get_tk_widget(), **_tt_opts
        )
        self._heatmap_tooltip_hourly = tk.Label(
            self._heatmap_hourly_canvas.get_tk_widget(), **_tt_opts
        )

        # Initialise data stores used by tooltip callbacks
        self._heatmap_cal_z: Optional[np.ndarray] = None
        self._heatmap_cal_grid_start: Optional[date] = None
        self._heatmap_cal_year: Optional[int] = None
        self._heatmap_cal_use_eur: bool = False
        self._heatmap_cal_use_co2: bool = False
        self._heatmap_hourly_z: Optional[np.ndarray] = None
        self._heatmap_hourly_use_eur: bool = False
        self._heatmap_hourly_use_co2: bool = False

        # Connect mouse-motion events (connections survive fig.clf())
        self._heatmap_cal_canvas.mpl_connect(
            "motion_notify_event", self._heatmap_cal_motion
        )
        self._heatmap_cal_canvas.mpl_connect(
            "figure_leave_event",
            lambda _e: self._heatmap_tooltip_cal.place_forget(),
        )
        self._heatmap_hourly_canvas.mpl_connect(
            "motion_notify_event", self._heatmap_hourly_motion
        )
        self._heatmap_hourly_canvas.mpl_connect(
            "figure_leave_event",
            lambda _e: self._heatmap_tooltip_hourly.place_forget(),
        )

        # Initial render (deferred so the tab has time to lay out)
        self.after(700, self._refresh_heatmap)

    # ── Data helpers ──────────────────────────────────────────────────────────

    def _heatmap_load_df(self, device_key: str, year: int):
        """Load a year's hourly-aggregated energy data for *device_key*.

        Returns a DataFrame with columns:
            timestamp   – Unix integer seconds (start of the hour)
            energy_kwh  – kWh consumed in that hour

        Returns None if no data is available.

        Data sources (in preference order):
        1. ``hourly_energy`` table — per-hour kWh, always correct scale.
           Available for years that have not been compressed by the retention
           policy (typically the current year and the previous year).
        2. ``monthly_energy`` table — compressed historical data.
           Each monthly total is distributed evenly across all hours of that
           month so that the heatmap colour scale stays meaningful.

        Using ``read_device_df()`` / ``query_samples()`` is intentionally
        avoided here: that method merges raw per-sample rows (~0.001 kWh each)
        with monthly-aggregate rows (~150 kWh each) into a single DataFrame,
        causing one extreme outlier that forces the entire colour scale to
        maximum and renders every other cell yellow.
        """
        try:
            import pandas as pd
            from calendar import monthrange

            start_ts = int(datetime(year, 1, 1).timestamp())
            end_ts = int(datetime(year + 1, 1, 1).timestamp())

            # ── 1. Hourly energy (primary) ────────────────────────────────────
            try:
                hourly_df = self.storage.db.query_hourly(
                    device_key, start_ts=start_ts, end_ts=end_ts
                )
                if not hourly_df.empty and "kwh" in hourly_df.columns:
                    ts_col = "hour_ts" if "hour_ts" in hourly_df.columns else "timestamp"
                    return pd.DataFrame({
                        "timestamp": hourly_df[ts_col].astype("int64"),
                        "energy_kwh": hourly_df["kwh"].fillna(0.0),
                    })
            except Exception as e:
                logger.debug(
                    "heatmap: hourly query failed for '%s' year=%s: %s", device_key, year, e
                )

            # ── 2. Monthly energy fallback (compressed historical years) ──────
            # Distribute each month's total evenly across all hours of that
            # month so the colour scale stays proportional.
            try:
                monthly_df = self.storage.db.query_monthly(
                    device_key, start_ts=start_ts, end_ts=end_ts
                )
                if not monthly_df.empty and "energy_kwh" in monthly_df.columns:
                    rows = []
                    for _, row in monthly_df.iterrows():
                        ts_val = row.get("timestamp")
                        if ts_val is None:
                            continue
                        ts_int = (
                            int(ts_val.timestamp())
                            if hasattr(ts_val, "timestamp")
                            else int(ts_val)
                        )
                        dt = datetime.fromtimestamp(ts_int)
                        days_in_month = monthrange(dt.year, dt.month)[1]
                        hours_in_month = days_in_month * 24
                        monthly_kwh = float(row.get("energy_kwh") or 0.0)
                        kwh_per_hour = (
                            monthly_kwh / hours_in_month if hours_in_month > 0 else 0.0
                        )
                        base_ts = int(datetime(dt.year, dt.month, 1).timestamp())
                        for h in range(hours_in_month):
                            h_ts = base_ts + h * 3600
                            if start_ts <= h_ts < end_ts:
                                rows.append({"timestamp": h_ts, "energy_kwh": kwh_per_hour})
                    if rows:
                        return pd.DataFrame(rows)
            except Exception as e:
                logger.debug(
                    "heatmap: monthly fallback failed for '%s' year=%s: %s", device_key, year, e
                )

            return None
        except Exception as e:
            logger.debug("heatmap load_df error for '%s' year=%s: %s", device_key, year, e)
            return None

    def _heatmap_daily(self, df, use_eur: bool, price_kwh: float, use_co2: bool = False) -> Dict[str, float]:
        """Aggregate DataFrame to daily totals.  Returns {date_str: value}."""
        result: Dict[str, float] = {}
        try:
            import pandas as pd

            ts_col = "timestamp" if "timestamp" in df.columns else "ts"

            # Convert Unix timestamp → local date
            # datetime.fromtimestamp uses local timezone (DST-aware)
            local_dates = df[ts_col].apply(lambda ts: datetime.fromtimestamp(int(ts)).date())

            tmp = df.copy()
            tmp["_date"] = local_dates

            if use_co2 and "co2_g" in df.columns:
                tmp["_val"] = df["co2_g"].fillna(0.0)
            else:
                tmp["_val"] = df["energy_kwh"].fillna(0.0)

            daily = tmp.groupby("_date")["_val"].sum()

            for d, val in daily.items():
                v = float(val)
                if use_eur and not use_co2:
                    v = v * price_kwh
                result[d.strftime("%Y-%m-%d")] = max(0.0, v)
        except Exception as e:
            logger.debug("heatmap _daily error: %s", e)
        return result

    def _heatmap_hourly(self, df, use_eur: bool, price_kwh: float, use_co2: bool = False) -> Dict[Tuple[int, int], float]:
        """Aggregate DataFrame to weekday×hour averages.

        Returns {(weekday, hour): value}  where weekday 0=Mon … 6=Sun.
        Each cell holds the *mean* kWh (or g CO₂) across all occurrences of
        that weekday/hour combination in the selected year (e.g. ~52 Thursdays),
        so the value represents a typical hourly consumption rather than a
        yearly sum.
        """
        result: Dict[Tuple[int, int], float] = {}
        try:
            ts_col = "timestamp" if "timestamp" in df.columns else "ts"

            def _wd_hr(ts: int) -> Tuple[int, int]:
                dt = datetime.fromtimestamp(int(ts))
                return dt.weekday(), dt.hour  # weekday(): 0=Mon…6=Sun

            tmp = df.copy()
            tmp["_wd_hr"] = tmp[ts_col].apply(_wd_hr)

            if use_co2 and "co2_g" in df.columns:
                tmp["_val"] = df["co2_g"].fillna(0.0)
            else:
                tmp["_val"] = df["energy_kwh"].fillna(0.0)

            grouped = tmp.groupby("_wd_hr")["_val"].mean()

            for (wd, hr), val in grouped.items():
                v = float(val)
                if use_eur and not use_co2:
                    v = v * price_kwh
                result[(int(wd), int(hr))] = max(0.0, v)
        except Exception as e:
            logger.debug("heatmap _hourly error: %s", e)
        return result

    def _heatmap_enrich_co2(self, df, device_key: str, year: int):
        """Add a ``co2_g`` column by joining hourly energy with CO₂ intensity.

        If real ENTSO-E data is available it is used; otherwise a static
        fallback factor from the pricing config is applied.
        """
        import pandas as pd

        ts_col = "timestamp" if "timestamp" in df.columns else "ts"
        result = df.copy()
        result["co2_g"] = 0.0

        co2_cfg = getattr(self.cfg, "co2", None)
        zone = getattr(co2_cfg, "bidding_zone", "DE_LU") or "DE_LU"

        start_ts = int(datetime(year, 1, 1).timestamp())
        end_ts = int(datetime(year + 1, 1, 1).timestamp())

        entsoe_used = False
        try:
            entsoe_token = getattr(co2_cfg, "entsoe_token", "") or ""
            if entsoe_token and hasattr(self.storage, "db"):
                df_co2 = self.storage.db.query_co2_intensity(zone, start_ts, end_ts)
                if not df_co2.empty:
                    # Align on hour_ts
                    merged = pd.merge(
                        result[[ts_col, "energy_kwh"]].rename(columns={ts_col: "hour_ts"}),
                        df_co2[["hour_ts", "intensity_g_per_kwh"]],
                        on="hour_ts", how="left",
                    )
                    # Fill missing intensities with NaN (will use fallback below)
                    matched = merged["intensity_g_per_kwh"].notna()
                    if matched.any():
                        result.loc[matched.values, "co2_g"] = (
                            merged.loc[matched, "energy_kwh"].values
                            * merged.loc[matched, "intensity_g_per_kwh"].values
                        )
                        entsoe_used = True
                        # For rows without ENTSO-E data, use static fallback
                        unmatched = ~matched
                        if unmatched.any():
                            try:
                                fallback_g = float(
                                    getattr(getattr(self.cfg, "pricing", None),
                                            "co2_intensity_g_per_kwh", 380.0) or 380.0
                                )
                            except Exception:
                                fallback_g = 380.0
                            result.loc[unmatched.values, "co2_g"] = (
                                result.loc[unmatched.values, "energy_kwh"].fillna(0.0) * fallback_g
                            )
        except Exception as e:
            logger.debug("heatmap CO₂ ENTSO-E join failed: %s", e)

        if not entsoe_used:
            # Static fallback: use configured g/kWh
            try:
                fallback_g = float(
                    getattr(getattr(self.cfg, "pricing", None),
                            "co2_intensity_g_per_kwh", 380.0) or 380.0
                )
            except Exception:
                fallback_g = 380.0
            result["co2_g"] = result["energy_kwh"].fillna(0.0) * fallback_g

        return result

    # ── Public refresh ────────────────────────────────────────────────────────

    def _refresh_heatmap(self) -> None:
        """Read selections, query data, and redraw both heatmaps."""
        # Guard: tab might not be built yet
        if not hasattr(self, "_heatmap_cal_fig"):
            return
        try:
            dev_names = getattr(self, "_heatmap_dev_names", [])
            dev_keys = getattr(self, "_heatmap_dev_keys", [])
            if not dev_keys:
                return

            sel_name = self._heatmap_device_var.get()
            try:
                idx = dev_names.index(sel_name)
                device_key = dev_keys[idx]
            except (ValueError, IndexError):
                device_key = dev_keys[0]

            unit_sel = self._heatmap_unit_var.get()
            use_eur = unit_sel == "€"
            use_co2 = unit_sel == "g CO₂"
            try:
                year = int(self._heatmap_year_var.get())
            except Exception:
                year = datetime.now().year

            try:
                price_kwh = float(
                    getattr(getattr(self.cfg, "pricing", None), "price_per_kwh", 0.0) or 0.0
                )
            except Exception:
                price_kwh = 0.0

            df = self._heatmap_load_df(device_key, year)
            if df is None:
                self._heatmap_show_no_data()
                return

            if use_co2:
                # Join hourly energy with CO₂ intensity from ENTSO-E
                df = self._heatmap_enrich_co2(df, device_key, year)

            daily = self._heatmap_daily(df, use_eur, price_kwh, use_co2)
            hourly = self._heatmap_hourly(df, use_eur, price_kwh, use_co2)

            self._draw_calendar_heatmap(daily, year, use_eur, use_co2)
            self._draw_hourly_heatmap(hourly, year, use_eur, use_co2)
        except Exception as e:
            logger.debug("_refresh_heatmap error: %s", e)

    def _heatmap_show_no_data(self) -> None:
        """Show "no data" message in both plots."""
        try:
            theme = self._resolve_plot_theme()
        except Exception:
            theme = "day"
        bg = "#111111" if theme == "night" else "#FFFFFF"
        fg = "#E6E6E6" if theme == "night" else "#000000"
        no_data = self.t("plots.no_data")

        for fig, ax_attr, canvas in (
            (self._heatmap_cal_fig, "_heatmap_cal_ax", self._heatmap_cal_canvas),
            (self._heatmap_hourly_fig, "_heatmap_hourly_ax", self._heatmap_hourly_canvas),
        ):
            try:
                fig.clf()
                ax = fig.add_subplot(111)
                setattr(self, ax_attr, ax)
                fig.patch.set_facecolor(bg)
                ax.set_facecolor(bg)
                ax.text(
                    0.5, 0.5, no_data,
                    ha="center", va="center", color=fg,
                    transform=ax.transAxes, fontsize=13,
                )
                ax.set_xticks([])
                ax.set_yticks([])
                for spine in ax.spines.values():
                    spine.set_color(fg)
                canvas.get_tk_widget().configure(bg=bg)
                canvas.draw()
            except Exception:
                pass

    # ── Calendar heatmap ─────────────────────────────────────────────────────

    def _draw_calendar_heatmap(
        self,
        daily: Dict[str, float],
        year: int,
        use_eur: bool,
        use_co2: bool = False,
    ) -> None:
        """Draw a GitHub-contribution-style calendar heatmap."""
        try:
            fig = self._heatmap_cal_fig
            ax = self._heatmap_cal_ax
            canvas = self._heatmap_cal_canvas

            # Clear the entire figure (removes axes + colorbars) and recreate axes
            fig.clf()
            ax = fig.add_subplot(111)
            self._heatmap_cal_ax = ax

            try:
                theme = self._resolve_plot_theme()
            except Exception:
                theme = "day"
            bg = "#111111" if theme == "night" else "#FFFFFF"
            fg = "#E6E6E6" if theme == "night" else "#000000"

            fig.patch.set_facecolor(bg)
            ax.set_facecolor(bg)

            if not daily:
                ax.text(
                    0.5, 0.5, self.t("plots.no_data"),
                    ha="center", va="center", color=fg,
                    transform=ax.transAxes, fontsize=13,
                )
                ax.set_xticks([])
                ax.set_yticks([])
                canvas.get_tk_widget().configure(bg=bg)
                canvas.draw()
                return

            # Build a 7 (rows=days) × 53 (cols=weeks) grid.
            # Row 0 = Monday (top), Row 6 = Sunday (bottom).
            jan1 = date(year, 1, 1)
            # Shift back to the Monday that starts the first displayed week
            grid_start = jan1 - timedelta(days=jan1.weekday())

            n_cols, n_rows = 53, 7
            z = np.full((n_rows, n_cols), np.nan)

            d = grid_start
            for col in range(n_cols):
                for row in range(n_rows):
                    if d.year == year:
                        key = d.strftime("%Y-%m-%d")
                        val = daily.get(key)
                        if val is not None:
                            z[row, col] = val
                        # else: leave as NaN (no data / future day)
                    d += timedelta(days=1)

            # Choose colormap and range
            import matplotlib.pyplot as _plt
            cmap_name = "YlOrRd" if use_co2 else "RdYlGn_r"
            cmap = _plt.colormaps[cmap_name].copy()
            bad_color = "#333333" if theme == "night" else "#E0E0E0"
            cmap.set_bad(color=bad_color)
            valid = z[~np.isnan(z)]
            vmax = float(np.max(valid)) if len(valid) > 0 and np.max(valid) > 0 else 1.0

            x = np.arange(n_cols + 1)
            y = np.arange(n_rows + 1)
            masked = np.ma.masked_invalid(z)
            pc = ax.pcolormesh(x, y, masked, cmap=cmap, vmin=0, vmax=vmax)

            # Colorbar
            try:
                unit_label = "g CO₂" if use_co2 else ("€" if use_eur else "kWh")
                cb = fig.colorbar(pc, ax=ax, orientation="vertical", pad=0.01, fraction=0.025)
                cb.ax.tick_params(colors=fg, labelsize=8)
                cb.set_label(unit_label, color=fg, fontsize=9)
            except Exception:
                pass

            # Month tick marks on x-axis
            month_ticks: List[Tuple[float, str]] = []
            prev_month = -1
            d = grid_start
            for col in range(n_cols):
                if d.year == year and d.month != prev_month:
                    month_ticks.append((col + 0.5, d.strftime("%b")))
                    prev_month = d.month
                d += timedelta(weeks=1)

            ax.set_xticks([pos for pos, _ in month_ticks])
            ax.set_xticklabels([lbl for _, lbl in month_ticks], color=fg, fontsize=9)

            # Weekday labels on y-axis (Mon at top → row 0)
            day_labels = self._heatmap_day_labels()
            ax.set_yticks([i + 0.5 for i in range(7)])
            ax.set_yticklabels(day_labels, color=fg, fontsize=9)
            ax.invert_yaxis()

            ax.set_xlim(0, n_cols)
            ax.set_ylim(7, 0)

            # Styling
            for spine in ax.spines.values():
                spine.set_color(fg)
            ax.tick_params(axis="both", colors=fg)

            unit_label = "g CO₂" if use_co2 else ("€" if use_eur else "kWh")
            ax.set_title(
                self.t("heatmap.calendar.subtitle", year=year, unit=unit_label),
                color=fg, fontsize=10, pad=4,
            )

            # Store data for tooltip callbacks
            self._heatmap_cal_z = z
            self._heatmap_cal_grid_start = grid_start
            self._heatmap_cal_year = year
            self._heatmap_cal_use_eur = use_eur
            self._heatmap_cal_use_co2 = use_co2

            try:
                canvas.get_tk_widget().configure(bg=bg)
            except Exception:
                pass

            fig.tight_layout(pad=0.5)
            canvas.draw()
        except Exception as e:
            logger.debug("_draw_calendar_heatmap error: %s", e)

    # ── Hour × Weekday heatmap ────────────────────────────────────────────────

    def _draw_hourly_heatmap(
        self,
        hourly: Dict[Tuple[int, int], float],
        year: int,
        use_eur: bool,
        use_co2: bool = False,
    ) -> None:
        """Draw a weekday × hour heatmap."""
        try:
            fig = self._heatmap_hourly_fig
            ax = self._heatmap_hourly_ax
            canvas = self._heatmap_hourly_canvas

            # Clear the entire figure (removes axes + colorbars) and recreate axes
            fig.clf()
            ax = fig.add_subplot(111)
            self._heatmap_hourly_ax = ax

            try:
                theme = self._resolve_plot_theme()
            except Exception:
                theme = "day"
            bg = "#111111" if theme == "night" else "#FFFFFF"
            fg = "#E6E6E6" if theme == "night" else "#000000"

            fig.patch.set_facecolor(bg)
            ax.set_facecolor(bg)

            if not hourly:
                ax.text(
                    0.5, 0.5, self.t("plots.no_data"),
                    ha="center", va="center", color=fg,
                    transform=ax.transAxes, fontsize=13,
                )
                ax.set_xticks([])
                ax.set_yticks([])
                canvas.get_tk_widget().configure(bg=bg)
                canvas.draw()
                return

            # Build 7 (weekdays) × 24 (hours) grid
            z = np.zeros((7, 24))
            for (wd, hr), val in hourly.items():
                if 0 <= wd < 7 and 0 <= hr < 24:
                    z[wd, hr] = val

            import matplotlib.pyplot as _plt
            cmap_name = "YlOrRd" if use_co2 else "RdYlGn_r"
            cmap = _plt.colormaps[cmap_name].copy()
            vmax = float(z.max()) if z.max() > 0 else 1.0

            x = np.arange(25)   # hour boundaries 0..24
            y = np.arange(8)    # weekday boundaries 0..7
            pc = ax.pcolormesh(x, y, z, cmap=cmap, vmin=0, vmax=vmax)

            # Colorbar
            try:
                unit_label = "g CO₂" if use_co2 else ("€" if use_eur else "kWh")
                cb = fig.colorbar(pc, ax=ax, orientation="vertical", pad=0.01, fraction=0.025)
                cb.ax.tick_params(colors=fg, labelsize=8)
                cb.set_label(unit_label, color=fg, fontsize=9)
            except Exception:
                pass

            # Hour labels on x-axis (every 2 hours)
            ax.set_xticks([h + 0.5 for h in range(0, 24, 2)])
            ax.set_xticklabels(
                [f"{h:02d}:00" for h in range(0, 24, 2)],
                color=fg, fontsize=8, rotation=45, ha="right",
            )

            # Weekday labels on y-axis (Mon at top)
            day_labels = self._heatmap_day_labels()
            ax.set_yticks([i + 0.5 for i in range(7)])
            ax.set_yticklabels(day_labels, color=fg, fontsize=9)

            ax.set_xlim(0, 24)
            ax.set_ylim(7, 0)  # Mon (row 0) at top, Sun (row 6) at bottom

            for spine in ax.spines.values():
                spine.set_color(fg)
            ax.tick_params(axis="both", colors=fg)

            unit_label = "g CO₂" if use_co2 else ("€" if use_eur else "kWh")
            ax.set_title(
                self.t("heatmap.hourly.subtitle", year=year, unit=unit_label),
                color=fg, fontsize=10, pad=4,
            )
            ax.set_xlabel(self.t("heatmap.hourly.xlabel"), color=fg, fontsize=9)
            ax.set_ylabel(self.t("heatmap.hourly.ylabel"), color=fg, fontsize=9)

            # Store data for tooltip callbacks
            self._heatmap_hourly_z = z
            self._heatmap_hourly_use_eur = use_eur
            self._heatmap_hourly_use_co2 = use_co2

            try:
                canvas.get_tk_widget().configure(bg=bg)
            except Exception:
                pass

            fig.tight_layout(pad=0.5)
            canvas.draw()
        except Exception as e:
            logger.debug("_draw_hourly_heatmap error: %s", e)

    # ── Tooltip callbacks ─────────────────────────────────────────────────────

    def _heatmap_cal_motion(self, event) -> None:
        """Show a tooltip with date + value when hovering over the calendar heatmap."""
        tooltip = getattr(self, "_heatmap_tooltip_cal", None)
        if tooltip is None:
            return
        try:
            z = self._heatmap_cal_z
            grid_start = self._heatmap_cal_grid_start
            year = self._heatmap_cal_year

            if event.inaxes is None or z is None or grid_start is None or event.xdata is None:
                tooltip.place_forget()
                return

            col = int(event.xdata)
            row = int(event.ydata)

            if not (0 <= col < 53 and 0 <= row < 7):
                tooltip.place_forget()
                return

            val = z[row, col]
            if np.isnan(val):
                tooltip.place_forget()
                return

            d = grid_start + timedelta(days=col * 7 + row)
            if d.year != year:
                tooltip.place_forget()
                return

            if getattr(self, "_heatmap_cal_use_co2", False):
                unit = "g CO₂"
            else:
                unit = "€" if self._heatmap_cal_use_eur else "kWh"
            text = f"{d.strftime('%d.%m.%Y')}: {val:.2f} {unit}"

            canvas_h = self._heatmap_cal_canvas.get_tk_widget().winfo_height()
            tx = int(event.x) + 12
            ty = max(2, canvas_h - int(event.y) - 32)

            tooltip.config(text=text)
            tooltip.lift()
            tooltip.place(x=tx, y=ty)
        except Exception:
            tooltip.place_forget()

    def _heatmap_hourly_motion(self, event) -> None:
        """Show a tooltip with weekday + hour + value when hovering over the hourly heatmap."""
        tooltip = getattr(self, "_heatmap_tooltip_hourly", None)
        if tooltip is None:
            return
        try:
            z = self._heatmap_hourly_z

            if event.inaxes is None or z is None or event.xdata is None:
                tooltip.place_forget()
                return

            col = int(event.xdata)   # hour  0-23
            row = int(event.ydata)   # weekday 0-6 (Mon=0)

            if not (0 <= col < 24 and 0 <= row < 7):
                tooltip.place_forget()
                return

            val = z[row, col]
            day_labels = self._heatmap_day_labels()
            day_name = day_labels[row]
            if getattr(self, "_heatmap_hourly_use_co2", False):
                unit = "g CO₂"
            else:
                unit = "€" if self._heatmap_hourly_use_eur else "kWh"
            text = f"{day_name} {col:02d}:00–{col + 1:02d}:00: {val:.2f} {unit}"

            canvas_h = self._heatmap_hourly_canvas.get_tk_widget().winfo_height()
            tx = int(event.x) + 12
            ty = max(2, canvas_h - int(event.y) - 32)

            tooltip.config(text=text)
            tooltip.lift()
            tooltip.place(x=tx, y=ty)
        except Exception:
            tooltip.place_forget()

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _heatmap_day_labels(self) -> List[str]:
        """Return localized day-of-week abbreviations Mon→Sun."""
        try:
            return [
                self.t("heatmap.day.mon"),
                self.t("heatmap.day.tue"),
                self.t("heatmap.day.wed"),
                self.t("heatmap.day.thu"),
                self.t("heatmap.day.fri"),
                self.t("heatmap.day.sat"),
                self.t("heatmap.day.sun"),
            ]
        except Exception:
            return ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"]
