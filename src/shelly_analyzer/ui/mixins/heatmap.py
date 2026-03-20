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
        ttk.Button(top, text=self.t("heatmap.refresh"), command=self._refresh_heatmap).pack(side="right")

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
            values=["kWh", "€"], width=7, state="readonly",
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

        # ── Scrollable content area ───────────────────────────────────────────
        outer = ttk.Frame(frm)
        outer.pack(fill="both", expand=True)

        scroll_canvas = tk.Canvas(outer, highlightthickness=0)
        scrollbar = ttk.Scrollbar(outer, orient="vertical", command=scroll_canvas.yview)
        self._heatmap_scroll_frame = ttk.Frame(scroll_canvas)
        self._heatmap_scroll_frame.bind(
            "<Configure>",
            lambda e: scroll_canvas.configure(scrollregion=scroll_canvas.bbox("all")),
        )
        _hm_win = scroll_canvas.create_window((0, 0), window=self._heatmap_scroll_frame, anchor="nw")
        scroll_canvas.configure(yscrollcommand=scrollbar.set)
        scroll_canvas.bind(
            "<Configure>",
            lambda e: scroll_canvas.itemconfigure(_hm_win, width=e.width),
        )
        scroll_canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        def _mwheel(event):
            try:
                scroll_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
            except Exception:
                pass

        scroll_canvas.bind_all("<MouseWheel>", _mwheel)

        scroll = self._heatmap_scroll_frame

        # Resolve initial background colour so plots don't flash white
        try:
            _init_bg = "#111111" if self._resolve_plot_theme() == "night" else "#FFFFFF"
        except Exception:
            _init_bg = "#FFFFFF"

        # ── Section 1: Calendar heatmap ───────────────────────────────────────
        sec1 = ttk.LabelFrame(scroll, text=self.t("heatmap.calendar.title"))
        sec1.pack(fill="x", padx=12, pady=(10, 4))

        self._heatmap_cal_fig = Figure(figsize=(14, 2.8), dpi=100)
        self._heatmap_cal_fig.patch.set_facecolor(_init_bg)
        self._heatmap_cal_ax = self._heatmap_cal_fig.add_subplot(111)
        self._heatmap_cal_canvas = FigureCanvasTkAgg(self._heatmap_cal_fig, master=sec1)
        self._heatmap_cal_canvas.get_tk_widget().configure(bg=_init_bg)
        self._heatmap_cal_canvas.get_tk_widget().pack(fill="x", expand=True, padx=4, pady=4)

        # ── Section 2: Hour × Weekday heatmap ────────────────────────────────
        sec2 = ttk.LabelFrame(scroll, text=self.t("heatmap.hourly.title"))
        sec2.pack(fill="x", padx=12, pady=(4, 12))

        self._heatmap_hourly_fig = Figure(figsize=(14, 3.5), dpi=100)
        self._heatmap_hourly_fig.patch.set_facecolor(_init_bg)
        self._heatmap_hourly_ax = self._heatmap_hourly_fig.add_subplot(111)
        self._heatmap_hourly_canvas = FigureCanvasTkAgg(self._heatmap_hourly_fig, master=sec2)
        self._heatmap_hourly_canvas.get_tk_widget().configure(bg=_init_bg)
        self._heatmap_hourly_canvas.get_tk_widget().pack(fill="x", expand=True, padx=4, pady=4)

        # Initial render (deferred so the tab has time to lay out)
        self.after(700, self._refresh_heatmap)

    # ── Data helpers ──────────────────────────────────────────────────────────

    def _heatmap_load_df(self, device_key: str, year: int):
        """Load a year's worth of samples for *device_key* as a DataFrame.

        Returns None if no data is available.
        """
        try:
            import pandas as pd

            start_ts = int(datetime(year, 1, 1).timestamp())
            end_ts = int(datetime(year + 1, 1, 1).timestamp())
            df = self.storage.read_device_df(device_key, start_ts=start_ts, end_ts=end_ts)
            if df is None or df.empty:
                return None
            # Ensure we have an energy_kwh column
            if "energy_kwh" not in df.columns:
                return None

            # Normalize timestamp columns to Unix integer seconds.
            # query_samples() converts raw SQLite integers to naive UTC datetime64
            # (nanoseconds). The heatmap helpers call datetime.fromtimestamp(int(ts))
            # which expects Unix seconds, so we convert back here.
            for col_name in ("timestamp", "ts"):
                if col_name in df.columns and pd.api.types.is_datetime64_any_dtype(df[col_name]):
                    df = df.copy()
                    df[col_name] = df[col_name].astype("int64") // 10 ** 9
                    break

            return df
        except Exception as e:
            logger.debug("heatmap load_df error for '%s' year=%s: %s", device_key, year, e)
            return None

    def _heatmap_daily(self, df, use_eur: bool, price_kwh: float) -> Dict[str, float]:
        """Aggregate DataFrame to daily totals.  Returns {date_str: value}."""
        result: Dict[str, float] = {}
        try:
            import pandas as pd

            ts_col = "timestamp" if "timestamp" in df.columns else "ts"

            # Convert Unix timestamp → local date
            # datetime.fromtimestamp uses local timezone (DST-aware)
            local_dates = df[ts_col].apply(lambda ts: datetime.fromtimestamp(int(ts)).date())
            kwh_series = df["energy_kwh"].fillna(0.0)

            tmp = df.copy()
            tmp["_date"] = local_dates
            tmp["_kwh"] = kwh_series
            daily = tmp.groupby("_date")["_kwh"].sum()

            for d, kwh in daily.items():
                val = float(kwh) * price_kwh if use_eur else float(kwh)
                result[d.strftime("%Y-%m-%d")] = max(0.0, val)
        except Exception as e:
            logger.debug("heatmap _daily error: %s", e)
        return result

    def _heatmap_hourly(self, df, use_eur: bool, price_kwh: float) -> Dict[Tuple[int, int], float]:
        """Aggregate DataFrame to weekday×hour totals (summed, not averaged).

        Returns {(weekday, hour): value}  where weekday 0=Mon … 6=Sun.
        """
        result: Dict[Tuple[int, int], float] = {}
        try:
            ts_col = "timestamp" if "timestamp" in df.columns else "ts"

            def _wd_hr(ts: int) -> Tuple[int, int]:
                dt = datetime.fromtimestamp(int(ts))
                return dt.weekday(), dt.hour  # weekday(): 0=Mon…6=Sun

            tmp = df.copy()
            tmp["_wd_hr"] = tmp[ts_col].apply(_wd_hr)
            tmp["_kwh"] = df["energy_kwh"].fillna(0.0)
            grouped = tmp.groupby("_wd_hr")["_kwh"].sum()

            for (wd, hr), kwh in grouped.items():
                val = float(kwh) * price_kwh if use_eur else float(kwh)
                result[(int(wd), int(hr))] = max(0.0, val)
        except Exception as e:
            logger.debug("heatmap _hourly error: %s", e)
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

            use_eur = self._heatmap_unit_var.get() == "€"
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

            daily = self._heatmap_daily(df, use_eur, price_kwh)
            hourly = self._heatmap_hourly(df, use_eur, price_kwh)

            self._draw_calendar_heatmap(daily, year, use_eur)
            self._draw_hourly_heatmap(hourly, year, use_eur)
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

        for fig, ax, canvas in (
            (self._heatmap_cal_fig, self._heatmap_cal_ax, self._heatmap_cal_canvas),
            (self._heatmap_hourly_fig, self._heatmap_hourly_ax, self._heatmap_hourly_canvas),
        ):
            try:
                ax.clear()
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
    ) -> None:
        """Draw a GitHub-contribution-style calendar heatmap."""
        try:
            fig = self._heatmap_cal_fig
            ax = self._heatmap_cal_ax
            canvas = self._heatmap_cal_canvas

            ax.clear()

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
                        z[row, col] = daily.get(key, 0.0)
                    d += timedelta(days=1)

            # Choose colormap and range
            cmap = "YlOrRd"
            valid = z[~np.isnan(z)]
            vmax = float(np.max(valid)) if len(valid) > 0 and np.max(valid) > 0 else 1.0

            x = np.arange(n_cols + 1)
            y = np.arange(n_rows + 1)
            masked = np.ma.masked_invalid(z)
            pc = ax.pcolormesh(x, y, masked, cmap=cmap, vmin=0, vmax=vmax)

            # Colorbar
            try:
                unit_label = "€" if use_eur else "kWh"
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

            unit_label = "€" if use_eur else "kWh"
            ax.set_title(
                self.t("heatmap.calendar.subtitle", year=year, unit=unit_label),
                color=fg, fontsize=10, pad=4,
            )

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
    ) -> None:
        """Draw a weekday × hour heatmap."""
        try:
            fig = self._heatmap_hourly_fig
            ax = self._heatmap_hourly_ax
            canvas = self._heatmap_hourly_canvas

            ax.clear()

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

            cmap = "YlOrRd"
            vmax = float(z.max()) if z.max() > 0 else 1.0

            x = np.arange(25)   # hour boundaries 0..24
            y = np.arange(8)    # weekday boundaries 0..7
            pc = ax.pcolormesh(x, y, z, cmap=cmap, vmin=0, vmax=vmax)

            # Colorbar
            try:
                unit_label = "€" if use_eur else "kWh"
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
            ax.set_ylim(0, 7)

            for spine in ax.spines.values():
                spine.set_color(fg)
            ax.tick_params(axis="both", colors=fg)

            unit_label = "€" if use_eur else "kWh"
            ax.set_title(
                self.t("heatmap.hourly.subtitle", year=year, unit=unit_label),
                color=fg, fontsize=10, pad=4,
            )
            ax.set_xlabel(self.t("heatmap.hourly.xlabel"), color=fg, fontsize=9)
            ax.set_ylabel(self.t("heatmap.hourly.ylabel"), color=fg, fontsize=9)

            try:
                canvas.get_tk_widget().configure(bg=bg)
            except Exception:
                pass

            fig.tight_layout(pad=0.5)
            canvas.draw()
        except Exception as e:
            logger.debug("_draw_hourly_heatmap error: %s", e)

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
