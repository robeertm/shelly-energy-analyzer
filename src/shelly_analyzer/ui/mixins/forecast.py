"""Forecast tab mixin – consumption forecasting with trend analysis."""
from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure


class ForecastMixin:
    """Adds the Forecast tab to the main application."""

    def _build_forecast_tab(self) -> None:
        frm = self.tab_forecast

        # ── Top bar ──────────────────────────────────────────────────────
        top = ttk.Frame(frm)
        top.pack(fill="x", padx=14, pady=(12, 4))
        ttk.Label(top, text=self.t("forecast.title"), font=("", 14, "bold")).pack(side="left")

        # ── Device selector ──────────────────────────────────────────────
        ctrl = ttk.Frame(frm)
        ctrl.pack(fill="x", padx=14, pady=(0, 6))
        self._forecast_dev_var = tk.StringVar()
        dev_names = [d.name for d in self.cfg.devices]
        cb = ttk.Combobox(ctrl, textvariable=self._forecast_dev_var, values=dev_names,
                         state="readonly", width=30)
        cb.pack(side="left", padx=(0, 6))
        if dev_names:
            cb.current(0)
        cb.bind("<<ComboboxSelected>>", lambda e: self._refresh_forecast_tab())

        # ── Content area (fills both directions like heatmap) ────────────
        content = ttk.Frame(frm)
        content.pack(fill="both", expand=True)
        content.rowconfigure(0, weight=0)  # cards
        content.rowconfigure(1, weight=3)  # main chart
        content.rowconfigure(2, weight=2)  # profiles
        content.columnconfigure(0, weight=1)

        # ── Summary cards ────────────────────────────────────────────────
        cards = ttk.Frame(content)
        cards.grid(row=0, column=0, sticky="ew", padx=14, pady=(4, 4))
        cards.columnconfigure((0, 1, 2, 3), weight=1)

        self._forecast_vars = {}
        for col, (key, label_key, icon) in enumerate([
            ("avg_daily", "forecast.avg_daily", "📊"),
            ("trend", "forecast.trend", "📈"),
            ("next_month", "forecast.next_month", "📅"),
            ("next_year", "forecast.next_year", "🗓️"),
        ]):
            card = ttk.LabelFrame(cards, text=f"{icon} {self.t(label_key)}")
            card.grid(row=0, column=col, sticky="nsew", padx=3, pady=3)
            v = tk.StringVar(value="–")
            self._forecast_vars[key] = v
            ttk.Label(card, textvariable=v, font=("", 13, "bold")).pack(anchor="center", padx=8, pady=8)

        # ── Main chart ───────────────────────────────────────────────────
        chart_lf = ttk.LabelFrame(content, text=self.t("forecast.chart.title"))
        chart_lf.grid(row=1, column=0, sticky="nsew", padx=14, pady=(4, 4))

        self._forecast_fig = Figure(figsize=(10, 3.5), dpi=96)
        self._forecast_ax = self._forecast_fig.add_subplot(111)
        self._forecast_canvas = FigureCanvasTkAgg(self._forecast_fig, master=chart_lf)
        self._forecast_canvas.get_tk_widget().pack(fill="both", expand=True)

        # ── Profile charts (side by side) ────────────────────────────────
        profiles = ttk.Frame(content)
        profiles.grid(row=2, column=0, sticky="nsew", padx=14, pady=(4, 12))
        profiles.columnconfigure(0, weight=1)
        profiles.columnconfigure(1, weight=1)
        profiles.rowconfigure(0, weight=1)

        wd_lf = ttk.LabelFrame(profiles, text=self.t("forecast.weekday.title"))
        wd_lf.grid(row=0, column=0, sticky="nsew", padx=(0, 4))

        hr_lf = ttk.LabelFrame(profiles, text=self.t("forecast.hourly.title"))
        hr_lf.grid(row=0, column=1, sticky="nsew", padx=(4, 0))

        self._forecast_profile_fig = Figure(figsize=(10, 2.5), dpi=96)
        self._forecast_wd_ax = self._forecast_profile_fig.add_subplot(121)
        self._forecast_hr_ax = self._forecast_profile_fig.add_subplot(122)

        # Split into two separate figures for proper scaling
        self._forecast_wd_fig = Figure(figsize=(5, 2.5), dpi=96)
        self._forecast_wd_ax = self._forecast_wd_fig.add_subplot(111)
        self._forecast_wd_canvas = FigureCanvasTkAgg(self._forecast_wd_fig, master=wd_lf)
        self._forecast_wd_canvas.get_tk_widget().pack(fill="both", expand=True)

        self._forecast_hr_fig = Figure(figsize=(5, 2.5), dpi=96)
        self._forecast_hr_ax = self._forecast_hr_fig.add_subplot(111)
        self._forecast_hr_canvas = FigureCanvasTkAgg(self._forecast_hr_fig, master=hr_lf)
        self._forecast_hr_canvas.get_tk_widget().pack(fill="both", expand=True)

        self.after(600, self._refresh_forecast_tab)

    def _refresh_forecast_tab(self) -> None:
        from shelly_analyzer.services.forecast import compute_forecast

        sel_name = self._forecast_dev_var.get()
        dev = None
        for d in self.cfg.devices:
            if d.name == sel_name:
                dev = d
                break
        if dev is None and self.cfg.devices:
            dev = self.cfg.devices[0]
        if dev is None:
            for k in self._forecast_vars:
                self._forecast_vars[k].set("–")
            return

        from datetime import date as _fc_date
        price = self._get_effective_unit_price()
        result = compute_forecast(
            self.storage.db, dev.key, dev.name,
            horizon_days=getattr(self.cfg.forecast, "horizon_days", 30),
            price_eur_per_kwh=price,
            history_days=getattr(self.cfg.forecast, "history_days", 90),
        )

        if result is None:
            for k in self._forecast_vars:
                self._forecast_vars[k].set("–")
            ax = self._forecast_ax
            ax.clear()
            ax.text(0.5, 0.5, self.t("forecast.no_data"), ha="center", va="center", fontsize=12, color="gray")
            ax.axis("off")
            self._forecast_canvas.draw_idle()
            return

        # Update cards
        self._forecast_vars["avg_daily"].set(f"{result.avg_daily_kwh:.1f} kWh")
        if abs(result.trend_pct_per_month) < 0.5:
            self._forecast_vars["trend"].set(self.t("forecast.trend.stable"))
        elif result.trend_pct_per_month > 0:
            self._forecast_vars["trend"].set(self.t("forecast.trend.rising", pct=f"{result.trend_pct_per_month:.1f}"))
        else:
            self._forecast_vars["trend"].set(self.t("forecast.trend.falling", pct=f"{abs(result.trend_pct_per_month):.1f}"))
        self._forecast_vars["next_month"].set(f"{result.forecast_next_month_kwh:.0f} kWh / {result.forecast_next_month_cost:.2f} €")
        self._forecast_vars["next_year"].set(f"{result.forecast_year_kwh:.0f} kWh / {result.forecast_year_cost:.2f} €")

        # Main chart
        import matplotlib.dates as mdates
        tc = self._get_theme_colors()
        ax = self._forecast_ax
        ax.clear()
        if result.history_dates and result.history_kwh:
            ax.bar(result.history_dates, result.history_kwh, color=tc["blue"], alpha=0.75,
                   label=self.t("forecast.chart.history"), width=0.8)
        if result.forecast_dates and result.forecast_kwh:
            ax.bar(result.forecast_dates, result.forecast_kwh, color=tc["red"], alpha=0.6,
                   label=self.t("forecast.chart.forecast"), width=0.8)
            ax.fill_between(result.forecast_dates, result.forecast_lower, result.forecast_upper,
                          color=tc["red"], alpha=0.1, label=self.t("forecast.chart.confidence"))
        ax.set_ylabel("kWh", fontsize=9)
        ax.legend(fontsize=8, loc="upper left")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m"))
        ax.tick_params(axis="x", rotation=45)
        ax.grid(axis="y", alpha=0.3)
        ax.set_axisbelow(True)
        self._apply_plot_theme(self._forecast_fig, ax, self._forecast_canvas)
        self._forecast_fig.tight_layout()
        self._forecast_canvas.draw_idle()

        # Weekday profile
        wd_ax = self._forecast_wd_ax
        wd_ax.clear()
        if result.weekday_profile:
            days = ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"]
            vals = [result.weekday_profile.get(i, 1.0) for i in range(7)]
            colors = [tc["red"] if v > 1.1 else tc["green"] if v < 0.9 else tc["blue"] for v in vals]
            wd_ax.bar(days, vals, color=colors, alpha=0.85)
            wd_ax.axhline(y=1.0, color=tc["muted"], linestyle="--", alpha=0.5)
            wd_ax.set_ylabel("Faktor", fontsize=9)
            wd_ax.grid(axis="y", alpha=0.3)
            wd_ax.set_axisbelow(True)
        self._apply_plot_theme(self._forecast_wd_fig, wd_ax, self._forecast_wd_canvas)
        self._forecast_wd_fig.tight_layout()
        self._forecast_wd_canvas.draw_idle()

        # Hourly profile
        hr_ax = self._forecast_hr_ax
        hr_ax.clear()
        if result.hourly_profile:
            hours = list(range(24))
            vals = [result.hourly_profile.get(h, 1.0) for h in hours]
            colors = [tc["red"] if v > 1.3 else tc["orange"] if v > 1.1 else tc["green"] if v < 0.7 else tc["blue"] for v in vals]
            hr_ax.bar(hours, vals, color=colors, alpha=0.85)
            hr_ax.axhline(y=1.0, color=tc["muted"], linestyle="--", alpha=0.5)
            hr_ax.set_xlabel("h", fontsize=9)
            hr_ax.set_xticks([0, 4, 8, 12, 16, 20])
            hr_ax.grid(axis="y", alpha=0.3)
            hr_ax.set_axisbelow(True)
        self._apply_plot_theme(self._forecast_hr_fig, hr_ax, self._forecast_hr_canvas)
        self._forecast_hr_fig.tight_layout()
        self._forecast_hr_canvas.draw_idle()
