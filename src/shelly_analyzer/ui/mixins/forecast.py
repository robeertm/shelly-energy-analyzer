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

        # Top bar
        top = ttk.Frame(frm)
        top.pack(fill="x", padx=10, pady=(10, 5))
        ttk.Label(top, text=self.t("forecast.title"), font=("", 14, "bold")).pack(side="left")

        # Summary cards
        cards = ttk.Frame(frm)
        cards.pack(fill="x", padx=10, pady=5)

        self._forecast_vars = {}
        card_defs = [
            ("avg_daily", "forecast.avg_daily", "📊"),
            ("trend", "forecast.trend", "📈"),
            ("next_month", "forecast.next_month", "📅"),
            ("next_year", "forecast.next_year", "🗓️"),
        ]
        for i, (key, label_key, icon) in enumerate(card_defs):
            card = ttk.LabelFrame(cards, text=f"{icon} {self.t(label_key)}")
            card.grid(row=0, column=i, padx=5, pady=5, sticky="nsew")
            cards.columnconfigure(i, weight=1)
            var = tk.StringVar(value="–")
            self._forecast_vars[key] = var
            ttk.Label(card, textvariable=var, font=("", 13, "bold")).pack(padx=10, pady=8)

        # Chart area
        chart_frame = ttk.Frame(frm)
        chart_frame.pack(fill="both", expand=True, padx=10, pady=5)

        self._forecast_fig = Figure(figsize=(10, 4), dpi=96)
        self._forecast_ax = self._forecast_fig.add_subplot(111)
        self._forecast_canvas = FigureCanvasTkAgg(self._forecast_fig, master=chart_frame)
        self._forecast_canvas.get_tk_widget().pack(fill="both", expand=True)

        # Device selector
        bottom = ttk.Frame(frm)
        bottom.pack(fill="x", padx=10, pady=(0, 10))
        ttk.Label(bottom, text="Gerät:").pack(side="left")
        self._forecast_dev_var = tk.StringVar()
        dev_names = [d.name for d in self.cfg.devices]
        self._forecast_dev_combo = ttk.Combobox(bottom, textvariable=self._forecast_dev_var, values=dev_names, state="readonly", width=30)
        self._forecast_dev_combo.pack(side="left", padx=5)
        if dev_names:
            self._forecast_dev_combo.current(0)
        self._forecast_dev_combo.bind("<<ComboboxSelected>>", lambda e: self._refresh_forecast_tab())

        # Weekday + hourly profile area
        profile_frame = ttk.Frame(frm)
        profile_frame.pack(fill="x", padx=10, pady=(0, 10))

        self._forecast_profile_fig = Figure(figsize=(10, 2.5), dpi=96)
        self._forecast_wd_ax = self._forecast_profile_fig.add_subplot(121)
        self._forecast_hr_ax = self._forecast_profile_fig.add_subplot(122)
        self._forecast_profile_canvas = FigureCanvasTkAgg(self._forecast_profile_fig, master=profile_frame)
        self._forecast_profile_canvas.get_tk_widget().pack(fill="both", expand=True)

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

        price = self.cfg.pricing.unit_price_gross()
        result = compute_forecast(
            self.storage.db, dev.key, dev.name,
            horizon_days=getattr(self.cfg.forecast, "horizon_days", 30),
            price_eur_per_kwh=price,
            history_days=getattr(self.cfg.forecast, "history_days", 90),
        )

        if result is None:
            for k in self._forecast_vars:
                self._forecast_vars[k].set(self.t("forecast.no_data"))
            self._forecast_ax.clear()
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

        self._forecast_vars["next_month"].set(
            f"{result.forecast_next_month_kwh:.0f} kWh / {result.forecast_next_month_cost:.2f} €"
        )
        self._forecast_vars["next_year"].set(
            f"{result.forecast_year_kwh:.0f} kWh / {result.forecast_year_cost:.2f} €"
        )

        # Draw main chart
        ax = self._forecast_ax
        ax.clear()

        import matplotlib.dates as mdates
        # History
        if result.history_dates and result.history_kwh:
            ax.bar(result.history_dates, result.history_kwh, color="#3498db", alpha=0.7,
                   label=self.t("forecast.chart.history"), width=0.8)

        # Forecast
        if result.forecast_dates and result.forecast_kwh:
            ax.bar(result.forecast_dates, result.forecast_kwh, color="#e74c3c", alpha=0.6,
                   label=self.t("forecast.chart.forecast"), width=0.8)
            # Confidence band
            ax.fill_between(result.forecast_dates, result.forecast_lower, result.forecast_upper,
                          color="#e74c3c", alpha=0.15, label=self.t("forecast.chart.confidence"))

        ax.set_title(self.t("forecast.chart.title"), fontsize=11)
        ax.set_ylabel("kWh")
        ax.legend(fontsize=8)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m"))
        ax.tick_params(axis="x", rotation=45)
        self._forecast_fig.tight_layout()
        self._forecast_canvas.draw_idle()

        # Draw profiles
        wd_ax = self._forecast_wd_ax
        wd_ax.clear()
        if result.weekday_profile:
            days = ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"]
            vals = [result.weekday_profile.get(i, 1.0) for i in range(7)]
            colors = ["#e74c3c" if v > 1.1 else "#27ae60" if v < 0.9 else "#3498db" for v in vals]
            wd_ax.bar(days, vals, color=colors, alpha=0.8)
            wd_ax.axhline(y=1.0, color="gray", linestyle="--", alpha=0.5)
            wd_ax.set_title(self.t("forecast.weekday.title"), fontsize=9)
            wd_ax.set_ylabel("Faktor")

        hr_ax = self._forecast_hr_ax
        hr_ax.clear()
        if result.hourly_profile:
            hours = list(range(24))
            vals = [result.hourly_profile.get(h, 1.0) for h in hours]
            colors = ["#e74c3c" if v > 1.3 else "#f39c12" if v > 1.1 else "#27ae60" if v < 0.7 else "#3498db" for v in vals]
            hr_ax.bar(hours, vals, color=colors, alpha=0.8)
            hr_ax.axhline(y=1.0, color="gray", linestyle="--", alpha=0.5)
            hr_ax.set_title(self.t("forecast.hourly.title"), fontsize=9)
            hr_ax.set_xlabel("h")

        self._forecast_profile_fig.tight_layout()
        self._forecast_profile_canvas.draw_idle()
