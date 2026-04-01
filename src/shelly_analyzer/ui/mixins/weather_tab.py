"""Weather correlation tab mixin."""
from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure


class WeatherMixin:
    """Adds the Weather Correlation tab."""

    def _build_weather_tab(self) -> None:
        frm = self.tab_weather

        # ── Top bar ──────────────────────────────────────────────────────
        top = ttk.Frame(frm)
        top.pack(fill="x", padx=14, pady=(12, 4))
        ttk.Label(top, text=self.t("weather.title"), font=("", 14, "bold")).pack(side="left")

        # ── Device selector ──────────────────────────────────────────────
        ctrl = ttk.Frame(frm)
        ctrl.pack(fill="x", padx=14, pady=(0, 6))
        self._weather_dev_var = tk.StringVar()
        dev_names = [d.name for d in self.cfg.devices]
        cb = ttk.Combobox(ctrl, textvariable=self._weather_dev_var, values=dev_names, state="readonly", width=30)
        cb.pack(side="left", padx=(0, 6))
        if dev_names:
            cb.current(0)
        cb.bind("<<ComboboxSelected>>", lambda e: self._refresh_weather_tab())

        # ── Content area (fills both directions) ─────────────────────────
        content = ttk.Frame(frm)
        content.pack(fill="both", expand=True)
        content.rowconfigure(0, weight=0)  # weather cards
        content.rowconfigure(1, weight=0)  # correlation cards
        content.rowconfigure(2, weight=1)  # charts
        content.columnconfigure(0, weight=1)

        # ── Current weather card ─────────────────────────────────────────
        weather_cards = ttk.Frame(content)
        weather_cards.grid(row=0, column=0, sticky="ew", padx=14, pady=(4, 4))
        weather_cards.columnconfigure((0, 1, 2, 3), weight=1)

        self._weather_vars = {}
        for col, (key, label_key, icon) in enumerate([
            ("temp", "weather.temp", "🌡️"),
            ("humidity", "weather.humidity", "💧"),
            ("wind", "weather.wind", "💨"),
            ("clouds", "weather.clouds", "☁️"),
        ]):
            card = ttk.LabelFrame(weather_cards, text=f"{icon} {self.t(label_key)}")
            card.grid(row=0, column=col, sticky="nsew", padx=3, pady=3)
            v = tk.StringVar(value="–")
            self._weather_vars[key] = v
            ttk.Label(card, textvariable=v, font=("", 13, "bold")).pack(anchor="center", padx=8, pady=8)

        # ── Correlation summary ──────────────────────────────────────────
        corr_cards = ttk.Frame(content)
        corr_cards.grid(row=1, column=0, sticky="ew", padx=14, pady=(0, 4))
        corr_cards.columnconfigure((0, 1, 2, 3, 4), weight=1)

        self._weather_corr_vars = {}
        for col, (key, label_key) in enumerate([
            ("r_value", "weather.r_value"),
            ("hdd", "weather.hdd"),
            ("cdd", "weather.cdd"),
            ("kwh_hdd", "weather.kwh_per_hdd"),
            ("kwh_cdd", "weather.kwh_per_cdd"),
        ]):
            card = ttk.LabelFrame(corr_cards, text=self.t(label_key))
            card.grid(row=0, column=col, sticky="nsew", padx=3, pady=3)
            v = tk.StringVar(value="–")
            self._weather_corr_vars[key] = v
            ttk.Label(card, textvariable=v, font=("", 12, "bold")).pack(anchor="center", padx=6, pady=6)

        # Interpretation
        self._weather_interp_var = tk.StringVar(value="")
        interp_lbl = ttk.Label(content, textvariable=self._weather_interp_var,
                  wraplength=900, foreground="gray")
        interp_lbl.grid(row=1, column=0, sticky="ew", padx=14, pady=(0, 0))
        # Re-grid corr_cards to row 1, interp to after
        corr_cards.grid(row=1, column=0, sticky="ew", padx=14, pady=(0, 0))
        interp_lbl.grid_forget()
        # Put interp inside corr_cards
        ttk.Label(corr_cards, textvariable=self._weather_interp_var,
                  wraplength=900, foreground="gray").grid(row=1, column=0, columnspan=5, sticky="w", pady=(2, 4))

        # ── Charts (fill remaining space) ────────────────────────────────
        chart_lf = ttk.LabelFrame(content, text=self.t("weather.chart.title"))
        chart_lf.grid(row=2, column=0, sticky="nsew", padx=14, pady=(4, 12))

        self._weather_fig = Figure(figsize=(10, 4), dpi=96)
        self._weather_scatter_ax = self._weather_fig.add_subplot(121)
        self._weather_ts_ax = self._weather_fig.add_subplot(122)
        self._weather_canvas = FigureCanvasTkAgg(self._weather_fig, master=chart_lf)
        self._weather_canvas.get_tk_widget().pack(fill="both", expand=True)

        self.after(800, self._refresh_weather_tab)

    def _refresh_weather_tab(self) -> None:
        import datetime
        import time
        import numpy as np
        from shelly_analyzer.services.weather import fetch_current_weather

        weather_cfg = getattr(self.cfg, "weather", None)
        api_key = getattr(weather_cfg, "api_key", "") if weather_cfg else ""
        lat = getattr(weather_cfg, "lat", 0.0) if weather_cfg else 0.0
        lon = getattr(weather_cfg, "lon", 0.0) if weather_cfg else 0.0
        city = getattr(weather_cfg, "city", "") if weather_cfg else ""

        if not api_key:
            for k in self._weather_vars:
                self._weather_vars[k].set(self.t("weather.no_data"))
            self._draw_empty_weather_charts("API-Key fehlt")
            return

        # Auto-geocode
        if (lat == 0 and lon == 0) and city:
            try:
                from shelly_analyzer.services.weather import geocode_city
                result = geocode_city(api_key, city)
                if result:
                    lat, lon, _ = result
                    import dataclasses
                    from shelly_analyzer.io.config import save_config
                    new_w = dataclasses.replace(weather_cfg, lat=lat, lon=lon)
                    self.cfg = dataclasses.replace(self.cfg, weather=new_w)
                    save_config(self.cfg, self.cfg_path)
            except Exception:
                pass

        if lat == 0 and lon == 0:
            for k in self._weather_vars:
                self._weather_vars[k].set("–")
            self._draw_empty_weather_charts("Stadt nicht gefunden")
            return

        # Fetch current weather
        snapshot = fetch_current_weather(api_key, lat, lon)
        if snapshot:
            self._weather_vars["temp"].set(f"{snapshot.temp_c:.1f} °C")
            self._weather_vars["humidity"].set(f"{snapshot.humidity_pct:.0f}%")
            self._weather_vars["wind"].set(f"{snapshot.wind_speed_ms:.1f} m/s")
            self._weather_vars["clouds"].set(f"{snapshot.clouds_pct:.0f}%")

            hour_ts = (int(snapshot.timestamp) // 3600) * 3600
            self.storage.db.upsert_weather([(
                hour_ts, snapshot.temp_c, snapshot.humidity_pct,
                snapshot.wind_speed_ms, snapshot.clouds_pct,
                snapshot.pressure_hpa, snapshot.description,
                int(time.time()),
            )])

        # Get device
        sel_name = self._weather_dev_var.get()
        dev = None
        for d in self.cfg.devices:
            if d.name == sel_name:
                dev = d
                break
        if dev is None and self.cfg.devices:
            dev = self.cfg.devices[0]
        if dev is None:
            self._draw_empty_weather_charts("Kein Gerät")
            return

        # Get weather + energy data
        now = datetime.datetime.now(datetime.timezone.utc)
        start_ts = int((now - datetime.timedelta(days=7)).timestamp())
        end_ts = int(now.timestamp())

        weather_df = self.storage.db.query_weather(start_ts, end_ts)
        hourly = self.storage.db.query_hourly(dev.key, start_ts=start_ts, end_ts=end_ts)

        # Build paired data directly (more robust than correlate_weather_energy)
        if weather_df.empty or hourly.empty:
            self._weather_interp_var.set("Noch zu wenig Daten. Wetterdaten werden stündlich gesammelt.")
            # Show energy data alone if available
            if not hourly.empty:
                self._draw_energy_only_chart(hourly, dev.name)
            else:
                self._draw_empty_weather_charts("Daten werden gesammelt...")
            return

        # Match weather + energy by hour
        w_by_hour = {}
        for _, row in weather_df.iterrows():
            h = int(row["hour_ts"])
            w_by_hour[h] = float(row["temp_c"]) if row["temp_c"] is not None else None

        matched_temps = []
        matched_kwh = []
        matched_hours = []

        for _, row in hourly.iterrows():
            h = int(row["hour_ts"])
            if h in w_by_hour and w_by_hour[h] is not None:
                matched_temps.append(w_by_hour[h])
                matched_kwh.append(float(row["kwh"]))
                matched_hours.append(datetime.datetime.fromtimestamp(h, tz=datetime.timezone.utc))

        if len(matched_temps) < 3:
            self._weather_interp_var.set(f"Erst {len(matched_temps)} gepaarte Datenpunkte. Mehr Daten werden stündlich gesammelt.")
            if not hourly.empty:
                self._draw_energy_only_chart(hourly, dev.name)
            else:
                self._draw_empty_weather_charts("Daten werden gesammelt...")
            return

        temps = np.array(matched_temps)
        kwh = np.array(matched_kwh)

        # Correlation
        r_val = float(np.corrcoef(temps, kwh)[0, 1]) if np.std(temps) > 0 and np.std(kwh) > 0 else 0.0

        # Degree days
        hdd = sum(max(0, 18.0 - t) / 24.0 for t in matched_temps)
        cdd = sum(max(0, t - 22.0) / 24.0 for t in matched_temps)
        total_kwh = float(kwh.sum())

        self._weather_corr_vars["r_value"].set(f"{r_val:.3f}")
        self._weather_corr_vars["hdd"].set(f"{hdd:.1f}")
        self._weather_corr_vars["cdd"].set(f"{cdd:.1f}")
        self._weather_corr_vars["kwh_hdd"].set(f"{total_kwh / hdd:.2f}" if hdd > 1 else "–")
        self._weather_corr_vars["kwh_cdd"].set(f"{total_kwh / cdd:.2f}" if cdd > 1 else "–")

        if r_val < -0.4:
            self._weather_interp_var.set(self.t("weather.interpretation.heating", r=f"{r_val:.2f}"))
        elif r_val > 0.4:
            self._weather_interp_var.set(self.t("weather.interpretation.cooling", r=f"{r_val:.2f}"))
        else:
            self._weather_interp_var.set(self.t("weather.interpretation.none", r=f"{r_val:.2f}"))

        # Theme colors
        tc = self._get_theme_colors()

        # Draw scatter plot – color by hour-of-day (useful extra dimension)
        ax1 = self._weather_scatter_ax
        ax1.clear()
        # Remove previous colorbar if it exists
        if hasattr(self, "_weather_cbar") and self._weather_cbar is not None:
            try:
                self._weather_cbar.remove()
            except Exception:
                pass
            self._weather_cbar = None
        hours_of_day = np.array([h.hour + h.minute / 60.0 for h in matched_hours])
        scatter = ax1.scatter(temps, kwh, c=hours_of_day, cmap="twilight_shifted",
                              vmin=0, vmax=24, alpha=0.65, s=22, edgecolors="none")
        self._weather_cbar = self._weather_fig.colorbar(scatter, ax=ax1, pad=0.02, shrink=0.8)
        self._weather_cbar.set_label(self.t("weather.chart.hour"), fontsize=8)
        self._weather_cbar.set_ticks([0, 6, 12, 18, 24])
        if len(temps) > 2:
            z = np.polyfit(temps, kwh, 1)
            p = np.poly1d(z)
            t_range = np.linspace(temps.min(), temps.max(), 50)
            ax1.plot(t_range, p(t_range), color=tc["red"], linestyle="--", alpha=0.7, linewidth=2)
        ax1.set_xlabel(self.t("weather.chart.temp_axis"), fontsize=9)
        ax1.set_ylabel(self.t("weather.chart.kwh_axis"), fontsize=9)
        ax1.set_title(self.t("weather.chart.scatter"), fontsize=10)
        ax1.grid(alpha=0.3)
        ax1.set_axisbelow(True)
        self._apply_plot_theme(self._weather_fig, ax1, self._weather_canvas)

        # Time series – proper date labels
        import matplotlib.dates as mdates
        ax2 = self._weather_ts_ax
        ax2.clear()
        # Remove stale twin axes from previous refreshes
        if hasattr(self, "_weather_ts_twin") and self._weather_ts_twin is not None:
            try:
                self._weather_ts_twin.remove()
            except Exception:
                pass
        ax2_twin = ax2.twinx()
        self._weather_ts_twin = ax2_twin
        dates = mdates.date2num(matched_hours)
        ax2.bar(dates, matched_kwh, color=tc["blue"], alpha=0.6,
                width=1.0 / 24.0, align="center")
        ax2_twin.plot(dates, matched_temps, color=tc["red"], linewidth=1.5, alpha=0.8)
        ax2.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(matched_hours) // (24 * 5))))
        ax2.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m."))
        ax2.xaxis.set_minor_locator(mdates.HourLocator(interval=6))
        self._weather_fig.autofmt_xdate(rotation=30, ha="right")
        ax2.set_ylabel(self.t("weather.chart.kwh_axis"), color=tc["blue"], fontsize=9)
        ax2_twin.set_ylabel(self.t("weather.chart.temp_axis"), color=tc["red"], fontsize=9)
        ax2.set_title(self.t("weather.chart.timeseries"), fontsize=10)
        ax2.grid(axis="y", alpha=0.3)
        ax2.set_axisbelow(True)
        self._apply_plot_theme(self._weather_fig, ax2)
        # Also theme the twin axis
        ax2_twin.tick_params(axis="both", colors=tc["fg"])
        for spine in ax2_twin.spines.values():
            spine.set_color(tc["fg"])

        self._weather_fig.tight_layout()
        self._weather_canvas.draw_idle()

    def _draw_empty_weather_charts(self, msg: str) -> None:
        tc = self._get_theme_colors()
        for ax in (self._weather_scatter_ax, self._weather_ts_ax):
            ax.clear()
            ax.text(0.5, 0.5, msg, ha="center", va="center", fontsize=11, color=tc["muted"])
            ax.axis("off")
            self._apply_plot_theme(self._weather_fig, ax, self._weather_canvas)
        self._weather_fig.tight_layout()
        self._weather_canvas.draw_idle()

    def _draw_energy_only_chart(self, hourly, dev_name: str) -> None:
        """Show energy data when no weather pairing available yet."""
        import pandas as pd
        tc = self._get_theme_colors()
        self._weather_scatter_ax.clear()
        self._weather_scatter_ax.text(0.5, 0.5, "Wetter-Daten werden\nstündlich gesammelt...",
                                      ha="center", va="center", fontsize=10, color=tc["muted"])
        self._weather_scatter_ax.axis("off")
        self._apply_plot_theme(self._weather_fig, self._weather_scatter_ax, self._weather_canvas)

        ax2 = self._weather_ts_ax
        ax2.clear()
        kwh = hourly["kwh"].values[-48:] if len(hourly) > 48 else hourly["kwh"].values
        ax2.bar(range(len(kwh)), kwh, color=tc["blue"], alpha=0.7)
        ax2.set_title(f"Verbrauch: {dev_name} (letzte {len(kwh)}h)", fontsize=10)
        ax2.set_xlabel("h", fontsize=9)
        ax2.set_ylabel("kWh", fontsize=9)
        ax2.grid(axis="y", alpha=0.3)
        ax2.set_axisbelow(True)
        self._apply_plot_theme(self._weather_fig, ax2)

        self._weather_fig.tight_layout()
        self._weather_canvas.draw_idle()
