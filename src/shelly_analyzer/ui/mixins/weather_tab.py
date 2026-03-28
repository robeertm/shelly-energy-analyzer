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

        # Top bar
        top = ttk.Frame(frm)
        top.pack(fill="x", padx=10, pady=(10, 5))
        ttk.Label(top, text=self.t("weather.title"), font=("", 14, "bold")).pack(side="left")

        # Current weather card
        curr = ttk.LabelFrame(frm, text=f"☀️ {self.t('weather.current')}")
        curr.pack(fill="x", padx=10, pady=5)

        weather_grid = ttk.Frame(curr)
        weather_grid.pack(fill="x", padx=10, pady=8)

        self._weather_vars = {}
        for i, (key, label_key, icon) in enumerate([
            ("temp", "weather.temp", "🌡️"),
            ("humidity", "weather.humidity", "💧"),
            ("wind", "weather.wind", "💨"),
            ("clouds", "weather.clouds", "☁️"),
        ]):
            ttk.Label(weather_grid, text=f"{icon} {self.t(label_key)}:", font=("", 10)).grid(row=0, column=i*2, padx=5, sticky="e")
            var = tk.StringVar(value="–")
            self._weather_vars[key] = var
            ttk.Label(weather_grid, textvariable=var, font=("", 11, "bold")).grid(row=0, column=i*2+1, padx=5, sticky="w")
            weather_grid.columnconfigure(i*2, weight=0)
            weather_grid.columnconfigure(i*2+1, weight=1)

        # Correlation summary
        corr_frame = ttk.LabelFrame(frm, text=f"📊 {self.t('weather.correlation')}")
        corr_frame.pack(fill="x", padx=10, pady=5)

        corr_grid = ttk.Frame(corr_frame)
        corr_grid.pack(fill="x", padx=10, pady=8)

        self._weather_corr_vars = {}
        for i, (key, label_key) in enumerate([
            ("r_value", "weather.r_value"),
            ("hdd", "weather.hdd"),
            ("cdd", "weather.cdd"),
            ("kwh_hdd", "weather.kwh_per_hdd"),
            ("kwh_cdd", "weather.kwh_per_cdd"),
        ]):
            ttk.Label(corr_grid, text=f"{self.t(label_key)}:").grid(row=0, column=i*2, padx=5, sticky="e")
            var = tk.StringVar(value="–")
            self._weather_corr_vars[key] = var
            ttk.Label(corr_grid, textvariable=var, font=("", 10, "bold")).grid(row=0, column=i*2+1, padx=5, sticky="w")

        # Interpretation
        self._weather_interp_var = tk.StringVar(value="")
        ttk.Label(corr_frame, textvariable=self._weather_interp_var, wraplength=800, foreground="#555").pack(padx=10, pady=(0, 8))

        # Charts: scatter plot + time series
        chart_frame = ttk.Frame(frm)
        chart_frame.pack(fill="both", expand=True, padx=10, pady=5)

        self._weather_fig = Figure(figsize=(10, 4), dpi=96)
        self._weather_scatter_ax = self._weather_fig.add_subplot(121)
        self._weather_ts_ax = self._weather_fig.add_subplot(122)
        self._weather_canvas = FigureCanvasTkAgg(self._weather_fig, master=chart_frame)
        self._weather_canvas.get_tk_widget().pack(fill="both", expand=True)

        # Device selector
        bottom = ttk.Frame(frm)
        bottom.pack(fill="x", padx=10, pady=(0, 10))
        ttk.Label(bottom, text="Gerät:").pack(side="left")
        self._weather_dev_var = tk.StringVar()
        dev_names = [d.name for d in self.cfg.devices]
        self._weather_dev_combo = ttk.Combobox(bottom, textvariable=self._weather_dev_var, values=dev_names, state="readonly", width=30)
        self._weather_dev_combo.pack(side="left", padx=5)
        if dev_names:
            self._weather_dev_combo.current(0)
        self._weather_dev_combo.bind("<<ComboboxSelected>>", lambda e: self._refresh_weather_tab())

        self.after(800, self._refresh_weather_tab)

    def _refresh_weather_tab(self) -> None:
        import datetime
        import time
        from shelly_analyzer.services.weather import fetch_current_weather, correlate_weather_energy

        weather_cfg = getattr(self.cfg, "weather", None)
        api_key = getattr(weather_cfg, "api_key", "") if weather_cfg else ""
        lat = getattr(weather_cfg, "lat", 0.0) if weather_cfg else 0.0
        lon = getattr(weather_cfg, "lon", 0.0) if weather_cfg else 0.0
        city = getattr(weather_cfg, "city", "") if weather_cfg else ""

        if not api_key:
            for k in self._weather_vars:
                self._weather_vars[k].set(self.t("weather.no_data"))
            return

        # Auto-geocode if lat/lon missing but city is set
        if (lat == 0 and lon == 0) and city:
            try:
                from shelly_analyzer.services.weather import geocode_city
                result = geocode_city(api_key, city)
                if result:
                    lat, lon, _ = result
                    # Persist the geocoded coordinates
                    from shelly_analyzer.io.config import WeatherConfig, save_config
                    import dataclasses
                    new_weather = dataclasses.replace(weather_cfg, lat=lat, lon=lon)
                    self.cfg = dataclasses.replace(self.cfg, weather=new_weather)
                    save_config(self.cfg, self.cfg_path)
            except Exception:
                pass

        if lat == 0 and lon == 0:
            for k in self._weather_vars:
                self._weather_vars[k].set(self.t("weather.no_data"))
            return

        # Fetch current weather
        snapshot = fetch_current_weather(api_key, lat, lon)
        if snapshot:
            self._weather_vars["temp"].set(f"{snapshot.temp_c:.1f} °C")
            self._weather_vars["humidity"].set(f"{snapshot.humidity_pct:.0f}%")
            self._weather_vars["wind"].set(f"{snapshot.wind_speed_ms:.1f} m/s")
            self._weather_vars["clouds"].set(f"{snapshot.clouds_pct:.0f}%")

            # Store weather data in DB
            hour_ts = (int(snapshot.timestamp) // 3600) * 3600
            self.storage.db.upsert_weather([(
                hour_ts, snapshot.temp_c, snapshot.humidity_pct,
                snapshot.wind_speed_ms, snapshot.clouds_pct,
                snapshot.pressure_hpa, snapshot.description,
                int(time.time()),
            )])

        # Correlation analysis
        sel_name = self._weather_dev_var.get()
        dev = None
        for d in self.cfg.devices:
            if d.name == sel_name:
                dev = d
                break
        if dev is None and self.cfg.devices:
            dev = self.cfg.devices[0]

        if dev is None:
            return

        # Get weather data from DB
        now = datetime.datetime.now(datetime.timezone.utc)
        start_ts = int((now - datetime.timedelta(days=30)).timestamp())
        end_ts = int(now.timestamp())
        weather_df = self.storage.db.query_weather(start_ts, end_ts)

        if weather_df.empty:
            self._weather_interp_var.set(self.t("weather.no_data"))
            return

        weather_rows = [
            {"timestamp": int(row["hour_ts"]), "temp_c": float(row["temp_c"])}
            for _, row in weather_df.iterrows()
            if row["temp_c"] is not None
        ]

        corr = correlate_weather_energy(self.storage.db, dev.key, dev.name, weather_rows, days=30)
        if corr is None:
            self._weather_interp_var.set(self.t("weather.no_data"))
            return

        # Update correlation cards
        self._weather_corr_vars["r_value"].set(f"{corr.r_temp_kwh:.3f}")
        self._weather_corr_vars["hdd"].set(f"{corr.hdd_total:.1f}")
        self._weather_corr_vars["cdd"].set(f"{corr.cdd_total:.1f}")
        self._weather_corr_vars["kwh_hdd"].set(f"{corr.kwh_per_hdd:.2f}")
        self._weather_corr_vars["kwh_cdd"].set(f"{corr.kwh_per_cdd:.2f}")

        # Interpretation
        r = corr.r_temp_kwh
        if r < -0.4:
            self._weather_interp_var.set(self.t("weather.interpretation.heating", r=f"{r:.2f}"))
        elif r > 0.4:
            self._weather_interp_var.set(self.t("weather.interpretation.cooling", r=f"{r:.2f}"))
        else:
            self._weather_interp_var.set(self.t("weather.interpretation.none", r=f"{r:.2f}"))

        # Draw scatter plot
        ax1 = self._weather_scatter_ax
        ax1.clear()
        if corr.temps and corr.kwh_vals:
            # Color by temperature
            import numpy as np
            temps = np.array(corr.temps)
            kwh = np.array(corr.kwh_vals)
            scatter = ax1.scatter(temps, kwh, c=temps, cmap="RdYlBu_r", alpha=0.6, s=15, edgecolors="none")
            # Trend line
            if len(temps) > 2:
                z = np.polyfit(temps, kwh, 1)
                p = np.poly1d(z)
                t_range = np.linspace(temps.min(), temps.max(), 50)
                ax1.plot(t_range, p(t_range), "r--", alpha=0.7, linewidth=2)
            ax1.set_xlabel("°C")
            ax1.set_ylabel("kWh")
            ax1.set_title(self.t("weather.chart.scatter"), fontsize=9)

        # Draw time series
        ax2 = self._weather_ts_ax
        ax2.clear()
        if corr.hours and corr.temps and corr.kwh_vals:
            ax2_twin = ax2.twinx()
            ax2.plot(corr.hours, corr.kwh_vals, color="#3498db", alpha=0.7, linewidth=1, label="kWh")
            ax2_twin.plot(corr.hours, corr.temps, color="#e74c3c", alpha=0.7, linewidth=1, label="°C")
            ax2.set_ylabel("kWh", color="#3498db")
            ax2_twin.set_ylabel("°C", color="#e74c3c")
            ax2.set_title(self.t("weather.chart.title"), fontsize=9)
            ax2.tick_params(axis="x", rotation=45)

        self._weather_fig.tight_layout()
        self._weather_canvas.draw_idle()
