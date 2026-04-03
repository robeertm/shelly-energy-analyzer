from __future__ import annotations
import logging
import threading
import tkinter as tk
from tkinter import ttk

_log = logging.getLogger(__name__)


class BatteryMixin:
    """Battery storage monitoring tab."""

    def _build_battery_tab(self) -> None:
        frm = self.tab_battery
        for w in frm.winfo_children():
            w.destroy()

        canvas = tk.Canvas(frm, highlightthickness=0)
        scrollbar = ttk.Scrollbar(frm, orient="vertical", command=canvas.yview)
        inner = ttk.Frame(canvas)
        inner.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        _win_id = canvas.create_window((0, 0), window=inner, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.bind("<Configure>", lambda e: canvas.itemconfigure(_win_id, width=e.width))
        scrollbar.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)

        ttk.Label(inner, text=self.t("battery.title"), font=("", 14, "bold")).pack(anchor="w", padx=12, pady=(12, 4))
        ttk.Label(inner, text=self.t("battery.hint"), foreground="gray").pack(anchor="w", padx=12, pady=(0, 8))

        # Summary cards
        summary_frm = ttk.Frame(inner)
        summary_frm.pack(fill="x", padx=12, pady=4)
        self._bat_soc = tk.StringVar(value="\u2014")
        self._bat_power = tk.StringVar(value="\u2014")
        self._bat_mode = tk.StringVar(value="\u2014")
        self._bat_cycles = tk.StringVar(value="\u2014")
        self._bat_efficiency = tk.StringVar(value="\u2014")

        for i, (label, var) in enumerate([
            ("SOC", self._bat_soc),
            (self.t("battery.power"), self._bat_power),
            (self.t("battery.mode"), self._bat_mode),
            (self.t("battery.cycles"), self._bat_cycles),
            (self.t("battery.efficiency"), self._bat_efficiency),
        ]):
            card = ttk.LabelFrame(summary_frm, text=label)
            card.grid(row=0, column=i, padx=4, pady=4, sticky="nsew")
            summary_frm.columnconfigure(i, weight=1)
            ttk.Label(card, textvariable=var, font=("", 14, "bold")).pack(padx=8, pady=6)

        # SOC chart
        self._bat_chart_frame = ttk.LabelFrame(inner, text=self.t("battery.soc_timeline"))
        self._bat_chart_frame.pack(fill="both", expand=True, padx=12, pady=8)

        ttk.Button(inner, text=self.t("battery.refresh_btn"), command=self._bat_refresh).pack(padx=12, pady=8)
        self._bat_refresh()

    def _bat_refresh(self) -> None:
        def _worker():
            try:
                from shelly_analyzer.services.battery import get_battery_status
                status = get_battery_status(self.storage.db, self.cfg.battery)
                def _update():
                    self._bat_soc.set(f"{status.soc_pct:.0f}%")
                    self._bat_power.set(f"{status.power_w:.0f} W")
                    self._bat_mode.set(self.t(f"battery.mode_{status.mode}"))
                    self._bat_cycles.set(str(status.cycle_count))
                    self._bat_efficiency.set(f"{status.avg_efficiency_pct:.1f}%")
                    self._bat_draw_soc_chart(status.soc_timeline)
                self.after(0, _update)
            except Exception as e:
                _log.error("Battery refresh: %s", e)
        threading.Thread(target=_worker, daemon=True).start()

    def _bat_draw_soc_chart(self, timeline) -> None:
        for w in self._bat_chart_frame.winfo_children():
            w.destroy()
        if not timeline:
            return
        try:
            from matplotlib.figure import Figure
            from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
            from datetime import datetime

            fig = Figure(figsize=(8, 3), dpi=100)
            ax = fig.add_subplot(111)
            times = [datetime.fromtimestamp(t[0]) for t in timeline]
            socs = [t[1] for t in timeline]
            ax.fill_between(times, socs, alpha=0.3, color="#4caf50")
            ax.plot(times, socs, color="#4caf50", linewidth=1.5)
            ax.set_ylabel("SOC %")
            ax.set_ylim(0, 100)
            ax.grid(True, alpha=0.3)
            fig.autofmt_xdate()
            fig.tight_layout()

            canvas = FigureCanvasTkAgg(fig, self._bat_chart_frame)
            canvas.draw()
            canvas.get_tk_widget().pack(fill="both", expand=True)
        except Exception as e:
            _log.debug("Battery SOC chart: %s", e)
