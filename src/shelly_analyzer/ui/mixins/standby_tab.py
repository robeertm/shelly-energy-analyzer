"""Standby-Killer Report tab mixin."""
from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure


class StandbyMixin:
    """Adds the Standby Killer Report tab."""

    def _build_standby_tab(self) -> None:
        frm = self.tab_standby

        # Top bar
        top = ttk.Frame(frm)
        top.pack(fill="x", padx=10, pady=(10, 5))
        ttk.Label(top, text=self.t("standby.title"), font=("", 14, "bold")).pack(side="left")
        ttk.Button(top, text=self.t("standby.refresh"), command=self._refresh_standby_tab).pack(side="right")

        # Summary cards
        summary = ttk.Frame(frm)
        summary.pack(fill="x", padx=10, pady=5)

        self._standby_total_cost_var = tk.StringVar(value="–")
        self._standby_total_kwh_var = tk.StringVar(value="–")

        card1 = ttk.LabelFrame(summary, text=f"💰 {self.t('standby.total_annual')}")
        card1.grid(row=0, column=0, padx=5, pady=5, sticky="nsew")
        ttk.Label(card1, textvariable=self._standby_total_cost_var, font=("", 16, "bold"), foreground="#e74c3c").pack(padx=15, pady=10)

        card2 = ttk.LabelFrame(summary, text=f"⚡ {self.t('standby.total_kwh')}")
        card2.grid(row=0, column=1, padx=5, pady=5, sticky="nsew")
        ttk.Label(card2, textvariable=self._standby_total_kwh_var, font=("", 16, "bold")).pack(padx=15, pady=10)

        summary.columnconfigure(0, weight=1)
        summary.columnconfigure(1, weight=1)

        # Tip label
        ttk.Label(frm, text=self.t("standby.tip"), wraplength=800, foreground="#888").pack(fill="x", padx=15, pady=3)

        # Table
        table_frame = ttk.Frame(frm)
        table_frame.pack(fill="both", expand=True, padx=10, pady=5)

        cols = ("device", "base_load", "annual_kwh", "annual_cost", "share", "risk")
        self._standby_tree = ttk.Treeview(table_frame, columns=cols, show="headings", height=10)
        for col, hdr_key, w in [
            ("device",     "standby.col.device",      200),
            ("base_load",  "standby.col.base_load",    100),
            ("annual_kwh", "standby.col.annual_kwh",   100),
            ("annual_cost","standby.col.annual_cost",   100),
            ("share",      "standby.col.share",         80),
            ("risk",       "standby.col.risk",           80),
        ]:
            self._standby_tree.heading(col, text=self.t(hdr_key))
            self._standby_tree.column(col, width=w, anchor="center" if col != "device" else "w")

        sb = ttk.Scrollbar(table_frame, orient="vertical", command=self._standby_tree.yview)
        self._standby_tree.configure(yscrollcommand=sb.set)
        self._standby_tree.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")

        # Chart area: bar chart + 24h profile
        chart_frame = ttk.Frame(frm)
        chart_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        self._standby_fig = Figure(figsize=(10, 3), dpi=96)
        self._standby_bar_ax = self._standby_fig.add_subplot(121)
        self._standby_profile_ax = self._standby_fig.add_subplot(122)
        self._standby_canvas = FigureCanvasTkAgg(self._standby_fig, master=chart_frame)
        self._standby_canvas.get_tk_widget().pack(fill="both", expand=True)

        self._standby_tree.bind("<<TreeviewSelect>>", self._on_standby_select)

        self.after(600, self._refresh_standby_tab)

    def _refresh_standby_tab(self) -> None:
        from shelly_analyzer.services.standby import generate_standby_report

        price = self.cfg.pricing.unit_price_gross()
        report = generate_standby_report(self.storage.db, self.cfg.devices, price)

        self._standby_total_cost_var.set(f"{report.total_annual_standby_cost:.2f} €/Jahr")
        self._standby_total_kwh_var.set(f"{report.total_annual_standby_kwh:.0f} kWh/Jahr")

        # Populate table
        self._standby_tree.delete(*self._standby_tree.get_children())
        self._standby_report = report

        for dev in report.devices:
            risk_label = self.t(f"standby.risk.{dev.risk}")
            self._standby_tree.insert("", "end", values=(
                dev.device_name,
                f"{dev.base_load_w:.0f} W",
                f"{dev.annual_standby_kwh:.0f}",
                f"{dev.annual_standby_cost:.2f}",
                f"{dev.standby_share_pct:.0f}%",
                risk_label,
            ))

        # Draw bar chart
        ax = self._standby_bar_ax
        ax.clear()
        if report.devices:
            names = [d.device_name[:15] for d in report.devices]
            costs = [d.annual_standby_cost for d in report.devices]
            colors = ["#e74c3c" if d.risk == "high" else "#f39c12" if d.risk == "medium" else "#27ae60" for d in report.devices]
            bars = ax.barh(names, costs, color=colors, alpha=0.8)
            ax.set_xlabel("€/Jahr")
            ax.set_title(self.t("standby.chart.title"), fontsize=9)
            # Add value labels
            for bar, cost in zip(bars, costs):
                ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
                       f"{cost:.0f}€", va="center", fontsize=8)

        self._standby_profile_ax.clear()
        self._standby_fig.tight_layout()
        self._standby_canvas.draw_idle()

    def _on_standby_select(self, event) -> None:
        sel = self._standby_tree.selection()
        if not sel or not hasattr(self, "_standby_report"):
            return
        idx = self._standby_tree.index(sel[0])
        if idx >= len(self._standby_report.devices):
            return
        dev = self._standby_report.devices[idx]

        ax = self._standby_profile_ax
        ax.clear()
        if dev.hourly_profile:
            hours = list(range(24))
            colors = ["#2c3e50" if 0 <= h <= 5 else "#3498db" for h in hours]
            ax.bar(hours, dev.hourly_profile, color=colors, alpha=0.8)
            ax.axhline(y=dev.base_load_w, color="#e74c3c", linestyle="--", alpha=0.7, label=f"Base: {dev.base_load_w:.0f} W")
            ax.set_title(self.t("standby.chart.profile", device=dev.device_name[:20]), fontsize=9)
            ax.set_xlabel("h")
            ax.set_ylabel("W")
            ax.legend(fontsize=7)

        self._standby_fig.tight_layout()
        self._standby_canvas.draw_idle()
