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

        # ── Top bar ──────────────────────────────────────────────────────
        top = ttk.Frame(frm)
        top.pack(fill="x", padx=14, pady=(12, 4))
        ttk.Label(top, text=self.t("standby.title"), font=("", 14, "bold")).pack(side="left")

        # ── Scrollable content ───────────────────────────────────────────
        outer = ttk.Frame(frm)
        outer.pack(fill="both", expand=True)
        canvas = tk.Canvas(outer, highlightthickness=0)
        scrollbar = ttk.Scrollbar(outer, orient="vertical", command=canvas.yview)
        self._standby_scroll = ttk.Frame(canvas)
        self._standby_scroll.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        _sw = canvas.create_window((0, 0), window=self._standby_scroll, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.bind("<Configure>", lambda e: canvas.itemconfigure(_sw, width=e.width))
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # ── Summary cards ────────────────────────────────────────────────
        cards = ttk.Frame(self._standby_scroll)
        cards.pack(fill="x", padx=14, pady=(8, 4))
        cards.columnconfigure((0, 1, 2, 3), weight=1)

        self._standby_vars = {}
        for col, (key, label_key, icon) in enumerate([
            ("total_cost", "standby.total_annual", "💰"),
            ("total_kwh", "standby.total_kwh", "⚡"),
        ]):
            card = ttk.LabelFrame(cards, text=f"{icon} {self.t(label_key)}")
            card.grid(row=0, column=col, sticky="nsew", padx=3, pady=3)
            v = tk.StringVar(value="–")
            self._standby_vars[key] = v
            ttk.Label(card, textvariable=v, font=("", 13, "bold")).pack(anchor="center", padx=8, pady=8)

        # ── Tip ──────────────────────────────────────────────────────────
        ttk.Label(self._standby_scroll, text=self.t("standby.tip"),
                  wraplength=900, foreground="gray").pack(anchor="w", padx=14, pady=(2, 4))

        # ── Table ────────────────────────────────────────────────────────
        table_lf = ttk.LabelFrame(self._standby_scroll, text=self.t("standby.col.device"))
        table_lf.pack(fill="x", padx=14, pady=(4, 4))

        cols = ("device", "base_load", "annual_kwh", "annual_cost", "share", "risk")
        self._standby_tree = ttk.Treeview(table_lf, columns=cols, show="headings", height=8)
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

        sb = ttk.Scrollbar(table_lf, orient="vertical", command=self._standby_tree.yview)
        self._standby_tree.configure(yscrollcommand=sb.set)
        self._standby_tree.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")

        # ── Charts ───────────────────────────────────────────────────────
        chart_lf = ttk.LabelFrame(self._standby_scroll, text=self.t("standby.chart.title"))
        chart_lf.pack(fill="both", expand=True, padx=14, pady=(4, 4))

        self._standby_fig = Figure(figsize=(10, 3.5), dpi=96)
        self._standby_bar_ax = self._standby_fig.add_subplot(121)
        self._standby_profile_ax = self._standby_fig.add_subplot(122)
        self._standby_canvas = FigureCanvasTkAgg(self._standby_fig, master=chart_lf)
        self._standby_canvas.get_tk_widget().pack(fill="both", expand=True)

        self._standby_tree.bind("<<TreeviewSelect>>", self._on_standby_select)
        self.after(600, self._refresh_standby_tab)

    def _refresh_standby_tab(self) -> None:
        from shelly_analyzer.services.standby import generate_standby_report

        price = self.cfg.pricing.unit_price_gross()
        report = generate_standby_report(self.storage.db, self.cfg.devices, price)

        self._standby_vars["total_cost"].set(f"{report.total_annual_standby_cost:.2f} €/Jahr")
        self._standby_vars["total_kwh"].set(f"{report.total_annual_standby_kwh:.0f} kWh/Jahr")

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

        # Bar chart
        ax = self._standby_bar_ax
        ax.clear()
        if report.devices:
            names = [d.device_name[:18] for d in report.devices]
            costs = [d.annual_standby_cost for d in report.devices]
            colors = ["#e74c3c" if d.risk == "high" else "#f39c12" if d.risk == "medium" else "#27ae60"
                     for d in report.devices]
            bars = ax.barh(names, costs, color=colors, alpha=0.85, edgecolor="white", linewidth=0.5)
            ax.set_xlabel("€/Jahr", fontsize=9)
            ax.set_title(self.t("standby.chart.title"), fontsize=10)
            for bar, cost in zip(bars, costs):
                if cost > 0:
                    ax.text(bar.get_width() + max(costs) * 0.02, bar.get_y() + bar.get_height() / 2,
                           f"{cost:.0f} €", va="center", fontsize=8, fontweight="bold")
            ax.grid(axis="x", alpha=0.3)
            ax.set_axisbelow(True)
        else:
            ax.text(0.5, 0.5, self.t("standby.no_data"), ha="center", va="center", fontsize=11, color="gray")
            ax.axis("off")

        # Profile (show first device by default)
        self._standby_profile_ax.clear()
        if report.devices:
            self._draw_standby_profile(report.devices[0])

        self._standby_fig.tight_layout()
        self._standby_canvas.draw_idle()

    def _draw_standby_profile(self, dev) -> None:
        ax = self._standby_profile_ax
        ax.clear()
        if dev.hourly_profile:
            hours = list(range(24))
            vals = dev.hourly_profile
            # Gradient colors: night=dark, day=blue
            colors = ["#34495e" if 0 <= h <= 5 else "#3498db" if 6 <= h <= 21 else "#34495e" for h in hours]
            ax.bar(hours, vals, color=colors, alpha=0.85, edgecolor="white", linewidth=0.5)
            ax.axhline(y=dev.base_load_w, color="#e74c3c", linestyle="--", alpha=0.8,
                      linewidth=1.5, label=f"Standby: {dev.base_load_w:.0f} W")
            ax.fill_between(hours, 0, dev.base_load_w, color="#e74c3c", alpha=0.08)
            ax.set_title(self.t("standby.chart.profile", device=dev.device_name[:20]), fontsize=10)
            ax.set_xlabel("h", fontsize=9)
            ax.set_ylabel("W", fontsize=9)
            ax.legend(fontsize=8, loc="upper right")
            ax.grid(axis="y", alpha=0.3)
            ax.set_axisbelow(True)
            ax.set_xticks([0, 4, 8, 12, 16, 20])

    def _on_standby_select(self, event) -> None:
        sel = self._standby_tree.selection()
        if not sel or not hasattr(self, "_standby_report"):
            return
        idx = self._standby_tree.index(sel[0])
        if idx >= len(self._standby_report.devices):
            return
        self._draw_standby_profile(self._standby_report.devices[idx])
        self._standby_fig.tight_layout()
        self._standby_canvas.draw_idle()
