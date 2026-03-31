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

        # ── Content area (fills both directions) ─────────────────────────
        content = ttk.Frame(frm)
        content.pack(fill="both", expand=True)
        content.rowconfigure(0, weight=0)  # cards
        content.rowconfigure(1, weight=0)  # tip + table
        content.rowconfigure(2, weight=1)  # charts
        content.columnconfigure(0, weight=1)

        # ── Summary cards ────────────────────────────────────────────────
        cards = ttk.Frame(content)
        cards.grid(row=0, column=0, sticky="ew", padx=14, pady=(4, 4))
        cards.columnconfigure((0, 1), weight=1)

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

        # ── Tip + Table ──────────────────────────────────────────────────
        mid = ttk.Frame(content)
        mid.grid(row=1, column=0, sticky="ew", padx=14, pady=(0, 4))

        ttk.Label(mid, text=self.t("standby.tip"), wraplength=900, foreground="gray").pack(anchor="w", pady=(2, 4))

        cols = ("device", "base_load", "annual_kwh", "annual_cost", "share", "risk")
        self._standby_tree = ttk.Treeview(mid, columns=cols, show="headings", height=6)
        for col_id, hdr_key, w in [
            ("device",     "standby.col.device",      200),
            ("base_load",  "standby.col.base_load",    100),
            ("annual_kwh", "standby.col.annual_kwh",   100),
            ("annual_cost","standby.col.annual_cost",   100),
            ("share",      "standby.col.share",         80),
            ("risk",       "standby.col.risk",           80),
        ]:
            self._standby_tree.heading(col_id, text=self.t(hdr_key))
            self._standby_tree.column(col_id, width=w, anchor="center" if col_id != "device" else "w")
        self._standby_tree.pack(fill="x")

        # ── Charts (fill remaining space) ────────────────────────────────
        charts = ttk.Frame(content)
        charts.grid(row=2, column=0, sticky="nsew", padx=14, pady=(4, 12))
        charts.columnconfigure(0, weight=1)
        charts.columnconfigure(1, weight=1)
        charts.rowconfigure(0, weight=1)

        bar_lf = ttk.LabelFrame(charts, text=self.t("standby.chart.title"))
        bar_lf.grid(row=0, column=0, sticky="nsew", padx=(0, 4))

        self._standby_bar_fig = Figure(figsize=(5, 3.5), dpi=96)
        self._standby_bar_ax = self._standby_bar_fig.add_subplot(111)
        self._standby_bar_canvas = FigureCanvasTkAgg(self._standby_bar_fig, master=bar_lf)
        self._standby_bar_canvas.get_tk_widget().pack(fill="both", expand=True)

        profile_lf = ttk.LabelFrame(charts, text="24h")
        profile_lf.grid(row=0, column=1, sticky="nsew", padx=(4, 0))

        self._standby_profile_fig = Figure(figsize=(5, 3.5), dpi=96)
        self._standby_profile_ax = self._standby_profile_fig.add_subplot(111)
        self._standby_profile_canvas = FigureCanvasTkAgg(self._standby_profile_fig, master=profile_lf)
        self._standby_profile_canvas.get_tk_widget().pack(fill="both", expand=True)

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
            self._standby_tree.insert("", "end", values=(
                dev.device_name, f"{dev.base_load_w:.0f} W", f"{dev.annual_standby_kwh:.0f}",
                f"{dev.annual_standby_cost:.2f}", f"{dev.standby_share_pct:.0f}%",
                self.t(f"standby.risk.{dev.risk}"),
            ))

        # Bar chart
        tc = self._get_theme_colors()
        ax = self._standby_bar_ax
        ax.clear()
        if report.devices:
            names = [d.device_name[:18] for d in report.devices]
            costs = [d.annual_standby_cost for d in report.devices]
            colors = [tc["red"] if d.risk == "high" else tc["orange"] if d.risk == "medium" else tc["green"] for d in report.devices]
            bars = ax.barh(names, costs, color=colors, alpha=0.85)
            ax.set_xlabel("€/Jahr", fontsize=9)
            for bar, cost in zip(bars, costs):
                if cost > 0:
                    ax.text(bar.get_width() + max(costs) * 0.02, bar.get_y() + bar.get_height() / 2,
                           f"{cost:.0f} €", va="center", fontsize=8, fontweight="bold", color=tc["fg"])
            ax.grid(axis="x", alpha=0.3)
            ax.set_axisbelow(True)
        else:
            ax.text(0.5, 0.5, self.t("standby.no_data"), ha="center", va="center", fontsize=11, color=tc["muted"])
            ax.axis("off")
        self._apply_plot_theme(self._standby_bar_fig, ax, self._standby_bar_canvas)
        self._standby_bar_fig.tight_layout()
        self._standby_bar_canvas.draw_idle()

        # Profile (first device)
        self._standby_profile_ax.clear()
        if report.devices:
            self._draw_standby_profile(report.devices[0])
        self._apply_plot_theme(self._standby_profile_fig, self._standby_profile_ax, self._standby_profile_canvas)
        self._standby_profile_fig.tight_layout()
        self._standby_profile_canvas.draw_idle()

    def _draw_standby_profile(self, dev) -> None:
        tc = self._get_theme_colors()
        ax = self._standby_profile_ax
        ax.clear()
        if dev.hourly_profile:
            hours = list(range(24))
            night_c = tc["purple"] if tc["bg"] == "#111111" else "#34495e"
            day_c = tc["blue"]
            colors = [night_c if 0 <= h <= 5 else day_c if 6 <= h <= 21 else night_c for h in hours]
            ax.bar(hours, dev.hourly_profile, color=colors, alpha=0.85)
            ax.axhline(y=dev.base_load_w, color=tc["red"], linestyle="--", alpha=0.8, linewidth=1.5,
                      label=f"Standby: {dev.base_load_w:.0f} W")
            ax.fill_between(hours, 0, dev.base_load_w, color=tc["red"], alpha=0.08)
            ax.set_title(dev.device_name[:20], fontsize=10)
            ax.set_xlabel("h", fontsize=9)
            ax.set_ylabel("W", fontsize=9)
            ax.legend(fontsize=8, loc="upper right")
            ax.grid(axis="y", alpha=0.3)
            ax.set_axisbelow(True)
            ax.set_xticks([0, 4, 8, 12, 16, 20])
            self._apply_plot_theme(self._standby_profile_fig, ax, self._standby_profile_canvas)

    def _on_standby_select(self, event) -> None:
        sel = self._standby_tree.selection()
        if not sel or not hasattr(self, "_standby_report"):
            return
        idx = self._standby_tree.index(sel[0])
        if idx >= len(self._standby_report.devices):
            return
        self._draw_standby_profile(self._standby_report.devices[idx])
        self._standby_profile_fig.tight_layout()
        self._standby_profile_canvas.draw_idle()
