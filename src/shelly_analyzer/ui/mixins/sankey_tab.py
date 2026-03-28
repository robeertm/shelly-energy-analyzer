"""Energy flow (Sankey) diagram tab mixin."""
from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure


class SankeyMixin:
    """Adds the Energy Flow (Sankey) tab."""

    def _build_sankey_tab(self) -> None:
        frm = self.tab_sankey

        # ── Top bar ──────────────────────────────────────────────────────
        top = ttk.Frame(frm)
        top.pack(fill="x", padx=14, pady=(12, 4))
        ttk.Label(top, text=self.t("sankey.title"), font=("", 14, "bold")).pack(side="left")

        # ── Period selector ──────────────────────────────────────────────
        ctrl = ttk.Frame(frm)
        ctrl.pack(fill="x", padx=14, pady=(0, 6))
        ttk.Label(ctrl, text=self.t("sankey.period")).pack(side="left", padx=(0, 6))
        self._sankey_period_var = tk.StringVar(value="today")
        for val, label_key in [
            ("today", "sankey.period.today"),
            ("week", "sankey.period.week"),
            ("month", "sankey.period.month"),
            ("year", "sankey.period.year"),
        ]:
            ttk.Radiobutton(ctrl, text=self.t(label_key), variable=self._sankey_period_var,
                          value=val, command=self._refresh_sankey_tab).pack(side="left", padx=4)

        # ── Content area (fills both directions) ─────────────────────────
        content = ttk.Frame(frm)
        content.pack(fill="both", expand=True)
        content.rowconfigure(0, weight=0)  # cards
        content.rowconfigure(1, weight=1)  # chart
        content.columnconfigure(0, weight=1)

        # ── Summary cards ────────────────────────────────────────────────
        cards = ttk.Frame(content)
        cards.grid(row=0, column=0, sticky="ew", padx=14, pady=(4, 4))
        cards.columnconfigure((0, 1, 2, 3, 4), weight=1)

        self._sankey_vars = {}
        for col, (key, label_key, icon) in enumerate([
            ("grid_import", "sankey.grid_import", "🔴"),
            ("total", "sankey.total", "🏠"),
            ("pv_production", "sankey.pv_production", "☀️"),
            ("self_consumption", "sankey.self_consumption", "♻️"),
            ("feed_in", "sankey.feed_in", "📤"),
        ]):
            card = ttk.LabelFrame(cards, text=f"{icon} {self.t(label_key)}")
            card.grid(row=0, column=col, sticky="nsew", padx=3, pady=3)
            v = tk.StringVar(value="–")
            self._sankey_vars[key] = v
            ttk.Label(card, textvariable=v, font=("", 13, "bold")).pack(anchor="center", padx=8, pady=8)

        # ── Sankey chart (fills remaining space) ──────────────────────────
        chart_lf = ttk.LabelFrame(content, text=self.t("sankey.title"))
        chart_lf.grid(row=1, column=0, sticky="nsew", padx=14, pady=(4, 12))

        self._sankey_fig = Figure(figsize=(10, 5), dpi=96)
        self._sankey_ax = self._sankey_fig.add_subplot(111)
        self._sankey_canvas = FigureCanvasTkAgg(self._sankey_fig, master=chart_lf)
        self._sankey_canvas.get_tk_widget().pack(fill="both", expand=True)

        self.after(600, self._refresh_sankey_tab)

    def _refresh_sankey_tab(self) -> None:
        from shelly_analyzer.services.sankey import compute_sankey

        period = self._sankey_period_var.get()
        data = compute_sankey(self.storage.db, self.cfg.devices, self.cfg.solar, period)

        self._sankey_vars["grid_import"].set(f"{data.grid_import_kwh:.2f} kWh")
        self._sankey_vars["total"].set(f"{data.total_consumption_kwh:.2f} kWh")
        self._sankey_vars["pv_production"].set(f"{data.pv_production_kwh:.2f} kWh")
        self._sankey_vars["self_consumption"].set(f"{data.self_consumption_kwh:.2f} kWh")
        self._sankey_vars["feed_in"].set(f"{data.feed_in_kwh:.2f} kWh")

        ax = self._sankey_ax
        ax.clear()

        if not data.flows:
            ax.text(0.5, 0.5, self.t("sankey.no_data"), ha="center", va="center",
                   fontsize=14, color="gray")
            ax.axis("off")
            self._sankey_canvas.draw_idle()
            return

        # Draw horizontal stacked bar chart as a simpler, cleaner energy flow visualization
        self._draw_energy_flow(ax, data)
        self._sankey_fig.tight_layout()
        self._sankey_canvas.draw_idle()

    def _draw_energy_flow(self, ax, data) -> None:
        """Draw a clean energy flow visualization using horizontal stacked bars."""
        import numpy as np

        ax.axis("off")
        if not data.flows or not data.nodes:
            return

        # Categorize flows
        sources = {}  # source → total kWh
        targets = {}  # target → total kWh
        for f in data.flows:
            sources[f.source] = sources.get(f.source, 0) + f.value_kwh
            targets[f.target] = targets.get(f.target, 0) + f.value_kwh

        # Left side: energy sources (Grid, PV)
        # Right side: energy consumers (devices)
        left_items = [(n, v) for n, v in sources.items() if n != "House"]
        right_items = [(n, v) for n, v in targets.items() if n != "House" and n != "Feed-in"]
        special = [(n, v) for n, v in targets.items() if n == "Feed-in"]

        left_items.sort(key=lambda x: x[1], reverse=True)
        right_items.sort(key=lambda x: x[1], reverse=True)

        color_map = dict(zip(data.nodes, data.node_colors))
        total = max(data.total_consumption_kwh, 0.01)

        # --- Draw source bars (left) ---
        y = 0.85
        ax.text(0.12, 0.95, "Quellen", ha="center", va="top", fontsize=11, fontweight="bold", color="#555")
        for name, val in left_items:
            color = color_map.get(name, "#3498db")
            w = max(val / total * 0.2, 0.01)
            ax.barh(y, w, height=0.06, left=0.02, color=color, alpha=0.9, edgecolor="white")
            ax.text(0.02 + w + 0.01, y, f"{name}  {val:.1f} kWh", va="center", fontsize=9, fontweight="bold")
            y -= 0.10

        # --- Central "House" node ---
        house_y = 0.5
        ax.add_patch(
            __import__("matplotlib.patches", fromlist=["FancyBboxPatch"]).FancyBboxPatch(
                (0.38, house_y - 0.08), 0.24, 0.16,
                boxstyle="round,pad=0.02", facecolor="#3498db", alpha=0.15, edgecolor="#3498db", linewidth=2
            )
        )
        ax.text(0.5, house_y, f"🏠 {data.total_consumption_kwh:.1f} kWh",
               ha="center", va="center", fontsize=12, fontweight="bold", color="#2c3e50")

        # --- Draw consumer bars (right) ---
        y = 0.85
        ax.text(0.82, 0.95, "Verbraucher", ha="center", va="top", fontsize=11, fontweight="bold", color="#555")
        dev_colors = ["#3498db", "#9b59b6", "#1abc9c", "#e67e22", "#2ecc71",
                     "#e91e63", "#00bcd4", "#ff9800", "#795548", "#607d8b"]
        for i, (name, val) in enumerate(right_items[:8]):
            color = dev_colors[i % len(dev_colors)]
            w = max(val / total * 0.2, 0.01)
            ax.barh(y, w, height=0.06, left=0.68, color=color, alpha=0.9, edgecolor="white")
            pct = val / total * 100
            ax.text(0.68 + w + 0.01, y, f"{name}  {val:.1f} kWh ({pct:.0f}%)", va="center", fontsize=9)
            y -= 0.10

        # Feed-in (if any)
        for name, val in special:
            ax.text(0.5, 0.12, f"📤 {self.t('sankey.feed_in')}: {val:.2f} kWh",
                   ha="center", va="center", fontsize=10, color="#27ae60", fontweight="bold")

        # Draw flow arrows
        for name, val in left_items:
            color = color_map.get(name, "#3498db")
            ax.annotate("", xy=(0.38, house_y), xytext=(0.22, 0.85 - left_items.index((name, val)) * 0.10),
                       arrowprops=dict(arrowstyle="->", color=color, lw=max(1, val / total * 5), alpha=0.4))

        for i, (name, val) in enumerate(right_items[:8]):
            color = dev_colors[i % len(dev_colors)]
            ax.annotate("", xy=(0.68, 0.85 - i * 0.10), xytext=(0.62, house_y),
                       arrowprops=dict(arrowstyle="->", color=color, lw=max(1, val / total * 5), alpha=0.4))

        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
