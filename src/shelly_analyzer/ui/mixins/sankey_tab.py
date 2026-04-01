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

        tc = self._get_theme_colors()
        self._apply_plot_theme(self._sankey_fig, ax, self._sankey_canvas)

        if not data.flows:
            ax.text(0.5, 0.5, self.t("sankey.no_data"), ha="center", va="center",
                   fontsize=14, color=tc["muted"])
            ax.axis("off")
            self._sankey_canvas.draw_idle()
            return

        # Draw horizontal stacked bar chart as a simpler, cleaner energy flow visualization
        self._draw_energy_flow(ax, data, tc)
        self._sankey_fig.tight_layout()
        self._sankey_canvas.draw_idle()

    def _draw_energy_flow(self, ax, data, tc=None) -> None:
        """Draw an enhanced energy flow visualization with gradient flows."""
        import numpy as np
        from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
        import matplotlib.patheffects as pe

        if tc is None:
            tc = self._get_theme_colors()

        ax.axis("off")
        if not data.flows or not data.nodes:
            return

        fg = tc["fg"]
        bg = tc["bg"]

        # Categorize flows
        sources = {}
        targets = {}
        for f in data.flows:
            sources[f.source] = sources.get(f.source, 0) + f.value_kwh
            targets[f.target] = targets.get(f.target, 0) + f.value_kwh

        left_items = [(n, v) for n, v in sources.items() if n != "House"]
        right_items = [(n, v) for n, v in targets.items() if n != "House" and n != "Feed-in"]
        special = [(n, v) for n, v in targets.items() if n == "Feed-in"]

        left_items.sort(key=lambda x: x[1], reverse=True)
        right_items.sort(key=lambda x: x[1], reverse=True)

        color_map = dict(zip(data.nodes, data.node_colors))
        total = max(data.total_consumption_kwh, 0.01)

        dev_colors = ["#2196F3", "#9C27B0", "#1abc9c", "#FF9800", "#4CAF50",
                      "#E91E63", "#00BCD4", "#FF5722", "#795548", "#607D8B"]

        # --- Source nodes (left) ---
        ax.text(0.10, 0.97, self.t("sankey.sources"), ha="center", va="top",
                fontsize=11, fontweight="bold", color=fg)
        src_positions = {}
        y = 0.88
        for name, val in left_items:
            color = color_map.get(name, tc["blue"])
            h = max(0.06, val / total * 0.25)
            box = FancyBboxPatch((0.01, y - h / 2), 0.18, h,
                                  boxstyle="round,pad=0.01", facecolor=color, alpha=0.85,
                                  edgecolor=color, linewidth=1.5)
            box.set_path_effects([pe.withSimplePatchShadow(offset=(1, -1), shadow_rgbFace=(0, 0, 0), alpha=0.15)])
            ax.add_patch(box)
            pct = val / total * 100
            ax.text(0.10, y, f"{name}\n{val:.1f} kWh ({pct:.0f}%)",
                    ha="center", va="center", fontsize=8, fontweight="bold", color="white")
            src_positions[name] = y
            y -= max(0.12, h + 0.04)

        # --- Central "House" node ---
        house_y = 0.50
        house_box = FancyBboxPatch((0.35, house_y - 0.10), 0.30, 0.20,
                                    boxstyle="round,pad=0.03", facecolor=tc["blue"], alpha=0.12,
                                    edgecolor=tc["blue"], linewidth=2.5)
        house_box.set_path_effects([pe.withSimplePatchShadow(offset=(2, -2), shadow_rgbFace=(0, 0, 0), alpha=0.1)])
        ax.add_patch(house_box)
        ax.text(0.50, house_y + 0.02, "\U0001f3e0", ha="center", va="center", fontsize=18)
        ax.text(0.50, house_y - 0.04, f"{data.total_consumption_kwh:.1f} kWh",
                ha="center", va="center", fontsize=11, fontweight="bold", color=fg)

        # --- Consumer nodes (right) ---
        ax.text(0.85, 0.97, self.t("sankey.consumers"), ha="center", va="top",
                fontsize=11, fontweight="bold", color=fg)
        tgt_positions = {}
        y = 0.88
        for i, (name, val) in enumerate(right_items[:8]):
            color = dev_colors[i % len(dev_colors)]
            h = max(0.05, val / total * 0.20)
            box = FancyBboxPatch((0.72, y - h / 2), 0.27, h,
                                  boxstyle="round,pad=0.01", facecolor=color, alpha=0.8,
                                  edgecolor=color, linewidth=1)
            box.set_path_effects([pe.withSimplePatchShadow(offset=(1, -1), shadow_rgbFace=(0, 0, 0), alpha=0.12)])
            ax.add_patch(box)
            pct = val / total * 100
            label = name if len(name) <= 14 else name[:12] + ".."
            ax.text(0.855, y, f"{label}  {val:.1f} kWh ({pct:.0f}%)",
                    ha="center", va="center", fontsize=7, fontweight="bold", color="white")
            tgt_positions[name] = y
            y -= max(0.09, h + 0.03)

        # Feed-in (if any)
        for name, val in special:
            ax.text(0.50, 0.08, f"\U0001f4e4 {self.t('sankey.feed_in')}: {val:.2f} kWh",
                    ha="center", va="center", fontsize=10, color=tc["green"], fontweight="bold",
                    path_effects=[pe.withStroke(linewidth=2, foreground=bg)])

        # --- Draw flow curves (bezier-like) ---
        from matplotlib.patches import FancyArrowPatch
        import matplotlib.path as mpath

        for name, val in left_items:
            color = color_map.get(name, tc["blue"])
            sy = src_positions.get(name, 0.5)
            lw = max(1.5, val / total * 8)
            arrow = FancyArrowPatch(
                (0.19, sy), (0.35, house_y),
                connectionstyle="arc3,rad=0.15",
                arrowstyle="-|>", color=color, linewidth=lw,
                alpha=0.35, mutation_scale=12,
            )
            ax.add_patch(arrow)

        for i, (name, val) in enumerate(right_items[:8]):
            color = dev_colors[i % len(dev_colors)]
            ty = tgt_positions.get(name, 0.5)
            lw = max(1.0, val / total * 6)
            arrow = FancyArrowPatch(
                (0.65, house_y), (0.72, ty),
                connectionstyle="arc3,rad=-0.12",
                arrowstyle="-|>", color=color, linewidth=lw,
                alpha=0.35, mutation_scale=10,
            )
            ax.add_patch(arrow)

        ax.set_xlim(-0.02, 1.02)
        ax.set_ylim(0, 1.02)
