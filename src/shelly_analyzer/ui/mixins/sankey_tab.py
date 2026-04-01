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
        """Draw energy flow: sources → house → consumers with thick band flows."""
        from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

        if tc is None:
            tc = self._get_theme_colors()

        ax.axis("off")
        if not data.flows or not data.nodes:
            return

        fg = tc["fg"]
        bg = tc["bg"]
        is_dark = bg.lower() in ("#111111", "#111", "#000", "#000000")

        sources = {}
        targets = {}
        for f in data.flows:
            sources[f.source] = sources.get(f.source, 0) + f.value_kwh
            targets[f.target] = targets.get(f.target, 0) + f.value_kwh

        left_items = [(n, v) for n, v in sources.items() if n != "House"]
        right_items = [(n, v) for n, v in targets.items() if n != "House" and n != "Feed-in"]
        feed_in = [(n, v) for n, v in targets.items() if n == "Feed-in"]

        left_items.sort(key=lambda x: x[1], reverse=True)
        right_items.sort(key=lambda x: x[1], reverse=True)
        right_items = right_items[:10]

        total = max(data.total_consumption_kwh, 0.01)
        n_right = max(len(right_items), 1)

        palette = ["#2196F3", "#9C27B0", "#26A69A", "#FF9800", "#66BB6A",
                    "#EC407A", "#29B6F6", "#FF7043", "#8D6E63", "#78909C"]
        src_color = "#e74c3c"  # red for grid
        pv_color = "#f39c12"   # orange for PV

        # Layout constants
        SRC_X = 0.02
        SRC_W = 0.16
        HOUSE_X = 0.32
        HOUSE_W = 0.18
        TGT_X = 0.64
        TGT_W = 0.35

        # Vertical centering: fit everything in 0.08..0.95
        top_y = 0.92
        bot_y = 0.10
        usable = top_y - bot_y

        # --- Source nodes (left, vertically centered) ---
        n_src = max(len(left_items), 1)
        src_h = min(0.12, usable / n_src - 0.02)
        src_spacing = usable / n_src
        src_positions = {}
        for i, (name, val) in enumerate(left_items):
            cy = top_y - i * src_spacing - src_spacing / 2
            color = pv_color if "pv" in name.lower() or "solar" in name.lower() else src_color
            box = FancyBboxPatch((SRC_X, cy - src_h / 2), SRC_W, src_h,
                                  boxstyle="round,pad=0.01", facecolor=color, alpha=0.9,
                                  edgecolor="none")
            ax.add_patch(box)
            pct = val / total * 100
            txt_c = "white"
            ax.text(SRC_X + SRC_W / 2, cy + 0.01, name,
                    ha="center", va="center", fontsize=8, fontweight="bold", color=txt_c)
            ax.text(SRC_X + SRC_W / 2, cy - 0.025, f"{val:.1f} kWh ({pct:.0f}%)",
                    ha="center", va="center", fontsize=7, color=txt_c, alpha=0.9)
            src_positions[name] = cy

        # --- House node (center) ---
        house_cy = (top_y + bot_y) / 2
        house_h = 0.14
        box = FancyBboxPatch((HOUSE_X, house_cy - house_h / 2), HOUSE_W, house_h,
                              boxstyle="round,pad=0.02",
                              facecolor=tc["blue"] if not is_dark else "#1a3a5c",
                              alpha=0.18, edgecolor=tc["blue"], linewidth=2)
        ax.add_patch(box)
        ax.text(HOUSE_X + HOUSE_W / 2, house_cy + 0.015,
                self.t("sankey.total"), ha="center", va="center", fontsize=8, color=fg)
        ax.text(HOUSE_X + HOUSE_W / 2, house_cy - 0.025,
                f"{data.total_consumption_kwh:.1f} kWh",
                ha="center", va="center", fontsize=11, fontweight="bold", color=fg)

        # --- Consumer nodes (right) ---
        tgt_h = min(0.08, usable / n_right - 0.01)
        tgt_spacing = usable / n_right
        tgt_positions = {}
        for i, (name, val) in enumerate(right_items):
            cy = top_y - i * tgt_spacing - tgt_spacing / 2
            color = palette[i % len(palette)]
            box = FancyBboxPatch((TGT_X, cy - tgt_h / 2), TGT_W, tgt_h,
                                  boxstyle="round,pad=0.01", facecolor=color, alpha=0.85,
                                  edgecolor="none")
            ax.add_patch(box)
            pct = val / total * 100
            label = name if len(name) <= 18 else name[:16] + ".."
            ax.text(TGT_X + TGT_W / 2, cy,
                    f"{label}   {val:.1f} kWh ({pct:.0f}%)",
                    ha="center", va="center", fontsize=7.5, fontweight="bold", color="white")
            tgt_positions[name] = cy

        # Feed-in below house
        for name, val in feed_in:
            if val > 0.001:
                ax.text(HOUSE_X + HOUSE_W / 2, bot_y - 0.02,
                        f"{self.t('sankey.feed_in')}: {val:.2f} kWh",
                        ha="center", va="center", fontsize=9, color=tc["green"], fontweight="bold")

        # --- Flow bands (thick, semi-transparent) ---
        for name, val in left_items:
            sy = src_positions[name]
            color = pv_color if "pv" in name.lower() or "solar" in name.lower() else src_color
            lw = max(3, val / total * 20)
            arrow = FancyArrowPatch(
                (SRC_X + SRC_W, sy), (HOUSE_X, house_cy),
                connectionstyle="arc3,rad=0.08",
                arrowstyle="-|>", color=color, linewidth=lw,
                alpha=0.25, mutation_scale=15,
            )
            ax.add_patch(arrow)

        for i, (name, val) in enumerate(right_items):
            ty = tgt_positions[name]
            color = palette[i % len(palette)]
            lw = max(2, val / total * 15)
            arrow = FancyArrowPatch(
                (HOUSE_X + HOUSE_W, house_cy), (TGT_X, ty),
                connectionstyle="arc3,rad=-0.05",
                arrowstyle="-|>", color=color, linewidth=lw,
                alpha=0.25, mutation_scale=12,
            )
            ax.add_patch(arrow)

        # Column headers
        ax.text(SRC_X + SRC_W / 2, 0.98, self.t("sankey.sources"),
                ha="center", va="top", fontsize=10, fontweight="bold", color=fg)
        ax.text(TGT_X + TGT_W / 2, 0.98, self.t("sankey.consumers"),
                ha="center", va="top", fontsize=10, fontweight="bold", color=fg)

        ax.set_xlim(-0.01, 1.01)
        ax.set_ylim(-0.05, 1.02)
