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

    @staticmethod
    def _sankey_band(ax, x0, y0, h0, x1, y1, h1, color, alpha=0.35):
        """Draw a smooth S-curve band (filled bezier polygon) between two vertical slots."""
        import matplotlib.path as mpath
        import matplotlib.patches as mpatches
        import numpy as np

        # Control point offset for smooth S-curve (40% of horizontal distance)
        dx = (x1 - x0) * 0.4

        # Top edge: cubic bezier from (x0, y0+h0/2) to (x1, y1+h1/2)
        top_l = (x0, y0 + h0 / 2)
        top_r = (x1, y1 + h1 / 2)
        # Bottom edge: cubic bezier from (x1, y1-h1/2) back to (x0, y0-h0/2)
        bot_r = (x1, y1 - h1 / 2)
        bot_l = (x0, y0 - h0 / 2)

        verts = [
            top_l,                           # start top-left
            (x0 + dx, y0 + h0 / 2),         # control 1
            (x1 - dx, y1 + h1 / 2),         # control 2
            top_r,                           # end top-right
            bot_r,                           # start bottom-right
            (x1 - dx, y1 - h1 / 2),         # control 1
            (x0 + dx, y0 - h0 / 2),         # control 2
            bot_l,                           # end bottom-left
            top_l,                           # close
        ]
        codes = [
            mpath.Path.MOVETO,
            mpath.Path.CURVE4, mpath.Path.CURVE4, mpath.Path.CURVE4,
            mpath.Path.LINETO,
            mpath.Path.CURVE4, mpath.Path.CURVE4, mpath.Path.CURVE4,
            mpath.Path.CLOSEPOLY,
        ]
        path = mpath.Path(verts, codes)
        patch = mpatches.PathPatch(path, facecolor=color, edgecolor="none", alpha=alpha)
        ax.add_patch(patch)

    def _draw_energy_flow(self, ax, data, tc=None) -> None:
        """Draw Sankey-style energy flow with smooth filled bezier bands."""
        from matplotlib.patches import FancyBboxPatch

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
        n_left = max(len(left_items), 1)
        n_right = max(len(right_items), 1)

        palette = ["#2196F3", "#9C27B0", "#26A69A", "#FF9800", "#66BB6A",
                    "#EC407A", "#29B6F6", "#FF7043", "#8D6E63", "#78909C"]
        src_color = "#e53935"
        pv_color = "#FF9800"

        # Layout
        SRC_X, SRC_W = 0.02, 0.15
        HOUSE_X, HOUSE_W = 0.30, 0.16
        TGT_X, TGT_W = 0.60, 0.39
        GAP = 0.015
        TOP, BOT = 0.90, 0.12
        usable = TOP - BOT

        # Compute node heights proportional to value
        src_total_h = usable - (n_left - 1) * GAP
        tgt_total_h = usable - (n_right - 1) * GAP

        # --- Source nodes ---
        src_cy = {}
        src_hs = {}
        y_cursor = TOP
        for name, val in left_items:
            h = max(0.04, (val / total) * src_total_h)
            cy = y_cursor - h / 2
            src_cy[name] = cy
            src_hs[name] = h
            color = pv_color if "pv" in name.lower() or "solar" in name.lower() else src_color
            box = FancyBboxPatch((SRC_X, cy - h / 2), SRC_W, h,
                                  boxstyle="round,pad=0.008", facecolor=color, alpha=0.92, edgecolor="none")
            ax.add_patch(box)
            pct = val / total * 100
            if h > 0.05:
                ax.text(SRC_X + SRC_W / 2, cy + 0.008, name,
                        ha="center", va="center", fontsize=8, fontweight="bold", color="white")
                ax.text(SRC_X + SRC_W / 2, cy - 0.018, f"{val:.1f} kWh ({pct:.0f}%)",
                        ha="center", va="center", fontsize=6.5, color="white", alpha=0.9)
            else:
                ax.text(SRC_X + SRC_W / 2, cy, f"{name} {val:.1f}",
                        ha="center", va="center", fontsize=6, fontweight="bold", color="white")
            y_cursor -= h + GAP

        # --- House node ---
        house_cy = (TOP + BOT) / 2
        house_h = min(0.14, usable * 0.25)
        box = FancyBboxPatch((HOUSE_X, house_cy - house_h / 2), HOUSE_W, house_h,
                              boxstyle="round,pad=0.015",
                              facecolor="#1565C0" if is_dark else "#E3F2FD",
                              alpha=0.6 if is_dark else 0.8,
                              edgecolor="#1976D2", linewidth=1.5)
        ax.add_patch(box)
        ax.text(HOUSE_X + HOUSE_W / 2, house_cy + 0.012,
                self.t("sankey.total"), ha="center", va="center", fontsize=7, color=fg, alpha=0.7)
        ax.text(HOUSE_X + HOUSE_W / 2, house_cy - 0.018,
                f"{data.total_consumption_kwh:.1f} kWh",
                ha="center", va="center", fontsize=10, fontweight="bold", color=fg)

        # --- Consumer nodes ---
        tgt_cy = {}
        tgt_hs = {}
        y_cursor = TOP
        for i, (name, val) in enumerate(right_items):
            h = max(0.035, (val / total) * tgt_total_h)
            cy = y_cursor - h / 2
            tgt_cy[name] = cy
            tgt_hs[name] = h
            color = palette[i % len(palette)]
            box = FancyBboxPatch((TGT_X, cy - h / 2), TGT_W, h,
                                  boxstyle="round,pad=0.008", facecolor=color, alpha=0.88, edgecolor="none")
            ax.add_patch(box)
            pct = val / total * 100
            label = name if len(name) <= 20 else name[:18] + ".."
            ax.text(TGT_X + TGT_W / 2, cy,
                    f"{label}   {val:.1f} kWh ({pct:.0f}%)",
                    ha="center", va="center", fontsize=7, fontweight="bold", color="white")
            y_cursor -= h + GAP

        # Feed-in
        for name, val in feed_in:
            if val > 0.001:
                ax.text(HOUSE_X + HOUSE_W / 2, BOT - 0.04,
                        f"{self.t('sankey.feed_in')}: {val:.2f} kWh",
                        ha="center", va="center", fontsize=9, color="#43A047", fontweight="bold")

        # --- Flow bands: Source → House ---
        # Stack bands on the house node left edge
        house_band_y = house_cy + house_h / 2  # start from top of house
        for name, val in left_items:
            color = pv_color if "pv" in name.lower() or "solar" in name.lower() else src_color
            band_h_src = src_hs[name]
            band_h_house = max(0.01, (val / total) * house_h)
            house_band_y -= band_h_house
            self._sankey_band(ax,
                              SRC_X + SRC_W, src_cy[name], band_h_src * 0.85,
                              HOUSE_X, house_band_y + band_h_house / 2, band_h_house,
                              color, alpha=0.30)

        # --- Flow bands: House → Consumers ---
        house_band_y = house_cy + house_h / 2  # start from top of house
        for i, (name, val) in enumerate(right_items):
            color = palette[i % len(palette)]
            band_h_tgt = tgt_hs[name]
            band_h_house = max(0.01, (val / total) * house_h)
            house_band_y -= band_h_house
            self._sankey_band(ax,
                              HOUSE_X + HOUSE_W, house_band_y + band_h_house / 2, band_h_house,
                              TGT_X, tgt_cy[name], band_h_tgt * 0.85,
                              color, alpha=0.30)

        # Column headers
        ax.text(SRC_X + SRC_W / 2, 0.97, self.t("sankey.sources"),
                ha="center", va="top", fontsize=9, fontweight="bold", color=fg)
        ax.text(TGT_X + TGT_W / 2, 0.97, self.t("sankey.consumers"),
                ha="center", va="top", fontsize=9, fontweight="bold", color=fg)

        ax.set_xlim(-0.01, 1.01)
        ax.set_ylim(-0.08, 1.01)
