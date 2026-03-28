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

        # Top bar
        top = ttk.Frame(frm)
        top.pack(fill="x", padx=10, pady=(10, 5))
        ttk.Label(top, text=self.t("sankey.title"), font=("", 14, "bold")).pack(side="left")

        # Period selector
        period_frm = ttk.Frame(frm)
        period_frm.pack(fill="x", padx=10, pady=5)
        ttk.Label(period_frm, text=self.t("sankey.period")).pack(side="left")
        self._sankey_period_var = tk.StringVar(value="today")
        for val, label_key in [
            ("today", "sankey.period.today"),
            ("week", "sankey.period.week"),
            ("month", "sankey.period.month"),
            ("year", "sankey.period.year"),
        ]:
            ttk.Radiobutton(period_frm, text=self.t(label_key), variable=self._sankey_period_var,
                          value=val, command=self._refresh_sankey_tab).pack(side="left", padx=5)

        # Summary cards
        cards = ttk.Frame(frm)
        cards.pack(fill="x", padx=10, pady=5)
        self._sankey_vars = {}
        card_defs = [
            ("grid_import", "sankey.grid_import", "🔴"),
            ("total", "sankey.total", "🏠"),
            ("pv_production", "sankey.pv_production", "☀️"),
            ("self_consumption", "sankey.self_consumption", "♻️"),
            ("feed_in", "sankey.feed_in", "📤"),
        ]
        for i, (key, label_key, icon) in enumerate(card_defs):
            card = ttk.LabelFrame(cards, text=f"{icon} {self.t(label_key)}")
            card.grid(row=0, column=i, padx=4, pady=4, sticky="nsew")
            cards.columnconfigure(i, weight=1)
            var = tk.StringVar(value="–")
            self._sankey_vars[key] = var
            ttk.Label(card, textvariable=var, font=("", 12, "bold")).pack(padx=8, pady=6)

        # Sankey chart area (using matplotlib)
        chart_frame = ttk.Frame(frm)
        chart_frame.pack(fill="both", expand=True, padx=10, pady=(5, 10))

        self._sankey_fig = Figure(figsize=(10, 5), dpi=96)
        self._sankey_ax = self._sankey_fig.add_subplot(111)
        self._sankey_canvas = FigureCanvasTkAgg(self._sankey_fig, master=chart_frame)
        self._sankey_canvas.get_tk_widget().pack(fill="both", expand=True)

        self.after(600, self._refresh_sankey_tab)

    def _refresh_sankey_tab(self) -> None:
        from shelly_analyzer.services.sankey import compute_sankey

        period = self._sankey_period_var.get()
        data = compute_sankey(self.storage.db, self.cfg.devices, self.cfg.solar, period)

        # Update cards
        self._sankey_vars["grid_import"].set(f"{data.grid_import_kwh:.2f} kWh")
        self._sankey_vars["total"].set(f"{data.total_consumption_kwh:.2f} kWh")
        self._sankey_vars["pv_production"].set(f"{data.pv_production_kwh:.2f} kWh")
        self._sankey_vars["self_consumption"].set(f"{data.self_consumption_kwh:.2f} kWh")
        self._sankey_vars["feed_in"].set(f"{data.feed_in_kwh:.2f} kWh")

        # Draw Sankey-like flow diagram using matplotlib
        ax = self._sankey_ax
        ax.clear()

        if not data.flows:
            ax.text(0.5, 0.5, self.t("sankey.no_data"), ha="center", va="center", fontsize=14, color="#888")
            ax.axis("off")
            self._sankey_canvas.draw_idle()
            return

        # Use alluvial/flow visualization with rectangles and bezier curves
        self._draw_sankey_matplotlib(ax, data)
        self._sankey_fig.tight_layout()
        self._sankey_canvas.draw_idle()

    def _draw_sankey_matplotlib(self, ax, data) -> None:
        """Draw a Sankey-like diagram using matplotlib patches."""
        import matplotlib.patches as mpatches
        from matplotlib.path import Path as MPath
        import numpy as np

        ax.axis("off")

        if not data.flows or not data.nodes:
            return

        # Categorize nodes by column
        sources = set()
        targets = set()
        for f in data.flows:
            sources.add(f.source)
            targets.add(f.target)

        # Column assignment: pure sources (col 0), intermediate (col 1), pure targets (col 2)
        col0 = [n for n in data.nodes if n in sources and n not in targets]
        col2 = [n for n in data.nodes if n in targets and n not in sources]
        col1 = [n for n in data.nodes if n not in col0 and n not in col2]

        if not col1:
            col1 = ["House"]

        columns = [col0, col1, col2]
        x_positions = [0.1, 0.45, 0.8]

        # Calculate y positions for each node
        node_positions = {}
        node_heights = {}

        total_flow = sum(f.value_kwh for f in data.flows)
        if total_flow <= 0:
            total_flow = 1.0

        for col_idx, col_nodes in enumerate(columns):
            if not col_nodes:
                continue
            # Get total value for nodes in this column
            node_vals = {}
            for n in col_nodes:
                val = sum(f.value_kwh for f in data.flows if f.source == n or f.target == n)
                node_vals[n] = max(val, 0.01)

            total_col = sum(node_vals.values())
            y_start = 0.1
            available = 0.8
            gap = 0.02 * len(col_nodes)

            for n in col_nodes:
                height = (node_vals[n] / total_col) * (available - gap)
                node_positions[n] = (x_positions[col_idx], y_start)
                node_heights[n] = height
                y_start += height + 0.02

        # Draw nodes
        node_color_map = dict(zip(data.nodes, data.node_colors))
        for name, (x, y) in node_positions.items():
            h = node_heights.get(name, 0.05)
            color = node_color_map.get(name, "#3498db")
            rect = mpatches.FancyBboxPatch((x - 0.03, y), 0.06, h,
                                           boxstyle="round,pad=0.005",
                                           facecolor=color, edgecolor="white", linewidth=1.5, alpha=0.9)
            ax.add_patch(rect)
            # Label
            ax.text(x, y + h / 2, name, ha="center", va="center", fontsize=8,
                   fontweight="bold", color="white", zorder=10)

        # Draw flows as curved paths
        source_y_offset = {n: 0.0 for n in data.nodes}
        target_y_offset = {n: 0.0 for n in data.nodes}

        for flow in sorted(data.flows, key=lambda f: f.value_kwh, reverse=True):
            if flow.source not in node_positions or flow.target not in node_positions:
                continue

            sx, sy = node_positions[flow.source]
            sh = node_heights.get(flow.source, 0.05)
            tx, ty = node_positions[flow.target]
            th = node_heights.get(flow.target, 0.05)

            # Flow height proportional to value
            total_s = sum(f.value_kwh for f in data.flows if f.source == flow.source)
            total_t = sum(f.value_kwh for f in data.flows if f.target == flow.target)
            flow_h_s = (flow.value_kwh / max(total_s, 0.01)) * sh
            flow_h_t = (flow.value_kwh / max(total_t, 0.01)) * th

            y1 = sy + source_y_offset[flow.source]
            y2 = ty + target_y_offset[flow.target]

            source_y_offset[flow.source] += flow_h_s
            target_y_offset[flow.target] += flow_h_t

            # Bezier curve
            mid_x = (sx + tx) / 2
            verts = [
                (sx + 0.03, y1),
                (mid_x, y1),
                (mid_x, y2),
                (tx - 0.03, y2),
                (tx - 0.03, y2 + flow_h_t),
                (mid_x, y2 + flow_h_t),
                (mid_x, y1 + flow_h_s),
                (sx + 0.03, y1 + flow_h_s),
                (sx + 0.03, y1),
            ]
            codes = [MPath.MOVETO, MPath.CURVE4, MPath.CURVE4, MPath.CURVE4,
                    MPath.LINETO, MPath.CURVE4, MPath.CURVE4, MPath.CURVE4,
                    MPath.CLOSEPOLY]
            path = MPath(verts, codes)
            color = flow.color if flow.color else "#cccccc"
            patch = mpatches.PathPatch(path, facecolor=color, alpha=0.35, edgecolor="none")
            ax.add_patch(patch)

            # Flow label
            label_x = mid_x
            label_y = (y1 + y2 + flow_h_s) / 2
            if flow.value_kwh >= 0.1:
                ax.text(label_x, label_y, f"{flow.value_kwh:.1f}", ha="center", va="center",
                       fontsize=7, color="#333", alpha=0.8)

        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.set_title(self.t("sankey.title"), fontsize=11)
