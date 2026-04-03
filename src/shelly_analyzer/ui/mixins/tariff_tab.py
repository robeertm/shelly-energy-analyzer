from __future__ import annotations
import logging
import threading
import tkinter as tk
from tkinter import ttk

_log = logging.getLogger(__name__)


class TariffMixin:
    """Tariff comparison tab."""

    def _build_tariff_tab(self) -> None:
        frm = self.tab_tariff
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

        ttk.Label(inner, text=self.t("tariff.title"), font=("", 14, "bold")).pack(anchor="w", padx=12, pady=(12, 4))
        ttk.Label(inner, text=self.t("tariff.hint"), foreground="gray").pack(anchor="w", padx=12, pady=(0, 8))

        # Table
        cols = ("name", "provider", "type", "annual", "monthly", "effective", "savings")
        self._tariff_tree = ttk.Treeview(inner, columns=cols, show="headings", height=12)
        for col, hdr, w in [
            ("name", self.t("tariff.col_name"), 160),
            ("provider", self.t("tariff.col_provider"), 100),
            ("type", self.t("tariff.col_type"), 70),
            ("annual", self.t("tariff.col_annual"), 100),
            ("monthly", self.t("tariff.col_monthly"), 90),
            ("effective", "ct/kWh", 70),
            ("savings", self.t("tariff.col_savings"), 100),
        ]:
            self._tariff_tree.heading(col, text=hdr)
            self._tariff_tree.column(col, width=w, anchor="center")
        self._tariff_tree.pack(fill="both", expand=True, padx=12, pady=8)

        # Chart frame
        self._tariff_chart_frame = ttk.Frame(inner)
        self._tariff_chart_frame.pack(fill="both", expand=True, padx=12, pady=8)

        ttk.Button(inner, text=self.t("tariff.refresh_btn"), command=self._tariff_refresh).pack(padx=12, pady=8)

        self._tariff_refresh()

    def _tariff_refresh(self) -> None:
        def _worker():
            try:
                from shelly_analyzer.services.tariff_compare import compare_tariffs
                results = compare_tariffs(
                    self.storage.db, self.cfg,
                    current_price_eur_per_kwh=self.cfg.pricing.electricity_price_eur_per_kwh,
                    current_base_fee_eur_per_year=self.cfg.pricing.base_fee_eur_per_year,
                )
                def _update():
                    self._tariff_tree.delete(*self._tariff_tree.get_children())
                    for r in results:
                        savings_text = f"{'\U0001f7e2 ' if r.savings_vs_current_eur > 0 else '\U0001f534 '}{r.savings_vs_current_eur:+.0f} \u20ac"
                        if r.is_current:
                            savings_text = "\u2014"
                        self._tariff_tree.insert("", "end", values=(
                            ("\u2192 " if r.is_current else "") + r.name,
                            r.provider,
                            r.tariff_type.upper(),
                            f"{r.annual_cost_eur:.0f} \u20ac",
                            f"{r.monthly_avg_eur:.0f} \u20ac",
                            f"{r.effective_price_ct:.1f}",
                            savings_text,
                        ))
                    self._tariff_draw_chart(results)
                self.after(0, _update)
            except Exception as e:
                _log.error("Tariff comparison: %s", e)
        threading.Thread(target=_worker, daemon=True).start()

    def _tariff_draw_chart(self, results) -> None:
        for w in self._tariff_chart_frame.winfo_children():
            w.destroy()
        if not results:
            return
        try:
            import matplotlib
            matplotlib.use("Agg")
            from matplotlib.figure import Figure
            from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

            fig = Figure(figsize=(8, 3), dpi=100)
            ax = fig.add_subplot(111)
            names = [r.name[:20] for r in results]
            costs = [r.annual_cost_eur for r in results]
            colors = ["#ff9800" if r.is_current else "#2196F3" for r in results]

            bars = ax.barh(names, costs, color=colors)
            ax.set_xlabel("\u20ac/Jahr")
            ax.invert_yaxis()
            fig.tight_layout()

            canvas = FigureCanvasTkAgg(fig, self._tariff_chart_frame)
            canvas.draw()
            canvas.get_tk_widget().pack(fill="both", expand=True)
        except Exception as e:
            _log.debug("Tariff chart: %s", e)
