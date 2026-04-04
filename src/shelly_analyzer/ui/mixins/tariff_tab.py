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
        ttk.Label(inner, text=self.t("tariff.hint"), foreground="gray").pack(anchor="w", padx=12, pady=(0, 4))

        # Consumption stats summary
        self._tariff_stats_var = tk.StringVar(value="")
        ttk.Label(inner, textvariable=self._tariff_stats_var, font=("", 11)).pack(anchor="w", padx=12, pady=(0, 8))

        # Card container for tariff results (replaces Treeview for dark-mode compat)
        self._tariff_cards_frame = ttk.Frame(inner)
        self._tariff_cards_frame.pack(fill="x", padx=12, pady=4)

        # Chart frame
        self._tariff_chart_frame = ttk.Frame(inner)
        self._tariff_chart_frame.pack(fill="x", padx=12, pady=8)

        self._tariff_refresh()

    def _tariff_refresh(self) -> None:
        def _worker():
            try:
                from shelly_analyzer.services.tariff_compare import compare_tariffs, _get_consumption_stats
                results = compare_tariffs(
                    self.storage.db, self.cfg,
                    current_price_eur_per_kwh=self.cfg.pricing.electricity_price_eur_per_kwh,
                    current_base_fee_eur_per_year=self.cfg.pricing.base_fee_eur_per_year,
                )
                stats = _get_consumption_stats(self.storage.db, self.cfg)
                def _update():
                    try:
                        if stats and hasattr(self, '_tariff_stats_var'):
                            self._tariff_stats_var.set(
                                f"{self.t('tariff.period')}: {stats['days']:.1f} {self.t('tariff.days')}  |  "
                                f"{self.t('tariff.consumption')}: {stats['total_kwh']:.0f} kWh  |  "
                                f"{self.t('tariff.annual_est')}: {stats['annual_kwh']:.0f} kWh/{self.t('tariff.year')}"
                            )
                        self._tariff_render_cards(results)
                        self._tariff_draw_chart(results)
                    except Exception as exc:
                        _log.error("Tariff _update error: %s", exc, exc_info=True)
                self.after(0, _update)
            except Exception as e:
                _log.error("Tariff comparison: %s", e)
        threading.Thread(target=_worker, daemon=True).start()

    def _tariff_render_cards(self, results) -> None:
        """Render tariff results as tk.Label cards (always visible, theme-proof)."""
        frm = self._tariff_cards_frame
        for w in frm.winfo_children():
            w.destroy()

        tc = self._get_theme_colors()

        for i, r in enumerate(results):
            if r.is_current:
                bg = "#2a2000" if tc["bg"] == "#111111" else "#fff3e0"
            else:
                bg = tc["bg"]
            border_color = "#ff9800" if r.is_current else tc.get("muted", "#555555")

            # Card frame with border effect
            card = tk.Frame(frm, bg=border_color, padx=1, pady=1)
            card.pack(fill="x", pady=2)
            card_inner = tk.Frame(card, bg=bg, padx=10, pady=6)
            card_inner.pack(fill="x")

            # Row 1: name + annual cost
            row1 = tk.Frame(card_inner, bg=bg)
            row1.pack(fill="x")

            name_text = r.name
            if r.is_current:
                name_text = "\u2192 " + name_text + "  [Aktuell]"
            tk.Label(row1, text=name_text, font=("", 12, "bold"),
                     fg="#ff9800" if r.is_current else tc["fg"], bg=bg,
                     anchor="w").pack(side="left")
            tk.Label(row1, text=f"{r.annual_cost_eur:.0f} \u20ac/Jahr", font=("", 12, "bold"),
                     fg=tc["fg"], bg=bg, anchor="e").pack(side="right")

            # Row 2: provider, type, monthly, ct/kWh, savings
            row2 = tk.Frame(card_inner, bg=bg)
            row2.pack(fill="x", pady=(2, 0))

            detail = f"{r.provider} \u00b7 {r.tariff_type.upper()} \u00b7 {r.monthly_avg_eur:.0f} \u20ac/Mon \u00b7 {r.effective_price_ct:.1f} ct/kWh"
            tk.Label(row2, text=detail, font=("", 10),
                     fg=tc.get("muted", "#888888"), bg=bg, anchor="w").pack(side="left")

            if not r.is_current:
                if r.savings_vs_current_eur > 0:
                    sav_text = f"\u25bc {r.savings_vs_current_eur:.0f} \u20ac"
                    sav_color = tc.get("green", "#27ae60")
                else:
                    sav_text = f"\u25b2 {abs(r.savings_vs_current_eur):.0f} \u20ac"
                    sav_color = tc.get("red", "#e74c3c")
                tk.Label(row2, text=sav_text, font=("", 10, "bold"),
                         fg=sav_color, bg=bg, anchor="e").pack(side="right")

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

            tc = self._get_theme_colors()
            fig = Figure(figsize=(8, 3), dpi=100)
            ax = fig.add_subplot(111)
            names = [r.name[:20] for r in results]
            costs = [r.annual_cost_eur for r in results]
            colors = ["#ff9800" if r.is_current else tc["blue"] for r in results]

            bars = ax.barh(names, costs, color=colors)
            ax.set_xlabel("\u20ac/Jahr")
            ax.invert_yaxis()
            fig.tight_layout()
            self._apply_plot_theme(fig, ax)

            canvas = FigureCanvasTkAgg(fig, self._tariff_chart_frame)
            canvas.draw()
            canvas.get_tk_widget().pack(fill="both", expand=True)
        except Exception as e:
            _log.debug("Tariff chart: %s", e)
