from __future__ import annotations
import logging
import threading
import tkinter as tk
from tkinter import ttk

_log = logging.getLogger(__name__)


class SmartScheduleMixin:
    """Smart scheduling tab: find cheapest spot-price time blocks."""

    def _build_smart_schedule_tab(self) -> None:
        frm = self.tab_smart_sched
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

        # Title
        ttk.Label(inner, text=self.t("smart_sched.title"), font=("", 14, "bold")).pack(anchor="w", padx=12, pady=(12, 4))
        ttk.Label(inner, text=self.t("smart_sched.hint"), foreground="gray").pack(anchor="w", padx=12, pady=(0, 12))

        # Controls
        ctrl = ttk.Frame(inner)
        ctrl.pack(fill="x", padx=12, pady=4)

        ttk.Label(ctrl, text=self.t("smart_sched.duration")).grid(row=0, column=0, padx=4, pady=4, sticky="w")
        self._ss_duration_var = tk.DoubleVar(value=getattr(self.cfg.smart_schedule, 'default_duration_hours', 3.0))
        ttk.Spinbox(ctrl, from_=0.5, to=12.0, increment=0.5, textvariable=self._ss_duration_var, width=6).grid(row=0, column=1, padx=4)
        ttk.Label(ctrl, text=self.t("common.hours")).grid(row=0, column=2, padx=4)

        ttk.Button(ctrl, text=self.t("smart_sched.find_btn"), command=self._ss_find_cheapest).grid(row=0, column=3, padx=12)

        # Result area
        self._ss_result_frame = ttk.LabelFrame(inner, text=self.t("smart_sched.result"))
        self._ss_result_frame.pack(fill="x", padx=12, pady=8)
        self._ss_result_label = ttk.Label(self._ss_result_frame, text=self.t("smart_sched.no_data"), wraplength=600)
        self._ss_result_label.pack(padx=12, pady=12)

        # Chart area
        self._ss_chart_frame = ttk.LabelFrame(inner, text=self.t("smart_sched.chart_title"))
        self._ss_chart_frame.pack(fill="both", expand=True, padx=12, pady=8)

    def _ss_find_cheapest(self) -> None:
        def _worker():
            try:
                from shelly_analyzer.services.smart_schedule import get_schedule_recommendations
                duration = self._ss_duration_var.get()
                zone = getattr(self.cfg.spot_price, 'bidding_zone', 'DE-LU')
                rec = get_schedule_recommendations(self.storage.db, zone, duration)
                if rec:
                    from datetime import datetime
                    start = datetime.fromtimestamp(rec.start_ts).strftime("%H:%M")
                    end = datetime.fromtimestamp(rec.end_ts).strftime("%H:%M")
                    text = (
                        f"{self.t('smart_sched.cheapest_block')}: {start} – {end}\n"
                        f"{self.t('smart_sched.avg_price')}: {rec.avg_price_ct:.2f} ct/kWh\n"
                        f"{self.t('smart_sched.savings')}: {rec.savings_vs_avg_ct:.2f} ct/kWh"
                    )
                else:
                    text = self.t("smart_sched.no_data")
                self.after(0, lambda: self._ss_result_label.configure(text=text))
            except Exception as e:
                self.after(0, lambda: self._ss_result_label.configure(text=f"Error: {e}"))
        threading.Thread(target=_worker, daemon=True).start()
