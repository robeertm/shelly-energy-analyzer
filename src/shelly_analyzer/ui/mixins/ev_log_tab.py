from __future__ import annotations
import logging
import threading
import tkinter as tk
from tkinter import ttk

_log = logging.getLogger(__name__)


class EvLogMixin:
    """EV charging log tab: detect and display charging sessions."""

    def _build_ev_log_tab(self) -> None:
        frm = self.tab_ev_log
        for w in frm.winfo_children():
            w.destroy()

        canvas = tk.Canvas(frm, highlightthickness=0)
        scrollbar = ttk.Scrollbar(frm, orient="vertical", command=canvas.yview)
        inner = ttk.Frame(canvas)
        inner.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=inner, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)

        ttk.Label(inner, text=self.t("ev_log.title"), font=("", 14, "bold")).pack(anchor="w", padx=12, pady=(12, 4))
        ttk.Label(inner, text=self.t("ev_log.hint"), foreground="gray").pack(anchor="w", padx=12, pady=(0, 8))

        # Summary cards
        summary_frm = ttk.Frame(inner)
        summary_frm.pack(fill="x", padx=12, pady=4)
        self._ev_total_sessions = tk.StringVar(value="0")
        self._ev_total_kwh = tk.StringVar(value="0.0 kWh")
        self._ev_total_cost = tk.StringVar(value="0.00 \u20ac")

        for i, (label, var) in enumerate([
            (self.t("ev_log.sessions"), self._ev_total_sessions),
            (self.t("ev_log.total_kwh"), self._ev_total_kwh),
            (self.t("ev_log.total_cost"), self._ev_total_cost),
        ]):
            card = ttk.LabelFrame(summary_frm, text=label)
            card.grid(row=0, column=i, padx=6, pady=4, sticky="nsew")
            summary_frm.columnconfigure(i, weight=1)
            ttk.Label(card, textvariable=var, font=("", 16, "bold")).pack(padx=12, pady=8)

        # Sessions table
        tree_frm = ttk.Frame(inner)
        tree_frm.pack(fill="both", expand=True, padx=12, pady=8)
        cols = ("date", "start", "end", "duration", "kwh", "peak_w", "cost")
        self._ev_tree = ttk.Treeview(tree_frm, columns=cols, show="headings", height=12)
        for col, hdr, w in [
            ("date", self.t("ev_log.col_date"), 100),
            ("start", self.t("ev_log.col_start"), 70),
            ("end", self.t("ev_log.col_end"), 70),
            ("duration", self.t("ev_log.col_duration"), 80),
            ("kwh", "kWh", 70),
            ("peak_w", self.t("ev_log.col_peak"), 80),
            ("cost", self.t("ev_log.col_cost"), 70),
        ]:
            self._ev_tree.heading(col, text=hdr)
            self._ev_tree.column(col, width=w, anchor="center")
        self._ev_tree.pack(fill="both", expand=True)

        ttk.Button(inner, text=self.t("ev_log.detect_btn"), command=self._ev_detect_sessions).pack(padx=12, pady=8)

        # Auto-detect on tab open
        self._ev_detect_sessions()

    def _ev_detect_sessions(self) -> None:
        def _worker():
            try:
                from shelly_analyzer.services.ev_charging_log import detect_charging_sessions, get_monthly_summary
                dev_key = self.cfg.ev_charging.wallbox_device_key
                if not dev_key:
                    return
                df = self.storage.read_device_df(dev_key)
                if df is None or df.empty:
                    return
                sessions = detect_charging_sessions(
                    df, dev_key,
                    threshold_w=self.cfg.ev_charging.detection_threshold_w,
                    min_duration_s=self.cfg.ev_charging.min_session_minutes * 60,
                    price_eur_per_kwh=self.cfg.pricing.electricity_price_eur_per_kwh,
                )
                summary = get_monthly_summary(sessions)

                def _update():
                    self._ev_total_sessions.set(str(summary.total_sessions))
                    self._ev_total_kwh.set(f"{summary.total_kwh:.1f} kWh")
                    self._ev_total_cost.set(f"{summary.total_cost:.2f} \u20ac")
                    self._ev_tree.delete(*self._ev_tree.get_children())
                    from datetime import datetime
                    for s in sessions[-50:]:  # Last 50
                        dt = datetime.fromtimestamp(s.start_ts)
                        dur_min = (s.end_ts - s.start_ts) / 60
                        self._ev_tree.insert("", "end", values=(
                            dt.strftime("%Y-%m-%d"),
                            dt.strftime("%H:%M"),
                            datetime.fromtimestamp(s.end_ts).strftime("%H:%M"),
                            f"{dur_min:.0f} min",
                            f"{s.energy_kwh:.2f}",
                            f"{s.peak_power_w:.0f}",
                            f"{s.cost_eur:.2f} \u20ac",
                        ))
                self.after(0, _update)
            except Exception as e:
                _log.error("EV session detection: %s", e)
        threading.Thread(target=_worker, daemon=True).start()
