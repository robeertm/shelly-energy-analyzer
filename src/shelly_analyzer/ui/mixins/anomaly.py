"""Anomaly detection tab mixin for Shelly Energy Analyzer.

Provides the Anomalies tab with:
- History log (Treeview) showing detected anomaly events
- Settings panel (sigma threshold, window, check toggles, notification channels)
- "Run Detection Now" button
- Notifications via Telegram / Webhook / E-mail
"""
from __future__ import annotations

import logging
import threading
from dataclasses import replace
from datetime import datetime
from typing import List, Optional

import tkinter as tk
from tkinter import ttk

from shelly_analyzer.io.config import AnomalyConfig, save_config
from shelly_analyzer.services.anomaly import AnomalyEvent, detect_anomalies
from shelly_analyzer.services.compute import load_device

logger = logging.getLogger(__name__)


class AnomalyMixin:
    """Anomaly-detection tab and background detection runner."""

    # ── Build ─────────────────────────────────────────────────────────────────

    def _build_anomaly_tab(self) -> None:
        frm = self.tab_anomaly

        # ── Top bar ───────────────────────────────────────────────────────────
        top = ttk.Frame(frm)
        top.pack(fill="x", padx=14, pady=(12, 4))
        ttk.Label(top, text=self.t("anomaly.title"), font=("", 14, "bold")).pack(side="left")
        ttk.Button(top, text=self.t("anomaly.refresh"), command=self._run_anomaly_detection).pack(side="right")
        ttk.Button(top, text=self.t("anomaly.clear"), command=self._anomaly_clear_history).pack(side="right", padx=(0, 6))

        # ── Settings panel ────────────────────────────────────────────────────
        cfg_frame = ttk.LabelFrame(frm, text=self.t("settings.anomaly.title"))
        cfg_frame.pack(fill="x", padx=14, pady=(0, 6))

        # Row 1: enabled + sigma
        r1 = ttk.Frame(cfg_frame)
        r1.pack(fill="x", padx=8, pady=(6, 2))

        self._anom_enabled_var = tk.BooleanVar(value=bool(getattr(self.cfg.anomaly, "enabled", False)))
        ttk.Checkbutton(
            r1, text=self.t("settings.anomaly.enabled"),
            variable=self._anom_enabled_var, command=self._anomaly_save_cfg
        ).pack(side="left", padx=(0, 20))

        ttk.Label(r1, text=self.t("settings.anomaly.sigma")).pack(side="left", padx=(0, 4))
        self._anom_sigma_var = tk.StringVar(value=str(getattr(self.cfg.anomaly, "sigma_threshold", 2.0)))
        sigma_entry = ttk.Entry(r1, textvariable=self._anom_sigma_var, width=6)
        sigma_entry.pack(side="left")
        sigma_entry.bind("<FocusOut>", lambda _e: self._anomaly_save_cfg())
        ttk.Label(r1, text=self.t("settings.anomaly.sigma.hint"), foreground="gray").pack(side="left", padx=(4, 16))

        ttk.Label(r1, text=self.t("settings.anomaly.min_dev")).pack(side="left", padx=(0, 4))
        self._anom_mindev_var = tk.StringVar(value=str(getattr(self.cfg.anomaly, "min_deviation_kwh", 0.1)))
        mindev_entry = ttk.Entry(r1, textvariable=self._anom_mindev_var, width=6)
        mindev_entry.pack(side="left")
        mindev_entry.bind("<FocusOut>", lambda _e: self._anomaly_save_cfg())

        ttk.Label(r1, text=self.t("settings.anomaly.window")).pack(side="left", padx=(16, 4))
        self._anom_window_var = tk.StringVar(value=str(getattr(self.cfg.anomaly, "window_days", 30)))
        win_entry = ttk.Entry(r1, textvariable=self._anom_window_var, width=5)
        win_entry.pack(side="left")
        win_entry.bind("<FocusOut>", lambda _e: self._anomaly_save_cfg())

        ttk.Label(r1, text=self.t("settings.anomaly.interval")).pack(side="left", padx=(16, 4))
        self._anom_interval_var = tk.StringVar(value=str(getattr(self.cfg.anomaly, "auto_interval_minutes", 15)))
        interval_entry = ttk.Entry(r1, textvariable=self._anom_interval_var, width=5)
        interval_entry.pack(side="left")
        interval_entry.bind("<FocusOut>", lambda _e: self._anomaly_save_cfg())
        ttk.Label(r1, text="min", foreground="gray").pack(side="left", padx=(2, 0))

        # Row 2: check toggles
        r2 = ttk.Frame(cfg_frame)
        r2.pack(fill="x", padx=8, pady=(2, 2))
        ttk.Label(r2, text=self.t("settings.anomaly.checks")).pack(side="left", padx=(0, 8))

        self._anom_check_daily_var = tk.BooleanVar(
            value=bool(getattr(self.cfg.anomaly, "check_unusual_daily", True))
        )
        ttk.Checkbutton(
            r2, text=self.t("settings.anomaly.check_daily"),
            variable=self._anom_check_daily_var, command=self._anomaly_save_cfg
        ).pack(side="left", padx=(0, 12))

        self._anom_check_night_var = tk.BooleanVar(
            value=bool(getattr(self.cfg.anomaly, "check_night_consumption", True))
        )
        ttk.Checkbutton(
            r2, text=self.t("settings.anomaly.check_night"),
            variable=self._anom_check_night_var, command=self._anomaly_save_cfg
        ).pack(side="left", padx=(0, 12))

        self._anom_check_peak_var = tk.BooleanVar(
            value=bool(getattr(self.cfg.anomaly, "check_power_peak_time", True))
        )
        ttk.Checkbutton(
            r2, text=self.t("settings.anomaly.check_peak"),
            variable=self._anom_check_peak_var, command=self._anomaly_save_cfg
        ).pack(side="left")

        # Row 3: notification channels
        r3 = ttk.Frame(cfg_frame)
        r3.pack(fill="x", padx=8, pady=(2, 8))
        ttk.Label(r3, text=self.t("settings.anomaly.notify")).pack(side="left", padx=(0, 8))

        self._anom_notify_tg_var = tk.BooleanVar(
            value=bool(getattr(self.cfg.anomaly, "action_telegram", False))
        )
        ttk.Checkbutton(
            r3, text=self.t("settings.anomaly.notify_telegram"),
            variable=self._anom_notify_tg_var, command=self._anomaly_save_cfg
        ).pack(side="left", padx=(0, 12))

        self._anom_notify_wh_var = tk.BooleanVar(
            value=bool(getattr(self.cfg.anomaly, "action_webhook", False))
        )
        ttk.Checkbutton(
            r3, text=self.t("settings.anomaly.notify_webhook"),
            variable=self._anom_notify_wh_var, command=self._anomaly_save_cfg
        ).pack(side="left", padx=(0, 12))

        self._anom_notify_email_var = tk.BooleanVar(
            value=bool(getattr(self.cfg.anomaly, "action_email", False))
        )
        ttk.Checkbutton(
            r3, text=self.t("settings.anomaly.notify_email"),
            variable=self._anom_notify_email_var, command=self._anomaly_save_cfg
        ).pack(side="left")

        # ── Status label ──────────────────────────────────────────────────────
        self._anomaly_status_var = tk.StringVar(value="")
        ttk.Label(frm, textvariable=self._anomaly_status_var, foreground="gray").pack(
            anchor="w", padx=14, pady=(0, 4)
        )

        # ── History treeview ──────────────────────────────────────────────────
        tree_frame = ttk.Frame(frm)
        tree_frame.pack(fill="both", expand=True, padx=14, pady=(0, 10))

        cols = ("time", "device", "type", "value", "sigma", "description")
        self._anomaly_tree = ttk.Treeview(tree_frame, columns=cols, show="headings", height=18)
        col_cfg = [
            ("time",        self.t("anomaly.col.time"),        150, False),
            ("device",      self.t("anomaly.col.device"),      140, False),
            ("type",        self.t("anomaly.col.type"),        180, False),
            ("value",       self.t("anomaly.col.value"),        80, False),
            ("sigma",       self.t("anomaly.col.sigma"),        50, False),
            ("description", self.t("anomaly.col.description"), 400, True),
        ]
        for cid, label, width, stretch in col_cfg:
            self._anomaly_tree.heading(cid, text=label)
            self._anomaly_tree.column(cid, width=width, stretch=stretch, anchor="w")

        sb_y = ttk.Scrollbar(tree_frame, orient="vertical", command=self._anomaly_tree.yview)
        sb_x = ttk.Scrollbar(tree_frame, orient="horizontal", command=self._anomaly_tree.xview)
        self._anomaly_tree.configure(yscrollcommand=sb_y.set, xscrollcommand=sb_x.set)

        sb_y.pack(side="right", fill="y")
        sb_x.pack(side="bottom", fill="x")
        self._anomaly_tree.pack(fill="both", expand=True)

        # Populate from existing in-memory log
        self._anomaly_refresh_tree()

        # Start the auto-detection timer
        self._anomaly_auto_id: Optional[str] = None
        if bool(getattr(self.cfg.anomaly, "enabled", False)):
            self.after(500, self._run_anomaly_detection)
            self._anomaly_schedule_auto()

    # ── Auto-detection timer ─────────────────────────────────────────────────

    def _anomaly_schedule_auto(self) -> None:
        """Schedule the next automatic detection run."""
        self._anomaly_cancel_auto()
        if not bool(getattr(self.cfg.anomaly, "enabled", False)):
            return
        interval = int(getattr(self.cfg.anomaly, "auto_interval_minutes", 15))
        if interval < 1:
            return
        ms = interval * 60 * 1000
        self._anomaly_auto_id = self.after(ms, self._anomaly_auto_tick)

    def _anomaly_cancel_auto(self) -> None:
        """Cancel a pending auto-detection timer."""
        aid = getattr(self, "_anomaly_auto_id", None)
        if aid is not None:
            try:
                self.after_cancel(aid)
            except Exception:
                pass
            self._anomaly_auto_id = None

    def _anomaly_auto_tick(self) -> None:
        """Periodic auto-detection callback."""
        self._anomaly_auto_id = None
        if bool(getattr(self.cfg.anomaly, "enabled", False)):
            self._run_anomaly_detection()
        # Re-schedule for next interval
        self._anomaly_schedule_auto()

    # ── Detection runner ──────────────────────────────────────────────────────

    def _run_anomaly_detection(self) -> None:
        """Start anomaly detection in a background thread."""
        if not bool(getattr(self.cfg.anomaly, "enabled", False)):
            try:
                self._anomaly_status_var.set(self.t("anomaly.not_enabled"))
            except Exception:
                pass
            return

        devices = list(getattr(self.cfg, "devices", []) or [])
        if not devices:
            try:
                self._anomaly_status_var.set(self.t("anomaly.no_data"))
            except Exception:
                pass
            return

        try:
            self._anomaly_status_var.set(self.t("anomaly.running"))
        except Exception:
            pass

        def _worker() -> None:
            new_events: List[AnomalyEvent] = []
            anm = self.cfg.anomaly
            sigma = float(getattr(anm, "sigma_threshold", 2.0))
            min_dev = float(getattr(anm, "min_deviation_kwh", 0.1))
            window = int(getattr(anm, "window_days", 30))
            check_daily = bool(getattr(anm, "check_unusual_daily", True))
            check_night = bool(getattr(anm, "check_night_consumption", True))
            check_peak = bool(getattr(anm, "check_power_peak_time", True))

            for dev in devices:
                try:
                    cd = load_device(self.storage, dev)
                    events = detect_anomalies(
                        df=cd.df,
                        device_key=dev.key,
                        device_name=dev.name,
                        sigma=sigma,
                        min_deviation_kwh=min_dev,
                        window_days=window,
                        check_unusual_daily=check_daily,
                        check_night_consumption=check_night,
                        check_power_peak_time=check_peak,
                    )
                    new_events.extend(events)
                except Exception:
                    logger.exception("Anomaly detection failed for device %s", dev.key)

            self.after(0, lambda: self._anomaly_on_results(new_events))

        threading.Thread(target=_worker, daemon=True).start()

    def _anomaly_on_results(self, new_events: List[AnomalyEvent]) -> None:
        """Called on the main thread after detection finishes."""
        if not hasattr(self, "_anomaly_log"):
            self._anomaly_log: List[AnomalyEvent] = []

        # Deduplicate by event_id
        existing_ids = {e.event_id for e in self._anomaly_log}
        fresh = [e for e in new_events if e.event_id not in existing_ids]
        self._anomaly_log = (fresh + self._anomaly_log)[: int(getattr(self.cfg.anomaly, "max_history", 200))]

        # Send notifications for fresh events
        for evt in fresh:
            try:
                self._anomaly_notify(evt)
            except Exception:
                logger.exception("Anomaly notification failed for %s", evt.event_id)

        try:
            self._anomaly_status_var.set(self.t("anomaly.done").format(n=len(new_events)))
        except Exception:
            pass

        self._anomaly_refresh_tree()

    def _anomaly_refresh_tree(self) -> None:
        """Repopulate the treeview from _anomaly_log."""
        tree = getattr(self, "_anomaly_tree", None)
        if tree is None:
            return

        for row in tree.get_children():
            tree.delete(row)

        log = getattr(self, "_anomaly_log", [])
        if not log:
            tree.insert("", "end", values=(
                "", "", self.t("anomaly.no_anomalies"), "", "", ""
            ))
            return

        type_labels = {
            "unusual_daily": self.t("anomaly.type.unusual_daily"),
            "night_consumption": self.t("anomaly.type.night_consumption"),
            "power_peak_time": self.t("anomaly.type.power_peak_time"),
        }

        for evt in log:
            ts = evt.timestamp
            ts_str = ts.strftime("%Y-%m-%d %H:%M") if isinstance(ts, datetime) else str(ts)
            type_label = type_labels.get(evt.anomaly_type, evt.anomaly_type)
            # Format value depending on type
            if evt.anomaly_type in ("unusual_daily", "night_consumption"):
                val_str = f"{evt.value:.2f} kWh"
            else:
                val_str = f"{evt.value:.0f} W"
            tree.insert("", "end", values=(
                ts_str,
                evt.device_name,
                type_label,
                val_str,
                f"{evt.sigma_count:.1f}",
                evt.description,
            ))

    def _anomaly_clear_history(self) -> None:
        self._anomaly_log = []
        self._anomaly_refresh_tree()
        try:
            self._anomaly_status_var.set("")
        except Exception:
            pass

    # ── Settings persistence ──────────────────────────────────────────────────

    def _anomaly_save_cfg(self) -> None:
        """Read UI widgets and persist anomaly config."""
        try:
            sigma = float(self._anom_sigma_var.get())
        except ValueError:
            sigma = 2.0
        try:
            min_dev = float(self._anom_mindev_var.get())
        except ValueError:
            min_dev = 0.1
        try:
            window = int(self._anom_window_var.get())
        except ValueError:
            window = 30
        try:
            interval = int(self._anom_interval_var.get())
        except ValueError:
            interval = 15

        new_anm = replace(
            self.cfg.anomaly,
            enabled=bool(self._anom_enabled_var.get()),
            sigma_threshold=max(0.5, sigma),
            min_deviation_kwh=max(0.0, min_dev),
            window_days=max(7, window),
            check_unusual_daily=bool(self._anom_check_daily_var.get()),
            check_night_consumption=bool(self._anom_check_night_var.get()),
            check_power_peak_time=bool(self._anom_check_peak_var.get()),
            action_telegram=bool(self._anom_notify_tg_var.get()),
            action_webhook=bool(self._anom_notify_wh_var.get()),
            action_email=bool(self._anom_notify_email_var.get()),
            auto_interval_minutes=max(1, interval),
        )
        self.cfg = replace(self.cfg, anomaly=new_anm)
        try:
            save_config(self.cfg, self.cfg_path)
        except Exception:
            logger.exception("Failed to save anomaly config")

        # Restart auto-detection timer with new settings
        self._anomaly_schedule_auto()

    # ── Notification dispatch ─────────────────────────────────────────────────

    def _anomaly_notify(self, evt: AnomalyEvent) -> None:
        """Send notifications for a newly detected anomaly."""
        anm = self.cfg.anomaly
        lang = str(getattr(self.cfg.ui, "language", "de") or "de")

        type_labels = {
            "unusual_daily": self.t("anomaly.type.unusual_daily"),
            "night_consumption": self.t("anomaly.type.night_consumption"),
            "power_peak_time": self.t("anomaly.type.power_peak_time"),
        }
        type_label = type_labels.get(evt.anomaly_type, evt.anomaly_type)

        if evt.anomaly_type in ("unusual_daily", "night_consumption"):
            val_str = f"{evt.value:.2f} kWh"
        else:
            val_str = f"{evt.value:.0f} W"

        subject = self.t("anomaly.notify.subject").format(device=evt.device_name)
        body = self.t("anomaly.notify.body").format(
            device=evt.device_name,
            type=type_label,
            value=val_str,
            sigma=evt.sigma_count,
            description=evt.description,
        )
        ts_str = evt.timestamp.strftime("%Y-%m-%d %H:%M") if isinstance(evt.timestamp, datetime) else ""
        full_msg = f"[{ts_str}] {subject}\n{body}"

        if bool(getattr(anm, "action_telegram", False)):
            try:
                if bool(getattr(self.cfg.ui, "telegram_enabled", False)):
                    self._alerts_send_telegram(full_msg)
            except Exception:
                logger.exception("Anomaly Telegram notify failed")

        if bool(getattr(anm, "action_webhook", False)):
            try:
                if bool(getattr(self.cfg.ui, "webhook_enabled", False)):
                    payload = {
                        "event": "anomaly",
                        "device": evt.device_key,
                        "device_name": evt.device_name,
                        "anomaly_type": evt.anomaly_type,
                        "value": evt.value,
                        "sigma": round(evt.sigma_count, 2),
                        "description": evt.description,
                        "timestamp": ts_str,
                    }
                    threading.Thread(
                        target=self._webhook_send_sync,
                        args=(payload,),
                        daemon=True,
                    ).start()
            except Exception:
                logger.exception("Anomaly Webhook notify failed")

        if bool(getattr(anm, "action_email", False)):
            try:
                if bool(getattr(self.cfg.ui, "email_enabled", False)):
                    threading.Thread(
                        target=self._email_send_sync,
                        kwargs={"subject": subject, "body": body},
                        daemon=True,
                    ).start()
            except Exception:
                logger.exception("Anomaly E-mail notify failed")
