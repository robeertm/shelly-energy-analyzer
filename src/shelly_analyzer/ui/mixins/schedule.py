"""Device scheduling tab mixin for Shelly Energy Analyzer.

Provides the "Schedules" tab with:
- Treeview listing all device schedules (Name, Device, On, Off, Days, Enabled, Backend)
- Add / Edit / Delete buttons
- Push-to-Shelly button for Gen2+ devices
- Load-from-Shelly button to import schedules stored on the device
- Local background scheduler (LocalScheduler) that fires on/off commands
  for Gen1 / non-pushed schedules
"""
from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import replace
from typing import Dict, List, Optional

import tkinter as tk
from tkinter import messagebox, ttk

from shelly_analyzer.io.config import AppConfig, DeviceSchedule, save_config
from shelly_analyzer.io.http import (
    ShellyHttp,
    schedule_create,
    schedule_delete,
    schedule_list,
)
from shelly_analyzer.services.scheduler import LocalScheduler, build_shelly_timespec

logger = logging.getLogger(__name__)

# Weekday short names (index 0=Mon … 6=Sun), overridden per lang via t()
_DAY_KEYS = [
    "sched.day.mon",
    "sched.day.tue",
    "sched.day.wed",
    "sched.day.thu",
    "sched.day.fri",
    "sched.day.sat",
    "sched.day.sun",
]


class ScheduleMixin:
    """Device schedule tab + background LocalScheduler."""

    # ── Lifecycle ────────────────────────────────────────────────────────────

    def _schedule_init(self) -> None:
        """Call once after config is loaded to start the background scheduler."""
        self._local_scheduler = LocalScheduler(
            get_config=lambda: self.cfg,
            get_http=lambda: getattr(self, "_http", ShellyHttp()),
        )
        self._local_scheduler.start()
        # Start CO₂ fetch service if the mixin is present
        try:
            self._co2_service_init()
        except AttributeError:
            pass

    def _schedule_stop(self) -> None:
        try:
            self._local_scheduler.stop()
        except Exception:
            pass

    # ── Tab builder ──────────────────────────────────────────────────────────

    def _build_schedule_tab(self) -> None:
        frm = self.tab_schedule

        # ── Top bar ───────────────────────────────────────────────────────────
        top = ttk.Frame(frm)
        top.pack(fill="x", padx=14, pady=(12, 4))

        ttk.Label(top, text=self.t("sched.title"), font=("", 14, "bold")).pack(side="left")
        ttk.Label(top, text=self.t("sched.hint"), wraplength=700, justify="left").pack(
            side="left", padx=(16, 0)
        )

        # ── Toolbar ───────────────────────────────────────────────────────────
        toolbar = ttk.Frame(frm)
        toolbar.pack(fill="x", padx=14, pady=(0, 4))

        ttk.Button(toolbar, text=self.t("sched.btn.add"), command=self._sched_add).pack(
            side="left", padx=(0, 4)
        )
        ttk.Button(toolbar, text=self.t("sched.btn.edit"), command=self._sched_edit).pack(
            side="left", padx=(0, 4)
        )
        ttk.Button(toolbar, text=self.t("sched.btn.delete"), command=self._sched_delete).pack(
            side="left", padx=(0, 12)
        )
        ttk.Button(
            toolbar, text=self.t("sched.btn.push"), command=self._sched_push_to_device
        ).pack(side="left", padx=(0, 4))
        ttk.Button(
            toolbar, text=self.t("sched.btn.load"), command=self._sched_load_from_device
        ).pack(side="left", padx=(0, 4))

        self._sched_status_var = tk.StringVar(value="")
        ttk.Label(toolbar, textvariable=self._sched_status_var, foreground="#555").pack(
            side="left", padx=(16, 0)
        )

        # ── Treeview ──────────────────────────────────────────────────────────
        tv_frame = ttk.Frame(frm)
        tv_frame.pack(fill="both", expand=True, padx=14, pady=(0, 12))

        cols = ("name", "device", "time_on", "time_off", "weekdays", "enabled", "backend")
        self._sched_tv = ttk.Treeview(tv_frame, columns=cols, show="headings", selectmode="browse")
        vsb = ttk.Scrollbar(tv_frame, orient="vertical", command=self._sched_tv.yview)
        self._sched_tv.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        self._sched_tv.pack(side="left", fill="both", expand=True)

        self._sched_tv.heading("name", text=self.t("sched.col.name"))
        self._sched_tv.heading("device", text=self.t("sched.col.device"))
        self._sched_tv.heading("time_on", text=self.t("sched.col.time_on"))
        self._sched_tv.heading("time_off", text=self.t("sched.col.time_off"))
        self._sched_tv.heading("weekdays", text=self.t("sched.col.weekdays"))
        self._sched_tv.heading("enabled", text=self.t("sched.col.enabled"))
        self._sched_tv.heading("backend", text=self.t("sched.col.backend"))

        self._sched_tv.column("name", width=160, minwidth=100)
        self._sched_tv.column("device", width=140, minwidth=80)
        self._sched_tv.column("time_on", width=70, minwidth=60, anchor="center")
        self._sched_tv.column("time_off", width=70, minwidth=60, anchor="center")
        self._sched_tv.column("weekdays", width=200, minwidth=120)
        self._sched_tv.column("enabled", width=70, minwidth=60, anchor="center")
        self._sched_tv.column("backend", width=110, minwidth=80, anchor="center")

        self._sched_tv.bind("<Double-1>", lambda _e: self._sched_edit())

        self._sched_refresh_tree()

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _sched_refresh_tree(self) -> None:
        tv = getattr(self, "_sched_tv", None)
        if tv is None:
            return
        for row in tv.get_children():
            tv.delete(row)
        schedules: List[DeviceSchedule] = list(getattr(self.cfg, "schedules", []) or [])
        dev_names: Dict[str, str] = {
            d.key: d.name for d in getattr(self.cfg, "devices", [])
        }
        day_names = [self.t(k) for k in _DAY_KEYS]
        for s in schedules:
            dev_label = dev_names.get(s.device_key, s.device_key)
            days_str = self._format_weekdays(s.weekdays, day_names)
            enabled_str = "✓" if s.enabled else "✗"
            if s.shelly_id_on >= 0 or s.shelly_id_off >= 0:
                backend_str = self.t("sched.backend.shelly")
            else:
                backend_str = self.t("sched.backend.local")
            tv.insert(
                "",
                "end",
                iid=s.schedule_id,
                values=(s.name, dev_label, s.time_on, s.time_off, days_str, enabled_str, backend_str),
            )

    def _format_weekdays(self, weekdays: List[int], day_names: List[str]) -> str:
        if len(weekdays) == 7:
            return self.t("sched.days.every")
        if weekdays == [0, 1, 2, 3, 4]:
            return self.t("sched.days.workdays")
        if weekdays == [5, 6]:
            return self.t("sched.days.weekend")
        return ", ".join(day_names[d] for d in sorted(weekdays) if 0 <= d <= 6)

    def _get_selected_schedule(self) -> Optional[DeviceSchedule]:
        tv = getattr(self, "_sched_tv", None)
        if tv is None:
            return None
        sel = tv.selection()
        if not sel:
            return None
        sid = sel[0]
        for s in getattr(self.cfg, "schedules", []):
            if s.schedule_id == sid:
                return s
        return None

    def _save_schedules(self, schedules: List[DeviceSchedule]) -> None:
        self.cfg = replace(self.cfg, schedules=schedules)
        try:
            save_config(self.cfg, self.cfg_path)
        except Exception as exc:
            logger.warning("Failed to save schedules: %s", exc)
        self._sched_refresh_tree()

    # ── CRUD actions ──────────────────────────────────────────────────────────

    def _sched_add(self) -> None:
        self._sched_open_dialog(existing=None)

    def _sched_edit(self) -> None:
        sel = self._get_selected_schedule()
        if sel is None:
            messagebox.showinfo(self.t("sched.title"), self.t("sched.msg.select"))
            return
        self._sched_open_dialog(existing=sel)

    def _sched_delete(self) -> None:
        sel = self._get_selected_schedule()
        if sel is None:
            messagebox.showinfo(self.t("sched.title"), self.t("sched.msg.select"))
            return
        if not messagebox.askyesno(
            self.t("sched.title"),
            self.t("sched.msg.confirm_delete", name=sel.name),
        ):
            return
        # Try to delete from device if pushed
        if sel.shelly_id_on >= 0 or sel.shelly_id_off >= 0:
            self._sched_delete_from_device_bg(sel)
        # Remove from config
        schedules = [s for s in getattr(self.cfg, "schedules", []) if s.schedule_id != sel.schedule_id]
        self._save_schedules(schedules)

    # ── Dialog ────────────────────────────────────────────────────────────────

    def _sched_open_dialog(self, existing: Optional[DeviceSchedule]) -> None:
        dlg = tk.Toplevel(self)
        dlg.title(self.t("sched.dlg.title_edit") if existing else self.t("sched.dlg.title_add"))
        dlg.resizable(False, False)
        dlg.grab_set()

        pad = {"padx": 8, "pady": 4}

        # Name
        ttk.Label(dlg, text=self.t("sched.dlg.name")).grid(row=0, column=0, sticky="w", **pad)
        name_var = tk.StringVar(value=existing.name if existing else "")
        ttk.Entry(dlg, textvariable=name_var, width=28).grid(row=0, column=1, columnspan=3, sticky="ew", **pad)

        # Device
        ttk.Label(dlg, text=self.t("sched.dlg.device")).grid(row=1, column=0, sticky="w", **pad)
        devices = list(getattr(self.cfg, "devices", []))
        dev_names = [f"{d.name} ({d.key})" for d in devices]
        dev_var = tk.StringVar()
        dev_box = ttk.Combobox(dlg, textvariable=dev_var, values=dev_names, state="readonly", width=26)
        dev_box.grid(row=1, column=1, columnspan=3, sticky="ew", **pad)
        if existing:
            for i, d in enumerate(devices):
                if d.key == existing.device_key:
                    dev_box.current(i)
                    break
        elif devices:
            dev_box.current(0)

        # Switch ID
        ttk.Label(dlg, text=self.t("sched.dlg.switch_id")).grid(row=2, column=0, sticky="w", **pad)
        switch_id_var = tk.StringVar(value=str(existing.switch_id) if existing else "0")
        ttk.Entry(dlg, textvariable=switch_id_var, width=6).grid(row=2, column=1, sticky="w", **pad)

        # On-time
        ttk.Label(dlg, text=self.t("sched.dlg.time_on")).grid(row=3, column=0, sticky="w", **pad)
        time_on_var = tk.StringVar(value=existing.time_on if existing else "06:00")
        ttk.Entry(dlg, textvariable=time_on_var, width=8).grid(row=3, column=1, sticky="w", **pad)

        # Off-time
        ttk.Label(dlg, text=self.t("sched.dlg.time_off")).grid(row=4, column=0, sticky="w", **pad)
        time_off_var = tk.StringVar(value=existing.time_off if existing else "07:00")
        ttk.Entry(dlg, textvariable=time_off_var, width=8).grid(row=4, column=1, sticky="w", **pad)

        # Weekdays
        ttk.Label(dlg, text=self.t("sched.dlg.weekdays")).grid(row=5, column=0, sticky="nw", **pad)
        wd_frame = ttk.Frame(dlg)
        wd_frame.grid(row=5, column=1, columnspan=3, sticky="w", **pad)
        wd_vars: List[tk.BooleanVar] = []
        existing_wdays = set(existing.weekdays) if existing else set(range(7))
        day_short = [self.t(k) for k in _DAY_KEYS]
        for i, label in enumerate(day_short):
            v = tk.BooleanVar(value=(i in existing_wdays))
            wd_vars.append(v)
            ttk.Checkbutton(wd_frame, text=label, variable=v).pack(side="left", padx=2)

        # Enabled
        ttk.Label(dlg, text=self.t("sched.dlg.enabled")).grid(row=6, column=0, sticky="w", **pad)
        enabled_var = tk.BooleanVar(value=existing.enabled if existing else True)
        ttk.Checkbutton(dlg, variable=enabled_var).grid(row=6, column=1, sticky="w", **pad)

        # Buttons
        btn_frame = ttk.Frame(dlg)
        btn_frame.grid(row=7, column=0, columnspan=4, pady=(8, 8))

        def _save() -> None:
            name = name_var.get().strip()
            if not name:
                messagebox.showwarning(self.t("sched.dlg.title_add"), self.t("sched.msg.need_name"))
                return
            idx = dev_box.current()
            if idx < 0 or not devices:
                messagebox.showwarning(self.t("sched.dlg.title_add"), self.t("sched.msg.need_device"))
                return
            dev_key = devices[idx].key
            try:
                swid = int(switch_id_var.get().strip() or "0")
            except ValueError:
                swid = 0
            time_on = time_on_var.get().strip() or "06:00"
            time_off = time_off_var.get().strip() or "07:00"

            def _is_valid_time(t: str) -> bool:
                try:
                    parts = t.split(":")
                    if len(parts) != 2:
                        return False
                    hh, mm = int(parts[0]), int(parts[1])
                    return 0 <= hh <= 23 and 0 <= mm <= 59
                except Exception:
                    return False

            if not _is_valid_time(time_on) or not _is_valid_time(time_off):
                messagebox.showwarning(self.t("sched.dlg.title_add"), self.t("sched.msg.invalid_time"))
                return

            weekdays = [i for i, v in enumerate(wd_vars) if v.get()]
            if not weekdays:
                messagebox.showwarning(self.t("sched.dlg.title_add"), self.t("sched.msg.need_day"))
                return

            schedules = list(getattr(self.cfg, "schedules", []) or [])
            if existing:
                schedules = [s for s in schedules if s.schedule_id != existing.schedule_id]
                new_sched = replace(
                    existing,
                    name=name,
                    device_key=dev_key,
                    time_on=time_on,
                    time_off=time_off,
                    weekdays=weekdays,
                    enabled=enabled_var.get(),
                    switch_id=swid,
                )
            else:
                new_sched = DeviceSchedule(
                    schedule_id=str(uuid.uuid4()),
                    device_key=dev_key,
                    name=name,
                    time_on=time_on,
                    time_off=time_off,
                    weekdays=weekdays,
                    enabled=enabled_var.get(),
                    switch_id=swid,
                )
            schedules.append(new_sched)
            self._save_schedules(schedules)
            dlg.destroy()

        ttk.Button(btn_frame, text=self.t("btn.apply"), command=_save).pack(side="left", padx=6)
        ttk.Button(btn_frame, text=self.t("sched.btn.cancel"), command=dlg.destroy).pack(side="left", padx=6)

        dlg.columnconfigure(1, weight=1)
        dlg.wait_window()

    # ── Push / Load from device ───────────────────────────────────────────────

    def _sched_push_to_device(self) -> None:
        sel = self._get_selected_schedule()
        if sel is None:
            messagebox.showinfo(self.t("sched.title"), self.t("sched.msg.select"))
            return
        dev = None
        for d in getattr(self.cfg, "devices", []):
            if d.key == sel.device_key:
                dev = d
                break
        if dev is None:
            messagebox.showerror(self.t("sched.title"), self.t("sched.msg.no_device"))
            return
        if getattr(dev, "gen", 0) < 2:
            messagebox.showinfo(self.t("sched.title"), self.t("sched.msg.gen1_hint"))
            return

        self._sched_status_var.set(self.t("sched.status.pushing"))
        self.update_idletasks()

        def _push() -> None:
            http = getattr(self, "_http", ShellyHttp())
            try:
                # Delete existing Shelly schedules for this entry if previously pushed
                if sel.shelly_id_on >= 0:
                    try:
                        schedule_delete(http, dev.host, sel.shelly_id_on)
                    except Exception:
                        pass
                if sel.shelly_id_off >= 0:
                    try:
                        schedule_delete(http, dev.host, sel.shelly_id_off)
                    except Exception:
                        pass

                ts_on = build_shelly_timespec(sel.time_on, sel.weekdays)
                ts_off = build_shelly_timespec(sel.time_off, sel.weekdays)

                res_on = schedule_create(
                    http, dev.host, ts_on,
                    [{"method": "Switch.Set", "params": {"id": sel.switch_id, "on": True}}],
                    enable=sel.enabled,
                )
                res_off = schedule_create(
                    http, dev.host, ts_off,
                    [{"method": "Switch.Set", "params": {"id": sel.switch_id, "on": False}}],
                    enable=sel.enabled,
                )
                shelly_id_on = int(res_on.get("id", -1))
                shelly_id_off = int(res_off.get("id", -1))

                # Persist the Shelly IDs back to config
                updated = replace(sel, shelly_id_on=shelly_id_on, shelly_id_off=shelly_id_off)
                schedules = [s if s.schedule_id != sel.schedule_id else updated
                             for s in getattr(self.cfg, "schedules", [])]
                self.after(0, lambda: self._save_schedules(schedules))
                self.after(0, lambda: self._sched_status_var.set(self.t("sched.status.pushed")))
            except Exception as exc:
                logger.exception("Push schedule to device failed")
                self.after(0, lambda: self._sched_status_var.set(
                    self.t("sched.status.push_error", err=str(exc))
                ))

        threading.Thread(target=_push, daemon=True).start()

    def _sched_load_from_device(self) -> None:
        """Import all Shelly schedules that switch relay/switch on/off."""
        sel = self._get_selected_schedule()
        # Use selected schedule's device, or ask user to first select one.
        # For simplicity: open a device picker dialog.
        devices = [d for d in getattr(self.cfg, "devices", []) if getattr(d, "gen", 0) >= 2]
        if not devices:
            messagebox.showinfo(self.t("sched.title"), self.t("sched.msg.no_gen2"))
            return

        # Quick picker
        dlg = tk.Toplevel(self)
        dlg.title(self.t("sched.dlg.load_title"))
        dlg.resizable(False, False)
        dlg.grab_set()
        ttk.Label(dlg, text=self.t("sched.dlg.load_pick")).pack(padx=14, pady=(10, 4))
        dev_var = tk.StringVar()
        dev_names = [f"{d.name} ({d.host})" for d in devices]
        dev_box = ttk.Combobox(dlg, textvariable=dev_var, values=dev_names, state="readonly", width=30)
        dev_box.pack(padx=14, pady=4)
        if devices:
            dev_box.current(0)

        def _do_load() -> None:
            idx = dev_box.current()
            if idx < 0:
                return
            dev = devices[idx]
            dlg.destroy()
            self._sched_status_var.set(self.t("sched.status.loading"))
            self.update_idletasks()

            def _fetch() -> None:
                http = getattr(self, "_http", ShellyHttp())
                try:
                    result = schedule_list(http, dev.host)
                    jobs = result.get("jobs", [])
                    imported = 0
                    new_schedules = list(getattr(self.cfg, "schedules", []) or [])
                    for job in jobs:
                        if not isinstance(job, dict):
                            continue
                        calls = job.get("calls", [])
                        timespec = str(job.get("timespec", "") or "")
                        shelly_id = int(job.get("id", -1))
                        enable = bool(job.get("enable", True))
                        # Parse Switch.Set calls
                        for call in calls:
                            if not isinstance(call, dict):
                                continue
                            method = str(call.get("method", ""))
                            params = call.get("params", {})
                            if method == "Switch.Set" and isinstance(params, dict):
                                on = bool(params.get("on", True))
                                switch_id = int(params.get("id", 0))
                                # Parse time from timespec: "0 mm hh * * dow"
                                parts = timespec.split()
                                if len(parts) >= 3:
                                    hh = parts[2].zfill(2)
                                    mm = parts[1].zfill(2)
                                    time_str = f"{hh}:{mm}"
                                else:
                                    time_str = "00:00"
                                # Create a local schedule entry (linked to Shelly)
                                new_s = DeviceSchedule(
                                    schedule_id=str(uuid.uuid4()),
                                    device_key=dev.key,
                                    name=f"{dev.name} {'ON' if on else 'OFF'} {time_str}",
                                    time_on=time_str if on else "00:00",
                                    time_off=time_str if not on else "00:00",
                                    weekdays=list(range(7)),
                                    enabled=enable,
                                    switch_id=switch_id,
                                    shelly_id_on=shelly_id if on else -1,
                                    shelly_id_off=shelly_id if not on else -1,
                                )
                                new_schedules.append(new_s)
                                imported += 1
                    self.after(0, lambda: self._save_schedules(new_schedules))
                    self.after(0, lambda: self._sched_status_var.set(
                        self.t("sched.status.loaded", n=imported)
                    ))
                except Exception as exc:
                    logger.exception("Load schedules from device failed")
                    self.after(0, lambda: self._sched_status_var.set(
                        self.t("sched.status.load_error", err=str(exc))
                    ))

            threading.Thread(target=_fetch, daemon=True).start()

        btn_row = ttk.Frame(dlg)
        btn_row.pack(pady=(4, 10))
        ttk.Button(btn_row, text=self.t("sched.btn.load"), command=_do_load).pack(side="left", padx=6)
        ttk.Button(btn_row, text=self.t("sched.btn.cancel"), command=dlg.destroy).pack(side="left", padx=6)
        dlg.wait_window()

    def _sched_delete_from_device_bg(self, sched: DeviceSchedule) -> None:
        """Background: delete Shelly schedules from device (best-effort)."""
        dev = None
        for d in getattr(self.cfg, "devices", []):
            if d.key == sched.device_key:
                dev = d
                break
        if dev is None or getattr(dev, "gen", 0) < 2:
            return

        def _del() -> None:
            http = getattr(self, "_http", ShellyHttp())
            for sid in (sched.shelly_id_on, sched.shelly_id_off):
                if sid >= 0:
                    try:
                        schedule_delete(http, dev.host, sid)
                    except Exception:
                        pass

        threading.Thread(target=_del, daemon=True).start()
