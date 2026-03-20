"""Local device scheduler service for Shelly Energy Analyzer.

For Gen2+ devices: schedules are pushed to the device via Schedule.Create RPC.
For Gen1 devices (or as a fallback): the app itself fires on/off commands at
the right time using a background thread that ticks every 30 seconds.

The scheduler is intentionally simple:
- It reads schedules from the current AppConfig (injected via callback).
- It fires a switch command when the current local time matches the on- or
  off-time of an enabled schedule on the current weekday.
- A simple "already-fired" set prevents double-firing within the same minute.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from shelly_analyzer.io.config import AppConfig, DeviceSchedule
from shelly_analyzer.io.http import ShellyHttp, set_switch_state

logger = logging.getLogger(__name__)

# (schedule_id, "on"|"off", YYYY-MM-DD HH:MM) – prevents double-firing
_FIRED_KEY = Tuple[str, str, str]


class LocalScheduler:
    """Background thread that fires on/off commands based on DeviceSchedule entries.

    Usage::

        scheduler = LocalScheduler(get_config=lambda: app.cfg,
                                   get_http=lambda: app.http_client)
        scheduler.start()
        …
        scheduler.stop()
    """

    def __init__(
        self,
        get_config: Callable[[], AppConfig],
        get_http: Callable[[], ShellyHttp],
        tick_seconds: float = 30.0,
    ) -> None:
        self._get_config = get_config
        self._get_http = get_http
        self._tick = tick_seconds
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        # Tracks which (schedule_id, direction, minute-key) have been fired.
        # Cleared once a minute changes so the set stays small.
        self._fired: Set[_FIRED_KEY] = set()
        self._last_minute: str = ""

    # ── Public API ──────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="LocalScheduler",
            daemon=True,
        )
        self._thread.start()
        logger.info("LocalScheduler started (tick=%.0fs)", self._tick)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("LocalScheduler stopped")

    # ── Internal ─────────────────────────────────────────────────────────────

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._tick_once()
            except Exception:
                logger.exception("LocalScheduler tick error")
            self._stop_event.wait(self._tick)

    def _tick_once(self) -> None:
        now = datetime.now()
        # weekday(): 0=Mon … 6=Sun  (matches our DeviceSchedule.weekdays convention)
        wd = now.weekday()
        hhmm = now.strftime("%H:%M")
        minute_key = now.strftime("%Y-%m-%d %H:%M")

        # Purge fired-set when minute changes to keep memory bounded.
        if minute_key != self._last_minute:
            self._fired.clear()
            self._last_minute = minute_key

        try:
            cfg = self._get_config()
        except Exception:
            return

        schedules: List[DeviceSchedule] = list(getattr(cfg, "schedules", []) or [])
        if not schedules:
            return

        # Build a host lookup from device_key -> (host, gen, switch_id_default)
        dev_map: Dict[str, Any] = {}
        for d in getattr(cfg, "devices", []):
            dev_map[d.key] = d

        for sched in schedules:
            if not sched.enabled:
                continue
            if wd not in sched.weekdays:
                continue

            dev = dev_map.get(sched.device_key)
            if dev is None:
                continue

            # For Gen2+ devices that had schedules pushed, the device handles
            # the timing itself.  We still fire locally as a safety net if the
            # device was unreachable at the time the schedule was originally
            # pushed (shelly_id_on == -1).
            gen2_on_pushed = sched.shelly_id_on >= 0
            gen2_off_pushed = sched.shelly_id_off >= 0

            if hhmm == sched.time_on:
                key: _FIRED_KEY = (sched.schedule_id, "on", minute_key)
                if key not in self._fired:
                    self._fired.add(key)
                    if not gen2_on_pushed:
                        self._fire(dev.host, sched.switch_id, True, sched.name)

            if hhmm == sched.time_off:
                key = (sched.schedule_id, "off", minute_key)
                if key not in self._fired:
                    self._fired.add(key)
                    if not gen2_off_pushed:
                        self._fire(dev.host, sched.switch_id, False, sched.name)

    def _fire(self, host: str, switch_id: int, on: bool, name: str) -> None:
        state_str = "ON" if on else "OFF"
        logger.info("Schedule '%s' → %s:%d %s", name, host, switch_id, state_str)
        try:
            http = self._get_http()
            set_switch_state(http, host, switch_id, on)
        except Exception as exc:
            logger.warning("Schedule fire failed for '%s': %s", name, exc)


def build_shelly_timespec(time_str: str, weekdays: List[int]) -> str:
    """Convert HH:MM + weekday list to a Shelly Gen2 cron timespec.

    Shelly cron: "ss mm hh dom month dow"
    dow: 0=Sun, 1=Mon, … 6=Sat (standard cron convention).
    Our weekdays: 0=Mon … 6=Sun (Python weekday() convention).
    """
    try:
        hh, mm = time_str.split(":")
        hh = int(hh)
        mm = int(mm)
    except Exception:
        hh, mm = 0, 0

    # Convert Python weekday to cron dow: Mon→1, Tue→2, … Sat→6, Sun→0
    _PY_TO_CRON = {0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 5: 6, 6: 0}
    if not weekdays or len(weekdays) == 7:
        dow_str = "*"
    else:
        dow_nums = sorted({_PY_TO_CRON[d] for d in weekdays if d in _PY_TO_CRON})
        dow_str = ",".join(str(x) for x in dow_nums)

    return f"0 {mm} {hh} * * {dow_str}"
