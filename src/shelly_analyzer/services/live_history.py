"""Minute history for devices that cannot download one.

Switches and smart plugs (``kind == "switch"``, e.g. Plug S/M Gen3) have no
EMData component, so the EMData sync has nothing to fetch and their history
stayed empty: no costs, heatmap, CO2 or plots, "Samples: 0" in About. What a
plug does report is live — power, voltage, current and, on Gen2+, the
``aenergy.total`` counter in Wh. This module turns that live feed into one row
per minute in the same ``samples`` table the EMData import fills, so every
view that reads a device's history works for them as well.

Rules the rows follow:

* **Raw values.** The measurement compensation (calibration) is applied when
  the history is *read*, so storing it here would apply it twice.
* **Energy from the counter when it is trustworthy**, otherwise a trapezoid over
  the power samples. A counter that falls (device restart) or jumps by more
  than a device could draw is not used for that interval.
* **No gap is made up.** An interval longer than ``MAX_GAP_S`` (poller outage,
  device offline) adds nothing, and the next interval starts afresh.
* **All three ``*_total_act_energy`` columns are set.** ``calculate_energy``
  only takes the exact per-interval path when the whole a/b/c triplet exists.
* Demo devices (``demo://`` hosts) are never recorded; the demo generator
  already writes their history.
"""
from __future__ import annotations

import logging
import math
import threading
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

BUCKET_S = 60
# Longest interval between two samples that still counts as "continuous".
MAX_GAP_S = 300
# A single switch channel never draws more than this; a counter that advances
# faster than that between two polls is a counter fault, not energy.
MAX_PLAUSIBLE_W = 25_000.0
# Rows kept per device when the database cannot be written (retried next time).
PENDING_CAP = 240
_WARN_EVERY_S = 60.0


def records_history(device: Any) -> bool:
    """True when the device's history is written from the live feed.

    That is every real ``kind == "switch"`` device. Demo devices have a
    generator of their own, and meters (``em``) have the EMData sync.
    """
    if device is None:
        return False
    if str(getattr(device, "kind", "em") or "em") != "switch":
        return False
    return not str(getattr(device, "host", "") or "").startswith("demo://")


def _finite(value: Any) -> Optional[float]:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def counter_wh_from_status(raw: Any) -> Optional[float]:
    """``aenergy.total`` (Wh) from a Switch.GetStatus payload, or None."""
    if not isinstance(raw, dict):
        return None
    block = raw.get("aenergy")
    if not isinstance(block, dict):
        return None
    total = _finite(block.get("total"))
    if total is None or total < 0.0:
        return None
    return total


class LiveHistoryRecorder:
    """Collects live samples into one row per device and minute.

    Thread-safe. ``observe`` is called from the feed thread for every sample,
    ``flush`` from the shutdown path. A failing database never raises: the rows
    stay queued (bounded) and are retried with the next completed minute.
    """

    def __init__(self, db: Any, bucket_s: int = BUCKET_S) -> None:
        self._db = db
        self._bucket_s = max(1, int(bucket_s))
        self._lock = threading.Lock()
        # device_key -> {"ts", "p", "c", "bucket", "written"}
        self._state: Dict[str, Dict[str, Any]] = {}
        self._pending: Dict[str, List[Dict[str, Any]]] = {}
        self._last_warn = 0.0

    # -- public ------------------------------------------------------------

    def observe(
        self,
        device_key: str,
        ts: float,
        power_w: float,
        voltage_v: float = 0.0,
        current_a: float = 0.0,
        counter_wh: Optional[float] = None,
    ) -> None:
        ts_i = int(ts)
        p = _finite(power_w)
        p = max(0.0, p) if p is not None else 0.0
        v = _finite(voltage_v) or 0.0
        i = _finite(current_a) or 0.0
        c = _finite(counter_wh)
        if c is not None and c < 0.0:
            c = None

        with self._lock:
            self._flush_stale_locked(ts_i)
            st = self._state.setdefault(
                str(device_key),
                {"ts": None, "p": 0.0, "c": None, "bucket": None, "written": None},
            )
            prev_ts = st["ts"]
            inc_wh = 0.0
            if prev_ts is not None:
                dt = ts_i - int(prev_ts)
                if dt <= 0:
                    # Duplicate second or a clock that stepped back: nothing to add,
                    # and the reference must stay where it is.
                    return
                if dt <= MAX_GAP_S:
                    prev_c = st["c"]
                    used_counter = False
                    if c is not None and prev_c is not None:
                        diff = c - prev_c
                        if 0.0 <= diff <= MAX_PLAUSIBLE_W * dt / 3600.0:
                            inc_wh = diff
                            used_counter = True
                    if not used_counter:
                        inc_wh = (float(st["p"]) + p) / 2.0 * dt / 3600.0
            st["ts"], st["p"], st["c"] = ts_i, p, c

            start = ts_i - ts_i % self._bucket_s
            bk = st["bucket"]
            if bk is not None and bk["start"] != start:
                self._emit_locked(str(device_key), st, bk)
                bk = None
            if bk is None:
                bk = self._open_bucket_locked(st, start)
                st["bucket"] = bk
            bk["n"] += 1
            bk["p"] += p
            bk["v"] += v
            bk["i"] += i
            bk["wh"] += inc_wh

    def flush(self) -> None:
        """Write every open (possibly partial) minute. Safe to call repeatedly;
        the interval reference is kept so a hot reload loses nothing."""
        with self._lock:
            for key, st in self._state.items():
                bk = st.get("bucket")
                if bk is not None:
                    self._emit_locked(key, st, bk)
                    st["bucket"] = None
            for key in list(self._pending):
                self._write_locked(key)

    # -- internals (lock held) ---------------------------------------------

    def _open_bucket_locked(self, st: Dict[str, Any], start: int) -> Dict[str, Any]:
        # A minute that was already written (flush on a hot reload) continues
        # instead of being overwritten by the later, shorter part of it.
        prev = st.get("written")
        if prev is not None and prev["start"] == start:
            return dict(prev)
        return {"start": start, "n": 0, "p": 0.0, "v": 0.0, "i": 0.0, "wh": 0.0}

    def _flush_stale_locked(self, now_ts: int) -> None:
        """A device that went quiet must not keep its last minute in memory."""
        for key, st in self._state.items():
            bk = st.get("bucket")
            if bk is not None and bk["start"] + 2 * self._bucket_s <= now_ts:
                self._emit_locked(key, st, bk)
                st["bucket"] = None

    def _emit_locked(self, key: str, st: Dict[str, Any], bk: Dict[str, Any]) -> None:
        st["written"] = dict(bk)
        n = max(1, int(bk["n"]))
        if bk["n"] <= 0 and bk["wh"] <= 0.0:
            return
        p_mean = float(bk["p"]) / n
        v_mean = float(bk["v"]) / n
        i_mean = float(bk["i"]) / n
        wh = float(bk["wh"])
        row = {
            "timestamp": int(bk["start"]),
            "a_act_power": p_mean,
            "b_act_power": 0.0,
            "c_act_power": 0.0,
            "total_power": p_mean,
            # A plug that does not report a voltage must not show 0 V curves.
            "a_voltage": v_mean if v_mean > 0.0 else None,
            "a_current": i_mean,
            # Per-interval Wh in all three phases: the exact energy path of
            # calculate_energy needs the complete triplet.
            "a_total_act_energy": wh,
            "b_total_act_energy": 0.0,
            "c_total_act_energy": 0.0,
            "energy_kwh": wh / 1000.0,
        }
        rows = self._pending.setdefault(key, [])
        # The same minute written twice (continued after a flush) replaces it.
        if rows and rows[-1]["timestamp"] == row["timestamp"]:
            rows[-1] = row
        else:
            rows.append(row)
        self._write_locked(key)

    def _write_locked(self, key: str) -> None:
        rows = self._pending.get(key)
        if not rows:
            return
        try:
            import pandas as pd

            self._db.insert_dataframe(key, pd.DataFrame(rows))
            rows.clear()
        except Exception as e:  # never let the history take the feed down
            if len(rows) > PENDING_CAP:
                del rows[: len(rows) - PENDING_CAP]
            now = time.time()
            if now - self._last_warn >= _WARN_EVERY_S:
                self._last_warn = now
                logger.warning("Live history for %s could not be written (%d row(s) queued): %s",
                               key, len(rows), e)
