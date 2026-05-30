from __future__ import annotations
import hashlib
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import List, Optional

import numpy as np
import pandas as pd

_log = logging.getLogger(__name__)


@dataclass
class ChargingSession:
    session_id: str
    device_key: str
    start_ts: int
    end_ts: int
    energy_kwh: float
    peak_power_w: float
    avg_power_w: float
    cost_eur: float = 0.0
    cost_model: str = "fixed"


@dataclass
class ChargingSummary:
    total_sessions: int = 0
    total_kwh: float = 0.0
    total_cost: float = 0.0
    avg_kwh_per_session: float = 0.0
    avg_duration_min: float = 0.0
    sessions: List[ChargingSession] = field(default_factory=list)


# In-process detection cache: keyed by (device_key, threshold, min_duration,
# price, max_gap). Hit while the underlying samples haven't grown.
_DETECT_CACHE: dict = {}
_DETECT_LOCK = threading.Lock()


def detect_charging_sessions(
    df: pd.DataFrame,
    device_key: str,
    threshold_w: float = 1500.0,
    min_duration_s: int = 300,
    price_eur_per_kwh: float = 0.30,
    max_gap_s: int = 900,
) -> List[ChargingSession]:
    """Detect EV charging sessions from a power time series.

    Vectorized via numpy — replaces the previous per-row iterrows loop, which
    was O(n) Python overhead per sample and dominated /api/ev_sessions latency
    on multi-month wallbox history (tens of thousands of rows).

    Semantics preserved from the iterative implementation:
      * A session starts when power first crosses ``threshold_w`` upward.
      * Hysteresis: it continues while power stays above ``threshold_w * 0.5``.
      * Brief drops below the hysteresis floor — up to ``max_gap_s`` seconds —
        are *bridged* so a single charge is not split by short data dips or
        one-off zero-power artifacts. The session ends after low has persisted
        longer than ``max_gap_s``; its end is the last active sample.
      * Bridged low-power samples are excluded from peak/avg/energy.
      * Sessions shorter than ``min_duration_s`` are dropped (brief spikes).
    """
    if df is None or df.empty:
        return []

    power_col = "total_power" if "total_power" in df.columns else "energy_kwh"
    if power_col not in df.columns or "timestamp" not in df.columns:
        return []

    df = df.sort_values("timestamp").reset_index(drop=True)

    _ts = df["timestamp"]
    if pd.api.types.is_datetime64_any_dtype(_ts):
        if getattr(_ts.dt, "tz", None) is not None:
            _ts = _ts.dt.tz_convert("UTC").dt.tz_localize(None)
        ts_arr = _ts.astype("datetime64[s]").astype("int64").to_numpy()
    else:
        ts_arr = pd.to_numeric(_ts, errors="coerce").fillna(0).astype("int64").to_numpy()

    power = pd.to_numeric(df[power_col], errors="coerce").fillna(0.0).to_numpy(dtype=np.float64)
    energy_arr = (
        pd.to_numeric(df["energy_kwh"], errors="coerce").fillna(0.0).to_numpy(dtype=np.float64)
        if "energy_kwh" in df.columns
        else None
    )

    n = len(power)
    if n == 0:
        return []

    cache_key = (
        device_key,
        float(threshold_w),
        int(min_duration_s),
        float(price_eur_per_kwh),
        int(max_gap_s),
    )
    max_ts = int(ts_arr[-1])
    with _DETECT_LOCK:
        cached = _DETECT_CACHE.get(cache_key)
        if cached and cached[0] == max_ts and cached[1] == n:
            return list(cached[2])

    above_low = power >= threshold_w * 0.5
    above_trigger = power >= threshold_w

    active_idx = np.where(above_low)[0]
    if active_idx.size == 0:
        with _DETECT_LOCK:
            _DETECT_CACHE[cache_key] = (max_ts, n, [])
        return []

    if active_idx.size == 1:
        cluster_bounds = [(int(active_idx[0]), int(active_idx[0]))]
    else:
        gaps_ts = ts_arr[active_idx[1:]] - ts_arr[active_idx[:-1]]
        break_positions = np.where(gaps_ts > max_gap_s)[0]
        if break_positions.size:
            starts = np.concatenate([[active_idx[0]], active_idx[break_positions + 1]])
            ends = np.concatenate([active_idx[break_positions], [active_idx[-1]]])
        else:
            starts = active_idx[:1]
            ends = active_idx[-1:]
        cluster_bounds = list(zip(starts.tolist(), ends.tolist()))

    sessions: List[ChargingSession] = []
    for s_idx, e_idx in cluster_bounds:
        seg_trigger = above_trigger[s_idx:e_idx + 1]
        if not seg_trigger.any():
            continue
        first_trigger = s_idx + int(np.argmax(seg_trigger))
        start_ts = int(ts_arr[first_trigger])
        end_ts = int(ts_arr[e_idx])
        duration_s = end_ts - start_ts
        if duration_s < min_duration_s:
            continue

        # Only above_low samples in [first_trigger, e_idx] contribute to
        # energy / peak / avg. Bridged low-power dips are excluded so they
        # don't pull the averages and energy totals down.
        active_mask = above_low[first_trigger:e_idx + 1]
        seg_power = power[first_trigger:e_idx + 1][active_mask]
        if seg_power.size == 0:
            continue
        if energy_arr is not None:
            seg_energy = energy_arr[first_trigger:e_idx + 1][active_mask]
            energy = float(seg_energy.sum())
            if energy <= 0:
                energy = float(seg_power.mean()) * duration_s / 3600.0 / 1000.0
        else:
            energy = float(seg_power.mean()) * duration_s / 3600.0 / 1000.0

        sid = hashlib.md5(f"{device_key}:{start_ts}:{end_ts}".encode()).hexdigest()[:12]
        sessions.append(ChargingSession(
            session_id=sid,
            device_key=device_key,
            start_ts=start_ts,
            end_ts=end_ts,
            energy_kwh=round(energy, 3),
            peak_power_w=round(float(seg_power.max()), 1),
            avg_power_w=round(float(seg_power.mean()), 1),
            cost_eur=round(energy * price_eur_per_kwh, 2),
            cost_model="fixed",
        ))

    with _DETECT_LOCK:
        _DETECT_CACHE[cache_key] = (max_ts, n, sessions)
    return sessions


def get_monthly_summary(sessions: List[ChargingSession]) -> ChargingSummary:
    """Aggregate charging sessions into a summary."""
    if not sessions:
        return ChargingSummary()

    total_kwh = sum(s.energy_kwh for s in sessions)
    total_cost = sum(s.cost_eur for s in sessions)
    durations = [(s.end_ts - s.start_ts) / 60 for s in sessions]

    return ChargingSummary(
        total_sessions=len(sessions),
        total_kwh=round(total_kwh, 2),
        total_cost=round(total_cost, 2),
        avg_kwh_per_session=round(total_kwh / len(sessions), 2),
        avg_duration_min=round(sum(durations) / len(durations), 1),
        sessions=sessions,
    )
