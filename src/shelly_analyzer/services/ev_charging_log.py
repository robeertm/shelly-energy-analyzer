from __future__ import annotations
import hashlib
import logging
import time
from dataclasses import dataclass, field
from typing import List, Optional

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


def detect_charging_sessions(
    df: pd.DataFrame,
    device_key: str,
    threshold_w: float = 1500.0,
    min_duration_s: int = 300,
    price_eur_per_kwh: float = 0.30,
) -> List[ChargingSession]:
    """Detect EV charging sessions from power time series.

    A session starts when total_power exceeds threshold_w and ends when it drops below.
    Sessions shorter than min_duration_s are discarded (brief spikes).
    """
    if df is None or df.empty:
        return []

    df = df.sort_values("timestamp").reset_index(drop=True)

    sessions: List[ChargingSession] = []
    in_session = False
    session_start = 0
    session_rows: List[int] = []

    power_col = "total_power" if "total_power" in df.columns else "energy_kwh"
    if power_col not in df.columns:
        return []

    for idx, row in df.iterrows():
        power = float(row.get(power_col, 0) or 0)
        ts = int(row["timestamp"])

        if not in_session and power >= threshold_w:
            in_session = True
            session_start = ts
            session_rows = [idx]
        elif in_session and power >= threshold_w * 0.5:  # Hysteresis at 50%
            session_rows.append(idx)
        elif in_session and power < threshold_w * 0.5:
            # End session
            in_session = False
            duration_s = ts - session_start
            if duration_s >= min_duration_s and session_rows:
                sub = df.iloc[session_rows]
                energy = float(sub["energy_kwh"].sum()) if "energy_kwh" in sub.columns else 0
                if energy <= 0:
                    # Estimate from power
                    avg_p = float(sub[power_col].mean())
                    energy = avg_p * duration_s / 3600 / 1000

                sid = hashlib.md5(f"{device_key}:{session_start}:{ts}".encode()).hexdigest()[:12]
                sessions.append(ChargingSession(
                    session_id=sid,
                    device_key=device_key,
                    start_ts=session_start,
                    end_ts=ts,
                    energy_kwh=round(energy, 3),
                    peak_power_w=round(float(sub[power_col].max()), 1),
                    avg_power_w=round(float(sub[power_col].mean()), 1),
                    cost_eur=round(energy * price_eur_per_kwh, 2),
                    cost_model="fixed",
                ))
            session_rows = []

    # Handle open session at end of data
    if in_session and session_rows:
        ts = int(df.iloc[-1]["timestamp"])
        duration_s = ts - session_start
        if duration_s >= min_duration_s:
            sub = df.iloc[session_rows]
            energy = float(sub["energy_kwh"].sum()) if "energy_kwh" in sub.columns else 0
            if energy <= 0:
                avg_p = float(sub[power_col].mean())
                energy = avg_p * duration_s / 3600 / 1000
            sid = hashlib.md5(f"{device_key}:{session_start}:{ts}".encode()).hexdigest()[:12]
            sessions.append(ChargingSession(
                session_id=sid, device_key=device_key,
                start_ts=session_start, end_ts=ts,
                energy_kwh=round(energy, 3),
                peak_power_w=round(float(sub[power_col].max()), 1),
                avg_power_w=round(float(sub[power_col].mean()), 1),
                cost_eur=round(energy * price_eur_per_kwh, 2),
                cost_model="fixed",
            ))

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
