from __future__ import annotations
import logging
import time
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

_log = logging.getLogger(__name__)


@dataclass
class CycleEvent:
    start_ts: int
    end_ts: int
    charge_kwh: float
    discharge_kwh: float
    efficiency_pct: float
    depth_pct: float  # Depth of discharge


@dataclass
class BatteryStatus:
    soc_pct: float = 0.0
    power_w: float = 0.0
    mode: str = "idle"  # charging | discharging | idle
    cycle_count: int = 0
    equivalent_cycles: float = 0.0  # SOC-swing based full-cycle equivalents
    total_charged_kwh: float = 0.0
    total_discharged_kwh: float = 0.0
    avg_efficiency_pct: float = 0.0
    efficiency_measured: bool = False  # False → nominal fallback, not a real round-trip
    capacity_kwh: float = 0.0
    cycles: List[CycleEvent] = field(default_factory=list)
    soc_timeline: List[Tuple[int, float]] = field(default_factory=list)  # [(ts, soc_pct), ...]
    optimal_charge_hours: List[int] = field(default_factory=list)


def compute_soc_timeline(
    samples, capacity_kwh: float, efficiency_pct: float = 95.0,
    initial_soc: float = 50.0,
) -> List[Tuple[int, float, float, str]]:
    """Compute SOC timeline from power samples.

    Returns list of (timestamp, soc_pct, power_w, mode).
    Positive power = charging, negative = discharging.
    """
    if not samples or capacity_kwh <= 0:
        return []

    eff = efficiency_pct / 100.0
    soc = initial_soc
    timeline = []
    prev_ts = None

    for ts, power_w in samples:
        if prev_ts is not None:
            dt_h = (ts - prev_ts) / 3600.0
            if dt_h > 0 and dt_h < 2:  # Skip gaps > 2h
                energy_kwh = abs(power_w) * dt_h / 1000.0
                if power_w > 50:  # Charging
                    soc += (energy_kwh * eff / capacity_kwh) * 100.0
                    mode = "charging"
                elif power_w < -50:  # Discharging
                    soc -= (energy_kwh / eff / capacity_kwh) * 100.0
                    mode = "discharging"
                else:
                    mode = "idle"
                soc = max(0.0, min(100.0, soc))
            else:
                mode = "idle"
        else:
            mode = "idle"

        timeline.append((ts, round(soc, 1), power_w, mode))
        prev_ts = ts

    return timeline


def detect_cycles(timeline: List[Tuple[int, float, float, str]], min_depth_pct: float = 10.0) -> List[CycleEvent]:
    """Detect charge/discharge cycles from SOC timeline."""
    if not timeline:
        return []

    cycles = []
    in_charge = False
    in_discharge = False
    charge_start = 0
    charge_kwh = 0.0
    discharge_kwh = 0.0
    peak_soc = 0.0
    trough_soc = 100.0
    prev_ts = None

    for ts, soc, power_w, mode in timeline:
        if prev_ts is not None:
            dt_h = (ts - prev_ts) / 3600.0
            if 0 < dt_h < 2:
                energy = abs(power_w) * dt_h / 1000.0
                if mode == "charging":
                    if not in_charge and not in_discharge:
                        charge_start = ts
                        charge_kwh = 0.0
                        discharge_kwh = 0.0
                        trough_soc = soc
                    in_charge = True
                    in_discharge = False
                    charge_kwh += energy
                    peak_soc = max(peak_soc, soc)
                elif mode == "discharging":
                    in_discharge = True
                    in_charge = False
                    discharge_kwh += energy
                    trough_soc = min(trough_soc, soc)
                elif mode == "idle" and in_discharge and discharge_kwh > 0:
                    # Cycle complete
                    depth = peak_soc - trough_soc
                    if depth >= min_depth_pct and charge_kwh > 0:
                        eff = (discharge_kwh / charge_kwh * 100) if charge_kwh > 0 else 0
                        cycles.append(CycleEvent(
                            start_ts=charge_start, end_ts=ts,
                            charge_kwh=round(charge_kwh, 3),
                            discharge_kwh=round(discharge_kwh, 3),
                            efficiency_pct=round(min(eff, 100), 1),
                            depth_pct=round(depth, 1),
                        ))
                    in_charge = False
                    in_discharge = False
                    charge_kwh = 0.0
                    discharge_kwh = 0.0
                    peak_soc = 0.0
                    trough_soc = 100.0
        prev_ts = ts

    return cycles


def optimal_charge_times(spot_prices: List[Tuple[int, float]], charge_hours: int = 4) -> List[int]:
    """Find optimal charging hours based on spot prices (cheapest N hours)."""
    if not spot_prices:
        return []
    sorted_prices = sorted(spot_prices, key=lambda x: x[1])
    return [ts for ts, _ in sorted_prices[:charge_hours]]


def get_battery_status(db, cfg) -> BatteryStatus:
    """Get comprehensive battery status from database."""
    status = BatteryStatus()

    if not cfg.enabled or not cfg.device_key:
        return status

    try:
        now = int(time.time())
        start_ts = now - 7 * 86400  # Last 7 days

        # Query power samples for battery device
        df = db.query_samples(cfg.device_key, start_ts, now)
        if df is None or df.empty:
            return status

        # Extract power time series (positive = charge, negative = discharge).
        # Vectorized: iterrows over the ~300k raw samples of a 7-day window (at a
        # 1–2 s poll) dominated latency (~10 s). Pull the two columns as numpy
        # arrays, then bucket to 1-minute mean power. A battery SOC curve moves
        # slowly, and mean-power-per-minute preserves the energy integral, so the
        # SOC estimate and cycle detection are unchanged while the point count
        # (and the JSON payload) drops ~30×.
        import numpy as np
        import pandas as pd
        try:
            _ts_col = df["timestamp"]
            if pd.api.types.is_datetime64_any_dtype(_ts_col):
                ts_arr = (_ts_col.astype("int64") // 1_000_000_000).to_numpy()
            else:
                ts_arr = pd.to_numeric(_ts_col, errors="coerce").fillna(0).astype("int64").to_numpy()
            pw_arr = pd.to_numeric(df.get("total_power", 0.0), errors="coerce").fillna(0.0).to_numpy(dtype=float)
        except Exception:
            return status

        _ok = ts_arr > 0
        ts_arr, pw_arr = ts_arr[_ok], pw_arr[_ok]
        if ts_arr.size == 0:
            return status

        if ts_arr.size > 1:
            # 1-minute buckets, energy-preserving (mean power); bucket ts = last.
            _bkt = ts_arr // 60
            _b = pd.DataFrame({"b": _bkt, "ts": ts_arr, "pw": pw_arr})
            _g = _b.groupby("b", sort=True)
            ts_ds = _g["ts"].last().to_numpy()
            pw_ds = _g["pw"].mean().to_numpy()
            samples = list(zip(ts_ds.tolist(), pw_ds.tolist()))
        else:
            samples = list(zip(ts_arr.tolist(), pw_arr.tolist()))

        if not samples:
            return status

        # Compute SOC timeline
        timeline = compute_soc_timeline(
            samples, cfg.capacity_kwh, cfg.efficiency_pct
        )

        # Anchor the integrated SOC curve to the real state-of-charge when an
        # external SOC entity is available (the integration starts from an
        # assumed 50% and drifts). Shift the whole curve so its last point
        # equals the measured SOC, clamped to [0, 100].
        _measured_soc = None
        try:
            from shelly_analyzer.services.pv_source import latest_readings
            _lr = latest_readings()
            if _lr.get("soc_pct") is not None:
                _measured_soc = float(_lr["soc_pct"])
        except Exception:
            _measured_soc = None
        if timeline and _measured_soc is not None:
            _shift = _measured_soc - timeline[-1][1]
            timeline = [
                (t[0], max(0.0, min(100.0, round(t[1] + _shift, 1))), t[2], t[3])
                for t in timeline
            ]

        if timeline:
            last = timeline[-1]
            status.soc_pct = last[1]
            status.power_w = last[2]
            status.mode = last[3]
            status.soc_timeline = [(t[0], t[1]) for t in timeline]

        status.capacity_kwh = round(float(cfg.capacity_kwh or 0.0), 2)

        # Detect cycles (for cycle_count + efficiency).
        cycles = detect_cycles(timeline)
        status.cycles = cycles
        status.cycle_count = len(cycles)
        # Round-trip efficiency is only meaningful when measured over a genuine
        # closed cycle (charge ≈ discharge). A charge-heavy partial cycle yields
        # a nonsensical ratio (e.g. 0.5%). Only trust cycles whose discharge is a
        # plausible fraction of their charge; otherwise fall back to the nominal
        # configured efficiency and flag it as not-measured.
        _real = [c for c in cycles if c.charge_kwh > 0 and 0.5 <= (c.discharge_kwh / c.charge_kwh) <= 1.05]
        nominal_eff = float(getattr(cfg, "efficiency_pct", 95.0) or 95.0)
        if _real:
            status.avg_efficiency_pct = round(sum(c.efficiency_pct for c in _real) / len(_real), 1)
            status.efficiency_measured = True
        else:
            status.avg_efficiency_pct = round(nominal_eff, 1)
            status.efficiency_measured = False

        # Equivalent full cycles over the window = sum of positive SOC swings / 100.
        _swing = 0.0
        _psoc = None
        for _t in timeline:
            _s = _t[1]
            if _psoc is not None and _s > _psoc:
                _swing += (_s - _psoc)
            _psoc = _s
        status.equivalent_cycles = round(_swing / 100.0, 2)

        # Total charged/discharged = ALL throughput over the window, independent
        # of whether a full cycle completed (cycle-only sums undercount).
        _charge_kwh = 0.0
        _discharge_kwh = 0.0
        _pt = None
        for _ts, _soc, _pw, _mode in timeline:
            if _pt is not None:
                _dt = (_ts - _pt) / 3600.0
                if 0 < _dt < 2:
                    _e = abs(_pw) * _dt / 1000.0
                    if _pw > 50:
                        _charge_kwh += _e
                    elif _pw < -50:
                        _discharge_kwh += _e
            _pt = _ts
        status.total_charged_kwh = round(_charge_kwh, 3)
        status.total_discharged_kwh = round(_discharge_kwh, 3)

    except Exception as e:
        _log.error("Battery status error: %s", e)

    return status
