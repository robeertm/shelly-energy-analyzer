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
    power_24h: List[Tuple[int, float]] = field(default_factory=list)  # [(ts, power_w), ...] last 24 h, 1-min buckets
    measured_since_ts: int = 0  # first battery_state row in the window (0 = curve is integrated)
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


def detect_cycles(timeline: List[Tuple[int, float, float, str]], min_depth_pct: float = 10.0,
                  hysteresis_pct: Optional[float] = None, capacity_kwh: float = 0.0) -> List[CycleEvent]:
    """Cycles from the SOC curve's turning points: trough → peak → trough.

    The earlier version closed a cycle at the first idle minute after the
    discharge had begun — a battery that pauses for one 1-minute bucket at
    -40 W then "completed" a cycle with 0.002 kWh discharged and 0 %
    efficiency, and no round trip ever counted as measured.  A turning point
    only counts once the curve has moved ``hysteresis_pct`` the other way, so
    a flat afternoon with small ripples stays one cycle.
    """
    if len(timeline) < 3:
        return []
    if hysteresis_pct is None:
        hysteresis_pct = min_depth_pct   # an evening dip that is not a cycle is not a turning point either
    # 1. turning points with hysteresis
    ext: List[Tuple[int, str]] = []          # (index, "min"|"max")
    cand_i, cand_soc = 0, timeline[0][1]
    direction = 0                            # +1 rising, -1 falling, 0 unknown
    run_max = run_min = timeline[0][1]      # until the first clear move, both are tracked
    run_max_i = run_min_i = 0
    for i in range(1, len(timeline)):
        soc = timeline[i][1]
        if direction == 0:
            if soc > run_max:
                run_max, run_max_i = soc, i
            if soc < run_min:
                run_min, run_min_i = soc, i
            # the first clear move also names the point it started from
            if run_max - soc >= hysteresis_pct:
                ext.append((run_max_i, "max"))
                direction, cand_i, cand_soc = -1, i, soc
            elif soc - run_min >= hysteresis_pct:
                ext.append((run_min_i, "min"))
                direction, cand_i, cand_soc = 1, i, soc
            continue
        if direction == 1:
            if soc > cand_soc:
                cand_i, cand_soc = i, soc
            elif cand_soc - soc >= hysteresis_pct:
                ext.append((cand_i, "max"))
                direction, cand_i, cand_soc = -1, i, soc
        else:
            if soc < cand_soc:
                cand_i, cand_soc = i, soc
            elif soc - cand_soc >= hysteresis_pct:
                ext.append((cand_i, "min"))
                direction, cand_i, cand_soc = 1, i, soc
    if direction == -1:
        ext.append((cand_i, "min"))          # the curve is still falling: close at its lowest point so far

    def _energy(i0: int, i1: int) -> Tuple[float, float]:
        ch = dis = 0.0
        for j in range(i0 + 1, i1 + 1):
            dt_h = (timeline[j][0] - timeline[j - 1][0]) / 3600.0
            if not (0 < dt_h < 2):
                continue
            pw = timeline[j][2]
            e = abs(pw) * dt_h / 1000.0
            if pw > 50:
                ch += e
            elif pw < -50:
                dis += e
        return ch, dis

    # 2. every max that sits between two mins is one cycle
    cycles: List[CycleEvent] = []
    for k in range(1, len(ext) - 1):
        if ext[k][1] != "max" or ext[k - 1][1] != "min" or ext[k + 1][1] != "min":
            continue
        i0, ip, i1 = ext[k - 1][0], ext[k][0], ext[k + 1][0]
        peak, trough = timeline[ip][1], timeline[i1][1]
        depth = peak - trough
        if depth < min_depth_pct:
            continue
        ch, dis_c = _energy(i0, ip)
        ch_d, dis = _energy(ip, i1)
        ch, dis = ch + ch_d, dis + dis_c        # everything in and out between the two troughs
        if ch <= 0:
            continue
        # Round trip: what came out over what went in — less what is still
        # inside when the second trough sits higher than the first (or plus
        # what was taken beyond it). Without that correction a cycle that ends
        # 20 % fuller reads as 60 % efficient.
        _delta_kwh = (trough - timeline[i0][1]) / 100.0 * capacity_kwh if capacity_kwh > 0 else 0.0
        _in_used = ch - _delta_kwh
        eff = min(dis / _in_used * 100.0, 100.0) if _in_used > 0.3 else 0.0
        cycles.append(CycleEvent(
            start_ts=timeline[i0][0], end_ts=timeline[i1][0],
            charge_kwh=round(ch, 3), discharge_kwh=round(dis, 3),
            efficiency_pct=round(eff, 1), depth_pct=round(depth, 1),
        ))
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

        # Query power samples for battery device.  Only two columns are read
        # below, and the samples table has around forty — a seven-day window is
        # ~300k rows, so SELECT * was the whole cost of this call (measured on a
        # live installation: 15 s for /api/battery, of which 0.13 s was the
        # computation).  Older DB layers without the parameter still work.
        try:
            df = db.query_samples(cfg.device_key, start_ts, now,
                                  columns=("timestamp", "total_power"))
        except TypeError:
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
                # Normalize tz then go via datetime64[s] so the epoch conversion
                # is correct regardless of the column's datetime resolution
                # (ns/us/s) — a hardcoded ÷1e9 collapses second-resolution
                # timestamps to a single bucket. Mirrors detect_charging_sessions.
                if getattr(_ts_col.dt, "tz", None) is not None:
                    _ts_col = _ts_col.dt.tz_convert("UTC").dt.tz_localize(None)
                ts_arr = _ts_col.astype("datetime64[s]").astype("int64").to_numpy()
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

        # The measured curve, where it exists (battery_state is written once a
        # minute since 17.1). Integration is only kept for the stretch BEFORE
        # the first measured row, pinned to the first measurement so the two
        # meet without a step. From then on every point is a reading.
        try:
            _ms = db.query_battery_state(cfg.device_key, start_ts, now)
        except Exception:
            _ms = None
        if _ms is not None and len(_ms) >= 60:
            _mts = _ms["timestamp"].astype("int64").to_numpy()
            _msoc = _ms["soc_pct"].astype(float).to_numpy()
            _mpw = _ms["power_w"].astype(float).fillna(0.0).to_numpy() if "power_w" in _ms.columns else np.zeros(len(_mts))
            _first = int(_mts[0])
            _head = [t for t in timeline if t[0] < _first]
            if _head:
                _shift = float(_msoc[0]) - _head[-1][1]
                _head = [(t[0], max(0.0, min(100.0, round(t[1] + _shift, 1))), t[2], t[3]) for t in _head]
            _tail = []
            for _i in range(len(_mts)):
                _p = float(_mpw[_i])
                _tail.append((int(_mts[_i]), round(float(_msoc[_i]), 1), _p,
                              "charging" if _p > 50 else ("discharging" if _p < -50 else "idle")))
            timeline = _head + _tail
            status.measured_since_ts = _first

        if timeline:
            last = timeline[-1]
            status.soc_pct = last[1]
            status.power_w = last[2]
            status.mode = last[3]
            status.soc_timeline = [(t[0], t[1]) for t in timeline]
            _day_ago = now - 86400
            status.power_24h = [(t[0], round(float(t[2]))) for t in timeline if t[0] >= _day_ago]

        status.capacity_kwh = round(float(cfg.capacity_kwh or 0.0), 2)

        # Detect cycles (for cycle_count + efficiency).
        cycles = detect_cycles(timeline, capacity_kwh=float(cfg.capacity_kwh or 0.0))
        status.cycles = cycles
        status.cycle_count = len(cycles)
        # Round-trip efficiency is only meaningful when measured over a genuine
        # closed cycle (charge ≈ discharge). A charge-heavy partial cycle yields
        # a nonsensical ratio (e.g. 0.5%). Only trust cycles whose discharge is a
        # plausible fraction of their charge; otherwise fall back to the nominal
        # configured efficiency and flag it as not-measured.
        # A measured efficiency needs a measured curve: the integrated one is
        # built from the same power with the configured efficiency, so its
        # cycles can only ever echo the setting back.
        _real = [c for c in cycles if 50.0 <= c.efficiency_pct <= 100.0] if status.measured_since_ts else []
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
