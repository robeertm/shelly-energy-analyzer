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
    total_charged_kwh: float = 0.0
    total_discharged_kwh: float = 0.0
    avg_efficiency_pct: float = 0.0
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

        # Extract power time series (positive = charge, negative = discharge)
        samples = []
        for _, row in df.iterrows():
            ts = int(row.get("timestamp", 0))
            power = float(row.get("total_power", 0) or 0)
            samples.append((ts, power))

        if not samples:
            return status

        # Compute SOC timeline
        timeline = compute_soc_timeline(
            samples, cfg.capacity_kwh, cfg.efficiency_pct
        )

        if timeline:
            last = timeline[-1]
            status.soc_pct = last[1]
            status.power_w = last[2]
            status.mode = last[3]
            status.soc_timeline = [(t[0], t[1]) for t in timeline]

        # Detect cycles
        cycles = detect_cycles(timeline)
        status.cycles = cycles
        status.cycle_count = len(cycles)
        status.total_charged_kwh = sum(c.charge_kwh for c in cycles)
        status.total_discharged_kwh = sum(c.discharge_kwh for c in cycles)
        if cycles:
            status.avg_efficiency_pct = round(
                sum(c.efficiency_pct for c in cycles) / len(cycles), 1
            )

    except Exception as e:
        _log.error("Battery status error: %s", e)

    return status
