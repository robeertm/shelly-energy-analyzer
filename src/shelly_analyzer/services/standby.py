"""Standby/base-load detection and savings report.

Identifies devices with constant power draw (standby consumers) and
calculates potential annual savings if they were switched off.
"""
from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass, field
from typing import List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class StandbyDevice:
    """Analysis result for a single device's standby behaviour."""
    device_key: str
    device_name: str
    # Base load (the ~constant minimum power draw)
    base_load_w: float = 0.0
    # Median nighttime power (00:00–05:00)
    night_median_w: float = 0.0
    # Percentage of time the device draws < 2× base load
    standby_pct: float = 0.0
    # Annual standby energy and cost
    annual_standby_kwh: float = 0.0
    annual_standby_cost: float = 0.0
    # Risk level: low / medium / high
    risk: str = "low"
    # Hourly power profile (hour → avg watts)
    hourly_profile: List[float] = field(default_factory=list)
    # Total energy in analysis period
    total_kwh: float = 0.0
    # Standby share of total
    standby_share_pct: float = 0.0


@dataclass
class StandbyReport:
    """Full standby-killer report across all devices."""
    devices: List[StandbyDevice] = field(default_factory=list)
    total_annual_standby_kwh: float = 0.0
    total_annual_standby_cost: float = 0.0
    analysis_days: int = 30
    generated_at: str = ""


def analyze_standby(
    db,
    device_key: str,
    device_name: str,
    price_eur_per_kwh: float = 0.3265,
    days: int = 30,
) -> Optional[StandbyDevice]:
    """Analyze standby consumption for a single device."""
    now = datetime.datetime.now(datetime.timezone.utc)
    start_ts = int((now - datetime.timedelta(days=days)).timestamp())
    end_ts = int(now.timestamp())

    hourly = db.query_hourly(device_key, start_ts=start_ts, end_ts=end_ts)
    logger.debug("Standby %s: query_hourly returned %d rows", device_key, len(hourly))

    # Fallback: if no hourly data, try to compute from raw samples
    if hourly.empty or len(hourly) < 6:
        try:
            samples = db.query_samples(device_key, start_ts=start_ts, end_ts=end_ts)
            logger.debug("Standby %s: fallback query_samples returned %d rows", device_key, 0 if samples is None else len(samples))
            if samples is not None and not samples.empty and "total_power" in samples.columns:
                # Synthesize hourly from samples
                ts_col = samples["timestamp"]
                if hasattr(ts_col.iloc[0], "timestamp"):
                    # Already datetime — convert to unix seconds
                    samples["hour_ts"] = (ts_col.astype("int64") // 10**9 // 3600) * 3600
                else:
                    samples["hour_ts"] = (ts_col.astype("int64") // 3600) * 3600
                agg_dict: dict = {"avg_power_w": ("total_power", "mean")}
                if "energy_kwh" in samples.columns:
                    agg_dict["kwh"] = ("energy_kwh", "sum")
                hourly = samples.groupby("hour_ts").agg(**agg_dict).reset_index()
                if "kwh" not in hourly.columns:
                    # Estimate kWh from average power if energy_kwh not available
                    hourly["kwh"] = hourly["avg_power_w"] / 1000.0
                hourly["kwh"] = hourly["kwh"].fillna(0)
                hourly["avg_power_w"] = hourly["avg_power_w"].fillna(0)
                logger.debug("Standby %s: synthesized %d hourly rows from samples", device_key, len(hourly))
        except Exception:
            logger.debug("Standby fallback failed for %s", device_key, exc_info=True)

    if hourly.empty or len(hourly) < 6:
        logger.debug("Standby %s: insufficient data (%d rows, need >=6)", device_key, len(hourly))
        return None

    # Convert to arrays
    kwh_arr = hourly["kwh"].values.astype(float)
    avg_power = hourly["avg_power_w"].values.astype(float)

    if "hour_ts" in hourly.columns:
        hours = pd.to_datetime(hourly["hour_ts"], unit="s", utc=True)
    else:
        hours = pd.Series(pd.date_range(start="2020-01-01", periods=len(hourly), freq="h", tz="UTC"))

    # Remove NaN
    valid = ~np.isnan(avg_power)
    if valid.sum() < 6:
        return None
    avg_power = avg_power[valid]
    kwh_arr = kwh_arr[valid]
    hours = hours[valid]

    # Base load: 10th percentile of hourly power (robust against spikes)
    base_load = float(np.percentile(avg_power, 10))

    # Night median (00:00-05:00)
    night_mask = hours.hour.isin([0, 1, 2, 3, 4])
    if night_mask.sum() > 5:
        night_median = float(np.median(avg_power[night_mask]))
    else:
        night_median = base_load

    # Use the higher of percentile and night median as standby estimate
    standby_w = max(base_load, night_median * 0.8)

    # Percentage of hours in standby (< 2× base load)
    threshold = max(standby_w * 2, standby_w + 20)
    standby_hours = (avg_power < threshold).sum()
    standby_pct = float(standby_hours / len(avg_power) * 100)

    # Annual standby energy
    annual_standby_kwh = standby_w * 8760 / 1000.0
    annual_standby_cost = annual_standby_kwh * price_eur_per_kwh

    # Hourly profile
    hour_of_day = hours.hour
    hourly_profile = [0.0] * 24
    for h in range(24):
        mask = hour_of_day == h
        if mask.sum() > 0:
            hourly_profile[h] = float(np.mean(avg_power[mask]))

    total_kwh = float(kwh_arr.sum())
    standby_share = (annual_standby_kwh / (total_kwh * 365 / max(days, 1)) * 100) if total_kwh > 0 else 0

    # Risk classification
    if annual_standby_cost > 50:
        risk = "high"
    elif annual_standby_cost > 20:
        risk = "medium"
    else:
        risk = "low"

    return StandbyDevice(
        device_key=device_key,
        device_name=device_name,
        base_load_w=round(standby_w, 1),
        night_median_w=round(night_median, 1),
        standby_pct=round(standby_pct, 1),
        annual_standby_kwh=round(annual_standby_kwh, 1),
        annual_standby_cost=round(annual_standby_cost, 2),
        risk=risk,
        hourly_profile=[round(v, 1) for v in hourly_profile],
        total_kwh=round(total_kwh, 2),
        standby_share_pct=round(min(standby_share, 100), 1),
    )


def generate_standby_report(
    db,
    devices: list,
    price_eur_per_kwh: float = 0.3265,
    days: int = 30,
) -> StandbyReport:
    """Generate a full standby-killer report across all devices."""
    results: List[StandbyDevice] = []

    for dev in devices:
        try:
            r = analyze_standby(db, dev.key, dev.name, price_eur_per_kwh, days)
            if r is not None:
                results.append(r)
        except Exception:
            logger.debug("Standby analysis failed for %s", dev.key, exc_info=True)

    # Sort by annual cost (highest first)
    results.sort(key=lambda x: x.annual_standby_cost, reverse=True)

    total_kwh = sum(r.annual_standby_kwh for r in results)
    total_cost = sum(r.annual_standby_cost for r in results)

    return StandbyReport(
        devices=results,
        total_annual_standby_kwh=round(total_kwh, 1),
        total_annual_standby_cost=round(total_cost, 2),
        analysis_days=days,
        generated_at=datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
    )
