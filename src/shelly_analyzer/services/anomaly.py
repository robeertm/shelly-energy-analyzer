"""Anomaly detection engine for Shelly Energy Analyzer.

Detects unusual consumption patterns using rolling mean + standard deviation:
- Unusual daily energy consumption (±Nσ from rolling baseline)
- Elevated night-time consumption ratio vs. day
- Power peaks occurring at unusual hours
"""
from __future__ import annotations

import math
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import List

import numpy as np
import pandas as pd


@dataclass
class AnomalyEvent:
    """A single detected anomaly."""

    event_id: str
    timestamp: datetime
    device_key: str
    device_name: str
    # "unusual_daily" | "night_consumption" | "power_peak_time"
    anomaly_type: str
    value: float
    expected_mean: float
    expected_std: float
    sigma_count: float
    description: str
    notified: bool = False


def _new_id() -> str:
    return uuid.uuid4().hex[:12]


def _deterministic_id(device_key: str, anomaly_type: str, ts) -> str:
    """Generate a deterministic event ID based on device, type, and date.

    Same anomaly on the same device+day always gets the same ID,
    preventing duplicate notifications.
    """
    import hashlib
    date_str = ""
    if hasattr(ts, "strftime"):
        date_str = ts.strftime("%Y-%m-%d")
    else:
        date_str = str(ts)[:10]
    raw = f"{device_key}:{anomaly_type}:{date_str}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def detect_anomalies(
    df: pd.DataFrame,
    device_key: str,
    device_name: str,
    sigma: float = 2.0,
    min_deviation_kwh: float = 0.1,
    window_days: int = 30,
    check_unusual_daily: bool = True,
    check_night_consumption: bool = True,
    check_power_peak_time: bool = True,
) -> List[AnomalyEvent]:
    """Run all configured anomaly checks on a device DataFrame.

    Parameters
    ----------
    df:
        DataFrame with columns ['timestamp', 'energy_kwh', 'total_power'].
    device_key / device_name:
        Identifiers for the device.
    sigma:
        Number of standard deviations required to flag an anomaly.
    min_deviation_kwh:
        Minimum absolute deviation (kWh) required for unusual_daily check.
    window_days:
        Rolling window size (days) used to compute the baseline statistics.
    check_*:
        Toggle individual check types.

    Returns
    -------
    List[AnomalyEvent]
        May be empty if no anomalies are detected or data is insufficient.
    """
    events: List[AnomalyEvent] = []

    if df is None or len(df) < 2:
        return events

    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").set_index("timestamp")

    if check_unusual_daily:
        events.extend(
            _check_unusual_daily(df, device_key, device_name, sigma, min_deviation_kwh, window_days)
        )

    if check_night_consumption:
        events.extend(
            _check_night_consumption(df, device_key, device_name, sigma, window_days)
        )

    if check_power_peak_time:
        events.extend(
            _check_power_peak_time(df, device_key, device_name, sigma, window_days)
        )

    return events


# ---------------------------------------------------------------------------
# Individual detectors
# ---------------------------------------------------------------------------


def _check_unusual_daily(
    df: pd.DataFrame,
    device_key: str,
    device_name: str,
    sigma: float,
    min_deviation_kwh: float,
    window_days: int,
) -> List[AnomalyEvent]:
    """Detect days where daily energy consumption deviates significantly."""
    events: List[AnomalyEvent] = []

    daily = df["energy_kwh"].resample("D").sum()
    if len(daily) < 5:
        return events

    win = min(window_days, len(daily) - 1)
    rolling_mean = daily.rolling(window=win, min_periods=5).mean()
    rolling_std = daily.rolling(window=win, min_periods=5).std()

    # Only evaluate the last 7 days to avoid flooding the log on first run
    for day in daily.index[-7:]:
        val = float(daily.at[day])
        mean = float(rolling_mean.at[day]) if day in rolling_mean.index else math.nan
        std = float(rolling_std.at[day]) if day in rolling_std.index else math.nan

        if math.isnan(mean) or math.isnan(std) or std <= 0:
            continue

        diff = abs(val - mean)
        sig = diff / std

        if sig < sigma or diff < min_deviation_kwh:
            continue

        direction = "high" if val > mean else "low"
        # Find the hour of peak consumption for a meaningful timestamp
        day_data = df[df.index.date == (day.date() if hasattr(day, 'date') else day)]
        if len(day_data) and "energy_kwh" in day_data.columns:
            peak_idx = day_data["energy_kwh"].idxmax()
            ts = peak_idx.to_pydatetime() if hasattr(peak_idx, 'to_pydatetime') else pd.Timestamp(day).to_pydatetime()
        else:
            ts = pd.Timestamp(day).to_pydatetime()
        events.append(
            AnomalyEvent(
                event_id=_deterministic_id(device_key, "unusual_daily", ts),
                timestamp=ts,
                device_key=device_key,
                device_name=device_name,
                anomaly_type="unusual_daily",
                value=val,
                expected_mean=mean,
                expected_std=std,
                sigma_count=sig,
                description=(
                    f"{val:.2f} kWh ({sig:.1f}σ {direction}, "
                    f"baseline {mean:.2f}±{std:.2f} kWh)"
                ),
            )
        )

    return events


def _check_night_consumption(
    df: pd.DataFrame,
    device_key: str,
    device_name: str,
    sigma: float,
    window_days: int,
) -> List[AnomalyEvent]:
    """Detect elevated night-time (22:00–06:00) consumption ratio."""
    events: List[AnomalyEvent] = []

    if "energy_kwh" not in df.columns:
        return events

    hour = df.index.hour
    night_mask = (hour >= 22) | (hour < 6)

    night_daily = df.loc[night_mask, "energy_kwh"].resample("D").sum()
    day_daily = df.loc[~night_mask, "energy_kwh"].resample("D").sum()

    if len(night_daily) < 7 or len(day_daily) < 7:
        return events

    total_daily = night_daily.add(day_daily, fill_value=0)
    ratio = night_daily / total_daily.replace(0, np.nan)
    ratio = ratio.dropna()

    if len(ratio) < 7:
        return events

    win = min(window_days, len(ratio) - 1)
    rolling_mean = ratio.rolling(window=win, min_periods=7).mean()
    rolling_std = ratio.rolling(window=win, min_periods=7).std()

    for day in ratio.index[-7:]:
        val = float(ratio.at[day]) if day in ratio.index else math.nan
        mean = float(rolling_mean.at[day]) if day in rolling_mean.index else math.nan
        std = float(rolling_std.at[day]) if day in rolling_std.index else math.nan
        night_kwh = float(night_daily.at[day]) if day in night_daily.index else 0.0

        if math.isnan(val) or math.isnan(mean) or math.isnan(std) or std <= 0:
            continue

        diff = val - mean  # only flag when HIGHER than expected
        sig = diff / std

        if sig < sigma or night_kwh < 0.05:
            continue

        # Find the hour of peak night consumption for a meaningful timestamp
        day_night = df[(df.index.date == (day.date() if hasattr(day, 'date') else day)) & night_mask]
        if len(day_night) and "energy_kwh" in day_night.columns:
            peak_idx = day_night["energy_kwh"].idxmax()
            ts = peak_idx.to_pydatetime() if hasattr(peak_idx, 'to_pydatetime') else pd.Timestamp(day).to_pydatetime()
        else:
            ts = pd.Timestamp(day).to_pydatetime()
        total = float(total_daily.at[day]) if day in total_daily.index else 0.0
        events.append(
            AnomalyEvent(
                event_id=_deterministic_id(device_key, "night_consumption", ts),
                timestamp=ts,
                device_key=device_key,
                device_name=device_name,
                anomaly_type="night_consumption",
                value=night_kwh,
                expected_mean=mean * total if total else mean,
                expected_std=std,
                sigma_count=sig,
                description=(
                    f"Night {night_kwh:.2f} kWh ({val*100:.0f}% of daily, "
                    f"{sig:.1f}σ above norm {mean*100:.0f}%)"
                ),
            )
        )

    return events


def _check_power_peak_time(
    df: pd.DataFrame,
    device_key: str,
    device_name: str,
    sigma: float,
    window_days: int,
) -> List[AnomalyEvent]:
    """Detect power peaks occurring at unusual hours."""
    events: List[AnomalyEvent] = []

    if "total_power" not in df.columns:
        return events

    hourly_max = df["total_power"].resample("h").max()
    if len(hourly_max) < 24:
        return events

    # Peak hour for each day
    daily_peak_idx = hourly_max.groupby(hourly_max.index.date).idxmax()
    if len(daily_peak_idx) < 7:
        return events

    peak_hours = pd.Series(
        [
            pd.Timestamp(idx).hour if not isinstance(idx, float) else math.nan
            for idx in daily_peak_idx.values
        ],
        index=daily_peak_idx.index,
        dtype="float64",
    ).dropna()

    if len(peak_hours) < 7:
        return events

    win = min(window_days, len(peak_hours) - 1)
    rolling_mean = peak_hours.rolling(window=win, min_periods=7).mean()
    rolling_std = peak_hours.rolling(window=win, min_periods=7).std()

    for day in peak_hours.index[-7:]:
        val = float(peak_hours.at[day]) if day in peak_hours.index else math.nan
        mean = float(rolling_mean.at[day]) if day in rolling_mean.index else math.nan
        std = float(rolling_std.at[day]) if day in rolling_std.index else math.nan

        if math.isnan(val) or math.isnan(mean) or math.isnan(std) or std <= 0:
            continue

        # Circular distance around 24 h
        diff = min(abs(val - mean), 24.0 - abs(val - mean))
        sig = diff / std

        if sig < sigma:
            continue

        # Peak power value for that day — use actual peak timestamp
        day_data = df[df.index.date == day]
        peak_val = float(day_data["total_power"].max()) if len(day_data) else 0.0
        if len(day_data):
            peak_idx = day_data["total_power"].idxmax()
            ts = peak_idx.to_pydatetime() if hasattr(peak_idx, 'to_pydatetime') else pd.Timestamp(day).to_pydatetime()
        else:
            ts = pd.Timestamp(day).to_pydatetime()

        events.append(
            AnomalyEvent(
                event_id=_deterministic_id(device_key, "power_peak_time", ts),
                timestamp=ts,
                device_key=device_key,
                device_name=device_name,
                anomaly_type="power_peak_time",
                value=peak_val,
                expected_mean=mean,
                expected_std=std,
                sigma_count=sig,
                description=(
                    f"Peak at {int(val):02d}:00 ({sig:.1f}σ from usual "
                    f"{mean:.0f}:00), max {peak_val:.0f} W"
                ),
            )
        )

    return events
