"""Energy consumption forecasting using linear regression + seasonal decomposition."""
from __future__ import annotations

import datetime
import logging
import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class ForecastResult:
    """Result of a consumption forecast."""
    device_key: str
    device_name: str
    # Historical daily kWh (for chart)
    history_dates: List[datetime.date] = field(default_factory=list)
    history_kwh: List[float] = field(default_factory=list)
    # Forecast daily kWh
    forecast_dates: List[datetime.date] = field(default_factory=list)
    forecast_kwh: List[float] = field(default_factory=list)
    # Confidence bands (±1σ)
    forecast_upper: List[float] = field(default_factory=list)
    forecast_lower: List[float] = field(default_factory=list)
    # Summary
    avg_daily_kwh: float = 0.0
    trend_pct_per_month: float = 0.0  # +2.5 = rising 2.5%/month
    forecast_next_month_kwh: float = 0.0
    forecast_next_month_cost: float = 0.0
    forecast_year_kwh: float = 0.0
    forecast_year_cost: float = 0.0
    # Seasonal pattern (hour → relative factor, 1.0 = average)
    hourly_profile: Dict[int, float] = field(default_factory=dict)
    # Weekday pattern (0=Mon → relative factor)
    weekday_profile: Dict[int, float] = field(default_factory=dict)


def compute_forecast(
    db,
    device_key: str,
    device_name: str,
    horizon_days: int = 30,
    price_eur_per_kwh: float = 0.3265,
    history_days: int = 90,
) -> Optional[ForecastResult]:
    """Compute consumption forecast for a single device.

    Uses linear regression on daily kWh with weekday seasonality adjustment.
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    start_ts = int((now - datetime.timedelta(days=history_days)).timestamp())
    end_ts = int(now.timestamp())

    # Query hourly data (much faster than raw samples)
    df = db.query_hourly(device_key, start_ts=start_ts, end_ts=end_ts)

    # Fallback to samples if hourly is empty
    if df.empty or len(df) < 6:
        try:
            samples = db.query_samples(device_key, start_ts=start_ts, end_ts=end_ts)
            if samples is not None and not samples.empty and "energy_kwh" in samples.columns:
                samples["hour_ts"] = (pd.to_datetime(samples["timestamp"]).astype(int) // 10**9 // 3600) * 3600
                df = samples.groupby("hour_ts").agg(kwh=("energy_kwh", "sum")).reset_index()
                df["kwh"] = df["kwh"].fillna(0)
        except Exception:
            pass

    if df.empty or len(df) < 6:
        return None

    # Convert hour_ts → datetime and aggregate to daily
    df["dt"] = pd.to_datetime(df["hour_ts"], unit="s", utc=True)
    df["date"] = df["dt"].dt.date
    daily = df.groupby("date").agg(kwh=("kwh", "sum")).reset_index()
    daily = daily.sort_values("date").reset_index(drop=True)

    if len(daily) < 3:
        return None

    # Remove partial first/last day
    if len(daily) > 2:
        daily = daily.iloc[1:-1].reset_index(drop=True)

    dates = daily["date"].tolist()
    kwh_vals = daily["kwh"].values.astype(float)

    # --- Linear regression ---
    x = np.arange(len(kwh_vals), dtype=float)
    mean_x, mean_y = x.mean(), kwh_vals.mean()
    ss_xx = ((x - mean_x) ** 2).sum()
    ss_xy = ((x - mean_x) * (kwh_vals - mean_y)).sum()

    if ss_xx > 0:
        slope = ss_xy / ss_xx
        intercept = mean_y - slope * mean_x
    else:
        slope = 0.0
        intercept = mean_y

    # Residual std for confidence bands
    predicted = intercept + slope * x
    residuals = kwh_vals - predicted
    sigma = float(np.std(residuals)) if len(residuals) > 2 else 0.0

    # Trend as %/month
    if mean_y > 0:
        trend_pct = (slope * 30.0 / mean_y) * 100.0
    else:
        trend_pct = 0.0

    # --- Weekday seasonality ---
    weekday_factors: Dict[int, float] = {}
    if len(daily) >= 14:
        daily["weekday"] = [d.weekday() for d in dates]
        daily["kwh_val"] = kwh_vals
        wd_means = daily.groupby("weekday")["kwh_val"].mean()
        overall_mean = kwh_vals.mean()
        if overall_mean > 0:
            for wd in range(7):
                weekday_factors[wd] = float(wd_means.get(wd, overall_mean) / overall_mean)

    # --- Hourly profile ---
    hourly_profile: Dict[int, float] = {}
    df["hour"] = df["dt"].dt.hour
    h_means = df.groupby("hour")["kwh"].mean()
    h_overall = h_means.mean()
    if h_overall > 0:
        for h in range(24):
            hourly_profile[h] = float(h_means.get(h, h_overall) / h_overall)

    # --- Generate forecast ---
    last_date = dates[-1]
    n_history = len(kwh_vals)
    forecast_dates: List[datetime.date] = []
    forecast_kwh: List[float] = []
    forecast_upper: List[float] = []
    forecast_lower: List[float] = []

    for i in range(1, horizon_days + 1):
        fd = last_date + datetime.timedelta(days=i)
        fx = n_history + i - 1
        base = intercept + slope * fx
        # Apply weekday factor
        wd = fd.weekday()
        factor = weekday_factors.get(wd, 1.0)
        val = max(0.0, base * factor)
        forecast_dates.append(fd)
        forecast_kwh.append(val)
        forecast_upper.append(max(0.0, val + 1.5 * sigma))
        forecast_lower.append(max(0.0, val - 1.5 * sigma))

    # Summary calculations
    forecast_month_kwh = sum(forecast_kwh[:30]) if len(forecast_kwh) >= 30 else sum(forecast_kwh)
    avg_daily = float(kwh_vals.mean())
    forecast_year_kwh = avg_daily * 365  # avg_daily already includes trend

    return ForecastResult(
        device_key=device_key,
        device_name=device_name,
        history_dates=dates,
        history_kwh=kwh_vals.tolist(),
        forecast_dates=forecast_dates,
        forecast_kwh=forecast_kwh,
        forecast_upper=forecast_upper,
        forecast_lower=forecast_lower,
        avg_daily_kwh=avg_daily,
        trend_pct_per_month=trend_pct,
        forecast_next_month_kwh=forecast_month_kwh,
        forecast_next_month_cost=forecast_month_kwh * price_eur_per_kwh,
        forecast_year_kwh=max(0.0, forecast_year_kwh),
        forecast_year_cost=max(0.0, forecast_year_kwh * price_eur_per_kwh),
        hourly_profile=hourly_profile,
        weekday_profile=weekday_factors,
    )


def compute_all_forecasts(
    db,
    devices: list,
    horizon_days: int = 30,
    price_eur_per_kwh: float = 0.3265,
    history_days: int = 90,
) -> List[ForecastResult]:
    """Compute forecasts for all configured devices."""
    results = []
    for dev in devices:
        try:
            r = compute_forecast(
                db, dev.key, dev.name,
                horizon_days=horizon_days,
                price_eur_per_kwh=price_eur_per_kwh,
                history_days=history_days,
            )
            if r is not None:
                results.append(r)
        except Exception:
            logger.debug("Forecast failed for %s", dev.key, exc_info=True)
    return results
