"""Weather data integration via OpenWeatherMap API for consumption correlation."""
from __future__ import annotations

import datetime
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

# OpenWeatherMap free tier: 1000 calls/day, 60 calls/min
_OWM_BASE = "https://api.openweathermap.org/data/2.5"
_OWM_ONECALL = "https://api.openweathermap.org/data/3.0/onecall"


@dataclass
class WeatherSnapshot:
    """A single weather observation."""
    timestamp: int  # Unix seconds
    temp_c: float
    humidity_pct: float
    wind_speed_ms: float
    clouds_pct: float
    description: str = ""
    icon: str = ""
    pressure_hpa: float = 0.0
    feels_like_c: float = 0.0


@dataclass
class WeatherCorrelation:
    """Result of weather–energy correlation analysis."""
    device_key: str
    device_name: str
    # Hourly paired data
    hours: List[datetime.datetime] = field(default_factory=list)
    temps: List[float] = field(default_factory=list)
    kwh_vals: List[float] = field(default_factory=list)
    # Correlation coefficient (Pearson r)
    r_temp_kwh: float = 0.0
    # Heating/cooling degree day analysis
    hdd_base_c: float = 18.0  # Heating degree day base
    cdd_base_c: float = 22.0  # Cooling degree day base
    hdd_total: float = 0.0
    cdd_total: float = 0.0
    kwh_per_hdd: float = 0.0
    kwh_per_cdd: float = 0.0
    # Summary
    avg_temp: float = 0.0
    total_kwh: float = 0.0
    period_days: int = 0


def fetch_current_weather(api_key: str, lat: float, lon: float) -> Optional[WeatherSnapshot]:
    """Fetch current weather from OpenWeatherMap."""
    try:
        resp = requests.get(
            f"{_OWM_BASE}/weather",
            params={"lat": lat, "lon": lon, "appid": api_key, "units": "metric"},
            timeout=10,
        )
        resp.raise_for_status()
        d = resp.json()
        main = d.get("main", {})
        wind = d.get("wind", {})
        clouds = d.get("clouds", {})
        weather = d.get("weather", [{}])[0] if d.get("weather") else {}
        return WeatherSnapshot(
            timestamp=int(d.get("dt", time.time())),
            temp_c=float(main.get("temp", 0)),
            humidity_pct=float(main.get("humidity", 0)),
            wind_speed_ms=float(wind.get("speed", 0)),
            clouds_pct=float(clouds.get("all", 0)),
            description=str(weather.get("description", "")),
            icon=str(weather.get("icon", "")),
            pressure_hpa=float(main.get("pressure", 0)),
            feels_like_c=float(main.get("feels_like", 0)),
        )
    except Exception:
        logger.debug("Failed to fetch weather", exc_info=True)
        return None


def fetch_weather_history(
    api_key: str, lat: float, lon: float, days: int = 7
) -> List[WeatherSnapshot]:
    """Fetch historical weather (last N days) using OneCall timemachine.

    Falls back to 5-day forecast data if OneCall is not available.
    """
    snapshots: List[WeatherSnapshot] = []
    now = int(time.time())

    # Try using the forecast/history endpoint (free tier)
    try:
        resp = requests.get(
            f"{_OWM_BASE}/forecast",
            params={"lat": lat, "lon": lon, "appid": api_key, "units": "metric"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        for item in data.get("list", []):
            main = item.get("main", {})
            wind = item.get("wind", {})
            clouds = item.get("clouds", {})
            weather = item.get("weather", [{}])[0] if item.get("weather") else {}
            snapshots.append(WeatherSnapshot(
                timestamp=int(item.get("dt", 0)),
                temp_c=float(main.get("temp", 0)),
                humidity_pct=float(main.get("humidity", 0)),
                wind_speed_ms=float(wind.get("speed", 0)),
                clouds_pct=float(clouds.get("all", 0)),
                description=str(weather.get("description", "")),
                icon=str(weather.get("icon", "")),
                pressure_hpa=float(main.get("pressure", 0)),
                feels_like_c=float(main.get("feels_like", 0)),
            ))
    except Exception:
        logger.debug("Weather history fetch failed", exc_info=True)

    return snapshots


def geocode_city(api_key: str, city: str) -> Optional[Tuple[float, float, str]]:
    """Resolve city name to (lat, lon, display_name)."""
    try:
        resp = requests.get(
            "https://api.openweathermap.org/geo/1.0/direct",
            params={"q": city, "appid": api_key, "limit": 1},
            timeout=10,
        )
        resp.raise_for_status()
        results = resp.json()
        if results:
            r = results[0]
            name = f"{r.get('name', city)}, {r.get('country', '')}"
            return float(r["lat"]), float(r["lon"]), name
    except Exception:
        logger.debug("Geocode failed for %s", city, exc_info=True)
    return None


def stale_current_from_weather_df(weather_df) -> Optional[Tuple[Dict[str, Any], int]]:
    """Build a ``current``-shaped dict from the newest stored observation.

    Used as a fallback when the live OpenWeatherMap call fails or is rate-limited
    so the weather tab always shows *something* (older data flagged as stale)
    instead of a blank hero. ``weather_df`` is expected ordered by ``hour_ts``
    ascending (as returned by ``Database.query_weather``); the last row is newest.

    Returns ``(current_data, as_of_ts)`` or ``None`` when there is no usable row.
    """
    if weather_df is None or getattr(weather_df, "empty", True):
        return None
    last = weather_df.iloc[-1]
    cols = weather_df.columns

    def _fnum(col: str) -> Optional[float]:
        if col not in cols:
            return None
        v = last[col]
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    temp = _fnum("temp_c")
    if temp is None:
        return None
    desc = ""
    if "description" in cols and last["description"] is not None:
        desc = str(last["description"])
    current = {
        "temp_c": temp,
        "feels_like_c": temp,
        "humidity_pct": _fnum("humidity_pct"),
        "wind_speed_ms": _fnum("wind_speed_ms"),
        "clouds_pct": _fnum("clouds_pct"),
        "pressure_hpa": _fnum("pressure_hpa") or 0,
        "description": desc,
    }
    try:
        as_of = int(last["hour_ts"])
    except (TypeError, ValueError, KeyError):
        as_of = 0
    return current, as_of


def correlate_weather_energy(
    db,
    device_key: str,
    device_name: str,
    weather_rows: List[Dict[str, Any]],
    days: int = 30,
) -> Optional[WeatherCorrelation]:
    """Correlate weather data with energy consumption.

    weather_rows: list of dicts with keys: timestamp, temp_c, humidity_pct, etc.
    """
    import numpy as np

    if not weather_rows:
        return None

    now = datetime.datetime.now(datetime.timezone.utc)
    start_ts = int((now - datetime.timedelta(days=days)).timestamp())
    end_ts = int(now.timestamp())

    # Get hourly energy
    hourly = db.query_hourly(device_key, start_ts=start_ts, end_ts=end_ts)
    if hourly.empty or len(hourly) < 24:
        return None

    # Build weather lookup (hour_ts → temp)
    weather_by_hour: Dict[int, float] = {}
    for w in weather_rows:
        h_ts = (int(w["timestamp"]) // 3600) * 3600
        weather_by_hour[h_ts] = float(w["temp_c"])

    # Match hours
    matched_hours = []
    matched_temps = []
    matched_kwh = []

    for _, row in hourly.iterrows():
        h_ts = int(row["hour_ts"])
        if h_ts in weather_by_hour:
            matched_hours.append(datetime.datetime.fromtimestamp(h_ts, tz=datetime.timezone.utc))
            matched_temps.append(weather_by_hour[h_ts])
            matched_kwh.append(float(row["kwh"]))

    if len(matched_temps) < 24:
        return None

    temps_arr = np.array(matched_temps)
    kwh_arr = np.array(matched_kwh)

    # Pearson correlation
    r_val = 0.0
    if len(temps_arr) > 2:
        t_std = np.std(temps_arr)
        k_std = np.std(kwh_arr)
        if t_std > 0 and k_std > 0:
            r_val = float(np.corrcoef(temps_arr, kwh_arr)[0, 1])

    # Degree days
    hdd_base = 18.0
    cdd_base = 22.0
    hdd_total = sum(max(0.0, hdd_base - t) / 24.0 for t in matched_temps)
    cdd_total = sum(max(0.0, t - cdd_base) / 24.0 for t in matched_temps)
    total_kwh = float(kwh_arr.sum())

    kwh_per_hdd = total_kwh / hdd_total if hdd_total > 1.0 else 0.0
    kwh_per_cdd = total_kwh / cdd_total if cdd_total > 1.0 else 0.0

    period_days = max(1, (end_ts - start_ts) // 86400)

    return WeatherCorrelation(
        device_key=device_key,
        device_name=device_name,
        hours=matched_hours,
        temps=matched_temps,
        kwh_vals=matched_kwh,
        r_temp_kwh=r_val,
        hdd_base_c=hdd_base,
        cdd_base_c=cdd_base,
        hdd_total=hdd_total,
        cdd_total=cdd_total,
        kwh_per_hdd=kwh_per_hdd,
        kwh_per_cdd=kwh_per_cdd,
        avg_temp=float(temps_arr.mean()),
        total_kwh=total_kwh,
        period_days=period_days,
    )
