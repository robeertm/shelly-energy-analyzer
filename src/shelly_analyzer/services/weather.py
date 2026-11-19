"""Weather data for the weather tab and the consumption correlation.

Three sources, picked in Settings → Weather (``weather.source``):

* ``open_meteo`` — Open-Meteo, no account and no key (default). Also fills the
  last 30 days hour by hour, so the correlation has data from the first day.
* ``met_no`` — MET Norway Locationforecast 2.0, no account and no key. Their
  terms ask for an identifying User-Agent with a contact and for caching;
  both are done here (the contact is the public project page).
* ``openweathermap`` — OpenWeatherMap, needs an API key.

Every source returns the same :class:`WeatherSnapshot`, so storage, the stale
fallback and the correlation do not care where the numbers came from.
"""
from __future__ import annotations

import datetime
import json
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

# OpenWeatherMap free tier: 1000 calls/day, 60 calls/min
_OWM_BASE = "https://api.openweathermap.org/data/2.5"
_OWM_ONECALL = "https://api.openweathermap.org/data/3.0/onecall"
_OPEN_METEO = "https://api.open-meteo.com/v1/forecast"
_MET_NO = "https://api.met.no/weatherapi/locationforecast/2.0/compact"
_PROJECT_URL = "https://github.com/robeertm/shelly-energy-analyzer"

SOURCES = ("open_meteo", "met_no", "openweathermap")
# Shown under the weather tab. Open-Meteo and MET Norway publish under
# CC BY 4.0, which asks for the source to be named.
SOURCE_CREDITS = {
    "open_meteo": "Open-Meteo (CC BY 4.0)",
    "met_no": "MET Norway (CC BY 4.0)",
    "openweathermap": "OpenWeatherMap",
}
# MET Norway asks clients not to ask more often than every 30 minutes.
MET_NO_MIN_INTERVAL_S = 1800

# One condition vocabulary for both keyless sources. The English text is what
# lands in the database (``description``); the key is used for translation.
CONDITION_TEXT: Dict[str, str] = {
    "clear": "clear sky",
    "mainly_clear": "mainly clear",
    "partly_cloudy": "partly cloudy",
    "overcast": "overcast",
    "fog": "fog",
    "drizzle": "drizzle",
    "rain": "rain",
    "freezing_rain": "freezing rain",
    "sleet": "sleet",
    "snow": "snow",
    "showers": "rain showers",
    "snow_showers": "snow showers",
    "thunderstorm": "thunderstorm",
}
_TEXT_TO_CONDITION = {v: k for k, v in CONDITION_TEXT.items()}

# WMO weather interpretation codes (Open-Meteo ``weather_code``).
_WMO: Dict[int, str] = {
    0: "clear", 1: "mainly_clear", 2: "partly_cloudy", 3: "overcast",
    45: "fog", 48: "fog",
    51: "drizzle", 53: "drizzle", 55: "drizzle",
    56: "freezing_rain", 57: "freezing_rain",
    61: "rain", 63: "rain", 65: "rain",
    66: "freezing_rain", 67: "freezing_rain",
    71: "snow", 73: "snow", 75: "snow", 77: "snow",
    80: "showers", 81: "showers", 82: "showers",
    85: "snow_showers", 86: "snow_showers",
    95: "thunderstorm", 96: "thunderstorm", 99: "thunderstorm",
}

_CACHE_LOCK = threading.Lock()
_CURRENT_CACHE: Dict[Tuple[str, float, float], Tuple[float, "WeatherSnapshot"]] = {}
_HISTORY_DONE: Dict[Tuple[float, float], float] = {}


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


def condition_key(description: str) -> str:
    """Condition key for a stored English description, ``""`` if unknown."""
    return _TEXT_TO_CONDITION.get(str(description or "").strip().lower(), "")


def _wmo_text(code: Any) -> str:
    try:
        key = _WMO.get(int(code), "")
    except (TypeError, ValueError):
        key = ""
    return CONDITION_TEXT.get(key, "")


def _metno_text(symbol_code: str) -> str:
    """MET Norway ``symbol_code`` (e.g. ``lightrainshowers_day``) → description."""
    sym = str(symbol_code or "").split("_")[0].lower()
    if not sym:
        return ""
    if "thunder" in sym:
        key = "thunderstorm"
    elif "sleet" in sym:
        key = "sleet"
    elif "snow" in sym:
        key = "snow_showers" if "showers" in sym else "snow"
    elif "rain" in sym:
        key = "showers" if "showers" in sym else "rain"
    elif sym == "fog":
        key = "fog"
    elif sym == "clearsky":
        key = "clear"
    elif sym == "fair":
        key = "mainly_clear"
    elif sym == "partlycloudy":
        key = "partly_cloudy"
    elif sym == "cloudy":
        key = "overcast"
    else:
        return sym
    return CONDITION_TEXT[key]


def _iso_to_ts(value: Any) -> Optional[int]:
    try:
        dt = datetime.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return int(dt.timestamp())


def _num(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def user_agent() -> str:
    """Identifying User-Agent with a contact, as MET Norway requires."""
    try:
        from shelly_analyzer import __version__ as ver
    except Exception:
        ver = "0"
    return f"shelly-energy-analyzer/{ver} (+{_PROJECT_URL})"


def fetch_current_openmeteo(lat: float, lon: float) -> Optional[WeatherSnapshot]:
    """Current conditions from Open-Meteo (no key)."""
    try:
        resp = requests.get(
            _OPEN_METEO,
            params={
                "latitude": round(float(lat), 4),
                "longitude": round(float(lon), 4),
                "current": "temperature_2m,relative_humidity_2m,apparent_temperature,"
                           "pressure_msl,cloud_cover,wind_speed_10m,weather_code",
                "wind_speed_unit": "ms",
                "timezone": "UTC",
            },
            headers={"User-Agent": user_agent()},
            timeout=10,
        )
        resp.raise_for_status()
        cur = resp.json().get("current") or {}
        if cur.get("temperature_2m") is None:
            return None
        temp = _num(cur.get("temperature_2m"))
        return WeatherSnapshot(
            timestamp=_iso_to_ts(cur.get("time")) or int(time.time()),
            temp_c=temp,
            humidity_pct=_num(cur.get("relative_humidity_2m")),
            wind_speed_ms=_num(cur.get("wind_speed_10m")),
            clouds_pct=_num(cur.get("cloud_cover")),
            description=_wmo_text(cur.get("weather_code")),
            pressure_hpa=_num(cur.get("pressure_msl")),
            feels_like_c=_num(cur.get("apparent_temperature"), temp),
        )
    except Exception:
        logger.debug("Open-Meteo current weather failed", exc_info=True)
        return None


def fetch_history_openmeteo(lat: float, lon: float, past_days: int = 30) -> List[WeatherSnapshot]:
    """Hourly observations of the last ``past_days`` days from Open-Meteo.

    Only hours that have already happened are returned, so a forecast never
    ends up in the correlation.
    """
    out: List[WeatherSnapshot] = []
    try:
        resp = requests.get(
            _OPEN_METEO,
            params={
                "latitude": round(float(lat), 4),
                "longitude": round(float(lon), 4),
                "hourly": "temperature_2m,relative_humidity_2m,pressure_msl,"
                          "cloud_cover,wind_speed_10m,weather_code",
                "past_days": max(1, min(int(past_days), 92)),
                "forecast_days": 1,
                "wind_speed_unit": "ms",
                "timezone": "UTC",
            },
            headers={"User-Agent": user_agent()},
            timeout=20,
        )
        resp.raise_for_status()
        h = resp.json().get("hourly") or {}
        now = time.time()

        def col(name: str, i: int) -> Any:
            vals = h.get(name) or []
            return vals[i] if i < len(vals) else None

        for i, t in enumerate(h.get("time") or []):
            ts = _iso_to_ts(t)
            temp = col("temperature_2m", i)
            if ts is None or ts > now or temp is None:
                continue
            out.append(WeatherSnapshot(
                timestamp=(ts // 3600) * 3600,
                temp_c=_num(temp),
                humidity_pct=_num(col("relative_humidity_2m", i)),
                wind_speed_ms=_num(col("wind_speed_10m", i)),
                clouds_pct=_num(col("cloud_cover", i)),
                description=_wmo_text(col("weather_code", i)),
                pressure_hpa=_num(col("pressure_msl", i)),
                feels_like_c=_num(temp),
            ))
    except Exception:
        logger.debug("Open-Meteo weather history failed", exc_info=True)
        return []
    return out


def history_backfill_due(lat: float, lon: float, every_s: int = 6 * 3600) -> bool:
    """True at most once per ``every_s`` per location (and marks it as done)."""
    key = (round(float(lat), 4), round(float(lon), 4))
    now = time.time()
    with _CACHE_LOCK:
        last = _HISTORY_DONE.get(key, 0.0)
        if now - last < every_s:
            return False
        _HISTORY_DONE[key] = now
        return True


def fetch_current_metno(lat: float, lon: float) -> Optional[WeatherSnapshot]:
    """Current conditions from MET Norway Locationforecast 2.0 (no key).

    MET Norway blocks clients without an identifying User-Agent and asks for
    coordinates with at most four decimals.
    """
    try:
        resp = requests.get(
            _MET_NO,
            params={"lat": round(float(lat), 4), "lon": round(float(lon), 4)},
            headers={"User-Agent": user_agent()},
            timeout=10,
        )
        resp.raise_for_status()
        series = ((resp.json().get("properties") or {}).get("timeseries")) or []
        if not series:
            return None
        now = time.time()
        # The first entry is normally the current hour; skip anything that is
        # already more than an hour old.
        entry = series[0]
        for e in series:
            ts_e = _iso_to_ts(e.get("time"))
            if ts_e is not None and ts_e + 3600 > now:
                entry = e
                break
        data = entry.get("data") or {}
        det = ((data.get("instant") or {}).get("details")) or {}
        if det.get("air_temperature") is None:
            return None
        temp = _num(det.get("air_temperature"))
        sym = ""
        for nxt in ("next_1_hours", "next_6_hours", "next_12_hours"):
            sym = (((data.get(nxt) or {}).get("summary")) or {}).get("symbol_code") or ""
            if sym:
                break
        return WeatherSnapshot(
            timestamp=_iso_to_ts(entry.get("time")) or int(now),
            temp_c=temp,
            humidity_pct=_num(det.get("relative_humidity")),
            wind_speed_ms=_num(det.get("wind_speed")),
            clouds_pct=_num(det.get("cloud_area_fraction")),
            description=_metno_text(sym),
            pressure_hpa=_num(det.get("air_pressure_at_sea_level")),
            feels_like_c=_num(det.get("apparent_air_temperature"), temp),
        )
    except Exception:
        logger.debug("MET Norway current weather failed", exc_info=True)
        return None


def fetch_current(
    source: str, api_key: str, lat: float, lon: float, min_interval_s: int = 0,
) -> Optional[WeatherSnapshot]:
    """Current conditions from the configured source.

    A successful answer is reused for ``min_interval_s`` seconds (MET Norway:
    never less than 30 minutes). OpenWeatherMap without a key sends nothing.
    """
    src = source if source in SOURCES else "open_meteo"
    if src == "openweathermap" and not str(api_key or "").strip():
        return None
    ttl = max(0, int(min_interval_s or 0))
    if src == "met_no":
        ttl = max(ttl, MET_NO_MIN_INTERVAL_S)
    key = (src, round(float(lat), 4), round(float(lon), 4))
    now = time.time()
    if ttl:
        with _CACHE_LOCK:
            hit = _CURRENT_CACHE.get(key)
        if hit and now - hit[0] < ttl:
            return hit[1]
    if src == "met_no":
        snap = fetch_current_metno(lat, lon)
    elif src == "openweathermap":
        snap = fetch_current_weather(api_key, lat, lon)
    else:
        snap = fetch_current_openmeteo(lat, lon)
    if snap is not None:
        with _CACHE_LOCK:
            _CURRENT_CACHE[key] = (now, snap)
    return snap


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
