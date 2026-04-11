"""CO2 forecast service: trend-based baseline + Open-Meteo weather adjustment.

Computes a 6h-ahead forecast of grid CO2 intensity per zone:
1. Baseline = median CO2 intensity per hour-of-day over the last 14 days
   of actual (non-estimated) data. This follows the zone's real trend.
2. Weather adjustment via Open-Meteo (free, no API key, global):
   strong wind → lower, clear sky + daylight → lower, cold → higher,
   hot → slightly higher, rain → lower in hydro-heavy zones.
3. Per-zone generation-mix profile weights how strongly each factor
   applies (e.g. wind matters more in DK/AU-SA than in CH/NO).

Runs hourly in the background. No DB writes — cache is in-memory and
exposed via /api/co2_live.
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

_OPEN_METEO = "https://api.open-meteo.com/v1/forecast"
_HORIZON_HOURS = 6

# Approximate population/load-weighted centroid per zone
_ZONE_CENTROIDS: Dict[str, Tuple[float, float]] = {
    # ── ENTSO-E (underscore form) ─────────────────────────────────────
    "DE_LU": (51.1, 10.4), "DE_AT_LU": (49.5, 12.0),
    "AT": (47.6, 14.6), "BE": (50.5, 4.5), "CH": (46.8, 8.2),
    "CZ": (49.8, 15.5), "DK_1": (56.0, 9.0), "DK_2": (55.0, 11.0),
    "ES": (40.0, -4.0), "FI": (63.0, 26.0), "FR": (46.2, 2.2),
    "GR": (39.0, 22.0), "HR": (45.1, 15.2), "HU": (47.0, 19.0),
    "IE_SEM": (53.4, -7.9),
    "IT_NORD": (45.5, 9.0), "IT_CNOR": (43.0, 11.0), "IT_CSUD": (41.5, 13.5),
    "IT_SUD": (40.0, 16.0), "IT_SICI": (37.5, 14.0), "IT_SARD": (40.0, 9.0),
    "IT_CALA": (39.0, 16.5),
    "NL": (52.1, 5.3),
    "NO_1": (59.9, 10.7), "NO_2": (58.5, 6.5), "NO_3": (63.4, 10.4),
    "NO_4": (68.0, 16.0), "NO_5": (60.4, 5.3),
    "PL": (52.0, 19.0), "PT": (39.4, -8.2), "RO": (45.9, 25.0),
    "SE_1": (67.0, 19.0), "SE_2": (64.0, 17.0), "SE_3": (59.5, 15.5),
    "SE_4": (56.5, 13.5),
    "SI": (46.0, 14.8), "SK": (48.7, 19.7),
    "GB": (54.0, -2.0), "UK": (54.0, -2.0), "IE": (53.4, -7.9),
    "BG": (42.7, 25.5), "EE": (58.6, 25.0), "LV": (56.9, 24.6),
    "LT": (55.2, 23.9), "RS": (44.0, 21.0), "AL": (41.2, 20.0),
    "BA": (43.9, 17.7), "ME": (42.7, 19.4), "MK": (41.6, 21.7),
    "XK": (42.6, 20.9), "MD": (47.4, 28.4), "CY": (35.1, 33.4),
    # ── Electricity Maps (hyphen form) ────────────────────────────────
    "DE": (51.1, 10.4),
    "US-CAL-CISO": (36.8, -119.4), "US-CAR-CPLE": (35.6, -79.3),
    "US-CAR-DUK": (35.5, -80.5), "US-CENT-SPP": (39.0, -98.0),
    "US-FLA-FPL": (27.9, -81.7), "US-MIDA-PJM": (39.9, -77.0),
    "US-MIDW-MISO": (43.0, -89.4), "US-NE-ISNE": (42.4, -71.6),
    "US-NW-BPAT": (45.5, -122.6), "US-NY-NYIS": (42.9, -75.5),
    "US-SE-SOCO": (32.8, -86.0), "US-SW-SRP": (33.5, -112.0),
    "US-TEN-TVA": (35.7, -86.7), "US-TEX-ERCO": (31.0, -100.0),
    "US-HI": (20.8, -156.3), "US-AK": (61.4, -149.9),
    "AU-NSW": (-32.0, 147.0), "AU-QLD": (-22.0, 144.0),
    "AU-SA": (-32.0, 135.0), "AU-TAS": (-42.0, 147.0),
    "AU-VIC": (-37.0, 144.0), "AU-WA": (-26.0, 121.0),
    "AU-NT": (-19.5, 133.0),
    "CA-ON": (50.0, -85.0), "CA-QC": (52.0, -72.0), "CA-BC": (54.0, -125.0),
    "CA-AB": (54.0, -115.0), "CA-SK": (54.0, -106.0), "CA-MB": (55.0, -98.0),
    "CA-NS": (45.0, -63.0), "CA-NB": (46.5, -66.5), "CA-NL": (53.5, -60.0),
    "CA-PE": (46.4, -63.2), "CA-YT": (64.0, -135.0), "CA-NT": (64.8, -124.0),
    "CA-NU": (70.3, -83.1),
    "JP-HKD": (43.0, 142.0), "JP-TH": (38.7, 140.7), "JP-TK": (36.2, 139.7),
    "JP-CB": (35.5, 137.0), "JP-KN": (34.7, 135.5), "JP-CG": (34.4, 133.0),
    "JP-SK": (33.7, 133.5), "JP-KY": (32.5, 131.0), "JP-ON": (26.2, 127.7),
    "JP": (36.2, 138.3),
    "CN": (35.0, 103.0), "IN": (20.6, 78.9), "KR": (36.5, 127.8),
    "TW": (23.7, 121.0), "SG": (1.35, 103.8), "HK": (22.3, 114.2),
    "TH": (15.0, 101.0), "VN": (14.1, 108.3), "ID": (-2.5, 118.0),
    "MY": (4.2, 101.9), "PH": (13.0, 122.0), "BD": (23.7, 90.4),
    "PK": (30.4, 69.3), "NP": (28.4, 84.1), "LK": (7.9, 80.8),
    "KZ": (48.0, 68.0), "UZ": (41.4, 64.6),
    "BR": (-14.2, -51.9), "AR": (-38.4, -63.6), "CL": (-35.7, -71.5),
    "PE": (-9.2, -75.0), "CO": (4.6, -74.1), "VE": (6.4, -66.6),
    "UY": (-32.5, -55.8), "EC": (-1.8, -78.2), "BO": (-16.3, -63.6),
    "PY": (-23.4, -58.4), "GT": (15.8, -90.2), "PA": (8.5, -80.8),
    "CR": (9.7, -83.8), "DO": (18.7, -70.2), "CU": (21.5, -77.8),
    "MX": (23.6, -102.6),
    "RU": (61.5, 105.3), "TR": (39.0, 35.0), "UA": (48.4, 31.2),
    "IL": (31.0, 34.9), "JO": (30.6, 36.2), "LB": (33.9, 35.9),
    "AE": (23.4, 53.8), "SA": (23.9, 45.1), "KW": (29.3, 47.5),
    "QA": (25.3, 51.2), "OM": (21.5, 55.9), "BH": (26.0, 50.6),
    "IR": (32.4, 53.7), "IQ": (33.2, 43.7),
    "EG": (26.8, 30.8), "ZA": (-30.6, 22.9), "NG": (9.1, 8.7),
    "KE": (-0.0, 37.9), "MA": (31.8, -7.1), "DZ": (28.0, 1.7),
    "TN": (33.9, 9.5), "LY": (26.3, 17.2), "ET": (9.1, 40.5),
    "GH": (7.9, -1.0), "CI": (7.5, -5.5), "TZ": (-6.4, 34.9),
    "UG": (1.4, 32.3), "ZM": (-13.1, 27.8), "ZW": (-19.0, 29.2),
    "NZ": (-40.9, 174.9), "IS": (64.9, -19.0),
}

# Rough per-zone generation-mix shares (wind, solar, thermal, hydro).
# Used to weight how strongly each weather factor adjusts the baseline.
_ZONE_ENERGY_PROFILE: Dict[str, Dict[str, float]] = {
    "DE_LU":       {"wind": 0.28, "solar": 0.12, "thermal": 0.45, "hydro": 0.04},
    "DE":          {"wind": 0.28, "solar": 0.12, "thermal": 0.45, "hydro": 0.04},
    "DK_1":        {"wind": 0.55, "solar": 0.05, "thermal": 0.25, "hydro": 0.00},
    "DK_2":        {"wind": 0.55, "solar": 0.05, "thermal": 0.25, "hydro": 0.00},
    "FR":          {"wind": 0.08, "solar": 0.04, "thermal": 0.12, "hydro": 0.10},
    "ES":          {"wind": 0.22, "solar": 0.17, "thermal": 0.30, "hydro": 0.13},
    "PT":          {"wind": 0.26, "solar": 0.07, "thermal": 0.28, "hydro": 0.22},
    "NL":          {"wind": 0.18, "solar": 0.16, "thermal": 0.55, "hydro": 0.00},
    "BE":          {"wind": 0.13, "solar": 0.09, "thermal": 0.25, "hydro": 0.02},
    "UK":          {"wind": 0.29, "solar": 0.05, "thermal": 0.40, "hydro": 0.02},
    "GB":          {"wind": 0.29, "solar": 0.05, "thermal": 0.40, "hydro": 0.02},
    "IE_SEM":      {"wind": 0.35, "solar": 0.02, "thermal": 0.55, "hydro": 0.02},
    "IE":          {"wind": 0.35, "solar": 0.02, "thermal": 0.55, "hydro": 0.02},
    "NO_1":        {"wind": 0.05, "solar": 0.00, "thermal": 0.02, "hydro": 0.90},
    "NO_2":        {"wind": 0.10, "solar": 0.00, "thermal": 0.02, "hydro": 0.85},
    "NO_3":        {"wind": 0.05, "solar": 0.00, "thermal": 0.02, "hydro": 0.90},
    "NO_4":        {"wind": 0.08, "solar": 0.00, "thermal": 0.02, "hydro": 0.88},
    "NO_5":        {"wind": 0.02, "solar": 0.00, "thermal": 0.02, "hydro": 0.95},
    "SE_1":        {"wind": 0.12, "solar": 0.01, "thermal": 0.05, "hydro": 0.40},
    "SE_2":        {"wind": 0.15, "solar": 0.01, "thermal": 0.05, "hydro": 0.40},
    "SE_3":        {"wind": 0.18, "solar": 0.02, "thermal": 0.10, "hydro": 0.30},
    "SE_4":        {"wind": 0.25, "solar": 0.02, "thermal": 0.15, "hydro": 0.20},
    "FI":          {"wind": 0.18, "solar": 0.01, "thermal": 0.20, "hydro": 0.20},
    "PL":          {"wind": 0.12, "solar": 0.07, "thermal": 0.75, "hydro": 0.02},
    "IT_NORD":     {"wind": 0.03, "solar": 0.15, "thermal": 0.55, "hydro": 0.18},
    "IT_CNOR":     {"wind": 0.05, "solar": 0.18, "thermal": 0.55, "hydro": 0.15},
    "IT_CSUD":     {"wind": 0.10, "solar": 0.22, "thermal": 0.55, "hydro": 0.08},
    "IT_SUD":      {"wind": 0.20, "solar": 0.25, "thermal": 0.50, "hydro": 0.03},
    "AT":          {"wind": 0.12, "solar": 0.04, "thermal": 0.20, "hydro": 0.60},
    "CH":          {"wind": 0.01, "solar": 0.06, "thermal": 0.05, "hydro": 0.58},
    "CZ":          {"wind": 0.02, "solar": 0.04, "thermal": 0.55, "hydro": 0.02},
    "SK":          {"wind": 0.00, "solar": 0.03, "thermal": 0.18, "hydro": 0.15},
    "HU":          {"wind": 0.02, "solar": 0.18, "thermal": 0.45, "hydro": 0.00},
    "RO":          {"wind": 0.11, "solar": 0.05, "thermal": 0.35, "hydro": 0.28},
    "GR":          {"wind": 0.18, "solar": 0.15, "thermal": 0.55, "hydro": 0.08},
    # US
    "US-TEX-ERCO": {"wind": 0.25, "solar": 0.08, "thermal": 0.55, "hydro": 0.00},
    "US-CAL-CISO": {"wind": 0.07, "solar": 0.17, "thermal": 0.45, "hydro": 0.12},
    "US-MIDW-MISO":{"wind": 0.15, "solar": 0.02, "thermal": 0.70, "hydro": 0.02},
    "US-MIDA-PJM": {"wind": 0.04, "solar": 0.02, "thermal": 0.55, "hydro": 0.02},
    "US-NW-BPAT":  {"wind": 0.10, "solar": 0.02, "thermal": 0.10, "hydro": 0.70},
    "US-NE-ISNE":  {"wind": 0.05, "solar": 0.04, "thermal": 0.55, "hydro": 0.05},
    "US-NY-NYIS":  {"wind": 0.04, "solar": 0.03, "thermal": 0.45, "hydro": 0.20},
    "US-SE-SOCO":  {"wind": 0.00, "solar": 0.03, "thermal": 0.70, "hydro": 0.03},
    "US-FLA-FPL":  {"wind": 0.00, "solar": 0.06, "thermal": 0.85, "hydro": 0.00},
    "US-CENT-SPP": {"wind": 0.40, "solar": 0.03, "thermal": 0.50, "hydro": 0.03},
    # Australia
    "AU-SA":       {"wind": 0.40, "solar": 0.20, "thermal": 0.40, "hydro": 0.00},
    "AU-VIC":      {"wind": 0.20, "solar": 0.10, "thermal": 0.65, "hydro": 0.05},
    "AU-NSW":      {"wind": 0.10, "solar": 0.12, "thermal": 0.70, "hydro": 0.08},
    "AU-QLD":      {"wind": 0.05, "solar": 0.15, "thermal": 0.75, "hydro": 0.02},
    "AU-TAS":      {"wind": 0.15, "solar": 0.01, "thermal": 0.10, "hydro": 0.74},
    "AU-WA":       {"wind": 0.14, "solar": 0.10, "thermal": 0.75, "hydro": 0.01},
    # Canada
    "CA-ON":       {"wind": 0.08, "solar": 0.02, "thermal": 0.10, "hydro": 0.25},
    "CA-QC":       {"wind": 0.04, "solar": 0.00, "thermal": 0.01, "hydro": 0.94},
    "CA-BC":       {"wind": 0.02, "solar": 0.00, "thermal": 0.03, "hydro": 0.89},
    "CA-AB":       {"wind": 0.10, "solar": 0.03, "thermal": 0.80, "hydro": 0.05},
    # Other big zones
    "NZ":          {"wind": 0.07, "solar": 0.01, "thermal": 0.20, "hydro": 0.60},
    "IS":          {"wind": 0.00, "solar": 0.00, "thermal": 0.00, "hydro": 0.70},
    "BR":          {"wind": 0.12, "solar": 0.06, "thermal": 0.20, "hydro": 0.60},
    "IN":          {"wind": 0.05, "solar": 0.06, "thermal": 0.80, "hydro": 0.08},
    "CN":          {"wind": 0.09, "solar": 0.05, "thermal": 0.68, "hydro": 0.16},
    "JP":          {"wind": 0.01, "solar": 0.10, "thermal": 0.75, "hydro": 0.08},
    "KR":          {"wind": 0.01, "solar": 0.05, "thermal": 0.65, "hydro": 0.02},
    "ZA":          {"wind": 0.06, "solar": 0.05, "thermal": 0.85, "hydro": 0.01},
    "MA":          {"wind": 0.14, "solar": 0.08, "thermal": 0.70, "hydro": 0.05},
}

_DEFAULT_PROFILE = {"wind": 0.15, "solar": 0.08, "thermal": 0.55, "hydro": 0.05}


def _zone_centroid(zone: str) -> Optional[Tuple[float, float]]:
    if not zone:
        return None
    if zone in _ZONE_CENTROIDS:
        return _ZONE_CENTROIDS[zone]
    parts = zone.replace("_", "-").split("-")
    for i in range(len(parts), 0, -1):
        candidate = "-".join(parts[:i])
        if candidate in _ZONE_CENTROIDS:
            return _ZONE_CENTROIDS[candidate]
    if parts and parts[0] in _ZONE_CENTROIDS:
        return _ZONE_CENTROIDS[parts[0]]
    return None


def _zone_profile(zone: str) -> Dict[str, float]:
    if zone in _ZONE_ENERGY_PROFILE:
        return _ZONE_ENERGY_PROFILE[zone]
    parts = zone.replace("_", "-").split("-")
    for i in range(len(parts), 0, -1):
        candidate = "-".join(parts[:i])
        if candidate in _ZONE_ENERGY_PROFILE:
            return _ZONE_ENERGY_PROFILE[candidate]
    if parts and parts[0] in _ZONE_ENERGY_PROFILE:
        return _ZONE_ENERGY_PROFILE[parts[0]]
    return _DEFAULT_PROFILE


@dataclass
class ForecastPoint:
    hour_ts: int
    intensity_g_per_kwh: float
    baseline_g_per_kwh: float
    weather_factor: float
    cloud_cover_pct: float
    wind_ms: float
    temp_c: float
    precip_mm: float


def _fetch_openmeteo(lat: float, lon: float, horizon_hours: int = _HORIZON_HOURS + 2) -> Optional[List[Dict[str, Any]]]:
    try:
        params = {
            "latitude": round(lat, 2),
            "longitude": round(lon, 2),
            "hourly": "temperature_2m,cloud_cover,wind_speed_10m,precipitation",
            "forecast_hours": max(6, min(horizon_hours, 24)),
            "timezone": "UTC",
        }
        r = requests.get(_OPEN_METEO, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        h = data.get("hourly", {})
        times = h.get("time", [])
        temps = h.get("temperature_2m", [])
        clouds = h.get("cloud_cover", [])
        winds = h.get("wind_speed_10m", [])
        precips = h.get("precipitation", [])
        out: List[Dict[str, Any]] = []
        for i, t in enumerate(times):
            try:
                dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                ts = (int(dt.timestamp()) // 3600) * 3600
                out.append({
                    "ts": ts,
                    "temp_c": float(temps[i]) if i < len(temps) else 15.0,
                    "cloud_pct": float(clouds[i]) if i < len(clouds) else 50.0,
                    "wind_ms": float(winds[i]) / 3.6 if i < len(winds) else 3.0,
                    "precip_mm": float(precips[i]) if i < len(precips) else 0.0,
                })
            except (ValueError, IndexError, TypeError):
                continue
        return out
    except Exception as e:
        logger.warning("Open-Meteo fetch failed for (%s,%s): %s", lat, lon, e)
        return None


def _compute_baseline_profile(db: Any, zone: str, lookback_days: int = 14) -> Dict[int, float]:
    """Median intensity per hour-of-day from the last N days of REAL data."""
    now_ts = int(time.time())
    start_ts = now_ts - lookback_days * 86400
    try:
        df = db.query_co2_intensity(zone, start_ts, now_ts)
    except Exception as e:
        logger.debug("Baseline query failed for %s: %s", zone, e)
        return {}
    if df is None or df.empty:
        return {}
    if "source" in df.columns:
        df = df[~df["source"].isin(["estimated", "forecast"])]
    if df.empty:
        return {}
    df = df.copy()
    df["hour_of_day"] = ((df["hour_ts"] // 3600) % 24).astype(int)
    grouped = df.groupby("hour_of_day")["intensity_g_per_kwh"].median()
    return {int(h): float(v) for h, v in grouped.items()}


def _daylight_factor(hour_utc: int, month: int, lat: float, lon: float) -> float:
    """Crude 0..1 daylight weight (sun above horizon). Uses local hour ~ utc + lon/15."""
    local_hour = (hour_utc + lon / 15.0) % 24
    if lat >= 0:
        seasonal = 2.0 if month in (5, 6, 7, 8) else (-1.5 if month in (11, 12, 1, 2) else 0.0)
    else:
        seasonal = 2.0 if month in (11, 12, 1, 2) else (-1.5 if month in (5, 6, 7, 8) else 0.0)
    sunrise = 6.0 - seasonal
    sunset = 18.0 + seasonal
    if local_hour < sunrise or local_hour >= sunset:
        return 0.0
    mid = (sunrise + sunset) / 2.0
    half = max(1.0, (sunset - sunrise) / 2.0)
    dist = abs(local_hour - mid)
    return max(0.0, 1.0 - (dist / half))


def _weather_adjust(
    baseline: float,
    weather: Dict[str, Any],
    profile: Dict[str, float],
    lat: float,
    lon: float,
) -> Tuple[float, float]:
    """Return (adjusted_intensity, factor) given baseline + weather."""
    wind_share = profile.get("wind", 0.15)
    solar_share = profile.get("solar", 0.08)
    thermal_share = profile.get("thermal", 0.55)
    hydro_share = profile.get("hydro", 0.05)

    dt = datetime.fromtimestamp(weather["ts"], tz=timezone.utc)

    wind_excess = (weather["wind_ms"] - 6.0) / 6.0
    wind_excess = max(-0.7, min(1.2, wind_excess))
    wind_adj = -wind_share * 0.55 * wind_excess

    daylight = _daylight_factor(dt.hour, dt.month, lat, lon)
    clearness = (100.0 - weather["cloud_pct"]) / 100.0
    solar_intensity = clearness * daylight
    solar_excess = solar_intensity - 0.25
    solar_adj = -solar_share * 0.60 * solar_excess

    temp = weather["temp_c"]
    if temp < 10.0:
        cold = min(1.0, (10.0 - temp) / 15.0)
        temp_adj = thermal_share * 0.25 * cold
    elif temp > 28.0:
        heat = min(1.0, (temp - 28.0) / 10.0)
        temp_adj = thermal_share * 0.15 * heat
    else:
        temp_adj = 0.0

    rain_adj = -hydro_share * 0.20 * min(1.0, weather["precip_mm"] / 3.0)

    factor = 1.0 + wind_adj + solar_adj + temp_adj + rain_adj
    factor = max(0.55, min(1.45, factor))
    return baseline * factor, factor


class Co2ForecastService:
    """Background service computing 6h CO2 intensity forecast per zone.

    Not persisted — cache is in-memory, refreshed every hour.
    """

    def __init__(self, db: Any, get_config: Any) -> None:
        self._db = db
        self._get_config = get_config
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._cache: Dict[str, List[ForecastPoint]] = {}
        self._last_run_ts: int = 0
        self._last_error: str = ""

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="Co2Forecaster", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def get_forecast(self, zone: str) -> List[ForecastPoint]:
        with self._lock:
            return list(self._cache.get(zone, []))

    def last_run_ts(self) -> int:
        return self._last_run_ts

    def last_error(self) -> str:
        return self._last_error

    def _run(self) -> None:
        if self._stop.wait(25.0):
            return
        while not self._stop.is_set():
            try:
                self.trigger_now()
            except Exception as e:
                logger.warning("CO2 forecast tick failed: %s", e)
                self._last_error = str(e)
            if self._stop.wait(3600):
                return

    def trigger_now(self) -> None:
        cfg = self._get_config()
        co2_cfg = getattr(cfg, "co2", None)
        if co2_cfg is None or not getattr(co2_cfg, "enabled", False):
            return
        zone = str(getattr(co2_cfg, "bidding_zone", "") or "").strip()
        if not zone:
            return
        points = self._compute_zone_forecast(zone)
        if points:
            with self._lock:
                self._cache[zone] = points
            self._last_run_ts = int(time.time())
            self._last_error = ""
            logger.info("CO2 forecast refreshed for %s: %d hours ahead", zone, len(points))
        else:
            self._last_error = "no forecast data"

    def _compute_zone_forecast(self, zone: str) -> List[ForecastPoint]:
        centroid = _zone_centroid(zone)
        if centroid is None:
            logger.debug("No centroid for zone %s, skipping forecast", zone)
            return []
        lat, lon = centroid
        profile = _zone_profile(zone)

        baseline_by_hour = _compute_baseline_profile(self._db, zone, lookback_days=14)
        if not baseline_by_hour:
            try:
                last_ts = self._db.latest_co2_ts(zone)
                if last_ts is not None:
                    df = self._db.query_co2_intensity(zone, last_ts, last_ts + 3600)
                    if df is not None and not df.empty:
                        last_val = float(df.iloc[-1]["intensity_g_per_kwh"])
                        baseline_by_hour = {h: last_val for h in range(24)}
            except Exception:
                pass
        if not baseline_by_hour:
            return []

        weather = _fetch_openmeteo(lat, lon, horizon_hours=_HORIZON_HOURS + 2)
        if not weather:
            return []

        now_hour_ts = (int(time.time()) // 3600) * 3600
        horizon_ts = now_hour_ts + _HORIZON_HOURS * 3600

        out: List[ForecastPoint] = []
        for w in weather:
            ts = w["ts"]
            if ts <= now_hour_ts or ts > horizon_ts:
                continue
            hour_of_day = (ts // 3600) % 24
            baseline = baseline_by_hour.get(hour_of_day)
            if baseline is None:
                baseline = sum(baseline_by_hour.values()) / len(baseline_by_hour)
            adjusted, factor = _weather_adjust(baseline, w, profile, lat, lon)
            out.append(ForecastPoint(
                hour_ts=ts,
                intensity_g_per_kwh=round(adjusted, 1),
                baseline_g_per_kwh=round(baseline, 1),
                weather_factor=round(factor, 3),
                cloud_cover_pct=round(w["cloud_pct"], 0),
                wind_ms=round(w["wind_ms"], 1),
                temp_c=round(w["temp_c"], 1),
                precip_mm=round(w["precip_mm"], 2),
            ))
            if len(out) >= _HORIZON_HOURS:
                break
        return out


def point_to_dict(p: ForecastPoint) -> Dict[str, Any]:
    return asdict(p)
