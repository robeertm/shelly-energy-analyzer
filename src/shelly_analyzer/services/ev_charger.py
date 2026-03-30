"""EV charging station lookup via OpenChargeMap API."""
from __future__ import annotations

import logging
import math
import time
from typing import Any, Dict, List

import requests

logger = logging.getLogger(__name__)

_OCM_BASE = "https://api.openchargemap.io/v3/poi/"
_CACHE: Dict[str, Any] = {}
_CACHE_TTL = 120  # seconds


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance in meters between two lat/lon points."""
    R = 6_371_000
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _status_from_ocm(conn: dict) -> str:
    """Map OpenChargeMap StatusType to simplified status string."""
    st = conn.get("StatusType") or {}
    sid = st.get("ID", 0)
    is_op = st.get("IsOperational")
    # StatusTypeID: 0=Unknown, 10=Currently Available, 20=Currently In Use,
    # 30=Temporarily Unavailable, 50=Operational, 100=Not Operational,
    # 150=Planned, 200=Removed
    if sid in (100, 150, 200) or is_op is False:
        return "unavailable"
    if sid == 20:
        return "occupied"
    if sid in (10, 50) or is_op is True:
        return "free"
    if sid == 30:
        return "unavailable"
    return "unknown"


def fetch_ev_chargers(
    lat: float,
    lon: float,
    radius_m: int = 500,
    max_results: int = 50,
    api_key: str = "",
) -> Dict[str, Any]:
    """Fetch nearby EV chargers from OpenChargeMap with in-memory caching."""
    cache_key = f"{lat:.4f},{lon:.4f},{radius_m}"
    now = time.time()
    cached = _CACHE.get(cache_key)
    if cached and (now - cached[0]) < _CACHE_TTL:
        return cached[1]

    distance_km = max(0.1, radius_m / 1000.0)
    params: Dict[str, Any] = {
        "latitude": lat,
        "longitude": lon,
        "distance": distance_km,
        "distanceunit": "KM",
        "maxresults": max_results,
        "compact": True,
        "verbose": False,
        "output": "json",
    }
    if api_key:
        params["key"] = api_key

    try:
        resp = requests.get(_OCM_BASE, params=params, timeout=10)
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        logger.warning("OpenChargeMap request failed: %s", e)
        return {"ok": False, "error": str(e), "stations": []}

    stations: List[Dict[str, Any]] = []
    for poi in (raw if isinstance(raw, list) else []):
        addr_info = poi.get("AddressInfo") or {}
        poi_lat = float(addr_info.get("Latitude") or 0)
        poi_lon = float(addr_info.get("Longitude") or 0)
        dist = round(_haversine_m(lat, lon, poi_lat, poi_lon))

        # Station-level status (fallback for connectors without own status)
        poi_status = _status_from_ocm(poi)
        poi_date_last_status = poi.get("DateLastStatusUpdate") or None

        connections = poi.get("Connections") or []
        connectors: List[Dict[str, Any]] = []
        free_count = 0
        unavail_count = 0
        unknown_count = 0
        for c in connections:
            ct = c.get("ConnectionType") or {}
            level = c.get("Level") or {}
            status = _status_from_ocm(c)
            # If connector has no own status, inherit from station
            if status == "unknown" and poi_status != "unknown":
                status = poi_status
            if status == "free":
                free_count += 1
            elif status == "unavailable":
                unavail_count += 1
            elif status == "unknown":
                unknown_count += 1
            connectors.append({
                "id": c.get("ID", 0),
                "type": ct.get("Title", "Unknown"),
                "kw": c.get("PowerKW") or (level.get("PowerKW") if level else None) or 0,
                "status": status,
                "status_since": poi_date_last_status,
            })

        total = len(connectors)
        if total == 0 or unavail_count == total:
            station_status = "unavailable"
        elif free_count > 0:
            station_status = "available"
        elif unknown_count == total:
            # All unknown — use station-level status or mark unknown
            station_status = poi_status if poi_status != "unknown" else "unknown"
        else:
            station_status = "occupied"

        stations.append({
            "id": poi.get("ID", 0),
            "name": addr_info.get("Title", ""),
            "address": ", ".join(filter(None, [
                addr_info.get("AddressLine1", ""),
                addr_info.get("Postcode", ""),
                addr_info.get("Town", ""),
            ])),
            "distance_m": dist,
            "lat": poi_lat,
            "lon": poi_lon,
            "status": station_status,
            "total_connectors": total,
            "free_connectors": free_count,
            "connectors": connectors,
        })

    stations.sort(key=lambda s: s["distance_m"])
    result: Dict[str, Any] = {"ok": True, "stations": stations}
    _CACHE[cache_key] = (now, result)
    return result
