"""EV charging station lookup – OpenChargeMap (primary) + Bundesnetzagentur (fallback)."""
from __future__ import annotations

import logging
import math
import time
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

_CACHE: Dict[str, Any] = {}
_CACHE_TTL = 120  # seconds

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# OpenChargeMap
# ---------------------------------------------------------------------------

_OCM_BASE = "https://api.openchargemap.io/v3/poi/"


def _ocm_status(obj: dict) -> str:
    """Extract status from an OCM POI or Connection object.

    Handles both full and compact response formats:
    - Full: ``{"StatusType": {"ID": 50, "IsOperational": true, ...}}``
    - Compact: ``{"StatusTypeID": 50}``  (flat integer)
    """
    # Full object
    st = obj.get("StatusType")
    if isinstance(st, dict):
        sid = st.get("ID", 0)
        is_op = st.get("IsOperational")
    else:
        # Compact / flat
        sid = int(obj.get("StatusTypeID") or obj.get("StatusType") or 0)
        is_op = None

    # StatusTypeID mapping:
    #   0  = Unknown
    #  10  = Currently Available
    #  20  = Currently In Use
    #  30  = Temporarily Unavailable
    #  50  = Operational
    #  75  = Partly Operational/Mixed
    # 100  = Not Operational
    # 150  = Planned (For Future Date)
    # 200  = Removed (Decommissioned)
    if sid in (100, 150, 200) or is_op is False:
        return "unavailable"
    if sid == 30:
        return "unavailable"
    if sid == 20:
        return "occupied"
    if sid == 10:
        return "free"
    if sid == 50 or is_op is True:
        # "Operational" — the station works, but we don't know if it's
        # currently free or occupied.  Show as "free" (green) since most
        # stations are idle most of the time, and it's more useful than gray.
        return "free"
    if sid == 75:
        return "occupied"  # partly operational → yellow
    return "unknown"


def _fetch_ocm(
    lat: float, lon: float, radius_m: int, max_results: int, api_key: str,
) -> Optional[List[dict]]:
    """Query OpenChargeMap and return parsed station list, or None on failure."""
    distance_km = max(0.1, radius_m / 1000.0)
    params: Dict[str, Any] = {
        "latitude": lat,
        "longitude": lon,
        "distance": distance_km,
        "distanceunit": "KM",
        "maxresults": max_results,
        "compact": False,
        "verbose": False,
        "output": "json",
    }
    if api_key:
        params["key"] = api_key

    try:
        resp = requests.get(_OCM_BASE, params=params, timeout=15)
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        logger.warning("OpenChargeMap request failed: %s", e)
        return None

    stations: List[Dict[str, Any]] = []
    for poi in (raw if isinstance(raw, list) else []):
        addr_info = poi.get("AddressInfo") or {}
        poi_lat = float(addr_info.get("Latitude") or 0)
        poi_lon = float(addr_info.get("Longitude") or 0)
        dist = round(_haversine_m(lat, lon, poi_lat, poi_lon))

        # Station-level status
        poi_status = _ocm_status(poi)
        poi_date = poi.get("DateLastStatusUpdate") or poi.get("DateLastVerified") or None
        num_points = poi.get("NumberOfPoints") or None

        connections = poi.get("Connections") or []
        connectors: List[Dict[str, Any]] = []
        free_count = 0
        unavail_count = 0
        occupied_count = 0

        for c in connections:
            ct = c.get("ConnectionType") or {}
            level = c.get("Level") or {}
            status = _ocm_status(c)
            # Inherit station status if connector has none
            if status == "unknown" and poi_status != "unknown":
                status = poi_status
            if status == "free":
                free_count += 1
            elif status == "unavailable":
                unavail_count += 1
            elif status == "occupied":
                occupied_count += 1
            connectors.append({
                "id": c.get("ID", 0),
                "type": ct.get("Title") or ct.get("FormalName") or "Unknown",
                "kw": c.get("PowerKW") or (level.get("PowerKW") if isinstance(level, dict) else None) or 0,
                "status": status,
                "status_since": poi_date,
            })

        # If OCM says N points but only lists fewer connections, fill up
        if num_points and isinstance(num_points, int) and num_points > len(connectors):
            for _ in range(num_points - len(connectors)):
                status = poi_status if poi_status != "unknown" else "unknown"
                if status == "free":
                    free_count += 1
                connectors.append({
                    "id": 0,
                    "type": "Unknown",
                    "kw": 0,
                    "status": status,
                    "status_since": poi_date,
                })

        total = len(connectors)
        if total == 0 or unavail_count == total:
            station_status = "unavailable"
        elif free_count > 0:
            station_status = "available"
        elif occupied_count > 0:
            station_status = "occupied"
        else:
            station_status = poi_status if poi_status != "unknown" else "unknown"

        stations.append({
            "id": poi.get("ID", 0),
            "name": addr_info.get("Title") or "",
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
            "source": "ocm",
        })

    return stations


# ---------------------------------------------------------------------------
# Bundesnetzagentur (German charging station registry – static locations)
# ---------------------------------------------------------------------------

_BNA_BASE = "https://ladestationen.api.bund.dev/"


def _fetch_bna(lat: float, lon: float, radius_m: int, max_results: int) -> Optional[List[dict]]:
    """Query Bundesnetzagentur Ladesäulenregister for stations near a point.

    This API provides official German static data (no real-time status).
    All stations are shown as 'free' (operational) since BNA only lists
    active public chargers.
    """
    try:
        # BNA uses a bounding-box approach; approximate from radius
        dlat = radius_m / 111_320.0
        dlon = radius_m / (111_320.0 * math.cos(math.radians(lat)))
        params: Dict[str, Any] = {
            "lat_min": lat - dlat,
            "lat_max": lat + dlat,
            "lon_min": lon - dlon,
            "lon_max": lon + dlon,
        }
        resp = requests.get(_BNA_BASE, params=params, timeout=10)
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        logger.warning("Bundesnetzagentur API failed: %s", e)
        return None

    stations: List[Dict[str, Any]] = []
    items = raw if isinstance(raw, list) else raw.get("data", raw.get("results", []))
    if not isinstance(items, list):
        return None

    for item in items:
        s_lat = float(item.get("latitude") or item.get("lat") or 0)
        s_lon = float(item.get("longitude") or item.get("lon") or 0)
        if s_lat == 0 and s_lon == 0:
            continue
        dist = round(_haversine_m(lat, lon, s_lat, s_lon))
        if dist > radius_m:
            continue

        # Extract connector info
        n_points = int(item.get("numberOfChargingPoints") or item.get("number_of_charging_points") or 1)
        power = float(item.get("maxPowerKW") or item.get("max_power_kw") or 0)
        operator = item.get("operator") or item.get("betreiber") or ""
        address = ", ".join(filter(None, [
            item.get("street") or item.get("strasse") or "",
            item.get("postcode") or item.get("plz") or "",
            item.get("city") or item.get("ort") or "",
        ]))
        name = operator or address.split(",")[0] if address else f"BNA-{item.get('id', '?')}"

        connectors = []
        for i in range(n_points):
            connectors.append({
                "id": i + 1,
                "type": item.get("plugType") or item.get("stecker_typ") or "AC",
                "kw": power,
                "status": "free",  # BNA only lists active stations
                "status_since": None,
            })

        stations.append({
            "id": hash(f"bna-{s_lat}-{s_lon}") & 0x7FFFFFFF,
            "name": name,
            "address": address,
            "distance_m": dist,
            "lat": s_lat,
            "lon": s_lon,
            "status": "available",
            "total_connectors": n_points,
            "free_connectors": n_points,
            "connectors": connectors,
            "source": "bna",
        })

    return stations


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_ev_chargers(
    lat: float,
    lon: float,
    radius_m: int = 500,
    max_results: int = 50,
    api_key: str = "",
) -> Dict[str, Any]:
    """Fetch nearby EV chargers. Tries OpenChargeMap first, BNA as fallback."""
    cache_key = f"{lat:.4f},{lon:.4f},{radius_m},{api_key[:8] if api_key else ''}"
    now = time.time()
    cached = _CACHE.get(cache_key)
    if cached and (now - cached[0]) < _CACHE_TTL:
        return cached[1]

    # Primary: OpenChargeMap (has international coverage + some status data)
    stations = _fetch_ocm(lat, lon, radius_m, max_results, api_key)

    # Fallback: Bundesnetzagentur (German official registry, static)
    if stations is None or len(stations) == 0:
        logger.info("OCM returned no results, trying Bundesnetzagentur fallback")
        bna = _fetch_bna(lat, lon, radius_m, max_results)
        if bna:
            stations = bna

    if stations is None:
        return {"ok": False, "error": "All data sources failed", "stations": []}

    stations.sort(key=lambda s: s["distance_m"])
    stations = stations[:max_results]

    result: Dict[str, Any] = {"ok": True, "stations": stations}
    _CACHE[cache_key] = (now, result)
    return result
