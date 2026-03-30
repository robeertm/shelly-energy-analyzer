"""EV charging station lookup – multi-source with deduplication.

Sources (queried in parallel, results merged):
  1. OpenChargeMap  – international, some status data (API key required)
  2. Bundesnetzagentur – official German registry, static locations
  3. OpenStreetMap / Overpass – community data, good German coverage
"""
from __future__ import annotations

import logging
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

_CACHE: Dict[str, Any] = {}
_CACHE_TTL = 120  # seconds

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6_371_000
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _dedup_stations(stations: List[Dict[str, Any]], threshold_m: float = 50) -> List[Dict[str, Any]]:
    """Remove duplicate stations from different sources within *threshold_m* meters.

    Keeps the entry with richer data (more connectors, or OCM over BNA/OSM).
    """
    SOURCE_PRIORITY = {"ocm": 0, "bna": 1, "osm": 2}
    kept: List[Dict[str, Any]] = []
    for s in stations:
        is_dup = False
        for k in kept:
            if _haversine_m(s["lat"], s["lon"], k["lat"], k["lon"]) < threshold_m:
                # Keep the one with better source / more connectors
                s_prio = SOURCE_PRIORITY.get(s.get("source", ""), 9)
                k_prio = SOURCE_PRIORITY.get(k.get("source", ""), 9)
                if s_prio < k_prio or (s_prio == k_prio and s["total_connectors"] > k["total_connectors"]):
                    kept[kept.index(k)] = s
                is_dup = True
                break
        if not is_dup:
            kept.append(s)
    return kept


# ---------------------------------------------------------------------------
# Source 1: OpenChargeMap
# ---------------------------------------------------------------------------

_OCM_BASE = "https://api.openchargemap.io/v3/poi/"


def _ocm_status(obj: dict) -> str:
    st = obj.get("StatusType")
    if isinstance(st, dict):
        sid = st.get("ID", 0)
        is_op = st.get("IsOperational")
    else:
        sid = int(obj.get("StatusTypeID") or obj.get("StatusType") or 0)
        is_op = None

    if sid in (100, 150, 200) or is_op is False:
        return "unavailable"
    if sid == 30:
        return "unavailable"
    if sid == 20:
        return "occupied"
    if sid == 10:
        return "free"
    if sid == 50 or is_op is True:
        return "free"
    if sid == 75:
        return "occupied"
    return "unknown"


def _fetch_ocm(lat: float, lon: float, radius_m: int, max_results: int, api_key: str) -> Optional[List[dict]]:
    distance_km = max(0.1, radius_m / 1000.0)
    params: Dict[str, Any] = {
        "latitude": lat, "longitude": lon,
        "distance": distance_km, "distanceunit": "KM",
        "maxresults": max_results,
        "compact": False, "verbose": False, "output": "json",
    }
    if api_key:
        params["key"] = api_key

    try:
        resp = requests.get(_OCM_BASE, params=params, timeout=15)
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        logger.warning("OpenChargeMap failed: %s", e)
        return None

    stations: List[Dict[str, Any]] = []
    for poi in (raw if isinstance(raw, list) else []):
        addr = poi.get("AddressInfo") or {}
        plat = float(addr.get("Latitude") or 0)
        plon = float(addr.get("Longitude") or 0)
        dist = round(_haversine_m(lat, lon, plat, plon))
        poi_status = _ocm_status(poi)
        poi_date = poi.get("DateLastStatusUpdate") or poi.get("DateLastVerified")
        num_points = poi.get("NumberOfPoints")

        connections = poi.get("Connections") or []
        connectors, free_c, unavail_c, occ_c = [], 0, 0, 0
        for c in connections:
            ct = c.get("ConnectionType") or {}
            lv = c.get("Level") or {}
            st = _ocm_status(c)
            if st == "unknown" and poi_status != "unknown":
                st = poi_status
            if st == "free": free_c += 1
            elif st == "unavailable": unavail_c += 1
            elif st == "occupied": occ_c += 1
            connectors.append({
                "id": c.get("ID", 0),
                "type": ct.get("Title") or ct.get("FormalName") or "Unknown",
                "kw": c.get("PowerKW") or (lv.get("PowerKW") if isinstance(lv, dict) else None) or 0,
                "status": st, "status_since": poi_date,
            })
        if num_points and isinstance(num_points, int) and num_points > len(connectors):
            for _ in range(num_points - len(connectors)):
                st = poi_status if poi_status != "unknown" else "unknown"
                if st == "free": free_c += 1
                connectors.append({"id": 0, "type": "Unknown", "kw": 0, "status": st, "status_since": poi_date})

        total = len(connectors)
        ss = "unavailable" if total == 0 or unavail_c == total else "available" if free_c > 0 else "occupied" if occ_c > 0 else (poi_status if poi_status != "unknown" else "unknown")

        stations.append({
            "id": poi.get("ID", 0),
            "name": addr.get("Title") or "",
            "address": ", ".join(filter(None, [addr.get("AddressLine1", ""), addr.get("Postcode", ""), addr.get("Town", "")])),
            "distance_m": dist, "lat": plat, "lon": plon,
            "status": ss, "total_connectors": total, "free_connectors": free_c,
            "connectors": connectors, "source": "ocm",
        })
    return stations


# ---------------------------------------------------------------------------
# Source 2: Bundesnetzagentur
# ---------------------------------------------------------------------------

_BNA_BASE = "https://ladestationen.api.bund.dev/"


def _fetch_bna(lat: float, lon: float, radius_m: int, max_results: int) -> Optional[List[dict]]:
    try:
        dlat = radius_m / 111_320.0
        dlon = radius_m / (111_320.0 * max(math.cos(math.radians(lat)), 0.01))
        params = {"lat_min": lat - dlat, "lat_max": lat + dlat, "lon_min": lon - dlon, "lon_max": lon + dlon}
        resp = requests.get(_BNA_BASE, params=params, timeout=10)
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        logger.warning("Bundesnetzagentur failed: %s", e)
        return None

    items = raw if isinstance(raw, list) else raw.get("data", raw.get("results", []))
    if not isinstance(items, list):
        return None

    stations: List[Dict[str, Any]] = []
    for it in items:
        slat = float(it.get("latitude") or it.get("lat") or 0)
        slon = float(it.get("longitude") or it.get("lon") or 0)
        if slat == 0 and slon == 0:
            continue
        dist = round(_haversine_m(lat, lon, slat, slon))
        if dist > radius_m:
            continue
        n = int(it.get("numberOfChargingPoints") or it.get("number_of_charging_points") or 1)
        pw = float(it.get("maxPowerKW") or it.get("max_power_kw") or 0)
        op = it.get("operator") or it.get("betreiber") or ""
        addr = ", ".join(filter(None, [it.get("street") or it.get("strasse") or "", it.get("postcode") or it.get("plz") or "", it.get("city") or it.get("ort") or ""]))
        name = op or (addr.split(",")[0] if addr else f"BNA-{it.get('id', '?')}")
        connectors = [{"id": i + 1, "type": it.get("plugType") or it.get("stecker_typ") or "AC", "kw": pw, "status": "free", "status_since": None} for i in range(n)]
        stations.append({
            "id": hash(f"bna-{slat}-{slon}") & 0x7FFFFFFF, "name": name, "address": addr,
            "distance_m": dist, "lat": slat, "lon": slon,
            "status": "available", "total_connectors": n, "free_connectors": n,
            "connectors": connectors, "source": "bna",
        })
    return stations


# ---------------------------------------------------------------------------
# Source 3: OpenStreetMap / Overpass API
# ---------------------------------------------------------------------------

_OVERPASS_URL = "https://overpass-api.de/api/interpreter"


def _fetch_osm(lat: float, lon: float, radius_m: int, max_results: int) -> Optional[List[dict]]:
    """Query Overpass for amenity=charging_station nodes/ways near a point."""
    query = f"""
    [out:json][timeout:10];
    (
      node["amenity"="charging_station"](around:{radius_m},{lat},{lon});
      way["amenity"="charging_station"](around:{radius_m},{lat},{lon});
    );
    out center {max_results};
    """
    try:
        resp = requests.post(_OVERPASS_URL, data={"data": query}, timeout=15)
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        logger.warning("Overpass/OSM failed: %s", e)
        return None

    stations: List[Dict[str, Any]] = []
    for el in raw.get("elements", []):
        tags = el.get("tags") or {}
        # node → lat/lon direct; way → center
        slat = float(el.get("lat") or (el.get("center") or {}).get("lat") or 0)
        slon = float(el.get("lon") or (el.get("center") or {}).get("lon") or 0)
        if slat == 0 and slon == 0:
            continue
        dist = round(_haversine_m(lat, lon, slat, slon))

        name = tags.get("name") or tags.get("operator") or tags.get("brand") or tags.get("network") or "Ladestation"
        addr = ", ".join(filter(None, [
            tags.get("addr:street", ""),
            tags.get("addr:housenumber", ""),
            tags.get("addr:postcode", ""),
            tags.get("addr:city", ""),
        ]))

        # Parse connector count from OSM tags
        n = 1
        for k in ("capacity", "charging_station:capacity", "sockets", "socket:type2:output"):
            if k in tags:
                try:
                    n = max(1, int(tags[k]))
                    break
                except (ValueError, TypeError):
                    pass

        # Parse power
        pw = 0.0
        for k in ("maxpower", "charging_station:output", "socket:type2:output"):
            raw_pw = tags.get(k, "")
            if raw_pw:
                try:
                    pw = float(str(raw_pw).replace("kW", "").replace(" ", "").replace(",", "."))
                    break
                except (ValueError, TypeError):
                    pass

        # Socket types
        socket_types = []
        for sk in ("socket:type2", "socket:type2_combo", "socket:chademo", "socket:type1", "socket:schuko", "socket:type2_cable"):
            if tags.get(sk) and tags[sk] not in ("no", "0"):
                nice = sk.replace("socket:", "").replace("_", " ").title()
                socket_types.append(nice)
        plug_type = ", ".join(socket_types) if socket_types else "AC"

        connectors = [{"id": i + 1, "type": plug_type, "kw": pw, "status": "free", "status_since": None} for i in range(n)]
        stations.append({
            "id": el.get("id", 0), "name": name, "address": addr,
            "distance_m": dist, "lat": slat, "lon": slon,
            "status": "available", "total_connectors": n, "free_connectors": n,
            "connectors": connectors, "source": "osm",
        })
    return stations


# ---------------------------------------------------------------------------
# Public API – parallel multi-source fetch + merge
# ---------------------------------------------------------------------------

def fetch_ev_chargers(
    lat: float,
    lon: float,
    radius_m: int = 500,
    max_results: int = 100,
    api_key: str = "",
) -> Dict[str, Any]:
    """Fetch nearby EV chargers from all sources, merge and deduplicate."""
    cache_key = f"{lat:.4f},{lon:.4f},{radius_m},{api_key[:8] if api_key else ''}"
    now = time.time()
    cached = _CACHE.get(cache_key)
    if cached and (now - cached[0]) < _CACHE_TTL:
        return cached[1]

    all_stations: List[Dict[str, Any]] = []
    sources_ok: List[str] = []
    sources_fail: List[str] = []

    # Query all sources in parallel
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {
            pool.submit(_fetch_ocm, lat, lon, radius_m, max_results, api_key): "ocm",
            pool.submit(_fetch_bna, lat, lon, radius_m, max_results): "bna",
            pool.submit(_fetch_osm, lat, lon, radius_m, max_results): "osm",
        }
        for fut in as_completed(futures, timeout=20):
            name = futures[fut]
            try:
                result = fut.result()
                if result:
                    all_stations.extend(result)
                    sources_ok.append(name)
                    logger.info("EV %s: %d stations", name, len(result))
                else:
                    sources_fail.append(name)
            except Exception as e:
                logger.warning("EV %s failed: %s", name, e)
                sources_fail.append(name)

    if not all_stations:
        return {"ok": False, "error": "All data sources failed", "stations": [], "sources": sources_fail}

    # Sort by distance, then deduplicate nearby entries from different sources
    all_stations.sort(key=lambda s: s["distance_m"])
    merged = _dedup_stations(all_stations, threshold_m=50)
    merged = merged[:max_results]

    result: Dict[str, Any] = {
        "ok": True,
        "stations": merged,
        "sources": sources_ok,
        "total_before_dedup": len(all_stations),
    }
    _CACHE[cache_key] = (now, result)
    return result
