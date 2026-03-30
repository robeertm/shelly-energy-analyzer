"""EV charging station lookup – multi-source with deduplication.

Sources (queried in parallel, results merged, best detail wins):
  1. EnBW/SMATRICS  – REAL-TIME status per connector (AVAILABLE/OCCUPIED/OUT_OF_SERVICE)
  2. OpenChargeMap   – international, some status data (API key required)
  3. Bundesnetzagentur – official German registry, static locations
  4. OpenStreetMap    – community data, good coverage, no key needed
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

# Connector type display names
_CONNECTOR_NAMES = {
    "IEC_62196_T2": "Typ 2",
    "IEC_62196_T2_COMBO": "CCS",
    "CHADEMO": "CHAdeMO",
    "IEC_62196_T1": "Typ 1",
    "IEC_62196_T1_COMBO": "CCS Typ 1",
    "DOMESTIC_F": "Schuko",
    "TESLA_S": "Tesla",
    "TESLA_R": "Tesla",
}

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


def _detail_score(s: dict) -> int:
    """Higher = more detailed data. Used to decide which duplicate to keep."""
    score = 0
    # Real-time status (not just "free" default)
    for c in s.get("connectors", []):
        if c.get("status_since"):
            score += 10
        if c.get("status") in ("free", "occupied", "unavailable"):
            score += 2
    # Source quality
    src = s.get("source", "")
    score += {"enbw": 20, "ocm": 5, "bna": 1, "osm": 1}.get(src, 0)
    # More connectors = more detail
    score += s.get("total_connectors", 0)
    return score


def _dedup_stations(stations: List[Dict[str, Any]], threshold_m: float = 50) -> List[Dict[str, Any]]:
    """Remove duplicates within *threshold_m* meters, keeping the one with best detail."""
    kept: List[Dict[str, Any]] = []
    for s in stations:
        is_dup = False
        for i, k in enumerate(kept):
            if _haversine_m(s["lat"], s["lon"], k["lat"], k["lon"]) < threshold_m:
                if _detail_score(s) > _detail_score(k):
                    kept[i] = s
                is_dup = True
                break
        if not is_dup:
            kept.append(s)
    return kept


# ---------------------------------------------------------------------------
# Source 1: EnBW / SMATRICS (REAL-TIME STATUS!)
# ---------------------------------------------------------------------------

_SMATRICS_BASE = "https://poi-service.smatrics-api.com"
_SMATRICS_KEY = "faMnWlXyZHvnhVL7KV3zJaMgSENEB7pwbIn-ZojNcPSsh4RzXWig5htGvwt3AWVR"


def _fetch_enbw(lat: float, lon: float, radius_m: int, max_results: int) -> Optional[List[dict]]:
    """Query SMATRICS/EnBW bounding-box API for real-time station data."""
    dlat = radius_m / 111_320.0
    dlon = radius_m / (111_320.0 * max(math.cos(math.radians(lat)), 0.01))
    body = {
        "latitude_south": lat - dlat,
        "latitude_north": lat + dlat,
        "longitude_west": lon - dlon,
        "longitude_east": lon + dlon,
    }
    try:
        resp = requests.post(
            f"{_SMATRICS_BASE}/b2c/locations/reduced/bounding-box",
            json=body,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {_SMATRICS_KEY}",
                "Origin": "https://enbw.smatrics.com",
                "Referer": "https://enbw.smatrics.com/",
            },
            timeout=15,
        )
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        logger.warning("EnBW/SMATRICS failed: %s", e)
        return None

    if not isinstance(raw, list):
        return None

    stations: List[Dict[str, Any]] = []
    for loc in raw:
        coords = loc.get("coordinates") or {}
        slat = float(coords.get("latitude") or 0)
        slon = float(coords.get("longitude") or 0)
        if slat == 0 and slon == 0:
            continue
        dist = round(_haversine_m(lat, lon, slat, slon))
        if dist > radius_m:
            continue

        names = loc.get("name") or []
        name = ""
        for n in names:
            if isinstance(n, dict):
                name = n.get("text", "")
                break
        if not name:
            name = (loc.get("partner_name") or loc.get("id") or "EnBW")
        operator = ""
        op_obj = loc.get("suboperator") or loc.get("operator") or {}
        if isinstance(op_obj, dict):
            operator = op_obj.get("name", "")

        evses = loc.get("evses") or []
        connectors, free_c, occ_c, unavail_c = [], 0, 0, 0
        for evse in evses:
            st_raw = (evse.get("status") or "UNKNOWN").upper()
            if st_raw == "AVAILABLE":
                st = "free"
                free_c += 1
            elif st_raw == "OCCUPIED" or st_raw == "CHARGING":
                st = "occupied"
                occ_c += 1
            elif st_raw in ("OUT_OF_SERVICE", "BLOCKED", "REMOVED", "INOPERATIVE"):
                st = "unavailable"
                unavail_c += 1
            else:
                st = "unknown"
            last_upd = evse.get("last_updated")
            for conn in (evse.get("connectors") or [{}]):
                std = conn.get("standard", "")
                connectors.append({
                    "id": conn.get("id", evse.get("evse_id", "")),
                    "type": _CONNECTOR_NAMES.get(std, std or "AC"),
                    "kw": float(conn.get("power_kw") or conn.get("max_electric_power") or 0),
                    "status": st,
                    "status_since": last_upd,
                })

        total = len(connectors)
        ss = "unavailable" if total == 0 or unavail_c == total else "available" if free_c > 0 else "occupied" if occ_c > 0 else "unknown"

        stations.append({
            "id": hash(loc.get("id", "")) & 0x7FFFFFFF,
            "name": name.strip(),
            "operator": operator,
            "address": "",
            "distance_m": dist, "lat": slat, "lon": slon,
            "status": ss, "total_connectors": total, "free_connectors": free_c,
            "connectors": connectors, "source": "enbw",
        })
    return stations


# ---------------------------------------------------------------------------
# Source 2: OpenChargeMap
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
    params: Dict[str, Any] = {
        "latitude": lat, "longitude": lon,
        "distance": max(0.1, radius_m / 1000.0), "distanceunit": "KM",
        "maxresults": max_results, "compact": False, "verbose": False, "output": "json",
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
        plat, plon = float(addr.get("Latitude") or 0), float(addr.get("Longitude") or 0)
        dist = round(_haversine_m(lat, lon, plat, plon))
        poi_status = _ocm_status(poi)
        poi_date = poi.get("DateLastStatusUpdate") or poi.get("DateLastVerified")
        connections = poi.get("Connections") or []
        connectors, free_c, occ_c, unavail_c = [], 0, 0, 0
        for c in connections:
            ct = c.get("ConnectionType") or {}
            lv = c.get("Level") or {}
            st = _ocm_status(c)
            if st == "unknown" and poi_status != "unknown":
                st = poi_status
            if st == "free": free_c += 1
            elif st == "occupied": occ_c += 1
            elif st == "unavailable": unavail_c += 1
            connectors.append({
                "id": c.get("ID", 0),
                "type": ct.get("Title") or ct.get("FormalName") or "Unknown",
                "kw": c.get("PowerKW") or (lv.get("PowerKW") if isinstance(lv, dict) else None) or 0,
                "status": st, "status_since": poi_date,
            })
        num_points = poi.get("NumberOfPoints")
        if num_points and isinstance(num_points, int) and num_points > len(connectors):
            for _ in range(num_points - len(connectors)):
                st = poi_status if poi_status != "unknown" else "unknown"
                if st == "free": free_c += 1
                connectors.append({"id": 0, "type": "Unknown", "kw": 0, "status": st, "status_since": poi_date})
        total = len(connectors)
        ss = "unavailable" if total == 0 or unavail_c == total else "available" if free_c > 0 else "occupied" if occ_c > 0 else (poi_status if poi_status != "unknown" else "unknown")
        stations.append({
            "id": poi.get("ID", 0), "name": addr.get("Title") or "",
            "address": ", ".join(filter(None, [addr.get("AddressLine1", ""), addr.get("Postcode", ""), addr.get("Town", "")])),
            "distance_m": dist, "lat": plat, "lon": plon,
            "status": ss, "total_connectors": total, "free_connectors": free_c,
            "connectors": connectors, "source": "ocm",
        })
    return stations


# ---------------------------------------------------------------------------
# Source 3: Bundesnetzagentur
# ---------------------------------------------------------------------------

_BNA_BASE = "https://ladestationen.api.bund.dev/"


def _fetch_bna(lat: float, lon: float, radius_m: int, max_results: int) -> Optional[List[dict]]:
    try:
        dlat = radius_m / 111_320.0
        dlon = radius_m / (111_320.0 * max(math.cos(math.radians(lat)), 0.01))
        resp = requests.get(_BNA_BASE, params={"lat_min": lat - dlat, "lat_max": lat + dlat, "lon_min": lon - dlon, "lon_max": lon + dlon}, timeout=10)
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
        slat, slon = float(it.get("latitude") or it.get("lat") or 0), float(it.get("longitude") or it.get("lon") or 0)
        if slat == 0 and slon == 0: continue
        dist = round(_haversine_m(lat, lon, slat, slon))
        if dist > radius_m: continue
        n = int(it.get("numberOfChargingPoints") or it.get("number_of_charging_points") or 1)
        pw = float(it.get("maxPowerKW") or it.get("max_power_kw") or 0)
        op = it.get("operator") or it.get("betreiber") or ""
        addr = ", ".join(filter(None, [it.get("street") or it.get("strasse") or "", it.get("postcode") or it.get("plz") or "", it.get("city") or it.get("ort") or ""]))
        name = op or (addr.split(",")[0] if addr else f"BNA-{it.get('id', '?')}")
        plug = it.get("plugType") or it.get("stecker_typ") or "AC"
        connectors = [{"id": i + 1, "type": plug, "kw": pw, "status": "free", "status_since": None} for i in range(n)]
        stations.append({"id": hash(f"bna-{slat}-{slon}") & 0x7FFFFFFF, "name": name, "address": addr,
            "distance_m": dist, "lat": slat, "lon": slon, "status": "available", "total_connectors": n, "free_connectors": n,
            "connectors": connectors, "source": "bna"})
    return stations


# ---------------------------------------------------------------------------
# Source 4: OpenStreetMap / Overpass
# ---------------------------------------------------------------------------

_OVERPASS_URL = "https://overpass-api.de/api/interpreter"


def _fetch_osm(lat: float, lon: float, radius_m: int, max_results: int) -> Optional[List[dict]]:
    query = f'[out:json][timeout:10];(node["amenity"="charging_station"](around:{radius_m},{lat},{lon});way["amenity"="charging_station"](around:{radius_m},{lat},{lon}););out center {max_results};'
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
        slat = float(el.get("lat") or (el.get("center") or {}).get("lat") or 0)
        slon = float(el.get("lon") or (el.get("center") or {}).get("lon") or 0)
        if slat == 0 and slon == 0: continue
        dist = round(_haversine_m(lat, lon, slat, slon))
        name = tags.get("name") or tags.get("operator") or tags.get("brand") or tags.get("network") or "Ladestation"
        addr = ", ".join(filter(None, [tags.get("addr:street", ""), tags.get("addr:housenumber", ""), tags.get("addr:postcode", ""), tags.get("addr:city", "")]))
        n = 1
        for k in ("capacity", "charging_station:capacity", "sockets"):
            if k in tags:
                try: n = max(1, int(tags[k])); break
                except (ValueError, TypeError): pass
        pw = 0.0
        for k in ("maxpower", "charging_station:output"):
            raw_pw = tags.get(k, "")
            if raw_pw:
                try: pw = float(str(raw_pw).replace("kW", "").replace(" ", "").replace(",", ".")); break
                except (ValueError, TypeError): pass
        socket_types = []
        for sk in ("socket:type2", "socket:type2_combo", "socket:chademo", "socket:type1", "socket:schuko", "socket:type2_cable"):
            if tags.get(sk) and tags[sk] not in ("no", "0"):
                socket_types.append(sk.replace("socket:", "").replace("_", " ").title())
        plug_type = ", ".join(socket_types) if socket_types else "AC"
        connectors = [{"id": i + 1, "type": plug_type, "kw": pw, "status": "free", "status_since": None} for i in range(n)]
        stations.append({"id": el.get("id", 0), "name": name, "address": addr,
            "distance_m": dist, "lat": slat, "lon": slon, "status": "available", "total_connectors": n, "free_connectors": n,
            "connectors": connectors, "source": "osm"})
    return stations


# ---------------------------------------------------------------------------
# Public API – parallel multi-source fetch + merge + filter
# ---------------------------------------------------------------------------

def fetch_ev_chargers(
    lat: float, lon: float, radius_m: int = 500, max_results: int = 100,
    api_key: str = "", min_kw: float = 0, plug_filter: str = "",
) -> Dict[str, Any]:
    """Fetch nearby EV chargers from all sources, merge, dedup, and optionally filter."""
    cache_key = f"{lat:.4f},{lon:.4f},{radius_m},{api_key[:8] if api_key else ''},{min_kw},{plug_filter}"
    now = time.time()
    cached = _CACHE.get(cache_key)
    if cached and (now - cached[0]) < _CACHE_TTL:
        return cached[1]

    all_stations: List[Dict[str, Any]] = []
    sources_ok, sources_fail = [], []

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {
            pool.submit(_fetch_enbw, lat, lon, radius_m, max_results): "enbw",
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

    # Sort by distance, deduplicate (best detail wins)
    all_stations.sort(key=lambda s: s["distance_m"])
    merged = _dedup_stations(all_stations, threshold_m=50)

    # Apply filters
    if min_kw > 0:
        merged = [s for s in merged if any(c.get("kw", 0) >= min_kw for c in s.get("connectors", []))]
    if plug_filter:
        pf = plug_filter.lower()
        merged = [s for s in merged if any(pf in c.get("type", "").lower() for c in s.get("connectors", []))]

    merged = merged[:max_results]
    result: Dict[str, Any] = {"ok": True, "stations": merged, "sources": sources_ok, "total_before_dedup": len(all_stations)}
    _CACHE[cache_key] = (now, result)
    return result
