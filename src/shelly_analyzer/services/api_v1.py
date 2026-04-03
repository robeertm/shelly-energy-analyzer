from __future__ import annotations
import json
import logging
import time
from typing import Any, Dict, List, Optional

_log = logging.getLogger(__name__)


OPENAPI_SPEC = {
    "openapi": "3.0.3",
    "info": {
        "title": "Shelly Energy Analyzer API",
        "version": "1.0.0",
        "description": "REST API for accessing energy monitoring data from Shelly devices.",
    },
    "paths": {
        "/api/v1/devices": {
            "get": {"summary": "List all configured devices", "responses": {"200": {"description": "Device list"}}},
        },
        "/api/v1/devices/{key}/samples": {
            "get": {
                "summary": "Get raw samples for a device",
                "parameters": [
                    {"name": "key", "in": "path", "required": True, "schema": {"type": "string"}},
                    {"name": "start", "in": "query", "schema": {"type": "integer"}, "description": "Start timestamp"},
                    {"name": "end", "in": "query", "schema": {"type": "integer"}, "description": "End timestamp"},
                    {"name": "limit", "in": "query", "schema": {"type": "integer", "default": 1000}},
                ],
                "responses": {"200": {"description": "Sample data"}},
            },
        },
        "/api/v1/devices/{key}/hourly": {
            "get": {"summary": "Get hourly aggregated energy data", "responses": {"200": {"description": "Hourly data"}}},
        },
        "/api/v1/costs": {
            "get": {"summary": "Get cost breakdown", "responses": {"200": {"description": "Cost data"}}},
        },
        "/api/v1/spot_prices": {
            "get": {"summary": "Get spot market prices", "responses": {"200": {"description": "Spot price data"}}},
        },
        "/api/v1/co2": {
            "get": {"summary": "Get CO2 grid intensity", "responses": {"200": {"description": "CO2 data"}}},
        },
    },
    "components": {
        "securitySchemes": {
            "BearerAuth": {"type": "http", "scheme": "bearer"},
        },
    },
    "security": [{"BearerAuth": []}],
}


def handle_v1_request(path: str, params: Dict[str, str], db, cfg) -> Dict[str, Any]:
    """Route and handle /api/v1/* requests."""

    # Strip /api/v1 prefix
    route = path.replace("/api/v1", "").rstrip("/") or "/"

    if route == "/openapi.json":
        return {"ok": True, "data": OPENAPI_SPEC}

    if route == "/devices":
        return _list_devices(cfg)

    if route.startswith("/devices/") and route.endswith("/samples"):
        key = route.split("/")[2]
        return _get_samples(db, key, params)

    if route.startswith("/devices/") and route.endswith("/hourly"):
        key = route.split("/")[2]
        return _get_hourly(db, key, params)

    if route == "/costs":
        return _get_costs(db, cfg, params)

    if route == "/spot_prices":
        return _get_spot_prices(db, cfg, params)

    if route == "/co2":
        return _get_co2(db, cfg, params)

    if route == "/status":
        return _get_status(cfg)

    return {"ok": False, "error": f"Unknown endpoint: {route}"}


def _list_devices(cfg) -> Dict:
    devices = []
    for d in cfg.devices:
        devices.append({
            "key": d.key,
            "name": d.name,
            "host": d.host,
            "kind": getattr(d, "kind", "em"),
            "phases": getattr(d, "phases", 3),
        })
    return {"ok": True, "data": {"devices": devices}}


def _get_samples(db, key: str, params: Dict) -> Dict:
    start = int(params.get("start", 0) or 0)
    end = int(params.get("end", 0) or 0)
    limit = min(int(params.get("limit", 1000) or 1000), 10000)

    if not start:
        start = int(time.time()) - 86400
    if not end:
        end = int(time.time())

    try:
        df = db.query_samples(key, start, end)
        if df is None or df.empty:
            return {"ok": True, "data": {"samples": [], "count": 0}}

        df = df.head(limit)
        samples = []
        for _, row in df.iterrows():
            s = {"timestamp": int(row.get("timestamp", 0))}
            for col in ["total_power", "energy_kwh", "a_act_power", "b_act_power", "c_act_power",
                        "a_voltage", "b_voltage", "c_voltage", "a_current", "b_current", "c_current"]:
                val = row.get(col)
                if val is not None and str(val) != 'nan':
                    s[col] = round(float(val), 4)
            samples.append(s)

        return {"ok": True, "data": {"samples": samples, "count": len(samples)}}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _get_hourly(db, key: str, params: Dict) -> Dict:
    start = int(params.get("start", 0) or 0)
    end = int(params.get("end", 0) or 0)

    if not start:
        start = int(time.time()) - 7 * 86400
    if not end:
        end = int(time.time())

    try:
        df = db.query_hourly(key, start, end)
        if df is None or df.empty:
            return {"ok": True, "data": {"hourly": [], "count": 0}}

        hourly = []
        for _, row in df.iterrows():
            hourly.append({
                "hour_ts": int(row.get("hour_ts", 0)),
                "kwh": round(float(row.get("kwh", 0) or 0), 4),
                "avg_power_w": round(float(row.get("avg_power_w", 0) or 0), 1),
                "max_power_w": round(float(row.get("max_power_w", 0) or 0), 1),
            })

        return {"ok": True, "data": {"hourly": hourly, "count": len(hourly)}}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _get_costs(db, cfg, params: Dict) -> Dict:
    period = params.get("period", "month")
    now = int(time.time())

    if period == "today":
        start = (now // 86400) * 86400
    elif period == "week":
        start = now - 7 * 86400
    elif period == "year":
        start = now - 365 * 86400
    else:  # month
        start = now - 30 * 86400

    total_kwh = 0.0
    devices_data = []

    for dev in cfg.devices:
        if getattr(dev, 'kind', 'em') != 'em':
            continue
        try:
            df = db.query_hourly(dev.key, start, now)
            if df is not None and not df.empty:
                kwh = float(df["kwh"].sum())
                total_kwh += kwh
                devices_data.append({"device_key": dev.key, "name": dev.name, "kwh": round(kwh, 3)})
        except Exception:
            pass

    price = cfg.pricing.electricity_price_eur_per_kwh
    cost = total_kwh * price

    return {"ok": True, "data": {
        "period": period,
        "total_kwh": round(total_kwh, 3),
        "total_cost_eur": round(cost, 2),
        "price_eur_per_kwh": price,
        "devices": devices_data,
    }}


def _get_spot_prices(db, cfg, params: Dict) -> Dict:
    start = int(params.get("start", 0) or 0)
    end = int(params.get("end", 0) or 0)
    zone = params.get("zone", getattr(cfg.spot_price, 'bidding_zone', 'DE-LU'))

    if not start:
        start = int(time.time()) - 24 * 3600
    if not end:
        end = int(time.time()) + 24 * 3600

    try:
        df = db.query_spot_prices(zone, start, end)
        if df is None or df.empty:
            return {"ok": True, "data": {"prices": [], "zone": zone}}

        prices = []
        for _, row in df.iterrows():
            prices.append({
                "slot_ts": int(row["slot_ts"]),
                "price_eur_mwh": round(float(row["price_eur_mwh"]), 2),
                "price_ct_kwh": round(float(row["price_eur_mwh"]) / 10.0, 2),
            })

        return {"ok": True, "data": {"prices": prices, "zone": zone, "count": len(prices)}}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _get_co2(db, cfg, params: Dict) -> Dict:
    start = int(params.get("start", 0) or 0)
    end = int(params.get("end", 0) or 0)
    zone = params.get("zone", getattr(cfg.co2, 'bidding_zone', 'DE_LU'))

    if not start:
        start = int(time.time()) - 24 * 3600
    if not end:
        end = int(time.time()) + 24 * 3600

    try:
        df = db.query_co2_intensity(zone, start, end)
        if df is None or df.empty:
            return {"ok": True, "data": {"intensity": [], "zone": zone}}

        intensity = []
        for _, row in df.iterrows():
            intensity.append({
                "hour_ts": int(row["hour_ts"]),
                "g_per_kwh": round(float(row["intensity_g_per_kwh"]), 1),
            })

        return {"ok": True, "data": {"intensity": intensity, "zone": zone, "count": len(intensity)}}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _get_status(cfg) -> Dict:
    return {"ok": True, "data": {
        "version": cfg.version,
        "devices_count": len(cfg.devices),
        "features": {
            "spot_price": getattr(cfg.spot_price, "enabled", False),
            "co2": getattr(cfg.co2, "enabled", False),
            "solar": getattr(cfg.solar, "enabled", False),
            "mqtt": getattr(cfg.mqtt, "enabled", False),
            "forecast": getattr(cfg.forecast, "enabled", False),
            "weather": getattr(cfg.weather, "enabled", False),
            "anomaly": getattr(cfg.anomaly, "enabled", False),
        },
        "timestamp": int(time.time()),
    }}
