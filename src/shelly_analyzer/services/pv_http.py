"""Local HTTP-API reader for the external PV / battery / grid source.

Currently supports the **Fronius Solar API** (``http://<inverter-ip>/solar_api``)
which needs no key or Home Assistant — just the device's IP. The reader returns
the RAW vendor values in the vendor's own sign convention; the preset declares
those conventions (e.g. Fronius ``P_Akku`` is +discharge) and pv_source applies
the normalisation to the analyzer's internal signs.

Returns the same 7-tuple as the Modbus/HA readers:
``(pv_w, batt_w, soc, grid_w, house_w, pv_today_kwh, counters)``. Never raises.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

ReadResult = Tuple[Optional[float], Optional[float], Optional[float],
                   Optional[float], Optional[float], Optional[float],
                   Dict[str, Optional[float]]]

_EMPTY: Dict[str, Optional[float]] = {
    "pv_total": None, "grid_import": None, "grid_export": None,
    "batt_charge": None, "batt_discharge": None,
}


def _num(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def parse_fronius_powerflow(payload: Dict[str, Any]) -> ReadResult:
    """Decode a Fronius ``GetPowerFlowRealtimeData`` JSON body.

    Pure function (no I/O) so it is unit-testable. Fronius Site fields:
      P_PV   – PV production (W, ≥0, null at night)
      P_Grid – + = import (draw), − = feed-in (export)
      P_Akku – + = discharging, − = charging   (BYD battery)
      rel_SOC / SOC – battery state of charge (%)
    """
    counters = dict(_EMPTY)
    try:
        site = (((payload or {}).get("Body") or {}).get("Data") or {}).get("Site") or {}
    except Exception:
        site = {}
    if not isinstance(site, dict):
        return (None, None, None, None, None, None, counters)

    pv_w = _num(site.get("P_PV"))
    if pv_w is None:
        pv_w = 0.0  # Fronius reports null for PV at night → treat as 0
    grid_w = _num(site.get("P_Grid"))
    batt_w = _num(site.get("P_Akku"))
    soc = _num(site.get("rel_SOC"))

    # Some firmwares expose SOC only per inverter.
    if soc is None:
        try:
            invs = (((payload or {}).get("Body") or {}).get("Data") or {}).get("Inverters") or {}
            for _k, _v in (invs.items() if isinstance(invs, dict) else []):
                if isinstance(_v, dict) and _v.get("SOC") is not None:
                    soc = _num(_v.get("SOC"))
                    break
        except Exception:
            pass

    house_w = None
    p_load = _num(site.get("P_Load"))
    if p_load is not None:
        house_w = abs(p_load)  # Fronius P_Load is negative for consumption

    return (pv_w, batt_w, soc, grid_w, house_w, None, counters)


def read_fronius(base_url: str, timeout: float = 8.0,
                 verify: bool = True) -> ReadResult:
    base = (base_url or "").strip().rstrip("/")
    if not base:
        return (None, None, None, None, None, None, dict(_EMPTY))
    if not base.startswith("http"):
        base = "http://" + base
    url = base + "/solar_api/v1/GetPowerFlowRealtimeData.fcgi"
    try:
        r = requests.get(url, timeout=timeout, verify=verify)
        if r.status_code != 200:
            return (None, None, None, None, None, None,
                    {**_EMPTY, "_error": f"HTTP {r.status_code}"})  # type: ignore
        return parse_fronius_powerflow(r.json())
    except Exception as e:
        logger.info("Fronius Solar API read failed: %s", e)
        return (None, None, None, None, None, None,
                {**_EMPTY, "_error": str(e)})  # type: ignore


def read_http(cfg) -> ReadResult:
    """Dispatch on ``cfg.http_kind``. Never raises."""
    kind = str(getattr(cfg, "http_kind", "fronius_solar_api") or "fronius_solar_api").lower()
    if kind == "fronius_solar_api":
        return read_fronius(str(getattr(cfg, "http_base_url", "") or ""))
    logger.info("unknown http_kind %r", kind)
    return (None, None, None, None, None, None,
            {**_EMPTY, "_error": f"unknown http_kind {kind}"})  # type: ignore
