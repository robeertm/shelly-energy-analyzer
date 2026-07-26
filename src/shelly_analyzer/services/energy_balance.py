"""Household energy balance — the single source of truth for the Costs and
CO₂ tabs once a PV / battery / grid source is present.

The analyzer historically priced every device as if all of its energy came from
the grid at the consumer tariff. That is correct for a grid-only home but wrong
once there is PV and/or storage: some energy is self-produced (cheaper, and only
carrying the panels' embodied CO₂), some is fed back to the grid (revenue, and a
CO₂ credit), and a tenant sub-circuit is billed at the full tariff regardless of
the owner's solar.

This module derives one physically consistent balance for a time range from the
device series already in the database:

    load = grid_import − grid_export + pv_production − batt_charge + batt_discharge

so the numbers on the Costs and CO₂ tabs always add up to the same picture. It is
fully generic: the grid meter, PV series, battery series and tenant circuits are
all taken from configuration, so any inverter/battery (Shelly-measured or bridged
in via the external PV source) works without code changes.

Sign conventions (as stored by Shelly EM data and the external PV source):
    grid meter : + = import from grid, − = export / feed-in
    battery    : + = charging,          − = discharging
    pv series  : ≥ 0 production
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Reserved synthetic device keys written by services/pv_source.py.
_PV_KEY = "pv"
_BATTERY_KEY = "battery"
_GRID_EXT_KEY = "grid_ext"


@dataclass
class EnergyBalance:
    """A consistent household balance over a time range (all kWh, magnitudes)."""

    # Grid connection.
    grid_import_kwh: float = 0.0      # drawn from grid
    grid_export_kwh: float = 0.0      # fed into grid (feed-in)
    # PV.
    pv_production_kwh: float = 0.0
    self_consumption_kwh: float = 0.0  # PV used on-site (production − export)
    # Battery.
    battery_charge_kwh: float = 0.0
    battery_discharge_kwh: float = 0.0
    battery_soc_pct: Optional[float] = None
    # Loads.
    total_load_kwh: float = 0.0       # everything the property consumed
    tenant_load_kwh: float = 0.0      # billed to tenants (always full tariff)
    owner_load_kwh: float = 0.0       # total − tenant
    # Metadata.
    autarky_pct: float = 0.0          # share of load NOT drawn from the grid
    self_sufficiency_pct: float = 0.0  # alias kept for clarity
    has_pv: bool = False
    has_battery: bool = False
    has_grid_meter: bool = False
    tenant_names: List[str] = field(default_factory=list)
    tenant_breakdown: Dict[str, float] = field(default_factory=dict)  # name → kWh

    def as_dict(self) -> Dict[str, Any]:
        return {
            "grid_import_kwh": round(self.grid_import_kwh, 3),
            "grid_export_kwh": round(self.grid_export_kwh, 3),
            "pv_production_kwh": round(self.pv_production_kwh, 3),
            "self_consumption_kwh": round(self.self_consumption_kwh, 3),
            "battery_charge_kwh": round(self.battery_charge_kwh, 3),
            "battery_discharge_kwh": round(self.battery_discharge_kwh, 3),
            "battery_soc_pct": (round(self.battery_soc_pct, 1)
                                if self.battery_soc_pct is not None else None),
            "total_load_kwh": round(self.total_load_kwh, 3),
            "tenant_load_kwh": round(self.tenant_load_kwh, 3),
            "owner_load_kwh": round(self.owner_load_kwh, 3),
            "autarky_pct": round(self.autarky_pct, 1),
            "has_pv": self.has_pv,
            "has_battery": self.has_battery,
            "has_grid_meter": self.has_grid_meter,
            "tenant_names": list(self.tenant_names),
            "tenant_breakdown": {k: round(v, 3) for k, v in self.tenant_breakdown.items()},
        }


def _hourly_split(db, key: str, start_ts: int, end_ts: int) -> tuple:
    """Return (positive_sum_kwh, negative_abs_sum_kwh) of a device's hourly kWh."""
    try:
        df = db.query_hourly(key, start_ts=start_ts, end_ts=end_ts)
    except Exception:
        return 0.0, 0.0
    if df is None or df.empty or "kwh" not in df.columns:
        return 0.0, 0.0
    import pandas as pd
    col = pd.to_numeric(df["kwh"], errors="coerce").fillna(0.0)
    pos = float(col[col > 0].sum())
    neg = float(col[col < 0].abs().sum())
    return pos, neg


def _hourly_sum(db, key: str, start_ts: int, end_ts: int) -> float:
    pos, neg = _hourly_split(db, key, start_ts, end_ts)
    return pos - neg


def compute_balance(db, cfg, start_ts: int, end_ts: int) -> EnergyBalance:
    """Compute the household energy balance for ``[start_ts, end_ts)``.

    Everything is optional: with no PV/grid meter configured the balance simply
    reports the grid figures it can find (or zeros), and callers fall back to the
    legacy per-device costing. ``has_pv`` / ``has_grid_meter`` tell the caller
    whether a solar-aware breakdown is meaningful.
    """
    bal = EnergyBalance()
    solar = getattr(cfg, "solar", None)
    pv_source = getattr(cfg, "pv_source", None)
    solar_on = bool(getattr(solar, "enabled", False)) if solar else False

    # ── Grid meter (signed): dedicated PV/net meter, else the grid meter, else
    #    the synthetic grid_ext series from the external source. ──────────────
    grid_key = ""
    if solar:
        grid_key = (str(getattr(solar, "grid_meter_device_key", "") or "")
                    or str(getattr(solar, "pv_meter_device_key", "") or ""))
    if not grid_key and pv_source is not None and getattr(pv_source, "enabled", False):
        # Only if the user actually mapped a grid entity/topic.
        if (getattr(pv_source, "grid_power_entity", "")
                or getattr(pv_source, "mqtt_grid_power_topic", "")):
            grid_key = _GRID_EXT_KEY
    if grid_key:
        imp, exp = _hourly_split(db, grid_key, start_ts, end_ts)
        bal.grid_import_kwh = imp
        bal.grid_export_kwh = exp
        bal.has_grid_meter = True

    # ── PV production (measured). ────────────────────────────────────────────
    pv_key = str(getattr(solar, "pv_production_device_key", "") or "") if solar else ""
    if not pv_key and pv_source is not None and getattr(pv_source, "enabled", False):
        if getattr(pv_source, "pv_power_entity", "") or getattr(pv_source, "mqtt_pv_power_topic", ""):
            pv_key = _PV_KEY
    if pv_key:
        prod, _ = _hourly_split(db, pv_key, start_ts, end_ts)
        if prod > 0:
            bal.pv_production_kwh = prod
            bal.has_pv = True

    # ── Battery charge / discharge. ──────────────────────────────────────────
    batt_key = _BATTERY_KEY
    if pv_source is not None and getattr(pv_source, "enabled", False) and (
            getattr(pv_source, "battery_power_entity", "")
            or getattr(pv_source, "mqtt_battery_power_topic", "")):
        charge, discharge = _hourly_split(db, batt_key, start_ts, end_ts)
        if charge > 0 or discharge > 0:
            bal.battery_charge_kwh = charge
            bal.battery_discharge_kwh = discharge
            bal.has_battery = True
    try:
        from shelly_analyzer.services.pv_source import latest_readings
        _lr = latest_readings()
        if _lr.get("soc_pct") is not None:
            bal.battery_soc_pct = float(_lr["soc_pct"])
    except Exception:
        pass

    # ── Self-consumed PV = production − export (clamped). ────────────────────
    bal.self_consumption_kwh = max(0.0, bal.pv_production_kwh - bal.grid_export_kwh)

    # ── Total load from the supply-side identity (no sub-meter double count):
    #    load = grid_import − grid_export + pv_production − charge + discharge.
    if bal.has_grid_meter or bal.has_pv:
        bal.total_load_kwh = max(0.0, (
            bal.grid_import_kwh - bal.grid_export_kwh
            + bal.pv_production_kwh
            - bal.battery_charge_kwh + bal.battery_discharge_kwh
        ))

    # ── Tenant circuits: billed at the full tariff regardless of solar. ──────
    tenant_cfg = getattr(cfg, "tenant", None)
    tenant_keys: List[str] = []
    key_to_tenant: Dict[str, str] = {}
    if tenant_cfg is not None and getattr(tenant_cfg, "enabled", False):
        for tdef in (getattr(tenant_cfg, "tenants", []) or []):
            tname = getattr(tdef, "name", "") or getattr(tdef, "tenant_id", "") or "Tenant"
            for k in (getattr(tdef, "device_keys", []) or []):
                k = str(k)
                if k and k not in tenant_keys:
                    tenant_keys.append(k)
                    key_to_tenant[k] = tname
    for k in tenant_keys:
        kwh = max(0.0, _hourly_sum(db, k, start_ts, end_ts))
        if kwh <= 0:
            continue
        tname = key_to_tenant.get(k, "Tenant")
        bal.tenant_breakdown[tname] = bal.tenant_breakdown.get(tname, 0.0) + kwh
        bal.tenant_load_kwh += kwh
    bal.tenant_names = list(bal.tenant_breakdown.keys())

    # Owner load = total − tenant (never negative). When we have no supply-side
    # load (grid-only, no meter), leave owner_load at 0 — the caller uses the
    # legacy per-device table instead.
    if bal.total_load_kwh > 0:
        bal.owner_load_kwh = max(0.0, bal.total_load_kwh - bal.tenant_load_kwh)
        bal.autarky_pct = min(100.0, max(0.0,
            (1.0 - bal.grid_import_kwh / bal.total_load_kwh) * 100.0))
    bal.self_sufficiency_pct = bal.autarky_pct

    return bal
