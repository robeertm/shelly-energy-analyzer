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


def _resolve_source_keys(cfg) -> tuple:
    """Return (grid_key, pv_key, batt_key) for the balance/CO₂ chain.

    Mirrors the resolution in :func:`compute_balance`: a dedicated grid/PV meter
    from ``SolarConfig`` if set, else the synthetic series from the external PV
    source when the user mapped the corresponding entity/topic.
    """
    solar = getattr(cfg, "solar", None)
    pv_source = getattr(cfg, "pv_source", None)
    _pv_enabled = pv_source is not None and getattr(pv_source, "enabled", False)

    grid_key = ""
    if solar:
        grid_key = (str(getattr(solar, "grid_meter_device_key", "") or "")
                    or str(getattr(solar, "pv_meter_device_key", "") or ""))
    if not grid_key and _pv_enabled and (
            getattr(pv_source, "grid_power_entity", "")
            or getattr(pv_source, "mqtt_grid_power_topic", "")):
        grid_key = _GRID_EXT_KEY

    pv_key = str(getattr(solar, "pv_production_device_key", "") or "") if solar else ""
    if not pv_key and _pv_enabled and (
            getattr(pv_source, "pv_power_entity", "")
            or getattr(pv_source, "mqtt_pv_power_topic", "")):
        pv_key = _PV_KEY

    batt_key = ""
    if _pv_enabled and (getattr(pv_source, "battery_power_entity", "")
                        or getattr(pv_source, "mqtt_battery_power_topic", "")):
        batt_key = _BATTERY_KEY
    return grid_key, pv_key, batt_key


def _tenant_key_map(cfg) -> tuple:
    """Return (tenant_keys, key_to_tenant_name)."""
    tenant_keys: List[str] = []
    key_to_tenant: Dict[str, str] = {}
    tenant_cfg = getattr(cfg, "tenant", None)
    if tenant_cfg is not None and getattr(tenant_cfg, "enabled", False):
        for tdef in (getattr(tenant_cfg, "tenants", []) or []):
            tname = getattr(tdef, "name", "") or getattr(tdef, "tenant_id", "") or "Tenant"
            for k in (getattr(tdef, "device_keys", []) or []):
                k = str(k)
                if k and k not in tenant_keys:
                    tenant_keys.append(k)
                    key_to_tenant[k] = tname
    return tenant_keys, key_to_tenant


@dataclass
class Co2Breakdown:
    """A physically consistent CO₂ attribution over a time range (all kg).

    Feed-in (export) is energy leaving the property: it is **never** valued as a
    negative footprint or a credit here — the CO₂ tab must never show negative
    grams. Every consumed kWh is charged the intensity of the source that
    actually served it in that hour: grid import at the (hourly) grid mix, PV
    self-consumption and battery discharge at their embodied factors.
    """

    property_kg: float = 0.0        # on-site consumption footprint (owner + tenant), ≥ 0
    owner_kg: float = 0.0
    tenant_kg: float = 0.0
    tenant_breakdown: Dict[str, float] = field(default_factory=dict)  # name → kg
    grid_kg: float = 0.0            # operational grid-import CO₂
    pv_embodied_kg: float = 0.0     # embodied CO₂ of directly self-consumed PV
    battery_embodied_kg: float = 0.0
    solar_saved_kg: float = 0.0     # avoided vs. buying the same solar energy from the grid
    load_kwh: float = 0.0           # total on-site consumption
    pv_self_kwh: float = 0.0        # PV used on-site directly (not stored, not exported)
    battery_discharge_kwh: float = 0.0
    grid_import_kwh: float = 0.0
    tenant_load_kwh: float = 0.0
    effective_intensity: float = 0.0   # property_kg × 1000 / load_kwh (g/kWh)
    grid_intensity_avg: float = 0.0
    solar_share_pct: float = 0.0    # share of non-battery load served by PV
    autarky_pct: float = 0.0
    has_solar: bool = False

    def as_dict(self) -> Dict[str, Any]:
        return {
            "net_kg": round(self.property_kg, 3),
            "property_kg": round(self.property_kg, 3),
            "owner_kg": round(self.owner_kg, 3),
            "tenant_kg": round(self.tenant_kg, 3),
            "tenant_breakdown": {k: round(v, 3) for k, v in self.tenant_breakdown.items()},
            "grid_kg": round(self.grid_kg, 3),
            "pv_embodied_kg": round(self.pv_embodied_kg, 3),
            "battery_embodied_kg": round(self.battery_embodied_kg, 3),
            "solar_saved_kg": round(self.solar_saved_kg, 3),
            "load_kwh": round(self.load_kwh, 3),
            "self_consumption_kwh": round(self.pv_self_kwh, 3),
            "battery_discharge_kwh": round(self.battery_discharge_kwh, 3),
            "grid_import_kwh": round(self.grid_import_kwh, 3),
            "tenant_load_kwh": round(self.tenant_load_kwh, 3),
            "effective_intensity": round(self.effective_intensity, 1),
            "grid_intensity": round(self.grid_intensity_avg, 1),
            "solar_share_pct": round(self.solar_share_pct, 1),
            "autarky_pct": round(self.autarky_pct, 1),
        }


def compute_co2(db, cfg, start_ts: int, end_ts: int,
                intensity_by_hour: Optional[Dict[int, float]] = None,
                default_intensity: float = 380.0) -> Co2Breakdown:
    """Attribute CO₂ to on-site consumption over ``[start_ts, end_ts]``, per hour.

    The generation chain is honoured hour by hour (sign conventions per module
    docstring: grid + = import / − = export, battery + = charge / − = discharge,
    PV ≥ 0):

        G_imp / G_exp   = max(0, grid) / max(0, −grid)
        B_chg / B_dis   = max(0, batt) / max(0, −batt)
        PV_direct       = max(0, pv − G_exp − B_chg)   # PV used directly by loads
        pool            = PV_direct + G_imp            # the non-battery supply bus
        solar_frac      = PV_direct / pool
        eff             = solar_frac·pv_emb + (1−solar_frac)·grid_intensity

    Every non-battery kWh is charged ``eff``; battery discharge (owner-served,
    since the tenant is grid-parallel and never battery-fed) is charged its
    embodied factor. **Export is charged nothing** — no negative CO₂, ever.

    The tenant, being grid-parallel, sees exactly the blended non-battery
    intensity ``eff``: when PV surplus flows the tenant's share of it lowers the
    tenant's CO₂ below the pure grid mix; at night it is the full grid mix. The
    owner keeps the remainder (including all battery discharge).
    """
    import pandas as pd

    imap = intensity_by_hour or {}
    di = float(default_intensity) if default_intensity and default_intensity > 0 else 380.0
    solar = getattr(cfg, "solar", None)
    pv_emb = float(getattr(solar, "pv_embodied_g_per_kwh", 40.0) or 0.0) if solar else 0.0
    bat_emb = float(getattr(solar, "battery_embodied_g_per_kwh", 60.0) or 0.0) if solar else 0.0

    grid_key, pv_key, batt_key = _resolve_source_keys(cfg)
    res = Co2Breakdown()
    if not grid_key and not pv_key:
        return res  # no supply-side series → caller uses the legacy per-device table

    def _series(key: str) -> Dict[int, float]:
        out: Dict[int, float] = {}
        if not key:
            return out
        try:
            df = db.query_hourly(key, start_ts=start_ts, end_ts=end_ts)
        except Exception:
            return out
        if df is None or df.empty or "kwh" not in df.columns:
            return out
        for h, k in zip(df["hour_ts"], pd.to_numeric(df["kwh"], errors="coerce").fillna(0.0)):
            out[int(h)] = float(k)
        return out

    g_s = _series(grid_key)
    pv_s = _series(pv_key)
    b_s = _series(batt_key)
    tenant_keys, key_to_tenant = _tenant_key_map(cfg)
    ten_s: Dict[int, Dict[str, float]] = {}
    for tk in tenant_keys:
        for h, k in _series(tk).items():
            row = ten_s.setdefault(h, {})
            tname = key_to_tenant[tk]
            row[tname] = row.get(tname, 0.0) + max(0.0, k)

    hours = set(g_s) | set(pv_s) | set(b_s) | set(ten_s)
    pool_sum = 0.0
    pvd_sum = 0.0
    int_sum = 0.0
    int_n = 0
    for h in hours:
        g = g_s.get(h, 0.0)
        pv = max(0.0, pv_s.get(h, 0.0))
        b = b_s.get(h, 0.0)
        gi = max(0.0, g)
        ge = max(0.0, -g)
        bch = max(0.0, b)
        bdis = max(0.0, -b)
        ci = imap.get(int(h), di)
        if not (ci and ci > 0):
            ci = di
        pv_direct = max(0.0, pv - ge - bch)
        pool = pv_direct + gi                    # non-battery load bus
        frac = (pv_direct / pool) if pool > 0 else 0.0
        eff = frac * pv_emb + (1.0 - frac) * ci

        grid_g = gi * ci
        pvd_g = pv_direct * pv_emb
        bat_g = bdis * bat_emb
        res.grid_kg += grid_g / 1000.0
        res.pv_embodied_kg += pvd_g / 1000.0
        res.battery_embodied_kg += bat_g / 1000.0
        res.property_kg += (grid_g + pvd_g + bat_g) / 1000.0
        res.load_kwh += pool + bdis
        res.pv_self_kwh += pv_direct
        res.battery_discharge_kwh += bdis
        res.grid_import_kwh += gi
        # What this solar+battery energy would have cost had it come from the grid.
        solar_energy = pv_direct + bdis
        res.solar_saved_kg += max(0.0, (solar_energy * ci - pvd_g - bat_g)) / 1000.0

        pool_sum += pool
        pvd_sum += pv_direct
        int_sum += ci
        int_n += 1

        for tname, tk in ten_s.get(h, {}).items():
            tk_c = min(tk, pool) if pool > 0 else 0.0   # tenant load is non-battery
            tg = tk_c * eff
            res.tenant_breakdown[tname] = res.tenant_breakdown.get(tname, 0.0) + tg / 1000.0
            res.tenant_kg += tg / 1000.0
            res.tenant_load_kwh += max(0.0, tk)

    res.owner_kg = max(0.0, res.property_kg - res.tenant_kg)
    res.grid_intensity_avg = (int_sum / int_n) if int_n else di
    res.effective_intensity = (res.property_kg * 1000.0 / res.load_kwh) if res.load_kwh > 0 else 0.0
    res.solar_share_pct = (pvd_sum / pool_sum * 100.0) if pool_sum > 0 else 0.0
    res.autarky_pct = min(100.0, max(0.0, (1.0 - res.grid_import_kwh / res.load_kwh) * 100.0)) if res.load_kwh > 0 else 0.0
    res.has_solar = (res.pv_embodied_kg > 0.0 or res.battery_embodied_kg > 0.0 or res.solar_saved_kg > 0.0)
    return res


def compute_balance(db, cfg, start_ts: int, end_ts: int,
                    live_today_kwh: Optional[Dict[str, float]] = None,
                    today_start_ts: Optional[int] = None) -> EnergyBalance:
    """Compute the household energy balance for ``[start_ts, end_ts)``.

    ``live_today_kwh`` (device_key → today's kWh from the live store) and
    ``today_start_ts`` (local-midnight epoch) let the balance stay correct for the
    current day: real sub-meters (tenant Shellys) only reach the DB on sync, so
    their hourly rollups lag intraday. When both are given and the period covers
    today, the tenant load uses hourly rollups up to today's midnight plus the
    live daily accumulator for today — no double count, always current.

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
    _use_live = (live_today_kwh is not None and today_start_ts is not None
                 and end_ts > int(today_start_ts))
    for k in tenant_keys:
        if _use_live:
            # Hourly rollups strictly BEFORE today's midnight (synced days) + live
            # daily accumulator for today (real sub-meters lag the DB until sync).
            # query_hourly bounds are inclusive, so end at midnight-1 to exclude
            # today's midnight-hour bucket (else it double-counts with live).
            cap = max(int(today_start_ts), int(start_ts))
            kwh = max(0.0, _hourly_sum(db, k, start_ts, cap - 1))
            kwh += max(0.0, float((live_today_kwh or {}).get(k, 0.0) or 0.0))
        else:
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
