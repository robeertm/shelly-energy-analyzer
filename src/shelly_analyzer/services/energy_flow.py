"""The energy-flow picture: where the energy came from and where it went.

One structure for a period (kWh) and for this instant (W), read off the same
supply chain as the CO₂/Costs/Solar tabs so every arrow carries the number
the other tabs show:

    sources  PV · battery (discharge) · grid (import)
    sinks    house consumers (each meter once, wiring-aware) · battery
             (charge) · grid (export)
    flows    pv→house, pv→battery, pv→grid, grid→house, grid→battery,
             battery→house — and every consumer split by origin
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)
_TZ = ZoneInfo("Europe/Berlin")


def _bounds(period: str, now: datetime):
    t0 = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if period == "week":
        return t0 - timedelta(days=now.weekday()), now
    if period == "month":
        return t0.replace(day=1), now
    if period == "year":
        return t0.replace(month=1, day=1), now
    if period == "yesterday":
        return t0 - timedelta(days=1), t0
    return t0, now


def _name(cfg, d) -> str:
    """A demo device carries an i18n key as its name until the next config
    load; resolve it here so the diagram never shows ``demo.device.…``."""
    try:
        from shelly_analyzer.i18n import resolve_name
        lang = str(getattr(getattr(cfg, "ui", None), "language", "en") or "en")
        return resolve_name(lang, str(getattr(d, "name", "") or getattr(d, "key", "")))
    except Exception:
        return str(getattr(d, "name", "") or getattr(d, "key", ""))


def _consumers(cfg) -> List[Any]:
    """Consumer devices to draw, each meter once (a child wired behind a parent
    is a branch off it, so the parent is shown net of it)."""
    from shelly_analyzer.services.energy_balance import device_role
    from shelly_analyzer.services.net_display import flow_children
    devs = [d for d in (getattr(cfg, "devices", []) or [])
            if str(getattr(d, "kind", "em")) == "em"
            and device_role(cfg, getattr(d, "key", "")) in ("owner", "tenant")]
    return devs, flow_children(devs)


def compute_energy_flow(db, cfg, period: str = "today", now: Optional[datetime] = None,
                        live_today: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
    """``live_today`` (device_key → kWh so far today, from the live store) lets
    a Shelly sub-meter that only reaches the database on sync still show
    today's full figure; the origin split of the hours already synced is
    scaled onto it."""
    from shelly_analyzer.services.energy_balance import (
        build_supply_chain, device_role, _series_map, _resolve_source_keys)
    now = now or datetime.now(_TZ)
    s_dt, e_dt = _bounds(period, now)
    s_ts, e_ts = int(s_dt.timestamp()), int(e_dt.timestamp())
    grid_key, pv_key, batt_key = _resolve_source_keys(cfg)
    devs, kids = _consumers(cfg)

    imap: Dict[int, float] = {}
    fb = float(getattr(getattr(cfg, "pricing", None), "co2_intensity_g_per_kwh", 380.0) or 380.0)
    try:
        import pandas as pd
        zone = str(getattr(getattr(cfg, "co2", None), "bidding_zone", "DE_LU") or "DE_LU")
        df = db.query_co2_intensity(zone, s_ts, e_ts + 3600)
        if df is not None and not df.empty:
            for h, v in zip(df["hour_ts"], pd.to_numeric(df["intensity_g_per_kwh"], errors="coerce")):
                if v == v and v > 0:
                    imap[int(h)] = float(v)
    except Exception:
        pass
    chain = build_supply_chain(db, cfg, s_ts, e_ts, imap, fb)

    pv = direct = bch_pv = bch_grid = ge = gi_load = bdis = load = g = 0.0
    for hm in chain.hours.values():
        pv += hm.pv; direct += hm.pv_direct; bch_pv += hm.bch_pv; bch_grid += hm.bch_grid
        ge += hm.ge; gi_load += hm.gi_load; bdis += hm.bdis; load += hm.load; g += hm.grams

    consumers = []
    cons_sum = 0.0
    for d in devs:
        hk = dict(_series_map(db, d.key, s_ts, e_ts))
        for ck in kids.get(d.key, []):
            for h, k in _series_map(db, ck, s_ts, e_ts).items():
                if h in hk:
                    hk[h] = max(0.0, hk[h] - max(0.0, k))
        role = device_role(cfg, d.key)
        parts = chain.device_grams(hk, role)
        kwh = parts["kwh"]
        if live_today and period in ("today", "week", "month", "year"):
            # Today's live accumulator, net of the children the same way.
            lt = live_today.get(d.key)
            if lt is not None:
                lt = max(0.0, float(lt))
                for ck in kids.get(d.key, []):
                    lt = max(0.0, lt - max(0.0, float(live_today.get(ck) or 0.0)))
                t0 = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
                synced_today = sum(max(0.0, k) for h, k in hk.items() if h >= t0)
                if lt > synced_today + 0.001:
                    extra = lt - synced_today
                    # The hours not yet synced get today's mix so far.
                    fg = fp = fb = 0.0
                    tk = 0.0
                    for h, k in hk.items():
                        if h >= t0 and k > 0:
                            g_, p_, b_ = chain.split(h, role)
                            fg += k * g_; fp += k * p_; fb += k * b_; tk += k
                    if tk > 0:
                        fg, fp, fb = fg / tk, fp / tk, fb / tk
                    else:
                        fg, fp, fb = 1.0, 0.0, 0.0
                    parts["kwh"] += extra; parts["kwh_grid"] += extra * fg
                    parts["kwh_pv"] += extra * fp; parts["kwh_bat"] += extra * fb
                    kwh = parts["kwh"]
        cons_sum += kwh
        consumers.append({
            "key": d.key, "name": _name(cfg, d), "role": role,
            "kwh": round(kwh, 3), "grid": round(parts["kwh_grid"], 3),
            "pv": round(parts["kwh_pv"], 3), "battery": round(parts["kwh_bat"], 3),
            "co2_g": round(parts["g"], 1),
            "net_of": list(kids.get(d.key, [])),
        })
    consumers.sort(key=lambda c: -c["kwh"])

    has_supply = chain.has_supply
    two_bus = bool(getattr(chain, "two_bus", False))
    if not has_supply:
        # Grid-only home: everything the consumers drew came off the grid —
        # and its grams are the consumers' grams (the chain has no hours of
        # its own here, so `g` would otherwise stay at zero).
        load = cons_sum
        gi_load = cons_sum
        g = sum(float(c["co2_g"]) for c in consumers)
    return {
        "ok": True, "unit": "kWh", "period": period, "has_supply": has_supply,
        "has_pv": bool(pv_key), "has_battery": bool(batt_key), "has_grid_meter": bool(grid_key),
        "tenant_bus": two_bus,
        "range": {"start": s_dt.isoformat(), "end": e_dt.isoformat()},
        "sources": {"pv": round(pv, 3), "battery": round(bdis, 3), "grid": round(gi_load + bch_grid, 3)},
        "sinks": {"house": round(load, 3), "battery": round(bch_pv + bch_grid, 3), "grid": round(ge, 3)},
        "flows": [
            {"from": "pv", "to": "house", "v": round(direct, 3)},
            {"from": "pv", "to": "battery", "v": round(bch_pv, 3)},
            {"from": "pv", "to": "grid", "v": round(ge, 3)},
            {"from": "grid", "to": "house", "v": round(gi_load, 3)},
            {"from": "grid", "to": "battery", "v": round(bch_grid, 3)},
            {"from": "battery", "to": "house", "v": round(bdis, 3)},
        ],
        "house": {
            "load": round(load, 3),
            "consumers_sum": round(cons_sum, 3),
            "autarky_pct": round((1.0 - gi_load / load) * 100.0, 1) if load > 0 else None,
            "self_consumption_pct": round((pv - ge) / pv * 100.0, 1) if pv > 0 else None,
            "co2_kg": round(g / 1000.0, 3),
            "intensity": round(g / load, 1) if load > 0 else None,
        },
        "consumers": consumers,
    }


def compute_energy_flow_live(cfg, live_snapshot: Dict[str, Any], bat_int: float = 62.0,
                             grid_intensity: float = 380.0, pv_mfg: float = 40.0) -> Dict[str, Any]:
    """Same picture for this instant, in watts, from the live store."""
    from shelly_analyzer.services.energy_balance import (
        _resolve_source_keys, _tenant_key_map, live_mix, live_mix_for_role, device_role)
    grid_key, pv_key, batt_key = _resolve_source_keys(cfg)
    devs, kids = _consumers(cfg)

    def _w(k):
        pts = live_snapshot.get(k) if k else None
        return float(pts[-1].get("power_total_w") or 0.0) if isinstance(pts, list) and pts else 0.0

    tenant_keys, _ = _tenant_key_map(cfg)
    tenant_w = sum(max(0.0, _w(k)) for k in tenant_keys)
    two_bus = bool(tenant_keys) and not bool(getattr(getattr(cfg, "solar", None), "battery_feeds_tenants", False))

    pv_w, grid_w, batt_w = _w(pv_key), _w(grid_key), _w(batt_key)
    gi = max(0.0, grid_w); ge = max(0.0, -grid_w); bch = max(0.0, batt_w); bdis = max(0.0, -batt_w)
    pv = max(0.0, pv_w)
    pv_after_export = max(0.0, pv - ge)
    bch_pv = min(bch, pv_after_export)
    bch_grid = max(0.0, bch - bch_pv)
    direct = max(0.0, pv_after_export - bch_pv)
    gi_load = max(0.0, gi - bch_grid)
    load = direct + gi_load + bdis
    mix = live_mix(pv_w, grid_w, batt_w, grid_intensity, pv_mfg, bat_int)
    consumers = []
    cons_sum = 0.0
    for d in devs:
        w = max(0.0, _w(d.key))
        for ck in kids.get(d.key, []):
            w = max(0.0, w - max(0.0, _w(ck)))
        cons_sum += w
        role = device_role(cfg, d.key)
        m = live_mix_for_role(cfg, role, pv_w, grid_w, batt_w, tenant_w, grid_intensity, pv_mfg, bat_int)
        consumers.append({"key": d.key, "name": _name(cfg, d), "role": role,
                          "kwh": round(w, 0), "grid": round(w * m["grid"], 0),
                          "pv": round(w * m["pv"], 0), "battery": round(w * m["battery"], 0),
                          "net_of": list(kids.get(d.key, []))})
    consumers.sort(key=lambda c: -c["kwh"])
    has_supply = bool(pv_key or grid_key)
    if not has_supply:
        load = cons_sum
        gi_load = cons_sum
    return {
        "ok": True, "unit": "W", "period": "now", "has_supply": has_supply,
        "has_pv": bool(pv_key), "has_battery": bool(batt_key), "has_grid_meter": bool(grid_key),
        "tenant_bus": two_bus,
        "sources": {"pv": round(pv, 0), "battery": round(bdis, 0), "grid": round(gi_load + bch_grid, 0)},
        "sinks": {"house": round(load, 0), "battery": round(bch, 0), "grid": round(ge, 0)},
        "flows": [
            {"from": "pv", "to": "house", "v": round(direct, 0)},
            {"from": "pv", "to": "battery", "v": round(bch_pv, 0)},
            {"from": "pv", "to": "grid", "v": round(ge, 0)},
            {"from": "grid", "to": "house", "v": round(gi_load, 0)},
            {"from": "grid", "to": "battery", "v": round(bch_grid, 0)},
            {"from": "battery", "to": "house", "v": round(bdis, 0)},
        ],
        "house": {"load": round(load, 0), "consumers_sum": round(cons_sum, 0),
                  "autarky_pct": round((1.0 - gi_load / load) * 100.0, 1) if load > 0 else None,
                  "self_consumption_pct": round((pv - ge) / pv * 100.0, 1) if pv > 0 else None,
                  "intensity": round(mix["intensity"], 1) if load > 0 else None,
                  "co2_g_per_h": round(load * mix["intensity"] / 1000.0, 1)},
        "consumers": consumers,
        "battery_soc_pct": None,
    }
