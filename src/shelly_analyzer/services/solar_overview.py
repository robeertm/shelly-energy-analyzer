"""Everything the Solar tab shows, computed in one place from the supply chain.

The tab used to sum "all devices except the grid meter" for the household —
which counted a wallbox behind the house meter twice and added the Shelly on
the grid connection as if it were a consumer — and priced CO₂ with a third
formula of its own. Now every figure here is read off the same hourly chain
the CO₂ and Costs tabs use (services/energy_balance.build_supply_chain), so
the four tabs cannot disagree.

Periods: today / week / month / year, each with the daily series that
belongs to it, plus the current day's hourly profile, the year's monthly
production, records, money, CO₂ and the next days' expectation.
"""
from __future__ import annotations

import calendar
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)
_TZ = ZoneInfo("Europe/Berlin")


def _period_bounds(period: str, now: datetime):
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if period == "week":
        return today - timedelta(days=now.weekday()), now
    if period == "month":
        return today.replace(day=1), now
    if period == "year":
        return today.replace(month=1, day=1), now
    if period == "30d":
        return today - timedelta(days=29), now
    return today, now


def _imap(db, cfg, s: int, e: int) -> tuple:
    import pandas as pd
    fb = float(getattr(getattr(cfg, "pricing", None), "co2_intensity_g_per_kwh", 380.0) or 380.0)
    out: Dict[int, float] = {}
    try:
        zone = str(getattr(getattr(cfg, "co2", None), "bidding_zone", "DE_LU") or "DE_LU")
        df = db.query_co2_intensity(zone, s, e + 3600)
        if df is not None and not df.empty:
            for h, v in zip(df["hour_ts"], pd.to_numeric(df["intensity_g_per_kwh"], errors="coerce")):
                if v == v and v > 0:
                    out[int(h)] = float(v)
    except Exception:
        pass
    if out:
        fb = sum(out.values()) / len(out)
    return out, fb


# Share of a year's PV yield that falls into each month at mid-European
# latitudes (PVGIS-style climatology, sums to 1). Extrapolating a summer's
# worth of data by 365/days would promise a winter that never comes; the
# observed days are weighted by the share of the year they represent instead.
_MONTH_SHARE = {1: 0.030, 2: 0.050, 3: 0.080, 4: 0.110, 5: 0.125, 6: 0.125,
                7: 0.125, 8: 0.115, 9: 0.090, 10: 0.065, 11: 0.035, 12: 0.025}


def _seasonal_scale(days: set) -> float:
    """Factor that turns the sum over ``days`` into a full-year estimate."""
    if not days:
        return 0.0
    covered = 0.0
    by_month: Dict[tuple, int] = {}
    for d in days:
        by_month[(d.year, d.month)] = by_month.get((d.year, d.month), 0) + 1
    for (y, m), n in by_month.items():
        dim = calendar.monthrange(y, m)[1]
        covered += _MONTH_SHARE[m] * min(1.0, n / dim)
    if covered >= 0.999:
        return 365.0 / len(days)
    return (1.0 / covered) if covered > 0 else 0.0


def _sum_chain(hours: Dict[int, Any]) -> Dict[str, float]:
    t = {"pv": 0.0, "export": 0.0, "import": 0.0, "charge": 0.0, "discharge": 0.0,
         "direct": 0.0, "load": 0.0, "charge_grid": 0.0, "g": 0.0}
    for hm in hours.values():
        t["pv"] += hm.pv; t["export"] += hm.ge; t["import"] += hm.gi
        t["charge"] += hm.bch; t["discharge"] += hm.bdis; t["direct"] += hm.pv_direct
        t["load"] += hm.load; t["charge_grid"] += hm.bch_grid; t["g"] += hm.grams
    return t


def compute_solar_overview(db, cfg, period: str = "today", now: Optional[datetime] = None) -> Dict[str, Any]:
    from shelly_analyzer.services.energy_balance import (
        _resolve_source_keys, build_supply_chain, compute_co2, _series_map)
    now = now or datetime.now(_TZ)
    solar = getattr(cfg, "solar", None)
    grid_key, pv_key, batt_key = _resolve_source_keys(cfg)
    if solar is None or not getattr(solar, "enabled", False) or not (pv_key or grid_key):
        return {"configured": False}

    p_start, p_end = _period_bounds(period, now)
    s_ts, e_ts = int(p_start.timestamp()), int(p_end.timestamp())
    imap, fb = _imap(db, cfg, s_ts, e_ts)
    chain = build_supply_chain(db, cfg, s_ts, e_ts, imap, fb)
    tot = _sum_chain(chain.hours)
    co2 = compute_co2(db, cfg, s_ts, e_ts, imap, fb, chain=chain)

    try:
        price = float(cfg.pricing.unit_price_gross())
    except Exception:
        price = 0.30
    try:
        feed = float(solar.effective_feed_in_for_date(now.date()))
    except Exception:
        feed = float(getattr(solar, "feed_in_tariff_eur_per_kwh", 0.082) or 0.082)

    pv = tot["pv"]
    self_use = max(0.0, pv - tot["export"])                # production that stayed (incl. what went into the battery)
    served = tot["direct"] + tot["discharge"]              # what actually served loads without the grid
    load = tot["load"]
    autarky = (1.0 - tot["import"] / load) * 100.0 if load > 0 else 0.0
    self_rate = (self_use / pv * 100.0) if pv > 0 else 0.0
    cap = float(getattr(getattr(cfg, "battery", None), "capacity_kwh", 0.0) or 0.0)
    if cap <= 0:
        cap = float(getattr(solar, "battery_kwh", 0.0) or 0.0)
    kw_peak = float(getattr(solar, "kw_peak", 0.0) or 0.0)

    # ── Daily series for the period (30 days for "today") ──────────────────
    d_start, d_end = _period_bounds("30d" if period in ("today", "week") else period, now)
    ds, de = int(d_start.timestamp()), int(d_end.timestamp())
    if (ds, de) == (s_ts, e_ts):
        dchain = chain
    else:
        dimap, dfb = _imap(db, cfg, ds, de)
        dchain = build_supply_chain(db, cfg, ds, de, dimap, dfb)
    days: Dict[str, Dict[str, float]] = {}
    for ts, hm in dchain.hours.items():
        k = datetime.fromtimestamp(ts, _TZ).strftime("%Y-%m-%d")
        d = days.setdefault(k, {"pv": 0.0, "direct": 0.0, "charge": 0.0, "export": 0.0,
                                "import": 0.0, "discharge": 0.0, "load": 0.0, "peak_w": 0.0})
        d["pv"] += hm.pv; d["direct"] += hm.pv_direct; d["charge"] += hm.bch_pv
        d["export"] += hm.ge; d["import"] += hm.gi; d["discharge"] += hm.bdis; d["load"] += hm.load
    daily = []
    for k in sorted(days):
        d = days[k]
        daily.append({"date": k, "pv_kwh": round(d["pv"], 2), "direct_kwh": round(d["direct"], 2),
                      "charge_kwh": round(d["charge"], 2), "export_kwh": round(d["export"], 2),
                      "import_kwh": round(d["import"], 2), "discharge_kwh": round(d["discharge"], 2),
                      "load_kwh": round(d["load"], 2),
                      "autarky_pct": round((1.0 - d["import"] / d["load"]) * 100.0, 1) if d["load"] > 0 else None})

    # ── Today's hourly profile ─────────────────────────────────────────────
    t0 = now.replace(hour=0, minute=0, second=0, microsecond=0)
    t_s = int(t0.timestamp())
    if period == "today":
        tchain = chain
    else:
        timap, tfb = _imap(db, cfg, t_s, int(now.timestamp()))
        tchain = build_supply_chain(db, cfg, t_s, int(now.timestamp()), timap, tfb)
    hourly = []
    for ts in sorted(tchain.hours):
        hm = tchain.hours[ts]
        hourly.append({"ts": ts, "hour": datetime.fromtimestamp(ts, _TZ).strftime("%H:%M"),
                       "pv_kwh": round(hm.pv, 3), "load_kwh": round(hm.load, 3),
                       "grid_kwh": round(hm.gi - hm.ge, 3), "battery_kwh": round(hm.bch - hm.bdis, 3),
                       "direct_kwh": round(hm.pv_direct, 3)})

    # ── Monthly production for the year (12 bars) ──────────────────────────
    y0 = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    ychain = chain if period == "year" else None
    if ychain is None:
        yimap, yfb = _imap(db, cfg, int(y0.timestamp()), int(now.timestamp()))
        ychain = build_supply_chain(db, cfg, int(y0.timestamp()), int(now.timestamp()), yimap, yfb)
    months: Dict[int, Dict[str, float]] = {m: {"pv": 0.0, "self": 0.0, "export": 0.0, "import": 0.0, "load": 0.0} for m in range(1, 13)}
    for ts, hm in ychain.hours.items():
        m = datetime.fromtimestamp(ts, _TZ).month
        months[m]["pv"] += hm.pv; months[m]["export"] += hm.ge; months[m]["import"] += hm.gi
        months[m]["self"] += hm.pv_direct + hm.bch_pv; months[m]["load"] += hm.load
    monthly = [{"month": m, "label": calendar.month_abbr[m], "pv_kwh": round(v["pv"], 1),
                "self_kwh": round(v["self"], 1), "export_kwh": round(v["export"], 1),
                "import_kwh": round(v["import"], 1), "load_kwh": round(v["load"], 1),
                "has_data": v["pv"] > 0 or v["load"] > 0}
               for m, v in months.items()]

    # ── Records (from the year's data) ─────────────────────────────────────
    ydays: Dict[str, Dict[str, float]] = {}
    for ts, hm in ychain.hours.items():
        k = datetime.fromtimestamp(ts, _TZ).strftime("%Y-%m-%d")
        d = ydays.setdefault(k, {"pv": 0.0, "import": 0.0, "load": 0.0, "export": 0.0})
        d["pv"] += hm.pv; d["import"] += hm.gi; d["load"] += hm.load; d["export"] += hm.ge
    best_pv = max(ydays.items(), key=lambda kv: kv[1]["pv"]) if ydays else None
    best_exp = max(ydays.items(), key=lambda kv: kv[1]["export"]) if ydays else None
    full = {k: v for k, v in ydays.items() if k != t0.strftime("%Y-%m-%d")}
    grid_free_days = sum(1 for v in full.values() if v["load"] > 0 and v["import"] < 0.05)
    peak_w = 0.0
    peak_ts = None
    try:
        import pandas as pd
        pdf = db.query_hourly(pv_key, start_ts=int(y0.timestamp()), end_ts=int(now.timestamp())) if pv_key else None
        if pdf is not None and not pdf.empty and "max_power_w" in pdf.columns:
            mp = pd.to_numeric(pdf["max_power_w"], errors="coerce").fillna(0.0)
            if len(mp):
                i = int(mp.idxmax())
                peak_w = float(mp.max())
                peak_ts = int(pdf.loc[i, "hour_ts"])
    except Exception:
        pass
    records = {
        "best_pv_day": {"date": best_pv[0], "kwh": round(best_pv[1]["pv"], 1)} if best_pv else None,
        "best_export_day": {"date": best_exp[0], "kwh": round(best_exp[1]["export"], 1)} if best_exp else None,
        "grid_free_days": grid_free_days,
        "days_with_data": len(full),
        "peak_w": round(peak_w, 0), "peak_ts": peak_ts,
    }

    # ── Money ──────────────────────────────────────────────────────────────
    savings = served * price
    revenue = tot["export"] * feed
    grid_cost = tot["import"] * price
    amort = None
    try:
        inv = float(getattr(solar, "investment_eur", 0.0) or 0.0)
        inst_year = int(getattr(solar, "installation_year", 0) or 0)
        ys = int((now - timedelta(days=365)).timestamp())
        yimap2, yfb2 = _imap(db, cfg, ys, int(now.timestamp()))
        y365 = build_supply_chain(db, cfg, ys, int(now.timestamp()), yimap2, yfb2)
        yt = _sum_chain(y365.hours)
        yday_set = {datetime.fromtimestamp(ts, _TZ).date() for ts in y365.hours}
        ydays_n = len(yday_set)
        scale = _seasonal_scale(yday_set)
        annual = ((yt["direct"] + yt["discharge"]) * price + yt["export"] * feed) * scale
        yco2 = compute_co2(db, cfg, ys, int(now.timestamp()), yimap2, yfb2, chain=y365)
        annual_co2 = (yco2.solar_saved_kg + yco2.export_saved_kg) * scale
        amort = {
            "investment_eur": round(inv, 2) if inv > 0 else None,
            "annual_savings_eur": round(annual, 2),
            "annual_pv_kwh": round(yt["pv"] * scale, 0),
            "annual_co2_saved_kg": round(annual_co2, 1),
            "basis_days": ydays_n,
            "payback_years": round(inv / annual, 1) if (inv > 0 and annual > 0) else None,
            "installation_year": inst_year or None,
            "years_running": (now.year - inst_year) if inst_year > 0 else None,
            "amortized_pct": (round(min(100.0, (now.year - inst_year) / (inv / annual) * 100.0), 1)
                              if (inv > 0 and annual > 0 and inst_year > 0 and now.year > inst_year) else None),
            "specific_yield_kwh_per_kwp": round(yt["pv"] * scale / kw_peak, 0) if kw_peak > 0 else None,
            # CO₂ payback of the plant itself: what it took to build the panels
            # against what they avoid per year.
            "co2_embodied_total_kg": round(kw_peak * float(getattr(solar, "co2_production_kg_per_kwp", 1000.0) or 1000.0), 0) if kw_peak > 0 else None,
            "co2_payback_years": (round(kw_peak * float(getattr(solar, "co2_production_kg_per_kwp", 1000.0) or 1000.0) / annual_co2, 1)
                                  if (kw_peak > 0 and annual_co2 > 0) else None),
        }
    except Exception:
        logger.debug("solar amortization failed", exc_info=True)

    # ── Battery ────────────────────────────────────────────────────────────
    soc = None
    try:
        from shelly_analyzer.services.pv_source import latest_readings
        lr = latest_readings()
        if lr.get("soc_pct") is not None:
            soc = round(float(lr["soc_pct"]), 1)
    except Exception:
        pass
    n_days = max(1, len({datetime.fromtimestamp(ts, _TZ).date() for ts in chain.hours}))
    battery = {
        "present": bool(batt_key), "capacity_kwh": round(cap, 1), "soc_pct": soc,
        "charge_kwh": round(tot["charge"], 2), "discharge_kwh": round(tot["discharge"], 2),
        "charge_from_grid_kwh": round(tot["charge_grid"], 2),
        "cycles": round(tot["discharge"] / cap, 2) if cap > 0 else None,
        "cycles_per_day": round(tot["discharge"] / cap / n_days, 2) if cap > 0 else None,
        "losses_kwh": round(max(0.0, tot["charge"] - tot["discharge"]), 2),
        "efficiency_pct": round(tot["discharge"] / tot["charge"] * 100.0, 1) if tot["charge"] > 1 and tot["discharge"] <= tot["charge"] else None,
        "share_of_load_pct": round(tot["discharge"] / load * 100.0, 1) if load > 0 else 0.0,
    }

    # ── Expectation for the coming days ────────────────────────────────────
    fc = None
    try:
        from shelly_analyzer.services.solar_forecast import compute_solar_forecast
        fc = compute_solar_forecast(db, cfg, days=7, now=now)
    except Exception:
        logger.debug("solar forecast failed", exc_info=True)

    return {
        "configured": True,
        "period": period,
        "range": {"start": p_start.strftime("%Y-%m-%d"), "end": now.strftime("%Y-%m-%d"), "days": n_days},
        "pv_kwh": round(pv, 3), "pv_production_kwh": round(pv, 3), "pv_measured": pv > 0,
        "feed_in_kwh": round(tot["export"], 3), "grid_kwh": round(tot["import"], 3),
        "self_kwh": round(self_use, 3), "direct_kwh": round(tot["direct"], 3),
        "served_kwh": round(served, 3),
        "household_kwh": round(load, 3), "tenant_kwh": round(co2.tenant_load_kwh, 3),
        "autarky_pct": round(max(0.0, min(100.0, autarky)), 1),
        "self_consumption_pct": round(max(0.0, min(100.0, self_rate)), 1),
        "solar_share_pct": round(co2.solar_share_pct, 1),
        "revenue_eur": round(revenue, 2), "savings_eur": round(savings, 2), "grid_cost_eur": round(grid_cost, 2),
        "net_benefit_eur": round(savings + revenue, 2),
        "feed_in_tariff": round(feed, 4), "unit_price": round(price, 4),
        "kw_peak": round(kw_peak, 2), "battery_kwh": round(cap, 1),
        "specific_yield_kwh_per_kwp": round(pv / kw_peak, 1) if kw_peak > 0 else None,
        "co2": co2.as_dict(),
        "co2_saved_kg": round(co2.solar_saved_kg + co2.export_saved_kg, 3),
        "co2_saved_self_kg": round(co2.solar_saved_kg, 3),
        "co2_saved_export_kg": round(co2.export_saved_kg, 3),
        "co2_grid_kg": round(co2.grid_kg, 3),
        "co2_embodied_kg": round(co2.pv_embodied_kg + co2.battery_origin_kg + co2.battery_embodied_kg, 3),
        "co2_intensity_g_per_kwh": round(co2.grid_intensity_avg, 1),
        "co2_effective_g_per_kwh": round(co2.effective_intensity, 1),
        "co2_source": "live" if imap else "static",
        "amortization": amort,
        "battery": battery,
        "battery_charge_kwh": round(tot["charge"], 3), "battery_discharge_kwh": round(tot["discharge"], 3),
        "battery_soc_pct": soc,
        "daily": daily, "hourly_today": hourly, "monthly": monthly, "records": records,
        "forecast": fc,
        "pv_meter_device_key": str(getattr(solar, "pv_meter_device_key", "") or ""),
        "grid_meter_device_key": grid_key, "signed_meter_device_key": grid_key,
    }
