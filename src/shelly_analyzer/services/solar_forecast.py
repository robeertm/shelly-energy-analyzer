"""PV, battery and grid expectation for the days ahead.

The consumption forecast (services/forecast.py) says how much the house will
use. This module says where it will come from: how much the roof will produce,
how much of that the house uses directly, what the battery will store and give
back, and what is left to buy or to feed in.

Production is not modelled from panel data — most owners do not know their
azimuth, and the analyzer already holds a better calibration: the roof's own
recent output against the irradiance that fell on it. Open-Meteo gives hourly
global irradiance (W/m²) for the past two weeks and the next week; the ratio
kWh-per-(W/m²·h) over the past hours IS the plant (size, orientation, shading,
inverter losses). Applied to the forecast irradiance it yields the expected
production hour by hour, with a per-hour-of-day correction so a west roof's
afternoon is not flattened into the morning.

Storage and grid follow by simulating the house hour by hour: the typical load
profile of the last two weeks against the expected production, the battery in
between (capacity, charge/discharge rate, round-trip efficiency), the grid for
the rest. That is the same order the real system works in.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import requests

logger = logging.getLogger(__name__)

_OPEN_METEO = "https://api.open-meteo.com/v1/forecast"
_TZ = ZoneInfo("Europe/Berlin")
_CACHE: Dict[str, Any] = {"key": None, "ts": 0.0, "data": None}
_CACHE_TTL_S = 1800
_LOCK = threading.Lock()


def _fetch_irradiance(lat: float, lon: float, past_days: int = 14, forecast_days: int = 7):
    """Hourly {ts: (ghi_w_m2, temp_c, cloud_pct)} from Open-Meteo, cached 30 min."""
    key = f"{round(lat, 2)},{round(lon, 2)},{past_days},{forecast_days}"
    now = time.time()
    with _LOCK:
        if _CACHE["key"] == key and (now - _CACHE["ts"]) < _CACHE_TTL_S and _CACHE["data"]:
            return _CACHE["data"]
    try:
        r = requests.get(_OPEN_METEO, params={
            "latitude": round(lat, 3), "longitude": round(lon, 3),
            "hourly": "shortwave_radiation,temperature_2m,cloud_cover",
            "past_days": max(1, min(past_days, 92)),
            "forecast_days": max(1, min(forecast_days, 16)),
            "timezone": "UTC",
        }, timeout=20)
        r.raise_for_status()
        h = r.json().get("hourly", {})
        out: Dict[int, tuple] = {}
        for i, t in enumerate(h.get("time", [])):
            try:
                dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                ts = (int(dt.timestamp()) // 3600) * 3600
                ghi = h["shortwave_radiation"][i]
                if ghi is None:
                    continue
                out[ts] = (float(ghi),
                           float(h.get("temperature_2m", [None])[i] or 0.0) if i < len(h.get("temperature_2m", [])) else 0.0,
                           float(h.get("cloud_cover", [None])[i] or 0.0) if i < len(h.get("cloud_cover", [])) else 0.0)
            except (ValueError, IndexError, TypeError, KeyError):
                continue
        with _LOCK:
            _CACHE.update(key=key, ts=now, data=out)
        return out
    except Exception as e:
        logger.warning("Open-Meteo irradiance fetch failed: %s", e)
        return None


def _site(cfg) -> Optional[tuple]:
    w = getattr(cfg, "weather", None)
    lat = float(getattr(w, "lat", 0.0) or 0.0) if w else 0.0
    lon = float(getattr(w, "lon", 0.0) or 0.0) if w else 0.0
    if abs(lat) > 0.01 or abs(lon) > 0.01:
        return lat, lon
    try:
        from shelly_analyzer.services.co2_forecast import _zone_centroid
        zone = str(getattr(getattr(cfg, "co2", None), "bidding_zone", "DE_LU") or "DE_LU")
        return _zone_centroid(zone)
    except Exception:
        return None


def _hourly_map(db, key: str, start_ts: int, end_ts: int) -> Dict[int, float]:
    from shelly_analyzer.services.energy_balance import _series_map
    return _series_map(db, key, start_ts, end_ts)


def compute_solar_forecast(db, cfg, days: int = 7, now: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
    """Expected PV / self-use / battery / grid per day for the next ``days``.

    Returns ``None`` when there is no PV series to calibrate against; a dict
    with ``available: False`` and a reason when the site or the weather is
    missing, so the tab can say why instead of showing nothing.
    """
    from shelly_analyzer.services.energy_balance import _resolve_source_keys, build_supply_chain
    grid_key, pv_key, batt_key = _resolve_source_keys(cfg)
    if not pv_key:
        return None
    now = now or datetime.now(_TZ)
    now_h = (int(now.timestamp()) // 3600) * 3600
    site = _site(cfg)
    if not site:
        return {"available": False, "reason": "no_site"}
    lat, lon = site
    irr = _fetch_irradiance(lat, lon, past_days=14, forecast_days=max(2, days + 1))
    if not irr:
        return {"available": False, "reason": "no_weather"}

    # ── Calibrate: recent production against recent irradiance ─────────────
    cal_start = now_h - 14 * 86400
    pv_hist = _hourly_map(db, pv_key, cal_start, now_h - 3600)
    num = den = 0.0
    by_hod_num: Dict[int, float] = {}
    by_hod_den: Dict[int, float] = {}
    for ts, kwh in pv_hist.items():
        g = irr.get(ts)
        if g is None:
            continue
        ghi = g[0]
        if ghi <= 5:
            continue
        hod = datetime.fromtimestamp(ts, _TZ).hour
        num += max(0.0, kwh)
        den += ghi
        by_hod_num[hod] = by_hod_num.get(hod, 0.0) + max(0.0, kwh)
        by_hod_den[hod] = by_hod_den.get(hod, 0.0) + ghi
    if den <= 0 or num <= 0:
        return {"available": False, "reason": "no_calibration"}
    ratio = num / den                     # kWh per (W/m² · h)
    # Hour-of-day correction, shrunk toward 1 where the sample is thin.
    hod_factor: Dict[int, float] = {}
    for hod in range(24):
        n_, d_ = by_hod_num.get(hod, 0.0), by_hod_den.get(hod, 0.0)
        if d_ > 0:
            raw = (n_ / d_) / ratio
            weight = min(1.0, d_ / (den / 8.0))
            hod_factor[hod] = 1.0 + (raw - 1.0) * weight
        else:
            hod_factor[hod] = 1.0
    cal_days = len({datetime.fromtimestamp(t, _TZ).date() for t in pv_hist})

    # ── Typical load profile of the last two weeks, by hour of day ─────────
    ch = build_supply_chain(db, cfg, cal_start, now_h)
    load_hod: Dict[int, List[float]] = {h: [] for h in range(24)}
    for ts, hm in ch.hours.items():
        load_hod[datetime.fromtimestamp(ts, _TZ).hour].append(hm.load)
    load_profile = {h: (sum(v) / len(v) if v else 0.0) for h, v in load_hod.items()}
    if sum(load_profile.values()) <= 0:
        # No supply-side load yet: fall back to the consumers' rollups.
        from shelly_analyzer.services.energy_balance import consumer_keys, consumer_hourly
        for k in consumer_keys(cfg):
            for ts, kwh in consumer_hourly(db, cfg, k, cal_start, now_h).items():
                load_hod[datetime.fromtimestamp(ts, _TZ).hour].append(max(0.0, kwh))
        load_profile = {h: (sum(v) / len(v) if v else 0.0) for h, v in load_hod.items()}

    # ── Battery parameters ──────────────────────────────────────────────────
    bcfg = getattr(cfg, "battery", None)
    solar = getattr(cfg, "solar", None)
    cap = float(getattr(bcfg, "capacity_kwh", 0.0) or 0.0) if bcfg else 0.0
    if cap <= 0:
        cap = float(getattr(solar, "battery_kwh", 0.0) or 0.0) if solar else 0.0
    if not batt_key:
        cap = 0.0
    eta = min(1.0, max(0.5, float(getattr(bcfg, "efficiency_pct", 95.0) or 95.0) / 100.0)) if bcfg else 0.95
    max_chg = float(getattr(bcfg, "max_charge_rate_kw", 0.0) or 0.0) if bcfg else 0.0
    max_dis = float(getattr(bcfg, "max_discharge_rate_kw", 0.0) or 0.0) if bcfg else 0.0
    usable = cap * 0.9
    soc_kwh = usable * 0.5
    try:
        from shelly_analyzer.services.pv_source import latest_readings
        soc_pct = latest_readings().get("soc_pct")
        if soc_pct is not None and cap > 0:
            soc_kwh = min(usable, max(0.0, float(soc_pct) / 100.0 * cap))
    except Exception:
        pass

    try:
        price = float(cfg.pricing.unit_price_gross())
    except Exception:
        price = 0.30
    try:
        feed = float(solar.effective_feed_in_for_date(now.date())) if solar else 0.082
    except Exception:
        feed = 0.082

    # ── Simulate hour by hour from now to the end of the horizon ───────────
    today0 = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_ts = int((today0 + timedelta(days=days)).timestamp())
    hours_out: List[Dict[str, Any]] = []
    days_out: Dict[str, Dict[str, float]] = {}
    ts = now_h
    while ts < end_ts:
        g = irr.get(ts)
        dt_l = datetime.fromtimestamp(ts, _TZ)
        hod = dt_l.hour
        ghi = g[0] if g else 0.0
        pv = max(0.0, ghi * ratio * hod_factor.get(hod, 1.0)) if ghi > 5 else 0.0
        load = load_profile.get(hod, 0.0)
        direct = min(pv, load)
        surplus = pv - direct
        deficit = load - direct
        chg = dis = 0.0
        if cap > 0 and surplus > 0:
            room = max(0.0, usable - soc_kwh)
            chg = min(surplus, room / eta if eta > 0 else room, max_chg if max_chg > 0 else surplus)
            soc_kwh += chg * eta
            surplus -= chg
        if cap > 0 and deficit > 0:
            dis = min(deficit, soc_kwh, max_dis if max_dis > 0 else deficit)
            soc_kwh -= dis
            deficit -= dis
        export = surplus
        imp = deficit
        day = dt_l.strftime("%Y-%m-%d")
        d = days_out.setdefault(day, {"pv_kwh": 0.0, "load_kwh": 0.0, "direct_kwh": 0.0,
                                      "charge_kwh": 0.0, "discharge_kwh": 0.0,
                                      "export_kwh": 0.0, "import_kwh": 0.0, "hours": 0,
                                      "ghi_sum": 0.0, "cloud_sum": 0.0, "temp_max": -99.0})
        d["pv_kwh"] += pv; d["load_kwh"] += load; d["direct_kwh"] += direct
        d["charge_kwh"] += chg; d["discharge_kwh"] += dis
        d["export_kwh"] += export; d["import_kwh"] += imp; d["hours"] += 1
        d["ghi_sum"] += ghi
        d["cloud_sum"] += (g[2] if g else 0.0)
        d["temp_max"] = max(d["temp_max"], (g[1] if g else -99.0))
        hours_out.append({"ts": ts, "pv_kwh": round(pv, 3), "load_kwh": round(load, 3),
                          "charge_kwh": round(chg, 3), "discharge_kwh": round(dis, 3),
                          "export_kwh": round(export, 3), "import_kwh": round(imp, 3),
                          "soc_kwh": round(soc_kwh, 2), "ghi": round(ghi, 0)})
        ts += 3600

    day_list = []
    for day, d in sorted(days_out.items()):
        load = d["load_kwh"]
        aut = (1.0 - d["import_kwh"] / load) * 100.0 if load > 0 else 0.0
        day_list.append({
            "date": day,
            "weekday": datetime.strptime(day, "%Y-%m-%d").strftime("%a"),
            "pv_kwh": round(d["pv_kwh"], 2), "load_kwh": round(load, 2),
            "direct_kwh": round(d["direct_kwh"], 2),
            "charge_kwh": round(d["charge_kwh"], 2), "discharge_kwh": round(d["discharge_kwh"], 2),
            "export_kwh": round(d["export_kwh"], 2), "import_kwh": round(d["import_kwh"], 2),
            "autarky_pct": round(max(0.0, min(100.0, aut)), 1),
            "self_consumption_pct": round(((d["direct_kwh"] + d["charge_kwh"]) / d["pv_kwh"] * 100.0) if d["pv_kwh"] > 0 else 0.0, 1),
            "cost_eur": round(d["import_kwh"] * price, 2),
            "revenue_eur": round(d["export_kwh"] * feed, 2),
            "savings_eur": round((d["direct_kwh"] + d["discharge_kwh"]) * price, 2),
            "cloud_pct": round(d["cloud_sum"] / d["hours"], 0) if d["hours"] else None,
            "temp_max_c": round(d["temp_max"], 1) if d["temp_max"] > -90 else None,
            "partial": day == today0.strftime("%Y-%m-%d"),
        })

    # Reference: the roof's recent daily average, so "tomorrow 12 kWh" can be
    # read against "lately 18 kWh a day".
    recent_days: Dict[str, float] = {}
    for ts_, kwh in pv_hist.items():
        recent_days[datetime.fromtimestamp(ts_, _TZ).strftime("%Y-%m-%d")] = recent_days.get(
            datetime.fromtimestamp(ts_, _TZ).strftime("%Y-%m-%d"), 0.0) + max(0.0, kwh)
    full_days = [v for k, v in recent_days.items() if k != today0.strftime("%Y-%m-%d")]
    recent_avg = (sum(full_days) / len(full_days)) if full_days else 0.0

    return {
        "available": True,
        "site": {"lat": round(lat, 3), "lon": round(lon, 3)},
        "calibration": {"days": cal_days, "kwh_per_kwhm2": round(ratio * 1000.0, 3),
                        "recent_avg_kwh_per_day": round(recent_avg, 2)},
        "battery": {"capacity_kwh": round(cap, 1), "usable_kwh": round(usable, 1),
                    "efficiency_pct": round(eta * 100.0, 0), "start_soc_kwh": round(days_out and hours_out[0]["soc_kwh"] or 0.0, 2)},
        "price_eur_per_kwh": round(price, 4), "feed_in_eur_per_kwh": round(feed, 4),
        "days": day_list,
        "hours": hours_out,
        "generated_ts": int(now.timestamp()),
    }


def compute_solar_outlook(db, cfg, now: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
    """The 7-day expectation plus where this month and the next 30 days land.

    Beyond the weather horizon the roof's recent daily average carries on,
    bent by the seasonal share of the calendar month (mid-European PV
    climatology), so a September outlook does not promise August.
    """
    from shelly_analyzer.services.energy_balance import build_supply_chain
    from shelly_analyzer.services.solar_overview import _MONTH_SHARE
    import calendar as _cal
    now = now or datetime.now(_TZ)
    fc = compute_solar_forecast(db, cfg, days=7, now=now)
    if fc is None:
        return None
    t0 = now.replace(hour=0, minute=0, second=0, microsecond=0)
    m0 = t0.replace(day=1)
    dim = _cal.monthrange(now.year, now.month)[1]
    # Month so far, from the chain (same numbers as the Solar tab).
    ch = build_supply_chain(db, cfg, int(m0.timestamp()), int(now.timestamp()))
    so_far = {"pv": 0.0, "import": 0.0, "export": 0.0, "load": 0.0, "discharge": 0.0}
    for hm in ch.hours.values():
        so_far["pv"] += hm.pv; so_far["import"] += hm.gi; so_far["export"] += hm.ge
        so_far["load"] += hm.load; so_far["discharge"] += hm.bdis
    # Recent 14-day daily averages as the carrier beyond the weather horizon.
    ch14 = build_supply_chain(db, cfg, int(t0.timestamp()) - 14 * 86400, int(t0.timestamp()) - 1)
    days14: Dict[str, Dict[str, float]] = {}
    for ts, hm in ch14.hours.items():
        k = datetime.fromtimestamp(ts, _TZ).strftime("%Y-%m-%d")
        d = days14.setdefault(k, {"pv": 0.0, "import": 0.0, "export": 0.0, "load": 0.0, "discharge": 0.0})
        d["pv"] += hm.pv; d["import"] += hm.gi; d["export"] += hm.ge; d["load"] += hm.load; d["discharge"] += hm.bdis
    n14 = max(1, len(days14))
    avg = {k: sum(v[k] for v in days14.values()) / n14 for k in ("pv", "import", "export", "load", "discharge")}
    try:
        price = float(cfg.pricing.unit_price_gross())
    except Exception:
        price = 0.30
    solar = getattr(cfg, "solar", None)
    try:
        feed = float(solar.effective_feed_in_for_date(now.date())) if solar else 0.082
    except Exception:
        feed = 0.082

    def _season(day: datetime) -> float:
        """PV factor for a day relative to today's month (interpolated)."""
        cur = _MONTH_SHARE[now.month]
        m = day.month
        return (_MONTH_SHARE[m] / cur) if cur > 0 else 1.0

    fc_days = {d["date"]: d for d in fc.get("days", [])} if fc.get("available") else {}
    horizon = []
    for i in range(0, 30):
        day = t0 + timedelta(days=i)
        key = day.strftime("%Y-%m-%d")
        if key in fc_days:
            d = fc_days[key]
            horizon.append({"date": key, "source": "weather", "pv_kwh": d["pv_kwh"], "import_kwh": d["import_kwh"],
                            "export_kwh": d["export_kwh"], "load_kwh": d["load_kwh"], "discharge_kwh": d["discharge_kwh"],
                            "partial": d.get("partial", False)})
        else:
            f = _season(day)
            pvd = avg["pv"] * f
            # Import/export follow the PV level: less sun → more grid, less feed-in.
            imp = max(0.0, avg["load"] - (avg["load"] - avg["import"]) * min(1.5, f))
            exp = max(0.0, avg["export"] * f)
            horizon.append({"date": key, "source": "climatology", "pv_kwh": round(pvd, 2), "import_kwh": round(imp, 2),
                            "export_kwh": round(exp, 2), "load_kwh": round(avg["load"], 2),
                            "discharge_kwh": round(min(avg["discharge"], pvd), 2), "partial": False})
    # Month end = so far (up to now) + remainder of today (weather day, partial) + the rest of the month.
    rest = [h for h in horizon if h["date"] > t0.strftime("%Y-%m-%d") and datetime.strptime(h["date"], "%Y-%m-%d").month == now.month]
    today_rest = next((h for h in horizon if h["date"] == t0.strftime("%Y-%m-%d")), None)
    month_end = {k: so_far[k] + (today_rest[k + "_kwh"] if today_rest else 0.0) + sum(h[k + "_kwh"] for h in rest)
                 for k in ("pv", "import", "export", "load")}
    d30 = [h for h in horizon if not h["partial"]]
    sum30 = {k: sum(h[k + "_kwh"] for h in d30) for k in ("pv", "import", "export", "load", "discharge")}
    return {
        "forecast": fc,
        "horizon": horizon,
        "recent_daily_avg": {k: round(v, 2) for k, v in avg.items()},
        "recent_days": n14,
        "month": {
            "label": now.strftime("%B %Y"), "days_in_month": dim, "day": now.day,
            "so_far": {k: round(v, 1) for k, v in so_far.items()},
            "expected": {k: round(v, 1) for k, v in month_end.items()},
            "expected_cost_eur": round(month_end["import"] * price, 2),
            "expected_revenue_eur": round(month_end["export"] * feed, 2),
            "expected_autarky_pct": round((1.0 - month_end["import"] / month_end["load"]) * 100.0, 1) if month_end["load"] > 0 else None,
        },
        "next_30_days": {
            "pv_kwh": round(sum30["pv"], 0), "import_kwh": round(sum30["import"], 1), "export_kwh": round(sum30["export"], 1),
            "load_kwh": round(sum30["load"], 0), "battery_kwh": round(sum30["discharge"], 0),
            "cost_eur": round(sum30["import"] * price, 2), "revenue_eur": round(sum30["export"] * feed, 2),
            "autarky_pct": round((1.0 - sum30["import"] / sum30["load"]) * 100.0, 1) if sum30["load"] > 0 else None,
            "battery_cycles": None,
        },
        "price_eur_per_kwh": round(price, 4), "feed_in_eur_per_kwh": round(feed, 4),
    }
