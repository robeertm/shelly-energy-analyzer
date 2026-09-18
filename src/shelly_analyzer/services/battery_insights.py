"""What the battery tab shows beyond "SOC and power right now".

Everything here is derived from two things the analyzer already has: the
per-hour supply chain (the same one every CO₂ figure runs on — so "charged
from PV" here equals "charged from PV" on the CO₂ tab, always) and the
minute-resolution SOC/power timeline of the battery meter.  No new
measurement is invented; the tab only reads the existing ones from more
angles: per day, per hour of the day, per cycle, in euros.
"""
from __future__ import annotations

import logging
import statistics
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

_log = logging.getLogger(__name__)
_TZ = ZoneInfo("Europe/Berlin")

FULL_PCT = 97.0   # at or above: the battery is "full" (PV surplus has nowhere to go)
EMPTY_PCT = 5.0   # at or below: "empty" (the evening runs on the grid)


def _prices(cfg, now: datetime) -> Tuple[float, float]:
    try:
        price = float(cfg.pricing.unit_price_gross())
    except Exception:
        price = 0.30
    solar = getattr(cfg, "solar", None)
    try:
        feed = float(solar.effective_feed_in_for_date(now.date()))
    except Exception:
        feed = float(getattr(solar, "feed_in_tariff_eur_per_kwh", 0.082) or 0.082) if solar else 0.082
    return price, feed


def _time_shares(timeline: Sequence[Tuple[int, float]]) -> Dict[str, Any]:
    """Share of the window the battery spent full / empty, and — per day — when
    it first became full and when it last ran empty (median over the days)."""
    if len(timeline) < 2:
        return {"full_share_pct": None, "empty_share_pct": None, "typical_full_at": None,
                "typical_empty_at": None, "days_full": 0, "days_empty": 0}
    full_s = empty_s = tot_s = 0.0
    first_full: Dict[str, int] = {}
    last_empty: Dict[str, int] = {}
    prev_ts, prev_soc = timeline[0]
    for ts, soc in timeline[1:]:
        dt = ts - prev_ts
        if 0 < dt < 7200:
            tot_s += dt
            if prev_soc >= FULL_PCT:
                full_s += dt
            if prev_soc <= EMPTY_PCT:
                empty_s += dt
        if soc >= FULL_PCT or soc <= EMPTY_PCT:
            d = datetime.fromtimestamp(ts, _TZ)
            key = d.strftime("%Y-%m-%d")
            if soc >= FULL_PCT and key not in first_full:
                first_full[key] = d.hour * 60 + d.minute
            if soc <= EMPTY_PCT:
                last_empty[key] = d.hour * 60 + d.minute
        prev_ts, prev_soc = ts, soc

    def _hhmm(minutes: Optional[float]) -> Optional[str]:
        if minutes is None:
            return None
        m = int(round(minutes))
        return "%02d:%02d" % (m // 60 % 24, m % 60)

    return {
        "full_share_pct": round(full_s / tot_s * 100.0, 1) if tot_s > 0 else None,
        "empty_share_pct": round(empty_s / tot_s * 100.0, 1) if tot_s > 0 else None,
        "typical_full_at": _hhmm(statistics.median(first_full.values())) if len(first_full) >= 2 else None,
        "typical_empty_at": _hhmm(statistics.median(last_empty.values())) if len(last_empty) >= 2 else None,
        "days_full": len(first_full), "days_empty": len(last_empty),
    }


def _soc_day_stats(timeline: Sequence[Tuple[int, float]]) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}
    for ts, soc in timeline:
        k = datetime.fromtimestamp(ts, _TZ).strftime("%Y-%m-%d")
        d = out.setdefault(k, {"min": soc, "max": soc})
        if soc < d["min"]:
            d["min"] = soc
        if soc > d["max"]:
            d["max"] = soc
    return out


def _soc_by_hour(timeline: Sequence[Tuple[int, float]]) -> List[Optional[float]]:
    sums = [0.0] * 24
    cnt = [0] * 24
    for ts, soc in timeline:
        h = datetime.fromtimestamp(ts, _TZ).hour
        sums[h] += soc
        cnt[h] += 1
    return [round(sums[h] / cnt[h], 1) if cnt[h] else None for h in range(24)]


def compute_battery_insights(db, cfg, status, now: Optional[datetime] = None,
                             long_days: int = 30, short_days: int = 7) -> Dict[str, Any]:
    """Extra blocks for /api/battery. `status` is the BatteryStatus of the
    short window (its timeline and cycles are reused, never recomputed)."""
    from shelly_analyzer.services.energy_balance import build_supply_chain
    from shelly_analyzer.services.solar_overview import _imap, _seasonal_scale

    now = now or datetime.now(_TZ)
    now_ts = int(now.timestamp())
    today0 = now.replace(hour=0, minute=0, second=0, microsecond=0)
    long_start = int((today0 - timedelta(days=long_days - 1)).timestamp())
    short_start = now_ts - short_days * 86400
    price, feed = _prices(cfg, now)
    cap = float(getattr(status, "capacity_kwh", 0.0) or 0.0)

    out: Dict[str, Any] = {
        "long_days": long_days, "unit_price": round(price, 4), "feed_in_tariff": round(feed, 4),
        "daily": [], "hour_profile": [], "cycles": [], "stats": {}, "money": None, "has_chain": False,
    }

    # ── The chain: 30 days, hourly ────────────────────────────────────────
    chain = None
    try:
        imap, fb = _imap(db, cfg, long_start, now_ts)
        chain = build_supply_chain(db, cfg, long_start, now_ts + 3600, imap, fb)
    except Exception:
        _log.debug("battery insights: chain failed", exc_info=True)
    hours = chain.hours if (chain is not None and chain.has_supply) else {}
    out["has_chain"] = bool(hours)

    timeline: List[Tuple[int, float]] = list(getattr(status, "soc_timeline", []) or [])
    day_soc = _soc_day_stats(timeline)

    # per local day
    days: Dict[str, Dict[str, float]] = {}
    for i in range(long_days):
        d = today0 - timedelta(days=long_days - 1 - i)
        days[d.strftime("%Y-%m-%d")] = {"charge_pv": 0.0, "charge_grid": 0.0, "discharge": 0.0,
                                        "avoided_g": 0.0, "saved_eur": 0.0, "co2_g": 0.0}
    for ts, h in hours.items():
        k = datetime.fromtimestamp(ts, _TZ).strftime("%Y-%m-%d")
        d = days.get(k)
        if d is None:
            continue
        d["charge_pv"] += h.bch_pv
        d["charge_grid"] += h.bch_grid
        d["discharge"] += h.bdis
        d["avoided_g"] += max(0.0, h.bdis * h.ci - h.g_bat)
        d["co2_g"] += h.g_bat
        # A discharged kWh replaced a bought one; a PV-charged kWh gave up its
        # feed-in payment; a grid-charged kWh was bought at the tariff.
        d["saved_eur"] += h.bdis * price - h.bch_pv * feed - h.bch_grid * price
    for k, d in days.items():
        s = day_soc.get(k)
        out["daily"].append({
            "date": k, "charge_pv_kwh": round(d["charge_pv"], 3), "charge_grid_kwh": round(d["charge_grid"], 3),
            "discharge_kwh": round(d["discharge"], 3), "saved_eur": round(d["saved_eur"], 3),
            "avoided_kg": round(d["avoided_g"] / 1000.0, 3), "co2_kg": round(d["co2_g"] / 1000.0, 3),
            "soc_min": s["min"] if s else None, "soc_max": s["max"] if s else None,
        })

    # ── Hour-of-day rhythm over the short window ─────────────────────────
    hs = [0.0] * 24
    hd = [0.0] * 24
    ndays = max(1, short_days)
    for ts, h in hours.items():
        if ts < short_start:
            continue
        hh = datetime.fromtimestamp(ts, _TZ).hour
        hs[hh] += h.bch
        hd[hh] += h.bdis
    soc_h = _soc_by_hour([p for p in timeline if p[0] >= short_start])
    out["hour_profile"] = [{"hour": i, "charge_kwh": round(hs[i] / ndays, 3), "discharge_kwh": round(hd[i] / ndays, 3),
                            "soc_pct": soc_h[i]} for i in range(24)]

    # ── Cycles (already detected for the short window) ───────────────────
    cyc = list(getattr(status, "cycles", []) or [])
    msince = int(getattr(status, "measured_since_ts", 0) or 0)
    out["cycles"] = [{"start_ts": c.start_ts, "end_ts": c.end_ts, "charge_kwh": c.charge_kwh,
                      "discharge_kwh": c.discharge_kwh, "efficiency_pct": c.efficiency_pct,
                      "depth_pct": c.depth_pct,
                      # η only means something on a measured curve (see battery.py)
                      "measured": bool(msince and c.start_ts >= msince)} for c in cyc[-12:]]

    # ── Stats ─────────────────────────────────────────────────────────────
    st = _time_shares(timeline)
    eq = float(getattr(status, "equivalent_cycles", 0.0) or 0.0)
    dis7 = float(getattr(status, "total_discharged_kwh", 0.0) or 0.0)
    st.update({
        "cycles_per_year_est": round(eq / short_days * 365.0, 0) if eq > 0 else None,
        "avg_dod_pct": round(sum(c.depth_pct for c in cyc) / len(cyc), 1) if cyc else None,
        "throughput_kwh_per_day": round(dis7 / short_days, 2),
        "capacity_turnover_pct": round(dis7 / short_days / cap * 100.0, 0) if cap > 0 else None,
        "short_days": short_days,
    })
    out["stats"] = st

    # ── Money over the long window ────────────────────────────────────────
    if hours:
        bdis = sum(h.bdis for h in hours.values())
        bpv = sum(h.bch_pv for h in hours.values())
        bgr = sum(h.bch_grid for h in hours.values())
        val = bdis * price
        forgone = bpv * feed
        bought = bgr * price
        net = val - forgone - bought
        day_set = {datetime.fromtimestamp(ts, _TZ).date() for ts in hours}
        try:
            scale = float(_seasonal_scale(day_set))
        except Exception:
            scale = 365.0 / max(1, len(day_set))
        out["money"] = {
            "days": len(day_set), "discharged_kwh": round(bdis, 2), "value_eur": round(val, 2),
            "pv_charge_forgone_eur": round(forgone, 2), "grid_charge_cost_eur": round(bought, 2),
            "net_eur": round(net, 2), "per_day_eur": round(net / max(1, len(day_set)), 3),
            "year_est_eur": round(net * scale, 0),
            "per_kwh_eur": round(net / bdis, 3) if bdis > 0 else None,
        }
    return out
