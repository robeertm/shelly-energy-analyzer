from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

_log = logging.getLogger(__name__)


@dataclass
class TariffResult:
    name: str
    provider: str
    tariff_type: str
    annual_cost_eur: float
    monthly_avg_eur: float
    effective_price_ct: float  # ct/kWh all-in
    savings_vs_current_eur: float = 0.0
    is_current: bool = False


# Pre-defined German provider tariff templates (approximate, 2025 values)
GERMAN_TARIFF_TEMPLATES = [
    {"name": "Stadtwerke Standard", "provider": "Stadtwerke", "tariff_type": "fixed",
     "price_eur_per_kwh": 0.3265, "base_fee_eur_per_year": 127.51},
    {"name": "Tibber Pulse", "provider": "Tibber", "tariff_type": "spot",
     "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 71.40, "spot_markup_ct": 15.3},
    {"name": "1Komma5° Dynamic", "provider": "1Komma5°", "tariff_type": "spot",
     "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 0.0, "spot_markup_ct": 14.5},
    {"name": "Ostrom Flex", "provider": "Ostrom", "tariff_type": "spot",
     "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 107.88, "spot_markup_ct": 16.0},
    {"name": "EON Strom Basis", "provider": "E.ON", "tariff_type": "fixed",
     "price_eur_per_kwh": 0.3399, "base_fee_eur_per_year": 167.88},
    {"name": "Vattenfall Easy", "provider": "Vattenfall", "tariff_type": "fixed",
     "price_eur_per_kwh": 0.3199, "base_fee_eur_per_year": 143.88},
    {"name": "EnBW Comfort", "provider": "EnBW", "tariff_type": "fixed",
     "price_eur_per_kwh": 0.3485, "base_fee_eur_per_year": 155.88},
    {"name": "Day/Night TOU", "provider": "Municipal", "tariff_type": "tou",
     "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 140.0,
     "ht_price": 0.35, "nt_price": 0.22, "ht_start": 6, "ht_end": 22},
]


def simulate_fixed_tariff(
    hourly_kwh: List[float],
    price_eur_per_kwh: float,
    base_fee_eur_per_year: float,
) -> float:
    """Simulate annual cost under a fixed tariff."""
    total_kwh = sum(hourly_kwh)
    hours_in_data = len(hourly_kwh)
    if hours_in_data <= 0:
        return 0.0
    annual_kwh = total_kwh * (8760 / hours_in_data)
    return annual_kwh * price_eur_per_kwh + base_fee_eur_per_year


def simulate_tou_tariff(
    hourly_kwh: List[float],
    hourly_hours: List[int],
    ht_price: float, nt_price: float,
    ht_start: int, ht_end: int,
    base_fee_eur_per_year: float,
) -> float:
    """Simulate annual cost under a time-of-use tariff."""
    total_cost = 0.0
    for kwh, hour in zip(hourly_kwh, hourly_hours):
        if ht_start <= hour < ht_end:
            total_cost += kwh * ht_price
        else:
            total_cost += kwh * nt_price
    hours_in_data = len(hourly_kwh)
    if hours_in_data <= 0:
        return 0.0
    annual_factor = 8760 / hours_in_data
    return total_cost * annual_factor + base_fee_eur_per_year


def simulate_spot_tariff(
    hourly_kwh: List[float],
    hourly_spot_ct: List[float],
    markup_ct: float,
    base_fee_eur_per_year: float,
    vat_rate: float = 0.19,
) -> float:
    """Simulate annual cost under a dynamic spot tariff."""
    total_cost = 0.0
    for kwh, spot_ct in zip(hourly_kwh, hourly_spot_ct):
        price_ct = (spot_ct + markup_ct) * (1.0 + vat_rate)
        total_cost += kwh * price_ct / 100.0
    hours_in_data = len(hourly_kwh)
    if hours_in_data <= 0:
        return 0.0
    annual_factor = 8760 / hours_in_data
    return total_cost * annual_factor + base_fee_eur_per_year


def _get_consumption_stats(db, cfg) -> Optional[Dict]:
    """Return consumption summary stats used for tariff comparison."""
    import time
    now = int(time.time())
    start_ts = now - 90 * 86400
    try:
        total_kwh = 0.0
        hours = 0
        devices = cfg.devices if hasattr(cfg, 'devices') else []
        for dev in devices:
            if getattr(dev, 'kind', 'em') != 'em':
                continue
            try:
                df = db.query_hourly(dev.key, start_ts, now)
                if df is not None and not df.empty:
                    total_kwh += float(df["kwh"].sum())
                    hours += len(df)
            except Exception:
                pass
        if hours <= 0:
            return None
        days = hours / 24
        annual_kwh = total_kwh * (8760 / hours)
        return {"total_kwh": total_kwh, "hours": hours, "days": days, "annual_kwh": annual_kwh}
    except Exception:
        return None


def compare_tariffs(
    db, cfg,
    current_price_eur_per_kwh: float = 0.3265,
    current_base_fee_eur_per_year: float = 127.51,
    vat_rate: float = 0.19,
) -> List[TariffResult]:
    """Compare current tariff against all templates using actual consumption data."""
    import time
    now = int(time.time())
    # Use last 90 days of hourly data
    start_ts = now - 90 * 86400

    results: List[TariffResult] = []

    try:
        # Collect hourly consumption across all devices
        hourly_kwh: List[float] = []
        hourly_hours: List[int] = []
        hourly_spot_ct: List[float] = []

        devices = cfg.devices if hasattr(cfg, 'devices') else []
        for dev in devices:
            if getattr(dev, 'kind', 'em') != 'em':
                continue
            try:
                df = db.query_hourly(dev.key, start_ts, now)
                if df is not None and not df.empty:
                    for _, row in df.iterrows():
                        h_ts = int(row.get("hour_ts", 0))
                        kwh = float(row.get("kwh", 0) or 0)
                        hourly_kwh.append(kwh)
                        hourly_hours.append((h_ts % 86400) // 3600)
            except Exception:
                pass

        if not hourly_kwh:
            return []

        # Get spot prices
        try:
            zone = getattr(cfg.spot_price, 'bidding_zone', 'DE-LU')
            spot_df = db.query_spot_prices(zone, start_ts, now)
            if spot_df is not None and not spot_df.empty:
                spot_map: Dict[int, float] = {}
                for _, r in spot_df.iterrows():
                    spot_map[int(r["slot_ts"])] = float(r["price_eur_mwh"]) / 10.0  # to ct/kWh
                # Fill spot prices per hour
                hourly_spot_ct = [spot_map.get(h, 5.0) for h in range(len(hourly_kwh))]
            else:
                hourly_spot_ct = [5.0] * len(hourly_kwh)  # Default 5 ct/kWh wholesale
        except Exception:
            hourly_spot_ct = [5.0] * len(hourly_kwh)

        total_kwh = sum(hourly_kwh)
        hours = len(hourly_kwh)
        annual_kwh = total_kwh * (8760 / hours) if hours > 0 else 0

        # Current tariff
        current_annual = annual_kwh * current_price_eur_per_kwh + current_base_fee_eur_per_year

        # Add current tariff
        results.append(TariffResult(
            name="Current tariff",
            provider="Current",
            tariff_type="fixed",
            annual_cost_eur=round(current_annual, 2),
            monthly_avg_eur=round(current_annual / 12, 2),
            effective_price_ct=round(current_price_eur_per_kwh * 100, 2),
            savings_vs_current_eur=0.0,
            is_current=True,
        ))

        # Simulate each template
        for tmpl in GERMAN_TARIFF_TEMPLATES:
            tt = tmpl.get("tariff_type", "fixed")
            if tt == "fixed":
                annual = simulate_fixed_tariff(
                    hourly_kwh,
                    tmpl["price_eur_per_kwh"],
                    tmpl["base_fee_eur_per_year"],
                )
                eff_ct = tmpl["price_eur_per_kwh"] * 100
            elif tt == "tou":
                annual = simulate_tou_tariff(
                    hourly_kwh, hourly_hours,
                    tmpl.get("ht_price", 0.35), tmpl.get("nt_price", 0.22),
                    tmpl.get("ht_start", 6), tmpl.get("ht_end", 22),
                    tmpl["base_fee_eur_per_year"],
                )
                eff_ct = round((annual - tmpl["base_fee_eur_per_year"]) / max(annual_kwh, 1) * 100, 2) if annual_kwh > 0 else 0
            elif tt == "spot":
                annual = simulate_spot_tariff(
                    hourly_kwh, hourly_spot_ct,
                    tmpl.get("spot_markup_ct", 15.0),
                    tmpl["base_fee_eur_per_year"],
                    vat_rate,
                )
                eff_ct = round((annual - tmpl["base_fee_eur_per_year"]) / max(annual_kwh, 1) * 100, 2) if annual_kwh > 0 else 0
            else:
                continue

            results.append(TariffResult(
                name=tmpl["name"],
                provider=tmpl.get("provider", ""),
                tariff_type=tt,
                annual_cost_eur=round(annual, 2),
                monthly_avg_eur=round(annual / 12, 2),
                effective_price_ct=round(eff_ct, 2),
                savings_vs_current_eur=round(current_annual - annual, 2),
            ))

        # Sort by annual cost
        results.sort(key=lambda r: r.annual_cost_eur)

    except Exception as e:
        _log.error("Tariff comparison error: %s", e)

    return results
