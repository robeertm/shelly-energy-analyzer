"""Tenant utility billing (Nebenkostenabrechnung).

Generates per-tenant annual utility bills based on device group assignments,
configurable billing periods, and proportional cost allocation.
"""
from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class TenantDef:
    """Definition of a single tenant."""
    tenant_id: str
    name: str
    device_keys: List[str] = field(default_factory=list)
    # Optional: flat/unit identifier
    unit: str = ""
    # Number of persons (for per-capita allocation of common areas)
    persons: int = 1
    # Move-in / move-out dates (ISO format, empty = full period)
    move_in: str = ""
    move_out: str = ""


@dataclass
class TenantLineItem:
    """A single line item on a tenant bill."""
    description: str
    kwh: float
    unit_price: float
    amount: float
    device_key: str = ""
    device_name: str = ""


@dataclass
class TenantBill:
    """A complete tenant utility bill."""
    tenant: TenantDef
    period_start: str  # ISO date
    period_end: str
    line_items: List[TenantLineItem] = field(default_factory=list)
    subtotal_net: float = 0.0
    vat_amount: float = 0.0
    total_gross: float = 0.0
    total_kwh: float = 0.0
    base_fee_share: float = 0.0
    # Common area allocation
    common_area_kwh: float = 0.0
    common_area_cost: float = 0.0
    # Comparison
    avg_cost_per_person: float = 0.0
    cost_per_sqm: float = 0.0


@dataclass
class TenantReport:
    """Complete tenant billing report for a period."""
    bills: List[TenantBill] = field(default_factory=list)
    period_start: str = ""
    period_end: str = ""
    total_kwh: float = 0.0
    total_cost: float = 0.0
    common_area_kwh: float = 0.0
    generated_at: str = ""


def generate_tenant_bills(
    db,
    tenants: List[TenantDef],
    devices: list,
    price_eur_per_kwh: float = 0.3265,
    base_fee_eur_per_year: float = 127.51,
    vat_rate: float = 0.19,
    period_start: Optional[str] = None,
    period_end: Optional[str] = None,
    common_device_keys: Optional[List[str]] = None,
) -> TenantReport:
    """Generate utility bills for all tenants.

    Args:
        db: EnergyDB instance
        tenants: list of TenantDef
        devices: list of DeviceConfig (for name lookup)
        price_eur_per_kwh: electricity price
        base_fee_eur_per_year: annual base fee to split
        vat_rate: VAT rate (0.19 = 19%)
        period_start/end: ISO dates (YYYY-MM-DD), defaults to last 12 months
        common_device_keys: devices whose cost is split among all tenants by person count
    """
    now = datetime.datetime.now()

    if period_end:
        end_dt = datetime.datetime.strptime(period_end, "%Y-%m-%d")
    else:
        end_dt = now
        period_end = now.strftime("%Y-%m-%d")

    if period_start:
        start_dt = datetime.datetime.strptime(period_start, "%Y-%m-%d")
    else:
        start_dt = end_dt - datetime.timedelta(days=365)
        period_start = start_dt.strftime("%Y-%m-%d")

    start_ts = int(start_dt.timestamp())
    end_ts = int(end_dt.timestamp())
    period_days = max(1, (end_dt - start_dt).days)

    # Device name lookup
    dev_names = {d.key: d.name for d in devices}
    common_keys = set(common_device_keys or [])

    # All tenant device keys (to find unassigned/common devices)
    all_tenant_keys = set()
    for t in tenants:
        all_tenant_keys.update(t.device_keys)

    total_persons = sum(t.persons for t in tenants) or 1

    # Pre-fetch common area energy
    common_area_kwh = 0.0
    common_area_by_device: Dict[str, float] = {}
    for ck in common_keys:
        hourly = db.query_hourly(ck, start_ts=start_ts, end_ts=end_ts)
        if not hourly.empty:
            kwh = float(hourly["kwh"].sum())
            common_area_kwh += kwh
            common_area_by_device[ck] = kwh

    # Base fee share per tenant (proportional by days in period)
    base_fee_daily = base_fee_eur_per_year / 365.0
    total_base_fee = base_fee_daily * period_days

    bills: List[TenantBill] = []

    for tenant in tenants:
        # Determine tenant's active days
        t_start = start_dt
        t_end = end_dt
        if tenant.move_in:
            try:
                mi = datetime.datetime.strptime(tenant.move_in, "%Y-%m-%d")
                if mi > t_start:
                    t_start = mi
            except ValueError:
                pass
        if tenant.move_out:
            try:
                mo = datetime.datetime.strptime(tenant.move_out, "%Y-%m-%d")
                if mo < t_end:
                    t_end = mo
            except ValueError:
                pass

        tenant_days = max(1, (t_end - t_start).days)
        t_start_ts = int(t_start.timestamp())
        t_end_ts = int(t_end.timestamp())

        line_items: List[TenantLineItem] = []
        total_kwh = 0.0

        # Per-device energy
        for dk in tenant.device_keys:
            hourly = db.query_hourly(dk, start_ts=t_start_ts, end_ts=t_end_ts)
            if hourly.empty:
                continue
            kwh = float(hourly["kwh"].sum())
            if kwh <= 0:
                continue
            total_kwh += kwh
            amount = kwh * price_eur_per_kwh
            line_items.append(TenantLineItem(
                description=f"Electricity consumption – {dev_names.get(dk, dk)}",
                kwh=round(kwh, 2),
                unit_price=price_eur_per_kwh,
                amount=round(amount, 2),
                device_key=dk,
                device_name=dev_names.get(dk, dk),
            ))

        # Common area share (by person count)
        person_share = tenant.persons / total_persons
        tenant_common_kwh = common_area_kwh * person_share
        tenant_common_cost = tenant_common_kwh * price_eur_per_kwh
        if tenant_common_kwh > 0.01:
            total_kwh += tenant_common_kwh
            line_items.append(TenantLineItem(
                description=f"Allgemeinstrom (Anteil {tenant.persons}/{total_persons} Pers.)",
                kwh=round(tenant_common_kwh, 2),
                unit_price=price_eur_per_kwh,
                amount=round(tenant_common_cost, 2),
            ))

        # Base fee share (proportional by days)
        base_share = total_base_fee * (tenant_days / period_days) / max(len(tenants), 1)
        if base_share > 0.01:
            line_items.append(TenantLineItem(
                description=f"Grundpreis (anteilig {tenant_days} Tage)",
                kwh=0,
                unit_price=0,
                amount=round(base_share, 2),
            ))

        subtotal = sum(li.amount for li in line_items)
        vat = subtotal * vat_rate
        total_gross = subtotal + vat

        bills.append(TenantBill(
            tenant=tenant,
            period_start=period_start,
            period_end=period_end,
            line_items=line_items,
            subtotal_net=round(subtotal, 2),
            vat_amount=round(vat, 2),
            total_gross=round(total_gross, 2),
            total_kwh=round(total_kwh, 2),
            base_fee_share=round(base_share, 2),
            common_area_kwh=round(tenant_common_kwh, 2),
            common_area_cost=round(tenant_common_cost, 2),
            avg_cost_per_person=round(total_gross / max(tenant.persons, 1), 2),
        ))

    total_kwh = sum(b.total_kwh for b in bills)
    total_cost = sum(b.total_gross for b in bills)

    return TenantReport(
        bills=bills,
        period_start=period_start,
        period_end=period_end,
        total_kwh=round(total_kwh, 2),
        total_cost=round(total_cost, 2),
        common_area_kwh=round(common_area_kwh, 2),
        generated_at=now.strftime("%Y-%m-%d %H:%M"),
    )
