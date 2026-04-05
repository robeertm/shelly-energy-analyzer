"""Tenant (Mieter) list + Nebenkostenabrechnung endpoints."""
from __future__ import annotations

import logging
from dataclasses import asdict
from typing import Any, Dict

from flask import Blueprint, current_app, jsonify, request

logger = logging.getLogger(__name__)

bp = Blueprint("tenants", __name__)


def _get_state():
    return current_app.extensions["state"]


@bp.route("/api/tenants", methods=["GET"])
def list_tenants():
    """Return all tenant definitions and common-area device keys."""
    state = _get_state()
    tc = getattr(state.cfg, "tenant", None)
    if tc is None:
        return jsonify({"ok": True, "enabled": False, "tenants": [], "common_device_keys": [], "billing_period_months": 12})
    tenants = []
    for t in (tc.tenants or []):
        tenants.append({
            "tenant_id": t.tenant_id,
            "name": t.name,
            "device_keys": list(t.device_keys or []),
            "unit": t.unit,
            "persons": int(t.persons or 1),
            "move_in": t.move_in,
            "move_out": t.move_out,
        })
    return jsonify({
        "ok": True,
        "enabled": bool(tc.enabled),
        "tenants": tenants,
        "common_device_keys": list(tc.common_device_keys or []),
        "billing_period_months": int(tc.billing_period_months or 12),
    })


@bp.route("/api/tenants", methods=["PUT"])
def update_tenants():
    """Replace the tenant config (list + common keys + settings)."""
    state = _get_state()
    from dataclasses import replace
    from shelly_analyzer.io.config import TenantConfig, TenantDef, save_config

    try:
        body = request.get_json(silent=True) or {}
        tenants_raw = body.get("tenants", []) or []
        tenants = []
        for t in tenants_raw:
            if not isinstance(t, dict):
                continue
            tenants.append(TenantDef(
                tenant_id=str(t.get("tenant_id") or "").strip(),
                name=str(t.get("name") or "").strip(),
                device_keys=[str(k) for k in (t.get("device_keys") or [])],
                unit=str(t.get("unit") or "").strip(),
                persons=int(t.get("persons") or 1),
                move_in=str(t.get("move_in") or "").strip(),
                move_out=str(t.get("move_out") or "").strip(),
            ))
        new_tc = TenantConfig(
            enabled=bool(body.get("enabled", True)),
            tenants=tenants,
            common_device_keys=[str(k) for k in (body.get("common_device_keys") or [])],
            billing_period_months=int(body.get("billing_period_months") or 12),
        )
        new_cfg = replace(state.cfg, tenant=new_tc)
        cfg_path = getattr(state, "_cfg_path", None)
        save_config(new_cfg, cfg_path)
        state.cfg = new_cfg
        state.reload_config(new_cfg)
        return jsonify({"ok": True, "tenant_count": len(tenants)})
    except Exception as e:
        logger.exception("Failed to update tenants")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/tenants/bill", methods=["GET"])
def compute_bills():
    """Compute tenant bills for a period.

    Query params: period_start=YYYY-MM-DD, period_end=YYYY-MM-DD (both optional).
    """
    state = _get_state()
    try:
        from shelly_analyzer.services.tenant import generate_tenant_bills, TenantDef as SvcTenantDef

        tc = getattr(state.cfg, "tenant", None)
        if tc is None or not tc.tenants:
            return jsonify({"ok": False, "error": "Keine Mieter konfiguriert"}), 400

        # Map config TenantDef → service TenantDef (same fields)
        svc_tenants = [
            SvcTenantDef(
                tenant_id=t.tenant_id,
                name=t.name,
                device_keys=list(t.device_keys or []),
                unit=t.unit,
                persons=int(t.persons or 1),
                move_in=t.move_in,
                move_out=t.move_out,
            )
            for t in tc.tenants
        ]

        # Pricing — always pass NET values to the service; VAT is applied once
        # at subtotal_net → gross inside the service. Previous versions passed
        # GROSS and then applied VAT again, double-counting 19% on the bill.
        try:
            vat_rate = float(state.cfg.pricing.vat_rate())
        except Exception:
            vat_rate = float(getattr(state.cfg.pricing, "vat_rate_percent", 19.0) or 19.0) / 100.0
        try:
            unit_price_net = float(state.cfg.pricing.unit_price_net())
        except Exception:
            unit_price_net = float(getattr(state.cfg.pricing, "electricity_price_eur_per_kwh", 0.0) or 0.0)
        try:
            base_fee_net = float(state.cfg.pricing.base_fee_year_net())
        except Exception:
            base_fee_net = float(getattr(state.cfg.pricing, "base_fee_eur_per_year", 0.0) or 0.0)

        # Tariff mode: "fixed" (default, uses PricingConfig) or "dynamic" (uses spot_prices DB)
        tariff_mode = request.args.get("tariff_mode") or ""
        if not tariff_mode:
            try:
                tariff_mode = str(getattr(state.cfg.spot_price, "tariff_type", "fixed") or "fixed")
            except Exception:
                tariff_mode = "fixed"
        tariff_mode = tariff_mode.lower().strip()

        # For dynamic tariff: compute volume-weighted avg spot price (ct/kWh → €/kWh net)
        price_eur_per_kwh = unit_price_net
        period_start = request.args.get("period_start") or None
        period_end = request.args.get("period_end") or None
        if tariff_mode == "dynamic":
            try:
                import datetime as _dt
                from shelly_analyzer.io.config import SpotPriceConfig
                spot_cfg = getattr(state.cfg, "spot_price", None)
                if spot_cfg is not None:
                    zone = str(getattr(spot_cfg, "bidding_zone", "DE-LU") or "DE-LU")
                    # Determine period bounds
                    _now = _dt.datetime.now()
                    _end = _dt.datetime.strptime(period_end, "%Y-%m-%d") if period_end else _now
                    _start = _dt.datetime.strptime(period_start, "%Y-%m-%d") if period_start else (_end - _dt.timedelta(days=365))
                    df_sp = state.storage.db.query_spot_prices(zone, int(_start.timestamp()), int(_end.timestamp()))
                    if df_sp is not None and not df_sp.empty:
                        avg_eur_mwh = float(df_sp["price_eur_mwh"].mean())
                        # Convert to €/kWh NET, then add surcharges (all net)
                        base_ct = avg_eur_mwh * 0.1  # ct/kWh gross-spot
                        surcharge_ct = float(spot_cfg.total_markup_ct())
                        final_ct_net = base_ct + surcharge_ct
                        price_eur_per_kwh = final_ct_net / 100.0  # €/kWh NET
            except Exception:
                logger.exception("Dynamic tariff lookup failed; falling back to fixed")
                price_eur_per_kwh = unit_price_net
                tariff_mode = "fixed"

        report = generate_tenant_bills(
            db=state.storage.db,
            tenants=svc_tenants,
            devices=list(state.cfg.devices),
            price_eur_per_kwh=price_eur_per_kwh,
            base_fee_eur_per_year=base_fee_net,
            vat_rate=vat_rate,
            period_start=period_start,
            period_end=period_end,
            common_device_keys=list(tc.common_device_keys or []),
        )

        bills = []
        for b in report.bills:
            bills.append({
                "tenant": {
                    "tenant_id": b.tenant.tenant_id,
                    "name": b.tenant.name,
                    "unit": b.tenant.unit,
                    "persons": b.tenant.persons,
                },
                "period_start": b.period_start,
                "period_end": b.period_end,
                "line_items": [
                    {
                        "description": li.description,
                        "kwh": round(li.kwh, 3),
                        "unit_price": round(li.unit_price, 4),
                        "amount": round(li.amount, 2),
                        "device_key": li.device_key,
                        "device_name": li.device_name,
                    }
                    for li in b.line_items
                ],
                "subtotal_net": round(b.subtotal_net, 2),
                "vat_amount": round(b.vat_amount, 2),
                "total_gross": round(b.total_gross, 2),
                "total_kwh": round(b.total_kwh, 3),
                "base_fee_share": round(b.base_fee_share, 2),
                "common_area_kwh": round(b.common_area_kwh, 3),
                "common_area_cost": round(b.common_area_cost, 2),
            })

        return jsonify({
            "ok": True,
            "report": {
                "period_start": report.period_start,
                "period_end": report.period_end,
                "total_kwh": round(report.total_kwh, 3),
                "total_cost": round(report.total_cost, 2),
                "common_area_kwh": round(report.common_area_kwh, 3),
                "generated_at": report.generated_at,
                "bills": bills,
                "tariff_mode": tariff_mode,
                "price_eur_per_kwh_net": round(price_eur_per_kwh, 4),
                "base_fee_eur_per_year_net": round(base_fee_net, 2),
                "vat_rate_percent": round(vat_rate * 100, 1),
            },
        })
    except Exception as e:
        logger.exception("Tenant billing failed")
        return jsonify({"ok": False, "error": str(e)}), 500
