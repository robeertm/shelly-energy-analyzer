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

        # Pricing
        unit_price = float(state.cfg.pricing.unit_price_gross())
        base_fee = float(getattr(state.cfg.pricing, "base_fee_gross", 0.0) or 0.0)
        if base_fee <= 0:
            # fall back to yearly fee from invoice config if present
            base_fee = float(getattr(getattr(state.cfg, "invoice", None), "base_fee_eur_per_year", 0.0) or 0.0)
        vat_rate = float(getattr(state.cfg.pricing, "vat_rate", 0.19) or 0.19)

        period_start = request.args.get("period_start") or None
        period_end = request.args.get("period_end") or None

        report = generate_tenant_bills(
            db=state.storage.db,
            tenants=svc_tenants,
            devices=list(state.cfg.devices),
            price_eur_per_kwh=unit_price,
            base_fee_eur_per_year=base_fee,
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
            },
        })
    except Exception as e:
        logger.exception("Tenant billing failed")
        return jsonify({"ok": False, "error": str(e)}), 500
