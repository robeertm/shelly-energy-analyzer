"""API endpoints for live state, history, config, and jobs."""
from __future__ import annotations

import json
import math
import time as _time
from datetime import datetime
from typing import Any, Dict, List, Optional

from flask import Blueprint, Response, current_app, jsonify, request

from shelly_analyzer.i18n import t as _t

bp = Blueprint("api_state", __name__)

# Reserved synthetic devices from the external PV/battery source get a friendly
# default label (else they'd show the raw key on the Live view).
_SYNTH_KEYS = ("pv", "battery", "grid_ext")


def _device_name(meta: Dict[str, Any], dkey: str, lang: str) -> str:
    if not meta.get("name") and dkey in _SYNTH_KEYS:
        return _t(lang, "live.synth." + dkey)
    return str(meta.get("name") or dkey)


def _get_state():
    return current_app.extensions["state"]


def _safe_f(v: float) -> float:
    return v if math.isfinite(v) else 0.0


def _compute_i_n(measured: float, ia: float, ib: float, ic: float,
                 va: float = 0.0, vb: float = 0.0, vc: float = 0.0) -> float:
    """Return neutral current: measured if > 0.01 A, else estimated from phase
    currents assuming 120°/180° phase offsets.
    3-phase: |I_N| = sqrt(I1² + I2² + I3² − I1·I2 − I2·I3 − I1·I3)
    2-phase: |I_N| = |I1 − I2|
    """
    try:
        if measured and abs(measured) > 0.01:
            return float(measured)
        a_on = (va > 0) or (abs(ia) > 0.01)
        b_on = (vb > 0) or (abs(ib) > 0.01)
        c_on = (vc > 0) or (abs(ic) > 0.01)
        active = sum(1 for x in (a_on, b_on, c_on) if x)
        if active >= 3:
            val = ia * ia + ib * ib + ic * ic - ia * ib - ib * ic - ia * ic
            return round(math.sqrt(val) if val > 0 else 0.0, 3)
        if active == 2:
            if a_on and b_on:
                return round(abs(ia - ib), 3)
            if a_on and c_on:
                return round(abs(ia - ic), 3)
            if b_on and c_on:
                return round(abs(ib - ic), 3)
        return 0.0
    except Exception:
        return float(measured or 0.0)


def _today_cost_ctx(state) -> Dict[str, Any]:
    """Today's chain-cost context, cached ~30 s (the shares move slowly).

    The naive per-sample ``cost_today`` (kWh × tariff) ignores the generation
    chain. This provides the pieces to make the live tiles depend on each other:
    the owner's grid-served share of today's consumption, today's grid
    import/export, the unit tariff and the feed-in tariff, plus the tenant keys.
    """
    now = _time.time()
    cached = getattr(state, "_today_cost_ctx_cache", None)
    if cached and (now - cached[0]) < 30:
        return cached[1]
    ctx: Dict[str, Any] = {"owner_share": 1.0, "grid_import": 0.0,
                           "grid_export": 0.0, "unit": 0.30, "feed_in": 0.082,
                           "tenant_set": set()}
    try:
        cfg = getattr(state, "cfg", None)
        db = getattr(getattr(state, "storage", None), "db", None)
        if cfg is not None and db is not None:
            _now_dt = datetime.now()
            _today_start = int(_now_dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
            _now_ts = int(now)
            try:
                ctx["unit"] = float(cfg.pricing.effective_pricing_for_date(_now_dt.date()).unit_price_gross())
            except Exception:
                ctx["unit"] = float(getattr(getattr(cfg, "pricing", None), "electricity_price_eur_per_kwh", 0.30) or 0.30)
            _solar = getattr(cfg, "solar", None)
            ctx["feed_in"] = float(getattr(_solar, "feed_in_tariff_eur_per_kwh", 0.082) or 0.082) if _solar else 0.082
            from shelly_analyzer.services.energy_balance import (
                compute_grid_cost_share, compute_balance, _tenant_key_map)
            ctx["owner_share"] = float(compute_grid_cost_share(db, cfg, [(_today_start, _now_ts)])[0])
            _b = compute_balance(db, cfg, _today_start, _now_ts)
            ctx["grid_import"] = float(_b.grid_import_kwh)
            ctx["grid_export"] = float(_b.grid_export_kwh)
            _tk, _ = _tenant_key_map(cfg)
            ctx["tenant_set"] = set(_tk)
    except Exception:
        pass
    state._today_cost_ctx_cache = (now, ctx)
    return ctx


def _apply_today_chain_costs(state, devices_list) -> None:
    """Override each live tile's ``cost_today`` with the chain-aware value.

    Grid meter = import·tariff − export·feed-in (net position); owner circuits
    pay only their grid-served share (battery/PV = free); the tenant pays the
    full tariff; PV and battery cost nothing.
    """
    ctx = _today_cost_ctx(state)
    unit = ctx["unit"]
    feed_in = ctx["feed_in"]
    share = ctx["owner_share"]
    tenant_set = ctx["tenant_set"]
    for d in devices_list:
        role = d.get("flow_role")
        key = d.get("key")
        kwh = float(d.get("today_kwh") or 0.0)
        if role in ("pv", "battery"):
            d["cost_today"] = 0.0
        elif role == "grid":
            d["cost_today"] = round(ctx["grid_import"] * unit - ctx["grid_export"] * feed_in, 2)
        elif key in tenant_set:
            d["cost_today"] = round(max(0.0, kwh) * unit, 2)
        else:
            d["cost_today"] = round(max(0.0, kwh) * unit * share, 2)


@bp.route("/api/state")
def api_state():
    state = _get_state()
    raw_snap = state.live_store.snapshot()

    # Flow devices (coloured green/red by direction in the live view) + today's
    # battery charge/discharge. ``grid`` = the configured signed grid meter (± Netz)
    # or the synthetic ``grid_ext`` from an external source.
    try:
        _solar = getattr(getattr(state, "cfg", None), "solar", None)
        grid_key = str(getattr(_solar, "grid_meter_device_key", "") or "")
        grid_display_key = str(getattr(_solar, "grid_display_device_key", "") or "")
    except Exception:
        grid_key = ""
        grid_display_key = ""
    try:
        from shelly_analyzer.services.pv_source import (
            daily_readings, PV_KEY, BATTERY_KEY, GRID_KEY)
        _daily = daily_readings()
    except Exception:
        _daily, PV_KEY, BATTERY_KEY, GRID_KEY = {}, "pv", "battery", "grid_ext"

    # Which device the live view shows (and colours) as the grid tile. Defaults
    # to the cost meter, but can be a different device — e.g. the cost is derived
    # from the accurate external "grid_ext" (EMMA) while the familiar Shelly grid
    # meter stays the visible coloured "Netz" tile.
    grid_display = grid_display_key or grid_key
    # The external "grid_ext" series is a COST-ONLY source when it isn't the
    # display grid — hide its (redundant) tile from the live view.
    grid_ext_hidden = (GRID_KEY != grid_display)

    def _flow_role(k: str):
        if k == PV_KEY:
            return "pv"
        if k == BATTERY_KEY:
            return "battery"
        if grid_display and k == grid_display:
            return "grid"
        return None

    appliances_map: Dict[str, List[Any]] = raw_snap.get("_appliances", {})
    switch_states_map: Dict[str, Any] = raw_snap.get("_switch_states", {})
    dev_meta_by_key: Dict[str, Dict[str, Any]] = {
        d.get("key", ""): d
        for d in (state.devices_meta or [])
        if isinstance(d, dict) and d.get("key")
    }

    devices_list: List[Dict[str, Any]] = []
    for dkey, points in raw_snap.items():
        if dkey.startswith("_") or not isinstance(points, list) or not points:
            continue
        # Hide the cost-only external grid series so the live view doesn't show a
        # second, redundant "Netz" tile next to the real grid meter.
        if grid_ext_hidden and dkey == GRID_KEY:
            continue
        latest: Dict[str, Any] = points[-1]
        meta = dev_meta_by_key.get(dkey, {})
        name = _device_name(meta, dkey, getattr(state, "lang", "en"))

        va = float(latest.get("va") or 0)
        vb = float(latest.get("vb") or 0)
        vc = float(latest.get("vc") or 0)
        ia = float(latest.get("ia") or 0)
        ib = float(latest.get("ib") or 0)
        ic = float(latest.get("ic") or 0)
        pa = float(latest.get("pa") or 0)
        pb = float(latest.get("pb") or 0)
        pc = float(latest.get("pc") or 0)
        non_zero_v = [v for v in [va, vb, vc] if v > 0]
        voltage_v = sum(non_zero_v) / len(non_zero_v) if non_zero_v else 0.0
        current_a = ia + ib + ic if (ib > 0 or ic > 0) else ia

        phases: List[Dict[str, float]] = []
        if vb > 0 or vc > 0:
            if va > 0:
                phases.append({"voltage_v": va, "current_a": ia, "power_w": pa})
            if vb > 0:
                phases.append({"voltage_v": vb, "current_a": ib, "power_w": pb})
            if vc > 0:
                phases.append({"voltage_v": vc, "current_a": ic, "power_w": pc})

        raw_appl = appliances_map.get(dkey, [])
        appl_objs = [
            {"icon": a.get("icon", ""), "id": a.get("id", "")}
            for a in raw_appl if isinstance(a, dict)
        ]

        qa_val = float(latest.get("qa") or 0)
        qb_val = float(latest.get("qb") or 0)
        qc_val = float(latest.get("qc") or 0)
        q_phases: List[Dict[str, float]] = []
        if vb > 0 or vc > 0:
            if va > 0:
                q_phases.append({"var": qa_val})
            if vb > 0:
                q_phases.append({"var": qb_val})
            if vc > 0:
                q_phases.append({"var": qc_val})

        dev_kind = str(meta.get("kind") or "em")
        switch_on = switch_states_map.get(dkey) if dev_kind == "switch" else None

        devices_list.append({
            "key": dkey,
            "name": name,
            "kind": dev_kind,
            "power_w": float(latest.get("power_total_w") or 0),
            "today_kwh": float(latest.get("kwh_today") or 0),
            "cost_today": float(latest.get("cost_today") or 0),
            "soc_pct": float(latest.get("soc_pct") or 0),
            "voltage_v": voltage_v,
            "current_a": current_a,
            "pf": float(latest.get("cosphi_total") or 0),
            "freq_hz": float(latest.get("freq_hz") or 50),
            "phases": phases,
            "q_phases": q_phases,
            "appliances": appl_objs,
            "i_n": _compute_i_n(float(latest.get("i_n") or 0), ia, ib, ic, va, vb, vc),
            "q_total_var": float(latest.get("q_total_var") or 0),
            "switch_on": switch_on,
            "flow_role": _flow_role(dkey),
            "today_in_kwh": float((_daily.get(dkey) or {}).get("charge", 0.0)),
            "today_out_kwh": float((_daily.get(dkey) or {}).get("discharge", 0.0)),
        })

    # Include configured devices that have not produced a sample yet
    # (newly added devices show up immediately as placeholder cards instead
    # of disappearing until the first poll succeeds).
    seen_keys = {d["key"] for d in devices_list}
    for m in (state.devices_meta or []):
        if not isinstance(m, dict):
            continue
        k = m.get("key") or ""
        if not k or k in seen_keys:
            continue
        devices_list.append({
            "key": k,
            "name": _device_name(m, k, getattr(state, "lang", "en")),
            "kind": str(m.get("kind") or "em"),
            "power_w": 0.0,
            "today_kwh": 0.0,
            "cost_today": 0.0,
            "soc_pct": 0.0,
            "voltage_v": 0.0,
            "current_a": 0.0,
            "pf": 0.0,
            "freq_hz": 0.0,
            "phases": [],
            "q_phases": [],
            "appliances": [],
            "i_n": 0.0,
            "q_total_var": 0.0,
            "switch_on": None,
            "flow_role": _flow_role(k),
            "today_in_kwh": 0.0,
            "today_out_kwh": 0.0,
            "pending": True,
        })

    # Net "meter behind meter" display: a device wired behind another (e.g. a
    # wallbox on a circuit fed through the house meter) is virtually subtracted
    # from its parent's tile so the parent shows its net load. Purely a display
    # transform — applied BEFORE the cost chain so the parent's cost follows the
    # net energy. ``?raw=1`` (or raw=true) bypasses it to show the gross values.
    _raw_view = str(request.args.get("raw", "")).strip().lower() in ("1", "true", "yes", "on")
    if not _raw_view:
        try:
            from shelly_analyzer.services.net_display import (
                net_display_children, apply_live_subtraction)
            _cfg = getattr(state, "cfg", None)
            _submap = net_display_children(getattr(_cfg, "devices", []) or []) if _cfg else {}
            if _submap:
                apply_live_subtraction(devices_list, _submap)
        except Exception:
            pass

    # Chain-aware "today" cost per tile (they depend on each other): grid nets
    # import@tariff − export@feed-in, owner circuits pay only their grid share,
    # tenant pays full, PV/battery cost nothing. Safe no-op if it can't compute.
    try:
        _apply_today_chain_costs(state, devices_list)
    except Exception:
        pass

    return jsonify({"devices": devices_list})


@bp.route("/api/history")
def api_history():
    state = _get_state()
    raw_snap = state.live_store.snapshot()
    hist: Dict[str, List[Dict[str, Any]]] = {}

    for dkey, points in raw_snap.items():
        if dkey.startswith("_") or not isinstance(points, list) or not points:
            continue
        pts_out = []
        for p in points:
            va = float(p.get("va") or 0)
            vb = float(p.get("vb") or 0)
            vc = float(p.get("vc") or 0)
            ia = float(p.get("ia") or 0)
            ib = float(p.get("ib") or 0)
            ic = float(p.get("ic") or 0)
            pa = float(p.get("pa") or 0)
            pb = float(p.get("pb") or 0)
            pc = float(p.get("pc") or 0)
            non_zero_v = [v for v in [va, vb, vc] if v > 0]
            voltage_v = sum(non_zero_v) / len(non_zero_v) if non_zero_v else 0.0
            current_a = ia + ib + ic if (ib > 0 or ic > 0) else ia

            phases: List[Dict[str, float]] = []
            if vb > 0 or vc > 0:
                if va > 0:
                    phases.append({"voltage_v": va, "current_a": ia, "power_w": pa})
                if vb > 0:
                    phases.append({"voltage_v": vb, "current_a": ib, "power_w": pb})
                if vc > 0:
                    phases.append({"voltage_v": vc, "current_a": ic, "power_w": pc})

            h_qa = float(p.get("qa") or 0)
            h_qb = float(p.get("qb") or 0)
            h_qc = float(p.get("qc") or 0)
            h_q_phases: List[Dict[str, float]] = []
            if vb > 0 or vc > 0:
                if va > 0:
                    h_q_phases.append({"var": h_qa})
                if vb > 0:
                    h_q_phases.append({"var": h_qb})
                if vc > 0:
                    h_q_phases.append({"var": h_qc})

            pts_out.append({
                "ts": int(p.get("ts") or 0) * 1000,
                "w": float(p.get("power_total_w") or 0),
                "v": voltage_v,
                "a": current_a,
                "phases": phases,
                "i_n": _compute_i_n(float(p.get("i_n") or 0), ia, ib, ic, va, vb, vc),
                "q": float(p.get("q_total_var") or 0),
                "q_phases": h_q_phases,
                "hz": float(p.get("freq_hz") or 0),
            })
        hist[dkey] = pts_out

    # Net "meter behind meter": subtract each flagged child's power series from
    # its parent's, so the Live sparklines / history chart stay consistent with
    # the netted tiles. ``?raw=1`` shows the gross series. Only ``w`` is netted.
    _raw_view = str(request.args.get("raw", "")).strip().lower() in ("1", "true", "yes", "on")
    if not _raw_view:
        try:
            from shelly_analyzer.services.net_display import (
                net_display_children, apply_history_subtraction)
            _cfg = getattr(state, "cfg", None)
            _submap = net_display_children(getattr(_cfg, "devices", []) or []) if _cfg else {}
            if _submap:
                apply_history_subtraction(hist, _submap)
        except Exception:
            pass

    return jsonify({"history": hist})


@bp.route("/api/config")
def api_config():
    state = _get_state()
    return jsonify(state.get_config_response())


@bp.route("/api/version")
def api_version():
    from shelly_analyzer import __version__
    return jsonify({"version": __version__})


@bp.route("/api/jobs")
def api_jobs():
    state = _get_state()
    return jsonify(state.get_jobs())


@bp.route("/api/job")
def api_job():
    state = _get_state()
    try:
        jid = int(request.args.get("id", -1))
    except Exception:
        jid = -1
    return jsonify(state.get_job(jid))


@bp.route("/api/widget")
def api_widget():
    from flask import request as _req
    state = _get_state()
    profile_id = _req.args.get("profile", "")
    try:
        payload = state.on_action("widget", {"profile": profile_id}) if state.on_action else {"ok": False, "error": "not available"}
    except Exception as e:
        payload = {"ok": False, "error": str(e)}
    resp = jsonify(payload)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp
