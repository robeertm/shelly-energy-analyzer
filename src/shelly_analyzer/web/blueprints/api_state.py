"""API endpoints for live state, history, config, and jobs."""
from __future__ import annotations

import json
import math
from typing import Any, Dict, List, Optional

from flask import Blueprint, Response, current_app, jsonify, request

bp = Blueprint("api_state", __name__)


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


@bp.route("/api/state")
def api_state():
    state = _get_state()
    raw_snap = state.live_store.snapshot()

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
        latest: Dict[str, Any] = points[-1]
        meta = dev_meta_by_key.get(dkey, {})
        name = str(meta.get("name") or dkey)

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
            "name": str(m.get("name") or k),
            "kind": str(m.get("kind") or "em"),
            "power_w": 0.0,
            "today_kwh": 0.0,
            "cost_today": 0.0,
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
            "pending": True,
        })

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
