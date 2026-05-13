"""API endpoints for data: costs, heatmap, solar, co2, compare, etc."""
from __future__ import annotations

from urllib.parse import parse_qs, urlparse
from typing import Any, Dict

from flask import Blueprint, current_app, jsonify, request

bp = Blueprint("api_data", __name__)


def _get_state():
    return current_app.extensions["state"]


def _get_qs_params() -> Dict[str, Any]:
    """Extract query string parameters as a flat dict."""
    return {k: v for k, v in request.args.items()}


def _action_endpoint(action_name: str):
    """Generic handler: delegate to on_action callback with query params."""
    state = _get_state()
    try:
        params = _get_qs_params()
        if state.on_action:
            payload = state.on_action(action_name, params)
        else:
            payload = {"ok": False, "error": "not available"}
    except Exception as e:
        payload = {"ok": False, "error": str(e)}
    return jsonify(payload)


@bp.route("/api/costs")
def api_costs():
    return _action_endpoint("costs")


@bp.route("/api/heatmap")
def api_heatmap():
    return _action_endpoint("heatmap")


@bp.route("/api/solar")
def api_solar():
    return _action_endpoint("solar")


@bp.route("/api/weather_correlation")
def api_weather_correlation():
    return _action_endpoint("weather_correlation")


@bp.route("/api/co2_live")
def api_co2_live():
    return _action_endpoint("co2_live")


@bp.route("/api/co2")
def api_co2():
    return _action_endpoint("co2")


@bp.route("/api/compare")
def api_compare():
    return _action_endpoint("compare")


@bp.route("/api/anomalies")
def api_anomalies():
    return _action_endpoint("anomalies")


@bp.route("/api/forecast")
def api_forecast():
    return _action_endpoint("forecast")


@bp.route("/api/standby")
def api_standby():
    return _action_endpoint("standby")


@bp.route("/api/sankey")
def api_sankey():
    return _action_endpoint("sankey")


@bp.route("/api/plots_data")
def api_plots_data():
    return _action_endpoint("plots_data")


@bp.route("/api/smart_schedule")
def api_smart_schedule():
    return _action_endpoint("smart_schedule")


@bp.route("/api/auto_schedule/status")
def api_auto_schedule_status():
    """Live decisions of the spot-price auto-scheduler.

    Returns one entry per configured rule with the latest decision
    (on/off/idle/skipped/no_data), the cheapest block found, daily run
    counter, and the last action taken. Mostly used by the Settings UI
    to render the dry-run preview before the user enables live switching.
    """
    state = _get_state()
    bg = getattr(state, "_bg", None)
    auto = getattr(bg, "_auto_scheduler", None) if bg is not None else None
    if auto is None:
        return jsonify({"ok": True, "running": False, "decisions": []})
    try:
        decisions = auto.get_decisions()
        return jsonify({
            "ok": True,
            "running": True,
            "decisions": [
                {
                    "rule_id": d.rule_id, "rule_name": d.rule_name,
                    "enabled": d.enabled, "dry_run": d.dry_run,
                    "decision": d.decision, "reason": d.reason,
                    "block_start_ts": d.block_start_ts, "block_end_ts": d.block_end_ts,
                    "block_avg_ct": d.block_avg_ct,
                    "last_evaluated_ts": d.last_evaluated_ts,
                    "runs_today": d.runs_today, "last_run_day": d.last_run_day,
                    "last_set_on": d.last_set_on, "last_action_ts": d.last_action_ts,
                    "last_error": d.last_error,
                }
                for d in decisions
            ],
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "decisions": []})


@bp.route("/api/ev_sessions")
def api_ev_sessions():
    return _action_endpoint("ev_sessions")


@bp.route("/api/tariff_compare")
def api_tariff_compare():
    return _action_endpoint("tariff_compare")


@bp.route("/api/battery")
def api_battery():
    return _action_endpoint("battery")


@bp.route("/api/advisor")
def api_advisor():
    return _action_endpoint("advisor")


@bp.route("/api/goals")
def api_goals():
    return _action_endpoint("goals")


@bp.route("/api/nilm_status")
def api_nilm_status():
    state = _get_state()
    try:
        store = state.live_store
        clusters = getattr(store, "_nilm_clusters", [])
        trans_count = getattr(store, "_nilm_transition_count", 0)
        payload = {
            "ok": True,
            "cluster_count": len(clusters),
            "transition_count": trans_count,
            "clusters": clusters[:10],
        }
    except Exception as e:
        payload = {"ok": False, "error": str(e), "cluster_count": 0, "transition_count": 0, "clusters": []}
    return jsonify(payload)


@bp.route("/api/nilm_detail")
def api_nilm_detail():
    """Rich NILM data for the dedicated NILM statistics tab."""
    state = _get_state()
    try:
        store = state.live_store
        clusters = list(getattr(store, "_nilm_clusters", []))
        trans_count = int(getattr(store, "_nilm_transition_count", 0))
        transitions = list(getattr(store, "_nilm_transitions", []))
        device_count = int(getattr(store, "_nilm_device_count", 0))

        # Hourly distribution from transitions
        hourly = [0] * 24
        for tr in transitions:
            try:
                import datetime
                h = datetime.datetime.fromtimestamp(tr["ts"]).hour
                hourly[h] += 1
            except Exception:
                pass

        # Per-device stats
        device_stats: Dict[str, Any] = {}
        for c in clusters:
            dk = c.get("device_key", "")
            if dk not in device_stats:
                device_stats[dk] = {"cluster_count": 0, "total_events": 0, "top_appliances": []}
            device_stats[dk]["cluster_count"] += 1
            device_stats[dk]["total_events"] += c.get("count", 0)
            if c.get("matched_appliance"):
                device_stats[dk]["top_appliances"].append({
                    "appliance": c["matched_appliance"],
                    "icon": c.get("icon", ""),
                    "centroid_w": c.get("centroid_w", 0),
                    "count": c.get("count", 0),
                })

        # Category breakdown from clusters
        from shelly_analyzer.services.appliance_detector import APPLIANCES
        cat_map = {a.id: a.category for a in APPLIANCES}
        categories: Dict[str, int] = {}
        for c in clusters:
            cat = cat_map.get(c.get("matched_appliance", ""), "unknown")
            categories[cat] = categories.get(cat, 0) + c.get("count", 0)

        # Appliance signatures reference
        signatures = [
            {"id": a.id, "icon": a.icon, "category": a.category,
             "power_min": a.power_min, "power_max": a.power_max,
             "pattern_type": a.pattern_type, "typical_duration_min": a.typical_duration_min}
            for a in APPLIANCES
        ]

        # Device name mapping
        device_names: Dict[str, str] = {}
        try:
            for d in state.cfg.devices:
                device_names[d.key] = getattr(d, "name", "") or d.key
        except Exception:
            pass

        # NILM-eligible devices (must match the gating in BackgroundServiceManager
        # ._init_nilm_learners: 3-phase EM only — switch-class plugs don't see
        # an aggregate household signal and 1-phase EMs see too little).
        nilm_devices = []
        try:
            for d in state.cfg.devices:
                if int(getattr(d, "phases", 3) or 3) < 3:
                    continue
                if str(getattr(d, "kind", "em")) == "switch":
                    continue
                nilm_devices.append({"key": d.key, "name": getattr(d, "name", "") or d.key})
        except Exception:
            pass

        payload = {
            "ok": True,
            "cluster_count": len(clusters),
            "transition_count": trans_count,
            "device_count": device_count,
            "clusters": clusters,
            "transitions": transitions[:200],
            "hourly_distribution": hourly,
            "device_stats": device_stats,
            "device_names": device_names,
            "nilm_devices": nilm_devices,
            "categories": categories,
            "signatures": signatures,
        }
    except Exception as e:
        payload = {"ok": False, "error": str(e)}
    return jsonify(payload)


@bp.route("/api/ev_chargers")
def api_ev_chargers():
    try:
        lat = float(request.args.get("lat", 0))
        lon = float(request.args.get("lon", 0))
        radius = max(100, min(10000, int(request.args.get("radius", 500))))
        api_key = str(request.args.get("key", "") or "")
        min_kw = float(request.args.get("min_kw", 0) or 0)
        plug = str(request.args.get("plug", "") or "")
        from shelly_analyzer.services.ev_charger import fetch_ev_chargers
        payload = fetch_ev_chargers(lat, lon, radius_m=radius, api_key=api_key, min_kw=min_kw, plug_filter=plug)
    except Exception as e:
        payload = {"ok": False, "error": str(e), "stations": []}
    return jsonify(payload)


@bp.route("/api/traffic")
def api_traffic():
    try:
        from shelly_analyzer.services.traffic import TrafficMonitor
        payload = TrafficMonitor.get().snapshot()
    except Exception as e:
        payload = {"error": str(e)}
    return jsonify(payload)


@bp.route("/api/v1/<path:subpath>")
def api_v1(subpath=""):
    state = _get_state()
    try:
        from shelly_analyzer.services import api_v1 as api_v1_mod
        payload = api_v1_mod.handle_v1_request("/api/v1/" + subpath, state)
    except Exception as e:
        payload = {"ok": False, "error": str(e)}
    return jsonify(payload)
