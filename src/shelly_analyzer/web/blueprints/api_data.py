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
