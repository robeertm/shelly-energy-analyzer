"""API endpoints for POST actions: /api/run, /api/set_window."""
from __future__ import annotations

import json
from typing import Any, Dict, Optional

from flask import Blueprint, current_app, jsonify, request

bp = Blueprint("api_actions", __name__)


def _get_state():
    return current_app.extensions["state"]


@bp.route("/api/run", methods=["POST"])
def api_run():
    state = _get_state()
    try:
        obj = request.get_json(silent=True) or {}
    except Exception:
        obj = {}
    action = str(obj.get("action", "") or "")
    params = obj.get("params") if isinstance(obj.get("params"), dict) else {}

    try:
        if action == "set_language":
            lang = str(params.get("language", "de") or "de")
            payload = {"ok": True, "language": lang}
            return jsonify(payload)

        if action in {"get_switch", "set_switch", "toggle_switch",
                       "get_freeze", "set_freeze", "toggle_freeze"}:
            if not state.on_action:
                payload = {"ok": False, "error": "Remote actions not available"}
            else:
                payload = state.on_action(action, params)
            return jsonify(payload)

        # Async job submission for everything else
        payload = state.submit_action(action, params)
        return jsonify(payload)

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/set_window", methods=["POST"])
def api_set_window():
    state = _get_state()
    minutes: Optional[int] = None

    # Try query parameter first
    try:
        if "minutes" in request.args:
            minutes = int(request.args["minutes"])
    except Exception:
        minutes = None

    # Try request body
    if minutes is None:
        try:
            obj = request.get_json(silent=True) or {}
            if "minutes" in obj:
                minutes = int(obj["minutes"])
        except Exception:
            minutes = None

    if minutes is None:
        return jsonify({"ok": False, "error": "missing minutes"}), 400

    try:
        minutes = state.set_window_minutes(minutes)
        return jsonify({"ok": True, "window_minutes": minutes})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
