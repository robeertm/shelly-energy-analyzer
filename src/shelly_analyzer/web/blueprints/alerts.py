"""Alert rules API: CRUD for AlertRule list."""
from __future__ import annotations

import logging
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict, List

from flask import Blueprint, current_app, jsonify, request

from shelly_analyzer.io.config import AlertRule, save_config

logger = logging.getLogger(__name__)

bp = Blueprint("alerts", __name__)


def _get_state():
    return current_app.extensions["state"]


def _rule_to_dict(r: AlertRule) -> Dict[str, Any]:
    return {
        "rule_id": r.rule_id, "enabled": r.enabled, "device_key": r.device_key,
        "metric": r.metric, "op": r.op, "threshold": r.threshold,
        "duration_seconds": r.duration_seconds, "cooldown_seconds": r.cooldown_seconds,
        "action_popup": r.action_popup, "action_beep": r.action_beep,
        "action_telegram": r.action_telegram, "action_webhook": r.action_webhook,
        "action_email": r.action_email, "message": r.message,
    }


def _save_rules(state, rules: List[AlertRule]) -> None:
    new_cfg = replace(state.cfg, alerts=rules)
    cfg_path = getattr(state, "_cfg_path", None) or Path("config.json")
    save_config(new_cfg, cfg_path)
    state.cfg = new_cfg
    state.reload_config(new_cfg)


@bp.route("/api/alerts", methods=["GET"])
def list_alerts():
    state = _get_state()
    return jsonify({"alerts": [_rule_to_dict(r) for r in (state.cfg.alerts or [])]})


@bp.route("/api/alerts", methods=["POST"])
def create_alert():
    state = _get_state()
    try:
        body = request.get_json(silent=True) or {}
        import uuid
        rule = AlertRule(
            rule_id=str(body.get("rule_id", "rule_" + uuid.uuid4().hex[:6])),
            enabled=bool(body.get("enabled", True)),
            device_key=str(body.get("device_key", "*") or "*"),
            metric=str(body.get("metric", "W") or "W").upper(),
            op=str(body.get("op", ">") or ">"),
            threshold=float(body.get("threshold", 0) or 0),
            duration_seconds=int(body.get("duration_seconds", 10) or 10),
            cooldown_seconds=int(body.get("cooldown_seconds", 120) or 120),
            action_popup=bool(body.get("action_popup", False)),
            action_beep=bool(body.get("action_beep", False)),
            action_telegram=bool(body.get("action_telegram", False)),
            action_webhook=bool(body.get("action_webhook", False)),
            action_email=bool(body.get("action_email", False)),
            message=str(body.get("message", "") or ""),
        )
        new_rules = list(state.cfg.alerts or []) + [rule]
        _save_rules(state, new_rules)
        return jsonify({"ok": True, "rule_id": rule.rule_id})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/alerts/<rule_id>", methods=["PUT"])
def update_alert(rule_id: str):
    state = _get_state()
    try:
        body = request.get_json(silent=True) or {}
        rules = list(state.cfg.alerts or [])
        idx = next((i for i, r in enumerate(rules) if r.rule_id == rule_id), None)
        if idx is None:
            return jsonify({"ok": False, "error": "Rule not found"}), 404
        old = rules[idx]
        updated = AlertRule(
            rule_id=rule_id,
            enabled=bool(body.get("enabled", old.enabled)),
            device_key=str(body.get("device_key", old.device_key) or "*"),
            metric=str(body.get("metric", old.metric) or "W").upper(),
            op=str(body.get("op", old.op) or ">"),
            threshold=float(body.get("threshold", old.threshold) or 0),
            duration_seconds=int(body.get("duration_seconds", old.duration_seconds) or 10),
            cooldown_seconds=int(body.get("cooldown_seconds", old.cooldown_seconds) or 120),
            action_popup=bool(body.get("action_popup", old.action_popup)),
            action_beep=bool(body.get("action_beep", old.action_beep)),
            action_telegram=bool(body.get("action_telegram", old.action_telegram)),
            action_webhook=bool(body.get("action_webhook", old.action_webhook)),
            action_email=bool(body.get("action_email", old.action_email)),
            message=str(body.get("message", old.message) or ""),
        )
        rules[idx] = updated
        _save_rules(state, rules)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/alerts/<rule_id>", methods=["DELETE"])
def delete_alert(rule_id: str):
    state = _get_state()
    rules = [r for r in (state.cfg.alerts or []) if r.rule_id != rule_id]
    if len(rules) == len(state.cfg.alerts or []):
        return jsonify({"ok": False, "error": "Rule not found"}), 404
    _save_rules(state, rules)
    return jsonify({"ok": True})
