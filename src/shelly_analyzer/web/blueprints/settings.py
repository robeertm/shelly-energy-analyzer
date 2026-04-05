"""Settings API: GET/PUT /api/settings, test endpoints."""
from __future__ import annotations

import json
import logging
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict

from flask import Blueprint, current_app, jsonify, request

from shelly_analyzer.io.config import AppConfig, load_config, save_config

logger = logging.getLogger(__name__)

bp = Blueprint("settings", __name__)


def _get_state():
    return current_app.extensions["state"]


def _cfg_to_json(cfg: AppConfig) -> Dict[str, Any]:
    """Serialize AppConfig to a JSON-safe dict using save_config's format."""
    # save_config writes to file — we need the dict. Read the raw JSON back.
    import tempfile
    tmp = Path(tempfile.mktemp(suffix=".json"))
    try:
        save_config(cfg, tmp)
        raw = json.loads(tmp.read_text(encoding="utf-8"))
        return raw
    finally:
        try:
            tmp.unlink()
        except Exception:
            pass


@bp.route("/api/i18n", methods=["GET"])
def get_i18n():
    """Return the effective i18n map + language for the current user session.
    Only keys starting with the given prefix (default "web.") are returned
    to keep the payload small. Used by the Settings page for client-side
    translation of its many hard-coded labels.
    """
    from shelly_analyzer.i18n import get_lang_map
    state = _get_state()
    lang = getattr(state, "lang", "de")
    prefix = request.args.get("prefix", "web.")
    full = get_lang_map(lang)
    # Filter to keys starting with prefix (saves bandwidth)
    filtered = {k: v for k, v in full.items() if k.startswith(prefix) or k.startswith("settings.") or k.startswith("toast.")}
    resp = jsonify({"lang": lang, "map": filtered})
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return resp


@bp.route("/api/settings", methods=["GET"])
def get_settings():
    """Return full config as JSON."""
    state = _get_state()
    data = _cfg_to_json(state.cfg)
    # Remove sensitive fields from GET response
    ui = data.get("ui", {})
    if "telegram_bot_token" in ui and ui["telegram_bot_token"]:
        ui["telegram_bot_token"] = "***"
    if "email_smtp_password" in ui and ui["email_smtp_password"]:
        ui["email_smtp_password"] = "***"
    advisor = data.get("advisor", {})
    if "openai_api_key" in advisor and advisor["openai_api_key"]:
        advisor["openai_api_key"] = "***"
    if "anthropic_api_key" in advisor and advisor["anthropic_api_key"]:
        advisor["anthropic_api_key"] = "***"
    resp = jsonify(data)
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return resp


@bp.route("/api/settings", methods=["PUT"])
def put_settings():
    """Update config from JSON. Accepts partial updates (merged with current)."""
    state = _get_state()
    try:
        updates = request.get_json(silent=True) or {}
    except Exception:
        return jsonify({"ok": False, "error": "invalid JSON"}), 400

    if not isinstance(updates, dict):
        return jsonify({"ok": False, "error": "expected JSON object"}), 400

    try:
        # Get current config as dict
        current = _cfg_to_json(state.cfg)

        # Deep merge updates into current
        _deep_merge(current, updates)

        # Don't overwrite masked secrets with "***"
        _restore_secrets(current, state.cfg)

        # Write merged config to file
        cfg_path = getattr(state, "_cfg_path", None) or Path("config.json")
        Path(cfg_path).write_text(
            json.dumps(current, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        # Reload config
        new_cfg = load_config(str(cfg_path))
        state.cfg = new_cfg
        state.reload_config(new_cfg)

        # Debug: log key effective fields so we can verify the round-trip
        try:
            logger.info(
                "Settings updated: ui.language=%s  ui.theme=%s  state.lang=%s",
                getattr(new_cfg.ui, "language", None),
                getattr(new_cfg.ui, "theme", None),
                getattr(state, "lang", None),
            )
        except Exception:
            pass

        # Reload action dispatcher if available (pass new lang so PDF exports + plots
        # translations pick up language switch immediately)
        new_lang = getattr(state, "lang", None)
        dispatcher = state.on_action
        if hasattr(dispatcher, "reload"):
            try:
                dispatcher.reload(new_cfg, lang=new_lang)  # type: ignore[union-attr]
            except TypeError:
                dispatcher.reload(new_cfg)  # type: ignore[union-attr]
        elif hasattr(dispatcher, "__self__") and hasattr(dispatcher.__self__, "reload"):
            try:
                dispatcher.__self__.reload(new_cfg, lang=new_lang)
            except TypeError:
                dispatcher.__self__.reload(new_cfg)

        logger.info("Settings updated and saved")
        return jsonify({"ok": True})

    except Exception as e:
        logger.error("Failed to save settings: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/settings/test-telegram", methods=["POST"])
def test_telegram():
    """Send a test message via Telegram."""
    state = _get_state()
    try:
        import requests as req
        token = state.cfg.ui.telegram_bot_token
        chat_id = state.cfg.ui.telegram_chat_id
        if not token or not chat_id:
            return jsonify({"ok": False, "error": "Telegram not configured"})
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        resp = req.post(url, json={"chat_id": chat_id, "text": "Shelly Energy Analyzer: Test message"}, timeout=10)
        if resp.status_code == 200:
            return jsonify({"ok": True, "message": "Test message sent"})
        else:
            return jsonify({"ok": False, "error": f"Telegram API: {resp.status_code} {resp.text[:200]}"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/settings/test-mqtt", methods=["POST"])
def test_mqtt():
    """Test MQTT connection."""
    state = _get_state()
    mqtt_cfg = getattr(state.cfg, "mqtt", None)
    if not mqtt_cfg or not getattr(mqtt_cfg, "enabled", False):
        return jsonify({"ok": False, "error": "MQTT not enabled"})
    try:
        import paho.mqtt.client as mqtt
        client = mqtt.Client()
        if mqtt_cfg.username:
            client.username_pw_set(mqtt_cfg.username, mqtt_cfg.password)
        client.connect(mqtt_cfg.broker, mqtt_cfg.port, 5)
        client.publish(f"{mqtt_cfg.topic_prefix}/test", "connection_test", qos=0)
        client.disconnect()
        return jsonify({"ok": True, "message": "MQTT connection successful"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/settings/test-influxdb", methods=["POST"])
def test_influxdb():
    """Test InfluxDB connection."""
    state = _get_state()
    influx_cfg = getattr(state.cfg, "influxdb", None)
    if not influx_cfg or not getattr(influx_cfg, "enabled", False):
        return jsonify({"ok": False, "error": "InfluxDB not enabled"})
    try:
        import requests as req
        url = influx_cfg.url.rstrip("/")
        if int(influx_cfg.version) >= 2:
            resp = req.get(f"{url}/health", timeout=5)
        else:
            resp = req.get(f"{url}/ping", timeout=5)
        if resp.status_code in (200, 204):
            return jsonify({"ok": True, "message": "InfluxDB reachable"})
        else:
            return jsonify({"ok": False, "error": f"InfluxDB: {resp.status_code}"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


def _deep_merge(base: dict, updates: dict) -> None:
    """Recursively merge updates into base dict."""
    for k, v in updates.items():
        if k in base and isinstance(base[k], dict) and isinstance(v, dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v


def _restore_secrets(merged: dict, cfg: AppConfig) -> None:
    """Restore masked secrets (***) from the original config."""
    ui = merged.get("ui", {})
    if ui.get("telegram_bot_token") == "***":
        ui["telegram_bot_token"] = cfg.ui.telegram_bot_token
    if ui.get("email_smtp_password") == "***":
        ui["email_smtp_password"] = cfg.ui.email_smtp_password
    advisor = merged.get("advisor", {})
    if advisor.get("openai_api_key") == "***":
        advisor["openai_api_key"] = getattr(cfg.advisor, "openai_api_key", "")
    if advisor.get("anthropic_api_key") == "***":
        advisor["anthropic_api_key"] = getattr(cfg.advisor, "anthropic_api_key", "")
