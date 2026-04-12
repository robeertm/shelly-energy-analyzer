"""Device management API: CRUD + discovery."""
from __future__ import annotations

import json
import logging
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict, List

from flask import Blueprint, current_app, jsonify, request

from shelly_analyzer.io.config import AppConfig, DeviceConfig, load_config, save_config

logger = logging.getLogger(__name__)

bp = Blueprint("devices", __name__)


def _get_state():
    return current_app.extensions["state"]


@bp.route("/api/devices", methods=["GET"])
def list_devices():
    """List all configured devices with optional live status."""
    state = _get_state()
    devices = []
    snap = state.live_store.snapshot() if state.live_store else {}

    for d in state.cfg.devices:
        info: Dict[str, Any] = {
            "key": d.key,
            "name": d.name,
            "host": d.host,
            "em_id": d.em_id,
            "kind": getattr(d, "kind", "em"),
            "gen": getattr(d, "gen", 0),
            "model": getattr(d, "model", ""),
            "phases": getattr(d, "phases", 3),
            "supports_emdata": getattr(d, "supports_emdata", True),
            "online": d.key in snap and bool(snap[d.key]),
            # Auth metadata: never expose the actual password to the browser.
            "username": getattr(d, "username", "admin") or "admin",
            "has_password": bool(getattr(d, "password", "") or ""),
        }
        devices.append(info)
    return jsonify({"devices": devices})


@bp.route("/api/devices", methods=["POST"])
def add_device():
    """Add a new device. Accepts {host, name?, key?, em_id?, kind?}."""
    state = _get_state()
    try:
        body = request.get_json(silent=True) or {}
    except Exception:
        return jsonify({"ok": False, "error": "invalid JSON"}), 400

    host = str(body.get("host", "") or "").strip()
    if not host:
        return jsonify({"ok": False, "error": "host is required"}), 400

    # Auto-probe the device if no key given
    key = str(body.get("key", "") or "").strip()
    name = str(body.get("name", "") or "").strip()
    kind = str(body.get("kind", "em") or "em").strip()
    em_id = int(body.get("em_id", 0) or 0)
    gen = int(body.get("gen", 0) or 0)
    model = str(body.get("model", "") or "")
    phases = int(body.get("phases", 3) or 3)
    username = str(body.get("username", "admin") or "admin")
    password = str(body.get("password", "") or "")

    supports_emdata = True
    if not key:
        # Try to auto-detect – probe_device returns a DiscoveredDevice dataclass
        # and raises ValueError when the host is not a Shelly.
        try:
            from shelly_analyzer.services.discovery import probe_device
            result = probe_device(host, username=username, password=password)
            key = host.replace(".", "_")
            name = name or (result.model or key)
            kind = result.kind or kind
            gen = int(result.gen or gen or 0)
            model = result.model or model
            phases = int(result.phases or phases or 3)
            em_id = int(result.component_id or em_id or 0)
            supports_emdata = bool(result.supports_emdata)
        except ValueError as ve:
            if str(ve) == "auth_required":
                return jsonify({
                    "ok": False,
                    "error": "auth_required",
                    "message": f"{host} is password-protected. Provide username and password.",
                }), 401
            key = host.replace(".", "_")
        except Exception:
            key = host.replace(".", "_")

    if not name:
        name = key

    # Check duplicate key
    if any(d.key == key for d in state.cfg.devices):
        return jsonify({"ok": False, "error": f"Device key '{key}' already exists"}), 409

    new_device = DeviceConfig(
        key=key, name=name, host=host, em_id=em_id,
        kind=kind, gen=gen, model=model, phases=phases,
        supports_emdata=supports_emdata,
        username=username, password=password,
    )

    # Add to config and save
    new_devices = list(state.cfg.devices) + [new_device]
    new_cfg = replace(state.cfg, devices=new_devices)
    cfg_path = getattr(state, "_cfg_path", None) or Path("config.json")
    save_config(new_cfg, cfg_path)
    state.cfg = new_cfg
    state.reload_config(new_cfg)
    # Restart background services (live poller etc.) so the new device list
    # takes effect – otherwise newly added devices stay "offline" until a
    # full app restart.
    try:
        bg = getattr(state, "_bg", None)
        if bg is not None:
            bg.reload(new_cfg)
    except Exception as e:
        logger.warning("Background reload after device change failed: %s", e)

    logger.info("Device added: %s (%s)", key, host)
    return jsonify({"ok": True, "device": {"key": key, "name": name, "host": host, "kind": kind}})


@bp.route("/api/devices/<key>", methods=["PUT"])
def update_device(key: str):
    """Update a device's settings."""
    state = _get_state()
    try:
        body = request.get_json(silent=True) or {}
    except Exception:
        return jsonify({"ok": False, "error": "invalid JSON"}), 400

    idx = next((i for i, d in enumerate(state.cfg.devices) if d.key == key), None)
    if idx is None:
        return jsonify({"ok": False, "error": f"Device '{key}' not found"}), 404

    d = state.cfg.devices[idx]
    # Don't overwrite the stored password with the masked placeholder.
    incoming_pw = body.get("password", None)
    if incoming_pw == "***" or incoming_pw is None:
        new_password = getattr(d, "password", "") or ""
    else:
        new_password = str(incoming_pw or "")
    updated = DeviceConfig(
        key=key,
        name=str(body.get("name", d.name)),
        host=str(body.get("host", d.host)),
        em_id=int(body.get("em_id", d.em_id)),
        kind=str(body.get("kind", getattr(d, "kind", "em"))),
        gen=int(body.get("gen", getattr(d, "gen", 0))),
        model=str(body.get("model", getattr(d, "model", ""))),
        phases=int(body.get("phases", getattr(d, "phases", 3))),
        supports_emdata=bool(body.get("supports_emdata", getattr(d, "supports_emdata", True))),
        username=str(body.get("username", getattr(d, "username", "admin")) or "admin"),
        password=new_password,
    )

    new_devices = list(state.cfg.devices)
    new_devices[idx] = updated
    new_cfg = replace(state.cfg, devices=new_devices)
    cfg_path = getattr(state, "_cfg_path", None) or Path("config.json")
    save_config(new_cfg, cfg_path)
    state.cfg = new_cfg
    state.reload_config(new_cfg)
    # Restart background services (live poller etc.) so the new device list
    # takes effect – otherwise newly added devices stay "offline" until a
    # full app restart.
    try:
        bg = getattr(state, "_bg", None)
        if bg is not None:
            bg.reload(new_cfg)
    except Exception as e:
        logger.warning("Background reload after device change failed: %s", e)

    logger.info("Device updated: %s", key)
    return jsonify({"ok": True})


@bp.route("/api/devices/<key>", methods=["DELETE"])
def delete_device(key: str):
    """Remove a device."""
    state = _get_state()
    if not any(d.key == key for d in state.cfg.devices):
        return jsonify({"ok": False, "error": f"Device '{key}' not found"}), 404

    new_devices = [d for d in state.cfg.devices if d.key != key]
    new_cfg = replace(state.cfg, devices=new_devices)
    cfg_path = getattr(state, "_cfg_path", None) or Path("config.json")
    save_config(new_cfg, cfg_path)
    state.cfg = new_cfg
    state.reload_config(new_cfg)
    # Restart background services (live poller etc.) so the new device list
    # takes effect – otherwise newly added devices stay "offline" until a
    # full app restart.
    try:
        bg = getattr(state, "_bg", None)
        if bg is not None:
            bg.reload(new_cfg)
    except Exception as e:
        logger.warning("Background reload after device change failed: %s", e)

    logger.info("Device removed: %s", key)
    return jsonify({"ok": True})


@bp.route("/api/devices/discover", methods=["POST"])
def discover_devices():
    """Run mDNS discovery for Shelly devices on the local network."""
    try:
        from shelly_analyzer.services.mdns import discover_shelly_mdns
        timeout = float(request.args.get("timeout", 5))
        results = discover_shelly_mdns(timeout_seconds=min(30, max(1, timeout)))
        existing_hosts = {(d.host or "").strip().lower() for d in _get_state().cfg.devices}
        devices = []
        for r in results:
            host = getattr(r, "host", "") or ""
            name = getattr(r, "name", "") or ""
            # Derive a stable key from the mDNS instance name (e.g. shellyem-84CCA8C1...)
            key = name.lower()
            gen = int(getattr(r, "gen", 0) or 0)
            model = getattr(r, "model", "") or ""
            d = {
                "host": host,
                "name": name,
                "key": key,
                "kind": "em",
                "gen": gen,
                "model": model,
                "already_added": host.strip().lower() in existing_hosts,
            }
            if host:
                devices.append(d)
        return jsonify({"ok": True, "devices": devices})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "devices": []})


@bp.route("/api/devices/probe", methods=["POST"])
def probe_device_endpoint():
    """Probe a specific IP/host for a Shelly device.

    Optionally accepts ``username`` + ``password`` for password-protected
    devices. Returns ``{ok: false, error: 'auth_required'}`` with HTTP 401
    when the device responded with a 401 and no credentials were supplied.
    """
    try:
        body = request.get_json(silent=True) or {}
        host = str(body.get("host", "") or "").strip()
        if not host:
            return jsonify({"ok": False, "error": "host is required"}), 400
        username = str(body.get("username", "admin") or "admin")
        password = str(body.get("password", "") or "")

        from shelly_analyzer.services.discovery import probe_device
        try:
            result = probe_device(host, username=username, password=password)
        except ValueError as ve:
            if str(ve) == "auth_required":
                return jsonify({
                    "ok": False,
                    "error": "auth_required",
                    "message": f"{host} is password-protected. Provide username and password.",
                }), 401
            return jsonify({"ok": False, "error": f"No Shelly at {host}: {ve}"})
        # DiscoveredDevice is a dataclass → expose as dict for the JSON response
        return jsonify({"ok": True, "device": {
            "host": result.host,
            "gen": int(result.gen),
            "model": result.model,
            "kind": result.kind,
            "component_id": int(result.component_id),
            "phases": int(result.phases),
            "supports_emdata": bool(result.supports_emdata),
            "product_name": getattr(result, "product_name", "") or "",
            "category": getattr(result, "category", "") or "",
            "series": getattr(result, "series", "") or "",
        }})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/devices/<key>/firmware", methods=["POST"])
def update_firmware(key: str):
    """Trigger firmware update for a device."""
    state = _get_state()
    d = next((d for d in state.cfg.devices if d.key == key), None)
    if not d:
        return jsonify({"ok": False, "error": f"Device '{key}' not found"}), 404

    try:
        from shelly_analyzer.io.http import ShellyHttp, HttpConfig, build_rpc_url
        http = ShellyHttp(HttpConfig(
            timeout_seconds=float(state.cfg.download.timeout_seconds),
            retries=1,
        ))
        _pw = getattr(d, "password", "") or ""
        if _pw:
            http.set_credentials(d.host, getattr(d, "username", "admin") or "admin", _pw)
        # Trigger OTA update via the centralized client (so auth is applied).
        resp = http.get(build_rpc_url(d.host, "Shelly.Update"))
        try:
            payload = resp.json()
        except Exception:
            payload = resp.text[:200]
        return jsonify({"ok": True, "response": payload})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/supported-devices", methods=["GET"])
def supported_devices():
    """Return the full Shelly device registry for the settings UI."""
    from shelly_analyzer.services.device_registry import get_supported_summary, CATEGORY_LABELS, SERIES_LABELS
    return jsonify({
        "devices": get_supported_summary(),
        "category_labels": CATEGORY_LABELS,
        "series_labels": SERIES_LABELS,
        "total": len(get_supported_summary()),
    })
