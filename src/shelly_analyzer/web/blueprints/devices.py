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

    if not key:
        # Try to auto-detect
        try:
            from shelly_analyzer.services.discovery import probe_device
            result = probe_device(host)
            if result:
                key = result.get("key") or host.replace(".", "_")
                name = name or result.get("name") or key
                kind = result.get("kind") or kind
                gen = result.get("gen") or gen
                model = result.get("model") or model
                phases = result.get("phases") or phases
            else:
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
    )

    # Add to config and save
    new_devices = list(state.cfg.devices) + [new_device]
    new_cfg = replace(state.cfg, devices=new_devices)
    cfg_path = getattr(state, "_cfg_path", None) or Path("config.json")
    save_config(new_cfg, cfg_path)
    state.cfg = new_cfg
    state.reload_config(new_cfg)

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
    )

    new_devices = list(state.cfg.devices)
    new_devices[idx] = updated
    new_cfg = replace(state.cfg, devices=new_devices)
    cfg_path = getattr(state, "_cfg_path", None) or Path("config.json")
    save_config(new_cfg, cfg_path)
    state.cfg = new_cfg
    state.reload_config(new_cfg)

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
    """Probe a specific IP/host for a Shelly device."""
    try:
        body = request.get_json(silent=True) or {}
        host = str(body.get("host", "") or "").strip()
        if not host:
            return jsonify({"ok": False, "error": "host is required"}), 400

        from shelly_analyzer.services.discovery import probe_device
        result = probe_device(host)
        if result:
            return jsonify({"ok": True, "device": result})
        else:
            return jsonify({"ok": False, "error": f"No Shelly device found at {host}"})
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
        from shelly_analyzer.io.http import ShellyHttp, HttpConfig
        http = ShellyHttp(HttpConfig(
            timeout_seconds=float(state.cfg.download.timeout_seconds),
            retries=1,
        ))
        # Trigger OTA update
        import requests as req
        resp = req.get(f"http://{d.host}/rpc/Shelly.Update", timeout=10)
        return jsonify({"ok": True, "response": resp.json() if resp.status_code == 200 else resp.text[:200]})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})
