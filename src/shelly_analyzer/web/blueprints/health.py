"""Health check API: ping devices, check firmware."""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, List

from flask import Blueprint, current_app, jsonify

logger = logging.getLogger(__name__)

bp = Blueprint("health", __name__)


def _get_state():
    return current_app.extensions["state"]


@bp.route("/api/health", methods=["GET"])
def health_all():
    """Ping all devices + check firmware status."""
    state = _get_state()
    import requests as req

    results: List[Dict[str, Any]] = []
    for d in state.cfg.devices:
        entry: Dict[str, Any] = {
            "key": d.key,
            "name": d.name,
            "host": d.host,
            "kind": getattr(d, "kind", "em"),
            "online": False,
            "latency_ms": None,
            "firmware": None,
            "firmware_update_available": False,
            "uptime_s": None,
            "error": None,
        }
        # Ping Shelly device
        try:
            t0 = time.time()
            # Gen2+ RPC first, fall back to Gen1 /status
            try:
                r = req.get(f"http://{d.host}/rpc/Shelly.GetStatus", timeout=3)
                if r.status_code == 200:
                    data = r.json()
                    entry["online"] = True
                    sys = data.get("sys", {}) or {}
                    entry["uptime_s"] = sys.get("uptime")
                    av = sys.get("available_updates") or {}
                    entry["firmware_update_available"] = bool(av.get("stable") or av.get("beta"))
            except Exception:
                pass

            if not entry["online"]:
                # Try Gen1
                try:
                    r = req.get(f"http://{d.host}/status", timeout=3)
                    if r.status_code == 200:
                        data = r.json()
                        entry["online"] = True
                        entry["uptime_s"] = data.get("uptime")
                        upd = data.get("update", {}) or {}
                        entry["firmware_update_available"] = upd.get("has_update", False)
                except Exception:
                    pass

            if entry["online"]:
                entry["latency_ms"] = int((time.time() - t0) * 1000)
                # Fetch firmware version via Shelly.GetDeviceInfo (Gen2+) or /shelly (Gen1)
                try:
                    r2 = req.get(f"http://{d.host}/rpc/Shelly.GetDeviceInfo", timeout=2)
                    if r2.status_code == 200:
                        info = r2.json()
                        entry["firmware"] = info.get("fw_id") or info.get("ver", "")
                    else:
                        r3 = req.get(f"http://{d.host}/shelly", timeout=2)
                        if r3.status_code == 200:
                            info = r3.json()
                            entry["firmware"] = info.get("fw") or info.get("ver", "")
                except Exception:
                    pass

        except Exception as e:
            entry["error"] = str(e)[:100]

        results.append(entry)

    return jsonify({"ok": True, "devices": results, "ts": int(time.time())})


@bp.route("/api/health/<device_key>/update", methods=["POST"])
def trigger_firmware_update(device_key: str):
    """Trigger OTA firmware update for a Shelly device."""
    state = _get_state()
    d = next((d for d in state.cfg.devices if d.key == device_key), None)
    if not d:
        return jsonify({"ok": False, "error": "Device not found"}), 404

    import requests as req
    try:
        # Try Gen2+ RPC endpoint first
        try:
            r = req.post(
                f"http://{d.host}/rpc/Shelly.Update",
                json={"stage": "stable"},
                timeout=10,
            )
            if r.status_code == 200:
                return jsonify({"ok": True, "gen": 2, "message": "Update gestartet",
                                "response": r.json() if r.text else {}})
        except Exception:
            pass

        # Fallback: Gen2+ GET
        try:
            r = req.get(f"http://{d.host}/rpc/Shelly.Update?stage=stable", timeout=10)
            if r.status_code == 200:
                return jsonify({"ok": True, "gen": 2, "message": "Update gestartet",
                                "response": r.json() if r.text else {}})
        except Exception:
            pass

        # Fallback: Gen1 OTA endpoint
        r = req.get(f"http://{d.host}/ota?update=1", timeout=10)
        if r.status_code == 200:
            return jsonify({"ok": True, "gen": 1, "message": "Update gestartet",
                            "response": r.text[:200]})
        return jsonify({"ok": False, "error": f"HTTP {r.status_code}: {r.text[:200]}"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/health/<device_key>", methods=["GET"])
def health_device(device_key: str):
    """Ping a single device."""
    state = _get_state()
    d = next((d for d in state.cfg.devices if d.key == device_key), None)
    if not d:
        return jsonify({"ok": False, "error": "Device not found"}), 404

    import requests as req
    t0 = time.time()
    try:
        r = req.get(f"http://{d.host}/rpc/Shelly.GetStatus", timeout=3)
        if r.status_code != 200:
            r = req.get(f"http://{d.host}/status", timeout=3)
        ok = r.status_code == 200
        latency = int((time.time() - t0) * 1000)
        return jsonify({"ok": ok, "latency_ms": latency, "data": r.json() if ok else None})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})
