"""Sync, import, data management, and schedule endpoints."""
from __future__ import annotations

import logging
from typing import Any, Dict

from flask import Blueprint, current_app, jsonify, request

logger = logging.getLogger(__name__)

bp = Blueprint("sync", __name__)


def _get_state():
    return current_app.extensions["state"]


@bp.route("/api/sync", methods=["POST"])
def trigger_sync():
    """Trigger a full sync (progress via /api/jobs)."""
    state = _get_state()
    try:
        body = request.get_json(silent=True) or {}
        mode = str(body.get("mode", "incremental") or "incremental")
        start_date = str(body.get("start_date", "") or "")
        params = {"mode": mode, "start_date": start_date}
        result = state.submit_action("sync", params)
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/sync/<device_key>", methods=["POST"])
def sync_device(device_key: str):
    """Sync a single device."""
    state = _get_state()
    try:
        body = request.get_json(silent=True) or {}
        mode = str(body.get("mode", "incremental") or "incremental")
        params = {"mode": mode, "device_key": device_key}
        result = state.submit_action("sync", params)
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/sync/status", methods=["GET"])
def sync_status():
    """Get sync status per device."""
    state = _get_state()
    try:
        devices = []
        for d in state.cfg.devices:
            meta = state.storage.db.get_device_meta(d.key) if hasattr(state.storage, 'db') else None
            last_ts = None
            if meta and isinstance(meta, dict):
                last_ts = meta.get("last_end_ts")
            devices.append({
                "key": d.key,
                "name": d.name,
                "last_sync_ts": last_ts,
            })
        return jsonify({"ok": True, "devices": devices})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/data/stats", methods=["GET"])
def data_stats():
    """Database statistics: size, row counts, etc."""
    state = _get_state()
    try:
        import os
        db_path = state.storage.base_dir / "energy.db"
        db_size = os.path.getsize(db_path) if db_path.exists() else 0

        # Get row counts per table
        db = state.storage.db
        stats = {"db_size_bytes": db_size, "db_size_mb": round(db_size / 1048576, 1)}

        try:
            import sqlite3
            conn = sqlite3.connect(str(db_path))
            cur = conn.cursor()
            for table in ["samples", "hourly_energy", "monthly_energy", "co2_intensity", "spot_prices"]:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    stats[f"{table}_rows"] = cur.fetchone()[0]
                except Exception:
                    stats[f"{table}_rows"] = 0
            conn.close()
        except Exception:
            pass

        # Per-device sample counts
        per_device = []
        for d in state.cfg.devices:
            try:
                import sqlite3
                conn = sqlite3.connect(str(db_path))
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM samples WHERE device_key = ?", (d.key,))
                count = cur.fetchone()[0]
                cur.execute("SELECT MIN(timestamp), MAX(timestamp) FROM samples WHERE device_key = ?", (d.key,))
                row = cur.fetchone()
                conn.close()
                per_device.append({
                    "key": d.key, "name": d.name,
                    "sample_count": count,
                    "first_ts": row[0] if row else None,
                    "last_ts": row[1] if row else None,
                })
            except Exception:
                per_device.append({"key": d.key, "name": d.name, "sample_count": 0})

        stats["devices"] = per_device
        return jsonify({"ok": True, **stats})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/data/cleanup", methods=["POST"])
def data_cleanup():
    """Apply data retention policy."""
    state = _get_state()
    try:
        db = state.storage.db
        if hasattr(db, "apply_retention"):
            db.apply_retention()
        return jsonify({"ok": True, "message": "Retention policy applied"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/schedules", methods=["GET"])
def list_schedules():
    """List all device schedules."""
    state = _get_state()
    schedules = []
    for s in getattr(state.cfg, "schedules", []) or []:
        schedules.append({
            "schedule_id": s.schedule_id,
            "device_key": s.device_key,
            "name": s.name,
            "time_on": s.time_on,
            "time_off": s.time_off,
            "weekdays": list(s.weekdays),
            "enabled": s.enabled,
            "switch_id": s.switch_id,
        })
    return jsonify({"schedules": schedules})


@bp.route("/api/schedules", methods=["POST"])
def create_schedule():
    """Create a new schedule."""
    state = _get_state()
    try:
        body = request.get_json(silent=True) or {}
        from shelly_analyzer.io.config import DeviceSchedule, save_config
        from dataclasses import replace
        import uuid

        sched = DeviceSchedule(
            schedule_id=str(body.get("schedule_id", str(uuid.uuid4())[:8])),
            device_key=str(body.get("device_key", "")),
            name=str(body.get("name", "Schedule")),
            time_on=str(body.get("time_on", "08:00")),
            time_off=str(body.get("time_off", "22:00")),
            weekdays=list(body.get("weekdays", [0, 1, 2, 3, 4, 5, 6])),
            enabled=bool(body.get("enabled", True)),
            switch_id=int(body.get("switch_id", 0)),
        )

        new_schedules = list(getattr(state.cfg, "schedules", []) or []) + [sched]
        new_cfg = replace(state.cfg, schedules=new_schedules)
        cfg_path = getattr(state, "_cfg_path", None)
        save_config(new_cfg, cfg_path)
        state.cfg = new_cfg
        state.reload_config(new_cfg)
        return jsonify({"ok": True, "schedule_id": sched.schedule_id})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/schedules/<schedule_id>", methods=["DELETE"])
def delete_schedule(schedule_id: str):
    """Delete a schedule."""
    state = _get_state()
    try:
        from shelly_analyzer.io.config import save_config
        from dataclasses import replace

        new_schedules = [s for s in (getattr(state.cfg, "schedules", []) or []) if s.schedule_id != schedule_id]
        if len(new_schedules) == len(getattr(state.cfg, "schedules", []) or []):
            return jsonify({"ok": False, "error": "Schedule not found"}), 404

        new_cfg = replace(state.cfg, schedules=new_schedules)
        cfg_path = getattr(state, "_cfg_path", None)
        save_config(new_cfg, cfg_path)
        state.cfg = new_cfg
        state.reload_config(new_cfg)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
