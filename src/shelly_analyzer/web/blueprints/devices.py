"""Device management API: CRUD + discovery."""
from __future__ import annotations

import json
import logging
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict, List

from flask import Blueprint, current_app, jsonify, request

from shelly_analyzer.io.config import (
    AppConfig,
    CompensationEntry,
    DeviceConfig,
    MainMeter,
    MeterReading,
    load_config,
    save_config,
)
from shelly_analyzer.i18n import t as _t

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

    # Synthetic pseudo-devices fed by the external PV source (PvSourceConfig).
    # They are not in cfg.devices but MUST be selectable in the Settings
    # device dropdowns (PV production, battery, grid meter), otherwise a
    # pre-set config value can never be picked from the UI. Marked synthetic so
    # the Devices section can render them read-only.
    try:
        pvs = getattr(state.cfg, "pv_source", None)
        if pvs is not None and getattr(pvs, "enabled", False):
            _existing = {d["key"] for d in devices}
            _synth = [
                ("pv", "PV (extern)"),
                ("battery", "Batterie (extern)"),
                ("grid_ext", "Netz (extern)"),
            ]
            for _k, _n in _synth:
                if _k in _existing:
                    continue
                devices.append({
                    "key": _k, "name": _n, "host": "", "em_id": 0,
                    "kind": "em", "gen": 0, "model": "external",
                    "phases": 3, "supports_emdata": False,
                    "online": _k in snap and bool(snap.get(_k)),
                    "username": "", "has_password": False,
                    "synthetic": True,
                })
    except Exception:
        pass

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
        compensation_percent=float(body.get("compensation_percent",
                                            getattr(d, "compensation_percent", 0.0)) or 0.0),
        # Preserve the dated calibration history — rebuilding DeviceConfig without
        # it silently wiped every calibration entry on any device edit.
        compensation_history=getattr(d, "compensation_history", ()) or (),
        # Meter cascade: which meter this device hangs under (main-meter id or
        # device key). Falls back to the existing value when not supplied.
        parent=str(body.get("parent", getattr(d, "parent", "")) or "").strip(),
        # Tenant sub-meter deducted from (not calibrated against) its parent meter.
        deduct_from_parent=bool(body.get("deduct_from_parent",
                                         getattr(d, "deduct_from_parent", False))),
        # Net "meter behind meter" display subtraction (Live + Plots), virtual.
        subtract_from_parent_display=bool(body.get("subtract_from_parent_display",
                                          getattr(d, "subtract_from_parent_display", False))),
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


@bp.route("/api/devices_kwh", methods=["GET"])
def devices_kwh():
    """Return per-device kWh totals over the last ?days=N days.

    Used by the Settings → Base fee split UI to prefill manual shares from
    actual consumption. Sourced from the pre-aggregated ``hourly_energy``
    table, so even multi-year totals stay cheap.
    """
    state = _get_state()
    try:
        import time as _t
        try:
            days = int(request.args.get("days", 30))
        except (TypeError, ValueError):
            days = 30
        days = max(1, min(days, 730))
        end_ts = int(_t.time())
        start_ts = end_ts - days * 86400
        out: Dict[str, float] = {}
        for d in state.cfg.devices:
            try:
                df = state.storage.db.query_hourly(d.key, start_ts=start_ts, end_ts=end_ts)
                if df is None or df.empty or "kwh" not in df.columns:
                    out[d.key] = 0.0
                else:
                    out[d.key] = round(float(df["kwh"].sum()), 3)
            except Exception:
                out[d.key] = 0.0
        return jsonify({"ok": True, "days": days, "kwh_by_device": out})
    except Exception as e:
        logger.exception("devices_kwh failed")
        return jsonify({"ok": False, "error": str(e)}), 500


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


# ── Measurement compensation ───────────────────────────────────────────────

def _comp_save_reload(state, new_cfg) -> None:
    cfg_path = getattr(state, "_cfg_path", None) or Path("config.json")
    save_config(new_cfg, cfg_path)
    state.cfg = new_cfg
    state.reload_config(new_cfg)
    try:
        bg = getattr(state, "_bg", None)
        if bg is not None:
            bg.reload(new_cfg)
    except Exception as e:
        logger.warning("Background reload after compensation change failed: %s", e)


def _comp_apply_percent(state, target: str, percent: float):
    """Return a new cfg with compensation_percent set on the target.
    target == 'global' -> all 3EM (kind 'em') devices; else a single device key."""
    devs = list(state.cfg.devices)
    for i, d in enumerate(devs):
        if target == "global":
            if str(getattr(d, "kind", "")) == "em":
                devs[i] = replace(d, compensation_percent=float(percent))
        elif d.key == target:
            devs[i] = replace(d, compensation_percent=float(percent))
    return replace(state.cfg, devices=devs)


def _comp_parse_ts(v) -> int:
    """Parse epoch seconds or an ISO/local datetime string to epoch seconds."""
    if v is None or v == "":
        raise ValueError("missing time")
    try:
        return int(float(v))
    except (TypeError, ValueError):
        pass
    from datetime import datetime
    s = str(v).strip().replace("Z", "")
    return int(datetime.fromisoformat(s).timestamp())


@bp.route("/api/compensation", methods=["GET"])
def get_compensation():
    state = _get_state()
    return jsonify({"ok": True, "devices": [
        {"key": d.key, "name": d.name, "kind": str(getattr(d, "kind", "em")),
         "compensation_percent": float(getattr(d, "compensation_percent", 0.0) or 0.0)}
        for d in state.cfg.devices
    ]})


@bp.route("/api/compensation/set", methods=["POST"])
def set_compensation_manual():
    state = _get_state()
    body = request.get_json(silent=True) or {}
    target = str(body.get("target", "")).strip()
    if not target:
        return jsonify({"ok": False, "error": "missing target"}), 400
    try:
        percent = float(body.get("percent"))
    except Exception:
        return jsonify({"ok": False, "error": "invalid percent"}), 400
    _comp_save_reload(state, _comp_apply_percent(state, target, percent))
    return jsonify({"ok": True, "target": target, "percent": percent})


@bp.route("/api/compensation/calibrate", methods=["POST"])
def calibrate_compensation():
    """Compute a compensation % from a meter comparison and apply it.
    Body: target ('global'|device_key), start, end (epoch or ISO), meter_start,
    meter_end (kWh). factor = meter_consumed / raw_app_kwh_over_period."""
    import pandas as pd
    state = _get_state()
    body = request.get_json(silent=True) or {}
    target = str(body.get("target", "")).strip()
    if not target:
        return jsonify({"ok": False, "error": "missing target"}), 400
    try:
        t0 = _comp_parse_ts(body.get("start"))
        t1 = _comp_parse_ts(body.get("end"))
        meter = float(body.get("meter_end")) - float(body.get("meter_start"))
    except Exception as e:
        return jsonify({"ok": False, "error": f"invalid input: {e}"}), 400
    _lang = getattr(state, "lang", "en")
    if t1 <= t0:
        return jsonify({"ok": False, "error": _t(_lang, "settings.compensation.err_end_after_start")}), 400
    if meter <= 0:
        return jsonify({"ok": False, "error": _t(_lang, "settings.compensation.err_meter_order")}), 400

    db = state.storage.db

    def _raw_kwh(dkey: str) -> float:
        dev = next((x for x in state.cfg.devices if x.key == dkey), None)
        cf = 1.0 + float(getattr(dev, "compensation_percent", 0.0) or 0.0) / 100.0 if dev else 1.0
        try:
            dfh = db.query_hourly(dkey, start_ts=t0, end_ts=t1)
            if dfh is not None and not dfh.empty and "kwh" in dfh.columns:
                comp_sum = float(pd.to_numeric(dfh["kwh"], errors="coerce").fillna(0).sum())
            else:
                comp_sum = 0.0
        except Exception:
            comp_sum = 0.0
        return comp_sum / cf if cf else comp_sum

    if target == "global":
        keys = [d.key for d in state.cfg.devices if str(getattr(d, "kind", "")) == "em"]
    else:
        keys = [target]
    raw = sum(_raw_kwh(k) for k in keys)
    if raw <= 0:
        return jsonify({"ok": False, "error": _t(_lang, "settings.compensation.err_no_data")}), 400

    factor = meter / raw
    percent = (factor - 1.0) * 100.0
    _comp_save_reload(state, _comp_apply_percent(state, target, percent))
    return jsonify({"ok": True, "target": target,
                    "percent": round(percent, 3), "factor": round(factor, 5),
                    "app_kwh_raw": round(raw, 3), "meter_kwh": round(meter, 3),
                    "keys": keys})


# ── Time-stamped calibration history ──────────────────────────────────────

def _comp_history_for_device(state, device_key: str) -> List[CompensationEntry]:
    d = next((x for x in state.cfg.devices if x.key == device_key), None)
    if d is None:
        return []
    return list(getattr(d, "compensation_history", ()) or ())


def _comp_set_history(state, device_key: str, entries: List[CompensationEntry]):
    """Return a new cfg with ``compensation_history`` replaced on the device.
    Also updates the legacy ``compensation_percent`` to mirror the *latest*
    entry so paths that haven't been migrated keep the right ‘current’ value."""
    entries = sorted(entries, key=lambda e: int(e.effective_from_ts))
    latest_pct = float(entries[-1].percent) if entries else None
    devs = list(state.cfg.devices)
    for i, d in enumerate(devs):
        if d.key == device_key:
            updates = {"compensation_history": tuple(entries)}
            if latest_pct is not None:
                updates["compensation_percent"] = latest_pct
            devs[i] = replace(d, **updates)
    return replace(state.cfg, devices=devs)


@bp.route("/api/compensation/history", methods=["GET"])
def get_compensation_history():
    """Return the calibration history for one device or all devices.
    Query: ?device=<key> (optional)."""
    state = _get_state()
    dev_key = (request.args.get("device") or "").strip()
    out = []
    for d in state.cfg.devices:
        if dev_key and d.key != dev_key:
            continue
        hist = getattr(d, "compensation_history", ()) or ()
        out.append({
            "key": d.key,
            "name": d.name,
            "kind": str(getattr(d, "kind", "em")),
            "compensation_percent": float(getattr(d, "compensation_percent", 0.0) or 0.0),
            "history": [
                {
                    "effective_from_ts": int(getattr(h, "effective_from_ts", 0) or 0),
                    "percent": float(getattr(h, "percent", 0.0) or 0.0),
                    "note": str(getattr(h, "note", "") or ""),
                    "meter_kwh": float(getattr(h, "meter_kwh", 0.0) or 0.0),
                    "raw_kwh": float(getattr(h, "raw_kwh", 0.0) or 0.0),
                }
                for h in hist
            ],
        })
    return jsonify({"ok": True, "devices": out})


@bp.route("/api/compensation/history", methods=["POST"])
def add_compensation_history():
    """Add a calibration entry.
    Body: {device, effective_from (epoch|ISO), percent, note?,
           meter_start?, meter_end?, raw_start_ts?, raw_end_ts?}.
    If meter_start/meter_end (+ raw_start_ts/raw_end_ts) are given, the
    percent is *computed* from raw vs. meter; otherwise the supplied percent
    is used verbatim."""
    import pandas as pd
    state = _get_state()
    body = request.get_json(silent=True) or {}
    device_key = str(body.get("device", "")).strip()
    if not device_key:
        return jsonify({"ok": False, "error": "missing device"}), 400
    dev = next((x for x in state.cfg.devices if x.key == device_key), None)
    if dev is None:
        return jsonify({"ok": False, "error": f"unknown device {device_key}"}), 404

    try:
        eff_ts = _comp_parse_ts(body.get("effective_from"))
    except Exception as e:
        return jsonify({"ok": False, "error": f"invalid effective_from: {e}"}), 400
    if eff_ts <= 0:
        return jsonify({"ok": False, "error": "effective_from must be a positive timestamp"}), 400

    note = str(body.get("note", "") or "")[:200]
    meter_kwh = 0.0
    raw_kwh = 0.0
    percent = None

    has_meter = body.get("meter_start") is not None and body.get("meter_end") is not None
    if has_meter:
        try:
            m_start = float(body.get("meter_start"))
            m_end = float(body.get("meter_end"))
        except Exception:
            return jsonify({"ok": False, "error": "invalid meter_start/meter_end"}), 400
        if m_end <= m_start:
            _lang = getattr(state, "lang", "en")
            return jsonify({"ok": False, "error": _t(_lang, "settings.compensation.err_meter_order")}), 400
        meter_kwh = m_end - m_start

        # Time range over which the meter delta was observed. Defaults to
        # ``effective_from`` minus 24h up to ``effective_from`` if unspecified.
        try:
            r_start = _comp_parse_ts(body.get("raw_start_ts")) if body.get("raw_start_ts") else eff_ts - 86400
            r_end = _comp_parse_ts(body.get("raw_end_ts")) if body.get("raw_end_ts") else eff_ts
        except Exception as e:
            return jsonify({"ok": False, "error": f"invalid raw range: {e}"}), 400
        if r_end <= r_start:
            _lang = getattr(state, "lang", "en")
            return jsonify({"ok": False, "error": _t(_lang, "settings.compensation.err_end_after_start")}), 400

        db = state.storage.db
        try:
            dfh = db.query_hourly(device_key, start_ts=r_start, end_ts=r_end)
        except Exception:
            dfh = None
        if dfh is None or dfh.empty or "kwh" not in dfh.columns:
            _lang = getattr(state, "lang", "en")
            return jsonify({"ok": False, "error": _t(_lang, "settings.compensation.err_no_data")}), 400
        comp_sum = float(pd.to_numeric(dfh["kwh"], errors="coerce").fillna(0).sum())
        # query_hourly applies the *current* compensation; un-do it to get raw.
        cf_now = 1.0 + float(getattr(dev, "compensation_percent", 0.0) or 0.0) / 100.0
        raw_kwh = comp_sum / cf_now if cf_now else comp_sum
        if raw_kwh <= 0:
            _lang = getattr(state, "lang", "en")
            return jsonify({"ok": False, "error": _t(_lang, "settings.compensation.err_no_data")}), 400
        factor = meter_kwh / raw_kwh
        percent = (factor - 1.0) * 100.0
    else:
        try:
            percent = float(body.get("percent"))
        except Exception:
            return jsonify({"ok": False, "error": "invalid percent"}), 400

    new_entry = CompensationEntry(
        effective_from_ts=int(eff_ts),
        percent=float(percent),
        note=note,
        meter_kwh=float(meter_kwh),
        raw_kwh=float(raw_kwh),
    )
    existing = _comp_history_for_device(state, device_key)
    # Replace an existing entry with the exact same effective_from_ts.
    existing = [e for e in existing if int(e.effective_from_ts) != int(eff_ts)]
    existing.append(new_entry)
    _comp_save_reload(state, _comp_set_history(state, device_key, existing))

    return jsonify({
        "ok": True,
        "device": device_key,
        "entry": {
            "effective_from_ts": int(eff_ts),
            "percent": round(float(percent), 3),
            "note": note,
            "meter_kwh": round(float(meter_kwh), 3),
            "raw_kwh": round(float(raw_kwh), 3),
        },
    })


@bp.route("/api/compensation/history", methods=["DELETE"])
def delete_compensation_history():
    """Delete one entry by ``effective_from_ts``.
    Body or query: {device, effective_from_ts}."""
    state = _get_state()
    body = request.get_json(silent=True) or {}
    device_key = str(body.get("device", "") or request.args.get("device", "")).strip()
    ts_raw = body.get("effective_from_ts", request.args.get("effective_from_ts"))
    try:
        ts_i = int(float(ts_raw))
    except Exception:
        return jsonify({"ok": False, "error": "invalid effective_from_ts"}), 400
    if not device_key:
        return jsonify({"ok": False, "error": "missing device"}), 400
    existing = _comp_history_for_device(state, device_key)
    new_list = [e for e in existing if int(e.effective_from_ts) != ts_i]
    if len(new_list) == len(existing):
        return jsonify({"ok": False, "error": "entry not found"}), 404
    _comp_save_reload(state, _comp_set_history(state, device_key, new_list))
    return jsonify({"ok": True, "device": device_key, "remaining": len(new_list)})


# ── Meter cascade (Hauptzähler / Zwischenzähler) ────────────────────────────
#
# A generic meter hierarchy: any number of physical reference meters
# (``main_meters``), each Shelly device attached to a parent (a main-meter id or
# another device key) via ``DeviceConfig.parent``. Calibration compares a meter's
# hand-read value against the SUM of its DIRECT measured children — so a house
# meter that covers e.g. "Haus" + "Wallbox" is calibrated once against both,
# instead of the meaningless "whole meter vs. one sub-meter". Sub-sub meters
# (switches behind a sub-meter) are NOT direct children of the main meter and are
# therefore never double-counted.

def _raw_kwh_over(state, db, dkey: str, t0: int, t1: int) -> float:
    """RAW, uncompensated measured kWh of a device over [t0, t1]. Uses
    ``compensate=False`` so calibration derives its factor from the physical
    measurement — stable no matter what dated compensation history already exists
    (dividing out the scalar factor drifted once a step-function history was set)."""
    import pandas as pd
    try:
        try:
            dfh = db.query_hourly(dkey, start_ts=t0, end_ts=t1, compensate=False)
        except TypeError:
            # DB without the compensate kwarg → undo the current scalar factor.
            dev = next((x for x in state.cfg.devices if x.key == dkey), None)
            cf = 1.0 + float(getattr(dev, "compensation_percent", 0.0) or 0.0) / 100.0 if dev else 1.0
            dfh = db.query_hourly(dkey, start_ts=t0, end_ts=t1)
            if dfh is not None and not dfh.empty and "kwh" in dfh.columns:
                return float(pd.to_numeric(dfh["kwh"], errors="coerce").fillna(0).sum()) / (cf or 1.0)
            return 0.0
        if dfh is not None and not dfh.empty and "kwh" in dfh.columns:
            return float(pd.to_numeric(dfh["kwh"], errors="coerce").fillna(0).sum())
    except Exception:
        pass
    return 0.0


def _raw_kwh_split_over(state, db, dkey: str, t0: int, t1: int):
    """RAW (uncompensated) measured energy of a device over [t0, t1] split by sign:
    returns ``(import_kwh, export_kwh)`` = (Σ positive hourly kWh, Σ |negative
    hourly kWh|). For a plain consumption meter ``export_kwh`` is 0; for a *signed*
    grid meter (+Bezug / −Einspeisung) it separates draw from feed-in — needed to
    calibrate a bidirectional grid connection meter against its Shelly."""
    import pandas as pd
    try:
        try:
            dfh = db.query_hourly(dkey, start_ts=t0, end_ts=t1, compensate=False)
        except TypeError:
            dfh = db.query_hourly(dkey, start_ts=t0, end_ts=t1)
        if dfh is not None and not dfh.empty and "kwh" in dfh.columns:
            col = pd.to_numeric(dfh["kwh"], errors="coerce").fillna(0.0)
            imp = float(col[col > 0].sum())
            exp = float(col[col < 0].abs().sum())
            return imp, exp
    except Exception:
        pass
    return 0.0, 0.0


def _comp_kwh_over(state, db, dkey: str, t0: int, t1: int) -> float:
    """COMPENSATED measured kWh of a device over [t0, t1] (applies its dated
    compensation history / flat factor). Used to subtract a deducted tenant
    sub-meter's *real* consumption from a shared utility-meter reading before the
    owner meters' drift factor is derived."""
    import pandas as pd
    try:
        dfh = db.query_hourly(dkey, start_ts=t0, end_ts=t1, compensate=True)
        if dfh is not None and not dfh.empty and "kwh" in dfh.columns:
            return float(pd.to_numeric(dfh["kwh"], errors="coerce").fillna(0).sum())
    except TypeError:
        # DB without the compensate kwarg → apply the scalar factor to raw.
        raw = _raw_kwh_over(state, db, dkey, t0, t1)
        dev = next((x for x in state.cfg.devices if x.key == dkey), None)
        cf = 1.0 + float(getattr(dev, "compensation_percent", 0.0) or 0.0) / 100.0 if dev else 1.0
        return raw * (cf or 1.0)
    except Exception:
        pass
    return 0.0


def _split_meter_children(devices, meter_id):
    """Direct em children of ``meter_id`` split into (calibrated, deducted).
    Deducted children are tenant sub-meters subtracted from the meter reading."""
    em = [d for d in devices
          if str(getattr(d, "parent", "") or "") == meter_id
          and str(getattr(d, "kind", "")) == "em"]
    calib = [d for d in em if not bool(getattr(d, "deduct_from_parent", False))]
    deduct = [d for d in em if bool(getattr(d, "deduct_from_parent", False))]
    return calib, deduct


def _set_history_on_cfg(cfg, device_key: str, entries):
    """Like ``_comp_set_history`` but operating on an arbitrary cfg, so several
    devices can be updated before a single save."""
    entries = sorted(entries, key=lambda e: int(e.effective_from_ts))
    latest_pct = float(entries[-1].percent) if entries else None
    devs = list(cfg.devices)
    for i, d in enumerate(devs):
        if d.key == device_key:
            updates = {"compensation_history": tuple(entries)}
            if latest_pct is not None:
                updates["compensation_percent"] = latest_pct
            devs[i] = replace(d, **updates)
    return replace(cfg, devices=devs)


def _slug(text: str) -> str:
    keep = [c.lower() if (c.isalnum()) else "-" for c in str(text)]
    s = "".join(keep).strip("-")
    while "--" in s:
        s = s.replace("--", "-")
    return s or "meter"


@bp.route("/api/meters", methods=["GET"])
def list_meters():
    """Return the meter tree: main meters plus every device with its parent, so
    the UI can render and edit the cascade."""
    state = _get_state()
    meters = [
        {
            "id": m.id, "name": m.name, "serial": m.serial,
            "readings": [
                {"ts": int(getattr(r, "ts", 0) or 0),
                 "kwh": float(getattr(r, "kwh", 0.0) or 0.0),
                 "export_kwh": float(getattr(r, "export_kwh", 0.0) or 0.0)}
                for r in sorted((getattr(m, "readings", ()) or ()), key=lambda r: int(getattr(r, "ts", 0) or 0))
            ],
            # Bidirectional grid meter = at least one reading logs a feed-in (2.8.0)
            # value → calibrated on import+export throughput.
            "bidirectional": any(float(getattr(r, "export_kwh", 0.0) or 0.0) > 0
                                 for r in (getattr(m, "readings", ()) or ())),
        }
        for m in (getattr(state.cfg, "main_meters", []) or [])
    ]
    devices = [
        {
            "key": d.key,
            "name": d.name,
            "kind": str(getattr(d, "kind", "em")),
            "parent": str(getattr(d, "parent", "") or ""),
            "compensation_percent": float(getattr(d, "compensation_percent", 0.0) or 0.0),
            "deduct_from_parent": bool(getattr(d, "deduct_from_parent", False)),
            "subtract_from_parent_display": bool(getattr(d, "subtract_from_parent_display", False)),
        }
        for d in state.cfg.devices
    ]
    return jsonify({"ok": True, "main_meters": meters, "devices": devices})


@bp.route("/api/meters", methods=["POST"])
def upsert_meter():
    """Add or update a main meter. Body: {id?, name, serial?}. Without id a new
    one is created (id derived from the name, uniquified)."""
    state = _get_state()
    body = request.get_json(silent=True) or {}
    name = str(body.get("name", "") or "").strip()
    serial = str(body.get("serial", "") or "").strip()
    mid = str(body.get("id", "") or "").strip()
    meters = list(getattr(state.cfg, "main_meters", []) or [])
    existing_ids = {m.id for m in meters}
    if mid:
        # update in place
        found = False
        for i, m in enumerate(meters):
            if m.id == mid:
                meters[i] = MainMeter(id=mid, name=name or m.name, serial=serial or m.serial)
                found = True
                break
        if not found:
            meters.append(MainMeter(id=mid, name=name, serial=serial))
    else:
        base = _slug(name or "meter")
        mid = base
        n = 2
        while mid in existing_ids:
            mid = f"{base}-{n}"
            n += 1
        meters.append(MainMeter(id=mid, name=name, serial=serial))
    _comp_save_reload(state, replace(state.cfg, main_meters=meters))
    return jsonify({"ok": True, "id": mid})


@bp.route("/api/meters/<mid>", methods=["DELETE"])
def delete_meter(mid: str):
    """Remove a main meter and detach any device that pointed at it (parent → '')."""
    state = _get_state()
    mid = str(mid or "").strip()
    meters = [m for m in (getattr(state.cfg, "main_meters", []) or []) if m.id != mid]
    devs = list(state.cfg.devices)
    for i, d in enumerate(devs):
        if str(getattr(d, "parent", "") or "") == mid:
            devs[i] = replace(d, parent="")
    _comp_save_reload(state, replace(state.cfg, main_meters=meters, devices=devs))
    return jsonify({"ok": True, "id": mid})


@bp.route("/api/compensation/calibrate_meter", methods=["POST"])
def calibrate_meter():
    """Calibrate a main meter against the SUM of its direct measured (``em``)
    children. Body: {meter_id, start, end, meter_start, meter_end,
    effective_from?}. Writes one dated calibration entry (same percent) to every
    direct em child. Switches and deeper sub-sub meters are excluded → no double
    counting."""
    state = _get_state()
    body = request.get_json(silent=True) or {}
    _lang = getattr(state, "lang", "en")
    meter_id = str(body.get("meter_id", "") or "").strip()
    if not meter_id:
        return jsonify({"ok": False, "error": "missing meter_id"}), 400
    try:
        t0 = _comp_parse_ts(body.get("start"))
        t1 = _comp_parse_ts(body.get("end"))
        meter = float(body.get("meter_end")) - float(body.get("meter_start"))
    except Exception as e:
        return jsonify({"ok": False, "error": f"invalid input: {e}"}), 400
    if t1 <= t0:
        return jsonify({"ok": False, "error": _t(_lang, "settings.compensation.err_end_after_start")}), 400
    if meter <= 0:
        return jsonify({"ok": False, "error": _t(_lang, "settings.compensation.err_meter_order")}), 400
    try:
        eff_raw = body.get("effective_from")
        eff_ts = _comp_parse_ts(eff_raw) if eff_raw not in (None, "") else t0
    except Exception:
        eff_ts = t0

    children, deduct_children = _split_meter_children(state.cfg.devices, meter_id)
    if not children:
        return jsonify({"ok": False,
                        "error": _t(_lang, "settings.compensation.err_no_children")}), 400

    db = state.storage.db
    # Subtract deducted tenant sub-meters (compensated → their real consumption)
    # from the meter reading so their usage doesn't inflate the owner factor.
    deduct = sum(_comp_kwh_over(state, db, d.key, t0, t1) for d in deduct_children)
    meter_eff = meter - deduct
    if meter_eff <= 0:
        return jsonify({"ok": False, "error": _t(_lang, "settings.compensation.err_meter_order")}), 400
    per = {d.key: _raw_kwh_over(state, db, d.key, t0, t1) for d in children}
    raw = sum(per.values())
    if raw <= 0:
        return jsonify({"ok": False, "error": _t(_lang, "settings.compensation.err_no_data")}), 400

    factor = meter_eff / raw
    percent = (factor - 1.0) * 100.0

    new_cfg = state.cfg
    for d in children:
        hist = [e for e in (getattr(d, "compensation_history", ()) or ())
                if int(e.effective_from_ts) != int(eff_ts)]
        hist.append(CompensationEntry(
            effective_from_ts=int(eff_ts),
            percent=percent,
            note=f"meter:{meter_id}",
            meter_kwh=meter_eff,
            raw_kwh=per[d.key],
        ))
        new_cfg = _set_history_on_cfg(new_cfg, d.key, hist)
    _comp_save_reload(state, new_cfg)

    return jsonify({
        "ok": True, "meter_id": meter_id,
        "percent": round(percent, 3), "factor": round(factor, 5),
        "deducted_kwh": round(deduct, 3), "meter_effective_kwh": round(meter_eff, 3),
        "meter_kwh": round(meter, 3), "raw_kwh": round(raw, 3),
        "children": [d.key for d in children],
        "per_child_raw": {k: round(v, 3) for k, v in per.items()},
    })


# ── Meter reading log (Zählerstand-Logbuch) ─────────────────────────────────
# The user logs hand-read meter values over time; calibration factors are derived
# from consumption BETWEEN consecutive readings. A single reading = baseline (no
# factor yet). Readings can be inserted at any date (older too) — the affected
# intervals are recomputed. Raw (uncompensated) app measurement is used so
# repeated recompute never drifts.

def _recompute_meter_from_readings(state, meter_id):
    """Derive dated calibration factors from a main meter's reading log and write
    them to its direct em children. Manual per-device entries (note != meter tag)
    are preserved. Returns the new cfg."""
    db = state.storage.db
    meter = next((m for m in (getattr(state.cfg, "main_meters", []) or []) if m.id == meter_id), None)
    if meter is None:
        return state.cfg
    readings = sorted((getattr(meter, "readings", ()) or ()), key=lambda r: int(r.ts))
    children, deduct_children = _split_meter_children(state.cfg.devices, meter_id)
    tag = "meter:" + str(meter_id)
    pre_tag = tag + ":pre"  # synthetic pre-first-reading step (weighted overall)

    # Bidirectional grid meter: at least one reading logs a feed-in register
    # (OBIS 2.8.0). Then calibration compares the meter's total THROUGHPUT
    # (Bezug 1.8.0 + Einspeisung 2.8.0) against the signed grid Shelly's own
    # throughput. This stays accurate when the owner barely imports (PV covers the
    # house, so most energy is fed back). A single measurement-gain factor is
    # applied to the signed device — correct for a CT whose error is the same in
    # both directions.
    bidir = any(float(getattr(r, "export_kwh", 0.0) or 0.0) > 0 for r in readings)
    grid_children = children
    nongrid_children = []
    if bidir and readings:
        # Only children that actually carry feed-in (negative/export energy) over
        # the reading span are the grid meter(s) being calibrated; pure-consumption
        # sub-meters (tenant, house) live on the load side and are auto-excluded —
        # a grid meter's registers already net them out.
        span0, span1 = int(readings[0].ts), int(readings[-1].ts)
        grid_children, nongrid_children = [], []
        for c in children:
            _, exp = _raw_kwh_split_over(state, db, c.key, span0, span1)
            (grid_children if exp > 1e-6 else nongrid_children).append(c)
        if not grid_children:   # no signed child → treat all as grid (best effort)
            grid_children = children

    derived = []  # (eff_ts, percent, meter_delta, raw_delta)
    for i in range(len(readings) - 1):
        t0, t1 = int(readings[i].ts), int(readings[i + 1].ts)
        if t1 <= t0:
            continue
        if bidir:
            # Meter throughput = Δ import register + Δ export register.
            imp_d = float(readings[i + 1].kwh) - float(readings[i].kwh)
            e0 = float(getattr(readings[i], "export_kwh", 0.0) or 0.0)
            e1 = float(getattr(readings[i + 1], "export_kwh", 0.0) or 0.0)
            exp_d = (e1 - e0) if (e0 > 0 and e1 >= e0) else 0.0
            meter_d = max(0.0, imp_d) + max(0.0, exp_d)
            raw_d = 0.0
            for c in grid_children:
                ci, ce = _raw_kwh_split_over(state, db, c.key, t0, t1)
                raw_d += ci + ce
        else:
            meter_d = float(readings[i + 1].kwh) - float(readings[i].kwh)
            # Subtract deducted tenant sub-meters (compensated → their real usage) so a
            # single meter covering owner + tenant still yields the OWNER drift factor.
            meter_d -= sum(_comp_kwh_over(state, db, c.key, t0, t1) for c in deduct_children)
            raw_d = sum(_raw_kwh_over(state, db, c.key, t0, t1) for c in children)
        if meter_d <= 0 or raw_d <= 0:
            continue
        pct = (meter_d / raw_d - 1.0) * 100.0
        derived.append((t0, pct, meter_d, raw_d))
    # Verbrauch VOR der ersten Ablesung hat keine eigene Messung → extrapolieren
    # mit dem verbrauchsgewichteten Gesamtfaktor (Gesamt-Zähler-Δ / Gesamt-roh-Δ).
    # Als synthetische Stufe ganz links (ts=1) abgelegt, sodass die Step-Funktion
    # sie automatisch für alle Samples vor der ersten echten Ablesung anwendet.
    # Notiz-Suffix ":pre" macht sie im Chart/Log identifizierbar (und ausblendbar).
    pre_entry = None
    if derived:
        sum_md = sum(md for (_t, _p, md, _r) in derived)
        sum_rd = sum(rd for (_t, _p, _m, rd) in derived)
        if sum_rd > 0:
            pct_overall = (sum_md / sum_rd - 1.0) * 100.0
            pre_entry = (1, pct_overall, sum_md, sum_rd)
    new_cfg = state.cfg
    # In bidirectional mode only the signed grid child(ren) carry the derived
    # factor; consumption-only children are excluded (see above).
    apply_children = grid_children if bidir else children
    for c in apply_children:
        kept = [e for e in (getattr(c, "compensation_history", ()) or ())
                if not str(getattr(e, "note", "")).startswith(tag)]
        made = [CompensationEntry(effective_from_ts=t0, percent=pct, note=tag,
                                  meter_kwh=md, raw_kwh=rd)
                for (t0, pct, md, rd) in derived]
        if pre_entry is not None:
            made.append(CompensationEntry(
                effective_from_ts=pre_entry[0], percent=pre_entry[1], note=pre_tag,
                meter_kwh=pre_entry[2], raw_kwh=pre_entry[3]))
        new_cfg = _set_history_on_cfg(new_cfg, c.key, kept + made)
    # Children that must NOT carry this meter's derived factor — deducted tenant
    # sub-meters (all modes) and consumption-only children auto-excluded from a
    # bidirectional grid meter — get any stale meter-tag entries stripped so they
    # drop an old shared factor (their own manual/flat compensation, note != meter
    # tag, is preserved).
    for c in list(deduct_children) + list(nongrid_children):
        old = getattr(c, "compensation_history", ()) or ()
        kept = [e for e in old if not str(getattr(e, "note", "")).startswith(tag)]
        if len(kept) != len(old):
            new_cfg = _set_history_on_cfg(new_cfg, c.key, kept)
    return new_cfg


@bp.route("/api/meters/<mid>/reading", methods=["POST"])
def add_meter_reading(mid):
    """Add (or replace at the same ts) a hand-read meter value, then recompute.
    Body: {ts, kwh, export_kwh?}. ``kwh`` = import register (OBIS 1.8.0 / Bezug);
    optional ``export_kwh`` = feed-in register (OBIS 2.8.0 / Einspeisung) for a
    bidirectional grid connection meter. ts may be older than existing readings."""
    state = _get_state()
    body = request.get_json(silent=True) or {}
    mid = str(mid or "").strip()
    try:
        ts = _comp_parse_ts(body.get("ts"))
        kwh = float(body.get("kwh"))
        _exp_raw = body.get("export_kwh")
        export_kwh = float(_exp_raw) if _exp_raw not in (None, "") else 0.0
    except Exception as e:
        return jsonify({"ok": False, "error": f"invalid input: {e}"}), 400
    meters = list(getattr(state.cfg, "main_meters", []) or [])
    idx = next((i for i, m in enumerate(meters) if m.id == mid), None)
    if idx is None:
        return jsonify({"ok": False, "error": "meter not found"}), 404
    m = meters[idx]
    rd = [r for r in (getattr(m, "readings", ()) or ()) if int(r.ts) != int(ts)]
    rd.append(MeterReading(ts=int(ts), kwh=kwh, export_kwh=max(0.0, export_kwh)))
    rd.sort(key=lambda r: int(r.ts))
    meters[idx] = replace(m, readings=tuple(rd))
    state.cfg = replace(state.cfg, main_meters=meters)  # recompute must see new readings
    new_cfg = _recompute_meter_from_readings(state, mid)
    _comp_save_reload(state, new_cfg)
    return jsonify({"ok": True, "meter_id": mid, "readings": len(rd)})


@bp.route("/api/meters/<mid>/reading/<ts>", methods=["DELETE"])
def delete_meter_reading(mid, ts):
    """Remove a reading by timestamp and recompute the cascade."""
    state = _get_state()
    mid = str(mid or "").strip()
    try:
        ts_i = int(float(ts))
    except Exception:
        return jsonify({"ok": False, "error": "invalid ts"}), 400
    meters = list(getattr(state.cfg, "main_meters", []) or [])
    idx = next((i for i, m in enumerate(meters) if m.id == mid), None)
    if idx is None:
        return jsonify({"ok": False, "error": "meter not found"}), 404
    m = meters[idx]
    rd = [r for r in (getattr(m, "readings", ()) or ()) if int(r.ts) != ts_i]
    meters[idx] = replace(m, readings=tuple(rd))
    state.cfg = replace(state.cfg, main_meters=meters)
    new_cfg = _recompute_meter_from_readings(state, mid)
    _comp_save_reload(state, new_cfg)
    return jsonify({"ok": True, "meter_id": mid, "readings": len(rd)})
