"""SSL API: GET /api/ssl/status, POST /api/ssl/regenerate."""
from __future__ import annotations

import logging
from pathlib import Path

from flask import Blueprint, current_app, jsonify

from shelly_analyzer.web.ssl_utils import force_regenerate, inspect_cert

logger = logging.getLogger(__name__)

bp = Blueprint("ssl", __name__)


def _get_state():
    return current_app.extensions["state"]


def _cert_dir() -> Path:
    state = _get_state()
    return Path(state.out_dir) / "data" / "runtime" / "ssl"


def _current_cert_path() -> Path:
    """Return the cert path the running server is using (custom or self-signed)."""
    state = _get_state()
    cfg = state.cfg
    mode = str(getattr(cfg.ui, "live_web_ssl_mode", "auto") or "auto").lower()
    if mode == "custom":
        custom = str(getattr(cfg.ui, "live_web_ssl_cert", "") or "").strip()
        if custom:
            return Path(custom)
    return _cert_dir() / "server.crt"


def _info_to_json(info) -> dict:
    return {
        "exists": info.exists,
        "path": info.path,
        "subject": info.subject,
        "issuer": info.issuer,
        "is_self_signed": info.is_self_signed,
        "not_before": info.not_before.isoformat() if info.not_before else None,
        "not_after": info.not_after.isoformat() if info.not_after else None,
        "days_remaining": info.days_remaining,
        "sha256": info.sha256,
        "error": info.error,
    }


@bp.route("/api/ssl/status", methods=["GET"])
def api_ssl_status():
    state = _get_state()
    cfg = state.cfg
    mode = str(getattr(cfg.ui, "live_web_ssl_mode", "auto") or "auto").lower()
    auto_renew = bool(getattr(cfg.ui, "live_web_ssl_auto_renew", True))
    renew_days = int(getattr(cfg.ui, "live_web_ssl_renew_days", 30) or 30)

    cert_path = _current_cert_path()
    info = inspect_cert(cert_path)

    is_https = bool(getattr(state, "_is_https", False))

    return jsonify({
        "ok": True,
        "mode": mode,
        "is_https": is_https,
        "auto_renew": auto_renew,
        "renew_days": renew_days,
        "will_renew": bool(
            mode != "custom"
            and auto_renew
            and info.days_remaining is not None
            and info.days_remaining < renew_days
        ),
        "cert": _info_to_json(info),
    })


@bp.route("/api/ssl/regenerate", methods=["POST"])
def api_ssl_regenerate():
    """Force-regenerate the self-signed cert. Only allowed in 'auto' mode —
    custom certs must be managed by the user. Takes effect after the next
    app restart."""
    state = _get_state()
    cfg = state.cfg
    mode = str(getattr(cfg.ui, "live_web_ssl_mode", "auto") or "auto").lower()
    if mode == "custom":
        return jsonify({
            "ok": False,
            "error": "Cannot regenerate a user-supplied (custom) certificate. "
                     "Switch SSL mode to 'auto' or replace the files manually.",
        }), 400
    try:
        cert, key, info = force_regenerate(_cert_dir())
        logger.info("SSL certificate regenerated via API: %s", cert)
        return jsonify({
            "ok": True,
            "message": "Certificate regenerated. Restart the app for it to take effect.",
            "cert": _info_to_json(info),
        })
    except Exception as e:
        logger.exception("SSL regenerate failed")
        return jsonify({"ok": False, "error": str(e)}), 500
