"""Dashboard page routes: /, /plots, /control."""
from __future__ import annotations

from flask import Blueprint, Response, current_app, request

bp = Blueprint("dashboard", __name__)


def _get_state():
    return current_app.extensions["state"]


def _gzip_response(raw: bytes, gz: bytes, content_type: str = "text/html; charset=utf-8") -> Response:
    """Return gzipped or raw response based on Accept-Encoding."""
    ae = request.headers.get("Accept-Encoding", "")
    if "gzip" in ae and gz:
        resp = Response(gz, content_type=content_type)
        resp.headers["Content-Encoding"] = "gzip"
    else:
        resp = Response(raw, content_type=content_type)
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


@bp.route("/")
@bp.route("/index.html")
def index():
    state = _get_state()
    return _gzip_response(state._dashboard_html, state._dashboard_html_gz)


@bp.route("/plots")
@bp.route("/plots/<path:subpath>")
def plots(subpath=None):
    state = _get_state()
    return _gzip_response(state._plots_html, state._plots_html_gz)


@bp.route("/control")
@bp.route("/control/<path:subpath>")
def control(subpath=None):
    state = _get_state()
    return _gzip_response(state._control_html, state._control_html_gz)
