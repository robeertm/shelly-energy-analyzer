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
    # First-run: no devices configured AND wizard not explicitly dismissed
    try:
        devs = list(getattr(state.cfg, "devices", []) or [])
        if not devs and request.args.get("skip_wizard") != "1":
            # Safety: if config on disk has devices but state lost them (e.g. after
            # a settings save that didn't preserve the devices list), reload first.
            try:
                from shelly_analyzer.io.config import load_config
                cfg_path = getattr(state, "_cfg_path", None)
                if cfg_path:
                    from pathlib import Path
                    fresh = load_config(str(cfg_path))
                    if fresh.devices:
                        state.cfg = fresh
                        state.reload_config(fresh)
                        devs = list(fresh.devices)
            except Exception:
                pass
            if not devs:
                from flask import redirect
                return redirect("/setup")
    except Exception:
        pass
    return _gzip_response(state._dashboard_html, state._dashboard_html_gz)


@bp.route("/setup")
def setup_page():
    from pathlib import Path
    from shelly_analyzer.web import _inject_version_badge
    tpl = Path(__file__).parent.parent / "templates" / "setup.html"
    if tpl.exists():
        html = _inject_version_badge(tpl.read_text(encoding="utf-8"))
        resp = Response(html.encode("utf-8"), content_type="text/html; charset=utf-8")
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        return resp
    return Response(b"<h1>Setup wizard not available</h1>", content_type="text/html")


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


@bp.route("/settings")
def settings_page():
    from pathlib import Path
    from shelly_analyzer.web import _inject_version_badge
    tpl = Path(__file__).parent.parent / "templates" / "settings.html"
    if tpl.exists():
        html = _inject_version_badge(tpl.read_text(encoding="utf-8"))
        resp = Response(html.encode("utf-8"), content_type="text/html; charset=utf-8")
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        return resp
    return Response(b"<h1>Settings page not yet available</h1>", content_type="text/html")
