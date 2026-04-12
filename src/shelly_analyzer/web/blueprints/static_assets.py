"""Static asset routes: plotly.js, widget.js, file downloads."""
from __future__ import annotations

from flask import Blueprint, Response, current_app, send_file

bp = Blueprint("static_assets", __name__)


def _get_state():
    return current_app.extensions["state"]


@bp.route("/static/plotly.min.js")
def plotly_js():
    state = _get_state()
    body = state.get_plotly_js()
    if not body:
        return Response(
            b"/* plotly.min.js not available. Install the python package 'plotly' */",
            status=404,
            content_type="application/javascript; charset=utf-8",
        )
    return Response(
        body,
        content_type="application/javascript; charset=utf-8",
        headers={"Cache-Control": "public, max-age=3600"},
    )


@bp.route("/widget.js")
def widget_js():
    from flask import request as _req
    state = _get_state()
    profile_id = _req.args.get("profile", "")
    body = state.get_widget_script(profile_id=profile_id).encode("utf-8")
    return Response(
        body,
        content_type="application/javascript; charset=utf-8",
        headers={"Cache-Control": "no-store"},
    )


@bp.route("/files/<path:rel_path>")
def serve_file(rel_path: str):
    state = _get_state()
    try:
        data, ctype = state.read_file_bytes(rel_path)
        return Response(
            data,
            content_type=ctype,
            headers={"Cache-Control": "no-store"},
        )
    except FileNotFoundError:
        return Response(status=404)
    except Exception:
        return Response(status=500)
