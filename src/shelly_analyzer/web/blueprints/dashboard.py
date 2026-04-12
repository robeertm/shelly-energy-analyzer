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


@bp.route("/api/setup/enable-demo", methods=["POST"])
def setup_enable_demo():
    """Activate demo mode: add demo devices, enable demo flag, save config, reload."""
    from dataclasses import replace
    from shelly_analyzer.io.config import save_config, DemoConfig
    from shelly_analyzer.services.demo import default_demo_devices
    state = _get_state()
    try:
        scenario = (request.get_json(silent=True) or {}).get("scenario", "household")
        new_demo = DemoConfig(enabled=True, seed=1234, scenario=str(scenario or "household"))
        existing_keys = {d.key for d in (state.cfg.devices or [])}
        new_devices = list(state.cfg.devices or [])
        for d in default_demo_devices():
            if d.key not in existing_keys:
                new_devices.append(d)
        new_cfg = replace(state.cfg, demo=new_demo, devices=new_devices)
        cfg_path = getattr(state, "_cfg_path", None)
        if cfg_path:
            save_config(new_cfg, cfg_path)
        state.cfg = new_cfg
        if hasattr(state, "reload_config"):
            state.reload_config(new_cfg)
        return Response(
            '{"ok": true, "devices_added": ' + str(len(new_devices) - len(existing_keys)) + '}',
            content_type="application/json",
        )
    except Exception as e:
        return Response(
            '{"ok": false, "error": "' + str(e).replace('"', "'") + '"}',
            content_type="application/json",
            status=500,
        )


@bp.route("/w")
def widget_page():
    """Standalone mini widget page for Android/PWA home screen."""
    profile = request.args.get("profile", "")
    from pathlib import Path
    tpl = Path(__file__).parent.parent / "templates" / "widget_page.html"
    if tpl.exists():
        html = tpl.read_text(encoding="utf-8")
        resp = Response(html.encode("utf-8"), content_type="text/html; charset=utf-8")
        resp.headers["Cache-Control"] = "no-store"
        return resp
    return Response(b"<h1>Widget page not available</h1>", content_type="text/html")


@bp.route("/widget-manifest.json")
def widget_manifest():
    """PWA manifest for home screen installation."""
    import json
    profile = request.args.get("profile", "")
    name = "Energy Widget"
    if profile:
        name += f" ({profile})"
    start_url = f"/w?profile={profile}" if profile else "/w"
    manifest = {
        "name": name,
        "short_name": "Energy",
        "description": "Shelly Energy Analyzer Widget",
        "start_url": start_url,
        "display": "standalone",
        "background_color": "#1a1a1a",
        "theme_color": "#ff9800",
        "icons": [
            {"src": "/widget-icon.svg", "sizes": "any", "type": "image/svg+xml"},
        ],
    }
    resp = Response(
        json.dumps(manifest),
        content_type="application/manifest+json",
        headers={"Cache-Control": "no-store"},
    )
    return resp


@bp.route("/widget-icon.svg")
def widget_icon():
    svg = '''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 192 192">
<rect width="192" height="192" rx="40" fill="#1a1a1a"/>
<text x="96" y="120" text-anchor="middle" font-size="100" fill="#ff9800">⚡</text>
</svg>'''
    return Response(svg, content_type="image/svg+xml")
