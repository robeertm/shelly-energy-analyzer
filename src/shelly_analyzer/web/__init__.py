"""Flask web application for the Shelly Energy Analyzer."""
from __future__ import annotations

import gzip
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from flask import Flask

from shelly_analyzer.io.config import AppConfig, load_config
from shelly_analyzer.io.storage import Storage
from shelly_analyzer.i18n import get_lang_map, normalize_lang, t as _t
from shelly_analyzer.services.webdash import (
    LiveStateStore,
    _render_template,
    _render_template_tokens,
    _plotly_min_js_bytes,
    _HTML_TEMPLATE,
    _PLOTS_TEMPLATE,
    _CONTROL_TEMPLATE,
)

logger = logging.getLogger(__name__)


# ── In-memory log ring buffer (for the web Sync/Log tab) ──
from collections import deque
import threading

_LOG_BUFFER: "deque" = deque(maxlen=2000)
_LOG_LOCK = threading.Lock()
# Whether to capture werkzeug HTTP access logs in the ring buffer.
# Off by default to keep the Sync/Log tab focused on app events.
_LOG_INCLUDE_HTTP: bool = False


def set_log_include_http(flag: bool) -> None:
    global _LOG_INCLUDE_HTTP
    _LOG_INCLUDE_HTTP = bool(flag)


def get_log_include_http() -> bool:
    return _LOG_INCLUDE_HTTP


class _WebLogHandler(logging.Handler):
    """Capture log records into an in-memory ring buffer for the web UI."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            # Skip werkzeug/http access logs unless explicitly enabled
            if not _LOG_INCLUDE_HTTP:
                name = record.name or ""
                if name.startswith("werkzeug") or name == "waitress.queue":
                    return
            msg = self.format(record)
            with _LOG_LOCK:
                _LOG_BUFFER.append({
                    "ts": int(record.created),
                    "level": record.levelname,
                    "name": record.name,
                    "msg": msg,
                })
        except Exception:
            pass


def get_log_entries(since_ts: int = 0, limit: int = 500) -> list:
    """Return log entries newer than since_ts (up to limit most recent)."""
    with _LOG_LOCK:
        arr = [e for e in _LOG_BUFFER if e["ts"] > int(since_ts)]
    return arr[-int(limit):]


def _install_web_log_handler() -> None:
    root = logging.getLogger()
    # Install only once
    for h in root.handlers:
        if isinstance(h, _WebLogHandler):
            return
    h = _WebLogHandler()
    h.setFormatter(logging.Formatter("%(message)s"))
    h.setLevel(logging.INFO)
    root.addHandler(h)


def _inject_version_badge(html: str) -> str:
    """Inject a small fixed version badge into the bottom-right of any page."""
    from shelly_analyzer import __version__
    badge = (
        '<div id="app-version-badge" style="position:fixed;'
        'top:2px;left:4px;z-index:9999;font-size:9px;'
        'color:rgba(128,128,128,0.6);font-family:ui-monospace,SFMono-Regular,Menlo,monospace;'
        'pointer-events:none;user-select:none;padding:0 3px">'
        f'v{__version__} \u00a9 Robert Manuwald</div>'
    )
    if "</body>" in html:
        return html.replace("</body>", badge + "</body>", 1)
    return html + badge


def _render_dashboard_html(state: "AppState") -> bytes:
    """Render the main dashboard HTML using the existing template engine."""
    from shelly_analyzer.web.app_context import AppState  # noqa: F811

    tpl = _HTML_TEMPLATE
    lang = state.lang
    web_i18n = get_lang_map(lang)

    values: Dict[str, str] = {
        "lang": lang,
        "web_live_title": _t(lang, "web.live.title"),
        "web_live_meta": _t(lang, "web.live.meta"),
        "web_nav_live": _t(lang, "web.nav.live"),
        "web_nav_control": _t(lang, "web.nav.control"),
        "web_pill_window": _t(lang, "web.pill.window"),
        "web_pill_url": _t(lang, "web.pill.url"),
        "web_tab_live": _t(lang, "web.tab.live"),
        "sync_btn_incremental": _t(lang, "web.sync.btn.incremental"),
        "sync_btn_full": _t(lang, "web.sync.btn.full"),
        "sync_btn_day": _t(lang, "web.sync.btn.day"),
        "sync_btn_week": _t(lang, "web.sync.btn.week"),
        "sync_btn_month": _t(lang, "web.sync.btn.month"),
        "sync_btn_status": _t(lang, "web.sync.btn.status"),
        "sync_opt_autoscroll": _t(lang, "web.sync.opt.autoscroll"),
        "sync_opt_http_logs": _t(lang, "web.sync.opt.http_logs"),
        "sync_status_loading": _t(lang, "web.sync.status.loading"),
        "sync_log": _t(lang, "web.sync.log"),
        "web_tab_costs": _t(lang, "web.tab.costs"),
        "web_tab_heatmap": _t(lang, "web.tab.heatmap"),
        "web_tab_solar": _t(lang, "web.tab.solar"),
        "web_tab_weather": _t(lang, "web.tab.weather"),
        "web_tab_compare": _t(lang, "web.tab.compare"),
        "web_tab_co2": _t(lang, "web.tab.co2"),
        "web_tab_anomalies": _t(lang, "web.tab.anomalies"),
        "web_tab_forecast": _t(lang, "web.tab.forecast"),
        "web_tab_standby": _t(lang, "web.tab.standby"),
        "web_tab_sankey": _t(lang, "web.tab.sankey"),
        "web_tab_ev": _t(lang, "web.tab.ev"),
        "web_ev_radius": _t(lang, "web.ev.radius"),
        "web_ev_city_placeholder": _t(lang, "web.ev.city_placeholder"),
        "web_ev_all_power": _t(lang, "web.ev.all_power"),
        "web_ev_all_plugs": _t(lang, "web.ev.all_plugs"),
        "web_ev_apikey_hint": _t(lang, "web.ev.apikey_hint"),
        "web_ev_save": _t(lang, "web.ev.save"),
        "t_patterns": _t(lang, "web.nilm.patterns"),
        "t_learning": _t(lang, "web.nilm.learning"),
        "t_transitions": _t(lang, "web.nilm.transitions"),
        "t_waiting": _t(lang, "web.nilm.waiting"),
        "web_tab_export": _t(lang, "web.tab.export"),
        "web_tab_tenants": _t(lang, "web.tab.tenants"),
        "smart_sched_title": _t(lang, "smart_sched.title") if _t(lang, "smart_sched.title") != "smart_sched.title" else "Smart Schedule",
        "ev_log_title": _t(lang, "ev_log.title") if _t(lang, "ev_log.title") != "ev_log.title" else "EV Charging Log",
        "tariff_title": _t(lang, "tariff.title") if _t(lang, "tariff.title") != "tariff.title" else "Tariff Comparison",
        "battery_title": _t(lang, "battery.title") if _t(lang, "battery.title") != "battery.title" else "Battery",
        "advisor_title": _t(lang, "advisor.title") if _t(lang, "advisor.title") != "advisor.title" else "AI Advisor",
        "goals_title": _t(lang, "goals.title") if _t(lang, "goals.title") != "goals.title" else "Goals & Badges",
        "exp_daterange": _t(lang, "web.control.export.daterange"),
        "exp_from": _t(lang, "web.control.plots.from"),
        "exp_to": _t(lang, "web.control.plots.to"),
        "exp_invoice_settings": _t(lang, "web.control.export.invoice_settings"),
        "exp_invoice": _t(lang, "web.control.invoice"),
        "exp_anchor": _t(lang, "web.control.anchor"),
        "exp_custom_note": _t(lang, "web.control.custom_note"),
        "exp_bundle_hours": _t(lang, "web.control.export.bundle_hours"),
        "exp_actions": _t(lang, "web.control.export.actions"),
        "exp_btn_pdf": _t(lang, "web.control.btn.pdf"),
        "exp_btn_invoices": _t(lang, "web.control.btn.invoices"),
        "exp_btn_excel": _t(lang, "web.control.btn.excel"),
        "exp_btn_bundle": _t(lang, "web.control.btn.bundle"),
        "exp_btn_report_day": _t(lang, "web.control.btn.report_day"),
        "exp_btn_report_month": _t(lang, "web.control.btn.report_month"),
        "exp_preview": _t(lang, "web.control.export.preview"),
        "exp_no_preview": _t(lang, "web.control.export.no_preview"),
        "exp_today": _t(lang, "web.control.export.today"),
        "exp_this_week": _t(lang, "web.control.export.this_week"),
        "exp_this_month": _t(lang, "web.control.export.this_month"),
        "exp_this_year": _t(lang, "web.control.export.this_year"),
        "exp_all": _t(lang, "web.control.export.all"),
        "exp_results": _t(lang, "web.control.export.results"),
        "exp_jobs": _t(lang, "web.control.export.jobs"),
        "exp_open_file": _t(lang, "web.control.export.open_file"),
        "exp_no_results": _t(lang, "web.control.export.no_results"),
        "exp_job_running": _t(lang, "web.control.export.job_running"),
        "exp_job_done": _t(lang, "web.control.export.job_done"),
        "exp_job_error": _t(lang, "web.control.export.job_error"),
        "web_btn_freeze_title": _t(lang, "web.dash.freeze_resume"),
        "web_btn_settings_title": _t(lang, "web.dash.device_settings"),
        "web_btn_theme_title": _t(lang, "web.btn.theme"),
        "web_loading": _t(lang, "web.loading"),
        "web_update_banner_title": _t(lang, "web.update_banner.title"),
        "web_update_banner_open": _t(lang, "web.update_banner.open"),
        "web_widget_title": _t(lang, "web.widget.title"),
        "web_widget_step1": _t(lang, "web.widget.step1"),
        "web_widget_step2": _t(lang, "web.widget.step2"),
        "web_widget_step3": _t(lang, "web.widget.step3"),
        "web_widget_step4": _t(lang, "web.widget.step4"),
        "web_widget_step5": _t(lang, "web.widget.step5"),
        "web_widget_btn_copy": _t(lang, "web.widget.btn.copy"),
        "web_widget_btn_download": _t(lang, "web.widget.btn.download"),
        "web_widget_copied": _t(lang, "web.widget.copied"),
        "web_dash_device_order": _t(lang, "web.dash.device_order"),
        "web_dash_done": _t(lang, "web.dash.done"),
        "t_error_label": _t(lang, "web.error_label"),
        "t_starting": _t(lang, "web.starting"),
        "t_job_started": _t(lang, "web.job_started"),
        "refresh_ms": str(int(max(250, state.refresh_seconds * 1000))),
        "window_min": str(int(max(1, state.window_minutes))),
        "window_options_json": json.dumps(state.available_windows, ensure_ascii=False),
        "devices_json": json.dumps(state.devices_meta, ensure_ascii=False),
        "i18n_json": json.dumps(web_i18n, ensure_ascii=False),
    }
    rendered = _render_template(tpl, values)
    # Note: the dashboard's gear icon (btn-live-settings, defined in the
    # template itself) already redirects to /settings#sec-devices, so no
    # extra icon injection is needed here — there was previously a 🔧
    # button alongside ⚙ which doubled up after v16.17.0.
    return _inject_version_badge(rendered).encode("utf-8")


def _render_control_html(state: "AppState") -> bytes:
    """Render the control page HTML."""
    from shelly_analyzer.web.app_context import AppState  # noqa: F811

    tpl = _CONTROL_TEMPLATE
    lang = state.lang

    values: Dict[str, str] = {
        "lang": lang,
        "web_nav_live": _t(lang, "web.nav.live"),
        "web_nav_control": _t(lang, "web.nav.control"),
        "web_control_title": _t(lang, "web.control.title"),
        "web_control_meta": _t(lang, "web.control.meta"),
        "web_control_sync": _t(lang, "web.control.sync"),
        "web_control_plots": _t(lang, "web.control.plots"),
        "web_control_open_plotly": _t(lang, "web.control.open_plotly"),
        "web_control_export": _t(lang, "web.control.export"),
        "web_control_jobs": _t(lang, "web.control.jobs"),
        "web_control_mode": _t(lang, "web.control.mode"),
        "web_control_start": _t(lang, "web.control.start"),
        "web_control_btn_sync": _t(lang, "web.control.btn.sync"),
        "web_control_plots_from": _t(lang, "web.control.plots.from"),
        "web_control_plots_to": _t(lang, "web.control.plots.to"),
        "web_control_btn_plots": _t(lang, "web.control.btn.plots"),
        "web_control_invoice": _t(lang, "web.control.invoice"),
        "web_control_anchor": _t(lang, "web.control.anchor"),
        "web_control_custom_note": _t(lang, "web.control.custom_note"),
        "web_control_btn_summary": _t(lang, "web.control.btn.pdf"),
        "web_control_btn_invoices": _t(lang, "web.control.btn.invoices"),
        "web_control_btn_excel": _t(lang, "web.control.btn.excel"),
        "web_control_btn_bundle": _t(lang, "web.control.btn.bundle"),
        "web_control_btn_report_day": _t(lang, "web.control.btn.report_day"),
        "web_control_btn_report_month": _t(lang, "web.control.btn.report_month"),
        "web_control_export_daterange": _t(lang, "web.control.export.daterange"),
        "web_control_export_invoice_settings": _t(lang, "web.control.export.invoice_settings"),
        "web_control_export_actions": _t(lang, "web.control.export.actions"),
        "web_control_export_preview": _t(lang, "web.control.export.preview"),
        "web_control_export_no_preview": _t(lang, "web.control.export.no_preview"),
        "web_control_export_today": _t(lang, "web.control.export.today"),
        "web_control_export_this_week": _t(lang, "web.control.export.this_week"),
        "web_control_export_this_month": _t(lang, "web.control.export.this_month"),
        "web_control_export_this_year": _t(lang, "web.control.export.this_year"),
        "web_control_export_all": _t(lang, "web.control.export.all"),
        "web_control_export_bundle_hours": _t(lang, "web.control.export.bundle_hours"),
        "web_control_jobs_meta": _t(lang, "web.control.jobs.meta"),
        "t_error_label": _t(lang, "web.error_label"),
        "t_starting": _t(lang, "web.starting"),
        "t_job_started": _t(lang, "web.job_started"),
    }
    rendered = _render_template(tpl, values)
    return _inject_version_badge(rendered).encode("utf-8")


def _render_plots_html(state: "AppState") -> bytes:
    """Render the Plotly plots page HTML."""
    from shelly_analyzer.web.app_context import AppState  # noqa: F811
    import html as html_mod

    tpl = _PLOTS_TEMPLATE
    lang = state.lang
    web_i18n = get_lang_map(lang)

    devs = state.devices_meta
    parts = []
    for d in devs:
        try:
            k = html_mod.escape(str(d.get("key", "") or ""))
            n = html_mod.escape(str(d.get("name", "") or k))
        except Exception:
            continue
        if not k:
            continue
        parts.append(f'<label class="devchip"><input type="checkbox" value="{k}"/><span>{n}</span></label>')
    devices_html = "\n".join(parts)
    if not devices_html.strip():
        devices_html = f"<div class='hint'>{_t(lang, 'web.plots.no_devices')}</div>"

    values: Dict[str, str] = {
        "lang": lang,
        "plots_title": _t(lang, "web.plots.title"),
        "web_nav_live": _t(lang, "web.nav.live"),
        "web_nav_control": _t(lang, "web.nav.control"),
        "web_btn_theme": _t(lang, "web.btn.theme"),
        "lbl_view": _t(lang, "web.plots.view"),
        "lbl_metric": _t(lang, "web.plots.metric"),
        "lbl_series": _t(lang, "web.plots.series"),
        "lbl_smooth": _t(lang, "web.plots.filter.smooth"),
        "lbl_deadband": _t(lang, "web.plots.filter.deadband"),
        "lbl_signhold": _t(lang, "web.plots.filter.signhold"),
        "lbl_mode": _t(lang, "web.plots.kwh_mode"),
        "lbl_range": _t(lang, "web.plots.range"),
        "lbl_from": _t(lang, "common.from"),
        "lbl_to": _t(lang, "common.to"),
        "lbl_devices": _t(lang, "web.plots.devices"),
        "hint_max2": _t(lang, "web.plots.max2"),
        "btn_apply": _t(lang, "btn.apply"),
        "devices_html": devices_html,
        "i18n_json": json.dumps(web_i18n, ensure_ascii=False),
        "devices_json": json.dumps(devs, ensure_ascii=False),
    }
    rendered = _render_template_tokens(tpl, values)
    # No version badge in plots — it's embedded as an iframe in the dashboard
    # which already has its own badge, resulting in a duplicate.
    return rendered.encode("utf-8")


def create_app(config_path: Optional[str] = None) -> Flask:
    """Flask application factory."""
    from shelly_analyzer.web.app_context import AppState

    # Capture logs into ring buffer so the web Sync/Log tab can display them
    _install_web_log_handler()

    # Determine config path
    if config_path is None:
        config_path = "config.json"
    cfg_path = Path(config_path).resolve()
    out_dir = cfg_path.parent

    # Load config
    cfg = load_config(str(cfg_path))

    # Initialize storage
    storage = Storage(base_dir=out_dir / "data")

    # Create app state
    state = AppState(cfg=cfg, storage=storage, out_dir=out_dir)
    state._cfg_path = cfg_path  # type: ignore[attr-defined]

    # Create Flask app
    app = Flask(
        __name__,
        template_folder=str(Path(__file__).parent / "templates"),
        static_folder=str(Path(__file__).parent / "static"),
    )
    app.config["SECRET_KEY"] = "shelly-energy-analyzer"

    # Store state in app extensions
    app.extensions["state"] = state

    # ── Authentication ─────────────────────────────────────────────────
    token = str(getattr(cfg.ui, "live_web_token", "") or "").strip()
    if token:
        from functools import wraps
        from flask import request as flask_request, abort, session, redirect, url_for

        @app.before_request
        def _check_auth():
            # Public endpoints (no auth required)
            public = {"/api/widget", "/widget.js", "/metrics", "/static/plotly.min.js",
                      "/w", "/widget-manifest.json", "/widget-icon.svg"}
            path = flask_request.path
            if path in public or path.startswith("/static/"):
                return None
            # Login page
            if path == "/login":
                return None
            # Check token in query param, header, or session
            t = (flask_request.args.get("t")
                 or flask_request.headers.get("X-API-Key")
                 or session.get("auth_token"))
            if t == token:
                session["auth_token"] = token
                return None
            # Not authenticated — show login for pages, 401 for API
            if path.startswith("/api/"):
                abort(401)
            return redirect(f"/login?next={path}")

        @app.route("/login", methods=["GET", "POST"])
        def login():
            from flask import request as req
            if req.method == "POST":
                t = req.form.get("token", "").strip()
                if t == token:
                    session["auth_token"] = token
                    next_url = req.args.get("next", "/")
                    return redirect(next_url)
                return _login_html(error="Invalid token"), 403
            return _login_html()

        def _login_html(error: str = "") -> str:
            err_div = f'<div style="color:#ff6b6b;margin:8px 0">{error}</div>' if error else ""
            return f"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Login – Shelly Energy Analyzer</title>
<style>body{{font-family:-apple-system,system-ui,sans-serif;background:#0b0f14;color:#e8eef6;display:flex;justify-content:center;align-items:center;min-height:100vh;margin:0}}
.card{{background:#121821;border:1px solid rgba(255,255,255,.08);border-radius:14px;padding:24px;max-width:400px;width:90%}}
input{{width:100%;box-sizing:border-box;padding:10px;border-radius:10px;border:1px solid rgba(255,255,255,.15);background:rgba(255,255,255,.04);color:#e8eef6;font-size:16px;margin:8px 0}}
button{{width:100%;padding:10px;border-radius:12px;border:none;background:#6aa7ff;color:#fff;font-size:16px;cursor:pointer;margin-top:8px}}</style></head>
<body><div class="card"><h2 style="margin:0 0 12px">Shelly Energy Analyzer</h2>
{err_div}<form method="post"><input name="token" type="password" placeholder="Token" autofocus>
<button type="submit">Login</button></form></div></body></html>"""

        logger.info("Authentication enabled (token required)")
    else:
        logger.info("Authentication disabled (no token configured)")

    # Pre-render HTML pages (same approach as webdash.py — render once at startup)
    state._dashboard_html = _render_dashboard_html(state)
    state._dashboard_html_gz = gzip.compress(state._dashboard_html, compresslevel=6)
    state._control_html = _render_control_html(state)
    state._control_html_gz = gzip.compress(state._control_html, compresslevel=6)
    state._plots_html = _render_plots_html(state)
    state._plots_html_gz = gzip.compress(state._plots_html, compresslevel=6)

    # Register blueprints
    from shelly_analyzer.web.blueprints.dashboard import bp as dashboard_bp
    from shelly_analyzer.web.blueprints.api_state import bp as api_state_bp
    from shelly_analyzer.web.blueprints.api_data import bp as api_data_bp
    from shelly_analyzer.web.blueprints.api_actions import bp as api_actions_bp
    from shelly_analyzer.web.blueprints.static_assets import bp as static_assets_bp
    from shelly_analyzer.web.blueprints.metrics import bp as metrics_bp
    from shelly_analyzer.web.blueprints.settings import bp as settings_bp
    from shelly_analyzer.web.blueprints.devices import bp as devices_bp
    from shelly_analyzer.web.blueprints.sync import bp as sync_bp
    from shelly_analyzer.web.blueprints.health import bp as health_bp
    from shelly_analyzer.web.blueprints.alerts import bp as alerts_bp
    from shelly_analyzer.web.blueprints.tenants import bp as tenants_bp
    from shelly_analyzer.web.blueprints.updates import bp as updates_bp
    from shelly_analyzer.web.blueprints.ssl import bp as ssl_bp

    app.register_blueprint(dashboard_bp)
    app.register_blueprint(api_state_bp)
    app.register_blueprint(api_data_bp)
    app.register_blueprint(api_actions_bp)
    app.register_blueprint(static_assets_bp)
    app.register_blueprint(metrics_bp)
    app.register_blueprint(settings_bp)
    app.register_blueprint(devices_bp)
    app.register_blueprint(sync_bp)
    app.register_blueprint(health_bp)
    app.register_blueprint(alerts_bp)
    app.register_blueprint(tenants_bp)
    app.register_blueprint(updates_bp)
    app.register_blueprint(ssl_bp)

    logger.info("Flask app created, %d devices configured", len(cfg.devices))
    return app
