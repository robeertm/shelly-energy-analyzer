"""Prometheus /metrics endpoint."""
from __future__ import annotations

from flask import Blueprint, Response, current_app

bp = Blueprint("metrics", __name__)


@bp.route("/metrics")
def prometheus_metrics():
    state = current_app.extensions["state"]
    try:
        from shelly_analyzer.services.prometheus_export import generate_metrics
        body = generate_metrics(
            state.live_store.snapshot(),
            state.cfg.devices if state.cfg else [],
            state.cfg,
        ).encode("utf-8")
    except Exception as e:
        body = f"# error: {e}\n".encode("utf-8")
    return Response(body, content_type="text/plain; charset=utf-8",
                    headers={"Cache-Control": "no-store"})
