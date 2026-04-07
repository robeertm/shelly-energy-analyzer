"""Shared application state for the Flask web app."""
from __future__ import annotations

import inspect
import json
import logging
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from shelly_analyzer.i18n import get_lang_map, normalize_lang
from shelly_analyzer.i18n import t as _t
from shelly_analyzer.io.config import AppConfig, load_config, save_config
from shelly_analyzer.io.storage import Storage
from shelly_analyzer.services.webdash import (
    LivePoint,
    LiveStateStore,
    _plotly_min_js_bytes,
    _SCRIPTABLE_WIDGET_JS,
)

logger = logging.getLogger(__name__)


class AppState:
    """Central application state, replacing the LiveWebDashboard + CoreMixin state.

    Holds config, storage, live data store, job queue, and device metadata.
    Accessed from Flask routes via ``flask.current_app.extensions['state']``.
    """

    def __init__(
        self,
        cfg: AppConfig,
        storage: Storage,
        *,
        out_dir: Optional[Path] = None,
    ) -> None:
        self.cfg = cfg
        self.storage = storage
        self.out_dir = Path(out_dir) if out_dir else Path.cwd()
        self.lang = normalize_lang(cfg.ui.language)

        # Live data store
        poll_s = max(0.2, float(cfg.ui.live_poll_seconds))
        retention_m = int(getattr(cfg.ui, "live_retention_minutes", 120))
        max_points = int((retention_m * 60.0) / poll_s) + 50
        self.live_store = LiveStateStore(max_points=max_points)

        # Device metadata
        self.devices_meta: List[Dict[str, Any]] = [
            {
                "key": d.key,
                "name": d.name,
                "kind": str(getattr(d, "kind", "") or ""),
                "phases": int(getattr(d, "phases", 3)),
            }
            for d in cfg.devices
        ]

        # Window settings
        self.window_minutes = int(cfg.ui.live_window_minutes)
        self.refresh_seconds = float(cfg.ui.live_web_refresh_seconds)
        self.available_windows = [5, 10, 15, 30, 60, 120]
        if self.window_minutes not in self.available_windows:
            self.available_windows.append(self.window_minutes)
            self.available_windows = sorted(set(self.available_windows))

        # Widget settings
        self.widget_domain = str(getattr(cfg.ui, "widget_domain", "") or "")
        self.widget_devices = str(getattr(cfg.ui, "widget_devices", "") or "")

        # Job store (for async export operations)
        self._jobs_lock = threading.Lock()
        self._jobs_by_id: Dict[int, Dict[str, Any]] = {}
        self._jobs_order: List[int] = []
        self._job_seq = 0

        # Action handler (set by background service manager)
        self.on_action: Optional[Callable] = None

        # Plotly JS bytes (cached)
        self._plotly_js: Optional[bytes] = None

        # Port (set after server starts)
        self.port = int(cfg.ui.live_web_port)
        self._is_https = False

    # ── Config hot-reload ──────────────────────────────────────────────

    def reload_config(self, cfg: AppConfig) -> None:
        """Hot-reload configuration."""
        import gzip
        old_lang = getattr(self, "lang", None)
        self.cfg = cfg
        self.lang = normalize_lang(cfg.ui.language)
        self.devices_meta = [
            {
                "key": d.key,
                "name": d.name,
                "kind": str(getattr(d, "kind", "") or ""),
                "phases": int(getattr(d, "phases", 3)),
            }
            for d in cfg.devices
        ]
        # Language or device list changed: re-render cached HTML templates
        # (dashboard/plots/control pages embed translations + device chips at startup).
        # Propagate config to background service manager (alert rules, Telegram, etc.)
        bg = getattr(self, "_bg", None)
        if bg is not None:
            bg.cfg = cfg

        try:
            from shelly_analyzer.web import _render_dashboard_html, _render_plots_html, _render_control_html
            self._dashboard_html = _render_dashboard_html(self)
            self._dashboard_html_gz = gzip.compress(self._dashboard_html, compresslevel=6)
            self._plots_html = _render_plots_html(self)
            self._plots_html_gz = gzip.compress(self._plots_html, compresslevel=6)
            self._control_html = _render_control_html(self)
            self._control_html_gz = gzip.compress(self._control_html, compresslevel=6)
            if old_lang != self.lang:
                logger.info("Language switched: %s → %s – HTML templates re-rendered", old_lang, self.lang)
        except Exception as e:
            logger.warning("HTML re-render after config reload failed: %s", e)

    # ── Job management (same as LiveWebDashboard) ──────────────────────

    def get_jobs(self) -> Dict[str, Any]:
        return {"jobs": self.list_jobs()}

    def list_jobs(self) -> List[Dict[str, Any]]:
        with self._jobs_lock:
            return [
                self._jobs_by_id[jid]
                for jid in self._jobs_order
                if jid in self._jobs_by_id
            ]

    def get_job(self, job_id: int) -> Dict[str, Any]:
        with self._jobs_lock:
            j = self._jobs_by_id.get(int(job_id))
            return {"job": j} if j else {"job": None}

    def _upsert_job(self, job: Dict[str, Any]) -> None:
        jid = int(job.get("id") or 0)
        if jid <= 0:
            return
        with self._jobs_lock:
            if jid not in self._jobs_by_id:
                self._jobs_order.append(jid)
            self._jobs_by_id[jid] = job
            if len(self._jobs_order) > 200:
                drop = self._jobs_order[: len(self._jobs_order) - 200]
                self._jobs_order = self._jobs_order[len(self._jobs_order) - 200 :]
                for d in drop:
                    self._jobs_by_id.pop(d, None)

    def update_job(self, job_id: int, **fields: Any) -> None:
        with self._jobs_lock:
            j = self._jobs_by_id.get(int(job_id))
            if not j:
                return
            self._jobs_by_id[int(job_id)] = {**j, **fields}

    def update_progress(
        self,
        job_id: int,
        device_key: str,
        done: int,
        total: int,
        message: str = "",
    ) -> None:
        done = int(max(0, done))
        total = int(max(1, total))
        pct = int(min(100, max(0, (done * 100) // total)))
        self.update_job(
            job_id,
            progress_overall=pct,
            progress_device=device_key,
            progress_done=done,
            progress_total=total,
            progress_message=message,
        )

    def submit_action(
        self, action: str, params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Run an action asynchronously and return job metadata."""
        if not self.on_action:
            raise RuntimeError("Remote actions not available")
        with self._jobs_lock:
            self._job_seq += 1
            job_id = self._job_seq
        job: Dict[str, Any] = {
            "id": job_id,
            "action": action,
            "params": params,
            "status": "running",
            "started_at": int(time.time()),
            "progress_overall": 0,
        }
        self._upsert_job(job)

        def progress_cb(
            device_key: str, done: int, total: int, message: str = ""
        ) -> None:
            self.update_progress(
                job_id, device_key=device_key, done=done, total=total, message=message
            )

        def runner() -> None:
            try:
                res: Dict[str, Any]
                if self.on_action is None:
                    raise RuntimeError("Remote actions not available")
                try:
                    sig = inspect.signature(self.on_action)
                    if len(sig.parameters) >= 3:
                        res = self.on_action(action, params, progress_cb)
                    else:
                        res = self.on_action(action, params)
                except TypeError:
                    res = self.on_action(action, params)
                self.update_job(
                    job_id,
                    status="done",
                    result=res,
                    finished_at=int(time.time()),
                    progress_overall=100,
                )
            except Exception as e:
                self.update_job(
                    job_id,
                    status="error",
                    error=str(e),
                    finished_at=int(time.time()),
                )

        t = threading.Thread(target=runner, daemon=True)
        t.start()
        return {"ok": True, "job_id": job_id}

    # ── Config endpoint ────────────────────────────────────────────────

    def get_config_response(self) -> Dict[str, Any]:
        """Data for /api/config endpoint."""
        cfg = self.cfg
        return {
            "window_minutes": int(self.window_minutes),
            "refresh_seconds": float(self.refresh_seconds),
            "available_windows": list(self.available_windows),
            "analyzer_running": True,  # Flask IS the analyzer
            "analyzer_heartbeat_ts": int(time.time()),
            "devices_meta": self.devices_meta,
            "lang": self.lang,
            "features": {
                "solar": bool(getattr(cfg.solar, "enabled", False)),
                "weather": bool(getattr(cfg.weather, "enabled", False)),
                "co2": bool(getattr(cfg.co2, "enabled", False)),
                "anomalies": bool(getattr(cfg.anomaly, "enabled", False)),
                "forecast": bool(getattr(cfg.forecast, "enabled", False)),
                "ev": bool(getattr(cfg.ev_charging, "enabled", False)),
                "ev_log": bool(getattr(cfg.ev_charging, "enabled", False)),
                "smart_sched": bool(getattr(cfg.smart_schedule, "enabled", False)),
                "tariff": bool(getattr(cfg.tariff_compare, "enabled", False)),
                "battery": bool(getattr(cfg.battery, "enabled", False)),
                "advisor": bool(getattr(cfg.advisor, "enabled", False)),
                "goals": bool(getattr(cfg.gamification, "enabled", False)),
                "tenants": bool(getattr(cfg.tenant, "enabled", False)),
            },
        }

    def set_window_minutes(self, minutes: int) -> int:
        minutes = int(minutes)
        if minutes <= 0:
            raise ValueError("minutes must be > 0")
        if minutes not in self.available_windows:
            self.available_windows.append(minutes)
            self.available_windows = sorted(set(self.available_windows))
        self.window_minutes = minutes
        approx_points = int(minutes * 60 * 2) + 100
        try:
            self.live_store.set_max_points(
                max(self.live_store.max_points, approx_points)
            )
        except Exception:
            pass
        return minutes

    # ── File serving ───────────────────────────────────────────────────

    def read_file_bytes(self, rel_path: str) -> Tuple[bytes, str]:
        """Read a file below out_dir/exports and return (bytes, content_type)."""
        rel = str(rel_path).lstrip("/")
        root = (self.out_dir / "exports").resolve()
        path = (root / rel).resolve()
        if root not in path.parents and path != root:
            raise FileNotFoundError(rel)
        if not path.exists() or not path.is_file():
            raise FileNotFoundError(rel)
        data = path.read_bytes()
        ext = path.suffix.lower()
        ctype = "application/octet-stream"
        if ext in {".png"}:
            ctype = "image/png"
        elif ext in {".jpg", ".jpeg"}:
            ctype = "image/jpeg"
        elif ext in {".pdf"}:
            ctype = "application/pdf"
        elif ext in {".zip"}:
            ctype = "application/zip"
        elif ext in {".json"}:
            ctype = "application/json; charset=utf-8"
        elif ext in {".txt", ".log"}:
            ctype = "text/plain; charset=utf-8"
        elif ext in {".csv"}:
            ctype = "text/csv; charset=utf-8"
        elif ext in {".xlsx"}:
            ctype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        return data, ctype

    # ── Widget ─────────────────────────────────────────────────────────

    def get_widget_script(self) -> str:
        script = _SCRIPTABLE_WIDGET_JS
        if self.widget_domain:
            default_addr = f"{self.widget_domain}:{self.port}"
            script = script.replace("192.168.1.50:8765", default_addr)
        return script

    def get_plotly_js(self) -> bytes:
        if self._plotly_js is None:
            self._plotly_js = _plotly_min_js_bytes()
        return self._plotly_js or b""
