"""Background service manager for the Flask web app.

Starts and manages all background services (live polling, scheduler, MQTT,
InfluxDB export, auto-sync) alongside the Flask server.
"""
from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from shelly_analyzer.io.config import AppConfig
from shelly_analyzer.io.storage import Storage
from shelly_analyzer.services.webdash import LivePoint, LiveStateStore

logger = logging.getLogger(__name__)


class BackgroundServiceManager:
    """Manages all background services that run alongside Flask."""

    def __init__(
        self,
        cfg: AppConfig,
        storage: Storage,
        live_store: LiveStateStore,
        *,
        out_dir: Path,
        on_action: Optional[Callable] = None,
    ) -> None:
        self.cfg = cfg
        self.storage = storage
        self.live_store = live_store
        self.out_dir = out_dir
        self.on_action = on_action

        self._live_poller = None
        self._scheduler = None
        self._mqtt_publisher = None
        self._influxdb_exporter = None
        self._feed_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._autosync_thread: Optional[threading.Thread] = None

        # Today kWh tracking per device (trapezoid integration of power samples)
        # state[device_key] = {"date": date, "kwh": float, "last_ts": int, "last_p": float}
        self._today_state: Dict[str, Dict[str, Any]] = {}
        self._today_kwh_lock = threading.Lock()

    def start_all(self) -> None:
        """Start all enabled background services."""
        self._stop_event.clear()
        self._start_live_poller()
        self._start_scheduler()
        self._start_mqtt()
        self._start_influxdb()
        self._start_autosync()
        self._write_runtime_devices_meta()
        logger.info("All background services started")

    def stop_all(self) -> None:
        """Gracefully stop all background services."""
        self._stop_event.set()
        if self._live_poller:
            try:
                self._live_poller.stop()
            except Exception:
                pass
        if self._scheduler:
            try:
                self._scheduler.stop()
            except Exception:
                pass
        if self._mqtt_publisher:
            try:
                self._mqtt_publisher.stop()
            except Exception:
                pass
        if self._influxdb_exporter:
            try:
                self._influxdb_exporter.stop()
            except Exception:
                pass
        logger.info("All background services stopped")

    def reload(self, cfg: AppConfig) -> None:
        """Hot-reload configuration."""
        self.cfg = cfg
        # Restart services that depend on config
        self.stop_all()
        self.start_all()

    # ── Live Polling ───────────────────────────────────────────────────

    def _start_live_poller(self) -> None:
        """Start multi-device live polling and feed into LiveStateStore."""
        if not self.cfg.devices:
            logger.info("No devices configured, skipping live poller")
            return

        try:
            from shelly_analyzer.services.live import MultiLivePoller

            self._live_poller = MultiLivePoller(
                devices=list(self.cfg.devices),
                download_cfg=self.cfg.download,
                poll_seconds=float(self.cfg.ui.live_poll_seconds),
            )
            self._live_poller.start()

            # Start feed thread: reads from poller queue and feeds LiveStateStore
            self._feed_thread = threading.Thread(target=self._feed_loop, daemon=True)
            self._feed_thread.start()
            logger.info("Live poller started for %d devices", len(self.cfg.devices))
        except Exception as e:
            logger.error("Failed to start live poller: %s", e)

    def _feed_loop(self) -> None:
        """Drain live samples from poller queue into LiveStateStore."""
        import queue
        from datetime import datetime

        unit_price = float(self.cfg.pricing.unit_price_gross())

        while not self._stop_event.is_set():
            try:
                sample = self._live_poller.samples.get(timeout=1.0)
            except queue.Empty:
                continue
            except Exception:
                break

            try:
                p = sample.power_w or {}
                v = sample.voltage_v or {}
                c = sample.current_a or {}
                r = sample.reactive_var or {}
                cp = sample.cosphi or {}
                f = sample.freq_hz or {}
                raw = sample.raw or {}

                # kWh-today: trapezoidal integration of power_w.total since start
                # of local day. Reset accumulator at midnight.
                ts_i = int(sample.ts or time.time())
                power_total = float(p.get("total", 0) or 0)
                kwh_today = self._accumulate_today_kwh(sample.device_key, ts_i, power_total)
                cost_today = kwh_today * unit_price

                point = LivePoint(
                    ts=int(sample.ts or time.time()),
                    power_total_w=float(p.get("total", 0) or 0),
                    va=float(v.get("a", 0) or 0),
                    vb=float(v.get("b", 0) or 0),
                    vc=float(v.get("c", 0) or 0),
                    ia=float(c.get("a", 0) or 0),
                    ib=float(c.get("b", 0) or 0),
                    ic=float(c.get("c", 0) or 0),
                    pa=float(p.get("a", 0) or 0),
                    pb=float(p.get("b", 0) or 0),
                    pc=float(p.get("c", 0) or 0),
                    q_total_var=float(r.get("total", 0) or 0),
                    qa=float(r.get("a", 0) or 0),
                    qb=float(r.get("b", 0) or 0),
                    qc=float(r.get("c", 0) or 0),
                    cosphi_total=float(cp.get("total", 0) or 0),
                    pfa=float(cp.get("a", 0) or 0),
                    pfb=float(cp.get("b", 0) or 0),
                    pfc=float(cp.get("c", 0) or 0),
                    kwh_today=kwh_today,
                    cost_today=cost_today,
                    freq_hz=float(f.get("total", 50) or 50),
                    i_n=float(raw.get("i_n", 0) or 0),
                    raw=raw,
                )
                self.live_store.update(sample.device_key, point)
            except Exception as e:
                logger.debug("Feed loop error: %s", e)

    def _accumulate_today_kwh(self, device_key: str, ts: int, power_w: float) -> float:
        """Trapezoid-integrate power (W) samples into kWh for the current local day.

        Resets automatically at local midnight. Returns current kWh total for today.
        """
        from datetime import datetime

        day = datetime.fromtimestamp(int(ts)).date()
        with self._today_kwh_lock:
            st = self._today_state.get(device_key)
            if not st or st.get("date") != day:
                st = {"date": day, "kwh": 0.0, "last_ts": None, "last_p": None}
                self._today_state[device_key] = st

            last_ts = st.get("last_ts")
            last_p = st.get("last_p")
            if last_ts is not None and last_p is not None:
                dt = float(int(ts) - int(last_ts))
                # Ignore gaps larger than 5 minutes (device was offline)
                if 0 < dt <= 300:
                    wh = (float(last_p) + float(power_w)) / 2.0 * (dt / 3600.0)
                    st["kwh"] = float(st.get("kwh", 0.0) or 0.0) + (wh / 1000.0)

            st["last_ts"] = int(ts)
            st["last_p"] = float(power_w)
            return float(st.get("kwh", 0.0) or 0.0)

    # ── Scheduler ──────────────────────────────────────────────────────

    def _start_scheduler(self) -> None:
        """Start the local device scheduler."""
        try:
            from shelly_analyzer.services.scheduler import LocalScheduler
            from shelly_analyzer.io.http import ShellyHttp, HttpConfig

            http_client = ShellyHttp(HttpConfig(
                timeout_seconds=float(self.cfg.download.timeout_seconds),
                retries=int(self.cfg.download.retries),
                backoff_base_seconds=float(self.cfg.download.backoff_base_seconds),
            ))
            self._scheduler = LocalScheduler(
                get_config=lambda: self.cfg,
                get_http=lambda: http_client,
            )
            self._scheduler.start()
        except Exception as e:
            logger.debug("Scheduler not started: %s", e)

    # ── MQTT ───────────────────────────────────────────────────────────

    def _start_mqtt(self) -> None:
        """Start MQTT Home Assistant publisher if enabled."""
        mqtt_cfg = getattr(self.cfg, "mqtt", None)
        if not mqtt_cfg or not getattr(mqtt_cfg, "enabled", False):
            return
        try:
            from shelly_analyzer.services.mqtt_ha import MqttPublisher
            self._mqtt_publisher = MqttPublisher(config=mqtt_cfg)
            self._mqtt_publisher.start()
        except Exception as e:
            logger.debug("MQTT not started: %s", e)

    # ── InfluxDB ───────────────────────────────────────────────────────

    def _start_influxdb(self) -> None:
        """Start InfluxDB exporter if enabled."""
        influx_cfg = getattr(self.cfg, "influxdb", None)
        if not influx_cfg or not getattr(influx_cfg, "enabled", False):
            return
        try:
            from shelly_analyzer.services.influxdb_export import InfluxDBExporter
            # Attach devices to storage for InfluxDBExporter
            try:
                self.storage.devices = list(self.cfg.devices)
            except Exception:
                pass
            self._influxdb_exporter = InfluxDBExporter(cfg=influx_cfg, storage=self.storage)
            self._influxdb_exporter.start()
        except Exception as e:
            logger.debug("InfluxDB exporter not started: %s", e)

    # ── Auto-sync ──────────────────────────────────────────────────────

    def _start_autosync(self) -> None:
        """Start periodic auto-sync if enabled."""
        if not getattr(self.cfg.ui, "autosync_enabled", False):
            return

        interval_m = int(getattr(self.cfg.ui, "autosync_interval_minutes", 0) or 0)
        interval_h = int(getattr(self.cfg.ui, "autosync_interval_hours", 12) or 12)
        if interval_m > 0:
            interval_s = max(60, interval_m * 60)  # minimum 1 minute
        else:
            interval_s = max(300, interval_h * 3600)  # minimum 5 minutes

        def _sync_loop():
            while not self._stop_event.is_set():
                self._stop_event.wait(interval_s)
                if self._stop_event.is_set():
                    break
                try:
                    from shelly_analyzer.services.sync import sync_all
                    mode = str(getattr(self.cfg.ui, "autosync_mode", "incremental") or "incremental")
                    now = int(time.time())
                    range_override = None
                    if mode == "day":
                        range_override = (max(0, now - 86400), now)
                    elif mode == "week":
                        range_override = (max(0, now - 7 * 86400), now)
                    elif mode == "month":
                        range_override = (max(0, now - 30 * 86400), now)
                    sync_all(self.cfg, self.storage, range_override=range_override, fallback_last_days=7)
                    logger.info("Auto-sync completed")
                except Exception as e:
                    logger.warning("Auto-sync failed: %s", e)

        self._autosync_thread = threading.Thread(target=_sync_loop, daemon=True)
        self._autosync_thread.start()
        logger.info("Auto-sync enabled (interval: %d s)", interval_s)

    # ── Runtime metadata ───────────────────────────────────────────────

    def _write_runtime_devices_meta(self) -> None:
        """Write device metadata to a runtime file for recovery."""
        try:
            runtime_dir = self.out_dir / "data" / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            meta = [
                {"key": d.key, "name": d.name, "kind": str(getattr(d, "kind", "") or ""), "phases": int(getattr(d, "phases", 3))}
                for d in self.cfg.devices
            ]
            (runtime_dir / "devices_meta.json").write_text(
                json.dumps({"devices_meta": meta}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as e:
            logger.debug("Failed to write devices_meta.json: %s", e)


# Need json import at module level
import json
