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

        # Today kWh tracking per device (for live cost calculation)
        self._today_kwh: Dict[str, float] = {}
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
            from shelly_analyzer.services.live import MultiLivePoller, DemoMultiLivePoller

            demo_cfg = getattr(self.cfg, "demo", None)
            if demo_cfg and getattr(demo_cfg, "enabled", False):
                from shelly_analyzer.services.demo import default_demo_devices, ensure_demo_csv
                demo_devs = default_demo_devices(getattr(demo_cfg, "scenario", "household"))
                for dd in demo_devs:
                    ensure_demo_csv(self.storage, dd, seed=getattr(demo_cfg, "seed", 1234))
                self._live_poller = DemoMultiLivePoller(
                    devices=[(dd.key, dd.host) for dd in demo_devs],
                    poll_seconds=float(self.cfg.ui.live_poll_seconds),
                )
            else:
                self._live_poller = MultiLivePoller(
                    devices=[(d.key, d.host) for d in self.cfg.devices],
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

        unit_price = float(self.cfg.pricing.unit_price_gross())

        while not self._stop_event.is_set():
            try:
                sample = self._live_poller.queue.get(timeout=1.0)
            except queue.Empty:
                continue
            except Exception:
                break

            try:
                # Calculate today kWh/cost
                kwh_today = 0.0
                cost_today = 0.0
                with self._today_kwh_lock:
                    prev = self._today_kwh.get(sample.device_key, 0.0)
                    if sample.energy_wh is not None and sample.energy_wh > 0:
                        kwh_today = sample.energy_wh / 1000.0
                    self._today_kwh[sample.device_key] = kwh_today
                cost_today = kwh_today * unit_price

                point = LivePoint(
                    ts=int(sample.ts or time.time()),
                    power_total_w=float(sample.total_power or 0),
                    va=float(sample.voltage_a or 0),
                    vb=float(sample.voltage_b or 0),
                    vc=float(sample.voltage_c or 0),
                    ia=float(sample.current_a or 0),
                    ib=float(sample.current_b or 0),
                    ic=float(sample.current_c or 0),
                    pa=float(sample.power_a or 0),
                    pb=float(sample.power_b or 0),
                    pc=float(sample.power_c or 0),
                    q_total_var=float(getattr(sample, "q_total_var", 0) or 0),
                    qa=float(getattr(sample, "qa", 0) or 0),
                    qb=float(getattr(sample, "qb", 0) or 0),
                    qc=float(getattr(sample, "qc", 0) or 0),
                    cosphi_total=float(getattr(sample, "cosphi_total", 0) or 0),
                    pfa=float(getattr(sample, "pfa", 0) or 0),
                    pfb=float(getattr(sample, "pfb", 0) or 0),
                    pfc=float(getattr(sample, "pfc", 0) or 0),
                    kwh_today=kwh_today,
                    cost_today=cost_today,
                    freq_hz=float(getattr(sample, "freq_hz", 50) or 50),
                    i_n=float(getattr(sample, "i_n", 0) or 0),
                    raw=getattr(sample, "raw", {}),
                )
                self.live_store.update(sample.device_key, point)
            except Exception as e:
                logger.debug("Feed loop error: %s", e)

    # ── Scheduler ──────────────────────────────────────────────────────

    def _start_scheduler(self) -> None:
        """Start the local device scheduler."""
        try:
            from shelly_analyzer.services.scheduler import LocalScheduler

            def get_schedules():
                return getattr(self.cfg, "schedules", []) or []

            def get_devices():
                return self.cfg.devices

            self._scheduler = LocalScheduler(
                get_schedules=get_schedules,
                get_devices=get_devices,
            )
            self._scheduler.start()
            logger.info("Local scheduler started")
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

            self._mqtt_publisher = MqttPublisher(
                broker=str(getattr(mqtt_cfg, "broker", "127.0.0.1") or "127.0.0.1"),
                port=int(getattr(mqtt_cfg, "port", 1883) or 1883),
                username=str(getattr(mqtt_cfg, "username", "") or ""),
                password=str(getattr(mqtt_cfg, "password", "") or ""),
                topic_prefix=str(getattr(mqtt_cfg, "topic_prefix", "shelly_analyzer") or "shelly_analyzer"),
                ha_discovery=bool(getattr(mqtt_cfg, "ha_discovery", True)),
                live_store=self.live_store,
                devices_meta=[
                    {"key": d.key, "name": d.name}
                    for d in self.cfg.devices
                ],
            )
            self._mqtt_publisher.start()
            logger.info("MQTT publisher started")
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

            self._influxdb_exporter = InfluxDBExporter(
                url=str(getattr(influx_cfg, "url", "http://127.0.0.1:8086") or "http://127.0.0.1:8086"),
                version=str(getattr(influx_cfg, "version", "2") or "2"),
                token=str(getattr(influx_cfg, "token", "") or ""),
                org=str(getattr(influx_cfg, "org", "") or ""),
                bucket=str(getattr(influx_cfg, "bucket", "shelly") or "shelly"),
                interval_seconds=int(getattr(influx_cfg, "interval_seconds", 10) or 10),
                live_store=self.live_store,
                devices_meta=[{"key": d.key, "name": d.name} for d in self.cfg.devices],
            )
            self._influxdb_exporter.start()
            logger.info("InfluxDB exporter started")
        except Exception as e:
            logger.debug("InfluxDB exporter not started: %s", e)

    # ── Auto-sync ──────────────────────────────────────────────────────

    def _start_autosync(self) -> None:
        """Start periodic auto-sync if enabled."""
        if not getattr(self.cfg.ui, "autosync_enabled", False):
            return

        interval_h = int(getattr(self.cfg.ui, "autosync_interval_hours", 12) or 12)
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
        logger.info("Auto-sync enabled (interval: %d hours)", interval_h)

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
