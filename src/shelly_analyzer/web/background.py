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

        # NILM (non-intrusive load monitoring) transition learners per device
        self._nilm_learners: Dict[str, Any] = {}
        self._nilm_last_cluster_ts: float = 0.0
        self._nilm_cluster_interval: float = 300.0  # re-cluster every 5 min

    def start_all(self) -> None:
        """Start all enabled background services."""
        self._stop_event.clear()
        self._init_nilm_learners()
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
                # Feed NILM learner
                self._observe_nilm(sample.device_key, ts_i, power_total)

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

    def _load_today_baseline(self, device_key: str, day_start_ts: int) -> tuple:
        """Read already-imported kWh for *today* from DB.

        Returns (baseline_kwh, baseline_last_ts) where baseline_last_ts is the
        end of the latest hour_ts we already have, so live accumulation only
        starts after it (no double counting after a sync).
        """
        try:
            db = getattr(self.storage, "db", None)
            if db is None:
                return 0.0, 0
            df = db.query_hourly(device_key, start_ts=int(day_start_ts), end_ts=int(time.time()))
            if df is None or df.empty:
                return 0.0, int(day_start_ts)
            total = float(df["kwh"].fillna(0).sum())
            # End of latest completed hour we have data for
            last_hour = int(df["hour_ts"].max())
            last_end = last_hour + 3600
            return total, last_end
        except Exception as e:
            logger.debug("Baseline lookup failed for %s: %s", device_key, e)
            return 0.0, int(day_start_ts)

    def _accumulate_today_kwh(self, device_key: str, ts: int, power_w: float) -> float:
        """Trapezoid-integrate power (W) samples into kWh for the current local day,
        starting from the DB baseline (already-synced hours of today).

        Resets automatically at local midnight. Returns total kWh for today.
        """
        from datetime import datetime

        ts_dt = datetime.fromtimestamp(int(ts))
        day = ts_dt.date()
        day_start = int(datetime(day.year, day.month, day.day).timestamp())

        with self._today_kwh_lock:
            st = self._today_state.get(device_key)
            if not st or st.get("date") != day:
                base_kwh, base_last_ts = self._load_today_baseline(device_key, day_start)
                st = {
                    "date": day,
                    "base_kwh": float(base_kwh),
                    "base_last_ts": int(base_last_ts),
                    "live_kwh": 0.0,
                    "last_ts": None,
                    "last_p": None,
                    "baseline_refreshed_at": int(time.time()),
                }
                self._today_state[device_key] = st

            # Periodically refresh baseline from DB (after each auto-sync the DB has
            # grown; we must pick that up and reset live accumulator to avoid double counting).
            if int(time.time()) - int(st.get("baseline_refreshed_at", 0)) > 600:
                base_kwh, base_last_ts = self._load_today_baseline(device_key, day_start)
                old_last = int(st.get("base_last_ts", 0))
                if int(base_last_ts) > old_last:
                    # DB advanced beyond where we were accumulating → drop overlapping live portion
                    st["base_kwh"] = float(base_kwh)
                    st["base_last_ts"] = int(base_last_ts)
                    st["live_kwh"] = 0.0
                    st["last_ts"] = None
                    st["last_p"] = None
                st["baseline_refreshed_at"] = int(time.time())

            base_last_ts = int(st.get("base_last_ts", day_start))

            # Only accumulate samples after the DB baseline end
            if int(ts) <= base_last_ts:
                st["last_ts"] = int(ts)
                st["last_p"] = float(power_w)
            else:
                last_ts = st.get("last_ts")
                last_p = st.get("last_p")
                # If previous sample was within the baseline window, restart from here
                if last_ts is None or last_p is None or int(last_ts) <= base_last_ts:
                    st["last_ts"] = int(ts)
                    st["last_p"] = float(power_w)
                else:
                    dt = float(int(ts) - int(last_ts))
                    if 0 < dt <= 300:
                        wh = (float(last_p) + float(power_w)) / 2.0 * (dt / 3600.0)
                        st["live_kwh"] = float(st.get("live_kwh", 0.0) or 0.0) + (wh / 1000.0)
                    st["last_ts"] = int(ts)
                    st["last_p"] = float(power_w)

            return float(st.get("base_kwh", 0.0) or 0.0) + float(st.get("live_kwh", 0.0) or 0.0)

    # ── NILM ───────────────────────────────────────────────────────────

    def _init_nilm_learners(self) -> None:
        """Create one TransitionLearner per 3-phase EM device; load persisted clusters."""
        try:
            from shelly_analyzer.services.appliance_detector import TransitionLearner
        except Exception as e:
            logger.debug("NILM disabled: %s", e)
            return
        runtime_dir = self.out_dir / "data" / "runtime" / "nilm"
        try:
            runtime_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        self._nilm_learners = {}
        for d in self.cfg.devices:
            # Only track 3-phase EM devices (switches have no meaningful NILM)
            if int(getattr(d, "phases", 3) or 3) < 3:
                continue
            if str(getattr(d, "kind", "em")) == "switch":
                continue
            try:
                persist = runtime_dir / f"{d.key}.json"
                self._nilm_learners[d.key] = TransitionLearner(
                    min_step_w=50.0,
                    max_clusters=20,
                    persist_path=persist,
                )
            except Exception as e:
                logger.debug("NILM learner init failed for %s: %s", d.key, e)
        # Seed store with persisted clusters immediately (so UI doesn't say "waiting")
        self._push_nilm_to_store()
        logger.info("NILM learners initialized for %d devices", len(self._nilm_learners))

    def _observe_nilm(self, device_key: str, ts: int, power_w: float) -> None:
        learner = self._nilm_learners.get(device_key)
        if learner is None:
            return
        try:
            learner.observe(device_key, float(ts), float(power_w))
        except Exception:
            pass
        # Periodically re-cluster and push to store
        now = time.time()
        if (now - self._nilm_last_cluster_ts) >= self._nilm_cluster_interval:
            self._nilm_last_cluster_ts = now
            self._push_nilm_to_store()

    def _push_nilm_to_store(self) -> None:
        try:
            all_clusters = []
            total_trans = 0
            for dk, lrn in self._nilm_learners.items():
                try:
                    cls = lrn.cluster()
                    total_trans += int(lrn.get_transition_count())
                    for c in cls:
                        all_clusters.append({
                            "matched_appliance": getattr(c, "matched_appliance", "") or "",
                            "count": int(getattr(c, "count", 0)),
                            "centroid_w": float(getattr(c, "centroid_w", 0.0)),
                            "icon": getattr(c, "icon", "") or "🔌",
                            "label": getattr(c, "label", "") or "",
                            "device_key": dk,
                        })
                except Exception:
                    continue
            store = self.live_store
            if store is not None:
                store._nilm_clusters = all_clusters  # type: ignore[attr-defined]
                store._nilm_transition_count = total_trans  # type: ignore[attr-defined]
        except Exception as e:
            logger.debug("NILM push failed: %s", e)

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
