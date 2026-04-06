"""Background service manager for the Flask web app.

Starts and manages all background services (live polling, scheduler, MQTT,
InfluxDB export, auto-sync, alert notifications) alongside the Flask server.
"""
from __future__ import annotations

import json
import logging
import math
import re
import threading
import time
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, date, timedelta
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
        self._co2_fetcher = None
        self._spot_fetcher = None
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

        # Alert rule evaluation state: {rule_id: {start_ts, triggered, last_trigger_ts}}
        self._alert_state: Dict[str, Dict[str, Any]] = {}
        # Summary scheduling thread
        self._summary_thread: Optional[threading.Thread] = None
        self._summary_last_daily: str = ""   # YYYY-MM-DD
        self._summary_last_monthly: str = ""  # YYYY-MM

    def start_all(self) -> None:
        """Start all enabled background services."""
        self._stop_event.clear()
        self._init_nilm_learners()
        self._start_live_poller()
        self._start_scheduler()
        self._start_mqtt()
        self._start_influxdb()
        self._start_autosync()
        self._start_co2_fetcher()
        self._start_spot_fetcher()
        self._start_summary_scheduler()
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
        if self._co2_fetcher:
            try:
                self._co2_fetcher.stop()
            except Exception:
                pass
        if self._spot_fetcher:
            try:
                self._spot_fetcher.stop()
            except Exception:
                pass
        # Flush NILM state so nothing learned is lost on shutdown/reload
        for lrn in self._nilm_learners.values():
            try:
                lrn.flush()
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

                # Evaluate alert rules against this sample
                try:
                    self._alerts_process_sample(sample)
                except Exception:
                    pass
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
        # Flush existing learners before replacing (e.g. after config reload)
        for lrn in list(self._nilm_learners.values()):
            try:
                lrn.flush()
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
            all_transitions: list = []
            total_trans = 0
            for dk, lrn in self._nilm_learners.items():
                try:
                    cls = lrn.cluster()
                    lrn.flush()
                    trans_count = int(lrn.get_transition_count())
                    total_trans += trans_count
                    for c in cls:
                        all_clusters.append({
                            "matched_appliance": getattr(c, "matched_appliance", "") or "",
                            "count": int(getattr(c, "count", 0)),
                            "centroid_w": float(getattr(c, "centroid_w", 0.0)),
                            "std_w": float(getattr(c, "std_w", 0.0)),
                            "typical_hour": int(getattr(c, "typical_hour", 12)),
                            "avg_duration_min": float(getattr(c, "avg_duration_min", 0.0)),
                            "icon": getattr(c, "icon", "") or "🔌",
                            "label": getattr(c, "label", "") or "",
                            "device_key": dk,
                        })
                    # Collect last 500 transitions for timeline/plots
                    with lrn._lock:
                        for tr in lrn._transitions[-500:]:
                            all_transitions.append({
                                "ts": tr.timestamp,
                                "device_key": tr.device_key,
                                "delta_w": round(tr.delta_w, 1),
                                "power_before": round(tr.power_before, 1),
                                "power_after": round(tr.power_after, 1),
                            })
                except Exception:
                    continue
            all_transitions.sort(key=lambda x: x["ts"], reverse=True)
            store = self.live_store
            if store is not None:
                store._nilm_clusters = all_clusters  # type: ignore[attr-defined]
                store._nilm_transition_count = total_trans  # type: ignore[attr-defined]
                store._nilm_transitions = all_transitions[:500]  # type: ignore[attr-defined]
                store._nilm_device_count = len(self._nilm_learners)  # type: ignore[attr-defined]
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
        """Start periodic auto-sync. An immediate one-shot sync is triggered
        on startup regardless of whether periodic auto-sync is enabled, so the
        Plots tab has fresh data as soon as the user opens the dashboard.
        """
        periodic_enabled = bool(getattr(self.cfg.ui, "autosync_enabled", False))

        interval_m = int(getattr(self.cfg.ui, "autosync_interval_minutes", 0) or 0)
        interval_h = int(getattr(self.cfg.ui, "autosync_interval_hours", 12) or 12)
        if interval_m > 0:
            interval_s = max(60, interval_m * 60)  # minimum 1 minute
        else:
            interval_s = max(300, interval_h * 3600)  # minimum 5 minutes

        mode_cfg = str(getattr(self.cfg.ui, "autosync_mode", "incremental") or "incremental")

        def _run_sync(label: str) -> None:
            try:
                from shelly_analyzer.services.sync import sync_all
                now = int(time.time())
                range_override = None
                if mode_cfg == "day":
                    range_override = (max(0, now - 86400), now)
                elif mode_cfg == "week":
                    range_override = (max(0, now - 7 * 86400), now)
                elif mode_cfg == "month":
                    range_override = (max(0, now - 30 * 86400), now)
                sync_all(self.cfg, self.storage, range_override=range_override, fallback_last_days=7)
                logger.info("%s completed", label)
            except Exception as e:
                logger.warning("%s failed: %s", label, e)

        def _sync_loop():
            # Immediate initial sync after a short warm-up (3 s) so the web
            # server is already accepting requests while sync runs.
            self._stop_event.wait(3.0)
            if self._stop_event.is_set():
                return
            _run_sync("Initial sync on startup")
            if not periodic_enabled:
                return
            while not self._stop_event.is_set():
                self._stop_event.wait(interval_s)
                if self._stop_event.is_set():
                    break
                _run_sync("Auto-sync")

        self._autosync_thread = threading.Thread(target=_sync_loop, daemon=True)
        self._autosync_thread.start()
        if periodic_enabled:
            logger.info("Auto-sync enabled (interval: %d s) + initial sync on startup", interval_s)
        else:
            logger.info("Auto-sync disabled, but running one-shot initial sync on startup")

    # ── CO2 / Spot fetchers ────────────────────────────────────────────

    def _start_co2_fetcher(self) -> None:
        """Start periodic ENTSO-E CO₂ intensity fetch if enabled."""
        co2_cfg = getattr(self.cfg, "co2", None)
        if co2_cfg is None or not getattr(co2_cfg, "enabled", False):
            logger.debug("CO2 fetcher disabled")
            return
        if not (getattr(co2_cfg, "entso_e_api_token", "") or ""):
            logger.warning("CO2 enabled but no ENTSO-E token configured")
            return
        try:
            from shelly_analyzer.services.entsoe import Co2FetchService
            self._co2_fetcher = Co2FetchService(
                db=self.storage.db,
                get_config=lambda: self.cfg,
            )
            self._co2_fetcher.start()
            # Trigger immediately so we have data shortly after startup
            self._co2_fetcher.trigger_now()
            logger.info("CO2 fetcher started (zone=%s, interval=%sh)",
                        getattr(co2_cfg, "bidding_zone", "?"),
                        getattr(co2_cfg, "fetch_interval_hours", 1))
        except Exception as e:
            logger.exception("CO2 fetcher failed to start: %s", e)

    def _start_spot_fetcher(self) -> None:
        """Start periodic spot-price fetch if enabled."""
        spot_cfg = getattr(self.cfg, "spot_price", None)
        if spot_cfg is None or not getattr(spot_cfg, "enabled", False):
            logger.debug("Spot-price fetcher disabled")
            return
        try:
            from shelly_analyzer.services.spot_price import SpotPriceFetchService
            self._spot_fetcher = SpotPriceFetchService(
                db=self.storage.db,
                get_config=lambda: self.cfg,
            )
            self._spot_fetcher.start()
            self._spot_fetcher.trigger_now()
            logger.info("Spot-price fetcher started (zone=%s)",
                        getattr(spot_cfg, "bidding_zone", "?"))
        except Exception as e:
            logger.exception("Spot-price fetcher failed to start: %s", e)

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

    # ── Alert Evaluation ──────────────────────────────────────────────

    def _alerts_value(self, s: Any, metric: str) -> float:
        """Extract a numeric metric value from a live sample."""
        m0 = (metric or "W").strip().upper()
        m = m0.replace(" ", "").replace("Φ", "PHI")

        phase = None
        if m.endswith("_L1"):
            phase = "a"; base = m[:-3]
        elif m.endswith("_L2"):
            phase = "b"; base = m[:-3]
        elif m.endswith("_L3"):
            phase = "c"; base = m[:-3]
        else:
            base = m

        def _mean_abc(d: dict) -> float:
            vals = [float(d.get(k, 0) or 0) for k in ("a", "b", "c") if float(d.get(k, 0) or 0) != 0]
            if vals:
                return sum(vals) / len(vals)
            return float(d.get("total", 0) or 0)

        def _sum_abc(d: dict) -> float:
            s = sum(float(d.get(k, 0) or 0) for k in ("a", "b", "c"))
            return s if s != 0 else float(d.get("total", 0) or 0)

        if base in {"W", "P", "POWER"}:
            d = getattr(s, "power_w", {}) or {}
            return float(d.get(phase, 0) or 0) if phase else float(d.get("total", 0) or 0)
        if base in {"V", "VOLT", "VOLTAGE"}:
            d = getattr(s, "voltage_v", {}) or {}
            return float(d.get(phase, 0) or 0) if phase else _mean_abc(d)
        if base in {"A", "AMP", "CURRENT"}:
            d = getattr(s, "current_a", {}) or {}
            if m0 == "A_N":
                try:
                    ia = float(d.get("a", 0) or 0)
                    ib = float(d.get("b", 0) or 0)
                    ic = float(d.get("c", 0) or 0)
                    if ia > 0 or ib > 0 or ic > 0:
                        inner = ia**2 + ib**2 + ic**2 - ia*ib - ib*ic - ia*ic
                        return math.sqrt(max(0, inner))
                except Exception:
                    pass
                return 0.0
            return float(d.get(phase, 0) or 0) if phase else _sum_abc(d)
        if base in {"VAR", "Q", "REACTIVE"}:
            d = getattr(s, "reactive_var", {}) or {}
            return float(d.get(phase, 0) or 0) if phase else float(d.get("total", 0) or 0)
        if base in {"COSPHI", "PF", "POWERFACTOR"}:
            d = getattr(s, "cosphi", {}) or {}
            return float(d.get(phase, 0) or 0) if phase else float(d.get("total", 0) or 0)
        if base in {"HZ", "FREQ", "FREQUENCY"}:
            return float((getattr(s, "freq_hz", {}) or {}).get("total", 0) or 0)
        return float((getattr(s, "power_w", {}) or {}).get("total", 0) or 0)

    def _alerts_process_sample(self, s: Any) -> None:
        """Evaluate all configured alert rules against a live sample."""
        rules = list(getattr(self.cfg, "alerts", []) or [])
        if not rules:
            return
        logger.debug("Evaluating %d alert rules for %s", len(rules), getattr(s, "device_key", "?"))

        for r in rules:
            try:
                if not getattr(r, "enabled", True):
                    continue
                devk = str(getattr(r, "device_key", "*") or "*").strip()
                if devk not in {"*", getattr(s, "device_key", "")}:
                    continue

                rid = str(getattr(r, "rule_id", "") or "") or f"{devk}:{getattr(r, 'metric', 'W')}"
                op = str(getattr(r, "op", ">") or ">").strip()
                thr = float(getattr(r, "threshold", 0) or 0)
                dur = int(getattr(r, "duration_seconds", 10) or 0)
                cd = int(getattr(r, "cooldown_seconds", 120) or 0)
                metric = str(getattr(r, "metric", "W") or "W")
                val = self._alerts_value(s, metric)

                cond = (
                    (val > thr) if op == ">" else
                    (val < thr) if op == "<" else
                    (val >= thr) if op in {">=", "=>"} else
                    (val <= thr) if op in {"<=", "=<"} else
                    (val == thr) if op in {"=", "=="} else
                    (val > thr)
                )

                st = self._alert_state.setdefault(rid, {"start_ts": None, "triggered": False, "last_trigger_ts": 0})
                if not cond:
                    st["start_ts"] = None
                    st["triggered"] = False
                    continue
                if st["start_ts"] is None:
                    st["start_ts"] = int(getattr(s, "ts", 0) or 0)
                if st.get("triggered"):
                    continue
                now_ts = int(getattr(s, "ts", 0) or 0)
                if dur > 0 and (now_ts - int(st["start_ts"] or now_ts)) < dur:
                    continue
                if cd > 0 and (now_ts - int(st.get("last_trigger_ts", 0) or 0)) < cd:
                    continue

                st["triggered"] = True
                st["last_trigger_ts"] = now_ts
                logger.info("Alert FIRED: rule=%s metric=%s val=%s %s %s", rid, metric, round(val, 2), op, thr)

                devname = str(getattr(s, "device_name", getattr(s, "device_key", "")) or getattr(s, "device_key", ""))
                msg_custom = str(getattr(r, "message", "") or "").strip()
                msg = msg_custom or f"Alert: {devname} – {metric} {op} {thr} (value: {round(val, 2)})"

                # Build detailed message
                ts_str = datetime.fromtimestamp(now_ts).strftime("%Y-%m-%d %H:%M:%S")
                detail_msg = (
                    f"🚨 Shelly Alert\n"
                    f"Time: {ts_str}\n"
                    f"Device: {devname} ({getattr(s, 'device_key', devk)})\n"
                    f"Rule: {metric} {op} {thr} (duration {dur}s, cooldown {cd}s)\n"
                    f"Value: {round(val, 4)}"
                )
                if msg_custom:
                    detail_msg += f"\nInfo: {msg_custom}"

                if bool(getattr(r, "action_telegram", False)):
                    threading.Thread(
                        target=self._telegram_send, args=(detail_msg,), daemon=True
                    ).start()

                if bool(getattr(r, "action_webhook", False)):
                    payload = {
                        "type": "alarm", "timestamp": ts_str, "rule_id": rid,
                        "device_key": str(getattr(s, "device_key", "") or devk),
                        "device_name": devname, "metric": metric,
                        "value": round(val, 4), "op": op, "threshold": round(thr, 4),
                        "message": msg, "source": "shelly-energy-analyzer",
                    }
                    threading.Thread(
                        target=self._webhook_send, args=(payload,), daemon=True
                    ).start()

                if bool(getattr(r, "action_email", False)):
                    subj = f"[Shelly Alert] {metric} {op} {thr} – {devname}"
                    threading.Thread(
                        target=self._email_send, args=(subj, detail_msg), daemon=True
                    ).start()

            except Exception as e:
                logger.debug("Alert rule evaluation error: %s", e)

    # ── Telegram ──────────────────────────────────────────────────────

    def _telegram_send(self, text: str) -> bool:
        """Send a Telegram message. Returns True on success."""
        try:
            ui = self.cfg.ui
            if not getattr(ui, "telegram_enabled", False):
                logger.debug("Telegram disabled in config")
                return False
            token = str(getattr(ui, "telegram_bot_token", "") or "").strip()
            chat_id = str(getattr(ui, "telegram_chat_id", "") or "").strip()
            if not token or not chat_id:
                logger.warning("Telegram token or chat_id missing")
                return False
        except Exception:
            return False

        try:
            import requests as req
            resp = req.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": text},
                timeout=10,
                verify=bool(getattr(ui, "telegram_verify_ssl", True)),
            )
            ok = resp.status_code == 200
            if not ok:
                logger.warning("Telegram send failed: %s %s", resp.status_code, resp.text[:200])
            return ok
        except Exception as e:
            logger.warning("Telegram send error: %s", e)
            return False

    # ── Webhook ───────────────────────────────────────────────────────

    def _webhook_send(self, payload: dict) -> bool:
        """Send a JSON webhook POST. Returns True on success."""
        try:
            ui = self.cfg.ui
            if not getattr(ui, "webhook_enabled", False):
                return False
            url = str(getattr(ui, "webhook_url", "") or "").strip()
            if not url:
                return False
        except Exception:
            return False

        try:
            body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
            r = urllib.request.Request(url, data=body, method="POST")
            r.add_header("Content-Type", "application/json")
            r.add_header("User-Agent", "ShellyEnergyAnalyzer")
            # Custom headers
            headers_str = str(getattr(ui, "webhook_custom_headers", "") or "").strip()
            if headers_str:
                try:
                    for k, v in json.loads(headers_str).items():
                        r.add_header(str(k), str(v))
                except Exception:
                    pass
            with urllib.request.urlopen(r, timeout=10) as resp:
                resp.read()
            return True
        except Exception as e:
            logger.warning("Webhook send error: %s", e)
            return False

    # ── Email ─────────────────────────────────────────────────────────

    def _email_send(self, subject: str, body: str, attachments: Optional[List[Path]] = None) -> bool:
        """Send an email via SMTP. Returns True on success."""
        try:
            ui = self.cfg.ui
            if not getattr(ui, "email_enabled", False):
                return False
            server = str(getattr(ui, "email_smtp_server", "") or "").strip()
            if not server:
                return False
            port = int(getattr(ui, "email_smtp_port", 587))
            user = str(getattr(ui, "email_smtp_user", "") or "").strip()
            password = str(getattr(ui, "email_smtp_password", "") or "").strip()
            from_addr = str(getattr(ui, "email_from_address", "") or "").strip() or user
            if not from_addr:
                return False
            recipients = [a.strip() for a in str(getattr(ui, "email_recipients", "") or "").split(",") if a.strip()]
            if not recipients:
                return False
            use_tls = bool(getattr(ui, "email_use_tls", True))
        except Exception:
            return False

        try:
            import smtplib
            import ssl as _ssl
            from email.mime.multipart import MIMEMultipart
            from email.mime.text import MIMEText
            from email.mime.base import MIMEBase
            from email import encoders

            msg = MIMEMultipart()
            msg["From"] = from_addr
            msg["To"] = ", ".join(recipients)
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "plain", "utf-8"))

            for att_path in (attachments or []):
                try:
                    p = Path(att_path)
                    if p.exists() and p.is_file():
                        part = MIMEBase("application", "octet-stream")
                        part.set_payload(p.read_bytes())
                        encoders.encode_base64(part)
                        part.add_header("Content-Disposition", f'attachment; filename="{p.name}"')
                        msg.attach(part)
                except Exception:
                    pass

            if use_tls:
                ctx = _ssl.create_default_context()
                if port == 465:
                    with smtplib.SMTP_SSL(server, port, context=ctx, timeout=15) as smtp:
                        if user and password:
                            smtp.login(user, password)
                        smtp.sendmail(from_addr, recipients, msg.as_string())
                else:
                    with smtplib.SMTP(server, port, timeout=15) as smtp:
                        smtp.ehlo()
                        smtp.starttls(context=ctx)
                        smtp.ehlo()
                        if user and password:
                            smtp.login(user, password)
                        smtp.sendmail(from_addr, recipients, msg.as_string())
            else:
                with smtplib.SMTP(server, port, timeout=15) as smtp:
                    smtp.ehlo()
                    if user and password:
                        smtp.login(user, password)
                    smtp.sendmail(from_addr, recipients, msg.as_string())
            return True
        except Exception as e:
            logger.warning("Email send error: %s", e)
            return False

    # ── Scheduled Summaries (Telegram + Email + Webhook) ──────────────

    def _start_summary_scheduler(self) -> None:
        """Start a background thread that checks for scheduled summaries."""
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Europe/Berlin")
        now = datetime.now(tz)
        # Initialise to today so we don't immediately fire on startup.
        # The summary will only be sent once per day, *after* the scheduled time,
        # on the *next* day boundary that hasn't been sent yet.
        self._summary_last_daily = now.strftime("%Y-%m-%d")
        self._summary_last_monthly = now.strftime("%Y-%m")
        # Restore persisted last-sent dates from config (survives restarts)
        try:
            ui = self.cfg.ui
            saved_d = str(getattr(ui, "telegram_daily_summary_last_sent", "") or "").strip()
            saved_m = str(getattr(ui, "telegram_monthly_summary_last_sent", "") or "").strip()
            if saved_d:
                self._summary_last_daily = saved_d
            if saved_m:
                self._summary_last_monthly = saved_m
        except Exception:
            pass
        self._summary_thread = threading.Thread(target=self._summary_loop, daemon=True)
        self._summary_thread.start()
        logger.info("Summary scheduler started (last_daily=%s, last_monthly=%s)",
                     self._summary_last_daily, self._summary_last_monthly)

    def _parse_hhmm(self, s: str) -> tuple:
        """Parse 'HH:MM' string into (hour, minute)."""
        try:
            m = re.match(r"^(\d{1,2})\s*:\s*(\d{1,2})$", (s or "").strip())
            if not m:
                return 0, 0
            return max(0, min(23, int(m.group(1)))), max(0, min(59, int(m.group(2))))
        except Exception:
            return 0, 0

    def _query_device_kwh(self, device_key: str, start_ts: int, end_ts: int) -> float:
        """Query hourly kWh for a device in a time range."""
        try:
            import pandas as pd
            df = self.storage.db.query_hourly(device_key, start_ts=start_ts, end_ts=end_ts)
            if df is not None and not df.empty and "kwh" in df.columns:
                return float(pd.to_numeric(df["kwh"], errors="coerce").fillna(0).sum())
        except Exception:
            pass
        return 0.0

    def _build_daily_summary(self) -> str:
        """Build a daily summary text with yesterday's data + comparison."""
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Europe/Berlin")
        now = datetime.now(tz)
        yesterday = now - timedelta(days=1)
        day_before = now - timedelta(days=2)

        unit_price = 0.30
        try:
            unit_price = float(self.cfg.pricing.unit_price_gross())
        except Exception:
            pass

        y_start = int(yesterday.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        y_end = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        db_start = int(day_before.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())

        lines = [f"📊 Daily Summary – {yesterday.strftime('%A, %d.%m.%Y')}", ""]

        total_kwh = 0.0
        total_prev = 0.0
        dev_lines = []

        for d in (self.cfg.devices or []):
            if str(getattr(d, "kind", "em")) == "switch":
                continue
            kwh = self._query_device_kwh(d.key, y_start, y_end)
            prev = self._query_device_kwh(d.key, db_start, y_start)
            total_kwh += kwh
            total_prev += prev
            cost = kwh * unit_price
            delta = ""
            if prev > 0:
                pct = ((kwh - prev) / prev) * 100
                arrow = "📈" if pct > 5 else "📉" if pct < -5 else "➡️"
                delta = f" {arrow} {pct:+.0f}%"
            if kwh > 0:
                dev_lines.append(f"  ⚡ {d.name}: {kwh:.2f} kWh | {cost:.2f} €{delta}")

        if dev_lines:
            lines.extend(dev_lines)
        else:
            lines.append("  No data for yesterday.")

        total_cost = total_kwh * unit_price
        lines.append("")
        lines.append(f"🔋 Total: {total_kwh:.2f} kWh | 💰 {total_cost:.2f} €")

        if total_prev > 0:
            pct = ((total_kwh - total_prev) / total_prev) * 100
            lines.append(f"📊 vs. day before: {pct:+.1f}%")

        # Monthly projection
        try:
            import calendar
            days_in_month = calendar.monthrange(now.year, now.month)[1]
            day_of_month = now.day
            if day_of_month > 1:
                # Use this month's average daily consumption so far
                m_start = int(now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).timestamp())
                m_kwh = sum(self._query_device_kwh(d.key, m_start, y_end) for d in (self.cfg.devices or []) if str(getattr(d, "kind", "em")) != "switch")
                if m_kwh > 0:
                    proj = m_kwh / (day_of_month - 1) * days_in_month
                    proj_cost = proj * unit_price
                    lines.append(f"📅 Month projection: ~{proj:.0f} kWh | ~{proj_cost:.0f} €")
        except Exception:
            pass

        return "\n".join(lines)

    def _build_monthly_summary(self) -> str:
        """Build a monthly summary text with comparison to previous month."""
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Europe/Berlin")
        now = datetime.now(tz)
        last_month_end = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        last_month_start = (last_month_end - timedelta(days=1)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        prev_month_start = (last_month_start - timedelta(days=1)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        month_label = last_month_start.strftime("%B %Y")

        unit_price = 0.30
        try:
            unit_price = float(self.cfg.pricing.unit_price_gross())
        except Exception:
            pass

        ms_ts = int(last_month_start.timestamp())
        me_ts = int(last_month_end.timestamp())
        ps_ts = int(prev_month_start.timestamp())

        lines = [f"📊 Monthly Summary – {month_label}", ""]
        total_kwh = 0.0
        total_prev = 0.0
        dev_data = []

        for d in (self.cfg.devices or []):
            if str(getattr(d, "kind", "em")) == "switch":
                continue
            kwh = self._query_device_kwh(d.key, ms_ts, me_ts)
            prev = self._query_device_kwh(d.key, ps_ts, ms_ts)
            total_kwh += kwh
            total_prev += prev
            if kwh > 0:
                dev_data.append((d.name, kwh, prev))

        # Sort by consumption descending
        dev_data.sort(key=lambda x: x[1], reverse=True)

        for name, kwh, prev in dev_data:
            cost = kwh * unit_price
            delta = ""
            if prev > 0:
                pct = ((kwh - prev) / prev) * 100
                arrow = "📈" if pct > 5 else "📉" if pct < -5 else "➡️"
                delta = f" {arrow} {pct:+.0f}%"
            lines.append(f"  ⚡ {name}: {kwh:.1f} kWh | {cost:.2f} €{delta}")

        total_cost = total_kwh * unit_price
        lines.append("")
        lines.append(f"🔋 Total: {total_kwh:.1f} kWh | 💰 {total_cost:.2f} €")

        if total_prev > 0:
            pct = ((total_kwh - total_prev) / total_prev) * 100
            lines.append(f"📊 vs. previous month: {pct:+.1f}%")

        # Daily average
        try:
            import calendar
            days = calendar.monthrange(last_month_start.year, last_month_start.month)[1]
            avg_daily = total_kwh / days
            lines.append(f"📅 Daily average: {avg_daily:.1f} kWh | {avg_daily * unit_price:.2f} €")
        except Exception:
            pass

        return "\n".join(lines)

    def _summary_loop(self) -> None:
        """Periodically check if daily/monthly summaries are due."""
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Europe/Berlin")

        # Wait for services to be ready
        self._stop_event.wait(10.0)

        while not self._stop_event.is_set():
            try:
                now = datetime.now(tz)
                ui = self.cfg.ui

                # ── Daily summary ──
                today_str = now.strftime("%Y-%m-%d")
                tg_daily = bool(getattr(ui, "telegram_daily_summary_enabled", False))
                em_daily = bool(getattr(ui, "email_daily_summary_enabled", False))
                wh_daily = bool(getattr(ui, "webhook_daily_summary_enabled", False))
                if (tg_daily or em_daily or wh_daily) and today_str != self._summary_last_daily:
                    d_hh, d_mm = self._parse_hhmm(getattr(ui, "telegram_daily_summary_time", "00:00"))
                    if now.hour > d_hh or (now.hour == d_hh and now.minute >= d_mm):
                        summary = self._build_daily_summary()
                        if tg_daily:
                            self._telegram_send(summary)
                        if em_daily:
                            self._email_send("Shelly Energy – Daily Summary", summary)
                        if wh_daily:
                            self._webhook_send({
                                "type": "daily_summary",
                                "timestamp": now.isoformat(),
                                "text": summary,
                                "source": "shelly-energy-analyzer",
                            })
                        self._summary_last_daily = today_str
                        self._persist_summary_dates()
                        logger.info("Daily summary sent")

                # ── Monthly summary (on 1st of each month) ──
                month_str = now.strftime("%Y-%m")
                tg_monthly = bool(getattr(ui, "telegram_monthly_summary_enabled", False))
                em_monthly = bool(getattr(ui, "email_monthly_summary_enabled", False))
                wh_monthly = bool(getattr(ui, "webhook_monthly_summary_enabled", False))
                if (tg_monthly or em_monthly or wh_monthly) and now.day <= 2 and month_str != self._summary_last_monthly:
                    m_hh, m_mm = self._parse_hhmm(getattr(ui, "telegram_monthly_summary_time", "00:00"))
                    if now.hour > m_hh or (now.hour == m_hh and now.minute >= m_mm):
                        summary = self._build_monthly_summary()
                        if tg_monthly:
                            self._telegram_send(summary)
                        if em_monthly:
                            self._email_send("Shelly Energy – Monthly Summary", summary)
                        if wh_monthly:
                            self._webhook_send({
                                "type": "monthly_summary",
                                "timestamp": now.isoformat(),
                                "text": summary,
                                "source": "shelly-energy-analyzer",
                            })
                        self._summary_last_monthly = month_str
                        self._persist_summary_dates()
                        logger.info("Monthly summary sent")

            except Exception as e:
                logger.debug("Summary loop error: %s", e)

            # Check every 60 seconds
            self._stop_event.wait(60.0)

    def _persist_summary_dates(self) -> None:
        """Write last-sent dates to config so they survive restarts."""
        try:
            from dataclasses import replace as _replace
            from shelly_analyzer.io.config import save_config
            new_ui = _replace(
                self.cfg.ui,
                telegram_daily_summary_last_sent=self._summary_last_daily,
                telegram_monthly_summary_last_sent=self._summary_last_monthly,
            )
            new_cfg = _replace(self.cfg, ui=new_ui)
            cfg_path = self.out_dir / "config.json"
            save_config(new_cfg, cfg_path)
            self.cfg = new_cfg
        except Exception as e:
            logger.debug("Failed to persist summary dates: %s", e)
