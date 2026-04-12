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
        self._co2_forecaster = None
        self._spot_fetcher = None
        self._feed_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._autosync_thread: Optional[threading.Thread] = None

        # Latest result from the periodic GitHub update check. Written by the
        # update-check thread, read by /api/updates/cached for the Live-tab
        # banner. None means "not checked yet".
        self._update_check_state: Optional[Dict[str, Any]] = None
        self._update_check_thread: Optional[threading.Thread] = None

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
        self._start_co2_forecaster()
        self._start_spot_fetcher()
        self._start_summary_scheduler()
        self._start_update_checker()
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
        if self._co2_forecaster:
            try:
                self._co2_forecaster.stop()
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
        from datetime import datetime, date as _date

        # Cache the effective price per date so we both honor the tariff schedule
        # and pick up config hot-reloads without an app restart.
        _price_cache: Dict[_date, float] = {}

        def _price_for(day: _date) -> float:
            p = _price_cache.get(day)
            if p is not None:
                return p
            try:
                p = float(self.cfg.pricing.effective_pricing_for_date(day).unit_price_gross())
            except Exception:
                p = float(getattr(self.cfg.pricing, "electricity_price_eur_per_kwh", 0.30) or 0.30)
            _price_cache[day] = p
            # Keep cache small: only today + yesterday are ever needed.
            if len(_price_cache) > 4:
                for old in list(_price_cache.keys()):
                    if old != day:
                        _price_cache.pop(old, None)
            return p

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
                cost_today = kwh_today * _price_for(datetime.fromtimestamp(ts_i).date())
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
        """Read already-imported kWh for *today* from the computed cache
        (same source as the Costs tab) so both show identical values.

        Falls back to the DB hourly table if computed data is unavailable.

        Returns (baseline_kwh, baseline_last_ts) where baseline_last_ts is the
        end of the latest data row we have, so live accumulation only
        starts after it (no double counting after a sync).
        """
        import pandas as pd

        # Primary: use computed DataFrame (same as Costs tab)
        try:
            dispatcher = getattr(self, "_dispatcher", None)
            if dispatcher is not None:
                cd = dispatcher.computed.get(device_key)
                if cd is not None and cd.df is not None and not cd.df.empty:
                    df = cd.df.copy()
                    if "timestamp" in df.columns and "energy_kwh" in df.columns:
                        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
                        df = df.dropna(subset=["timestamp"])
                        try:
                            if df["timestamp"].dt.tz is None:
                                df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
                            from datetime import datetime, timezone
                            local_tz = datetime.now().astimezone().tzinfo
                            df["timestamp"] = df["timestamp"].dt.tz_convert(local_tz)
                        except Exception:
                            pass
                        day_start_dt = pd.Timestamp.fromtimestamp(day_start_ts)
                        try:
                            day_start_dt = day_start_dt.tz_localize(df["timestamp"].dt.tz)
                        except Exception:
                            pass
                        mask = df["timestamp"] >= day_start_dt
                        ekwh = pd.to_numeric(df.loc[mask, "energy_kwh"], errors="coerce").fillna(0.0)
                        total = float(ekwh.sum())
                        if not df.loc[mask].empty:
                            last_ts = int(df.loc[mask, "timestamp"].max().timestamp())
                        else:
                            last_ts = int(day_start_ts)
                        return total, last_ts
        except Exception as e:
            logger.debug("Computed baseline lookup failed for %s: %s", device_key, e)

        # Fallback: DB hourly table
        try:
            db = getattr(self.storage, "db", None)
            if db is None:
                return 0.0, 0
            df = db.query_hourly(device_key, start_ts=int(day_start_ts), end_ts=int(time.time()))
            if df is None or df.empty:
                return 0.0, int(day_start_ts)
            total = float(df["kwh"].fillna(0).sum())
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
            for _d in self.cfg.devices:
                _pw = getattr(_d, "password", "") or ""
                if _pw:
                    http_client.set_credentials(
                        _d.host,
                        getattr(_d, "username", "admin") or "admin",
                        _pw,
                    )
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
        """Start periodic CO₂ intensity fetch if enabled. Dispatches to
        Electricity Maps for global zones (hyphen form, API key set) or
        ENTSO-E for EU zones (underscore form)."""
        co2_cfg = getattr(self.cfg, "co2", None)
        if co2_cfg is None or not getattr(co2_cfg, "enabled", False):
            logger.debug("CO2 fetcher disabled")
            return

        zone = str(getattr(co2_cfg, "bidding_zone", "") or "")
        em_key = str(getattr(co2_cfg, "electricity_maps_api_key", "") or "")
        entsoe_token = str(getattr(co2_cfg, "entso_e_api_token", "") or "")

        # Electricity Maps zones are hyphen-coded (no underscores). If the
        # zone looks global and a key is present, use Electricity Maps
        # instead of ENTSO-E.
        use_electricity_maps = bool(em_key) and ("_" not in zone) and zone
        if use_electricity_maps:
            try:
                from shelly_analyzer.services.electricity_maps import ElectricityMapsFetchService
                self._co2_fetcher = ElectricityMapsFetchService(
                    db=self.storage.db,
                    get_config=lambda: self.cfg,
                )
                self._co2_fetcher.start()
                self._co2_fetcher.trigger_now()
                logger.info("CO2 fetcher started via Electricity Maps (zone=%s)", zone)
                return
            except Exception as e:
                logger.exception("Electricity Maps fetcher failed to start: %s", e)

        if not entsoe_token:
            logger.warning(
                "CO2 enabled but no provider available: zone=%s, entso_e_token=%s, em_key=%s",
                zone, bool(entsoe_token), bool(em_key),
            )
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
            logger.info("CO2 fetcher started via ENTSO-E (zone=%s, interval=%sh)",
                        zone or "?",
                        getattr(co2_cfg, "fetch_interval_hours", 1))
        except Exception as e:
            logger.exception("CO2 fetcher failed to start: %s", e)

    def _start_co2_forecaster(self) -> None:
        """Start the 6h CO2 forecast + recent-gap backfill service
        (trend + Open-Meteo weather)."""
        co2_cfg = getattr(self.cfg, "co2", None)
        if co2_cfg is None or not getattr(co2_cfg, "enabled", False):
            return
        try:
            from shelly_analyzer.services.co2_forecast import Co2ForecastService
            self._co2_forecaster = Co2ForecastService(
                db=self.storage.db,
                get_config=lambda: self.cfg,
            )
            self._co2_forecaster.start()
            # Trigger an immediate run so backfill + forward forecast are
            # populated within seconds of startup, not after the 25s warmup.
            try:
                threading.Timer(2.0, self._co2_forecaster.trigger_now).start()
            except Exception:
                pass
            logger.info("CO2 forecaster started (zone=%s)",
                        getattr(co2_cfg, "bidding_zone", "?"))
        except Exception as e:
            logger.exception("CO2 forecaster failed to start: %s", e)

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

    # ── Periodic Update Check ──────────────────────────────────────────

    def _start_update_checker(self) -> None:
        """Periodically query GitHub for the latest release and cache the
        result so the Live-tab banner can surface new versions without
        hitting the network on every page load."""
        upd_cfg = getattr(self.cfg, "updates", None)
        if upd_cfg is None or not getattr(upd_cfg, "check_on_start", True):
            logger.debug("Update checker disabled via config")
            return
        self._update_check_thread = threading.Thread(
            target=self._update_check_loop, name="UpdateChecker", daemon=True,
        )
        self._update_check_thread.start()
        logger.info("Update checker started")

    def _update_check_loop(self) -> None:
        """Run an initial check ~15s after startup, then re-check every 6h."""
        from shelly_analyzer import __version__
        from shelly_analyzer.services.updater import check_latest_release, is_newer

        # Short delay so the app is up before we hit the network.
        if self._stop_event.wait(5.0):
            return

        while not self._stop_event.is_set():
            try:
                repo = str(getattr(getattr(self.cfg, "updates", None), "repo", "") or "").strip()
                if not repo:
                    self._update_check_state = {
                        "ok": False, "error": "updates.repo not configured",
                        "current": __version__,
                        "checked_at": int(time.time()),
                    }
                else:
                    info = check_latest_release(repo)
                    self._update_check_state = {
                        "ok": True,
                        "current": __version__,
                        "repo": repo,
                        "reachable": info.reachable,
                        "status": info.status,
                        "latest_tag": info.latest_tag,
                        "has_update": bool(
                            info.latest_tag and is_newer(info.latest_tag, __version__)
                        ),
                        "asset_url": info.asset_url,
                        "asset_name": info.asset_name,
                        "checked_at": int(time.time()),
                    }
                    if self._update_check_state.get("has_update"):
                        logger.info(
                            "Update available: %s (current %s)",
                            info.latest_tag, __version__,
                        )
            except Exception as e:
                logger.warning("Update check failed: %s", e)
                self._update_check_state = {
                    "ok": False, "error": str(e),
                    "current": __version__,
                    "checked_at": int(time.time()),
                }

            # Re-check every 5 minutes so users see new releases quickly.
            if self._stop_event.wait(300):
                return

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
            # Split into chunks if > 4000 chars
            chunks = []
            if len(text) <= 4000:
                chunks = [text]
            else:
                lines = text.split("\n")
                chunk = ""
                for line in lines:
                    if len(chunk) + len(line) + 1 > 4000:
                        chunks.append(chunk)
                        chunk = line
                    else:
                        chunk = chunk + "\n" + line if chunk else line
                if chunk:
                    chunks.append(chunk)

            ok = True
            verify = bool(getattr(ui, "telegram_verify_ssl", True))
            for chunk in chunks:
                resp = req.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": chat_id, "text": chunk},
                    timeout=10,
                    verify=verify,
                )
                if resp.status_code != 200:
                    logger.warning("Telegram send failed: %s %s", resp.status_code, resp.text[:200])
                    ok = False
            return ok
        except Exception as e:
            logger.warning("Telegram send error: %s", e)
            return False

    def _telegram_send_photo(self, photo_path: Path, caption: str = "") -> bool:
        """Send a photo via Telegram. Returns True on success."""
        try:
            ui = self.cfg.ui
            if not getattr(ui, "telegram_enabled", False):
                return False
            token = str(getattr(ui, "telegram_bot_token", "") or "").strip()
            chat_id = str(getattr(ui, "telegram_chat_id", "") or "").strip()
            if not token or not chat_id:
                return False
        except Exception:
            return False
        try:
            import requests as req
            with open(photo_path, "rb") as f:
                data = {"chat_id": chat_id}
                if caption:
                    data["caption"] = caption[:1024]
                resp = req.post(
                    f"https://api.telegram.org/bot{token}/sendPhoto",
                    data=data,
                    files={"photo": f},
                    timeout=15,
                    verify=bool(getattr(ui, "telegram_verify_ssl", True)),
                )
            ok = resp.status_code == 200
            if not ok:
                logger.warning("Telegram photo failed: %s %s", resp.status_code, resp.text[:200])
            return ok
        except Exception as e:
            logger.warning("Telegram photo error: %s", e)
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

    def _email_send(
        self,
        subject: str,
        body: str,
        attachments: Optional[List[Path]] = None,
        html_body: Optional[str] = None,
        inline_images: Optional[Dict[str, Path]] = None,
    ) -> bool:
        """Send an email via SMTP. Returns True on success.

        If ``html_body`` is provided, the email is sent as a multipart/alternative
        message: clients that prefer HTML get the rich layout, clients that only
        support plain text fall back to ``body``. ``inline_images`` maps a
        ``Content-ID`` (e.g. ``"chart.png"``) to a file path — those files are
        attached as inline related parts so the HTML body can reference them as
        ``<img src="cid:chart.png">`` without breaking preview apps.
        """
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
            from email.mime.image import MIMEImage
            from email import encoders

            # Outer container: "mixed" lets us attach files + HTML/plain/inline
            # parts together. Inside, we use "related" → "alternative" so
            # inline images travel with the HTML and plain-text fallback works.
            msg = MIMEMultipart("mixed")
            msg["From"] = from_addr
            msg["To"] = ", ".join(recipients)
            msg["Subject"] = subject

            if html_body:
                related = MIMEMultipart("related")
                alt = MIMEMultipart("alternative")
                alt.attach(MIMEText(body, "plain", "utf-8"))
                alt.attach(MIMEText(html_body, "html", "utf-8"))
                related.attach(alt)
                for cid, img_path in (inline_images or {}).items():
                    try:
                        p = Path(img_path)
                        if p.exists() and p.is_file():
                            img = MIMEImage(p.read_bytes())
                            img.add_header("Content-ID", f"<{cid}>")
                            img.add_header("Content-Disposition", "inline", filename=p.name)
                            related.attach(img)
                    except Exception:
                        pass
                msg.attach(related)
            else:
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

    def _query_device_hourly(self, device_key: str, start_ts: int, end_ts: int):
        """Query hourly data for a device. Returns DataFrame with kwh, avg_power_w, hour_ts."""
        try:
            import pandas as pd
            df = self.storage.db.query_hourly(device_key, start_ts=start_ts, end_ts=end_ts)
            if df is not None and not df.empty:
                return df
        except Exception:
            pass
        return None

    def _build_daily_data(self) -> Dict[str, Any]:
        """Collect a rich daily stat dict used by text, HTML and chart builders.

        One query pass over the DB → fed into every output format. Cuts
        down duplicated work and keeps the three report shapes in sync.
        """
        import calendar
        from zoneinfo import ZoneInfo

        tz = ZoneInfo("Europe/Berlin")
        now = datetime.now(tz)
        yesterday = now - timedelta(days=1)
        day_before = now - timedelta(days=2)
        # Same weekday one week ago for weekday-matched comparison.
        same_wd_last_week = yesterday - timedelta(days=7)

        unit_price = 0.30
        try:
            unit_price = float(self.cfg.pricing.unit_price_gross())
        except Exception:
            pass

        y_start = int(yesterday.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        y_end = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        db_start = int(day_before.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        sw_start = int(same_wd_last_week.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        sw_end = sw_start + 86400

        total_kwh = 0.0
        total_prev = 0.0
        total_same_wd = 0.0
        dev_data: List[Dict[str, Any]] = []
        hourly_total = [0.0] * 24
        hourly_per_device: Dict[str, List[float]] = {}
        max_power_w = 0.0
        max_power_hour = -1

        for d in (self.cfg.devices or []):
            if str(getattr(d, "kind", "em")) == "switch":
                continue
            kwh = self._query_device_kwh(d.key, y_start, y_end)
            prev = self._query_device_kwh(d.key, db_start, y_start)
            same_wd = self._query_device_kwh(d.key, sw_start, sw_end)
            total_kwh += kwh
            total_prev += prev
            total_same_wd += same_wd

            peak_w = 0.0
            peak_hour = -1
            vals = [0.0] * 24
            hdf = self._query_device_hourly(d.key, y_start, y_end)
            if hdf is not None and not hdf.empty:
                for _, row in hdf.iterrows():
                    try:
                        h_ts = int(row.get("hour_ts", 0))
                        h = datetime.fromtimestamp(h_ts, tz=tz).hour
                        hw = float(row.get("kwh", 0) or 0)
                        hourly_total[h] += hw
                        vals[h] += hw
                        pw = float(row.get("avg_power_w", 0) or 0)
                        if pw > peak_w:
                            peak_w = pw
                            peak_hour = h
                        if pw > max_power_w:
                            max_power_w = pw
                            max_power_hour = h
                    except Exception:
                        pass
            if kwh > 0:
                dev_data.append({
                    "name": d.name, "kwh": kwh, "prev": prev, "same_wd": same_wd,
                    "cost": kwh * unit_price, "peak_w": peak_w, "peak_hour": peak_hour,
                    "share_pct": 0.0,  # filled in below
                })
                hourly_per_device[d.name] = vals

        dev_data.sort(key=lambda x: x["kwh"], reverse=True)
        for dd in dev_data:
            dd["share_pct"] = (dd["kwh"] / total_kwh * 100) if total_kwh > 0 else 0.0
            if dd["prev"] > 0:
                dd["delta_pct"] = ((dd["kwh"] - dd["prev"]) / dd["prev"]) * 100
            else:
                dd["delta_pct"] = None

        # Biggest mover (absolute kWh change vs previous day)
        biggest_mover: Optional[Dict[str, Any]] = None
        for dd in dev_data:
            if dd["prev"] > 0:
                diff = dd["kwh"] - dd["prev"]
                if biggest_mover is None or abs(diff) > abs(biggest_mover.get("diff", 0)):
                    biggest_mover = {"name": dd["name"], "diff": diff, "from": dd["prev"], "to": dd["kwh"]}

        total_cost = total_kwh * unit_price
        prev_total_cost = total_prev * unit_price

        # Peak hour + top-3 hours (consumption, not power)
        hours_sorted = sorted(enumerate(hourly_total), key=lambda x: -x[1])
        top_hours = [(h, v) for h, v in hours_sorted if v > 0][:3]
        peak_h = hours_sorted[0][0] if hours_sorted and hours_sorted[0][1] > 0 else -1
        peak_h_kwh = hourly_total[peak_h] if peak_h >= 0 else 0.0
        # Cheapest 3 hours (lowest non-zero consumption — useful for "when we were away")
        low_hours = [(h, v) for h, v in sorted(enumerate(hourly_total), key=lambda x: x[1]) if v > 0][:3]

        # Night base load (00–05 avg)
        night_kwh = sum(hourly_total[h] for h in range(0, 5))
        standby_w = (night_kwh / 5) * 1000 if night_kwh > 0 else 0.0
        standby_annual_kwh = standby_w * 8760 / 1000 if standby_w > 0 else 0.0
        standby_annual_cost = standby_annual_kwh * unit_price

        # Load factor: avg power / peak power (1.0 = perfectly flat)
        avg_w = (total_kwh / 24) * 1000 if total_kwh > 0 else 0.0
        load_factor = (avg_w / max_power_w) if max_power_w > 0 else 0.0

        # CO2
        co2_kg = 0.0
        co2_g_per_kwh = 0.0
        try:
            co2_cfg = getattr(self.cfg, "co2", None)
            if co2_cfg and getattr(co2_cfg, "enabled", False):
                co2_g_per_kwh = float(getattr(self.cfg.pricing, "co2_intensity_g_per_kwh", 380) or 380)
                co2_kg = total_kwh * co2_g_per_kwh / 1000
        except Exception:
            pass

        # Month-to-date projection
        proj_kwh = 0.0
        proj_cost = 0.0
        try:
            days_in_month = calendar.monthrange(now.year, now.month)[1]
            if now.day > 1:
                m_start = int(now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).timestamp())
                m_kwh = sum(
                    self._query_device_kwh(d.key, m_start, y_end)
                    for d in (self.cfg.devices or [])
                    if str(getattr(d, "kind", "em")) != "switch"
                )
                if m_kwh > 0:
                    proj_kwh = m_kwh / (now.day - 1) * days_in_month
                    proj_cost = proj_kwh * unit_price
        except Exception:
            pass

        # Spot price savings (if dynamic)
        spot_cost = None
        try:
            zone = getattr(self.cfg.spot_price, "bidding_zone", None)
            if zone and getattr(self.cfg.spot_price, "enabled", False):
                df = self.storage.db.query_spot_prices(zone, y_start, y_end)
                if df is not None and not df.empty:
                    spot_map = {}
                    for _, r in df.iterrows():
                        spot_map[int(r["slot_ts"])] = float(r["price_eur_mwh"]) / 1000.0
                    spot_total = 0.0
                    for h in range(24):
                        slot_ts = y_start + h * 3600
                        p = spot_map.get(slot_ts)
                        if p is not None:
                            spot_total += hourly_total[h] * p
                    if spot_total > 0:
                        spot_cost = spot_total
        except Exception:
            pass

        return {
            "tz": tz,
            "now": now,
            "yesterday": yesterday,
            "y_start": y_start,
            "y_end": y_end,
            "date_label": yesterday.strftime("%A, %d.%m.%Y"),
            "unit_price": unit_price,
            "total_kwh": total_kwh,
            "total_cost": total_cost,
            "total_prev": total_prev,
            "prev_total_cost": prev_total_cost,
            "total_same_wd": total_same_wd,
            "dev_data": dev_data,
            "hourly_total": hourly_total,
            "hourly_per_device": hourly_per_device,
            "max_power_w": max_power_w,
            "max_power_hour": max_power_hour,
            "avg_w": avg_w,
            "load_factor": load_factor,
            "peak_h": peak_h,
            "peak_h_kwh": peak_h_kwh,
            "top_hours": top_hours,
            "low_hours": low_hours,
            "night_kwh": night_kwh,
            "standby_w": standby_w,
            "standby_annual_kwh": standby_annual_kwh,
            "standby_annual_cost": standby_annual_cost,
            "co2_kg": co2_kg,
            "co2_g_per_kwh": co2_g_per_kwh,
            "proj_kwh": proj_kwh,
            "proj_cost": proj_cost,
            "spot_cost": spot_cost,
            "biggest_mover": biggest_mover,
        }

    def _build_daily_summary(self) -> str:
        """Build a rich daily summary text message (Telegram / webhook / plain email)."""
        data = self._build_daily_data()
        return self._format_daily_text(data)

    def _format_daily_text(self, data: Dict[str, Any]) -> str:
        """Format the daily data dict as a plain-text Telegram/webhook message."""
        lines: List[str] = [f"📊 Daily report – {data['date_label']}", ""]

        # Headline KPIs
        lines.append(f"🔋 Total: {data['total_kwh']:.2f} kWh | {data['total_cost']:.2f} €")
        if data["total_prev"] > 0:
            pct = ((data["total_kwh"] - data["total_prev"]) / data["total_prev"]) * 100
            arrow = "📈" if pct > 5 else "📉" if pct < -5 else "➡️"
            diff_eur = (data["total_kwh"] - data["total_prev"]) * data["unit_price"]
            lines.append(f"{arrow} vs. previous day: {pct:+.1f}% ({diff_eur:+.2f} €)")
        if data["total_same_wd"] > 0:
            pct = ((data["total_kwh"] - data["total_same_wd"]) / data["total_same_wd"]) * 100
            arrow = "📈" if pct > 5 else "📉" if pct < -5 else "➡️"
            lines.append(f"{arrow} vs. same weekday last week: {pct:+.1f}%")
        lines.append("")

        # Device breakdown
        lines.append("⚡ Devices:")
        for dd in data["dev_data"]:
            delta = ""
            if dd["delta_pct"] is not None:
                pct = dd["delta_pct"]
                arrow = "📈" if pct > 5 else "📉" if pct < -5 else "➡️"
                delta = f" {arrow}{pct:+.0f}%"
            peak_info = f" | peak {dd['peak_w']:.0f} W @ {dd['peak_hour']:02d}:00" if dd["peak_hour"] >= 0 else ""
            lines.append(
                f"  {dd['name']}: {dd['kwh']:.2f} kWh | {dd['cost']:.2f} € "
                f"({dd['share_pct']:.0f}%){delta}{peak_info}"
            )
        if not data["dev_data"]:
            lines.append("  No data available.")
        lines.append("")

        # Biggest mover vs previous day
        mover = data["biggest_mover"]
        if mover and abs(mover["diff"]) * data["unit_price"] >= 0.20:
            arrow = "📈" if mover["diff"] > 0 else "📉"
            lines.append(
                f"{arrow} Biggest mover: {mover['name']} "
                f"{mover['from']:.1f} → {mover['to']:.1f} kWh "
                f"({mover['diff']:+.1f} kWh, {mover['diff'] * data['unit_price']:+.2f} €)"
            )

        # Peak hour + top-3 hours
        if data["peak_h"] >= 0:
            lines.append(
                f"⏰ Peak hour: {data['peak_h']:02d}:00–{data['peak_h'] + 1:02d}:00 "
                f"({data['peak_h_kwh']:.2f} kWh)"
            )
        if data["top_hours"]:
            tops = ", ".join(f"{h:02d}h ({v:.2f} kWh)" for h, v in data["top_hours"])
            lines.append(f"🏆 Top 3 hours: {tops}")
        if data["low_hours"]:
            lows = ", ".join(f"{h:02d}h ({v:.2f} kWh)" for h, v in data["low_hours"])
            lines.append(f"💤 Lowest 3 hours: {lows}")

        # Max power + load factor
        if data["max_power_w"] > 0:
            lf_pct = data["load_factor"] * 100
            lines.append(
                f"🚀 Max power: {data['max_power_w']:.0f} W @ {data['max_power_hour']:02d}:00  "
                f"(load factor {lf_pct:.0f}%, avg {data['avg_w']:.0f} W)"
            )

        # Standby / night base load
        if data["standby_w"] > 0:
            lines.append(
                f"🌙 Night base load: ~{data['standby_w']:.0f} W  "
                f"(~{data['standby_annual_kwh']:.0f} kWh/year, ~{data['standby_annual_cost']:.0f} €/year)"
            )

        # CO2
        if data["co2_kg"] > 0:
            lines.append(f"🌍 CO₂: {data['co2_kg']:.1f} kg  ({data['co2_g_per_kwh']:.0f} g/kWh grid)")

        # Spot price savings vs fixed
        if data["spot_cost"] is not None:
            delta = data["spot_cost"] - data["total_cost"]
            icon = "💚" if delta < 0 else "💔" if delta > 0 else "➡️"
            lines.append(
                f"{icon} Spot cost today: {data['spot_cost']:.2f} €  "
                f"({delta:+.2f} € vs. fixed)"
            )

        # Month projection
        if data["proj_kwh"] > 0:
            lines.append(f"📅 Month projection: ~{data['proj_kwh']:.0f} kWh | ~{data['proj_cost']:.0f} €")

        # 24h mini text chart
        hourly = data["hourly_total"]
        if any(v > 0 for v in hourly):
            lines.append("")
            lines.append("📊 24h profile:")
            max_h = max(hourly) or 1
            for h in [0, 3, 6, 9, 12, 15, 18, 21]:
                val = hourly[h]
                bar_len = int(val / max_h * 12)
                bar = "█" * bar_len + "░" * (12 - bar_len)
                lines.append(f"  {h:02d}h {bar} {val:.2f}")

        return "\n".join(lines)

    def _build_monthly_data(self) -> Dict[str, Any]:
        """Collect a rich monthly stat dict used by text, HTML and chart builders."""
        import calendar
        from zoneinfo import ZoneInfo

        tz = ZoneInfo("Europe/Berlin")
        now = datetime.now(tz)
        last_month_end = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        last_month_start = (last_month_end - timedelta(days=1)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        prev_month_start = (last_month_start - timedelta(days=1)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        month_label = last_month_start.strftime("%B %Y")
        days_in_month = calendar.monthrange(last_month_start.year, last_month_start.month)[1]

        unit_price = 0.30
        try:
            unit_price = float(self.cfg.pricing.unit_price_gross())
        except Exception:
            pass

        ms_ts = int(last_month_start.timestamp())
        me_ts = int(last_month_end.timestamp())
        ps_ts = int(prev_month_start.timestamp())

        total_kwh = 0.0
        total_prev = 0.0
        dev_data: List[Dict[str, Any]] = []
        # day number (1..N) -> kwh
        daily_totals: Dict[int, float] = {}
        # weekday 0=Mon..6=Sun -> list of daily kwh
        weekday_totals: Dict[int, List[float]] = {i: [] for i in range(7)}
        # hour-of-day 0..23 -> cumulative kwh across the whole month
        hour_totals = [0.0] * 24

        for d in (self.cfg.devices or []):
            if str(getattr(d, "kind", "em")) == "switch":
                continue
            kwh = self._query_device_kwh(d.key, ms_ts, me_ts)
            prev = self._query_device_kwh(d.key, ps_ts, ms_ts)
            total_kwh += kwh
            total_prev += prev
            if kwh > 0:
                dev_data.append({
                    "name": d.name, "kwh": kwh, "prev": prev,
                    "cost": kwh * unit_price, "share_pct": 0.0, "delta_pct": None,
                })

            hdf = self._query_device_hourly(d.key, ms_ts, me_ts)
            if hdf is not None and not hdf.empty:
                for _, row in hdf.iterrows():
                    try:
                        h_ts = int(row.get("hour_ts", 0))
                        dt_row = datetime.fromtimestamp(h_ts, tz=tz)
                        kwh_row = float(row.get("kwh", 0) or 0)
                        daily_totals[dt_row.day] = daily_totals.get(dt_row.day, 0.0) + kwh_row
                        hour_totals[dt_row.hour] += kwh_row
                    except Exception:
                        pass

        # Daily sums → weekday bucket
        for day_num, day_kwh in daily_totals.items():
            try:
                dt_day = last_month_start.replace(day=day_num)
                weekday_totals[dt_day.weekday()].append(day_kwh)
            except Exception:
                pass

        dev_data.sort(key=lambda x: x["kwh"], reverse=True)
        for dd in dev_data:
            dd["share_pct"] = (dd["kwh"] / total_kwh * 100) if total_kwh > 0 else 0.0
            if dd["prev"] > 0:
                dd["delta_pct"] = ((dd["kwh"] - dd["prev"]) / dd["prev"]) * 100

        total_cost = total_kwh * unit_price
        prev_total_cost = total_prev * unit_price
        avg_daily = total_kwh / days_in_month if days_in_month else 0.0
        avg_daily_cost = avg_daily * unit_price

        best_day = worst_day = None
        best_day_kwh = worst_day_kwh = 0.0
        if daily_totals:
            nonzero = {d: v for d, v in daily_totals.items() if v > 0}
            if nonzero:
                best_day = min(nonzero, key=nonzero.get)
                worst_day = max(nonzero, key=nonzero.get)
                best_day_kwh = nonzero[best_day]
                worst_day_kwh = nonzero[worst_day]

        # Weekday averages
        wd_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        weekday_avgs: List[float] = []
        for i in range(7):
            vals = weekday_totals[i]
            weekday_avgs.append(sum(vals) / len(vals) if vals else 0.0)
        # Weekday vs weekend ratio
        wkday_vals = [v for i in range(5) for v in weekday_totals[i]]
        wkend_vals = [v for i in (5, 6) for v in weekday_totals[i]]
        wkday_avg = sum(wkday_vals) / len(wkday_vals) if wkday_vals else 0.0
        wkend_avg = sum(wkend_vals) / len(wkend_vals) if wkend_vals else 0.0

        # Peak hour of day (summed across all days of month)
        peak_hour_idx = -1
        peak_hour_kwh = 0.0
        if any(v > 0 for v in hour_totals):
            peak_hour_idx = max(range(24), key=lambda h: hour_totals[h])
            peak_hour_kwh = hour_totals[peak_hour_idx]

        # Biggest mover (abs kWh change vs previous month)
        biggest_mover: Optional[Dict[str, Any]] = None
        for dd in dev_data:
            if dd["prev"] > 0:
                diff = dd["kwh"] - dd["prev"]
                if biggest_mover is None or abs(diff) > abs(biggest_mover.get("diff", 0)):
                    biggest_mover = {"name": dd["name"], "diff": diff, "from": dd["prev"], "to": dd["kwh"]}

        # CO2
        co2_kg = 0.0
        co2_g_per_kwh = 0.0
        try:
            co2_g_per_kwh = float(getattr(self.cfg.pricing, "co2_intensity_g_per_kwh", 380) or 380)
            co2_kg = total_kwh * co2_g_per_kwh / 1000
        except Exception:
            pass

        year_proj = avg_daily * 365
        year_cost = year_proj * unit_price

        return {
            "tz": tz,
            "month_label": month_label,
            "days_in_month": days_in_month,
            "last_month_start": last_month_start,
            "last_month_end": last_month_end,
            "ms_ts": ms_ts,
            "me_ts": me_ts,
            "unit_price": unit_price,
            "total_kwh": total_kwh,
            "total_cost": total_cost,
            "total_prev": total_prev,
            "prev_total_cost": prev_total_cost,
            "dev_data": dev_data,
            "daily_totals": daily_totals,
            "weekday_avgs": weekday_avgs,
            "wd_labels": wd_labels,
            "wkday_avg": wkday_avg,
            "wkend_avg": wkend_avg,
            "hour_totals": hour_totals,
            "peak_hour_idx": peak_hour_idx,
            "peak_hour_kwh": peak_hour_kwh,
            "avg_daily": avg_daily,
            "avg_daily_cost": avg_daily_cost,
            "best_day": best_day,
            "best_day_kwh": best_day_kwh,
            "worst_day": worst_day,
            "worst_day_kwh": worst_day_kwh,
            "co2_kg": co2_kg,
            "co2_g_per_kwh": co2_g_per_kwh,
            "year_proj": year_proj,
            "year_cost": year_cost,
            "biggest_mover": biggest_mover,
        }

    def _build_monthly_summary(self) -> str:
        data = self._build_monthly_data()
        return self._format_monthly_text(data)

    def _format_monthly_text(self, data: Dict[str, Any]) -> str:
        lines: List[str] = [f"📊 Monthly report – {data['month_label']}", ""]

        # Headline
        lines.append(f"🔋 Total: {data['total_kwh']:.1f} kWh | {data['total_cost']:.2f} €")
        if data["total_prev"] > 0:
            pct = ((data["total_kwh"] - data["total_prev"]) / data["total_prev"]) * 100
            diff_kwh = data["total_kwh"] - data["total_prev"]
            diff_cost = diff_kwh * data["unit_price"]
            arrow = "📈" if pct > 5 else "📉" if pct < -5 else "➡️"
            lines.append(f"{arrow} vs. previous month: {pct:+.1f}% ({diff_kwh:+.1f} kWh | {diff_cost:+.2f} €)")
        lines.append(f"📅 Daily average: {data['avg_daily']:.1f} kWh | {data['avg_daily_cost']:.2f} €")
        lines.append("")

        # Device ranking
        lines.append("⚡ Device ranking:")
        for i, dd in enumerate(data["dev_data"]):
            medal = ["🥇", "🥈", "🥉"][i] if i < 3 else f"  {i + 1}."
            delta = ""
            if dd["delta_pct"] is not None:
                p = dd["delta_pct"]
                arrow = "📈" if p > 5 else "📉" if p < -5 else "➡️"
                delta = f" {arrow}{p:+.0f}%"
            lines.append(
                f"{medal} {dd['name']}: {dd['kwh']:.1f} kWh | {dd['cost']:.2f} € | {dd['share_pct']:.0f}%{delta}"
            )
        if not data["dev_data"]:
            lines.append("  No data available.")
        lines.append("")

        # Biggest mover
        mover = data["biggest_mover"]
        if mover and abs(mover["diff"]) * data["unit_price"] >= 1.0:
            arrow = "📈" if mover["diff"] > 0 else "📉"
            lines.append(
                f"{arrow} Biggest mover: {mover['name']} "
                f"{mover['from']:.0f} → {mover['to']:.0f} kWh "
                f"({mover['diff']:+.0f} kWh, {mover['diff'] * data['unit_price']:+.2f} €)"
            )

        # Best / worst day
        if data["best_day"] is not None:
            lines.append(f"✅ Best day: {data['best_day']:02d}. ({data['best_day_kwh']:.1f} kWh)")
        if data["worst_day"] is not None:
            lines.append(f"❌ Worst day: {data['worst_day']:02d}. ({data['worst_day_kwh']:.1f} kWh)")

        # Weekday avgs + weekend vs weekday
        if any(v > 0 for v in data["weekday_avgs"]):
            wd = data["weekday_avgs"]
            parts = " | ".join(f"{lbl} {v:.1f}" for lbl, v in zip(data["wd_labels"], wd))
            lines.append(f"📆 Weekday avg (kWh): {parts}")
            if data["wkday_avg"] > 0 and data["wkend_avg"] > 0:
                diff_pct = ((data["wkend_avg"] - data["wkday_avg"]) / data["wkday_avg"]) * 100
                icon = "🏡" if diff_pct > 0 else "🏢"
                lines.append(
                    f"{icon} Weekend vs. weekday: {data['wkend_avg']:.1f} vs. {data['wkday_avg']:.1f} kWh "
                    f"({diff_pct:+.0f}%)"
                )

        # Peak hour of day (aggregated)
        if data["peak_hour_idx"] >= 0:
            lines.append(
                f"⏰ Busiest hour of day: {data['peak_hour_idx']:02d}:00 "
                f"({data['peak_hour_kwh']:.1f} kWh over {data['days_in_month']} days)"
            )

        # CO2
        if data["co2_kg"] > 0:
            lines.append(f"🌍 CO₂: {data['co2_kg']:.1f} kg | {data['co2_kg'] / 22:.0f} tree-days")

        # Year projection
        if data["year_proj"] > 0:
            lines.append(f"📆 Year projection: ~{data['year_proj']:.0f} kWh | ~{data['year_cost']:.0f} €")

        # Mini daily bar chart (text)
        daily_totals = data["daily_totals"]
        if daily_totals:
            lines.append("")
            lines.append("📊 Daily profile:")
            max_d = max(daily_totals.values()) or 1
            for d in sorted(daily_totals.keys()):
                v = daily_totals[d]
                bar_len = int(v / max_d * 16)
                bar = "█" * bar_len + "░" * (16 - bar_len)
                lines.append(f"  {d:02d}. {bar} {v:.1f}")

        return "\n".join(lines)

    # ── HTML email body builders ──────────────────────────────────────

    _HTML_PALETTE = [
        "#3b82f6", "#ef4444", "#f59e0b", "#22c55e", "#8b5cf6",
        "#ec4899", "#14b8a6", "#f97316", "#a78bfa", "#facc15",
    ]

    def _html_card(self, title: str, value: str, sub: str = "", color: str = "#6aa7ff") -> str:
        return (
            '<td style="padding:6px">'
            '<div style="background:#1a2332;border-left:3px solid ' + color + ';'
            'border-radius:6px;padding:10px 12px;min-width:120px">'
            '<div style="font-size:10px;color:#9fb0c3;text-transform:uppercase;letter-spacing:0.5px">' + self._html_esc(title) + '</div>'
            '<div style="font-size:18px;color:#ffffff;font-weight:700;margin-top:2px">' + self._html_esc(value) + '</div>'
            + ('<div style="font-size:10px;color:#9fb0c3;margin-top:1px">' + self._html_esc(sub) + '</div>' if sub else '')
            + '</div></td>'
        )

    @staticmethod
    def _html_esc(s) -> str:
        import html as _html
        return _html.escape(str(s))

    def _html_header(self, title: str, subtitle: str) -> str:
        return (
            '<tr><td style="background:linear-gradient(135deg,#6aa7ff 0%,#8b5cf6 100%);'
            'padding:18px 24px;border-radius:10px 10px 0 0">'
            '<div style="font-size:22px;font-weight:700;color:#ffffff">⚡ ' + self._html_esc(title) + '</div>'
            '<div style="font-size:12px;color:rgba(255,255,255,0.88);margin-top:2px">' + self._html_esc(subtitle) + '</div>'
            '</td></tr>'
        )

    def _html_arrow(self, pct: float) -> str:
        if pct > 5:
            return '<span style="color:#ef4444">▲ {:+.1f}%</span>'.format(pct)
        if pct < -5:
            return '<span style="color:#22c55e">▼ {:+.1f}%</span>'.format(pct)
        return '<span style="color:#9fb0c3">➡ {:+.1f}%</span>'.format(pct)

    def _format_daily_html(self, data: Dict[str, Any], chart_cid: Optional[str] = None) -> str:
        """Render the daily report as a styled HTML email body."""
        esc = self._html_esc
        unit_price = data["unit_price"]

        # Build per-device rows
        dev_rows = []
        for i, dd in enumerate(data["dev_data"]):
            color = self._HTML_PALETTE[i % len(self._HTML_PALETTE)]
            delta_cell = "—"
            if dd["delta_pct"] is not None:
                delta_cell = self._html_arrow(dd["delta_pct"])
            peak_cell = "—" if dd["peak_hour"] < 0 else f"{dd['peak_w']:.0f} W @ {dd['peak_hour']:02d}:00"
            dev_rows.append(
                '<tr>'
                '<td style="padding:6px 8px;border-bottom:1px solid #1e2937">'
                '<span style="display:inline-block;width:10px;height:10px;background:' + color + ';border-radius:2px;margin-right:6px"></span>'
                + esc(dd["name"]) + '</td>'
                '<td style="padding:6px 8px;border-bottom:1px solid #1e2937;text-align:right;color:#e8eef6">' + f"{dd['kwh']:.2f} kWh" + '</td>'
                '<td style="padding:6px 8px;border-bottom:1px solid #1e2937;text-align:right;color:#e8eef6">' + f"{dd['cost']:.2f} €" + '</td>'
                '<td style="padding:6px 8px;border-bottom:1px solid #1e2937;text-align:right;color:#9fb0c3">' + f"{dd['share_pct']:.0f}%" + '</td>'
                '<td style="padding:6px 8px;border-bottom:1px solid #1e2937;text-align:right">' + delta_cell + '</td>'
                '<td style="padding:6px 8px;border-bottom:1px solid #1e2937;text-align:right;color:#9fb0c3">' + esc(peak_cell) + '</td>'
                '</tr>'
            )

        # KPI cards
        total_line = f"{data['total_kwh']:.2f} kWh"
        cost_line = f"{data['total_cost']:.2f} €"
        vs_prev_sub = ""
        if data["total_prev"] > 0:
            pct = ((data["total_kwh"] - data["total_prev"]) / data["total_prev"]) * 100
            icon = "▲" if pct > 0 else "▼"
            vs_prev_sub = f"{icon} {pct:+.1f}% vs. yesterday"
        peak_sub = ""
        if data["peak_h"] >= 0:
            peak_sub = f"peak {data['peak_h']:02d}:00 ({data['peak_h_kwh']:.2f} kWh)"

        kpis = (
            '<tr><td><table cellpadding="0" cellspacing="0" style="width:100%"><tr>'
            + self._html_card("Total energy", total_line, vs_prev_sub, "#6aa7ff")
            + self._html_card("Total cost", cost_line, f"@ {unit_price*100:.1f} ct/kWh", "#22c55e")
            + self._html_card("Max power", f"{data['max_power_w']:.0f} W", f"avg {data['avg_w']:.0f} W · load {data['load_factor']*100:.0f}%", "#f59e0b")
            + self._html_card("CO₂", f"{data['co2_kg']:.1f} kg" if data["co2_kg"] > 0 else "–", f"{data['co2_g_per_kwh']:.0f} g/kWh grid" if data['co2_g_per_kwh'] > 0 else "", "#8b5cf6")
            + '</tr></table></td></tr>'
        )

        extras = []
        if data["standby_w"] > 0:
            extras.append(
                '<tr><td style="padding:8px 16px;color:#9fb0c3;font-size:12px">🌙 <b style="color:#e8eef6">Night base load:</b> '
                f"~{data['standby_w']:.0f} W  (~{data['standby_annual_kwh']:.0f} kWh/year, ~{data['standby_annual_cost']:.0f} €/year)"
                '</td></tr>'
            )
        if data["top_hours"]:
            tops = ", ".join(f"{h:02d}h ({v:.2f})" for h, v in data["top_hours"])
            extras.append(
                '<tr><td style="padding:4px 16px;color:#9fb0c3;font-size:12px">🏆 <b style="color:#e8eef6">Top 3 hours:</b> ' + esc(tops) + '</td></tr>'
            )
        if data["biggest_mover"]:
            m = data["biggest_mover"]
            sign = "+" if m["diff"] > 0 else ""
            icon = "📈" if m["diff"] > 0 else "📉"
            extras.append(
                '<tr><td style="padding:4px 16px;color:#9fb0c3;font-size:12px">'
                + icon + ' <b style="color:#e8eef6">Biggest mover:</b> ' + esc(m["name"])
                + f" {m['from']:.1f} → {m['to']:.1f} kWh ({sign}{m['diff']:.1f})"
                + '</td></tr>'
            )
        if data["spot_cost"] is not None:
            delta = data["spot_cost"] - data["total_cost"]
            icon = "💚" if delta < 0 else "💔" if delta > 0 else "➡"
            extras.append(
                '<tr><td style="padding:4px 16px;color:#9fb0c3;font-size:12px">'
                + icon + f' <b style="color:#e8eef6">Spot cost today:</b> {data["spot_cost"]:.2f} €  '
                f'(<span style="color:{"#22c55e" if delta < 0 else "#ef4444"}">{delta:+.2f} €</span> vs. fixed)'
                '</td></tr>'
            )
        if data["proj_kwh"] > 0:
            extras.append(
                '<tr><td style="padding:4px 16px 12px 16px;color:#9fb0c3;font-size:12px">📅 <b style="color:#e8eef6">Month projection:</b> '
                f"~{data['proj_kwh']:.0f} kWh | ~{data['proj_cost']:.0f} €"
                '</td></tr>'
            )

        chart_row = ""
        if chart_cid:
            chart_row = (
                '<tr><td style="padding:10px 16px">'
                '<img src="cid:' + esc(chart_cid) + '" alt="chart" style="width:100%;max-width:780px;border-radius:8px;display:block">'
                '</td></tr>'
            )

        return (
            '<!doctype html><html><body style="margin:0;padding:0;background:#0b0f14;'
            'font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,sans-serif;color:#e8eef6">'
            '<table cellpadding="0" cellspacing="0" style="width:100%;max-width:820px;margin:0 auto;background:#121821;border-radius:10px;border:1px solid #1e2937">'
            + self._html_header("Daily Energy Report", data["date_label"])
            + kpis
            + chart_row
            + '<tr><td style="padding:14px 16px 6px 16px;font-size:13px;color:#e8eef6;font-weight:700">⚡ Devices</td></tr>'
            + '<tr><td style="padding:0 16px">'
            + '<table cellpadding="0" cellspacing="0" style="width:100%;font-size:12px">'
            + '<thead><tr><th style="text-align:left;color:#9fb0c3;padding:6px 8px;border-bottom:1px solid #2a3542;font-weight:600">Name</th>'
            + '<th style="text-align:right;color:#9fb0c3;padding:6px 8px;border-bottom:1px solid #2a3542;font-weight:600">kWh</th>'
            + '<th style="text-align:right;color:#9fb0c3;padding:6px 8px;border-bottom:1px solid #2a3542;font-weight:600">Cost</th>'
            + '<th style="text-align:right;color:#9fb0c3;padding:6px 8px;border-bottom:1px solid #2a3542;font-weight:600">Share</th>'
            + '<th style="text-align:right;color:#9fb0c3;padding:6px 8px;border-bottom:1px solid #2a3542;font-weight:600">Δ day-1</th>'
            + '<th style="text-align:right;color:#9fb0c3;padding:6px 8px;border-bottom:1px solid #2a3542;font-weight:600">Peak</th>'
            + '</tr></thead><tbody>'
            + "".join(dev_rows)
            + '</tbody></table></td></tr>'
            + "".join(extras)
            + '<tr><td style="padding:14px 16px;border-top:1px solid #1e2937;background:#0f1520;font-size:10px;color:#6a7a8a;text-align:center">'
            'Shelly Energy Analyzer · generated ' + datetime.now().strftime("%Y-%m-%d %H:%M")
            + '</td></tr>'
            '</table></body></html>'
        )

    def _format_monthly_html(self, data: Dict[str, Any], chart_cid: Optional[str] = None) -> str:
        """Render the monthly report as a styled HTML email body."""
        esc = self._html_esc

        # Device ranking rows
        dev_rows = []
        medals = ["🥇", "🥈", "🥉"]
        for i, dd in enumerate(data["dev_data"]):
            color = self._HTML_PALETTE[i % len(self._HTML_PALETTE)]
            medal = medals[i] if i < 3 else f"{i + 1}."
            delta_cell = "—"
            if dd["delta_pct"] is not None:
                delta_cell = self._html_arrow(dd["delta_pct"])
            dev_rows.append(
                '<tr>'
                '<td style="padding:6px 8px;border-bottom:1px solid #1e2937;width:30px;text-align:center">' + medal + '</td>'
                '<td style="padding:6px 8px;border-bottom:1px solid #1e2937">'
                '<span style="display:inline-block;width:10px;height:10px;background:' + color + ';border-radius:2px;margin-right:6px"></span>'
                + esc(dd["name"]) + '</td>'
                '<td style="padding:6px 8px;border-bottom:1px solid #1e2937;text-align:right;color:#e8eef6">' + f"{dd['kwh']:.1f} kWh" + '</td>'
                '<td style="padding:6px 8px;border-bottom:1px solid #1e2937;text-align:right;color:#e8eef6">' + f"{dd['cost']:.2f} €" + '</td>'
                '<td style="padding:6px 8px;border-bottom:1px solid #1e2937;text-align:right;color:#9fb0c3">' + f"{dd['share_pct']:.0f}%" + '</td>'
                '<td style="padding:6px 8px;border-bottom:1px solid #1e2937;text-align:right">' + delta_cell + '</td>'
                '</tr>'
            )

        # KPI cards
        total_line = f"{data['total_kwh']:.0f} kWh"
        cost_line = f"{data['total_cost']:.2f} €"
        vs_prev_sub = ""
        if data["total_prev"] > 0:
            pct = ((data["total_kwh"] - data["total_prev"]) / data["total_prev"]) * 100
            icon = "▲" if pct > 0 else "▼"
            vs_prev_sub = f"{icon} {pct:+.1f}% vs. prev. month"

        kpis = (
            '<tr><td><table cellpadding="0" cellspacing="0" style="width:100%"><tr>'
            + self._html_card("Total energy", total_line, vs_prev_sub, "#6aa7ff")
            + self._html_card("Total cost", cost_line, f"@ {data['unit_price']*100:.1f} ct/kWh", "#22c55e")
            + self._html_card("Daily average", f"{data['avg_daily']:.1f} kWh", f"{data['avg_daily_cost']:.2f} €/day", "#f59e0b")
            + self._html_card("CO₂", f"{data['co2_kg']:.1f} kg" if data["co2_kg"] > 0 else "–", f"{data['co2_kg'] / 22:.0f} tree-days" if data["co2_kg"] > 0 else "", "#8b5cf6")
            + '</tr></table></td></tr>'
        )

        extras = []
        if data["best_day"] is not None and data["worst_day"] is not None:
            extras.append(
                '<tr><td style="padding:8px 16px;color:#9fb0c3;font-size:12px">'
                '✅ <b style="color:#22c55e">Best day:</b> ' + f"{data['best_day']:02d}." + f" ({data['best_day_kwh']:.1f} kWh)"
                + '   ❌ <b style="color:#ef4444">Worst day:</b> ' + f"{data['worst_day']:02d}." + f" ({data['worst_day_kwh']:.1f} kWh)"
                + '</td></tr>'
            )
        if data["wkday_avg"] > 0 and data["wkend_avg"] > 0:
            diff_pct = ((data["wkend_avg"] - data["wkday_avg"]) / data["wkday_avg"]) * 100
            icon = "🏡" if diff_pct > 0 else "🏢"
            extras.append(
                '<tr><td style="padding:4px 16px;color:#9fb0c3;font-size:12px">'
                + icon + ' <b style="color:#e8eef6">Weekend vs. weekday:</b> '
                + f"{data['wkend_avg']:.1f} vs. {data['wkday_avg']:.1f} kWh ({diff_pct:+.0f}%)"
                + '</td></tr>'
            )
        if data["biggest_mover"]:
            m = data["biggest_mover"]
            icon = "📈" if m["diff"] > 0 else "📉"
            extras.append(
                '<tr><td style="padding:4px 16px;color:#9fb0c3;font-size:12px">'
                + icon + ' <b style="color:#e8eef6">Biggest mover:</b> ' + esc(m["name"])
                + f" {m['from']:.0f} → {m['to']:.0f} kWh ({m['diff']:+.0f})"
                + '</td></tr>'
            )
        if data["peak_hour_idx"] >= 0:
            extras.append(
                '<tr><td style="padding:4px 16px;color:#9fb0c3;font-size:12px">⏰ <b style="color:#e8eef6">Busiest hour of day:</b> '
                f"{data['peak_hour_idx']:02d}:00 ({data['peak_hour_kwh']:.1f} kWh over {data['days_in_month']} days)"
                + '</td></tr>'
            )
        if data["year_proj"] > 0:
            extras.append(
                '<tr><td style="padding:4px 16px 12px 16px;color:#9fb0c3;font-size:12px">📆 <b style="color:#e8eef6">Year projection:</b> '
                f"~{data['year_proj']:.0f} kWh | ~{data['year_cost']:.0f} €"
                + '</td></tr>'
            )

        chart_row = ""
        if chart_cid:
            chart_row = (
                '<tr><td style="padding:10px 16px">'
                '<img src="cid:' + esc(chart_cid) + '" alt="chart" style="width:100%;max-width:780px;border-radius:8px;display:block">'
                '</td></tr>'
            )

        return (
            '<!doctype html><html><body style="margin:0;padding:0;background:#0b0f14;'
            'font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,sans-serif;color:#e8eef6">'
            '<table cellpadding="0" cellspacing="0" style="width:100%;max-width:820px;margin:0 auto;background:#121821;border-radius:10px;border:1px solid #1e2937">'
            + self._html_header("Monthly Energy Report", data["month_label"])
            + kpis
            + chart_row
            + '<tr><td style="padding:14px 16px 6px 16px;font-size:13px;color:#e8eef6;font-weight:700">⚡ Device ranking</td></tr>'
            + '<tr><td style="padding:0 16px">'
            + '<table cellpadding="0" cellspacing="0" style="width:100%;font-size:12px">'
            + '<thead><tr>'
            + '<th style="padding:6px 8px;border-bottom:1px solid #2a3542"></th>'
            + '<th style="text-align:left;color:#9fb0c3;padding:6px 8px;border-bottom:1px solid #2a3542;font-weight:600">Name</th>'
            + '<th style="text-align:right;color:#9fb0c3;padding:6px 8px;border-bottom:1px solid #2a3542;font-weight:600">kWh</th>'
            + '<th style="text-align:right;color:#9fb0c3;padding:6px 8px;border-bottom:1px solid #2a3542;font-weight:600">Cost</th>'
            + '<th style="text-align:right;color:#9fb0c3;padding:6px 8px;border-bottom:1px solid #2a3542;font-weight:600">Share</th>'
            + '<th style="text-align:right;color:#9fb0c3;padding:6px 8px;border-bottom:1px solid #2a3542;font-weight:600">Δ month-1</th>'
            + '</tr></thead><tbody>'
            + "".join(dev_rows)
            + '</tbody></table></td></tr>'
            + "".join(extras)
            + '<tr><td style="padding:14px 16px;border-top:1px solid #1e2937;background:#0f1520;font-size:10px;color:#6a7a8a;text-align:center">'
            'Shelly Energy Analyzer · generated ' + datetime.now().strftime("%Y-%m-%d %H:%M")
            + '</td></tr>'
            '</table></body></html>'
        )

    # Color palette shared by every chart subplot.
    _CHART_PALETTE = [
        "#3b82f6", "#ef4444", "#f59e0b", "#22c55e", "#8b5cf6",
        "#ec4899", "#14b8a6", "#f97316", "#a78bfa", "#facc15",
    ]

    def _style_dark_ax(self, ax) -> None:
        ax.set_facecolor("#0b0f14")
        ax.tick_params(colors="#9fb0c3", labelsize=7)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        for spine in ax.spines.values():
            spine.set_color("#333")

    def _generate_summary_chart(self, chart_type: str, data: Optional[Dict[str, Any]] = None) -> Optional[Path]:
        """Generate a 4-panel summary chart as PNG. Returns path or None.

        Daily: stacked hourly per device, device totals bar, cumulative
        cost curve, 24h power profile.

        Monthly: daily stacked vs average, device pie, weekday profile,
        hour-of-day aggregate.
        """
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt

            if chart_type == "daily":
                data = data if data is not None else self._build_daily_data()
                return self._render_daily_chart(data, plt)
            if chart_type == "monthly":
                data = data if data is not None else self._build_monthly_data()
                return self._render_monthly_chart(data, plt)
            return None
        except Exception as e:
            logger.warning("Chart generation failed: %s", e)
            return None

    def _render_daily_chart(self, data: Dict[str, Any], plt) -> Optional[Path]:
        fig, axes = plt.subplots(2, 2, figsize=(11, 7), facecolor="#121821")
        for row in axes:
            for ax in row:
                self._style_dark_ax(ax)

        dev_hourly = data["hourly_per_device"]
        dev_names = [dd["name"] for dd in data["dev_data"]]
        unit_price = data["unit_price"]
        hours = list(range(24))
        palette = self._CHART_PALETTE

        # ── (0,0) Stacked hourly per device ──
        ax1 = axes[0][0]
        bottom = [0.0] * 24
        for i, name in enumerate(dev_names):
            vals = dev_hourly.get(name, [0.0] * 24)
            ax1.bar(hours, vals, bottom=bottom, color=palette[i % len(palette)], label=name, width=0.88)
            bottom = [b + v for b, v in zip(bottom, vals)]
        ax1.set_title(
            f"Hourly – {data['yesterday'].strftime('%a %d.%m.%Y')}",
            color="#e8eef6", fontsize=11, fontweight="bold",
        )
        ax1.set_xlabel("Hour", color="#9fb0c3", fontsize=9)
        ax1.set_ylabel("kWh", color="#9fb0c3", fontsize=9)
        if dev_names:
            ax1.legend(fontsize=6, facecolor="#121821", edgecolor="#333", labelcolor="#e8eef6", loc="upper left")

        # ── (0,1) Device totals bar with €/kWh labels ──
        ax2 = axes[0][1]
        if dev_names:
            kwh_vals = [dd["kwh"] for dd in data["dev_data"]]
            x = range(len(dev_names))
            bars = ax2.bar(x, kwh_vals, color=[palette[i % len(palette)] for i in range(len(dev_names))], width=0.6)
            ax2.set_xticks(list(x))
            ax2.set_xticklabels([n[:12] for n in dev_names], rotation=30, ha="right", fontsize=7)
            ax2.set_ylabel("kWh", color="#9fb0c3", fontsize=9)
            ax2.set_title("Device consumption", color="#e8eef6", fontsize=11, fontweight="bold")
            for bar, kwh in zip(bars, kwh_vals):
                cost = kwh * unit_price
                ax2.text(
                    bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + 0.02,
                    f"{kwh:.2f} kWh\n{cost:.2f} €",
                    ha="center", va="bottom", color="#e8eef6", fontsize=6,
                )

        # ── (1,0) Cumulative cost curve ──
        ax3 = axes[1][0]
        cum_cost = []
        running = 0.0
        for h in range(24):
            running += data["hourly_total"][h] * unit_price
            cum_cost.append(running)
        ax3.fill_between(hours, cum_cost, color="#22c55e", alpha=0.25)
        ax3.plot(hours, cum_cost, color="#22c55e", linewidth=2)
        ax3.set_title(
            f"Cumulative cost – total {data['total_cost']:.2f} €",
            color="#e8eef6", fontsize=11, fontweight="bold",
        )
        ax3.set_xlabel("Hour", color="#9fb0c3", fontsize=9)
        ax3.set_ylabel("€", color="#9fb0c3", fontsize=9)
        if data["peak_h"] >= 0:
            ax3.axvline(data["peak_h"], color="#ef4444", linestyle="--", linewidth=0.8, alpha=0.6)
            ax3.text(
                data["peak_h"], cum_cost[data["peak_h"]] * 0.95,
                f"peak {data['peak_h']:02d}h",
                color="#ef4444", fontsize=7, ha="right", va="top",
            )

        # ── (1,1) Total-kWh profile vs night base load ──
        ax4 = axes[1][1]
        vals24 = data["hourly_total"]
        bar_colors = []
        max_v = max(vals24) if any(v > 0 for v in vals24) else 1.0
        for v in vals24:
            if v >= max_v * 0.8:
                bar_colors.append("#ef4444")
            elif v >= max_v * 0.5:
                bar_colors.append("#f59e0b")
            else:
                bar_colors.append("#3b82f6")
        ax4.bar(hours, vals24, color=bar_colors, width=0.85)
        if data["standby_w"] > 0:
            standby_kwh = data["standby_w"] / 1000.0
            ax4.axhline(standby_kwh, color="#a78bfa", linestyle="--", linewidth=0.8,
                        label=f"night base {data['standby_w']:.0f} W")
            ax4.legend(fontsize=6, facecolor="#121821", edgecolor="#333", labelcolor="#e8eef6", loc="upper left")
        ax4.set_title("Total hourly profile", color="#e8eef6", fontsize=11, fontweight="bold")
        ax4.set_xlabel("Hour", color="#9fb0c3", fontsize=9)
        ax4.set_ylabel("kWh", color="#9fb0c3", fontsize=9)

        plt.suptitle(
            f"Shelly Energy Analyzer · Daily Report · {data['date_label']}",
            color="#e8eef6", fontsize=13, fontweight="bold", y=0.995,
        )
        plt.tight_layout(pad=1.2, rect=(0, 0, 1, 0.96))
        out = self.out_dir / "data" / "runtime" / "summary_daily.png"
        out.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(str(out), dpi=150, bbox_inches="tight", facecolor="#121821")
        plt.close(fig)
        return out

    def _render_monthly_chart(self, data: Dict[str, Any], plt) -> Optional[Path]:
        fig, axes = plt.subplots(2, 2, figsize=(11, 7), facecolor="#121821")
        for row in axes:
            for ax in row:
                self._style_dark_ax(ax)

        daily_totals = data["daily_totals"]
        palette = self._CHART_PALETTE

        # ── (0,0) Daily bars vs average ──
        ax1 = axes[0][0]
        if daily_totals:
            days = sorted(daily_totals.keys())
            vals = [daily_totals[d] for d in days]
            avg = sum(vals) / len(vals) if vals else 0
            bar_colors = ["#dc2626" if v > avg * 1.3 else "#f59e0b" if v > avg else "#3b82f6" for v in vals]
            ax1.bar(days, vals, color=bar_colors, width=0.75)
            ax1.axhline(y=avg, color="#22c55e", linestyle="--", linewidth=1, label=f"Avg {avg:.1f} kWh")
            ax1.legend(fontsize=7, facecolor="#121821", edgecolor="#333", labelcolor="#e8eef6", loc="upper right")
        ax1.set_title(
            f"Daily – {data['month_label']}",
            color="#e8eef6", fontsize=11, fontweight="bold",
        )
        ax1.set_xlabel("Day", color="#9fb0c3", fontsize=9)
        ax1.set_ylabel("kWh", color="#9fb0c3", fontsize=9)

        # ── (0,1) Device share (donut) ──
        ax2 = axes[0][1]
        dev_data = data["dev_data"]
        if dev_data:
            names = [dd["name"][:14] for dd in dev_data]
            kwh_vals = [dd["kwh"] for dd in dev_data]
            wedges, _, autotexts = ax2.pie(
                kwh_vals, labels=names, autopct="%1.0f%%",
                colors=[palette[i % len(palette)] for i in range(len(dev_data))],
                wedgeprops={"width": 0.4, "edgecolor": "#0b0f14"},
                textprops={"color": "#e8eef6", "fontsize": 7},
            )
            for at in autotexts:
                at.set_fontsize(7)
                at.set_color("#ffffff")
            ax2.set_title(
                f"Device share – {data['total_kwh']:.0f} kWh",
                color="#e8eef6", fontsize=11, fontweight="bold",
            )

        # ── (1,0) Weekday profile ──
        ax3 = axes[1][0]
        wd = data["weekday_avgs"]
        if any(v > 0 for v in wd):
            colors_wd = ["#3b82f6"] * 5 + ["#f97316"] * 2  # weekdays blue, weekend orange
            ax3.bar(data["wd_labels"], wd, color=colors_wd, width=0.65)
            for i, v in enumerate(wd):
                if v > 0:
                    ax3.text(i, v + max(wd) * 0.02, f"{v:.1f}", ha="center", va="bottom",
                             color="#e8eef6", fontsize=7)
        ax3.set_title("Weekday average", color="#e8eef6", fontsize=11, fontweight="bold")
        ax3.set_ylabel("kWh/day", color="#9fb0c3", fontsize=9)

        # ── (1,1) Hour-of-day aggregate across the month ──
        ax4 = axes[1][1]
        ht = data["hour_totals"]
        if any(v > 0 for v in ht):
            max_ht = max(ht)
            bar_colors = []
            for v in ht:
                if v >= max_ht * 0.8:
                    bar_colors.append("#ef4444")
                elif v >= max_ht * 0.5:
                    bar_colors.append("#f59e0b")
                else:
                    bar_colors.append("#3b82f6")
            ax4.bar(range(24), ht, color=bar_colors, width=0.85)
        ax4.set_title("Busiest hours (month-wide)", color="#e8eef6", fontsize=11, fontweight="bold")
        ax4.set_xlabel("Hour", color="#9fb0c3", fontsize=9)
        ax4.set_ylabel("kWh", color="#9fb0c3", fontsize=9)

        plt.suptitle(
            f"Shelly Energy Analyzer · Monthly Report · {data['month_label']}",
            color="#e8eef6", fontsize=13, fontweight="bold", y=0.995,
        )
        plt.tight_layout(pad=1.2, rect=(0, 0, 1, 0.96))
        out = self.out_dir / "data" / "runtime" / "summary_monthly.png"
        out.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(str(out), dpi=150, bbox_inches="tight", facecolor="#121821")
        plt.close(fig)
        return out

    def _generate_summary_pdf(self, chart_type: str, text: str) -> Optional[Path]:
        """Generate a PDF with summary text + chart. Returns path or None."""
        try:
            from reportlab.lib.pagesizes import A4
            from reportlab.lib.units import cm
            from reportlab.pdfgen import canvas as pdfcanvas
            from reportlab.lib.utils import ImageReader

            out = self.out_dir / "data" / "runtime" / f"summary_{chart_type}.pdf"
            out.parent.mkdir(parents=True, exist_ok=True)
            c = pdfcanvas.Canvas(str(out), pagesize=A4)
            w, h = A4

            # Title
            c.setFont("Helvetica-Bold", 16)
            c.drawString(2 * cm, h - 2 * cm, f"Shelly Energy Analyzer – {chart_type.title()} Report")

            # Date
            c.setFont("Helvetica", 10)
            c.setFillColorRGB(0.5, 0.5, 0.5)
            c.drawString(2 * cm, h - 2.8 * cm, datetime.now().strftime("%d.%m.%Y %H:%M"))

            # Text
            c.setFillColorRGB(0, 0, 0)
            c.setFont("Helvetica", 9)
            y = h - 4 * cm
            for line in text.split("\n"):
                # Strip emoji for PDF (reportlab can't render them)
                clean = line
                for ch in list(clean):
                    if ord(ch) > 0xFFFF:
                        clean = clean.replace(ch, "")
                if y < 3 * cm:
                    c.showPage()
                    y = h - 2 * cm
                    c.setFont("Helvetica", 9)
                c.drawString(2 * cm, y, clean.strip())
                y -= 0.4 * cm

            # Chart image
            chart_path = self.out_dir / "data" / "runtime" / f"summary_{chart_type}.png"
            if chart_path.exists():
                try:
                    img = ImageReader(str(chart_path))
                    iw, ih = img.getSize()
                    max_w = w - 4 * cm
                    scale = min(max_w / iw, 1.0)
                    draw_w = iw * scale
                    draw_h = ih * scale
                    if y - draw_h < 2 * cm:
                        c.showPage()
                        y = h - 2 * cm
                    c.drawImage(str(chart_path), 2 * cm, y - draw_h, width=draw_w, height=draw_h)
                except Exception:
                    pass

            # Footer
            c.setFont("Helvetica", 7)
            c.setFillColorRGB(0.5, 0.5, 0.5)
            c.drawString(2 * cm, 1 * cm, "\u00a9 Robert Manuwald – Shelly Energy Analyzer")

            c.save()
            return out
        except Exception as e:
            logger.warning("PDF generation failed: %s", e)
            return None

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
                        daily_data = self._build_daily_data()
                        summary = self._format_daily_text(daily_data)
                        chart_path = self._generate_summary_chart("daily", daily_data)
                        if tg_daily:
                            self._telegram_send(summary)
                            if chart_path and chart_path.exists():
                                self._telegram_send_photo(chart_path, "Daily report · charts")
                        if em_daily:
                            pdf_path = self._generate_summary_pdf("daily", summary)
                            attachments = [pdf_path] if pdf_path and pdf_path.exists() else []
                            inline = {}
                            if chart_path and chart_path.exists():
                                inline["daily-chart.png"] = chart_path
                                attachments.append(chart_path)
                            html_body = self._format_daily_html(
                                daily_data,
                                chart_cid="daily-chart.png" if inline else None,
                            )
                            self._email_send(
                                "Shelly Energy – Daily Report",
                                summary,
                                attachments=attachments if attachments else None,
                                html_body=html_body,
                                inline_images=inline if inline else None,
                            )
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
                        monthly_data = self._build_monthly_data()
                        summary = self._format_monthly_text(monthly_data)
                        chart_path = self._generate_summary_chart("monthly", monthly_data)
                        if tg_monthly:
                            self._telegram_send(summary)
                            if chart_path and chart_path.exists():
                                self._telegram_send_photo(chart_path, "Monthly report · charts")
                        if em_monthly:
                            pdf_path = self._generate_summary_pdf("monthly", summary)
                            attachments = [pdf_path] if pdf_path and pdf_path.exists() else []
                            inline = {}
                            if chart_path and chart_path.exists():
                                inline["monthly-chart.png"] = chart_path
                                attachments.append(chart_path)
                            html_body = self._format_monthly_html(
                                monthly_data,
                                chart_cid="monthly-chart.png" if inline else None,
                            )
                            self._email_send(
                                "Shelly Energy – Monthly Report",
                                summary,
                                attachments=attachments if attachments else None,
                                html_body=html_body,
                                inline_images=inline if inline else None,
                            )
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
