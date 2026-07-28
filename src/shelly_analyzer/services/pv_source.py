"""External PV / battery / grid data source.

Bridges a PV/battery system that is *not* measured by a Shelly into the
analyzer. Common inverters and batteries (Huawei, SolarEdge, Fronius, SMA,
Kostal, Sungrow, GoodWe, …) are already integrated in Home Assistant, so the
default source reads their entities over the HA REST API. Alternatively the
values can be subscribed from MQTT.

Readings are ingested as synthetic devices — ``pv``, ``battery`` and
``grid_ext`` — into the same energy database (via ``EnergyDB.insert_dataframe``,
the exact path Shelly samples use) and the in-memory live store. Every
downstream calculation (autarky, self-consumption, PV yield, sankey, battery
SOC, spot cost, CO₂) therefore works with the real production/storage data
without any change to the compute code.

Reserved synthetic device keys (do not use for Shelly devices):
    pv        – PV production power (W, ≥ 0)
    battery   – battery power (W, + = charging, − = discharging)
    grid_ext  – grid connection power (W, + = import, − = export)
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any, Callable, Dict, Optional

import requests

logger = logging.getLogger(__name__)

# Reserved synthetic device keys.
PV_KEY = "pv"
BATTERY_KEY = "battery"
GRID_KEY = "grid_ext"

# Supervisor proxy used when running as a Home Assistant add-on and no explicit
# base URL/token is configured.
_SUPERVISOR_CORE = "http://supervisor/core"

# Thread-safe snapshot of the most recent external readings. Consumers (the
# battery/solar endpoints) read this for values that are more precise live than
# what can be derived from the DB (e.g. a real state-of-charge entity).
_LATEST: Dict[str, Any] = {
    "ts": 0,
    "pv_w": None,
    "battery_w": None,      # normalised: + = charging, − = discharging
    "soc_pct": None,
    "grid_w": None,         # normalised: + = import, − = export
    "house_w": None,
    "pv_today_kwh": None,
}
_LATEST_LOCK = threading.Lock()

# Module-level snapshot of today's per-key energy accumulators (charge/discharge/
# import/export/production, kWh). Mirrors the poller's in-memory ``self._day`` so
# the live /api/state endpoint can show e.g. the battery's kWh IN and OUT today
# without holding a reference to the running PvSource instance.
_DAILY: Dict[str, Dict[str, float]] = {}
_DAILY_LOCK = threading.Lock()


def latest_readings() -> Dict[str, Any]:
    """Return a copy of the latest external readings (may contain ``None``)."""
    with _LATEST_LOCK:
        return dict(_LATEST)


def daily_readings() -> Dict[str, Dict[str, float]]:
    """Snapshot of today's per-key energy accumulators (kWh). Empty until the
    poller has integrated at least one interval."""
    with _DAILY_LOCK:
        return {k: dict(v) for k, v in _DAILY.items()}


def _store_daily(key: str, acc: Dict[str, float]) -> None:
    with _DAILY_LOCK:
        _DAILY[key] = dict(acc)


def _store_latest(**vals: Any) -> None:
    with _LATEST_LOCK:
        _LATEST.update(vals)


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f:  # NaN
        return None
    return f


def _dig_json(payload: str, path: str) -> Any:
    """Extract a value from a (possibly JSON) MQTT payload via a dotted path."""
    if not path:
        return payload
    try:
        obj: Any = json.loads(payload)
    except (ValueError, TypeError):
        return payload
    for part in path.split("."):
        if isinstance(obj, list):
            try:
                obj = obj[int(part)]
                continue
            except (ValueError, IndexError):
                return None
        if isinstance(obj, dict):
            obj = obj.get(part)
        else:
            return None
    return obj


class PvSourceService:
    """Background poller/subscriber for an external PV/battery/grid source."""

    def __init__(self, storage, live_store, get_config: Callable[[], Any]) -> None:
        self.storage = storage
        self.live_store = live_store
        self._get_config = get_config
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._trigger = threading.Event()
        self._mqtt_client = None
        # Latest raw values received over MQTT, keyed by role.
        self._mqtt_vals: Dict[str, Optional[float]] = {}
        self._mqtt_lock = threading.Lock()
        self._log_cb: Optional[Callable[[str], None]] = None
        self.last_error: Optional[str] = None
        # Per-key previous sample (ts, power_w) for trapezoidal energy
        # integration, and a per-key daily energy accumulator so the live tile
        # can show a meaningful "today" figure and so the DB hourly rollup gets
        # real interval energy (samples carry only instantaneous power).
        self._prev_sample: Dict[str, tuple] = {}
        self._day: Dict[str, Dict[str, Any]] = {}
        self._acc_lock = threading.Lock()
        # Latest cumulative energy-counter readings (kWh) from the source, keyed
        # by sub-metric (pv_total / grid_import / grid_export / batt_charge /
        # batt_discharge). Populated each tick when the user mapped counter
        # entities; drives EXACT interval energy via counter deltas.
        self._energy_counters: Dict[str, Optional[float]] = {}
        # Previous counter value per sub-metric, for delta computation.
        self._prev_counter: Dict[str, float] = {}

    def set_log_callback(self, cb: Callable[[str], None]) -> None:
        self._log_cb = cb

    def _svc_log(self, msg: str) -> None:
        logger.info(msg)
        if self._log_cb:
            try:
                self._log_cb(msg)
            except Exception:
                pass

    # ── lifecycle ──────────────────────────────────────────────────────
    def start(self) -> None:
        cfg = self._pv_cfg()
        if cfg is None or not getattr(cfg, "enabled", False):
            return
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        if str(getattr(cfg, "source_type", "homeassistant")) == "mqtt":
            self._start_mqtt(cfg)
        self._thread = threading.Thread(target=self._run, name="PvSourceService", daemon=True)
        self._thread.start()
        self._svc_log(f"PV source started (source={getattr(cfg, 'source_type', '?')})")

    def stop(self) -> None:
        self._stop.set()
        self._trigger.set()
        if self._mqtt_client is not None:
            try:
                self._mqtt_client.loop_stop()
                self._mqtt_client.disconnect()
            except Exception:
                pass
            self._mqtt_client = None
        if self._thread:
            self._thread.join(timeout=8)

    def trigger_now(self) -> None:
        self._trigger.set()

    def _pv_cfg(self):
        try:
            return getattr(self._get_config(), "pv_source", None)
        except Exception:
            return None

    # ── main loop ──────────────────────────────────────────────────────
    def _run(self) -> None:
        self._trigger.wait(3.0)
        self._trigger.clear()
        while not self._stop.is_set():
            cfg = self._pv_cfg()
            if cfg is None or not getattr(cfg, "enabled", False):
                break
            try:
                self._tick(cfg)
            except Exception as e:  # never let the loop die
                self.last_error = str(e)
                logger.exception("PV source tick error")
            interval = max(5, int(getattr(cfg, "poll_interval_seconds", 15) or 15))
            self._trigger.wait(interval)
            self._trigger.clear()

    def _tick(self, cfg) -> None:
        source = str(getattr(cfg, "source_type", "homeassistant"))
        if source == "mqtt":
            pv_w, batt_w, soc, grid_w, house_w, pv_today = self._read_mqtt(cfg)
        elif source == "modbus":
            pv_w, batt_w, soc, grid_w, house_w, pv_today = self._read_modbus(cfg)
        elif source == "http":
            pv_w, batt_w, soc, grid_w, house_w, pv_today = self._read_http(cfg)
        else:
            pv_w, batt_w, soc, grid_w, house_w, pv_today = self._read_ha(cfg)

        # Normalise sign conventions to internal (+charge / +import).
        if batt_w is not None and str(getattr(cfg, "battery_power_sign", "charge_positive")) == "discharge_positive":
            batt_w = -batt_w
        if grid_w is not None and str(getattr(cfg, "grid_power_sign", "import_positive")) == "export_positive":
            grid_w = -grid_w

        ts = int(time.time())
        _store_latest(ts=ts, pv_w=pv_w, battery_w=batt_w, soc_pct=soc,
                      grid_w=grid_w, house_w=house_w, pv_today_kwh=pv_today)

        if pv_w is not None:
            self._ingest(PV_KEY, "pv", ts, max(0.0, pv_w), kwh_today=pv_today, soc=None)
        if batt_w is not None:
            self._ingest(BATTERY_KEY, "battery", ts, batt_w, kwh_today=None, soc=soc)
        if grid_w is not None:
            self._ingest(GRID_KEY, "grid", ts, grid_w, kwh_today=None, soc=None)

    # ── energy accumulation ────────────────────────────────────────────
    def _day_acc_for(self, key: str, ts: int) -> Dict[str, float]:
        """Return today's per-key accumulator, (re)seeding it from the DB on a
        date rollover so the figure reflects the whole day and survives add-on
        restarts. Caller must hold ``self._acc_lock``."""
        today = time.strftime("%Y-%m-%d", time.localtime(ts))
        acc = self._day.get(key)
        if acc is None or acc.get("date") != today:
            acc = {"date": today, "charge": 0.0, "discharge": 0.0,
                   "import": 0.0, "export": 0.0, "production": 0.0}
            self._seed_from_db(key, ts, acc)
            self._day[key] = acc
        return acc

    def _tile_today(self, role: str, acc: Dict[str, float], power_w: float) -> float:
        """Direction-appropriate daily energy for the live tile: while charging
        show today's charged kWh, while discharging today's discharged, etc.
        (the frontend colours it green/red by the live power direction)."""
        if role == "battery":
            return acc["charge"] if float(power_w) >= 0 else acc["discharge"]
        if role == "grid":
            return acc["import"] if float(power_w) >= 0 else acc["export"]
        return acc["production"]

    def _accumulate(self, key: str, role: str, ts: int, power_w: float) -> tuple:
        """Integrate interval energy since the previous sample (trapezoidal,
        capped like the DB ingest at 10 min so a gap can't inflate energy) and
        roll it into a per-key daily accumulator.

        Returns ``(energy_kwh_signed, kwh_today)`` where ``kwh_today`` is the
        role-appropriate live-tile figure: PV → produced today (≥0); battery →
        NET today (charge − discharge, signed → + charging, − discharging);
        grid → NET import today (import − export, signed).
        """
        energy_kwh = 0.0
        with self._acc_lock:
            prev = self._prev_sample.get(key)
            self._prev_sample[key] = (int(ts), float(power_w))
            if prev is not None:
                dt = int(ts) - int(prev[0])
                if 0 < dt <= 600:
                    energy_kwh = ((float(power_w) + float(prev[1])) / 2.0) * (dt / 3600.0) / 1000.0

            acc = self._day_acc_for(key, ts)
            if energy_kwh > 0:
                acc["charge"] += energy_kwh
                acc["import"] += energy_kwh
                acc["production"] += energy_kwh
            elif energy_kwh < 0:
                acc["discharge"] += -energy_kwh
                acc["export"] += -energy_kwh
            # Mirror into the module-level snapshot so the live endpoint can read
            # today's charge/discharge (battery IN/OUT) without the instance.
            _store_daily(key, acc)
            tile = self._tile_today(role, acc, power_w)
        return energy_kwh, tile

    def _counter_delta(self, ckey: str, cur: Optional[float]) -> float:
        """Non-negative delta of a cumulative counter since the previous poll.
        Returns 0 on the first sample, on a counter reset (delta < 0, e.g. an
        inverter reboot), or on an implausible jump (> 100 kWh in one interval),
        so a discontinuity never spikes the energy total."""
        if cur is None:
            return 0.0
        prev = self._prev_counter.get(ckey)
        self._prev_counter[ckey] = float(cur)
        if prev is None:
            return 0.0
        d = float(cur) - float(prev)
        if d < 0 or d > 100:
            return 0.0
        return d

    def _role_counter_parts(self, role: str) -> Optional[Dict[str, float]]:
        """Per-sub-metric interval energy (kWh) for a role from cumulative-counter
        deltas, or ``None`` when no counter is mapped for this role (→ fall back
        to power integration). Consumes the counter deltas exactly once."""
        ec = self._energy_counters or {}
        if role == "pv":
            if ec.get("pv_total") is None:
                return None
            return {"production": self._counter_delta("pv_total", ec.get("pv_total"))}
        if role == "grid":
            if ec.get("grid_import") is None and ec.get("grid_export") is None:
                return None
            return {"import": self._counter_delta("grid_import", ec.get("grid_import")),
                    "export": self._counter_delta("grid_export", ec.get("grid_export"))}
        if role == "battery":
            if ec.get("batt_charge") is None and ec.get("batt_discharge") is None:
                return None
            return {"charge": self._counter_delta("batt_charge", ec.get("batt_charge")),
                    "discharge": self._counter_delta("batt_discharge", ec.get("batt_discharge"))}
        return None

    def _accumulate_counter(self, key: str, role: str, ts: int, power_w: float,
                            parts: Dict[str, float]) -> tuple:
        """Roll counter-delta sub-metrics into today's accumulator and return the
        SIGNED interval energy for the DB rollup (import/charge/production +,
        export/discharge −) plus the live-tile figure. This is the EXACT path:
        summed over a day it equals the source counters' daily delta."""
        with self._acc_lock:
            acc = self._day_acc_for(key, ts)
            for sub, d in parts.items():
                if d:
                    acc[sub] = acc.get(sub, 0.0) + float(d)
            _store_daily(key, acc)
            tile = self._tile_today(role, acc, power_w)
        if role == "grid":
            signed = parts.get("import", 0.0) - parts.get("export", 0.0)
        elif role == "battery":
            signed = parts.get("charge", 0.0) - parts.get("discharge", 0.0)
        else:  # pv
            signed = parts.get("production", 0.0)
        return signed, tile

    def _seed_from_db(self, key: str, ts: int, acc: Dict[str, float]) -> None:
        """Populate today's accumulator from the DB hourly rollup (local day)."""
        try:
            lt = time.localtime(ts)
            midnight = int(time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday,
                                        0, 0, 0, 0, 0, -1)))
            df = self.storage.db.query_hourly(key, start_ts=midnight, end_ts=int(ts))
            if df is None or getattr(df, "empty", True) or "kwh" not in df.columns:
                return
            import pandas as pd
            col = pd.to_numeric(df["kwh"], errors="coerce").fillna(0.0)
            pos = float(col[col > 0].sum())
            neg = float(col[col < 0].abs().sum())
            acc["charge"] = acc["import"] = acc["production"] = pos
            acc["discharge"] = acc["export"] = neg
        except Exception as e:
            logger.debug("PV daily seed failed for %s: %s", key, e)

    def daily_energy(self) -> Dict[str, Dict[str, float]]:
        """Snapshot of today's per-key energy accumulators (charge/discharge/
        import/export/production, kWh). Empty until the poller has run."""
        with self._acc_lock:
            return {k: dict(v) for k, v in self._day.items()}

    # ── ingest into live store + DB ────────────────────────────────────
    def _ingest(self, key: str, role: str, ts: int, power_w: float,
                kwh_today: Optional[float], soc: Optional[float]) -> None:
        # EXACT path: when the user mapped cumulative energy counters for this
        # role, derive interval energy from the counter delta (matches the source
        # system to the decimal). Otherwise integrate instantaneous power.
        parts = self._role_counter_parts(role)
        if parts is not None:
            energy_kwh, acc_today = self._accumulate_counter(key, role, ts, power_w, parts)
        else:
            energy_kwh, acc_today = self._accumulate(key, role, ts, power_w)
        # PV "today": prefer the inverter's own cumulative counter when the user
        # mapped one (kwh_today), else the integrated accumulator.
        tile_kwh = float(kwh_today) if kwh_today is not None else float(acc_today)

        # 1) Live store (for the live view / power-flow).
        try:
            from shelly_analyzer.services.webdash import LivePoint
            lp = LivePoint(
                ts=ts, power_total_w=float(power_w),
                va=0.0, vb=0.0, vc=0.0, ia=0.0, ib=0.0, ic=0.0,
                kwh_today=tile_kwh,
                soc_pct=float(soc) if soc is not None else 0.0,
                raw={"external": True},
            )
            self.live_store.update(key, lp)
        except Exception as e:
            logger.debug("PV live-store update failed for %s: %s", key, e)

        # 2) Energy DB (samples + hourly rollup) — same path as Shelly data, so
        #    every historical calculation picks it up automatically. We supply the
        #    integrated interval energy (signed: + charge/import, − discharge/
        #    export); synthetic samples carry only instantaneous power and the
        #    hourly rollup SUMs ``energy_kwh`` — without it every rollup is 0
        #    (this is why the battery showed 0.000 kWh everywhere).
        try:
            import pandas as pd
            df = pd.DataFrame([{
                "timestamp": int(ts),
                "total_power": float(power_w),
                "energy_kwh": float(energy_kwh),
            }])
            self.storage.db.insert_dataframe(key, df)
        except Exception as e:
            logger.debug("PV DB insert failed for %s: %s", key, e)

    # ── Home Assistant REST ────────────────────────────────────────────
    def _ha_conn(self, cfg):
        base = str(getattr(cfg, "ha_base_url", "") or "").rstrip("/")
        token = str(getattr(cfg, "ha_token", "") or "")
        if not base:
            base = _SUPERVISOR_CORE
        if not token:
            token = os.environ.get("SUPERVISOR_TOKEN", "") or os.environ.get("HASSIO_TOKEN", "")
        return base, token

    def _read_ha(self, cfg):
        base, token = self._ha_conn(cfg)
        if not token:
            self.last_error = "No HA token (set a long-lived token, or enable homeassistant_api on the add-on)."
            self._svc_log("PV source: " + self.last_error)
            return (None, None, None, None, None, None)
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        verify = bool(getattr(cfg, "ha_verify_ssl", True))

        def _state(entity: str) -> Optional[float]:
            entity = (entity or "").strip()
            if not entity:
                return None
            try:
                r = requests.get(f"{base}/api/states/{entity}", headers=headers, timeout=8, verify=verify)
                if r.status_code != 200:
                    self.last_error = f"HA {r.status_code} for {entity}"
                    return None
                st = r.json().get("state")
                if st in (None, "unknown", "unavailable", ""):
                    return None
                return _to_float(st)
            except Exception as e:
                self.last_error = f"HA request failed: {e}"
                return None

        pv_w = _state(getattr(cfg, "pv_power_entity", ""))
        batt_w = _state(getattr(cfg, "battery_power_entity", ""))
        soc = _state(getattr(cfg, "battery_soc_entity", ""))
        grid_w = _state(getattr(cfg, "grid_power_entity", ""))
        house_w = _state(getattr(cfg, "house_power_entity", ""))
        pv_today = _state(getattr(cfg, "pv_energy_today_entity", ""))
        # Cumulative energy counters (kWh) for EXACT interval energy via deltas.
        # Read each tick; missing/unmapped ones stay None → role falls back to
        # power integration.
        self._energy_counters = {
            "pv_total": _state(getattr(cfg, "pv_energy_total_entity", "")),
            "grid_import": _state(getattr(cfg, "grid_import_energy_entity", "")),
            "grid_export": _state(getattr(cfg, "grid_export_energy_entity", "")),
            "batt_charge": _state(getattr(cfg, "battery_charge_energy_entity", "")),
            "batt_discharge": _state(getattr(cfg, "battery_discharge_energy_entity", "")),
        }
        if pv_w is not None or batt_w is not None or grid_w is not None:
            self.last_error = None
        return (pv_w, batt_w, soc, grid_w, house_w, pv_today)

    # ── Modbus TCP (no Home Assistant needed) ──────────────────────────
    def _read_modbus(self, cfg):
        try:
            from shelly_analyzer.services.pv_modbus import read_modbus
        except Exception as e:
            self.last_error = f"Modbus unavailable: {e}"
            self._svc_log("PV source: " + self.last_error)
            return (None, None, None, None, None, None)
        pv_w, batt_w, soc, grid_w, house_w, pv_today, counters = read_modbus(cfg)
        err = counters.pop("_error", None) if isinstance(counters, dict) else None
        self._energy_counters = {
            "pv_total": counters.get("pv_total"),
            "grid_import": counters.get("grid_import"),
            "grid_export": counters.get("grid_export"),
            "batt_charge": counters.get("batt_charge"),
            "batt_discharge": counters.get("batt_discharge"),
        }
        if pv_w is not None or batt_w is not None or grid_w is not None:
            self.last_error = None
        elif err:
            self.last_error = f"Modbus: {err}"
        return (pv_w, batt_w, soc, grid_w, house_w, pv_today)

    # ── Local/cloud HTTP API (no Home Assistant needed) ────────────────
    def _read_http(self, cfg):
        try:
            from shelly_analyzer.services.pv_http import read_http
        except Exception as e:
            self.last_error = f"HTTP source unavailable: {e}"
            self._svc_log("PV source: " + self.last_error)
            return (None, None, None, None, None, None)
        pv_w, batt_w, soc, grid_w, house_w, pv_today, counters = read_http(cfg)
        err = counters.pop("_error", None) if isinstance(counters, dict) else None
        self._energy_counters = {
            "pv_total": counters.get("pv_total"),
            "grid_import": counters.get("grid_import"),
            "grid_export": counters.get("grid_export"),
            "batt_charge": counters.get("batt_charge"),
            "batt_discharge": counters.get("batt_discharge"),
        }
        if pv_w is not None or batt_w is not None or grid_w is not None:
            self.last_error = None
        elif err:
            self.last_error = f"HTTP: {err}"
        return (pv_w, batt_w, soc, grid_w, house_w, pv_today)

    # ── MQTT subscribe ─────────────────────────────────────────────────
    def _start_mqtt(self, cfg) -> None:
        try:
            import paho.mqtt.client as mqtt
        except Exception as e:
            self._svc_log(f"PV source: paho-mqtt unavailable ({e})")
            return
        app = self._get_config()
        mcfg = getattr(app, "mqtt", None)
        broker = str(getattr(mcfg, "broker", "127.0.0.1") or "127.0.0.1")
        port = int(getattr(mcfg, "port", 1883) or 1883)
        user = str(getattr(mcfg, "username", "") or "")
        pw = str(getattr(mcfg, "password", "") or "")

        topic_role = {
            str(getattr(cfg, "mqtt_pv_power_topic", "") or ""): "pv",
            str(getattr(cfg, "mqtt_battery_power_topic", "") or ""): "battery",
            str(getattr(cfg, "mqtt_battery_soc_topic", "") or ""): "soc",
            str(getattr(cfg, "mqtt_grid_power_topic", "") or ""): "grid",
            str(getattr(cfg, "mqtt_house_power_topic", "") or ""): "house",
        }
        topic_role = {t: r for t, r in topic_role.items() if t}
        if not topic_role:
            self._svc_log("PV source (MQTT): no topics configured")
            return
        json_path = str(getattr(cfg, "mqtt_json_path", "") or "")

        def _on_connect(client, userdata, flags, rc, *args):
            for t in topic_role:
                try:
                    client.subscribe(t)
                except Exception:
                    pass
            self._svc_log(f"PV source (MQTT) subscribed to {len(topic_role)} topic(s)")

        def _on_message(client, userdata, msg):
            role = topic_role.get(msg.topic)
            if not role:
                return
            try:
                payload = msg.payload.decode("utf-8", "ignore")
            except Exception:
                return
            val = _to_float(_dig_json(payload, json_path))
            if val is None:
                return
            with self._mqtt_lock:
                self._mqtt_vals[role] = val

        try:
            client = mqtt.Client()
            if user:
                client.username_pw_set(user, pw)
            client.on_connect = _on_connect
            client.on_message = _on_message
            client.connect_async(broker, port, keepalive=60)
            client.loop_start()
            self._mqtt_client = client
        except Exception as e:
            self._svc_log(f"PV source (MQTT) connect failed: {e}")

    def _read_mqtt(self, cfg):
        # MQTT source carries instantaneous power only — no cumulative counters,
        # so the exact-energy path stays disabled (power integration is used).
        self._energy_counters = {}
        with self._mqtt_lock:
            v = dict(self._mqtt_vals)
        return (v.get("pv"), v.get("battery"), v.get("soc"),
                v.get("grid"), v.get("house"), None)
