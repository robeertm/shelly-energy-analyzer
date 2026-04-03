from __future__ import annotations
import logging
import threading
import time
from typing import Optional

_log = logging.getLogger(__name__)


class InfluxDBExporter:
    """Push energy metrics to InfluxDB via HTTP line protocol."""

    def __init__(self, cfg, storage=None):
        self.cfg = cfg
        self.storage = storage
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_push_ts: int = 0

    def start(self) -> None:
        if self._running or not self.cfg.enabled:
            return
        self._running = True
        self._last_push_ts = int(time.time()) - self.cfg.push_interval_seconds
        self._thread = threading.Thread(target=self._run_loop, name="InfluxDBExport", daemon=True)
        self._thread.start()
        _log.info("InfluxDB exporter started (url=%s, bucket=%s)", self.cfg.url, self.cfg.bucket)

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=10)
            self._thread = None

    def _run_loop(self) -> None:
        while self._running:
            try:
                self._push()
            except Exception as e:
                _log.error("InfluxDB push error: %s", e)
            time.sleep(self.cfg.push_interval_seconds)

    def _push(self) -> None:
        if not self.storage:
            return

        import requests

        now = int(time.time())
        since = self._last_push_ts

        db = self.storage.db
        lines = []

        for dev in (self.storage.devices if hasattr(self.storage, 'devices') else []):
            key = dev.key if hasattr(dev, 'key') else str(dev)
            try:
                df = db.query_samples(key, since, now)
                if df is None or df.empty:
                    continue

                measurement = self.cfg.measurement
                for _, row in df.iterrows():
                    ts_ns = int(row.get("timestamp", 0)) * 1_000_000_000
                    fields = []
                    for col in ["total_power", "energy_kwh", "a_voltage", "b_voltage", "c_voltage",
                                "a_current", "b_current", "c_current",
                                "a_act_power", "b_act_power", "c_act_power"]:
                        val = row.get(col)
                        if val is not None and str(val) != 'nan':
                            fields.append(f"{col}={float(val)}")

                    if fields:
                        line = f'{measurement},device={key} {",".join(fields)} {ts_ns}'
                        lines.append(line)
            except Exception as e:
                _log.debug("InfluxDB: skip device %s: %s", key, e)

        if not lines:
            self._last_push_ts = now
            return

        body = "\n".join(lines)

        try:
            if self.cfg.version >= 2:
                # InfluxDB 2.x
                url = f"{self.cfg.url.rstrip('/')}/api/v2/write"
                params = {"org": self.cfg.org, "bucket": self.cfg.bucket, "precision": "ns"}
                headers = {"Authorization": f"Token {self.cfg.token}", "Content-Type": "text/plain"}
            else:
                # InfluxDB 1.x
                url = f"{self.cfg.url.rstrip('/')}/write"
                params = {"db": self.cfg.bucket, "precision": "ns"}
                headers = {"Content-Type": "text/plain"}
                if self.cfg.token:
                    headers["Authorization"] = f"Basic {self.cfg.token}"

            resp = requests.post(url, params=params, headers=headers, data=body, timeout=10)
            if resp.status_code in (200, 204):
                _log.debug("InfluxDB: pushed %d points", len(lines))
            else:
                _log.warning("InfluxDB: HTTP %d: %s", resp.status_code, resp.text[:200])
        except Exception as e:
            _log.error("InfluxDB HTTP error: %s", e)

        self._last_push_ts = now
