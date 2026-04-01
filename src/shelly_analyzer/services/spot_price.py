"""Dynamic spot market electricity price service.

Fetches day-ahead spot prices from free public APIs and stores them in
the EnergyDB for cost comparison calculations.

Supported APIs:
  - Energy-Charts (Fraunhofer ISE): 15-min resolution from Oct 2025, hourly before
  - aWATTar: hourly resolution, history from 2015

Both APIs are free, public, and require no authentication.
"""
from __future__ import annotations

import logging
import math
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# API clients
# ---------------------------------------------------------------------------

_EC_BASE = "https://api.energy-charts.info"
_AWATTAR_BASE = "https://api.awattar.de/v1/marketdata"

# Conservative request delay (seconds) between API calls
_REQUEST_DELAY = 2.0

# Maximum chunk size for a single API request (seconds) = 30 days
_MAX_CHUNK = 30 * 86400


def fetch_energy_charts(
    zone: str, start_ts: int, end_ts: int,
) -> List[Tuple[int, float, int]]:
    """Fetch spot prices from Energy-Charts API.

    Returns list of (slot_ts_utc, price_eur_mwh, resolution_seconds).
    """
    start_iso = datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso = datetime.fromtimestamp(end_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    url = f"{_EC_BASE}/price"
    params = {"bzn": zone, "start": start_iso, "end": end_iso}

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    unix_seconds = data.get("unix_seconds", [])
    prices = data.get("price", [])

    if not unix_seconds or not prices or len(unix_seconds) != len(prices):
        return []

    # Detect resolution from consecutive timestamps
    resolution = 3600
    if len(unix_seconds) >= 2:
        delta = unix_seconds[1] - unix_seconds[0]
        if 800 <= delta <= 1000:
            resolution = 900  # 15-min
        elif 3500 <= delta <= 3700:
            resolution = 3600  # hourly

    results = []
    for ts, price in zip(unix_seconds, prices):
        if price is not None:
            results.append((int(ts), float(price), resolution))
    return results


def fetch_awattar(start_ts: int, end_ts: int) -> List[Tuple[int, float, int]]:
    """Fetch spot prices from aWATTar API.

    Returns list of (slot_ts_utc, price_eur_mwh, resolution_seconds).
    """
    start_ms = start_ts * 1000
    end_ms = end_ts * 1000
    url = _AWATTAR_BASE
    params = {"start": str(start_ms), "end": str(end_ms)}

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    entries = data.get("data", [])
    results = []
    for entry in entries:
        ts = int(entry.get("start_timestamp", 0)) // 1000
        price = entry.get("marketprice")
        if ts > 0 and price is not None:
            results.append((ts, float(price), 3600))
    return results


# ---------------------------------------------------------------------------
# Background fetch service
# ---------------------------------------------------------------------------


class SpotPriceFetchService:
    """Background service that periodically fetches spot market prices
    and stores them in the EnergyDB.

    Usage::

        svc = SpotPriceFetchService(db=db, get_config=lambda: app.cfg)
        svc.start()
        svc.trigger_now()
        svc.stop()
    """

    def __init__(self, db, get_config) -> None:
        self._db = db
        self._get_config = get_config
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._trigger_event = threading.Event()
        self._last_fetch_ts: float = 0.0
        self._last_error: Optional[str] = None
        self._log_callback = None

    def set_log_callback(self, cb) -> None:
        self._log_callback = cb

    def _svc_log(self, msg: str) -> None:
        logger.info("SpotPriceFetchService: %s", msg)
        cb = self._log_callback
        if cb is not None:
            try:
                cb(msg)
            except Exception:
                pass

    @property
    def last_error(self) -> Optional[str]:
        return self._last_error

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="SpotPriceFetchService",
            daemon=True,
        )
        self._thread.start()
        logger.info("SpotPriceFetchService started")

    def stop(self) -> None:
        self._stop_event.set()
        self._trigger_event.set()
        if self._thread:
            self._thread.join(timeout=10)
        logger.info("SpotPriceFetchService stopped")

    def trigger_now(self) -> None:
        self._last_fetch_ts = 0.0
        self._trigger_event.set()

    def _run(self) -> None:
        self._trigger_event.wait(8.0)
        self._trigger_event.clear()
        while not self._stop_event.is_set():
            try:
                self._tick()
            except Exception:
                logger.exception("SpotPriceFetchService tick error")
            try:
                cfg = self._get_config()
                interval_h = getattr(getattr(cfg, "spot_price", None), "fetch_interval_hours", 1) or 1
            except Exception:
                interval_h = 1
            self._trigger_event.wait(interval_h * 3600)
            self._trigger_event.clear()

    def _tick(self) -> None:
        try:
            cfg = self._get_config()
        except Exception as e:
            self._svc_log(f"Spot: config error: {e}")
            return

        spot_cfg = getattr(cfg, "spot_price", None)
        if spot_cfg is None or not getattr(spot_cfg, "enabled", False):
            self._svc_log("Spot: disabled in config, skipping")
            return

        zone = getattr(spot_cfg, "bidding_zone", "DE-LU") or "DE-LU"
        primary_api = getattr(spot_cfg, "primary_api", "energy_charts") or "energy_charts"

        self._svc_log(f"Spot: checking for missing prices (zone={zone}, api={primary_api})")

        now_ts = int(time.time())

        # Find oldest energy measurement to know how far back to fetch
        oldest_measurement = self._db.oldest_measurement_ts()
        if oldest_measurement is None:
            self._svc_log("Spot: no measurements found, fetching last 2 days")
            oldest_measurement = now_ts - 2 * 86400
        else:
            d_oldest = datetime.fromtimestamp(oldest_measurement, tz=timezone.utc).strftime("%Y-%m-%d")
            self._svc_log(f"Spot: oldest measurement: {d_oldest}")

        # Range: oldest measurement to now + 24h (day-ahead prices)
        range_start = (oldest_measurement // 3600) * 3600
        range_end = ((now_ts // 3600) + 25) * 3600

        if range_start >= range_end:
            self._svc_log("Spot: range_start >= range_end, nothing to do")
            return

        # Check existing data
        latest_ts = self._db.latest_spot_price_ts(zone)
        if latest_ts:
            d_latest = datetime.fromtimestamp(latest_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
            self._svc_log(f"Spot: latest price in DB: {d_latest}")
        else:
            self._svc_log("Spot: no prices in DB yet")

        # Find gaps
        gaps = self._db.find_spot_price_gaps(zone, range_start, range_end)
        if not gaps:
            self._svc_log("Spot: no gaps found, all prices up to date")
            return

        # Merge nearby gaps into fetch ranges (day-aligned)
        fetch_ranges: List[Tuple[int, int]] = []
        for gap_start, gap_end in gaps:
            aligned_start = (gap_start // 86400) * 86400
            aligned_end = min(((gap_end + 86399) // 86400) * 86400, range_end)
            if fetch_ranges and aligned_start <= fetch_ranges[-1][1]:
                fetch_ranges[-1] = (fetch_ranges[-1][0], max(fetch_ranges[-1][1], aligned_end))
            else:
                fetch_ranges.append((aligned_start, aligned_end))

        total_hours_missing = sum((e - s) // 3600 for s, e in gaps)
        d_from = datetime.fromtimestamp(fetch_ranges[0][0], tz=timezone.utc).strftime("%Y-%m-%d")
        d_to = datetime.fromtimestamp(fetch_ranges[-1][1], tz=timezone.utc).strftime("%Y-%m-%d")
        self._svc_log(
            f"Spot Import: {total_hours_missing} missing hours, "
            f"{len(fetch_ranges)} range(s), zone {zone} ({d_from} \u2192 {d_to})"
        )

        fetched_at = now_ts
        total_written = 0

        for fr_start, fr_end in fetch_ranges:
            if self._stop_event.is_set():
                break

            # Split into chunks of max 30 days
            chunk_start = fr_start
            while chunk_start < fr_end and not self._stop_event.is_set():
                chunk_end = min(chunk_start + _MAX_CHUNK, fr_end)

                d_cs = datetime.fromtimestamp(chunk_start, tz=timezone.utc).strftime("%Y-%m-%d")
                d_ce = datetime.fromtimestamp(chunk_end, tz=timezone.utc).strftime("%Y-%m-%d")
                self._svc_log(f"Spot: fetching {d_cs} \u2192 {d_ce} via {primary_api}...")

                rows = self._fetch_chunk(primary_api, zone, chunk_start, chunk_end)
                if rows:
                    db_rows = [
                        (ts, zone, price, res, source, fetched_at)
                        for ts, price, res, source in rows
                    ]
                    written = self._db.upsert_spot_prices(db_rows)
                    total_written += written
                    self._svc_log(f"Spot: {written} prices written to DB ({d_cs}\u2192{d_ce})")
                else:
                    self._svc_log(f"Spot: no data returned for {d_cs}\u2192{d_ce}")

                chunk_start = chunk_end
                if chunk_start < fr_end:
                    time.sleep(_REQUEST_DELAY)

        if total_written > 0:
            self._svc_log(f"Spot Import complete: {total_written} prices stored in total")
        else:
            self._svc_log("Spot Import: no prices could be fetched")
        self._last_error = None

    def _fetch_chunk(
        self, primary_api: str, zone: str, start_ts: int, end_ts: int
    ) -> List[Tuple[int, float, int, str]]:
        """Fetch a chunk of spot prices. Returns (ts, price_eur_mwh, resolution, source)."""
        # Try primary API
        try:
            if primary_api == "energy_charts":
                raw = fetch_energy_charts(zone, start_ts, end_ts)
                if raw:
                    return [(ts, p, r, "energy_charts") for ts, p, r in raw]
                else:
                    self._svc_log(f"Spot: energy_charts returned empty for zone={zone}")
            else:
                raw = fetch_awattar(start_ts, end_ts)
                if raw:
                    return [(ts, p, r, "awattar") for ts, p, r in raw]
                else:
                    self._svc_log("Spot: awattar returned empty")
        except Exception as e:
            self._svc_log(f"Spot: primary API ({primary_api}) error: {e}")
            logger.warning("SpotPrice primary API (%s) failed: %s", primary_api, e)

        # Fallback to other API
        fallback = "awattar" if primary_api == "energy_charts" else "energy_charts"
        self._svc_log(f"Spot: trying fallback API ({fallback})...")
        time.sleep(_REQUEST_DELAY)
        try:
            if primary_api == "energy_charts":
                raw = fetch_awattar(start_ts, end_ts)
                source = "awattar"
            else:
                raw = fetch_energy_charts(zone, start_ts, end_ts)
                source = "energy_charts"
            if raw:
                return [(ts, p, r, source) for ts, p, r in raw]
            else:
                self._svc_log(f"Spot: fallback API ({fallback}) also returned empty")
        except Exception as e:
            self._svc_log(f"Spot: fallback API ({fallback}) error: {e}")
            logger.warning("SpotPrice fallback API failed: %s", e)
            self._last_error = str(e)

        return []
