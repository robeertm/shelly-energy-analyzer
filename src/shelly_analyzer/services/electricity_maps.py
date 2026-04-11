"""Electricity Maps API client for global grid CO₂ intensity.

Electricity Maps (electricitymap.org) provides real-time and historical
carbon intensity for ~200 grid zones worldwide, filling the gap that
ENTSO-E leaves outside the EU.

The free tier is rate-limited (300 requests/month) but perfectly adequate
for a home-use tool that polls once per hour. Users must register at
https://api.electricitymap.org and paste the returned key into the CO₂
settings. No key → no global data; the app falls back to ENTSO-E.

This client intentionally only implements the endpoints we actually need:

- ``/v3/carbon-intensity/latest`` – current hour
- ``/v3/carbon-intensity/history`` – last 24h (rolling)

Historical backfill beyond 24h is a paid feature, so the fetcher simply
appends the current hour on each tick instead of asking for ranges.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

_BASE = "https://api.electricitymap.org/v3"


def fetch_latest(zone: str, api_key: str) -> Optional[Tuple[int, float]]:
    """Return (hour_ts_utc, intensity_g_per_kwh) for the zone's current
    hour, or None if the API rejects the request.

    The response's ``datetime`` field is an ISO-8601 UTC timestamp at
    minute granularity; we floor it to the hour so the value aligns
    with the co2_intensity table (which is keyed by hour_ts)."""
    if not zone or not api_key:
        return None
    url = f"{_BASE}/carbon-intensity/latest"
    headers = {"auth-token": api_key}
    try:
        resp = requests.get(url, params={"zone": zone}, headers=headers, timeout=15)
        if resp.status_code == 401:
            logger.warning("Electricity Maps: 401 unauthorized — check API key")
            return None
        if resp.status_code == 404:
            logger.warning("Electricity Maps: zone %s not found", zone)
            return None
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("Electricity Maps latest fetch failed: %s", e)
        return None

    intensity = data.get("carbonIntensity")
    iso_ts = data.get("datetime")
    if intensity is None or not iso_ts:
        return None
    try:
        dt = datetime.fromisoformat(str(iso_ts).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        # Floor to the hour.
        hour_ts = int(dt.replace(minute=0, second=0, microsecond=0).timestamp())
        return hour_ts, float(intensity)
    except Exception:
        return None


def fetch_history(zone: str, api_key: str) -> List[Tuple[int, float]]:
    """Return the last ~24h of hourly carbon intensities for the zone.

    Each entry is (hour_ts_utc, intensity_g_per_kwh). Empty list on
    error."""
    if not zone or not api_key:
        return []
    url = f"{_BASE}/carbon-intensity/history"
    headers = {"auth-token": api_key}
    try:
        resp = requests.get(url, params={"zone": zone}, headers=headers, timeout=20)
        if resp.status_code in (401, 403):
            logger.warning("Electricity Maps history: %s — check API key", resp.status_code)
            return []
        if resp.status_code == 404:
            logger.warning("Electricity Maps history: zone %s not found", zone)
            return []
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("Electricity Maps history fetch failed: %s", e)
        return []

    history = data.get("history") or []
    rows: List[Tuple[int, float]] = []
    for entry in history:
        try:
            intensity = entry.get("carbonIntensity")
            iso_ts = entry.get("datetime")
            if intensity is None or not iso_ts:
                continue
            dt = datetime.fromisoformat(str(iso_ts).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            hour_ts = int(dt.replace(minute=0, second=0, microsecond=0).timestamp())
            rows.append((hour_ts, float(intensity)))
        except Exception:
            continue
    return rows


class ElectricityMapsFetchService:
    """Background service that polls Electricity Maps every hour and
    writes the result into the ``co2_intensity`` table under the same
    zone id used in the config. Drop-in parallel to Co2FetchService —
    BackgroundServiceManager picks one or the other based on whether
    an Electricity Maps API key is configured and whether the selected
    zone looks like an Electricity Maps zone (no underscores)."""

    def __init__(self, db, get_config) -> None:
        self._db = db
        self._get_config = get_config
        self._stop_event = threading.Event()
        self._trigger_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_error: Optional[str] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run, name="ElectricityMapsFetcher", daemon=True
        )
        self._thread.start()
        logger.info("Electricity Maps fetcher started")

    def stop(self) -> None:
        self._stop_event.set()
        self._trigger_event.set()
        if self._thread:
            self._thread.join(timeout=10)

    def trigger_now(self) -> None:
        self._trigger_event.set()

    def _run(self) -> None:
        # Initial delay so the app is fully started before we hit the API.
        if self._stop_event.wait(10.0):
            return
        while not self._stop_event.is_set():
            try:
                self._tick()
            except Exception:
                logger.exception("Electricity Maps tick failed")
            # Once per hour — matches the ENTSO-E cadence and keeps us
            # well under the free-tier quota (720 req/month allowed).
            if self._stop_event.wait(3600):
                return

    def _tick(self) -> None:
        cfg = self._get_config()
        co2_cfg = getattr(cfg, "co2", None)
        if co2_cfg is None or not getattr(co2_cfg, "enabled", False):
            return
        api_key = str(getattr(co2_cfg, "electricity_maps_api_key", "") or "")
        if not api_key:
            return
        zone = str(getattr(co2_cfg, "bidding_zone", "") or "")
        if not zone or "_" in zone:
            # Zone looks like an ENTSO-E zone — leave it to Co2FetchService.
            return

        logger.info("Electricity Maps: fetching zone=%s", zone)
        now_ts = int(time.time())

        # Prefer the history endpoint (24h window) so we get a wide backfill
        # on first run, then the latest endpoint for subsequent polls.
        rows = fetch_history(zone, api_key)
        if not rows:
            latest = fetch_latest(zone, api_key)
            if latest is not None:
                rows = [latest]

        if not rows:
            self._last_error = "no data returned"
            return

        db_rows = [
            (hour_ts, zone, intensity, "electricity_maps", now_ts)
            for hour_ts, intensity in rows
        ]
        try:
            written = self._db.upsert_co2_intensity(db_rows)
            logger.info("Electricity Maps: wrote %d rows for zone=%s", written, zone)
            self._last_error = None
        except Exception as e:
            logger.warning("Electricity Maps DB write failed: %s", e)
            self._last_error = str(e)
