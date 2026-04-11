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
# AEMO NEM aggregated 30-min price feed (public, no auth required).
# The "elec_nem_summary" endpoint returns the latest dispatch + trading
# prices per region. For historical prices we use the official CSV report.
_AEMO_CURRENT = "https://aemo.com.au/aemo/api/v1/visdata/nem/spot"
_AEMO_CSV_BASE = "https://nemweb.com.au/Reports/Current/TradingIS_Reports"
# EIA open data (requires free API key registered at eia.gov/opendata).
_EIA_BASE = "https://api.eia.gov/v2"
# FX rate (USD→EUR, AUD→EUR) — ECB daily rates via exchangerate.host (free,
# no key). Cached for the process lifetime so we don't hit the API per call.
_FX_BASE = "https://api.frankfurter.app"

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


_FX_CACHE: Dict[str, float] = {}  # {"USD": 0.92, "AUD": 0.61}
_FX_CACHE_DATE: Optional[str] = None  # YYYY-MM-DD of the last successful refresh


def _fx_rate_to_eur(currency: str) -> float:
    """Return the current FX rate from ``currency`` to EUR. Cached daily.

    Falls back to a hard-coded sane value if the API is unreachable so a
    network blip doesn't nuke months of historical pricing. Frankfurter's
    rates are quoted as 1 EUR = X foreign, so we invert."""
    global _FX_CACHE_DATE
    cur = (currency or "EUR").upper()
    if cur == "EUR":
        return 1.0
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _FX_CACHE_DATE != today:
        _FX_CACHE.clear()
    if cur in _FX_CACHE:
        return _FX_CACHE[cur]
    try:
        url = f"{_FX_BASE}/latest"
        resp = requests.get(url, params={"from": "EUR", "to": cur}, timeout=8)
        resp.raise_for_status()
        data = resp.json()
        eur_to_cur = float(data.get("rates", {}).get(cur, 0))
        if eur_to_cur > 0:
            rate = 1.0 / eur_to_cur
            _FX_CACHE[cur] = rate
            _FX_CACHE_DATE = today
            return rate
    except Exception as e:
        logger.warning("FX lookup %s→EUR failed: %s", cur, e)
    # Conservative fallbacks (as of late 2025)
    fallbacks = {"USD": 0.92, "AUD": 0.61, "GBP": 1.17, "CAD": 0.68, "JPY": 0.0060}
    return fallbacks.get(cur, 1.0)


def fetch_aemo(zone: str, start_ts: int, end_ts: int) -> List[Tuple[int, float, int]]:
    """Fetch AEMO NEM dispatch prices for an Australian region.

    Parameters
    ----------
    zone : str
        App zone code (``AU-NSW``, ``AU-VIC``, ``AU-QLD``, ``AU-SA``,
        ``AU-TAS``).  Mapped to the AEMO region id internally.

    Returns
    -------
    list[tuple[int, float, int]]
        ``(slot_ts_utc, price_eur_mwh, resolution_seconds)``.  AEMO
        reports in AUD/MWh; we convert using the daily FX rate so the
        stored values are comparable to EU spot prices.

    Notes
    -----
    AEMO's "visdata/nem/spot" endpoint only returns the most recent ~48h
    snapshot.  True historical backfill would need to parse the public
    NEMWeb CSV archives which change structure across years.  For now we
    only attempt to fill the 48h window and silently skip older gaps.
    """
    region_map = {
        "AU-NSW": "NSW1", "AU-VIC": "VIC1", "AU-QLD": "QLD1",
        "AU-SA":  "SA1",  "AU-TAS": "TAS1",
    }
    region = region_map.get((zone or "").upper())
    if not region:
        logger.warning("AEMO: unknown zone %s", zone)
        return []
    try:
        resp = requests.get(_AEMO_CURRENT, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("AEMO fetch failed: %s", e)
        return []

    rows: List[Tuple[int, float, int]] = []
    fx = _fx_rate_to_eur("AUD")
    # Structure: {"ELEC_NEM_SUMMARY": [{"REGIONID": "NSW1", "PRICE": 42.15,
    # "SETTLEMENTDATE": "2025-12-31T23:30:00", ...}, ...]}
    entries = data.get("ELEC_NEM_SUMMARY") or data.get("data") or []
    for entry in entries:
        try:
            rid = str(entry.get("REGIONID") or entry.get("regionid") or "").upper()
            if rid != region:
                continue
            price_aud = float(entry.get("PRICE") or entry.get("price") or 0)
            settle = str(entry.get("SETTLEMENTDATE") or entry.get("settlementdate") or "")
            if not settle:
                continue
            # AEMO timestamps are in Australia/Brisbane time (AEST, +10) and
            # carry no tz suffix. Parse as naive and localise.
            from zoneinfo import ZoneInfo
            dt = datetime.fromisoformat(settle.replace("Z", "")).replace(
                tzinfo=ZoneInfo("Australia/Brisbane")
            )
            ts = int(dt.timestamp())
            if ts < start_ts or ts > end_ts:
                continue
            price_eur = price_aud * fx
            rows.append((ts, price_eur, 1800))  # NEM dispatch is 5 min, trading 30 min
        except Exception:
            continue
    return rows


def fetch_eia(zone: str, start_ts: int, end_ts: int, api_key: str) -> List[Tuple[int, float, int]]:
    """Fetch US wholesale electricity prices from EIA open data.

    Uses EIA dataset ``electricity/wholesale/daily-region-data`` which
    reports LMP averages per NERC region in USD/MWh.  Values are
    converted to EUR/MWh via the daily FX rate.
    """
    if not api_key:
        logger.warning("EIA fetch skipped: no API key configured")
        return []
    region = (zone or "").upper().replace("US-", "")
    if not region:
        return []
    start_date = datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime("%Y-%m-%d")
    end_date = datetime.fromtimestamp(end_ts, tz=timezone.utc).strftime("%Y-%m-%d")
    url = f"{_EIA_BASE}/electricity/wholesale/daily-region-data/data/"
    params = {
        "api_key": api_key,
        "frequency": "daily",
        "data[0]": "value",
        "facets[respondent][]": region,
        "start": start_date,
        "end": end_date,
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
        "length": 5000,
    }
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("EIA fetch failed for %s: %s", region, e)
        return []
    rows: List[Tuple[int, float, int]] = []
    fx = _fx_rate_to_eur("USD")
    records = (data.get("response") or {}).get("data") or []
    for r in records:
        try:
            period = str(r.get("period") or "")
            if not period:
                continue
            # Daily granularity: period = "YYYY-MM-DD"
            dt = datetime.fromisoformat(period).replace(tzinfo=timezone.utc)
            ts = int(dt.timestamp())
            if ts < start_ts or ts > end_ts:
                continue
            val = r.get("value")
            if val is None:
                continue
            price_usd = float(val)
            price_eur = price_usd * fx
            # Daily resolution = 86400s; we flag this so the DB knows the
            # slot spans a whole day.
            rows.append((ts, price_eur, 86400))
        except Exception:
            continue
    return rows


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
        """Fetch a chunk of spot prices. Returns (ts, price_eur_mwh, resolution, source).

        The primary_api hint is only used within the EU (where both
        energy_charts and awattar speak the same zones). For US / AU
        zones the dispatcher forces the right provider regardless of
        primary_api so users can't misconfigure themselves into an
        infinite empty-fetch loop."""
        from shelly_analyzer.services.zones import spot_provider_for_zone
        dispatched = spot_provider_for_zone(zone)
        if dispatched in {"eia", "aemo"}:
            primary_api = dispatched

        # Try primary API
        try:
            if primary_api == "eia":
                cfg = self._get_config()
                api_key = str(getattr(getattr(cfg, "spot_price", None), "eia_api_key", "") or "")
                raw = fetch_eia(zone, start_ts, end_ts, api_key)
                if raw:
                    return [(ts, p, r, "eia") for ts, p, r in raw]
                self._svc_log(f"Spot: EIA returned empty for zone={zone} (key set: {bool(api_key)})")
                return []
            if primary_api == "aemo":
                raw = fetch_aemo(zone, start_ts, end_ts)
                if raw:
                    return [(ts, p, r, "aemo") for ts, p, r in raw]
                self._svc_log(f"Spot: AEMO returned empty for zone={zone}")
                return []
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
