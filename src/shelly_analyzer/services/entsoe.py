"""ENTSO-E Transparency Platform API client for grid CO₂ intensity.

Fetches actual generation per production type (DocumentType=A75) for a given
bidding zone and calculates the grid CO₂ intensity in g/kWh using standard
emission factors.

Rate limiting: ENTSO-E enforces 400 requests/min per token.  We are
conservative and cap at 1 request/min by default.

References:
  https://transparency.entsoe.eu/content/static_content/Static%20content/
  web%20api/Guide.html
"""
from __future__ import annotations

import logging
import math
import threading
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ENTSO-E REST API base URL
_API_BASE = "https://web-api.entsoe.eu/api"

# ENTSO-E psrType codes → human-readable fuel names
_PSR_NAMES: Dict[str, str] = {
    "B01": "biomass",
    "B02": "lignite",
    "B03": "coal",        # Fossil Hard coal
    "B04": "gas",         # Fossil Gas
    "B05": "oil",         # Fossil Oil
    "B06": "oil_shale",
    "B07": "peat",
    "B08": "hydro_pumped",
    "B09": "hydro_run",
    "B10": "hydro_reservoir",
    "B11": "marine",
    "B12": "nuclear",
    "B13": "other_renewable",
    "B14": "solar",
    "B15": "geothermal",
    "B16": "wind_offshore",
    "B17": "wind_onshore",
    "B18": "other",
    "B19": "waste",
    "B20": "wind",          # aggregated fallback
}

# CO₂ emission factors in g CO₂eq/kWh (lifecycle, IPCC AR5 median values)
_CO2_FACTORS: Dict[str, float] = {
    "biomass":        230.0,
    "lignite":       1100.0,
    "coal":           820.0,
    "gas":            490.0,
    "oil":            650.0,
    "oil_shale":      650.0,
    "peat":           900.0,
    "hydro_pumped":    24.0,
    "hydro_run":        4.0,
    "hydro_reservoir":  4.0,
    "marine":           8.0,
    "nuclear":         12.0,
    "other_renewable": 30.0,
    "solar":           45.0,
    "geothermal":      38.0,
    "wind_offshore":   12.0,
    "wind_onshore":    11.0,
    "wind":            11.0,
    "other":          300.0,
    "waste":          300.0,
}

# XML namespace used in ENTSO-E responses
_NS = "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"


def _ts_to_entsoe_fmt(ts: int) -> str:
    """Convert Unix timestamp to ENTSO-E datetime string YYYYMMDDHHММ."""
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%Y%m%d%H%M")


def _parse_generation_xml(xml_text: str) -> Dict[str, Dict[int, float]]:
    """Parse ENTSO-E A75 XML.  Returns {fuel: {hour_ts: MW}}.

    Each time series contains multiple Period blocks.  Each Period has a
    resolution (PT60M or PT15M) and quantity points.  We aggregate all
    points into hourly buckets.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        logger.error("XML parse error: %s", exc)
        return {}

    results: Dict[str, Dict[int, float]] = {}

    for ts in root.iter(f"{{{_NS}}}TimeSeries"):
        # Get fuel type
        psr_el = ts.find(f".//{{{_NS}}}psrType")
        if psr_el is None:
            continue
        psr_code = (psr_el.text or "").strip()
        fuel = _PSR_NAMES.get(psr_code, "other")

        for period in ts.iter(f"{{{_NS}}}Period"):
            start_el = period.find(f"{{{_NS}}}timeInterval/{{{_NS}}}start")
            res_el = period.find(f"{{{_NS}}}resolution")
            if start_el is None or res_el is None:
                continue

            try:
                start_dt = datetime.strptime(start_el.text.strip(), "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc)
            except ValueError:
                continue

            resolution_text = (res_el.text or "PT60M").strip()
            if resolution_text == "PT15M":
                step_minutes = 15
            elif resolution_text == "PT30M":
                step_minutes = 30
            else:
                step_minutes = 60

            if fuel not in results:
                results[fuel] = {}

            for point in period.iter(f"{{{_NS}}}Point"):
                pos_el = point.find(f"{{{_NS}}}position")
                qty_el = point.find(f"{{{_NS}}}quantity")
                if pos_el is None or qty_el is None:
                    continue
                try:
                    pos = int(pos_el.text.strip()) - 1  # 1-based → 0-based
                    qty = float(qty_el.text.strip())
                except (ValueError, TypeError):
                    continue

                pt_dt = start_dt + timedelta(minutes=pos * step_minutes)
                # Snap to hour boundary
                hour_dt = pt_dt.replace(minute=0, second=0, microsecond=0)
                hour_ts = int(hour_dt.timestamp())

                results[fuel][hour_ts] = results[fuel].get(hour_ts, 0.0) + qty

    return results


def calculate_intensity(generation_mix: Dict[str, Dict[int, float]]) -> Dict[int, float]:
    """Compute weighted-average CO₂ intensity per hour from a generation mix.

    Returns {hour_ts: g_per_kwh}.
    """
    all_hours = set()
    for fuel_hours in generation_mix.values():
        all_hours.update(fuel_hours.keys())

    result: Dict[int, float] = {}
    for hour_ts in sorted(all_hours):
        total_mw = 0.0
        weighted_co2 = 0.0
        for fuel, fuel_hours in generation_mix.items():
            mw = fuel_hours.get(hour_ts, 0.0)
            if mw <= 0:
                continue
            factor = _CO2_FACTORS.get(fuel, 300.0)
            total_mw += mw
            weighted_co2 += mw * factor
        if total_mw > 0:
            result[hour_ts] = weighted_co2 / total_mw
        else:
            result[hour_ts] = 0.0
    return result


class EntsoeClient:
    """ENTSO-E Transparency Platform API client.

    Usage::

        client = EntsoeClient(api_token="...", bidding_zone="DE_LU")
        rows = client.fetch_intensity(start_ts=..., end_ts=...)
        # rows: list of (hour_ts, zone, intensity_g_per_kwh, source, fetched_at)

    The client enforces a per-instance rate limit of 1 request per 62 seconds
    to stay well within ENTSO-E's quota.
    """

    def __init__(
        self,
        api_token: str,
        bidding_zone: str = "DE_LU",
        min_request_interval: float = 62.0,
    ) -> None:
        self.api_token = api_token
        self.bidding_zone = bidding_zone
        self._min_interval = min_request_interval
        self._last_request_ts: float = 0.0
        self._lock = threading.Lock()

    # ── Public ───────────────────────────────────────────────────────────────

    def fetch_intensity(
        self,
        start_ts: int,
        end_ts: int,
    ) -> List[Tuple[int, str, float, str, int]]:
        """Fetch CO₂ intensity for [start_ts, end_ts).

        Returns a list of (hour_ts, zone, intensity_g_per_kwh, source, fetched_at)
        ready for EnergyDB.upsert_co2_intensity().

        Raises RuntimeError on API errors.
        """
        if not self.api_token:
            raise RuntimeError("No ENTSO-E API token configured.")

        xml_text = self._fetch_generation_xml(start_ts, end_ts)
        mix = _parse_generation_xml(xml_text)
        intensity = calculate_intensity(mix)

        now_ts = int(time.time())
        rows = [
            (hour_ts, self.bidding_zone, g_per_kwh, "entsoe", now_ts)
            for hour_ts, g_per_kwh in sorted(intensity.items())
            if start_ts <= hour_ts < end_ts
        ]
        logger.info(
            "EntsoeClient: fetched %d intensity points for zone %s",
            len(rows),
            self.bidding_zone,
        )
        return rows

    # ── Internal ─────────────────────────────────────────────────────────────

    def _wait_rate_limit(self) -> None:
        with self._lock:
            elapsed = time.monotonic() - self._last_request_ts
            if elapsed < self._min_interval:
                wait = self._min_interval - elapsed
                logger.debug("EntsoeClient: rate-limit wait %.1fs", wait)
                time.sleep(wait)
            self._last_request_ts = time.monotonic()

    def _fetch_generation_xml(self, start_ts: int, end_ts: int) -> str:
        """HTTP GET to ENTSO-E API and return raw XML text."""
        import urllib.request
        import urllib.parse
        import urllib.error

        self._wait_rate_limit()

        params = {
            "securityToken": self.api_token,
            "documentType": "A75",
            "processType": "A16",
            "in_Domain": self.bidding_zone,
            "periodStart": _ts_to_entsoe_fmt(start_ts),
            "periodEnd": _ts_to_entsoe_fmt(end_ts),
        }
        url = _API_BASE + "?" + urllib.parse.urlencode(params)
        logger.debug("EntsoeClient: GET %s", url[:120])

        try:
            req = urllib.request.Request(url)
            req.add_header("Accept", "application/xml")
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                return raw.decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            body = ""
            try:
                body = exc.read().decode("utf-8", errors="replace")[:300]
            except Exception:
                pass
            raise RuntimeError(
                f"ENTSO-E API HTTP {exc.code}: {exc.reason}. {body}"
            ) from exc
        except Exception as exc:
            raise RuntimeError(f"ENTSO-E API request failed: {exc}") from exc


class Co2FetchService:
    """Background service that periodically fetches CO₂ intensity from ENTSO-E
    and stores results in the EnergyDB.

    Usage::

        svc = Co2FetchService(db=db, get_config=lambda: app.cfg)
        svc.start()
        svc.trigger_now()   # optional: fetch immediately
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
        self._force_backfill: bool = False
        self._progress_callback = None  # callable(day_fetched: int, total_days: int) | None

    def set_progress_callback(self, cb) -> None:
        """Set a callback invoked during chunk fetching: cb(day_fetched, total_days).

        Called from the background thread – must be thread-safe (use a queue).
        Pass None to remove.
        """
        self._progress_callback = cb

    @property
    def last_error(self) -> Optional[str]:
        return self._last_error

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="Co2FetchService",
            daemon=True,
        )
        self._thread.start()
        logger.info("Co2FetchService started")

    def stop(self) -> None:
        self._stop_event.set()
        self._trigger_event.set()
        if self._thread:
            self._thread.join(timeout=10)
        logger.info("Co2FetchService stopped")

    def trigger_now(self, force: bool = False) -> None:
        """Force an immediate fetch.

        force=True re-fetches from backfill_days ago, ignoring any already-stored
        data (useful for the "Backfill now" button).
        """
        self._last_fetch_ts = 0.0
        if force:
            self._force_backfill = True
        self._trigger_event.set()

    def _run(self) -> None:
        # Small initial delay so the app has time to fully initialize
        self._stop_event.wait(5.0)
        while not self._stop_event.is_set():
            try:
                self._tick()
            except Exception:
                logger.exception("Co2FetchService tick error")
            # Wait up to fetch_interval_hours, but wake early on trigger
            try:
                cfg = self._get_config()
                interval_h = getattr(getattr(cfg, "co2", None), "fetch_interval_hours", 1) or 1
            except Exception:
                interval_h = 1
            self._trigger_event.wait(interval_h * 3600)
            self._trigger_event.clear()

    def _tick(self) -> None:
        try:
            cfg = self._get_config()
        except Exception:
            return

        co2_cfg = getattr(cfg, "co2", None)
        if co2_cfg is None or not getattr(co2_cfg, "enabled", False):
            return
        token = getattr(co2_cfg, "entso_e_api_token", "") or ""
        if not token:
            return

        zone = getattr(co2_cfg, "bidding_zone", "DE_LU") or "DE_LU"
        backfill_days = getattr(co2_cfg, "backfill_days", 7) or 7

        now_ts = int(time.time())
        # Determine fetch start: force-backfill resets to backfill_days ago;
        # otherwise start from the next hour after the latest stored hour.
        force = self._force_backfill
        self._force_backfill = False
        latest_ts = self._db.latest_co2_ts(zone)
        if force or latest_ts is None:
            start_ts = now_ts - backfill_days * 86400
        else:
            # Start from the next hour after the latest stored hour
            start_ts = latest_ts + 3600

        # Snap to hour boundary
        start_ts = (start_ts // 3600) * 3600
        end_ts = ((now_ts // 3600) + 1) * 3600  # next full hour

        if start_ts >= end_ts:
            logger.debug("Co2FetchService: data is up to date for zone %s", zone)
            return

        # Split into chunks of at most 7 days to stay within API limits
        client = EntsoeClient(api_token=token, bidding_zone=zone)
        chunk_s = 7 * 86400
        all_rows = []
        cursor = start_ts
        total_days = max(1, math.ceil((end_ts - start_ts) / 86400))
        days_fetched = 0
        cb = self._progress_callback
        while cursor < end_ts and not self._stop_event.is_set():
            chunk_end = min(cursor + chunk_s, end_ts)
            if cb is not None:
                try:
                    cb(days_fetched, total_days)
                except Exception:
                    pass
            try:
                rows = client.fetch_intensity(cursor, chunk_end)
                all_rows.extend(rows)
                self._last_error = None
            except Exception as exc:
                self._last_error = str(exc)
                logger.warning("Co2FetchService: fetch failed: %s", exc)
                if cb is not None:
                    try:
                        cb(days_fetched, total_days)
                    except Exception:
                        pass
                break
            days_fetched += max(1, round((chunk_end - cursor) / 86400))
            cursor = chunk_end

        if cb is not None:
            try:
                cb(total_days, total_days)
            except Exception:
                pass

        if all_rows:
            written = self._db.upsert_co2_intensity(all_rows)
            logger.info("Co2FetchService: stored %d intensity points", written)
        self._last_fetch_ts = time.time()
