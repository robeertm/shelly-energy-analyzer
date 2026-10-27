"""Time-of-Use windows from the OpenEI Utility Rate Database (URDB).

Owns: the search for the residential rates near an address, fetching one URDB
rate and converting its energy schedule for one month into the app's TOU
windows (``TouRate``), plus the daily refresh loop.
Needs: a rate label (or the rate page URL) and an OpenEI API key; an empty key
uses ``DEMO_KEY``, which allows only 10 requests per day for each IP address.
Changes: ``cfg.tou`` through :func:`apply_tou` (config file + hot reload).
Tests: ``tests/test_urdb.py``.

Limits, on purpose, to keep the existing TOU model unchanged:
- only the schedule of the current month (re-applied when the month changes),
- only the first tier of each period (``rate + adj``, $/kWh),
- no fixed, minimum or demand charges,
- holidays count as ordinary days.
"""
from __future__ import annotations

import logging
import re
import threading
import time
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from shelly_analyzer.io.config import TouConfig, TouRate

logger = logging.getLogger(__name__)

API_URL = "https://api.openei.org/utility_rates"
DEMO_KEY = "DEMO_KEY"
# A URDB label is a 24-digit hex id, e.g. .../IURDB/rate/view/5cc0e8c45457a3d8137b23c6
_LABEL_RE = re.compile(r"\b([0-9a-fA-F]{24})\b")


def parse_rate_label(text: str) -> str:
    """Return the URDB label from a label or from a rate page URL."""
    text = str(text or "").strip()
    m = _LABEL_RE.search(text)
    if m:
        return m.group(1).lower()
    if "/" in text:
        raise ValueError("No URDB rate label in this URL")
    return text


def _query(params: Dict[str, Any], api_key: str, timeout: float) -> Dict[str, Any]:
    """One URDB API request (version 8, JSON). Raises RuntimeError on errors."""
    params = dict(params, version=8, format="json", api_key=api_key or DEMO_KEY)
    try:
        resp = requests.get(API_URL, params=params, timeout=timeout)
    except requests.RequestException as e:
        # The exception text contains the request URL, and with it the API key.
        raise RuntimeError(f"URDB request failed ({type(e).__name__})") from None
    try:
        data = resp.json()
    except ValueError:
        raise RuntimeError(f"URDB: HTTP {resp.status_code}") from None
    err = data.get("error") if isinstance(data, dict) else None
    if err:
        msg = (err.get("message") or err.get("code")) if isinstance(err, dict) else err
        raise RuntimeError(f"URDB: {msg}")
    if resp.status_code != 200 or not isinstance(data, dict):
        raise RuntimeError(f"URDB: HTTP {resp.status_code}")
    return data


def fetch_rate(label: str, api_key: str = "", timeout: float = 20.0) -> Dict[str, Any]:
    """Fetch one rate with full detail from the URDB API (version 8)."""
    items = _query({"detail": "full", "getpage": label}, api_key, timeout).get("items")
    if not items:
        raise RuntimeError(f"URDB: rate {label} not found")
    return items[0]


# URDB maps a point to one utility. A big city can be in the area of a second
# utility (San Francisco: the city utility and PG&E), so the search also takes
# the utilities within this distance; the list groups the rates by utility.
SEARCH_RADIUS_MILES = 5


def search_rates(location: str = "", api_key: str = "", lat: float = 0.0, lon: float = 0.0,
                 now: Optional[float] = None, timeout: float = 30.0) -> List[Dict[str, Any]]:
    """Residential rates in effect now near an address or ZIP code, or near
    ``lat``/``lon`` when ``location`` is empty.

    Returns ``{"label", "utility", "name"}`` dicts, one for each utility and
    rate name (the newest version), sorted by utility and name."""
    now = int(now if now is not None else time.time())
    params: Dict[str, Any] = {"detail": "minimal", "sector": "Residential", "approved": "true",
                              "effective_on_date": now, "limit": 500,
                              "radius": SEARCH_RADIUS_MILES, "co_limit": 10}
    location = str(location or "").strip()
    if location:
        params["address"] = location
    elif lat or lon:
        params["lat"], params["lon"] = float(lat), float(lon)
    else:
        raise ValueError("Enter an address or ZIP code, or set latitude and longitude in Settings → Weather")
    newest: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for it in _query(params, api_key, timeout).get("items") or []:
        start, end = int(it.get("startdate") or 0), int(it.get("enddate") or 0)
        if not it.get("label") or start > now or (end and end <= now):
            continue
        key = (str(it.get("utility") or ""), str(it.get("name") or ""))
        if key not in newest or start > int(newest[key].get("startdate") or 0):
            newest[key] = it
    out = [{"label": str(it["label"]), "utility": k[0], "name": k[1]} for k, it in newest.items()]
    return sorted(out, key=lambda r: (r["utility"].lower(), r["name"].lower()))


def _period_price(tiers: Any) -> float:
    """Price of one period: its first tier, rate + adj ($/kWh)."""
    if not isinstance(tiers, list) or not tiers or not isinstance(tiers[0], dict):
        raise ValueError("URDB rate has an energy period without tiers")
    t = tiers[0]
    return round(float(t.get("rate") or 0.0) + float(t.get("adj") or 0.0), 5)


def _windows(row: List[int]) -> List[Tuple[int, int, int]]:
    """Runs of the same period in one 24-hour row as (start, end, period).

    ``end`` is exclusive and wraps past midnight when end < start. One period
    for the whole day gives (0, 0, period)."""
    if len(row) != 24:
        raise ValueError("URDB schedule row does not have 24 hours")
    if len(set(row)) == 1:
        return [(0, 0, row[0])]
    first = next(h for h in range(24) if row[h] != row[h - 1])
    out: List[Tuple[int, int, int]] = []
    start = first
    for i in range(1, 25):
        h = (first + i) % 24
        if i == 24 or row[h] != row[start]:
            out.append((start, h, row[start]))
            start = h
    return sorted(out)


def tou_rates_for_month(rate: Dict[str, Any], month: int) -> List[TouRate]:
    """TOU windows for ``month`` (1-12).

    Weekday windows (Mon-Fri only) come first and cover every weekday hour.
    The weekend windows follow, so with "first matching rate wins" they only
    match on Saturday and Sunday. When both rows are the same, one set of
    all-days windows is returned."""
    structure = rate.get("energyratestructure")
    weekday = rate.get("energyweekdayschedule")
    weekend = rate.get("energyweekendschedule") or weekday
    if not structure or not weekday:
        raise ValueError("URDB rate has no energy time-of-use schedule")
    if len(weekday) != 12 or len(weekend) != 12:
        raise ValueError("URDB schedule does not have 12 months")
    month = int(month)
    if not 1 <= month <= 12:
        raise ValueError("month must be 1-12")
    prices = [_period_price(p) for p in structure]
    wd = [int(p) for p in weekday[month - 1]]
    we = [int(p) for p in weekend[month - 1]]
    for p in wd + we:
        if not 0 <= p < len(prices):
            raise ValueError(f"URDB schedule uses period {p}, but the rate has {len(prices)} periods")
    groups = [(wd, False)] if wd == we else [(wd, True), (we, False)]
    out: List[TouRate] = []
    for row, weekdays_only in groups:
        for start, end, p in _windows(row):
            out.append(TouRate(name=f"Period {p + 1}", price_eur_per_kwh=prices[p],
                               start_hour=start, end_hour=end, weekdays_only=weekdays_only))
    return out


def load_tou_rates(rate_text: str, api_key: str = "",
                   month: Optional[int] = None) -> Tuple[List[TouRate], Dict[str, Any]]:
    """Fetch a URDB rate and convert it for ``month`` (default: this month)."""
    label = parse_rate_label(rate_text)
    if not label:
        raise ValueError("No URDB rate given")
    rate = fetch_rate(label, api_key)
    return tou_rates_for_month(rate, month or date.today().month), rate


def apply_tou(state: Any, tou: TouConfig) -> None:
    """Save ``tou`` to the config file and hot-reload it, like a settings save."""
    from shelly_analyzer.io.config import save_config
    new_cfg = replace(state.cfg, tou=tou)
    save_config(new_cfg, getattr(state, "_cfg_path", None) or Path("config.json"))
    state.cfg = new_cfg
    state.reload_config(new_cfg)


def refresh_once(state: Any, month: Optional[int] = None) -> bool:
    """Rebuild the TOU windows from the configured URDB rate.

    Saves only when the windows changed; returns True in that case."""
    tou = state.cfg.tou
    if not str(getattr(tou, "urdb_rate", "") or "").strip():
        return False
    rates, rate = load_tou_rates(tou.urdb_rate, tou.urdb_api_key, month)
    if list(rates) == list(tou.rates):
        return False
    apply_tou(state, replace(state.cfg.tou, rates=rates))
    logger.info("URDB TOU windows updated: %s - %s (%d windows)",
                rate.get("utility", "?"), rate.get("name", "?"), len(rates))
    return True


def refresh_loop(state: Any, stop_event: threading.Event) -> None:
    """Refresh once a day, and within an hour after the month changes.

    On an error the current windows stay, and the next try is a day later."""
    last_try = 0.0
    last_month = 0
    if stop_event.wait(60.0):
        return
    while not stop_event.is_set():
        month = date.today().month
        if month != last_month or time.time() - last_try >= 86400:
            last_try, last_month = time.time(), month
            try:
                refresh_once(state, month)
            except Exception as e:
                logger.warning("URDB TOU refresh failed, keeping the current windows: %s", e)
        if stop_event.wait(3600.0):
            return
