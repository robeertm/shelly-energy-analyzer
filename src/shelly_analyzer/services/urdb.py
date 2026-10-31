"""Electricity price and Time-of-Use windows from the OpenEI Utility Rate Database (URDB).

Owns: the search for the residential rates near an address, fetching one URDB
rate, and turning it into Settings -> Pricing (price per kWh, base fee) and
Settings -> TOU (the windows of one month), plus the daily refresh loop.
Needs: a rate label (or the rate page URL) and an OpenEI API key; an empty key
uses ``DEMO_KEY``, which allows only 10 requests per day for each IP address.
Changes: ``cfg.pricing`` and ``cfg.tou`` through :func:`apply_config`
(config file + hot reload).
Tests: ``tests/test_urdb.py``.

Limits, on purpose, to keep the existing price model unchanged:
- only the schedule of the current month (re-applied when the month changes),
- only the first tier of each period (``rate + adj``, $/kWh),
- the fixed charge becomes the base fee; minimum and demand charges are ignored,
- holidays count as ordinary days.
"""
from __future__ import annotations

import json
import logging
import re
import threading
import time
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from shelly_analyzer.io.config import TouRate

logger = logging.getLogger(__name__)

API_URL = "https://api.openei.org/utility_rates"
DEMO_KEY = "DEMO_KEY"
# A URDB label is a 24-digit hex id, e.g. .../IURDB/rate/view/5cc0e8c45457a3d8137b23c6
_LABEL_RE = re.compile(r"\b([0-9a-fA-F]{24})\b")
# Upper bound for one API answer. A full rate is a few kB, a search with
# limit=500 in "minimal" detail well under 1 MB.
MAX_RESPONSE_BYTES = 8 * 1024 * 1024


def parse_rate_label(text: str) -> str:
    """Return the URDB label from a label or from a rate page URL.

    Only the 24-digit hex id is used; the rest of the text (for example the
    host of a pasted URL) is never requested. Empty text gives ""."""
    text = str(text or "").strip()
    if not text:
        return ""
    m = _LABEL_RE.search(text)
    if m:
        return m.group(1).lower()
    raise ValueError("No URDB rate label (24 hex digits) in this text")


def _query(params: Dict[str, Any], api_key: str, timeout: float) -> Dict[str, Any]:
    """One URDB API request (version 8, JSON). Raises RuntimeError on errors.

    The key goes in the ``X-Api-Key`` header, not in the URL, so it never shows
    up in a logged request line or in the text of a network error. Redirects are
    never followed: ``requests`` would carry the header to the new host."""
    params = dict(params, version=8, format="json")
    headers = {"X-Api-Key": api_key or DEMO_KEY, "Accept": "application/json"}
    try:
        resp = requests.get(API_URL, params=params, headers=headers, timeout=timeout, stream=True,
                            allow_redirects=False)
    except requests.RequestException as e:
        raise RuntimeError(f"URDB request failed ({type(e).__name__})") from None
    try:
        body = b""
        for chunk in resp.iter_content(chunk_size=65536):
            body += chunk
            if len(body) > MAX_RESPONSE_BYTES:
                raise RuntimeError("URDB: answer too large")
    except RuntimeError:
        raise
    except Exception as e:  # read errors (requests or urllib3)
        raise RuntimeError(f"URDB request failed ({type(e).__name__})") from None
    finally:
        try:
            resp.close()
        except Exception:
            pass
    if 300 <= resp.status_code < 400:
        raise RuntimeError(f"URDB: unexpected redirect (HTTP {resp.status_code})")
    try:
        data = json.loads(body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
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
    if not isinstance(items, list) or not items or not isinstance(items[0], dict):
        raise RuntimeError(f"URDB: rate {label} not found")
    return items[0]


# URDB maps a point to one utility. A big city can be in the area of a second
# utility (a city utility plus the regional one), so the search also takes
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
    try:
        return round(float(t.get("rate") or 0.0) + float(t.get("adj") or 0.0), 6)
    except (TypeError, ValueError):
        raise ValueError("URDB rate has a price that is not a number") from None


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


def _month_schedule(rate: Dict[str, Any], month: int) -> Tuple[List[float], List[int], List[int]]:
    """Period prices and the weekday/weekend rows (24 period indexes) of ``month``.

    Raises ValueError with a readable message for every shape URDB can send
    that this import cannot use (no energy charges, missing or short schedules,
    unknown periods), so a caller never crashes on a TypeError/IndexError and
    the settings stay as they were."""
    month = int(month)
    if not 1 <= month <= 12:
        raise ValueError("month must be 1-12")
    if not isinstance(rate, dict):
        raise ValueError("URDB rate is not an object")
    structure = rate.get("energyratestructure")
    weekday = rate.get("energyweekdayschedule")
    weekend = rate.get("energyweekendschedule") or weekday
    if not isinstance(structure, list) or not structure:
        raise ValueError("URDB rate has no energy charges (no price per kWh)")
    if not isinstance(weekday, list) or not weekday:
        raise ValueError("URDB rate has no energy schedule")
    if not isinstance(weekend, list) or len(weekday) != 12 or len(weekend) != 12:
        raise ValueError("URDB schedule does not have 12 months")
    prices = [_period_price(p) for p in structure]
    rows = []
    for sched in (weekday, weekend):
        row = sched[month - 1]
        if not isinstance(row, list) or len(row) != 24:
            raise ValueError("URDB schedule row does not have 24 hours")
        try:
            rows.append([int(p) for p in row])
        except (TypeError, ValueError):
            raise ValueError("URDB schedule has a period that is not a number") from None
    wd, we = rows
    for p in wd + we:
        if not 0 <= p < len(prices):
            raise ValueError(f"URDB schedule uses period {p}, but the rate has {len(prices)} periods")
    return prices, wd, we


def tou_rates_for_month(rate: Dict[str, Any], month: int) -> List[TouRate]:
    """TOU windows for ``month`` (1-12).

    Weekday windows (Mon-Fri only) come first and cover every weekday hour.
    The weekend windows follow, so with "first matching rate wins" they only
    match on Saturday and Sunday. When both rows are the same, one set of
    all-days windows is returned."""
    prices, wd, we = _month_schedule(rate, month)
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


def price_for_month(rate: Dict[str, Any], month: int) -> float:
    """One price per kWh for ``month``: the first-tier prices averaged over the
    hours of a week (5 weekdays, 2 weekend days). A flat rate gives its price."""
    prices, wd, we = _month_schedule(rate, month)
    total = sum(prices[p] for p in wd) * 5 + sum(prices[p] for p in we) * 2
    return round(total / (24 * 7), 6)


# URDB fixed charge units -> factor to a yearly base fee.
_FEE_PER_YEAR = {"$/month": 12.0, "$/day": 365.0, "$/year": 1.0}


def fixed_fee_per_year(rate: Dict[str, Any]) -> Optional[float]:
    """The rate's fixed charge as a yearly base fee, or None if URDB has none."""
    v = rate.get("fixedchargefirstmeter")
    factor = _FEE_PER_YEAR.get(str(rate.get("fixedchargeunits") or "$/month"))
    if v is None or factor is None:
        return None
    try:
        return round(float(v) * factor, 2)
    except (TypeError, ValueError):
        return None


def config_from_rate(cfg: Any, rate: Dict[str, Any], month: int, **urdb_fields: str) -> Any:
    """``cfg`` with Settings -> Pricing and Settings -> TOU set from a URDB rate.

    Pricing gets the price per kWh for ``month`` and the base fee (both as final
    prices, "incl. VAT"). TOU gets the windows of ``month`` and is on only when
    the month has more than one price. ``urdb_fields`` (urdb_rate, urdb_api_key,
    urdb_location) are stored with the TOU settings."""
    rates = tou_rates_for_month(rate, month)
    tou = replace(cfg.tou, rates=rates, enabled=len({r.price_eur_per_kwh for r in rates}) > 1, **urdb_fields)
    p: Dict[str, Any] = {"electricity_price_eur_per_kwh": price_for_month(rate, month), "price_includes_vat": True}
    fee = fixed_fee_per_year(rate)
    if fee is not None:
        p.update(base_fee_eur_per_year=fee, base_fee_includes_vat=True)
    return replace(cfg, tou=tou, pricing=replace(cfg.pricing, **p))


def apply_config(state: Any, new_cfg: Any) -> None:
    """Save ``new_cfg`` to the config file and hot-reload it, like a settings save."""
    from shelly_analyzer.io.config import save_config
    save_config(new_cfg, getattr(state, "_cfg_path", None) or Path("config.json"))
    state.cfg = new_cfg
    state.reload_config(new_cfg)


def refresh_once(state: Any, month: Optional[int] = None) -> bool:
    """Set Pricing and TOU again from the configured URDB rate.

    Saves only when something changed; returns True in that case."""
    tou = state.cfg.tou
    if not str(getattr(tou, "urdb_rate", "") or "").strip():
        return False
    label = parse_rate_label(tou.urdb_rate)
    rate = fetch_rate(label, tou.urdb_api_key)
    new_cfg = config_from_rate(state.cfg, rate, month or date.today().month)
    if new_cfg.tou == state.cfg.tou and new_cfg.pricing == state.cfg.pricing:
        return False
    apply_config(state, new_cfg)
    logger.info("URDB rate applied: %s - %s (%.5f/kWh, %d TOU windows)",
                rate.get("utility", "?"), rate.get("name", "?"),
                new_cfg.pricing.electricity_price_eur_per_kwh, len(new_cfg.tou.rates))
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
