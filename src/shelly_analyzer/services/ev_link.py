"""Handing the charge log to a car app — the read-only EV-Tracker link.

Two programs know half of a home charge each. A car app knows *which* car was
plugged in, what its state of charge did and how far it then drove. This
analyzer knows how many kilowatt-hours went through the wallbox and — where a
grid meter and a PV or battery series cover the window — how many of them came
from the sun, from the house battery and from the grid, and what that cost.
Neither can work out the other's half on its own.

So this module publishes the analyzer's half and nothing else:

* **Pull, never push.** The car app asks; the analyzer answers. A push would
  have to guess whether the other side is reachable, keep a queue and retry —
  and would write into a database it cannot see. The reader decides when it
  wants data, and it is the reader that owns the matching.
* **Only settled charges.** While a car is still drawing, the log keeps
  extending the entry and its id moves with the window. A reader that fetched
  mid-charge would file the same charge again once it ended. A charge is
  offered only after ``link_settle_minutes`` of silence.
* **The same numbers as the tab.** Everything here comes out of the very
  ``ev_sessions`` / ``ev_charge_curve`` actions the EV-Log tab is drawn from —
  not a second implementation that could drift away from it.
* **No identity of its own.** The payload says which wallbox the energy went
  through and when. It never claims to know which car that was: with two cars
  on one wallbox only the reader, who knows what its own car did, can say.
"""
from __future__ import annotations

import logging
import secrets
import time
from typing import Any, Callable, Dict, List, Optional

_log = logging.getLogger(__name__)

# What the link may be asked for at once. A car app backfilling its history
# walks years; asking for all of it in one request would read the whole device
# table into memory on a Pi.
MAX_DAYS = 400
DEFAULT_DAYS = 30


def new_link_token() -> str:
    """A fresh link token. 32 hex chars — long enough that guessing is not a
    threat model, short enough to retype off a screen if it comes to that."""
    return secrets.token_hex(16)


def _settle_s(cfg) -> int:
    return max(0, int(getattr(cfg.ev_charging, "link_settle_minutes", 20) or 0)) * 60


def token_ok(cfg, presented: str) -> bool:
    """Does ``presented`` open the link?

    Empty never matches empty: a config with no token set must not be opened by
    a request that sends no token. Compared in constant time so the answer does
    not leak the token one character at a time.
    """
    want = str(getattr(cfg.ev_charging, "link_token", "") or "")
    got = str(presented or "")
    if not want or not got:
        return False
    if not bool(getattr(cfg.ev_charging, "link_enabled", False)):
        return False
    return secrets.compare_digest(want, got)


def _device_name(cfg, key: str) -> str:
    for d in getattr(cfg, "devices", []) or []:
        if str(getattr(d, "key", "")) == key:
            return str(getattr(d, "name", "") or key)
    return key


def _coverage(entry: Dict[str, Any]) -> float:
    """How much of the charge the supply meters actually measured, 0..1.

    A grouped charge has no coverage of its own — it is the energy-weighted
    mean of the sessions it is made of, so a long measured session is not
    outvoted by a two-minute unmeasured one.
    """
    subs = entry.get("sessions") or []
    if not subs:
        return float(entry.get("coverage") or 0.0)
    tot = sum(float(s.get("energy_kwh") or 0.0) for s in subs)
    if tot <= 0:
        vals = [float(s.get("coverage") or 0.0) for s in subs]
        return round(sum(vals) / len(vals), 3) if vals else 0.0
    return round(sum(float(s.get("coverage") or 0.0) * float(s.get("energy_kwh") or 0.0)
                     for s in subs) / tot, 3)


def info(cfg, version: str = "") -> Dict[str, Any]:
    """What the reader needs before it can ask anything useful.

    Chiefly: which wallbox this is. A car app that is bound to a *different*
    wallbox must be able to tell, otherwise two households sharing a link token
    by accident would file each other's charges.
    """
    ec = cfg.ev_charging
    key = str(getattr(ec, "wallbox_device_key", "") or "")
    # Asking the balance chain itself rather than reading SolarConfig here: it
    # also resolves the synthetic series an external PV source writes, and a
    # second resolution would answer "no sun measured" on exactly the installs
    # that do have one.
    from shelly_analyzer.services.energy_balance import _resolve_source_keys
    grid_key, pv_key, batt_key = _resolve_source_keys(cfg)
    return {
        "product": "shelly-energy-analyzer",
        "version": str(version or ""),
        "link_version": 1,
        "enabled": bool(getattr(ec, "enabled", False)),
        "wallbox": {"device_key": key, "name": _device_name(cfg, key)},
        # Every wallbox-shaped device, so a reader with two cars on two boxes
        # can bind each car to the right one instead of guessing from a name.
        "wallboxes": [
            {"device_key": str(getattr(d, "key", "")),
             "name": str(getattr(d, "name", "") or getattr(d, "key", ""))}
            for d in (getattr(cfg, "devices", []) or [])
        ],
        "currency": "EUR",
        "price_eur_per_kwh": float(getattr(cfg.pricing, "electricity_price_eur_per_kwh", 0.0) or 0.0),
        "cost_source_mode": str(getattr(ec, "cost_source_mode", "auto") or "auto"),
        "solar_cost_model": str(getattr(ec, "solar_cost_model", "free") or "free"),
        # Whether a source split is possible here at all. A reader that sees
        # solar=false must not show an empty PV bar and call it "0 % sun" —
        # there is simply no measurement, which is a different statement.
        "sources": {"grid": bool(grid_key), "solar": bool(pv_key), "battery": bool(batt_key)},
        "settle_minutes": int(getattr(ec, "link_settle_minutes", 20) or 0),
        "max_days": MAX_DAYS,
        "server_ts": int(time.time()),
        "tz_offset_s": -int(time.timezone if not time.localtime().tm_isdst else time.altzone),
    }


def charges(on_action: Callable[[str, Dict[str, str]], Dict[str, Any]],
            cfg, params: Dict[str, str]) -> Dict[str, Any]:
    """Finished charges, newest first, shaped for a reader that is not us.

    ``?days=N`` (1..MAX_DAYS) or ``?since=<unix>`` — ``since`` is what a car app
    actually wants on its regular poll ("anything new after my last one"), and
    is translated into the day window the log already caches on, so a poll every
    few minutes keeps hitting that cache instead of re-reading the database.
    """
    ec = cfg.ev_charging
    key = str(getattr(ec, "wallbox_device_key", "") or "")
    if not key:
        return {"ok": False, "error": "no wallbox configured"}

    now = int(time.time())
    try:
        since = int(float(params.get("since") or 0))
    except (TypeError, ValueError):
        since = 0
    if since > 0:
        # One day of overlap on purpose: a charge that was still settling on the
        # last poll must come back on this one, and a re-offered charge costs
        # the reader nothing — it files by id.
        days = max(1, min(MAX_DAYS, int((now - since) / 86400) + 2))
    else:
        try:
            days = int(float(params.get("days") or DEFAULT_DAYS))
        except (TypeError, ValueError):
            days = DEFAULT_DAYS
        days = max(1, min(MAX_DAYS, days))

    res = on_action("ev_sessions", {"days": str(days)})
    if not isinstance(res, dict) or not res.get("ok"):
        return {"ok": False, "error": str((res or {}).get("error") or "ev log unavailable")}
    data = res.get("data") or {}

    # Grouped charges when grouping is on, single sessions otherwise — the same
    # unit the tab lists, so "17 charges" means the same thing on both sides.
    entries: List[Dict[str, Any]] = data.get("charges") or []
    grouped = bool(entries)
    if not grouped:
        entries = data.get("sessions") or []

    cutoff = now - _settle_s(cfg)
    out: List[Dict[str, Any]] = []
    still_running = 0
    for e in entries:
        end_ts = int(e.get("end_ts") or 0)
        start_ts = int(e.get("start_ts") or 0)
        if end_ts <= 0 or start_ts <= 0:
            continue
        if end_ts > cutoff:
            still_running += 1
            continue
        if since and end_ts < since:
            continue
        energy = round(float(e.get("energy_kwh") or 0.0), 3)
        sol = round(float(e.get("solar_kwh") or 0.0), 3)
        bat = round(float(e.get("battery_kwh") or 0.0), 3)
        grd = round(float(e.get("grid_kwh") or 0.0), 3)
        model = str(e.get("cost_model") or "fixed")
        out.append({
            # Stable for the life of the charge: it is derived from the device
            # and the first session's window, both of which stop moving once the
            # charge is over — which is exactly when we start offering it.
            "id": str(e.get("group_id") or e.get("session_id") or ""),
            "device_key": key,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "duration_s": max(0, end_ts - start_ts),
            "energy_kwh": energy,
            # Only where the meters really covered the window. An unmeasured
            # charge reports zeros AND cost_model "fixed" — never a zero that
            # could be read as "no sun today".
            "solar_kwh": sol if model == "source" else None,
            "battery_kwh": bat if model == "source" else None,
            "grid_kwh": grd if model == "source" else None,
            "cost_eur": round(float(e.get("cost_eur") or 0.0), 2),
            "cost_model": model,
            "coverage": _coverage(e),
            "avg_power_w": round(float(e.get("avg_power_w") or 0.0), 1),
            "peak_power_w": round(float(e.get("peak_power_w") or 0.0), 1),
            "session_count": int(e.get("session_count") or 1),
        })

    out.sort(key=lambda c: c["start_ts"], reverse=True)
    payload = info(cfg)
    payload.update({
        "grouped": grouped,
        "window_days": days,
        "since": since or None,
        "charges": out,
        # Named, not hidden: a reader that sees its charge missing should be
        # able to tell "not settled yet" from "not detected".
        "pending_settle": still_running,
    })
    return {"ok": True, "data": payload}


def curve(on_action: Callable[[str, Dict[str, str]], Dict[str, Any]],
          cfg, params: Dict[str, str]) -> Dict[str, Any]:
    """One charge as a curve — the very payload the EV-Log tab draws.

    Handed through unchanged on purpose: the reader is meant to draw the same
    picture, and any reshaping here would be a second chance for the two to
    disagree.
    """
    if not str(getattr(cfg.ev_charging, "wallbox_device_key", "") or ""):
        return {"ok": False, "error": "no wallbox configured"}
    res = on_action("ev_charge_curve", {"start": str(params.get("start") or ""),
                                        "end": str(params.get("end") or "")})
    if not isinstance(res, dict):
        return {"ok": False, "error": "curve unavailable"}
    return res


def handle(route: str, params: Dict[str, str], cfg,
           on_action: Optional[Callable[[str, Dict[str, str]], Dict[str, Any]]],
           version: str = "") -> Dict[str, Any]:
    """Route ``/api/v1/ev/<route>``. Unknown routes are an error, not a guess."""
    r = (route or "").strip("/")
    if r in ("", "info"):
        return {"ok": True, "data": info(cfg, version)}
    if on_action is None:
        return {"ok": False, "error": "not available"}
    if r == "charges":
        return charges(on_action, cfg, params)
    if r == "curve":
        return curve(on_action, cfg, params)
    return {"ok": False, "error": "unknown endpoint: %s" % r}
