"""Network traffic monitor – tracks bytes sent/received across all HTTP calls."""

from __future__ import annotations

import json
import threading
import time
import urllib.request
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
import requests.adapters

# ---------------------------------------------------------------------------
# Categories derived from URL patterns
# ---------------------------------------------------------------------------

_CATEGORY_RULES: List[Tuple[Callable[[str], bool], str]] = [
    (lambda u: "/rpc/" in u or "/emdata/" in u or "/relay/" in u or "/status" in u, "shelly"),
    (lambda u: "entsoe.eu" in u, "entsoe"),
    (lambda u: "energy-charts.info" in u or "awattar.de" in u or "awattar.at" in u, "spot_price"),
    (lambda u: "openweathermap.org" in u, "weather"),
    (lambda u: "api.telegram.org" in u, "telegram"),
    (lambda u: "api.github.com" in u or "raw.githubusercontent.com" in u, "github"),
    (lambda u: "127.0.0.1" in u or "localhost" in u, "local"),
]


def _categorize(url: str) -> str:
    low = url.lower()
    for test, cat in _CATEGORY_RULES:
        try:
            if test(low):
                return cat
        except Exception:
            continue
    return "other"


# ---------------------------------------------------------------------------
# Traffic record
# ---------------------------------------------------------------------------

class _Record:
    __slots__ = ("requests_count", "bytes_sent", "bytes_received")

    def __init__(self) -> None:
        self.requests_count = 0
        self.bytes_sent = 0
        self.bytes_received = 0

    def add(self, sent: int, received: int) -> None:
        self.requests_count += 1
        self.bytes_sent += sent
        self.bytes_received += received


# ---------------------------------------------------------------------------
# TrafficMonitor singleton
# ---------------------------------------------------------------------------

class TrafficMonitor:
    """Global network traffic tracker.  Call ``install()`` once at startup."""

    _instance: Optional["TrafficMonitor"] = None

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # category -> _Record (cumulative since app start)
        self._totals: Dict[str, _Record] = {}
        # Per-second snapshots for live rate calculation
        self._recent: List[Tuple[float, int, int]] = []  # (ts, sent, recv)
        self._start_ts = time.time()
        self._installed = False

    # -- public API ----------------------------------------------------------

    @classmethod
    def get(cls) -> "TrafficMonitor":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def install(self) -> None:
        """Monkey-patch requests + urllib to intercept traffic."""
        if self._installed:
            return
        self._installed = True
        self._patch_requests()
        self._patch_urllib()

    def record(self, url: str, bytes_sent: int, bytes_received: int) -> None:
        cat = _categorize(url)
        ts = time.time()
        with self._lock:
            if cat not in self._totals:
                self._totals[cat] = _Record()
            self._totals[cat].add(bytes_sent, bytes_received)
            self._recent.append((ts, bytes_sent, bytes_received))
            # Keep only last 120 seconds
            cutoff = ts - 120
            while self._recent and self._recent[0][0] < cutoff:
                self._recent.pop(0)

    def snapshot(self) -> Dict[str, Any]:
        """Return current stats for UI display."""
        with self._lock:
            now = time.time()
            uptime = max(1, now - self._start_ts)
            cats: Dict[str, Dict[str, int]] = {}
            total_sent = 0
            total_recv = 0
            total_reqs = 0
            for cat, rec in self._totals.items():
                cats[cat] = {
                    "requests": rec.requests_count,
                    "sent": rec.bytes_sent,
                    "received": rec.bytes_received,
                }
                total_sent += rec.bytes_sent
                total_recv += rec.bytes_received
                total_reqs += rec.requests_count

            # Live rate (last 10 seconds)
            cutoff_10 = now - 10
            rate_sent = 0
            rate_recv = 0
            for ts, s, r in self._recent:
                if ts >= cutoff_10:
                    rate_sent += s
                    rate_recv += r
            rate_sent_ps = rate_sent / 10.0
            rate_recv_ps = rate_recv / 10.0

            return {
                "uptime_s": int(uptime),
                "total_sent": total_sent,
                "total_received": total_recv,
                "total_requests": total_reqs,
                "rate_sent_bps": rate_sent_ps,
                "rate_recv_bps": rate_recv_ps,
                "categories": cats,
            }

    # -- monkey-patching -----------------------------------------------------

    def _patch_requests(self) -> None:
        original_send = requests.adapters.HTTPAdapter.send

        monitor = self

        def patched_send(adapter_self: Any, request: Any, *args: Any, **kwargs: Any) -> Any:
            # Estimate bytes sent
            sent = len(request.body or b"") if request.body else 0
            sent += len(str(request.headers)) if request.headers else 0
            url = str(request.url or "")

            resp = original_send(adapter_self, request, *args, **kwargs)

            # Estimate bytes received
            recv = 0
            if resp is not None:
                content = getattr(resp, "_content", None)
                if content:
                    recv = len(content)
                else:
                    recv = int(resp.headers.get("Content-Length", 0))
                recv += len(str(resp.headers)) if resp.headers else 0

            monitor.record(url, sent, recv)
            return resp

        requests.adapters.HTTPAdapter.send = patched_send  # type: ignore[assignment]

    def _patch_urllib(self) -> None:
        original_urlopen = urllib.request.urlopen

        monitor = self

        def patched_urlopen(url_or_req: Any, data: Any = None, timeout: Any = None, **kwargs: Any) -> Any:
            # Extract URL
            if isinstance(url_or_req, urllib.request.Request):
                url = url_or_req.full_url
                sent = len(url_or_req.data or b"") if url_or_req.data else 0
                sent += sum(len(k) + len(v) for k, v in url_or_req.header_items())
            else:
                url = str(url_or_req)
                sent = len(data or b"") if data else 0

            # Build kwargs for original call
            call_kwargs: Dict[str, Any] = {}
            if data is not None:
                call_kwargs["data"] = data
            if timeout is not None:
                call_kwargs["timeout"] = timeout
            call_kwargs.update(kwargs)

            resp = original_urlopen(url_or_req, **call_kwargs)

            # Wrap response to track bytes read
            recv = 0
            if hasattr(resp, "length") and resp.length:
                recv = int(resp.length)
            elif hasattr(resp, "headers"):
                recv = int(resp.headers.get("Content-Length", 0))

            monitor.record(url, sent, recv)
            return resp

        urllib.request.urlopen = patched_urlopen  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def fmt_bytes(n: int) -> str:
    """Format bytes to human readable string."""
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    if n < 1024 * 1024 * 1024:
        return f"{n / (1024 * 1024):.1f} MB"
    return f"{n / (1024 * 1024 * 1024):.2f} GB"


def fmt_rate(bps: float) -> str:
    """Format bytes/sec to human readable rate."""
    if bps < 1024:
        return f"{bps:.0f} B/s"
    if bps < 1024 * 1024:
        return f"{bps / 1024:.1f} KB/s"
    return f"{bps / (1024 * 1024):.1f} MB/s"


_CAT_LABELS = {
    "shelly": "Shelly Devices",
    "entsoe": "ENTSO-E API",
    "spot_price": "Spot Prices",
    "weather": "OpenWeather",
    "telegram": "Telegram",
    "github": "GitHub",
    "local": "Local/Web",
    "other": "Other",
}

_CAT_ICONS = {
    "shelly": "\U0001f50c",
    "entsoe": "\U0001f33f",
    "spot_price": "\u26a1",
    "weather": "\U0001f321\ufe0f",
    "telegram": "\U0001f4ac",
    "github": "\U0001f504",
    "local": "\U0001f3e0",
    "other": "\U0001f4e1",
}
