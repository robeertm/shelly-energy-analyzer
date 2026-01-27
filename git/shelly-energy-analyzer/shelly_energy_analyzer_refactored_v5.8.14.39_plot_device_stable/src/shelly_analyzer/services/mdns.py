from __future__ import annotations

import re
import time
import threading
from dataclasses import dataclass
from typing import Dict, List, Optional

try:
    # zeroconf is the de-facto Python mDNS/DNS-SD implementation
    from zeroconf import Zeroconf, ServiceBrowser, ServiceStateChange
except Exception as e:  # pragma: no cover
    Zeroconf = None  # type: ignore
    ServiceBrowser = None  # type: ignore
    ServiceStateChange = None  # type: ignore
    _IMPORT_ERROR = e
else:
    _IMPORT_ERROR = None


@dataclass(frozen=True)
class MdnsShelly:
    name: str          # instance name (without ._shelly._tcp.local.)
    host: str          # IPv4 address
    port: int
    model: str = ""
    gen: int = 0
    service_type: str = ""
    txt: Dict[str, str] = None  # type: ignore


_SHELLY_NAME_RE = re.compile(r"^shelly", re.IGNORECASE)
_MAC_SUFFIX_RE = re.compile(r"-[0-9a-f]{6,}$", re.IGNORECASE)


def _decode_txt(props: Dict[bytes, bytes]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in (props or {}).items():
        try:
            ks = k.decode("utf-8", "ignore")
        except Exception:
            ks = str(k)
        try:
            vs = v.decode("utf-8", "ignore")
        except Exception:
            vs = str(v)
        out[ks] = vs
    return out


def discover_shelly_mdns(timeout_seconds: float = 3.5) -> List[MdnsShelly]:
    """
    Discover Shelly devices via mDNS (DNS-SD) on the local network.

    Shelly devices advertise `_shelly._tcp.local.` (Gen2+) and also `_http._tcp.local.`.
    We browse both and filter for instances that look like Shelly.
    """
    if Zeroconf is None:
        raise RuntimeError(f"zeroconf not available: {_IMPORT_ERROR}")

    service_types = [
        "_shelly._tcp.local.",
        "_http._tcp.local.",
    ]

    zc = Zeroconf()
    found: Dict[str, MdnsShelly] = {}
    lock = threading.Lock()

    def _maybe_add(service_type: str, name: str) -> None:
        # Filter early: for _http we only want Shelly-like instance names.
        inst = (name or "").split(".")[0]
        if service_type.startswith("_http") and not _SHELLY_NAME_RE.search(inst):
            return

        try:
            info = zc.get_service_info(service_type, name, timeout=1200)
        except Exception:
            return
        if not info:
            return

        try:
            addrs = info.parsed_addresses()
        except Exception:
            addrs = []
        if not addrs:
            return

        host = addrs[0]
        port = int(getattr(info, "port", 80) or 80)
        txt = _decode_txt(getattr(info, "properties", {}) or {})
        model = (txt.get("app") or txt.get("model") or "").strip()
        gen = 0
        try:
            gen = int((txt.get("gen") or "0").strip() or 0)
        except Exception:
            gen = 0

        # Fallback model guess from instance prefix (common on Gen1 as well).
        if not model:
            guess = _MAC_SUFFIX_RE.sub("", inst)
            model = guess.upper()

        key = f"{host}:{port}"
        dev = MdnsShelly(
            name=inst,
            host=host,
            port=port,
            model=model,
            gen=gen,
            service_type=service_type,
            txt=txt,
        )
        with lock:
            found[key] = dev

    def on_change(zeroconf=None, service_type: str = "", name: str = "", state_change=None, **_kw) -> None:
        try:
            if state_change == ServiceStateChange.Added:
                _maybe_add(service_type, name)
        except Exception:
            pass

    browsers: List[ServiceBrowser] = []
    try:
        for st in service_types:
            try:
                browsers.append(ServiceBrowser(zc, st, handlers=[on_change]))
            except Exception:
                # Some platforms might not like one of the service types; ignore.
                pass

        # Collect for timeout_seconds
        t0 = time.time()
        while (time.time() - t0) < max(0.5, timeout_seconds):
            time.sleep(0.05)

    finally:
        try:
            for b in browsers:
                try:
                    b.cancel()
                except Exception:
                    pass
        finally:
            try:
                zc.close()
            except Exception:
                pass

    # Stable ordering
    out = list(found.values())
    out.sort(key=lambda d: (d.host, d.name))
    return out
