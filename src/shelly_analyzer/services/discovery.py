from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from shelly_analyzer.io.http import HttpConfig, ShellyHttp, rpc_call


@dataclass(frozen=True)
class DiscoveredDevice:
    host: str
    gen: int
    model: str
    kind: str  # "em" | "switch" | "unknown"
    component_id: int
    phases: int
    supports_emdata: bool
    probed_at: int

    def label(self) -> str:
        base = (self.model or "").strip() or "Shelly"
        if self.kind == "em":
            return f"{base} (EM)"
        if self.kind == "switch":
            return f"{base} (Switch)"
        return base


def _coerce_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return int(default)


def _json_from_response(resp) -> Optional[Dict[str, Any]]:
    try:
        if resp is None:
            return None
        # Some Shellys respond with text/plain but valid JSON
        return resp.json()
    except Exception:
        return None


def _detect_gen1_kind(status: Dict[str, Any]) -> Tuple[str, int, int, bool]:
    # Returns (kind, component_id, phases, supports_emdata)
    kind = "unknown"
    component_id = 0
    phases = 1
    supports_emdata = False

    if isinstance(status.get("emeters"), list) and status["emeters"]:
        kind = "em"
        phases = 3 if len(status["emeters"]) >= 3 else 1
        supports_emdata = True  # Gen1 EM/3EM have CSV endpoints
        return kind, component_id, phases, supports_emdata

    if isinstance(status.get("meters"), list) and status["meters"]:
        # e.g. Shelly 1PM
        kind = "switch"
        phases = 1
        return kind, component_id, phases, supports_emdata

    if isinstance(status.get("relays"), list) and status["relays"]:
        kind = "switch"
        phases = 1
        return kind, component_id, phases, supports_emdata

    return kind, component_id, phases, supports_emdata


def _detect_gen2_kind(status: Dict[str, Any]) -> Tuple[str, int, int]:
    kind = "unknown"
    component_id = 0
    phases = 1

    # Components in Shelly Gen2 status use keys like "em:0", "switch:0"
    if isinstance(status, dict):
        # Prefer EM components first
        for k, v in status.items():
            if isinstance(k, str) and k.startswith("em:"):
                kind = "em"
                component_id = _coerce_int(k.split(":", 1)[1], 0)
                phases = 1
                if isinstance(v, dict):
                    # 3-phase devices typically expose a/b/c voltage/current keys
                    keys = set(v.keys())
                    if any(x in keys for x in ("b_voltage", "c_voltage", "b_current", "c_current", "b_act_power", "c_act_power")):
                        phases = 3
                    # Some firmwares use a_/b_/c_ prefixes
                    if any(x in keys for x in ("a_voltage", "b_voltage", "c_voltage", "a_current", "b_current", "c_current")):
                        phases = 3
                return kind, component_id, phases

        # Then switch/relay/light components
        for k in status.keys():
            if isinstance(k, str) and (k.startswith("switch:") or k.startswith("relay:") or k.startswith("light:")):
                kind = "switch"
                component_id = _coerce_int(k.split(":", 1)[1], 0)
                return kind, component_id, phases

    return kind, component_id, phases

def probe_device(host: str, timeout_seconds: float = 2.0) -> DiscoveredDevice:
    """Probe a host and return Shelly details.

    STRICT detection: if the host does not look like a Shelly (Gen1 or Gen2),
    this function raises ValueError to avoid false positives during scans.
    """
    host = str(host or "").strip()
    if not host:
        raise ValueError("empty host")

    http = ShellyHttp(HttpConfig(timeout_seconds=float(timeout_seconds), retries=1, backoff_base_seconds=0.1))
    now = int(time.time())

    # --- Try Gen2 RPC device info ---
    model = ""
    kind = "unknown"
    gen = 0
    component_id = 0
    phases = 1
    supports_emdata = False

    devinfo: Optional[Dict[str, Any]] = None
    status: Optional[Dict[str, Any]] = None

    try:
        devinfo = rpc_call(http, host, "Shelly.GetDeviceInfo")
        if isinstance(devinfo, dict):
            # typical keys: id, mac, model, app, gen
            model = str(devinfo.get("model") or devinfo.get("app") or "")
            gen = 2
    except Exception:
        devinfo = None

    if devinfo is not None:
        # It's very likely a Shelly Gen2 if we got a dict with some identifiers
        if not any(k in devinfo for k in ("id", "mac", "model", "app")):
            raise ValueError("not a Shelly (rpc deviceinfo missing keys)")

        try:
            status = rpc_call(http, host, "Shelly.GetStatus")
        except Exception:
            status = None

        if isinstance(status, dict):
            kind, component_id, phases = _detect_gen2_kind(status)
            # Model heuristic: treat known 3-phase EM devices as 3-phase even if status lacks b_/c_ keys
            if kind == "em" and int(phases) <= 1:
                ml = (model or "").lower()
                if ml.startswith("spem-003") or "3em" in ml:
                    phases = 3


            # emdata CSV is mainly Gen1; keep False for Gen2
            supports_emdata = False

        return DiscoveredDevice(
            host=host,
            gen=gen or 2,
            model=model.strip() or "Shelly",
            kind=kind,
            component_id=int(component_id),
            phases=int(phases),
            supports_emdata=bool(supports_emdata),
            probed_at=now,
        )

    # --- Try Gen1 endpoints: /shelly + /status ---
    try:
        r = http.get(f"http://{host}/shelly")
        j = _json_from_response(r)
        if isinstance(j, dict) and ("type" in j or "mac" in j):
            gen = 1
            model = str(j.get("type") or j.get("model") or "Shelly").strip()
        else:
            j = None
    except Exception:
        j = None

    if gen == 1:
        try:
            rs = http.get(f"http://{host}/status")
            st = _json_from_response(rs)
        except Exception:
            st = None

        if not isinstance(st, dict):
            # still accept, because /shelly was valid
            st = {}

        kind, component_id, phases, supports_emdata = _detect_gen1_kind(st)

        return DiscoveredDevice(
            host=host,
            gen=1,
            model=model or "Shelly",
            kind=kind,
            component_id=int(component_id),
            phases=int(phases),
            supports_emdata=bool(supports_emdata),
            probed_at=now,
        )

    # No Shelly signature matched
    raise ValueError("not a Shelly")
