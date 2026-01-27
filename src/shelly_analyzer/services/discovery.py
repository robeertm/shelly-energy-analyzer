from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from shelly_analyzer.io.http import HttpConfig, ShellyHttp, build_csv_url, rpc_call


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
        base = self.model.strip() or "Shelly"
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


def _detect_components(status: Dict[str, Any]) -> Tuple[str, int, int]:
    """Return (kind, component_id, phases).

    Prefer EM meters over switch devices. For Gen2 3-phase meters, an EM component
    typically exists with id 0 and EMData with id 0.
    """
    keys = [k for k in status.keys() if isinstance(k, str)]

    em_ids = []
    sw_ids = []
    for k in keys:
        m = re.match(r"^(em|switch):(\d+)$", k)
        if not m:
            continue
        typ, sid = m.group(1), _coerce_int(m.group(2), 0)
        if typ == "em":
            em_ids.append(sid)
        elif typ == "switch":
            sw_ids.append(sid)

    if em_ids:
        return "em", min(em_ids), 3
    if sw_ids:
        return "switch", min(sw_ids), 1
    return "unknown", 0, 1


def _status_has_emdata(status: Dict[str, Any]) -> bool:
    # EMData components are identified with emdata:<id> keys in Shelly.GetStatus
    # in many Gen2 firmwares.
    for k in status.keys():
        if isinstance(k, str) and re.match(r"^emdata:\d+$", k):
            return True
    return False


def _list_methods(client: ShellyHttp, host: str) -> Optional[list[str]]:
    try:
        data = rpc_call(client, host, "Shelly.ListMethods")
    except Exception:
        return None

    # Possible shapes: {"methods": [..]} or {"result": {"methods": [..]}} etc.
    methods: Optional[list[str]] = None
    if isinstance(data.get("methods"), list):
        methods = [str(m) for m in data.get("methods") if isinstance(m, str)]
    elif isinstance(data.get("result"), dict) and isinstance(data["result"].get("methods"), list):
        methods = [str(m) for m in data["result"].get("methods") if isinstance(m, str)]
    else:
        # Sometimes returned as a dict of method -> metadata
        if all(isinstance(k, str) for k in data.keys()):
            methods = [str(k) for k in data.keys()]

    return methods


def _supports_emdata(client: ShellyHttp, host: str, em_id: int, status: Optional[Dict[str, Any]] = None) -> bool:
    """Best-effort detection of EMData CSV support.

    We intentionally avoid calling EMData.GetRecords with fixed parameters here,
    because different FW versions can be picky. Instead:
    1) Look for emdata:<id> in status
    2) Check Shelly.ListMethods for EMData.*
    3) As last resort, try a tiny CSV download request.
    """
    if isinstance(status, dict) and _status_has_emdata(status):
        return True

    methods = _list_methods(client, host)
    if methods:
        if any(m.startswith("EMData.") for m in methods):
            return True
        # Some devices expose EM1Data (single-phase meters)
        if any(m.startswith("EM1Data.") for m in methods):
            return True

    # Last resort: attempt a minimal CSV request. If it 200's, we support it.
    try:
        now = int(time.time())
        ts = max(0, now - 60)
        url = build_csv_url(host, int(em_id), ts, now, add_keys=True)
        _ = client.get(url)
        return True
    except Exception:
        return False


def probe_device(host: str, timeout_seconds: float = 2.0) -> DiscoveredDevice:
    """Probe a Shelly by IP/host and infer model + what API component to use.

    Designed to be fast and safe to call from the GUI.
    """
    http = ShellyHttp(HttpConfig(timeout_seconds=float(timeout_seconds), retries=1, backoff_base_seconds=0.1))
    now = int(time.time())

    # Defaults
    gen = 0
    model = ""
    kind = "unknown"
    component_id = 0
    phases = 1
    supports_emdata = False

    info: Optional[Dict[str, Any]] = None
    status: Optional[Dict[str, Any]] = None

    # Gen2+/Plus/Pro: Shelly.GetDeviceInfo + Shelly.GetStatus
    try:
        info = rpc_call(http, host, "Shelly.GetDeviceInfo")
        if isinstance(info, dict):
            gen = _coerce_int(info.get("gen", info.get("generation", 0)), 0)
            model = str(info.get("model", info.get("app", "")) or "")
    except Exception:
        info = None

    if info is not None:
        try:
            status = rpc_call(http, host, "Shelly.GetStatus")
        except Exception:
            status = None

    if isinstance(status, dict):
        kind, component_id, phases = _detect_components(status)
        if kind == "em":
            supports_emdata = _supports_emdata(http, host, component_id, status=status)
        else:
            supports_emdata = False

        if not model:
            model = "Shelly"

        return DiscoveredDevice(
            host=str(host),
            gen=int(gen),
            model=str(model),
            kind=str(kind),
            component_id=int(component_id),
            phases=int(phases),
            supports_emdata=bool(supports_emdata),
            probed_at=now,
        )

    # Fallback: try /shelly (works for many devices, also Gen1)
    try:
        r = http.get(f"http://{host}/shelly")
        data = r.json()
        if isinstance(data, dict):
            model = str(data.get("model", data.get("type", "Shelly")) or "Shelly")
            gen = _coerce_int(data.get("gen", 1), 1)
    except Exception:
        pass

    # If Gen1, we can try /status to detect 3EM/EM and mark it as EM-capable.
    if int(gen) <= 1:
        try:
            st = http.get(f"http://{host}/status").json()
            if isinstance(st, dict) and isinstance(st.get("emeters"), list) and len(st.get("emeters")) >= 1:
                # Shelly EM has 2, Shelly 3EM has 3 emeters.
                em_len = len(st.get("emeters"))
                kind = "em"
                phases = 3 if em_len >= 3 else 1
                component_id = 0
                # Gen1 has CSV export per phase at /emeter/{idx}/em_data.csv
                supports_emdata = True
        except Exception:
            pass

    return DiscoveredDevice(
        host=str(host),
        gen=int(gen),
        model=str(model or "Shelly"),
        kind=str(kind),
        component_id=int(component_id),
        phases=int(phases),
        supports_emdata=bool(supports_emdata),
        probed_at=now,
    )
