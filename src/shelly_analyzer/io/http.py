from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests


@dataclass(frozen=True)
class HttpConfig:
    timeout_seconds: float = 8.0
    retries: int = 3
    backoff_base_seconds: float = 1.5
    # Note: this string is intentionally static to keep the HTTP layer standalone.
    user_agent: str = "ShellyEnergyAnalyzer (+requests)"


class ShellyHttp:
    """Centralized HTTP client (retry/backoff, consistent timeouts, shared session)."""

    def __init__(self, cfg: Optional[HttpConfig] = None) -> None:
        self.cfg = cfg or HttpConfig()
        self._sess = requests.Session()

    def get(self, url: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> requests.Response:
        base_headers = {"User-Agent": self.cfg.user_agent}
        if headers:
            base_headers.update(headers)

        last_err: Optional[BaseException] = None
        for attempt in range(1, self.cfg.retries + 1):
            try:
                r = self._sess.get(url, params=params, headers=base_headers, timeout=self.cfg.timeout_seconds)
                r.raise_for_status()
                return r
            except Exception as e:
                last_err = e
                if attempt < self.cfg.retries:
                    time.sleep(self.cfg.backoff_base_seconds * (2 ** (attempt - 1)))
        assert last_err is not None
        raise last_err

    def post(
        self,
        url: str,
        json_body: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> requests.Response:
        base_headers = {"User-Agent": self.cfg.user_agent, "Content-Type": "application/json"}
        if headers:
            base_headers.update(headers)

        last_err: Optional[BaseException] = None
        for attempt in range(1, self.cfg.retries + 1):
            try:
                r = self._sess.post(url, json=json_body or {}, headers=base_headers, timeout=self.cfg.timeout_seconds)
                r.raise_for_status()
                return r
            except Exception as e:
                last_err = e
                if attempt < self.cfg.retries:
                    time.sleep(self.cfg.backoff_base_seconds * (2 ** (attempt - 1)))
        assert last_err is not None
        raise last_err


def _normalize_rpc_params(obj: Any) -> Any:
    """Normalize params so Shelly RPC accepts them reliably.

    Notably, some firmwares are picky about boolean casing when sent as
    query parameters. We prefer POST+JSON, but we still normalize values
    to be safe.
    """
    if isinstance(obj, bool):
        return bool(obj)
    if isinstance(obj, (int, float, str)) or obj is None:
        return obj
    if isinstance(obj, dict):
        return {str(k): _normalize_rpc_params(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_normalize_rpc_params(x) for x in obj]
    # fallback for unknown types
    return str(obj)


def build_csv_url(host: str, em_id: int, ts: int, end_ts: int, add_keys: bool = True) -> str:
    base = f"http://{host}/emdata/{em_id}/data.csv"
    params = []
    if add_keys:
        params.append("add_keys=true")
    params.append(f"ts={int(ts)}")
    params.append(f"end_ts={int(end_ts)}")
    return base + "?" + "&".join(params)


def build_rpc_url(host: str, method: str) -> str:
    # Shelly Gen2+ uses /rpc/MethodName
    return f"http://{host}/rpc/{method}"


def rpc_call(client: ShellyHttp, host: str, method: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Call a Gen2+ RPC endpoint and return JSON object.

    We use POST+JSON for best compatibility across Shelly firmwares
    (especially for Switch.Set and other write actions).
    """
    url = build_rpc_url(host, method)
    r = client.post(url, json_body=_normalize_rpc_params(params or {}))
    data = r.json()
    if not isinstance(data, dict):
        raise ValueError(f"Unexpected JSON from {method}")
    return data



def get_emdata_records(client: ShellyHttp, host: str, em_id: int, ts: int = 0) -> Dict[str, Any]:
    """Return EMData blocks metadata (helps determine the oldest available history)."""
    return rpc_call(client, host, "EMData.GetRecords", {"id": int(em_id), "ts": int(ts)})


def get_earliest_emdata_ts(records_payload: Dict[str, Any]) -> Optional[int]:
    """Extract the earliest plausible unix timestamp from EMData.GetRecords payload."""
    blocks = records_payload.get("data_blocks") or []
    earliest: Optional[int] = None
    for b in blocks:
        if not isinstance(b, dict):
            continue
        ts = b.get("ts")
        if isinstance(ts, (int, float)):
            ts_i = int(ts)
            # Filter out obviously non-epoch values (doc examples sometimes show small numbers)
            if ts_i >= 1_000_000_000:
                if earliest is None or ts_i < earliest:
                    earliest = ts_i
    return earliest

def download_csv(client: ShellyHttp, host: str, em_id: int, ts: int, end_ts: int) -> bytes:
    url = build_csv_url(host, em_id, ts, end_ts)
    r = client.get(url)
    return r.content


def get_em_status(client: ShellyHttp, host: str, em_id: int) -> Dict[str, Any]:
    return rpc_call(client, host, "EM.GetStatus", {"id": int(em_id)})


def get_shelly_status(client: ShellyHttp, host: str) -> Dict[str, Any]:
    """Return whole-device status (Gen2+).

    This endpoint is used as a *fallback* for cases where component-specific
    status calls (e.g. Switch.GetStatus) return an error payload or a shape we
    cannot parse reliably.
    """

    return rpc_call(client, host, "Shelly.GetStatus", {})


def get_switch_status(client: ShellyHttp, host: str, switch_id: int) -> Dict[str, Any]:
    """Return switch status with robust fallbacks.

    Primary (Gen2+/Plus/Pro): Switch.GetStatus
    Fallback 1 (Gen2+): Shelly.GetStatus -> component block (switch:<id>, relay:<id>, ...)
    Fallback 2 (best-effort Gen1): /status -> relays[<id>]
    """

    sid = int(switch_id)

    # --- Primary: Gen2 component status
    try:
        data = rpc_call(client, host, "Switch.GetStatus", {"id": sid})
    except Exception:
        data = {"error": {"message": "Switch.GetStatus failed"}}

    # Happy path: already contains a recognizable state.
    if isinstance(data, dict) and "error" not in data:
        for k in ("output", "ison", "on", "state"):
            if k in data:
                return data

    # --- Fallback: whole-device status (still Gen2)
    try:
        full = get_shelly_status(client, host)
        if isinstance(full, dict):
            # Common component keys in Shelly.GetStatus
            for comp_key in (f"switch:{sid}", f"relay:{sid}"):
                block = full.get(comp_key)
                if isinstance(block, dict):
                    merged = dict(block)
                    merged.setdefault("_source", "Shelly.GetStatus")
                    return merged

            # If the configured id is wrong (common with mixed device types),
            # but there is exactly ONE switch/relay component, use it.
            comp_blocks = []
            for k, v in full.items():
                if isinstance(v, dict) and (isinstance(k, str) and (k.startswith("switch:") or k.startswith("relay:"))):
                    comp_blocks.append((k, v))
            if len(comp_blocks) == 1:
                merged = dict(comp_blocks[0][1])
                merged.setdefault("_source", f"Shelly.GetStatus:{comp_blocks[0][0]}")
                return merged

            # Some firmwares may expose arrays
            for arr_key in ("switches", "relays"):
                arr = full.get(arr_key)
                if isinstance(arr, list):
                    if 0 <= sid < len(arr) and isinstance(arr[sid], dict):
                        merged = dict(arr[sid])
                        merged.setdefault("_source", "Shelly.GetStatus")
                        return merged
                    if len(arr) == 1 and isinstance(arr[0], dict):
                        merged = dict(arr[0])
                        merged.setdefault("_source", "Shelly.GetStatus")
                        return merged
    except Exception:
        pass

    # --- Fallback: Gen1 classic endpoint
    try:
        r = client.get(f"http://{host}/status")
        js = r.json()
        relays = js.get("relays")
        if isinstance(relays, list):
            if 0 <= sid < len(relays) and isinstance(relays[sid], dict):
                merged = dict(relays[sid])
                merged.setdefault("_source", "/status")
                return merged
            if len(relays) == 1 and isinstance(relays[0], dict):
                merged = dict(relays[0])
                merged.setdefault("_source", "/status")
                return merged
    except Exception:
        pass

    return data


def set_switch_state(client: ShellyHttp, host: str, switch_id: int, on: bool) -> Dict[str, Any]:
    """Set switch state.

    Prefer Gen2/Plus/Pro RPC (Switch.Set). If that fails, fall back to
    Gen1 relay endpoint (/relay/<id>?turn=on|off) which is used by devices
    like Shelly Plug S (SNSW-001P16EU).
    """
    try:
        return rpc_call(client, host, "Switch.Set", {"id": int(switch_id), "on": bool(on)})
    except Exception:
        # Gen1 fallback: returns JSON with ison etc.
        turn = "on" if on else "off"
        url = f"http://{host}/relay/{int(switch_id)}?turn={turn}"
        return client.get_json(url)
