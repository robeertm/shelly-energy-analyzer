from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from requests.auth import HTTPBasicAuth, HTTPDigestAuth


@dataclass(frozen=True)
class HttpConfig:
    timeout_seconds: float = 8.0
    retries: int = 3
    backoff_base_seconds: float = 1.5
    # Note: this string is intentionally static to keep the HTTP layer standalone.
    user_agent: str = "ShellyEnergyAnalyzer (+requests)"


def _host_of(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").lower()
    except Exception:
        return ""


class ShellyHttp:
    """Centralized HTTP client (retry/backoff, consistent timeouts, shared session).

    Per-host credentials may be registered via :meth:`set_credentials`. When
    a request hits a host with credentials, Digest auth is tried first
    (Gen2+ default); on a 401 the client probes the WWW-Authenticate header
    and falls back to Basic auth (Gen1) for that host. The chosen scheme is
    cached so subsequent calls go through with one round trip.
    """

    def __init__(self, cfg: Optional[HttpConfig] = None) -> None:
        self.cfg = cfg or HttpConfig()
        self._sess = requests.Session()
        # host (lowercased, no port) -> (username, password)
        self._creds: Dict[str, Tuple[str, str]] = {}
        # host -> 'digest' | 'basic' (cached after the first successful call)
        self._auth_scheme: Dict[str, str] = {}

    # ── credentials ───────────────────────────────────────────────────
    def set_credentials(self, host: str, username: str, password: str) -> None:
        """Register credentials for a host (no protocol, no port)."""
        if not host:
            return
        h = host.split(":")[0].strip().lower()
        if not h:
            return
        if username and password:
            self._creds[h] = (str(username), str(password))
        else:
            self._creds.pop(h, None)
            self._auth_scheme.pop(h, None)

    def clear_credentials(self) -> None:
        self._creds.clear()
        self._auth_scheme.clear()

    def has_credentials(self, host: str) -> bool:
        h = (host or "").split(":")[0].strip().lower()
        return h in self._creds

    def _auth_for_host(self, host: str) -> Optional[requests.auth.AuthBase]:
        if not host:
            return None
        creds = self._creds.get(host)
        if not creds:
            return None
        user, pw = creds
        scheme = self._auth_scheme.get(host, "digest")
        if scheme == "basic":
            return HTTPBasicAuth(user, pw)
        return HTTPDigestAuth(user, pw)

    def _switch_scheme_from_response(self, host: str, resp: requests.Response) -> bool:
        """Inspect WWW-Authenticate on a 401 and update the cached scheme."""
        if resp is None or resp.status_code != 401:
            return False
        wa = (resp.headers.get("WWW-Authenticate") or "").lower()
        new_scheme: Optional[str] = None
        if "digest" in wa:
            new_scheme = "digest"
        elif "basic" in wa:
            new_scheme = "basic"
        if new_scheme and self._auth_scheme.get(host) != new_scheme:
            self._auth_scheme[host] = new_scheme
            return True
        # No header — try the other scheme as a blind fallback.
        cur = self._auth_scheme.get(host, "digest")
        flipped = "basic" if cur == "digest" else "digest"
        self._auth_scheme[host] = flipped
        return True

    # ── request helpers ──────────────────────────────────────────────
    def _do_request(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> requests.Response:
        base_headers = {"User-Agent": self.cfg.user_agent}
        if method == "POST":
            base_headers["Content-Type"] = "application/json"
        if headers:
            base_headers.update(headers)

        host = _host_of(url)
        auth = self._auth_for_host(host)

        last_err: Optional[BaseException] = None
        max_attempts = max(self.cfg.retries, 1)
        for attempt in range(1, max_attempts + 1):
            try:
                if method == "GET":
                    r = self._sess.get(
                        url, params=params, headers=base_headers,
                        timeout=self.cfg.timeout_seconds, auth=auth,
                    )
                else:
                    r = self._sess.post(
                        url, json=json_body or {}, headers=base_headers,
                        timeout=self.cfg.timeout_seconds, auth=auth,
                    )
                # 401 → if we have credentials, try the other scheme once
                if r.status_code == 401 and host in self._creds and attempt == 1:
                    if self._switch_scheme_from_response(host, r):
                        auth = self._auth_for_host(host)
                        continue  # retry immediately with the new scheme
                r.raise_for_status()
                return r
            except Exception as e:
                last_err = e
                if attempt < max_attempts:
                    time.sleep(self.cfg.backoff_base_seconds * (2 ** (attempt - 1)))
        if last_err is None:
            raise RuntimeError(f"ShellyHttp.{method.lower()}: no attempts were made (retries < 1)")
        raise last_err

    def get(self, url: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> requests.Response:
        return self._do_request("GET", url, params=params, headers=headers)

    def post(
        self,
        url: str,
        json_body: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> requests.Response:
        return self._do_request("POST", url, json_body=json_body, headers=headers)


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


def schedule_list(client: ShellyHttp, host: str) -> Dict[str, Any]:
    """List all schedules on a Gen2+ device (Schedule.List)."""
    return rpc_call(client, host, "Schedule.List", {})


def schedule_create(
    client: ShellyHttp,
    host: str,
    timespec: str,
    calls: List[Dict[str, Any]],
    enable: bool = True,
) -> Dict[str, Any]:
    """Create a schedule on a Gen2+ device (Schedule.Create).

    timespec follows cron-like format: "ss mm hh * * dow"
    where dow is comma-separated 0-based day-of-week (0=Sun … 6=Sat).
    """
    return rpc_call(client, host, "Schedule.Create", {
        "enable": bool(enable),
        "timespec": str(timespec),
        "calls": list(calls),
    })


def schedule_delete(client: ShellyHttp, host: str, schedule_id: int) -> Dict[str, Any]:
    """Delete a schedule on a Gen2+ device (Schedule.Delete)."""
    return rpc_call(client, host, "Schedule.Delete", {"id": int(schedule_id)})


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
        return client.get(url).json()
