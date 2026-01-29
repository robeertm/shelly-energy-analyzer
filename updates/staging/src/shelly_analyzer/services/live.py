from __future__ import annotations

import queue
import threading
import time
import logging
import concurrent.futures
import math
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from shelly_analyzer.io.http import ShellyHttp, HttpConfig, get_em_status, get_switch_status
from shelly_analyzer.io.config import DeviceConfig, DownloadConfig


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)


def parse_live_fields(data: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
    """Normalize Shelly EM/3EM EM.GetStatus JSON to per-phase fields.

    Returns dict with keys: power_w, voltage_v, current_a
    Each contains phase keys a/b/c and a total for power.

    It prefers instantaneous keys and falls back to *_avg_* where available.
    """

    pa = _safe_float(data.get("a_act_power", data.get("act_power_a", 0.0)))
    pb = _safe_float(data.get("b_act_power", data.get("act_power_b", 0.0)))
    pc = _safe_float(data.get("c_act_power", data.get("act_power_c", 0.0)))

    def pick_voltage(phase: str) -> float:
        return _safe_float(data.get(f"{phase}_voltage", data.get(f"{phase}_avg_voltage", 0.0)))

    def pick_current(phase: str) -> float:
        return _safe_float(data.get(f"{phase}_current", data.get(f"{phase}_avg_current", 0.0)))

    va, vb, vc = pick_voltage("a"), pick_voltage("b"), pick_voltage("c")
    ia, ib, ic = pick_current("a"), pick_current("b"), pick_current("c")

    # Apparent power (VA): prefer device-provided apparent power fields if present, else derive via V*I.
    def pick_apparent(phase: str, v: float, i: float) -> float:
        return _safe_float(
            data.get(f"{phase}_aprt_power",
                     data.get(f"{phase}_apparent_power",
                              data.get(f"{phase}_apparent", v * i)))
        )

    sa, sb, sc = pick_apparent("a", va, ia), pick_apparent("b", vb, ib), pick_apparent("c", vc, ic)
    s_total = sa + sb + sc

    # Reactive power (VAR): prefer device-provided reactive power; else derive from P and S with a conservative sign.
    def pick_reactive(phase: str, p: float, s: float) -> float:
        q = _safe_float(
            data.get(f"{phase}_react_power",
                     data.get(f"{phase}_reactive_power",
                              data.get(f"{phase}_reactive", float("nan"))))
        )
        if math.isfinite(q) and abs(q) > 0.0:
            return q
        # Derive magnitude; sign is ambiguous without dedicated field, so use sign of active power.
        mag = math.sqrt(max((s * s) - (p * p), 0.0))
        return (mag if p >= 0 else -mag)

    qa, qb, qc = pick_reactive("a", pa, sa), pick_reactive("b", pb, sb), pick_reactive("c", pc, sc)
    q_total = qa + qb + qc

    # Power factor / cos φ: prefer device-provided pf; else derive as P/S.
    def pick_pf(phase: str, p: float, s: float) -> float:
        pf = _safe_float(
            data.get(f"{phase}_pf",
                     data.get(f"{phase}_power_factor",
                              data.get(f"{phase}_cosphi", float("nan"))))
        )
        if math.isfinite(pf) and abs(pf) > 0.0:
            return pf
        return (p / s) if s else 0.0

    pfa, pfb, pfc = pick_pf("a", pa, sa), pick_pf("b", pb, sb), pick_pf("c", pc, sc)
    p_total = pa + pb + pc
    pf_total = (p_total / s_total) if s_total else 0.0

    return {
        "power_w": {"a": pa, "b": pb, "c": pc, "total": p_total},
        "voltage_v": {"a": va, "b": vb, "c": vc},
        "current_a": {"a": ia, "b": ib, "c": ic},
        "reactive_var": {"a": qa, "b": qb, "c": qc, "total": q_total},
        "cosphi": {"a": pfa, "b": pfb, "c": pfc, "total": pf_total},
    }



def parse_switch_fields(data: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
    """Normalize Switch.GetStatus JSON to the same per-phase structure.

    For 1P devices like Shelly Plus 1PM we map values to phase 'a' and keep b/c as 0.
    """
    p = _safe_float(data.get("apower", data.get("power", 0.0)))
    v = _safe_float(data.get("voltage", 0.0))
    c = _safe_float(data.get("current", 0.0))
    return {
        "power_w": {"a": p, "b": 0.0, "c": 0.0, "total": p},
        "voltage_v": {"a": v, "b": 0.0, "c": 0.0},
        "current_a": {"a": c, "b": 0.0, "c": 0.0},
    }


@dataclass(frozen=True)
class LiveSample:
    device_key: str
    device_name: str
    ts: int
    power_w: Dict[str, float]  # a/b/c/total
    voltage_v: Dict[str, float]  # a/b/c
    current_a: Dict[str, float]  # a/b/c
    reactive_var: Dict[str, float]  # a/b/c/total (VAR)
    cosphi: Dict[str, float]  # a/b/c/total (cos φ)
    raw: Dict[str, Any]


class LivePoller:
    """Background poller that never touches Tkinter directly.

    It publishes LiveSample objects to `self.samples`.
    """

    def __init__(self, device: DeviceConfig, download_cfg: DownloadConfig, poll_seconds: float = 1.0) -> None:
        self.device = device
        self.poll_seconds = float(poll_seconds)
        self._stop = threading.Event()
        self.samples: "queue.Queue[LiveSample]" = queue.Queue()
        self.errors: "queue.Queue[str]" = queue.Queue()

        self._http = ShellyHttp(
            HttpConfig(
                timeout_seconds=download_cfg.timeout_seconds,
                retries=download_cfg.retries,
                backoff_base_seconds=download_cfg.backoff_base_seconds,
            )
        )
        self._thread = threading.Thread(target=self._run, name=f"LivePoller-{device.key}", daemon=True)

    def start(self) -> None:
        if not self._thread.is_alive():
            self._thread.start()


    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        log = logging.getLogger(__name__)
        err_count = 0
        while not self._stop.is_set():
            ts = int(time.time())
            try:
                if getattr(self.device, "kind", "em") == "switch":
                    data = get_switch_status(self._http, self.device.host, self.device.em_id)
                    fields = parse_switch_fields(data)
                else:
                    data = get_em_status(self._http, self.device.host, self.device.em_id)
                    fields = parse_live_fields(data)
                self.samples.put(
                    LiveSample(
                        device_key=self.device.key,
                        device_name=self.device.name,
                        ts=ts,
                        power_w=fields["power_w"],
                        voltage_v=fields["voltage_v"],
                        current_a=fields["current_a"],
                        reactive_var=fields.get("reactive_var", {"a":0.0,"b":0.0,"c":0.0,"total":0.0}),
                        cosphi=fields.get("cosphi", {"a":0.0,"b":0.0,"c":0.0,"total":0.0}),
                        raw=data,
                    )
                )
                err_count = 0
            except Exception as e:
                self.errors.put(f"{self.device.name}: {e}")
                err_count += 1
                try:
                    log.warning("Live poll failed for %s (%s): %s", self.device.name, self.device.host, e)
                except Exception:
                    pass

            # Exponential backoff on repeated errors (max 30s), but keep stop responsive.
            sleep_s = float(self.poll_seconds)
            if err_count >= 2:
                sleep_s = min(30.0, float(self.poll_seconds) * (2.0 ** min(err_count - 1, 5)))

            if self._stop.wait(sleep_s):
                break


class MultiLivePoller:
    """Poll multiple Shelly devices with a shared ThreadPool.

    This keeps the UI responsive even with many devices while avoiding one
    thread per device.

    It publishes LiveSample objects to `self.samples` and error dicts to
    `self.errors`.
    """

    def __init__(
        self,
        devices: Iterable[DeviceConfig],
        download_cfg: DownloadConfig,
        poll_seconds: float = 1.0,
        max_workers: Optional[int] = None,
    ) -> None:
        self.devices: List[DeviceConfig] = list(devices)
        self.poll_seconds = float(poll_seconds)
        self._stop = threading.Event()
        self.samples: "queue.Queue[LiveSample]" = queue.Queue()
        self.errors: "queue.Queue[Dict[str, Any]]" = queue.Queue()

        self._http_cfg = HttpConfig(
            timeout_seconds=download_cfg.timeout_seconds,
            retries=download_cfg.retries,
            backoff_base_seconds=download_cfg.backoff_base_seconds,
        )

        self._thread_local = threading.local()

        # Per-device backoff state
        self._err_count: Dict[str, int] = {d.key: 0 for d in self.devices}
        self._next_due_ts: Dict[str, float] = {d.key: 0.0 for d in self.devices}

        if max_workers is None:
            # Good default for network IO without overwhelming Shellys
            max_workers = min(8, max(1, len(self.devices)))
        self._max_workers = int(max_workers)

        self._thread = threading.Thread(target=self._run, name="MultiLivePoller", daemon=True)

    def start(self) -> None:
        if not self._thread.is_alive():
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _http(self) -> ShellyHttp:
        http = getattr(self._thread_local, "http", None)
        if http is None:
            http = ShellyHttp(self._http_cfg)
            self._thread_local.http = http
        return http

    def _fetch_one(self, d: DeviceConfig, ts: int) -> LiveSample:
        http = self._http()
        if str(getattr(d, "kind", "em")) == "switch":
            data = get_switch_status(http, d.host, d.em_id)
            fields = parse_switch_fields(data)
        else:
            data = get_em_status(http, d.host, d.em_id)
            fields = parse_live_fields(data)
        return LiveSample(
            device_key=d.key,
            device_name=d.name,
            ts=ts,
            power_w=fields["power_w"],
            voltage_v=fields["voltage_v"],
            current_a=fields["current_a"],
            reactive_var=fields.get("reactive_var", {"a": 0.0, "b": 0.0, "c": 0.0, "total": 0.0}),
            cosphi=fields.get("cosphi", {"a": 0.0, "b": 0.0, "c": 0.0, "total": 0.0}),
            raw=data,
        )

    def _backoff_seconds(self, base: float, err_count: int) -> float:
        # Keep aligned with LivePoller backoff (cap 30s).
        if err_count < 2:
            return base
        return min(30.0, float(base) * (2.0 ** min(err_count - 1, 5)))

    def _run(self) -> None:
        log = logging.getLogger(__name__)
        poll = max(0.2, float(self.poll_seconds))

        while not self._stop.is_set():
            started = time.time()
            ts = int(started)

            # Select devices that are due (respect per-device backoff)
            due: List[DeviceConfig] = []
            now = started
            for d in self.devices:
                if now >= float(self._next_due_ts.get(d.key, 0.0)):
                    due.append(d)

            if due:
                with concurrent.futures.ThreadPoolExecutor(max_workers=self._max_workers) as ex:
                    futs = {ex.submit(self._fetch_one, d, ts): d for d in due}
                    try:
                        iterator = concurrent.futures.as_completed(futs, timeout=max(0.1, poll))
                        for fut in iterator:
                            d = futs[fut]
                            try:
                                s = fut.result()
                                self.samples.put(s)
                                self._err_count[d.key] = 0
                                self._next_due_ts[d.key] = time.time() + poll
                            except Exception as e:
                                ec = int(self._err_count.get(d.key, 0)) + 1
                                self._err_count[d.key] = ec
                                backoff = self._backoff_seconds(poll, ec)
                                self._next_due_ts[d.key] = time.time() + backoff
                                self.errors.put(
                                    {
                                        "device_key": d.key,
                                        "device_name": d.name,
                                        "error": str(e),
                                    }
                                )
                                try:
                                    log.warning("Live poll failed for %s (%s): %s", d.name, d.host, e)
                                except Exception:
                                    pass
                    except concurrent.futures.TimeoutError:
                        # Some Shellys didn't respond within the poll window.
                        # They will be retried on the next tick; we keep the loop alive.
                        pass

            # Wait until next tick; keep stop responsive.
            elapsed = time.time() - started
            sleep_s = max(0.05, poll - elapsed)
            if self._stop.wait(sleep_s):
                break


# ---------------- Demo poller (no network) ----------------

from shelly_analyzer.services.demo import DemoState, gen_sample
from shelly_analyzer.io.config import DemoConfig


class DemoMultiLivePoller:
    """Generate realistic live samples without Shelly devices (Demo Mode)."""

    def __init__(
        self,
        devices: Iterable[DeviceConfig],
        demo_cfg: DemoConfig,
        poll_seconds: float = 1.0,
    ) -> None:
        self.devices: List[DeviceConfig] = list(devices)
        self.poll_seconds = float(poll_seconds)
        self.demo_cfg = demo_cfg
        self.samples: "queue.Queue[LiveSample]" = queue.Queue()
        self.errors: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._state = DemoState(seed=int(getattr(demo_cfg, "seed", 1234)), scenario=str(getattr(demo_cfg, "scenario", "household")))

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return

        self._stop.clear()

        def _run() -> None:
            next_t = time.time()
            while not self._stop.is_set():
                now = time.time()
                if now < next_t:
                    time.sleep(min(0.05, max(0.0, next_t - now)))
                    continue
                ts = int(time.time())
                try:
                    for d in self.devices:
                        fields = gen_sample(d, float(ts), self._state)
                        self.samples.put(
                            LiveSample(
                                device_key=d.key,
                                device_name=d.name,
                                ts=ts,
                                power_w=fields.get("power_w", {}),
                                voltage_v=fields.get("voltage_v", {}),
                                current_a=fields.get("current_a", {}),
                                reactive_var=fields.get("reactive_var", {"a": 0.0, "b": 0.0, "c": 0.0, "total": 0.0}),
                                cosphi=fields.get("cosphi", {"a": 0.0, "b": 0.0, "c": 0.0, "total": 0.0}),
                                raw={
                                    "demo": True,
                                    "kind": str(getattr(d, "kind", "em")),
                                    "output": bool(self._state.switches.get(d.key, True)),
                                    "ts": ts,
                                },
                            )
                        )
                except Exception as e:
                    self.errors.put({"error": str(e)})
                next_t += self.poll_seconds
                # avoid drift runaway
                if next_t < time.time() - 5:
                    next_t = time.time() + self.poll_seconds

        self._thread = threading.Thread(target=_run, name="DemoMultiLivePoller", daemon=True)
        self._thread.start()

    def set_switch(self, device_key: str, on: bool) -> None:
        """Set demo switch state (used by UI toggle)."""
        try:
            self._state.switches[str(device_key)] = bool(on)
        except Exception:
            pass


    def stop(self) -> None:
        self._stop.set()

    def join(self, timeout: Optional[float] = None) -> None:
        if self._thread:
            self._thread.join(timeout=timeout)