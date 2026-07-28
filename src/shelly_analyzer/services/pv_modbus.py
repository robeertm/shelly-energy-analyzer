"""Modbus TCP reader for the external PV / battery / grid source.

Reads an inverter / hybrid / dongle / energy-manager **directly** over Modbus
TCP, so a PV system can feed the analyzer with **no Home Assistant** in the
loop. Two register maps are supported:

    "sunspec" – the SunSpec Alliance standard (SolarEdge, SMA, Fronius Gen24,
                Kostal, Sungrow, GoodWe, SolaX, …). The model blocks are
                auto-discovered by scanning for the "SunS" marker.
    "huawei"  – Huawei SUN2000 + LUNA2000 proprietary registers.

Everything is decoded into the analyzer's internal convention by the caller:
PV ≥ 0, battery + = charge, grid + = import (sign flips applied in pv_source).

The reader NEVER raises — on any error the affected value is ``None`` and
``last_error`` carries the reason, so the poll loop degrades gracefully.

Returns a 7-tuple ``(pv_w, batt_w, soc, grid_w, house_w, pv_today_kwh,
counters)`` where ``counters`` is a dict of cumulative kWh values (or ``None``)
for exact energy accounting, mirroring the HA path.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# SunSpec constants
_SUNS_MARKER = (0x5375, 0x6E53)          # "SunS"
_SUNSPEC_BASES = (40000, 50000, 0)       # 0-based addresses to probe
_INVERTER_MODELS = (101, 102, 103, 111, 112, 113)
_METER_MODELS = (201, 202, 203, 204, 211, 212, 213, 214)
_STORAGE_MODEL = 124

ReadResult = Tuple[Optional[float], Optional[float], Optional[float],
                   Optional[float], Optional[float], Optional[float],
                   Dict[str, Optional[float]]]


# ── word decoders (big-endian, high word first = Modbus/SunSpec standard) ──
def _u16(regs: List[int], i: int) -> int:
    return regs[i] & 0xFFFF


def _s16(regs: List[int], i: int) -> int:
    v = regs[i] & 0xFFFF
    return v - 0x10000 if v >= 0x8000 else v


def _u32(regs: List[int], i: int) -> int:
    return ((regs[i] & 0xFFFF) << 16) | (regs[i + 1] & 0xFFFF)


def _s32(regs: List[int], i: int) -> int:
    v = _u32(regs, i)
    return v - 0x100000000 if v >= 0x80000000 else v


def _scaled(value: float, sf: int) -> float:
    """Apply a SunSpec scale factor (int8, may be negative)."""
    try:
        if sf >= 0x8000:
            sf -= 0x10000
        return float(value) * (10.0 ** int(sf))
    except Exception:
        return float(value)


def _import_client():
    """Return the ModbusTcpClient class across pymodbus 2.x / 3.x, or None."""
    try:
        from pymodbus.client import ModbusTcpClient  # pymodbus 3.x
        return ModbusTcpClient
    except Exception:
        pass
    try:
        from pymodbus.client.sync import ModbusTcpClient  # pymodbus 2.x
        return ModbusTcpClient
    except Exception:
        return None


class _Session:
    """Thin wrapper over a pymodbus client that hides 2.x/3.x call differences."""

    def __init__(self, host: str, port: int, unit: int, timeout: float = 6.0):
        cls = _import_client()
        if cls is None:
            raise RuntimeError("pymodbus not installed — run pip install pymodbus")
        self.unit = int(unit)
        try:
            self.client = cls(host=host, port=int(port), timeout=timeout)
        except TypeError:
            self.client = cls(host, port=int(port))
        self.client.connect()

    def read(self, address: int, count: int) -> Optional[List[int]]:
        """Read *count* holding registers at *address* (0-based). None on error."""
        c = self.client
        unit = self.unit
        last = None
        for kwargs in ({"count": count, "slave": unit},   # pymodbus 3.x
                       {"count": count, "unit": unit},     # pymodbus 2.x
                       {"count": count, "device_id": unit}):  # pymodbus 3.7+
            try:
                rr = c.read_holding_registers(address, **kwargs)
            except TypeError as e:
                last = e
                continue
            except Exception as e:
                last = e
                continue
            if rr is None:
                continue
            if hasattr(rr, "isError") and rr.isError():
                last = rr
                continue
            regs = getattr(rr, "registers", None)
            if regs:
                return list(regs)
        if last is not None:
            logger.debug("modbus read %s/%s failed: %s", address, count, last)
        return None

    def close(self) -> None:
        try:
            self.client.close()
        except Exception:
            pass


# ── SunSpec ────────────────────────────────────────────────────────────────
def _sunspec_models(sess: "_Session") -> Tuple[int, Dict[int, Tuple[int, int]]]:
    """Discover the SunSpec base and model blocks.

    Returns ``(base, {model_id: (data_start_addr, length)})`` where
    ``data_start_addr`` points at the first DATA register (after ID+L).
    """
    base = -1
    for cand in _SUNSPEC_BASES:
        regs = sess.read(cand, 2)
        if regs and (regs[0] & 0xFFFF, regs[1] & 0xFFFF) == _SUNS_MARKER:
            base = cand
            break
    if base < 0:
        return -1, {}

    models: Dict[int, Tuple[int, int]] = {}
    ptr = base + 2
    for _ in range(64):  # generous safety bound
        hdr = sess.read(ptr, 2)
        if not hdr:
            break
        model_id = hdr[0] & 0xFFFF
        length = hdr[1] & 0xFFFF
        if model_id in (0xFFFF, 0):
            break
        # keep the FIRST occurrence of each model id
        models.setdefault(model_id, (ptr + 2, length))
        ptr += 2 + length
        if length == 0 or length > 200:
            break
    return base, models


def _sunspec_read(sess: "_Session") -> ReadResult:
    counters: Dict[str, Optional[float]] = {
        "pv_total": None, "grid_import": None, "grid_export": None,
        "batt_charge": None, "batt_discharge": None,
    }
    _, models = _sunspec_models(sess)
    if not models:
        return (None, None, None, None, None, None, counters)

    pv_w: Optional[float] = None
    pv_kwh: Optional[float] = None
    grid_w: Optional[float] = None
    soc: Optional[float] = None

    # Inverter model: AC power (W @ off14, W_SF @ off15), lifetime Wh (acc32 @
    # off24, WH_SF @ off26). Offsets are register indices from the DATA start.
    inv = next((models[m] for m in _INVERTER_MODELS if m in models), None)
    if inv:
        start, length = inv
        d = sess.read(start, min(length, 60)) if length else None
        if d and len(d) > 15:
            pv_w = _scaled(_s16(d, 14), _s16(d, 15))
        if d and len(d) > 26:
            wh = _u32(d, 24)
            pv_kwh = _scaled(wh, _s16(d, 26)) / 1000.0  # Wh → kWh
    if pv_kwh is not None and pv_kwh >= 0:
        counters["pv_total"] = pv_kwh

    # Meter model (grid): total real power W @ off17, W_SF @ off21.
    mtr = next((models[m] for m in _METER_MODELS if m in models), None)
    if mtr:
        start, length = mtr
        d = sess.read(start, min(length, 60)) if length else None
        if d and len(d) > 21:
            grid_w = _scaled(_s16(d, 17), _s16(d, 21))

    # Storage model 124: SOC (ChaState @ off8, %). Scale factor location varies;
    # normalise a 0-10000 reading down to 0-100.
    if _STORAGE_MODEL in models:
        start, length = models[_STORAGE_MODEL]
        d = sess.read(start, min(length, 30)) if length else None
        if d and len(d) > 8:
            raw = _u16(d, 8)
            if raw != 0xFFFF:
                soc = raw / 100.0 if raw > 100 else float(raw)

    return (pv_w, None, soc, grid_w, None, None, counters)


# ── Huawei SUN2000 / LUNA2000 ────────────────────────────────────────────
def _huawei_read(sess: "_Session") -> ReadResult:
    counters: Dict[str, Optional[float]] = {
        "pv_total": None, "grid_import": None, "grid_export": None,
        "batt_charge": None, "batt_discharge": None,
    }
    pv_w = batt_w = soc = grid_w = None
    pv_kwh = None

    # PV input power (int32 W) @ 32064; lifetime yield (uint32, gain 100 → kWh)
    # @ 32106. Read the 32064..32107 window in a couple of small reads.
    r = sess.read(32064, 2)
    if r:
        pv_w = float(_s32(r, 0))
    r = sess.read(32106, 2)
    if r:
        pv_kwh = _u32(r, 0) / 100.0
        if pv_kwh >= 0:
            counters["pv_total"] = pv_kwh

    # Power meter active power (int32 W) @ 37113.
    r = sess.read(37113, 2)
    if r:
        grid_w = float(_s32(r, 0))

    # LUNA2000: charge/discharge power (int32 W, + = charge) @ 37001;
    # state of capacity (uint16, gain 10 → %) @ 37004.
    r = sess.read(37001, 2)
    if r:
        batt_w = float(_s32(r, 0))
    r = sess.read(37004, 1)
    if r:
        soc = _u16(r, 0) / 10.0

    return (pv_w, batt_w, soc, grid_w, None, None, counters)


def read_modbus(cfg) -> ReadResult:
    """Read the configured Modbus source once. Never raises."""
    empty: Dict[str, Optional[float]] = {
        "pv_total": None, "grid_import": None, "grid_export": None,
        "batt_charge": None, "batt_discharge": None,
    }
    host = str(getattr(cfg, "modbus_host", "") or "").strip()
    if not host:
        return (None, None, None, None, None, None, empty)
    port = int(getattr(cfg, "modbus_port", 502) or 502)
    unit = int(getattr(cfg, "modbus_unit_id", 1) or 1)
    reg_map = str(getattr(cfg, "modbus_register_map", "sunspec") or "sunspec").lower()

    sess = None
    try:
        sess = _Session(host, port, unit)
        if reg_map == "huawei":
            return _huawei_read(sess)
        return _sunspec_read(sess)
    except Exception as e:
        logger.info("modbus read failed: %s", e)
        return (None, None, None, None, None, None, {**empty, "_error": str(e)})  # type: ignore
    finally:
        if sess is not None:
            sess.close()
