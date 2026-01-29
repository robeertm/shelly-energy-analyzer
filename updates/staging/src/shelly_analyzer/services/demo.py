from __future__ import annotations

import math
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from shelly_analyzer.io.config import DeviceConfig, DemoConfig
from shelly_analyzer.io.storage import Storage

# ---------- Demo devices ----------

def default_demo_devices() -> List[DeviceConfig]:
    # Realistic mix: one 3-phase meter and one 1-phase switch meter
    return [
        DeviceConfig(
            key="demo1",
            name="demo.device.house_3p",
            host="demo://house",
            em_id=0,
            kind="em",
            gen=2,
            model="SPEM-003CEBEU (Demo)",
            phases=3,
            supports_emdata=True,
        ),
        DeviceConfig(
            key="demo2",
            name="demo.device.garage_1p",
            host="demo://garage",
            em_id=0,
            kind="switch",
            gen=2,
            model="SNSW-001P16EU (Demo)",
            phases=1,
            supports_emdata=False,
        ),
    ]


# ---------- Live data generator ----------

@dataclass
class DemoState:
    seed: int
    scenario: str = "household"
    started_at: float = time.time()
    # Mutable per-device switch state (for demo switch devices)
    switches: Dict[str, bool] = None  # key -> on/off
    rw_p: Dict[str, float] = None  # key -> random-walk power component
    rw_v: Dict[str, float] = None  # key -> random-walk voltage component

    def __post_init__(self) -> None:
        if self.switches is None:
            self.switches = {}
        if self.rw_p is None:
            self.rw_p = {}
        if self.rw_v is None:
            self.rw_v = {}


def _daily_phase(t: float) -> float:
    """0..1 daily phase in local time."""
    lt = time.localtime(t)
    seconds = lt.tm_hour * 3600 + lt.tm_min * 60 + lt.tm_sec
    return seconds / 86400.0


def gen_sample(device: DeviceConfig, t: float, st: DemoState) -> Dict[str, Dict[str, float]]:
    """Return live field dicts: power_w/voltage_v/current_a/reactive_var/cosphi.

    The generator aims to feel "alive": it includes small stochastic fluctuations
    (deterministic per second via the seed) and a gentle random-walk component.
    """
    # Deterministic per-second RNG (stable across runs for the same seed)
    rnd = random.Random(st.seed + (hash(device.key) & 0xFFFF) + int(t))
    ph = _daily_phase(t)

    # Small random-walk per device to avoid perfectly smooth/linear curves
    p_rw = st.rw_p.get(device.key, 0.0)
    v_rw = st.rw_v.get(device.key, 0.0)
    p_rw = 0.985 * p_rw + rnd.gauss(0.0, 18.0)
    v_rw = 0.990 * v_rw + rnd.gauss(0.0, 0.08)
    st.rw_p[device.key] = p_rw
    st.rw_v[device.key] = v_rw

    # Base voltage around 230V with drift + gentle random-walk + jitter
    v_base = 230.0 + 2.5 * math.sin(2 * math.pi * ph + 0.7) + v_rw + rnd.gauss(0.0, 0.25)

    # Household active power profile (W): morning/evening peaks + random appliance bursts
    base = 180.0 + 40.0 * math.sin(2 * math.pi * ph)
    peak1 = 1200.0 * math.exp(-((ph - 0.32) / 0.06) ** 2)   # ~7:40
    peak2 = 1600.0 * math.exp(-((ph - 0.78) / 0.07) ** 2)   # ~18:45
    noise = 60.0 * math.sin(2 * math.pi * (ph * 6.0)) + 40.0 * math.sin(2 * math.pi * (ph * 17.0))
    # Add stochastic jitter + random-walk component (keeps it "alive")
    p_total = max(0.0, base + peak1 + peak2 + noise + p_rw + rnd.gauss(0.0, 45.0))

    # Add a few deterministic "events"
    # Kettle event around morning and evening
    kettle = 0.0
    if 0.29 < ph < 0.31 or 0.74 < ph < 0.76:
        kettle = 1800.0

    p_total += kettle

    # Occasional appliance bursts (deterministic per day): e.g. washing machine / vacuum
    burst = 0.0
    # 2 short bursts per day at pseudo-random phases
    burst_phase1 = (0.12 + (st.seed % 37) / 100.0) % 1.0
    burst_phase2 = (0.55 + (st.seed % 29) / 100.0) % 1.0
    if abs(ph - burst_phase1) < 0.008:
        burst = 900.0
    elif abs(ph - burst_phase2) < 0.010:
        burst = 650.0
    p_total += burst

    # Switch devices: follow on/off state
    if device.kind == "switch":
        on = st.switches.get(device.key, True)
        st.switches[device.key] = on
        p_total = 35.0 if on else 0.0  # idle vs off

    # 3-phase split
    if getattr(device, "phases", 1) >= 3 and device.kind == "em":
        # Split unevenly but stable
        pa = p_total * 0.42
        pb = p_total * 0.33
        pc = p_total * 0.25
        va, vb, vc = v_base + 0.8, v_base - 0.6, v_base + 0.2
    else:
        pa, pb, pc = p_total, 0.0, 0.0
        va, vb, vc = v_base, 0.0, 0.0

    # Power factor varies with load: 0.92..0.99
    pf = 0.92 + 0.07 * (1.0 - math.exp(-p_total / 800.0))
    pf = max(0.5, min(1.0, pf))

    # Apparent S per phase, then derive I
    def phase_vals(p: float, v: float) -> Tuple[float, float, float]:
        s = p / pf if pf else p
        i = s / v if v else 0.0
        q = math.copysign(math.sqrt(max(s * s - p * p, 0.0)), p)
        return s, i, q

    sa, ia, qa = phase_vals(pa, va if va else v_base)
    sb, ib, qb = phase_vals(pb, vb if vb else v_base)
    sc, ic, qc = phase_vals(pc, vc if vc else v_base)

    q_total = qa + qb + qc
    s_total = sa + sb + sc
    p_sum = pa + pb + pc
    pf_total = (p_sum / s_total) if s_total else 0.0

    return {
        "power_w": {"a": pa, "b": pb, "c": pc, "total": p_sum},
        "voltage_v": {"a": va, "b": vb, "c": vc},
        "current_a": {"a": ia, "b": ib, "c": ic},
        "reactive_var": {"a": qa, "b": qb, "c": qc, "total": q_total},
        "cosphi": {"a": pf, "b": pf, "c": pf, "total": pf_total},
    }


# ---------- Demo CSV generator (for Plots) ----------

def ensure_demo_csv(storage: Storage, devices: Iterable[DeviceConfig], demo: DemoConfig, days: int = 7) -> None:
    """Create demo CSV chunks if none exist. Keeps it lightweight."""
    now = int(time.time())
    start = now - days * 86400

    for d in devices:
        # If already has CSVs, don't overwrite
        existing = storage.list_csv_files(d.key)
        if existing:
            continue

        # Generate 15-minute samples
        rows: List[str] = []
        header = [
            "timestamp",
            "a_act_power", "b_act_power", "c_act_power",
            "a_voltage", "b_voltage", "c_voltage",
            "a_current", "b_current", "c_current",
        ]
        rows.append(",".join(header))

        st = DemoState(seed=int(getattr(demo, "seed", 1234)), scenario=str(getattr(demo, "scenario", "household")))
        for ts in range(start, now + 1, 900):
            fields = gen_sample(d, float(ts), st)
            pw = fields["power_w"]
            vv = fields["voltage_v"]
            aa = fields["current_a"]
            line = [
                str(ts),
                f"{pw['a']:.3f}", f"{pw['b']:.3f}", f"{pw['c']:.3f}",
                f"{vv['a']:.3f}", f"{vv['b']:.3f}", f"{vv['c']:.3f}",
                f"{aa['a']:.6f}", f"{aa['b']:.6f}", f"{aa['c']:.6f}",
            ]
            rows.append(",".join(line))

        content = ("\n".join(rows) + "\n").encode("utf-8")
        storage.save_chunk(d.key, start, now, content)
