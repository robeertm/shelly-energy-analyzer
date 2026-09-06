from __future__ import annotations

import math
import random
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from shelly_analyzer.io.config import DeviceConfig, DemoConfig
from shelly_analyzer.io.storage import Storage

# ---------- Demo devices ----------

def default_demo_devices() -> List[DeviceConfig]:
    """The simulated house: a 3-phase house meter, a 1-phase switch, and —
    since v16.80 — a wallbox and a signed grid meter.

    The last two exist so demo mode can show the pages that need a *supply*
    side: the solar dashboard, the battery tab and above all the EV log's
    source split, which needs to know what fed the car. Without them those
    pages were the only ones nobody could try without hardware.
    """
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
        DeviceConfig(
            key="demo3",
            name="demo.device.wallbox",
            host="demo://wallbox",
            em_id=0,
            kind="em",
            gen=2,
            model="SPEM-003CEBEU (Demo)",
            phases=3,
            supports_emdata=True,
        ),
        DeviceConfig(
            key="demo4",
            name="demo.device.grid",
            host="demo://grid",
            em_id=0,
            kind="em",
            gen=2,
            model="SPEM-003CEBEU (Demo)",
            phases=3,
            supports_emdata=True,
        ),
    ]


# Reserved keys the demo writes its supply side to — the same synthetic keys an
# external PV source uses, so the demo exercises the real code path.
# The simulated installation. Deliberately modest: see demo_solar().
PV_KWP = 4.2
BATTERY_KWH = 5.0
BATTERY_MAX_W = 2600.0

DEMO_PV_KEY = "pv"
DEMO_BATTERY_KEY = "battery"
DEMO_WALLBOX_KEY = "demo3"
DEMO_GRID_KEY = "demo4"


# ---------- Live data generator ----------

@dataclass
class DemoState:
    seed: int
    scenario: str = "household"
    started_at: float = field(default_factory=time.time)
    # Mutable per-device switch state (for demo switch devices)
    switches: Dict[str, bool] = None  # key -> on/off
    rw_p: Dict[str, float] = None  # key -> random-walk power component
    rw_v: Dict[str, float] = None  # key -> random-walk voltage component
    soc_kwh: float = 2.2           # simulated battery charge, carried over time
    step_s: float = 900.0          # seconds between two generated samples

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


def _day_index(t: float) -> int:
    return int(t // 86400)


def _plain_house_w(t: float) -> float:
    """The household draw without the per-device random walk.

    The solar model needs *one* house figure that every caller computes
    identically. gen_sample's own value carries a random walk kept per device,
    so asking it would give the wallbox a different house than the battery saw —
    which is exactly how the battery ended up contributing 0 kWh to a charge it
    had visibly helped with.
    """
    ph = _daily_phase(t)
    base = 180.0 + 40.0 * math.sin(2 * math.pi * ph)
    peak1 = 1200.0 * math.exp(-((ph - 0.32) / 0.06) ** 2)
    peak2 = 1600.0 * math.exp(-((ph - 0.78) / 0.07) ** 2)
    return max(45.0, base + peak1 + peak2)


def _day_plan(seed: int, day: int):
    """Cloud cover and the day's charging plan — deterministic per day."""
    dr = random.Random(seed * 7919 + day)
    cloud = dr.choice([1.0, 0.95, 0.9, 0.8, 0.6, 0.4, 0.25, 1.0, 0.85])
    r = dr.random()
    plan = []
    if cloud >= 0.8 and r < 0.40:
        plan.append(("surplus", 11.0 + dr.random() * 1.5, 3.0 + dr.random() * 2.0))
    elif r < 0.72:
        plan.append(("evening", 15.0 + dr.random() * 1.5, 5.0))
    elif r < 0.86:
        plan.append(("cloudy", 12.5 + dr.random(), 2.5 + dr.random()))
    if dr.random() > 0.80:
        plan.append(("night", 1.5 + dr.random() * 2.0, 2.0 + dr.random() * 1.5))
    return cloud, plan


def _pv_w(h: float, cloud: float) -> float:
    if not (5.0 < h < 21.0):
        return 0.0
    return max(0.0, PV_KWP * 1000.0 * (math.sin((h - 5.0) / 16.0 * math.pi) ** 1.7) * cloud)


def _wallbox_w(h: float, plan, pv: float, house: float) -> float:
    for kind, start, dur in plan:
        if not (start <= h < start + dur):
            continue
        if kind == "night":
            return 7200.0
        if kind == "surplus":
            w = min(7200.0, max(0.0, (pv - house) * 0.92))
            return w if w >= 1400.0 else 0.0     # below the car's minimum it pauses
        return min(7200.0, max(3600.0, (pv - house) * 0.92))
    return 0.0


_SOC_CACHE: Dict[Tuple[int, int], List[float]] = {}
_SOC_STEP = 300          # the day is integrated in 5-minute steps


def _soc_curve(seed: int, day: int, day_start: float) -> List[float]:
    """State of charge across one day, integrated once and remembered.

    A pure function of (seed, day): every series — PV, battery, wallbox, grid —
    reads the same trajectory, so they cannot drift apart the way a per-device
    running total did.
    """
    key = (seed, day)
    hit = _SOC_CACHE.get(key)
    if hit is not None:
        return hit
    cloud, plan = _day_plan(seed, day)
    # Start where the previous evening would plausibly have left it: a small
    # reserve. The first modelled day is the only one this approximates.
    soc = BATTERY_KWH * 0.25
    out = []
    n = 86400 // _SOC_STEP
    for i in range(n + 1):
        h = (i * _SOC_STEP) / 3600.0
        pv = _pv_w(h, cloud)
        house = _plain_house_w(day_start + i * _SOC_STEP)
        load = house + _wallbox_w(h, plan, pv, house)
        net = pv - load
        if net > 150.0 and soc < BATTERY_KWH:
            b = min(net * 0.85, BATTERY_MAX_W)
        elif net < -80.0 and soc > 0.25:
            b = -min(-net, BATTERY_MAX_W)
        else:
            b = 0.0
        out.append(soc)
        soc = max(0.0, min(BATTERY_KWH, soc + b * _SOC_STEP / 3600.0 / 1000.0))
    if len(_SOC_CACHE) > 400:
        _SOC_CACHE.clear()
    _SOC_CACHE[key] = out
    return out


def demo_solar(t: float, st: DemoState = None, house_w: float = None) -> Dict[str, float]:
    """The supply side of the simulated house at time ``t``.

    Returns ``{"pv", "battery", "wallbox", "grid"}`` in watts, with the app's
    sign conventions: grid ``+`` import / ``−`` export, battery ``+`` charging /
    ``−`` discharging, PV ``≥ 0``.

    A **pure function of the time and the seed** — no running state. Every
    caller therefore gets identical numbers for the same instant, which is what
    lets the four series reconcile: ``grid = load − pv + battery``.

    ``st``/``house_w`` are accepted for call-site compatibility and ignored:
    the house figure has to be the one every series agrees on, not the one the
    calling device happened to have walked to.

    The simulated year deliberately contains all three stories a source split
    has to tell apart — sunny midday surplus charges, afternoon charges that run
    into the evening (sun → battery → grid), and night charges that are pure
    grid. A demo that could only show one of them would prove nothing.
    """
    seed = int(getattr(st, "seed", 1234)) if st is not None else 1234
    day = int(t // 86400)
    day_start = day * 86400.0
    # Local midnight, not UTC midnight: the profile is expressed in local hours.
    lt = time.localtime(t)
    h = lt.tm_hour + lt.tm_min / 60.0 + lt.tm_sec / 3600.0
    local_midnight = t - h * 3600.0

    cloud, plan = _day_plan(seed, day)
    pv = _pv_w(h, cloud)
    house = _plain_house_w(t)
    wallbox = _wallbox_w(h, plan, pv, house)
    load = house + wallbox

    soc_curve = _soc_curve(seed, day, local_midnight)
    idx = max(0, min(len(soc_curve) - 1, int(h * 3600.0 / _SOC_STEP)))
    soc = soc_curve[idx]

    net = pv - load
    if net > 150.0 and soc < BATTERY_KWH:
        battery = min(net * 0.85, BATTERY_MAX_W)
    elif net < -80.0 and soc > 0.25:
        battery = -min(-net, BATTERY_MAX_W)
    else:
        battery = 0.0

    return {"pv": pv, "battery": battery, "wallbox": wallbox,
            "grid": load - pv + battery}


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
    # 🔴 Bounded.  An AR(1) walk with a=0.985 and sigma=18 settles at a standard
    # deviation of 18/sqrt(1-0.985**2) = 104 W — larger than the night-time base
    # load, so `max(0.0, ...)` below pinned the house at exactly 0 W for hours
    # every evening.  A real house never draws nothing.
    p_rw = max(-90.0, min(90.0, 0.985 * p_rw + rnd.gauss(0.0, 18.0)))
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
    # Standby floor: fridge, router, standby losses.  Clamping to zero would
    # show a house that has been disconnected from the grid.
    STANDBY_W = 45.0
    p_total = max(STANDBY_W, base + peak1 + peak2 + noise + p_rw + rnd.gauss(0.0, 45.0))

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

    # The wallbox and the grid meter are not independent loads: they come from
    # the shared solar model, so PV, battery, wallbox and grid reconcile
    # (grid = load − pv + battery) no matter which device asked.
    if device.key in (DEMO_WALLBOX_KEY, DEMO_GRID_KEY):
        sol = demo_solar(t, st)
        p_total = sol["wallbox"] if device.key == DEMO_WALLBOX_KEY else sol["grid"]

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

    # Power factor varies with load: 0.92..0.99.
    # 🔴 abs(): the grid meter goes negative while exporting, and a negative
    # argument turns exp() into a huge number, which pushed the factor far
    # outside its range and then got clamped to 0.5 for the whole sunny half of
    # the day. The factor depends on how hard the meter works, not which way.
    pf = 0.92 + 0.07 * (1.0 - math.exp(-abs(p_total) / 800.0))
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
    """Create demo data if none exist. v6: writes directly to DB."""
    now = int(time.time())
    start = now - days * 86400

    for d in devices:
        # If already has data (DB or CSV), don't overwrite
        if storage.has_usable_data(d.key):
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


def ensure_demo_supply(storage: Storage, demo: DemoConfig, days: int = 7) -> int:
    """Write the demo house's PV and battery history.

    These two are not Shelly devices — they are the same synthetic keys an
    external inverter source writes to (``pv`` / ``battery``), so demo mode
    exercises exactly the code path a real PV installation takes: the solar
    dashboard, the battery tab, and the EV log's source split all read them
    without knowing where they came from.

    Returns the number of rows written; 0 when the series already exist.
    """
    import pandas as pd

    now = int(time.time())
    start = now - int(days) * 86400
    STEP = 900
    written = 0
    for key in (DEMO_PV_KEY, DEMO_BATTERY_KEY):
        try:
            if storage.has_usable_data(key):
                continue
        except Exception:
            pass
        st = DemoState(seed=int(getattr(demo, "seed", 1234)),
                       scenario=str(getattr(demo, "scenario", "household")))
        ts_l, w_l, e_l = [], [], []
        for ts in range(start, now + 1, STEP):
            sol = demo_solar(float(ts), st)
            w = sol["pv"] if key == DEMO_PV_KEY else sol["battery"]
            ts_l.append(pd.to_datetime(ts, unit="s"))
            w_l.append(round(w, 1))
            e_l.append(abs(w) * STEP / 3600.0 / 1000.0)
        if ts_l:
            storage.db.insert_dataframe(key, pd.DataFrame(
                {"timestamp": ts_l, "total_power": w_l, "energy_kwh": e_l}))
            written += len(ts_l)
    return written
