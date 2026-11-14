"""Builders for the installation types the walkthrough runs against.

Every builder writes ``config.json`` plus a SQLite database into a directory and
returns nothing; the devices use documentation-only addresses (192.0.2.0/24) so
no network call can succeed.  ``INSTALLATIONS`` maps a short id to a
``(label, builder, env)`` tuple; ``env`` holds extra environment variables for the
server process (for example ``TZ``).

Ids: a plugs only, b meter + plugs, c Gen1 3EM, d single-phase meter,
e fresh install without data, f PV + feed-in, g TOU tariff in another time zone,
h tenant electricity.
"""
from __future__ import annotations

import json
import math
import random
import shutil
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, Tuple

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import pandas as pd  # noqa: E402

from shelly_analyzer.io.config import load_config  # noqa: E402
from shelly_analyzer.io.storage import Storage  # noqa: E402
from shelly_analyzer.services.live_history import LiveHistoryRecorder  # noqa: E402

PLUG_NAMES = [("plug0", "Boiler"), ("plug1", "Freezer"), ("plug2", "TV"), ("plug3", "Lamp"),
              ("plug4", "Router"), ("plug5", "Coffee machine"), ("plug6", "Heater"), ("plug7", "Idle plug")]
PLUG_MODEL = "S3PL-00112EU"  # Plug Gen3


def _base_cfg(devices, lang="en", extra=None):
    cfg = {
        "devices": devices,
        "ui": {"language": lang, "skin": "classic", "theme": "auto", "live_web_enabled": True,
               "live_web_ssl_mode": "off", "autosync_enabled": False},
        "pricing": {"electricity_price_eur_per_kwh": 0.32, "price_includes_vat": True, "vat_enabled": True,
                    "vat_rate_percent": 19.0, "base_fee_eur_per_year": 120.0, "co2_intensity_g_per_kwh": 380.0},
        "co2": {"enabled": True, "bidding_zone": "DE_LU"},
    }
    for k, v in (extra or {}).items():
        if isinstance(v, dict) and isinstance(cfg.get(k), dict):
            cfg[k].update(v)
        else:
            cfg[k] = v
    return cfg


def _write(target: Path, cfg: dict) -> Path:
    target.mkdir(parents=True, exist_ok=True)
    p = target / "config.json"
    p.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
    load_config(p)  # fail early on a schema problem
    return p


def _plug(key, name, i, parent=""):
    d = {"key": key, "name": name, "host": f"192.0.2.{20 + i}", "em_id": 0, "kind": "switch", "gen": 3,
         "model": PLUG_MODEL, "phases": 1, "supports_emdata": False}
    if parent:
        d["parent"] = parent
    return d


def _profile(key: str, tsec: int, day: int, rnd: random.Random) -> float:
    h = tsec / 3600.0
    if key == "plug0":
        return 1850 + rnd.gauss(0, 15) if (5.5 <= h < 6.75 or 17 <= h < 17.75) and (tsec // 300) % 4 != 3 else 4.0
    if key == "plug1":
        return 88 + rnd.gauss(0, 4) if (tsec % 2400) < 900 else 1.5
    if key == "plug2":
        return 105 + 25 * math.sin(tsec / 400.0) + rnd.gauss(0, 6) if 18.5 <= h < 23 else 0.6
    if key == "plug3":
        return 9.0 + rnd.gauss(0, 0.2) if (6.5 <= h < 7.5 or 18 <= h < 23.2) else 0.0
    if key == "plug4":
        return 11.0 + 1.2 * math.sin(tsec / 900.0) + rnd.gauss(0, 0.3)
    if key == "plug5":
        return 1150 + rnd.gauss(0, 40) if (7.0 <= h < 7.12 or 13.0 <= h < 13.05) else 1.0
    if key == "plug6":
        return 1000 + rnd.gauss(0, 12) if (6 <= h < 8 or 18.5 <= h < 22) and (tsec // 420) % 2 == 0 else 3.0
    return 0.0  # plug7: always 0 W


def _fill_plugs(target: Path, plugs, days: float = 2.0, step: int = 60, seed: int = 11):
    rnd = random.Random(seed)
    st = Storage(target / "data")
    rec = LiveHistoryRecorder(st.db)
    now = datetime.now().astimezone().replace(second=0, microsecond=0)
    start = (now - timedelta(days=days)).replace(minute=0)
    t = start
    while t <= now:
        mid = t.replace(hour=0, minute=0, second=0)
        tsec = int((t - mid).total_seconds())
        day = (t.date() - start.date()).days
        ts = int(t.timestamp())
        for key in plugs:
            w = max(0.0, _profile(key, tsec, day, rnd))
            rec.observe(key, ts, w, 230.0 + rnd.gauss(0, 0.8), w / 230.0)
        t += timedelta(seconds=step)
    rec.flush()
    _fill_co2(st, start, now, rnd)


def _fill_co2(st, start, now, rnd):
    rows = []
    hts = int(start.timestamp()) // 3600 * 3600
    endts = int(now.timestamp())
    while hts <= endts:
        hh = datetime.fromtimestamp(hts).astimezone().hour
        ci = 325 + 125 * math.cos(2 * math.pi * (hh - 19) / 24) + rnd.gauss(0, 12)
        rows.append((hts, "DE_LU", round(max(200.0, min(450.0, ci)), 1), "test", endts))
        hts += 3600
    st.db.upsert_co2_intensity(rows)


def _fill_meter(target: Path, key: str, phases: int = 3, days: float = 2.0, base_w: float = 450.0, seed: int = 5):
    """Minute rows (power + running energy counters) for one meter."""
    rnd = random.Random(seed)
    st = Storage(target / "data")
    now = datetime.now().astimezone().replace(second=0, microsecond=0)
    start = now - timedelta(days=days)
    n = int(days * 24 * 60)
    rows = []
    for m in range(n):
        t = start + timedelta(minutes=m)
        h = t.hour + t.minute / 60.0
        total = base_w + 250 * math.sin((h - 6) / 24 * 2 * math.pi) ** 2 + (1800 if 18 <= h < 19 else 0) + rnd.gauss(0, 15)
        total = max(30.0, total)
        parts = [total / phases] * phases + [0.0] * (3 - phases)
        wh = [p / 60.0 for p in parts]  # one row = the energy of that minute
        rows.append({"timestamp": int(t.timestamp()), "a_act_power": parts[0], "b_act_power": parts[1],
                     "c_act_power": parts[2], "total_power": total,
                     "a_total_act_energy": wh[0], "b_total_act_energy": wh[1], "c_total_act_energy": wh[2],
                     "energy_kwh": sum(wh) / 1000.0})
    st.db.insert_dataframe(key, pd.DataFrame(rows))
    _fill_co2(st, start, now, rnd)


# --- builders -----------------------------------------------------------------------

def build_a(target: Path, lang: str = "en"):
    """Plugs only: 8 x Plug Gen3, two days of minute history, one plug always 0 W."""
    devices = [_plug(k, n, i) for i, (k, n) in enumerate(PLUG_NAMES)]
    _write(target, _base_cfg(devices, lang))
    _fill_plugs(target, [k for k, _ in PLUG_NAMES])


def build_b(target: Path, lang: str = "en"):
    """Pro 3EM meter with plugs hanging behind it."""
    meter = {"key": "house", "name": "House", "host": "192.0.2.10", "em_id": 0, "kind": "em", "gen": 2,
             "model": "SPEM-003CEBEU", "phases": 3, "supports_emdata": True}
    plugs = [_plug(k, n, i, parent="house") for i, (k, n) in enumerate(PLUG_NAMES[:5])]
    _write(target, _base_cfg([meter] + plugs, lang))
    _fill_meter(target, "house", 3)
    _fill_plugs(target, [k for k, _ in PLUG_NAMES[:5]])


def build_c(target: Path, lang: str = "en"):
    """Gen1 3EM: flat status fields, no EMData."""
    dev = {"key": "house", "name": "House", "host": "192.0.2.11", "em_id": 0, "kind": "em", "gen": 1,
           "model": "SHEM-3", "phases": 3, "supports_emdata": False}
    _write(target, _base_cfg([dev], lang))
    _fill_meter(target, "house", 3, seed=6)


def build_d(target: Path, lang: str = "en"):
    """Pro EM-50 (single-phase meter)."""
    dev = {"key": "house", "name": "House", "host": "192.0.2.12", "em_id": 0, "kind": "em", "gen": 2,
           "model": "SPEM-002CEBEU50", "phases": 1, "supports_emdata": True}
    _write(target, _base_cfg([dev], lang))
    _fill_meter(target, "house", 1, base_w=300.0, seed=7)


def build_e(target: Path, lang: str = "en"):
    """Fresh install: devices configured, nothing recorded yet."""
    devs = [{"key": "house", "name": "House", "host": "192.0.2.13", "em_id": 0, "kind": "em", "gen": 2,
             "model": "SPEM-003CEBEU", "phases": 3, "supports_emdata": True},
            _plug("plug0", "Boiler", 0)]
    _write(target, _base_cfg(devs, lang))
    Storage(target / "data")  # create the empty database


_SIM_CACHE: Dict[str, Path] = {}


def _simulate(target: Path, lang: str, skin: str, days: int = 10):
    subprocess.run([sys.executable, str(ROOT / "scripts" / "simulate_dataset.py"), str(target), "--days", str(days),
                    "--lang", lang, "--skin", skin], check=True, stdout=subprocess.DEVNULL)


def build_f(target: Path, lang: str = "en"):
    """PV, battery, grid meter with feed-in (full simulated house, aurora skin)."""
    _simulate(target, lang, "aurora")


def build_h(target: Path, lang: str = "en"):
    """Tenant electricity (the simulated house has a tenant flat on its own meter), classic skin."""
    materialize("f", target, lang)  # same simulated house, other skin
    cfgp = target / "config.json"
    cfg = json.loads(cfgp.read_text())
    cfg["ui"]["skin"] = "classic"
    cfgp.write_text(json.dumps(cfg, indent=2))


def build_g(target: Path, lang: str = "en"):
    """TOU tariff built from a URDB-style schedule, server time zone America/New_York."""
    dev = {"key": "house", "name": "House", "host": "192.0.2.14", "em_id": 0, "kind": "em", "gen": 2,
           "model": "SPEM-003CEBEU", "phases": 3, "supports_emdata": True}
    rates = [{"name": "Peak", "price_eur_per_kwh": 0.31, "start_hour": 16, "end_hour": 21, "weekdays_only": True},
             {"name": "Off-peak", "price_eur_per_kwh": 0.14, "start_hour": 21, "end_hour": 16, "weekdays_only": False}]
    extra = {"tou": {"enabled": True, "rates": rates, "urdb_rate": "0123456789abcdef01234567",
                     "urdb_location": "10001"},
             "ev_charging": {"tariff_type": "tou"}}
    _write(target, _base_cfg([dev], lang, extra))
    _fill_meter(target, "house", 3, seed=9)


INSTALLATIONS: Dict[str, Tuple[str, Callable[..., None], Dict[str, str]]] = {
    "a": ("plugs only", build_a, {}),
    "b": ("meter + plugs", build_b, {}),
    "c": ("gen1 3em", build_c, {}),
    "d": ("single phase meter", build_d, {}),
    "e": ("fresh no data", build_e, {}),
    "f": ("pv + feed-in", build_f, {}),
    "g": ("tou tariff new york", build_g, {"TZ": "America/New_York"}),
    "h": ("tenant", build_h, {}),
}



# --- per-process cache: building the data is the slow part, so each installation is built once
# and copied for every use; the cache is removed when the process ends. ----------------------
import atexit  # noqa: E402
import tempfile  # noqa: E402
import threading  # noqa: E402

_CACHE_ROOT = None
_CACHE_LOCK = threading.Lock()
_BUILD_LOCKS: Dict[str, threading.Lock] = {}


def materialize(inst: str, target: Path, lang: str = "en") -> None:
    """Copy the (once built) installation ``inst`` into ``target``."""
    global _CACHE_ROOT
    with _CACHE_LOCK:
        if _CACHE_ROOT is None:
            _CACHE_ROOT = Path(tempfile.mkdtemp(prefix="sea-e2e-cache-"))
            atexit.register(lambda: shutil.rmtree(_CACHE_ROOT, ignore_errors=True))
        lock = _BUILD_LOCKS.setdefault(inst, threading.Lock())
    src = _CACHE_ROOT / inst
    with lock:
        if not src.exists():
            tmp = _CACHE_ROOT / (inst + ".part")
            INSTALLATIONS[inst][1](tmp, lang)
            tmp.rename(src)
    shutil.copytree(src, target, dirs_exist_ok=True)
