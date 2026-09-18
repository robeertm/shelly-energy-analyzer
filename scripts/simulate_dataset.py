#!/usr/bin/env python3
"""Build a simulated installation for screenshots and manual testing.

    python scripts/simulate_dataset.py <target-dir> [--days 60] [--seed 7]
    shelly-analyzer --config <target-dir>/config.json --port 8799 --no-ssl

The house: a 9.8 kWp roof, a 10 kWh battery, a three-phase house meter, a
tenant flat on its own meter, an 11 kW wallbox and a signed grid meter. The
four supply series reconcile minute by minute (grid = load − pv + battery),
the battery's state of charge is written as a measured curve, the grid's
CO₂ intensity follows a daily shape, and the house meter's second-by-second
switching of the last days is fed through the NILM learner so the appliance
tab has something honest to show. No real installation's data is used.

Everything is written through the package's own storage layer, so the hourly
rollups, the session detection and every tab read exactly what a real
installation would have produced.
"""
from __future__ import annotations

import argparse
import json
import math
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pandas as pd  # noqa: E402

from shelly_analyzer.io.storage import Storage  # noqa: E402

TZ = ZoneInfo("Europe/Berlin")
KWP = 9.8
BAT_KWH = 10.0
BAT_MAX_W = 3500.0
BAT_EFF = 0.95

DEVICES = [
    {"key": "house", "name": "House", "phases": 3},
    {"key": "flat", "name": "Apartment", "phases": 3},
    {"key": "wallbox", "name": "Wallbox", "phases": 3},
    {"key": "grid", "name": "Grid", "phases": 3},
]


def _clearsky(h: float, doy: int) -> float:
    """0..1 clear-sky shape for the hour of the day at ~52° N, by day of year."""
    decl = 23.44 * math.sin(2 * math.pi * (284 + doy) / 365)
    lat = 52.5
    half = math.degrees(math.acos(max(-1, min(1, -math.tan(math.radians(lat)) * math.tan(math.radians(decl)))))) / 15
    rise, set_ = 12.9 - half, 12.9 + half
    if h <= rise or h >= set_:
        return 0.0
    x = (h - rise) / (set_ - rise)
    return max(0.0, math.sin(math.pi * x)) ** 1.35


class Day:
    """What one day looks like: clouds, who runs the washing machine, whether the car charges."""

    def __init__(self, rnd: random.Random, date: datetime, prev_cloud: float):
        # clouds persist from day to day
        self.cloud = max(0.0, min(0.95, 0.6 * prev_cloud + 0.4 * rnd.random() + rnd.gauss(0, 0.12)))
        self.cloud_ripple = [rnd.random() for _ in range(48)]
        wd = date.weekday()
        self.dishwasher_at = rnd.choice([7.6, 20.2, 21.0]) if rnd.random() < 0.7 else None
        self.washer_at = rnd.choice([9.5, 10.2, 14.0]) if rnd.random() < 0.55 else None
        self.dryer_at = (self.washer_at + 2.2) if (self.washer_at and rnd.random() < 0.5) else None
        self.oven_at = rnd.choice([12.1, 18.4]) if rnd.random() < 0.4 else None
        self.kettles = sorted(rnd.choice([6.9, 7.2, 7.5]) + k * 0.3 for k in range(rnd.randint(1, 3))) + [rnd.uniform(15, 21) for _ in range(rnd.randint(0, 2))]
        self.tv_from = rnd.uniform(19.0, 20.5)
        self.tv_to = self.tv_from + rnd.uniform(1.5, 3.5)
        # the car: weekday evening 11 kW, weekend midday on the sun, sometimes nothing
        r = rnd.random()
        if wd >= 5 and r < 0.65:
            self.car = ("noon", rnd.uniform(11.0, 12.5), rnd.uniform(1.5, 3.0), 4600.0)
        elif wd < 5 and r < 0.45:
            self.car = ("evening", rnd.uniform(17.5, 19.5), rnd.uniform(0.8, 1.8), 11000.0)
        elif r < 0.55:
            self.car = ("night", rnd.uniform(22.0, 23.5), rnd.uniform(2.0, 4.0), 11000.0)
        else:
            self.car = None
        self.flat_evening = rnd.uniform(600, 1100)
        self.away = rnd.random() < 0.08


def house_w(h: float, d: Day, rnd_noise: float) -> float:
    """Owner's circuits at the local hour h (fridge, base, cooking, appliances)."""
    base = 210 + 60 * math.sin(2 * math.pi * (h - 3) / 24)
    if d.away:
        base = 140
    morning = 900 * math.exp(-((h - 7.4) / 0.6) ** 2)
    evening = 1300 * math.exp(-((h - 19.0) / 1.2) ** 2)
    w = base + (0 if d.away else morning + evening)
    # fridge: 120 W, 14 min on / 31 min off
    if (h * 60) % 45 < 14:
        w += 120
    if d.dishwasher_at and d.dishwasher_at <= h < d.dishwasher_at + 1.6:
        t = h - d.dishwasher_at
        w += 1900 if (t < 0.35 or 0.9 < t < 1.2) else 90
    if d.washer_at and d.washer_at <= h < d.washer_at + 1.5:
        t = h - d.washer_at
        w += 2150 if t < 0.4 else (300 if t < 1.3 else 500)
    if d.dryer_at and d.dryer_at <= h < d.dryer_at + 1.8:
        w += 2500 if ((h - d.dryer_at) * 60) % 8 < 6 else 150
    if d.oven_at and d.oven_at <= h < d.oven_at + 0.9:
        w += 2900 if ((h - d.oven_at) * 60) % 5 < 3 else 60
    for k in d.kettles:
        if k <= h < k + 0.05:
            w += 1600
    if d.tv_from <= h < d.tv_to:
        w += 140
    return max(60.0, w + rnd_noise)


def flat_w(h: float, d: Day, rnd_noise: float) -> float:
    base = 130 + 30 * math.sin(2 * math.pi * (h - 4) / 24)
    if (h * 60 + 7) % 50 < 12:
        base += 95
    w = base + 500 * math.exp(-((h - 7.0) / 0.5) ** 2) + d.flat_evening * math.exp(-((h - 20.0) / 1.4) ** 2)
    return max(50.0, w + rnd_noise)


def car_w(h: float, d: Day, pv: float, house: float) -> float:
    if not d.car:
        return 0.0
    kind, start, dur, pmax = d.car
    if not (start <= h < start + dur):
        return 0.0
    if kind == "noon":
        # surplus charging: what the roof has left, 1.4 kW minimum
        return max(1400.0, min(pmax, pv - house - 200))
    return pmax


def run(target: Path, days: int, seed: int) -> None:
    rnd = random.Random(seed)
    target.mkdir(parents=True, exist_ok=True)
    data_dir = target / "data"
    if (data_dir / "energy.db").exists():
        (data_dir / "energy.db").unlink()
    storage = Storage(data_dir)
    db = storage.db

    now = datetime.now(TZ).replace(second=0, microsecond=0)
    start = (now - timedelta(days=days - 1)).replace(hour=0, minute=0)
    fine_from = now - timedelta(days=8)          # 1-minute resolution for the last 8 days

    soc = 0.55 * BAT_KWH
    cloud = 0.4
    buf = {d["key"]: [] for d in DEVICES}
    buf["pv"] = []
    buf["battery"] = []
    soc_rows = []
    co2_rows = []
    nilm_series = []                              # (ts, w) of the house at 2 s for the last 3 days
    t = start
    day = Day(rnd, t, cloud)
    cur_date = t.date()
    n_rows = 0

    def flush():
        nonlocal n_rows
        for key, rows in buf.items():
            if not rows:
                continue
            df = pd.DataFrame(rows, columns=["timestamp", "total_power", "energy_kwh", "a_act_power", "b_act_power", "c_act_power", "a_voltage", "b_voltage", "c_voltage"])
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
            db.insert_dataframe(key, df)
            n_rows += len(rows)
            rows.clear()

    while t <= now:
        if t.date() != cur_date:
            cur_date = t.date()
            cloud = day.cloud
            day = Day(rnd, t, cloud)
            flush()
        step = 60 if t >= fine_from else 300
        h = t.hour + t.minute / 60 + t.second / 3600
        doy = t.timetuple().tm_yday
        ripple = day.cloud_ripple[int(h * 2) % 48]
        pv = KWP * 1000 * _clearsky(h, doy) * (1 - 0.85 * day.cloud) * (0.75 + 0.5 * ripple) + rnd.gauss(0, 15)
        pv = max(0.0, pv)
        hw = house_w(h, day, rnd.gauss(0, 25))
        fw = flat_w(h, day, rnd.gauss(0, 12))
        cw = car_w(h, day, pv, hw)
        load = hw + fw + cw
        net = pv - load
        dt_h = step / 3600
        if net > 100 and soc < BAT_KWH * 0.995:
            bat = min(net, BAT_MAX_W, (BAT_KWH - soc) / BAT_EFF / dt_h * 1000)
        elif net < -100 and soc > BAT_KWH * 0.05:
            bat = -min(-net, BAT_MAX_W, (soc - BAT_KWH * 0.05) * BAT_EFF / dt_h * 1000)
        else:
            bat = 0.0
        soc += (bat * BAT_EFF if bat > 0 else bat / BAT_EFF) * dt_h / 1000
        soc = max(0.0, min(BAT_KWH, soc))
        grid = load - pv + bat
        ts = int(t.timestamp())
        v = 230 + 2.5 * math.sin(2 * math.pi * h / 24) + rnd.gauss(0, 0.3)

        def row(w, split=(0.42, 0.33, 0.25)):
            return (ts, round(w, 1), w * dt_h / 1000, round(w * split[0], 1), round(w * split[1], 1), round(w * split[2], 1), round(v + 0.8, 1), round(v - 0.6, 1), round(v + 0.2, 1))
        buf["house"].append(row(hw))
        buf["flat"].append(row(fw, (0.5, 0.3, 0.2)))
        buf["wallbox"].append(row(cw, (0.334, 0.333, 0.333)))
        buf["grid"].append(row(grid))
        buf["pv"].append(row(pv))
        buf["battery"].append(row(bat))
        if t >= fine_from:
            soc_rows.append((ts, round(soc / BAT_KWH * 100, 1), round(bat, 1)))
        if t.minute == 0 and t.second == 0:
            # grid intensity: cleaner around noon (wind + sun), dirtier in the evening
            ci = 330 + 110 * math.cos(2 * math.pi * (h - 19) / 24) + 60 * (day.cloud - 0.5) + rnd.gauss(0, 18)
            co2_rows.append((ts, "DE_LU", round(max(120.0, ci), 1), "simulated", ts))
        if t >= now - timedelta(days=3):
            # the NILM learner sees the house every 2 s; appliances step, the rest wobbles
            for s in range(0, step, 2):
                hh = h + s / 3600
                nilm_series.append((ts + s, house_w(hh, day, rnd.gauss(0, 6))))
        t += timedelta(seconds=step)
    flush()

    with db._write_lock:
        conn = db._conn()
        with conn:
            conn.executemany("INSERT OR REPLACE INTO battery_state (timestamp, device_key, soc_pct, power_w, mode) VALUES (?, ?, ?, ?, ?)",
                             [(ts, "battery", s, p, "charging" if p > 50 else ("discharging" if p < -50 else "idle")) for ts, s, p in soc_rows])
    db.upsert_co2_intensity(co2_rows)

    # NILM: let the real learner see the house and persist what it learned
    from shelly_analyzer.services.appliance_detector import TransitionLearner
    nilm_dir = data_dir / "runtime" / "nilm"
    nilm_dir.mkdir(parents=True, exist_ok=True)
    lrn = TransitionLearner(min_step_w=50.0, max_clusters=20, persist_path=nilm_dir / "house.json")
    for ts, w in nilm_series:
        lrn.observe("house", ts, w)
    clusters = lrn.cluster(device_hint="house")
    lrn.flush()

    cfg = {
        "version": "17.1.2",
        "devices": [{"key": d["key"], "name": d["name"], "host": "demo://" + d["key"], "em_id": 0, "kind": "em", "gen": 2,
                     "model": "SPEM-003CEBEU", "phases": d["phases"], "supports_emdata": True} for d in DEVICES],
        "ui": {"language": "de", "theme": "auto", "live_web_enabled": True, "live_web_port": 8799, "live_web_ssl_mode": "off",
               "autosync_enabled": False},
        "demo": {"enabled": True, "seed": seed, "scenario": "pv-home"},
        "pricing": {"electricity_price_eur_per_kwh": 0.32, "price_includes_vat": True, "vat_enabled": True, "vat_rate_percent": 19.0,
                    "base_fee_eur_per_year": 120.0, "base_fee_includes_vat": True, "co2_intensity_g_per_kwh": 380.0},
        "solar": {"enabled": True, "grid_meter_device_key": "grid", "pv_production_device_key": "pv", "battery_device_key": "battery",
                  "feed_in_tariff_eur_per_kwh": 0.082, "kw_peak": KWP, "battery_kwh": BAT_KWH, "pv_embodied_g_per_kwh": 40.0,
                  "battery_manufacturing_g_per_kwh": 20.0, "investment_eur": 18500.0, "installation_year": 2024},
        "battery": {"enabled": True, "device_key": "battery", "capacity_kwh": BAT_KWH, "max_charge_rate_kw": 3.5, "max_discharge_rate_kw": 3.5,
                    "efficiency_pct": 95.0},
        "tenant": {"enabled": True, "tenants": [{"tenant_id": "flat", "name": "Apartment", "device_keys": ["flat"], "persons": 2}],
                   "billing_period_months": 12},
        "ev_charging": {"enabled": True, "wallbox_device_key": "wallbox", "detection_threshold_w": 1500.0, "min_session_minutes": 5},
        "co2": {"enabled": True, "bidding_zone": "DE_LU", "green_threshold_g_per_kwh": 250.0, "dirty_threshold_g_per_kwh": 400.0},
        "weather": {"enabled": False, "lat": 52.52, "lon": 13.405},
        "forecast": {"enabled": True, "horizon_days": 30, "history_days": 90},
        "gamification": {"enabled": True},
    }
    (target / "config.json").write_text(json.dumps(cfg, indent=2), encoding="utf-8")
    print(f"{n_rows} samples, {len(soc_rows)} SOC rows, {len(co2_rows)} intensity hours, "
          f"NILM: {lrn.get_transition_count()} steps → {len(clusters)} clusters "
          f"({', '.join(c.label for c in clusters)})")
    print(f"config: {target / 'config.json'}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("target")
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    run(Path(a.target), a.days, a.seed)
