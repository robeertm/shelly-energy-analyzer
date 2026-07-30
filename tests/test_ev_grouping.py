"""Intelligent EV charging-session grouping (surplus-pause aware).

A surplus (PV) charge fragments into many short sessions whenever solar drops
below the car's minimum charge power. group_sessions_into_charges merges the
fragments back into one physical charge, but only when the pause was actually
forced by low surplus (car still plugged in) — not when surplus was going spare
(car unplugged).
"""
import os
import sys

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.services.ev_charging_log import (
    ChargingSession, group_sessions_into_charges,
)

BASE = 1_700_000_000


def _s(sid, start, end, kwh=5.0):
    return ChargingSession(session_id=sid, device_key="wb", start_ts=start,
                           end_ts=end, energy_kwh=kwh, peak_power_w=7000,
                           avg_power_w=6000, cost_eur=round(kwh * 0.3, 2))


def _two_sessions_1h_gap():
    a = _s("a", BASE, BASE + 3600)
    b = _s("b", BASE + 7200, BASE + 10800)  # 3600 s gap
    ts = np.arange(BASE, BASE + 11000, 60)
    return a, b, ts


def test_low_surplus_gap_is_merged():
    a, b, ts = _two_sessions_1h_gap()
    low = np.full(ts.shape, 500.0)  # < 1500 W floor → couldn't charge → pause
    g = group_sessions_into_charges([a, b], max_gap_s=14400,
                                    surplus_ts=ts, surplus_export_w=low,
                                    min_charge_w=1500)
    assert len(g) == 1
    assert g[0].session_count == 2
    assert g[0].energy_kwh == 10.0
    assert g[0].cost_eur == 3.0
    assert g[0].start_ts == a.start_ts and g[0].end_ts == b.end_ts
    assert [s.session_id for s in g[0].sessions] == ["a", "b"]


def test_high_surplus_gap_is_not_merged():
    a, b, ts = _two_sessions_1h_gap()
    high = np.full(ts.shape, 5000.0)  # surplus spare → car unplugged → split
    g = group_sessions_into_charges([a, b], max_gap_s=14400,
                                    surplus_ts=ts, surplus_export_w=high,
                                    min_charge_w=1500)
    assert len(g) == 2


def test_overnight_gap_stays_separate_even_if_low_surplus():
    a, b, ts = _two_sessions_1h_gap()
    low = np.full(ts.shape, 500.0)
    c = _s("c", BASE + 50000, BASE + 53600)  # ~39 200 s gap > max_gap
    g = group_sessions_into_charges([a, b, c], max_gap_s=14400,
                                    surplus_ts=ts, surplus_export_w=low,
                                    min_charge_w=1500)
    assert len(g) == 2
    assert g[0].session_count == 2 and g[1].session_count == 1


def test_gap_only_fallback_without_surplus_series():
    a, b, _ = _two_sessions_1h_gap()
    g = group_sessions_into_charges([a, b], max_gap_s=14400)
    assert len(g) == 1  # no series → gap alone decides


def test_no_surplus_samples_in_gap_falls_back_to_gap():
    a, b, _ = _two_sessions_1h_gap()
    # series exists but covers a different time range → no samples in the gap
    ts = np.arange(BASE + 100000, BASE + 110000, 60)
    exp = np.full(ts.shape, 9000.0)
    g = group_sessions_into_charges([a, b], max_gap_s=14400,
                                    surplus_ts=ts, surplus_export_w=exp,
                                    min_charge_w=1500)
    assert len(g) == 1  # gap within max_gap and no evidence → merge


def test_empty_and_single():
    assert group_sessions_into_charges([]) == []
    a = _s("a", BASE, BASE + 3600)
    g = group_sessions_into_charges([a])
    assert len(g) == 1 and g[0].session_count == 1 and g[0].group_id == "a"
