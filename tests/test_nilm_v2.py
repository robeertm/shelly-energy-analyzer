"""NILM v2: a step is an appliance only when it holds, pairs with a stop, and
its size, run length, rhythm and time of day agree with a known signature.
The old detector named every 60–90 W step a "fan"; a house with no fan got
ten of them."""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.services.appliance_detector import (  # noqa: E402
    TransitionLearner, classify_cluster, generic_label_for)


def test_a_flicker_is_not_a_switch_on():
    l = TransitionLearner(min_step_w=50)
    t0 = 1_700_000_000
    for i in range(5):
        l.observe("haus", t0 + i, 300.0)
    assert l.observe("haus", t0 + 5, 400.0) is None          # candidate, not yet confirmed
    l.observe("haus", t0 + 6, 300.0)                          # back down → spike
    l.observe("haus", t0 + 7, 300.0)
    assert l.get_transition_count() == 0


def test_a_held_step_is_confirmed_and_paired():
    l = TransitionLearner(min_step_w=50)
    t0 = 1_700_000_000
    for i in range(5):
        l.observe("haus", t0 + i, 300.0)
    tr = None
    for i in range(5, 9):
        tr = l.observe("haus", t0 + i, 400.0) or tr
    assert tr is not None and abs(tr.delta_w - 100) < 1e-6 and tr.timestamp == t0 + 5
    for i in range(9, 20):
        l.observe("haus", t0 + i, 400.0)
    for i in range(20, 26):
        l.observe("haus", t0 + i, 300.0)                     # off after ~15 s
    assert l.get_transition_count() == 2
    runs = l._pair_runs()
    assert len(runs) == 1 and abs(list(runs.values())[0] - 15 / 60) < 0.05


def test_sixty_watts_for_hours_in_the_evening_is_a_light_not_a_fan():
    hist = [0] * 24
    for h in (18, 19, 20, 21, 22):
        hist[h] += 4
    ranked = classify_cluster(60.0, duration_min=180, runs_per_day=2, hour_hist=hist)
    ids = [s.id for s, _ in ranked]
    assert ids[0] in ("led_light", "tv"), ids
    assert ids.index("fan") > 1 if "fan" in ids else True


def test_a_fridge_is_recognised_by_its_rhythm():
    hist = [3] * 24                                           # day and night alike
    ranked = classify_cluster(120.0, duration_min=18, runs_per_day=30, hour_hist=hist)
    assert ranked[0][0].id == "fridge" and ranked[0][1] > 0.5


def test_a_kettle_is_short_and_hot():
    hist = [0] * 24
    hist[7] = 5; hist[12] = 3; hist[19] = 2
    ranked = classify_cluster(2100.0, duration_min=3, runs_per_day=4, hour_hist=hist)
    assert ranked[0][0].id == "kettle"


def test_a_wallbox_circuit_is_the_car_even_at_single_phase():
    ranked = classify_cluster(1400.0, duration_min=70, runs_per_day=1.5, hour_hist=[1] * 24, device_hint="wallbox wallbox")
    assert ranked[0][0].id == "ev_charger"


def test_flicker_at_five_minutes_a_hundred_times_a_day_is_named_honestly():
    ranked = classify_cluster(55.0, duration_min=5, runs_per_day=170, hour_hist=[7] * 24)
    assert not ranked or ranked[0][1] < 0.45
    assert generic_label_for(55.0) == "small_load"
    assert generic_label_for(2100.0) == "large_load"


def test_pre_v17_store_is_not_relearned(tmp_path):
    p = tmp_path / "haus.json"
    p.write_text('{"clusters": [{"cluster_id": 0, "centroid_w": 65, "std_w": 2, "count": 100, "label": "fan", "matched_appliance": "fan"}], "transitions": [{"timestamp": 1, "device_key": "haus", "delta_w": 65, "power_before": 0, "power_after": 65}]}')
    l = TransitionLearner(persist_path=p)
    assert l.get_transition_count() == 0 and l.get_clusters() == []
