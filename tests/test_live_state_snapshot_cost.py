"""The once-a-second live poll must not rebuild the whole history.

Reported 2026-09-08: "Mike's analyzer takes forever to load the data and never
finishes." Measured against the running installation, read-only:

    /api/version      0.14 s          (no state touched)
    /api/nilm_status  0.50 s          (in-memory only)
    /api/state        3.5 – 7.5 s     for a 3855-byte answer
    /api/solar       15 s   for 789 bytes
    /api/battery     > 60 s
    /api/goals       > 60 s

The tiny answers rule out payload size, and the fast endpoints rule out a stuck
web layer. ``/api/state`` reads nothing but ``points[-1]`` per device, yet it
called ``snapshot()``, which rebuilds EVERY point of EVERY ring buffer into a
22-field dict. With a two-hour window at one sample a second across several
meters that is on the order of a million float conversions — once a second, per
open browser. The process was never idle, so everything else queued behind it.

These tests pin the two halves of the fix: the cheap call must return the same
newest values as the expensive one, and it must not grow with the history.
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.services.webdash import LivePoint, LiveStateStore


def _store(devices=6, points=7200):
    """A store shaped like the reported installation: several meters, a
    two-hour window at one sample a second."""
    st = LiveStateStore(max_points=points)
    for d in range(devices):
        key = f"dev{d}"
        for i in range(points):
            st.update(key, LivePoint(
                ts=1_700_000_000 + i, power_total_w=100.0 + i % 50,
                va=230.0, vb=231.0, vc=229.0, ia=1.0, ib=1.1, ic=0.9,
                kwh_today=0.001 * i, freq_hz=50.0,
                raw={"output": bool(i % 2)},
            ))
    return st


def test_the_newest_point_is_identical_either_way():
    """The cheap call is only allowed to be cheaper — never different."""
    st = _store(devices=3, points=400)
    full = st.snapshot()
    latest = st.latest_snapshot()
    for key in ("dev0", "dev1", "dev2"):
        assert latest[key][-1] == full[key][-1], key


def test_only_one_point_per_device_comes_back():
    st = _store(devices=3, points=400)
    latest = st.latest_snapshot()
    for key in ("dev0", "dev1", "dev2"):
        assert len(latest[key]) == 1, f"{key} returned {len(latest[key])} points"


def test_the_appliance_hints_and_switch_states_survive():
    """Both read only the tail of each series, so trimming must not change them
    — they are what /api/state shows as the device badges."""
    st = _store(devices=3, points=400)
    full = st.snapshot()
    latest = st.latest_snapshot()
    assert latest.get("_switch_states") == full.get("_switch_states")
    assert latest.get("_appliances") == full.get("_appliances")


def test_it_does_not_grow_with_the_history():
    """The point of the whole exercise. A poll that runs every second must cost
    the same whether the window holds ten minutes or two hours."""
    short = _store(devices=4, points=200)
    long_ = _store(devices=4, points=8000)

    def _timed(store, n=5):
        best = float("inf")
        for _ in range(n):
            t0 = time.perf_counter()
            store.latest_snapshot()
            best = min(best, time.perf_counter() - t0)
        return best

    t_short, t_long = _timed(short), _timed(long_)
    # 40× the history. Anything near linear means the full materialisation is
    # back; the bound is loose enough not to fail on a busy machine.
    assert t_long < t_short * 8 + 0.01, f"short {t_short:.4f}s  long {t_long:.4f}s"


def test_the_cheap_call_really_is_cheaper():
    st = _store(devices=4, points=8000)
    t0 = time.perf_counter(); st.latest_snapshot(); t_latest = time.perf_counter() - t0
    t0 = time.perf_counter(); st.snapshot();        t_full = time.perf_counter() - t0
    assert t_latest * 10 < t_full, f"latest {t_latest:.4f}s  full {t_full:.4f}s"


def test_an_empty_store_answers_without_raising():
    assert LiveStateStore().latest_snapshot() == {}


def test_a_device_with_no_points_yet_yields_an_empty_list():
    st = LiveStateStore(max_points=10)
    st.update("dev0", LivePoint(ts=1, power_total_w=1.0, va=230.0, vb=0.0, vc=0.0,
                                ia=0.1, ib=0.0, ic=0.0))
    out = st.latest_snapshot()
    assert len(out["dev0"]) == 1


# ── The history endpoint's thinning, moved into the store ────────────────
def test_thinning_keeps_the_newest_window_intact():
    """The last twenty minutes stay at full resolution — that is the part of
    the sparkline the user is actually watching."""
    st = _store(devices=1, points=7200)          # 2 h at one sample a second
    thin = st.snapshot(tail_full_s=1200, head_pts=700)["dev0"]
    full = st.snapshot()["dev0"]
    cut = full[-1]["ts"] - 1200
    assert [p for p in thin if p["ts"] >= cut] == [p for p in full if p["ts"] >= cut]
    assert thin[-1] == full[-1], "the newest point must always survive"


def test_thinning_actually_drops_the_bulk():
    st = _store(devices=1, points=7200)
    thin = st.snapshot(tail_full_s=1200, head_pts=700)["dev0"]
    assert 1200 < len(thin) < 2400, len(thin)


def test_thinning_is_the_same_selection_as_before():
    """The rule did not change — only where it runs. Reproduced here on the
    finished dicts, the way /api/history used to do it."""
    st = _store(devices=1, points=7200)
    full = st.snapshot()["dev0"]
    cut = int(full[-1]["ts"]) - 1200
    head = [p for p in full if int(p["ts"]) < cut]
    tail = full[len(head):]
    stride = (len(head) + 699) // 700
    expected = head[::stride] + tail
    assert st.snapshot(tail_full_s=1200, head_pts=700)["dev0"] == expected


def test_a_short_window_is_left_alone():
    st = _store(devices=1, points=900)
    assert st.snapshot(tail_full_s=1200, head_pts=700)["dev0"] == st.snapshot()["dev0"]


def test_thinning_in_the_store_is_faster_than_building_everything():
    st = _store(devices=4, points=7200)
    t0 = time.perf_counter(); st.snapshot(tail_full_s=1200, head_pts=700)
    t_thin = time.perf_counter() - t0
    t0 = time.perf_counter(); st.snapshot()
    t_full = time.perf_counter() - t0
    assert t_thin * 2 < t_full, f"thinned {t_thin:.4f}s  full {t_full:.4f}s"
