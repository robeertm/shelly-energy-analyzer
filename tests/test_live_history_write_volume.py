"""The Live-tab restart cache must not wear out the disk (issue #20).

Reported 2026-11-17 for an installation with eight plugs at the default 1 s
poll: the analyzer process wrote 25 GB in 12.5 h (~48 GB a day on 17.10.2,
~30 GB a day on 17.10.4) while the database was 8.6 MB. Not the database:
``data/runtime/live_history.json`` held the whole 2 h buffer — ~58,000
samples, one JSON object with 23 named fields each, 10–20 MB — and was
rewritten every 30 s.

These tests pin the fix: a compact, compressed format 2, no write while
nothing changed, a save every five minutes instead of every 30 s, and the old
file still readable so the update itself does not empty the Live tab. The
NILM learner file had the same kind of flaw once its transition list was full.
"""
import gzip
import json
import os
import random
import sys
import time
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.services.webdash import LivePoint, LiveStateStore  # noqa: E402

POINTS = 7250          # 2 h at 1 s + 50, as app_context sizes the buffer
DEVICES = 8


def _plug_store(devices=DEVICES, points=POINTS, seed=20):
    """Eight single-phase plugs as a Shelly reports them: 0.1 W, 0.1 V, 1 mA,
    a day counter in Wh; loads switch on and off now and then."""
    rnd = random.Random(seed)
    st = LiveStateStore(max_points=points)
    t0 = int(time.time()) - points
    for d in range(devices):
        kwh, load = 1.25, 0.0
        for i in range(points):
            if i % 600 == 0:
                load = rnd.choice([0.0, 0.0, 4.8, 62.0, 1180.0])
            w = round(load + rnd.uniform(-0.4, 0.4), 1) if load else 0.0
            kwh += w / 3_600_000.0
            st.update(f"plug{d}", LivePoint(
                ts=t0 + i, power_total_w=w, va=round(rnd.uniform(229.0, 233.0), 1),
                vb=0.0, vc=0.0, ia=round(w / 230.0, 3), ib=0.0, ic=0.0, pa=w,
                kwh_today=round(kwh, 5), cost_today=kwh * 0.31, freq_hz=50.0))
    return st


def test_a_full_snapshot_of_eight_plugs_stays_small(tmp_path):
    st = _plug_store()
    path = tmp_path / "live_history.json.gz"
    assert st.dump_to_path(path) is True
    per_point = path.stat().st_size / (DEVICES * POINTS)
    # Format 1 was ~180–350 bytes per sample. Columns + rounding + gzip: a few.
    assert per_point < 10, f"{per_point:.1f} bytes per sample"
    plain = tmp_path / "live_history.json"
    st.dump_to_path(plain, skip_unchanged=False)
    assert plain.stat().st_size / (DEVICES * POINTS) < 50


def test_a_day_of_saves_writes_well_under_a_gigabyte(tmp_path):
    """The number the reporter measures: bytes a day from this file."""
    from shelly_analyzer.web.background import LIVE_HISTORY_SAVE_SECONDS
    st = _plug_store()
    path = tmp_path / "live_history.json.gz"
    st.dump_to_path(path)
    per_day = path.stat().st_size * (86400 / LIVE_HISTORY_SAVE_SECONDS)
    assert per_day < 200e6, f"{per_day / 1e6:.0f} MB a day"


def test_an_unchanged_store_is_not_written_again(tmp_path):
    st = _plug_store(devices=1, points=100)
    path = tmp_path / "live_history.json.gz"
    assert st.dump_to_path(path) is True
    before = path.stat().st_mtime_ns
    time.sleep(0.02)
    assert st.dump_to_path(path) is False
    assert path.stat().st_mtime_ns == before
    st.update("plug0", LivePoint(ts=int(time.time()), power_total_w=5.0, va=230.0,
                                 vb=0.0, vc=0.0, ia=0.02, ib=0.0, ic=0.0))
    assert st.dump_to_path(path) is True


def test_a_deleted_file_is_written_again_even_without_new_samples(tmp_path):
    st = _plug_store(devices=1, points=10)
    path = tmp_path / "live_history.json.gz"
    st.dump_to_path(path)
    path.unlink()
    assert st.dump_to_path(path) is True
    assert path.exists()


def _close(a, b, tol):
    return abs(a - b) <= tol


def test_the_window_survives_the_round_trip(tmp_path):
    src = LiveStateStore(max_points=POINTS)
    now = int(time.time())
    # A three-phase meter with every field set, a gap in the timestamps, a
    # battery SoC and a non-finite reading that must come back as 0.
    stamps = [now - 300, now - 299, now - 298, now - 120, now - 119]
    for i, ts in enumerate(stamps):
        src.update("meter", LivePoint(
            ts=ts, power_total_w=1234.567 + i, va=230.12, vb=231.27, vc=229.98,
            ia=1.23456, ib=2.34567, ic=float("nan"), pa=400.04, pb=500.05, pc=334.47,
            q_total_var=-12.34, qa=-1.11, qb=-2.22, qc=-3.33, cosphi_total=0.98765,
            pfa=0.991, pfb=0.982, pfc=0.973, kwh_today=3.1234567, cost_today=0.96789,
            freq_hz=49.987, i_n=0.1234, soc_pct=57.25))
    for name in ("live_history.json.gz", "live_history.json"):
        path = tmp_path / name
        src.dump_to_path(path, skip_unchanged=False)
        dst = LiveStateStore(max_points=POINTS)
        assert dst.load_from_path(path) == len(stamps)
        got = list(dst._by_device["meter"])
        want = list(src._by_device["meter"])
        assert [p.ts for p in got] == stamps
        for g, w in zip(got, want):
            assert _close(g.power_total_w, w.power_total_w, 0.05)
            assert _close(g.va, w.va, 0.05) and _close(g.vc, w.vc, 0.05)
            assert _close(g.ia, w.ia, 0.0005) and g.ic == 0.0
            assert _close(g.pc, w.pc, 0.05) and _close(g.qb, w.qb, 0.05)
            assert _close(g.cosphi_total, w.cosphi_total, 0.0005)
            assert _close(g.pfc, w.pfc, 0.0005)
            assert _close(g.kwh_today, w.kwh_today, 0.0000005)
            assert _close(g.cost_today, w.cost_today, 0.00005)
            assert _close(g.freq_hz, w.freq_hz, 0.005)
            assert _close(g.i_n, w.i_n, 0.0005)
            assert _close(g.soc_pct, w.soc_pct, 0.05)


def test_fields_that_never_leave_their_default_are_left_out(tmp_path):
    st = _plug_store(devices=1, points=50)
    path = tmp_path / "live_history.json"
    st.dump_to_path(path)
    data = json.loads(path.read_text())
    assert data["version"] == 2
    cols = data["devices"]["plug0"]["cols"]
    for absent in ("vb", "vc", "ib", "ic", "pb", "pc", "qa", "freq_hz", "soc_pct"):
        assert absent not in cols
    dst = LiveStateStore(max_points=100)
    dst.load_from_path(path)
    p = dst._by_device["plug0"][-1]
    assert p.freq_hz == 50.0 and p.vb == 0.0


def test_the_file_of_the_previous_release_is_still_read(tmp_path):
    """Format 1, uncompressed, as 17.10.4 wrote it: the update that brings the
    new format must not empty the Live tab."""
    now = int(time.time())
    v1 = {"version": 1, "max_points": 100, "saved_at": now, "devices": {"plug1": [
        {"ts": now - 2, "power_total_w": 61.7, "va": 230.4, "vb": 0.0, "vc": 0.0,
         "ia": 0.268, "ib": 0.0, "ic": 0.0, "pa": 61.7, "pb": 0.0, "pc": 0.0,
         "q_total_var": 0.0, "qa": 0.0, "qb": 0.0, "qc": 0.0, "cosphi_total": 0.0,
         "pfa": 0.0, "pfb": 0.0, "pfc": 0.0, "kwh_today": 0.41234, "cost_today": 0.1278,
         "freq_hz": 50.0, "i_n": 0.0},
        {"ts": now - 1, "power_total_w": 62.1, "va": 230.5, "ia": 0.27, "kwh_today": 0.41236},
    ]}}
    path = tmp_path / "live_history.json"
    path.write_text(json.dumps(v1))
    st = LiveStateStore(max_points=100)
    assert st.load_from_path(path) == 2
    last = st._by_device["plug1"][-1]
    assert last.power_total_w == 62.1 and last.kwh_today == 0.41236 and last.freq_hz == 50.0


def test_a_damaged_file_loads_nothing_instead_of_raising(tmp_path):
    path = tmp_path / "live_history.json.gz"
    path.write_bytes(gzip.compress(b'{"version": 2, "devices": {"p": {"ts0": 1, "dts": [1], "cols": {"va": [1]}}}}'))
    assert LiveStateStore(max_points=10).load_from_path(path) == 0
    path.write_bytes(b"\x1f\x8bnot gzip")
    assert LiveStateStore(max_points=10).load_from_path(path) == 0


def test_the_saver_runs_every_few_minutes_not_every_30_seconds():
    from shelly_analyzer.web import background
    assert background.LIVE_HISTORY_SAVE_SECONDS >= 300
    src = Path(background.__file__).read_text(encoding="utf-8")
    body = src.split("def _start_live_history_saver", 1)[1].split("\n    def ", 1)[0]
    assert "LIVE_HISTORY_SAVE_SECONDS" in body
    assert "wait(30.0)" not in body


def test_saving_writes_the_new_file_and_retires_the_old_one(tmp_path):
    from shelly_analyzer.web.background import (BackgroundServiceManager,
                                                legacy_live_history_path, live_history_path)
    legacy = legacy_live_history_path(tmp_path)
    legacy.parent.mkdir(parents=True)
    legacy.write_text('{"version": 1, "devices": {}}')
    mgr = SimpleNamespace(live_store=_plug_store(devices=1, points=20), out_dir=tmp_path)
    mgr._live_history_path = lambda: live_history_path(tmp_path)
    BackgroundServiceManager._save_live_history(mgr)
    assert live_history_path(tmp_path).exists()
    assert live_history_path(tmp_path).read_bytes()[:2] == b"\x1f\x8b"
    assert not legacy.exists()


def test_the_nilm_file_is_not_rewritten_on_every_transition_once_full(tmp_path):
    """At the 5000-transition cap len() stays 5000, so ``len % 10 == 0`` was
    true for every new transition and the whole file (indent=2) was written
    each time. Now: one save per ten transitions, full or not."""
    from shelly_analyzer.services.appliance_detector import PowerTransition, TransitionLearner
    lrn = TransitionLearner(persist_path=tmp_path / "nilm.json")
    lrn._transitions = [PowerTransition(timestamp=float(i), device_key="m", delta_w=100.0,
                                        power_before=0.0, power_after=100.0)
                        for i in range(lrn._max_transitions)]
    saves = []
    lrn._save = lambda path: (saves.append(path), setattr(lrn, "_unsaved", 0))
    ts, found = 1_000_000.0, 0
    for _ in range(25):            # 50 steps: 25 on, 25 off
        for w in (0.0, 0.0, 0.0, 0.0, 800.0, 800.0, 800.0, 800.0,
                  800.0, 800.0, 800.0, 0.0, 0.0, 0.0, 0.0):
            if lrn.observe("m", ts, w) is not None:
                found += 1
            ts += 1.0
    assert found >= 40
    assert len(lrn._transitions) == lrn._max_transitions
    assert len(saves) == found // TransitionLearner.SAVE_EVERY_TRANSITIONS
