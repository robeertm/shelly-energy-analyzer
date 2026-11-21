"""Every periodic disk writer stays small (issue #20 follow-up, 17.11.1).

17.10.5 fixed the Live-tab restart cache that wrote tens of GB a day. The
follow-up measured the whole process with eight simulated plugs at 1 s
(``tests/e2e/write_load.py``) and went through every other writer:

* the minute history of plugs was one SQLite transaction per plug and minute —
  eight commits a minute, each dirtying its own WAL, ``-shm`` and hourly-table
  pages; now one transaction a minute for all of them;
* the NILM learner file (three-phase meters, up to ~0.9 MB) was written twice
  every five minutes plus after every ten transitions — now only when it
  changed, at most every 30 minutes, compact;
* werkzeug logged every request at INFO: one open Live tab is ~86,000 lines a
  day in the journal or the container log — now only failed requests;
* a crash between writing and renaming a Live snapshot left a temp file that
  nothing ever removed.

The process-level budget itself is held by ``tests/e2e/test_write_load_budget.py``.
"""
import json
import logging
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.io.storage import Storage  # noqa: E402
from shelly_analyzer.services.live_history import BATCH_GRACE_S, LiveHistoryRecorder  # noqa: E402

PLUGS = [f"plug{i}" for i in range(8)]


def _t0():
    return (int(time.time()) // 3600 - 3) * 3600


class _CountingDB:
    """Wraps the real EnergyDB and counts transactions of the minute history."""

    def __init__(self, real):
        self.real = real
        self.batches = []

    def insert_dataframes(self, frames):
        self.batches.append(sorted(frames))
        return self.real.insert_dataframes(frames)

    def insert_dataframe(self, key, df):  # pragma: no cover - must not be used
        raise AssertionError("one transaction per device again")


def _feed(rec, t0, seconds, watts=100.0, skew=None):
    """All eight plugs, once a second, each answering a little later than the last."""
    for s in range(seconds):
        for i, key in enumerate(PLUGS):
            off = (skew or {}).get(key, 0)
            rec.observe(key, t0 + s + off, watts + i, 230.0, (watts + i) / 230.0)


def test_eight_plugs_write_one_transaction_a_minute():
    with tempfile.TemporaryDirectory() as tmp:
        st = Storage(Path(tmp) / "data")
        db = _CountingDB(st.db)
        rec = LiveHistoryRecorder(db)
        t0 = _t0()
        _feed(rec, t0, 5 * 60 + 2)
        # Five minutes completed; every one of them is a single batch with all plugs.
        assert len(db.batches) == 5, db.batches
        assert all(b == sorted(PLUGS) for b in db.batches)
        rec.flush()
        for key in PLUGS:
            df = st.db.query_samples(key)
            assert len(df) == 6  # five full minutes + the open one from flush()


def test_the_rows_are_the_same_as_written_one_by_one():
    with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
        sa, sb = Storage(Path(a) / "data"), Storage(Path(b) / "data")
        rec = LiveHistoryRecorder(sa.db)
        t0 = _t0()
        _feed(rec, t0, 3 * 60 + 1)
        rec.flush()
        for key in PLUGS:
            df = sa.db.query_samples(key)
            assert not df.empty
            sb.db.insert_dataframe(key, df)  # the per-device path
        for key in PLUGS:
            ha = sa.db._conn().execute(
                "SELECT hour_ts, round(kwh, 9), sample_count FROM hourly_energy WHERE device_key=? ORDER BY 1",
                (key,)).fetchall()
            hb = sb.db._conn().execute(
                "SELECT hour_ts, round(kwh, 9), sample_count FROM hourly_energy WHERE device_key=? ORDER BY 1",
                (key,)).fetchall()
            assert ha and ha == hb


def test_a_late_device_does_not_hold_the_minute_back_forever():
    with tempfile.TemporaryDirectory() as tmp:
        st = Storage(Path(tmp) / "data")
        db = _CountingDB(st.db)
        rec = LiveHistoryRecorder(db)
        t0 = _t0()
        # plug7 stops answering after 30 s; the others go on.
        for s in range(0, 60 + BATCH_GRACE_S + 3):
            for i, key in enumerate(PLUGS):
                if key == "plug7" and s > 30:
                    continue
                rec.observe(key, t0 + s, 50.0, 230.0, 0.2)
        assert db.batches, "the minute of the seven plugs never reached the database"
        assert "plug0" in db.batches[0]


def test_a_failing_database_is_retried_next_minute_not_every_second():
    class _Flaky(_CountingDB):
        fail = True

        def insert_dataframes(self, frames):
            self.batches.append(sorted(frames))
            if self.fail:
                raise sqlite3.OperationalError("database is locked")
            return self.real.insert_dataframes(frames)

    with tempfile.TemporaryDirectory() as tmp:
        st = Storage(Path(tmp) / "data")
        db = _Flaky(st.db)
        rec = LiveHistoryRecorder(db)
        t0 = _t0()
        _feed(rec, t0, 2 * 60 + 5)
        # One attempt for the first minute, one for the second — not ~480.
        assert 1 <= len(db.batches) <= 3, len(db.batches)
        db.fail = False
        _feed(rec, t0 + 2 * 60 + 5, 60)
        rec.flush()
        assert len(st.db.query_samples("plug3")) >= 3


# ── NILM learner file ─────────────────────────────────────────────────────────

def _full_learner(path):
    from shelly_analyzer.services.appliance_detector import PowerTransition, TransitionLearner
    lrn = TransitionLearner(min_step_w=50.0, max_clusters=20, persist_path=path)
    t = 1.7e9
    for i in range(lrn._max_transitions):
        t += 120.0 + (i % 7) * 40.0
        w = (60, 120, 800, 1500, 2000)[i % 5] + (i % 13)
        lrn._transitions.append(PowerTransition(
            timestamp=t, device_key="m", delta_w=w if i % 2 == 0 else -w,
            power_before=300.0, power_after=300.0 + w))
    return lrn


def test_reclustering_every_five_minutes_does_not_rewrite_the_file(tmp_path):
    from shelly_analyzer.services.appliance_detector import TransitionLearner
    path = tmp_path / "meter.json"
    lrn = _full_learner(path)
    lrn._unsaved = 50
    lrn._last_save = time.time() - TransitionLearner.SAVE_MIN_INTERVAL_S - 1
    writes = []
    real_write = Path.write_text

    def spy(self, data, *a, **k):
        if self == path:
            writes.append(len(data))
        return real_write(self, data, *a, **k)

    Path.write_text = spy
    try:
        lrn.cluster(device_hint="house")       # due: one write
        for _ in range(12):                    # an hour of 5-minute re-clusters
            lrn.cluster(device_hint="house")
        lrn.flush()                            # nothing changed since: no write
    finally:
        Path.write_text = real_write
    assert len(writes) == 1, writes
    # Compact: no indentation, and the learning survives a reload.
    text = path.read_text()
    assert "\n" not in text
    assert writes[0] < 700_000, writes[0]
    again = TransitionLearner(min_step_w=50.0, max_clusters=20, persist_path=path)
    assert len(again._transitions) == len(lrn._transitions)
    assert len(again._clusters) == len(lrn._clusters)


def test_a_stop_writes_what_was_learned_since_the_last_save(tmp_path):
    from shelly_analyzer.services.appliance_detector import TransitionLearner
    path = tmp_path / "meter.json"
    lrn = TransitionLearner(min_step_w=50.0, persist_path=path)
    ts = 1_000_000.0
    for _ in range(3):
        for w in (0.0,) * 4 + (900.0,) * 6 + (0.0,) * 6:
            lrn.observe("m", ts, w)
            ts += 1.0
    assert lrn.get_transition_count() >= 4
    assert not path.exists()                   # periodic save not due yet
    lrn.flush()                                # stop / update / reload
    data = json.loads(path.read_text())
    assert len(data["transitions"]) == lrn.get_transition_count()


def test_the_background_service_no_longer_saves_twice_per_recluster():
    import inspect
    from shelly_analyzer.web.background import BackgroundServiceManager
    src = inspect.getsource(BackgroundServiceManager._push_nilm_to_store)
    assert "lrn.flush()" not in src


# ── console log ───────────────────────────────────────────────────────────────

def _access_record(status, path="/api/state"):
    return logging.LogRecord(
        "werkzeug", logging.INFO, __file__, 1,
        '%s - - [%s] "%s" %s %s',
        ("127.0.0.1", "21/Nov/2026 10:36:28", f"GET {path} HTTP/1.1", str(status), "-"), None)


def test_successful_requests_are_not_logged_failed_ones_are():
    from shelly_analyzer import web
    f = web._QuietAccessLog()
    root = logging.getLogger()
    old = root.level
    root.setLevel(logging.INFO)
    try:
        assert f.filter(_access_record(200)) is False
        assert f.filter(_access_record(304)) is False
        assert f.filter(_access_record(404)) is True
        assert f.filter(_access_record(500)) is True
        # Colour codes around the status (a terminal) change nothing.
        colored = _access_record("\x1b[33m200\x1b[0m")
        assert f.filter(colored) is False
        # Warnings and errors of werkzeug itself always pass.
        warn = logging.LogRecord("werkzeug", logging.WARNING, __file__, 1, "boom", None, None)
        assert f.filter(warn) is True
        # The log tab's "HTTP" switch and debug logging bring them back.
        web.set_log_include_http(True)
        assert f.filter(_access_record(200)) is True
        web.set_log_include_http(False)
        root.setLevel(logging.DEBUG)
        assert f.filter(_access_record(200)) is True
    finally:
        root.setLevel(old)
        web.set_log_include_http(False)


def test_the_filter_sits_on_the_werkzeug_logger_once(tmp_path):
    from shelly_analyzer import web
    web._install_quiet_access_log()
    web._install_quiet_access_log()
    flt = [f for f in logging.getLogger("werkzeug").filters if isinstance(f, web._QuietAccessLog)]
    assert len(flt) == 1


# ── orphaned temp files ───────────────────────────────────────────────────────

def test_a_temp_file_left_by_a_crash_is_removed_a_fresh_one_is_not(tmp_path):
    from shelly_analyzer.web.background import remove_orphaned_live_history_tmp
    rt = tmp_path / "data" / "runtime"
    rt.mkdir(parents=True)
    old = rt / ".live_history.tf1lri_x.json.tmp"
    old.write_bytes(b"x" * 1000)
    past = time.time() - 6 * 3600
    os.utime(old, (past, past))
    fresh = rt / ".live_history.abc123.tmp"      # a save in progress right now
    fresh.write_bytes(b"y")
    keep = rt / "live_history.json.gz"
    keep.write_bytes(b"z")
    os.utime(keep, (past, past))
    assert remove_orphaned_live_history_tmp(tmp_path) == 1
    assert not old.exists()
    assert fresh.exists() and keep.exists()
    assert remove_orphaned_live_history_tmp(tmp_path / "nowhere") == 0


# ── no rewrite of the history on every start ──────────────────────────────────

def test_plug_rows_carry_the_neutral_current_so_a_restart_rewrites_nothing():
    """The start-up backfill computed ``n_avg_current`` for every row that had
    phase currents but none of its own — and the plug minute history never
    wrote one, so each restart or update rewrote every plug row since the
    previous start (measured: the whole 3 MB database of two days of history)."""
    with tempfile.TemporaryDirectory() as tmp:
        st = Storage(Path(tmp) / "data")
        rec = LiveHistoryRecorder(st.db)
        _feed(rec, _t0(), 3 * 60 + 1)
        rec.flush()
        conn = st.db._conn()
        rows = conn.execute("SELECT a_current, n_avg_current FROM samples").fetchall()
        assert rows and all(n is not None and abs(n - a) < 1e-9 for a, n in rows)
        before = conn.total_changes
        st.db._backfill_n_current(conn)
        assert conn.total_changes == before


def test_the_insert_computes_what_the_backfill_would_have():
    import pandas as pd
    from shelly_analyzer.io.database import _neutral_current
    with tempfile.TemporaryDirectory() as tmp:
        st = Storage(Path(tmp) / "data")
        t0 = _t0()
        df = pd.DataFrame([
            {"timestamp": t0, "a_current": 4.0, "b_current": 1.5, "c_current": 2.25, "energy_kwh": 0.01},
            {"timestamp": t0 + 60, "a_avg_current": 3.0, "b_avg_current": 3.0, "c_avg_current": 3.0},
            {"timestamp": t0 + 120, "a_current": 2.0, "n_avg_current": 0.7},   # measured value wins
            {"timestamp": t0 + 180, "total_power": 5.0},                         # no current at all
        ])
        st.db.insert_dataframe("em", df)
        got = [r[0] for r in st.db._conn().execute(
            "SELECT n_avg_current FROM samples WHERE device_key='em' ORDER BY timestamp")]
        # The same rows without the value, healed by the backfill:
        st.db._conn().execute("UPDATE samples SET n_avg_current = NULL WHERE device_key='em'")
        st.db._conn().commit()
        st.db._backfill_n_current(st.db._conn())
        healed = [r[0] for r in st.db._conn().execute(
            "SELECT n_avg_current FROM samples WHERE device_key='em' ORDER BY timestamp")]
        assert got[0] == pytest.approx(healed[0]) == pytest.approx(_neutral_current(None, 4.0, 1.5, 2.25))
        assert got[1] == pytest.approx(0.0, abs=1e-9)                        # balanced load
        assert got[2] == 0.7
        assert got[3] is None and healed[3] is None


def test_an_emdata_import_without_an_n_column_is_complete_too():
    csv_text = ("timestamp,a_current,b_current,c_current,a_act_power,b_act_power,c_act_power\n"
                f"{_t0()},1.0,2.0,3.0,230,460,690\n{_t0() + 60},1.0,2.0,3.0,230,460,690\n")
    with tempfile.TemporaryDirectory() as tmp:
        st = Storage(Path(tmp) / "data")
        assert st.db.insert_csv_bytes("em3", csv_text.encode()) == 2
        conn = st.db._conn()
        assert all(r[0] is not None for r in conn.execute("SELECT n_avg_current FROM samples"))
        before = conn.total_changes
        st.db._backfill_n_current(conn)
        assert conn.total_changes == before
