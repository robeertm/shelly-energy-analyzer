"""The two hot spots the resource bench found (``tests/e2e/perf_bench.py``).

Eight plugs at a 1 s poll, nice 19 on one core:

* ``EnergyDB.query_samples`` read every one of the 65 columns of the samples
  table and dropped the empty ones afterwards. A plug fills about a dozen, so
  a full history read built five times the Python objects it kept — the
  start-up rebuild of the cached histories was the largest memory peak of the
  process and the largest CPU block after the start.
* the Live-tab restart cache rounded every field of every sample one function
  call at a time, also the 16 fields a plug never fills: ~1 s of CPU per save
  of a full 2 h window (every 5 min and on every stop).

The tests check the mechanism, not a time: which columns the SELECT names,
which columns are rounded value by value, and that the results are exactly
what the old code produced.
"""
import json
import math
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.io.storage import Storage
from shelly_analyzer.services import webdash
from shelly_analyzer.services.live_history import LiveHistoryRecorder
from shelly_analyzer.services.webdash import LivePoint


# --- query_samples ---------------------------------------------------------------------

@pytest.fixture()
def plug_db(tmp_path):
    st = Storage(tmp_path / "data")
    rec = LiveHistoryRecorder(st.db)
    t0 = 1_760_000_000 // 60 * 60
    for i in range(240):
        w = 0.0 if i % 7 == 0 else 40.0 + (i % 13)
        rec.observe("plug0", t0 + i * 60, w, 230.0 + (i % 3) * 0.5, w / 230.0)
        rec.observe("plug1", t0 + i * 60, 5.0, 231.0, 0.02)
    rec.flush()
    return st.db, t0


def _old_query(db, key, start_ts=None, end_ts=None):
    """What query_samples returned before: SELECT *, then drop the empty columns."""
    conn = db._conn()
    cond, params = ["device_key = ?"], [key]
    if start_ts is not None:
        cond.append("timestamp >= ?")
        params.append(int(start_ts))
    if end_ts is not None:
        cond.append("timestamp <= ?")
        params.append(int(end_ts))
    df = pd.read_sql_query(
        f"SELECT * FROM samples WHERE {' AND '.join(cond)} ORDER BY timestamp", conn, params=params)
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.tz_localize(None)
    df = df.drop(columns=["device_key"])
    db._apply_comp(df, key)
    if not df.empty:
        df = df.dropna(axis=1, how="all")
    return df


def _traced_selects(db, fn):
    conn = db._conn()
    seen = []
    conn.set_trace_callback(seen.append)
    try:
        out = fn()
    finally:
        conn.set_trace_callback(None)
    return out, [s for s in seen if "FROM samples" in s and "ORDER BY timestamp" in s]


def test_full_read_names_only_the_columns_that_hold_a_value(plug_db):
    db, _t0 = plug_db
    df, selects = _traced_selects(db, lambda: db.query_samples("plug0"))
    assert len(selects) == 1, selects
    sel = selects[0]
    assert "SELECT *" not in sel, sel
    all_cols = [r[1] for r in db._conn().execute("PRAGMA table_info(samples)")]
    named = [c for c in all_cols if f'"{c}"' in sel.split(" FROM ")[0]]
    assert len(all_cols) > 40
    # A plug fills a handful of columns; the empty ones are never read.
    assert len(named) < len(all_cols) / 2, named
    filled = db._conn().execute(
        "SELECT " + ", ".join(f'count("{c}")' for c in all_cols) + " FROM samples WHERE device_key='plug0'"
    ).fetchone()
    expected = [c for c, n in zip(all_cols, filled) if n and c != "device_key"]
    assert named == expected
    assert "timestamp" in named and "a_act_power" in named and "device_key" not in named


@pytest.mark.parametrize("rng", [(None, None), ("mid", None), (None, "mid"), ("mid", "late")])
def test_full_read_returns_exactly_what_select_star_returned(plug_db, rng):
    db, t0 = plug_db
    pos = {"mid": t0 + 100 * 60, "late": t0 + 180 * 60, None: None}
    s, e = pos[rng[0]], pos[rng[1]]
    for key in ("plug0", "plug1"):
        new = db.query_samples(key, start_ts=s, end_ts=e)
        old = _old_query(db, key, start_ts=s, end_ts=e)
        assert list(new.columns) == list(old.columns)
        pd.testing.assert_frame_equal(new.reset_index(drop=True), old.reset_index(drop=True))


def test_empty_range_keeps_every_column_as_before(plug_db):
    db, t0 = plug_db
    new = db.query_samples("plug0", start_ts=t0 + 10 ** 7)
    old = _old_query(db, "plug0", start_ts=t0 + 10 ** 7)
    assert new.empty and list(new.columns) == list(old.columns)


def test_a_column_subset_is_still_read_as_asked(plug_db):
    db, _t0 = plug_db
    df, selects = _traced_selects(db, lambda: db.query_samples("plug0", columns=["total_power"]))
    assert list(df.columns) == ["timestamp", "total_power"]
    assert 'SELECT "timestamp", "total_power"' in selects[0]


# --- Live-tab cache encoder ------------------------------------------------------------

def _old_encode(points):
    ts = [int(p.ts) for p in points]
    cols = {}
    for name, default, decimals in webdash._PERSIST_FIELDS:
        col = [webdash._round_value(getattr(p, name), decimals) for p in points]
        if any(v != default for v in col):
            cols[name] = col
    dts = [b - a for a, b in zip(ts, ts[1:])]
    return {"ts0": ts[0] if ts else 0, "dts": dts, "cols": cols}


def _plug_points(n=600, odd=None):
    pts = []
    for i in range(n):
        w = 0.0 if i % 50 < 10 else 61.7 + (i % 9) * 0.37
        kw = dict(ts=1_760_000_000 + i, power_total_w=w, va=230.0 + (i % 5) * 0.31, vb=0.0, vc=0.0,
                  ia=round(w / 230.0, 4), ib=0.0, ic=0.0, pa=w, kwh_today=1.25 + i * 1e-5,
                  cost_today=0.4 + i * 3e-6, freq_hz=50.0)
        if odd and i in odd:
            kw.update(odd[i])
        pts.append(LivePoint(**kw))
    return pts


def test_default_only_fields_are_never_rounded_value_by_value(monkeypatch):
    calls = []
    real = webdash._round_column

    def counting(values, decimals):
        calls.append(len(values))
        return real(values, decimals)

    monkeypatch.setattr(webdash, "_round_column", counting)
    pts = _plug_points()
    enc = webdash._encode_points(pts)
    filled = len(enc["cols"])
    # power, va, ia, pa, kwh_today, cost_today vary; the other 17 fields
    # (including freq_hz at its 50 Hz default) are recognised in one pass.
    assert filled == 6, sorted(enc["cols"])
    assert len(calls) == filled, (len(calls), filled)
    assert len(calls) < len(webdash._PERSIST_FIELDS) / 2


@pytest.mark.parametrize("odd", [
    None,
    {3: {"qa": float("nan")}},                      # not finite -> 0, column then all default
    {4: {"pfa": float("inf")}, 5: {"pfb": -float("inf")}},
    {7: {"soc_pct": 1e-9}},                          # rounds to the default
    {8: {"vb": 0}, 9: {"ic": -0.0}},                 # int zero / negative zero
    {10: {"freq_hz": 49.987}, 11: {"q_total_var": 12}},
    {12: {"power_total_w": float("nan")}},           # NaN inside a filled column
])
def test_encoder_output_is_byte_equal_to_the_old_one(odd):
    pts = _plug_points(odd=odd)
    new = json.dumps(webdash._encode_points(pts), separators=(",", ":"), allow_nan=False)
    old = json.dumps(_old_encode(pts), separators=(",", ":"), allow_nan=False)
    assert new == old


def test_round_column_matches_round_value_for_each_value():
    vals = [0.0, -0.0, 0, 1, 2.5, 2.55, -3.14159, 1e-12, 123456.789, True, 49.995]
    for d in (1, 2, 3, 6):
        assert webdash._round_column(list(vals), d) == [webdash._round_value(v, d) for v in vals]
        # same types too: an int stays an int where _round_value gives one
        assert [type(x) for x in webdash._round_column(list(vals), d)] == \
            [type(webdash._round_value(v, d)) for v in vals]
    bad = [1.0, float("nan"), 2.0]
    assert webdash._round_column(bad, 1) == [1, 0, 2]


def test_saved_file_still_loads_back(tmp_path):
    st = webdash.LiveStateStore(max_points=2000)
    for p in _plug_points(900, odd={3: {"qa": float("nan")}}):
        st.update("plug0", p)
    path = tmp_path / "live_history.json.gz"
    assert st.dump_to_path(path, skip_unchanged=False)
    st2 = webdash.LiveStateStore(max_points=2000)
    assert st2.load_from_path(path, max_age_seconds=10 ** 10) == 900
    a, b = list(st._by_device["plug0"]), list(st2._by_device["plug0"])
    assert [p.ts for p in a] == [p.ts for p in b]
    assert all(math.isclose(x.power_total_w, y.power_total_w, abs_tol=0.051) for x, y in zip(a, b))


# --- freed memory goes back to the OS ---------------------------------------------------

def test_release_free_memory_runs_on_glibc_and_is_harmless_elsewhere():
    from shelly_analyzer.core import memory
    import platform
    ok = memory.release_free_memory()
    if sys.platform.startswith("linux") and platform.libc_ver()[0] == "glibc":
        assert ok is True
    else:
        assert ok in (True, False)
    assert memory.release_free_memory() == ok   # looked up once, same answer


def test_warm_cycle_and_full_rebuild_hand_memory_back(monkeypatch):
    """The warm loop trims after every cycle and after every full device read."""
    import inspect
    import tokenize
    import io as _io
    from shelly_analyzer.web import action_dispatch as ad

    def code_only(fn):
        src = inspect.getsource(fn)
        out = []
        for tok in tokenize.generate_tokens(_io.StringIO(src).readline):
            if tok.type in (tokenize.COMMENT, tokenize.STRING):
                continue
            out.append(tok.string)
        return " ".join(out)

    assert "release_free_memory ( )" in code_only(ad.ActionDispatcher._warm_loop)
    full = code_only(ad.ActionDispatcher._load_device_cached)
    # the call sits on the full-read path, after load_device
    assert full.index("load_device ( self . storage") < full.index("release_free_memory ( )")
