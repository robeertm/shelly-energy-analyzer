"""History for switches and plugs that have no EMData (GitHub issue #18).

A plug such as the Plug S/M Gen3 has no EMData component, so the EMData sync
never had anything to import for it: About said "Samples: 0", Costs, Heatmap,
CO2, Energy Flow and Compare stayed empty, and every sync logged a 404 for
``EMData.GetData``. What a plug does report is live. ``services/live_history``
now writes one row per minute from that live feed into the same table the
EMData import fills.

What these tests pin down:

* the recorder: energy from the counter when it can be trusted, a trapezoid
  when it cannot (reset, implausible jump, no counter), no energy made up over
  a gap, nothing lost on a hot reload, a failing database never raises;
* the wiring: only real switches are recorded (never meters, never demo hosts),
  the sync does not ask a switch for EMData any more;
* the two things that must NOT change for an installation whose plugs hang
  behind a meter: every whole-house figure stays what it was with and without
  the plugs' rows, and "kWh today" neither jumps nor counts a minute twice.

The Plots page change (an empty kWh chart no longer goes through Plotly) is
guarded by a source test and a syntax check of the rendered page script; that
is weaker than a browser run and says so.
"""
import json
import os
import queue
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.io.config import DeviceConfig, load_config
from shelly_analyzer.io.storage import Storage
from shelly_analyzer.services.compute import load_device
from shelly_analyzer.services.live import LiveSample
from shelly_analyzer.services.live_history import (
    LiveHistoryRecorder, counter_wh_from_status, records_history,
)
from shelly_analyzer.services.webdash import LiveStateStore

MIN = 60


def _t0(day=(2026, 3, 10), hour=10):
    """A minute-aligned local timestamp on a fixed day (no dependency on today)."""
    return int(datetime(day[0], day[1], day[2], hour, 0).timestamp())


def _run(rec, key, t0, seconds, watts, wh0=None, step=1):
    """Feed ``seconds`` of constant load; the counter (if any) is exact."""
    for s in range(0, seconds + 1, step):
        wh = None if wh0 is None else wh0 + watts * s / 3600.0
        rec.observe(key, t0 + s, watts, 230.0, watts / 230.0, wh)


def _storage(tmp):
    return Storage(Path(tmp) / "data")


def _rows_wh(st, key):
    df = st.db.query_samples(key)
    return df, (float(df["energy_kwh"].sum()) * 1000.0 if not df.empty else 0.0)


# ── the recorder ─────────────────────────────────────────────────────────────

def test_a_plug_gets_minute_rows_that_every_history_view_can_read():
    with tempfile.TemporaryDirectory() as tmp:
        st = _storage(tmp)
        rec = LiveHistoryRecorder(st.db)
        t0 = _t0()
        _run(rec, "plug1", t0, 180, 230.0, wh0=1000.0)       # three minutes at 230 W
        rec.flush()

        want_wh = 230.0 * 180 / 3600.0                         # 11.5 Wh
        df, got_wh = _rows_wh(st, "plug1")
        assert not df.empty
        assert abs(got_wh - want_wh) < 1e-6, (got_wh, want_wh)

        # the app's own loader: the exact per-interval path, not the power guess
        dev = DeviceConfig(key="plug1", name="Plug", host="192.0.2.1", kind="switch", supports_emdata=False)
        cd = load_device(st, dev)
        assert not cd.df.empty
        assert abs(float(cd.df["energy_kwh"].sum()) - want_wh / 1000.0) < 1e-9
        assert set(cd.df["calc_method"]) == {"interval(Wh)"}

        hourly = st.db.query_hourly("plug1")
        assert abs(float(hourly["kwh"].sum()) - want_wh / 1000.0) < 1e-9
        assert st.db.has_data("plug1")


def test_the_counter_beats_the_power_samples_when_both_are_there():
    """A counter catches what happens between two polls; the power samples
    cannot. Here the plug claims 100 W but its counter says 200 W worth."""
    with tempfile.TemporaryDirectory() as tmp:
        st = _storage(tmp)
        rec = LiveHistoryRecorder(st.db)
        t0 = _t0()
        for s in range(0, 121):
            rec.observe("plug1", t0 + s, 100.0, 230.0, 0.4, 50.0 + 200.0 * s / 3600.0)
        rec.flush()
        _, got = _rows_wh(st, "plug1")
        assert abs(got - 200.0 * 120 / 3600.0) < 1e-6


def test_a_counter_reset_falls_back_to_the_power_and_is_never_negative():
    with tempfile.TemporaryDirectory() as tmp:
        st = _storage(tmp)
        rec = LiveHistoryRecorder(st.db)
        t0 = _t0()
        _run(rec, "plug1", t0, 60, 100.0, wh0=5000.0)
        # the device restarted: the counter starts again near zero
        for s in range(61, 121):
            rec.observe("plug1", t0 + s, 100.0, 230.0, 0.4, 0.5 + 100.0 * (s - 61) / 3600.0)
        rec.flush()
        df, got = _rows_wh(st, "plug1")
        assert (pd.to_numeric(df["energy_kwh"]) >= 0).all()
        # one second was integrated from the power (100 W · 1 s), the rest from the counter
        want = 100.0 * 120 / 3600.0
        assert abs(got - want) < 0.05, (got, want)


def test_a_counter_that_jumps_by_more_than_a_plug_could_draw_is_not_believed():
    with tempfile.TemporaryDirectory() as tmp:
        st = _storage(tmp)
        rec = LiveHistoryRecorder(st.db)
        t0 = _t0()
        rec.observe("plug1", t0, 100.0, 230.0, 0.4, 10.0)
        rec.observe("plug1", t0 + 1, 100.0, 230.0, 0.4, 10.0 + 1_000_000.0)   # 1 MWh in a second
        rec.observe("plug1", t0 + 2, 100.0, 230.0, 0.4, 10.0 + 1_000_000.0 + 0.03)
        rec.flush()
        _, got = _rows_wh(st, "plug1")
        assert got < 1.0, got          # 2 s of 100 W, not a megawatt-hour


def test_nothing_is_made_up_over_a_gap():
    with tempfile.TemporaryDirectory() as tmp:
        st = _storage(tmp)
        rec = LiveHistoryRecorder(st.db)
        t0 = _t0()
        _run(rec, "plug1", t0, 60, 100.0)                       # a minute of load …
        _run(rec, "plug1", t0 + 60 + 1000, 60, 100.0)           # … 1000 s of silence, a minute again
        rec.flush()
        _, got = _rows_wh(st, "plug1")
        assert abs(got - 2 * 100.0 * 60 / 3600.0) < 1e-6, got   # not the 27.8 Wh of the gap


def test_a_flush_on_a_hot_reload_does_not_lose_the_rest_of_that_minute():
    with tempfile.TemporaryDirectory() as tmp:
        st = _storage(tmp)
        rec = LiveHistoryRecorder(st.db)
        t0 = _t0()
        _run(rec, "plug1", t0, 30, 600.0)
        rec.flush()                                   # stop_all() of a reload, mid-minute
        for s in range(31, 60):                       # the same minute goes on
            rec.observe("plug1", t0 + s, 600.0, 230.0, 2.6)
        rec.flush()
        df, got = _rows_wh(st, "plug1")
        assert len(df) == 1
        assert abs(got - 600.0 * 59 / 3600.0) < 1e-6, got


def test_a_device_that_goes_quiet_still_gets_its_last_minute_written():
    with tempfile.TemporaryDirectory() as tmp:
        st = _storage(tmp)
        rec = LiveHistoryRecorder(st.db)
        t0 = _t0()
        _run(rec, "plug1", t0, 30, 100.0)
        assert st.db.query_samples("plug1").empty               # still in memory
        rec.observe("plug2", t0 + 200, 10.0)                    # another device speaks later
        assert not st.db.query_samples("plug1").empty


def test_a_failing_database_never_raises_and_the_rows_come_later():
    class _Flaky:
        def __init__(self, real):
            self.real, self.fail, self.calls = real, True, 0

        def insert_dataframe(self, key, df):
            self.calls += 1
            if self.fail:
                raise RuntimeError("database is locked")
            return self.real.insert_dataframe(key, df)

    with tempfile.TemporaryDirectory() as tmp:
        st = _storage(tmp)
        flaky = _Flaky(st.db)
        rec = LiveHistoryRecorder(flaky)
        t0 = _t0()
        _run(rec, "plug1", t0, 150, 100.0)                       # two minutes complete while it is down
        assert st.db.query_samples("plug1").empty
        flaky.fail = False
        _run(rec, "plug1", t0 + 151, 70, 100.0)
        rec.flush()
        df, got = _rows_wh(st, "plug1")
        assert len(df) >= 3
        assert got > 100.0 * 200 / 3600.0 * 0.95


def test_the_status_payload_gives_the_counter_in_wh():
    assert counter_wh_from_status({"aenergy": {"total": 1234.5}}) == 1234.5
    assert counter_wh_from_status({"aenergy": {"total": -1}}) is None
    assert counter_wh_from_status({"aenergy": "x"}) is None
    assert counter_wh_from_status({"output": True}) is None
    assert counter_wh_from_status(None) is None


# ── the wiring ───────────────────────────────────────────────────────────────

def _cfg_json(tmp, devices, extra=None):
    p = Path(tmp) / "config.json"
    body = {"devices": devices}
    body.update(extra or {})
    p.write_text(json.dumps(body))
    return p


def _sample(key, ts, watts, wh=None):
    raw = {"output": True}
    if wh is not None:
        raw["aenergy"] = {"total": wh}
    return LiveSample(
        device_key=key, device_name=key, ts=ts,
        power_w={"a": watts, "b": 0.0, "c": 0.0, "total": watts},
        voltage_v={"a": 230.0, "b": 0.0, "c": 0.0},
        current_a={"a": watts / 230.0, "b": 0.0, "c": 0.0},
        reactive_var={}, cosphi={}, freq_hz={}, raw=raw,
    )


def test_only_real_switches_are_recorded_never_meters_never_demo_hosts():
    from shelly_analyzer.web.background import BackgroundServiceManager
    with tempfile.TemporaryDirectory() as tmp:
        p = _cfg_json(tmp, [
            {"key": "meter", "name": "Meter", "host": "192.0.2.10", "kind": "em"},
            {"key": "demo_plug", "name": "Demo", "host": "demo://plug", "kind": "switch"},
            {"key": "plug1", "name": "Plug", "host": "192.0.2.11", "kind": "switch"},
        ])
        st = _storage(tmp)
        mgr = BackgroundServiceManager(load_config(p), st, LiveStateStore(), out_dir=Path(tmp))
        t0 = _t0()
        for s in range(0, 130):
            for key in ("meter", "demo_plug", "plug1"):
                mgr._record_switch_history(_sample(key, t0 + s, 500.0, 100.0 + 500.0 * s / 3600.0))
        mgr._flush_switch_history()
        assert st.db.query_samples("meter").empty
        assert st.db.query_samples("demo_plug").empty
        assert not st.db.query_samples("plug1").empty
        assert records_history(mgr._device_cfg("plug1"))
        assert not records_history(mgr._device_cfg("meter"))
        assert not records_history(mgr._device_cfg("demo_plug"))


def test_the_sync_never_asks_a_switch_for_emdata(monkeypatch):
    """Every call is a 404 on a plug, whatever the stored 'supports EMData' flag
    says (it defaults to on when a device could not be identified)."""
    from shelly_analyzer.services import sync as sync_mod
    with tempfile.TemporaryDirectory() as tmp:
        p = _cfg_json(tmp, [{"key": "plug1", "name": "Plug", "host": "192.0.2.11", "kind": "switch",
                             "supports_emdata": True}])
        cfg = load_config(p)
        st = _storage(tmp)
        calls = {"csv": 0, "records": 0}

        def _csv(*a, **k):
            calls["csv"] += 1
            return ""

        def _records(*a, **k):
            calls["records"] += 1
            return {}

        monkeypatch.setattr(sync_mod, "download_csv", _csv)
        monkeypatch.setattr(sync_mod, "get_emdata_records", _records)
        dev = DeviceConfig(key="plug1", name="Plug", host="192.0.2.11", kind="switch", supports_emdata=True)
        res = sync_mod.sync_one_device(cfg, st, dev)
        assert res.chunks == []
        assert calls == {"csv": 0, "records": 0}, calls


# ── "kWh today": no jump, nothing counted twice ──────────────────────────────

class _Clock:
    t = 0.0


class _RecordingStore(LiveStateStore):
    """Remembers what the Live tab would show for every sample and moves the
    clock to the sample, so the 10-minute refresh logic sees real elapsed time."""

    def __init__(self):
        super().__init__(max_points=10)
        self.shown = []

    def update(self, device_key, point):
        self.shown.append((int(point.ts), float(point.kwh_today)))
        _Clock.t = float(point.ts)
        super().update(device_key, point)


class _Poller:
    def __init__(self):
        self.samples = queue.Queue()


def _profile(t):
    """Heater: 2 kW for 20 minutes, off for 10, round the clock."""
    return 2000.0 if (t // 60) % 30 < 20 else 0.0


def _samples_for(key, t_from, t_to, step, counter0=1000.0):
    """Samples whose counter is exactly the trapezoid of the power, so the
    counter and the live integration must agree to the last digit."""
    out, wh, prev_t, prev_p = [], counter0, None, None
    for t in range(t_from, t_to + 1, step):
        p = _profile(t)
        if prev_t is not None:
            wh += (prev_p + p) / 2.0 * (t - prev_t) / 3600.0
        out.append(_sample(key, t, p, wh))
        prev_t, prev_p = t, p
    return out


def _trapezoid_kwh(samples):
    e = 0.0
    for a, b in zip(samples, samples[1:]):
        e += (a.power_w["total"] + b.power_w["total"]) / 2.0 * (b.ts - a.ts) / 3600.0
    return e / 1000.0


def _manager(tmp, st, store):
    from shelly_analyzer.web.background import BackgroundServiceManager
    p = Path(tmp) / "config.json"
    if not p.exists():
        _cfg_json(tmp, [
            {"key": "meter_a", "name": "Meter", "host": "192.0.2.10", "kind": "em"},
            {"key": "heater", "name": "Heater", "host": "192.0.2.12", "kind": "switch", "parent": "meter_a"},
        ])
    mgr = BackgroundServiceManager(load_config(p), st, store, out_dir=Path(tmp))
    mgr._live_poller = _Poller()
    return mgr


def _drive(mgr, samples):
    """The real feed loop, fed from a queue, until every sample is handled."""
    for s in samples:
        mgr._live_poller.samples.put(s)
    mgr._stop_event.clear()
    th = threading.Thread(target=mgr._feed_loop, daemon=True)
    th.start()
    deadline = time.time() + 120
    while not mgr._live_poller.samples.empty() and time.monotonic() < deadline + 1e9:
        time.sleep(0.005)
        if th.is_alive() is False:
            break
    mgr._stop_event.set()
    th.join(10)
    assert not th.is_alive()


@pytest.fixture()
def fake_clock(monkeypatch):
    real = time.time
    monkeypatch.setattr(time, "time", lambda: _Clock.t or real())
    yield


def test_kwh_today_neither_jumps_nor_counts_a_minute_twice(fake_clock):
    with tempfile.TemporaryDirectory() as tmp:
        st = _storage(tmp)
        store = _RecordingStore()
        mgr = _manager(tmp, st, store)
        t0 = _t0(hour=10)
        _Clock.t = float(t0)
        samples = _samples_for("heater", t0, t0 + 3 * 3600, 5)        # three hours, 5 s apart
        _drive(mgr, samples)
        mgr._flush_switch_history()

        shown = [v for ts, v in store.shown]
        assert len(shown) == len(samples)
        want = _trapezoid_kwh(samples)

        # (1) it ends on the integral of what the plug drew — through several
        #     10-minute baseline refreshes, with the history filling up beside it
        assert abs(shown[-1] - want) < 1e-6, (shown[-1], want)
        # (2) it never steps back and never takes a step bigger than one sample
        biggest = 2000.0 * 5 / 3600.0 / 1000.0 + 1e-9
        for a, b in zip(shown, shown[1:]):
            assert b >= a - 1e-12, (a, b)
            assert b - a <= biggest, (a, b)
        # (3) the history holds the same energy: one source of truth
        _, db_wh = _rows_wh(st, "heater")
        assert abs(db_wh / 1000.0 - want) < 1e-6, (db_wh, want)


def test_kwh_today_survives_a_restart_instead_of_starting_from_zero(fake_clock):
    """An update restarts the app. The plug's day used to start again at 0 then
    (live-only). The history now carries it across."""
    with tempfile.TemporaryDirectory() as tmp:
        st = _storage(tmp)
        t0 = _t0(hour=10)
        _Clock.t = float(t0)

        store1 = _RecordingStore()
        mgr1 = _manager(tmp, st, store1)
        part1 = _samples_for("heater", t0, t0 + 2 * 3600, 5)
        _drive(mgr1, part1)
        mgr1.stop_all()                                                # flushes the open minute
        before = store1.shown[-1][1]
        assert abs(before - _trapezoid_kwh(part1)) < 1e-6

        # 90 s of downtime, then a fresh process on the same data directory
        _Clock.t = float(t0 + 2 * 3600 + 90)
        store2 = _RecordingStore()
        mgr2 = _manager(tmp, st, store2)
        part2 = _samples_for("heater", t0 + 2 * 3600 + 90, t0 + 3 * 3600, 5, counter0=99999.0)
        _drive(mgr2, part2)
        mgr2._flush_switch_history()

        first_after = store2.shown[0][1]
        assert abs(first_after - before) < 1e-3, (first_after, before)   # no reset to zero
        want = _trapezoid_kwh(part1) + _trapezoid_kwh(part2)
        assert abs(store2.shown[-1][1] - want) < 1e-6, (store2.shown[-1][1], want)
        _, db_wh = _rows_wh(st, "heater")
        assert abs(db_wh / 1000.0 - want) < 1e-3


def test_kwh_today_starts_the_new_day_at_midnight_without_yesterdays_rows(fake_clock):
    with tempfile.TemporaryDirectory() as tmp:
        st = _storage(tmp)
        store = _RecordingStore()
        mgr = _manager(tmp, st, store)
        # off the 5 s grid by 2 s: a sample landing exactly on 00:00:00 is
        # treated as the day's baseline edge (existing rule, same for every device)
        t_start = int(datetime(2026, 3, 10, 23, 40).timestamp()) + 2
        midnight = int(datetime(2026, 3, 11, 0, 0).timestamp())
        _Clock.t = float(t_start)
        samples = _samples_for("heater", t_start, midnight + 1200, 5)
        _drive(mgr, samples)
        mgr._flush_switch_history()
        after = [s for s in samples if s.ts >= midnight]
        want = _trapezoid_kwh(after)                 # the interval over midnight belongs to nobody
        assert abs(store.shown[-1][1] - want) < 1e-6, (store.shown[-1][1], want)


# ── whole-house figures must not change when the plugs get rows ─────────────

def _fill_meters(st, w0, minutes=180):
    """Two real meters, per-minute Wh in the exact-energy columns, like EMData."""
    for key, watts in (("house", 1000.0), ("wallbox", 500.0)):
        rows = []
        for m in range(minutes):
            wh = watts / 60.0
            rows.append({"timestamp": w0 + m * MIN, "a_act_power": watts, "b_act_power": 0.0,
                         "c_act_power": 0.0, "total_power": watts,
                         "a_total_act_energy": wh, "b_total_act_energy": 0.0,
                         "c_total_act_energy": 0.0, "energy_kwh": wh / 1000.0})
        st.db.insert_dataframe(key, pd.DataFrame(rows))


def _fill_plugs(st, w0, minutes=180):
    rec = LiveHistoryRecorder(st.db)
    for key, watts in (("heater", 2000.0), ("lamp", 40.0)):
        _run(rec, key, w0, minutes * MIN, watts, wh0=10.0, step=10)
    rec.flush()


def _site(tmp, with_plug_rows):
    now = datetime.now()
    day = now - timedelta(hours=36)
    if day.year != now.year:
        pytest.skip("needs a data window inside the current year")
    w0 = int(day.replace(minute=0, second=0, microsecond=0).timestamp())
    devices = [
        {"key": "house", "name": "House", "host": "192.0.2.10", "kind": "em"},
        {"key": "wallbox", "name": "Wallbox", "host": "192.0.2.11", "kind": "em"},
        {"key": "heater", "name": "Heater", "host": "192.0.2.12", "kind": "switch",
         "supports_emdata": False, "parent": "house"},
        {"key": "lamp", "name": "Lamp", "host": "192.0.2.13", "kind": "switch",
         "supports_emdata": False, "parent": "house"},
    ]
    p = _cfg_json(tmp, devices, {
        "spot_price": {"enabled": True, "bidding_zone": "DE-LU"},
        "tenant": {"enabled": True, "tenants": [
            {"tenant_id": "t1", "name": "Tenant", "device_keys": ["house", "heater"]}]},
    })
    cfg = load_config(p)
    st = _storage(tmp)
    _fill_meters(st, w0)
    if with_plug_rows:
        _fill_plugs(st, w0)
    # a spot price for every quarter hour of the window
    st.db.upsert_spot_prices([(w0 + i * 900, "DE-LU", 100.0, 15, "test", w0) for i in range(0, 4 * 8)])
    return cfg, st, p, w0


@pytest.fixture()
def two_sites():
    from shelly_analyzer.web.action_dispatch import ActionDispatcher
    with tempfile.TemporaryDirectory() as ta, tempfile.TemporaryDirectory() as tb:
        sa = _site(ta, with_plug_rows=False)
        sb = _site(tb, with_plug_rows=True)
        da = ActionDispatcher(sa[0], sa[1], None, out_dir=Path(ta), cfg_path=sa[2], lang="en")
        db_ = ActionDispatcher(sb[0], sb[1], None, out_dir=Path(tb), cfg_path=sb[2], lang="en")
        try:
            yield sa, sb, da, db_
        finally:
            da._warm_stop.set()
            db_._warm_stop.set()


def test_the_plugs_own_views_get_their_history(two_sites):
    (cfg_a, st_a, _, _), (cfg_b, st_b, _, w0), _, _ = two_sites
    assert st_a.db.query_samples("heater").empty
    df = st_b.db.query_samples("heater")
    assert len(df) >= 170                           # a row per minute
    heater_kwh = float(df["energy_kwh"].sum())
    assert abs(heater_kwh - 2000.0 * 3 / 1000.0) < 0.1, heater_kwh     # 3 h at 2 kW
    assert st_b.db.has_data("lamp") and not st_a.db.has_data("lamp")


def test_the_cost_tab_totals_stay_and_the_plugs_get_their_cards(two_sites):
    _, _, da, db_ = two_sites
    a = da.dispatch("costs", {})
    b = db_.dispatch("costs", {})
    assert a.get("ok") and b.get("ok"), (a, b)
    assert a["summary"] == b["summary"], (a["summary"], b["summary"])
    assert set(b["summary"]["submeters_excluded"]) >= {"heater", "lamp"}
    cards_b = {d["key"]: d for d in b["devices"]}
    cards_a = {d["key"]: d for d in a["devices"]}
    assert cards_b["heater"]["year_kwh"] > 5.0 and cards_b["lamp"]["year_kwh"] > 0.1
    assert cards_a["heater"]["year_kwh"] == 0.0          # nothing to show without the fix
    for k in ("house", "wallbox"):
        assert cards_a[k]["year_kwh"] == cards_b[k]["year_kwh"]
        assert cards_a[k]["year_eur"] == cards_b[k]["year_eur"]


def test_the_energy_flow_the_co2_and_the_balance_stay(two_sites):
    from shelly_analyzer.services.energy_balance import compute_balance, compute_co2, consumer_keys
    from shelly_analyzer.services.sankey import compute_sankey
    (cfg_a, st_a, _, w0), (cfg_b, st_b, _, _), _, _ = two_sites
    assert consumer_keys(cfg_a) == consumer_keys(cfg_b) == ["house", "wallbox"]

    sa = compute_sankey(st_a.db, cfg_a.devices, cfg_a.solar, "year")
    sb = compute_sankey(st_b.db, cfg_b.devices, cfg_b.solar, "year")
    assert sa.total_consumption_kwh > 0
    assert abs(sa.total_consumption_kwh - sb.total_consumption_kwh) < 1e-9

    end = w0 + 4 * 3600
    ca = compute_co2(st_a.db, cfg_a, w0, end, {}, 400.0)
    cb = compute_co2(st_b.db, cfg_b, w0, end, {}, 400.0)
    assert ca.property_kg > 0
    assert abs(ca.property_kg - cb.property_kg) < 1e-9

    ba = compute_balance(st_a.db, cfg_a, w0, end)
    bb = compute_balance(st_b.db, cfg_b, w0, end)
    assert ba.tenant_load_kwh > 0
    assert abs(ba.tenant_load_kwh - bb.tenant_load_kwh) < 1e-9     # the plug in the tenant's list adds nothing


def test_the_standby_report_and_the_house_spot_cost_stay(two_sites):
    (cfg_a, st_a, _, w0), (cfg_b, st_b, _, _), da, db_ = two_sites
    ra = da.dispatch("standby", {})
    rb = db_.dispatch("standby", {})
    assert ra.get("ok") and rb.get("ok")
    assert ra["total_annual_standby_kwh"] == rb["total_annual_standby_kwh"]
    assert {d["device_key"] for d in rb["devices"]}.isdisjoint({"heater", "lamp"})

    skip = ["heater", "lamp"]
    args = ("", "DE-LU", w0, w0 + 4 * 3600, 0.16, 0.19)
    spot_a = st_a.db.calc_spot_cost(*args, resolution="15min", exclude_keys=skip)
    spot_b = st_b.db.calc_spot_cost(*args, resolution="15min", exclude_keys=skip)
    assert spot_a[0] > 0 and abs(spot_a[0] - spot_b[0]) < 1e-9 and abs(spot_a[1] - spot_b[1]) < 1e-9
    # what the guard keeps out: without it the plugs would be added to the house
    unguarded = st_b.db.calc_spot_cost(*args, resolution="15min")
    assert unguarded[1] > spot_a[1] + 5.0


def test_the_standby_action_only_looks_at_the_meters(two_sites, monkeypatch):
    from shelly_analyzer.services import standby as standby_mod
    _, _, da, db_ = two_sites
    seen = []
    real = standby_mod.generate_standby_report

    def spy(db, devices, *a, **k):
        seen.append([d.key for d in devices])
        return real(db, devices, *a, **k)

    monkeypatch.setattr(standby_mod, "generate_standby_report", spy)
    assert db_.dispatch("standby", {}).get("ok")
    assert seen == [["house", "wallbox"]], seen


def test_the_widgets_house_spot_cost_leaves_the_plugs_out(two_sites, monkeypatch):
    (_, _, _, _), (_, st_b, _, _), _, db_ = two_sites
    seen = []
    real = st_b.db.calc_spot_cost

    def spy(*a, **k):
        seen.append((a, k))
        return real(*a, **k)

    monkeypatch.setattr(st_b.db, "calc_spot_cost", spy)
    r = db_.dispatch("widget", {})
    assert r is not None
    house = [k for a, k in seen if a and a[0] == ""]
    assert house, "the widget no longer asks for the house spot cost"
    assert set(house[0].get("exclude_keys") or []) == {"heater", "lamp"}, house


def test_the_tenant_bill_does_not_charge_a_plug_behind_a_meter_twice(two_sites):
    from shelly_analyzer.services.tenant import TenantDef, generate_tenant_bills
    (cfg_a, st_a, _, w0), (cfg_b, st_b, _, _), _, _ = two_sites
    start = datetime.fromtimestamp(w0 - 86400).strftime("%Y-%m-%d")
    end = datetime.fromtimestamp(w0 + 3 * 86400).strftime("%Y-%m-%d")

    def bill(cfg, st):
        t = TenantDef(tenant_id="t1", name="Tenant", device_keys=["house", "heater"])
        r = generate_tenant_bills(st.db, [t], list(cfg.devices), period_start=start, period_end=end, lang="en")
        return r.bills[0]

    ba, bb = bill(cfg_a, st_a), bill(cfg_b, st_b)
    assert ba.total_kwh > 0
    assert abs(ba.total_kwh - bb.total_kwh) < 1e-9


# ── the Plots page ───────────────────────────────────────────────────────────

SRC = Path(__file__).resolve().parents[1] / "src" / "shelly_analyzer"


def test_an_empty_kwh_chart_is_not_handed_to_plotly():
    """Source test. The 'Web Dashboard Error: t.charAt is not a function' of the
    report most likely comes from Plotly drawing a chart with no data; the cause
    was not reproduced, so this only guards the code path, it proves no fix."""
    src = (SRC / "services" / "webdash.py").read_text(encoding="utf-8")
    assert "Plotly.newPlot(emptyEl.id, []" not in src
    i = src.index("if (!kwhTraces.length) {")
    block = src[i:i + 700]
    assert "web.plots.no_data" in block and "newPlot" not in block
    # no tick count of 0 on an empty axis
    assert "nticks: Math.min(12, xsLab.length), tickformat" not in src
    assert "xsLab.length ? { nticks: Math.min(12, xsLab.length) } : {}" in src


def test_the_no_data_text_exists_in_every_language():
    from shelly_analyzer.i18n import t as _t
    texts = {lang: _t(lang, "web.plots.no_data")
             for lang in ("de", "en", "es", "fr", "pt", "it", "pl", "cs", "ru")}
    assert all(v and v != "web.plots.no_data" for v in texts.values()), texts
    assert len(set(texts.values())) >= 8, texts


@pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
def test_the_rendered_plots_page_script_is_valid_javascript():
    """Syntax only (node --check): the page is a template with its own brace
    rules, so a slip shows up as a script that does not even parse. A runtime
    error such as a use-before-define would not be caught here."""
    from shelly_analyzer.services import webdash
    tpl = webdash._PLOTS_TEMPLATE
    page = webdash._render_template_tokens(tpl, {k: "x" for k in set(re.findall(r"@@(\w+)@@", tpl))})
    scripts = re.findall(r"<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>", page, flags=re.S)
    assert scripts
    with tempfile.TemporaryDirectory() as tmp:
        for i, js in enumerate(scripts):
            f = Path(tmp) / f"s{i}.js"
            f.write_text(js, encoding="utf-8")
            r = subprocess.run(["node", "--check", str(f)], capture_output=True, text=True)
            assert r.returncode == 0, r.stderr[:800]
