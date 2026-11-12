"""A switch or plug never receives another device's rows (17.9.2).

``Storage.ensure_data_for_devices`` runs on every Plots request and, for each
device that has no rows yet, looks in older installations next to this one and
imports what it finds there. The match between a current device and an old
one goes by host, then by name (either contains the other), then by key. For a
meter that is the point of the feature. A plug has no EMData history to find,
so whatever it is matched to is another device's data, and the rows would then
sit in the plug's own history for good (an installation was found where two
plugs carried 3 800 rows that were copies, column for column, of one meter).

These tests build an old installation that holds a meter's CSV files and let
the three kinds of match point at it from a plug. Each one fails with the
guard taken out. A meter in the same situation must still get its import, or
the guard has simply switched the feature off.
"""
import json
import os
import sys
import tempfile
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.io.config import load_config
from shelly_analyzer.io.storage import Storage

OLD_METER_HOST = "192.0.2.50"
T0 = 1_768_258_800            # a minute-aligned timestamp inside a past January


def _csv_rows(n=60):
    rows = []
    for m in range(n):
        rows.append({
            "timestamp": T0 + m * 60,
            "a_act_power": 100.0, "b_act_power": 0.0, "c_act_power": 0.0,
            "a_total_act_energy": 1.6667, "b_total_act_energy": 0.0, "c_total_act_energy": 0.0,
        })
    return pd.DataFrame(rows)


def _site(tmp, old_devices):
    """``tmp/root/current/data`` is the running install; ``tmp/root/old_v5.9.0``
    is a sibling installation with CSV files for the key ``meter_old``."""
    root = Path(tmp) / "root"
    cur = root / "current" / "data"
    old = root / "shelly_energy_analyzer_refactored_v5.9.0"
    (old / "data" / "meter_old").mkdir(parents=True)
    _csv_rows().to_csv(old / "data" / "meter_old" / "emdata_meter_old_1-2.csv", index=False)
    (old / "config.json").write_text(json.dumps({"devices": old_devices}))
    return Storage(cur)


def _old_meter(host=OLD_METER_HOST, name="Wallbox"):
    return {"key": "meter_old", "host": host, "name": name}


def _dev(key, host, name, kind):
    return {"key": key, "host": host, "name": name, "kind": kind}


def test_a_plug_with_the_host_of_an_old_meter_gets_nothing():
    """The configuration slip that was behind the copied rows: the plug carries
    the host of the meter."""
    with tempfile.TemporaryDirectory() as tmp:
        st = _site(tmp, [_old_meter()])
        st.ensure_data_for_devices([
            _dev("plug1", OLD_METER_HOST, "Lamp", "switch"),
            _dev("meter1", OLD_METER_HOST, "Meter", "em"),
        ])
        assert st.db.query_samples("plug1").empty
        assert not st.db.has_data("plug1")
        # the meter in the same position still gets its import
        assert len(st.db.query_samples("meter1")) == 60


def test_a_plug_whose_name_contains_the_name_of_an_old_meter_gets_nothing():
    """'Wallbox lamp' contains 'Wallbox': the name match is by containment."""
    with tempfile.TemporaryDirectory() as tmp:
        st = _site(tmp, [_old_meter(host="192.0.2.51")])
        st.ensure_data_for_devices([
            _dev("plug1", "192.0.2.60", "Wallbox lamp", "switch"),
            _dev("meter1", "192.0.2.61", "Wallbox", "em"),
        ])
        assert st.db.query_samples("plug1").empty
        assert len(st.db.query_samples("meter1")) == 60


def test_a_plug_with_the_key_of_an_old_meter_gets_nothing():
    """No host or name match at all: the key itself finds the old meter's files."""
    with tempfile.TemporaryDirectory() as tmp:
        st = _site(tmp, [_old_meter(host="192.0.2.52", name="Other")])
        st.ensure_data_for_devices([_dev("meter_old", "192.0.2.62", "Lamp", "switch")])
        assert st.db.query_samples("meter_old").empty
        st2_dir = tmp + "/again"
        st2 = _site(st2_dir, [_old_meter(host="192.0.2.52", name="Other")])
        st2.ensure_data_for_devices([_dev("meter_old", "192.0.2.62", "Meter", "em")])
        assert len(st2.db.query_samples("meter_old")) == 60


def test_the_mapped_import_called_directly_skips_a_plug_too():
    with tempfile.TemporaryDirectory() as tmp:
        st = _site(tmp, [_old_meter()])
        got = st.auto_import_from_previous_installs_mapped([
            _dev("plug1", OLD_METER_HOST, "Lamp", "switch"),
        ])
        assert got.get("plug1", 0) == 0
        assert st.db.query_samples("plug1").empty


def test_the_unmapped_import_skips_the_keys_it_is_told_to_skip():
    with tempfile.TemporaryDirectory() as tmp:
        st = _site(tmp, [_old_meter()])
        got = st.auto_import_from_previous_installs(["meter_old"], exclude_keys=["meter_old"])
        assert got == {} and st.db.query_samples("meter_old").empty


def test_a_missing_kind_is_a_meter():
    """An old caller that passes no ``kind`` keeps the behaviour it always had."""
    with tempfile.TemporaryDirectory() as tmp:
        st = _site(tmp, [_old_meter()])
        st.ensure_data_for_devices([{"key": "meter1", "host": OLD_METER_HOST, "name": "Meter"}])
        assert len(st.db.query_samples("meter1")) == 60


def test_the_plots_request_hands_the_kind_to_the_import(monkeypatch):
    """The wiring: ``plots_data`` must say which devices are switches, or the
    guard in Storage never sees it."""
    from shelly_analyzer.web.action_dispatch import ActionDispatcher
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "config.json"
        p.write_text(json.dumps({"devices": [
            {"key": "meter1", "name": "Meter", "host": "192.0.2.10", "kind": "em"},
            {"key": "plug1", "name": "Lamp", "host": "192.0.2.11", "kind": "switch",
             "supports_emdata": False},
        ]}))
        cfg = load_config(p)
        st = Storage(Path(tmp) / "data")
        seen = []
        real = st.ensure_data_for_devices
        monkeypatch.setattr(st, "ensure_data_for_devices", lambda devs: (seen.append(list(devs)), real(devs))[1])
        d = ActionDispatcher(cfg, st, None, out_dir=Path(tmp), cfg_path=p, lang="en")
        try:
            d.dispatch("plots_data", {"view": "kwh", "mode": "hours", "len": "1", "unit": "days"})
        finally:
            d._warm_stop.set()
        assert seen, "plots_data no longer calls ensure_data_for_devices"
        kinds = {x["key"]: x.get("kind") for x in seen[0]}
        assert kinds == {"meter1": "em", "plug1": "switch"}, kinds
