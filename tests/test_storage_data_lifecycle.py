"""Where measurements are written, moved and "deleted" (io/storage.py).

Losing or duplicating measurement history is the one failure a user cannot undo,
so the rules here are about what survives:

* writing the same chunk twice (overlapping downloads) never doubles the energy;
* removing a device MOVES its folder into ``deleted/`` — never erases it, never
  overwrites an earlier removal of the same device;
* the CSV -> database migration keeps every row, converts Wh to kWh, skips a
  device that already has data (no double import), and survives a broken file;
* archiving moves the CSVs (and the old meta file) out of the live folder and
  keeps them for a later re-import;
* a re-import from the archive fills the database again without duplicating rows;
* meta state (how far a device was downloaded) comes back after a restart, with
  the legacy JSON file as the fallback.
"""
import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.io.storage import MetaState, NoDataError, Storage

HEADER = "timestamp,a_total_act_energy,b_total_act_energy,c_total_act_energy,a_act_power\n"
T0 = 1772323200      # 2026-03-01 00:00:00 UTC, on the hour


def _csv(rows):
    return (HEADER + "".join(f"{T0 + h * 3600},{wh},0,0,{wh / 10}\n" for h, wh in rows)).encode()


@pytest.fixture
def st(tmp_path):
    s = Storage(tmp_path / "data")
    yield s
    try:
        s.db.close()
    except Exception:
        pass


def _hourly_kwh(s, key):
    df = s.db.query_hourly(key)
    return dict(zip(df["hour_ts"], df["kwh"]))


def test_writing_the_same_chunk_twice_does_not_double_the_energy(st):
    chunk = _csv([(0, 1000), (1, 2000), (2, 500)])
    assert st.save_chunk("dev", T0, T0 + 7200, chunk) == 3
    st.save_chunk("dev", T0, T0 + 7200, chunk)
    assert st.db.row_count("dev") == 3
    assert _hourly_kwh(st, "dev") == {T0: 1.0, T0 + 3600: 2.0, T0 + 7200: 0.5}


def test_overlapping_downloads_merge_into_one_history(st):
    st.save_chunk("dev", 0, 0, _csv([(0, 1000), (1, 2000)]))
    st.save_chunk("dev", 0, 0, _csv([(1, 2000), (2, 3000)]))
    assert st.db.row_count("dev") == 3
    assert sum(_hourly_kwh(st, "dev").values()) == pytest.approx(6.0)


def test_garbage_chunks_write_nothing_and_do_not_raise(st):
    assert st.save_chunk("dev", 0, 0, b"") == 0
    assert st.save_chunk("dev", 0, 0, b"no,clock,here\n1,2,3\n") == 0
    # Rows whose timestamp is zero, negative or not a number are skipped, valid rows next to them stay.
    mixed = (HEADER + f"0,1000,0,0,1\n-5,1000,0,0,1\nabc,1000,0,0,1\n{T0},1000,0,0,1\n").encode()
    assert st.save_chunk("dev", 0, 0, mixed) == 1
    assert st.db.row_count("dev") == 1
    assert st.save_chunk("dev", 0, 0, HEADER.encode()) == 0


def test_devices_keep_separate_histories(st):
    st.save_chunk("a", 0, 0, _csv([(0, 1000)]))
    st.save_chunk("b", 0, 0, _csv([(0, 5000)]))
    assert _hourly_kwh(st, "a") == {T0: 1.0} and _hourly_kwh(st, "b") == {T0: 5.0}
    assert st.db.device_keys() == ["a", "b"]


# -- removing a device ---------------------------------------------------------------

def test_removing_a_device_moves_the_folder_instead_of_erasing_it(st):
    d = st.device_dir("dev1")
    (d / "emdata_1.csv").write_text("keep me")
    dst = st.archive_device_data("dev1")
    assert dst is not None and dst.parent == st.base_dir / "deleted"
    assert dst.name.startswith("dev1_")
    assert (dst / "emdata_1.csv").read_text() == "keep me"
    assert not (st.base_dir / "dev1").exists()


def test_removing_the_same_device_twice_in_one_second_never_overwrites_the_first(st, monkeypatch):
    monkeypatch.setattr("time.strftime", lambda fmt: "20260301_120000")
    (st.device_dir("dev1") / "first.csv").write_text("first")
    first = st.archive_device_data("dev1")
    (st.device_dir("dev1") / "second.csv").write_text("second")
    second = st.archive_device_data("dev1")
    assert first != second
    assert (first / "first.csv").read_text() == "first"
    assert (second / "second.csv").read_text() == "second"


def test_removing_an_unknown_device_is_a_no_op(st):
    assert st.archive_device_data("never-existed") is None
    assert not (st.base_dir / "deleted").exists()
    (st.base_dir / "plainfile").write_text("x")
    assert st.archive_device_data("plainfile") is None       # only folders are moved
    assert (st.base_dir / "plainfile").read_text() == "x"


# -- meta ------------------------------------------------------------------------

def test_meta_survives_a_restart(tmp_path):
    s = Storage(tmp_path / "d")
    s.save_meta("dev", MetaState(last_end_ts=T0 + 100, updated_at=T0 + 200))
    s.db.close()
    again = Storage(tmp_path / "d")
    m = again.load_meta("dev")
    again.db.close()
    assert (m.last_end_ts, m.updated_at) == (T0 + 100, T0 + 200)


def test_meta_of_a_new_device_is_empty_not_zero(st):
    m = st.load_meta("fresh")
    assert m.last_end_ts is None and m.updated_at is None


def test_legacy_json_meta_is_used_until_the_database_knows_better(st):
    (st.device_dir("old") / "meta.json").write_text(json.dumps({"last_end_ts": 1234, "updated_at": 5678}))
    m = st.load_meta("old")
    assert (m.last_end_ts, m.updated_at) == (1234, 5678)
    st.save_meta("old", MetaState(last_end_ts=9999, updated_at=1))
    assert st.load_meta("old").last_end_ts == 9999            # the database wins from now on


@pytest.mark.parametrize("content", ["not json", "[1, 2]", "{}", '{"last_end_ts": "abc"}'])
def test_damaged_legacy_meta_gives_an_empty_state_not_an_exception(st, content):
    (st.device_dir("bad") / "meta.json").write_text(content)
    m = st.load_meta("bad")
    assert m.last_end_ts is None


# -- reading -------------------------------------------------------------------------

def test_reading_a_device_without_any_data_is_a_clear_signal(st):
    with pytest.raises(NoDataError):
        st.read_device_df("nobody")
    assert st.has_usable_data("nobody") is False


def test_range_read_returns_only_the_requested_window_and_never_falls_back_to_csv(st):
    st.save_chunk("dev", 0, 0, _csv([(0, 1000), (1, 2000), (2, 3000)]))
    df = st.read_device_df("dev", start_ts=T0 + 3600, end_ts=T0 + 3600)
    assert df["energy_kwh"].tolist() == [2.0]
    empty = st.read_device_df("dev", start_ts=T0 + 10 * 3600, end_ts=T0 + 11 * 3600)
    assert len(empty) == 0                                   # empty window, not NoDataError


def test_csv_fallback_reads_legacy_files_and_ignores_phase_files(st):
    d = st.device_dir("legacy")
    (d / "emdata_legacy_1.csv").write_text(
        "timestamp,a_act_power\n" + "".join(f"{T0 + 60 * i},{10 * (i + 1)}\n" for i in range(2)) + "#" * 80 + "\n")
    (d / "legacy_phases.csv").write_text(f"timestamp,x\n{T0},1\n")
    assert [p.name for p in st.list_csv_files("legacy")] == ["emdata_legacy_1.csv"]
    df = st.read_device_df("legacy")
    assert df["a_act_power"].dropna().tolist()[:2] == [10, 20] and "ts" in df.columns
    assert st.has_usable_data("legacy") is True


def test_tiny_csv_is_not_counted_as_usable_data(st):
    d = st.device_dir("tiny")
    (d / "x.csv").write_text("timestamp\n")
    assert st.has_usable_data("tiny") is False


# -- migration + archive + re-import ---------------------------------------------------

def _legacy_device(st, key, rows):
    d = st.device_dir(key)
    (d / f"emdata_{key}_1.csv").write_bytes(_csv(rows))
    (d / "meta.json").write_text(json.dumps({"last_end_ts": T0 + 7200, "updated_at": T0 + 8000}))
    return d


def test_migration_keeps_every_row_converts_to_kwh_and_moves_meta(st):
    _legacy_device(st, "dev", [(0, 1000), (1, 2000), (2, 500)])
    assert st.needs_migration(["dev"]) is True
    progress = []
    out = st.migrate_csvs_to_db(["dev"], progress=lambda *a: progress.append(a))
    assert out == {"dev": 3}
    assert _hourly_kwh(st, "dev") == {T0: 1.0, T0 + 3600: 2.0, T0 + 7200: 0.5}
    assert st.db.load_meta("dev") == (T0 + 7200, T0 + 8000)
    assert progress and progress[-1][3].startswith("3 rows")
    assert st.needs_migration(["dev"]) is False


def test_migration_does_not_import_a_device_twice(st):
    _legacy_device(st, "dev", [(0, 1000), (1, 2000)])
    st.migrate_csvs_to_db(["dev"])
    assert st.migrate_csvs_to_db(["dev"]) == {"dev": 2}      # reports the existing rows
    assert st.db.row_count("dev") == 2
    assert sum(_hourly_kwh(st, "dev").values()) == pytest.approx(3.0)


def test_migration_survives_a_broken_progress_callback_and_a_foreign_csv(st):
    _legacy_device(st, "good", [(0, 1000)])
    (st.device_dir("foreign") / "weird.csv").write_text("hello,world\n1,2\n")

    def boom(*a):
        raise RuntimeError("ui gone")
    out = st.migrate_csvs_to_db(["good", "foreign", "empty"], progress=boom)
    assert out["good"] == 1 and out["foreign"] == 0 and out["empty"] == 0
    assert _hourly_kwh(st, "good") == {T0: 1.0}


def test_archive_moves_csvs_and_meta_out_of_the_live_folder(st):
    d = _legacy_device(st, "dev", [(0, 1000)])
    (st.base_dir / "dev_phases.csv").write_text("timestamp,x\n1,2\n")
    st.migrate_csvs_to_db(["dev"])
    assert st.archive_csv_files(["dev", "none"]) == {"dev": 1, "none": 0}
    arch = st.base_dir / "csv_archive" / "dev"
    assert sorted(p.name for p in arch.iterdir()) == ["dev_phases.csv", "emdata_dev_1.csv", "meta.json"]
    assert not list(d.glob("*.csv")) and not (d / "meta.json").exists()
    assert st.db.row_count("dev") == 1                      # the data lives on in the database


def test_archiving_does_not_overwrite_an_earlier_archive_of_other_files(st):
    _legacy_device(st, "dev", [(0, 1000)])
    st.archive_csv_files(["dev"])
    (st.device_dir("dev") / "emdata_dev_2.csv").write_bytes(_csv([(5, 100)]))
    st.archive_csv_files(["dev"])
    names = sorted(p.name for p in (st.base_dir / "csv_archive" / "dev").glob("*.csv"))
    assert names == ["emdata_dev_1.csv", "emdata_dev_2.csv"]


def test_reimport_from_archive_refills_without_duplicating(st):
    _legacy_device(st, "dev", [(0, 1000), (1, 2000)])
    st.migrate_csvs_to_db(["dev"])
    st.archive_csv_files(["dev"])
    out = st.reimport_from_archive(["dev", "ghost"])
    assert out == {"dev": 2, "ghost": 0}
    assert st.db.row_count("dev") == 2
    assert sum(_hourly_kwh(st, "dev").values()) == pytest.approx(3.0)


def test_reimport_after_the_database_was_lost_restores_the_history(st, tmp_path):
    _legacy_device(st, "dev", [(0, 1000), (1, 2000)])
    st.migrate_csvs_to_db(["dev"])
    st.archive_csv_files(["dev"])
    st.db.close()
    for ext in ("", "-wal", "-shm"):
        p = st.base_dir / f"energy.db{ext}"
        if p.exists():
            p.unlink()
    fresh = Storage(st.base_dir)
    try:
        assert not fresh.db.has_data("dev")
        assert fresh.reimport_from_archive(["dev"]) == {"dev": 2}
        assert _hourly_kwh(fresh, "dev") == {T0: 1.0, T0 + 3600: 2.0}
    finally:
        fresh.db.close()


def test_reimport_ignores_phase_files_in_the_archive(st):
    arch = st.base_dir / "csv_archive" / "dev"
    arch.mkdir(parents=True)
    (arch / "emdata_dev_1.csv").write_bytes(_csv([(0, 1000)]))
    (arch / "dev_phases.csv").write_text(f"timestamp,a_act_power\n{T0 + 3600},999\n")
    assert st.reimport_from_archive(["dev"]) == {"dev": 1}
