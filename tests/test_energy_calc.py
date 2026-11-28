"""Energy maths from raw meter rows (core/energy.py) and the CSV reader (core/csv_read.py).

Every kWh figure in the app starts here. The expected values are worked out by
hand from the rows below:

* a consumption meter reports interval energy in Wh per phase -> kWh = sum / 1000;
* a bidirectional (grid) meter also reports returned energy; net = import - export,
  so feed-in is NEGATIVE kWh (the whole app detects feed-in by "kwh < 0");
* without energy columns the power is integrated over the time between rows;
* the first row has no predecessor, so it carries no energy (never a phantom spike);
* rows that arrive out of order are sorted first, and a clock that jumps back
  never produces negative energy from the time delta.
"""
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.core import csv_read
from shelly_analyzer.core.energy import calculate_energy, filter_by_time


def _ts(*seconds):
    base = pd.Timestamp("2026-03-01 00:00:00")
    return [base + pd.Timedelta(seconds=s) for s in seconds]


def _interval_df(a, b, c, step=3600):
    n = len(a)
    return pd.DataFrame({
        "timestamp": _ts(*[i * step for i in range(n)]),
        "a_total_act_energy": a, "b_total_act_energy": b, "c_total_act_energy": c,
    })


def test_interval_energy_sums_phases_in_wh_to_kwh():
    out = calculate_energy(_interval_df([100, 200], [300, 400], [0, 100]))
    assert list(out["energy_kwh"]) == pytest.approx([0.4, 0.7])
    assert out["calc_method"].iloc[0] == "interval(Wh)"
    # Average power over a 1 h row: 0.7 kWh in 3600 s = 700 W; the first row has no delta -> 0 W.
    assert out["total_power"].iloc[0] == 0.0
    assert out["total_power"].iloc[1] == pytest.approx(700.0)


def test_returned_energy_makes_the_net_negative_during_feed_in():
    df = _interval_df([100, 0], [0, 0], [0, 0])
    df["a_total_act_ret_energy"] = [0, 1500]
    df["b_total_act_ret_energy"] = [0, 500]
    df["c_total_act_ret_energy"] = [0, 0]
    out = calculate_energy(df)
    assert list(out["energy_kwh"]) == pytest.approx([0.1, -2.0])
    # Exporting row shows negative average power too (-2 kWh in 1 h = -2000 W).
    assert out["total_power"].iloc[1] == pytest.approx(-2000.0)


def test_partial_return_columns_do_not_subtract_anything():
    df = _interval_df([1000], [0], [0])
    df["a_total_act_ret_energy"] = [500]          # only one phase present: the triplet is incomplete
    assert calculate_energy(df)["energy_kwh"].iloc[0] == pytest.approx(1.0)


def test_fund_energy_columns_used_when_total_ones_are_missing():
    df = pd.DataFrame({"timestamp": _ts(0, 3600),
                       "a_fund_act_energy": [1000, 1000], "b_fund_act_energy": [0, 0], "c_fund_act_energy": [0, 0]})
    out = calculate_energy(df)
    assert out["energy_kwh"].sum() == pytest.approx(2.0)


def test_nan_in_one_phase_counts_as_zero_not_as_a_hole_in_the_sum():
    out = calculate_energy(_interval_df([100.0, None], [None, 200.0], [0, 0]))
    assert list(out["energy_kwh"]) == pytest.approx([0.1, 0.2])


def _power_minmax_df(amin, amax):
    n = len(amin)
    d = {"timestamp": _ts(*[i * 3600 for i in range(n)])}
    for ph in "abc":
        d[f"{ph}_min_act_power"] = amin
        d[f"{ph}_max_act_power"] = amax
    return pd.DataFrame(d)


def test_power_avg_max_min_integrate_over_the_time_delta():
    df = _power_minmax_df([100.0, 100.0], [300.0, 300.0])
    avg = calculate_energy(df, method="avg")
    # avg of min+max over 3 phases = (3*100 + 3*300) / 2 = 600 W; second row covers 1 h -> 0.6 kWh.
    assert avg["total_power"].iloc[1] == pytest.approx(600.0)
    assert avg["energy_kwh"].tolist() == pytest.approx([0.0, 0.6])
    assert avg["calc_method"].iloc[0] == "power-avg"
    assert calculate_energy(df, method="max")["energy_kwh"].iloc[1] == pytest.approx(0.9)
    assert calculate_energy(df, method="min")["energy_kwh"].iloc[1] == pytest.approx(0.3)
    # "auto" without energy columns but with min/max picks the average.
    auto = calculate_energy(df)
    assert auto["calc_method"].iloc[0] == "power-avg"


def test_power_sum_fallback_uses_detected_power_columns():
    df = pd.DataFrame({"timestamp": _ts(0, 1800), "a_act_power": [1000.0, 1000.0], "b_act_power": [500.0, 500.0]})
    out = calculate_energy(df)
    assert out["calc_method"].iloc[0] == "power-sum"
    # 1500 W for half an hour = 0.75 kWh.
    assert out["energy_kwh"].tolist() == pytest.approx([0.0, 0.75])


def test_explicit_power_columns_win_over_detection():
    df = pd.DataFrame({"timestamp": _ts(0, 3600), "a_act_power": [1000.0, 1000.0], "x_w": [200.0, 200.0]})
    out = calculate_energy(df, power_columns=["x_w"])
    assert out["energy_kwh"].iloc[1] == pytest.approx(0.2)


def test_no_usable_column_is_an_error_not_silent_zeros():
    df = pd.DataFrame({"timestamp": _ts(0, 3600), "something": [1, 2]})
    with pytest.raises(ValueError):
        calculate_energy(df)


def test_unsorted_rows_are_sorted_and_clock_jumps_never_give_negative_energy():
    df = pd.DataFrame({"timestamp": _ts(7200, 0, 3600),
                       "a_act_power": [1000.0, 1000.0, 1000.0]})
    out = calculate_energy(df)
    assert list(out["timestamp"]) == _ts(0, 3600, 7200)
    assert out["energy_kwh"].tolist() == pytest.approx([0.0, 1.0, 1.0])
    # Duplicate timestamps (delta 0) contribute nothing.
    dup = pd.DataFrame({"timestamp": _ts(0, 0, 3600), "a_act_power": [1000.0] * 3})
    assert calculate_energy(dup)["energy_kwh"].tolist() == pytest.approx([0.0, 0.0, 1.0])


def test_input_frame_is_not_mutated():
    df = _interval_df([100, 200], [0, 0], [0, 0])
    before = df.copy()
    calculate_energy(df)
    pd.testing.assert_frame_equal(df, before)


def test_filter_by_time_is_inclusive_on_both_ends_and_returns_a_copy():
    df = pd.DataFrame({"timestamp": _ts(0, 60, 120, 180), "v": [1, 2, 3, 4]})
    t = _ts(60, 120)
    assert filter_by_time(df, t[0], t[1])["v"].tolist() == [2, 3]
    assert filter_by_time(df, start=t[1])["v"].tolist() == [3, 4]
    assert filter_by_time(df, end=t[0])["v"].tolist() == [1, 2]
    everything = filter_by_time(df)
    assert len(everything) == 4
    everything["v"] = 0
    assert df["v"].tolist() == [1, 2, 3, 4]


# -- CSV reader ----------------------------------------------------------------

def test_timestamp_scales_seconds_milliseconds_and_text():
    sec = csv_read._parse_timestamp_col(pd.Series([1_772_323_200, 1_772_326_800]))
    ms = csv_read._parse_timestamp_col(pd.Series([1_772_323_200_000, 1_772_326_800_000]))
    iso = csv_read._parse_timestamp_col(pd.Series(["2026-03-01 00:00:00", "2026-03-01 01:00:00"]))
    assert sec.tolist() == ms.tolist()
    assert sec.iloc[0] == pd.Timestamp("2026-03-01 00:00:00")
    assert iso.iloc[1] == pd.Timestamp("2026-03-01 01:00:00")


def test_read_csv_files_merges_dedupes_and_survives_a_bad_file(tmp_path):
    (tmp_path / "a.csv").write_text("timestamp,a_act_power\n1772323200,10\n1772326800,20\n")
    (tmp_path / "b.csv").write_text("timestamp,a_act_power\n1772326800,25\n1772330400,30\n")   # overlaps a.csv
    (tmp_path / "bad.csv").write_text("nothing,useful\n1,2\n")
    (tmp_path / "semi.csv").write_text("ts;a_act_power\n1772334000;40\n")
    df = csv_read.read_csv_files([tmp_path / "a.csv", tmp_path / "b.csv", tmp_path / "bad.csv",
                                  tmp_path / "semi.csv", tmp_path / "missing.csv"])
    assert df["a_act_power"].tolist() == [10, 25, 30, 40]            # the overlap keeps the LAST value
    assert df["timestamp"].is_monotonic_increasing


def test_read_csv_files_fails_loudly_when_nothing_is_usable(tmp_path):
    (tmp_path / "bad.csv").write_text("nothing,useful\n1,2\n")
    with pytest.raises(ValueError):
        csv_read.read_csv_files([tmp_path / "bad.csv"])
    with pytest.raises(ValueError):
        csv_read.read_csv_files([])


def test_detect_power_columns_prefers_the_known_phase_columns():
    df = pd.DataFrame({"a_act_power": [1.0], "total_power_misc": [2.0], "name": ["x"]})
    assert csv_read.detect_power_columns(df) == ["a_act_power"]
    df2 = pd.DataFrame({"my_power": [1.0], "label_power": ["x"], "other": [3]})
    assert csv_read.detect_power_columns(df2) == ["my_power"]       # text column is not a power column
