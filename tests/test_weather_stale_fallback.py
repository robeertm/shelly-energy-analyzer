"""Weather tab must always show *something* even when the live OWM call fails.

When ``fetch_current_weather`` returns None (network error / rate-limited free
tier), the dispatcher falls back to the newest stored observation and flags it
stale, so the hero never blanks. This covers the pure fallback builder.

Run: python3 tests/test_weather_stale_fallback.py  (no pytest dependency)
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pandas as pd  # noqa: E402

from shelly_analyzer.services.weather import stale_current_from_weather_df  # noqa: E402


def _df(rows):
    cols = ["hour_ts", "temp_c", "humidity_pct", "wind_speed_ms",
            "clouds_pct", "pressure_hpa", "description", "fetched_at"]
    return pd.DataFrame(rows, columns=cols)


def test_empty_returns_none():
    assert stale_current_from_weather_df(_df([])) is None
    assert stale_current_from_weather_df(None) is None


def test_picks_newest_row():
    # query_weather returns ascending by hour_ts → newest is last.
    df = _df([
        (1000, 5.0, 80, 2.0, 90, 1005, "clouds", 1001),
        (4600, 12.5, 55, 3.4, 20, 1012, "clear sky", 4601),
    ])
    res = stale_current_from_weather_df(df)
    assert res is not None
    current, as_of = res
    assert as_of == 4600
    assert current["temp_c"] == 12.5
    # feels_like falls back to temp when unknown
    assert current["feels_like_c"] == 12.5
    assert current["humidity_pct"] == 55.0
    assert current["description"] == "clear sky"


def test_missing_temp_is_unusable():
    df = _df([(2000, None, 60, 1.0, 50, 1000, "fog", 2001)])
    assert stale_current_from_weather_df(df) is None


def test_null_numeric_fields_become_none_not_crash():
    df = _df([(3000, 9.0, None, None, None, None, None, 3001)])
    current, as_of = stale_current_from_weather_df(df)
    assert as_of == 3000
    assert current["temp_c"] == 9.0
    assert current["humidity_pct"] is None
    assert current["pressure_hpa"] == 0  # None → 0 for display
    assert current["description"] == ""


if __name__ == "__main__":
    test_empty_returns_none()
    test_picks_newest_row()
    test_missing_temp_is_unusable()
    test_null_numeric_fields_become_none_not_crash()
    print("OK")
