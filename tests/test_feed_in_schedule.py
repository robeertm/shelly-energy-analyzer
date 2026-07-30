"""Feed-in tariff schedule: config round-trip + date-effective lookup.

Mirrors the purchase-price tariff_schedule mechanism for the export tariff.
"""
import datetime as dt
import json
import tempfile
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.io.config import (
    SolarConfig, FeedInPeriod, load_config, save_config,
)


def test_effective_feed_in_for_date_picks_latest_active():
    s = SolarConfig(
        feed_in_tariff_eur_per_kwh=0.082,
        feed_in_schedule=[
            FeedInPeriod("2026-01-01", 0.07),
            FeedInPeriod("2026-08-01", 0.05),
        ],
    )
    # before any period → base
    assert s.effective_feed_in_for_date(dt.date(2025, 12, 31)) == 0.082
    # between first and second → first
    assert s.effective_feed_in_for_date(dt.date(2026, 3, 15)) == 0.07
    # on/after second → second
    assert s.effective_feed_in_for_date(dt.date(2026, 8, 1)) == 0.05
    assert s.effective_feed_in_for_date(dt.date(2026, 9, 1)) == 0.05


def test_effective_feed_in_accepts_datetime():
    s = SolarConfig(feed_in_schedule=[FeedInPeriod("2026-08-01", 0.05)])
    assert s.effective_feed_in_for_date(dt.datetime(2026, 8, 2, 13, 0)) == 0.05


def test_effective_feed_in_no_schedule_falls_back_to_base():
    s = SolarConfig(feed_in_tariff_eur_per_kwh=0.09)
    assert s.effective_feed_in_for_date(dt.date(2026, 9, 1)) == 0.09


def test_effective_feed_in_ignores_bad_dates():
    s = SolarConfig(
        feed_in_tariff_eur_per_kwh=0.082,
        feed_in_schedule=[FeedInPeriod("not-a-date", 0.01),
                          FeedInPeriod("2026-08-01", 0.05)],
    )
    # bad start_date is skipped, good one still applies
    assert s.effective_feed_in_for_date(dt.date(2026, 9, 1)) == 0.05
    assert s.effective_feed_in_for_date(dt.date(2026, 1, 1)) == 0.082


def test_config_round_trip_preserves_schedule():
    raw = {
        "version": "0",
        "devices": [],
        "solar": {
            "enabled": True,
            "feed_in_tariff_eur_per_kwh": 0.082,
            "feed_in_schedule": [
                {"start_date": "2026-08-01", "feed_in_tariff_eur_per_kwh": 0.05},
            ],
        },
    }
    f = tempfile.mktemp(suffix=".json")
    try:
        open(f, "w").write(json.dumps(raw))
        cfg = load_config(f)
        assert [(p.start_date, p.feed_in_tariff_eur_per_kwh)
                for p in cfg.solar.feed_in_schedule] == [("2026-08-01", 0.05)]
        # serialize back and reload
        save_config(cfg, f)
        again = json.load(open(f))
        assert again["solar"]["feed_in_schedule"] == [
            {"start_date": "2026-08-01", "feed_in_tariff_eur_per_kwh": 0.05}]
        cfg2 = load_config(f)
        assert [(p.start_date, p.feed_in_tariff_eur_per_kwh)
                for p in cfg2.solar.feed_in_schedule] == [("2026-08-01", 0.05)]
    finally:
        try:
            os.unlink(f)
        except OSError:
            pass


def test_empty_schedule_serializes_as_empty_list():
    s = SolarConfig()
    assert s.feed_in_schedule == []
