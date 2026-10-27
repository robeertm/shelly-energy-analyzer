"""TOU windows from the OpenEI Utility Rate Database (services/urdb.py).

A URDB schedule is a 12 x 24 matrix of period indexes per month and hour,
one for weekdays and one for weekends. It becomes TouRate windows for one
month: weekday windows (Mon-Fri only) first, weekend windows after them, so
"first matching rate wins" picks the weekend windows only on Sat/Sun.
"""
import datetime as dt
import json
import os
import sys
import tempfile
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import requests

from shelly_analyzer.io.config import TouConfig, TouRate, load_config, save_config
from shelly_analyzer.services import urdb

# Periods: 0 summer peak, 1 summer off-peak, 2 winter peak, 3 winter off-peak.
SUMMER_WD = [1] * 16 + [0] * 5 + [1] * 3   # peak 16-21 on weekdays
SUMMER_WE = [1] * 24                       # off-peak all weekend
WINTER = [3] * 16 + [2] * 5 + [3] * 3      # peak 16-21 every day
RATE = {
    "label": "0123456789abcdef01234567",
    "utility": "Test Utility",
    "name": "Test TOU",
    "energyratestructure": [
        [{"rate": 0.45, "adj": 0.04, "unit": "kWh"}],
        [{"rate": 0.38, "max": 300, "unit": "kWh"}, {"rate": 0.50, "unit": "kWh"}],
        [{"rate": 0.36, "unit": "kWh"}],
        [{"rate": 0.33, "adj": -0.01, "unit": "kWh"}],
    ],
    # June-September summer, the rest winter.
    "energyweekdayschedule": [WINTER] * 5 + [SUMMER_WD] * 4 + [WINTER] * 3,
    "energyweekendschedule": [WINTER] * 5 + [SUMMER_WE] * 4 + [WINTER] * 3,
}


def test_windows_merge_the_overnight_run():
    assert urdb._windows(SUMMER_WD) == [(16, 21, 0), (21, 16, 1)]
    assert urdb._windows([1] * 12 + [0] * 12) == [(0, 12, 1), (12, 0, 0)]
    assert urdb._windows([5] * 24) == [(0, 0, 5)]


def test_summer_month_weekday_windows_then_weekend_windows():
    rates = urdb.tou_rates_for_month(RATE, 7)
    assert rates == [
        TouRate("Period 1", 0.49, 16, 21, True),
        TouRate("Period 2", 0.38, 21, 16, True),
        TouRate("Period 2", 0.38, 0, 0, False),
    ]


def test_winter_month_same_rows_give_one_all_days_set():
    rates = urdb.tou_rates_for_month(RATE, 1)
    assert rates == [
        TouRate("Period 3", 0.36, 16, 21, False),
        TouRate("Period 4", 0.32, 21, 16, False),
    ]


def test_price_at_uses_first_matching_window():
    tou = TouConfig(enabled=True, rates=urdb.tou_rates_for_month(RATE, 7))
    monday, saturday = dt.datetime(2026, 7, 6, 17), dt.datetime(2026, 7, 4, 17)
    assert monday.weekday() == 0 and saturday.weekday() == 5
    assert tou.price_at(monday) == 0.49
    assert tou.price_at(monday.replace(hour=21)) == 0.38
    assert tou.price_at(monday.replace(hour=3)) == 0.38
    assert tou.price_at(saturday) == 0.38
    weekdays_only = TouConfig(rates=[TouRate("Peak", 0.5, 6, 22, True)])
    assert weekdays_only.price_at(saturday) is None


def test_bad_period_index_is_an_error():
    bad = dict(RATE, energyweekdayschedule=[[9] * 24] * 12)
    try:
        urdb.tou_rates_for_month(bad, 1)
    except ValueError as e:
        assert "period 9" in str(e)
    else:
        raise AssertionError("expected ValueError")


def test_parse_rate_label_accepts_label_or_url():
    label = "5cc0e8c45457a3d8137b23c6"
    assert urdb.parse_rate_label(label) == label
    assert urdb.parse_rate_label(f"https://apps.openei.org/IURDB/rate/view/{label}#1__Basic_Information") == label
    try:
        urdb.parse_rate_label("https://apps.openei.org/IURDB/")
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError")


def test_network_error_does_not_show_the_api_key():
    boom = requests.ConnectionError("Max retries exceeded with url: /utility_rates?api_key=SECRET123")
    with mock.patch.object(urdb.requests, "get", side_effect=boom):
        try:
            urdb.fetch_rate("0123456789abcdef01234567", "SECRET123")
        except RuntimeError as e:
            assert "SECRET123" not in str(e)
        else:
            raise AssertionError("expected RuntimeError")


def test_api_error_message_is_passed_on():
    class _Resp:
        status_code = 403

        def json(self):
            return {"error": {"code": "API_KEY_INVALID", "message": "An invalid api_key was supplied."}}

    with mock.patch.object(urdb.requests, "get", return_value=_Resp()):
        try:
            urdb.fetch_rate("0123456789abcdef01234567", "bad")
        except RuntimeError as e:
            assert "invalid api_key" in str(e)
        else:
            raise AssertionError("expected RuntimeError")


def test_search_rates_keeps_current_newest_rates_sorted_by_utility():
    now = 1_790_000_000
    items = [
        {"label": "a" * 24, "utility": "Pacific Gas & Electric Co", "name": "E-TOU-C", "startdate": now - 900},
        {"label": "b" * 24, "utility": "Pacific Gas & Electric Co", "name": "E-TOU-C", "startdate": now - 100},
        {"label": "c" * 24, "utility": "City Utility", "name": "Residential", "startdate": now - 50},
        {"label": "d" * 24, "utility": "City Utility", "name": "Old", "startdate": 1, "enddate": now - 1},
        {"label": "e" * 24, "utility": "City Utility", "name": "Next year", "startdate": now + 10},
    ]
    seen = {}

    class _Resp:
        status_code = 200

        def json(self):
            return {"items": items}

    def _get(url, params=None, timeout=None):
        seen.update(params)
        return _Resp()

    with mock.patch.object(urdb.requests, "get", side_effect=_get):
        rates = urdb.search_rates("94110", "key", now=now)
    assert rates == [
        {"label": "c" * 24, "utility": "City Utility", "name": "Residential"},
        {"label": "b" * 24, "utility": "Pacific Gas & Electric Co", "name": "E-TOU-C"},
    ]
    assert seen["address"] == "94110" and "lat" not in seen
    assert seen["sector"] == "Residential" and seen["effective_on_date"] == now
    assert seen["radius"] == urdb.SEARCH_RADIUS_MILES

    seen.clear()
    with mock.patch.object(urdb.requests, "get", side_effect=_get):
        urdb.search_rates("", "key", lat=37.77, lon=-122.42, now=now)
    assert seen["lat"] == 37.77 and seen["lon"] == -122.42 and "address" not in seen


def test_search_rates_needs_a_place():
    try:
        urdb.search_rates("", "key")
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError")


def test_config_round_trip_keeps_urdb_fields():
    raw = {
        "version": "0",
        "devices": [],
        "tou": {
            "enabled": True,
            "urdb_rate": "5cc0e8c45457a3d8137b23c6",
            "urdb_api_key": "abc",
            "urdb_location": "94110",
            "rates": [{"name": "Period 1", "price_eur_per_kwh": 0.49, "start_hour": 16,
                       "end_hour": 21, "weekdays_only": True}],
        },
    }
    f = tempfile.mktemp(suffix=".json")
    try:
        with open(f, "w") as fh:
            fh.write(json.dumps(raw))
        cfg = load_config(f)
        assert cfg.tou.urdb_rate == "5cc0e8c45457a3d8137b23c6"
        assert cfg.tou.urdb_api_key == "abc"
        assert cfg.tou.urdb_location == "94110"
        save_config(cfg, f)
        with open(f) as fh:
            again = json.load(fh)
        assert again["tou"]["urdb_rate"] == "5cc0e8c45457a3d8137b23c6"
        assert again["tou"]["urdb_api_key"] == "abc"
        assert again["tou"]["urdb_location"] == "94110"
        # An old config without the fields still loads.
        with open(f, "w") as fh:
            fh.write(json.dumps({"version": "0", "devices": [], "tou": {"enabled": False}}))
        old = load_config(f)
        assert old.tou.urdb_rate == "" and old.tou.urdb_api_key == "" and old.tou.urdb_location == ""
    finally:
        try:
            os.unlink(f)
        except OSError:
            pass


if __name__ == "__main__":
    import inspect as _inspect
    _mod = sys.modules[__name__]
    _fns = [(n, f) for n, f in vars(_mod).items()
            if n.startswith("test_") and callable(f)]
    _fns.sort(key=lambda kv: _inspect.getsourcelines(kv[1])[1])
    for _n, _f in _fns:
        _f()
        print("OK  %s" % _n)
    print("\n%d tests passed (%s)." % (len(_fns), os.path.basename(__file__)))
