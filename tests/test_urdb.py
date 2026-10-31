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
# A flat, tiered rate with a monthly fixed charge, shaped like a real URDB rate.
FLAT = {
    "label": "69ef9ad6ca9e815b0e0a185a",
    "utility": "Regional Utility",
    "name": "Residential Service",
    "energyratestructure": [[{"rate": 0.146284, "adj": 0.046581, "max": 600, "unit": "kWh"},
                             {"rate": 0.165701, "adj": 0.046581, "unit": "kWh"}]],
    "energyweekdayschedule": [[0] * 24] * 12,
    "energyweekendschedule": [[0] * 24] * 12,
    "fixedchargefirstmeter": 7.49,
    "fixedchargeunits": "$/month",
}


class _FakeResp:
    """What requests.get(..., stream=True) returns, as far as urdb reads it."""

    def __init__(self, payload, status_code=200, raw=None):
        self.status_code = status_code
        self._body = raw if raw is not None else json.dumps(payload).encode()

    def iter_content(self, chunk_size=1):
        for i in range(0, len(self._body), chunk_size):
            yield self._body[i:i + chunk_size]

    def close(self):
        pass


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
    assert urdb.parse_rate_label("") == ""
    for bad in ("https://apps.openei.org/IURDB/", "residential", "abc&api_key=x"):
        try:
            urdb.parse_rate_label(bad)
        except ValueError:
            pass
        else:
            raise AssertionError(f"expected ValueError for {bad!r}")


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
    resp = _FakeResp({"error": {"code": "API_KEY_INVALID", "message": "An invalid api_key was supplied."}}, 403)
    with mock.patch.object(urdb.requests, "get", return_value=resp):
        try:
            urdb.fetch_rate("0123456789abcdef01234567", "bad")
        except RuntimeError as e:
            assert "invalid api_key" in str(e)
        else:
            raise AssertionError("expected RuntimeError")


def test_search_rates_keeps_current_newest_rates_sorted_by_utility():
    now = 1_790_000_000
    items = [
        {"label": "a" * 24, "utility": "Regional Utility", "name": "Time of Use", "startdate": now - 900},
        {"label": "b" * 24, "utility": "Regional Utility", "name": "Time of Use", "startdate": now - 100},
        {"label": "c" * 24, "utility": "City Utility", "name": "Residential", "startdate": now - 50},
        {"label": "d" * 24, "utility": "City Utility", "name": "Old", "startdate": 1, "enddate": now - 1},
        {"label": "e" * 24, "utility": "City Utility", "name": "Next year", "startdate": now + 10},
    ]
    seen = {}

    def _get(url, params=None, headers=None, timeout=None, stream=False, allow_redirects=True):
        seen.update(params)
        return _FakeResp({"items": items})

    with mock.patch.object(urdb.requests, "get", side_effect=_get):
        rates = urdb.search_rates("94110", "key", now=now)
    assert rates == [
        {"label": "c" * 24, "utility": "City Utility", "name": "Residential"},
        {"label": "b" * 24, "utility": "Regional Utility", "name": "Time of Use"},
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


def test_price_for_month_averages_the_week_and_flat_rate_keeps_its_price():
    july = (19 * 0.38 + 5 * 0.49) * 5 + 24 * 0.38 * 2
    assert abs(urdb.price_for_month(RATE, 7) - july / 168) < 1e-6
    assert abs(urdb.price_for_month(RATE, 1) - (19 * 0.32 + 5 * 0.36) / 24) < 1e-6
    assert urdb.price_for_month(FLAT, 10) == 0.192865


def test_fixed_charge_becomes_a_yearly_base_fee():
    assert urdb.fixed_fee_per_year(FLAT) == 89.88
    assert urdb.fixed_fee_per_year(dict(FLAT, fixedchargeunits="$/day", fixedchargefirstmeter=0.5)) == 182.5
    assert urdb.fixed_fee_per_year(dict(FLAT, fixedchargefirstmeter=None)) is None
    assert urdb.fixed_fee_per_year(dict(FLAT, fixedchargeunits="$/kW")) is None


def test_config_from_rate_sets_pricing_and_turns_tou_on_only_for_tou_rates():
    f = tempfile.mktemp(suffix=".json")
    try:
        with open(f, "w") as fh:
            fh.write(json.dumps({"version": "0", "devices": [],
                                 "pricing": {"electricity_price_eur_per_kwh": 0.32, "base_fee_eur_per_year": 120,
                                             "price_includes_vat": False}}))
        cfg = load_config(f)
    finally:
        os.unlink(f)
    flat = urdb.config_from_rate(cfg, FLAT, 10, urdb_rate="69ef9ad6ca9e815b0e0a185a")
    assert flat.pricing.electricity_price_eur_per_kwh == 0.192865
    assert flat.pricing.base_fee_eur_per_year == 89.88
    assert flat.pricing.price_includes_vat and flat.pricing.base_fee_includes_vat
    assert flat.tou.enabled is False and len(flat.tou.rates) == 1
    assert flat.tou.urdb_rate == "69ef9ad6ca9e815b0e0a185a"
    tou = urdb.config_from_rate(cfg, RATE, 7)
    assert tou.tou.enabled is True and len(tou.tou.rates) == 3
    # RATE has no fixed charge: the base fee stays as it was.
    assert tou.pricing.base_fee_eur_per_year == 120


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



# ── Added in review: no foreign URL, no key in the URL, bad answers ────────

def _capture_get(payload, calls):
    def _get(url, params=None, headers=None, timeout=None, stream=False, allow_redirects=True):
        calls.append({"url": url, "params": dict(params or {}), "headers": dict(headers or {}),
                      "timeout": timeout, "allow_redirects": allow_redirects})
        return _FakeResp(payload)
    return _get


def test_a_pasted_url_never_decides_where_the_request_goes():
    """Only the 24-hex label of a pasted text is used; the host is fixed."""
    calls = []
    label = "5cc0e8c45457a3d8137b23c6"
    pasted = f"http://169.254.169.254/latest/meta-data/{label}"
    with mock.patch.object(urdb.requests, "get", side_effect=_capture_get({"items": [RATE]}, calls)):
        urdb.fetch_rate(urdb.parse_rate_label(pasted), "SECRET123")
    assert len(calls) == 1
    assert calls[0]["url"] == "https://api.openei.org/utility_rates"
    assert calls[0]["params"]["getpage"] == label
    assert calls[0]["timeout"] and calls[0]["timeout"] > 0


def test_api_key_goes_in_a_header_not_in_the_url():
    calls = []
    with mock.patch.object(urdb.requests, "get", side_effect=_capture_get({"items": [RATE]}, calls)):
        urdb.fetch_rate("0123456789abcdef01234567", "SECRET123")
        urdb.fetch_rate("0123456789abcdef01234567", "")
    assert "api_key" not in calls[0]["params"]
    assert "SECRET123" not in json.dumps(calls[0]["params"])
    assert calls[0]["headers"]["X-Api-Key"] == "SECRET123"
    assert calls[1]["headers"]["X-Api-Key"] == urdb.DEMO_KEY


def test_redirects_are_not_followed_so_the_key_never_leaves():
    calls = []
    with mock.patch.object(urdb.requests, "get", side_effect=_capture_get({"items": [RATE]}, calls)):
        urdb.fetch_rate("0123456789abcdef01234567", "SECRET123")
    assert calls[0]["allow_redirects"] is False
    redirect = _FakeResp({}, status_code=302, raw=b"")
    with mock.patch.object(urdb.requests, "get", return_value=redirect):
        try:
            urdb.fetch_rate("0123456789abcdef01234567", "SECRET123")
        except RuntimeError as e:
            assert "redirect" in str(e) and "SECRET123" not in str(e)
        else:
            raise AssertionError("a redirect must be an error")


def test_api_key_is_not_logged_on_refresh_failure(caplog=None):
    import logging
    records = []

    class _H(logging.Handler):
        def emit(self, r):
            records.append(r.getMessage())

    h = _H()
    logging.getLogger().addHandler(h)
    try:
        state = mock.Mock()
        state.cfg.tou = TouConfig(urdb_rate="0123456789abcdef01234567", urdb_api_key="SECRET123")
        boom = requests.ConnectionError("url: /utility_rates?api_key=SECRET123")
        stop = mock.Mock()
        stop.wait.side_effect = [False, True]   # skip the start delay, then stop
        stop.is_set.return_value = False
        with mock.patch.object(urdb.requests, "get", side_effect=boom):
            urdb.refresh_loop(state, stop)
    finally:
        logging.getLogger().removeHandler(h)
    assert records, "the failure must be logged"
    assert not any("SECRET123" in m for m in records)


def test_oversized_answer_is_refused():
    big = b'{"items": [' + b" " * (urdb.MAX_RESPONSE_BYTES + 10) + b"]}"
    with mock.patch.object(urdb.requests, "get", return_value=_FakeResp(None, raw=big)):
        try:
            urdb.fetch_rate("0123456789abcdef01234567", "k")
        except RuntimeError as e:
            assert "too large" in str(e)
        else:
            raise AssertionError("expected RuntimeError")


def test_answers_without_usable_data_give_readable_errors():
    """A rate without energy charges or schedules, empty items, or odd shapes
    must raise ValueError/RuntimeError with a message, never TypeError/IndexError."""
    for items in ([], None, ["not a dict"], {"a": 1}):
        with mock.patch.object(urdb.requests, "get", return_value=_FakeResp({"items": items})):
            try:
                urdb.fetch_rate("0123456789abcdef01234567", "k")
            except RuntimeError as e:
                assert "not found" in str(e)
            else:
                raise AssertionError(f"expected RuntimeError for items={items!r}")
    broken = [
        {k: v for k, v in RATE.items() if k != "energyratestructure"},
        {k: v for k, v in RATE.items() if k != "energyweekdayschedule"},
        dict(RATE, energyratestructure=None),
        dict(RATE, energyweekdayschedule="x" * 12),
        dict(RATE, energyweekdayschedule=[[0] * 23] * 12),
        dict(RATE, energyweekdayschedule=[None] * 12),
        dict(RATE, energyweekendschedule=[[0] * 24] * 11),
        dict(RATE, energyweekdayschedule=[["a"] * 24] * 12),
        dict(RATE, energyratestructure=[[{"rate": "n/a"}]]),
        dict(RATE, energyratestructure=[[]]),
    ]
    for bad in broken:
        for fn in (urdb.tou_rates_for_month, urdb.price_for_month):
            try:
                fn(bad, 7)
            except ValueError as e:
                assert str(e)
            else:
                raise AssertionError(f"expected ValueError from {fn.__name__}")
    assert urdb.fixed_fee_per_year(dict(FLAT, fixedchargefirstmeter="n/a")) is None


def _settings_client(cfg, saved):
    from flask import Flask
    from shelly_analyzer.web.blueprints import settings as settings_bp
    app = Flask(__name__)
    state = mock.Mock()
    state.cfg = cfg
    app.extensions["state"] = state
    app.register_blueprint(settings_bp.bp)
    return app.test_client(), state


def _empty_cfg():
    f = tempfile.mktemp(suffix=".json")
    try:
        with open(f, "w") as fh:
            fh.write(json.dumps({"version": "0", "devices": []}))
        return load_config(f)
    finally:
        os.unlink(f)


def test_failed_import_leaves_the_settings_unchanged():
    cfg = _empty_cfg()
    saved = []
    client, state = _settings_client(cfg, saved)
    no_schedule = {k: v for k, v in RATE.items() if k != "energyweekdayschedule"}
    with mock.patch.object(urdb, "apply_config", side_effect=lambda s, c: saved.append(c)), \
         mock.patch.object(urdb.requests, "get", return_value=_FakeResp({"items": [no_schedule]})):
        r = client.post("/api/settings/tou-urdb-import", json={"rate": "0123456789abcdef01234567"}).get_json()
    assert r["ok"] is False and "schedule" in r["error"]
    assert saved == [] and state.cfg is cfg

    with mock.patch.object(urdb, "apply_config", side_effect=lambda s, c: saved.append(c)), \
         mock.patch.object(urdb.requests, "get") as get:
        r = client.post("/api/settings/tou-urdb-import", json={"rate": "https://example.com/x"}).get_json()
        assert r == {"ok": False, "code": "bad_rate", "error": r["error"]}
        r = client.post("/api/settings/tou-urdb-import", json={"rate": ""}).get_json()
        assert r["code"] == "no_rate"
        assert get.call_count == 0
    assert saved == []


def test_successful_import_returns_fields_for_the_ui_text():
    cfg = _empty_cfg()
    saved = []
    client, state = _settings_client(cfg, saved)
    with mock.patch.object(urdb, "apply_config", side_effect=lambda s, c: saved.append(c)), \
         mock.patch.object(urdb.requests, "get", return_value=_FakeResp({"items": [FLAT]})):
        r = client.post("/api/settings/tou-urdb-import",
                        json={"rate": "https://apps.openei.org/IURDB/rate/view/69ef9ad6ca9e815b0e0a185a"}).get_json()
    assert r["ok"] is True
    assert r["utility"] == "Regional Utility" and r["price"] == 0.192865 and r["base_fee"] == 89.88
    assert r["tou_enabled"] is False and r["windows"] == 1
    # The label is stored, not the pasted URL.
    assert saved[0].tou.urdb_rate == "69ef9ad6ca9e815b0e0a185a"


def test_search_without_a_place_sends_nothing():
    cfg = _empty_cfg()
    client, state = _settings_client(cfg, [])
    with mock.patch.object(urdb.requests, "get") as get:
        r = client.post("/api/settings/tou-urdb-search", json={"location": ""}).get_json()
    assert r["ok"] is False and r["code"] == "no_location"
    assert get.call_count == 0


def test_get_settings_masks_the_key_and_put_keeps_it():
    from shelly_analyzer.web.blueprints import settings as settings_bp
    cfg = _empty_cfg()
    from dataclasses import replace
    cfg = replace(cfg, tou=replace(cfg.tou, urdb_api_key="SECRET123"))
    client, state = _settings_client(cfg, [])
    state._cfg_path = None
    body = client.get("/api/settings").get_data(as_text=True)
    assert "SECRET123" not in body
    merged = {"tou": {"urdb_api_key": "***"}}
    settings_bp._restore_secrets(merged, cfg)
    assert merged["tou"]["urdb_api_key"] == "SECRET123"


def test_month_change_uses_the_local_calendar_month():
    """The refresh applies the schedule of the local month the loop sees, and a
    new month triggers a refresh at once instead of waiting a day."""
    seen = []
    state = mock.Mock()
    state.cfg.tou = TouConfig(urdb_rate="0123456789abcdef01234567")
    months = iter([10, 10, 11])

    class _D(dt.date):
        @classmethod
        def today(cls):
            return dt.date(2026, next(months), 1)

    stop = mock.Mock()
    stop.wait.side_effect = [False, False, False, True]
    stop.is_set.return_value = False
    with mock.patch.object(urdb, "date", _D), \
         mock.patch.object(urdb, "refresh_once", side_effect=lambda s, m: seen.append(m)):
        urdb.refresh_loop(state, stop)
    assert seen == [10, 11]


def test_tou_off_keeps_the_flat_price_for_mqtt():
    """Users without TOU (the default) see no change in the published price."""
    from shelly_analyzer.web.background import BackgroundServiceManager
    cfg = _empty_cfg()
    bg = BackgroundServiceManager.__new__(BackgroundServiceManager)
    bg.cfg = cfg
    flat = float(cfg.pricing.effective_pricing_for_date(dt.date.today()).unit_price_gross())
    assert cfg.tou.enabled is False
    assert bg._current_tariff_price() == flat


def test_tou_on_publishes_the_active_window_price():
    from dataclasses import replace
    from shelly_analyzer.web.background import BackgroundServiceManager
    cfg = _empty_cfg()
    cfg = replace(cfg, tou=TouConfig(enabled=True, rates=[TouRate("All day", 0.4242, 0, 0, False)]))
    bg = BackgroundServiceManager.__new__(BackgroundServiceManager)
    bg.cfg = cfg
    assert bg._current_tariff_price() == 0.4242


def test_tou_price_follows_the_vat_setting_of_the_main_price():
    from dataclasses import replace
    from shelly_analyzer.web.background import BackgroundServiceManager
    cfg = _empty_cfg()
    tou = TouConfig(enabled=True, rates=[TouRate("All day", 0.40, 0, 0, False)])
    for incl, vat_on, expect in ((True, True, 0.40), (False, True, 0.40 * 1.19), (False, False, 0.40)):
        pr = replace(cfg.pricing, price_includes_vat=incl, vat_enabled=vat_on, vat_rate_percent=19.0)
        bg = BackgroundServiceManager.__new__(BackgroundServiceManager)
        bg.cfg = replace(cfg, pricing=pr, tou=tou)
        assert abs(bg._current_tariff_price() - expect) < 1e-9, (incl, vat_on)


def test_all_app_languages_have_the_urdb_texts():
    from shelly_analyzer.i18n import _I18N
    keys = [k for k in _I18N["en"] if "urdb" in k]
    assert len(keys) >= 19
    for lang in ("de", "en", "es", "fr", "it", "pl", "pt", "cs", "ru"):
        missing = [k for k in keys if not _I18N.get(lang, {}).get(k)]
        assert not missing, (lang, missing)
    for lang in ("de", "es", "fr", "it", "pl", "pt", "cs", "ru"):
        same = [k for k in keys if _I18N[lang][k] == _I18N["en"][k]
                and k not in ("settings.field.tou.urdb_api_key",)]
        assert not same, (lang, same)


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
