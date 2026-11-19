"""Weather without an account (17.11.0).

The weather tab only knew OpenWeatherMap and refused to ask anything while the
key field was empty. Now ``weather.source`` picks Open-Meteo (default, no key),
MET Norway (no key) or OpenWeatherMap (key); the coordinates from Settings →
Weather are all the keyless sources need.
"""
import json
import os
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.io.config import load_config, save_config  # noqa: E402
from shelly_analyzer.io.storage import Storage  # noqa: E402
from shelly_analyzer.services import weather as wx  # noqa: E402

LANGS = ("de", "en", "es", "fr", "pt", "it", "pl", "cs", "ru")


@pytest.fixture(autouse=True)
def _fresh_caches():
    wx._CURRENT_CACHE.clear()
    wx._HISTORY_DONE.clear()
    yield
    wx._CURRENT_CACHE.clear()
    wx._HISTORY_DONE.clear()


class _Resp:
    def __init__(self, payload, status=200, headers=None):
        self._payload = payload
        self.status_code = status
        self.headers = headers or {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(f"{self.status_code}")


def _iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M")


def _om_current(temp=7.2):
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    return {"current": {
        "time": _iso(now), "temperature_2m": temp, "relative_humidity_2m": 81,
        "apparent_temperature": 4.9, "pressure_msl": 1013.4, "cloud_cover": 64,
        "wind_speed_10m": 3.6, "weather_code": 2,
    }}


def _om_hourly(days=30):
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = now - timedelta(days=days)
    times, temps = [], []
    t = start
    while t <= now + timedelta(hours=23):  # forecast hours must be skipped
        times.append(_iso(t))
        temps.append(round(5 + (t.hour % 12), 1))
        t += timedelta(hours=1)
    n = len(times)
    return {"hourly": {
        "time": times, "temperature_2m": temps, "relative_humidity_2m": [70] * n,
        "pressure_msl": [1010.0] * n, "cloud_cover": [50] * n,
        "wind_speed_10m": [2.0] * n, "weather_code": [61] * n,
    }}


def _metno():
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return {"properties": {"timeseries": [{
        "time": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "data": {
            "instant": {"details": {
                "air_temperature": 3.4, "relative_humidity": 92.1, "wind_speed": 5.2,
                "cloud_area_fraction": 100.0, "air_pressure_at_sea_level": 1002.3,
            }},
            "next_1_hours": {"summary": {"symbol_code": "lightrainshowers_day"}},
        },
    }]}}


def _dispatcher(tmp, weather):
    p = Path(tmp) / "config.json"
    p.write_text(json.dumps({
        "devices": [{"key": "d1", "name": "D1", "host": "192.0.2.10"}],
        "weather": weather,
    }))
    cfg, st = load_config(p), Storage(Path(tmp) / "data")
    from shelly_analyzer.web.action_dispatch import ActionDispatcher
    return ActionDispatcher(cfg, st, None, out_dir=Path(tmp), cfg_path=p, lang="en"), st


def _fake_get(url, params=None, headers=None, timeout=None):
    if "open-meteo" in url:
        if params and "hourly" in params:
            return _Resp(_om_hourly(int(params.get("past_days", 30))))
        return _Resp(_om_current())
    if "met.no" in url:
        return _Resp(_metno())
    raise AssertionError(f"unexpected request to {url}")


# --- the core: no key, coordinates set → the tab works -----------------------

def test_weather_tab_works_without_any_key():
    with tempfile.TemporaryDirectory() as tmp:
        ad, _st = _dispatcher(tmp, {"enabled": True, "source": "open_meteo",
                                    "lat": 52.52, "lon": 13.41})
        with mock.patch("requests.get", side_effect=_fake_get):
            out = ad.dispatch("weather_correlation", {})
        assert out["ok"] is True, out
        assert out["source"] == "open_meteo"
        assert out["current"]["temp_c"] == pytest.approx(7.2)
        assert out["current"]["wind_speed_ms"] == pytest.approx(3.6)
        assert out["current"]["description"] == "Partly cloudy"
        assert "Open-Meteo" in out["credit"] and "CC BY" in out["credit"]


def test_open_meteo_fills_the_last_30_days_so_correlation_has_data():
    with tempfile.TemporaryDirectory() as tmp:
        ad, st = _dispatcher(tmp, {"enabled": True, "lat": 52.52, "lon": 13.41})
        with mock.patch("requests.get", side_effect=_fake_get):
            ad.dispatch("weather_correlation", {})
        now = int(time.time())
        df = st.db.query_weather(now - 31 * 86400, now + 3 * 86400)
        assert len(df) >= 29 * 24
        # no forecast hour ends up in the table
        assert int(df["hour_ts"].max()) <= now


def test_backfill_runs_at_most_every_six_hours():
    with tempfile.TemporaryDirectory() as tmp:
        ad, _st = _dispatcher(tmp, {"enabled": True, "lat": 52.52, "lon": 13.41,
                                    "fetch_interval_minutes": 0})
        calls = []

        def spy(url, params=None, **kw):
            calls.append(params or {})
            return _fake_get(url, params=params, **kw)

        with mock.patch("requests.get", side_effect=spy):
            ad.dispatch("weather_correlation", {})
            ad.dispatch("weather_correlation", {})
        assert sum(1 for c in calls if "hourly" in c) == 1
        assert sum(1 for c in calls if "current" in c) == 2


def test_no_location_says_so_and_sends_nothing():
    with tempfile.TemporaryDirectory() as tmp:
        ad, _st = _dispatcher(tmp, {"enabled": True})
        with mock.patch("requests.get") as g:
            out = ad.dispatch("weather_correlation", {})
        assert out["ok"] is False
        assert "latitude" in out["error"].lower()
        g.assert_not_called()


def test_openweathermap_without_key_sends_nothing():
    with tempfile.TemporaryDirectory() as tmp:
        ad, _st = _dispatcher(tmp, {"enabled": True, "source": "openweathermap",
                                    "lat": 52.52, "lon": 13.41})
        with mock.patch("requests.get") as g:
            out = ad.dispatch("weather_correlation", {})
            assert wx.fetch_current("openweathermap", "", 52.52, 13.41) is None
        assert out["ok"] is False
        g.assert_not_called()


def test_met_no_through_the_tab():
    with tempfile.TemporaryDirectory() as tmp:
        ad, _st = _dispatcher(tmp, {"enabled": True, "source": "met_no",
                                    "lat": 59.9139, "lon": 10.7522})
        with mock.patch("requests.get", side_effect=_fake_get) as g:
            out = ad.dispatch("weather_correlation", {})
        assert out["ok"] is True and out["source"] == "met_no"
        assert out["current"]["temp_c"] == pytest.approx(3.4)
        assert out["current"]["description"] == "Rain showers"
        # MET Norway source: no Open-Meteo backfill
        assert all("met.no" in c.args[0] for c in g.call_args_list)


# --- parsers ------------------------------------------------------------------

def test_open_meteo_parser_and_request():
    with mock.patch("requests.get", return_value=_Resp(_om_current())) as g:
        s = wx.fetch_current_openmeteo(52.520008, 13.404954)
    assert s.temp_c == pytest.approx(7.2)
    assert s.humidity_pct == 81 and s.clouds_pct == 64
    assert s.pressure_hpa == pytest.approx(1013.4)
    assert s.feels_like_c == pytest.approx(4.9)
    assert s.description == "partly cloudy"
    assert abs(s.timestamp - time.time()) < 120
    params = g.call_args.kwargs["params"]
    assert params["wind_speed_unit"] == "ms"
    assert params["latitude"] == 52.52 and params["longitude"] == 13.405


def test_open_meteo_error_is_none_not_an_exception():
    with mock.patch("requests.get", return_value=_Resp({}, status=500)):
        assert wx.fetch_current_openmeteo(52.5, 13.4) is None
    import requests
    with mock.patch("requests.get", side_effect=requests.ConnectionError("down")):
        assert wx.fetch_current_openmeteo(52.5, 13.4) is None
        assert wx.fetch_history_openmeteo(52.5, 13.4) == []


def test_met_no_parser_user_agent_and_coordinates():
    with mock.patch("requests.get", return_value=_Resp(_metno())) as g:
        s = wx.fetch_current_metno(59.913868123, 10.752245789)
    assert s.temp_c == pytest.approx(3.4)
    assert s.humidity_pct == pytest.approx(92.1)
    assert s.wind_speed_ms == pytest.approx(5.2)
    assert s.clouds_pct == 100
    assert s.pressure_hpa == pytest.approx(1002.3)
    assert s.feels_like_c == pytest.approx(3.4)  # no apparent temperature → air temperature
    assert s.description == "rain showers"
    ua = g.call_args.kwargs["headers"]["User-Agent"]
    assert "shelly-energy-analyzer" in ua and "github.com/robeertm" in ua
    for v in g.call_args.kwargs["params"].values():
        assert len(str(v).split(".")[1]) <= 4


def test_met_no_is_asked_at_most_every_30_minutes():
    with mock.patch("requests.get", return_value=_Resp(_metno())) as g:
        a = wx.fetch_current("met_no", "", 59.9139, 10.7522, min_interval_s=0)
        b = wx.fetch_current("met_no", "", 59.9139, 10.7522, min_interval_s=0)
    assert a is not None and b is a
    assert g.call_count == 1


def test_met_no_403_is_none():
    with mock.patch("requests.get", return_value=_Resp({}, status=403)):
        assert wx.fetch_current_metno(59.9, 10.7) is None
        assert wx.fetch_current("met_no", "", 59.9, 10.7) is None


def test_symbol_and_wmo_vocabulary():
    assert wx._metno_text("clearsky_night") == "clear sky"
    assert wx._metno_text("heavysnowshowersandthunder_day") == "thunderstorm"
    assert wx._metno_text("lightsleet") == "sleet"
    assert wx._metno_text("cloudy") == "overcast"
    assert wx._wmo_text(95) == "thunderstorm"
    assert wx._wmo_text(None) == ""
    for text in wx.CONDITION_TEXT.values():
        assert wx.condition_key(text)


# --- configuration ------------------------------------------------------------

def _load(weather):
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "config.json"
        body = {"devices": []}
        if weather is not None:
            body["weather"] = weather
        p.write_text(json.dumps(body))
        return load_config(p).weather


def test_existing_owm_install_stays_on_owm():
    assert _load({"enabled": True, "api_key": "abc", "lat": 1, "lon": 2}).source == "openweathermap"


def test_default_is_open_meteo():
    assert _load(None).source == "open_meteo"
    assert _load({"enabled": True}).source == "open_meteo"
    assert _load({"source": "bogus", "api_key": "abc"}).source == "open_meteo"
    assert _load({"source": "MET_NO"}).source == "met_no"


def test_source_survives_save_and_load():
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "config.json"
        p.write_text(json.dumps({"devices": [], "weather": {"source": "met_no", "api_key": "k"}}))
        cfg = load_config(p)
        save_config(cfg, p)
        assert json.loads(p.read_text())["weather"]["source"] == "met_no"
        assert load_config(p).weather.source == "met_no"


# --- texts --------------------------------------------------------------------

def test_new_texts_in_all_languages():
    from shelly_analyzer.i18n import _I18N
    keys = ["settings.field.weather.source", "settings.hint.weather.source",
            "settings.opts.weather.source.open_meteo", "settings.opts.weather.source.met_no",
            "settings.opts.weather.source.openweathermap", "settings.hint.weather.api_key",
            "settings.hint.weather.fetch_interval_minutes",
            "web.weather.no_location", "web.weather.credit"]
    keys += ["web.weather.wx." + k for k in wx.CONDITION_TEXT]
    for lang in LANGS:
        for k in keys:
            assert str(_I18N[lang].get(k, "")).strip(), (lang, k)
        assert "{src}" in _I18N[lang]["web.weather.credit"]


def test_weather_tab_names_the_source():
    from shelly_analyzer.services import webdash
    src = Path(webdash.__file__).read_text(encoding="utf-8")
    assert "web.weather.credit" in src and "_wxCredit(d)" in src


def test_traffic_counts_keyless_weather_as_weather():
    from shelly_analyzer.services import traffic
    assert traffic._categorize("https://api.open-meteo.com/v1/forecast?x=1") == "weather"
    assert traffic._categorize("https://api.met.no/weatherapi/locationforecast/2.0/compact") == "weather"
