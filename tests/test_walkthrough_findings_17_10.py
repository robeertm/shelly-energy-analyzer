"""Findings of the customer walkthrough fixed in 17.10.0.

* The language dropdown on the dashboard posted ``set_language``; the server
  wrote the config to ``cwd/config.json`` (not the file it was started with)
  and never switched the session language, so the page stayed in the old
  language until a restart.
* A fresh installation without samples logged a traceback ("No data found for
  device") for every compare request.
* The month projection and the daily average divided by the calendar days of
  the month, so an installation measured for two days looked far too cheap.
"""
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


def _write_cfg(tmp_path: Path, lang: str = "en") -> Path:
    p = tmp_path / "config.json"
    p.write_text(json.dumps({
        "devices": [{"key": "house", "name": "House", "host": "192.0.2.10", "kind": "em"}],
        "ui": {"language": lang},
    }))
    return p


# ── 1: language switch ────────────────────────────────────────────────────────

def test_set_language_writes_the_server_config_and_switches_the_page(tmp_path, monkeypatch):
    from shelly_analyzer.web import create_app

    cfg = _write_cfg(tmp_path)
    elsewhere = tmp_path / "cwd"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    app = create_app(str(cfg))
    c = app.test_client()

    r = c.post("/api/run", json={"action": "set_language", "params": {"language": "fr"}})
    assert r.status_code == 200 and r.get_json()["ok"]

    # persisted where the server reads it, nothing written into the cwd
    assert json.loads(cfg.read_text())["ui"]["language"] == "fr"
    assert not (elsewhere / "config.json").exists()

    # the very next page load is French, without a restart
    state = app.extensions["state"]
    assert state.lang == "fr"
    html = c.get("/").get_data(as_text=True)
    assert 'lang="fr"' in html[:2000]


# ── 3: empty installation ─────────────────────────────────────────────────────

def test_fresh_installation_compare_logs_no_traceback(tmp_path, caplog):
    from shelly_analyzer.io.config import load_config
    from shelly_analyzer.io.storage import NoDataError, Storage
    from shelly_analyzer.web.action_dispatch import ActionDispatcher

    cfg_path = _write_cfg(tmp_path)
    st = Storage(tmp_path / "data")
    with pytest.raises(NoDataError):
        st.read_device_df("house")
    # NoDataError stays a ValueError for every older caller
    assert issubclass(NoDataError, ValueError)

    ad = ActionDispatcher(load_config(cfg_path), st, None, out_dir=tmp_path, cfg_path=cfg_path, lang="en")
    today = datetime.now().date()
    with caplog.at_level(logging.DEBUG):
        out = ad._cmp_load_daily("house", today - timedelta(days=7), today, False, 0.3)
    assert out == {}
    assert not [r for r in caplog.records if r.exc_info], "empty installation must not log a traceback"


# ── 4: daily average / projection divisor ─────────────────────────────────────

def test_month_days_with_data_counts_from_the_first_sample():
    from shelly_analyzer.web.action_dispatch import month_days_with_data

    tz = ZoneInfo("Europe/Berlin")
    now = datetime(2026, 11, 20, 12, 0, tzinfo=tz)
    month_start = datetime(2026, 11, 1, tzinfo=tz)
    # history older than the month: calendar days
    old = int(datetime(2026, 9, 1, tzinfo=tz).timestamp())
    assert month_days_with_data(now, month_start, old) == pytest.approx(19.5)
    # installed on the 18th at noon: two days, not 19.5
    fresh = int(datetime(2026, 11, 18, 12, 0, tzinfo=tz).timestamp())
    assert month_days_with_data(now, month_start, fresh) == pytest.approx(2.0)
    # first hour: never below one day
    first_hour = int(datetime(2026, 11, 20, 11, 30, tzinfo=tz).timestamp())
    assert month_days_with_data(now, month_start, first_hour) == 1.0
    # no data at all: calendar days
    assert month_days_with_data(now, month_start, None) == pytest.approx(19.5)


def test_costs_summary_reports_data_days_for_a_fresh_installation(tmp_path):
    from shelly_analyzer.io.config import load_config
    from shelly_analyzer.io.storage import Storage
    from shelly_analyzer.services.live_history import LiveHistoryRecorder
    from shelly_analyzer.web.action_dispatch import ActionDispatcher

    p = tmp_path / "config.json"
    p.write_text(json.dumps({
        "devices": [{"key": "p0", "name": "Plug", "host": "192.0.2.11", "kind": "switch", "supports_emdata": False}],
        "pricing": {"electricity_price_eur_per_kwh": 0.30},
    }))
    st = Storage(tmp_path / "data")
    now = int(datetime.now().timestamp()) // 60 * 60
    rec = LiveHistoryRecorder(st.db)
    for s in range(0, 6 * 3600 + 1, 30):  # six hours of history
        rec.observe("p0", now - 6 * 3600 + s, 100.0, 230.0, 100.0 / 230.0)
    rec.flush()
    ad = ActionDispatcher(load_config(p), st, None, out_dir=tmp_path, cfg_path=p, lang="en")
    out = ad.dispatch("costs", {})
    assert out.get("ok"), out
    s = out["summary"]
    assert s["data_days"] == pytest.approx(1.0)
    berlin_now = datetime.now(ZoneInfo("Europe/Berlin"))
    calendar_days = (berlin_now - berlin_now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)).total_seconds() / 86400
    assert s["short_history"] is (calendar_days > 1.5)
    # projection = month energy over one day × days of the month (not / calendar days)
    import calendar
    dim = calendar.monthrange(berlin_now.year, berlin_now.month)[1]
    assert s["proj_kwh"] == pytest.approx(round(s["month_kwh"] * dim, 1), abs=0.2)


def test_costs_pane_divides_by_data_days():
    src = (Path(__file__).resolve().parent.parent / "src/shelly_analyzer/services/webdash.py").read_text(encoding="utf-8")
    assert "new Date().getDate());\n    html += _nilmMetricCard('💰'" not in src
    assert "s.data_days" in src and "web.costs.short_history" in src


# ── offline update check ──────────────────────────────────────────────────────

def test_offline_update_check_logs_one_line_without_traceback(tmp_path, monkeypatch, caplog):
    import urllib.error
    from shelly_analyzer.web import create_app
    import shelly_analyzer.web.blueprints.updates as upd

    def _offline(*a, **k):
        raise urllib.error.URLError(ConnectionRefusedError(111, "Connection refused"))

    monkeypatch.setattr(upd, "fetch_releases", _offline)
    upd._releases_cache.clear()
    app = create_app(str(_write_cfg(tmp_path)))
    with caplog.at_level(logging.DEBUG):
        r = app.test_client().get("/api/updates/releases?force=1")
    assert r.status_code in (200, 502, 503)
    recs = [x for x in caplog.records if "updates releases failed" in x.getMessage()]
    assert recs, "the failure is still logged"
    assert not any(x.exc_info for x in recs), "no internet must not log a traceback"


def test_goals_pane_says_by_how_much_a_goal_is_exceeded():
    src = (Path(__file__).resolve().parent.parent / "src/shelly_analyzer/services/webdash.py").read_text(encoding="utf-8")
    assert src.count("t('web.goals.exceeded'") == 2  # weekly and monthly goal
    assert "\\u20ac/W \\u2022" not in src  # the cryptic "€/W • €/M" savings figure is gone
    from shelly_analyzer.i18n import _I18N
    for lang in ("de", "en", "es", "fr", "pt", "it", "pl", "cs", "ru"):
        for key in ("web.goals.exceeded", "web.goals.saved_week"):
            assert "{v}" in _I18N[lang][key], (lang, key)
