# -*- coding: utf-8 -*-
"""Telegram-Text und HTML-Mail in der eingestellten Sprache.

🔴 Robert, 27.09.2026: „warum werden die reports auf englisch erstellt wenn
man deutsch eingestellt hat in der app?" — `background.py` hatte bis 17.5.1
**null** Übersetzungsaufrufe. Jeder Text stand als englisches Literal im
Quelltext, und `strftime("%A")` lieferte den Wochentag in der C-Locale des
Containers, also ebenfalls englisch.

Diese Proben prüfen, was wirklich herauskommt — im Quelltext sieht man den
Fehler nie, die Zeilen sind ja alle richtig.
"""
from __future__ import annotations

import datetime as _dt
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.i18n import LANGS  # noqa: E402
from shelly_analyzer.web.background import BackgroundServiceManager  # noqa: E402


def dienst(lang: str) -> BackgroundServiceManager:
    """Nur die Formatierer, ohne den ganzen Dienst hochzufahren."""
    class Ui:
        language = lang

    class Cfg:
        ui = Ui()

    s = BackgroundServiceManager.__new__(BackgroundServiceManager)
    s.cfg = Cfg()
    return s


TAG = {
    "date_label": "Freitag, 25.09.2026", "unit_price": 0.3287,
    "total_kwh": 8.43, "total_cost": 2.77, "total_prev": 9.59,
    "total_same_wd": 9.70, "avg_w": 351.0,
    "dev_data": [{"name": "Haus", "kwh": 7.34, "cost": 2.41, "share_pct": 87.0,
                  "delta_pct": -13.0, "peak_w": 1633.0, "peak_hour": 12}],
    "biggest_mover": {"name": "Haus", "from": 8.44, "to": 7.34, "diff": -1.10},
    "peak_h": 12, "peak_h_kwh": 1.72, "top_hours": [(12, 1.72)],
    "low_hours": [(23, 0.07)], "max_power_w": 1633.0, "max_power_hour": 12,
    "load_factor": 0.215, "standby_w": 147.0, "standby_annual_kwh": 1288.0,
    "standby_annual_cost": 423.0, "co2_kg": 2.5, "co2_g_per_kwh": 292.0,
    "spot_cost": 1.58, "proj_kwh": 253.0, "proj_cost": 83.0,
    "hourly_total": [0.15] * 24,
}

MONAT = {
    "month_label": "August 2026", "days_in_month": 31, "unit_price": 0.3287,
    "total_kwh": 261.0, "total_cost": 85.79, "total_prev": 248.0,
    "avg_daily": 8.42, "avg_daily_cost": 2.77,
    "dev_data": [{"name": "Haus", "kwh": 221.0, "cost": 72.6,
                  "share_pct": 85.0, "delta_pct": 6.0}],
    "biggest_mover": {"name": "Haus", "from": 205.0, "to": 221.0, "diff": 16.0},
    "best_day": 14, "best_day_kwh": 5.1, "worst_day": 3, "worst_day_kwh": 12.8,
    "weekday_avgs": [8.1, 8.0, 8.3, 8.2, 8.9, 9.4, 9.1],
    "wd_labels": ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"],
    "wkday_avg": 8.3, "wkend_avg": 9.25, "peak_hour_idx": 19,
    "peak_hour_kwh": 21.4, "co2_kg": 76.2, "co2_g_per_kwh": 292.0,
    "year_proj": 3074.0, "year_cost": 1010.0,
    "daily_totals": {1: 8.2, 2: 9.1, 3: 12.8},
}

BAUER = [("_format_daily_text", TAG), ("_format_monthly_text", MONAT),
         ("_format_daily_html", TAG), ("_format_monthly_html", MONAT)]


def _sichtbar(text: str) -> str:
    """Bei HTML nur den Text, nicht die Auszeichnung."""
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", text))


@pytest.mark.parametrize("bauer,daten", BAUER)
def test_deutscher_auszug_ist_deutsch(bauer, daten):
    txt = _sichtbar(getattr(dienst("de"), bauer)(daten))
    for muss in ("Gesamt", "Geräte"):
        assert muss in txt, "fehlt auf Deutsch: %r" % muss
    for darf_nicht in ("Total", "Devices", "Device ranking", "Best day",
                       "Biggest mover", "Night base load", "Year projection"):
        assert darf_nicht not in txt, "noch englisch: %r" % darf_nicht


@pytest.mark.parametrize("bauer,daten", BAUER)
def test_die_probe_wuerde_den_alten_stand_rot_machen(bauer, daten):
    """Gegenprobe: auf Englisch MUSS genau das drinstehen, was vorher dastand."""
    txt = _sichtbar(getattr(dienst("en"), bauer)(daten))
    assert "Total" in txt
    assert ("Devices" in txt) or ("Device ranking" in txt)


@pytest.mark.parametrize("bauer,daten", BAUER)
def test_zahlen_folgen_der_sprache(bauer, daten):
    de = _sichtbar(getattr(dienst("de"), bauer)(daten))
    en = _sichtbar(getattr(dienst("en"), bauer)(daten))
    assert "8,43" in de or "261,0" in de or "85,79" in de
    assert "8.43" in en or "261.0" in en or "85.79" in en
    # 1 633 W: deutscher Tausenderpunkt gegen englisches Komma
    assert ("1.633" in de) or ("3.074" in de)
    assert ("1,633" in en) or ("3,074" in en)


@pytest.mark.parametrize("lang", list(LANGS))
@pytest.mark.parametrize("bauer,daten", BAUER)
def test_kein_platzhalter_bleibt_stehen(lang, bauer, daten):
    """🔴 `t()` formatiert mit `str.format`.

    Ein Platzhalter, den eine Übersetzung anders schreibt als das Englische
    ({wert} statt {v}), wird nicht ersetzt und steht roh in der Mail. Genau
    das sieht man im Quelltext nie.
    """
    txt = getattr(dienst(lang), bauer)(daten)
    offen = re.findall(r"\{[a-z_]+\}", txt)
    assert not offen, "%s/%s: unersetzte Platzhalter %r" % (lang, bauer, offen)


@pytest.mark.parametrize("lang", list(LANGS))
def test_wochentag_und_monat_folgen_der_sprache(lang):
    """Der Wochentag kam aus `strftime`, also aus der C-Locale des Containers.

    Das ist der Grund, warum im deutschen Bericht „Friday, 25.09.2026" stand.
    """
    from shelly_analyzer.i18n import (month_name_local, weekday_name_local,
                                      weekday_names_local)
    tag = _dt.date(2026, 9, 25)                     # ein Freitag
    assert weekday_name_local(lang, tag)
    assert month_name_local(lang, 9)
    assert len(weekday_names_local(lang)) == 7
    if lang == "de":
        assert weekday_name_local(lang, tag) == "Freitag"
        assert month_name_local(lang, 9) == "September"
    if lang != "en":
        assert weekday_name_local(lang, tag) != "Friday"


@pytest.mark.parametrize("bauer,daten", BAUER)
def test_keine_englische_zahl_im_deutschen_auszug(bauer, daten):
    """Dieselbe Regel wie im PDF: ein Punkt mit ein oder zwei Ziffern
    dahinter, direkt vor einer Einheit. Der deutsche Tausenderpunkt hat
    immer drei ("1.633 W"), ein Datum steht nie vor einer Einheit."""
    txt = _sichtbar(getattr(dienst("de"), bauer)(daten))
    muster = re.compile(r"\d\.\d{1,2}\s*(?:%|kWh|€|W\b|ct|kg|g/kWh)")
    schlecht = muster.findall(txt)
    assert not schlecht, "%s: englische Schreibweise %r" % (bauer, schlecht)


@pytest.mark.parametrize("bauer,daten", BAUER)
def test_die_zahlenprobe_merkt_eine_englische_zahl(bauer, daten):
    """Gegenprobe: derselbe Auszug auf Englisch MUSS auffallen."""
    txt = _sichtbar(getattr(dienst("en"), bauer)(daten))
    muster = re.compile(r"\d\.\d{1,2}\s*(?:%|kWh|€|W\b|ct|kg|g/kWh)")
    assert muster.search(txt), "%s: die Probe prueft nichts" % bauer
