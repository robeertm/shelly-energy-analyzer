#!/usr/bin/env python3
"""Fill an EMPTY installation with a plausible demo dataset.

    python3 tools/seed_demo.py /path/to/config.json

Why this exists: every screen of this program is a view onto measurements. A
fresh install shows empty charts, so there is no honest way to show what it
looks like — and the README pictures could only ever be made from Robert's own
house. Those are his electricity bills, his working hours and his holidays,
read off a chart. They do not belong in a public repository, and they cannot be
regenerated when the interface changes.

Two devices over thirteen months, five-minute samples, plus the grid data the
pages draw on: CO2 intensity, spot prices and weather. The random generator is
SEEDED, so the same command always produces the same numbers — a picture taken
today matches one taken next year.

🔴 Refuses to touch a database that already holds samples. Nobody's real
   history gets mixed with invented numbers.

🔴 Everything in here is invented. There is no address, no serial number and no
   real tariff — this file lives in a public repository.
"""
from __future__ import annotations

import datetime as dt
import math
import random
import sys
from pathlib import Path

HIER = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HIER / "src"))

import pandas as pd                                            # noqa: E402

from shelly_analyzer.io.database import EnergyDB               # noqa: E402

# ── Was gebaut wird ─────────────────────────────────────────────────────────
TAGE = 400                 # gut dreizehn Monate: ein volles Jahr im Monatsbild
TAKT_S = 300               # fuenf Minuten — fein genug fuer Tagesverlauf und
                           # Standby, grob genug fuer eine kleine Datei
# 🔴 ZWEI Zaehler und KEINE Photovoltaik — so wie die Anlage, aus der die
#    bisherigen Bilder stammen.
#
#    Erster Wurf: die PV hing hinter dem Hauszaehler. Mittags wurde die Leistung
#    negativ, die Waermekarte liess die Stunden 10–18 GRAU (ein Mittelwert <= 0
#    ist dort „nichts") und die Jahresuebersicht bekam zwei rote Streifen. Das
#    alte, aus echten Daten gemachte Bild hat dort ueberall Farbe.
#
#    Zweiter Wurf: ein eigener Zaehler „PV" mit Erzeugung. Auch falsch — der
#    `pv_meter_device_key` der App ist kein Erzeugungs-, sondern ein NETZzaehler
#    (`kwh < 0` = Einspeisung, `kwh >= 0` = Bezug, Eigenverbrauch = Haushalt
#    minus Bezug). Mit einem Erzeugungszaehler zaehlte die Zusammenfassung die
#    Produktion als Verbrauch mit: 7316 kWh im Jahr statt 3616.
#
#    Ein fremdes Datenmodell halb nachzubauen ist schlechter, als es wegzulassen.
GERAETE = {"shelly1": "Haus", "shelly2": "Server"}

# 🔑 Groesse der Anlage: NACHGERECHNET, nicht geschaetzt. Mit 1200 W Spitze und
#    300 W Grundlast kommt ein Jahr auf rund 3600 kWh Bezug und 1460 kWh
#    Einspeisung — ein Haus mit kleiner Anlage. Mit 3400 W (mein erster Wurf)
#    waren es 4900 kWh Einspeisung gegen 3200 kWh Bezug: das Haus waere
#    Netto-Exporteur gewesen, die Jahresuebersicht blieb leer und die
#    Kostenseite haette Unsinn gezeigt.
GRUNDLAST_W = 300.0


def _tagesform(stunde: float) -> float:
    """Haushaltslast ueber den Tag: Nachtsenke, Morgen- und Abendspitze."""
    morgen = math.exp(-((stunde - 7.2) ** 2) / 2.2)
    abend = math.exp(-((stunde - 19.0) ** 2) / 5.5)
    return 0.32 + 0.9 * morgen + 1.25 * abend


def _jahresform(tag_im_jahr: int) -> float:
    """Winter kostet mehr: Licht, Heizungspumpe, kuerzere Tage."""
    return 1.0 + 0.30 * math.cos(2 * math.pi * (tag_im_jahr - 10) / 365.0)


def _sonne(stunde: float, tag_im_jahr: int) -> float:
    """PV-Ertrag, 0..1 — im Sommer laenger und hoeher.

    🔴 Das Vorzeichen ist der ganze Punkt: Tag 172 ist die Sonnenwende, dort
       muss `laenge` GROSS sein (7,7 h statt 1,5 h). Mein erster Wurf hatte ein
       `* -1` am Kosinus und liess die Anlage im Winter am laengsten laufen.
    """
    laenge = 4.6 + 3.1 * math.cos(2 * math.pi * (tag_im_jahr - 172) / 365.0)
    if abs(stunde - 13.0) > laenge:
        return 0.0
    return max(0.0, math.cos(math.pi * (stunde - 13.0) / (2 * laenge))) ** 1.6


def _wolken(t: dt.datetime) -> float:
    """Wieviel von der moeglichen Sonne kommt an diesem Tag an — 0,12 bis 1,0.

    🔑 Ohne das liefert die Anlage 1570 kWh je kWp im Jahr — das waere
       Suedspanien. Mit einem Tageswert fuer die Bewoelkung sind es 963, also
       der deutsche Durchschnitt. NACHGERECHNET, nicht geschaetzt.
    🔑 Derselbe Wert faerbt auch `clouds_pct` im Wetter, damit Wetterseite und
       Solarseite nicht zwei verschiedene Tage beschreiben.
    """
    r = random.Random(555 + t.toordinal())
    tij = t.timetuple().tm_yday
    klar = 0.45 + 0.25 * math.cos(2 * math.pi * (tij - 172) / 365.0)
    return max(0.12, min(1.0, r.betavariate(2.2, 1.6) * (0.55 + klar)))


def _laedt_heute(t: dt.datetime) -> bool:
    """Laedt das Auto an diesem Abend? Rund zwei Tage je Woche, gestreut."""
    return random.Random(90210 + t.toordinal()).random() < 2.0 / 7.0


def baue_geraet(rng: random.Random, schluessel: str, beginn: dt.datetime,
                takte: int) -> pd.DataFrame:
    """Ein Geraet, Takt fuer Takt. Drei Phasen, weil der Shelly 3EM drei misst."""
    zeilen = []
    for i in range(takte):
        t = beginn + dt.timedelta(seconds=i * TAKT_S)
        std = t.hour + t.minute / 60.0
        tij = t.timetuple().tm_yday

        if schluessel == "shelly1":
            leistung = GRUNDLAST_W * _tagesform(std) * _jahresform(tij)
            # Kochen, Waschmaschine, Backofen — kurze, kraeftige Spitzen.
            if rng.random() < 0.015:
                leistung += rng.uniform(900, 2400)
            # Waermepumpe laeuft im Winter oefter an.
            if rng.random() < 0.05 * _jahresform(tij):
                leistung += rng.uniform(400, 900)
            # 🔑 Das Auto laedt an rund zwei Abenden je Woche — aber an
            #    WECHSELNDEN. `t.toordinal() % 7` traefe jede Woche denselben
            #    Wochentag und malte zwei rote Streifen quer durch das Jahr.
            if 18 <= std < 23 and _laedt_heute(t):
                leistung += 3600
        else:
            # Server: fast flach, mit leichter Last am Tag.
            leistung = 118.0 + 26.0 * _tagesform(std) * 0.5 + rng.gauss(0, 4.0)
            if rng.random() < 0.004:
                leistung += rng.uniform(60, 180)

        leistung += rng.gauss(0, 9.0)
        # Auf drei Phasen verteilen — nie exakt gleich, so sieht keine Anlage aus.
        a = leistung * rng.uniform(0.30, 0.40)
        bq = leistung * rng.uniform(0.28, 0.38)
        cq = leistung - a - bq
        spannung = lambda: 231.0 + rng.gauss(0, 1.4)             # noqa: E731
        strom = lambda w: abs(w) / 230.0                          # noqa: E731
        wh = leistung * TAKT_S / 3600.0

        zeilen.append({
            "timestamp": t,
            "a_act_power": round(a, 1), "b_act_power": round(bq, 1), "c_act_power": round(cq, 1),
            "a_voltage": round(spannung(), 1), "b_voltage": round(spannung(), 1),
            "c_voltage": round(spannung(), 1),
            "a_current": round(strom(a), 3), "b_current": round(strom(bq), 3),
            "c_current": round(strom(cq), 3),
            "a_total_act_energy": round(max(0.0, a) * TAKT_S / 3600.0, 3),
            "b_total_act_energy": round(max(0.0, bq) * TAKT_S / 3600.0, 3),
            "c_total_act_energy": round(max(0.0, cq) * TAKT_S / 3600.0, 3),
            "a_total_act_ret_energy": round(max(0.0, -a) * TAKT_S / 3600.0, 3),
            "b_total_act_ret_energy": round(max(0.0, -bq) * TAKT_S / 3600.0, 3),
            "c_total_act_ret_energy": round(max(0.0, -cq) * TAKT_S / 3600.0, 3),
            "n_avg_current": round(abs(a - bq) / 230.0, 3),
            "freq_hz": round(50.0 + rng.gauss(0, 0.012), 3),
            "total_power": round(leistung, 1),
            "energy_kwh": round(wh / 1000.0, 6),
        })
    return pd.DataFrame(zeilen)


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__.strip().splitlines()[2].strip())
        return 2
    cfg = Path(sys.argv[1]).resolve()
    # 🔴 Die Ablage liegt NICHT neben der Konfiguration, sondern in `data/`
    #    darunter — `web/__init__.py` baut sie als `Storage(base_dir=out_dir /
    #    "data")`. Mein erster Lauf schrieb 230 000 Messwerte daneben; die
    #    Oberflaeche zeigte eine leere Waermekarte und hatte recht damit.
    basis = cfg.parent / "data"
    basis.mkdir(parents=True, exist_ok=True)
    # 🔑 Die Zonen stehen in der Konfiguration — und sie sind NICHT gleich
    #    geschrieben: CO2 „DE_LU", Boersenpreise „DE-LU". Wer eine davon raet,
    #    schreibt Zeilen, die nie jemand abfragt.
    import json as _json
    _k = _json.loads(cfg.read_text(encoding="utf-8"))
    zone_co2 = str((_k.get("co2") or {}).get("bidding_zone") or "DE_LU")
    zone_preis = str((_k.get("spot_price") or {}).get("bidding_zone") or "DE-LU")
    db = EnergyDB(basis / "energy.db")

    # 🔴 Erst fragen, dann schreiben.
    belegt = [k for k in GERAETE if db.has_data(k)]
    if belegt:
        print("🔴 ABBRUCH: in %s liegen schon Messwerte (%s)."
              % (basis / "energy.db", ", ".join(belegt)))
        print("   Dieser Generator schreibt NUR in eine leere Ablage — er soll")
        print("   keine echten Daten mit erfundenen vermischen.")
        return 1

    ende = dt.datetime.now().replace(minute=0, second=0, microsecond=0)
    beginn = ende - dt.timedelta(days=TAGE)
    takte_pro_tag = 86400 // TAKT_S

    gesamt = 0
    for schluessel, name in GERAETE.items():
        rng = random.Random(4711 + sum(ord(c) for c in schluessel))
        geschrieben = 0
        # In Monatsstuecken, damit nie ein 100-MB-DataFrame im Speicher liegt.
        tag = 0
        while tag < TAGE:
            stueck = min(30, TAGE - tag)
            df = baue_geraet(rng, schluessel,
                             beginn + dt.timedelta(days=tag),
                             stueck * takte_pro_tag)
            geschrieben += db.insert_dataframe(schluessel, df)
            tag += stueck
            print("  %-9s %4d/%d Tage" % (name, tag, TAGE), end="\r", flush=True)
        db.save_meta(schluessel, int(ende.timestamp()), int(ende.timestamp()))
        print("  %-9s %d Messwerte            " % (name, geschrieben))
        gesamt += geschrieben

    # ── Netzdaten, die die Seiten mitzeichnen ───────────────────────────────
    rng = random.Random(1848)
    co2, preise, wetter = [], [], []
    jetzt = int(dt.datetime.now().timestamp())
    stunde = beginn.replace(minute=0, second=0, microsecond=0)
    while stunde <= ende:
        ts = int(stunde.timestamp())
        h = stunde.hour
        tij = stunde.timetuple().tm_yday
        # Mittags viel Sonne im Netz → weniger CO2 je kWh.
        g = 420 - 150 * _sonne(h, tij) - 40 * math.cos(2 * math.pi * (tij - 172) / 365)
        co2.append((ts, zone_co2, round(max(90.0, g + rng.gauss(0, 18)), 1), "demo", jetzt))
        # Boersenpreis folgt grob dem Gegenteil der Sonne.
        p = 78 + 46 * _tagesform(h) - 55 * _sonne(h, tij) + rng.gauss(0, 9)
        preise.append((ts, zone_preis, round(p, 2), 3600, "demo", jetzt))
        temp = 10.5 - 9.0 * math.cos(2 * math.pi * (tij - 20) / 365) \
            + 4.0 * math.sin(math.pi * (h - 4) / 16) + rng.gauss(0, 1.1)
        # 🔑 Bewoelkung aus DERSELBEN Quelle wie der PV-Ertrag.
        bedeckt = round(100.0 * (1.0 - _wolken(stunde)), 1)
        wetter.append((ts, round(temp, 1), round(rng.uniform(45, 92), 1),
                       round(abs(rng.gauss(2.6, 1.3)), 1), bedeckt,
                       round(rng.gauss(1013, 7), 1), "demo", jetzt))
        stunde += dt.timedelta(hours=1)

    db.upsert_co2_intensity(co2)
    db.upsert_spot_prices(preise)
    db.upsert_weather(wetter)
    db.close()

    print("Fertig: %d Messwerte, %d Stunden CO2 (%s) / Preis (%s) / Wetter in %s"
          % (gesamt, len(co2), zone_co2, zone_preis, basis / "energy.db"))
    print("Alles erfunden — keine echte Anlage, kein echter Tarif, keine Adresse.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
