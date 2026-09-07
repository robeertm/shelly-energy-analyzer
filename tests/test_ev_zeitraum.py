"""Die Zeitraum-Wahl im EV-Log: vier Voreinstellungen plus ein eigener Zeitraum.

Robert: „nicht die letzten 7d 30d 90d anzeigen sondern letzte woche, monat
3 monate 6 monate und custom".

Die beiden Fensterformen cachen unterschiedlich, und genau daran kann man sie
falsch bauen:

  • rollend  (``?days=N``) — der Start wandert jede Sekunde. Der Cache MUSS
    deshalb über die LÄNGE gehen, sonst trifft er nie.
  • explizit (``?start=&end=``) — feste Grenzen, also über die Grenzen.

Die Tests binden die echten Methoden an schlanke Attrappen (Stil aus
``test_ev_window_cache.py``), damit die echte Logik läuft und nicht ein Nachbau.
"""
import os
import sys
import threading
import types

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.web.action_dispatch import ActionDispatcher  # noqa: E402


class _FakeDB:
    def __init__(self, max_ts):
        self._max_ts = max_ts

    def max_timestamp(self, device_key):
        return self._max_ts


class _FakeStorage:
    def __init__(self, max_ts):
        self.db = _FakeDB(max_ts)
        self.aufrufe = []          # (start_ts, end_ts) je echtem Lesevorgang

    def read_device_df(self, device_key, start_ts=None, end_ts=None):
        self.aufrufe.append((start_ts, end_ts))
        return pd.DataFrame({"timestamp": [start_ts or 0], "total_power": [7000.0]})


def _mache(max_ts=1_700_000_000):
    obj = types.SimpleNamespace()
    obj.storage = _FakeStorage(max_ts)
    obj.live_store = None
    obj._ev_window_cache = {}
    obj._ev_window_lock = threading.Lock()
    obj._ev_window_ttl = 900.0
    obj._ev_read_window_df = types.MethodType(ActionDispatcher._ev_read_window_df, obj)
    return obj


def test_rollendes_fenster_cacht_ueber_die_laenge():
    """Zweimal dasselbe rollende Fenster = EIN Lesevorgang."""
    d = _mache()
    d._ev_read_window_df("wallbox", 30)
    d._ev_read_window_df("wallbox", 30)
    assert len(d.storage.aufrufe) == 1, d.storage.aufrufe
    # ...und ein anderes Fenster liest neu, statt das alte auszuliefern.
    d._ev_read_window_df("wallbox", 180)
    assert len(d.storage.aufrufe) == 2, d.storage.aufrufe
    print("OK  rollendes Fenster: gleiche Länge trifft den Cache, andere nicht")


def test_expliziter_zeitraum_liest_mit_beiden_grenzen():
    """Der eigene Zeitraum muss ein ENDE mitgeben — sonst ist er kein Zeitraum."""
    d = _mache()
    a, b = 1_600_000_000, 1_600_864_000
    d._ev_read_window_df("wallbox", 10, a, b)
    assert d.storage.aufrufe == [(a, b)], d.storage.aufrufe
    d._ev_read_window_df("wallbox", 10, a, b)
    assert len(d.storage.aufrufe) == 1, "identischer Zeitraum sollte cachen"
    print("OK  expliziter Zeitraum: liest start UND end, cacht über die Grenzen")


def test_zeitraum_und_rollendes_fenster_werden_nicht_verwechselt():
    """🔴 Die eigentliche Falle: gleiche Tageszahl, verschiedene Fenster.

    Ein eigener 10-Tage-Zeitraum und das rollende 10-Tage-Fenster hätten unter
    einem gemeinsamen Schlüssel dieselben Daten geliefert — der Nutzer sieht
    dann irgendein Fenster, nur nicht seines.
    """
    d = _mache()
    d._ev_read_window_df("wallbox", 10)                                  # rollend
    d._ev_read_window_df("wallbox", 10, 1_600_000_000, 1_600_864_000)    # eigen
    assert len(d.storage.aufrufe) == 2, d.storage.aufrufe
    assert d.storage.aufrufe[0][1] is None, "das rollende Fenster hat kein Ende"
    assert d.storage.aufrufe[1][1] == 1_600_864_000
    print("OK  beide Formen haben getrennte Cache-Schlüssel")


def _tag_parser():
    """Holt die Datumsauswertung aus dem Quelltext des Dispatchers heraus.

    Sie ist dort eine lokale Hilfsfunktion; nachbauen würde eine zweite
    Wahrheit schaffen. Also die echte Definition ausführen.
    """
    import re
    pfad = os.path.join(os.path.dirname(__file__), "..", "src", "shelly_analyzer",
                        "web", "action_dispatch.py")
    quelle = open(pfad, encoding="utf-8").read()
    m = re.search(r"\n( +)def _tag_zu_ts\(text, ende=False\):.*?\n\1    return int\(d\.timestamp\(\)\)",
                  quelle, re.S)
    assert m, "Datumsauswertung im Dispatcher nicht gefunden"
    block = "\n".join(z[len(m.group(1)):] for z in m.group(0).strip("\n").split("\n"))
    raum = {}
    exec(block, {}, raum)
    return raum["_tag_zu_ts"]


def test_datumsauswertung():
    f = _tag_parser()
    import datetime as dt
    a = f("2026-03-01")
    b = f("2026-03-01", ende=True)
    assert b - a == 86400, (a, b)
    assert dt.datetime.fromtimestamp(a).strftime("%Y-%m-%d") == "2026-03-01"
    for müll in (None, "", "morgen", "2026-13-45", "01.03.2026"):
        assert f(müll) is None, müll
    print("OK  Datumsauswertung: Ende ist Tagesende, Unsinn ergibt None")


def test_oberflaeche_hat_den_eigenen_zeitraum():
    """Nur das, was DIESE Datei besitzt: die Von/Bis-Felder und dass die alten
    Tageszahl-Labels weg sind. Die Voreinstellungen selbst prüft
    ``test_ev_monatsgrenzen.py`` — dort laufen sie wirklich."""
    pfad = os.path.join(os.path.dirname(__file__), "..", "src", "shelly_analyzer",
                        "services", "webdash.py")
    ganz = open(pfad, encoding="utf-8").read()
    i = ganz.index("function renderEvLog(")
    js = ganz[i:i + 4000]
    for alt_label in ("'7d'", "'30d'", "'90d'", "'1y'"):
        assert alt_label not in js, "altes Kurzlabel %s noch in der EV-Leiste" % alt_label
    assert "ev-cust-from" in js and "ev-cust-to" in js, "Datumsfelder fehlen"
    assert "web.ev.win_custom" in js
    assert "evApplyCustom" in ganz and "evToggleCustom" in ganz
    assert "evSetWindow" not in ganz, "der alte Tageszahl-Umschalter lebt noch"
    print("OK  eigener Zeitraum: Von/Bis-Felder da, keine Tageszahl-Labels mehr")


def test_uebersetzt_in_allen_sprachen():
    """🔴 Lehre aus früher: ein fehlender Schlüssel fällt still auf Englisch zurück."""
    from shelly_analyzer.i18n import t, LANGS
    neu = ("web.ev.win_week", "web.ev.win_month_now", "web.ev.win_month_prev",
           "web.ev.win_3months", "web.ev.win_6months", "web.ev.win_custom",
           "web.ev.from", "web.ev.to", "web.ev.apply")
    englisch = [t("en", k) for k in neu]
    for lang in LANGS:
        werte = [t(lang, k) for k in neu]
        assert all(w and not w.startswith("web.ev.") for w in werte), (lang, werte)
        if lang != "en":
            assert werte != englisch, "%s fällt komplett auf Englisch zurück" % lang
    print("OK  8 Schlüssel in allen %d Sprachen übersetzt" % len(LANGS))


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items())
           if k.startswith("test_") and callable(v)]
    for f in fns:
        f()
    print("\n%d Tests bestanden" % len(fns))
