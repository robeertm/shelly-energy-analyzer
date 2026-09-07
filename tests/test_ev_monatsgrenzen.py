# -*- coding: utf-8 -*-
"""Die Monats-Voreinstellungen des EV-Logs — ausgeführt, nicht gelesen.

Robert: „letzter monat soll wirklich vorhergehender monat sein … immer volle
monate nicht überlappen!"

Monatsarithmetik bricht an genau drei Stellen: am Jahreswechsel, im Februar und
im Schaltjahr. Ein Test, der nur den Quelltext liest, sieht davon nichts — also
wird die echte JS-Funktion in node ausgeführt, mit festgehaltener Uhr.
"""
import json
import os
import re
import subprocess
import sys

HIER = os.path.dirname(__file__)
QUELLE = os.path.join(HIER, "..", "src", "shelly_analyzer", "services", "webdash.py")


def _funktionen() -> str:
    """Holt `_evIso` und `_evPresetRange` im Original aus der Vorlage.

    Die Vorlage ist ein Python-Format-String: doppelte Klammern sind dort
    maskiert und werden beim Ausliefern zu einfachen. Genau das wird hier
    rückgängig gemacht, damit wirklich der ausgelieferte Code läuft.
    """
    quelle = open(QUELLE, encoding="utf-8").read()
    aus = []
    for name in ("_evIso", "_evPresetRange"):
        m = re.search(r"\n( *)function " + name + r"\(.*?\n\1\}\}", quelle, re.S)
        assert m, "Funktion %s nicht gefunden" % name
        block = "\n".join(z[len(m.group(1)):] for z in m.group(0).strip("\n").split("\n"))
        aus.append(block.replace("{{", "{").replace("}}", "}"))
    return "\n".join(aus)


def _laufe(heute: str) -> dict:
    """Führt die echten Funktionen mit `heute` als Systemdatum aus."""
    # Die Uhr wird als PARAMETER hereingereicht, nicht global ueberschrieben:
    # `class Date extends Date` landet sonst in der temporalen Todeszone.
    js = """
const _ECHT = Date;
function _uhr(iso) {
  return class extends _ECHT {
    constructor(...a) { if (a.length === 0) { super(iso + 'T12:00:00'); } else { super(...a); } }
    static now() { return new _ECHT(iso + 'T12:00:00').getTime(); }
  };
}
const aus = (function (Date) {
%s
  const o = {};
  for (const n of ['week', 'month_now', 'month_prev', 'm3', 'm6', 'custom']) {
    o[n] = _evPresetRange(n);
  }
  return o;
})(_uhr('%s'));
console.log(JSON.stringify(aus));
""" % (_funktionen(), heute)
    r = subprocess.run(["node", "-e", js], capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr[:800]
    return json.loads(r.stdout)


def _letzter_tag(iso: str) -> bool:
    import calendar
    j, m, tg = (int(x) for x in iso.split("-"))
    return tg == calendar.monthrange(j, m)[1]


FAELLE = {
    # Normalfall
    "2026-09-07": {"month_now": ("2026-09-01", "2026-09-07"),
                   "month_prev": ("2026-08-01", "2026-08-31"),
                   "m3": ("2026-06-01", "2026-08-31"),
                   "m6": ("2026-03-01", "2026-08-31")},
    # 🔴 Jahreswechsel: der Vormonat liegt im Vorjahr
    "2026-01-15": {"month_now": ("2026-01-01", "2026-01-15"),
                   "month_prev": ("2025-12-01", "2025-12-31"),
                   "m3": ("2025-10-01", "2025-12-31"),
                   "m6": ("2025-07-01", "2025-12-31")},
    # 🔴 Februar, kein Schaltjahr
    "2026-03-01": {"month_now": ("2026-03-01", "2026-03-01"),
                   "month_prev": ("2026-02-01", "2026-02-28")},
    # 🔴 Februar im Schaltjahr
    "2024-03-05": {"month_now": ("2024-03-01", "2024-03-05"),
                   "month_prev": ("2024-02-01", "2024-02-29")},
    # 🔴 Monatsletzter als „heute" — der laufende Monat ist dann voll
    "2026-01-31": {"month_now": ("2026-01-01", "2026-01-31"),
                   "month_prev": ("2025-12-01", "2025-12-31")},
}


def test_monatsgrenzen_stimmen():
    for heute, erwartet in FAELLE.items():
        r = _laufe(heute)
        for name, (a, b) in erwartet.items():
            assert r[name] == {"start": a, "end": b}, (heute, name, r[name], (a, b))
    print("OK  %d Stichtage: Jahreswechsel, Februar, Schaltjahr, Monatsletzter" % len(FAELLE))


def test_immer_volle_monate():
    """Jede Voreinstellung außer dem laufenden Monat endet auf einem Monatsletzten."""
    for heute in FAELLE:
        r = _laufe(heute)
        for name in ("month_prev", "m3", "m6"):
            a, b = r[name]["start"], r[name]["end"]
            assert a.endswith("-01"), (heute, name, "beginnt nicht am Monatsersten", a)
            assert _letzter_tag(b), (heute, name, "endet nicht am Monatsletzten", b)
        assert r["month_now"]["start"].endswith("-01"), (heute, r["month_now"])
    print("OK  volle Monate: Anfang am Ersten, Ende am Letzten")


def test_kein_ueberlappen_mit_dem_laufenden_monat():
    """🔴 Der eigentliche Auftrag: der Vormonat darf NICHT in diesen hineinreichen.

    Vorher war „Monat" ein rollendes 30-Tage-Fenster — am 7.9. also 8.8.–7.9.,
    also quer über beide Monate. Genau das soll nicht mehr sein.
    """
    import datetime as dt
    for heute in FAELLE:
        r = _laufe(heute)
        jetzt_a = dt.date.fromisoformat(r["month_now"]["start"])
        vor_b = dt.date.fromisoformat(r["month_prev"]["end"])
        assert vor_b < jetzt_a, (heute, "Vormonat reicht in den laufenden hinein", vor_b, jetzt_a)
        assert vor_b + dt.timedelta(days=1) == jetzt_a, (heute, "Lücke zwischen den Monaten")
        # die längeren Fenster enden ebenfalls vor dem laufenden Monat
        for name in ("m3", "m6"):
            assert dt.date.fromisoformat(r[name]["end"]) < jetzt_a, (heute, name)
            assert dt.date.fromisoformat(r[name]["start"]) <= dt.date.fromisoformat(r["month_prev"]["start"])
    print("OK  kein Fenster reicht in den laufenden Monat hinein")


def test_woche_und_custom_rechnen_keinen_zeitraum():
    """Die Woche bleibt ein rollendes Fenster, „custom" kommt aus den Feldern."""
    r = _laufe("2026-09-07")
    assert r["week"] is None, r["week"]
    assert r["custom"] is None, r["custom"]
    print("OK  Woche und eigener Zeitraum laufen nicht über die Monatsrechnung")


def test_leiste_und_uebersetzungen():
    quelle = open(QUELLE, encoding="utf-8").read()
    i = quelle.index("function renderEvLog(")
    js = quelle[i:i + 4000]
    for name in ("'week'", "'month_now'", "'month_prev'", "'m3'", "'m6'"):
        assert "winBtn(" + name in js, name
    assert "web.ev.win_month_now" in js and "web.ev.win_month_prev" in js
    assert "web.ev.win_month'" not in js, "der alte Sammel-Schlüssel wird noch benutzt"
    sys.path.insert(0, os.path.join(HIER, "..", "src"))
    from shelly_analyzer.i18n import t, LANGS
    for k in ("web.ev.win_month_now", "web.ev.win_month_prev"):
        werte = [t(l, k) for l in LANGS]
        assert all(w and not w.startswith("web.ev.") for w in werte), (k, werte)
        assert len(set(werte)) >= 7, ("zu viele Sprachen teilen einen Text", k, werte)
    print("OK  Leiste hat die 5 Voreinstellungen, beide Monats-Schlüssel in %d Sprachen" % len(LANGS))


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items())
           if k.startswith("test_") and callable(v)]
    for f in fns:
        f()
    print("\n%d Tests bestanden" % len(fns))
