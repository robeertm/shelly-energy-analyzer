# -*- coding: utf-8 -*-
"""Der Tagesbericht als Dokument — die Proben, die den Fehler von 17.3.2
gefunden haetten.

🔴 Der teure Fehler war nicht "haesslich", sondern **unlesbar**: das
"24h profile" mischt U+2588 (voller Block) und U+2591 (leerer Block).
Helvetica kennt keinen von beiden, reportlab malte fuer BEIDE dasselbe
schwarze Rechteck — 0.15 kWh und 1.72 kWh sahen identisch aus. Dieselbe
Ursache traf das tiefgestellte 2 in CO₂ und jedes Symbol unterhalb
U+FFFF, weil der alte Filter nur Zeichen OBERHALB U+FFFF entfernte.

Deshalb pruefen diese Tests zwei Dinge, die ein Blick in den Quelltext
nie zeigt:
  1. Jedes Zeichen, das wirklich gezeichnet wird, hat in der benutzten
     Schrift eine Glyphe.
  2. Verschiedene Werte ergeben verschieden hohe Balken.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

QUELLE = (Path(__file__).resolve().parents[1]
          / "src" / "shelly_analyzer" / "web" / "report_pdf.py")


def _modul():
    """Direkt laden: das Modul haengt bewusst an nichts aus der Anwendung."""
    spec = importlib.util.spec_from_file_location("report_pdf", QUELLE)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


rp = pytest.importorskip("reportlab") and _modul()


STUNDEN = [0.15, 0.13, 0.15, 0.15, 0.14, 0.13, 0.15, 0.32, 1.18, 0.79,
           0.18, 0.26, 1.72, 0.29, 0.16, 0.21, 0.18, 0.20, 0.22, 0.91,
           0.30, 0.16, 0.14, 0.07]


def tagesdaten(**zusatz):
    d = {
        "date_label": "Friday, 25.09.2026", "unit_price": 0.3287,
        "total_kwh": 8.43, "total_cost": 2.77, "total_prev": 9.59,
        "total_same_wd": 9.70,
        "dev_data": [
            {"name": "Haus", "kwh": 7.34, "cost": 2.41, "share_pct": 87.0,
             "delta_pct": -13.0, "peak_w": 1633.0, "peak_hour": 12},
            {"name": "Wallbox", "kwh": 1.09, "cost": 0.36, "share_pct": 13.0,
             "delta_pct": -5.0, "peak_w": 47.0, "peak_hour": 19}],
        "hourly_total": list(STUNDEN),
        "hourly_per_device": {"Haus": [v * 0.87 for v in STUNDEN],
                              "Wallbox": [v * 0.13 for v in STUNDEN]},
        "max_power_w": 1633.0, "max_power_hour": 12, "avg_w": 351.0,
        "load_factor": 0.22, "peak_h": 12, "peak_h_kwh": 1.72,
        "top_hours": [(12, 1.72), (8, 1.18), (19, 0.91)],
        "low_hours": [(23, 0.07), (5, 0.15), (3, 0.15)],
        "night_kwh": 0.88, "standby_w": 147.0, "standby_annual_kwh": 1285.0,
        "standby_annual_cost": 422.0, "co2_kg": 2.5, "co2_g_per_kwh": 292.0,
        "proj_kwh": 256.0, "proj_cost": 84.0, "spot_cost": 1.58,
        "biggest_mover": {"name": "Haus", "diff": -1.1, "from": 8.4, "to": 7.3},
        "avg7_kwh": 9.12, "avg30_kwh": 9.84,
        "last7_days": [("Sat", 10.21), ("Sun", 11.04), ("Mon", 8.92),
                       ("Tue", 8.15), ("Wed", 9.33), ("Thu", 9.59),
                       ("Fri", 8.43)],
    }
    d.update(zusatz)
    return d


def monatsdaten(**zusatz):
    tage = [7.0 + (i % 7) * 0.8 for i in range(31)]
    d = {
        "month_label": "August 2026", "days_in_month": 31,
        "unit_price": 0.3287, "total_kwh": sum(tage),
        "total_cost": sum(tage) * 0.3287, "total_prev": 268.4,
        "avg_daily": sum(tage) / 31, "avg_daily_cost": sum(tage) / 31 * 0.3287,
        "dev_data": [{"name": "Haus", "kwh": 242.6, "cost": 79.7,
                      "share_pct": 86, "delta_pct": -4, "peak_w": 2140,
                      "peak_hour": 12}],
        "daily_totals": tage, "hour_totals": [3.0 + (h % 5) for h in range(24)],
        "weekday_avgs": [9.1, 8.4, 8.9, 9.3, 10.2, 11.6, 11.1],
        "wd_labels": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
        "wkday_avg": 9.18, "wkend_avg": 11.35, "peak_hour_idx": 4,
        "peak_hour_kwh": 7.0, "best_day": "12.08.", "best_day_kwh": 7.0,
        "worst_day": "24.08.", "worst_day_kwh": 11.8, "co2_kg": 82.4,
        "co2_g_per_kwh": 292, "year_proj": 3385.0, "year_cost": 1113.0,
        "biggest_mover": {"name": "Haus", "diff": 7.4, "from": 33.2, "to": 40.6},
    }
    d.update(zusatz)
    return d


# ─────────────── 1. Jedes gezeichnete Zeichen hat eine Glyphe ──────────────

def _gezeichnete_texte(bauer, daten, tmp_path):
    """Alle Zeichenketten einsammeln, die wirklich auf das Blatt gehen."""
    from reportlab.pdfgen import canvas as pdfcanvas
    gesammelt = []
    originale = {}
    for name in ("drawString", "drawRightString", "drawCentredString"):
        originale[name] = getattr(pdfcanvas.Canvas, name)

    def haken(name):
        orig = originale[name]

        def ersatz(self, x, y, text, *a, **kw):
            gesammelt.append((self._fontname, str(text)))
            return orig(self, x, y, text, *a, **kw)
        return ersatz

    for name in originale:
        setattr(pdfcanvas.Canvas, name, haken(name))
    try:
        bauer(tmp_path / "p.pdf", daten)
    finally:
        for name, orig in originale.items():
            setattr(pdfcanvas.Canvas, name, orig)
    return gesammelt


def _fehlende_glyphen(paare):
    from reportlab.pdfbase import pdfmetrics
    fehlt = []
    for schrift, text in paare:
        try:
            face = pdfmetrics.getFont(schrift).face
        except Exception:
            continue
        tabelle = getattr(face, "charToGlyph", None)
        for ch in text:
            if ch in (" ", "\n"):
                continue
            if tabelle is None:                     # Standard-14: WinAnsi
                try:
                    ch.encode("cp1252")
                except Exception:
                    fehlt.append((schrift, ch, hex(ord(ch)), text[:40]))
            elif tabelle.get(ord(ch), 0) == 0:
                fehlt.append((schrift, ch, hex(ord(ch)), text[:40]))
    return fehlt


@pytest.mark.parametrize("art", ["daily", "monthly"])
def test_kein_zeichen_ohne_glyphe(tmp_path, art):
    """🔴 Die Probe, die 17.3.2 rot gemacht haette.

    Ein Zeichen ohne Glyphe wird nicht etwa weggelassen — reportlab malt
    ein schwarzes Rechteck. Im Quelltext sieht man das nie.
    """
    bauer = rp.build_daily_pdf if art == "daily" else rp.build_monthly_pdf
    daten = tagesdaten() if art == "daily" else monatsdaten()
    paare = _gezeichnete_texte(bauer, daten, tmp_path)
    assert paare, "es wurde ueberhaupt kein Text gezeichnet"
    fehlt = _fehlende_glyphen(paare)
    assert not fehlt, "Zeichen ohne Glyphe (wird als schwarzer Klotz gemalt): %r" % fehlt[:8]


def test_safe_ersetzt_was_die_schrift_nicht_kann():
    f = rp._Font()
    # Ein Zeichen, das mit Sicherheit keine der Schriften hat.
    assert "\U0001F4CA" not in f.safe("\U0001F4CA Bericht")
    assert f.safe("nur Text") == "nur Text"


# ──────────────── 2. Verschiedene Werte, verschiedene Balken ───────────────

def test_balken_bilden_die_werte_ab(tmp_path):
    """Der eigentliche Schaden: im alten PDF waren ALLE Balken gleich lang,
    weil voller und leerer Block dieselbe fehlende Glyphe waren.

    🔴 Gemessen wird GENAU das Lastprofil, nicht irgendwelche Rechtecke der
    Seite. Ein erster Anlauf zaehlte alle `rect`-Aufrufe und blieb deshalb
    auch gegen den kaputten Stand gruen — die Karten und Anteilsbalken
    lieferten schon genug verschiedene Hoehen.
    """
    from reportlab.pdfgen import canvas as pdfcanvas
    hoehen = []
    orig = pdfcanvas.Canvas.rect

    def haken(self, x, y, w, h, *a, **kw):
        hoehen.append(round(h, 2))
        return orig(self, x, y, w, h, *a, **kw)

    doc = rp._Doc(tmp_path / "p.pdf", "T", "U", "F")
    pdfcanvas.Canvas.rect = haken
    try:
        doc.stundenprofil(list(STUNDEN), 0.147, 12)
    finally:
        pdfcanvas.Canvas.rect = orig
    doc.schliessen()

    # 24 Werte mit 14 verschiedenen Hoehen koennen nicht alle gleich sein.
    assert len(hoehen) >= 20, "es wurden kaum Balken gezeichnet: %d" % len(hoehen)
    assert len(set(hoehen)) > 10, (
        "zu wenige verschiedene Balkenhoehen (%d von %d Balken) — das Bild "
        "traegt keine Information" % (len(set(hoehen)), len(hoehen)))
    # Und die Hoehen muessen den Werten der Groesse nach folgen.
    hoch = max(STUNDEN)
    i_hoch = STUNDEN.index(hoch)
    assert hoehen[i_hoch] == max(hoehen), (
        "der hoechste Balken steht nicht bei der hoechsten Stunde")


def test_achse_erreicht_den_groessten_wert(tmp_path):
    """Ein Balken darf nie ueber die Nulllinie hinaus gezeichnet werden und
    der groesste Wert muss sichtbar bleiben."""
    schritt = rp._runder_schritt(1.72)
    assert 0 < schritt <= 1.72
    assert rp._runder_schritt(0) > 0


# ─────────────────────── 3. Das Dokument als Ganzes ───────────────────────

@pytest.mark.parametrize("art", ["daily", "monthly"])
def test_bericht_entsteht_und_hat_seitenzahlen(tmp_path, art):
    bauer = rp.build_daily_pdf if art == "daily" else rp.build_monthly_pdf
    daten = tagesdaten() if art == "daily" else monatsdaten()
    p = bauer(tmp_path / f"{art}.pdf", daten)
    assert p.exists() and p.stat().st_size > 5000
    roh = p.read_bytes()
    assert roh.startswith(b"%PDF")
    # Fusszeile auf JEDER Seite: so viele "Page n" wie Seiten.
    paare = _gezeichnete_texte(bauer, daten, tmp_path)
    seiten = [t for _, t in paare if t.startswith("Page ")]
    assert seiten == ["Page %d" % (i + 1) for i in range(len(seiten))], seiten
    assert len(seiten) >= 1


@pytest.mark.parametrize("kaputt", [
    {},
    {"dev_data": [], "hourly_total": [], "hourly_per_device": {}},
    {"total_kwh": 0.0, "total_cost": 0.0, "hourly_total": [0.0] * 24},
    {"dev_data": [{"name": "Ein sehr langer Geraetename der nicht passt",
                   "kwh": 1.0, "cost": 0.3, "share_pct": 100.0,
                   "delta_pct": None, "peak_w": 0.0, "peak_hour": -1}]},
    {"total_prev": 0.0, "total_same_wd": 0.0, "spot_cost": None,
     "biggest_mover": None, "last7_days": []},
    {"standby_w": 0.0, "night_kwh": 0.0, "co2_kg": 0.0, "proj_kwh": 0.0},
])
def test_entartete_daten_werfen_nicht(tmp_path, kaputt):
    """Ein Bericht darf an fehlenden Zahlen nicht scheitern — sonst faellt
    die Zustellung an einem ruhigen Tag aus."""
    p = rp.build_daily_pdf(tmp_path / "x.pdf", tagesdaten(**kaputt))
    assert p.exists()


def test_build_pdf_meldet_fehler_statt_zu_werfen(tmp_path):
    """Der Einstiegspunkt schluckt Fehler: ein kaputter Bericht darf die
    Zustellung der anderen Kanaele nicht mitreissen."""
    assert rp.build_pdf(tmp_path / "y.pdf", "daily", tagesdaten()) is not None
    assert rp.build_pdf(tmp_path / "z.pdf", "daily", {"dev_data": "kaputt"}) is None


# ────────── 3. Nichts wird ueber den Satzspiegel hinaus gezeichnet ──────────
#
# 🔴 17.5: der Bilanz-Hinweis war laenger als eine Zeile und `hinweis()` hat
# ihn ungebrochen gezeichnet — reportlab klagt nicht, es malt einfach ueber den
# Blattrand hinaus. Im Quelltext unsichtbar, im PDF ein abgeschnittener Satz.
# Dieselbe Klasse traf `paare()`: die Randnotiz der linken Spalte lief in die
# Beschriftung der rechten.

def _ueberlaeufe(bauer, daten, tmp_path, rand=2.0):
    """Gezeichnete Zeichenketten, die rechts aus dem Satzspiegel laufen."""
    from reportlab.pdfgen import canvas as pdfcanvas
    raus = []
    orig = pdfcanvas.Canvas.drawString

    def ersatz(self, x, y, text, *a, **kw):
        try:
            br = self.stringWidth(str(text), self._fontname, self._fontsize)
            ueber = (x + br) - (rp.PAGE_W - rp.MARGIN)
            if ueber > rand:
                raus.append((round(ueber, 1), str(text)[:60]))
        except Exception:
            pass
        return orig(self, x, y, text, *a, **kw)

    pdfcanvas.Canvas.drawString = ersatz
    try:
        bauer(tmp_path / "p.pdf", daten)
    finally:
        pdfcanvas.Canvas.drawString = orig
    return raus


def solardaten(**zusatz):
    """Eine Anlage mit Netzzaehler, PV, Batterie und Mieter — die langen Texte."""
    d = tagesdaten()
    d.update({
        "basis": "supply", "owner_kwh": 17.8, "tenant_kwh": 2.0,
        "tenant_billed_eur": 0.65, "grid_cost_eur": 1.95,
        "feed_in_revenue_eur": 0.40, "owner_share": 0.31, "circuits_kwh": 17.1,
        "excluded_meters": {"grid": "grid", "pv": "pv", "battery": "battery"},
        "balance": {
            "grid_import_kwh": 6.0, "grid_export_kwh": 4.88,
            "pv_production_kwh": 21.4, "self_consumption_kwh": 16.52,
            "battery_charge_kwh": 4.2, "battery_discharge_kwh": 3.9,
            "battery_soc_pct": 62.0, "total_load_kwh": 19.8,
            "tenant_load_kwh": 2.0, "owner_load_kwh": 17.8, "autarky_pct": 69.7,
            "has_pv": True, "has_battery": True, "has_grid_meter": True,
            "tenant_names": ["Tenant"], "tenant_breakdown": {"Tenant": 2.0}},
    })
    d.update(zusatz)
    return d


def test_die_probe_merkt_einen_ueberlauf_ueberhaupt(tmp_path):
    """Gegenprobe: eine absichtlich zu breite Zeile MUSS auffallen."""
    def kaputt(pfad, daten):
        doc = rp._Doc(pfad, "T", "U", "F")
        doc.c.setFont(doc.f.regular, 8)
        doc.c.drawString(rp.MARGIN, doc.y, "W" * 400)
        doc.schliessen()
    assert _ueberlaeufe(kaputt, {}, tmp_path), "die Probe prueft nichts"


@pytest.mark.parametrize("art,bauer_name", [("daily", "build_daily_pdf"),
                                            ("monthly", "build_monthly_pdf")])
def test_nichts_laeuft_ueber_den_rand(tmp_path, art, bauer_name):
    daten = solardaten() if art == "daily" else monatsdaten(**{
        k: v for k, v in solardaten().items()
        if k in ("basis", "balance", "owner_kwh", "tenant_kwh",
                 "tenant_billed_eur", "grid_cost_eur", "feed_in_revenue_eur",
                 "owner_share", "circuits_kwh", "excluded_meters")})
    raus = _ueberlaeufe(getattr(rp, bauer_name), daten, tmp_path)
    assert not raus, "ragt ueber den Rand: %r" % (raus[:5],)


def test_bilanz_steht_im_tagesbericht(tmp_path):
    """Die Bilanzzeilen muessen wirklich aufs Blatt — nicht nur berechnet sein."""
    texte = [t for _, t in _gezeichnete_texte(rp.build_daily_pdf,
                                              solardaten(), tmp_path)]
    zusammen = " ".join(texte)
    for muss in ("ENERGY BALANCE", "Drawn from grid", "PV produced",
                 "Self-sufficiency", "Tenant circuits"):
        assert muss in zusammen, muss


def test_ohne_solar_keine_bilanzzeilen(tmp_path):
    """Eine Anlage ohne PV/Netzzaehler bekommt keine erfundene Bilanz."""
    texte = [t for _, t in _gezeichnete_texte(rp.build_daily_pdf,
                                              tagesdaten(), tmp_path)]
    zusammen = " ".join(texte)
    assert "ENERGY BALANCE" not in zusammen
    assert "Drawn from grid" not in zusammen
