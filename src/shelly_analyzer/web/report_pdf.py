# -*- coding: utf-8 -*-
"""Typeset the daily and monthly energy reports as proper PDF documents.

Replaces the earlier approach of dumping the Telegram message into a PDF
canvas line by line. Two things made that unusable:

  * 🔴 Every bar of the "24h profile" came out as the SAME black box. The
    text chart mixes U+2588 (full block) and U+2591 (light shade); base-14
    Helvetica has neither, so reportlab drew a filled rectangle for both.
    A chart in which "0.15" and "1.72" look identical carries no
    information at all. The same happened to the subscript in CO₂ and to
    every emoji in the Basic Multilingual Plane -- the old filter dropped
    only codepoints above U+FFFF, so ⚡ ⏰ ➡ went straight through to a
    font that cannot draw them.
  * A chat message and a document are not the same artefact. Monospace
    padding, emoji bullets and a fixed-width bar chart are right in
    Telegram and wrong on A4.

Two rules follow from that and are enforced below:

  1. **Draw, do not typeset.** Every bar, share and indicator is a vector
     rectangle. A rectangle renders in every viewer; a glyph only renders
     if the font happens to carry it.
  2. **Never write a character the font cannot draw.** `_Font.safe()`
     checks each string against the registered face and transliterates
     what is missing, so a missing glyph becomes "CO2" and never a blob.
"""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from shelly_analyzer.i18n import (format_datetime_local,
                                  format_number_local, normalize_lang,
                                  t as _t)

logger = logging.getLogger(__name__)

# ── Seitenmasse ────────────────────────────────────────────────────────────
PAGE_W, PAGE_H = 595.28, 841.89          # A4 in Punkt
MARGIN = 42.0
CONTENT_W = PAGE_W - 2 * MARGIN

# ── Farben (Druckfassung: heller Grund, kraeftige Akzente) ─────────────────
INK = (0.086, 0.125, 0.169)
INK_SOFT = (0.278, 0.333, 0.412)
MUTED = (0.455, 0.514, 0.592)
RULE = (0.886, 0.910, 0.941)
CARD_BG = (0.973, 0.980, 0.989)
BAND = (0.059, 0.090, 0.165)
BAND_TEXT = (1.0, 1.0, 1.0)
ACCENT = (0.145, 0.388, 0.922)
ACCENT_SOFT = (0.733, 0.812, 0.988)
GOOD = (0.016, 0.588, 0.412)
BAD = (0.863, 0.149, 0.149)
WARN = (0.851, 0.467, 0.024)
NIGHT = (0.404, 0.365, 0.808)

# Geraetefarben in der Reihenfolge der Liste
DEV_COLORS = [ACCENT, (0.914, 0.333, 0.310), (0.016, 0.588, 0.412),
              (0.851, 0.467, 0.024), NIGHT, (0.020, 0.600, 0.702)]


# ═══════════════════════════ Schriften ════════════════════════════════════

class _Font:
    """Haelt die benutzten Schriftnamen und sorgt dafuer, dass nie ein
    Zeichen gesetzt wird, das die Schrift nicht kennt.

    Reihenfolge: DejaVu (kommt mit matplotlib, beste Deckung) → Vera (liegt
    reportlab bei, also IMMER da) → Helvetica (Notnagel, magere Deckung).
    """

    ERSATZ = {
        "€": "EUR", "₂": "2", "→": "->", "←": "<-",
        "↑": "+", "↓": "-", "·": "-", "–": "-",
        "—": "-", "…": "...", " ": " ", "≈": "~",
        "³": "3", "²": "2", "≥": ">=", "≤": "<=",
    }

    def __init__(self) -> None:
        self.regular = "Helvetica"
        self.bold = "Helvetica-Bold"
        self._kann: Optional[set] = None
        self._register()

    def _kandidaten(self) -> List[Tuple[str, Path, Path]]:
        aus: List[Tuple[str, Path, Path]] = []
        try:                                  # matplotlib ist optional
            import matplotlib
            d = Path(matplotlib.__file__).parent / "mpl-data" / "fonts" / "ttf"
            aus.append(("DejaVuSans", d / "DejaVuSans.ttf", d / "DejaVuSans-Bold.ttf"))
        except Exception:
            pass
        try:
            import reportlab
            d = Path(reportlab.__file__).parent / "fonts"
            aus.append(("Vera", d / "Vera.ttf", d / "VeraBd.ttf"))
        except Exception:
            pass
        return aus

    def _register(self) -> None:
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        for name, reg, bold in self._kandidaten():
            if not (reg.exists() and bold.exists()):
                continue
            try:
                pdfmetrics.registerFont(TTFont(name, str(reg)))
                pdfmetrics.registerFont(TTFont(name + "-Bold", str(bold)))
                self.regular, self.bold = name, name + "-Bold"
                face = pdfmetrics.getFont(name).face
                self._kann = None
                self._face = face
                return
            except Exception:
                continue

    def kann(self, ch: str) -> bool:
        face = getattr(self, "_face", None)
        if face is None:                      # Helvetica: nur WinAnsi
            try:
                ch.encode("cp1252")
                return True
            except Exception:
                return False
        try:
            return face.charToGlyph.get(ord(ch), 0) != 0
        except Exception:
            return True

    def safe(self, s: Any) -> str:
        """Jedes Zeichen, das die Schrift nicht zeichnen kann, ersetzen —
        sonst malt reportlab einen schwarzen Klotz und niemand sieht es,
        bis das Dokument gedruckt vor einem liegt."""
        s = str(s)
        if all(self.kann(c) for c in s):
            return s
        aus = []
        for c in s:
            if self.kann(c):
                aus.append(c)
            else:
                aus.append(self.ERSATZ.get(c, ""))
        return "".join(aus)


# ═══════════════════════════ Zeichenflaeche ═══════════════════════════════

class _Doc:
    """Duenne Schicht ueber dem Canvas: Cursor, Seitenumbruch, Fusszeile."""

    def __init__(self, pfad: Path, titel: str, untertitel: str,
                 fuss: str, lang: str = "de") -> None:
        from reportlab.pdfgen import canvas as pdfcanvas
        self.lang = normalize_lang(lang)
        self.f = _Font()
        self.c = pdfcanvas.Canvas(str(pfad), pagesize=(PAGE_W, PAGE_H))
        self.c.setTitle(self.f.safe(titel))
        self.c.setAuthor("Shelly Energy Analyzer")
        self.titel, self.untertitel, self.fuss = titel, untertitel, fuss
        self.seite = 0
        self.y = 0.0
        self._neue_seite(erste=True)

    # ── Sprache ──
    def T(self, schluessel: str, **kw: Any) -> str:
        """Ein Text des Berichts in der eingestellten Sprache."""
        return _t(self.lang, "report." + schluessel, **kw)

    def z(self, v: float, stellen: Optional[int] = None) -> str:
        """Eine Zahl mit den Trennzeichen der eingestellten Sprache."""
        return _zahl(v, stellen, self.lang)

    # ── Seitengeruest ──
    def _neue_seite(self, erste: bool = False) -> None:
        if not erste:
            self._fusszeile()
            self.c.showPage()
        self.seite += 1
        if erste:
            self._masthead()
        else:
            self._kopf_folgeseite()

    def _masthead(self) -> None:
        c, f = self.c, self.f
        h = 74.0
        c.setFillColorRGB(*BAND)
        c.rect(0, PAGE_H - h, PAGE_W, h, stroke=0, fill=1)
        # Akzentkante links — ein gezeichneter Balken, kein Zeichen
        c.setFillColorRGB(*ACCENT)
        c.rect(0, PAGE_H - h, 5, h, stroke=0, fill=1)
        c.setFillColorRGB(*BAND_TEXT)
        c.setFont(f.bold, 17)
        c.drawString(MARGIN, PAGE_H - 33, f.safe(self.titel))
        c.setFont(f.regular, 9.5)
        c.setFillColorRGB(0.66, 0.72, 0.80)
        c.drawString(MARGIN, PAGE_H - 49, f.safe(self.untertitel))
        c.setFont(f.regular, 8)
        c.drawRightString(PAGE_W - MARGIN, PAGE_H - 33,
                          f.safe("SHELLY ENERGY ANALYZER"))
        c.drawRightString(PAGE_W - MARGIN, PAGE_H - 49,
                          f.safe(format_datetime_local(self.lang,
                                                      datetime.now())))
        self.y = PAGE_H - h - 26

    def _kopf_folgeseite(self) -> None:
        c, f = self.c, self.f
        c.setFillColorRGB(*MUTED)
        c.setFont(f.regular, 8)
        c.drawString(MARGIN, PAGE_H - 34, f.safe(self.titel))
        c.drawRightString(PAGE_W - MARGIN, PAGE_H - 34, f.safe(self.untertitel))
        c.setStrokeColorRGB(*RULE)
        c.setLineWidth(0.6)
        c.line(MARGIN, PAGE_H - 42, PAGE_W - MARGIN, PAGE_H - 42)
        self.y = PAGE_H - 62

    def _fusszeile(self) -> None:
        """Auf JEDER Seite, nicht nur auf der letzten."""
        c, f = self.c, self.f
        c.setStrokeColorRGB(*RULE)
        c.setLineWidth(0.6)
        c.line(MARGIN, 40, PAGE_W - MARGIN, 40)
        c.setFillColorRGB(*MUTED)
        c.setFont(f.regular, 7.5)
        c.drawString(MARGIN, 29, f.safe(self.fuss))
        c.drawRightString(PAGE_W - MARGIN, 29,
                          f.safe(self.T("page", n=self.seite)))

    def platz(self, hoehe: float) -> None:
        if self.y - hoehe < 56:
            self._neue_seite()

    def schliessen(self) -> None:
        self._fusszeile()
        self.c.save()

    # ── Bausteine ──
    def abschnitt(self, titel: str, braucht: float = 0.0) -> None:
        """`braucht` ist die Hoehe des Blocks, der GLEICH FOLGT. Ohne das
        landet die Ueberschrift am Fuss der Seite und ihr Inhalt auf der
        naechsten — genau das ist beim ersten Lauf passiert."""
        self.y -= 10                      # Luft vor der Ueberschrift
        self.platz(30 + braucht)
        c, f = self.c, self.f
        c.setFillColorRGB(*INK)
        c.setFont(f.bold, 10)
        c.drawString(MARGIN, self.y, f.safe(titel.upper()))
        br = c.stringWidth(f.safe(titel.upper()), f.bold, 10)
        c.setStrokeColorRGB(*RULE)
        c.setLineWidth(0.8)
        c.line(MARGIN + br + 10, self.y + 3, PAGE_W - MARGIN, self.y + 3)
        self.y -= 16

    def kpi_reihe(self, karten: Sequence[Dict[str, Any]]) -> None:
        """Vier Kennzahlen nebeneinander. Die farbige Oberkante ist ein
        gezeichneter Balken — sie traegt die Wertung (gut/schlecht)."""
        if not karten:
            return
        self.platz(64)
        c, f = self.c, self.f
        n = len(karten)
        luecke = 10.0
        b = (CONTENT_W - luecke * (n - 1)) / n
        oben = self.y
        hoehe = 58.0
        for i, k in enumerate(karten):
            x = MARGIN + i * (b + luecke)
            c.setFillColorRGB(*CARD_BG)
            c.setStrokeColorRGB(*RULE)
            c.setLineWidth(0.7)
            c.roundRect(x, oben - hoehe, b, hoehe, 4, stroke=1, fill=1)
            c.setFillColorRGB(*k.get("color", ACCENT))
            c.rect(x + 0.7, oben - 3.2, b - 1.4, 2.6, stroke=0, fill=1)
            # 🔴 Die Karte begrenzte ihren Text nicht. „VS. STESSO GIORNO
            # DELLA SETTIMANA" lief 33 pt über den Blattrand hinaus —
            # reportlab klagt nicht, es malt einfach weiter. Englisch fiel
            # das nie auf, weil dort „VS. SAME WEEKDAY" steht.
            innen = b - 20.0
            c.setFillColorRGB(*MUTED)
            self._text_passend(x + 10, oben - 17, k["label"].upper(),
                               innen, f.regular, 7.2, 5.4)
            c.setFillColorRGB(*k.get("value_color", INK))
            self._text_passend(x + 10, oben - 37, k["value"],
                               innen, f.bold, 16.0, 10.0)
            if k.get("sub"):
                c.setFillColorRGB(*MUTED)
                self._text_passend(x + 10, oben - 49, k["sub"],
                                   innen, f.regular, 7.6, 5.6)
        self.y = oben - hoehe - 18

    def _text_passend(self, x: float, y: float, text: str, breite: float,
                      schrift: str, groesse: float,
                      kleinste: float) -> None:
        """Text, der in *breite* passen MUSS: erst kleiner setzen, dann kürzen.

        Kleiner setzen ist das mildere Mittel — eine Beschriftung in 6 pt
        ist noch lesbar, eine abgeschnittene ist weg. Erst wenn selbst die
        kleinste Stufe nicht reicht, wird gekürzt.
        """
        c, f = self.c, self.f
        sicher = f.safe(str(text or ""))
        if not sicher:
            return
        g = groesse
        while g > kleinste and c.stringWidth(sicher, schrift, g) > breite:
            g -= 0.2
        c.setFont(schrift, g)
        if c.stringWidth(sicher, schrift, g) > breite:
            sicher = _kuerzen_breit(sicher, breite, c, schrift, g)
        c.drawString(x, y, sicher)

    # Anteil der Spaltenbreite, den die Beschriftung bekommt. Bei einer
    # Spalte ist das Blatt mehr als doppelt so breit, die Beschriftung
    # braucht aber nicht mehr Platz — sonst klafft eine Lücke bis zum Wert.
    _PAARE_LABEL = {1: 0.24, 2: 0.46}

    def _paare_anteil(self, zeilen: Sequence[Tuple[str, str, Optional[str]]],
                      spalten: int) -> float:
        """Welchen Anteil der Spaltenbreite die Beschriftung bekommt.

        Bei ZWEI Spalten ist der Anteil fest — dort ist ohnehin alles knapp.
        Bei EINER Spalte richtet er sich nach dem längsten Namen: „Stan
        naładowania magazynu" braucht mehr als „Battery state of charge",
        und ein fester Anteil hätte ihn wieder abgeschnitten.
        """
        anteil = self._PAARE_LABEL.get(spalten, 0.46)
        if spalten != 1 or not zeilen:
            return anteil
        c, f = self.c, self.f
        sb = CONTENT_W / spalten
        breit = max(c.stringWidth(f.safe(l), f.regular, 8.2)
                    for l, _, _ in zeilen)
        return min(0.52, max(anteil, (breit + 10.0) / sb))

    def _paare_passen(self, zeilen: Sequence[Tuple[str, str, Optional[str]]],
                      spalten: int) -> bool:
        """Passt JEDE Beschriftung und JEDE Randnotiz ohne Kürzen?"""
        c, f = self.c, self.f
        pro = (len(zeilen) + spalten - 1) // spalten
        sb = CONTENT_W / spalten
        anteil = self._paare_anteil(zeilen, spalten)
        for i, (label, wert, notiz) in enumerate(zeilen):
            sp = i // pro
            x = MARGIN + sp * sb
            if c.stringWidth(f.safe(label), f.regular, 8.2) > sb * anteil - 6:
                return False
            if not notiz:
                continue
            x_wert = x + sb * anteil
            x_notiz = x_wert + c.stringWidth(f.safe(wert), f.bold, 8.6) + 6
            frei = ((PAGE_W - MARGIN) - x_notiz - 2 if sp == spalten - 1
                    else (x + sb) - x_notiz - 10)
            if c.stringWidth(f.safe(notiz), f.regular, 7.6) > frei:
                return False
        return True

    def paare(self, zeilen: Sequence[Tuple[str, str, Optional[str]]],
              spalten: int = 2) -> None:
        """Label → Wert, in Spalten. Dritter Eintrag ist eine Randnotiz.

        🔴 Erst messen, dann setzen. Zwei Spalten sind kompakt, aber eine
        mitten im Wort abgeschnittene Randnotiz („77% selbst gen…") ist
        keine Angabe mehr. Deutsch, Polnisch und Russisch brauchen für
        dieselbe Aussage deutlich mehr Breite als Englisch — und selbst
        Englisch reichte nicht überall („same consumpti…"). Passt nicht
        alles, fällt der Block auf EINE Spalte zurück: eine Zeile mehr
        kostet nichts, das Blatt ist an dieser Stelle ohnehin halb leer.
        """
        if not zeilen:
            return
        c, f = self.c, self.f
        if spalten > 1 and not self._paare_passen(zeilen, spalten):
            spalten = 1
        pro = (len(zeilen) + spalten - 1) // spalten
        sb = CONTENT_W / spalten
        anteil = self._paare_anteil(zeilen, spalten)
        zh = 15.0
        self.platz(pro * zh + 6)
        oben = self.y
        for i, (label, wert, notiz) in enumerate(zeilen):
            sp, ze = divmod(i, pro)
            x = MARGIN + sp * sb
            y = oben - ze * zh
            # 🔴 Jede Zelle bleibt in ihrer Spalte. Ohne Begrenzung lief die
            # Randnotiz der linken Spalte in die Beschriftung der rechten —
            # zwei Angaben klebten aneinander und keine war mehr lesbar.
            x_wert = x + sb * anteil
            c.setFillColorRGB(*MUTED)
            c.setFont(f.regular, 8.2)
            c.drawString(x, y, _kuerzen_breit(f.safe(label), sb * anteil - 6,
                                              c, f.regular, 8.2))
            c.setFillColorRGB(*INK)
            c.setFont(f.bold, 8.6)
            c.drawString(x_wert, y, f.safe(wert))
            if notiz:
                br = c.stringWidth(f.safe(wert), f.bold, 8.6)
                x_notiz = x_wert + br + 6
                # Die LETZTE Spalte endet am Blattrand, nicht an einer
                # Spaltenbreite — sonst kuerzt man dort Text weg, fuer den
                # Platz da ist.
                if sp == spalten - 1:
                    frei = (PAGE_W - MARGIN) - x_notiz - 2
                else:
                    frei = (x + sb) - x_notiz - 10      # Luft zur Nachbarspalte
                kurz = _kuerzen_breit(f.safe(notiz), frei, c, f.regular, 7.6)
                if kurz:
                    c.setFillColorRGB(*MUTED)
                    c.setFont(f.regular, 7.6)
                    c.drawString(x_notiz, y, kurz)
        self.y = oben - pro * zh - 8

    def hinweis(self, text: str) -> None:
        """Eine Fussnote unter einem Block.

        🔴 Sie MUSS umbrechen. `drawString` schneidet nicht ab, sondern zeichnet
        ueber den Blattrand hinaus — im Quelltext unsichtbar, im gedruckten PDF
        ein abgeschnittener Satz. Aufgefallen ist es erst am gerenderten Bild,
        als der Bilanz-Hinweis laenger wurde als eine Zeile.
        """
        c, f = self.c, self.f
        roh = f.safe(str(text or "")).split()
        if not roh:
            return
        zeilen: List[str] = []
        akt = ""
        for wort in roh:
            probe = (akt + " " + wort).strip()
            if c.stringWidth(probe, f.regular, 7.8) <= CONTENT_W:
                akt = probe
            else:
                if akt:
                    zeilen.append(akt)
                akt = wort
        if akt:
            zeilen.append(akt)
        self.platz(len(zeilen) * 10 + 8)
        c.setFillColorRGB(*MUTED)
        c.setFont(f.regular, 7.8)
        for z in zeilen:
            c.drawString(MARGIN, self.y, z)
            self.y -= 10
        self.y -= 3

    # ── Diagramme: gezeichnet, nicht gesetzt ──
    def stundenprofil(self, stunden: Sequence[float], grundlast_kwh: float = 0.0,
                      spitze: int = -1, hoehe: float = 102.0,
                      x_beschriftung: str = "") -> None:
        """24 (oder 31) Balken als Rechtecke. Eine Achse, die den groessten
        Wert wirklich erreicht — sonst behauptet das Bild etwas anderes als
        die Zahl daneben."""
        if not stunden:
            return
        self.platz(hoehe + 34)
        c, f = self.c, self.f
        n = len(stunden)
        oben = self.y
        grund = oben - hoehe                       # Nulllinie
        hoch = max(stunden) or 1.0
        # Achsenteilung auf eine runde Zahl OBERHALB des Maximums
        schritt = _runder_schritt(hoch)
        deckel = schritt * (int(hoch / schritt) + 1) if hoch % schritt else hoch

        # Gitter + Achsenbeschriftung
        c.setFont(f.regular, 6.6)
        i = 0
        while schritt * i <= deckel + 1e-9:
            wert = schritt * i
            y = grund + (wert / deckel) * hoehe
            c.setStrokeColorRGB(*RULE)
            c.setLineWidth(0.4)
            c.line(MARGIN + 26, y, PAGE_W - MARGIN, y)
            c.setFillColorRGB(*MUTED)
            c.drawRightString(MARGIN + 21, y - 2.2, self.z(wert))
            i += 1

        links = MARGIN + 26
        breite = (PAGE_W - MARGIN) - links
        bb = breite / n
        for h, v in enumerate(stunden):
            x = links + h * bb
            bh = (v / deckel) * hoehe
            if h == spitze:
                c.setFillColorRGB(*WARN)
            elif 0 <= h <= 5 and n == 24:
                c.setFillColorRGB(*NIGHT)          # Nachtstunden abgesetzt
            else:
                c.setFillColorRGB(*ACCENT)
            if bh > 0.4:
                c.rect(x + bb * 0.16, grund, bb * 0.68, bh, stroke=0, fill=1)

        # Grundlast als gestrichelte Linie — der Wert, der immer laeuft
        if grundlast_kwh > 0:
            y = grund + (grundlast_kwh / deckel) * hoehe
            c.setStrokeColorRGB(*MUTED)
            c.setLineWidth(0.8)
            c.setDash(2.5, 2)
            c.line(links, y, PAGE_W - MARGIN, y)
            c.setDash()
            c.setFillColorRGB(*MUTED)
            c.setFont(f.regular, 6.4)
            c.drawString(links + 3, y + 3, f.safe(self.T("base_load_short")))

        # Nulllinie
        c.setStrokeColorRGB(*INK_SOFT)
        c.setLineWidth(0.8)
        c.line(links, grund, PAGE_W - MARGIN, grund)

        # Stundenbeschriftung: jede zweite bei 24, sonst jede dritte
        c.setFillColorRGB(*MUTED)
        c.setFont(f.regular, 6.4)
        takt = 2 if n <= 24 else 3
        for h in range(0, n, takt):
            x = links + h * bb + bb / 2
            c.drawCentredString(x, grund - 9, str(h if n <= 24 else h + 1))
        c.setFont(f.regular, 6.8)
        c.drawCentredString(links + breite / 2, grund - 19, f.safe(x_beschriftung))
        self.y = grund - 28

    def geraete_tabelle(self, geraete: Sequence[Dict[str, Any]],
                        waehrung: str = "EUR") -> None:
        """Kopfzeile, eine Zeile je Geraet, Anteil als gezeichneter Balken."""
        if not geraete:
            self.hinweis(self.T("no_devices"))
            return
        c, f = self.c, self.f
        kopf = 13.0
        zh = 16.0
        self.platz(kopf + zh * len(geraete) + 8)
        # Spalten
        x_name = MARGIN
        x_kwh = MARGIN + 150
        x_bal = MARGIN + 205
        w_bal = 96.0
        x_pct = x_bal + w_bal + 8
        x_eur = MARGIN + 366
        x_dlt = MARGIN + 424
        x_pk = PAGE_W - MARGIN

        c.setFillColorRGB(*MUTED)
        c.setFont(f.regular, 7.0)
        c.drawString(x_name, self.y, f.safe(self.T("col.device")))
        c.drawRightString(x_kwh, self.y, f.safe(self.T("col.kwh")))
        c.drawString(x_bal, self.y, f.safe(self.T("col.share")))
        c.drawRightString(x_eur, self.y, f.safe(waehrung.upper()))
        c.drawRightString(x_dlt, self.y, f.safe(self.T("col.vs_prev")))
        c.drawRightString(x_pk, self.y, f.safe(self.T("col.peak")))
        self.y -= 5
        c.setStrokeColorRGB(*RULE)
        c.setLineWidth(0.7)
        c.line(MARGIN, self.y, PAGE_W - MARGIN, self.y)
        self.y -= 12

        for i, d in enumerate(geraete):
            farbe = DEV_COLORS[i % len(DEV_COLORS)]
            y = self.y
            c.setFillColorRGB(*farbe)
            c.rect(x_name, y - 1.2, 3, 8.5, stroke=0, fill=1)
            c.setFillColorRGB(*INK)
            c.setFont(f.bold, 8.6)
            c.drawString(x_name + 9, y, f.safe(_kuerzen(
                str(d.get("name", "")), 22, c, f.bold, 8.6)))
            c.setFont(f.regular, 8.6)
            c.drawRightString(x_kwh, y, self.z(float(d.get("kwh", 0)), 2))

            anteil = max(0.0, min(100.0, float(d.get("share_pct", 0)))) / 100.0
            c.setFillColorRGB(*RULE)
            c.roundRect(x_bal, y - 1.5, w_bal, 7, 2, stroke=0, fill=1)
            if anteil > 0:
                c.setFillColorRGB(*farbe)
                c.roundRect(x_bal, y - 1.5, max(2.2, w_bal * anteil), 7, 2,
                            stroke=0, fill=1)
            c.setFillColorRGB(*MUTED)
            c.setFont(f.regular, 7.4)
            c.drawString(x_pct, y, "%.0f%%" % float(d.get("share_pct", 0)))

            c.setFillColorRGB(*INK)
            c.setFont(f.regular, 8.6)
            c.drawRightString(x_eur, y, self.z(float(d.get("cost", 0)), 2))

            dp = d.get("delta_pct")
            if dp is None:
                c.setFillColorRGB(*MUTED)
                c.drawRightString(x_dlt, y, f.safe("-"))
            else:
                c.setFillColorRGB(*(GOOD if dp < 0 else BAD if dp > 0 else MUTED))
                c.setFont(f.bold, 8.4)
                c.drawRightString(x_dlt, y, "%+.0f%%" % dp)

            c.setFillColorRGB(*MUTED)
            c.setFont(f.regular, 7.8)
            ph = int(d.get("peak_hour", -1))
            pk = ("%s W @ %02d:00" % (self.z(float(d.get("peak_w", 0)), 0), ph)
                  if ph >= 0 else "-")
            c.drawRightString(x_pk, y, f.safe(pk))

            self.y -= zh
            if i < len(geraete) - 1:
                c.setStrokeColorRGB(*RULE)
                c.setLineWidth(0.35)
                c.line(MARGIN, self.y + 9, PAGE_W - MARGIN, self.y + 9)
        self.y -= 4

    def verlauf(self, punkte: Sequence[Tuple[str, float]], preis: float,
                hoehe: float = 68.0) -> None:
        """Balken je Tag mit Beschriftung, Wert ueber dem Balken und dem
        Mittel als Linie. Der heutige (letzte) Balken ist abgesetzt."""
        if not punkte:
            return
        self.platz(hoehe + 34)
        c, f = self.c, self.f
        oben = self.y
        grund = oben - hoehe
        werte = [v for _, v in punkte]
        hoch = max(werte) or 1.0
        mittel = sum(werte) / len(werte)
        n = len(punkte)
        bb = CONTENT_W / n
        # Zuerst die Linie, dann die Balken darueber: so verdeckt sie keine
        # Zahl und bleibt trotzdem in den Luecken sichtbar.
        y_m = grund + (mittel / hoch) * (hoehe - 12)
        c.setStrokeColorRGB(*WARN)
        c.setLineWidth(0.8)
        c.setDash(3, 2)
        c.line(MARGIN, y_m, PAGE_W - MARGIN, y_m)
        c.setDash()
        for i, (label, v) in enumerate(punkte):
            x = MARGIN + i * bb
            bh = (v / hoch) * (hoehe - 12)
            letzte = i == n - 1
            c.setFillColorRGB(*(ACCENT if letzte else ACCENT_SOFT))
            c.roundRect(x + bb * 0.20, grund, bb * 0.60, max(1.0, bh), 2,
                        stroke=0, fill=1)
            # Der Wert steht IM Balken, wenn er dort Platz hat — ausserhalb
            # kollidiert er mit der Mittelwertlinie und dem Nachbarbalken.
            if bh >= 16:
                c.setFillColorRGB(1, 1, 1) if letzte else c.setFillColorRGB(*INK)
                c.setFont(f.bold if letzte else f.regular, 7.0)
                c.drawCentredString(x + bb / 2, grund + bh - 11, self.z(v, 2))
            else:
                c.setFillColorRGB(*(INK if letzte else MUTED))
                c.setFont(f.bold if letzte else f.regular, 7.0)
                c.drawCentredString(x + bb / 2, grund + bh + 4, self.z(v, 2))
            c.setFillColorRGB(*MUTED)
            c.setFont(f.regular, 6.6)
            c.drawCentredString(x + bb / 2, grund - 10, f.safe(label))
        self._verlauf_mittel = mittel
        c.setStrokeColorRGB(*INK_SOFT)
        c.setLineWidth(0.7)
        c.line(MARGIN, grund, PAGE_W - MARGIN, grund)
        self.y = grund - 20

    def stundentabelle(self, stunden: Sequence[float], preis: float,
                       gruppen: int = 3) -> None:
        """Jede einzelne Stunde mit kWh und Kosten. Drei Gruppen
        nebeneinander, damit 24 Zeilen auf wenige Zentimeter passen."""
        if not stunden:
            return
        c, f = self.c, self.f
        n = len(stunden)
        pro = (n + gruppen - 1) // gruppen
        zh = 11.0
        self.platz(pro * zh + 22)
        oben = self.y
        gb = CONTENT_W / gruppen
        hoch = max(stunden) or 1.0

        for g in range(gruppen):
            x = MARGIN + g * gb
            c.setFillColorRGB(*MUTED)
            c.setFont(f.regular, 6.6)
            c.drawString(x, oben, f.safe(self.T("col.hour")))
            c.drawRightString(x + gb * 0.40, oben, f.safe(self.T("col.kwh")))
            c.drawRightString(x + gb * 0.60, oben, f.safe(self.T("col.eur")))
            c.drawString(x + gb * 0.64, oben, f.safe(self.T("col.share")))
        c.setStrokeColorRGB(*RULE)
        c.setLineWidth(0.6)
        c.line(MARGIN, oben - 4, PAGE_W - MARGIN, oben - 4)

        for i, v in enumerate(stunden):
            g, ze = divmod(i, pro)
            x = MARGIN + g * gb
            y = oben - 14 - ze * zh
            nacht = 0 <= i <= 5
            c.setFillColorRGB(*(NIGHT if nacht else INK_SOFT))
            c.setFont(f.regular, 7.2)
            c.drawString(x, y, "%02d" % i)
            c.setFillColorRGB(*INK)
            c.setFont(f.regular, 7.2)
            c.drawRightString(x + gb * 0.40, y, self.z(v, 2))
            c.setFillColorRGB(*INK_SOFT)
            c.drawRightString(x + gb * 0.60, y, self.z(v * preis, 2))
            # Anteilsbalken — gezeichnet, nicht gesetzt
            bw = gb * 0.30
            c.setFillColorRGB(*RULE)
            c.rect(x + gb * 0.64, y - 0.4, bw, 4.4, stroke=0, fill=1)
            if v > 0:
                c.setFillColorRGB(*(NIGHT if nacht else ACCENT))
                c.rect(x + gb * 0.64, y - 0.4, max(0.8, bw * (v / hoch)), 4.4,
                       stroke=0, fill=1)
        self.y = oben - 14 - pro * zh - 4

    def geraete_profile(self, je_geraet: Dict[str, Sequence[float]],
                        spalten: int = 2, hoehe: float = 46.0) -> None:
        """Ein kleines Stundenprofil je Geraet, alle auf DERSELBEN Achse —
        sonst sieht eine Wallbox mit 1 kWh aus wie ein Haus mit 8."""
        eintraege = [(k, list(v)) for k, v in (je_geraet or {}).items()
                     if any(x > 0 for x in v)]
        if not eintraege:
            return
        c, f = self.c, self.f
        hoch = max(max(v) for _, v in eintraege) or 1.0
        zeilen = (len(eintraege) + spalten - 1) // spalten
        self.platz(zeilen * (hoehe + 20) + 6)
        sb = CONTENT_W / spalten
        oben = self.y
        for i, (name, werte) in enumerate(eintraege):
            ze, sp = divmod(i, spalten)      # Zeile, dann Spalte

            x0 = MARGIN + sp * sb
            y0 = oben - ze * (hoehe + 20) - hoehe
            farbe = DEV_COLORS[i % len(DEV_COLORS)]
            c.setFillColorRGB(*farbe)
            c.rect(x0, y0 + hoehe + 5, 3, 8, stroke=0, fill=1)
            c.setFillColorRGB(*INK)
            c.setFont(f.bold, 8.0)
            c.drawString(x0 + 8, y0 + hoehe + 5, f.safe(name))
            c.setFillColorRGB(*MUTED)
            c.setFont(f.regular, 7.0)
            c.drawRightString(x0 + sb - 20, y0 + hoehe + 5,
                              f.safe("%s kWh" % self.z(sum(werte), 2)))
            bb = (sb - 20) / len(werte)
            for h, v in enumerate(werte):
                bh = (v / hoch) * hoehe
                if bh <= 0.3:
                    continue
                c.setFillColorRGB(*farbe)
                c.rect(x0 + h * bb, y0, bb * 0.72, bh, stroke=0, fill=1)
            c.setStrokeColorRGB(*RULE)
            c.setLineWidth(0.6)
            c.line(x0, y0, x0 + sb - 20, y0)
            c.setFillColorRGB(*MUTED)
            c.setFont(f.regular, 6.0)
            for h in (0, 6, 12, 18):
                c.drawCentredString(x0 + h * bb + bb / 2, y0 - 7, "%02d" % h)
        self.y = oben - zeilen * (hoehe + 20) - 10

    def seitenumbruch(self) -> None:
        """Kein Umbruch, wenn die Seite ohnehin frisch ist — sonst entsteht
        eine leere Seite hinter einem Block, der gerade erst umgebrochen
        wurde."""
        if self.y > PAGE_H - 120:
            return
        self._neue_seite()

    def tagesbalken(self, werte: Sequence[float], beschriftung: str,
                    hoehe: float = 96.0) -> None:
        """Monatsansicht: ein Balken je Tag."""
        self.stundenprofil(list(werte), 0.0, -1, hoehe, beschriftung)


# ═══════════════════════════ Helfer ═══════════════════════════════════════

def _runder_schritt(hoch: float) -> float:
    """Eine Achsenteilung, die ein Mensch lesen kann."""
    if hoch <= 0:
        return 1.0
    import math
    roh = hoch / 4.0
    groesse = 10 ** math.floor(math.log10(roh))
    for m in (1, 2, 2.5, 5, 10):
        if roh <= groesse * m:
            return groesse * m
    return groesse * 10


def _zahl(v: float, stellen: Optional[int] = None,
          lang: str = "en") -> str:
    """Eine Zahl in der Schreibweise der Sprache.

    🔴 Bis 17.5.1 schrieb der Bericht IMMER "1 234.56" — Punkt als
    Dezimaltrenner, egal welche Sprache eingestellt war. Auf Deutsch ist
    das nicht nur fremd, sondern zweideutig: "1.234" liest sich dort als
    Tausender, nicht als Komma-Zahl.
    """
    if stellen is None:
        stellen = 0 if abs(v) >= 100 else (1 if abs(v) >= 10 else 2)
    return format_number_local(lang, v, int(stellen))


def _vorz(v: float, stellen: int = 2, lang: str = "en") -> str:
    """Dieselbe Zahl mit ausdruecklichem Vorzeichen (+1,20 / -0,40)."""
    return ("+" if v >= 0 else "−") + _zahl(abs(v), stellen, lang)


def _kuerzen(s: str, max_zeichen: int, c, schrift: str, groesse: float) -> str:
    if len(s) <= max_zeichen:
        return s
    return s[: max_zeichen - 1] + "."


def _kuerzen_breit(s: str, max_breite: float, c, schrift: str, groesse: float) -> str:
    """Auf eine BREITE kuerzen, nicht auf eine Zeichenzahl.

    🔴 Eine Zeichenzahl schaetzt die Breite und liegt bei „17.80 kWh" und bei
    „Selbstversorgungsgrad" verschieden daneben; reportlab zeichnet dann ueber
    den Rand oder in die Nachbarspalte, ohne zu klagen.
    """
    if max_breite <= 0:
        return ""
    if c.stringWidth(s, schrift, groesse) <= max_breite:
        return s
    kurz = s
    while kurz and c.stringWidth(kurz + "\u2026", schrift, groesse) > max_breite:
        kurz = kurz[:-1]
    return (kurz + "\u2026") if kurz else ""


def _pfeil_farbe(pct: Optional[float], weniger_ist_besser: bool = True):
    if pct is None:
        return MUTED
    if abs(pct) < 1.0:
        return MUTED
    besser = pct < 0 if weniger_ist_besser else pct > 0
    return GOOD if besser else BAD


def _vergleich(kwh: float, bezug: float, preis: float,
               label: str, lang: str = "en") -> Optional[Dict[str, Any]]:
    if not bezug or bezug <= 0:
        return None
    pct = (kwh - bezug) / bezug * 100.0
    return {
        "label": label,
        "value": "%+.1f%%" % pct,
        "sub": _t(lang, "report.cmp.sub",
                  kwh=_vorz(kwh - bezug, 2, lang),
                  eur=_vorz((kwh - bezug) * preis, 2, lang)),
        "color": _pfeil_farbe(pct),
        "value_color": _pfeil_farbe(pct),
    }


# ═══════════════════════════ Tagesbericht ═════════════════════════════════


def _bilanz_zeilen(data: Dict[str, Any],
                   lang: str = "en") -> List[Tuple[str, str, Optional[str]]]:
    """Die Energiebilanz als Beschriftung/Wert/Zusatz-Zeilen.

    Nur was wirklich gemessen ist: ohne Netzzaehler keine erfundene Netzzahl,
    ohne Batterie keine Batteriezeile. Eine leere Liste heisst „diese Anlage hat
    nur Verbrauchszaehler" — dann bleibt der Bericht wie vorher.
    """
    def T(k: str, **kw: Any) -> str:
        return _t(lang, "report." + k, **kw)

    def z(v: float, stellen: Optional[int] = None) -> str:
        return _zahl(v, stellen, lang)

    b = data.get("balance") or {}
    preis = float(data.get("unit_price") or 0.0)
    zeilen: List[Tuple[str, str, Optional[str]]] = []

    last = float(b.get("total_load_kwh") or 0.0)
    if b.get("has_grid_meter"):
        imp = float(b.get("grid_import_kwh") or 0.0)
        exp = float(b.get("grid_export_kwh") or 0.0)
        zeilen.append((T("bal.grid_in"), "%s kWh" % z(imp, 2),
                       T("bal.grid_in_sub",
                         eur=z(float(data.get("grid_cost_eur") or 0), 2),
                         ct=z(preis * 100, 1))))
        if exp > 0:
            verg = float(data.get("feed_in_revenue_eur") or 0.0)
            zeilen.append((T("bal.grid_out"), "%s kWh" % z(exp, 2),
                           T("bal.grid_out_sub", eur=z(verg, 2))))
        zeilen.append((T("bal.grid_net"), "%s kWh" % z(imp - exp, 2),
                       "%s EUR" % z(float(data.get("total_cost") or 0), 2)))

    if b.get("has_pv"):
        prod = float(b.get("pv_production_kwh") or 0.0)
        eigen = float(b.get("self_consumption_kwh") or 0.0)
        zeilen.append((T("bal.pv_prod"), "%s kWh" % z(prod, 2),
                       T("bal.pv_used_pct",
                         v="%.0f" % min(100.0, eigen / prod * 100))
                       if prod > 0 else None))
        # 🔴 NICHT gegen die Last rechnen: der Eigenverbrauch enthaelt auch,
        # was in die Batterie ging, die Last nicht — auf einer guten Anlage
        # kommen dabei "108 % of load" heraus (an einer echten Anlage mit
        # Speicher gemessen, 26.09.2026).
        # Was davon die LAST gedeckt hat, ist der Eigenverbrauch OHNE das, was
        # in die Batterie ging.
        _lade = float(b.get("battery_charge_kwh") or 0.0)
        _direkt = max(0.0, eigen - _lade)
        # Kurz halten: neben einem breiten Wert bleiben in der Spalte nur rund
        # 75 pt, eine lange Zusatzzeile wird darin abgeschnitten.
        _zusatz = (T("bal.pv_of_load",
                     v="%.0f" % min(100.0, _direkt / last * 100))
                   if last > 0 else None)
        zeilen.append((T("bal.pv_self"), "%s kWh" % z(eigen, 2), _zusatz))

    if b.get("has_battery"):
        lade = float(b.get("battery_charge_kwh") or 0.0)
        ent = float(b.get("battery_discharge_kwh") or 0.0)
        zeilen.append((T("bal.bat_charge"), "%s kWh" % z(lade, 2), None))
        # 🔴 Entladen/Geladen EINES Zeitraums ist KEIN Wirkungsgrad: was heute
        # hineingeht, kommt morgen heraus. "35 % round trip" liest sich wie ein
        # Defekt, ist aber ein Speicher, der voller endet als er anfing.
        zeilen.append((T("bal.bat_discharge"), "%s kWh" % z(ent, 2),
                       T("bal.bat_net", v=_vorz(lade - ent, 2, lang))
                       if (lade or ent) else None))
        soc = b.get("battery_soc_pct")
        if soc is not None:
            zeilen.append((T("bal.bat_soc"), "%s%%" % z(float(soc), 0), None))

    if float(b.get("autarky_pct") or 0) > 0:
        zeilen.append((T("bal.autarky"), "%.0f%%" % float(b["autarky_pct"]),
                       T("bal.not_bought")))

    anteil = float(data.get("owner_share") or 1.0)
    if anteil < 0.999:
        zeilen.append((T("bal.grid_share"), "%.0f%%" % (anteil * 100),
                       T("bal.rest_pv")))

    # Die Aufteilung lohnt nur, wenn es beide Seiten gibt. Wo JEDER Kreis einer
    # Einheit zugeordnet ist (der Eigentuemer fuehrt seine eigene Wohnung als
    # Einheit), waere "Mieter 12,51 kWh / Eigen 0,00 kWh" eine Aussage ueber die
    # Konfiguration, nicht ueber den Strom.
    mieter = float(data.get("tenant_kwh") or 0.0)
    eigner = float(data.get("owner_kwh") or 0.0)
    if mieter > 0 and eigner > 0:
        zeilen.append((T("bal.tenant"), "%s kWh" % z(mieter, 2),
                       T("bal.tenant_sub",
                         eur=z(float(data.get("tenant_billed_eur") or 0), 2))))
        zeilen.append((T("bal.owner"), "%s kWh" % z(eigner, 2),
                       T("bal.owner_sub", v="%.0f" % (eigner / last * 100))
                       if last > 0 else None))

    return zeilen


def _bilanz_hinweis(data: Dict[str, Any], lang: str = "en") -> str:
    """Woher die Kopfzahl kommt — und welche Zaehler absichtlich fehlen."""
    def T(k: str, **kw: Any) -> str:
        return _t(lang, "report." + k, **kw)

    def z(v: float, stellen: Optional[int] = None) -> str:
        return _zahl(v, stellen, lang)

    basis = str(data.get("basis") or "devices")
    kreise = float(data.get("circuits_kwh") or 0.0)
    gesamt = float(data.get("total_kwh") or 0.0)
    teile: List[str] = []
    if basis == "supply":
        teile.append(T("note.supply"))
        if kreise > 0 and gesamt > 0:
            deckung = kreise / gesamt * 100
            if deckung > 105.0:
                # Kann echt sein (ein Kreis misst mehr, als die Versorgung
                # hergibt: Kalibrierung, oder ein Zaehler ausserhalb der
                # Bilanzgrenze). Als Befund schreiben, nicht als Deckungsgrad
                # verkleiden — "203 % gedeckt" liest sich wie ein Fehler.
                teile.append(T("note.over", a=z(kreise, 1), b=z(gesamt, 1),
                               p="%.0f" % (deckung - 100.0)))
            else:
                teile.append(T("note.cover", a=z(kreise, 1), b=z(gesamt, 1),
                               p="%.0f" % deckung))
    else:
        teile.append(T("note.devices"))
    aus = data.get("excluded_meters") or {}
    if aus:
        rollen = {"grid": T("role.grid"), "pv": T("role.pv"),
                  "battery": T("role.battery")}
        namen = ["%s (%s)" % (k, rollen.get(v, v)) for k, v in sorted(aus.items())]
        teile.append(T("note.excluded", list=", ".join(namen)))
    return "  ".join(teile)


def build_daily_pdf(out: Path, data: Dict[str, Any],
                    chart_png: Optional[Path] = None,
                    lang: str = "de") -> Path:
    out.parent.mkdir(parents=True, exist_ok=True)
    lang = normalize_lang(lang)
    preis = float(data.get("unit_price") or 0.0)
    kwh = float(data.get("total_kwh") or 0.0)
    kosten = float(data.get("total_cost") or 0.0)

    def T(k: str, **kw: Any) -> str:
        return _t(lang, "report." + k, **kw)

    def z(v: float, stellen: Optional[int] = None) -> str:
        return _zahl(v, stellen, lang)

    doc = _Doc(out, T("title.daily"), str(data.get("date_label", "")),
               T("generated"), lang=lang)

    # ── Kennzahlen ──
    karten: List[Dict[str, Any]] = [
        {"label": T("kpi.consumption"), "value": "%s kWh" % z(kwh, 2),
         "sub": T("kpi.avg_w", v=z(float(data.get("avg_w") or 0), 0)),
         "color": ACCENT},
        {"label": T("kpi.cost"), "value": "%s EUR" % z(kosten, 2),
         "sub": (T("kpi.ct_kwh", v=z(kosten / kwh * 100, 1))) if kwh > 0 else "",
         "color": ACCENT},
    ]
    v1 = _vergleich(kwh, float(data.get("total_prev") or 0), preis,
                    T("kpi.vs_prev_day"), lang)
    v2 = _vergleich(kwh, float(data.get("total_same_wd") or 0), preis,
                    T("kpi.vs_same_wd"), lang)
    for v in (v1, v2):
        if v:
            karten.append(v)
    doc.kpi_reihe(karten[:4])

    # Zweite Reihe nur, wenn es die laengeren Bezuege wirklich gibt.
    zweite: List[Dict[str, Any]] = []
    for feld, schluessel in (("avg7_kwh", "kpi.vs_avg7"),
                             ("avg30_kwh", "kpi.vs_avg30")):
        v = _vergleich(kwh, float(data.get(feld) or 0), preis, T(schluessel), lang)
        if v:
            zweite.append(v)
    if data.get("spot_cost") is not None:
        delta = float(data["spot_cost"]) - kosten
        zweite.append({
            "label": T("kpi.spot_vs_fixed"),
            "value": "%s EUR" % _vorz(delta, 2, lang),
            "sub": T("kpi.spot_would_be", v=z(float(data["spot_cost"]), 2)),
            "color": GOOD if delta < 0 else BAD if delta > 0 else MUTED,
            "value_color": GOOD if delta < 0 else BAD if delta > 0 else INK})
    if float(data.get("co2_kg") or 0) > 0:
        zweite.append({
            "label": T("kpi.co2"),
            "value": "%s kg" % z(float(data["co2_kg"]), 1),
            "sub": T("kpi.grid_mix",
                     v=z(float(data.get("co2_g_per_kwh") or 0), 0)),
            "color": MUTED, "value_color": INK})
    if zweite:
        doc.kpi_reihe(zweite[:4])

    # ── Energiebilanz: ohne sie ist eine Verbrauchszahl eine Behauptung ──
    bilanz = _bilanz_zeilen(data, lang)
    if bilanz:
        doc.abschnitt(T("sec.balance"), braucht=90)
        doc.paare(bilanz, spalten=2)
        doc.hinweis(_bilanz_hinweis(data, lang))

    # ── Lastgang ──
    stunden = list(data.get("hourly_total") or [])
    if stunden and any(v > 0 for v in stunden):
        doc.abschnitt(T("sec.load_profile"))
        grund_kwh = float(data.get("standby_w") or 0) / 1000.0
        doc.stundenprofil(stunden, grund_kwh, int(data.get("peak_h", -1)),
                          x_beschriftung=T("axis.hour_of_day"))
        spitzen = []
        if int(data.get("peak_h", -1)) >= 0:
            spitzen.append(T("lp.peak", a="%02d" % int(data["peak_h"]),
                             b="%02d" % (int(data["peak_h"]) + 1),
                             v=z(float(data.get("peak_h_kwh") or 0), 2)))
        if float(data.get("standby_w") or 0) > 0:
            spitzen.append(T("lp.base", v=z(float(data["standby_w"]), 0)))
        spitzen.append(T("lp.night"))
        doc.hinweis("  ".join(spitzen))

    # ── Geraete ──
    doc.abschnitt(T("sec.devices"))
    doc.geraete_tabelle(list(data.get("dev_data") or []))
    mover = data.get("biggest_mover")
    if mover and preis and abs(float(mover.get("diff", 0))) * preis >= 0.20:
        d = float(mover["diff"])
        doc.hinweis(T("mover", name=mover.get("name", "?"),
                      a=z(float(mover["from"]), 1), b=z(float(mover["to"]), 1),
                      d=_vorz(d, 2, lang), e=_vorz(d * preis, 2, lang)))

    # ── Auswertung ── (braucht: sonst steht die Ueberschrift allein am Fuss)
    doc.abschnitt(T("sec.analysis"), braucht=95)
    zeilen: List[Tuple[str, str, Optional[str]]] = []

    if float(data.get("max_power_w") or 0) > 0:
        zeilen.append((T("an.max_power"),
                       "%s W" % z(float(data["max_power_w"]), 0),
                       T("an.at_hour", h="%02d" % int(data.get("max_power_hour", 0)))))
        avg_w = float(data.get("avg_w") or 0)
        if avg_w > 0:
            zeilen.append((T("an.p2a"),
                           "%s x" % z(float(data["max_power_w"]) / avg_w, 1),
                           T("an.load_factor",
                             v="%.0f" % (float(data.get("load_factor") or 0) * 100))))

    # 🔴 Die Grundlast als ANTEIL — die absolute Wattzahl allein sagt
    # niemandem, ob sie viel ist. 147 W sind 42 % eines sparsamen Tages.
    standby_w = float(data.get("standby_w") or 0)
    if standby_w > 0:
        grund_tag = standby_w * 24.0 / 1000.0
        zeilen.append((T("an.base_load"), "%s W" % z(standby_w, 0),
                       T("an.kwh_day", v=z(grund_tag, 2))))
        if kwh > 0:
            zeilen.append((T("an.base_share"), "%.0f%%" % (grund_tag / kwh * 100),
                           T("an.of_day_cons")))
        zeilen.append((T("an.base_year"),
                       "%s kWh" % z(float(data.get("standby_annual_kwh") or 0), 0),
                       T("an.approx_eur",
                         v=z(float(data.get("standby_annual_cost") or 0), 0))))

    nacht = float(data.get("night_kwh") or 0)
    if nacht > 0 and kwh > 0:
        zeilen.append((T("an.night"), "%s kWh" % z(nacht, 2),
                       T("an.pct_of_day", v="%.0f" % (nacht / kwh * 100))))
        zeilen.append((T("an.day"), "%s kWh" % z(kwh - nacht, 2),
                       T("an.pct_of_day", v="%.0f" % ((kwh - nacht) / kwh * 100))))

    tops = data.get("top_hours") or []
    if tops:
        zeilen.append((T("an.busiest"),
                       ", ".join("%02dh" % h for h, _ in tops),
                       T("an.together", v=z(sum(v for _, v in tops), 2))))
    lows = data.get("low_hours") or []
    if lows:
        zeilen.append((T("an.quietest"),
                       ", ".join("%02dh" % h for h, _ in lows),
                       T("an.together", v=z(sum(v for _, v in lows), 2))))

    if data.get("spot_cost") is not None and kwh > 0:
        sc = float(data["spot_cost"])
        zeilen.append((T("an.eff_paid"), T("kpi.ct_kwh", v=z(kosten / kwh * 100, 1)),
                       T("an.fixed_tariff")))
        zeilen.append((T("an.spot"), T("kpi.ct_kwh", v=z(sc / kwh * 100, 1)),
                       T("an.same_cons")))

    if float(data.get("proj_kwh") or 0) > 0:
        zeilen.append((T("an.month_proj"),
                       "%s kWh" % z(float(data["proj_kwh"]), 0),
                       T("an.approx_eur", v=z(float(data.get("proj_cost") or 0), 0))))

    doc.paare(zeilen, spalten=2)

    # ── Wochenverlauf: gehoert zur Uebersicht, nicht in den Anhang ──
    verlauf = list(data.get("last7_days") or [])
    if len(verlauf) >= 3:
        doc.abschnitt(T("sec.last7"), braucht=100)
        doc.verlauf([(str(a), float(b)) for a, b in verlauf], preis)
        mittel = getattr(doc, "_verlauf_mittel", 0.0)
        if mittel:
            doc.hinweis(T("last7.note", v=z(mittel, 2)))

    # ── Anhang: die Einzelheiten auf einer eigenen Seite ──
    je_geraet = data.get("hourly_per_device") or {}
    if je_geraet or (stunden and any(v > 0 for v in stunden)):
        doc.seitenumbruch()

    if stunden and any(v > 0 for v in stunden):
        doc.abschnitt(T("sec.hour_by_hour"), braucht=120)
        doc.stundentabelle(stunden, preis)
    if je_geraet:
        doc.abschnitt(T("sec.hour_by_device"), braucht=140)
        doc.geraete_profile(je_geraet)
        doc.hinweis(T("devprofiles.note"))

    schluss = T("close.daily",
                period=data.get("date_label") or T("period_fallback"),
                ct=z(preis * 100, 1))
    if not bilanz:
        schluss += "  " + _bilanz_hinweis(data, lang)
    doc.hinweis(schluss)

    doc.schliessen()
    return out


# ═══════════════════════════ Monatsbericht ════════════════════════════════

def build_monthly_pdf(out: Path, data: Dict[str, Any],
                      chart_png: Optional[Path] = None,
                      lang: str = "de") -> Path:
    out.parent.mkdir(parents=True, exist_ok=True)
    lang = normalize_lang(lang)
    preis = float(data.get("unit_price") or 0.0)
    kwh = float(data.get("total_kwh") or 0.0)
    kosten = float(data.get("total_cost") or 0.0)

    def T(k: str, **kw: Any) -> str:
        return _t(lang, "report." + k, **kw)

    def z(v: float, stellen: Optional[int] = None) -> str:
        return _zahl(v, stellen, lang)

    doc = _Doc(out, T("title.monthly"), str(data.get("month_label", "")),
               T("generated"), lang=lang)

    karten: List[Dict[str, Any]] = [
        {"label": T("kpi.consumption"), "value": "%s kWh" % z(kwh, 1),
         "sub": T("kpi.per_day_kwh", v=z(float(data.get("avg_daily") or 0), 2)),
         "color": ACCENT},
        {"label": T("kpi.cost"), "value": "%s EUR" % z(kosten, 2),
         "sub": T("kpi.per_day_eur",
                  v=z(float(data.get("avg_daily_cost") or 0), 2)),
         "color": ACCENT},
    ]
    v = _vergleich(kwh, float(data.get("total_prev") or 0), preis,
                   T("kpi.vs_prev_month"), lang)
    if v:
        karten.append(v)
    if float(data.get("year_proj") or 0) > 0:
        karten.append({"label": T("kpi.year_proj"),
                       "value": "%s kWh" % z(float(data["year_proj"]), 0),
                       "sub": T("an.approx_eur",
                                v=z(float(data.get("year_cost") or 0), 0)),
                       "color": MUTED, "value_color": INK})
    doc.kpi_reihe(karten[:4])

    zweite: List[Dict[str, Any]] = []
    if float(data.get("co2_kg") or 0) > 0:
        zweite.append({"label": T("kpi.co2"),
                       "value": "%s kg" % z(float(data["co2_kg"]), 1),
                       "sub": T("kpi.grid_mix",
                                v=z(float(data.get("co2_g_per_kwh") or 0), 0)),
                       "color": MUTED, "value_color": INK})
    if float(data.get("wkday_avg") or 0) > 0:
        zweite.append({"label": T("kpi.wkday_avg"),
                       "value": "%s kWh" % z(float(data["wkday_avg"]), 2),
                       "sub": T("kpi.mon_to_fri"), "color": ACCENT,
                       "value_color": INK})
    if float(data.get("wkend_avg") or 0) > 0:
        zweite.append({"label": T("kpi.wkend_avg"),
                       "value": "%s kWh" % z(float(data["wkend_avg"]), 2),
                       "sub": T("kpi.sat_sun"), "color": NIGHT,
                       "value_color": INK})
    if kwh > 0:
        zweite.append({"label": T("kpi.eff_price"),
                       "value": T("kpi.ct_kwh", v=z(kosten / kwh * 100, 1)),
                       "sub": T("kpi.configured_tariff"), "color": MUTED,
                       "value_color": INK})
    if zweite:
        doc.kpi_reihe(zweite[:4])

    # ── Energiebilanz des Monats ──
    bilanz = _bilanz_zeilen(data, lang)
    if bilanz:
        doc.abschnitt(T("sec.balance"), braucht=90)
        doc.paare(bilanz, spalten=2)
        doc.hinweis(_bilanz_hinweis(data, lang))

    tage = list(data.get("daily_totals") or [])
    if tage and any(v > 0 for v in tage):
        doc.abschnitt(T("sec.day_by_day"), braucht=130)
        doc.stundenprofil(tage, 0.0, -1, 102.0, T("axis.day_of_month"))
        teile = []
        if data.get("best_day"):
            teile.append(T("mon.best_day", d=data["best_day"],
                           v=z(float(data.get("best_day_kwh") or 0), 2)))
        if data.get("worst_day"):
            teile.append(T("mon.worst_day", d=data["worst_day"],
                           v=z(float(data.get("worst_day_kwh") or 0), 2)))
        if teile:
            doc.hinweis("  ".join(teile))

    doc.abschnitt(T("sec.devices"), braucht=60)
    doc.geraete_tabelle(list(data.get("dev_data") or []))
    mover = data.get("biggest_mover")
    if mover and preis and abs(float(mover.get("diff", 0))) * preis >= 0.50:
        d = float(mover["diff"])
        doc.hinweis(T("mover", name=mover.get("name", "?"),
                      a=z(float(mover["from"]), 1), b=z(float(mover["to"]), 1),
                      d=_vorz(d, 1, lang), e=_vorz(d * preis, 2, lang)))

    stunden = list(data.get("hour_totals") or [])
    if stunden and any(v > 0 for v in stunden):
        doc.abschnitt(T("sec.typical_day"), braucht=130)
        doc.stundenprofil(stunden, 0.0, int(data.get("peak_hour_idx", -1)),
                          102.0, T("axis.hour_of_day"))
        doc.hinweis(T("mon.typical",
                      h="%02d" % int(data.get("peak_hour_idx", 0)),
                      v=z(float(data.get("peak_hour_kwh") or 0), 1)))

    wd = list(data.get("weekday_avgs") or [])
    labels = list(data.get("wd_labels") or [])
    if wd and labels and len(wd) == len(labels):
        doc.abschnitt(T("sec.by_weekday"), braucht=100)
        doc.verlauf([(str(a), float(b)) for a, b in zip(labels, wd)], preis)
        doc.hinweis(T("mon.by_weekday"))

    schluss = T("close.monthly",
                period=data.get("month_label") or T("period_fallback"),
                days=int(data.get("days_in_month") or 0),
                ct=z(preis * 100, 1))
    if not bilanz:
        schluss += "  " + _bilanz_hinweis(data, lang)
    doc.hinweis(schluss)
    doc.schliessen()
    return out


def build_pdf(out: Path, kind: str, data: Dict[str, Any],
              chart_png: Optional[Path] = None,
              lang: str = "de") -> Optional[Path]:
    """Einstiegspunkt fuer background.py."""
    try:
        if kind == "monthly":
            return build_monthly_pdf(out, data, chart_png, lang)
        return build_daily_pdf(out, data, chart_png, lang)
    except Exception as exc:                       # pragma: no cover
        logger.warning("PDF report failed: %s", exc)
        return None
