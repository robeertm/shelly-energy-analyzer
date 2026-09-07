"""The invoice address could be typed but never changed.

Robert: „im shelly analyzer lässt sich unter einstellungen bei rechnung keine
adresse eintragen, die verschwindet immer".

The settings page sends `billing.issuer.address` — one string from a textarea.
The config model stores `address_lines` — a list. `_cfg_to_json` emits BOTH, and
`PUT /api/settings` deep-merges the incoming keys into that dump. So after a
save the object holds the NEW `address` next to the OLD `address_lines` — and
the parser only looked at `address` when `address_lines` was missing:

    lines = obj.get("address_lines")
    if not isinstance(lines, list):        # ← never true after the first save
        addr_str = obj.get("address", "")

The typed text was therefore written to config.json, dropped on reload, and the
previous address came back. Exactly "it disappears".

These tests go through the real save/load pair, not a re-implementation.
"""
import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.io.config import load_config, save_config  # noqa: E402


def _deep_merge(dst, src):
    """The merge PUT /api/settings performs, in miniature."""
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _deep_merge(dst[k], v)
        else:
            dst[k] = v
    return dst


def _speichern_wie_die_oberflaeche(pfad, eingabe):
    """Was der Endpunkt tut: aktuellen Stand dumpen, mergen, schreiben, neu laden."""
    from shelly_analyzer.web.blueprints.settings import _cfg_to_json  # noqa: WPS433
    cfg = load_config(pfad)
    aktuell = _cfg_to_json(cfg)
    _deep_merge(aktuell, eingabe)
    Path(pfad).write_text(json.dumps(aktuell, ensure_ascii=False, indent=2), encoding="utf-8")
    return load_config(pfad)


def _frisch():
    d = tempfile.mkdtemp()
    p = os.path.join(d, "config.json")
    Path(p).write_text('{"devices": []}', encoding="utf-8")
    return p


def test_a_typed_address_survives_the_save():
    """🔴 Der gemeldete Fehler: eintippen, speichern, und es steht wieder das Alte da."""
    p = _frisch()
    cfg = _speichern_wie_die_oberflaeche(p, {"billing": {"issuer": {"address": "Example Street 1\n12345 Example City"}}})
    assert cfg.billing.issuer.address_lines == ["Example Street 1", "12345 Example City"], \
        cfg.billing.issuer.address_lines
    print("OK  erste Eingabe kommt an")

    # ...und jetzt das Entscheidende: ein ZWEITES Mal ändern.
    cfg = _speichern_wie_die_oberflaeche(p, {"billing": {"issuer": {"address": "Other Road 2\n54321 Other Town"}}})
    assert cfg.billing.issuer.address_lines == ["Other Road 2", "54321 Other Town"], \
        ("die zweite Eingabe wurde verworfen: %r" % (cfg.billing.issuer.address_lines,))
    print("OK  Änderung einer bestehenden Adresse kommt an")


def test_the_customer_address_too():
    p = _frisch()
    _speichern_wie_die_oberflaeche(p, {"billing": {"customer": {"address": "A 1\nB 2"}}})
    cfg = _speichern_wie_die_oberflaeche(p, {"billing": {"customer": {"address": "C 3\nD 4"}}})
    assert cfg.billing.customer.address_lines == ["C 3", "D 4"], cfg.billing.customer.address_lines
    print("OK  gilt für Rechnungsempfänger genauso")


def test_multiline_stays_multiline():
    """Das Feld heißt „multiline" — dann müssen auch mehrere Zeilen ankommen."""
    p = _frisch()
    cfg = _speichern_wie_die_oberflaeche(p, {"billing": {"issuer": {"address": "Zeile 1\nZeile 2\nZeile 3"}}})
    assert len(cfg.billing.issuer.address_lines) == 3, cfg.billing.issuer.address_lines
    print("OK  drei Zeilen bleiben drei Zeilen")


def test_an_empty_address_does_not_wipe_the_lines():
    """Ein leeres Feld ist keine Anweisung zu löschen — sonst reicht ein Speichern
    einer anderen Sektion, um die Anschrift zu verlieren."""
    p = _frisch()
    _speichern_wie_die_oberflaeche(p, {"billing": {"issuer": {"address": "X 1\nY 2"}}})
    cfg = _speichern_wie_die_oberflaeche(p, {"billing": {"issuer": {"name": "Neu"}}})
    assert cfg.billing.issuer.address_lines == ["X 1", "Y 2"], cfg.billing.issuer.address_lines
    assert cfg.billing.issuer.name == "Neu"
    print("OK  Speichern ohne Adressfeld lässt die Anschrift stehen")


def test_a_config_written_by_hand_still_works():
    """Wer nur `address_lines` in die Datei schreibt, muss weiter bedient werden."""
    p = _frisch()
    Path(p).write_text(json.dumps({
        "devices": [],
        "billing": {"issuer": {"name": "N", "address_lines": ["L1", "L2"]}},
    }), encoding="utf-8")
    cfg = load_config(p)
    assert cfg.billing.issuer.address_lines == ["L1", "L2"], cfg.billing.issuer.address_lines
    print("OK  reine address_lines-Konfiguration funktioniert weiter")


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items())
           if k.startswith("test_") and callable(v)]
    for f in fns:
        f()
    print("\n%d Tests bestanden" % len(fns))
