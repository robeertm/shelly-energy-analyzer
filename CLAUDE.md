# Shelly Energy Analyzer — Arbeitsanweisung für den Werkstatt-Agenten

Der Analyzer misst Roberts Stromverbrauch (4 Shelly-Geräte), speist Home Assistant per
MQTT und rechnet Kosten/CO₂. Dieses Repo wird über die **Werkstatt** beauftragt: Robert
schreibt einen Auftrag, du setzt ihn im Code um, danach wird automatisch auf **Roberts
eigene VM** deployt.

## 🔴 Regel 1: Du veröffentlichst KEIN Release

Das hier ist **kein Privatprojekt** — es ist ein öffentliches Produkt, das **fremde Nutzer
installiert haben**. Ein Release betrifft die alle.

- **Niemals** taggen (`git tag`), **niemals** ein Release erzeugen, **niemals** den
  Release-Workflow auslösen.
- **Niemals** die Version bumpen — weder `src/shelly_analyzer/__init__.py __version__`
  noch `pyproject.toml`. Ein Versionssprung ohne Release löst bei allen Nutzern einen
  **Endlos-Update-Loop** aus (bekannter Vorfall v16.29.0).
- Dein Deploy geht **nur auf Roberts VM** und ist genau der Schritt „am Pi live testen,
  BEVOR releast wird". Das Releasen macht Robert danach bewusst von Hand.
- Ergänze deine Änderung im `CHANGELOG.md` unter einem Abschnitt **„Unreleased"** —
  nicht unter einer neuen Versionsnummer.

## 🔒 Regel 2: Es sind Abrechnungsdaten

`energy.db` und `csv_archive/` sind die Grundlage für **echte Abrechnungen** (u. a.
Mieter/Wallbox). Sie liegen auf der VM unter `~/shelly-energy-analyzer/data` — das ist ein
**Symlink** auf eine separate 100-GB-Platte und liegt **nicht** im Repo.

- Kein Code, der Messwerte löscht, überschreibt oder rückwirkend „korrigiert", ohne dass
  Robert genau das beauftragt hat.
- **DB-Migrationen nur additiv** (neue Spalte/Tabelle), niemals `DROP`/`DELETE` auf
  Messreihen. Der Code muss mit einer alten DB weiterlaufen.
- Energiezähler sind `total_increasing`: **niemals** rückwärts springen lassen — HA
  verdoppelt sonst die Statistik-Summe (echter Vorfall, v16.32.2).
- `config.json` (Geräte-IPs, VM-spezifisch) **nicht anfassen** — wird beim Deploy bewusst
  übersprungen. Neue Einstellungen brauchen einen Default im Code.

## ✅ Regel 3: Vor dem Abliefern prüfen

- Geänderte Python-Dateien mit `python3 -m ast` bzw. `ast.parse` gegenprüfen.
- Bei Frontend-Änderungen `node --check` auf das gerenderte Bundle.
- Kaputter Code = **Home Assistant bekommt keine Energiedaten mehr**. Lieber vorsichtig.

## Technisches

- **Python**, Flask/Werkzeug, uvicorn-artig über eigenes Startskript, **HTTPS auf 8765**.
- Läuft als systemd-Dienst `shelly-analyzer` in einer VM auf der Synology-NAS.
  Auf derselben VM läuft **DocuSort** — nicht anfassen.
- MQTT → Home Assistant (Broker `100.104.211.12:1883`), ~76 Sensoren, Discovery-basiert.
  ⚠️ `entity_id` wird bei der **Erst**-Discovery aus dem Namen gebildet und ist danach fix —
  Umbenennungen ändern nur den Anzeigenamen. Niemals `unique_id`-Schema ändern, sonst
  verlieren alle HA-Entities ihre Historie.
- Neue Abhängigkeiten in `requirements.txt` (werden beim Deploy installiert).

## Deploy — machst du NICHT selbst

Dateien ändern → committen → nach `main` pushen. Den Rest erledigt der Workflow: die VM
holt `main`, synchronisiert **nur den Code** (ohne `data`, `config.json`, `logs`, `.venv`)
und startet den Dienst neu.

## Ton

Antworte am Ende kurz und in einfachen Worten: was ist neu, wo sieht man es.
