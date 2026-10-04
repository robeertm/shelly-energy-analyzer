#!/usr/bin/env bash
# Faehrt EIN veroeffentlichtes Abbild an und sagt, ob es laeuft.
#
#   ./.github/rauchprobe.sh <abbild> <datenverzeichnis> <name> <port> [erwartete_version]
#
# 🔴 WARUM DAS HIER STEHT (04.10.2026)
#
# `docker.yml` setzte `:latest`, sobald ein Commit auf main landete, und
# Watchtower verteilt dieses Etikett. Geprueft wurde am gebauten Abbild nichts.
# Bei DocuSort ging so eine Fassung an Roberts Kunden, die auf bestehenden
# Datenbanken nicht startete — verteilt binnen einer Stunde, und ohne Eingriff
# nicht zurueckgekommen, weil Watchtower abstuerzende Container ueberspringt.
#
# Drei Fragen, und jede einzelne war dort die, die den Fehler gesehen haette:
#
#   1. antwortet `/api/version` mit 200?   (HTTP 000 war die Antwort)
#   2. steht ein Absturz im Protokoll?     (7 Treffer waren es)
#   3. haengt es in einer Startschleife?   (restarting, restarts=10)
#
# 🔑 Die erste Frage allein genuegt nicht: ein Container in einer
# Neustartschleife antwortet zwischendurch. Darum alle drei.
#
# 🔑 `/api/version` liefert die Version mit — die Probe sagt also, WELCHE
# Fassung da wirklich laeuft, nicht welche draufsteht.
set -euo pipefail

ABBILD="${1:?Abbild fehlt}"
DATEN="${2:?Datenverzeichnis fehlt}"
NAME="${3:?Name fehlt}"
PORT="${4:?Port fehlt}"
ERWARTET="${5:-}"

mkdir -p "$DATEN"
docker rm -f "$NAME" >/dev/null 2>&1 || true

echo "── $NAME: $ABBILD auf $DATEN (Port $PORT) ──"
# 🔴 `--restart unless-stopped` ist kein Beiwerk: ohne Neustartregel bleibt ein
# Absturz ein stilles `exited` und `RestartCount` steht auf 0. Mit ihr sieht man
# die Startschleife, die der Benutzer auch sieht.
docker run -d --name "$NAME" --restart unless-stopped \
  -p "127.0.0.1:${PORT}:8765" \
  -v "${DATEN}:/data" \
  -e TZ=Europe/Berlin \
  "$ABBILD" >/dev/null

fehler=0
antwort=""
code="000"
# 🔑 180 s: das Abbild bringt numpy/pandas/pillow mit, und der erste Start legt
# Datenbank und Zwischenspeicher an. Die HEALTHCHECK-Zeile im Dockerfile gibt
# ihm selbst 60 s Anlauf.
for _ in $(seq 1 90); do
    code="$(curl -s -o /tmp/rauch-$NAME.json -w '%{http_code}' "http://127.0.0.1:${PORT}/api/version" || echo 000)"
    if [ "$code" = "200" ]; then antwort="$(cat /tmp/rauch-$NAME.json)"; break; fi
    sleep 2
done
if [ "$code" = "200" ]; then
    echo "  OK   /api/version antwortet 200  — $antwort"
else
    echo "  🔴   /api/version antwortet $code"
    fehler=1
fi

if [ -n "$antwort" ] && [ -n "$ERWARTET" ]; then
    LAEUFT="$(printf '%s' "$antwort" | sed -n 's/.*"version"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')"
    if [ "$LAEUFT" = "$ERWARTET" ]; then
        echo "  OK   es laeuft wirklich $ERWARTET"
    else
        echo "  🔴   erwartet war $ERWARTET, es laeuft ${LAEUFT:-unbekannt}"
        fehler=1
    fi
fi

# 🔑 `grep -c` mit `|| true`: ohne Treffer gibt grep 1 zurueck und `set -e`
# wuerde die Probe beenden, bevor sie ihr Urteil sagen kann.
treffer="$(docker logs "$NAME" 2>&1 | grep -c 'Traceback\|OperationalError\|SyntaxError' || true)"
if [ "$treffer" = "0" ]; then
    echo "  OK   kein Absturz im Protokoll"
else
    echo "  🔴   $treffer Absturz-Spuren im Protokoll"
    fehler=1
fi

status="$(docker inspect -f '{{.State.Status}}' "$NAME")"
neustarts="$(docker inspect -f '{{.RestartCount}}' "$NAME")"
if [ "$status" = "running" ] && [ "$neustarts" = "0" ]; then
    echo "  OK   laeuft, keine Neustarts"
else
    echo "  🔴   Status=$status Neustarts=$neustarts"
    fehler=1
fi

if [ "$fehler" != "0" ]; then
    echo "── Protokoll von $NAME ──"
    docker logs "$NAME" 2>&1 | tail -60
fi

# Der Container geht weg, das Datenverzeichnis BLEIBT — der Aufstiegstest
# braucht genau das, was die vorige Fassung hinterlassen hat.
docker rm -f "$NAME" >/dev/null 2>&1 || true
rm -f "/tmp/rauch-$NAME.json"

if [ "$fehler" != "0" ]; then
    echo "🔴 NICHT AUSLIEFERN — $ABBILD"
    exit 1
fi
echo "✅ $ABBILD ist angefahren und laeuft"
