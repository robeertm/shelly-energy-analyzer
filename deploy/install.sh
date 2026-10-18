#!/usr/bin/env sh
# Shelly Energy Analyzer — one-command Docker install.
#
#   curl -fsSL https://raw.githubusercontent.com/robeertm/shelly-energy-analyzer/main/deploy/install.sh | sh
#
# Writes a docker-compose.yml into $SEA_DIR (default ./shelly-energy-analyzer),
# pulls the published image and starts it, together with a Watchtower that
# keeps it up to date. Re-running is safe: an existing docker-compose.yml is
# kept, never overwritten.
#
# Optional, as environment variables in front of `sh`:
#   SEA_DIR=/volume1/docker/shelly     where everything goes
#   SEA_PORT=8765                      port of the dashboard
#   SEA_HOST_NETWORK=1                 host network (Linux): mDNS discovery works
#   TS_AUTHKEY=tskey-auth-...          also join your tailnet (written to .env, 0600)
set -eu

DIR="${SEA_DIR:-./shelly-energy-analyzer}"
PORT="${SEA_PORT:-8765}"
IMAGE="${SEA_IMAGE:-ghcr.io/robeertm/shelly-energy-analyzer:latest}"
TZ_DEFAULT="${TZ:-Europe/Berlin}"
HOSTNET="${SEA_HOST_NETWORK:-0}"

say()  { printf '  %s\n' "$1"; }
warn() { printf '  ! %s\n' "$1"; }
die()  { printf '\n  %s\n\n' "$1"; exit 1; }

command -v docker >/dev/null 2>&1 || die "Docker is missing. Install Docker, then run this again."
if docker compose version >/dev/null 2>&1; then
  COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE="docker-compose"
else
  die "Docker Compose is missing. Install the compose plugin and run this again."
fi

printf '\n  Shelly Energy Analyzer\n\n'
say "Installing into $DIR"
# 🔴 Create every directory the compose file mounts. Plain Docker creates a
# missing bind source itself (as root); Synology's Container Manager refuses
# with "Bind mount failed: … does not exist".
mkdir -p "$DIR/data"

if [ "$HOSTNET" = "1" ]; then
  NETZ="    network_mode: host
    # Host network: mDNS discovery of Shellys works, the dashboard listens on
    # port 8765 of this machine directly."
  [ "$PORT" = "8765" ] || warn "SEA_PORT is ignored with the host network — the dashboard uses 8765."
  PORT=8765
else
  NETZ="    ports:
      - \"$PORT:8765\"
    # Device discovery (mDNS) cannot see through Docker's bridge network.
    # Adding Shellys by IP address always works. For discovery on Linux,
    # replace \`ports:\` with \`network_mode: host\`."
fi

if [ -f "$DIR/docker-compose.yml" ]; then
  warn "docker-compose.yml exists — keeping it."
else
  cat > "$DIR/docker-compose.yml" <<YAML
services:
  shelly-energy-analyzer:
    image: $IMAGE
    container_name: shelly-energy-analyzer
    restart: unless-stopped
$NETZ
    environment:
      - TZ=\${TZ:-$TZ_DEFAULT}
      # Optional: reach the dashboard over Tailscale. Put TS_AUTHKEY into .env.
      - TS_AUTHKEY=\${TS_AUTHKEY:-}
    volumes:
      - ./data:/data

  # Keeps the analyzer up to date, nightly at 04:00. Inside the container the
  # in-app updater is switched off on purpose; this is how new versions arrive.
  # It needs the Docker socket and watches ONLY the analyzer. Delete this
  # service to update by hand: docker compose pull && docker compose up -d
  watchtower:
    image: ghcr.io/nicholas-fedor/watchtower:latest
    container_name: shelly-energy-analyzer-watchtower
    restart: unless-stopped
    environment:
      - TZ=\${TZ:-$TZ_DEFAULT}
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
    command:
      - --cleanup
      # Without this flag a container that keeps restarting is invisible to
      # Watchtower — a broken version would never be replaced by a good one.
      - --include-restarting
      - --schedule
      - \${WATCHTOWER_SCHEDULE:-0 0 4 * * *}
      - shelly-energy-analyzer
YAML
  say "Wrote $DIR/docker-compose.yml"
fi

if [ -n "${TS_AUTHKEY:-}" ]; then
  if [ -f "$DIR/.env" ] && grep -q '^TS_AUTHKEY=' "$DIR/.env"; then
    warn ".env already has a TS_AUTHKEY — keeping it."
  else
    ( umask 077; printf 'TS_AUTHKEY=%s\n' "$TS_AUTHKEY" >> "$DIR/.env" )
    chmod 600 "$DIR/.env" 2>/dev/null || true
    say "Wrote TS_AUTHKEY to $DIR/.env (readable only by you)"
  fi
fi

say "Pulling $IMAGE"
( cd "$DIR" && $COMPOSE pull && $COMPOSE up -d )

# `hostname -I` exists only on Linux; never let the summary die on it.
HOST="$( (hostname -I 2>/dev/null || true) | awk '{print $1}')"
[ -n "$HOST" ] || HOST="$( (ipconfig getifaddr en0 2>/dev/null || true) )"
[ -n "$HOST" ] || HOST="localhost"
cat <<DONE

  The Shelly Energy Analyzer is starting (first start takes up to a minute).

    Dashboard     http://$HOST:$PORT
    Its data      $DIR/data

  Open the dashboard and follow the setup wizard — demo mode works without
  any Shelly at all.

  Updates       arrive by themselves every night (Watchtower, 04:00).
                By hand: cd $DIR && $COMPOSE pull && $COMPOSE up -d
  Logs          cd $DIR && $COMPOSE logs -f shelly-energy-analyzer

DONE
