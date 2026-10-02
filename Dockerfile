# Shelly Energy Analyzer — container image
#
# One process does the whole job: it polls the Shellys over their local HTTP
# API, keeps the SQLite database, publishes to MQTT for Home Assistant and
# serves the Flask dashboard. Everything it owns lives under /data, which you
# mount — the image carries code, never state.
FROM python:3.12-slim

LABEL org.opencontainers.image.title="Shelly Energy Analyzer" \
      org.opencontainers.image.description="Polls Shelly meters, keeps the history, publishes to MQTT for Home Assistant and serves a live dashboard." \
      org.opencontainers.image.source="https://github.com/robeertm/shelly-energy-analyzer"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=Europe/Berlin \
    SEA_PORT=8765 \
    TS_STATE_DIR=/data/tailscale \
    TS_HOSTNAME=shelly-energy-analyzer

# Build tools for wheels that may need compiling (numpy/pandas ship wheels, but
# pymodbus/reportlab/pillow occasionally need a compiler on slim images).
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

# ── Tailscale, carried in the image ─────────────────────────────────────────
# 🔑 WHY IN THE IMAGE AND NOT AS A SIDECAR
# This service used to live in a VM that was on the tailnet as
# `shelly-energy-analyzer`. Home Assistant, bookmarks and the dashboard all
# know that name. Carrying Tailscale inside means the container can take over
# the SAME identity — nothing that points at the old name has to be touched.
#
# 🔑 WHY IT WORKS WITHOUT PRIVILEGES — measured on this NAS, not hoped:
# `tailscaled --tun=userspace-networking` needs neither `NET_ADMIN` nor
# `/dev/net/tun`. The same construction already runs in the Postwache and
# DocuSort images on the same machine.
#
# 🔴 A PINNED VERSION NEVER MOVES BY ITSELF. The image is rebuilt on every
# push, but Tailscale stays on exactly this number. A build that pulls "the
# newest" is not repeatable and would let a bad release in silently.
ARG TARGETARCH
ARG TAILSCALE_VERSION=1.102.4
RUN set -eux; \
    curl -fsSL "https://pkgs.tailscale.com/stable/tailscale_${TAILSCALE_VERSION}_${TARGETARCH}.tgz" \
      -o /tmp/ts.tgz; \
    tar xzf /tmp/ts.tgz -C /tmp; \
    mv /tmp/tailscale_${TAILSCALE_VERSION}_${TARGETARCH}/tailscaled /usr/local/bin/; \
    mv /tmp/tailscale_${TAILSCALE_VERSION}_${TARGETARCH}/tailscale  /usr/local/bin/; \
    rm -rf /tmp/ts.tgz /tmp/tailscale_*; \
    tailscaled --version

WORKDIR /app
COPY . /app
RUN pip install .

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# 🔴 THE WORKING DIRECTORY IS PART OF THE CONTRACT.
# `io/storage.py` resolves the database as `Path.cwd() / "data" / "energy.db"`.
# With /data as the working directory the history therefore lives at
# /data/data/energy.db — exactly the layout the migration writes.
VOLUME ["/data"]
WORKDIR /data
EXPOSE 8765

# 🔑 The check asks the app's own version endpoint — it does no work and needs
# no database read.
HEALTHCHECK --interval=60s --timeout=5s --start-period=60s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8765/api/version || exit 1

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
