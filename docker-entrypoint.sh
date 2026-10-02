#!/bin/sh
# Start Tailscale (when it has an identity or a key), then the analyzer.
#
# 🔴 TLS IS TAILSCALE'S JOB HERE, NOT THE APP'S.
# In the VM the app served HTTPS itself from a certificate file that somebody
# had to renew. Behind `tailscale serve` the certificate is fetched and renewed
# by Tailscale, so the app speaks plain HTTP on 8765 and `--no-ssl` is correct.
set -eu

SOCKET=/tmp/sea-tailscaled.sock
PORT="${SEA_PORT:-8765}"

starte_tailscale() {
  mkdir -p "$TS_STATE_DIR"
  # 🔑 Userspace networking: no NET_ADMIN, no /dev/net/tun. Outgoing traffic to
  #    the Shellys and the MQTT broker keeps using the normal Docker network —
  #    only tailnet traffic goes through this.
  tailscaled --tun=userspace-networking \
             --state="$TS_STATE_DIR/tailscaled.state" \
             --socket="$SOCKET" \
             --statedir="$TS_STATE_DIR" >/var/log/tailscaled.log 2>&1 &
  # „started" is not „answers". Ask, do not assume.
  i=0
  while [ "$i" -lt 30 ]; do
    if tailscale --socket="$SOCKET" status >/dev/null 2>&1; then break; fi
    if tailscale --socket="$SOCKET" status 2>&1 | grep -q "Logged out"; then break; fi
    i=$((i + 1)); sleep 0.5
  done

  if [ -n "${TS_AUTHKEY:-}" ]; then
    tailscale --socket="$SOCKET" up --authkey="$TS_AUTHKEY" \
      --hostname="${TS_HOSTNAME:-shelly-energy-analyzer}" --accept-dns=false || true
  else
    tailscale --socket="$SOCKET" up \
      --hostname="${TS_HOSTNAME:-shelly-energy-analyzer}" --accept-dns=false || true
  fi

  # 🔴 `serve` returning 0 means „accepted", not „is in place". The check after
  #    it costs one call and is the difference between a promise and a
  #    measurement. Without MagicDNS and HTTPS Certificates in the tailnet this
  #    step fails — and then the page is not reachable over Tailscale at all.
  if tailscale --socket="$SOCKET" serve --bg --https=443 "http://127.0.0.1:${PORT}"; then
    if tailscale --socket="$SOCKET" serve status --json 2>/dev/null | grep -q '"HTTPS": *true'; then
      echo "Tailscale: serving https://$(tailscale --socket=$SOCKET status --json 2>/dev/null \
            | sed -n 's/.*"DNSName": *"\([^"]*\)\.".*/\1/p' | head -1)"
    else
      echo "Tailscale: serve was accepted but nothing is published on 443." >&2
      echo "Tailscale: that is what it looks like when MagicDNS and HTTPS Certificates are off in the tailnet." >&2
    fi
  else
    echo "Tailscale: could not publish the page — the dashboard is still reachable on port ${PORT}." >&2
  fi
}

if [ -n "${TS_AUTHKEY:-}" ] || [ -f "${TS_STATE_DIR}/tailscaled.state" ]; then
  starte_tailscale || echo "Tailscale: did not start — carrying on without it." >&2
else
  echo "Tailscale: no identity and no key — starting without it."
fi

exec python -m shelly_analyzer --config /data/config.json \
     --host 0.0.0.0 --port "$PORT" --no-ssl "$@"
