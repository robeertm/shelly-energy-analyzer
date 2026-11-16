#!/usr/bin/env bash
# Sourced by promote.yml: `source .github/retry-registry.sh`.
# Runs a registry command, retrying (max 3 attempts, 20 s / 60 s backoff) ONLY when
# its output looks like a transient registry/network error. Everything else fails
# at once with the original exit code. Output of every attempt stays in the log.
retry_registry() {
  local attempt=1 max=3 rc out
  local -a delay=(20 60)
  out="$(mktemp)"
  while :; do
    "$@" > "$out" 2>&1 && rc=0 || rc=$?
    cat "$out"
    if [ "$rc" -eq 0 ]; then rm -f "$out"; return 0; fi
    if [ "$attempt" -ge "$max" ] || ! grep -v -i 'permission denied' "$out" | grep -qiE \
      'denied|TLS handshake timeout|i/o timeout|connection reset|connection refused|\bEOF\b|Bad Gateway|Service Unavailable|Gateway Time-?out|(status|code|HTTP/[0-9.]+)[^0-9]{0,3}50[234]\b|too many requests|\b429\b|toomanyrequests|net/http|context deadline exceeded|no such host|Temporary failure in name resolution'; then
      rm -f "$out"; return "$rc"
    fi
    echo "::warning::registry/network error, retry $((attempt + 1))/$max in ${delay[$((attempt - 1))]}s" >&2
    sleep "${delay[$((attempt - 1))]}"
    attempt=$((attempt + 1))
  done
}
