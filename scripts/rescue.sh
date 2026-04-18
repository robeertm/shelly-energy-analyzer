#!/usr/bin/env bash
# Shelly Energy Analyzer — one-shot rescue script (Linux + macOS).
#
# Recovers installs that got stuck after clicking "Install update" on a
# release older than v16.26.1, where the in-app updater dies under systemd /
# launchd cgroup cleanup and leaves the service in "inactive (dead)" state
# with a stale lock file and a staged but never-applied update in /tmp.
#
# What this script does (in order):
#   1. Detects the install directory (CWD, $HOME/shelly-energy-analyzer,
#      $HOME/shelly_energy_analyzer_v6, or user-provided via --app-dir).
#   2. Stops the running service if any (systemd, launchd, or plain pgrep).
#   3. Cleans up the stale single-instance lock and any half-staged update.
#   4. Either `git reset --hard` to the latest tag (when the install is a
#      git checkout) or downloads + extracts the release ZIP.
#   5. Runs `pip install -e .` inside the existing venv so the module entry
#      point picks up the new code.
#   6. Starts the service again.
#   7. Verifies the API answers on https://localhost:PORT/api/version.
#
# Safe: config.json, data/, logs/, .venv/ are preserved. Uses your existing
# virtualenv — nothing is installed system-wide.
#
# Usage (Linux — systemd service):
#   curl -sSL https://raw.githubusercontent.com/robeertm/shelly-energy-analyzer/main/scripts/rescue.sh | sudo -E bash
#
# Usage (macOS — launchd or plain terminal):
#   curl -sSL https://raw.githubusercontent.com/robeertm/shelly-energy-analyzer/main/scripts/rescue.sh | bash
#
# Options via env vars:
#   SEA_APP_DIR=/path/to/install    Override install-dir detection
#   SEA_TAG=v16.26.3                Pin a specific release (default: latest)
#   SEA_PORT=8765                   Override the expected port (default: 8765)
#   SEA_NO_RESTART=1                Do everything but restart the service
set -euo pipefail

# If we were launched via `sudo ... bash`, HOME is root's. Restore the
# calling user's HOME so install-dir detection finds their checkout, and
# remember which user we should run pip/git as.
if [[ -n "${SUDO_USER:-}" ]] && [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
  REAL_USER="$SUDO_USER"
  REAL_HOME="$(getent passwd "$SUDO_USER" | cut -d: -f6)"
  [[ -n "$REAL_HOME" ]] && export HOME="$REAL_HOME"
else
  REAL_USER="$(id -un)"
fi

# Helper: run a command as the install's owner (not as root). Falls back
# to plain execution when we're not root.
as_user() {
  if [[ "${EUID:-$(id -u)}" -eq 0 ]] && [[ "$REAL_USER" != "root" ]]; then
    sudo -u "$REAL_USER" "$@"
  else
    "$@"
  fi
}

RED=$'\033[31m'; YEL=$'\033[33m'; GRN=$'\033[32m'; BLU=$'\033[34m'; CLR=$'\033[0m'
log()  { printf '%s[rescue]%s %s\n' "$BLU" "$CLR" "$*"; }
warn() { printf '%s[rescue]%s %s\n' "$YEL" "$CLR" "$*" >&2; }
err()  { printf '%s[rescue]%s %s\n' "$RED" "$CLR" "$*" >&2; }
ok()   { printf '%s[rescue]%s %s\n' "$GRN" "$CLR" "$*"; }

REPO="robeertm/shelly-energy-analyzer"
DEFAULT_PORT="${SEA_PORT:-8765}"

# ── 1. Locate install dir ────────────────────────────────────────────────
find_app_dir() {
  if [[ -n "${SEA_APP_DIR:-}" ]]; then
    echo "$SEA_APP_DIR"; return 0
  fi
  local candidates=(
    "$PWD"
    "$HOME/shelly-energy-analyzer"
    "$HOME/shelly_energy_analyzer_v6"
    "/opt/shelly-energy-analyzer"
  )
  for d in "${candidates[@]}"; do
    if [[ -f "$d/pyproject.toml" ]] && grep -q 'shelly-energy-analyzer\|shelly_analyzer' "$d/pyproject.toml" 2>/dev/null; then
      echo "$d"; return 0
    fi
  done
  return 1
}

APP_DIR="$(find_app_dir)" || {
  err "Couldn't find a Shelly Energy Analyzer install. Pass SEA_APP_DIR=/path/to/install."
  exit 1
}
APP_DIR="$(cd "$APP_DIR" && pwd)"
log "install dir: $APP_DIR"

# Preflight: must contain our layout
[[ -d "$APP_DIR/src/shelly_analyzer" ]] || {
  err "$APP_DIR is missing src/shelly_analyzer — not a valid install."
  exit 1
}

# Current installed version (best-effort)
CURRENT_VER="$(grep -oE '__version__\s*=\s*"[^"]+"' "$APP_DIR/src/shelly_analyzer/__init__.py" 2>/dev/null | head -1 | sed -E 's/.*"([^"]+)"/\1/' || true)"
log "current version: ${CURRENT_VER:-unknown}"

# ── 2. Detect + stop service ─────────────────────────────────────────────
SERVICE_KIND="none"   # systemd | launchd | plain | none
SERVICE_NAME=""

detect_service() {
  case "$(uname -s)" in
    Linux)
      if command -v systemctl >/dev/null 2>&1; then
        local name
        name="$(systemctl list-unit-files --type=service --no-legend 2>/dev/null | awk '/shelly/{print $1; exit}')"
        if [[ -n "$name" ]]; then
          SERVICE_KIND="systemd"; SERVICE_NAME="$name"; return
        fi
      fi
      ;;
    Darwin)
      if command -v launchctl >/dev/null 2>&1; then
        local name
        name="$(launchctl list 2>/dev/null | awk 'tolower($3) ~ /shelly/{print $3; exit}')"
        if [[ -n "$name" ]]; then
          SERVICE_KIND="launchd"; SERVICE_NAME="$name"; return
        fi
      fi
      ;;
  esac
  # Fall back to pgrep
  if pgrep -f "shelly_analyzer" >/dev/null 2>&1; then
    SERVICE_KIND="plain"
  fi
}

stop_service() {
  case "$SERVICE_KIND" in
    systemd)
      log "stopping systemd service $SERVICE_NAME"
      if [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
        systemctl stop "$SERVICE_NAME"
      elif sudo -n true 2>/dev/null; then
        sudo systemctl stop "$SERVICE_NAME"
      else
        err "need root for 'systemctl stop $SERVICE_NAME'. Re-run as:"
        err "  curl -sSL https://raw.githubusercontent.com/robeertm/shelly-energy-analyzer/main/scripts/rescue.sh | sudo -E bash"
        exit 2
      fi
      ;;
    launchd)
      log "stopping launchd job $SERVICE_NAME"
      launchctl stop "$SERVICE_NAME" 2>/dev/null || true
      ;;
    plain)
      log "sending SIGTERM to running shelly_analyzer processes"
      pkill -f "shelly_analyzer" || true
      ;;
    none)
      log "no running service detected"
      ;;
  esac
  # Wait up to 10 s for the process to actually exit
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    if ! pgrep -f "shelly_analyzer" >/dev/null 2>&1; then return; fi
    sleep 1
  done
  warn "shelly_analyzer still running after 10s — continuing anyway"
}

start_service() {
  if [[ "${SEA_NO_RESTART:-0}" == "1" ]]; then
    warn "SEA_NO_RESTART=1 set, skipping start"
    return
  fi
  case "$SERVICE_KIND" in
    systemd)
      log "starting systemd service $SERVICE_NAME"
      if [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
        systemctl start "$SERVICE_NAME"
      else
        sudo systemctl start "$SERVICE_NAME"
      fi
      ;;
    launchd)
      log "starting launchd job $SERVICE_NAME"
      launchctl start "$SERVICE_NAME" || true
      ;;
    plain|none)
      warn "no managed service: start the app manually (e.g. ./start.sh or ./start.command)"
      ;;
  esac
}

detect_service
log "service backend: $SERVICE_KIND${SERVICE_NAME:+ ($SERVICE_NAME)}"
stop_service

# ── 3. Clean up stale artefacts ──────────────────────────────────────────
rm -f "$APP_DIR/.shelly_analyzer.lock"
rm -rf /tmp/sea_update_* /tmp/tmp*.zip 2>/dev/null || true
# Reset dev-build noise so git reset doesn't complain
if [[ -d "$APP_DIR/.git" ]]; then
  (cd "$APP_DIR" && git checkout -- src/shelly_energy_analyzer.egg-info/ 2>/dev/null) || true
fi

# ── 4. Pull fresh code ───────────────────────────────────────────────────
TAG="${SEA_TAG:-}"
if [[ -z "$TAG" ]]; then
  log "fetching latest release tag from GitHub"
  TAG="$(curl -sSfL "https://api.github.com/repos/$REPO/releases/latest" 2>/dev/null \
        | grep -E '"tag_name"' | head -1 | sed -E 's/.*"([^"]+)".*/\1/' || true)"
  if [[ -z "$TAG" ]]; then
    err "could not determine latest tag (GitHub API unreachable or rate-limited). Set SEA_TAG=vX.Y.Z manually."
    exit 1
  fi
fi
log "target release: $TAG"

if [[ -d "$APP_DIR/.git" ]]; then
  log "using git to reset to $TAG (as user $REAL_USER)"
  as_user bash -c "cd '$APP_DIR' && git fetch --tags --quiet origin && git reset --hard '$TAG' --quiet"
else
  log "no git checkout — downloading release ZIP"
  case "$(uname -s)" in
    Linux)  ASSET="shelly_energy_analyzer_${TAG}_linux.zip" ;;
    Darwin) ASSET="shelly_energy_analyzer_${TAG}_macos.zip" ;;
    *)      err "unsupported OS: $(uname -s)"; exit 1 ;;
  esac
  URL="https://github.com/$REPO/releases/download/$TAG/$ASSET"
  TMP_ZIP="$(mktemp -t sea_rescue.XXXXXX).zip"
  trap 'rm -f "$TMP_ZIP"' EXIT
  log "downloading $URL"
  curl -sSfL --retry 3 -o "$TMP_ZIP" "$URL" || { err "download failed"; exit 1; }
  STAGING="$(mktemp -d -t sea_rescue_staging.XXXXXX)"
  trap 'rm -f "$TMP_ZIP"; rm -rf "$STAGING"' EXIT
  log "extracting to $STAGING"
  (cd "$STAGING" && unzip -q "$TMP_ZIP")
  # If there's a single top-level dir, descend into it
  entries=( "$STAGING"/* )
  if [[ ${#entries[@]} -eq 1 && -d "${entries[0]}" ]]; then
    STAGING="${entries[0]}"
  fi
  log "copying files to $APP_DIR (preserving config.json, data/, logs/, .venv/)"
  # Mirror updater_helper EXCLUDE_NAMES so user data survives
  EXCLUDES=(
    --exclude=".venv" --exclude="data" --exclude="logs"
    --exclude="config.json" --exclude="config.example.json"
    --exclude=".git" --exclude=".github" --exclude=".claude" --exclude=".vscode"
    --exclude="__pycache__" --exclude="docs" --exclude="*.pyc" --exclude=".DS_Store"
  )
  if command -v rsync >/dev/null 2>&1; then
    rsync -a "${EXCLUDES[@]}" "$STAGING/" "$APP_DIR/"
  else
    # tar-based fallback — works on bare macOS too
    ( cd "$STAGING" && tar --exclude=".venv" --exclude="data" --exclude="logs" \
                          --exclude="config.json" --exclude="config.example.json" \
                          --exclude=".git" --exclude=".github" --exclude=".claude" --exclude=".vscode" \
                          --exclude="__pycache__" --exclude="docs" --exclude="*.pyc" --exclude=".DS_Store" \
                          -cf - . | (cd "$APP_DIR" && tar -xf -) )
  fi
fi

NEW_VER="$(grep -oE '__version__\s*=\s*"[^"]+"' "$APP_DIR/src/shelly_analyzer/__init__.py" | head -1 | sed -E 's/.*"([^"]+)"/\1/')"
log "files replaced — installed version: $NEW_VER"

# Clear stale bytecode
find "$APP_DIR/src" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

# ── 5. Refresh venv editable install (cheap — no-op if already present) ──
VENV_PY=""
if [[ -x "$APP_DIR/.venv/bin/python" ]]; then
  VENV_PY="$APP_DIR/.venv/bin/python"
elif [[ -x "$APP_DIR/.venv/Scripts/python.exe" ]]; then
  VENV_PY="$APP_DIR/.venv/Scripts/python.exe"
fi
if [[ -n "$VENV_PY" ]]; then
  log "refreshing venv editable install (as user $REAL_USER)"
  as_user "$VENV_PY" -m pip install -q -e "$APP_DIR" >/dev/null 2>&1 || warn "pip install -e . warned (not fatal)"
  if [[ -f "$APP_DIR/requirements.txt" ]]; then
    as_user "$VENV_PY" -m pip install -q -r "$APP_DIR/requirements.txt" >/dev/null 2>&1 || warn "requirements install warned (not fatal)"
  fi
else
  warn "no .venv found in $APP_DIR — skipping pip"
fi

# ── 6. Restart ───────────────────────────────────────────────────────────
start_service

# ── 7. Verify ────────────────────────────────────────────────────────────
if [[ "${SEA_NO_RESTART:-0}" != "1" && "$SERVICE_KIND" != "none" && "$SERVICE_KIND" != "plain" ]]; then
  for i in 1 2 3 4 5 6 7 8 9 10; do
    sleep 2
    out="$(curl -sk -m 3 "https://localhost:$DEFAULT_PORT/api/version" 2>/dev/null || true)"
    if [[ -n "$out" && "$out" == *"$NEW_VER"* ]]; then
      ok "service is up: $out"
      exit 0
    fi
  done
  warn "API didn't respond with version $NEW_VER within 20s — check the service logs."
  case "$SERVICE_KIND" in
    systemd)  warn "journalctl -u $SERVICE_NAME -n 50 --no-pager" ;;
    launchd)  warn "launchctl print user/$(id -u)/$SERVICE_NAME  (or inspect /Library/Logs)" ;;
  esac
  exit 1
fi

ok "rescue complete — version $NEW_VER installed"
