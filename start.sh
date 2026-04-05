#!/usr/bin/env bash
set -euo pipefail

# Ensure we run from this folder
cd "$(dirname "$0")"

choose_py() {
  local candidates=(python3 python)
  for py in "${candidates[@]}"; do
    if command -v "$py" >/dev/null 2>&1; then
      echo "$py"
      return 0
    fi
  done
  return 1
}

PY=$(choose_py) || {
  echo "No Python 3 found. Please install Python 3.10+."
  exit 1
}

# Create venv if missing
if [[ ! -d .venv ]]; then
  "$PY" -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate

VENV_PY="$(pwd)/.venv/bin/python"

"$VENV_PY" -m pip install -U pip >/dev/null
"$VENV_PY" -m pip install -r requirements.txt >/dev/null
"$VENV_PY" -m pip install -e . >/dev/null

echo "Starting Shelly Energy Analyzer (Flask web server)..."
"$VENV_PY" -m shelly_analyzer "$@"
