#!/usr/bin/env bash
set -euo pipefail

# Ensure we run from this folder
cd "$(dirname "$0")"

choose_py() {
  local candidates=(python3 python)
  for py in "${candidates[@]}"; do
    if command -v "$py" >/dev/null 2>&1; then
      if "$py" -c "import tkinter" >/dev/null 2>&1; then
        echo "$py"
        return 0
      fi
    fi
  done
  return 1
}

PY=$(choose_py) || {
  echo "No Python with Tkinter found."
  echo "On Debian/Ubuntu you may need: sudo apt-get install python3-tk"
  echo "On Fedora: sudo dnf install python3-tkinter"
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

"$VENV_PY" -m shelly_analyzer
