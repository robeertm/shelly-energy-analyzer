#!/bin/zsh
set -e
# Ensure we run from this folder (even when launched via Finder)
cd "$(dirname "$0")"

# Prefer python.org Python (Frameworks) and ensure Tkinter works.
choose_py () {
  local CANDIDATES=(
    "/Library/Frameworks/Python.framework/Versions/3.14/bin/python3"
    "/Library/Frameworks/Python.framework/Versions/3.13/bin/python3"
    "/Library/Frameworks/Python.framework/Versions/3.12/bin/python3"
    "python3"
    "python"
  )
  for py in "${CANDIDATES[@]}"; do
    if [[ -x "$py" ]] || command -v "$py" >/dev/null 2>&1; then
      "$py" -c "import tkinter" >/dev/null 2>&1 && { echo "$py"; return 0; }
    fi
  done
  return 1
}

PY=$(choose_py) || {
  echo "No Python with Tkinter found."
  echo "Please install Python from python.org (includes Tkinter on macOS) or use a Tk-enabled Python."
  exit 1
}

# Create venv if missing
if [[ ! -d ".venv" ]]; then
  "$PY" -m venv .venv
fi

source .venv/bin/activate

# Always use venv python from here on
VENV_PY="$(pwd)/.venv/bin/python"

"$VENV_PY" -m pip install -U pip >/dev/null
"$VENV_PY" -m pip install -r requirements.txt >/dev/null
"$VENV_PY" -m pip install -e . >/dev/null

"$VENV_PY" -m shelly_analyzer
