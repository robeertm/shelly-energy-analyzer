# Shelly Energy Analyzer (Refactored)

A desktop app that downloads CSV data from Shelly EM / 3EM devices, calculates energy (kWh), shows live power, and can send Telegram summaries and alerts.

## Features
- Live plots: Day/Night toggle (All/Day/Night) to focus on daytime vs nighttime samples.

- Download + merge Shelly EM/3EM CSV exports into local time series
- kWh / W / V / A plots and reports
- Live dashboard (background polling)
- Telegram: daily/monthly summaries + alert triggers

## Quick start

### macOS

1. Install Python **3.10+** (recommended: from python.org, includes Tkinter on macOS).
2. Unzip the project.
3. Double-click **`start.command`**.

The app creates a local virtualenv (`.venv`), installs dependencies, and starts the GUI.

> If `start.command` is not executable after unzipping: `chmod +x start.command`.

### Windows 10/11

1. Install Python **3.10+** from python.org (check **"Add python.exe to PATH"**).
2. Unzip the project.
3. Double-click **`start.bat`**.

First run creates `.venv` and installs dependencies.

### Linux

1. Install Python **3.10+**.
2. Make sure Tkinter is installed:
   - Debian/Ubuntu: `sudo apt-get install python3-tk`
   - Fedora: `sudo dnf install python3-tkinter`
3. Unzip the project.
4. Run:

```bash
chmod +x start.sh
./start.sh
```

## Configuration

**Do not commit your real `config.json`** (it may contain secrets like Telegram bot token/chat id).

1. Copy `config.example.json` to `config.json`
2. Edit devices and options.

Example (minimal):

```json
{
  "version": "4.1.0",
  "devices": [
    {"key": "shelly1", "name": "House", "host": "192.168.3.175", "em_id": 0},
    {"key": "shelly2", "name": "Server", "host": "192.168.3.189", "em_id": 0}
  ]
}
```

## Data storage

Runtime data is stored under:
- `data/<device_key>/` (raw CSV chunks, merged files)
- `meta.json` files track download progress

> `data/` is in `.gitignore` because it is user-specific.

## Running without start scripts

### macOS/Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
pip install -r requirements.txt
pip install -e .
python -m shelly_analyzer
```

### Windows (PowerShell)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
pip install -r requirements.txt
pip install -e .
python -m shelly_analyzer
```

## Publishing on GitHub

This repository is licensed under **MIT** (see `LICENSE`).

### What to commit

✅ Commit:
- Source code (`src/`)
- `requirements.txt`, `pyproject.toml`
- `start.command`, `start.bat`, `start.sh`
- `config.example.json`
- Documentation (`README.md`, `CHANGELOG.md`)

❌ Do not commit:
- `config.json` (secrets)
- `.venv/`, `data/`, logs, `__pycache__/`

### Automated GitHub Releases

A ready-to-use GitHub Actions workflow is included:

- Tag a version: `git tag v5.8.14.38 && git push origin v5.8.14.38`
- The workflow builds ZIP artifacts and publishes a GitHub Release.

Workflow file:
- `.github/workflows/release.yml`

Artifacts created:
- `shelly_energy_analyzer_<TAG>_windows.zip`
- `shelly_energy_analyzer_<TAG>_macos.zip`
- `shelly_energy_analyzer_<TAG>_linux.zip`

