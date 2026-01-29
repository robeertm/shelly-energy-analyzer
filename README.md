# Shelly Energy Analyzer

A cross-platform desktop application to analyze, visualize and export energy data from Shelly devices (1‑phase and 3‑phase) — including live dashboards, historical plots, CSV/PDF exports and Telegram alerts.

This repository is **GitHub-ready** (MIT license, clean structure, no secrets). Releases are intended to be published via **GitHub Releases** so the built‑in updater can find and download new versions.

## Key Features

- 📊 **Live Monitoring**
  - Real-time power, voltage and current
  - 1‑phase and 3‑phase devices (L1 / L2 / L3 + total)
  - Day / Night / Auto theme switching

- 📈 **Historical Analysis**
  - Plots for W / V / A / kWh / VAR / cosφ
  - Per-device and per-phase views
  - CSV-based storage (offline-friendly)

- 📤 **Exports**
  - CSV export for further analysis
  - PDF reports (daily / monthly)

- 🔔 **Notifications**
  - Telegram alerts
  - Daily & monthly summaries
  - Configurable thresholds

- 🧙 **Setup Wizard**
  - Automatic discovery (mDNS / IP scan)
  - Manual IP/host entry
  - Quiet first-run experience (no error spam)

- 🎭 **Demo Mode**
  - No Shelly devices required
  - Realistic demo data (live + history CSVs)
  - Great for testing and screenshots

- 🖥 **Cross‑Platform**
  - macOS / Windows / Linux
  - One-click start scripts

## Quick Start (End Users)

### macOS
- Double-click `start.command`
- If macOS blocks it once: in Terminal, run `chmod +x start.command` in the folder.

### Windows
- Double-click `start.bat`

### Linux
```bash
chmod +x start.sh
./start.sh
```

## Demo Mode

Demo Mode lets you test the full application **without any Shelly devices**.

### Enable Demo Mode
- On first start, choose **“Demo mode”** in the setup wizard
  **or**
- Set in `config.json`:
```json
{
  "demo": { "enabled": true }
}
```

Demo Mode will generate:
- realistic live data (with jitter + load spikes)
- CSV history for plots and exports

## Languages

The UI supports:
- English
- German
- Spanish

Change language in **Settings → Language**.

## Running From Source (Developers)

Requirements:
- Python 3.11+ (recommended)

Install and run:
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -U pip
pip install -r requirements.txt
python -m shelly_analyzer
```

## Updates (GitHub Releases)

The built-in updater checks the latest release on GitHub.

Notes:
- If you are offline or GitHub is blocked by DNS/VPN/firewall, the app will show an “offline/timeout” message.
- **Download & install** is only enabled when a newer version than the current one is available.

To publish an update:
1. Create a git tag (example: `v5.9.2.9`)
2. Push the tag
3. Create a GitHub Release for the tag and attach the release ZIP artifact

## Upload to GitHub (Command Cheat Sheet)

### 1) Create the repo locally and commit
```bash
git init
git add .
git commit -m "Initial commit"
```

### 2) Connect to GitHub and push
Create an empty repo on GitHub first, then:
```bash
git branch -M main
git remote add origin <YOUR_GITHUB_REPO_URL>
git push -u origin main
```

### 3) Tag a release (recommended)
```bash
git tag v5.9.2.9
git push origin v5.9.2.9
```

### 4) Typical workflow afterwards
```bash
git status
git add -A
git commit -m "Fix updater behavior"
git push
```

---

License: MIT (see `LICENSE`).

## GitHub upload (commands)

Run these commands **inside the extracted project folder** (where `README.md` and `src/` are):

```bash
pwd
ls
# Make sure you are in the correct folder (should show version 5.9.2.9 in folder name)

git status
git add .
git commit -m "Release v5.9.2.9"
git push

git tag v5.9.2.9
git push origin v5.9.2.9
```
