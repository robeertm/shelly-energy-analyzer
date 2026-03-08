# Shelly Energy Analyzer

A cross-platform desktop application to analyze, visualize and export energy data from Shelly devices (1‑phase and 3‑phase) — including live dashboards, cost tracking, historical plots, CSV/PDF exports and Telegram alerts.

This repository is **GitHub-ready** (MIT license, clean structure, no secrets). Releases are published via **GitHub Releases** so the built‑in updater can find and download new versions.

## Key Features

- 📊 **Live Monitoring**
  - Real-time power, voltage and current
  - 1‑phase and 3‑phase devices (L1 / L2 / L3 + total)
  - Live cost display (kWh × configured price)
  - Phase balance indicator for 3-phase devices (detects imbalance)
  - Day / Night / Auto theme switching

- 💰 **Cost Dashboard**
  - Dedicated "Costs" tab with today / week / month / year overview
  - Monthly cost projection based on current usage
  - Previous month comparison (% change)
  - Per-device breakdown with cost share and bar chart

- 📈 **Historical Analysis**
  - Plots for W / V / A / kWh / VAR / cosφ
  - Per-device and per-phase views
  - CSV-based storage (offline-friendly)

- 📤 **Exports**
  - CSV export for further analysis
  - PDF energy reports (daily / monthly)
  - PDF invoices with optional company logo

- 🔔 **Telegram Notifications**
  - Configurable threshold alerts with optional plots
  - Daily & monthly summaries with kWh bar charts
  - Previous period comparison (vs. yesterday / last month)
  - Standby base load detection (projected yearly cost)

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
  - Web dashboard (accessible from any device on the network)

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
- On first start, choose **"Demo mode"** in the setup wizard
  **or**
- Set in `config.json`:
```json
{
  "demo": { "enabled": true }
}
```

Demo Mode will generate realistic live data (with jitter + load spikes) and CSV history for plots and exports.

## Languages

The UI supports:
- English
- German
- Spanish
- French (partial)

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
- If you are offline or GitHub is blocked by DNS/VPN/firewall, the app will show an "offline/timeout" message.
- **Download & install** is only enabled when a newer version than the current one is available.

To publish an update:
```bash
git add -A
git commit -m "Description of changes"
git push
git tag v5.9.2.51
git push origin v5.9.2.51
```

The GitHub Actions workflow will automatically build the release ZIP and publish it.

---

License: MIT (see `LICENSE`).
