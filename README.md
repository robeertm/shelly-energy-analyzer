# Shelly Energy Analyzer

A cross-platform desktop application to analyze, visualize and export energy data from Shelly devices (1‑phase and 3‑phase) — including live dashboards, cost tracking, historical plots, CSV/PDF exports, Telegram alerts, solar/PV monitoring, anomaly detection, device scheduling and much more.

This repository is **GitHub-ready** (MIT license, clean structure, no secrets). Releases are published via **GitHub Releases** so the built‑in updater can find and download new versions.

> **Current version: v8.0.0** — all 10 major feature modules complete.

---

## Key Features

### 📊 Live Monitoring
- Real-time power, voltage, current and **grid frequency (Hz)**
- **Neutral conductor current (I_N)** for 3-phase devices — computed via phasor vector sum with per-phase power-factor angles
- 1‑phase and 3‑phase devices (L1 / L2 / L3 + total)
- Live cost display (kWh × configured price)
- Phase balance indicator for 3-phase devices (detects imbalance)
- Interactive legend toggle on live plots (click L1/L2/L3/N to show/hide)
- Day / Night / Auto theme switching

### 💰 Cost Dashboard
- Dedicated "Costs" tab with today / week / month / year overview
- Monthly cost projection based on current usage
- Previous month comparison (% change)
- Per-device breakdown with cost share and bar chart

### 💱 Time-of-Use (TOU) Tariffs
- Define multiple time-based electricity price zones (peak, off-peak, etc.)
- Automatic cost calculation using the correct rate per time window
- Configurable per day-of-week and hour range
- Seamless integration with cost dashboard and exports

### 📈 Historical Analysis
- Plots for W / V / A / kWh / VAR / cosφ / **Hz (grid frequency)**
- Per-device and per-phase views
- SQLite-based storage (fast range queries, WAL mode)

### 🗓 Heatmap Calendar
- Calendar-style heatmap showing daily energy consumption at a glance
- Hourly heatmap view — spot usage patterns by hour-of-day vs. day-of-week
- Colour intensity scales automatically to the selected period

### 🔍 Comparison Mode
- Compare two arbitrary date ranges side-by-side (e.g. this week vs. last week)
- Overlaid line charts for any metric (kWh, W, cost, …)
- Percentage delta indicators for quick at-a-glance diff

### ⚠️ Automatic Anomaly Detection
- Rolling mean ± N × σ (configurable sigma multiplier) algorithm
- Detects consumption spikes and unexpected dips in real time and in history
- Highlighted anomaly markers on time-series plots
- Anomaly log with timestamp, device, metric and deviation magnitude

### ☀️ PV / Solar Dashboard
- Feed-in energy tracking (kWh exported to grid)
- Self-consumption calculation (solar energy used locally)
- **Autarky rate** (% of demand covered by own solar production)
- Time-series chart of production, consumption and net grid draw
- Works with any Shelly EM/PM device installed at the grid connection point

### 📤 Exports
- CSV export for further analysis
- PDF energy reports (daily / monthly)
- PDF invoices with optional company logo
- **E-mail reports** with PDF attachment — send scheduled or on-demand reports via SMTP

### 🔔 Notifications
- **Telegram bot** — threshold alerts (W, V, A, A_N, VAR, cos φ, Hz) with optional plots; daily & monthly summaries with kWh bar charts; previous period comparison; standby base-load detection
- **Webhook notifications** — HTTP POST to any endpoint (Home Assistant, n8n, Zapier, …) on threshold breach or scheduled events; fully configurable JSON payload template

### 📦 Device Grouping
- Logical groups across multiple physical Shelly devices
- Aggregated energy, power and cost view per group
- Independent group dashboard with combined time-series plots

### ⏰ Device Scheduling
- Create on/off schedules for Shelly Gen2 devices via the official RPC API
- Visual schedule editor (time slots, days-of-week)
- Manage and delete existing schedules directly from the app

### 🧙 Setup Wizard
- Automatic discovery (mDNS / IP scan)
- Manual IP/host entry
- Quiet first-run experience (no error spam)

### 🎭 Demo Mode
- No Shelly devices required
- Realistic demo data (live + history CSVs)
- Great for testing and screenshots

### 🖥 Cross‑Platform
- macOS / Windows / Linux
- One-click start scripts
- Web dashboard (accessible from any device on the network) with live charts, neutral current (dashed N line), phase balance, cost overview

---

## Quick Start (End Users)

### macOS
```
Double-click start.command
```
If macOS blocks it: run `chmod +x start.command` in Terminal first.

### Windows
```
Double-click start.bat
```

### Linux
```bash
chmod +x start.sh
./start.sh
```

---

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

Demo Mode generates realistic live data (with jitter + load spikes) and CSV history for plots and exports.

---

## Languages

The UI supports 9 languages (change in **Settings → Language**):

| Code | Language   |
|------|------------|
| `de` | German     |
| `en` | English    |
| `es` | Spanish    |
| `fr` | French     |
| `pt` | Portuguese |
| `it` | Italian    |
| `pl` | Polish     |
| `cs` | Czech      |
| `ru` | Russian    |

---

## Running From Source (Developers)

Requirements: Python 3.10+ (3.11+ recommended)

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -U pip
pip install -r requirements.txt
python -m shelly_analyzer
```

---

## Updates (GitHub Releases)

The built-in updater checks the latest release on GitHub automatically.

- If you are offline or GitHub is blocked by DNS/VPN/firewall, the app will show an "offline/timeout" message.
- **Download & install** is only enabled when a newer version than the current one is available.

To publish an update:
```bash
git add -A
git commit -m "Description of changes"
git push
git tag v8.0.0
git push origin v8.0.0
```

The GitHub Actions workflow will automatically build the release ZIP and publish it.

---

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for the full version history.

---

License: MIT (see `LICENSE`).
