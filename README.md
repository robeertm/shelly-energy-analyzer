# Shelly Energy Analyzer

A cross-platform desktop application to analyze, visualize and export energy data from Shelly devices (1‑phase and 3‑phase) — including live dashboards, historical plots, CSV/PDF exports and Telegram alerts.

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

## Quick Start

### macOS
- Double-click `start.command`
- If macOS blocks it: run `chmod +x start.command` in the folder once.

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
Demo Mode uses the same translation system as real devices.
