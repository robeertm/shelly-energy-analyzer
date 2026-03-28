# Shelly Energy Analyzer

A cross-platform desktop application to analyze, visualize and export energy data from Shelly devices (1-phase and 3-phase) — including live dashboards, cost tracking, historical plots, CSV/PDF exports, Telegram alerts, solar/PV monitoring, anomaly detection, NILM appliance detection, device scheduling, consumption forecasting, weather correlation, tenant billing and much more.

This repository is **GitHub-ready** (MIT license, clean structure, no secrets). Releases are published via **GitHub Releases** so the built-in updater can find and download new versions.

> **Current version: v12.1.2** — all major feature modules complete.

---

## Screenshots

### Desktop App (Live Tab with 3-Phase Details)
![Desktop Live](docs/screenshots/01_live_desktop.png)

### Web Dashboard (Mobile)

| Live | Live (expanded) | Costs |
|------|----------------|-------|
| ![Live](docs/screenshots/01_live.png) | ![Detail](docs/screenshots/01_live_detail.png) | ![Costs](docs/screenshots/02_costs.png) |

| Heatmap | CO2 | Forecast |
|---------|-----|----------|
| ![Heatmap](docs/screenshots/03_heatmap.png) | ![CO2](docs/screenshots/06_co2.png) | ![Forecast](docs/screenshots/08_forecast.png) |

| Energy Flow | Standby | Export |
|-------------|---------|--------|
| ![Energy Flow](docs/screenshots/10_energy_flow.png) | ![Standby](docs/screenshots/09_standby.png) | ![Export](docs/screenshots/11_export.png) |

| Solar | Compare | Anomalies |
|-------|---------|-----------|
| ![Solar](docs/screenshots/04_solar.png) | ![Compare](docs/screenshots/05_compare.png) | ![Anomalies](docs/screenshots/07_anomalies.png) |

---

## Key Features

### 📊 Live Monitoring
- Real-time power, voltage, current and **grid frequency (Hz)**
- **Neutral conductor current (I_N)** for 3-phase devices — computed via phasor vector sum with per-phase power-factor angles
- 1-phase and 3-phase devices (L1 / L2 / L3 + total)
- Live cost display (kWh x configured price)
- Phase balance indicator for 3-phase devices (detects imbalance)
- Interactive legend toggle on live plots (click L1/L2/L3/N to show/hide)
- Day / Night / Auto theme switching

### 💰 Cost Dashboard
- Dedicated "Costs" tab with today / week / month / year overview
- Monthly cost projection based on current usage
- Previous month comparison (% change)
- Per-device breakdown with cost share and bar chart
- **CO2 tracking with real grid data** — per-device CO2 footprint (Today / Week / Month / Year / Forecast in kg) using real hourly ENTSO-E grid intensity data; falls back to configurable static CO2 intensity (g/kWh) if ENTSO-E is not configured

### 💱 Time-of-Use (TOU) Tariffs
- Define multiple time-based electricity price zones (peak, off-peak, etc.)
- Automatic cost calculation using the correct rate per time window
- Configurable per day-of-week and hour range
- Seamless integration with cost dashboard and exports

### 📈 Historical Analysis & Consumption Forecast
- Plots for W / V / A / kWh / VAR / cos phi / Hz (grid frequency) / CO2 emissions
- CO2 emissions plot tab — hourly energy x real grid CO2 intensity (ENTSO-E), colour-coded bars by intensity level
- Per-device and per-phase views
- SQLite-based storage (fast range queries, WAL mode)
- **Consumption forecasting** — linear regression with weekday/hourly seasonality on historical daily data; trend analysis (rising/falling/stable in %/month); projected costs for next month and next year; confidence bands; weekday and hourly profile charts with color-coded patterns (red = above average, green = below average)

### 🗓 Heatmap Calendar
- Calendar-style heatmap showing daily energy consumption at a glance
- Hourly heatmap view — spot usage patterns by hour-of-day vs. day-of-week
- **Two unit modes**: kWh and g CO2 — CO2 mode uses real ENTSO-E hourly grid intensity data with distinct colour scheme
- Colour intensity scales automatically to the selected period
- Available in both the desktop app and the web dashboard

### 🔍 Comparison Mode
- Compare two arbitrary date ranges side-by-side (e.g. this week vs. last week)
- **Quick-compare buttons** — one click to compare Month, Quarter, Half-Year or Year against the previous period
- Overlaid line charts for any metric (kWh, W, cost, ...)
- Weekly granularity — aggregate daily data into ISO calendar weeks
- Percentage delta indicators for quick at-a-glance diff

### ⚠️ Automatic Anomaly Detection
- Rolling mean +/- N x sigma (configurable sigma multiplier) algorithm
- Detects consumption spikes and unexpected dips in real time and in history
- Anomaly log with timestamp, device, metric and deviation magnitude
- Automatic periodic detection with configurable interval
- Notification via Telegram, Webhook and E-mail

### ☀️ PV / Solar Dashboard & Amortization
- Feed-in energy tracking (kWh exported to grid)
- Self-consumption calculation (solar energy used locally)
- **Autarky rate** (% of demand covered by own solar production)
- Time-series chart of production, consumption and net grid draw
- **CO2 savings** — displays avoided CO2 (kg), tree-day and car-km equivalents using real ENTSO-E grid intensity
- **PV amortization calculator** — investment payback analysis with configurable investment amount, installation year, and panel degradation rate; shows annual savings, payback period, ROI after 20 years, total CO2 saved; amortization timeline chart with cumulative savings vs. investment crossover point
- System config — configurable installed capacity (kWp), battery storage (kWh), embodied CO2 per kWp

### 🔌 Standby Killer Report
- Identifies devices with constant base load (standby consumers)
- Per-device analysis: base load (W), annual standby kWh, annual standby cost, share of total
- Risk classification (high / medium / low) based on annual standby cost
- 24h load profile per device with standby threshold line
- Cost comparison bar chart sorted by savings potential
- Available in both desktop app and web dashboard

### 🌡️ Weather Correlation
- Integration with **OpenWeatherMap API** — current weather display (temperature, humidity, wind, clouds)
- **Temperature vs. consumption correlation** — Pearson correlation coefficient (r), scatter plot with trend line
- Heating Degree Days (HDD) and Cooling Degree Days (CDD) analysis
- kWh per HDD/CDD for heating/cooling efficiency assessment
- Automatic interpretation (heating correlation / cooling correlation / no dependency)
- Weather data persisted hourly in SQLite for historical correlation
- Graceful handling when paired data is still being collected

### ⚡ Energy Flow Diagram
- Visual energy flow: Grid -> House -> Devices and PV -> Self-consumption / Feed-in
- Per-device consumption breakdown with percentage shares
- Period selector (Today / Week / Month / Year)
- Available in both desktop app and web dashboard

### 📡 Home Assistant MQTT Integration
- **MQTT publisher** for any MQTT broker (Mosquitto, HiveMQ, etc.)
- **Home Assistant auto-discovery** — creates sensor entities automatically for each Shelly device
- Publishes: power (W), voltage (V), current (A), energy (kWh), frequency (Hz), power factor, CO2 rate (g/h)
- Per-phase metrics (L1/L2/L3) for 3-phase devices
- Configurable broker, port, username/password, TLS, topic prefix, publish interval
- Graceful fallback when `paho-mqtt` is not installed

### 🏠 Tenant Utility Billing (Nebenkostenabrechnung)
- **Multi-tenant support** — assign Shelly devices to tenants via checkbox UI in settings
- Per-tenant annual utility bill with line items, subtotal, VAT, and gross total
- **Common area electricity** — split among all tenants by person count
- Pro-rated base fee allocation
- **Move-in date** — billing calculation starts from the configured move-in date
- Device exclusivity — devices assigned to one tenant are grayed out for others
- **PDF invoice export** per tenant
- Cost comparison bar chart and kWh share pie chart
- Donut chart for single-tenant view

### 🔌 ML-Enhanced NILM Appliance Detection
- Non-Intrusive Load Monitoring — identifies which appliances are running from live wattage alone
- ~25 built-in device profiles (fridge, washing machine, dishwasher, EV charger, heat pump, etc.)
- **ML learning engine** — k-means clustering on power transitions (step changes) to discover recurring appliance patterns automatically
- Learned clusters matched against built-in database with confidence scoring
- Cluster data persisted across sessions
- Top-3 matches shown in desktop app and web dashboard

### 📤 Exports & E-mail Reports

#### CSV / PDF / Excel
- CSV export for further analysis
- **Excel export** — `.xlsx` with one sheet per device, optionally filtered by date range
- **Rich daily PDF report**: 6 KPI tiles, device breakdown, stacked 24h chart, per-device mini-charts
- **Rich monthly PDF report**: KPI tiles, comparison to previous month, weekday vs. weekend analysis, Top-5 consumer ranking

#### Invoices (PDF)
- Professional A4 invoice with sender/recipient from `BillingConfig`
- Invoice number format `{Prefix}-{YYYY}-{MM}-001` (configurable prefix)
- Optional company logo, coloured table header, alternating row shading
- **Per-device invoices**: each device gets its own individual invoice PDF

#### Scheduled E-mail
- Automated daily and monthly e-mail reports via SMTP with rich PDF attachments
- Monthly e-mail invoice attachment — optional per device + combined
- Send-now buttons for immediate on-demand delivery

### 🔔 Notifications
- **Telegram bot** — threshold alerts (W, V, A, VAR, cos phi, Hz) with optional plots; daily & monthly summaries with kWh bar charts and CO2 charts
- **Webhook notifications** — HTTP POST to any endpoint (Home Assistant, n8n, Zapier, ...) on threshold breach or scheduled events; configurable JSON payload template

### 📦 Device Grouping
- Logical groups across multiple physical Shelly devices
- Aggregated energy, power and cost view per group

### ⏰ Device Scheduling
- Create on/off schedules for Shelly Gen2 devices via the official RPC API
- Visual schedule editor (time slots, days-of-week)
- Manage and delete existing schedules directly from the app

### 🔄 Data Sync
- Pull historical data from Shelly devices into the local SQLite database
- Sync progress bar with real-time status (e.g. "Device 2/3 - Chunk 5/12")
- Retention policy: raw data compressed to monthly aggregates after 2 years

### 🧙 Setup Wizard
- Automatic discovery (mDNS / IP scan)
- Manual IP/host entry
- Quiet first-run experience (no error spam)

### 🎭 Demo Mode
- No Shelly devices required
- Realistic demo data (live + history CSVs)
- Great for testing and screenshots

### 🌐 Web Dashboard (Mobile-Friendly SPA)
- Full single-page app (SPA) accessible from any device on the local network
- **11 tabs** matching the desktop application:
  - **Live** — real-time device cards with colour-coded power, sparkline charts, collapsible detail rows, NILM appliance chips, freeze button, time-scale selector
  - **Costs** — per-device cost overview with ENTSO-E CO2 tracking
  - **Heatmap** — interactive yearly calendar heatmap and weekday x hour heatmap; horizontally scrollable on mobile with readable 3-char month labels
  - **Solar** — PV dashboard with feed-in, self-consumption, autarky %, CO2 savings, inline settings
  - **Comparison** — period-over-period comparison with device selectors, grouped bar chart, delta display
  - **CO2** — live grid intensity, 24h chart, fuel mix, per-device CO2 rates, summary cards
  - **Anomalies** — detected events with type, timestamp, sigma, description
  - **Forecast** — consumption forecast with history + prediction bar chart, weekday and hourly profile charts (Canvas-based, matching CO2 tab style)
  - **Standby** — standby cost summary, per-device cards with risk badges, cost bar chart and 24h load profile (Canvas-based)
  - **Energy Flow** — summary cards + per-device consumption breakdown chart with period selector
  - **Export** — PDF summaries, reports, invoices, Excel, ZIP bundles; inline preview
- **Dark / Light mode** toggle with auto-detection and localStorage persistence
- **Full i18n** — renders in the same language as the desktop app (all 9 supported languages)
- Device order & visibility settings via gear icon modal
- Gzip-compressed HTML (~75% smaller payload) for fast mobile page loads
- **Single-row horizontal scrolling nav bar** — all tabs accessible without wrapping
- Mobile-first design: bottom navigation, min 44px touch targets, 360px to 1920px viewport

### 🖥 Cross-Platform
- macOS / Windows / Linux
- One-click start scripts

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

The UI supports 9 languages (change in **Settings -> Language**):

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
- **Version history** — the updater shows the last 10 releases; you can install any of them, including older versions (downgrade).

To publish an update:
```bash
git add -A
git commit -m "Description of changes"
git push
git tag v12.1.2
git push origin v12.1.2
```

The GitHub Actions workflow will automatically build the release ZIP and publish it.

---

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for the full version history.

---

License: MIT (see `LICENSE`).
