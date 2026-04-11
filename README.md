<div align="center">

# ⚡ Shelly Energy Analyzer

**Self-hosted energy monitoring, cost tracking and smart automation for Shelly EM / 3EM devices.**
No cloud. No subscription. No data lock-in.

[![License: Proprietary – Free to use](https://img.shields.io/badge/license-Proprietary%20%7C%20Free%20to%20use-blue)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue?logo=python&logoColor=white)](https://www.python.org/)
[![Flask](https://img.shields.io/badge/flask-3.0%2B-black?logo=flask&logoColor=white)](https://flask.palletsprojects.com/)
[![Release](https://img.shields.io/github/v/release/robeertm/shelly-energy-analyzer?logo=github)](https://github.com/robeertm/shelly-energy-analyzer/releases/latest)
[![Last commit](https://img.shields.io/github/last-commit/robeertm/shelly-energy-analyzer?logo=git&logoColor=white)](https://github.com/robeertm/shelly-energy-analyzer/commits/main)
[![i18n](https://img.shields.io/badge/i18n-9%20languages-brightgreen)](#languages)
[![Platforms](https://img.shields.io/badge/platform-macOS%20%7C%20Linux%20%7C%20Windows-lightgrey)](#quick-start-end-users)

![Hero](docs/screenshots/desktop_01_live.png)

</div>

---

## 🎯 Why Shelly Energy Analyzer?

You bought Shelly EM / 3EM meters. You want to know **how much you spend**, **when you spend it**, and **what to change** — without your data leaving your home and without a monthly bill.

Commercial energy dashboards lock you into subscriptions, truncate history after a few months, ignore your actual tariff, and push you toward proprietary clouds. **Shelly Energy Analyzer** is a single **self-hosted Flask web app** that reads your Shellys directly over LAN and turns their data into 23 real dashboards with real-time cost, real-time CO₂ and real automation.

### What it does that others don't

- 💰 **Knows your exact tariff** — fixed, time-of-use, dynamic spot market (EU via Energy-Charts / aWATTar, **USA via EIA, Australia via AEMO**), multi-step schedules with future price changes — and shows live € cost per device, per second
- 🌱 **Real grid CO₂ intensity** — EU via ENTSO-E (with cross-border flow correction), **rest of the world via Electricity Maps (92 zones across North & South America, Asia, Oceania, Africa and the Middle East)** — never a flat 380 g/kWh average
- 🔮 **Trend + weather CO₂ forecast (6 h ahead)** — per-hour-of-day median of the last 14 days × Open-Meteo wind / sun / temp / rain × per-zone generation-mix profile (150+ zones). Rendered as a dashed continuation of the main CO₂ chart, plus a 6-cell strip with weather icons
- 🧠 **Built-in NILM** — k-means clustering on power transitions automatically identifies appliances from the total-power trace. ~25 built-in device profiles plus learned patterns
- ☀️ **PV surplus automation** — state machine that switches boilers / wallboxes on when solar excess is available, off when it drops, with priority-ordered consumer list and debounce
- ⏰ **Smart scheduling** — finds the cheapest 1–12 h time block tomorrow from day-ahead spot prices and can push the schedule to a Shelly Gen2 relay automatically
- 🔐 **Password-protected Shellys supported** — Gen 1 (Basic auth), Gen 2 / 3 / 4 Plus / Pro (Digest auth), all on Windows / macOS / Linux. Per-host credentials with auto-detection of the correct scheme; setup wizard and device card prompt for the password whenever a device responds 401
- 🏠 **Native Home Assistant integration** via MQTT auto-discovery — sensors appear automatically, no extra YAML
- 📱 **Real iOS home-screen widget** via Scriptable (3 sizes) — live power, today's cost, spot price, CO₂ intensity, tap-to-open dashboard
- 🌐 **Full REST API v1** + **InfluxDB line-protocol push** + **Prometheus `/metrics`** for your own stack
- 🔒 **100 % self-hosted** — your energy data never leaves your LAN
- 🆓 **Zero subscription** — no cloud, no accounts, no analytics
- ⬆ **Self-updating** — background thread polls GitHub every hour for new releases; the Live tab shows a one-click banner to install or roll back to any of the last 10 versions, entirely from the browser

### Who it's for

- 🏠 **Home owners** with Shelly 1PM / Plus 1PM / EM / 3EM at the grid connection or per circuit
- ☀️ **PV / solar prosumers** tracking self-consumption, autarky, feed-in and investment amortisation
- 🏢 **Landlords** needing per-tenant sub-metering and Nebenkostenabrechnung PDFs
- ⚡ **Dynamic-tariff customers worldwide** — Tibber, aWATTar, Ostrom, 1Komma5°, E.ON Spot in Europe; Griddy/Rhythm/ERCOT Retailers in Texas; Amber Electric in Australia; and anyone comparing their fixed contract against live wholesale prices
- 🔧 **Home Assistant / Node-RED tinkerers** pulling metrics into MQTT, InfluxDB or Prometheus
- 📊 **Data nerds** who want raw SQLite access, CSV/PDF/Excel exports and a REST API

---

## ⚡ Quickstart

```bash
git clone https://github.com/robeertm/shelly-energy-analyzer.git
cd shelly-energy-analyzer

# macOS
./start.command

# Linux
chmod +x start.sh && ./start.sh

# Windows
start.bat
```

Open <https://localhost:8765> and follow the 5-step setup wizard — it will discover your Shellys via mDNS, ask for your tariff, and you're live.

> **No Shellys yet?** Enable **Demo Mode** in Settings → Advanced to explore the full UI with realistic generated data. Great for seeing what the tool can do before buying hardware.

---

## Screenshots

All desktop shots are captured at native **4K (3840×2160)**, all mobile shots at **iPhone 16 Pro Max** resolution (1290×2796).

### Desktop (4K)

| Live | Costs | Heatmap |
|------|-------|---------|
| ![Live](docs/screenshots/desktop_01_live.png) | ![Costs](docs/screenshots/desktop_02_costs.png) | ![Heatmap](docs/screenshots/desktop_03_heatmap.png) |

| Weather | CO₂ | Anomalies |
|---------|-----|-----------|
| ![Weather](docs/screenshots/desktop_05_weather.png) | ![CO2](docs/screenshots/desktop_06_co2.png) | ![Anomalies](docs/screenshots/desktop_07_anomalies.png) |

| Forecast | Standby | Energy Flow |
|----------|---------|-------------|
| ![Forecast](docs/screenshots/desktop_08_forecast.png) | ![Standby](docs/screenshots/desktop_09_standby.png) | ![Energy Flow](docs/screenshots/desktop_10_energy_flow.png) |

| Goals | NILM |
|-------|------|
| ![Goals](docs/screenshots/desktop_12_goals.png) | ![NILM](docs/screenshots/desktop_14_nilm.png) |

#### Dark theme

| Live | Costs | Heatmap | CO₂ |
|------|-------|---------|-----|
| ![Live Dark](docs/screenshots/desktop_01_live_dark.png) | ![Costs Dark](docs/screenshots/desktop_02_costs_dark.png) | ![Heatmap Dark](docs/screenshots/desktop_03_heatmap_dark.png) | ![CO2 Dark](docs/screenshots/desktop_06_co2_dark.png) |

### Mobile (iPhone 16 Pro Max)

| Live | Costs | Heatmap |
|------|-------|---------|
| ![Live](docs/screenshots/mobile_01_live.png) | ![Costs](docs/screenshots/mobile_02_costs.png) | ![Heatmap](docs/screenshots/mobile_03_heatmap.png) |

| Weather | CO₂ | Anomalies |
|---------|-----|-----------|
| ![Weather](docs/screenshots/mobile_05_weather.png) | ![CO2](docs/screenshots/mobile_06_co2.png) | ![Anomalies](docs/screenshots/mobile_07_anomalies.png) |

| Forecast | Standby | Energy Flow |
|----------|---------|-------------|
| ![Forecast](docs/screenshots/mobile_08_forecast.png) | ![Standby](docs/screenshots/mobile_09_standby.png) | ![Energy Flow](docs/screenshots/mobile_10_energy_flow.png) |

| Goals | NILM |
|-------|------|
| ![Goals](docs/screenshots/mobile_12_goals.png) | ![NILM](docs/screenshots/mobile_14_nilm.png) |

#### Dark theme

| Live | Costs | Heatmap | CO₂ |
|------|-------|---------|-----|
| ![Live Dark](docs/screenshots/mobile_01_live_dark.png) | ![Costs Dark](docs/screenshots/mobile_02_costs_dark.png) | ![Heatmap Dark](docs/screenshots/mobile_03_heatmap_dark.png) | ![CO2 Dark](docs/screenshots/mobile_06_co2_dark.png) |

### iOS Widget (Scriptable)

| Small | Medium | Large |
|-------|--------|-------|
| ![Small](docs/screenshots/widget_small.png) | ![Medium](docs/screenshots/widget_medium.png) | ![Large](docs/screenshots/widget_large.png) |

---

## Key Features

### 📊 Live Monitoring
- Real-time power, voltage, current and **grid frequency (Hz)**
- **Neutral conductor current (I_N)** for 3-phase devices — computed via phasor vector sum with per-phase power-factor angles
- 1-phase and 3-phase devices (L1 / L2 / L3 + total)
- Live cost display (kWh x configured price)
- Phase balance indicator for 3-phase devices (detects imbalance)
- Interactive legend toggle on live plots (click L1/L2/L3/N to show/hide)
- Day / Night / Auto theme switching — all tabs and charts respect the selected theme
- **Tariff schedule** — define future price changes with start dates; the app automatically uses the correct price for any date range

### 💰 Cost Dashboard
- Dedicated "Costs" tab with today / week / month / year overview
- Monthly cost projection based on current usage
- Previous month comparison (% change)
- Per-device breakdown with cost share and bar chart
- **CO2 tracking with real grid data** — per-device CO2 footprint (Today / Week / Month / Year / Forecast in kg) using real hourly grid intensity. **EU** via ENTSO-E (EIC bidding zones + cross-border flow correction), **rest of the world** via Electricity Maps (92 global zones — USA CAISO/ERCOT/ISO-NE/NYISO/PJM/MISO, all Canadian provinces, Mexico, Brazil regions, Argentina, Chile, Japan by region, South Korea, Taiwan, China, India, Indonesia, Australia NEM, NZ, South Africa, Israel, Turkey, UAE, Saudi Arabia, …). Falls back to a configurable static intensity (g/kWh) if neither provider is configured.
- **Dynamic spot price comparison** — shows what each period would cost with a dynamic tariff (EPEX Spot + configurable markup + VAT) alongside your fixed tariff; orange-highlighted delta per card
- **24h spot market price chart** — rolling bar chart with colour-coded bars (green = cheap, red = expensive) and fixed-price reference line; shown in both desktop and web dashboard

### ⚡ Dynamic Spot Market Prices — Worldwide
- **Automatic price import** from free public APIs — provider is auto-selected based on the chosen bidding zone:
  - 🇪🇺 **Europe** — Energy-Charts (Fraunhofer ISE, 15-min resolution from Oct 2025) and aWATTar (hourly, history from 2015). 45 bidding zones: DE-LU, AT, CH, BE, BG, CZ, DK1/DK2, EE, ES, FI, FR, GB, GR, HR, HU, IE, IT (7 regions), LT, LV, ME, MK, NL, NO1–NO5, PL, PT, RO, RS, SE1–SE4, SI, SK. **No API key needed.**
  - 🇺🇸 **USA** — [EIA open data](https://www.eia.gov/opendata/) wholesale daily LMP per NERC region: `US-CAL`, `US-CAR`, `US-CENT`, `US-FLA`, `US-MIDA` (PJM), `US-MIDW` (MISO), `US-NE` (ISO-NE), `US-NW` (BPA), `US-NY` (NYISO), `US-SE`, `US-SW`, `US-TEN`, `US-TEX` (ERCOT). Free API key required (register at eia.gov/opendata/register.php).
  - 🇦🇺 **Australia** — AEMO NEM dispatch feed: `AU-NSW`, `AU-QLD`, `AU-SA`, `AU-TAS`, `AU-VIC`. **No API key needed.**
- **Automatic currency conversion** — USD and AUD prices are converted to EUR/MWh via daily ECB rates so all cost math and dashboards work unchanged regardless of which region you're in.
- Background service backfills from oldest measurement timestamp, auto-dispatches by zone prefix — you pick a zone, the app picks the right provider.
- Configurable markup (default 16 ct/kWh net) covering grid fees, taxes, and supplier margin
- VAT toggle — apply your configured VAT rate on top of spot price + markup
- **Plots sub-tab "Dyn. Preis"** — grouped bar chart comparing fixed vs. dynamic tariff costs per hour/day/week/month
- **Compare tab "vs. Dynamic Tariff"** — one-click toggle to compare your fixed tariff against spot prices for any period

### 💱 Time-of-Use (TOU) Tariffs
- Define multiple time-based electricity price zones (peak, off-peak, etc.)
- Automatic cost calculation using the correct rate per time window
- Configurable per day-of-week and hour range
- Seamless integration with cost dashboard and exports

### 📈 Historical Analysis & Consumption Forecast
- Plots for W / V / A / kWh / VAR / cos phi / Hz (grid frequency) / CO2 emissions / **dynamic prices**
- CO2 emissions plot tab — hourly energy x real grid CO2 intensity (ENTSO-E), colour-coded bars by intensity level
- **Dynamic price plot tab** — grouped bar chart comparing fixed tariff cost vs. spot market cost per period, with totals and delta display
- Per-device and per-phase views
- SQLite-based storage (fast range queries, WAL mode)
- **Consumption forecasting** — linear regression with weekday/hourly seasonality on historical daily data; trend analysis (rising/falling/stable in %/month); projected costs for next month and next year; confidence bands; weekday and hourly profile charts with color-coded patterns (red = above average, green = below average)

### 🗓 Heatmap Calendar
- Calendar-style heatmap showing daily energy consumption at a glance
- Hourly heatmap view — spot usage patterns by hour-of-day vs. day-of-week
- **Two unit modes**: kWh and g CO2 — CO2 mode uses real ENTSO-E hourly grid intensity data with distinct colour scheme
- Colour intensity scales automatically to the selected period

### 🔍 Comparison Mode
- Compare two arbitrary date ranges side-by-side (e.g. this week vs. last week)
- **Quick-compare buttons** — one click to compare Month, Quarter, Half-Year or Year against the previous period
- **"vs. Dynamic Tariff" toggle** — compare your fixed tariff costs against spot market prices for any period
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
- **Dedicated NILM statistics tab** — top-10 pattern cards with sparkline plots, hourly activity heatmap, category donut chart, per-device breakdown, recent transitions timeline, appliance signature database reference

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
- **Telegram bot** — threshold alerts (W, V, A, VAR, cos phi, Hz) with optional plots; daily & monthly summaries with kWh bar charts, CO2 charts, and **dynamic spot price comparison** (total spot cost, average price, per-device delta vs. fixed tariff)
- **E-mail reports** — automated daily and monthly PDF reports now include spot price KPIs (total spot cost, average ct/kWh, current price, fixed tariff comparison)
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

### 🧙 First-Run Setup Wizard (Web)
- Browser-based 5-step wizard at `/setup` (auto-redirected on first launch when no devices are configured)
- Automatic discovery (mDNS)
- Manual IP/host entry
- Pricing / base fee / VAT setup
- Optional spot-price bidding zone selection

### 🎭 Demo Mode
- No Shelly devices required
- Realistic demo data (live + history CSVs)
- Great for testing and screenshots

### 🌐 Web Dashboard (Mobile-Friendly SPA)
- Full single-page app (SPA) accessible from any device on the local network — the **sole UI** of the application
- **23 tabs**:
  - **Live** — real-time device cards with colour-coded power, sparkline charts, collapsible detail rows, NILM appliance chips, freeze button, time-scale selector
  - **Plots** — interactive historical charts (W/V/A/VAR/cos φ) with phase selection, time ranges, and kWh totals (Plotly.js)
  - **Costs** — per-device cost overview with ENTSO-E CO2 tracking, dynamic spot price comparison, and 24h spot market price chart; current spot price prominently displayed with color-coded delta
  - **Heatmap** — interactive yearly calendar heatmap and weekday x hour heatmap; horizontally scrollable on mobile with readable 3-char month labels
  - **Solar** — PV dashboard with feed-in, self-consumption, autarky %, CO2 savings, inline settings
  - **Weather** — weather correlation analysis: consumption vs. temperature timeline
  - **Comparison** — period-over-period comparison with device selectors, grouped bar chart, delta display
  - **CO2** — live grid intensity, 24h chart, fuel mix, per-device CO2 rates, summary cards
  - **Anomalies** — detected events with type, timestamp, sigma, description
  - **Forecast** — consumption forecast with history + prediction bar chart, weekday and hourly profile charts (Canvas-based, matching CO2 tab style)
  - **Standby** — standby cost summary, per-device cards with risk badges, cost bar chart and 24h load profile (Canvas-based)
  - **Energy Flow** — summary cards + per-device consumption breakdown chart with period selector
  - **EV Chargers** — nearby EV charging stations via OpenChargeMap API with radius, power, and plug filters
  - **NILM** — ML pattern statistics: top-10 patterns with sparkline plots, hourly activity heatmap, category donut, per-device breakdown, transitions timeline, appliance signature database
  - **Export** — PDF summaries, reports, invoices, Excel, ZIP bundles; inline preview
  - **Schedule** — smart time-based scheduling for Shelly switches
  - **EV Log** — electric vehicle charging session history
  - **Tariff** — electricity tariff comparison
  - **Battery** — home battery / storage simulation
  - **Advisor** — AI energy advisor with personalised tips
  - **Goals** — energy saving goals with progress tracking
  - **Tenants** — multi-tenant sub-metering and utility cost allocation
  - **Sync** — live data synchronisation log with status panel
- **Dark / Light mode** toggle with auto-detection and localStorage persistence
- **Full i18n** — all 9 supported languages
- Device order & visibility settings via gear icon modal
- Gzip-compressed HTML (~75% smaller payload) for fast mobile page loads
- **Single-row horizontal scrolling nav bar** — all tabs accessible without wrapping
- Mobile-first design: bottom navigation, min 44px touch targets, 360px to 1920px viewport
- **SSL/HTTPS support** — three modes: Auto (self-signed), Custom (Let's Encrypt / own certs), Off (plain HTTP)

### 📱 iOS Widget (Scriptable)
- Real iOS home screen widget via the **Scriptable** app for live energy data at a glance
- **Three widget sizes**:
  - **Small** — current power (W), today's consumption + cost, spot price with delta, CO₂ intensity
  - **Medium** — all of small + month stats, full-width spot price chart + CO₂ intensity chart
  - **Large** — full detail with spot chart, CO₂ chart, metrics grid (today/month/projection), spot cost comparison, per-device breakdown
- **CO₂ intensity chart** — color-coded bars (green/yellow/orange/red) based on ENTSO-E thresholds, with green and dirty threshold reference lines
- **Spot price chart** — 24h bar chart with color-coded bars relative to fixed tariff and fixed-price reference line
- **Tap-to-refresh** — tapping the widget opens a live detail view in Scriptable with all metrics, per-device data, and a "Dashboard öffnen" button to open the full web dashboard in Safari
- **Auto-refresh** — widget refreshes every 5 minutes via `refreshAfterDate`
- **Device filter** — configurable which Shellys appear in the widget (`widget_devices` in Settings)
- **Auto-domain** — server domain auto-detected from SSL certificate CN; baked into the downloadable script
- **Widget setup UI** — step-by-step instructions, "Copy Script" and "Download .js" buttons in web dashboard settings
- Dark/Light mode support (follows iOS system appearance)

### 🔒 SSL / Let's Encrypt
- **SSL mode selector** in Settings → Web Dashboard: Auto (self-signed), Custom (own certs), Off (HTTP only)
- **Let's Encrypt integration** — use certbot certificates for trusted HTTPS without browser warnings
- **Certificate monitoring** — daily background check of certificate expiry with color-coded status indicator (green >30d, orange ≤30d, red ≤7d)
- **Auto-renewal** — optional automatic `certbot renew` when certificate is within configured threshold (default: 30 days); copies renewed certs to app directory

### ⏱ Smart Scheduling (Spot Price Optimizer)
- **Find cheapest time blocks** from day-ahead spot market prices for running large appliances
- Configurable duration (0.5–12 hours), shows average price and savings vs. daily average
- Optional **auto-scheduling** via Shelly RPC relay control

### ☀️ PV Surplus Control
- **Automatic relay switching** based on solar excess power with state machine (IDLE → PENDING_ON → ON → PENDING_OFF)
- Configurable thresholds (on/off), debounce timer, and **priority-ordered consumer list**
- Switches on consumers (boiler, wallbox) when surplus exceeds threshold; switches off when surplus drops

### 🚗 EV Charging Log
- **Automatic detection** of electric vehicle charging sessions from wallbox power patterns
- Logs each session: start/end time, energy (kWh), peak power, duration, and cost
- Monthly summary with total sessions, kWh, and cost breakdown

### 💱 Tariff Comparison
- Compare actual consumption costs across **8 pre-defined German electricity tariffs** (Stadtwerke, Tibber, 1Komma5°, Ostrom, E.ON, Vattenfall, EnBW, HT/NT)
- Simulates **fixed, time-of-use, and dynamic spot** tariff models
- Shows annual savings potential per tariff

### 🔋 Battery Storage Monitoring
- Track battery **state of charge (SOC)**, charge/discharge cycles, and round-trip efficiency
- SOC timeline chart, cycle detection, optimal charging time recommendations based on spot prices

### 📊 InfluxDB / Prometheus Export
- **InfluxDB**: Push energy metrics via HTTP line protocol (v1.x + v2.x). Configurable interval, measurement name, authentication
- **Prometheus**: Expose `/metrics` endpoint in text exposition format. Gauges for power, voltage, current, frequency per device/phase

### 🔌 REST API v1
- Formalized API: `/api/v1/devices`, `/api/v1/devices/{key}/samples`, `/api/v1/costs`, `/api/v1/spot_prices`, `/api/v1/co2`, `/api/v1/openapi.json`
- **Bearer token authentication**, CORS headers, rate limiting

### 🤖 AI Energy Advisor
- **Rule-based tips** from standby analysis, spot price spreads, consumption trends, and weather data
- Optional **LLM enrichment** via Ollama (local), OpenAI, or Anthropic API for natural language summaries
- Sorted by savings potential (€/year)

### 🏆 Gamification (Goals & Achievements)
- Weekly and monthly **consumption goals** with auto-calculated targets (90% of previous period)
- **10 badges**: Energy Saver, Standby Killer, Solar Champion, 7/30-day Streak, Night Saver, Peak Avoider, and more
- **Streak tracking** with progress visualization

### 🏠 Multi-Location Support
- Manage **multiple sites** (home, office, vacation home) with separate device sets
- Optional **separate databases** per location
- Location switcher in the web dashboard; aggregate view across all locations

### 🖥 Cross-Platform
- macOS / Windows / Linux
- One-click start scripts — opens dashboard at `https://localhost:8765` in your default browser

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

Demo Mode lets you test the full application **without any Shelly devices** — enable it in Settings → Advanced. It generates realistic live data (with jitter + load spikes) and CSV history for plots and exports.

---

## Languages

The UI supports 9 languages. **The app always starts in English** — switch to any other language in **Settings → Language** during the session; the next restart resets back to English.

| Code | Language   |
|------|------------|
| `en` | English (default) |
| `de` | German     |
| `es` | Spanish    |
| `fr` | French     |
| `pt` | Portuguese |
| `it` | Italian    |
| `pl` | Polish     |
| `cs` | Czech      |
| `ru` | Russian    |

---

## Password-Protected Shellys

If you have set an admin password on your Shelly's web UI, the analyzer will reach it transparently — both the **5-step setup wizard** and the **Settings → Devices** page prompt for the password whenever a device responds `401 Unauthorized`.

| Generation | Models                                    | Auth scheme |
|------------|-------------------------------------------|-------------|
| **Gen 1**  | Shelly 1, 1PM, 2.5, EM, 3EM, Plug S       | HTTP Basic  |
| **Gen 2**  | Plus 1, Plus 1PM, Plus 2PM, Plus Plug S   | HTTP Digest |
| **Gen 3**  | Pro 1, Pro 1PM, Pro 4PM, Pro 3EM          | HTTP Digest |
| **Gen 4**  | latest Plus / Pro firmware                | HTTP Digest |

The HTTP client (`io/http.py`) registers per-host credentials, tries Digest first, parses the `WWW-Authenticate` header on 401, falls back to Basic if needed, and caches the working scheme so subsequent calls go through with one round trip. Works on Windows / macOS / Linux without any platform-specific code (built on `requests.auth.HTTPDigestAuth` / `HTTPBasicAuth`).

Credentials are stored in `config.json` per-device (`username` + `password`) and never returned to the browser — `GET /api/devices` only exposes the username and a `has_password: bool` flag, and `PUT /api/devices/<key>` honours the `***` masked placeholder so the saved password isn't accidentally overwritten when you edit other fields.

All HTTP touch-points pick up the credentials automatically: the live poller, historical CSV sync, the local scheduler, switch toggling, firmware OTA, mDNS rescan and the "Probe" button on the Settings page.

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
git tag v14.3.6
git push origin v14.3.6
```

The GitHub Actions workflow will automatically build the release ZIP and publish it.

---

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for the full version history.

---

Copyright (c) 2026 Robert Manuwald. Free to use. See `LICENSE` for details.
