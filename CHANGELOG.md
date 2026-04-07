# Changelog

## 16.13.49 - 2026-04-07
### Changed
- **Heatmap tab significantly enhanced** with new visualizations and statistics:
  - **8 summary metric cards** – total, daily average, days with data, peak hour, weekday avg, weekend avg, peak day (with date), best day (with date)
  - **Color legend** under calendar with gradient preview and max value label
  - **EUR unit option** – new unit selector for cost-based heatmap (green→purple gradient)
  - **Monthly breakdown bar chart** – 12 bars colored by intensity with value labels
  - **Weekday pattern bar chart** – 7 bars (blue=weekday, orange=weekend) with legend
  - **Year label** in calendar title for clarity
  - **API enhanced** – `/api/heatmap` now returns `summary` object with total, avg_daily, days_with_data, peak_day, min_day, peak_hour, weekday/weekend averages, monthly totals, and per-weekday averages

## 16.13.48 - 2026-04-07
### Changed
- **Weather tab completely redesigned** with rich visualizations:
  - **Weather hero card** – large temperature display with description and "feels like"
  - **6 metric cards** – temperature, humidity, wind, clouds, pressure (hPa), feels-like
  - **Correlation banner** – color-coded interpretation icon (heating/cooling/neutral) with data point count
  - **6 correlation metrics** – Pearson r, HDD, CDD, total kWh, kWh/HDD, kWh/CDD
  - **Temperature zone breakdown** – avg consumption for cold (<10°C), mild (10-20°C), warm (>20°C)
  - **Humidity correlation** – separate Pearson r for humidity vs consumption
  - **Comfort zone bar chart** – horizontal bars showing avg kWh/h per temperature bucket (8 ranges)
  - **Scatter + Timeline side-by-side** – two-column layout on desktop
  - **Daily temperature range chart** – min/max range lines with colored dots + kWh bars overlay
  - **Best/Worst energy days** – top 3 lowest and highest consumption days with weather context
  - **API enhanced** – returns `daily` aggregates, `humidity_corr`, `comfort_zones`, `feels_like_c`, `pressure_hpa`, `description`, temperature zone averages

- **Goals tab completely redesigned** with gamification system:
  - **Level & XP hero** – large level display with XP bar, rank title (Bronze→Master), background fill
  - **Streak visualization** – fire emojis (up to 10), current + best streak display
  - **Enhanced goal cards** – weekly + monthly with large numbers, achievement icons, remaining/saved text
  - **4 statistics metrics** – daily average, daily target, days under target, savings in EUR
  - **30-day daily chart** – bar chart with green (under target) / red (over target) bars and dashed target line
  - **Weekly trend chart** – 8-week bar chart with target markers on each bar
  - **Best/Worst days ranking** – top 10 best and top 5 worst days with percentage of target
  - **Rich badge grid** – responsive auto-fill grid with icon, name, description, progress bar, percentage
  - **XP system** – 10 XP per streak day, 50 per badge, 100 for weekly goal, 200 for monthly goal, 5 per day under target
  - **API enhanced** – returns `daily_history`, `weekly_history`, `level`, `xp`, `savings_eur`, `best_days`, `worst_days`

## 16.13.47 - 2026-04-07
### Changed
- **Feature-gated tabs** – tabs like Solar, Battery, EV, EV Log, Schedule, Tariff, Weather, CO2, Anomalies, Forecast, Advisor, Goals, and Tenants are now hidden from the navigation when their corresponding feature is not enabled in Settings. `/api/config` now returns a `features` map with enabled flags. If the user navigates to a disabled tab (e.g. via saved state), the app falls back to the Live tab.

## 16.13.46 - 2026-04-07
### Changed
- **Compare tab completely redesigned** with rich visualizations:
  - **4 overview metrics** – A total, B total, delta with color + icon, daily average comparison
  - **Visual total comparison** – horizontal A vs B bars with percentage fill and values
  - **Delta indicator** – centered large icon + percentage + absolute difference
  - **Daily average chart** – side-by-side bar comparison
  - **Timeline chart** – grouped bars with legend (blue=A, orange=B)
  - **Cumulative line chart** – running total of A vs B as overlaid lines with end labels
  - **API enhanced** – returns `label_a/b`, `name_a/b`, `days_a/b`, `avg_a/b` for richer UI

## 16.13.45 - 2026-04-07
### Changed
- **Costs tab significantly enhanced** with new visualizations and statistics:
  - **Cost donut chart** – per-device monthly cost breakdown with legend
  - **Cost ranking bar chart** – devices sorted by monthly cost
  - **Period comparison chart** – grouped bars for today/week/month/year with kWh bars and EUR labels
  - **Per-device comparison bars** – horizontal progress bars showing each device's monthly share with kWh, EUR, and percentage
  - **Projection card** – 4 metric cards: monthly projection, yearly projection, vs. last month delta, daily average cost
  - **API `summary` object** – `/api/costs` now returns aggregated summary with today/week/month/year/last_month kWh+EUR and projection

## 16.13.44 - 2026-04-07
### Added
- **Tariff schedule editor in Settings** – new list field in "Preise & Tarif" to add/edit/delete tariff periods (start date + electricity price + base fee). Hint text explains that historical data keeps the old price.

### Fixed
- **Costs tab now uses date-based pricing** – when tariff periods are configured, the Costs tab calculates costs per day using the effective price on that date (via `effective_pricing_for_date()`). Previously all historical ranges used today's price, resulting in wrong costs for periods before a price change. Only applies to fixed tariff mode; dynamic/spot pricing is unaffected.

## 16.13.43 - 2026-04-07
### Changed
- **Daily and monthly reports completely rewritten** with much richer content:
  - **Daily report**: Per-device kWh + cost + peak hour + peak W, total with comparison to day before, peak consumption hour, night standby estimate (W + annual kWh), CO2 estimate, monthly projection, and ASCII 24h bar chart
  - **Monthly report**: Ranked device list with medals, per-device share %, comparison to previous month (kWh + cost delta), daily average, best/worst day, CO2 + tree-days, year projection
  - **All in German** for better readability

### Added
- **Telegram chart photos** – daily and monthly summaries now include a matplotlib-generated PNG chart sent via `sendPhoto`:
  - Daily: stacked hourly bar chart + per-device totals with cost labels
  - Monthly: daily totals bar chart with average line + device pie chart
  - Dark theme matching the app's dark mode
- **Email PDF attachments** – email summaries now include a PDF report (reportlab) with summary text + embedded chart image, plus the chart PNG as separate attachment. Footer with copyright.
- **`_telegram_send_photo()`** – new method for sending images via Telegram Bot API
- **`_generate_summary_chart()`** – matplotlib chart generation for daily/monthly (stacked bars, pie charts, dark theme)
- **`_generate_summary_pdf()`** – reportlab PDF generation with text + chart embedding
- **Telegram long message support** – messages > 4000 chars are automatically split into chunks

## 16.13.42 - 2026-04-06
### Changed
- **Forecast tab completely redesigned** with rich visualizations:
  - **Overview metrics** – daily average, trend with color-coded arrow, next month + next year kWh and cost
  - **Trend analysis card** – large trend indicator with percentage, history summary (total kWh, average W), days of data used
  - **Cost projection bar chart** – month vs. year cost comparison
  - **Main chart with confidence band** – history bars (blue) + forecast bars (red) with shaded upper/lower confidence interval, dashed divider line, legend
  - **Weekday profile** – Mon–Sun relative factors with color coding (green = below average, red = above), threshold line at 1.0
  - **Hourly profile** – 00–23h with same color coding, hint text explaining the factor
  - **Two-column layouts** – trend + cost side by side, weekday + hourly side by side
  - **German weekday names** – localized via t() calls

## 16.13.41 - 2026-04-06
### Changed
- **Anomaly tab completely redesigned** with rich statistics and visualizations:
  - **Overview metrics** – total events, types detected, devices affected, max/avg sigma deviation
  - **Type donut chart** – visual breakdown by anomaly type (unusual daily, night consumption, peak time) with colored legend
  - **Device breakdown** – per-device event count with progress bars, type chips, and color-coded categories
  - **Sigma distribution chart** – bar chart showing event distribution across sigma buckets (2-3σ, 3-4σ, etc.)
  - **Rich event timeline** – each event with type icon, color-coded dot, device name, value (kWh/W), sigma bar indicator, timestamp, and description
  - **Empty state** – friendly message with checkmark when no anomalies detected
  - **API extended** – returns `total_count`, `type_counts`, `device_counts`, `max_sigma`, `avg_sigma`, `model`, `sigma_threshold`

## 16.13.40 - 2026-04-06
### Changed
- **CO2 tab significantly enhanced** with new analytics and visualizations:
  - **CO2 analytics card** – average, minimum (with hour), maximum (with hour) intensity
  - **Trend indicator** – compares last 6h vs previous 6h average, shows rising/falling/stable with percentage
  - **Green score card** – renewable energy percentage as animated ring chart, A–E rating scale based on current intensity
  - **Fuel mix donut chart** – visual breakdown of generation sources with legend (replaces simple stacked bar)
  - **Fuel detail table** – scrollable table with color dots per fuel type, MW, %, and lifecycle CO2 factor
  - **Two-column layout** – analytics + score side by side, fuel donut + detail side by side

## 16.13.39 - 2026-04-06
### Changed
- **All tabs now use 80% width on desktop** – previously only NILM and Standby had the wider layout (80%), all other tabs were limited to 66%. Now consistent across all tabs.

## 16.13.38 - 2026-04-06
### Fixed
- **Standby device cards now fill full width** – device grid adapts columns to device count (2 devices = 2 columns, 3 = 3, etc.) instead of fixed 3-column grid leaving empty space. Standby pane uses 80% width on desktop (like NILM). Mobile stays single column.

## 16.13.37 - 2026-04-06
### Changed
- **Standby tab completely redesigned** with rich statistics and per-device detail:
  - **4 overview metric cards** – annual cost, annual kWh, average base load, risk overview (high/medium/low counts)
  - **Cost donut chart** – per-device standby cost breakdown with legend
  - **Cost ranking bar chart** – devices sorted by annual standby cost, color-coded by risk
  - **Per-device detail cards** – each with: base load W, night median W, standby time %, annual kWh and cost, standby share %, standby-vs-active progress bar, and individual 24h power profile mini chart with standby threshold line
  - **24h comparison chart** – all devices overlaid as line charts with color legend, shared Y axis
  - **Savings potential section** – actionable tips per device with estimated annual savings
  - **Empty state** – friendly message with progress hint when no data available
- **API extended** – `/api/standby` now returns `night_median_w`, `standby_pct`, `total_kwh` per device

## 16.13.36 - 2026-04-06
### Fixed
- **Settings page failed to load** – typographic quotation marks (`„"`) in a JavaScript string hint caused rendering issues on some browsers. Replaced with plain text.

## 16.13.35 - 2026-04-06
### Added
- **Tariff type selector in "Preise & Tarif" settings** – prominent "Tarif-Modell" dropdown (Fester Tarif / Dynamischer Tarif) directly in the pricing section so users don't have to navigate to the Spot-Preise section to switch. Selecting "Dynamisch" automatically enables `spot_price.enabled`. Spot-Preise section now shows hints that tariff type is configured in Preise & Tarif.

## 16.13.34 - 2026-04-06
### Fixed
- **Standby tab always showed 0** – the standby analysis relied exclusively on the `hourly_energy` DB table (populated by sync). If no sync had run or the DB was empty, every device returned `None` and the tab was blank. Now falls back to the computed DataFrames (CSV-based, same source as Costs/Plots) via new `analyze_standby_from_df()` function. Computes base load (10th percentile), night median, annual standby kWh/cost, risk level, and 24h hourly profile from whatever data is available.

## 16.13.33 - 2026-04-06
### Fixed
- **Live tab today kWh now matches Costs/Plots tabs** – Live tab was using the DB hourly table as baseline for today's kWh, while Costs and Plots used the computed DataFrame (CSV-based). Now Live reads from the same computed cache as Costs, so both show identical values. Falls back to DB hourly if computed data is unavailable.
- **Costs summary cards still excluded 1-phase devices** – the summary section (top metric cards) still had the old `phases >= 3` filter from before v16.13.26; only the per-device detail section was fixed. Now both sections include all non-switch devices.

## 16.13.32 - 2026-04-06
### Fixed
- **Navigation bar centered on desktop** – bottom nav uses `justify-content: center` on viewports ≥ 900px so tabs sit in the middle of the screen. Mobile remains horizontally scrollable as before.

## 16.13.31 - 2026-04-06
### Changed
- **License changed from MIT to proprietary** – software remains free to use, but copyright belongs to Robert Manuwald. Modification, redistribution, and sublicensing require written permission. Updated LICENSE, pyproject.toml, README.md, and version badge now shows `© Robert Manuwald`.

## 16.13.30 - 2026-04-06
### Fixed
- **Version badge shown twice in Plots tab** – the `/plots` page is embedded as an iframe inside the dashboard, both had the `_inject_version_badge` overlay, resulting in duplicate version text. Removed badge injection from the plots page.

## 16.13.29 - 2026-04-06
### Added
- **Dedicated NILM statistics tab** – new "NILM" tab (brain icon) with rich, colorful statistics for Non-Intrusive Load Monitoring:
  - **Overview metric cards** – pattern count, transitions detected, devices monitored, appliance categories
  - **Top 10 detected patterns** – each with icon, power centroid (W), event count, percentage share, standard deviation, peak hour, mini power-profile sparkline canvas, and color-coded power bar
  - **Hourly activity heatmap** – bar chart showing transition frequency by hour of day with blue→red gradient
  - **Category donut chart** – visual breakdown of detected appliance categories (kitchen, laundry, cooling, heating, etc.)
  - **Per-device breakdown** – cluster count, total events, and top matched appliances per monitored device
  - **Recent transitions timeline** – last 30 power transitions with timestamp, device, power before/after, and on/off indicator
  - **Appliance signature database** – expandable reference of all 25 built-in appliance signatures with power ranges and pattern types
  - **Learning progress** – when < 10 transitions detected, shows progress bar and status
- **`/api/nilm_detail` endpoint** – returns rich NILM data: clusters with std_w/typical_hour, raw transitions with timestamps, hourly distribution, per-device stats, category breakdown, and appliance signature reference
- **Responsive layout** – pattern grid scales from 1→2→3 columns, two-column layout for categories + devices on desktop, full-width on mobile
- **Full i18n** – ~40 new translation keys (DE/EN/ES) for all NILM tab strings; appliance names and category labels translated; other languages fall back to English

### Removed
- **NILM badge from header bar** – replaced by the dedicated NILM tab

## 16.13.28 - 2026-04-06
### Fixed
- **Daily summary fired on every restart** – `_summary_last_daily` initialized as empty string, so `"" != "2026-04-06"` was always true and the summary sent immediately on startup. Now initializes to today's date, and restores the last-sent date from the persisted config field `telegram_daily_summary_last_sent` / `telegram_monthly_summary_last_sent`.
- **Summary dates now persisted to config.json** – survive restarts without re-sending.
- **Summary only sent when at least one channel is enabled** – skips building the summary entirely if none of Telegram/Email/Webhook summaries are enabled.

### Changed
- **Daily summary much more informative** – per-device kWh + cost, comparison vs. day before (📈/📉/➡️ with % delta), monthly projection based on this month's average, sorted output.
- **Monthly summary improved** – per-device breakdown sorted by consumption, comparison vs. previous month, daily average calculation.

## 16.13.27 - 2026-04-06
### Fixed
- **Alert rules never triggered Telegram/Email/Webhook** – root cause: the `BackgroundServiceManager` kept its own copy of `AppConfig` from startup. When alert rules were created/updated via the Settings or Alerts API, the new config was written to `state.cfg` and `state.reload_config()` was called, but the background manager's `self.cfg` was never updated. Result: `self.cfg.alerts` was always the empty list from startup. Fix: `reload_config()` now propagates the config to `state._bg.cfg` so alert rules, Telegram credentials, and all notification settings are immediately visible to the background feed loop.
- Added debug + info logging to alert evaluation and Telegram sending for diagnostics.

## 16.13.26 - 2026-04-06
### Fixed
- **Costs tab showed no data for 1-phase devices** – the costs action filtered for `phases >= 3` only, excluding all single-phase Shelly devices. Now includes all non-switch devices regardless of phase count.
- **Computed device cache never refreshed** – the lazy-loaded cache was populated on first access and never invalidated (except on config reload). If the first request arrived before the initial sync completed, the cache stayed empty forever. Now auto-refreshes every 2 minutes.

### Added
- **Telegram alert notifications** – alert rules with `action_telegram=true` now send detailed messages via Telegram Bot API when thresholds are breached. Supports all metrics (W, V, A, VAR, cos φ, Hz, A_N) with per-phase granularity, configurable duration and cooldown.
- **Email alert notifications** – alert rules with `action_email=true` send SMTP emails with subject line and detailed body. Supports TLS/STARTTLS, multiple recipients, and file attachments.
- **Webhook alert notifications** – alert rules with `action_webhook=true` POST JSON payloads to the configured webhook URL with device, metric, value, and threshold data. Supports custom HTTP headers.
- **Scheduled daily summaries** – sends per-device kWh + cost breakdown for the previous day via Telegram, email, and/or webhook at the configured time (`telegram_daily_summary_time`).
- **Scheduled monthly summaries** – sends per-device kWh + cost breakdown for the previous month (on the 1st–2nd of each month) via all configured channels.

## 16.13.25 - 2026-04-05
### Changed
- **README rewritten for web-only architecture** – removed the "Desktop App" screenshots section (17 images) and all desktop-app wording. The app is now described as a cross-platform **web application** with a browser UI. "Setup Wizard" section points to the new `/setup` web wizard. Removed references to `config.json`, matplotlib plots, treeviews, and other desktop-only concepts.
- **Deleted 17 desktop screenshot files** from `docs/screenshots/` (`desktop_*.png`). Web, Plots and iOS Widget screenshots kept.

## 16.13.24 - 2026-04-05
### Added
- **First-run setup wizard (Web)** – the old desktop onboarding flow returns as a web page at **`/setup`**. 5 steps:
  1. Welcome
  2. Devices: mDNS scan + manual IP add, with inline "added" list
  3. Pricing: electricity price, base fee, VAT toggle
  4. Spot prices (optional): enable + bidding zone (DE-LU, AT, CH, FR, NL, BE)
  5. Done → link to dashboard
- **Auto-redirect from `/` to `/setup`** when no devices are configured. Append `?skip_wizard=1` to bypass.
- **"🪄 Einrichtungs-Assistent" button** in Settings → Devices to re-enter the wizard any time.

## 16.13.23 - 2026-04-05
### Fixed
- **Device auto-detection on Add-by-IP and Probe didn't work** – `probe_device()` returns a `DiscoveredDevice` dataclass (attributes: `gen`, `model`, `kind`, `component_id`, `phases`, `supports_emdata`) and raises `ValueError` when the host is not a Shelly. Both blueprint endpoints treated it as a dict and called `.get()` on it, so detection always silently fell through to "host.replace('.', '_')" as key and defaulted `kind=em`, `phases=3`. Now:
  - `/api/devices/probe` converts the dataclass to a JSON dict (`gen`, `model`, `kind`, `phases`, `supports_emdata`, `component_id`).
  - `POST /api/devices` correctly reads the dataclass attributes, picks up the real `kind`, `gen`, `model`, `phases`, `component_id` (→ `em_id`) and `supports_emdata`.

## 16.13.22 - 2026-04-05
### Changed
- **Settings → Devices: full editor per device** (matching the old desktop tkinter form):
  - Editable inline fields: **Name**, **Host / IP**, **EM-ID**, **Type** (em / switch / unknown), **Generation** (auto / 1 / 2 / 3), **Phases** (1 / 2 / 3), **Model**, **supports_emdata** toggle.
  - **💾 Save** button per card writes directly via `PUT /api/devices/<key>`.
  - **🔌 Probe** button per card tests connectivity via `/api/devices/probe`.
  - **🔌 Alle prüfen** button probes every configured device.
  - Online/offline indicator shown explicitly in each card.

## 16.13.21 - 2026-04-05
### Fixed
- **New devices invisible in Live until first sample arrives** – `/api/state` only returned devices that were already present in the live-state snapshot, so a freshly added device (via IP) stayed invisible in the Live tab until the very first successful poll landed in the store (or indefinitely if the device was unreachable). `/api/state` now also returns configured devices that have no snapshot yet, with zeroed values and `pending:true`. The device card becomes visible immediately; real values fill in as soon as the poller receives the first sample.

## 16.13.20 - 2026-04-05
### Fixed
- **Newly added devices stayed "offline" (grey dot) until app restart** – adding or removing a device through the Settings UI updated the config but did **not** restart the live poller, so the new device never appeared in the live-state snapshot. `/api/devices` (POST / PUT / DELETE) now calls `BackgroundServiceManager.reload(new_cfg)` after the config save, which restarts the live poller with the new device list. A device added via IP goes green within one poll interval (≈ 1 s).

## 16.13.19 - 2026-04-05
### Fixed
- **mDNS scan: "discover_shelly_mdns() got an unexpected keyword argument 'timeout'"** – the blueprint passed `timeout=` while the service function expects `timeout_seconds=`. Also: the blueprint treated the returned `MdnsShelly` dataclass like a dict and called `.get()` on it. Fixed both: now using `timeout_seconds=` and attribute access (`getattr(r, "host", "")` etc.). "Already added" now compares by host IP against the configured device hosts.

## 16.13.18 - 2026-04-05
### Changed
- **Settings: list editors replace JSON textareas** – TOU periods, PV-Surplus consumers, Tariff Compare entries and Tenants are now edited via proper per-item cards with labelled input fields, add/delete buttons and a device dropdown where applicable. No JSON knowledge required.
  - Each list item is a card with an auto-laying-out grid of real inputs (text, number, select, checkbox, select-device, comma-separated list).
  - **➕ Hinzufügen** button appends a new item pre-filled with sensible defaults; **✕** removes an item; changes are saved with the rest of the section.
  - Works responsively: the item grid collapses to one column on mobile.

## 16.13.17 - 2026-04-05
### Changed
- **Settings: all config now editable via UI; no more references to config.json**:
  - Removed all "edit via config.json" hints from TOU, PV-Surplus, Tariff Compare, Billing and Tenant sections.
  - Added a **JSON editor** field type for list-typed settings: TOU periods, PV-Surplus consumers, custom tariff lists, tenant list. Uses a monospace textarea with placeholder examples, parsed back into real arrays on save with a clear error toast if JSON is invalid.
  - **Billing** section now includes multi-line `address` textareas for both issuer and customer, plus customer `email` and `vat_id`.

## 16.13.16 - 2026-04-05
### Changed
- **Settings tab: layout overhauled**
  - **Checkbox fields as toggle pills**: label + checkbox now sit inside a tinted, rounded row with a hover effect. Clicking anywhere on the row toggles the checkbox.
  - **Consistent field heights**: every grid cell has `min-height:58px` so the two-column grid no longer looks staggered.
  - **Mobile sidebar**: the left tree view becomes a horizontal scroll strip (sticky under the header) instead of a wrapped pill block eating half the screen. Active tab uses an underline instead of a left border.
  - **Alarm rules**: the four notification checkboxes (Telegram / Webhook / E-Mail / Popup+Beep) now use the new toggle style, consistent with the rest of the boolean fields.

## 16.13.15 - 2026-04-05
### Fixed
- **Plots tab Hz: historical values = 0** – old DB rows had `freq_hz = 0` (or NULL) because earlier sync runs did not populate the field. 0-values are now treated as `NaN` and appear as gaps in the plot instead of a 0-line. Grid frequency is never 0, so this filter is physically correct.

## 16.13.14 - 2026-04-05
### Fixed
- **Plots tab Hz: "Length of values (0) does not match length of index (1441)"** – `_wva_series()` initialised `y = pd.Series(dtype=float)` (length 0) when the frequency column was missing, which clashed with the index assignment `pd.Series(y.to_numpy(), index=ts, ...)`. It now uses `y = [NaN] * len(df)`, plus a defensive length-check at the end that re-shapes `y` to `len(ts)` if any branch leaves it short. Also added `avg_freq_hz` as a fallback column alias for DataFrames coming from monthly aggregates.

## 16.13.13 - 2026-04-05
### Added
- **Plots tab: "Hz" available as metric** – new "Hz" option in the metric dropdown between A and VAR. The DB has stored `freq_hz` since v6.x, so historical values are available from the start of recording. Backend handler `_wva_series` already understood HZ/FREQ/FREQUENCY, only the frontend dropdown entry was missing.

## 16.13.12 - 2026-04-05
### Fixed
- **Live tab: sparklines reset after tab switch** – two causes:
  1. `/api/history` did not include `hz` at all → the Hz sparkline had no server history to fall back on.
  2. `loadHistory()` only ran on the first page load (`_historyLoaded` flag); on returning to the Live tab the server buffer was not refreshed. After >60 s away, all client-side buffer entries were filtered out by `liveWindowSec`, so the plot started again with a single point.
- **Fixes**:
  - `/api/history` now returns `hz`.
  - `stopLive()` resets `_historyLoaded=false`, so `startLive()` re-fetches server history on return.
  - `loadHistory()` merge now dedupes by timestamp and sorts chronologically (instead of blindly prepending), which also prevents duplicates on repeated refreshes.
  - Redraw block in `loadHistory()` now also updates `sp-in-*` (neutral current) and `sp-hz-*` (frequency) sparklines.

## 16.13.11 - 2026-04-05
### Fixed
- **I_N fallback did not take effect because the wrong endpoint was patched** – v16.13.10 added `_compute_i_n()` to `services/webdash.py` only, but the active Flask app serves `/api/state` and `/api/history` via the `web/blueprints/api_state.py` blueprint. The `i_n` returned from there was still the raw (often 0) measured value. `_compute_i_n()` is now also present in the blueprint and is used by both endpoints. The function additionally detects active phases by voltage **or** current (some Shelly models report only one phase voltage but all phase currents).

## 16.13.10 - 2026-04-05
### Added
- **Neutral-line current fallback: compute from phase currents** – if the device does not report a real I_N (no neutral CT attached), I_N is now estimated:
  - 3-phase: `|I_N| = √(I₁² + I₂² + I₃² − I₁·I₂ − I₂·I₃ − I₁·I₃)` (assuming 120° phase offsets)
  - 2-phase: `|I_N| = |I₁ − I₂|` (split-phase)
  - 1-phase: 0
  - A balanced load yields 0 A; an unbalanced load yields the magnitude of the neutral-conductor current. Applied to both `/api/live_state` and `/api/history`, so sparkline, detail chart and device card show meaningful values even without an N clamp.

## 16.13.9 - 2026-04-05
### Fixed
- **Live tab: clicking the Hz sparkline opened the I_N neutral-current detail chart** – the click handler and detail-chart renderer did not handle `metric='hz'`; the `else` branch fell through to "neutral" and also plotted the phase currents on top, which looked "weirdly shifted". Now there is a dedicated `hz` case with the correct title "Frequency (Hz)", a cyan line (#06b6d4) and a single series without phases.

## 16.13.8 - 2026-04-05
### Added
- **Live tab: frequency sparkline (Hz) under neutral current** – new mini-timeseries per device card, shows the grid frequency (around 50 Hz) with relative scaling so deviations (49.9 / 50.1) become visible. Colour: cyan (#06b6d4).

## 16.13.7 - 2026-04-05
### Changed
- **Costs tab: forecast tile now shows the dyn./fixed difference** – like the Today/Week/Month tiles, the forecast tile in the dynamic-tariff section now shows the arrow ↑/↓ with the €-difference vs. the fixed tariff (green if cheaper, red if more expensive).

## 16.13.6 - 2026-04-05
### Changed
- **Schedule tab: complete rewrite – now shows date, context and a clear action**:
  - **Duration picker** (1h/2h/3h/4h/6h/8h) – buttons to switch the block length
  - **Top-3 cheapest time windows** in the next 24 h (instead of just one), non-overlapping, with 🥇🥈🥉 ranking
  - **Date + weekday** ("Today · Monday" / "Tomorrow · Tuesday" / "Tuesday, 07.04.2026") instead of bare time
  - **Context tiles**: avg. 24 h spot, fixed tariff, cheapest hour, most expensive hour
  - **Savings vs. fixed tariff** (not just vs. 24 h average): green/red ct/kWh delta
  - **Example calculation per block**: "For 2 kWh of load (e.g. washing machine): −X ct vs. fixed tariff"
  - **Top action hint**: "Start washing machine / dishwasher / dryer / EV / heat pump during one of these windows"
  - **Prices including grid fees, electricity tax, KWK/§19/offshore surcharges, supplier margin and VAT** from the spot-price settings (previously raw price only)
- Backend `/api/smart_schedule?duration=H` now returns `blocks[]`, `avg_24h_ct`, `cheapest_hour_ct`, `most_expensive_hour_ct`, `fixed_ct`, `zone`.

## 16.13.5 - 2026-04-05
### Fixed
- **Plots tab: hour labels shifted by 2 h (UTC instead of local time)** – kWh bars and W/V/A time-series showed hours in UTC. During a sync at 21:00 local (Europe/Berlin DST = UTC+2) the most recently filled hour was labelled "18:00" instead of "20:00". Fixed in `_stats_series()` and `_wva_series()`: UTC timestamps read from the DB are now converted to Europe/Berlin before hour / day / week labels are generated. `_label_to_ts_range()` interprets labels as local time consistently, so the CO₂ and price lookups per bucket query the correct DB time ranges.

## 16.13.4 - 2026-04-05
### Changed
- **iOS widget: CO₂ chart shows full 24 h + current value robustly** – the `/api/widget` query for `co2_chart` now covers the last 24 h (previously -12h/+12h, which left half the window empty since CO₂ has no forecasts). `co2_current` now uses the **most recent available value** from those 24 h (instead of just the current hour), so the latest figure still shows even if a fetch went missing.
- **CO₂ chart label in the widget shows the current value** – the label above the mini bar chart now reads, for example, "CO₂ g/kWh · 24h · 187" instead of just "CO₂ g/kWh".

## 16.13.3 - 2026-04-05
### Changed
- **Plots tab always static (no zoom)** – the Plots tab now uses the simple mobile plots everywhere, without zoom/pan/scroll-zoom, regardless of viewport width. `isMobileView()` inside the Plots page always returns `true`, i.e. `fixedrange`, `dragmode: false`, `displayModeBar: false` and `doubleClick: false` for every Plotly chart.

## 16.13.2 - 2026-04-05
### Changed
- **Initial sync on app start** – regardless of whether auto-sync is enabled, `sync_all()` runs once in the background 3 s after server start, so the first dashboard load has fresh data in the DB. If auto-sync is enabled the periodic cycle still runs afterwards.
- **Plots tab refreshes on tab switch** – when switching to the Plots tab, the iframe Plotly view is always redrawn (via `__scheduleApplyPlots()` on the iframe window). So after the initial sync you automatically see the fresh data when switching to the Plots tab.

## 16.13.1 - 2026-04-05
### Fixed
- **Tenant billing: VAT double-counted** – the `/api/tenants/bill` endpoint passed gross prices to the service, and the service added 19 % VAT on top. Net prices are now used; VAT is applied once at the end against the subtotal. Also affects the `base fee` line: the base fee was passed in gross and then increased by 19 % again.

### Added
- **Tenant billing: tariff picker per bill** – new "Tariff" dropdown next to From/To with options *Auto (settings)* / *Fixed* / *Dynamic*. "Auto" uses the mode stored in `spot_price.tariff_type`; "Dynamic" computes a volume-weighted average spot price from the `spot_prices` DB table (including grid fees, electricity tax, KWK, §19, offshore, supplier margin from the `spot_price` config).
- **Tenant summary row shows tariff mode + effective price + base-fee info**: e.g. "⚡ Dynamic tariff · 24.87 ct/kWh net · base fee: 107.15 €/year net · VAT 19 %".
- **Base-fee rows show `–`** instead of "0.0" / "0.0000" for the kWh / price columns (only the amount is meaningful).

## 16.13.0 - 2026-04-05
### Changed
- **Plots tab remembers last settings** – every control change (view, metric, phases, time range, devices, filters) is persisted in `localStorage['sea_plots_qp']` and restored on the next page load. Works both in the dashboard Plots tab (iframe) and on a direct `/plots` call.
- **First visit shows kWh bars for the last 24 h** – new default view instead of timeseries/W/1h: `view=kwh` / `mode=hours` / `len=24 hours`. Shows hourly energy use plus CO₂ / price bars for the last 24 hours.

## 16.12.5 - 2026-04-05
### Changed
- **Kosten-Kachel-Label "Prognose (Monat)" → "Prognose"** – das (Monat) Suffix umbrach in der schmalen 4-Spalten-Gitteransicht auf zwei Zeilen und machte die Kachel höher als die anderen. Monat ist aus dem Kontext klar (vierte Spalte nach Heute/Woche/Monat). Gilt für alle Sprachen: Prognose / Forecast / Pronóstico / Prévision / Previsão / Previsione / Prognoza / Předpověď / Прогноз.

## 16.12.4 - 2026-04-05
### Fixed
- **Kosten-Kacheln: Werte auf gleiche Höhe** – Metric-Cards in den "Heute/Woche/Monat/Prognose" Reihen hatten unterschiedliche Höhen, weil manche Karten eine Sub-Zeile (z. B. "-1.55 €", "8.260 kWh") hatten und manche nicht (z. B. Dyn.-Tarif-Prognose). `metricCardHtml()` rendert jetzt **immer** ein `.metric-sub` div (mit `&nbsp;` Platzhalter bei leerem Sub), damit alle Kacheln identische 3-Zeilen-Struktur haben und die €-Werte horizontal auf gleicher Höhe stehen.

## 16.12.3 - 2026-04-05
### Added
- **Settings-Seite: Übersetzungen für alle 9 Sprachen** – Section-Titel, Gruppen-Überschriften, Buttons (Speichern/Test/Scan/…), Toast-Messages und Page-Title jetzt in **DE, EN, ES, FR, PT, IT, PL, CS, RU** mit nativen Übersetzungen (statt englischem Fallback). 59 UI-Strings × 7 neue Sprachen = 413 neue Übersetzungseinträge.
- Die 200 einzelnen **Feld-Labels** (settings.field.*) bleiben in nicht-DE/EN-Sprachen auf Englisch — für Detail-Settings ist das üblich.

## 16.12.2 - 2026-04-05
### Changed
- **i18n Pass 3: Settings field labels** – alle **200 Feld-Labels** in der Settings-Seite laufen jetzt durch i18n mit DE + EN. Betrifft jede Einstellung in jeder Sektion: Anzeige, Web-Server, Preise & Tarif, Auto-Sync, Telegram, E-Mail, Webhook, Anomalien, MQTT, InfluxDB, Prometheus, API, Updates, Solar, Batterie, PV-Überschuss, Spot-Preise, CO₂, Wetter, Prognose, Smart Schedule, EV Charging, KI-Berater, Gamification, Rechnung, Mieter, Multi-Location, Download, Demo.
- i18n-Keys folgen dem Schema `settings.field.<config_path>` (z. B. `settings.field.pricing.electricity_price_eur_per_kwh` → "Electricity price €/kWh"). renderField() nutzt `T("settings.field."+f.key, f.label)` mit automatischem DE-Fallback.

## 16.12.1 - 2026-04-05
### Changed
- **i18n Pass 2: Settings-Seite** – Seitentitel, Sidebar-Gruppen (Grundeinstellungen / Benachrichtigungen / Integrationen / Energie / Features / Erweitert), **alle 34 Section-Titel** (Geräte, Anzeige, Web-Server, Preise & Tarif, Telegram, E-Mail, MQTT, InfluxDB, Solar, Batterie, Spot-Preise, CO₂, Wetter, etc.) sowie die häufigsten Buttons (Speichern, Test) + Toast-Messages (Gespeichert, Fehler, Gerät hinzugefügt/entfernt, Regel erstellt/gelöscht) + Health-Check Status laufen jetzt durch i18n mit DE + EN Fallback. Field-Labels der einzelnen Settings-Felder bleiben vorerst deutsch (~200 Strings, Pass 3).
- **Neue /api/i18n Endpoint** – liefert den aktiven Sprach-Map mit `?prefix=` Filter. Wird vom Settings-Template beim Laden abgerufen, damit Client-seitig `T(key, fallback_de)` funktioniert.

## 16.12.0 - 2026-04-05
### Changed
- **i18n-Audit Pass 1: Plots & Sync Tab** – alle kürzlich hinzugefügten deutsch-hartcodierten Strings jetzt über das i18n-System (`t('web.xxx', {vars})`) mit DE + EN Übersetzungen. Betrifft:
  - **Plots Tab**: CO₂-/Strompreis-Titel (inkl. Ampel-Schwellen), Zusatzkosten/MwSt-Info, Fixpreis-Info, Trace-Namen ("Dynamisch"/"Fixpreis"), "I_N (Neutralleiter)"
  - **Sync Tab**: alle Buttons (Inkrement-Sync / Vollständiger Sync / Heute / Woche / Monat / Status), Labels (Auto-Scroll, HTTP-Logs, Log), Status-Nachrichten ("Lade Status…", "Starte Sync…", "Sync läuft…", "Netzwerkfehler…")
- **t() Helper in Dashboard + Plots JS**: unterstützt jetzt `{var}` Interpolation (`t('key', {mode:'day', job:42})` → "Sync läuft (mode=day, job=42) …").
- **Noch hartcodiert (Pass 2 folgt):** Settings-Seite (`/settings`) ist weiterhin komplett deutsch (~80 Labels/Section-Titel), sowie Export- und Control-Tab-Details. Fallback: fehlende Keys in anderen Sprachen zeigen Englisch.

## 16.11.6 - 2026-04-05
### Fixed
- **Dashboard Quick-Language-Switcher triggerte kein HTML-Re-Render** – der Sprach-Selector im Live-Settings-Modal rief `/api/run` mit `action:set_language` auf. Der Handler speicherte zwar die Sprache nach `config.json`, aber rief `state.reload_config()` **nicht** auf – das gecachte HTML blieb in der alten Sprache, so dass die Seite nach `window.location.reload()` wieder deutsch/alt aussah. Genau der Bug, der "springt zurück" auslöste. Jetzt ruft `set_language` vollständig `state.reload_config(new_cfg)` + `dispatcher.reload(new_cfg, lang=…)` auf.
- Debug-Log für Sprachwechsel in `/api/settings PUT` hinzugefügt, damit sich der Round-Trip im Server-Log verifizieren lässt.

## 16.11.5 - 2026-04-05
### Fixed
- **Settings: Dropdown springt nach Auswahl zurück** – nach "Speichern" wurde der Client-`cfg` Cache nicht aktualisiert; beim nächsten Re-Render (z. B. von einem anderen Tab) zeigte das Select wieder den alten Wert. Jetzt holt `saveSection()` direkt nach PUT die frischen Settings vom Server, schreibt sie in `cfg` und zeichnet die betroffene Section neu. Damit ist sichtbar verifiziert, dass der Server den Wert tatsächlich persistiert hat.
- **no-cache Header auf /settings + /api/settings** – stellt sicher, dass Browser und Proxies immer den aktuellen State holen, nicht den vor-Save-Snapshot.

## 16.11.4 - 2026-04-05
### Changed
- **Plots auf Mobile: kein Zoom/Pan mehr** – auf Touch-Geräten mit Viewport ≤760px sind Plotly-Charts jetzt statisch: `fixedrange:true` auf beiden Achsen, `dragmode:false`, keine Mode-Bar, kein Scroll-Zoom, kein Doppelklick. Hover bleibt aktiv. Der Browser kann wieder normal durch den Plots-Tab scrollen ohne dass Pinch/Swipe vom Chart abgefangen werden. Desktop unverändert (voll interaktiv).

### Fixed
- **Sprachwechsel: ActionDispatcher bekam neue Sprache nicht** – `settings.py` rief `dispatcher.reload(new_cfg)` ohne `lang=` auf, dadurch blieben PDF-Exporte und `/api/plots_data` Texte in der alten Sprache. Jetzt wird `state.lang` an den Dispatcher weitergegeben (mit Fallback für ältere Dispatcher-Signaturen).

## 16.11.3 - 2026-04-05
### Fixed
- **Plots-Tab auf Mobile winzig** – die `#pane-plots.active { height: … }` Regel stand nur im Desktop-Media-Query (≥900px), auf dem Handy hatte der Pane keine Höhe → iframe mit `height:100%` kollabierte auf wenige Pixel. Jetzt greift die Höhe auf allen Bildschirmen (`calc(100dvh - 140px)` Mobile, `calc(100vh - 96px)` Desktop) und das `#panes` Padding wird im Plots-Tab auf 0 gesetzt, damit der Iframe die volle Breite bekommt.

## 16.11.2 - 2026-04-05
### Fixed
- **Sprachänderung in Settings wirkte erst nach Server-Neustart** – die HTML-Templates für Dashboard, Plots und Control werden beim Serverstart einmal gerendert und gecached (mit aufgelösten `{t('…')}` Übersetzungen). `reload_config()` aktualisierte `state.lang`, aber nicht die gecachten Bytes. Jetzt werden alle drei HTML-Templates bei jeder Config-Änderung neu gerendert + gzip-komprimiert. Sprachwechsel greift sofort nach Reload.

## 16.11.1 - 2026-04-05
### Added
- **Fixpreis-Vergleichsbalken im Preis-Plot** – zweiter (grauer) Balken pro Bucket zeigt die Kosten beim Fixtarif aus `pricing.electricity_price_eur_per_kwh` (brutto), direkt neben den farbigen Dynamisch-Balken gruppiert. Sofort vergleichbar wie viel man mit/ohne Dynamic-Tariff zahlt.

## 16.11.0 - 2026-04-05
### Added
- **CO₂ & Preis-Balken pro Gerät** – eigene Karte je Gerät für CO₂ (g) und Strompreis (€), je Bucket – parallel zu den kWh-Balken.
- **Spotpreis inkl. Zusatzkosten & MwSt** – Strompreis verwendet jetzt den Endkundenpreis: Spot-Wholesale + Netzentgelte + Stromsteuer + KWK/§19/Offshore-Umlagen + Lieferanten-Marge + 19 % MwSt (aus `spot_price` Config, Komponenten via `total_markup_ct()`). Titel zeigt z. B. "inkl. 13.45 ct/kWh Zusatzkosten + 19 % MwSt".
- **Dark-Mode-Synchronisation** – der Plots-Iframe reagiert jetzt auf Theme-Wechsel im Dashboard (storage-Event Listener auf `sea_theme`) und zeichnet Plotly-Charts mit den neuen Farben neu.

### Changed
- **Balken-Beschriftung entfernt** – CO₂/Preis-Balken haben keine aufgedruckten Werte mehr, nur noch Mouseover-Tooltip (Intensität · Σ Bucket-Summe).

## 16.10.2 - 2026-04-05
### Changed
- **Plots kWh: Ampel-Farben für CO₂ & Preis** – jeder Balken wird nach seiner **Intensität pro kWh** grün/gelb/rot eingefärbt:
  - CO₂: grün < `green_threshold_g_per_kwh` (Default 150 g/kWh), rot ≥ `dirty_threshold_g_per_kwh` (Default 400 g/kWh), gelb dazwischen
  - Preis: grün/gelb/rot nach Perzentilen (33% / 66%) des angezeigten Zeitraums in ct/kWh
  - Hover zeigt Intensität + absolute Summe des Buckets.
- **Duplikate "kWh" in Plot-Titeln entfernt** – Per-Plot-Titel zeigt nur noch den Gerätenamen, nicht mehr "Gerät – kWh" (Meta-Zeile hat bereits "kWh • days").

## 16.10.1 - 2026-04-05
### Fixed
- **Plots kWh: `name 'unit' is not defined`** – v16.10.0 nutzte `unit` im neuen CO₂/Preis-Aggregationsblock, die Variable wird aber erst in `_stats_series` lokal gebildet. Jetzt im `view=kwh` Zweig lokal aus `mode` geparst (inkl. Suffix-Stripping wie `hours:24`).

## 16.10.0 - 2026-04-05
### Added
- **Plots → kWh view: CO₂ & dynamische Preise** – neue Balkendiagramme für CO₂-Emissionen (g pro Bucket) und Strompreis (€ pro Bucket), aggregiert aus `co2_intensity`/`spot_prices` × Summen-kWh aller Geräte pro Bucket. Zeigen nur wenn CO₂/Spot-Price-Module aktiv und Daten vorhanden.

### Changed
- **kWh-Balken pro Gerät getrennt** – je Gerät eigener Plot (statt gruppierter Balken in einem Chart). Erleichtert Lesen bei unterschiedlichen Größenordnungen.
- **X-Achsen-Labels lesbarer** – `tickangle: 'auto'` + `automargin` statt fixem 45°, sodass Plotly abhängig von Bucket-Anzahl automatisch rotiert und nie abgeschnitten wird.

## 16.9.3 - 2026-04-05
### Changed
- **Plots tab polish** – five fixes on the embedded `/plots` page:
  - Double scrollbars removed: only the iframe scrolls, outer pane fills the viewport.
  - Own day/night toggle removed (inherits from dashboard theme toggle).
  - Obsolete link to `/control` removed (Export is its own dashboard tab).
  - Each plot now shows the device name as a title above the chart.
  - Neutral conductor current (I_N) rendered in grey + dashed when viewing per-phase current (A).
- When `/plots` is opened standalone (not embedded), the topbar with home link + theme toggle still shows.

## 16.9.2 - 2026-04-05
### Fixed
- **Plots: "Fetch is aborted" beim Phasen-Umschalten** – jeder Control-Change (Phasen/Metric/Zeitbereich) startete sofort einen neuen `/api/plots_data` Request, ohne den vorigen abzubrechen; bei großen Zeiträumen lief der alte Request in das 12 s Timeout und zeigte die `AbortError` als Nutzer-Fehler. Jetzt: vorherige in-flight Fetch wird sauber ersetzt (`superseded`, nicht als Fehler angezeigt), echtes Timeout auf 60 s erhöht mit deutscher Meldung "Zeitüberschreitung – Zeitbereich verkleinern".

## 16.9.1 - 2026-04-05
### Fixed
- **Settings page stuck in dark mode** – `settings.html` had hard-coded dark CSS variables with no light-mode fallback. Added `:root[data-theme="light"]` override and theme bootstrapping from `sea_theme` localStorage key, so the settings page now follows the same light/dark theme as the dashboard.

## 16.9.0 - 2026-04-05
### Added
- **Plots tab in dashboard** – dedicated tab (📊 Plots) embedding the full-featured `/plots` page: historical time series for W / V / A / VAR / cos φ across all three phases (total or per-phase view) with flexible time ranges (minutes / hours / days) plus the kWh aggregation view (hours / days / weeks / months). Shares the dashboard theme. Lazy-loaded on first activation.

## 16.8.5 - 2026-04-05
### Fixed
- **"Inkrement-Sync" button appeared to do nothing** – `services/sync.py` had **zero** logging, so the Sync tab's log pane stayed empty while sync ran silently in a background thread. Added INFO logs for range, per-chunk progress, and final summary. Also the status panel no longer gets overwritten 50ms later by `refreshSyncStatus()` – now shows "Sync läuft …" until the job actually touches the DB (3s debounce).

## 16.8.4 - 2026-04-05
### Fixed
- **Sync tab "Lade Status…" stuck forever** – `/api/sync/status` called a non-existent `db.get_device_meta()` method and failed silently; the JS swallowed the `ok:false` response. Endpoint now uses `storage.load_meta()` and the UI shows real status text / errors instead of the permanent loading placeholder.

## 16.8.3 - 2026-04-05
### Changed
- **NILM status in header** – "NILM ML: lerne/…" badge moved from inside the Live tab to the top-right header next to the clock, visible on every tab.
- **All tabs centered + 66% width on desktop** – same layout rule as Live now applies to every pane (Costs, Heatmap, Solar, Weather, CO₂, …) on screens ≥900px. Mobile unchanged.

## 16.8.2 - 2026-04-05
### Changed
- **Live view centered + plots fill vertical space (desktop)** – live grid centered horizontally and sparklines now scale with viewport height (`clamp(56px, 11vh, 180px)` / `clamp(40px, 8vh, 130px)`) so plots use the full window height on desktop. Mobile view unchanged.

## 16.8.1 - 2026-04-05
### Changed
- **Live view width on desktop** – `#live-grid` now capped at 66% viewport width on screens ≥900px so device cards/sparklines don't stretch edge-to-edge on wide monitors. Mobile view unchanged (full width).

## 16.8.0 - 2026-04-05
### Fixed
- **CO₂ & spot-price fetchers never started in Flask app** – `Co2FetchService` (ENTSO-E) and `SpotPriceFetchService` existed but `BackgroundServiceManager.start_all()` didn't instantiate them. So `/api/co2` / `/api/plots_data` returned empty data for fresh installs. Both are now started on app boot (honours `co2.enabled` + `entso_e_api_token` and `spot_price.enabled`) and triggered immediately so data is available within ~1 min.

### Added
- **Manual refresh endpoints**: `POST /api/co2/refresh`, `POST /api/spot/refresh`, `GET /api/co2/status` – trigger an immediate fetch and check service status (enabled, token, last_error, last_fetch_ts).

## 16.7.3 - 2026-04-05
### Added
- **"Now" marker in spot-price and CO₂ charts** – red dashed vertical line with "jetzt" label marks the current hour in both the 24h Dynamic Price chart (Costs tab) and the CO₂ intensity chart (CO₂ tab).

## 16.7.2 - 2026-04-05
### Fixed
- **NILM forgetting learned state** – transitions were only auto-saved every 10 samples and on shutdown nothing was flushed. Now: flush on every re-cluster (every 5 min), flush on `BackgroundServiceManager.stop_all()`, flush existing learners before replacing on config reload, and register `atexit`/SIGTERM hooks in `__main__` so Ctrl-C persists state.

## 16.7.1 - 2026-04-05
### Fixed
- **Mieter-Abrechnung: "float() argument must be a string or a real number, not 'method'"** – `pricing.vat_rate` is a method on PricingConfig, not an attribute. Wrapped call in try/except with fallback to `vat_rate_percent / 100`. Also switched `base_fee_gross` lookup to `base_fee_eur_per_year`.

## 16.7.0 - 2026-04-05
### Added
- **Mieter tab (Nebenkostenabrechnung)** – new 🏘 Mieter pane:
  - List and edit tenants (name, Wohnung, persons, move-in/out dates, assigned devices)
  - Configure common-area devices (Allgemeinstrom) split per person
  - Compute per-tenant bills for any period with line items (per-device kWh, unit price, amount) + net/VAT/gross totals
- **New API endpoints**:
  - `GET /api/tenants` – list tenants + config
  - `PUT /api/tenants` – upsert entire tenant config
  - `GET /api/tenants/bill?period_start=...&period_end=...` – compute bills via existing `services/tenant.generate_tenant_bills()`

## 16.6.6 - 2026-04-05
### Fixed
- **Blank dashboard (only nav + header visible)** – duplicate `cb` variable declaration (var + const) in `pollSyncLogs()` threw a SyntaxError that aborted the whole inline script. Renamed to `httpCb`. All tabs render again.

## 16.6.5 - 2026-04-05
### Changed
- **Sync/Log tab: HTTP access logs filtered by default** – werkzeug request lines (every poll, every GET) no longer flood the log window. New "HTTP-Logs" checkbox in the Sync pane enables them on demand. New endpoint `POST /api/logs/config` with `{include_http: bool}`.

## 16.6.4 - 2026-04-05
### Changed
- **Version badge moved to top-left** – previously top-right.

## 16.6.3 - 2026-04-05
### Changed
- **Version badge moved to top-right** – was covering the bottom navigation bar; now sits discreetly in the top-right corner of every page.

## 16.6.2 - 2026-04-05
### Changed
- **Reverted mobile hamburger** – bottom navigation stays visible and horizontally scrollable on mobile as before. Hamburger button hidden on all viewports.

## 16.6.1 - 2026-04-05
### Added
- **Version badge on every page** – small `v<X.Y.Z>` badge fixed at bottom-right of Dashboard, Plots, Control and Settings pages so the running version is always visible.

## 16.6.0 - 2026-04-05
### Added
- **NILM (appliance detection) wired into web app** – `TransitionLearner` instantiated per 3-phase EM device on startup, fed from the live sample loop, re-clusters every 5 min. Persisted clusters seeded immediately so the live-view NILM badge no longer stays stuck on "waiting for data". Persisted to `data/runtime/nilm/<device>.json`.
- **Sync tab with live log window** – new 🔄 Sync pane (bottom-nav + hamburger drawer) with buttons for Incremental/Full/Day/Week/Month sync, per-device last-sync status and a streaming log window that tails server logs via `/api/logs` (2 s polling, auto-scroll toggle).
- **In-memory log ring buffer** – web app installs a log handler capturing up to 2 000 records; exposed via `GET /api/logs?since=<ts>&limit=<n>`.

## 16.5.1 - 2026-04-05
### Fixed
- **Live kWh today: include already-synced history** – live accumulator previously only summed energy since app start. Now reads baseline from `hourly_energy` (all kWh of today up to the latest synced hour) and only adds live-integrated samples *after* that point. Refreshes every 10 min to pick up new auto-sync data without double counting.

## 16.5.0 - 2026-04-05
### Fixed
- **Live view per-device kWh/cost stuck at 0** – Feed loop now trapezoid-integrates `power_w.total` samples into kWh per device with automatic midnight reset. Previously tried to read a non-existent cumulative sensor field from `raw`. Cost = kWh × gross unit price.
- **Live view: devices now always stacked vertically** (single column) – removes 2-column layout on wide monitors for easier scanning.
- **Live view: Play/Pause button removed** – live updates now always run, no pause control.
- **Language selector reverting to German after reload** – `set_language` action only returned `{ok:true}` without persisting. Now writes `ui.language` into config.json and updates AppState.
- **Shelly firmware updates: only stable, no beta** – `/api/health/<key>/update` now installs only the stable stage; beta-only updates return a clear error. Health check no longer flags beta-only availability as "update available".

### Added
- **Auto-sync: sub-hour granularity** – new `ui.autosync_interval_minutes` config field (0 = use hours). Editable in the Auto-Sync settings section. Minimum interval: 1 minute when minutes > 0.
- **Mobile hamburger menu** – on screens ≤600px the bottom navigation is replaced by a left-side drawer opened via a ☰ header button. Desktop layout unchanged.
- **CO₂ Erzeugermix sorted by share descending** – largest generators appear first in the stacked bar and the table.

## 16.4.2 - 2026-04-05
### Fixed
- **Firmware-Update: "bad or missing url parameter"** – Update-Endpunkt schickte immer `stage=stable`, auch wenn nur `beta` verfügbar war. Jetzt wird `available_updates` aus `Shelly.GetStatus` gelesen, Stage dynamisch gewählt (stable bevorzugt, sonst beta). Handhabt auch "Already in progress" (Code -106) als Erfolg.

## 16.4.1 - 2026-04-05
### Added
- **Firmware-Update direkt aus Health Check** – Pro Gerät mit verfügbarem Update erscheint ein gelber "⬆ Update starten"-Button. Zusätzlich "⬆ Alle updaten"-Button für Batch-Update aller Shellys mit pending Updates. Unterstützt Gen1 (/ota?update=1) und Gen2+ (RPC Shelly.Update).

## 16.4.0 - 2026-04-05
### Added
- **Alarm-Regeln Editor** (`/api/alerts` CRUD) – List, Create, Update, Delete AlertRule entries. Each rule with device_key, metric, operator, threshold, duration, cooldown, actions (telegram/webhook/email/popup/beep), message. New "Alarm-Regeln" section in settings with inline editor for each rule.
- **Health Check** (`/api/health`) – Ping all Shellys, show online status, latency, uptime, firmware version, firmware update availability. New "Health Check" section with "Jetzt prüfen" button.
- **`/api/version`** – Returns actual app `__version__` (not stale config.json version).
### Fixed
- **Version in "Über" section** – Was showing stale config.json version (15.0.23). Now fetches real app version from `/api/version`. Config version shown separately.

## 16.3.0 - 2026-04-05
### Added
- **Comprehensive settings UI** – Complete rewrite of `/settings` page covering ALL 30+ config sections (was: only 7 tabs). Organized in 6 groups: Grundeinstellungen, Benachrichtigungen, Integrationen, Energie, Features, Erweitert.
- **Settings link in dashboard header** – New 🔧 button next to ☀ theme toggle links directly to `/settings`. No more hidden settings.
- **Device management UI** – mDNS discovery button, IP-probe add, delete devices, online status indicators.
- **Connection test buttons** – Test MQTT, InfluxDB, Telegram connections directly from the settings UI.

Sections covered (all editable via web browser):
- Grundeinstellungen: Geräte, Anzeige, Web-Server, Preise & Tarif, TOU, Auto-Sync
- Benachrichtigungen: Telegram, E-Mail, Webhook, Anomalien
- Integrationen: MQTT, InfluxDB, Prometheus, REST API, Updates
- Energie: Solar, Batterie, PV-Überschuss, Spot-Preise, CO₂, Wetter, Prognose
- Features: Smart Schedule, EV Charging, Tarifvergleich, KI-Berater, Gamification
- Erweitert: Rechnung, Mieter, Multi-Location, Download/CSV, Demo, Über

## 16.2.2 - 2026-04-05
### Fixed
- **Blank web page** – Dashboard HTML rendered with Unicode escape sequences (e.g. `\u2265` instead of `≥`) because the extracted HTML template files were saved with JSON-escaped unicode. Reverted to importing `_HTML_TEMPLATE`, `_PLOTS_TEMPLATE`, `_CONTROL_TEMPLATE` directly from `services/webdash.py` (where strings are native UTF-8). Removed broken template files.

## 16.2.1 - 2026-04-05
### Fixed
- **Background services startup** – `MultiLivePoller` was called with wrong signature (missing `download_cfg`, wrong `devices` format). Fixed to pass `DeviceConfig` list and `DownloadConfig`.
- **LiveSample field mapping** – `_feed_loop` now correctly reads `power_w['total']`, `voltage_v['a']`, `current_a['a']` etc. (LiveSample uses Dict fields per phase, not flat attributes).
- **MQTT/InfluxDB/Scheduler signatures** – `MqttPublisher(config=...)`, `InfluxDBExporter(cfg=..., storage=...)`, `LocalScheduler(get_config=..., get_http=...)` — corrected to match actual constructors.
- **Port fallback** – If configured port is in use, auto-try next 19 ports (same behavior as old webdash.py). No more "Address already in use" crash.

## 16.2.0 - 2026-04-05
### Removed
- **Deleted entire `ui/` directory** – 27,896 lines of tkinter desktop code removed (27 files). The app is now 100% web-only.
### Added
- **Token-based authentication** – If `ui.live_web_token` is set in config, all pages/APIs require authentication. Login page at `/login`. Public endpoints: `/api/widget`, `/widget.js`, `/metrics`. Token can be passed via query param `?t=`, header `X-API-Key`, or session cookie.
### Changed
- **Launch scripts updated** – `start.sh`, `start.bat`, `start.command` no longer check for tkinter. They now start the Flask web server directly. Support `--port`, `--no-ssl`, `--debug` flags via pass-through.
- **action_dispatch.py** – Import path changed from `ui._shared` to `web.utils` (no tkinter dependency).

## 16.1.0 - 2026-04-05
### Added
- **Settings API** – `GET/PUT /api/settings` for full config CRUD, with secret masking and partial update support. Test endpoints for Telegram, MQTT, and InfluxDB connections.
- **Device Management API** – `GET/POST/PUT/DELETE /api/devices`, `POST /api/devices/discover` (mDNS scan), `POST /api/devices/probe` (IP probe), `POST /api/devices/<key>/firmware` (OTA update).
- **Sync & Data API** – `POST /api/sync` (trigger sync), `GET /api/sync/status`, `GET /api/data/stats` (DB size, row counts), `POST /api/data/cleanup` (retention).
- **Schedule API** – `GET/POST/DELETE /api/schedules` for device schedule CRUD.
- **Settings web page** – `/settings` route serving the settings management UI.
- **Total: 62 routes** (was 41 in v16.0.0).

## 16.0.0 - 2026-04-04
### Changed
- **BREAKING: Migrated from tkinter desktop app to Flask web-only** – The entire application is now a Flask web server. No more desktop GUI. All 19 feature panes (Live, Costs, Heatmap, Solar, Weather, Compare, CO2, Anomalies, Forecast, Standby, Sankey, EV Chargers, Export, Smart Schedule, EV Log, Tariff, Battery, Advisor, Goals) are served via the web dashboard.
- **New `web/` package replaces `ui/` and `services/webdash.py`** – Clean Flask blueprint architecture: `dashboard`, `api_state`, `api_data`, `api_actions`, `static_assets`, `metrics`. ~28,500 lines of desktop UI code removed.
- **Background services run alongside Flask** – `BackgroundServiceManager` starts live polling, scheduler, MQTT, InfluxDB export, and auto-sync in daemon threads.
- **Action dispatch extracted to standalone module** – `web/action_dispatch.py` handles all web actions (sync, export, switch control, widget data, costs, etc.) without any tkinter dependency.
- **Entry point now boots Flask** – `shelly-analyzer` CLI starts the Flask server. New flags: `--config`, `--host`, `--port`, `--no-ssl`, `--debug`.
### Added
- **Flask + Waitress dependencies** – Added `flask>=3.0` and `waitress>=3.0` for cross-platform web serving.
### Removed
- **pywebview dependency** – No longer needed (was for desktop webview).
- **matplotlib moved to optional** – Only needed for Telegram chart PNGs. Web uses Plotly.js. Install via `pip install shelly-energy-analyzer[charts]`.

## 15.0.23 - 2026-04-04
### Fixed
- **Startup crash: 'AppConfig' has no attribute 'web'** – Prometheus hint label used `self.cfg.web.port` but `web` config doesn't exist. Fixed with safe `getattr` chain.

## 15.0.22 - 2026-04-04
### Added
- **Desktop: InfluxDB & Prometheus settings UI** – New settings sections for InfluxDB export (URL, token, org, bucket, version, interval) and Prometheus endpoint. InfluxDB exporter starts/stops automatically on settings save.
- **Web: language selector populated** – Language dropdown in web settings now shows all 9 languages (was empty). Current language pre-selected on page load.
### Fixed
- **Web: /metrics Prometheus endpoint** – Fixed broken import (was `metrics.render`, now correctly calls `generate_metrics` from `prometheus_export.py`).

## 15.0.21 - 2026-04-04
### Added
- **i18n: ~940 new translations for ES/FR/PT** – Spanish now has 1328/1329 keys (near-complete), French and Portuguese 1284/1329. All remaining gaps fall back to English. Covers forecast, standby, weather, Sankey, MQTT, solar amortization, tenant billing, smart scheduling, EV log, tariff, battery, advisor, goals, and web dashboard sections.

## 15.0.20 - 2026-04-04
### Changed
- **Desktop: remove refresh buttons from new tabs** – Tariff, Battery, Advisor, and Goals tabs no longer have manual "Aktualisieren" buttons. Data refreshes automatically on tab switch via `_on_tab_changed()`.

## 15.0.19 - 2026-04-04
### Fixed
- **Cross-platform: 7 compatibility fixes for Windows/Linux**
  1. **i18n: 11 missing EN translation keys** – Added missing keys (telegram detail, web CO₂ labels). All 9 languages now have complete fallback chains (FR/PT/IT/PL/CS/RU inherit from EN via `_mk_lang`; ES falls back to EN at runtime).
  2. **Tab bar hiding: platform-aware offset** – `place(y=-30)` macOS hack replaced with platform-specific offsets (macOS: 30px, Windows: 26px, Linux: 32px).
  3. **SSL certificate generation: cross-platform** – Added `cryptography` library as primary method (pure Python, works everywhere). Falls back to `openssl` CLI only if library unavailable. CN extraction also uses `cryptography` first.
  4. **Emoji in desktop UI: auto-strip on unsupported platforms** – New `_strip_emoji()` function detects platform emoji support. Desktop `self.t()` automatically strips emoji on Linux and older Windows. macOS and Win10+ retain emoji.
  5. **Tariff tab: light/dark mode card colors** – Current tariff highlight uses `#fff3e0` (warm) on light theme, `#2a2000` on dark. Chart uses theme-aware colors via `_apply_plot_theme()`.
  6. **Hardcoded grays (#888888)** – Reviewed; these are equally readable on both dark and light backgrounds (muted gray). No change needed.
  7. **Network defaults: localhost → 127.0.0.1** – MQTT broker, InfluxDB URL, and Ollama URL defaults changed from `localhost` to `127.0.0.1` for reliable resolution on all platforms.

## 15.0.18 - 2026-04-03
### Fixed
- **Desktop tariff comparison: replaced invisible Treeview with card layout** – macOS Aqua dark mode makes ttk.Treeview rows invisible (both `ttk.Style` and `tag_configure` are ignored). Replaced Treeview entirely with `tk.Label`/`tk.Frame` card layout using explicit `fg`/`bg` colors. Each tariff is a bordered card showing name, annual cost, provider details, and savings indicator. Current tariff highlighted with orange border.
### Added
- **Tariff comparison: consumption summary line** – Shows data period (days), total consumption (kWh), and annualized estimate above the tariff cards.

## 15.0.15 - 2026-04-03
### Fixed
- **Web dashboard: new panes outside #panes container (root cause)** – An extra `</div>` after the export pane closed the `#panes` container prematurely. The 6 new panes ended up as children of `#app` instead of `#panes`, placing them below the scroll area. Removed the stray `</div>`. Verified via Playwright that `pane-battery.parentElement.id` is now `panes` (was `app`).

## 15.0.14 - 2026-04-03
### Fixed
- **Web dashboard: CSS fixes for content alignment** – Changed `#panes` from `flex: 1` to `flex: 1 1 0%` with `min-height: 0`. Without `min-height: 0`, the flex child's implicit min-height prevented the scroll container from sizing correctly, causing short panes to float in the middle. Reverted the `min-height: calc(100vh - 180px)` on `.pane.active` which made the problem worse.
- **Goals badges: uneven grid alignment** – Changed badge grid from `auto-fill, minmax(64px, 1fr)` to fixed `repeat(5, 1fr)`. Badge names now use `text-overflow: ellipsis` and `white-space: nowrap` so long names like "Energiesparer" don't push other badges aside. Reduced emoji size to 20px and text to 8px for uniform appearance.

## 15.0.13 - 2026-04-03
### Fixed
- **Web dashboard: pane content pushed to bottom** – Root cause found via Playwright DOM inspection: `.pane.active` with short content caused the `#panes` flex container to collapse, and `padding-bottom: 120px` pushed the content to the very bottom. Fix: added `min-height: calc(100vh - 180px)` to `.pane.active` so every pane fills the visible area and content starts at the top. This affects ALL panes, not just the new ones.

## 15.0.12 - 2026-04-03
### Fixed
- **Web dashboard: complete rebuild of new tabs** – Removed all old pane HTML and JS. Rebuilt from scratch following the exact pattern of the working Costs/Forecast/Standby tabs:
  - Each pane is `<div id="pane-xxx" class="pane"><div id="xxx-content"><p class="loading-msg">Lade…</p></div></div>` (single content container)
  - Each JS function uses `async function loadXxx()` → `renderXxx(data, el)` pattern
  - All content rendered via `el.innerHTML = html` into one container
  - Uses `metricCardHtml()`, `.card`, `.card-title`, `.metric-grid`, `.card-grid` CSS classes
  - Loading placeholder shown while fetching, error message on failure

## 15.0.10 - 2026-04-03
### Fixed
- **Web dashboard: scroll to top on tab switch**

## 15.0.9 - 2026-04-03
### Fixed
- **Web dashboard: content starts at top** – Removed extra padding and headers from new panes for compact mobile layout.

## 15.0.8 - 2026-04-03
### Fixed
- **Web dashboard: responsive scaling** – Redesigned all 6 new web panes for mobile:
  - Panes have proper `padding:12px` and section titles matching existing tabs
  - Tariff comparison: replaced 7-column table with mobile-friendly card layout (name, price, savings per row)
  - EV log: reduced table to 5 columns (removed redundant end time/peak), smaller font size (12px)
  - Battery: smaller metric cards (18px values instead of 28px), German mode labels
  - Goals: badges use CSS Grid `auto-fill` instead of fixed 80px widths
  - All metric values scaled down from 24-28px to 18px for mobile readability
  - Added template variable titles (i18n) for each pane header

## 15.0.7 - 2026-04-03
### Fixed
- **Web dashboard: panes inside container** – Moved 6 new pane divs inside `#panes` container.

## 15.0.6 - 2026-04-03
### Fixed
- **Web dashboard: new tabs JS bugs** – (1) IIFE wrapper removed. (2) Undefined `qs()` calls removed.

## 15.0.5 - 2026-04-03
### Fixed
- **Web dashboard: new tabs empty** – Action handlers for the 6 new features were inserted *after* the `raise ValueError("Unknown action")` line in `_web_action_dispatch`, so they were never reached. Moved all handlers *before* the raise. Removed duplicate handlers.

## 15.0.3 - 2026-04-03
### Fixed
- **Native tab row hidden** – The built-in ttk.Notebook tab row was still visible on macOS despite the Tabless style. Now clipped off-screen via `place(y=-30)` so only the custom scrollable button bar is visible.

## 15.0.2 - 2026-04-03
### Fixed
- **Scrollable tab bar** – Replaced the built-in ttk.Notebook tab row (which truncates labels on small screens) with a custom horizontally-scrollable button row. Every tab label is always fully readable regardless of window size. Mousewheel scrolls the tab bar. Active tab is visually highlighted.
- **New tabs fill window width** – All 6 new tabs (Smart Plan, EV Log, Tariff, Battery, Advisor, Goals) now stretch their content to fill the full window width instead of being stuck at a narrow default.

## 15.0.0 - 2026-04-03
### Added
- **Smart Scheduling** – New tab that finds the cheapest time block from spot market prices for running large appliances (washer, dryer, dishwasher). Configurable duration (0.5–12h), shows average price and savings vs. daily average. Optional auto-scheduling via Shelly RPC.
- **PV Surplus Control** – Automatic relay switching based on solar excess power. State machine with configurable thresholds (on/off), debounce timer, and priority-ordered consumer list. When surplus exceeds threshold, switches on consumers (boiler, wallbox); switches off when surplus drops.
- **EV Charging Log** – Automatic detection of electric vehicle charging sessions from wallbox power patterns. Logs each session with start/end time, energy (kWh), peak power, duration, and cost. Monthly summary with total sessions, kWh, and cost.
- **Tariff Comparison** – Compare actual consumption costs across 8 pre-defined German electricity tariffs (Stadtwerke, Tibber, 1Komma5°, Ostrom, E.ON, Vattenfall, EnBW, HT/NT). Simulates fixed, time-of-use, and dynamic spot tariffs. Shows annual savings potential.
- **Battery Storage Monitoring** – Track battery state of charge (SOC), charge/discharge cycles, round-trip efficiency, and power flow. SOC timeline chart, cycle detection, and optimal charging time recommendations based on spot prices.
- **InfluxDB Export** – Push energy metrics to InfluxDB v1.x or v2.x via HTTP line protocol. Configurable push interval, measurement name, and authentication. Supports all device metrics (power, voltage, current, energy).
- **Prometheus Metrics** – Expose `/metrics` endpoint in Prometheus text exposition format. Gauges for power (W), voltage (V), current (A), frequency (Hz) per device and phase. Ready for Grafana dashboards.
- **REST API v1** – Formalized API with `/api/v1/devices`, `/api/v1/devices/{key}/samples`, `/api/v1/devices/{key}/hourly`, `/api/v1/costs`, `/api/v1/spot_prices`, `/api/v1/co2`, `/api/v1/status`, and `/api/v1/openapi.json`. Bearer token authentication, CORS headers, rate limiting.
- **AI Energy Advisor** – Personalized energy-saving tips from rule-based analysis of standby consumption, spot price spreads, consumption trends, and weather data. Optional LLM enrichment via Ollama (local), OpenAI, or Anthropic API for natural language summaries.
- **Gamification** – Weekly and monthly consumption goals with auto-calculated targets (90% of previous period). 10 badges (Energy Saver, Standby Killer, Solar Champion, 7/30-day Streak, Night Saver, Peak Avoider, and more). Streak tracking with progress visualization.
- **Multi-Location Support** – Manage multiple sites (home, office, vacation home) with separate device sets and optional separate databases. Location switcher in desktop UI and web dashboard. Aggregate view across all locations.
- **Desktop Dark/Light Mode** – Theme toggle in Settings (Auto/Light/Dark). Affects all matplotlib plots, treeviews, and UI elements. Auto mode detects macOS system appearance.
- **Web Language Selector** – Language switcher in web dashboard settings modal. Supports all 9 languages (DE, EN, ES, FR, PT, IT, PL, CS, RU).

### Changed
- **Version bump to 15.0.0** – Major release with 12 new features.
- **6 new desktop tabs** – Smart Plan, E-Auto, Tariff Compare, Battery, AI Advisor, Goals.
- **6 new web dashboard panes** – Matching web UI for all new features.
- **4 new database tables** – `ev_charging_sessions`, `battery_state`, `badges`, `goals`.
- **11 new config sections** – SmartScheduleConfig, PvSurplusConfig, EvChargingConfig, TariffCompareConfig, BatteryConfig, InfluxDBConfig, PrometheusConfig, ApiConfig, AdvisorConfig, GamificationConfig, MultiLocationConfig.
- **~320 new i18n keys** per language across all 9 supported languages.

## 14.3.4 - 2026-04-03
### Fixed
- **Widget power doubled** – Widget API summed power from all devices instead of only the selected widget devices. Now correctly filters by `widget_devices` config.
- **Widget charts full width** – Spot and CO₂ charts now span the full widget width with `applyFittingContentMode` and fixed height constraints, instead of being squeezed into a small right column. Medium widget restructured: metrics left/right at top, charts below.

### Added
- **Widget CO₂ chart** – iOS Scriptable widget now shows live CO₂ grid intensity (g/kWh) with color-coded bars (green/yellow/orange/red based on thresholds). Small widget shows current value, medium widget shows value + mini chart, large widget shows prominent value + full chart with green/dirty threshold lines. Tap-to-refresh detail view also includes CO₂ intensity.

## 14.3.1 - 2026-04-02
### Added
- **Widget tap-to-refresh** – Tapping the iOS widget opens Scriptable with a live detail view: current power, today/month/projection costs, spot price comparison, per-device breakdown, and a "Dashboard öffnen" button that opens Safari to the full web dashboard.

## 14.3.0 - 2026-04-02
### Added
- **SSL certificate monitoring** – Daily background check of custom SSL certificate expiry. Status shown in Settings → Web Dashboard with color-coded indicator:
  - ✅ Green: >30 days remaining (shows exact date + days)
  - 🔶 Orange: ≤30 days remaining
  - ⚠️ Red: ≤7 days remaining
- **SSL auto-renewal** – When enabled, automatically runs `certbot renew` and copies renewed certs when expiry is within configured threshold (default: 30 days). Runs in background thread.
- **Settings UI** – Auto-Renew checkbox + configurable renewal threshold (days) in Web Dashboard section. Persisted in config (`live_web_ssl_auto_renew`, `live_web_ssl_renew_days`).

## 14.2.0 - 2026-04-02
### Added
- **Widget device filter** – New `widget_devices` config field (comma-separated device keys) to choose which Shellys appear in the iOS widget. Empty = all 3-phase devices. Configurable in Settings → Web Dashboard.
- **Widget auto-domain** – `widget_domain` auto-detected from SSL certificate CN (e.g. `energie.maro-datacenter.de`). Baked into the Scriptable script download so no manual parameter needed.
- **Widget auto-refresh** – `refreshAfterDate` set to 5 minutes, so iOS refreshes the widget regularly.
- **Per-device data in widget** – Large widget shows per-device breakdown (name, power W, today kWh, today €). API `/api/widget` now includes `devices` array.
- **Widget settings UI** – Domain and device filter fields in Settings → Web Dashboard with helper text showing available device keys.

## 14.1.2 - 2026-04-02
### Fixed
- **Scriptable widget crash: `dc.setAlpha is not a function`** – Scriptable's DrawContext has no `setAlpha()` method. Fixed by passing alpha directly to `new Color(hex, alpha)` constructor. Future/past bar transparency now works correctly.

## 14.1.1 - 2026-04-02
### Fixed
- **SSL settings not persisted** – `live_web_ssl_mode`, `live_web_ssl_cert`, `live_web_ssl_key` were missing from config loader and saver in `config.py`, causing the fields to be dropped on every save/load cycle. Added to both `load_config()` and `save_config()`.

## 14.1.0 - 2026-04-02
### Added
- **SSL/HTTPS settings** – New HTTPS mode selector in Settings → Web Dashboard:
  - **Auto** (default): Self-signed certificate (existing behavior)
  - **Custom**: Use your own certificate files (Let's Encrypt, etc.) with file browser for cert + key paths
  - **Off**: Plain HTTP for LAN-only use without certificate issues
- Settings are persisted in config (`live_web_ssl_mode`, `live_web_ssl_cert`, `live_web_ssl_key`)

### Fixed
- **Scriptable widget protocol** – Now tries both `https://` and `http://` to match server SSL mode

## 14.0.3 - 2026-04-02
### Fixed
- **Scriptable widget: HTTPS support** – Widget tried `http://` but server uses HTTPS with self-signed certificate. Now tries `https` first, falls back to `http`. Better offline error message with WiFi hint.

## 14.0.2 - 2026-04-02
### Fixed
- **Web dashboard crash on startup** – Local variable `html = _render_template(...)` shadowed `import html` module, causing `html.escape()` to fail with `'str' object has no attribute 'escape'`. Renamed to `_rendered_html`. This broke the Plotly plots page device list and caused connection resets.

## 14.0.1 - 2026-04-02
### Fixed
- **Widget API crash** – `/api/widget` route referenced undefined `qs` variable, causing the web server to drop the connection. Now passes empty dict like other API routes.
- **Widget power reading** – Fixed LiveStateStore snapshot access: use correct `power_total_w` key and list-of-dicts format. Added fallback to computed dataframes when live store is unavailable.

## 14.0.0 - 2026-04-02
### Added
- **iOS Widget (Scriptable)** – Real iOS home screen widget for live energy data. Three sizes supported:
  - **Small**: Current power (W), today's consumption + cost, current spot price with delta
  - **Medium**: All of small + month stats + mini spot price chart with color-coded bars
  - **Large**: Full detail with spot chart, metrics grid (today/month/projection), spot cost comparison
- **`/api/widget` endpoint** – Compact JSON API optimized for widget polling: power, consumption, costs, spot price, mini chart data
- **`/widget.js` route** – Downloadable Scriptable script with baked-in server URL
- **Widget setup in web settings** – Step-by-step install instructions with "Copy Script" and "Download .js" buttons, auto-detected server address as widget parameter

## 13.10.1 - 2026-04-02
### Fixed
- **Web costs tab: current price display** – Removed duplicate ⚡ icon, shortened text to prevent line breaks on narrow screens, added `white-space:nowrap`.
- **App costs tab: current price label invisible** – `tk.Label` now uses theme-aware background color (`_get_theme_colors()`) so it blends into the ttk frame on both light and dark themes. Font size increased to 14pt.

## 13.10.0 - 2026-04-02
### Added
- **Current spot price display** – App and web costs tab now show the current kWh spot price prominently above the 24h chart, with color-coded delta vs. fixed tariff (green = cheaper, red = more expensive).
- **Spot prices in notifications** – Telegram summaries and email PDF reports now include dynamic spot cost totals, average spot price, current price, and per-device spot cost comparison vs. fixed tariff.
- **Web costs tab: color-coded price comparison** – Dynamic tariff difference indicators (↑/↓) in device cards are now color-coded: green when spot is cheaper, red when more expensive.

### Fixed
- **App costs tab: opaque tooltip** – Spot chart hover tooltip is now fully opaque (was 95% transparent) and rendered on top with `zorder=99`, so it properly covers chart bars instead of showing through.

## 13.9.27 - 2026-04-02
### Changed
- **Web costs tab: removed CO2 section** – CO2 data (today/week/month/projected kg) removed from device cost cards. CO2 tracking remains available in the dedicated CO2 tab.

## 13.9.26 - 2026-04-02
### Fixed
- **CO2 tab: timer accumulation** – Refresh timer now cancels the previous one before scheduling, preventing exponentially increasing CPU usage over time.
- **CO2 tab: macOS scroll** – Removed broken local mousewheel binding (delta/120=0 on macOS). Global handler covers it.
- **CO2 tab: local timezone** – Heatmap, intensity chart, and all tables now show local time instead of UTC. German users no longer see times off by 1-2 hours.
- **ENTSO-E: sub-hourly averaging** – PT15M generation/load data (4 points per hour) was summed instead of averaged, inflating MW values up to 4x. Now correctly averages sub-hourly values.
- **Web: phase data update** – Phase values updated in the wrong element because `allKvs[1]` index shifted when reactive power/balance elements were added. Now uses stable `#kv-phases-{key}` ID selector.
- **Web: duplicate `read_file_bytes`** – Removed first (dead) definition, kept second with `.csv` support added.
- **Updates: dead code removed** – Orphaned ZIP download block after `_updates_open_release` that always failed with undefined `zip_path`/`rel.zip_url`, showing misleading "unreachable" status.

## 13.9.25 - 2026-04-02
### Fixed
- **Mousewheel scrolling unified** – Removed 4 redundant per-tab mousewheel bindings (costs, settings main, settings devices, groups) that hijacked the global handler and broke scrolling in other tabs. The single smart global handler at app init now handles all tabs correctly, including macOS small-delta support.
- **Frozen dataclass mutation** – Telegram settings from setup wizard were silently lost because `UiConfig` is frozen. Now uses `dataclasses.replace()` to create a new config properly.
- **self.root.after → self.after** – Fixed 5 occurrences where `self.root.after()` raised `AttributeError` because the class IS the root (inherits from Tk). Telegram/webhook/email test callbacks now schedule correctly on the main thread.
- **Live history export** – `self._live_history` was never initialized, causing live tab PNG/PDF export to silently fail. Now initialized as empty dict.
- **Grid overlap in pricing settings** – CO2 presets button and base-fee-VAT checkbox both occupied row 3 column 2. Moved CO2 presets to row 4.
- **Forecast year formula** – Removed double-counted trend term (`slope * 365 * 182.5`) from annual forecast. `avg_daily` already includes the trend.
- **Web: freeze button handler accumulation** – Each tab switch added another click handler. Now removes old handler before adding.
- **Web: dark mode in Sankey/Traffic charts** – Used wrong detection (`body.classList`) instead of `documentElement.dataset.theme`.
- **Web: theme key mismatch** – Live page used `sea_theme`, other pages used `sea_web_theme`. Now unified to `sea_theme`.
- **Web: CSS `--text` variable** – Chart detail legend referenced undefined `--text`, changed to `--fg`.
- **Web: spot chart listener leak** – mousemove/touchmove handlers accumulated on each redraw. Now cleaned up before re-adding.
- **Web: Control page auth tokens** – `qs()` always returned empty string, breaking authenticated downloads. Now returns `window.location.search`.
- **Plots: device tab binding** – Second `NotebookTabChanged` bind replaced the first. Added `add="+"`.
- **Web plots: column name mismatch** – Timeseries data used hardcoded `df["ts"]` instead of checking for `"timestamp"` column.

### Added
- **Spot price settings: "Standard" button** – Resets all dynamic tariff surcharge fields back to defaults.

## 13.9.23 - 2026-04-01
### Fixed
- **Costs tab: spot chart fully locked** – Disconnected all matplotlib interactive events (scroll, button press/release, key press) on the spot chart canvas so the plot can no longer be panned, zoomed, or scrolled within its frame. Mousewheel and Linux scroll buttons (Button-4/5) forwarded to the outer scroll canvas.

## 13.9.22 - 2026-04-01
### Fixed
- **Costs tab: spot chart no longer scrollable** – Disabled matplotlib's built-in scroll-zoom on the spot price chart. Mousewheel now scrolls the page instead of zooming the plot. Chart axis navigation disabled via `set_navigate(False)`.

## 13.9.21 - 2026-04-01
### Fixed
- **Weather tab: fixed plot widths** – Scatter + timeseries use a GridSpec with dedicated colorbar column (width ratio 1:0.04:1). The colorbar now reuses a fixed `cax` instead of `fig.colorbar(ax=...)` which stole space from the scatter plot on every refresh. Both plots stay equal width permanently.
- **Weather tab: colorbar dark mode** – Colorbar tick labels and axis label now use theme foreground color instead of hardcoded black.
- **Costs tab: spot tooltip dark mode** – Tooltip background and text color adapt to the current theme.

## 13.9.19 - 2026-04-01
### Fixed
- **Web spot chart: dark mode** – Chart now properly detects `data-theme="dark"` instead of wrong `classList.contains('dark')`. Background, grid, labels, and tooltip all adapt to the current theme. Theme toggle triggers immediate chart redraw.
- **Dynamic tariff: lower defaults** – Offshore surcharge updated to 2025 value (0.656 ct, was 0.816). Default supplier margin lowered to 1.50 ct (was 2.50) to better match Tibber/similar providers. Total default markup now ~15.3 ct/kWh (was ~16.4).
- **Traffic sparkline removed** – Removed jittery sparkline canvas. Traffic table rows updated in-place. Columns stretch properly.

## 13.9.15 - 2026-04-01
### Fixed
- **Traffic frame width** – Reduced Treeview column initial widths to minimal values so the tree doesn't force the parent frame wider than the window. Hidden #0 column set to 0 width.

## 13.9.14 - 2026-04-01
### Fixed
- **Traffic table: columns stretch to fill width** – All Treeview columns now use `stretch=True` with proper `minwidth` values, so they distribute evenly across the available frame width.

## 13.9.13 - 2026-04-01
### Fixed
- **Traffic sparkline: no more jitter** – Fixed bar count derived from window width (3px each). Bars are right-aligned to current second, empty seconds filled with zeros. No more changing bar count or layout shifts.

## 13.9.12 - 2026-04-01
### Fixed
- **Traffic sparkline fits window** – Samples aggregated to 1-second buckets, bar count limited to window width (3px min per bar). Only the most recent N seconds shown, all bars equal width.

## 13.9.11 - 2026-04-01
### Improved
- **Traffic chart: compact sparkline bars** – Replaced tall matplotlib plot with a thin 36px tkinter Canvas sparkline. Blue bars = download, orange stacked = upload. Fits neatly below the traffic table.

## 13.9.10 - 2026-04-01
### Changed
- **Network traffic: 0.5s sampling** – Rate history sampled every 500ms (was 3s). Desktop UI updates at 0.5s, web polls at 500ms. Buffer holds 600 samples (~5 min).

## 13.9.9 - 2026-04-01
### Changed
- **Network traffic chart: 5 min window** – Live rate chart now shows last 5 minutes instead of 1 hour for a more responsive view.

## 13.9.8 - 2026-04-01
### Fixed
- **Dyn. Preis tab not detected** – `_active_metric_key()` now recognizes the dynamic price tab so the shared controls correctly apply to it.

### Improved
- **Plots controls centered above tabs** – Dropdown + Letzte N spinbox now sit centered above the metric sub-tabs (kWh/V/A/W/…), not below. Removed header, reload, debug, and date range row.

## 13.9.7 - 2026-04-01
### Improved
- **Plots tab: minimal clean layout** – Removed header label, reload button, debug checkbox, date range row (Von/Bis/Anwenden/Zurücksetzen). Single control bar (dropdown + Letzte N) now sits at the bottom below all tabs. Controls apply instantly on tab switch, dropdown change, spinbox change, focus out, or click away.

## 13.9.6 - 2026-04-01
### Improved
- **Plots tab: single shared control bar** – One dropdown (Alle/Stunden/Tage/Wochen/Monate) + one "Letzte" spinbox above all sub-tabs. Controls apply to whichever metric tab is active. Instant refresh on dropdown change, spinbox increment/decrement, Enter key, focus out, or mouse click away. No per-tab duplicate controls.

## 13.9.5 - 2026-04-01
### Improved
- **Plots tab: unified controls** – All sub-tabs (kWh, V, A, W, VAR, cos φ, Hz, CO₂, Dyn. Preis) now use the same compact control: one dropdown (Alle/Stunden/Tage/Wochen/Monate) + "Letzte" spinbox. No more duplicate dropdowns or Apply buttons – changes apply immediately on selection.
- **Live traffic rate chart** – Desktop and web now show a real-time line chart of download (blue) and upload (orange) rates over the last hour, replacing the static category bar chart.

## 13.9.4 - 2026-04-01
### Added
- **Web Sankey: canvas-based energy flow diagram** – The web dashboard now renders a proper Sankey-style energy flow diagram with bezier flow bands matching the desktop version (Sources → House → Consumers columns, proportional band widths, color-coded flows).
- **Network traffic chart** – Desktop and web both show a small horizontal bar chart visualizing received bytes per category alongside the traffic table.

### Improved
- **Plots tab: dropdowns replace buttons** – All fixed granularity buttons (all/hours/days/weeks/months) in kWh, CO₂, and Dyn. Preis tabs replaced with dropdown selectors. WVA preset buttons (5m/15m/1h/6h/24h/7d/30d) replaced with dropdown. Cleaner, more consistent UI.
- **Network traffic: all categories visible** – Treeview height increased to 8 rows with scrollbar. Added missing "⚡ Spot Prices" category to web traffic display.

## 13.9.3 - 2026-04-01
### Improved
- **Energy flow: proper Sankey bands** – Replaced ugly arrow patches with smooth filled bezier-curve bands (S-shaped ribbons). Each flow is a cubic bezier polygon connecting source to house to consumer with proportional width. Bands stack correctly on the house node edges. No more arrow artifacts.

## 13.9.2 - 2026-04-01
### Improved
- **Energy flow diagram: complete visual overhaul** – Compact three-column layout (Sources → House → Consumers) with no wasted space. Fixed-height node boxes vertically auto-distributed. Thick semi-transparent flow bands with proportional width. Grid=red, PV=orange, devices=distinct color palette. White text on dark backgrounds for readability. Removed broken emoji rendering. Dark mode aware.

## 13.9.1 - 2026-04-01
### Fixed
- **Web costs tab: no data displayed** – Fixed `_spot_cfg` referenced before definition in the costs API handler. Variable is now defined at the top of the handler block.
- **Traffic monitor: missing response bytes** – The monkey-patch now force-reads `resp.content` before measuring, so API response sizes are correctly tracked. Spot price API calls now appear as "⚡ Spot Prices" category.

### Improved
- **Web costs tab: regrouped layout** – Per-device cards now show: spot price 24h chart at top → fixed tariff costs (blue label) → dynamic tariff costs (orange) → CO₂ section. Clearer visual hierarchy.
- **Energy flow diagram: much prettier** – Desktop Sankey completely redesigned with rounded node boxes, shadow effects, proportional flow widths, bezier curve arrows with arrowheads, and percentage labels. Source/consumer labels with white text on colored backgrounds.
- **Weather tab: 7-day rolling window** – Time series chart now shows last 7 days instead of 30, making hourly patterns much clearer.

## 13.9.0 - 2026-04-01
### Added
- **Tariff type selector: Fixed or Dynamic** – New radio button in Settings → Preise & Tarife to choose between fixed price tariff and dynamic spot market tariff as the PRIMARY billing method. When "Dynamic" is selected, ALL cost calculations throughout the entire app use spot market prices (EPEX Spot + surcharges + VAT) instead of the configured fixed price.
- **Dynamic tariff as primary affects everything** – Costs tab, live cost_today, web dashboard costs, standby analysis, tenant billing, forecast projections, solar savings, PDF exports, and invoices all automatically switch to dynamic pricing when selected. The comparison line flips: shows what the fixed tariff would have cost instead.

### Changed
- Central `_get_effective_unit_price()` and `_calc_cost_for_range()` helpers dispatch to the correct pricing method based on tariff type. All downstream UI and service code uses these helpers.
- Invoice generation uses average spot price for the billing period when dynamic tariff is active.

## 13.8.4 - 2026-04-01
### Added
- **Spot price tooltip on hover** – Mouse-over on the 24h spot price chart (desktop + web) shows a detailed tooltip with timestamp, raw spot price (ct/kWh), and total price including surcharges. Web version works with touch as well.
- **Detailed markup breakdown in settings** – The single "16 ct/kWh" markup field is replaced by 7 individually configurable components: Netzentgelte (8.50), Stromsteuer (2.05), Konzessionsabgabe (1.66), KWK-Aufschlag (0.277), §19 StromNEV (0.643), Offshore-Umlage (0.816), Anbieter-Marge (2.50). Live sum display updates as you type. All values are 2025 defaults for Germany. Legacy single-value configs are auto-migrated.
- **Spot price traffic category** – Energy-Charts and aWATTar API calls now appear as "⚡ Spot Prices" in the network traffic monitor instead of "Other".

### Improved
- All spot price calculations now use the sum of breakdown components instead of the legacy single markup value.

## 13.8.3 - 2026-04-01
### Added
- **Costs tab: 24h spot market price chart** – Rolling bar chart showing spot electricity prices with colour-coded bars (green = cheap, red = expensive relative to fixed tariff). Fixed-price reference line and average price display. Available in both desktop (matplotlib) and web dashboard (Canvas).

### Fixed
- **Web dashboard: spot price HTML rendering** – Dynamic tariff comparison in costs tab no longer shows raw HTML tags; now correctly displays plain-text delta values.
- **Dark mode theme switching refreshes all tabs** – Changing the plot theme (day/night/auto) in Settings now immediately refreshes Weather, Forecast, CO₂, Standby, Solar, Sankey, Tenant, Plots, and Costs tabs. Previously only the Live tab was updated.
- **Weather tab: empty scatter plot on repeated refresh** – Fixed twin-axis accumulation that caused the temperature vs. consumption chart to appear empty after multiple refreshes. Old twin axes are now properly cleaned up.
- **Weather tab: colorbar removal error** – Protected against matplotlib colorbar removal failures that could break the scatter plot.

### Improved
- **README.md updated** – Added documentation for dynamic spot prices, 24h price chart, "vs. Dynamic Tariff" comparison, tariff schedule, and theme switching improvements. Version updated to v13.8.3.

## 13.8.2 - 2026-04-01
### Added
- **Plots tab: "Dyn. Preis" sub-tab** – New sub-tab after CO₂ showing a grouped bar chart comparing fixed tariff costs (blue) vs dynamic spot market costs (orange) for the selected device and time range. Supports hourly/daily/weekly/monthly granularity with custom range controls. Displays total costs, delta, and per-bar annotations. Full dark mode support and all 9 languages.

## 13.8.1 - 2026-04-01
### Improved
- **Spot price service: comprehensive sync logging** – Every step of the spot price import is now logged to the Sync tab: config check, oldest measurement detection, existing DB state, gap analysis, per-chunk fetch progress with API source, DB write counts, and error details. The spot price service is also triggered immediately on app startup alongside CO2 data.

## 13.8.0 - 2026-04-01
### Added
- **Dynamic spot market electricity prices** – New feature that imports EPEX Spot day-ahead prices from free public APIs (Energy-Charts with 15-min resolution from Oct 2025, aWATTar hourly fallback). Background service automatically backfills prices from the first measurement timestamp.
- **Costs tab: dynamic tariff comparison** – Every cost card (today/week/month/year) now shows what the same consumption would cost with a dynamic tariff (spot price + configurable markup + VAT). Displayed in orange alongside the fixed tariff cost, both in desktop and web dashboard.
- **Compare tab: fixed vs. dynamic tariff mode** – New "vs. Dynamic Tariff" toggle in the comparison tab lets users directly compare their fixed tariff costs against dynamic spot market prices for any period. Available in both desktop and web dashboard.
- **Spot price settings** – Full configuration UI under Settings: enable/disable, choose API source (Energy-Charts or aWATTar), set bidding zone, configure markup (default 16 ct/kWh net for grid fees, taxes, supplier margin), VAT toggle, fetch interval.
- **New `spot_prices` database table** – Stores fetched spot prices with support for both 15-min and hourly resolution. Efficient hourly join with `hourly_energy` table for cost calculations.
- **i18n: all 9 languages** – Full translation coverage for spot price feature (de, en, es, fr, pt, it, pl, cs, ru).

## 13.7.2 - 2026-04-01
### Fixed
- **Anomaly detection settings now persist across app restarts** – The `_save_settings()` method was reconstructing `AppConfig` without the `anomaly`, `groups`, `demo`, and `schedules` fields, causing them to reset to defaults whenever settings were saved. All existing config fields are now preserved.

## 13.7.1 - 2026-03-31
### Fixed
- **Settings "Save Config" button no longer cut off on small monitors** – Bottom action bar (Save, Reload, Screenshot) is now packed before the notebook widget, ensuring it always stays visible regardless of screen size.
- **Comprehensive i18n audit: ~50 hardcoded strings replaced with translations** – Fixed German-only messagebox titles/messages across Telegram, Webhook, Email, Tenant, CO₂, NILM, and Sync. All user-facing text now uses the translation system (`self.t()`), supporting all 9 configured languages. Web dashboard JS error strings also translated via template placeholders.

## 13.7.0 - 2026-03-31
### Added
- **Dark mode for Solar, Energy Flow (Sankey), and Tenant tabs** – All three tabs now fully respect the plot theme setting. Amortization chart, energy flow diagram, and tenant pie/bar charts all adapt to dark mode with proper backgrounds, text colors, and themed data series.

### Improved
- **Plots tab: better layout scaling** – Chart figures now use larger height (5.0 vs 3.6) at 96 dpi for better window fill. Bottom margins recalculated to prevent X-axis label clipping on various screen sizes. Less wasted black space below charts.

### Fixed
- **Solar tab charts no longer scroll out of view** – Mousewheel scrolling restricted to the content area (same fix as CO₂ tab in v13.5.2).

## 13.6.0 - 2026-03-31
### Improved
- **Dark mode support for all desktop chart tabs** – Weather, CO₂, Standby, and Forecast tabs now fully respect the plot theme setting (auto/day/night). All matplotlib figures get themed backgrounds, axis colors, grid lines, tick labels, and legend frames. Data series use theme-aware colors (brighter in dark mode). New `_get_theme_colors()` helper provides consistent color palettes across all tabs.

## 13.5.5 - 2026-03-31
### Improved
- **Screenshots now capture full scrollable content** – For tabs with scrollable areas (e.g., main settings, CO₂ tab), the screenshot function now automatically scrolls through the content and saves multiple parts (`_part1.png`, `_part2.png`, ...) so nothing is missed. Scrollable canvases are auto-detected in the widget tree.

## 13.5.4 - 2026-03-31
### Improved
- **Screenshots now include plots sub-tabs** – The screenshot function now also iterates through all metric sub-tabs (kWh, V, A, W, VAR, cosφ, Hz, CO₂) and their device sub-tabs within the Plots tab, capturing each combination (e.g. `plots_kWh_Shelly3EM.png`).

## 13.5.3 - 2026-03-31
### Added
- **"Screenshots aller Tabs" button in settings** – Takes a screenshot of every main app tab and every settings sub-tab automatically. Saves numbered PNGs to `screenshots/YYYYMMDD_HHMMSS/` in the project directory. Useful for documentation or bug reports.

## 13.5.2 - 2026-03-31
### Improved
- **CO₂ chart color gradient now includes yellow/orange** – The intensity line chart and heatmap strip now use a 4-stop gradient (green → yellow-green → orange → red) instead of the previous direct green-to-red ramp. Both charts share the same color function for consistency.
### Fixed
- **CO₂ chart no longer scrolls away** – The mousewheel scrolling in the CO₂ tab was globally bound, causing the intensity chart to scroll out of view when using the mouse wheel over it. Now only scrolls when the mouse is over the scrollable content area, not over embedded charts.

## 13.5.1 - 2026-03-31
### Improved
- **Desktop CO₂ intensity chart now colorful like the web version** – The 24h CO₂ intensity line chart in the desktop app now uses the same green→yellow→red color gradient as the web dashboard. Each line segment and fill area is colored based on intensity relative to the green/dirty thresholds, replacing the previous flat blue.

## 13.5.0 - 2026-03-31
### Added
- **Shelly firmware update check & OTA in health check** – The health check now queries each device for available firmware updates. A new "Update" column shows ⬆ with the new version if an update is available, or ✓ if current. Two new buttons: "Update" (selected device) and "Alle updaten" (all devices with pending updates). Supports both Gen2+ (RPC `Shelly.CheckForUpdate` / `Shelly.Update`) and Gen1 (`/ota?update=true`).

## 13.4.0 - 2026-03-31
### Added
- **Tariff schedule – enter future price changes in advance** – New "Tarifzeitplan" section in desktop settings allows adding future electricity prices and base fees with a start date (e.g., "from 2026-10-01: 0.44 €/kWh"). The correct price is automatically used for cost calculations based on the current date. Tariff schedule is shown in the web dashboard costs tab with active/upcoming indicators. All cost consumers (costs tab, forecast, standby, live cards) now use date-aware pricing.

## 13.3.5 - 2026-03-31
### Fixed
- **Standby tab now actually shows data** – The standby analysis failed silently when `avg_power_w` was NULL in the database (common for older data or devices that only log kWh). Now falls back to calculating average power from kWh values (1 kWh/h = 1000W). Also improved sample synthesis to work when only `energy_kwh` is available without `total_power`.

## 13.3.4 - 2026-03-31
### Fixed
- **Desktop weather scatter plot no longer accumulates colorbars** – The hour-of-day colorbar legend was added on every tab refresh without removing the previous one, causing them to stack up. Now properly removed before re-drawing.

## 13.3.3 - 2026-03-31
### Fixed
- **Weather timeline X-axis labels no longer overlap on mobile** – Reduced max labels from 8 to 4 on narrow screens (<400px) and shortened format to just `dd.mm` (without hour suffix) to prevent text overlap on phone browsers.

## 13.3.2 - 2026-03-31
### Fixed
- **Web dashboard surrogate encoding fix (part 2)** – Three more emoji surrogate pairs in the weather card labels (🌡️💧💨) were still using JS-style `\ud83c\udf21` notation which Python cannot encode as UTF-8. Replaced with proper `\U0001fXXX` codepoints.

## 13.3.1 - 2026-03-31
### Fixed
- **Web dashboard startup crash fixed** – Unicode surrogate pairs (`\ud83c\udf19`) in the weather scatter legend could not be encoded as UTF-8, preventing the dashboard from starting. Replaced with proper Unicode codepoints.

## 13.3.0 - 2026-03-31
### Added
- **Weather tab on web dashboard** – New "Wetter" / "Weather" tab between Solar and Vergleich in the web dashboard. Shows current weather cards (temperature, humidity, wind, cloud cover), Pearson r / HDD / CDD correlation metrics, an interactive scatter plot (temperature vs. consumption, colored by hour-of-day), and a dual-axis timeline chart (bars = kWh, line = °C). Data served via new `/api/weather_correlation` endpoint.

### Improved
- **Desktop weather tab charts reworked** – Scatter plot dots now colored by hour-of-day (twilight palette) instead of redundant temperature color. Time series chart now uses proper date labels (dd.mm.) instead of numeric hour indices, and has a clearer title ("Zeitverlauf: Verbrauch & Temperatur").

## 13.2.3 - 2026-03-31
### Fixed
- **NILM ML web status now actually works** – The `/api/nilm_status` endpoint referenced `dashboard._state_store` but the dashboard stores it as `dashboard.store`. This caused the API to always return `cluster_count=0` and `transition_count=0`, keeping the badge permanently stuck on "warte auf Daten…". Also includes the v13.2.2 startup push fix (loaded clusters are pushed to the web store immediately at startup).

## 13.2.1 - 2026-03-30
### Fixed
- **NILM ML now truly persists across restarts** – Three issues fixed: (1) Added `flush()` method that saves immediately regardless of transition count. (2) Auto-save every 10 new transitions to avoid data loss between 5-minute cluster cycles. (3) App close handler (`WM_DELETE_WINDOW`) now flushes all NILM learners to disk before exiting. Previously, transitions collected between the last `cluster()` call and app exit were lost.
- **Mousewheel/trackpad scrolling works everywhere in desktop app** – Added global `<MouseWheel>` event binding that routes scroll events to the scrollable widget under the cursor (TreeView, Listbox, Text, Canvas). Previously, TreeView and Listbox widgets only responded when clicking directly on the scrollbar. Works on macOS, Windows, and Linux.

## 13.2.0 - 2026-03-30
### Added
- **EnBW/SMATRICS as 4th EV data source with REAL-TIME status** – Reverse-engineered the SMATRICS POI bounding-box API used by the EnBW mobility+ map. Returns per-connector real-time status (AVAILABLE/OCCUPIED/OUT_OF_SERVICE) with `last_updated` timestamps. This is the first source with actual live occupancy data.
- **Smart deduplication by detail score** – When multiple sources report the same station, the one with richer data wins (real-time status > static data, more connectors > fewer). EnBW data with live status always takes priority over OCM/BNA/OSM static entries.
- **Power filter** – Filter by minimum charging power (≥11 kW, ≥22 kW, ≥50 kW, ≥150 kW)
- **Plug type filter** – Filter by connector type (Typ 2, CCS, CHAdeMO, Schuko)
- **EnBW stations marked with ⚡** on brick tiles to indicate real-time data availability
- **German connector type names** – OCPI connector codes (IEC_62196_T2, etc.) mapped to readable names (Typ 2, CCS, CHAdeMO)

## 13.1.1 - 2026-03-30
### Added
- **EV Charger: multi-source parallel fetch** – All three data sources are now queried simultaneously and results merged with proximity-based deduplication (50m threshold):
  - **OpenChargeMap** – international, with status data (API key)
  - **Bundesnetzagentur** – official German registry, 184k+ stations
  - **OpenStreetMap/Overpass** – community data, good coverage, no key needed
  - Sources queried in parallel (ThreadPoolExecutor) for fast response
  - Deduplication keeps the entry with richer data (OCM > BNA > OSM)
  - Station count and active sources shown above the grid ("42 Stationen · OpenChargeMap + Bundesnetzagentur + OpenStreetMap")
  - max_results increased to 100

## 13.1.0 - 2026-03-30
### Added
- **Auto-HTTPS for web dashboard** – The web server now automatically generates a self-signed TLS certificate on first start (via `openssl` CLI, available on macOS/Linux/Raspberry Pi) and serves over HTTPS. This enables the browser Geolocation API for the EV Charger tab on smartphones over LAN. The certificate is stored in `data/runtime/ssl/` and valid for 10 years. Falls back to HTTP gracefully if `openssl` is not available. Users see a one-time browser warning to accept the self-signed certificate, after which GPS works seamlessly.

## 13.0.5 - 2026-03-30
### Changed
- **EV Charger: larger text on brick tiles** – Distance and availability count (e.g. "2/4 frei", "350 m") now display at 14px bold instead of 10px, making them much easier to read on mobile.

## 13.0.4 - 2026-03-30
### Fixed
- **EV Charger: complete rewrite of status detection** – Switched OCM from compact to full mode for richer status data. Improved StatusTypeID mapping: "Operational" (ID 50) now shows as green/free instead of gray/unknown, "Partly Operational" (ID 75) as yellow. Handles both compact (flat `StatusTypeID`) and full (`StatusType.ID`) response formats. Fills missing connectors from `NumberOfPoints`. Added Bundesnetzagentur (ladestationen.api.bund.dev) as fallback data source for German stations when OCM fails or returns empty.

## 13.0.3 - 2026-03-30
### Fixed
- **EV Charger: connector status now inherits from station** – OpenChargeMap often provides status only at the station level, not per connector. Connectors with "unknown" status now inherit the station's status. The station's `DateLastStatusUpdate` is shown as "since" timestamp on each connector brick. Station bricks with unknown status now show connector count instead of "unknown". Occupied stations show "0/N frei" format.

## 13.0.2 - 2026-03-30
### Fixed
- **EV Charger: 403 Forbidden from OpenChargeMap** – OpenChargeMap now requires a free API key. Added an API key input field that appears automatically when a 403 error occurs. The key is stored in localStorage so it only needs to be entered once. Users can get a free key at openchargemap.org/site/develop/api.

## 13.0.1 - 2026-03-30
### Added
- **City search for EV Charger tab** – Since GPS requires HTTPS and doesn't work on LAN, the EV tab now has a text input field for entering a city name. Uses Nominatim (OpenStreetMap) for free geocoding. Priority: city input → GPS → cached position. Also added 10km radius option.

## 13.0.0 - 2026-03-30
### Added
- **EV Charger tab in web dashboard** – New "Ladesäulen" tab shows nearby EV charging stations as colored brick tiles using the smartphone's GPS position. Powered by OpenChargeMap (free, no API key required).
  - **Colored bricks**: Green (≥1 connector free), Yellow (all occupied), Red (defective/unavailable), Gray (status unknown)
  - **Sorted by distance**: Top-left = nearest, bottom-right = furthest
  - **Configurable radius**: 100m, 500m, 1km, 2km, 5km via dropdown
  - **Detail modal**: Tap a brick to see station address, distance, and individual connector bricks with type (CCS, Type 2, etc.), power (kW), and status
  - **Server-side proxy**: Backend fetches from OpenChargeMap with 120s cache to avoid rate limits and CORS issues
  - **i18n**: German and English translations included

## 12.9.18 - 2026-03-30
### Fixed
- **Anomaly Telegram/notification spam on app restart** – The set of already-notified anomaly event IDs (`_anomaly_notified_ids`) was only kept in memory. After every app restart it was empty, so all anomalies from the last 7 days were re-detected and re-notified. The notified IDs are now persisted to `data/runtime/anomaly_notified_ids.json` and loaded on startup, preventing duplicate notifications across restarts and updates.

## 12.9.17 - 2026-03-30
### Fixed
- **Switch status wrong on first web Live load** – The `LivePoint` dataclass lacked a `raw` field, so the Shelly device's raw status data (containing `output`/`ison` for switch state) was never passed through to the web API snapshot. Switch devices always showed "Aus" on first load until a toggle was performed. Now `LivePoint` carries the `raw` dict and the switch state is correctly extracted from live polling data from the first sample onward.

## 12.9.16 - 2026-03-30
### Fixed
- **NILM ML transitions now persist across app restarts** – Previously only the computed clusters were saved to disk, but the raw transition data (power step changes) was lost on restart. After collecting 10+ new transitions, the old clusters were overwritten with only the new data, losing all prior learning. Now both clusters AND transitions are saved/loaded from the JSON file, so the ML model continuously improves over time instead of starting from scratch after each restart or update.

## 12.9.15 - 2026-03-30
### Added
- **Switch control in web Live tab** – Switchable Shelly devices (kind=switch) now show their current on/off status with a colored badge and a Toggle button directly on the device card. Tapping Toggle sends a `toggle_switch` API call and immediately updates the status. The switch state is extracted from the live polling data and updates in real-time alongside power readings.

## 12.9.14 - 2026-03-30
### Fixed
- **Scroll areas inconsistent in web dashboard** – Several scrollable containers (`#panes`, `.hm-table-wrap`, `.modal-panel`, `.joblog`) were missing `-webkit-overflow-scrolling: touch`, causing trackpad/touch scrolling to only work when directly on the scrollbar (especially on Safari/iOS). All scroll containers now have consistent momentum scrolling enabled.

## 12.9.13 - 2026-03-30
### Fixed
- **CO₂ fetch crash: `Co2FetchService has no attribute bidding_zone`** – The fuel mix solar estimation called `self.bidding_zone` on the `Co2FetchService` object, but that attribute only exists on `EntsoeClient`. Changed both occurrences (lines 1160/1308) to use the local `zone` variable instead. This caused intermittent chunk failures during CO₂ data import.

## 12.9.12 - 2026-03-30
### Fixed
- **Anomaly type not translated in web dashboard** – Raw keys like "unusual_daily" were shown instead of translated labels (e.g. "Ungewöhnl. Tagesverbrauch" in German). Now uses the i18n `t()` function with `anomaly.type.*` keys.
- **ML appliance chips hidden in Live tab** – The NILM appliance recognition badges (e.g. "❄️ Fridge", "☕ Kettle") were only visible inside the collapsed detail section. Moved them outside the expandable area so they're always visible on each device card. Chips also update on every live refresh cycle.
- **Standby detection more robust** – Improved the raw-sample fallback: handles both datetime and integer timestamps, estimates kWh from average power when `energy_kwh` is unavailable, and adds debug logging. When no standby data is found, the web UI now shows per-device diagnostic info (hourly/sample row counts) to help identify why.

### Added
- **CO₂ intensity timestamp in web dashboard** – The live CO₂ hero card now shows the date and time of the displayed intensity value (e.g. "DE_LU · ENTSO-E · 30.03.2026 14:00"), making it easy to verify how fresh the data is.

## 12.9.10 - 2026-03-29
### Fixed
- **Heatmap month labels drifting right of grid** – Each month label span was sized at `calCellSize + gap` while the parent flex container also applied `gap: 2px`, double-counting the spacing. Over 53 weeks this caused ~106px of cumulative drift, pushing "Dez" past the actual grid tiles. Fixed by setting label span width to `calCellSize` only, matching the grid column width exactly.
- **Heatmap calendar not filling to end of December** – The week generation loop could stop one week short. Replaced with an explicit Dec 31 coverage check.
- **Heatmap columns compressed in portrait mode** – Week columns and label spans now have `flex-shrink: 0` so the grid scrolls horizontally instead of compressing.

## 12.9.6 - 2026-03-29
### Fixed
- **Web dashboard completely blank (all browsers)** – The CO₂ range selector buttons generated `loadCo2(\' + r + \')` in the JS template. The `_render_template` function converts `{{`→`{` and `}}`→`}`, but also leaves `\'` as a bare `'`, producing `loadCo2('' + r + '')` — a **JS SyntaxError**. Since all JS is in one `<script>` block, the entire page's JavaScript failed to parse and nothing rendered. Fixed by using `\\u0027` (JS unicode escape for single quote) instead of `\'`.
- **UTF-16 surrogate cleanup** – Added `ensure_ascii=False` to all `json.dumps` calls (API responses and HTML-embedded JSON) to prevent emoji characters from being serialized as invalid UTF-16 surrogate pairs.

## 12.9.2 - 2026-03-29
### Fixed
- **Web CO₂ range parameter now passed to API** – The `/api/co2?range=7d` query parameter was not forwarded to the handler, so the web dashboard always showed 24h regardless of button selection.
- **Fixed CO₂ range parameter type** – The range parameter was incorrectly treated as a list instead of a string, causing a potential crash in the web CO₂ handler.

## 12.9.1 - 2026-03-29
### Fixed
- **Fuel mix backfill for full history** – The generation mix (Kraftwerksmix) was only available for hours fetched in the current session. Historical hours imported by older versions had CO₂ intensity but no fuel mix data. The service now checks fuel mix coverage for the entire measurement range and backfills missing chunks from ENTSO-E automatically. Navigating backwards through the fuel mix display now shows real generation data for all historical hours.

## 12.9.0 - 2026-03-29
### Added
- **CO₂ chart range selector** – Both the desktop CO₂ tab and web dashboard now have range buttons (24h, 7 Days, 30 Days, All) to view historical CO₂ intensity data. Previously only the last 24 hours were shown despite having months of data in the database.
- **Auto-sync on app startup** – The app automatically starts an incremental data sync from all configured Shelly devices 2 seconds after launch. No more manual "Sync" click needed.
- **ENTSO-E check triggered on startup** – The CO₂ fetch service runs immediately when the app opens, checking for missing historical data right away.

### Changed
- **ENTSO-E CO₂ data persisted per chunk** – Each 7-day chunk is written to the database immediately after download (crash-safe), instead of buffering all data in memory until the end.
- **ENTSO-E gap detection scans full history** – Every fetch cycle checks the entire range from oldest measurement to now. Only actual gaps are fetched from the API, aligned to day boundaries and merged into efficient ranges.
- **Estimated placeholders limited to last 48h** – Old estimated values are purged each cycle so historical gaps get real ENTSO-E data. Only the last 48 hours get estimated placeholders where the API may not yet have data.

## 12.7.3 - 2026-03-29
### Fixed
- **Consistent nav icons in web dashboard** – Forecast, Standby, and Energy Flow tabs were missing dedicated `nav-icon` spans, causing their emoji icons to render at label size (9px) instead of the standard icon size (18px). All 11 web nav tabs now use the same icon+label structure for uniform appearance.
- **Added emoji icons to all desktop tabs** – Sync (🔄), Charts (📊), Live (📡), Costs (💰), Heatmap (🔥), Export (📤), and Settings (⚙️) tabs now have icons matching the other tabs. Updated across all 9 languages.

## 12.7.2 - 2026-03-29
### Fixed
- **Removed misleading "Jetzt rückfüllen" hint** – The CO₂ mix table showed "Keine Daten – bitte 'Jetzt rückfüllen' ausführen" even though the backfill button was removed in v12.7.0. Updated all 9 languages to show "No data – loading automatically …" instead.
- **Removed obsolete backfill i18n keys** – Cleaned up `co2.settings.backfill` and `co2.settings.backfill_btn` translation keys in all 9 languages (no longer used since auto-backfill).
- **Log messages say "Import" instead of "Backfill"** – All ENTSO-E sync log messages now use "CO₂ Import" wording since the process is fully automatic.

## 12.7.1 - 2026-03-29
### Fixed
- **Web dashboard crash: UTF-16 surrogates in traffic JS** – The network traffic category icons used `\ud83d\udd0c` style escape sequences which cause a `surrogates not allowed` error during gzip compression. Replaced with direct Unicode emoji characters.

## 12.7.0 - 2026-03-29
### Changed
- **ENTSO-E auto-backfill replaces manual backfill** – The CO₂ fetch service now automatically determines how far back to fetch by looking at the oldest energy measurement in the database. No more manual "Rückfüllen" button or "backfill_days" setting needed. When CO₂ is enabled with a valid token, the service fetches intensity data for the entire period covered by your Shelly measurements.
- **Removed from settings:** "Rückfülltage" field and "Jetzt rückfüllen" button. The service handles everything automatically in the background.

### Fixed
- **Kraftwerksmix navigation no longer jumps to "Now"** – The 60-second auto-refresh was resetting the fuel mix navigation offset to 0, causing the display to jump back to the current hour while browsing historical data. The offset is now preserved during auto-refresh.
- **Historical fuel mix data now stored for all hours** – Previously only the latest hour's fuel mix was saved to the database during each fetch chunk. Now all hours' mix data from each ENTSO-E response is stored, so navigating through past hours with ◀/▶ buttons shows actual generation mix data instead of "no data".

## 12.6.0 - 2026-03-29
### Added
- **Network traffic monitor** – New "Network Traffic" section in the Sync tab showing real-time bandwidth usage and cumulative data transferred by the app. Displays:
  - **Live rate** – current download/upload speed (↓/↑ B/s, KB/s, MB/s)
  - **Cumulative totals** – total bytes received/sent, request count, and app uptime
  - **Per-category breakdown table** – traffic split by source: Shelly Devices, ENTSO-E API, OpenWeather, Telegram, GitHub, and Other. Sorted by received bytes, updated every 2 seconds.
  - Intercepts all HTTP traffic (both `requests` library and `urllib`) via monkey-patching at startup – zero changes needed in individual services.
- **Web /control page: traffic card** – The web control page shows the same traffic stats with a live-updating table (polled every 3 seconds via `/api/traffic` endpoint).
- **New API endpoint `/api/traffic`** – Returns current traffic snapshot with per-category breakdown, rates, and totals.

## 12.5.2 - 2026-03-29
### Fixed
- **Web nav: duplicate icons on Forecast, Standby, Energy Flow** – The nav buttons had a hardcoded emoji in `<span class="nav-icon">` AND the i18n tab label already contained the same emoji. Removed the redundant `nav-icon` spans so each icon appears only once.

## 12.5.1 - 2026-03-29
### Fixed
- **CHANGELOG in English** – Translated the v12.5.0 changelog entry from German to English. All changelog entries are now consistently in English.

## 12.5.0 - 2026-03-29
### Added
- **Generation mix time navigation** – The generation mix (Kraftwerksmix) in the CO₂ tab now has navigation buttons to browse historical fuel mix data:
  - **◀ Day / Day ▶** — jump 24 hours backward/forward
  - **◀ h / h ▶** — jump one hour backward/forward
  - **Now** — jump to the most recent available hour
  - Timestamp shows weekday + date + time (e.g. "Saturday, 29.03.2026  14:00")
  - Source indicator next to timestamp: **✅ ENTSO-E** (real data) or **⚠️ Estimated** (gap-filled)
  - Automatically finds the nearest available hour when no data exists for the selected hour

## 12.4.1 - 2026-03-29
### Fixed
- **Solar now appears in ENTSO-E Kraftwerksmix** – ENTSO-E often delivers solar generation data (B16) with 1-2 day delay, causing it to be missing from the fuel mix display. When solar data is absent, the app now estimates solar generation based on installed capacity per country (82 GW for DE_LU, etc.) and a typical capacity factor profile by hour-of-day and month. Estimated solar is included in both the CO₂ intensity calculation and the Kraftwerksmix table. This affects all three places where the fuel mix is stored: initial fetch, periodic updates, and recovery fetch.

## 12.4.0 - 2026-03-29
### Fixed
- **CO₂ calculation now includes solar offset** – Previously all device consumption was multiplied by grid CO₂ intensity, ignoring PV self-consumption entirely. Now the CO₂ tab correctly separates grid import from PV self-consumption:
  - **Grid import** (positive kWh from PV meter) → multiplied by ENTSO-E CO₂ intensity
  - **PV self-consumption** → CO₂-free (not counted)
  - **Feed-in** (negative kWh) → subtracted as CO₂ credit (avoided grid emissions)
  - Summary cards show net CO₂ with solar savings indicator (☀️ -X.XX kg)
- Applies to both **desktop CO₂ tab** and **web CO₂ tab** (`/api/co2` endpoint)

## 12.3.1 - 2026-03-29
### Fixed
- **Anomaly spam eliminated** – Event IDs are now deterministic (based on device + type + date instead of random UUID). The same anomaly on the same day always produces the same ID, so it's only notified once. A separate `_anomaly_notified_ids` set tracks which events have already been sent — even across detection cycles.
- **Web anomalies now show data** – The web `/api/anomalies` endpoint now reads from the desktop app's `_anomaly_log` (shared state) instead of re-running detection independently. This means anomalies appear in the web dashboard as soon as they're detected by the auto-timer. Falls back to a one-time detection if the log is empty.

## 12.3.0 - 2026-03-29
### Added
- **NILM mode toggle (click to switch)** – Click on the "Mögliche Geräte" area in the desktop Live tab to cycle between three detection modes:
  - **Kombiniert** (default) – Static signature matching with ML confidence boost (+15% for confirmed patterns)
  - **Statisch** – Pure built-in signature matching only (25 appliance profiles)
  - **ML only** – Shows only ML-learned patterns that match current power reading. Includes unknown patterns (not in built-in database) with power range display. Great for discovering devices the static database doesn't cover.
- Mode indicator shows current mode + number of ML patterns learned per device, e.g. "[Kombiniert | 5 ML-Muster]"
- Hint text below appliance display shows "Klick: Modus wechseln"

## 12.2.1 - 2026-03-28
### Fixed
- **NILM ML: per-device learners for 3-phase EMs only** – Each 3-phase Shelly EM device now gets its own independent TransitionLearner with its own cluster file (`nilm_clusters_{device_key}.json`). Single-phase devices and switches are excluded. Previously all devices shared one global learner which mixed transitions from different circuits, making clustering meaningless. Status display now shows per-device breakdown ("Haus: 5 Muster/47 Trans., Server: 3 Muster/22 Trans.").

## 12.2.0 - 2026-03-28
### Added
- **ML NILM fully integrated** – The TransitionLearner is now instantiated on app startup and fed with every live power reading. It detects step changes (>50W) in power consumption, clusters them via k-means, and matches recurring patterns against the built-in appliance database. Learned clusters are persisted to `data/runtime/nilm_clusters.json` and survive app restarts.
- **NILM learning status in desktop Live tab** – Shows a status indicator ("NILM: X Muster / Y Transitionen") in the Live tab header that updates every 5 minutes as new patterns are discovered.
- **NILM learning status in web Live tab** – A badge at the bottom of the Live pane shows cluster count and top-3 learned patterns (e.g. "NILM ML: 5 patterns learned | 100W x12  2200W x8  40W x15"). Updates every 30 seconds via `/api/nilm_status` endpoint.
- **NILM progress in Sync log** – Every 30 minutes, the current learning progress is logged in the Sync tab (transition count, cluster count, top patterns).
- **ML-boosted appliance confidence** – When a static appliance match (identify_appliance) is also confirmed by an ML-learned cluster with 5+ observations, confidence is boosted by +15%. This makes frequently-seen appliances rank higher and appear with green dots more often.
- **New API endpoint `/api/nilm_status`** – Returns the current NILM ML cluster count and top clusters for the web dashboard.

## 12.1.2 - 2026-03-28
### Fixed
- **Web heatmap: yearly calendar readable on mobile** – Calendar cells now have a minimum size of 10px (was 4px) so they remain visible and tappable. Month labels always use 3-character format (Jan, Feb, ...) instead of single letters. Container scrolls horizontally on narrow screens with smooth touch scrolling instead of squeezing cells to illegible sizes.

## 12.1.1 - 2026-03-28
### Fixed
- **Desktop plots scale to monitor size** – All new tabs (Forecast, Standby, Weather, Sankey, Tenant) now use `grid` layout with `sticky="nsew"` and `weight=1` instead of scrollable frames. Charts fill the available space vertically and horizontally, matching the Heatmap tab behavior. Resizing the window or using a larger monitor scales all plots proportionally.

## 12.1.0 - 2026-03-28
### Changed
- **Web charts: Canvas instead of Plotly** – Replaced all Plotly.js charts in Forecast, Standby, and Sankey tabs with lightweight HTML5 Canvas bar charts matching the CO₂ tab style. No more heavy Plotly library loading, no zoom/pan overhead. Charts render instantly with theme-aware colors, grid lines, value labels, and threshold markers. Removed Plotly.js script tag from main dashboard page.

### Fixed
- **Forecast chart** – Now shows history (blue) + forecast (red) bars with date labels, plus weekday and hourly profile bar charts with color-coding (red=high, green=low, blue=normal).
- **Standby chart** – Cost-per-device bar chart with risk colors + 24h load profile with standby threshold line. Charts render inside `.card` containers matching dashboard design.
- **Sankey/Energy Flow** – Replaced Plotly Sankey with simple device consumption bar chart. Shows per-device kWh breakdown with colors matching the flow diagram.

## 12.0.8 - 2026-03-28
### Fixed
- **Web dashboard: Plotly.js now loaded on main page** – Plotly.js was only included on the `/plots` sub-page but not on the main dashboard HTML. All chart tabs (Forecast, Standby, Sankey) showed no plots because `typeof Plotly === 'undefined'`. Added `<script defer src="/static/plotly.min.js">` to the main HTML template.

## 12.0.7 - 2026-03-28
### Fixed
- **Web forecast: weekday + hourly profile charts now render** – The profile data had integer keys in Python (`{0: 1.05}`) which became string keys in JSON (`{"0": 1.05}`). JS now accesses both `String(i)` and `i` to handle both formats. API also explicitly converts keys to strings for consistency.

## 12.0.6 - 2026-03-28
### Fixed
- **Web forecast: device key not sent** – The forecast device selector accessed `d[0]`/`d[1]` but DEVICES is an array of `{key, name}` objects. Fixed to use `d.key`/`d.name`, so the API now receives a valid device key and returns forecast data.

## 12.0.5 - 2026-03-28
### Fixed
- **Weather tab: graceful empty state** – When not enough paired weather+energy data exists, the charts now show a helpful message ("Wetter-Daten werden stündlich gesammelt...") and the energy-only chart of the last 48h instead of empty axes. Cards and correlation values use proper LabelFrame + grid layout matching other tabs.
- **Tenant tab: move-in date (Einzugsdatum)** – Added move-in date field to tenant settings. The yearly billing calculation now starts from the move-in date. Settings UI shows date field with explanation text.
- **Tenant tab: chart redesign** – Grouped bar chart (kWh + € side by side), donut chart for single tenant, proper value labels, consistent colors and styling matching Solar/CO₂ tabs. Empty state shows info message instead of blank chart.
- **Web forecast: fix empty state** – Fixed device selector initialization (ensure selectedIndex=0 on first load) so the forecast API receives a valid device_key. Added loading indicator and better error display.

## 12.0.4 - 2026-03-28
### Fixed
- **Web dashboard crash: UTF-8 surrogate encoding error** – Removed UTF-16 surrogate escape sequences (`\ud83d\udfe2`) from the standby tab JavaScript that caused a `'utf-8' codec can't encode characters: surrogates not allowed` error during HTML template gzip compression. The web dashboard now starts correctly.

## 12.0.3 - 2026-03-28
### Fixed
- **Web nav bar: single row with horizontal scroll** – Bottom navigation now uses flexbox instead of CSS grid, enabling horizontal scrolling when all tabs don't fit on screen. No more two-row wrapping on mobile.
- **Web Forecast/Standby/Sankey: proper card design** – All new web tabs now use the same `metricCardHtml()` + `.card` + `.metric-grid` CSS patterns as Costs and Solar tabs. Cards have proper borders, rounded corners, accent colors, and responsive grid layout.
- **Web Forecast: Plotly charts** – Forecast tab now renders proper Plotly bar charts for history + prediction with confidence bands, plus weekday and hourly profile charts with color-coded bars.
- **Web Standby: device cards + charts** – Standby tab now shows per-device cards with risk badges, horizontal bar chart, and 24h load profile with base-load line.
- **Web Sankey: Plotly Sankey diagram** – Energy flow tab now renders an interactive Plotly Sankey diagram instead of broken plain text. Period buttons use proper `btn btn-outline btn-sm` styling.
- **Tenant settings: Shelly device checkboxes** – Tenants can now be assigned devices via checkboxes showing all configured Shelly devices by name. Devices already assigned to another tenant are disabled/grayed out. Each tenant row is a labeled frame with name, unit, person count, and device assignment. Add/remove tenants via buttons.

## 12.0.2 - 2026-03-28
### Fixed
- **Web Forecast tab** – Fixed broken API endpoint (undefined `qs_params` variable) and device selector (referenced `_devices` instead of `DEVICES`). Forecast tab now loads and renders correctly in the web dashboard.
- **Web Sankey tab** – Fixed broken API endpoint (same `qs_params` issue). Energy flow diagram now renders via Plotly.js.
- **Standby tab redesign** – Completely redesigned to match Solar/CO₂ tab patterns: scrollable content area, consistent padding (14px), proper LabelFrame sections, grid-aligned cards, cleaner bar chart with grid lines and value labels, improved 24h profile with night shading.
- **Forecast tab redesign** – Redesigned with scrollable layout, LabelFrame chart containers, consistent card grid, and polished chart styling (grid lines, edge colors, axis formatting).
- **Sankey tab redesign** – Replaced broken matplotlib Sankey with clean energy flow visualization: source bars (left) → house node (center) → consumer bars (right) with flow arrows and percentage labels.
- **Tenant settings UI** – Tenants can now be fully configured in the Settings tab (no config.json editing needed). Add/remove tenants with name, unit, person count, and device keys. Common area meters configurable via UI.

## 12.0.1 - 2026-03-28
### Fixed
- **Weather: auto-geocode city** – The weather tab now automatically resolves the city name to lat/lon coordinates via the OpenWeatherMap Geocoding API. Previously lat/lon stayed at 0 after entering a city, causing "no data". Geocoding also runs when settings are saved.
- **Standby: reduced minimum data requirement** – The standby analysis now works with as few as 6 hours of data (previously required 48h). Falls back to raw sample data when hourly aggregation is not yet available.
- **Forecast: reduced minimum data requirement** – Forecast now works with 3+ days of data (previously 7). Falls back to raw samples when hourly data is missing.
- **All new tabs: removed Refresh buttons** – Removed manual "Aktualisieren" buttons from Forecast, Standby, Weather, Sankey, and Tenant tabs. Tabs now refresh automatically when selected via tab switch.
- **Auto-refresh on tab switch** – All new tabs (Forecast, Standby, Weather, Sankey, Tenant) now refresh their content automatically when the user switches to them.

## 12.0.0 - 2026-03-28
### Added
- **Consumption Forecast** – New 📈 Forecast tab with linear regression + weekday/hourly seasonality. Shows daily consumption prediction with confidence bands, trend analysis (rising/falling/stable), and projected costs for next month and year. Includes weekday and hourly profile charts with color-coded patterns. Available in both desktop app and web dashboard.
- **Standby Killer Report** – New 🔌 Standby tab that identifies devices with constant base load. Shows annual standby cost and kWh per device, risk classification (high/medium/low), and 24h load profiles. Highlights saving potential with cost breakdown bar chart. Available in desktop and web dashboard.
- **Weather Correlation** – New 🌡️ Weather tab integrating OpenWeatherMap API. Shows current weather conditions, Pearson correlation (r) between temperature and consumption, heating/cooling degree days (HDD/CDD), and scatter plot with trend line. Helps identify heating/cooling-related consumption patterns.
- **Energy Flow Diagram (Sankey)** – New ⚡ Energy Flow tab with interactive Sankey diagram. Visualizes energy flows: Grid → House → Devices and PV → Self-consumption / Feed-in. Period selector (today/week/month/year). Uses Plotly.js in web dashboard for interactive Sankey rendering.
- **ML-enhanced NILM** – Appliance detector now includes a learning engine with k-means clustering on power transitions. Automatically discovers recurring appliance patterns from step changes in power consumption. Clusters are persisted and matched against the built-in appliance database.
- **Home Assistant MQTT Integration** – New MQTT publisher that sends device metrics to any MQTT broker. Supports Home Assistant auto-discovery (creates sensor entities automatically). Publishes power, voltage, current, energy, frequency, power factor, and CO₂ rate per device. Configurable broker, authentication, TLS, and publish interval.
- **PV Amortization Calculator** – New section in the ☀️ Solar tab showing investment payback analysis. Displays investment amount, annual savings, payback period, ROI after 20 years, and total CO₂ saved. Includes amortization timeline chart with cumulative savings vs. investment crossover point. Accounts for panel degradation.
- **Tenant Utility Billing** – New 🏠 Tenants tab for multi-tenant utility billing (Nebenkostenabrechnung). Supports per-tenant device assignment, common area electricity split by person count, pro-rated base fees, move-in/move-out handling, and PDF invoice export. Includes cost comparison bar chart and kWh share pie chart.
- **New config sections** – Added `forecast`, `weather`, `mqtt`, and `tenant` configuration blocks in config.json with full settings UI in the desktop app.
- **Weather data storage** – New `weather_data` table in the SQLite database for persisting hourly weather observations.
- **3 new web dashboard tabs** – Forecast, Standby, and Sankey/Energy Flow tabs with interactive Plotly.js charts and KPI cards.

## 11.20.4 - 2026-03-28
### Fixed
- **Fuel mix recovery: rate limit + wider window** – The fuel mix recovery fetch was failing silently due to ENTSO-E rate limiting (62s between API calls). Now waits 65s before the recovery request. Also, initial backfill now always covers at least 2 days to ensure yesterday's data (which is reliably available) is included even when today's hasn't been published yet.

## 11.20.3 - 2026-03-28
### Fixed
- **Fuel mix recovery on empty DB** – When no fuel mix data exists in the database (e.g. after upgrade to v11.20.2), the service now automatically fetches generation data from the previous 48 hours to populate the mix. This covers the case where ENTSO-E has no data for the current hour yet but does have recent historical data. The recovered data is stored in the DB for future use.

## 11.20.2 - 2026-03-28
### Fixed
- **Fuel mix always visible** – The generation/fuel mix (Kraftwerksmix) is now persisted to the database. Previously it was only held in memory and disappeared on app restart or when no ENTSO-E fetch had completed yet. Both the desktop app and web dashboard now fall back to the last stored mix from the DB, so the mix is always shown as long as data has been fetched at least once.

## 11.20.1 - 2026-03-28
### Fixed
- **Web CO₂: live intensity dropping to 0** – The `/api/co2_live` endpoint queried only the exact current hour slot from the database. When ENTSO-E data for the current hour hadn't been fetched yet (fetch lag), the intensity returned 0 g/kWh, overwriting the correct value from the initial load. Now queries the last 3 hours and uses the most recent available entry, matching the behavior of the full `/api/co2` endpoint.

## 11.20.0 - 2026-03-28
### Fixed
- **Anomaly timestamps** – Anomaly events now show the actual time of the anomalous activity instead of always 00:00. Unusual daily consumption shows the hour of peak usage, night consumption shows the peak night hour, and power peak time shows the actual peak moment.

### Added
- **Automatic anomaly detection** – New periodic auto-detection timer runs anomaly checks every N minutes (default 15 min, configurable in settings). When anomalies are found, notifications are sent immediately via configured channels (Telegram, Webhook, Email) without manual intervention.
- **Auto-check interval setting** – New "Auto-Prüfung alle: N min" field in the anomaly settings panel. Timer restarts automatically when settings change.

## 11.19.7 - 2026-03-28
### Removed
- **Heatmap: € filter removed** – Removed the Euro/cost filter option from the heatmap in both the desktop app and the web live dashboard. Only kWh and g CO₂ filters remain.

## 11.19.6 - 2026-03-28
### Added
- **Web CO₂: yearly summary card** – The web CO₂ tab now shows a "Year" summary card alongside today/week/month, displaying total CO₂ in kg since January 1st with tree-days and car-km equivalents.
- **Live CO₂ device rates auto-refresh** – Per-device CO₂ rates in the web CO₂ tab now update every 1 second via a lightweight `/api/co2_live` polling endpoint using LiveStateStore snapshots.

## 11.19.5 - 2026-03-28
### Added
- **Web CO₂: per-device 24h bar charts** – Each 3-phase Shelly EM device gets its own 24h rolling CO₂ bar chart in the web CO₂ tab. Bars are color-coded by grid intensity (green/yellow/red), with tooltips showing g CO₂ and kWh per hour. Shows total 24h CO₂ in kg per device. Single-phase devices and switches are excluded.

## 11.19.3 - 2026-03-28
### Added
- **Web CO₂ tab** – New dedicated CO₂ tab in the web live dashboard with:
  - Live grid CO₂ intensity hero card with color-coded value (green/yellow/red)
  - Summary cards: today, week, month CO₂ in kg + tree-days and car-km equivalents
  - 24h intensity line chart (canvas) with color-gradient line and threshold markers
  - 24h heatmap strip with green→red color coding per hour
  - Per-device live CO₂ rate table (g CO₂/h based on current watts × intensity)
  - Generation mix stacked bar + detail table with fuel type, MW, share %, and emission factor
  - Cross-border flow source indicator (ENTSO-E vs ENTSO-E + Cross-Border)
- **New API endpoint** `/api/co2` – Returns current intensity, 24h hourly data, device rates, fuel mix, and period summaries

## 11.18.2 - 2026-03-28
### Added
- **Cross-border flow CO₂ adjustment** – CO₂ intensity calculation now optionally accounts for physical electricity imports/exports between bidding zones using ENTSO-E A11 (cross-border flows) and A65 (system total load) data. Imported electricity is weighted with the source zone's CO₂ intensity, exports are subtracted from local generation. No additional API token required.
- **36 European neighbor zone mappings** – Full interconnection topology for all supported ENTSO-E bidding zones (DE_LU, AT, FR, PL, etc.)
- **Static fallback intensities** – Annual average CO₂ intensities for 38 zones (based on EEA/ENTSO-E 2022-2024 data) used as neighbor zone defaults to minimize API calls
- **Desktop settings: cross-border checkbox** – New toggle in ENTSO-E settings: "Cross-Border-Flows einbeziehen" to enable/disable the feature from the UI
- **Graceful degradation** – Falls back to local-only intensity if A65 or A11 requests fail; partial neighbor data is used when available

## 11.17.3 - 2026-03-27
### Fixed
- **Setup wizard: missing tabs after first-run** – The wizard finish handler did not enable or build the Solar, Anomaly, Schedule, and CO₂ tabs. Users completing the first-run wizard saw these 4 tabs remain disabled/empty until restarting the app. Now all tabs are correctly activated and built on wizard completion.

## 11.17.2 - 2026-03-27
### Fixed
- **Web dashboard blank page fix** – The solar settings toggle button used escaped quotes in an inline `onclick` handler that broke the entire page's JavaScript execution. Replaced with a proper `addEventListener` binding after DOM insertion. Also fixed `_solarSettingsHtml()` to read config fields from both `data.config` (unconfigured response) and directly from `data` (configured response).

## 11.17.1 - 2026-03-27
### Added
- **Desktop settings: new solar fields** – Settings → Solar / PV now shows input fields for installed capacity (kWp), battery storage (kWh), and lifecycle CO₂ per kWp. All three are persisted to config.json on save.
- **Web solar tab: inline settings** – Solar tab in the web live dashboard now shows a ⚙️ Settings button that expands a config panel for: enable/disable, PV meter device, feed-in tariff, kWp, battery, CO₂/kWp. Changes are saved live via the new `save_solar_config` API action.
- **Web solar tab: setup form** – When solar is not yet configured, the tab shows the full settings form directly instead of just "not configured", so users can set it up from their phone.

## 11.17.0 - 2026-03-27
### Added
- **Solar CO₂ savings** – The Solar tab (web + desktop) now calculates and displays how much CO₂ was avoided by PV production. Uses real ENTSO-E grid intensity when available, falls back to static 380 g/kWh. Shows:
  - CO₂ saved (kg) – PV production × grid intensity
  - CO₂ from grid (kg) – actual grid import × intensity
  - Equivalents: tree-days and car-km avoided
- **Solar system config** – New configurable fields in `solar` config section:
  - `kw_peak` – installed PV capacity in kWp
  - `battery_kwh` – battery storage capacity in kWh (0 = no battery)
  - `co2_production_kg_per_kwp` – embodied CO₂ per kWp for lifecycle analysis (default 1000 kg)
- **Web Solar tab: system info cards** – When `kw_peak` is configured, shows installed capacity, battery size, and embodied CO₂ from panel production
- **Costs tab: solar CO₂ offset** – Monthly costs API response now includes `solar_co2_saved_month_kg` showing PV-displaced CO₂ emissions
- **Desktop Solar tab: 4 new CO₂ cards** – CO₂ saved, CO₂ grid, tree-days equivalent, car-km avoided

## 11.16.0 - 2026-03-27
### Improved
- **Web export: professional PDF reports** – Both "PDF Summary + Plots" and "Report (Tag/Monat)" now generate the same rich, professional multi-page PDFs previously only available in email reports. Includes:
  - Header band with title/date, 6 KPI boxes (kWh, EUR, CO₂, avg W, peak W, peak hour)
  - Color-coded +/- % comparison vs. previous period
  - Enhanced device table with % share and CO₂ columns
  - Highlights/Lowlights section (top/lowest consumer, peak hours)
  - Stacked hourly/daily consumption chart (per-device colors)
  - CO₂ emissions chart with ENTSO-E intensity-based green→yellow→red gradient
  - Per-device detail pages with mini charts and operating stats
  - Monthly reports additionally include weekday/weekend split, best/worst day, top-5 ranking chart
- **Summary uses smart format selection** – Single-day ranges produce a daily report layout; multi-day ranges produce a monthly report layout with daily aggregation.
- **Fallback to old format** – If the professional report data builder fails, the system falls back to the original simple PDF format instead of crashing.

## 11.15.2 - 2026-03-27
### Fixed
- **Web export: no more raw JSON in results** – Async actions (invoices, reports) that return a job object (`{"ok":true,"job":{...}}`) are now correctly recognized and show a clean "✓ Job #N gestartet" info card instead of dumping raw JSON as an error.
- **Web export: mobile-friendly file names** – Long file names in both results and job cards now truncate with ellipsis instead of overflowing the screen. Job file links use a dedicated compact layout with icon + truncated name.
- **Web export: cleaner styling** – Smaller icons (24px), slimmer progress bars (6px), tighter spacing. Unified `_expHandleResult()` / `handleResult()` dispatchers replace duplicated if/else chains in all button handlers.

## 11.15.1 - 2026-03-27
### Fixed
- **Web export: file cards instead of broken preview** – Export results now display as clear file cards with type icon (📄 PDF, 📊 Excel, 📦 ZIP), filename, and a prominent "Öffnen" button that opens the file directly on smartphone/tablet. Replaces the previous unreadable JSON/iframe preview.
- **Web export: live jobs panel** – The export tab now includes a "Running Jobs" section that polls `/api/jobs` every 2 seconds, showing real-time progress bars, status badges (running/done/error), and clickable download links when jobs complete. Polling stops automatically when leaving the tab.
- **Control page export: same file-card fix** – The /control page export section also uses the new file-card layout with direct "Öffnen" links instead of the old broken preview.

## 11.15.0 - 2026-03-27
### Added
- **Web live view: Export tab** – New "Export" tab (📥) in the bottom navigation bar of the live web dashboard. Provides the same full export functionality previously only available on the /control page: PDF summaries, invoices, Excel export, daily/monthly reports, and ZIP bundles – all with inline preview (PDF iframe, image grid, download links), quick date presets, configurable bundle hours, and loading spinners on buttons. Translated in all 9 languages.

## 11.14.0 - 2026-03-27
### Improved
- **Web export tab: complete redesign** – Export card now spans full width with a two-column layout: date range (with quick-select buttons for Today/Week/Month/Year/All and native date pickers) on the left, invoice & bundle settings on the right. Six action buttons with icons and labels in a responsive grid, each showing a loading spinner during generation.
- **Web export tab: inline preview** – New preview area at the bottom of the export card displays generated PDFs as embedded iframes, images in a clickable grid, and download links in the header for quick access.
- **Web export tab: Excel export** – New "Excel Export" button exports device energy data as `.xlsx` with one sheet per device, optionally filtered by date range.
- **Web export tab: configurable bundle hours** – ZIP bundle hours are now adjustable via an input field (default 48h) instead of being hardcoded.

## 11.13.0 - 2026-03-27
### Improved
- **CO₂ charts: smooth intensity-based gradient coloring** – CO₂ bar charts in Telegram summaries and email/PDF reports now use the same smooth green→yellow→red gradient as the desktop app, colored by actual ENTSO-E grid intensity (g CO₂/kWh) per bar instead of fixed absolute thresholds. Uses configurable green/dirty thresholds from CO₂ settings.

## 11.12.0 - 2026-03-27
### Added
- **Web live view: reactive power display** – Each device card now shows total reactive power (VAR) in the expandable detail section, with a dedicated sparkline chart. Clicking the sparkline opens an enlarged detail chart with per-phase breakdown (L1/L2/L3), just like the existing power/voltage/current charts.
- **Web live view: phase balance indicator** – Multi-phase devices now show a phase balance row displaying each phase's share of total power as a percentage (e.g. L1 33% · L2 35% · L3 32%).
- **Web live view: translated appliance names** – NILM device analysis chips now display properly translated appliance names (e.g. "Refrigerator" instead of raw ID "fridge") using the i18n system for all supported languages.

## 11.11.0 - 2026-03-27
### Added
- **CO₂ plots in Telegram & email reports** – Daily and monthly Telegram summaries now include CO₂ emission bar charts (hourly/daily) when ENTSO-E data is available. Email/PDF reports add a dedicated CO₂ chart page with green→yellow→red color coding by emission intensity.
- **Reusable CO₂ calculation helper** – New `_calc_co2_for_range()` and `_calc_co2_hourly_series()` methods provide a single source of truth for ENTSO-E-weighted CO₂ calculations with automatic static fallback.

### Improved
- **All CO₂ calculations now use ENTSO-E data** – Previously, email reports, Telegram summaries, webhook payloads, per-device breakdowns, standby estimates, and kWh plot labels all used a static 380 g/kWh factor. They now query the ENTSO-E database for real hourly grid intensity and fall back to the static value only when no ENTSO-E data is available. Affected: `_build_email_report_data`, `_build_telegram_summary`, webhook daily/monthly payloads, plots tab CO₂ labels, cost tab CO₂ calculations.
- **CO₂ source indicator** – Telegram summaries now show "(ENTSO-E)" or "(statisch)" next to CO₂ values so users know the data source.

### Removed
- **Dead code cleanup** – Removed 12 unused functions/classes: `export_dataframe_csv`, `OverallReport`, `_draw_device_table` (non-enhanced), `suggest_time_range`, `combine_devices`, `download_file`, `install_update_zip`, `rebuild_hourly`, `oldest_sample_ts`, `oldest_monthly_ts`, `pack_csvs`, `base_fee_year_gross`.

## 11.10.0 - 2026-03-27
### Added
- **Web dashboard: real ENTSO-E CO₂ in costs tab** – The web costs tab now computes CO₂ emissions using actual hourly grid intensity from ENTSO-E instead of a static g/kWh factor. Falls back to the configured static intensity if ENTSO-E is unavailable.
- **Web dashboard: CO₂ heatmap mode** – The web heatmap now offers "g CO₂" as a third unit alongside kWh and €, with a distinct yellow → orange → red colour scheme. Uses real ENTSO-E data with static fallback.
- **README updated** – Documents all new CO₂ features across plots, heatmap, cost tab, and web dashboard.

## 11.9.1 - 2026-03-27
### Added
- **CO₂ bar coloring by intensity** – Bars in the CO₂ plots tab are now colored on a green → yellow → red gradient based on the average grid CO₂ intensity for each time bucket. Uses the existing green/dirty threshold settings.
- **kWh + CO₂ labels above bars** – Each bar now shows both the energy consumption (kWh) and the CO₂ emissions (g/kg) as a two-line annotation above each bar.
- **Custom range controls for CO₂ plot tab** – The CO₂ plots tab now has the same "Last N [hours/days/weeks/months]" input fields and Apply button as the kWh tab.
- **Heatmap: CO₂ display mode** – The heatmap tab now offers "g CO₂" as a third unit alongside kWh and €. Uses a distinct YlOrRd (yellow → orange → red) color scheme. Both the calendar heatmap and the weekday×hour heatmap show CO₂ emissions using real ENTSO-E hourly grid intensity data, with fallback to the static g/kWh factor from pricing settings.

### Improved
- **Cost tab uses real ENTSO-E CO₂ data** – The cost tab now computes CO₂ emissions using actual hourly grid intensity from ENTSO-E instead of a static g/kWh factor. Falls back to the configured static intensity if ENTSO-E is not set up or has no data for the period.

## 11.7.0 - 2026-03-27
### Added
- **CO₂ emissions plot tab** – New "CO₂" sub-tab in Plots joins hourly energy consumption with real-time grid CO₂ intensity data to show actual emissions per device. Supports four granularities (hours, days, weeks, months) with smart default time windows (24h, 30d, 12w, 1y). Bar chart automatically switches between g and kg CO₂ units. Uses the same device notebook, time range controls, theme, and annotation style as existing plot tabs.

## 11.6.0 - 2026-03-27
### Improved
- **CO₂ backfill: retry, skip & gap-fill** – When an ENTSO-E API request fails (e.g. HTTP 503), the service now retries the chunk up to 3 times with increasing backoff (30s, 60s) before skipping it and continuing with the next chunk. Previously a single failure aborted the entire backfill. On subsequent fetches, the service detects gaps (missing hours or hours filled with estimated data) and automatically attempts to reload real data for those ranges. Any hours that remain unfilled after all retries are populated with estimated intensity values (average of surrounding real data, or 400 g/kWh fallback) marked with `source="estimated"` so they are clearly identifiable and will be replaced by real data on the next successful fetch.

### Fixed
- **Version stuck at 11.4.1** – The `__version__` in `__init__.py` (used at runtime by the app, settings, and update checker) was not updated alongside `pyproject.toml` since v11.4.1. Now corrected to 11.6.0.

## 11.5.0 - 2026-03-27
### Added
- **CO₂ tab: "Reset DB" button** – A new button in the CO₂ tab title bar lets the user delete all stored CO₂ intensity data and trigger a complete re-fetch from ENTSO-E. A confirmation dialog prevents accidental resets. After deletion, the background service automatically starts a fresh backfill. Useful when data quality is suspect (e.g. after correcting PSR mappings or bidding zone changes). Button label and dialog text translated into all 9 languages.

## 11.4.1 - 2026-03-27
### Fixed
- **"Check Now" button now refreshes the changelog** – Clicking "Jetzt prüfen" / "Check now" in Settings > Updates only checked for new versions but did not re-fetch the changelog from GitHub. The changelog text widget would only update on app restart or after installing a new version. The button now calls `_updates_on_check_clicked()` which triggers both the version check and a changelog re-fetch.

## 11.4.0 - 2026-03-27
### Added
- **CO₂ tab: "Reload" button** – A button in the CO₂ tab title bar lets the user trigger an immediate data fetch from ENTSO-E without waiting for the next scheduled auto-fetch. Clicking it calls `Co2FetchService.trigger_now()` which wakes the background service instantly. Button label translated into all 9 languages (de, en, es, fr, pt, it, pl, cs, ru).

## 11.3.0 - 2026-03-27
### Fixed
- **Critical: ENTSO-E PSR type mapping was completely wrong** – The `_PSR_NAMES` dictionary in `entsoe.py` mapped every code from B03 onwards to the wrong fuel type. The mapping appeared to have been constructed by skipping B03 (Fossil Coal-derived gas) and then shifting all subsequent codes by one position. Concrete examples of the corruption: B05 (Fossil Hard coal) was labelled `"oil"` and received an oil CO₂ factor (650 g/kWh instead of 820); B08 (Fossil Peat, ~1150 g/kWh) was labelled `"hydro_pumped"` and received a lifecycle factor of 24 g/kWh; B14 (Nuclear, 12 g/kWh) was labelled `"solar"` and received 45 g/kWh; B16 (Solar, 45 g/kWh) was labelled `"wind_offshore"` and received 12 g/kWh. All 20 PSR codes (B01–B20) are now mapped correctly according to the official ENTSO-E API documentation (Table 8).
- **CO₂ emission factors corrected** – Several factors were wrong or missing: `coal_gas` (B03, Fossil Coal-derived gas) added at 700 g/kWh; `hard_coal` (B05) added at 820 g/kWh; `oil_shale` (B07) corrected from 650 → 800 g/kWh; `peat` (B08) corrected from 900 → 1150 g/kWh; `waste` (B17) corrected from 300 → 330 g/kWh; `other` (B20) raised from 300 → 400 g/kWh. Stale internal names (`"coal"`, `"wind"`) removed.

### Added
- **CO₂ tab: Generation Mix table** – A new "Generation Mix – most recently fetched hour" section appears in the CO₂ tab. After any backfill the table is populated automatically and shows each fuel type present in the ENTSO-E response with its MW, percentage share, CO₂ factor (g/kWh), and percentage CO₂ contribution. A footer row shows totals and the resulting intensity (g/kWh) for cross-checking. The mix is also logged line-by-line to the Sync tab during each backfill chunk.
- **`FUEL_DISPLAY_NAMES` dict** – All 20 internal fuel keys mapped to human-readable strings including the ENTSO-E B-code (e.g. `"Wind onshore (B19)"`), exported from `entsoe.py` for use in the UI.
- **i18n**: all new `co2.mix.*` UI strings translated into all 9 languages (de, en, es, fr, pt, it, pl, cs, ru).

## 11.2.0 - 2026-03-27
### Added
- **CO₂ tab: two scrollable data tables for value verification** – Two `ttk.Treeview` tables are now displayed below the heatmap strip so the user can inspect the raw imported numbers:
  - *Table 1 – Loaded CO₂ Intensity Values*: shows all stored hourly intensity rows for the active bidding zone (up to 500 rows, newest first). Columns: Date/Time | CO₂ Intensity (g/kWh) | Source.
  - *Table 2 – CO₂ per Device (last 24 h)*: JOIN of `hourly_energy` × `co2_intensity` on `hour_ts`, one row per device per hour. Columns: Date/Time | Device | kWh | CO₂ Intensity (g/kWh) | CO₂ (g). This lets you verify that the calculation `kWh × g/kWh = g CO₂` is correct.
- **CO₂ intensity values reviewed** – The IPCC lifecycle emission factors and the weighted-average calculation in `entsoe.py` are correct. Germany (DE_LU) typically shows 200–450 g/kWh depending on the generation mix; higher values during periods of high lignite/coal dispatch are expected.
- **i18n**: all new UI strings translated into all 9 languages (de, en, es, fr, pt, it, pl, cs, ru).

## 11.1.8 - 2026-03-27
### Fixed
- **CO₂ tab: historical backfill data never displayed** – `_refresh_co2_tab()` always queried only the last 24 hours (`now − 86400 … now`). When the user ran a backfill for a date range in the past (e.g. Feb 1–8) the query returned zero rows and the tab showed "No data", even though the 168 data points were successfully written to the database. The method now falls back to `db.latest_co2_ts(zone)` whenever the 24-hour window is empty, then queries the 24-hour window ending at that timestamp so any historically-imported data is always displayed.

## 11.1.7 - 2026-03-27
### Fixed
- **ENTSO-E HTTP errors no longer show raw HTML in the UI** – When the ENTSO-E API returns an HTTP error (e.g. 503 Service Unavailable), the error response body (which is an HTML page) was appended to the RuntimeError message and displayed verbatim in the status label. The body is now discarded; only the HTTP status code and reason phrase are included (e.g. `"ENTSO-E API HTTP 503: Service Unavailable"`). The test-connection button also maps 502/503/504 errors to a clean `"ENTSO-E API nicht erreichbar (HTTP 5xx)"` message.

## 11.1.6 - 2026-03-27
### Fixed
- **CO₂ tab: `self.db` AttributeError prevents all data display and writes** – The CO₂ mixin and the backfill button in core referenced `self.db`, which does not exist on the App object (the correct accessor is `self.storage.db`). This caused a silent `AttributeError` on every refresh and every backfill write, meaning data was fetched from ENTSO-E successfully but never written to the database, and the tab always showed "No data". Fixed all five occurrences across `co2.py` (4×) and `core.py` (1×). Added a 60-second periodic auto-refresh timer so the tab updates automatically when background data arrives.

## 11.1.5 - 2026-03-26
### Fixed
- **CO₂ tab: still empty when `co2.enabled` not yet saved** – `_refresh_co2_tab()` previously returned early (showing "No CO₂ data") whenever `co2.enabled = false` in the saved config, even if data had just been imported via the "Backfill now" button. The `enabled` flag correctly controls whether the background fetch service runs auto-fetches — it must not prevent already-stored local data from being displayed. The early-exit guard is removed; the method now always queries the local `co2_intensity` table and shows whatever is available.
- **CO₂ tab: wrong zone after backfill without saving settings** – If the user changed the bidding zone in the dropdown and clicked "Backfill now" before pressing "Save Settings", data was stored under the UI zone (e.g. `AT`) but `_refresh_co2_tab()` queried the saved zone (`DE_LU`), returning an empty result. The method now falls back to the zone currently shown in the UI when the saved-config query returns empty.

## 11.1.4 - 2026-03-26
### Fixed
- **CO₂ tab: data not shown after backfill** – After a successful ENTSO-E backfill the CO₂ tab remained empty. The polling loop now triggers a `_refresh_co2_tab()` call 500 ms after the import completes so charts, live value, and summary cards are populated immediately without a manual tab switch.
- **Settings "Live & Preis": vertical scrollbar** – The settings sub-tab containing language, pricing, TOU, auto-sync, appearance, solar, CO₂, live, and web settings had no scrollbar, causing controls to be clipped on small screens. The content is now wrapped in a `Canvas`+`Scrollbar` scrollable frame that also responds to the mouse wheel while hovered.

## 11.1.3 - 2026-03-26
### Fixed
- **"Backfill now" still silent when settings not yet saved** – The button previously called `svc.trigger_now(force=True)` which ran through the background `Co2FetchService`. The service reads the *saved* config, so if the user had not pressed "Save Settings" after entering the token, `_tick()` returned silently (token empty or `enabled=False`). The button now runs its own background thread and reads the token, zone, and backfill-days directly from the current UI fields — identical to how the "Test Connection" button works — so it always uses what is currently on screen.
- **No visibility into backfill progress in the Sync tab** – Backfill activity (start, each ENTSO-E API chunk, received data point count, errors, and final summary) is now written to the Sync tab's log area. `Co2FetchService` gained a `set_log_callback()` hook wired to `_log_sync()` on startup, so the automatic background service also logs there. The manual backfill button logs via `after(0, …)` to keep all Tkinter writes on the main thread.
- **Silent empty-response failures** – `EntsoeClient.fetch_intensity()` now raises an explicit `RuntimeError` if the API returns an empty body, and logs a warning with the first 300 chars of the response if no `GL_MarketDocument` is found, making unexpected ENTSO-E error documents visible in the application log.

## 11.1.2 - 2026-03-26
### Fixed
- **"Backfill now" button did nothing when data already existed** – `trigger_now()` woke the background fetch service, but `_tick()` immediately returned early because it saw `start_ts >= end_ts` (data was current). The button now passes `force=True`, which resets the fetch window back to `backfill_days` ago so the full historical range is re-imported regardless of what is already in the database.
- **Progress bar never advanced during backfill** – The `Co2FetchService` was writing progress updates into a queue, but nothing on the main thread ever read from it. A `_co2_poll_progress()` loop (rescheduled every 400 ms via `after()`) now drains the queue, updates `ttk.Progressbar` value and the status label with "Importing CO₂ data… day N/M", and resets the bar when the import finishes.
- **Fetch errors not shown to the user** – If a backfill chunk failed, `last_error` was set on the service but never surfaced in the UI. The progress-poll loop now reads `last_error` on completion and displays it in both the progress label and the status label so the user can see what went wrong.
- **`co2.status.importing` translation key added** – New i18n key with day/total placeholders translated into all 9 UI languages (de, en, es, fr, pt, it, pl, cs, ru).

## 11.1.1 - 2026-03-26
### Fixed
- **ENTSO-E API: wrong base URL caused DNS resolution error** – The API base URL was incorrectly set to `https://web.api.entsoe.eu/api` (non-existent host), causing "nodename nor servname provided, or not known" errors on every request. Corrected to `https://web-api.tp.entsoe.eu/api`.
- **ENTSO-E API: bidding zone names not mapped to EIC codes** – The `in_Domain` parameter was sent with human-readable zone names (e.g. `DE_LU`) instead of the required EIC codes (e.g. `10Y1001A1001A83F`). Added a complete `_EIC_CODES` mapping for 40 European bidding zones; unknown zones fall back to passing the raw value.

## 11.1.0 - 2026-03-26
### Added
- **ENTSO-E "Test Connection" button** – New button in the CO₂ settings panel (next to "Backfill now"). Sends a minimal test request to the ENTSO-E API with the currently-entered token and bidding zone, then shows a green "Connection successful ✓" or a red error message ("Token invalid", "API unreachable", etc.) inline below the button. Runs in a background thread so the UI stays responsive. Translated into all 9 UI languages (de, en, es, fr, pt, it, pl, cs, ru).
- **CO₂ import progress bar** – A `ttk.Progressbar` with a status label is now displayed at the top of the CO₂ tab while historical data is being backfilled from ENTSO-E. Shows "Importing CO₂ data… Day 7/30" (translated per language) and advances as each 7-day API chunk completes. Resets and clears automatically when the import finishes. Implemented via the existing queue-drain loop to keep all UI updates on the main thread.

## 11.0.0 - 2026-03-26
### Added
- **CO₂ intensity tab** – New "🌿 CO₂" tab showing real-time and historical grid carbon intensity sourced from the [ENTSO-E Transparency Platform](https://transparency.entsoe.eu/).
  - **Live section**: current grid CO₂ intensity in g CO₂/kWh, colour-coded green / yellow / red against configurable thresholds; per-device CO₂ emission rate (g/h) based on current power draw.
  - **24 h intensity chart**: line chart with fill showing the last 24 hours of grid CO₂ intensity; green and dirty threshold lines overlaid.
  - **Summary cards**: total CO₂ emitted today / this week / this month (in kg) with equivalents – car kilometres driven and trees needed per year to offset – calculated from actual device energy consumption.
  - **24 h heatmap strip**: colour-coded hour-by-hour bar showing green → yellow → red intensity profile for the last 24 hours.
- **ENTSO-E API client** (`services/entsoe.py`) – `EntsoeClient` fetches actual generation per production type (DocumentType A75) for any ENTSO-E bidding zone, parses the XML response, and computes weighted-average CO₂ intensity using standard IPCC lifecycle emission factors (lignite 1100, coal 820, gas 490, oil 650, nuclear 12, hydro 4, wind 11, solar 45, biomass 230 g CO₂eq/kWh). Enforces a 62-second inter-request rate limit.
- **Background fetch service** (`Co2FetchService`) – Daemon thread that periodically fetches new intensity data from ENTSO-E, automatically backfills missing historical hours up to the configured number of days, and stores results in the new `co2_intensity` SQLite table.
- **`co2_intensity` database table** – New SQLite table with columns `hour_ts` (primary key), `zone`, `intensity_g_per_kwh`, `source`, `fetched_at`; EnergyDB gains `upsert_co2_intensity`, `query_co2_intensity`, `latest_co2_ts`, and `oldest_co2_ts` helpers.
- **`Co2Config` dataclass** – New configuration block (`co2` key in `config.json`) with: `enabled`, `entso_e_api_token`, `bidding_zone` (default `DE_LU`), `fetch_interval_hours`, `backfill_days`, `show_green_dirty_hours`, `green_threshold_g_per_kwh` (150), `dirty_threshold_g_per_kwh` (400). Fully wired into `load_config` / `save_config`.
- **CO₂ settings section** – New panel in the Settings tab for configuring the ENTSO-E API token (masked input), bidding zone (dropdown with all ENTSO-E zones), fetch interval, backfill days, green/dirty thresholds, and a "Backfill now" button.
- **Internationalisation** – ~30 new translation keys (`co2.*`) added for all 9 UI languages (de, en, es, fr, pt, it, pl, cs, ru).

## 10.7.1 - 2026-03-26
### Fixed
- **Live view: I_N neutral current plot empty on first load** – The `/api/history` endpoint returned historical sparkline data for power, voltage, and current but omitted the `i_n` field. As a result, the neutral current plot appeared flat until new live readings arrived. The field is now included in every history point so the plot is pre-populated from stored data on first page load.

## 10.7.0 - 2026-03-26
### Changed
- **Updates tab: changelog refreshes on every update check** – Clicking "Check for updates" now also re-fetches `CHANGELOG.md` from GitHub in a background thread, so the changelog viewer always shows the latest remote content before the user decides to install an update.

## 10.6.1 - 2026-03-26
### Fixed
- **Voltage detail chart: L1 shown as dashed line** – The dashed-line style was incorrectly applied to the first series (`si===0`) whenever multiple series were present. For voltage (and phase-power / neutral-current) charts the first series is a phase line, not an aggregate total. The dash is now only applied when the chart actually contains a total/aggregate series (power `w` and current `a` with 3-phase data). All three phase voltage lines (L1, L2, L3) are now drawn as solid lines with equal weight.

## 10.6.0 - 2026-03-26
### Changed
- **Live view: removed Phase Power mini-plot** – The per-phase power (W) sparkline in the device card expand section has been removed.
- **Live view: added Neutral Current (I_N) mini-plot** – A new sparkline showing the neutral conductor current (I_N) is now displayed for 3-phase devices. Clicking the plot opens the zoomable detail chart. Colour: purple (#a855f7). Localised label in all 9 UI languages.
- **Voltage detail chart: phases only** – The zoomable voltage detail chart no longer shows the average/total voltage line. Only the three individual phase voltages (L1, L2, L3) are plotted, giving a cleaner view of per-phase voltage balance.

## 10.5.0 - 2026-03-25
### Summary
Consolidation release grouping all features and fixes from v10.3.0–v10.4.0 into a single tagged version so that the auto-updater picks up the full set of improvements.

### Added
- **Live view: clickable mini-plots with zoomable detail chart** – Clicking any sparkline in a device card opens a full-screen detail chart overlay showing all collected live data. 3-phase devices render L1/L2/L3 as separate coloured lines plus a dashed aggregate total. Supports scroll-wheel zoom, pinch-to-zoom on touch screens, drag-to-pan, auto-scaled Y-axis labels (W/kW, V, A), and a zoom-percentage indicator. Dismissable via ✕ button or backdrop tap. Chart updates live with each polling interval.
- **Changelog viewer on Updates tab** – The Updates tab now contains a scrollable changelog section fetched from GitHub (`raw.githubusercontent.com`) at startup in a background thread. Headings are rendered bold; network errors are shown as a localised message. All 9 UI languages (de, en, es, fr, pt, it, pl, cs, ru) have localised strings.
- **Sync progress bar** – A `ttk.Progressbar` and status label appear in the Sync tab during an active sync, filling 0 → 100 % as chunks are downloaded across all devices. The label shows current device index and chunk count (e.g. "Device 2/3 · Chunk 5/12"). Localised in all 9 languages.
- **Per-device invoice PDFs in monthly e-mail** – When "Attach invoice" is enabled, the monthly e-mail attaches a separate PDF invoice for each configured device. If more than one device is configured, the combined invoice is also attached. Applies to both scheduled send and the manual "Send Now" button.

### Fixed
- **Costs tab: missing kWh on week/month device cards** – WOCHE and MONAT cost cards were missing the kWh sub-value; the frontend template was passing an empty string. The API already returned the data; only the rendering was broken.
- **Invoice PDF: overlapping column headers** – Widened the unit-price column; added a coloured header row, alternating row shading, and highlighted totals band for a professional appearance.
- **Critical: broken auto-update check** – GitHub repository URL in `UpdatesConfig` contained a typo (`robeertm` → `robertm`), causing all update checks to silently fail with a 404.
- **Duplicate `groups` field in `AppConfig`** – Field was declared twice; the redundant declaration was removed.
- **Schedule time validation** – `build_shelly_timespec()` now validates hour/minute range and logs a warning on fallback. The schedule editor shows a localised error if the time is invalid.
- **`ShellyHttp.get/post` crash with `-O` flag or `retries < 1`** – Replaced `assert last_err is not None` with an explicit guard to avoid `TypeError` in optimised Python mode.
- **`DemoState.started_at` shared across instances** – Changed from a module-level `time.time()` default to `field(default_factory=time.time)`.
- **Frequency calculation in `database.py`** – `_get("freq_hz")` was evaluated twice per call; refactored to read each CSV column exactly once.
- **PDF report crash on e-mail send** – `_rl_set_fill`/`_rl_set_stroke` unpacked hex colour strings char-by-char into `setFillColorRGB`, producing too many arguments. Added `_hex_to_rgb` helper.
- **Hourly heatmap day order** – Y-axis inverted so Monday appears at top and Sunday at bottom.

## 10.4.0 - 2026-03-25
### Added
- **Changelog viewer on Updates tab**: The Updates tab now contains a scrollable changelog section at the bottom. On startup it fetches `CHANGELOG.md` directly from the GitHub repository (`raw.githubusercontent.com`) in a background thread and renders it with minimal markdown formatting (headings `#`/`##`/`###` are displayed bold). Network errors (offline, timeout) are shown as a localised error message. All 9 UI languages (de, en, es, fr, pt, it, pl, cs, ru) have localized strings for the section title, loading indicator, and error state.
- **Live view: clickable mini-plots with zoomable detail chart** – Clicking any sparkline in the device card (power W, voltage V, current A, or phase-power) now opens a full-screen detail chart overlay. The detail chart shows all collected live data points for the selected metric and — for 3-phase devices — renders each phase (L1/L2/L3) as a separate coloured line alongside the aggregate total (dashed). The chart supports:
  - **Scroll-wheel zoom** on desktop (zoom in/out centred on the cursor position)
  - **Pinch-to-zoom** on mobile/touch screens
  - **Drag to pan** (mouse or single-finger touch) across the time axis
  - Y-axis with auto-scaled labels (W/kW, V, A), time labels on the X-axis, and a zoom-percentage indicator when zoomed in
  - A colour-coded legend (Total + L1/L2/L3) for 3-phase devices
  - Dismiss by clicking the ✕ button or tapping the backdrop
  The chart updates live as new data arrives from the polling interval.

## 10.3.3 - 2026-03-25
### Fixed
- **Costs tab: missing kWh on week/month device cards** – Per-device cost cards for WOCHE and MONAT showed only the euro amount with no kWh sub-value, while HEUTE and PROGNOSE showed kWh correctly. The frontend template was passing an empty string instead of `fmt(d.week_kwh,3,'kWh')` and `fmt(d.month_kwh,3,'kWh')`. The API already returned these fields; only the rendering was broken.

## 10.3.2 - 2026-03-25
### Added
- **Sync progress bar**: A `ttk.Progressbar` and a status label now appear in the Sync tab while a sync is running. The bar fills from 0 → 100 % as chunks are downloaded across all devices, and the label shows the current device index and chunk count (e.g. "Device 2/3 · Chunk 5/12"). Both are reset to empty once the sync completes. All 9 UI languages (de, en, es, fr, pt, it, pl, cs, ru) have localized strings for the progress states.

### Fixed
- **Invoice PDF: overlapping column headers** – Widened the unit-price column in `export_pdf_invoice()` so that "Einzelpreis"/"Unit Price" no longer overlaps adjacent columns. Added a colored table-header row, alternating row shading, and a highlighted totals band for a professional invoice appearance.

## 10.3.1 - 2026-03-25
### Fixed
- **`ShellyHttp.get/post` crash with Python `-O` flag or `retries < 1`** – Both retry loops used `assert last_err is not None` before `raise last_err`. Python's optimised mode (`-O`) silently disables assertions, turning `raise last_err` into `raise None` which throws `TypeError: exceptions must derive from BaseException` instead of the original network error. The `assert` is replaced with an explicit `if last_err is None: raise RuntimeError(...)`. The loop bounds now also guard against a misconfigured `retries=0` via `max(self.cfg.retries, 1)`.
- **`DemoState.started_at` shared across all instances** – The dataclass field `started_at: float = time.time()` evaluated `time.time()` once at *module import time*, so every `DemoState` instance shared the same timestamp (the time the app was first launched). Changed to `field(default_factory=time.time)` so each instance records its own creation time.
- **Frequency calculation in `database.py` called `_get("freq_hz")` twice** – The previous inline conditional expression evaluated `_get("freq_hz")` twice (once for the truthiness check, once for the value) and used an unreadable immediately-invoked lambda. Refactored to a readable three-line pre-computation that reads each CSV column exactly once.

## 10.3.0 - 2026-03-25
### Fixed
- **Critical: broken auto-update check** – GitHub repository URL in `UpdatesConfig` contained a typo (`robeertm` instead of `robertm`), causing all update checks to silently fail with a 404. The default value in `config.py` is now correct.
- **Duplicate `groups` field in `AppConfig`** – The `groups: List[DeviceGroup]` field was declared twice in the `AppConfig` dataclass (once after `devices` and again after `anomaly`). Python silently kept only the last occurrence, misplacing the field in the serialisation order. The redundant second declaration has been removed.
- **Schedule time validation** – `build_shelly_timespec()` in `scheduler.py` now validates that hour (0–23) and minute (0–59) are in range and logs a warning when falling back to `00:00`, instead of silently producing a wrong cron expression. The schedule editor dialog also validates the time inputs before saving and shows a localised error message (`sched.msg.invalid_time`) if the entered value is not a valid `HH:MM` time.
- **i18n: added `sched.msg.invalid_time`** – New translation key added for all 9 supported languages (de, en, es, fr, pt, it, pl, cs, ru).

## 10.2.0 - 2026-03-24
### Added
- **Per-device invoice PDFs in monthly e-mail**: When "Attach invoice" is enabled, the monthly e-mail now generates and attaches a separate invoice PDF for each configured device (e.g. House, Garage, Office). Each per-device invoice contains only the energy consumption and cost for that single device. If more than one device is configured, the combined invoice (all devices as individual line items) is also attached. This applies to both the scheduled monthly send (`_email_summary_tick`) and the manual "Send Now" button (`_email_send_monthly_now`).

## 10.1.2 - 2026-03-24
### Fixed
- **PDF report crash on e-mail send**: `_rl_set_fill` / `_rl_set_stroke` unpacked hex color strings (e.g. `"#1E6B8C"`) char-by-char into `setFillColorRGB()`, producing 7 + self = 8 arguments and crashing with *"takes from 4 to 5 positional arguments but 8 were given"*. Helper `_hex_to_rgb` added; both helpers now convert hex strings to `(r, g, b)` float tuples before calling reportlab.
- **Hourly heatmap day order (App)**: Weekdays in the weekday × hour heatmap were rendered bottom-to-top (Monday at the bottom, Sunday at the top). Y-axis is now inverted (`ylim(7, 0)`) so Monday appears at the top and Sunday at the bottom.

## 10.1.1 - 2026-03-24
### Fixed
- **PDF reports more compact**: All matplotlib figures reduced to 6×2.5 inches at 110 DPI (mini-charts 5×1.5 / 100 DPI). The overview plot is now embedded directly on page 1 if space permits; per-device mini-plots appear two per row in a 2-column layout.
- **Monthly report – most/least expensive day incorrect**: Daily consumption calculated from power data (`resample("D").mean() / 1000` → kW only, not kWh) corrected. Calculation now uses correct hourly integration: `resample("h").mean() / 1000` → kWh/h, then daily sum.
- **"Attach invoice" not saved**: `email_monthly_invoice_enabled` was missing from `save_config` and `load_config` in `config.py` – the checkbox was always reset to unchecked after restart.
- **Typo**: "Rechnung anhangen" → "Rechnung anhängen" in the German translation.

## 10.1.0 - 2026-03-24
### Added
- **Massively expanded daily report (PDF)**
  - 6 KPI tiles instead of 3: total kWh, cost, CO₂, average power, peak power, peak hour
  - Comparison to the previous day **and** the same weekday of the previous week (with colour-coded +/-%)
  - Expanded device breakdown table with share column (%)
  - Highlights / lowlights: top consumer, most efficient consumer, hour with highest and lowest consumption
  - Page 2: **Stacked 24h bar chart** (each device in its own colour with legend)
  - From page 3: **Per-device section** with mini 24h bar chart, operating hours, Min/Avg/Max kWh/h, average and peak power, share of total consumption

- **Massively expanded monthly report (PDF)**
  - 6 KPI tiles: total kWh, cost, CO₂, daily average, most expensive day, cheapest day
  - Comparison to previous month with absolute and percentage delta
  - **Weekday vs. weekend** average comparison
  - Page 2: **Stacked daily bar chart** (all devices)
  - From page 3: **Per-device section** with mini daily bar chart, Min/Avg/Max kWh/day, trend (Rising/Falling/Stable)
  - Last page: **Top-5 consumer ranking** as a horizontal bar chart + ranking table with kWh and share

- **Improved invoice (PDF)**
  - Sender and recipient are now fully read from `BillingConfig` (name, address, VAT ID, email, IBAN, BIC)
  - Invoice number in format `{Prefix}-{YYYY}-{MM}-001` (configurable via `billing.invoice_prefix`)
  - Due date is automatically calculated from `billing.payment_terms_days`
  - Logo from `billing.invoice_logo_path` is embedded when present

- **New matplotlib helper functions** in `services/export.py`
  - `_DEVICE_PALETTE` – consistent 10-colour palette; each device always gets the same colour
  - `_make_stacked_hourly_chart` / `_make_stacked_daily_chart` – stacked bar charts
  - `_make_device_mini_chart_hourly` / `_make_device_mini_chart_daily` – small per-device charts
  - `_make_top5_bar_chart` – horizontal top-5 bar chart
  - `_draw_device_table_enhanced` – device table with share column (%)

- **`EmailReportData`** extended by 11 new fields (all optional/with defaults for backward compatibility):
  `per_device_hourly`, `per_device_daily`, `peak_hour`, `avg_power_w`, `peak_power_w`,
  `prev_same_weekday_kwh`, `weekday_avg_kwh`, `weekend_avg_kwh`,
  `best_day_date`, `best_day_kwh`, `worst_day_date`, `worst_day_kwh`

- **`_build_email_report_data`** now complete: collects per-device hourly and daily time series,
  computes peak hour, weekday/weekend split, best/worst day, and comparison to the same
  weekday of the previous week

## 10.0.3 - 2026-03-24
### Fixed
- **Critical: PDF email attachments always empty** — `_build_email_report_data` and
  `_build_report_totals` called `storage.query_samples()`, a method that does not
  exist on the `Storage` class. This raised an `AttributeError` for every device,
  which was silently swallowed by the per-device `except` handler, leaving `totals`
  permanently empty and producing blank PDFs. Both functions now correctly call
  `storage.read_device_df(dev.key, start_ts=..., end_ts=...)`.
- **Silent exception swallowing** — PDF generation errors in `_email_summary_tick`,
  `_email_send_daily_now`, and `_email_send_monthly_now` are now logged as
  warnings (with full traceback) instead of being silently dropped.
- **0-byte attachment guard** — email attachment lists now only include files where
  `stat().st_size > 0`, preventing empty-file attachments when PDF generation is
  skipped due to missing data.

### Added
- **Detailed logging** throughout the PDF-report pipeline:
  `_build_email_report_data`, `_build_report_totals`, and all email-send workers
  now emit `INFO`-level log lines showing device count, kWh totals, PDF file size,
  and any per-device errors — making future diagnosis straightforward.

## 10.0.2 - 2026-03-24
### Fixed
- **`_build_email_report_data` DB column names** — corrected column lookup order so
  `total_power` and `energy_kwh` (the actual DB columns) are tried before the
  non-existent aliases `total_act_power` / `total_act` / `energy_wh`.

## 10.0.1 - 2026-03-24
### Fixed
- **`_build_report_totals` DB column names** — same column-name correction applied
  to the fallback totals builder used when the rich `EmailReportData` path is
  unavailable.

## 10.0.0 - 2026-03-24
### Added
- **Rich email PDF reports** — daily and monthly email reports now contain real
  data, professional layout and embedded matplotlib charts instead of a bare header:
  - **Daily report**: Branded header band, KPI tiles (kWh / cost / CO₂), device
    breakdown table with alternating rows, % comparison to previous day, and a
    full-page 24-hour bar chart (peak hour highlighted in amber).
  - **Monthly report**: Same KPI tiles, device table, % comparison to previous
    month, top-5 consumer ranking with share percentages, and a full-page
    per-day bar chart for the entire month.
  - All charts are rendered via matplotlib (Agg backend) and embedded as PNG in
    the PDF; a fallback to the existing plain-text summary is used if no device
    data is available.
- **Invoice attachment for monthly email** — a new checkbox
  "Rechnung anhängen" / "Attach invoice" in the e-mail settings lets users
  automatically attach an A4 invoice PDF (consumption × price, optional VAT)
  to the monthly summary e-mail.
- **`EmailReportData` dataclass** in `services/export.py` carries the full
  time-series payload (hourly kWh, per-day kWh, previous-period totals, CO₂,
  VAT) used by the two new export functions
  `export_pdf_email_daily` / `export_pdf_email_monthly`.
- **`_build_email_report_data`** method in core builds the rich payload
  (including hourly/daily pandas resampling and prior-period comparison) that
  is passed to the new PDF functions.

## 9.11.0 - 2026-03-23
### Improved
- **Web live view: key-value label alignment** — device detail cards now use
  `minmax(100px, auto)` for the label column and a `min-width: 100px` on `dt`
  elements, so U / I / cos φ / Grid frequency / Phase 1–3 / I_N all start at the
  same horizontal position regardless of label length.
- **Web compare tab: smart granularity per preset** — quick-preset buttons now
  automatically select the most meaningful granularity:
  Month → daily (unchanged), Quarter → weekly, Half Year → monthly, Year → monthly.
  A new "Weekly" option is also available in the granularity dropdown for manual use.
  The backend (`_cmp_align_weekly`) aggregates daily data into ISO calendar weeks
  (labelled W01, W02 …).

## 9.10.0 - 2026-03-23
### Fixed
- **Web heatmap: revert GitHub-style redesign** — the yearly calendar heatmap
  is restored to the original Green→Yellow→Red gradient (`ratioColor`) with the
  classic CSS grid classes (`.hm-grid` / `.hm-week`). The GitHub-style 4-level
  green palette, day-of-week side labels, and 13 px cell cap introduced in
  v9.9.0 are removed.
- **Web: bottom navigation still covers content** — `#panes` padding-bottom
  increased to `calc(120px + env(safe-area-inset-bottom, 0px))` to provide
  ample clearance below the last card even on fully-expanded views and iPhone
  home-indicator devices.

### Added
- **Web costs tab: CO₂ display** — each device card in the Costs tab now shows
  a CO₂ section (Today / Week / Month / Forecast in kg) calculated from the
  configured CO₂ intensity (g/kWh, default 380 g/kWh). The section is hidden
  when CO₂ intensity is set to 0.

## 9.9.0 - 2026-03-23
### Fixed
- **Web heatmap: unprofessional appearance** — yearly calendar heatmap redesigned to
  GitHub contribution style: cells capped at 13 px, 3 px gap, 4-level green colour
  scale (light→dark), day-of-week labels (Mon/Wed/Fri) on the left side, and month
  labels correctly aligned with week columns.
- **Web: bottom navigation covers content** — `#panes` padding-bottom now uses
  `calc(80px + env(safe-area-inset-bottom, 0px))` so the last card is never
  hidden under the nav bar even on iPhone with a home indicator.
- **Web compare tab: daily chart never rendered** — the frontend was checking
  `data.series_a` while the API returns `data.values_a`; fixed field names so
  daily/monthly granularity bar charts now display correctly.
- **Web compare tab: "Month" preset shows only total** — the Month quick-preset
  button now automatically uses `gran=daily` so all individual days are plotted
  (matching the desktop app behaviour).
- **Web live view: I_N (neutral current) missing for 3-phase devices** — the
  `/api/state` response now includes the computed neutral current `i_n`; the
  device detail card shows an "I_N (N)" row (in Ampere) when the value is
  non-zero, and it updates on every polling tick.

## 9.8.0 - 2026-03-23
### Fixed
- **Web live view: detail values not updating** — voltage (U), current (I),
  cos φ, frequency, and per-phase values in the expanded device card were only
  rendered once when the card was built; `updateDeviceCard` now refreshes all
  detail `<dd>` elements on every polling tick.
- **Web mobile: slow initial page load** — HTML pages are now served
  gzip-compressed (~75 % smaller) when the browser sends `Accept-Encoding: gzip`.
  All three HTML endpoints (live dashboard, control, plots) are pre-compressed
  at startup for zero per-request overhead.

## 9.7.0 - 2026-03-23
### Fixed
- **Web live view: historical sparklines on page load** — on first open, the
  live tab now pre-populates sparklines with all data already collected by the
  app instead of starting from empty buffers. A new `/api/history` endpoint
  returns the full in-memory store (server-side UNIX timestamps converted to
  JS milliseconds) and `loadHistory()` merges it into `sparkData` on startup.
  The 1 min–2 h timescale selector buttons work correctly with this pre-loaded
  data.
- **Web costs: wrong forecast value** — the "Forecast (Month)" metric card was
  displaying `year_eur` (full-year energy cost) instead of the correctly
  calculated `proj_eur` (month-to-date ÷ elapsed days × days-in-month). The
  formula now matches the desktop app. The card also shows the projected kWh as
  sub-label.
- **Web calendar heatmap: no border + overflow on iPhone** — the yearly calendar
  is now wrapped in a `.card` div (matching the hourly heatmap styling). Cell
  size is calculated from both viewport width and height so both the calendar
  and hourly heatmap fit within the visible area on narrow/short screens
  (e.g. iPhone) without a horizontal or vertical scroll bar. Minimum cell size
  reduced from 10 px to 4 px (calendar) and from 12 px to 8 px (hourly) to
  accommodate small displays.
- **Web heatmap: month labels no longer clipped on narrow screens** — portrait/mobile
  views (width < 500 px) now show single-character month abbreviations (J F M A …)
  via `Intl.DateTimeFormat month:'narrow'`; landscape/wide views show three-character
  abbreviations (Jan Feb Mär …) via `month:'short'`. Font size is reduced to 8 px
  on narrow screens. Span `overflow` changed from `hidden` to `visible` so labels
  can spill into the adjacent empty week columns without being clipped.

### Changed
- **Web compare tab: bar chart always shown** — the comparison bar chart now renders
  for all granularities including "Total". When only aggregate totals are available
  (no per-day/month series), a simple side-by-side 2-bar chart is drawn showing
  Device A vs Device B using the existing canvas `drawBars` function. The daily and
  monthly granularities continue to render the full time-series chart as before.

## 9.5.0 - 2026-03-22
### Fixed
- **Desktop heatmap: empty cells for missing/future days** — calendar cells that
  have no recorded data (including future dates) are now rendered in light grey
  (`#E0E0E0` day / `#333333` night) instead of solid green. Only days with
  actual consumption data participate in the green→yellow→red colour scale.
  Previously all missing days received value `0.0` and were painted green
  (the lowest-value colour of `RdYlGn_r`), making it impossible to distinguish
  "zero real consumption" from "no data yet". Fixed by keeping those cells as
  `NaN` in the grid and applying `cmap.set_bad()` to a neutral grey.

## 9.4.0 - 2026-03-22
### Changed
- **Web dashboard: full i18n** — the web/mobile dashboard now renders in the
  same language as the desktop app (de, en, es, fr, pt, it, pl, cs, ru).
  All UI texts are translated: tab navigation labels, button tooltips, device
  card labels (Voltage/Current/Freq/Phase), costs summary (Today/Week/Month/
  Year/Year proj.), heatmap hourly pattern title, solar field names, compare
  controls and presets, anomaly status badges, loading/error/info messages,
  and the device-order modal header and Done button.
- **Heatmap: locale-aware month and day labels** — month abbreviations and
  weekday names in the calendar and hourly heatmap are now generated via
  `Intl.DateTimeFormat` using the active language instead of being hardcoded
  in German/English.
- **i18n: new `web.dash.*` translation keys** — 31 new keys added for all 9
  supported languages covering the dashboard UI texts listed above.

## 9.3.0 - 2026-03-22
### Changed
- **Heatmap: responsive layout** — both the yearly calendar and the hourly
  pattern (weekday × hour) heatmap now calculate cell size dynamically from the
  available viewport width (`window.innerWidth`), so cells fill the full screen
  on desktop and shrink gracefully on mobile instead of being cut off.
- **Heatmap: full 0–23 hours visible** — the hourly pattern previously appeared
  to show only 0–12 because the fixed 26 px cell width overflowed the card
  without a visible scrollbar on narrow screens; cells are now sized to fit all
  24 columns within the available pane width (minimum 12 px per cell).
- **Heatmap: reduced hour labels** — hour axis now labels every 3rd hour
  (0, 3, 6, 9, 12, 15, 18, 21) instead of every hour, reducing label clutter.
- **Heatmap: German weekday abbreviations** — day labels changed from English
  (Mo/Tu/We…) to German (Mo/Di/Mi/Do/Fr/Sa/So).
- **Heatmap: unified design** — calendar and hourly pattern share the same
  green→yellow→red gradient, 2 px border-radius, `var(--chipbg)` background for
  empty cells, and responsive cell-sizing logic.
- **Heatmap: re-renders on window resize** — a debounced `resize` listener
  reloads the heatmap when the pane is active, keeping the layout correct after
  orientation changes or browser window resizes.

## 9.2.0 - 2026-03-22
### Added
- **Live tab: time scale selector** — six buttons (1min, 5min, 15min, 30min,
  1h, 2h) above the device grid let users choose the visible history window.
  All data points are kept in a JS ring-buffer of up to 2 hours
  (`MAX_HIST_PTS = ceil(7200000 / REFRESH_MS)`); switching scale immediately
  redraws the sparklines using only the points within the selected window.
- **Live detail view: voltage & current sparklines** — expanding a device card
  now shows two additional mini-sparklines (amber for voltage, green for
  current) using a relative y-axis so small fluctuations are visible.
- **Live detail view: phase power sparkline** — three-phase devices additionally
  show a multi-line sparkline (red/blue/green per phase) for per-phase active
  power in the expanded detail section.
### Fixed
- **Device labels not updating after reorder** — after changing device order in
  the Settings modal, the live grid was rebuilt in-place by DOM position without
  re-rendering card content, so device names and power values stayed associated
  with the wrong cards until a full page reload.  `renderLive` now compares the
  first card's DOM id against the expected first device key and triggers a
  complete grid rebuild whenever the order has changed.

## 9.1.0 - 2026-03-22
### Added
- **Live tab: device order & visibility settings** — gear icon (⚙) in the
  header opens a modal panel listing all devices.  Each row has a visibility
  checkbox and ▲/▼ buttons to reorder.  Preferences are persisted in
  `localStorage` (`device_order`, `hidden_devices`) and applied on every live
  render.
### Fixed
- **Web heatmap calendar blank** — the `/api/heatmap` endpoint returns
  `calendar` as a list of `{date, value}` objects, but the JS was reading
  `data.daily` (undefined), so the calendar never rendered.  The frontend now
  converts `data.calendar` into a date-keyed dict before building the grid.
### Changed
- **Heatmap colors green→yellow→red** — web heatmap (calendar and hourly
  pattern) now uses a green→yellow→red gradient (`ratioColor` helper) instead
  of the previous blue scale, giving an intuitive low→high energy indication.
- **Desktop heatmap colormap** — both the calendar and hourly-pattern plots in
  the Tkinter UI now use `RdYlGn_r` (reversed Red-Yellow-Green, i.e. green for
  low, red for high) instead of `YlOrRd`.

## 9.0.3 - 2026-03-21
### Fixed
- **Web UI STILL broken after v9.0.2** — A second `\'`-escaping bug existed in
  `initCompare()`.  The preset buttons were built with:
  `onclick="loadComparePreset(\'' + p[0] + '\')"`.
  Again, `\'` in a Python `"""..."""` string is just `'`, so the rendered JS
  contained `loadComparePreset('' + p[0] + '')` — two adjacent string literals
  without a concatenation operator, which is a JS syntax error.  Because any
  syntax error in a `<script>` block aborts the entire block, the page stayed
  completely non-interactive (same symptom as the v9.0.2 bug).
  Fixed by doubling the backslash (`\\'` → stored `\'`) so the backslash
  survives Python's escape processing and JavaScript sees a properly escaped
  single-quote inside the string.

## 9.0.2 - 2026-03-21
### Fixed
- **Web UI completely blank (all tabs)** — A JavaScript syntax error in the
  BOOT IIFE prevented the entire main script block from executing.  In the
  Python `"""..."""` template string, `\'` is a plain single-quote `'`, so
  the querySelector call rendered as `'.nav-btn[onclick*='' + last + '']'` —
  two adjacent string literals with no operator, which is invalid JS.
  Fixed by using double quotes inside the CSS attribute selector:
  `'.nav-btn[onclick*="' + last + '"]'`.  With the script broken, only the
  hard-coded `class="pane active"` on the Live pane was visible; it was empty
  because `startLive()` / `tick()` never ran, and all other tabs stayed
  `display:none` because `switchPane()` was undefined.

## 9.0.1 - 2026-03-21
### Fixed
- **Live tab blank** — `/api/state` was returning the raw `LiveStateStore`
  snapshot (`{device_key: [...points]}`), but the v9 JS frontend expects
  `{devices: [{key, name, power_w, today_kwh, ...}]}`.  The server-side
  handler now transforms the snapshot into the correct device-list format,
  populating `power_w`, `today_kwh`, `cost_today`, `voltage_v`, `current_a`,
  `pf`, `freq_hz`, `phases` (multi-phase Shellys), and `appliances` (NILM
  chip labels as strings).
- **NILM appliance chips** — appliance objects `{icon, id, conf}` are now
  serialised as `"<icon> <id>"` strings before being sent to the browser, so
  they display correctly instead of `[object Object]`.

## 9.0.0 - 2026-03-21
### Added
- **Complete Web UI rewrite (major).** The mobile web dashboard is now a
  full-featured single-page app (SPA) with a bottom navigation bar matching
  all tabs of the desktop application.
- **6 tabs in the web UI:** Live, Costs, Heatmap, Solar, Comparison, Anomalies.
- **Live tab** — real-time device cards with color-coded power (green/yellow/red),
  sparkline canvas chart, collapsible detail rows (voltage, current, cos φ),
  and NILM appliance chips.  Freeze button in the header.
- **Costs tab** — cost overview per device with today/week/month/year and
  monthly projection, powered by the existing `/api/costs` endpoint.
- **Heatmap tab** — interactive GitHub-style calendar heatmap (daily kWh/€)
  and a 7 × 24 weekday × hour heatmap with color scale.  Device, unit and
  year selectors.  Touch tooltip on tap.  New `/api/heatmap` endpoint.
- **Solar tab** — PV dashboard with feed-in, grid consumption, self-consumption,
  autarky %, PV production, feed-in revenue and cost savings.  Period buttons
  (Today/Week/Month/Year).  New `/api/solar` endpoint.
- **Comparison tab** — period-over-period comparison with device A/B selectors,
  date range inputs, unit (kWh/€) and granularity (total/daily/monthly) toggles,
  quick-preset buttons (month/quarter/half-year/year), grouped Canvas bar chart
  and delta display.  New `/api/compare` endpoint.
- **Anomalies tab** — lists detected anomaly events (type, device, timestamp,
  sigma, description) with an enabled/disabled status badge.  New `/api/anomalies`
  endpoint.
- **Mobile-first design** — bottom navigation bar with emoji icons, min 44 px
  touch targets, works at 360 px viewport width, responsive grid up to 1920 px.
- **Dark/Light mode toggle** with `prefers-color-scheme` auto-detection and
  localStorage persistence.
- **Smooth tab transitions** with fade-in animation.
- **New i18n keys** (`web.tab.*`, `web.no_data`) added for all 9 supported
  languages (de, en, es, fr, pt, it, pl, cs, ru).
- **4 new backend API routes:** `/api/heatmap`, `/api/solar`, `/api/compare`,
  `/api/anomalies` — all backed by new action handlers in `LiveWebMixin`.
### Fixed (from 8.6.1/8.6.2)
- **Heatmap weekday × hour: values too high (sum → mean).** Aggregation
  changed from `sum()` to `mean()` so each cell shows the average hourly
  consumption for that weekday/hour combination.
- **Version string sync.** All version strings now consistently reflect 9.0.0.

## 8.6.2 - 2026-03-21
### Fixed
- **Version inconsistency resolved.** `__init__.py` was not updated when 8.6.1
  was tagged, causing the app to display 8.6.0 while the updater reported 8.6.1
  as available. All version strings (`__init__.py`, `pyproject.toml`) are now
  in sync at 8.6.2.

## 8.6.1 - 2026-03-21
### Fixed
- **Heatmap weekday × hour: values too high (sum → mean).** The
  weekday × hour heatmap was accumulating all occurrences of a given
  weekday/hour slot across the entire year (≈52 values per cell) using
  `sum()`, producing values like 8.8 kWh for a single hour instead of a
  typical ~0.2 kWh.  Changed the aggregation to `mean()` so each cell now
  shows the average hourly consumption for that weekday/hour combination.

## 8.6.0 - 2026-03-21
### Added
- **Heatmap tooltips.** Hovering over a day cell in the calendar heatmap now
  shows a floating tooltip with the date and consumption value
  (e.g. "15.01.2026: 12.4 kWh").  Hovering over a cell in the
  weekday × hour heatmap shows the weekday, time slot, and value
  (e.g. "Mon 14:00–15:00: 0.8 kWh").  Tooltips are rendered as a dark
  `tk.Label` floating over the canvas and disappear when the cursor
  leaves the plot area.  Both heatmaps use `mpl_connect
  ('motion_notify_event')` for zero-dependency, always-available
  interactivity.

## 8.5.0 - 2026-03-21
### Changed
- **Auto-refresh on tab switch.** The Costs, Heatmap, Solar, and Comparison tabs
  now reload their data automatically whenever the user switches to them.  The
  manual "Refresh" button has been removed from all four tabs.
- **Auto-refresh on control changes (Comparison tab).** Changing device, date
  range (Return / focus-out), unit, or granularity in the Comparison tab now
  triggers an immediate refresh without requiring a button click.
- **Heatmap fills full window height.** Both heatmap plots (calendar and
  weekday × hour) now expand to fill all available vertical space instead of
  being constrained to fixed heights in a scrollable container.  The scroll
  container has been replaced with a simple frame so the two sections split the
  available height equally and scale with window resize.

## 8.4.0 - 2026-03-20
### Added
- **NILM appliance detector in the web dashboard.** The live web view now shows
  the top-3 appliance candidates (icon, localised name, confidence dot + %) below
  the metrics grid for each device, updated on every poll cycle.  Uses the same
  `identify_appliance()` engine as the desktop Live tab.  Results are hidden in
  compact/sparkline mode and localised in all 9 supported languages.
- **`/api/state` extended with `_appliances` field.**  The JSON response now
  includes a top-level `_appliances` object (`{device_key: [{icon, id, conf}]}`)
  computed from the latest power reading, so the web frontend can render hints
  without an extra round-trip.

## 8.3.0 - 2026-03-20
### Added
- **NILM-Light appliance detector.** The Live tab now shows a "Possible devices"
  line below the realtime metrics for each Shelly device.  On every new sample the
  current total power (W) is matched against a built-in database of 25 household
  appliance power signatures and the top-3 candidates are displayed with a
  confidence indicator (🟢 ≥ 70 %, 🟡 ≥ 40 %, 🔴 < 40 %) and percentage score.
  A small hint line reminds the user that results are estimates based on typical
  power ranges.
- **`ApplianceSignature` dataclass and `identify_appliance()` function**
  (`src/shelly_analyzer/services/appliance_detector.py`).  Includes signatures for
  refrigerator, freezer, washing machine, tumble dryer, dishwasher, oven, hob,
  microwave, kettle, coffee machine, toaster, iron, hair dryer, vacuum cleaner,
  EV charger, heat pump, instantaneous water heater, hot water boiler, TV,
  PC/gaming PC, laptop, router, LED lighting, air conditioner, and fan.
  Confidence is computed as a linear falloff from the centre of each appliance's
  power range, with a ±5 % tolerance zone at the boundaries (fixed confidence 0.25).
- **i18n for all 9 languages.** New translation keys `live.appliance.title`,
  `live.appliance.hint`, and `appliance.{id}.name` (25 device names) added for
  de, en, es, fr, pt, it, pl, cs, ru.

## 8.2.0 - 2026-03-20
### Added
- **Version history in updater.** The Settings → Updates tab now shows the last 10 GitHub
  releases in a scrollable list. The currently installed version is marked with a "(current)"
  indicator. Users can select any version and click "Install selected version" to install it,
  enabling rollbacks to older releases when needed.
- **Downgrade / reinstall warnings.** Selecting a version older than the installed one shows
  an orange warning. Selecting the currently installed version shows an informational note.
  Versions without a downloadable asset show a "no download available" notice and disable
  the install button.
- **`fetch_releases()` in updater service.** New function queries
  `GET /repos/{owner}/{repo}/releases?per_page=10`, picks the correct platform ZIP asset
  (macOS / Windows / Linux), and returns a typed `ReleaseEntry` list.
- **i18n for all 9 languages.** New translation keys for the version-history section
  (`updates.versions_title`, `updates.current_version`, `updates.current_indicator`,
  `updates.install_selected`, `updates.version_list_loading`, `updates.version_list_empty`,
  `updates.downgrade_warning`, `updates.reinstall_note`, `updates.no_asset_warning`) added
  for de, en, es, fr, pt, it, pl, cs, ru.

## 8.1.1 - 2026-03-20
### Fixed
- **Anomaly detection state not persisted across restarts.** `save_config()` was missing
  the `"anomaly"` key entirely — the enabled flag, notification channels (Telegram / Webhook
  / E-Mail), and all detection parameters were never written back to `config.json`, so every
  restart reset them to defaults.
- **Anomaly detection not auto-started on app launch.** When the detection had been enabled
  during a previous session it is now automatically triggered 500 ms after the Anomalies tab
  is built, matching the persisted `enabled` state.

## 8.1.0 - 2026-03-20
### Added
- **Compare tab: quick-compare buttons.** Four one-click presets ("This vs. Last Month",
  "This vs. Last Quarter", "This vs. Last Half-Year", "This vs. Last Year") automatically
  fill both period A and B date fields and immediately trigger a refresh. Translatable
  labels added for all 9 supported languages (de, en, es, fr, pt, it, pl, cs, ru).
### Fixed
- **Compare tab: EUR unit conversion used wrong config field.** `_refresh_compare` read
  `pricing.price_per_kwh` (non-existent attribute, always resolved to 0.0) instead of
  `pricing.electricity_price_eur_per_kwh`, causing all € values to show as 0.00.

## 8.0.5 - 2026-03-20
### Fixed
- **Compare tab: blank chart when DB range query returns empty rows.**
  `Storage.read_device_df()` fell through to the legacy CSV path whenever a date-range query
  returned zero rows — even for devices fully stored in SQLite.  Because no CSV files exist
  post-migration, the fallback raised `ValueError`, which `_cmp_load_daily` silently swallowed,
  yielding an empty result dict and zero-height bars.
  Fix: when `has_data(device_key)` is True, always return the DataFrame from the DB (empty or
  not) without touching the CSV path.

## 8.0.4 - 2026-03-20
### Fixed
- **Comparison tab showed no data (remaining cases).** `_cmp_load_daily` relied solely on
  `read_device_df` with a timestamp range filter. When `query_samples` returned an empty
  DataFrame for the selected period (e.g. `hourly_energy` / `samples` mismatch), the code
  silently fell back to the CSV path, found no CSV files, caught the `ValueError`, and
  returned `{}` — causing invisible zero-height bars. Fixed by:
  1. **Primary path now uses `query_hourly`** — the pre-aggregated `hourly_energy` table
     is always populated during sync, stores integer `hour_ts` (no datetime-conversion
     issues), and its `kwh` column is guaranteed by `COALESCE(SUM(energy_kwh), 0)`.
  2. **Raw-samples fallback retained** for very old databases where `hourly_energy` was
     never rebuilt.
  3. **Explicit "no data" message** shown in the chart when both periods return empty
     results, instead of invisible zero-height bars.
  4. **Exception logging upgraded** from `logger.debug` to `logger.warning` with full
     traceback in `_cmp_load_daily`, `_refresh_compare`, and `_draw_compare_chart`.

## 8.0.3 - 2026-03-20
### Fixed
- **Responsive tab scaling (Costs, Heatmap, Solar):** Charts in these tabs now correctly rescale when the window is resized. Previously, plots were only rendered at their initial size and did not adapt to window size changes.

## 8.0.2 - 2026-03-20
### Fixed
- **Heatmap: both plots rendered almost entirely yellow** — caused by monthly-aggregate
  rows (containing the full monthly kWh total, e.g. 150 kWh) being mixed into the same
  DataFrame as raw per-sample rows (~0.001 kWh each) by `query_samples()`. The single
  outlier value dominated the colour scale (`vmax ≈ 150`), collapsing all other cells
  to yellow. The weekday×hour heatmap additionally showed exactly one isolated bright
  cell (the month with the highest total mapped to a single weekday/hour bucket).
  **Fix:** `_heatmap_load_df()` now reads from the `hourly_energy` table (per-hour kWh,
  correct scale) instead of calling `read_device_df()`. For compressed historical years
  where hourly data has been purged by the retention policy, it falls back to
  `monthly_energy` and distributes each month's total evenly across all hours of that
  month, keeping the colour scale proportional.

## 8.0.1 - 2026-03-20
### Fixed
- **Heatmap colorbar stacking:** Colorbars no longer accumulate on each refresh. The figure is now fully cleared (`fig.clf()`) before redrawing, preventing legend/colorbar buildup.
- **Comparison mode showing no data:** Fixed timestamp conversion in the comparison tab to handle both timezone-aware and timezone-naive datetime64 columns (same root cause as earlier heatmap bug).
- **Email settings UI labels:** Added all missing `settings.email.*` i18n keys for German and English. Labels previously showed raw key names instead of human-readable text.
- **Duplicate "E-Mail Test" button:** The test button was rendered twice on the same grid cell (row 3, col 5), overlapping the "Send daily now" button. The test button now has its own row.

## 8.0.0 - 2026-03-20
### Added
- **Device Scheduling (⏰ Schedules tab) — Feature 10/10, completing the full feature set.**
  A new `ScheduleMixin` introduces a dedicated "Schedules" tab for automatic timed control of Shelly plugs and switches.
  - **`DeviceSchedule` dataclass** persisted under the `schedules` key in `config.json`:
    - `schedule_id` – local UUID (stable across restarts)
    - `device_key` – references a configured device
    - `name` – human-readable label (e.g. "Hot Water", "Space Heater")
    - `time_on` / `time_off` – HH:MM local time for switching on/off
    - `weekdays` – list of 0–6 integers (0 = Monday … 6 = Sunday)
    - `enabled` – master toggle per schedule
    - `switch_id` – relay/switch index on the device
    - `shelly_id_on` / `shelly_id_off` – Shelly Gen2 Schedule IDs (-1 = local-only)
  - **Shelly Gen2+ RPC integration:** Three new helpers in `io/http.py`:
    - `schedule_list()` — `Schedule.List` RPC
    - `schedule_create()` — `Schedule.Create` RPC (cron timespec, Switch.Set calls)
    - `schedule_delete()` — `Schedule.Delete` RPC
  - **"Push to Device" button:** Converts a local schedule to two Shelly Gen2 cron jobs (one for ON, one for OFF) via `Schedule.Create` and persists the returned Shelly IDs back to `config.json`. Deletes previously pushed jobs before re-pushing.
  - **"Load from Device" button:** Imports schedules stored directly on a Gen2+ device (`Schedule.List`) and creates local `DeviceSchedule` entries linked to the Shelly job IDs.
  - **Local app scheduler (`services/scheduler.py`):** A `LocalScheduler` background daemon thread (30-second tick) that fires `Switch.Set` commands for schedules not pushed to the device (Gen1 devices and locally-only entries). Uses a per-minute fired-set to prevent double-firing.
    - Helper `build_shelly_timespec()` converts HH:MM + weekday list to Shelly cron format ("ss mm hh * * dow").
  - **Schedule editor UI:**
    - Treeview with columns: Name, Device, On-Time, Off-Time, Weekdays, Enabled, Backend (Shelly RPC / App local)
    - **Add / Edit / Delete** buttons with confirmation dialog for deletion
    - Editor dialog: name field, device dropdown, relay ID, on-time, off-time, weekday checkboxes (Mo–Su), enabled toggle
    - Weekdays display: "Daily", "Mon–Fri", "Sat–Sun", or comma-separated day abbreviations
    - Status line showing push/load progress and errors
  - **Full i18n support** in all 9 languages (de, en, es, fr, pt, it, pl, cs, ru) for all labels, buttons, messages, and weekday names.
  - **Backward compatible:** `schedules` array defaults to `[]`; existing `config.json` files load without changes.

## 7.9.0 - 2026-03-20
### Added
- **Device Groups & Aggregation.** Multiple Shelly devices can now be combined into logical groups (e.g. "Apartment 1", "Workshop", "Whole House") and displayed as a single aggregated unit.
  - **DeviceGroup** dataclass persisted under the `groups` key in `config.json`:
    - `name` – display name of the group
    - `device_keys` – list of device keys belonging to this group
  - **Dropdown extension:** The "View" dropdown in the top bar now shows three categories: individual device pages (existing), named groups (prefix `📦`), and a "Total" entry (`🔢 Gesamt / Total`) that aggregates all devices.
  - **UiConfig additions:** Two new fields track the selected view — `selected_view_type` ("page" | "group" | "all") and `selected_view_group` (group name).
  - **Costs tab aggregation:** When a group or "Total" is selected, the costs tab displays an additional aggregate section at the top showing summed kWh, cost (€), CO₂, TOU breakdown, projected month cost, and comparison vs. previous month for all devices in the selection.
  - **Plots / Live tab compatibility:** When a group is selected, the plots and live tabs show the first two devices of the group (consistent with the existing 2-device display limit). The costs tab always aggregates all member devices.
  - **Settings → Groups tab (new subtab):** A dedicated scrollable "Groups" editor in the Settings tab allows creating, editing, and deleting groups. Each group has a name field and per-device checkboxes. Changes are saved to `config.json` and the dropdown is updated immediately.
  - **Telegram summary groups section:** When groups are configured, the Telegram daily/monthly summary now appends a "📦 Groups" section listing each group's total kWh, cost, and share of overall consumption.
  - **Full i18n support** in all 9 languages (de, en, es, fr, pt, it, pl, cs, ru): settings tab name, editor labels (name, devices, add, delete, save), costs aggregate title, and total label.
  - **Backward compatible:** `groups` array is written back to `config.json` on first use; existing configs without this key continue to work with no migration needed.

## 7.8.0 - 2026-03-20
### Added
- **Anomaly Detection (🔍 Anomalies tab).** A new `AnomalyMixin` adds automatic detection of unusual consumption patterns using rolling mean + standard deviation statistics.
  - **Three independent detectors:**
    - **Unusual daily consumption:** Compares each day's total kWh against a rolling N-day baseline (mean ± Nσ). Flags days that deviate significantly with configurable minimum absolute deviation to suppress noise.
    - **Elevated night consumption:** Tracks the ratio of night-time energy (22:00–06:00) to total daily energy over a rolling baseline. Flags days where the night ratio is significantly higher than usual, indicating unexpected standby loads or overnight usage.
    - **Power peak at unusual hour:** Identifies the hour of the daily maximum power draw and compares it to the rolling distribution of typical peak times (using circular distance for 24-hour wrap). Flags peaks that occur at statistically unusual hours.
  - **Statistical foundation:** All detectors use a configurable rolling window (default 30 days) to compute mean and standard deviation. Anomalies are triggered when the deviation exceeds `sigma_threshold` × σ (default 2.0σ).
  - **AnomalyConfig** dataclass persisted under the `anomaly` key in `config.json`:
    - `enabled` – master switch
    - `sigma_threshold` – sensitivity (number of std deviations; lower = more sensitive)
    - `min_deviation_kwh` – minimum kWh deviation to suppress false positives
    - `window_days` – rolling baseline window length
    - `check_unusual_daily`, `check_night_consumption`, `check_power_peak_time` – toggles per detector
    - `action_telegram`, `action_webhook`, `action_email` – notification channels
    - `max_history` – maximum entries retained in the in-memory history log
  - **Anomaly tab UI:**
    - Top bar with title, "Run Detection Now" button, and "Clear history" button.
    - Inline settings panel (enabled toggle, sigma, min deviation, window, check toggles, notification channel checkboxes) – changes are saved to `config.json` immediately.
    - Treeview history log showing: timestamp, device name, anomaly type (translated), value, σ count, and description.
    - Status line showing running/completed state and event count.
  - **Notifications:** When a new anomaly is detected, optional notifications are dispatched via Telegram (`_alerts_send_telegram`), Webhook (`_webhook_send_sync` with structured JSON payload including `event`, `device`, `anomaly_type`, `value`, `sigma`, `description`), and E-mail (`_email_send_sync`).
  - **Full i18n support** in all 9 languages (de, en, es, fr, pt, it, pl, cs, ru).
  - **Backward compatible:** `anomaly` section is written back to existing `config.json` on first run with all defaults; no migration needed.

## 7.7.0 - 2026-03-20
### Fixed
- **Heatmap tab no longer shows blank charts.** The calendar heatmap and weekday×hour heatmap were silently failing because `query_samples()` converts raw SQLite Unix integer timestamps to naive UTC `datetime64` objects. The heatmap helpers called `datetime.fromtimestamp(int(ts))` on those objects, which returns nanoseconds instead of seconds, causing an overflow that the `except` clause swallowed. Fixed by normalizing timestamp columns back to Unix integer seconds immediately after loading in `_heatmap_load_df` (and in `_cmp_load_daily`). The same normalization is applied in the new Comparison tab.

### Added
- **Comparison Mode tab (🔀 Compare).** A new tab (`CompareMixin`) lets users overlay two arbitrary time periods — or two different devices — side-by-side in a grouped bar chart.
  - **Two period pickers:** Period A and Period B each have an independent device selector (Combobox over all configured Shelly devices) and two date entry fields (From / To, accepting YYYY-MM-DD or DD.MM.YYYY).
  - **Default values:** Period A defaults to current year so far; Period B defaults to the full previous year.
  - **Unit toggle:** Switch between kWh and € (uses the configured flat-rate or TOU pricing).
  - **Granularity selector:** Choose Total (single bar pair), Daily (relative day-by-day alignment), or Monthly (relative month index).
  - **Delta summary strip:** Always-visible bold label showing `A: X kWh | B: Y kWh | Δ: +Z kWh (+P%)` above the chart.
  - **Matplotlib bar chart:** Side-by-side grouped bars in blue (A) / orange (B) with full light/dark theme support, automatic x-axis label decimation for dense daily views, and a legend when more than one data point exists.
  - **Full i18n support** in all 9 languages (de, en, es, fr, pt, it, pl, cs, ru).

## 7.6.0 - 2026-03-20
### Added
- **Time-of-Use (TOU) / Multi-Tariff Pricing.** The app now supports time-variable electricity tariffs (peak/off-peak), replacing the single flat rate when enabled.
  - **TouConfig** dataclass with `enabled` flag and a list of **TouRate** entries, each defining a name (e.g. HT/NT or Peak/Off-Peak), price (€/kWh), start hour, end hour (overnight windows supported when end < start), and an optional weekdays-only flag (Mon–Fri).
  - **Default rates:** HT (0.35 €/kWh, 06:00–22:00) and NT (0.22 €/kWh, 22:00–06:00); fully configurable.
  - **Timestamp-based cost calculation:** Each energy interval is matched to the applicable tariff window, enabling accurate cost breakdowns regardless of mix of peak/off-peak consumption. Falls back gracefully to flat rate if TOU is disabled or no rates are defined.
  - **Costs tab breakdown:** Each time-range card (Today, Week, Month, Year) now shows a per-tariff breakdown line, e.g. `HT: 12.34 kWh / 4.32 € | NT: 8.00 kWh / 1.76 €`.
  - **Month projection** and **previous month comparison** are also computed using the TOU cost model.
  - **Telegram & Webhook summaries** include the TOU cost breakdown per tariff name when TOU is active.
  - **Settings UI – TOU editor:** A new "Time-of-Use / Time-variable Pricing (TOU)" section in Settings → main tab allows enabling TOU, adding/editing/removing tariff windows via a Treeview with add/edit/remove buttons and an inline dialog (name, price, start hour, end hour, weekdays-only checkbox).
  - **Persisted in config.json** under the `tou` key with full serialisation/deserialisation, including backward-compatible defaults for existing installations.
  - **Full i18n support** in all 9 languages (de, en, es, fr, pt, it, pl, cs, ru).

## 7.5.0 - 2026-03-20
### Added
- **PV/Solar Integration.** A new "☀️ Solar" tab provides a complete solar energy overview for Shelly setups with a bidirectional grid meter.
  - **Configurable PV meter:** Select the Shelly device at the grid connection point (negative power = export to grid) via Settings → Solar / PV.
  - **Feed-in tariff (Einspeisevergütung):** Configurable €/kWh rate to calculate feed-in revenue.
  - **Automatic metric calculation from bidirectional power readings:**
    - **Einspeisung / Grid feed-in (kWh):** Energy exported to the grid (intervals where power < 0).
    - **Netzbezug / Grid consumption (kWh):** Energy imported from the grid (intervals where power ≥ 0).
    - **Eigenverbrauch / Self-consumption (kWh):** Calculated from other configured Shelly devices (total household consumption minus grid import). Requires at least one other device to be configured.
    - **Autarkiegrad / Self-sufficiency (%):** Self-consumption as a share of total household consumption.
    - **PV-Erzeugung / PV production (kWh):** Estimated total PV output (self-consumption + feed-in).
    - **Einspeisevergütung / Feed-in revenue (€):** Feed-in energy × configured tariff.
    - **Ersparte Kosten / Cost savings (€):** Self-consumed energy × electricity price.
  - **Period selector:** View statistics for Today, This Week, This Month, or This Year.
  - **Daily bar chart:** Visual breakdown of feed-in vs. grid import per day, with full light/dark theme support.
  - **Telegram & Webhook integration:** Solar data (feed-in, grid consumption, self-consumption, autarky, PV production, revenue, savings) is appended to daily and monthly summaries. Webhook payloads also include a structured `solar` sub-object with all numeric metrics.
  - **Full i18n support** in all 9 languages (de, en, es, fr, pt, it, pl, cs, ru).
  - **Graceful degradation:** When no PV meter is configured or no other devices exist, the tab shows informative messages rather than errors.

## 7.1.0 - 2026-03-19
### Added
- **Webhook / Home Assistant Integration.** A new generic HTTP POST webhook system allows sending real-time alarm notifications and scheduled daily/monthly summaries to any HTTP endpoint — fully independent of Telegram.
  - **Configurable URL and custom headers:** Supports arbitrary HTTP headers (as a JSON object), enabling Bearer token auth, API keys, and other authentication schemes.
  - **JSON payload** for all event types: alarms include `device_key`, `device_name`, `metric`, `value`, `op`, `threshold`, `duration_seconds`, `message`, and `timestamp`. Summaries include `period_start`, `period_end`, and the full summary `message`.
  - **Per-type enable/disable:** Alarm webhooks, daily summaries, and monthly summaries can be toggled independently.
  - **Alarm-level webhook action:** Each alert rule has a new "Webhook" checkbox column; when enabled, a webhook POST is fired whenever that rule triggers (requires global webhook alarm toggle to also be on).
  - **Scheduled summaries:** Daily and monthly webhook summaries fire at the configured time (reuses the Telegram daily/monthly time setting), with a grace window to prevent duplicates. State is persisted in `data/webhook_summary_state.json`.
  - **Test button** in settings to verify the webhook URL immediately.
  - **Compatible with:** Home Assistant webhooks, ntfy.sh, Node-RED, Zapier, Make, n8n, generic HTTP listeners, and any service accepting a JSON POST.
  - **Fully i18n-compatible** (German and English, other languages fall back to English via the existing fallback chain).
### Fixed
- **Update checker fails on 3-part versions (e.g. 7.0.0).** The version parser regex only accepted 4-part tags (X.Y.Z.W). Tags like `v7.0.0` were rejected, causing the updater to never find newer releases. Now accepts both 3- and 4-part versions.

## 7.0.0 - 2026-03-19
### Added
- **Consumption Heatmap tab.** A new "Heatmap" tab provides two visualizations:
  - **Calendar heatmap (GitHub-contribution-graph style):** Each day is shown as a colored cell (yellow → red), arranged in a week grid for the selected year. Color encodes daily consumption in kWh or €.
  - **Weekday × Hour heatmap:** A 7×24 grid showing when consumption is typically highest throughout the week. Rows = days of week (Mon–Sun), columns = hours (00:00–23:00).
- **Full i18n support** for the heatmap tab in all 9 supported languages (de, en, es, fr, pt, it, pl, cs, ru), including localized day-of-week abbreviations.
- **Light/Dark theme** integration: both heatmaps respect the global plot theme (Auto/Day/Night) and update on refresh.
- **Interactive controls:** device selector, unit toggle (kWh / €), and year selector with auto-refresh on selection change.

## 6.0.1.12 - 2026-03-19
### Fixed
- **CRITICAL: Gen1 switch toggle crashes with AttributeError.** `ShellyHttp` has no `get_json()` method. The Gen1 relay fallback in `set_switch_state()` called `client.get_json(url)`, causing an `AttributeError` on every switch attempt for Gen1 devices (e.g. Shelly Plug S). Fixed to `client.get(url).json()`.
- **Startup device probe resets `updates` and `demo` settings.** When device changes were detected during the startup probe, `AppConfig` was rebuilt without `updates=` and `demo=`, resetting both to defaults and saving to `config.json`. Demo mode users lost their demo configuration on every launch.
- **UI settings reset after restart (load/save gaps in config.py).** `live_daynight_mode`, `live_day_start`, `live_night_start` were saved to `config.json` but never loaded back. `plot_theme_mode`, `telegram_alarm_plots_enabled`, `telegram_summary_load_w` were loaded but never saved. All 6 fields are now correctly round-tripped.
- **Single-phase EM devices forced to 3 phases.** `phases=1` in `config.json` for an EM device was always overwritten to 3 on load. Single-phase EM configurations are now respected.
- **PDF invoice: lines overflow page margin.** With many invoice line items, there was no page break check per line. Lines are now continued on the next page when `y < 5 cm`.
- **Database: `n_avg_current` backfill processed rows without current data.** The WHERE clause used `COALESCE(x, 0)` which is never NULL, so all rows with `n_avg_current IS NULL` were selected — including rows with no current measurements at all. The condition now checks raw columns directly.

## 6.0.1.11 - 2026-03-18
### Added
- **Database retention policy.** Data older than 2 full calendar years is automatically compressed to monthly aggregates on startup. The current year and the previous year are kept at full resolution (down to the second). Older data is aggregated into a new `monthly_energy` table (kWh, avg/min/max power, per-phase voltage & current, neutral current, grid frequency) and the raw samples are deleted. Queries transparently merge monthly and raw data, so historical plots and cost calculations work seamlessly across all time ranges.

## 6.0.1.10 - 2026-03-17
### Fixed
- **False neutral current alarm on live start.** When live monitoring was restarted, the alert state retained `start_ts` from the previous session. The duration check passed immediately on the first sample, causing a false trigger. Alert state is now cleared when live monitoring starts.

## 6.0.1.8 - 2026-03-17
### Fixed
- **Night mode: white stripes on plot edges.** Canvas widget background color is now set at creation time, eliminating visible white edges on V, A, W, VAR, cos φ, and Hz plots in dark mode.

## 6.0.1.7 - 2026-03-17
### Fixed
- **Night mode: white stripes on plot panel edges.** Set canvas widget background to match the dark theme at creation time instead of only after the first redraw.

## 6.0.1.6 - 2026-03-17
### Fixed
- **N current showing 0 in plots.** Neutral current was not displayed in historical plot data.
- **VAR and cos φ plots empty.** Reactive power and power factor plots showed no data due to a query issue.

## 6.0.1.5 - 2026-03-17
### Added
- **Neutral current in historical plots.** The N current is now shown as a dashed gray line in the Plots tab (A / current charts) for historical data, matching the live view style.
- **Automatic backfill of `n_avg_current` in database.** On startup, any historical samples missing `n_avg_current` are computed from phase currents using the 120° displacement formula and stored permanently.
- **On-the-fly N current computation for plots.** Even without stored `n_avg_current`, the Plots tab computes N from L1/L2/L3 phase currents.

## 6.0.1.4 - 2026-03-16
### Changed
- **Global plot theme setting.** Day/Night theme moved from Live tab quick-controls to Settings → Live & Pricing → Display. Now applies to **all** plots (Live + History/Plots tab). Options: Auto (System), Day (light), Night (dark). Auto detects OS dark mode.
- **Removed per-plot theme toggle** from the Live tab control bar. Theme is now centrally managed in Settings.

### Added
- **History plots: day/night theming.** All kWh bar charts, W/V/A/cosφ/Hz time-series, and phase sub-plots in the Plots tab now respect the global theme setting.

## 6.0.1.3 - 2026-03-16
### Fixed
- **Night mode: grid lines now clearly visible.** Changed grid color from `#444444` (barely visible) to `#AAAAAA` and increased alpha from 0.25 to 0.4 in dark mode. Grid lines are now easy to read against the dark background.
- **Auto theme follows system dark mode.** The "Auto" day/night toggle now detects the OS-level dark mode setting (macOS `AppleInterfaceStyle`, Windows `AppsUseLightTheme` registry, Linux GNOME `color-scheme`) instead of relying solely on time-of-day. Falls back to time-based switching if system detection is unavailable.

## 6.0.1.2 - 2026-03-16
### Added
- **Alarm rules: neutral current (A_N) as trigger metric.** The `A_N` metric is now available in the alarm configuration dropdown (Settings → Devices → Alarms). It calculates the neutral conductor current via the same phasor vector sum used in the live views. Triggers, duration, cooldown, popup, beep, and Telegram notifications all work identically to other metrics.

## 6.0.1.1 - 2026-03-16
### Fixed
- **Web dashboard: neutral current now matches desktop app calculation.** Switched from simplified phasor sum to the full vector calculation with per-phase power-factor angles (`atan2(Q, P)`), identical to the desktop live view.
- **Web dashboard: neutral current shown as dashed gray line in current chart.** The `I (N)` series is now rendered as a dashed gray line (matching the desktop app's `N` series style) alongside the L1/L2/L3 phase current lines for 3-phase devices.

## 6.0.1.0 - 2026-03-16
### Added
- **Web dashboard: neutral conductor current (Neutralleiterstrom).** The live web dashboard now shows the calculated neutral current `I (N)` for 3-phase devices. Computed in real-time via phasor summation from phase currents (120° separation). Displayed between the current and phase balance rows in the KV panel.

## 6.0.0.10 - 2026-03-16

### Added
- **Neutral current & apparent power (3-phase).** For 3-phase devices, the neutral conductor current is now computed via vector sum of the three phase currents, correctly accounting for the phase displacement (cos φ / reactive power) between phases. The calculation uses `I_N = |I_a∠(-φ_a) + I_b∠(-120°-φ_b) + I_c∠(+120°-φ_c)|` where `φ_x = arctan2(Q_x, P_x)`. The result is:
  - Shown in the Live tab status line (Hz row): `N: X.XX A / Y VA`
  - Plotted as a dashed gray line ("N") in the Current (A) live chart
  - Stored in the live time-series buffer (`n_current`) for scrollback
- **Interactive legend toggle on all live plots.** Clicking a legend entry (L1, L2, L3, N) in the Voltage and Current live charts now toggles that series on/off. Hidden series are dimmed in the legend (alpha 0.3). State persists across the 1-second redraw cycle.

## 6.0.0.9 - 2026-03-13
### Changed
- **Release version corrected to 6.0.0.9.** Project metadata, package version strings, example config version, release folder name, and ZIP asset were all aligned from `6.0.0.7` to **`6.0.0.9`**.
- **Live tab: grid frequency remains on its own dedicated status row.** This release keeps the UI improvement introduced in the previous build, but publishes it as the corrected GitHub-ready release version **6.0.0.9**.

## 6.0.0.7 - 2026-03-13
### Changed
- **Live tab: grid frequency now has its own status row.** The desktop Live view previously appended `Hz` to the VAR / cos φ line, which became crowded on 3-phase devices. The Live cards now render a dedicated fourth line for grid frequency (`Netzfrequenz / Grid frequency`) so the status area is easier to scan.

## 6.0.0.6 - 2026-03-10

### Fixed
- **Plots: 1-second axis jitter on Hz / V / A tabs eliminated (root cause).** On macOS/Tk, `<Configure>` events fire not only on genuine widget resizes but also on internal Tk geometry re-layouts triggered by `update_idletasks()` calls (which occur inside `_resize_figure_to_widget`). These spurious events all carry the *same* `(width, height)` as before. The `_on_plots_canvas_configure` handler was reacting to every one of them, scheduling a full matplotlib figure redraw each time. This produced a ~1-2 second jitter loop — most visible on live-data tabs (Hz, V, A) where redraws already happen periodically. The fix: track the last-seen `(width, height)` per canvas widget and skip the redraw entirely when the size has not actually changed. A genuine window resize always delivers a new `(w, h)` pair and will still trigger the correct redraw.

## 6.0.0.5 - 2026-03-10

### Fixed
- **Plots: size jitter every ~1 second eliminated.** `fig.set_size_inches()` was called with `forward=True`, which tells matplotlib to resize the Tk canvas widget to match the figure. This fired a `<Configure>` event on the canvas → `_on_plots_canvas_configure` → `_resize_plots_figures_only` → `_resize_figure_to_widget` → another resize → infinite feedback loop visible as the plot alternating between two sizes every second. Changed to `forward=False` so the figure is fitted into the existing canvas without propagating a resize event back.

## 6.0.0.4 - 2026-03-10

### Added
- **Grid frequency (Hz) support.** The Shelly Pro 3EM measures grid frequency on each phase (`a_freq`, `b_freq`, `c_freq`). The average frequency is now:
  - Extracted in `parse_live_fields()` and stored in the new `LiveSample.freq_hz` field
  - Shown in the Tkinter live view (appended to the VAR/cos φ status line)
  - Shown in the web dashboard KV panel ("Netzfrequenz / Grid frequency")
  - Stored in the SQLite database as the new `freq_hz` column (schema migration is automatic)
  - Available as an **"Hz" tab** in the Plots view (live data fallback applies, same as V/A)
  - Usable in Telegram alerts (metric "Hz")

## 6.0.0.3 - 2026-03-10

### Fixed
- **Per-phase V and A plots now work.** The Shelly EMData CSV uses `a_avg_voltage` / `a_avg_current` (not `a_voltage` / `a_current`). The DB import now maps these to the base columns via fallback, so per-phase voltage and current plots are populated correctly. Auto re-import detects and fixes existing databases.

## 6.0.0.2 - 2026-03-09

### Fixed
- **V/A/VAR/cos φ plots now work with full Shelly EMData CSV data.** The DB schema was missing ~42 columns that the Shelly Pro 3EM EMData CSV format provides (voltage min/max/avg, current min/max/avg, apparent power, reactive energy, neutral current). All columns are now stored and used for plots.
- **Automatic re-import from csv_archive on upgrade.** When the app detects that existing DB data was imported with the old schema (missing voltage/current/apparent power columns), it automatically re-imports the archived CSVs to fill the new columns. No manual action required.
- **VAR and cos φ plots now work.** VAR (reactive power) and cos φ (power factor) are derived from active power (P) and apparent power (S) per phase. The expanded DB schema now includes the apparent power and reactive energy columns needed for this computation.
- **Live data fallback extended to VAR/cos φ.** Previously only V and A would fall back to live polling data when historical data lacked the needed columns. Now VAR, cos φ, and per-phase power also fall back to the in-memory live store (which has pre-computed Q and PF from the device).
- **Dead code removed.** Unreachable `elif` branches for VAR/COSPHI in `_wva_series()` that were shadowed by an earlier branch catching the same metric values have been removed and replaced with a clean post-computation override.

## 6.0.0.1 - 2026-03-09

### Fixed
- **Plots: V/A/per-phase W showed nothing after DB migration.** DB schema columns (a_voltage, a_current, …) were always present in query results even when all values were NULL, preventing the live-data fallback for V/A plots. Now drops all-NULL columns from query results.
- **Float 0.0 treated as missing data.** `or` operator on power readings treated a valid 0 W reading as falsy, silently discarding it. Fixed with explicit `is not None` checks.
- **Transaction safety.** Hourly aggregation update now runs inside the same DB transaction as the sample insert, preventing inconsistent state on crash.
- **Absurd energy values during data gaps.** Added 10-minute cap on sample intervals — gaps longer than that are treated as missing data instead of being integrated.
- **Unsorted timestamps.** `ts_min`/`ts_max` for hourly aggregation now computed from actual data instead of assuming sorted order.
- **DB read errors crash instead of CSV fallback.** `read_device_df()` now catches DB errors and falls back to CSV gracefully.
- **`base_dir` swap leaked stale DB connection.** `ensure_data_for_devices()` now resets the DB instance when switching data directories.
- **`save_meta()` crash on DB error.** Now falls back to JSON file if DB write fails.
- **Migration silently skipped.** `keys` variable could be unbound if auto-import raised before assignment, causing migration to silently fail.

## 6.0.0.0 - 2026-03-09

### Changed
- **CSV → SQLite migration**: All energy data is now stored in a SQLite database (`data/energy.db`) instead of individual CSV files. This dramatically improves read performance for plots, stats, and cost calculations.
- **Automatic migration on first startup**: Existing CSV data is automatically imported into the database. Original CSV files are moved to `data/csv_archive/` as backup.
- **New `EnergyDB` class** (`io/database.py`): Thread-safe SQLite wrapper with WAL mode, per-thread connections, pre-computed energy on insert, and hourly aggregation table.
- **Storage layer rewritten** (`io/storage.py`): `save_chunk()` now writes directly to DB. `read_device_df()` queries DB first with CSV fallback. Device metadata stored in DB.
- **Sync simplified** (`services/sync.py`): Removed `pack_csvs()` — DB handles deduplication via `INSERT OR IGNORE`.
- **Demo data** (`services/demo.py`): Demo generator now checks DB for existing data.

## 5.9.2.60 - 2026-03-08

### Improved
- **Performance & memory optimizations across core modules**:
  - `live.py`: Persistent `ThreadPoolExecutor` — avoids recreating threads every poll cycle and blocking on timed-out HTTP requests. Bounded queues prevent unbounded memory growth.
  - `webdash.py`: `LiveStateStore` uses `deque` (O(1) append) instead of list with O(n) slice. Lock released before JSON serialization to reduce contention.
  - `energy.py`: Removed redundant DataFrame copy on every energy calculation.
  - `csv_read.py`: Timestamp column detection moved to module-level function.
  - `storage.py`: Deduplicated legacy glob results across case variants.

## 5.9.2.59 - 2026-03-08

### Fixed
- **Web cost panel redesigned for proper dark/light visibility**. Cost cards now use layered backgrounds: panel uses `var(--card)`, device sections use `var(--bg)`, inner metric cards use `var(--card)` again — creating visible contrast in both themes. Previously cards used `var(--chipbg)` which was invisible in dark mode.
- Device names shown in accent color with separator line.
- € amounts: large (20px), bold (800), centered, accent color.
- Labels: uppercase, small, muted — matching the KV panel style.
- Refresh button styled as a proper button matching the dashboard controls.
- Proper responsive grid: 4 columns on desktop, 2×2 on phone, 1 column on small screens.

## 5.9.2.58 - 2026-03-08

### Fixed
- **Web cost panel CSS was completely broken**: The `.kv` CSS block was not closed, causing all cost panel classes (`.cost-panel`, `.cost-card`, etc.) to be nested inside `.kv` and therefore invalid. This made the cost panel render as unstyled plain text. Fixed by properly closing `.kv` before the cost CSS. Also fixed the `.kv` block itself which lost its `color` and `margin` properties.
- Cost panel now has proper card styling with rounded borders, uppercase labels, large € amounts in accent color, proper spacing and responsive grid layout matching the device cards above.
- Refresh button uses the same `.navlink` pill style as the theme toggle and other controls.

## 5.9.2.57 - 2026-03-08

### Fixed
- **Web cost panel: mobile responsive layout**. Cost cards now use CSS classes instead of inline styles. On phones (<600px): 2×2 grid for today/week/month/year cards, stacked projection/comparison. On very small screens (<360px): single column. Matches the responsive behavior of the rest of the web dashboard (device cards, plots).

## 5.9.2.56 - 2026-03-08

### Changed
- **Web dashboard: Full cost overview matching desktop app**. The cost panel now shows per 3-phase device: today/week/month/year cards (kWh + €), monthly projection, and previous month comparison (% change). Data is fetched via new `/api/costs` endpoint from CSV history (not just live data). Refreshes every 60 seconds + manual refresh button. Fully theme-aware (dark/light).
- New API endpoint `/api/costs` computes historical energy costs per device and time range.

## 5.9.2.55 - 2026-03-08

### Fixed
- **Web dashboard cost summary: theme support**. The cost overview panel now uses CSS variables (`--card`, `--border`, `--fg`, `--muted`, `--accent`) instead of hardcoded white/gray colors. Correctly adapts to dark and light theme, matching the style of device cards.

## 5.9.2.54 - 2026-03-08

### Fixed
- **Startup crash on Python 3.14**: `LivePoint` dataclass had default-valued fields (`pa`/`pb`/`pc`) before non-default fields (`va`/`vb`/`vc`), which is not allowed. Reordered fields so all non-default fields come first.

## 5.9.2.53 - 2026-03-08

### Added
- **Web dashboard: Phase balance indicator**. The live key-values section now shows phase balance for 3-phase devices (✅/⚠️/❌ with per-phase W breakdown), matching the desktop Live tab.
- **Web dashboard: Cost summary panel**. A new "Cost Overview (today)" section appears below the device cards, showing per-device power (W), kWh today, and cost (€) for all 3-phase devices. Includes a total row when multiple 3-phase devices are present. Updates live with each refresh cycle.
- **Per-phase power data** (`pa`/`pb`/`pc`) added to `LivePoint` and transmitted to the web dashboard for accurate balance calculation.

## 5.9.2.52 - 2026-03-08

### Changed
- **Cost dashboard redesigned**: Now shows only 3-phase devices (3EM, Pro 3EM). Each device gets its own section with today/week/month/year cards, monthly projection, and previous month comparison. Single-phase and switch devices are excluded. Scrollable layout for multiple devices.

## 5.9.2.51 - 2026-03-08

### Fixed
- **Phase balance always showed 0%**: The old calculation divided by 3 phases even when only 1 phase had load (e.g. single-phase consumer on a 3EM). Balance is now only calculated when at least 2 phases are active (>5W). With only 1 active phase, the per-phase power distribution is shown instead (e.g. `W: 1200/0/0`).

## 5.9.2.50 - 2026-03-08

### Added
- **Cost dashboard**: New "Costs" tab with overview cards for today/week/month/year, monthly projection, previous month comparison, and per-device breakdown with bar chart.
- **Phase balance indicator**: Live tab now shows phase symmetry for 3-phase devices (✅ ≥90%, ⚠️ ≥70%, ❌ <70%). Helps detect phase imbalance.
- **Telegram: previous period comparison**: Daily and monthly summaries now include the previous period's consumption and percentage change (📈/📉).
- **PDF logo upload**: Settings → Billing now has a company logo picker (PNG/JPG) that renders top-right on PDF invoices.

### Changed
- i18n: New translation keys for cost tab, phase balance, and logo picker (DE/EN/ES/FR).
- BillingConfig: New field `invoice_logo_path`.
- export_pdf_invoice: New parameter `logo_path`.

## 5.9.2.49 - 2026-03-08

### Added
- **Live cost display**: Live tab and web dashboard now show estimated daily cost in € next to "kWh today" (based on configured gross electricity price).
- **Standby detection in Telegram summaries**: Daily and monthly reports now include a standby base load analysis (10th percentile of power values), projected to kWh/year and €/year.
- **Log rotation**: Log files are now automatically rotated daily and deleted after 30 days. Old log files are cleaned up on startup.

### Changed
- i18n: New translation keys `live.cards.cost_today` and `web.kv.cost_today` for DE/EN/ES/FR.
- Web dashboard: `LivePoint` now includes `cost_today` field.

## 5.9.2.48 - 2026-03-08

### Fixed
- **Critical: Telegram summaries now sync data before sending.** Previously, summaries at 00:00 missed the last hours of data because `_build_telegram_summary()` read stale CSV files without calling `sync_all()` first. New method `_sync_before_telegram_summary()` ensures fresh data for both scheduled sends and "Send now" buttons.
- **Critical: Removed duplicate `_telegram_summary_tick()` implementation (~400 lines).** The method was defined twice; the second definition overwrote the first and bypassed the arm-logic state machine.
- **Removed ~500 lines of dead code** after the `return` statement in `_build_telegram_summary()`.
- Fix: `self.log.warning()` → `logging.getLogger(__name__).warning()` — `self.log` was never defined (`AttributeError`).
- Fix: Shadowed variable `t` in `export_pdf_summary()` — loop variable `for t in totals:` overwrote the imported `t()` translation function. Renamed to `row`.
- Fix: `self.root.after()` → `self.after()` in liveweb.py — `self.root` is not defined.
- Fix: `self._show_msgbox()` → `messagebox.showinfo()`/`showwarning()` — method was never defined.
- Fix: `self._save_settings_devices()` → `self._save_settings()` — method was never defined.
- Fix: `Optional[ReleaseInfo]` → `Optional[Any]` — `ReleaseInfo` type was not imported.

## 5.9.2.47

- Fix invoice PDF export: define totals (net_total/vat_amount/gross_total) for new layout.

## 5.9.2.46 - 2026-02-01
- Fix invoice PDF export: define missing `_fmt_qty` formatter (quantity column).

## 5.9.2.45 - 2026-02-01
- Fix: pyproject.toml formatting (version/description split) so pip install works.

## 5.9.2.44 - 2026-02-01
- Invoice PDF layout improved (address blocks, table alignment, totals).
- Invoice lines now correctly include device + period placeholders.
- Pricing footer VAT percent fixed.

## 5.9.2.43 - 2026-02-01
- Fix: invoice PDF export accepts `device_label` (and prints it in header).
- Fix: PDF invoice export accepts `period_label` again.

## 5.9.2.41 - 2026-02-01
- Fix: invoice PDF export per Shelly: pricing base fee now supports method attributes (no TypeError).
- Improvement: safer numeric pricing retrieval for vat/unit price/base fee.

## 5.9.2.40 - 2026-02-01
- Fix: invoice export period bounds now imports pandas (pd) to avoid NameError.

## 5.9.2.39
- Fix: Invoice PDF per Shelly export is robust again (no silent abort; per-device error logging).

## 5.9.2.37 - 2026-02-01
- Fix: Telegram alarm plots now use live ring-buffer data first (last 10 min), so plots are reliably attached even if CSV lagged.
- Fallback: If live buffer is unavailable, falls back to CSV as before.

## 5.9.2.36 - 2026-01-31
- Fix: updater helper is now spawned detached; app restarts automatically after update on macOS/Linux.

## 5.9.2.35 - 2026-01-31
- Fix: updater helper restarts reliably even if start.command loses executable bit.

## 5.9.2.34 - 2026-01-31
- Fix: after auto-updates on macOS/Linux, start scripts could lose executable permissions. Updater now re-applies chmod +x, clears macOS quarantine, and restarts via /bin/bash.

## 5.9.2.33 - 2026-01-31
- Fix: Telegram alerts were built but never sent; alarms now reliably send notifications (with optional plots).

## 5.9.2.32 - 2026-01-30
- Fix: Telegram daily summary plot hourly bucketing could become all-zero. Now uses `resample()` on a coupled `(timestamp, energy_kwh)` dataframe.

## 5.9.2.31 - 2026-01-30
- Fix: Telegram daily summary kWh hourly buckets now computed from the same filtered energy dataframe.

## 5.9.2.30 - 2026-01-30
- Fix: startup crash (IndentationError) in Telegram kWh series helper.

## 5.9.2.29 - 2026-01-30
- Fix: Telegram daily kWh plot now uses the same timestamp series as the summary.

## 5.9.2.28 - 2026-01-30
- Fix: Telegram daily summary plots no longer become all-zero when energy is computed via `calculate_energy()`.

## 5.9.2.27 - 2026-01-30
- Fix: Telegram daily plot could show 0.00 kWh due to timezone-index mismatch. Indices are now normalized to local tz-naive before reindexing.

## 5.9.2.25 - 2026-01-30
- Fix: Telegram daily kWh plots were still empty while monthly plots worked. Daily bucketing now uses a local-calendar-date filter (Europe/Berlin).
- Fix: Telegram daily summary and plots now derive interval energy from EMData phase Wh columns when present.

## 5.9.2.24 - 2026-01-30
- Fix: daily Telegram summary now always uses previous calendar day (00:00–23:59:59 Europe/Berlin) — including "Send now".
- Fix: daily kWh hourly bars were empty for EMData-style CSVs.

## 5.9.2.22 - 2026-01-30
- Fix: daily Telegram "Send now" now uses current calendar day (00:00..now) so hourly kWh plots are not empty.

## 5.9.2.21 - 2026-01-30
- Fix: Telegram daily summary plots now always attach images (fallback window if "previous day" yields no data).

## 5.9.2.20 - 2026-01-30
- Fix: Telegram kWh series timestamp parsing + robust tz/naive fallback.
- Fix: monthly "Send now" now uses Europe/Berlin timestamps consistently.
- Fix: consistent timezone handling for all Telegram summaries.

## 5.9.2.17 - 2026-01-30
- Telegram daily summary now uses the previous calendar day (00:00–24:00 local) instead of a rolling window.
- Fix: daily summary plots missing on some setups due to timezone/naive timestamp slicing.

## 5.9.2.16 - 2026-01-30
- Telegram summaries are now consistent: daily = last 24h (kWh per hour bar chart), monthly = last 30 days (kWh per day bar chart).
- Plots now include total kWh + total cost in the title.
- Added per-device summary plots for daily + monthly.
- Telegram summary text: per-device lines now include cost.

## 5.9.2.14 - 2026-01-30
- Telegram summaries: daily/monthly kWh plots now use bars and include total kWh + cost in the title.

## 5.9.2.13 - 2026-01-30
- Fix: crash on startup due to IndentationError in Telegram helper methods.

## 5.9.2.12 - 2026-01-30
- Fix: Telegram daily/monthly summary plots were not generated (missing `return`).
- Fix: Telegram send-with-images now reports failures correctly.
- Added: Telegram option to enable/disable alarm plots in alerts.

## 5.9.2.10
- Refactor: split oversized `ui/app.py` into `ui/app_main.py` + mixins for better maintainability.
- Added: Telegram plots as images for alarms (last 10 min V/A/W) and summaries (last 24h kWh / last 30 days kWh).

## 5.9.2.8 - 2026-01-29
- Fix: auto-restart after updates on macOS (ensure executable bits + clear quarantine).

## 5.9.2.6
- Version bump + packaging (start.command/start.sh executable).

## 5.9.2.4
- Fix: update page no longer installs when no newer release is available.
- Fix: prevent stale/slow update checks from overwriting newer results (sequence id).

## 5.9.2.3
- Fix: update page buttons now work (GitHub check used undefined function before).
- Fix: restore missing GUI entrypoint (`run_gui`).

## 5.9.2.1
- Add GitHub-based update checker and optional auto-update.
- Demo Mode: realistic live data with jitter/random-walk and deterministic appliance bursts.
- Demo Mode: generate demo CSV history (7 days) for plots/exports automatically.
- Setup Wizard: option to enable Demo Mode.

## 5.8.14.55
- Fix: setup wizard now persists added devices (AppConfig is frozen).
- Fix: ZIP start scripts ship with executable bits on macOS/Linux.

## 5.8.14.54
- Setup Wizard: Next/Finish auto-add selected devices before validating.

## 5.8.14.53
- Fix: setup wizard Next/Back navigation now works reliably.
- Setup wizard: add manual IP/host entry to add Shellys without discovery.

## 5.8.14.51 - 2026-01-27
- Setup wizard: first-run guided flow (Devices → Telegram optional → Finish).
- Auto-discovery: find Shelly devices via mDNS (zeroconf) and quick local /24 IP scan.
- First-run UX: other tabs stay disabled until at least one device is configured.

## 5.8.14.47
- Live: Day/Night auto mode; plot label/legend colors now switch with theme.

## 5.8.14.44
- Live: add Day/Night filter toggle for live plots (All/Day/Night), persisted in config.

## 5.8.14.40
- Fixed: startup crash due to indentation errors in plot UI helpers.

## 5.8.14.39
- Fixed: plot UI now keeps selected device stable when switching between metric tabs.
- Added: optional ◀/▶ buttons to cycle device pages quickly.

## 5.8.14.38
- Linux support: added `start.sh` (venv + install + run).
- GitHub Actions release workflow: tag `v*` builds ZIP assets and publishes a GitHub Release.

## 5.8.14.37
- Windows start via `start.bat` (creates `.venv`, installs requirements, starts the app).

## 5.8.14.36
- Telegram scheduler: spam protection when enabling + "last attempt" status.

## 5.8.14.35
- Alert triggers fix (alert logic active again).

## 5.8.14.34
- Telegram daily summary (midnight) fix.
