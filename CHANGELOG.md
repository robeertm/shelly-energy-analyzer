# Changelog

## 11.8.1 - 2026-03-27
### Fixed
- **CO₂ bar labels now shown on all bars up to 31** – Previously labels were thinned starting at 25 bars, causing every-other-bar gaps in "Days" mode (30 bars). Threshold raised to 31.
- **More spacing between kWh and CO₂ text** – Increased line spacing (0.85 → 1.15) and vertical offset above bars to prevent text overlap.

### Improved
- **Cost tab uses real ENTSO-E CO₂ data** – The cost tab now computes CO₂ emissions using actual hourly grid intensity from ENTSO-E (joined with per-device hourly energy) instead of a static g/kWh factor. Falls back to the configured static intensity if ENTSO-E is not set up or has no data for the period.

## 11.8.0 - 2026-03-27
### Added
- **CO₂ bar coloring by intensity** – Bars in the CO₂ plots tab are now colored on a green → yellow → red gradient based on the average grid CO₂ intensity for each time bucket. Uses the existing green/dirty threshold settings.
- **kWh + CO₂ labels above bars** – Each bar now shows both the energy consumption (kWh) and the CO₂ emissions (g/kg) as a two-line annotation. Font size adapts to widget size; labels are thinned on dense charts to avoid overlap.
- **Custom range controls for CO₂ tab** – The CO₂ plots tab now has the same "Last N [hours/days/weeks/months]" input fields and Apply button as the kWh tab, allowing flexible time range selection.
- **Total kWh in CO₂ chart title** – The chart title now shows both total kWh and total CO₂ for the displayed range.

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
