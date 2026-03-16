# Changelog

## 6.0.1.4 - 2026-03-16
### Changed
- **Global plot theme setting.** Day/Night theme moved from Live tab quick-controls to Settings → Live & Preis → Darstellung. Now applies to **all** plots (Live + History/Plots tab). Options: Auto (System), Tag (hell), Nacht (dunkel). Auto detects OS dark mode.
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
