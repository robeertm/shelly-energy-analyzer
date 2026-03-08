# Changelog

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
