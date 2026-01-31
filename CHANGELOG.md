## 5.9.2.36 - 2026-01-31

- Fix: updater helper is now spawned detached; app restarts automatically after update on macOS/Linux.
- Fix: restart process is started via nohup/bash with detached stdio, so it survives app shutdown.

## 5.9.2.35 - 2026-01-31

- Fix: updater helper restarts reliably even if start.command loses executable bit (resolve restart path relative to app dir, fallback to start scripts, chmod +x).

## 5.9.2.34 - 2026-01-31

- Fix: After auto-updates on macOS/Linux, start scripts could lose executable permissions and the app would not restart. Updater helper now re-applies chmod +x, clears macOS quarantine best-effort, and restarts via /bin/bash for .command/.sh.

## 5.9.2.33 - 2026-01-31

- Fix: Telegram alerts were built but never sent; alarms now reliably send notifications (with optional plots).

## 5.9.2.32 - 2026-01-30

- Fix: Telegram **daily** summary plot hourly bucketing could still become all-zero even though the daily window kWh was correct.
  We now bucket via `resample()` on a coupled `(timestamp, energy_kwh)` dataframe (no floor/groupby + reindex mismatch).

## 5.9.2.31 - 2026-01-30

- Fix: Telegram daily summary kWh hourly buckets now computed from the same filtered energy dataframe (prevents all-zero/empty daily plots).

## 5.9.2.30 - 2026-01-30

- Fix: Startup crash (IndentationError) in Telegram kWh series helper.

## 5.9.2.29 - 2026-01-30

- Fix: Telegram daily kWh plot now uses the same timestamp series as the summary (no zeroing due to timestamp source mismatch).

## 5.9.2.28

- Fix: Telegram daily summary plots no longer become all-zero when energy is computed via calculate_energy().
  Timestamp bucketing now uses a robust (timestamp + energy_kwh) frame and supports dfs with DateTimeIndex.

## 5.9.2.27 - 2026-01-30

- Fix: Telegram daily plot could show **0.00 kWh** even though the daily summary had real kWh.
  Root cause was a timezone-index mismatch during reindexing (series tz-aware vs. expected buckets).
  We now normalize all Telegram summary plot indices to **local tz-naive** before reindexing.

## 5.9.2.25 - 2026-01-30

- Fix: Telegram **daily** kWh plots were still empty (0.00 kWh) while monthly plots worked.
  Daily hourly bucketing now uses a **local-calendar-date filter** (Europe/Berlin) to avoid tz/naive edge cases.
- Fix: Telegram daily summary and plots now derive interval energy consistently from **EMData phase Wh columns** (`a/b/c_total_act_energy`) when present.

## 5.9.2.24 - 2026-01-30

- Fix: Daily Telegram summary now always uses **previous calendar day** (00:00–23:59:59 Europe/Berlin) — including "Jetzt senden".
- Fix: Daily kWh hourly bars were empty for EMData-style CSVs (interval Wh columns). We now derive `energy_kwh` directly from `a/b/c_total_act_energy` when present.
- Extra: Minimal console diagnostics for daily kWh windows (rows + total kWh) to help future debugging.

## 5.9.2.22 - 2026-01-30

- Fix: Daily Telegram "Jetzt senden" now uses current calendar day (00:00..now) so hourly kWh plots are not empty.
- Daily plot title adapts (Heute vs. Vortag) based on the selected window.

## 5.9.2.21 - 2026-01-30
- Fix: Telegram **daily summary plots** now always attach images (fallback window if "previous day" yields no data; better robustness).
- Fix: More robust daily time window selection to avoid empty plot lists.

## 5.9.2.20 - 2026-01-30
- Fix: Telegram kWh series timestamp parsing + robust tz/naive fallback (prevents empty daily/monthly plots).
- Fix: Monthly 'Jetzt senden' now uses Europe/Berlin timestamps consistently.


- Fix Telegram summaries: consistent timezone handling (assume UTC for naive epoch timestamps, convert to Europe/Berlin).
- Fix daily 'Send now': always uses previous calendar day (00:00–24:00) and now reliably attaches plots.
- Fix monthly summary: avoid sending empty per-device plots (skip devices with 0 kWh in the window).

## 5.9.2.18 - 2026-01-30

## 5.9.2.17 - 2026-01-30
- Telegram **Daily summary** now uses the **previous calendar day** (00:00–24:00 local) instead of a rolling window.
- Fix: daily summary plots missing on some setups due to timezone/naive timestamp slicing; timestamps are normalized to **Europe/Berlin** for slicing/bucketing.
- Daily plot titles updated to **Vortag (pro Stunde)** and include total kWh + cost (same as monthly).
## 5.9.2.16 - 2026-01-30
- Telegram summaries are now consistent:
  - **Daily**: last **24 hours**, **kWh per hour** as **bar chart**.
  - **Monthly**: last **30 days**, **kWh per day** as **bar chart**.
- Plots now include **total kWh + total cost** in the title (price taken from **Pricing** settings).
- Added **per-device** (per Shelly) summary plots for daily + monthly.
- Telegram summary text: per-device lines now also include **cost**.

## 5.9.2.14 - 2026-01-30
- Telegram summaries: daily/monthly kWh plots now use bars and include total kWh + cost in the title.
- Daily summary: sends the same kWh bar plot (24h) with totals.

## 5.9.2.13 - 2026-01-30

- Fix: crash on startup due to `IndentationError` in `ui/mixins/core.py` (Telegram helper methods). Indentation normalized; app starts again.

## 5.9.2.12 - 2026-01-30

- Fix: Telegram daily/monthly summary plots were not generated (missing `return` in `_telegram_kwh_series`). Plots (24h / 30 days) are now created and sent.
- Fix: Telegram send-with-images now reports failures correctly when images are expected.
- Added (from 5.9.2.11): Telegram option to enable/disable alarm plots (last 10 min V/A/W) in alerts.
## 5.9.2.10

- Refactor: split oversized `ui/app.py` into `ui/app_main.py` + mixins (`ui/mixins/*`) and shared helpers (`ui/_shared.py`) to improve maintainability.
- No functional changes intended; `shelly_analyzer.ui.app:run_gui` remains the stable entrypoint.


### Added
- Telegram: send plots as images for alarms (last 10 min V/A/W) and summaries (last 24h kWh / last 30 days kWh).
## 5.9.2.8

## 5.9.2.8 - 2026-01-29

- Fix: auto-restart after updates on macOS (ensure executable bits + clear quarantine; restart via bash).
- Docs: clarify Git push workflow and avoiding wrong-folder pushes.

- Fix: Auto-restart after updates on macOS/Linux (ensure executable bits, clear quarantine, restart via /bin/bash).

## 5.9.2.6

- Version bump + packaging (start.command/start.sh executable).

## 5.9.2.5
- Docs: README updated (updater behavior + GitHub upload commands).

## 5.9.2.2

## v5.9.2.3
- Fix: Update page buttons now work (GitHub check used undefined function before, always showed “GitHub not reachable”).  
- UX: Immediate status feedback on update actions; “Open release page” now works even if no check result is available.

- Fix: restore missing GUI entrypoint (`run_gui`) so `python -m shelly_analyzer` / start scripts work again.

## 5.9.2.1
- Add GitHub-based update checker and optional auto-update.
- Non-blocking startup check with short timeout and clear offline message.
- Updater helper replaces app files and restarts; preserves config/data.

## 5.9.2.1
- Fix demo sync: generate CSV locally (no HTTP to demo://)

## 5.9.2.1


## v5.9.2.1

- Demo Mode: add realistic jitter/random-walk to live data and deterministic appliance bursts.
- Demo Mode: generate demo CSV history (7 days) for plots/exports automatically (if no CSV exists).
- Fix: Demo switch toggle in Live now works without network.
- Fix: Tkinter callback NameError in live status error handler.

- Add Demo Mode (no Shellys required): realistic fake devices, live data generator, and demo CSV data for plots.
- Setup Wizard: option to enable Demo Mode.

## 5.8.14.55
- Fix: Setup wizard now persists added devices (AppConfig is frozen).
- Fix: ZIP start scripts ship with executable bits on macOS/Linux.

## 5.8.14.52 - 2026-01-27

## v5.8.14.54
- Setup Wizard: Next/Finish auto-add selected (or all discovered) devices before validating.


## 5.8.14.53
- Fix: Setup wizard Next/Back navigation now works reliably (tab selection by id).
- Improvement: Next is enabled when selecting discovered devices; selected devices are auto-added when proceeding.

- Setup wizard: add manual IP/host entry to add Shellys without discovery.
- IP /24 scan: stricter Shelly detection (no false positives on non-Shelly devices).
- Fix: repair setup wizard mDNS button wiring and remove corrupted code fragments.

## 5.8.14.49

## 5.8.14.51 - 2026-01-27

- Fix: prevent AttributeError on first-run setup when Sync tab is not built yet (`sync_summary`).
- Fix: buffer sync log messages until Sync tab exists.

- Setup wizard: first-run guided flow (Devices → Telegram optional → Finish).
- Auto-discovery: find Shelly devices via mDNS (zeroconf) and quick local /24 IP scan, then add selected devices to config.
- First-run UX: other tabs stay disabled until at least one device is configured (prevents CSV/no-device popups).

## 5.8.14.48
- First-run: if no config exists, open Settings → Devices and keep startup quiet (no missing CSV spam).

## 5.8.14.45

## 5.8.14.47
- Live: Day/Night 'Auto' mode clarified; plot label/legend colors now switch with theme.
- Live: Theme is re-applied after legend/layout so all labels follow Day/Night.
- Fix: Live Day/Night control now actually applies (persisted mode + immediate redraw).
- Change: Live Day/Night now switches plot appearance (light/dark theme). `All` = auto by time.

# Changelog

## 5.9.2.4
- Fix: Update page no longer installs when no newer release is available; Install button is disabled unless an update is actually newer.
- Fix: Prevent stale/slow update checks from overwriting newer results (sequence id).

## 5.8.14.44

- Live: add Day/Night filter toggle for live plots (All/Day/Night), persisted in config.
- Fix: keep repository GitHub-ready (no caches), keep release workflow intact.

## 5.8.14.40
- Fixed: Startup crash due to indentation errors in plot UI helpers (Windows/macOS/Linux).
- Fixed: Plot device-page cycle helpers correctly scoped inside the UI (no more IndentationError).

## 5.8.14.39
- Fixed: Plot UI now keeps the selected device stable when switching between metric tabs (kWh/V/A/W/VAR/cosφ).
- Improved: Plot tab rebuilds preserve the active metric/device selection (no more "jumping" state).
- Added: Optional ◀/▶ buttons to cycle device pages quickly.


## v5.8.14.38
- Linux support: added `start.sh` (venv + install + run)
- Docs switched to English
- GitHub Actions release workflow: tag `v*` builds ZIP assets and publishes a GitHub Release

## v5.8.14.37
- Windows start via `start.bat` (creates `.venv`, installs requirements, starts the app)
- Includes the latest scheduler/Telegram fixes from previous iterations

## v5.8.14.36
- Telegram scheduler: spam protection when enabling + "last attempt" status

## v5.8.14.35
- Alert triggers fix (alert logic active again)

## v5.8.14.34
- Telegram daily summary (midnight) fix
