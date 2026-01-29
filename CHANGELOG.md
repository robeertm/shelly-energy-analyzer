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