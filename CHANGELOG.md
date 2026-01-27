# Changelog
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
