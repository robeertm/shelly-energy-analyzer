@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM Ensure we run from this folder (even when launched via Explorer)
cd /d "%~dp0"

REM Use UTF-8 in console + Python IO to avoid cp1252 issues
chcp 65001 >nul
set PYTHONUTF8=1

REM Prefer Windows Python launcher if available
where py >nul 2>nul
if %errorlevel%==0 (
  set "PY=py -3"
) else (
  set "PY=python"
)

REM Basic Python availability check
%PY% -c "import sys; assert sys.version_info>= (3,10)" >nul 2>nul
if %errorlevel% NEQ 0 (
  echo [!] Python 3.10+ not found.
  echo     Please install Python from python.org and enable "Add python.exe to PATH".
  pause
  exit /b 1
)

REM Create venv if missing
if not exist ".venv\Scripts\python.exe" (
  echo [i] Creating venv in .venv ...
  %PY% -m venv .venv
  if %errorlevel% NEQ 0 goto :err
)

call ".venv\Scripts\activate.bat"
if %errorlevel% NEQ 0 goto :err

echo [i] Installing/updating requirements ...
python -m pip install -U pip >nul
python -m pip install -r requirements.txt
if %errorlevel% NEQ 0 goto :err
python -m pip install -e .
if %errorlevel% NEQ 0 goto :err

echo [i] Starting GUI ...
python -m shelly_analyzer
exit /b 0

:err
echo.
echo [!] Startup failed. See details above.
pause
exit /b 1
