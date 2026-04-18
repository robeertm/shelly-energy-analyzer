# Shelly Energy Analyzer — one-shot rescue script (Windows / PowerShell).
#
# Recovers installs that got stuck after clicking "Install update" on a
# release older than v16.26.1, where the in-app updater path leaves the
# service dead or half-updated.
#
# Usage (PowerShell, from anywhere):
#   iwr https://raw.githubusercontent.com/robeertm/shelly-energy-analyzer/main/scripts/rescue.ps1 | iex
#
# Options via environment variables (set before the one-liner):
#   $env:SEA_APP_DIR  = "C:\path\to\install"     # override install-dir detection
#   $env:SEA_TAG      = "v16.26.3"               # pin a specific release
#   $env:SEA_PORT     = "8765"                   # expected local port
#   $env:SEA_NO_RESTART = "1"                    # skip starting the service again
#
# Safe: config.json, data/, logs/, .venv/ are preserved. Uses the existing
# virtualenv for pip; nothing installed system-wide.
[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$Repo = "robeertm/shelly-energy-analyzer"
$Port = if ($env:SEA_PORT) { $env:SEA_PORT } else { "8765" }

function Log($m)  { Write-Host "[rescue] $m" -ForegroundColor Blue }
function Warn($m) { Write-Host "[rescue] $m" -ForegroundColor Yellow }
function ErrW($m) { Write-Host "[rescue] $m" -ForegroundColor Red }
function Ok($m)   { Write-Host "[rescue] $m" -ForegroundColor Green }

# ── 1. Locate install dir ────────────────────────────────────────────────
function Find-AppDir {
  if ($env:SEA_APP_DIR) { return $env:SEA_APP_DIR }
  $candidates = @(
    $PWD.Path,
    (Join-Path $env:USERPROFILE "shelly-energy-analyzer"),
    (Join-Path $env:USERPROFILE "shelly_energy_analyzer_v6"),
    "C:\shelly-energy-analyzer",
    "C:\opt\shelly-energy-analyzer"
  )
  foreach ($d in $candidates) {
    if (Test-Path (Join-Path $d "pyproject.toml")) {
      if (Select-String -Path (Join-Path $d "pyproject.toml") -Pattern "shelly-energy-analyzer|shelly_analyzer" -Quiet) {
        return (Resolve-Path $d).Path
      }
    }
  }
  return $null
}

$AppDir = Find-AppDir
if (-not $AppDir) {
  ErrW "Couldn't find a Shelly Energy Analyzer install. Set `$env:SEA_APP_DIR = 'C:\path'."
  exit 1
}
Log "install dir: $AppDir"

if (-not (Test-Path (Join-Path $AppDir "src\shelly_analyzer"))) {
  ErrW "$AppDir is missing src\shelly_analyzer — not a valid install."
  exit 1
}

$initFile = Join-Path $AppDir "src\shelly_analyzer\__init__.py"
$currentVer = $null
if (Test-Path $initFile) {
  $m = Select-String -Path $initFile -Pattern '__version__\s*=\s*"([^"]+)"' | Select-Object -First 1
  if ($m) { $currentVer = $m.Matches[0].Groups[1].Value }
}
Log "current version: $(if ($currentVer) { $currentVer } else { 'unknown' })"

# ── 2. Detect + stop service ─────────────────────────────────────────────
$ServiceKind = "none"   # service | plain | none
$ServiceName = $null

$svc = Get-Service -ErrorAction SilentlyContinue | Where-Object { $_.Name -match "shelly" -or $_.DisplayName -match "shelly" } | Select-Object -First 1
if ($svc) {
  $ServiceKind = "service"
  $ServiceName = $svc.Name
} elseif (Get-Process -ErrorAction SilentlyContinue | Where-Object { $_.CommandLine -match "shelly_analyzer" }) {
  $ServiceKind = "plain"
}
Log "service backend: $ServiceKind$(if ($ServiceName) { " ($ServiceName)" })"

function Stop-App {
  switch ($ServiceKind) {
    "service" {
      Log "stopping Windows service $ServiceName"
      try { Stop-Service -Name $ServiceName -Force -ErrorAction Stop }
      catch { Warn "Stop-Service failed: $_ — continuing" }
    }
    "plain" {
      Log "killing shelly_analyzer processes"
      Get-Process python -ErrorAction SilentlyContinue |
        Where-Object { $_.Path -and (Get-CimInstance Win32_Process -Filter "ProcessId=$($_.Id)").CommandLine -match "shelly_analyzer" } |
        ForEach-Object { Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue }
    }
    default { Log "no running service detected" }
  }
  # Wait up to 10 s for processes to exit
  for ($i=0; $i -lt 10; $i++) {
    $still = Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match "shelly_analyzer" }
    if (-not $still) { return }
    Start-Sleep -Seconds 1
  }
  Warn "shelly_analyzer still running after 10s — continuing anyway"
}

function Start-App {
  if ($env:SEA_NO_RESTART -eq "1") { Warn "SEA_NO_RESTART=1, skipping start"; return }
  switch ($ServiceKind) {
    "service" {
      Log "starting Windows service $ServiceName"
      Start-Service -Name $ServiceName
    }
    default {
      Warn "no managed service: start manually (e.g. .\start.bat)"
    }
  }
}

Stop-App

# ── 3. Clean up stale artefacts ──────────────────────────────────────────
$lockFile = Join-Path $AppDir ".shelly_analyzer.lock"
if (Test-Path $lockFile) { Remove-Item $lockFile -Force }
Get-ChildItem -Path $env:TEMP -Filter "sea_update_*" -Directory -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
Get-ChildItem -Path $env:TEMP -Filter "tmp*.zip" -File -ErrorAction SilentlyContinue | Remove-Item -Force -ErrorAction SilentlyContinue

if (Test-Path (Join-Path $AppDir ".git")) {
  Push-Location $AppDir
  & git checkout -- src\shelly_energy_analyzer.egg-info\ 2>$null
  Pop-Location
}

# ── 4. Pull fresh code ───────────────────────────────────────────────────
$Tag = $env:SEA_TAG
if (-not $Tag) {
  Log "fetching latest release tag from GitHub"
  try {
    $latest = Invoke-RestMethod "https://api.github.com/repos/$Repo/releases/latest" -UseBasicParsing -Headers @{ "User-Agent" = "shelly-rescue" }
    $Tag = $latest.tag_name
  } catch {
    ErrW "could not reach GitHub API. Set `$env:SEA_TAG = 'vX.Y.Z' manually."
    exit 1
  }
}
Log "target release: $Tag"

if (Test-Path (Join-Path $AppDir ".git")) {
  Log "using git to reset to $Tag"
  Push-Location $AppDir
  & git fetch --tags --quiet origin
  & git reset --hard $Tag --quiet
  Pop-Location
} else {
  Log "no git checkout — downloading release ZIP"
  $Asset = "shelly_energy_analyzer_${Tag}_windows.zip"
  $Url = "https://github.com/$Repo/releases/download/$Tag/$Asset"
  $TmpZip = Join-Path $env:TEMP ("sea_rescue_" + [Guid]::NewGuid() + ".zip")
  $Staging = Join-Path $env:TEMP ("sea_rescue_staging_" + [Guid]::NewGuid())
  New-Item -ItemType Directory -Path $Staging | Out-Null
  try {
    Log "downloading $Url"
    Invoke-WebRequest -Uri $Url -OutFile $TmpZip -UseBasicParsing
    Log "extracting to $Staging"
    Expand-Archive -Path $TmpZip -DestinationPath $Staging -Force
    # If single top-level folder, descend into it
    $topLevel = Get-ChildItem $Staging | Where-Object { -not $_.Name.StartsWith(".") }
    if ($topLevel.Count -eq 1 -and $topLevel[0].PSIsContainer) {
      $Staging = $topLevel[0].FullName
    }
    $exclude = @("\.venv", "data", "logs", "config\.json", "config\.example\.json", "\.git", "\.github", "\.claude", "\.vscode", "__pycache__", "docs")
    Log "copying files to $AppDir (preserving config.json, data/, logs/, .venv/)"
    Get-ChildItem $Staging -Force | ForEach-Object {
      $match = $false
      foreach ($ex in $exclude) { if ($_.Name -match "^$ex$") { $match = $true; break } }
      if ($_.Name -match '\.pyc$' -or $_.Name -eq '.DS_Store') { $match = $true }
      if (-not $match) {
        $dst = Join-Path $AppDir $_.Name
        if ($_.PSIsContainer) {
          if (Test-Path $dst) { Remove-Item $dst -Recurse -Force -ErrorAction SilentlyContinue }
          Copy-Item $_.FullName $dst -Recurse -Force
        } else {
          Copy-Item $_.FullName $dst -Force
        }
      }
    }
  } finally {
    Remove-Item $TmpZip -ErrorAction SilentlyContinue
    if (Test-Path $Staging) {
      # If $Staging descended into a subdir, try to nuke the parent extraction dir too
      $extractionRoot = (Split-Path $Staging -Parent)
      if ($extractionRoot -like "*sea_rescue_staging_*") {
        Remove-Item $extractionRoot -Recurse -Force -ErrorAction SilentlyContinue
      } else {
        Remove-Item $Staging -Recurse -Force -ErrorAction SilentlyContinue
      }
    }
  }
}

$m = Select-String -Path $initFile -Pattern '__version__\s*=\s*"([^"]+)"' | Select-Object -First 1
$NewVer = if ($m) { $m.Matches[0].Groups[1].Value } else { "unknown" }
Log "files replaced — installed version: $NewVer"

# Clear stale bytecode
Get-ChildItem -Path (Join-Path $AppDir "src") -Directory -Recurse -Filter "__pycache__" -ErrorAction SilentlyContinue |
  Remove-Item -Recurse -Force -ErrorAction SilentlyContinue

# ── 5. Refresh venv editable install ────────────────────────────────────
$VenvPy = Join-Path $AppDir ".venv\Scripts\python.exe"
if (-not (Test-Path $VenvPy)) { $VenvPy = Join-Path $AppDir ".venv\bin\python" }

if (Test-Path $VenvPy) {
  Log "refreshing venv editable install"
  & $VenvPy -m pip install -q -e $AppDir 2>$null | Out-Null
  $req = Join-Path $AppDir "requirements.txt"
  if (Test-Path $req) {
    & $VenvPy -m pip install -q -r $req 2>$null | Out-Null
  }
} else {
  Warn "no .venv found in $AppDir — skipping pip"
}

# ── 6. Restart + verify ──────────────────────────────────────────────────
Start-App

if ($env:SEA_NO_RESTART -ne "1" -and $ServiceKind -eq "service") {
  # Accept any self-signed cert on localhost for the health check
  Add-Type -TypeDefinition @"
using System.Net;
using System.Net.Security;
public class TlsAllow { public static bool Accept(object s, System.Security.Cryptography.X509Certificates.X509Certificate c, System.Security.Cryptography.X509Certificates.X509Chain ch, SslPolicyErrors e) { return true; } }
"@ -ErrorAction SilentlyContinue
  [System.Net.ServicePointManager]::ServerCertificateValidationCallback = [TlsAllow]::Accept

  for ($i=1; $i -le 10; $i++) {
    Start-Sleep -Seconds 2
    try {
      $resp = Invoke-WebRequest -UseBasicParsing -Uri "https://localhost:$Port/api/version" -TimeoutSec 3 -ErrorAction Stop
      if ($resp.Content -match $NewVer) {
        Ok "service is up: $($resp.Content)"
        exit 0
      }
    } catch { }
  }
  Warn "API didn't respond with $NewVer within 20s — check Event Viewer / service logs."
  exit 1
}

Ok "rescue complete — version $NewVer installed"
