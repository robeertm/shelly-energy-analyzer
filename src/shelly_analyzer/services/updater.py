
from __future__ import annotations

import json
import os
import platform
import re
import shutil
import sys
import tempfile
import threading
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

DEFAULT_TIMEOUT_S = 3.0

_TAG_RE = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)\.(\d+)$")

def parse_version(tag: str) -> Optional[Tuple[int,int,int,int]]:
    if not tag:
        return None
    m = _TAG_RE.match(tag.strip())
    if not m:
        return None
    return tuple(int(x) for x in m.groups())

def is_newer(a: str, b: str) -> bool:
    """Return True if version a > version b (tags like v5.9.1.3)."""
    va = parse_version(a.lstrip("v"))
    vb = parse_version(b.lstrip("v"))
    if va is None or vb is None:
        return False
    return va > vb

def detect_platform_suffix() -> str:
    if sys.platform.startswith("win"):
        return "windows"
    if sys.platform == "darwin":
        return "macos"
    return "linux"

@dataclass
class UpdateInfo:
    reachable: bool
    status: str
    latest_tag: Optional[str] = None
    asset_url: Optional[str] = None
    asset_name: Optional[str] = None

def _http_get_json(url: str, timeout_s: float = DEFAULT_TIMEOUT_S) -> dict:
    req = urllib.request.Request(url, headers={
        "User-Agent": "shelly-energy-analyzer-updater",
        "Accept": "application/vnd.github+json",
    })
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        data = resp.read().decode("utf-8")
    return json.loads(data)

def _pick_asset(release: dict, platform_suffix: str) -> Tuple[Optional[str], Optional[str]]:
    assets = release.get("assets") or []
    want = f"_{platform_suffix}.zip"
    for a in assets:
        name = a.get("name") or ""
        url = a.get("browser_download_url") or ""
        if name.endswith(want) and url:
            return url, name
    # fallback: any zip
    for a in assets:
        name = a.get("name") or ""
        url = a.get("browser_download_url") or ""
        if name.lower().endswith(".zip") and url:
            return url, name
    return None, None

def check_latest_release(repo: str, timeout_s: float = DEFAULT_TIMEOUT_S) -> UpdateInfo:
    api = f"https://api.github.com/repos/{repo}/releases/latest"
    try:
        release = _http_get_json(api, timeout_s=timeout_s)
        tag = release.get("tag_name") or ""
        if not tag:
            return UpdateInfo(False, "GitHub reachable, but no latest release tag found.")
        suffix = detect_platform_suffix()
        url, name = _pick_asset(release, suffix)
        if not url:
            return UpdateInfo(True, f"Latest is {tag}, but no ZIP asset found for {suffix}.", latest_tag=tag)
        return UpdateInfo(True, f"Latest on GitHub: {tag}", latest_tag=tag, asset_url=url, asset_name=name)
    except Exception as e:
        return UpdateInfo(False, f"GitHub not reachable (offline/timeout): {e}")

def download_file(url: str, dst: Path, timeout_s: float = 10.0) -> None:
    req = urllib.request.Request(url, headers={"User-Agent":"shelly-energy-analyzer-updater"})
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        dst.write_bytes(resp.read())

def install_update_zip(zip_path: Path, app_dir: Path) -> Tuple[bool, str]:
    """
    Extract zip into staging dir, then copy into app_dir excluding user data.
    This is 'in-process' install; best-effort for mac/linux. Windows may need helper.
    """
    import zipfile
    staging = Path(tempfile.mkdtemp(prefix="sea_update_"))
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(staging)

        # detect root folder (if zip contains a single top folder)
        entries = [p for p in staging.iterdir()]
        root = staging
        if len(entries) == 1 and entries[0].is_dir():
            root = entries[0]

        exclude = {"config.json", "data", "logs", ".venv", ".git"}
        for item in root.iterdir():
            name = item.name
            if name in exclude:
                continue
            dst = app_dir / name
            if dst.exists():
                if dst.is_dir() and not dst.is_symlink():
                    shutil.rmtree(dst)
                else:
                    try: dst.unlink()
                    except Exception:
                        pass
            if item.is_dir() and not item.is_symlink():
                shutil.copytree(item, dst)
            else:
                shutil.copy2(item, dst)
        return True, "Update installed. Restart the app."
    except Exception as e:
        return False, f"Install failed: {e}"
    finally:
        try:
            shutil.rmtree(staging, ignore_errors=True)
        except Exception:
            pass
