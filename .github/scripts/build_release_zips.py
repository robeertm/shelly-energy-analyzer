#!/usr/bin/env python3
"""Build OS-friendly ZIP artifacts for GitHub Releases.

Creates:
  dist/shelly_energy_analyzer_<TAG>_windows.zip
  dist/shelly_energy_analyzer_<TAG>_macos.zip
  dist/shelly_energy_analyzer_<TAG>_linux.zip

Notes:
- Excludes: .git, .github, dist, build, .venv, data, logs, __pycache__, *.pyc, config.json
- Ensures executable bit for start.command and start.sh inside the ZIP.
"""

from __future__ import annotations

import fnmatch
import os
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

EXCLUDE_DIRS = {
    ".git",
    ".github",
    "dist",
    "build",
    ".venv",
    "venv",
    "__pycache__",
    "data",
    "logs",
}

EXCLUDE_FILES = {
    "config.json",
}

EXCLUDE_GLOBS = [
    "*.pyc",
    "*.pyo",
    "*.pyd",
    "*.so",
    "*.egg-info/*",
    "*.log",
    "*.sqlite",
    "*.sqlite3",
]

EXECUTABLES = {
    "start.command": 0o755,
    "start.sh": 0o755,
}


def should_exclude(path: Path) -> bool:
    rel = path.relative_to(ROOT)
    parts = rel.parts
    if parts and parts[0] in EXCLUDE_DIRS:
        return True
    if path.is_dir():
        return False
    if rel.name in EXCLUDE_FILES:
        return True
    for pat in EXCLUDE_GLOBS:
        if fnmatch.fnmatch(rel.as_posix(), pat) or fnmatch.fnmatch(rel.name, pat):
            return True
    return False


def add_file(zf: zipfile.ZipFile, file_path: Path) -> None:
    rel = file_path.relative_to(ROOT).as_posix()
    info = zipfile.ZipInfo(rel)
    info.date_time = (1980, 1, 1, 0, 0, 0)

    # Default perms: 644
    mode = 0o644
    if rel in EXECUTABLES:
        mode = EXECUTABLES[rel]

    info.external_attr = (mode & 0xFFFF) << 16

    with file_path.open("rb") as f:
        data = f.read()
    zf.writestr(info, data, compress_type=zipfile.ZIP_DEFLATED)


def build_zip(tag: str, suffix: str) -> Path:
    dist = ROOT / "dist"
    dist.mkdir(exist_ok=True)

    out = dist / f"shelly_energy_analyzer_{tag}_{suffix}.zip"
    if out.exists():
        out.unlink()

    with zipfile.ZipFile(out, "w") as zf:
        for p in ROOT.rglob("*"):
            if should_exclude(p):
                continue
            if p.is_dir():
                continue
            add_file(zf, p)

    return out


def main() -> int:
    tag = sys.argv[1] if len(sys.argv) > 1 else "dev"

    # Normalize tag (keep as-is if user uses vX.Y...)
    tag = tag.strip()

    # Build three artifacts (they contain all start scripts, but are labeled for users)
    windows_zip = build_zip(tag, "windows")
    macos_zip = build_zip(tag, "macos")
    linux_zip = build_zip(tag, "linux")

    print(f"Built: {windows_zip}")
    print(f"Built: {macos_zip}")
    print(f"Built: {linux_zip}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
