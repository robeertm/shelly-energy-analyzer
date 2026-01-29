from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import List


EXCLUDE_NAMES = {
    ".venv",
    "data",
    "logs",
    "config.json",
    ".git",
    ".github",  # not needed for runtime install
    "__pycache__",
}


def _wait_for_pid(pid: int, timeout_s: float = 15.0) -> None:
    if pid <= 0:
        return
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        try:
            # signal 0 checks existence on Unix
            if os.name != "nt":
                os.kill(pid, 0)
            else:
                # On Windows: no reliable kill(0); just sleep a bit.
                pass
            time.sleep(0.25)
        except Exception:
            return
    # timeout: proceed anyway


def _safe_rmtree(p: Path) -> None:
    try:
        if p.exists():
            shutil.rmtree(p)
    except Exception:
        pass


def _copy_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        _safe_rmtree(dst)
    shutil.copytree(src, dst)


def _copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def _iter_items(folder: Path) -> List[Path]:
    return [folder / x for x in os.listdir(folder)]


def _venv_python(app_dir: Path) -> Path | None:
    v = app_dir / ".venv"
    if not v.exists():
        return None
    if os.name == "nt":
        py = v / "Scripts" / "python.exe"
    else:
        py = v / "bin" / "python"
    return py if py.exists() else None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--app-dir", required=True)
    ap.add_argument("--staging-dir", required=True)
    ap.add_argument("--restart", required=True)
    ap.add_argument("--wait-pid", type=int, default=0)
    ap.add_argument("--update-deps", type=int, default=1)
    args = ap.parse_args()

    app_dir = Path(args.app_dir).resolve()
    staging = Path(args.staging_dir).resolve()
    restart = Path(args.restart).resolve()

    _wait_for_pid(int(args.wait_pid or 0))

    # Replace app files, preserve user data/config/venv.
    for item in _iter_items(staging):
        name = item.name
        if name in EXCLUDE_NAMES:
            continue
        if name.endswith(".pyc") or name == ".DS_Store":
            continue

        dst = app_dir / name
        try:
            if item.is_dir():
                _copy_tree(item, dst)
            else:
                _copy_file(item, dst)
        except Exception as e:
            print(f"[updater] Failed to copy {name}: {e}", file=sys.stderr)

    # Update dependencies if venv exists and requirements.txt exists
    if int(args.update_deps or 0) == 1:
        req = app_dir / "requirements.txt"
        py = _venv_python(app_dir)
        if py and req.exists():
            try:
                subprocess.run([str(py), "-m", "pip", "install", "-r", str(req)], check=False)
            except Exception as e:
                print(f"[updater] pip install failed: {e}", file=sys.stderr)

    # Restart app
    try:
        if os.name == "nt":
            subprocess.Popen(["cmd", "/c", "start", "", str(restart)], cwd=str(app_dir))
        else:
            subprocess.Popen([str(restart)], cwd=str(app_dir))
    except Exception as e:
        print(f"[updater] restart failed: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
