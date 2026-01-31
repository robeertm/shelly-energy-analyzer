from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import List


def _ensure_executable(p: Path) -> None:
    """Ensure a script is executable (macOS/Linux)."""
    try:
        if not p.exists():
            return
        # Add +x for user/group/other
        mode = p.stat().st_mode
        p.chmod(mode | 0o111)
    except Exception:
        pass



def _spawn_detached(cmd: List[str], cwd: Path) -> None:
    """Spawn a process fully detached from the current session (best-effort)."""
    try:
        if os.name == "nt":
            # On Windows, use DETACHED_PROCESS + NEW_PROCESS_GROUP
            subprocess.Popen(
                cmd,
                cwd=str(cwd),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP,
                close_fds=False,
            )
        else:
            # On macOS/Linux: nohup + new session + no stdio
            subprocess.Popen(
                cmd,
                cwd=str(cwd),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
                close_fds=True,
            )
    except Exception:
        pass

def _clear_quarantine(target: Path) -> None:
    """Best-effort remove Gatekeeper quarantine attributes on macOS."""
    try:
        if sys.platform != "darwin":
            return
        subprocess.run(["xattr", "-dr", "com.apple.quarantine", str(target)], check=False)
    except Exception:
        pass


def _restart_app(restart: Path, app_dir: Path) -> None:
    """Restart using a robust method (bash for .command/.sh)."""
    if os.name == "nt":
        # Prefer start.bat inside app_dir if restart is missing
        if not restart.exists():
            cand = app_dir / "start.bat"
            if cand.exists():
                restart = cand
        subprocess.Popen(["cmd", "/c", "start", "", str(restart)], cwd=str(app_dir))
        return

    # Fallback to known start scripts if restart path is wrong/missing
    if not restart.exists():
        for cand_name in ("start.command", "start.sh"):  # macOS/Linux
            cand = app_dir / cand_name
            if cand.exists():
                restart = cand
                break

    # Ensure exec bits on common start scripts (ZIP extraction may drop +x)
    _ensure_executable(app_dir / "start.command")
    _ensure_executable(app_dir / "start.sh")
    _ensure_executable(restart)

    # Remove quarantine best-effort
    _clear_quarantine(app_dir)

    # On macOS, .command/.sh are shell scripts; running via bash avoids relying on exec bit.
    # IMPORTANT: spawn fully detached so the restart survives the app shutdown / terminal closing.
    suffix = restart.suffix.lower()
    if suffix in (".command", ".sh"):
        if os.name == "nt":
            _spawn_detached([str(restart)], cwd=app_dir)
        else:
            _spawn_detached(["/usr/bin/nohup", "/bin/bash", str(restart)], cwd=app_dir)
    else:
        _spawn_detached([str(restart)], cwd=app_dir)

    # Give the child a moment to launch before we exit
    try:
        time.sleep(0.5)
    except Exception:
        pass


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

    # Resolve restart path robustly: if relative, interpret relative to app_dir
    restart_arg = Path(args.restart)
    if restart_arg.is_absolute():
        restart = restart_arg.resolve()
    else:
        restart = (app_dir / restart_arg).resolve()

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
        _restart_app(restart, app_dir)
    except Exception as e:
        print(f"[updater] restart failed: {e}", file=sys.stderr)
        return 2
    return 0



if __name__ == "__main__":
    raise SystemExit(main())